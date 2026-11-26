"""Conciliação posterior de um passe de carteira já lançado no Tester local."""
import json
from pathlib import Path
import re
import shutil
from tempfile import TemporaryDirectory

from ..analytics.data_eligibility import tester_report
from ..laboratory import ultra_market, ultra_market_portfolio_audit, ultra_market_preset
from ..laboratory.ultra_cycle_report import write_market_report
from . import mt5_tools as mt5

_SUCCESS = {"single_owner_matches", "portfolio_matches", "portfolio_without_overlap"}


def _file(root, relative):
    path = root / relative
    if (Path(relative).is_absolute() or ".." in Path(relative).parts or not path.is_file()
            or not path.resolve().is_relative_to(root.resolve())):
        raise ValueError("Arquivo do passe ausente ou fora da pasta de origem.")
    return path


def _save_inputs(folder, stage, receipt):
    inputs = dict(receipt["source_files"])
    if inputs.get("UltraMarketPortfolio.mq5") != receipt["source_sha256"]:
        raise ValueError("Fonte principal divergente no recibo do passe.")
    if any(Path(name).suffix not in (".mq5", ".mqh") for name in inputs):
        raise ValueError("Extensão inesperada no inventário de fontes do passe.")
    inputs["UltraMarketPortfolio.ex5"] = receipt["binary_sha256"]
    for name, checksum in inputs.items():
        origin = _file(folder, name)
        if ultra_market._sha(origin) != checksum:
            raise ValueError("Hash divergente no inventário do passe.")
        target = stage / name
        target.parent.mkdir(exist_ok=True, parents=True)
        shutil.copyfile(origin, target)
    for name in ("preset-frozen.json", "tester.ini", "compile.log", "portfolio.set", "successor-frozen.json"):
        if (folder / name).exists():
            shutil.copyfile(_file(folder, name), stage / name)


def _copy_journal(agent, target, receipt):
    """Preserva as linhas do mesmo vínculo, inclusive quando o journal vira à meia-noite."""
    probe = ultra_market.PROBE_MARKER
    preset_marker = ultra_market_portfolio_audit.MARKER + "PRESET|"
    begin = f"{probe}BEGIN|{receipt['manifest_hash']}|{receipt['binding']}|"
    end = f"{probe}END|{receipt['binding']}|"
    header = []
    active = False
    with target.open("x", encoding="utf-8") as output:
        for path in sorted(agent.glob("*.log")):
            with path.open("rb") as stream:
                encoding = "utf-16" if stream.read(2) in (b"\xff\xfe", b"\xfe\xff") else "utf-8-sig"
            with path.open(encoding=encoding, errors="strict") as stream:
                for line in stream:
                    if active:
                        if probe + "BEGIN|" in line or preset_marker in line:
                            return False
                        output.write(line)
                        if end in line:
                            return True
                    elif preset_marker in line:
                        header = [line] if preset_marker + receipt["preset_sha256"] + "|" in line else []
                    elif probe + "BEGIN|" in line:
                        if begin in line and header:
                            output.writelines(header)
                            output.write(line)
                            active = True
                        header = []
                    elif header and ultra_market_portfolio_audit.MARKER in line:
                        header.append(line)
    return False


def _check_files(folder, receipt):
    for name, key in (("UltraMarketPortfolio.ex5", "binary_sha256"), ("report.htm", "report_sha256"),
                      (receipt["journal"], "journal_sha256")):
        if ultra_market._sha(_file(folder, name)) != receipt[key]:
            raise ValueError("Os arquivos da conciliação divergiram do recibo salvo.")
    expected_tape = receipt["comparison"]["tick_comparison"]["tape_sha256"]
    if ultra_market._sha(_file(folder, "ticks-on-tick.bin")) != expected_tape:
        raise ValueError("A fita binária divergiu da conciliação salva.")


def _read_saved(target, receipt, checksum):
    if target.is_symlink() or not target.is_dir():
        raise ValueError("Destino de conciliação incompatível.")
    saved = json.loads(_file(target, "receipt.json").read_text(encoding="utf-8"))
    if (saved.get("original_receipt_sha256") != checksum or saved["manifest_hash"] != receipt["manifest_hash"]
            or saved["binding"] != receipt["binding"] or saved["preset_sha256"] != receipt["preset_sha256"]):
        raise ValueError("A conciliação existente pertence a outro recibo.")
    _check_files(target, saved)
    return saved


def recover(config, snapshot, source):
    """Consulta o passe existente. Só publica a nova conciliação após conferir as evidências finais."""
    source = Path(source).resolve(strict=True)
    folder = source.parent
    receipt = json.loads(source.read_text(encoding="utf-8"))
    attempt, binding = receipt.get("attempt", ""), receipt.get("binding", "")
    port = receipt.get("tester_port")
    if (source.name != "receipt.json" or not isinstance(attempt, str) or not re.fullmatch(r"[a-f0-9]{32}", attempt) or folder.name != attempt
            or receipt.get("mode") != "portfolio" or not isinstance(binding, str) or not re.fullmatch(r"[a-f0-9]{64}", binding)
            or receipt.get("tape") != f"MT5XUltraMarket_{binding}.bin" or type(port) is not int or not 1 <= port <= 65535):
        raise ValueError("A retomada exige o recibo original de uma carteira lançada no Tester.")
    preset = ultra_market_preset.read(_file(folder, "preset-frozen.json"))
    if preset != receipt["preset"] or preset["preset_sha256"] != receipt["preset_sha256"]:
        raise ValueError("O preset diverge do passe original.")
    target = folder / "reconciled"
    checksum = ultra_market._sha(source)
    pending = dict(status="awaiting_native_completion", receipt_path=str(source), attempt=attempt)
    if receipt["status"] not in _SUCCESS and not target.exists() and mt5.terminal_running(config):
        return pending
    manifest = ultra_market.verify(snapshot)
    if receipt["manifest_hash"] != manifest["manifest_hash"]:
        raise ValueError("A fita diverge do passe original.")
    if receipt["status"] in _SUCCESS:
        _check_files(folder, receipt)
        return receipt
    if target.exists():
        return _read_saved(target, receipt, checksum)
    with mt5.tester_lease():
        if target.exists():
            return _read_saved(target, receipt, checksum)
        if mt5.terminal_running(config):
            return pending
        data = mt5.data_dir(config).resolve()
        common = mt5.common_files_dir(config)
        report = data / f"UltraMarket_{attempt[:16]}.htm"
        agent = data / "Tester" / f"Agent-127.0.0.1-{port}" / "logs"
        if not report.is_file() or common is None or not agent.is_dir():
            return dict(pending, status="native_report_unavailable")
        with TemporaryDirectory(prefix=".ultra-retomada-", dir=folder) as temporary:
            stage = Path(temporary) / "reconciled"
            stage.mkdir()
            relative = Path("logs") / "agent.log"
            (stage / relative).parent.mkdir()
            if not _copy_journal(agent, stage / relative, receipt):
                return dict(pending, status="native_journal_unconfirmed")
            _save_inputs(folder, stage, receipt)
            shutil.copyfile(report, stage / "report.htm")
            tape = stage / "ticks-on-tick.bin"
            shutil.copyfile(_file(common, receipt["tape"]), tape)
            raw = mt5.read_log_text(stage / relative)
            html = mt5.read_log_text(stage / "report.htm")
            ticks = ultra_market.compare_probe(snapshot, tape, binding=binding, agent_journal=raw)
            result = ultra_market_portfolio_audit.compare(receipt, ticks, raw, html, common, stage,
                                                          snapshot=snapshot, manifest=manifest)
            checked = dict(receipt, receipt_path=str(target / "receipt.json"), comparison=result,
                           status=result["status"], last_completed="report_compared", report_error=None,
                           original_receipt_sha256=checksum, original_receipt=str(source),
                           tester_data_quality=tester_report(html, "4"), report_sha256=ultra_market._sha(stage / "report.htm"),
                           journal=relative.as_posix(), journal_sha256=ultra_market._sha(stage / relative),
                           portfolio_report="carteira.html")
            checked.pop("reason", None)
            write_market_report(checked, stage / "carteira.html")
            (stage / "receipt.json").write_text(json.dumps(checked, indent=2, allow_nan=False, ensure_ascii=False) + "\n",
                                                encoding="utf-8")
            stage.rename(target)
            return checked
