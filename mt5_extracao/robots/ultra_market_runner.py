"""Operação serializada da sonda de ticks ou do controle S09 no Tester local."""
from datetime import datetime, timedelta
import json
from pathlib import Path
import shutil
from uuid import uuid4

from ..analytics.data_eligibility import tester_report
from ..laboratory import ultra_market, ultra_market_cycle
from . import mt5_tools as mt5, safety_lib, ultra_hadouken, ultra_runner


def _headers(config, expert, terminal):
    deps = {f"HADOUKEN/{name}": path for name, path in ultra_hadouken.headers(config).items()}
    lib = safety_lib.lib_dir(config)
    if lib is None:
        raise ValueError("Configure a biblioteca Safety fixada antes do controle S09.")
    contract = safety_lib.read_manifest(lib)
    name = "HDK_ExecutionResult.mqh"
    header = lib / "Include" / contract["include_dir"] / name
    if ultra_runner._sha(header) != contract["sha256"].get(name):
        raise ValueError("O contrato de resultado do Safety diverge do manifesto da biblioteca.")
    deps[name] = header
    for name, path in deps.items():
        ultra_runner._install(path, expert.parent / name, terminal)
    return {name: ultra_runner._sha(path) for name, path in deps.items()}


def run(config, snapshot, output, *, mode="ticks", timeout=300):
    if mode not in ("ticks", "s09_cycle") or type(timeout) is not int or not 10 <= timeout <= 1800:
        raise ValueError("Use ticks ou s09_cycle no Tester e um prazo de 10 a 1800 segundos.")
    state = ultra_market.verify(snapshot)
    data = mt5.data_dir(config).resolve()
    if data != mt5.install_dir(config).resolve():
        raise ValueError("Use um perfil portátil autorizado do MT5 para a reprodução da fita.")
    attempt = uuid4().hex
    folder = Path(output).resolve() / attempt
    folder.mkdir(parents=True, mode=0o700)
    target = folder / "receipt.json"
    receipt = dict(attempt=attempt, mode=mode, manifest_hash=state["manifest_hash"], receipt_path=str(target))

    def save(status, **details):
        receipt.update(details, status=status)
        stage = target.with_suffix(".tmp")
        stage.write_text(json.dumps(receipt, indent=2, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")
        stage.replace(target)
        return receipt.copy()

    save("preparing")
    cycle = mode == "s09_cycle"
    name = "UltraMarketCycle" if cycle else "UltraMarketProbe"
    source = folder / f"{name}.mq5"
    export = ultra_market_cycle.export if cycle else ultra_market.export_probe
    try:
        bundle = export(snapshot, source, attempt=attempt)
        key = "UltraMarket_" + attempt[:16]
        expert = data / "MQL5" / "Experts" / "MT5XUltra" / key / source.name
        save("exported", **bundle)
        startup = ultra_runner._demo_startup()
        with mt5.tester_lease():
            if mt5.terminal_running(config):
                return save("terminal_busy")
            ultra_runner._install(source, expert, data)
            if cycle:
                save("compiling", headers=_headers(config, expert, data))
            result = mt5.compile_source(expert, mt5.find_metaeditor(config), mt5.wine_prefix(config), timeout=timeout)
            if result.log_path and Path(result.log_path).is_file():
                shutil.copyfile(result.log_path, folder / "compile.log")
            if not result.ok or result.warnings:
                return save("compile_failed", reason=result.summary)
            binary = folder / f"{name}.ex5"
            shutil.copyfile(result.compiled, binary)
            dates = [datetime.fromisoformat(state["time"][k]) for k in ("start_inclusive", "end_exclusive")]
            ini = mt5.tester_ini(mt5.expert_name(Path(result.compiled), data), bundle["symbol"], "M1",
                dates[0], dates[1] + timedelta(days=1), 1000, key, model="4", visual=False,
                currency=state["account"]["currency"])
            leverage = state["account"]["leverage"]
            if type(leverage) is not int or leverage < 1:
                return save("account_contract_missing", reason="Alavancagem da conta indisponível.")
            ini += f"ExecutionMode=0\r\nLeverage=1:{leverage}\r\n"
            (folder / "tester.ini").write_bytes(ini.encode("utf-16"))
            save("compiled", binary_sha256=ultra_runner._sha(binary), tester_ini="tester.ini")
            before = ultra_runner._offsets(data, "Tester")
            error = None
            try:
                html = mt5.run_tester(config, data, startup + ini, key, timeout=timeout)
                shutil.copyfile(html, folder / "report.htm")
                report_text = mt5.read_log_text(folder / "report.htm")
                save("report_saved", report_sha256=ultra_runner._sha(folder / "report.htm"),
                     tester_data_quality=tester_report(report_text, "4"))
            except RuntimeError as exc:
                error = str(exc)
            finally:
                logs = ultra_runner._collect(data, "Tester", before, folder / "logs")
            agent_logs = [p for p in logs if any(part.casefold().startswith("agent-")
                          for part in p.relative_to(folder / "logs").parts[:-1])]
            marker = ultra_market_cycle.MARKER if cycle else ultra_market.PROBE_MARKER
            journal, _ = ultra_runner._marked(agent_logs, marker)
            if journal is None:
                return save("journal_missing", reason=error or "Sem os marcadores do agente local.")
            save("journal_saved", journal=journal.relative_to(folder).as_posix(),
                 journal_sha256=ultra_runner._sha(journal))
            raw = mt5.read_log_text(journal)
            if cycle:
                if error:
                    return save("report_missing", reason=error)
                data_result = ultra_market_cycle.compare(snapshot, binding=bundle["binding"], agent_journal=raw,
                                                         tester_html=report_text)
            else:
                common = mt5.common_files_dir(config)
                if common is None:
                    return save("tape_missing")
                tape = folder / "ticks-on-tick.bin"
                shutil.copyfile(common / bundle["tape"], tape)
                data_result = ultra_market.compare_probe(snapshot, tape, binding=bundle["binding"], agent_journal=raw)
            return save(data_result["status"], comparison=data_result, report_error=error)
    except (OSError, ValueError, RuntimeError) as exc:
        return save("failed", reason=str(exc))
