"""Instala e confere uma execução Ultra no terminal portátil configurado, sob a posse do Tester."""
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
from types import SimpleNamespace
from uuid import uuid4

from ..laboratory import ultra_tester
from . import mt5_tools as mt5
from . import ultra_restart


def _sha(path):
    return sha256(path.read_bytes()).hexdigest()


def _install(source, target, root):
    if not target.resolve().is_relative_to(root):
        raise ValueError("O destino MQL5 sai do perfil configurado.")
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        if _sha(source) != _sha(target):
            raise ValueError(f"Há outro conteúdo em {target.name}. O arquivo foi preservado.")
    else:
        staged = target.with_name(target.name + "." + uuid4().hex)
        try:
            shutil.copyfile(source, staged)
            os.link(staged, target)
        finally:
            staged.unlink(missing_ok=True)
    return target


def _logs(data, component):
    roots = [data / "MQL5" / "logs"] if component == "MQL5" else list((data / "Tester").glob("**/logs"))
    return sorted(file for root in roots for file in root.glob("*.log"))


def _offsets(data, component):
    result = {}
    for path in _logs(data, component):
        with path.open("rb") as stream:
            size = stream.seek(0, os.SEEK_END)
            stream.seek(max(0, size-256))
            result[path] = (size, stream.read())
    return result


def _collect(data, component, previous, dest):
    """Conserva os bytes da nova execução, incluindo rotação, em journals separados por origem."""
    dest.mkdir()
    collected = []
    for path in _logs(data, component):
        offset, tail = previous.get(path, (0, b""))
        with path.open("rb") as stream:
            bom = stream.read(2)
            stream.seek(max(0, offset-len(tail)))
            if stream.read(len(tail)) != tail:
                offset = 0
            stream.seek(offset)
            chunk = stream.read()
        if chunk:
            output = dest / path.relative_to(data)
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_bytes((bom if offset and bom in (b"\xff\xfe", b"\xfe\xff") else b"") + chunk)
            collected.append(output)
    return collected


def _script(config, data, expert, chart, tag, output, timeout):
    before = _offsets(data, "MQL5")
    ini = data / ("ultra_" + tag + ".ini")
    prefix = mt5.wine_prefix(config)
    command = '"{}" /portable /config:"{}"'.format(
        mt5._windows_path(mt5.validate_terminal(config), prefix), mt5._windows_path(ini, prefix))
    settings = ("[Experts]\r\nEnabled=1\r\nAllowLiveTrading=0\r\nAllowDllImport=0\r\n"
                f"[StartUp]\r\nSymbol={chart}\r\nPeriod=M1\r\nScript={expert}\r\nShutdownTerminal=1\r\n")
    with ini.open("xb") as handle:
        handle.write(settings.encode("utf-16"))
    try:
        mt5.run_windows_command(command, data, prefix, timeout=timeout)
    finally:
        ini.unlink(missing_ok=True)
        journals = _collect(data, "MQL5", before, output)
    return journals


def _marked(journals, marker):
    found = []
    for path in journals:
        records = [line.partition(marker)[2] for line in mt5.read_log_text(path).splitlines() if marker in line]
        if records:
            found.append((path, records))
    if not found:
        return None, []
    path, records = found[0]
    if any(other != records for _, other in found[1:]):
        raise ValueError("Os journals do agente e do terminal divergem entre si.")
    return path, records


def run(db, config, package, output, *, origin="strategy_tester", chart="EURUSD", timeout=300, resume=None, prepare_only=False):
    if origin not in ultra_tester.MARKERS or not re.fullmatch(r"[\w.#-]{1,31}", chart):
        raise ValueError("Origem ou símbolo do gráfico auxiliar inválido.")
    if type(timeout) is not int or not 10 <= timeout <= 1800:
        raise ValueError("O prazo por etapa deve estar entre 10 e 1800 segundos.")
    if resume is not None and origin != "terminal_restart":
        raise ValueError("A retomada por recibo exige a origem terminal_restart.")
    if prepare_only and origin != "strategy_tester":
        raise ValueError("A preparação do símbolo se aplica à origem strategy_tester.")
    output = Path(output).resolve()
    output.mkdir(mode=0o700, parents=True, exist_ok=True)
    attempt = uuid4().hex
    folder = output / attempt
    folder.mkdir(mode=0o700)
    receipt = {"version": 1, "attempt": attempt, "origin": origin, "directory": str(folder),
               "receipt_path": str(folder / "receipt.json"), "native_order_parity": "pending"}
    def save(status, **extra):
        receipt.update(status=status, **extra)
        temp = folder / "receipt.tmp"
        temp.write_text(json.dumps(receipt, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        temp.replace(folder / "receipt.json")
        return receipt.copy()
    save("preparing")
    try:
        bundle = folder / "bundle"
        parent = None
        options = {"prepare_tester": True} if origin == "strategy_tester" else {}
        if origin == "terminal_restart":
            parent = ultra_restart.read_parent(resume, package["package_hash"])
            options["restart_id"] = parent[1]["restart_id"] if parent else attempt
            if parent:
                receipt["parent_receipt"] = str(parent[0])
        exported = ultra_tester.export(db, package, bundle, **options)
        receipt.update(package_hash=exported["package_hash"], symbol=exported["symbol"], input_hashes=exported["sha256"])
        prefix = mt5.wine_prefix(config)
        data = mt5.data_dir(config).resolve()
        if data != mt5.install_dir(config).resolve():
            return save("portable_profile_required")
        with mt5.tester_lease():
            if mt5.terminal_running(config):
                return save("terminal_busy")
            session = SimpleNamespace(id=attempt, directory=data, child_config=config)
            mt5.register_native_context(session)
            try:
                return _execute(config, db, data, prefix, folder, bundle, exported, package, origin, chart, timeout, save, parent, prepare_only)
            finally:
                mt5.release_native_context(session)
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
        return save("failed", error_type=type(error).__name__, reason=str(error))


def _execute(config, db, data, prefix, folder, bundle, exported, package, origin, chart, timeout, save, parent, prepare_only):
    mql = data / "MQL5"
    marker = "MT5X_ULTRA_DEMO|"
    probe = origin != "strategy_tester"
    main = {"terminal_history": "UltraTerminalProbe", "terminal_restart": "UltraRestartProbe",
            "strategy_tester": "UltraPreparedTester"}[origin]
    scripts = [("UltraImport", "Scripts"), (main, "Scripts" if probe else "Experts")]
    if not probe:
        scripts.insert(1, ("UltraTesterContract", "Scripts"))
    if not probe and not prepare_only:
        shutil.copyfile(ultra_tester.TEMPLATES / "UltraDemoStatus.mq5.tmpl", bundle / "UltraDemoStatus.mq5")
        scripts.insert(0, ("UltraDemoStatus", "Scripts"))
    editor = mt5.find_metaeditor(config)
    names, binaries = {}, {}
    for name, kind in scripts:
        source = bundle / (name + ".mq5")
        subdir = Path("MT5XUltra") / (name + "_" + _sha(source)[:20])
        dest = _install(source, mql / kind / subdir / source.name, data)
        compiled = mt5.compile_source(dest, editor, prefix, timeout=timeout)
        if compiled.log_path and Path(compiled.log_path).is_file():
            shutil.copyfile(compiled.log_path, folder / (name + ".compile.log"))
        if not compiled.ok or compiled.warnings:
            return save("compile_failed", component=name, detail=compiled.summary)
        binaries[name] = _sha(Path(compiled.compiled))
        names[name] = (subdir / name).as_posix().replace("/", "\\")
    save("compiled", binary_sha256=binaries)
    def invoke(name, phase=""):
        return _script(config, data, names[name], chart, folder.name + name + phase, folder / (name + phase), timeout)
    csv = bundle / (exported["symbol"] + ".csv")
    _install(csv, mql / "Files" / csv.name, data)
    _, imported = _marked(invoke("UltraImport"), "MT5X_ULTRA_IMPORT|")
    if imported != [f'{exported["package_hash"]}|{exported["ticks"]}|0']:
        return save("import_unconfirmed")
    save("ticks_verified", ticks=exported["ticks"])
    if not probe:
        contract = exported["tester_contract"]
        journals = invoke("UltraTesterContract")
        _, ready = _marked(journals, "MT5X_ULTRA_PREP|")
        expected = [exported["package_hash"], contract["symbol"], contract["primer_bars"], exported["ticks"],
                    contract["tick_cash_cents_per_step"], contract["margin_cents_per_step"]]
        if ready != ["|".join(map(str, expected))]:
            _, errors = _marked(journals, "MT5X_ULTRA_PREP_ERROR|")
            return save("tester_contract_unconfirmed", observed=ready, property_errors=errors, tester_contract=contract)
        times = csv.read_text(encoding="utf-8").splitlines()
        def day(line):
            return datetime.fromtimestamp(int(line.partition(",")[0])/1000, timezone.utc).date()
        report = "ultra_" + folder.name
        ini = mt5.tester_ini(names[main], contract["symbol"], "M1", day(times[0]), day(times[-1])+timedelta(days=1),
                             package["policy"]["cash"]/100, report, model="4", visual=False, currency=package["policy"]["currency"])
        (folder / "tester.ini").write_bytes(ini.encode("utf-16"))
        state = save("tester_contract_verified", tester_contract=contract, observed=ready, tester_ini="tester.ini")
        if prepare_only:
            return state
    if not probe:
        _, records = _marked(invoke("UltraDemoStatus"), marker)
        if records != ["1|1"]:
            states = {"0|0": "demo_login_required", "0|1": "demo_login_required", "1|0": "demo_account_required"}
            observed = records[0] if len(records) == 1 else ""
            return save(states.get(observed, "demo_status_unavailable"), demo_check=records)
    journal = None
    if origin == "terminal_restart":
        journal, pending = ultra_restart.complete(
            folder=folder, data=data, exported=exported, parent=parent, invoke=lambda phase: invoke(main, phase),
            marked=_marked, install=_install, save=save, running=lambda: mt5.terminal_running(config))
        if pending:
            return pending
    elif probe:
        logs = invoke(main)
    else:
        before = _offsets(data, "Tester")
        try:
            path = mt5.run_tester(config, data, ini, report, timeout=timeout)
        finally:
            logs = _collect(data, "Tester", before, folder / "tester-logs")
        shutil.copyfile(path, folder / path.name)
    if journal is None:
        journal, _ = _marked(logs, ultra_tester.MARKERS[origin])
    if journal is None:
        return save("journal_missing")
    result = ultra_tester.compare(db, package, journal.read_bytes(), origin=origin)
    return save(result["status"], comparison=result, journal=journal.relative_to(folder).as_posix())
