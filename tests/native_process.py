"""Respostas do processo nativo falso; não valida compilação MQL5 real."""
import re
from pathlib import Path
from mt5_extracao.bootstrap import paths


def compile_observer(command, prefix):
    match = re.search(r'/compile:"([^"]+)"', command)
    if match is None:
        return None
    source = paths.wine_to_linux_path(match.group(1), prefix) if prefix is not None else Path(match.group(1))
    if source.name != "mt5x_session.mq5":
        return None
    log_match = re.search(r'/log:"([^"]+)"', command)
    log = paths.wine_to_linux_path(log_match.group(1), prefix) if log_match is not None and prefix is not None else source.with_suffix(".log")
    source.with_suffix(".ex5").write_bytes(b"compiled-test-observer")
    log.write_bytes("Result: 0 errors, 0 warnings\n".encode("utf-16"))
    return 1


def logical_expert(directory, name):
    target = Path(directory) / "MQL5" / "Experts" / Path(*name.replace("\\", "/").split("/"))
    source = target.with_suffix(".mq5")
    if source.stem == "mt5x_session" and source.is_file():
        match = re.search(r'#include "([^"]+\.mq5)"', source.read_text(encoding="utf-8"))
        if match is not None:
            return Path(match.group(1)).stem
    return target.stem


def configure_test_safety(config, folder, version="1.4.0"):
    """Manifesto falso; o ensaio de processo não certifica código MQL5 nem Safety real."""
    from tests.test_robots_safety_lib import make_lib
    library = make_lib(Path(folder) / "safety-fixture", version=version)
    config.set("ROBOTS", "safety_lib_dir", str(library))


def fake_session_commands(monkeypatch):
    """Liga as sessões privadas ao processo falso escolhido pelo ensaio."""
    from mt5_extracao.robots import sessions, mt5_tools
    import psutil
    # O processo é falso: não varrer processos reais do host (inclusive Windows).
    # Ensaios de ownership instalam seu inventário explícito após esta fixture.
    monkeypatch.setattr(psutil, "process_iter", lambda: iter(()))
    def command(self, command, cwd, prefix, timeout, compiling=False):
        return mt5_tools.run_windows_command(command, cwd, prefix, timeout)
    monkeypatch.setattr(sessions.Session, "_run_command", command)
