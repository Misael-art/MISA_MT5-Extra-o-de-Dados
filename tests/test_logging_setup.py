"""T4.2: logging centralizado."""
import logging
import logging.handlers
import subprocess
import sys
from pathlib import Path

from mt5_extracao import logging_setup

REPO = Path(__file__).resolve().parents[1]


def _ours():
    return [h for h in logging.getLogger().handlers if getattr(h, logging_setup._MARK, False)]


def test_setup_is_idempotent_and_rotating(tmp_path):
    try:
        path = logging_setup.setup_logging(tmp_path / "logs")
        logging_setup.setup_logging(tmp_path / "logs")
        handlers = _ours()
        assert len(handlers) == 2   # um arquivo + um console, mesmo chamando duas vezes
        fh = next(h for h in handlers if isinstance(h, logging.handlers.RotatingFileHandler))
        assert fh.maxBytes == 5 * 1024 * 1024 and fh.backupCount == 5
        logging.getLogger("mt5_extracao.teste").info("mensagem única")
        fh.flush()
        assert path.read_text(encoding="utf-8").count("mensagem única") == 1
    finally:
        logging_setup.teardown_logging()
    assert _ours() == []


def test_console_level_is_separate(tmp_path, capsys):
    try:
        path = logging_setup.setup_logging(tmp_path, console_level=logging.WARNING)
        log = logging.getLogger("mt5_extracao.teste2")
        log.info("só no arquivo")
        log.warning("nos dois")
        err = capsys.readouterr().err
        assert "nos dois" in err and "só no arquivo" not in err
        for h in _ours():
            h.flush()
        text = path.read_text(encoding="utf-8")
        assert "só no arquivo" in text and "nos dois" in text
    finally:
        logging_setup.teardown_logging()


def test_importing_modules_creates_no_logs_dir_nor_handlers(tmp_path):
    code = (
        "import logging, os\n"
        "import mt5_extracao.database_manager, mt5_extracao.mt5_connector, mt5_extracao.error_handler\n"
        "import mt5_extracao.security, mt5_extracao.data_exporter, mt5_extracao.indicator_calculator\n"
        "import mt5_extracao.historical_extractor\n"
        "names = [n for n in logging.root.manager.loggerDict if n.startswith('mt5_extracao')]\n"
        "bad = [n for n in names if getattr(logging.getLogger(n), 'handlers', [])]\n"
        "assert not bad, bad\n"
        "assert not os.path.exists('logs'), 'pasta logs criada no diretório atual'\n"
    )
    env = {"PYTHONPATH": str(REPO)}
    import os
    env = {**os.environ, **env}
    r = subprocess.run([sys.executable, "-c", code], cwd=str(tmp_path), capture_output=True, text=True, env=env)
    assert r.returncode == 0, r.stderr[-2000:]


def test_no_module_configures_handlers():
    offenders = []
    for path in (REPO / "mt5_extracao").rglob("*.py"):
        if path.name == "logging_setup.py":
            continue
        text = path.read_text(encoding="utf-8")
        if "if not log.handlers" in text or 'os.makedirs("logs"' in text:
            offenders.append(path.name)
    assert not offenders
