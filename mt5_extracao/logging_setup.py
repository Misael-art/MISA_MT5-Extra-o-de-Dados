"""
Configuração central de logging.

Os módulos só fazem `log = logging.getLogger(__name__)`; quem configura handlers são
os pontos de entrada (app.py, CLI), uma única vez:

    from mt5_extracao.logging_setup import setup_logging
    setup_logging()                              # logs/mt5_extracao.log + console
    setup_logging(console_level=logging.WARNING) # CLI: console só com avisos e erros

O arquivo gira a cada 5 MB e guarda 5 cópias. Chamar de novo substitui os handlers
anteriores (não duplica linhas). teardown_logging() remove o que foi instalado.
"""
import logging
import logging.handlers
import sys
from pathlib import Path
from typing import Optional, Union

LOG_FILE = "mt5_extracao.log"
FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
MAX_BYTES = 5 * 1024 * 1024
BACKUP_COUNT = 5
_MARK = "_mt5_extracao_handler"


def default_log_dir() -> Path:
    from mt5_extracao.bootstrap import paths
    return paths.PROJECT_ROOT / "logs"


class _StderrHandler(logging.StreamHandler):
    """Escreve no sys.stderr do momento da escrita (funciona com redirecionamentos e com o pytest)."""

    def __init__(self):
        super().__init__()

    @property
    def stream(self):
        return sys.stderr

    @stream.setter
    def stream(self, value):
        pass


def setup_logging(log_dir: Optional[Union[str, Path]] = None, level: int = logging.INFO,
                  console: bool = True, console_level: Optional[int] = None,
                  filename: str = LOG_FILE) -> Optional[Path]:
    """Instala o handler de arquivo (rotativo) e o de console no logger raiz. Retorna o arquivo de log."""
    teardown_logging()
    root = logging.getLogger()
    root.setLevel(min(level, console_level or level))
    formatter = logging.Formatter(FORMAT)
    log_path = None
    log_dir = Path(log_dir) if log_dir is not None else default_log_dir()
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / filename
        fh = logging.handlers.RotatingFileHandler(log_path, maxBytes=MAX_BYTES, backupCount=BACKUP_COUNT,
                                                  encoding="utf-8")
        fh.setLevel(level)
        fh.setFormatter(formatter)
        setattr(fh, _MARK, True)
        root.addHandler(fh)
    except OSError as e:   # pasta sem permissão de escrita: segue só com o console
        print(f"Aviso: não foi possível gravar logs em {log_dir} ({e}).", file=sys.stderr)
        log_path = None
    if console:
        ch = _StderrHandler()
        ch.setLevel(console_level if console_level is not None else level)
        ch.setFormatter(formatter)
        setattr(ch, _MARK, True)
        root.addHandler(ch)
    return log_path


def teardown_logging() -> None:
    root = logging.getLogger()
    for handler in list(root.handlers):
        if getattr(handler, _MARK, False):
            root.removeHandler(handler)
            handler.close()

