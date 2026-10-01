"""
Montagem dos componentes de extração a partir do config.ini.

Usado pela interface gráfica (app.py) e pela linha de comando (mt5_extracao.cli)
para que as duas leiam a configuração exatamente do mesmo jeito. Sem Tkinter.
"""
import configparser
import logging
import os
import sys
from typing import Optional

log = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = os.path.join("config", "config.ini")
DEFAULT_DB_PATH = os.path.join("database", "mt5_data.db")
APP_NAME = "mt5-extracao"


def project_root() -> str:
    from mt5_extracao.bootstrap import paths
    return str(paths.PROJECT_ROOT)


def resolve_config_path(config_path: str = DEFAULT_CONFIG_PATH) -> str:
    """Caminho relativo que não existe no diretório atual é procurado na pasta do projeto
    (permite rodar o mt5x de qualquer pasta)."""
    if os.path.isabs(config_path) or os.path.exists(config_path):
        return config_path
    candidate = os.path.join(project_root(), config_path)
    return candidate if os.path.exists(candidate) else config_path


def user_data_dir() -> str:
    """Pasta de dados do usuário (sem dependências): %LOCALAPPDATA%\\mt5-extracao no Windows,
    ~/Library/Application Support/mt5-extracao no macOS, $XDG_DATA_HOME ou ~/.local/share/mt5-extracao no Linux."""
    if os.name == "nt":
        base = os.environ.get("LOCALAPPDATA") or os.path.join(os.path.expanduser("~"), "AppData", "Local")
    elif sys.platform == "darwin":
        base = os.path.join(os.path.expanduser("~"), "Library", "Application Support")
    else:
        base = os.environ.get("XDG_DATA_HOME") or os.path.join(os.path.expanduser("~"), ".local", "share")
    return os.path.join(base, APP_NAME)


def resolve_db_path(config: configparser.ConfigParser, db_path: Optional[str] = None) -> str:
    """
    Caminho do banco:
    - db_path explícito (ex.: mt5x --db) ou [DATABASE] path absoluto: usado como está;
    - [DATABASE] path relativo: dentro de [APP] data_dir, que pode ser
        vazio (padrão) -> pasta do projeto (comportamento de sempre, independente do diretório atual);
        auto            -> pasta de dados do usuário (user_data_dir());
        um caminho      -> essa pasta.
    Migração: com data_dir definido, se o banco já existe na pasta do projeto e ainda não no novo
    local, continua usando o do projeto (instalações antigas seguem funcionando).
    """
    if db_path:
        return db_path
    path = config.get("DATABASE", "path", fallback=DEFAULT_DB_PATH).strip() or DEFAULT_DB_PATH
    path = os.path.expanduser(path)
    if os.path.isabs(path):
        return path
    in_project = os.path.normpath(os.path.join(project_root(), path))
    data_dir = config.get("APP", "data_dir", fallback="").strip()
    if not data_dir:
        return in_project
    base = user_data_dir() if data_dir.lower() == "auto" else os.path.expanduser(data_dir)
    target = os.path.normpath(os.path.join(base, path))
    if os.path.exists(in_project) and not os.path.exists(target):
        log.info(f"Banco existente em {in_project}; mantido (o novo local seria {target}). "
                 "Para mudar, mova o arquivo para lá.")
        return in_project
    return target


def load_config(config_path: str = DEFAULT_CONFIG_PATH) -> configparser.ConfigParser:
    """Lê o config.ini. Lança FileNotFoundError se o arquivo não existir."""
    config_path = resolve_config_path(config_path)
    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"Arquivo de configuração não encontrado: {config_path}. "
            "Execute o instalador ou: python -m mt5_extracao.bootstrap configure")
    config = configparser.ConfigParser()
    config.read(config_path, encoding="utf-8")
    return config


def database_url(config: configparser.ConfigParser) -> str:
    """
    URL do PostgreSQL a partir de [DATABASE] url. A senha vem de DB_PASSWORD (variável de ambiente
    ou .env do projeto) e nunca do config.ini. Sem driver na URL, usa psycopg (versão 3).
    """
    from sqlalchemy.engine import make_url
    raw = config.get("DATABASE", "url", fallback="").strip()
    if not raw:
        raise ValueError("Com [DATABASE] type = postgresql, defina [DATABASE] url, por exemplo: "
                         "postgresql://usuario@localhost:5432/mt5 (senha em DB_PASSWORD no arquivo .env)")
    url = make_url(raw)
    if url.password is not None:
        log.warning("A senha está em [DATABASE] url no config.ini; mova-a para DB_PASSWORD no arquivo .env.")
    else:
        password = os.environ.get("DB_PASSWORD")
        if password is None:
            from mt5_extracao.bootstrap.config_builder import read_env_file
            password = read_env_file(os.path.join(project_root(), ".env")).get("DB_PASSWORD")
        if password:
            url = url.set(password=password)
    if url.drivername == "postgresql":
        url = url.set(drivername="postgresql+psycopg")
    return url.render_as_string(hide_password=False)


def create_db_manager(config: configparser.ConfigParser, db_path: Optional[str] = None):
    from mt5_extracao.database_manager import DatabaseManager
    db_type = config.get("DATABASE", "type", fallback="sqlite").strip().lower()
    if db_type == "postgresql":
        path = db_path or database_url(config)
    else:
        path = resolve_db_path(config, db_path)
    return DatabaseManager(db_type=db_type, db_path=path,
                           time_basis=config.get("APP", "time_basis", fallback="broker").strip().lower(),
                           broker_utc_offset=config.getfloat("APP", "broker_utc_offset", fallback=-3.0))


def create_external_source(config: configparser.ConfigParser):
    """Fonte externa para o fallback M1 ([FALLBACK]) ou None."""
    from mt5_extracao.external_data_source import CsvExternalSource, DummyExternalSource
    try:
        enabled = config.getboolean("FALLBACK", "external_source_m1_fallback_enabled", fallback=False)
        source_type = (config.get("FALLBACK", "external_source_m1_type", fallback="") or "").strip().lower()
    except (configparser.Error, ValueError) as e:
        log.error(f"Erro ao ler [FALLBACK] do config.ini: {e}. Fallback desativado.")
        return None
    if not enabled:
        log.info("Fallback M1 para fontes externas está desabilitado na configuração.")
        return None
    if source_type == "dummy":
        log.info("Fallback M1 habilitado. Usando DummyExternalSource.")
        return DummyExternalSource()
    if source_type == "csv":
        csv_dir = config.get("FALLBACK", "csv_dir", fallback="").strip()
        log.info(f"Fallback M1 habilitado. Usando arquivos CSV de '{csv_dir}'.")
        return CsvExternalSource(csv_dir)
    log.warning(f"Fallback M1 habilitado, mas o tipo '{source_type}' não é reconhecido. Fallback inativo.")
    return None


def chunk_config(config: configparser.ConfigParser) -> dict:
    """Tamanho dos blocos (dias) por timeframe, de [EXTRACTION]."""
    return {
        "m1": config.getint("EXTRACTION", "chunk_days_m1", fallback=30),
        "m5_m15": config.getint("EXTRACTION", "chunk_days_m5_m15", fallback=90),
        "default": config.getint("EXTRACTION", "chunk_days_default", fallback=365),
    }


def create_extractor(config, connector, db_manager, indicator_calculator):
    from mt5_extracao.historical_extractor import HistoricalExtractor
    return HistoricalExtractor(
        connector=connector,
        db_manager=db_manager,
        indicator_calculator=indicator_calculator,
        external_source=create_external_source(config),
        chunk_config=chunk_config(config),
        session=(config.get("APP", "session_start", fallback="09:00"),
                 config.get("APP", "session_end", fallback="18:30")),
    )
