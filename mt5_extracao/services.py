"""
Montagem dos componentes de extração a partir do config.ini.

Usado pela interface gráfica (app.py) e pela linha de comando (mt5_extracao.cli)
para que as duas leiam a configuração exatamente do mesmo jeito. Sem Tkinter.
"""
import configparser
import logging
import os
from typing import Optional

log = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = os.path.join("config", "config.ini")


def load_config(config_path: str = DEFAULT_CONFIG_PATH) -> configparser.ConfigParser:
    """Lê o config.ini. Lança FileNotFoundError se o arquivo não existir."""
    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"Arquivo de configuração não encontrado: {config_path}. "
            "Execute o instalador ou: python -m mt5_extracao.bootstrap configure")
    config = configparser.ConfigParser()
    config.read(config_path, encoding="utf-8")
    return config


def create_db_manager(config: configparser.ConfigParser, db_path: Optional[str] = None):
    from mt5_extracao.database_manager import DatabaseManager
    db_type = config.get("DATABASE", "type", fallback="sqlite")
    path = db_path or config.get("DATABASE", "path", fallback="database/mt5_data.db")
    return DatabaseManager(db_type=db_type, db_path=path)


def create_external_source(config: configparser.ConfigParser):
    """Fonte externa para o fallback M1 ([FALLBACK]) ou None."""
    from mt5_extracao.external_data_source import DummyExternalSource
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
    )
