"""
Geração idempotente de config/config.ini, .env e config/default_symbols.json.

Regras:
- Valores já existentes no config.ini do usuário são PRESERVADOS; só chaves
  ausentes recebem o padrão. Overrides explícitos (ex.: --mt5-path) vencem.
- A senha nunca vai para o config.ini; credenciais ficam no .env (lido por
  mt5_extracao.security.CredentialManager), com permissão 600 no Linux.
- Toda escrita é atômica (arquivo temporário + os.replace).
"""
import configparser
import json
import os
import tempfile
from collections import OrderedDict
from pathlib import Path
from typing import Dict, Optional

from . import paths

# Seções/chaves lidas pelo app (app.py, mt5_connector.py) + ponte Linux.
DEFAULTS: "OrderedDict[str, OrderedDict[str, str]]" = OrderedDict([
    ("MT5", OrderedDict([
        ("path", ""),            # diretório com terminal64.exe no host
        ("windows_path", ""),    # mesmo diretório no formato C:\... (Wine)
        ("wine_prefix", ""),     # prefixo Wine (somente Linux)
    ])),
    ("BRIDGE", OrderedDict([
        ("enabled", "false"),    # true no Linux: MT5 acessado via RPyC dentro do Wine
        ("host", paths.DEFAULT_BRIDGE_HOST),
        ("port", str(paths.DEFAULT_BRIDGE_PORT)),
        ("wine_python", ""),     # C:\Python311\python.exe dentro do prefixo
    ])),
    ("DATABASE", OrderedDict([
        ("type", "sqlite"),
        ("path", "database/mt5_data.db"),
    ])),
    ("EXTRACTION", OrderedDict([
        ("chunk_days_m1", "30"),
        ("chunk_days_m5_m15", "90"),
        ("chunk_days_default", "365"),
    ])),
    ("FALLBACK", OrderedDict([
        ("external_source_m1_fallback_enabled", "False"),
        ("external_source_m1_type", "Dummy"),   # Dummy (teste) ou Csv
        ("csv_dir", ""),                         # pasta com <símbolo>.csv (tipo Csv)
    ])),
    ("APP", OrderedDict([
        ("session_start", "09:00"),  # início do pregão (relatório de qualidade: lacunas)
        ("session_end", "18:30"),    # fim do pregão
        ("time_basis", "broker"),    # broker = horário da corretora (como o MT5 devolve); utc = converte
        ("broker_utc_offset", "-3"), # fuso da corretora em horas (B3: -3), usado com time_basis = utc
    ])),
    ("STRATEGY", OrderedDict([       # backtest e validação (mt5x backtest/validate/report)
        ("initial_equity", "100000"),
        ("risk_per_trade", "0.01"),      # 1% do capital por operação
        ("commission_per_lot", "0"),     # por lote e por lado, na moeda da conta
        ("slippage_points", "0"),
        ("max_drawdown", "0.20"),        # limite do drawdown p95 do Monte Carlo (Filtro C)
        ("min_profit_factor", "1.3"),
        ("min_trades", "100"),
    ])),
    ("SCREENER", OrderedDict([       # triagem de ativos (mt5x screen)
        ("max_spread_atr", "0.10"),
        ("min_liquidity_percentile", "30"),
        ("max_bad_bars", "0.01"),
        ("min_years_daily", "3"),
        ("min_years_intraday", "1"),
        ("spike_multiple", "3"),
        ("max_spike_share", "0.02"),
        ("max_correlation", "0.7"),
        ("portfolio", ""),               # símbolos da carteira, separados por vírgula
        ("events_file", ""),             # CSV time,symbol,impact (opcional)
        ("event_window_hours", "4"),
    ])),
])

DEFAULT_SYMBOLS = OrderedDict([
    ("indices_futuros_b3", ["WIN$N", "WIN$", "IND$N"]),
    ("dolar_futuro_b3", ["WDO$N", "WDO$", "DOL$N"]),
    ("acoes_b3", ["PETR4", "VALE3", "ITUB4", "BBDC4", "BBAS3", "ABEV3", "B3SA3", "WEGE3"]),
    ("forex", ["EURUSD", "GBPUSD", "USDJPY", "USDBRL"]),
])


def load_config(path: Path) -> configparser.ConfigParser:
    cfg = _new_parser()
    if Path(path).exists():
        cfg.read(path, encoding="utf-8")
    return cfg


def _new_parser() -> configparser.ConfigParser:
    # interpolation=None: senhas/caminhos com "%" não quebram o parser
    cfg = configparser.ConfigParser(interpolation=None)
    cfg.optionxform = str  # preserva maiúsculas/minúsculas das chaves
    return cfg


def build_config(existing: Optional[configparser.ConfigParser] = None,
                 overrides: Optional[Dict[str, Dict[str, str]]] = None) -> configparser.ConfigParser:
    """
    Mescla: padrões < existente < overrides.
    Seções/chaves desconhecidas do arquivo existente são mantidas.
    """
    result = _new_parser()
    for section, keys in DEFAULTS.items():
        result[section] = dict(keys)
    if existing is not None:
        for section in existing.sections():
            if not result.has_section(section):
                result.add_section(section)
            for key, value in existing.items(section, raw=True):
                result[section][key] = value
    for section, keys in (overrides or {}).items():
        if not result.has_section(section):
            result.add_section(section)
        for key, value in keys.items():
            if value is not None:
                result[section][key] = str(value)
    return result


def atomic_write_text(path: Path, text: str, private: bool = False) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=path.name + ".", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)
        if private and os.name != "nt":
            os.chmod(tmp, 0o600)
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise


def write_config(path: Path, cfg: configparser.ConfigParser) -> None:
    import io
    buf = io.StringIO()
    buf.write("# Gerado por: python -m mt5_extracao.bootstrap configure\n")
    buf.write("# Pode ser editado manualmente; valores existentes são preservados ao reconfigurar.\n\n")
    cfg.write(buf)
    atomic_write_text(path, buf.getvalue())


def read_env_file(path: Path) -> "OrderedDict[str, str]":
    values: "OrderedDict[str, str]" = OrderedDict()
    path = Path(path)
    if not path.exists():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = _unescape(value[1:-1])
        values[key.strip()] = value
    return values


def _unescape(value: str) -> str:
    """Desfaz \\\\ e \\' (mesmo formato aceito pelo python-dotenv)."""
    out = []
    i = 0
    while i < len(value):
        ch = value[i]
        if ch == "\\" and i + 1 < len(value) and value[i + 1] in ("\\", "'", '"'):
            out.append(value[i + 1])
            i += 2
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def _quote_env(value: str) -> str:
    if value == "" or any(ch in value for ch in " #'\"=\\$"):
        escaped = value.replace("\\", "\\\\").replace("'", "\\'")
        return "'" + escaped + "'"
    return value


def write_env_credentials(path: Path, login: Optional[str], password: Optional[str],
                          server: Optional[str]) -> bool:
    """
    Grava/atualiza MT5_LOGIN, MT5_PASSWORD e MT5_SERVER no .env, preservando
    outras variáveis. Valores None não alteram a chave existente.
    Retorna True se algo foi escrito.
    """
    updates = {"MT5_LOGIN": login, "MT5_PASSWORD": password, "MT5_SERVER": server}
    updates = {k: v for k, v in updates.items() if v is not None}
    if not updates:
        return False
    current = read_env_file(path)
    current.update(updates)
    lines = ["# Credenciais do MetaTrader 5 - NÃO versione este arquivo"]
    lines += [f"{k}={_quote_env(v)}" for k, v in current.items()]
    atomic_write_text(path, "\n".join(lines) + "\n", private=True)
    return True


def ensure_symbols_file(path: Path) -> bool:
    """Cria default_symbols.json se não existir. Retorna True se criou."""
    path = Path(path)
    if path.exists():
        return False
    atomic_write_text(path, json.dumps(DEFAULT_SYMBOLS, indent=2, ensure_ascii=False) + "\n")
    return True


def ensure_work_dirs(root: Path = paths.PROJECT_ROOT) -> None:
    for name in paths.WORK_DIRS:
        (Path(root) / name).mkdir(parents=True, exist_ok=True)
