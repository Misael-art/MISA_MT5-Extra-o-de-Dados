"""
Calendário econômico de várias fontes (T16.3a), unido e enriquecido numa tabela só: `_economic_events` (aditiva).

Fontes (registro `SOURCES`, aberto para novas):
- "csv": o `[SCREENER] events_file` de sempre (`time,symbol,impact`, separado por vírgula), com colunas opcionais
  `name,country,currency,actual,forecast,previous,override`. `symbol` pode ser um ativo, uma moeda ou `*`.
- "mt5": o arquivo exportado pelo script MT5X_CalendarExport.mq5 (`calendar/mt5_export.py`).

    import_source(db, config, "csv")                 # ou "mt5"
    events = merged(db, start, end)                  # uma linha por evento, com as fontes e a surpresa
    events_for(events, "WIN$N", specs, mapping)      # eventos que valem para o ativo

Impacto: 0 nenhum, 1 baixo, 2 médio, 3 alto (a escala do MT5).
"""
import hashlib
import logging
import re
import unicodedata
from datetime import datetime
from typing import Callable, Dict, Iterable, List, Optional

import pandas as pd
from sqlalchemy import inspect, text

log = logging.getLogger(__name__)

TABLE = "_economic_events"
NONE, LOW, MEDIUM, HIGH = 0, 1, 2, 3
IMPACT_LABELS = {NONE: "nenhum", LOW: "baixo", MEDIUM: "médio", HIGH: "alto"}
_IMPACT_WORDS = {"alto": HIGH, "alta": HIGH, "high": HIGH, "3": HIGH, "medio": MEDIUM, "médio": MEDIUM,
                 "media": MEDIUM, "média": MEDIUM, "medium": MEDIUM, "moderate": MEDIUM, "2": MEDIUM,
                 "baixo": LOW, "baixa": LOW, "low": LOW, "1": LOW, "nenhum": NONE, "none": NONE, "0": NONE}
COLUMNS = {
    "event_key": "TEXT PRIMARY KEY",
    "event_time": "TIMESTAMP NOT NULL",
    "event_id": "TEXT",
    "name": "TEXT",
    "country": "TEXT",
    "currency": "TEXT",
    "symbol": "TEXT",          # ativo, moeda ou '*' (CSV); vazio no MT5 (liga pela moeda)
    "impact": "INTEGER",
    "actual": "REAL",
    "forecast": "REAL",
    "previous": "REAL",
    "override": "INTEGER",     # 1 = impacto marcado à mão (vence o das outras fontes)
    "source": "TEXT NOT NULL",
    "imported_at": "TEXT",
}
MATCH_MINUTES = 1             # mesmo evento em duas fontes: horários a até 1 minuto


def impact_of(value) -> int:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return NONE
    return _IMPACT_WORDS.get(str(value).strip().lower(), NONE)


def ensure_table(db) -> None:
    with db.engine.begin() as conn:
        cols = ", ".join(f"{name} {kind}" for name, kind in COLUMNS.items())
        conn.execute(text(f"CREATE TABLE IF NOT EXISTS {TABLE} ({cols})"))
        existing = {c["name"] for c in inspect(conn).get_columns(TABLE)}
        for name, kind in COLUMNS.items():
            if name not in existing:
                conn.execute(text(f"ALTER TABLE {TABLE} ADD COLUMN {name} "
                                  f"{kind.replace(' NOT NULL', '').replace(' PRIMARY KEY', '')}"))
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx{TABLE}_time ON {TABLE} (event_time)"))


def _key(source: str, row: dict) -> str:
    ident = row.get("event_id") or f"{row['event_time']}|{row.get('symbol') or ''}|{row.get('currency') or ''}|" \
                                    f"{row.get('name') or ''}"
    return f"{source}:" + hashlib.sha1(str(ident).encode("utf-8")).hexdigest()[:16]


def save(db, source: str, rows: Iterable[dict]) -> int:
    """Grava (ou atualiza) os eventos de uma fonte. Reimportar não duplica."""
    rows = list(rows)
    ensure_table(db)
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    clean = []
    for row in rows:
        r = {name: row.get(name) for name in COLUMNS}
        r["event_time"] = pd.Timestamp(r["event_time"]).to_pydatetime()
        r["source"], r["imported_at"] = source, stamp
        r["event_key"] = _key(source, r)
        for name in ("actual", "forecast", "previous"):
            value = r.get(name)
            r[name] = None if value is None or value == "" or pd.isna(value) else float(value)
        r["impact"] = int(r["impact"]) if r.get("impact") is not None else NONE
        r["override"] = 1 if r.get("override") else 0
        clean.append(r)
    if not clean:
        return 0
    names = list(COLUMNS)
    with db.engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {TABLE} WHERE event_key = :k"), [{"k": r["event_key"]} for r in clean])
        conn.execute(text(f"INSERT INTO {TABLE} ({', '.join(names)}) VALUES ({', '.join(':' + n for n in names)})"),
                     clean)
    return len(clean)


def load(db, start=None, end=None, source: Optional[str] = None) -> pd.DataFrame:
    if db is None or getattr(db, "engine", None) is None or not inspect(db.engine).has_table(TABLE):
        return pd.DataFrame(columns=list(COLUMNS))
    where, params = [], {}
    if start is not None:
        where.append("event_time >= :s")
        params["s"] = pd.Timestamp(start).to_pydatetime()
    if end is not None:
        where.append("event_time < :e")
        params["e"] = pd.Timestamp(end).to_pydatetime()
    if source:
        where.append("source = :src")
        params["src"] = source
    with db.engine.connect() as conn:
        frame = pd.read_sql(text(f"SELECT * FROM {TABLE}" + (" WHERE " + " AND ".join(where) if where else "") +
                                 " ORDER BY event_time"), conn, params=params)
    frame["event_time"] = pd.to_datetime(frame["event_time"])
    return frame


# --- fontes ------------------------------------------------------------------------------

def read_csv_file(path) -> List[dict]:
    """O events_file de sempre (vírgula), aceitando as colunas novas. Erro claro se faltar time/symbol/impact."""
    frame = pd.read_csv(path)
    missing = [c for c in ("time", "symbol", "impact") if c not in frame.columns]
    if missing:
        raise ValueError(f"Arquivo de eventos {path} sem as colunas: {', '.join(missing)} (esperado: time,symbol,impact)")
    rows = []
    for rec in frame.to_dict("records"):
        symbol = str(rec.get("symbol") or "").strip().upper()
        currency = str(rec.get("currency") or "").strip().upper() or None
        if currency is None and re.fullmatch(r"[A-Z]{3}", symbol or ""):
            currency = symbol                      # "USD" na coluna symbol: é uma moeda
        rows.append({"event_time": pd.Timestamp(rec["time"]), "symbol": symbol or "*",
                     "impact": impact_of(rec.get("impact")), "name": rec.get("name") if pd.notna(rec.get("name"))
                     else None, "country": rec.get("country") if pd.notna(rec.get("country")) else None,
                     "currency": currency, "actual": rec.get("actual"), "forecast": rec.get("forecast"),
                     "previous": rec.get("previous"),
                     "override": str(rec.get("override", "")).strip().lower() in ("1", "true", "sim", "yes")})
    return rows


def _import_csv(db, config, path=None) -> int:
    path = path or (config.get("SCREENER", "events_file", fallback="").strip() if config is not None else "")
    if not path:
        raise ValueError("Nenhum arquivo de eventos: informe o caminho ou preencha [SCREENER] events_file.")
    return save(db, "csv", read_csv_file(path))


def _import_mt5(db, config, path=None) -> int:
    from mt5_extracao.trade_analytics.calendar import mt5_export
    path = path or mt5_export.find_export(config)
    if path is None:
        raise FileNotFoundError("Nenhuma exportação do calendário do MT5 encontrada em Common\\Files. Rode "
                                "`mt5x trades calendar export` (ou o script MT5X_CalendarExport no MT5).")
    return save(db, "mt5", mt5_export.read_export(path))


SOURCES: Dict[str, Callable] = {"csv": _import_csv, "mt5": _import_mt5}


def import_source(db, config, name: str, path=None) -> int:
    if name not in SOURCES:
        raise ValueError(f"Fonte desconhecida: {name} (use {', '.join(SOURCES)})")
    count = SOURCES[name](db, config, path)
    log.info(f"Calendário: {count} evento(s) importado(s) da fonte {name}.")
    return count


# --- união e enriquecimento ---------------------------------------------------------------

def _words(name) -> set:
    if not name or (isinstance(name, float) and pd.isna(name)):
        return set()
    plain = unicodedata.normalize("NFKD", str(name).lower()).encode("ascii", "ignore").decode()
    return {w for w in re.findall(r"[a-z0-9]+", plain) if len(w) > 2}


def _same_event(a: dict, b: dict) -> bool:
    if abs((a["event_time"] - b["event_time"]).total_seconds()) > MATCH_MINUTES * 60:
        return False
    keys_a = {a.get("currency"), a.get("symbol")} - {None, "", "*"}
    keys_b = {b.get("currency"), b.get("symbol")} - {None, "", "*"}
    if keys_a and keys_b and not keys_a & keys_b:
        return False
    wa, wb = _words(a.get("name")), _words(b.get("name"))
    if wa and wb:
        return len(wa & wb) / len(wa | wb) >= 0.5
    return True        # uma das fontes não tem nome (CSV antigo): mesmo horário e mesma moeda bastam


def merged(db, start=None, end=None) -> pd.DataFrame:
    """
    Uma linha por evento: as fontes unidas (coluna `sources`), o impacto escolhido (marcado à mão > MT5 > maior) e a
    surpresa (real − previsto, quando os dois existem).
    """
    raw = load(db, start, end)
    cols = ["event_time", "name", "country", "currency", "symbol", "impact", "actual", "forecast", "previous",
            "surprise", "sources"]
    if raw.empty:
        return pd.DataFrame(columns=cols)
    groups: List[List[dict]] = []
    for rec in raw.sort_values(["event_time", "source"]).to_dict("records"):
        placed = False
        for group in reversed(groups[-50:]):      # só os grupos recentes podem casar (ordenado por horário)
            if (rec["event_time"] - group[0]["event_time"]).total_seconds() > MATCH_MINUTES * 60:
                break
            if rec["source"] not in {g["source"] for g in group} and _same_event(group[0], rec):
                group.append(rec)
                placed = True
                break
        if not placed:
            groups.append([rec])
    out = []
    for group in groups:
        by = {g["source"]: g for g in group}
        manual = next((g for g in group if g.get("override")), None)
        main = by.get("mt5") or group[0]
        impact = manual["impact"] if manual else (main["impact"] if "mt5" in by else max(g["impact"] for g in group))
        pick = lambda name: next((g[name] for g in [main] + group if g.get(name) is not None and  # noqa: E731
                                  not (isinstance(g[name], float) and pd.isna(g[name]))), None)
        actual, forecast = pick("actual"), pick("forecast")
        out.append({"event_time": main["event_time"], "name": pick("name"), "country": pick("country"),
                    "currency": pick("currency"), "symbol": pick("symbol") or "", "impact": int(impact),
                    "actual": actual, "forecast": forecast, "previous": pick("previous"),
                    "surprise": actual - forecast if actual is not None and forecast is not None else None,
                    "sources": ",".join(sorted(by))})
    return pd.DataFrame(out, columns=cols)


def parse_mapping(raw: str) -> Dict[str, List[str]]:
    """`[TRADE_ANALYTICS] event_symbols`: 'BRL: WIN$N WDO$N; USD: WDO$N' -> {'BRL': ['WIN$N', 'WDO$N'], ...}."""
    out: Dict[str, List[str]] = {}
    for part in (raw or "").split(";"):
        if ":" not in part:
            continue
        key, symbols = part.split(":", 1)
        out[key.strip().upper()] = [s.strip().upper() for s in re.split(r"[,\s]+", symbols) if s.strip()]
    return out


def keys_for_symbol(symbol: str, profit_currency: Optional[str], mapping: Dict[str, List[str]]) -> set:
    """O que liga um evento ao ativo: o próprio ativo, a moeda de lucro, as moedas no nome (EURUSD) e o mapeamento."""
    symbol = symbol.upper()
    keys = {symbol, "*"}            # "*" casa só com a coluna symbol (CSV), nunca com moeda
    if profit_currency:
        keys.add(profit_currency.upper())
    keys |= {symbol[i:i + 3] for i in (0, 3)} if re.fullmatch(r"[A-Z]{6}", symbol) else set()
    keys |= {cur for cur, symbols in mapping.items() if symbol in symbols}
    return keys


def events_for(events: pd.DataFrame, symbol: str, profit_currency: Optional[str] = None,
               mapping: Optional[Dict[str, List[str]]] = None) -> pd.DataFrame:
    """Eventos (de `merged`) que valem para o ativo."""
    if events.empty:
        return events
    keys = keys_for_symbol(symbol, profit_currency, mapping or {})
    sym = events["symbol"].fillna("").str.upper()     # "*" só quando a fonte disse "vale para todos"
    cur = events["currency"].fillna("").str.upper()
    # CSV antigo: 'symbol' é trecho do nome do ativo (como o asset_screener sempre leu)
    partial = sym.apply(lambda s: s not in ("", "*") and s in symbol.upper())
    return events[sym.isin(keys) | cur.isin(keys) | partial]


def status(db) -> dict:
    """Cobertura por fonte: {fonte: {"events": n, "first": data, "last": data}}."""
    raw = load(db)
    out = {}
    for source, part in raw.groupby("source"):
        out[source] = {"events": int(len(part)), "first": part["event_time"].min(), "last": part["event_time"].max(),
                       "high_impact": int((part["impact"] == HIGH).sum())}
    return out
