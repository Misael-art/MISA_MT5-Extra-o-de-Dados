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
import json
import logging
import re
import unicodedata
from datetime import datetime, timezone
from typing import Callable, Dict, Iterable, List, Optional

import pandas as pd
from sqlalchemy import inspect, text

log = logging.getLogger(__name__)

TABLE = "_economic_events"
REVISION_TABLE = "_economic_event_revisions"
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
    "first_observed_at": "TEXT",
    "source_received_at": "TEXT",
}
REVISION_COLUMNS = {
    "event_key": "TEXT NOT NULL",
    "revision_no": "INTEGER NOT NULL",
    "revision_hash": "TEXT NOT NULL",
    "event_time": "TIMESTAMP NOT NULL",
    "event_id": "TEXT",
    "name": "TEXT",
    "country": "TEXT",
    "currency": "TEXT",
    "symbol": "TEXT",
    "impact": "INTEGER",
    "actual": "REAL",
    "forecast": "REAL",
    "previous": "REAL",
    "override": "INTEGER",
    "source": "TEXT NOT NULL",
    "imported_at": "TEXT",
    "first_observed_at": "TEXT",
    "source_received_at": "TEXT",
    "observed_at": "TEXT",
}
_REVISION_HASH_FIELDS = ("event_time", "event_id", "name", "country", "currency", "symbol", "impact",
                         "actual", "forecast", "previous", "override", "source", "source_received_at")
MATCH_MINUTES = 1             # mesmo evento em duas fontes: horários a até 1 minuto


def impact_of(value) -> int:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return NONE
    return _IMPACT_WORDS.get(str(value).strip().lower(), NONE)


def availability_timestamp(value, field="source_received_at") -> Optional[str]:
    """Normaliza timestamp de disponibilidade para UTC; não aceita horário sem fuso."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    try:
        if pd.isna(value):
            return None
        stamp = pd.Timestamp(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{field} precisa conter uma data válida com fuso horário.") from exc
    if pd.isna(stamp) or stamp.tzinfo is None:
        raise ValueError(f"{field} precisa conter um fuso horário, por exemplo 2026-01-09T10:30:00-03:00.")
    return stamp.tz_convert("UTC").isoformat()


def _hash_value(value):
    if value is None:
        return None
    if isinstance(value, (pd.Timestamp, datetime)):
        return pd.Timestamp(value).isoformat()
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        value = value.item()
    return value


def _revision_hash(row: dict) -> str:
    payload = {}
    for name in _REVISION_HASH_FIELDS:
        value = row.get(name)
        if name == "event_time" and value is not None and pd.notna(value):
            value = pd.Timestamp(value).isoformat()
        payload[name] = _hash_value(value)
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _insert_revision(conn, row, revision_no, observed_at, fingerprint=None):
    values = {name: row.get(name) for name in REVISION_COLUMNS}
    values["revision_hash"] = fingerprint or _revision_hash(row)
    values["revision_no"] = int(revision_no)
    values["observed_at"] = observed_at
    names = list(REVISION_COLUMNS)
    conn.execute(text(f"INSERT INTO {REVISION_TABLE} ({', '.join(names)}) "
                      f"VALUES ({', '.join(':' + name for name in names)})"), values)


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
        revision_cols = ", ".join(f"{name} {kind}" for name, kind in REVISION_COLUMNS.items())
        conn.execute(text(f"CREATE TABLE IF NOT EXISTS {REVISION_TABLE} ({revision_cols}, "
                          "PRIMARY KEY (event_key, revision_no))"))
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx{REVISION_TABLE}_available "
                          f"ON {REVISION_TABLE} (event_key, source_received_at, observed_at)"))


def _key(source: str, row: dict) -> str:
    ident = row.get("event_id") or f"{row['event_time']}|{row.get('symbol') or ''}|{row.get('currency') or ''}|" \
                                    f"{row.get('name') or ''}"
    return f"{source}:" + hashlib.sha1(str(ident).encode("utf-8")).hexdigest()[:16]


def save(db, source: str, rows: Iterable[dict], *, observed_at=None) -> int:
    """Grava eventos e conserva revisões observadas; reimportação igual é idempotente."""
    rows = list(rows)
    ensure_table(db)
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    observed_stamp = availability_timestamp(observed_at or datetime.now(timezone.utc), "observed_at")
    clean = []
    for row in rows:
        r = {name: row.get(name) for name in COLUMNS}
        r["event_time"] = pd.Timestamp(r["event_time"]).to_pydatetime()
        r["source"], r["imported_at"] = source, stamp
        # first_observed_at é local e só pode ser preenchido ao receber uma versão nova.
        r["first_observed_at"] = None
        r["source_received_at"] = availability_timestamp(row.get("source_received_at"))
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
        for row in clean:
            current = conn.execute(text(f"SELECT * FROM {TABLE} WHERE event_key=:key"),
                                   {"key": row["event_key"]}).mappings().first()
            current = dict(current) if current is not None else None
            if current is not None:
                row["first_observed_at"] = current.get("first_observed_at")
                compare_with_current_time = {**row, "source_received_at": current.get("source_received_at")}
                same_event_data = _revision_hash(compare_with_current_time) == _revision_hash(current)
                if same_event_data and row["source_received_at"] is None:
                    # Uma reimportação sem a coluna opcional não apaga uma disponibilidade já recebida.
                    row["source_received_at"] = current.get("source_received_at")
            else:
                row["first_observed_at"] = observed_stamp

            history = conn.execute(text(f"SELECT revision_no, revision_hash FROM {REVISION_TABLE} "
                                        "WHERE event_key=:key ORDER BY revision_no DESC LIMIT 1"),
                                  {"key": row["event_key"]}).mappings().first()
            if current is not None and history is None:
                # Versões anteriores à trilha recebem revisão sem disponibilidade presumida.
                old = dict(current)
                old.setdefault("first_observed_at", None)
                old.setdefault("source_received_at", None)
                old_fingerprint = _revision_hash(old)
                old_available = availability_timestamp(old.get("first_observed_at"), "first_observed_at")
                _insert_revision(conn, old, 1, old_available, old_fingerprint)
                history = {"revision_no": 1, "revision_hash": old_fingerprint}

            fingerprint = _revision_hash(row)
            if history is None or history["revision_hash"] != fingerprint:
                revision_no = int(history["revision_no"]) + 1 if history is not None else 1
                _insert_revision(conn, row, revision_no, observed_stamp, fingerprint)

            conn.execute(text(f"DELETE FROM {TABLE} WHERE event_key=:key"), {"key": row["event_key"]})
            conn.execute(text(f"INSERT INTO {TABLE} ({', '.join(names)}) "
                              f"VALUES ({', '.join(':' + name for name in names)})"), row)
    return len(clean)


def load(db, start=None, end=None, source: Optional[str] = None) -> pd.DataFrame:
    if db is None or getattr(db, "engine", None) is None or not inspect(db.engine).has_table(TABLE):
        return pd.DataFrame(columns=list(COLUMNS))
    revision_exists = inspect(db.engine).has_table(REVISION_TABLE)
    prefix = "e." if revision_exists else ""
    where, params = [], {}
    if start is not None:
        where.append(prefix + "event_time >= :s")
        params["s"] = pd.Timestamp(start).to_pydatetime()
    if end is not None:
        where.append(prefix + "event_time < :e")
        params["e"] = pd.Timestamp(end).to_pydatetime()
    if source:
        where.append(prefix + "source = :src")
        params["src"] = source
    if revision_exists:
        query = (f"SELECT e.*, r.revision_no, r.observed_at, "
                 "COALESCE(r.source_received_at, r.observed_at) AS available_at "
                 f"FROM {TABLE} e LEFT JOIN {REVISION_TABLE} r ON r.event_key=e.event_key "
                 f"AND r.revision_no=(SELECT MAX(r2.revision_no) FROM {REVISION_TABLE} r2 "
                 "WHERE r2.event_key=e.event_key)" +
                 (" WHERE " + " AND ".join(where) if where else "") + " ORDER BY e.event_time")
    else:
        query = f"SELECT * FROM {TABLE}" + (" WHERE " + " AND ".join(where) if where else "") + \
            " ORDER BY event_time"
    with db.engine.connect() as conn:
        frame = pd.read_sql(text(query), conn, params=params)
    frame["event_time"] = pd.to_datetime(frame["event_time"])
    if "imported_at" in frame:
        frame["imported_at"] = pd.to_datetime(frame["imported_at"], errors="coerce")
    if "available_at" in frame:
        frame["available_at"] = pd.to_datetime(frame["available_at"], errors="coerce", utc=True)
    for name in ("first_observed_at", "source_received_at", "observed_at"):
        if name in frame:
            frame[name] = pd.to_datetime(frame[name], errors="coerce", utc=True)
    return frame


def load_revisions(db, event_key=None, source: Optional[str] = None) -> pd.DataFrame:
    """Lista snapshots por fonte/evento, inclusive versões sem disponibilidade conhecida."""
    columns = list(REVISION_COLUMNS) + ["available_at"]
    if db is None or getattr(db, "engine", None) is None or not inspect(db.engine).has_table(REVISION_TABLE):
        return pd.DataFrame(columns=columns)
    where, params = [], {}
    if event_key is not None:
        where.append("event_key = :event_key")
        params["event_key"] = str(event_key)
    if source is not None:
        where.append("source = :source")
        params["source"] = str(source)
    sql = f"SELECT *, COALESCE(source_received_at, observed_at) AS available_at FROM {REVISION_TABLE}"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY event_key, revision_no"
    with db.engine.connect() as conn:
        frame = pd.read_sql(text(sql), conn, params=params)
    if not frame.empty:
        frame["event_time"] = pd.to_datetime(frame["event_time"])
        for name in ("first_observed_at", "source_received_at", "observed_at", "available_at"):
            frame[name] = pd.to_datetime(frame[name], errors="coerce", utc=True)
    return frame


def load_asof(db, decision_time, start=None, end=None, source: Optional[str] = None) -> pd.DataFrame:
    """Devolve a última versão disponível até decision_time; legado sem horário fica desconhecido."""
    columns = list(REVISION_COLUMNS) + ["available_at"]
    decision = availability_timestamp(decision_time, "decision_time")
    if decision is None:
        raise ValueError("decision_time precisa conter um horário com fuso explícito.")
    if db is None or getattr(db, "engine", None) is None or not inspect(db.engine).has_table(REVISION_TABLE):
        return pd.DataFrame(columns=columns)
    where = ["COALESCE(r.source_received_at, r.observed_at) IS NOT NULL",
             "COALESCE(r.source_received_at, r.observed_at) <= :decision"]
    params = {"decision": decision}
    result_filters = []
    if start is not None:
        result_filters.append("event_time >= :start")
        params["start"] = pd.Timestamp(start).to_pydatetime()
    if end is not None:
        result_filters.append("event_time < :end")
        params["end"] = pd.Timestamp(end).to_pydatetime()
    if source is not None:
        where.append("r.source = :source")
        params["source"] = str(source)
    sql = ("SELECT * FROM (SELECT r.*, COALESCE(r.source_received_at, r.observed_at) AS available_at, "
           f"ROW_NUMBER() OVER (PARTITION BY r.event_key ORDER BY COALESCE(r.source_received_at, r.observed_at) DESC, "
           f"r.revision_no DESC) AS _asof_rank FROM {REVISION_TABLE} r WHERE " + " AND ".join(where) +
           ") ranked WHERE _asof_rank=1" +
           (" AND " + " AND ".join(result_filters) if result_filters else "") + " ORDER BY event_time")
    with db.engine.connect() as conn:
        frame = pd.read_sql(text(sql), conn, params=params)
    if not frame.empty:
        frame = frame.drop(columns=["_asof_rank"])
        frame["event_time"] = pd.to_datetime(frame["event_time"])
        for name in ("first_observed_at", "source_received_at", "observed_at", "available_at"):
            frame[name] = pd.to_datetime(frame[name], errors="coerce", utc=True)
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
                     "source_received_at": availability_timestamp(rec.get("source_received_at")),
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
    return _merge_rows(load(db, start, end))


def merged_asof(db, decision_time, start=None, end=None) -> pd.DataFrame:
    """Une somente versões comprovadamente disponíveis até decision_time."""
    return _merge_rows(load_asof(db, decision_time, start, end))


def _merge_rows(raw) -> pd.DataFrame:
    cols = ["event_time", "name", "country", "currency", "symbol", "impact", "actual", "forecast", "previous",
            "surprise", "surprise_available_at", "sources"]
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
        pick = lambda name: next((g for g in [main] + group if pd.notna(g.get(name))), None)  # noqa: E731
        actual_record, forecast_record = pick("actual"), pick("forecast")
        actual = actual_record.get("actual") if actual_record is not None else None
        forecast = forecast_record.get("forecast") if forecast_record is not None else None
        surprise_available_at = None
        if actual is not None and forecast is not None and actual_record is not None and forecast_record is not None:
            def version_available(record):
                for name in ("available_at", "source_received_at", "observed_at", "first_observed_at"):
                    value = record.get(name)
                    if value is not None and pd.notna(value):
                        return pd.Timestamp(value)
                return None

            times = [version_available(actual_record), version_available(forecast_record)]
            if all(value is not None for value in times):
                surprise_available_at = max(times)
        pick_value = lambda name: (next((g[name] for g in [main] + group if pd.notna(g.get(name))), None))  # noqa: E731
        out.append({"event_time": main["event_time"], "name": pick_value("name"), "country": pick_value("country"),
                    "currency": pick_value("currency"), "symbol": pick_value("symbol") or "", "impact": int(impact),
                    "actual": actual, "forecast": forecast, "previous": pick_value("previous"),
                    "surprise": actual - forecast if actual is not None and forecast is not None else None,
                    "surprise_available_at": surprise_available_at,
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
