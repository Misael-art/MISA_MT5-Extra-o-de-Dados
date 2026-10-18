"""Fatos de instrumentos: vigência e conhecimento independentes, sem inferir eventos."""
import hashlib
import json
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import inspect, text

TABLE = "_instrument_facts"
KINDS = frozenset({"specification", "adjustment", "rollover", "universe", "series"})
_COLUMNS = ["revision_id", "symbol", "kind", "event_key", "valid_from", "valid_to", "known_at",
            "source", "payload_json", "recorded_at"]


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def utc(value):
    """Fatos externos precisam declarar fuso; nenhuma suposição sobre o relógio do broker."""
    stamp = pd.Timestamp(value)
    if pd.isna(stamp) or stamp.tzinfo is None:
        raise ValueError("Informe um instante com fuso explícito para a vigência/conhecimento do instrumento.")
    return stamp.tz_convert("UTC").isoformat()


def broker_utc(value, offset):
    stamp = pd.Timestamp(value)
    return stamp.tz_convert("UTC") if stamp.tzinfo is not None else (
        stamp.tz_localize("UTC") - pd.Timedelta(hours=float(offset)))


def _label(value, name):
    if not isinstance(value, str) or not value.strip() or len(value) > 256 or any(
            c in value for c in ("/", "\\", "@", "\n", "\r")):
        raise ValueError(f"{name} precisa ser um rótulo sem caminhos, URLs ou credenciais.")
    return value


def _payload(kind, payload, *, historical=False):
    data = dict(payload)
    if kind == "specification":
        from mt5_extracao.database_manager import DatabaseManager
        allowed = set(DatabaseManager.SPEC_FIELDS) | {"spread_profile"}
        if not data or set(data) - allowed:
            raise ValueError("Especificação histórica aceita somente propriedades de contrato conhecidas.")
        if historical and not set(DatabaseManager.SPEC_FIELDS) <= set(data):
            raise ValueError("Especificação histórica incompleta; informe propriedades de preço e lote.")
        for key in set(data) - {"currency_profit", "spread_profile"}:
            if data[key] is not None and (isinstance(data[key], bool) or
                                         not isinstance(data[key], (int, float))):
                raise ValueError("Propriedade de contrato numérica inválida.")
        for key in ("point", "tick_value", "volume_min", "volume_step", "volume_max") if historical else ():
            if data[key] is None or data[key] <= 0:
                raise ValueError("Preço/tick/lote histórico precisa ser positivo.")
    else:
        allowed = {
            "adjustment": {"method", "factor", "offset", "reason"},
            "rollover": {"from_contract", "to_contract", "method", "factor", "offset"},
            "universe": {"member", "universe"},
            "series": {"role", "execution_symbol", "adjustment_basis"},
        }[kind]
        if set(data) - allowed:
            raise ValueError("Fato contém campos desconhecidos; informe apenas o evento documentado.")
        if kind in ("adjustment", "rollover"):
            if data.get("method") not in ("none", "ratio", "difference"):
                raise ValueError("Informe o método de transformação: none, ratio ou difference.")
            if data["method"] == "ratio" and (isinstance(data.get("factor"), bool) or
                    not isinstance(data.get("factor"), (int, float)) or data["factor"] <= 0):
                raise ValueError("Ajuste por razão exige fator positivo documentado.")
            if data["method"] == "difference" and (isinstance(data.get("offset"), bool) or
                    not isinstance(data.get("offset"), (int, float))):
                raise ValueError("Ajuste por diferença exige deslocamento documentado.")
            forbidden = {"factor", "offset"} if data["method"] == "none" else {
                "offset" if data["method"] == "ratio" else "factor"}
            if set(data) & forbidden:
                raise ValueError("Fator/deslocamento contradiz o método declarado; concilie a fonte.")
        if kind == "rollover":
            for key in ("from_contract", "to_contract"):
                _label(data.get(key), key)
            if data["from_contract"] == data["to_contract"]:
                raise ValueError("Rolagem precisa distinguir os dois contratos.")
        if kind == "universe":
            if type(data.get("member")) is not bool:
                raise ValueError("Participação no universo precisa ser true ou false, sem inferir pela base atual.")
            _label(data.get("universe"), "Universo")
        if kind == "series":
            if data.get("role") not in ("continuous_study", "executable_contract"):
                raise ValueError("Separe série contínua de estudo e contrato executável.")
            if data["role"] == "executable_contract":
                _label(data.get("execution_symbol"), "Contrato executável")
    _json(data)  # Recusar NaN/Infinity e tipos não serializáveis antes de escrever.
    return data


def ensure_schema(connection):
    connection.execute(text(f"CREATE TABLE IF NOT EXISTS {TABLE} (revision_id TEXT PRIMARY KEY, "
                            "symbol TEXT NOT NULL, kind TEXT NOT NULL, event_key TEXT NOT NULL, "
                            "valid_from TEXT, valid_to TEXT, known_at TEXT NOT NULL, source TEXT NOT NULL, "
                            "payload_json TEXT NOT NULL, recorded_at TEXT NOT NULL)"))


def record(db, symbol, kind, payload, *, source, known_at, valid_from=None, valid_to=None,
           event_key=None, connection=None):
    """Acrescenta fato declarado. Sem valid_from é observação atual, nunca prova histórica."""
    _label(symbol, "Símbolo")
    _label(source, "Fonte")
    if kind not in KINDS:
        raise ValueError("Tipo de fato do instrumento desconhecido.")
    begin = utc(valid_from) if valid_from is not None else None
    end = utc(valid_to) if valid_to is not None else None
    known = utc(known_at)
    if end is not None and (begin is None or pd.Timestamp(end) <= pd.Timestamp(begin)):
        raise ValueError("Fim da vigência precisa ser posterior ao início.")
    data = _payload(kind, payload, historical=begin is not None)
    default_key = f"{data['universe']}:{begin}" if kind == "universe" else begin or "current_observation"
    key = _label(event_key or default_key, "Chave do evento")
    content = dict(symbol=symbol, kind=kind, event_key=key, valid_from=begin, valid_to=end,
                   known_at=known, source=source, payload_json=_json(data))
    revision = hashlib.sha256(_json(content).encode("utf-8")).hexdigest()
    if connection is None:
        with db.engine.begin() as conn:
            return record(db, symbol, kind, data, source=source, known_at=known, valid_from=begin,
                          valid_to=end, event_key=key, connection=conn)
    ensure_schema(connection)
    connection.execute(text(f"INSERT INTO {TABLE} ({','.join(_COLUMNS)}) VALUES "
                            "(:revision_id,:symbol,:kind,:event_key,:valid_from,:valid_to,:known_at,"
                            ":source,:payload_json,:recorded_at) ON CONFLICT(revision_id) DO NOTHING"),
                       dict(content, revision_id=revision, recorded_at=datetime.now(timezone.utc).isoformat()))
    actual = connection.execute(text(f"SELECT * FROM {TABLE} WHERE revision_id=:r"),
                                {"r": revision}).mappings().one()
    if any(actual[key] != value for key, value in content.items()):
        raise ValueError("Revisão do instrumento sem integridade; restaure o registro original.")
    return revision


def history(db, symbol=None, *, connection=None):
    if connection is None:
        with db.consistent_read() as conn:
            return history(db, symbol, connection=conn)
    if not inspect(connection).has_table(TABLE):
        return pd.DataFrame(columns=_COLUMNS)
    query = f"SELECT {','.join(_COLUMNS)} FROM {TABLE}"
    if symbol is not None:
        query += " WHERE symbol=:symbol"
    return pd.read_sql(text(query + " ORDER BY symbol,kind,event_key,known_at,revision_id"),
                       connection, params={"symbol": symbol})


def select(frame, kind, at, *, known_at=None):
    """Última revisão conhecida de cada evento; intervalo [início,fim), sem lookahead."""
    instant = pd.Timestamp(utc(at))
    cutoff = pd.Timestamp(utc(known_at if known_at is not None else at))
    if frame.empty:
        return frame.copy()
    rows = frame[(frame.kind == kind) & frame.valid_from.notna() &
                 (pd.to_datetime(frame.known_at, utc=True, format="ISO8601") <= cutoff)].copy()
    # Uma correção pode invalidar/restringir o intervalo anterior: resolver revisão antes da vigência.
    keys = ["symbol", "kind", "event_key"]
    latest = rows.groupby(keys)["known_at"].transform("max")
    rows = rows[rows.known_at == latest]
    if rows.duplicated(keys, keep=False).any():
        raise ValueError("Revisões do instrumento conflitantes no mesmo instante de conhecimento; concilie a fonte.")
    rows = rows[rows.valid_from.notna()]
    begin = pd.to_datetime(rows.valid_from, utc=True, format="ISO8601")
    end = pd.to_datetime(rows.valid_to, utc=True, format="ISO8601")
    return rows[(begin <= instant) & (end.isna() | (instant < end))].reset_index(drop=True)


def specification(frame, at, *, known_at=None):
    rows = select(frame, "specification", at, known_at=known_at)
    if rows.empty:
        return None
    rows = rows.sort_values("valid_from")
    latest = rows[rows.valid_from == rows.valid_from.iloc[-1]]
    if len(latest) != 1:
        raise ValueError("Especificações históricas conflitantes; concilie fontes antes de simular.")
    row = latest.iloc[0].to_dict()
    row["payload"] = json.loads(row["payload_json"])
    return row


def universe_at(db, universe, at, *, known_at=None):
    """Inclui descontinuados quando vigentes no instante pedido; não usa symbols_get atual."""
    frame = history(db)
    rows = select(frame, "universe", at, known_at=known_at)
    members = {}
    revisions = []
    candidates = []
    for _, row in rows.iterrows():
        payload = json.loads(row.payload_json)
        if payload["universe"] == universe:
            candidates.append((row, payload["member"]))
    for symbol in sorted({row.symbol for row, _ in candidates}):
        own = [(row, member) for row, member in candidates if row.symbol == symbol]
        begin = max(row.valid_from for row, _ in own)
        latest = [(row, member) for row, member in own if row.valid_from == begin]
        if len({member for _, member in latest}) > 1:
            raise ValueError("Declarações de universo conflitantes; concilie as fontes.")
        members[symbol] = latest[0][1]
        revisions.extend(row.revision_id for row, _ in latest)
    has_source = any(json.loads(row.payload_json).get("universe") == universe
                     for _, row in frame[frame.kind == "universe"].iterrows())
    return {"status": "declared" if candidates else "unknown_at_instant" if has_source else "source_unavailable",
            "members": sorted(symbol for symbol, member in members.items() if member),
            "revision_ids": sorted(revisions), "knowledge_cutoff": utc(known_at if known_at is not None else at)}


def context(frame, bars, offset):
    """Qualifica o período por conhecimento em cada barra, sem aplicar transformações implícitas."""
    refs, payloads, missing = [], set(), 0
    times = pd.DatetimeIndex(pd.to_datetime(bars.time))
    times = times.tz_localize("UTC") - pd.Timedelta(hours=float(offset)) if times.tz is None else times.tz_convert("UTC")
    # Uma seleção só muda nas fronteiras de vigência/conhecimento, não em cada barra.
    boundaries = pd.DatetimeIndex(pd.to_datetime(frame[["valid_from", "valid_to", "known_at"]].stack(), utc=True, format="ISO8601")).unique().sort_values()
    slots = pd.Series(boundaries.searchsorted(times, side="right"))
    samples = [(times[group.index[0]], len(group)) for _, group in slots.groupby(slots)]
    roles = set()
    for instant, count in samples:
        fact = specification(frame, instant)
        if fact is None:
            missing += count
        else:
            refs.append(fact["revision_id"])
            payloads.add(fact["payload_json"])
    status = "unknown" if missing == len(bars) else "partial" if missing else "varying" if len(payloads) > 1 else "known"
    kinds = set(frame.kind) if not frame.empty else set()
    for instant, _ in samples:
        for _, row in select(frame, "series", instant).iterrows():
            roles.add(json.loads(row.payload_json)["role"])
    return {"version": 1, "specification_status": status, "specification_revisions": sorted(set(refs)),
            "current_observation_revisions": sorted(frame.loc[
                (frame.kind == "specification") & frame.valid_from.isna(), "revision_id"].tolist()),
            "missing_specification_bars": missing, "specification_value_sets": len(payloads),
            "knowledge_basis": "bar_time", "interval_basis": "utc_half_open",
            "series_role": next(iter(roles)) if len(roles) == 1 else "mixed" if roles else "unknown",
            "adjustments_status": "declared_not_applied" if "adjustment" in kinds else "source_unavailable",
            "rollovers_status": "declared_not_executed" if "rollover" in kinds else "source_unavailable",
            "universe_status": "declared" if "universe" in kinds else "source_unavailable",
            "session_basis": "observed_calendar_separate"}


def validate_history(frame):
    """Valida fatos também na importação, sem criar tabelas ou substituir registros."""
    if list(frame.columns) != _COLUMNS:
        raise ValueError("Histórico do instrumento sem integridade; colunas desconhecidas.")
    for row in frame.to_dict("records"):
        _label(row["symbol"], "Símbolo")
        _label(row["source"], "Fonte")
        _label(row["event_key"], "Chave do evento")
        if row["kind"] not in KINDS:
            raise ValueError("Histórico do instrumento sem integridade; tipo desconhecido.")
        utc(row["known_at"])
        utc(row["recorded_at"])
        if row["valid_from"] is not None:
            utc(row["valid_from"])
        if row["valid_to"] is not None:
            utc(row["valid_to"])
            if row["valid_from"] is None or pd.Timestamp(row["valid_to"]) <= pd.Timestamp(row["valid_from"]):
                raise ValueError("Histórico do instrumento sem integridade; intervalo inválido.")
        _payload(row["kind"], json.loads(row["payload_json"]), historical=row["valid_from"] is not None)
        content = {k: row[k] for k in _COLUMNS if k not in ("revision_id", "recorded_at")}
        if hashlib.sha256(_json(content).encode("utf-8")).hexdigest() != row["revision_id"]:
            raise ValueError("Revisão do instrumento sem integridade; hash divergente.")


def require_supported_spec(spec):
    if spec.provenance.get("specification_value_sets", 0) > 1:
        raise ValueError("Propriedades de contrato variam no período; o motor usa especificação constante. "
                         "Separe períodos por vigência antes de simular; não use a propriedade atual como histórica.")
