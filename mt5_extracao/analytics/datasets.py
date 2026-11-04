"""Revisões imutáveis dos dados de pesquisa na base canônica (T20.4)."""
import copy
import hashlib
import json
import math
import re
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import inspect, text

from mt5_extracao import timeframes

VERSION = 3
BLOCK_ROWS = 512
_LOCK = threading.RLock()
_TABLES = ("_dataset_origin", "_dataset_blocks", "_dataset_manifests", "_dataset_references")
_SESSION_COLUMNS = ("symbol", "session_id", "open_utc", "close_utc", "open_broker", "close_broker",
                    "segments_json", "source", "timezone_offset_hours", "observed_at_utc", "input_hash", "revision")


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _hash(payload):
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def selection_time(value):
    return pd.Timestamp(value).isoformat() if value is not None else None


def _value(value):
    if value is pd.NA:
        return {"missing": "pdNA"}
    if value is pd.NaT:
        return {"missing": "NaT"}
    if isinstance(value, (pd.Timestamp, datetime)):
        return {"timestamp": pd.Timestamp(value).isoformat()}
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, float) and not math.isfinite(value):
        return {"number": "nan" if math.isnan(value) else "inf" if value > 0 else "-inf"}
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise ValueError(f"Tipo de dado não suportado pelo snapshot: {type(value).__name__}; revise a origem.")


def _restore(value):
    if not isinstance(value, dict):
        return value
    if set(value) == {"timestamp"}:
        return pd.Timestamp(value["timestamp"])
    if value == {"missing": "pdNA"}:
        return pd.NA
    if value == {"missing": "NaT"}:
        return pd.NaT
    if set(value) == {"number"} and value["number"] in ("nan", "inf", "-inf"):
        return float(value["number"])
    raise ValueError("Snapshot sem integridade: valor desconhecido; reimporte uma cópia íntegra.")


def _frame_payload(frame):
    return {"columns": list(frame.columns), "dtypes": [str(dtype) for dtype in frame.dtypes],
            "rows": [[_value(value) for value in row] for row in frame.itertuples(index=False, name=None)]}


def _decode_frame(payload):
    frame = pd.DataFrame([[_restore(v) for v in row] for row in payload["rows"]], columns=payload["columns"])
    for column, dtype in zip(payload["columns"], payload["dtypes"]):
        if not re.fullmatch(r"(?:object|string|bool|boolean|[uU]?[iI]nt(?:8|16|32|64)|[fF]loat(?:32|64)|datetime64\[[^\]]+\])", dtype):
            raise ValueError("Snapshot sem integridade: tipo de coluna não suportado.")
        if dtype.startswith("datetime64"):
            frame[column] = pd.to_datetime(frame[column]).astype(dtype)
        else:
            frame[column] = frame[column].astype(dtype)
    return frame


def _schema(db):
    with _LOCK, db.engine.begin() as conn:
        conn.execute(text("CREATE TABLE IF NOT EXISTS _dataset_origin (slot TEXT PRIMARY KEY, origin_id TEXT NOT NULL)"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS _dataset_blocks (block_id TEXT PRIMARY KEY, payload_json TEXT NOT NULL)"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS _dataset_manifests (manifest_id TEXT PRIMARY KEY, "
                          "payload_json TEXT NOT NULL, captured_at TEXT NOT NULL)"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS _dataset_references (run_id TEXT PRIMARY KEY, "
                          "manifest_id TEXT NOT NULL REFERENCES _dataset_manifests(manifest_id))"))
        conn.execute(text("INSERT INTO _dataset_origin (slot,origin_id) VALUES ('default',:id) "
                          "ON CONFLICT(slot) DO NOTHING"), {"id": uuid.uuid4().hex})
        return conn.execute(text("SELECT origin_id FROM _dataset_origin WHERE slot='default'")).scalar_one()


def _source(source, origin):
    source = dict(source or {})
    if set(source) - {"provider", "server", "derivation"}:
        raise ValueError("Proveniência aceita somente provider, server e derivation; não informe conta ou credenciais.")
    for value in source.values():
        if value is not None and (not isinstance(value, str) or len(value) > 256 or
                                  any(c in value for c in ("/", "\\", "@", "\n", "\r"))):
            raise ValueError("Proveniência deve usar rótulos sem caminhos, URLs ou dados pessoais.")
    return {"namespace": origin, "provider": source.get("provider") or "local_database",
            "server": source.get("server") or None, "derivation": source.get("derivation") or "stored_ohlcv_v1",
            "server_status": "declared" if source.get("server") else "unknown"}


_SOURCE_SLOTS = ("provider", "server")


def _slots(conn):
    return {row.slot: row.origin_id for row in conn.execute(text("SELECT slot, origin_id FROM _dataset_origin"))}


def identity(db):
    """Identidade lógica da base: vive no conteúdo do banco/schema, nunca no nome do arquivo.

    Backup/restauração e movimentação conservam o namespace; derivação independente usa ``fork``.
    ``provider``/``server`` são a fonte declarada da base (``None`` = não declarada, sem feed inventado).
    """
    namespace = _schema(db)
    with db.engine.connect() as conn:
        slots = _slots(conn)
    return {"namespace": namespace, "parent_namespace": slots.get("parent"),
            "lineage": "fork" if slots.get("parent") else "original",
            "provider": slots.get("provider"), "server": slots.get("server"),
            "backend": db.engine.dialect.name}


def fork(db):
    """Declara esta cópia como base derivada independente: novo namespace, origem registrada.

    Uma cópia que vai divergir precisa passar por aqui; sem isso, cópia = backup da mesma base.
    """
    parent = _schema(db)
    child = uuid.uuid4().hex
    with _LOCK, db.engine.begin() as conn:
        conn.execute(text("UPDATE _dataset_origin SET origin_id=:child WHERE slot='default'"), {"child": child})
        conn.execute(text("INSERT INTO _dataset_origin (slot,origin_id) VALUES ('parent',:parent) "
                          "ON CONFLICT(slot) DO UPDATE SET origin_id=excluded.origin_id"), {"parent": parent})
    return identity(db)


def declare_source(db, source):
    """Vincula a base a uma fonte; fonte incompatível é recusada, nunca misturada.

    A primeira declaração fixa ``provider``/``server``; repeti-la é idempotente. Outra fonte exige
    uma base nova, porque as chaves por símbolo não distinguem feeds; ``fork`` conserva a fonte.
    """
    wanted = _source(source, "validation")
    _schema(db)
    with _LOCK, db.engine.begin() as conn:
        slots = _slots(conn)
        for slot in _SOURCE_SLOTS:
            value = wanted[slot] if slot == "server" else (source or {}).get("provider")
            if value is None:
                continue
            current = slots.get(slot)
            if current is not None and current != value:
                raise ValueError(f"Base vinculada a {slot}={current!r}; recusada a gravação de {value!r}. "
                                 "Grave essa fonte numa base nova (outro banco ou schema); fork() deriva a mesma "
                                 "fonte e conserva a origem dos fatos herdados.")
            if current is None:
                conn.execute(text("INSERT INTO _dataset_origin (slot,origin_id) VALUES (:slot,:value)"),
                             {"slot": slot, "value": value})
    current = identity(db)
    return {key: current[key] for key in _SOURCE_SLOTS}


def _bound_source(db, source):
    """Fonte efetiva da captura: a declarada na base, validada contra a informada."""
    declared = declare_source(db, source) if source else identity(db)
    merged = dict(source or {})
    for slot in _SOURCE_SLOTS:
        if declared.get(slot) is not None:
            merged[slot] = declared[slot]
    return merged or None


def _blocks(frame, time_column, blocks):
    # Agrupar por dia evita deslocar todo o histórico se uma barra antiga chegar tarde.
    groups = [frame] if frame.empty else [group for _, group in frame.groupby(
        pd.to_datetime(frame[time_column], format="ISO8601").dt.strftime("%Y-%m-%d"), sort=True, dropna=False)]
    ids = []
    for group in groups:
        for begin in range(0, max(1, len(group)), BLOCK_ROWS):
            payload = _json(_frame_payload(group.iloc[begin:begin + BLOCK_ROWS].reset_index(drop=True)))
            digest = _hash(payload)
            blocks[digest] = payload
            ids.append(digest)
    return ids


@dataclass(frozen=True)
class Snapshot:
    manifest_id: str
    _manifest: dict
    _frames: dict

    @property
    def manifest(self):
        return copy.deepcopy(self._manifest)

    @property
    def bars(self):
        return self._frames["bars"].copy(deep=True)

    @property
    def events(self):
        return self._frames["events"].copy(deep=True)

    @property
    def market_calendar(self):
        return self._frames["market_calendar"].copy(deep=True)

    @property
    def instrument_history(self):
        frame = self._frames.get("instrument_history")
        return frame.copy(deep=True) if frame is not None else None

    @property
    def quality(self):
        from mt5_extracao.analytics import data_eligibility
        recorded = self._manifest["derivation"].get("data_quality")
        if self._manifest["schema_version"] >= 3 and recorded is not None:
            return copy.deepcopy(recorded)
        tf = timeframes.parse(self._manifest["timeframe"])
        report = data_eligibility.assess(self.bars, bar_minutes=tf.minutes or 1440)
        report["evidence_basis"] = "legacy_snapshot_no_acquisition_proof"
        return report

    def market_frame(self, family):
        if family not in ("ticks", "book"):
            raise ValueError("Família de mercado inválida.")
        if family not in self._frames:
            raise ValueError("Família não capturada nesta revisão; capture-a antes de consumir.")
        return self._frames[family].copy(deep=True)

    @property
    def spec_row(self):
        row = self._manifest["specification"]
        return {key: _restore(value) for key, value in row.items()} if row else None

    @property
    def spec(self):
        from mt5_extracao.strategies.backtester import SymbolSpec
        from mt5_extracao.strategies.specs import to_symbol_spec
        from mt5_extracao.analytics import instruments
        provenance = self._manifest["derivation"].get("instrument_context", {
            "specification_status": "unknown", "historical_validity": "legacy_snapshot_without_history"})
        row = self.spec_row
        if provenance["specification_status"] == "known":
            fact = instruments.specification(self._frames["instrument_history"], instruments.broker_utc(
                self.bars.time.iloc[0], self._manifest["time"]["broker_utc_offset_hours"]))
            row = fact["payload"]
        result = to_symbol_spec(self._manifest["symbol"], row) if row else SymbolSpec(
            symbol=self._manifest["symbol"], known=False)
        result.provenance = copy.deepcopy(provenance)
        return result

    def view(self):
        return SnapshotView(self)


class SnapshotView:
    """Leitor do snapshot para consumidores existentes; nunca consulta a base atual."""
    def __init__(self, snapshot):
        self.snapshot = snapshot

    @property
    def snapshot_events(self):
        return self.snapshot.events

    def load_ohlcv(self, symbol, timeframe_name, start=None, end=None):
        tf = timeframes.parse(timeframe_name)
        manifest = self.snapshot._manifest
        if symbol != manifest["symbol"] or tf is None or tf.name != manifest["timeframe"]:
            raise ValueError("Snapshot não contém esse ativo/timeframe; capture a origem correspondente.")
        frame = self.snapshot.bars
        if start is not None:
            frame = frame[frame.time >= pd.Timestamp(start)]
        if end is not None:
            frame = frame[frame.time <= pd.Timestamp(end)]
        return frame.reset_index(drop=True)

    def get_symbol_spec(self, symbol):
        if symbol != self.snapshot._manifest["symbol"]:
            raise ValueError("Snapshot não contém a especificação desse ativo.")
        from mt5_extracao.analytics import instruments
        row = self.snapshot.spec_row
        spec = self.snapshot.spec
        if spec.provenance.get("specification_status") == "known":
            row = instruments.specification(self.snapshot._frames["instrument_history"], instruments.broker_utc(
                self.snapshot.bars.time.iloc[0], self.snapshot._manifest["time"]["broker_utc_offset_hours"]))["payload"]
        return dict(row, provenance=spec.provenance) if row else None

    def load_market_frame(self, symbol, family, start_utc=None, end_utc=None):
        if symbol != self.snapshot._manifest["symbol"]:
            raise ValueError("Snapshot não contém esse ativo.")
        frame = self.snapshot.market_frame(family)
        if start_utc is not None:
            frame = frame[frame.event_time_utc >= pd.Timestamp(start_utc)]
        if end_utc is not None:
            frame = frame[frame.event_time_utc < pd.Timestamp(end_utc)]
        return frame.reset_index(drop=True)


_EVIDENCE_COLUMNS = ["table_name", "block_start", "block_end", "rows", "status", "quality_json"]
_QUALITY_KEYS = {"quality_version", "family", "origin", "origin_basis", "reported_fields", "minute_proofs",
                 "received_at", "complete_market_coverage", "clock_basis", "symbol", "bar_proofs", "duplicate_bar_times", "timeframe"}
_DERIVED_FIELDS = ["symbol", "tf", "session_id", "bar_start_utc", "bar_start_broker", "bar_end_utc", "open", "high",
                   "low", "close", "tick_volume", "real_volume", "real_volume_known", "quality_status",
                   "coverage_ratio", "revision", "input_hash", "computed_at_utc"]


def _derived_evidence(symbol, tf, conn):
    if not inspect(conn).has_table("_bars_derived"):
        return pd.DataFrame(columns=_DERIVED_FIELDS)
    return pd.read_sql(text("SELECT " + ",".join(_DERIVED_FIELDS) + " FROM _bars_derived "
                             "WHERE symbol=:symbol AND tf=:tf ORDER BY bar_start_utc,session_id,revision"), conn,
                       params={"symbol": symbol, "tf": "1D" if tf.name == "D1" else tf.name})


def _evidence_items(frame):
    items = []
    for raw in frame.quality_json.dropna():
        item = json.loads(raw)
        if item.get("quality_version") == 1 and item.get("family") in ("ticks", "book", "bars"):
            if set(item) - _QUALITY_KEYS:
                raise ValueError("Metadados de coleta desconhecidos; confira o contrato de evidência.")
            items.append(item)
    return items


def _acquisition_evidence(db, symbol, families, conn, first_utc, end_utc, bars):
    frame = pd.DataFrame(columns=_EVIDENCE_COLUMNS)
    if not families or not inspect(conn).has_table("_extraction_log"):
        return frame
    tables = [db.find_table_for_symbol(symbol, family, connection=conn) for family in families]
    tables = [table for table in tables if table is not None]
    if not tables:
        return frame
    marks = ",".join(f":t{i}" for i in range(len(tables)))
    frame = pd.read_sql(text(f"SELECT {','.join(_EVIDENCE_COLUMNS)} FROM _extraction_log "
                             f"WHERE table_name IN ({marks}) ORDER BY table_name,block_start,block_end"),
                        conn, params={f"t{i}": table for i, table in enumerate(tables)})
    first_minute = int(first_utc.value // 60_000_000_000)
    last_minute = int((end_utc.value - 1) // 60_000_000_000)
    times = {pd.Timestamp(value).isoformat() for value in bars.time}
    tf = timeframes.parse(families[0])
    for index, value in frame.quality_json.items():
        # Nunca exportar configuração/metadados livres de um importador externo.
        try:
            source = json.loads(value) if value else {}
            clean = {"quality_version": source.get("quality_version") if source.get("quality_version") == 1 else None,
                     "family": source.get("family") if source.get("family") in ("ticks", "book", "bars") else None,
                     "origin": source.get("origin") if source.get("origin") in ("observed", "generated") else "unknown",
                     "origin_basis": "provider_declaration_api_response_not_independent_venue_validation",
                     "symbol": symbol if source.get("symbol") == symbol else None,
                     "timeframe": tf.name if tf is not None and source.get("timeframe") == tf.name else None,
                     "complete_market_coverage": None}
            clean["minute_proofs"] = [dict(minute=proof["minute"], rows=proof["rows"], sha256=proof["sha256"])
                                      for proof in source.get("minute_proofs", ())
                                      if type(proof.get("minute")) is int and type(proof.get("rows")) is int and proof["rows"] >= 0
                                      and re.fullmatch(r"[a-f0-9]{64}", str(proof.get("sha256", "")))
                                      and first_minute <= proof["minute"] <= last_minute]
            clean["reported_fields"] = [field for field in source.get("reported_fields", ())
                                        if field in {"time", "open", "high", "low", "close", "spread", "real_volume", "tick_volume", *db.TICK_FIELDS, *db.BOOK_FIELDS}]
            clean["bar_proofs"] = [dict(time=proof["time"], sha256=proof["sha256"]) for proof in source.get("bar_proofs", ())
                                   if proof.get("time") in times and re.fullmatch(r"[a-f0-9]{64}", str(proof.get("sha256", "")))]
            clean["duplicate_bar_times"] = [value for value in source.get("duplicate_bar_times", ()) if value in times]
            received = pd.Timestamp(source.get("received_at")) if source.get("received_at") else pd.NaT
            clean["received_at"] = received.tz_convert("UTC").isoformat() if pd.notna(received) and received.tzinfo else None
            if source.get("clock_basis") == "collector_request_utc_not_venue_event_time":
                clean["clock_basis"] = source["clock_basis"]
            frame.at[index, "quality_json"] = _json(clean)
        except (ValueError, TypeError, KeyError, AttributeError):
            frame.at[index, "quality_json"] = None
    return frame


def capture(db, symbol, timeframe, start=None, end=None, *, source=None, families=()):
    """Captura dados consistentes e grava blocos deduplicados após encerrar a leitura."""
    tf = timeframes.parse(timeframe)
    if tf is None:
        raise ValueError(f"Timeframe inválido: {timeframe}")
    if db is None or getattr(db, "engine", None) is None:
        raise ValueError("Banco indisponível; confira a conexão antes de capturar os dados.")
    _source(source, "validation")  # Rejeitar configuração pessoal antes de qualquer escrita.
    if isinstance(families, str) or set(families) - {"ticks", "book"}:
        raise ValueError("Famílias adicionais aceitas: ticks e book; informe uma lista explícita.")
    if hasattr(db, "require_fixed_offset"):
        db.require_fixed_offset("A captura de snapshot (datasets.capture)")
    origin = _schema(db)
    source = _bound_source(db, source)
    from mt5_extracao.trade_analytics import calendar
    from mt5_extracao.analytics import instruments
    from mt5_extracao.analytics import data_eligibility
    with db.consistent_read() as conn:
        bars = db.load_ohlcv(symbol, tf.label, start, end, connection=conn)
        if bars.empty:
            raise ValueError(f"Sem barras de {symbol} {tf.name}; extraia o período antes de capturar os dados.")
        first_utc = instruments.broker_utc(bars.time.iloc[0], db.broker_utc_offset)
        last = pd.Timestamp(bars.time.iloc[-1])
        close = last + (pd.offsets.MonthBegin(1) if tf.name == "MN1" else pd.Timedelta(minutes=tf.minutes or 1440))
        end_utc = instruments.broker_utc(close, db.broker_utc_offset)
        market = {family: db.load_market_frame(symbol, family, first_utc, end_utc, connection=conn)
                  for family in sorted(set(families))}
        acquisition_evidence = _acquisition_evidence(db, symbol, (tf.label, *families), conn, first_utc, end_utc, bars)
        derived = _derived_evidence(symbol, tf, conn)
        spec = db.get_symbol_spec(symbol, connection=conn)
        instrument_history = instruments.history(db, symbol, connection=conn)
        instruments.validate_history(instrument_history)
        if spec:
            fields = set(db.SPEC_FIELDS) | {"symbol", "updated_at", "spread_profile"}
            spec = {key: value for key, value in spec.items() if key in fields}
        events = calendar.merged(db, connection=conn)
        if not events.empty:
            events = events.sort_values(["event_time", "sources", "symbol", "currency", "name"],
                                        na_position="last").reset_index(drop=True)
        sessions = pd.DataFrame(columns=list(_SESSION_COLUMNS))
        if inspect(conn).has_table("_market_calendar"):
            sessions = pd.read_sql(text("SELECT " + ",".join(_SESSION_COLUMNS) +
                                        " FROM _market_calendar WHERE symbol=:symbol "
                                        "ORDER BY open_utc,session_id,revision"), conn, params={"symbol": symbol})
    blocks = {}
    instrument_context = instruments.context(instrument_history, bars, db.broker_utc_offset)
    if instrument_context["specification_status"] == "known":
        fact = instruments.specification(instrument_history, instruments.broker_utc(bars.time.iloc[0], db.broker_utc_offset))
        spec = dict(fact["payload"], symbol=symbol)
    manifest = {"schema_version": VERSION, "source": _source(source, origin), "symbol": symbol,
                "timeframe": tf.name, "selection": {"start": selection_time(start), "end": selection_time(end)},
                "period": {"first": str(bars.time.iloc[0]), "last": str(bars.time.iloc[-1]), "rows": len(bars)},
                "time": {"storage_basis": db.time_basis, "consumer_basis": "broker",
                         "broker_utc_offset_hours": db.broker_utc_offset,
                         "offset_state": db.time_context()["offset_state"] if hasattr(db, "time_context") else "declared"},
                "specification": {key: _value(value) for key, value in spec.items()} if spec else None,
                "derivation": {"ohlcv_reader": 1, "economic_calendar_merge": 1,
                               "session_free_metadata": "omitted_input_hash_retained",
                               "instrument_context": instrument_context,
                               "market_families": sorted(set(families)),
                               "data_quality": data_eligibility.assess(bars, bar_minutes=tf.minutes or 1440,
                                   ticks=market.get("ticks"), book=market.get("book"), evidence=_evidence_items(acquisition_evidence), derived=derived, symbol=symbol, timeframe=tf.name)},
                "frames": {"bars": _blocks(bars, "time", blocks),
                           "events": _blocks(events, "event_time", blocks),
                           "market_calendar": _blocks(sessions, "open_utc", blocks),
                           "instrument_history": _blocks(instrument_history, "known_at", blocks),
                           "acquisition_evidence": _blocks(acquisition_evidence, "block_start", blocks),
                           "derived_bars": _blocks(derived, "bar_start_utc", blocks),
                           **{family: _blocks(frame, "event_time_utc", blocks) for family, frame in market.items()}}}
    payload = _json(manifest)
    bundle = {"format": f"mt5x.dataset.v{VERSION}", "manifest_id": _hash(payload), "manifest": manifest, "blocks": blocks}
    _persist(db, bundle)
    return load(db, bundle["manifest_id"])


def _validated(bundle):
    try:
        if set(bundle) != {"format", "manifest_id", "manifest", "blocks"}:
            raise ValueError("envelope desconhecido")
        manifest = bundle["manifest"]
        version = manifest["schema_version"]
        if version not in (1, 2, 3) or bundle["format"] != f"mt5x.dataset.v{version}":
            raise ValueError("versão desconhecida")
        if _hash(_json(manifest)) != bundle["manifest_id"]:
            raise ValueError("hash do manifesto divergente")
        if set(manifest) != {"schema_version", "source", "symbol", "timeframe", "selection", "period", "time",
                             "specification", "derivation", "frames"}:
            raise ValueError("campos de manifesto desconhecidos")
        provenance = manifest["source"]
        if not re.fullmatch(r"[a-f0-9]{32}", provenance["namespace"]):
            raise ValueError("origem lógica inválida")
        if _source({k: provenance[k] for k in ("provider", "server", "derivation")}, provenance["namespace"]) != provenance:
            raise ValueError("proveniência inválida")
        frames = {}
        referenced = set()
        roles = {"bars", "events", "market_calendar"} | ({"instrument_history"} if version >= 2 else set())
        if version == 3:
            families = manifest["derivation"]["market_families"]
            if not isinstance(families, list) or families != sorted(set(families)) or set(families) - {"ticks", "book"}:
                raise ValueError("famílias de mercado inválidas")
            roles |= {"acquisition_evidence", "derived_bars", *families}
        for role, ids in manifest["frames"].items():
            if role not in roles or not ids:
                raise ValueError("coleção desconhecida")
            chunks = []
            for digest in ids:
                referenced.add(digest)
                payload = bundle["blocks"][digest]
                if _hash(payload) != digest:
                    raise ValueError("hash do bloco divergente")
                chunks.append(_decode_frame(json.loads(payload)))
            frames[role] = pd.concat(chunks, ignore_index=True) if len(chunks) > 1 else chunks[0]
        if set(frames) != roles:
            raise ValueError("coleção ausente")
        if set(bundle["blocks"]) != referenced:
            raise ValueError("blocos não cobertos pelo manifesto")
        bars = frames["bars"]
        if (bars.empty or manifest["period"]["rows"] != len(bars) or
                manifest["period"]["first"] != str(bars.time.iloc[0]) or
                manifest["period"]["last"] != str(bars.time.iloc[-1])):
            raise ValueError("período divergente do conteúdo")
        if version >= 2:
            from mt5_extracao.analytics import instruments
            instruments.validate_history(frames["instrument_history"])
            if any(frames["instrument_history"].symbol != manifest["symbol"]):
                raise ValueError("histórico pertence a outro instrumento")
            context = instruments.context(frames["instrument_history"], bars, manifest["time"]["broker_utc_offset_hours"])
            if manifest["derivation"].get("instrument_context") != context:
                raise ValueError("vigência do instrumento divergente do histórico")
            if context["specification_status"] == "known":
                fact = instruments.specification(frames["instrument_history"], instruments.broker_utc(
                    bars.time.iloc[0], manifest["time"]["broker_utc_offset_hours"]))
                expected = {k: _value(v) for k, v in dict(fact["payload"], symbol=manifest["symbol"]).items()}
                if manifest["specification"] != expected:
                    raise ValueError("propriedades divergentes da vigência declarada")
        if version == 3:
            from mt5_extracao.analytics import data_eligibility
            if list(frames["acquisition_evidence"].columns) != _EVIDENCE_COLUMNS:
                raise ValueError("colunas de evidência desconhecidas")
            if list(frames["derived_bars"].columns) != _DERIVED_FIELDS:
                raise ValueError("colunas de derivação desconhecidas")
            for role in ("ticks", "book", "derived_bars"):
                if role in frames and ("symbol" not in frames[role] or any(frames[role].symbol != manifest["symbol"])):
                    raise ValueError("família de outro ativo")
            tf = timeframes.parse(manifest["timeframe"])
            quality = data_eligibility.assess(bars, bar_minutes=tf.minutes or 1440, ticks=frames.get("ticks"),
                         book=frames.get("book"), evidence=_evidence_items(frames["acquisition_evidence"]), derived=frames["derived_bars"], symbol=manifest["symbol"], timeframe=tf.name)
            if quality != manifest["derivation"].get("data_quality"):
                raise ValueError("qualidade divergente dos dados capturados")
        return Snapshot(bundle["manifest_id"], copy.deepcopy(manifest), frames)
    except (KeyError, TypeError, ValueError, AttributeError, IndexError, OverflowError) as error:
        raise ValueError("Snapshot sem integridade; reimporte uma cópia íntegra ou recapture a origem.") from error


def _persist(db, bundle):
    _validated(bundle)
    _schema(db)
    with _LOCK, db.engine.begin() as conn:
        for digest, payload in bundle["blocks"].items():
            conn.execute(text("INSERT INTO _dataset_blocks (block_id,payload_json) VALUES (:id,:payload) "
                              "ON CONFLICT(block_id) DO NOTHING"), {"id": digest, "payload": payload})
            actual = conn.execute(text("SELECT payload_json FROM _dataset_blocks WHERE block_id=:id"),
                                  {"id": digest}).scalar_one()
            if actual != payload:
                raise ValueError("Bloco existente sem integridade; restaure uma cópia íntegra em destino isolado.")
        payload = _json(bundle["manifest"])
        conn.execute(text("INSERT INTO _dataset_manifests (manifest_id,payload_json,captured_at) "
                          "VALUES (:id,:payload,:stamp) ON CONFLICT(manifest_id) DO NOTHING"),
                     {"id": bundle["manifest_id"], "payload": payload,
                      "stamp": datetime.now(timezone.utc).isoformat()})
        actual = conn.execute(text("SELECT payload_json FROM _dataset_manifests WHERE manifest_id=:id"),
                              {"id": bundle["manifest_id"]}).scalar_one()
        if actual != payload:
            raise ValueError("Manifesto existente sem integridade; restaure uma cópia íntegra.")


def _bundle(db, manifest_id):
    if (db is None or getattr(db, "engine", None) is None or
            not re.fullmatch(r"[a-f0-9]{64}", str(manifest_id)) or not inspect(db.engine).has_table("_dataset_manifests")):
        raise ValueError("Revisão de dados indisponível; importe o snapshot ou recapture a origem.")
    with db.consistent_read() as conn:
        if not inspect(conn).has_table("_dataset_blocks"):
            raise ValueError("Snapshot sem integridade; recupere os blocos de uma cópia íntegra.")
        payload = conn.execute(text("SELECT payload_json FROM _dataset_manifests WHERE manifest_id=:id"),
                               {"id": manifest_id}).scalar()
        if payload is None:
            raise ValueError("Revisão de dados indisponível; importe o snapshot ou recapture a origem.")
        try:
            manifest = json.loads(payload)
            blocks = {}
            for ids in manifest["frames"].values():
                for digest in ids:
                    value = conn.execute(text("SELECT payload_json FROM _dataset_blocks WHERE block_id=:id"),
                                         {"id": digest}).scalar()
                    if value is None:
                        raise ValueError("bloco ausente")
                    blocks[digest] = value
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("Snapshot sem integridade; reimporte uma cópia íntegra.") from error
    return {"format": f"mt5x.dataset.v{manifest['schema_version']}", "manifest_id": manifest_id, "manifest": manifest, "blocks": blocks}


def load(db, manifest_id):
    return _validated(_bundle(db, manifest_id))


def bind(db, run_id, manifest_id, *, connection=None):
    """Referência imutável; com connection integra a mesma transação que publica o run."""
    if connection is None:
        load(db, manifest_id)
        with db.engine.begin() as conn:
            return bind(db, run_id, manifest_id, connection=conn)
    if not inspect(connection).has_table("_dataset_manifests") or connection.execute(text(
            "SELECT 1 FROM _dataset_manifests WHERE manifest_id=:id"), {"id": manifest_id}).scalar() is None:
        raise ValueError("Revisão de dados indisponível; não publique o run sem recuperar a origem.")
    connection.execute(text("INSERT INTO _dataset_references (run_id,manifest_id) VALUES (:run,:id) "
                            "ON CONFLICT(run_id) DO NOTHING"), {"run": run_id, "id": manifest_id})
    actual = connection.execute(text("SELECT manifest_id FROM _dataset_references WHERE run_id=:run"),
                                {"run": run_id}).scalar_one()
    if actual != manifest_id:
        raise ValueError("Run já referencia outra revisão; crie uma execução nova para os dados alterados.")


def for_run(db, run_id):
    if db is None or getattr(db, "engine", None) is None:
        return None
    with db.engine.connect() as conn:
        ident = None
        if inspect(conn).has_table("_dataset_references"):
            ident = conn.execute(text("SELECT manifest_id FROM _dataset_references WHERE run_id=:run"),
                                 {"run": run_id}).scalar()
        if inspect(conn).has_table("_robot_runs"):
            metrics = conn.execute(text("SELECT metrics_json FROM _robot_runs WHERE run_id=:run"),
                                   {"run": run_id}).scalar()
            if metrics:
                try:
                    advertised = json.loads(metrics).get("data_manifest_id")
                except (ValueError, AttributeError) as error:
                    raise ValueError("Proveniência do run sem integridade; recupere o registro original.") from error
                if advertised is not None and advertised != ident:
                    raise ValueError("Referência de dados indisponível; recupere o snapshot antes de analisar o run.")
    return load(db, ident) if ident is not None else None


def export_snapshot(db, manifest_id, destination):
    bundle = _bundle(db, manifest_id)
    _validated(bundle)
    path = Path(destination)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = _json(bundle)
    # Não sobrescrever um arquivo do usuário ou uma exportação anterior diferente.
    try:
        with path.open("x", encoding="utf-8", newline="\n") as output:
            output.write(payload)
    except FileExistsError:
        if path.read_text(encoding="utf-8") != payload:
            raise ValueError("Destino já contém outro arquivo; escolha um nome novo para exportar.") from None
    return str(path)


def import_snapshot(db, source):
    try:
        bundle = json.loads(Path(source).read_text(encoding="utf-8"))
        snapshot = _validated(bundle)
    except (ValueError, TypeError, OSError) as error:
        raise ValueError("Arquivo sem integridade; confira o pacote de dados antes de importar.") from error
    _persist(db, bundle)
    return snapshot
