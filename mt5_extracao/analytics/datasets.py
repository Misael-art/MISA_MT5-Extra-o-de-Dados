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

VERSION = 2
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


def capture(db, symbol, timeframe, start=None, end=None, *, source=None):
    """Captura dados consistentes e grava blocos deduplicados após encerrar a leitura."""
    tf = timeframes.parse(timeframe)
    if tf is None:
        raise ValueError(f"Timeframe inválido: {timeframe}")
    if db is None or getattr(db, "engine", None) is None:
        raise ValueError("Banco indisponível; confira a conexão antes de capturar os dados.")
    _source(source, "validation")  # Rejeitar configuração pessoal antes de qualquer escrita.
    origin = _schema(db)
    from mt5_extracao.trade_analytics import calendar
    from mt5_extracao.analytics import instruments
    with db.consistent_read() as conn:
        bars = db.load_ohlcv(symbol, tf.label, start, end, connection=conn)
        if bars.empty:
            raise ValueError(f"Sem barras de {symbol} {tf.name}; extraia o período antes de capturar os dados.")
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
                         "broker_utc_offset_hours": db.broker_utc_offset},
                "specification": {key: _value(value) for key, value in spec.items()} if spec else None,
                "derivation": {"ohlcv_reader": 1, "economic_calendar_merge": 1,
                               "session_free_metadata": "omitted_input_hash_retained",
                               "instrument_context": instrument_context},
                "frames": {"bars": _blocks(bars, "time", blocks),
                           "events": _blocks(events, "event_time", blocks),
                           "market_calendar": _blocks(sessions, "open_utc", blocks),
                           "instrument_history": _blocks(instrument_history, "known_at", blocks)}}
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
        if version not in (1, 2) or bundle["format"] != f"mt5x.dataset.v{version}":
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
        roles = {"bars", "events", "market_calendar"} | ({"instrument_history"} if version == 2 else set())
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
        if version == 2:
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
