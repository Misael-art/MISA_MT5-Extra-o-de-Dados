"""Persistência versionada e consultas point-in-time para características analíticas.

Os timestamps deste módulo são sempre UTC e exigem fuso explícito. Cada correção
é anexada como uma revisão; consultas históricas escolhem a última revisão que
já estava disponível no instante pedido.
"""
import hashlib
import json
import math
import threading
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import inspect, text

DEFINITIONS_TABLE = "_feature_definitions"
VALUES_TABLE = "_feature_values"

_CREATE_DEFINITIONS = f"""
CREATE TABLE IF NOT EXISTS {DEFINITIONS_TABLE} (
    feature_id TEXT NOT NULL,
    algorithm_version TEXT NOT NULL,
    name TEXT NOT NULL,
    parameters_json TEXT NOT NULL,
    dependencies_json TEXT NOT NULL,
    registered_at_utc TEXT NOT NULL,
    PRIMARY KEY (feature_id, algorithm_version)
)
"""

_CREATE_VALUES = f"""
CREATE TABLE IF NOT EXISTS {VALUES_TABLE} (
    symbol TEXT NOT NULL,
    tf TEXT NOT NULL,
    observation_time TEXT NOT NULL,
    available_at_utc TEXT NOT NULL,
    observed_at_utc TEXT NOT NULL,
    valid_until_utc TEXT,
    feature_id TEXT NOT NULL,
    algorithm_version TEXT NOT NULL,
    numeric_value DOUBLE PRECISION,
    categorical_value TEXT,
    quality TEXT NOT NULL,
    source_revision TEXT NOT NULL,
    revision INTEGER NOT NULL,
    value_hash TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    PRIMARY KEY (symbol, tf, observation_time, feature_id, algorithm_version, revision),
    FOREIGN KEY (feature_id, algorithm_version)
        REFERENCES {DEFINITIONS_TABLE} (feature_id, algorithm_version)
)
"""

_CREATE_ASOF_INDEX = f"CREATE INDEX IF NOT EXISTS ix_feature_values_asof " \
                     f"ON {VALUES_TABLE} (symbol, tf, feature_id, available_at_utc, observation_time, revision)"
_CREATE_SOURCE_INDEX = f"CREATE INDEX IF NOT EXISTS ix_feature_values_source_revision " \
                       f"ON {VALUES_TABLE} (feature_id, algorithm_version, source_revision)"
_SCHEMA_LOCK = threading.Lock()

VALUE_COLUMNS = (
    "symbol", "tf", "observation_time", "available_at_utc", "observed_at_utc", "valid_until_utc",
    "feature_id", "algorithm_version", "numeric_value", "categorical_value", "quality", "source_revision",
    "revision", "metadata_json",
)


def _usable(db):
    return db is not None and getattr(db, "engine", None) is not None


def _require_db(db):
    if not _usable(db):
        raise ValueError("O banco analítico não está conectado.")


def _canonical_json(value, label):
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} precisa conter apenas valores JSON válidos.") from exc


def _utc_text(value, label):
    try:
        stamp = pd.Timestamp(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{label} precisa ser uma data e hora válida em UTC.") from exc
    if pd.isna(stamp) or stamp.tzinfo is None:
        raise ValueError(f"{label} precisa incluir fuso horário (use UTC, por exemplo 2026-10-14T17:25:00Z).")
    return stamp.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")


def ensure_schema(db):
    """Cria somente as tabelas internas novas; não altera tabelas históricas."""
    _require_db(db)
    with _SCHEMA_LOCK, db.engine.begin() as conn:
        conn.execute(text(_CREATE_DEFINITIONS))
        conn.execute(text(_CREATE_VALUES))
        conn.execute(text(_CREATE_ASOF_INDEX))
        conn.execute(text(_CREATE_SOURCE_INDEX))


def register_definition(db, feature_id, name, algorithm_version, parameters, dependencies):
    """Registra uma definição imutável; para mudar algoritmo ou parâmetros, crie outra versão."""
    _require_db(db)
    if feature_id is None or name is None or algorithm_version is None:
        raise ValueError("feature_id, name e algorithm_version são obrigatórios.")
    feature_id = str(feature_id).strip()
    name = str(name).strip()
    algorithm_version = str(algorithm_version).strip()
    if not feature_id or not name or not algorithm_version:
        raise ValueError("feature_id, name e algorithm_version são obrigatórios.")
    parameters_json = _canonical_json(parameters, "parameters")
    dependencies_json = _canonical_json(dependencies, "dependencies")
    registered_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")
    ensure_schema(db)
    with db.engine.begin() as conn:
        existing = conn.execute(text(
            f"SELECT name, parameters_json, dependencies_json FROM {DEFINITIONS_TABLE} "
            "WHERE feature_id = :feature_id AND algorithm_version = :version"),
            {"feature_id": feature_id, "version": algorithm_version}).mappings().first()
        if existing:
            if (existing["name"], existing["parameters_json"], existing["dependencies_json"]) == (
                    name, parameters_json, dependencies_json):
                return False
            raise ValueError("A definição já existe com este algoritmo; use uma nova algorithm_version.")
        conn.execute(text(
            f"INSERT INTO {DEFINITIONS_TABLE} "
            "(feature_id, algorithm_version, name, parameters_json, dependencies_json, registered_at_utc) "
            "VALUES (:feature_id, :version, :name, :parameters, :dependencies, :registered_at)"),
            {"feature_id": feature_id, "version": algorithm_version, "name": name,
             "parameters": parameters_json, "dependencies": dependencies_json, "registered_at": registered_at})
    return True


def list_definitions(db):
    """Devolve todas as versões registradas, com JSON já decodificado."""
    if not _usable(db) or not inspect(db.engine).has_table(DEFINITIONS_TABLE):
        return []
    with db.engine.connect() as conn:
        rows = conn.execute(text(
            f"SELECT feature_id, algorithm_version, name, parameters_json, dependencies_json, registered_at_utc "
            f"FROM {DEFINITIONS_TABLE} ORDER BY registered_at_utc, feature_id, algorithm_version")).mappings()
        result = []
        for row in rows:
            item = dict(row)
            item["parameters"] = json.loads(item.pop("parameters_json"))
            item["dependencies"] = json.loads(item.pop("dependencies_json"))
            result.append(item)
        return result


def _normalize_value(value):
    if value is None:
        raise ValueError("A característica precisa ter valor numérico ou categórico.")
    if hasattr(value, "item"):
        try:
            value = value.item()
        except (ValueError, AttributeError):
            pass
    if isinstance(value, bool):
        return None, "true" if value else "false"
    if isinstance(value, (int, float)):
        number = float(value)
        if not math.isfinite(number):
            raise ValueError("O valor numérico da característica precisa ser finito.")
        return number, None
    if isinstance(value, str) and value:
        return None, value
    raise ValueError("O valor categórico precisa ser texto não vazio.")


def save_value(db, symbol, timeframe, feature_id, algorithm_version, observation_time, value, *,
               available_at=None, observed_at=None, valid_until=None, source_revision="0", quality="VALID",
               metadata=None):
    """Grava um fato ou acrescenta revisão se o resultado ou a origem mudou.

    ``observation_time`` é o instante descrito pela característica. ``observed_at``
    registra quando a fonte foi observada; ``available_at`` é quando o sistema
    pôde usar o valor. A disponibilidade nunca deve ser inferida do instante de
    mercado: o produtor precisa passá-la quando diferir de ``observed_at``.
    """
    _require_db(db)
    row = _value_record(symbol, timeframe, feature_id, algorithm_version, observation_time, value,
                        available_at=available_at, observed_at=observed_at, valid_until=valid_until,
                        source_revision=source_revision, quality=quality, metadata=metadata)
    feature_id, algorithm_version = row["feature_id"], row["algorithm_version"]
    available, value_hash = row["available_at_utc"], row["value_hash"]
    ensure_schema(db)
    key = {name: row[name] for name in ("symbol", "tf", "observation_time", "feature_id", "algorithm_version")}
    with db.engine.begin() as conn:
        definition = conn.execute(text(
            f"SELECT 1 FROM {DEFINITIONS_TABLE} WHERE feature_id = :feature_id AND algorithm_version = :version"),
            {"feature_id": feature_id, "version": algorithm_version}).first()
        if not definition:
            raise ValueError(f"Característica não registrada: {feature_id}@{algorithm_version}.")
        current = conn.execute(text(
            f"SELECT revision, value_hash, available_at_utc FROM {VALUES_TABLE} WHERE symbol = :symbol AND tf = :tf "
            "AND observation_time = :observation_time AND feature_id = :feature_id "
            "AND algorithm_version = :algorithm_version ORDER BY revision DESC LIMIT 1"), key).mappings().first()
        if current and current["value_hash"] == value_hash:
            return int(current["revision"])
        if current and available < current["available_at_utc"]:
            raise ValueError("available_at não pode retroceder em uma revisão da mesma característica.")
        revision = int(current["revision"]) + 1 if current else 1
        params = dict(row, revision=revision)
        conn.execute(text(
            f"INSERT INTO {VALUES_TABLE} "
            "(symbol, tf, observation_time, available_at_utc, observed_at_utc, valid_until_utc, feature_id, "
            "algorithm_version, numeric_value, categorical_value, quality, source_revision, revision, value_hash, "
            "metadata_json) VALUES (:symbol, :tf, :observation_time, :available_at_utc, :observed_at_utc, "
            ":valid_until_utc, :feature_id, :algorithm_version, :numeric_value, :categorical_value, :quality, "
            ":source_revision, :revision, :value_hash, :metadata_json)"), params)
    return revision


def _empty_values():
    return pd.DataFrame(columns=VALUE_COLUMNS)


def query_as_of(db, as_of, *, symbol=None, timeframe=None, feature_id=None, algorithm_version=None,
                start=None, end=None):
    """Consulta a última revisão disponível em ``as_of`` para cada instante observado.

    ``start`` é inclusivo e ``end`` exclusivo; ambos filtram ``observation_time``.
    Sem ``algorithm_version``, versões de algoritmo distintas são devolvidas em
    linhas separadas para que uma comparação histórica não descarte proveniência.
    """
    _require_db(db)
    as_of_utc = _utc_text(as_of, "as_of")
    if not inspect(db.engine).has_table(VALUES_TABLE):
        return _empty_values()
    where = ["available_at_utc <= :as_of"]
    params = {"as_of": as_of_utc}
    if symbol is not None:
        where.append("symbol = :symbol")
        params["symbol"] = str(symbol)
    if timeframe is not None:
        where.append("tf = :tf")
        params["tf"] = str(timeframe)
    if feature_id is not None:
        where.append("feature_id = :feature_id")
        params["feature_id"] = str(feature_id)
    if algorithm_version is not None:
        where.append("algorithm_version = :algorithm_version")
        params["algorithm_version"] = str(algorithm_version)
    if start is not None:
        where.append("observation_time >= :start")
        params["start"] = _utc_text(start, "start")
    if end is not None:
        where.append("observation_time < :end")
        params["end"] = _utc_text(end, "end")
    sql = f"""
        WITH eligible AS (
            SELECT {', '.join(VALUE_COLUMNS)},
                   ROW_NUMBER() OVER (
                       PARTITION BY symbol, tf, observation_time, feature_id, algorithm_version
                       ORDER BY available_at_utc DESC, observed_at_utc DESC, revision DESC
                   ) AS point_rank
            FROM {VALUES_TABLE}
            WHERE {' AND '.join(where)}
        )
        SELECT {', '.join(VALUE_COLUMNS)} FROM eligible
        WHERE point_rank = 1 AND (valid_until_utc IS NULL OR valid_until_utc > :as_of)
        ORDER BY symbol, tf, feature_id, algorithm_version, observation_time
    """
    with db.engine.connect() as conn:
        frame = pd.read_sql(text(sql), conn, params=params)
    if frame.empty:
        return _empty_values()
    for name in ("observation_time", "available_at_utc", "observed_at_utc", "valid_until_utc"):
        frame[name] = pd.to_datetime(frame[name], utc=True, errors="coerce")
    frame["revision"] = frame["revision"].astype(int)
    return frame


def query_history(db, *, symbol=None, timeframe=None, feature_id=None, algorithm_version=None, start=None, end=None):
    """Consulta todas as revisões, inclusive fatos vencidos ou corrigidos depois.

    Use esta saída com ``asof_join`` para reconstruir o que uma decisão histórica
    poderia conhecer. ``query_as_of`` continua sendo um retrato único e descarta
    revisões que ainda não estavam disponíveis naquele instante.
    """
    _require_db(db)
    if not inspect(db.engine).has_table(VALUES_TABLE):
        return _empty_values()
    where, params = [], {}
    for column, value, parameter in (
            ("symbol", symbol, "symbol"), ("tf", timeframe, "tf"),
            ("feature_id", feature_id, "feature_id"),
            ("algorithm_version", algorithm_version, "algorithm_version")):
        if value is not None:
            where.append(f"{column} = :{parameter}")
            params[parameter] = str(value)
    if start is not None:
        where.append("observation_time >= :start")
        params["start"] = _utc_text(start, "start")
    if end is not None:
        where.append("observation_time < :end")
        params["end"] = _utc_text(end, "end")
    sql = f"SELECT {', '.join(VALUE_COLUMNS)} FROM {VALUES_TABLE}"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY symbol, tf, feature_id, algorithm_version, observation_time, available_at_utc, revision"
    with db.engine.connect() as conn:
        frame = pd.read_sql(text(sql), conn, params=params)
    if frame.empty:
        return _empty_values()
    for name in ("observation_time", "available_at_utc", "observed_at_utc", "valid_until_utc"):
        frame[name] = pd.to_datetime(frame[name], utc=True, errors="coerce")
    frame["revision"] = frame["revision"].astype(int)
    return frame


def _value_record(symbol, timeframe, feature_id, algorithm_version, observation_time, value, *, available_at=None,
                  observed_at=None, valid_until=None, source_revision="0", quality="VALID", metadata=None):
    """Valida um valor como ``save_value`` e devolve a linha pronta, ainda sem número de revisão."""
    if any(item is None for item in (symbol, timeframe, feature_id, algorithm_version)):
        raise ValueError("symbol, timeframe, feature_id e algorithm_version são obrigatórios.")
    symbol, timeframe = str(symbol).strip(), str(timeframe).strip()
    feature_id, algorithm_version = str(feature_id).strip(), str(algorithm_version).strip()
    quality = str(quality).strip()
    if not symbol or not timeframe or not feature_id or not algorithm_version or not quality:
        raise ValueError("symbol, timeframe, feature_id, algorithm_version e quality são obrigatórios.")
    observation = _utc_text(observation_time, "observation_time")
    observed = _utc_text(observed_at if observed_at is not None else datetime.now(timezone.utc), "observed_at")
    available = _utc_text(available_at if available_at is not None else observed, "available_at")
    until = _utc_text(valid_until, "valid_until") if valid_until is not None else None
    if available < observed:
        raise ValueError("available_at não pode ser anterior a observed_at.")
    if until is not None and until <= available:
        raise ValueError("valid_until precisa ser posterior a available_at.")
    numeric, categorical = _normalize_value(value)
    if (source_revision is None or isinstance(source_revision, bool)
            or not isinstance(source_revision, (str, int)) or not str(source_revision).strip()):
        raise ValueError("source_revision precisa identificar a revisão da origem (use '0' se não houver revisão).")
    source_revision = str(source_revision).strip()
    if metadata is None:
        metadata = {}
    if not isinstance(metadata, dict):
        raise ValueError("metadata precisa ser um objeto JSON.")
    metadata_json = _canonical_json(metadata, "metadata")
    digest_payload = {"numeric_value": numeric, "categorical_value": categorical, "quality": quality,
                      "source_revision": source_revision, "valid_until_utc": until, "metadata_json": metadata_json}
    return {"symbol": symbol, "tf": timeframe, "observation_time": observation, "feature_id": feature_id,
            "algorithm_version": algorithm_version, "available_at_utc": available, "observed_at_utc": observed,
            "valid_until_utc": until, "numeric_value": numeric, "categorical_value": categorical,
            "quality": quality, "source_revision": source_revision,
            "value_hash": hashlib.sha256(_canonical_json(digest_payload, "value").encode("utf-8")).hexdigest(),
            "metadata_json": metadata_json}


def save_values(db, records):
    """Grava muitos valores numa única transação, com as mesmas regras de ``save_value``.

    ``records`` é uma sequência de dicionários com os argumentos de ``save_value``
    (``symbol``, ``timeframe``, ``feature_id``, ``algorithm_version``,
    ``observation_time``, ``value`` e os opcionais). Tudo é validado antes da
    primeira escrita; um erro não deixa gravação parcial. Devolve a contagem de
    linhas ``inserted`` (primeira revisão), ``revised`` e ``unchanged``.
    """
    _require_db(db)
    rows = []
    for item in records:
        item = dict(item)
        args = [item.pop(name) for name in ("symbol", "timeframe", "feature_id", "algorithm_version",
                                             "observation_time", "value")]
        rows.append(_value_record(*args, **item))
    counts = {"inserted": 0, "revised": 0, "unchanged": 0}
    if not rows:
        return counts
    ensure_schema(db)
    with db.engine.begin() as conn:
        pairs = {(row["feature_id"], row["algorithm_version"]) for row in rows}
        for feature_id, version in pairs:
            if not conn.execute(text(
                    f"SELECT 1 FROM {DEFINITIONS_TABLE} WHERE feature_id = :feature_id AND algorithm_version = :version"),
                    {"feature_id": feature_id, "version": version}).first():
                raise ValueError(f"Característica não registrada: {feature_id}@{version}.")
        current = {}
        for symbol, tf, feature_id, version in {(r["symbol"], r["tf"], r["feature_id"], r["algorithm_version"])
                                                for r in rows}:
            result = conn.execute(text(
                f"SELECT observation_time, revision, value_hash, available_at_utc FROM {VALUES_TABLE} "
                "WHERE symbol = :symbol AND tf = :tf AND feature_id = :feature_id "
                "AND algorithm_version = :version ORDER BY observation_time, revision"),
                {"symbol": symbol, "tf": tf, "feature_id": feature_id, "version": version}).mappings()
            for found in result:
                current[(symbol, tf, found["observation_time"], feature_id, version)] = dict(found)
        inserts = []
        for row in rows:
            key = (row["symbol"], row["tf"], row["observation_time"], row["feature_id"], row["algorithm_version"])
            previous = current.get(key)
            if previous and previous["value_hash"] == row["value_hash"]:
                counts["unchanged"] += 1
                continue
            if previous and row["available_at_utc"] < previous["available_at_utc"]:
                raise ValueError("available_at não pode retroceder em uma revisão da mesma característica "
                                 f"({row['feature_id']} em {row['observation_time']}).")
            row["revision"] = int(previous["revision"]) + 1 if previous else 1
            counts["revised" if previous else "inserted"] += 1
            current[key] = row
            inserts.append(row)
        if inserts:
            conn.execute(text(
                f"INSERT INTO {VALUES_TABLE} "
                "(symbol, tf, observation_time, available_at_utc, observed_at_utc, valid_until_utc, feature_id, "
                "algorithm_version, numeric_value, categorical_value, quality, source_revision, revision, value_hash, "
                "metadata_json) VALUES (:symbol, :tf, :observation_time, :available_at_utc, :observed_at_utc, "
                ":valid_until_utc, :feature_id, :algorithm_version, :numeric_value, :categorical_value, :quality, "
                ":source_revision, :revision, :value_hash, :metadata_json)"), inserts)
    return counts


def asof_join(decisions, values, *, by=None):
    """Liga cada instante de decisão ao último valor que já estava disponível nele.

    ``values`` deve conter o histórico completo de revisões, normalmente de
    ``query_history``. A identidade padrão da série usa as colunas disponíveis entre
    ``symbol``, ``tf``, ``feature_id`` e ``algorithm_version``. Dimensões também
    presentes em ``decisions`` restringem a junção; dimensões ausentes expandem uma
    linha por série. Use ``by`` para definir outra identidade.

    A função escolhe a observação mais recente disponível e, para ela, a última
    revisão que já existia na decisão. Valores vencidos e barras superiores ainda
    abertas ficam ausentes. ``query_as_of`` serve para um retrato único: seu resultado
    já descartou revisões antigas e observações vencidas no instante consultado.
    """
    if "decision_time" not in decisions:
        raise ValueError("decisions precisa conter decision_time.")
    frame = decisions.copy()
    times = pd.to_datetime(frame["decision_time"], utc=True)
    if times.isna().any():
        raise ValueError("decision_time precisa ser uma data e hora válida com fuso.")
    frame["decision_time"] = times
    frame["_order"] = range(len(frame))
    if values is not None and not values.empty:
        if by is None:
            by = [name for name in ("symbol", "tf", "feature_id", "algorithm_version")
                  if name in values.columns]
        elif isinstance(by, str):
            by = [by]
        else:
            by = list(by)
    else:
        by = list(by or ())
    if values is None or values.empty:
        for name in ("observation_time", "available_at_utc", "numeric_value", "categorical_value", "quality",
                     "revision"):
            frame[name] = None
        return frame.drop(columns="_order")
    required = {"observation_time", "available_at_utc", "valid_until_utc"}
    missing = required - set(values.columns)
    if missing:
        raise ValueError("values sem colunas obrigatórias: " + ", ".join(sorted(missing)))
    missing_keys = set(by) - set(values.columns)
    if missing_keys:
        raise ValueError("values sem dimensões de série: " + ", ".join(sorted(missing_keys)))
    history = values.copy()
    for name in ("observation_time", "available_at_utc", "valid_until_utc"):
        history[name] = pd.to_datetime(history[name], utc=True, errors="coerce")
    if history[["observation_time", "available_at_utc"]].isna().any().any():
        raise ValueError("values contém observation_time ou available_at_utc inválido.")
    keys = history[by].drop_duplicates().to_dict("records") if by else [{}]
    parts = []
    for key in keys:
        mask = pd.Series(True, index=history.index)
        for name, value in key.items():
            mask &= history[name] == value
        group = history[mask].sort_values(["available_at_utc", "observation_time"] +
                                          (["revision"] if "revision" in history else []))
        # Um lote pode tornar várias observações disponíveis no mesmo instante.
        # Primeiro escolha a revisão mais nova de cada observação naquele instante;
        # depois mantenha só a observação mais recente do fluxo.
        group = group.drop_duplicates(["available_at_utc", "observation_time"], keep="last")
        newest = group.groupby("available_at_utc", sort=True)["observation_time"].transform("max")
        group = group[group["observation_time"] == newest].sort_values("available_at_utc")
        group["_latest_observation"] = group["observation_time"].cummax()
        group = group[group["observation_time"] == group["_latest_observation"]].drop(
            columns="_latest_observation")
        left = frame
        for name, value in key.items():
            if name in left.columns:
                left = left[left[name] == value]
        if left.empty:
            continue
        left = left.sort_values("decision_time").assign(**{name: value for name, value in key.items()
                                                            if name not in left.columns})
        drop_keys = [name for name in by if name in group.columns]
        merged = pd.merge_asof(left, group.drop(columns=drop_keys), left_on="decision_time", right_on="available_at_utc",
                               direction="backward", allow_exact_matches=True)
        expired = merged["valid_until_utc"].notna() & (merged["valid_until_utc"] <= merged["decision_time"])
        value_columns = [name for name in group.columns if name not in drop_keys]
        merged.loc[expired, value_columns] = None
        parts.append(merged)
    if not parts:
        for name in ("observation_time", "available_at_utc", "numeric_value", "categorical_value", "quality",
                     "revision"):
            if name not in frame.columns:
                frame[name] = None
        return frame.drop(columns="_order")
    result = pd.concat(parts, ignore_index=True).sort_values(["_order"] + by).drop(columns="_order")
    return result.reset_index(drop=True)
