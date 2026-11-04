"""Persistência versionada e consultas point-in-time para características analíticas.

Os timestamps deste módulo são sempre UTC e exigem fuso explícito. Cada correção
é anexada como uma revisão; consultas históricas escolhem a última revisão que
já estava disponível no instante pedido.
"""
import hashlib
import json
from contextlib import nullcontext
import logging
import math
import threading
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import bindparam, inspect, text

DEFINITIONS_TABLE = "_feature_definitions"
VALUES_TABLE = "_feature_values"
DEFINITION_SCHEMA_VERSION = 2
log = logging.getLogger(__name__)

_CREATE_DEFINITIONS = f"""
CREATE TABLE IF NOT EXISTS {DEFINITIONS_TABLE} (
    feature_id TEXT NOT NULL,
    algorithm_version TEXT NOT NULL,
    name TEXT NOT NULL,
    parameters_json TEXT NOT NULL,
    dependencies_json TEXT NOT NULL,
    registered_at_utc TEXT NOT NULL,
    family TEXT,
    formula TEXT,
    unit TEXT,
    warmup_count INTEGER,
    warmup_unit TEXT,
    source_id TEXT,
    timeframe TEXT,
    availability_rule TEXT,
    value_type TEXT,
    definition_schema_version INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (feature_id, algorithm_version)
)
"""

_DEFINITION_METADATA_COLUMNS = {
    "family": "TEXT",
    "formula": "TEXT",
    "unit": "TEXT",
    "warmup_count": "INTEGER",
    "warmup_unit": "TEXT",
    "source_id": "TEXT",
    "timeframe": "TEXT",
    "availability_rule": "TEXT",
    "value_type": "TEXT",
}

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
_CREATE_KEY_INDEX = f"CREATE INDEX IF NOT EXISTS ix_feature_values_key_revision " \
                    f"ON {VALUES_TABLE} (symbol, tf, feature_id, algorithm_version, observation_time, revision)"
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


def _utc_timestamp(value, label):
    try:
        stamp = pd.Timestamp(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{label} precisa ser uma data e hora válida em UTC.") from exc
    if pd.isna(stamp) or stamp.tzinfo is None:
        raise ValueError(f"{label} precisa incluir fuso horário (use UTC, por exemplo 2026-10-14T17:25:00Z).")
    return stamp.tz_convert("UTC")


def _utc_text(value, label):
    return _utc_timestamp(value, label).strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")


def ensure_schema(db):
    """Cria as tabelas internas e migra metadados de definição somente por adição."""
    _require_db(db)
    with _SCHEMA_LOCK, db.engine.begin() as conn:
        conn.execute(text(_CREATE_DEFINITIONS))
        columns = {column["name"] for column in inspect(conn).get_columns(DEFINITIONS_TABLE)}
        for name, sql_type in _DEFINITION_METADATA_COLUMNS.items():
            if name not in columns:
                conn.execute(text(f"ALTER TABLE {DEFINITIONS_TABLE} ADD COLUMN {name} {sql_type}"))
        if "definition_schema_version" not in columns:
            conn.execute(text(f"ALTER TABLE {DEFINITIONS_TABLE} ADD COLUMN "
                              "definition_schema_version INTEGER NOT NULL DEFAULT 1"))
        conn.execute(text(_CREATE_VALUES))
        conn.execute(text(_CREATE_ASOF_INDEX))
        conn.execute(text(_CREATE_KEY_INDEX))
        conn.execute(text(_CREATE_SOURCE_INDEX))


def _legacy_name(definition):
    return definition["definition_schema_version"] == 1 and definition["name"] == "value_type"


def register_definition(db, feature_id, name, algorithm_version, parameters, dependencies, *, family=None,
                        formula=None, unit=None, warmup_count=None, warmup_unit=None, source_id=None, timeframe=None,
                        availability_rule=None, value_type=None):
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
    metadata = {"family": family, "formula": formula, "unit": unit, "warmup_count": warmup_count,
                "warmup_unit": warmup_unit, "source_id": source_id, "timeframe": timeframe,
                "availability_rule": availability_rule, "value_type": value_type}
    for field in ("family", "formula", "unit", "warmup_unit", "source_id", "timeframe", "availability_rule",
                  "value_type"):
        value = metadata[field]
        if value is not None:
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field} precisa ser texto não vazio quando informado.")
            metadata[field] = value.strip()
    if warmup_count is not None:
        if isinstance(warmup_count, bool) or not isinstance(warmup_count, int) or warmup_count < 0:
            raise ValueError("warmup_count precisa ser um inteiro maior ou igual a zero.")
        if metadata["warmup_unit"] is None:
            raise ValueError("warmup_unit é obrigatório quando warmup_count é informado.")
    elif warmup_unit is not None:
        raise ValueError("warmup_count é obrigatório quando warmup_unit é informado.")
    if value_type is not None and metadata["value_type"] not in {"NUMBER", "CATEGORY", "BOOLEAN"}:
        raise ValueError("value_type precisa ser NUMBER, CATEGORY ou BOOLEAN.")
    registered_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")
    ensure_schema(db)
    with db.engine.begin() as conn:
        existing = conn.execute(text(
            f"SELECT name, parameters_json, dependencies_json, family, formula, unit, warmup_count, warmup_unit, source_id, "
            f"timeframe, availability_rule, value_type, definition_schema_version FROM {DEFINITIONS_TABLE} "
            "WHERE feature_id = :feature_id AND algorithm_version = :version"),
            {"feature_id": feature_id, "version": algorithm_version}).mappings().first()
        if existing:
            existing_metadata = {key: existing[key] for key in metadata}
            same = (existing["parameters_json"], existing["dependencies_json"], existing_metadata) == (
                parameters_json, dependencies_json, metadata)
            if same:
                if existing["name"] == name:
                    return False
                if _legacy_name(existing):
                    log.warning("Definição %s@%s mantém nome legado não verificável; repetição dos demais campos "
                                "aceita sem renomear ou modificar valores.", feature_id, algorithm_version)
                    return False
            raise ValueError("A definição já existe com este algoritmo; use uma nova versão do algoritmo.")
        conn.execute(text(
            f"INSERT INTO {DEFINITIONS_TABLE} "
            "(feature_id, algorithm_version, name, parameters_json, dependencies_json, registered_at_utc, family, "
            "formula, unit, warmup_count, warmup_unit, source_id, timeframe, availability_rule, value_type, "
            "definition_schema_version) "
            "VALUES (:feature_id, :version, :name, :parameters, :dependencies, :registered_at, :family, :formula, "
            ":unit, :warmup_count, :warmup_unit, :source_id, :timeframe, :availability_rule, :value_type, "
            ":definition_schema_version)"),
            {"feature_id": feature_id, "version": algorithm_version, "name": name,
             "parameters": parameters_json, "dependencies": dependencies_json, "registered_at": registered_at,
             "definition_schema_version": DEFINITION_SCHEMA_VERSION, **metadata})
    return True


def list_definitions(db, *, connection=None):
    """Devolve todas as versões registradas, com JSON já decodificado."""
    reader = connection if connection is not None else getattr(db, "engine", None)
    if not _usable(db) or not inspect(reader).has_table(DEFINITIONS_TABLE):
        return []
    columns = {column["name"] for column in inspect(reader).get_columns(DEFINITIONS_TABLE)}
    metadata_columns = ", ".join(name if name in columns else f"NULL AS {name}"
                                  for name in _DEFINITION_METADATA_COLUMNS)
    schema_column = ("definition_schema_version" if "definition_schema_version" in columns else
                     "1 AS definition_schema_version")
    with (nullcontext(connection) if connection is not None else db.engine.connect()) as conn:
        rows = conn.execute(text(
            f"SELECT feature_id, algorithm_version, name, parameters_json, dependencies_json, registered_at_utc, "
            f"{metadata_columns}, {schema_column} "
            f"FROM {DEFINITIONS_TABLE} ORDER BY registered_at_utc, feature_id, algorithm_version")).mappings()
        result = []
        for row in rows:
            item = dict(row)
            item["parameters"] = json.loads(item.pop("parameters_json"))
            item["dependencies"] = json.loads(item.pop("dependencies_json"))
            item["name_status"] = "UNVERIFIED_LEGACY_NAME" if _legacy_name(item) else "DECLARED"
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


def _validate_declared_value_type(value, value_type, *, quality="VALID"):
    """Confere o valor contra o tipo declarado; definições legadas sem tipo seguem aceitas."""
    if value_type is None or (value is None and quality in {"INVALIDATED", "UNAVAILABLE"}):
        return
    if hasattr(value, "item"):
        try:
            value = value.item()
        except (ValueError, AttributeError):
            pass
    valid = {
        "NUMBER": (not isinstance(value, bool) and isinstance(value, (int, float))
                   and math.isfinite(float(value))),
        "CATEGORY": isinstance(value, str) and bool(value.strip()),
        "BOOLEAN": isinstance(value, bool),
    }.get(value_type, False)
    if not valid:
        raise ValueError(f"O valor informado não corresponde ao value_type {value_type} da definição.")


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
            f"SELECT value_type FROM {DEFINITIONS_TABLE} WHERE feature_id = :feature_id AND algorithm_version = :version"),
            {"feature_id": feature_id, "version": algorithm_version}).first()
        if not definition:
            raise ValueError(f"Característica não registrada: {feature_id}@{algorithm_version}.")
        _validate_declared_value_type(value, definition[0], quality=row["quality"])
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


def _asof_statement(as_of, *, symbol=None, timeframe=None, feature_id=None, feature_ids=None,
                    algorithm_version=None, start=None, end=None, ordered_for=None, after=None, limit=None):
    """Uma regra de revisão/validade para a leitura completa e a consulta paginada."""
    where = ["available_at_utc <= :as_of"]
    params = {"as_of": _utc_text(as_of, "as_of")}
    if symbol is not None:
        where.append("symbol = :symbol")
        params["symbol"] = str(symbol)
    if timeframe is not None:
        where.append("tf = :tf")
        params["tf"] = str(timeframe)
    if feature_id is not None:
        where.append("feature_id = :feature_id")
        params["feature_id"] = str(feature_id)
    if feature_ids is not None:
        where.append("feature_id IN :feature_ids")
        params["feature_ids"] = list(feature_ids)
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
    """
    if ordered_for is not None:
        # A ordem do contrato é a lexical Python, não a locale do servidor PostgreSQL.
        collation = {"sqlite":"BINARY", "postgresql":'"C"'}[ordered_for]
        ident, version = f"feature_id COLLATE {collation}", f"algorithm_version COLLATE {collation}"
        if after is not None:
            sql += f" AND ({ident} > :after_feature OR ({ident} = :after_feature AND " \
                   f"(observation_time > :after_time OR (observation_time = :after_time AND {version} > :after_version))))"
            params.update(after_feature=after[0], after_time=_utc_text(after[1], "cursor"), after_version=after[2])
        sql += f" ORDER BY {ident}, observation_time, {version}"
    else:
        sql += " ORDER BY symbol, tf, feature_id, algorithm_version, observation_time"
    if limit is not None:
        sql += " LIMIT :page_limit"
        params["page_limit"] = limit
    statement = text(sql)
    if feature_ids is not None:
        statement = statement.bindparams(bindparam("feature_ids", expanding=True))
    return statement, params


def query_as_of(db, as_of, *, symbol=None, timeframe=None, feature_id=None, algorithm_version=None,
                start=None, end=None):
    """Última revisão disponível; start inclusivo/end exclusivo filtram observation_time.

    Versões distintas continuam em linhas separadas. A regra SQL é compartilhada
    com o leitor paginado; esta API conserva o DataFrame completo solicitado.
    """
    _require_db(db)
    statement, params = _asof_statement(as_of, symbol=symbol, timeframe=timeframe, feature_id=feature_id,
                                      algorithm_version=algorithm_version, start=start, end=end)
    if not inspect(db.engine).has_table(VALUES_TABLE):
        return _empty_values()
    with db.engine.connect() as conn:
        frame = pd.read_sql(statement, conn, params=params)
    if frame.empty:
        return _empty_values()
    for name in ("observation_time", "available_at_utc", "observed_at_utc", "valid_until_utc"):
        frame[name] = pd.to_datetime(frame[name], utc=True, errors="coerce")
    frame["revision"] = frame["revision"].astype(int)
    return frame


def with_base_identity(db, frame):
    """Carimba a identidade lógica da base (T20.4) em fatos lidos deste banco.

    Os fatos do store não guardam a base nas chaves: o banco/schema é a base. Antes de juntar
    resultados de bancos diferentes, cada leitura precisa deste carimbo para não confundir origens.
    """
    from mt5_extracao.analytics import datasets

    base = datasets.identity(db)
    stamped = frame.copy()
    for column, key in (("base_namespace", "namespace"), ("base_lineage", "lineage"),
                        ("base_provider", "provider"), ("base_server", "server")):
        stamped[column] = base[key]
    return stamped


def query_history(db, *, symbol=None, timeframe=None, feature_id=None, algorithm_version=None, start=None, end=None,
                  connection=None, max_rows=None):
    """Consulta todas as revisões, inclusive fatos vencidos ou corrigidos depois.

    Use esta saída com ``asof_join`` para reconstruir o que uma decisão histórica
    poderia conhecer. ``query_as_of`` continua sendo um retrato único e descarta
    revisões que ainda não estavam disponíveis naquele instante.
    """
    _require_db(db)
    if max_rows is not None and (isinstance(max_rows, bool) or not isinstance(max_rows, int) or max_rows < 0):
        raise ValueError("max_rows precisa ser um inteiro não negativo.")
    if not inspect(connection if connection is not None else db.engine).has_table(VALUES_TABLE):
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
    if max_rows is not None:
        sql += " LIMIT :row_limit"
        params["row_limit"] = max_rows + 1
    with (nullcontext(connection) if connection is not None else db.engine.connect()) as conn:
        frame = pd.read_sql(text(sql), conn, params=params)
    if max_rows is not None and len(frame) > max_rows:
        raise ValueError(f"Histórico excede {max_rows} fatos; reduza a janela ou a seleção de indicadores.")
    if frame.empty:
        return _empty_values()
    for name in ("observation_time", "available_at_utc", "observed_at_utc", "valid_until_utc"):
        frame[name] = pd.to_datetime(frame[name], utc=True, errors="coerce")
    frame["revision"] = frame["revision"].astype(int)
    return frame


def latest_values(db, *, symbol=None, timeframe=None, feature_ids=None, algorithm_version=None):
    """Devolve a última revisão gravada de cada valor, sem carregar o histórico inteiro."""
    _require_db(db)
    if not inspect(db.engine).has_table(VALUES_TABLE):
        return _empty_values()
    where, params = [], {}
    for column, value, parameter in (("symbol", symbol, "symbol"), ("tf", timeframe, "tf"),
                                     ("algorithm_version", algorithm_version, "algorithm_version")):
        if value is not None:
            where.append(f"{column} = :{parameter}")
            params[parameter] = str(value)
    expanding = False
    if feature_ids is not None:
        ids = sorted({str(item) for item in feature_ids if str(item)})
        if not ids:
            return _empty_values()
        where.append("feature_id IN :feature_ids")
        params["feature_ids"] = ids
        expanding = True
    sql = f"""
        WITH ranked AS (
            SELECT {', '.join(VALUE_COLUMNS)},
                   ROW_NUMBER() OVER (
                       PARTITION BY symbol, tf, observation_time, feature_id, algorithm_version
                       ORDER BY revision DESC
                   ) AS value_rank
            FROM {VALUES_TABLE}
            {"WHERE " + " AND ".join(where) if where else ""}
        )
        SELECT {', '.join(VALUE_COLUMNS)} FROM ranked WHERE value_rank = 1
        ORDER BY symbol, tf, feature_id, algorithm_version, observation_time
    """
    statement = text(sql)
    if expanding:
        statement = statement.bindparams(bindparam("feature_ids", expanding=True))
    with db.engine.connect() as conn:
        frame = pd.read_sql(statement, conn, params=params)
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
    if value is None:
        if quality not in {"INVALIDATED", "UNAVAILABLE"}:
            raise ValueError("Valor ausente só é permitido com quality='INVALIDATED' ou 'UNAVAILABLE'.")
        numeric, categorical = None, None
    else:
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


def save_values(db, records, *, transaction_guard=None, transaction_hook=None):
    """Grava muitos valores numa única transação, com as mesmas regras de ``save_value``.

    ``records`` é uma sequência de dicionários com os argumentos de ``save_value``
    (``symbol``, ``timeframe``, ``feature_id``, ``algorithm_version``,
    ``observation_time``, ``value`` e os opcionais). Tudo é validado antes da
    primeira escrita; um erro não deixa gravação parcial. ``transaction_guard``
    confere a geração dentro da exclusão de escrita, antes dos valores;
    ``transaction_hook`` confirma o checkpoint depois deles. Devolve a contagem de
    linhas ``inserted`` (primeira revisão), ``revised`` e ``unchanged``.
    """
    _require_db(db)
    rows = []
    typed_values = []
    for item in records:
        item = dict(item)
        args = [item.pop(name) for name in ("symbol", "timeframe", "feature_id", "algorithm_version",
                                             "observation_time", "value")]
        typed_values.append((args[2], args[3], args[5]))
        rows.append(_value_record(*args, **item))
    counts = {"inserted": 0, "revised": 0, "unchanged": 0}
    if not rows and transaction_guard is None and transaction_hook is None:
        return counts
    ensure_schema(db)
    with db.engine.begin() as conn:
        # Seleção da revisão e inserção pertencem à mesma exclusão de escrita.
        if conn.dialect.name == "sqlite" and not conn.connection.driver_connection.in_transaction:
            conn.exec_driver_sql("BEGIN IMMEDIATE")
        if transaction_guard is not None:
            transaction_guard(conn)
        pairs = {(row["feature_id"], row["algorithm_version"]) for row in rows}
        definitions = {}
        for feature_id, version in sorted(pairs):
            lock = " FOR UPDATE" if conn.dialect.name == "postgresql" else ""
            definition = conn.execute(text(
                f"SELECT value_type FROM {DEFINITIONS_TABLE} WHERE feature_id = :feature_id "
                "AND algorithm_version = :version" + lock),
                {"feature_id": feature_id, "version": version}).first()
            if not definition:
                raise ValueError(f"Característica não registrada: {feature_id}@{version}.")
            definitions[(feature_id, version)] = definition[0]
        for (feature_id, version, value), row in zip(typed_values, rows):
            _validate_declared_value_type(value, definitions[(feature_id, version)], quality=row["quality"])
        current = {}
        requested_times = {}
        for row in rows:
            identity = (row["symbol"], row["tf"], row["feature_id"], row["algorithm_version"])
            requested_times.setdefault(identity, set()).add(row["observation_time"])
        for (symbol, tf, feature_id, version), times in requested_times.items():
            ordered_times = sorted(times)
            for offset in range(0, len(ordered_times), 400):
                chunk = ordered_times[offset:offset + 400]
                placeholders = ", ".join(f":observation_{index}" for index in range(len(chunk)))
                params = {"symbol": symbol, "tf": tf, "feature_id": feature_id, "version": version}
                params.update({f"observation_{index}": value for index, value in enumerate(chunk)})
                result = conn.execute(text(
                    f"SELECT observation_time, revision, value_hash, available_at_utc FROM {VALUES_TABLE} "
                    "WHERE symbol = :symbol AND tf = :tf AND feature_id = :feature_id "
                    "AND algorithm_version = :version "
                    f"AND observation_time IN ({placeholders}) ORDER BY observation_time, revision"), params).mappings()
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
        if transaction_hook is not None:
            transaction_hook(conn, counts)
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
    times = pd.to_datetime(frame["decision_time"].map(
        lambda value: _utc_timestamp(value, "decision_time")), utc=True)
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
    for name in ("observation_time", "available_at_utc", "valid_until_utc", *(
            ("observed_at_utc",) if "observed_at_utc" in history else ())):
        history[name] = pd.to_datetime(history[name].map(
            lambda value: pd.NaT if name == "valid_until_utc" and pd.isna(value)
            else _utc_timestamp(value, name)), utc=True)
    if history[["observation_time", "available_at_utc"]].isna().any().any():
        raise ValueError("values contém observation_time ou available_at_utc inválido.")
    keys = history[by].drop_duplicates().to_dict("records") if by else [{}]
    if (by and all(name in frame and frame[name].dtype == history[name].dtype for name in by)
            and not history[by].isna().any().any() and not frame[by].isna().any().any()):
        # Identidade já completa: merge_asof pode processar todas as séries de
        # uma vez. A expansão parcial/heterogênea mantém o contrato anterior.
        left = frame.merge(history[by].drop_duplicates(), on=by, how="inner", sort=False)
        if left.empty:
            for name in ("observation_time", "available_at_utc", "numeric_value", "categorical_value", "quality", "revision"):
                if name not in frame:
                    frame[name] = None
            return frame.drop(columns="_order")
        group = history.sort_values(["available_at_utc", "observation_time"] +
            (["observed_at_utc"] if "observed_at_utc" in history else []) +
            (["revision"] if "revision" in history else []))
        group = group.drop_duplicates(list(dict.fromkeys(by + ["available_at_utc", "observation_time"])), keep="last")
        newest = group.groupby(list(dict.fromkeys(by + ["available_at_utc"])), sort=False)["observation_time"].transform("max")
        group = group[group.observation_time == newest]
        latest = group.groupby(by, sort=False)["observation_time"].cummax()
        group = group[group.observation_time == latest]
        merged = pd.merge_asof(left.sort_values("decision_time"), group.sort_values("available_at_utc"), by=by,
            left_on="decision_time", right_on="available_at_utc", direction="backward", allow_exact_matches=True)
        expired = merged.valid_until_utc.notna() & (merged.valid_until_utc <= merged.decision_time)
        merged.loc[expired, [name for name in group if name not in by]] = None
        return merged.sort_values(["_order"] + by).drop(columns="_order").reset_index(drop=True)
    parts = []
    for key in keys:
        mask = pd.Series(True, index=history.index)
        for name, value in key.items():
            mask &= history[name] == value
        group = history[mask].sort_values(["available_at_utc", "observation_time"] +
                                          (["observed_at_utc"] if "observed_at_utc" in history else []) +
                                          (["revision"] if "revision" in history else []))
        # Um lote pode tornar várias observações disponíveis no mesmo instante.
        # Mesmo desempate do query_as_of: recebimento e, depois, revisão.
        # Primeiro escolha a revisão efetiva de cada observação naquele instante;
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



RELATIONS_TABLE = "_feature_relations"


def _relation_payload(dto):
    """Valida o contrato e a identidade antes de aceitar bytes persistidos."""
    import re
    from mt5_extracao.analytics import relation_dto
    keys = ("left", "right", "relation", "scope", "population", "provenance", "generated_at_utc", "synthetic_fixture")
    try:
        checked = relation_dto.build_pair_relation_dto(**{key: dto[key] for key in keys})
        provenance = checked["provenance"]
        ident = provenance["analysis_id"]
        lineage = provenance["lineage_manifests"]
        if (checked != dto or not re.fullmatch(r"[a-f0-9]{64}", ident) or not isinstance(lineage, list)
                or not 1 <= len(lineage) <= 32 or len(set(lineage)) != len(lineage)
                or provenance["snapshot_manifest_id"] not in lineage
                or any(not isinstance(item, str) or not re.fullmatch(r"[a-f0-9]{64}", item) for item in lineage)):
            raise ValueError("Envelope/linhagem inválidos.")
        actual = relation_dto.pair_analysis_id(provenance["snapshot_manifest_id"], checked["scope"]["settings"],
            provenance["sample_hash"], provenance["implementation_sha256"], lineage)
        if actual != ident:
            raise ValueError("Identidade não reconcilia com população/implementação/seleção.")
        raw = relation_dto.dumps(checked)
        if len(raw.encode()) > 16 * 1024 * 1024:
            raise ValueError("Artefato excede16MiB; reduza a seleção.")
        semantic = json.loads(raw)
        semantic.pop("generated_at_utc")
        semantic["provenance"].pop("limits", None)
        semantic_hash = hashlib.sha256(_canonical_json(semantic, "relation semantics").encode()).hexdigest()
        return checked, raw, hashlib.sha256(raw.encode()).hexdigest(), semantic_hash
    except (KeyError, TypeError, AttributeError) as exc:
        raise ValueError("Artefato de relação incompleto; recalcule pela consulta congelada.") from exc


def _relation_schema(db):
    _require_db(db)
    with _SCHEMA_LOCK, db.engine.begin() as conn:
        if conn.dialect.name == "postgresql":
            conn.execute(text("SELECT pg_advisory_xact_lock(181502)"))
        elif not conn.connection.driver_connection.in_transaction:
            conn.exec_driver_sql("BEGIN IMMEDIATE")
        conn.execute(text(f"CREATE TABLE IF NOT EXISTS {RELATIONS_TABLE} ("
            "analysis_id TEXT PRIMARY KEY, base_namespace TEXT NOT NULL, manifest_id TEXT NOT NULL, "
            "symbol TEXT NOT NULL, tf TEXT NOT NULL, as_of_utc TEXT NOT NULL, "
            "payload_json TEXT NOT NULL, payload_sha256 TEXT NOT NULL, semantic_sha256 TEXT NOT NULL, "
            "created_at_utc TEXT NOT NULL, "
            "FOREIGN KEY(manifest_id) REFERENCES _dataset_manifests(manifest_id))"))


def _relation_row(row):
    dto, raw, digest, semantic = _relation_payload(json.loads(row["payload_json"]))
    provenance, scope = dto["provenance"], dto["scope"]
    if (raw != row["payload_json"] or digest != row["payload_sha256"] or semantic != row["semantic_sha256"]
            or any(row[key] != value for key, value in dict(analysis_id=provenance["analysis_id"],
                base_namespace=provenance["base_namespace"], manifest_id=provenance["snapshot_manifest_id"],
                symbol=scope["symbol"], tf=scope["timeframe"], as_of_utc=provenance["as_of_utc"]).items())):
        raise ValueError("Relação persistida sem integridade; restaure o artefato original.")
    return dto


def _relation_sources(db, dto):
    from mt5_extracao.analytics import datasets
    provenance = dto["provenance"]
    root = datasets.load(db, provenance["snapshot_manifest_id"])
    if (root.manifest["source"]["namespace"] != provenance["base_namespace"]
            or any(root.manifest[key] != dto["scope"][key] for key in ("symbol", "timeframe"))):
        raise ValueError("Relação e snapshot pertencem a contextos diferentes.")
    for ident in provenance["lineage_manifests"]:
        if ident == root.manifest_id:
            continue
        origin = datasets.load(db, ident)
        if any(origin.manifest[key] != root.manifest[key] for key in ("source", "symbol", "timeframe", "time")):
            raise ValueError("Origem da relação pertence a outro contexto.")


def save_pair_relation(db, dto, *, transaction_guard=None, transaction_hook=None):
    """Publica resultado e referências juntos; repetição devolve o primeiro recibo."""
    from mt5_extracao.analytics import datasets
    _require_db(db)
    dto, raw, digest, semantic = _relation_payload(dto)
    if transaction_guard is not None:
        transaction_guard(None)
    _relation_sources(db, dto)
    _relation_schema(db)
    provenance, scope = dto["provenance"], dto["scope"]
    ident = provenance["analysis_id"]
    with db.engine.begin() as conn:
        if conn.dialect.name == "sqlite" and not conn.connection.driver_connection.in_transaction:
            conn.exec_driver_sql("BEGIN IMMEDIATE")
        if transaction_guard is not None:
            transaction_guard(conn)
        inserted = conn.execute(text(f"INSERT INTO {RELATIONS_TABLE} "
            "(analysis_id,base_namespace,manifest_id,symbol,tf,as_of_utc,payload_json,payload_sha256,semantic_sha256,created_at_utc) "
            "VALUES (:id,:base,:manifest,:symbol,:tf,:asof,:payload,:digest,:semantic,:created) "
            "ON CONFLICT(analysis_id) DO NOTHING"), dict(id=ident,base=provenance["base_namespace"],
                manifest=provenance["snapshot_manifest_id"],symbol=scope["symbol"],tf=scope["timeframe"],
                asof=provenance["as_of_utc"],payload=raw,digest=digest,semantic=semantic,
                created=datetime.now(timezone.utc).isoformat())).rowcount
        stored = conn.execute(text(f"SELECT * FROM {RELATIONS_TABLE} WHERE analysis_id=:id"), {"id":ident}).mappings().one()
        original = _relation_row(stored)
        if stored["semantic_sha256"] != semantic:
            raise ValueError("Identidade de relação já contém outro resultado; preserve o original e revise a implementação.")
        for manifest_id in provenance["lineage_manifests"]:
            datasets.bind(db, "relation:"+ident+":"+manifest_id, manifest_id, connection=conn)
        if transaction_hook is not None:
            transaction_hook(conn)
        if transaction_guard is not None:
            transaction_guard(conn)
        result = dict(status="INSERTED" if inserted else "UNCHANGED", analysis_id=ident,
            payload_sha256=stored["payload_sha256"], created_at_utc=stored["created_at_utc"], relation=original)
    return result


def load_pair_relation(db, analysis_id):
    """Consulta pelo ID sem recalcular, criar schema ou completar com dados ativos."""
    import re
    _require_db(db)
    if not isinstance(analysis_id, str) or not re.fullmatch(r"[a-f0-9]{64}", analysis_id):
        raise ValueError("analysis_id precisa ser o identificador hexadecimal de64 caracteres.")
    if not inspect(db.engine).has_table(RELATIONS_TABLE):
        raise ValueError("Relação não persistida nesta base; execute relation com --persist.")
    with db.consistent_read() as conn:
        row = conn.execute(text(f"SELECT * FROM {RELATIONS_TABLE} WHERE analysis_id=:id"), {"id":analysis_id}).mappings().first()
        if row is None:
            raise ValueError("Relação não persistida nesta base; confira o identificador e a base.")
        dto = _relation_row(row)
        if not inspect(conn).has_table("_dataset_references"):
            raise ValueError("Referências de origem ausentes; recupere os registros originais.")
        for ident in dto["provenance"]["lineage_manifests"]:
            reference = conn.execute(text("SELECT manifest_id FROM _dataset_references WHERE run_id=:id"),
                {"id":"relation:"+analysis_id+":"+ident}).scalar()
            if reference != ident:
                raise ValueError("Referência de origem da relação sem integridade; recupere o registro original.")
    _relation_sources(db, dto)
    return dto
