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
from sqlalchemy import bindparam, inspect, text

DEFINITIONS_TABLE = "_feature_definitions"
VALUES_TABLE = "_feature_values"
# Fronteira transacional (V4-02): um contador de linha única serializa as transações de escrita do store;
# a geração reservada dentro da transação só fica visível no commit, então "geração <= G confirmada"
# é exatamente o conjunto de revisões já confirmadas quando o leitor leu G.
CLOCK_TABLE = "_feature_store_clock"
GENERATIONS_TABLE = "_feature_store_generations"

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
        conn.execute(text(_CREATE_VALUES))
        value_columns = {column["name"] for column in inspect(conn).get_columns(VALUES_TABLE)}
        if "recorded_at_utc" not in value_columns:
            # Relógio do escritor ao gravar a revisão (informativo); NULL = fato anterior ao contrato.
            conn.execute(text(f"ALTER TABLE {VALUES_TABLE} ADD COLUMN recorded_at_utc TEXT"))
        if "generation" not in value_columns:
            # Geração da transação que gravou a revisão; NULL = legado, visibilidade desconhecida.
            conn.execute(text(f"ALTER TABLE {VALUES_TABLE} ADD COLUMN generation BIGINT"))
        conn.execute(text(f"CREATE TABLE IF NOT EXISTS {CLOCK_TABLE} (id INTEGER PRIMARY KEY, generation BIGINT NOT NULL)"))
        conn.execute(text(f"INSERT INTO {CLOCK_TABLE} (id, generation) VALUES (1, 0) ON CONFLICT (id) DO NOTHING"))
        if "knowledge_frontier_utc" not in {c["name"] for c in inspect(conn).get_columns(CLOCK_TABLE)}:
            # Maior known_at já CONFIRMADO; carimbos pendentes terão horário maior (V5-01).
            conn.execute(text(f"ALTER TABLE {CLOCK_TABLE} ADD COLUMN knowledge_frontier_utc TEXT"))
        conn.execute(text(f"CREATE TABLE IF NOT EXISTS {GENERATIONS_TABLE} (generation BIGINT PRIMARY KEY, "
                          "started_at_utc TEXT NOT NULL, committed_at_utc TEXT)"))
        conn.execute(text(_CREATE_ASOF_INDEX))
        conn.execute(text(_CREATE_KEY_INDEX))
        conn.execute(text(_CREATE_SOURCE_INDEX))


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
    for name in ("family", "formula", "unit", "warmup_unit", "source_id", "timeframe", "availability_rule",
                 "value_type"):
        value = metadata[name]
        if value is not None:
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} precisa ser texto não vazio quando informado.")
            metadata[name] = value.strip()
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
            f"timeframe, availability_rule, value_type FROM {DEFINITIONS_TABLE} "
            "WHERE feature_id = :feature_id AND algorithm_version = :version"),
            {"feature_id": feature_id, "version": algorithm_version}).mappings().first()
        if existing:
            existing_metadata = {key: existing[key] for key in metadata}
            if (existing["name"], existing["parameters_json"], existing["dependencies_json"], existing_metadata) == (
                    name, parameters_json, dependencies_json, metadata):
                return False
            raise ValueError("A definição já existe com este algoritmo; use uma nova versão do algoritmo.")
        conn.execute(text(
            f"INSERT INTO {DEFINITIONS_TABLE} "
            "(feature_id, algorithm_version, name, parameters_json, dependencies_json, registered_at_utc, family, "
            "formula, unit, warmup_count, warmup_unit, source_id, timeframe, availability_rule, value_type) "
            "VALUES (:feature_id, :version, :name, :parameters, :dependencies, :registered_at, :family, :formula, "
            ":unit, :warmup_count, :warmup_unit, :source_id, :timeframe, :availability_rule, :value_type)"),
            {"feature_id": feature_id, "version": algorithm_version, "name": name,
             "parameters": parameters_json, "dependencies": dependencies_json, "registered_at": registered_at,
             **metadata})
    return True


def list_definitions(db):
    """Devolve todas as versões registradas, com JSON já decodificado."""
    if not _usable(db) or not inspect(db.engine).has_table(DEFINITIONS_TABLE):
        return []
    columns = {column["name"] for column in inspect(db.engine).get_columns(DEFINITIONS_TABLE)}
    metadata_columns = ", ".join(name if name in columns else f"NULL AS {name}"
                                  for name in _DEFINITION_METADATA_COLUMNS)
    with db.engine.connect() as conn:
        rows = conn.execute(text(
            f"SELECT feature_id, algorithm_version, name, parameters_json, dependencies_json, registered_at_utc, "
            f"{metadata_columns} "
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


_INSERT_VALUES = (f"INSERT INTO {VALUES_TABLE} "
                  "(symbol, tf, observation_time, available_at_utc, observed_at_utc, valid_until_utc, feature_id, "
                  "algorithm_version, numeric_value, categorical_value, quality, source_revision, revision, value_hash, "
                  "metadata_json, recorded_at_utc, generation) VALUES (:symbol, :tf, :observation_time, "
                  ":available_at_utc, :observed_at_utc, :valid_until_utc, :feature_id, :algorithm_version, "
                  ":numeric_value, :categorical_value, :quality, :source_revision, :revision, :value_hash, "
                  ":metadata_json, :recorded_at_utc, :generation)")


def _insert_revisions(conn, rows):
    """Único caminho de gravação de revisões (save_value e save_values): reserva a geração da transação
    (bloqueando outros escritores até o commit) e grava relógio e geração em todas as linhas."""
    if not rows:
        return None
    conn.execute(text(f"UPDATE {CLOCK_TABLE} SET generation = generation + 1 WHERE id = 1"))
    generation = int(conn.execute(text(f"SELECT generation FROM {CLOCK_TABLE} WHERE id = 1")).scalar_one())
    recorded = _utc_text(datetime.now(timezone.utc), "recorded_at")
    conn.execute(text(f"INSERT INTO {GENERATIONS_TABLE} (generation, started_at_utc) VALUES (:g, :t)"),
                 {"g": generation, "t": recorded})
    for row in rows:
        row["recorded_at_utc"], row["generation"] = recorded, generation
    conn.execute(text(_INSERT_VALUES), rows)
    return generation


def _lock_store(conn):
    """Exclusão de escrita do store, ANTES de ler revisões vigentes (V5-02). SQLite: BEGIN IMMEDIATE;
    PostgreSQL: trava a linha do contador até o commit. Leitores não são bloqueados."""
    if conn.dialect.name == "sqlite":
        if not conn.connection.driver_connection.in_transaction:
            conn.exec_driver_sql("BEGIN IMMEDIATE")
    else:
        conn.execute(text(f"SELECT generation FROM {CLOCK_TABLE} WHERE id = 1 FOR UPDATE"))


def _stamp_committed(db, generation):
    """Depois do commit dos dados: carimba quando a geração passou a ser CONHECIDA.

    O carimbo é outra transação sob a mesma exclusão do contador: o horário é calculado já com o
    bloqueio, é sempre posterior à fronteira confirmada anterior, e a fronteira avança na mesma
    transação. Assim, enquanto este carimbo não confirma, todo leitor vê a fronteira antiga e o
    carimbo pendente fica acima dela; depois de confirmado, nenhum corte <= fronteira antiga muda.
    Também carimba (agora, nunca antes) gerações confirmadas cujo carimbo se perdeu. Leitores
    nunca carimbam.
    """
    if generation is None:
        return
    with db.engine.begin() as conn:
        _lock_store(conn)
        frontier = conn.execute(text(f"SELECT knowledge_frontier_utc FROM {CLOCK_TABLE} WHERE id = 1")).scalar()
        now = datetime.now(timezone.utc)
        if frontier is not None:
            now = max(now, pd.Timestamp(frontier).to_pydatetime() + pd.Timedelta(microseconds=1))
        stamp = _utc_text(now, "committed_at")
        conn.execute(text(f"UPDATE {GENERATIONS_TABLE} SET committed_at_utc = :t "
                          "WHERE committed_at_utc IS NULL AND generation <= :g"), {"t": stamp, "g": generation})
        conn.execute(text(f"UPDATE {CLOCK_TABLE} SET knowledge_frontier_utc = :t WHERE id = 1"), {"t": stamp})


def store_snapshot(db):
    """Fotografia de leitura: última geração confirmada e o instante da leitura (sem escrever)."""
    _require_db(db)
    with db.engine.connect() as conn:
        row = None
        if inspect(conn).has_table(CLOCK_TABLE):
            columns = {c["name"] for c in inspect(conn).get_columns(CLOCK_TABLE)}
            frontier = "knowledge_frontier_utc" if "knowledge_frontier_utc" in columns else "NULL"
            row = conn.execute(text(f"SELECT generation, {frontier} FROM {CLOCK_TABLE} WHERE id = 1")).first()
    generation, frontier = (row[0], row[1]) if row else (0, None)
    return {"generation": int(generation or 0), "taken_at": _utc_text(datetime.now(timezone.utc), "snapshot"),
            "knowledge_frontier": frontier}


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
        _lock_store(conn)  # revisão vigente, idempotência, número e INSERT sob a mesma exclusão
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
        generation = _insert_revisions(conn, [dict(row, revision=revision)])
    _stamp_committed(db, generation)
    return revision


def _empty_values():
    return pd.DataFrame(columns=VALUE_COLUMNS)


BASES = ("research", "knowledge")


def query_as_of(db, as_of, *, symbol=None, timeframe=None, feature_id=None, algorithm_version=None,
                start=None, end=None, basis="research", generation_through=None, knowledge_through=None,
                knowledge_frontier=None, with_recorded=False):
    """Consulta a última revisão disponível em ``as_of`` para cada instante observado.

    ``basis='research'`` (padrão, comportamento histórico) usa a disponibilidade declarada pelo
    produtor: uma reconstrução retrospectiva aparece como disponível no fechamento da barra.
    ``basis='knowledge'`` exige que a revisão tenha carimbo de conhecimento confirmado com
    ``known_at <= min(as_of, knowledge_through, knowledge_frontier)``. ``known_at`` é o horário do
    carimbo, tomado sob a exclusão do contador DEPOIS do commit dos dados (nunca antecipa a
    visibilidade); a fronteira é o maior carimbo já confirmado, de modo que um carimbo pendente
    nunca altera um corte já respondido. Legado sem geração fica de fora.
    ``generation_through`` fixa uma fotografia transacional (``store_snapshot``): revisões de
    gerações posteriores — confirmadas depois da 1ª página — não entram.
    ``start`` é inclusivo e ``end`` exclusivo; ambos filtram ``observation_time``.
    """
    _require_db(db)
    as_of_utc = _utc_text(as_of, "as_of")
    if basis not in BASES:
        raise ValueError("basis deve ser 'research' ou 'knowledge'.")
    if not inspect(db.engine).has_table(VALUES_TABLE):
        return _empty_values()
    columns = {c["name"] for c in inspect(db.engine).get_columns(VALUES_TABLE)}
    contract = "generation" in columns and inspect(db.engine).has_table(GENERATIONS_TABLE)
    where = ["v.available_at_utc <= :as_of"]
    params = {"as_of": as_of_utc}
    for name, column in (("symbol", "symbol"), ("timeframe", "tf"), ("feature_id", "feature_id"),
                         ("algorithm_version", "algorithm_version")):
        value = locals()[name]
        if value is not None:
            where.append(f"v.{column} = :{name}")
            params[name] = str(value)
    if start is not None:
        where.append("v.observation_time >= :start")
        params["start"] = _utc_text(start, "start")
    if end is not None:
        where.append("v.observation_time < :end")
        params["end"] = _utc_text(end, "end")
    if generation_through is not None and contract:
        where.append("(v.generation IS NULL OR v.generation <= :generation_through)")
        params["generation_through"] = int(generation_through)
    if basis == "knowledge":
        if not contract:
            return _empty_values()
        bounds = [as_of_utc]
        if knowledge_through is not None:
            bounds.append(_utc_text(knowledge_through, "knowledge_through"))
        # Sem fronteira explícita, usa a confirmada agora: carimbos pendentes ficam acima dela (V5-01).
        frontier = knowledge_frontier if knowledge_frontier is not None else store_snapshot(db)["knowledge_frontier"]
        if frontier is None:
            return _empty_values()
        bounds.append(_utc_text(frontier, "knowledge_frontier"))
        cut = min(bounds)
        where.append("v.generation IS NOT NULL AND g.committed_at_utc IS NOT NULL AND g.committed_at_utc <= :known_cut")
        params["known_cut"] = cut
    join = f"LEFT JOIN {GENERATIONS_TABLE} g ON g.generation = v.generation" if contract else ""
    extra = (["v.recorded_at_utc", "v.generation", "g.committed_at_utc AS known_at_utc"]
             if with_recorded and contract else [])
    selected = [f"v.{name}" for name in VALUE_COLUMNS] + extra
    names = list(VALUE_COLUMNS) + (["recorded_at_utc", "generation", "known_at_utc"] if extra else [])
    sql = f"""
        WITH eligible AS (
            SELECT {', '.join(selected)},
                   ROW_NUMBER() OVER (
                       PARTITION BY v.symbol, v.tf, v.observation_time, v.feature_id, v.algorithm_version
                       ORDER BY v.available_at_utc DESC, v.observed_at_utc DESC, v.revision DESC
                   ) AS point_rank
            FROM {VALUES_TABLE} v {join}
            WHERE {' AND '.join(where)}
        )
        SELECT {', '.join(names)} FROM eligible
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


def save_values(db, records, *, transaction_hook=None):
    """Grava muitos valores numa única transação, com as mesmas regras de ``save_value``.

    ``records`` é uma sequência de dicionários com os argumentos de ``save_value``
    (``symbol``, ``timeframe``, ``feature_id``, ``algorithm_version``,
    ``observation_time``, ``value`` e os opcionais). Tudo é validado antes da
    primeira escrita; um erro não deixa gravação parcial. Devolve a contagem de
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
    if not rows and transaction_hook is None:
        return counts
    ensure_schema(db)
    generation = None
    with db.engine.begin() as conn:
        # Seleção da revisão e inserção pertencem à mesma exclusão de escrita.
        _lock_store(conn)
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
            generation = _insert_revisions(conn, inserts)
        if transaction_hook is not None:
            transaction_hook(conn, counts)
    _stamp_committed(db, generation)
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
    for name in ("observation_time", "available_at_utc", "valid_until_utc"):
        history[name] = pd.to_datetime(history[name].map(
            lambda value: pd.NaT if name == "valid_until_utc" and pd.isna(value)
            else _utc_timestamp(value, name)), utc=True)
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
