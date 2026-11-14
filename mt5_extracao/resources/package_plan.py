"""Inventário/plano de pacotes no pilar Recursos/Dados, sem exportar ou gravar.

Uma base lógica por banco/schema (BD03). A projeção do snapshot não representa
uma cópia integral das tabelas legadas. Toda lacuna continua explícita no plano.
"""
import hashlib
import itertools
import json

from sqlalchemy import inspect, text
from sqlalchemy.exc import DBAPIError

from ..analytics import datasets
from . import storage

MAX_TABLES = 1024
MAX_COLUMNS = 4096
MAX_TOTAL_COLUMNS = 16384
MAX_REGISTRATIONS = 10000
MAX_SYMBOLS = 256
_PLAN_FIELDS = {"schema_version", "kind", "destination", "base", "requested_symbols", "inventory", "inventory_sha256",
                "missing_registrations", "orphan_registrations", "scope", "dependencies", "transfer", "state",
                "export_ready", "reunion_ready", "source_written", "remaining", "plan_sha256"}


def _hash(value):
    return hashlib.sha256(datasets._json(value).encode("utf-8")).hexdigest()


def _selection(symbols):
    if isinstance(symbols, str):
        raise ValueError("Informe uma lista de ativos registrados, sem deduzir pelo nome da tabela.")
    try:
        values=list(itertools.islice(symbols,MAX_SYMBOLS+1))
    except TypeError:
        raise ValueError("Informe uma lista de ativos registrados.") from None
    if (not values or len(values)>MAX_SYMBOLS or any(type(s) is not str or not s or len(s)>128
            or any(ord(c)<32 or ord(c)==127 for c in s) for s in values)):
        raise ValueError("Escolha1…256 identidades de ativos válidas, com até128 caracteres.")
    return sorted(set(values))


def _schema(connection, check):
    inspector=inspect(connection)
    names=sorted(n for n in inspector.get_table_names() if not n.startswith("sqlite_"))
    views=sorted(inspector.get_view_names())
    if len(names)+len(views)>MAX_TABLES:
        raise ValueError("Inventário excede1024 tabelas/views; reduza o escopo pelo owner antes de planejar.")
    tables=[];total_columns=0
    for name in names:
        check()
        columns=inspector.get_columns(name)
        total_columns+=len(columns)
        if len(columns)>MAX_COLUMNS or total_columns>MAX_TOTAL_COLUMNS:
            raise ValueError("Inventário excede4096 colunas/tabela ou16384 no total; preserve a base e confira seu contrato.")
        definition=[]
        for col in columns:
            definition.append(dict(name=col["name"],type=str(col["type"]),nullable=bool(col["nullable"]),
                default_sha256=_hash(col.get("default")),computed_sha256=_hash(col.get("computed"))))
        primary=inspector.get_pk_constraint(name)
        foreign=inspector.get_foreign_keys(name)
        constraints=dict(primary=primary,foreign=foreign,unique=inspector.get_unique_constraints(name),
                         checks=inspector.get_check_constraints(name),indexes=inspector.get_indexes(name))
        dependencies=sorted({fk["referred_table"] for fk in foreign if fk.get("referred_table")})
        tables.append(dict(table=name,kind=storage._kind(name,{c["name"] for c in columns}),columns=definition,
            primary_key=list(primary.get("constrained_columns") or ()),declared_dependencies=dependencies,
            cross_schema_dependencies=sorted({fk["referred_schema"] for fk in foreign if fk.get("referred_schema")
                                             and fk["referred_schema"]!=inspector.default_schema_name}),
            constraints_sha256=_hash(constraints)))
    check()
    return tables,[dict(view=name,definition_sha256=_hash(inspector.get_view_definition(name))) for name in views]


def plan(db, symbols, *, check=lambda:None, statement_timeout_ms=None):
    """Só metadata/registro e identidade existentes; nunca inicializa dataset/DDL.

    Usa consistent_read do DatabaseManager. Na CLI a conexão já é aberta em
    modo somente leitura, inclusive para bases legadas sem tabelas de controle.
    Falha/cancelamento descarta a leitura inteira, sem devolver prefixo aprovado.
    """
    selected=_selection(symbols);check()
    try:
        return _read_plan(db, selected, check, statement_timeout_ms)
    except DBAPIError as exc:
        if getattr(exc.orig, "sqlstate", getattr(exc.orig, "pgcode", None)) == "57014":
            raise TimeoutError("Prazo da consulta PostgreSQL do inventário esgotado.") from exc
        if getattr(exc.orig, "sqlite_errorcode", None) in (5, 6):
            raise RuntimeError("Origem SQLite ocupada durante o inventário. Consulte novamente quando liberar.") from exc
        raise RuntimeError("Falha ao ler o schema ou os registros de origem do inventário.") from exc


def _read_plan(db, selected, check, statement_timeout_ms):
    with db.consistent_read() as connection:
        if connection.dialect.name == "postgresql" and statement_timeout_ms is not None:
            connection.execute(text("SELECT set_config('statement_timeout', :timeout, true)"),
                               {"timeout": str(statement_timeout_ms)})
        return _inventory(db, connection, selected, check)


def _inventory(db, connection, selected, check):
    tables,views=_schema(connection,check)
    by_name={t["table"]:t for t in tables}
    try:
        identity=datasets.identity(db,connection=connection) if "_dataset_origin" in by_name else None
    except KeyError as exc:
        raise ValueError("Identidade BD03 incompleta; preserve a origem e reconcilie pelo owner Dados.") from exc
    if identity is not None and not identity.get("namespace"):
        raise ValueError("Identidade BD03 incompleta; preserve a origem e reconcilie pelo owner Dados.")
    registered=[]
    if "_symbol_tables" in by_name:
        registered=[dict(row) for row in connection.execute(text(
            "SELECT symbol,timeframe,table_name FROM _symbol_tables ORDER BY symbol,timeframe,table_name LIMIT :limit"),
            {"limit":MAX_REGISTRATIONS+1}).mappings()]
        if len(registered)>MAX_REGISTRATIONS:
            raise ValueError("Registro excede10000 séries; escolha recorte validado antes de planejar.")
    by_table={}
    for row in registered:
        check()
        if row["table_name"] in by_table:
            raise ValueError("Tabela registrada para mais de uma identidade; não deduzir posse pelo nome.")
        by_table[row["table_name"]]=row
    missing=[row for row in registered if row["table_name"] not in by_name]
    for table in tables:
        registration=by_table.get(table["table"])
        table["registration"]=registration
        table["selection"]=("selected" if registration["symbol"] in selected else "other_registered_asset") if registration else "requires_owner_scope"
        table["row_count"]=None;table["data_sha256"]=None
        table["clock_basis"]="not_established_by_schema"
    check()
    unavailable=[s for s in selected if not any(r["symbol"]==s and r["table_name"] in by_name for r in registered)]
    schema=dict(tables=tables,views=views)
    payload=dict(schema_version=1,kind="asset_package_reunion_plan",destination="collection_separate_origins",
        base=identity,requested_symbols=selected,inventory=schema,inventory_sha256=_hash(schema),
        missing_registrations=unavailable,orphan_registrations=missing,
        scope="schema and existing identity/registry only; no market rows scanned",
        dependencies="declared foreign keys only; logical producer/consumer dependencies require owner validation",
        transfer=dict(snapshot="datasets.export_snapshot/import_snapshot: selected typed projection; not full raw database",
                      full_sqlite="resources.files.consistent_snapshot: full logical base; not an asset-filtered package"),
        state="planning_only",export_ready=False,reunion_ready=False,source_written=False,
        remaining=["family owner selectors and dependency closure for every table/view",
                   "lossless columns/revisions/clock/source hashes and payload validation",
                   "durable operation, revalidation, budget/cancellation and idempotent QA transport",
                   "owner destination/consent before private-data or external transport"])
    payload["plan_sha256"]=_hash(payload)
    return payload


def validate_plan(value):
    """Conferência do recibo de planejamento, sem transformar metadata em pacote."""
    try:
        clone=json.loads(datasets._json(value))
        valid=set(clone)==_PLAN_FIELDS
        claimed=clone.pop("plan_sha256")
        valid=(valid and claimed==_hash(clone) and type(clone["schema_version"]) is int and clone["schema_version"]==1 and clone["kind"]=="asset_package_reunion_plan"
            and clone["inventory_sha256"]==_hash(clone["inventory"]) and clone["destination"]=="collection_separate_origins"
            and clone["state"]=="planning_only" and clone["export_ready"] is False
            and clone["reunion_ready"] is False and clone["source_written"] is False)
    except (ValueError,TypeError,KeyError):
        valid=False
    if not valid:
        raise ValueError("Plano sem integridade/schema conhecido; preserve a origem e refaça a consulta.")
    return clone|{"plan_sha256":claimed}
