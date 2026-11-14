"""Transporte SQLite de séries registradas e do seu histórico de aquisição."""

import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import tempfile
from contextlib import closing

from sqlalchemy import bindparam, text
from sqlalchemy.exc import DBAPIError

from . import actions, files, package_plan, storage

DATABASE = "data.sqlite"
MANIFEST = "manifest.json"
FORMAT = "mt5x.sqlite-market-package.v1"
_MAX_MANIFEST_BYTES = 8 * 1024 * 1024
_MARKET_KINDS = {storage.KIND_BARS, storage.KIND_TICKS, storage.KIND_BOOK}
_CONTROL_FILTERS = {
    "_dataset_origin": None,
    "_metadata": ("key", ("time_basis", "broker_timezone")),
    "_symbol_tables": ("table_name", None),
    "_symbol_specs": ("symbol", None),
    "_extraction_log": ("table_name", None),
    "_ohlcv_versions": ("table_name", None),
    "_ohlcv_receipts": ("table_name", None),
    "_instrument_facts": ("symbol", None),
    "_market_calendar": ("symbol", None),
    "_bars_derived": ("symbol", None),
    "_analytics_source_sequences": ("symbol", None),
}


def _quote(identifier):
    return '"' + identifier.replace('"', '""') + '"'


def _sha_file(path, check):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            check()
            digest.update(chunk)
    return digest.hexdigest()


def _scope(plan):
    if plan["base"] is None:
        raise ValueError("A origem precisa de um namespace BD03 já registrado.")
    if plan["missing_registrations"] or any(
            row["symbol"] in plan["requested_symbols"] for row in plan["orphan_registrations"]):
        raise ValueError("A seleção possui séries ausentes ou órfãs. Confira o inventário da origem.")
    selected = {t["table"] for t in plan["inventory"]["tables"]
                if t["selection"] == "selected" and t["kind"] in _MARKET_KINDS}
    registrations = {t["table"] for t in plan["inventory"]["tables"] if t["selection"] == "selected"}
    if registrations != selected:
        raise ValueError("Há séries selecionadas com uma família de dados ainda sem suporte ao transporte.")
    included = {t["table"]: t for t in plan["inventory"]["tables"]
                if t["table"] in selected or t["table"] in _CONTROL_FILTERS}
    for table in included.values():
        if table["cross_schema_dependencies"] or set(table["declared_dependencies"]) - included.keys():
            raise ValueError(f"Tabela {table['table']} depende de uma origem fora do pacote.")
    return selected, included


def _copy_table(source, target, name, selector, check):
    schema = source.execute(text("SELECT type, name, sql FROM sqlite_schema WHERE tbl_name = :name "
                                 "AND sql IS NOT NULL ORDER BY type = 'table' DESC, name"), {"name": name}).all()
    if (not schema or schema[0].type != "table"
            or not re.match(r"^CREATE\s+TABLE\b", schema[0].sql, re.I)
            or any(row.type not in {"table", "index"} for row in schema)):
        raise ValueError(f"A tabela {name} usa uma estrutura SQLite ainda sem suporte no pacote.")
    target.execute(schema[0].sql)
    columns = [row[1] for row in target.execute(f"PRAGMA table_xinfo({_quote(name)})") if row[6] == 0]
    if not columns:
        raise ValueError(f"A tabela {name} não tem colunas graváveis no pacote.")
    fields = ", ".join(map(_quote, columns))
    query = f"SELECT {fields} FROM {_quote(name)}"
    if selector is None:
        result = source.execute(text(query))
    else:
        column, values = selector
        result = source.execute(text(f"{query} WHERE {_quote(column)} IN :selection")
                                .bindparams(bindparam("selection", expanding=True)), {"selection": tuple(values)})
    insert = f"INSERT INTO {_quote(name)} ({fields}) VALUES ({', '.join('?' for _ in columns)})"
    count = 0
    try:
        for batch in result.partitions(512):
            check()
            target.executemany(insert, (tuple(row) for row in batch))
            count += len(batch)
    finally:
        result.close()
    for index in schema[1:]:
        check()
        target.execute(index.sql)
    return count


def _write_payload(db, symbols, database, check):
    with db.consistent_read() as source:
        # Inventory and export use the same transaction and see the same schema and registry.
        plan = package_plan._inventory(db, source, symbols, check)
        raw_tables, included = _scope(plan)
        with closing(sqlite3.connect(database)) as target, target:
            target.execute("PRAGMA trusted_schema = OFF")
            target.set_progress_handler(lambda: _continue_sql(check), 10000)
            target.execute("BEGIN")
            counts = {}
            for name in sorted(included):
                check()
                selector = _CONTROL_FILTERS.get(name)
                if selector is not None:
                    column, values = selector
                    selector = (column, values or (symbols if column == "symbol" else raw_tables))
                counts[name] = _copy_table(source, target, name, selector, check)
            if target.execute("PRAGMA foreign_key_check").fetchone() is not None:
                raise ValueError("O recorte contém vínculos de dados fora do pacote.")
            if target.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
                raise ValueError("O conteúdo exportado falhou na verificação SQLite.")
            check()
        clock = source.execute(text("SELECT key,value FROM _metadata WHERE key IN ('time_basis','broker_timezone')")) \
            if "_metadata" in included else ()
        return {
            "format": FORMAT,
            "base": plan["base"],
            "symbols": symbols,
            "clock_metadata": dict(row for row in clock),
            "source_inventory_sha256": plan["inventory_sha256"],
            "tables": counts,
            "excluded_tables": sorted({t["table"] for t in plan["inventory"]["tables"]} - included.keys()),
            "excluded_views": [view["view"] for view in plan["inventory"]["views"]],
        }


def _continue_sql(check):
    try:
        check()
    except TimeoutError:
        return 1
    return 0


def _publish(stage, destination):
    if destination.exists() or destination.is_symlink():
        raise FileExistsError("O destino foi criado durante a operação. Confira o resultado antes de repetir.")
    stage.rename(destination)
    if os.name != "nt":
        descriptor = os.open(destination.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def export(db, symbols, destination, *, check):
    """Publica um diretório novo após verificação e sincronização do payload."""
    if db.dialect != "sqlite" or not db.read_only or not db.is_connected():
        raise ValueError("A exportação de séries registradas requer uma origem SQLite somente leitura.")
    symbols = package_plan._selection(symbols)
    destination = Path(destination).absolute()
    parent = destination.parent.resolve(strict=True)
    destination = parent / destination.name
    source_bytes = sum(storage._sqlite_files(db).values())
    if shutil.disk_usage(parent).free < source_bytes * 2:
        raise RuntimeError("Espaço livre insuficiente para preparar e conferir o pacote.")
    with actions.exclusive(parent, "exportar pacote de mercado"):
        if destination.exists() or destination.is_symlink():
            raise FileExistsError("O destino já existe. Escolha um diretório novo para o pacote.")
        stage = Path(tempfile.mkdtemp(prefix=".mt5x-package-", dir=parent))
        try:
            database = stage / DATABASE
            descriptor = os.open(database, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            os.close(descriptor)
            manifest = _write_payload(db, symbols, database, check)
            manifest.update(database_bytes=database.stat().st_size, database_sha256=_sha_file(database, check))
            manifest["package_id"] = package_plan._hash(manifest)
            receipt = stage / MANIFEST
            receipt.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            if receipt.stat().st_size > _MAX_MANIFEST_BYTES:
                raise ValueError("Manifesto do pacote excede o limite de leitura.")
            files._sync_artifact(database)
            files._sync_artifact(receipt)
            check()
            _publish(stage, destination)
        except (DBAPIError, sqlite3.Error) as exc:
            check()
            raise RuntimeError("Falha ao copiar ou verificar as tabelas SQLite selecionadas.") from exc
        finally:
            if stage.is_dir():
                shutil.rmtree(stage)
    return manifest
