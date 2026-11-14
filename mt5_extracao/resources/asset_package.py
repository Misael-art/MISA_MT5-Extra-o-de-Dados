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
    "_feature_values": ("symbol", None),
    "_dataset_manifests": ("json_extract(payload_json, '$.symbol')", None),
    "_chart_study_notes": ("symbol", None),
    "_preset_revisions": ("symbol", None),
    "_preset_labels": ("symbol", None),
    "_preset_selections": ("symbol", None),
}
_LINKED_FILTERS = {
    "_feature_definitions": ("_feature_values", "(feature_id, algorithm_version)",
                             "SELECT feature_id, algorithm_version FROM _feature_values WHERE symbol IN :selection"),
    "_dataset_blocks": ("_dataset_manifests", "block_id", """
        SELECT blocks.value FROM _dataset_manifests AS manifest
        JOIN json_each(manifest.payload_json, '$.frames') AS frames
        JOIN json_each(frames.value) AS blocks
        WHERE json_extract(manifest.payload_json, '$.symbol') IN :selection
    """),
    "_chart_study_captures": ("_chart_study_notes", "note_id",
                             "SELECT note_id FROM _chart_study_notes WHERE symbol IN :selection"),
    "_chart_study_requests": ("_chart_study_notes", "note_id",
                             "SELECT note_id FROM _chart_study_notes WHERE symbol IN :selection"),
    "_preset_imports": ("_preset_revisions", "(preset_id, revision)",
                        "SELECT preset_id, revision FROM _preset_revisions WHERE symbol IN :selection"),
}
_REQUIRED_TABLES = {"_feature_values": "_feature_definitions", "_dataset_manifests": "_dataset_blocks",
                    "_preset_selections": "_preset_revisions", "_preset_labels": "_preset_revisions",
                    "_dataset_references": "_dataset_manifests"}
_RUN_DEPENDENCIES = ("_native_input_receipts", "_preset_native_runs", "_robot_checks", "_trade_features",
                     "_trade_features_runs", "_trade_trajectories", "_dataset_references")


def _quote(identifier):
    return '"' + identifier.replace('"', '""') + '"'


def _sha_file(path, check):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            check()
            digest.update(chunk)
    return digest.hexdigest()


def _scope(plan, source, check):
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
    inventory = {table["table"]: table for table in plan["inventory"]["tables"]}
    symbols = plan["requested_symbols"]
    run_filters = _run_filters(source, inventory, symbols, check)
    applicable = {name for name, (owner, _, _) in _LINKED_FILTERS.items() if owner in inventory}
    included = {name: table for name, table in inventory.items()
                if name in selected or name in _CONTROL_FILTERS or name in applicable or name in run_filters}
    for name, dependency in _REQUIRED_TABLES.items():
        if name in included and dependency not in included:
            raise ValueError(f"A família {name} requer a tabela de origem {dependency}.")
    for table in included.values():
        if table["cross_schema_dependencies"] or set(table["declared_dependencies"]) - included.keys():
            raise ValueError(f"Tabela {table['table']} depende de uma origem fora do pacote.")
    return selected, included, run_filters


def _run_filters(connection, inventory, symbols, check):
    owners = [table for table in ("_robot_runs", "_trade_ledger") if table in inventory]
    if not owners:
        return {}
    candidates = " UNION ".join(f"SELECT run_id FROM {table} WHERE symbol IN :selection" for table in owners)
    if "_trade_ledger" in inventory:
        mixed = text(f"SELECT 1 FROM _trade_ledger WHERE run_id IN ({candidates}) "
                     "AND (symbol IS NULL OR symbol NOT IN :selection) LIMIT 1")
        if connection.execute(mixed.bindparams(bindparam("selection", expanding=True)), {"selection": symbols}).first():
            raise ValueError("Há execução de vários ativos ou com ativo desconhecido. Exporte o escopo completo da execução.")
    if "_robot_runs" in inventory:
        check()
        mixed = text(f"SELECT 1 FROM _robot_runs WHERE run_id IN ({candidates}) AND symbol NOT IN :selection LIMIT 1")
        if connection.execute(mixed.bindparams(bindparam("selection", expanding=True)), {"selection": symbols}).first():
            raise ValueError("O ativo declarado pela execução diverge do escopo selecionado.")
    return {table: (f"run_id IN ({candidates})", symbols) for table in (*owners, *_RUN_DEPENDENCIES)
            if table in inventory}


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
        clause, values = selector
        result = source.execute(text(f"{query} WHERE {clause}")
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
        raw_tables, included, run_filters = _scope(plan, source, check)
        with closing(sqlite3.connect(database)) as target, target:
            target.execute("PRAGMA trusted_schema = OFF")
            target.set_progress_handler(lambda: _continue_sql(check), 10000)
            target.execute("BEGIN")
            counts = {}
            for name in sorted(included):
                check()
                selector = run_filters.get(name) or _table_selector(name, symbols, raw_tables)
                counts[name] = _copy_table(source, target, name, selector, check)
            _check_links(target, counts)
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


def _table_selector(name, symbols, raw_tables):
    if name in _LINKED_FILTERS:
        _, key, subquery = _LINKED_FILTERS[name]
        return f"{key} IN ({subquery})", symbols
    selector = _CONTROL_FILTERS.get(name)
    if selector is None:
        return None
    column, values = selector
    if column == "table_name":
        values = raw_tables
    elif values is None:
        values = symbols
    return f"{column} IN :selection", values


def _check_links(connection, tables):
    checks = {
        "_dataset_references": """
            SELECT 1 FROM _dataset_references AS ref
            LEFT JOIN _dataset_manifests AS manifest ON ref.manifest_id = manifest.manifest_id
            WHERE manifest.manifest_id IS NULL LIMIT 1
        """,
        "_dataset_manifests": """
            SELECT 1 FROM _dataset_manifests AS manifest
            JOIN json_each(manifest.payload_json, '$.frames') AS frames
            JOIN json_each(frames.value) AS blocks
            LEFT JOIN _dataset_blocks AS data ON data.block_id = blocks.value
            WHERE data.block_id IS NULL LIMIT 1
        """,
        "_preset_selections": """
            SELECT 1 FROM _preset_selections AS choice
            LEFT JOIN _preset_revisions AS preset ON preset.preset_id = choice.preset_id
                AND preset.revision = choice.revision AND preset.robot_key = choice.robot_key
                AND preset.symbol = choice.symbol AND preset.timeframe = choice.timeframe
            WHERE preset.preset_id IS NULL LIMIT 1
        """,
    }
    for table, sql in checks.items():
        if table in tables and connection.execute(sql).fetchone() is not None:
            raise ValueError(f"O pacote tem vínculos incompletos na família {table}.")


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


def inspect_package(directory, *, check):
    directory = Path(directory)
    receipt, database = directory / MANIFEST, directory / DATABASE
    if (directory.is_symlink() or receipt.is_symlink() or database.is_symlink()
            or not receipt.is_file() or not database.is_file()):
        raise ValueError("O pacote precisa conter banco e manifesto locais, sem links.")
    try:
        with receipt.open("rb") as stream:
            raw = stream.read(_MAX_MANIFEST_BYTES + 1)
        if len(raw) > _MAX_MANIFEST_BYTES:
            raise ValueError
        manifest = json.loads(raw)
        fields = {"format", "base", "symbols", "clock_metadata", "source_inventory_sha256", "tables",
                  "excluded_tables", "excluded_views", "database_bytes", "database_sha256", "package_id"}
        if not isinstance(manifest, dict) or manifest.keys() != fields or manifest["format"] != FORMAT:
            raise ValueError
        identity = manifest["base"]
        symbols = package_plan._selection(manifest["symbols"])
        counts = manifest["tables"]
        if (set(identity) != {"namespace", "backend", "lineage", "parent_namespace", "provider", "server"}
                or identity["backend"] != "sqlite" or not isinstance(identity["namespace"], str)
                or not identity["namespace"] or len(identity["namespace"]) > 256
                or symbols != manifest["symbols"] or not isinstance(counts, dict) or not counts
                or any(type(count) is not int or count < 0 for count in counts.values())):
            raise ValueError
        claimed = manifest["package_id"]
        if claimed != package_plan._hash({key: value for key, value in manifest.items() if key != "package_id"}):
            raise ValueError
    except (TypeError, ValueError, KeyError, UnicodeError) as exc:
        raise ValueError("Manifesto de pacote inválido ou alterado.") from exc
    if database.stat().st_size != manifest["database_bytes"] or _sha_file(database, check) != manifest["database_sha256"]:
        raise ValueError("O conteúdo SQLite não corresponde ao manifesto.")
    try:
        with closing(sqlite3.connect(database.absolute().as_uri() + "?mode=ro&immutable=1", uri=True)) as db:
            db.execute("PRAGMA trusted_schema = OFF")
            db.set_progress_handler(lambda: _continue_sql(check), 10000)
            schema = db.execute("SELECT name, type, sql FROM sqlite_schema").fetchall()
            if any(kind not in {"table", "index"} or (kind == "table" and
                   not re.match(r"^CREATE\s+TABLE\b", sql or "", re.I)) for _, kind, sql in schema):
                raise ValueError("O pacote contém objetos SQLite que a coleção não executa.")
            present = {name for name, kind, _ in schema if kind == "table" and not name.startswith("sqlite_")}
            if present != counts.keys() or db.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
                raise ValueError("O schema SQLite não corresponde ao manifesto.")
            if db.execute("PRAGMA foreign_key_check").fetchone() is not None:
                raise ValueError("O pacote tem vínculos SQLite incompletos.")
            _check_links(db, present)
            slots = dict(db.execute("SELECT slot, origin_id FROM _dataset_origin"))
            actual_symbols = {row[0] for row in db.execute("SELECT DISTINCT symbol FROM _symbol_tables")}
            clock = dict(db.execute("SELECT key,value FROM _metadata WHERE key IN ('time_basis','broker_timezone')")) \
                if "_metadata" in present else {}
            if (slots.get("default") != identity["namespace"] or slots.get("provider") != identity["provider"]
                    or slots.get("server") != identity["server"] or slots.get("parent") != identity["parent_namespace"]
                    or identity["lineage"] != ("fork" if slots.get("parent") else "original")
                    or actual_symbols != set(symbols) or clock != manifest["clock_metadata"]):
                raise ValueError("O pacote tem origem ou seleção diferente do manifesto.")
            for table, expected in counts.items():
                check()
                if db.execute(f"SELECT COUNT(*) FROM {_quote(table)}").fetchone()[0] != expected:
                    raise ValueError("A contagem de linhas do pacote difere do manifesto.")
    except sqlite3.Error as exc:
        check()
        raise ValueError("O pacote SQLite não pôde ser verificado.") from exc
    return manifest


def reunion_plan(source, collection, *, check):
    """Lê e valida origem e destino. Um namespace/seleção divergente exige decisão explícita."""
    manifest = inspect_package(source, check=check)
    symbols = set(manifest["symbols"])
    namespace_key = package_plan._hash(manifest["base"]["namespace"])
    selection_key = package_plan._hash(manifest["symbols"])
    collection = Path(collection).resolve(strict=True)
    if not collection.is_dir():
        raise ValueError("A coleção precisa ser um diretório local existente.")
    target = collection / f"{namespace_key}-{selection_key}"
    status = "ready"
    if target.exists() or target.is_symlink():
        existing = inspect_package(target, check=check)
        status = "already_present" if existing == manifest else "conflict"
    else:
        for sibling in collection.glob(f"{namespace_key}-*"):
            check()
            existing = inspect_package(sibling, check=check)
            if existing["base"] != manifest["base"] or existing["clock_metadata"] != manifest["clock_metadata"]:
                status = "conflict"
                break
            if symbols.intersection(existing["symbols"]):
                status = "conflict"
                break
    return {"state": status, "package_id": manifest["package_id"], "target": str(target),
            "namespace": manifest["base"]["namespace"], "symbols": manifest["symbols"]}


def reunite(source, collection, *, check):
    collection = Path(collection).resolve(strict=True)
    with actions.exclusive(collection, "reunir pacote de mercado"):
        plan = reunion_plan(source, collection, check=check)
        if plan["state"] == "conflict":
            raise ValueError("A coleção já contém conteúdo diferente ou sobreposto para a mesma origem.")
        if plan["state"] == "already_present":
            return plan
        if shutil.disk_usage(collection).free < (Path(source) / DATABASE).stat().st_size * 2:
            raise RuntimeError("Espaço livre insuficiente para incorporar e conferir o pacote.")
        target = Path(plan["target"])
        stage = Path(tempfile.mkdtemp(prefix=".mt5x-package-", dir=collection))
        try:
            for name in (MANIFEST, DATABASE):
                with (Path(source) / name).open("rb") as src, (stage / name).open("xb") as dst:
                    for chunk in iter(lambda: src.read(1024 * 1024), b""):
                        check()
                        dst.write(chunk)
                files._sync_artifact(stage / name)
            manifest = inspect_package(stage, check=check)
            if manifest["package_id"] != plan["package_id"]:
                raise ValueError("O conteúdo da origem mudou após o plano de reunião.")
            _publish(stage, target)
        finally:
            if stage.is_dir():
                shutil.rmtree(stage)
    return {**plan, "state": "imported"}
