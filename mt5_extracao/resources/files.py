"""
Cópias de segurança da base e limpeza de arquivos antigos (T15.7).

- backup: cópia consistente do SQLite pela API de backup (funciona com a base em uso), conferida com
  `PRAGMA quick_check`; guarda as `keep` mais novas. PostgreSQL: recusa e indica o pg_dump.
- old_files / clean_files: arquivos de logs e exportações mais velhos que N dias. O log em uso nunca entra.

Tudo sob a trava de actions.exclusive e registrado em _resource_actions.
"""
import logging
import json
import os
import sqlite3
import hashlib
import re
import uuid
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

from mt5_extracao.resources import actions, storage
from mt5_extracao.resources import policy as policy_mod

log = logging.getLogger(__name__)

BACKUP_SUFFIX = ".db"
ACTIVE_LOGS = {"mt5_extracao.log", "mt5_bridge.log", "mt5_bridge.pid", "mt5_bridge_process.json", "mt5_bridge.lock"}


def backup_dir(db, policy) -> Path:
    if policy.backup_dir:
        return Path(os.path.expanduser(policy.backup_dir))
    if getattr(db, "db_type", None) == "sqlite" and getattr(db, "db_path", None):
        return Path(db.db_path).resolve().parent / "copias"
    return Path("database") / "copias"


def list_backups(db, config) -> List[dict]:
    """Cópias existentes, a mais nova primeiro: [{path, bytes, created_at}]."""
    folder = backup_dir(db, policy_mod.load(config))
    if not folder.is_dir():
        return []
    items = [{"path": str(p), "bytes": p.stat().st_size, "created_at": datetime.fromtimestamp(p.stat().st_mtime)}
             for p in folder.glob(f"*{BACKUP_SUFFIX}") if p.is_file()]
    return sorted(items, key=lambda i: i["created_at"], reverse=True)


def last_backup(db, config) -> Optional[datetime]:
    items = list_backups(db, config)
    return items[0]["created_at"] if items else None


def backup_due(db, config, now: Optional[datetime] = None) -> bool:
    policy = policy_mod.load(config)
    last = last_backup(db, config)
    return last is None or (now or datetime.now()) - last >= timedelta(hours=policy.backup_interval_hours)


def consistent_snapshot(source, target) -> None:
    """Snapshot SQLite somente leitura na origem; destino novo, conferido, sem sobrescrita."""
    source, target = Path(source).resolve(), Path(target)
    descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.close(descriptor)
    try:
        with closing(sqlite3.connect(source.as_uri() + "?mode=ro", uri=True, timeout=30)) as src:
            dst = sqlite3.connect(str(target))
            try:
                src.backup(dst, pages=4096)
                if dst.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
                    raise ValueError("A cópia SQLite não passou no quick_check.")
            finally:
                dst.close()
    except BaseException:
        target.unlink(missing_ok=True)
        raise


def _sync_artifact(path):
    """Confirma o arquivo próprio e, no POSIX, sua entrada no diretório."""
    path = Path(path)
    with path.open("r+b") as stream:
        os.fsync(stream.fileno())
    if os.name != "nt":
        descriptor = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def backup(db, config, origin: str = "ui", prune: bool = True) -> dict:
    """Cria uma cópia conferida e aplica a retenção às cópias elegíveis. Nunca lança."""
    policy = policy_mod.load(config)
    result = {"ok": False, "path": None, "bytes": None, "pruned": [], "error": None}
    if db.dialect != "sqlite":
        result["error"] = ("cópia automática só para SQLite; no PostgreSQL use pg_dump "
                           "(ex.: pg_dump -Fc banco > copia.dump)")
        return result
    try:
        with actions.exclusive(policy_mod.archive_dir(db, policy), f"cópia ({origin})"):
            return _backup_locked(db, config, origin, prune)
    except actions.Busy as error:
        result["error"] = str(error)
    except Exception as error:
        log.error("Falha na cópia da base: %s", error)
        result["error"] = str(error)
        actions.audit(db, "copiar", None, origin, False, detail={"error": str(error)})
    return result


def _backup_locked(db, config, origin, prune, target=None):
    """Mesmo produtor de cópias, chamado com o lease de Recursos já adquirido."""
    policy = policy_mod.load(config)
    folder = backup_dir(db, policy)
    result = {"ok": False, "path": None, "bytes": None, "pruned": [], "error": None}
    folder.mkdir(mode=0o700, parents=True, exist_ok=True)
    source = Path(db.db_path).resolve()
    size = sum(storage._sqlite_files(db).values())
    disk = storage.disk_usage(folder)
    if disk and disk["free"] < size * 1.1:
        raise RuntimeError(f"disco livre ({storage.human_bytes(disk['free'])}) menor que o tamanho da base "
                           f"({storage.human_bytes(size)}); a cópia não foi feita")
    if target is None:
        stamp = f"{source.stem}__{datetime.now():%Y%m%d-%H%M%S}"
        target, n = folder / f"{stamp}{BACKUP_SUFFIX}", 1
        while target.exists():
            target, n = folder / f"{stamp}-{n}{BACKUP_SUFFIX}", n + 1
    elif target.exists():
        raise FileExistsError("A cópia desta operação já existe; consulte seu resultado.")
    partial = target.with_suffix(".parcial")
    consistent_snapshot(source, partial)
    partial.replace(target)
    _sync_artifact(target)
    result.update(ok=True, path=str(target), bytes=target.stat().st_size)
    for old in (list_backups(db, config)[policy.backup_keep:] if prune else []):
        try:
            if _retain_restore_copy(Path(old["path"])):
                continue
            Path(old["path"]).unlink()
            result["pruned"].append(old["path"])
        except OSError as e:
            log.warning(f"Cópia antiga não apagada ({old['path']}): {e}")
    actions.audit(db, "copiar", target.name, origin, True, detail={"bytes": result["bytes"],
                                                                   "pruned": result["pruned"]})
    log.info(f"Cópia da base criada: {target}")
    return result


# --- arquivos antigos ----------------------------------------------------------------------

def _folders(db, policy) -> dict:
    """{rótulo: (pasta, dias)} das pastas com limpeza ligada (dias > 0)."""
    out = {}
    if policy.logs_keep_days:
        out["Logs"] = (storage.EXTRA_DIRS["Logs"](db, None), policy.logs_keep_days)
    if policy.exports_keep_days:
        out["Exportações"] = (storage.EXTRA_DIRS["Exportações"](db, None), policy.exports_keep_days)
    return out


def _protected_artifacts(db, config, *, check=lambda: None):
    """Referências dos donos existentes; não carrega curvas/trades nem cria catálogo."""
    from sqlalchemy import inspect, text
    files, directories = set(), set()
    def add(value, directory=False):
        if value is None or value == "":
            return
        if not isinstance(value, str):
            raise ValueError("Referência de artefato inválida; confira os registros antes de limpar arquivos.")
        path = Path(value).expanduser().resolve()
        (directories if directory else files).add(path)
    for path in actions.control_lock_paths(policy_mod.archive_dir(db, policy_mod.load(config))):
        add(str(path))
    if config is not None:
        if config.get("MT5", "path", fallback="").strip():
            from mt5_extracao.robots import mt5_tools
            add(str(mt5_tools.install_dir(config)), directory=True)
        for value in config.get("ROBOTS", "library_dirs", fallback="").split(";"):
            if value.strip():
                add(value.strip(), directory=True)
    if db is None or getattr(db, "engine", None) is None:
        return files, directories
    with db.engine.connect() as conn:
        if inspect(conn).has_table("_robot_runs"):
            columns = {row["name"] for row in inspect(conn).get_columns("_robot_runs")}
            if "metrics_json" in columns:
                for (raw,) in conn.execute(text("SELECT metrics_json FROM _robot_runs")):
                    check()
                    try:
                        metrics = json.loads(raw or "{}")
                    except (TypeError, ValueError):
                        raise ValueError("Metadados de execução ilegíveis; restaure/confira a base antes de limpar arquivos.") from None
                    if not isinstance(metrics, dict):
                        raise ValueError("Metadados de execução inválidos; confira a base antes de limpar arquivos.")
                    for field in ("report", "tester_log", "artifact_report", "trades_csv"):
                        add(metrics.get(field))
                    add(metrics.get("artifact_directory"), directory=True)
                    artifacts = metrics.get("artifact_files", [])
                    if not isinstance(artifacts, list):
                        raise ValueError("Lista de artefatos inválida; confira a execução antes de limpar arquivos.")
                    for value in artifacts:
                        add(value)
        if inspect(conn).has_table("_robots"):
            columns = {row["name"] for row in inspect(conn).get_columns("_robots")}
            if "path" in columns:
                for (value,) in conn.execute(text("SELECT path FROM _robots")):
                    if value:
                        path = Path(value).expanduser()
                        add(str(path), directory=path.is_dir())
    from mt5_extracao.robots import budget
    for reference in budget.retained_artifacts(db, check=check):
        for path in reference["files"]:
            add(path)
        for path in reference["directories"]:
            add(path, directory=True)
    # Sessões antigas já têm relatorio.json na pasta privada, mesmo sem campo novo.
    for path in tuple(files):
        parent = path.parent
        if (parent / "relatorio.json").is_file() and (parent / "terminal64.exe").is_file():
            directories.add(parent)
    return files, directories


def _artifact_protected(path, files, directories):
    resolved = path.resolve()
    if resolved in files or any(resolved == root or root in resolved.parents for root in directories):
        return True
    # Uma sessão sem referência SQL ainda pode estar preparando/rodando. Sem
    # prova de posse/estado não aplicar a limpeza genérica dentro da instalação.
    for parent in path.parents:
        if parent.name in ("robot_sessions", "mt5x_sessions"):
            return True
    return False


def old_files(db, config, now: Optional[datetime] = None, *, check=None) -> List[dict]:
    """Arquivos que a limpeza apagaria: [{path, folder, bytes, modified}]. O log em uso fica de fora."""
    policy = policy_mod.load(config)
    now = now or datetime.now()
    found = []
    if check is not None:
        check()
    protected_files, protected_dirs = _protected_artifacts(db, config, check=check or (lambda: None))
    for label, (folder, days) in _folders(db, policy).items():
        if check is not None:
            check()
        folder = Path(folder)
        if not folder.is_dir():
            continue
        cutoff = now - timedelta(days=days)
        for root, _dirs, names in os.walk(folder):
            if check is not None:
                check()
            for name in names:
                if check is not None:
                    check()
                if label == "Logs" and name in ACTIVE_LOGS:
                    continue
                path = Path(root) / name
                try:
                    stat = path.stat()
                except OSError:
                    continue
                modified = datetime.fromtimestamp(stat.st_mtime)
                if modified < cutoff:
                    # Arquivo recente não é candidato. Resolver referências só
                    # após o corte evita percorrer seus ancestrais sem necessidade.
                    if _artifact_protected(path, protected_files, protected_dirs):
                        continue
                    found.append({"path": str(path), "folder": label, "bytes": stat.st_size, "modified": modified})
    return sorted(found, key=lambda f: f["modified"])


def clean_files(db, config, origin: str = "ui", now: Optional[datetime] = None) -> dict:
    policy = policy_mod.load(config)
    result = {"ok": True, "removed": 0, "bytes": 0, "errors": []}
    with actions.exclusive(policy_mod.archive_dir(db, policy), f"arquivos ({origin})"):
        for item in old_files(db, config, now):
            try:
                Path(item["path"]).unlink()
                result["removed"] += 1
                result["bytes"] += item["bytes"]
            except OSError as e:
                result["errors"].append(f"{item['path']}: {e}")
        if result["removed"] or result["errors"]:
            actions.audit(db, "arquivos", None, origin, not result["errors"],
                          detail={"removed": result["removed"], "bytes": result["bytes"], "errors": result["errors"]})
    result["ok"] = not result["errors"]
    return result


# --- voltar a uma cópia ----------------------------------------------------------------------

def _restore_inventory(conn):
    """Schema e contagens da fotografia mantida aberta durante a restauração."""
    schema = conn.execute("SELECT type, name, tbl_name, sql FROM sqlite_master "
                          "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name").fetchall()
    tables = [row[1] for row in schema if row[0] == "table"]
    if not tables:
        raise ValueError("A cópia não contém tabelas; escolha uma cópia válida da base.")
    counts = {name: conn.execute('SELECT COUNT(*) FROM "' + name.replace('"', '""') + '"').fetchone()[0]
              for name in tables}
    return schema, counts


_RESTORE_SCHEMA = "mt5x-restore/v1"
_RESTORE_STATES = {"prepared", "applying", "succeeded", "failed", "uncertain", "recovered"}


def _restore_id(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{32}", value):
        raise ValueError("ID de restauração inválido; consulte resources restore-status.")
    return value


def _restore_manifest_path(folder, owner_key, operation_id):
    return Path(folder) / f"restore__{owner_key}_{_restore_id(operation_id)}.restore.json"


def _restore_control_dir():
    shared, _local = actions.control_lock_paths(Path("."))
    return shared.parent / "restore_receipts"


def _load_restore(path):
    path = Path(path)
    if path.is_symlink() or path.stat().st_size > 256 * 1024:
        raise ValueError("Manifesto de restauração indisponível; preserve a cópia e confira a operação.")
    try:
        record = json.loads(path.read_text(encoding="utf-8"))
        json.dumps(record, allow_nan=False)
        datetime.fromisoformat(record["created_at"])
        datetime.fromisoformat(record["updated_at"])
        owner_key = record["owner_key"]
        operation_id = _restore_id(record["operation_id"])
        expected = _restore_manifest_path(path.parent, owner_key, operation_id)
        safety = Path(record["safety_copy"])
        safety_name = path.name.removesuffix(".restore.json") + ".db"
        valid = (record["schema"] == _RESTORE_SCHEMA and isinstance(owner_key, str)
                 and re.fullmatch(r"[0-9a-f]{64}", owner_key)
                 and path == expected and record["state"] in _RESTORE_STATES
                 and safety.is_absolute() and safety.name == safety_name
                 and record["safety_copy"] == str(safety.resolve())
                 and isinstance(record["source"], str) and Path(record["source"]).is_absolute()
                 and isinstance(record["active_path"], str) and Path(record["active_path"]).is_absolute()
                 and isinstance(record["created_at"], str)
                 and isinstance(record["result"], dict)
                 and isinstance(record["result"].get("ok"), bool)
                 and record["result"].get("operation_id") == operation_id
                 and record["result"].get("state") == record["state"]
                 and record["result"]["ok"] == (record["state"] == "succeeded"))
        if record["state"] in {"applying", "succeeded", "uncertain", "recovered"}:
            valid = valid and isinstance(record.get("safety_sha256"), str) and re.fullmatch(
                r"[0-9a-f]{64}", record["safety_sha256"])
        if record.get("recovery_of") is not None:
            _restore_id(record["recovery_of"])
        if record.get("recovery_operation_id") is not None:
            _restore_id(record["recovery_operation_id"])
        if not valid:
            raise ValueError
        return record
    except (KeyError, TypeError, ValueError, AttributeError):
        raise ValueError("Manifesto de restauração ilegível ou com schema desconhecido; confira a operação.") from None


def _save_restore(path, record):
    from mt5_extracao.bootstrap.config_builder import atomic_write_text
    path = Path(path)
    if path.is_symlink():
        raise ValueError("Manifesto de restauração contém link; preserve a operação.")
    record["updated_at"] = datetime.now(timezone.utc).isoformat()
    atomic_write_text(path, json.dumps(record, ensure_ascii=False, allow_nan=False), private=True)
    _sync_artifact(path)


def _restore_records(db, config):
    """Lê e reconcilia recibos sob o lease comum de Recursos."""
    owner_key = session_owner_key(db, physical_only=True)
    if not owner_key:
        raise ValueError("A identidade da base não está disponível; preserve a operação.")
    folder = backup_dir(db, policy_mod.load(config)).resolve()
    control = _restore_control_dir()
    records = []
    active = Path(db.db_path).resolve()
    for path in control.glob("restore__*.restore.json"):
        record = _load_restore(path)
        original = Path(record["active_path"])
        pending = record["state"] in {"prepared", "applying", "uncertain"}
        if record["owner_key"] != owner_key:
            if pending and os.path.normcase(str(original)) == os.path.normcase(str(active)):
                raise ValueError(f"A identidade da base mudou com a restauração {record['operation_id']} pendente; "
                                 "preserve os recibos e confira a operação.")
            continue
        if not pending or (original.is_file() and original.samefile(active)):
            records.append(record)
        elif pending:
            raise ValueError(f"A localização original da restauração {record['operation_id']} não confirma sua identidade; "
                             "preserve os recibos e confira a operação.")
    for path in folder.glob(f"restore__{owner_key}_*.db"):
        if not control.joinpath(path.with_suffix(".restore.json").name).is_file():
            raise ValueError("Cópia de restauração sem manifesto; confira seu resultado antes de outra troca.")
    for record in records:
        if record["state"] == "prepared":
            # O lease foi obtido e o marco applying nunca foi confirmado. O
            # produtor só toca a base depois de persistir esse segundo marco.
            record["state"] = "failed"
            record["result"].update(state="failed", error="Preparo interrompido antes da troca da base.")
            _save_restore(_restore_manifest_path(control, owner_key, record["operation_id"]), record)
    recovered = {r["recovery_of"]: r["operation_id"] for r in records
                 if r["state"] == "succeeded" and r.get("recovery_of")}
    for record in records:
        if record["operation_id"] in recovered:
            record["state"] = "recovered"
            record["recovery_operation_id"] = recovered[record["operation_id"]]
    return owner_key, folder, sorted(records, key=lambda record: record["created_at"], reverse=True)


def _restore_outcome(record):
    """Chamado sob o lease que exclui uma troca ainda em execução."""
    state = record["state"]
    if state in {"prepared", "applying"}:
        state = "uncertain"
    return {**record["result"], "operation_id": record["operation_id"], "state": state,
            "source": record["source"], "safety_copy": record["safety_copy"],
            "created_at": record["created_at"], "updated_at": record.get("updated_at"),
            "receipt_path": str(_restore_manifest_path(_restore_control_dir(), record["owner_key"],
                                                       record["operation_id"])),
            "safety_copy_available": Path(record["safety_copy"]).is_file(),
            "recovery_operation_id": record.get("recovery_operation_id")}


def restore_status(db, config, operation_id=None):
    """Consulta e reconcilia os recibos privados sob o lease de Recursos."""
    try:
        if db.dialect != "sqlite":
            raise ValueError("Consulta de restauração de cópia disponível para SQLite.")
        policy = policy_mod.load(config)
        with actions.exclusive(policy_mod.archive_dir(db, policy), "consultar restauração"):
            _owner_key, _folder, records = _restore_records(db, config)
            if operation_id is None:
                return {"ok": True, "state": "history", "operations": [_restore_outcome(r) for r in records]}
            operation_id = _restore_id(operation_id)
            record = next((r for r in records if r["operation_id"] == operation_id), None)
            if record is None:
                raise ValueError("Resultado indisponível para este ID; nenhuma troca foi solicitada.")
            return _restore_outcome(record)
    except actions.Busy as error:
        return {"ok": False, "state": "running", "operation_id": operation_id, "error": str(error)}
    except (OSError, ValueError) as error:
        return {"ok": False, "state": "unknown", "operation_id": operation_id, "error": str(error)}


def _retain_restore_copy(path):
    if not path.name.startswith("restore__"):
        return False
    try:
        record = _load_restore(_restore_control_dir() / path.with_suffix(".restore.json").name)
        return (record["safety_copy"] != str(path.resolve())
                or record["state"] not in {"succeeded", "failed", "recovered"})
    except (OSError, ValueError):
        return True


def _file_sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def restore_backup(db, config, backup_path, origin: str = "ui", *, operation_id=None, recovery_of=None) -> dict:
    """Restaura uma fotografia verificada, com intenção e resultado nos recibos de Recursos.

    Um ID informado consulta a intenção anterior. Recuperação de resultado incerto exige
    pedido novo e explícito para a cópia de segurança vinculada e conferida por SHA-256.
    Extrações e backtests devem estar parados, conforme o contrato de Recursos.
    """
    path = Path(backup_path).resolve()
    result = {"ok": False, "safety_copy": None, "error": None, "operation_id": operation_id, "state": "failed"}
    record, manifest, applying = None, None, False
    if db.dialect != "sqlite":
        result["error"] = "só para SQLite; no PostgreSQL use pg_restore"
        return result
    try:
        if operation_id is not None and recovery_of is not None:
            raise ValueError("Consulta por ID e recuperação são solicitações distintas.")
        policy = policy_mod.load(config)
        with actions.exclusive(policy_mod.archive_dir(db, policy), f"voltar à cópia ({origin})"):
            owner_key, folder, records = _restore_records(db, config)
            if operation_id is not None:
                operation_id = _restore_id(operation_id)
                existing = next((r for r in records if r["operation_id"] == operation_id), None)
                if existing is None:
                    return {**result, "state": "unknown", "error": "Resultado indisponível para este ID."}
                return _restore_outcome(existing)
            pending = [r for r in records if r["state"] in {"prepared", "applying", "uncertain"}]
            previous = None
            if recovery_of is not None:
                recovery_of = _restore_id(recovery_of)
                previous = next((r for r in pending if r["operation_id"] == recovery_of), None)
                if previous is None:
                    raise ValueError("A operação indicada não tem recuperação incerta disponível.")
                if path != Path(previous["safety_copy"]) or _file_sha256(path) != previous["safety_sha256"]:
                    raise ValueError("A cópia de segurança vinculada está indisponível ou diverge do SHA-256.")
            if any(r is not previous for r in pending):
                blocked = next(r for r in pending if r is not previous)
                return {**_restore_outcome(blocked), "ok": False,
                        "error": "Existe uma restauração sem resultado confirmado. Consulte o ID antes de outra troca."}
            active = Path(db.db_path).resolve()
            if not path.is_file():
                raise ValueError("A cópia escolhida não existe; nada foi alterado.")
            if path.samefile(active):
                raise ValueError("Escolha uma cópia diferente da base ativa; nada foi alterado.")
            source_identity = path.stat()
            with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=10)) as src:
                src.execute("BEGIN")
                if src.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
                    raise ValueError("A cópia escolhida não passou na conferência; nada foi alterado.")
                expected = _restore_inventory(src)
                operation_id = uuid.uuid4().hex
                manifest = _restore_manifest_path(_restore_control_dir(), owner_key, operation_id)
                safety_path = folder / (manifest.name.removesuffix(".restore.json") + ".db")
                if manifest.exists() or safety_path.exists():
                    raise ValueError("Este ID já existe; consulte seu resultado antes de continuar.")
                folder.mkdir(mode=0o700, parents=True, exist_ok=True)
                manifest.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                from mt5_extracao.process_owner import current
                result.update(operation_id=operation_id, safety_copy=str(safety_path),
                              receipt_path=str(manifest), state="prepared")
                record = {"schema": _RESTORE_SCHEMA, "owner_key": owner_key, "operation_id": operation_id,
                          "active_path": str(active),
                          "source": str(path), "safety_copy": str(safety_path), "safety_sha256": None,
                          "owner": current(), "created_at": datetime.now(timezone.utc).isoformat(), "state": "prepared",
                          "recovery_of": recovery_of, "result": dict(result)}
                _save_restore(manifest, record)
                _backup_locked(db, config, f"{origin}, antes de voltar à cópia", False, target=safety_path)
                record["safety_sha256"] = _file_sha256(safety_path)
                now = path.stat()
                if (session_owner_key(db, physical_only=True) != owner_key or path.samefile(active)
                        or (now.st_dev, now.st_ino, now.st_size, now.st_mtime_ns)
                        != (source_identity.st_dev, source_identity.st_ino, source_identity.st_size,
                            source_identity.st_mtime_ns)):
                    raise ValueError("A identidade da origem ou da base mudou durante o preparo; confira a operação.")
                if previous is not None and _file_sha256(path) != previous["safety_sha256"]:
                    raise ValueError("A cópia de segurança mudou durante o preparo; confira a operação.")
                result["state"] = record["state"] = "applying"
                record["result"] = dict(result)
                _save_restore(manifest, record)
                applying = True
                db.engine.dispose()
                try:
                    with closing(sqlite3.connect(active.as_uri() + "?mode=rw", uri=True, timeout=60)) as dst:
                        src.backup(dst)
                        if dst.execute("PRAGMA quick_check").fetchall() != [("ok",)] or _restore_inventory(dst) != expected:
                            raise RuntimeError("A base trocada diverge da fotografia escolhida.")
                finally:
                    db.engine.dispose()
                    db._time_basis_checked = False
                    if isinstance(getattr(db, "_table_cache", None), dict):
                        db._table_cache.clear()
                result.update(ok=True, state="succeeded")
                record.update(state="succeeded", result=dict(result))
                _save_restore(manifest, record)
            if previous is not None:
                previous.update(state="recovered", recovery_operation_id=operation_id)
                previous["result"]["state"] = "recovered"
                _save_restore(_restore_manifest_path(_restore_control_dir(), owner_key, recovery_of), previous)
        actions.audit(db, "voltar à cópia", path.name, origin, True, detail=dict(result))
    except Exception as error:
        result.update(ok=False, state="uncertain" if applying else "failed", error=str(error))
        if record is not None:
            record.update(state=result["state"], result=dict(result))
            try:
                _save_restore(manifest, record)
            except Exception:
                result["state"] = "uncertain" if applying else "unknown"
                result["error"] += " O recibo não pôde ser atualizado; consulte o ID e preserve as cópias."
        log.error("Falha ao voltar à cópia %s, operação %s, estado %s: %s",
                  path, result["operation_id"], result["state"], result["error"])
    return result


# --- perfis privados: retirada reversível, sem apagar relatórios -----------------------------

def session_owner_key(db, *, physical_only=False):
    """Vínculo ao banco criador, sem guardar URI nem credenciais no manifesto."""
    import hashlib
    try:
        url = db.engine.url
        backend = url.get_backend_name()
        if backend == "sqlite":
            if not url.database or url.database == ":memory:" or url.query.get("mode") == "memory":
                return None
            path = Path(url.database).expanduser().resolve()
            info = path.stat()
            if physical_only:
                if info.st_ino <= 0:
                    return None
                identity = [backend, info.st_dev, info.st_ino]
            else:
                identity = [backend, os.path.normcase(str(path)), info.st_dev, info.st_ino]
        elif backend == "postgresql":
            # Usuário/options distinguem namespaces/RLS; senha não faz parte da identidade.
            if not url.host:
                return None
            identity = [backend, url.host, url.port or 5432, url.database,
                        url.username, url.query.get("options", "")]
        else:
            return None
        return hashlib.sha256(json.dumps(identity, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()
    except (AttributeError, OSError, TypeError, ValueError):
        return None


def _session_path(config, session_id, quarantined=False):
    import re
    from mt5_extracao.robots.sessions import session_root
    if not isinstance(session_id, str) or not re.fullmatch(r"[0-9a-f]{32}", session_id):
        raise ValueError("Identificador de sessão inválido; use mt5x resources sessions.")
    root = session_root(config)
    if root.is_symlink() or (root / ".quarantine").is_symlink():
        raise ValueError("A raiz das sessões contém link; os arquivos foram preservados.")
    return root / ".quarantine" / session_id if quarantined else root / session_id


def _session_manifest(directory):
    if not directory.is_dir() or directory.is_symlink():
        raise ValueError("Pasta de sessão própria indisponível; nenhum arquivo foi movido.")
    if any(path.is_symlink() for path in directory.rglob("*")):
        raise ValueError("A sessão contém links; os arquivos foram preservados.")
    try:
        payload = json.loads((directory / "relatorio.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        raise ValueError("Manifesto da sessão ilegível; os arquivos foram preservados.") from None
    if (not isinstance(payload, dict) or payload.get("artifact_owner") != "mt5x-session" or
            payload.get("artifact_version") != 1 or payload.get("session") != directory.name):
        raise ValueError("Não há prova de posse deste perfil; os arquivos foram preservados.")
    if (payload.get("state") not in ("completed", "failed", "cancelled") or
            payload.get("resources_released") is not True or not payload.get("finished_at")):
        raise ValueError("A sessão não comprovou encerramento e liberação de recursos; seus arquivos foram preservados.")
    try:
        datetime.fromisoformat(payload["finished_at"])
    except (TypeError, ValueError):
        raise ValueError("Data de encerramento inválida; os arquivos foram preservados.") from None
    return payload


def _session_referenced(db, config, directory, protected=None):
    from sqlalchemy import inspect, text
    files, directories = protected if protected is not None else _protected_artifacts(db, config)
    resolved = directory.resolve()
    if any(path == resolved or resolved in path.parents for path in files):
        return True
    if any(path == resolved or path in resolved.parents or resolved in path.parents for path in directories):
        return True
    with db.engine.connect() as conn:
        if inspect(conn).has_table("_robots"):
            for (raw,) in conn.execute(text("SELECT meta_json FROM _robots")):
                try:
                    meta = json.loads(raw or "{}")
                    evidence = meta.get("demo_sessions", [])
                    if not isinstance(evidence, list):
                        raise ValueError()
                    if any(item["session"] == directory.name for item in evidence):
                        return True
                except (ValueError, TypeError, KeyError, AttributeError):
                    raise ValueError("Referências de sessão ilegíveis no catálogo; confira a base antes de mover arquivos.") from None
    return False


def list_sessions(db, config):
    """Inspeção dos perfis da raiz canônica; não cria catálogo nem remove pastas."""
    from mt5_extracao.robots.sessions import session_root
    root = session_root(config)
    result = []
    protected = None
    for quarantined, folder in ((False, root), (True, root / ".quarantine")):
        if not folder.is_dir() or folder.is_symlink():
            continue
        for directory in sorted(folder.iterdir()):
            if directory.name in (".quarantine", ".compile-cache"):
                continue
            item = {"session": directory.name, "path": str(directory), "quarantined": quarantined,
                    "eligible": False, "reason": ""}
            try:
                _session_path(config, directory.name, quarantined)
                payload = _session_manifest(directory)
                item["state"] = payload["state"]
                owner = session_owner_key(db)
                if owner is None or payload.get("owner_store") != owner:
                    raise ValueError("Perfil de outro banco ou sem vínculo verificável; arquivos preservados.")
                if not quarantined and protected is None:
                    protected = _protected_artifacts(db, config)
                if not quarantined and _session_referenced(db, config, directory, protected):
                    item["reason"] = "Artefatos referenciados por execução, catálogo ou lote."
                else:
                    item["eligible"] = True
            except ValueError as error:
                item["reason"] = str(error)
            result.append(item)
    return result


def move_session(db, config, session_id, *, restore=False, origin="ui"):
    """Move atomicamente dentro da raiz; restaura sem substituir destino existente."""
    from mt5_extracao.robots import mt5_tools
    policy = policy_mod.load(config)
    result = {"ok": False, "session": session_id, "restored": restore, "path": None, "error": None}
    action = "restaurar perfil" if restore else "quarentena de perfil"
    try:
        with actions.exclusive(policy_mod.archive_dir(db, policy), f"{action} ({origin})"):
            with mt5_tools.tester_lease():
                source = _session_path(config, session_id, restore)
                destination = _session_path(config, session_id, not restore)
                payload = _session_manifest(source)
                owner = session_owner_key(db)
                if owner is None or payload.get("owner_store") != owner:
                    raise ValueError("Perfil de outro banco ou sem vínculo verificável; arquivos preservados.")
                if not restore and _session_referenced(db, config, source):
                    raise ValueError("A sessão tem artefatos referenciados; nenhum arquivo foi movido.")
                if mt5_tools._native_context_processes({"directory": str(source),
                                                       "prefix": payload.get("wine_prefix", "")}):
                    raise ValueError("A sessão ainda tem processos ativos; seus arquivos foram preservados.")
                if destination.exists() or destination.is_symlink():
                    raise ValueError("O destino já existe; nenhum arquivo foi sobrescrito.")
                destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                source.rename(destination)
                result.update(ok=True, path=str(destination))
                actions.audit(db, action, session_id, origin, True,
                              detail={"source": str(source), "destination": str(destination), "reversible": True})
    except (actions.Busy, ValueError, OSError, RuntimeError) as error:
        result["error"] = str(error)
        actions.audit(db, action, session_id, origin, False, detail={"error": str(error)})
    return result
