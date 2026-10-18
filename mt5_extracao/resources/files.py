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
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

from mt5_extracao.resources import actions, storage
from mt5_extracao.resources import policy as policy_mod

log = logging.getLogger(__name__)

BACKUP_SUFFIX = ".db"
ACTIVE_LOGS = {"mt5_extracao.log"}


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


def backup(db, config, origin: str = "ui", prune: bool = True) -> dict:
    """Cria uma cópia conferida e (com prune) apaga as mais velhas além de `backup_keep`. Nunca lança."""
    policy = policy_mod.load(config)
    folder = backup_dir(db, policy)
    result = {"ok": False, "path": None, "bytes": None, "pruned": [], "error": None}
    if db.dialect != "sqlite":
        result["error"] = ("cópia automática só para SQLite; no PostgreSQL use pg_dump "
                           "(ex.: pg_dump -Fc banco > copia.dump)")
        return result
    try:
        with actions.exclusive(policy_mod.archive_dir(db, policy), f"cópia ({origin})"):
            folder.mkdir(parents=True, exist_ok=True)
            source = Path(db.db_path).resolve()
            size = sum(storage._sqlite_files(db).values())
            disk = storage.disk_usage(folder)
            if disk and disk["free"] < size * 1.1:
                raise RuntimeError(f"disco livre ({storage.human_bytes(disk['free'])}) menor que o tamanho da base "
                                   f"({storage.human_bytes(size)}); a cópia não foi feita")
            stamp = f"{source.stem}__{datetime.now():%Y%m%d-%H%M%S}"
            target, n = folder / f"{stamp}{BACKUP_SUFFIX}", 1
            while target.exists():                   # duas cópias no mesmo segundo não se sobrescrevem
                target, n = folder / f"{stamp}-{n}{BACKUP_SUFFIX}", n + 1
            partial = target.with_suffix(".parcial")
            src = sqlite3.connect(str(source), timeout=30)
            dst = sqlite3.connect(str(partial))
            try:
                src.backup(dst, pages=4096)
                check = dst.execute("PRAGMA quick_check").fetchone()[0]
            finally:
                dst.close()
                src.close()
            if check != "ok":
                partial.unlink(missing_ok=True)
                raise RuntimeError(f"a cópia não passou na conferência ({check})")
            partial.replace(target)
            result.update(ok=True, path=str(target), bytes=target.stat().st_size)
            for old in (list_backups(db, config)[policy.backup_keep:] if prune else []):
                try:
                    Path(old["path"]).unlink()
                    result["pruned"].append(old["path"])
                except OSError as e:
                    log.warning(f"Cópia antiga não apagada ({old['path']}): {e}")
            actions.audit(db, "copiar", target.name, origin, True, detail={"bytes": result["bytes"],
                                                                           "pruned": result["pruned"]})
            log.info(f"Cópia da base criada: {target}")
    except actions.Busy as e:
        result["error"] = str(e)
    except Exception as e:
        log.error(f"Falha na cópia da base: {e}")
        result["error"] = str(e)
        actions.audit(db, "copiar", None, origin, False, detail={"error": str(e)})
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


def _protected_artifacts(db, config):
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


def old_files(db, config, now: Optional[datetime] = None) -> List[dict]:
    """Arquivos que a limpeza apagaria: [{path, folder, bytes, modified}]. O log em uso fica de fora."""
    policy = policy_mod.load(config)
    now = now or datetime.now()
    found = []
    protected_files, protected_dirs = _protected_artifacts(db, config)
    for label, (folder, days) in _folders(db, policy).items():
        folder = Path(folder)
        if not folder.is_dir():
            continue
        cutoff = now - timedelta(days=days)
        for root, _dirs, names in os.walk(folder):
            for name in names:
                if label == "Logs" and name in ACTIVE_LOGS:
                    continue
                path = Path(root) / name
                if _artifact_protected(path, protected_files, protected_dirs):
                    continue
                try:
                    stat = path.stat()
                except OSError:
                    continue
                modified = datetime.fromtimestamp(stat.st_mtime)
                if modified < cutoff:
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

def restore_backup(db, config, backup_path, origin: str = "ui") -> dict:
    """
    Volta a base inteira ao estado de uma cópia. Antes, faz uma cópia do estado atual (se ela falhar, nada muda).
    A troca usa a API de backup do SQLite sobre a base aberta; extrações e backtests devem estar parados.
    """
    path = Path(backup_path)
    result = {"ok": False, "safety_copy": None, "error": None}
    if db.dialect != "sqlite":
        result["error"] = "só para SQLite; no PostgreSQL use pg_restore"
        return result
    try:
        src = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10)
        try:
            check = src.execute("PRAGMA quick_check").fetchone()[0]
        finally:
            src.close()
        if check != "ok":
            raise RuntimeError(f"a cópia escolhida não passou na conferência ({check}); nada foi alterado")
        safety = backup(db, config, origin=f"{origin}, antes de voltar à cópia", prune=False)   # não apaga a escolhida
        if not safety["ok"]:
            raise RuntimeError(f"não foi possível guardar o estado atual antes de trocar ({safety['error']}); "
                               "nada foi alterado")
        result["safety_copy"] = safety["path"]
        policy = policy_mod.load(config)
        with actions.exclusive(policy_mod.archive_dir(db, policy), f"voltar à cópia ({origin})"):
            db.engine.dispose()                      # solta as conexões do programa antes de sobrescrever
            src = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10)
            dst = sqlite3.connect(str(Path(db.db_path).resolve()), timeout=60)
            try:
                src.backup(dst)
                check = dst.execute("PRAGMA quick_check").fetchone()[0]
            finally:
                dst.close()
                src.close()
            db.engine.dispose()
            if isinstance(getattr(db, "_table_cache", None), dict):
                db._table_cache.clear()
            if check != "ok":
                raise RuntimeError(f"a base trocada não passou na conferência ({check}); volte à cópia de segurança "
                                   f"{safety['path']}")
        result["ok"] = True
        actions.audit(db, "voltar à cópia", path.name, origin, True, detail={"safety_copy": safety["path"]})
        log.info(f"Base trocada pela cópia {path} (estado anterior guardado em {safety['path']}).")
    except actions.Busy as e:
        result["error"] = str(e)
    except Exception as e:
        log.error(f"Falha ao voltar à cópia {path}: {e}")
        result["error"] = str(e)
        actions.audit(db, "voltar à cópia", path.name, origin, False, detail={"error": str(e)})
    return result


# --- perfis privados: retirada reversível, sem apagar relatórios -----------------------------

def session_owner_key(db):
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


def _session_referenced(db, config, directory):
    from sqlalchemy import inspect, text
    files, directories = _protected_artifacts(db, config)
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
    for quarantined, folder in ((False, root), (True, root / ".quarantine")):
        if not folder.is_dir() or folder.is_symlink():
            continue
        for directory in sorted(folder.iterdir()):
            if directory.name == ".quarantine":
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
                if not quarantined and _session_referenced(db, config, directory):
                    item["reason"] = "Artefatos referenciados por execução ou catálogo."
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
