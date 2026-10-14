"""
Cópias de segurança da base e limpeza de arquivos antigos (T15.7).

- backup: cópia consistente do SQLite pela API de backup (funciona com a base em uso), conferida com
  `PRAGMA quick_check`; guarda as `keep` mais novas. PostgreSQL: recusa e indica o pg_dump.
- old_files / clean_files: arquivos de logs e exportações mais velhos que N dias. O log em uso nunca entra.

Tudo sob a trava de actions.exclusive e registrado em _resource_actions.
"""
import logging
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


def old_files(db, config, now: Optional[datetime] = None) -> List[dict]:
    """Arquivos que a limpeza apagaria: [{path, folder, bytes, modified}]. O log em uso fica de fora."""
    policy = policy_mod.load(config)
    now = now or datetime.now()
    found = []
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
