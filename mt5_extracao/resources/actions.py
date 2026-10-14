"""
Ações de gestão de recursos (T15.3), todas auditadas em `_resource_actions` e sob uma trava de arquivo, para que
a interface e o agendador do sistema não mexam na base ao mesmo tempo.

- archive_tables: copia cada tabela para `<arquivo>/<tabela>__<data>.db` (SQLite), confere as linhas e só então
  remove a tabela e os registros de controle dela. Reversível com restore_archive.
- restore_archive: devolve a tabela (com o schema original) e o registro em `_symbol_tables`.
- vacuum: devolve ao disco o espaço livre da base (confere antes se há disco para a cópia temporária).
- clean_orphans: apaga registros de controle de tabelas que não existem mais.
"""
import json
import logging
import os
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd
from sqlalchemy import create_engine, inspect, text

from mt5_extracao.resources import policy as policy_mod
from mt5_extracao.resources import storage

log = logging.getLogger(__name__)

ARCHIVE_SUFFIX = ".db"
LOCK_NAME = ".lock"
LOCK_STALE_SECONDS = 6 * 3600
CHUNK_ROWS = 50_000

_ACTIONS = """
CREATE TABLE IF NOT EXISTS _resource_actions (
    at TEXT NOT NULL,
    action TEXT NOT NULL,
    target TEXT,
    origin TEXT NOT NULL,
    ok INTEGER NOT NULL,
    bytes_before INTEGER,
    bytes_after INTEGER,
    detail TEXT
)
"""


class Busy(RuntimeError):
    """Outra ação de recursos está em andamento."""


# --- trava e auditoria -----------------------------------------------------------------

@contextmanager
def exclusive(folder: Path, owner: str = ""):
    """Trava por arquivo criado com O_EXCL; uma trava de mais de 6 horas é considerada abandonada e removida."""
    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=True)
    lock = folder / LOCK_NAME
    if lock.exists():
        try:
            if time.time() - lock.stat().st_mtime > LOCK_STALE_SECONDS:
                log.warning(f"Trava abandonada removida: {lock}")
                lock.unlink()
        except OSError:
            pass
    try:
        fd = os.open(str(lock), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        holder = ""
        try:
            holder = lock.read_text(encoding="utf-8").strip()
        except OSError:
            pass
        raise Busy("Outra ação de gestão de recursos está em andamento" + (f" ({holder})" if holder else "") +
                   ". Aguarde terminar; se nada estiver rodando, apague o arquivo " + str(lock) + ".")
    try:
        os.write(fd, f"{owner} pid={os.getpid()} {datetime.now():%Y-%m-%d %H:%M:%S}".encode("utf-8"))
        os.close(fd)
        yield
    finally:
        try:
            lock.unlink()
        except OSError:
            pass


def audit(db, action: str, target: str, origin: str, ok: bool, before=None, after=None, detail=None) -> None:
    try:
        with db.engine.begin() as conn:
            conn.execute(text(_ACTIONS))
            conn.execute(text("INSERT INTO _resource_actions VALUES (:at, :a, :t, :o, :ok, :b, :af, :d)"),
                         {"at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "a": action, "t": target, "o": origin,
                          "ok": int(bool(ok)), "b": before, "af": after,
                          "d": json.dumps(detail, ensure_ascii=False, default=str) if detail is not None else None})
    except Exception as e:
        log.error(f"Não foi possível registrar a ação {action} na auditoria: {e}")


def load_actions(db, limit: int = 200) -> List[dict]:
    if db is None or getattr(db, "engine", None) is None or not inspect(db.engine).has_table("_resource_actions"):
        return []
    with db.engine.connect() as conn:
        rows = [dict(r) for r in conn.execute(
            text("SELECT * FROM _resource_actions ORDER BY at DESC, rowid DESC LIMIT :n"
                 if db.dialect == "sqlite" else "SELECT * FROM _resource_actions ORDER BY at DESC LIMIT :n"),
            {"n": int(limit)}).mappings()]
    for r in rows:
        r["detail"] = json.loads(r["detail"]) if r.get("detail") else None
    return rows


def _forget_table(db, table: str) -> None:
    cache = getattr(db, "_table_cache", None)
    if isinstance(cache, dict):
        for key in [k for k, v in cache.items() if v == table]:
            cache.pop(key, None)


def _control_rows(conn, table: str) -> dict:
    out = {}
    if inspect(conn).has_table("_symbol_tables"):
        out["symbol_tables"] = [dict(r) for r in conn.execute(
            text("SELECT symbol, timeframe, table_name FROM _symbol_tables WHERE table_name = :t"),
            {"t": table}).mappings()]
    return out


# --- arquivar --------------------------------------------------------------------------

def _archive_one_sqlite(db, table: str, target: Path, info: dict) -> int:
    """Cópia rápida: o arquivo novo anexa a base de origem e copia com o mesmo CREATE TABLE. Devolve as linhas."""
    source = str(Path(db.db_path).resolve())
    con = sqlite3.connect(str(target), timeout=30)
    try:
        con.execute("ATTACH DATABASE ? AS src", (source,))
        create_sql = con.execute("SELECT sql FROM src.sqlite_master WHERE type = 'table' AND name = ?",
                                 (table,)).fetchone()
        if not create_sql:
            raise RuntimeError(f"tabela {table} não encontrada")
        info["create_sql"] = create_sql[0]
        info["indexes"] = [r[0] for r in con.execute(
            "SELECT sql FROM src.sqlite_master WHERE type = 'index' AND tbl_name = ? AND sql IS NOT NULL", (table,))]
        con.execute(create_sql[0])
        con.execute(f'INSERT INTO main."{table}" SELECT * FROM src."{table}"')
        _write_info(con, info)
        con.commit()
        con.execute("DETACH DATABASE src")
        return con.execute(f'SELECT COUNT(*) FROM main."{table}"').fetchone()[0]
    finally:
        con.close()


def _archive_one_generic(db, table: str, target: Path, info: dict) -> int:
    """PostgreSQL (ou qualquer outro): copia em blocos com pandas para um SQLite."""
    arc = create_engine(f"sqlite:///{target}")
    copied = 0
    try:
        with db.engine.connect() as conn:
            for chunk in pd.read_sql(text(f'SELECT * FROM "{table}"'), conn, chunksize=CHUNK_ROWS):
                chunk.to_sql(table, arc, if_exists="append", index=False)
                copied += len(chunk)
        if copied == 0:      # tabela vazia: mantém pelo menos as colunas
            columns = [c["name"] for c in inspect(db.engine).get_columns(table)]
            pd.DataFrame(columns=columns).to_sql(table, arc, if_exists="replace", index=False)
        con = arc.raw_connection()
        try:
            _write_info(con, info)
            con.commit()
        finally:
            con.close()
    finally:
        arc.dispose()
    return copied


def _write_info(con, info: dict) -> None:
    con.execute("CREATE TABLE IF NOT EXISTS _archive_info (key TEXT PRIMARY KEY, value TEXT)")
    for key, value in info.items():
        con.execute("INSERT OR REPLACE INTO _archive_info VALUES (?, ?)",
                    (key, json.dumps(value, ensure_ascii=False, default=str)))


def archive_tables(db, config, tables: Iterable[str], origin: str = "ui", reasons: Optional[dict] = None,
                   progress=None) -> List[dict]:
    """
    Arquiva e remove as tabelas, uma a uma (uma falha não impede as outras). Devolve
    [{table, ok, archive, rows, bytes_freed, error}]. Tabelas internas ('_...') são recusadas.
    """
    policy = policy_mod.load(config)
    folder = policy_mod.archive_dir(db, policy)
    results = []
    tables = list(dict.fromkeys(tables))
    with exclusive(folder, f"arquivar ({origin})"):
        existing = set(inspect(db.engine).get_table_names())
        for index, table in enumerate(tables):
            if progress:
                progress(index, len(tables), table)
            results.append(_archive_one(db, table, folder, existing, origin, (reasons or {}).get(table)))
    return results


def _archive_one(db, table, folder, existing, origin, reason) -> dict:
    result = {"table": table, "ok": False, "archive": None, "rows": 0, "bytes_freed": None, "error": None}
    if table.startswith("_") or table.startswith("sqlite_"):
        result["error"] = "tabela interna não pode ser arquivada"
        return result
    if table not in existing:
        result["error"] = "tabela não existe"
        return result
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    target = folder / f"{table}__{stamp}{ARCHIVE_SUFFIX}"
    n = 1
    while target.exists():
        target = folder / f"{table}__{stamp}-{n}{ARCHIVE_SUFFIX}"
        n += 1
    try:
        with db.engine.connect() as conn:
            rows_before = int(conn.execute(text(f'SELECT COUNT(*) FROM "{table}"')).scalar() or 0)
            controls = _control_rows(conn, table)
        info = {"table": table, "rows": rows_before, "archived_at": datetime.now().isoformat(timespec="seconds"),
                "dialect": db.dialect, "origin": origin, "reasons": reason, "controls": controls}
        disk = storage.disk_usage(folder)
        size_before = storage.measure(db, with_dirs=False).get("db_bytes")
        copied = (_archive_one_sqlite if db.dialect == "sqlite" else _archive_one_generic)(db, table, target, info)
        if copied != rows_before:
            raise RuntimeError(f"cópia conferida com {copied} linha(s), a tabela tem {rows_before}; nada foi removido")
        with db.engine.begin() as conn:
            conn.execute(text(f'DROP TABLE "{table}"'))
            if inspect(conn).has_table("_symbol_tables"):
                conn.execute(text("DELETE FROM _symbol_tables WHERE table_name = :t"), {"t": table})
            if inspect(conn).has_table("_extraction_log"):
                conn.execute(text("DELETE FROM _extraction_log WHERE table_name = :t"), {"t": table})
        _forget_table(db, table)
        result.update(ok=True, archive=str(target), rows=copied)
        audit(db, "arquivar", table, origin, True, size_before, None,
              {"archive": str(target), "rows": copied, "reasons": reason,
               "disk_free": disk.get("free") if disk else None})
        log.info(f"Tabela {table} arquivada em {target} ({copied} linhas).")
    except Exception as e:
        log.error(f"Falha ao arquivar {table}: {e}")
        result["error"] = str(e)
        try:
            if target.exists() and not result["ok"]:
                target.unlink()
        except OSError:
            pass
        audit(db, "arquivar", table, origin, False, detail={"error": str(e)})
    return result


# --- arquivo: listar e restaurar --------------------------------------------------------

def _read_info(path: Path) -> dict:
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10)
    try:
        return {k: json.loads(v) for k, v in con.execute("SELECT key, value FROM _archive_info")}
    finally:
        con.close()


def list_archives(db, config) -> List[dict]:
    """Arquivos disponíveis, o mais novo primeiro: [{path, table, rows, archived_at, bytes, symbol, timeframe, error}]."""
    folder = policy_mod.archive_dir(db, policy_mod.load(config))
    if not folder.is_dir():
        return []
    out = []
    for path in sorted(folder.glob(f"*{ARCHIVE_SUFFIX}"), key=lambda p: p.stat().st_mtime, reverse=True):
        item = {"path": str(path), "bytes": path.stat().st_size, "table": path.stem.split("__")[0], "rows": None,
                "archived_at": None, "symbol": None, "timeframe": None, "reasons": None, "error": None}
        try:
            info = _read_info(path)
            controls = (info.get("controls") or {}).get("symbol_tables") or [{}]
            item.update(table=info.get("table", item["table"]), rows=info.get("rows"),
                        archived_at=info.get("archived_at"), reasons=info.get("reasons"),
                        symbol=controls[0].get("symbol"), timeframe=controls[0].get("timeframe"))
        except Exception as e:
            item["error"] = f"arquivo sem informações legíveis: {e}"
        out.append(item)
    return out


def restore_archive(db, config, archive_path, origin: str = "ui") -> dict:
    """Devolve a tabela à base. Recusa se a tabela já existir (nada é sobrescrito)."""
    path = Path(archive_path)
    policy = policy_mod.load(config)
    folder = policy_mod.archive_dir(db, policy)
    result = {"archive": str(path), "ok": False, "table": None, "rows": 0, "error": None}
    with exclusive(folder, f"restaurar ({origin})"):
        try:
            info = _read_info(path)
            table = info["table"]
            result["table"] = table
            if inspect(db.engine).has_table(table):
                raise RuntimeError(f"a tabela {table} já existe na base; nada foi alterado")
            if db.dialect == "sqlite" and info.get("dialect") == "sqlite" and info.get("create_sql"):
                con = sqlite3.connect(str(Path(db.db_path).resolve()), timeout=30)
                try:
                    con.execute("ATTACH DATABASE ? AS arc", (str(path),))
                    con.execute("BEGIN")
                    con.execute(info["create_sql"])
                    con.execute(f'INSERT INTO main."{table}" SELECT * FROM arc."{table}"')
                    for sql in info.get("indexes") or []:
                        con.execute(sql)
                    rows = con.execute(f'SELECT COUNT(*) FROM main."{table}"').fetchone()[0]
                    if rows != info.get("rows"):
                        raise RuntimeError(f"restauradas {rows} linha(s), o arquivo diz {info.get('rows')}")
                    con.commit()
                except Exception:
                    con.rollback()
                    raise
                finally:
                    con.close()
            else:
                arc = create_engine(f"sqlite:///{path}")
                rows = 0
                try:
                    with arc.connect() as src:
                        for chunk in pd.read_sql(text(f'SELECT * FROM "{table}"'), src, chunksize=CHUNK_ROWS):
                            chunk.to_sql(table, db.engine, if_exists="append", index=False)
                            rows += len(chunk)
                finally:
                    arc.dispose()
            with db.engine.begin() as conn:
                for row in (info.get("controls") or {}).get("symbol_tables") or []:
                    conn.execute(text("DELETE FROM _symbol_tables WHERE symbol = :symbol AND timeframe = :timeframe"),
                                 row)
                    conn.execute(text("INSERT INTO _symbol_tables (symbol, timeframe, table_name) "
                                      "VALUES (:symbol, :timeframe, :table_name)"), row)
            _forget_table(db, table)
            restored = path.parent / "restaurados"
            restored.mkdir(exist_ok=True)
            path.replace(restored / path.name)       # o arquivo não é apagado: vai para restaurados/
            result.update(ok=True, rows=rows)
            audit(db, "restaurar", table, origin, True, detail={"archive": str(path), "rows": rows})
            log.info(f"Tabela {table} restaurada de {path} ({rows} linhas).")
        except Exception as e:
            log.error(f"Falha ao restaurar {path}: {e}")
            result["error"] = str(e)
            audit(db, "restaurar", result["table"] or path.name, origin, False, detail={"error": str(e)})
    return result


# --- otimizar e órfãos ------------------------------------------------------------------

def vacuum(db, config, origin: str = "ui") -> dict:
    """VACUUM (SQLite) ou VACUUM ANALYZE (PostgreSQL). No SQLite exige disco livre de pelo menos o tamanho da base."""
    folder = policy_mod.archive_dir(db, policy_mod.load(config))
    before = storage.measure(db, with_dirs=False)
    result = {"ok": False, "bytes_before": before.get("db_bytes"), "bytes_after": None, "error": None}
    with exclusive(folder, f"otimizar ({origin})"):
        try:
            if db.dialect == "sqlite":
                disk = before.get("disk") or {}
                need = (before.get("db_bytes") or 0) - (before.get("reclaimable_bytes") or 0)
                if disk.get("free") is not None and disk["free"] < need * 1.1:
                    raise RuntimeError(f"disco livre ({storage.human_bytes(disk['free'])}) menor que o necessário "
                                       f"para otimizar ({storage.human_bytes(need * 1.1)}). Arquive séries ou libere "
                                       "espaço antes.")
            with db.engine.connect() as conn:
                conn = conn.execution_options(isolation_level="AUTOCOMMIT")
                conn.execute(text("VACUUM" if db.dialect == "sqlite" else "VACUUM ANALYZE"))
            after = storage.measure(db, with_dirs=False).get("db_bytes")
            result.update(ok=True, bytes_after=after)
            audit(db, "otimizar", None, origin, True, result["bytes_before"], after)
        except Exception as e:
            log.error(f"Falha ao otimizar a base: {e}")
            result["error"] = str(e)
            audit(db, "otimizar", None, origin, False, result["bytes_before"], detail={"error": str(e)})
    return result


def clean_orphans(db, config, origin: str = "ui") -> dict:
    from mt5_extracao.resources import hygiene
    folder = policy_mod.archive_dir(db, policy_mod.load(config))
    with exclusive(folder, f"órfãos ({origin})"):
        orphans = hygiene.find_orphans(db)
        removed = {"symbol_tables": 0, "extraction_log": 0}
        with db.engine.begin() as conn:
            for table in orphans["symbol_tables"]:
                removed["symbol_tables"] += conn.execute(
                    text("DELETE FROM _symbol_tables WHERE table_name = :t"), {"t": table}).rowcount or 0
            for table in orphans["extraction_log"]:
                removed["extraction_log"] += conn.execute(
                    text("DELETE FROM _extraction_log WHERE table_name = :t"), {"t": table}).rowcount or 0
        if any(removed.values()):
            audit(db, "órfãos", None, origin, True, detail={"orphans": orphans, "removed": removed})
    return {"ok": True, "orphans": orphans, "removed": removed}
