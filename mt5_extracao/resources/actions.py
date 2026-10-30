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
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd
from sqlalchemy import create_engine, inspect, text

from mt5_extracao.resources import policy as policy_mod
from mt5_extracao.resources import storage

log = logging.getLogger(__name__)

ARCHIVE_SUFFIX = ".db"
LOCK_NAME = ".lock"
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

def control_lock_paths(folder):
    from mt5_extracao.bootstrap.paths import state_dir
    return (state_dir() / "resources" / LOCK_NAME, Path(folder) / LOCK_NAME)


@contextmanager
def exclusive(folder: Path, owner: str = ""):
    """Mesmo lease local entre configurações/cópias do App; mantém o gate da pasta."""
    shared, local = control_lock_paths(folder)
    with _exclusive_folder(shared.parent, owner):
        if shared.resolve() == local.resolve():
            yield
        else:
            with _exclusive_folder(local.parent, owner):
                yield


@contextmanager
def _exclusive_folder(folder: Path, owner: str = ""):
    """Lease do arquivo existente; idade nunca comprova que um consumidor saiu."""
    from mt5_extracao.process_owner import current, is_alive
    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=True)
    lock = folder / LOCK_NAME
    if lock.is_symlink():
        raise Busy("A trava de Recursos contém link; confira o diagnóstico antes de alterar arquivos.")
    handle = None
    try:
        descriptor = os.open(lock, os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600)
        handle = os.fdopen(descriptor, "r+b")
        try:
            _lock_resource_file(handle)
        except OSError:
            raise Busy("Outra ação de gestão de recursos está em andamento. Aguarde terminar.") from None
        try:
            handle.seek(0)
            raw = handle.read()
            if raw:
                try:
                    previous = json.loads(raw)
                    if (not isinstance(previous, dict) or previous.get("resource_lock_version") != 2 or
                            previous.get("state") not in ("active", "released")):
                        raise ValueError
                except (ValueError, TypeError):
                    raise Busy("Trava antiga ou ilegível sem prova de liberação; confira o proprietário da ação antes de remover o arquivo .lock da pasta de arquivo.") from None
                if previous["state"] == "active" and is_alive(previous.get("identity")):
                    raise Busy("A trava de Recursos não comprovou encerramento do proprietário; os arquivos foram preservados.")
            payload = dict(resource_lock_version=2, identity=current(), state="active", action=owner)
            def write(state):
                payload["state"] = state
                handle.seek(0); handle.write(json.dumps(payload, ensure_ascii=False).encode("utf-8"))
                handle.truncate(); handle.flush()
            write("active")
            try:
                yield
            finally:
                write("released")
        finally:
            _lock_resource_file(handle, release=True)
    finally:
        if handle is not None:
            handle.close()


def _lock_resource_file(handle, *, release=False):
    # Mantém o inode: unlink permitiria dois consumidores com handles distintos.
    handle.seek(0)
    if os.name == "nt":
        import msvcrt
        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK if release else msvcrt.LK_NBLCK, 1)
    else:
        import fcntl
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN if release else fcntl.LOCK_EX | fcntl.LOCK_NB)


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

def _archive_one_sqlite(db, table: str, target: Path, info: dict, where: str = "", params: dict = None) -> int:
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
        con.execute(f'INSERT INTO main."{table}" SELECT * FROM src."{table}"' + where.replace(":cut", "?"),
                    tuple((params or {}).values()))
        _write_info(con, info)
        con.commit()
        con.execute("DETACH DATABASE src")
        return con.execute(f'SELECT COUNT(*) FROM main."{table}"').fetchone()[0]
    finally:
        con.close()


def _archive_one_generic(db, table: str, target: Path, info: dict, where: str = "", params: dict = None) -> int:
    """PostgreSQL (ou qualquer outro): copia em blocos com pandas para um SQLite."""
    arc = create_engine(f"sqlite:///{target}")
    copied = 0
    try:
        with db.engine.connect() as conn:
            for chunk in pd.read_sql(text(f'SELECT * FROM "{table}"{where}'), conn, params=params or {},
                                     chunksize=CHUNK_ROWS):
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
    Formato do arquivo: [RESOURCES] archive_format (sqlite, padrão; ou parquet, bem menor, exige pyarrow).
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
            results.append(_move_rows(db, table, folder, existing, origin, (reasons or {}).get(table),
                                      policy.archive_format))
    return results


def trim_tables(db, config, plan: Dict[str, int], origin: str = "ui", now: Optional[datetime] = None) -> List[dict]:
    """
    Apara séries: as linhas mais velhas que `dias` (plan = {tabela: dias}) vão para o arquivo e saem da base; a
    tabela fica, com o período recente. Funciona no SQLite e no PostgreSQL. Restaurar devolve as linhas.
    """
    policy = policy_mod.load(config)
    folder = policy_mod.archive_dir(db, policy)
    now = now or datetime.now()
    results = []
    with exclusive(folder, f"aparar ({origin})"):
        existing = set(inspect(db.engine).get_table_names())
        for table, days in plan.items():
            results.append(_move_rows(db, table, folder, existing, origin, None, policy.archive_format,
                                      before=now - timedelta(days=int(days)), keep_days=int(days)))
    return results


def _time_column(db, table: str) -> Optional[str]:
    columns = {c["name"] for c in inspect(db.engine).get_columns(table)}
    return next((c for c in ("time", "time_utc") if c in columns), None)


def _cutoff_param(db, before: datetime):
    """O SQLite guarda o horário como texto ('AAAA-MM-DD HH:MM:SS...'): compara como texto; o PostgreSQL, como data."""
    return before.strftime("%Y-%m-%d %H:%M:%S") if db.dialect == "sqlite" else before


def _drop_old_chunks(db, conn, table: str, before: datetime) -> int:
    """
    TimescaleDB: descarta de uma vez os blocos (chunks) inteiros anteriores ao corte, muito mais rápido que DELETE
    linha a linha; o DELETE que vem depois cuida do bloco parcial. Devolve as linhas descartadas (0 fora do Timescale).
    """
    if db.dialect != "postgresql" or not getattr(db, "_timescale", False):
        return 0
    hyper = conn.execute(text("SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name = :t"),
                         {"t": table}).scalar()
    if not hyper:
        return 0
    before_count = int(conn.execute(text(f'SELECT COUNT(*) FROM "{table}"')).scalar() or 0)
    conn.execute(text("SELECT drop_chunks(:t, older_than => :cut)"), {"t": table, "cut": before})
    after_count = int(conn.execute(text(f'SELECT COUNT(*) FROM "{table}"')).scalar() or 0)
    log.info(f"{table}: drop_chunks descartou {before_count - after_count} linha(s) antes de {before}.")
    return before_count - after_count


def _new_target(folder: Path, table: str, suffix: str) -> Path:
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    target, n = folder / f"{table}__{stamp}{suffix}", 1
    while target.exists() or _sidecar(target).exists():
        target, n = folder / f"{table}__{stamp}-{n}{suffix}", n + 1
    return target


def _sidecar(path: Path) -> Path:
    return Path(str(path) + ".json")


def _copy_parquet(db, table: str, target: Path, info: dict, where: str, params: dict) -> int:
    with db.engine.connect() as conn:
        frame = pd.read_sql(text(f'SELECT * FROM "{table}"{where}'), conn, params=params)
    frame.to_parquet(target, index=False, compression="zstd")
    _sidecar(target).write_text(json.dumps(info, ensure_ascii=False, default=str), encoding="utf-8")
    return len(pd.read_parquet(target, columns=[frame.columns[0]])) if len(frame.columns) else 0


def _move_rows(db, table, folder, existing, origin, reason, fmt="sqlite", before=None, keep_days=None) -> dict:
    """Arquiva a tabela inteira (before=None) ou só as linhas anteriores a `before` (aparar)."""
    trim = before is not None
    action = "aparar" if trim else "arquivar"
    result = {"table": table, "ok": False, "archive": None, "rows": 0, "bytes_freed": None, "error": None}
    if table.startswith("_") or table.startswith("sqlite_"):
        result["error"] = f"tabela interna não pode ser {'aparada' if trim else 'arquivada'}"
        return result
    if table not in existing:
        result["error"] = "tabela não existe"
        return result
    if fmt == "parquet":
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            result["error"] = "formato parquet exige o pyarrow (pip install pyarrow); nada foi alterado"
            return result
    where, params = "", {}
    if trim:
        column = _time_column(db, table)
        if column is None:
            result["error"] = "tabela sem coluna de horário: não dá para aparar por idade"
            return result
        where, params = f' WHERE "{column}" < :cut', {"cut": _cutoff_param(db, before)}
    target = _new_target(folder, table, ".parquet" if fmt == "parquet" else ARCHIVE_SUFFIX)
    try:
        with db.engine.connect() as conn:
            rows_before = int(conn.execute(text(f'SELECT COUNT(*) FROM "{table}"{where}'), params).scalar() or 0)
            controls = _control_rows(conn, table)
        if trim and rows_before == 0:
            result.update(ok=True, rows=0)
            return result
        info = {"table": table, "rows": rows_before, "archived_at": datetime.now().isoformat(timespec="seconds"),
                "dialect": db.dialect, "origin": origin, "reasons": reason, "controls": controls,
                "kind": "trim" if trim else "table", "format": fmt}
        if trim:
            info.update(before=before.isoformat(timespec="seconds"), keep_days=keep_days)
        if db.dialect == "sqlite":
            with db.engine.connect() as conn:
                info["create_sql"] = conn.execute(text(
                    "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = :t"), {"t": table}).scalar()
                info["indexes"] = [r[0] for r in conn.execute(text(
                    "SELECT sql FROM sqlite_master WHERE type = 'index' AND tbl_name = :t AND sql IS NOT NULL"),
                    {"t": table})]
        size_before = storage.measure(db, with_dirs=False).get("db_bytes")
        if fmt == "parquet":
            copied = _copy_parquet(db, table, target, info, where, params)
        elif db.dialect == "sqlite":
            copied = _archive_one_sqlite(db, table, target, info, where, params)
        else:
            copied = _archive_one_generic(db, table, target, info, where, params)
        if copied != rows_before:
            raise RuntimeError(f"cópia conferida com {copied} linha(s), a tabela tem {rows_before}; nada foi removido")
        with db.engine.begin() as conn:
            if trim:
                deleted = _drop_old_chunks(db, conn, table, before)
                deleted += conn.execute(text(f'DELETE FROM "{table}"{where}'), params).rowcount
                if deleted != rows_before:
                    raise RuntimeError(f"seriam apagadas {deleted} linha(s), a cópia tem {rows_before}; "
                                       "nada foi removido")
            else:
                conn.execute(text(f'DROP TABLE "{table}"'))
                if inspect(conn).has_table("_symbol_tables"):
                    conn.execute(text("DELETE FROM _symbol_tables WHERE table_name = :t"), {"t": table})
                if inspect(conn).has_table("_extraction_log"):
                    conn.execute(text("DELETE FROM _extraction_log WHERE table_name = :t"), {"t": table})
        if not trim:
            _forget_table(db, table)
        result.update(ok=True, archive=str(target), rows=copied)
        audit(db, action, table, origin, True, size_before, None,
              {"archive": str(target), "rows": copied, "reasons": reason, "keep_days": keep_days, "format": fmt})
        log.info(f"Tabela {table}: {copied} linha(s) {'aparadas' if trim else 'arquivadas'} em {target}.")
    except Exception as e:
        log.error(f"Falha ao {action} {table}: {e}")
        result["error"] = str(e)
        for leftover in (target, _sidecar(target)):
            try:
                if leftover.exists():
                    leftover.unlink()
            except OSError:
                pass
        audit(db, action, table, origin, False, detail={"error": str(e)})
    return result


# --- arquivo: listar e restaurar --------------------------------------------------------

def _read_info(path: Path) -> dict:
    path = Path(path)
    if path.suffix == ".parquet":
        return json.loads(_sidecar(path).read_text(encoding="utf-8"))
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10)
    try:
        return {k: json.loads(v) for k, v in con.execute("SELECT key, value FROM _archive_info")}
    finally:
        con.close()


def list_archives(db, config) -> List[dict]:
    """
    Arquivos disponíveis, o mais novo primeiro:
    [{path, table, rows, archived_at, bytes, symbol, timeframe, kind ('table' ou 'trim'), format, error}].
    """
    folder = policy_mod.archive_dir(db, policy_mod.load(config))
    if not folder.is_dir():
        return []
    out = []
    paths = list(folder.glob(f"*{ARCHIVE_SUFFIX}")) + list(folder.glob("*.parquet"))
    for path in sorted(paths, key=lambda p: p.stat().st_mtime, reverse=True):
        item = {"path": str(path), "bytes": path.stat().st_size, "table": path.stem.split("__")[0], "rows": None,
                "archived_at": None, "symbol": None, "timeframe": None, "reasons": None, "kind": "table",
                "format": "parquet" if path.suffix == ".parquet" else "sqlite", "keep_days": None, "error": None}
        try:
            info = _read_info(path)
            controls = (info.get("controls") or {}).get("symbol_tables") or [{}]
            item.update(table=info.get("table", item["table"]), rows=info.get("rows"),
                        archived_at=info.get("archived_at"), reasons=info.get("reasons"),
                        kind=info.get("kind", "table"), keep_days=info.get("keep_days"),
                        symbol=controls[0].get("symbol"), timeframe=controls[0].get("timeframe"))
        except Exception as e:
            item["error"] = f"arquivo sem informações legíveis: {e}"
        out.append(item)
    return out


def _insert_ignoring(db, table: str, frame: pd.DataFrame) -> int:
    """Insere as linhas; as que já existem (mesma chave) ficam como estão. Devolve as linhas enviadas."""
    if frame.empty:
        return 0
    columns = list(frame.columns)
    names = ", ".join(f'"{c}"' for c in columns)
    values = ", ".join(f":p{i}" for i in range(len(columns)))
    if db.dialect == "sqlite":
        sql = f'INSERT OR IGNORE INTO "{table}" ({names}) VALUES ({values})'
    else:
        sql = f'INSERT INTO "{table}" ({names}) VALUES ({values}) ON CONFLICT DO NOTHING'
    data = frame.astype(object).where(pd.notna(frame), None)
    records = [{f"p{i}": v for i, v in enumerate(row)} for row in data.itertuples(index=False, name=None)]
    with db.engine.begin() as conn:
        for start in range(0, len(records), CHUNK_ROWS):
            conn.execute(text(sql), records[start:start + CHUNK_ROWS])
    return len(records)


def _frames(path: Path, table: str):
    if path.suffix == ".parquet":
        yield pd.read_parquet(path)
        return
    arc = create_engine(f"sqlite:///{path}")
    try:
        with arc.connect() as src:
            yield from pd.read_sql(text(f'SELECT * FROM "{table}"'), src, chunksize=CHUNK_ROWS)
    finally:
        arc.dispose()


def restore_archive(db, config, archive_path, origin: str = "ui") -> dict:
    """
    Devolve à base o que o arquivo guarda. Série inteira: recusa se a tabela já existir (nada é sobrescrito).
    Linhas aparadas: voltam para a tabela existente; linhas que já estão lá ficam como estão.
    """
    path = Path(archive_path)
    policy = policy_mod.load(config)
    folder = policy_mod.archive_dir(db, policy)
    result = {"archive": str(path), "ok": False, "table": None, "rows": 0, "error": None}
    with exclusive(folder, f"restaurar ({origin})"):
        try:
            info = _read_info(path)
            table, trim = info["table"], info.get("kind") == "trim"
            result["table"] = table
            exists = inspect(db.engine).has_table(table)
            if trim and not exists:
                raise RuntimeError(f"a tabela {table} não está na base; restaure antes a série inteira")
            if not trim and exists:
                raise RuntimeError(f"a tabela {table} já existe na base; nada foi alterado")
            if db.dialect == "sqlite" and path.suffix == ARCHIVE_SUFFIX and info.get("dialect") == "sqlite" and \
                    (trim or info.get("create_sql")):
                con = sqlite3.connect(str(Path(db.db_path).resolve()), timeout=30)
                try:
                    con.execute("ATTACH DATABASE ? AS arc", (str(path),))
                    con.execute("BEGIN")
                    if trim:
                        rows = con.execute(f'INSERT OR IGNORE INTO main."{table}" SELECT * FROM arc."{table}"').rowcount
                    else:
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
                if not trim:
                    if db.dialect == "sqlite" and info.get("create_sql"):
                        with db.engine.begin() as conn:
                            conn.execute(text(info["create_sql"]))
                rows = 0
                for frame in _frames(path, table):
                    if not inspect(db.engine).has_table(table):
                        frame.head(0).to_sql(table, db.engine, index=False)
                    rows += _insert_ignoring(db, table, frame)
                if not trim:
                    with db.engine.begin() as conn:
                        for sql in (info.get("indexes") or []) if db.dialect == "sqlite" else []:
                            conn.execute(text(sql))
            if not trim:
                with db.engine.begin() as conn:
                    for row in (info.get("controls") or {}).get("symbol_tables") or []:
                        conn.execute(text("DELETE FROM _symbol_tables WHERE symbol = :symbol AND "
                                          "timeframe = :timeframe"), row)
                        conn.execute(text("INSERT INTO _symbol_tables (symbol, timeframe, table_name) "
                                          "VALUES (:symbol, :timeframe, :table_name)"), row)
                _forget_table(db, table)
            restored = path.parent / "restaurados"
            restored.mkdir(exist_ok=True)
            path.replace(restored / path.name)       # o arquivo não é apagado: vai para restaurados/
            if _sidecar(path).exists():
                _sidecar(path).replace(restored / _sidecar(path).name)
            result.update(ok=True, rows=rows)
            audit(db, "restaurar", table, origin, True, detail={"archive": str(path), "rows": rows, "trim": trim})
            log.info(f"Tabela {table}: {rows} linha(s) restaurada(s) de {path}.")
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
