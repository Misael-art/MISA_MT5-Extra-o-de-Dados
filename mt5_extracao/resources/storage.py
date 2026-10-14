"""
Medição de espaço (T15.1): tamanho da base e de cada tabela, espaço recuperável, disco livre e pastas do projeto;
retratos periódicos em `_resource_snapshots` e a projeção de crescimento. Só leitura, salvo `record_snapshot`.

    report = measure(db)        # {"db_bytes", "reclaimable_bytes", "disk", "tables": [...], "dirs": {...}, ...}
    record_snapshot(db, report)
    growth(load_snapshots(db))  # {"bytes_per_day", "days_to_full"} ou None
"""
import logging
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

from sqlalchemy import inspect, text

log = logging.getLogger(__name__)

KIND_BARS, KIND_TICKS, KIND_BOOK, KIND_INTERNAL, KIND_OTHER = "barras", "ticks", "book", "interna", "outra"

# Pastas medidas além da base: rótulo -> função que devolve o caminho (registro aberto para novas frentes, T15.7)
EXTRA_DIRS: Dict[str, callable] = {}


def _project_root() -> Path:
    from mt5_extracao.bootstrap import paths
    return Path(paths.PROJECT_ROOT)


EXTRA_DIRS["Logs"] = lambda db, archive: _project_root() / "logs"
EXTRA_DIRS["Exportações"] = lambda db, archive: _project_root() / "exports"
EXTRA_DIRS["Arquivo"] = lambda db, archive: archive

_SNAPSHOTS = """
CREATE TABLE IF NOT EXISTS _resource_snapshots (
    taken_at TEXT PRIMARY KEY,
    db_bytes INTEGER NOT NULL,
    reclaimable_bytes INTEGER,
    disk_free INTEGER,
    disk_total INTEGER,
    tables INTEGER,
    rows INTEGER
)
"""


def _usable(db) -> bool:
    return db is not None and getattr(db, "engine", None) is not None


def dir_bytes(path) -> int:
    """Soma dos arquivos da pasta (recursiva); 0 se não existir. Erros de permissão são ignorados."""
    total = 0
    path = Path(path)
    if not path.exists():
        return 0
    for root, _dirs, files in os.walk(path, onerror=lambda e: None):
        for name in files:
            try:
                total += os.path.getsize(os.path.join(root, name))
            except OSError:
                pass
    return total


def disk_usage(path) -> Optional[dict]:
    """{"path", "total", "used", "free", "free_pct"} do disco que contém `path` (sobe até um pai que exista)."""
    path = Path(path).resolve()
    while not path.exists() and path.parent != path:
        path = path.parent
    try:
        usage = shutil.disk_usage(path)
    except OSError:
        return None
    return {"path": str(path), "total": usage.total, "used": usage.used, "free": usage.free,
            "free_pct": 100.0 * usage.free / usage.total if usage.total else 0.0}


def _kind(name: str, columns: set) -> str:
    if name.startswith("_"):
        return KIND_INTERNAL
    if "close" in columns:
        return KIND_BARS
    if "time_msc" in columns and "bid" in columns:
        return KIND_TICKS
    if "time_utc" in columns:
        return KIND_BOOK
    return KIND_OTHER


def _sqlite_files(db) -> Dict[str, int]:
    main = Path(db.db_path)
    return {suffix or "base": os.path.getsize(str(main) + suffix)
            for suffix in ("", "-wal", "-shm", "-journal") if os.path.exists(str(main) + suffix)}


def _sqlite_sizes(conn) -> Dict[str, int]:
    """Bytes por tabela (índices somados à tabela) pelo `dbstat`; {} se o SQLite não tiver a extensão."""
    try:
        owners = {r[0]: r[1] for r in conn.execute(text(
            "SELECT name, tbl_name FROM sqlite_master WHERE type IN ('table', 'index')")).fetchall()}
        sizes: Dict[str, int] = {}
        for name, size in conn.execute(text("SELECT name, SUM(pgsize) FROM dbstat GROUP BY name")).fetchall():
            owner = owners.get(name, name)
            sizes[owner] = sizes.get(owner, 0) + int(size or 0)
        return sizes
    except Exception as e:      # dbstat ausente: o chamador estima pelas linhas
        log.debug(f"dbstat indisponível: {e}")
        return {}


def measure(db, archive: Optional[Path] = None, with_dirs: bool = True) -> dict:
    """
    Retrato do espaço. Nunca lança: o que não puder ser medido fica None e o motivo vai em "warnings".
    tables: [{table, kind, rows, bytes, estimated (bytes estimados pelas linhas), symbol, timeframe, start, end}]
    """
    report = {"measured_at": datetime.now(), "dialect": getattr(db, "dialect", None), "db_bytes": None,
              "files": {}, "reclaimable_bytes": None, "disk": None, "tables": [], "dirs": {}, "warnings": []}
    if not _usable(db):
        report["warnings"].append("Base de dados indisponível.")
        return report
    try:
        inventory = {item["table"]: item for item in db.data_inventory()}
    except Exception as e:
        log.error(f"Não foi possível ler o inventário da base: {e}")
        report["warnings"].append(f"Inventário indisponível: {e}")
        inventory = {}
    dialect = db.dialect
    try:
        with db.engine.connect() as conn:
            inspector = inspect(conn)
            names = [n for n in inspector.get_table_names() if not n.startswith("sqlite_")]
            if dialect == "sqlite":
                files = _sqlite_files(db)
                report["files"] = files
                report["db_bytes"] = sum(files.values())
                page_size = conn.execute(text("PRAGMA page_size")).scalar() or 0
                free_pages = conn.execute(text("PRAGMA freelist_count")).scalar() or 0
                report["reclaimable_bytes"] = int(page_size) * int(free_pages)
                sizes = _sqlite_sizes(conn)
                report["disk"] = disk_usage(Path(db.db_path).resolve().parent)
            else:
                report["db_bytes"] = int(conn.execute(text("SELECT pg_database_size(current_database())")).scalar())
                sizes = {r[0]: int(r[1]) for r in conn.execute(text(
                    "SELECT relname, pg_total_relation_size(relid) FROM pg_catalog.pg_statio_user_tables")).fetchall()}
            tables = []
            for name in names:
                item = inventory.get(name)
                if item is not None:
                    rows = item["rows"]
                    columns = {"close"}
                else:
                    columns = {c["name"] for c in inspector.get_columns(name)}
                    rows = int(conn.execute(text(f'SELECT COUNT(*) FROM "{name}"')).scalar() or 0)
                tables.append({"table": name, "kind": _kind(name, columns), "rows": rows, "bytes": sizes.get(name),
                               "estimated": name not in sizes,
                               "symbol": item["symbol"] if item else None,
                               "timeframe": item["timeframe"] if item else None,
                               "start": item["start"] if item else None, "end": item["end"] if item else None})
    except Exception as e:
        log.error(f"Erro ao medir a base: {e}")
        report["warnings"].append(f"Medição incompleta: {e}")
        return report
    _estimate_missing(tables, report["db_bytes"], report["reclaimable_bytes"])
    tables.sort(key=lambda t: (t["bytes"] or 0), reverse=True)
    report["tables"] = tables
    if with_dirs:
        from mt5_extracao.resources import policy as policy_mod
        archive = archive or policy_mod.archive_dir(db)
        for label, locate in EXTRA_DIRS.items():
            try:
                path = Path(locate(db, archive))
                report["dirs"][label] = {"path": str(path), "bytes": dir_bytes(path)}
            except Exception as e:
                report["warnings"].append(f"Pasta {label}: {e}")
    return report


def _estimate_missing(tables: List[dict], db_bytes: Optional[int], reclaimable: Optional[int]) -> None:
    """Sem `dbstat`: divide o espaço ocupado entre as tabelas na proporção das linhas (marcadas como estimadas)."""
    missing = [t for t in tables if t["bytes"] is None]
    if not missing or not db_bytes:
        return
    known = sum(t["bytes"] for t in tables if t["bytes"] is not None)
    budget = max(0, db_bytes - (reclaimable or 0) - known)
    total_rows = sum(t["rows"] for t in missing) or len(missing)
    for t in missing:
        t["bytes"] = int(budget * (t["rows"] or (1 if total_rows == len(missing) else 0)) / total_rows)
        t["estimated"] = True


def totals_by_kind(report: dict) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for t in report.get("tables", []):
        out[t["kind"]] = out.get(t["kind"], 0) + (t["bytes"] or 0)
    return out


# --- retratos e crescimento ------------------------------------------------------------

def record_snapshot(db, report: dict, keep_days: int = 365) -> bool:
    """Grava o retrato (um por minuto, no máximo) e apaga os mais velhos que `keep_days`. False se não deu."""
    if not _usable(db) or report.get("db_bytes") is None:
        return False
    disk = report.get("disk") or {}
    taken = report["measured_at"].strftime("%Y-%m-%d %H:%M")
    rows = sum(t["rows"] for t in report["tables"] if t["kind"] != KIND_INTERNAL)
    data_tables = sum(1 for t in report["tables"] if t["kind"] != KIND_INTERNAL)
    cutoff = (report["measured_at"] - timedelta(days=keep_days)).strftime("%Y-%m-%d %H:%M")
    try:
        with db.engine.begin() as conn:
            conn.execute(text(_SNAPSHOTS))
            conn.execute(text("DELETE FROM _resource_snapshots WHERE taken_at = :t OR taken_at < :c"),
                         {"t": taken, "c": cutoff})
            conn.execute(text("INSERT INTO _resource_snapshots VALUES (:t, :b, :r, :f, :tot, :n, :rows)"),
                         {"t": taken, "b": int(report["db_bytes"]), "r": report.get("reclaimable_bytes"),
                          "f": disk.get("free"), "tot": disk.get("total"), "n": data_tables, "rows": rows})
        return True
    except Exception as e:
        log.error(f"Não foi possível gravar o retrato de espaço: {e}")
        return False


def load_snapshots(db, days: Optional[int] = None) -> List[dict]:
    """Retratos do mais antigo ao mais novo ([] se ainda não há)."""
    if not _usable(db) or not inspect(db.engine).has_table("_resource_snapshots"):
        return []
    sql, params = "SELECT * FROM _resource_snapshots", {}
    if days:
        sql += " WHERE taken_at >= :c"
        params["c"] = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M")
    with db.engine.connect() as conn:
        rows = [dict(r) for r in conn.execute(text(sql + " ORDER BY taken_at"), params).mappings()]
    for r in rows:
        r["taken_at"] = datetime.strptime(r["taken_at"], "%Y-%m-%d %H:%M")
    return rows


def growth(snapshots: List[dict], window_days: int = 30) -> Optional[dict]:
    """
    Crescimento da base por dia (regressão simples entre o primeiro e o último retrato da janela) e em quantos dias o
    disco enche nesse ritmo. None com menos de dois retratos separados por pelo menos uma hora.
    """
    if len(snapshots) < 2:
        return None
    last = snapshots[-1]
    window = [s for s in snapshots if s["taken_at"] >= last["taken_at"] - timedelta(days=window_days)]
    first = window[0]
    hours = (last["taken_at"] - first["taken_at"]).total_seconds() / 3600
    if hours < 1:
        return None
    per_day = (last["db_bytes"] - first["db_bytes"]) / hours * 24
    days_to_full = None
    if per_day > 0 and last.get("disk_free"):
        days_to_full = last["disk_free"] / per_day
    return {"bytes_per_day": per_day, "days_to_full": days_to_full, "since": first["taken_at"]}


def human_bytes(value) -> str:
    """1536 -> '1,5 KB' (padrão brasileiro); None -> '—'."""
    if value is None:
        return "—"
    value = float(value)
    sign = "-" if value < 0 else ""
    value = abs(value)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            text_value = f"{value:.0f}" if unit == "B" else f"{value:.1f}".replace(".", ",")
            return f"{sign}{text_value} {unit}"
        value /= 1024
    return f"{sign}{value}"
