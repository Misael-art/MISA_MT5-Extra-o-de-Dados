"""
Política de recursos lida de `[RESOURCES]` no config.ini. Toda chave tem padrão (`fallback=`): uma configuração
antiga, sem a seção, funciona com tudo desligado.
"""
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Tuple

SECTION = "RESOURCES"
RULE_CODES = ("sem_lucro", "sem_estudo", "obsoleta", "vazia")


@dataclass
class Policy:
    auto_enabled: bool = False
    monitor_interval_minutes: int = 60
    alert_db_gb: float = 10.0
    alert_disk_free_pct: float = 10.0
    auto_vacuum: bool = False
    vacuum_min_mb: float = 200.0
    auto_clean_orphans: bool = False
    auto_archive: bool = False
    auto_archive_rules: Tuple[str, ...] = ("obsoleta", "vazia")
    obsolete_days: int = 180
    archive_dir: str = ""
    keep_snapshots_days: int = 365
    auto_backup: bool = False
    backup_interval_hours: int = 24
    backup_keep: int = 5
    backup_dir: str = ""
    auto_clean_files: bool = False
    logs_keep_days: int = 30
    exports_keep_days: int = 0
    errors: list = field(default_factory=list)   # chaves com valor inválido (usou o padrão)


def _get(config, key, cast, default, errors):
    if config is None or not config.has_section(SECTION):
        return default
    raw = config.get(SECTION, key, fallback=None)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        if cast is bool:
            value = str(raw).strip().lower()
            if value not in ("1", "0", "true", "false", "yes", "no", "sim", "nao", "não", "on", "off"):
                raise ValueError(raw)
            return value in ("1", "true", "yes", "sim", "on")
        result = cast(str(raw).strip().replace(",", "."))
        if isinstance(result, (int, float)) and result < 0:
            raise ValueError(raw)
        return result
    except ValueError:
        errors.append(f"[{SECTION}] {key} = {raw!r} é inválido; usado o padrão {default!r}")
        return default


def load(config) -> Policy:
    errors: list = []
    d = Policy()
    rules_raw = config.get(SECTION, "auto_archive_rules", fallback=None) if config is not None and \
        config.has_section(SECTION) else None
    rules = d.auto_archive_rules
    if rules_raw is not None and rules_raw.strip():
        parsed = tuple(r.strip() for r in rules_raw.split(",") if r.strip())
        unknown = [r for r in parsed if r not in RULE_CODES]
        if unknown:
            errors.append(f"[{SECTION}] auto_archive_rules: regra(s) desconhecida(s) {', '.join(unknown)} ignorada(s)")
        rules = tuple(r for r in parsed if r in RULE_CODES)
    return Policy(
        auto_enabled=_get(config, "auto_enabled", bool, d.auto_enabled, errors),
        monitor_interval_minutes=max(5, _get(config, "monitor_interval_minutes", int, d.monitor_interval_minutes,
                                             errors)),
        alert_db_gb=_get(config, "alert_db_gb", float, d.alert_db_gb, errors),
        alert_disk_free_pct=_get(config, "alert_disk_free_pct", float, d.alert_disk_free_pct, errors),
        auto_vacuum=_get(config, "auto_vacuum", bool, d.auto_vacuum, errors),
        vacuum_min_mb=_get(config, "vacuum_min_mb", float, d.vacuum_min_mb, errors),
        auto_clean_orphans=_get(config, "auto_clean_orphans", bool, d.auto_clean_orphans, errors),
        auto_archive=_get(config, "auto_archive", bool, d.auto_archive, errors),
        auto_archive_rules=rules,
        obsolete_days=max(1, _get(config, "obsolete_days", int, d.obsolete_days, errors)),
        archive_dir=(config.get(SECTION, "archive_dir", fallback="") if config is not None and
                     config.has_section(SECTION) else "") or "",
        keep_snapshots_days=max(1, _get(config, "keep_snapshots_days", int, d.keep_snapshots_days, errors)),
        auto_backup=_get(config, "auto_backup", bool, d.auto_backup, errors),
        backup_interval_hours=max(1, _get(config, "backup_interval_hours", int, d.backup_interval_hours, errors)),
        backup_keep=max(1, _get(config, "backup_keep", int, d.backup_keep, errors)),
        backup_dir=(config.get(SECTION, "backup_dir", fallback="") if config is not None and
                    config.has_section(SECTION) else "") or "",
        auto_clean_files=_get(config, "auto_clean_files", bool, d.auto_clean_files, errors),
        logs_keep_days=_get(config, "logs_keep_days", int, d.logs_keep_days, errors),
        exports_keep_days=_get(config, "exports_keep_days", int, d.exports_keep_days, errors),
        errors=errors)


def archive_dir(db, policy: Optional[Policy] = None) -> Path:
    """Pasta do arquivo: a configurada, ou `arquivo/` ao lado do arquivo SQLite (ou `database/arquivo` no PostgreSQL)."""
    if policy is not None and policy.archive_dir:
        return Path(os.path.expanduser(policy.archive_dir))
    if getattr(db, "db_type", None) == "sqlite" and getattr(db, "db_path", None):
        return Path(db.db_path).resolve().parent / "arquivo"
    return Path("database") / "arquivo"
