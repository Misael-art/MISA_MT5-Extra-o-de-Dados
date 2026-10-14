"""
Limites que bloqueiam a gravação (T15.9). O extrator chama o guarda antes de gravar cada bloco: se o disco estiver
abaixo de `[RESOURCES] block_disk_free_pct` ou a base acima de `block_db_gb`, a extração para com a mensagem do
motivo, em vez de encher o disco até falhar no meio de uma gravação. Ambos 0 (padrão) = nunca bloqueia.

    guard = make(db, config)       # None quando nenhum limite está ligado
    motivo = guard()               # None = pode gravar; texto = bloqueado

Barato: mede no máximo a cada `interval` segundos (tamanho do arquivo e disco livre, sem varrer tabelas).
"""
import logging
import time
from pathlib import Path
from typing import Callable, Optional

from sqlalchemy import text

from mt5_extracao.resources import policy as policy_mod
from mt5_extracao.resources import storage

log = logging.getLogger(__name__)

GB = 1024 ** 3


def _db_bytes(db) -> Optional[int]:
    try:
        if db.dialect == "sqlite":
            return sum(storage._sqlite_files(db).values())
        with db.engine.connect() as conn:
            return int(conn.execute(text("SELECT pg_database_size(current_database())")).scalar())
    except Exception as e:
        log.debug(f"Tamanho da base não medido pelo guarda: {e}")
        return None


def check(db, policy) -> Optional[str]:
    """Motivo do bloqueio, ou None."""
    if policy.block_disk_free_pct and getattr(db, "db_type", None) == "sqlite":
        disk = storage.disk_usage(Path(db.db_path).resolve().parent)
        if disk and disk["free_pct"] < policy.block_disk_free_pct:
            return (f"disco com só {disk['free_pct']:.1f}% livre (limite para gravar: {policy.block_disk_free_pct:g}%). "
                    "Libere espaço na aba Recursos (arquivar, aparar, otimizar) ou ajuste [RESOURCES] "
                    "block_disk_free_pct")
    if policy.block_db_gb:
        size = _db_bytes(db)
        if size is not None and size > policy.block_db_gb * GB:
            return (f"a base ocupa {storage.human_bytes(size)}, acima do limite para gravar "
                    f"({storage.human_bytes(policy.block_db_gb * GB)}). Libere espaço na aba Recursos ou ajuste "
                    "[RESOURCES] block_db_gb")
    return None


def make(db, config, interval: float = 30.0) -> Optional[Callable[[], Optional[str]]]:
    """Guarda com cache curto; None se nenhum limite estiver ligado (o extrator nem chama)."""
    policy = policy_mod.load(config)
    if not (policy.block_disk_free_pct or policy.block_db_gb):
        return None
    state = {"at": None, "reason": None}     # None: a primeira chamada sempre mede

    def guard() -> Optional[str]:
        now = time.monotonic()
        if state["at"] is None or now - state["at"] >= interval:
            state["at"], state["reason"] = now, check(db, policy)
        return state["reason"]
    return guard

