"""
Automação opt-in (T15.5): um ciclo mede, grava o retrato, avalia os alertas e, só com as chaves ligadas em
`[RESOURCES]`, executa as ações permitidas. Sem `auto_enabled = true`, `run_cycle(..., origin="auto")` só mede.

O ciclo roda pela interface (agendador da aba Recursos) ou pelo sistema operacional (`mt5x resources auto`, no cron
ou no Agendador de Tarefas). A trava de `actions.exclusive` impede dois ciclos ao mesmo tempo.
"""
import logging
from datetime import datetime, timedelta
from typing import Iterable, List, Optional

from sqlalchemy import text

from mt5_extracao.resources import actions, hygiene, storage
from mt5_extracao.resources import policy as policy_mod

log = logging.getLogger(__name__)

LAST_CYCLE_KEY = "resources_last_cycle"
GB = 1024 ** 3
MB = 1024 ** 2


def alerts(report: dict, policy) -> List[dict]:
    """[{level: 'aviso'|'crítico', text}] a partir do retrato e dos limites da política."""
    out = []
    db_bytes = report.get("db_bytes")
    if policy.alert_db_gb and db_bytes is not None and db_bytes > policy.alert_db_gb * GB:
        out.append({"level": "aviso", "code": "base_grande",
                    "text": f"A base ocupa {storage.human_bytes(db_bytes)}, acima do limite de "
                            f"{storage.human_bytes(policy.alert_db_gb * GB)}. Veja a aba Recursos > Higienização."})
    disk = report.get("disk")
    if disk and policy.alert_disk_free_pct and disk["free_pct"] < policy.alert_disk_free_pct:
        out.append({"level": "crítico", "code": "disco_cheio",
                    "text": f"Disco com só {disk['free_pct']:.1f}% livre ({storage.human_bytes(disk['free'])}). "
                            "Arquive séries antigas ou libere espaço; extrações podem falhar."})
    reclaim = report.get("reclaimable_bytes")
    if reclaim and reclaim > max(policy.vacuum_min_mb, 1) * MB:
        out.append({"level": "aviso", "code": "recuperavel",
                    "text": f"{storage.human_bytes(reclaim)} livres dentro da base podem voltar ao disco com "
                            "\"Otimizar\"."})
    return out


def last_cycle(db) -> Optional[datetime]:
    try:
        value = db.get_metadata(LAST_CYCLE_KEY)
        return datetime.fromisoformat(value) if value else None
    except Exception:
        return None


def _set_last_cycle(db, when: datetime) -> None:
    try:
        with db.engine.begin() as conn:
            conn.execute(text("DELETE FROM _metadata WHERE key = :k"), {"k": LAST_CYCLE_KEY})
            conn.execute(text("INSERT INTO _metadata (key, value) VALUES (:k, :v)"),
                         {"k": LAST_CYCLE_KEY, "v": when.isoformat(timespec="seconds")})
    except Exception as e:
        log.error(f"Não foi possível gravar a hora do último ciclo de recursos: {e}")


def is_due(policy, last: Optional[datetime], now: Optional[datetime] = None) -> bool:
    if not policy.auto_enabled:
        return False
    now = now or datetime.now()
    return last is None or now - last >= timedelta(minutes=policy.monitor_interval_minutes)


def run_cycle(db, config, origin: str = "auto", selected: Iterable[str] = (), notify_fn=None,
              now: Optional[datetime] = None) -> dict:
    """
    Um ciclo. origin='auto' respeita as chaves de automação; outra origem (ex.: 'ui', botão "Rodar agora") só mede e
    avalia os alertas, a menos que a automação esteja ligada. Devolve {report, alerts, actions, policy_errors}.
    """
    policy = policy_mod.load(config)
    now = now or datetime.now()
    report = storage.measure(db, policy_mod.archive_dir(db, policy))
    storage.record_snapshot(db, report, policy.keep_snapshots_days)
    found_alerts = alerts(report, policy)
    done: List[dict] = []
    if policy.auto_enabled:
        try:
            if policy.auto_clean_orphans:
                cleaned = actions.clean_orphans(db, config, origin)
                if any(cleaned["removed"].values()):
                    done.append({"action": "órfãos", "result": cleaned["removed"]})
            if policy.auto_archive and policy.auto_archive_rules:
                sizes = {t["table"]: t["bytes"] for t in report["tables"]}
                found = hygiene.scan(db, policy, selected, sizes, now=now, rules=policy.auto_archive_rules)
                targets = {c["table"]: c["reasons"] for c in found["candidates"]}
                if targets:
                    results = actions.archive_tables(db, config, list(targets), origin, reasons=targets)
                    done.append({"action": "arquivar", "result": results})
            reclaim = storage.measure(db, with_dirs=False).get("reclaimable_bytes") if done else \
                report.get("reclaimable_bytes")
            if policy.auto_vacuum and reclaim and reclaim >= policy.vacuum_min_mb * MB:
                done.append({"action": "otimizar", "result": actions.vacuum(db, config, origin)})
        except actions.Busy as e:
            done.append({"action": "aguardando", "result": str(e)})
        _set_last_cycle(db, now)
    if notify_fn:
        for alert in found_alerts:
            if alert["code"] != "recuperavel":
                notify_fn("Recursos: " + ("atenção" if alert["level"] == "aviso" else "crítico"), alert["text"],
                          alert["level"])
        archived = [r for d in done if d["action"] == "arquivar" for r in d["result"] if r["ok"]]
        if archived:
            notify_fn("Recursos: séries arquivadas", f"{len(archived)} série(s) arquivada(s) automaticamente. "
                                                     "Restaure pela aba Recursos > Arquivo.", "aviso")
    return {"report": report, "alerts": found_alerts, "actions": done, "policy_errors": policy.errors,
            "ran_at": now}
