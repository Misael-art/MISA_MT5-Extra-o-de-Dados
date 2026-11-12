"""A GUI solicita consultas e recuperação pelo worker de Recursos."""
from mt5_extracao.ui import resources


def panel_with_worker(monkeypatch):
    panel = resources.ResourcesPanel.__new__(resources.ResourcesPanel)
    panel.frame = None
    db, config = object(), object()
    panel._db = lambda: db
    panel._config = lambda: config
    work = []
    panel._run_action = lambda label, task, done: work.append((task, done))
    monkeypatch.setattr(resources.messagebox, "showinfo", lambda *args, **kwargs: None)
    return panel, work, db, config


def test_restore_status_io_is_deferred_until_worker_runs(monkeypatch):
    panel, work, db, config = panel_with_worker(monkeypatch)
    calls = []

    def status(actual_db, actual_config):
        calls.append((actual_db, actual_config))
        return {"ok": True, "operations": []}

    monkeypatch.setattr(resources.files, "restore_status", status)
    panel.check_restore()
    assert not calls and len(work) == 1
    task, done = work[0]
    done(task())
    assert calls == [(db, config)]


def test_recovery_requires_confirmation_and_is_requested_from_worker(monkeypatch):
    panel, work, db, config = panel_with_worker(monkeypatch)
    operation_id = "1" * 32
    older_uncertain = {"operation_id": operation_id, "state": "uncertain", "safety_copy_available": True,
                       "safety_copy": "previous.db"}
    monkeypatch.setattr(resources.files, "restore_status", lambda *args: {
        "ok": True, "operations": [{"operation_id": "2" * 32, "state": "failed"}, older_uncertain]})
    confirmations = []
    monkeypatch.setattr(resources.messagebox, "askyesno", lambda *args, **kwargs: confirmations.append(args) or True)
    calls = []
    monkeypatch.setattr(resources.files, "restore_backup", lambda *args, **kwargs: calls.append((args, kwargs)))
    panel.check_restore()
    task, done = work[0]
    done(task())
    assert len(confirmations) == 1 and not calls and len(work) == 2
    work[1][0]()
    assert calls == [((db, config, "previous.db", "ui"), {"recovery_of": operation_id})]


def test_declining_recovery_schedules_no_mutation(monkeypatch):
    panel, work, _db, _config = panel_with_worker(monkeypatch)
    monkeypatch.setattr(resources.files, "restore_status", lambda *args: {
        "ok": True, "operations": [{"operation_id": "1" * 32, "state": "uncertain",
                                    "safety_copy_available": True, "safety_copy": "previous.db"}]})
    monkeypatch.setattr(resources.messagebox, "askyesno", lambda *args, **kwargs: False)
    panel.check_restore()
    task, done = work[0]
    done(task())
    assert len(work) == 1
