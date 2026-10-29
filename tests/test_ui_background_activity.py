"""Espera visível no worker/fila reais: concorrência, animação, erro e encerramento."""
import queue
import threading
import time

import pytest

tk = pytest.importorskip("tkinter")
from tkinter import ttk

from mt5_extracao.ui.components import BackgroundActivity
from mt5_extracao.ui.theme import Theme
from mt5_extracao.ui_manager import UIManager


@pytest.fixture
def ui():
    try:
        root = tk.Tk()
    except tk.TclError as error:
        pytest.skip(f"sem display gráfico: {error}")
    ui = UIManager.__new__(UIManager)
    ui.root, ui.theme, ui._ui_queue = root, Theme(root), queue.Queue()
    root.geometry("900x300")
    ui.background_activity = BackgroundActivity(root)
    ui.background_activity.pack(fill=tk.X)
    ui.notebook = ttk.Notebook(root)
    ui.notebook.pack(fill=tk.BOTH, expand=True)
    for name in ("Análise", "Laboratório", "Recursos"):
        ui.notebook.add(ttk.Frame(ui.notebook), text=name)
    ui.releases = []
    yield ui
    for event in ui.releases:
        event.set()
    root.destroy()


def _release(ui):
    event = threading.Event()
    ui.releases.append(event)
    return event


def _result(ui):
    fn, args = ui._ui_queue.get(timeout=5)
    return fn(*args)


def _pump(ui, seconds=.15):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        ui.root.update()
        time.sleep(.01)


def test_overlapping_tasks_keep_animation_and_original_context_until_last_callback(ui):
    first, second = _release(ui), _release(ui)
    callbacks = []
    activity = ui.background_activity
    ui.run_bg(lambda: (first.wait(5), 1)[1], lambda result, error: callbacks.append((result, error, len(activity.pending))))
    ui.notebook.select(1)
    ui.run_bg(lambda: (second.wait(5), 2)[1], lambda result, error: callbacks.append((result, error, len(activity.pending))))
    _pump(ui)
    assert activity.progress.winfo_viewable()
    text = activity.message.cget("text")
    assert "Análise" in text and "Laboratório" in text and "2 tarefas" in text and "aguarde" in text
    before = float(activity.progress.cget("value"))
    _pump(ui)
    assert float(activity.progress.cget("value")) != before
    ui.notebook.select(2)  # trocar a tela não relabela nem perde a tarefa anterior
    second.set()
    _result(ui)
    assert len(activity.pending) == 1 and "Análise" in activity.message.cget("text")
    assert "Recursos" not in activity.message.cget("text")
    first.set()
    _result(ui)
    _pump(ui)
    assert callbacks == [(2, None, 2), (1, None, 1)]
    assert not activity.pending and not activity.progress.winfo_viewable()
    assert "finalizado" in activity.message.cget("text") and not activity.elapsed.cget("text")
    assert activity._tick_job is None


@pytest.mark.parametrize("failure", ["worker", "callback"])
def test_error_clears_wait_and_allows_next_request(ui, failure):
    activity = ui.background_activity
    release = _release(ui)
    def work():
        release.wait(5)
        if failure == "worker":
            raise ValueError("falha controlada")
        return 7
    seen = []
    def done(result, error):
        seen.append((result, error))
        if failure == "callback":
            raise RuntimeError("falha ao apresentar")
    ui.run_bg(work, done)
    release.set()
    if failure == "callback":
        with pytest.raises(RuntimeError, match="apresentar"):
            _result(ui)
    else:
        _result(ui)
        assert seen[0][0] is None and isinstance(seen[0][1], ValueError)
    assert not activity.pending and "erro" in activity.message.cget("text")
    ui.run_bg(lambda: 8, lambda *_: None)
    _result(ui)
    assert "erro" not in activity.message.cget("text") and not activity.pending


def test_chained_request_and_stale_callback_do_not_stop_new_wait(ui):
    activity = ui.background_activity
    release = _release(ui)
    def next_request(*_):
        ui.run_bg(lambda: release.wait(5), lambda *_: None)
    ui.run_bg(lambda: "antigo", next_request)
    _result(ui)
    assert len(activity.pending) == 1
    # Um encerramento duplicado/obsoleto não pode apagar outro processamento.
    activity.finish(object())
    assert len(activity.pending) == 1
    release.set()
    _result(ui)
    assert not activity.pending


def test_destroy_during_wait_cancels_animation_and_late_result_is_safe(ui):
    release = _release(ui)
    activity = ui.background_activity
    ui.run_bg(lambda: release.wait(5), lambda *_: None)
    _pump(ui)
    activity.destroy()
    assert activity._closed and activity._tick_job is None and not activity.pending
    release.set()
    _result(ui)


def test_background_caller_marshals_indicator_and_callback_to_ui_thread(ui):
    release = _release(ui)
    seen = []
    caller = threading.Thread(target=lambda: ui.run_bg(lambda: release.wait(5),
        lambda *_: seen.append(threading.current_thread())))
    caller.start()
    caller.join(5)
    assert not caller.is_alive() and not ui.background_activity.pending
    _result(ui)  # begin enfileirado; nenhum Tk na thread chamadora
    assert ui.background_activity.pending
    release.set()
    _result(ui)
    assert seen == [threading.main_thread()] and not ui.background_activity.pending


def test_worker_start_error_does_not_leave_permanent_wait(ui, monkeypatch):
    def failed_start(_thread):
        raise RuntimeError("sem recurso para iniciar")
    monkeypatch.setattr(threading.Thread, "start", failed_start)
    with pytest.raises(RuntimeError, match="sem recurso"):
        ui.run_bg(lambda: 1, lambda *_: None)
    assert not ui.background_activity.pending
    assert ui.background_activity._tick_job is None
    assert "erro" in ui.background_activity.message.cget("text")
