"""Consulta real do store, sem cálculo/MT5 nem escrita no journal por leitura."""
import configparser
import copy
from types import SimpleNamespace

import pytest
from sqlalchemy import inspect

from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.experiments import service, store
from tests.experiment_helpers import setup

tk = pytest.importorskip("tkinter")


@pytest.fixture
def env(tmp_path):
    try:
        root = tk.Tk()
    except tk.TclError as error:
        pytest.skip(f"sem display gráfico: {error}")
    from mt5_extracao.ui.theme import Theme
    from mt5_extracao.ui.experiments import ExperimentsDialog
    db = DatabaseManager(db_path=str(tmp_path / "registry.db"))
    jobs = []
    ui = SimpleNamespace(root=root, theme=Theme(root), app=SimpleNamespace(db_manager=db),
                         run_bg=lambda work, done: jobs.append((work, done)))
    dialog = ExperimentsDialog(ui)
    def drain():
        for _ in range(30):
            root.update()
            if not jobs:
                return
            work, done = jobs.pop(0)
            try:
                value = work()
            except Exception as error:
                done(None, error)
            else:
                done(value, None)
        raise AssertionError("consulta não estabilizou")
    yield SimpleNamespace(root=root, db=db, ui=ui, dialog=dialog, jobs=jobs, drain=drain)
    root.destroy()
    db.close()


def read(widget):
    return widget.get("1.0", "end-1c")


def register(env):
    definition, composition, bars = setup(env.db, "synthetic_study")
    first = service.create(env.db, definition)["protocol_id"]
    service.holdout_access(env.db, first, "adjust", purpose="tuning")
    definition = copy.deepcopy(definition)
    definition["revision"] = 2
    second = service.create(env.db, definition)["protocol_id"]
    discarded = service.request(env.db, second, "discard", composition, discard_reason="controle QA")
    invalid = composition.to_dict()
    invalid["sizing"]["params"]["compound"] = False
    refused = service.request(env.db, second, "refuse", invalid)
    return first, second, discarded, refused


def test_empty_legacy_database_is_read_without_creating_experiment_tables(env):
    before = inspect(env.db.engine).get_table_names()
    assert store.list_protocols(env.db) == []
    env.drain()
    assert env.dialog.protocols.get_children() == ()
    assert "Nenhum protocolo" in env.dialog.note.cget("text")
    assert inspect(env.db.engine).get_table_names() == before


def test_real_metadata_rejections_and_cross_protocol_exposure_do_not_mutate_journal(env, monkeypatch):
    from mt5_extracao.ui_manager import UIManager
    first, second, discarded, refused = register(env)
    before = store.status(env.db, second)
    monkeypatch.setattr(service, "evaluate", lambda *a, **kw: pytest.fail("consulta não calcula"))
    env.dialog.reload();env.drain()
    assert set(env.dialog.protocols.get_children()) == {first, second}
    env.dialog.protocols.selection_set(second);env.drain()
    assert env.dialog.snapshot["protocol"]["protocol_id"] == second
    assert env.dialog.requests.item(discarded["request_id"], "values")[0] == "Descartado"
    assert env.dialog.requests.item(refused["request_id"], "values")[0] == "Recusado"
    env.dialog.requests.selection_set(refused["request_id"]);env.drain()
    assert refused["error"] in read(env.dialog.request_detail)
    assert "Usada para ajuste" in read(env.dialog.exposure_text)
    assert "Reserva final qualificada: não" in read(env.dialog.exposure_text)
    assert "evaluation_request" in read(env.dialog.protocol_text)
    assert store.status(env.db, second) == before
    opened = UIManager.open_experiment_registry(env.ui)
    assert opened.title() == "Registro de estudos"
    opened.destroy();env.drain()


def test_stale_callback_close_and_error_then_refresh(env, monkeypatch):
    first, second, _, _ = register(env)
    env.drain()
    env.dialog.protocols.selection_set(first);env.root.update()
    old_work, old_done = env.jobs.pop(0)
    env.jobs.clear()
    old_snapshot = old_work()
    env.dialog.protocols.selection_set(second);env.root.update();env.drain()
    old_done(old_snapshot, None)
    assert env.dialog.snapshot["protocol"]["protocol_id"] == second
    original = store.status
    def broken(*args):
        raise ValueError("leitura QA indisponível")
    monkeypatch.setattr(store, "status", broken)
    env.dialog.select_protocol();env.drain()
    assert env.dialog.snapshot is None and "Atualizar" in env.dialog.note.cget("text")
    monkeypatch.setattr(store, "status", original)
    env.dialog.refresh_button.invoke();env.drain()
    assert env.dialog.snapshot["protocol"]["protocol_id"] == second
    env.dialog.select_protocol();work, done = env.jobs.pop(0)
    env.dialog.destroy()
    done(work(), None)  # fechamento não tenta tocar widgets destruídos


@pytest.mark.parametrize("dark", [False, True])
def test_small_window_keyboard_readonly_details_and_theme(env, dark):
    first, second, _, _ = register(env)
    env.drain()
    if env.ui.theme.dark != dark:
        env.ui.theme.toggle()
    env.dialog.geometry("700x600")
    env.root.update()
    env.dialog.protocols.focus_set()
    env.dialog.protocols.selection_set(second)
    env.drain()
    assert env.dialog.protocol_id == second
    for widget in (env.dialog.protocol_text, env.dialog.exposure_text, env.dialog.request_detail):
        assert str(widget.cget("state")) == "disabled"
    env.dialog.tabs.select(env.dialog.pages["Protocolo"])
    env.dialog.protocol_text.focus_set()
    env.dialog.protocol_text.event_generate("<Next>")
    env.root.update()
    assert env.dialog.protocol_text.winfo_viewable()
    button = env.dialog.refresh_button
    assert button.winfo_rootx() + button.winfo_width() <= env.dialog.winfo_rootx() + env.dialog.winfo_width()
    assert button.winfo_viewable()


def test_reused_observation_is_labeled_as_reuse_not_a_new_execution():
    from mt5_extracao.ui.experiments import STATES
    assert STATES["reused"] == "Resultado reutilizado"
    assert STATES["reused"] != STATES["run_recorded"]
