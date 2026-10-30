"""Comandos dos cards, confirmação deliberada e ciclo de vida, com provedores falsos."""
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.test_ui_main_window import ui, _labels
from mt5_extracao.robots import catalog, controls
from mt5_extracao.ui import robot_controls
from tests.test_manual_panel_play import manual


def test_timeframe_selector_orders_duration_deduplicates_and_preserves_exact_context(ui):
    panel, robot, preset = _tested(ui)
    robot.timeframes = ("H4", "1 hora", "M15", "D1", "H1", "M1", "W1", "MN1", "M30")
    dialog = robot_controls.SelectionDialog(panel.controls, robot, "asset")
    assert tuple(dialog.tf_combo.cget("values")) == ("M1", "M5", "M15", "M30", "H1", "H4", "D1", "W1", "MN1")
    assert dialog.tf.get() == "H1"
    ui.pump(.1)
    assert dialog.window.grab_current() == dialog.window
    dialog.apply()
    assert panel.controls.chosen[robot.key] == ("WIN$N", "H1", preset)
    ui.toggle_theme()
    again = robot_controls.SelectionDialog(panel.controls, robot, "asset")
    assert again.tf.get() == "H1"
    again.window.destroy()


def test_python_timeframes_preserve_current_table_and_equivalent_label_after_refresh(ui):
    panel = ui.robots_panel
    panel.select("python:donchian")
    detail = panel.detail
    labels = ["H4", "1 hora", "M15", "M1", "D1", "M5", "H1", "1 minuto"]
    ui.symbol_series["WIN$N"] = [dict(symbol="WIN$N", timeframe=value, rows=600) for value in labels]
    detail.timeframe_var.set("H1")
    detail._symbol_changed()
    assert tuple(detail.timeframe_combo.cget("values")) == ("M1", "M5", "M15", "H1", "H4", "D1")
    assert detail.timeframe_var.get() == "H1"
    ui.symbol_series["WIN$N"] = [dict(symbol="WIN$N", timeframe=value, rows=600) for value in labels if value != "H1"]
    detail._symbol_changed()
    assert detail.timeframe_var.get() == "1 hora"
    detail.inventory_changed()
    assert detail.timeframe_var.get() == "1 hora"
    ui.symbol_series["WIN$N"] = []
    detail._symbol_changed()
    assert detail.timeframe_var.get() == "" and not detail.timeframe_combo.cget("values")


def test_manual_panel_play_is_available_without_backtest_and_respects_offline_mode(ui, manual):
    panel = ui.robots_panel
    panel.robots.insert(0, manual)
    panel.render()
    widgets = panel.controls.widgets[manual.key]
    assert not widgets["buttons"]["play"].instate(["disabled"])
    assert "sem backtest" in widgets["hint"].cget("text")
    assert manual.runs == []
    ui.app.offline = True
    panel.render()
    assert panel.controls.widgets[manual.key]["buttons"]["play"].instate(["disabled"])


def _tested(ui):
    panel = ui.robots_panel
    robot = next(r for r in panel.robots if r.kind == catalog.MQL5)
    preset = next(iter(robot.presets))
    robot.runs = [{"run_id": "test", "source": "strategy tester", "symbol": "WIN$N", "timeframe": "H1",
                   "data_start": "2026-01-01", "data_end": "2026-02-01", "created_at": "2026-02-02",
                   "params": dict(controls.effective_values(robot, preset), preset=preset), "equity": [],
                   "metrics": {"source_fingerprint": catalog.fingerprint(robot), "trades": 40,
                               "net_profit": -200, "return_pct": -.02, "profit_factor": .8, "max_drawdown_pct": .1, "win_rate": .4}}]
    panel.controls.chosen[robot.key] = "WIN$N", "H1", preset
    panel.selected_key = robot.key
    panel.render()
    return panel, robot, preset


@pytest.mark.parametrize("manual_panel", [False, True])
def test_play_offers_chart_choice_with_family_default_without_changing_risk(ui, manual, monkeypatch, manual_panel):
    panel, automatic, preset = _tested(ui)
    robot = manual if manual_panel else automatic
    if manual_panel:
        panel.controls.chosen[robot.key] = "EURUSD", "H1", None
    monkeypatch.setattr(controls, "play_issues", lambda *a: [])
    account = {"login": 123, "server": "FakeDemo", "demo": True}
    snapshot = {"currency": "USD", "positions": [], "totals": {"positions": 0, "orders": 0,
                "balance": 10000, "equity": 10000, "risk_pct": 0, "without_stop": 0,
                "margin_free": 10000, "risk_complete": True}}
    dialog = robot_controls.ExecutionDialog(panel.controls, robot, "play", account, snapshot)
    assert dialog.visible.get() is manual_panel
    original = controls.effective_values(robot, None if manual_panel else preset)
    dialog.visible.set(not manual_panel)
    assert controls.effective_values(robot, None if manual_panel else preset) == original
    assert not dialog.consent.confirmed and dialog.stage == 1
    dialog.window.destroy()


def test_replay_confirmation_passes_visible_and_launch_window_anchor(ui, monkeypatch):
    panel, robot, preset = _tested(ui)
    calls = []
    monkeypatch.setattr(panel.controls, "start", lambda *a, **kw: calls.append((a, kw)))
    dialog = robot_controls.ExecutionDialog(panel.controls, robot, "replay")
    assert dialog.visible.get()
    dialog.dates[0].set("2026-01-01"); dialog.dates[1].set("2026-01-02")
    anchor = (ui.root.winfo_rootx() + ui.root.winfo_width() // 2,
              ui.root.winfo_rooty() + ui.root.winfo_height() // 2)
    dialog.confirm()
    assert len(calls) == 1 and calls[0][1] == {"visible": True, "window_anchor": anchor}


def test_cards_have_drawn_actions_and_selected_preset_governs_stats_play_and_detail(ui):
    panel, robot, preset = _tested(ui)
    widget = panel.controls.widgets[robot.key]
    assert set(widget["buttons"]) == {"play", "replay", "backtest"}
    assert not widget["buttons"]["play"].instate(["disabled"])
    for name, button in widget["buttons"].items():
        assert button.cget("text") == name.upper() and button.cget("image")
    assert "0,80" in _labels(panel.cards[robot.key])
    other = next(name for name in robot.presets if name != preset)
    panel.controls._select(robot, "SELETOR · preset", other)
    assert panel.controls.widgets[robot.key]["buttons"]["play"].instate(["disabled"])
    assert "0,80" not in _labels(panel.cards[robot.key])
    assert panel.detail.source_var.get() == "Preset " + other
    ui.toggle_theme()
    assert panel.controls.chosen[robot.key][2] == other
    assert panel.controls.widgets[robot.key]["buttons"]["play"].instate(["disabled"])
    assert all(r.key not in panel.controls.widgets for r in panel.robots if r.kind == catalog.PYTHON)


def test_searchable_selectors_limit_rows_and_apply_context_only_on_request(ui):
    panel, robot, preset = _tested(ui)
    original = panel.controls.chosen[robot.key]
    ui.app.symbols = [f"ASSET{i:05}" for i in range(12000)] + ["WIN$N"]
    dialog = robot_controls.SelectionDialog(panel.controls, robot, "asset")
    assert len(dialog.tree.get_children()) == 500
    assert "12001" in dialog.count.cget("text")
    assert dialog.tree.item(dialog.tree.selection()[0], "values")[0] == "WIN$N"
    dialog.search.set("asset11999")
    assert len(dialog.tree.get_children()) == 1
    assert panel.controls.chosen[robot.key] == original
    dialog.tree.selection_set(dialog.tree.get_children()[0])
    dialog.tf.set("H4")
    dialog.apply()
    assert panel.controls.chosen[robot.key] == ("ASSET11999", "H4", preset)
    assert panel.controls.widgets[robot.key]["buttons"]["play"].instate(["disabled"])
    dialog = robot_controls.SelectionDialog(panel.controls, robot, "preset")
    dialog.search.set(controls.DEFAULT_PRESET)
    dialog.tree.selection_set(dialog.tree.get_children()[0])
    dialog.apply()
    assert panel.controls.chosen[robot.key][2] is None


def test_card_actions_reflow_without_truncation_and_missing_metrics_are_dashes(ui):
    panel, robot, preset = _tested(ui)
    for key in ("profit_factor", "max_drawdown_pct", "win_rate"):
        robot.runs[0]["metrics"].pop(key)
    panel.render()
    ui.notebook.select(panel.frame)
    for geometry in ("1280x800", "1920x1080", "1280x800"):
        ui.root.geometry(geometry)
        ui.pump(.2)
        buttons = panel.controls.widgets[robot.key]["buttons"]
        for button in buttons.values():
            assert button.winfo_width() >= button.winfo_reqwidth()
        assert buttons["backtest"].winfo_rooty() >= buttons["play"].winfo_rooty()
    ui.theme.font_small.configure(size=ui.theme.font_small.cget("size") + 6)
    panel.render()
    ui.pump(.2)
    buttons = panel.controls.widgets[robot.key]["buttons"]
    assert buttons["backtest"].winfo_rooty() > buttons["replay"].winfo_rooty() > buttons["play"].winfo_rooty()
    assert all(button.winfo_width() >= button.winfo_reqwidth() for button in buttons.values())
    assert "—" in _labels(panel.cards[robot.key])


def test_play_dialog_requires_typed_account_double_step_and_separate_negative_consent(ui, monkeypatch):
    panel, robot, preset = _tested(ui)
    monkeypatch.setattr(controls, "play_issues", lambda *a: [])
    calls, prompts = [], []
    monkeypatch.setattr(panel.controls, "start", lambda *a, **kw: calls.append(a))
    monkeypatch.setattr(robot_controls.messagebox, "showerror", lambda *a, **k: prompts.append(a))
    decision = [False]
    monkeypatch.setattr(robot_controls.messagebox, "askyesno", lambda *a, **k: decision[0])
    account = {"login": 123, "server": "FakeDemo", "demo": True}
    snapshot = {"currency": "USD", "positions": [], "totals": {"positions": 0, "orders": 0,
                "balance": 10000, "equity": 10000, "risk_pct": 0, "without_stop": 0,
                "margin_free": 10000, "risk_complete": True}}
    dialog = robot_controls.ExecutionDialog(panel.controls, robot, "play", account, snapshot)
    assert not dialog.consent.reviewed and not calls
    dialog.confirm()
    ui.pump(.1)
    assert dialog.stage == 2 and dialog.consent.reviewed and not calls
    assert dialog.account_entry.winfo_rooty() + dialog.account_entry.winfo_height() < dialog.button.winfo_rooty()
    dialog.account_entry.insert(0, "wrong")
    dialog.confirm()
    assert prompts and not calls
    dialog.account_entry.delete(0, "end")
    dialog.account_entry.insert(0, "123")
    dialog.confirm()
    assert not calls and not dialog.consent.confirmed
    decision[0] = True
    dialog.confirm()
    assert len(calls) == 1
    consent = calls[0][-1]
    assert consent.confirmed and consent.negative_confirmed and not consent.real_authorized


def test_real_dialog_red_band_and_explicit_opt_in_never_auto_authorize(ui, monkeypatch):
    panel, robot, preset = _tested(ui)
    monkeypatch.setattr(controls, "play_issues", lambda *a: [])
    monkeypatch.setattr(robot_controls.messagebox, "showerror", lambda *a, **k: None)
    monkeypatch.setattr(robot_controls.messagebox, "askyesno", lambda *a, **k: True)
    calls = []
    monkeypatch.setattr(panel.controls, "start", lambda *a, **kw: calls.append(a))
    account = {"login": 999, "server": "FakeReal", "demo": False}
    snapshot = {"currency": "USD", "positions": [], "totals": {"positions": 0, "orders": 0,
                "balance": 10000, "equity": 10000, "risk_pct": 0, "without_stop": 0,
                "margin_free": 10000, "risk_complete": True}}
    dialog = robot_controls.ExecutionDialog(panel.controls, robot, "play", account, snapshot)
    bands = [w for w in dialog.labels if "CONTA REAL" in w.cget("text")]
    assert len(bands) == 1 and str(bands[0].cget("background")) == ui.theme.colors["bad"]
    dialog.confirm()
    dialog.account_entry.insert(0, "999")
    dialog.confirm()
    assert not calls and not dialog.real_var.get()
    dialog.real_var.set(True)
    dialog.confirm()
    assert calls[0][-1].real_authorized


def test_replay_has_one_start_action_and_checks_date_order_without_account_consent(ui, monkeypatch):
    panel, robot, preset = _tested(ui)
    calls, errors = [], []
    monkeypatch.setattr(panel.controls, "start", lambda *a, **kw: calls.append(a))
    monkeypatch.setattr(robot_controls.messagebox, "showerror", lambda *a, **k: errors.append(a))
    dialog = robot_controls.ExecutionDialog(panel.controls, robot, "replay")
    dialog.dates[0].set("invalid")
    dialog.confirm()
    assert errors and not calls
    dialog.dates[0].set("2026-01-01")
    dialog.confirm()
    assert len(calls) == 1 and calls[0][1] == "replay" and calls[0][-1] is None


def test_session_status_pulses_by_scoped_telemetry_with_text_and_stops_own_job(ui):
    panel, robot, preset = _tested(ui)
    state = ["waiting", "Ativo · sem posição"]
    cancelled = []
    session = SimpleNamespace(active=True, state="running", symbol="WIN$N", timeframe="H1", mode="replay",
                              telemetry=lambda: (tuple(state), 1 if state[0] != "waiting" else 0, -2 if state[0] == "loss" else 2),
                              cancel=lambda: cancelled.append(True), report=Path("missing"))
    panel.controls.sessions[robot.key] = session
    panel.render()
    for key, text in (("waiting", "sem posição"), ("loss", "perda"), ("profit", "lucro")):
        state[:] = key, "Em operação · " + text
        if panel.controls._pulse is not None:
            panel.frame.after_cancel(panel.controls._pulse)
        panel.controls._tick()
        assert key in panel.cards[robot.key].cget("style")
        assert text in panel.controls.widgets[robot.key]["status"].cget("text")
    assert panel.controls.widgets[robot.key]["stop"].winfo_manager()
    panel.controls.stop(robot.key)
    assert cancelled == [True]


def test_reopened_report_preserves_finished_duration_and_explains_incomplete_work(ui):
    panel, robot, preset = _tested(ui)
    session = SimpleNamespace(robot=robot, symbol="WIN$N", timeframe="H1", preset=preset, mode="replay",
                              state="cancelled", started=100, finished=112, error="Interrompido pelo usuário",
                              record=None, report=Path("missing"))
    for _ in range(2):
        robot_controls.ReportDialog(ui, session)
        window = next(w for w in ui.root.winfo_children() if w.winfo_class() == "Toplevel" and w.title().startswith("Relatório"))
        text = "\n".join(_labels(window))
        assert "Tempo total da sessão: 12,0 s" in text
        assert "Não há métricas finais" in text
        window.destroy()
