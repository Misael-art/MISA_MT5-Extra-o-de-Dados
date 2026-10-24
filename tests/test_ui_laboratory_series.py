"""Percurso Tk real: vazio, fontes, resposta obsoleta, ampliar/restaurar e tema."""
import copy
import time

import pytest

from tests.test_ui_laboratory import panel
from tests.test_laboratory_series import source_db, snapshots, analysis


def open_series(panel):
    from mt5_extracao.ui.laboratory import SERIES_TAB
    panel.tabs.select(panel.tab_frames[SERIES_TAB])
    panel.render_visible()
    panel.ui.root.geometry("1280x800")
    panel.pump(lambda: panel.series_panel is not None, 5)
    return panel.series_panel


def test_real_series_area_is_separate_from_demo_and_ledger_filters(panel):
    series = open_series(panel)
    panel.pump(lambda: "Base sem séries compatíveis" in series.status.cget("text"))
    assert series.report is None
    assert not panel.filter_bar.winfo_manager()
    assert not panel.kpi_row.winfo_manager()
    assert "fechamento nominal" in series.notice.message.cget("text")
    assert len(series.panels) == 5
    panel.tabs.select(panel.tab_frames["Visão geral"])
    panel.render_visible()
    assert panel.filter_bar.winfo_manager()


def test_all_views_render_frozen_report_and_small_screen_can_scroll_wide(panel, snapshots):
    series = open_series(panel)
    report = analysis(snapshots, strategy_key="ema_atr")
    frozen = copy.deepcopy(report)
    series.set_report(report)
    panel.ui.root.update()
    assert all(item.chart.figure.axes for item in series.panels.values())
    series.extended.set(True)
    series._layout()
    panel.ui.root.update()
    assert series._columns == 2
    assert series.canvas.xview()[1] < 1
    series.extended.set(False)
    series._layout()
    panel.ui.root.update()
    assert series._columns == 1
    assert series.report == frozen
    assert series.panels["relations"].chart.figure.axes[0].images[0].norm.vmin == -1
    assert series.panels["relations"].chart.figure.axes[0].images[0].norm.vmax == 1


def test_chart_maximize_restore_uses_same_report_and_current_monitor(panel, snapshots, monkeypatch):
    from mt5_extracao.ui import laboratory_series
    series = open_series(panel)
    series.set_report(analysis(snapshots))
    identity = series.report["analysis_id"]
    monkeypatch.setattr(laboratory_series, "monitor_geometry", lambda _: (0, 0, 1280, 800))
    chart = series.panels["price"]
    chart.expand()
    panel.ui.root.update()
    assert chart.popup.winfo_width() == 1280
    assert chart.popup.winfo_height() == 750
    chart.restore()
    assert chart.popup is None and series.report["analysis_id"] == identity
    assert chart.expand_button.cget("text").startswith("Ampliar")


def test_selection_changed_during_compute_cannot_replace_frozen_result(panel, snapshots, monkeypatch):
    from mt5_extracao.laboratory import service
    series = open_series(panel)
    series._inventory = [dict(symbol="X", timeframe="M5")]
    series.symbol.set("X")
    series._symbol_changed()
    old = analysis(snapshots)
    series.set_report(old)
    queued = []
    monkeypatch.setattr(panel.ui, "run_bg", lambda work, done: queued.append((work, done)))
    series.analyze()
    assert series.pending
    series.params.set('{"fast": 21}')
    queued[-1][1](dict(old, analysis_id="stale"), None)
    assert not series.pending and series.report is old
    assert "não publicado" in series.status.cget("text")


def test_presets_use_existing_generator_and_frozen_context_without_promoting_correlation(panel, snapshots):
    from types import SimpleNamespace
    import tkinter as tk
    series = open_series(panel)
    series.set_report(analysis(snapshots))
    robot = SimpleNamespace(key="mql5:hdk_s09_referencia")
    calls = []
    controls = SimpleNamespace(chosen={}, open_presets=lambda r: calls.append(r.key) or object())
    panel.ui.robots_panel = SimpleNamespace(robots=[robot], controls=controls)
    window = series.open_presets()
    assert window is not None
    buttons = [child for child in window.winfo_children()[0].winfo_children() if child.winfo_class() == "TButton"]
    buttons[0].invoke()
    assert calls == [robot.key]
    assert controls.chosen[robot.key] == ("X", "M5", None)


def test_no_profile_does_not_fabricate_an_exportable_preset(panel, snapshots):
    series = open_series(panel)
    series.set_report(analysis(snapshots))
    assert series.open_presets() is None
    assert "Nenhum perfil nativo" in series.status.cget("text")


def test_visual_reduction_preserves_rare_events_and_conflicts_not_as_neutral():
    from mt5_extracao.ui.laboratory_series import timeline_bins
    import numpy as np
    values = [0] * 1000
    values[4], values[6], values[104] = 1, -1, 1
    data, boundaries = timeline_bins([dict(directions=values), dict(directions=[None] * 1000)], 10)
    assert boundaries.tolist() == list(range(0, 1001, 100))
    assert data[0, :3].tolist() == [2, 1, 0]
    assert np.isnan(data[1]).all()
    assert values[4] == 1 and values[6] == -1


def test_retained_higher_timeframe_intent_gets_one_marker_not_multiple_entries(panel, snapshots):
    from matplotlib.collections import PathCollection
    series = open_series(panel)
    report = analysis(snapshots)
    channel = report["channels"][0]
    channel["rule"] = "candidate"
    channel["values"] = [1, 1, 1] + [None] * (len(report["times"]) - 3)
    channel["source_ends"] = [report["times"][0]] * 3 + [None] * (len(report["times"]) - 3)
    series.set_report(report)
    ax = series.panels["price"].chart.figure.axes[0]
    assert sum(len(item.get_offsets()) for item in ax.collections if isinstance(item, PathCollection)) == 1


@pytest.mark.parametrize("geometry,expected", [("1280x800", False), ("2100x1250", True)])
def test_large_monitor_does_not_force_wide_layout_into_compact_window(panel, monkeypatch, geometry, expected):
    from mt5_extracao.ui import laboratory_series
    series = open_series(panel)
    monkeypatch.setattr(laboratory_series, "monitor_geometry", lambda _: (0, 0, 2200, 1400))
    panel.ui.root.geometry(geometry)
    panel.ui.root.update()
    series._display_initialized = False
    series.on_show()
    assert series.extended.get() is expected


def test_typed_parameters_accept_decimal_comma_and_reject_invalid_before_capture(panel, monkeypatch):
    import json
    series = open_series(panel)
    series.parameter_values["fast"].set("21")
    series.parameter_values["stop_atr"].set("1,25")
    assert json.loads(series.params.get())["fast"] == 21
    assert json.loads(series.params.get())["stop_atr"] == 1.25
    series.parameter_values["fast"].set("21.5")
    monkeypatch.setattr(panel.ui, "run_bg", lambda *args: pytest.fail("não capturar parâmetros inválidos"))
    series.analyze()
    assert "Média curta" in series.status.cget("text")
    assert "JSON" not in series.status.cget("text")


def test_strategy_changes_rebuild_typed_parameter_fields_from_registry(panel):
    from mt5_extracao.composition.registry import STRATEGY_PARAMETERS
    series = open_series(panel)
    series.strategy.set("stoch_ema9")
    series._strategy_changed()
    assert set(series.parameter_values) == set(STRATEGY_PARAMETERS["stoch_ema9"])
    series.parameter_values["k_period"].set("1")
    assert series._params_error
    series.strategy.set("Sem estratégia")
    series._strategy_changed()
    assert series.parameter_values == {} and series._params_error is None


def test_invalid_strategy_parameter_relationship_refuses_before_snapshot(panel, monkeypatch):
    series = open_series(panel)
    series._inventory = [dict(symbol="X", timeframe="M5")]
    series.symbol.set("X")
    series._symbol_changed()
    series.strategy.set("stoch_ema9")
    series._strategy_changed()
    series.parameter_values["lower_level"].set("80")
    assert series._params_error is None  # campos isoladamente válidos; relação é do contrato real.
    monkeypatch.setattr(panel.ui, "run_bg", lambda *args: pytest.fail("não capturar estratégia inválida"))
    series.analyze()
    assert not series.pending


def test_expanded_chart_uses_absolute_negative_monitor_coordinates(panel, monkeypatch):
    from mt5_extracao.ui import laboratory_series
    series = open_series(panel)
    monkeypatch.setattr(laboratory_series, "monitor_geometry", lambda _: (-1024, -50, 1024, 850))
    chart = series.panels["price"]
    chart.expand()
    panel.ui.root.update()
    assert (chart.popup.winfo_x(), chart.popup.winfo_y()) == (-1024, -50)
    chart.restore()


@pytest.mark.parametrize("windows", [False, True])
def test_monitor_discovery_uses_window_anchor_and_releases_backend(panel, monkeypatch, windows):
    from mt5_extracao.bootstrap import paths
    from mt5_extracao.robots import session_window
    from mt5_extracao.ui.laboratory_series import monitor_geometry
    monkeypatch.setattr(paths, "is_windows", lambda: windows)
    seen = []
    class FakePlacement:
        def __init__(self, anchor):
            seen.append(anchor)
            self.monitor = session_window.Monitor(-2048, 0, 2048, 1200)
        def close(self):
            seen.append("closed")
    monkeypatch.setattr(session_window, "Placement", FakePlacement)
    root = panel.ui.root
    root.geometry("1280x800+0+0")
    root.update()
    assert monitor_geometry(root) == (-2048, 0, 2048, 1200)
    assert seen == [(root.winfo_rootx() + root.winfo_width() // 2,
                     root.winfo_rooty() + root.winfo_height() // 2), "closed"]
