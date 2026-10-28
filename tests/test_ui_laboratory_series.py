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
    if panel.ui.root.winfo_screenwidth() < 1280 or panel.ui.root.winfo_screenheight() < 800:
        pytest.skip("Tela física menor que o monitor simulado; o WM limita a janela (runner Windows).")
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


def test_stored_m7_draws_as_synthetic_and_stays_out_of_native_capture(panel):
    from types import SimpleNamespace
    import pandas as pd
    from mt5_extracao.analytics import resampling
    from mt5_extracao.laboratory import service
    from tests.test_analytics_calendar import m1_bars

    panel.db.save_ohlcv_data("EURUSD", "1 minuto", m1_bars(pd.date_range("2026-09-14 00:00", periods=7, freq="min")))
    resampling.resample_symbol(panel.db, "EURUSD", ["M7"], symbol_info=SimpleNamespace(name="EURUSD"))
    series = open_series(panel)

    def listed():
        return series.tf_list.size() > 0 and any(
            "sintético" in series.tf_list.get(i) for i in range(series.tf_list.size()))

    panel.pump(listed, 5)
    labels = [series.tf_list.get(i) for i in range(series.tf_list.size())]
    assert "M7 · sintético" in labels
    assert "M1" in labels
    series.tf_list.selection_clear(0, "end")
    synthetic = labels.index("M7 · sintético")
    native = labels.index("M1")
    series.tf_list.selection_set(native)
    series.tf_list.selection_set(synthetic)
    series.analyze()
    assert "não entram na captura nativa" in series.status.cget("text")
    series.tf_list.selection_clear(0, "end")
    series.tf_list.selection_set(synthetic)
    series.analyze()
    panel.pump(lambda: series.derived_read is not None and not series.pending, 5)
    assert series.report is None
    title = series.panels["price"].chart.figure.axes[0].get_title(loc="left")
    assert "sintético" in title and "não é tempo do MT5" in title
    assert len(series.panels["price"].chart.figure.axes[0].collections) == 2
    relations = series.panels["relations"].chart.figure.axes[0].texts[0].get_text()
    assert "não entram neste recorte" in relations
    with pytest.raises(ValueError, match="timeframes distintos"):
        service.capture_selection(panel.db, symbol="EURUSD", timeframes_selected=["M7"])
    series.export()
    assert "não se exporta como estudo" in series.status.cget("text")


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
    width, height = (int(part) for part in geometry.split("x"))
    if (panel.ui.root.winfo_width(), panel.ui.root.winfo_height()) != (width, height):
        pytest.skip("O gerenciador de janelas não aplicou a geometria pedida (tela física menor/ocupada).")
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


def test_empty_channels_show_reason_instead_of_gray_area(panel, snapshots):
    # SINTÉTICO: força canais sem valores; a UI deve exibir motivo/ação, não só área cinza.
    series = open_series(panel)
    report = analysis(snapshots)
    for channel in report["channels"]:
        channel["values"] = [None] * len(report["times"])
        channel["directions"] = [None] * len(report["times"])
        channel["diagnosis"] = dict(code="WARMUP_EXCEEDS_WINDOW", message="M15: 3 barras.", action="Antecipe o início.")
    for pair in report["pairs"]:
        pair["correlation"] = None
    series.set_report(report)
    assert "Antecipe o início" in series.status.cget("text")
    texts = [text.get_text() for key in ("relations", "timeline") for ax in
             series.panels[key].chart.figure.axes for text in ax.texts]
    texts += [text.get_text() for key in ("relations", "timeline") for text in series.panels[key].chart.figure.texts]
    assert sum("Antecipe o início" in text for text in texts) == 2


def test_single_bar_segment_stays_visible(panel, snapshots):
    series = open_series(panel)
    report = analysis(snapshots)
    channel = report["channels"][0]
    channel["values"] = [None] * len(report["times"])
    channel["values"][5] = 42.0
    series.set_report(report)
    feature = series.panels["price"].chart.figure.axes[1]
    assert any(line.get_marker() == "o" for line in feature.lines)


def test_compare_list_is_empty_until_the_user_adds_another_strategy(panel, monkeypatch):
    import json
    from mt5_extracao.strategies import get_strategy
    from mt5_extracao.ui.laboratory_series import comparison_arguments
    series = open_series(panel)
    assert series.compare_list.curselection() == ()
    assert comparison_arguments("ema_atr", {"fast": 12}, [])["strategy_key"] == "ema_atr"
    series._inventory = [dict(symbol="X", timeframe="M5")]
    series.symbol.set("X")
    series._symbol_changed()
    series.compare_list.selection_set(series._compare_keys.index("donchian"))
    seen = {}
    monkeypatch.setattr(panel.ui, "run_bg", lambda work, done: seen.setdefault("work", work))
    monkeypatch.setattr("mt5_extracao.laboratory.service.capture_selection", lambda *args, **kwargs: [])
    monkeypatch.setattr("mt5_extracao.laboratory.service.analyze",
                        lambda snapshots, **kwargs: seen.update(kwargs) or {"analysis_id": "x", "channels": []})
    series.analyze()
    seen["work"]()
    assert [item["key"] if isinstance(item, dict) else item for item in seen["strategies"]] == ["ema_atr", "donchian"]
    assert seen["strategies"][0]["params"]["fast"] == json.loads(series.params.get())["fast"]
    assert seen["strategies"][1] == "donchian"
    series.strategy.set("Sem estratégia")
    series._strategy_changed()
    series.compare_list.selection_clear(0, "end")
    series.compare_list.selection_set(series._compare_keys.index("donchian"))
    assert comparison_arguments(None, {}, ["donchian"]) == dict(strategies=["donchian"])
    assert get_strategy("donchian").params  # o serviço aplica o padrão canônico desta chave


def test_two_strategies_share_the_price_clock_as_intentions(panel, snapshots):
    from matplotlib.collections import PathCollection
    from mt5_extracao.ui.laboratory_series import intention_channels
    series = open_series(panel)
    report = analysis(snapshots, strategies=["ema_atr", "donchian"])
    series.set_report(report)
    marks = intention_channels(report, series.detail_channel.get(), series.study.removed, series.study.hidden)
    assert [row["id"] for row in marks] == ["M5:strategy:ema_atr", "M5:strategy:donchian"]
    ax = series.panels["price"].chart.figure.axes[0]
    title = ax.get_title(loc="left")
    assert "intenção" in title and "execução" in title
    expected = 0
    for channel in marks:
        seen = set()
        for value, end in zip(channel["values"], channel["source_ends"]):
            if value in (1, -1) and end not in seen:
                expected += 1
                seen.add(end)
    got = sum(len(item.get_offsets()) for item in ax.collections if isinstance(item, PathCollection))
    assert got == expected and expected > 0
    legend = [text.get_text() for text in ax.get_legend().get_texts()]
    assert sum(text.startswith("intenção ·") for text in legend) == 2
    assert "operação ausente" in series.status.cget("text")
    assert "execução" in series.notice.message.cget("text")
    labels = [tick.get_text() for tick in series.panels["timeline"].chart.figure.axes[0].get_yticklabels()]
    assert len(labels) == 4 and all(label.endswith("· intenção") for label in labels)
    axes = series.panels["price"].chart.figure.axes
    assert axes[0].get_xlim() == axes[-1].get_xlim()


def test_feature_search_filters_inventory_and_keeps_selection(panel):
    series = open_series(panel)
    before = set(series._chosen_features)
    series.feature_query.set("efficiency")
    assert all("efficiency" in ident for ident in series._visible_features)
    series.feature_query.set("")
    assert series._chosen_features == before
    assert len(series._visible_features) == len(series.catalog["indicators"])


def test_asset_chart_groups_units_toggles_legend_and_annotates(panel, snapshots, tmp_path):
    from types import SimpleNamespace
    series = open_series(panel)
    report = analysis(snapshots)
    channel = copy.deepcopy(report["channels"][0])
    channel.update(id="M5:trend.fake_line", rule="price", unit="price_units", name="Linha de preço")
    report["channels"].append(channel)
    series.set_report(report)
    chart = series.panels["price"].chart
    axes = chart.figure.axes
    assert len(axes) == 1 + len({row["unit"] for row in report["channels"] if row["rule"] != "price"})
    assert any(line.get_label().endswith("Linha de preço") for line in axes[0].lines)
    # Legenda alterna visibilidade sem perder o canal.
    label = next(line.get_label() for line in axes[0].lines if line.get_label().endswith("Linha de preço"))
    series._legend_picked(chart, SimpleNamespace(artist=SimpleNamespace(get_label=lambda: label)))
    assert series.study.hidden == {"M5:trend.fake_line"}
    assert not any(line.get_label() == label for line in chart.figure.axes[0].lines)
    ax = chart._price_axis
    x0, x1 = ax.get_xlim()
    y = sum(ax.get_ylim()) / 2
    series.tool.set("Linha horizontal")
    series._chart_clicked(chart, SimpleNamespace(inaxes=ax, xdata=x0 + 1e-3, ydata=y))
    series.tool.set("Região")
    for x in (x0 + (x1 - x0) * .2, x0 + (x1 - x0) * .4):
        series._chart_clicked(chart, SimpleNamespace(inaxes=chart._price_axis, xdata=x, ydata=y))
    series.tool.set("Nota")
    series._chart_clicked(chart, SimpleNamespace(inaxes=chart._price_axis, xdata=x0, ydata=y))
    assert "Anotação recusada" in series.status.cget("text")  # nota sem texto
    series.note_text.set("teste")
    series._chart_clicked(chart, SimpleNamespace(inaxes=chart._price_axis, xdata=x0, ydata=y))
    assert [item["kind"] for item in series.study.items] == ["hline", "region", "note"]
    series.remove_selected_annotation()
    series.undo_annotation()
    path = tmp_path / "estudo.json"
    series.export_study(str(path))
    series.study.remove(series.study.items[0]["id"])
    series.open_study(str(path))
    assert [item["kind"] for item in series.study.items] == ["hline", "region", "note"]
    assert series.study.hidden == {"M5:trend.fake_line"}


def test_asset_bench_moves_geometry_removes_series_scrolls_units_and_keeps_view(panel, snapshots, tmp_path):
    from types import SimpleNamespace
    series = open_series(panel)
    report = analysis(snapshots)
    base = report["channels"][0]
    for index in range(5):  # 6 unidades além do preço: acima do antigo teto de 3
        channel = copy.deepcopy(base)
        channel.update(id=f"M5:fake.unit_{index}", unit=f"unit_{index}", name=f"Unidade {index}")
        report["channels"].append(channel)
    series.set_report(report)
    chart = series.panels["price"].chart
    non_price = {row["unit"] for row in report["channels"] if row["rule"] not in ("price", "candidate")}
    assert len(chart.figure.axes) == 1 + len(non_price) and len(non_price) > 3
    frozen, analysis_id = series.report, series.study.analysis_id
    # Remover série da vista: sem recongelar, e desfazer devolve.
    series.detail_channel.set("M5:fake.unit_0")
    series.remove_detail_series()
    assert series.report is frozen and series.study.analysis_id == analysis_id
    assert len(chart.figure.axes) == len(non_price)
    series.undo_annotation()
    assert len(chart.figure.axes) == 1 + len(non_price)
    # Criar, selecionar e arrastar uma linha horizontal.
    ax = chart._price_axis
    (x0, x1), (y0, y1) = ax.get_xlim(), ax.get_ylim()
    middle = (y0 + y1) / 2
    series.tool.set("Linha horizontal")
    series._chart_clicked(chart, SimpleNamespace(inaxes=ax, xdata=x0, ydata=middle))
    series.tool.set("Selecionar/mover")
    ax = chart._price_axis
    series._chart_clicked(chart, SimpleNamespace(inaxes=ax, xdata=(x0 + x1) / 2, ydata=middle))
    assert series.study.selected == series.study.items[0]["id"]
    delta = (y1 - y0) * .1
    series._chart_released(chart, SimpleNamespace(inaxes=chart._price_axis, xdata=(x0 + x1) / 2, ydata=middle + delta))
    assert series.study.items[0]["y"] == pytest.approx(middle + delta)
    series.undo_annotation()
    assert series.study.items[0]["y"] == pytest.approx(middle)
    # Borda da região redimensiona só aquele lado.
    series.tool.set("Região")
    for frac in (.6, .8):
        series._chart_clicked(chart, SimpleNamespace(inaxes=chart._price_axis, xdata=x0 + (x1 - x0) * frac, ydata=middle))
    region = series.study.items[-1]
    series.tool.set("Selecionar/mover")
    series._chart_clicked(chart, SimpleNamespace(inaxes=chart._price_axis, xdata=region["x1"], ydata=y0 + (y1 - y0) * .9))
    series._chart_released(chart, SimpleNamespace(inaxes=chart._price_axis, xdata=x0 + (x1 - x0) * .9, ydata=middle))
    assert series.study.items[-1]["x0"] == region["x0"]
    assert series.study.items[-1]["x1"] == pytest.approx(x0 + (x1 - x0) * .9)
    series.undo_annotation()
    series.undo_annotation()
    assert series.study.selected is None and len(series.study.items) == 1
    series.study.select(series.study.items[0]["id"])
    # Zoom e cursor sobrevivem a ampliar/restaurar e a redesenhos.
    zoom = (x0 + (x1 - x0) * .25, x0 + (x1 - x0) * .5)
    chart._price_axis.set_xlim(*zoom)
    series._cursor_moved(chart, SimpleNamespace(inaxes=chart._price_axis, xdata=zoom[0], ydata=middle))
    series.panels["price"].expand()
    detail = series.panels["price"].detail
    assert detail._price_axis.get_xlim() == pytest.approx(zoom)
    assert detail._cursors[0].get_visible() and detail._cursors[0].get_xdata()[0] == pytest.approx(zoom[0])
    assert all(axis.get_xlim() == pytest.approx(zoom) for axis in detail.figure.axes)
    series.panels["price"].restore()
    series.panels["price"].redraw()
    assert chart._price_axis.get_xlim() == pytest.approx(zoom) and series.study.selected is not None
    # Exportar/reabrir preserva vista, identidade e séries; estudo incompatível é recusado.
    series.study.remove_series("M5:fake.unit_1")
    path = tmp_path / "bancada.json"
    series.export_study(str(path))
    series.study = None
    series.set_report(report)
    series.open_study(str(path))
    assert series.study.removed == {"M5:fake.unit_1"} and series.study.view["xlim"] == pytest.approx(list(zoom))
    assert series.study.identity["series"][0]["parameters"] == base["parameters"]
    other = copy.deepcopy(report)
    other["channels"] = [row for row in other["channels"] if row["id"] != "M5:fake.unit_1"]
    series.set_report(other)
    series.open_study(str(path))
    assert "séries ausentes" in series.status.cget("text")
