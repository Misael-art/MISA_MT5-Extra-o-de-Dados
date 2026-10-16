"""T18.8: apresentação passiva das relações e aderência no Laboratório."""
import gc

import numpy as np
import pytest

tk = pytest.importorskip("tkinter")
pytest.importorskip("matplotlib")


@pytest.fixture
def relations_panel():
    try:
        root = tk.Tk()
    except tk.TclError as exc:
        pytest.skip(f"sem display gráfico: {exc}")
    from mt5_extracao.ui.relations import RelationsPanel
    from mt5_extracao.ui.theme import Theme

    theme = Theme(root)
    panel = RelationsPanel(root, theme)
    panel.frame.pack(fill=tk.BOTH, expand=True)
    root.geometry("1280x800")
    root.update()
    yield panel, root
    root.update_idletasks()
    root.destroy()
    gc.collect()


def test_relation_views_keep_fixture_scope_explicit(relations_panel):
    panel, _root = relations_panel
    assert [panel.notebook.tab(index, "text") for index in range(panel.notebook.index("end"))] == [
        "Matriz", "Linha do tempo", "Ganhos e perdas", "Convergências", "Candidatos"
    ]
    assert "DADOS SINTÉTICOS" in panel.synthetic_notice.message.cget("text")
    assert "não são resultados analíticos reais" in panel.synthetic_notice.message.cget("text")
    assert panel.advanced.expanded is False
    from mt5_extracao.ui.relations import _SYNTHETIC_FIXTURE
    assert "ranking congelado" in _SYNTHETIC_FIXTURE["timeline"]["reference"].casefold()


def test_signed_matrix_is_distinct_from_utility_and_keyboard_selects_detail(relations_panel):
    panel, root = relations_panel
    grid = panel.indicator_grid
    positive = grid.cells[0][1]
    inverse = grid.cells[1][2]
    assert positive["label"].startswith("+") and positive["utility_state"] == "INCONCLUSIVE"
    assert inverse["label"].startswith("−") and inverse["utility_state"] == "INCONCLUSIVE"
    grid._key(type("Event", (), {"keysym": "Right"})())
    root.update_idletasks()
    detail = panel._details["matrix-indicators"].get("1.0", "end-1c")
    assert "RSI(14) × MACD(12,26,9)" in panel.matrix_details["indicators"].title_label.cget("text")
    assert "Associação direta" in detail
    assert "utilidade INCONCLUSIVE" in detail
    assert panel.indicator_grid.cells[2][3]["label"] == "—"
    assert "variável constante" in panel.indicator_grid.cells[2][3]["effect_ci"]


def test_timeline_preserves_gaps_and_exposes_chronological_windows(relations_panel):
    panel, _root = relations_panel
    axis = panel.timeline_chart.figure.axes[0]
    from matplotlib.colors import to_rgba
    neutral = to_rgba(panel.theme.colors["axis"])
    assert axis.patches
    assert all(np.allclose(patch.get_facecolor()[:3], neutral[:3]) for patch in axis.patches)
    from matplotlib import dates as mdates
    from mt5_extracao.ui.relations import _SYNTHETIC_FIXTURE, _date
    windows = _SYNTHETIC_FIXTURE["timeline"]["series"][0]["windows"]
    np.testing.assert_equal(axis.lines[0].get_ydata(), [np.nan, 0.26, 0.03, -0.04, 0.19])
    np.testing.assert_equal(axis.lines[0].get_xdata(),
                            mdates.date2num([_date(window["center"]) for window in windows]))
    values = [panel.timeline_table.tree.item(iid, "values") for iid in panel._timeline_iids]
    assert [row[1] for row in values] == ["Aquecimento", "Treino", "Validação", "OOS", "OOS"]
    assert values[-1][-2] == "STALE"
    assert values[-1][-1] == "UNKNOWN"
    assert panel.timeline_table.tree.item("timeline-2", "tags") == ("DETERIORATING",)
    chart_panel = panel.timeline_chart.widget.master
    assert "Lacunas não são conectadas" in panel.timeline_method_note.cget("text")


def test_short_window_scrolls_instead_of_compressing_chart_or_table(relations_panel):
    panel, root = relations_panel
    root.geometry("1280x480")
    panel.notebook.select(panel.pages["Linha do tempo"])
    root.update()
    viewport = panel._page_scrolls["Linha do tempo"]
    assert viewport.scrollbar.winfo_manager() == "grid"
    assert viewport.content.winfo_reqheight() > viewport.canvas.winfo_height()
    assert panel.timeline_chart.widget.winfo_height() >= 100
    assert panel.timeline_table.winfo_height() >= 150


def test_distribution_denominators_and_incomplete_costs_are_visible(relations_panel):
    panel, _root = relations_panel
    rows = [panel.distribution_table.tree.item(iid, "values")
            for iid in panel.distribution_table.tree.get_children()]
    assert rows == [("Compra", "38", "28", "4", "2", "72", "70", "UNKNOWN", "CURRENT"),
                    ("Venda", "34", "31", "3", "4", "72", "68", "UNKNOWN", "CURRENT")]
    assert "População:" in panel.distribution_population_label.cget("text")
    assert "expectativa líquida não exibida" in panel.distribution_notice.message.cget("text").casefold()


def test_convergence_and_candidates_are_read_only_examples(relations_panel):
    panel, _root = relations_panel
    convergence_rows = [panel.convergence_table.tree.item(iid, "values")
                        for iid in panel.convergence_table.tree.get_children()]
    assert [row[0] for row in convergence_rows] == [
        "Baseline Expert", "ADX(14) isolado", "ATR(14) isolado", "ADX(14) + ATR(14)"
    ]
    panel.convergence_var.set("RSI(14) + CCI(20)")
    panel._draw_convergence()
    assert panel.convergence_table.tree.item("convergence-3", "tags") == ("INCONCLUSIVE",)
    assert "OOS" in panel.convergence_table.tree.item("convergence-0", "values")[2] or \
        "IC 95%" in panel.convergence_table.tree.item("convergence-0", "values")[2]

    candidates = [panel.candidate_table.tree.item(iid, "values")
                  for iid in panel.candidate_table.tree.get_children()]
    assert len(candidates) == 2
    assert all("EXEMPLO" in row[0] for row in candidates)
    assert "não há ação de aplicar" in panel.candidate_notice.message.cget("text").casefold()
    assert not hasattr(panel, "apply_candidate")


def _luminance(color):
    channels = [int(color[index:index + 2], 16) / 255 for index in (1, 3, 5)]
    linear = [value / 12.92 if value <= 0.04045 else ((value + 0.055) / 1.055) ** 2.4
              for value in channels]
    return sum(weight * value for weight, value in zip((0.2126, 0.7152, 0.0722), linear))


def _contrast(first, second):
    lighter, darker = sorted((_luminance(first), _luminance(second)), reverse=True)
    return (lighter + 0.05) / (darker + 0.05)


def test_theme_refresh_keeps_matrix_text_contrast_and_plot(relations_panel):
    panel, _root = relations_panel
    theme = panel.theme
    fills = []
    for _ in range(2):
        theme.toggle()
        panel.render()
        c = theme.colors
        for row, col in ((0, 1), (1, 2), (2, 3), (3, 0)):
            rectangle = next(item for item in panel.indicator_grid.canvas.find_withtag(f"cell:{row}:{col}")
                             if panel.indicator_grid.canvas.type(item) == "rectangle")
            fill = panel.indicator_grid.canvas.itemcget(rectangle, "fill")
            fills.append(fill)
            assert _contrast(c["ink"], fill) >= 4.5
            assert _contrast(c["ink2"], fill) >= 4.5
    assert len(set(fills[:4])) > 1 and len(set(fills[4:])) > 1
    assert panel.timeline_chart.figure.axes[0].get_ylim() == (-1.0, 1.0)


def test_semantic_colors_require_explicit_producer_evidence(relations_panel):
    from mt5_extracao.ui.relations import _SYNTHETIC_FIXTURE, _visual_state

    adverse = _SYNTHETIC_FIXTURE["convergence"]["pairs"][1]["rows"][3]
    assert _visual_state(adverse) == "INCONCLUSIVE"
    assert _visual_state({**adverse, "evidence_state": "VALIDATED", "support_state": "ADEQUATE",
                          "freshness_state": "CURRENT", "criterion_state": "PREDECLARED"}) == "ADVERSE"
    favorable = {"utility_state": "FAVORABLE", "evidence_state": "VALIDATED", "segment": "OOS",
                 "validation_scope": "FINAL_RESERVE", "support_state": "ADEQUATE", "freshness_state": "CURRENT"}
    assert _visual_state(favorable) == "FAVORABLE"
    assert _visual_state({**favorable, "freshness_state": "STALE"}) == "INCONCLUSIVE"
    deterioration = _SYNTHETIC_FIXTURE["timeline"]["series"][0]["windows"][2]
    assert _visual_state(deterioration) == "DETERIORATING"


def test_scope_is_not_a_fake_filter_and_detail_stacks_on_narrow_windows(relations_panel):
    panel, root = relations_panel
    assert not hasattr(panel, "filter_vars")
    panel.timeline_detail.set_expanded(True)
    panel.state_legend.set_expanded(True)
    panel.notebook.select(panel.pages["Linha do tempo"])
    root.update()
    root.geometry("820x800")
    root.update()
    assert panel.timeline_split.winfo_width() < 900
    assert panel.timeline_split._relations_orientation == tk.VERTICAL
    assert panel.timeline_detail.master.grid_info()["row"] == 1
    assert panel.scope_filter_bar._relations_columns == 2
    assert panel.timeline_ranking_note.grid_info()["row"] == 1
    assert [label.grid_info()["column"] for label, _state, _text in panel._legend_labels] == [0, 0, 0, 0]
    assert all(label.cget("wraplength") <= panel._legend_labels_frame.winfo_width()
               for label, _state, _text in panel._legend_labels)
    panel.notebook.select(panel.pages["Convergências"])
    root.update()
    assert panel.convergence_comparison_note.grid_info()["row"] == 1
    assert "expectativa líquida" in panel.convergence_notice.message.cget("text")
    assert panel.convergence_notice.message.cget("wraplength") < 760
    panel.notebook.select(panel.pages["Linha do tempo"])
    root.update()
    root.geometry("1280x800")
    root.update()
    if panel.timeline_split._relations_orientation == tk.HORIZONTAL:
        assert panel.timeline_detail.winfo_width() >= panel.timeline_split._relations_detail_minwidth
        assert panel.timeline_detail.master.grid_info()["row"] == 0
    else:
        assert panel.timeline_detail.master.grid_info()["row"] == 1
    assert panel.timeline_ranking_note.grid_info()["row"] == 0


def test_timeline_redraw_replaces_pick_callback_instead_of_accumulating(relations_panel):
    panel, _root = relations_panel
    callbacks = panel.timeline_chart.canvas.callbacks.callbacks["button_press_event"]
    before = len(callbacks)
    panel._draw_timeline()
    panel._draw_timeline()
    callbacks = panel.timeline_chart.canvas.callbacks.callbacks["button_press_event"]
    assert len(callbacks) == before


def test_timeline_keeps_exact_points_and_internal_gap_in_both_themes(relations_panel, monkeypatch):
    from copy import deepcopy
    from matplotlib.path import Path
    from mt5_extracao.ui.relations import _SYNTHETIC_FIXTURE

    panel, root = relations_panel
    series = deepcopy(_SYNTHETIC_FIXTURE["timeline"]["series"][0])
    series["windows"][2].update(value=None, value_text="—", n="n=0")
    monkeypatch.setattr(panel, "_current_timeline_series", lambda: series)
    for _ in range(2):
        panel.theme.toggle()
        panel._draw_timeline()
        root.update_idletasks()
        axis = panel.timeline_chart.figure.axes[0]
        line = axis.lines[0]
        np.testing.assert_equal(line.get_ydata(), [np.nan, 0.26, np.nan, -0.04, 0.19])
        assert len(line.get_xdata()) == len(series["windows"])
        # NaN inicia dois trechos; nenhuma linha atravessa a janela ausente.
        codes = [code for _vertices, code in line.get_path().iter_segments()]
        assert codes.count(Path.MOVETO) == 2
        assert len(axis.collections) == 3
        assert panel.timeline_table.tree.item("timeline-2", "values")[2] == "—"
        note = panel.timeline_method_note.cget("text")
        assert "sem suavização" in note and "Lacunas não são conectadas" in note
        panel.timeline_table.tree.selection_set("timeline-3")
        panel._timeline_selected(None)
        assert "OOS" in panel._details["timeline"].get("1.0", "end-1c")


def test_split_views_keep_readable_details_through_resize_and_themes(relations_panel):
    panel, root = relations_panel
    panel.timeline_detail.set_expanded(True)
    for _ in range(2):
        for width in (1920, 1280, 820, 1920):
            root.geometry(f"{width}x1080")
            panel.notebook.select(panel.pages["Linha do tempo"])
            root.update()
            split = panel.timeline_split
            detail = panel.timeline_detail
            assert detail.winfo_width() >= split._relations_detail_minwidth
            assert detail.winfo_rootx() >= split.winfo_rootx()
            assert detail.winfo_rootx() + detail.winfo_width() <= split.winfo_rootx() + split.winfo_width()
            if split._relations_orientation == tk.HORIZONTAL:
                assert detail.master.grid_info()["row"] == 0
            else:
                assert detail.master.grid_info()["row"] == 1
            panel.timeline_table.tree.selection_set("timeline-3")
            panel._timeline_selected(None)
            assert "OOS" in panel._details["timeline"].get("1.0", "end-1c")
            assert panel.timeline_chart.widget.winfo_height() >= 100
        panel.theme.toggle()


def test_detail_spacing_is_owned_by_split_container(relations_panel):
    panel, root = relations_panel
    for name in ("timeline", "distribution", "convergence"):
        detail = getattr(panel, name + "_detail")
        detail.set_expanded(True)
        root.update()
        assert str(detail.pack_info().get("padx", "0")) == "0"
        split = detail.master.master
        gap = detail.master.grid_info()["padx"]
        if split._relations_orientation == tk.HORIZONTAL:
            assert gap != 0
        else:
            assert gap == 0


def test_secondary_information_starts_closed_without_hiding_primary_warnings(relations_panel):
    panel, root = relations_panel
    assert len(panel._detail_panels) == 6
    for detail in [*panel._detail_panels.values(), panel.state_legend, panel.timeline_explanation]:
        assert not detail.expanded
        assert detail.body.winfo_manager() == ""
        assert "ℹ" in detail.toggle_button.cget("text")
    assert panel.synthetic_notice.winfo_ismapped()
    assert "DADOS SINTÉTICOS" in panel.synthetic_notice.message.cget("text")
    assert panel.distribution_notice.winfo_manager()
    assert "expectativa líquida" in panel.distribution_notice.message.cget("text")
    assert "evidência ≠ autorização" in panel.state_legend.title_label.cget("text")
    panel.state_legend.toggle_button.invoke()
    root.update()
    assert all(label.winfo_ismapped() for label, _state, _text in panel._legend_labels)
    panel.timeline_explanation.toggle_button.invoke()
    assert "Lacunas não são conectadas" in panel.timeline_method_note.cget("text")


def test_closed_selection_details_update_and_release_space_without_changing_plot(relations_panel):
    panel, root = relations_panel
    panel.notebook.select(panel.pages["Linha do tempo"])
    for _ in range(2):
        for width in (1920, 1280, 820):
            root.geometry(f"{width}x900")
            detail = panel.timeline_detail
            detail.set_expanded(False)
            root.update()
            viewport = panel._page_scrolls["Linha do tempo"]
            closed_width = panel.timeline_chart.widget.winfo_width()
            closed_height = viewport.content.winfo_reqheight()
            observed = panel.timeline_chart.figure.axes[0].lines[0].get_ydata().copy()
            table = [panel.timeline_table.tree.item(iid, "values") for iid in panel._timeline_iids]
            for iid, segment in (("timeline-0", "Aquecimento"), ("timeline-3", "OOS")):
                panel.timeline_table.tree.selection_set(iid)
                panel._timeline_selected(None)
                assert not detail.expanded
                assert segment in panel._details["timeline"].get("1.0", "end-1c")
            selected_text = panel._details["timeline"].get("1.0", "end-1c")
            detail.toggle_button.invoke()
            root.update()
            assert detail.expanded
            assert (closed_width > panel.timeline_chart.widget.winfo_width() or
                    closed_height < viewport.content.winfo_reqheight())
            assert panel._details["timeline"].get("1.0", "end-1c") == selected_text
            np.testing.assert_equal(panel.timeline_chart.figure.axes[0].lines[0].get_ydata(), observed)
            assert [panel.timeline_table.tree.item(iid, "values") for iid in panel._timeline_iids] == table
            text = panel._details["timeline"]
            text.focus_force()
            root.update()
            text.event_generate("<Escape>")
            root.update()
            assert not detail.expanded
            assert root.focus_get() is detail.toggle_button
            assert text.get("1.0", "end-1c") == selected_text
        panel.theme.toggle()
