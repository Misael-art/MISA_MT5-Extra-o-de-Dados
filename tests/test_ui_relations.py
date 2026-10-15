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
    observed = axis.lines[0].get_ydata()
    assert np.isnan(observed[0])
    assert len(observed) > 5
    assert np.isclose(observed[1], 0.26)
    assert np.isclose(observed[-1], 0.19)
    from mt5_extracao.ui.relations import _smooth_observed_segments
    _, separated = _smooth_observed_segments((0, 1, 2, 3, 4, 5), (0.1, 0.2, 0.3, np.nan, -0.2, -0.1))
    assert np.isnan(separated).sum() == 1
    assert np.isclose(separated[0], 0.1) and np.isclose(separated[-1], -0.1)
    values = [panel.timeline_table.tree.item(iid, "values") for iid in panel._timeline_iids]
    assert [row[1] for row in values] == ["Aquecimento", "Treino", "Validação", "OOS", "OOS"]
    assert values[-1][-2] == "STALE"
    assert values[-1][-1] == "UNKNOWN"
    assert panel.timeline_table.tree.item("timeline-2", "tags") == ("DETERIORATING",)
    chart_panel = panel.timeline_chart.widget.master
    assert "Lacunas não são conectadas" in chart_panel.winfo_children()[-1].cget("text")


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
    assert panel.timeline_split._relations_orientation == tk.HORIZONTAL
    assert panel.timeline_detail.master.grid_info()["row"] == 0
    assert panel.timeline_ranking_note.grid_info()["row"] == 0


def test_timeline_redraw_replaces_pick_callback_instead_of_accumulating(relations_panel):
    panel, _root = relations_panel
    callbacks = panel.timeline_chart.canvas.callbacks.callbacks["button_press_event"]
    before = len(callbacks)
    panel._draw_timeline()
    panel._draw_timeline()
    callbacks = panel.timeline_chart.canvas.callbacks.callbacks["button_press_event"]
    assert len(callbacks) == before
