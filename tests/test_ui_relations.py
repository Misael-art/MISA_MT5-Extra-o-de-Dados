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


def _load_timeline_layout(panel, series=None):
    """A fixture só entra no teste de layout. A tela padrão permanece sem números de exemplo."""
    from copy import deepcopy
    from mt5_extracao.ui.relations import _SYNTHETIC_FIXTURE
    series = deepcopy(series or _SYNTHETIC_FIXTURE["timeline"]["series"][0])
    panel._timelines = [series]
    panel.timeline_select.configure(values=[series["label"]])
    panel.timeline_var.set(series["label"])
    panel._draw_timeline()
    return series


def test_relation_views_keep_fixture_scope_explicit(relations_panel):
    panel, _root = relations_panel
    assert [panel.notebook.tab(index, "text") for index in range(panel.notebook.index("end"))] == [
        "Inventário", "Matriz", "Linha do tempo", "Ganhos e perdas", "Convergências", "Candidatos"
    ]
    notice = panel.synthetic_notice.message.cget("text")
    assert "DADOS SINTÉTICOS" not in notice
    assert "nenhum número de exemplo" in notice
    assert "não é lucro" in notice
    assert panel.advanced.expanded is False
    assert not panel.timeline_table.tree.get_children()
    assert not panel.candidate_table.tree.get_children()
    from mt5_extracao.ui.relations import _SYNTHETIC_FIXTURE
    assert "ranking congelado" in _SYNTHETIC_FIXTURE["timeline"]["reference"].casefold()


@pytest.fixture(scope="module")
def lab_report(tmp_path_factory):
    import pandas as pd
    from mt5_extracao import timeframes
    from mt5_extracao.database_manager import DatabaseManager
    from mt5_extracao.laboratory import service

    db = DatabaseManager(db_path=str(tmp_path_factory.mktemp("lab") / "lab.sqlite"), broker_utc_offset=0)
    for tf, freq, count in (("M5", "5min", 960), ("M15", "15min", 320)):
        t = np.arange(count)
        close = 100 + np.sin(t / 5) * 3 + t / 300
        db.save_ohlcv_data("X", timeframes.parse(tf).label, pd.DataFrame(dict(
            time=pd.date_range("2025-01-06", periods=count, freq=freq), open=close - .1, high=close + .5,
            low=close - .5, close=close, tick_volume=10 + t % 7, real_volume=0, spread=1)))
    snapshots = service.capture_selection(db, symbol="X", timeframes_selected=["M15", "M5"])
    report = service.analyze(snapshots, feature_ids=["momentum.rsi_14", "structure.fractals_5_strict_provisional"],
                             as_of="2025-02-01T00:00:00Z")
    db.close()
    return report


def test_indicator_matrix_is_real_lab_report_with_reasons_and_lag(relations_panel, lab_report):
    from mt5_extracao.laboratory import service
    panel, root = relations_panel
    grid = panel.indicator_grid
    assert grid.cells[0][0]["strength"] == "missing" and "Congele uma análise" in grid.cells[0][0]["reason"]
    panel.set_lab_report(lab_report)
    matrix = service.relation_matrix(lab_report)
    assert len(grid.rows) == len(matrix["channels"]) == 4
    for row, cells in zip(grid.cells, matrix["cells"]):
        for shown, cell in zip(row, cells):
            assert (shown["label"] in ("provisório", "mesma série")) == (cell["effect"] is None)
            assert shown["n"] == f"n={cell['n_pairs']}" and shown["utility_state"] == "INCONCLUSIVE"
    assert "Dados reais" in panel.lab_matrix_status.cget("text")
    grid._key(type("Event", (), {"keysym": "Right"})())
    root.update_idletasks()
    detail = panel._details["matrix-indicators"].get("1.0", "end-1c")
    assert "Saída provisória" in detail and "fixture" not in detail.casefold()
    panel.lab_lag.set("3")
    panel._refresh_lab_matrix()
    assert grid.cells[0][0]["label"] == "mesma série"
    assert "candidata preditiva +3" in panel.indicator_grid.cells[0][2]["segment"]
    target = service.target_matrix(lab_report, horizon=3)
    assert panel.target_grid.columns == ("Retorno +3",)
    for shown, cell in zip(panel.target_grid.cells, target["cells"][0]):
        assert shown[0]["n"] == f"n={cell['n_pairs']}"
        assert "fixture" not in shown[0]["reason"].casefold()
    channel_id = next(c["id"] for c in lab_report["channels"] if c.get("directions"))
    outcomes = service.direction_outcomes(lab_report, channel_id, horizon=3)
    rows = [panel.distribution_table.tree.item(iid, "values")
            for iid in panel.distribution_table.tree.get_children()]
    buy = outcomes["directions"][0]
    assert rows[0][1:4] == (str(buy["wins"]), str(buy["losses"]), str(buy["flats"]))
    assert int(rows[0][1]) + int(rows[0][2]) + int(rows[0][3]) == int(rows[0][5]) == buy["n_total"]
    assert "EXEMPLO" not in "".join(panel.candidate_table.tree.item(iid, "values")[0]
                                    for iid in panel.candidate_table.tree.get_children())
    # Mesma análise de novo não recalcula; resposta antiga é descartada.
    request = panel._lab_request
    panel.set_lab_report(lab_report)
    assert panel._lab_request == request


def test_timeline_preserves_gaps_and_exposes_chronological_windows(relations_panel):
    panel, _root = relations_panel
    axis = panel.timeline_chart.figure.axes[0]
    assert any("Congele uma análise" in text.get_text() for text in axis.texts)
    _load_timeline_layout(panel)
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
    assert not panel.distribution_table.tree.get_children()
    assert "nenhuma análise congelada" in panel.distribution_population_label.cget("text")
    notice = panel.distribution_notice.message.cget("text").casefold()
    assert "expectativa líquida" in notice and "não é calculada" in notice
    panel._apply_outcomes({
        "population": "X M15 · retorno bruto em 1 barras",
        "cost_status": "custos não aplicados",
        "directions": [
            {"label": "Intenção de compra", "wins": 2, "losses": 1, "flats": 0, "censored": 0,
             "n_total": 3, "n_mature": 3, "presence_state": "UNKNOWN", "freshness_state": "CURRENT",
             "evidence_state": "EXPLORATORY", "utility_state": "INCONCLUSIVE", "reason": "retorno bruto"},
        ],
    })
    rows = [panel.distribution_table.tree.item(iid, "values")
            for iid in panel.distribution_table.tree.get_children()]
    assert rows == [("Intenção de compra", "2", "1", "0", "0", "3", "3", "UNKNOWN", "CURRENT")]
    assert int(rows[0][1]) + int(rows[0][2]) + int(rows[0][3]) == int(rows[0][5])


def test_convergence_and_candidates_are_read_only_examples(relations_panel):
    panel, _root = relations_panel
    assert not panel.convergence_table.tree.get_children()
    assert not panel.candidate_table.tree.get_children()
    panel._apply_holdout([{
        "label": "M15:a → M15:b +1", "effect": 0.4, "effect_text": "+0,40",
        "ci": "IC 95%: +0,10 a +0,60", "n": "n=40", "delta": 0.3, "delta_text": "+0,30",
        "verdict": "HOLDOUT_CONSISTENT", "utility_state": "INCONCLUSIVE",
        "evidence_state": "EXPLORATORY", "freshness_state": "CURRENT", "presence_state": "UNKNOWN",
        "support_state": "corte 70/30 do serviço",
        "reason": "Diferença de ρ contra a persistência do alvo. Não é R por operação.",
    }])
    row = panel.convergence_table.tree.item("convergence-0", "values")
    assert row[0] == "M15:a → M15:b +1" and row[4] == "+0,30"
    assert "R/op" not in " ".join(row)
    assert panel.convergence_table.tree.item("convergence-0", "tags") == ("INCONCLUSIVE",)
    panel._apply_candidates([{
        "id": "M15:a→M15:b", "proposal": "a antecede b", "scope": "X · grade M15",
        "window": "ajuste 70% · fora 30%", "evidence_state": "EXPLORATORY",
        "freshness_state": "CURRENT", "presence_state": "UNKNOWN", "utility_state": "INCONCLUSIVE",
        "validity": "pesquisa; sem ação de aplicar preset ou Expert", "reason": "consistência exploratória",
    }])
    candidates = [panel.candidate_table.tree.item(iid, "values")
                  for iid in panel.candidate_table.tree.get_children()]
    assert len(candidates) == 1 and candidates[0][0] == "M15:a→M15:b"
    assert "EXEMPLO" not in candidates[0][0]
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


def test_theme_refresh_keeps_matrix_text_contrast_and_plot(relations_panel, lab_report):
    panel, _root = relations_panel
    panel.set_lab_report(lab_report)
    theme = panel.theme
    fills = []
    for _ in range(2):
        theme.toggle()
        panel.render()
        c = theme.colors
        for row, col in ((0, 0), (0, 1), (0, 2), (2, 0)):
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
    _load_timeline_layout(panel)
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
    assert len(panel._detail_panels) == 7
    for detail in [*panel._detail_panels.values(), panel.state_legend, panel.timeline_explanation]:
        assert not detail.expanded
        assert detail.body.winfo_manager() == ""
        assert str(detail.information_icon) in detail.toggle_button.cget("image")
        assert detail.information_icon.width() >= 14
    assert panel.synthetic_notice.winfo_ismapped()
    assert "nenhum número de exemplo" in panel.synthetic_notice.message.cget("text")
    assert "DADOS SINTÉTICOS" not in panel.synthetic_notice.message.cget("text")
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
    _load_timeline_layout(panel)
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


def test_inventory_view_lists_whole_registry_with_pages_search_and_reason(relations_panel):
    from mt5_extracao.analytics import indicator_registry
    panel, root = relations_panel
    seen = set()
    while True:
        tree = panel.inventory_table.tree
        seen |= {tree.item(iid, "values")[0] for iid in tree.get_children()}
        if not panel._inventory_next_cursor:
            break
        panel._inventory_page(1)
    assert seen == {row["feature_id"] for row in indicator_registry.definitions()}
    assert "sem base conectada" in panel.inventory_status.cget("text")
    panel.inventory_query.set("inexistente-xyz")
    panel.load_inventory()
    assert "limpe a busca" in panel.inventory_status.cget("text")
    assert not panel.inventory_table.tree.get_children()


def test_inventory_drops_stale_response_and_reports_error(relations_panel):
    panel, _root = relations_panel
    pending = []
    panel._run_bg = lambda work, done: pending.append((work, done))
    panel.inventory_query.set("rsi")
    panel.load_inventory()
    panel.inventory_query.set("atr")
    panel.load_inventory()
    old, new = pending
    new[1](*panel._call(new[0]))
    shown = {panel.inventory_table.tree.item(iid, "values")[0] for iid in panel.inventory_table.tree.get_children()}
    old[1](*panel._call(old[0]))
    assert shown == {panel.inventory_table.tree.item(iid, "values")[0]
                     for iid in panel.inventory_table.tree.get_children()}
    from mt5_extracao.laboratory import inventory
    assert shown and shown == {row["feature_id"] for row in inventory.search(query="atr")["items"]}
    panel.load_inventory()
    pending[-1][1](None, RuntimeError("base bloqueada"))
    assert "Inventário indisponível: base bloqueada" in panel.inventory_status.cget("text")


def test_inventory_distinguishes_declared_name_from_legacy_placeholder(relations_panel, tmp_path):
    from sqlalchemy import text
    from mt5_extracao.analytics import feature_store
    from mt5_extracao.database_manager import DatabaseManager
    panel, root = relations_panel
    db = DatabaseManager(db_path=str(tmp_path / "qa-names.sqlite"), broker_utc_offset=0)
    try:
        feature_store.register_definition(db, "qa.named", "Série QA legível", "1", {}, [], family="qa")
        feature_store.register_definition(db, "qa.old", "Nome antigo", "1", {}, [], family="qa")
        with db.engine.begin() as conn:
            conn.execute(text("UPDATE _feature_definitions SET name='value_type', definition_schema_version=1 "
                              "WHERE feature_id='qa.old'"))
        panel.db = db
        panel.inventory_query.set("qa.")
        panel.load_inventory()
        tree = panel.inventory_table.tree
        labels = {row["feature_id"]:tree.set(iid, "name") for iid, row in panel._inventory_rows.items()}
        assert labels == {"qa.named":"Série QA legível", "qa.old":"Nome não verificável"}
        iid = next(k for k, row in panel._inventory_rows.items() if row["feature_id"] == "qa.old")
        tree.selection_set(iid)
        panel._inventory_selected(None)
        assert "Nome legado não verificável" in panel._details["inventory"].get("1.0", "end-1c")
        assert next(d for d in feature_store.list_definitions(db) if d["feature_id"] == "qa.old")["name"] == "value_type"
    finally:
        panel.db = None
        db.close()
