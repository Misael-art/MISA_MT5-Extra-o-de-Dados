"""Seletor/relatório Tk sobre versões armazenadas, sem whitelist de fórmulas locais."""
import copy
import json

import pytest

from mt5_extracao.analytics import feature_store
from mt5_extracao.laboratory import service
from mt5_extracao.ui.laboratory_series import stored_choice_key
from tests.test_feature_snapshots import db
from tests.test_laboratory_stored_features import fill, register
from tests.test_ui_laboratory import panel
from tests.test_ui_laboratory_series import open_series


def ready(panel, db):
    panel.ui.app.db_manager = db
    series = open_series(panel)
    panel.pump(lambda:not series._loading_sources)
    return series


def test_versions_origin_search_refresh_and_hidden_choices_are_preserved(panel, db):
    register(db, ident="qa.metric", version="v2", unit="fraction")
    series = ready(panel, db)
    series.feature_query.set("qa.metric")
    assert series.feature_list.size() == 2
    assert "percent" in series.feature_list.get(0) and "fraction" in series.feature_list.get(1)
    series.clear_features_button.invoke()
    series.feature_list.selection_set(1)
    series.feature_list.event_generate("<<ListboxSelect>>")
    assert series._chosen_features == {stored_choice_key("qa.metric", "v2")}
    series.feature_origin.set("Cálculo local");series._filter_features()
    assert series.feature_list.size() == 0 and "Nenhum indicador" in series.feature_hint.cget("text")
    assert len(series._chosen_features) == 1
    series.feature_origin.set("Armazenados");series._filter_features()
    assert series.feature_list.curselection() == (1,)
    series.feature_query.set("v1 percent")
    assert series.feature_list.size() == 1
    register(db, ident="qa.novo")
    series.reload_button.invoke()
    panel.pump(lambda:not series._loading_sources)
    series.feature_query.set("qa.novo")
    assert series.feature_list.size() == 1 and len(series._chosen_features) == 1
    series.clear_features_button.invoke()
    assert not series._chosen_features


def test_stored_only_native_capture_renders_units_and_reports_no_direction(panel, db):
    fill(db, [1., 2., 3., 4.])
    feature_store.save_value(db, "QA", "M5", "qa.metric", "v1", "2025-01-01T00:00Z", 999.,
                            available_at="2025-01-01T00:06Z", observed_at="2025-01-01T00:06Z")
    series = ready(panel, db)
    series.clear_features_button.invoke()
    series.feature_query.set("qa.metric")
    series.feature_list.selection_set(0)
    series.feature_list.event_generate("<<ListboxSelect>>")
    series.strategy.set("Sem estratégia")
    series.strategy_box.event_generate("<<ComboboxSelected>>")
    series.analyze()
    assert series.pending and "Congelando" in series.status.cget("text")
    panel.pump(lambda:series.report is not None and not series.pending)
    report = series.report
    assert report is not None, series.status.cget("text")
    channel = report["channels"][0]
    assert report["selection"]["feature_ids"] == []
    assert channel["values"] == [1., 2., 3., 4.] and channel["directions"] == [None] * 4
    assert channel["unit"] == "percent"
    assert "congelados" in series.notice.message.cget("text")
    assert "disponibilidade declarada" in series.channel_details.cget("text")
    assert any(axis.get_title(loc="left") == "percent" for axis in series.panels["price"].chart.figure.axes)
    assert "não declaram regra de direção" in series.panels["timeline"].chart.figure.axes[0].texts[0].get_text()
    assert "Sem regra de direção" in series.panels["lags"].chart.figure.axes[0].texts[0].get_text()
    frozen = copy.deepcopy(report)
    series.set_report(service.load_report(json.dumps(report)))
    assert series.report == frozen
    panel.tabs.select(panel.tab_frames["Relações e aderência"])
    panel.render_visible()
    relations = panel.relations_panel
    panel.pump(lambda:relations.lab_matrix is not None)
    assert "Regra de direção não declarada" in relations.distribution_notice.message.cget("text")
    from mt5_extracao.ui.relations import _lab_cell
    cell = _lab_cell(relations.lab_matrix["cells"][0][0], relations.lab_matrix)
    assert "disponibilidade declarada" in cell["available_at"]
    assert "Regra de direção não declarada" in cell["reason"]


def test_single_derived_selection_refuses_stored_choices_before_work(panel, db, monkeypatch):
    series = ready(panel, db)
    series.symbol.set("QA")
    series._tf_items = [dict(timeframe="M7", synthetic=True)]
    series.tf_list.delete(0, "end");series.tf_list.insert("end", "M7 · sintético")
    series.tf_list.selection_set(0)
    series._chosen_features = {stored_choice_key("qa.metric", "v1")}
    monkeypatch.setattr(panel.ui, "run_bg", lambda *a:pytest.fail("não ignorar seleção"))
    series.analyze()
    assert "snapshots nativos" in series.status.cget("text") and not series.pending


def test_selector_has_no_200_entry_limit(panel, db):
    series = ready(panel, db)
    spec = series.catalog["stored_indicators"][0]
    series.catalog["stored_indicators"] = [dict(spec, feature_id=f"qa.externo_{i}", name=f"QA {i}") for i in range(205)]
    series.feature_origin.set("Armazenados");series._filter_features()
    assert series.feature_list.size() == 205
    series.feature_query.set("qa.externo_204")
    assert series.feature_list.size() == 1
    assert "qa.externo_204" in series.feature_list.get(0)


def test_category_presence_is_distinct_from_numeric_coverage(panel, db):
    from tests.test_laboratory_stored_features import report
    register(db, ident="qa.category", kind="CATEGORY", unit="state")
    fill(db, ["compra"], "qa.category")
    series = ready(panel, db)
    series.set_report(report(db, (("qa.category", "v1"),)))
    detail = series.channel_details.cget("text")
    assert "fatos disponíveis 4/4" in detail and "cobertura numérica 0/4" in detail
    assert "não usada como número" in detail


def test_stored_units_use_the_declared_label_without_inferring_price_semantics(panel, db):
    from tests.test_laboratory_stored_features import report
    register(db, ident="qa.metric", version="v2", unit="fraction")
    fill(db, [.1, .2, .3, .4], version="v2")
    series = ready(panel, db)
    series.set_report(report(db, (("qa.metric", "v1"), ("qa.metric", "v2"))))
    titles = [axis.get_title(loc="left") for axis in series.panels["price"].chart.figure.axes]
    assert "fraction" in titles and "percent" in titles
    assert "Fração do preço" not in titles
