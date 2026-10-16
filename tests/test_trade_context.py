import json

import pandas as pd
import pytest

from mt5_extracao.trade_analytics import context, export


def book():
    return pd.DataFrame({"run_id": ["a", "a", "b"], "source": ["python"] * 3,
                         "entry_time": [pd.Timestamp("2025-01-01"), None, pd.Timestamp("2025-01-02")],
                         "exit_time": pd.to_datetime(["2025-01-02", "2025-01-03", "2025-01-04"]),
                         "direction": [1, None, 0], "completeness": ["completo", "parcial", "parcial"],
                         "time_ref": ["entrada", "saida", "entrada"], "pnl": [10., -2., 0.]})


def test_population_describes_observed_fields_without_inventing_cause_or_mutation(tmp_path):
    lab = book()
    before = lab.copy(deep=True)
    c = context.population(lab, 12345)
    assert c["n"] == 3 and c["runs"] == 2 and c["partial"] == 2
    assert c["missing_entry"] == 1 and c["missing_direction"] == 2
    assert c["sources"] == ["python"] and c["observed_overlap"] is True
    assert c["initial_equity_reference"] == 12345
    assert c["currency_status"] == c["costs_status"] == c["timezone_status"] == "not_recorded"
    assert c["aggregation"] == "descriptive_runs"
    assert "MQL5" not in context.summary(c) + context.details(c)
    assert "sem horário de entrada" in context.summary(c)
    assert "não carteira executável" in context.summary(c)
    assert "não prova duplicidade" in context.details(c)
    json.dumps(c, allow_nan=False)
    path = export.proposal("robot:python:x", [], {}, folder=tmp_path, context=c)
    assert json.loads(path.read_text())["contexto_populacao"] == c
    pd.testing.assert_frame_equal(lab, before)


def test_missing_direction_alone_does_not_claim_missing_entry():
    lab = book().iloc[[0]].copy()
    lab["direction"] = None
    lab["completeness"] = "parcial"
    c = context.population(lab)
    assert c["missing_entry"] == 0 and c["missing_direction"] == 1
    assert "sem horário de entrada" not in context.summary(c)
    assert "sem direção válida" in context.summary(c)
    assert "carteira executável" not in context.summary(c)
    assert c["observed_overlap"] is False


def test_nonoverlap_is_not_a_claim_about_whole_test_coverage():
    lab = book().iloc[[0, 2]].copy()
    lab.loc[lab["run_id"].eq("b"), "entry_time"] = pd.Timestamp("2025-02-01")
    lab.loc[lab["run_id"].eq("b"), "exit_time"] = pd.Timestamp("2025-02-02")
    c = context.population(lab)
    assert c["observed_overlap"] is False and c["observed_run_intervals"] == 2
    assert "não representam toda a cobertura" in context.details(c)


@pytest.mark.parametrize("capital", [None, float("inf"), float("nan"), "inválido"])
def test_empty_and_invalid_capital_remain_unknown(capital):
    c = context.population(pd.DataFrame(), capital)
    assert c["n"] == c["runs"] == 0 and c["observed_overlap"] is None
    assert c["aggregation"] == "unknown" and c["initial_equity_reference"] is None
    json.dumps(c, allow_nan=False)


def test_legacy_minimal_rows_are_readable():
    c = context.population(pd.DataFrame({"run_id": ["old"], "pnl": [-1]}))
    assert c["n"] == 1 and c["missing_entry"] == c["missing_direction"] == 1
    assert c["observed_overlap"] is None
