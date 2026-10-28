"""Cruzamentos do Laboratório: o efeito sai do serviço e as contagens fecham."""
import numpy as np
import pandas as pd
import pytest

from mt5_extracao.laboratory import service


def _report(closes, values, directions):
    return dict(
        analysis_id="a" * 64, symbol="X", grid_timeframe="M15",
        selection=dict(as_of="2025-02-01T00:00:00Z"),
        formula_source_sha256="b" * 64,
        qualification=dict(exploratory=True),
        times=list(pd.date_range("2025-01-06", periods=len(closes), freq="15min").astype(str)),
        prices=[[c, c, c, c] for c in closes],
        channels=[dict(id="M15:probe", name="Sonda", timeframe="M15", values=values,
                       directions=directions, events=None, diagnosis=None)],
        pairs=[dict(left="M15:probe", right="M15:other", lag_bars=1, relation="PREDICTIVE_CANDIDATE",
                    out_of_sample=dict(verdict="HOLDOUT_CONSISTENT", holdout_correlation=0.4,
                                       naive_holdout_correlation=0.1, holdout_pairs=40, holdout_ci_95=[0.1, 0.6])),
               dict(left="M15:probe", right="M15:other", lag_bars=0, relation="DESCRIPTIVE", out_of_sample=None)],
    )


def test_forward_return_leaves_the_unfinished_bar_out_and_matches_the_ratio():
    closes = pd.Series([100, 110, 99, 99, 80, 96], dtype=float)
    out = service.forward_close_return(closes, 1)
    np.testing.assert_allclose(out.iloc[:5], [0.1, 99 / 110 - 1, 0.0, 80 / 99 - 1, 0.2])
    assert np.isnan(out.iloc[-1])


def test_target_of_its_own_return_is_one_and_constant_is_not_zero():
    closes = [100, 110, 99, 120, 108, 140, 126, 150]
    returns = service.forward_close_return(pd.Series(closes, dtype=float), 1)
    report = _report(closes, list(returns), [1] * len(closes))
    cell = service.target_matrix(report, horizon=1)["cells"][0][0]
    assert cell["effect"] == pytest.approx(1.0) and cell["reason"] is None
    flat = _report(closes, [3] * len(closes), [1] * len(closes))
    blocked = service.target_matrix(flat, horizon=1)["cells"][0][0]
    assert blocked["effect"] is None and blocked["reason"] == "CONSTANT_SERIES"


def test_direction_counts_close_and_a_gap_is_not_a_zero_correlation():
    closes = [100, 110, 99, 99, 80, 96]
    directions = [1, 1, 1, -1, -1, 0]
    report = _report(closes, directions, directions)
    rows = service.direction_outcomes(report, "M15:probe", horizon=1)["directions"]
    buy, sell = rows
    assert (buy["wins"], buy["losses"], buy["flats"], buy["n_total"]) == (1, 1, 1, 3)
    assert (sell["wins"], sell["losses"], sell["flats"], sell["n_total"]) == (1, 1, 0, 2)
    assert buy["wins"] + buy["losses"] + buy["flats"] == buy["n_total"]
    # Quatro blocos exigem ao menos três barras cada; oito pontos deixariam todo bloco abaixo do mínimo.
    values = [1, 2, 3, None, None, None, 4, 5, 6, 7, 8, 9]
    other = [1, 2, 3, None, None, None, 6, 5, 4, 9, 8, 7]
    gap = _report([100 + i for i in range(12)], values, [0] * 12)
    gap["channels"].append(dict(id="M15:other", name="Outro", timeframe="M15", values=other,
                                directions=[0] * 12, events=None, diagnosis=None))
    windows = service.stability_windows(gap, "M15:probe", "M15:other", lag=0, blocks=4)["windows"]
    assert windows[1]["value"] is None and windows[1]["reason"] == "INSUFFICIENT_PAIRS"
    assert windows[0]["value"] == pytest.approx(1.0)
    assert windows[2]["value"] == pytest.approx(-1.0)
    assert " " in windows[0]["center"]


def test_candidates_keep_only_the_service_verdict():
    rows = service.research_candidates(_report([100, 110, 120], [1, 2, 3], [1, 0, -1]))
    assert len(rows) == 1 and rows[0]["validity"].startswith("pesquisa")
    compared = service.holdout_comparison(_report([100, 110, 120], [1, 2, 3], [1, 0, -1]))
    assert compared[0]["delta"] == pytest.approx(0.3)
    assert "não é r por operação" in compared[0]["reason"].casefold()
    assert "R/op" not in compared[0]["reason"]
