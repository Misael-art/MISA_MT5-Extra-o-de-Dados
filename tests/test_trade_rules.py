"""T16.9: regras de filtro, simulador e a separação dos dados (walk-forward com folga + reserva final)."""
import configparser

import numpy as np
import pandas as pd
import pytest

from mt5_extracao.trade_analytics import rules


def _lab(n_days=300, seed=5, friday=-15.0):
    rng = np.random.default_rng(seed)
    rows = []
    for k, d in enumerate(pd.bdate_range("2024-01-08", periods=n_days)):
        for h in (9, 11):
            e = d + pd.Timedelta(hours=h, minutes=5)
            pnl = rng.normal(2, 10) + (friday if d.dayofweek == 4 else 0)
            rows.append({"run_id": "r", "seq": len(rows), "entry_time": e, "exit_time": e + pd.Timedelta(minutes=40),
                         "pnl": pnl, "day_of_week": d.dayofweek, "event_impact": 3 if k % 10 == 0 else 0,
                         "volatility_bucket": ("baixa", "média", "alta")[k % 3]})
    return pd.DataFrame(rows)


def test_rules_remove_exactly_what_they_say():
    lab = _lab(50)
    assert set(rules.apply(lab, [{"tipo": "excluir_dia", "dias": [4]}])["day_of_week"]) == {0, 1, 2, 3}
    window = rules.apply(lab, [{"tipo": "janela_horaria", "inicio": "10:00", "fim": "12:00"}])
    assert set(pd.to_datetime(window["entry_time"]).dt.hour) == {11}
    assert (rules.apply(lab, [{"tipo": "excluir_noticia", "impacto_min": 3}])["event_impact"] < 3).all()
    assert "alta" not in set(rules.apply(lab, [{"tipo": "excluir_volatilidade", "faixas": ["alta"]}])["volatility_bucket"])
    assert len(rules.apply(lab, [])) == len(lab)                       # sem regra, o original
    with pytest.raises(ValueError):
        rules.apply(lab, [{"tipo": "inventada"}])


def test_switch_mode_uses_the_resimulated_result():
    lab = _lab(30)
    other = lab[["run_id", "seq"]].assign(pnl=100.0)
    out = rules.apply(lab, [{"tipo": "trocar_modo", "modo": "trailing_atr", "quando": {"volatility_bucket": ["alta"]}}],
                      {"trailing_atr": other})
    assert (out.loc[out["volatility_bucket"] == "alta", "pnl"] == 100).all()
    assert (out.loc[out["volatility_bucket"] != "alta", "pnl"] == lab.loc[lab["volatility_bucket"] != "alta", "pnl"]).all()
    with pytest.raises(ValueError):
        rules.apply(lab, [{"tipo": "trocar_modo", "modo": "x", "quando": {}}])


def test_split_keeps_the_holdout_untouched_and_folds_ordered():
    lab = _lab(300)
    plan = rules.split(lab)
    assert not plan.weak and len(plan.folds) == 3
    assert plan.holdout.mean() == pytest.approx(0.2, abs=0.01)
    last_search = pd.to_datetime(lab["entry_time"])[~plan.holdout].max()
    assert pd.to_datetime(lab["entry_time"])[plan.holdout].min() > last_search
    for search, check in plan.folds:
        assert not (search & plan.holdout).any() and not (check & plan.holdout).any()
        assert pd.to_datetime(lab["entry_time"])[search].max() < pd.to_datetime(lab["entry_time"])[check].min()


def test_few_trades_reduce_folds_then_mark_weak():
    cfg = configparser.ConfigParser()
    cfg["TRADE_ANALYTICS"] = {"min_trades_cell": "30"}
    mid = rules.split(_lab(70), cfg)
    assert not mid.weak and 1 <= len(mid.folds) < 3 and "prova" in mid.note
    tiny = rules.split(_lab(20), cfg)
    assert tiny.weak and tiny.folds == [] and "fraca" in tiny.note


def test_evaluate_shows_a_real_effect_out_of_sample():
    out = rules.evaluate(_lab(300), [{"tipo": "excluir_dia", "dias": [4]}])
    assert out["folds_improved"] == len(out["folds"]) == 3
    assert out["holdout"]["after"]["expectancy"] > out["holdout"]["before"]["expectancy"]
    assert out["removed"] > 0 and out["rules"] == ["Não operar Sex"]


def test_evaluate_on_noise_does_not_improve_everywhere():
    lab = _lab(300, friday=0.0)
    out = rules.evaluate(lab, [{"tipo": "excluir_dia", "dias": [4]}])
    gain = out["holdout"]["after"]["expectancy"] - out["holdout"]["before"]["expectancy"]
    assert abs(gain) < 2.0                                              # sem efeito real, nada de salto
