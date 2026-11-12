"""T16.15: recuperação condicional (H1–H4, H6), excursões por caso e estudo pareado."""
import json

import numpy as np
import pandas as pd
import pytest

from mt5_extracao.strategies.backtester import EXIT_REASONS, BacktestConfig, SymbolSpec
from mt5_extracao.trade_analytics.execution import engine, modes, reversal, study

# point = tick = 0,1 e valor do tick 0,1: 1,0 de preço vale 1,0 de dinheiro por lote (contas à vista nos testes)
SPEC = SymbolSpec(symbol="X", point=0.1, tick_size=0.1, tick_value=0.1, known=False)
ZERO = BacktestConfig(swap=False)


def _bars(rows):
    """rows = [(open, high, low, close)], barras de 15 min."""
    frame = pd.DataFrame(rows, columns=["open", "high", "low", "close"], dtype=float)
    frame.insert(0, "time", pd.date_range("2026-01-05 10:00", periods=len(frame), freq="15min"))
    return frame.assign(tick_volume=1, spread=0, real_volume=0)


def _entry(bars, direction=1, stop=90.0, target=110.0):
    return pd.DataFrame([{"seq": 0, "direction": direction, "entry_time": bars.time.iloc[0], "entry_price": 100.0,
                          "stop_price": stop, "target_price": target, "volume": 1.0,
                          "exit_time": bars.time.iloc[-1], "exit_reason": EXIT_REASONS["end"]}])


def _signals(n, reversal_at=(), continuation_at=(), direction=1):
    empty = lambda: np.zeros(n, dtype=bool)
    out = {"reversal": {1: empty(), -1: empty()}, "continuation": {1: empty(), -1: empty()}}
    out["reversal"][direction][list(reversal_at)] = True
    out["continuation"][direction][list(continuation_at)] = True
    return out


def _run(mode, bars, signals, entry=None, cfg=ZERO, **params):
    policy = modes.MODES[mode].policy(engine.ExitPolicy(), params)
    row = _entry(bars) if entry is None else entry
    return engine.simulate(row, bars, SPEC, cfg, policy, pd.Series(np.ones(len(bars))), signals=signals)


# Compra a 100, stop 90 (R = 10). Cai até a zona (fecha 93), repica e depois despenca.
DIP = [(100, 100.5, 99, 99.5), (99.5, 99.5, 93, 93), (93.5, 97.5, 93, 97), (97, 97.2, 91, 92), (92, 92, 85, 86)]


def test_base_takes_the_full_stop_and_records_both_lenses():
    bars = _bars(DIP)
    result = _run("ponto_fixo", bars, None)
    trade, case = result.trades[0], result.cases[0]
    assert trade.exit_reason == EXIT_REASONS["stop"] and trade.pnl == pytest.approx(-10)
    assert case["mae_price"] == pytest.approx(10) and case["mae_r"] == pytest.approx(1)
    assert case["mfe_price"] == pytest.approx(0.5) and case["bars_to_mfe"] == 0 and case["bars_to_mae"] == 4
    assert case["floating_min"] == pytest.approx(-10) and case["peak_volume"] == 1 and case["intervention"] is None
    assert case["rebound_after_mae_price"] == 0                      # nada observado depois do extremo final


def test_single_add_exits_at_net_breakeven_of_the_set():
    bars = _bars(DIP)
    result = _run("recuperacao_unica", bars, _signals(len(bars), reversal_at=[1]))
    trade, case = result.trades[0], result.cases[0]
    # sinal no fechamento da barra 1 → adição na abertura da barra 2 (93,5); equilíbrio = (100 + 93,5) / 2
    assert trade.exit_reason == EXIT_REASONS["recovery"] and trade.exit_price == pytest.approx(96.75)
    assert trade.pnl == pytest.approx(0) and trade.volume == 2 and trade.target_price == 110   # alvo inicial preservado
    assert result.adds == 1 and case["intervention"] == "adicao" and case["intervention_bar"] == 2
    # na barra de saída só o preço executado conta: o pior flutuante observado é o da barra 1, com um lote
    assert case["added_volume"] == 1 and case["floating_min"] == pytest.approx(-7.0)


def test_add_enlarges_the_loss_when_the_move_continues():
    rows = DIP[:2] + [(93.5, 94, 89, 89.5)]                         # adiciona a 93,5 e o stop chega na mesma barra
    bars = _bars(rows)
    result = _run("recuperacao_unica", bars, _signals(len(bars), reversal_at=[1]))
    assert result.trades[0].exit_reason == EXIT_REASONS["stop"]
    assert result.trades[0].pnl == pytest.approx(-10 - 3.5)         # 1R + f × (preço da adição − stop)


def test_decision_is_causal_no_signal_no_zone_and_gap_keep_the_base():
    bars = _bars(DIP)
    assert _run("recuperacao_unica", bars, _signals(len(bars))).adds == 0                    # sem sinal
    assert _run("recuperacao_unica", bars, _signals(len(bars), reversal_at=[0])).adds == 0   # sinal fora da zona
    late = _run("recuperacao_unica", bars, _signals(len(bars), reversal_at=[2]))             # barra 2 fecha a −0,3 R
    assert late.adds == 0 and late.trades[0].pnl == pytest.approx(-10)
    gap = _bars(DIP[:2] + [(88, 89, 87, 88)])                        # abre além do stop: vale o stop, sem adição
    result = _run("recuperacao_unica", gap, _signals(len(gap), reversal_at=[1]))
    assert result.adds == 0 and result.trades[0].exit_price == pytest.approx(88)


def test_short_position_mirrors_the_long_one():
    rows = [(100, 101, 99.5, 100.5), (100.5, 107, 100.5, 107), (106.5, 107, 102.5, 103), (103, 109, 102.8, 108),
            (108, 115, 108, 114)]
    bars = _bars(rows)
    entry = _entry(bars, direction=-1, stop=110.0, target=90.0)
    result = _run("recuperacao_unica", bars, _signals(len(bars), reversal_at=[1], direction=-1), entry=entry)
    assert result.trades[0].exit_reason == EXIT_REASONS["recovery"]
    assert result.trades[0].exit_price == pytest.approx(103.25) and result.trades[0].pnl == pytest.approx(0)


def test_costs_move_the_breakeven_and_spread_is_paid_on_the_add():
    rows = DIP[:2] + [(93.5, 98.5, 93, 98)]
    bars = _bars(rows).assign(spread=1)                              # 1 ponto × 0,1 = 0,1 de spread
    cfg = BacktestConfig(swap=False, commission_per_lot=0.5, real_spread=False)
    result = _run("recuperacao_unica", bars, _signals(len(bars), reversal_at=[1]), cfg=cfg)
    trade = result.trades[0]
    # compra a adição no ask (93,6); equilíbrio = média (96,8) + 2 × comissão por lote (1,0)
    assert trade.exit_price == pytest.approx(97.8)
    assert trade.pnl == pytest.approx((97.8 - 100) + (97.8 - 93.6) - 4 * 0.5)       # −0,0: equilíbrio líquido


def test_managed_recovery_protects_then_invalidates():
    rows = DIP[:2] + [(93.5, 97.5, 93.2, 97), (97, 97.2, 96.8, 97), (97, 97, 96, 96.2)]
    bars = _bars(rows)
    result = _run("recuperacao_gerida", bars, _signals(len(bars), reversal_at=[1]))
    trade = result.trades[0]
    # tocou o equilíbrio (96,75) na barra 2: o stop sobe para ele e fecha ali na barra 4
    assert trade.exit_reason == EXIT_REASONS["breakeven"] and trade.exit_price == pytest.approx(96.75)
    broken = _bars(DIP[:2] + [(93.5, 94, 92.5, 92.8), (92.6, 93, 91, 92)])   # fecha abaixo da mínima do sinal (93)
    result = _run("recuperacao_gerida", broken, _signals(len(broken), reversal_at=[1]))
    assert result.trades[0].exit_reason == EXIT_REASONS["invalidation"]
    assert result.trades[0].exit_price == pytest.approx(92.6)


def test_early_exit_and_independent_reentry():
    bars = _bars(DIP)
    early = _run("saida_antecipada", bars, _signals(len(bars), continuation_at=[1]))
    assert early.trades[0].exit_reason == EXIT_REASONS["early"] and early.trades[0].pnl == pytest.approx(-6.5)
    assert early.adds == 0 and early.cases[0]["intervention"] == "saida"
    indep = _run("reversao_independente", bars, _signals(len(bars), reversal_at=[1]))
    single = _run("recuperacao_unica", bars, _signals(len(bars), reversal_at=[1]))
    # mesmo nível de saída (96,75): H6 realiza −6,5 e ganha +3,25 com um lote; H2 empata com dois
    assert indep.trades[0].exit_price == single.trades[0].exit_price == pytest.approx(96.75)
    assert indep.trades[0].pnl == pytest.approx(-6.5 + 3.25) and indep.trades[0].volume == 1
    assert single.trades[0].pnl - indep.trades[0].pnl == pytest.approx(3.25)     # o que o lote carregado rendeu


def test_exposure_cap_and_invalid_setups_are_refused():
    bars = _bars(DIP)
    capped = _run("recuperacao_proporcional", bars, _signals(len(bars), reversal_at=[1]), teto_lotes=2)
    assert capped.adds == 0 and capped.capped == 1 and capped.max_volume == 1
    policy = modes.MODES["recuperacao_unica"].policy(engine.ExitPolicy(), {})
    with pytest.raises(ValueError, match="sinais causais"):
        engine.simulate(_entry(bars), bars, SPEC, ZERO, policy, None)
    with pytest.raises(ValueError, match="gradiente"):
        engine.simulate(_entry(bars), bars, SPEC, ZERO, policy.with_(grid_levels=1), None,
                        signals=_signals(len(bars)))
    assert all(m.study_only for m in modes.recovery_modes().values())
    assert not set(modes.recovery_modes()) & set(modes.exit_modes())          # a comparação padrão não muda


def test_signals_never_look_ahead():
    from tests.market_data import random_walk
    bars = random_walk(400, seed=11, vol=0.8, freq="15min")
    full = reversal.compute(bars)
    cut = reversal.compute(bars.iloc[:250])
    for kind in ("reversal", "continuation"):
        for side in (1, -1):
            assert np.array_equal(full[kind][side][:250], cut[kind][side])
    assert full["reversal"][1].any() and full["reversal"][-1].any()
    assert not (full["continuation"][1] & full["reversal"][1]).any()
    with pytest.raises(ValueError, match="desconhecido"):
        reversal.parse("rsi,adivinhacao")
    with pytest.raises(ValueError, match="min_sinais"):
        reversal.compute(bars, ("rsi",), min_signals=2)


@pytest.fixture
def lab(tmp_path):
    from mt5_extracao import robots
    from mt5_extracao.database_manager import DatabaseManager
    from mt5_extracao.robots import backtest
    from tests.market_data import random_walk
    db = DatabaseManager(db_path=str(tmp_path / "b.db"))
    db.save_ohlcv_data("BBB", "15 minutos", random_walk(3000, seed=4, vol=0.6, freq="15min", spread=10))
    robot = next(r for r in robots.load_catalog(None, db) if r.key == "python:donchian")
    run = backtest.run_python(db, None, robot, "BBB", "M15")
    cfg = tmp_path / "config.ini"
    cfg.write_text(f"[DATABASE]\ntype = sqlite\npath = {db.db_path}\n", encoding="utf-8")
    yield db, run, str(cfg)
    db.close()


def test_paired_study_on_a_real_run(lab):
    db, run, _cfg = lab
    report = study.run(db, None, run["run_id"])
    variants = report["variants"]
    assert set(variants) == set(study.VARIANTS) and report["study_only"]
    base = variants["H0"]
    assert base["verdict"] == "base" and base["diff_net"] == 0 and base["n_intervened"] == 0
    assert base["metrics"]["net"] == pytest.approx(run["metrics"]["net_profit"], rel=1e-9)
    for label, item in variants.items():
        assert item["n_cases"] == base["n_cases"]                    # uma operação original = um caso
        assert item["recovered"] + item["aggravated"] <= item["n_intervened"]
        assert item["verdict"] in ("base", "aprofundar", "reprovada", "amostra insuficiente")
        if item["verdict"] == "reprovada":
            assert item["reasons"]
    assert variants["H2"]["adds"] > 0 and variants["H2"]["max_volume"] > base["max_volume"]
    assert variants["H1"]["adds"] == 0 and variants["H1"]["max_volume"] == base["max_volume"]
    assert report["fidelity"]["grade"] == "D" and report["decision_certified"] and not report["intrabar_certified"]
    assert report["excursions"]["losers"]["mae_r_p50"] > report["excursions"]["winners"]["mae_r_p50"]
    json.dumps(report)                                               # serializável para --json


def test_recovery_frequency_alone_never_approves():
    base = pd.DataFrame({"seq": range(40), "pnl": [-10.0] * 40,
                         "entry_time": pd.date_range("2026-01-05", periods=40, freq="D"),
                         "exit_time": pd.date_range("2026-01-05 01:00", periods=40, freq="D")})

    class Fake:
        """39 casos recuperados (+1 cada) e um desastre (−200): frequência alta, cauda pior."""
        trades = [type("T", (), {"pnl": -9.0 if i else -210.0, "entry_time": base.entry_time[i],
                                 "exit_time": base.exit_time[i], "r_multiple": -1.0, "volume": 2.0})() for i in range(40)]
        source_seqs = list(range(40))
        cases = [{"seq": i, "intervention": "adicao", "added_volume": 1.0, "floating_min": -210.0} for i in range(40)]
        metrics, max_volume, adds, capped = {}, 2.0, 40, 0
    got = study._paired("H2", "recuperacao_unica", Fake, base, 100_000.0, np.arange(40) >= 32, 30, 5.0)
    assert got["recovered"] == 39 and got["aggravated"] == 1 and got["verdict"] == "reprovada"
    assert any("cauda" in reason or "intervalo" in reason for reason in got["reasons"])


def test_cli_recovery_fidelity_and_compare(lab, capsys):
    from mt5_extracao import cli
    _db, run, cfg = lab

    def call(*args):
        code = cli.main(["--config", cfg, "--json", "trades", *args])
        return code, json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    code, out = call("recovery", "--run", run["run_id"], "--signals", "rsi,bollinger", "--zone", "0.5")
    assert code == cli.EXIT_OK and out["data"]["recovery"]["protocol"]["signals"] == ["rsi", "bollinger"]
    assert out["data"]["recovery"]["protocol"]["zone_r"] == 0.5
    code, out = call("fidelity", "--symbol", "BBB", "--timeframe", "M15")
    assert code == cli.EXIT_OK and out["data"]["fidelity"]["grade"] == "D"
    code, out = call("compare", "--run", run["run_id"], "--modes", "ponto_fixo,recuperacao_unica")
    assert code == cli.EXIT_OK and out["data"]["modes"]["recuperacao_unica"]["study_only"] is True
    assert cli.main(["--config", cfg, "trades", "recovery", "--run", run["run_id"], "--zone", "1.5"]) == cli.EXIT_USAGE
