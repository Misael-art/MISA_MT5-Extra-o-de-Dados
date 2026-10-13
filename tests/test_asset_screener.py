"""T6.6: triagem de ativos (Filtro A eliminatório e Filtro B score)."""
import configparser

import numpy as np
import pandas as pd
import pytest

from mt5_extracao.strategies import asset_screener as sc
from mt5_extracao.strategies.backtester import SymbolSpec
from tests.market_data import ohlc_from_close, random_walk, trend

SPEC = SymbolSpec(point=0.01, tick_size=0.01, tick_value=0.01, volume_min=0.01, volume_step=0.01)
DAILY = 1440


def daily(n=1200, seed=1, spread=5, **kw):
    return random_walk(n, seed=seed, spread=spread, freq="D", **kw)


def checks(result):
    return {c["name"]: c for c in result["checks"]}


def test_filter_a_passes_clean_long_history():
    a = sc.filter_a(daily(), SPEC, DAILY, sc.ScreenerConfig())
    assert all(c["passed"] for c in a["checks"]), [c["text"] for c in a["checks"] if not c["passed"]]
    assert a["metrics"]["years"] > 3


def test_filter_a_rejects_with_reasons():
    cfg = sc.ScreenerConfig()
    expensive = sc.filter_a(daily(spread=40), SPEC, DAILY, cfg)    # spread 0,40 vs ATR ~1,2
    c = checks(expensive)["Spread/ATR"]
    assert not c["passed"] and c["text"].startswith("spread/ATR alto (")
    short = checks(sc.filter_a(daily(n=300), SPEC, DAILY, cfg))["Histórico"]
    assert not short["passed"] and "mínimo 3 D1/H4" in short["text"]
    tiny = SymbolSpec(point=0.01, tick_size=0.01, tick_value=100, volume_min=1, volume_step=1)
    assert not checks(sc.filter_a(daily(), tiny, DAILY, cfg))["Lote mínimo"]["passed"]


def test_filter_a_spikes_and_bad_bars():
    df = daily()
    df.loc[df.index[-500:][::10], "spread"] = 100        # 10% das barras com pico
    df.loc[df.index[-50], "open"] = df["close"].iloc[-51] + 1000   # gap anômalo
    df.loc[df.index[-50], "high"] = df.loc[df.index[-50], "open"]
    a = checks(sc.filter_a(df, SPEC, DAILY, sc.ScreenerConfig(max_bad_bars=0.0005)))
    assert not a["Picos de spread"]["passed"]
    assert not a["Barras com problema"]["passed"] and "gaps > 5×ATR" in a["Barras com problema"]["text"]


def test_unknown_spec_warns_instead_of_failing():
    a = sc.filter_a(daily(), SymbolSpec(known=False), DAILY, sc.ScreenerConfig())
    assert "Spread/ATR" not in checks(a) and "Lote mínimo" not in checks(a)
    assert any("mt5x specs" in w for w in a["warnings"])


def test_filter_b_regimes_and_strategy():
    cfg = sc.ScreenerConfig()
    up = sc.filter_b(trend(400, step=0.5), DAILY, cfg, spread_atr=0.02)
    assert up["regime"] == "tendência" and up["direction"] == "alta"
    assert up["strategy"] in ("donchian", "ema_atr")
    flat = ohlc_from_close(100 + np.random.default_rng(4).normal(0, 1, 400))
    lat = sc.filter_b(flat, 60, cfg, spread_atr=0.02)
    assert lat["regime"] == "lateral" and lat["strategy"] == "bb_rsi"
    assert 0 <= up["score"] <= 100 and 0 <= lat["score"] <= 100
    assert sc.suggest_strategy("tendência", 50, 5, 0.5) == "orb"
    assert sc.suggest_strategy("tendência", 50, 60, 0.5) == "pullback"
    assert sc.suggest_strategy("transição", 10, 60, 0.5) == "squeeze"


def test_cost_and_correlation_lower_score():
    df, cfg = trend(400), sc.ScreenerConfig()
    cheap = sc.filter_b(df, DAILY, cfg, spread_atr=0.01)["score"]
    assert sc.filter_b(df, DAILY, cfg, spread_atr=0.09)["score"] < cheap
    corr = sc.filter_b(df, DAILY, cfg, spread_atr=0.01, correlation=0.95)
    assert corr["score"] < cheap and any("correlação" in n for n in corr["notes"])


def test_event_blocks(tmp_path):
    df = daily()
    last = df["time"].iloc[-1]
    path = tmp_path / "eventos.csv"
    pd.DataFrame({"time": [last + pd.Timedelta(hours=2), last + pd.Timedelta(hours=30)],
                  "symbol": ["USD", "EUR"], "impact": ["alto", "alto"]}).to_csv(path, index=False)
    events = sc.load_events(str(path))
    assert sc.upcoming_event("EURUSD", events, last, 4).startswith("evento de alto impacto (USD)")
    assert sc.upcoming_event("WIN$N", events, last, 4) is None
    b = sc.filter_b(df, DAILY, sc.ScreenerConfig(), 0.02, event="evento X")
    assert b["score"] == 0 and b["strategy"] is None and b["notes"][0].endswith("bloqueado")
    assert sc.load_events(str(tmp_path / "nao_existe.csv")).empty
    bad = tmp_path / "ruim.csv"
    bad.write_text("data,ativo\n")
    with pytest.raises(ValueError, match="time,symbol,impact"):
        sc.load_events(str(bad))


def test_screen_ranking_liquidity_and_table():
    data = {"BOM": daily(seed=1, volume=5000), "MEDIO": daily(seed=2, volume=3000),
            "ILIQUIDO": daily(seed=3, volume=10), "CARO": daily(seed=4, spread=60, volume=4000)}
    rows = sc.screen(data, {k: SPEC for k in data}, DAILY, timeframe_name="D1")
    by = {r["symbol"]: r for r in rows}
    assert not by["ILIQUIDO"]["valid"] and any("liquidez baixa" in x for x in by["ILIQUIDO"]["reasons"])
    assert not by["CARO"]["valid"] and by["CARO"]["score"] == 0
    assert by["BOM"]["valid"] and by["MEDIO"]["valid"]
    assert [r["valid"] for r in rows] == sorted([r["valid"] for r in rows], reverse=True)
    table = sc.format_table(rows)
    assert table.splitlines()[0].startswith("Ativo")
    assert "❌" in table and "spread/ATR alto" in table and "não validada" in table


def test_screen_prefers_validated_strategy():
    data = {"A": trend(1200, step=0.3, freq="D", spread=1)}
    base = sc.screen(data, {"A": SPEC}, DAILY, timeframe_name="D1")[0]
    suggested = base["strategy"]
    other = "ema_atr" if suggested == "donchian" else "donchian"
    val = pd.DataFrame({"symbol": ["A"], "timeframe": ["D1"], "strategy": [other], "approved": [1]})
    row = sc.screen(data, {"A": SPEC}, DAILY, validations=val, timeframe_name="D1")[0]
    assert row["strategy"] == other and row["validated"].startswith("✅")


def test_config_from_ini():
    cfg = configparser.ConfigParser()
    cfg.read_string("[SCREENER]\nmax_spread_atr = 0.05\n[STRATEGY]\nrisk_per_trade = 0.005\n"
                    "[APP]\nsession_start = 10:00\n")
    s = sc.ScreenerConfig.from_config(cfg)
    assert s.max_spread_atr == 0.05 and s.risk_per_trade == 0.005 and s.session_start == "10:00"
    assert s.max_correlation == 0.7
