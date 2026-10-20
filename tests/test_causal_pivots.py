"""T19.6: referências por janelas/listas racionais, sem algoritmo do produto."""
import json
from fractions import Fraction

import numpy as np
import pandas as pd
import pytest

from mt5_extracao.analytics import feature_store, indicator_context, indicator_materializer as materializer, indicator_registry, resampling
from mt5_extracao.data_exporter import DataExporter
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.strategies import indicators
from tests.test_indicator_materialization import db, facts, seed, source

F = "structure.fractals_5_strict_"
Z = "structure.zigzag_causal_d12_pct1_b3_"
IDS = [prefix + side for prefix in (F, Z) for side in ("top", "bottom", "provisional")]


def trap(count=84):
    # Extremos espaçados, empates e barras que poderiam repintar um desenho final.
    values = [100 + [0, 2, 5, 9, 14, 20, 20, 15, 8, 1, -5, -10, -15, -15, -9, -2, 4, 10][i % 18]
              for i in range(count)]
    return pd.DataFrame({"high": np.array(values) + 0.5, "low": np.array(values) - 0.5})


def independent_fractals(frame):
    highs, lows = list(frame.high), list(frame.low)
    events = {"top": [], "bottom": []}
    for origin in range(2, len(frame) - 2):
        for kind, values in (("top", highs), ("bottom", lows)):
            window = values[origin - 2:origin + 3]
            valid = all(np.isfinite(highs[j]) and np.isfinite(lows[j]) and highs[j] >= lows[j]
                        for j in range(origin - 2, origin + 3))
            extreme = max(window) if kind == "top" else min(window)
            if valid and window[2] == extreme and window.count(extreme) == 1:
                events[kind].append((origin, origin + 2, values[origin]))
    return events


def independent_zigzag(frame, depth=12, deviation=Fraction(1, 100), backstep=3):
    """Oráculo por segmentos: busca exaustiva de extremos, preços racionais."""
    high = [Fraction(float(v)) for v in frame.high]
    low = [Fraction(float(v)) for v in frame.low]
    events, candidate = [], None
    for end in range(depth - 1, len(frame)):
        positions = list(range(end - depth + 1, end + 1))
        highest = sorted(positions, key=lambda j: (-high[j], j))[0]
        lowest = sorted(positions, key=lambda j: (low[j], j))[0]
        if candidate is None and highest != lowest:
            side = "top" if highest < lowest else "bottom"
            origin = min(highest, lowest)
            candidate = (side, origin, high[origin] if side == "top" else low[origin])
        if candidate is None:
            continue
        side, origin, price = candidate
        extreme = high[end] if side == "top" else low[end]
        better = extreme > price if side == "top" else extreme < price
        if better:
            candidate = (side, end, extreme)
            continue
        opposite = low[end] if side == "top" else high[end]
        threshold = price * (1 - deviation if side == "top" else 1 + deviation)
        reached = opposite <= threshold if side == "top" else opposite >= threshold
        at_extreme = opposite == (min(low[j] for j in positions) if side == "top" else max(high[j] for j in positions))
        if end - origin >= backstep and reached and at_extreme:
            events.append((side, origin, end, float(price)))
            candidate = ("bottom" if side == "top" else "top", end, opposite)
    return events


def event_list(snapshots):
    return [(side, p["origin"], i, p["price"])
            for i, snapshot in enumerate(snapshots) for side in ("top", "bottom")
            if (p := snapshot[side]) is not None and p["confirmation"] == i]


def test_fractals_independent_windows_ties_borders_and_delayed_confirmation():
    frame = trap()
    # Um pico estrito conhecido manualmente: origem 4, confirmação 6.
    manual = pd.DataFrame({"high": [1, 2, 3, 4, 9, 4, 3], "low": [0] * 7})
    out = indicators.causal_fractals(manual)
    assert all(s["top"] is None for s in out[:6])
    assert out[6]["top"] == dict(kind="top", price=9., origin=4, confirmation=6)
    actual = event_list(indicators.causal_fractals(frame))
    expected = independent_fractals(frame)
    assert actual == sorted([(side, *event) for side, events in expected.items() for event in events], key=lambda e: (e[2], e[0] == "bottom"))
    assert all(origin not in (5, 6, 12, 13) for _, origin, _, _ in actual)  # empates rejeitados
    flat = indicators.causal_fractals(pd.DataFrame({"high": [1.] * 9, "low": [0.] * 9}))
    assert event_list(flat) == [] and flat[-1]["provisional"] == []
    assert not indicators.causal_fractals(frame.iloc[:4])[-1]["ready"]
    outside = pd.DataFrame({"high": [1, 2, 5, 2, 1], "low": [0, -1, -5, -1, 0]})
    assert {e[0] for e in event_list(indicators.causal_fractals(outside))} == {"top", "bottom"}


def test_zigzag_independent_rational_reference_and_provisional_tail():
    frame = trap()
    result = indicators.causal_zigzag(frame)
    assert event_list(result) == independent_zigzag(frame)
    assert len(event_list(result)) >= 5
    assert result[-1]["provisional"] and result[-1]["provisional"][0]["confirmation"] is None
    assert event_list(indicators.causal_zigzag(pd.DataFrame({"high": [100.] * 40, "low": [99.] * 40}))) == []


@pytest.mark.parametrize("calculator", [indicators.causal_fractals, indicators.causal_zigzag])
def test_every_prefix_is_identical_after_future_extension(calculator):
    frame = trap(100)
    frame.index = ["bar-" + str(i * 3) for i in range(len(frame))]
    whole = calculator(frame)
    for end in [1, 4, 5, 11, 12, 13, 19, 35, 73, 99]:
        assert calculator(frame.iloc[:end]) == whole[:end]
    short = calculator(frame.iloc[:73]); tail = frame.copy()
    tail.iloc[73:, tail.columns.get_loc("high")] += 1000
    tail.iloc[73:, tail.columns.get_loc("low")] -= 50
    assert calculator(tail)[:73] == short
    assert event_list(short) == [e for e in event_list(whole) if e[2] < 73]


@pytest.mark.parametrize("bad", [np.nan, np.inf, -np.inf, "reversed"])
def test_missing_invalid_source_resets_window_and_levels(bad):
    frame = trap(50)
    frame.loc[30, "high"] = frame.loc[30, "low"] - 1 if isinstance(bad, str) else bad
    for calculator, count in ((indicators.causal_fractals, 5), (indicators.causal_zigzag, 12)):
        result = calculator(frame)
        assert not result[30]["valid"] and result[30]["top"] is result[30]["bottom"] is None
        assert all(not r["ready"] for r in result[31:30 + count])
        assert result[30 + count]["ready"]
        assert result[31:] == [dict(r, top=None if r["top"] is None else {**r["top"], "origin": r["top"]["origin"] + 31, "confirmation": r["top"]["confirmation"] + 31},
                                   bottom=None if r["bottom"] is None else {**r["bottom"], "origin": r["bottom"]["origin"] + 31, "confirmation": r["bottom"]["confirmation"] + 31},
                                   provisional=[{**p, "origin": p["origin"] + 31} for p in r["provisional"]])
                                for r in calculator(frame.iloc[31:])]
    rows = indicator_registry.compute_core(frame, feature_ids=IDS)
    assert all(r["value"] is None and r["state"] == "SOURCE_VALUE_MISSING" for r in rows if r["bar_index"] == 30)
    assert all(r["state"] == "SOURCE_UNAVAILABLE" for r in indicator_registry.compute_core(frame.drop(columns="low"), feature_ids=IDS))


def test_zigzag_backstep_ties_threshold_and_outside_bar_order():
    frame = pd.DataFrame({"high": [100, 110, 120, 120, 119, 118, 125, 130, 129, 128],
                          "low": [99, 109, 119, 119, 100, 89, 80, 129, 95, 94]})
    result = indicators.causal_zigzag(frame, depth=4, deviation=0.25, backstep=3)
    # Candidato de topo avança na outside 6: a mínima 80 não pode confirmar 125 nessa barra.
    assert not any(e[2] == 6 for e in event_list(result))
    assert all(e[2] - e[1] >= 3 for e in event_list(result))
    assert result[3]["provisional"][0]["origin"] == 0  # fundo inicial, empate de topo preserva origem 2
    assert event_list(result) == independent_zigzag(frame, depth=4, deviation=Fraction(1, 4), backstep=3)
    exact = pd.DataFrame({"high": [100, 120, 120, 110, 109], "low": [99, 119, 119, 90, 89]})
    expected = independent_zigzag(exact, depth=3, deviation=Fraction(1, 4), backstep=1)
    assert event_list(indicators.causal_zigzag(exact, depth=3, deviation=0.25, backstep=1)) == expected


@pytest.mark.parametrize("kwargs", [{"depth": 1}, {"backstep": 0}, {"backstep": 12}, {"deviation": 0}, {"deviation": np.nan}, {"depth": True}, {"deviation": None}, {"deviation": "1%"}])
def test_zigzag_rejects_undefined_parameters(kwargs):
    with pytest.raises(ValueError):
        indicators.causal_zigzag(trap(), **kwargs)


def test_registry_contract_and_provisional_values_are_never_strategy_signals():
    frame = trap()
    records = indicator_registry.compute_core(frame, feature_ids=IDS)
    for row in records:
        if row["feature_id"].endswith("provisional"):
            assert row["value"] is None and row["state"] != "AVAILABLE"
        elif row["state"] == "AVAILABLE":
            pivot = row["pivot_metadata"]
            assert pivot["pivot_origin_position"] < pivot["pivot_confirmation_position"] <= row["bar_index"]
    assert {d["feature_id"] for d in indicator_registry.definitions()}.issuperset(IDS)
    assert len(indicator_registry.definitions()) == 52


def source_trap(count=60):
    frame = source(count)
    prices = trap(count)
    for name in ("high", "low"):
        frame[name] = np.repeat(prices[name].to_numpy(), 5)
    frame["open"] = frame["close"] = (frame.high + frame.low) / 2
    return frame


def test_pivot_origin_confirmation_and_context_boundary(db):
    seed(db, source_trap())
    materializer.materialize(db, "TEST", "M5", feature_ids=IDS, batch_size=31)
    for prefix in (F, Z):
        ident = prefix + "top"
        history = feature_store.query_history(db, feature_id=ident)
        row = history[history.quality == "VALID"].iloc[0]
        meta = json.loads(row.metadata_json)
        origin = pd.Timestamp(meta["pivot_origin_time_utc"])
        known = pd.Timestamp(meta["pivot_known_at_utc"])
        confirmation = pd.Timestamp(meta["pivot_confirmation_time_utc"])
        assert origin < confirmation == known == row.available_at_utc
        assert meta["display_time_utc"] == origin.isoformat()
        decisions = pd.DataFrame({"decision_time": [origin, known - pd.Timedelta(nanoseconds=1), known]})
        context = indicator_context.context(db, decisions, symbol="TEST", timeframe="M5", feature_ids=[ident])
        assert context[ident + "::state"].tolist()[-1] == "AVAILABLE"
        assert all(v != "AVAILABLE" for v in context[ident + "::state"].tolist()[:2])
        assert context.iloc[1][ident + "::state"] != "AVAILABLE"
        assert context.iloc[2][ident + "::value"] == row.numeric_value
    context = indicator_context.context(db, pd.DataFrame({"decision_time": ["2026-10-01T00:00Z"]}),
                                        symbol="TEST", timeframe="M5", feature_ids=[F + "provisional", Z + "provisional"])
    assert all(context.iloc[0][ident + "::value"] is None for ident in (F + "provisional", Z + "provisional"))


def test_incremental_reopen_and_csv_reconcile_with_independent_reference(db, tmp_path):
    seed(db, source_trap(36)); materializer.materialize(db, "TEST", "M5", feature_ids=IDS, batch_size=17)
    initial = facts(db)
    decisions = pd.DataFrame({"decision_time": ["2026-09-01T10:30Z", "2026-09-01T12:00Z"]})
    prior_context = indicator_context.context(db, decisions, symbol="TEST", timeframe="M5", feature_ids=IDS)
    seed(db, source_trap(60).iloc[180:])
    appended = materializer.materialize(db, "TEST", "M5", feature_ids=IDS, batch_size=19)
    assert appended["write_from"] == 36 and appended["revised"] == 0
    final = facts(db)
    assert all(row in final for row in initial)
    pd.testing.assert_frame_equal(prior_context, indicator_context.context(db, decisions, symbol="TEST", timeframe="M5", feature_ids=IDS))
    assert materializer.materialize(db, "TEST", "M5", feature_ids=IDS)["skipped"]
    complete = DatabaseManager(db_path=str(tmp_path / "complete.sqlite")); complete.broker_utc_offset = 0
    try:
        seed(complete, source_trap(60)); materializer.materialize(complete, "TEST", "M5", feature_ids=IDS)
        assert facts(complete) == final
    finally:
        complete.close()
    db.close(); reopened = DatabaseManager(db_path=db.db_path)
    try:
        assert materializer.materialize(reopened, "TEST", "M5", feature_ids=IDS)["skipped"]
        bars = resampling.latest_bars(reopened, "TEST", "M5")
        calculated = indicator_registry.compute_core(bars, feature_ids=IDS)
        assert event_list(indicators.causal_zigzag(bars)) == independent_zigzag(bars)
        ref = independent_fractals(bars)
        assert {s: [(o, c, p) for k, o, c, p in event_list(indicators.causal_fractals(bars)) if k == s] for s in ref} == ref
        path = tmp_path / "pivôs.csv"
        DataExporter(reopened).export_indicator_values(path, symbol="TEST", timeframe="M5", as_of="2026-10-01T00:00Z", feature_ids=IDS)
        csv = pd.read_csv(path, keep_default_na=False, float_precision="round_trip")
        assert len(csv) == 60
        for ident in IDS:
            expected = [r for r in calculated if r["feature_id"] == ident]
            stored = feature_store.query_as_of(reopened, "2026-10-01T00:00Z", feature_id=ident)
            pure = np.array([r["value"] if r["value"] is not None else np.nan for r in expected])
            np.testing.assert_array_equal(pure, stored.numeric_value.to_numpy(dtype=float))
            np.testing.assert_array_equal(pure, np.array([float(v) if v != "" else np.nan for v in csv[ident + "::value"]]))
            assert csv[ident + "::state"].tolist() == [r["state"] for r in expected]
            assert [json.loads(v) for v in csv[ident + "::metadata_json"]] == [json.loads(v) for v in stored.metadata_json]
    finally:
        reopened.close()


def test_source_correction_preserves_old_decisions_and_exports_revision(db, tmp_path):
    frame = source_trap(60); seed(db, frame)
    materializer.materialize(db, "TEST", "M5", feature_ids=IDS)
    asof = pd.Timestamp("2026-09-02T00:00Z")
    before = indicator_context.projection(db, symbol="TEST", timeframe="M5", as_of=asof, feature_ids=IDS)
    decisions = pd.DataFrame({"decision_time": ["2026-09-01T10:30Z", "2026-09-01T12:00Z"]})
    old_context = indicator_context.context(db, decisions, symbol="TEST", timeframe="M5", feature_ids=IDS)
    changed = frame.iloc[[20]].copy(); changed["high"] = 150
    seed(db, changed)
    receipt = resampling.latest_bars(db, "TEST", "M5").loc[lambda x: x.revision > 1, "computed_at_utc"].max()
    materializer.materialize(db, "TEST", "M5", feature_ids=IDS)
    pd.testing.assert_frame_equal(before, indicator_context.projection(db, symbol="TEST", timeframe="M5", as_of=asof, feature_ids=IDS))
    pd.testing.assert_frame_equal(old_context, indicator_context.context(db, decisions, symbol="TEST", timeframe="M5", feature_ids=IDS))
    after = feature_store.query_as_of(db, receipt, symbol="TEST")
    revised = after[after.revision > 1]
    assert len(revised) > 0 and revised.available_at_utc.ge(receipt).all()
    assert any(json.loads(v).get("pivot_known_at_utc") == receipt.isoformat() for v in revised.metadata_json)
    path = tmp_path / "revisões.csv"
    DataExporter(db).export_indicator_values(path, symbol="TEST", timeframe="M5", as_of=receipt, feature_ids=IDS)
    csv = pd.read_csv(path, keep_default_na=False)
    for ident in IDS:
        part = after[after.feature_id == ident]
        assert csv[ident + "::source_revision"].tolist() == part.source_revision.tolist()
        assert csv[ident + "::revision"].tolist() == part.revision.tolist()
        assert [json.loads(v) for v in csv[ident + "::metadata_json"]] == [json.loads(v) for v in part.metadata_json]



@pytest.mark.parametrize("width", [0, -1, True, 1.5])
def test_fractal_rejects_undefined_width(width):
    with pytest.raises(ValueError):
        indicators.causal_fractals(trap(), width=width)


def test_empty_missing_nonpositive_and_registry_future_extension(monkeypatch):
    assert indicators.causal_fractals(trap(0)) == indicators.causal_zigzag(trap(0)) == []
    zeros = pd.DataFrame({"high": [0.] * 15, "low": [0.] * 15})
    assert all(not s["valid"] for s in indicators.causal_zigzag(zeros))
    frame = trap(70)
    whole = indicator_registry.compute_core(frame, feature_ids=IDS)
    prefix = indicator_registry.compute_core(frame.iloc[:41], feature_ids=IDS)
    assert prefix == [r for r in whole if r["bar_index"] < 41]
    def legacy_forbidden(_bars):
        raise AssertionError("Pivôs não dependem de cálculos legados")
    monkeypatch.setattr(indicator_registry, "_legacy_calculated_series", legacy_forbidden)
    assert indicator_registry.compute_core(frame, feature_ids=IDS) == whole



def test_known_ineligible_bar_breaks_pivot_window_and_preserves_asof(db, monkeypatch):
    seed(db, source_trap(50))
    materializer.materialize(db, "TEST", "M5", feature_ids=IDS)
    before = feature_store.query_as_of(db, "2026-09-02T00:00Z", symbol="TEST")
    raw = resampling.latest_bars(db, "TEST", "M5", closed_only=False)
    cutoff = pd.Timestamp("2026-10-20T12:00Z")
    raw.loc[30, "quality_status"] = "PARTIAL"
    raw.loc[30, "revision"] = 2
    raw.loc[30, "computed_at_utc"] = cutoff
    closed = raw.drop(index=30).reset_index(drop=True)
    monkeypatch.setattr(resampling, "latest_source_sequence", lambda *a, **k: 2)
    monkeypatch.setattr(resampling, "latest_bars", lambda *a, closed_only=True, **k: closed.copy() if closed_only else raw.copy())
    materializer.materialize(db, "TEST", "M5", feature_ids=IDS)
    pd.testing.assert_frame_equal(before, feature_store.query_as_of(db, cutoff - pd.Timedelta(nanoseconds=1), symbol="TEST"))
    after = feature_store.query_as_of(db, cutoff, symbol="TEST")
    for prefix, warmup in ((F, 5), (Z, 12)):
        first = after[(after.feature_id == prefix + "top") & (after.observation_time == raw.iloc[31].bar_start_utc)].iloc[0]
        assert first.quality == "UNAVAILABLE" and json.loads(first.metadata_json)["state"] == "WARMUP"
        for offset in range(1, warmup):
            row = after[(after.feature_id == prefix + "top") & (after.observation_time == raw.iloc[30 + offset].bar_start_utc)].iloc[0]
            assert row.quality == "UNAVAILABLE"



def test_new_known_ineligible_bar_invalidates_unchanged_eligible_prefix(db, monkeypatch):
    seed(db, source_trap(50))
    raw = resampling.latest_bars(db, "TEST", "M5", closed_only=False)
    gap = raw.drop(index=30).reset_index(drop=True)
    monkeypatch.setattr(resampling, "latest_source_sequence", lambda *a, **k: 1)
    monkeypatch.setattr(resampling, "latest_bars", lambda *a, **k: gap.copy())
    materializer.materialize(db, "TEST", "M5", feature_ids=IDS)
    before = feature_store.query_as_of(db, "2026-09-02T00:00Z", symbol="TEST")
    raw.loc[30, "quality_status"] = "PARTIAL"
    raw.loc[30, "computed_at_utc"] = pd.Timestamp("2026-10-20T12:00Z")
    raw.loc[30, "revision"] = 1
    monkeypatch.setattr(resampling, "latest_source_sequence", lambda *a, **k: 2)
    monkeypatch.setattr(resampling, "latest_bars", lambda *a, closed_only=True, **k: gap.copy() if closed_only else raw.copy())
    result = materializer.materialize(db, "TEST", "M5", feature_ids=IDS)
    assert result["write_from"] == 0 and result["revised"] > 0
    pd.testing.assert_frame_equal(before, feature_store.query_as_of(db, "2026-10-20T11:59Z", symbol="TEST"))
    after = feature_store.query_as_of(db, "2026-10-21T00:00Z", symbol="TEST")
    assert json.loads(after[(after.feature_id == F+"top") & (after.observation_time == raw.iloc[31].bar_start_utc)].iloc[0].metadata_json)["state"] == "WARMUP"
