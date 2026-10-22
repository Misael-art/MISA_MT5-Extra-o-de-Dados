"""BD02: bancada compacta, referências independentes e orçamento de artefatos."""
import math
import time
from pathlib import Path

import pytest

import bench
import bench_reference as ref
from mt5_extracao.analytics import indicator_registry

FIXTURES = Path(__file__).resolve().parent / "fixtures"
PERSISTENT_BUDGET = 100 * 1024 * 1024
TEMPORARY_BUDGET = 250 * 1024 * 1024


def _registry(frame, feature_id):
    values = [None] * len(frame)
    for row in indicator_registry.iter_core(frame, feature_ids=[feature_id]):
        value = row["value"]
        values[row["bar_index"]] = None if value is None or (isinstance(value, float) and math.isnan(value)) else value
    return values


def _close(expected, actual, rel):
    assert len(expected) == len(actual)
    for index, (want, got) in enumerate(zip(expected, actual)):
        if want is None:
            assert got is None, index
        else:
            assert got == pytest.approx(want, rel=rel, abs=1e-12), index


def test_scenarios_are_deterministic_synthetic_and_cover_warmup():
    first, second = bench.scenarios(), bench.scenarios()
    max_warmup = max(spec["warmup_count"] for spec in indicator_registry.definitions())
    for name, frame in first.items():
        assert frame.attrs["synthetic"] is True and frame.attrs["origin"] == bench.ORIGIN, name
        assert bench.fingerprint(frame) == bench.fingerprint(second[name]), name
        if not name.startswith("volume_"):
            assert len(frame) >= 2 * max_warmup, name


def test_two_bases_share_the_symbol_but_not_the_identity_or_data():
    a, b = bench.two_bases_same_symbol()
    assert set(a["symbol"]) == set(b["symbol"]) == {"EURUSD"}
    assert a.attrs["base_id"] != b.attrs["base_id"]
    assert bench.fingerprint(a)[0] != bench.fingerprint(b)[0]


def test_volume_states_are_distinct_and_never_zero_by_default():
    states = bench.volume_states()
    assert (states["zero_known"]["real_volume"] == 0).all()
    assert states["absent"]["real_volume"].isna().all() and (states["absent"]["real_volume_known"] == 0).all()
    partial = states["partial_unknown"]
    assert partial["real_volume"].isna().sum() == 1 and (partial["real_volume_known"] == 0).sum() == 1


def test_defects_are_visible_in_the_frames():
    s = bench.scenarios()
    assert len(s["gap"]) == len(s["trend"]) - 5
    assert s["duplicate"]["bar_start_utc"].duplicated().sum() == 1
    assert not s["out_of_order"]["bar_start_utc"].is_monotonic_increasing
    assert set(s["revision"]["revision"]) == {1, 2}
    assert s["missing_close"]["close"].isna().sum() == 1
    dates_per_session = s["overnight"].groupby("session_id")["bar_start_utc"].agg(lambda t: t.dt.date.nunique())
    assert (dates_per_session == 2).all()  # cada sessão atravessa a meia-noite UTC


@pytest.mark.parametrize("scenario", ["trend", "lateral", "flat", "random_walk", "scale_0.0001", "scale_100000"])
def test_registry_matches_independent_references(scenario):
    frame = bench.scenarios()[scenario]
    close, high, low = (frame[c].tolist() for c in ("close", "high", "low"))
    # Tolerâncias: aritmética de ponto flutuante em ordens distintas de soma (1e-9 relativo).
    _close(ref.returns(close, 1), _registry(frame, "price.return_1"), 1e-9)
    _close(ref.returns(close, 20), _registry(frame, "price.return_20"), 1e-9)
    _close(ref.efficiency_ratio(close, 20), _registry(frame, "trend.efficiency_ratio_20"), 1e-9)
    _close(ref.bollinger_width(close, 20, 2.0), _registry(frame, "volatility.bb_20_width"), 1e-9)
    _close(ref.donchian_upper(high, 20, 0), _registry(frame, "channel.donchian_20_current_upper"), 1e-12)
    _close(ref.donchian_upper(high, 20, 1), _registry(frame, "channel.donchian_20_previous_upper"), 1e-12)
    _close(ref.wilder(ref.true_range(high, low, close), 14, "first"), _registry(frame, "volatility.atr_14"), 1e-9)
    _close(ref.rsi(close, 14, "first"), _registry(frame, "momentum.rsi_14"), 1e-9)


def test_wilder_seed_divergence_is_measured_not_hidden():
    """Achado BD02→BD08a: o registry semeia RMA na 1ª observação; Wilder clássico usa SMA."""
    frame = bench.random_walk()
    tr = ref.true_range(*(frame[c].tolist() for c in ("high", "low", "close")))
    first, sma = ref.wilder(tr, 14, "first"), ref.wilder(tr, 14, "sma")
    registry = _registry(frame, "volatility.atr_14")
    assert registry[13] == pytest.approx(first[13]) and registry[13] != pytest.approx(sma[13], rel=1e-6)
    assert registry[-1] == pytest.approx(sma[-1], rel=1e-6)  # converge após ~200 barras


def test_artifact_budget():
    persistent = sum(path.stat().st_size for path in FIXTURES.rglob("*") if path.is_file())
    started = time.perf_counter()
    temporary = sum(bench.fingerprint(frame)[1] for frame in bench.scenarios().values())
    elapsed = time.perf_counter() - started
    assert persistent <= PERSISTENT_BUDGET
    assert temporary <= TEMPORARY_BUDGET
    assert elapsed < 30
