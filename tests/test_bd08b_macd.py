"""BD08b/T19.3: MACD first-seeded, dependências independentes e consumo causal."""
import json
import math
from decimal import Decimal, localcontext

import numpy as np
import pandas as pd
import pytest

import bench
from mt5_extracao import cli, sdk
from mt5_extracao.analytics import (datasets, feature_store, indicator_contracts, indicator_materializer,
                                   indicator_registry, query_service, resampling)
from mt5_extracao.database_manager import DatabaseManager
from tests.test_indicator_materialization import seed
from tests.test_bd08b_wilder import source

IDS = ("momentum.macd_line_12_26_9", "momentum.macd_signal_12_26_9",
       "momentum.macd_histogram_12_26_9", "momentum.macd_histogram_atr_14")
NOW = "2100-01-01T00:00Z"


def _number(value):
    try:
        value = float(value)
        return Decimal.from_float(value) if math.isfinite(value) else None
    except (TypeError, ValueError):
        return None


def _expected(frame, reset_before=None):
    """Oráculo decimal60; nenhuma chamada a EWM, calculador ou produtor.

    MACD usa apenas fechamentos finitos; ATR reinicia por seu próprio OHLC.
    O sinal começa na primeira linha visível (26ª barra), não na primeira barra.
    """
    result = {ident: [] for ident in IDS}
    atr_values = []
    fast = slow = signal = atr = previous = None
    count = atr_count = 0
    resets = [False] * len(frame) if reset_before is None else list(reset_before)
    with localcontext() as context:
        context.prec = 60
        for i, row in enumerate(frame.to_dict("records")):
            if resets[i]:
                fast = slow = signal = atr = previous = None
                count = atr_count = 0
            close, high, low = (_number(row.get(key)) for key in ("close", "high", "low"))
            line = visible_signal = histogram = visible_atr = None
            if close is None:
                fast = slow = signal = None
                count = 0
            else:
                count += 1
                fast = close if fast is None else fast + (close-fast) * Decimal(2)/13
                slow = close if slow is None else slow + (close-slow) * Decimal(2)/27
                if count >= 26:
                    line = fast-slow
                    signal = line if signal is None else signal + (line-signal) / 5
                    if count >= 34:
                        visible_signal, histogram = signal, line-signal
            if close is None or high is None or low is None or not low <= close <= high:
                atr = previous = None
                atr_count = 0
            else:
                tr = high-low if previous is None else max(high-low, abs(high-previous), abs(low-previous))
                atr = tr if atr is None else (13*atr+tr)/14
                previous = close
                atr_count += 1
                if atr_count >= 14:
                    visible_atr = atr
            for ident, value in zip(IDS[:3], (line, visible_signal, histogram)):
                result[ident].append(value)
            result[IDS[3]].append(histogram/visible_atr if histogram is not None and
                                  visible_atr is not None and visible_atr > 0 else None)
            atr_values.append(visible_atr)
    return result, atr_values


def _budget(frame, ident, atr):
    scale = max((abs(float(v)) for v in frame.get("close", []) if _number(v) is not None), default=0)
    value = 64*np.finfo(float).eps*scale
    return value / float(atr) if ident == IDS[3] else value


def _assert_values(frame, reset_before=None):
    expected, atr = _expected(frame, reset_before)
    rows = list(indicator_registry.iter_core(frame, feature_ids=IDS))
    assert len(rows) == len(frame)*len(IDS)
    positions = {label: i for i, label in enumerate(frame.index)}
    # Cancelamento das EMAs pode deixar histograma próximo de zero. O orçamento
    # absoluto depende da escala da entrada (64 eps float64); não promete erro
    # relativo uniforme. A razão propaga esse orçamento pelo denominador ATR.
    for row in rows:
        i = positions[row["bar_index"]]
        want = expected[row["feature_id"]][i]
        if want is None:
            assert row["value"] is None and row["state"] != "AVAILABLE", row
        else:
            tolerance = _budget(frame, row["feature_id"], atr[i])
            assert row["value"] == pytest.approx(float(want), rel=1e-12, abs=tolerance), row
            assert row["state"] == "AVAILABLE"
    return rows


@pytest.mark.parametrize("scenario", ["trend", "lateral", "flat", "random_walk", "scale_0.0001", "scale_100000"])
def test_macd_matches_independent_decimal_reference(scenario):
    _assert_values(bench.scenarios()[scenario])


@pytest.mark.parametrize("bad", [np.nan, np.inf, -np.inf, "ausente"])
def test_missing_close_restarts_both_dependencies_and_full_warmup(bad):
    frame = bench.random_walk(n=85)
    frame["close"] = frame.close.astype(object)
    frame.loc[40, "close"] = bad
    rows = _assert_values(frame)
    keyed = {(r["feature_id"], r["bar_index"]): r for r in rows}
    for ident in IDS:
        assert keyed[ident, 40]["state"] == "SOURCE_VALUE_MISSING"
        assert keyed[ident, 41]["state"] == "WARMUP"
    assert keyed[IDS[0], 66]["state"] == "AVAILABLE"
    assert keyed[IDS[1], 73]["state"] == "WARMUP"
    assert keyed[IDS[1], 74]["state"] == "AVAILABLE"


@pytest.mark.parametrize("column,bad", [("high", np.nan), ("low", np.inf), ("low", 1e6)])
def test_invalid_ohlc_resets_atr_but_does_not_reset_close_only_macd(column, bad):
    frame = bench.random_walk(n=75)
    frame.loc[40, column] = bad
    rows = _assert_values(frame)
    keyed = {(r["feature_id"], r["bar_index"]): r for r in rows}
    assert keyed[IDS[3], 40]["state"] == "SOURCE_VALUE_MISSING"
    assert keyed[IDS[3], 41]["state"] == "WARMUP"
    assert keyed[IDS[3], 54]["state"] == "AVAILABLE"
    assert all(keyed[ident, 41]["state"] == "AVAILABLE" for ident in IDS[:3])


@pytest.mark.parametrize("missing", ["close", "high", "low"])
def test_absent_columns_do_not_invalidate_unrelated_macd_outputs(missing):
    frame = bench.random_walk(n=60).drop(columns=missing)
    rows = _assert_values(frame)
    for row in rows:
        if missing == "close" or row["feature_id"] == IDS[3]:
            assert row["state"] == "SOURCE_UNAVAILABLE" and missing in row["reason"]
        elif row["bar_index"] >= 33:
            assert row["state"] == "AVAILABLE"


def test_first_seed_ema_signal_and_unshifted_units_are_explicit():
    frame = bench.random_walk(n=50)
    rows = _assert_values(frame)
    by_id = {ident: [r for r in rows if r["feature_id"] == ident] for ident in IDS}
    assert next(r["bar_index"] for r in by_id[IDS[0]] if r["state"] == "AVAILABLE") == 25
    assert all(next(r["bar_index"] for r in by_id[ident] if r["state"] == "AVAILABLE") == 33 for ident in IDS[1:])
    expected, _ = _expected(frame)
    # Semente SMA e sinal SMA são variantes diferentes, não equivalências nativas.
    fast_sma = sum(frame.close.iloc[:12])/12
    for value in frame.close.iloc[12:26]:
        fast_sma += (value-fast_sma)*2/13
    sma_seed_line = fast_sma - sum(frame.close.iloc[:26])/26
    assert float(expected[IDS[0]][25]) != pytest.approx(sma_seed_line, rel=1e-6)
    sma_signal = sum(float(value) for value in expected[IDS[0]][25:34])/9
    assert float(expected[IDS[1]][33]) != pytest.approx(sma_signal, rel=1e-6)
    for ident in IDS:
        spec = indicator_contracts.contract(ident)
        assert spec["algorithm_version"] == "1.1"
        assert spec["parameters"]["seed"] == "first_observation"
        assert spec["parameters"]["signal_method"] == "EMA"
        assert spec["parameters"]["missing_policy"] == "reset_and_reseed_per_dependency"
        assert "test_bd08b_macd.py" in spec["reference"]
        assert not spec["parameters"].get("plot_shift_bars", 0)
    assert indicator_contracts.contract(IDS[0])["unit"] == "price_units"
    assert indicator_contracts.contract(IDS[3])["unit"] == "atr_units"


@pytest.mark.parametrize("price", [0.0, -100.0, 100.0])
def test_zero_is_valid_macd_but_zero_atr_is_invalid_denominator(price):
    frame = bench.flat(n=50, price=price)
    rows = _assert_values(frame)
    last = {r["feature_id"]: r for r in rows if r["bar_index"] == 49}
    assert all(last[ident]["value"] == 0.0 and last[ident]["state"] == "AVAILABLE" for ident in IDS[:3])
    assert last[IDS[3]]["value"] is None and last[IDS[3]]["state"] == "INVALID_DENOMINATOR"


def test_known_ineligible_reset_calendar_gap_and_temporal_index():
    frame = bench.random_walk(n=90)
    frame["_macd_reset_before"] = frame["_wilder_reset_before"] = False
    frame.loc[40, ["_macd_reset_before", "_wilder_reset_before"]] = True
    frame.index = pd.date_range("2026-09-01", periods=90, freq="5min")
    _assert_values(frame, [i == 40 for i in range(90)])
    frame = frame.drop(columns=["_macd_reset_before", "_wilder_reset_before"]).drop(index=frame.index[39])
    _assert_values(frame)  # lacuna sem observação não cria candle nem reset.


def test_future_append_preserves_exact_prefix_and_reset_matches_new_series():
    frame = bench.random_walk(n=100)
    frame.loc[40, "close"] = np.nan
    short = list(indicator_registry.iter_core(frame.iloc[:80], feature_ids=IDS))
    long = list(indicator_registry.iter_core(frame, feature_ids=IDS))
    assert short == [r for r in long if r["bar_index"] < 80]
    after = list(indicator_registry.iter_core(frame.iloc[41:], feature_ids=IDS))
    assert after == [r for r in long if r["bar_index"] >= 41]

def _materialize(db, **kw):
    return indicator_materializer.materialize(db, "TEST", "M5", feature_ids=IDS, **kw)


def _select(db, **kw):
    return query_service.series(db, symbol="TEST", timeframe="M5", feature_ids=list(IDS),
                                as_of=NOW, basis="knowledge", page_size=5000, **kw)


def _assert_stored_reference(db):
    bars = resampling.latest_bars(db, "TEST", "M5")
    env = _select(db)
    assert env["status"] == "ok" and len(env["values"]) == len(bars) * len(IDS)
    expected, _atr = _expected(bars)
    indices = {query_service._iso(t): i for i, t in enumerate(bars.bar_start_utc)}
    for row in env["values"]:
        want = expected[row["feature_id"]][indices[row["observation_time"]]]
        if want is None:
            assert row["value"] is None and row["quality"] != "VALID"
        else:
            assert row["value"] == pytest.approx(float(want), rel=1e-12,
                                                       abs=_budget(bars, row["feature_id"], _atr[indices[row["observation_time"]]]))
            assert row["quality"] == "VALID"
    assert env["knowledge_horizon"] and env["reasons"]
    json.dumps(env, allow_nan=False)
    return env


def exercise_store(db):
    """Mesmo percurso SQLite/PG; fixture compacta, lotes pequenos, append e revisão."""
    frame = source()
    seed(db, frame.iloc[:250])
    datasets.identity(db)
    _materialize(db, batch_size=7)
    before = _assert_stored_reference(db)
    seed(db, frame)
    result = _materialize(db, batch_size=5000)
    after = _assert_stored_reference(db)
    assert result["revised"] == 0 and result["write_from"] == 50
    keyed = lambda env: {(r["feature_id"], r["observation_time"]): r for r in env["values"]}
    assert all(keyed(after)[k] == v for k, v in keyed(before).items())
    assert _materialize(db)["skipped"]
    cut = after["knowledge_horizon"]
    frozen = query_service.series(db, symbol="TEST", timeframe="M5", feature_ids=list(IDS),
                                  as_of=cut, basis="knowledge", page_size=5000)
    first = query_service.series(db, symbol="TEST", timeframe="M5", feature_ids=list(IDS),
                                 as_of=NOW, basis="knowledge", page_size=17)
    correction = frame.iloc[[40 * 5 + 4]].copy()
    correction["close"] += 5
    correction["high"] = correction["close"] + .5
    seed(db, correction)
    assert _materialize(db)["revised"] > 0
    latest = _assert_stored_reference(db)
    assert latest["values"] != after["values"]
    covered = query_service.series(db, symbol="TEST", timeframe="M5", feature_ids=list(IDS),
                                   as_of=cut, basis="knowledge", page_size=5000)
    assert covered["values"] == frozen["values"]
    values, page = [], first
    while True:
        assert page["status"] == "ok", page["errors"]
        assert page["snapshot"] == first["snapshot"] and page["knowledge_horizon"] == first["knowledge_horizon"]
        values += page["values"]
        cursor = page["page"]["next_cursor"]
        if not cursor:
            break
        page = query_service.series(db, symbol="TEST", timeframe="M5", feature_ids=list(IDS), as_of=NOW,
                                     basis="knowledge", page_size=17, cursor=cursor)
    assert values == after["values"]
    return latest


def test_store_append_revision_and_covered_cursor(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "macd.sqlite"), broker_utc_offset=0)
    try:
        exercise_store(db)
    finally:
        db.close()


def exercise_ineligible_source(db, monkeypatch):
    seed(db, source(n=70))
    _materialize(db)
    raw = resampling.latest_bars(db, "TEST", "M5", closed_only=False)
    cutoff = raw.computed_at_utc.max() + pd.Timedelta(seconds=1)
    before = feature_store.query_as_of(db, cutoff - pd.Timedelta(nanoseconds=1), symbol="TEST")
    raw.loc[30, ["quality_status", "revision", "computed_at_utc"]] = ["PARTIAL", 2, cutoff]
    closed = raw.drop(index=30).reset_index(drop=True)
    monkeypatch.setattr(resampling, "latest_source_sequence", lambda *a, **k: 2)
    monkeypatch.setattr(resampling, "latest_bars", lambda *a, closed_only=True, **k:
                        closed.copy() if closed_only else raw.copy())
    result = _materialize(db, batch_size=7)
    assert result["write_from"] == 0 and result["revised"] > 0
    pd.testing.assert_frame_equal(before, feature_store.query_as_of(db, cutoff - pd.Timedelta(nanoseconds=1), symbol="TEST"))
    reference = raw.copy()
    reference.loc[30, ["high", "low", "close"]] = np.nan
    expected, _atr = _expected(reference)
    for ident in IDS:
        latest = feature_store.query_as_of(db, cutoff, symbol="TEST", feature_id=ident).set_index("observation_time")
        invalid = latest.loc[raw.iloc[30].bar_start_utc]
        assert invalid.quality == "INVALIDATED" and pd.isna(invalid.numeric_value)
        for i in range(31, len(raw)):
            record = latest.loc[raw.iloc[i].bar_start_utc]
            assert record.available_at_utc >= cutoff
            want = expected[ident][i]
            if want is None:
                assert pd.isna(record.numeric_value) and record.quality == "UNAVAILABLE"
            else:
                assert record.numeric_value == pytest.approx(float(want), rel=1e-12, abs=_budget(reference, ident, _atr[i]))
                assert record.quality == "VALID"


def test_known_ineligible_source_revises_warmup_and_keeps_historical_facts(tmp_path, monkeypatch):
    db = DatabaseManager(db_path=str(tmp_path / "ineligible.sqlite"), broker_utc_offset=0)
    try:
        exercise_ineligible_source(db, monkeypatch)
    finally:
        db.close()


def test_cancelled_batches_resume_without_reseeding_or_duplicate_revisions(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "resume.sqlite"), broker_utc_offset=0)
    try:
        seed(db, source(n=45))
        calls = 0

        def cancel():
            nonlocal calls
            calls += 1
            return calls >= 4

        with pytest.raises(indicator_materializer.Cancelled):
            _materialize(db, batch_size=7, cancel_check=cancel)
        confirmed = feature_store.query_history(db, symbol="TEST")
        assert len(confirmed) == 7
        assert _materialize(db, batch_size=11)["job"]["status"] == "COMPLETE"
        _assert_stored_reference(db)
        history = feature_store.query_history(db, symbol="TEST")
        assert len(history) == 45 * len(IDS) and history.revision.eq(1).all()
    finally:
        db.close()



def test_sdk_and_cli_use_the_same_horizon_and_definitions(tmp_path, capsys):
    path = tmp_path / "macd #1.sqlite"
    db = DatabaseManager(db_path=str(path), broker_utc_offset=0)
    try:
        seed(db, source(n=45))
        _materialize(db)
        expected = _assert_stored_reference(db)
        cut = expected["knowledge_horizon"]
        direct = query_service.series(db, symbol="TEST", timeframe="M5", feature_ids=list(IDS),
                                      as_of=cut, basis="knowledge", page_size=5000)
        via_sdk = sdk.query_series(db, symbol="TEST", timeframe="M5", feature_ids=list(IDS),
                                   as_of=cut, basis="knowledge", page_size=5000)
        assert via_sdk["values"] == direct["values"] and via_sdk["definitions"] == direct["definitions"]
        config = tmp_path / "cli.ini"
        config.write_text(f"[DATABASE]\ntype = sqlite\npath = {path}\n[APP]\nbroker_utc_offset = 0\n", encoding="utf-8")
        args = ["--config", str(config), "--json", "analytics", "features", "series", "--symbol", "TEST",
                "--tf", "M5", "--as-of", cut, "--basis", "knowledge", "--page-size", "5000"]
        for ident in IDS:
            args += ["--feature", ident]
        assert cli.main(args) == 0
        envelope = json.loads(capsys.readouterr().out)["data"]["envelope"]
        assert envelope["values"] == direct["values"]
        assert envelope["knowledge_horizon"] == direct["knowledge_horizon"]
        assert envelope["base"] == direct["base"]
    finally:
        db.close()


def exercise_version_migration(db):
    for ident in IDS:
        params = {"fast": 12, "slow": 26, "signal": 9}
        if ident == IDS[3]:
            params["atr_period"] = 14
        feature_store.register_definition(db, ident, "Legacy MACD", "1.0", params, [],
                                          unit="legacy-unit", value_type="NUMBER")
        feature_store.save_value(db, "TEST", "M5", ident, "1.0", "2026-09-01T09:00Z", 50.0,
                                  available_at="2026-09-01T09:05Z", observed_at="2026-09-01T09:05Z")
    previous = feature_store.query_history(db, symbol="TEST")
    definitions_before = feature_store.list_definitions(db)
    indicator_registry.register_core(db, "M5")
    indicator_registry.register_core(db, "M5")
    definitions = {(r["feature_id"], r["algorithm_version"]): r for r in feature_store.list_definitions(db)}
    for row in definitions_before:
        assert definitions[row["feature_id"], "1.0"] == row
    for ident in IDS:
        assert definitions[ident, "1.1"]["parameters"]["seed"] == "first_observation"
        assert definitions[ident, "1.1"]["parameters"]["signal_method"] == "EMA"
    assert indicator_registry.get_definition("volatility.atr_14")["algorithm_version"] == "1.1"
    assert indicator_registry.get_definition("price.return_1")["algorithm_version"] == "1.0"
    pd.testing.assert_frame_equal(previous, feature_store.query_history(db, symbol="TEST"))
    seed(db, source(n=45))
    _materialize(db)
    assert all(indicator_materializer.algorithm_version(i).startswith("1.1+") for i in IDS)
    old = query_service.series(db, symbol="TEST", timeframe="M5", feature_ids=list(IDS), as_of=NOW,
                               basis="knowledge", algorithm_version="1.0")
    assert old["status"] == "ok" and len(old["values"]) == len(IDS)
    assert all(row["value"] == 50.0 and row["revision"] == 1 for row in old["values"])
    assert all(d["versions"]["1.0"]["unit"] == "legacy-unit" for d in old["definitions"].values())


def test_direct_registry_migrates_without_replacing_legacy_definitions_or_values(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "legacy-macd.sqlite"), broker_utc_offset=0)
    try:
        exercise_version_migration(db)
    finally:
        db.close()
