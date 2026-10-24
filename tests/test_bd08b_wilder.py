"""BD08b/T19.3: RSI/ATR first-seeded, ausência, prefixo e consumo canônico."""
import json
import math

import numpy as np
import pandas as pd
import pytest

import bench
import bench_reference as ref
from mt5_extracao import cli, sdk
from mt5_extracao.analytics import (datasets, feature_store, indicator_contracts, indicator_materializer,
                                   indicator_registry, query_service, resampling)
from mt5_extracao.database_manager import DatabaseManager
from tests.test_indicator_materialization import seed

IDS = ("momentum.rsi_14", "momentum.rsi_14_slope_3", "volatility.atr_14", "volatility.atr_pct_14")
NOW = "2100-01-01T00:00Z"


def _expected(frame, reset_before=None):
    """Oráculo por recorrência escalar, sem pandas EWM nem funções do produto.

    A semente é a PRIMEIRA observação, não SMA. Ausência interrompe o segmento;
    a primeira TR após interrupção usa high-low, sem fechamento anterior conhecido.
    """
    result = {ident: [] for ident in IDS}
    previous = gain = loss = atr = atr_previous = None
    rsi_count = atr_count = 0
    resets = [False] * len(frame) if reset_before is None else reset_before
    for i, (h, l, c) in enumerate(zip(frame.high, frame.low, frame.close)):
        if resets[i]:
            previous = gain = loss = atr = atr_previous = None
            rsi_count = atr_count = 0
        rsi_value = atr_value = None
        if not math.isfinite(c):
            previous = gain = loss = None
            rsi_count = 0
        else:
            if previous is not None:
                delta = c - previous
                up, down = max(delta, 0.0), max(-delta, 0.0)
                gain = up if gain is None else (13 * gain + up) / 14
                loss = down if loss is None else (13 * loss + down) / 14
                rsi_count += 1
                if rsi_count >= 14:
                    rsi_value = 50.0 if gain == loss == 0 else 100.0 if loss == 0 else 100 - 100 / (1 + gain / loss)
            previous = c
        if not all(math.isfinite(v) for v in (h, l, c)) or not l <= c <= h:
            atr = atr_previous = None
            atr_count = 0
        else:
            tr = h - l if atr_previous is None else max(h - l, abs(h - atr_previous), abs(l - atr_previous))
            atr = tr if atr is None else (13 * atr + tr) / 14
            atr_previous = c
            atr_count += 1
            if atr_count >= 14:
                atr_value = atr
        result[IDS[0]].append(rsi_value)
        history = result[IDS[0]]
        slope = rsi_value - history[i - 3] if i >= 3 and all(v is not None for v in history[i - 3:i + 1]) else None
        result[IDS[1]].append(slope)
        result[IDS[2]].append(atr_value)
        result[IDS[3]].append(atr_value / c if atr_value is not None and c > 0 else None)
    return result


def _assert_values(frame, reset_before=None):
    expected = _expected(frame, reset_before)
    rows = list(indicator_registry.iter_core(frame, feature_ids=IDS))
    assert len(rows) == len(frame) * len(IDS)
    positions = {label: i for i, label in enumerate(frame.index)}
    for row in rows:
        want = expected[row["feature_id"]][positions[row["bar_index"]]]
        if want is None:
            assert row["value"] is None and row["state"] != "AVAILABLE", row
        else:
            # EWM vetorizado e recorrência (13*s+x)/14 arredondam em ordens distintas.
            assert row["value"] == pytest.approx(want, rel=1e-11, abs=1e-11), row
            assert row["state"] == "AVAILABLE"
    return rows


@pytest.mark.parametrize("scenario", ["trend", "lateral", "flat", "random_walk", "scale_0.0001", "scale_100000"])
def test_wilder_matches_independent_scalar_reference(scenario):
    _assert_values(bench.scenarios()[scenario])


@pytest.mark.parametrize("column,bad", [("close", np.nan), ("close", np.inf), ("high", np.inf),
                                      ("low", 1e6), ("close", 1e6)])
def test_missing_or_invalid_source_reseeds_instead_of_becoming_available(column, bad):
    frame = bench.random_walk(n=65)
    frame.loc[25, column] = bad
    rows = _assert_values(frame)
    at_gap = {r["feature_id"]: r for r in rows if r["bar_index"] == 25}
    assert at_gap[IDS[2]]["state"] == "SOURCE_VALUE_MISSING"
    assert at_gap[IDS[2]]["reason"]
    assert all(r["value"] is None for r in rows if r["feature_id"] == IDS[2] and 26 <= r["bar_index"] < 39)


def test_known_ineligible_bar_resets_both_families_without_synthesizing_candles():
    frame = bench.random_walk(n=65)
    frame["_wilder_reset_before"] = False
    frame.loc[26, "_wilder_reset_before"] = True
    _assert_values(frame, frame._wilder_reset_before.tolist())


def test_seed_is_explicit_and_not_confused_with_sma_or_mt5():
    frame = bench.random_walk()
    tr = ref.true_range(frame.high.tolist(), frame.low.tolist(), frame.close.tolist())
    values = [r["value"] for r in indicator_registry.iter_core(frame, feature_ids=[IDS[2]])]
    assert values[13] == pytest.approx(ref.wilder(tr, 14, "first")[13])
    assert values[13] != pytest.approx(ref.wilder(tr, 14, "sma")[13], rel=1e-6)
    for ident in IDS:
        spec = indicator_contracts.contract(ident)
        assert spec["parameters"]["seed"] == "first_observation"
        assert spec["parameters"]["missing_policy"] == "reset_and_reseed"
        assert "test_bd08b_wilder.py" in spec["reference"]


@pytest.mark.parametrize("missing", ["close", "high", "low"])
def test_absent_column_is_reported_without_breaking_other_families(missing):
    frame = bench.random_walk().drop(columns=missing)
    rows = list(indicator_registry.iter_core(frame, feature_ids=IDS))
    for row in rows:
        if missing == "close" or row["feature_id"].startswith("volatility."):
            assert row["state"] == "SOURCE_UNAVAILABLE" and row["value"] is None
            assert missing in row["reason"]
    if missing != "close":
        assert any(r["state"] == "AVAILABLE" for r in rows if r["feature_id"] == IDS[0])


def test_flat_zero_and_invalid_denominator_are_distinct():
    frame = bench.flat(n=40)
    rows = _assert_values(frame)
    last = {r["feature_id"]: r for r in rows if r["bar_index"] == 39}
    assert [last[i]["value"] for i in IDS] == [50.0, 0.0, 0.0, 0.0]
    zero = bench.flat(n=40, price=0.0)
    rows = _assert_values(zero)
    pct = [r for r in rows if r["feature_id"] == IDS[3]][-1]
    assert pct["state"] == "INVALID_DENOMINATOR" and pct["value"] is None


def test_future_append_keeps_every_prefix_including_missing_segments():
    frame = bench.with_missing(bench.random_walk(n=90), 30)
    short = list(indicator_registry.iter_core(frame.iloc[:65], feature_ids=IDS))
    long = list(indicator_registry.iter_core(frame, feature_ids=IDS))
    assert short == [row for row in long if row["bar_index"] < 65]


def source(n=90):
    """450 M1 sintéticas; fonte reutilizada sem download nem histórico DEMO."""
    close = np.repeat(bench.random_walk(n=n).close.to_numpy(), 5)
    previous = np.r_[close[0], close[:-1]]
    return pd.DataFrame(dict(time=pd.date_range("2026-09-01 09:00", periods=len(close), freq="min"),
                             open=previous, high=np.maximum(previous, close) + .5,
                             low=np.minimum(previous, close) - .5, close=close,
                             tick_volume=10, real_volume=0, spread=0))


def _materialize(db, **kw):
    return indicator_materializer.materialize(db, "TEST", "M5", feature_ids=IDS, **kw)


def _select(db, **kw):
    return query_service.series(db, symbol="TEST", timeframe="M5", feature_ids=list(IDS),
                                as_of=NOW, basis="knowledge", page_size=5000, **kw)


def _assert_stored_reference(db):
    bars = resampling.latest_bars(db, "TEST", "M5")
    env = _select(db)
    assert env["status"] == "ok" and len(env["values"]) == len(bars) * len(IDS)
    expected = _expected(bars)
    indices = {query_service._iso(t): i for i, t in enumerate(bars.bar_start_utc)}
    for row in env["values"]:
        want = expected[row["feature_id"]][indices[row["observation_time"]]]
        if want is None:
            assert row["value"] is None and row["quality"] != "VALID"
        else:
            assert row["value"] == pytest.approx(want, rel=1e-11, abs=1e-11)
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
    db = DatabaseManager(db_path=str(tmp_path / "wilder.sqlite"), broker_utc_offset=0)
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
    expected = _expected(reference)
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
                assert record.numeric_value == pytest.approx(want, rel=1e-11, abs=1e-11)
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


def exercise_version_migration(db):
    """A API direta register_core também precisa distinguir o contrato anterior."""
    old_parameters = {IDS[0]: {"period": 14, "method": "Wilder", "source": "close"},
                      IDS[1]: {"period": 14, "slope_bars": 3},
                      IDS[2]: {"period": 14, "method": "Wilder"}, IDS[3]: {"period": 14}}
    for ident in IDS:
        feature_store.register_definition(db, ident, "Legacy first seed", "1.0", old_parameters[ident], [],
                                          unit="legacy-unit", value_type="NUMBER")
        feature_store.save_value(db, "TEST", "M5", ident, "1.0", "2026-09-01T09:00Z", 50.0,
                                  available_at="2026-09-01T09:05Z", observed_at="2026-09-01T09:05Z")
    previous = feature_store.query_history(db, symbol="TEST")
    indicator_registry.register_core(db, "M5")
    indicator_registry.register_core(db, "M5")  # idempotente sobre legado
    definitions = {(r["feature_id"], r["algorithm_version"]): r for r in feature_store.list_definitions(db)}
    for ident in IDS:
        assert indicator_registry.get_definition(ident)["algorithm_version"] == "1.1"
        assert definitions[ident, "1.0"]["parameters"] == old_parameters[ident]
        assert definitions[ident, "1.1"]["parameters"]["seed"] == "first_observation"
    assert indicator_registry.get_definition("price.return_1")["algorithm_version"] == "1.0"
    pd.testing.assert_frame_equal(previous, feature_store.query_history(db, symbol="TEST"))
    seed(db, source(n=45))
    _materialize(db)
    for ident in IDS:
        assert indicator_materializer.algorithm_version(ident).startswith("1.1+")
    old = query_service.series(db, symbol="TEST", timeframe="M5", feature_ids=list(IDS), as_of=NOW,
                               basis="knowledge", algorithm_version="1.0")
    assert old["status"] == "ok" and len(old["values"]) == len(IDS)
    assert all(row["value"] == 50.0 and row["revision"] == 1 for row in old["values"])
    assert all(d["versions"]["1.0"]["unit"] == "legacy-unit" for d in old["definitions"].values())


def test_direct_registry_migrates_without_replacing_legacy_definitions_or_values(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "legacy.sqlite"), broker_utc_offset=0)
    try:
        exercise_version_migration(db)
    finally:
        db.close()


def test_sdk_and_cli_use_the_same_horizon_and_definitions(tmp_path, capsys):
    path = tmp_path / "wilder #1.sqlite"
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
