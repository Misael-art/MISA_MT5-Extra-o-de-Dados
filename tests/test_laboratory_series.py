"""Laboratório: fórmulas reais e snapshots SQLite, sem terminal/GUI ou fills fictícios."""
import copy
import json

import numpy as np
import pandas as pd
import pytest

from mt5_extracao.analytics import datasets, indicator_registry
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.laboratory import service
from mt5_extracao.strategies import REGISTRY, get_strategy


@pytest.fixture
def source_db(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "lab.sqlite"), broker_utc_offset=0)
    for tf, freq, count in (("M5", "5min", 960), ("M15", "15min", 320)):
        from mt5_extracao import timeframes
        t = np.arange(count)
        close = 100 + np.sin(t / 5) * 3 + t / 300
        frame = pd.DataFrame(dict(time=pd.date_range("2025-01-06", periods=count, freq=freq),
            open=close - .1, high=close + .5, low=close - .5, close=close,
            tick_volume=10 + t % 7, real_volume=0, spread=1))
        db.save_ohlcv_data("X", timeframes.parse(tf).label, frame)
    yield db
    db.close()


@pytest.fixture
def snapshots(source_db):
    return service.capture_selection(source_db, symbol="X", timeframes_selected=["M15", "M5"])


def analysis(snapshots, **kwargs):
    return service.analyze(snapshots, feature_ids=["momentum.rsi_14"],
                           as_of="2025-02-01T00:00:00Z", **kwargs)


def test_catalog_discovers_every_formula_strategy_and_typed_defaults():
    catalog = service.catalog()
    assert {row["feature_id"] for row in catalog["indicators"]} == {
        row["feature_id"] for row in indicator_registry.definitions()}
    assert {row["key"] for row in catalog["strategies"]} == set(REGISTRY)
    assert all(row["parameters"] and row["warmup"] for row in catalog["strategies"])
    json.dumps(catalog, allow_nan=False)


def test_frozen_formula_values_match_registry_and_identity_survives_source_edit(source_db, snapshots):
    before = [item.bars for item in snapshots]
    result = analysis(snapshots, strategy_key="ema_atr")
    assert result["grid_timeframe"] == "M5"
    assert [item["timeframe"] for item in result["populations"]] == ["M5", "M15"]
    channel = result["channels"][0]
    expected = list(indicator_registry.iter_core(snapshots[0].bars, feature_ids=["momentum.rsi_14"]))
    assert channel["values"] == [row["value"] for row in expected]
    assert result["qualification"]["historical_availability_proven"] is False
    assert result["formula_source_sha256"] == service.formula_source_hash()
    again = analysis(list(reversed(snapshots)), strategy_key="ema_atr")
    assert result == again
    frame = snapshots[0].bars.copy()
    frame.loc[0, "close"] += .1
    source_db.save_ohlcv_data("X", "5 minutos", frame)
    assert analysis([datasets.load(source_db, item.manifest_id) for item in snapshots], strategy_key="ema_atr") == result
    for item, old in zip(snapshots, before):
        pd.testing.assert_frame_equal(item.bars, old)
    json.dumps(result, allow_nan=False)


def test_higher_timeframe_cannot_be_seen_before_close_and_never_fills_across_gap():
    end = pd.to_datetime(["2025-01-01T00:15Z", "2025-01-01T01:00Z"])
    channel = dict(timeframe="M15", source_end=end, values=pd.Series([10., 20.]), direction=pd.Series([1., -1.]))
    grid = pd.to_datetime(["2025-01-01T00:10Z", "2025-01-01T00:15Z", "2025-01-01T00:25Z",
                           "2025-01-01T00:30Z", "2025-01-01T01:00Z"])
    joined = service._align(channel, grid)
    assert joined.value.isna().tolist() == [True, False, False, True, False]
    assert joined.value.iloc[1:3].tolist() == [10., 10.]
    assert joined.value.iloc[-1] == 20.
    assert all(row.source_end <= row.at for row in joined.itertuples() if pd.notna(row.source_end))


def test_cutoff_filters_formulas_and_does_not_publish_still_open_bar(snapshots):
    result = service.analyze(snapshots[:1], feature_ids=["price.return_1"], as_of="2025-01-06T00:12:00Z")
    assert result["times"] == ["2025-01-06T00:05:00+00:00", "2025-01-06T00:10:00+00:00"]
    assert result["populations"][0]["closed_rows"] == 2


def test_constant_and_missing_pairs_are_unavailable_not_zero_or_perfect():
    end = pd.date_range("2025-01-01", periods=12, freq="min", tz="UTC")
    frame = pd.DataFrame(dict(direction=np.ones(12), source_end=end))
    constant = service._pair(frame, frame, 0)
    assert constant["correlation"] is None and constant["reason"] == "CONSTANT_SERIES"
    frame.direction = np.nan
    assert service._pair(frame, frame, 0)["n_pairs"] == 0
    assert service._pair(frame, frame, 0)["agreement"] is None


def test_lag_is_prior_predictor_and_counts_repeated_source_pairs():
    at = pd.date_range("2025-01-01", periods=6, freq="min", tz="UTC")
    left = pd.DataFrame(dict(direction=[1, -1, 0, 1, -1, 0], source_end=at))
    right = pd.DataFrame(dict(direction=[0, 1, -1, 0, 1, -1], source_end=at))
    lag = service._pair(left, right, 1)
    assert lag["correlation"] == pytest.approx(1)
    assert lag["n_pairs"] == 5
    assert lag["unique_source_pairs"] == 5


def test_volume_unknown_remains_blocked_and_warmup_remains_missing(snapshots):
    result = service.analyze(snapshots, feature_ids=["volume.real", "momentum.rsi_14"],
                             strategy_key="vwap_bands", as_of="2025-02-01T00:00:00Z")
    real = result["channels"][0]
    vwap = result["channels"][2]
    assert not real["eligibility"]["eligible"] and real["available"] == 0
    assert not vwap["eligibility"]["eligible"] and vwap["available"] == 0
    rsi = result["channels"][1]
    assert rsi["values"][:14] == [None] * 14


@pytest.mark.parametrize("change,match", [
    (dict(as_of="2025-01-01"), "fuso"),
    (dict(feature_ids=["unknown"]), "desconhecido"),
    (dict(strategy_key="ema_atr", strategy_params=dict(fast=True)), "finito"),
    (dict(feature_ids=["momentum.rsi_14"] * 2), "duplicados"),
    (dict(feature_ids=[], strategy_key=None), "Escolha"),
])
def test_bad_selection_refused(snapshots, change, match):
    args = dict(feature_ids=["momentum.rsi_14"], as_of="2025-02-01T00:00:00Z")
    args.update(change)
    with pytest.raises(ValueError, match=match):
        service.analyze(snapshots, **args)


def test_source_clock_and_duplicate_timeframe_cannot_mix(snapshots):
    with pytest.raises(ValueError, match="snapshot por timeframe"):
        analysis([snapshots[0], snapshots[0]])
    wrong = copy.deepcopy(snapshots[1])
    wrong._manifest["source"]["namespace"] = "a" * 32
    with pytest.raises(ValueError, match="origem"):
        analysis([snapshots[0], wrong])
    unknown = copy.deepcopy(snapshots[0])
    unknown._manifest["time"]["offset_state"] = "unknown"
    with pytest.raises(ValueError, match="offset"):
        analysis([unknown])


def test_monthly_expiry_and_negative_broker_offset():
    frame = pd.DataFrame(dict(time=pd.to_datetime(["2025-01-01", "2025-02-01"])))
    _, ends = service._bounds(frame, service.timeframes.Timeframe.MN1, -3)
    assert ends[0].isoformat() == "2025-02-01T03:00:00+00:00"


def test_cli_catalog_does_not_open_database_or_gui(monkeypatch, capsys):
    from mt5_extracao import cli
    monkeypatch.setattr(cli.Context, "db", property(lambda _: pytest.fail("não abrir DB")))
    assert cli.main(["--json", "laboratory", "catalog"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] and payload["data"]["laboratory_version"] == 1


def test_cli_and_sdk_produce_same_frozen_diagnostic(snapshots, source_db, tmp_path, capsys, monkeypatch):
    from mt5_extracao import cli
    monkeypatch.setattr(cli.Context, "db", property(lambda _: source_db))
    argv = ["--json", "laboratory", "analyze", "--as-of", "2025-02-01T00:00:00Z",
            "--feature", "momentum.rsi_14"]
    for item in snapshots:
        argv.extend(["--manifest", item.manifest_id])
    path = tmp_path / "diagnostic.json"
    assert cli.main(argv + ["--out", str(path)]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["data"] == analysis(snapshots) == json.loads(path.read_text(encoding="utf-8"))
    assert cli.main(argv + ["--out", str(path)]) == 2
    assert json.loads(capsys.readouterr().out)["ok"] is False
    assert json.loads(path.read_text(encoding="utf-8")) == payload["data"]
    # O estudo da UI é validado pela mesma análise exportada, sem desktop.
    from mt5_extracao.laboratory.annotations import Study
    study = Study.for_report(payload["data"])
    study.add("hline", y=1.0, text="nível")
    study.remove_series(payload["data"]["channels"][0]["id"])
    study_path = tmp_path / "estudo.json"
    study_path.write_text(study.to_json(), encoding="utf-8")
    assert cli.main(["--json", "laboratory", "study", "--analysis", str(path), "--study", str(study_path)]) == 0
    summary = json.loads(capsys.readouterr().out)["data"]
    assert summary["removed_series"] == [payload["data"]["channels"][0]["id"]]
    assert summary["items"][0]["text"] == "nível" and summary["analysis_id"] == payload["data"]["analysis_id"]
    study_path.write_text(study.to_json().replace(payload["data"]["analysis_id"], "f" * 64), encoding="utf-8")
    assert cli.main(["--json", "laboratory", "study", "--analysis", str(path), "--study", str(study_path)]) == 2
    assert "outra análise" in capsys.readouterr().out


def test_cli_freeze_and_strategy_only_share_canonical_snapshots(source_db, monkeypatch, capsys):
    from mt5_extracao import cli
    monkeypatch.setattr(cli.Context, "db", property(lambda _: source_db))
    assert cli.main(["--json", "laboratory", "freeze", "--symbol", "X", "--tf", "M15"]) == 0
    ident = json.loads(capsys.readouterr().out)["data"]["manifests"][0]["id"]
    assert cli.main(["--json", "laboratory", "analyze", "--manifest", ident, "--strategy", "ema_atr",
                     "--strategy-only", "--as-of", "2025-02-01T00:00:00Z"]) == 0
    data = json.loads(capsys.readouterr().out)["data"]
    assert len(data["channels"]) == 1 and data["channels"][0]["rule"] == "candidate"


def test_reverse_higher_tf_lag_is_included_and_repetition_is_explicit(snapshots):
    result = analysis(snapshots)
    assert any(pair["left"].startswith("M15:") and pair["right"].startswith("M5:") and pair["lag_bars"] == 1
               for pair in result["pairs"])
    pair = next(pair for pair in result["pairs"] if pair["left"].startswith("M5:") and
                pair["right"].startswith("M15:") and pair["lag_bars"] == 0)
    assert pair["right_source_bars"] < pair["n_pairs"]


def test_large_selection_refuses_before_capture_or_formula(source_db, snapshots, monkeypatch):
    monkeypatch.setattr(service, "MAX_BARS", 5)
    monkeypatch.setattr(datasets, "capture", lambda *a, **k: pytest.fail("não persistir recusa de limite"))
    with pytest.raises(ValueError, match="janela"):
        service.capture_selection(source_db, symbol="X", timeframes_selected=["M5"])
    with pytest.raises(ValueError, match="janela menor"):
        analysis(snapshots)


def test_empty_channel_reports_warmup_reason_and_action(source_db):
    # SINTÉTICO: ~80 barras M15 não aquecem uma estratégia com aquecimento maior que a janela.
    strategy = "ema_atr"
    snapshots = service.capture_selection(source_db, symbol="X", timeframes_selected=["M15", "M5"],
                                          start="2025-01-08T12:00:00")
    assert get_strategy(strategy).min_bars > len(snapshots[1].bars)
    result = analysis(snapshots, strategy_key=strategy)
    by_id = {row["id"]: row for row in result["channels"]}
    empty = by_id[f"M15:strategy:{strategy}"]
    assert empty["source_available"] == 0
    assert empty["diagnosis"]["code"] == "WARMUP_EXCEEDS_WINDOW"
    assert "Antecipe o início" in empty["diagnosis"]["action"]
    assert by_id["M5:momentum.rsi_14"]["diagnosis"] is None
    json.dumps(result, allow_nan=False)


def test_oversized_or_empty_window_suggests_concrete_action(source_db, monkeypatch):
    monkeypatch.setattr(service, "MAX_BARS", 100)
    with pytest.raises(ValueError, match="Informe início a partir de 2025-01-08T23:40:00"):
        service.capture_selection(source_db, symbol="X", timeframes_selected=["M5"])
    with pytest.raises(ValueError, match="nenhuma barra de X"):
        service.capture_selection(source_db, symbol="X", timeframes_selected=["M5"], start="2030-01-01")


def test_lag_zero_is_descriptive_and_lagged_pairs_carry_fixed_temporal_split(snapshots):
    result = analysis(snapshots)
    for pair in result["pairs"]:
        if pair["lag_bars"] == 0:
            assert pair["relation"] == "DESCRIPTIVE" and pair["out_of_sample"] is None
        else:
            assert pair["relation"] == "PREDICTIVE_CANDIDATE"
            split = pair["out_of_sample"]
            if pair["reason"] is None:
                assert split["fit_pairs"] + split["holdout_pairs"] == pair["n_pairs"]
                assert split["fit_fraction"] == service.FIT_FRACTION
    assert any("fora da amostra" in text for text in result["limitations"])
    json.dumps(result, allow_nan=False)


@pytest.mark.parametrize("tail,verdict", [(1, "HOLDOUT_CONSISTENT"), (-1, "HOLDOUT_SIGN_FLIP")])
def test_temporal_split_flags_relation_that_does_not_survive_holdout(tail, verdict):
    import numpy as np
    import pandas as pd
    rng = np.random.default_rng(7)
    x = pd.Series(rng.choice([-1.0, 1.0], 200))
    y = x.copy()
    y.iloc[140:] *= tail                     # ajuste: relação positiva; fora: mantém ou inverte
    noise = rng.random(200) < .1
    y[noise] *= -1
    split = service._temporal_split(x, y)
    assert split["fit_pairs"] == 140 and split["holdout_pairs"] == 60
    assert split["fit_correlation"] > .5 and split["verdict"] == verdict
    assert service._temporal_split(x[:40], y[:40])["verdict"] == "HOLDOUT_INSUFFICIENT"
    unrelated = pd.Series(rng.choice([-1.0, 1.0], 200))
    assert service._temporal_split(x, unrelated)["verdict"] == "FIT_WEAK"


def test_null_band_tracks_autocorrelation_and_gates_consistent_verdict(monkeypatch):
    import numpy as np
    import pandas as pd
    rng = np.random.default_rng(11)
    iid = pd.Series(rng.choice([-1.0, 1.0], 600)), pd.Series(rng.choice([-1.0, 1.0], 600))
    # Sinais retidos em blocos de 40 barras (como intenção de TF maior): acaso produz |r| maior.
    blocks = [pd.Series(np.repeat(rng.choice([-1.0, 1.0], 15), 40)) for _ in range(2)]
    assert service._null_band(*blocks, 60) > 2 * service._null_band(*iid, 60)
    x = pd.Series(rng.choice([-1.0, 1.0], 200))
    assert service._temporal_split(x, x.copy())["verdict"] == "HOLDOUT_CONSISTENT"
    monkeypatch.setattr(service, "_null_band", lambda *a: 0.99)
    split = service._temporal_split(x, x * np.where(rng.random(200) < .2, -1, 1))
    assert split["verdict"] == "WITHIN_NULL" and split["null_band_95"] == 0.99
    assert split["control"] == "circular_shift_of_predictor_in_holdout"


def test_predictor_must_beat_target_persistence():
    import numpy as np
    import pandas as pd
    rng = np.random.default_rng(5)
    target = pd.Series(np.repeat(rng.choice([-1.0, 1.0], 40), 10))   # alvo muito persistente
    lagged = target.shift(1).bfill()
    noisy = lagged * np.where(rng.random(400) < .15, -1, 1)           # preditor = alvo passado + ruído
    split = service._temporal_split(noisy, target, lagged)
    assert split["verdict"] == "NO_GAIN_OVER_PERSISTENCE"
    assert abs(split["naive_holdout_correlation"]) >= abs(split["holdout_correlation"])


def test_block_ci_is_deterministic_contains_estimate_and_widens_with_persistence():
    import numpy as np
    import pandas as pd
    rng = np.random.default_rng(3)
    x = pd.Series(rng.normal(size=600))
    y = x * .5 + pd.Series(rng.normal(size=600))
    low, high = service._block_ci(x, y)
    assert low < x.corr(y) < high and service._block_ci(x, y) == [low, high]
    persistent = [pd.Series(np.repeat(rng.normal(size=20), 30)) for _ in range(2)]
    p_low, p_high = service._block_ci(*persistent)
    assert p_high - p_low > high - low
    assert service._block_ci(x[:8], y[:8]) is None


def test_predictive_lags_include_longer_fixed_horizon(snapshots):
    lags = {pair["lag_bars"] for pair in analysis(snapshots)["pairs"]}
    assert lags == {0, *service.PREDICTIVE_LAGS} and 6 in lags
