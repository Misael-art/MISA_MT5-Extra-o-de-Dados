"""T17.25: recuperação de contexto e identidade dos resultados de execução."""
import pandas as pd
import pytest

from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.strategies.backtester import BacktestConfig, SymbolSpec
from mt5_extracao.trade_analytics import aggregate, ledger
from mt5_extracao.trade_analytics.execution import engine, runner
from tests.market_data import random_walk


@pytest.fixture
def db(tmp_path):
    database = DatabaseManager(db_path=str(tmp_path / "recovery.db"))
    yield database
    database.close()


def _trade(bars, **changes):
    row = {"entry_time": bars.time.iloc[30], "exit_time": bars.time.iloc[35], "direction": 1,
           "entry_price": 100.0, "stop_price": 1.0, "target_price": None, "pnl": 5.0, "volume": 1.0,
           "exit_reason": "fim dos dados"}
    return dict(row, **changes)


def _save(db, rows, run_id="r"):
    ledger.save_trades(db, run_id, rows, robot_key="mql5:r", symbol="X", timeframe="M5")


def test_context_refreshes_when_bars_arrive(db):
    bars = random_walk(50, seed=4, vol=.2, freq="5min")
    _save(db, [_trade(bars)])
    assert aggregate.frame(db, None).atr_pct.isna().all()
    db.save_ohlcv_data("X", "5 minutos", bars)
    assert aggregate.frame(db, None).atr_pct.notna().all()


def test_correction_inside_unchanged_range_updates_context_and_then_uses_cache(db, monkeypatch):
    from mt5_extracao.trade_analytics import enrich
    bars = random_walk(50, seed=4, vol=.2, freq="5min")
    db.save_ohlcv_data("X", "5 minutos", bars)
    _save(db, [_trade(bars)], "a")
    _save(db, [_trade(bars)], "b")
    first = aggregate.frame(db, None)
    changed = bars.copy(deep=True)
    changed.loc[29, "real_volume"] = 987654
    db.save_ohlcv_data("X", "5 minutos", changed)
    second = aggregate.frame(db, None)
    assert (second.entry_volume.astype(float) == 987654).all()
    assert not (first.entry_volume.astype(float) == 987654).any()
    monkeypatch.setattr(enrich, "enrich", lambda *_: pytest.fail("dados inalterados devem vir do cache"))
    third = aggregate.frame(db, None)
    assert (third.entry_volume.astype(float) == 987654).all()


def test_partial_run_explains_missing_entry_instead_of_showing_zero_results(db):
    bars = random_walk(50, seed=4, vol=.2, freq="5min")
    db.save_ohlcv_data("X", "5 minutos", bars)
    _save(db, [{"exit_time": bars.time.iloc[35], "pnl": 5, "volume": 1}], "old")
    with pytest.raises(ValueError, match="horário de entrada"):
        runner.compare(db, None, "old", ["ponto_fixo"])
    assert ledger.load(db, run_id="old").entry_time.isna().all()


def test_all_skipped_run_reports_what_is_needed(db):
    bars = random_walk(50, seed=4, vol=.2, freq="5min")
    db.save_ohlcv_data("X", "5 minutos", bars)
    _save(db, [_trade(bars, stop_price=None)])
    with pytest.raises(ValueError, match="1 excluídas.*stop"):
        runner.compare(db, None, "r", ["ponto_fixo"])


def test_simulation_keeps_identity_for_simultaneous_approximated_entries():
    bars = random_walk(50, seed=4, vol=.2, freq="5min")
    entry = bars.time.iloc[30] + pd.Timedelta(minutes=1)
    trades = pd.DataFrame([_trade(bars, seq=seq, entry_time=entry) for seq in (7, 11)])
    result = engine.simulate(trades, bars, SymbolSpec("X", known=False), BacktestConfig(swap=False),
                             engine.ExitPolicy())
    assert len(result.trades) == 2 and result.approximate == 2
    assert result.source_seqs == [7, 11]
    assert all(trade.entry_time != entry for trade in result.trades)


def test_comparison_frames_keep_every_original_seq_and_explain_skips(db):
    bars = random_walk(50, seed=4, vol=.2, freq="5min")
    db.save_ohlcv_data("X", "5 minutos", bars)
    entry = bars.time.iloc[30] + pd.Timedelta(minutes=1)
    _save(db, [_trade(bars, entry_time=entry), _trade(bars, entry_time=entry), _trade(bars, stop_price=None)])
    table = runner.compare(db, None, "r", ["ponto_fixo", "trailing_atr"])
    for item in table.values():
        assert item["metrics"]["n"] == len(item["frame"]) == 2
        assert item["frame"]["seq"].tolist() == [0, 1]
        assert item["frame"]["run_id"].tolist() == ["r", "r"]
        assert item["result"].skipped == 1 and item["result"].approximate == 2


@pytest.mark.parametrize("warm", [False, True])
@pytest.mark.parametrize("point", ["before", "after"])
def test_failed_feature_append_never_marks_incomplete_cache_as_fresh(db, monkeypatch, warm, point):
    from mt5_extracao.trade_analytics import cache
    from sqlalchemy import text
    bars = random_walk(50, seed=4, vol=.2, freq="5min")
    db.save_ohlcv_data("X", "5 minutos", bars)
    _save(db, [_trade(bars)])
    if warm:
        aggregate.frame(db, None)
    with db.engine.connect() as conn:
        before = conn.execute(text(f"SELECT * FROM {cache.RUNS}")).fetchall() if warm else []
    changed = bars.copy()
    changed.loc[29, "real_volume"] = 987654
    db.save_ohlcv_data("X", "5 minutos", changed)
    original = pd.DataFrame.to_sql
    calls = []
    def fail_after_append(frame, *args, **kwargs):
        calls.append(len(frame))
        if len(calls) == 1 and point == "before":
            raise RuntimeError("falha de armazenamento antes de inserir linhas")
        result = original(frame, *args, **kwargs)
        if len(calls) == 1:
            raise RuntimeError("falha de armazenamento após inserir linhas")
        return result
    monkeypatch.setattr(pd.DataFrame, "to_sql", fail_after_append)
    assert aggregate.frame(db, None).entry_volume.iloc[0] == 987654
    with db.engine.connect() as conn:
        assert conn.execute(text(f"SELECT * FROM {cache.RUNS}")).fetchall() == before
    assert aggregate.frame(db, None).entry_volume.iloc[0] == 987654
    assert calls == [1, 1]      # a falha exige recomputar; só a segunda gravação torna o cache atual


def test_sequence_risk_metrics_match_original_operations_after_entry_reordering(db):
    import numpy as np
    from tests.test_trade_sizing import _robot
    bars = random_walk(50, seed=4, vol=.2, freq="5min")
    db.save_ohlcv_data("X", "5 minutos", bars)
    offsets = np.random.default_rng(17).integers(0, 4, 20)
    _save(db, [_trade(bars, entry_time=bars.time.iloc[30 + offset],
                      exit_time=bars.time.iloc[40], volume=float(k + 1), stop_price=50+k)
               for k, offset in enumerate(offsets)])
    robot = _robot([("InpLotMultiplier", 2, "multiplicador de lote")], facts={"risk_pct": 1.0})
    table = runner.compare(db, None, "r", ["ponto_fixo", "martingale"], robot=robot)
    base = table["ponto_fixo"]["result"]
    original = dict(zip(base.source_seqs, base.trades))
    sequence = table["martingale"]["sequence"]["trades"]
    assert set(sequence.seq) == set(original) and len(sequence) == 20
    expected = [original[row.seq].r_multiple * row.volume / original[row.seq].volume
                for row in sequence.itertuples() if not row.skipped]
    assert table["martingale"]["metrics"]["expectancy_r"] == pytest.approx(sum(expected) / len(expected))
