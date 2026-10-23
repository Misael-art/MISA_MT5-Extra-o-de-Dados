"""BD04: ingestão idempotente, revisão preservada, falha sem sucesso falso e ausência sem zero."""
from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import text

from mt5_extracao.analytics import datasets, resampling
from mt5_extracao.database_manager import DatabaseManager
from tests.test_analytics_calendar import m1_bars

SYMBOL = "EURUSD"
INFO = SimpleNamespace(name=SYMBOL)


def _db(tmp_path):
    tmp_path.mkdir(parents=True, exist_ok=True)
    return DatabaseManager(db_type="sqlite", db_path=str(tmp_path / "bd04.sqlite"), broker_utc_offset=-3)


def _raw_count(db):
    table = db.get_table_name_for_symbol(SYMBOL, "1 minuto")
    with db.engine.connect() as conn:
        return conn.execute(text(f'SELECT COUNT(*) FROM "{table}"')).scalar_one()


def _derived(db):
    with db.engine.connect() as conn:
        return pd.read_sql(text("SELECT bar_start_utc, revision, close FROM _bars_derived WHERE tf='M3' "
                                "ORDER BY bar_start_utc, revision"), conn)


def test_repeat_correction_and_snapshot_preserve_history(tmp_path):
    db = _db(tmp_path)
    bars = m1_bars(pd.date_range("2026-09-14 09:00", periods=6, freq="min"))
    assert db.save_ohlcv_data(SYMBOL, "1 minuto", bars)
    assert db.save_ohlcv_data(SYMBOL, "1 minuto", bars)
    assert _raw_count(db) == 6
    resampling.resample_symbol(db, SYMBOL, ["M3"], symbol_info=INFO)
    second = resampling.resample_symbol(db, SYMBOL, ["M3"], symbol_info=INFO)[0]
    assert (second["inserted"], second["corrected"]) == (0, 0)
    before = datasets.capture(db, SYMBOL, "M1")

    corrected = bars.copy()
    corrected.loc[2, "close"] = 250.0
    corrected.loc[2, "high"] = 251.0
    assert db.save_ohlcv_data(SYMBOL, "1 minuto", corrected.iloc[[2]])
    assert _raw_count(db) == 6
    summary = resampling.resample_symbol(db, SYMBOL, ["M3"], symbol_info=INFO)[0]
    assert summary["corrected"] == 1

    derived = _derived(db)
    first_bar = derived[derived.bar_start_utc == derived.bar_start_utc.min()]
    assert first_bar["revision"].tolist() == [1, 2]
    assert first_bar["close"].tolist() == [102.0, 250.0]  # revisão 1 preservada
    assert datasets.load(db, before.manifest_id).bars["close"].iloc[2] == 102.0  # snapshot imutável


def test_failed_save_reports_failure_and_writes_nothing(tmp_path):
    db = _db(tmp_path)
    bad = m1_bars(pd.date_range("2026-09-14 09:00", periods=3, freq="min")).drop(columns=["time"])
    assert db.save_ohlcv_data(SYMBOL, "1 minuto", bad) is False
    table = db.get_table_name_for_symbol(SYMBOL, "1 minuto")
    with db.engine.connect() as conn:
        exists = conn.execute(text("SELECT name FROM sqlite_master WHERE name=:t"), {"t": table}).first()
    assert exists is None or _raw_count(db) == 0


def test_unknown_real_volume_stays_null_not_zero(tmp_path):
    db = _db(tmp_path)
    bars = m1_bars(pd.date_range("2026-09-14 09:00", periods=3, freq="min"), real_volume=[0, None, 7])
    assert db.save_ohlcv_data(SYMBOL, "1 minuto", bars)
    stored = db.load_ohlcv(SYMBOL, "1 minuto")["real_volume"].tolist()
    assert stored[0] == 0 and pd.isna(stored[1]) and stored[2] == 7


def test_out_of_order_ingestion_equals_ordered(tmp_path):
    bars = m1_bars(pd.date_range("2026-09-14 09:00", periods=6, freq="min"))
    ordered, shuffled = _db(tmp_path / "a"), _db(tmp_path / "b")
    assert ordered.save_ohlcv_data(SYMBOL, "1 minuto", bars)
    assert shuffled.save_ohlcv_data(SYMBOL, "1 minuto", bars.iloc[[3, 0, 5, 1, 4, 2]])
    pd.testing.assert_frame_equal(ordered.load_ohlcv(SYMBOL, "1 minuto"), shuffled.load_ohlcv(SYMBOL, "1 minuto"))
    for db in (ordered, shuffled):
        resampling.resample_symbol(db, SYMBOL, ["M3"], symbol_info=INFO)
    pd.testing.assert_frame_equal(_derived(ordered), _derived(shuffled))


# --- Contrato de recebimento (V02/V07 + R01–R03 da 2ª revisão) ---
# Cada gravação é um RECEBIMENTO (instante em que este sistema recebeu o payload). Prevalece o conteúdo
# do último recebimento; legado sem metadado tem chegada desconhecida e não é elegível no modo causal.

T1, T2, T3, T4 = (pd.Timestamp(v, tz="UTC") for v in
                  ("2026-09-14 13:00", "2026-09-15 13:00", "2026-09-16 13:00", "2026-09-17 13:00"))


def _bar(db, close, received_at):
    bars = m1_bars(pd.date_range("2026-09-14 09:00", periods=3, freq="min"))
    bars.loc[1, ["close", "high"]] = [close, max(close, bars.loc[1, "high"])]
    assert db.save_ohlcv_data(SYMBOL, "1 minuto", bars, received_at=received_at)


def _current(db):
    return db.load_ohlcv(SYMBOL, "1 minuto")["close"].iloc[1]


def _as_of(db, as_of, mode="causal"):
    return db.load_ohlcv_as_of(SYMBOL, "1 minuto", as_of, mode=mode)


def _close_at(db, as_of):
    frame = _as_of(db, as_of)
    row = frame[frame["time"] == pd.Timestamp("2026-09-14 09:01")]
    return row["close"].iloc[0] if len(row) else None


def test_r01_first_write_keeps_its_receipt_and_precedence(tmp_path):
    db = _db(tmp_path)
    _bar(db, 250.0, T2)
    _bar(db, 101.0, T1)  # chegou depois, mas foi recebido antes: não prevalece
    assert _current(db) == 250.0
    assert _as_of(db, T3)["known_since_utc"].iloc[1] == T2


def test_r01_explicit_and_default_receipts_are_known_legacy_stays_unknown(tmp_path):
    db = _db(tmp_path)
    _bar(db, 101.0, T1)
    _bar(db, 250.0, T2)
    assert _as_of(db, T2 - pd.Timedelta(hours=1))["known_since_utc"].iloc[1] == T1
    default = _db(tmp_path / "default")
    before = pd.Timestamp.now(tz="UTC")
    _bar(default, 101.0, None)
    assert _as_of(default, pd.Timestamp.now(tz="UTC"))["known_since_utc"].iloc[1] >= before
    legacy = _db(tmp_path / "legacy")
    _bar(legacy, 101.0, None)
    with legacy.engine.begin() as conn:  # banco gravado antes do contrato: bruto sem recebimentos
        conn.execute(text("DELETE FROM _ohlcv_receipts"))
        conn.execute(text("DELETE FROM _ohlcv_versions"))
    assert _close_at(legacy, T4) is None  # chegada desconhecida: não elegível no modo causal
    inspect = _as_of(legacy, T4, mode="inspect")
    assert inspect["arrival_state"].iloc[1] == "unknown" and inspect["known_since_utc"].iloc[1] is None
    _bar(legacy, 250.0, T2)  # correção posterior arquiva o legado sem inventar sua chegada
    assert _close_at(legacy, T2) == 250.0 and _close_at(legacy, T2 - pd.Timedelta(hours=1)) is None


def test_r02_cuts_before_observation_close_receipt_and_each_revision(tmp_path):
    db = _db(tmp_path)  # offset -3: barra 09:01 local = 12:01 UTC, fecha 12:02 UTC
    _bar(db, 101.0, T1)
    _bar(db, 250.0, T2)
    _bar(db, 300.0, T3)
    assert _close_at(db, pd.Timestamp("2026-09-14 11:00", tz="UTC")) is None   # antes da observação
    assert _close_at(db, pd.Timestamp("2026-09-14 12:01:30", tz="UTC")) is None  # antes do fechamento
    assert _close_at(db, pd.Timestamp("2026-09-14 12:30", tz="UTC")) is None   # fechada, não recebida
    assert _close_at(db, T1) == 101.0
    assert _close_at(db, T2 - pd.Timedelta(seconds=1)) == 101.0
    assert _close_at(db, T2) == 250.0 and _close_at(db, T3) == 300.0
    assert _current(db) == 300.0  # latest é outra consulta


def test_r02_naive_timestamps_are_refused(tmp_path):
    db = _db(tmp_path)
    with pytest.raises(ValueError, match="fuso"):
        _bar(db, 101.0, pd.Timestamp("2026-09-14 13:00"))
    _bar(db, 101.0, T1)
    with pytest.raises(ValueError, match="fuso"):
        _as_of(db, pd.Timestamp("2026-09-15 00:00"))


def test_r03_equal_content_reconfirmed_later_keeps_precedence(tmp_path):
    db = _db(tmp_path)
    _bar(db, 101.0, T1)
    _bar(db, 250.0, T2)
    _bar(db, 250.0, T4)  # nova observação com conteúdo igual: atualiza o recebimento usado na precedência
    _bar(db, 300.0, T3)
    assert _current(db) == 250.0
    assert _close_at(db, T3) == 300.0 and _close_at(db, T4) == 250.0


def test_r03_same_event_retry_is_idempotent_and_ties_are_order_independent(tmp_path):
    db = _db(tmp_path)
    _bar(db, 101.0, T1)
    assert _receipts(db) == 3  # três barras, um recebimento cada
    _bar(db, 101.0, T1)       # retry do MESMO evento
    assert _receipts(db) == 3
    one, two = _db(tmp_path / "one"), _db(tmp_path / "two")
    for db_, order in ((one, (250.0, 300.0)), (two, (300.0, 250.0))):
        for close in order:
            _bar(db_, close, T2)  # empate: mesmo recebimento, conteúdos divergentes
    assert _current(one) == _current(two)
    assert _as_of(one, T3)["conflict"].iloc[1] and _as_of(two, T3)["conflict"].iloc[1]


def test_batch_with_duplicate_times_is_order_independent(tmp_path):
    one, two = _db(tmp_path / "one"), _db(tmp_path / "two")
    bars = m1_bars(pd.date_range("2026-09-14 09:00", periods=2, freq="min"))
    newer = bars.copy()
    newer["close"] += 5
    assert one.save_ohlcv_data(SYMBOL, "1 minuto", bars, received_at=T1)
    assert two.save_ohlcv_data(SYMBOL, "1 minuto", newer, received_at=T2)
    assert two.save_ohlcv_data(SYMBOL, "1 minuto", bars, received_at=T1)
    assert one.save_ohlcv_data(SYMBOL, "1 minuto", newer, received_at=T2)
    pd.testing.assert_frame_equal(one.load_ohlcv(SYMBOL, "1 minuto"), two.load_ohlcv(SYMBOL, "1 minuto"))


def _receipts(db):
    with db.engine.connect() as conn:
        return conn.execute(text("SELECT COUNT(*) FROM _ohlcv_receipts")).scalar_one()


def test_failure_in_second_chunk_rolls_back_everything_and_retry_is_clean(tmp_path, monkeypatch):
    from sqlalchemy import event
    db = _db(tmp_path)
    _bar(db, 101.0, T1)
    monkeypatch.setattr(DatabaseManager, "_UPSERT_CHUNK_ROWS", 2)
    calls = {"n": 0}

    def fail_second_insert(conn, cursor, statement, *args):
        if statement.lstrip().upper().startswith("INSERT INTO") and "_ohlcv_revisions" not in statement:
            calls["n"] += 1
            if calls["n"] == 2:
                raise RuntimeError("falha injetada no segundo bloco")

    event.listen(db.engine, "before_cursor_execute", fail_second_insert)
    bars = m1_bars(pd.date_range("2026-09-14 09:00", periods=6, freq="min"))
    bars["close"] += 1000
    assert db.save_ohlcv_data(SYMBOL, "1 minuto", bars, received_at=T2) is False
    event.remove(db.engine, "before_cursor_execute", fail_second_insert)
    stored = db.load_ohlcv(SYMBOL, "1 minuto")
    assert len(stored) == 3 and stored["close"].max() < 1000      # 1º bloco desfeito, nada parcial
    assert _receipts(db) == 3                                       # só os recebimentos da 1ª gravação
    assert db.save_ohlcv_data(SYMBOL, "1 minuto", bars, received_at=T2)  # retry íntegro
    assert len(db.load_ohlcv(SYMBOL, "1 minuto")) == 6 and _receipts(db) == 9


def test_real_volume_capability_changes_only_dependent_consumers_and_snapshot_stays(tmp_path):
    """E06: ausente → disponível → ausente. Preço segue elegível; volume real muda; snapshot congela."""
    from mt5_extracao.analytics import data_eligibility
    db = _db(tmp_path)
    times = pd.date_range("2026-09-14 09:00", periods=30, freq="min")
    states = []
    for real in (None, 5.0, None):
        bars = m1_bars(times, real_volume=[real] * len(times))
        db.save_ohlcv_data(SYMBOL, "1 minuto", bars, received_at=pd.Timestamp.now(tz="UTC"))
        report = data_eligibility.assess(db.load_ohlcv(SYMBOL, "1 minuto"))
        states.append((data_eligibility.evaluate(report, ["ohlcv"])["eligible"],
                       data_eligibility.evaluate(report, ["real_volume"])["eligible"]))
        if real is not None:
            frozen = datasets.capture(db, SYMBOL, "M1")
    assert states == [(True, False), (True, True), (True, False)]
    assert (datasets.load(db, frozen.manifest_id).bars["real_volume"] == 5.0).all()
    assert db.load_ohlcv(SYMBOL, "1 minuto")["real_volume"].isna().all()
    assert set(data_eligibility.FAMILIES) >= {"tick_volume", "real_volume", "aggressor", "book_snapshots"}
