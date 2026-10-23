"""BD04: ingestão idempotente, revisão preservada, falha sem sucesso falso e ausência sem zero."""
from types import SimpleNamespace

import pandas as pd
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


# --- V02/V07: revisões do bruto sem captura prévia, precedência, falha transacional e capacidade ---

T1, T2, T3 = (pd.Timestamp(v, tz="UTC") for v in ("2026-09-14 13:00", "2026-09-15 13:00", "2026-09-16 13:00"))


def _bar(db, close, received_at):
    bars = m1_bars(pd.date_range("2026-09-14 09:00", periods=3, freq="min"))
    bars.loc[1, ["close", "high"]] = [close, max(close, bars.loc[1, "high"])]
    assert db.save_ohlcv_data(SYMBOL, "1 minuto", bars, received_at=received_at)


def _close_at(db, as_of):
    return db.load_ohlcv_as_of(SYMBOL, "1 minuto", as_of)["close"].iloc[1]


def _revisions(db):
    with db.engine.connect() as conn:
        return pd.read_sql(text("SELECT revision, status, received_at_utc FROM _ohlcv_revisions ORDER BY revision"), conn)


def test_two_corrections_without_prior_capture_are_all_preserved_as_of(tmp_path):
    db = _db(tmp_path)
    _bar(db, 101.0, T1)
    _bar(db, 250.0, T2)
    _bar(db, 300.0, T3)
    assert db.load_ohlcv(SYMBOL, "1 minuto")["close"].iloc[1] == 300.0          # vigente (latest)
    assert _close_at(db, T2 - pd.Timedelta(hours=1)) == 101.0                    # antes da 1ª correção
    assert _close_at(db, T2) == 250.0 and _close_at(db, T3) == 300.0
    early = db.load_ohlcv_as_of(SYMBOL, "1 minuto", T2 - pd.Timedelta(hours=1))
    assert early["known_since_utc"].iloc[1] is None  # 1ª versão: chegada não registrada, não inventada
    assert _revisions(db)["status"].tolist() == ["superseded", "superseded", "accepted"]


def test_stale_payload_after_newer_one_does_not_overwrite(tmp_path):
    db = _db(tmp_path)
    _bar(db, 101.0, T1)
    _bar(db, 250.0, T2)
    _bar(db, 101.0, T1)  # retry tardio do payload antigo, com seu instante original de recebimento
    assert db.load_ohlcv(SYMBOL, "1 minuto")["close"].iloc[1] == 250.0
    assert _revisions(db)["status"].tolist() == ["superseded", "accepted", "stale"]
    assert _close_at(db, T3) == 250.0  # stale nunca vira a versão conhecida


def test_repeated_event_is_idempotent_and_unlabelled_retry_follows_explicit_policy(tmp_path):
    db = _db(tmp_path)
    _bar(db, 101.0, T1)
    _bar(db, 250.0, T2)
    _bar(db, 250.0, T2)
    assert len(_revisions(db)) == 2
    # Política explícita: sem received_at, o instante de gravação vale; o retry passa a ser a versão mais nova.
    _bar(db, 101.0, None)
    assert db.load_ohlcv(SYMBOL, "1 minuto")["close"].iloc[1] == 101.0
    assert _close_at(db, T3) == 250.0  # o que se sabia em T3 continua consultável


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
    assert _revisions(db).empty                                    # histórico também desfeito
    assert db.save_ohlcv_data(SYMBOL, "1 minuto", bars, received_at=T2)  # retry íntegro
    assert len(db.load_ohlcv(SYMBOL, "1 minuto")) == 6 and len(_revisions(db)) == 6


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
