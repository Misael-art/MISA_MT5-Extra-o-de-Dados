"""V4-01/V4-02: todo escritor segue o contrato temporal; fotografia = gerações confirmadas."""
import threading

import pandas as pd
import pytest
from sqlalchemy import text

from mt5_extracao.analytics import feature_store
from mt5_extracao.database_manager import DatabaseManager

SYMBOL, TF, FID, VER = "EURUSD", "M5", "price.return_1", "v4-probe"
T0 = pd.Timestamp("2026-09-14T09:00Z")
FAR = pd.Timestamp("2100-01-01", tz="UTC")


def _db(path):
    db = DatabaseManager(db_path=str(path))
    feature_store.register_definition(db, FID, "probe", VER, {}, [], value_type="NUMBER")
    return db


def _record(minute, value):
    stamp = T0 + pd.Timedelta(minutes=minute)
    return dict(symbol=SYMBOL, timeframe=TF, feature_id=FID, algorithm_version=VER, observation_time=stamp,
                value=value, observed_at=stamp, available_at=stamp)


def _values(db, basis="research", **kw):
    frame = feature_store.query_as_of(db, kw.pop("as_of", FAR), symbol=SYMBOL, timeframe=TF, feature_id=FID,
                                      basis=basis, **kw)
    return frame["numeric_value"].tolist()


def _rows(db):
    with db.engine.connect() as conn:
        return conn.execute(text("SELECT revision, recorded_at_utc, generation FROM _feature_values "
                                 "ORDER BY observation_time, revision")).fetchall()


# --- V4-01: save_value e save_values têm o mesmo contrato ------------------------------------------

def test_v4_01_single_and_batch_writers_are_equivalent(tmp_path):
    batch, single = _db(tmp_path / "b.sqlite"), _db(tmp_path / "s.sqlite")
    feature_store.save_values(batch, [_record(m, float(m)) for m in (0, 5, 10)])
    for m in (0, 5, 10):
        r = _record(m, float(m))
        feature_store.save_value(single, r.pop("symbol"), r.pop("timeframe"), r.pop("feature_id"),
                                 r.pop("algorithm_version"), r.pop("observation_time"), r.pop("value"), **r)
    for db in (batch, single):
        assert all(row[1] is not None and row[2] is not None for row in _rows(db))
        assert _values(db) == _values(db, basis="knowledge") == [0.0, 5.0, 10.0]


def test_v4_01_single_writer_insert_revision_retry_and_rollback(tmp_path):
    db = _db(tmp_path / "one.sqlite")
    feature_store.save_values(db, [_record(m, float(m)) for m in (0, 5, 10)])
    snap = feature_store.store_snapshot(db)
    def one(minute, value):
        r = _record(minute, value)
        return feature_store.save_value(db, r.pop("symbol"), r.pop("timeframe"), r.pop("feature_id"),
                                        r.pop("algorithm_version"), r.pop("observation_time"), r.pop("value"), **r)
    one(15, 15.0)                                   # inserção individual depois da fotografia
    assert _values(db, generation_through=snap["generation"]) == [0.0, 5.0, 10.0]
    assert _values(db, basis="knowledge") == [0.0, 5.0, 10.0, 15.0]
    first = _rows(db)[2]
    assert one(10, 30.0) == 2                       # revisão individual
    assert _values(db) == _values(db, basis="knowledge") == [0.0, 5.0, 30.0, 15.0]
    assert _rows(db)[2] == first                    # horário/geração original preservados
    count = len(_rows(db))
    assert one(10, 30.0) == 2 and len(_rows(db)) == count   # retry sem mudança: nada novo
    clock = feature_store.store_snapshot(db)["generation"]
    with pytest.raises(ValueError):
        one(20, "texto")                             # erro: nada gravado, geração não consumida
    assert len(_rows(db)) == count and feature_store.store_snapshot(db)["generation"] == clock


def test_v4_01_legacy_without_generation_stays_unknown(tmp_path):
    db = _db(tmp_path / "legacy.sqlite")
    feature_store.save_values(db, [_record(0, 1.0)])
    with db.engine.begin() as conn:
        conn.execute(text("UPDATE _feature_values SET recorded_at_utc = NULL, generation = NULL"))
    assert _values(db) == [1.0] and _values(db, basis="knowledge") == []


# --- V4-02: commit em voo, rollback e conhecimento no mesmo corte ----------------------------------

def _inflight(db, records, *, fail=False):
    """Escritor em outra thread/conexão, parado depois do INSERT e antes do commit."""
    inserted, release = threading.Event(), threading.Event()
    errors = []

    def hook(_conn, _counts):
        inserted.set()
        assert release.wait(30)
        if fail:
            raise RuntimeError("falha injetada antes do commit")

    def run():
        try:
            feature_store.save_values(db, records, transaction_hook=hook)
        except Exception as exc:  # noqa: BLE001 - registrado para o teste
            errors.append(exc)

    thread = threading.Thread(target=run)
    thread.start()
    assert inserted.wait(30)
    return release, thread, errors


@pytest.mark.parametrize("fail", [False, True])
def test_v4_02_inflight_revision_never_enters_an_open_snapshot(tmp_path, fail):
    db = _db(tmp_path / f"inflight{int(fail)}.sqlite")
    feature_store.save_values(db, [_record(m, v) for m, v in ((0, 1.0), (5, 2.0), (10, 3.0))])
    reader = DatabaseManager(db_path=db.db_path)            # conexão independente
    release, thread, errors = _inflight(db, [_record(10, 300.0), _record(15, 4.0)], fail=fail)
    snap = feature_store.store_snapshot(reader)
    cut = pd.Timestamp.now(tz="UTC")
    before_research = _values(reader, generation_through=snap["generation"])
    before_knowledge = _values(reader, basis="knowledge", as_of=cut)
    release.set()
    thread.join(30)
    assert bool(errors) == fail
    assert before_research == [1.0, 2.0, 3.0]
    assert _values(reader, generation_through=snap["generation"]) == before_research   # página seguinte
    assert _values(reader, basis="knowledge", as_of=cut) == before_knowledge == [1.0, 2.0, 3.0]
    assert _values(reader, basis="knowledge", as_of=cut, knowledge_through=snap["taken_at"]) == [1.0, 2.0, 3.0]
    expected = [1.0, 2.0, 3.0] if fail else [1.0, 2.0, 300.0, 4.0]
    assert _values(reader) == expected and _values(reader, basis="knowledge") == expected


def test_v4_02_lost_commit_stamp_is_repaired_conservatively_by_the_next_writer(tmp_path, monkeypatch):
    db = _db(tmp_path / "stamp.sqlite")
    real = feature_store._stamp_committed
    monkeypatch.setattr(feature_store, "_stamp_committed", lambda *_a: None)  # interrupção após o commit
    feature_store.save_values(db, [_record(0, 1.0)])
    assert _values(db) == [1.0] and _values(db, basis="knowledge") == []    # confirmado, carimbo ausente
    monkeypatch.setattr(feature_store, "_stamp_committed", real)
    before_repair = pd.Timestamp.now(tz="UTC")
    feature_store.save_values(db, [_record(5, 2.0)])
    assert _values(db, basis="knowledge") == [1.0, 2.0]
    assert _values(db, basis="knowledge", as_of=before_repair) == []          # nunca conhecido antes do reparo
