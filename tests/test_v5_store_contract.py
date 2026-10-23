"""V5-01/V5-02: fronteira de conhecimento confirmada e exclusão antes de escolher a revisão."""
import threading

import pandas as pd
import pytest
from sqlalchemy import event

from mt5_extracao.analytics import feature_store
from mt5_extracao.database_manager import DatabaseManager
from tests.test_v4_store_contract import FID, VER, _db, _record, _values


def _pause_on(engine, prefix, when="after_cursor_execute"):
    """Barreira determinística: pausa o escritor antes/depois de executar a instrução com o prefixo."""
    reached, release = threading.Event(), threading.Event()

    def hook(_conn, _cursor, statement, *_args):
        if statement.lstrip().startswith(prefix) and not reached.is_set():
            reached.set()
            assert release.wait(30)

    event.listen(engine, when, hook)
    return reached, release, lambda: event.remove(engine, when, hook)


def _in_thread(fn):
    errors = []

    def run():
        try:
            fn()
        except Exception as exc:  # noqa: BLE001 - registrado para o teste
            errors.append(exc)

    thread = threading.Thread(target=run)
    thread.start()
    return thread, errors


def test_v5_01_pending_stamp_never_changes_a_covered_cut(tmp_path):
    db = _db(tmp_path / "stamp.sqlite")
    feature_store.save_values(db, [_record(m, v) for m, v in ((0, 1.0), (5, 2.0), (10, 3.0))])
    reader = DatabaseManager(db_path=db.db_path)
    reached, release, remove = _pause_on(db.engine, "UPDATE _feature_store_generations")
    thread, errors = _in_thread(lambda: feature_store.save_values(db, [_record(10, 300.0)]))
    assert reached.wait(30)                           # dados confirmados, carimbo em voo
    snap = feature_store.store_snapshot(reader)
    cut = pd.Timestamp.now(tz="UTC")
    assert _values(reader) == [1.0, 2.0, 300.0]       # research já vê o dado confirmado
    before = _values(reader, basis="knowledge", as_of=cut, knowledge_frontier=snap["knowledge_frontier"])
    assert before == [1.0, 2.0, 3.0]
    release.set()
    thread.join(30)
    remove()
    # Mesmo cursor (fronteira fixada) e qualquer corte <= fronteira declarada: nada muda.
    assert _values(reader, basis="knowledge", as_of=cut, knowledge_frontier=snap["knowledge_frontier"]) == before
    assert _values(reader, basis="knowledge", as_of=snap["knowledge_frontier"]) == before
    later = feature_store.store_snapshot(reader)
    assert later["knowledge_frontier"] > snap["knowledge_frontier"]
    assert _values(reader, basis="knowledge") == [1.0, 2.0, 300.0]


def test_v5_01_frontier_is_monotonic_and_lost_stamp_is_repaired_after_it(tmp_path, monkeypatch):
    db = _db(tmp_path / "front.sqlite")
    feature_store.save_values(db, [_record(0, 1.0)])
    first = feature_store.store_snapshot(db)["knowledge_frontier"]
    real = feature_store._stamp_committed
    monkeypatch.setattr(feature_store, "_stamp_committed", lambda *_a: None)
    feature_store.save_values(db, [_record(5, 2.0)])           # carimbo perdido
    assert feature_store.store_snapshot(db)["knowledge_frontier"] == first
    assert _values(db, basis="knowledge") == [1.0]
    monkeypatch.setattr(feature_store, "_stamp_committed", real)
    feature_store.save_values(db, [_record(10, 3.0)])
    assert feature_store.store_snapshot(db)["knowledge_frontier"] > first
    assert _values(db, basis="knowledge", as_of=first) == [1.0]  # reparo nunca antecipa
    assert _values(db, basis="knowledge") == [1.0, 2.0, 3.0]


def _single(db, minute, value):
    r = _record(minute, value)
    return feature_store.save_value(db, r.pop("symbol"), r.pop("timeframe"), r.pop("feature_id"),
                                    r.pop("algorithm_version"), r.pop("observation_time"), r.pop("value"), **r)


@pytest.mark.parametrize("second", ["single", "batch"])
@pytest.mark.parametrize("same", [False, True])
def test_v5_02_concurrent_writers_queue_instead_of_colliding(tmp_path, second, same):
    db = _db(tmp_path / f"queue-{second}-{int(same)}.sqlite")
    feature_store.save_values(db, [_record(10, 3.0)])
    other = DatabaseManager(db_path=db.db_path)
    # Como na sonda do revisor: o 1º já leu a revisão vigente e pára antes de reservar a geração.
    reached, release, remove = _pause_on(db.engine, "UPDATE _feature_store_clock SET generation",
                                         when="before_cursor_execute")
    first, errors_a = _in_thread(lambda: _single(db, 10, 30.0))
    assert reached.wait(30)                           # 1º escritor travado depois do INSERT
    value = 30.0 if same else 300.0
    write = (lambda: _single(other, 10, value)) if second == "single" else \
        (lambda: feature_store.save_values(other, [_record(10, value)]))
    read_current = threading.Event()

    def saw_read(_conn, _cursor, statement, *_args):
        if "FROM _feature_values" in statement and "revision" in statement:
            read_current.set()

    event.listen(other.engine, "before_cursor_execute", saw_read)
    later, errors_b = _in_thread(write)
    # Sem exclusão antecipada, o 2º lê a revisão vigente (1) enquanto o 1º está travado: colisão.
    # Com a exclusão, fica bloqueado antes dessa leitura; o limite curto cobre esse caso.
    read_current.wait(3)
    release.set()
    first.join(30)
    later.join(30)
    remove()
    assert errors_a == [] and errors_b == []          # fila, não IntegrityError
    history = feature_store.query_history(db, feature_id=FID)
    assert history["numeric_value"].tolist() == ([3.0, 30.0] if same else [3.0, 30.0, 300.0])
    assert history["revision"].tolist() == list(range(1, len(history) + 1))


def test_v5_01_stamp_rollback_keeps_frontier_and_next_writer_repairs(tmp_path, monkeypatch):
    db = _db(tmp_path / "rollback.sqlite")
    feature_store.save_values(db, [_record(0, 1.0)])
    frontier = feature_store.store_snapshot(db)["knowledge_frontier"]

    real_update = feature_store._stamp_committed

    def failing(database, generation):
        listener = lambda c, cu, statement, *a: (_ for _ in ()).throw(RuntimeError("falha no carimbo")) \
            if statement.lstrip().startswith("UPDATE _feature_store_clock SET knowledge_frontier") else None
        event.listen(database.engine, "before_cursor_execute", listener)
        try:
            real_update(database, generation)
        finally:
            event.remove(database.engine, "before_cursor_execute", listener)

    monkeypatch.setattr(feature_store, "_stamp_committed", failing)
    with pytest.raises(RuntimeError):
        feature_store.save_values(db, [_record(5, 2.0)])      # dados confirmados, carimbo desfeito
    assert feature_store.store_snapshot(db)["knowledge_frontier"] == frontier
    assert _values(db) == [1.0, 2.0] and _values(db, basis="knowledge") == [1.0]
    monkeypatch.setattr(feature_store, "_stamp_committed", real_update)
    feature_store.save_values(db, [_record(10, 3.0)])
    assert _values(db, basis="knowledge") == [1.0, 2.0, 3.0]
