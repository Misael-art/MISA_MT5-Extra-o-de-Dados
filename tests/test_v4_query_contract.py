"""V4-01/02 na consulta pública: escritor individual e transação em voo entre páginas."""
import pandas as pd

from mt5_extracao.analytics import feature_store, query_service
from mt5_extracao.database_manager import DatabaseManager
from tests.test_v4_store_contract import FID, SYMBOL, TF, VER, _inflight, _record


def _base(tmp_path):
    from mt5_extracao.analytics import datasets
    db = DatabaseManager(db_path=str(tmp_path / "pages.sqlite"))
    feature_store.register_definition(db, FID, "probe", VER, {}, [], value_type="NUMBER")
    feature_store.save_values(db, [_record(m, v) for m, v in ((0, 1.0), (5, 2.0), (10, 3.0))])
    datasets.identity(db)
    return db




def _all_pages(db, first, basis="research", as_of="2100-01-01T00:00Z"):
    values, cursor = [r["value"] for r in first["values"]], first["page"]["next_cursor"]
    while cursor:
        page = query_service.series(db, symbol=SYMBOL, timeframe=TF, feature_ids=[FID], as_of=as_of, basis=basis,
                                    page_size=1, cursor=cursor)
        assert page["status"] == "ok", page["errors"]
        values += [r["value"] for r in page["values"]]
        cursor = page["page"]["next_cursor"]
    return values


def test_v4_service_pages_ignore_single_writer_after_first_page(tmp_path):
    db = _base(tmp_path)
    for basis in ("research", "knowledge"):
        first = query_service.series(db, symbol=SYMBOL, timeframe=TF, feature_ids=[FID], as_of="2100-01-01T00:00Z",
                                     basis=basis, page_size=1)
        r = _record(15, 4.0) if basis == "research" else _record(20, 5.0)
        feature_store.save_value(db, r.pop("symbol"), r.pop("timeframe"), r.pop("feature_id"),
                                 r.pop("algorithm_version"), r.pop("observation_time"), r.pop("value"), **r)
        assert _all_pages(db, first, basis) == [1.0, 2.0, 3.0] + ([4.0] if basis == "knowledge" else [])


def test_v4_service_inflight_revision_does_not_mix_pages(tmp_path):
    db = _base(tmp_path)
    reader = DatabaseManager(db_path=db.db_path, read_only=True)
    release, thread, errors = _inflight(db, [_record(10, 300.0)])
    cut = pd.Timestamp.now(tz="UTC").isoformat()
    first = query_service.series(reader, symbol=SYMBOL, timeframe=TF, feature_ids=[FID], as_of="2100-01-01T00:00Z",
                                 page_size=1)
    knowledge_before = query_service.series(reader, symbol=SYMBOL, timeframe=TF, feature_ids=[FID], as_of=cut,
                                            basis="knowledge")["values"]
    release.set()
    thread.join(30)
    assert not errors
    assert _all_pages(reader, first) == [1.0, 2.0, 3.0]          # antes: [1, 2, 300]
    knowledge_after = query_service.series(reader, symbol=SYMBOL, timeframe=TF, feature_ids=[FID], as_of=cut,
                                           basis="knowledge")["values"]
    assert [r["value"] for r in knowledge_after] == [r["value"] for r in knowledge_before] == [1.0, 2.0, 3.0]
