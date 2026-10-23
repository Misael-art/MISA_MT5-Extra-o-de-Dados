"""V5-01 na consulta pública: carimbo em voo não altera páginas nem corte coberto (serviço/SDK/CLI)."""
import json

import pandas as pd
from sqlalchemy import event

from mt5_extracao import cli, sdk
from mt5_extracao.analytics import datasets, feature_store, query_service
from mt5_extracao.database_manager import DatabaseManager
from tests.test_v4_store_contract import SYMBOL, TF, VER, _record
from tests.test_v5_store_contract import _in_thread, _pause_on

FEATURES = ["price.return_1", "price.return_5"]


def _seed(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "v5.sqlite"))
    rows = []
    for feature in FEATURES:
        feature_store.register_definition(db, feature, "probe", VER, {}, [], value_type="NUMBER")
        rows += [dict(_record(m, v), feature_id=feature) for m, v in ((0, 1.0), (5, 2.0), (10, 3.0))]
    feature_store.save_values(db, rows)
    datasets.identity(db)
    return db


def _series(db, **kw):
    args = dict(symbol=SYMBOL, timeframe=TF, feature_ids=FEATURES, as_of="2100-01-01T00:00Z", basis="knowledge",
                algorithm_version=VER)
    args.update(kw)
    return query_service.series(db, **args)


def test_v5_pending_stamp_between_pages_and_same_cut(tmp_path, capsys):
    db = _seed(tmp_path)
    reader = DatabaseManager(db_path=db.db_path, read_only=True)
    statements = []
    event.listen(reader.engine, "before_cursor_execute", lambda c, cu, s, *a: statements.append(s.split()[0].upper()))
    reached, release, remove = _pause_on(db.engine, "UPDATE _feature_store_generations")
    thread, errors = _in_thread(lambda: feature_store.save_values(
        db, [dict(_record(10, 300.0), feature_id=f) for f in FEATURES]))
    assert reached.wait(30)
    first = _series(reader, page_size=1)
    cut = first["snapshot"]["taken_at"]
    same_cut_before = [r["value"] for r in _series(reader, as_of=cut)["values"]]
    release.set()
    thread.join(30)
    remove()
    assert not errors
    values, cursor = [r["value"] for r in first["values"]], first["page"]["next_cursor"]
    while cursor:
        page = _series(reader, page_size=1, cursor=cursor)
        assert page["status"] == "ok", page["errors"]
        values += [r["value"] for r in page["values"]]
        cursor = page["page"]["next_cursor"]
    assert values == [1.0, 2.0, 3.0, 1.0, 2.0, 3.0]                 # sem 300 nas páginas
    assert same_cut_before == [1.0, 2.0, 3.0, 1.0, 2.0, 3.0]
    horizon = first["knowledge_horizon"]
    assert horizon and any("Conhecimento confirmado só até" in r for r in first["reasons"])
    covered = [r["value"] for r in _series(reader, as_of=horizon)["values"]]
    assert covered == same_cut_before                                  # corte coberto não mudou
    now = _series(reader)
    assert [r["value"] for r in now["values"]] == [1.0, 2.0, 300.0, 1.0, 2.0, 300.0]
    assert now["knowledge_horizon"] > horizon
    via_sdk = sdk.query_series(reader, symbol=SYMBOL, timeframe=TF, feature_ids=FEATURES, as_of=horizon,
                               basis="knowledge", algorithm_version=VER)
    assert [r["value"] for r in via_sdk["values"]] == covered
    assert set(statements) <= {"SELECT", "PRAGMA", "WITH"}
    reader.engine.dispose()
    config = tmp_path / "cli.ini"
    config.write_text(f"[DATABASE]\ntype = sqlite\npath = {db.db_path}\n", encoding="utf-8")
    code = cli.main(["--config", str(config), "--json", "analytics", "features", "series", "--symbol", SYMBOL,
                     "--tf", TF, "--feature", FEATURES[0], "--feature", FEATURES[1], "--as-of", horizon,
                     "--basis", "knowledge"])
    out = json.loads(capsys.readouterr().out)["data"]["envelope"]
    assert code == 0 and [r["value"] for r in out["values"] if r["algorithm_version"] == VER] == covered
