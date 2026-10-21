"""Percurso create/request/evaluate/status/access real, sem MT5 ou banco do dono."""
import json

from mt5_extracao import cli
from mt5_extracao.database_manager import DatabaseManager
from tests.experiment_helpers import setup
from tests.test_cli_agent import run


def test_cli_manual_protocol_request_evaluate_holdout_and_reopen_json(tmp_path,capsys,monkeypatch):
    path=tmp_path/"experiment.sqlite"
    db=DatabaseManager(db_path=str(path))
    definition,composition,_=setup(db)
    db.close()
    config=tmp_path/"config.ini"
    config.write_text(f"[DATABASE]\ntype=sqlite\npath={path}\n[APP]\nnotifications=false\n",encoding="utf-8")
    protocol_file=tmp_path/"protocol.json";protocol_file.write_text(json.dumps(definition),encoding="utf-8")
    composition_file=tmp_path/"composition.json";composition_file.write_text(composition.to_json(),encoding="utf-8")
    monkeypatch.setattr(cli.Context,"connector",lambda *_: (_ for _ in ()).throw(AssertionError("MT5 não autorizado")))
    base=["--config",str(config),"--json","experiments"]
    created=run(capsys,base+["create","--file",str(protocol_file)])
    assert created["ok"],created
    pid=created["data"]["protocol_id"]
    requested=run(capsys,base+["request","--protocol",pid,"--key","manual-1","--composition",str(composition_file),"--role","holdout"])
    rid=requested["data"]["request_id"]
    observed=run(capsys,base+["evaluate",rid])
    assert observed["ok"] and observed["data"]["request"]["state"]=="run_recorded",observed
    assert not observed["data"]["ledger_qualified"] and not observed["data"]["qualified_final_holdout"]
    again=run(capsys,base+["evaluate",rid])
    assert again["data"]["run"]["run_id"]==observed["data"]["run"]["run_id"]
    access=run(capsys,base+["holdout_access",pid,"--key","adjustment","--purpose","tuning"])
    assert access["data"]["recorded"]
    state=run(capsys,base+["status",pid])
    assert state["data"]["exposure"]["holdout"]["status"]=="used_for_tuning"
    assert "metrics" not in state["data"]


def test_cli_rejects_duplicate_json_keys(tmp_path,capsys):
    source=tmp_path/"invalid.json";source.write_text('{"seed":1,"seed":2}')
    config=tmp_path/"config.ini"
    config.write_text(f"[DATABASE]\ntype=sqlite\npath={tmp_path/'private.sqlite'}\n",encoding="utf-8")
    result=run(capsys,["--config",str(config),"--json","experiments","create","--file",str(source)])
    assert not result["ok"] and "duplicada" in result["errors"][0]
