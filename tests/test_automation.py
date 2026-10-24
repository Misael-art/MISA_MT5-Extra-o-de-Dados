"""SDK de agentes por JSON: contratos, composição e CLI reais, sem MT5/Tk."""
import io
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from mt5_extracao.automation import open_workflow, protocol
from mt5_extracao.automation.catalog import command_tree
from mt5_extracao.automation.commands import serve
from mt5_extracao.automation.service import Workflow, run_cli


@pytest.mark.parametrize("body", ['{"a":1,"a":2}', '{"a":NaN}', '{"a":Infinity}', '{"a":-Infinity}'])
def test_strict_json(body):
    with pytest.raises(ValueError):
        protocol.loads(body)


@pytest.mark.parametrize("value", [True, False, "2", float("nan"), float("inf"), None, []])
def test_numbers_reject_coercion(value):
    with pytest.raises(ValueError):
        protocol.number(value, "n", 1, 3)


def test_recursive_catalog_is_entire_actual_parser_and_does_not_connect():
    tree = command_tree()
    def paths(node, prefix=()):
        for child in node["subcommands"]:
            path = prefix + (child["command"],)
            yield path
            yield from paths(child, path)
    names = set(paths(tree))
    assert ("workflow", "serve") in names
    assert ("analytics", "features", "series") in names
    assert ("backtest",) in names
    assert any(p[:1] == ("experiments",) for p in names)
    assert ("trades", "report") in names
    assert all("arguments" in row for row in tree["subcommands"])


def test_cli_discovery_and_sdk_import_do_not_import_tk_or_mt5():
    code = "from mt5_extracao.sdk import open_workflow; from mt5_extracao.automation.service import Workflow; import sys; assert 'tkinter' not in sys.modules; assert 'MetaTrader5' not in sys.modules; assert Workflow.methods()['protocol']==1"
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    result = subprocess.run([sys.executable, "-m", "mt5_extracao.cli", "--json", "workflow", "methods"], capture_output=True, text=True)
    payload = json.loads(result.stdout)
    assert result.returncode == 0 and payload["ok"] and payload["data"]["protocol"] == 1


def test_call_ids_are_idempotent_and_different_payload_cannot_reuse_id(monkeypatch):
    w = open_workflow(None, {})
    calls = []
    monkeypatch.setattr(w, "_dispatch", lambda m,p: calls.append(p) or {"n":len(calls)})
    request = dict(id="one", method="dummy", params={"a":1})
    assert w.call(request) == w.call(request)
    assert len(calls) == 1
    with pytest.raises(ValueError, match="divergente"):
        w.call(dict(request, params={"a":2}))
    assert len(calls) == 1


def test_manifest_resolves_typed_refs_and_stops_after_failure(monkeypatch):
    w = open_workflow(None, {})
    seen = []
    def dispatch(method, params):
        seen.append(params)
        if method == "first": return dict(session="abc", nested=dict(value=3))
        if method == "fail": raise ValueError("recusa")
        return params
    monkeypatch.setattr(w, "_dispatch", dispatch)
    result = w.run(dict(version=1, steps=[
        dict(id="a",method="first"),
        dict(id="b",method="next",params=dict(session={"$ref":"a.session"}, n={"$ref":"a.nested.value"})),
        dict(id="c",method="fail"),dict(id="d",method="next")]))
    assert not result["ok"] and result["stopped_at"] == "c" and not result["rollback_performed"]
    assert seen[1] == dict(session="abc", n=3) and len(seen) == 3
    assert not w.run(dict(version=1,steps=[dict(id="bad",method="next",params={"x":{"$ref":"missing.x"}})]))["ok"]


@pytest.mark.parametrize("manifest", [
    dict(version=True, steps=[]), dict(version=2, steps=[]), dict(version=1, steps=[]),
    dict(version=1, steps=[dict(id="a",method="x"),dict(id="a",method="x")]),
    dict(version=1,steps=[dict(id="a",method="x"),dict(id="b",method="x",extra=1)])])
def test_manifest_shape_is_checked_before_any_effect(monkeypatch, manifest):
    w = open_workflow(None,{})
    monkeypatch.setattr(w,"_dispatch",lambda *args: pytest.fail("não pode executar"))
    with pytest.raises(ValueError): w.run(manifest)


def test_jsonl_keeps_one_receipt_per_line_after_bad_input():
    w = open_workflow(None,{})
    incoming=io.StringIO('not json\n'+json.dumps(dict(id="a",method="session.status"))+'\n'+json.dumps(dict(id="b",method="unknown"))+'\n')
    outgoing=io.StringIO();serve(w,incoming,outgoing)
    rows=[json.loads(line) for line in outgoing.getvalue().splitlines()]
    assert len(rows)==3 and not rows[0]["ok"] and rows[1]["data"]=={"sessions":[]} and not rows[2]["ok"]


def test_cli_run_uses_argv_no_shell_and_preserves_exit(monkeypatch,tmp_path):
    captured=[]
    def run(argv,**kw):
        captured.append((argv,kw));return SimpleNamespace(returncode=0,stdout=json.dumps(dict(ok=True,exit_code=0,data={},errors=[])),stderr="")
    monkeypatch.setattr(subprocess,"run",run)
    result=run_cli(["commands"],config_path=tmp_path/"my config.ini",db_override=tmp_path/"test.sqlite")
    assert result["ok"] and "shell" not in captured[0][1]
    assert captured[0][0][0]==sys.executable and captured[0][0][-1]=="commands"
    for args in (["workflow","serve"],["sh","-c","evil"],"commands"):
        with pytest.raises(ValueError):run_cli(args,config_path="cfg")
    assert len(captured)==1


def test_actual_cli_manifest_and_jsonl_offline(tmp_path):
    cfg=tmp_path/"config.ini";cfg.write_text("[DATABASE]\ntype=sqlite\n")
    common=[sys.executable,"-m","mt5_extracao.cli","--json","--config",str(cfg),"--db",str(tmp_path/"db.sqlite")]
    manifest=tmp_path/"workflow.json"
    manifest.write_text(json.dumps(dict(version=1,steps=[dict(id="a",method="cli.run",params=dict(arguments=["tables"])),dict(id="b",method="session.status")])))
    r=subprocess.run(common+["workflow","run","--file",str(manifest)],capture_output=True,text=True,timeout=45)
    assert r.returncode==0,(r.stdout,r.stderr)
    assert json.loads(r.stdout)["data"]["steps"][0]["data"]["data"]["tables"]==[]
    r=subprocess.run(common+["workflow","serve"],input='{"id":"bad","method":"unknown"}\n{"id":"good","method":"session.status"}\n',capture_output=True,text=True,timeout=45)
    assert r.returncode==0,(r.stdout,r.stderr)
    rows=[json.loads(x) for x in r.stdout.splitlines()]
    assert len(rows)==2 and not rows[0]["ok"] and rows[1]["ok"]


def test_sdk_missing_backend_is_a_structured_refusal(monkeypatch):
    from mt5_extracao import mt5_backend
    from mt5_extracao.automation.service import _reader
    monkeypatch.setattr(mt5_backend,"get_mt5",lambda:None)
    with pytest.raises(ValueError,match="Backend MT5 indisponível"):_reader()


def test_failed_cli_exit_is_not_a_successful_workflow_receipt(monkeypatch):
    w=open_workflow(None,{},config_path="cfg")
    from mt5_extracao.automation import service
    monkeypatch.setattr(service,"run_cli",lambda *a,**kw:dict(ok=False,exit_code=2,errors=["argumento recusado"]))
    result=w.call(dict(id="a",method="cli.run",params=dict(arguments=["tables"])))
    assert not result["ok"] and result["error_code"]=="cli_failed" and result["data"]["exit_code"]==2


@pytest.mark.parametrize("data", [dict(state="failed",error="compile recusado"),dict(state="preparing",wait_timed_out=True)])
def test_wait_failure_stops_manifest_without_running_next_effect(monkeypatch,data):
    w=open_workflow(None,{})
    monkeypatch.setattr(w.controller,"wait",lambda p:data)
    result=w.run(dict(version=1,steps=[dict(id="a",method="session.wait",params=dict(session="abc")),dict(id="b",method="unknown")]))
    assert not result["ok"] and result["stopped_at"]=="a" and result["steps"][0]["data"]==data


@pytest.mark.parametrize("code",[0,1])
def test_cli_doctor_preserves_bootstrap_check_list_in_normalized_receipt(monkeypatch,code):
    checks=[dict(name="probe",status="OK" if code==0 else "FAIL")]
    monkeypatch.setattr(subprocess,"run",lambda *a,**k:SimpleNamespace(returncode=code,stdout=json.dumps(checks),stderr=""))
    result=run_cli(["doctor"],config_path="cfg")
    assert result["ok"]==(code==0) and result["exit_code"]==code and result["data"]["checks"]==checks
