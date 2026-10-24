"""Os mesmos Default/Manual/compiler/store pelo SDK, sem editor Tk."""
from tests.test_s09_native_profile import robot
from tests.test_default_presets import db
from mt5_extracao.automation import open_workflow


def test_default_manual_revision_name_export_import_and_selection(db,robot,tmp_path,monkeypatch):
    with open_workflow(db,{}) as workflow:
        monkeypatch.setattr(workflow.controller,"_robot",lambda key:robot)
        def call(method,**params):
            result=workflow.call(dict(id=str(len(workflow.receipts)),method="presets."+method,params=params))
            assert result["ok"],result
            return result["data"]
        context=dict(robot=robot.key,symbol="EURUSD",timeframe="M5")
        default=call("default",**context)
        own=call("save",robot=robot.key,source_id=default["identity"]["id"],name="Agente teste")
        pid=own["identity"]["id"]
        call("rename",preset_id=pid,name="Nome revisto")
        assert call("get",preset_id=pid)["identity"]["label"]=="Nome revisto"
        assert any(p["identity"]["label"]=="Nome revisto" for p in call("list",**context)["presets"])
        comp=own["composition"];comp["strategy"]["params"]["fast"]=21
        changed=call("revise",robot=robot.key,preset_id=pid,composition=comp,expected_revision=1)
        assert changed["identity"]["revision"]==2
        selected=call("select",**context,preset_id=pid,revision=2)
        assert selected["selected"]["identity"]["revision"]==2
        export=call("export",robot=robot.key,preset_id=pid,directory=str(tmp_path/"exports"),test_context=dict(start="2026-10-14",end="2026-10-15",deposit=100000))
        imported=call("import",robot=robot.key,manifest=export["manifest_path"])
        assert imported["identity"]["id"]==pid
        assert call("get",preset_id=default["identity"]["id"])["composition"]==default["composition"]
