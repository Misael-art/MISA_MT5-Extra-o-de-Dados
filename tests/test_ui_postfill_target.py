"""E14: o modo R pós-fill percorre Manual e pacote sem executar o Tester."""
import json
from pathlib import Path

import pytest

from tests.test_default_presets import db
from tests.test_ui_preset_workflow import named, view
from mt5_extracao.composition import native
from mt5_extracao.robots import catalog, mql


@pytest.fixture
def robot(tmp_path, monkeypatch):
    source=tmp_path/"HDK_S09_Referencia.mq5"
    source.write_text("// fixture de mapper/UI; não certifica MT5\n", encoding="utf-8")
    item=catalog.Robot(native.ROBOT_KEY,catalog.MQL5,"S09",version=native.POSTFILL_ROBOT_VERSION,
                       path=str(source),files=[str(source)],inputs=[mql.MqlInput(name,kind,"0")
                       for name,kind in native.POSTFILL_INPUT_TYPES.items()])
    monkeypatch.setattr(native,"POSTFILL_APPROVED_FINGERPRINT",catalog.fingerprint(item))
    monkeypatch.setattr(native,"POSTFILL_HADOUKEN_REVISION","f"*40)
    return item


def test_manual_postfill_target_roundtrips_and_exports_distinct_native_mode(view):
    assert view.postfill_available
    assert "R pós-fill" in view.common_widgets["target_mode"].cget("values")
    named(view)
    view.common_on.set(True)
    view.common_fields["target_mode"].set("R pós-fill")
    view.common_fields["target_r"].set("2,5")
    assert view.common_widgets["target_r"].instate(["!disabled"])
    assert view.common_widgets["target_points"].instate(["disabled"])
    view.save.invoke()
    assert view.current.identity.revision==2,view.status.get()
    assert view.current.composition.to_dict()["profit_exit"]=={"mode":"postfill_r_v1","params":{"target_r":2.5}}
    view.export.invoke()
    manifests=list(Path(view.export_dir.get()).rglob("*.manifest.json"))
    assert len(manifests)==1,view.status.get()
    native_data=json.loads(manifests[0].read_text(encoding="utf-8"))["native"]
    assert native_data["parameters"]["InpTargetMode"]=="3"
    assert native_data["parameters"]["InpTargetR"]=="2.5"
    assert native_data["capabilities"]["bindings"]["profit_exit.params.target_r"]["unit"]=="initial_actual_R"
    assert not native_data["operational_acceptance"]
    view.reopen.invoke()
    assert view.common_fields["target_mode"].get()=="R pós-fill"
    assert view.execution is None
