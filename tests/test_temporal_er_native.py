from dataclasses import replace
from pathlib import Path
import pytest
from mt5_extracao.composition import Composition, native
from mt5_extracao.robots import catalog, mql
from mt5_extracao.strategies.temporal_er import EfficiencyTemporalPolicy
from tests.test_temporal_native import robot as temporal_robot
from tests.test_partial_plan import robot as partial_robot
from tests.test_volume_sizing import robot as lots_robot, db
from tests.test_s09_native_profile import robot as legacy_robot
from tests.test_ui_preset_workflow import view, named

@pytest.fixture
def robot(temporal_robot,tmp_path,monkeypatch):
    source=tmp_path/'er.mq5';source.write_bytes(Path(temporal_robot.path).read_bytes()+b'\n// ER reviewed test profile\n')
    inputs=[mql.MqlInput(name,kind,'0') for name,kind,unit in native.ER_INPUTS.values()]
    out=replace(temporal_robot,version='1.40',path=str(source),files=[str(source)],inputs=temporal_robot.inputs+inputs)
    monkeypatch.setattr(native,'ER_TEMPORAL_APPROVED_FINGERPRINT',catalog.fingerprint(out));return out

def selection():return dict(mode='temporal_er_v1',params=EfficiencyTemporalPolicy(er_min_seconds=30,maximum_er=.4).to_dict())

def test_native_profile_preserves_old_profiles_and_binds_er(robot,temporal_robot):
    data=native.default_composition(robot,symbol='EURUSD',timeframe='M5').to_dict();data['conditional_policy']=selection()
    out=native.native_adapter(Composition(data),robot).describe('er_manual')
    assert len(out['parameters'])==32 and out['profile_version']==5
    assert out['expected_native_inputs']['InpTimeERMinSeconds']==30
    assert out['expected_native_inputs']['InpTimeERMaximum']==.4
    assert out['capabilities']['bindings']['conditional_policy.params.maximum_er']['unit']=='fraction'
    with pytest.raises(ValueError,match='conditional_policy'):native.native_adapter(Composition(data),temporal_robot)
    off=native.native_adapter(native.default_composition(robot,symbol='EURUSD',timeframe='M5'),robot).describe('default')
    assert off['expected_native_inputs']['InpTimeERMinSeconds']==0

def test_default_then_named_manual_er_save_reopen_and_invalid_values(view):
    assert view.er_enabled and view.er_toggle.instate(['disabled'])
    named(view);view.time_on.set(True);view.er_on.set(True)
    view.er_fields['er_min_seconds'].set('30');view.er_fields['maximum_er'].set('0.4')
    assert not view.preview_error,view.lot_preview.get()
    view.save.invoke();assert not view.dirty,view.status.get()
    view.reopen.invoke();assert view.er_on.get()
    assert view.current.composition.to_dict()['conditional_policy']==selection()
    for key,value in [('maximum_er','1.1'),('er_min_seconds','-1')]:
        view.er_fields[key].set(value);assert view.preview_error and view.backtest.instate(['disabled'])
        view.reopen.invoke();assert not view.preview_error
    view.er_on.set(False);view.time_fields['max_seconds'].set('60')
    assert view.composition_from_fields().to_dict()['conditional_policy']['mode']=='temporal_v1'
