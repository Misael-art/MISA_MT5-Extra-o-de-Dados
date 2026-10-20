from dataclasses import replace
from pathlib import Path
import pytest
from mt5_extracao.composition import Composition,native
from mt5_extracao.robots import catalog,mql
from tests.test_partial_plan import robot as partial_robot
from tests.test_volume_sizing import robot as lots_robot,db
from tests.test_s09_native_profile import robot as legacy_robot
from tests.test_ui_preset_workflow import view,named

@pytest.fixture
def robot(partial_robot,tmp_path,monkeypatch):
    source=tmp_path/'temporal.mq5';source.write_bytes(Path(partial_robot.path).read_bytes()+b'\n// temporal test profile\n')
    inputs=[mql.MqlInput(name,kind,'2' if key=='money_digits' else '0') for key,(name,kind,unit) in native.TIME_INPUTS.items()]
    out=replace(partial_robot,version='1.30',path=str(source),files=[str(source)],inputs=partial_robot.inputs+inputs)
    monkeypatch.setattr(native,'TEMPORAL_APPROVED_FINGERPRINT',catalog.fingerprint(out));return out

def selection(**kwargs):
    from mt5_extracao.strategies.temporal import TemporalPolicy
    return dict(mode='temporal_v1',params=TemporalPolicy(max_seconds=30,**kwargs).to_dict())

def test_profile_maps_all_inputs_and_old_profile_does_not_gain_support(robot,partial_robot):
    data=native.default_composition(robot,symbol='EURUSD',timeframe='M5').to_dict();data['conditional_policy']=selection(gain_seconds=10,loss_seconds=20,tie_seconds=25)
    out=native.native_adapter(Composition(data),robot).describe('time_manual')
    assert len(out['parameters'])==30 and out['profile_version']==4
    assert out['expected_native_inputs']['InpTimeMaxSeconds']==30 and out['expected_native_inputs']['InpTimeGainSeconds']==10
    assert out['capabilities']['bindings']['conditional_policy.params.minimum_profit']['unit']=='account_money'
    assert out['expected_native_inputs']['InpTimeMoneyDigits']==2
    with pytest.raises(ValueError,match='conditional_policy'):native.native_adapter(Composition(data),partial_robot)

def test_time_editor_is_default_readonly_then_named_persisted_and_invalid_deadline_blocked(view):
    assert view.temporal_enabled and view.time_toggle.instate(['disabled'])
    named(view);view.time_on.set(True)
    view.time_fields['max_seconds'].set('30');view.time_fields['flat_band'].set('0.01')
    assert not view.preview_error,view.lot_preview.get()
    view.save.invoke();assert not view.dirty,view.status.get()
    view.reopen.invoke();assert view.time_on.get() and view.time_fields['max_seconds'].get()=='30'
    assert view.current.composition.to_dict()['conditional_policy']['params']['flat_band']==.01
    view.time_fields['max_seconds'].set('-1');assert view.preview_error and view.backtest.instate(['disabled'])
