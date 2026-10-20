from dataclasses import replace
from pathlib import Path
import pytest
from mt5_extracao.composition import Composition,native
from mt5_extracao.robots import catalog,mql
from mt5_extracao.strategies.temporal_donchian import DonchianTemporalPolicy
from tests.test_temporal_er_native import robot as er_robot
from tests.test_temporal_native import robot as temporal_robot
from tests.test_partial_plan import robot as partial_robot
from tests.test_volume_sizing import robot as lots_robot,db
from tests.test_s09_native_profile import robot as legacy_robot
from tests.test_ui_preset_workflow import view,named

@pytest.fixture
def robot(er_robot,tmp_path,monkeypatch):
    source=tmp_path/'dc.mq5';source.write_bytes(Path(er_robot.path).read_bytes()+b'\n// Donchian reviewed test profile\n')
    inputs=[mql.MqlInput(name,kind,'0') for name,kind,unit in native.DONCHIAN_INPUTS.values()]
    out=replace(er_robot,version='1.50',path=str(source),files=[str(source)],inputs=er_robot.inputs+inputs)
    monkeypatch.setattr(native,'DONCHIAN_TEMPORAL_APPROVED_FINGERPRINT',catalog.fingerprint(out));return out

def selection():return dict(mode='temporal_donchian_v1',params=DonchianTemporalPolicy(channel_idle_seconds=30).to_dict())

def test_new_profile_maps_33_inputs_and_old_er_profile_stays_blocked(robot,er_robot):
    data=native.default_composition(robot,symbol='EURUSD',timeframe='M5').to_dict();data['conditional_policy']=selection()
    out=native.native_adapter(Composition(data),robot).describe('dc_manual')
    assert len(out['parameters'])==33 and out['profile_version']==6 and out['expected_native_inputs']['InpTimeDonchianIdleSeconds']==30
    assert out['capabilities']['bindings']['conditional_policy.params.channel_idle_seconds']['unit']=='second'
    with pytest.raises(ValueError,match='conditional_policy'):native.native_adapter(Composition(data),er_robot)

def test_default_readonly_named_channel_editor_reopens_and_invalid_time_blocks(view):
    assert view.donchian_enabled and view.dc_toggle.instate(['disabled'])
    named(view);view.time_on.set(True);view.dc_on.set(True);view.dc_seconds.set('30')
    assert not view.preview_error,view.lot_preview.get()
    view.save.invoke();assert not view.dirty,view.status.get();view.reopen.invoke()
    assert view.dc_on.get() and view.current.composition.to_dict()['conditional_policy']==selection()
    view.dc_seconds.set('0');assert view.preview_error and view.backtest.instate(['disabled'])
    view.reopen.invoke();view.dc_on.set(False);view.time_fields['max_seconds'].set('60')
    assert view.composition_from_fields().to_dict()['conditional_policy']['mode']=='temporal_v1'
