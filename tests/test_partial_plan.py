from dataclasses import replace
from pathlib import Path
import pytest
from mt5_extracao.composition import Composition,native
from mt5_extracao.composition.partials import preview_plan
from mt5_extracao.robots import catalog,mql
from tests.test_volume_sizing import robot as lots_robot,info,db
from tests.test_s09_native_profile import robot as legacy_robot
from tests.test_ui_preset_workflow import view,named

@pytest.fixture
def robot(lots_robot,tmp_path,monkeypatch):
    source=tmp_path/'partial.mq5';source.write_bytes(Path(lots_robot.path).read_bytes()+b'\n// reviewed partial fixture\n')
    inputs=[mql.MqlInput('InpPartialStages','int','0'),mql.MqlInput('InpPartialBreakeven','bool','false')]
    for i in (1,2):
        inputs += [mql.MqlInput(f'InpPartial{name}{i}',kind,default) for name,kind,default in
                   [('Trigger','double',str(.5*i)),('Quantity','double','.5'),('Mode','int','0'),('Base','int',str(i-1))]]
    out=replace(lots_robot,version='1.20',path=str(source),files=[str(source)],inputs=list(lots_robot.inputs)+inputs)
    monkeypatch.setattr(native,'PARTIALS_APPROVED_FINGERPRINT',catalog.fingerprint(out))
    return out

def plan(quantity=1,mode='staged_lots',base1=0,base2=1):
    return dict(mode=mode,params=dict(count=2,trigger1_r=.5,quantity1=quantity,base1=base1,
                trigger2_r=1.,quantity2=quantity,base2=base2,breakeven_after_confirmation=True))

@pytest.mark.parametrize('initial,quantity,minimum,step,expected',[(3,1,1,1,[2,1]),(.3,.1,.01,.01,[.2,.1])])
def test_decimal_schedule_initial_and_remaining(initial,quantity,minimum,step,expected):
    out=preview_plan(plan(quantity),volume=initial,minimum=minimum,maximum=100,step=step)
    assert [r['remaining'] for r in out]==expected
    # Último saldo é do alvo/SL; as duas reduções não são novas operações.
    fractional=preview_plan(plan(.5,'staged_fraction',base2=1),volume=initial,minimum=.01,maximum=100,step=.01)
    assert fractional[1]['reduction']<=fractional[0]['remaining']*.5

def test_infeasible_indivisible_half_and_invalid_sum_do_not_approve_be():
    for p in [dict(mode='fixed_r',params=dict(trigger_r=1,fraction=.5,breakeven_after_confirmation=True)),plan(.5,'staged_fraction')]:
        with pytest.raises(ValueError,match='inviável'):
            preview_plan(p,volume=1,minimum=1,maximum=100,step=1)
    with pytest.raises(ValueError,match='Soma'):
        preview_plan(plan(.6,'staged_fraction',base2=0),volume=3,minimum=.01,maximum=100,step=.01)

def test_native_profile_binds_explicit_units_and_preserves_old_profile(robot,lots_robot):
    data=native.default_composition(robot,symbol='EURUSD',timeframe='M5').to_dict();data['partial_exit_plan']=plan()
    out=native.S09NativeAdapter(Composition(data),robot).describe('manual_partial')
    assert out['profile_version']==3 and len(out['receipt_fields'])==22
    assert out['expected_native_inputs']['InpPartialStages']==2 and out['expected_native_inputs']['InpPartialBreakeven'] is True
    assert out['capabilities']['bindings']['partial_exit_plan.params.quantity1']['unit']=='lot'
    with pytest.raises(ValueError,match='partial_exit_plan'):native.S09NativeAdapter(Composition(data),lots_robot)

def test_ui_partial_schedule_persists_reopens_and_rejects_no_saldo(view):
    view.ui.app.mt5_connector=type('Connector',(),{'get_symbol_info':lambda self,symbol:info()})()
    view.reopen.invoke();named(view)
    view.lot_mode.set('Volume fixo');view.fixed_volume.set('3')
    view.partial_mode.set('Lotes');view.partial_count.set('2')
    for i in (1,2):view.partial_fields[f'quantity{i}'].set('1')
    view.partial_be.set(True)
    assert not view.preview_error,view.partial_preview.get()
    view.save.invoke();assert not view.dirty,view.status.get()
    view.reopen.invoke();assert view.partial_count.get()=='2' and view.partial_be.get()
    assert '3.0 → 2.0 → 1.0' in view.partial_preview.get()
    view.partial_fields['quantity2'].set('2')
    assert view.preview_error and view.backtest.instate(['disabled'])
