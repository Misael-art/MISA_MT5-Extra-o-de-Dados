"""Pacote fechado/contrato e integração no catálogo, sem certificar execução nativa."""
import hashlib
import json
from pathlib import Path
import pytest
from mt5_extracao import sdk,cli
from mt5_extracao.robots import catalog,contract,mql,super_atr_research
from mt5_extracao.analytics import super_atr
from mt5_extracao.experiments.schema import digest
from tests.test_super_atr_materializer import db


@pytest.fixture
def framework(tmp_path,monkeypatch):
    root=tmp_path/'framework_fixture'
    for rel,source in [('Strategies/HDK_StrategyRobot.mqh','// Explicit fixture for packaging, not native host\n'),('Indicators/HDK_SuperATR.mqh','// Explicit fixture for packaging, not formula\n')]:
        p=root/'Include/Framework/HADOUKEN'/rel;p.parent.mkdir(parents=True,exist_ok=True);p.write_text(source)
    monkeypatch.setattr(super_atr,'source',lambda *a,**k:(dict(framework_commit='1'*40,header_sha256='2'*64),None))
    # Histórico Git simulado desta fixture explícita de empacotamento.
    from mt5_extracao.robots import super_atr_reference
    frozen={p.relative_to(root).as_posix():p.read_bytes() for p in root.rglob('*') if p.is_file()}
    monkeypatch.setattr(super_atr_reference,'_reference_bytes',lambda _,rel:frozen[rel])
    return root


def generate(tmp_path,framework,**kw):
    return sdk.create_super_atr_research_robot('SuperAtrQA',tmp_path/'package',framework_root=framework,seed_start='2025-01-01T00:00',magic=654321,**kw)


def test_package_keeps_native_host_closure_typed_inputs_and_disabled_entries(tmp_path,framework):
    files=generate(tmp_path,framework);robot=catalog.robot_from_file(files[0]);receipt=json.loads(files[4].read_text())
    assert not contract.validate(robot)
    assert {i.name:i.type for i in robot.inputs if i.name in ('InpSeedStart','InpBaseTF','InpResearchOrders')}==dict(InpSeedStart='datetime',InpBaseTF='ENUM_TIMEFRAMES',InpResearchOrders='bool')
    values={i.name:i.default for i in robot.inputs}
    assert values['InpResearchOrders']=='false' and values['InpMoveGuard']=='false'
    assert robot.risk.facts['mandatory_stop'] is True
    assert len(robot.files)==4 and len(robot.missing_includes)==0
    assert receipt['research_contract_sha256']==digest(receipt['research_contract'])
    assert receipt['research_contract']['entries_enabled_default'] is False
    assert receipt['research_contract']['predictive_qualified'] is False
    for name,expected in receipt['research_closure'].items():assert hashlib.sha256((tmp_path/'package'/name).read_bytes()).hexdigest()==expected
    assert 'OrderSend(' not in files[0].read_text() and 'HDK_STRATEGY_OBSERVE_ENTRIES' in files[0].read_text()
    assert mql.parse_set(files[3].read_text())['InpMagicNumber']=='654321'


@pytest.mark.parametrize('kwargs',[
    dict(seed_start='2025-01-01'),dict(seed_start='2025-01-01T00:00Z'),dict(seed_start='2025-1-1T0:0'),dict(seed_start='2025-01-01T00:05'),
    dict(seed_start='2025-01-01T00:00',base_timeframe='M7'),
    dict(seed_start='2025-01-01T00:00',confirm_timeframe='D1'),
    dict(seed_start='2025-01-01T00:00',confirm_timeframe='M5'),
    dict(seed_start='2025-01-01T00:00',second_timeframe='M10')])
def test_strategy_requires_explicit_common_seed_and_native_ordered_tfs(kwargs):
    with pytest.raises(ValueError):super_atr_research.definition(**kwargs)


def test_preflight_preserves_existing_include_and_does_not_write_framework(tmp_path,framework):
    before={p.relative_to(framework):p.read_bytes() for p in framework.rglob('*') if p.is_file()}
    with pytest.raises(ValueError,match='fora'):sdk.create_super_atr_research_robot('SuperAtrQA',framework,framework_root=framework,seed_start='2025-01-01T00:00')
    target=tmp_path/'package/Include/Framework/HADOUKEN/Indicators/HDK_SuperATR.mqh';target.parent.mkdir(parents=True);target.write_text('Preserve this different include')
    with pytest.raises(FileExistsError,match='difere'):generate(tmp_path,framework)
    assert not (tmp_path/'package/Experts').exists() and target.read_text()=='Preserve this different include'
    assert before=={p.relative_to(framework):p.read_bytes() for p in framework.rglob('*') if p.is_file()}


def test_no_overwrite_and_duplicate_magic_preflight(tmp_path,framework):
    files=generate(tmp_path,framework);before=[p.read_bytes() for p in files]
    with pytest.raises(FileExistsError):generate(tmp_path,framework)
    robot=catalog.robot_from_file(files[0])
    with pytest.raises(ValueError,match='Magic já usado'):generate(tmp_path,framework,robots=[robot])
    assert before==[p.read_bytes() for p in files]


def test_cli_reuses_generator_and_imports_only_owned_package(db,tmp_path,framework,monkeypatch,capsys):
    monkeypatch.setattr(cli.Context,'db',property(lambda _:db))
    monkeypatch.chdir(tmp_path)
    cfg=tmp_path/'qa.ini';cfg.write_text(f'[DATABASE]\ntype=sqlite\npath={db.db_path}\n',encoding='utf-8')
    args=['--config',str(cfg),'--json','robots','new','SuperAtrCLI','--dir',str(tmp_path/'cli-package'),'--template','super-atr-research','--framework',str(framework),'--seed-start','2025-01-01T00:00','--second-confirm-tf','H1']
    assert cli.main(args)==0;result=json.loads(capsys.readouterr().out)['data']
    source=next(p for p in result['files'] if p.endswith('.mq5'));r=catalog.robot_from_file(source)
    assert 'InpSecondConfirm=true' in __import__('pathlib').Path(source).read_text().replace(' ','')
    assert r.timeframes==('M5',) and not contract.validate(r)


def test_cancellation_removes_only_files_created_by_this_generation(tmp_path,framework):
    calls=[]
    def cancel():calls.append(True);return len(calls)>=5
    with pytest.raises(super_atr.indicator_materializer.Cancelled):generate(tmp_path,framework,cancel_check=cancel)
    assert not any(p.is_file() for p in (tmp_path/'package').rglob('*'))


def test_partial_write_failure_rolls_back_owned_files(tmp_path,framework,monkeypatch):
    original=Path.open
    class Broken:
        def __init__(self,file):self.file=file
        def __enter__(self):return self
        def write(self,data):self.file.write(data[:10]);raise OSError('controlled disk failure')
        def __exit__(self,*args):self.file.close()
    def open_file(path,mode='r',*args,**kwargs):
        file=original(path,mode,*args,**kwargs)
        return Broken(file) if mode=='xb' and path.name.endswith('.robot.json') else file
    monkeypatch.setattr(Path,'open',open_file)
    with pytest.raises(OSError,match='disk failure'):generate(tmp_path,framework)
    assert not any(p.is_file() for p in (tmp_path/'package').rglob('*'))


@pytest.mark.parametrize('magic',[False,0,1.5])
def test_invalid_magic_is_not_silently_replaced(tmp_path,framework,magic):
    with pytest.raises(ValueError,match='Magic'):sdk.create_super_atr_research_robot('SuperAtrQA',tmp_path/'package',framework_root=framework,seed_start='2025-01-01T00:00',magic=magic)


def test_reference_variant_package_declares_sa_ref_01_and_keeps_default_sa01_off(tmp_path,framework):
    files=generate(tmp_path,framework,reference=True)
    robot=catalog.robot_from_file(files[0]);receipt=json.loads(files[4].read_text())
    values={i.name:i.default for i in robot.inputs}
    assert values['InpVariant']=='0' and values['InpResearchOrders']=='false'      # fonte: padrão SA-01, entradas desligadas
    ref=mql.parse_set(files[5].read_text())
    assert files[5].name=='SuperAtrQA_SAREF01.set'
    assert ref['InpVariant']=='1' and ref['InpRefStopATR']=='2.0' and ref['InpRefTargetR']=='2.0' and ref['InpRefMinBars']=='100'
    assert ref['InpRiskPct']=='0.25' and ref['InpResearchOrders']=='true' and ref['InpSecondConfirm']=='false'
    contract_=receipt['research_contract']
    assert contract_['strategy_version']=='SA-REF-01/1' and contract_['confirm_timeframes']==[]
    assert contract_['reference_contract']['key']=='flip1/variable2atr/target2r' and 'host_limits_note' in contract_
    source=files[0].read_text()
    assert 'SAREF_DECISION' in source and 'MQL_TESTER' in source and 'ACCOUNT_TRADE_MODE_DEMO' in source


def test_reference_generation_refuses_changed_host_before_writing_package(tmp_path,framework):
    host=framework/'Include/Framework/HADOUKEN/Strategies/HDK_StrategyRobot.mqh'
    host.write_text('// New management requires its own profile, not SA-REF-01\n')
    with pytest.raises(ValueError,match='origem congelada SA-REF-01'):
        generate(tmp_path,framework,reference=True)
    assert not (tmp_path/'package').exists()


def test_reference_presets_separate_historical_reproduction_from_demo_qualification(tmp_path,framework):
    files=generate(tmp_path,framework,reference=True)
    reproduction=files[5].read_text()
    assert 'REPRODUÇÃO HISTÓRICA' in reproduction and 'NÃO usar em DEMO' in reproduction
    assert all(f'{k}=100.0' in reproduction for k in ('InpDailyLossPct','InpWeeklyLossPct','InpMaxDrawdownPct'))
    readme=(files[0].parent/'README.md').read_text()
    assert 'SAREF01_DEMO' in readme and 'Não usar em DEMO' in readme
    assert 'reprodução histórica' in json.loads((files[0].parent/'SuperAtrQA.robot.json').read_text())['description']
    demo=super_atr_research.demo_preset('SuperAtrQA',tmp_path/'package',seed_start='2026-09-01T00:00',magic=654321)
    values=mql.parse_set(demo.read_text())
    assert demo.name=='SuperAtrQA_SAREF01_DEMO.set' and 'ARMADO explicitamente' in demo.read_text()
    assert values['InpResearchOrders']=='true' and values['InpVariant']=='1' and values['InpSeedStart']=='2026.09.01 00:00'
    assert (values['InpRiskPct'],values['InpDailyLossPct'],values['InpWeeklyLossPct'],values['InpMaxDrawdownPct'])==('0.25','2.0','4.0','10.0')
    with pytest.raises(FileExistsError):
        super_atr_research.demo_preset('SuperAtrQA',tmp_path/'package',seed_start='2026-09-01T00:00',magic=654321)


@pytest.mark.parametrize('kwargs',[dict(seed_start='2026-09-01T00:03'),dict(seed_start='2026-09-01'),
    dict(seed_start='2026-09-01T00:00',max_drawdown_pct=100.),dict(seed_start='2026-09-01T00:00',daily_loss_pct=5.),
    dict(seed_start='2026-09-01T00:00',risk_pct=1.)])
def test_demo_preset_refuses_misaligned_seed_or_relaxed_protection(tmp_path,kwargs):
    with pytest.raises(ValueError):
        super_atr_research.demo_preset('SuperAtrQA',tmp_path,magic=654321,**kwargs)
