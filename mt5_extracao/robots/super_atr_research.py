"""SA-01: pacote de consumidor de pesquisa pelo scaffold e host existentes."""
from datetime import datetime
import hashlib
import json
from pathlib import Path
import tempfile

from mt5_extracao import timeframes
from mt5_extracao.analytics import super_atr
from mt5_extracao.experiments.schema import digest
from . import battery,catalog,mql,scaffold

TEMPLATE='SuperAtrResearch.mq5.tmpl'


def definition(seed_start,base_timeframe='M5',confirm_timeframe='M15',second_timeframe=None):
    try:
        seed=datetime.strptime(seed_start,'%Y-%m-%dT%H:%M')
    except (TypeError,ValueError):raise ValueError('Semente explícita em horário broker: YYYY-MM-DDTHH:MM, sem fuso implícito.') from None
    if seed.strftime('%Y-%m-%dT%H:%M')!=seed_start:raise ValueError('Use exatamente YYYY-MM-DDTHH:MM para identificar a semente broker.')
    frames=[timeframes.parse(v) for v in (base_timeframe,confirm_timeframe)]
    if second_timeframe is not None:frames.append(timeframes.parse(second_timeframe))
    if any(t is None or t.minutes>=1440 for t in frames):raise ValueError('Use TFs nativos intradiários reconhecidos; não há agregação oculta.')
    if any(a.minutes>=b.minutes for a,b in zip(frames,frames[1:])):raise ValueError('Confirmadores precisam ser mais longos e distintos do TF base.')
    if any((seed.hour*60+seed.minute)%t.minutes for t in frames):raise ValueError('Semente deve coincidir com abertura broker em todos os TFs.')
    return dict(strategy_id='super_atr_closed_confirmation',strategy_version='0.1-research',seed_start_broker=seed_start,
        time_basis='native_broker_server; no host-timezone conversion; UTC offset not inferred',
        base_timeframe=frames[0].name,confirm_timeframes=[t.name for t in frames[1:]],
        curves=[dict(timeframe=t.name,period=20,mean=0,deviation=1.,initial_side='up') for t in frames],
        entry='new_base_flip_and_all_latest_closed_confirmers_same_side',confirmation_age='0 <= decision - nominal_close < native_period',
        simultaneous_closes='include only bars whose end <= decision; current forming bars excluded',
        decision='new_base_open; consume_once_even_missing_or_refused; no_intrabar_retry',
        seed='fixed_first_real_bar_must_exist_on_all_TFs; max20000; no rolling reseed',
        revision='exact observed HLC/time prefix; mismatch latches entry block for this instance',
        initial_stop='current_base_line; host validates protective side, tick, stops, freeze and lot',
        take_profit=None,exit='opposite_base_flip_even_when_confirmer_missing',move_guard_default=False,
        risk_pct=.5,daily_loss_pct=2.,weekly_loss_pct=4.,max_drawdown_pct=10.,max_positions=1,
        host='Framework/HADOUKEN/Strategies/HDK_StrategyRobot.mqh',entry_observation='HDK_STRATEGY_OBSERVE_ENTRIES',
        account_gate='tester_or_demo_before_StrategyInit_and_each_tick',entries_enabled_default=False,
        restart='skip_current_base_decision; existing host magic+symbol guard; no cross-instance guarantee',
        historical_availability_qualified=False,operational_qualified=False,predictive_qualified=False)


def create(name,folder,*,framework_root,seed_start,base_timeframe='M5',confirm_timeframe='M15',second_timeframe=None,robots=(),config=None,magic=None,cancel_check=None):
    """Gera pacote local fechado; não compila/inicia MT5 nem altera o framework."""
    robots=list(robots)
    def guard():
        if cancel_check is not None and cancel_check():raise super_atr.indicator_materializer.Cancelled('Geração cancelada; arquivos próprios incompletos removidos.')
    guard()
    contract=definition(seed_start,base_timeframe,confirm_timeframe,second_timeframe)
    artifact,_=super_atr.source(framework_root)
    guard()
    if not scaffold._NAME.fullmatch(name or ''):raise ValueError('Nome inválido; use3…40 letras/números/underscore, iniciando com letra.')
    root=Path(folder).expanduser().resolve();framework=Path(framework_root).expanduser().resolve()
    if root==framework or framework in root.parents:raise ValueError('Gere artefatos fora da dependência HADOUKEN fixada; preserve seu estado detached/limpo.')
    # Template e closure são revisados antes de criar qualquer arquivo permanente.
    values=dict(NAME=name,MAGIC=scaffold.new_magic(robots) if magic is None else magic,SEED=datetime.strptime(seed_start,'%Y-%m-%dT%H:%M').strftime('%Y.%m.%d %H:%M'),
        BASE=contract['base_timeframe'],CONFIRM=contract['confirm_timeframes'][0],SECOND_ENABLED='true' if second_timeframe else 'false',
        SECOND=contract['confirm_timeframes'][-1] if second_timeframe else 'H1')
    if type(values['MAGIC']) is not int or not 100000<=values['MAGIC']<=999999:raise ValueError('Magic de pesquisa exige inteiro exclusivo de seis dígitos.')
    if any(str(values['MAGIC'])==str(battery.magic_number(r)) for r in robots):raise ValueError('Magic já usado no catálogo; escolha identificador exclusivo.')
    source=scaffold.render(TEMPLATE,**values)
    with tempfile.TemporaryDirectory(prefix='mt5x-super-atr-source-') as tmp:
        probe=Path(tmp)/'probe.mq5';probe.write_text(source,encoding='utf-8')
        includes,missing=mql.resolve_includes(probe,framework/'Include')
    includes,assets,absent=catalog._study_dependencies(framework,includes)
    missing += [p for p in absent if p not in missing]
    internal=[p for p in missing if p.startswith('Framework/') or p.startswith('HDK_')]
    if internal:raise ValueError('Closure HADOUKEN incompleta: '+', '.join(internal))
    payload={root/p.resolve().relative_to(framework):p.read_bytes() for p in includes+assets}
    closure={p.relative_to(root).as_posix():hashlib.sha256(raw).hexdigest() for p,raw in payload.items()}
    contract.update(framework_commit=artifact['framework_commit'],header_sha256=artifact['header_sha256'],
        source_sha256=hashlib.sha256(source.encode('utf-8')).hexdigest(),closure_sha256=digest(closure),platform_standard_includes=missing)
    target=root/'Experts'/name;preset=root/'Presets'/f'{name}_Conservador.set'
    manifest=dict(contract=1,name=name,description='Super ATR multi-TF · candidato de pesquisa Tester+DEMO; entradas desligadas',
        timeframes=[contract['base_timeframe']],regime='pesquisa',markets='',
        risk=dict(mandatory_stop=True),
        inputs=dict(magic='InpMagicNumber',risk_pct='InpRiskPct',daily_loss_pct='InpDailyLossPct',max_drawdown_pct='InpMaxDrawdownPct',stop='InpBaseDeviation',export_trades='InpExportTrades'),
        tunable=['InpBasePeriod','InpBaseMean','InpBaseDeviation','InpConfirmPeriod','InpConfirmMean','InpConfirmDeviation'])
    research=dict(research_contract=contract,research_contract_sha256=digest(contract),research_closure=closure)
    preset_text='; Candidato de pesquisa; entradas desligadas; nenhuma qualificação operacional\n'+''.join(f'{k}={v}\n' for k,v in dict(InpMagicNumber=values['MAGIC'],InpResearchOrders='false',InpSeedStart=values['SEED'],InpBaseTF=timeframes.parse(base_timeframe).value,InpConfirmTF=timeframes.parse(confirm_timeframe).value,InpSecondConfirm=values['SECOND_ENABLED'],InpSecondTF=timeframes.parse(second_timeframe or 'H1').value,InpRiskPct=.5,InpDailyLossPct=2.,InpWeeklyLossPct=4.,InpMaxDrawdownPct=10.,InpMoveGuard='false',InpExportTrades='false').items())
    preset_text+=''.join(f'Inp{role}{key}={value}\n' for role in ('Base','Confirm','Second') for key,value in [('Period',20),('Mean',0),('Deviation',1)])+'InpDeviationPoints=20\n'
    payload.update({target/f'{name}.mq5':source.encode('utf-8'),target/f'{name}.robot.json':(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n').encode('utf-8'),
        target/f'{name}.research.json':(json.dumps(research,ensure_ascii=False,indent=2)+'\n').encode('utf-8'),
        preset:preset_text.encode('utf-8'),target/'README.md':('Super ATR de pesquisa0.1\n\nContrato e hashes no research.json. Semente em horário broker precisa existir em todos os TFs. Entradas vêm desligadas. Somente Tester+DEMO, com risco/executor/painel HADOUKEN existentes. Use o compile/check/Session oficiais; este pacote não libera perfis automáticos nem qualifica operação. Padrões20/SMA/d1 são hipóteses, não seleção retrospectiva. G19.15 não aprovou candidato sob custos assumidos. Estado retido não é novo evento. Restart pula decisão corrente; não garante exclusividade entre instâncias. Prefixo revisado bloqueia novas entradas nesta instância; proteção existente permanece no host.\n').encode('utf-8')})
    if target.exists() or preset.exists():raise FileExistsError('Robô/preset já existe; nenhum arquivo foi alterado.')
    if any(r.key==f'mql5:{catalog._slug(name)}' for r in robots):raise ValueError('Nome já existe no catálogo.')
    for path,raw in payload.items():
        if path.exists() and path.read_bytes()!=raw:raise FileExistsError('Include existente difere da closure; preserve-o e reconcilie o destino: '+str(path))
    created=[]
    try:
        for path,raw in payload.items():
            guard()
            if path.exists():continue
            path.parent.mkdir(parents=True,exist_ok=True)
            with path.open('xb') as stream:
                created.append(path);stream.write(raw)
    except BaseException:
        for path in reversed(created):path.unlink()
        raise
    return [target/f'{name}.mq5',target/f'{name}.robot.json',target/'README.md',preset,target/f'{name}.research.json']+[p for p in payload if p.is_relative_to(root/'Include') or p.is_relative_to(root/'Indicators')]
