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


REFERENCE_README=('SA-REF-01 · presets\n\n'
    '- Conservador: desarmado (InpResearchOrders=false), variante SA-01.\n'
    '- SAREF01: reprodução histórica no Strategy Tester; armado e limites do host em 100% (perda diária, semanal, drawdown). Não usar em DEMO.\n'
    '- SAREF01_DEMO: qualificação DEMO, gerado à parte com semente própria declarada e limites do host 2%/4%/10%.\n'
    'Variante e regras: research.json (reference_contract). Decisões: SAREF_DECISION/SAREF_BLOCKED no log.\n\n')


def demo_preset(name,folder,*,seed_start,magic,risk_pct=.25,daily_loss_pct=2.,weekly_loss_pct=4.,max_drawdown_pct=10.):
    """Preset de qualificação DEMO do SA-REF-01: armamento explícito, semente declarada e limites operacionais do host.

    A semente integra a identidade do cálculo: é sempre informada, nunca trocada em silêncio. Não sobrescreve.
    """
    seed=datetime.strptime(seed_start,'%Y-%m-%dT%H:%M')
    if seed.strftime('%Y-%m-%dT%H:%M')!=seed_start or seed.minute%5:raise ValueError('Semente broker YYYY-MM-DDTHH:MM alinhada a M5.')
    if not (0<risk_pct<=.25 and 0<daily_loss_pct<=2. and 0<weekly_loss_pct<=4. and 0<max_drawdown_pct<=10.):
        raise ValueError('Preset DEMO aceita risco ≤0,25% e limites do host ≤2%/4%/10%; não relaxa proteções.')
    target=Path(folder).expanduser().resolve()/'Presets'/f'{name}_SAREF01_DEMO.set'
    if target.exists():raise FileExistsError('Preset DEMO já existe; nenhum arquivo foi alterado.')
    values=dict(InpMagicNumber=magic,InpResearchOrders='true',InpSeedStart=seed.strftime('%Y.%m.%d %H:%M'),
        InpBaseTF=timeframes.parse('M5').value,InpConfirmTF=timeframes.parse('M15').value,InpSecondConfirm='false',
        InpSecondTF=timeframes.parse('H1').value,InpVariant=1,InpRefStopATR=2.0,InpRefTargetR=2.0,InpRefMinBars=100,
        InpRiskPct=risk_pct,InpDailyLossPct=daily_loss_pct,InpWeeklyLossPct=weekly_loss_pct,InpMaxDrawdownPct=max_drawdown_pct,
        InpMoveGuard='false',InpExportTrades='true',InpBasePeriod=20,InpBaseMean=0,InpBaseDeviation=1,InpConfirmPeriod=20,
        InpConfirmMean=0,InpConfirmDeviation=1,InpSecondPeriod=20,InpSecondMean=0,InpSecondDeviation=1,InpDeviationPoints=20)
    text=('; SA-REF-01 QUALIFICAÇÃO DEMO · ARMADO explicitamente (InpResearchOrders=true) · gate do código: só Tester/DEMO.\n'
          f'; Semente declarada {seed_start} (identidade diferente da reprodução histórica). Limites do host: '
          f'perda diária {daily_loss_pct}%, semanal {weekly_loss_pct}%, drawdown {max_drawdown_pct}%, risco {risk_pct}%.\n')
    text+=''.join(f'{k}={v}\n' for k,v in values.items())
    target.parent.mkdir(parents=True,exist_ok=True)
    with target.open('x',encoding='utf-8') as stream:stream.write(text)
    return target


def create(name,folder,*,framework_root,seed_start,base_timeframe='M5',confirm_timeframe='M15',second_timeframe=None,robots=(),config=None,magic=None,cancel_check=None,reference=False):
    """Gera pacote local fechado; não compila/inicia MT5 nem altera o framework."""
    robots=list(robots)
    def guard():
        if cancel_check is not None and cancel_check():raise super_atr.indicator_materializer.Cancelled('Geração cancelada; arquivos próprios incompletos removidos.')
    guard()
    contract=definition(seed_start,base_timeframe,confirm_timeframe,second_timeframe)
    if reference:
        # SA-REF-01: mesmo pacote/host; InpVariant=1 desliga confirmadores e usa as regras G19.15 congeladas.
        from .super_atr_reference import CONTRACT
        if base_timeframe!='M5':raise ValueError('SA-REF-01 é congelado em M5.')
        contract.update(strategy_id='super_atr_reference_g19_15',strategy_version='SA-REF-01/1',reference_contract=CONTRACT,
            confirm_timeframes=[],entry='strict_flip_d1_closed_M5; next M1 open; >=100 consecutive same-day M5 closes',
            initial_stop='close_signal_bar -/+ 2*ATR20_same_bar',take_profit='2R',exit='target_or_stop_or_end_of_day; no opposite flip exit',
            risk_pct=.25,daily_loss_pct=100.,weekly_loss_pct=100.,max_drawdown_pct=100.,
            host_limits_note='limites diários/semanais/DD do host desligados (100%) porque o estudo não tem stop diário; declarado, não omitido',
            decision_log='SAREF_DECISION no log do Tester, uma linha por virada (inclusive inelegível)')
    artifact,_=super_atr.source(framework_root)
    guard()
    if not scaffold._NAME.fullmatch(name or ''):raise ValueError('Nome inválido; use3…40 letras/números/underscore, iniciando com letra.')
    root=Path(folder).expanduser().resolve();framework=Path(framework_root).expanduser().resolve()
    if root==framework or framework in root.parents:raise ValueError('Gere artefatos fora da dependência HADOUKEN fixada; preserve seu estado detached/limpo.')
    # Template e closure são revisados antes de criar qualquer arquivo permanente.
    values=dict(NAME=name,MAGIC=scaffold.new_magic(robots) if magic is None else magic,SEED=datetime.strptime(seed_start,'%Y-%m-%dT%H:%M').strftime('%Y.%m.%d %H:%M'),
        BASE=contract['base_timeframe'],CONFIRM=timeframes.parse(confirm_timeframe).name,SECOND_ENABLED='true' if second_timeframe else 'false',
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
    description=('Super ATR · SA-REF-01 (G19.15) e SA-01 · fonte e preset Conservador desarmados; preset SAREF01 = reprodução '
                 'histórica no Tester, armado e com limites do host em 100%; DEMO só pelo preset _SAREF01_DEMO' if reference else
                 'Super ATR multi-TF · candidato de pesquisa Tester+DEMO; entradas desligadas')
    manifest=dict(contract=1,name=name,description=description,
        timeframes=[contract['base_timeframe']],regime='pesquisa',markets='',
        risk=dict(mandatory_stop=True),
        inputs=dict(magic='InpMagicNumber',risk_pct='InpRiskPct',daily_loss_pct='InpDailyLossPct',max_drawdown_pct='InpMaxDrawdownPct',stop='InpBaseDeviation',export_trades='InpExportTrades'),
        tunable=['InpBasePeriod','InpBaseMean','InpBaseDeviation','InpConfirmPeriod','InpConfirmMean','InpConfirmDeviation'])
    research=dict(research_contract=contract,research_contract_sha256=digest(contract),research_closure=closure)
    preset_text='; Candidato de pesquisa; entradas desligadas; nenhuma qualificação operacional\n'+''.join(f'{k}={v}\n' for k,v in dict(InpMagicNumber=values['MAGIC'],InpResearchOrders='false',InpSeedStart=values['SEED'],InpBaseTF=timeframes.parse(base_timeframe).value,InpConfirmTF=timeframes.parse(confirm_timeframe).value,InpSecondConfirm=values['SECOND_ENABLED'],InpSecondTF=timeframes.parse(second_timeframe or 'H1').value,InpRiskPct=.5,InpDailyLossPct=2.,InpWeeklyLossPct=4.,InpMaxDrawdownPct=10.,InpMoveGuard='false',InpExportTrades='false').items())
    preset_text+=''.join(f'Inp{role}{key}={value}\n' for role in ('Base','Confirm','Second') for key,value in [('Period',20),('Mean',0),('Deviation',1)])+'InpDeviationPoints=20\n'
    if reference:
        ref_text=('; SA-REF-01 REPRODUÇÃO HISTÓRICA no Strategy Tester (G19.15 flip1/variable2atr/target2r).\n'
                  '; ARMADO (InpResearchOrders=true) e limites do host em 100% (perda diária, semanal e drawdown), porque o estudo\n'
                  '; não tem stop diário. NÃO usar em DEMO: para qualificação use o preset _SAREF01_DEMO (demo_preset).\n')+''.join(
            f'{k}={v}\n' for k,v in dict(InpMagicNumber=values['MAGIC'],InpResearchOrders='true',InpSeedStart=values['SEED'],
                InpBaseTF=timeframes.parse('M5').value,InpConfirmTF=timeframes.parse(confirm_timeframe).value,InpSecondConfirm='false',
                InpSecondTF=timeframes.parse('H1').value,InpVariant=1,InpRefStopATR=2.0,InpRefTargetR=2.0,InpRefMinBars=100,
                InpRiskPct=.25,InpDailyLossPct=100.,InpWeeklyLossPct=100.,InpMaxDrawdownPct=100.,InpMoveGuard='false',
                InpExportTrades='true',InpBasePeriod=20,InpBaseMean=0,InpBaseDeviation=1,InpConfirmPeriod=20,InpConfirmMean=0,
                InpConfirmDeviation=1,InpSecondPeriod=20,InpSecondMean=0,InpSecondDeviation=1,InpDeviationPoints=20).items())
        payload[root/'Presets'/f'{name}_SAREF01.set']=ref_text.encode('utf-8')
    payload.update({target/f'{name}.mq5':source.encode('utf-8'),target/f'{name}.robot.json':(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n').encode('utf-8'),
        target/f'{name}.research.json':(json.dumps(research,ensure_ascii=False,indent=2)+'\n').encode('utf-8'),
        preset:preset_text.encode('utf-8'),target/'README.md':((REFERENCE_README if reference else '')+'Super ATR de pesquisa0.1\n\nContrato e hashes no research.json. Semente em horário broker precisa existir em todos os TFs. Entradas vêm desligadas. Somente Tester+DEMO, com risco/executor/painel HADOUKEN existentes. Use o compile/check/Session oficiais; este pacote não libera perfis automáticos nem qualifica operação. Padrões20/SMA/d1 são hipóteses, não seleção retrospectiva. G19.15 não aprovou candidato sob custos assumidos. Estado retido não é novo evento. Restart pula decisão corrente; não garante exclusividade entre instâncias. Prefixo revisado bloqueia novas entradas nesta instância; proteção existente permanece no host.\n').encode('utf-8')})
    if reference and (root/'Presets'/f'{name}_SAREF01.set').exists():raise FileExistsError('Preset SA-REF-01 já existe; nenhum arquivo foi alterado.')
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
    return [target/f'{name}.mq5',target/f'{name}.robot.json',target/'README.md',preset,target/f'{name}.research.json']+([root/'Presets'/f'{name}_SAREF01.set'] if reference else [])+[p for p in payload if p.is_relative_to(root/'Include') or p.is_relative_to(root/'Indicators')]
