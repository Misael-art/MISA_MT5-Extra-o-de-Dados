"""LAB-06: o mesmo robô e a mesma revisão de preset em ativos diferentes.

Lê execuções já gravadas. Não abre backtest, não usa fila e não trata o último
run de cada ativo como prova de que o preset é o mesmo.
"""
import hashlib
from itertools import combinations
import json
import math
import time
import pandas as pd

from mt5_extracao.robots import store
from mt5_extracao import profit_factor, timeframes


_RELATION_STATUS = {
    'AVAILABLE': 'Retornos comparáveis',
    'INCOMPATIBLE_RECEIPTS': 'Recibos incompatíveis',
    'NO_VERIFIED_INTRADAY_VALUATIONS': 'Curvas completas de fechamento intradiário indisponíveis',
    'INCOMPATIBLE_CURVE_CLOCKS': 'Relógios das curvas incompatíveis',
    'INSUFFICIENT_PAIRS': 'Menos de 30 pares válidos',
    'CONSTANT_SERIES': 'Uma das séries de retornos é constante',
    'UNDEFINED': 'Coeficiente indefinido',
}


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)

# Contrato de comparação de recibos (comparison_context): o que precisa coincidir para resultados serem
# economicamente comparáveis. Reusado por compare_runs e pelo estudo condicional (T19.52).
ACCOUNT_CONTEXT = (('engine', 'Motor'), ('code_sha256', 'Código do motor'),
                   ('settings', 'Configuração de capital/risco/custos'), ('trade_from', 'Início efetivo'),
                   ('source', 'Origem'), ('clock', 'Relógio'), ('currency', 'Moeda da conta'))
MISSING_RECEIPT = 'recibo de motor/configuração completo ausente'


def context_receipt_problems(ctx):
    """Problemas de um recibo comparison_context isolado; vazio só quer dizer recibo completo, não igualdade."""
    from dataclasses import asdict
    from mt5_extracao.strategies.backtester import BacktestConfig
    if not isinstance(ctx, dict) or ctx.get('schema_version') != 1 or not ctx.get('code_sha256'):
        return [MISSING_RECEIPT]
    problems = []
    required = {'engine', 'settings', 'trade_from', 'source', 'clock', 'instrument_spec', 'currency',
                'currency_source', 'external_cashflows'}
    settings = ctx.get('settings')
    if (not required <= set(ctx) or ctx.get('engine') != 'python_ohlcv_stop_first' or not isinstance(settings, dict)
            or set(settings) != set(asdict(BacktestConfig()))):
        problems.append('configuração de execução incompleta/incompatível')
    if not isinstance(ctx.get('instrument_spec'), dict) or ctx['instrument_spec'].get('known') is not True:
        problems.append('especificação do instrumento desconhecida')
    if ctx.get('external_cashflows') is not False:
        problems.append('fluxos externos presentes/desconhecidos; comparação desta versão exige ausência explícita')
    return problems


def context_differences(first, other):
    """Dimensões materiais do contrato que diferem entre dois recibos."""
    first, other = first or {}, other or {}
    return [label + ' diferente' for key, label in ACCOUNT_CONTEXT if first.get(key) != other.get(key)]


def comparable_instrument_spec(spec):
    """Especificação sem a contagem por barra da auditoria (não é revisão/valor do contrato)."""
    if not isinstance(spec, dict):
        return spec
    spec = dict(spec)
    provenance = dict(spec.get('provenance') or {})
    provenance.pop('missing_specification_bars', None)
    spec['provenance'] = provenance
    return spec


def _body(run, *, varied_dimension="symbol"):
    """Hash da composição sem a dimensão em comparação."""
    metrics = run.get("metrics") or {}
    composition = metrics.get("composition")
    if not isinstance(composition, dict):
        return None, "composição ausente; o último run deste ativo não prova a revisão"
    context = composition.get("context")
    if not isinstance(context, dict) or not context.get("symbol") or not context.get("timeframe"):
        return None, "composição sem ativo e timeframe; revisão não identificada"
    if context["symbol"] != run.get("symbol") or context["timeframe"] != run.get("timeframe"):
        return None, "ativo ou timeframe do run difere da composição gravada"
    try:
        body = json.loads(_canonical(composition))
        body["context"].pop(varied_dimension)
        digest = hashlib.sha256(_canonical(body).encode("utf-8")).hexdigest()
    except (TypeError, ValueError):
        return None, "composição com valor não finito; revisão não identificada"
    return digest, None


def _number(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _costs(metrics):
    raw = metrics.get("costs")
    if not isinstance(raw, dict):
        return None
    found = {}
    for key in ("commission", "swap", "fee"):
        if key not in raw:
            continue
        number = _number(raw[key])
        if number is not None:
            found[key] = number
    return found or None


def _spec(metrics):
    raw = metrics.get("instrument_context")
    found = {}
    if isinstance(raw, dict):
        for key in ("currency_profit", "point", "tick_size", "tick_value", "volume_min"):
            if key in raw and raw[key] is not None:
                found[key] = raw[key]
    if metrics.get("currency"):
        found["currency"] = metrics["currency"]
    return found or None


def _curve(run):
    points = []
    for item in run.get("equity") or []:
        if not isinstance(item, (list, tuple)) or len(item) < 2:
            continue
        value = _number(item[1])
        if value is None:
            continue
        points.append((str(item[0]), value))
    metrics = run.get("metrics") or {}
    initial = _number(metrics.get("initial_equity"))
    basis = "initial_equity"
    if initial is None or initial <= 0:
        initial = points[0][1] if points else None
        basis = "primeiro ponto da curva gravada"
    if initial is None or initial <= 0:
        return None, "capital inicial ausente ou não positivo; curva não normalizada"
    return dict(times=[item[0] for item in points], fraction=[item[1] / initial for item in points], basis=basis), None


def _run_row(run, body_hash, curve, curve_reason):
    metrics = run.get("metrics") or {}
    return dict(run_id=run.get("run_id"), symbol=run.get("symbol"), timeframe=run.get("timeframe"),
                created_at=run.get("created_at"), composition_hash=metrics.get("composition_hash"),
                body_hash=body_hash, strategy=(metrics.get("composition") or {}).get("strategy"),
                interval=dict(start=run.get("data_start"), end=run.get("data_end"), bars=run.get("bars")),
                spec=_spec(metrics), costs=_costs(metrics),
                risk_per_trade=_number(metrics.get("risk_per_trade")),
                real_spread=metrics.get("real_spread") if isinstance(metrics.get("real_spread"), bool) else None,
                curve=curve, curve_reason=curve_reason)


def compare(db, robot_key):
    """Relatório somente leitura. `opened_backtest` permanece falso."""
    key = (robot_key or "").strip()
    empty = dict(robot_key=key, opened_backtest=False, latest_per_asset_is_proof=False,
                 match="composition_without_symbol", groups=[], unproven=[], curves=[],
                 latest_runs_share_body=False)
    if not key:
        return dict(empty, status="empty", reason="Escolha um robô na lente por robô.",
                    notice="A comparação de preset não dispara backtest.", title="")
    runs = store.load_runs(db, key) if db is not None else []
    if not runs:
        return dict(empty, status="empty",
                    reason="Nenhuma execução gravada deste robô. A tela não dispara backtest.",
                    notice="Sem runs gravados. O último resultado por ativo, se existir fora desta base, não foi usado.",
                    title="")
    grouped, unproven = {}, []
    latest = {}
    for run in runs:
        slot = (run.get("symbol"), run.get("timeframe"))
        latest.setdefault(slot, run)  # load_runs já vem do mais recente para o mais antigo
        body_hash, why = _body(run)
        curve, curve_reason = _curve(run)
        row = _run_row(run, body_hash, curve, curve_reason)
        if body_hash is None:
            row["why"] = why
            unproven.append(row)
            continue
        grouped.setdefault(body_hash, []).append(row)
    latest_bodies = []
    for run in latest.values():
        body_hash, _why = _body(run)
        latest_bodies.append(body_hash)
    share = len(latest) >= 2 and None not in latest_bodies and len(set(latest_bodies)) == 1
    groups = []
    for body_hash, rows in grouped.items():
        symbols = sorted({row["symbol"] for row in rows})
        currencies = {((row.get("spec") or {}).get("currency") or (row.get("spec") or {}).get("currency_profit"))
                      for row in rows}
        currencies.discard(None)
        groups.append(dict(body_hash=body_hash, symbols=symbols, runs=rows,
                           currencies=sorted(currencies),
                           scale=("fração do capital inicial; moedas não convertidas"
                                  if len(currencies) != 1 else f"fração do capital inicial · {next(iter(currencies))}")))
    groups.sort(key=lambda item: (-len(item["symbols"]), -len(item["runs"]), item["body_hash"]))
    chosen = groups[0] if groups else None
    curves = []
    if chosen is not None:
        for row in chosen["runs"]:
            if row["curve"] is None:
                continue
            curves.append(dict(symbol=row["symbol"], timeframe=row["timeframe"],
                               composition_hash=row["composition_hash"] or "",
                               times=row["curve"]["times"], fraction=row["curve"]["fraction"],
                               basis=row["curve"]["basis"]))
    if chosen is None:
        reason = "Nenhuma execução tem a composição gravada. Sem isso os ativos não são a mesma revisão."
        title = ""
    elif len(chosen["symbols"]) < 2:
        reason = "Esta revisão está gravada em um único ativo. Não há cruzamento entre ativos."
        title = f"Revisão {chosen['body_hash'][:12]} · um ativo · {chosen['scale']}"
    else:
        reason = ""
        title = f"Revisão {chosen['body_hash'][:12]} · {len(chosen['symbols'])} ativos · {chosen['scale']}"
    others = [item["body_hash"][:12] for item in groups[1:4]]
    notice = ("Prova = composição gravada, sem o ativo no hash. O último run de cada ativo não é a prova. "
              "Operação e execução não são refeitas nesta tela.")
    if unproven:
        notice += f" {len(unproven)} execução(ões) sem revisão comprovada ficam de fora do gráfico."
    if others:
        notice += " Outras revisões, não sobrepostas: " + ", ".join(others) + "."
    if chosen is not None:
        missing = [row["symbol"] for row in chosen["runs"] if row["curve"] is None]
        if missing:
            notice += " Sem curva normalizada: " + ", ".join(missing) + "."
    return dict(robot_key=key, opened_backtest=False, latest_per_asset_is_proof=False,
                match="composition_without_symbol", status="ready" if chosen else "unproven",
                latest_runs_share_body=share, reason=reason, notice=notice, title=title,
                groups=groups, unproven=unproven, curves=curves[:8])


def _financial_row(run, *, varied_dimension="symbol"):
    from mt5_extracao.robots.backtest import downsample
    metrics=run.get('metrics') or {};body,reason=_body(run,varied_dimension=varied_dimension)
    curves={};issues=[]
    for key in ('equity','account_balance','floating_equity','performance_equity'):
        raw=run.get(key) or []
        if len(raw)>200000:raise ValueError('Curva excede200 mil pontos; selecione uma execução menor explicitamente.')
        try:
            times=pd.to_datetime([p[0] for p in raw]);values=[_number(p[1]) for p in raw]
            if any(v is None for v in values) or times.hasnans or not times.is_monotonic_increasing:raise ValueError()
            curves[key]=downsample([[str(p[0]),v] for p,v in zip(raw,values)])
        except (ValueError,TypeError,IndexError):curves[key]=[];issues.append(key+': curva inválida')
    def last(key):return curves[key][-1][1] if curves[key] else None
    summary={key:_number(metrics.get(key)) for key in ('trades','net_profit','initial_equity','return_pct','max_drawdown_pct','win_rate','profit_factor',
        'max_floating_drawdown_pct','max_floating_drawdown_money','time_weighted_return_pct')}
    summary.update(final_balance=last('account_balance'),final_equity=last('floating_equity'))
    return dict(run_id=run['run_id'],robot_key=run['robot_key'],symbol=run['symbol'],timeframe=run['timeframe'],source=run['source'],
        interval=dict(start=run['data_start'],end=run['data_end'],bars=run['bars']),body_hash=body,revision_reason=reason,
        composition_hash=metrics.get('composition_hash'),composition=metrics.get('composition'),data_manifest_id=run.get('data_manifest_id'),
        effective_params=run.get('params'),
        profit_factor_status=metrics.get('profit_factor_status'),profit_factor_note=profit_factor.reason(metrics),
        context=metrics.get('comparison_context'),summary=summary,curves=curves,curve_issues=issues,
        currency=metrics.get('currency'),costs=_costs(metrics),valuation=run.get('valuation'),
        data_quality=metrics.get('data_quality'),instrument_context=_spec(metrics))


def scope_run(db, run, checkpoint=None):
    """Prova de pertencimento: tentativa publicada do journal, nunca o último/melhor run."""
    from mt5_extracao.robots import budget, backtest_scope
    receipt = run.get('metrics', {}).get('backtest_scope')
    if not isinstance(receipt, dict) or not receipt.get('batch_id'):
        raise ValueError('Execução sem vínculo comprovado ao plano revisado.')
    saved = checkpoint if checkpoint is not None else budget.checkpoint(db, receipt['batch_id'])
    if saved is None or saved['batch_id'] != receipt['batch_id']:
        raise ValueError('Journal da execução não encontrado nesta base.')
    review = saved['plan'].get('context', {}).get('review', {})
    if review.get('version') != backtest_scope.VERSION:
        raise ValueError('Revisão do plano ausente ou incompatível.')
    entry = next((e for e in saved['plan']['jobs'] if e['key'] == receipt.get('job_key')), None)
    head = next((h for h in saved['jobs'] if h['job_key'] == receipt.get('job_key')), None)
    if entry is None or head is None or head['run_id'] != run['run_id'] or head['status'] != 'completed':
        raise ValueError('Run não corresponde à tentativa concluída registrada no lote.')
    expected = dict(saved['plan']['context']['provenance'], batch_id=saved['batch_id'],
        plan_signature=saved['plan']['signature'], job_key=entry['key'], symbol=entry['symbol'],
        timeframe=entry['timeframe'], data_manifest_id=entry.get('data_manifest_id'))
    if _canonical(receipt) != _canonical(expected) or (run['robot_key'], run['symbol'], run['timeframe'], run.get('data_manifest_id')) != (
            entry['robot'], entry['symbol'], entry['timeframe'], entry.get('data_manifest_id')):
        raise ValueError('Preset, contexto ou dados do resultado divergem do plano revisado.')
    if entry.get('execution', {}).get('mode') == 'python' and _canonical(run.get('params')) != _canonical(entry['execution']['params']):
        raise ValueError('Parâmetros do resultado divergem do preset congelado.')
    if entry.get('execution', {}).get('mode') == 'python' and run['metrics'].get('composition') is not None:
        raise ValueError('Composição adicional não foi revisada neste plano de parâmetros Python.')
    return saved


def _observed_interval(row):
    from mt5_extracao.trade_analytics.trajectories.clocks import bar_end_in_source
    context = row['context']
    if not isinstance(context, dict) or context.get('engine') != 'python_ohlcv_stop_first':
        return None
    try:
        begin = pd.Timestamp(row['interval']['start'])
        finish = pd.Timestamp(bar_end_in_source(row['interval']['end'], row['timeframe']))
        if pd.notna(begin) and pd.notna(finish) and begin < finish:
            return dict(start=begin.isoformat(), end=finish.isoformat())
    except (ValueError, TypeError, OverflowError):
        pass
    return None


def _closing_valuations(run):
    frame = timeframes.parse(run['timeframe'])
    valuation = run.get('valuation') or {}
    raw = run.get('performance_equity') or []
    if (frame is None or frame.minutes >= 1440 or not raw or len(raw) != run['bars'] or not isinstance(valuation, dict)
            or valuation.get('resolution') != 'bar_close' or valuation.get('timestamp_basis') != 'bar_start'):
        return None
    try:
        opens = pd.DatetimeIndex(pd.to_datetime([point[0] for point in raw]))
        if (opens.hasnans or not opens.is_monotonic_increasing or not opens.is_unique
                or opens[0] != pd.Timestamp(run['data_start']) or opens[-1] != pd.Timestamp(run['data_end'])):
            return None
        step = pd.Timedelta(minutes=frame.minutes)
        values = pd.Series([_number(point[1]) for point in raw], index=opens + step, dtype=float)
        return frame.minutes, values
    except (ValueError, TypeError, IndexError, OverflowError):
        return None


def _returns_at_close(values, bars, horizon):
    previous = values.shift(bars)
    regular = values.index.to_series().diff().eq(horizon / bars).rolling(bars).sum().eq(bars)
    returns = values.div(previous.where(previous > 0)).sub(1)
    return returns.where(regular & (values > 0)).replace([math.inf, -math.inf], math.nan)


def _timeframe_relations(rows, valuations, compatible, guard):
    durations = {frame.name: frame.minutes for frame in timeframes.Timeframe}
    ordered = sorted(rows, key=lambda row: (durations.get(row['timeframe'], math.inf), row['timeframe'], row['run_id']))
    pairs = []
    for left, right in combinations(ordered, 2):
        guard()
        a, b = valuations.get(left['run_id']), valuations.get(right['run_id'])
        pair = dict(left_run_id=left['run_id'], right_run_id=right['run_id'], left_timeframe=left['timeframe'],
                    right_timeframe=right['timeframe'], horizon_minutes=None, n_pairs=0, coefficient=None)
        if not compatible:
            pair['status'] = 'INCOMPATIBLE_RECEIPTS'
        elif a is None or b is None:
            pair['status'] = 'NO_VERIFIED_INTRADAY_VALUATIONS'
        elif (a[1].index.tz is None) != (b[1].index.tz is None):
            pair['status'] = 'INCOMPATIBLE_CURVE_CLOCKS'
        else:
            minutes = math.lcm(a[0], b[0])
            horizon = pd.Timedelta(minutes=minutes)
            first = _returns_at_close(a[1], minutes // a[0], horizon).rename('left')
            second = _returns_at_close(b[1], minutes // b[0], horizon).rename('right')
            sample = pd.concat((first, second), axis=1, join='inner').dropna()
            pair.update(horizon_minutes=minutes, n_pairs=len(sample))
            if len(sample) < 30:
                pair['status'] = 'INSUFFICIENT_PAIRS'
            elif any(sample[key].nunique() < 2 for key in ('left', 'right')):
                pair['status'] = 'CONSTANT_SERIES'
            else:
                coefficient = _number(sample['left'].corr(sample['right']))
                pair.update(status='AVAILABLE' if coefficient is not None else 'UNDEFINED', coefficient=coefficient)
        pair['status_label'] = _RELATION_STATUS[pair['status']]
        pairs.append(pair)
    return dict(method='pearson', basis='unitized_equity_common_bar_closes', lag_minutes=0, minimum_pairs=30,
                predictive_status='NOT_EVALUATED_OUT_OF_SAMPLE', pairs=pairs)


def compare_runs(db,run_ids,*,mode,guard=None):
    """Seleção explícita de runs; compara recibos, não gera uma execução nova."""
    if mode not in ('same_asset','same_preset','same_strategy','same_scope'):
        raise ValueError('Modo exige same_asset, same_preset, same_strategy ou same_scope.')
    compare_timeframes = mode in ('same_strategy', 'same_scope')
    low, high = (1, 512) if mode == 'same_scope' else (2, 8)
    if not isinstance(run_ids,(list,tuple)) or not low<=len(run_ids)<=high or any(not isinstance(i,str) or not i.strip() for i in run_ids) or len(set(run_ids))!=len(run_ids):raise ValueError(f'Selecione{low} a{high} run_ids distintos.')
    if guard is None:
        from mt5_extracao.analytics import indicator_materializer
        started=dict(started_monotonic=time.monotonic())
        guard=lambda:indicator_materializer._guard(started,None,None,120,1024)
    from mt5_extracao.analytics import datasets
    from mt5_extracao.composition import Composition
    rows=[];problems=[];scope_checkpoint=None;valuations={}
    reference_bar_times = None
    for ident in run_ids:
        guard();found=store.load_runs(db,run_id=ident)
        if len(found)!=1:raise ValueError('Execução não encontrada: '+ident)
        run=found[0]
        row=_financial_row(run,varied_dimension='timeframe' if mode=='same_strategy' else 'symbol')
        rows.append(row);ctx=row['context']
        if mode == 'same_scope':
            scope_checkpoint = scope_run(db, run, scope_checkpoint)
            row['body_hash'] = run['metrics']['backtest_scope']['preset_identity']['content_hash']
            row['revision_reason'] = None
        else:
            try:
                comp=Composition(row['composition'])
                if comp.config_hash!=row['composition_hash']:raise ValueError('hash da composição diverge')
                if comp.to_dict()['context']!=dict(symbol=row['symbol'],timeframe=row['timeframe']):raise ValueError('contexto da composição diverge')
            except (ValueError,TypeError):problems.append(ident+': revisão de composição não comprovada')
        if compare_timeframes:
            row['observed_interval'] = _observed_interval(row)
        if mode == 'same_strategy':
            valuations[ident] = _closing_valuations(run)
        receipt=context_receipt_problems(ctx)
        problems.extend(ident+': '+p for p in receipt)
        if receipt==[MISSING_RECEIPT]:continue
        if not ctx.get('currency') or ctx.get('currency')!=row['currency']:problems.append(ident+': moeda da conta desconhecida/inconsistente')
        if row['curve_issues']:problems.append(ident+': curva inválida')
        if any(row['summary'][k] is None for k in ('trades','net_profit','initial_equity','max_drawdown_pct','final_balance','final_equity','max_floating_drawdown_pct')):problems.append(ident+': métricas/patrimônio completo ausentes')
        if not (row['summary']['initial_equity'] or 0)>0:problems.append(ident+': capital inicial inválido')
        try:
            snapshot=datasets.for_run(db,ident)
            if snapshot is None:raise ValueError('Snapshot indisponível')
            manifest=snapshot.manifest
            if (manifest['symbol'],manifest['timeframe'])!=(row['symbol'],row['timeframe']) or manifest['source']!=ctx['source'] or manifest['time']!=ctx['clock']:raise ValueError()
            if not (row['data_quality'] or {}).get('eligibility',{}).get('eligible'):raise ValueError()
            if mode != 'same_scope':
                period = manifest['period']
                interval = row['interval']
                if (snapshot.manifest_id != row['data_manifest_id'] or period['rows'] != interval['bars']
                        or pd.Timestamp(period['first']) != pd.Timestamp(interval['start'])
                        or pd.Timestamp(period['last']) != pd.Timestamp(interval['end'])):
                    raise ValueError('Período da execução não coincide com o snapshot')
            if mode == 'same_preset':
                bar_times = pd.DatetimeIndex(snapshot.bars['time'])
                if reference_bar_times is None:
                    reference_bar_times = bar_times
                elif not reference_bar_times.equals(bar_times):
                    problems.append(f'{ident}: Horários das barras diferem entre as execuções.')
        except (ValueError,TypeError,KeyError):problems.append(ident+': referência congelada/qualidade não comprovada')
    first=rows[0]
    for row in rows[1:]:
        if not compare_timeframes and (row['timeframe']!=first['timeframe'] or row['interval']!=first['interval']):problems.append('Períodos, quantidade de barras ou timeframes diferentes')
        if mode=='same_asset' and (row['symbol']!=first['symbol'] or row['data_manifest_id']!=first['data_manifest_id']):problems.append('Mesmo ativo exige o mesmo snapshot de dados')
        if mode=='same_preset' and (row['body_hash'] is None or row['body_hash']!=first['body_hash']):problems.append('Presets diferem além do ativo')
        if mode=='same_scope' and (row['body_hash'] is None or row['body_hash']!=first['body_hash']):problems.append('Composições diferem além do ativo/TF do mesmo plano')
        if mode=='same_strategy':
            if row['symbol'] != first['symbol']:
                problems.append('Comparação entre tempos gráficos exige o mesmo ativo')
            if (row['robot_key'] != first['robot_key'] or row['body_hash'] is None
                    or row['body_hash'] != first['body_hash']
                    or _canonical(row['effective_params']) != _canonical(first['effective_params'])):
                problems.append('Estratégia ou parâmetros diferem além do timeframe')
        a,b=first['context'] or {},row['context'] or {}
        problems.extend(context_differences(a,b))
        if mode=='same_asset' and a.get('instrument_spec')!=b.get('instrument_spec'):problems.append('Especificação do instrumento diferente')
    if mode=='same_preset' and len({r['symbol'] for r in rows})<2:problems.append('Mesmo preset entre ativos exige ao menos dois ativos')
    if mode=='same_strategy' and len({r['timeframe'] for r in rows}) != len(rows):
        problems.append('Escolha uma execução por timeframe')
    configuration_compatible=not problems
    if compare_timeframes:
        specs={}
        for row in rows:
            spec=comparable_instrument_spec((row['context'] or {}).get('instrument_spec'))
            if row['symbol'] in specs and specs[row['symbol']]!=spec:problems.append('Especificação do instrumento difere entre TFs do mesmo ativo')
            specs.setdefault(row['symbol'],spec)
        configuration_compatible=not problems
        if any(r['observed_interval'] is None for r in rows):problems.append('Cobertura temporal gravada não comprovada')
        elif any(r['observed_interval']!=first['observed_interval'] for r in rows[1:]):problems.append('Coberturas temporais diferentes; métricas permanecem nas janelas integrais de cada TF')
    guard();result=dict(schema_version=1,mode=mode,run_ids=list(run_ids),rows=rows,compatible=not problems,reasons=list(dict.fromkeys(problems)),
        opened_backtest=False,financial_qualification=False,ranked=False,
        limitations=['Resultados já gravados; nenhuma ordem, backtest ou conversão cambial nesta consulta.',
            'Composição, motor, custos, período, origem, relógio e moeda são comparados; recibo ausente não presume equivalência.',
            'Métricas vêm do motor canônico sobre a série completa. Curvas exibidas usam redução gráfica existente até240 pontos.',
            'Saldo de encerradas e patrimônio a mercado são separados; marcação por fechamento não mede drawdown intrabar.',
            'Compatibilidade de recibos não aprova estratégia, qualidade financeira ou desempenho futuro.'])
    if compare_timeframes:result['configuration_compatible']=configuration_compatible
    if mode == 'same_strategy':
        result['timeframe_relations'] = _timeframe_relations(rows, valuations, result['compatible'], guard)
    result['comparison_sha256']=hashlib.sha256(_canonical(result).encode()).hexdigest()
    return result


def validate_comparison(result):
    if not isinstance(result,dict) or result.get('comparison_sha256')!=hashlib.sha256(_canonical({k:v for k,v in result.items() if k!='comparison_sha256'}).encode()).hexdigest():raise ValueError('Comparação alterada ou sem recibo de integridade.')
    return json.loads(_canonical(result))
