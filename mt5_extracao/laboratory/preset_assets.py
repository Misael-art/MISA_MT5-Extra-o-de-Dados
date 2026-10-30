"""LAB-06: o mesmo robô e a mesma revisão de preset em ativos diferentes.

Lê execuções já gravadas. Não abre backtest, não usa fila e não trata o último
run de cada ativo como prova de que o preset é o mesmo.
"""
import hashlib
import json
import math
import time
import pandas as pd

from mt5_extracao.robots import store


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _body(run):
    """Hash da composição sem o ativo. O hash completo continua o de cada execução."""
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
        body["context"].pop("symbol")
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


def _financial_row(run):
    from mt5_extracao.robots.backtest import downsample
    metrics=run.get('metrics') or {};body,reason=_body(run)
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
    summary={key:_number(metrics.get(key)) for key in ('trades','net_profit','initial_equity','return_pct','max_drawdown_pct',
        'max_floating_drawdown_pct','max_floating_drawdown_money','time_weighted_return_pct')}
    summary.update(final_balance=last('account_balance'),final_equity=last('floating_equity'))
    return dict(run_id=run['run_id'],robot_key=run['robot_key'],symbol=run['symbol'],timeframe=run['timeframe'],source=run['source'],
        interval=dict(start=run['data_start'],end=run['data_end'],bars=run['bars']),body_hash=body,revision_reason=reason,
        composition_hash=metrics.get('composition_hash'),composition=metrics.get('composition'),data_manifest_id=run.get('data_manifest_id'),
        context=metrics.get('comparison_context'),summary=summary,curves=curves,curve_issues=issues,
        currency=metrics.get('currency'),costs=_costs(metrics),valuation=run.get('valuation'),
        data_quality=metrics.get('data_quality'),instrument_context=_spec(metrics))


def compare_runs(db,run_ids,*,mode,guard=None):
    """Seleção explícita de runs; compara recibos, não gera uma execução nova."""
    if mode not in ('same_asset','same_preset'):raise ValueError('Modo exige same_asset ou same_preset.')
    if not isinstance(run_ids,(list,tuple)) or not 2<=len(run_ids)<=8 or any(not isinstance(i,str) or not i.strip() for i in run_ids) or len(set(run_ids))!=len(run_ids):raise ValueError('Selecione2 a8 run_ids distintos.')
    if guard is None:
        from mt5_extracao.analytics import indicator_materializer
        started=dict(started_monotonic=time.monotonic())
        guard=lambda:indicator_materializer._guard(started,None,None,120,1024)
    from mt5_extracao.analytics import datasets
    from mt5_extracao.composition import Composition
    from dataclasses import asdict
    from mt5_extracao.strategies.backtester import BacktestConfig
    rows=[];problems=[];snapshots={}
    for ident in run_ids:
        guard();found=store.load_runs(db,run_id=ident)
        if len(found)!=1:raise ValueError('Execução não encontrada: '+ident)
        run=found[0];row=_financial_row(run);rows.append(row);ctx=row['context']
        try:
            comp=Composition(row['composition'])
            if comp.config_hash!=row['composition_hash']:raise ValueError('hash da composição diverge')
            if comp.to_dict()['context']!=dict(symbol=row['symbol'],timeframe=row['timeframe']):raise ValueError('contexto da composição diverge')
        except (ValueError,TypeError):problems.append(ident+': revisão de composição não comprovada')
        if not isinstance(ctx,dict) or ctx.get('schema_version')!=1 or not ctx.get('code_sha256'):
            problems.append(ident+': recibo de motor/configuração completo ausente');continue
        required={'engine','settings','trade_from','source','clock','instrument_spec','currency','currency_source','external_cashflows'}
        settings=ctx.get('settings')
        if (not required<=set(ctx) or ctx.get('engine')!='python_ohlcv_stop_first' or not isinstance(settings,dict) or set(settings)!=set(asdict(BacktestConfig()))):
            problems.append(ident+': configuração de execução incompleta/incompatível')
        if not isinstance(ctx.get('instrument_spec'),dict) or ctx['instrument_spec'].get('known') is not True:problems.append(ident+': especificação do instrumento desconhecida')
        if ctx.get('external_cashflows') is not False:problems.append(ident+': fluxos externos presentes/desconhecidos; comparação desta versão exige ausência explícita')
        if not ctx.get('currency') or ctx.get('currency')!=row['currency']:problems.append(ident+': moeda da conta desconhecida/inconsistente')
        if row['curve_issues']:problems.append(ident+': curva inválida')
        if any(row['summary'][k] is None for k in ('trades','net_profit','initial_equity','max_drawdown_pct','final_balance','final_equity','max_floating_drawdown_pct')):problems.append(ident+': métricas/patrimônio completo ausentes')
        if not (row['summary']['initial_equity'] or 0)>0:problems.append(ident+': capital inicial inválido')
        try:
            snapshot=datasets.load(db,row['data_manifest_id']);manifest=snapshot.manifest;snapshots[ident]=manifest
            if (manifest['symbol'],manifest['timeframe'])!=(row['symbol'],row['timeframe']) or manifest['source']!=ctx['source'] or manifest['time']!=ctx['clock']:raise ValueError()
            if not (row['data_quality'] or {}).get('eligibility',{}).get('eligible'):raise ValueError()
        except (ValueError,TypeError,KeyError):problems.append(ident+': referência congelada/qualidade não comprovada')
    first=rows[0]
    for row in rows[1:]:
        if row['timeframe']!=first['timeframe'] or row['interval']!=first['interval']:problems.append('Períodos, quantidade de barras ou timeframes diferentes')
        if mode=='same_asset' and (row['symbol']!=first['symbol'] or row['data_manifest_id']!=first['data_manifest_id']):problems.append('Mesmo ativo exige o mesmo snapshot de dados')
        if mode=='same_preset' and (row['body_hash'] is None or row['body_hash']!=first['body_hash']):problems.append('Presets diferem além do ativo')
        a,b=first['context'] or {},row['context'] or {}
        for key,label in [('engine','Motor'),('code_sha256','Código do motor'),('settings','Configuração de capital/risco/custos'),('trade_from','Início efetivo'),('source','Origem'),('clock','Relógio'),('currency','Moeda da conta')]:
            if a.get(key)!=b.get(key):problems.append(label+' diferente')
        if mode=='same_asset' and a.get('instrument_spec')!=b.get('instrument_spec'):problems.append('Especificação do instrumento diferente')
    if mode=='same_preset' and len({r['symbol'] for r in rows})<2:problems.append('Mesmo preset entre ativos exige ao menos dois ativos')
    guard();result=dict(schema_version=1,mode=mode,run_ids=list(run_ids),rows=rows,compatible=not problems,reasons=list(dict.fromkeys(problems)),
        opened_backtest=False,financial_qualification=False,ranked=False,
        limitations=['Resultados já gravados; nenhuma ordem, backtest ou conversão cambial nesta consulta.',
            'Composição, motor, custos, período, origem, relógio e moeda são comparados; recibo ausente não presume equivalência.',
            'Métricas vêm do motor canônico sobre a série completa. Curvas exibidas usam redução gráfica existente até240 pontos.',
            'Saldo de encerradas e patrimônio a mercado são separados; marcação por fechamento não mede drawdown intrabar.',
            'Compatibilidade de recibos não aprova estratégia, qualidade financeira ou desempenho futuro.'])
    result['comparison_sha256']=hashlib.sha256(_canonical(result).encode()).hexdigest()
    return result


def validate_comparison(result):
    if not isinstance(result,dict) or result.get('comparison_sha256')!=hashlib.sha256(_canonical({k:v for k,v in result.items() if k!='comparison_sha256'}).encode()).hexdigest():raise ValueError('Comparação alterada ou sem recibo de integridade.')
    return json.loads(_canonical(result))
