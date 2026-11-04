"""
Matriz de resultados: ativo x timeframe x robô, para ver o conjunto de uma vez (painel `mt5x panel`).

    m = build(db, config)
    m["cells"]                      # uma por (robô, ativo, timeframe) com backtest gravado
    positives(m, "mql5:meurobo")    # onde esse robô teve resultado positivo, do melhor para o pior

Três regras para a matriz não enganar (docs/PLANO_DE_TRABALHO.md, "Painel macro de resultados"):
1. "Positivo" não é "aprovado": APPROVED só para quem passou na bateria (robô MQL5) ou na validação (estratégia
   Python) naquele ativo e timeframe.
2. Com menos de MIN_TRADES operações a célula é FEW_TRADES, qualquer que seja o lucro. E um backtest Python de
   ativo sem especificação de contrato é UNRELIABLE: os valores em dinheiro não valem (visto no host: profit
   factor 0,00 em GBPUSD só por falta da especificação).
3. Cada célula diz de onde o número veio (`origin`): Python é otimista em custo; no Strategy Tester, o modelo.

Cada célula traz a execução MAIS RECENTE da combinação, não a melhor. Só leitura: não roda teste nenhum.
"""
from datetime import datetime
from typing import Dict, List, Optional

from . import catalog, store
from mt5_extracao import profit_factor

MIN_TRADES = 30
APPROVED, REJECTED, PROMISING, FEW_TRADES, NEGATIVE = "aprovado", "reprovado", "promissor", "poucas operações", "negativo"
UNRELIABLE = "não confiável"
UNTESTED, NO_DATA = "sem teste", "sem base"
STATUS_LABELS = {
    APPROVED: "Passou na bateria de segurança (robô MQL5) ou na validação (estratégia Python) neste ativo e timeframe",
    REJECTED: "Foi avaliado neste ativo e timeframe e reprovou",
    PROMISING: "Deu lucro com 30 ou mais operações, mas ainda não passou pela bateria ou pela validação",
    FEW_TRADES: "Menos de 30 operações: o resultado não prova nada, qualquer que seja",
    NEGATIVE: "Perdeu dinheiro no teste",
    UNRELIABLE: "Backtest Python de um ativo sem especificação de contrato: lote e valores são aproximados. Rode "
                "'mt5x specs --symbols <ativo>' e teste de novo",
    UNTESTED: "Há barras na base, mas este robô ainda não foi testado aqui",
    NO_DATA: "Não há barras deste ativo neste timeframe na base",
}
PYTHON_ORIGIN = "backtester Python (otimista em custo)"


def _timeframe_order() -> Dict[str, int]:
    from mt5_extracao.timeframes import Timeframe
    return {tf.name: tf.minutes for tf in Timeframe}


def _status(net: float, trades: int, verdict: Optional[bool], reliable: bool = True) -> str:
    if not reliable:
        return UNRELIABLE
    if trades < MIN_TRADES:
        return FEW_TRADES
    if verdict is True:
        return APPROVED
    if verdict is False:
        return REJECTED
    return PROMISING if net > 0 else NEGATIVE


def build(db, config=None, robots: Optional[List] = None) -> dict:
    """
    {"generated_at", "timeframes" (do menor para o maior), "symbols", "inventory": {ativo: {tf: {...}}},
     "robots": [{key, name, kind, kind_label, stage}], "cells": [...], "statuses": {situação: explicação}}.
    Robôs sem nenhum backtest aparecem em "robots" mesmo assim, para o painel mostrar o que falta testar.
    """
    from mt5_extracao import resample, timeframes
    from mt5_extracao.strategies.validation import load_validations
    robots = catalog.load_catalog(config, db) if robots is None else list(robots)
    by_key = {r.key: r for r in robots}
    order = _timeframe_order()

    built = resample.origins(db)
    inventory: Dict[str, Dict[str, dict]] = {}
    for item in db.data_inventory():
        tf = timeframes.parse(item["timeframe"])
        if tf is None or not item["rows"]:
            continue
        inventory.setdefault(item["symbol"], {})[tf.name] = {
            "bars": item["rows"], "start": str(item["start"])[:10], "end": str(item["end"])[:10],
            "built_from": built.get((item["symbol"], tf.name), "")}

    # veredito por (robô, ativo, timeframe): a bateria mais recente (MQL5) ou a validação (Python)
    verdicts: Dict[tuple, dict] = {}
    runs = store.load_runs(db)
    for check in reversed(store.load_checks(db, runs=runs)):      # da mais antiga para a mais recente: a última vence
        robot = by_key.get(check["robot_key"])
        if robot is None or not check.get("symbol"):
            continue
        current = not check.get("fingerprint") or check["fingerprint"] == catalog.fingerprint(robot)
        verdicts[(check["robot_key"], check["symbol"], check.get("timeframe", ""))] = {
            "approved": bool(check["approved"]) and current, "when": check["created_at"][:16],
            "kind": "bateria de segurança" + ("" if current else " (o código mudou depois: não vale mais)")}
    validations = load_validations(db)
    for row in (validations.itertuples() if validations is not None and len(validations) else ()):
        key = f"{catalog.PYTHON}:{row.strategy}"
        if key in by_key:
            verdicts[(key, row.symbol, row.timeframe)] = {"approved": bool(row.approved), "when": str(row.created_at)[:16],
                                                          "kind": "validação fora da amostra"}

    cells, seen = [], set()
    for run in runs:                                             # mais recentes primeiro
        robot = by_key.get(run["robot_key"])
        combo = (run["robot_key"], run["symbol"], run["timeframe"])
        if robot is None or run["source"] in catalog.HIDDEN_RUN_SOURCES or combo in seen:
            continue
        seen.add(combo)
        m = run["metrics"]
        trades, net = int(m.get("trades", 0)), float(m.get("net_profit", 0.0))
        verdict = verdicts.get(combo)
        if verdict and profit_factor.finite(m.get("profit_factor")) is None:
            verdict = {**verdict, "approved": False, "kind": verdict["kind"] + "; PF indefinido neste backtest"}
        origin = PYTHON_ORIGIN if robot.kind == catalog.PYTHON else \
            "Strategy Tester, " + str(m.get("tester_model") or "modelo não registrado")
        cells.append({
            "robot": run["robot_key"], "symbol": run["symbol"], "timeframe": run["timeframe"],
            "status": _status(net, trades, verdict["approved"] if verdict else None,
                              reliable=robot.kind != catalog.PYTHON or bool(m.get("spec_known", True))),
            "trades": trades, "win_rate": float(m.get("win_rate", 0.0)),
            "profit_factor": profit_factor.finite(m.get("profit_factor")),
            "profit_factor_status": m.get("profit_factor_status", "legacy_reported"),
            "profit_factor_note": profit_factor.reason(m), "return_pct": float(m.get("return_pct", 0.0)),
            "net_profit": net, "max_drawdown_pct": float(m.get("max_drawdown_pct", 0.0)),
            "period": f"{run['data_start'][:10]} a {run['data_end'][:10]}", "tested_at": run["created_at"][:16],
            "origin": origin, "preset": run.get("params", {}).get("preset", "") if robot.kind == catalog.MQL5 else "",
            "verdict": f"{verdict['kind']} em {verdict['when']}" if verdict else ""})

    symbols = sorted(set(inventory) | {c["symbol"] for c in cells})
    used = {tf for per in inventory.values() for tf in per} | {c["timeframe"] for c in cells}
    return {"generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "timeframes": sorted(used, key=lambda name: order.get(name, 10 ** 9)), "symbols": symbols,
            "inventory": inventory, "min_trades": MIN_TRADES, "statuses": STATUS_LABELS,
            "robots": [{"key": r.key, "name": r.name, "kind": r.kind, "kind_label": r.kind_label, "stage": r.stage_label,
                        "tested": sum(1 for c in cells if c["robot"] == r.key)} for r in robots],
            "cells": cells}


def cell_status(matrix: dict, robot_key: str, symbol: str, timeframe: str) -> str:
    """A situação de uma posição da matriz, inclusive quando não há teste (UNTESTED) nem base (NO_DATA)."""
    for cell in matrix["cells"]:
        if (cell["robot"], cell["symbol"], cell["timeframe"]) == (robot_key, symbol, timeframe):
            return cell["status"]
    return UNTESTED if timeframe in matrix["inventory"].get(symbol, {}) else NO_DATA


def positives(matrix: dict, robot_key: str) -> List[dict]:
    """
    Visão por robô: as combinações em que ele teve resultado positivo com operações suficientes, as aprovadas
    primeiro e depois pelo profit factor. Uma célula reprovada não entra, mesmo com lucro.
    """
    rows = [c for c in matrix["cells"] if c["robot"] == robot_key and c["status"] in (APPROVED, PROMISING)]
    return sorted(rows, key=lambda c: (c["status"] != APPROVED, profit_factor.sort_key(c["profit_factor"]), -c["return_pct"]))


def compare_scope(db, batch_id, *, guard=None):
    """Matriz do plano escolhido e métricas gravadas; nenhum executor é chamado."""
    import hashlib
    import time
    import pandas as pd
    from . import budget, backtest_scope
    from mt5_extracao.laboratory import preset_assets
    if guard is None:
        from mt5_extracao.analytics import indicator_materializer
        started = dict(started_monotonic=time.monotonic())
        guard = lambda: indicator_materializer._guard(started, None, None, 120, 1024)
    guard()
    plan = backtest_scope.inspect_plan(db, batch_id)
    saved = budget.checkpoint(db, batch_id)
    if len(plan['rows']) > 512:
        raise ValueError('Comparação excede512 células; escolha um lote menor explicitamente.')
    heads = {(e['symbol'], e['timeframe']): e for e in plan['executions']}
    cells, records = [], []
    for item in plan['rows']:
        guard()
        head = heads.get((item['symbol'], item['timeframe']), {})
        state = head.get('status', 'pending') if item['eligible'] else 'excluded'
        reason = item['reason'] or (head.get('history') or [{}])[-1].get('reason', '')
        cell = dict(symbol=item['symbol'], timeframe=item['timeframe'], state=state,
                    reason=reason, limitations=item['limitations'], bars=item['bars'],
                    data_manifest_id=item.get('data_manifest_id'), run_id=head.get('run_id'), result=None,
                    history=head.get('history', []))
        if state == 'completed':
            found = store.load_runs(db, run_id=head['run_id'])
            if not found:
                cell.update(state='missing_result', reason='Run registrado no journal não está disponível nesta base.')
            else:
                try:
                    preset_assets.scope_run(db, found[0], saved)
                    records.append(found[0]['run_id'])
                except ValueError as error:
                    cell.update(state='inconsistent', reason=str(error))
        cells.append(cell)
    order = _timeframe_order()
    cells.sort(key=lambda c: (c['symbol'], order.get(c['timeframe'], 10**9)))
    comparison = preset_assets.compare_runs(db, records, mode='same_scope', guard=guard) if records else dict(
        rows=[], compatible=False, reasons=['Sem resultados concluídos e comprovados neste lote.'], limitations=[])
    by_id = {r['run_id']: r for r in comparison['rows']}
    for cell in cells:
        if cell['run_id'] in by_id:
            cell['result'] = by_id[cell['run_id']]
    intervals = [r['observed_interval'] for r in comparison['rows'] if r.get('observed_interval')]
    coverage = None
    if intervals:
        try:
            begin = max(pd.Timestamp(i['start']) for i in intervals)
            end = min(pd.Timestamp(i['end']) for i in intervals)
            if pd.isna(begin) or pd.isna(end):raise ValueError('Janela ausente')
            coverage = dict(start=str(begin), end=str(end), overlaps=begin <= end,
                            basis='Interseção da cobertura das barras (fim de barra pelo calendário do TF); métricas NÃO recalculadas nesta janela.',
                            covers_requested_window=begin<=pd.Timestamp(plan['start']) and end>=pd.Timestamp(plan['end']))
            if not coverage['overlaps']:
                comparison['compatible']=False;comparison['reasons'].append('Janelas gravadas sem interseção comum')
        except (ValueError,TypeError):
            comparison['compatible']=False;comparison['reasons'].append('Janelas gravadas ausentes ou relógios incompatíveis')
    counts = {state: sum(c['state'] == state for c in cells) for state in sorted({c['state'] for c in cells})}
    if counts.get('inconsistent') or counts.get('missing_result'):
        comparison['compatible']=False;comparison['reasons'].append('Células sem resultado ou vínculo comprovado permanecem separadas')
    result = dict(schema_version=1, mode='same_scope', batch_id=batch_id, plan_signature=plan['signature'],
        plan_status=plan['status'], preset_identity=plan['preset_identity'], robot=plan['robot'], robot_name=plan['robot_name'],
        symbols=sorted(plan['symbols']), timeframes=sorted({c['timeframe'] for c in cells}, key=lambda t: order.get(t, 10**9)),
        interval=dict(start=plan['start'], end=plan['end']), settings=plan['settings'], model=plan['model'],
        cost_basis=plan['cost_basis'], cells=cells, counts=counts, coverage=coverage,
        rows=comparison['rows'], run_ids=records, compatible=comparison['compatible'], reasons=comparison['reasons'],
        configuration_compatible=comparison.get('configuration_compatible',False),
        opened_backtest=False, ranked=False, financial_qualification=False,
        limitations=comparison['limitations'] + ['Tentativas identificadas pelo journal do lote; sem escolher o último/melhor run de cada TF.',
            'Cada métrica usa a janela integral gravada do seu TF; a interseção é informativa, sem simular uma janela comum.',
            'Pendente, excluído, interrompido, falha, resultado ausente e zero operações são estados distintos.'])
    result['comparison_sha256'] = hashlib.sha256(preset_assets._canonical(result).encode()).hexdigest()
    guard()
    return result
