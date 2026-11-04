"""LAB-06: o mesmo robô e a mesma revisão de preset em ativos diferentes.

Lê execuções já gravadas. Não abre backtest, não usa fila e não trata o último
run de cada ativo como prova de que o preset é o mesmo.
"""
import hashlib
import json
import math

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
    body = json.loads(_canonical(composition))
    body["context"].pop("symbol")
    try:
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
