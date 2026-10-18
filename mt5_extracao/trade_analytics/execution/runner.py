"""
Re-simular um run do livro (T16.6): junta o que o motor precisa a partir da base.

    result = resimulate(db, config, run_id, policy, mode="ponto_fixo")   # SimResult
    resimulate(..., save=True)   # grava no livro como source "resim:<modo>" (run_id "<run>#<modo>")

Barras: as do timeframe do run (as mesmas do backtest). Especificação e custos: os do backtester
(`specs.load_spec`, `research.backtest_config`). ATR do trailing: o da própria estratégia, quando o robô é Python;
senão o ATR(14) das barras.
"""
from typing import Optional

from mt5_extracao import timeframes
from mt5_extracao.trade_analytics import ledger
from mt5_extracao.trade_analytics.execution import engine

RESIM_PREFIX = "resim:"


def _source_record(db, run_id, source):
    from mt5_extracao.robots import store
    current = run_id
    while True:
        rows = store.load_runs(db, run_id=current)
        if rows:
            return rows[0]
        if not str(source).startswith(RESIM_PREFIX) or "#" not in current:
            return None
        current = current.rsplit("#", 1)[0]


def _cashflows(record, bars):
    import math
    import pandas as pd
    if record is None or record.get("account_cashflows") is None:
        return None
    flows = pd.Series(0.0, index=pd.DatetimeIndex(bars["time"]))
    try:
        for when, amount in record["account_cashflows"]:
            when = pd.Timestamp(when)
            if when not in flows.index or isinstance(amount, bool) or not math.isfinite(float(amount)):
                raise ValueError("fluxo fora da grade ou inválido")
            flows.loc[when] += float(amount)
    except (TypeError, ValueError, OverflowError) as error:
        raise ValueError("Fluxos externos do run não conciliam com as barras; recupere a origem antes de re-simular.") from error
    return flows


def base_policy(robot_key: str, params: Optional[dict] = None) -> engine.ExitPolicy:
    """Modo base: as regras de saída da estratégia Python; para os demais robôs, só stop, alvo e saídas originais."""
    if str(robot_key).startswith("python:"):
        from mt5_extracao.strategies import get_strategy
        return engine.ExitPolicy.from_rules(get_strategy(robot_key.split(":", 1)[1], **(params or {})).exit_rules())
    return engine.ExitPolicy()


def inputs(db, config, run_id: str, *, freeze_current: bool = False) -> dict:
    """{"trades", "bars", "spec", "cfg", "atr", "robot_key", "symbol", "timeframe"} de um run do livro."""
    from mt5_extracao.strategies import indicators, research, specs
    trades = ledger.load(db, run_id=run_id)
    if trades.empty:
        raise ValueError(f"Run {run_id} não está no livro de operações.")
    if trades["entry_time"].isna().all():
        raise ValueError(f"Nenhuma das {len(trades)} operações tem horário de entrada. Este run antigo só permite "
                         "análise pela saída. Escolha um run completo ou refaça o backtest com exportação completa.")
    first = trades.iloc[0]
    tf = timeframes.parse(first["timeframe"]) if first["timeframe"] else None
    if tf is None:
        raise ValueError(f"Run {run_id} sem timeframe conhecido.")
    from mt5_extracao.analytics import datasets
    record = _source_record(db, run_id, first.get("source"))
    snapshot = datasets.for_run(db, run_id)
    if snapshot is None and record is not None and record.get("data_manifest_id"):
        snapshot = datasets.for_run(db, record["run_id"])
    captured_current = snapshot is None and freeze_current
    if captured_current:
        # Esta revisão pertence à nova re-simulação, não ao backtest antigo.
        snapshot = datasets.capture(db, first["symbol"], tf)
    bars = snapshot.view().load_ohlcv(first["symbol"], tf.label) if snapshot is not None else db.load_ohlcv(
        first["symbol"], tf.label)
    if bars.empty:
        raise ValueError(f"Sem barras de {first['symbol']} {tf.label} na base: extraia o histórico antes de re-simular.")
    bars = bars.sort_values("time").reset_index(drop=True)
    atr = None
    if str(first["robot_key"]).startswith("python:"):
        from mt5_extracao.strategies import get_strategy
        try:
            atr = get_strategy(first["robot_key"].split(":", 1)[1]).gerar_sinais(bars)["atr"]
        except Exception:
            atr = None
    if atr is None:
        atr = indicators.atr(bars, 14)
    flows = _cashflows(record, bars)
    from mt5_extracao.analytics import data_eligibility
    quality = snapshot.quality if snapshot is not None else None
    if quality is not None:
        quality["eligibility"] = data_eligibility.evaluate(quality, ("ohlcv",))
        quality["data_manifest_id"] = snapshot.manifest_id
    return {"trades": trades, "bars": bars, "spec": snapshot.spec if snapshot is not None else specs.load_spec(db, first["symbol"]),
            "data_manifest_id": snapshot.manifest_id if snapshot is not None else None,
            "data_provenance_status": "current_data_captured_for_resimulation" if captured_current else
                                      "frozen_reference" if snapshot is not None else "legacy_current_data",
            "data_quality": quality,
            "cfg": research.backtest_config(config), "atr": atr, "robot_key": first["robot_key"],
            "symbol": first["symbol"], "timeframe": first["timeframe"], "cashflows": flows,
            "source_run_id": record["run_id"] if record else None,
            "cashflow_source": "recorded_run" if flows is not None else "legacy_unknown"}


def resimulate(db, config, run_id: str, policy: Optional[engine.ExitPolicy] = None, mode: str = "ponto_fixo",
               save: bool = False, data: Optional[dict] = None) -> engine.SimResult:
    data = data or inputs(db, config, run_id, freeze_current=save)
    policy = policy or base_policy(data["robot_key"])
    quality = data.get("data_quality")
    if quality is not None and not quality["eligibility"]["eligible"]:
        raise ValueError("Dados incompatíveis com a re-simulação: " + " ".join(quality["eligibility"]["reasons"]))
    result = engine.simulate(data["trades"], data["bars"], data["spec"], data["cfg"], policy, data["atr"],
                             cashflows=data.get("cashflows"),
                             observation_timeframe=data["timeframe"] if save else None)
    result.valuation["cashflow_source"] = data.get("cashflow_source", "unspecified")
    result.valuation["data_quality"] = quality
    if result.valuation["cashflow_source"] in ("legacy_unknown", "unspecified"):
        result.valuation["status"] = "limited"
    if save:
        if data.get("data_manifest_id") is not None:
            from mt5_extracao.analytics import datasets
            datasets.bind(db, f"{run_id}#{mode}", data["data_manifest_id"])
        ledger.save_trades(db, f"{run_id}#{mode}", result.trades, robot_key=data["robot_key"], symbol=data["symbol"],
                           timeframe=data["timeframe"], source=RESIM_PREFIX + mode)
        from dataclasses import asdict
        from mt5_extracao.robots.backtest import _execution_code_digest
        from mt5_extracao.trade_analytics.trajectories import store as trajectory_store
        from mt5_extracao.trade_analytics.trajectories.clocks import Clock
        result.trajectory["valuation"] = dict(result.valuation)
        trajectory_store.save(db, f"{run_id}#{mode}", result.trajectory, source=RESIM_PREFIX + mode,
                              clock=Clock("broker", db.broker_utc_offset) if data.get("data_manifest_id") is None else None,
                              producer_identity={"source_run_id": run_id, "code": _execution_code_digest(),
                                                 "data_provenance_status": data.get("data_provenance_status"),
                                                 "policy": asdict(policy), "settings": asdict(data["cfg"])})
    return result


def _frame(result):
    """Uma linha por saída, identificada pelo seq do livro (nunca pelo horário arredondado)."""
    import pandas as pd
    if not result.trades:
        raise ValueError(f"Nenhuma operação pôde ser re-simulada ({result.skipped} excluídas). Confira entrada, "
                         "direção, preço, stop, volume e cobertura das barras; refaça o backtest se faltarem dados.")
    return pd.DataFrame([dict(vars(trade), seq=seq) for trade, seq in zip(result.trades, result.source_seqs)])


def compare(db, config, run_id: str, modes_wanted=None, params: Optional[dict] = None, save: bool = False,
            robot=None, preset: Optional[str] = None) -> dict:
    """
    O mesmo run em vários modos, sobre as MESMAS entradas: {modo: {"result", "name", "metrics", ...}}.
    `params` = {modo: {parâmetro: valor}}. As barras e custos são lidos uma vez só. Martingale e anti-martingale só
    entram com `robot` que os suporte (as travas vêm dele); pedidos sem suporte são recusados com ValueError.
    """
    from mt5_extracao.trade_analytics import stats
    from mt5_extracao.trade_analytics.execution import modes as registry, sizing
    data = inputs(db, config, run_id, freeze_current=save)
    base = base_policy(data["robot_key"])
    initial = data["cfg"].initial_equity
    allowed = registry.available(robot, preset)
    wanted = list(modes_wanted or registry.exit_modes())      # sem lista: os modos de saída (como na T16.7)
    refused = [k for k in wanted if k not in allowed]
    if refused:
        raise ValueError(f"Modo(s) {', '.join(refused)} indisponível(is) para {data['robot_key']}: o robô não tem "
                         "parâmetro de martingale, ou o preset bloqueia preço médio.")
    out = {}
    base_result = None
    for key in wanted:
        mode = registry.MODES[key]
        values = dict(mode.params, **(params or {}).get(key, {}))
        if key in registry.SEQUENCE:
            base_result = base_result or resimulate(db, config, run_id, base, "ponto_fixo", data=data)
            frame = _frame(base_result)
            support = sizing.martingale_support(robot, preset)
            safety = sizing.robot_safety(robot, preset, config, data["spec"])
            safety.max_levels = int(values.get("niveis", safety.max_levels))
            seq = sizing.apply(frame, initial, support["multiplier"], safety, anti=registry.SEQUENCE[key])
            rows = seq["trades"][~seq["trades"]["skipped"]]
            base_volume = rows["seq"].map(frame.set_index("seq")["volume"])
            out[key] = {"result": None, "name": mode.name, "study_only": True, "sequence": seq,
                        "account_balance": None, "floating_equity": None,
                        "performance_equity": None, "account_cashflows": None,
                        "valuation": {"status": "unavailable", "closed_curve_basis": "closed_trades",
                                      "reason": "Estudo sequencial sem trajetória das posições abertas."},
                        "safety_notes": safety.notes, "frame": rows[["seq", "pnl", "entry_time", "exit_time"]].assign(run_id=run_id),
                        "metrics": stats.metrics(rows["pnl"], initial,
                                                 rows["r_multiple"] * rows["volume"] / base_volume)}
            if save:
                if data.get("data_manifest_id") is not None:
                    from mt5_extracao.analytics import datasets
                    datasets.bind(db, f"{run_id}#{key}", data["data_manifest_id"])
                ledger.save_trades(db, f"{run_id}#{key}", rows.to_dict("records"), robot_key=data["robot_key"],
                                   symbol=data["symbol"], timeframe=data["timeframe"], source=RESIM_PREFIX + key)
            continue
        policy = mode.policy(base, values)
        result = resimulate(db, config, run_id, policy, key, save=save, data=data)
        if key == "ponto_fixo":
            base_result = result
        frame = _frame(result)
        pnl = [t.pnl for t in result.trades]
        out[key] = {"result": result, "name": mode.name, "study_only": mode.study_only,
                    "metrics": {**stats.metrics(pnl, initial, [t.r_multiple for t in result.trades]),
                                **result.metrics},
                    "account_balance": result.account_balance, "floating_equity": result.floating_equity,
                    "performance_equity": result.performance_equity, "account_cashflows": result.account_cashflows,
                    "valuation": result.valuation,
                    "adds": result.adds, "capped": result.capped, "max_volume": result.max_volume,
                    "frame": frame[["seq", "pnl", "entry_time", "exit_time"]].assign(run_id=run_id)}
    return out
