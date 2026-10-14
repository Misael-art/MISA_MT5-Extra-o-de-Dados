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


def base_policy(robot_key: str, params: Optional[dict] = None) -> engine.ExitPolicy:
    """Modo base: as regras de saída da estratégia Python; para os demais robôs, só stop, alvo e saídas originais."""
    if str(robot_key).startswith("python:"):
        from mt5_extracao.strategies import get_strategy
        return engine.ExitPolicy.from_rules(get_strategy(robot_key.split(":", 1)[1], **(params or {})).exit_rules())
    return engine.ExitPolicy()


def inputs(db, config, run_id: str) -> dict:
    """{"trades", "bars", "spec", "cfg", "atr", "robot_key", "symbol", "timeframe"} de um run do livro."""
    from mt5_extracao.strategies import indicators, research, specs
    trades = ledger.load(db, run_id=run_id)
    if trades.empty:
        raise ValueError(f"Run {run_id} não está no livro de operações.")
    first = trades.iloc[0]
    tf = timeframes.parse(first["timeframe"]) if first["timeframe"] else None
    if tf is None:
        raise ValueError(f"Run {run_id} sem timeframe conhecido.")
    bars = db.load_ohlcv(first["symbol"], tf.label)
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
    return {"trades": trades, "bars": bars, "spec": specs.load_spec(db, first["symbol"]),
            "cfg": research.backtest_config(config), "atr": atr, "robot_key": first["robot_key"],
            "symbol": first["symbol"], "timeframe": first["timeframe"]}


def resimulate(db, config, run_id: str, policy: Optional[engine.ExitPolicy] = None, mode: str = "ponto_fixo",
               save: bool = False, data: Optional[dict] = None) -> engine.SimResult:
    data = data or inputs(db, config, run_id)
    policy = policy or base_policy(data["robot_key"])
    result = engine.simulate(data["trades"], data["bars"], data["spec"], data["cfg"], policy, data["atr"])
    if save:
        ledger.save_trades(db, f"{run_id}#{mode}", result.trades, robot_key=data["robot_key"], symbol=data["symbol"],
                           timeframe=data["timeframe"], source=RESIM_PREFIX + mode)
    return result


def compare(db, config, run_id: str, modes_wanted=None, params: Optional[dict] = None, save: bool = False) -> dict:
    """
    O mesmo run em vários modos, sobre as MESMAS entradas: {modo: {"result": SimResult, "metrics": {...}}}.
    `params` = {modo: {parâmetro: valor}}. As barras e custos são lidos uma vez só.
    """
    from mt5_extracao.trade_analytics import stats
    from mt5_extracao.trade_analytics.execution import modes as registry
    data = inputs(db, config, run_id)
    base = base_policy(data["robot_key"])
    initial = data["cfg"].initial_equity
    out = {}
    for key in modes_wanted or list(registry.exit_modes()):
        mode = registry.MODES[key]
        policy = mode.policy(base, (params or {}).get(key))
        result = resimulate(db, config, run_id, policy, key, save=save, data=data)
        pnl = [t.pnl for t in result.trades]
        out[key] = {"result": result, "name": mode.name,
                    "metrics": stats.metrics(pnl, initial, [t.r_multiple for t in result.trades])}
    return out
