"""
Backtester barra a barra com custos reais e dimensionamento por risco.

    resultado = run_backtest(df, get_strategy("donchian"), spec, BacktestConfig(risk_per_trade=0.01))
    resultado.metrics["profit_factor"]

Premissas (conservadoras; ver docs/estrategias.md):
1. Barras do MT5 são BID. Compra entra/sai no ASK = bid + spread × point; venda no bid.
   O spread vem da coluna `spread` de cada barra (pontos) ou de spec.spread_points.
2. Sinal no fechamento da barra i -> execução na abertura da barra i+1 (+ slippage).
3. Stop e alvo tocados na mesma barra: conta o STOP. Gap além do stop executa na abertura.
4. Lote = capital × risco / perda por lote no stop, arredondado para baixo no volume_step.
   Abaixo do lote mínimo, a operação é pulada (skipped_min_lot).
5. Drawdown medido na curva de capital das operações fechadas.
"""
import math
from dataclasses import asdict, dataclass, field
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from .base import Strategy

EXIT_REASONS = {
    "stop": "stop", "breakeven": "stop no zero a zero", "trailing": "stop móvel", "target": "alvo",
    "signal": "sinal de saída", "time": "tempo máximo", "eod": "fim do dia", "end": "fim dos dados",
}


@dataclass
class SymbolSpec:
    symbol: str = ""
    point: float = 0.01
    digits: int = 2
    tick_size: float = 0.0        # 0 = igual ao point
    tick_value: float = 1.0       # valor de 1 tick para 1 lote, na moeda da conta
    volume_min: float = 1.0
    volume_step: float = 1.0
    volume_max: float = 1e6
    contract_size: float = 1.0
    currency_profit: str = ""
    spread_points: float = 0.0    # usado quando os dados não têm a coluna spread
    known: bool = True            # False = especificação genérica (rode `mt5x specs`)

    @property
    def tick(self) -> float:
        return self.tick_size if self.tick_size and self.tick_size > 0 else self.point

    @property
    def money_per_price_unit(self) -> float:
        """Valor (moeda da conta) de 1,0 de variação de preço para 1 lote."""
        return self.tick_value / self.tick

    def lots_for_risk(self, risk_money: float, stop_distance: float) -> float:
        """Maior lote (múltiplo de volume_step) cuja perda no stop não passa de risk_money; 0 se < mínimo."""
        if stop_distance <= 0 or risk_money <= 0:
            return 0.0
        raw = risk_money / (stop_distance * self.money_per_price_unit)
        step = self.volume_step if self.volume_step > 0 else self.volume_min
        vol = math.floor(raw / step + 1e-9) * step
        vol = min(round(vol, 8), self.volume_max)
        return vol if vol >= self.volume_min - 1e-12 else 0.0


@dataclass
class BacktestConfig:
    initial_equity: float = 100_000.0
    risk_per_trade: float = 0.01       # 1% do capital por operação
    slippage_points: float = 0.0       # por execução a mercado
    commission_per_lot: float = 0.0    # por lote e por lado, na moeda da conta
    compound: bool = True              # risco sobre o capital atual (False = capital inicial)


@dataclass
class Trade:
    direction: int
    entry_time: pd.Timestamp
    exit_time: pd.Timestamp
    entry_price: float
    exit_price: float
    volume: float
    pnl: float
    r_multiple: float
    bars: int
    exit_reason: str
    equity_before: float

    @property
    def return_pct(self) -> float:
        return self.pnl / self.equity_before if self.equity_before else 0.0


@dataclass
class BacktestResult:
    strategy: str
    trades: List[Trade]
    equity: pd.Series
    metrics: Dict[str, float]
    skipped_min_lot: int = 0
    skipped_gap: int = 0
    config: BacktestConfig = field(default_factory=BacktestConfig)

    def trades_frame(self) -> pd.DataFrame:
        rows = [asdict(t) for t in self.trades]
        cols = list(Trade.__dataclass_fields__)
        return pd.DataFrame(rows, columns=cols)


def compute_metrics(trades: List[Trade], initial_equity: float) -> Dict[str, float]:
    pnl = np.array([t.pnl for t in trades], dtype=float)
    r = np.array([t.r_multiple for t in trades], dtype=float)
    wins, losses = pnl[pnl > 0], pnl[pnl < 0]
    gross_profit, gross_loss = float(wins.sum()), float(-losses.sum())
    if gross_loss > 0:
        pf = gross_profit / gross_loss
    else:
        pf = math.inf if gross_profit > 0 else 0.0
    equity = initial_equity + np.concatenate([[0.0], np.cumsum(pnl)])
    peak = np.maximum.accumulate(equity)
    dd = np.where(peak > 0, (peak - equity) / peak, 0.0)
    reasons: Dict[str, int] = {}
    for t in trades:
        reasons[t.exit_reason] = reasons.get(t.exit_reason, 0) + 1
    return {
        "trades": int(len(trades)),
        "win_rate": float(len(wins) / len(trades)) if trades else 0.0,
        "profit_factor": float(pf),
        "payoff": float(wins.mean() / -losses.mean()) if len(wins) and len(losses) else 0.0,
        "expectancy_r": float(r.mean()) if len(r) else 0.0,
        "total_r": float(r.sum()),
        "net_profit": float(pnl.sum()),
        "return_pct": float(pnl.sum() / initial_equity) if initial_equity else 0.0,
        "max_drawdown_pct": float(dd.max()),
        "avg_bars": float(np.mean([t.bars for t in trades])) if trades else 0.0,
        "exit_reasons": reasons,
    }


def run_backtest(df: pd.DataFrame, strategy: Strategy, spec: Optional[SymbolSpec] = None,
                 config: Optional[BacktestConfig] = None, start_index: int = 0,
                 signals: Optional[pd.DataFrame] = None) -> BacktestResult:
    """
    start_index: só abre operações a partir desta barra (as anteriores servem de aquecimento
    dos indicadores; usado no walk-forward).
    """
    spec = spec or SymbolSpec(known=False)
    cfg = config or BacktestConfig()
    data = df.reset_index(drop=True)
    sig = (signals if signals is not None else strategy.gerar_sinais(data)).reset_index(drop=True)
    rules = strategy.exit_rules()
    n = len(data)

    o, h, l, c = (data[k].to_numpy(dtype=float) for k in ("open", "high", "low", "close"))
    times = pd.to_datetime(data["time"])
    if "spread" in data.columns:
        spread_pts = data["spread"].fillna(spec.spread_points).to_numpy(dtype=float)
    else:
        spread_pts = np.full(n, float(spec.spread_points))
    spr = spread_pts * spec.point
    slip = cfg.slippage_points * spec.point
    ppu = spec.money_per_price_unit
    days = times.dt.normalize().to_numpy()
    last_of_day = np.append(days[1:] != days[:-1], True)

    entry_long, entry_short = sig["entry_long"].to_numpy(), sig["entry_short"].to_numpy()
    exit_long, exit_short = sig["exit_long"].to_numpy(), sig["exit_short"].to_numpy()
    stop_long, stop_short = sig["stop_long"].to_numpy(dtype=float), sig["stop_short"].to_numpy(dtype=float)
    tgt_long, tgt_short = sig["target_long"].to_numpy(dtype=float), sig["target_short"].to_numpy(dtype=float)
    atr = sig["atr"].to_numpy(dtype=float)

    equity = cfg.initial_equity
    trades: List[Trade] = []
    skipped_min_lot = skipped_gap = 0
    pos: Optional[dict] = None
    pending_entry = 0
    pending_exit = False

    def fill_exit(d, bid_price, i, market=True):
        price = bid_price + (spr[i] if d < 0 else 0.0)   # venda a descoberto recompra no ask
        return price - d * slip if market else price

    def close_part(volume, price):
        pos["pnl"] += (price - pos["entry"]) * pos["dir"] * ppu * volume - cfg.commission_per_lot * volume
        pos["remaining"] = round(pos["remaining"] - volume, 8)

    def close_all(i, price, reason):
        nonlocal equity, pos
        close_part(pos["remaining"], price)
        equity += pos["pnl"]
        trades.append(Trade(direction=pos["dir"], entry_time=times.iloc[pos["i"]], exit_time=times.iloc[i],
                            entry_price=pos["entry"], exit_price=float(price), volume=pos["volume"],
                            pnl=float(pos["pnl"]), r_multiple=float(pos["pnl"] / pos["risk_money"]),
                            bars=i - pos["i"] + 1, exit_reason=EXIT_REASONS[reason],
                            equity_before=pos["equity_before"]))
        pos = None

    for i in range(n):
        # 1) saída por sinal pendente, na abertura
        if pos is not None and pending_exit:
            close_all(i, fill_exit(pos["dir"], o[i], i), "signal")
        pending_exit = False

        # 2) entrada pendente, na abertura
        if pos is None and pending_entry:
            d, s = pending_entry, i - 1
            entry = o[i] + spr[i] + slip if d > 0 else o[i] - slip
            stop = stop_long[s] if d > 0 else stop_short[s]
            risk_px = (entry - stop) * d
            if not np.isfinite(risk_px) or risk_px <= 0:
                skipped_gap += 1     # abriu além do stop
            else:
                base = equity if cfg.compound else cfg.initial_equity
                volume = spec.lots_for_risk(base * cfg.risk_per_trade, risk_px)
                if volume <= 0:
                    skipped_min_lot += 1
                else:
                    target = tgt_long[s] if d > 0 else tgt_short[s]
                    if not np.isfinite(target) and rules.target_r:
                        target = entry + d * rules.target_r * risk_px
                    pos = {"dir": d, "entry": entry, "stop": stop, "stop_kind": "stop",
                           "target": target if np.isfinite(target) else None, "volume": volume,
                           "remaining": volume, "risk_px": risk_px, "risk_money": risk_px * ppu * volume,
                           "pnl": -cfg.commission_per_lot * volume, "i": i, "partial_done": False,
                           "equity_before": equity}
        pending_entry = 0

        # 3) stop, parcial e alvo dentro da barra
        if pos is not None:
            d = pos["dir"]
            # preços do lado que fecha a posição: compra fecha no bid, venda no ask
            lo, hi, op = (l[i], h[i], o[i]) if d > 0 else (l[i] + spr[i], h[i] + spr[i], o[i] + spr[i])
            stop_hit = lo <= pos["stop"] if d > 0 else hi >= pos["stop"]
            if stop_hit:
                level = min(op, pos["stop"]) if d > 0 else max(op, pos["stop"])
                close_all(i, level - d * slip, pos["stop_kind"])
            else:
                if rules.partial_r and not pos["partial_done"]:
                    level = pos["entry"] + d * rules.partial_r * pos["risk_px"]
                    if (hi >= level) if d > 0 else (lo <= level):
                        pos["partial_done"] = True
                        step = spec.volume_step or spec.volume_min
                        part = math.floor(pos["volume"] * rules.partial_fraction / step + 1e-9) * step
                        if part >= spec.volume_min and pos["remaining"] - part >= spec.volume_min - 1e-12:
                            close_part(part, max(op, level) if d > 0 else min(op, level))
                        if rules.breakeven_after_partial:
                            pos["stop"] = max(pos["stop"], pos["entry"]) if d > 0 else min(pos["stop"], pos["entry"])
                            pos["stop_kind"] = "breakeven"
                tgt = pos["target"]
                if tgt is not None and ((hi >= tgt) if d > 0 else (lo <= tgt)):
                    close_all(i, max(op, tgt) if d > 0 else min(op, tgt), "target")

        # 4) no fechamento: trailing, tempo, fim do dia, sinal
        if pos is not None:
            d = pos["dir"]
            if rules.trailing_atr and np.isfinite(atr[i]):
                new = c[i] - d * rules.trailing_atr * atr[i]
                if (new > pos["stop"]) if d > 0 else (new < pos["stop"]):
                    pos["stop"], pos["stop_kind"] = new, "trailing"
            if rules.max_bars and i - pos["i"] + 1 >= rules.max_bars:
                close_all(i, fill_exit(d, c[i], i), "time")
            elif rules.eod_exit and last_of_day[i]:
                close_all(i, fill_exit(d, c[i], i), "eod")
            elif (exit_long[i] if d > 0 else exit_short[i]):
                pending_exit = True

        # 5) novos sinais de entrada (executados na próxima abertura)
        if i + 1 >= start_index and i < n - 1 and (pos is None or pending_exit):
            if not (rules.eod_exit and last_of_day[i]):
                want = 1 if entry_long[i] else (-1 if entry_short[i] else 0)
                if want and (pos is None or want != pos["dir"]):
                    pending_entry = want

    if pos is not None:
        close_all(n - 1, fill_exit(pos["dir"], c[n - 1], n - 1), "end")

    eq_times = [times.iloc[0]] + [t.exit_time for t in trades] if n else []
    eq_vals = [cfg.initial_equity] + list(cfg.initial_equity + np.cumsum([t.pnl for t in trades])) if n else []
    return BacktestResult(strategy=strategy.describe(), trades=trades,
                          equity=pd.Series(eq_vals, index=pd.DatetimeIndex(eq_times), dtype=float),
                          metrics=compute_metrics(trades, cfg.initial_equity),
                          skipped_min_lot=skipped_min_lot, skipped_gap=skipped_gap, config=cfg)
