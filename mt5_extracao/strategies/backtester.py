"""
Backtester barra a barra com custos reais e dimensionamento por risco.

    resultado = run_backtest(df, get_strategy("donchian"), spec, BacktestConfig(risk_per_trade=0.01))
    resultado.metrics["profit_factor"]

Premissas (conservadoras; ver docs/estrategias.md):
1. Barras do MT5 são BID. Compra entra no ASK e sai no BID; venda entra no BID e sai no ASK.
   ASK = bid + spread × point.
   O spread vem da coluna `spread` de cada barra (pontos) ou de spec.spread_points. Essa coluna é o MENOR
   spread do período da barra; com o spread medido nos ticks (spec.spread_by_hour, de `mt5x specs --calibrate`),
   cada execução paga o maior dos dois: o da barra ou o típico daquela hora (na abertura de uma barra que
   começa na hora cheia, o do primeiro tick da hora, que na virada do dia é bem maior).
2. Sinal no fechamento da barra i -> execução na abertura da barra i+1 (+ slippage).
3. Stop e alvo tocados na mesma barra: conta o STOP. Gap além do stop executa na abertura.
4. Lote = capital × risco / perda por lote no stop, arredondado para baixo no volume_step.
   Abaixo do lote mínimo, a operação é pulada (skipped_min_lot).
5. Drawdown medido na curva de capital das operações fechadas.
6. Swap: a posição que atravessa a virada do dia paga (ou recebe) o swap do símbolo, em pontos por lote por
   noite, triplo no dia da semana informado pela corretora. Só o modo "em pontos" é calculado.
"""
import math
from dataclasses import asdict, dataclass, field
from decimal import Decimal, ROUND_FLOOR
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from .base import ExitRules, Strategy
from .equity import cashflow_values, floating_metrics, unitized_equity, valuation_metadata

EXIT_REASONS = {
    "stop": "stop", "breakeven": "stop no zero a zero", "trailing": "stop móvel", "target": "alvo",
    "time_absolute":"prazo absoluto", "time_progress":"prazo sem lucro mínimo",
    "time_gain":"prazo de ganho", "time_loss":"prazo de perda", "time_tie":"prazo de empate", "time_er":"eficiência baixa após idade mínima", "time_donchian":"retorno ao canal sem renovação", "time_atr":"horizonte ATR limitado",
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
    swap_long: float = 0.0        # swap de uma noite comprado, por lote (em pontos, se swap_mode == 1)
    swap_short: float = 0.0       # swap de uma noite vendido
    swap_mode: int = 0            # SYMBOL_SWAP_MODE do MT5: 1 = em pontos (o único calculado aqui)
    swap_triple_day: int = 3      # dia do swap triplo, como no MT5 (0 = domingo ... 3 = quarta)
    spread_by_hour: Optional[tuple] = None   # spread real por hora do dia, em pontos (mt5x specs --calibrate)
    spread_at_open: Optional[tuple] = None   # spread no primeiro tick de cada hora (o que a abertura da barra paga)
    known: bool = True            # False = especificação genérica (rode `mt5x specs`)
    provenance: dict = field(default_factory=dict)  # Vigência/conhecimento separados da disponibilidade atual.

    def swap_nights(self, entry_time, exit_time) -> int:
        """Viradas de dia entre a entrada e a saída, contando três na noite do swap triplo; fim de semana não conta."""
        day, last = pd.Timestamp(entry_time).normalize(), pd.Timestamp(exit_time).normalize()
        nights = 0
        while day < last:
            if day.weekday() < 5:                                   # segunda a sexta
                nights += 3 if (day.weekday() + 1) % 7 == self.swap_triple_day else 1
            day += pd.Timedelta(days=1)
        return nights

    def swap_money(self, direction: int, volume: float, entry_time, exit_time) -> float:
        """Swap da posição na moeda da conta (negativo = custo). Zero se o modo do símbolo não é em pontos."""
        rate = self.swap_long if direction > 0 else self.swap_short
        if self.swap_mode != 1 or not rate:
            return 0.0
        return rate * self.point * self.money_per_price_unit * volume * self.swap_nights(entry_time, exit_time)

    @property
    def tick(self) -> float:
        return self.tick_size if self.tick_size and self.tick_size > 0 else self.point

    @property
    def money_per_price_unit(self) -> float:
        """Valor (moeda da conta) de 1,0 de variação de preço para 1 lote."""
        return self.tick_value / self.tick

    def lots_for_risk(self, risk_money: float, stop_distance: float) -> float:
        """Maior lote (múltiplo de volume_step) cuja perda no stop não passa de risk_money; 0 se < mínimo."""
        if not all(math.isfinite(v) for v in (risk_money, stop_distance)):
            raise ValueError("Risco e distância do stop devem ser finitos; confira os dados da operação.")
        if stop_distance <= 0 or risk_money <= 0:
            return 0.0
        values = (self.tick, self.tick_value, self.volume_min, self.volume_step, self.volume_max)
        if not all(math.isfinite(v) and v > 0 for v in values) or self.volume_max < self.volume_min:
            raise ValueError("Especificação de tick/lote inválida; atualize o contrato do símbolo com mt5x specs.")
        tick, tick_value, minimum, step, maximum = (Decimal(str(v)) for v in values)
        raw = Decimal(str(risk_money)) * tick / (Decimal(str(stop_distance)) * tick_value)
        # Aplicar o teto ANTES de quantizar preserva o passo mesmo se o teto não for múltiplo.
        # Decimal evita promover um volume acima do orçamento com um epsilon absoluto.
        vol = (min(raw, maximum) / step).to_integral_value(rounding=ROUND_FLOOR) * step
        return float(vol) if vol >= minimum else 0.0


@dataclass
class BacktestConfig:
    initial_equity: float = 100_000.0
    risk_per_trade: float = 0.01       # 1% do capital por operação
    slippage_points: float = 0.0       # por execução a mercado
    commission_per_lot: float = 0.0    # por lote e por lado, na moeda da conta
    compound: bool = True              # risco sobre o capital atual (False = capital inicial)
    swap: bool = True                  # cobra o swap das posições que viram o dia
    real_spread: bool = True           # usa o spread medido nos ticks, quando a especificação tem (senão, o da barra)


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
    stop_price: Optional[float] = None     # stop inicial (o Laboratório re-simula a partir dele; T16.6)
    target_price: Optional[float] = None   # alvo inicial, quando houve

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
    account_balance: pd.Series = field(default_factory=lambda: pd.Series(dtype=float))
    floating_equity: pd.Series = field(default_factory=lambda: pd.Series(dtype=float))
    performance_equity: pd.Series = field(default_factory=lambda: pd.Series(dtype=float))
    account_cashflows: pd.Series = field(default_factory=lambda: pd.Series(dtype=float))
    valuation: dict = field(default_factory=dict)
    trajectory: dict = field(default_factory=dict)

    def trades_frame(self) -> pd.DataFrame:
        rows = [asdict(t) for t in self.trades]
        cols = list(Trade.__dataclass_fields__)
        return pd.DataFrame(rows, columns=cols)


def compute_metrics(trades: List[Trade], initial_equity: float) -> Dict[str, float]:
    from mt5_extracao import profit_factor
    pf = profit_factor.fields(t.pnl for t in trades)
    if pf["profit_factor_status"] == "invalid_pnl":
        raise ValueError("PnL ausente ou inválido; confira as operações antes de calcular métricas.")
    pnl = np.array([t.pnl for t in trades], dtype=float)
    r = np.array([t.r_multiple for t in trades], dtype=float)
    wins, losses = pnl[pnl > 0], pnl[pnl < 0]
    equity = initial_equity + np.concatenate([[0.0], np.cumsum(pnl)])
    peak = np.maximum.accumulate(equity)
    dd = np.divide(peak - equity, peak, out=np.zeros_like(peak), where=peak > 0)
    reasons: Dict[str, int] = {}
    for t in trades:
        reasons[t.exit_reason] = reasons.get(t.exit_reason, 0) + 1
    return {
        "trades": int(len(trades)),
        "win_rate": float(len(wins) / len(trades)) if trades else 0.0,
        **pf,
        "payoff": float(wins.mean() / -losses.mean()) if len(wins) and len(losses) else 0.0,
        "expectancy_r": float(r.mean()) if len(r) else 0.0,
        "total_r": float(r.sum()),
        "net_profit": float(pnl.sum()),
        "return_pct": float(pnl.sum() / initial_equity) if initial_equity else 0.0,
        "max_drawdown_pct": float(dd.max()),
        "avg_bars": float(np.mean([t.bars for t in trades])) if trades else 0.0,
        "exit_reasons": reasons,
    }


def execution_spreads(data: pd.DataFrame, spec: "SymbolSpec", cfg: "BacktestConfig"):
    """
    Spread (em preço) de cada barra dentro dela, na abertura e no fechamento, como o backtest usa nas execuções.
    Separado de run_backtest para a re-simulação do Laboratório (T16.6) usar exatamente as mesmas regras.
    """
    n = len(data)
    times = pd.to_datetime(data["time"])
    if "spread" in data.columns:
        spread_pts = data["spread"].fillna(spec.spread_points).to_numpy(dtype=float)
    else:
        spread_pts = np.full(n, float(spec.spread_points))
    spr = spread_pts * spec.point
    # Spread de cada execução: na abertura (spr_open), no fechamento (spr_close) e dentro da barra (spr).
    spr_open = spr_close = spr
    if cfg.real_spread and spec.spread_by_hour and n:
        profile = np.array([np.nan if v is None else float(v) for v in spec.spread_by_hour]) * spec.point
        step = times.diff().median() if n > 1 else pd.Timedelta(hours=1)
        hours = max(1, int(round(step / pd.Timedelta(hours=1)))) if step >= pd.Timedelta(hours=1) else 1
        open_hour = times.dt.hour.to_numpy()
        close_hour = (times + step - pd.Timedelta(minutes=1)).dt.hour.to_numpy()
        at_open = profile
        if spec.spread_at_open:
            at_open = np.array([np.nan if v is None else float(v) for v in spec.spread_at_open]) * spec.point
        on_the_hour = (times.dt.minute == 0).to_numpy()            # a barra abre na hora cheia?
        spr_open = np.fmax(spr, np.where(on_the_hour, at_open[open_hour], profile[open_hour]))
        spr_close = np.fmax(spr, profile[close_hour])
        # dentro da barra o spread foi, no mínimo, o menor das horas que ela cobre
        spanned = np.stack([profile[(open_hour + k) % 24] for k in range(min(hours, 24))])
        floor = np.where(np.isnan(spanned), np.inf, spanned).min(axis=0)
        spr = np.fmax(spr, np.where(np.isfinite(floor), floor, np.nan))
    return spr, spr_open, spr_close


def _fixed_point_distance(points, spec, *, label):
    """Conversão de unidade de preço; o lote/PnL continuam no serviço existente."""
    if points is None:
        return None
    if (not spec.known or not spec.symbol or not math.isfinite(spec.point) or spec.point <= 0
            or not math.isfinite(spec.tick_size) or spec.tick_size < 0):
        raise ValueError(f"{label} fixo requer especificação conhecida de point/tick; atualize o ativo com mt5x specs.")
    distance = float(points) * spec.point
    ratio = distance / spec.tick
    if (not math.isfinite(distance) or distance <= 0 or not math.isfinite(ratio)
            or not math.isclose(ratio, round(ratio), rel_tol=0, abs_tol=1e-6)):
        raise ValueError(f"{label} em pontos não é múltiplo do tick do ativo; escolha uma distância válida.")
    return distance


def run_backtest(df: pd.DataFrame, strategy: Strategy, spec: Optional[SymbolSpec] = None,
                 config: Optional[BacktestConfig] = None, start_index: int = 0,
                 signals: Optional[pd.DataFrame] = None, cashflows=None, check=None,
                 observation_timeframe=None, exit_rules_override: Optional[ExitRules] = None,
                 entry_policy=None, entry_time_basis=None, cooldown=None) -> BacktestResult:
    """
    start_index: só abre operações a partir desta barra (as anteriores servem de aquecimento
    dos indicadores; usado no walk-forward).
    """
    if check is not None:
        check()
    spec = spec or SymbolSpec(known=False)
    from mt5_extracao.analytics.instruments import require_supported_spec
    require_supported_spec(spec)
    cfg = config or BacktestConfig()
    data = df.reset_index(drop=True)
    if exit_rules_override is not None and not isinstance(exit_rules_override, ExitRules):
        raise ValueError("Política de saída inválida; use as regras tipadas do motor existente.")
    if signals is None and exit_rules_override is not None and exit_rules_override.stop_points is not None:
        signals = strategy.gerar_sinais(data, exit_rules_override=exit_rules_override)
    sig = (signals if signals is not None else strategy.gerar_sinais(data)).reset_index(drop=True)
    if check is not None:
        check()
    rules = exit_rules_override if exit_rules_override is not None else strategy.exit_rules()
    fixed_target = _fixed_point_distance(getattr(rules, "target_points", None), spec, label="Alvo")
    fixed_stop = _fixed_point_distance(getattr(rules, "stop_points", None), spec, label="Stop")
    n = len(data)

    o, h, l, c = (data[k].to_numpy(dtype=float) for k in ("open", "high", "low", "close"))
    times = pd.to_datetime(data["time"])
    session_trace, session_counts = [], {}
    cooldown_trace, last_close_msc = [], 0
    if cooldown is not None:
        from .cooldown import CooldownPolicy
        if not isinstance(cooldown,CooldownPolicy) or observation_timeframe is None:
            raise ValueError("Cooldown exige contrato tipado e timeframe explícito para o fechamento conhecido.")
        from mt5_extracao.trade_analytics.trajectories.clocks import bar_end_in_source
    if entry_policy is not None or cooldown is not None:
        from .session_entry import SessionEntryPolicy, source_seconds
        if entry_policy is not None and not isinstance(entry_policy, SessionEntryPolicy):
            raise ValueError("Filtro de sessão exige contrato tipado.")
        if entry_time_basis != "broker":
            raise ValueError("Filtro de sessão exige entry_time_basis='broker' declarado; não inferir fuso/UTC da série.")
        source_times = [source_seconds(t) for t in times]
        if any(b <= a for a, b in zip(source_times, source_times[1:])):
            raise ValueError("Relógio de sessão repetido/regrediu; confira a fonte, sem presumir DST.")
    journal = None
    if observation_timeframe is not None:
        from mt5_extracao.trade_analytics.trajectories.observations import Journal
        journal = Journal(times, observation_timeframe, "python_model")
    temporal=rules.temporal
    temporal_trace=[]
    from mt5_extracao.strategies.temporal_er import observation_values,decide
    er_values=observation_values(temporal,data)
    if temporal is not None and temporal.enabled and observation_timeframe is None:
        raise ValueError("Política temporal requer timeframe explícito para a disponibilidade do fechamento.")
    if temporal is not None and temporal.enabled:
        from .temporal import source_age_ms
        from mt5_extracao.trade_analytics.trajectories.clocks import bar_end_in_source
    flows = cashflow_values(cashflows, times)
    cash_total = 0.0
    spr, spr_open, spr_close = execution_spreads(data, spec, cfg)
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
    balances = np.full(n, cfg.initial_equity, dtype=float)
    floating = np.full(n, cfg.initial_equity, dtype=float)

    def fill_exit(d, bid_price, spread, market=True):
        price = bid_price + (spread if d < 0 else 0.0)   # venda a descoberto recompra no ask
        return price - d * slip if market else price

    def close_part(volume, price, i, kind="partial", phase="intrabar"):
        if cfg.swap:
            pos["pnl"] += spec.swap_money(pos["dir"], volume, times.iloc[pos["i"]], times.iloc[i])
        pos["pnl"] += (price - pos["entry"]) * pos["dir"] * ppu * volume - cfg.commission_per_lot * volume
        pos["remaining"] = round(pos["remaining"] - volume, 8)
        if journal is not None:
            journal.record(len(trades), i, kind, phase, pos["remaining"], pos["pnl"],
                           direction=pos["dir"],
                           net_pnl=pos["pnl"] if kind == "exit" else None,
                           fill_price=price, fill_volume=volume)

    def close_all(i, price, reason, phase=None):
        nonlocal equity, pos, last_close_msc
        phase = phase or ("open" if reason == "signal" else "close" if reason in ("time", "eod", "end") else "intrabar")
        close_part(pos["remaining"], price, i, "exit", phase)
        if cooldown is not None:
            known = times.iloc[i] if phase == "open" else bar_end_in_source(times.iloc[i],observation_timeframe)
            last_close_msc = source_seconds(known)*1000
        equity += pos["pnl"]
        trades.append(Trade(direction=pos["dir"], entry_time=times.iloc[pos["i"]], exit_time=times.iloc[i],
                            entry_price=pos["entry"], exit_price=float(price), volume=pos["volume"],
                            pnl=float(pos["pnl"]), r_multiple=float(pos["pnl"] / pos["risk_money"]),
                            bars=i - pos["i"] + 1, exit_reason=EXIT_REASONS[reason],
                            equity_before=pos["equity_before"], stop_price=float(pos["initial_stop"]),
                            target_price=None if pos["target"] is None else float(pos["target"])))
        pos = None

    for i in range(n):
        if check is not None and i % 256 == 0:
            check()
        # Nova decisão do fechamento anterior só atua na próxima abertura.
        # SL/TP já atravessados no gap precedem a ordem temporal; high/low futuros não entram.
        if pos is not None and pos.get("pending_temporal"):
            d=pos["dir"];op=o[i]+(spr_open[i] if d<0 else 0)
            stop_gap=op<=pos["stop"] if d>0 else op>=pos["stop"]
            target_gap=pos["target"] is not None and (op>=pos["target"] if d>0 else op<=pos["target"])
            reason=pos["stop_kind"] if stop_gap else "target" if target_gap else pos["pending_temporal"]
            temporal_trace.append(dict(event="execution",cycle_seq=len(trades),available_at=times.iloc[i].isoformat(),requested=pos["pending_temporal"],reason=reason))
            close_all(i,op if target_gap and not stop_gap else fill_exit(d,o[i],spr_open[i]),reason,"open")
        # 1) saída por sinal pendente, na abertura
        if pos is not None and pending_exit:
            close_all(i, fill_exit(pos["dir"], o[i], spr_open[i]), "signal")
        pending_exit = False

        # 2) entrada pendente, na abertura
        session_decision = None
        if pos is None and pending_entry and entry_policy is not None:
            at = source_times[i]
            session = entry_policy.session_start(at)
            session_decision = entry_policy.decide(at, session_counts.get(session, 0))
            session_trace.append(dict(event="check", at=at, direction=pending_entry, **session_decision))
            if not session_decision["allowed"]:
                pending_entry = 0
        if pos is None and pending_entry and cooldown is not None:
            decision = cooldown.decide(source_times[i]*1000,last_close_msc)
            cooldown_trace.append(dict(direction=pending_entry,**decision))
            if not decision["allowed"]:pending_entry=0
        if pos is None and pending_entry:
            d, s = pending_entry, i - 1
            entry = o[i] + spr_open[i] + slip if d > 0 else o[i] - slip
            stop = (entry - d * fixed_stop) if fixed_stop is not None else (stop_long[s] if d > 0 else stop_short[s])
            risk_px = (entry - stop) * d
            if not np.isfinite(risk_px) or risk_px <= 0:
                skipped_gap += 1     # abriu além do stop
            else:
                base = equity + cash_total if cfg.compound else cfg.initial_equity
                volume = spec.lots_for_risk(base * cfg.risk_per_trade, risk_px)
                if volume <= 0:
                    skipped_min_lot += 1
                else:
                    target = (entry + d * fixed_target) if fixed_target is not None else (tgt_long[s] if d > 0 else tgt_short[s])
                    if not np.isfinite(target) and rules.target_r:
                        target = entry + d * rules.target_r * risk_px
                    pos = {"dir": d, "entry": entry, "stop": stop, "stop_kind": "stop", "initial_stop": stop,
                           "target": target if np.isfinite(target) else None, "volume": volume,
                           "remaining": volume, "risk_px": risk_px, "risk_money": risk_px * ppu * volume,
                           "pnl": -cfg.commission_per_lot * volume, "i": i, "partial_done": False,
                           "equity_before": equity + cash_total}
                    if entry_policy is not None:
                        session = session_decision["session_start"]
                        session_counts[session] = session_counts.get(session, 0) + 1
                        session_trace.append(dict(event="fill", at=source_times[i], direction=d,
                                                  session_start=session, cycles=session_counts[session]))
                    if journal is not None:
                        journal.record(len(trades), i, "entry", "open", volume, pos["pnl"],
                                       direction=d,
                                       fill_price=entry, fill_volume=volume)
        pending_entry = 0

        # 3) stop, parcial e alvo dentro da barra
        if pos is not None:
            d = pos["dir"]
            # preços do lado que fecha a posição: compra fecha no bid, venda no ask
            lo, hi, op = (l[i], h[i], o[i]) if d > 0 else (l[i] + spr[i], h[i] + spr[i], o[i] + spr[i])
            stop_hit = lo <= pos["stop"] if d > 0 else hi >= pos["stop"]
            if stop_hit:
                level = min(op, pos["stop"]) if d > 0 else max(op, pos["stop"])
                close_all(i, level - d * slip, pos["stop_kind"],
                          "open" if (op <= pos["stop"] if d > 0 else op >= pos["stop"]) else "intrabar")
            else:
                if rules.partial_r and not pos["partial_done"]:
                    level = pos["entry"] + d * rules.partial_r * pos["risk_px"]
                    if (hi >= level) if d > 0 else (lo <= level):
                        step = spec.volume_step or spec.volume_min
                        part = math.floor(pos["volume"] * rules.partial_fraction / step + 1e-9) * step
                        if part >= spec.volume_min and pos["remaining"] - part >= spec.volume_min - 1e-12:
                            close_part(part, max(op, level) if d > 0 else min(op, level), i,
                                       phase="open" if (op >= level if d > 0 else op <= level) else "intrabar")
                            # Só o preenchimento válido confirma a parcial e autoriza proteger o saldo.
                            pos["partial_done"] = True
                            if rules.breakeven_after_partial:
                                pos["stop"] = max(pos["stop"], pos["entry"]) if d > 0 else min(pos["stop"], pos["entry"])
                                pos["stop_kind"] = "breakeven"
                tgt = pos["target"]
                if tgt is not None and ((hi >= tgt) if d > 0 else (lo <= tgt)):
                    close_all(i, max(op, tgt) if d > 0 else min(op, tgt), "target",
                              "open" if (op >= tgt if d > 0 else op <= tgt) else "intrabar")

        # 4) no fechamento: trailing, tempo, fim do dia, sinal
        if pos is not None:
            d = pos["dir"]
            if rules.trailing_atr and np.isfinite(atr[i]):
                new = c[i] - d * rules.trailing_atr * atr[i]
                if (new > pos["stop"]) if d > 0 else (new < pos["stop"]):
                    pos["stop"], pos["stop_kind"] = new, "trailing"
            if rules.max_bars and i - pos["i"] + 1 >= rules.max_bars:
                close_all(i, fill_exit(d, c[i], spr_close[i]), "time")
            elif rules.eod_exit and last_of_day[i]:
                close_all(i, fill_exit(d, c[i], spr_close[i]), "eod")
            elif (exit_long[i] if d > 0 else exit_short[i]):
                pending_exit = True
            if pos is not None and not pending_exit and temporal is not None and temporal.enabled:
                available=bar_end_in_source(times.iloc[i],observation_timeframe)
                age=source_age_ms(times.iloc[pos["i"]],available)
                mark=c[i]+(spr_close[i] if d<0 else 0)
                net=pos["pnl"]+(mark-pos["entry"])*d*ppu*pos["remaining"]
                if cfg.swap:net+=spec.swap_money(d,pos["remaining"],times.iloc[pos["i"]],times.iloc[i])
                decision,er_context=decide(temporal,age,float(net),er_values,i,direction=d,opened_index=pos["i"])
                if decision not in ("hold","unavailable"):
                    pos["pending_temporal"]=decision
                    temporal_trace.append(dict(event="decision",cycle_seq=len(trades),available_at=available.isoformat(),opened_at=times.iloc[pos["i"]].isoformat(),age_ms=age,net_observed=float(net),reason=decision,**er_context))

        # 5) novos sinais de entrada (executados na próxima abertura)
        if i + 1 >= start_index and i < n - 1 and (pos is None or pending_exit):
            if not (rules.eod_exit and last_of_day[i]):
                want = 1 if entry_long[i] else (-1 if entry_short[i] else 0)
                if want and (pos is None or want != pos["dir"]):
                    pending_entry = want

        # Marcação no fechamento: saldo inclui parciais/custos realizados; patrimônio inclui a posição aberta.
        balances[i] = equity
        floating[i] = equity
        if pos is not None:
            balances[i] += pos["pnl"]
            mark = c[i] + (spr_close[i] if pos["dir"] < 0 else 0)
            marked = (mark - pos["entry"]) * pos["dir"] * ppu * pos["remaining"]
            floating[i] = balances[i] + marked
            marked_pnl = pos["pnl"] + marked
            if cfg.swap:
                accrued_swap = spec.swap_money(pos["dir"], pos["remaining"], times.iloc[pos["i"]], times.iloc[i])
                floating[i] += accrued_swap
                marked_pnl += accrued_swap
            if journal is not None:
                journal.record(len(trades), i, "mark", "close", pos["remaining"], pos["pnl"],
                               net_pnl=marked_pnl, direction=pos["dir"])
        cash_total += flows[i]
        balances[i] += cash_total
        floating[i] += cash_total

    if pos is not None:
        close_all(n - 1, fill_exit(pos["dir"], c[n - 1], spr_close[n - 1]), "end")
        balances[-1] = floating[-1] = equity + cash_total

    eq_times = [times.iloc[0]] + [t.exit_time for t in trades] if n else []
    eq_vals = [cfg.initial_equity] + list(cfg.initial_equity + np.cumsum([t.pnl for t in trades])) if n else []
    performance = unitized_equity(floating, flows, cfg.initial_equity)
    trajectory=journal.payload(valuation_metadata(spec,cfg)) if journal is not None else {}
    if entry_policy is not None:
        trajectory.update(session_events=session_trace, session_entry_policy=entry_policy.to_dict(),
                          session_time_basis="broker_source_clock; fixed anchored day; no inferred UTC/DST")
    if cooldown is not None:
        trajectory.update(cooldown_events=cooldown_trace,cooldown_policy={"seconds":cooldown.seconds},cooldown_time_basis="broker_source_clock; final known-at; intrabar bar-end")
    if temporal is not None and temporal.enabled:
        trajectory.update(temporal_decisions=temporal_trace,temporal_policy=temporal.to_dict(),temporal_time_basis="source_clock_elapsed; not qualified session time")
    return BacktestResult(strategy=strategy.describe(), trades=trades,
                          equity=pd.Series(eq_vals, index=pd.DatetimeIndex(eq_times), dtype=float),
                          metrics={**compute_metrics(trades, cfg.initial_equity),
                                   **floating_metrics(performance, cfg.initial_equity),
                                   "floating_drawdown_basis": "unitized_initial_capital" if np.any(flows) else "account_equity"},
                          skipped_min_lot=skipped_min_lot, skipped_gap=skipped_gap, config=cfg,
                          account_balance=pd.Series(balances, index=pd.DatetimeIndex(times), dtype=float),
                          floating_equity=pd.Series(floating, index=pd.DatetimeIndex(times), dtype=float),
                          performance_equity=pd.Series(performance, index=pd.DatetimeIndex(times), dtype=float),
                          account_cashflows=pd.Series(flows, index=pd.DatetimeIndex(times), dtype=float),
                          valuation=valuation_metadata(spec, cfg),
                          trajectory=trajectory)
