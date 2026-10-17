"""
Motor de re-simulação (T16.6). Pega as entradas de um run (horário, direção, preço, stop inicial, alvo, volume) e
refaz a gestão da posição barra a barra, com as MESMAS regras de execução do backtester (`strategies/backtester.py`):

- ordem dentro da barra: saída por sinal na abertura → stop → parcial → alvo → no fechamento: trailing, tempo, fim do
  dia. Com stop e alvo na mesma barra, o stop vem primeiro (conservador);
- gap: stop preenchido na abertura quando ela já passou do nível;
- compra fecha no bid, venda no ask; spread por barra (`execution_spreads`), escorregamento e comissão do
  `BacktestConfig`; swap pelo `SymbolSpec`.

As saídas que não dependem do preço (sinal da estratégia, tempo, fim do dia, fim dos dados) são as do run original:
com `keep_original_exits`, a posição sai na mesma barra em que saiu de verdade, se ainda estiver aberta. Isso é o que
torna o modo base idêntico ao backtest (paridade testada) e os outros modos comparáveis a ele.

    policy = ExitPolicy.from_rules(strategy.exit_rules())     # modo base do robô Python
    result = simulate(trades, bars, spec, cfg, policy, atr)    # trades = ledger.load(...) de um run
"""
import math
from dataclasses import dataclass, field, replace
from typing import List, Optional

import numpy as np
import pandas as pd

from mt5_extracao.strategies.backtester import EXIT_REASONS, BacktestConfig, SymbolSpec, Trade, execution_spreads

_PRICE_EXITS = {EXIT_REASONS[k] for k in ("stop", "breakeven", "trailing", "target")}
_OPEN_EXITS = {EXIT_REASONS["signal"]}                         # saem na abertura da barra
_CLOSE_EXITS = {EXIT_REASONS[k] for k in ("time", "eod", "end")}   # saem no fechamento da barra


@dataclass
class ExitPolicy:
    """Como a posição é gerida depois da entrada. None = não usa a regra."""
    target_r: Optional[float] = None          # alvo em R; None + use_original_target = o alvo do run
    use_original_target: bool = True
    partial_r: Optional[float] = None
    partial_fraction: float = 0.5
    breakeven_after_partial: bool = True
    breakeven_r: Optional[float] = None       # leva o stop para a entrada ao atingir N × R (sem parcial)
    trailing_atr: Optional[float] = None      # stop móvel a N × ATR do fechamento
    trailing_bars: Optional[int] = None       # stop móvel na mínima (compra) / máxima (venda) das últimas N barras
    max_bars: Optional[int] = None
    eod_exit: bool = False
    keep_original_exits: bool = True          # sinal, tempo, fim do dia e fim dos dados do run original
    # Gradiente (T16.8): entradas adicionais na mesma operação. "contra" = preço médio; "favor" = piramidação
    grid_levels: int = 0
    grid_direction: str = "contra"
    grid_spacing_points: Optional[float] = None    # espaçamento fixo, em pontos do ativo
    grid_spacing_atr: Optional[float] = None       # espaçamento dinâmico: k × ATR da barra anterior à entrada
    grid_lot_factor: float = 1.0                   # lote de cada nível = lote inicial × fator
    grid_max_volume: Optional[float] = None        # teto de exposição (lotes somados)
    grid_trail_stop: bool = False                  # piramidação: o stop do conjunto sobe para a entrada anterior

    @classmethod
    def from_rules(cls, rules) -> "ExitPolicy":
        """O modo base de uma estratégia Python: as regras de saída dela."""
        return cls(target_r=None, use_original_target=True, partial_r=rules.partial_r,
                   partial_fraction=rules.partial_fraction, breakeven_after_partial=rules.breakeven_after_partial,
                   trailing_atr=rules.trailing_atr, max_bars=rules.max_bars, eod_exit=rules.eod_exit)

    def with_(self, **changes) -> "ExitPolicy":
        return replace(self, **changes)


@dataclass
class SimResult:
    trades: List[Trade]
    skipped: int                  # operações sem stop, sem entrada ou fora das barras
    approximate: int              # entradas sem barra no horário exato (resolução diferente da do run)
    adds: int = 0                 # entradas adicionais do gradiente
    capped: int = 0               # níveis não abertos por causa do teto de exposição
    max_volume: float = 0.0       # maior exposição (lotes somados) numa operação
    source_seqs: List[Optional[int]] = field(default_factory=list)  # identidade original; horário pode ser aproximado


def _bar_index(times: pd.Series, when) -> Optional[int]:
    k = int(times.searchsorted(pd.Timestamp(when), side="left"))
    return k if k < len(times) else None


def simulate(trades: pd.DataFrame, bars: pd.DataFrame, spec: SymbolSpec, cfg: BacktestConfig,
             policy: ExitPolicy, atr: Optional[pd.Series] = None) -> SimResult:
    """Re-simula cada operação (independentes: o motor não abre posição nova, só gere as que o run abriu)."""
    data = bars.reset_index(drop=True)
    times = pd.to_datetime(data["time"]).reset_index(drop=True)
    o, h, l, c = (data[k].to_numpy(dtype=float) for k in ("open", "high", "low", "close"))
    spr, spr_open, spr_close = execution_spreads(data, spec, cfg)
    slip = cfg.slippage_points * spec.point
    ppu = spec.money_per_price_unit
    days = times.dt.normalize().to_numpy()
    last_of_day = np.append(days[1:] != days[:-1], True) if len(days) else np.array([], dtype=bool)
    atr_v = np.asarray(atr, dtype=float) if atr is not None else np.full(len(data), np.nan)
    out: List[Trade] = []
    source_seqs = []
    skipped = approximate = adds = capped = 0
    stats_volume = [0.0]
    n = len(data)

    for t in trades.sort_values("entry_time").itertuples(index=False):
        d = int(t.direction) if t.direction in (1, -1) else 0
        if not d or pd.isna(t.entry_time) or t.stop_price is None or pd.isna(t.stop_price) or not n:
            skipped += 1
            continue
        i0 = _bar_index(times, t.entry_time)
        if i0 is None:
            skipped += 1
            continue
        if times.iloc[i0] != pd.Timestamp(t.entry_time):
            approximate += 1
        entry, stop, volume = float(t.entry_price), float(t.stop_price), float(t.volume)
        risk_px = (entry - stop) * d
        if not np.isfinite(risk_px) or risk_px <= 0 or volume <= 0:
            skipped += 1
            continue
        if policy.target_r is not None:
            target = entry + d * policy.target_r * risk_px
        elif policy.use_original_target and t.target_price is not None and not pd.isna(t.target_price):
            target = float(t.target_price)
        else:
            target = None
        exit_orig = _bar_index(times, t.exit_time) if not pd.isna(t.exit_time) else None
        reason_orig = t.exit_reason
        pos = {"stop": stop, "kind": "stop", "remaining": volume, "pnl": -cfg.commission_per_lot * volume,
               "partial": False, "be": False, "lots": [[entry, volume]], "next_level": 1}
        grid = policy.grid_levels > 0
        spacing = None
        if grid:
            if policy.grid_spacing_atr:
                ref = atr_v[i0 - 1] if i0 > 0 else np.nan        # ATR da barra fechada antes da entrada
                spacing = policy.grid_spacing_atr * ref if np.isfinite(ref) else None
            elif policy.grid_spacing_points:
                spacing = policy.grid_spacing_points * spec.point
            grid = bool(spacing and spacing > 0)
        sign = -1 if policy.grid_direction == "contra" else 1

        def fill_exit(bid_price, spread, market=True):
            price = bid_price + (spread if d < 0 else 0.0)
            return price - d * slip if market else price

        def close_part(vol, price):
            # fecha `vol` lotes, do mais antigo para o mais novo (com gradiente há vários preços de entrada)
            left = vol
            for lot in pos["lots"]:
                take = min(lot[1], left)
                if take <= 0:
                    continue
                pos["pnl"] += (price - lot[0]) * d * ppu * take - cfg.commission_per_lot * take
                lot[1] = round(lot[1] - take, 8)
                left = round(left - take, 8)
            pos["lots"] = [lot for lot in pos["lots"] if lot[1] > 1e-12]
            pos["remaining"] = round(pos["remaining"] - vol, 8)

        def add_levels(i, lo, hi, op):
            """Abre os níveis do gradiente tocados nesta barra (ordem limite/stop, preenchida no nível ou na abertura)."""
            nonlocal adds, capped
            while pos["next_level"] <= policy.grid_levels:
                k = pos["next_level"]
                level = entry + d * sign * k * spacing
                touched = ((lo <= level) if d > 0 else (hi >= level)) if sign < 0 else \
                    ((hi >= level) if d > 0 else (lo <= level))
                if not touched:
                    break
                lot = volume * policy.grid_lot_factor
                if policy.grid_max_volume and pos["remaining"] + lot > policy.grid_max_volume + 1e-12:
                    capped += policy.grid_levels - k + 1
                    pos["next_level"] = policy.grid_levels + 1
                    break
                # limite (contra) preenche no nível ou melhor; stop (a favor) no nível ou pior, se abriu além
                gapped = (op < level) if (d > 0) == (sign < 0) else (op > level)
                price = op if gapped else level
                # os níveis são medidos no lado que fecha a posição (bid na compra, ask na venda); entrar é o outro lado
                price = price + spr[i] if d > 0 else price - spr[i]
                previous = pos["lots"][-1][0]
                pos["lots"].append([price, lot])
                pos["remaining"] = round(pos["remaining"] + lot, 8)
                pos["pnl"] -= cfg.commission_per_lot * lot
                pos["next_level"] = k + 1
                adds += 1
                if sign > 0 and policy.grid_trail_stop:
                    if (previous > pos["stop"]) if d > 0 else (previous < pos["stop"]):
                        pos["stop"], pos["kind"] = previous, "trailing"
            stats_volume[0] = max(stats_volume[0], pos["remaining"])
            stats_volume_trade[0] = max(stats_volume_trade[0], pos["remaining"])

        def close_all(i, price, reason):
            pos["peak_volume"] = max(volume, stats_volume_trade[0])
            if cfg.swap:
                pos["pnl"] += spec.swap_money(d, pos["remaining"], times.iloc[i0], times.iloc[i])
            close_part(pos["remaining"], price)
            out.append(Trade(direction=d, entry_time=times.iloc[i0], exit_time=times.iloc[i], entry_price=entry,
                             exit_price=float(price), volume=float(pos["peak_volume"]), pnl=float(pos["pnl"]),
                             r_multiple=float(pos["pnl"] / (risk_px * ppu * volume)), bars=i - i0 + 1,
                             exit_reason=EXIT_REASONS[reason], equity_before=float("nan"), stop_price=stop,
                             target_price=target))
            source_seqs.append(getattr(t, "seq", None))

        closed = False
        stats_volume[0] = max(stats_volume[0], volume)
        stats_volume_trade = [volume]
        for i in range(i0, n):
            if policy.keep_original_exits and i == exit_orig and reason_orig in _OPEN_EXITS and i > i0:
                close_all(i, fill_exit(o[i], spr_open[i]), "signal")
                closed = True
                break
            lo, hi, op = (l[i], h[i], o[i]) if d > 0 else (l[i] + spr[i], h[i] + spr[i], o[i] + spr[i])
            if grid and sign < 0:
                add_levels(i, lo, hi, op)       # contra: assume o pior caso (o nível enche antes do stop)
            if (lo <= pos["stop"]) if d > 0 else (hi >= pos["stop"]):
                level = min(op, pos["stop"]) if d > 0 else max(op, pos["stop"])
                close_all(i, level - d * slip, pos["kind"])
                closed = True
                break
            if grid and sign > 0:
                add_levels(i, lo, hi, op)       # a favor: só depois de conferir que o stop não foi atingido
            if policy.partial_r and not pos["partial"] and not grid:
                level = entry + d * policy.partial_r * risk_px
                if (hi >= level) if d > 0 else (lo <= level):
                    pos["partial"] = True
                    step = spec.volume_step or spec.volume_min
                    part = math.floor(volume * policy.partial_fraction / step + 1e-9) * step
                    if part >= spec.volume_min and pos["remaining"] - part >= spec.volume_min - 1e-12:
                        close_part(part, max(op, level) if d > 0 else min(op, level))
                    if policy.breakeven_after_partial:
                        pos["stop"] = max(pos["stop"], entry) if d > 0 else min(pos["stop"], entry)
                        pos["kind"] = "breakeven"
            if policy.breakeven_r and not pos["be"]:
                level = entry + d * policy.breakeven_r * risk_px
                if (hi >= level) if d > 0 else (lo <= level):
                    pos["be"] = True
                    if (entry > pos["stop"]) if d > 0 else (entry < pos["stop"]):
                        pos["stop"], pos["kind"] = entry, "breakeven"
            if target is not None and ((hi >= target) if d > 0 else (lo <= target)):
                close_all(i, max(op, target) if d > 0 else min(op, target), "target")
                closed = True
                break
            # no fechamento
            new = None
            if policy.trailing_atr and np.isfinite(atr_v[i]):
                new = c[i] - d * policy.trailing_atr * atr_v[i]
            if policy.trailing_bars and i - policy.trailing_bars + 1 >= i0:
                window = slice(i - policy.trailing_bars + 1, i + 1)
                candidate = l[window].min() if d > 0 else h[window].max() + spr[i]
                new = candidate if new is None else (max(new, candidate) if d > 0 else min(new, candidate))
            if new is not None and ((new > pos["stop"]) if d > 0 else (new < pos["stop"])):
                pos["stop"], pos["kind"] = new, "trailing"
            if policy.max_bars and i - i0 + 1 >= policy.max_bars:
                close_all(i, fill_exit(c[i], spr_close[i]), "time")
                closed = True
                break
            if policy.eod_exit and last_of_day[i]:
                close_all(i, fill_exit(c[i], spr_close[i]), "eod")
                closed = True
                break
            if policy.keep_original_exits and i == exit_orig and reason_orig in _CLOSE_EXITS:
                kind = next(k for k, v in EXIT_REASONS.items() if v == reason_orig)
                close_all(i, fill_exit(c[i], spr_close[i]), kind)
                closed = True
                break
        if not closed:
            close_all(n - 1, fill_exit(c[n - 1], spr_close[n - 1]), "end")
    return SimResult(out, skipped, approximate, adds, capped, stats_volume[0], source_seqs=source_seqs)
