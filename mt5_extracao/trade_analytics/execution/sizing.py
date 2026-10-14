"""
Dimensionamento entre operações (T16.8): martingale e anti-martingale aplicados à sequência de um run, SÓ para o robô
que os suporta e com as travas de segurança dele (decisão de 2026-10-14; plano, seção 4.4).

O resultado de uma operação é proporcional ao lote (preço × valor do ponto × lote, comissão e swap por lote), então
reescalar o lote reescala o resultado. A sequência é percorrida na ordem de entrada, como o robô faria:

- martingale: depois de uma perda, o lote seguinte é multiplicado; depois de um ganho, volta ao inicial.
  Anti-martingale: o contrário. O número de multiplicações seguidas tem teto (`max_levels`).
- travas do robô, na ordem em que disparam: drawdown máximo (para tudo), perda diária (para o dia), risco por operação
  (corta o lote), `volume_max` do ativo (corta o lote).

    support = martingale_support(robot, preset)          # None = o robô não suporta: o modo nem aparece
    safety = robot_safety(robot, preset, config, spec)
    out = apply(trades, initial_equity, support["multiplier"], safety)
"""
from dataclasses import dataclass, field
from typing import Dict, Optional

import numpy as np
import pandas as pd


@dataclass
class Safety:
    risk_pct: Optional[float] = None          # % do capital por operação
    daily_loss_pct: Optional[float] = None    # % do capital no início do dia
    max_drawdown_pct: Optional[float] = None  # % do pico
    volume_max: Optional[float] = None
    max_levels: int = 4                        # multiplicações seguidas
    notes: list = field(default_factory=list)  # travas que vieram do teto global, não do robô


def martingale_support(robot, preset: Optional[str] = None) -> Optional[dict]:
    """{"param", "multiplier"} se o robô tem parâmetro de martingale e o preset não o bloqueia; senão None."""
    from mt5_extracao.robots import risk
    inputs = getattr(robot, "inputs", None) or []
    roles = risk.find_inputs(inputs)
    if "martingale" not in roles:
        return None
    values = (getattr(robot, "presets", {}) or {}).get(preset or "", {})

    def value(role):
        item = roles.get(role)
        return None if item is None else values.get(item.name, item.default)
    if "block_averaging" in roles and risk._truthy(value("block_averaging")):
        return None
    number = risk._number(value("martingale"))
    return {"param": roles["martingale"].name, "multiplier": number if number and number > 1 else 2.0}


def robot_safety(robot, preset: Optional[str], config, spec=None) -> Safety:
    """As travas do robô (do preset, senão do robô); o que faltar vem do teto global e é anotado."""
    rating = (getattr(robot, "preset_risks", {}) or {}).get(preset or "") or getattr(robot, "risk", None)
    facts = dict(getattr(rating, "facts", {}) or {})
    safety = Safety(risk_pct=facts.get("risk_pct"), daily_loss_pct=facts.get("daily_loss_pct"),
                    max_drawdown_pct=facts.get("max_drawdown_pct"),
                    volume_max=getattr(spec, "volume_max", None) if spec is not None else None)
    if safety.risk_pct is None:
        try:
            safety.risk_pct = float(config.get("ROBOTS", "max_real_risk_pct", fallback="1.0")) if config else 1.0
        except ValueError:
            safety.risk_pct = 1.0
        safety.notes.append(f"risco por operação: o robô não declara; usado o teto global de {safety.risk_pct:g}%")
    if safety.daily_loss_pct is None:
        safety.notes.append("perda diária: o robô não declara; sem essa trava na simulação")
    if safety.max_drawdown_pct is None:
        safety.notes.append("drawdown máximo: o robô não declara; sem essa trava na simulação")
    return safety


def apply(trades: pd.DataFrame, initial_equity: float, multiplier: float, safety: Safety,
          anti: bool = False) -> Dict[str, object]:
    """
    Reescala lote e resultado de cada operação. Devolve {"trades": DataFrame (volume, pnl reescalados; skipped e
    reason por linha), "triggers": {trava: vezes}, "max_streak", "max_volume", "max_loss", "levels_used"}.
    """
    rows = trades.sort_values("entry_time").reset_index(drop=True).copy()
    triggers = {"teto de multiplicações": 0, "risco por operação": 0, "volume máximo": 0, "perda diária": 0,
                "drawdown máximo": 0}
    equity = peak = float(initial_equity)
    level, streak, max_streak, max_volume, max_loss = 0, 0, 0, 0.0, 0.0
    day, day_start, day_pnl = None, equity, 0.0
    halted = False
    volumes, pnls, skipped, reasons = [], [], [], []
    for t in rows.itertuples(index=False):
        base_vol, base_pnl = float(t.volume), float(t.pnl)
        today = pd.Timestamp(t.entry_time).normalize()
        if today != day:
            day, day_start, day_pnl = today, equity, 0.0
        why = None
        if halted:
            why = "drawdown máximo"
        elif safety.daily_loss_pct and day_pnl <= -safety.daily_loss_pct / 100 * day_start:
            why = "perda diária"
        if why:
            triggers[why] += 1 if why == "perda diária" else 0
            volumes.append(0.0)
            pnls.append(0.0)
            skipped.append(True)
            reasons.append(why)
            continue
        factor = multiplier ** level
        volume = base_vol * factor
        reason = None
        r = getattr(t, "r_multiple", None)
        if safety.risk_pct and r is not None and not pd.isna(r) and r != 0 and base_pnl != 0:
            risk_per_lot = abs(base_pnl / r) / base_vol             # dinheiro em risco por lote nesta operação
            allowed = safety.risk_pct / 100 * equity / risk_per_lot
            if volume > allowed + 1e-12:
                volume, reason = max(allowed, 0.0), "risco por operação"
                triggers[reason] += 1
        if safety.volume_max and volume > safety.volume_max + 1e-12:
            volume, reason = safety.volume_max, "volume máximo"
            triggers[reason] += 1
        pnl = base_pnl * volume / base_vol if base_vol else 0.0
        equity += pnl
        day_pnl += pnl
        peak = max(peak, equity)
        max_volume, max_loss = max(max_volume, volume), min(max_loss, pnl)
        won = pnl > 0
        streak = 0 if won else streak + 1
        max_streak = max(max_streak, streak)
        grow = (not won) if not anti else won
        if grow:
            if level + 1 > safety.max_levels:
                triggers["teto de multiplicações"] += 1
                level = 0
            else:
                level += 1
        else:
            level = 0
        if safety.max_drawdown_pct and peak > 0 and (peak - equity) / peak >= safety.max_drawdown_pct / 100:
            halted = True
            triggers["drawdown máximo"] += 1
        volumes.append(volume)
        pnls.append(pnl)
        skipped.append(False)
        reasons.append(reason)
    rows["volume"], rows["pnl"], rows["skipped"], rows["reason"] = volumes, pnls, skipped, reasons
    return {"trades": rows, "triggers": triggers, "max_streak": max_streak, "max_volume": max_volume,
            "max_loss": max_loss, "final_equity": equity, "halted": halted}
