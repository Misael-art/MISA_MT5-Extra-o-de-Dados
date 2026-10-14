"""
Regras de filtro e simulador (T16.9), com a separação dos dados da seção 6.1 do plano.

Regras são dados (para exportar, gravar e mostrar):
    {"tipo": "excluir_dia", "dias": [4]}                          # 0 = segunda ... 4 = sexta
    {"tipo": "janela_horaria", "inicio": "10:00", "fim": "12:00"} # só opera dentro da janela (entrada)
    {"tipo": "excluir_janela", "inicio": "12:00", "fim": "13:30"} # não opera dentro da janela
    {"tipo": "excluir_noticia", "impacto_min": 3}                 # tira operações com notícia desse impacto ou maior
    {"tipo": "excluir_volatilidade", "faixas": ["alta"]}
    {"tipo": "trocar_modo", "modo": "trailing_atr", "quando": {"volatility_bucket": ["alta"]}}

    filtered = apply(lab, regras, modos)        # modos = {modo: DataFrame re-simulado com run_id, seq, pnl}
    plan = split(lab, config)                   # provas do walk-forward + reserva final
    result = evaluate(lab, regras, config, modos)   # antes × depois em cada prova e na reserva

Separação (seção 6.1): os últimos `holdout_fraction` do histórico (por horário de entrada) ficam na reserva; o resto é
cortado em `wf_folds` blocos e cada prova k procura nos blocos 1..k e confere no bloco k+1, com uma folga (embargo) do
tamanho da operação mais longa entre a procura e a conferência. Poucas operações: menos provas, até duas; se nem assim
houver `min_trades_cell` em cada conferência, só a reserva, e o resultado é marcado "evidência fraca".
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from mt5_extracao.trade_analytics import aggregate, stats

TYPES = ("excluir_dia", "janela_horaria", "excluir_janela", "excluir_noticia", "excluir_volatilidade",
         "trocar_modo")


def describe(rule: dict) -> str:
    """Texto curto para a tela e para o arquivo exportado."""
    kind = rule.get("tipo")
    if kind == "excluir_dia":
        return "Não operar " + ", ".join(aggregate.day_label(d) for d in rule.get("dias", []))
    if kind == "janela_horaria":
        return f"Operar só entre {rule.get('inicio')} e {rule.get('fim')}"
    if kind == "excluir_janela":
        return f"Não operar entre {rule.get('inicio')} e {rule.get('fim')}"
    if kind == "excluir_noticia":
        return f"Não operar com notícia de impacto {rule.get('impacto_min', 3)} ou maior na janela"
    if kind == "excluir_volatilidade":
        return "Não operar com volatilidade " + ", ".join(rule.get("faixas", []))
    if kind == "trocar_modo":
        cond = "; ".join(f"{k} em {', '.join(map(str, v))}" for k, v in rule.get("quando", {}).items())
        return f"Trocar para {rule.get('modo')} quando {cond}"
    return str(rule)


def _minutes(text: str) -> int:
    h, m = str(text).split(":")
    return int(h) * 60 + int(m)


def _mask(lab: pd.DataFrame, rule: dict) -> pd.Series:
    """True = a regra tira a operação (para trocar_modo: True = a operação troca de modo)."""
    kind = rule.get("tipo")
    if kind == "excluir_dia":
        return lab["day_of_week"].isin(rule.get("dias", []))
    if kind in ("janela_horaria", "excluir_janela"):
        t = pd.to_datetime(lab["entry_time"].fillna(lab["exit_time"]))
        minutes = t.dt.hour * 60 + t.dt.minute
        start, end = _minutes(rule["inicio"]), _minutes(rule["fim"])
        inside = (minutes >= start) & (minutes < end)
        return inside if kind == "excluir_janela" else ~inside
    if kind == "excluir_noticia":
        return pd.to_numeric(lab["event_impact"], errors="coerce").fillna(0) >= rule.get("impacto_min", 3)
    if kind == "excluir_volatilidade":
        return lab["volatility_bucket"].isin(rule.get("faixas", []))
    if kind == "trocar_modo":
        mask = pd.Series(True, index=lab.index)
        for col, values in rule.get("quando", {}).items():
            mask &= lab[col].isin(values)
        return mask
    raise ValueError(f"Regra desconhecida: {kind} (use {', '.join(TYPES)})")


def apply(lab: pd.DataFrame, rules: List[dict], modes: Optional[Dict[str, pd.DataFrame]] = None) -> pd.DataFrame:
    """O livro depois das regras: linhas excluídas saem; linhas com troca de modo recebem o resultado daquele modo."""
    out = lab.copy()
    for rule in rules or []:
        mask = _mask(out, rule)
        if rule.get("tipo") == "trocar_modo":
            other = (modes or {}).get(rule["modo"])
            if other is None:
                raise ValueError(f"Sem a re-simulação do modo {rule['modo']}: rode-a antes de usar a regra.")
            alt = other.set_index(["run_id", "seq"])["pnl"]
            keys = list(zip(out.loc[mask, "run_id"], out.loc[mask, "seq"]))
            out.loc[mask, "pnl"] = [alt.get(k, np.nan) for k in keys]
            out.loc[mask, "mode"] = rule["modo"]
            out = out.dropna(subset=["pnl"])
        else:
            out = out[~mask]
    return out


@dataclass
class Split:
    holdout: pd.Series                          # máscara da reserva final
    folds: List[tuple] = field(default_factory=list)   # [(máscara de procura, máscara de conferência)]
    weak: bool = False
    note: str = ""


def _setting(config, key, default, cast):
    try:
        return cast(config.get("TRADE_ANALYTICS", key, fallback=str(default))) if config is not None else default
    except (ValueError, AttributeError):
        return default


def split(lab: pd.DataFrame, config=None) -> Split:
    folds = max(2, _setting(config, "wf_folds", 4, int))
    hold_frac = min(0.5, max(0.05, _setting(config, "holdout_fraction", 0.2, float)))
    minimum = aggregate.min_trades(config)
    when = pd.to_datetime(lab["entry_time"].fillna(lab["exit_time"]))
    order = when.sort_values()
    cut = order.iloc[int(len(order) * (1 - hold_frac))] if len(order) else None
    holdout = when >= cut if cut is not None else pd.Series(False, index=lab.index)
    rest = order[order < cut] if cut is not None else order
    duration = (pd.to_datetime(lab["exit_time"]) - when).max() if len(lab) else pd.Timedelta(0)
    embargo = duration if pd.notna(duration) else pd.Timedelta(0)
    for k in range(folds, 1, -1):
        edges = [rest.iloc[int(len(rest) * j / k)] for j in range(k)] + [cut]
        plan = []
        for j in range(1, k):
            search = (when < edges[j] - embargo) & ~holdout
            check = (when >= edges[j]) & (when < edges[j + 1]) & ~holdout
            plan.append((search, check))
        if all(c.sum() >= minimum and s.sum() >= minimum for s, c in plan):
            note = "" if k == folds else f"poucas operações: {k - 1} prova(s) em vez de {folds - 1}"
            return Split(holdout, plan, weak=False, note=note)
    return Split(holdout, [], weak=True, note="poucas operações para o walk-forward: só a reserva final; evidência fraca")


def evaluate(lab: pd.DataFrame, rules: List[dict], config=None, modes=None) -> dict:
    """Antes × depois das regras, na procura (tudo menos a reserva), em cada prova e na reserva final."""
    initial = aggregate._initial_equity(config)
    plan = split(lab, config)
    after_all = apply(lab, rules, modes)

    def pair(mask):
        before = lab[mask]
        after = after_all[after_all.index.isin(before.index)]
        return {"before": stats.metrics(before.sort_values("exit_time")["pnl"], initial),
                "after": stats.metrics(after.sort_values("exit_time")["pnl"], initial)}
    folds = [pair(check) for _search, check in plan.folds]
    improved = sum(1 for f in folds if f["after"]["expectancy"] > f["before"]["expectancy"])
    return {"rules": [describe(r) for r in rules], "search": pair(~plan.holdout), "folds": folds,
            "folds_improved": improved, "holdout": pair(plan.holdout), "weak": plan.weak, "note": plan.note,
            "removed": int(len(lab) - len(after_all))}
