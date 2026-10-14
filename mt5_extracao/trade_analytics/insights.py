"""
Insights automáticos (T16.10): procura janelas onde o robô perde (ou ganha) de forma consistente e propõe a regra de
filtro correspondente — com as regras da seção 6 do plano para não vender sorte como descoberta.

1. Procura só nas partes de procura (tudo menos a reserva final; `rules.split`).
2. Cada combinação testada conta: a significância é corrigida por múltiplos testes (Benjamini-Hochberg, 1%).
3. Amostra mínima de `min_trades_cell` operações no grupo e no resto.
4. "validado" exige: significativo depois da correção, melhora em TODAS as provas do walk-forward e na reserva final,
   com o efeito presente também na reserva (teste próprio, 5%). Senão fica "exploratório" (pista, não conclusão).
5. Com poucas operações para o walk-forward, nada passa de "exploratório" ("evidência fraca").

    found = discover(lab, config)        # [Insight], o mais forte primeiro
    save(db, scope, found)               # _trade_insights (aditiva)
"""
import json
import math
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import List, Optional

import numpy as np
import pandas as pd
from sqlalchemy import inspect, text

from mt5_extracao.trade_analytics import aggregate, rules

EXPLORATORY, VALIDATED = "exploratório", "validado fora da amostra"
TABLE = "_trade_insights"
HOLDOUT_ALPHA = 0.05           # teste unilateral do efeito na reserva final
DIMENSIONS = {
    "day_of_week": ("Dia da semana", lambda v: aggregate.day_label(v), lambda v: {"tipo": "excluir_dia", "dias": [int(v)]}),
    "time_bucket": ("Faixa horária", str, None),
    "event_impact": ("Notícia", lambda v: {0: "sem evento", 1: "impacto baixo", 2: "impacto médio",
                                           3: "impacto alto"}.get(int(v), v),
                     lambda v: {"tipo": "excluir_noticia", "impacto_min": int(v)} if int(v) >= 1 else None),
    "volatility_bucket": ("Volatilidade", lambda v: f"volatilidade {v}",
                          lambda v: {"tipo": "excluir_volatilidade", "faixas": [v]}),
}


def _time_rule(bucket: str, minutes: int) -> dict:
    start = int(bucket[:2]) * 60 + int(bucket[3:5])
    end = start + minutes
    return {"tipo": "excluir_janela", "inicio": bucket, "fim": f"{end // 60:02d}:{end % 60:02d}"}


@dataclass
class Insight:
    dimension: str
    value: object
    label: str
    text: str
    rule: Optional[dict]
    state: str
    n: int
    expectancy: float
    rest_expectancy: float
    p_value: float
    p_adjusted: float
    tested: int
    folds_improved: int
    folds: int
    holdout_before: Optional[float] = None
    holdout_after: Optional[float] = None
    weak: bool = False
    notes: List[str] = field(default_factory=list)


def _p_value(group: np.ndarray, rest: np.ndarray) -> float:
    """Welch, bilateral: o grupo difere do resto?"""
    from scipy import stats as st
    if len(group) < 2 or len(rest) < 2:
        return 1.0
    p = st.ttest_ind(group, rest, equal_var=False).pvalue
    return 1.0 if p is None or math.isnan(p) else float(p)


def _bh(pvalues: List[float]) -> List[float]:
    """Benjamini-Hochberg: p ajustados, na ordem original."""
    m = len(pvalues)
    if not m:
        return []
    order = np.argsort(pvalues)
    adjusted = np.empty(m)
    running = 1.0
    for rank in range(m, 0, -1):
        k = order[rank - 1]
        running = min(running, pvalues[k] * m / rank)
        adjusted[k] = running
    return adjusted.tolist()


def discover(lab: pd.DataFrame, config=None, alpha: float = 0.01) -> List[Insight]:
    minimum = aggregate.min_trades(config)
    bucket_min = int(config.get("TRADE_ANALYTICS", "time_bucket_min", fallback="30")) if config is not None else 30
    plan = rules.split(lab, config)
    search = lab[~plan.holdout]
    candidates = []
    for dim, (title, fmt, make_rule) in DIMENSIONS.items():
        if dim not in search or search[dim].isna().all():
            continue
        for value, group in search.dropna(subset=[dim]).groupby(dim):
            rest = search.drop(group.index)
            if len(group) < minimum or len(rest) < minimum:
                continue
            p = _p_value(group["pnl"].to_numpy(float), rest["pnl"].to_numpy(float))
            rule = make_rule(value) if make_rule else None
            if dim == "time_bucket":
                rule = _time_rule(str(value), bucket_min)
            candidates.append((dim, value, title, fmt(value), group, rest, p, rule))
    adjusted = _bh([c[6] for c in candidates])
    out = []
    for (dim, value, title, label, group, rest, p, rule), p_adj in zip(candidates, adjusted):
        g_mean, r_mean = float(group["pnl"].mean()), float(rest["pnl"].mean())
        notes = []
        folds_improved = folds = 0
        hb = ha = None
        validated = False
        if rule is not None and g_mean < r_mean:          # proposta: tirar o grupo que puxa para baixo
            check = rules.evaluate(lab, [rule], config)
            hb, ha = check["holdout"]["before"]["expectancy"], check["holdout"]["after"]["expectancy"]
            # walk-forward de verdade: em cada prova, o grupo tem de ser pior OLHANDO SÓ a procura daquela prova, e
            # tirá-lo tem de melhorar a conferência dela (que a procura da prova não viu)
            folds = len(plan.folds)
            for fold_search, fold_check in plan.folds:
                inside = _member(lab, dim, value)
                seen, unseen = lab[fold_search], lab[fold_check]
                worse_before = len(seen[inside[fold_search]]) and \
                    seen[inside[fold_search]]["pnl"].mean() < seen[~inside[fold_search]]["pnl"].mean()
                improves = len(unseen) and unseen[~inside[fold_check]]["pnl"].mean() > unseen["pnl"].mean()
                folds_improved += int(bool(worse_before and improves))
            # a reserva final é o único juiz independente da escolha: o efeito tem de aparecer NELA, com teste próprio
            # (unilateral, 5%), não só "melhorar um pouco" (o que o ruído faz em metade das vezes)
            in_holdout = lab[plan.holdout & _member(lab, dim, value)]["pnl"].to_numpy(float)
            out_holdout = lab[plan.holdout & ~_member(lab, dim, value)]["pnl"].to_numpy(float)
            holdout_p = _p_value(in_holdout, out_holdout) / 2 if len(in_holdout) >= 2 else 1.0
            same_sign = len(in_holdout) > 0 and in_holdout.mean() < out_holdout.mean()
            validated = (p_adj < alpha and not plan.weak and folds > 0 and folds_improved == folds and ha > hb
                         and same_sign and holdout_p < HOLDOUT_ALPHA)
            if same_sign and holdout_p >= HOLDOUT_ALPHA:
                notes.append(f"na reserva final o efeito vai no mesmo sentido, mas sem força (p {holdout_p:.2f})")
            if plan.weak:
                notes.append("evidência fraca: poucas operações para o walk-forward")
        state = VALIDATED if validated else EXPLORATORY
        direction = "pior" if g_mean < r_mean else "melhor"
        text = (f"{label}: expectativa {_money(g_mean)} por operação em {len(group)} operações, {direction} que o resto "
                f"({_money(r_mean)}). Testadas {len(candidates)} combinações; p ajustado {p_adj:.3f}.")
        if hb is not None:
            text += (f" Na reserva final (não usada na procura), aplicar a regra levaria a expectativa de "
                     f"{_money(hb)} para {_money(ha)}; melhorou em {folds_improved} de {folds} prova(s).")
        out.append(Insight(dim, value if not isinstance(value, np.generic) else value.item(), label, text, rule, state,
                           len(group), g_mean, r_mean, p, p_adj, len(candidates), folds_improved, folds, hb, ha,
                           plan.weak, notes))
    out.sort(key=lambda i: (i.state != VALIDATED, i.p_adjusted))
    return out


def _member(lab, dim, value):
    return lab[dim] == value


def _money(value) -> str:
    text = f"{abs(value):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return ("-" if value < 0 else "") + text


def save(db, scope: str, found: List[Insight]) -> int:
    """Grava os insights de um escopo (robô ou ativo), substituindo os anteriores dele."""
    with db.engine.begin() as conn:
        conn.execute(text(f"CREATE TABLE IF NOT EXISTS {TABLE} (scope TEXT NOT NULL, created_at TEXT NOT NULL, "
                          f"state TEXT NOT NULL, text TEXT NOT NULL, rule_json TEXT, data_json TEXT)"))
        conn.execute(text(f"DELETE FROM {TABLE} WHERE scope = :s"), {"s": scope})
        stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for i in found:
            conn.execute(text(f"INSERT INTO {TABLE} VALUES (:s, :t, :st, :tx, :r, :d)"),
                         {"s": scope, "t": stamp, "st": i.state, "tx": i.text,
                          "r": json.dumps(i.rule, ensure_ascii=False) if i.rule else None,
                          "d": json.dumps(asdict(i), ensure_ascii=False, default=str)})
    return len(found)


def load(db, scope: Optional[str] = None) -> List[dict]:
    if not inspect(db.engine).has_table(TABLE):
        return []
    sql, params = f"SELECT * FROM {TABLE}", {}
    if scope:
        sql += " WHERE scope = :s"
        params["s"] = scope
    with db.engine.connect() as conn:
        return [dict(r) for r in conn.execute(text(sql + " ORDER BY created_at DESC"), params).mappings()]


# --- modo de execução por contexto (pilar 3) ---------------------------------------------------

def mode_insights(lab: pd.DataFrame, modes: dict, config=None, alpha: float = 0.01) -> List[Insight]:
    """
    Em que contexto um modo re-simulado rende mais (ou menos) que o modo base, sobre AS MESMAS operações.
    `modes` = {modo: DataFrame com run_id, seq, pnl}. Teste pareado (diferença operação a operação), procura só fora
    da reserva, correção por múltiplos testes e efeito confirmado na reserva — os mesmos critérios dos outros insights.
    """
    from scipy import stats as st
    minimum = aggregate.min_trades(config)
    plan = rules.split(lab, config)
    candidates = []
    for mode, frame in modes.items():
        alt = frame.set_index(["run_id", "seq"])["pnl"]
        paired = lab.assign(alt=[alt.get(k, np.nan) for k in zip(lab["run_id"], lab["seq"])]).dropna(subset=["alt"])
        paired = paired.assign(diff=paired["alt"] - paired["pnl"])
        search = paired[~plan.holdout.reindex(paired.index, fill_value=False)]
        for dim, (title, fmt, _rule) in DIMENSIONS.items():
            if dim not in search or search[dim].isna().all():
                continue
            for value, group in search.dropna(subset=[dim]).groupby(dim):
                if len(group) < minimum:
                    continue
                res = st.ttest_1samp(group["diff"].to_numpy(float), 0.0)
                p = 1.0 if res.pvalue is None or math.isnan(res.pvalue) else float(res.pvalue)
                candidates.append((mode, dim, value, fmt(value), group, paired, p))
    adjusted = _bh([c[-1] for c in candidates])
    out = []
    for (mode, dim, value, label, group, paired, p), p_adj in zip(candidates, adjusted):
        gain = float(group["diff"].mean())
        hold = paired[plan.holdout.reindex(paired.index, fill_value=False) & (paired[dim] == value)]
        hold_p = 1.0
        if len(hold) >= 2:
            res = st.ttest_1samp(hold["diff"].to_numpy(float), 0.0)
            if res.pvalue is not None and not math.isnan(res.pvalue):
                hold_p = float(res.pvalue) / 2 if np.sign(hold["diff"].mean()) == np.sign(gain) else 1.0
        validated = p_adj < alpha and not plan.weak and hold_p < HOLDOUT_ALPHA
        word = "rende mais" if gain > 0 else "rende menos"
        text = (f"{label}: o modo {mode} {word} que o base em {_money(abs(gain))} por operação, nas mesmas "
                f"{len(group)} operações (procura). Testadas {len(candidates)} combinações de modo × contexto; "
                f"p ajustado {p_adj:.3f}. Na reserva final: {len(hold)} operações, efeito "
                f"{'confirmado' if hold_p < HOLDOUT_ALPHA else 'não confirmado'}.")
        rule = {"tipo": "trocar_modo", "modo": mode, "quando": {dim: [value if not isinstance(value, np.generic)
                                                                      else value.item()]}} if gain > 0 else None
        out.append(Insight(dim, value if not isinstance(value, np.generic) else value.item(), f"{label} × {mode}",
                           text, rule, VALIDATED if validated else EXPLORATORY, len(group),
                           float(group["alt"].mean()), float(group["pnl"].mean()), p, p_adj, len(candidates), 0,
                           len(plan.folds), None, None, plan.weak, [f"modo: {mode}"]))
    out.sort(key=lambda i: (i.state != VALIDATED, i.p_adjusted))
    return out
