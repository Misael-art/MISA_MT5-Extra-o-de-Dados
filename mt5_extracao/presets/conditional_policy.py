"""
Política condicional de estudo (T19.52a): primeiro recorte do Ultra sobre o livro de operações.

    report = study(db, config, "EURUSD", "M15")
    report["status"]            # "elegível", "reprovada" ou "sem candidato elegível"

Com runs JÁ gravados de vários setups do mesmo ativo e timeframe, escolhe na PROCURA qual setup operar em cada contexto
conhecido na entrada (período do dia × regime de volatilidade point-in-time), congela a prioridade e percorre a
VALIDAÇÃO e a RESERVA em ordem de entrada como uma conta de posição única. Contexto sem regra bloqueia a entrada (R00).
O piso de fator de lucro é condição de elegibilidade nas duas provas, nunca uma garantia; o "oráculo" que escolhe o
melhor setup olhando o próprio trecho avaliado sai rotulado como não executável e não entra no parecer.

É diagnóstico: não gera `.set`, não altera o HADOUKEN e não substitui T19.52–54 (predicados tipados, pacote, execução
nativa, UI). Contrato completo: docs/propostas/PILARES_DADOS_OPERACAO_PRESETS.md, seção 8.5, e
docs/propostas/ANALYTICS_OPERACOES.md, seção 11.12.
"""
import hashlib
import json
from typing import Iterable, Optional

import pandas as pd

from mt5_extracao import profit_factor
from mt5_extracao.trade_analytics import aggregate, stats

VERSION = 1
DIMENSIONS = ("period", "volatility_bucket")
PERIOD_HOURS = 4
MIN_PROFIT_FACTOR = 2.0
TOP_SETUPS = 2            # setups validados aceitos por contexto, em ordem de prioridade
FALLBACK = "R00"


def _context(lab: pd.DataFrame, dims) -> pd.Series:
    data = lab.copy()
    hour = pd.to_numeric(data.get("hour"), errors="coerce")
    data["period"] = [None if pd.isna(h) else f"{int(h) // PERIOD_HOURS * PERIOD_HOURS:02d}h" for h in hour]
    missing = [d for d in dims if d not in data]
    if missing:
        raise ValueError(f"Dimensão de contexto desconhecida: {', '.join(missing)}.")
    return data[list(dims)].apply(
        lambda row: None if any(pd.isna(v) for v in row) else " | ".join(str(v) for v in row), axis=1)


def _one_run_per_setup(lab: pd.DataFrame, runs: Optional[Iterable[str]]) -> pd.DataFrame:
    """Um run por setup: repetir o mesmo backtest não aumenta a amostra."""
    if runs:
        chosen = lab[lab.run_id.isin(list(runs))]
        if chosen.groupby("robot_key").run_id.nunique().max() > 1:
            raise ValueError("Informe no máximo um run por robô; runs repetidos inflariam a amostra.")
        return chosen
    first = lab.groupby("robot_key").run_id.min()
    return lab[lab.run_id.isin(set(first))]


def _segments(lab: pd.DataFrame, fractions) -> pd.Series:
    """Procura / validação / reserva por tempo de entrada; operação que cruza um corte fica fora (embargo)."""
    order = lab.entry_time.sort_values()
    first = order.iloc[min(len(order) - 1, int(len(order) * fractions[0]))]
    second = order.iloc[min(len(order) - 1, int(len(order) * (fractions[0] + fractions[1])))]
    label = pd.Series("embargo", index=lab.index)
    label[lab.exit_time < first] = "search"
    label[(lab.entry_time >= first) & (lab.exit_time < second)] = "validation"
    label[lab.entry_time >= second] = "holdout"
    return label


def _metrics(part: pd.DataFrame, initial: float) -> dict:
    ordered = part.sort_values("exit_time")
    out = stats.metrics(ordered.pnl, initial)
    return {key: out[key] for key in ("n", "win_rate", "profit_factor", "profit_factor_status", "expectancy", "net",
                                      "max_drawdown_pct")}


def _rules(search: pd.DataFrame, min_rule_trades: int, top: int) -> dict:
    """{contexto: [setups em prioridade]}: só setups lucrativos na procura, com n mínimo no contexto."""
    table = {}
    for context, cell in search.groupby("context"):
        ranked = []
        for setup, part in cell.groupby("robot_key"):
            pf = profit_factor.fields(part.pnl)
            if len(part) >= min_rule_trades and profit_factor.passes(pf["profit_factor"], 1.0):
                ranked.append((profit_factor.sort_key(pf["profit_factor"]), -float(part.pnl.mean()), setup,
                               {"setup": setup, "n": int(len(part)), "profit_factor": pf["profit_factor"],
                                "expectancy": float(part.pnl.mean())}))
        if ranked:
            table[context] = [item[3] for item in sorted(ranked, key=lambda item: item[:3])[:top]]
    return table


def _walk(part: pd.DataFrame, accept) -> pd.DataFrame:
    """Conta de posição única: percorre em ordem de entrada e só aceita com a posição anterior encerrada."""
    taken, free_at = [], None
    ordered = part.assign(_rank=[accept(row) for row in part.itertuples(index=False)])
    ordered = ordered[ordered["_rank"].notna()].sort_values(["entry_time", "_rank", "robot_key", "seq"], kind="stable")
    for row in ordered.itertuples():
        if free_at is None or row.entry_time >= free_at:
            taken.append(row.Index)
            free_at = row.exit_time
    return part.loc[taken]


def study(db, config, symbol: str, timeframe: str, *, runs: Optional[Iterable[str]] = None,
          dims: Iterable[str] = DIMENSIONS, min_profit_factor: float = MIN_PROFIT_FACTOR,
          min_rule_trades: Optional[int] = None, top: int = TOP_SETUPS,
          fractions=(0.6, 0.2, 0.2)) -> dict:
    dims = tuple(dims)
    if abs(sum(fractions) - 1) > 1e-9 or any(f <= 0 for f in fractions) or len(fractions) != 3:
        raise ValueError("As frações de procura, validação e reserva devem ser três valores positivos somando 1.")
    if not profit_factor.finite(min_profit_factor) or min_profit_factor < 1:
        raise ValueError("O piso de fator de lucro deve ser um número finito a partir de 1.")
    minimum = aggregate.min_trades(config)
    min_rule_trades = int(min_rule_trades or minimum)
    if min_rule_trades < 1 or int(top) < 1:
        raise ValueError("Mínimo de operações por regra e número de setups por contexto devem ser positivos.")
    lab = aggregate.frame(db, config, symbol=symbol, timeframe=timeframe)
    if not lab.empty:
        lab = lab[(lab.completeness == "completo") & lab.entry_time.notna() & lab.exit_time.notna()
                  & lab.pnl.notna()]
        lab = _one_run_per_setup(lab, runs)
    if lab.empty or lab.robot_key.nunique() < 2:
        raise ValueError(f"São necessários runs completos de ao menos dois setups em {symbol} {timeframe} "
                         "(veja mt5x trades runs).")
    lab = lab.reset_index(drop=True)
    lab["context"] = _context(lab, dims)
    lab["segment"] = _segments(lab, fractions)
    initial = aggregate._initial_equity(config)
    search = lab[(lab.segment == "search") & lab.context.notna()]
    rules = _rules(search, min_rule_trades, int(top))
    priority = {context: {item["setup"]: rank for rank, item in enumerate(items)} for context, items in rules.items()}
    overall = []
    for setup, part in lab[lab.segment == "search"].groupby("robot_key"):
        pf = profit_factor.fields(part.pnl)["profit_factor"]
        if len(part) >= minimum:
            overall.append((profit_factor.sort_key(pf), setup))
    static = sorted(overall)[0][1] if overall else None
    policy = {"schema_version": VERSION, "kind": "conditional_policy_study", "symbol": symbol, "timeframe": timeframe,
              "dimensions": list(dims), "period_hours": PERIOD_HOURS, "rules": rules, "fallback": FALLBACK,
              "fallback_action": "block_new_entries", "min_rule_trades": min_rule_trades, "top": int(top),
              "runs": sorted(lab.run_id.unique()), "native_set_equivalent": False, "activation_authorized": False}
    policy["policy_id"] = hashlib.sha256(json.dumps(policy, sort_keys=True, ensure_ascii=False,
                                                    separators=(",", ":")).encode("utf-8")).hexdigest()
    segments = {}
    for name in ("search", "validation", "holdout"):
        part = lab[lab.segment == name]
        routed = _walk(part, lambda row: priority.get(row.context, {}).get(row.robot_key))
        eligible_signals = int(part.apply(lambda row: row.robot_key in priority.get(row.context, {}), axis=1).sum()) \
            if len(part) else 0
        oracle_rules = {c: {item["setup"]: r for r, item in enumerate(items)}
                        for c, items in _rules(part[part.context.notna()], 1, 1).items()}
        segments[name] = {
            "n_signals": int(len(part)), "blocked_no_rule": int(len(part) - eligible_signals),
            "blocked_position_open": int(eligible_signals - len(routed)),
            "policy": _metrics(routed, initial),
            "rule_usage": {str(k): int(v) for k, v in routed.context.value_counts().items()},
            "static_best": None if static is None else {"setup": static, **_metrics(
                _walk(part, lambda row: 0 if row.robot_key == static else None), initial)},
            "accept_all": _metrics(_walk(part, lambda row: 0), initial),
            "setups": {setup: _metrics(rows, initial) for setup, rows in part.groupby("robot_key")},
            "oracle_not_executable": _metrics(
                _walk(part, lambda row: oracle_rules.get(row.context, {}).get(row.robot_key)), initial),
        }
    reasons = []
    if not rules:
        status = "sem candidato elegível"
        reasons.append(f"Nenhum setup lucrativo com ao menos {min_rule_trades} operações em algum contexto da procura.")
    else:
        for name, label in (("validation", "validação"), ("holdout", "reserva final")):
            got = segments[name]["policy"]
            if got["n"] < minimum:
                reasons.append(f"Na {label} a política fez {got['n']} operação(ões); o mínimo é {minimum}.")
            elif not profit_factor.passes(got["profit_factor"], min_profit_factor - 1e-12):
                reasons.append(f"Na {label} o fator de lucro foi {profit_factor.text(got['profit_factor'])}; "
                               f"o piso é {profit_factor.text(min_profit_factor)}.")
        reference = segments["holdout"]["static_best"]
        if reference is not None and segments["holdout"]["policy"]["net"] <= reference["net"]:
            reasons.append(f"Na reserva final a política não supera o melhor setup estático ({reference['setup']}).")
        status = "reprovada" if reasons else "elegível"
    return {"schema_version": VERSION, "status": status, "reasons": reasons, "study_only": True,
            "min_profit_factor": float(min_profit_factor), "policy": policy, "segments": segments,
            "embargoed": int((lab.segment == "embargo").sum()),
            "context_missing": int(lab.context.isna().sum()),
            "note": "Piso de fator de lucro é condição de elegibilidade, não garantia; resultado sobre barras e "
                    "custos do backtest de origem."}
