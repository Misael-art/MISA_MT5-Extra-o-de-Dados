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

Identidade: `policy_id` identifica só a política (regras, contexto, fallback, runs). `evaluation_id` identifica a
avaliação inteira: política, frações e datas dos cortes, piso, mínimos, capital, escolha de run e a proveniência de
cada run (moeda, manifesto de dados, execução). `promotion_blockers` lista o que impede promover o estudo a Ultra
(T19.53/54) mesmo quando o parecer é "elegível": comparabilidade não comprovada não vira aprovação por omissão.
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


def _one_run_per_setup(lab: pd.DataFrame, runs: Optional[Iterable[str]]):
    """Um run por setup: repetir o mesmo backtest não aumenta a amostra. Devolve (linhas, escolha explícita)."""
    available = {str(robot): sorted(str(r) for r in part.run_id.unique()) for robot, part in lab.groupby("robot_key")}
    if runs:
        chosen = lab[lab.run_id.isin(list(runs))]
        if chosen.groupby("robot_key").run_id.nunique().max() > 1:
            raise ValueError("Informe no máximo um run por robô; runs repetidos inflariam a amostra.")
        mode = "explicit"
    else:
        first = lab.groupby("robot_key").run_id.min()
        chosen = lab[lab.run_id.isin(set(first))]
        mode = "lowest_run_id"
    picked = {str(robot): str(part.run_id.iloc[0]) for robot, part in chosen.groupby("robot_key")}
    ignored = {robot: [r for r in ids if r != picked.get(robot)] for robot, ids in available.items()}
    selection = {"mode": mode, "chosen": picked, "ignored": {k: v for k, v in ignored.items() if v}}
    return chosen, selection


def _cut_points(lab: pd.DataFrame, fractions, cuts=None):
    """Instantes dos cortes: calculados pela distribuição das entradas ou congelados pelo chamador."""
    if cuts is not None:
        tz = getattr(pd.to_datetime(lab.entry_time).dt, "tz", None)
        first, second = (pd.Timestamp(c) for c in cuts)
        if tz is not None:
            first, second = (c.tz_localize(tz) if c.tzinfo is None else c.tz_convert(tz) for c in (first, second))
        elif first.tzinfo is not None or second.tzinfo is not None:
            raise ValueError("Cortes com fuso não se aplicam a horários de entrada sem fuso.")
        if not first < second:
            raise ValueError("Os cortes congelados devem estar em ordem: procura < validação < reserva.")
        return first, second
    order = lab.entry_time.sort_values()
    first = order.iloc[min(len(order) - 1, int(len(order) * fractions[0]))]
    second = order.iloc[min(len(order) - 1, int(len(order) * (fractions[0] + fractions[1])))]
    return pd.Timestamp(first), pd.Timestamp(second)


def _segments(lab: pd.DataFrame, fractions, cuts=None) -> pd.Series:
    """Procura / validação / reserva por tempo de entrada; operação que cruza um corte fica fora (embargo)."""
    return _label(lab, *_cut_points(lab, fractions, cuts))


def _label(lab: pd.DataFrame, first, second) -> pd.Series:
    label = pd.Series("embargo", index=lab.index)
    label[lab.exit_time < first] = "search"
    label[(lab.entry_time >= first) & (lab.exit_time < second)] = "validation"
    label[lab.entry_time >= second] = "holdout"
    return label


def _iso(value) -> Optional[str]:
    return None if value is None or pd.isna(value) else pd.Timestamp(value).isoformat()


def _run_records(db, lab: pd.DataFrame) -> dict:
    """Metadados declarados de cada run usado (sem criar outro contrato): moeda, manifesto, execução."""
    if db is None or getattr(db, "engine", None) is None:
        return {}
    from mt5_extracao.trade_analytics import cache
    with db.consistent_read() as connection:
        rows = cache._run_part(db, lab[["run_id"]].drop_duplicates(), connection=connection)
    records = {}
    for row in rows:
        try:
            metrics = json.loads(row.get("metrics_json") or "{}")
        except (TypeError, ValueError):
            metrics = {}
        identity = metrics.get("execution_identity") if isinstance(metrics.get("execution_identity"), dict) else {}
        records[str(row["run_id"])] = {
            "source": row.get("source"), "currency": metrics.get("currency"),
            "data_manifest_id": metrics.get("data_manifest_id"),
            "execution_digest": identity.get("digest"), "code_sha256": identity.get("code_sha256"),
            "comparison_context": metrics.get("comparison_context")}
    return records


def _comparability(lab: pd.DataFrame, records: dict, cuts) -> dict:
    """Runs combinados numa conta única precisam de mesma moeda e de janela que cubra a reserva."""
    windows, blockers = {}, []
    for run_id, part in lab.groupby("run_id"):
        windows[str(run_id)] = {"first_entry": _iso(part.entry_time.min()), "last_exit": _iso(part.exit_time.max())}
    runs = sorted(windows)
    currencies = {r: (records.get(r) or {}).get("currency") for r in runs}
    known = {c for c in currencies.values() if c}
    if len(known) > 1:
        raise ValueError("Runs em moedas diferentes não podem ser combinados numa conta única: "
                         + ", ".join(f"{r}={c}" for r, c in currencies.items()) + ".")
    if not known or len(known) != 1 or any(not c for c in currencies.values()):
        blockers.append("Moeda de algum run não declarada; resultados combinados sem comparabilidade comprovada.")
    manifests = {r: (records.get(r) or {}).get("data_manifest_id") for r in runs}
    if any(not m for m in manifests.values()):
        blockers.append("Manifesto de dados ausente em algum run; fonte e revisão das barras não comprovadas.")
    elif len(set(manifests.values())) > 1:
        blockers.append("Runs com manifestos de dados diferentes; confirme mesma fonte, revisão e custos antes de promover.")
    if any(not (records.get(r) or {}).get("execution_digest") for r in runs):
        blockers.append("Identidade de execução ausente em algum run; código e parâmetros não comprovados.")
    second = cuts[1]
    short = [r for r, w in windows.items() if w["last_exit"] is None or pd.Timestamp(w["last_exit"]) < second]
    if short:
        blockers.append("Run(s) sem operações depois do início da reserva: " + ", ".join(short)
                        + "; a reserva não compara os setups na mesma janela.")
    status = "comprovada" if not blockers else "não comprovada"
    return {"status": status, "currency": next(iter(known)) if len(known) == 1 else None,
            "windows": windows, "runs": {r: records.get(r) for r in runs}, "blockers": blockers}


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
          fractions=(0.6, 0.2, 0.2), cuts=None) -> dict:
    """`cuts=(início da validação, início da reserva)` reaplica cortes já congelados em vez de recalculá-los."""
    dims = tuple(dims)
    if abs(sum(fractions) - 1) > 1e-9 or any(f <= 0 for f in fractions) or len(fractions) != 3:
        raise ValueError("As frações de procura, validação e reserva devem ser três valores positivos somando 1.")
    if not profit_factor.finite(min_profit_factor) or min_profit_factor < 1:
        raise ValueError("O piso de fator de lucro deve ser um número finito a partir de 1.")
    minimum = aggregate.min_trades(config)
    min_rule_trades = int(min_rule_trades or minimum)
    if min_rule_trades < 1 or int(top) < 1:
        raise ValueError("Mínimo de operações por regra e número de setups por contexto devem ser positivos.")
    lab = aggregate.frame(db, config, symbol=symbol, timeframe=timeframe, include_repeated_runs=bool(runs))
    if not lab.empty:
        lab = lab[(lab.completeness == "completo") & lab.entry_time.notna() & lab.exit_time.notna()
                  & lab.pnl.notna()]
        lab, selection = _one_run_per_setup(lab, runs)
    if lab.empty or lab.robot_key.nunique() < 2:
        raise ValueError(f"São necessários runs completos de ao menos dois setups em {symbol} {timeframe} "
                         "(veja mt5x trades runs).")
    lab = lab.reset_index(drop=True)
    lab["context"] = _context(lab, dims)
    cut_points = _cut_points(lab, fractions, cuts)
    lab["segment"] = _label(lab, *cut_points)
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
    comparability = _comparability(lab, _run_records(db, lab), cut_points)
    segmentation = {"fractions": [float(f) for f in fractions], "cuts": [_iso(c) for c in cut_points],
                    "cuts_frozen_by_caller": cuts is not None, "basis": "entry_time; cruzamento de corte = embargo"}
    evaluation = {"schema_version": VERSION, "kind": "conditional_policy_evaluation",
                  "policy_id": policy["policy_id"], "segmentation": segmentation,
                  "min_profit_factor": float(min_profit_factor), "min_trades": int(minimum),
                  "min_rule_trades": int(min_rule_trades), "initial_equity": float(initial),
                  "run_selection": selection, "runs": comparability["runs"], "windows": comparability["windows"]}
    evaluation["evaluation_id"] = hashlib.sha256(json.dumps(
        evaluation, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()
    blockers = list(comparability["blockers"])
    if selection["mode"] != "explicit" and selection["ignored"]:
        blockers.append("Havia outros runs do mesmo robô e a escolha foi pelo menor identificador; informe os runs "
                        "explicitamente antes de promover.")
    if status != "elegível":
        blockers.insert(0, f"Parecer do estudo: {status}.")
    return {"schema_version": VERSION, "status": status, "reasons": reasons, "study_only": True,
            "min_profit_factor": float(min_profit_factor), "policy": policy, "segments": segments,
            "evaluation": evaluation, "comparability": comparability["status"],
            "promotion_blockers": blockers,
            "embargoed": int((lab.segment == "embargo").sum()),
            "context_missing": int(lab.context.isna().sum()),
            "note": "Piso de fator de lucro é condição de elegibilidade, não garantia; resultado sobre barras e "
                    "custos do backtest de origem."}
