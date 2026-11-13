"""Proveniência descritiva do recorte recebido, sem alterar métricas ou inferir custos."""
import math

import pandas as pd


def population(lab, initial_equity=100_000.0):
    """Intervalos são das operações observadas, não os períodos completos dos testes.

    O contrato do ledger não registra moeda nem custos discriminados. Sua ausência
    não significa custo zero, nem certifica que PnL/PF sejam líquidos.
    """
    n = len(lab)
    def column(name):
        return lab[name] if name in lab else pd.Series(index=lab.index, dtype=object)
    runs = column("run_id").dropna().unique()
    repeats = lab.attrs.get("repeated_runs") or {}
    groups = repeats.get("groups") or {}
    visible = {str(run): int(count) for run, count in column("run_id").value_counts().items()}
    repeated_runs = sum(len(aliases) for run, aliases in groups.items() if run in visible)
    repeated_trades = sum(visible.get(run, 0) * len(aliases) for run, aliases in groups.items())
    unqualified = sum(run in visible for run in repeats.get("unqualified") or [])
    refs = sorted(str(v) for v in column("time_ref").dropna().unique())
    starts, ends = [], []
    for _, group in lab.groupby("run_id", sort=False) if "run_id" in lab else []:
        exit_times = pd.to_datetime(group.get("exit_time"), errors="coerce", utc=True)
        entry_times = pd.to_datetime(group.get("entry_time"), errors="coerce", utc=True)
        if exit_times is None or not exit_times.notna().any():
            continue
        first = entry_times.fillna(exit_times).min() if entry_times is not None else exit_times.min()
        starts.append(first)
        ends.append(exit_times.max())
    intervals = sorted(zip(starts, ends))
    overlap, last = False, None
    for start, end in intervals:
        if last is not None and start < last:
            overlap = True
        last = max(last, end) if last is not None else end
    try:
        capital = float(initial_equity)
        if not math.isfinite(capital):
            capital = None
    except (TypeError, ValueError):
        capital = None
    return {"version": 1, "n": n, "runs": len(runs),
            "repeated_runs_excluded": repeated_runs, "repeated_trades_excluded": repeated_trades,
            "unqualified_python_runs": unqualified, "include_repeated_runs": repeats.get("include_repeated_runs", False),
            "missing_run_id": int(column("run_id").isna().sum()),
            "partial": int(column("completeness").eq("parcial").sum()),
            "missing_entry": int(column("entry_time").isna().sum()),
            "missing_direction": int((~column("direction").isin([-1, 1])).sum()),
            "time_references": refs, "sources": sorted(str(v) for v in column("source").dropna().unique()),
            "aggregation": "descriptive_runs" if len(runs) > 1 else "single_run" if len(runs) == 1 else "unknown",
            "observed_run_intervals": len(intervals), "observed_overlap": overlap if intervals else None,
            "initial_equity_reference": capital, "currency_status": "not_recorded",
            "costs_status": "not_recorded", "timezone_status": "not_recorded"}


def temporal_label(refs):
    refs = set(refs)
    if refs == {"entrada"}:
        return "Entrada"
    if refs == {"saida"}:
        return "Saída (entrada indisponível)"
    if refs == {"entrada", "saida"}:
        return "Referência mista: entrada quando disponível; saída nas demais"
    return "Referência temporal indisponível"


def summary(context):
    parts = []
    if context.get("repeated_runs_excluded"):
        parts.append(f"{context['repeated_runs_excluded']} execuções repetidas agrupadas "
                     f"({context['repeated_trades_excluded']} operações).")
    if context.get("unqualified_python_runs"):
        parts.append(f"{context['unqualified_python_runs']} execuções Python com identidade incompleta. "
                     "A equivalência dessas execuções permanece desconhecida.")
    if context.get("include_repeated_runs"):
        parts.append("Consulta completa inclui todas as execuções guardadas.")
    if context["runs"] > 1:
        parts.append(f"{context['runs']} execuções: agregação descritiva, não carteira executável.")
    if context["missing_entry"]:
        parts.append(f"{context['missing_entry']} sem horário de entrada: calendário pela saída.")
    elif context["missing_direction"]:
        parts.append(f"{context['missing_direction']} sem direção válida.")
    parts.append("Moeda e custos não documentados no livro.")
    return " ".join(parts)


def details(context):
    capital = context["initial_equity_reference"]
    capital_text = f"{capital:,.2f}" if capital is not None else "desconhecido"
    overlap = ("Há sobreposição entre intervalos de operações observadas; isso não prova duplicidade."
               if context["observed_overlap"] else "Sem sobreposição demonstrada entre os intervalos observados.")
    return (f"{context['n']} operações · {context['runs']} execuções identificadas.\n"
            f"Sem entrada: {context['missing_entry']}; sem direção válida: {context['missing_direction']}. "
            "A parcialidade não identifica tecnologia, versão ou causa.\n"
            f"Referência temporal: {temporal_label(context['time_references'])}. Fuso não documentado.\n"
            f"Fontes registradas: {', '.join(context['sources']) or 'não informadas'}.\n"
            f"Queda máxima: referência inicial {capital_text}; curva ordenada pelas saídas. "
            "A agregação de runs não reproduz posições, margem ou capital de uma carteira.\n"
            f"{overlap} Os intervalos não representam toda a cobertura dos testes.\n"
            "Moeda e custos discriminados não constam deste contrato; não presumir custo zero "
            "nem certificar resultado/PF líquido. Os números originais são preservados.")
