"""Consulta de estados monetários registrados pelo produtor canônico, sem reexecutá-lo."""
import copy
from numbers import Integral

from .paths import _number


def _records(payload, clock):
    if payload.get("schema_version") != 1 or payload.get("timestamp_basis") != "source_clock":
        raise ValueError("Formato de observações desconhecido; recupere o produtor compatível.")
    groups = {}
    for raw in payload.get("records", []):
        row = dict(raw)
        seq = row.get("cycle_seq")
        if isinstance(seq, bool) or not isinstance(seq, Integral) or seq < 0:
            raise ValueError("Identidade de ciclo inválida nas observações.")
        group = groups.setdefault(int(seq), [])
        event_seq = row.get("event_seq")
        if (isinstance(event_seq, bool) or not isinstance(event_seq, Integral) or event_seq != len(group) or
                row.get("kind") not in ("entry", "add", "partial", "mark", "exit")):
            raise ValueError("Sequência de eventos incompleta ou duplicada no ciclo.")
        start, end = clock.timestamp(row["start_time"]), clock.timestamp(row["end_time"])
        available = clock.timestamp(row["available_at"])
        phase = row.get("phase")
        if (end < start or available != end or phase not in ("open", "intrabar", "close") or
                (phase != "intrabar" and start != end) or
                (group and (end < group[-1]["_end"] or start < group[0]["_start"]))):
            raise ValueError("Intervalo ou disponibilidade temporal incoerente no ciclo.")
        volume = _number(row.get("volume_open"))
        if volume is None or volume < 0 or (row["kind"] == "exit" and volume != 0) or (row["kind"] != "exit" and volume == 0):
            raise ValueError("Exposição inválida no registro do ciclo.")
        if not group and (row["kind"] != "entry" or volume <= 0 or phase != "open"):
            raise ValueError("Ciclo sem primeiro preenchimento explícito.")
        if group and (row["kind"] == "entry" or group[-1]["kind"] == "exit"):
            raise ValueError("Ciclo reutilizado depois de encerrado ou com entrada duplicada.")
        for name in ("realized_net", "net_pnl", "fill_price", "fill_volume"):
            if row.get(name) is not None and _number(row[name]) is None:
                raise ValueError("Estado monetário não finito no registro do ciclo.")
        row.update(_start=start, _end=end)
        group.append(row)
    return groups


def summarize(payload, *, run_id, source, clock, as_of=None, thresholds=()):
    """Um DTO por ciclo explícito; valores do motor não são recalculados.

    Intervalos intrabarra permanecem intervalos. Censura temporal não preenche
    valores de PnL entre estados observados. O chamador liga run/seq ao ledger
    na persistência; esta função pura não certifica essa ligação nem o mercado.
    """
    if not isinstance(source, str):
        raise ValueError("Origem do livro deve ser declarada explicitamente.")
    expected = "python_model" if source == "python" else "resimulation_model" if source.startswith("resim:") else None
    if expected is None or payload.get("source") != expected or not run_id:
        raise ValueError("Origem do produtor não corresponde ao livro; não transfira observações entre fontes.")
    levels = [_number(v) for v in thresholds]
    if any(v is None or v == 0 for v in levels) or len(set(levels)) != len(levels):
        raise ValueError("Limiares monetários devem ser finitos, distintos e diferentes de zero.")
    cutoff = clock.timestamp(as_of) if as_of is not None else None
    units = []
    for seq, group in sorted(_records(payload, clock).items()):
        entered = group[0]["_start"]
        exit_ = group[-1] if group[-1]["kind"] == "exit" else None
        if cutoff is None and exit_ is None:
            raise ValueError("Ciclo aberto exige as_of explícito; não use o horário do computador.")
        limit = cutoff if cutoff is not None else exit_["_end"]
        row = {"schema_version": 1, "identity": {"run_id": run_id, "seq": seq, "source": source},
               "observation_unit": "cycle", "unit": "account_money", "currency": payload.get("currency"),
               "valuation": copy.deepcopy(payload.get("valuation", {})), "status": "not_started",
               "age_min": None, "age_lower_min": None, "age_upper_min": None, "right_censored": None,
               "final_net_pnl": None, "mfe_observed": None, "mae_observed": None,
               "cash_state": "unavailable",
               "mfe_time": None, "mae_time": None, "points": [], "thresholds": [],
               "limitations": ["sampled_model_states_not_continuous_ticks"],
               "sequence_basis": "modeled_execution_order", "timestamp_basis": "source_clock"}
        if payload.get("currency") is None:
            row["limitations"].append("currency_code_unknown")
        if row["valuation"].get("status") != "modeled":
            row["limitations"].append("valuation_not_fully_qualified")
        if limit < entered:
            units.append(row)
            continue
        visible = [r for r in group if r["_end"] <= limit]
        closed = bool(visible and visible[-1]["kind"] == "exit")
        # Entre observações não sabemos se já houve saída intrabarra. A censura
        # usa o último estado positivo conhecido, sem consultar o próximo evento.
        end_min = visible[-1]["_start"] if closed else visible[-1]["_end"]
        end_max = visible[-1]["_end"]
        if not closed and end_max < limit:
            row["limitations"].append("as_of_between_observations")
        lower, upper = ((v - entered).total_seconds() / 60 for v in (end_min, end_max))
        row.update(status="observed", start_utc=entered.isoformat(), end_utc=end_max.isoformat(),
                   requested_as_of_utc=limit.isoformat(),
                   age_lower_min=lower, age_upper_min=upper, age_min=lower if lower == upper else None,
                   right_censored=not closed,
                   final_net_pnl=visible[-1].get("net_pnl") if closed else None)
        for item in visible:
            point = {k: v for k, v in item.items() if not k.startswith("_")}
            point.update(start_utc=item["_start"].isoformat(), end_utc=item["_end"].isoformat(),
                         age_start_min=(item["_start"] - entered).total_seconds() / 60,
                         age_end_min=(item["_end"] - entered).total_seconds() / 60)
            row["points"].append(point)
        marked = [p for p in row["points"] if p.get("net_pnl") is not None]
        if marked:
            row["cash_state"] = "observed_model"
        else:
            row["limitations"].append("net_valuation_unavailable")
        if closed and row["final_net_pnl"] is None:
            row["limitations"].append("final_net_unavailable")
        for key, favorable in (("mfe", True), ("mae", False)):
            if marked:
                chosen = (max if favorable else min)(marked, key=lambda p: p["net_pnl"])
                value = chosen["net_pnl"] if favorable else -chosen["net_pnl"]
                row[key + "_observed"] = max(0.0, value)
                if value > 0:
                    row[key + "_time"] = {k: chosen[k] for k in (
                        "start_utc", "end_utc", "age_start_min", "age_end_min")}
        for level in levels:
            hit = next((p for p in marked if (p["net_pnl"] >= level if level > 0 else p["net_pnl"] <= level)), None)
            row["thresholds"].append({"level": level, "state": "first_observed_hit" if hit else "unknown",
                                      "window": {k: hit[k] for k in (
                                          "start_utc", "end_utc", "age_start_min", "age_end_min")} if hit else None,
                                      "first_absolute_crossing_known": False})
        units.append(row)
    return units
