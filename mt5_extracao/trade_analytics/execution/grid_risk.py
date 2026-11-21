"""Projeção nominal do risco da cesta inteira, antes de re-simular o ciclo."""
from fractions import Fraction
from math import ceil, isfinite

from mt5_extracao.analytics import instruments
from mt5_extracao.composition.schema import _decode, _object, _version


_FIELDS = ("version", "mode", "side", "anchor_ticks", "stop_ticks", "spacing", "volumes",
           "currency", "risk_cap_cash", "round_trip_cost_per_lot", "adverse_stop_ticks")
_MODES = ("direct", "fixed_adverse", "atr_adverse", "directional")


def _ticks(value, field, *, allow_zero=False):
    if type(value) is not int or value < (0 if allow_zero else 1):
        raise ValueError(f"{field} exige um número inteiro de ticks.")
    return value


def _number(value, field, *, allow_zero=False):
    try:
        valid = type(value) in (int, float) and isfinite(value)
    except OverflowError:
        valid = False
    if not valid or value < 0 or (not allow_zero and not value):
        raise ValueError(f"{field} exige um número finito {'não negativo' if allow_zero else 'positivo'}.")
    return Fraction(str(value))


def _spacing(mode, raw):
    if mode == "atr_adverse":
        _object(raw, ("atr_in_ticks", "multiplier"), "spacing")
        atr = _number(raw["atr_in_ticks"], "atr_in_ticks")
        multiplier = _number(raw["multiplier"], "multiplier")
        return ceil(atr * multiplier)
    if mode == "direct":
        if type(raw) is not int or raw != 0:
            raise ValueError("A entrada direta exige espaçamento zero.")
        return 0
    return _ticks(raw, "spacing")


def report(snapshot, definition, *, as_of):
    """Usa a especificação vigente e conhecida no corte do snapshot."""
    plan = _decode(definition)
    _object(plan, _FIELDS, "grid")
    _version(plan["version"], "grid.version")
    mode, side = plan["mode"], plan["side"]
    if mode not in _MODES or side not in ("long", "short"):
        raise ValueError("Declare modo de grade e lado reconhecidos.")
    d = 1 if side == "long" else -1
    anchor = _ticks(plan["anchor_ticks"], "anchor_ticks")
    stop = _ticks(plan["stop_ticks"], "stop_ticks")
    adverse = _ticks(plan["adverse_stop_ticks"], "adverse_stop_ticks", allow_zero=True)
    stress_stop = stop - d * adverse
    if stress_stop <= 0 or (anchor - stop) * d <= 0:
        raise ValueError("Stop e ponto de estresse precisam ser válidos no lado protetor da âncora.")
    spacing = _spacing(mode, plan["spacing"])
    volumes = plan["volumes"]
    if type(volumes) is not list or not volumes or (len(volumes) == 1) != (mode == "direct"):
        raise ValueError("Informe um volume na entrada direta ou um vetor de volumes com ao menos dois níveis.")
    cap = _number(plan["risk_cap_cash"], "risk_cap_cash")
    costs = _number(plan["round_trip_cost_per_lot"], "round_trip_cost_per_lot", allow_zero=True)

    cutoff = instruments.utc(as_of)
    history = snapshot.instrument_history
    fact = instruments.specification(history, cutoff) if history is not None else None
    if fact is None:
        raise ValueError("Snapshot sem especificação vigente e conhecida no corte.")
    spec = fact["payload"]
    if not isinstance(plan["currency"], str) or not plan["currency"] or plan["currency"] != spec.get("currency_profit"):
        raise ValueError("A moeda do orçamento precisa coincidir com a especificação do ativo.")
    tick_size = _number(spec.get("tick_size"), "tick_size")
    tick_value = _number(spec.get("tick_value"), "tick_value")
    minimum, step, maximum = (_number(spec.get(key), key) for key in ("volume_min", "volume_step", "volume_max"))

    levels = []
    total_volume = weighted_ticks = 0
    direction = 1 if mode == "directional" else -1
    for i, raw_lot in enumerate(volumes):
        lot = _number(raw_lot, "volumes")
        if not minimum <= lot <= maximum or lot % step != 0:
            raise ValueError(f"Volume fora dos limites ou do passo no nível {i}.")
        level = anchor + d * direction * i * spacing
        if level <= 0 or (level - stop) * d <= 0:
            raise ValueError(f"Nível {i} no stop ou além dele.")
        total_volume += lot
        weighted_ticks += lot * level
        marked = d * (level * total_volume - weighted_ticks) * tick_value
        levels.append({"index": i, "price_ticks": level, "volume": float(lot),
                       "intent": "entry" if i == 0 else "stop_entry" if mode == "directional" else "limit_entry",
                       "gross_mark_at_level_cash": marked})

    gross = d * (weighted_ticks - stop * total_volume) * tick_value
    cost_cash = costs * total_volume
    nominal_loss = gross + cost_cash
    stress_loss = nominal_loss + adverse * total_volume * tick_value
    try:
        total, gross_cash, cost, nominal, stress = map(float, (total_volume, gross, cost_cash, nominal_loss, stress_loss))
        for row in levels:
            row["gross_mark_at_level_cash"] = float(row["gross_mark_at_level_cash"])
    except OverflowError as exc:
        raise ValueError("O resultado excede a faixa numérica do relatório.") from exc
    return {"version": 1, "snapshot_id": snapshot.manifest_id, "symbol": snapshot.manifest["symbol"],
            "as_of": cutoff, "specification_revision": fact["revision_id"], "mode": mode, "side": side,
            "currency": plan["currency"], "tick_size": float(tick_size), "spacing_ticks": spacing, "levels": levels,
            "stop_ticks": stop, "stress_stop_ticks": stress_stop, "total_volume": total,
            "gross_loss_at_stop_cash": gross_cash, "modeled_round_trip_cost_cash": cost,
            "loss_at_stop_cash": nominal, "loss_at_stress_cash": stress, "risk_cap_cash": float(cap),
            "risk_status": "within_modeled_cap" if stress_loss <= cap else "exceeds_modeled_cap",
            "basis": "all_levels_nominal_with_initial_stop", "margin_status": "unassessed",
            "execution_status": "unassessed"}
