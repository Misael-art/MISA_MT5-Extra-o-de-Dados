"""Prévia pela especificação atual; a execução nativa revalida preço, risco e margem."""
from decimal import Decimal, ROUND_FLOOR
import math

from ..strategies.backtester import SymbolSpec


class SizingPreviewUnavailable(ValueError):
    """Não há dados para a prévia; somente o Tester pode validar seu contexto real."""


def preview_sizing(sizing, *, info, stop_points, deposit):
    if info is None:
        raise SizingPreviewUnavailable("Especificação atual indisponível; reabra para atualizar a prévia.")
    names = {"point": "point", "tick_size": "trade_tick_size", "tick_value": "trade_tick_value",
             "volume_min": "volume_min", "volume_max": "volume_max", "volume_step": "volume_step"}
    values = {key: getattr(info, name, None) for key, name in names.items()}
    if any(type(v) not in (int, float) or not math.isfinite(v) or v <= 0 for v in values.values()):
        raise SizingPreviewUnavailable("Especificação atual incompleta: confira ponto, tick, valor, mínimo, máximo e passo; sem valores presumidos.")
    spec = SymbolSpec(**values)
    if spec.volume_max < spec.volume_min:
        raise SizingPreviewUnavailable("Máximo do símbolo menor que o mínimo; atualize a especificação.")
    if not math.isfinite(deposit) or deposit <= 0 or not math.isfinite(stop_points) or stop_points <= 0:
        raise ValueError("Capital simulado e SL devem ser positivos e finitos para a prévia.")
    params = sizing["params"]
    budget = deposit * params["risk_fraction"]
    distance = stop_points * spec.point
    maximum_risk_volume = spec.lots_for_risk(budget, distance)
    requested = None
    effective = maximum_risk_volume
    if sizing["mode"] == "fixed_volume":
        requested = params["volume"]
        raw, step, maximum = map(lambda v: Decimal(str(v)), (requested, spec.volume_step, spec.volume_max))
        effective = float((min(raw, maximum) / step).to_integral_value(rounding=ROUND_FLOOR) * step)
        if effective > maximum_risk_volume:
            raise ValueError("Volume fixo excede o orçamento de risco; reduza o volume ou o SL, sem elevar o limite.")
    if effective < spec.volume_min:
        raise ValueError("Volume efetivo abaixo do mínimo; nenhuma entrada pode ser enviada nesse contexto.")
    return dict(requested=requested, effective=effective, minimum=spec.volume_min, maximum=spec.volume_max,
                step=spec.volume_step, risk_money=effective * distance * spec.money_per_price_unit,
                risk_budget=budget, margin_known=False, source="current_symbol_info", historical_qualification=False)
