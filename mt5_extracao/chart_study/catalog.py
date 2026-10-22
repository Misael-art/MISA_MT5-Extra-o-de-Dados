"""Projeção do catálogo existente para ferramentas gráficas; sem outro motor."""
from mt5_extracao.analytics import indicator_registry
from mt5_extracao.strategies import REGISTRY


def rule(spec):
    ident, unit = spec["feature_id"], spec["unit"]
    if unit == "points_0_100":
        return "midpoint_50", "Valor acima/abaixo de50"
    if unit == "channel_fraction":
        return "midpoint_half", "Acima/abaixo do meio do canal"
    if any(part in ident for part in ("_slope_", "return_", "alignment", "distance_atr", "histogram", "momentum.ao_", "momentum.ac_")):
        return "zero", "Valor positivo/negativo"
    if unit == "price_units" and ident.startswith(("trend.", "channel.", "structure.")):
        return "price", "Fechamento acima/abaixo da linha"
    return "rising", "Valor crescente/decrescente"


def entries():
    items = []
    for key, cls in REGISTRY.items():
        items.append(dict(id="strategy:" + key, name=cls.info.name, origin="extractor_strategy", scale="signal",
                          rule="candidate", legend="Compra/venda = candidato; demais barras neutras; não são trades",
                          parameters=cls.default_params, warmup=cls.min_bars))
    for spec in indicator_registry.definitions():
        color_rule, legend = rule(spec)
        scale = "price" if color_rule == "price" else "percent_0_100" if spec["unit"] == "points_0_100" else spec["unit"]
        if scale == "price_units":
            scale = "price_distance"
        items.append(dict(id="feature:" + spec["feature_id"], name=spec["name"], origin="extractor_indicator",
                          scale=scale, rule=color_rule, legend=legend, parameters=spec["parameters"],
                          warmup=spec["warmup_count"], algorithm_version=spec["algorithm_version"]))
    return items


def get(ident):
    for item in entries():
        if item["id"] == ident:
            return item
    raise ValueError("Indicador/estratégia não pertence ao catálogo de estudo.")
