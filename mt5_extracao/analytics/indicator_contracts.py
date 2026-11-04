"""BD06 (T18.1/T19.3): contrato completo de cada definição do registry, sem fórmula paralela.

O registry continua dono da fórmula, parâmetros, unidade, aquecimento e disponibilidade. Este módulo
acrescenta o que o contrato do plano exige e o registry não guardava: pergunta/significado, tipo de
saída e buffers, regra de ausência, alinhamento, revisão, consumidores, referência independente e
apresentação. Não entra em ``indicator_materializer.implementation_signature``: mudar estes metadados
não versiona valores numéricos (assinatura granular continua pendente em BD13).
"""
from mt5_extracao.analytics import indicator_registry

CONTRACT_VERSION = 1
FIELDS = ("question", "output_kind", "buffers", "missing_rule", "alignment", "revision_rule", "consumers",
          "reference", "presentation")
_CATEGORY_UNITS = {"direction_code_-1_0_1"}
_REVISION = "revisão da barra de origem gera nova revisão do valor (source_revision); versões anteriores preservadas"
_CONSUMERS = ("store/indicator_materializer", "trade_analytics.indicator_context", "data_exporter",
              "chart_study (recalcula; leitura do store pendente BD19d)", "CLI features")
_PENDING = "pendente: referência independente própria ainda não escrita (BD08b)"

# (prefixo, pergunta, referência, painel, traço)
_FAMILIES = (
    ("momentum.rsi_", "Qual a força relativa recente de altas e baixas do fechamento?",
     "tests/test_bd08b_wilder.py::_expected (semente first; reinício após ausência)", "oscillator", "line"),
    ("volatility.atr", "Qual a amplitude típica recente das barras, incluindo gaps?",
     "tests/test_bd08b_wilder.py::_expected (semente first; reinício após ausência)", "oscillator", "line"),
    ("volatility.bb_", "Quão largas estão as bandas de Bollinger e onde está o fechamento nelas?",
     "tests/bench_reference.py::bollinger_width (largura); posição: " + _PENDING, "oscillator", "line"),
    ("volatility.kc_", "Quão largo está o canal de Keltner e onde está o fechamento nele?", _PENDING,
     "oscillator", "line"),
    ("momentum.macd_", "Qual a diferença entre médias exponenciais rápida e lenta do fechamento?",
     "tests/test_bd08b_macd.py::_expected (decimal60; EMA first seed; reinício por entrada)",
     "oscillator", "histogram"),
    ("price.return_", "Quanto o fechamento variou em N barras, como fração?",
     "tests/bench_reference.py::returns", "oscillator", "line"),
    ("trend.ema20_", "A quantos ATR o fechamento está da média exponencial de 20?", _PENDING, "oscillator", "line"),
    ("trend.efficiency_ratio", "Quão direto foi o caminho do preço em N barras?",
     "tests/bench_reference.py::efficiency_ratio", "oscillator", "line"),
    ("volume.", "Qual o volume da barra, na unidade da própria fonte?", "valor da fonte (sem cálculo)",
     "volume", "histogram"),
    ("trend.alligator_", "Como se alinham as médias suavizadas do Alligator?",
     "docs/analytics/T19.4_ALLIGATOR.md", "price", "line"),
    ("momentum.ao_", "Qual o momento do preço mediano entre médias de 5 e 34?",
     "docs/analytics/T19.5_AO_AC.md", "oscillator", "histogram"),
    ("momentum.ac_", "O momento do AO está acelerando ou desacelerando?",
     "docs/analytics/T19.5_AO_AC.md", "oscillator", "histogram"),
    ("structure.", "Qual o último pivô confirmado, conhecido só após a confirmação?",
     "docs/analytics/T19.6_PIVOS_CAUSAIS.md", "price", "markers"),
    ("channel.donchian_", "Onde estão os extremos de N barras e o fechamento em relação a eles?",
     "tests/bench_reference.py::donchian_upper (upper); demais saídas: " + _PENDING, "price", "line"),
)


def _family(feature_id):
    for prefix, *rest in _FAMILIES:
        if feature_id.startswith(prefix):
            return rest
    raise ValueError(f"Definição sem contrato BD06: {feature_id}; acrescente a família em indicator_contracts.")


def contract(feature_id):
    """Definição do registry + campos de contrato BD06."""
    spec = indicator_registry.get_definition(feature_id)
    question, reference, panel, trace = _family(feature_id)
    parameters = spec["parameters"]
    category = spec["unit"] in _CATEGORY_UNITS
    states = parameters.get("states") or parameters.get("colors")
    spec.update({
        "contract_version": CONTRACT_VERSION,
        "question": question,
        # Armazenado como NUMBER (contrato de gravação T18.1); semântica categórica explícita aqui.
        "output_kind": "category" if category else "number",
        "buffers": [{"name": feature_id, "kind": "category" if category else "number", "unit": spec["unit"],
                     **({"categories": states} if states else {})}],
        "missing_rule": parameters.get("missing_policy",
                                       "janela finita completa exigida; ausência devolve None com estado/motivo, nunca zero"),
        "alignment": parameters.get("value_basis", "valor da barra de cálculo fechada, sem deslocamento") +
                     ("; desenho deslocado " + str(parameters["plot_shift_bars"]) + " barra(s), valor não"
                      if parameters.get("plot_shift_bars") else ""),
        "revision_rule": _REVISION,
        "consumers": list(_CONSUMERS),
        "reference": reference,
        "presentation": {"panel": panel, "trace": trace},
    })
    return spec


def contracts():
    return [contract(item["feature_id"]) for item in indicator_registry.definitions()]


def missing_fields():
    """{feature_id: [campos vazios]} — vazio quando todo o registry tem contrato completo."""
    gaps = {}
    for item in contracts():
        empty = [name for name in FIELDS if item.get(name) in (None, "", [], {})]
        if empty:
            gaps[item["feature_id"]] = empty
    return gaps


def pending_references():
    return sorted(item["feature_id"] for item in contracts() if "pendente" in item["reference"])
