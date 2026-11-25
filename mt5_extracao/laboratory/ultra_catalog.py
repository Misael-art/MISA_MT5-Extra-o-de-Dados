"""Inventário operacional do Ultra sobre os registros do Laboratório e da composição."""
from dataclasses import asdict

from ..composition.registry import NATIVE_STRATEGY_DEFAULTS
from ..strategies import REGISTRY
from .ultra import SIGNALS


# O nome do artefato descreve a referência canônica de um robô independente.
# A presença dele, por si só, não autoriza a emissão pelo Expert concorrente.
DETAILS = {
    "ema_atr": ("HDK_EmaAtr", "EMA rápida/lenta/de tendência, ATR Wilder",
                "EMAs e ATR recursivos desde o prefixo, cada saída após seu período",
                "Cruzamento alinhado à tendência, saída no cruzamento inverso, SL e trailing por ATR"),
    "donchian": ("HDK_Donchian", "Canais anteriores, ADX, ATR Wilder e SMA do ATR",
                 "Canal prévio completo, mais ADX disponível ou SMA do ATR completa",
                 "Rompimento filtrado, saída no canal oposto e SL por ATR"),
    "bb_rsi": ("HDK_BbRsi", "Bollinger, RSI Wilder, ADX e ATR Wilder",
               "Bollinger com janela cheia, RSI após primeiro delta e ADX após suas duas suavizações",
               "Toque e RSI extremo com ADX lateral, saída na média ou por tempo e SL por ATR"),
    "orb": ("HDK_Orb", "Faixa de abertura e filtro de amplitude por ATR",
            "A faixa só está disponível após sua janela, ancorada na sessão completa",
            "Primeiro rompimento da faixa, stop oposto, alvo proporcional, saída até o fim da sessão"),
    "pullback": ("HDK_Pullback", "EMA de tendência, ADX, RSI Wilder e swings",
                 "Prefixo recursivo de EMA/ADX/RSI, dois RSI para a virada e janela de swing completa",
                 "Virada do RSI na tendência, SL no swing, parcial em 1R com breakeven e alvo em R"),
    "squeeze": ("HDK_Squeeze", "Bollinger, Keltner, momentum close−SMA, ATR Wilder",
                "Bollinger e Keltner disponíveis, compressão estrita em uma barra anterior da janela",
                "Rompimento confirmado pelo sinal de momentum, saída na inversão, stop e trailing por ATR"),
    "s09_reference": ("HDK_S09_Referencia", "EMA curta e longa",
                      "Prefixo íntegro desde o primeiro fechamento, aquecimento declarado e cruzamento",
                      "Cruzamento em barra fechada, entrada seguinte, SL/TP em pontos no preço do fill"),
    "stoch_ema9": ("HDK_StochEma9", "K por razão de somas, D por média, EMA de prefixo",
                   "Prefixo íntegro e janela K/slowing/D, com aquecimento mínimo de 300 barras",
                   "Cruzamento do estocástico extremo alinhado à EMA, SL móvel na EMA e alvo opcional"),
    "vwap_bands": ("HDK_VwapBands", "HLC3 ponderado por volume escolhido causalmente",
                   "Âncora por data do servidor, denominador positivo e aquecimento de 300 barras",
                   "Toque de uma banda, SL além dela, TP na banda interior congelado no sinal"),
    "madrid": ("HDK_Madrid", "Dez EMAs de prefixo e ATR Wilder",
               "300 barras, dados completos para o leque e ATR positivo",
               "Cruzamento da primeira EMA com leque alinhado e afastamento limitado, guard e TP"),
    "ha_hilo": ("HDK_HaHiLo", "Heiken-Ashi recursivo e HiLo",
                "300 barras e linha HiLo válida, com sementes calculadas no prefixo",
                "HA do mesmo lado da abertura e do HiLo, SL de sinal e saída na condição oposta"),
    "pattern_definition": (None, "Regras causais de hipótese versionada",
                           "Depende das observações, janelas e tempos gráficos da definição",
                           "Ação/instante declarados, invalidação de entrada quando suportada"),
    "super_atr_triple": ("HDK_SuperATRTriplo", "Três níveis de Super ATR",
                         "Prefixo nativo com semente datada e tipo de média declarado",
                         "Sinal nativo e proteção nos níveis declarados; entradas exigem habilitação"),
    "super_atr_triple_plot": ("HDK_SuperATRTriplo", "Variante gráfica do Super ATR triplo",
                              "Mesma semente e aquecimento da variante nativa",
                              "Mesmo contrato de execução e configuração adicional de plots"),
}

# Evidência do livro Ultra existente. Cada modo declara o recorte efetivamente exercitado.
EVIDENCE = {
    "donchian": "Sinal M1 e gestão fixa do Ultra nas duas fitas sintéticas; a saída de canal e o SL por ATR são da estratégia de referência.",
    "s09_reference": "Sinal M1 e gestão fixa do Ultra nas duas fitas sintéticas com ordens; a amostra não representa EURUSD.",
    "pattern_definition": "Subconjunto OHLC/prior_close M1; houve negócios em ensaio com divergência no tick 199.",
}


def physical_signal_supported(key):
    return key in SIGNALS and key in EVIDENCE


def catalog():
    keys = [*REGISTRY, *NATIVE_STRATEGY_DEFAULTS, "pattern_definition"]
    unmapped = sorted(set(keys).difference(DETAILS))
    strategies = []
    for key in keys:
        model = REGISTRY[key]() if key in REGISTRY else None
        native, indicators, warmup, rules = DETAILS.get(key, (None, None, None, None))
        strategies.append(dict(key=key, name=model.info.name if model else (native or key),
            reference=model.__class__.__module__ if model else (
                "mt5_extracao.analytics.pattern_engine" if key == "pattern_definition" else native),
            native_reference=native, indicators=indicators, warmup=warmup, reference_rules=rules,
            default_params=dict(model.params) if model else NATIVE_STRATEGY_DEFAULTS.get(key),
            minimum_reference_bars=model.min_bars if model else None,
            data=list(model.required_columns) if model else None,
            reference_timeframes=list(model.info.timeframes) if model else None,
            reference_exits=asdict(model.exit_rules()) if model else None,
            decision_clock=("bar_close" if model else "definition_or_native_contract"),
            ultra_scope=("bounded_m1_synthetic" if physical_signal_supported(key) else "missing"),
            supported_account_modes=(["hedging", "netting_exclusive_cycle"] if physical_signal_supported(key) else []),
            evidence=EVIDENCE.get(key), full_reference_execution=False, eurusd_market="pending",
            eurusd_data_constraint=("volume_source=2 exige negócios; volume_source=1 usa cotações; modo Auto é causal."
                                    if key == "vwap_bands" else None)))
    return dict(strategies=strategies, unmapped=unmapped, python_count=len(REGISTRY),
                missing_portfolio=[s["key"] for s in strategies if s["ultra_scope"] == "missing"],
                full_portfolio_coverage=all(row["full_reference_execution"] for row in strategies))
