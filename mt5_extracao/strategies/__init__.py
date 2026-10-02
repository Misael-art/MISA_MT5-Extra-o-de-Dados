"""
Estratégias, backtest, validação e triagem de ativos (Fase 6).

Uso:
    from mt5_extracao.strategies import get_strategy, REGISTRY
    sinais = get_strategy("donchian").gerar_sinais(df)
"""
from .base import ExitRules, SIGNAL_COLUMNS, Strategy, StrategyInfo
from .bb_rsi import BollingerRsiReversion
from .donchian import DonchianTurtle
from .ema_atr import EmaAtrTrend
from .orb import OpeningRangeBreakout
from .pullback import PullbackMomentum
from .squeeze import Squeeze

REGISTRY = {cls.key: cls for cls in (EmaAtrTrend, DonchianTurtle, BollingerRsiReversion,
                                     OpeningRangeBreakout, PullbackMomentum, Squeeze)}

# Práticas a evitar no início (exibidas na CLI, no relatório e na documentação)
AVOID = (
    ("Martingale", "dobra a posição após perdas: uma sequência ruim zera a conta"),
    ("Grid sem stop", "acumula posições contra o movimento sem limite de perda"),
    ("Scalping de poucos pontos", "custos (spread, corretagem, slippage) consomem a vantagem"),
    ("Arbitragem de latência", "exige infraestrutura dedicada; no varejo o resultado é ilusório"),
    ("ML caixa-preta", "sem entender o porquê, é fácil ajustar ao ruído (overfitting)"),
)


def get_strategy(key: str, **params) -> Strategy:
    try:
        cls = REGISTRY[key.strip().lower()]
    except KeyError:
        raise ValueError(f"Estratégia desconhecida: '{key}'. Disponíveis: {', '.join(REGISTRY)}") from None
    return cls(**params)


def parse_keys(text: str):
    """'all' / 'todas' / vazio -> todas; senão lista separada por vírgula (validada)."""
    if not text or text.strip().lower() in ("all", "todas"):
        return list(REGISTRY)
    keys = [k.strip().lower() for k in text.split(",") if k.strip()]
    for k in keys:
        get_strategy(k)
    return keys


__all__ = ["AVOID", "ExitRules", "REGISTRY", "SIGNAL_COLUMNS", "Strategy", "StrategyInfo",
           "get_strategy", "parse_keys"]
