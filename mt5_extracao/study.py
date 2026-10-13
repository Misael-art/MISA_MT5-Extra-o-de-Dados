"""
Números de estudo sobre as barras salvas na base: resumo do período, indicadores do gráfico,
sazonalidade (hora do dia e dia da semana) e os textos de ajuda mostrados na interface.

Só pandas/numpy: a interface (aba Análise) apenas desenha o que sai daqui.
"""
from datetime import timedelta
from typing import Optional

import numpy as np
import pandas as pd

from mt5_extracao.strategies import indicators

# Períodos oferecidos na aba Análise: (rótulo, dias corridos; None = tudo o que há na base)
PERIODS = (("1 mês", 30), ("3 meses", 90), ("6 meses", 180), ("1 ano", 365), ("Tudo", None))

WEEKDAYS = ("Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom")

# Explicações curtas (dicas da interface e Ajuda > Glossário)
GLOSSARY = {
    "Último preço": "Fechamento da barra mais recente do período carregado.",
    "Retorno no período": "Variação do fechamento entre a primeira e a última barra do período.",
    "Volatilidade anualizada": "Desvio-padrão dos retornos por barra, convertido para um ano de 252 pregões. "
                               "Quanto maior, mais o preço oscila.",
    "Queda máxima": "Maior queda do preço a partir de um topo anterior dentro do período (drawdown). "
                    "Mostra o pior momento que quem estava comprado enfrentou.",
    "Amplitude média": "Média de (máxima - mínima) de cada barra, em % do fechamento. "
                       "Ajuda a dimensionar stops e alvos.",
    "Barras de alta": "Percentual de barras que fecharam acima da abertura.",
    "Volume médio": "Média do volume de ticks por barra (número de mudanças de preço).",
    "Spread médio": "Média do spread registrado nas barras, em pontos. É um custo de cada operação.",
    "Média móvel (MM)": "Média dos últimos N fechamentos. Preço acima da média sugere tendência de alta; "
                        "abaixo, de baixa.",
    "Bandas de Bollinger": "Média de 20 barras mais e menos 2 desvios-padrão. Bandas estreitas indicam "
                           "pouca volatilidade; preço fora das bandas indica movimento esticado.",
    "IFR (RSI)": "Índice de Força Relativa de 14 barras, de 0 a 100. Acima de 70 costuma ser lido como "
                 "sobrecomprado e abaixo de 30 como sobrevendido.",
    "Sazonalidade": "Comportamento médio por hora do dia e por dia da semana. Mostra quando o ativo "
                    "costuma oscilar mais e se há viés de alta ou baixa em algum horário.",
    "Lacunas no pregão": "Barras que faltam dentro do horário de negociação. Muitas lacunas distorcem "
                         "indicadores e backtests: vale extrair o período de novo.",
}


def period_start(end, days: Optional[int]):
    """Início do período de `days` dias corridos que termina em `end` (None = sem limite)."""
    if days is None or end is None:
        return None
    return pd.Timestamp(end) - timedelta(days=days)


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Cópia das barras com ma20, ma50, bb_upper, bb_lower e rsi."""
    out = df.copy()
    bands = indicators.bollinger(out["close"], 20, 2.0)
    out["ma20"] = bands["mid"]
    out["ma50"] = indicators.sma(out["close"], 50)
    out["bb_upper"] = bands["upper"]
    out["bb_lower"] = bands["lower"]
    out["rsi"] = indicators.rsi(out["close"], 14)
    return out


def summarize(df: pd.DataFrame) -> dict:
    """Resumo do período para os cartões da aba Análise (dicionário vazio se não houver barras)."""
    if df is None or df.empty:
        return {}
    close = df["close"].astype(float)
    returns = close.pct_change().dropna()
    times = pd.to_datetime(df["time"])
    days = max(1, times.dt.date.nunique())
    bars_per_year = len(df) / days * 252
    peak = close.cummax()
    span = (df["high"] - df["low"]) / close
    summary = {
        "bars": int(len(df)),
        "start": times.iloc[0].to_pydatetime(),
        "end": times.iloc[-1].to_pydatetime(),
        "last": float(close.iloc[-1]),
        "high": float(df["high"].max()),
        "low": float(df["low"].min()),
        "return_pct": float((close.iloc[-1] / close.iloc[0] - 1) * 100) if close.iloc[0] else 0.0,
        "volatility_pct": float(returns.std(ddof=0) * np.sqrt(bars_per_year) * 100) if len(returns) > 1 else 0.0,
        "max_drawdown_pct": float((close / peak - 1).min() * 100),
        "mean_range_pct": float(span.mean() * 100),
        "up_bars_pct": float((df["close"] > df["open"]).mean() * 100),
        "mean_volume": float(df["tick_volume"].mean()) if "tick_volume" in df.columns else None,
        "mean_spread": float(df["spread"].mean()) if "spread" in df.columns and df["spread"].notna().any() else None,
    }
    return summary


def seasonality(df: pd.DataFrame) -> dict:
    """
    Retorno médio (%) e amplitude média (%) por hora do dia e por dia da semana:
    {"hour": DataFrame, "weekday": DataFrame}, colunas return_pct, range_pct e bars.
    Em barras diárias ou maiores não há hora do dia: "hour" vem vazio.
    """
    empty = pd.DataFrame(columns=["return_pct", "range_pct", "bars"])
    if df is None or len(df) < 2:
        return {"hour": empty, "weekday": empty}
    times = pd.to_datetime(df["time"])
    data = pd.DataFrame({
        "return_pct": (df["close"].astype(float) / df["open"].astype(float) - 1) * 100,
        "range_pct": (df["high"] - df["low"]) / df["close"] * 100,
        "hour": times.dt.hour.values, "weekday": times.dt.weekday.values})

    def by(column):
        grouped = data.groupby(column).agg(return_pct=("return_pct", "mean"), range_pct=("range_pct", "mean"),
                                           bars=("return_pct", "size"))
        return grouped.sort_index()

    intraday = times.dt.hour.nunique() > 1
    return {"hour": by("hour") if intraday else empty, "weekday": by("weekday")}


def compress(df: pd.DataFrame, max_candles: int = 240):
    """
    Junta barras vizinhas para o gráfico não passar de `max_candles` candles.
    Retorna (barras, fator): fator 1 = nada foi agrupado; 5 = cada candle representa 5 barras.
    """
    if len(df) <= max_candles:
        return df.reset_index(drop=True), 1
    factor = int(np.ceil(len(df) / max_candles))
    groups = np.arange(len(df)) // factor
    spec = {"time": "first", "open": "first", "high": "max", "low": "min", "close": "last"}
    if "tick_volume" in df.columns:
        spec["tick_volume"] = "sum"
    return df.groupby(groups).agg(spec).reset_index(drop=True), factor


def freshness(end, bar_minutes: int, now=None) -> str:
    """Texto curto sobre a atualização da série: 'em dia', 'há 3 dias'..."""
    if end is None:
        return "sem dados"
    now = now or pd.Timestamp.now()
    delta = pd.Timestamp(now) - pd.Timestamp(end)
    days = delta.days
    # Fim de semana e a própria barra em formação não contam como atraso
    if delta <= timedelta(minutes=bar_minutes) + timedelta(days=3):
        return "em dia"
    if days < 60:
        return f"há {days} dias"
    return f"há {days // 30} meses"
