"""Opening Range Breakout: rompimento da faixa dos primeiros minutos do dia, zerando no fim do dia."""
import pandas as pd

from .base import ExitRules, Strategy, StrategyInfo


class OpeningRangeBreakout(Strategy):
    key = "orb"
    info = StrategyInfo(
        name="Opening Range Breakout", regime="rompimento", win_rate="40–50%", reward_risk="1:1,5 a 1:2",
        complexity="média", timeframes=("M5", "M15"), markets="WIN/WDO, DAX, US500/NAS100",
        summary="Faixa = máxima/mínima dos primeiros 30 min do dia; compra no fechamento acima da faixa, "
                "stop no lado oposto, alvo de 1,5× a faixa e saída obrigatória no fim do dia.")
    default_params = {"range_minutes": 30, "target_mult": 1.5, "min_range_atr": 0.0}
    param_grid = {"range_minutes": [15, 30], "target_mult": [1.5, 2.0]}
    min_bars = 100

    def _signals(self, df, out):
        p = self.params
        times = pd.to_datetime(df["time"])
        day = times.dt.normalize()
        # Abertura da sessão = primeira barra de cada dia presente nos dados
        session_open = times.groupby(day).transform("min")
        in_range = times < session_open + pd.Timedelta(minutes=float(p["range_minutes"]))
        rng_high = df["high"].where(in_range).groupby(day).transform("max")
        rng_low = df["low"].where(in_range).groupby(day).transform("min")
        size = rng_high - rng_low
        after = ~in_range & (size > p["min_range_atr"] * out["atr"].fillna(0))
        above = after & (df["close"] > rng_high)
        below = after & (df["close"] < rng_low)
        # Apenas o primeiro rompimento de cada dia
        first = (above | below) & ((above | below).astype(int).groupby(day).cumsum() == 1)
        out["entry_long"] = first & above
        out["entry_short"] = first & below
        out["stop_long"] = rng_low
        out["stop_short"] = rng_high
        out["target_long"] = df["close"] + p["target_mult"] * size
        out["target_short"] = df["close"] - p["target_mult"] * size
        out.loc[size.isna() | (size <= 0), ["entry_long", "entry_short"]] = False

    def exit_rules(self):
        return ExitRules(eod_exit=True)
