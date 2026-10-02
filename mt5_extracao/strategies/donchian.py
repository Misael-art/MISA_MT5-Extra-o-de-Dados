"""Donchian / Turtle: rompimento de 20 barras, saída no rompimento contrário de 10."""
from . import indicators as ind
from .base import Strategy, StrategyInfo, as_int


class DonchianTurtle(Strategy):
    key = "donchian"
    info = StrategyInfo(
        name="Donchian/Turtle", regime="tendência", win_rate="30–40%", reward_risk="1:2,5 ou mais",
        complexity="baixa", timeframes=("H4", "D1"), markets="commodities, índices, FX",
        summary="Compra no rompimento da máxima de 20 barras e sai no rompimento da mínima de 10; "
                "só opera com ADX > 20 ou ATR acima da média.")
    default_params = {"entry_period": 20, "exit_period": 10, "adx_min": 20.0, "atr_avg": 50, "stop_atr": 2.0}
    param_grid = {"entry_period": [20, 40, 55], "exit_period": [10, 20]}

    def _signals(self, df, out):
        p = self.params
        close = df["close"]
        entry = ind.donchian(df, as_int(p["entry_period"])).shift(1)   # canal das barras ANTERIORES
        exit_ = ind.donchian(df, as_int(p["exit_period"])).shift(1)
        adx = ind.adx(df, 14)["adx"]
        active = (adx > p["adx_min"]) | (out["atr"] > ind.sma(out["atr"], as_int(p["atr_avg"])))
        out["entry_long"] = active & (close > entry["upper"])
        out["entry_short"] = active & (close < entry["lower"])
        out["exit_long"] = close < exit_["lower"]
        out["exit_short"] = close > exit_["upper"]
        out["stop_long"] = close - p["stop_atr"] * out["atr"]
        out["stop_short"] = close + p["stop_atr"] * out["atr"]
