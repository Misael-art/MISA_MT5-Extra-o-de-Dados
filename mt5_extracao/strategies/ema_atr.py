"""Tendência EMA + ATR: cruzamento de médias a favor da EMA longa, stop e trailing por ATR."""
from . import indicators as ind
from .base import ExitRules, Strategy, StrategyInfo, as_int


class EmaAtrTrend(Strategy):
    key = "ema_atr"
    info = StrategyInfo(
        name="Tendência EMA+ATR", regime="tendência", win_rate="35–45%", reward_risk="1:2 ou mais",
        complexity="baixa", timeframes=("H1", "H4", "D1"), markets="índices, FX, commodities",
        summary="Compra quando a EMA rápida cruza acima da lenta com o preço acima da EMA200; "
                "stop de 2×ATR e trailing de 3×ATR.")
    default_params = {"fast": 20, "slow": 50, "trend": 200, "stop_atr": 2.0, "trail_atr": 3.0}
    param_grid = {"fast": [10, 20, 30], "slow": [50, 100]}

    def _signals(self, df, out):
        p = self.params
        close = df["close"]
        fast, slow, trend = ind.ema(close, as_int(p["fast"])), ind.ema(close, as_int(p["slow"])), \
            ind.ema(close, as_int(p["trend"]))
        up, down = self.crossed_above(fast, slow), self.crossed_below(fast, slow)
        out["entry_long"] = up & (close > trend)
        out["entry_short"] = down & (close < trend)
        out["exit_long"] = down
        out["exit_short"] = up
        out["stop_long"] = close - p["stop_atr"] * out["atr"]
        out["stop_short"] = close + p["stop_atr"] * out["atr"]

    def exit_rules(self):
        return ExitRules(trailing_atr=self.params["trail_atr"])
