"""Pullback em momentum: tendência forte, RSI recua abaixo de 40 e volta a subir."""
from . import indicators as ind
from .base import ExitRules, Strategy, StrategyInfo, as_int


class PullbackMomentum(Strategy):
    key = "pullback"
    info = StrategyInfo(
        name="Pullback em momentum", regime="tendência", win_rate="45–55%", reward_risk="1:2",
        complexity="média", timeframes=("H1", "H4"), markets="índices, FX, ações líquidas",
        summary="Com preço acima da EMA200 e ADX > 25, compra quando o RSI(14) recua abaixo de 40 e vira "
                "para cima; stop abaixo do último fundo, alvo 2R com parcial em 1R.")
    default_params = {"trend": 200, "adx_min": 25.0, "rsi_period": 14, "rsi_pullback": 40.0,
                      "swing_bars": 10, "target_r": 2.0}
    param_grid = {"rsi_pullback": [35.0, 40.0, 45.0], "swing_bars": [5, 10]}

    def _signals(self, df, out):
        p = self.params
        close = df["close"]
        trend = ind.ema(close, as_int(p["trend"]))
        strong = ind.adx(df, 14)["adx"] > p["adx_min"]
        r = ind.rsi(close, as_int(p["rsi_period"]))
        turn_up = (r.shift(1) < p["rsi_pullback"]) & (r > r.shift(1))
        turn_down = (r.shift(1) > 100 - p["rsi_pullback"]) & (r < r.shift(1))
        out["entry_long"] = (close > trend) & strong & turn_up
        out["entry_short"] = (close < trend) & strong & turn_down
        n = as_int(p["swing_bars"])
        out["stop_long"] = ind.swing_low(df, n)
        out["stop_short"] = ind.swing_high(df, n)

    def exit_rules(self):
        return ExitRules(target_r=self.params["target_r"], partial_r=1.0, partial_fraction=0.5,
                         breakeven_after_partial=True)
