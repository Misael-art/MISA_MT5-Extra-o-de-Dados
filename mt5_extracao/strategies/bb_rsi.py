"""Reversão à média: toque na banda de Bollinger com RSI(2) extremo em mercado lateral."""
from . import indicators as ind
from .base import ExitRules, Strategy, StrategyInfo, as_int


class BollingerRsiReversion(Strategy):
    key = "bb_rsi"
    info = StrategyInfo(
        name="Reversão BB/RSI", regime="lateral", win_rate="60–70%", reward_risk="≈ 1:1",
        complexity="baixa", timeframes=("M15", "H1", "H4", "D1"), markets="índices, FX, ações líquidas",
        summary="Com ADX < 20, compra quando a mínima toca a banda inferior e o RSI(2) < 10; "
                "sai na banda central; stop de 1,5×ATR ou tempo máximo.")
    default_params = {"bb_period": 20, "bb_k": 2.0, "rsi_period": 2, "rsi_low": 10.0,
                      "adx_max": 20.0, "stop_atr": 1.5, "max_bars": 10}
    param_grid = {"bb_k": [1.5, 2.0, 2.5], "rsi_low": [5.0, 10.0]}

    def _signals(self, df, out):
        p = self.params
        close = df["close"]
        bb = ind.bollinger(close, as_int(p["bb_period"]), p["bb_k"])
        r = ind.rsi(close, as_int(p["rsi_period"]))
        lateral = ind.adx(df, 14)["adx"] < p["adx_max"]
        out["entry_long"] = lateral & (df["low"] <= bb["lower"]) & (r < p["rsi_low"])
        out["entry_short"] = lateral & (df["high"] >= bb["upper"]) & (r > 100 - p["rsi_low"])  # simétrico: RSI(2) > 90
        out["exit_long"] = close >= bb["mid"]
        out["exit_short"] = close <= bb["mid"]
        out["stop_long"] = close - p["stop_atr"] * out["atr"]
        out["stop_short"] = close + p["stop_atr"] * out["atr"]

    def exit_rules(self):
        return ExitRules(max_bars=as_int(self.params["max_bars"]))
