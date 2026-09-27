"""Squeeze: Bollinger dentro do Keltner (compressão) e rompimento a favor do momentum."""
from . import indicators as ind
from .base import ExitRules, Strategy, StrategyInfo, as_int


class Squeeze(Strategy):
    key = "squeeze"
    info = StrategyInfo(
        name="Squeeze BB/Keltner", regime="compressão → rompimento", win_rate="40–50%",
        reward_risk="1:2 ou mais", complexity="média", timeframes=("H1", "H4"),
        markets="ouro, índices, cripto",
        summary="Quando as Bandas de Bollinger ficam dentro do Canal de Keltner, espera o rompimento da "
                "banda a favor do momentum; stop de 1,5×ATR e trailing de 2,5×ATR.")
    default_params = {"period": 20, "bb_k": 2.0, "kc_mult": 1.5, "lookback": 5, "stop_atr": 1.5,
                      "trail_atr": 2.5}
    param_grid = {"kc_mult": [1.25, 1.5, 2.0], "lookback": [3, 5]}

    def _signals(self, df, out):
        p = self.params
        n = as_int(p["period"])
        close = df["close"]
        bb = ind.bollinger(close, n, p["bb_k"])
        kc = ind.keltner(df, n, p["kc_mult"])
        squeeze_on = (bb["upper"] < kc["upper"]) & (bb["lower"] > kc["lower"])
        recent = squeeze_on.astype(int).rolling(as_int(p["lookback"]), min_periods=1).max().shift(1) == 1
        momentum = close - ind.sma(close, n)
        out["entry_long"] = recent & (close > bb["upper"]) & (momentum > 0)
        out["entry_short"] = recent & (close < bb["lower"]) & (momentum < 0)
        out["exit_long"] = (momentum < 0) & (momentum.shift(1) >= 0)
        out["exit_short"] = (momentum > 0) & (momentum.shift(1) <= 0)
        out["stop_long"] = close - p["stop_atr"] * out["atr"]
        out["stop_short"] = close + p["stop_atr"] * out["atr"]

    def exit_rules(self):
        return ExitRules(trailing_atr=self.params["trail_atr"])
