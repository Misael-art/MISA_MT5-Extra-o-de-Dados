"""TM07: horizonte ATR causal, limitado desde o primeiro fill."""
from dataclasses import dataclass
import math
import numpy as np
from . import indicators
from .temporal_donchian import DonchianTemporalPolicy

ATR_PERIOD = 14
ATR_SEED_BARS = 250
ATR_FEATURE = "volatility.atr_14"

@dataclass(frozen=True)
class AtrTemporalPolicy(DonchianTemporalPolicy):
    atr_base_seconds: int = 0
    atr_min_seconds: int = 0

    def __post_init__(self):
        super().__post_init__()
        for value in (self.atr_base_seconds, self.atr_min_seconds):
            if type(value) is not int or not 0 <= value <= 2147483647:
                raise ValueError("Prazos ATR devem ser segundos inteiros representáveis; zero desabilita.")
        if self.atr_base_seconds:
            if not 0 < self.atr_min_seconds <= self.atr_base_seconds <= self.max_seconds:
                raise ValueError("ATR requer 0 < mínimo <= prazo base <= teto absoluto desde primeiro fill.")
        elif self.atr_min_seconds:
            raise ValueError("Mínimo ATR requer prazo base positivo.")

    @property
    def enabled(self):
        return super().enabled or self.atr_base_seconds > 0

    def horizon(self, baseline, current):
        if not self.atr_base_seconds or any(type(v) not in (int, float) or not math.isfinite(v) or v <= 0 for v in (baseline, current)):
            return None
        ratio = baseline / current
        if ratio >= self.max_seconds / self.atr_base_seconds:
            return self.max_seconds
        if ratio <= self.atr_min_seconds / self.atr_base_seconds:
            return self.atr_min_seconds
        # Segundo inteiro superior; banda de 1 ns resolve ruído na igualdade.
        return max(self.atr_min_seconds, math.ceil(self.atr_base_seconds * ratio - 1e-9))

    def decision(self, age_ms, net, er=None, *, idle_ms=None, returned=False, atr_baseline=None, atr_current=None):
        prior = super().decision(age_ms, net, er=er, idle_ms=idle_ms, returned=returned)
        if prior.startswith("time_") or type(age_ms) is not int or age_ms < 0:
            return prior
        horizon = self.horizon(atr_baseline, atr_current)
        return "time_atr" if horizon is not None and age_ms >= horizon * 1000 else prior


def observations(policy, data):
    from .temporal_donchian import observations as channel_observations
    result = channel_observations(policy, data)
    result.update(atr_data=data, atr_positions={})
    return result


def decide(policy, age_ms, net, values, index, *, direction, opened_index):
    from .temporal_donchian import decide as channel_decide
    # O helper anterior chama policy.decision sem ATR, preservando prioridades.
    prior, metadata = channel_decide(policy, age_ms, net, values, index, direction=direction, opened_index=opened_index)
    cache = values["atr_positions"]
    if opened_index not in cache:
        start = opened_index - ATR_SEED_BARS
        data = values["atr_data"].iloc[max(0, start):]
        valid = np.isfinite(data[["high", "low", "close"]]).all(axis=1) & data.high.ge(data.low) & data.close.between(data.low, data.high)
        series = indicators.atr(data, ATR_PERIOD)
        cache[opened_index] = (start, series, valid.cummin())
    start, series, valid = cache[opened_index]
    baseline = current = None
    if start >= 0 and index >= opened_index and bool(valid.iloc[index-start]):
        baseline, current = float(series.iloc[ATR_SEED_BARS-1]), float(series.iloc[index-start])
    horizon = policy.horizon(baseline, current)
    known = horizon is not None
    metadata.update(atr_feature=ATR_FEATURE, atr_seed_bars=ATR_SEED_BARS,
                    atr_baseline=baseline if known else None, atr_current=current if known else None,
                    atr_horizon_seconds=horizon, atr_seed_index=start if known else None)
    if prior.startswith("time_") or type(age_ms) is not int or age_ms < 0:
        return prior, metadata
    return ("time_atr" if known and age_ms >= horizon * 1000 else prior), metadata
