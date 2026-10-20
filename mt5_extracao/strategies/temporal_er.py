"""TM05: ER canônico fechado e idade, sem mudar TemporalPolicy v1."""
from dataclasses import dataclass
import math
from .temporal import TemporalPolicy
from .indicators import efficiency_ratio

ER_PERIOD = 20
ER_FEATURE = "trend.efficiency_ratio_20"
# Fração ER em [0,1], 20 diferenças: igualdade numérica mantém, nunca força saída.
# Contrato inicial explícito; não altera o valor/armazenamento do indicador canônico.
ER_EQUALITY_BAND = 1e-12


@dataclass(frozen=True)
class EfficiencyTemporalPolicy(TemporalPolicy):
    er_min_seconds: int = 0
    maximum_er: float = 0

    def __post_init__(self):
        super().__post_init__()
        if type(self.er_min_seconds) is not int or not 0 <= self.er_min_seconds <= 2147483647:
            raise ValueError("Idade mínima ER deve ser segundos inteiros entre0 e2147483647.")
        if type(self.maximum_er) not in (int, float) or not math.isfinite(self.maximum_er) or not 0 <= self.maximum_er <= 1:
            raise ValueError("Limiar ER deve ser fração finita entre0 e1; igualdade mantém a posição.")

    @property
    def enabled(self):
        return super().enabled or self.er_min_seconds > 0

    def decision(self, age_ms, net, er=None):
        prior = super().decision(age_ms, net)
        if prior.startswith("time_") or type(age_ms) is not int or age_ms < 0:
            return prior
        if (self.er_min_seconds and age_ms >= self.er_min_seconds * 1000
                and type(er) in (int, float) and math.isfinite(er) and 0 <= er <= 1
                and self.maximum_er - er > ER_EQUALITY_BAND):
            return "time_er"
        return prior


def observation_values(policy, data):
    """Mesma fórmula do registry; série causal, aquecimento de21 closes."""
    return efficiency_ratio(data["close"], ER_PERIOD) if isinstance(policy, EfficiencyTemporalPolicy) else None


def decide(policy, age_ms, net, values, index):
    if values is None:
        return policy.decision(age_ms, net), {}
    raw = float(values.iloc[index])
    value = raw if math.isfinite(raw) and 0 <= raw <= 1 else None
    return policy.decision(age_ms, net, er=value), {"er_feature": ER_FEATURE, "er_value": value}


def from_selection(selection):
    cls = EfficiencyTemporalPolicy if selection["mode"] == "temporal_er_v1" else TemporalPolicy
    return cls(**selection["params"])
