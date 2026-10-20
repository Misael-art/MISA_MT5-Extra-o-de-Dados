"""Espera somente após fechamento total conhecido; nunca por redução parcial."""
from dataclasses import dataclass


@dataclass(frozen=True)
class CooldownPolicy:
    seconds: int

    def __post_init__(self):
        if type(self.seconds) is not int or not 1 <= self.seconds <= 2147483647:
            raise ValueError("Cooldown exige segundos inteiros de1 a2147483647.")

    def decide(self, at_msc, last_close_msc):
        if type(at_msc) is not int or at_msc <= 0 or type(last_close_msc) is not int or not 0 <= last_close_msc <= at_msc:
            reason = "history_unavailable"
        else:
            reason = "cooldown" if last_close_msc and at_msc-last_close_msc < self.seconds*1000 else "allowed"
        return dict(at_msc=at_msc, last_close_msc=last_close_msc, seconds=self.seconds,
                    allowed=reason == "allowed", reason=reason)


def from_filters(filters):
    selected = [row for row in filters if row["mode"] == "cooldown_v1"]
    if len(selected) > 1:
        raise ValueError("Use apenas um filtro cooldown_v1.")
    return CooldownPolicy(**selected[0]["params"]) if selected else None
