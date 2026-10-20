"""T19.20 inicial: entradas no relógio da fonte, sessão ancorada e ciclos preenchidos.

Não deduz fuso, DST, feriados ou abertura futura de mercado. O relógio broker
declarado é o mesmo do calendário observado; a sessão diária usa sua âncora.
"""
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import calendar


def source_seconds(value):
    if hasattr(value, "to_pydatetime"):
        value = value.to_pydatetime()
    if not isinstance(value, datetime) or value.tzinfo is not None:
        raise ValueError("Sessão exige horário da corretora sem fuso embutido; não presumir conversão UTC/DST.")
    return calendar.timegm(value.timetuple())


@dataclass(frozen=True)
class SessionEntryPolicy:
    start_minute: int = 0
    end_minute: int = 0
    exclude_start_minute: int = 0
    exclude_end_minute: int = 0
    weekdays: int = 127
    anchor_minute: int = 0
    max_cycles: int = 0

    def __post_init__(self):
        for key, value in asdict(self).items():
            maximum = 127 if key == "weekdays" else 1000000 if key == "max_cycles" else 1439
            minimum = 1 if key == "weekdays" else 0
            if type(value) is not int or not minimum <= value <= maximum:
                raise ValueError(f"{key}: informe inteiro de {minimum} a {maximum}.")

    def session_start(self, at):
        if type(at) is not int or at < 86400:
            raise ValueError("Relógio da sessão indisponível; não reiniciar contador por ausência.")
        return ((at - self.anchor_minute * 60) // 86400) * 86400 + self.anchor_minute * 60

    def decide(self, at, cycles):
        session = self.session_start(at)
        weekday = datetime.fromtimestamp(session, timezone.utc).weekday()
        minute = (at % 86400) // 60
        if type(cycles) is not int or cycles < 0:
            reason = "history_unavailable"
        elif not self.weekdays & (1 << weekday):
            reason = "weekday"
        elif not inside(minute, self.start_minute, self.end_minute):
            reason = "outside_window"
        elif self.exclude_start_minute != self.exclude_end_minute and inside(
                minute, self.exclude_start_minute, self.exclude_end_minute):
            reason = "excluded_window"
        elif self.max_cycles and cycles >= self.max_cycles:
            reason = "quota"
        else:
            reason = "allowed"
        return dict(session_start=session, weekday=weekday, minute=minute,
                    cycles=cycles, allowed=reason == "allowed", reason=reason)

    def to_dict(self):
        return asdict(self)


def inside(minute, start, end):
    # [início, fim); igualdade permite 24h na janela, desabilita a exclusão.
    return True if start == end else start <= minute < end if start < end else minute >= start or minute < end


def from_filters(filters):
    if not filters:
        return None
    if len(filters) != 1 or filters[0]["mode"] != "session_v1":
        raise ValueError("Este adaptador suporta somente o filtro session_v1; outros filtros mantêm seus gates.")
    return SessionEntryPolicy(**filters[0]["params"])
