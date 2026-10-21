"""Relógios explícitos da trajetória (T19.14), sem inferência do fuso do host."""
from dataclasses import dataclass
from datetime import timedelta, timezone
import math
from typing import Optional
from numbers import Real

import pandas as pd

from mt5_extracao import timeframes


@dataclass(frozen=True)
class Clock:
    """Base declarada da fonte; offset broker fixo não comprova uma regra histórica de DST."""

    basis: str
    broker_utc_offset_hours: Optional[float] = None

    def __post_init__(self):
        offset = self.broker_utc_offset_hours
        if self.basis not in ("utc", "broker"):
            raise ValueError("Relógio desconhecido; declare utc ou broker com offset conhecido.")
        if self.basis == "utc":
            if offset is not None:
                raise ValueError("Relógio UTC não recebe offset da corretora.")
        elif isinstance(offset, bool) or not isinstance(offset, Real) or not math.isfinite(offset) or abs(offset) > 14:
            raise ValueError("Relógio broker exige offset UTC finito entre -14 e 14 horas.")

    @property
    def zone(self):
        return timezone.utc if self.basis == "utc" else timezone(timedelta(hours=float(self.broker_utc_offset_hours)))

    def timestamp(self, value):
        if value is None or isinstance(value, (bool, Real)):
            raise ValueError("Horário ausente ou sem unidade; informe timestamp da fonte.")
        try:
            stamp = pd.Timestamp(value)
            if pd.isna(stamp):
                raise ValueError("horário ausente")
            if stamp.tzinfo is None:
                stamp = stamp.tz_localize(self.zone)
            elif self.basis == "broker" and stamp.utcoffset() != self.zone.utcoffset(None):
                raise ValueError("offset do timestamp difere da declaração broker")
            return stamp.tz_convert("UTC")
        except (TypeError, ValueError, OverflowError) as error:
            raise ValueError("Horário inválido ou incompatível com o relógio declarado; confira a fonte.") from error


def _period(start, end, clock):
    begin, finish = clock.timestamp(start), clock.timestamp(end)
    if finish < begin:
        raise ValueError("Fim anterior à entrada; confira os horários da operação.")
    return begin, finish


def elapsed_minutes(start, end, clock):
    begin, finish = _period(start, end, clock)
    return (finish - begin).total_seconds() / 60


def bar_end_in_source(start, timeframe):
    """Fim nominal no relógio da fonte, sem atribuir UTC a timestamps sem fuso."""
    tf = timeframes.parse(timeframe)
    if tf is None:
        raise ValueError("Timeframe sem intervalo conhecido; declare a resolução da fonte.")
    if start is None or isinstance(start, (bool, Real)):
        raise ValueError("Horário ausente ou sem unidade; informe timestamp da fonte.")
    stamp = pd.Timestamp(start)
    if pd.isna(stamp):
        raise ValueError("Horário ausente; confira a barra da fonte.")
    return stamp + (pd.DateOffset(months=1) if tf == timeframes.Timeframe.MN1 else pd.Timedelta(minutes=tf.minutes))


def bar_intervals(starts, timeframe, clock):
    """Intervalos [abertura, fechamento), com mês de calendário no relógio da fonte."""
    tf = timeframes.parse(timeframe)
    if tf is None:
        raise ValueError("Timeframe sem intervalo conhecido; declare a resolução da fonte.")
    rows = []
    for value in starts:
        begin = clock.timestamp(value)
        finish = bar_end_in_source(begin.tz_convert(clock.zone), tf).tz_convert("UTC")
        rows.append((begin, finish))
    frame = pd.DataFrame(rows, columns=["start_utc", "end_utc"]).sort_values("start_utc").reset_index(drop=True)
    if len(frame) > 1 and (frame.start_utc.array[1:] < frame.end_utc.array[:-1]).any():
        raise ValueError("Barras duplicadas ou sobrepostas; confira a resolução e a revisão dos dados.")
    return frame


def complete_bars(start, end, intervals, clock):
    """Conta só barras disponíveis inteiramente contidas, sem preencher gaps com divisão de minutos."""
    begin, finish = _period(start, end, clock)
    if intervals.empty:
        return 0
    return int(((intervals.start_utc >= begin) & (intervals.end_utc <= finish)).sum())


def trading_minutes(start, end, sessions, clock, *, coverage=None):
    """Interseção com sessões UTC. Sem cobertura declarada retorna None, inclusive em gaps."""
    begin, finish = _period(start, end, clock)
    if begin == finish:
        return 0.0
    if sessions is None or coverage is None:
        return None
    utc = Clock("utc")
    first, last = _period(*coverage, utc)
    if begin < first or finish > last:
        return None
    intervals = []
    for row in sessions.itertuples(index=False):
        opened, closed = _period(row.open_utc, row.close_utc, utc)
        if closed == opened:
            raise ValueError("Sessão sem duração; confira o calendário da revisão.")
        left, right = max(begin, opened), min(finish, closed)
        if left < right:
            intervals.append((left, right))
    total, current = 0.0, None
    for left, right in sorted(intervals):
        if current is None:
            current = (left, right)
        elif left <= current[1]:
            current = (current[0], max(right, current[1]))
        else:
            total += (current[1] - current[0]).total_seconds() / 60
            current = (left, right)
    if current is not None:
        total += (current[1] - current[0]).total_seconds() / 60
    return total
