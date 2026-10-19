"""Registro observacional dos motores: não calcula preços, custos ou preenchimentos."""
import copy
import math

import pandas as pd

from mt5_extracao import timeframes
from .clocks import bar_end_in_source


class Journal:
    """Um ciclo explícito por posição do motor, da entrada à exposição zero.

    A sequência é a do modelo conservador, não uma reconstrução dos ticks reais.
    Os tempos permanecem no relógio da fonte até a ligação à revisão dos dados.
    """

    def __init__(self, times, timeframe, source):
        tf = timeframes.parse(timeframe)
        if tf is None:
            raise ValueError("Observações exigem timeframe conhecido, sem inferir gaps como duração da barra.")
        self.times = [pd.Timestamp(t) for t in times]
        self.ends = [bar_end_in_source(t, tf) for t in self.times]
        self.timeframe, self.source = tf.name, source
        self.records, self._sequences = [], {}

    def record(self, cycle_seq, i, kind, phase, volume_open, realized_net, *, net_pnl=None,
               source_seq=None, fill_price=None, fill_volume=None, direction=None):
        if phase not in ("open", "intrabar", "close"):
            raise ValueError("Fase temporal desconhecida na observação do motor.")
        start = self.ends[i] if phase == "close" else self.times[i]
        end = self.times[i] if phase == "open" else self.ends[i]
        def number(value):
            if value is None:
                return None
            result = float(value)
            if not math.isfinite(result):
                raise ValueError("Estado não finito no registro da trajetória; confira os dados do modelo.")
            return result
        sequence = self._sequences.get(cycle_seq, 0)
        self._sequences[cycle_seq] = sequence + 1
        self.records.append({"cycle_seq": int(cycle_seq), "event_seq": sequence,
                             "direction": int(direction) if direction in (1, -1) else None,
                             "source_seq": int(source_seq) if source_seq is not None and pd.notna(source_seq) else None,
                             "bar_index": i, "bar_time": self.times[i].isoformat(), "kind": kind, "phase": phase,
                             "start_time": start.isoformat(), "end_time": end.isoformat(),
                             "available_at": end.isoformat(), "volume_open": number(volume_open),
                             "realized_net": number(realized_net), "net_pnl": number(net_pnl),
                             "fill_price": number(fill_price), "fill_volume": number(fill_volume)})

    def payload(self, valuation):
        return {"schema_version": 1, "source": self.source, "timeframe": self.timeframe,
                "timestamp_basis": "source_clock", "sequence_basis": "modeled_execution_order",
                "observation_unit": "cycle_from_first_fill_to_flat", "currency": None,
                "valuation": copy.deepcopy(valuation), "records": copy.deepcopy(self.records)}
