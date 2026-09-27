"""
Timeframes do MetaTrader 5: fonte única de valores, nomes legíveis e minutos por barra.

Permite usar os timeframes sem importar o pacote MetaTrader5 (que só existe no
Windows). Os nomes legíveis ("1 minuto", "1 hora"...) compõem os nomes das
tabelas no banco: NÃO os altere.
"""
import unicodedata
from enum import Enum
from typing import Optional


class Timeframe(Enum):
    """Membro = valor oficial da constante TIMEFRAME_* do MT5; label = nome legível; minutes = minutos por barra."""

    def __new__(cls, mt5_value, label, minutes):
        obj = object.__new__(cls)
        obj._value_ = mt5_value
        obj.label = label
        obj.minutes = minutes
        return obj

    M1 = (1, "1 minuto", 1)
    M2 = (2, "2 minutos", 2)
    M3 = (3, "3 minutos", 3)
    M4 = (4, "4 minutos", 4)
    M5 = (5, "5 minutos", 5)
    M6 = (6, "6 minutos", 6)
    M10 = (10, "10 minutos", 10)
    M12 = (12, "12 minutos", 12)
    M15 = (15, "15 minutos", 15)
    M20 = (20, "20 minutos", 20)
    M30 = (30, "30 minutos", 30)
    H1 = (16385, "1 hora", 60)
    H2 = (16386, "2 horas", 120)
    H3 = (16387, "3 horas", 180)
    H4 = (16388, "4 horas", 240)
    H6 = (16390, "6 horas", 360)
    H8 = (16392, "8 horas", 480)
    H12 = (16396, "12 horas", 720)
    D1 = (16408, "1 dia", 1440)
    W1 = (32769, "1 semana", 10080)
    MN1 = (49153, "1 mês", 43200)

    @property
    def mt5(self) -> int:
        """Valor inteiro a passar para as funções do MetaTrader5."""
        return self.value


# Timeframes oferecidos na interface (mesma lista e ordem de antes)
MAIN = (Timeframe.M1, Timeframe.M5, Timeframe.M15, Timeframe.M30, Timeframe.H1,
        Timeframe.H4, Timeframe.D1, Timeframe.W1, Timeframe.MN1)

# Constantes no formato do pacote MetaTrader5 (TIMEFRAME_M1 = 1, ...)
for _tf in Timeframe:
    globals()[f"TIMEFRAME_{_tf.name}"] = _tf.value

VALUES = frozenset(tf.value for tf in Timeframe)
MINUTES = {tf.value: tf.minutes for tf in Timeframe}


def _norm(text: str) -> str:
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    return text.lower().replace(" ", "").replace("_", "")


_ALIASES = {}
for _tf in Timeframe:
    _ALIASES[_norm(_tf.name)] = _tf               # m1, h1, mn1...
    _ALIASES[_norm("timeframe" + _tf.name)] = _tf  # timeframem1
    _ALIASES[_norm(_tf.label)] = _tf              # 1minuto, 1hora, 1mes...
_ALIASES.update({
    "1m": Timeframe.M1, "5m": Timeframe.M5, "15m": Timeframe.M15, "30m": Timeframe.M30,
    "1min": Timeframe.M1, "5min": Timeframe.M5, "15min": Timeframe.M15, "30min": Timeframe.M30,
    "minuto": Timeframe.M1, "h": Timeframe.H1, "hora": Timeframe.H1, "1h": Timeframe.H1,
    "4h": Timeframe.H4, "4hour": Timeframe.H4, "4horas": Timeframe.H4,
    "d": Timeframe.D1, "day": Timeframe.D1, "dia": Timeframe.D1, "diario": Timeframe.D1,
    "w": Timeframe.W1, "week": Timeframe.W1, "semana": Timeframe.W1, "semanal": Timeframe.W1,
    "month": Timeframe.MN1, "mes": Timeframe.MN1, "mensal": Timeframe.MN1,
})

# Minutos -> timeframe (para valores antigos gravados em minutos)
_BY_MINUTES = {tf.minutes: tf for tf in reversed(list(Timeframe))}
for _tf in MAIN:
    _BY_MINUTES[_tf.minutes] = _tf


def parse(value) -> Optional[Timeframe]:
    """
    Converte Timeframe, valor do MT5 (16385), minutos (60) ou texto ("M1", "1min", "1 hora", "H4")
    para Timeframe. Retorna None se não reconhecer.
    """
    if isinstance(value, Timeframe):
        return value
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        if value in VALUES:
            return Timeframe(value)
        return _BY_MINUTES.get(value)
    if isinstance(value, str):
        key = _norm(value)
        if key in _ALIASES:
            return _ALIASES[key]
        if key.isdigit():
            return parse(int(key))
    return None
