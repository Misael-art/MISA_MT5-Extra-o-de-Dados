"""
Verificações de qualidade de séries OHLCV (funções puras, sem MT5 nem banco).

    report = check_quality(df, bar_minutes=1, session_start="09:00", session_end="18:30")

Verificações:
  invalid_ohlc   high < low, ou open/close fora de [low, high]
  zero_volume    tick_volume == 0
  duplicate_time mais de uma barra com o mesmo horário
  gaps           intervalo maior que 1 barra dentro do pregão (só timeframes intradiários;
                 fins de semana e horários fora do pregão são ignorados)
  bars_per_day   barras por dia (mínimo, máximo e média)
"""
from datetime import time as _time
from typing import Optional

import pandas as pd

MAX_EXAMPLES = 20


def _parse_hhmm(text: str) -> _time:
    hh, mm = text.strip().split(":")
    return _time(int(hh), int(mm))


def _examples(times) -> list:
    return [str(t) for t in list(times)[:MAX_EXAMPLES]]


def check_quality(df: pd.DataFrame, bar_minutes: int = 1, session_start: str = "09:00",
                  session_end: str = "18:30") -> dict:
    """Retorna um dicionário com contagens e até 20 exemplos de cada problema."""
    report = {"rows": 0, "invalid_ohlc": {"count": 0, "examples": []},
              "zero_volume": {"count": 0, "examples": []},
              "duplicate_time": {"count": 0, "examples": []},
              "gaps": {"count": 0, "examples": []},
              "bars_per_day": {"days": 0, "min": 0, "max": 0, "mean": 0.0}}
    if df is None or df.empty:
        return report

    data = df.copy()
    data["time"] = pd.to_datetime(data["time"])
    data = data.sort_values("time").reset_index(drop=True)
    report["rows"] = int(len(data))

    # 1. OHLC inconsistente
    bad = (data["high"] < data["low"]) \
        | (data["open"] > data["high"]) | (data["open"] < data["low"]) \
        | (data["close"] > data["high"]) | (data["close"] < data["low"])
    report["invalid_ohlc"] = {"count": int(bad.sum()), "examples": _examples(data.loc[bad, "time"])}

    # 2. Volume zero
    if "tick_volume" in data.columns:
        zero = data["tick_volume"].fillna(0) == 0
        report["zero_volume"] = {"count": int(zero.sum()), "examples": _examples(data.loc[zero, "time"])}

    # 3. Horários duplicados
    dup = data["time"].duplicated(keep="first")
    report["duplicate_time"] = {"count": int(dup.sum()), "examples": _examples(data.loc[dup, "time"])}

    # 4. Lacunas dentro do pregão (somente intradiário)
    unique = data.loc[~dup, "time"].reset_index(drop=True)
    if bar_minutes < 1440 and len(unique) > 1:
        start, end = _parse_hhmm(session_start), _parse_hhmm(session_end)
        step = pd.Timedelta(minutes=bar_minutes)
        prev, cur = unique.iloc[:-1].reset_index(drop=True), unique.iloc[1:].reset_index(drop=True)
        same_day = prev.dt.date.values == cur.dt.date.values
        weekday = cur.dt.weekday < 5
        in_session = (prev.dt.time >= start) & (cur.dt.time <= end)
        gap = same_day & weekday.values & in_session.values & ((cur - prev) > step).values
        gap_starts = prev[gap]
        report["gaps"] = {"count": int(gap.sum()),
                          "examples": [f"{a} -> {b}" for a, b in
                                       list(zip(gap_starts, cur[gap]))[:MAX_EXAMPLES]]}

    # 5. Barras por dia
    per_day = unique.dt.date.value_counts()
    report["bars_per_day"] = {"days": int(len(per_day)), "min": int(per_day.min()),
                              "max": int(per_day.max()), "mean": round(float(per_day.mean()), 1)}
    return report


def summarize(report: dict) -> str:
    """Resumo de uma linha para log."""
    return (f"{report['rows']} barras; OHLC inválido: {report['invalid_ohlc']['count']}; "
            f"volume zero: {report['zero_volume']['count']}; duplicadas: {report['duplicate_time']['count']}; "
            f"lacunas no pregão: {report['gaps']['count']}; barras/dia: {report['bars_per_day']['min']}"
            f"–{report['bars_per_day']['max']}")


def has_problems(report: Optional[dict]) -> bool:
    if not report:
        return False
    return any(report[k]["count"] for k in ("invalid_ohlc", "zero_volume", "duplicate_time", "gaps"))
