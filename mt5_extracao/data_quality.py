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

import numpy as np
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
    report = {key: {"count": 0, "examples": []} for key in
              ("invalid_ohlc", "nonfinite_ohlc", "invalid_time", "unordered_time", "invalid_volume",
               "zero_volume", "duplicate_time", "gaps")}
    report.update(rows=0, bars_per_day={"days": 0, "min": 0, "max": 0, "mean": 0.0})
    if df is None or df.empty:
        return report

    data = df.copy()
    # Mede a entrada antes de ordenar; não corrige nem remove observações da fonte.
    data["time"] = pd.to_datetime(data.get("time", pd.Series(pd.NaT, index=data.index)),
                                  errors="coerce", format="mixed")
    if not isinstance(data.time.dtype, pd.DatetimeTZDtype) and not pd.api.types.is_datetime64_dtype(data.time.dtype):
        # Misturar offsets ou horários com/sem fuso exige um contrato, nunca UTC presumido.
        data["time"] = pd.NaT
    report["rows"] = int(len(data))
    invalid_time = data.time.isna()
    report["invalid_time"] = {"count": int(invalid_time.sum()),
                              "examples": _examples(df.index[invalid_time])}
    valid_times = data.loc[~invalid_time, "time"]
    backwards = valid_times.diff() < pd.Timedelta(0)
    report["unordered_time"] = {"count": int(backwards.sum()), "examples": _examples(valid_times[backwards])}
    prices = data.reindex(columns=["open", "high", "low", "close"]).apply(pd.to_numeric, errors="coerce")
    nonfinite = ~np.isfinite(prices).fillna(False).all(axis=1)
    report["nonfinite_ohlc"] = {"count": int(nonfinite.sum()), "examples": _examples(data.loc[nonfinite, "time"])}

    # 1. OHLC inconsistente
    bad = (prices["high"] < prices["low"]) \
        | (prices["open"] > prices["high"]) | (prices["open"] < prices["low"]) \
        | (prices["close"] > prices["high"]) | (prices["close"] < prices["low"])
    report["invalid_ohlc"] = {"count": int(bad.sum()), "examples": _examples(data.loc[bad, "time"])}

    # 2. Volume zero
    if "tick_volume" in data.columns:
        zero = pd.to_numeric(data["tick_volume"], errors="coerce").eq(0)
        report["zero_volume"] = {"count": int(zero.sum()), "examples": _examples(data.loc[zero, "time"])}
    volumes = data[[col for col in ("tick_volume", "real_volume") if col in data]].apply(pd.to_numeric, errors="coerce")
    invalid_volume = (~np.isfinite(volumes).fillna(False) | (volumes < 0)).any(axis=1)
    report["invalid_volume"] = {"count": int(invalid_volume.sum()),
                                "examples": _examples(data.loc[invalid_volume, "time"])}

    # 3. Horários duplicados
    dup = data["time"].duplicated(keep="first") & ~invalid_time
    report["duplicate_time"] = {"count": int(dup.sum()), "examples": _examples(data.loc[dup, "time"])}

    # 4. Lacunas dentro do pregão (somente intradiário)
    unique = data.loc[~dup & ~invalid_time, "time"].sort_values().reset_index(drop=True)
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
    if not per_day.empty:
        report["bars_per_day"] = {"days": int(len(per_day)), "min": int(per_day.min()),
                                  "max": int(per_day.max()), "mean": round(float(per_day.mean()), 1)}
    return report


def summarize(report: dict) -> str:
    """Resumo de uma linha para log."""
    return (f"{report['rows']} barras; OHLC inválido: {report['invalid_ohlc']['count']}; "
            f"volume zero: {report['zero_volume']['count']}; duplicadas: {report['duplicate_time']['count']}; "
            f"lacunas no pregão: {report['gaps']['count']}; barras/dia: {report['bars_per_day']['min']}"
            f"–{report['bars_per_day']['max']}; OHLC ausente/não finito: {report.get('nonfinite_ohlc', {}).get('count', 0)}; "
            f"horários inválidos: {report.get('invalid_time', {}).get('count', 0)}; "
            f"fora de ordem: {report.get('unordered_time', {}).get('count', 0)}; "
            f"volume inválido: {report.get('invalid_volume', {}).get('count', 0)}")


def blocks_price_analysis(report: dict) -> bool:
    """Preços e cronologia inválidos impedem métricas; volume/gaps têm diagnóstico próprio."""
    return any(report.get(key, {}).get("count", 0) for key in
               ("invalid_ohlc", "nonfinite_ohlc", "invalid_time", "unordered_time", "duplicate_time"))


def has_problems(report: Optional[dict]) -> bool:
    if not report:
        return False
    return blocks_price_analysis(report) or any(report.get(k, {}).get("count", 0)
                                               for k in ("zero_volume", "invalid_volume", "gaps"))
