"""
Paridade Python x MQL5: a versão MQL5 de uma estratégia faz as mesmas operações que o protótipo em Python?

    report = compare(python_run["trades"], mql5_run["trades"], tolerance_minutes=60)
    report.ok            # >= 95% das operações casadas e diferença total de resultado <= 5%

As operações gravadas em `_robot_runs` são [horário de saída, resultado, volume]: a direção não é guardada, então
o casamento é pelo horário de saída, dentro de uma tolerância (por padrão, uma barra). O relatório mostra os
números; diferenças legítimas existem (modelo de ticks, spread por barra, horário da corretora) e quem decide é a
pessoa. O critério não deve ser afrouxado para um robô passar.
"""
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Optional

MIN_MATCHED_SHARE = 0.95
MAX_RESULT_DIFFERENCE = 0.05


@dataclass
class ParityReport:
    python_trades: int
    mql5_trades: int
    matched: int
    only_python: List[list] = field(default_factory=list)       # operações sem par do outro lado
    only_mql5: List[list] = field(default_factory=list)
    python_result: float = 0.0
    mql5_result: float = 0.0
    same_sign: int = 0                                           # pares em que os dois ganharam ou os dois perderam
    tolerance_minutes: float = 0.0

    @property
    def matched_share(self) -> float:
        total = max(self.python_trades, self.mql5_trades)
        return self.matched / total if total else 0.0

    @property
    def result_difference(self) -> Optional[float]:
        """Diferença do resultado total, relativa ao do Python (None se o Python deu zero)."""
        if not self.python_result:
            return None if self.mql5_result else 0.0
        return abs(self.mql5_result - self.python_result) / abs(self.python_result)

    @property
    def ok(self) -> bool:
        difference = self.result_difference
        return bool(self.matched) and self.matched_share >= MIN_MATCHED_SHARE and \
            difference is not None and difference <= MAX_RESULT_DIFFERENCE

    def to_dict(self) -> dict:
        return {"ok": self.ok, "python_trades": self.python_trades, "mql5_trades": self.mql5_trades,
                "matched": self.matched, "matched_share": self.matched_share, "same_sign": self.same_sign,
                "python_result": self.python_result, "mql5_result": self.mql5_result,
                "result_difference": self.result_difference, "tolerance_minutes": self.tolerance_minutes,
                "only_python": self.only_python, "only_mql5": self.only_mql5}


def _when(stamp) -> datetime:
    return datetime.strptime(str(stamp)[:19].replace("T", " "), "%Y-%m-%d %H:%M:%S")


def compare(python_trades: List[list], mql5_trades: List[list], tolerance_minutes: float = 60.0) -> ParityReport:
    """Casa as operações pelo horário de saída (cada uma no máximo uma vez, a mais próxima dentro da tolerância)."""
    tolerance = timedelta(minutes=tolerance_minutes)
    left = sorted(python_trades, key=lambda t: _when(t[0]))
    right = sorted(mql5_trades, key=lambda t: _when(t[0]))
    used = [False] * len(right)
    report = ParityReport(len(left), len(right), 0, tolerance_minutes=tolerance_minutes,
                          python_result=sum(float(t[1]) for t in left), mql5_result=sum(float(t[1]) for t in right))
    start = 0
    for trade in left:
        when = _when(trade[0])
        while start < len(right) and _when(right[start][0]) < when - tolerance:
            start += 1
        best, best_gap = None, None
        for j in range(start, len(right)):
            other = _when(right[j][0])
            if other > when + tolerance:
                break
            gap = abs(other - when)
            if not used[j] and (best_gap is None or gap < best_gap):
                best, best_gap = j, gap
        if best is None:
            report.only_python.append(list(trade))
            continue
        used[best] = True
        report.matched += 1
        if (float(trade[1]) > 0) == (float(right[best][1]) > 0):
            report.same_sign += 1
    report.only_mql5 = [list(t) for t, taken in zip(right, used) if not taken]
    return report


def run(db, config, robot, strategy_robot, symbol: str, timeframe: str, start=None, end=None,
        preset: Optional[str] = None, runner=None, is_running=None) -> dict:
    """
    Roda o backtest Python e o do Strategy Tester no mesmo período e compara. `robot` é o MQL5; `strategy_robot`,
    a estratégia Python do catálogo. Devolve {"report", "python_run", "mql5_run"}. O MT5 precisa estar fechado.
    """
    from mt5_extracao import timeframes
    from . import backtest
    tf = timeframes.parse(timeframe)
    if tf is None:
        raise ValueError(f"Timeframe inválido: {timeframe}")
    python_run = backtest.run_python(db, config, strategy_robot, symbol, timeframe, start, end)
    period_start = _when(python_run["data_start"])
    period_end = _when(python_run["data_end"]) + timedelta(minutes=tf.minutes)
    mql5_run = backtest.run_mql5(db, config, robot, symbol, timeframe, period_start, period_end, preset,
                                 run=runner, is_running=is_running)
    return {"report": compare(python_run["trades"], mql5_run["trades"], float(tf.minutes)),
            "python_run": python_run, "mql5_run": mql5_run}
