"""
Paridade Python x MQL5: a versão MQL5 de uma estratégia faz as mesmas operações que o protótipo em Python?

    report = compare(python_run["trades"], mql5_run["trades"], tolerance_minutes=60)
    report.ok            # >= 95% casadas, >= 95% dos pares a até 5%, diferença total <= 1% do giro bruto
                         # e mediana da razão entre os pares a até 2% de 1

As operações gravadas em `_robot_runs` são [horário de saída, resultado, volume]: a direção não é guardada, então
o casamento é pelo horário de saída, dentro de uma tolerância (por padrão, uma barra). O relatório mostra os
números; diferenças legítimas existem (modelo de ticks, spread por barra, horário da corretora) e quem decide é a
pessoa. O critério não deve ser afrouxado para um robô passar.
"""
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Optional

# Critério (decisão do dono do projeto em 2026-10-12, depois da medição registrada em docs/sdk/exatidao.md):
# as três condições precisam valer. A diferença sobre o resultado LÍQUIDO continua no relatório, mas deixou de
# ser critério: ela fica instável quando o líquido é pequeno perto do giro.
MIN_MATCHED_SHARE = 0.95          # operações casadas pelo horário de saída
PAIR_TOLERANCE = 0.05             # um par é "próximo" se os resultados diferem até 5% do resultado do Python
MIN_PAIRS_CLOSE_SHARE = 0.95      # pares próximos, entre os casados
MAX_DIFFERENCE_OF_GROSS = 0.01    # diferença do resultado total, sobre o giro bruto do Python
MAX_MEDIAN_BIAS = 0.02            # desvio da mediana da razão MQL5/Python entre os pares: pega um viés sistemático
                                  # (ex.: lote sempre 4% menor), que passaria nas outras duas medidas
PARITY_TESTER_MODEL = "0"         # "cada tick": a paridade compara a LÓGICA; o custo real é outra medida


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
    warmup_until: str = ""                                       # operações encerradas antes disso ficam de fora
    excluded_warmup: int = 0
    pairs_close: int = 0             # pares cujo resultado difere no máximo PAIR_TOLERANCE do resultado do Python
    median_ratio: float = 1.0        # mediana de resultado MQL5 / resultado Python entre os pares
    gross: float = 0.0               # giro bruto do Python: soma dos resultados em valor absoluto

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
    def pairs_close_share(self) -> float:
        return self.pairs_close / self.matched if self.matched else 0.0

    @property
    def difference_of_gross(self) -> float:
        """Diferença do resultado total em relação ao giro bruto: não explode quando o resultado líquido é pequeno."""
        return abs(self.mql5_result - self.python_result) / self.gross if self.gross else 0.0

    @property
    def ok(self) -> bool:
        return bool(self.matched) and self.matched_share >= MIN_MATCHED_SHARE and \
            self.pairs_close_share >= MIN_PAIRS_CLOSE_SHARE and \
            self.difference_of_gross <= MAX_DIFFERENCE_OF_GROSS and abs(self.median_ratio - 1.0) <= MAX_MEDIAN_BIAS

    def to_dict(self) -> dict:
        return {"ok": self.ok, "python_trades": self.python_trades, "mql5_trades": self.mql5_trades,
                "matched": self.matched, "matched_share": self.matched_share, "same_sign": self.same_sign,
                "python_result": self.python_result, "mql5_result": self.mql5_result,
                "result_difference": self.result_difference, "tolerance_minutes": self.tolerance_minutes,
                "warmup_until": self.warmup_until, "excluded_warmup": self.excluded_warmup,
                "pairs_close": self.pairs_close, "pairs_close_share": self.pairs_close_share,
                "difference_of_gross": self.difference_of_gross, "median_ratio": self.median_ratio,
                "only_python": self.only_python, "only_mql5": self.only_mql5}


def _when(stamp) -> datetime:
    return datetime.strptime(str(stamp)[:19].replace("T", " "), "%Y-%m-%d %H:%M:%S")


def compare(python_trades: List[list], mql5_trades: List[list], tolerance_minutes: float = 60.0,
            warmup_until=None) -> ParityReport:
    """
    Casa as operações pelo horário de saída (cada uma no máximo uma vez, a mais próxima dentro da tolerância).
    `warmup_until`: operações encerradas antes desse horário ficam de fora dos dois lados. Serve para o início do
    período, em que o Python ainda não tem barras para os indicadores e o Strategy Tester já tem histórico.
    """
    excluded = 0
    if warmup_until is not None:
        cutoff = _when(warmup_until)
        kept = [[t for t in trades if _when(t[0]) >= cutoff] for trades in (python_trades, mql5_trades)]
        excluded = len(python_trades) + len(mql5_trades) - len(kept[0]) - len(kept[1])
        python_trades, mql5_trades = kept
    tolerance = timedelta(minutes=tolerance_minutes)
    left = sorted(python_trades, key=lambda t: _when(t[0]))
    right = sorted(mql5_trades, key=lambda t: _when(t[0]))
    used = [False] * len(right)
    report = ParityReport(len(left), len(right), 0, tolerance_minutes=tolerance_minutes,
                          python_result=sum(float(t[1]) for t in left), mql5_result=sum(float(t[1]) for t in right))
    start, ratios = 0, []
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
        if abs(float(right[best][1]) - float(trade[1])) <= PAIR_TOLERANCE * abs(float(trade[1])):
            report.pairs_close += 1
        if float(trade[1]):
            ratios.append(float(right[best][1]) / float(trade[1]))
    if ratios:
        ratios.sort()
        middle = len(ratios) // 2
        report.median_ratio = ratios[middle] if len(ratios) % 2 else (ratios[middle - 1] + ratios[middle]) / 2
    report.gross = sum(abs(float(t[1])) for t in left)
    report.only_mql5 = [list(t) for t, taken in zip(right, used) if not taken]
    report.warmup_until, report.excluded_warmup = (str(warmup_until) if warmup_until is not None else ""), excluded
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
    # Como em run_declared: os dois lados começam no mesmo instante, sem posição e com o mesmo capital, e o robô usa
    # o risco por operação do backtester Python, com os limites de perda abertos. Sem isso a comparação mede a
    # diferença de configuração, não a de lógica.
    from mt5_extracao.strategies import get_strategy, research
    strategy = get_strategy(strategy_robot.key.split(":", 1)[1])
    bars = db.load_ohlcv(symbol, tf.label, start, end)
    warmup = int(strategy.min_bars)
    if len(bars) <= warmup + 1:
        raise ValueError(research.missing_message([symbol], tf) if bars.empty else
                         f"Poucas barras de {symbol} para comparar: {len(bars)} (o aquecimento usa {warmup}).")
    warm_end = _when(str(bars["time"].iloc[warmup]))
    trade_from = (warm_end + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    python_run = backtest.run_python(db, config, strategy_robot, symbol, timeframe, start, end,
                                     source=backtest.PARITY_SOURCE, trade_from=trade_from, real_spread=False)
    period_end = _when(python_run["data_end"]) + timedelta(minutes=tf.minutes)
    mql5_run = backtest.run_mql5(db, config, robot, symbol, timeframe, trade_from, period_end, preset,
                                 params=parity_params(robot, config), run=runner, is_running=is_running,
                                 source=backtest.DEEP_SOURCE, model=PARITY_TESTER_MODEL)
    report = compare(python_run["trades"], mql5_run["trades"], float(tf.minutes))
    report.warmup_until = trade_from.strftime("%Y-%m-%d %H:%M:%S")      # início comum da comparação
    return {"report": report, "python_run": python_run, "mql5_run": mql5_run}


def parity_params(robot, config) -> dict:
    """
    Parâmetros do robô para a comparação valer: o mesmo risco por operação do backtester Python e os limites de
    perda abertos (o Python não tem limite diário nem de drawdown; com eles ligados o robô deixaria de operar em
    dias em que o Python opera). Vale só para o teste de paridade, no Strategy Tester.
    """
    from mt5_extracao.strategies import research
    from . import contract
    known = {i.name for i in robot.inputs}
    params = {}
    risk_input = contract.input_name(robot, "risk_pct")
    if risk_input:
        params[risk_input] = f"{research.backtest_config(config).risk_per_trade * 100:g}"
    for name in (contract.input_name(robot, "daily_loss_pct"), contract.input_name(robot, "max_drawdown_pct"),
                 "InpWeeklyLossPct"):
        if name and name in known:
            params[name] = "100"
    return params


def run_declared(db, config, robot, symbol: str, timeframe: str, start=None, end=None, runner=None,
                 is_running=None) -> dict:
    """
    Paridade de um robô gerado por `mt5x robots generate` com a própria declaração, executada em Python.
    Devolve {"report", "python_run", "mql5_run"}. O MT5 precisa estar fechado.
    """
    from mt5_extracao import timeframes
    from mt5_extracao.strategies.declared import DeclaredStrategy
    from . import backtest, generate
    tf = timeframes.parse(timeframe)
    if tf is None:
        raise ValueError(f"Timeframe inválido: {timeframe}")
    spec = generate.load_spec(robot)
    strategy = DeclaredStrategy(spec)
    # Os dois lados precisam começar no mesmo instante, sem posição e com o mesmo capital. O Strategy Tester começa
    # em FromDate (um dia inteiro) já com histórico para os indicadores; o Python precisa de barras antes para
    # aquecê-los. Por isso a comparação começa na primeira barra do dia seguinte ao fim do aquecimento.
    bars = db.load_ohlcv(symbol, tf.label, start, end)
    warmup = max([item.period for item in spec.indicators.values()] + [spec.atr_period]) + 1
    if len(bars) <= warmup + 1:
        from mt5_extracao.strategies import research
        raise ValueError(research.missing_message([symbol], tf) if bars.empty else
                         f"Poucas barras de {symbol} para comparar: {len(bars)} (o aquecimento usa {warmup}).")
    warm_end = _when(str(bars["time"].iloc[warmup]))
    trade_from = (warm_end + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    python_run = backtest.run_python(db, config, robot, symbol, timeframe, start, end, strategy=strategy,
                                     source=backtest.PARITY_SOURCE, trade_from=trade_from, real_spread=False)
    period_end = _when(python_run["data_end"]) + timedelta(minutes=tf.minutes)
    mql5_run = backtest.run_mql5(db, config, robot, symbol, timeframe, trade_from, period_end,
                                 params=parity_params(robot, config), run=runner, is_running=is_running,
                                 source=backtest.DEEP_SOURCE, model=PARITY_TESTER_MODEL)
    report = compare(python_run["trades"], mql5_run["trades"], float(tf.minutes))
    report.warmup_until = trade_from.strftime("%Y-%m-%d %H:%M:%S")      # início comum da comparação
    return {"report": report, "python_run": python_run, "mql5_run": mql5_run}
