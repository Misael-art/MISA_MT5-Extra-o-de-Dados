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


# --- Reconciliação detalhada (decisão → posição → saída), um para um --------------------------------------
# Complementa compare(): em vez de casar só o resultado pelo horário de saída, casa cada operação pela
# entrada (minuto, direção), sem sobrescrever repetições, e devolve uma linha de divergência por operação.

NATIVE_REASONS = {"sl": "stop", "tp": "alvo"}           # rótulos do HDK_TradeExport → rótulos do backtester


def read_trade_export(path) -> List[dict]:
    """Todas as colunas do CSV de operações do HADOUKEN (separador ';'), sem descartar campos."""
    from . import mql
    lines = [line for line in mql.read_text(path).splitlines() if line.strip()]
    if not lines:
        return []
    header = [h.strip().lower() for h in lines[0].split(";")]
    if "entry_time" not in header or "time" not in header:
        raise ValueError("Exportação sem entry_time/time: não dá para reconciliar por entrada.")
    return [dict(zip(header, (c.strip() for c in line.split(";")))) for line in lines[1:]]


def parse_decision_lines(text: str, prefix: str = "SAREF_DECISION") -> List[dict]:
    """Linhas de decisão impressas pelo Expert (chave=valor), na ordem do log."""
    out = []
    for line in text.splitlines():
        at = line.find(prefix + " ")
        if at < 0:
            continue
        fields = dict(part.split("=", 1) for part in line[at + len(prefix) + 1:].split() if "=" in part)
        stamp = line[at + len(prefix) + 1:].split(" side=")[0].split("decision=")[-1]
        fields["decision"] = stamp
        out.append(fields)
    return out


def parse_close_lines(text: str) -> dict:
    """Motivos de volume executado, por ticket; saldo final exige CSV/histórico.

    Preserva o log legado das referências e lê a mensagem explícita do executor
    atual. Aceitação, parcial do broker e recusa não são execução confirmada.
    """
    import re
    closes = {}
    pattern = (r"Posição #(\d+) fechada \(([^)]*)\)|"
               r"(?:Redução|Volume de fechamento) #(\d+) executado: [0-9]+\.[0-9]{8} lote\(s\) \(([^)]*)\); conferir saldo")
    for match in re.finditer(pattern, text):
        ticket, reason = (match.group(1), match.group(2)) if match.group(1) else (match.group(3), match.group(4))
        closes.setdefault(ticket, []).append(reason)
    return closes


HOST_REASONS = {"fim do dia": "fim do dia", "saída por sinal": "sinal", "tempo máximo": "tempo"}


def _minute(stamp: str) -> datetime:
    text = str(stamp).strip().replace(".", "-", 2).replace("T", " ")
    return datetime.strptime(text[:16], "%Y-%m-%d %H:%M")


def _num(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# Tolerâncias explícitas por unidade; nunca aumentadas para aprovar. Ausente/inválido/não finito ≠ igual.
TOLERANCES = {"price": 5e-6, "volume": 1e-9, "money": 0.005}
FIELD_GROUPS = {
    "protection": (("sl", "price"), ("tp", "price")),
    "execution": (("entry_price", "price"), ("exit_price", "price"), ("volume", "volume")),
    "financial": (("result", "money"), ("commission", "money"), ("swap", "money"), ("fee", "money")),
}


def _finite(value):
    import math
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def parse_strategy_events(text: str) -> dict:
    """Decisões SAREF e sinais do host do log agregado, independente da ordem das origens.

    Cada identidade de evento (tipo + campos) é contada por origem. Cópias da mesma linha em origens
    distintas (log do agente e do Tester) colapsam; repetição dentro de uma origem é preservada: o número
    final de ocorrências é o MÁXIMO entre origens, e as contagens por origem ficam em `_sources`.
    Eventos com campos diferentes nunca se fundem, então conflitos continuam visíveis como ambiguidade.
    """
    import re
    from collections import Counter
    source = "log"
    per_source = {}                     # identidade -> Counter(origem -> ocorrências)
    first = {}
    for line in text.splitlines():
        header = re.match(r"===== (.+) =====", line.strip())
        if header:
            source = header.group(1)
            continue
        if "SAREF_DECISION " in line:
            body = line[line.index("SAREF_DECISION ") + 15:]
            fields = dict(p.split("=", 1) for p in body.split() if "=" in p)
            fields["decision"] = body.split(" side=")[0].split("decision=")[-1]
            kind = "decision"
        elif "SAREF_BLOCKED " in line:
            body = line[line.index("SAREF_BLOCKED ") + 14:]
            fields = dict(p.split("=", 1) for p in body.split() if "=" in p)
            fields["decision"] = body.split(" reason=")[0].split("decision=")[-1]
            kind = "blocked"
        elif "HDK_STRATEGY_SIGNAL " in line:
            body = line[line.index("HDK_STRATEGY_SIGNAL ") + 20:]
            fields = dict(p.split("=", 1) for p in body.split() if "=" in p)
            if " reason=" in body:
                fields["reason"] = body.split(" reason=", 1)[1].strip()
            kind = "signal"
        else:
            refusal = re.search(r"HDK#(\d+)\s+\w+\s+(COMPRA|VENDA) recusada: (.+)$", line)
            if not refusal:
                continue
            stamp = re.search(r"\d{4}\.\d\d\.\d\d \d\d:\d\d:\d\d", line)
            fields = dict(decision=stamp[0][:16] if stamp else "", magic=refusal[1],
                          side="1" if refusal[2] == "COMPRA" else "-1", reason=refusal[3].strip())
            kind = "refusal"
        identity = (kind, tuple(sorted(fields.items())))
        per_source.setdefault(identity, Counter())[source] += 1
        first.setdefault(identity, fields)
    events = {"decision": [], "signal": [], "blocked": [], "refusal": []}
    for identity in sorted(per_source, key=lambda i: (i[0], i[1])):
        counts = per_source[identity]
        for _ in range(max(counts.values())):
            events[identity[0]].append(dict(first[identity], _sources=dict(sorted(counts.items()))))
    return events


def _link(entry_minute, direction, position_id, events):
    """Cadeia decisão → sinal do host (order) → posição. Tempo sozinho é só candidato."""
    from datetime import timezone
    decisions = [d for d in events.get("decision", []) if _minute(d["decision"]) == entry_minute]
    compatible = [d for d in decisions if d.get("side") == str(direction) and d.get("eligible") == "1"]
    if not decisions:
        return "missing", None, "sem decisão no minuto de entrada"
    if not compatible:
        return "incompatible", decisions[0], "decisão do minuto é de outro lado ou inelegível"
    if len(compatible) > 1:
        return "ambiguous", None, "mais de uma decisão compatível no mesmo minuto e origem"
    decision = compatible[0]
    bar_start = int(entry_minute.replace(tzinfo=timezone.utc).timestamp()) - 300
    signals = [s for s in events.get("signal", []) if s.get("event_at") == str(bar_start)
               and s.get("direction") == str(direction) and s.get("opened") == "1"]
    if len(signals) == 1 and position_id and signals[0].get("order") == str(position_id):
        return "confirmed", decision, "decisão, sinal do host (order) e position_id coincidem"
    if len(signals) > 1:
        return "ambiguous", decision, "mais de um sinal aberto do host para a mesma barra"
    return "candidate_temporal", decision, "só coincidência de minuto/lado; ordem/posição não provadas"


def reconcile_trades(python_trades: List[dict], native_rows: List[dict], native_decisions: Optional[List[dict]] = None,
                     *, host_closes: Optional[dict] = None, events: Optional[dict] = None,
                     python_costs: Optional[dict] = None, tolerances: Optional[dict] = None) -> dict:
    """Um para um por (minuto de entrada, direção), sem sobrescrever colisões.

    Compara sinal, proteção, execução e resultado financeiro em grupos separados, com unidade e tolerância
    declaradas. `python_costs` só informa custos conhecidos do cenário (ex.: zero); sem ele, custos do Python
    são ausentes e o grupo financeiro não fecha. `events` (parse_strategy_events) prova o vínculo causal.
    """
    tol = dict(TOLERANCES, **(tolerances or {}))
    if events is None:
        events = {"decision": list(native_decisions or []), "signal": []}

    def key_python(t):
        return _minute(str(t["entry_time"])), 1 if t["direction"] > 0 else -1

    def key_native(r):
        return _minute(r["entry_time"]), 1 if r["direction"].lower() in ("buy", "long", "1") else -1

    groups_p, groups_n = {}, {}
    for t in python_trades:
        groups_p.setdefault(key_python(t), []).append(t)
    for r in native_rows:
        groups_n.setdefault(key_native(r), []).append(r)
    collisions = sorted({str(k) for k, v in list(groups_p.items()) + list(groups_n.items()) if len(v) > 1})
    rows, only_python, only_native = [], [], []
    for key in sorted(set(groups_p) | set(groups_n)):
        left, right = groups_p.get(key, []), groups_n.get(key, [])
        for i in range(max(len(left), len(right))):
            p = left[i] if i < len(left) else None
            n = right[i] if i < len(right) else None
            if p is None or n is None:
                side = only_native if p is None else only_python
                link = _link(key[0], key[1], n.get("position_id"), events) if n is not None else None
                side.append(dict(entry=str(key[0]), direction=key[1], reason=(n or p).get("exit_reason"),
                                 position_id=(n or {}).get("position_id"), link=link[0] if link else None))
                continue
            python_values = dict(sl=p.get("stop_price"), tp=p.get("target_price"), entry_price=p.get("entry_price"),
                                 exit_price=p.get("exit_price"), volume=p.get("volume"), result=p.get("pnl"),
                                 **{k: (python_costs or {}).get(k) for k in ("commission", "swap", "fee")})
            comparisons, flags = {}, []
            for group, fields in FIELD_GROUPS.items():
                for field, unit in fields:
                    a, b = _finite(n.get(field)), _finite(python_values.get(field))
                    if a is None or b is None:
                        state = "missing_native" if a is None else "missing_python"
                    else:
                        state = "equal" if abs(a - b) <= tol[unit] else "different"
                    comparisons[field] = dict(group=group, unit=unit, tolerance=tol[unit], native=a, python=b,
                                              diff=None if a is None or b is None else a - b, state=state)
                    if state != "equal":
                        flags.append(f"{field}:{state}")
            native_reason = NATIVE_REASONS.get(n.get("exit_reason", ""), n.get("exit_reason") or "(vazio)")
            reason_source = "csv" if native_reason not in ("(vazio)", "expert") else None
            logged = (host_closes or {}).get(str(n.get("position_id")), [])
            if native_reason in ("(vazio)", "expert") and len(logged) == 1 and logged[0] in HOST_REASONS:
                native_reason, reason_source = HOST_REASONS[logged[0]], "log_host"
            if native_reason != p["exit_reason"]:
                flags.append("exit_reason:different")
            exit_delta = (_minute(n["time"]) - _minute(str(p["exit_time"]))).total_seconds() / 60
            if exit_delta:
                flags.append("exit_minute:different")
            link, decision, link_note = _link(key[0], key[1], n.get("position_id"), events)
            groups_state = {g: all(c["state"] == "equal" for c in comparisons.values() if c["group"] == g)
                            for g in FIELD_GROUPS}
            groups_state["execution"] = groups_state["execution"] and native_reason == p["exit_reason"] and not exit_delta
            rows.append(dict(entry=str(key[0]), direction=key[1], position_id=n.get("position_id"),
                             link=link, link_note=link_note, decision=decision,
                             native_reason=native_reason, native_reason_source=reason_source,
                             python_reason=p["exit_reason"], exit_delta_min=exit_delta,
                             comparisons=comparisons, groups=groups_state, flags=flags,
                             identical=not flags and link == "confirmed"))
    summary = dict(
        python=len(python_trades), native=len(native_rows), paired=len(rows), collisions=collisions,
        only_python=len(only_python), only_native=len(only_native), tolerances=tol,
        identical=sum(r["identical"] for r in rows),
        groups_equal={g: sum(r["groups"][g] for r in rows) for g in ("protection", "execution", "financial")},
        links={state: sum(r["link"] == state for r in rows)
               for state in ("confirmed", "candidate_temporal", "ambiguous", "incompatible", "missing")},
        field_states={f: {st: sum(r["comparisons"][f]["state"] == st for r in rows)
                          for st in ("equal", "different", "missing_native", "missing_python")}
                      for fields in FIELD_GROUPS.values() for f, _ in fields},
        exit_reason_different=sum("exit_reason:different" in r["flags"] for r in rows),
        exit_minute_different=sum("exit_minute:different" in r["flags"] for r in rows),
        empty_native_reason=sum(r["native_reason"] == "(vazio)" for r in rows),
        reason_from_host_log=sum(r["native_reason_source"] == "log_host" for r in rows))
    return dict(summary=summary, rows=rows, only_python=only_python, only_native=only_native)
