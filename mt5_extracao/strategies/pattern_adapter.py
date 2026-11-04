"""Adaptador de pesquisa T13.4-P05 para o backtester canônico; nunca envia ordens."""

from dataclasses import asdict, dataclass, replace
import math

import pandas as pd

from mt5_extracao.analytics.hypotheses import Definition, digest
from .base import ExitRules, Strategy, StrategyInfo
from .backtester import BacktestConfig, SymbolSpec, execution_spreads, run_backtest


def _utc(value, label):
    try:
        stamp = pd.Timestamp(value)
    except (ValueError, TypeError, OverflowError) as exc:
        raise ValueError(f"{label} exige instante UTC explícito.") from exc
    if pd.isna(stamp) or stamp.tzinfo is None:
        raise ValueError(f"{label} exige instante UTC explícito.")
    return stamp.tz_convert("UTC")


def _positive(value, label, *, zero=False):
    invalid = isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value)
    if not invalid:
        invalid = value < 0 if zero else value <= 0
    if invalid:
        raise ValueError(f"{label} exige número {'não negativo' if zero else 'positivo'} finito.")


@dataclass(frozen=True)
class PatternExecutionPolicy:
    order_type: str
    position_policy: str
    stop_points: float
    target_r: float | None
    target_points: float | None
    max_bars: int
    initial_equity: float
    risk_per_trade: float
    compound: bool
    overnight_policy: str

    def __post_init__(self):
        if self.order_type != "market_next_open" or self.position_policy != "single_position":
            raise ValueError("Entrada limitada/concorrente não tem preenchimento compatível neste adaptador.")
        _positive(self.stop_points, "Stop em pontos")
        if (self.target_r is None) == (self.target_points is None):
            raise ValueError("Declare exatamente um alvo em R ou pontos.")
        _positive(self.target_r if self.target_r is not None else self.target_points, "Alvo")
        if type(self.max_bars) is not int or self.max_bars < 1:
            raise ValueError("Prazo máximo exige barras positivas.")
        _positive(self.initial_equity, "Capital")
        _positive(self.risk_per_trade, "Risco por operação")
        if self.risk_per_trade >= 1 or type(self.compound) is not bool:
            raise ValueError("Risco deve ser fração menor que um; capitalização precisa ser explícita.")
        if self.overnight_policy not in {"allow_with_spec_swap", "eod_utc"}:
            raise ValueError("Política overnight incompatível ou ausente.")


@dataclass(frozen=True)
class PatternCostModel:
    model_id: str
    source: str
    scenario: str
    price_basis: str
    spread_basis: str
    spread_points: float | None
    commission_per_lot_side: float
    slippage_points: float
    swap_policy: str
    account_currency: str
    valid_from_utc: str
    valid_until_utc: str
    calibrated_at_utc: str

    def __post_init__(self):
        if not self.model_id or not self.source or not self.account_currency:
            raise ValueError("Modelo de custo exige identidade, origem e moeda explícitas.")
        if self.scenario not in {"observed", "hypothetical"} or self.price_basis != "bid_ohlc":
            raise ValueError("Cenário ou base de preço sem suporte; OHLC deve ser bid.")
        if self.spread_basis not in {"bar_points", "fixed_points"}:
            raise ValueError("Spread exige barras observadas ou hipótese fixa declarada.")
        if self.spread_basis == "fixed_points":
            _positive(self.spread_points, "Spread fixo", zero=True)
        elif self.spread_points is not None:
            raise ValueError("Spread de barras não aceita valor fixo concorrente.")
        _positive(self.commission_per_lot_side, "Comissão por lado", zero=True)
        _positive(self.slippage_points, "Slippage", zero=True)
        if self.swap_policy not in {"spec_points", "zero_hypothesis"}:
            raise ValueError("Swap exige especificação suportada ou hipótese zero explícita.")
        first, last, calibrated = (_utc(getattr(self, key), key) for key in
                                   ("valid_from_utc", "valid_until_utc", "calibrated_at_utc"))
        if first >= last or calibrated > first:
            raise ValueError("Modelo de custo precisa ser conhecido antes do período de validade.")
        if self.scenario == "observed" and self.swap_policy == "zero_hypothesis":
            raise ValueError("Hipótese de swap zero não é custo observado.")


class _PatternStrategy(Strategy):
    key = "pattern_research"
    info = StrategyInfo("Padrão causal de pesquisa", "pesquisa", "não estimada", "não estimada",
                        "baixa", (), "fonte declarada", "Evento do P02; execução do backtester existente")
    min_bars = 1

    def __init__(self, signals, rules):
        super().__init__()
        self._pattern_signals = signals
        self._pattern_rules = rules

    def _signals(self, df, out):
        for index, side in self._pattern_signals.items():
            out.loc[index, "entry_" + side] = True

    def exit_rules(self):
        return self._pattern_rules


def run_pattern_simulation(bars, events, definition: Definition, *, source_namespace: str,
                           symbol_spec: SymbolSpec, policy: PatternExecutionPolicy,
                           costs: PatternCostModel):
    """Transforma eventos disponíveis em sinais e delega fills/PnL ao motor atual."""
    if not isinstance(definition, Definition) or not isinstance(policy, PatternExecutionPolicy) or not isinstance(costs, PatternCostModel):
        raise ValueError("Definição, política e custos tipados obrigatórios.")
    body = definition.body
    if body["side"] not in {"long", "short"} or body["simulated_entry"] != "next_executable_open":
        raise ValueError("Hipótese não traduzível para entrada de mercado após a decisão.")
    if body["cost_model_id"] != costs.model_id:
        raise ValueError("Modelo de custo diverge da hipótese congelada.")
    if not isinstance(symbol_spec, SymbolSpec) or not symbol_spec.known or symbol_spec.symbol not in body["universe_definition"]["symbols"]:
        raise ValueError("Especificação conhecida do ativo e universo são obrigatórios.")
    if not isinstance(source_namespace, str) or not source_namespace:
        raise ValueError("Namespace da fonte obrigatório.")
    if not isinstance(bars, pd.DataFrame) or bars.empty:
        raise ValueError("Simulação exige barras da fonte congelada.")
    required = {"time", "bar_start_utc", "bar_end_utc", "available_at_utc", "revision", "source_namespace",
                "symbol", "timeframe", "open", "high", "low", "close"}
    if required - set(bars):
        raise ValueError("Barras sem identidade, preço ou relógio de disponibilidade.")
    data = bars.reset_index(drop=True).copy()
    if not (data.source_namespace.eq(source_namespace).all() and data.symbol.eq(symbol_spec.symbol).all() and
            data.timeframe.eq(body["timeframe"]).all()):
        raise ValueError("Barras misturam fonte, ativo ou timeframe da hipótese.")
    for field in ("open", "high", "low", "close"):
        data[field] = pd.to_numeric(data[field], errors="coerce")
        if data[field].isna().any() or not data[field].map(math.isfinite).all():
            raise ValueError("OHLC exige preços finitos completos para simular fills.")
    if ((data.low <= 0) | (data.low > data.high) | (data.open < data.low) |
            (data.open > data.high) | (data.close < data.low) | (data.close > data.high)).any():
        raise ValueError("OHLC contraditório ou não positivo para a simulação.")
    if any(type(value) is not int or value < 1 for value in data.revision.tolist()):
        raise ValueError("Revisão de cada barra precisa ser inteira positiva.")
    starts = [_utc(value, "bar_start_utc") for value in data.bar_start_utc]
    ends = [_utc(value, "bar_end_utc") for value in data.bar_end_utc]
    available = [_utc(value, "available_at_utc") for value in data.available_at_utc]
    times = [_utc(value, "time") for value in data.time]
    if times != starts or any(not start < end or known < end for start, end, known in zip(starts, ends, available)) or any(
            ends[i] > starts[i + 1] for i in range(len(data) - 1)):
        raise ValueError("Barras precisam estar ordenadas, sem sobreposição, com relógios UTC coerentes.")
    if starts[0] < _utc(costs.valid_from_utc, "valid_from_utc") or ends[-1] > _utc(costs.valid_until_utc, "valid_until_utc"):
        raise ValueError("Custo fora da vigência das barras; segmente a simulação.")
    if costs.spread_basis == "bar_points":
        if "spread" not in data or data.spread.isna().any() or not data.spread.map(
                lambda value: not isinstance(value, bool) and isinstance(value, (int, float))
                and math.isfinite(value) and value >= 0).all():
            raise ValueError("Spread de barras precisa estar presente e ser não negativo em todo o recorte.")
    else:
        data["spread"] = costs.spread_points
    if policy.overnight_policy == "allow_with_spec_swap" and costs.swap_policy != "spec_points":
        raise ValueError("Pernoite exige swap conhecido da especificação.")
    if costs.swap_policy == "spec_points" and symbol_spec.swap_mode != 1:
        raise ValueError("Modo de swap do ativo não é calculado pelo backtester.")
    if costs.account_currency != symbol_spec.currency_profit and not symbol_spec.provenance.get("tick_value_account_currency") == costs.account_currency:
        raise ValueError("Moeda do tick_value não comprovada para a conta declarada.")
    spec = replace(symbol_spec, spread_points=float(costs.spread_points or 0), spread_by_hour=None,
                   spread_at_open=None)
    config = BacktestConfig(initial_equity=policy.initial_equity, risk_per_trade=policy.risk_per_trade,
                            slippage_points=costs.slippage_points,
                            commission_per_lot=costs.commission_per_lot_side,
                            compound=policy.compound, swap=costs.swap_policy == "spec_points",
                            real_spread=False)
    rules = ExitRules(stop_points=policy.stop_points, target_r=policy.target_r,
                      target_points=policy.target_points, max_bars=policy.max_bars,
                      eod_exit=policy.overnight_policy == "eod_utc")
    if not isinstance(events, (list, tuple)):
        raise ValueError("Eventos precisam ser lista ordenável de identidades P02.")
    start_index = {stamp: index for index, stamp in enumerate(starts)}
    if len(start_index) != len(starts):
        raise ValueError("Barras duplicadas não podem alimentar sinais.")
    mapped, journal, used_ids = {}, [], set()
    for event in events:
        if not isinstance(event, dict) or not {"event_id", "definition_hash", "source_namespace", "symbol",
                                             "timeframe", "side", "bar_start_utc", "bar_end_utc",
                                             "available_at_utc", "revision", "decision_at_utc"} <= set(event):
            raise ValueError("Evento sem identidade ou decisão causal.")
        if (event["event_id"] in used_ids or event["definition_hash"] != definition.definition_hash or
                event["source_namespace"] != source_namespace or event["symbol"] != symbol_spec.symbol or
                event["timeframe"] != body["timeframe"] or event["side"] != body["side"]):
            raise ValueError("Evento duplicado ou de outra definição/fonte.")
        used_ids.add(event["event_id"])
        original = start_index.get(_utc(event["bar_start_utc"], "bar_start_utc"))
        decision = _utc(event["decision_at_utc"], "decision_at_utc")
        event_available = _utc(event["available_at_utc"], "available_at_utc")
        if (original is None or _utc(event["bar_end_utc"], "bar_end_utc") != ends[original] or
                event_available != available[original] or event["revision"] != data.revision.iloc[original] or
                decision < event_available):
            raise ValueError("Evento sem barra de origem ou anterior ao fechamento.")
        identity = {"definition_hash": definition.definition_hash, "source_namespace": source_namespace,
                    "symbol": symbol_spec.symbol, "bar_start_utc": starts[original].isoformat(),
                    "revision": event["revision"], "available_at_utc": event_available.isoformat()}
        if event["event_id"] != digest(identity):
            raise ValueError("Identidade/hash do evento diverge da fonte congelada.")
        signal = next((index for index in range(original, len(data) - 1)
                       if ends[index] >= decision and available[index] <= ends[index]), None)
        row = {"event_id": event["event_id"], "definition_hash": definition.definition_hash,
               "source_namespace": source_namespace, "symbol": symbol_spec.symbol,
               "decision_at_utc": decision.isoformat(), "signal_bar_start_utc": None,
               "execution_bar_start_utc": None, "status": "censored_no_next_open"}
        if signal is not None:
            if signal in mapped:
                raise ValueError("Dois eventos competem pela mesma abertura; política de agregação ausente.")
            mapped[signal] = event["side"]
            row.update(signal_bar_start_utc=starts[signal].isoformat(),
                       execution_bar_start_utc=starts[signal + 1].isoformat(), status="signal_admitted")
        journal.append(row)
    strategy = _PatternStrategy(mapped, rules)
    result = run_backtest(data, strategy, spec, config)
    fills = {trade.entry_time: trade for trade in result.trades}
    for row in journal:
        if row["status"] == "signal_admitted":
            fill = fills.get(_utc(row["execution_bar_start_utc"], "execution_bar_start_utc"))
            row["status"] = "simulated_fill" if fill is not None else "not_executed_by_backtester"
            if fill is not None:
                row["exit_at_utc"] = fill.exit_time.isoformat()
                row["net_pnl_account_currency"] = fill.pnl
    spread, _, _ = execution_spreads(data, spec, config)
    ambiguous = []
    for trade in result.trades:
        if trade.exit_reason != "stop" or trade.target_price is None:
            continue
        index = start_index[trade.exit_time]
        low, high = float(data.low.iloc[index]), float(data.high.iloc[index])
        if trade.direction < 0:
            low, high = low + spread[index], high + spread[index]
        if low <= trade.stop_price <= high and low <= trade.target_price <= high:
            ambiguous.append(starts[index].isoformat())
    return {"backtest": result, "events": journal, "ambiguous_stop_first_bars": ambiguous,
            "cost_model": asdict(costs), "policy": asdict(policy),
            "cost_status": "hypothetical" if costs.scenario == "hypothetical" else "declared_observed",
            "economic_approval": False, "native_set_equivalent": False,
            "portfolio_qualified": False}
