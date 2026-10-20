"""Adaptador base: delega sinal, regras de saída e backtest ao motor existente."""
from dataclasses import dataclass, replace

from .registry import BASE_MODES, EXECUTION_ORDER
from .schema import Composition, Problem


def base_composition(strategy_key, *, symbol, timeframe, params=None, risk_fraction=0.01, compound=True):
    """Retrato dos parâmetros efetivos; não cria nem persiste o preset Default (T19.55)."""
    from ..strategies import get_strategy

    strategy = get_strategy(strategy_key, **({} if params is None else params))
    data = {
        "composition_schema_version": 1,
        "strategy": {"key": strategy.key, "params": dict(strategy.params)},
        "context": {"symbol": symbol, "timeframe": timeframe},
        **{slot: {"mode": mode, "params": {}} for slot, mode in BASE_MODES.items()},
        "filters": [], "execution_order": list(EXECUTION_ORDER),
        "bar_policy": {"intrabar_priority": "stop_first", "position_limit": 1},
    }
    data["sizing"]["params"] = {"risk_fraction": risk_fraction, "compound": compound}
    return Composition(data)


def compatibility(composition, *, backend="python"):
    """Suporte do adaptador instalado; um sidecar externo não concede suporte."""
    if not isinstance(composition, Composition):
        composition = Composition(composition)
    if backend != "python":
        return [Problem("backend", "adaptador modular nativo genérico indisponível (T19.23); o perfil S09 requer S09NativeAdapter com fonte aprovada; "
                                   "use os fluxos legados ou o backtest Python. Declaração não autoriza execução.")]
    data = composition.to_dict()
    problems = [Problem(slot, f"modo {data[slot]['mode']} sem adaptador Python; use {mode} ou aguarde a capacidade.")
                for slot, mode in BASE_MODES.items()
                if data[slot]["mode"] not in ({mode, "fixed_points", "fixed_r"} if slot == "profit_exit"
                                             else {mode, "fixed_points"} if slot == "protection" else {mode,"temporal_v1","temporal_er_v1","temporal_donchian_v1","temporal_atr_v1"} if slot=="conditional_policy" else {mode})]
    if data["strategy"]["key"] == "pullback":
        problems.append(Problem("partial_exit_plan", "Pullback legado usa parcial ainda não qualificada (T19.57); "
                                                    "o novo adaptador não libera esse modo até corrigir confirmação/BE."))
    if any(module["mode"] != "session_v1" for module in data["filters"]):
        problems.append(Problem("filters", "filtros adicionais sem adaptador; remova-os do modo base. "
                                           "Filtros internos da estratégia continuam ativos."))
    return problems


@dataclass(frozen=True)
class BaseAdapter:
    composition: Composition

    def __post_init__(self):
        if not isinstance(self.composition, Composition):
            object.__setattr__(self, "composition", Composition(self.composition))
        problems = compatibility(self.composition)
        if problems:
            raise ValueError("; ".join(f"{p.field}: {p.message}" for p in problems))

    def strategy(self):
        from ..strategies import get_strategy

        value = self.composition.to_dict()["strategy"]
        return get_strategy(value["key"], **value["params"])

    def backtest_config(self, config=None):
        from ..strategies.backtester import BacktestConfig

        sizing = self.composition.to_dict()["sizing"]["params"]
        # Fração 0.01 é 1%, sem conversão implícita para input percentual MQL5.
        return replace(config if config is not None else BacktestConfig(),
                       risk_per_trade=sizing["risk_fraction"], compound=sizing["compound"])

    def run(self, bars, spec, config=None, *, timeframe, time_basis=None):
        from ..strategies.backtester import run_backtest

        context = self.composition.to_dict()["context"]
        if spec.symbol != context["symbol"]:
            raise ValueError("context.symbol: especificação de outro ativo; carregue o contrato do símbolo selecionado.")
        if timeframe != context["timeframe"]:
            raise ValueError("context.timeframe: série difere da composição; carregue a série selecionada.")
        strategy = self.strategy()
        value = self.composition.to_dict()
        profit, protection = value["profit_exit"], value["protection"]
        temporal=value["conditional_policy"]
        from ..strategies.session_entry import from_filters
        entry_policy = from_filters(value["filters"])
        if profit["mode"] == protection["mode"] == "strategy_default" and temporal["mode"]=="disabled":
            return run_backtest(bars, strategy, spec, self.backtest_config(config),
                                **({"entry_policy":entry_policy,"entry_time_basis":time_basis} if entry_policy is not None else {}))
        selection = {}
        if temporal["mode"] in ("temporal_v1","temporal_er_v1","temporal_donchian_v1","temporal_atr_v1"):
            from ..strategies.temporal_er import from_selection
            selection["temporal"]=from_selection(temporal)
        if profit["mode"] != "strategy_default":
            selection.update(target_r=None, target_points=None)
            key = "target_points" if profit["mode"] == "fixed_points" else "target_r"
            selection[key] = profit["params"][key]
        if protection["mode"] == "fixed_points":
            selection["stop_points"] = protection["params"]["stop_points"]
        rules = replace(strategy.exit_rules(), **selection)
        signals = (strategy.gerar_sinais(bars, exit_rules_override=rules) if rules.stop_points is not None
                   else strategy.gerar_sinais(bars)).copy()
        if profit["mode"] != "strategy_default":
            signals[["target_long", "target_short"]] = float("nan")
        return run_backtest(bars, strategy, spec, self.backtest_config(config),
                            signals=signals, exit_rules_override=rules,
                            **({"entry_policy":entry_policy,"entry_time_basis":time_basis} if entry_policy is not None else {}),
                            **({"observation_timeframe":timeframe} if temporal["mode"] in ("temporal_v1","temporal_er_v1","temporal_donchian_v1","temporal_atr_v1") else {}))
