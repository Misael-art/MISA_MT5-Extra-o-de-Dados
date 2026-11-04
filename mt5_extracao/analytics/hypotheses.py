"""Contrato tipado de hipótese T13.4; não executa texto nem gera ordens."""

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import hashlib
import json

from mt5_extracao import timeframes


SCHEMA_VERSION = 1
FIELD_UNITS = {"open": "price", "high": "price", "low": "price", "close": "price",
               "prior_close": "price", "reference_price": "price", "session_minute": "minutes"}
REQUIRED = {"schema_version", "pattern_id", "strategy_version", "original_hypothesis", "origin",
            "parent_attempt_ids", "universe_definition", "data_requirements", "timeframe",
            "timezone_calendar_versions", "features_and_versions", "context_expression",
            "condition_expression", "decision_clock", "price_reference", "side",
            "simulated_entry", "outcome_definition", "repetition_policy", "invalidation_rules",
            "execution_policy", "cost_model_id", "complexity", "implementation_signature"}
MAX_DEPTH, MAX_NODES = 8, 64


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def _number(value, unit):
    if not isinstance(value, str):
        raise ValueError("Literal numérico exige texto decimal explícito, sem inferência de percentual.")
    try:
        number = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError("Literal decimal inválido.") from exc
    if not number.is_finite():
        raise ValueError("Literal precisa ser finito.")
    if unit == "fraction" and not -1 <= number <= 1:
        raise ValueError("Fração fora de [-1,1]; 1,3% deve ser declarado como 0.013.")
    return str(number)


def expression_unit(node, *, depth=0, budget=None):
    """Valida AST JSON e devolve unidade; recusa operadores/nomes implícitos."""
    if budget is None:
        budget = [0]
    budget[0] += 1
    if depth > MAX_DEPTH or budget[0] > MAX_NODES or not isinstance(node, dict):
        raise ValueError("Expressão inválida ou acima dos limites de profundidade/termos.")
    op = node.get("op")
    if op == "field" and set(node) == {"op", "name"} and node["name"] in FIELD_UNITS:
        return FIELD_UNITS[node["name"]]
    if op == "literal" and set(node) == {"op", "unit", "value"} and node["unit"] in {"price", "fraction", "minutes"}:
        _number(node["value"], node["unit"])
        return node["unit"]
    if op == "percent_change" and set(node) == {"op", "value", "reference"}:
        if (expression_unit(node["value"], depth=depth + 1, budget=budget) != "price" or
                expression_unit(node["reference"], depth=depth + 1, budget=budget) != "price"):
            raise ValueError("Variação percentual exige dois preços declarados.")
        return "fraction"
    if op == "compare" and set(node) == {"op", "comparison", "left", "right"}:
        if node["comparison"] not in {"gt", "gte", "lt", "lte"}:
            raise ValueError("Comparação não suportada; igualdade exige tolerância de tick explícita.")
        left = expression_unit(node["left"], depth=depth + 1, budget=budget)
        right = expression_unit(node["right"], depth=depth + 1, budget=budget)
        if left != right or left == "boolean":
            raise ValueError("Comparação exige unidades numéricas iguais.")
        return "boolean"
    if op == "crosses_above" and set(node) == {"op", "left", "right"}:
        left = expression_unit(node["left"], depth=depth + 1, budget=budget)
        right = expression_unit(node["right"], depth=depth + 1, budget=budget)
        if left != right or left == "boolean":
            raise ValueError("Cruzamento exige unidades numéricas iguais e estado anterior conhecido.")
        return "boolean"
    if op in {"and", "or"} and set(node) == {"op", "terms"} and isinstance(node["terms"], list) and 2 <= len(node["terms"]) <= 8:
        if any(expression_unit(term, depth=depth + 1, budget=budget) != "boolean" for term in node["terms"]):
            raise ValueError("Operador lógico exige condições booleanas.")
        return "boolean"
    if op == "not" and set(node) == {"op", "term"}:
        if expression_unit(node["term"], depth=depth + 1, budget=budget) != "boolean":
            raise ValueError("NOT exige condição booleana.")
        return "boolean"
    raise ValueError(f"Nó de expressão desconhecido ou incompleto: {op!r}.")


@dataclass(frozen=True)
class Definition:
    _body_json: str
    definition_hash: str

    @property
    def body(self):
        return json.loads(self._body_json)

    def export(self):
        return {**self.body, "definition_hash": self.definition_hash}


def compile_definition(raw):
    if not isinstance(raw, dict) or set(raw) != REQUIRED:
        raise ValueError("Definição requer exatamente os campos versionados de T13.4.")
    body = json.loads(canonical(raw))
    if type(body["schema_version"]) is not int or body["schema_version"] != SCHEMA_VERSION or not isinstance(body["pattern_id"], str) or not body["pattern_id"].strip():
        raise ValueError("Versão ou pattern_id inválidos.")
    if body["origin"] not in {"user", "exploratory", "derived"} or body["side"] not in {"long", "short", "none"}:
        raise ValueError("Origem ou lado da hipótese inválido.")
    tf = timeframes.parse(body["timeframe"])
    if tf is None:
        raise ValueError("Timeframe da hipótese inválido.")
    body["timeframe"] = tf.name
    if body["decision_clock"] != "closed_bar_available" or body["simulated_entry"] != "next_executable_open":
        raise ValueError("Relógio ou política de entrada sem contrato causal.")
    if body["execution_policy"] != "research_only":
        raise ValueError("P02 produz pesquisa, sem autorização de execução nativa.")
    if (not isinstance(body["outcome_definition"], dict) or
            set(body["outcome_definition"]) != {"horizon_bars"} or
            type(body["outcome_definition"]["horizon_bars"]) is not int or
            not 1 <= body["outcome_definition"]["horizon_bars"] <= 500):
        raise ValueError("Horizonte futuro precisa de 1 a 500 barras explícitas.")
    if body["repetition_policy"] not in {"all", "first_per_session"}:
        raise ValueError("Política de repetição inválida.")
    if body["context_expression"] is not None and expression_unit(body["context_expression"]) != "boolean":
        raise ValueError("Contexto precisa ser booleano.")
    if expression_unit(body["condition_expression"]) != "boolean":
        raise ValueError("Condição precisa ser booleana.")
    for key in ("parent_attempt_ids", "data_requirements", "features_and_versions", "invalidation_rules"):
        if not isinstance(body[key], list):
            raise ValueError(f"{key} precisa ser lista explícita.")
    if body["features_and_versions"] or body["invalidation_rules"]:
        raise ValueError("P02 ainda não consome features ou invalidações declaradas; não as ignore silenciosamente.")
    if not body["data_requirements"] or not all(isinstance(value, str) and value for value in body["data_requirements"]):
        raise ValueError("Requisitos de dados explícitos são obrigatórios.")
    for key in ("universe_definition", "timezone_calendar_versions", "complexity"):
        if not isinstance(body[key], dict):
            raise ValueError(f"{key} precisa ser objeto explícito.")
    universe = body["universe_definition"]
    if (set(universe) != {"mode", "symbols"} or universe["mode"] != "explicit" or
            not isinstance(universe["symbols"], list) or not universe["symbols"] or
            any(not isinstance(symbol, str) or not symbol for symbol in universe["symbols"]) or
            len(set(universe["symbols"])) != len(universe["symbols"])):
        raise ValueError("P02 exige universo explícito de símbolos distintos.")
    for key in ("strategy_version", "original_hypothesis", "price_reference", "implementation_signature"):
        if not isinstance(body[key], str) or not body[key]:
            raise ValueError(f"{key} obrigatório.")
    if body["price_reference"] not in {"close", "prior_close", "reference_price"}:
        raise ValueError("Referência de preço não disponível no avaliador causal.")
    if body["cost_model_id"] is not None and (not isinstance(body["cost_model_id"], str) or not body["cost_model_id"]):
        raise ValueError("cost_model_id deve ser identificador ou null para estudo de eventos.")
    return Definition(_body_json=canonical(body), definition_hash=digest(body))


def signal_spec_condition(spec, side):
    """Adapta só a lógica `close`/limiar do SignalSpec; não converte stops ou MQL5."""
    from mt5_extracao.strategies.declared import SignalSpec

    if not isinstance(spec, SignalSpec) or side not in {"long", "short"}:
        raise ValueError("Informe SignalSpec válido e lado explícito.")
    conditions = getattr(spec, side)
    if not conditions:
        raise ValueError("SignalSpec não declara condição para este lado.")

    def operand(value):
        if value == "close":
            return {"op": "field", "name": "close"}
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return {"op": "literal", "unit": "price", "value": str(value)}
        raise ValueError("Indicador do SignalSpec exige versão materializada; adaptador de condição recusado.")

    terms = []
    for operator, left, right in conditions:
        first, second = operand(left), operand(right)
        if operator in ("above", "below"):
            term = {"op": "compare", "comparison": "gt" if operator == "above" else "lt",
                    "left": first, "right": second}
        elif operator in ("cross_above", "cross_below"):
            term = {"op": "crosses_above", "left": first if operator == "cross_above" else second,
                    "right": second if operator == "cross_above" else first}
        else:
            raise ValueError("Operador do SignalSpec sem equivalência declarada.")
        terms.append(term)
    result = terms[0] if len(terms) == 1 else {"op": "and", "terms": terms}
    expression_unit(result)
    return result
