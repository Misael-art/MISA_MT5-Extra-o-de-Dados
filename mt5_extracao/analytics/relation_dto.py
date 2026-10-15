"""Contrato JSON v1 das relações; preserva proveniência e estados separados."""
import json
import math
from datetime import date, datetime, timezone

from mt5_extracao.analytics.indicator_registry import ALGORITHM_VERSION


RELATION_SCHEMA_VERSION = 1
_INDICATOR_FIELDS = (
    "feature_id", "algorithm_version", "formula", "parameters", "unit", "warmup_count", "warmup_unit",
    "source_id", "timeframe", "availability_rule", "value_type", "value", "state", "reason",
)
_TARGET_FIELDS = (
    "target_id", "algorithm_version", "timeframe", "target_kind", "formula", "unit", "horizon_bars",
    "label_start_rule", "label_end_rule", "maturity_rule", "availability_rule",
)
_RELATION_FIELDS = (
    "method", "effect", "effect_ci_low", "effect_ci_high", "p_raw", "p_adjusted", "n_pairs", "n_sessions",
    "evidence_state", "utility_state", "freshness_state", "state", "reason",
)
_INDICATOR_TEXT_FIELDS = tuple(field for field in _INDICATOR_FIELDS if field not in {
    "parameters", "warmup_count", "value", "reason",
})
_TARGET_TEXT_FIELDS = tuple(field for field in _TARGET_FIELDS if field != "horizon_bars")
_RELATION_TEXT_FIELDS = tuple(field for field in _RELATION_FIELDS if field not in {
    "effect", "effect_ci_low", "effect_ci_high", "p_raw", "p_adjusted", "n_pairs", "n_sessions", "reason",
})


def _required_text(item, fields, name):
    missing = [field for field in fields if item.get(field) is None]
    if missing:
        raise ValueError(f"{name} sem campos obrigatórios: " + ", ".join(missing))
    for field in fields:
        value = item[field]
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{name}.{field} não pode ser texto vazio.")


def _utc_timestamp(value, label):
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ValueError(f"{label} precisa estar normalizado em UTC (sufixo Z).")
    try:
        stamp = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError(f"{label} precisa ser uma data e hora UTC válida.") from exc
    if stamp.tzinfo is None or stamp.utcoffset() != timezone.utc.utcoffset(stamp):
        raise ValueError(f"{label} precisa estar normalizado em UTC (sufixo Z).")
    return stamp.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _finite_optional_number(item, key):
    value = item.get(key)
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(float(value)):
        raise ValueError(f"{key} precisa ser número finito ou null.")


def _json_ready(value):
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    if hasattr(value, "item"):
        try:
            return _json_ready(value.item())
        except (ValueError, AttributeError):
            pass
    raise ValueError(f"Tipo não serializável no DTO: {type(value).__name__}.")


def build_relation_dto(*, indicator, target, relation, scope, provenance, generated_at_utc,
                       synthetic_fixture=False):
    """Valida e devolve envelope relation_schema_version=1 sem mudar o schema T13.6."""
    if not isinstance(indicator, dict) or not isinstance(target, dict) or not isinstance(relation, dict):
        raise ValueError("indicator, target e relation precisam ser objetos.")
    if not isinstance(scope, dict) or not isinstance(provenance, dict):
        raise ValueError("scope e provenance precisam ser objetos.")
    if not isinstance(synthetic_fixture, bool):
        raise ValueError("synthetic_fixture precisa ser booleano.")
    indicator = dict(indicator)
    target = dict(target)
    relation = dict(relation)
    scope = dict(scope)
    provenance = dict(provenance)
    missing_indicator = [field for field in _INDICATOR_FIELDS if field not in indicator]
    missing_relation = [field for field in _RELATION_FIELDS if field not in relation]
    if missing_indicator:
        raise ValueError("indicator sem campos obrigatórios: " + ", ".join(missing_indicator))
    if missing_relation:
        raise ValueError("relation sem campos obrigatórios: " + ", ".join(missing_relation))
    _required_text(indicator, _INDICATOR_TEXT_FIELDS, "indicator")
    _required_text(target, _TARGET_TEXT_FIELDS, "target")
    _required_text(relation, _RELATION_TEXT_FIELDS, "relation")
    if not isinstance(indicator["parameters"], dict):
        raise ValueError("indicator.parameters precisa ser objeto.")
    if indicator["value_type"] not in {"NUMBER", "CATEGORY", "BOOLEAN"}:
        raise ValueError("indicator.value_type inválido.")
    if indicator["reason"] is not None and not isinstance(indicator["reason"], str):
        raise ValueError("indicator.reason precisa ser texto ou null.")
    if indicator["value"] is None and not indicator["reason"]:
        raise ValueError("Indicador sem valor precisa de reason explícito.")
    if indicator["value"] is None and indicator["state"] == "AVAILABLE":
        raise ValueError("Indicador AVAILABLE precisa ter valor.")
    if indicator["value"] is not None and indicator["state"] != "AVAILABLE":
        raise ValueError("Indicador com valor precisa estar no estado AVAILABLE.")
    if indicator["value"] is not None:
        if indicator["value_type"] == "NUMBER":
            _finite_optional_number(indicator, "value")
        elif indicator["value_type"] == "BOOLEAN" and not isinstance(indicator["value"], bool):
            raise ValueError("indicator.value precisa ser booleano para value_type BOOLEAN.")
        elif indicator["value_type"] == "CATEGORY" and not isinstance(indicator["value"], str):
            raise ValueError("indicator.value precisa ser texto para value_type CATEGORY.")
    if (isinstance(indicator["warmup_count"], bool) or not isinstance(indicator["warmup_count"], int)
            or indicator["warmup_count"] < 0):
        raise ValueError("indicator.warmup_count precisa ser inteiro não negativo.")
    if (isinstance(target["horizon_bars"], bool) or not isinstance(target["horizon_bars"], int)
            or target["horizon_bars"] < 1):
        raise ValueError("target.horizon_bars precisa ser inteiro positivo.")
    if target["target_kind"] not in {"CONTINUOUS", "BINARY", "CATEGORICAL"}:
        raise ValueError("target.target_kind inválido.")
    if indicator["timeframe"] != target["timeframe"]:
        raise ValueError("indicator e target precisam usar o mesmo timeframe nesta versão do contrato.")
    for field in ("effect", "effect_ci_low", "effect_ci_high", "p_raw", "p_adjusted"):
        _finite_optional_number(relation, field)
    for field in ("p_raw", "p_adjusted"):
        if relation[field] is not None and not 0 <= float(relation[field]) <= 1:
            raise ValueError(f"relation.{field} precisa estar entre zero e um.")
    if (relation["effect_ci_low"] is not None and relation["effect_ci_high"] is not None
            and relation["effect_ci_low"] > relation["effect_ci_high"]):
        raise ValueError("relation.effect_ci_low não pode exceder effect_ci_high.")
    for field in ("n_pairs", "n_sessions"):
        count = relation[field]
        if count is not None and (isinstance(count, bool) or not isinstance(count, int) or count < 0):
            raise ValueError(f"relation.{field} precisa ser inteiro não negativo ou null.")
    if relation["effect"] is None and not relation["reason"]:
        raise ValueError("Relação sem efeito precisa de reason explícito.")
    if relation["reason"] is not None and not isinstance(relation["reason"], str):
        raise ValueError("relation.reason precisa ser texto ou null.")
    generated_at_utc = _utc_timestamp(generated_at_utc, "generated_at_utc")
    dto = {
        "relation_schema_version": RELATION_SCHEMA_VERSION,
        "kind": "INDICATOR_RELATION",
        "generated_at_utc": generated_at_utc,
        "synthetic_fixture": bool(synthetic_fixture),
        "scope": scope,
        "indicator": {field: indicator.get(field) for field in _INDICATOR_FIELDS},
        "target": {field: target.get(field) for field in _TARGET_FIELDS},
        "relation": {field: relation.get(field) for field in _RELATION_FIELDS},
        "provenance": provenance,
    }
    safe = _json_ready(dto)
    json.dumps(safe, ensure_ascii=False, allow_nan=False)
    return safe


def dumps(dto):
    """Serializa o DTO rejeitando NaN/Infinity em vez de emitir JSON não padrão."""
    if not isinstance(dto, dict) or dto.get("relation_schema_version") != RELATION_SCHEMA_VERSION:
        raise ValueError("relation_schema_version não suportado.")
    return json.dumps(_json_ready(dto), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def synthetic_fixture():
    """Exemplo de contrato sem estatística inventada; T18.2 ainda calcula as relações."""
    return build_relation_dto(
        generated_at_utc="2026-10-15T12:00:00Z",
        synthetic_fixture=True,
        scope={"asset": "SYNTHETIC", "timeframe": "M1", "direction": None, "expert": None},
        indicator={
            "feature_id": "momentum.rsi_14", "algorithm_version": ALGORITHM_VERSION,
            "formula": "Wilder RSI(14) applied to close", "parameters": {"period": 14, "method": "Wilder"},
            "unit": "points_0_100", "warmup_count": 15, "warmup_unit": "bars",
            "source_id": "bars_derived:M1:close", "timeframe": "M1",
            "availability_rule": "closed_bar_received; late revisions become available when received",
            "value_type": "NUMBER", "value": None, "state": "WARMUP", "reason": "REQUIRES_15_BARS",
        },
        target={
            "target_id": "return.close_forward_3b", "algorithm_version": "1.0", "timeframe": "M1",
            "target_kind": "CONTINUOUS", "formula": "close[t+3] / close[t] - 1", "unit": "fraction",
            "horizon_bars": 3, "label_start_rule": "decision_bar_close",
            "label_end_rule": "close_at_horizon_bars_after_decision",
            "maturity_rule": "all source bars through label_end are closed and their outcome inputs are received",
            "availability_rule": "max(received_at of outcome source bars and revisions); never available before label_end",
        },
        relation={
            "method": "spearman", "effect": None, "effect_ci_low": None, "effect_ci_high": None,
            "p_raw": None, "p_adjusted": None, "n_pairs": 0, "n_sessions": 0,
            "evidence_state": "INSUFFICIENT", "utility_state": "INCONCLUSIVE", "freshness_state": "UNAVAILABLE",
            "state": "NOT_COMPUTED", "reason": "T18.2 ainda não calculou relações; fixture contém somente o contrato.",
        },
        provenance={"source_mode": "synthetic_contract_fixture", "dataset_hash": None,
                    "source_revision": None, "as_of_utc": "2026-10-15T12:00:00Z"},
    )


