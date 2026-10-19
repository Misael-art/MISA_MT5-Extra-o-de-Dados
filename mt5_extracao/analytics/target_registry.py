"""Definições tipadas e versionadas de rótulos futuros (não são características)."""
import json
from datetime import datetime, timezone

from sqlalchemy import inspect, text


TABLE = "_target_definitions"
_CREATE_TABLE = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    target_id TEXT NOT NULL,
    algorithm_version TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    target_kind TEXT NOT NULL,
    formula TEXT NOT NULL,
    parameters_json TEXT NOT NULL,
    unit TEXT NOT NULL,
    horizon_bars INTEGER NOT NULL,
    label_start_rule TEXT NOT NULL,
    label_end_rule TEXT NOT NULL,
    maturity_rule TEXT NOT NULL,
    availability_rule TEXT NOT NULL,
    registered_at_utc TEXT NOT NULL,
    PRIMARY KEY (target_id, algorithm_version, timeframe)
)
"""
_CREATE_HORIZON_INDEX = f"CREATE INDEX IF NOT EXISTS ix_target_definitions_horizon " \
                        f"ON {TABLE} (timeframe, horizon_bars, target_id)"


def _require_db(db):
    if db is None or getattr(db, "engine", None) is None:
        raise ValueError("O banco analítico não está conectado.")


def _check_write_guard(write_guard):
    if write_guard is not None:
        blocked = write_guard()
        if blocked:
            raise RuntimeError(f"Registro de alvo bloqueado pelo limite de recursos: {blocked}")


def _json(value):
    if not isinstance(value, dict):
        raise ValueError("parameters precisa ser um objeto JSON.")
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValueError("parameters precisa conter apenas valores JSON válidos.") from exc


def ensure_schema(db, *, write_guard=None):
    """Cria apenas a tabela interna de alvos e seu índice."""
    _require_db(db)
    _check_write_guard(write_guard)
    with db.engine.begin() as conn:
        conn.execute(text(_CREATE_TABLE))
        conn.execute(text(_CREATE_HORIZON_INDEX))


def standard_targets(timeframe, horizons=(1, 3, 5, 10, 20)):
    """Retorna alvos de retorno futuro com maturidade explícita por timeframe."""
    timeframe = str(timeframe).strip()
    if not timeframe:
        raise ValueError("timeframe precisa ser informado.")
    results = []
    seen = set()
    for raw_horizon in horizons:
        if isinstance(raw_horizon, bool) or not isinstance(raw_horizon, int) or raw_horizon < 1:
            raise ValueError("Cada horizonte precisa ser inteiro positivo.")
        if raw_horizon in seen:
            continue
        seen.add(raw_horizon)
        results.append({
            "target_id": f"return.close_forward_{raw_horizon}b",
            "algorithm_version": "1.0",
            "timeframe": timeframe,
            "target_kind": "CONTINUOUS",
            "formula": f"close[t+{raw_horizon}] / close[t] - 1",
            "parameters": {"horizon_bars": raw_horizon, "price_basis": "close", "side": "unsigned_market_return"},
            "unit": "fraction",
            "horizon_bars": raw_horizon,
            "label_start_rule": "decision_bar_close",
            "label_end_rule": "close_at_horizon_bars_after_decision",
            "maturity_rule": "all source bars through label_end are closed and their outcome inputs are received",
            "availability_rule": "max(received_at of outcome source bars and revisions); never available before label_end",
        })
    return results


def register(db, definition, *, write_guard=None):
    """Persiste uma definição imutável; correções exigem nova versão do algoritmo."""
    _require_db(db)
    _check_write_guard(write_guard)
    required = ("target_id", "algorithm_version", "timeframe", "target_kind", "formula", "unit", "horizon_bars",
                "label_start_rule", "label_end_rule", "maturity_rule", "availability_rule")
    item = dict(definition)
    missing = [key for key in required if item.get(key) is None]
    if missing:
        raise ValueError("Campos obrigatórios ausentes: " + ", ".join(missing))
    for key in required:
        value = item[key]
        if key == "horizon_bars":
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError("horizon_bars precisa ser inteiro positivo.")
        elif not isinstance(value, str) or not value.strip():
            raise ValueError(f"{key} precisa ser texto não vazio.")
        else:
            item[key] = value.strip()
    if item["target_kind"] not in {"CONTINUOUS", "BINARY", "CATEGORICAL"}:
        raise ValueError("target_kind precisa ser CONTINUOUS, BINARY ou CATEGORICAL.")
    params_json = _json(item.get("parameters", {}))
    item["algorithm_version"] = str(item["algorithm_version"])
    item["timeframe"] = str(item["timeframe"])
    _check_write_guard(write_guard)
    ensure_schema(db, write_guard=write_guard)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")
    with db.engine.begin() as conn:
        existing = conn.execute(text(
            f"SELECT target_kind, formula, parameters_json, unit, horizon_bars, label_start_rule, label_end_rule, "
            f"maturity_rule, availability_rule FROM {TABLE} WHERE target_id = :target_id "
            "AND algorithm_version = :version AND timeframe = :timeframe"),
            {"target_id": item["target_id"], "version": item["algorithm_version"],
             "timeframe": item["timeframe"]}).mappings().first()
        values = (item["target_kind"], item["formula"], params_json, item["unit"], item["horizon_bars"],
                  item["label_start_rule"], item["label_end_rule"], item["maturity_rule"],
                  item["availability_rule"])
        if existing:
            if tuple(existing.values()) == values:
                return False
            raise ValueError("O alvo já existe com esta versão; use outra algorithm_version.")
        conn.execute(text(
            f"INSERT INTO {TABLE} (target_id, algorithm_version, timeframe, target_kind, formula, parameters_json, "
            "unit, horizon_bars, label_start_rule, label_end_rule, maturity_rule, availability_rule, registered_at_utc) "
            "VALUES (:target_id, :version, :timeframe, :target_kind, :formula, :parameters, :unit, :horizon, "
            ":start_rule, :end_rule, :maturity, :availability, :registered_at)"),
            {"target_id": item["target_id"], "version": item["algorithm_version"], "timeframe": item["timeframe"],
             "target_kind": item["target_kind"], "formula": item["formula"], "parameters": params_json,
             "unit": item["unit"], "horizon": item["horizon_bars"], "start_rule": item["label_start_rule"],
             "end_rule": item["label_end_rule"], "maturity": item["maturity_rule"],
             "availability": item["availability_rule"], "registered_at": now})
    return True


def register_standard(db, timeframe, *, horizons=(1, 3, 5, 10, 20), write_guard=None):
    """Registra de forma idempotente os rótulos futuros padrão do timeframe."""
    results = []
    for definition in standard_targets(timeframe, horizons):
        results.append(register(db, definition, write_guard=write_guard))
    return {"inserted": sum(results), "unchanged": len(results) - sum(results), "timeframe": timeframe}


def list_targets(db, *, timeframe=None):
    if db is None or getattr(db, "engine", None) is None or not inspect(db.engine).has_table(TABLE):
        return []
    where = " WHERE timeframe = :timeframe" if timeframe is not None else ""
    params = {"timeframe": str(timeframe)} if timeframe is not None else {}
    with db.engine.connect() as conn:
        rows = conn.execute(text(
            f"SELECT target_id, algorithm_version, timeframe, target_kind, formula, parameters_json, unit, "
            f"horizon_bars, label_start_rule, label_end_rule, maturity_rule, availability_rule, registered_at_utc "
            f"FROM {TABLE}{where} ORDER BY timeframe, horizon_bars, target_id, algorithm_version"), params).mappings()
        result = []
        for row in rows:
            item = dict(row)
            item["parameters"] = json.loads(item.pop("parameters_json"))
            result.append(item)
        return result
