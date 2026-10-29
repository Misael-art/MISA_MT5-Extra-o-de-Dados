"""Associações contínuas e alvos futuros com maturidade e inferência temporal explícitas.

Esta camada trabalha com amostras já alinhadas. Características precisam estar
disponíveis no instante de decisão; um alvo futuro só entra quando estiver
maturado e recebido até ``as_of``. As estatísticas são descritivas e não medem
rentabilidade nem causalidade.
"""
import hashlib
import json
import math
from numbers import Real

import numpy as np
import pandas as pd
from scipy.stats import rankdata

from mt5_extracao.analytics import target_registry


FEATURE_PAIR = "FEATURE_PAIR"
FEATURE_TARGET = "FEATURE_TARGET"
_TARGET_BAR_QUALITIES = {"COMPLETE", "NO_TRADES", "CORRECTED"}
_TARGET_COLUMNS = [
    "symbol", "timeframe", "session_id", "observation_time", "decision_time", "label_end_utc",
    "target_id", "algorithm_version", "horizon_bars", "value", "state", "reason",
    "available_at_utc", "source_revision",
]


def _utc_timestamp(value, label):
    try:
        stamp = pd.Timestamp(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{label} precisa ser uma data e hora válida com fuso.") from exc
    if pd.isna(stamp) or stamp.tzinfo is None:
        raise ValueError(f"{label} precisa incluir fuso horário.")
    return stamp.tz_convert("UTC")


def _utc_series(values, label, *, allow_missing=False):
    result = []
    for value in values:
        if pd.isna(value):
            if not allow_missing:
                raise ValueError(f"{label} não pode conter datas ausentes.")
            result.append(pd.NaT)
        else:
            result.append(_utc_timestamp(value, label))
    return pd.Series(pd.to_datetime(result, utc=True), index=values.index)


def _scope(frame, columns, label):
    missing = [name for name in columns if name not in frame.columns]
    if missing:
        raise ValueError(f"{label} sem dimensões obrigatórias: " + ", ".join(missing))
    values = {}
    for name in columns:
        unique = frame[name].dropna().astype(str).unique()
        if (len(unique) != 1 or not unique[0].strip() or frame[name].isna().any()
                or not all(isinstance(value, str) and value.strip() for value in frame[name])):
            raise ValueError(f"{label} precisa conter exatamente um valor de {name}.")
        values[name] = unique[0]
    return values


def _bar_fingerprint(row):
    input_hash = row.get("input_hash")
    if input_hash is not None and not pd.isna(input_hash):
        source = str(input_hash)
    else:
        material = {
            "open": _json_number(row.get("open")),
            "high": _json_number(row.get("high")),
            "low": _json_number(row.get("low")),
            "close": _json_number(row.get("close")),
            "tick_volume": _json_number(row.get("tick_volume")),
            "real_volume": _json_number(row.get("real_volume")),
        }
        source = json.dumps(material, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return [str(row["bar_start_utc"]), str(row["bar_end_utc"]), str(row["session_id"]),
            int(row.get("revision", 1)), source]


def _json_number(value):
    if value is None or pd.isna(value):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def build_forward_close_targets(bars, *, horizons=(1, 3, 5, 10, 20), as_of):
    """Calcula ``close[t+h] / close[t] - 1`` em barras fechadas e maduras.

    ``bars`` deve ser uma única série de revisões atuais, normalmente a saída de
    ``resampling.latest_bars(..., closed_only=True)``. Horizontes contam as
    próximas barras observadas do timeframe, inclusive entre sessões. O alvo só
    recebe valor quando o fechamento do horizonte e todas as revisões de origem
    já eram conhecidos em ``as_of``. A cauda imatura continua presente com
    ``value=None`` e um motivo explícito.
    """
    if not isinstance(bars, pd.DataFrame):
        raise ValueError("bars precisa ser um DataFrame.")
    required = {"symbol", "tf", "session_id", "bar_start_utc", "bar_end_utc", "computed_at_utc", "close"}
    missing = sorted(required - set(bars.columns))
    if missing:
        raise ValueError("bars sem colunas obrigatórias: " + ", ".join(missing))
    if bars.empty:
        return pd.DataFrame(columns=_TARGET_COLUMNS)
    cutoff = _utc_timestamp(as_of, "as_of")
    scope = _scope(bars, ("symbol", "tf"), "bars")
    definitions = target_registry.standard_targets(scope["tf"], horizons)
    work = bars.copy().reset_index(drop=True)
    work["bar_start_utc"] = _utc_series(work["bar_start_utc"], "bar_start_utc")
    work["bar_end_utc"] = _utc_series(work["bar_end_utc"], "bar_end_utc")
    work["computed_at_utc"] = _utc_series(work["computed_at_utc"], "computed_at_utc")
    work = work.sort_values(["bar_start_utc", "session_id"], kind="stable").reset_index(drop=True)
    if work["bar_start_utc"].duplicated().any():
        raise ValueError("bars contém mais de uma revisão atual para o mesmo início de barra.")
    if (work["bar_end_utc"] <= work["bar_start_utc"]).any():
        raise ValueError("Cada barra precisa terminar depois de começar.")
    if len(work) > 1 and (work["bar_start_utc"].iloc[1:].reset_index(drop=True) <
                          work["bar_end_utc"].iloc[:-1].reset_index(drop=True)).any():
        raise ValueError("bars contém barras sobrepostas.")
    work["close"] = pd.to_numeric(work["close"], errors="coerce")
    if "revision" not in work:
        work["revision"] = 1
    revisions = pd.to_numeric(work["revision"], errors="coerce")
    if revisions.isna().any() or (revisions < 1).any() or (revisions % 1 != 0).any():
        raise ValueError("revision precisa conter inteiros positivos.")
    work["revision"] = revisions.astype(int)
    rows = []
    for definition in definitions:
        horizon = definition["horizon_bars"]
        target_id = definition["target_id"]
        for index, start in work.iterrows():
            end_index = index + horizon
            result = {
                "symbol": scope["symbol"], "timeframe": scope["tf"], "session_id": start["session_id"],
                "observation_time": start["bar_start_utc"], "decision_time": start["bar_end_utc"],
                "label_end_utc": pd.NaT, "target_id": target_id,
                "algorithm_version": definition["algorithm_version"], "horizon_bars": horizon,
                "value": None, "state": "NOT_MATURE", "reason": "HORIZON_BARS_NOT_AVAILABLE",
                "available_at_utc": pd.NaT, "source_revision": None,
            }
            if end_index >= len(work):
                rows.append(result)
                continue

            sources = work.iloc[index:end_index + 1]
            finish = work.iloc[end_index]
            result["label_end_utc"] = finish["bar_end_utc"]
            revision_data = [_bar_fingerprint(row) for _, row in sources.iterrows()]
            result["source_revision"] = "bars:" + hashlib.sha256(
                json.dumps(revision_data, separators=(",", ":"), ensure_ascii=False,
                           allow_nan=False).encode("utf-8")).hexdigest()[:24]
            available_at = max(finish["bar_end_utc"], sources["computed_at_utc"].max())
            result["available_at_utc"] = available_at

            bad_quality = False
            if "quality_status" in sources:
                bad_quality |= not sources["quality_status"].astype(str).str.upper().isin(
                    _TARGET_BAR_QUALITIES).all()
            if "coverage_ratio" in sources:
                coverage = pd.to_numeric(sources["coverage_ratio"], errors="coerce")
                bad_quality |= not coverage.ge(1.0 - 1e-9).all()
            if "is_partial" in sources:
                partial = pd.to_numeric(sources["is_partial"], errors="coerce").fillna(1)
                bad_quality |= partial.eq(1).any()
            if bad_quality:
                result.update(state="UNAVAILABLE", reason="SOURCE_BAR_NOT_ACCEPTABLE")
            elif finish["bar_end_utc"] > cutoff:
                result.update(state="NOT_MATURE", reason="HORIZON_END_AFTER_AS_OF")
            elif available_at > cutoff:
                result.update(state="NOT_AVAILABLE_AS_OF", reason="SOURCE_REVISION_RECEIVED_AFTER_AS_OF")
            else:
                start_close, end_close = start["close"], finish["close"]
                if not np.isfinite(start_close) or not np.isfinite(end_close):
                    result.update(state="UNAVAILABLE", reason="CLOSE_VALUE_MISSING")
                elif start_close <= 0:
                    result.update(state="UNAVAILABLE", reason="INVALID_START_CLOSE_DENOMINATOR")
                else:
                    value = float(end_close / start_close - 1.0)
                    if math.isfinite(value):
                        result.update(value=value, state="MATURE", reason=None)
                    else:
                        result.update(state="UNAVAILABLE", reason="NON_FINITE_TARGET")
            rows.append(result)
    return pd.DataFrame(rows, columns=_TARGET_COLUMNS)


def _result_base(kind, method, as_of, scope):
    return {
        "kind": kind, "method": method, "as_of_utc": as_of.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "scope": scope, "status": "INSUFFICIENT", "reason_codes": [], "effect": None,
        "ci_low": None, "ci_high": None, "confidence_level": 0.95, "p_raw": None, "p_adjusted": None,
        "n_total": 0, "n_pairs": 0, "n_sessions": 0, "n_missing_x": 0, "n_missing_y": 0,
        "n_immature": 0, "n_temporal_rejected": 0, "n_after_as_of": 0, "n_non_numeric": 0,
        "period_start_utc": None, "period_end_utc": None,
        "inference": {"method": "circular moving-session-block bootstrap with wild sign-flip null",
                      "bootstrap_replicates": 0, "valid_replicates": 0, "block_length_sessions": None,
                      "seed": None},
    }


def _numeric_column(series):
    non_null = series.notna()
    wrong_type = pd.Series(False, index=series.index)
    wrong_type.loc[non_null] = series.loc[non_null].map(
        lambda value: isinstance(value, (bool, np.bool_)) or not isinstance(value, Real))
    numeric = pd.to_numeric(series.where(~wrong_type), errors="coerce")
    invalid = non_null & (wrong_type | numeric.isna() | ~np.isfinite(numeric))
    numeric = numeric.where(np.isfinite(numeric))
    return numeric.astype(float), invalid


def _ranked_standardized(x, y, method):
    if method == "spearman":
        x = rankdata(x, method="average").astype(float)
        y = rankdata(y, method="average").astype(float)
    x = x.astype(float)
    y = y.astype(float)
    x_centered, y_centered = x - x.mean(), y - y.mean()
    x_std, y_std = float(x_centered.std()), float(y_centered.std())
    x_tol = np.finfo(float).eps * max(1.0, float(np.max(np.abs(x)))) * 32
    y_tol = np.finfo(float).eps * max(1.0, float(np.max(np.abs(y)))) * 32
    if x_std <= x_tol or y_std <= y_tol:
        return None
    return x_centered / x_std, y_centered / y_std


def _moving_block_indices(session_count, block_length, rng):
    starts = rng.integers(0, session_count, size=int(math.ceil(session_count / block_length)))
    return [int((start + offset) % session_count)
            for start in starts for offset in range(block_length)][:session_count]


def analyze_association(samples, *, kind=FEATURE_PAIR, method="pearson", as_of, minimum_pairs=30,
                        minimum_sessions=10, bootstrap_replicates=1000, seed=7, block_length_sessions=None,
                        guard=None):
    """Calcula Pearson/Spearman em uma população homogênea e com tempo validado.

    Colunas obrigatórias: ``symbol``, ``timeframe``, ``session_id``,
    ``decision_time``, ``x_value``, ``y_value``, ``x_available_at_utc`` e
    ``y_available_at_utc``. Para ``FEATURE_TARGET``, informe também
    ``y_state`` (MATURE para rótulos utilizáveis) e ``label_end_utc``.
    Linhas fora do corte temporal, imaturas, ausentes ou tardias são contadas e
    excluídas com motivo; amostras de ativos/timeframes mistos são rejeitadas.
    """
    if not isinstance(samples, pd.DataFrame):
        raise ValueError("samples precisa ser um DataFrame.")
    if guard is not None:
        if not callable(guard):
            raise ValueError("guard precisa ser uma função de verificação de recursos/cancelamento.")
        guard()
    if kind not in {FEATURE_PAIR, FEATURE_TARGET}:
        raise ValueError("kind precisa ser FEATURE_PAIR ou FEATURE_TARGET.")
    method = str(method).lower()
    if method not in {"pearson", "spearman"}:
        raise ValueError("method precisa ser pearson ou spearman para variáveis contínuas.")
    required = {"symbol", "timeframe", "session_id", "decision_time", "x_value", "y_value",
                "x_available_at_utc", "y_available_at_utc"}
    if kind == FEATURE_TARGET:
        required.update({"y_state", "label_end_utc"})
    missing = sorted(required - set(samples.columns))
    if missing:
        raise ValueError("samples sem colunas obrigatórias: " + ", ".join(missing))
    cutoff = _utc_timestamp(as_of, "as_of")
    minimum_pairs = _positive_int(minimum_pairs, "minimum_pairs")
    minimum_sessions = _positive_int(minimum_sessions, "minimum_sessions")
    bootstrap_replicates = _positive_int(bootstrap_replicates, "bootstrap_replicates")
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise ValueError("seed precisa ser inteiro.")
    if block_length_sessions is not None:
        _positive_int(block_length_sessions, "block_length_sessions")
    if samples.empty:
        result = _result_base(kind, method, cutoff, {"symbol": None, "timeframe": None})
        result["reason_codes"] = ["NO_SAMPLES"]
        return result
    scope = _scope(samples, ("symbol", "timeframe"), "samples")
    result = _result_base(kind, method, cutoff, scope)
    result["n_total"] = int(len(samples))
    work = samples.copy().reset_index(drop=True)
    if not all(isinstance(value, str) and value.strip() for value in work.session_id):
        raise ValueError("session_id exige identidades de sessão explícitas e não vazias.")
    for column in ("decision_time",):
        work[column] = _utc_series(work[column], column)
    for column in ("x_available_at_utc", "y_available_at_utc"):
        work[column] = _utc_series(work[column], column, allow_missing=True)
    if kind == FEATURE_TARGET:
        work["label_end_utc"] = _utc_series(work["label_end_utc"], "label_end_utc", allow_missing=True)
    x, bad_x = _numeric_column(work["x_value"])
    y, bad_y = _numeric_column(work["y_value"])

    keep = work["decision_time"] <= cutoff
    result["n_after_as_of"] = int((~keep).sum())
    temporal_x = work["x_available_at_utc"].notna() & (work["x_available_at_utc"] <= work["decision_time"])
    temporal = keep & ~temporal_x
    if kind == FEATURE_PAIR:
        temporal_y = work["y_available_at_utc"].notna() & (work["y_available_at_utc"] <= work["decision_time"])
        temporal |= keep & ~temporal_y
    result["n_temporal_rejected"] = int(temporal.sum())
    keep &= temporal_x
    if kind == FEATURE_PAIR:
        keep &= temporal_y
    else:
        state_mature = work["y_state"].astype(str).str.upper().eq("MATURE")
        mature = (state_mature & work["label_end_utc"].notna() &
                  (work["label_end_utc"] <= cutoff) & work["y_available_at_utc"].notna() &
                  (work["y_available_at_utc"] <= cutoff) &
                  (work["y_available_at_utc"] >= work["label_end_utc"]) &
                  (work["label_end_utc"] > work["decision_time"]))
        result["n_immature"] = int((keep & ~mature).sum())
        keep &= mature

    result["n_non_numeric"] = int((keep & (bad_x | bad_y)).sum())
    result["n_missing_x"] = int((keep & work["x_value"].isna()).sum())
    result["n_missing_y"] = int((keep & work["y_value"].isna()).sum())
    complete = x.notna() & y.notna() & ~bad_x & ~bad_y
    candidate = keep & complete
    valid_positions = np.flatnonzero(candidate.to_numpy())
    valid = work.iloc[valid_positions].copy()
    valid["_x"] = x.iloc[valid_positions].to_numpy()
    valid["_y"] = y.iloc[valid_positions].to_numpy()
    valid = valid.sort_values(["decision_time"], kind="stable").reset_index(drop=True)
    result["n_pairs"] = int(len(valid))
    session_order = list(dict.fromkeys(valid["session_id"].astype(str)))
    result["n_sessions"] = len(session_order)
    if len(valid):
        result["period_start_utc"] = valid["decision_time"].min().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        result["period_end_utc"] = valid["decision_time"].max().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    if result["n_non_numeric"]:
        result["status"] = "UNSUPPORTED_VALUE_TYPE"
        result["reason_codes"] = ["CONTINUOUS_NUMERIC_VALUES_REQUIRED"]
        return result
    runs = valid.session_id.loc[valid.session_id.ne(valid.session_id.shift())]
    if runs.duplicated().any():
        raise ValueError("Sessões intercaladas não formam blocos cronológicos válidos.")
    reasons = []
    if len(valid) < minimum_pairs:
        reasons.append("MINIMUM_PAIRS_NOT_MET")
    if len(session_order) < minimum_sessions:
        reasons.append("MINIMUM_SESSIONS_NOT_MET")
    if reasons:
        result["reason_codes"] = reasons
        return result

    x_values, y_values = valid["_x"].to_numpy(), valid["_y"].to_numpy()
    standardized = _ranked_standardized(x_values, y_values, method)
    if standardized is None:
        result["status"] = "CONSTANT_INPUT"
        result["reason_codes"] = ["X_CONSTANT_OR_NEAR_CONSTANT" if np.std(x_values) <=
                                  np.finfo(float).eps * max(1.0, float(np.max(np.abs(x_values)))) * 32
                                  else "Y_CONSTANT_OR_NEAR_CONSTANT"]
        return result
    x_standardized, y_standardized = standardized
    score = x_standardized * y_standardized
    observed = float(score.mean())
    if not math.isfinite(observed):
        result["status"] = "INSUFFICIENT"
        result["reason_codes"] = ["NON_FINITE_EFFECT"]
        return result
    observed = float(np.clip(observed, -1., 1.))

    session_positions = {}
    for position, session in enumerate(valid["session_id"].astype(str)):
        session_positions.setdefault(session, []).append(position)
    clusters = [np.asarray(session_positions[session], dtype=int) for session in session_order]
    block_length = block_length_sessions
    if block_length is None:
        block_length = max(1, int(math.ceil(len(clusters) ** (1.0 / 3.0))))
    block_length = _positive_int(block_length, "block_length_sessions")
    if block_length > len(clusters):
        raise ValueError("block_length_sessions não pode exceder n_sessions.")

    rng = np.random.default_rng(seed)
    estimates, null_estimates = [], []
    for _ in range(bootstrap_replicates):
        if guard is not None:
            guard()
        sampled_sessions = _moving_block_indices(len(clusters), block_length, rng)
        sampled_positions = np.concatenate([clusters[index] for index in sampled_sessions])
        estimate = _correlation(valid["_x"].to_numpy()[sampled_positions],
                                valid["_y"].to_numpy()[sampled_positions], method)
        if estimate is not None and math.isfinite(estimate):
            estimates.append(estimate)

        signs = np.ones(len(clusters), dtype=float)
        for start in range(0, len(clusters), block_length):
            signs[start:start + block_length] = rng.choice((-1.0, 1.0))
        row_signs = np.ones(len(valid), dtype=float)
        for index, cluster in enumerate(clusters):
            row_signs[cluster] = signs[index]
        null_estimates.append(float(np.mean(score * row_signs)))

    if len(estimates) < max(2, int(bootstrap_replicates * 0.8)):
        result["status"] = "INSUFFICIENT"
        result["reason_codes"] = ["TOO_FEW_VALID_BOOTSTRAP_REPLICATES"]
        result["inference"].update({"bootstrap_replicates": bootstrap_replicates,
                                    "valid_replicates": len(estimates),
                                    "block_length_sessions": block_length, "seed": seed})
        return result
    ci_low, ci_high = np.quantile(estimates, [0.025, 0.975])
    p_raw = (1 + np.count_nonzero(np.abs(null_estimates) >= abs(observed))) / (len(null_estimates) + 1)
    result.update(status="OK", effect=observed, ci_low=float(ci_low), ci_high=float(ci_high), p_raw=float(p_raw))
    result["inference"].update({"bootstrap_replicates": bootstrap_replicates,
                                "valid_replicates": len(estimates),
                                "block_length_sessions": block_length, "seed": seed})
    if guard is not None:
        guard()
    return result


def _positive_int(value, label):
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{label} precisa ser inteiro positivo.")
    return value


def _correlation(x, y, method):
    standardized = _ranked_standardized(np.asarray(x, dtype=float), np.asarray(y, dtype=float), method)
    if standardized is None:
        return None
    x_standardized, y_standardized = standardized
    return float(np.clip(np.mean(x_standardized * y_standardized), -1., 1.))


def adjust_bonferroni(results, *, tests_counted):
    """Copia resultados e ajusta p-values contando também testes sem efeito válido.

    ``tests_counted`` representa a família definida antes da busca e precisa ser
    pelo menos o número de resultados recebidos. Não filtre comparações inválidas
    para diminuir o tamanho da família.
    """
    if not isinstance(results, (list, tuple)):
        raise ValueError("results precisa ser uma lista ou tupla de resultados.")
    tests_counted = _positive_int(tests_counted, "tests_counted")
    if tests_counted < len(results):
        raise ValueError("tests_counted não pode ser menor que o número de comparações solicitadas.")
    adjusted = []
    for item in results:
        if not isinstance(item, dict):
            raise ValueError("Cada resultado precisa ser um objeto.")
        result = dict(item)
        p_raw = result.get("p_raw")
        if p_raw is None:
            result["p_adjusted"] = None
        else:
            try:
                p = float(p_raw)
            except (TypeError, ValueError) as exc:
                raise ValueError("p_raw precisa estar entre zero e um ou ser nulo.") from exc
            if not math.isfinite(p) or p < 0 or p > 1:
                raise ValueError("p_raw precisa estar entre zero e um ou ser nulo.")
            result["p_adjusted"] = min(1.0, p * tests_counted)
        result["multiple_testing"] = {"method": "Bonferroni family-wise", "tests_counted": tests_counted}
        adjusted.append(result)
    return adjusted
