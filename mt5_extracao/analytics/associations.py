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


def _pair_signature():
    import platform
    import scipy
    from pathlib import Path
    from mt5_extracao import timeframes
    from mt5_extracao.analytics import data_eligibility, datasets, feature_store, indicator_context, indicator_materializer, instruments, relation_dto
    source = indicator_materializer._source_components
    components = dict(scheme="frozen_pair_v1", relation_schema=relation_dto.PAIR_RELATION_SCHEMA_VERSION,
        engine=source(__file__, ("_pair_signature", "_pair_grid", "_pair_sessions", "_pair_context", "_pair_lineage",
            "_pair_sample_hash", "query_frozen_pair", "analyze_association", "_result_base", "_numeric_column",
            "_ranked_standardized", "_moving_block_indices", "_correlation", "_positive_int", "_scope",
            "_utc_timestamp", "_utc_series", "adjust_bonferroni")),
        context=source(indicator_context.__file__, ("from_snapshot", "_snapshot_context", "_snapshot_scope",
            "_snapshot_observations", "_context_rows", "_state", "_value", "_ids", "_version", "records")),
        store=source(feature_store.__file__, ("asof_join", "_utc_timestamp", "_canonical_json")),
        dto=source(relation_dto.__file__, ("build_pair_relation_dto", "_validate_relation", "_required_text",
            "_finite_optional_number", "_utc_timestamp", "_json_ready", "pair_analysis_id", "pair_contract_reason")),
        snapshot=indicator_materializer._object_source_component(datasets.Snapshot),
        snapshot_reader=hashlib.sha256(Path(datasets.__file__).read_bytes()).hexdigest(),
        eligibility=hashlib.sha256(Path(data_eligibility.__file__).read_bytes()).hexdigest(),
        broker_time=source(instruments.__file__, ("broker_utc",)), timeframe=source(timeframes.__file__, ("parse", "_norm")),
        python=platform.python_version(), numpy=np.__version__, pandas=pd.__version__, scipy=scipy.__version__)
    return hashlib.sha256(feature_store._canonical_json(components, "pair implementation").encode()).hexdigest()


def _pair_grid(snapshot, cutoff):
    from mt5_extracao import timeframes
    from mt5_extracao.analytics import instruments
    manifest = snapshot.manifest
    tf = timeframes.parse(manifest["timeframe"])
    bars = snapshot.bars
    starts = pd.Series([instruments.broker_utc(stamp, manifest["time"]["broker_utc_offset_hours"])
                        for stamp in bars.time], dtype="datetime64[ns, UTC]")
    step = pd.offsets.MonthBegin(1) if tf.name == "MN1" else pd.Timedelta(minutes=tf.minutes)
    ends = starts + step
    if starts.duplicated().any() or not starts.is_monotonic_increasing:
        raise ValueError("Grade congelada duplicada ou fora de ordem; revise a captura antes de relacionar.")
    positions = np.flatnonzero((ends <= cutoff).to_numpy())
    return pd.DataFrame(dict(start=starts.iloc[positions].array, at=ends.iloc[positions].array)), step, bars.iloc[positions]


def _pair_sessions(snapshot, times, starts, cutoff, guard):
    """Somente segmentos do calendário congelado cuja revisão era conhecida no corte."""
    calendar = snapshot.market_calendar
    output = pd.Series([None] * len(times), dtype=object)
    if calendar.empty or not len(times):
        return output, []
    calendar = calendar.copy()
    calendar["known"] = _utc_series(calendar.observed_at_utc, "calendar.observed_at_utc")
    calendar = calendar.loc[calendar.known <= cutoff].sort_values(["known", "revision"]).drop_duplicates("session_id", keep="last")
    intervals, labels, receipts = [], [], []
    for row in calendar.itertuples(index=False):
        guard()
        segments = json.loads(row.segments_json)
        used = False
        for segment in segments:
            first, last = (_utc_timestamp(segment[key], "calendar." + key) for key in ("open_utc", "close_utc"))
            if first >= last:
                raise ValueError("Segmento de sessão congelada sem intervalo válido.")
            if last < times.min() or first >= times.max():
                continue
            if float(row.timezone_offset_hours) != float(snapshot.manifest["time"]["broker_utc_offset_hours"]):
                raise ValueError("Calendário congelado usa outro contexto de fuso; reconcilie a origem.")
            used = True
            intervals.append(pd.Interval(first, last, closed="right"));labels.append(row.session_id)
        if used:
            receipts.append(dict(session_id=row.session_id, revision=int(row.revision), input_hash=row.input_hash,
                                 known_at_utc=row.known.isoformat(), source=row.source))
    if intervals:
        index = pd.IntervalIndex(intervals)
        if index.is_overlapping:
            raise ValueError("Segmentos de sessões sobrepostos; a população seria ambígua.")
        for position, match in enumerate(index.get_indexer(times)):
            if match >= 0 and pd.notna(starts.iloc[position]) and starts.iloc[position] >= index[match].left:
                output.iloc[position] = labels[match]
    return output, receipts


def _pair_context(snapshot, pair, times, present, guard):
    from mt5_extracao.analytics import indicator_context
    columns = ("value", "state", "reason", "observation_time", "available_at_utc", "valid_until_utc",
               "revision", "source_revision", "metadata_json")
    result = pd.DataFrame({key: pd.Series([None] * len(times), dtype=object) for key in columns})
    result["state"], result["reason"] = "UNAVAILABLE", "NO_REFERENCE_BAR"
    positions = np.flatnonzero(present.to_numpy())
    if len(positions):
        guard()
        context = indicator_context.from_snapshot(snapshot, pd.DataFrame(dict(decision_time=times.iloc[positions].array)),
            feature_ids=[pair[0]], algorithm_version=pair[1])
        for key in columns:
            result.loc[positions, key] = context[pair[0] + "::" + key].astype(object).to_numpy()
    guard()
    return result


def _pair_lineage(db, snapshot, pair, metadata, cutoff, guard, images=None):
    """Lê origens imutáveis da materialização; nunca completa com contratos/fatos live."""
    from mt5_extracao.analytics import datasets
    images = {snapshot.manifest_id: snapshot} if images is None else images
    visited, visiting = set(), set()
    identity = snapshot.manifest
    def image(ident):
        if ident not in images:
            if len(images) >= 32:
                raise ValueError("Mais de32 snapshots de origem; reduza a janela da relação.")
            guard();images[ident] = datasets.load(db, ident);guard()
        other = images[ident].manifest
        if any(other[key] != identity[key] for key in ("symbol", "timeframe", "source", "time")):
            raise ValueError("Origem materializada pertence a outro contexto/base.")
        return images[ident]
    def visit(current, chosen, selected_metadata=None):
        key = current.manifest_id, tuple(chosen)
        if key in visiting:
            raise ValueError("Ciclo na proveniência congelada.")
        if key in visited:
            return
        visiting.add(key);guard()
        definitions = {(item["feature_id"], item["algorithm_version"]): item for item in current.feature_definitions}
        spec = definitions.get(tuple(chosen))
        if spec is None:
            raise ValueError("Contrato ancestral não foi congelado.")
        if spec.get("family") == "structure" or chosen[0].startswith("structure."):
            raise ValueError("UNVERIFIED_STORED_EVENT: confirmação/identidade do pivô não qualificada.")
        parameters = spec.get("parameters") or {}
        if spec.get("family") == "frozen_composite":
            for parent in parameters["composition"]["inputs"]:
                visit(current, (parent["feature_id"], parent["algorithm_version"]))
        elif spec.get("family") == "materialized_composite":
            contract = parameters["materialization"]
            if selected_metadata is None:
                history = current.feature_history
                selected_metadata = history.loc[(history.feature_id == chosen[0]) &
                    (history.algorithm_version == chosen[1]) & (history.available_at_utc <= cutoff), "metadata_json"]
            if selected_metadata.isna().any():
                raise ValueError("Proveniência materializada ausente.")
            for raw in set(selected_metadata.dropna()):
                record = json.loads(raw)
                if record.get("basis") != "materialized_frozen_context":
                    raise ValueError("Proveniência materializada ausente.")
                origin = image(record["materialization"]["source_manifest_id"])
                target = chosen[0], contract["recipe_version"]
                original = next((item for item in origin.feature_definitions if
                    (item["feature_id"], item["algorithm_version"]) == target), None)
                if original is None or original["parameters"].get("composition") != contract["recipe"]:
                    raise ValueError("Receita ancestral diverge da versão materializada.")
                visit(origin, target)
        visiting.remove(key);visited.add(key)
    visit(snapshot, pair, metadata)
    return sorted(images)


def _pair_sample_hash(frame, guard):
    from mt5_extracao.analytics import feature_store, indicator_context
    digest = hashlib.sha256()
    for start in range(0, len(frame), 256):
        guard()
        for row in indicator_context.records(frame.iloc[start:start+256]):
            digest.update(feature_store._canonical_json(row, "pair sample").encode());digest.update(b"\n")
    guard()
    return digest.hexdigest()


def query_frozen_pair(db, manifest_id, *, left, right, as_of, lag_bars=0, method="pearson", minimum_pairs=30,
                      minimum_sessions=10, bootstrap_replicates=999, seed=7, block_length_sessions=None,
                      tests_counted=None, synthetic_fixture=False, timeout_seconds=120, max_memory_mb=2048,
                      cancel_check=None):
    """Associação referencial entre duas versões congeladas, sem persistência ou ordem."""
    import time
    from datetime import datetime, timezone
    from mt5_extracao import timeframes
    from mt5_extracao.analytics import data_eligibility, datasets, feature_store, indicator_materializer, producers, query_service, relation_dto
    pairs = [datasets._feature_selection([pair])[0] for pair in (left, right)]
    cutoff = _utc_timestamp(as_of, "as_of")
    if cutoff > pd.Timestamp.now(tz="UTC"):
        raise ValueError("Corte futuro não certifica recebimentos; escolha as_of já ocorrido.")
    if (isinstance(lag_bars, bool) or not isinstance(lag_bars, int) or not 0 <= lag_bars <= 10000
            or not isinstance(synthetic_fixture, bool)):
        raise ValueError("Confira lag_bars (0 a10000) e synthetic_fixture.")
    for value, label in ((minimum_pairs, "minimum_pairs"), (minimum_sessions, "minimum_sessions"),
                         (bootstrap_replicates, "bootstrap_replicates")):
        _positive_int(value, label)
    if minimum_pairs < 3:
        raise ValueError("minimum_pairs precisa ser pelo menos3 na consulta referencial.")
    if bootstrap_replicates > 5000 or not isinstance(seed, int) or isinstance(seed, bool):
        raise ValueError("Use até5000 réplicas e seed inteiro.")
    if block_length_sessions is not None:_positive_int(block_length_sessions, "block_length_sessions")
    if tests_counted is not None:_positive_int(tests_counted, "tests_counted")
    if (isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)) or
            not 0 < timeout_seconds <= producers.MAX_JOB_TIMEOUT_SECONDS or isinstance(max_memory_mb, bool) or
            not isinstance(max_memory_mb, (int, float)) or not 0 < max_memory_mb <= producers.MAX_JOB_MEMORY_MB):
        raise ValueError("Timeout/memória fora dos limites existentes dos jobs Analytics.")
    method = str(method).lower()
    if method not in ("pearson", "spearman"):
        raise ValueError("Use pearson ou spearman para pares numéricos.")
    run = dict(started_monotonic=time.monotonic())
    def guard():
        if cancel_check is not None and cancel_check():
            raise indicator_materializer.Cancelled("Consulta de relações cancelada; nenhum resultado parcial publicado.")
        producers._check_limits(run, timeout_seconds, max_memory_mb)
    guard();snapshot = datasets.load(db, manifest_id);guard()
    manifest = snapshot.manifest
    definitions = {(item["feature_id"], item["algorithm_version"]): item for item in snapshot.feature_definitions}
    if any(pair not in definitions for pair in pairs):
        raise ValueError("Versão não congelada; capture os dois contratos antes de relacionar.")
    descriptors = [dict(feature_id=pair[0], algorithm_version=pair[1], unit=definitions[pair].get("unit"),
        value_type=definitions[pair].get("value_type"), transform="identity", definition=definitions[pair]) for pair in pairs]
    grid, step, bars = _pair_grid(snapshot, cutoff)
    if len(grid)*2 > query_service.MAX_SELECTION_ROWS:
        raise ValueError("Par excede o limite de indicador/decisão; reduza a janela congelada.")
    times = grid["at"]
    reference = times.copy() if lag_bars == 0 else times - (pd.offsets.MonthBegin(lag_bars)
        if timeframes.parse(manifest["timeframe"]).name == "MN1" else step*lag_bars)
    present = reference.isin(times)
    x = _pair_context(snapshot, pairs[0], reference, present, guard)
    y = _pair_context(snapshot, pairs[1], times, pd.Series(True, index=grid.index), guard)
    sessions, calendar_proof = _pair_sessions(snapshot, times, grid.start, cutoff, guard)
    starts_by_end = dict(zip(times, grid.start))
    reference_starts = reference.map(starts_by_end)
    previous_sessions, _ = _pair_sessions(snapshot, reference, reference_starts, cutoff, guard)
    sample = pd.DataFrame(dict(symbol=manifest["symbol"], timeframe=manifest["timeframe"],
        decision_time=times, reference_time=reference, session_id=sessions, x_value=x.value, y_value=y.value,
        x_available_at_utc=x.available_at_utc, y_available_at_utc=y.available_at_utc))
    for prefix, context in (("x", x), ("y", y)):
        for field in ("observation_time", "revision", "source_revision", "state", "reason"):
            sample[prefix+"_"+field] = context[field]
    eligible = pd.Series(True, index=sample.index)
    exclusions = {}
    sample["exclusion"] = None
    def reject(code, mask):
        take = eligible & mask
        if take.any():
            exclusions[code] = int(take.sum());sample.loc[take,"exclusion"] = code;eligible.loc[take] = False
    check = data_eligibility.evaluate(snapshot.quality, ("ohlcv",))
    prefix_quality = data_eligibility.evaluate(data_eligibility.assess(bars, bar_minutes=timeframes.parse(manifest["timeframe"]).minutes or 1440), ("ohlcv",))
    blocked = None
    if not check["eligible"] or not prefix_quality["eligible"]:blocked = "INELIGIBLE_DATA"
    elif (contract_reason := relation_dto.pair_contract_reason(descriptors)) is not None:blocked = contract_reason
    elif pairs[0] == pairs[1] and lag_bars == 0:blocked = "SAME_SERIES"
    lineage = {snapshot.manifest_id}
    lineage_images = {snapshot.manifest_id: snapshot}
    lineage_diagnostics = []
    if blocked is None:
        for pair, context in zip(pairs, (x,y)):
            try:
                lineage.update(_pair_lineage(db, snapshot, pair, context.loc[context.state == "AVAILABLE","metadata_json"], cutoff, guard, lineage_images))
            except (KeyError, TypeError, ValueError) as exc:
                blocked = "UNVERIFIED_STORED_EVENT" if "UNVERIFIED_STORED_EVENT" in str(exc) else "UNVERIFIED_MATERIALIZED_LINEAGE"
                lineage_diagnostics.append(dict(feature_id=pair[0], algorithm_version=pair[1], reason=str(exc)))
                break
    if blocked is not None:reject(blocked, pd.Series(True,index=sample.index))
    reject("NO_REFERENCE_BAR", ~present)
    reject("SESSION_UNAVAILABLE_AS_OF", sessions.isna() | previous_sessions.isna())
    reject("CROSS_SESSION_LAG", sessions.ne(previous_sessions))
    reject("LEFT_UNAVAILABLE", x.state.ne("AVAILABLE") | x.value.isna())
    reject("RIGHT_UNAVAILABLE", y.state.ne("AVAILABLE") | y.value.isna())
    reject("FUTURE_OBSERVATION", (pd.to_datetime(x.observation_time,utc=True) > reference) |
                                   (pd.to_datetime(y.observation_time,utc=True) > times))
    unique_fields = [prefix+"_"+field for prefix in ("x","y") for field in ("observation_time","revision","source_revision")]
    distinct = int(sample.loc[eligible,unique_fields].drop_duplicates().shape[0])
    result = analyze_association(sample.loc[eligible], method=method, as_of=cutoff, minimum_pairs=minimum_pairs,
        minimum_sessions=minimum_sessions, bootstrap_replicates=bootstrap_replicates, seed=seed,
        block_length_sessions=block_length_sessions, guard=guard) if distinct >= minimum_pairs else _result_base(FEATURE_PAIR, method, cutoff, dict(symbol=manifest["symbol"],timeframe=manifest["timeframe"]))
    if distinct < minimum_pairs:
        result.update(n_total=int(eligible.sum()), n_pairs=int(eligible.sum()), n_sessions=int(sessions.loc[eligible].nunique()),
            reason_codes=[blocked] if blocked else list(exclusions) if not eligible.any() and exclusions else
                         ["MINIMUM_DISTINCT_SOURCE_PAIRS_NOT_MET"])
    if tests_counted is not None:result = adjust_bonferroni([result],tests_counted=tests_counted)[0]
    sample_hash = _pair_sample_hash(sample, guard)
    implementation = _pair_signature();guard()
    settings = dict(left=list(pairs[0]), right=list(pairs[1]), as_of=cutoff.isoformat(), lag_bars=lag_bars, method=method,
        minimum_pairs=minimum_pairs, minimum_sessions=minimum_sessions, bootstrap_replicates=bootstrap_replicates,
        seed=seed, block_length_sessions=block_length_sessions, tests_counted=tests_counted, synthetic_fixture=synthetic_fixture)
    analysis_id = relation_dto.pair_analysis_id(manifest_id, settings, sample_hash, implementation, lineage)
    population = dict(n_total=len(sample), n_eligible=int(eligible.sum()), n_distinct_source_pairs=distinct, exclusions=exclusions,
        n_left_sources=int(sample.loc[eligible,["x_"+f for f in ("observation_time","revision","source_revision")]].drop_duplicates().shape[0]),
        n_right_sources=int(sample.loc[eligible,["y_"+f for f in ("observation_time","revision","source_revision")]].drop_duplicates().shape[0]),
        quality=dict(frozen=check,prefix=prefix_quality), statistics=result)
    relation = dict(method=method,effect=result["effect"],effect_ci_low=result["ci_low"],effect_ci_high=result["ci_high"],
        p_raw=result["p_raw"],p_adjusted=result["p_adjusted"],n_pairs=result["n_pairs"],n_sessions=result["n_sessions"],
        evidence_state="EXPLORATORY" if result["status"]=="OK" else "INSUFFICIENT", utility_state="INCONCLUSIVE",
        freshness_state="FROZEN_AS_OF",state=result["status"],reason="; ".join(result["reason_codes"]) or None)
    return relation_dto.build_pair_relation_dto(left=descriptors[0],right=descriptors[1],relation=relation,
        scope=dict(symbol=manifest["symbol"],timeframe=manifest["timeframe"],lag_bars=lag_bars,
            lag_rule="left at right_close minus nominal lag; missing reference bars and cross-session pairs excluded",
            settings=settings),population=population,
        provenance=dict(base_namespace=manifest["source"]["namespace"],source=manifest["source"],snapshot_manifest_id=manifest_id,
            lineage_manifests=sorted(lineage),lineage_diagnostics=lineage_diagnostics,
            calendar=calendar_proof,as_of_utc=cutoff.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            analysis_id=analysis_id,sample_hash=sample_hash,implementation_sha256=implementation,
            input_basis="frozen_snapshot",limits=dict(timeout_seconds=timeout_seconds,max_memory_mb=max_memory_mb,
                max_pairs=query_service.MAX_SELECTION_ROWS//2,max_lineage_snapshots=32),
            limitations=["Associação descritiva, sem causalidade, ganho ou sinal operacional.",
                "Fontes retidas e autocorrelação não são observações independentes; contagem distinta não é n efetivo certificado.",
                "Inferência por blocos depende de hipóteses temporais; seleção de testes e utilidade financeira não qualificadas.",
                "Nenhuma direção, normalização adicional ou calendário é inferido; versões/recebimentos são os declarados no snapshot."]),
        generated_at_utc=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),synthetic_fixture=synthetic_fixture)



def persist_frozen_pair(db, manifest_id, *, left, right, as_of, lag_bars=0, method="pearson", minimum_pairs=30,
                        minimum_sessions=10, bootstrap_replicates=999, seed=7, block_length_sessions=None,
                        tests_counted=None, synthetic_fixture=False, timeout_seconds=120, max_memory_mb=2048,
                        cancel_check=None, write_guard=None):
    """Calcula pelo serviço canônico e publica um único artefato imutável."""
    import time
    from mt5_extracao.analytics import feature_store, indicator_materializer, producers
    run = dict(started_monotonic=time.monotonic())
    def guard(connection=None):
        if cancel_check is not None and cancel_check():
            raise indicator_materializer.Cancelled("Persistência de relação cancelada; nenhum artefato parcial publicado.")
        if write_guard is not None and (reason := write_guard()):
            raise producers.WriteBlocked(f"Persistência de relação bloqueada: {reason}.")
        # A consulta valida os limites antes de chamar o guarda de recursos.
        if connection is not None:
            producers._check_limits(run, timeout_seconds, max_memory_mb)
    guard()
    dto = query_frozen_pair(db, manifest_id, left=left, right=right, as_of=as_of, lag_bars=lag_bars,
        method=method, minimum_pairs=minimum_pairs, minimum_sessions=minimum_sessions,
        bootstrap_replicates=bootstrap_replicates, seed=seed, block_length_sessions=block_length_sessions,
        tests_counted=tests_counted, synthetic_fixture=synthetic_fixture, timeout_seconds=timeout_seconds,
        max_memory_mb=max_memory_mb, cancel_check=cancel_check)
    return feature_store.save_pair_relation(db, dto, transaction_guard=guard)
