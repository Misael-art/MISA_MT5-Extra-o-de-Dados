"""Adaptador de leitura para o Laboratório; nunca usa saída/PnL como contexto."""
import json

import pandas as pd

from mt5_extracao.analytics import datasets, indicator_context, indicator_materializer, indicator_registry, resampling
from mt5_extracao.trade_analytics.trajectories import store
from mt5_extracao.trade_analytics.trajectories.clocks import Clock


def for_trades(db, trades, *, feature_ids=None, clocks=None):
    """Contexto anterior à entrada, com clock declarado ou sidecar conciliado.

    `clocks` permite um relógio explicitamente declarado pelo consumidor. Runs
    com snapshot fixado não recebem dados de outro conjunto: a ausência dessa
    vinculação é devolvida como estado, preservando a análise legada.
    """
    ids = indicator_materializer.selected_features(feature_ids)
    frame = trades[["run_id", "seq", "symbol", "timeframe", "entry_time"]].copy().reset_index(drop=True)
    frame["decision_time"] = pd.Series([None] * len(frame), dtype="object")
    frame["context_state"], frame["context_reason"] = "UNAVAILABLE", "CLOCK_REQUIRED"
    frame["run_manifest_id"] = None
    parts = []
    for run_id, group in frame.groupby("run_id", sort=False):
        snapshot = datasets.for_run(db, run_id)
        if snapshot is not None:
            frame.loc[group.index, "run_manifest_id"] = snapshot.manifest_id
            frame.loc[group.index, "context_reason"] = "MATERIALIZATION_DATASET_BINDING_REQUIRED"
            continue
        clock = (clocks or {}).get(run_id)
        if clock is None:
            try:
                trajectory = store.load(db, run_id)
                clock = Clock(**trajectory["clock"]) if trajectory is not None else None
            except ValueError:
                frame.loc[group.index, "context_reason"] = "INVALID_TRAJECTORY_CLOCK_OR_PROVENANCE"
                continue
        for index, row in group.iterrows():
            if pd.isna(row.entry_time):
                frame.loc[index, "context_reason"] = "ENTRY_TIME_REQUIRED"
                continue
            stamp = pd.Timestamp(row.entry_time)
            if clock is None and stamp.tzinfo is None:
                continue
            try:
                decision = clock.timestamp(stamp) if clock is not None else stamp.tz_convert("UTC")
            except (ValueError, TypeError):
                frame.loc[index, "context_reason"] = "INVALID_ENTRY_CLOCK"
                continue
            frame.loc[index, "decision_time"] = decision
            frame.loc[index, ["context_state", "context_reason"]] = ["DECLARED_CLOCK", None]
    valid = frame[frame.context_state == "DECLARED_CLOCK"]
    for (symbol, timeframe), group in valid.groupby(["symbol", "timeframe"], sort=False):
        try:
            resampling.parse_target(timeframe)
        except ValueError:
            frame.loc[group.index, "context_state"] = "UNAVAILABLE"
            frame.loc[group.index, "context_reason"] = "DERIVED_TIMEFRAME_REQUIRED"
            continue
        query = group[["decision_time"]].copy()
        query["_trade_row"] = group.index
        part = indicator_context.context(db, query, symbol=symbol, timeframe=timeframe, feature_ids=ids)
        parts.append(part.set_index("_trade_row"))
    implementations = indicator_materializer._implementation_signatures(ids)
    columns = {}
    for ident in ids:
        spec = indicator_registry.get_definition(ident)
        for field in ("value", "state", "reason", "algorithm_version", "unit", "parameters", "observation_time",
                      "available_at_utc", "valid_until_utc", "source_revision", "metadata_json"):
            name = ident + "::" + field
            columns[name] = pd.Series([None] * len(frame), dtype="object")
        columns[ident + "::state"] = "UNAVAILABLE"
        columns[ident + "::reason"] = list(frame.context_reason)
        columns[ident + "::algorithm_version"] = indicator_materializer.algorithm_version(ident, implementations[ident])
        columns[ident + "::unit"] = spec["unit"]
        columns[ident + "::parameters"] = json.dumps(spec["parameters"], sort_keys=True, separators=(",", ":"))
    output = pd.DataFrame(columns, index=frame.index)
    if parts:
        extra = pd.concat(parts).drop(columns=["symbol", "tf", "decision_time"], errors="ignore")
        output.loc[extra.index, extra.columns] = extra
    return pd.concat([frame.drop(columns="entry_time"), output], axis=1)
