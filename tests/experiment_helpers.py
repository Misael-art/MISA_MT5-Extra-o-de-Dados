"""Dados determinísticos privados para o journal, sem conexão MT5."""
from dataclasses import asdict

from mt5_extracao.analytics import datasets
from mt5_extracao.composition import base_composition
from mt5_extracao.robots.budget import Limits
from tests.market_data import trend

original_run = None


def process_crash(*args, **kwargs):
    """Somente processos Python dos testes; código idêntico na recuperação."""
    import os
    if os.environ.get("MT5X_EXPERIMENT_CRASH_PHASE") == "before":
        os._exit(23)
    result = original_run(*args, **kwargs)
    if os.environ.get("MT5X_EXPERIMENT_CRASH_PHASE") == "after":
        os._exit(24)
    return result


def setup(db, name="study"):
    bars = trend(630, freq="h", start="2026-01-05")
    db.save_ohlcv_data("QA", "1 hora", bars)
    roles = {}
    for name_role, end, start in (("train", 359, 300), ("validation", 479, 360), ("holdout", 629, 480)):
        snap = datasets.capture(db, "QA", "H1", end=bars.time.iloc[end])
        roles[name_role] = dict(data_manifest_id=snap.manifest_id, evaluation_start=bars.time.iloc[start].isoformat())
    definition = dict(schema_version=1, name=name, revision=1,
        criteria=dict(id="declared_legacy", version=1), objective=dict(id="total_r", version=1), grid=[], seed=42,
        budget=dict(scope="evaluation_request", limits=asdict(Limits(min_free_mb=0))), roles=roles)
    composition = base_composition("ema_atr", symbol="QA", timeframe="H1", risk_fraction=.003)
    return definition, composition, bars
