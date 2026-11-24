"""Produz o snapshot M1 sintético e a definição Ultra usados no ensaio reproduzível."""
import argparse
import json
from pathlib import Path
import sys
from types import SimpleNamespace

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from mt5_extracao.analytics import datasets, instruments
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.strategies import get_strategy
from mt5_extracao.strategies.specs import spec_from_symbol_info


def prepare(db):
    symbol = "QA_ULTRA"
    origin = pd.Timestamp("2026-10-05T09:00Z")
    prices = [100] * 32 + [104, 108, 103, 97, 93, 97, 103, 108, 111,
                            108, 103, 98, 94, 90, 94, 101, 107, 110, 109,
                            105, 100, 97, 93, 92, 93, 100, 108, 109, 108, 104]
    ticks = []
    bars = []
    previous = prices[0]
    for i, close in enumerate(prices):
        start = origin + pd.Timedelta(minutes=i)
        opening = {48: previous - 6, 53: previous + 5, 60: previous - 3}.get(i, previous)
        quotes = [opening, opening, opening, opening, close, close]
        for second, bid in zip((0, 1, 2, 3, 30, 55), quotes):
            ticks.append(dict(time_msc=int(start.value // 1_000_000) + second * 1000,
                              bid=bid, ask=bid + 1, last=0, flags=6, volume=0, volume_real=0))
        bars.append(dict(time=start.tz_localize(None), open=opening, high=max(quotes), low=min(quotes),
                         close=close, spread=1, tick_volume=len(quotes), real_volume=0))
        previous = close
    contract = spec_from_symbol_info(SimpleNamespace(point=1, digits=0, trade_tick_size=1, trade_tick_value=1,
                                  volume_min=1, volume_max=10, volume_step=1, trade_contract_size=1,
                                  currency_profit="USD"))
    instruments.record(db, symbol, "specification", contract, source="synthetic_contract",
                       valid_from="2026-10-01T00:00Z", known_at="2026-09-30T00:00Z")
    db.save_ohlcv_data(symbol, "1 minuto", pd.DataFrame(bars))
    db.save_ticks(db.get_table_name_for_symbol(symbol, "ticks"), pd.DataFrame(ticks))
    snapshot = datasets.capture(db, symbol, "M1", families=["ticks"],
                                source={"provider": "synthetic", "server": "QA", "derivation": "bid_ticks_m1"})
    modules = []
    for owner, key, changes in [
            ("A", "donchian", dict(entry_period=3, exit_period=2, atr_avg=3, adx_min=1)),
            ("B", "s09_reference", dict(fast=2, slow=4, warmup_bars=6, stop_points=3, target_points=8))]:
        modules.append(dict(owner=owner, strategy=key, params=get_strategy(key, **changes).params,
                            steps=2, stop=3, target=8, breakeven=4 if owner == "A" else 0,
                            invalidate=3, ttl_ms=60000, max_losses=10 if owner == "A" else 2, daily_goal=100000))
    return dict(version=1, manifest_id=snapshot.manifest_id, sample_kind="synthetic", currency="USD",
                session="UTC_day", account_mode="modeled_hedging", modules=modules, cash=100000,
                margin=24700, fee=5, risk_limit=3000, fill_steps_per_tick=1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", required=True, type=Path, help="diretório novo e exclusivo da demonstração")
    args = parser.parse_args()
    args.directory.mkdir(parents=True, exist_ok=False)
    db = DatabaseManager(db_path=str(args.directory / "demo.sqlite"), broker_utc_offset=0)
    try:
        definition = prepare(db)
        (args.directory / "campaign.json").write_text(json.dumps(definition, indent=2) + "\n", encoding="utf-8")
        datasets.export_snapshot(db, definition["manifest_id"], args.directory / "snapshot.json")
    finally:
        db.close()


if __name__ == "__main__":
    main()
