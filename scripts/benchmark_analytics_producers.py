"""Compara produtores em reconstrução completa e atualização incremental SQLite sintética.

Exemplo: python scripts/benchmark_analytics_producers.py --sessions 20 --minutes 120 --append-sessions 1
O banco é temporário; nenhuma base configurada pelo usuário é aberta.
"""
import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

import numpy as np
import pandas as pd

from mt5_extracao.analytics import producer_jobs, producers, resampling
from mt5_extracao.database_manager import DatabaseManager


INFO = SimpleNamespace(name="BENCH", description="benchmark sintético", path="Synthetic\\BENCH", digits=5,
                       currency_profit="USD")


def synthetic_m1(start_session, session_count, minutes):
    rows = []
    first_day = pd.Timestamp("2026-01-05 09:00:00") + pd.Timedelta(days=int(start_session))
    for session_index in range(int(session_count)):
        time_index = pd.date_range(first_day + pd.Timedelta(days=session_index), periods=int(minutes), freq="min")
        close = 1.1 + (start_session + session_index) * 0.0001 + np.cumsum(
            0.00002 * np.sin(np.arange(int(minutes)) / 9.0) + 0.000003)
        rows.extend({"time": stamp, "open": value - 0.00001, "high": value + 0.00003,
                     "low": value - 0.00003, "close": value, "tick_volume": 20 + index % 7,
                     "spread": 2, "real_volume": 10 + index % 5}
                    for index, (stamp, value) in enumerate(zip(time_index, close)))
    return pd.DataFrame(rows)


def database_bytes(db):
    return producer_jobs._db_bytes(db) or 0


def run_producers(db):
    before = database_bytes(db)
    started = time.perf_counter()
    results = producers.compute(db, "BENCH", "M5")
    wall_ms = int((time.perf_counter() - started) * 1000)
    after = database_bytes(db)
    return {"wall_ms": wall_ms, "database_bytes_before": before, "database_bytes_after": after,
            "database_growth_bytes": max(0, after - before),
            "producer_duration_ms": sum(item["job"]["duration_ms"] for item in results),
            "peak_rss_bytes": max(item["job"]["peak_rss_bytes"] for item in results),
            "rows_inserted": sum(item["inserted"] for item in results),
            "rows_revised": sum(item["revised"] for item in results),
            "rows_unchanged": sum(item["unchanged"] for item in results),
            "families": results}


def worker(mode, db_path, sessions, minutes, append_sessions):
    db = DatabaseManager(db_type="sqlite", db_path=str(db_path), broker_utc_offset=0)
    try:
        if mode == "full":
            history = synthetic_m1(0, sessions + append_sessions, minutes)
            db.save_ohlcv_data("BENCH", "1 minuto", history)
            resampling.resample_symbol(db, "BENCH", ["M5"], symbol_info=INFO)
            measurement = run_producers(db)
            measurement["bars_available"] = len(resampling.latest_bars(db, "BENCH", "M5"))
            return measurement

        initial = synthetic_m1(0, sessions, minutes)
        db.save_ohlcv_data("BENCH", "1 minuto", initial)
        resampling.resample_symbol(db, "BENCH", ["M5"], symbol_info=INFO)
        initial_run = run_producers(db)
        appended = synthetic_m1(sessions, append_sessions, minutes)
        db.save_ohlcv_data("BENCH", "1 minuto", appended)
        resampling.resample_symbol(db, "BENCH", ["M5"], symbol_info=INFO)
        measurement = run_producers(db)
        measurement["bars_available"] = len(resampling.latest_bars(db, "BENCH", "M5"))
        measurement["initial_full_run"] = {key: initial_run[key] for key in
                                            ("wall_ms", "database_growth_bytes", "peak_rss_bytes", "rows_inserted")}
        return measurement
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sessions", type=int, default=20, help="sessões da base antes da carga incremental")
    parser.add_argument("--minutes", type=int, default=120, help="barras M1 sintéticas por sessão")
    parser.add_argument("--append-sessions", type=int, default=1, help="sessões adicionadas no cenário incremental")
    parser.add_argument("--worker", choices=("full", "incremental"))
    parser.add_argument("--database")
    args = parser.parse_args()
    if args.sessions < 8 or args.minutes < 30 or args.append_sessions < 1:
        parser.error("use ao menos 8 sessões, 30 minutos por sessão e uma sessão incremental")
    if args.worker:
        result = worker(args.worker, args.database, args.sessions, args.minutes, args.append_sessions)
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return

    with TemporaryDirectory(prefix="mt5x-analytics-bench-") as temporary:
        reports = {}
        for mode in ("full", "incremental"):
            database = Path(temporary) / f"{mode}.sqlite"
            command = [sys.executable, str(Path(__file__).resolve()), "--worker", mode,
                       "--database", str(database), "--sessions", str(args.sessions),
                       "--minutes", str(args.minutes), "--append-sessions", str(args.append_sessions)]
            completed = subprocess.run(command, check=True, capture_output=True, text=True)
            reports[mode] = json.loads(completed.stdout)
        reports["comparison"] = {
            "history_bars_full": reports["full"]["bars_available"],
            "history_bars_incremental": reports["incremental"]["bars_available"],
            "producer_speedup_vs_full": round(reports["full"]["wall_ms"] /
                                                max(reports["incremental"]["wall_ms"], 1), 2),
            "incremental_history_fraction": round(args.append_sessions / (args.sessions + args.append_sessions), 4),
        }
        print(json.dumps(reports, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
