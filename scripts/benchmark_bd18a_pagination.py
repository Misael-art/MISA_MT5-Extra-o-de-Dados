"""Mede a paginação BD18a (``query_service.series``) com volume sintético em SQLite e PostgreSQL.

Exemplo:
    python scripts/benchmark_bd18a_pagination.py --facts 5000 50000 200000 --backend sqlite
    MT5X_TEST_PG_URL=postgresql+psycopg://... python scripts/benchmark_bd18a_pagination.py --backend postgresql

Cada volume usa um banco novo (SQLite temporário ou schema PostgreSQL isolado, removido no fim);
nenhuma base configurada pelo usuário é aberta. Mede primeira página, página seguinte pelo cursor e
pico de alocações Python (tracemalloc, chamada separada). Não mede a memória interna do banco.
Saída: JSON com o SHA do App, backend, volume e medianas.
"""
import argparse
import gc
import json
import os
import statistics
import subprocess
import sys
import time
import tracemalloc
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd

from mt5_extracao.analytics import datasets, feature_store, query_service
from mt5_extracao.database_manager import DatabaseManager

FEATURE = "bench.metric"
START = pd.Timestamp("2025-01-01T00:00Z")


@contextmanager
def open_db(backend, pg_url):
    if backend == "sqlite":
        with TemporaryDirectory(prefix="bd18a-bench-") as folder:
            db = DatabaseManager(db_path=str(Path(folder) / "bench.sqlite"), broker_utc_offset=0)
            try:
                yield db
            finally:
                db.close()
    else:
        from tests.postgres_helpers import isolated_postgresql_database
        with isolated_postgresql_database(pg_url, broker_utc_offset=0) as db:
            yield db


def seed(db, facts):
    datasets.declare_source(db, {"provider": "bench", "server": "Synthetic"})
    feature_store.register_definition(db, FEATURE, "Série de bancada", "1", {"period": 1}, ["bars:M1"],
        family="bench", unit="fraction", value_type="NUMBER", warmup_count=0, warmup_unit="bars",
        availability_rule="declared available_at")
    for begin in range(0, facts, 20_000):
        records = []
        for i in range(begin, min(facts, begin + 20_000)):
            observation = START + pd.Timedelta(minutes=i)
            records.append(dict(symbol="BENCH", timeframe="M1", feature_id=FEATURE, algorithm_version="1",
                                observation_time=observation, value=float(i % 97) / 97,
                                available_at=observation + pd.Timedelta(minutes=1),
                                observed_at=observation + pd.Timedelta(minutes=1)))
        feature_store.save_values(db, records)


def timed(call, repeats):
    samples = []
    for _ in range(repeats):
        gc.collect()
        began = time.perf_counter()
        result = call()
        samples.append((time.perf_counter() - began) * 1000)
    return result, samples


def measure(db, facts, page_size, repeats):
    as_of = (START + pd.Timedelta(minutes=facts + 2)).isoformat()
    query = dict(symbol="BENCH", timeframe="M1", feature_ids=[FEATURE], as_of=as_of, page_size=page_size)
    first = query_service.series(db, **query)
    if first.get("status") != "ok" or len(first["values"]) != min(page_size, facts):
        raise RuntimeError(f"Consulta de bancada falhou: {json.dumps(first)[:400]}")
    cursor = first["page"]["next_cursor"]
    query_service.series(db, **query)                       # aquecimento
    _, first_ms = timed(lambda: query_service.series(db, **query), repeats)
    second, next_ms = timed(lambda: query_service.series(db, **query, cursor=cursor), repeats)
    if cursor and second.get("status") != "ok":
        raise RuntimeError(f"Página pelo cursor falhou: {json.dumps(second)[:400]}")
    gc.collect()
    tracemalloc.start()
    query_service.series(db, **query)
    peak = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()
    return dict(facts=facts, page_size=page_size, repeats=repeats,
                first_page_ms_median=round(statistics.median(first_ms), 2),
                first_page_ms_max=round(max(first_ms), 2),
                next_page_ms_median=round(statistics.median(next_ms), 2),
                python_peak_bytes=peak)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--facts", type=int, nargs="+", default=[5_000, 50_000])
    parser.add_argument("--backend", choices=("sqlite", "postgresql"), default="sqlite")
    parser.add_argument("--page-size", type=int, default=500)
    parser.add_argument("--repeats", type=int, default=5)
    args = parser.parse_args(argv)
    pg_url = os.environ.get("MT5X_TEST_PG_URL")
    if args.backend == "postgresql" and not pg_url:
        parser.error("defina MT5X_TEST_PG_URL (servidor de QA, nunca a base do proprietário)")
    sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=PROJECT_ROOT, capture_output=True, text=True).stdout.strip()
    rows = []
    for facts in args.facts:
        with open_db(args.backend, pg_url) as db:
            began = time.perf_counter()
            seed(db, facts)
            row = measure(db, facts, args.page_size, args.repeats)
            row["seed_seconds"] = round(time.perf_counter() - began, 1)
        rows.append(row)
        print(json.dumps(row), file=sys.stderr)
    print(json.dumps(dict(app_sha=sha, backend=args.backend, python=sys.version.split()[0], results=rows), indent=2))


if __name__ == "__main__":
    main()
