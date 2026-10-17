"""Estudo retrospectivo limitado. Não conecta MT5, não opera contas, não aprova candidatos.

python -m mt5_extracao.strategies.pf19_study --help
"""
import argparse
import dataclasses
import hashlib
import itertools
import os
import json
import math
from pathlib import Path
import sqlite3
import subprocess
import sys
import time

import numpy as np
import pandas as pd

from mt5_extracao import profit_factor

MODULE = "mt5_extracao.strategies.pf19_study"


def _json(value):
    if isinstance(value, dict):
        return {str(k): _json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json(v) for v in value]
    if isinstance(value, (pd.Timestamp, pd.Period)):
        return str(value)
    if isinstance(value, np.generic):
        return _json(value.item())
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _write(path, value):
    Path(path).write_text(json.dumps(_json(value), ensure_ascii=False, indent=2, allow_nan=False) + "\n",
                          encoding="utf-8")


def pf_interval(pnl, days, sessions, block=5, resamples=2000, seed=20261016):
    """Reamostra GP/GL dos trades em blocos de sessões; não classifica o saldo diário."""
    if block < 1 or resamples < 1:
        raise ValueError("Bloco e reamostragens precisam ser positivos.")
    values = np.asarray(pnl, dtype=float)
    if len(values) != len(days) or not np.isfinite(values).all():
        raise ValueError("PnLs finitos e dias alinhados são obrigatórios.")
    index = pd.DatetimeIndex(pd.to_datetime(sessions)).normalize().unique().sort_values()
    trade_days = pd.DatetimeIndex(pd.to_datetime(days)).normalize()
    if not trade_days.isin(index).all():
        raise ValueError("Toda operação precisa pertencer a uma sessão observada.")
    daily = pd.DataFrame({"gp": np.maximum(values, 0), "gl": np.maximum(-values, 0)}, index=trade_days)
    daily = daily.groupby(level=0).sum().reindex(index, fill_value=0)
    result = {"low": None, "high": None, "level": .95, "resamples": resamples, "defined": 0,
              "undefined": resamples, "block_sessions": block, "market_sessions": len(index), "seed": seed,
              "method": "circular moving blocks of observed sessions; trade GP/GL sums", "exploratory": True}
    if len(index) < 2 * block:
        return result
    rng = np.random.default_rng(seed)
    starts = rng.integers(0, len(index), size=(resamples, math.ceil(len(index) / block)))
    indices = ((starts[:, :, None] + np.arange(block)) % len(index)).reshape(resamples, -1)[:, :len(index)]
    totals = daily.to_numpy()[indices].sum(axis=1)
    defined = totals[:, 1] > 0
    result.update(defined=int(defined.sum()), undefined=int((~defined).sum()))
    # Não esconder réplicas indefinidas num IC condicionado às restantes.
    if defined.all():
        result["low"], result["high"] = map(float, np.quantile(totals[:, 0] / totals[:, 1], [.025, .975]))
    return result


def eligibility(metrics, interval, sessions_traded, concentration, positive_quarters, protocol):
    gates = protocol["acceptance"]
    return {"independent_untouched_oos": False, "costs_and_currency_verified": False,
            "closed_trades": metrics["trades"] >= gates["min_closed_trades"],
            "sessions_traded": sessions_traded >= gates["min_trading_sessions"],
            "losses": metrics["losses"] >= gates["min_losses"],
            "profit_factor": (profit_factor.finite(metrics.get("profit_factor")) is not None and
                              metrics["profit_factor"] >= gates["min_pf"]),
            "pf_ci95_lower": interval["low"] is not None and interval["low"] > gates["pf_ci95_lower_gt"],
            "positive_expectancy": metrics["net_profit"] > 0 and metrics["expectancy"] > 0,
            "closed_equity_drawdown": metrics["max_drawdown_pct"] <= gates["max_drawdown"],
            "concentration": concentration is not None and concentration <= gates["concentration_top5_gross_profit_max_share"],
            "positive_quarters": positive_quarters >= gates["min_positive_quarters"],
            "parameter_and_delay_stability": False, "intratrade_equity_and_margin": False,
            "mt5_reconciliation": False}


def trial_cases(protocol):
    universe = protocol["universe"]
    cases = list(itertools.product(universe["timeframes"], universe["strategies"], ["base", "adverso_modelado"]))
    if universe["symbols"] != ["EURUSD"] or not set(universe["timeframes"]) <= {"H1", "M15"} or \
            not set(universe["strategies"]) <= {"ema_atr", "donchian"}:
        raise ValueError("Este recorte aceita somente o universo pré-definido EURUSD/H1/M15/EMAATR/Donchian.")
    budget = protocol["budget"]
    if not cases or len(cases) > min(8, budget["max_trials"]) or budget["max_workers"] != 1:
        raise ValueError("Orçamento inválido: até oito tentativas sequenciais.")
    if not 0 < budget["max_wall_seconds"] <= 600 or not 0 < budget["max_trial_seconds"] <= 60:
        raise ValueError("Prazo deve respeitar os tetos de600s e60s por tentativa.")
    return cases


def calculate_trial(database, protocol, case):
    from mt5_extracao import timeframes
    from mt5_extracao.database_manager import DatabaseManager
    from mt5_extracao.strategies import get_strategy, specs
    from mt5_extracao.trade_analytics import stats
    from mt5_extracao.strategies.backtester import BacktestConfig, run_backtest

    started = time.monotonic()
    tf, key, scenario = case
    strategy = get_strategy(key)
    db = DatabaseManager(db_path=str(database))
    try:
        frame = db.load_ohlcv("EURUSD", timeframes.parse(tf).label)
        spec = specs.load_spec(db, "EURUSD")
    finally:
        db.close()
    if frame.empty or not spec.known:
        raise ValueError("Base ou especificação ausente; nenhuma operação foi inventada.")
    period = protocol["periods"]
    start, end = pd.Timestamp(period["evaluation_start"]), pd.Timestamp(period["evaluation_end_exclusive"])
    stamps = pd.to_datetime(frame["time"])
    frame = frame.loc[stamps < end].copy()
    stamps = pd.to_datetime(frame["time"])
    warmup = int(period["warmup_bars"])
    first = max(0, int((stamps < start).sum()) - warmup)
    frame = frame.iloc[first:].reset_index(drop=True)
    stamps = pd.to_datetime(frame["time"])
    if stamps.duplicated().any() or not stamps.is_monotonic_increasing:
        raise ValueError("Horários duplicados ou fora de ordem.")
    prices = frame[["open", "high", "low", "close"]].to_numpy(dtype=float)
    if not np.isfinite(prices).all() or (prices <= 0).any() or \
            (frame["high"] < frame[["open", "close", "low"]].max(axis=1)).any() or \
            (frame["low"] > frame[["open", "close", "high"]].min(axis=1)).any():
        raise ValueError("OHLC inválido; corrigir origem antes de simular.")
    raw_data_sha = hashlib.sha256(frame.to_json(date_format="iso", orient="split").encode()).hexdigest()
    model = protocol["simulation"]
    config = BacktestConfig(initial_equity=model["initial_equity"], risk_per_trade=model["risk_per_trade"],
                            compound=model["compound"], swap=model["swap"], real_spread=model["real_spread_profile"],
                            commission_per_lot=model["base_commission_per_lot_per_side"],
                            slippage_points=model["base_slippage_points"])
    if scenario != "base":
        stress = model["stress"]
        mult = stress["spread_multiplier"]
        frame["spread"] *= mult
        scale = lambda values: tuple(None if v is None else v * mult for v in values) if values else None
        spec = dataclasses.replace(spec, spread_points=spec.spread_points * mult,
                                   spread_by_hour=scale(spec.spread_by_hour), spread_at_open=scale(spec.spread_at_open))
        config = dataclasses.replace(config, commission_per_lot=stress["commission_per_lot_per_side"],
                                     slippage_points=config.slippage_points + stress["extra_slippage_points_per_market_fill"])
    start_index = max(int((stamps < start).sum()), warmup, strategy.min_bars)
    if start_index >= len(frame):
        raise ValueError("Sem barras após o aquecimento registrado.")
    data_sha = hashlib.sha256(frame.to_json(date_format="iso", orient="split").encode()).hexdigest()
    result = run_backtest(frame, strategy, spec, config, start_index=start_index)
    trades = result.trades_frame()
    values = trades["pnl"].to_numpy(dtype=float)
    days = pd.to_datetime(trades["exit_time"])
    sessions = pd.DatetimeIndex(stamps.iloc[start_index:]).normalize().unique()
    inference = protocol["inference"]
    interval = pf_interval(values, days, sessions, inference["block_sessions"], inference["resamples"], inference["seed"])
    gains = np.sort(values[values > 0])
    concentration = float(gains[-5:].sum() / gains.sum()) if len(gains) else None
    quarters = {}
    for quarter in pd.period_range(start, end - pd.Timedelta(microseconds=1), freq="Q"):
        sample = values[pd.DatetimeIndex(days).to_period("Q") == quarter]
        quarters[str(quarter)] = {**profit_factor.fields(sample), "trades": len(sample), "net": float(sample.sum())}
    sessions_traded = int(pd.DatetimeIndex(days).normalize().nunique())
    metrics = {**result.metrics, "expectancy": stats.metrics(values, config.initial_equity)["expectancy"]}
    gates = eligibility(metrics, interval, sessions_traded, concentration,
                        sum(v["net"] > 0 for v in quarters.values()), protocol)
    return {"case": {"symbol": "EURUSD", "timeframe": tf, "strategy": key, "scenario": scenario},
            "params": strategy.params, "config": dataclasses.asdict(config), "spec": dataclasses.asdict(spec),
            "period": {"source_first": str(stamps.iloc[0]), "source_last": str(stamps.iloc[-1]),
                       "trade_from": str(stamps.iloc[start_index]), "warmup_rows": start_index,
                       "bars": len(frame), "market_sessions": len(sessions), "timezone": "broker; not independently verified"},
            "modeled_data_sha256": data_sha, "raw_data_sha256": raw_data_sha, "metrics": metrics, "pf_ci95": interval,
            "sessions_traded": sessions_traded, "top5_gross_profit_share": concentration, "quarters": quarters,
            "gates": gates, "approved": all(gates.values()), "independent_oos": False, "costs_certified": False,
            "trades": trades.to_dict("records"), "elapsed_seconds": time.monotonic() - started}


def _worker(spec):
    payload = json.loads(Path(spec).read_text(encoding="utf-8"))
    _write(payload["result"], calculate_trial(payload["database"], payload["protocol"], payload["case"]))


def execute(database, protocol_path, output, ui_acceptance):
    import psutil

    protocol = json.loads(Path(protocol_path).read_text(encoding="utf-8"))
    acceptance = json.loads(Path(ui_acceptance).read_text(encoding="utf-8"))
    if acceptance.get("accepted") is not True:
        raise ValueError("Falta aceite crítico da UI documentado antes da campanha.")
    cases = trial_cases(protocol)
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    frozen = output / "input-frozen.sqlite"
    with sqlite3.connect(Path(database).resolve().as_uri() + "?mode=ro", uri=True) as source, \
            sqlite3.connect(frozen) as destination:
        source.backup(destination)
        if destination.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise ValueError("Cópia SQLite inconsistente.")
    started = time.monotonic()
    package = Path(__file__).resolve().parents[1]
    fingerprints = {str(p.relative_to(package)): hashlib.sha256(p.read_bytes()).hexdigest()
                    for p in sorted((package / "strategies").glob("*.py"))}
    for p in [package / "profit_factor.py", package / "trade_analytics/stats.py"]:
        fingerprints[str(p.relative_to(package))] = hashlib.sha256(p.read_bytes()).hexdigest()
    git = subprocess.run(["git", "rev-parse", "HEAD"], cwd=package.parent, capture_output=True, text=True)
    manifest = {"status": "RUNNING", "git_sha": git.stdout.strip() if git.returncode == 0 else None,
                "source_sha256": fingerprints, "python": sys.version, "pandas": pd.__version__, "numpy": np.__version__,
                "started_utc": pd.Timestamp.now(tz="UTC").isoformat(),
                "protocol_sha256": hashlib.sha256(Path(protocol_path).read_bytes()).hexdigest(),
                "frozen_input_sha256": hashlib.sha256(frozen.read_bytes()).hexdigest(),
                "ui_acceptance": acceptance, "attempts": [], "approved": 0,
                "classification": "retrospective exploration; modeled costs, not independent OOS"}
    _write(output / "protocol.json", protocol)
    _write(output / "manifest.json", manifest)
    budget = protocol["budget"]
    try:
        for number, case in enumerate(cases, 1):
            remaining = budget["max_wall_seconds"] - (time.monotonic() - started)
            if remaining <= 0:
                manifest["attempts"].append({"case": case, "state": "CANCELLED_BUDGET"})
                continue
            payload = output / f"trial-{number:02d}-input.json"
            result_file = output / f"trial-{number:02d}-result.json"
            _write(payload, {"database": str(frozen.resolve()), "protocol": protocol,
                             "case": case, "result": str(result_file.resolve())})
            timeout = min(budget["max_trial_seconds"], remaining)
            peak, cpu, reason = 0., 0., None
            before = time.monotonic()
            with (output / f"trial-{number:02d}.log").open("w", encoding="utf-8") as log:
                proc = subprocess.Popen([sys.executable, "-m", MODULE, "--worker", str(payload.resolve())],
                                        stdout=log, stderr=subprocess.STDOUT,
                                        env={**os.environ, "OMP_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1",
                                             "MKL_NUM_THREADS": "1"})
                try:
                    while proc.poll() is None:
                        try:
                            process = psutil.Process(proc.pid)
                            peak = max(peak, process.memory_info().rss / 1024 ** 2)
                            times = process.cpu_times()
                            cpu = max(cpu, times.user + times.system)
                        except psutil.NoSuchProcess:
                            pass
                        if time.monotonic() - before > timeout or peak > budget["max_rss_mb"]:
                            reason = "TIMEOUT" if peak <= budget["max_rss_mb"] else "MEMORY_LIMIT"
                            proc.kill()
                            break
                        time.sleep(.05)
                    code = proc.wait(timeout=15)
                finally:
                    if proc.poll() is None:
                        proc.kill(); proc.wait(timeout=15)
            good = code == 0 and result_file.exists() and reason is None
            manifest["attempts"].append({"case": case, "state": "COMPLETED" if good else reason or "FAILED",
                                         "exit_code": code, "result": result_file.name if good else None,
                                         "peak_rss_mb": peak, "sampled_cpu_seconds": cpu, "seconds": time.monotonic() - before})
            _write(output / "manifest.json", manifest)
        manifest["status"] = "COMPLETED" if all(v["state"] == "COMPLETED" for v in manifest["attempts"]) else "COMPLETED_WITH_FAILURES"
    except BaseException:
        manifest["status"] = "INTERRUPTED"
        raise
    finally:
        manifest["seconds"] = time.monotonic() - started
        manifest["ended_utc"] = pd.Timestamp.now(tz="UTC").isoformat()
        _write(output / "manifest.json", manifest)
    return manifest


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path)
    parser.add_argument("--protocol", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--ui-acceptance", type=Path)
    parser.add_argument("--worker", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if args.worker:
        _worker(args.worker)
        return 0
    if any(getattr(args, name) is None for name in ["database", "protocol", "output", "ui_acceptance"]):
        parser.error("Informe --database, --protocol, --output e --ui-acceptance.")
    manifest = execute(args.database, args.protocol, args.output, args.ui_acceptance)
    print(json.dumps({"status": manifest["status"], "attempts": len(manifest["attempts"]),
                      "approved": 0, "seconds": manifest["seconds"]}))
    return 0 if manifest["status"] == "COMPLETED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
