"""Reexecuta a comparação contra o CSV observado no MT5, sem precisar do terminal."""
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import numpy as np
import pandas as pd

from mt5_extracao.strategies.backtester import BacktestConfig, SymbolSpec
from mt5_extracao.trade_analytics.execution.engine import ExitPolicy, simulate


def verify(directory=None):
    directory = Path(directory or Path(__file__).parent)
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    for name, digest in manifest["sha256"].items():
        if hashlib.sha256((directory / name).read_bytes()).hexdigest() != digest:
            raise ValueError(f"Referência nativa alterada: {name}; confira a origem antes de comparar.")
    native = pd.read_csv(directory / "native.csv", sep=";")
    assert len(native) == 24 and native.currency.unique().tolist() == ["USD"]
    last = native.iloc[-1]
    assert last.entry_bar > 0 and last.exit_bar > last.entry_bar
    assert float(native.volume.max()) == 0.01
    data = native.rename(columns={"spread_close": "spread"}).copy()
    data["time"] = pd.to_datetime(native.bar_time, unit="s")
    # As chaves representam barras, como o motor. Instantes nativos dos deals
    # são preservados no CSV; isto não testa paridade de fills intrabarra.
    source = pd.DataFrame([dict(direction=1, entry_time=pd.to_datetime(last.entry_bar, unit="s"),
                                exit_time=pd.to_datetime(last.exit_bar, unit="s"),
                                entry_price=last.entry_price, stop_price=last.initial_stop,
                                target_price=None, volume=0.01, exit_reason="sinal de saída")])
    spec = SymbolSpec(point=float(last.point), tick_size=float(last.tick_size),
                      tick_value=float(last.tick_value), volume_min=0.01, volume_step=0.01)
    cfg = BacktestConfig(initial_equity=1000, real_spread=False, swap=False)
    result = simulate(source, data, spec, cfg, ExitPolicy())
    # Tolerância monetária absoluta, menor que um centavo; sem tolerância relativa.
    np.testing.assert_allclose(result.account_balance, native.balance, atol=1e-8, rtol=0)
    np.testing.assert_allclose(result.floating_equity, native.equity, atol=1e-8, rtol=0)
    # Reconciliar o custo líquido zero do cenário, em vez de supô-lo para outros brokers.
    gross = (last.exit_price - last.entry_price) / last.tick_size * last.tick_value * 0.01
    assert abs(gross - (last.balance - 1000)) < 1e-8
    peak = np.maximum.accumulate(native.equity.to_numpy())
    drawdown = peak - native.equity.to_numpy()
    assert drawdown.max() > 0
    assert abs(result.metrics["max_floating_drawdown_money"] - drawdown.max()) < 1e-8
    assert abs(result.metrics["max_floating_drawdown_pct"] - (drawdown / peak).max()) < 1e-12
    assert result.approximate == 0
    return {"rows": len(native), "trades": len(result.trades), "currency": "USD",
            "balance_max_abs": float(np.abs(result.account_balance.to_numpy() - native.balance).max()),
            "equity_max_abs": float(np.abs(result.floating_equity.to_numpy() - native.equity).max()),
            "drawdown_money": result.metrics["max_floating_drawdown_money"],
            "drawdown_pct": result.metrics["max_floating_drawdown_pct"]}


if __name__ == "__main__":
    print(json.dumps(verify(), ensure_ascii=False, indent=2))
