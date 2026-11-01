"""SA-REF-01: candidato de referência G19.15 congelado (flip1 / variable2atr / target2r).

Gêmeo Python para reconciliar com o Expert nativo. Não reimplementa a estratégia: importa o
código do estudo G19.15 e o adaptador do header Super ATR do commit HADOUKEN fixado no manifesto,
conferidos byte a byte contra o pin, e executa só o livro congelado no backtester canônico do App.
Nenhuma ordem, sessão MT5, otimização ou reescolha de parâmetros acontece aqui.
"""
from dataclasses import asdict
import json
from pathlib import Path
import subprocess
import sys
import tempfile

STUDY = "Tools/research_super_atr"
PROTOCOL = "Documentation/HADOUKEN_2/estudos/SUPER_ATR_GESTAO_20261023/protocolo.json"
PINNED_FILES = (PROTOCOL, f"{STUDY}/__init__.py", f"{STUDY}/operations.py", f"{STUDY}/expanded.py",
                f"{STUDY}/study.py", f"{STUDY}/model.py", f"{STUDY}/capture.py", f"{STUDY}/report.py")

CONTRACT = {
    "id": "SA-REF-01", "version": 1,
    "origin": "G19.15 protocolo.json (referência predefinida, não escolhida após ver testes)",
    "key": "flip1/variable2atr/target2r",
    "symbol": "EURUSD", "role": "later", "window_broker": ["2026-10-01T00:00:00", "2026-10-20T00:00:00"],
    "signal": "Super ATR M5 (agregado diário de M1), período20, SMA, curva d1; virada estrita na barra M5 fechada",
    "eligibility": ">=100 fechamentos M5 consecutivos desde início do dia/buraco",
    "entry": "próximo open M1 consecutivo após a decisão M5",
    "stop": "nível = close M5 do sinal ∓ 2×ATR20 (mesma barra)",
    "target": "2R", "opposite_flip_exit": False, "daily_exit": "último M1 presente do dia",
    "risk": "0,25% de 100.000 USD assumidos, sem composição, 1 ciclo por vez",
    "costs": ["zero", "base", "stress"], "uncertainty_seed": 20261026,
    "known_parity_risks": [
        "semente do indicador: estudo desde 2026-07-08; base do App desde 2026-07-22; Expert limitado a 20.000 barras",
        "M5 do estudo agrega M1 por dia; Expert lê M5 nativo do servidor",
        "cotações do estudo tratadas como BID; Tester usa spread/ticks do modelo escolhido",
        "custos do estudo são cenários constantes, não históricos da corretora",
    ],
    "published_reference": {"later/zero": {"cycles": 102, "pf": 1.03, "net_usd": 473.53},
                            "later/base": {"cycles": 98, "pf": 0.90, "net_usd": -1593.62}},
}


def _pinned_study(framework_root, manifest_path=None):
    """Confere header/adaptador (SA-02) e os fontes do estudo contra o commit fixado; importa do pin."""
    from mt5_extracao.analytics import super_atr
    from mt5_extracao.analytics.super_atr import _normalized
    root = Path(framework_root).expanduser().resolve()
    source, _model = super_atr.source(root, manifest_path)
    commit = source["framework_commit"]
    for relative in PINNED_FILES:
        expected = subprocess.run(["git", "-C", str(root), "show", f"{commit}:{relative}"],
                                  capture_output=True, timeout=10, check=True).stdout
        if _normalized((root / relative).read_bytes()) != _normalized(expected):
            raise ValueError(f"{relative} difere do commit fixado; reconcilie a composição antes do ensaio.")
    tools = str(root / "Tools")
    if tools not in sys.path:
        sys.path.insert(0, tools)
    from research_super_atr import operations
    spec = json.loads((root / PROTOCOL).read_text(encoding="utf-8"))
    return operations, spec, source


def m1_rows(db, symbol, end):
    """Todas as M1 da base até o fim da janela: prefixo completo semeia o indicador, como no estudo."""
    from research_super_atr.study import minute
    frame = db.load_ohlcv(symbol, "1 minuto", None, end)
    if frame is None or frame.empty:
        raise ValueError(f"Sem M1 de {symbol} na base até {end}.")
    rows = []
    for row in frame.itertuples(index=False):
        stamp = str(row.time)[:19].replace(" ", "T")
        rows.append({"time": minute(stamp), "open": float(row.open), "high": float(row.high),
                     "low": float(row.low), "close": float(row.close)})
    return rows


def python_book(db, framework_root, *, cost="zero", manifest_path=None):
    """Executa o livro congelado na janela do contrato e devolve decisões, trades e métricas."""
    operations, spec, source = _pinned_study(framework_root, manifest_path)
    from mt5_extracao.strategies.backtester import run_backtest
    from mt5_extracao.strategies.base import Strategy
    from research_super_atr.study import minute
    if cost not in spec["costs"]:
        raise ValueError("Custo fora do protocolo congelado.")
    lo, hi = spec["periods"][CONTRACT["role"]]
    if [lo, hi] != CONTRACT["window_broker"]:
        raise ValueError("Janela do protocolo diverge do contrato SA-REF-01.")
    rows = m1_rows(db, CONTRACT["symbol"], hi)
    with tempfile.TemporaryDirectory(prefix="sa-ref-") as private:
        study = operations.OperationalStudy(spec, rows, operations.RealModel(), lambda: None, private)
    lo_m, hi_m = minute(lo), minute(hi)
    mask = (study.times >= lo_m) & (study.times < hi_m)
    data = study.m1.loc[mask].reset_index(drop=True)
    signals = study.signals["flip1"].loc[mask].reset_index(drop=True).copy()
    signals[["exit_long", "exit_short"]] = False                     # target2r: sem saída por virada
    sp, cfg = study.cost_objects(cost)

    class ReferenceConsumer(Strategy):
        def describe(self):
            return "SA-REF-01 consumidor do sinal G19.15; não é estratégia registrada"

    result = run_backtest(data, ReferenceConsumer(), sp, cfg, signals=signals,
                          exit_rules_override=operations.management_rules("variable2atr", "target2r"))
    decisions = signals[signals.entry_long | signals.entry_short]
    return {
        "contract": CONTRACT, "cost": cost, "framework": {k: source[k] for k in
                                                         ("framework_commit", "header_sha256", "adapter_sha256")},
        "m1_rows": int(mask.sum()), "seed_first_m1": str(study.m1.time.iloc[0]),
        "decisions": [{"time": str(data.time.iloc[i]), "side": "long" if decisions.entry_long[i] else "short",
                       "stop": float(decisions.stop_long[i] if decisions.entry_long[i] else decisions.stop_short[i]),
                       "atr": float(decisions.atr[i])} for i in decisions.index],
        "trades": [{k: (str(v) if k in ("entry_time", "exit_time") else v) for k, v in asdict(t).items()}
                   for t in result.trades],
        "metrics": result.metrics, "skipped_gap": result.skipped_gap, "skipped_min_lot": result.skipped_min_lot,
    }
