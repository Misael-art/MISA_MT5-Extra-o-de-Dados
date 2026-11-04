"""SA-REF-01: candidato de referência G19.15 congelado (flip1 / variable2atr / target2r).

Gêmeo Python para reconciliar com o Expert nativo. Não reimplementa a estratégia: importa o
código do estudo G19.15 e o adaptador do header Super ATR da origem histórica SA-REF-01,
conferidos byte a byte contra essa origem, e executa só o livro congelado no backtester canônico do App.
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
# Origem imutável do contrato1; repinar desenvolvimento não revisa a referência.
REFERENCE_FRAMEWORK_COMMIT = "b730ed4a8df74f5f20eb6e289798d0232c370994"

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


def _reference_bytes(root, relative):
    try:
        return subprocess.run(["git", "-C", str(root), "show", f"{REFERENCE_FRAMEWORK_COMMIT}:{relative}"],
                              capture_output=True, timeout=10, check=True).stdout
    except (OSError, subprocess.SubprocessError):
        raise ValueError(f"Não foi possível conferir {relative} na origem congelada SA-REF-01; preserve a referência e use perfil próprio.") from None


def verify_reference_files(framework_root, relatives):
    """Confere a closure solicitada contra a origem; não cria checkout ou fallback."""
    from mt5_extracao.analytics.super_atr import _normalized
    root = Path(framework_root).expanduser().resolve()
    for relative in dict.fromkeys(relatives):
        expected = _reference_bytes(root, relative)
        if _normalized((root / relative).read_bytes()) != _normalized(expected):
            raise ValueError(f"{relative} difere da origem congelada SA-REF-01; uma nova versão/perfil exige contrato próprio.")


def _pinned_study(framework_root, manifest_path=None):
    """Confere header/adaptador (SA-02) e os fontes do estudo contra o commit fixado; importa do pin."""
    from mt5_extracao.analytics import super_atr
    root = Path(framework_root).expanduser().resolve()
    source, _model = super_atr.source(root, manifest_path)
    composition_commit = source["framework_commit"]
    commit = REFERENCE_FRAMEWORK_COMMIT
    verify_reference_files(root, (super_atr.HEADER, super_atr.ADAPTER, *PINNED_FILES))
    source = dict(source, framework_commit=commit, composition_framework_commit=composition_commit)
    tools = str(root / "Tools")
    if tools not in sys.path:
        sys.path.insert(0, tools)
    from research_super_atr import operations
    spec = json.loads((root / PROTOCOL).read_text(encoding="utf-8"))
    return operations, spec, source


def _rows_from_frame(frame):
    from research_super_atr.study import minute
    rows = []
    for row in frame.itertuples(index=False):
        stamp = str(row.time)[:19].replace(" ", "T")
        rows.append({"time": minute(stamp), "open": float(row.open), "high": float(row.high),
                     "low": float(row.low), "close": float(row.close)})
    return rows


def m1_rows(db, symbol, end):
    """Todas as M1 da base até o fim da janela: prefixo completo semeia o indicador, como no estudo."""
    frame = db.load_ohlcv(symbol, "1 minuto", None, end)
    if frame is None or frame.empty:
        raise ValueError(f"Sem M1 de {symbol} na base até {end}.")
    return _rows_from_frame(frame)


def m1_rows_readonly(sqlite_path, symbol, end):
    """M1 de uma base SQLite aberta em modo somente leitura: não cria arquivo, tabela nem migração."""
    import pandas as pd
    from sqlalchemy import create_engine, text
    from mt5_extracao.database_manager import DatabaseManager
    path = Path(sqlite_path).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Base inexistente: {path}; nada foi criado.")
    engine = create_engine(f"sqlite:///file:{path}?mode=ro&uri=true")
    try:
        with engine.connect() as conn:
            table = DatabaseManager.table_for_symbol(conn, symbol, "1 minuto")
            if table is None:
                raise ValueError(f"Sem tabela M1 de {symbol} nesta base.")
            frame = pd.read_sql(text(f'SELECT time, open, high, low, close FROM "{table}" WHERE time < :end ORDER BY time'),
                                conn, params={"end": str(end).replace("T", " ")})
    finally:
        engine.dispose()
    if frame.empty:
        raise ValueError(f"Sem M1 de {symbol} até {end}.")
    return _rows_from_frame(frame)


def m1_rows_frozen(csv_path):
    """M1 congeladas do pacote de reprodução (CSV gz: time,open,high,low,close)."""
    import pandas as pd
    frame = pd.read_csv(csv_path, float_precision="round_trip")      # sem perda: igual à base exportada
    if list(frame.columns[:5]) != ["time", "open", "high", "low", "close"] or frame.empty:
        raise ValueError("CSV congelado fora do formato time,open,high,low,close.")
    return _rows_from_frame(frame)


def python_book(db, framework_root, *, cost="zero", manifest_path=None, rows=None):
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
    if rows is None:
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
