"""
Importadores do livro de operações (T16.2): de onde vêm as linhas do `_trade_ledger`.

- Backtest Python: `record_python_run` grava as operações completas (`strategies.backtester.Trade`). Chamado por
  `robots/backtest.run_python` logo depois de gravar o run.
- Strategy Tester (robôs MQL5): `record_mql5_run` lê o `HDK_trades_<magic>.csv`. No formato antigo
  (`time;symbol;type;volume;result`) a operação entra como `parcial` (sem horário de entrada). No formato estendido
  (T16.2b, no HADOUKEN: `entry_time;entry_price;exit_price;sl;tp;direction;exit_reason`) entra `completa`.
- Runs antigos: `import_existing_runs` traz para o livro os runs de `_robot_runs` que ainda não estão nele (com o que
  o run guardou: saída, resultado e volume, ou seja `parcial`).

Execuções auxiliares (bateria, paridade) não entram: não são o resultado de um robô.
"""
import hashlib
import json
import logging
from pathlib import Path
from typing import Iterable, List, Optional

from mt5_extracao.trade_analytics import ledger

log = logging.getLogger(__name__)

PYTHON, MQL5 = "python", "mql5_tester"
_BUY = {"buy", "compra", "long", "0", "1.0", "+1", "1"}
_SELL = {"sell", "venda", "short", "-1", "-1.0"}


def params_hash(params: Optional[dict]) -> str:
    """Identidade curta da configuração do run (parâmetros em ordem), para comparar configurações no Laboratório."""
    raw = json.dumps(params or {}, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]


def _hidden(run: dict) -> bool:
    from mt5_extracao.robots import catalog
    return run.get("source") in catalog.HIDDEN_RUN_SOURCES


def _kind(robot_key: str) -> str:
    return PYTHON if str(robot_key).startswith("python:") else MQL5


def record_python_run(db, run: dict, trades: Iterable, *, trajectory=None) -> int:
    """Operações completas de um backtest Python. 0 se o run for auxiliar."""
    if _hidden(run):
        return 0
    count = ledger.save_trades(db, run["run_id"], trades, robot_key=run["robot_key"], symbol=run["symbol"],
                               timeframe=run["timeframe"], source=PYTHON, params_hash=params_hash(run.get("params")))
    if trajectory:
        from mt5_extracao.trade_analytics.trajectories import store
        store.save(db, run["run_id"], trajectory, source=PYTHON,
                   producer_identity=(run.get("metrics") or {}).get("execution_identity"))
    return count


def _direction(value: str) -> Optional[int]:
    value = (value or "").strip().lower()
    if value in _SELL:
        return -1
    if value in _BUY:
        return 1
    return None


def read_hdk_trades_full(path) -> List[dict]:
    """
    Operações do HDK_trades.csv como dicionários do livro. Aceita os dois formatos; colunas desconhecidas são
    ignoradas. A coluna `time` é a saída. `direction` (ou, sem ela, `type`) diz compra ou venda.
    """
    from mt5_extracao.robots import mql
    from mt5_extracao.robots.backtest import _number
    lines = [line.strip() for line in mql.read_text(path).splitlines() if line.strip()]
    if not lines:
        return []
    header = [h.strip().lower() for h in lines[0].split(";")]
    if "time" not in header or "result" not in header:
        raise ValueError(f"{Path(path).name} não tem as colunas esperadas (time;symbol;type;volume;result).")
    at = {name: i for i, name in enumerate(header)}
    out = []
    for cells in (line.split(";") for line in lines[1:]):
        def cell(name):
            i = at.get(name)
            return cells[i].strip() if i is not None and i < len(cells) else ""
        if len(cells) <= max(at["time"], at["result"]):
            continue
        direction = _direction(cell("direction")) if "direction" in at else None
        if direction is None and "entry_time" in at:          # formato estendido sem direction: usa type
            direction = _direction(cell("type"))
        row = {"exit_time": cell("time"), "pnl": _number(cell("result")),
               "volume": _number(cell("volume")) if "volume" in at else None,
               "entry_time": cell("entry_time") or None, "direction": direction,
               "exit_reason": cell("exit_reason") or None}
        for src, dst in (("entry_price", "entry_price"), ("exit_price", "exit_price"), ("sl", "stop_price"),
                         ("tp", "target_price")):
            if src in at and cell(src):
                value = _number(cell(src))
                row[dst] = value if value else None           # 0 no CSV do MT5 = sem stop/alvo
        out.append(row)
    return out


def record_mql5_run(db, record: dict, trades_csv=None) -> int:
    """Operações de um run do Strategy Tester: do CSV (quando houver) ou do que o run guardou."""
    if _hidden(record):
        return 0
    if trades_csv is not None and Path(trades_csv).is_file():
        trades = read_hdk_trades_full(trades_csv)
    else:
        trades = [{"exit_time": t[0], "pnl": float(t[1]), "volume": float(t[2]) if len(t) > 2 else None}
                  for t in record.get("trades") or []]
    return ledger.save_trades(db, record["run_id"], trades, robot_key=record["robot_key"], symbol=record["symbol"],
                              timeframe=record["timeframe"], source=MQL5,
                              params_hash=params_hash(record.get("params")))


def import_existing_runs(db, include_hidden: bool = False) -> dict:
    """Runs de `_robot_runs` que ainda não estão no livro. Devolve {"runs": n, "trades": n, "skipped": n}."""
    from mt5_extracao.robots import store
    known = {r["run_id"] for r in ledger.runs(db)}
    done = {"runs": 0, "trades": 0, "skipped": 0}
    for run in store.load_runs(db):
        if run["run_id"] in known or (_hidden(run) and not include_hidden):
            done["skipped"] += 1
            continue
        trades = [{"exit_time": t[0], "pnl": float(t[1]), "volume": float(t[2]) if len(t) > 2 else None}
                  for t in run.get("trades") or []]
        done["trades"] += ledger.save_trades(db, run["run_id"], trades, robot_key=run["robot_key"],
                                             symbol=run["symbol"], timeframe=run["timeframe"],
                                             source=_kind(run["robot_key"]), params_hash=params_hash(run.get("params")))
        done["runs"] += 1
    return done


def safely(fn, *args, **kwargs) -> int:
    """Gravar no livro nunca derruba o backtest: em erro, registra no log e segue."""
    try:
        return fn(*args, **kwargs)
    except Exception as e:
        log.error(f"Laboratório: não foi possível gravar as operações no livro: {e}")
        return 0
