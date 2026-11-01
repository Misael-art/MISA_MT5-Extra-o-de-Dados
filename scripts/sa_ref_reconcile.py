#!/usr/bin/env python3
"""SA-REF-01: reconcilia um ensaio do Strategy Tester com o gêmeo Python, sem caminhos pessoais.

Uso (na raiz do App, com o .venv do projeto):
  .venv/bin/python scripts/sa_ref_reconcile.py --framework ../hadouken \
      --agent-log TESTER_AGENT.log --trades-csv SESSAO_trades.csv --out PASTA_NOVA [--config config/config.ini]

Lê a base pela configuração oficial (só leitura), roda o livro congelado e grava decisoes.json e
reconciliacao.json em uma pasta nova. Não inicia MT5, não envia ordens e não altera a base.
"""
import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def read_agent_log(path):
    raw = Path(path).read_bytes()
    return raw.decode("utf-16") if raw[:2] in (b"\xff\xfe", b"\xfe\xff") else raw.decode("utf-8", errors="replace")


def reconcile_decisions(native_lines, python_decisions):
    """Decisões elegíveis nativas × gêmeo (sinal Python no último M1 do M5; decisão = abertura seguinte)."""
    native = {}
    collisions = []
    for line in native_lines:
        when = datetime.strptime(line["decision"], "%Y.%m.%d %H:%M")
        if line.get("eligible") != "1":
            continue
        if when in native:
            collisions.append(str(when))
        native[when] = dict(side=int(line["side"]), stop=float(line["stop"]), run=int(line["run"]))
    python = {datetime.fromisoformat(d["time"]) + timedelta(minutes=1): d for d in python_decisions}
    both = sorted(set(native) & set(python))
    same = [k for k in both if native[k]["side"] == (1 if python[k]["side"] == "long" else -1)]
    diffs = sorted(abs(native[k]["stop"] - python[k]["stop"]) for k in same)
    return dict(native_eligible=len(native), python=len(python), matched=len(both), same_side=len(same),
                collisions=collisions, stop_abs_diff_max=diffs[-1] if diffs else None,
                only_native=[dict(time=str(k), **native[k]) for k in sorted(set(native) - set(python))],
                only_python=[str(k) for k in sorted(set(python) - set(native))])


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--framework", required=True, type=Path)
    parser.add_argument("--agent-log", required=True, type=Path)
    parser.add_argument("--trades-csv", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--config", default="config/config.ini")
    parser.add_argument("--cost", default="zero", choices=("zero", "base", "stress"))
    args = parser.parse_args(argv)
    if args.out.exists():
        raise SystemExit("Pasta de saída já existe; escolha outra (nada é sobrescrito).")
    from mt5_extracao import services
    from mt5_extracao.robots import parity, super_atr_reference as ref
    db = services.create_db_manager(services.load_config(args.config))
    book = ref.python_book(db, args.framework, cost=args.cost)
    text = read_agent_log(args.agent_log)
    lines = parity.parse_decision_lines(text)
    decisions = reconcile_decisions(lines, book["decisions"])
    trades = parity.reconcile_trades(book["trades"], parity.read_trade_export(args.trades_csv), lines,
                                     host_closes=parity.parse_close_lines(text))
    args.out.mkdir(parents=True)
    (args.out / "decisoes.json").write_text(json.dumps(decisions, indent=1, default=str), encoding="utf-8")
    (args.out / "reconciliacao.json").write_text(json.dumps(trades, indent=1, default=str), encoding="utf-8")
    summary = dict(contract=book["contract"]["id"], cost=args.cost, framework=book["framework"],
                   python_metrics={k: book["metrics"].get(k) for k in ("trades", "profit_factor", "net_profit")},
                   decisions={k: v for k, v in decisions.items() if not k.startswith("only")},
                   trades=trades["summary"])
    (args.out / "resumo.json").write_text(json.dumps(summary, indent=1, default=str), encoding="utf-8")
    print(json.dumps(summary, indent=1, default=str))


if __name__ == "__main__":
    main()
