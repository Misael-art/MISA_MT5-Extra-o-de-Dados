#!/usr/bin/env python3
"""Auditoria operacional DEMO, sem negociação nem contrato histórico de outubro.

snapshot lê carteira/histórico via backend oficial já conectado; audit usa só arquivos congelados.
Saídas novas nunca sobrescrevem evidência anterior. Leia docs/sdk/demo_operacional.md.
"""
import argparse
from datetime import datetime
import json
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from mt5_extracao.robots import session_audit


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="action", required=True)
    for command in ("snapshot", "audit"):
        p = sub.add_parser(command)
        p.add_argument("--session-id", required=True)
        p.add_argument("--symbol", required=True)
        p.add_argument("--magic", required=True, type=int)
        p.add_argument("--out", required=True, type=Path)
        if command == "snapshot":
            p.add_argument("--start", required=True, type=datetime.fromisoformat)
            p.add_argument("--end", required=True, type=datetime.fromisoformat)
            p.add_argument("--config", default="config/config.ini")
        else:
            p.add_argument("--tf", required=True)
            p.add_argument("--log", required=True, action="append", type=Path)
            for name in ("preset", "source", "wrapper", "binary", "inputs", "snapshot"):
                p.add_argument("--"+name, required=True, type=Path)
            p.add_argument("--trades-csv", type=Path)
    args = parser.parse_args(argv)
    if args.out.exists():
        raise ValueError("Saída já existe; escolha novo recibo.")
    if args.action == "snapshot":
        from mt5_extracao.mt5_backend import get_mt5
        report = session_audit.capture_snapshot(get_mt5(args.config), session_id=args.session_id,
            symbol=args.symbol, magic=args.magic, start=args.start, end=args.end)
    else:
        from mt5_extracao.robots.super_atr_research import AUDIT_INPUT_FIELDS
        report = session_audit.audit_files(session_id=args.session_id, symbol=args.symbol, magic=args.magic,
            timeframe=args.tf, logs=args.log, preset=args.preset, source=args.source, wrapper=args.wrapper,
            binary=args.binary, inputs=args.inputs, fields=AUDIT_INPUT_FIELDS, snapshot=args.snapshot,
            trades_csv=args.trades_csv)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("x", encoding="utf-8") as handle:
        handle.write(json.dumps(report, ensure_ascii=False, indent=2)+"\n")
    args.out.chmod(0o600)
    print(json.dumps(dict(out=str(args.out), states=report.get("states"), final_state=report.get("final_state"),
                          interrupt=report.get("interrupt")), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
