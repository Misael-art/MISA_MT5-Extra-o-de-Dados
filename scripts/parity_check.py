"""
Teste de paridade Windows x Linux (T2.6): prova que a extração pela ponte (Wine)
é idêntica à extração local no Windows.

Rode o mesmo comando nas duas máquinas, na mesma conta/corretora, e compare:

    python scripts/parity_check.py extract --symbol EURUSD --date 2026-10-09 --out linux.parquet
    python scripts/parity_check.py extract --symbol EURUSD --date 2026-10-09 --out windows.parquet
    python scripts/parity_check.py compare linux.parquet windows.parquet

Requer pyarrow (requirements-optional.txt). Códigos de saída: 0 = ok/idêntico, 1 = falha/diferente.
"""
import argparse
import datetime
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def cmd_extract(args) -> int:
    from mt5_extracao import mt5_connector as connector_module
    connector = connector_module.MT5Connector(config_path=args.config)
    if not connector.initialize():
        print("ERRO: não foi possível conectar ao MetaTrader 5. Rode o doctor.")
        return 1
    day = datetime.datetime.strptime(args.date, "%Y-%m-%d")
    timeframe = connector_module.mt5.TIMEFRAME_M1
    df = connector.get_rates_range(args.symbol, timeframe, day, day + datetime.timedelta(hours=23, minutes=59))
    if df is None or df.empty:
        print(f"ERRO: sem dados M1 de {args.symbol} em {args.date}.")
        return 1
    df.to_parquet(args.out, index=False)
    print(f"{len(df)} barras M1 de {args.symbol} em {args.date} salvas em {args.out}")
    return 0


def cmd_compare(args) -> int:
    a, b = pd.read_parquet(args.a), pd.read_parquet(args.b)
    try:
        pd.testing.assert_frame_equal(a, b, check_dtype=False)
    except AssertionError as e:
        print(f"DIFERENTE ({len(a)} x {len(b)} barras):\n{e}")
        return 1
    print(f"IDÊNTICO: {len(a)} barras, colunas {list(a.columns)}")
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Paridade da extração M1 entre Windows e Linux.")
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("extract", help="Extrai um dia de M1 e salva em Parquet")
    p.add_argument("--symbol", default="WIN$N")
    p.add_argument("--date", required=True, help="Dia útil completo, AAAA-MM-DD")
    p.add_argument("--out", required=True)
    p.add_argument("--config", default="config/config.ini")
    p.set_defaults(func=cmd_extract)
    p = sub.add_parser("compare", help="Compara dois arquivos Parquet")
    p.add_argument("a")
    p.add_argument("b")
    p.set_defaults(func=cmd_compare)
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
