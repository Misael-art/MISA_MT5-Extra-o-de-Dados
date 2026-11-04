"""Importação de dados públicos da B3 (COTAHIST diário e negócios → barras) num banco próprio."""
import io
import sqlite3
import zipfile

import pandas as pd
import pytest

from mt5_extracao import b3_public, cli

HEADER = ("DataReferencia;CodigoInstrumento;AcaoAtualizacao;PrecoNegocio;QuantidadeNegociada;HoraFechamento;"
          "CodigoIdentificadorNegocio;TipoSessaoPregao;DataNegocio;CodigoParticipanteComprador;"
          "CodigoParticipanteVendedor")


def cotahist_line(code, day, o, h, l, c, trades, qty, market="010"):
    p = lambda v: f"{round(v * 100):013d}"  # noqa: E731
    line = (f"01{day}02{code:<12}{market}{'EMPRESA':<12}{'ON NM':<10}{'':3}{'R$':<4}"
            f"{p(o)}{p(h)}{p(l)}{p(c)}{p(c)}{p(c)}{p(c)}{trades:05d}{qty:018d}{0:018d}")
    return line.ljust(245, "0")


def zipped(name, text):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(name, text)
    return buffer.getvalue()


def trades_text(rows):
    return "\n".join([HEADER] + [f"2026-11-03;WINZ26;{act};{price};{qty};{clock};{tid};1;2026-11-03;3;88"
                                 for act, price, qty, clock, tid in rows]) + "\n"


def test_cotahist_reads_spot_lot_bars_and_ignores_other_markets():
    text = "\n".join(["00COTAHIST.2026BOVESPA 20261103".ljust(245),
                      cotahist_line("PETR4", "20261103", 51.2, 51.21, 49.15, 50.71, 18206, 93496300),
                      cotahist_line("PETR4F", "20261103", 51, 52, 50, 51, 9, 90, market="020"),
                      cotahist_line("VALE3", "20261103", 61.51, 62.7, 60.21, 60.25, 57411, 21064100),
                      "99COTAHIST".ljust(245)])
    bars = b3_public.parse_cotahist(zipped("COTAHIST_D03112026.TXT", text.encode("latin-1")))
    assert sorted(bars) == ["PETR4", "VALE3"]
    row = bars["PETR4"].iloc[0]
    assert (row.time, row.open, row.high, row.low, row.close, row.real_volume) == (
        pd.Timestamp("2026-11-03"), 51.2, 51.21, 49.15, 50.71, 93496300)
    assert list(b3_public.parse_cotahist(text.encode("latin-1"), ["vale3"])) == ["VALE3"]


def test_trades_drop_cancelled_and_become_minute_bars():
    raw = trades_text([("0", "100,0", 1, "090000100", 10), ("0", "105,0", 2, "90030000", 20),
                       ("0", "999,0", 5, "090040000", 30), ("2", "999,0", 5, "090040000", 30),
                       ("0", "98,5", 3, "090105000", 40)]).encode()
    trades = b3_public.parse_trades(zipped("03-11-2026_NEGOCIOSAVISTA_WINZ26.txt", raw))
    assert trades.attrs["symbol"] == "WINZ26" and len(trades) == 3
    bars = b3_public.trades_to_bars(trades)
    assert bars[["open", "high", "low", "close", "tick_volume", "real_volume"]].values.tolist() == [
        [100.0, 105.0, 100.0, 105.0, 2, 3], [98.5, 98.5, 98.5, 98.5, 1, 3]]
    assert bars["time"].tolist() == [pd.Timestamp("2026-11-03 09:00"), pd.Timestamp("2026-11-03 09:01")]
    assert b3_public.trades_to_bars(trades, 5)["tick_volume"].tolist() == [3]


def test_download_refuses_files_above_manual_limit(tmp_path, monkeypatch):
    class Response:
        headers = {"Content-Length": str(89_056_788)}
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def raise_for_status(self): pass
        def iter_content(self, size): yield b"x"
    monkeypatch.setattr(b3_public.requests, "get", lambda *a, **k: Response())
    with pytest.raises(ValueError, match="manualmente"):
        b3_public.download("https://b3/COTAHIST_A2025.ZIP", tmp_path, "COTAHIST_A2025.ZIP")
    assert list(tmp_path.iterdir()) == []


def test_cli_imports_local_files_into_dedicated_b3_database(tmp_path):
    db = tmp_path / "b3.db"
    trades = tmp_path / "WINZ26.zip"
    trades.write_bytes(zipped("x.txt", trades_text([("0", "100,0", 1, "090000100", 10)]).encode()))
    daily = tmp_path / "COTAHIST_D03112026.ZIP"
    daily.write_bytes(zipped("d.txt", cotahist_line("PETR4", "20261103", 1, 2, 0.5, 1.5, 3, 4).encode()))
    assert cli.main(["--db", str(db), "b3", "trades", "--file", str(trades)]) == cli.EXIT_OK
    assert cli.main(["--db", str(db), "b3", "daily", "--file", str(daily)]) == cli.EXIT_OK
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT close, real_volume FROM winz26_1_minuto").fetchall() == [(100.0, 1)]
        assert conn.execute("SELECT close FROM petr4_1_dia").fetchall() == [(1.5,)]
        origin = dict(conn.execute("SELECT slot, origin_id FROM _dataset_origin").fetchall())
    assert origin.get("server") == "B3"


def test_cli_trades_from_b3_requires_symbols(tmp_path):
    assert cli.main(["--db", str(tmp_path / "b3.db"), "b3", "trades", "--from", "2026-11-03"]) != cli.EXIT_OK


def test_contract_specs_for_futures_and_stocks():
    win, wdo, stock = (b3_public.spec_for(c) for c in ("WINZ26", "wdoz26", "PETR4"))
    assert (win["tick_size"], win["tick_value"], win["volume_min"]) == (5.0, 1.0, 1.0)
    assert (wdo["tick_size"], wdo["tick_value"]) == (0.5, 5.0)
    assert (stock["volume_min"], stock["tick_size"], stock["currency_profit"]) == (100.0, 0.01, "BRL")
    assert b3_public.spec_for("WIN$N") is None and b3_public.spec_for("PETRA1") is None


def test_empty_trades_file_saves_nothing(tmp_path):
    empty = tmp_path / "WINZ26.zip"
    empty.write_bytes(zipped("x.txt", (HEADER + "\n").encode()))
    assert cli.main(["--db", str(tmp_path / "b3.db"), "b3", "trades", "--file", str(empty)]) != cli.EXIT_OK
