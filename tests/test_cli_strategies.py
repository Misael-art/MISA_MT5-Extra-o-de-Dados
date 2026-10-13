"""T6.7: comandos de estratégia da CLI e relatório HTML."""
import pandas as pd
import pytest

import mt5_extracao.mt5_connector as connector_module
from mt5_extracao import cli
from mt5_extracao.database_manager import DatabaseManager
from tests.market_data import random_walk, trend


@pytest.fixture
def env(fake_mt5, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    def fake_initialize(self, *a, **k):
        self.is_initialized = True
        return True
    monkeypatch.setattr(connector_module.MT5Connector, "initialize", fake_initialize)
    db_path = tmp_path / "db.sqlite"
    db = DatabaseManager(db_path=str(db_path))
    db.save_ohlcv_data("AAA", "1 dia", random_walk(1300, seed=1, spread=5, volume=4000))
    db.save_ohlcv_data("BBB", "1 dia", trend(1300, step=0.2, noise=1.0, freq="D", spread=5, volume=3000))
    db.save_ohlcv_data("CCC", "1 dia", random_walk(1300, seed=3, spread=400, volume=10))
    cfg = tmp_path / "config.ini"
    cfg.write_text(f"[DATABASE]\ntype = sqlite\npath = {db_path}\n[STRATEGY]\nmin_trades = 5\n",
                   encoding="utf-8")
    return ["--config", str(cfg)], db


def test_strategies_lists_six_and_avoid(capsys):
    assert cli.main(["strategies"]) == 0
    out = capsys.readouterr().out
    for key in ("ema_atr", "donchian", "bb_rsi", "orb", "pullback", "squeeze"):
        assert key in out
    assert "Evite no início" in out and "Martingale" in out and "não garante" in out


def test_specs_from_fake_mt5(env, fake_mt5, capsys):
    args, db = env
    symbol = fake_mt5.symbols[0]
    assert cli.main(args + ["specs", "--symbols", symbol]) == 0
    assert "lote mín. 1" in capsys.readouterr().out
    assert db.get_symbol_spec(symbol) is not None
    assert cli.main(args + ["specs", "--symbols", "NAO_EXISTE"]) == 1


def test_backtest_command(env, tmp_path, capsys):
    args, _ = env
    out_csv = tmp_path / "ops.csv"
    assert cli.main(args + ["backtest", "--symbol", "BBB", "--tf", "D1", "--strategy", "donchian",
                            "--param", "entry_period=30", "--trades-out", str(out_csv)]) == 0
    out = capsys.readouterr().out
    assert "entry_period=30" in out and "Profit factor" in out and "especificação ausente" in out
    assert len(pd.read_csv(out_csv)) > 0


def test_backtest_usage_errors(env, capsys):
    args, _ = env
    assert cli.main(args + ["backtest", "--symbol", "BBB", "--tf", "D1", "--strategy", "martingale"]) == 2
    assert cli.main(args + ["backtest", "--symbol", "BBB", "--tf", "D1", "--strategy", "orb",
                            "--param", "x"]) == 2
    assert cli.main(args + ["backtest", "--symbol", "ZZZ", "--tf", "D1", "--strategy", "orb"]) == 2
    assert "mt5x extract --symbols ZZZ" in capsys.readouterr().err


def test_screen_uses_all_symbols_with_data(env, capsys):
    args, _ = env
    assert cli.main(args + ["screen", "--tf", "D1"]) == 0
    out = capsys.readouterr().out
    assert out.splitlines()[0].startswith("Ativo")
    assert "AAA" in out and "BBB" in out and "CCC" in out
    assert "❌" in out   # CCC: ilíquido e caro (mesmo sem especificação, a liquidez relativa reprova)


def test_validate_and_report(env, tmp_path, capsys):
    args, db = env
    assert cli.main(args + ["validate", "--symbols", "BBB", "--tf", "D1", "--strategies", "donchian"]) == 0
    out = capsys.readouterr().out
    assert "BBB" in out and "donchian" in out and "combinação" in out
    report = tmp_path / "rel.html"
    assert cli.main(args + ["report", "--tf", "D1", "--out", str(report)]) == 0
    html = report.read_text(encoding="utf-8")
    assert html.startswith("<!doctype html>") and "Matriz ativo × estratégia" in html
    assert "BBB" in html and ("✅ aprovada" in html or "❌ reprovada" in html)
    assert "não garantem resultados futuros" in html and "Martingale" in html
    assert "http" not in html   # autocontido


def test_report_without_validations_explains_next_step(env, tmp_path):
    args, _ = env
    report = tmp_path / "r.html"
    assert cli.main(args + ["report", "--tf", "D1", "--out", str(report)]) == 0
    assert "mt5x validate" in report.read_text(encoding="utf-8")


def test_research_without_data(env, capsys):
    args, _ = env
    assert cli.main(args + ["screen", "--tf", "H1"]) == 2
    assert "nenhum símbolo com dados em H1" in capsys.readouterr().err


def test_cli_output_survives_non_utf8_stdout(tmp_path):
    """Windows com saída redirecionada (cp1252): não pode quebrar com ✅/❌."""
    import subprocess
    import sys
    from pathlib import Path
    env = {**__import__("os").environ, "PYTHONIOENCODING": "cp1252"}
    r = subprocess.run([sys.executable, "-m", "mt5_extracao.cli", "strategies"], capture_output=True,
                       cwd=str(Path(__file__).resolve().parents[1]), env=env)
    assert r.returncode == 0, r.stderr.decode("cp1252", "replace")
