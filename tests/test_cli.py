import csv
import datetime as dt
import subprocess
import sys
from pathlib import Path

import pytest

import mt5_extracao.historical_extractor as he_module
import mt5_extracao.mt5_connector as connector_module
from mt5_extracao import cli, timeframes

REPO = Path(__file__).resolve().parents[1]


@pytest.fixture
def env(fake_mt5, tmp_path, monkeypatch):
    """config.ini temporário + MT5 falso já 'inicializado'."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(he_module.time, "sleep", lambda s: None)
    monkeypatch.setattr(connector_module.time, "sleep", lambda s: None)

    def fake_initialize(self, *a, **k):
        self.is_initialized = True
        return True
    monkeypatch.setattr(connector_module.MT5Connector, "initialize", fake_initialize)

    class FixedNow(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return dt.datetime(2024, 1, 4, 12, 0)
    # "agora" fixo para a atualização incremental não buscar até a data real
    monkeypatch.setattr(he_module, "datetime", FixedNow)
    cfg = tmp_path / "config.ini"
    cfg.write_text(f"[DATABASE]\ntype = sqlite\npath = {tmp_path / 'db.sqlite'}\n"
                   "[EXTRACTION]\nchunk_days_m1 = 1\n", encoding="utf-8")
    return ["--config", str(cfg)]


def test_help_runs_as_module():
    r = subprocess.run([sys.executable, "-m", "mt5_extracao.cli", "--help"], cwd=str(REPO),
                       capture_output=True, text=True)
    assert r.returncode == 0 and "extract" in r.stdout


def test_extract_tables_update_export(env, tmp_path, capsys):
    assert cli.main(env + ["extract", "--symbols", "WIN$N", "--tf", "M1",
                           "--from", "2024-01-02", "--to", "2024-01-03"]) == 0
    assert cli.main(env + ["tables"]) == 0
    out = capsys.readouterr().out
    assert "win_n_1_minuto" in out and "2024-01-03 17:59" in out

    assert cli.main(env + ["update", "--symbols", "WIN$N", "--tf", "M1"]) == 0

    target = tmp_path / "saida.csv"
    assert cli.main(env + ["export", "--table", "win_n_1_minuto", "--out", str(target)]) == 0
    with open(target, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) >= 2 * 540
    assert rows[0]["time"].startswith("2024-01-02 09:00")


def test_symbols(env, capsys):
    assert cli.main(env + ["symbols"]) == 0
    assert "WIN$N" in capsys.readouterr().out


@pytest.mark.parametrize("argv", [
    ["extract", "--symbols", "WIN$N", "--tf", "X9", "--from", "2024-01-02"],
    ["extract", "--symbols", "WIN$N", "--tf", "M1", "--from", "02/01/2024"],
    ["extract", "--symbols", " , ", "--tf", "M1", "--from", "2024-01-02"],
    ["export", "--table", "nao_existe"],
])
def test_usage_errors_exit_2(env, argv):
    assert cli.main(env + argv) == 2


def test_missing_config_exit_1(tmp_path, capsys):
    assert cli.main(["--config", str(tmp_path / "nao.ini"), "tables"]) == 1
    assert "configure" in capsys.readouterr().err


def test_schedule_commands_quote_dollar():
    cron, schtasks = cli.schedule_commands("15m", ["WIN$N", "WDO$N"], timeframes.Timeframe.M1, root=Path("/opt/mt5x"))
    assert cron.startswith("*/15 9-18 * * 1-5 cd /opt/mt5x && ./run.sh --cli update --symbols 'WIN$N,WDO$N' --tf M1")
    assert "/SC MINUTE /MO 15" in schtasks and "--symbols WIN$N,WDO$N" in schtasks
    cron_h, sch_h = cli.schedule_commands("2h", ["PETR4"], timeframes.Timeframe.H1, root=Path("/opt/x"))
    assert cron_h.startswith("0 9-18/2 * * 1-5") and "/SC HOURLY /MO 2" in sch_h


@pytest.mark.parametrize("bad", ["15", "0m", "60m", "25h", "abc"])
def test_schedule_every_invalid(bad):
    with pytest.raises(ValueError):
        cli.schedule_commands(bad, ["A"], timeframes.Timeframe.M1)
