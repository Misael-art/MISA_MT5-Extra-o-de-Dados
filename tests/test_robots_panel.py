"""T11.7: painel visual em um arquivo HTML só, com os dados embutidos e sem nada da internet."""
import json
import re
import uuid

import pytest

from mt5_extracao import cli
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.robots import catalog, matrix, panel, scaffold, store
from mt5_extracao.timeframes import Timeframe
from tests.market_data import trend


@pytest.fixture
def env(tmp_path):
    db = DatabaseManager(db_type="sqlite", db_path=str(tmp_path / "db.sqlite"))
    db.save_ohlcv_data("EURUSD", Timeframe.H1.label, trend(60, freq="h"))
    scaffold.create("Painel", tmp_path / "robos", magic=720001)
    cfg = tmp_path / "config.ini"
    cfg.write_text(f"[DATABASE]\ntype = sqlite\npath = {tmp_path / 'db.sqlite'}\n[ROBOTS]\nlibrary_dirs = "
                   f"{tmp_path / 'robos'}\n", encoding="utf-8")
    store.save_run(db, {"run_id": uuid.uuid4().hex, "robot_key": "mql5:painel", "source": "strategy tester",
                        "symbol": "EURUSD", "timeframe": "H1", "data_start": "2025-01-01 00:00:00",
                        "data_end": "2025-12-31 00:00:00", "bars": 0, "params": {"preset": "Painel_Conservador"},
                        "metrics": {"trades": 80, "net_profit": 420.0, "profit_factor": 1.4, "return_pct": 0.042,
                                    "win_rate": 0.45, "max_drawdown_pct": 0.06, "tester_model": "cada tick com ticks reais",
                                    "report": "/home/alguem/segredo/mt5x_painel.htm"},
                        "equity": [], "trades": [["2025-01-02 10:00:00", 5.0, 0.1]], "created_at": "2026-01-01 10:00:00"})
    return db, cfg, tmp_path


def embedded(html):
    return json.loads(re.search(r'<script id="data" type="application/json">(.*?)</script>', html, re.S).group(1))


def test_panel_is_one_self_contained_file(env):
    db, cfg, tmp_path = env
    config = cli.services.load_config(str(cfg))
    html = panel.render(matrix.build(db, config))
    assert html.startswith("<!doctype html>") and 'lang="pt-BR"' in html
    assert not re.search(r'(src|href)\s*=\s*["\']https?:', html) and "http" not in html      # nada da internet
    assert "@import" not in html and "<link" not in html
    data = embedded(html)
    assert data["symbols"] == ["EURUSD"] and data["timeframes"] == ["H1"]
    cell = data["cells"][0]
    assert (cell["robot"], cell["status"], cell["trades"]) == ("mql5:painel", matrix.PROMISING, 80)
    assert cell["origin"] == "Strategy Tester, cada tick com ticks reais" and cell["preset"] == "Painel_Conservador"
    for word in ("Por ativo", "Por robô", "O que cada situação significa", "mais recente", "otimistas em custo"):
        assert word in html


def test_panel_carries_no_personal_paths_trades_or_accounts(env):
    db, cfg, tmp_path = env
    html = panel.render(matrix.build(db, cli.services.load_config(str(cfg))))
    assert "/home/" not in html and "segredo" not in html and str(tmp_path) not in html
    assert '"trades":[[' not in html and "equity" not in html                # só os números da célula, não as operações


def test_text_from_the_catalog_cannot_break_out_of_the_script_tag(env):
    db, cfg, tmp_path = env
    data = matrix.build(db, cli.services.load_config(str(cfg)))
    data["robots"][0]["name"] = 'X</script><script>alert(1)</script><!-- '
    html = panel.render(data)
    assert html.count("</script>") == 2                                       # o dos dados e o do código, nada mais
    assert "<" not in html.split('id="data" type="application/json">')[1].split("</script>")[0]
    assert embedded(html)["robots"][0]["name"].startswith("X</script>")       # o texto chega inteiro, como dado


def test_every_status_has_icon_text_and_legend(env):
    db, cfg, _ = env
    html = panel.render(matrix.build(db, cli.services.load_config(str(cfg))))
    icons = json.loads(re.search(r"var ICON = (\{.*?\});", html).group(1))
    assert set(icons) == set(matrix.STATUS_LABELS)                            # nenhuma situação fica só na cor
    assert all(icons.values())


def test_cli_panel_writes_the_file_and_reports_the_counts(env, capsys, monkeypatch):
    db, cfg, tmp_path = env
    out = tmp_path / "saida" / "painel.html"
    assert cli.main(["--config", str(cfg), "panel", "--out", str(out)]) == cli.EXIT_OK
    text = capsys.readouterr().out
    assert out.is_file() and "1 ativo(s), 1 timeframe(s)" in text and "1 promissor" in text and "--open" in text
    opened = []
    import webbrowser
    monkeypatch.setattr(webbrowser, "open", lambda url: opened.append(url))
    assert cli.main(["--config", str(cfg), "--json", "panel", "--out", str(out), "--open"]) == cli.EXIT_OK
    payload = json.loads(capsys.readouterr().out)["data"]
    assert payload["cells"] == 1 and payload["by_status"] == {"promissor": 1} and opened[0].startswith("file://")
    empty = tmp_path / "vazio.ini"
    empty.write_text(f"[DATABASE]\ntype = sqlite\npath = {tmp_path / 'vazio.sqlite'}\n", encoding="utf-8")
    assert cli.main(["--config", str(empty), "panel", "--out", str(tmp_path / "v.html")]) == cli.EXIT_OK
    assert "Ainda não há backtests" in capsys.readouterr().out
