"""T7.1: a janela principal abre, lista os ativos, mostra a base e desenha a análise (sem MT5 real)."""
import configparser
import time
import types
from pathlib import Path

import pytest

from mt5_extracao.database_manager import DatabaseManager
from tests.market_data import random_walk

tk = pytest.importorskip("tkinter")
LIBRARY = Path(__file__).parent / "robots_library"


@pytest.fixture
def ui(tmp_path):
    try:
        root = tk.Tk()
    except tk.TclError as e:  # servidor sem tela (ex.: CI Linux sem Xvfb)
        pytest.skip(f"sem display gráfico: {e}")
    from mt5_extracao.ui_manager import UIManager
    db = DatabaseManager(db_path=str(tmp_path / "ui.db"))
    assert db.save_ohlcv_data("WIN$N", "1 minuto", random_walk(600, freq="min", start="2024-01-02 09:00"))
    connector = types.SimpleNamespace(
        is_initialized=True, account_summary=lambda: {"login": 1, "server": "Demo", "demo": True},
        get_symbol_info=lambda s: types.SimpleNamespace(spread=5, last=100.0, bid=0, digits=2))
    app = types.SimpleNamespace(root=root, db_manager=db, mt5_connector=connector, mt5_initialized=True,
                                config=_config(), selected_symbols=[],
                                timeframes=[("1 minuto", 1)], symbols=["WIN$N", "WDO$N", "PETR4"])
    manager = UIManager(app)
    manager.favorites_file = str(tmp_path / "favorites.json")
    manager.prefs_file = str(tmp_path / "ui_prefs.json")
    manager.favorite_symbols, manager.prefs = [], {}
    manager.setup_ui()
    manager.pump = lambda seconds=0.6: _pump(root, seconds)
    manager.pump()
    yield manager
    root.destroy()


def _config():
    config = configparser.ConfigParser()
    config.read_string(f"[ROBOTS]\nlibrary_dirs = {LIBRARY}\n")
    return config


def _pump(root, seconds):
    end = time.time() + seconds
    while time.time() < end:
        root.update()
        time.sleep(0.02)


def test_symbols_list_marks_what_is_in_the_database(ui):
    tree = ui.symbols_tree
    assert list(tree.get_children()) == ["WIN$N", "WDO$N", "PETR4"]
    assert tree.set("WIN$N", "Na base") == "M1 (600 barras)"
    assert tree.set("PETR4", "Na base") == ""
    ui.filter_var.set("data")
    ui.filter_symbols()
    assert list(tree.get_children()) == ["WIN$N"]
    ui.filter_var.set("all")
    ui.search_var.set("wdo")
    ui.filter_symbols()
    assert list(tree.get_children()) == ["WDO$N"]


def test_selection_and_favorites_are_remembered(ui):
    ui.symbols_tree.selection_set("PETR4")
    ui.add_symbols()
    ui.toggle_favorites()
    assert ui.app.selected_symbols == ["PETR4"]
    assert ui.symbols_tree.set("PETR4", "Seleção") == "✓ na seleção"
    assert list(ui.symbols_tree.get_children())[0] == "PETR4"          # favoritos vêm primeiro
    assert ui._load_json(ui.prefs_file, {})["selected"] == ["PETR4"]
    assert ui.load_favorites() == ["PETR4"]
    ui.selected_listbox.selection_set(0)
    ui.remove_symbols()
    assert ui.app.selected_symbols == []


def test_inventory_and_analysis_tabs(ui):
    rows = list(ui.inventory_panel.rows.values())
    assert [(r["symbol"], r["timeframe"], r["rows"]) for r in rows] == [("WIN$N", "1 minuto", 600)]
    assert ui.open_analysis("WIN$N", "1 minuto")
    ui.pump(1.5)
    panel = ui.analysis_panel
    assert panel.data is not None and len(panel.data) == 600
    assert set(panel._canvases) == {"chart", "season", "returns"}
    assert panel.tile_values["last"].cget("text") != "—"
    assert "Barras analisadas: 600" in panel.quality_text.get("1.0", "end")


def test_theme_toggle_redraws_without_errors(ui):
    ui.open_analysis("WIN$N")
    ui.pump(1.2)
    ui.toggle_theme()
    ui.pump(0.4)
    assert ui.theme.dark is True
    assert ui._load_json(ui.prefs_file, {})["dark"] is True
    assert set(ui.analysis_panel._canvases) == {"chart", "season", "returns"}


def _labels(widget):
    texts = [widget.cget("text")] if widget.winfo_class() == "TLabel" else []
    for child in widget.winfo_children():
        texts += _labels(child)
    return texts


def test_robots_tab_shows_cards_and_detail(ui):
    """T8.2: cards de robôs MQL5 e Python, filtros, e o painel que abre ao clicar no card."""
    panel = ui.robots_panel
    assert ui.notebook.tab(panel.frame, "text") == "Robôs"
    assert len(panel.cards) == 8 and {"mql5:demoguard", "mql5:martingale", "python:donchian"} <= set(panel.cards)
    card_text = " | ".join(_labels(panel.cards["mql5:demoguard"]))
    assert "DemoGuard" in card_text and "MQL5" in card_text and "v1.20" in card_text
    assert "Risco moderado" in card_text and "3 preset(s): risco Baixo a Alto" in card_text
    assert "Estágio 1 de 6: Importado · falta: compilar no MetaEditor" in card_text
    assert "Sem teste gravado" in card_text and "não compilado" in card_text

    panel.select("mql5:demoguard")
    detail = panel.detail
    assert detail.title.cget("text") == "DemoGuard"
    assert panel.cards["mql5:demoguard"].cget("style") == "Selected.Card.TFrame"
    tree = detail.inputs_tree
    groups = [tree.item(i, "text") for i in tree.get_children()]
    assert groups[:2] == ["1. Risco", "2. Stop"]
    values = {tree.item(i, "text"): tree.item(i, "values")[0] for g in tree.get_children() for i in tree.get_children(g)}
    assert values["InpProfile"] == "DEMO_PROFILE_MODERATE"
    detail.source_var.set("Preset Demo_Agressivo")
    detail._show_values()
    values = {tree.item(i, "text"): tree.item(i, "values")[0] for g in tree.get_children() for i in tree.get_children(g)}
    assert values["InpProfile"] == "DEMO_PROFILE_AGGRESSIVE (2) *" and values["InpRiskPct"] == "1.0"
    assert "Risco alto" in detail.risk_label.cget("text")

    panel.filters["risk"][0].set("Alto")
    panel.render()
    assert set(panel.cards) == {"mql5:martingale"}
    panel.filters["risk"][0].set("Todos")
    panel.search_var.set("donchian")
    assert set(panel.cards) == {"python:donchian"}
    assert "não opera" in detail.live_text.cget("text")


def test_robots_tab_shows_recorded_validation_and_imports(ui, tmp_path):
    from mt5_extracao.strategies import validation
    verdict = {"strategy": "donchian", "approved": True, "status": "✅ aprovada", "params": {},
               "oos_metrics": {"profit_factor": 1.62, "trades": 240},
               "checks": [{"name": "Janelas walk-forward positivas", "value": 0.8}],
               "robustness": {"robust": True}, "monte_carlo": {"dd_percentile": 0.124},
               "data_start": "2020-01-01", "data_end": "2024-01-01"}
    validation.save_validation(ui.app.db_manager, "WIN$N", "D1", verdict)
    panel = ui.robots_panel
    panel.reload()
    ui.pump()
    text = " | ".join(_labels(panel.cards["python:donchian"]))
    assert "1,62" in text and "240" in text and "12,4%" in text and "✓ aprovada em WIN$N D1" in text
    assert "Estágio 2 de 3: Testado" in text
    panel.select("python:donchian")
    assert len(panel.detail.tests_tree.get_children()) == 1

    loose = tmp_path / "Avulso.mq5"
    loose.write_text('input double InpRiskPct = 0.5; // Risco por trade (%)\n'
                     'input int InpStopPoints = 200; // Stop (pontos)\nvoid OnTick() { }\n', encoding="utf-8")
    from mt5_extracao import robots
    panel.run_import(lambda: [robots.import_robot(ui.app.db_manager, loose)])
    ui.pump(1.0)
    assert "mql5:avulso" in panel.cards and panel.selected_key == "mql5:avulso"
    assert panel.detail.title.cget("text") == "Avulso"


def test_robots_tab_survives_theme_toggle(ui):
    ui.toggle_theme()
    ui.pump(0.4)
    assert len(ui.robots_panel.cards) == 8
    assert ui.robots_panel.canvas.cget("background") == ui.theme.colors["page"]


def test_backtest_from_the_robot_card(ui):
    """T8.3: rodar o backtest pelo painel do robô grava a execução e atualiza o card."""
    panel = ui.robots_panel
    panel.select("python:donchian")
    detail = panel.detail
    assert detail.symbol_var.get() == "WIN$N" and detail.timeframe_var.get() == "1 minuto"
    assert detail.run_tiles["trades"].cget("text") == "—" and "Rodar backtest" in detail.run_status.cget("text")
    detail.run_backtest()
    ui.pump(2.5)
    assert detail.shown_run is not None and detail.shown_run["symbol"] == "WIN$N"
    assert detail.run_tiles["trades"].cget("text") != "—"
    assert "WIN$N M1 · 600 barras" in detail.run_status.cget("text")
    text = " | ".join(_labels(panel.cards["python:donchian"]))
    assert "Último backtest em WIN$N M1" in text and "Estágio 2 de 3: Testado" in text
    assert detail.robot.key == "python:donchian" and len(detail.robot.runs) == 1     # painel segue no mesmo robô
    ui.toggle_theme()
    ui.pump(0.4)
    assert detail.shown_run is not None

    panel.select("mql5:demoguard")
    assert not detail.run_box.winfo_ismapped() and "Strategy Tester" in detail.tests_text.cget("text")
