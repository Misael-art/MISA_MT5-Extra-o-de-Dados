"""T7.1: a janela principal abre, lista os ativos, mostra a base e desenha a análise (sem MT5 real)."""
import configparser
import time
import types

import pytest

from mt5_extracao.database_manager import DatabaseManager
from tests.market_data import random_walk

tk = pytest.importorskip("tkinter")


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
                                config=configparser.ConfigParser(), selected_symbols=[],
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
