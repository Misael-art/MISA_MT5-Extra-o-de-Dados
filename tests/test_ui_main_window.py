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
    manager.theme.rich_glyphs = True      # os testes conferem os símbolos, qualquer que seja o Tk da máquina
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
    assert list(tree.get_children()) == ["WIN$N", "PETR4", "WDO$N"]    # com volume primeiro; o resto em ordem alfabética
    assert tree.set("WIN$N", "Detalhes na Base") == "M1 (600 barras)"
    assert tree.set("PETR4", "Detalhes na Base") == ""
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
    assert ui.symbols_tree.set("PETR4", "Seleção") == "☑"
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
    deadline = time.time() + 15                                  # a importação roda em outra thread
    while "mql5:avulso" not in panel.cards and time.time() < deadline:
        ui.pump(0.2)
    ui.pump(0.4)
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


def test_results_panel_button_opens_the_live_panel(ui, tmp_path, monkeypatch):
    """T11.8/T11.9: o painel tem botão na aba Robôs e item nos menus; abre o painel vivo (servidor local)."""
    import urllib.request
    opened = []
    button = ui.robots_panel.panel_button
    assert "Painel de resultados" in button.cget("text")
    try:
        type(ui).open_results_panel(ui, opened.append)
        url = opened[0]
        assert url.startswith("http://127.0.0.1:") and "?k=" in url
        html = urllib.request.urlopen(url, timeout=5).read().decode("utf-8")
        assert "Painel de resultados" in html and "WIN$N" in html           # o ativo que está na base de teste
        type(ui).open_results_panel(ui, opened.append)                      # segundo clique: o mesmo servidor
        assert opened[1] == url
    finally:
        ui._panel_server.stop()
    labels = []
    menu_bar = ui.root.nametowidget(ui.root.cget("menu"))
    for i in range(menu_bar.index("end") + 1):
        if menu_bar.type(i) != "cascade":                                    # no Windows a barra tem outras entradas
            continue
        submenu = menu_bar.nametowidget(menu_bar.entrycget(i, "menu"))
        labels += [submenu.entrycget(j, "label") for j in range(submenu.index("end") + 1)
                   if submenu.type(j) == "command"]
    assert sum("Painel de resultados" in label for label in labels) == 2


def test_results_panel_falls_back_to_the_snapshot_file(ui, tmp_path, monkeypatch):
    from mt5_extracao.bootstrap import paths
    from mt5_extracao.robots import panel_server

    def no_port(*a, **k):
        raise OSError("sem porta")
    monkeypatch.setattr(panel_server, "PanelServer", no_port)
    monkeypatch.setattr(paths, "PROJECT_ROOT", tmp_path)                  # não grava na pasta exports do projeto
    opened = []
    type(ui).open_results_panel(ui, opened.append)
    ui.pump(1.0)
    target = tmp_path / "exports" / "painel_resultados.html"
    assert target.is_file() and opened == [target.resolve().as_uri()]


def _click(tree, handler, iid, column):
    """Clica no meio de uma célula, pelo mesmo caminho do mouse (a linha precisa estar visível)."""
    tree.update_idletasks()
    x, y, width, height = tree.bbox(iid, column)
    return handler(types.SimpleNamespace(x=x + width // 2, y=y + height // 2))


def test_asset_table_columns_star_and_checkbox_clicks(ui):
    """T7.2: colunas novas, clique na estrela grava o favorito no banco e clique na caixa marca o ativo."""
    from mt5_extracao import asset_table
    tree = ui.symbols_tree
    ui.notebook.select(0)
    ui.pump(0.3)
    assert tree.cget("columns") == asset_table.COLUMNS
    assert tree.item("WIN$N", "values") == ("☆", "WIN$N", "600,0 mil", "⚪ N/A", "🔴 Precisa de atualização", "❌ Não",
                                            "M1 (600 barras)", "☐")
    assert tree.item("PETR4", "values")[2:5] == ("—", "⚪ N/A", "⚪ Sem dados")

    assert _click(tree, ui._symbol_clicked, "WDO$N", "★") == "break"
    assert list(tree.get_children()) == ["WDO$N", "WIN$N", "PETR4"] and tree.set("WDO$N", "★") == "★"
    assert asset_table.load_prefs(ui.app.db_manager)["favorites"] == ["WDO$N"]         # gravado no SQLite
    assert ui.inventory_panel.tree.set(next(iter(ui.inventory_panel.rows)), "★") == "☆"
    _click(tree, ui._symbol_clicked, "WDO$N", "★")
    assert asset_table.load_prefs(ui.app.db_manager)["favorites"] == [] and tree.set("WDO$N", "★") == "☆"

    _click(tree, ui._symbol_clicked, "PETR4", "Seleção")
    assert ui.app.selected_symbols == ["PETR4"] and tree.set("PETR4", "Seleção") == "☑"
    assert ui.selected_count_label.cget("text") == "1 ativo(s) selecionado(s)"
    ui.search_var.set("w")
    ui.filter_symbols()
    ui.select_all_visible()                                                             # só as linhas visíveis
    assert sorted(ui.app.selected_symbols) == ["PETR4", "WDO$N", "WIN$N"]
    assert ui.selected_count_label.cget("text") == "3 ativo(s) selecionado(s)"
    ui.clear_selection()
    assert ui.app.selected_symbols == [] and tree.set("WIN$N", "Seleção") == "☐"
    assert ui.selected_count_label.cget("text") == "0 ativo(s) selecionado(s)"

    ui.search_var.set("")
    ui.favorite_symbols.append("PETR4")
    ui.sort_symbols("Volume")
    assert list(tree.get_children()) == ["PETR4", "WIN$N", "WDO$N"]                     # favorito segue no topo
    ui.sort_symbols("Volume")
    assert list(tree.get_children()) == ["PETR4", "WIN$N", "WDO$N"]                     # sem volume fica por último


def test_favorites_file_is_migrated_to_the_database(ui):
    from mt5_extracao import asset_table
    ui.favorite_symbols = ["WDO$N"]                # o que veio do favorites.json antigo
    ui._sync_favorites()
    assert asset_table.load_prefs(ui.app.db_manager)["favorites"] == ["WDO$N"]
    ui.favorite_symbols = []
    ui._sync_favorites()                           # daí em diante quem vale é o banco
    assert ui.favorite_symbols == ["WDO$N"]


def test_backtest_button_follows_the_checked_rows_and_runs_in_background(ui):
    """T7.2: botão desabilitado sem marcação; diálogo de robôs; bateria em segundo plano atualiza a coluna."""
    from mt5_extracao.ui.backtest_dialog import BacktestDialog
    panel = ui.inventory_panel
    ui.notebook.select(panel.frame)
    ui.pump(0.3)
    iid = next(iter(panel.rows))
    assert panel.backtest_button.cget("text") == "Iniciar Backtest" and panel.backtest_button.instate(["disabled"])
    assert _click(panel.tree, panel._clicked, iid, "Seleção") == "break"
    assert panel.backtest_button.cget("text") == "Iniciar Backtest (1 selecionado)"
    assert panel.backtest_button.instate(["!disabled"]) and panel.tree.set(iid, "Seleção") == "☑"
    assert panel.count_label.cget("text") == "1 ativo(s) selecionado(s)"
    panel.uncheck_all()
    assert panel.backtest_button.instate(["disabled"]) and panel.count_label.cget("text") == "0 ativo(s) selecionado(s)"
    panel.check_all()
    assert panel.checked == {iid}

    chosen = []
    dialog = BacktestDialog(ui, panel.checked_items(), ui.robots_panel.robots, chosen.extend)
    assert dialog.run_button.instate(["disabled"]) and len(dialog.vars) == 8
    dialog.set_all(True)
    assert dialog.run_button.cget("text") == "Executar 8 Backtest(s)" and "MQL5" in dialog.note.cget("text")
    dialog.set_all(False)
    for key in ("python:donchian", "python:ema_atr"):
        dialog.vars[key].set(True)
    dialog._update()
    assert dialog.run_button.cget("text") == "Executar 2 Backtest(s)"
    dialog.confirm()
    assert sorted(r.key for r in chosen) == ["python:donchian", "python:ema_atr"]

    assert panel.tree.set(iid, "Robô Lucrativo") == "⚪ N/A"
    batch = panel.run_backtests(panel.checked_items(), chosen)
    assert panel.backtest_button.instate(["disabled"])                 # uma bateria por vez
    deadline = time.time() + 60
    while (batch.running or panel.progress_frame.winfo_manager()) and time.time() < deadline:
        ui.pump(0.2)
    assert [r["error"] for r in batch.results] == ["", ""]
    assert panel.tree.set(iid, "Robô Lucrativo") != "⚪ N/A"
    assert ui.symbols_tree.set("WIN$N", "Robô Lucrativo") == panel.tree.set(iid, "Robô Lucrativo")
    assert ui.status_label.cget("text") == "Backtests concluídos: 2 testado(s), 0 com falha."
    assert panel.backtest_button.instate(["!disabled"]) and panel.progress_var.get() == 100.0
    ui.pump(1.0)                                                       # o catálogo é relido ao fim
    ui.refresh_inventory()
    ui.pump(1.0)
    assert panel.tree.set(iid, "Robô Lucrativo") != "⚪ N/A" and panel.checked == {iid}   # relido do banco


def test_benchmark_is_saved_and_shown(ui):
    from mt5_extracao import asset_table
    ui.set_benchmark("WIN$N", "IBOV")
    iid = next(iter(ui.inventory_panel.rows))
    assert ui.inventory_panel.tree.set(iid, "Benchmark") == "🎯 Sim"
    assert ui.symbols_tree.set("WIN$N", "Benchmark") == "🎯 Sim" and ui.symbols_tree.set("PETR4", "Benchmark") == "❌ Não"
    assert asset_table.load_prefs(ui.app.db_manager)["benchmarks"] == {"WIN$N": "IBOV"}
    ui.set_benchmark("WIN$N", "")
    assert ui.inventory_panel.tree.set(iid, "Benchmark") == "❌ Não"


def test_tables_stay_readable_when_tk_cannot_draw_the_symbols(ui):
    """Tk sem Xft (Linux) não desenha emoji, estrela nem caixa: as tabelas trocam por texto simples."""
    ui.theme.rich_glyphs = False
    ui.favorite_symbols.append("PETR4")
    ui.select_symbols(["PETR4"])
    ui.filter_symbols()
    assert ui.symbols_tree.item("PETR4", "values")[0::7] == ("*", "[x]")
    assert ui.symbols_tree.item("WIN$N", "values") == ("·", "WIN$N", "600,0 mil", "N/A", "Precisa de atualização", "Não",
                                                       "M1 (600 barras)", "[ ]")
    panel = ui.inventory_panel
    panel.show(ui.inventory)
    panel.check_all()
    assert panel.tree.set(next(iter(panel.rows)), "Seleção") == "[x]"
