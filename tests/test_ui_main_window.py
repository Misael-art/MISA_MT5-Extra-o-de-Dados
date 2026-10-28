"""T7.1: a janela principal abre, lista os ativos, mostra a base e desenha a análise (sem MT5 real)."""
import configparser
import time
import types
from pathlib import Path

import pytest

from mt5_extracao.database_manager import DatabaseManager
from tests.market_data import random_walk
from tests.test_robots_catalog import PYTHON_KEYS

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
    # A base é lida em segundo plano: em máquina lenta (runner Windows) 0,6 s não bastam. Espera até 10 s.
    end = time.time() + 10
    while not manager.symbol_series and time.time() < end:
        manager.pump(0.1)
    yield manager
    root.destroy()
    import gc
    gc.collect()        # lixo do Tk coletado na thread principal (veja tests/test_ui_laboratory.py)


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


def test_analysis_invalid_prices_clear_old_metrics_and_show_quality(ui, monkeypatch):
    from mt5_extracao import study
    import pandas as pd
    ui.open_analysis("WIN$N", "1 minuto")
    ui.pump(1.5)
    panel = ui.analysis_panel
    assert panel.data is not None
    invalid = panel.data.copy(deep=True)
    invalid.loc[invalid.index[2], "high"] = float("nan")
    before = invalid.copy(deep=True)
    monkeypatch.setattr(ui.app.db_manager, "load_ohlcv", lambda *a, **k: invalid)
    monkeypatch.setattr(study, "summarize", lambda *a: pytest.fail("métrica calculada com preço inválido"))
    panel.load()
    end = time.monotonic() + 5
    while "Análise indisponível" not in panel.status.cget("text") and time.monotonic() < end:
        ui.pump(.05)
    assert "Análise indisponível" in panel.status.cget("text")
    assert panel.data is None and panel.result is None and not panel._canvases
    assert all(label.cget("text") == "—" for label in panel.tile_values.values())
    assert panel.tabs.select() == str(panel.quality_tab)
    quality = panel.quality_text.get("1.0", "end")
    assert "Barras com preço ausente, não numérico ou não finito: 1" in quality
    assert "prontos para estudo" not in quality and "Sobrescrever" not in quality
    pd.testing.assert_frame_equal(invalid, before)


def test_clean_quality_is_a_scoped_check_not_a_coverage_certificate(ui):
    from mt5_extracao import data_quality
    panel = ui.analysis_panel
    panel._show_quality(data_quality.check_quality(random_walk(10)), ("09:00", "18:30"))
    text = panel.quality_text.get("1.0", "end")
    assert "não certificam cobertura completa" in text
    assert "prontos para estudo" not in text


def test_empty_selection_does_not_keep_quality_from_previous_series(ui, monkeypatch):
    import pandas as pd
    ui.open_analysis("WIN$N", "1 minuto")
    ui.pump(1.5)
    panel = ui.analysis_panel
    assert "Barras analisadas: 600" in panel.quality_text.get("1.0", "end")
    monkeypatch.setattr(ui.app.db_manager, "load_ohlcv", lambda *a, **k: pd.DataFrame())
    panel.load()
    end = time.monotonic() + 5
    while "Sem barras" not in panel.status.cget("text") and time.monotonic() < end:
        ui.pump(.05)
    assert "Sem barras" in panel.quality_text.get("1.0", "end")
    assert "Barras analisadas" not in panel.quality_text.get("1.0", "end")
    assert panel.data is None and panel.result is None


def test_offline_mode_keeps_terminal_actions_disabled_and_blocks_direct_calls(ui, monkeypatch):
    from tkinter import ttk
    from mt5_extracao import ui_manager
    calls = []

    def forbidden(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("ação de terminal/conta em modo offline")

    ui.app.offline = True
    ui.app.mt5_connector.initialize = forbidden
    ui.app.mt5_connector.account_summary = forbidden
    ui.mt5_admin_button = ttk.Button(ui.mt5_view_button.master, text="Admin simulado")
    monkeypatch.setattr(ui, "run_bg", forbidden)
    monkeypatch.setattr(ui, "_missing_account", forbidden)
    monkeypatch.setattr(ui_manager.messagebox, "askyesno", forbidden)
    monkeypatch.setattr(ui_manager.messagebox, "showerror", forbidden)
    for _ in range(2):
        assert not ui.refresh_mt5_status()
        assert not ui._mt5_connected()
        for button in (ui.mt5_view_button, ui.check_button, ui.mt5_admin_button, ui.get_history_button,
                       ui.start_button, ui.stop_button):
            assert button.instate(["disabled"])
        assert ui.mt5_status_value.cget("text") == "Análise local (offline)"
        assert "sem --offline" in ui.status_label.cget("text")
        ui.check_mt5_status()
        ui.connect_mt5(startup=True)
        completed = []
        ui.open_mt5_view(on_done=completed.append)
        assert completed == [None]
        ui._offer_demo_account()
        ui._reconnect_mt5()
        ui.launch_mt5_as_admin()
        ui._check_admin_mode()
        ui.start_collection()
        ui.extract_historical_data()
        ui.toggle_collection_buttons(False)
        ui.filter_symbols()
        assert "configurados" in ui.symbols_label.cget("text")
        assert "na conta" not in ui.symbols_label.cget("text")
        assert calls == []
        ui.toggle_theme()


def test_theme_toggle_redraws_without_errors(ui):
    ui.open_analysis("WIN$N")
    ui.pump(1.2)
    ui.toggle_theme()
    ui.pump(0.4)
    assert ui.theme.dark is True
    assert ui._load_json(ui.prefs_file, {})["dark"] is True
    assert set(ui.analysis_panel._canvases) == {"chart", "season", "returns"}


def test_dense_tabs_reflow_and_assets_remain_reachable_in_a_small_window(ui):
    ui.root.geometry("900x620")
    ui.pump(0.4)
    assert ui._symbols_stacked is True
    assert ui._symbols_left_outer.grid_info()["row"] == 0
    assert ui._symbols_right_outer.grid_info()["row"] == 1
    assert ui.symbols_scrollbar.winfo_manager() == "pack"

    ui.notebook.select(ui.analysis_panel.frame)
    ui.pump(0.3)
    assert ui.analysis_panel._controls_stacked is True
    assert ui.analysis_panel._metric_columns <= 3

    ui.notebook.select(ui.robots_panel.frame)
    ui.pump(0.3)
    assert ui.robots_panel._body_stacked is True
    assert ui.robots_panel._filterbar_stacked is True
    assert ui.robots_panel.body_scrollbar.winfo_manager() == "pack"

    ui.notebook.select(ui.resources_panel.frame)
    ui.pump(0.3)
    assert ui.resources_panel._kpi_columns == 2
    assert ui.resources_panel._limits_stacked is True


def _labels(widget):
    texts = [widget.cget("text")] if widget.winfo_class() == "TLabel" else []
    for child in widget.winfo_children():
        texts += _labels(child)
    return texts


def test_robots_tab_shows_cards_and_detail(ui):
    """T8.2: cards de robôs MQL5 e Python, filtros, e o painel que abre ao clicar no card."""
    panel = ui.robots_panel
    assert ui.notebook.tab(panel.frame, "text") == "Robôs"
    assert set(panel.cards) == {"mql5:demoguard", "mql5:martingale", *("python:"+key for key in PYTHON_KEYS)}
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


def test_robot_asset_results_icons_survive_theme_resize_and_select_detail(ui):
    from mt5_extracao.robots import store
    for ident, symbol, net, trades in [("a", "EURUSD", 100, 10), ("b", "EURUSD", -20, 10),
                                      ("c", "WIN$N", 50, 10), ("d", "WDO$N", 0, 10),
                                      ("e", "PETR4", 0, 0)]:
        store.save_run(ui.app.db_manager, dict(run_id=ident, robot_key="python:donchian",
            symbol=symbol, timeframe="H1", source="backtest python", created_at="2026-01-01T00:00:00",
            data_start="2025-01-01", data_end="2026-01-01", bars=600,
            params={}, metrics=dict(net_profit=net, trades=trades, profit_factor=1.2,
                max_drawdown_pct=.1, win_rate=.5), equity=[], trades=[]))
    panel = ui.robots_panel
    panel.reload()
    ui.notebook.select(panel.frame)
    ui.pump()
    def descendants(widget):
        for child in widget.winfo_children():
            yield child
            yield from descendants(child)
    for geometry in ("1280x800", "1920x1080"):
        ui.root.geometry(geometry)
        for _ in range(2):
            panel.search_var.set("Donchian")
            ui.pump(.3)
            card = panel.cards["python:donchian"]
            text = " | ".join(_labels(card))
            assert all(t in text for t in ("4 ativos testados", "Último backtest por ativo", "1 positivo",
                                           "1 negativo", "1 zerado(s)", "1 sem resultado"))
            signals = {w.cget("text"): w for w in descendants(card)
                       if w.winfo_class() == "TLabel" and w.cget("text") in ("1 positivo", "1 negativo")}
            assert len(signals) == 2
            for label in signals.values():
                image = label.cget("image")
                assert image and ui.root.tk.call("image", "width", image[0]) > 0
                assert label.winfo_height() >= label.winfo_reqheight()
            assert signals["1 positivo"].cget("background") != signals["1 negativo"].cget("background")
            panel.select(None)
            signals["1 positivo"].event_generate("<Button-1>")
            ui.pump(.1)
            assert panel.selected_key == panel.detail.robot.key == "python:donchian"
            assert panel.detail.robot.headline["approved"] is None
            ui.toggle_theme()
            ui.pump(.2)


def test_robots_tab_survives_theme_toggle(ui):
    ui.toggle_theme()
    ui.pump(0.4)
    assert len(ui.robots_panel.cards) == len(PYTHON_KEYS)+2
    assert ui.robots_panel.canvas.cget("background") == ui.theme.colors["page"]


def test_backtest_from_the_robot_card(ui):
    """T8.3: rodar o backtest pelo painel do robô grava a execução e atualiza o card."""
    panel = ui.robots_panel
    panel.select("python:donchian")
    detail = panel.detail
    assert detail.symbol_var.get() == "WIN$N" and detail.timeframe_var.get() == "1 minuto"
    assert detail.run_tiles["trades"].cget("text") == "—" and "Rodar backtest" in detail.run_status.cget("text")
    detail.run_backtest()
    # o backtest roda em segundo plano (e grava no livro do Laboratório): no runner do Windows 2,5 s não bastam
    end = time.time() + 20      # o resultado e o card do robô também se atualizam em segundo plano
    while time.time() < end and (detail.shown_run is None or "Último backtest em WIN$N M1" not in
                                 " | ".join(_labels(panel.cards["python:donchian"]))):
        ui.pump(0.2)
    assert detail.shown_run is not None and detail.shown_run["symbol"] == "WIN$N"
    assert detail.run_tiles["trades"].cget("text") != "—"
    assert "WIN$N M1 · 600 barras" in detail.run_status.cget("text")
    assert "Dados com limitações" in detail.run_status.cget("text")
    assert detail.shown_run["metrics"]["data_quality"]["eligibility"]["eligible"]
    assert "para esta seleção" in detail.result_context.cget("text")
    assert ui.profit_cells  # o detalhe usa o mesmo hook de atualização imediata do lote
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
    """
    Clica no meio de uma célula, pelo mesmo caminho do mouse. Se a coluna está fora da área visível (a janela de
    teste é mais estreita no Windows e a última coluna ficava de fora), rola a tabela na horizontal antes, como a
    pessoa faria com a barra de rolagem.
    """
    tree.update_idletasks()
    tree.see(iid)
    box = tree.bbox(iid, column)
    if not box or box[0] < 0 or box[0] + box[2] > tree.winfo_width():
        tree.xview_moveto(1.0 if column == tree.cget("columns")[-1] else 0.0)
        tree.update_idletasks()
        box = tree.bbox(iid, column)
    assert box and 0 <= box[0] and box[0] + box[2] <= tree.winfo_width(), f"coluna {column} fora da área visível"
    x, y, width, height = box
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
    assert dialog.run_button.instate(["disabled"]) and len(dialog.vars) == len(PYTHON_KEYS)+2
    dialog.set_all(True)
    assert dialog.run_button.cget("text") == f"Executar {len(PYTHON_KEYS)+2} Backtest(s)" and "MQL5" in dialog.note.cget("text")
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


# --- T12.L2: opções da organização da janela do MT5 em Arquivo > Configurações ----------------------------------

TERMINAL_INI = ("[Pane-32841]\r\nID=32841\r\nIsVisible=1\r\n[Pane-32843]\r\nID=32843\r\nIsVisible=1\r\n"
                "[Pane-32900]\r\nID=32900\r\nIsVisible=1\r\n[Settings]\r\nSound=1\r\n")


@pytest.fixture
def settings(ui, tmp_path, monkeypatch):
    """Diálogo de configurações com a pasta do MT5 e o config.ini em uma pasta temporária (sem MT5 real)."""
    from mt5_extracao.ui import settings as settings_module
    data, common = tmp_path / "mt5", tmp_path / "Common"
    (data / "Config").mkdir(parents=True)
    common.mkdir()
    (data / "Config" / "terminal.ini").write_bytes(TERMINAL_INI.encode("utf-16"))
    config_file = tmp_path / "config.ini"
    config_file.write_text("# meu arquivo\n[MT5]\n# onde está o terminal\npath = /opt/mt5\n", encoding="utf-8")
    ui.app.config_path = str(config_file)
    state = types.SimpleNamespace(data=data, common=common, config_file=config_file, running=False)
    monkeypatch.setattr(settings_module.mt5_tools, "data_dir", lambda config, mt5=None: state.data)
    monkeypatch.setattr(settings_module.mt5_tools, "common_files_dir", lambda config: state.common)
    monkeypatch.setattr(settings_module.mt5_tools, "terminal_running", lambda config=None: state.running)
    monkeypatch.setattr(settings_module.SettingsDialog, "CLOCK_WAIT", 0.05)     # sem esperar o relógio do servidor
    dialog = ui.open_settings()
    ui.pump(0.5)
    dialog.env = state
    yield dialog
    if dialog.window.winfo_exists():
        dialog.window.destroy()


def test_settings_show_the_layout_options_on_by_default(ui, settings):
    from mt5_extracao.robots import layout
    assert settings.window.title() == "Configurações"
    assert "Ligada." == settings.status_label.cget("text")
    assert set(settings.vars) == {"manage_layout", "layout_hide_navigation", "layout_hide_toolbox",
                                  "layout_hide_toolbars", "layout_own_profile", "view_panel"}
    assert all(var.get() for var in settings.vars.values())
    assert [settings.checks[key].cget("text") for key, *_ in layout.OPTION_KEYS] == [
        "Fechar a Observação do Mercado e o Navegador", "Fechar a Caixa de Ferramentas e o Testador de Estratégia",
        "Esconder as barras de ferramentas", "Abrir o gráfico do robô sozinho e maximizado",
        "Mostrar o painel do HADOUKEN no gráfico"]
    assert "Nenhum arranjo guardado" in settings.arrangement_label.cget("text")
    assert settings.restore_button.instate(["disabled"]) and not settings.panel_button.winfo_manager()
    assert "conta 1 em Demo (demonstração)" in " ".join(_labels(settings.window))
    assert str(settings.env.config_file) in " ".join(_labels(settings.window))


def test_settings_save_each_option_keeping_the_comments_of_the_file(ui, settings):
    from mt5_extracao.robots import layout
    settings.checks["layout_hide_toolbars"].invoke()                    # o clique do usuário
    text = settings.env.config_file.read_text(encoding="utf-8")
    assert text == "# meu arquivo\n[MT5]\n# onde está o terminal\npath = /opt/mt5\nlayout_hide_toolbars = false\n"
    assert ui.app.config.getboolean("MT5", "layout_hide_toolbars") is False       # vale sem reiniciar o aplicativo
    assert "Salvo em config.ini" in settings.feedback.cget("text") and "Aplicar e abrir" in settings.feedback.cget("text")
    assert layout.options(ui.app.config).toolbars is False and layout.options(ui.app.config).navigation is True
    settings.checks["layout_hide_toolbars"].invoke()
    assert "layout_hide_toolbars = true\n" in settings.env.config_file.read_text(encoding="utf-8")

    settings.checks["manage_layout"].invoke()                           # o interruptor geral desabilita as partes
    assert "Desligada: o programa não mexe" in settings.status_label.cget("text")
    assert all(settings.checks[key].instate(["disabled"]) for key, *_ in layout.OPTION_KEYS)
    assert not layout.enabled(ui.app.config, settings.env.common)
    settings.checks["manage_layout"].invoke()
    assert all(settings.checks[key].instate(["!disabled"]) for key, *_ in layout.OPTION_KEYS)
    for key, *_ in layout.OPTION_KEYS:
        settings.checks[key].invoke()
    assert "sem nenhuma parte marcada" in settings.status_label.cget("text")
    assert settings.env.config_file.read_text(encoding="utf-8").startswith("# meu arquivo\n[MT5]\n# onde está o terminal\n")


def test_settings_reopen_with_what_was_saved(ui, settings):
    settings.checks["layout_own_profile"].invoke()
    settings.window.destroy()
    again = ui.open_settings()
    ui.pump(0.5)
    assert again.vars["layout_own_profile"].get() is False and again.vars["layout_hide_toolbox"].get() is True
    again.window.destroy()


@pytest.mark.parametrize("restore_delay", [0.0, 1.0])
def test_settings_restore_the_arrangement_only_with_the_terminal_closed(ui, settings, monkeypatch, restore_delay):
    from mt5_extracao.robots import layout
    original_restore = layout.restore
    restore_calls = []

    def restore_with_delay(data):
        restore_calls.append(data)
        time.sleep(restore_delay)  # I/O deliberadamente lento; não é a sincronização do teste.
        return original_restore(data)

    monkeypatch.setattr(layout, "restore", restore_with_delay)

    def wait_for_state(expected_backup, *, operation=False):
        def ready():
            pending = settings.feedback.cget("text").startswith("Conferindo se o MetaTrader 5")
            return (not (operation and pending) and settings.state is not None
                    and settings.state["has_arrangement"] == expected_backup
                    and settings.restore_button.instate(["!disabled"] if expected_backup else ["disabled"]))

        deadline = time.monotonic() + 10
        while not ready() and time.monotonic() < deadline:
            ui.pump(0.05)
        assert ready(), "A operação e a atualização do diálogo não terminaram em 10 s"

    layout.apply(settings.env.data)
    settings.refresh()
    wait_for_state(True)
    assert "Há um arranjo anterior guardado" in settings.arrangement_label.cget("text")
    assert "barra de ferramentas" in settings.arrangement_label.cget("text")
    assert settings.restore_button.instate(["!disabled"])
    settings.env.running = True
    settings.restore_button.invoke()
    wait_for_state(True, operation=True)
    assert "está aberto" in settings.feedback.cget("text") and layout.has_arrangement(settings.env.data)
    assert not restore_calls
    settings.env.running = False
    settings.restore_button.invoke()
    wait_for_state(False, operation=True)
    assert "Arranjo anterior devolvido" in settings.feedback.cget("text")
    assert "continua ligada" in settings.feedback.cget("text")
    assert layout.ini_path(settings.env.data).read_bytes().decode("utf-16") == TERMINAL_INI
    assert settings.restore_button.instate(["disabled"])

    assert restore_calls == [settings.env.data]


def test_settings_show_and_undo_the_panel_button_switch(ui, settings):
    from mt5_extracao.robots import layout
    layout.set_panel_off(settings.env.common, True)                     # botão TELA do painel do HADOUKEN
    settings.refresh()
    ui.pump(0.5)
    assert "Desligada pelo botão TELA" in settings.status_label.cget("text")
    assert settings.panel_button.winfo_manager() == "pack" and settings.vars["manage_layout"].get() is True
    settings.panel_button.invoke()
    ui.pump(0.5)
    assert not (settings.env.common / layout.OFF_FILE).exists() and settings.status_label.cget("text") == "Ligada."
    assert not settings.panel_button.winfo_manager()


def test_settings_work_without_the_mt5_folder_and_report_a_write_failure(ui, settings, monkeypatch):
    from mt5_extracao.ui import settings as settings_module

    def missing(config, mt5=None):
        raise FileNotFoundError("sem pasta")
    monkeypatch.setattr(settings_module.mt5_tools, "data_dir", missing)
    settings.env.common = None
    settings.refresh()
    ui.pump(0.5)
    assert "Pasta de dados do MetaTrader 5 não encontrada" in settings.arrangement_label.cget("text")
    assert settings.restore_button.instate(["disabled"]) and settings.status_label.cget("text") == "Ligada."

    def refuse(*args):
        raise PermissionError("somente leitura")
    monkeypatch.setattr(settings_module.config_builder, "set_config_value", refuse)
    before = settings.env.config_file.read_text(encoding="utf-8")
    settings.checks["layout_hide_toolbox"].invoke()
    assert "Não foi possível gravar" in settings.feedback.cget("text")
    assert settings.vars["layout_hide_toolbox"].get() is True           # a caixa volta: nada foi salvo
    assert settings.env.config_file.read_text(encoding="utf-8") == before
    assert ui.app.config.getboolean("MT5", "layout_hide_toolbox", fallback=True) is True


# --- T12.L3: botão "Abrir visão MT5" e disposição do painel nas opções -------------------------------------------

@pytest.fixture
def view_calls(ui, settings, monkeypatch):
    """Troca a abertura real do MT5 por um registro; `answer` é a resposta à pergunta de fechar e reabrir."""
    from mt5_extracao.robots import mt5_tools, view
    from mt5_extracao.ui import settings as settings_module
    calls = types.SimpleNamespace(opened=[], asked=[], answer=True, fail=None, env=settings.env, symbols=[])
    monkeypatch.setattr(mt5_tools, "terminal_running", lambda config=None: calls.env.running)

    def open_view(config, close_terminal=False, spawn=None, symbol="", timeframe="", **extra):
        calls.symbols.append(symbol)
        if calls.fail:
            raise calls.fail
        calls.opened.append(close_terminal)
        return {"reopened": close_terminal, "organized": True, "hidden": ["barra de ferramentas"], "restored": False,
                "profile": "", "chart": "", "panel": "", "panel_trading": False, "panel_note": "",
                "panel_layout": "auto", "panel_charts": 0, "bridge": False}
    monkeypatch.setattr(view, "open_view", open_view)

    def ask(title, message, **kwargs):
        calls.asked.append(message)
        return calls.answer
    monkeypatch.setattr("mt5_extracao.ui_manager.messagebox.askyesno", ask)
    monkeypatch.setattr("mt5_extracao.ui_manager.messagebox.showerror", lambda *a, **k: calls.asked.append(a[1]))
    assert settings_module.mt5_tools is mt5_tools
    return calls


def test_open_view_button_has_an_icon_and_opens_without_asking_when_mt5_is_closed(ui, view_calls):
    button = ui.mt5_view_button
    assert button.cget("text") == "Abrir visão MT5" and str(button.cget("image")) and ui._view_icon.width() == 20
    assert str(button.cget("compound")) == "left"
    button.invoke()
    assert button.instate(["disabled"])                                  # um pedido por vez
    ui.pump(0.8)
    assert view_calls.opened == [False] and view_calls.asked == []
    assert view_calls.symbols == [""]                                     # sem ativo em foco: o gráfico que já havia
    assert button.instate(["!disabled"])
    ui.update_symbol_details("PETR4")
    button.invoke()
    ui.pump(0.8)
    assert view_calls.symbols == ["", "PETR4"]                       # o ativo em foco dá o gráfico da visão
    assert ui.status_label.cget("text") == "Status: MetaTrader 5 aberto com a visão escolhida"


def test_open_view_asks_before_closing_an_open_terminal(ui, view_calls):
    view_calls.env.running, view_calls.answer = True, False
    ui.mt5_view_button.invoke()
    ui.pump(0.8)
    assert len(view_calls.asked) == 1 and "Fechar e abrir de novo agora?" in view_calls.asked[0]
    assert "robô" in view_calls.asked[0] and view_calls.opened == []      # respondeu Não: nada foi feito
    assert ui.status_label.cget("text") == "Status: visão do MetaTrader 5 não alterada"
    view_calls.answer = True
    ui.mt5_view_button.invoke()
    ui.pump(0.8)
    assert view_calls.opened == [True] and "Verificar conexão" in ui.status_label.cget("text")


def test_open_view_reports_a_failure_with_what_to_check(ui, view_calls):
    view_calls.fail = FileNotFoundError("Terminal não encontrado em /opt/mt5")
    ui.mt5_view_button.invoke()
    ui.pump(0.8)
    assert "Terminal não encontrado em /opt/mt5" in view_calls.asked[0] and "[MT5] path" in view_calls.asked[0]
    assert ui.mt5_view_button.instate(["!disabled"]) and "não foi possível" in ui.status_label.cget("text")


def test_settings_choose_the_panel_layout_and_apply_now(ui, settings, view_calls):
    from mt5_extracao.robots import layout
    assert settings.panel_var.get() == "Automática (pelo tamanho do gráfico)"
    assert list(settings.panel_combo.cget("values")) == [label for label, _ in layout.PANEL_LAYOUTS.values()]
    settings.panel_combo.set("Ampla (duas colunas, telas grandes)")
    settings.panel_combo.event_generate("<<ComboboxSelected>>")
    ui.pump(0.2)
    text = settings.env.config_file.read_text(encoding="utf-8")
    assert text.endswith("path = /opt/mt5\npanel_layout = wide\n") and text.startswith("# meu arquivo\n")
    assert layout.panel_layout(ui.app.config) == "wide" and "Aplicar e abrir" in settings.feedback.cget("text")

    assert settings.apply_button.cget("text") == "Aplicar e abrir o MT5 agora" and str(settings.apply_button.cget("image"))
    settings.apply_button.invoke()
    ui.pump(0.9)
    assert view_calls.opened == [False] and settings.apply_button.instate(["!disabled"])
    assert "MetaTrader 5 aberto" in settings.feedback.cget("text")
    assert "barra de ferramentas" in settings.feedback.cget("text")

    view_calls.env.running, view_calls.answer = True, False              # MT5 aberto e o usuário desiste
    settings.apply_button.invoke()
    ui.pump(0.9)
    assert view_calls.opened == [False] and settings.feedback.cget("text") == "Nada foi alterado no MetaTrader 5."
    settings.window.destroy()
    again = ui.open_settings()
    ui.pump(0.5)
    assert again.panel_var.get() == "Ampla (duas colunas, telas grandes)"
    again.window.destroy()


# --- a janela aparece antes de conectar; o MT5 abre organizado; a falta de conexão é explicada --------------------

@pytest.fixture
def connecting(ui, monkeypatch):
    """Conector e MT5 simulados para `connect_mt5`: `ok` decide se conecta; `online` e `running`, o diagnóstico."""
    from mt5_extracao import connection_help
    from mt5_extracao.robots import mt5_tools, view
    env = types.SimpleNamespace(ok=True, online=True, running=False, opened=[], warned=[], tries=0, slow=0.0)

    def initialize(recursion_count=0):
        env.tries += 1
        time.sleep(env.slow)
        ui.app.mt5_connector.is_initialized = env.ok
        return env.ok
    ui.app.mt5_connector.initialize = initialize
    ui.app.mt5_connector.is_initialized = False
    ui.app.mt5_initialized = False
    ui.app.load_symbols = lambda: setattr(ui.app, "symbols", ["EURUSD", "PETR4"])
    monkeypatch.setattr(mt5_tools, "terminal_running", lambda config=None: env.running)
    monkeypatch.setattr(mt5_tools, "use_config", lambda config: None)

    def open_view(config, close_terminal=False, spawn=None, symbol="", timeframe="", **extra):
        env.opened.append(symbol)
        env.running = True
        return {"reopened": False, "organized": True, "hidden": [], "restored": False, "profile": "", "chart": "",
                "panel": "", "panel_trading": False, "panel_note": "",
                "panel_layout": "auto", "panel_charts": 0, "bridge": False}
    monkeypatch.setattr(view, "open_view", open_view)
    monkeypatch.setattr(connection_help, "internet_ok", lambda *a, **k: env.online)
    monkeypatch.setattr("mt5_extracao.ui_manager.messagebox.showwarning", lambda t, m, **k: env.warned.append(m))
    monkeypatch.setattr(type(ui), "_missing_account", staticmethod(lambda: False))
    ui.refresh_mt5_status(quiet=True)
    return env


def test_connect_runs_in_background_opens_mt5_organized_and_loads_the_symbols(ui, connecting):
    connecting.slow = 0.6
    ui.select_symbols(["PETR4"])
    ui.connect_mt5(startup=True)
    assert ui.mt5_status_value.cget("text") == "Abrindo e conectando ao MetaTrader 5..."
    assert ui.check_button.instate(["disabled"])
    ui.connect_mt5()                                                   # segundo pedido enquanto conecta: ignorado
    ui.pump(0.2)
    ui.notebook.select(1)                                              # a janela segue utilizável enquanto conecta
    assert ui.notebook.index("current") == 1
    ui.pump(1.2)
    assert connecting.opened == [""] and connecting.tries == 1         # MT5 fechado: aberto já organizado, uma vez
    # sem ativo: a visão parte do gráfico que a pessoa já tinha, não do primeiro item da seleção salva
    assert ui.app.mt5_initialized is True and sorted(ui.symbols_tree.get_children()) == ["EURUSD", "PETR4"]
    assert "Conectado: conta 1 em Demo" in ui.mt5_status_value.cget("text") and connecting.warned == []
    assert ui.check_button.instate(["!disabled"])


def test_connect_does_not_reopen_a_terminal_that_is_already_open(ui, connecting):
    connecting.running = True
    ui.connect_mt5()
    ui.pump(0.8)
    assert connecting.opened == [] and connecting.tries == 1 and ui.app.mt5_initialized is True


@pytest.mark.parametrize("worker_delay", [0.0, 1.0])
def test_no_internet_is_said_plainly_with_what_to_do(ui, connecting, worker_delay):
    connecting.slow = worker_delay

    def wait_for_attempt():
        deadline = time.monotonic() + 10
        while getattr(ui, "_connecting", False) and time.monotonic() < deadline:
            ui.pump(0.05)
        assert not getattr(ui, "_connecting", False), "A tentativa de conexão não terminou em 10 s"
        assert ui.check_button.instate(["!disabled"])

    connecting.ok, connecting.online = False, False
    ui.connect_mt5(startup=True)
    wait_for_attempt()
    assert len(connecting.warned) == 1 and "sem internet" in connecting.warned[0]
    assert "Verificar conexão" in connecting.warned[0]
    assert ui.status_label.cget("text") == "Status: sem conexão com o MetaTrader 5 (sem internet)"
    assert ui.mt5_status_value.cget("text") == "MetaTrader 5 desconectado" and ui.app.mt5_initialized is False
    assert ui.check_button.instate(["!disabled"])

    connecting.online = True                                           # com internet: o terminal abriu e não entrou
    ui.check_mt5_status()                                              # o botão "Verificar conexão"
    wait_for_attempt()
    assert "não entrou na conta" in connecting.warned[1]
    connecting.ok = True                                               # a rede voltou
    ui.check_mt5_status()
    wait_for_attempt()
    assert len(connecting.warned) == 2 and "Conectado" in ui.mt5_status_value.cget("text")


def test_a_failure_to_organize_the_window_does_not_stop_the_connection(ui, connecting, monkeypatch):
    from mt5_extracao.robots import view

    def broken(*args, **kwargs):
        raise FileNotFoundError("terminal.ini ilegível")
    monkeypatch.setattr(view, "open_view", broken)
    ui.connect_mt5()
    ui.pump(0.8)
    assert connecting.tries == 1 and ui.app.mt5_initialized is True


def test_closing_the_window_while_connecting_does_not_raise(ui, connecting):
    """Visto no host: fechar a janela durante a verificação gerava uma exceção não tratada."""
    def destroyed(*args, **kwargs):
        raise tk.TclError('invalid command name ".!frame.!frame.!label2"')
    ui.refresh_mt5_status = destroyed
    ui.check_mt5_status()                                              # não propaga
    from mt5_extracao.database_manager import DatabaseManager
    assert callable(DatabaseManager.close)
    ui.app.db_manager.close()
    ui.app.db_manager.close()                                          # encerrar duas vezes não dá erro


def test_settings_release_the_stop_and_turn_the_demo_block_on_and_off(ui, settings):
    """T12.L4: em demo o painel manual abre liberado; a pessoa pode religar o bloqueio e liberar o PARAR TUDO."""
    from mt5_extracao.robots import layout
    kill = settings.env.common / "HDK_KILL.txt"
    assert settings.block_var.get() is False and not settings.stop_row.winfo_manager()
    assert "bloqueado mesmo em conta de demonstração" in settings.block_check.cget("text")
    settings.block_check.invoke()
    assert settings.env.config_file.read_text(encoding="utf-8").endswith("panel_demo_blocked = true\n")
    assert layout.panel_blocked(ui.app.config) is True and "só para visualização" in settings.feedback.cget("text")
    settings.block_check.invoke()
    assert layout.panel_blocked(ui.app.config) is False and "liberado para operar" in settings.feedback.cget("text")

    kill.write_text("PARAR TUDO acionado pelo mt5x")
    settings.refresh()
    ui.pump(0.5)
    assert settings.stop_row.winfo_manager() == "pack" and "PARAR TUDO acionado" in settings.stop_label.cget("text")
    settings.block_check.invoke()
    settings.block_check.invoke()                                       # ao liberar, lembra que a parada segue ativa
    assert "O PARAR TUDO continua acionado" in settings.feedback.cget("text") and kill.exists()
    settings.release_button.invoke()                                    # só um clique da pessoa libera
    ui.pump(0.5)
    assert not kill.exists() and not settings.stop_row.winfo_manager()
    assert "PARAR TUDO liberado" in settings.feedback.cget("text")


def test_settings_any_hour_option_and_server_clock(ui, settings):
    """O painel conta o horário no relógio do servidor; a opção de operar em qualquer horário fica gravada."""
    from mt5_extracao.robots import layout
    assert settings.hours_var.get() is False and "qualquer horário" in settings.hours_check.cget("text")
    assert "horário do painel é o do servidor" in settings.server_label.cget("text")     # sem cotação: só o aviso
    settings.hours_check.invoke()
    assert settings.env.config_file.read_text(encoding="utf-8").endswith("panel_any_hours = true\n")
    assert layout.panel_any_hours(ui.app.config) is True and "qualquer horário" in settings.feedback.cget("text")
    assert "Aplicar e abrir o MT5 agora" in settings.feedback.cget("text")
    frozen = time.time() + 6 * 3600
    ui.app.mt5_connector.get_symbol_info = lambda s: types.SimpleNamespace(spread=5, last=1.0, bid=0, digits=2, time=frozen)
    settings.refresh()
    ui.pump(0.5)
    assert "Sem cotação recente" in settings.server_label.cget("text")      # cotação parada (mercado fechado): não afirma
    ui.app.mt5_connector.get_symbol_info = lambda s: types.SimpleNamespace(
        spread=5, last=1.0, bid=0, digits=2, time=time.time() + 6 * 3600)                 # servidor 6 h à frente do UTC
    settings.refresh()
    ui.pump(0.6)
    text = settings.server_label.cget("text")
    assert text.startswith("Horário do servidor da corretora agora: ") and "do seu computador" in text
    assert settings.hours_var.get() is True
    settings.hours_check.invoke()
    assert layout.panel_any_hours(ui.app.config) is False and "janela de horário" in settings.feedback.cget("text")


def test_automatic_reconnection_also_opens_the_terminal_organized(ui, connecting):
    """Visto no host: a pessoa fechou o MT5 e a reconexão automática o reabriu sem a organização e sem o painel."""
    ui.app.mt5_connector.prepare_terminal = None            # o conector de verdade tem este gancho
    ui.connect_mt5()
    ui.pump(0.8)
    hook = ui.app.mt5_connector.prepare_terminal
    assert callable(hook) and connecting.opened == [""]     # aberto organizado uma vez, na conexão
    hook()                                                   # reconexão com o terminal aberto: nada a fazer
    assert connecting.opened == [""]
    connecting.running = False                               # a pessoa fechou o MetaTrader 5
    hook()                                                   # o conector chama isto antes de reconectar
    assert connecting.opened == ["", ""]


def test_connector_calls_the_prepare_hook_before_connecting(monkeypatch, tmp_path):
    from mt5_extracao import mt5_connector
    calls = []
    fake = types.SimpleNamespace(initialize=lambda **k: calls.append("initialize") or False,
                                 last_error=lambda: (-1, "sem terminal"), shutdown=lambda: None)
    monkeypatch.setattr(mt5_connector, "mt5", fake)
    cfg = tmp_path / "config.ini"
    cfg.write_text(f"[MT5]\npath = {tmp_path}\n", encoding="utf-8")
    connector = mt5_connector.MT5Connector(config_path=str(cfg))
    assert connector.prepare_terminal is None
    connector.prepare_terminal = lambda: calls.append("preparar")
    connector.initialize()
    assert calls and calls[0] == "preparar" and calls.count("preparar") == 1      # uma vez, antes de conectar

    def broken():
        raise RuntimeError("terminal.ini ilegível")
    connector.prepare_terminal = broken
    connector.initialize()                                   # falha ao preparar não impede a tentativa de conexão


def test_inventory_attention_keeps_sort_selection_refresh_and_theme(ui):
    from mt5_extracao import asset_table
    from mt5_extracao.ui.visual_tokens import result_attention_colors
    ui.profit_cells = {("WIN$N", "M1"): {"python:a": (10, .01)}}
    panel = ui.inventory_panel
    panel.show(ui.inventory)
    key = next(iter(panel.rows))
    assert panel.tree.item(key, "tags") == ("result.positive",)
    ui.notebook.select(panel.frame)
    ui.pump(0.2)
    ui.root.focus_force()
    panel.tree.focus_set()
    end = time.monotonic() + 5
    while ui.root.focus_get() is not panel.tree and time.monotonic() < end:
        ui.pump(0.05)
    assert ui.root.focus_get() is panel.tree
    panel.tree.selection_set(key)
    panel.tree.event_generate("<space>")
    panel.sort_by(asset_table.COL_PROFIT)
    assert panel.tree.selection() == (key,) and key in panel.checked
    ui.profit_cells[("WIN$N", "M1")]["python:a"] = (-10, -.01)
    panel.profit_changed("WIN$N")
    assert panel.tree.item(key, "tags") == ("result.negative",)
    assert "-1,0%" in panel.tree.set(key, asset_table.COL_PROFIT)
    ui.theme.toggle()
    fill, text = result_attention_colors(-1, ui.theme.colors, background="field")
    assert str(panel.tree.tag_configure("result.negative", "background")) == fill
    assert str(panel.tree.tag_configure("result.negative", "foreground")) == text

    from tkinter import ttk
    style = ttk.Style(ui.root)
    assert str(style.lookup(panel.tree.cget("style"), "background", ("selected",))) == ui.theme.colors["accent"]
    assert str(style.lookup(panel.tree.cget("style"), "foreground", ("selected",))) == ui.theme.colors["on_accent"]


def test_robot_backtest_keeps_result_context_and_pending_request(ui, monkeypatch):
    from copy import deepcopy
    panel = ui.robots_panel
    panel.select("python:donchian")
    detail = panel.detail
    run = {"run_id": "context-test", "robot_key": "python:donchian", "symbol": "WIN$N", "timeframe": "M1",
           "bars": 600, "data_start": "2024-01-02 09:00", "data_end": "2024-01-02 18:59",
           "created_at": "2024-02-01 12:00", "metrics": {"trades": 2, "return_pct": -0.1},
           "equity": [["2024-01-02", 100], ["2024-01-03", 90]]}
    detail.show_run(run)
    assert "Qualidade dos dados não registrada" in detail.run_status.cget("text")
    assert "Resultado armazenado" in detail.result_context.cget("text")
    pending, recorded = [], []
    monkeypatch.setattr(ui, "run_bg", lambda work, done: pending.append((work, done)))
    monkeypatch.setattr(ui, "backtest_recorded", lambda result: recorded.append(result))
    monkeypatch.setattr(panel, "reload", lambda: None)
    detail.run_backtest()
    detail.run_backtest()
    assert len(pending) == 1 and detail.run_button.instate(["disabled"])
    detail.period_var.set("1 mês")
    pending.pop()[1](run, None)
    assert recorded == [run] and detail.shown_run is run
    assert "Resultado anterior" in detail.result_context.cget("text")
    detail.run_backtest()
    pending.pop()[1](None, RuntimeError("falha simulada"))
    assert detail.shown_run is run and "falha simulada" in detail.execution_status.cget("text")
    assert "não foi atualizado" in detail.execution_status.cget("text")
    assert "600 barras" in detail.run_status.cget("text")
    detail.run_backtest()
    updated = deepcopy(run);updated["run_id"] = "updated"
    pending.pop()[1](updated, None)
    assert "para esta seleção" in detail.result_context.cget("text")
    detail.show_run(deepcopy(updated))  # releitura do catálogo mantém a proveniência do mesmo run
    assert "para esta seleção" in detail.result_context.cget("text")
    detail.run_backtest()
    panel.select("python:ema_atr")
    before = detail.shown_run
    pending.pop()[1](run, None)
    assert detail.robot.key == "python:ema_atr" and detail.shown_run is before
    assert recorded[-1] is run


def test_robot_backtest_displays_stored_composition_without_recalculating(ui):
    from mt5_extracao.composition import base_composition
    panel = ui.robots_panel
    panel.select("python:donchian")
    value = base_composition("donchian", symbol="WIN$N", timeframe="M1", risk_fraction=.0025, compound=False).to_dict()
    value["protection"] = {"mode": "fixed_points", "params": {"stop_points": 100}}
    value["profit_exit"] = {"mode": "fixed_r", "params": {"target_r": 1.5}}
    run = {"run_id": "composed", "robot_key": "python:donchian", "symbol": "WIN$N", "timeframe": "M1",
           "bars": 600, "data_start": "2024-01-02", "data_end": "2024-01-03", "created_at": "2024-02-01",
           "metrics": {"trades": 2, "composition": value}, "equity": [["2024-01-02", 100], ["2024-01-03", 90]]}
    panel.detail.show_run(run)
    text = panel.detail.run_status.cget("text")
    for expected in ("Composição registrada", "capital inicial", "100 pontos", "1,5 R"):
        assert expected in text
    assert panel.detail.shown_run is run and run["equity"][-1][1] == 90


def test_robot_backtest_controls_reflow_inside_available_width(ui):
    ui.notebook.select(ui.robots_panel.frame)
    ui.robots_panel.select("python:donchian")
    detail = ui.robots_panel.detail
    detail.tabs.select(1)
    for _ in range(2):
        for width in (1920, 1280, 820, 1920):
            ui.root.geometry(f"{width}x800");ui.pump(0.15)
            for cell in detail.run_control_cells:
                assert cell.winfo_ismapped()
                assert cell.winfo_rootx() >= detail.run_controls.winfo_rootx()
                assert cell.winfo_rootx() + cell.winfo_width() <= detail.run_controls.winfo_rootx() + detail.run_controls.winfo_width()
            assert detail.run_button.winfo_width() >= detail.run_button.winfo_reqwidth()
        ui.toggle_theme()


@pytest.mark.parametrize("value,expected", [(2.5, "+2,50%"), (-2.5, "-2,50%"), (0, "0,00%"),
                                           (None, "—"), (float("nan"), "—"), (float("inf"), "—")])
def test_market_return_attention_preserves_zero_missing_and_theme(ui, value, expected):
    from mt5_extracao.ui.visual_tokens import result_attention_colors, contrast_ratio
    from tkinter import ttk
    panel = ui.analysis_panel
    summary = {"last": 1.2, "return_pct": value, "volatility_pct": 2.0, "max_drawdown_pct": -3.0,
               "mean_range_pct": .1, "up_bars_pct": 50.0}
    original = dict(summary)
    for _ in range(2):
        panel._show_summary(summary)
        label = panel.tile_values["return_pct"]
        assert label.cget("text") == expected
        fill, ink = result_attention_colors(value, ui.theme.colors)
        style = ttk.Style(ui.root)
        assert style.lookup(label.cget("style"), "background").lower() == fill.lower()
        assert style.lookup(label.cget("style"), "foreground").lower() == ink.lower()
        assert contrast_ratio(ink, fill) >= 4.5
        ui.toggle_theme()
        assert label.cget("text") == expected
    assert summary == original
    assert "Não mede lucro do robô" in panel.return_legend.cget("text")
    panel._empty("Sem dados")
    assert panel.tile_values["return_pct"].cget("text") == "—"


def _set_ui_scaling(ui, scaling):
    ui.root.tk.call("tk", "scaling", scaling)
    for name in ("font", "font_small", "font_bold", "font_section", "font_title", "font_value"):
        font = getattr(ui.theme, name)
        font.configure(size=font.cget("size"))
    if scaling >= 2:
        # Também cobre a fonte maior usada pela aplicação real, não apenas o Tk padrão da fixture.
        ui.theme.font.configure(size=13)
        ui.theme.font_bold.configure(size=13)
    ui._schedule_header_layout()
    ui.analysis_panel._schedule_layout()


@pytest.mark.parametrize("scaling", [96 / 72, 144 / 72])
def test_analysis_figure_height_does_not_follow_backend_initial_request(ui, monkeypatch, scaling):
    from matplotlib.backends import backend_tkagg

    _set_ui_scaling(ui, scaling)
    original = backend_tkagg.FigureCanvasTkAgg

    class InflatedCanvas(original):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.get_tk_widget().configure(height=1920)

    monkeypatch.setattr(backend_tkagg, "FigureCanvasTkAgg", InflatedCanvas)
    panel = ui.analysis_panel
    assert ui.open_analysis("WIN$N", "1 minuto")
    deadline = time.monotonic() + 10
    while panel.data is None and time.monotonic() < deadline:
        ui.pump(.05)
    assert panel.data is not None
    panel.redraw()
    # Também reproduz a página maior que o viewport retida pelo Notebook nativo.
    panel.tabs.configure(height=2048)
    for width in (1280, 900, 1920, 1280):
        ui.root.geometry(f"{width}x800")
        ui.pump(.15)
        widget = panel._canvases["chart"].get_tk_widget()
        assert widget.winfo_height() >= ui.theme.font.metrics("linespace") * 24
        assert widget.winfo_height() <= max(480, ui.theme.font.metrics("linespace") * 24,
                                            panel.results_viewport.canvas.winfo_height())


@pytest.mark.parametrize("scaling", [96 / 72, 144 / 72])
def test_analysis_keeps_native_text_and_chart_size_through_resize(ui, scaling):
    import pandas as pd
    from matplotlib.backend_bases import MouseEvent

    _set_ui_scaling(ui, scaling)
    panel = ui.analysis_panel
    assert ui.open_analysis("WIN$N", "1 minuto")
    deadline = time.monotonic() + 10
    while panel.data is None and time.monotonic() < deadline:
        ui.pump(.05)
    assert panel.data is not None
    original = panel.data.copy(deep=True)
    summary = dict(panel.result["summary"])
    for _ in range(2):
        for width in (1920, 1280, 900, 1920):
            ui.root.geometry(f"{width}x800")
            ui.pump(.15)
            for cell in panel.filter_cells:
                assert cell.winfo_rootx() >= panel.controls.winfo_rootx()
                assert cell.winfo_rootx() + cell.winfo_width() <= panel.controls.winfo_rootx() + panel.controls.winfo_width()
                for widget in cell.winfo_children():
                    assert widget.winfo_width() >= widget.winfo_reqwidth()
            for card in panel.metric_cards:
                for label in (card.title_label, card.value_label):
                    assert label.winfo_width() >= label.winfo_reqwidth()
            canvas = panel._canvases["chart"]
            assert canvas.get_tk_widget().winfo_height() >= ui.theme.font.metrics("linespace") * 24
            assert canvas.get_tk_widget().winfo_height() <= max(
                480, ui.theme.font.metrics("linespace") * 24, panel.results_viewport.canvas.winfo_height())
            price = canvas.figure.axes[0]
            x, y = price.transData.transform((10, sum(price.get_ylim()) / 2))
            canvas.callbacks.process("motion_notify_event", MouseEvent("motion_notify_event", canvas, x, y))
            ui.pump(.05)
            assert "Abertura" in panel.readout.cget("text")
            assert int(panel.readout.cget("wraplength")) <= panel.chart_tab.winfo_width()
            viewport = panel.results_viewport
            if width == 900:
                assert viewport.scrollbar.winfo_manager() == "grid"
                before = viewport.canvas.yview()[0]
                viewport.canvas.focus_force()
                viewport.canvas.event_generate("<Next>")
                ui.pump(.05)
                assert viewport.canvas.yview()[0] > before
            viewport.canvas.yview_moveto(0)
            assert panel.result["summary"] == summary
            pd.testing.assert_frame_equal(panel.data, original)
        ui.toggle_theme()


@pytest.mark.parametrize("scaling", [96 / 72, 144 / 72])
def test_header_preserves_connection_actions_and_whole_caption_when_it_fits(ui, scaling):
    from tkinter import ttk

    _set_ui_scaling(ui, scaling)
    # Exercita também a ação exclusiva do Windows sem abrir um terminal real.
    if len(ui.header_action_buttons) == 2:
        ui.header_action_buttons.insert(0, ttk.Button(ui._header_actions, text="Iniciar MT5 como administrador"))
    for text in ("MetaTrader 5 desconectado", "Conectado: conta 123 em Servidor de demonstração com identificação extensa"):
        ui.mt5_status_value.configure(text=text)
        for width in (1920, 1280, 900, 1920):
            ui.root.geometry(f"{width}x800")
            ui._schedule_header_layout()
            ui.pump(.12)
            widgets = [ui.brand_label, ui.mt5_status_value, *ui.header_action_buttons]
            for widget in widgets:
                assert widget.winfo_ismapped()
                assert widget.winfo_width() >= widget.winfo_reqwidth()
                assert widget.winfo_rootx() >= ui.main_header.winfo_rootx()
                assert widget.winfo_rootx() + widget.winfo_width() <= ui.main_header.winfo_rootx() + ui.main_header.winfo_width()
            if ui.caption_label.winfo_ismapped():
                assert ui.caption_label.winfo_width() >= ui.caption_label.winfo_reqwidth()
            # Nenhum par de controles essenciais ocupa o mesmo retângulo.
            for index, first in enumerate(widgets):
                for second in widgets[index + 1:]:
                    assert (first.winfo_rootx() + first.winfo_width() <= second.winfo_rootx() or
                            second.winfo_rootx() + second.winfo_width() <= first.winfo_rootx() or
                            first.winfo_rooty() + first.winfo_height() <= second.winfo_rooty() or
                            second.winfo_rooty() + second.winfo_height() <= first.winfo_rooty())


def test_analysis_cancels_scheduled_geometry_when_destroyed(ui):
    panel = ui.analysis_panel
    panel._schedule_layout()
    job = panel._layout_job
    assert job is not None
    panel.frame.destroy()
    assert panel._layout_job is None
    assert job not in ui.root.tk.call("after", "info")


def test_analysis_refits_cards_after_a_large_display_value(ui):
    _set_ui_scaling(ui, 2)
    panel = ui.analysis_panel
    ui.notebook.select(panel.frame)
    ui.root.geometry("1280x800")
    summary = {"last": 123456789012345.0, "return_pct": 0, "volatility_pct": 2,
               "max_drawdown_pct": -3, "mean_range_pct": .1, "up_bars_pct": 50}
    original = dict(summary)
    panel._show_summary(summary)
    ui.pump(.2)
    label = panel.tile_values["last"]
    assert label.winfo_width() >= label.winfo_reqwidth()
    assert label.winfo_rootx() + label.winfo_width() <= panel.results_viewport.canvas.winfo_rootx() + panel.results_viewport.canvas.winfo_width()
    assert summary == original


def _save_legacy_pf_check(db, key, pnls):
    from mt5_extracao.robots import store
    run = {"run_id": "legacy-pf", "robot_key": key, "source": "backtest", "symbol": "WIN$N",
           "timeframe": "M1", "data_start": "2024-01-02", "data_end": "2024-01-03", "bars": 600,
           "metrics": {"trades": len(pnls), "profit_factor": 99 if pnls else 0,
                       "max_drawdown_pct": 0.0, "win_rate": 1.0 if pnls else 0,
                       "return_pct": 0.0, "net_profit": sum(pnls), "expectancy_r": 0.0},
           "equity": [], "trades": [["2024-01-02T10:00:00", pnl, 1] for pnl in pnls],
           "created_at": "2026-01-01"}
    store.save_run(db, run)
    store.save_check(db, {"check_id": "legacy-check", "robot_key": key, "approved": True,
                         "allows_real": True, "static_status": "aprovada", "statistical_status": "aprovada",
                         "run_id": run["run_id"], "items": [], "created_at": "2026-01-02"})


@pytest.mark.parametrize("pnls", [[], [5.0, 2.0]])
def test_persisted_pf_guard_renders_rejection_without_rewriting_saved_approval(tmp_path, pnls):
    from sqlalchemy import text
    from mt5_extracao.robots import store
    from mt5_extracao.ui.robot_detail import battery_text
    db = DatabaseManager(db_path=str(tmp_path / "checks.db"))
    try:
        _save_legacy_pf_check(db, "mql5:painel", pnls)
        for _ in range(2):
            check = store.load_checks(db, "mql5:painel")[0]
            rendered = battery_text(types.SimpleNamespace(key="mql5:painel", checks=[check]))
            assert rendered.startswith("✗ Reprovada")
            assert "✗ PF definido na execução vinculada:" in rendered
            assert "Rode novamente a validação/bateria com uma amostra adequada." in rendered
            assert not check["approved"] and not check["allows_real"]
            assert len(check["items"]) == 1
        with db.engine.connect() as conn:
            assert conn.execute(text("SELECT approved,allows_real,items_json FROM _robot_checks")).one() == (1, 1, "[]")
    finally:
        db.close()


def test_robot_card_click_opens_persisted_pf_rejection_in_both_themes(ui, monkeypatch):
    from sqlalchemy import text
    errors = []
    monkeypatch.setattr(ui.root, "report_callback_exception", lambda kind, error, tb: errors.append((kind, str(error))))
    panel = ui.robots_panel
    panel.select("python:donchian")
    _save_legacy_pf_check(ui.app.db_manager, "mql5:demoguard", [])
    panel.reload()
    ui.notebook.select(panel.frame)
    ui.pump(.1)
    for _ in range(2):
        panel.cards["mql5:demoguard"].event_generate("<Button-1>", x=5, y=5)
        ui.pump(.1)
        assert errors == []
        assert panel.detail.robot.key == "mql5:demoguard"
        panel.detail.tabs.select(2)
        shown = panel.detail.battery_text.cget("text")
        assert shown.startswith("✗ Reprovada") and "✗ PF definido na execução vinculada:" in shown
        assert "Rode novamente" in shown
        panel.select("python:donchian")
        ui.toggle_theme()
    with ui.app.db_manager.engine.connect() as conn:
        assert conn.execute(text("SELECT approved,allows_real,items_json FROM _robot_checks")).one() == (1, 1, "[]")


def test_long_persisted_battery_reaches_final_reason_in_small_and_large_windows(ui):
    import json
    from sqlalchemy import text

    _save_legacy_pf_check(ui.app.db_manager, "mql5:demoguard", [])
    items = [{"group": "estática", "id": f"item-{index}", "name": f"Verificação {index}",
              "icon": "✓", "status": "ok", "detail": "Explicação longa de um resultado anterior. " * 5,
              "fix": ""} for index in range(25)]
    recorded = json.dumps(items, ensure_ascii=False)
    with ui.app.db_manager.engine.begin() as conn:
        conn.execute(text("UPDATE _robot_checks SET items_json = :items"), {"items": recorded})
    panel = ui.robots_panel
    panel.reload()
    ui.notebook.select(panel.frame)
    panel.select("mql5:demoguard")
    for geometry, dark in [("1280x800", False), ("1920x1080", True)]:
        ui.root.geometry(geometry)
        if ui.theme.dark != dark:
            ui.toggle_theme()
        panel.detail.tabs.select(2)
        ui.pump(.3)
        viewport = panel.detail.battery_viewport
        assert viewport.scrollbar.winfo_manager() == "grid"
        viewport.canvas.yview_moveto(0)
        viewport.canvas.focus_force()
        ui.pump(.1)
        viewport.canvas.event_generate("<Next>")
        ui.pump(.1)
        assert viewport.canvas.yview()[0] > 0
        viewport.canvas.yview_moveto(1.0)
        ui.pump(.1)
        assert viewport.canvas.yview()[0] > 0 and viewport.canvas.yview()[1] == 1.0
        label = panel.detail.battery_text
        assert int(label.cget("wraplength")) <= viewport.content.winfo_width()
        assert label.winfo_rootx() + label.winfo_width() <= (
            viewport.canvas.winfo_rootx() + viewport.canvas.winfo_width() + 2)
        assert label.winfo_rooty() + label.winfo_height() <= (
            viewport.canvas.winfo_rooty() + viewport.canvas.winfo_height() + 2)
        assert "✗ PF definido na execução vinculada:" in label.cget("text")
        assert "Rode novamente" in label.cget("text")
    with ui.app.db_manager.engine.connect() as conn:
        assert conn.execute(text("SELECT approved,allows_real,items_json FROM _robot_checks")).one() == (1, 1, recorded)


def test_batch_budget_is_shown_as_interrupted_and_keeps_checkpoint(ui):
    from mt5_extracao.robots import budget, store
    panel = ui.inventory_panel
    ui.app.config.set("ROBOTS", "batch_max_jobs", "1")
    chosen = [robot for robot in ui.robots_panel.robots if robot.key in ("python:ema_atr", "python:donchian")]
    batch = panel.run_backtests([dict(symbol="WIN$N", timeframe="1 minuto")], chosen)
    deadline = time.time() + 30
    while (batch.running or panel.progress_frame.winfo_manager()) and time.time() < deadline:
        ui.pump(.1)
    assert not batch.running
    assert "Backtests interrompidos" in ui.status_label.cget("text")
    assert "Orçamento" in ui.status_label.cget("text")
    assert len(store.load_runs(ui.app.db_manager)) == 1
    saved = budget.checkpoint(ui.app.db_manager, batch.batch_id)
    assert saved["status"] == "interrupted" and saved["owner"] == {}
    assert not panel.progress_frame.winfo_manager()


@pytest.mark.parametrize("result", [(1, 0, 0), (0, 1, 0), (0, 0, 1)])
def test_extraction_dialog_update_progress_cancel_and_result_are_coherent(ui, result):
    from tkinter import ttk
    calls = []
    class Extractor:
        def update_symbols(self, **kwargs):
            calls.append(kwargs)
            kwargs["update_progress_callback"](35, "Retomando bloco pendente")
        def cancel_extraction(self):
            calls.append("cancel")
    ui.app.historical_extractor = Extractor()
    ui.app.selected_symbols = ["WIN$N"]
    ui.extract_historical_data()
    dialog = next(w for w in ui.root.winfo_children()
                  if isinstance(w, tk.Toplevel) and w.title() == "Configurar Extração de Dados Históricos")
    def descendants(w):
        for child in w.winfo_children():
            yield child
            yield from descendants(child)
    widgets = list(descendants(dialog))
    buttons = {w.cget("text"): w for w in widgets if isinstance(w, ttk.Button)}
    buttons["Atualizar até agora"].invoke()
    kwargs = calls[0]
    assert kwargs["symbols"] == ["WIN$N"] and kwargs["timeframe_val"] == 1
    def texts():
        return [ui.root.getvar(w.cget("textvariable")) for w in widgets
                if isinstance(w, ttk.Label) and w.cget("textvariable")]
    assert "Retomando bloco pendente" in texts()
    assert str(buttons["Atualizar até agora"].cget("state")) == "disabled"
    if result[2]:
        buttons["Cancelar"].invoke()
        assert calls[-1] == "cancel" and "Cancelando..." in texts()
    kwargs["finished_callback"](*result)
    summary = next(str(t) for t in texts() if str(t).startswith("Concluído:"))
    assert f"{result[0]} ativo(s) com sucesso" in summary
    assert ("com falha" in summary) == bool(result[1])
    assert ("cancelado(s)" in summary) == bool(result[2])
    assert str(buttons["Atualizar até agora"].cget("state")) == "normal"
    dialog.destroy()
