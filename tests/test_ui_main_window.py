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
                                  "layout_hide_toolbars", "layout_own_profile"}
    assert all(var.get() for var in settings.vars.values())
    assert [settings.checks[key].cget("text") for key, *_ in layout.OPTION_KEYS] == [
        "Fechar a Observação do Mercado e o Navegador", "Fechar a Caixa de Ferramentas e o Testador de Estratégia",
        "Esconder as barras de ferramentas", "Abrir o gráfico do robô sozinho e maximizado"]
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


def test_settings_restore_the_arrangement_only_with_the_terminal_closed(ui, settings):
    from mt5_extracao.robots import layout
    layout.apply(settings.env.data)
    settings.refresh()
    ui.pump(0.5)
    assert "Há um arranjo anterior guardado" in settings.arrangement_label.cget("text")
    assert "barra de ferramentas" in settings.arrangement_label.cget("text")
    assert settings.restore_button.instate(["!disabled"])
    settings.env.running = True
    settings.restore_button.invoke()
    ui.pump(0.6)
    assert "está aberto" in settings.feedback.cget("text") and layout.has_arrangement(settings.env.data)
    settings.env.running = False
    settings.restore_button.invoke()
    ui.pump(0.6)
    assert "Arranjo anterior devolvido" in settings.feedback.cget("text")
    assert "continua ligada" in settings.feedback.cget("text")
    assert layout.ini_path(settings.env.data).read_bytes().decode("utf-16") == TERMINAL_INI
    assert settings.restore_button.instate(["disabled"])


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

    def open_view(config, close_terminal=False, spawn=None, symbol="", timeframe=""):
        calls.symbols.append(symbol)
        if calls.fail:
            raise calls.fail
        calls.opened.append(close_terminal)
        return {"reopened": close_terminal, "organized": True, "hidden": ["barra de ferramentas"], "restored": False,
                "profile": "", "chart": "", "panel_layout": "auto", "panel_charts": 0, "bridge": False}
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

    def open_view(config, close_terminal=False, spawn=None, symbol="", timeframe=""):
        env.opened.append(symbol)
        env.running = True
        return {"reopened": False, "organized": True, "hidden": [], "restored": False, "profile": "", "chart": "",
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


def test_no_internet_is_said_plainly_with_what_to_do(ui, connecting):
    connecting.ok, connecting.online = False, False
    ui.connect_mt5(startup=True)
    ui.pump(0.8)
    assert len(connecting.warned) == 1 and "sem internet" in connecting.warned[0]
    assert "Verificar conexão" in connecting.warned[0]
    assert ui.status_label.cget("text") == "Status: sem conexão com o MetaTrader 5 (sem internet)"
    assert ui.mt5_status_value.cget("text") == "MetaTrader 5 desconectado" and ui.app.mt5_initialized is False
    assert ui.check_button.instate(["!disabled"])

    connecting.online = True                                           # com internet: o terminal abriu e não entrou
    ui.check_mt5_status()                                              # o botão "Verificar conexão"
    ui.pump(0.8)
    assert "não entrou na conta" in connecting.warned[1]
    connecting.ok = True                                               # a rede voltou
    ui.check_mt5_status()
    ui.pump(0.8)
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
