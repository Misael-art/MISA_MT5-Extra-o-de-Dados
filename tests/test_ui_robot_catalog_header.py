"""Cabeçalho real do catálogo, sem executar importação ou backtest."""
import pytest
import time
from tests.test_ui_main_window import ui, _set_ui_scaling


def enlarge(ui, request):
    previous = float(ui.root.tk.call("tk", "scaling"))
    request.addfinalizer(lambda: ui.root.tk.call("tk", "scaling", previous))
    _set_ui_scaling(ui, 2.0)
    ui.theme.set_scale(1.5)
    ui.root.geometry("900x620")
    ui.notebook.select(ui.robots_panel.frame)
    ui.pump(.3)


@pytest.mark.parametrize("dark", [False, True])
@pytest.mark.parametrize("dpi", [96, 144])
def test_catalog_summary_and_reload_error_fit_actual_page_width(ui, request, dark, dpi):
    previous = float(ui.root.tk.call("tk", "scaling"))
    request.addfinalizer(lambda: ui.root.tk.call("tk", "scaling", previous))
    _set_ui_scaling(ui, dpi / 72)
    if ui.theme.dark != dark:
        ui.toggle_theme()
    panel = ui.robots_panel
    ui.notebook.select(panel.frame)
    end = time.monotonic() + 10
    while not panel.robots:
        ui.pump(.1)
        assert time.monotonic() < end, "catálogo não carregou"
    panel.select(panel.robots[0].key)
    selected = panel.selected_key
    text = panel.summary.cget("text")
    page = panel.page_viewport.canvas
    for scale in (1.5, 1.0):
        ui.theme.set_scale(scale)
        for width, height in ((1920, 1000), (1280, 800), (900, 620), (820, 800), (1920, 1000)):
            ui.root.geometry(f"{width}x{height}");ui.pump(.12)
            assert panel.summary.winfo_reqwidth() <= page.winfo_width()
            assert panel.summary.cget("text") == text and panel.selected_key == selected
            assert str(panel.detail.robot.key) == selected
    ui.theme.set_scale(1.5);ui.root.geometry("900x620");ui.pump(.15)
    message = "Falha controlada de leitura: mantenha o catálogo atual e confira a pasta informada. " * 5
    panel._loaded(None, message);ui.pump(.15)
    assert panel.summary.cget("text") == "Não foi possível ler o catálogo de robôs: " + message
    assert panel.summary.winfo_reqwidth() <= page.winfo_width()
    assert panel.summary.winfo_reqheight() > ui.theme.font_small.metrics("linespace")
    assert panel.selected_key == selected
    panel.render();ui.pump(.1)
    assert panel.summary.cget("text") == text


def test_search_keeps_room_for_typing_between_header_actions(ui, request):
    enlarge(ui, request)
    panel = ui.robots_panel
    search = next(w for w in panel.filterbar_search.winfo_children() if w.winfo_class() == "TEntry")
    assert search.winfo_width() >= ui.theme.font.measure("0" * 12), (search.winfo_width(), ui.theme.font.measure("0" * 12))
    for button in (panel.import_button, panel.panel_button):
        assert button.winfo_reqwidth() <= button.winfo_width()
        assert button.winfo_rootx() >= panel.page_viewport.canvas.winfo_rootx()
        assert button.winfo_rootx() + button.winfo_width() <= panel.page_viewport.canvas.winfo_rootx() + panel.page_viewport.canvas.winfo_width()
