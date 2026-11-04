"""Nomes das áreas e mensagens longas continuam acessíveis sem mudar modelos."""
import pytest
from tests.test_ui_main_window import ui, _set_ui_scaling


def enlarged(ui, request):
    previous = float(ui.root.tk.call("tk", "scaling"))
    request.addfinalizer(lambda: ui.root.tk.call("tk", "scaling", previous))
    _set_ui_scaling(ui, 2.0)
    ui.theme.set_scale(1.5)
    ui.root.geometry("900x620")
    ui.pump(.3)


def test_area_names_have_readable_selector_when_tabs_cannot_fit(ui, request):
    enlarged(ui, request)
    names = tuple(ui.notebook.tab(tab, "text") for tab in ui.notebook.tabs())
    navigation = getattr(ui, "area_navigation", None)
    assert navigation is not None, "abas comprimem nomes; falta caminho legível de seleção"
    assert navigation.combo.winfo_viewable()
    assert tuple(navigation.combo.cget("values")) == names
    from tkinter import ttk
    # O ttk representa o layout vazio pelo elemento "null" em algumas versões.
    assert "Notebook.label" not in repr(ttk.Style(ui.root).layout("AreaOnly.TNotebook.Tab"))
    navigation.combo.focus_force();ui.pump(.05)
    assert ui.root.focus_get() == navigation.combo
    before = list(ui.app.selected_symbols)
    navigation.combo.current(names.index("Robôs"))
    navigation.combo.event_generate("<<ComboboxSelected>>");ui.pump(.1)
    assert ui.notebook.select() == str(ui.robots_panel.frame)
    ui.notebook.select(ui.inventory_panel.frame);ui.pump(.1)
    assert navigation.combo.get() == "Minha base"
    assert ui.app.selected_symbols == before
    for _ in range(2):
        ui.root.geometry("1920x1000");ui.pump(.15)
        ui.root.geometry("900x620");ui.pump(.15)
    assert navigation.combo.get() == "Minha base"


def test_long_status_and_base_keep_full_details_inside_available_width(ui, request):
    enlarged(ui, request)
    message = "Sem conexão para atualizar esta seleção; os dados já extraídos continuam disponíveis. " * 8
    ui.update_status(message);ui.pump(.15)
    assert ui.status_label.winfo_reqwidth() <= ui.status_label.master.winfo_width()
    assert ui.base_label.winfo_reqwidth() <= ui.base_label.master.winfo_width()
    button = getattr(ui, "status_details_button", None)
    assert button is not None and button.winfo_viewable()
    button.focus_force();ui.pump(.05)
    assert ui.root.focus_get() == button
    button.event_generate("<KeyPress-space>")
    button.event_generate("<KeyRelease-space>");ui.pump(.1)
    windows = [w for w in ui.root.winfo_children() if w.winfo_class() == "Toplevel" and w.title() == "Status da aplicação"]
    assert len(windows) == 1
    def descendants(widget):
        for child in widget.winfo_children():
            yield child
            yield from descendants(child)
    text = next(w for w in descendants(windows[0]) if w.winfo_class() == "Text")
    scrollbar = next(w for w in descendants(windows[0]) if w.winfo_class() == "TScrollbar")
    assert message in text.get("1.0", "end-1c")
    assert str(text.cget("state")) == "disabled"
    assert str(text.cget("font")) == str(ui.theme.font)
    assert scrollbar.winfo_viewable()
    text.focus_force();text.event_generate("<Control-End>");ui.pump(.05)
    assert text.yview()[1] >= .999
    windows[0].destroy()


def test_navigation_handles_selection_before_first_idle_layout(ui):
    from tkinter import ttk
    from mt5_extracao.ui.components import AreaNavigation
    notebook = ttk.Notebook(ui.root)
    for name in ("Primeira", "Segunda"):
        notebook.add(ttk.Frame(notebook), text=name)
    navigation = AreaNavigation(ui.notebook.master, notebook, ui.theme)
    errors = []
    previous = ui.root.report_callback_exception
    ui.root.report_callback_exception = lambda kind, value, trace: errors.append(str(value))
    try:
        notebook.event_generate("<<NotebookTabChanged>>", when="now")
        assert navigation.combo.get() == "Primeira"
        assert not errors
    finally:
        ui.root.report_callback_exception = previous
        navigation.destroy();notebook.destroy()


@pytest.mark.parametrize("dark", [False, True])
@pytest.mark.parametrize("dpi", [96, 144])
def test_navigation_and_messages_survive_repeated_size_theme_and_font_changes(ui, request, dark, dpi):
    previous = float(ui.root.tk.call("tk", "scaling"))
    request.addfinalizer(lambda: ui.root.tk.call("tk", "scaling", previous))
    _set_ui_scaling(ui, dpi / 72)
    if ui.theme.dark != dark:
        ui.toggle_theme()
    selected = str(ui.inventory_panel.frame)
    ui.notebook.select(selected)
    message = "Dados indisponíveis nesta seleção; revise o contexto antes de tentar novamente. " * 4
    base = "Base: 12 ativos, 194.855 barras; referência persistida preservada."
    ui.update_status(message)
    ui.status_bar.set_base(base)
    for scale in (1.5, 1.0):
        ui.theme.set_scale(scale)
        for width, height in ((1920, 1000), (1280, 800), (900, 620), (820, 800), (1920, 1000)):
            ui.root.geometry(f"{width}x{height}");ui.pump(.12)
            assert ui.notebook.select() == selected
            assert ui.area_navigation.combo.get() == "Minha base"
            assert ui.status_bar.status_text == message and ui.status_bar.base_text == base
            assert ui.status_label.winfo_reqwidth() <= ui.status_label.master.winfo_width()
            assert ui.base_label.winfo_reqwidth() <= ui.base_label.master.winfo_width()
            assert ui.status_label.winfo_viewable() and ui.base_label.winfo_viewable()
    assert not ui.area_navigation.combo.winfo_viewable()  # todas as abas cabem no perfil final
    assert ui.notebook.cget("style") == ui.area_navigation.original_style
