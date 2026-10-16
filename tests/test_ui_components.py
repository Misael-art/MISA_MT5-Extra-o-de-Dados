"""T17.2: contratos dos componentes reutilizáveis de apresentação."""
import pytest

tk = pytest.importorskip("tkinter")
from tkinter import ttk

from mt5_extracao.ui.components import (
    ChartPanel,
    ComparisonSummary,
    ContentPanel,
    DetailsPanel,
    FilterBar,
    MetricCard,
    Notice,
    PageHeader,
    ProgressPanel,
    StatePanel,
    StatusBadge,
    TablePanel,
)
from mt5_extracao.ui.theme import DARK, LIGHT, Theme


@pytest.fixture
def app():
    try:
        root = tk.Tk()
    except tk.TclError as exc:
        pytest.skip(f"sem display gráfico: {exc}")
    theme = Theme(root)
    yield root, theme
    root.destroy()


def test_header_filter_and_metric_expose_real_controls(app):
    root, theme = app
    calls = []
    header = PageHeader(root, "Laboratório", "operações por robô")
    header.pack(fill=tk.X)
    button = header.add_action("Atualizar", lambda: calls.append("refresh"), style="Accent.TButton")
    assert button.cget("takefocus") in ("1", "ttk::takefocus")
    assert any("focus" in states for states, _color in ttk.Style(root).map("TButton", "bordercolor"))
    button.invoke()
    assert calls == ["refresh"]
    assert header.title_label.cget("text") == "Laboratório"
    assert header.subtitle_label.cget("text") == "operações por robô"
    with pytest.raises(ValueError, match="rótulo e callback"):
        header.add_action("Ação sem callback", None)

    filters = FilterBar(root)
    filters.pack(fill=tk.X)
    combo = ttk.Combobox(filters, values=("Tudo", "Último ano"), state="readonly")
    combo.pack(side=tk.LEFT)
    assert combo.master is filters
    assert ttk.Style(root).lookup("Card.TFrame", "background") == LIGHT["surface"]

    metric = MetricCard(root, "Operações", 12, "em 2 runs", hint="Total do recorte", theme=theme)
    metric.pack(fill=tk.X)
    metric.set_value("18", "em 3 runs", style="Good.TLabel")
    assert metric.value_label.cget("text") == "18"
    assert metric.detail_label.cget("text") == "em 3 runs"
    assert metric.value_label.cget("style") == "Good.TLabel"
    metric.set_value("20", "")
    assert metric.detail_label.winfo_manager() == ""


@pytest.mark.parametrize("kind", ["success", "warning", "error", "info", "neutral", "partial", "loading"])
def test_status_badges_keep_icon_text_and_theme_colors(app, kind):
    root, theme = app
    badge = StatusBadge(root, "Dados parciais", kind=kind, theme=theme)
    badge.pack()
    assert "Dados parciais" in badge.cget("text")
    assert badge.cget("text") != "Dados parciais"
    style = ttk.Style(root)
    assert style.lookup(badge.cget("style"), "foreground")
    theme.toggle()
    assert theme.colors == DARK
    assert style.lookup(badge.cget("style"), "foreground")
    badge.set_status("Completo", kind="success")
    assert "Completo" in badge.cget("text")
    with pytest.raises(ValueError, match="Estado desconhecido"):
        badge.set_status(kind="unknown")


def test_notices_and_states_require_callbacks_for_visible_actions(app):
    root, theme = app
    calls = []
    notice = Notice(root, "Há resultados incompletos.", kind="partial", action_text="Revisar",
                    command=lambda: calls.append("review"), theme=theme)
    notice.pack(fill=tk.X)
    assert notice.badge.cget("style") == "Warning.Status.TLabel"
    assert "Parcial" in notice.badge.cget("text")
    assert "incompletos" in notice.message.cget("text")
    notice.action_button.invoke()
    assert calls == ["review"]
    with pytest.raises(ValueError, match="rótulo e callback"):
        Notice(root, "Ação sem callback", action_text="Executar")

    panel = StatePanel(root, "Base indisponível", "Conecte a base para carregar as operações.",
                       kind="unavailable", theme=theme)
    panel.pack(fill=tk.X)
    assert panel.badge.kind == "unavailable"
    assert "Indisponível" in panel.badge.cget("text")
    assert panel.action_button is None
    with pytest.raises(ValueError, match="Estado de painel"):
        StatePanel(root, "Outro", "Mensagem", kind="working")


def test_chart_table_and_content_panels_expose_containers(app):
    root, theme = app
    content = ContentPanel(root, "Resumo", "Dados da seleção")
    content.pack(fill=tk.BOTH, expand=True)
    assert content.body.winfo_manager() == "pack"

    chart = ChartPanel(root, "Expectativa", "Por faixa de volatilidade")
    chart.pack(fill=tk.BOTH, expand=True)
    chart.set_notice("Amostra parcial", kind="partial", theme=theme)
    assert chart.notice is not None
    assert chart.notice.winfo_manager() == "pack"
    chart.set_notice(None)
    assert chart.notice is None

    table = TablePanel(root, "Comparação", ("Ativo", "Operações"), (140, 90),
                       anchors=("w", "e"), height=4, xscroll=True)
    table.pack(fill=tk.BOTH, expand=True)
    table.tree.insert("", tk.END, values=("WIN$N", "12"))
    assert table.tree.item(table.tree.get_children()[0], "values") == ("WIN$N", "12")
    assert table.tree.cget("columns") == ("Ativo", "Operações")


def test_progress_comparison_and_details_are_presentation_only(app):
    root, _theme = app
    calls = []
    progress = ProgressPanel(root, "Backtests", value=25, maximum=50, detail="1 de 2",
                             cancel_text="Cancelar", on_cancel=lambda: calls.append("cancel"))
    progress.pack(fill=tk.X)
    progress.set_progress(50, "2 de 2")
    progress.cancel_button.invoke()
    assert float(progress.progress.cget("value")) == 50
    assert progress.detail_label.cget("text") == "2 de 2"
    assert calls == ["cancel"]
    with pytest.raises(ValueError, match="maior que zero"):
        ProgressPanel(root, "Inválido", value=0, maximum=0)

    summary = ComparisonSummary(root, "Por ativo", [("WIN$N", "12 ops", "amostra suficiente"),
                                                       ("WDO$N", "6 ops")], columns=2)
    summary.pack(fill=tk.X)
    visible = " ".join(label.cget("text") for cell in summary.body.winfo_children()
                       for label in cell.winfo_children())
    assert "WIN$N" in visible and "12 ops" in visible and "WDO$N" in visible
    summary.set_items([("EURUSD", "4 ops")])
    assert len(summary.body.winfo_children()) == 1
    with pytest.raises(ValueError, match="Cada item"):
        summary.set_items([("inválido",)])
    assert len(summary.body.winfo_children()) == 1

    details = DetailsPanel(root, "Detalhes", expanded=False)
    details.pack(fill=tk.X)
    ttk.Label(details.body, text="Identificador do run").pack()
    assert details.body.winfo_manager() == ""
    details.toggle_button.invoke()
    assert details.body.winfo_manager() == "pack"
    assert details.expanded is True
    assert "Identificador" in details.body.winfo_children()[0].cget("text")
    details.toggle_button.invoke()
    assert details.body.winfo_manager() == ""


def test_information_details_support_keyboard_and_keep_hidden_content(app):
    import time

    root, theme = app
    states = []
    details = DetailsPanel(root, "Comprovação da seleção", expanded=False,
                           information=True, on_toggle=states.append, theme=theme)
    details.pack(fill=tk.X)
    text = tk.Text(details.body, height=3)
    text.insert("1.0", "Aquecimento · n=0 · motivo completo preservado")
    text.pack(fill=tk.X)
    root.geometry("640x360")
    root.update()
    assert details.toggle_button.cget("text") == "Detalhes"
    icon = details.information_icon
    assert str(icon) in details.toggle_button.cget("image")
    assert icon.width() >= 14
    assert sum(not icon.transparency_get(x, y) for x in range(icon.width()) for y in range(icon.height())) > 20
    assert details.body.winfo_manager() == ""
    details.toggle_button.focus_force()
    details.toggle_button.event_generate("<Return>")
    root.update()
    assert details.expanded and details.body.winfo_ismapped()
    details.toggle_button.event_generate("<KeyPress-space>")
    details.toggle_button.event_generate("<KeyRelease-space>")
    deadline = time.monotonic() + 2
    while details.expanded and time.monotonic() < deadline:
        root.update()
        time.sleep(.01)
    assert not details.expanded
    details.toggle_button.invoke()
    text.focus_force()
    root.update()
    details.set_expanded(False)
    root.update()
    assert root.focus_get() is details.toggle_button
    assert text.get("1.0", "end-1c") == "Aquecimento · n=0 · motivo completo preservado"
    details.toggle_button.invoke()
    details.toggle_button.event_generate("<Escape>")
    root.update()
    assert not details.expanded
    assert states == [True, False, True, False, True, False]
    theme.rich_glyphs = False
    theme.toggle()
    assert str(details.information_icon) != str(icon)
    assert str(details.information_icon) in details.toggle_button.cget("image")
    assert text.get("1.0", "end-1c") == "Aquecimento · n=0 · motivo completo preservado"


@pytest.mark.parametrize("palette", [LIGHT, DARK])
def test_semantic_component_styles_have_readable_foreground(app, palette):
    root, theme = app
    theme.dark = palette is DARK
    theme.colors = palette
    theme.apply()
    style = ttk.Style(root)
    for name, token in (("Success", "good"), ("Warning", "warn"), ("Error", "bad"),
                        ("Info", "accent"), ("Neutral", "ink2")):
        assert style.lookup(f"{name}.Status.TLabel", "foreground") == palette[token]
        assert style.lookup(f"{name}.Status.TLabel", "background") == palette["surface"]


def test_scrollable_content_preserves_height_and_keyboard_access(app):
    from mt5_extracao.ui.components import ScrollableContent
    root, theme = app
    root.geometry("500x240")
    viewport = ScrollableContent(root, theme)
    viewport.pack(fill=tk.BOTH, expand=True)
    for index in range(30):
        ttk.Label(viewport.content, text=f"Linha {index}").pack()
    root.update()
    assert viewport.scrollbar.winfo_manager() == "grid"
    assert viewport.content.winfo_height() > viewport.canvas.winfo_height()
    root.focus_force()
    viewport.canvas.focus_set()
    initial = viewport.canvas.yview()[0]
    viewport.canvas.event_generate("<Next>")
    root.update()
    assert viewport.canvas.yview()[0] > 0
    viewport.canvas.event_generate("<Prior>")
    root.update()
    assert viewport.canvas.yview()[0] == initial
    theme.toggle()
    assert viewport.canvas.cget("background") == theme.colors["surface"]


def test_matrix_cells_preserve_values_and_one_keyboard_entry(app):
    from mt5_extracao.ui.components import MatrixTable
    root, theme = app
    root.geometry("300x200")
    matrix = MatrixTable(root, ("Hora", "Seg", "Ter"), (80, 160, 160), theme, height=3)
    matrix.pack(fill=tk.BOTH, expand=True)
    rows = [[("+1,00 · n=2*", theme.colors["surface"], theme.colors["ink"])] * 2 for _ in range(20)]
    matrix.set_rows([str(i) for i in range(20)], rows)
    root.update()
    assert sum(bool(int(cell.cget("takefocus"))) for row in matrix.cells for cell in row) == 1
    header_y = matrix.headers[0].winfo_rooty()
    root.focus_force();matrix.cells[0][0].focus_set();root.update()
    matrix.cells[0][0].event_generate("<End>");root.update()
    assert root.focus_get() is matrix.cells[-1][0]
    assert matrix.canvas.yview()[0] > 0
    assert matrix.headers[0].winfo_rooty() == header_y
    assert sum(bool(int(cell.cget("takefocus"))) for row in matrix.cells for cell in row) == 1
    assert matrix.cells[-1][0].cget("text") == "+1,00 · n=2*"
    matrix.set_rows(["única"], rows[:1]);root.update()
    assert len(matrix.cells) == 1
    assert matrix.content.winfo_reqheight() < 200


def test_scrollable_content_fills_available_height_and_restores_scroll(app):
    from mt5_extracao.ui.components import ScrollableContent
    root, theme = app
    viewport = ScrollableContent(root, theme)
    viewport.pack(fill=tk.BOTH, expand=True)
    content = ttk.Frame(viewport.content, height=280)
    content.pack(fill=tk.BOTH, expand=True)
    for _ in range(2):
        for height in (600, 280, 160, 600):
            root.geometry(f"500x{height}")
            root.update()
            expected = max(viewport.content.winfo_reqheight(), viewport.canvas.winfo_height() - 2)
            assert viewport.content.winfo_height() == expected
            assert viewport.scrollbar.winfo_ismapped() == (height <= 280)
            if height <= 280:
                root.focus_force();viewport.canvas.focus_set()
                viewport.canvas.event_generate("<Next>");root.update()
                assert viewport.canvas.yview()[0] > 0
            else:
                assert viewport.canvas.yview() == (0.0, 1.0)
                assert content.winfo_height() >= 580
        theme.toggle()


def test_scrollable_content_tracks_wrapped_height_after_width_changes(app):
    from mt5_extracao.ui.components import ScrollableContent
    root, theme = app
    viewport = ScrollableContent(root, theme)
    viewport.pack(fill=tk.BOTH, expand=True)
    label = ttk.Label(viewport.content, text="Texto de análise com várias palavras. " * 40)
    label.pack(fill=tk.X)
    viewport.content.bind("<Configure>", lambda event: label.configure(wraplength=max(80, event.width - 8)), add="+")
    for width in (800, 300, 800, 300):
        root.geometry(f"{width}x240");root.update()
        assert viewport.content.winfo_height() == max(viewport.content.winfo_reqheight(), 238)
    viewport._content_changed()
    assert viewport._geometry_sync is not None
    viewport.destroy()
    assert viewport._geometry_sync is None
    root.update()
