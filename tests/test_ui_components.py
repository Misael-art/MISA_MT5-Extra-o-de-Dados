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
