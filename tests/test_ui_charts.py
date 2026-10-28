"""T17.3: legibilidade, amostra e rótulos dos gráficos do Laboratório."""
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
from matplotlib.figure import Figure

from mt5_extracao.ui import charts
from mt5_extracao.ui.visual_tokens import LIGHT, TYPOGRAPHY


def _chart():
    return SimpleNamespace(theme=SimpleNamespace(colors=dict(LIGHT)), figure=Figure(figsize=(6, 3)), hover=[])


def test_heatmap_keeps_explicit_sample_counts_hatching_and_readable_ticks():
    chart = _chart()
    ax = chart.figure.add_subplot(111)
    value = pd.DataFrame([[1.2, np.nan], [-0.4, 0.0]], index=("baixa", "alta"), columns=("09h", "10h"))
    count = pd.DataFrame([[12, 0], [44, 5]], index=value.index, columns=value.columns)
    small = pd.DataFrame([[True, True], [False, True]], index=value.index, columns=value.columns)

    charts.heatmap(chart, ax, value, count, small, "Expectativa", metric_name="Expectativa")

    assert [text.get_text() for text in ax.texts] == ["1,20", "—", "-0,40", "0,00"]
    assert all(text.get_rotation() == 0 for text in ax.texts)
    assert len(ax.patches) == 3
    assert all(text.get_fontsize() >= TYPOGRAPHY["chart_tick"] for text in ax.texts)
    assert all(label.get_fontsize() >= TYPOGRAPHY["chart_tick"] for label in ax.get_xticklabels())
    assert chart.figure.axes[1].get_ylabel() == "Expectativa (moeda da conta)"
    event = SimpleNamespace(inaxes=ax, xdata=0, ydata=0)
    assert "Expectativa" in chart.hover[0](event)
    assert "n=12" in chart.hover[0](event)
    assert "amostra pequena" in chart.hover[0](event)


def test_bars_show_value_and_count_and_explain_small_sample_on_hover():
    chart = _chart()
    ax = chart.figure.add_subplot(111)
    charts.bars(chart, ax, ("Seg", "Ter", "Qua"), (-1.25, 0.5, 0.2), n=(4, 8, 20),
                small=(True, False, False), title="Expectativa")

    annotations = [text.get_text() for text in ax.texts]
    assert annotations == ["-1,25", "0,50", "0,20"]
    assert all(text.get_rotation() == 0 for text in ax.texts)
    assert ax.patches[0].get_hatch() == "///"
    legend = ax.get_legend()
    assert legend.get_title().get_text().startswith("n total=32")
    assert "pouca amostra" in [text.get_text() for text in legend.get_texts()]
    assert ax.get_ylabel() == "Expectativa (moeda da conta)"
    event = SimpleNamespace(inaxes=ax, xdata=0)
    assert "n=4" in chart.hover[0](event)
    assert "amostra pequena" in chart.hover[0](event)


def test_correlation_heatmap_has_fixed_scale_and_no_fake_counts():
    chart = _chart()
    ax = chart.figure.add_subplot(111)
    corr = pd.DataFrame([[1.0, 0.2], [0.2, 1.0]], index=("a", "b"), columns=("a", "b"))
    counts = pd.DataFrame(0, index=corr.index, columns=corr.columns)
    missing = pd.DataFrame(False, index=corr.index, columns=corr.columns)

    charts.heatmap(chart, ax, corr, counts, missing, "Correlação", metric_name="Correlação", show_counts=False)

    assert ax.images[0].norm.vmin == -1 and ax.images[0].norm.vmax == 1
    assert not ax.texts
    assert chart.figure.axes[1].get_ylabel() == "Correlação"


def test_crowded_bars_move_samples_to_legend_and_tooltip():
    chart = _chart()
    ax = chart.figure.add_subplot(111)
    values = np.linspace(-1, 1, 15)
    counts = np.arange(15) + 1
    charts.bars(chart, ax, tuple(str(i) for i in range(15)), values, n=counts,
                small=np.zeros(15, dtype=bool), title="Dia do mês")

    assert not ax.texts  # sem n=… repetido nem valores verticais
    assert ax.get_legend().get_title().get_text().startswith("n total=120")
    event = SimpleNamespace(inaxes=ax, xdata=14)
    assert "14" in chart.hover[0](event)
    assert "n=15" in chart.hover[0](event)


def test_curve_and_scatter_legends_are_outside_the_data_axes():
    chart = _chart()
    ax = chart.figure.add_subplot(111)
    series = pd.Series((1.0, 2.0, 0.5), index=pd.date_range("2026-01-01", periods=3))
    charts.overlay(chart, ax, {"WIN$N": series}, "Resultado acumulado")
    legend_box = ax.get_legend().get_bbox_to_anchor().transformed(ax.transAxes.inverted())
    assert legend_box.x0 >= 1.0
    assert "moeda da conta" in ax.get_ylabel()

    chart = _chart()
    ax = chart.figure.add_subplot(111)
    charts.scatter_win_loss(chart, ax, (1, 2, 3), (2, -1, 3), "Volatilidade", "ATR %")
    legend_box = ax.get_legend().get_bbox_to_anchor().transformed(ax.transAxes.inverted())
    assert legend_box.x0 >= 1.0
    assert "moeda da conta" in ax.get_ylabel()


def test_bars_keep_missing_measurements_distinct_from_observed_zero():
    chart = _chart()
    ax = chart.figure.add_subplot(111)
    charts.bars(chart, ax, ["ausente", "NaN", "infinito", "zero", "perda"],
                [None, np.nan, np.inf, 0.0, -1.0], n=[0, 0, 0, 3, 4],
                small=[True, True, True, False, False])
    assert all(np.isnan(rect.get_height()) for rect in ax.patches[:3])
    assert ax.patches[3].get_height() == 0.0
    assert [text.get_text() for text in ax.texts] == ["0,00", "-1,00"]
    assert len(ax.collections[0].get_offsets()) == 3 and len(ax.collections[1].get_offsets()) == 1
    assert "sem dados" in [text.get_text() for text in ax.get_legend().get_texts()]
    missing = chart.hover[0](SimpleNamespace(inaxes=ax, xdata=0))
    assert "—" in missing and "n=0" in missing and "amostra pequena" not in missing
    assert "0,00" in chart.hover[0](SimpleNamespace(inaxes=ax, xdata=3))


@pytest.mark.parametrize("dpi", [100, 144])
@pytest.mark.parametrize("width,height", [(1280, 800), (1920, 1080)])
@pytest.mark.parametrize("dark", [False, True])
def test_native_dpi_mapping_and_resize_keep_complete_legend(dpi, width, height, dark):
    """DPI pode mudar no Map sem Configure Tk: o layout deve seguir a figura real."""
    tk = pytest.importorskip("tkinter")
    from mt5_extracao.ui.theme import Theme

    try:
        root = tk.Tk()
    except tk.TclError as error:
        pytest.skip(f"sem display gráfico: {error}")
    original_scaling = root.tk.call("tk", "scaling")
    root.withdraw()
    try:
        root.tk.call("tk", "scaling", dpi / 72)
        root.geometry(f"{width}x{height}")
        theme = Theme(root)
        if theme.dark != dark:
            theme.toggle()
        chart = charts.Chart(root, theme, height=3.5)
        chart.widget.pack(fill=tk.BOTH, expand=True)
        ax = chart.figure.add_subplot(111)
        dates = pd.date_range("2025-01-01", periods=3)
        curves = {name: pd.Series([0.0, -29953.75 + index * 12000, -4366.9 - index * 7000], index=dates)
                  for index, name in enumerate(["Trailing por ATR", "Ponto fixo", "Gradiente dinâmico por ATR"])}
        expected = {name: series.copy() for name, series in curves.items()}
        charts.overlay(chart, ax, curves, "Mesmas entradas, modos diferentes: resultado acumulado")
        chart.draw()
        root.deiconify()
        root.update()
        # Nenhum draw/layout corretivo no teste: a primeira renderização é a observada pelo usuário.
        for index, geometry in enumerate((f"{width}x{height}", "1000x650", f"{width}x{height}")):
            if index == 1:
                theme.toggle()
                chart.clear()
                ax = chart.figure.add_subplot(111)
                charts.overlay(chart, ax, curves, "Mesmas entradas, modos diferentes: resultado acumulado")
                chart.draw()
            root.geometry(geometry)
            root.update()
            renderer = chart.canvas.get_renderer()
            legend = ax.get_legend().get_window_extent(renderer)
            bounds = chart.figure.bbox
            assert bounds.contains(legend.x0, legend.y0)
            assert bounds.contains(legend.x1, legend.y1), (bounds.bounds, legend.bounds)
            assert legend.x0 >= ax.bbox.x1
            assert ax.bbox.width >= bounds.width * .35
            labels = [text.get_text() for text in ax.get_legend().get_texts()]
            assert len(labels) == len(curves)
            for line in ax.lines[:len(curves)]:
                name = next(name for name in curves if line.get_label().startswith(name + " ("))
                np.testing.assert_array_equal(line.get_ydata(), expected[name].values)
                assert charts.plain(f"{name} ({charts.money(expected[name].iloc[-1])})") in labels
            for name in curves:
                pd.testing.assert_series_equal(curves[name], expected[name])
    finally:
        # Tk mantém a escala no display mesmo quando a próxima janela usa outro Tk().
        root.tk.call("tk", "scaling", original_scaling)
        root.destroy()
        import gc
        gc.collect()


def test_bar_colors_follow_magnitude_with_neutral_zero():
    from matplotlib.colors import to_rgba
    chart = _chart()
    ax = chart.figure.add_subplot(111)
    charts.bars(chart, ax, list("abcde"), [4.0, 0.4, 0.0, -0.4, -4.0])
    strong, weak, zero, weak_loss, strong_loss = (patch.get_facecolor() for patch in ax.patches)
    surface = np.array(to_rgba(LIGHT["surface"]))
    distance = lambda rgba: np.abs(np.array(rgba) - surface).sum()
    assert distance(strong) > distance(weak) > 0 and distance(strong_loss) > distance(weak_loss) > 0
    assert zero == to_rgba(LIGHT["muted"])
    assert strong != strong_loss
