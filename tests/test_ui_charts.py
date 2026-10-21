"""T17.3: legibilidade, amostra e rótulos dos gráficos do Laboratório."""
from types import SimpleNamespace

import numpy as np
import pandas as pd
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

    assert [text.get_text() for text in ax.texts] == ["n=12", "—\nn=0", "n=44", "n=5"]
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
    assert annotations == ["-1,25\nn=4", "0,50\nn=8", "0,20\nn=20"]
    assert ax.patches[0].get_hatch() == "///"
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


def test_crowded_bars_keep_compact_sample_labels_and_tooltip_values():
    chart = _chart()
    ax = chart.figure.add_subplot(111)
    values = np.linspace(-1, 1, 15)
    counts = np.arange(15) + 1
    charts.bars(chart, ax, tuple(str(i) for i in range(15)), values, n=counts,
                small=np.zeros(15, dtype=bool), title="Dia do mês")

    annotations = [text.get_text() for text in ax.texts]
    assert annotations[0] == "n=1" and annotations[-1] == "n=15"
    assert all(text.get_rotation() == 90 for text in ax.texts)
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


def test_smoothed_line_preserves_observed_points_and_stays_within_their_range():
    chart = _chart()
    ax = chart.figure.add_subplot(111)
    observed_x = np.arange(5, dtype=float)
    observed_y = np.asarray((10.0, 35.0, 45.0, 82.0, 90.0))

    curves, markers = charts.smooth_line(ax, observed_x, observed_y, color=LIGHT["ink2"])

    assert len(curves) == len(markers) == 1
    assert len(curves[0].get_xdata()) > len(observed_x)
    assert list(markers[0].get_xdata()) == list(observed_x)
    assert list(markers[0].get_ydata()) == list(observed_y)
    assert min(curves[0].get_ydata()) >= observed_y.min()
    assert max(curves[0].get_ydata()) <= observed_y.max()
    assert curves[0].get_xdata()[0] == observed_x[0]
    assert curves[0].get_xdata()[-1] == observed_x[-1]


def test_smoothed_line_keeps_gaps_between_missing_or_unobserved_buckets():
    chart = _chart()
    ax = chart.figure.add_subplot(111)
    x = np.arange(7, dtype=float)
    y = np.asarray((10.0, 20.0, 30.0, np.nan, 60.0, 70.0, 80.0))
    breaks = np.asarray((False, False, False, False, False, True, False))

    curves, markers = charts.smooth_line(ax, x, y, color=LIGHT["ink2"], break_before=breaks)

    assert len(curves) == 2
    assert len(markers) == 3
    assert all(curve.get_xdata()[-1] <= 2 for curve in curves[:1])
    assert all(curve.get_xdata()[0] >= 4 for curve in curves[1:])
