"""
Gráficos reutilizáveis do Laboratório (T16.11), em matplotlib dentro do Tk, com as cores do tema.

Regras visuais do plano (seção 5.3): paleta divergente acessível (azul = ganho, laranja = perda, nunca só verde e
vermelho), célula com amostra pequena hachurada e cinza, e o `n` de cada célula sempre à vista. Ao parar o mouse num
gráfico, `Chart` mostra a dica que a função de desenho registrou.
"""
import tkinter as tk
from typing import Callable, Dict, List, Optional

import numpy as np
import pandas as pd

from mt5_extracao.ui.visual_tokens import TYPOGRAPHY


_TICK = TYPOGRAPHY["chart_tick"]
_LABEL = TYPOGRAPHY["chart_label"]
_TITLE = TYPOGRAPHY["chart_title"]
_TOOLTIP = TYPOGRAPHY["chart_tooltip"]


class Chart:
    """Uma figura que é limpa e redesenhada (sem recriar o widget), com dicas ao parar o mouse."""

    def __init__(self, parent, theme, height=3.2):
        from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
        from matplotlib.figure import Figure
        self.theme = theme
        self.figure = Figure(figsize=(5.8, height), dpi=100, facecolor=theme.colors["surface"])
        self.canvas = FigureCanvasTkAgg(self.figure, master=parent)
        self.widget = self.canvas.get_tk_widget()
        self.widget.configure(background=theme.colors["surface"], highlightthickness=0)
        self.hover: List[Callable] = []          # cada desenho registra f(event) -> texto ou None
        self._tip = None
        self.canvas.mpl_connect("motion_notify_event", self._moved)

    def clear(self):
        self.figure.clear()
        self.figure.set_facecolor(self.theme.colors["surface"])
        self.widget.configure(background=self.theme.colors["surface"])
        self.hover = []
        self._tip = None

    def draw(self):
        self.figure.tight_layout(pad=1.4)
        self.canvas.draw_idle()

    def message(self, text):
        """Gráfico vazio com uma explicação (sem dados, amostra parcial...)."""
        self.clear()
        ax = self.figure.add_subplot(111)
        ax.axis("off")
        ax.text(0.5, 0.5, text, ha="center", va="center", wrap=True, color=self.theme.colors["ink2"], fontsize=_LABEL,
                transform=ax.transAxes)
        self.draw()

    def _moved(self, event):
        text = None
        for f in self.hover:
            text = f(event)
            if text:
                break
        if self._tip is None and text:
            self._tip = self.figure.text(0.01, 0.01, "", fontsize=_TOOLTIP, color=self.theme.colors["ink"],
                                         bbox={"facecolor": self.theme.colors["field"], "edgecolor":
                                               self.theme.colors["border"], "boxstyle": "round,pad=0.3"})
        if self._tip is not None:
            self._tip.set_text(text or "")
            self._tip.set_visible(bool(text))
            self.canvas.draw_idle()


def plain(text) -> str:
    """Texto literal no matplotlib: '$' de WIN$N não vira fórmula."""
    return str(text).replace("$", r"\$")


def style_axis(ax, theme, title=None, grid_axis="y"):
    c = theme.colors
    ax.set_facecolor(c["surface"])
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(c["axis"])
    ax.tick_params(colors=c["muted"], labelsize=_TICK, length=0)
    if grid_axis:
        ax.grid(True, axis=grid_axis, color=c["grid"], linewidth=0.6)
    ax.set_axisbelow(True)
    if title:
        ax.set_title(plain(title), color=c["ink2"], fontsize=_TITLE, loc="left", pad=8)


def diverging_cmap(theme):
    """Laranja (perda) → superfície → azul (ganho): legível para daltonismo e nos dois temas."""
    from matplotlib.colors import LinearSegmentedColormap
    c = theme.colors
    return LinearSegmentedColormap.from_list("lab", [c["series"][1], c["surface"], c["series"][0]])


def money(value) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return "—"
    text = f"{abs(value):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return ("-" if value < 0 else "") + text


def heatmap(chart: Chart, ax, value: pd.DataFrame, n: pd.DataFrame, small: pd.DataFrame, title: str,
            row_labels: Optional[Dict] = None, col_labels: Optional[Dict] = None, metric_name="Expectativa",
            show_counts=True):
    """Escala divergente centrada em zero, cor e trama para ausências e amostras pequenas, e legenda explícita."""
    from matplotlib.colors import TwoSlopeNorm
    from matplotlib.patches import Rectangle
    theme = chart.theme
    c = theme.colors
    style_axis(ax, theme, title, grid_axis=None)
    if value.empty:
        ax.text(0.5, 0.5, "Sem operações neste recorte.", ha="center", va="center", transform=ax.transAxes,
                color=c["ink2"], fontsize=_LABEL)
        ax.set_xticks([])
        ax.set_yticks([])
        return
    data = value.values.astype(float)
    span = 1.0 if metric_name == "Correlação" else np.nanmax(np.abs(data)) if np.isfinite(data).any() else 1.0
    span = span or 1.0
    cmap = diverging_cmap(theme).with_extremes(bad=c["border"])
    image = ax.imshow(np.ma.masked_invalid(data), cmap=cmap, norm=TwoSlopeNorm(0, -span, span), aspect="auto")
    colorbar = chart.figure.colorbar(image, ax=ax, fraction=0.046, pad=0.04)
    colorbar_label = f"{metric_name} (moeda da conta)" if metric_name == "Expectativa" else metric_name
    colorbar.set_label(plain(colorbar_label), color=c["muted"], fontsize=_LABEL, labelpad=8)
    colorbar.ax.tick_params(colors=c["muted"], labelsize=_TICK, length=0)
    colorbar.outline.set_edgecolor(c["axis"])
    for (i, j), v in np.ndenumerate(data):
        count = int(n.values[i, j]) if (i < n.shape[0] and j < n.shape[1]) else 0
        is_small = bool(small.values[i, j]) if (i < small.shape[0] and j < small.shape[1]) else True
        if not np.isfinite(v):
            ax.add_patch(Rectangle((j - 0.5, i - 0.5), 1, 1, facecolor="none", edgecolor=c["muted"], hatch="...",
                                   linewidth=0, alpha=0.55))
        elif is_small:          # trama por cima da cor: dá para ver a tendência e saber que é pouca amostra
            ax.add_patch(Rectangle((j - 0.5, i - 0.5), 1, 1, facecolor="none", edgecolor=c["muted"], hatch="///",
                                   linewidth=0, alpha=0.35))
        if show_counts:
            text = f"—\nn={count}" if not np.isfinite(v) else f"n={count}"
            ax.text(j, i, text, ha="center", va="center", fontsize=_TICK,
                    fontweight="bold", color=c["ink2"] if is_small else c["ink"])
    rows = [row_labels.get(r, r) if row_labels else r for r in value.index]
    cols = [col_labels.get(k, k) if col_labels else k for k in value.columns]
    ax.set_xticks(range(len(cols)))
    ax.set_xticklabels([plain(k) for k in cols], rotation=45 if len(cols) > 12 else 0, fontsize=_TICK)
    ax.set_yticks(range(len(rows)))
    ax.set_yticklabels([plain(r) for r in rows], fontsize=_TICK)
    ax.tick_params(axis="x", labeltop=True, labelbottom=False)

    def hover(event):
        if event.inaxes is not ax or event.xdata is None:
            return None
        j, i = int(round(event.xdata)), int(round(event.ydata))
        if not (0 <= i < data.shape[0] and 0 <= j < data.shape[1]):
            return None
        count = int(n.values[i, j])
        note = "  (amostra pequena: não conclua nada daqui)" if bool(small.values[i, j]) else ""
        amount = f" (n={count})" if show_counts else ""
        shown = money(data[i, j]) if metric_name != "Correlação" else f"{data[i, j]:.2f}".replace(".", ",")
        return f"{rows[i]} × {cols[j]}: {metric_name} {shown}{amount}{note if show_counts else ''}"
    chart.hover.append(hover)


def bars(chart: Chart, ax, labels, values, n=None, small=None, title="", fmt=money, signed=True):
    """Barras azul (≥ 0) e laranja (< 0); amostra identificada, valor e n no hover."""
    theme = chart.theme
    c = theme.colors
    style_axis(ax, theme, title)
    values = [float(v) if v is not None else 0.0 for v in values]
    colors = [c["series"][0] if (v >= 0 or not signed) else c["series"][1] for v in values]
    drawn = ax.bar(range(len(values)), values, color=colors, width=0.65, linewidth=0)
    for k, rect in enumerate(drawn):
        if small is not None and small[k]:
            rect.set_hatch("///")
            rect.set_alpha(0.45)
        if n is not None:
            count = int(n[k])
            crowded = len(values) > 14
            label = f"n={count}"
            if not crowded:
                label = f"{fmt(values[k])}\n{label}"
            end = rect.get_y() + rect.get_height()
            positive = values[k] >= 0
            ax.annotate(label, xy=(rect.get_x() + rect.get_width() / 2, end), xytext=(0, 4 if positive else -4),
                        textcoords="offset points", ha="center", va="bottom" if positive else "top",
                        rotation=90 if crowded else 0, fontsize=_TICK, color=c["ink2"] if small is not None and small[k]
                        else c["ink"], clip_on=False)
    if signed:
        ax.axhline(0, color=c["axis"], linewidth=0.8)
    ax.set_ylabel("Expectativa (moeda da conta)", color=c["muted"], fontsize=_LABEL)
    ax.margins(y=0.22)
    ax.set_xticks(range(len(labels)))
    crowded = len(labels) > 12
    ax.set_xticklabels([plain(k) for k in labels], fontsize=_TICK, rotation=45 if crowded else 0,
                       ha="right" if crowded else "center")

    def hover(event):
        if event.inaxes is not ax or event.xdata is None:
            return None
        k = int(round(event.xdata))
        if not 0 <= k < len(values):
            return None
        extra = f" (n={int(n[k])})" if n is not None else ""
        warn = "  (amostra pequena)" if small is not None and small[k] else ""
        return f"{labels[k]}: {fmt(values[k])}{extra}{warn}"
    chart.hover.append(hover)


def smooth_line(ax, x, y, color, break_before=None, linewidth=1.8, markersize=3.5):
    """Desenha uma linha PCHIP sem extrapolar nem atravessar valores ou faixas ausentes.

    Os marcadores continuam nos valores observados; a interpolação serve apenas para suavizar o traço entre eles.
    `break_before` permite separar períodos sem operações mesmo quando as categorias do eixo estão comprimidas.
    """
    from scipy.interpolate import PchipInterpolator

    x, y = np.asarray(x, dtype=float), np.asarray(y, dtype=float)
    if x.ndim != 1 or y.ndim != 1 or x.shape != y.shape:
        raise ValueError("Os eixos da linha precisam ter o mesmo formato unidimensional.")
    breaks = np.zeros(x.shape, dtype=bool) if break_before is None else np.asarray(break_before, dtype=bool)
    if breaks.shape != x.shape:
        raise ValueError("Os pontos de quebra precisam corresponder aos valores da linha.")
    valid = np.isfinite(x) & np.isfinite(y)
    groups = []
    current = []
    for index in range(len(x)):
        if not valid[index]:
            if current:
                groups.append(current)
                current = []
            continue
        if breaks[index] and current:
            groups.append(current)
            current = []
        current.append(index)
    if current:
        groups.append(current)

    curves, markers = [], []
    for indices in groups:
        xs, ys = x[indices], y[indices]
        if len(xs) > 1:
            if np.any(np.diff(xs) <= 0):
                raise ValueError("Os pontos da linha precisam estar em ordem crescente.")
            if len(xs) > 2:
                dense_x = np.linspace(xs[0], xs[-1], max(25, (len(xs) - 1) * 12 + 1))
                dense_y = PchipInterpolator(xs, ys)(dense_x)
            else:
                dense_x, dense_y = xs, ys
            curve, = ax.plot(dense_x, dense_y, color=color, linewidth=linewidth, solid_capstyle="round",
                             zorder=3)
            curves.append(curve)
        marker, = ax.plot(xs, ys, linestyle="None", marker="o", markersize=markersize, color=color, zorder=4)
        markers.append(marker)
    return curves, markers


def overlay(chart: Chart, ax, curves: Dict[str, pd.Series], title=""):
    """Curvas acumuladas com valores finais identificados fora da área de dados."""
    theme = chart.theme
    c = theme.colors
    style_axis(ax, theme, title, grid_axis="both")
    palette = list(c["series"]) + [c["ink2"], c["muted"], c["warn"], c["good"]]
    for k, (name, series) in enumerate(sorted(curves.items(), key=lambda kv: -kv[1].iloc[-1] if len(kv[1]) else 0)):
        if series.empty:
            continue
        ax.plot(series.index, series.values, color=palette[k % len(palette)], linewidth=1.4,
                label=plain(f"{name} ({money(series.iloc[-1])})"))
    ax.axhline(0, color=c["axis"], linewidth=0.8)
    if curves:
        legend = ax.legend(fontsize=_TICK, frameon=False, loc="upper left", bbox_to_anchor=(1.02, 1.0),
                           borderaxespad=0)
        for text in legend.get_texts():
            text.set_color(c["ink2"])
    ax.set_ylabel("Resultado acumulado (moeda da conta)", color=c["muted"], fontsize=_LABEL)
    ax.set_xlabel("Data de saída", color=c["muted"], fontsize=_LABEL)
    chart.figure.autofmt_xdate()


def scatter_win_loss(chart: Chart, ax, x, pnl, title="", xlabel="", x_fmt=None):
    """Ganhadoras (azul, círculo) e perdedoras (laranja, x) — forma e cor, para não depender só da cor."""
    theme = chart.theme
    c = theme.colors
    style_axis(ax, theme, title, grid_axis="both")
    x, pnl = np.asarray(x, dtype=float), np.asarray(pnl, dtype=float)
    ok = ~(np.isnan(x) | np.isnan(pnl))
    win = ok & (pnl > 0)
    loss = ok & (pnl <= 0)
    ax.scatter(x[win], pnl[win], s=10, color=c["series"][0], alpha=0.6, label=f"vencedoras ({win.sum()})")
    ax.scatter(x[loss], pnl[loss], s=12, color=c["series"][1], alpha=0.6, marker="x", label=f"perdedoras ({loss.sum()})")
    ax.axhline(0, color=c["axis"], linewidth=0.8)
    ax.set_xlabel(xlabel, color=c["muted"], fontsize=_LABEL)
    ax.set_ylabel("Resultado (moeda da conta)", color=c["muted"], fontsize=_LABEL)
    if x_fmt:
        from matplotlib.ticker import FuncFormatter
        ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _p: x_fmt(v)))
    legend = ax.legend(fontsize=_TICK, frameon=False, loc="upper left", bbox_to_anchor=(1.02, 1.0),
                       borderaxespad=0)
    for text in legend.get_texts():
        text.set_color(c["ink2"])
