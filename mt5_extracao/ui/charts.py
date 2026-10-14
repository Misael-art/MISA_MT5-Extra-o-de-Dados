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


class Chart:
    """Uma figura que é limpa e redesenhada (sem recriar o widget), com dicas ao parar o mouse."""

    def __init__(self, parent, theme, height=3.2):
        from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
        from matplotlib.figure import Figure
        self.theme = theme
        self.figure = Figure(figsize=(6, height), dpi=100, facecolor=theme.colors["surface"])
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
        self.canvas.draw_idle()

    def message(self, text):
        """Gráfico vazio com uma explicação (sem dados, amostra parcial...)."""
        self.clear()
        ax = self.figure.add_subplot(111)
        ax.axis("off")
        ax.text(0.5, 0.5, text, ha="center", va="center", wrap=True, color=self.theme.colors["ink2"], fontsize=9,
                transform=ax.transAxes)
        self.draw()

    def _moved(self, event):
        text = None
        for f in self.hover:
            text = f(event)
            if text:
                break
        if self._tip is None and text:
            self._tip = self.figure.text(0.01, 0.01, "", fontsize=8, color=self.theme.colors["ink"],
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
    ax.tick_params(colors=c["muted"], labelsize=8, length=0)
    if grid_axis:
        ax.grid(True, axis=grid_axis, color=c["grid"], linewidth=0.6)
    ax.set_axisbelow(True)
    if title:
        ax.set_title(plain(title), color=c["ink2"], fontsize=9, loc="left", pad=4)


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
            row_labels: Optional[Dict] = None, col_labels: Optional[Dict] = None, metric_name="Expectativa"):
    """Valor por célula na paleta divergente centrada em zero; célula pequena hachurada; n escrito em cada célula."""
    from matplotlib.colors import TwoSlopeNorm
    from matplotlib.patches import Rectangle
    theme = chart.theme
    c = theme.colors
    style_axis(ax, theme, title, grid_axis=None)
    if value.empty:
        ax.text(0.5, 0.5, "Sem operações neste recorte.", ha="center", va="center", transform=ax.transAxes,
                color=c["ink2"], fontsize=9)
        ax.set_xticks([])
        ax.set_yticks([])
        return
    data = value.values.astype(float)
    span = np.nanmax(np.abs(data)) if np.isfinite(data).any() else 1.0
    span = span or 1.0
    ax.imshow(np.ma.masked_invalid(data), cmap=diverging_cmap(theme), norm=TwoSlopeNorm(0, -span, span),
              aspect="auto")
    for (i, j), v in np.ndenumerate(data):
        count = int(n.values[i, j]) if (i < n.shape[0] and j < n.shape[1]) else 0
        is_small = bool(small.values[i, j]) if (i < small.shape[0] and j < small.shape[1]) else True
        if is_small:            # trama por cima da cor: dá para ver a tendência e saber que é pouca amostra
            ax.add_patch(Rectangle((j - 0.5, i - 0.5), 1, 1, facecolor="none", edgecolor=c["muted"], hatch="///",
                                   linewidth=0, alpha=0.35))
        if count:
            ax.text(j, i, str(count), ha="center", va="center", fontsize=7,
                    color=c["muted"] if is_small else c["ink"])
    rows = [row_labels.get(r, r) if row_labels else r for r in value.index]
    cols = [col_labels.get(k, k) if col_labels else k for k in value.columns]
    ax.set_xticks(range(len(cols)))
    ax.set_xticklabels([plain(k) for k in cols], rotation=45 if len(cols) > 12 else 0, fontsize=7 if len(cols) > 12 else 8)
    ax.set_yticks(range(len(rows)))
    ax.set_yticklabels([plain(r) for r in rows], fontsize=6 if len(rows) > 16 else 8)
    ax.tick_params(axis="x", labeltop=True, labelbottom=False)

    def hover(event):
        if event.inaxes is not ax or event.xdata is None:
            return None
        j, i = int(round(event.xdata)), int(round(event.ydata))
        if not (0 <= i < data.shape[0] and 0 <= j < data.shape[1]):
            return None
        count = int(n.values[i, j])
        note = "  (amostra pequena: não conclua nada daqui)" if bool(small.values[i, j]) else ""
        amount = f" em {count} operação(ões)" if count else ""
        shown = money(data[i, j]) if metric_name != "Correlação" else f"{data[i, j]:.2f}".replace(".", ",")
        return f"{rows[i]} × {cols[j]}: {metric_name} {shown}{amount}{note if count else ''}"
    chart.hover.append(hover)


def bars(chart: Chart, ax, labels, values, n=None, small=None, title="", fmt=money, signed=True):
    """Barras azul (≥ 0) e laranja (< 0); pequenas hachuradas; valor e n acima de cada barra."""
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
    if signed:
        ax.axhline(0, color=c["axis"], linewidth=0.8)
    ax.set_xticks(range(len(labels)))
    crowded = len(labels) > 12
    ax.set_xticklabels([plain(k) for k in labels], fontsize=7 if crowded else 8, rotation=45 if crowded else 0,
                       ha="right" if crowded else "center")

    def hover(event):
        if event.inaxes is not ax or event.xdata is None:
            return None
        k = int(round(event.xdata))
        if not 0 <= k < len(values):
            return None
        extra = f" em {int(n[k])} operação(ões)" if n is not None else ""
        warn = "  (amostra pequena)" if small is not None and small[k] else ""
        return f"{labels[k]}: {fmt(values[k])}{extra}{warn}"
    chart.hover.append(hover)


def overlay(chart: Chart, ax, curves: Dict[str, pd.Series], title=""):
    """Curvas de resultado acumulado sobrepostas, com o valor final na legenda."""
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
        legend = ax.legend(fontsize=7, frameon=False, loc="upper left")
        for text in legend.get_texts():
            text.set_color(c["ink2"])
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
    ax.set_xlabel(xlabel, color=c["muted"], fontsize=8)
    if x_fmt:
        from matplotlib.ticker import FuncFormatter
        ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _p: x_fmt(v)))
    legend = ax.legend(fontsize=7, frameon=False, loc="upper right")
    for text in legend.get_texts():
        text.set_color(c["ink2"])
