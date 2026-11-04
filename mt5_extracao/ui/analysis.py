"""Aba "Análise": gráfico de candles com indicadores, números do período, sazonalidade e qualidade dos dados."""
import logging
import tkinter as tk
from tkinter import ttk

import numpy as np
import pandas as pd

from mt5_extracao import data_quality, study, timeframes
from mt5_extracao.ui.visual_tokens import result_attention_colors, result_attention_role
from mt5_extracao.ui.components import FilterBar, MetricCard, ScrollableContent
from mt5_extracao.ui.theme import Tooltip, fmt_int, fmt_num

log = logging.getLogger(__name__)

# (rótulo, chave no resumo, texto do glossário)
TILES = (("Último preço", "last", "Último preço"),
         ("Retorno no período", "return_pct", "Retorno no período"),
         ("Volatilidade anual.", "volatility_pct", "Volatilidade anualizada"),
         ("Queda máxima", "max_drawdown_pct", "Queda máxima"),
         ("Amplitude por barra", "mean_range_pct", "Amplitude média"),
         ("Barras de alta", "up_bars_pct", "Barras de alta"))


class AnalysisPanel:
    def __init__(self, parent, ui):
        self.ui = ui
        self.theme = ui.theme
        self.data = None       # barras do período, com indicadores
        self.candles = None    # barras agrupadas para o gráfico
        self.factor = 1
        self._request = 0      # descarta respostas de cargas antigas
        self._canvases = {}
        self._figure_frames = {}
        self.available = {}    # símbolo -> timeframes (rótulos) com barras na base
        self.frame = ttk.Frame(parent, padding=12)
        self._layout_job = None
        self.frame.bind("<Destroy>", self._destroyed, add="+")

        controls = FilterBar(self.frame, padding=10)
        self.controls = controls
        controls.pack(fill=tk.X)
        data_filters = ttk.Frame(controls, style="Surface.TFrame")
        period_filters = ttk.Frame(controls, style="Surface.TFrame")
        indicator_filters = ttk.Frame(controls, style="Surface.TFrame")
        self.filter_cells = (data_filters, period_filters, indicator_filters)
        for column, cell in enumerate(self.filter_cells):
            cell.grid(row=0, column=column, sticky="w")
        self.indicator_filters = indicator_filters
        ttk.Label(data_filters, text="Ativo", style="Muted.TLabel").pack(side=tk.LEFT)
        self.symbol_var = tk.StringVar()
        self.symbol_combo = ttk.Combobox(data_filters, textvariable=self.symbol_var, state="readonly", width=14)
        self.symbol_combo.pack(side=tk.LEFT, padx=(6, 14))
        self.symbol_combo.bind("<<ComboboxSelected>>", lambda e: self._symbol_changed())
        ttk.Label(data_filters, text="Timeframe", style="Muted.TLabel").pack(side=tk.LEFT)
        self.timeframe_var = tk.StringVar()
        self.timeframe_combo = ttk.Combobox(data_filters, textvariable=self.timeframe_var, state="readonly", width=12)
        self.timeframe_combo.pack(side=tk.LEFT, padx=(6, 14))
        self.timeframe_combo.bind("<<ComboboxSelected>>", lambda e: self.load())
        ttk.Label(period_filters, text="Período", style="Muted.TLabel").pack(side=tk.LEFT)
        self.period_var = tk.StringVar(value=study.PERIODS[1][0])
        for label, _days in study.PERIODS:
            ttk.Radiobutton(period_filters, text=label, value=label, variable=self.period_var, style="Toolbutton",
                            command=self.load).pack(side=tk.LEFT, padx=(6 if label == study.PERIODS[0][0] else 1, 0))
        self.overlays = {}
        for key, label, tip in (("ma20", "MM 20", "Média móvel (MM)"), ("ma50", "MM 50", "Média móvel (MM)"),
                                ("bb", "Bollinger", "Bandas de Bollinger")):
            var = tk.BooleanVar(value=key != "ma50")
            self.overlays[key] = var
            check = ttk.Checkbutton(indicator_filters, text=label, variable=var, style="Surface.TCheckbutton",
                                    command=self.draw_chart)
            check.pack(side=tk.LEFT, padx=(0 if key == "ma20" else 10, 0))
            Tooltip(check, study.GLOSSARY[tip], self.theme)
        self._controls_stacked = None
        self._controls_layout = None
        controls.bind("<Configure>", self._schedule_layout, add="+")

        self.results_viewport = ScrollableContent(self.frame, self.theme)
        self.results_viewport.pack(fill=tk.BOTH, expand=True)
        self.results_viewport.canvas.bind("<Configure>", self._schedule_layout, add="+")
        results = self.results_viewport.content
        tiles = ttk.Frame(results, style="Surface.TFrame")
        tiles.pack(fill=tk.X, pady=10)
        self.metric_cards = []
        self.tile_values = {}
        for i, (label, key, glossary) in enumerate(TILES):
            metric = MetricCard(tiles, title=label, value="—", hint=study.GLOSSARY[glossary],
                                theme=self.theme, padding=10)
            metric.grid(row=0, column=i, sticky="nsew", padx=(0 if i == 0 else 8, 0))
            self.metric_cards.append(metric)
            self.tile_values[key] = metric.value_label
            metric.value_label.bind("<Configure>", self._schedule_layout, add="+")
        self.return_legend = ttk.Label(results,
            text="Retorno do preço: azul suave = alta · discreto = queda · zero neutro · — = indisponível. Não mede lucro do robô.",
            style="Page.Muted.TLabel", wraplength=1100, justify=tk.LEFT)
        self.return_legend.pack(fill=tk.X, pady=(0, 4))
        self.return_legend.bind("<Configure>", lambda event: self.return_legend.configure(wraplength=max(180, event.width)))
        self._return_value = None
        self.theme.on_change(self._refresh_return_attention)
        self._metric_columns = 0
        tiles.bind("<Configure>", self._schedule_layout, add="+")

        self.status = ttk.Label(self.frame, text="", style="Page.Muted.TLabel", wraplength=1200,
                                justify=tk.LEFT)
        self.status.pack(fill=tk.X, side=tk.BOTTOM, pady=(6, 0))
        self.frame.bind("<Configure>", self._resize_status, add="+")
        self.tabs = ttk.Notebook(results)
        self.tabs.pack(fill=tk.BOTH, expand=True)
        self.tabs.bind("<<NotebookTabChanged>>", self._schedule_layout, add="+")
        self.chart_tab = ttk.Frame(self.tabs, style="Surface.TFrame")
        self.season_tab = ttk.Frame(self.tabs, style="Surface.TFrame")
        self.returns_tab = ttk.Frame(self.tabs, style="Surface.TFrame")
        self.quality_tab = ttk.Frame(self.tabs, style="Surface.TFrame", padding=14)
        self.tabs.add(self.chart_tab, text="Gráfico")
        self.tabs.add(self.season_tab, text="Sazonalidade")
        self.tabs.add(self.returns_tab, text="Retornos")
        self.tabs.add(self.quality_tab, text="Qualidade dos dados")
        self.readout = ttk.Label(self.chart_tab, text="", style="Muted.TLabel", justify=tk.LEFT)
        self.readout.pack(fill=tk.X, padx=12, pady=(8, 0))
        self.readout.bind("<Configure>", lambda event: self.readout.configure(wraplength=max(1, event.width)))
        self.readout.bind("<Configure>", self._schedule_layout, add="+")
        self.quality_text = self.theme.classic(tk.Text(self.quality_tab, wrap=tk.WORD, height=10, padx=10, pady=10))
        self.quality_text.pack(fill=tk.BOTH, expand=True)
        self.quality_text.configure(state=tk.DISABLED)
        self.theme.on_change(self.redraw)
        self.theme.on_change(self._schedule_layout)

    def _schedule_layout(self, _event=None):
        if self._layout_job is None and self.frame.winfo_exists():
            self._layout_job = self.frame.after_idle(self._layout)

    def _destroyed(self, event):
        if event.widget is self.frame and self._layout_job is not None:
            self.frame.after_cancel(self._layout_job)
            self._layout_job = None

    def _layout(self):
        # Mantém a marca durante update_idletasks para coalescer Configure reentrante.
        self.frame.update_idletasks()
        self._layout_controls()
        self._layout_metrics()
        self._layout_figures()
        self._layout_job = None

    def _layout_figures(self):
        if not self._figure_frames:
            return
        selected = self.tabs.nametowidget(self.tabs.select())
        notebook_trim = max(0, self.tabs.winfo_height() - selected.winfo_height())
        fixed = (self.metric_cards[0].master.winfo_reqheight() + 20
                 + self.return_legend.winfo_reqheight() + 4 + notebook_trim + 12)
        minimum = max(480, self.theme.font.metrics("linespace") * 24)
        for key, frame in self._figure_frames.items():
            readout = self.readout.winfo_reqheight() + 8 if key == "chart" else 0
            inset = 2 * int(self.results_viewport.canvas.cget("highlightthickness"))
            available = self.results_viewport.canvas.winfo_height() - inset - fixed - readout
            height = max(minimum, available) if frame.master is selected else minimum
            if int(frame.cget("height")) != height:
                frame.configure(height=height)
                frame.rowconfigure(0, minsize=height)

    def _layout_controls(self):
        widths = [cell.winfo_reqwidth() for cell in self.filter_cells]
        available = max(1, self.controls.winfo_width() - 22)
        columns = (3 if sum(widths) + 24 <= available else
                   2 if widths[0] + widths[1] + 12 <= available else 1)
        self._controls_stacked = columns < 3
        key = (columns, tuple(widths))
        if key == self._controls_layout:
            return
        self._controls_layout = key
        for column in range(3):
            self.controls.columnconfigure(column, weight=1 if column == columns - 1 else 0, minsize=0)
        for index, cell in enumerate(self.filter_cells):
            cell.grid_configure(row=index // columns, column=index % columns,
                                columnspan=columns if columns == 2 and index == 2 else 1,
                                sticky="w", padx=(0 if index % columns == 0 else 12, 0),
                                pady=(8 if index >= columns else 0, 0))

    def _resize_status(self, event):
        self.status.configure(wraplength=max(240, event.width - 28))

    def _layout_metrics(self):
        available = self.metric_cards[0].master.winfo_width()
        required = max(card.winfo_reqwidth() for card in self.metric_cards)
        columns = next((n for n in (6, 3, 2) if n * required + (n - 1) * 8 <= available), 1)
        if columns == self._metric_columns:
            return
        self._metric_columns = columns
        for column in range(6):
            self.metric_cards[0].master.columnconfigure(
                column, weight=1 if column < columns else 0,
                uniform="analysis-metric" if column < columns else "")
        for index, metric in enumerate(self.metric_cards):
            metric.grid(row=index // columns, column=index % columns, sticky="nsew",
                        padx=(0, 8 if index % columns < columns - 1 else 0), pady=(0, 8))

    # --- escolha do ativo -------------------------------------------------------------

    def set_inventory(self, inventory):
        """Atualiza as listas de ativo/timeframe com o que existe na base."""
        self.available = {}
        for item in inventory:
            if item["rows"] and timeframes.parse(item["timeframe"]):
                self.available.setdefault(item["symbol"], []).append(item["timeframe"])
        for labels in self.available.values():
            labels.sort(key=lambda label: timeframes.parse(label).minutes)
        symbols = sorted(self.available)
        self.symbol_combo["values"] = symbols
        if not symbols:
            self.symbol_var.set("")
            self.timeframe_var.set("")
            self._empty("A base ainda não tem barras. Na aba Ativos, escolha um ativo e clique em "
                        "\"Extrair histórico\"; depois volte aqui.")
            return
        if self.symbol_var.get() not in self.available:
            self.symbol_var.set(symbols[0])
            self._symbol_changed()
        else:
            self.timeframe_combo["values"] = self.available[self.symbol_var.get()]

    def _symbol_changed(self):
        labels = self.available.get(self.symbol_var.get(), [])
        self.timeframe_combo["values"] = labels
        if self.timeframe_var.get() not in labels:
            self.timeframe_var.set(labels[0] if labels else "")
        self.load()

    def open(self, symbol, timeframe_label=None):
        if symbol not in self.available:
            return False
        self.symbol_var.set(symbol)
        labels = self.available[symbol]
        self.timeframe_combo["values"] = labels
        self.timeframe_var.set(timeframe_label if timeframe_label in labels else labels[0])
        self.load()
        return True

    # --- carga (em segundo plano) -----------------------------------------------------

    def load(self):
        symbol, label = self.symbol_var.get(), self.timeframe_var.get()
        if not symbol or not label:
            return
        self._request += 1
        request = self._request
        days = dict(study.PERIODS)[self.period_var.get()]
        end = next((item["end"] for item in self.ui.inventory
                    if item["symbol"] == symbol and item["timeframe"] == label), None)
        self.status.config(text=f"Carregando {symbol} ({label})...")
        db = self.ui.app.db_manager
        config = self.ui.app.config
        minutes = timeframes.parse(label).minutes

        def work():
            bars = db.load_ohlcv(symbol, label, start=study.period_start(end, days))
            if bars.empty:
                return None
            session = (config.get("APP", "session_start", fallback="09:00"),
                       config.get("APP", "session_end", fallback="18:30")) if config else ("09:00", "18:30")
            quality = data_quality.check_quality(bars, minutes, *session)
            if data_quality.blocks_price_analysis(quality):
                return {"quality": quality, "session": session, "blocked": True}
            return {"bars": study.add_indicators(bars), "summary": study.summarize(bars),
                    "seasonality": study.seasonality(bars, bar_minutes=minutes),
                    "quality": quality, "session": session}

        def done(result, error):
            if request != self._request:
                return
            if error or result is None:
                self._empty(f"Não foi possível ler {symbol} ({label}): {error}" if error
                            else f"Sem barras de {symbol} ({label}) neste período.")
                return
            if result.get("blocked"):
                self._empty(f"Análise indisponível para {symbol} ({label}): preços ou horários inválidos. "
                            "Veja Qualidade dos dados e confira a fonte antes de analisar.")
                self._show_quality(result["quality"], result["session"])
                self.tabs.select(self.quality_tab)
                return
            self.data = result["bars"]
            self.result = result
            self.candles, self.factor = study.compress(self.data)
            self._show_summary(result["summary"])
            self._show_quality(result["quality"], result["session"])
            self.redraw()
            note = f" Cada candle do gráfico agrupa {self.factor} barras." if self.factor > 1 else ""
            s = result["summary"]
            self.status.config(text=f"{symbol} · {label} · {fmt_int(s['bars'])} barras de "
                                    f"{s['start']:%d/%m/%Y} a {s['end']:%d/%m/%Y}.{note} "
                                    f"Retornos: {s['return_pairs_used']}/{s['return_pairs_total']} pares válidos; "
                                    f"{s['return_pairs_excluded']} excluídos (divisão indefinida). "
                                    + ("Métricas indisponíveis: " + "; ".join(f"{name}: {reason}" for name, reason in
                                        s["unavailable_reasons"].items()) + ". " if s["unavailable_reasons"] else "")
                                    + "Passe o mouse sobre os números e sobre o gráfico para ver detalhes.")

        self.ui.run_bg(work, done)

    def _empty(self, message):
        self.data = self.candles = None
        self.result = None
        self.quality_text.configure(state=tk.NORMAL)
        self.quality_text.delete("1.0", tk.END)
        self.quality_text.insert(tk.END, message)
        self.quality_text.configure(state=tk.DISABLED)
        for value in self.tile_values.values():
            value.config(text="—", style="Value.TLabel")
        self._return_value = None
        self._refresh_return_attention()
        self.status.config(text=message)
        self.readout.config(text="")
        for canvas in self._canvases.values():
            canvas.get_tk_widget().destroy()
        self._canvases = {}
        for frame in self._figure_frames.values():
            frame.destroy()
        self._figure_frames = {}
        self._schedule_layout()

    def _show_summary(self, s):
        digits = 5 if s["last"] < 10 else 2
        self.tile_values["last"].config(text=fmt_num(s["last"], digits))
        self._return_value = s.get("return_pct")
        self._refresh_return_attention()
        self.tile_values["volatility_pct"].config(text=fmt_num(s["volatility_pct"], 1, "%"))
        self.tile_values["max_drawdown_pct"].config(text=fmt_num(s["max_drawdown_pct"], 2, "%"))
        self.tile_values["mean_range_pct"].config(text=fmt_num(s["mean_range_pct"], 3, "%"))
        self.tile_values["up_bars_pct"].config(text=fmt_num(s["up_bars_pct"], 1, "%"))
        self._schedule_layout()

    def _refresh_return_attention(self):
        value = self._return_value
        role = result_attention_role(value)
        fill, foreground = result_attention_colors(value, self.theme.colors)
        style = "MarketReturn.Value.TLabel"
        ttk.Style(self.frame).configure(style, background=fill, foreground=foreground, font=self.theme.font_value)
        prefix = {"positive": "+", "negative": "-", "zero": ""}.get(role, "")
        text = "—" if role == "absent" else prefix + fmt_num(abs(float(value)), 2, "%")
        self.tile_values["return_pct"].configure(text=text, style=style)

    def _show_quality(self, report, session):
        lines = [f"Barras analisadas: {fmt_int(report['rows'])}",
                 f"Dias com dados: {report['bars_per_day']['days']} "
                 f"(de {report['bars_per_day']['min']} a {report['bars_per_day']['max']} barras por dia, "
                 f"média {fmt_num(report['bars_per_day']['mean'], 1)})", ""]
        checks = (("gaps", f"Intervalos sem barras na janela configurada ({session[0]}–{session[1]})",
                   "Possíveis lacunas: confira sessões, feriados e blocos de extração. Esta verificação não distingue mercado fechado de falha de coleta."),
                  ("invalid_time", "Horários inválidos ou ausentes", "Confira o contrato temporal e a fonte; nenhum horário foi preenchido."),
                  ("unordered_time", "Horários fora de ordem na entrada", "Confira a ordenação na origem antes de calcular retornos."),
                  ("nonfinite_ohlc", "Barras com preço ausente, não numérico ou não finito", "Métricas de preço indisponíveis; confira a fonte sem preencher valores."),
                  ("invalid_ohlc", "Barras com preços inconsistentes (ex.: máxima menor que a mínima)",
                   "Dado suspeito vindo da corretora. Confira estas barras antes de usá-las."),
                  ("duplicate_time", "Horários repetidos", "Confira origem, revisões e contrato temporal antes de corrigir a base."),
                  ("invalid_volume", "Barras com volume ausente, negativo ou não finito", "Volume ausente não é zero; confira a fonte antes de analisar volume."),
                  ("zero_volume", "Barras sem volume",
                   "Comum fora do horário de maior liquidez. Só preocupa se forem muitas."))
        for key, label, advice in checks:
            check = report.get(key, {"count": 0, "examples": []})
            count = check["count"]
            lines.append(f"{'✓' if not count else '⚠'}  {label}: {fmt_int(count)}")
            if count:
                lines.append(f"     {advice}")
                lines.append("     Exemplos: " + "; ".join(str(x) for x in check["examples"][:3]))
        if not data_quality.has_problems(report):
            lines += ["", "Nenhum problema detectado pelas verificações acima neste período."]
        lines += ["", "O horário do pregão usado na verificação vem de [APP] session_start / session_end "
                      "no arquivo de configuração. Não é um calendário oficial por símbolo.",
                  "Estas verificações não certificam cobertura completa, fuso histórico, origem nem fechamento das barras."]
        db = getattr(self.ui.app, "db_manager", None)
        if hasattr(db, "time_context"):
            clock = db.time_context()
            lines += [f"Relógio configurado: base={clock['time_basis']}; estado={clock['offset_state']}; "
                      f"offset={clock['broker_utc_offset']}; zona={clock['broker_timezone'] or 'não declarada'}. "
                      "Configuração declarada não comprova o fuso da fonte nem mudanças históricas de offset."]
        self.quality_text.configure(state=tk.NORMAL)
        self.quality_text.delete("1.0", tk.END)
        self.quality_text.insert(tk.END, "\n".join(lines))
        self.quality_text.configure(state=tk.DISABLED)

    # --- gráficos ---------------------------------------------------------------------

    def _figure(self, key, parent):
        """Figura do matplotlib dentro da aba (recriada a cada desenho)."""
        from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
        from matplotlib.figure import Figure
        old = self._canvases.pop(key, None)
        if old:
            old.get_tk_widget().destroy()
        old_frame = self._figure_frames.pop(key, None)
        if old_frame:
            old_frame.destroy()
        c = self.theme.colors
        fig = Figure(dpi=100, facecolor=c["surface"])
        # O backend pode mudar o tamanho pedido pelo Canvas ao aplicar o DPI real.
        # O contêiner conserva a altura legível sem disputar configure() com ele.
        frame = ttk.Frame(parent, style="Surface.TFrame")
        # O Notebook nativo pode dar à página a altura requisitada por outra aba.
        # Só a largura vem do pack; a altura segue fonte e espaço útil do viewport.
        frame.pack(fill=tk.X, expand=False, padx=6, pady=6)
        frame.grid_propagate(False)
        frame.columnconfigure(0, weight=1)
        self._figure_frames[key] = frame
        canvas = FigureCanvasTkAgg(fig, master=frame)
        widget = canvas.get_tk_widget()
        # A requisição inicial também pode incorporar DPI; não é a altura de referência.
        height = max(480, self.theme.font.metrics("linespace") * 24)
        frame.configure(height=height)
        frame.rowconfigure(0, weight=1, minsize=height)
        widget.configure(background=c["surface"], highlightthickness=0)
        widget.grid(row=0, column=0, sticky="nsew")
        self._canvases[key] = canvas
        self._schedule_layout()
        return fig, canvas

    def _style_axis(self, ax, title=None):
        c = self.theme.colors
        ax.set_facecolor(c["surface"])
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
        for side in ("left", "bottom"):
            ax.spines[side].set_color(c["axis"])
        ax.tick_params(colors=c["muted"], labelsize=8, length=0)
        ax.grid(True, axis="y", color=c["grid"], linewidth=0.6)
        ax.set_axisbelow(True)
        if title:
            ax.set_title(title, color=c["ink2"], fontsize=9, loc="left", pad=4)

    def redraw(self):
        if self.data is None:
            return
        try:
            self.draw_chart()
            self.draw_seasonality()
            self.draw_returns()
        except ImportError:
            self.status.config(text="Gráficos indisponíveis: instale o matplotlib (pip install matplotlib).")
        except Exception as e:
            log.exception("Erro ao desenhar a análise")
            self.status.config(text=f"Erro ao desenhar o gráfico: {e}")

    def draw_chart(self):
        if self.candles is None:
            return
        from matplotlib.ticker import MaxNLocator
        c = self.theme.colors
        df = self.candles
        self.readout.config(text="")
        fig, canvas = self._figure("chart", self.chart_tab)
        grid = fig.add_gridspec(3, 1, height_ratios=(3.2, 1, 1), hspace=0.22, left=0.06, right=0.97,
                                top=0.94, bottom=0.07)
        price = fig.add_subplot(grid[0])
        volume = fig.add_subplot(grid[1], sharex=price)
        rsi = fig.add_subplot(grid[2], sharex=price)
        x = np.arange(len(df))
        up = (df["close"] >= df["open"]).values
        colors = np.where(up, c["up"], c["down"])
        price.vlines(x, df["low"], df["high"], colors=colors, linewidth=0.8)
        body_low = np.minimum(df["open"], df["close"])
        body = np.maximum((df["close"] - df["open"]).abs(), (df["high"].max() - df["low"].min()) * 0.0008)
        price.bar(x, body, bottom=body_low, width=0.7, color=colors, linewidth=0)

        # Indicadores calculados nas barras originais; no gráfico vale o valor da última barra de cada candle
        last = (np.arange(len(df)) + 1) * self.factor - 1
        last[-1] = len(self.data) - 1
        ind = self.data.iloc[np.minimum(last, len(self.data) - 1)].reset_index(drop=True)
        if self.overlays["ma20"].get():
            price.plot(x, ind["ma20"], color=c["series"][0], linewidth=1.6, label="MM 20")
        if self.overlays["ma50"].get():
            price.plot(x, ind["ma50"], color=c["series"][1], linewidth=1.6, label="MM 50")
        if self.overlays["bb"].get():
            price.plot(x, ind["bb_upper"], color=c["series"][2], linewidth=1.1, linestyle=(0, (4, 3)),
                       label="Bollinger (20, 2)")
            price.plot(x, ind["bb_lower"], color=c["series"][2], linewidth=1.1, linestyle=(0, (4, 3)))
        if price.get_legend_handles_labels()[0]:
            price.legend(loc="lower left", bbox_to_anchor=(0, 1.0), fontsize=8, frameon=False,
                         labelcolor=c["ink2"], ncol=3, borderaxespad=0.2)

        if "tick_volume" in df.columns:
            volume.bar(x, df["tick_volume"], width=0.7, color=c["muted"], linewidth=0)
        rsi.plot(x, ind["rsi"], color=c["series"][0], linewidth=1.4)
        for level in (30, 70):
            rsi.axhline(level, color=c["axis"], linewidth=0.8, linestyle=(0, (3, 3)))
        rsi.set_ylim(0, 100)
        rsi.set_yticks((30, 70))
        self._style_axis(price)
        self._style_axis(volume, "Volume (ticks)")
        self._style_axis(rsi, "IFR (14)")
        price.tick_params(labelbottom=False)
        volume.tick_params(labelbottom=False)
        volume.yaxis.set_major_locator(MaxNLocator(3))

        times = pd.to_datetime(df["time"])
        intraday = timeframes.parse(self.timeframe_var.get()).minutes * self.factor < 1440
        fmt = "%d/%m %H:%M" if intraday else "%d/%m/%y"
        ticks = np.linspace(0, len(df) - 1, min(8, len(df))).astype(int)
        rsi.set_xticks(ticks)
        rsi.set_xticklabels([times.iloc[i].strftime(fmt) for i in ticks])
        price.set_xlim(-1, len(df))

        cross = [ax.axvline(0, color=c["muted"], linewidth=0.7, visible=False) for ax in (price, volume, rsi)]

        def hover(event):
            inside = event.inaxes in (price, volume, rsi) and event.xdata is not None
            for line in cross:
                line.set_visible(inside)
            if not inside:
                self.readout.config(text="")
                canvas.draw_idle()
                return
            i = int(min(max(round(event.xdata), 0), len(df) - 1))
            for line in cross:
                line.set_xdata([i, i])
            row, extra = df.iloc[i], ind.iloc[i]
            digits = 5 if row["close"] < 10 else 2
            parts = [times.iloc[i].strftime("%d/%m/%Y %H:%M"),
                     f"Abertura {fmt_num(row['open'], digits)}", f"Máxima {fmt_num(row['high'], digits)}",
                     f"Mínima {fmt_num(row['low'], digits)}", f"Fechamento {fmt_num(row['close'], digits)}"]
            if "tick_volume" in df.columns:
                parts.append(f"Volume {fmt_int(row['tick_volume'])}")
            if pd.notna(extra["rsi"]):
                parts.append(f"IFR {fmt_num(extra['rsi'], 1)}")
            self.readout.config(text="   ·   ".join(parts))
            canvas.draw_idle()

        canvas.mpl_connect("motion_notify_event", hover)
        canvas.mpl_connect("figure_leave_event", lambda e: hover(e))
        canvas.draw_idle()

    def draw_seasonality(self):
        from matplotlib.ticker import FuncFormatter
        c = self.theme.colors
        season = self.result["seasonality"]
        fig, canvas = self._figure("season", self.season_tab)
        grid = fig.add_gridspec(1, 2, left=0.07, right=0.98, top=0.88, bottom=0.14, wspace=0.22)
        hour_ax, day_ax = fig.add_subplot(grid[0]), fig.add_subplot(grid[1])
        self._style_axis(hour_ax, "Amplitude média por hora do dia (%): quando o ativo mais oscila")
        self._style_axis(day_ax, "Retorno médio por barra em cada dia da semana (%)")
        hours = season["hour"]
        if hours.empty or not hours["range_pct"].notna().any():
            hour_ax.text(0.5, 0.5, "Sem amostra por hora nesta seleção.\nBarras diárias ou maiores não têm visão horária." if hours.empty
                         else "Amplitude indisponível: denominadores inválidos.",
                         ha="center", va="center", transform=hour_ax.transAxes, color=c["ink2"], fontsize=9)
            hour_ax.set_xticks([])
            hour_ax.set_yticks([])
        else:
            hour_ax.bar(hours.index, hours["range_pct"], width=0.72, color=c["series"][0], linewidth=0)
            hour_ax.set_xticks(list(hours.index))
            hour_ax.set_xticklabels([f"{h}h" for h in hours.index])
        for ax in (hour_ax, day_ax):
            ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _pos: fmt_num(v, 3, "%")))
        days = season["weekday"]
        if not days.empty and days["return_pct"].notna().any():
            day_ax.bar(range(len(days)), days["return_pct"], width=0.6, linewidth=0,
                       color=[c["up"] if v >= 0 else c["down"] for v in days["return_pct"]])
            day_ax.axhline(0, color=c["axis"], linewidth=0.9)
            day_ax.set_xticks(range(len(days)))
            day_ax.set_xticklabels([study.WEEKDAYS[d] for d in days.index])
            span = max(days["return_pct"].abs().max(), 1e-9)
            for i, value in enumerate(days["return_pct"]):
                day_ax.annotate(fmt_num(value, 4, "%"), (i, value), ha="center", color=c["ink2"], fontsize=8,
                                xytext=(0, 4 if value >= 0 else -11), textcoords="offset points")
            day_ax.set_ylim(-span * 1.35, span * 1.35)
        else:
            day_ax.text(.5, .5, "Retorno indisponível: denominadores inválidos.", transform=day_ax.transAxes,
                        ha="center", color=c["ink2"])
        for ax, data, column in ((hour_ax, hours, "range_samples"), (day_ax, days, "return_samples")):
            if not data.empty:
                ax.set_xlabel(f"Amostra: {int(data[column].sum())}/{int(data.bars.sum())} barras com razão definida",
                              color=c["ink2"], fontsize=8)
        canvas.draw_idle()

    def draw_returns(self):
        from matplotlib.ticker import FuncFormatter
        c = self.theme.colors
        fig, canvas = self._figure("returns", self.returns_tab)
        ax = fig.add_subplot(111)
        fig.subplots_adjust(left=0.06, right=0.98, top=0.88, bottom=0.14)
        returns = study.bar_returns(self.data["close"]).dropna() * 100
        total = max(0, len(self.data) - 1)
        self._style_axis(ax, "Distribuição dos retornos por barra (%): com que frequência cada variação acontece")
        if len(returns) > 1:
            low, high = returns.quantile(0.005), returns.quantile(0.995)
            ax.hist(returns.clip(low, high), bins=61, color=c["series"][0], linewidth=0, rwidth=0.88)
            ax.axvline(0, color=c["axis"], linewidth=0.9)
            q5, q95 = returns.quantile(0.05), returns.quantile(0.95)
            note = (f"Média {fmt_num(returns.mean(), 4, '%')}   ·   Mediana {fmt_num(returns.median(), 4, '%')}\n"
                    f"90% das barras variam entre {fmt_num(q5, 3, '%')} e {fmt_num(q95, 3, '%')}\n"
                    f"Máximo {fmt_num(returns.max(), 2, '%')}   ·   Mínimo {fmt_num(returns.min(), 2, '%')}\n"
                    f"n={len(returns)}/{total} pares; excluídos {total-len(returns)} indefinidos\n"
                    "Histograma: extremos agrupados nos quantis 0,5%–99,5%; métricas usam valores originais.")
            ax.text(0.985, 0.95, note, transform=ax.transAxes, ha="right", va="top", color=c["ink2"], fontsize=9,
                    linespacing=1.6)
            ax.set_ylabel("Barras", color=c["muted"], fontsize=8)
            ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _pos: fmt_num(v, 2, "%")))
        else:
            ax.text(.5, .5, f"Amostra insuficiente: {len(returns)}/{total} pares válidos; "
                    f"{total-len(returns)} excluídos por divisão indefinida.", transform=ax.transAxes,
                    ha="center", color=c["ink2"])
        canvas.draw_idle()
