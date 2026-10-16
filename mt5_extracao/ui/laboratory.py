"""
Aba "Laboratório" (Fase 16, T16.11): onde os robôs ganham e perdem, pelas duas lentes.

- Por robô: o robô escolhido em todos os ativos e timeframes em que foi testado.
- Por ativo: todos os robôs testados no ativo escolhido.

Guias: Visão geral, Tempo e calendário, Microestrutura, Comparativo, Execução (o mesmo run re-simulado em vários modos,
T16.6 a T16.8) e Insights (insights validados, simulador de regras e exportação da proposta, T16.9 e T16.10). Os
números vêm de mt5_extracao/trade_analytics (sem Tkinter); toda conta roda em segundo plano e só a guia visível é
desenhada.
"""
import logging
import tkinter as tk
from datetime import timedelta
from tkinter import ttk

import numpy as np
import pandas as pd

from mt5_extracao.ui import charts
from mt5_extracao.ui.components import (ChartPanel, ContentPanel, FilterBar, MatrixTable, MetricCard, Notice,
                                       PageHeader, ScrollableContent, TablePanel)
from mt5_extracao.ui.theme import Tooltip, fmt_int, fmt_num
from mt5_extracao.ui.visual_tokens import SPACING, result_attention_colors
from mt5_extracao.ui.visual_tokens import TYPOGRAPHY

log = logging.getLogger(__name__)

LENS_ROBOT, LENS_ASSET = "robot", "asset"
PERIODS = (("Tudo", None), ("Último ano", 365), ("Últimos 6 meses", 182), ("Últimos 3 meses", 91))
RELATIONS_TAB = "Relações e aderência"
TABS = ("Visão geral", "Tempo e calendário", "Microestrutura", "Comparativo", "Execução", "Insights",
        RELATIONS_TAB)
DISABLED_TABS: dict = {}
WEEKDAYS = {0: "Seg", 1: "Ter", 2: "Qua", 3: "Qui", 4: "Sex", 5: "Sáb", 6: "Dom"}
IMPACTS = {0: "Sem evento", 1: "Baixo", 2: "Médio", 3: "Alto"}
SHORT_MODES = {"ponto_fixo": "Base", "alvo_r": "Alvo R", "break_even": "BE", "trailing_atr": "Trail ATR",
               "trailing_barras": "Trail N", "gradiente_fixo": "Grad. fixo", "gradiente_dinamico": "Grad. ATR",
               "gradiente_direcional": "Pirâm.", "martingale": "Mart.", "anti_martingale": "Anti-mart."}
KPIS = (("n", "Operações", "Quantas operações há no recorte."),
        ("win_rate", "Acerto", "Percentual de operações com resultado positivo."),
        ("profit_factor", "Fator de lucro", "Soma dos ganhos dividida pela soma das perdas. Acima de 1, o robô ganhou "
                                            "mais do que perdeu."),
        ("expectancy", "Expectativa", "Resultado médio por operação, com o intervalo de 90% (bootstrap por dias)."),
        ("max_drawdown_pct", "Queda máxima", "Maior queda da curva de resultado a partir de um topo."))
NOTE_STYLE = {"success": "Success", "warning": "Warning", "error": "Error", "info": "Info",
              "neutral": "Neutral", "partial": "Warning", "loading": "Info", "empty": "Neutral",
              "unavailable": "Error"}
NOTE_LABEL = {"success": "Concluído", "warning": "Aviso", "error": "Erro", "info": "Informação",
              "neutral": "Status", "partial": "Parcial", "loading": "Carregando", "empty": "Sem dados",
              "unavailable": "Indisponível"}


class LaboratoryPanel:
    def __init__(self, parent, ui):
        self.ui = ui
        self.theme = ui.theme
        self.frame = ttk.Frame(parent, padding=SPACING["md"])
        self.runs = []
        self.lab = None            # livro enriquecido da seleção (sem filtros de tela)
        self.view = None           # com os filtros de tela
        self.loaded = False
        self._request = 0
        self._charts = {}
        self._chart_panels = {}
        self._analysis_notebooks = {}
        self._analysis_views = {}
        self._drawn = set()        # guias já desenhadas com o recorte atual
        self.relations_panel = None  # F18 é apresentada por componente separado e sem acesso ao ledger F16.

        head = PageHeader(self.frame, "Laboratório",
                          "operações dos robôs: onde ganham, onde perdem e como executar melhor")
        head.pack(fill=tk.X, pady=(0, SPACING["md"]))
        self.import_button = head.add_action(
            "Importar backtests antigos", self.import_runs,
            tooltip="Traz para o Laboratório os backtests gravados antes dele existir. Os de robôs MQL5 entram sem o "
                    "horário de entrada (análise só por horário de saída).", theme=self.theme)
        self.refresh_button = head.add_action("Atualizar", self.reload, style="Accent.TButton")

        # Cabeçalho e seleção permanecem fora da área rolável de resultados.
        for widget in (head.title_label, head.subtitle_label, head.action_frame):
            widget.pack_forget()
        head.title_label.grid(row=0, column=0, sticky="w")
        head.action_frame.grid(row=0, column=1, sticky="e")
        head.subtitle_label.grid(row=1, column=0, columnspan=2, sticky="w", pady=(SPACING["xs"], 0))
        head.columnconfigure(0, weight=1)
        head.bind("<Configure>", lambda event: head.subtitle_label.configure(wraplength=max(180, event.width)))
        self._build_filters()
        self.results_viewport = ScrollableContent(self.frame, self.theme)
        self.results_viewport.pack(fill=tk.BOTH, expand=True)
        self.results_body = self.results_viewport.content
        self._build_kpis()
        self.notice = Notice(self.results_body, "", kind="info", theme=self.theme)
        self.notice.message.configure(wraplength=1100)
        self.notice.bind("<Configure>", self._resize_notice, add="+")
        self.note = self.notice.message  # texto de estado preservado para as telas e testes existentes.
        self._note_kind = "info"
        self.tabs = ttk.Notebook(self.results_body, style="Compact.TNotebook")
        self.tabs.pack(fill=tk.BOTH, expand=True)
        self.tab_frames = {}
        for name in TABS:
            tab = ttk.Frame(self.tabs, padding=8, style="Surface.TFrame")
            self.tabs.add(tab, text=name, state="disabled" if name in DISABLED_TABS else "normal")
            self.tab_frames[name] = tab
            if name in DISABLED_TABS:
                ttk.Label(tab, text=DISABLED_TABS[name], style="Muted.TLabel").pack(pady=20)
        self.tabs.bind("<<NotebookTabChanged>>", lambda e: self.render_visible(), add="+")
        self.mode_results = {}     # modo -> DataFrame (run_id, seq, pnl, ...) do run comparado
        self.mode_metrics = {}
        self.found = []            # insights da última procura
        self.evaluation = None
        self._build_execution_tab()
        self._build_insights_tab()
        self.theme.on_change(self._theme_changed)

    # --- montagem ----------------------------------------------------------------------

    def _resize_notice(self, event):
        reserve = self.notice.badge.winfo_reqwidth() + 4 * SPACING["sm"]
        self.note.configure(wraplength=max(180, event.width - reserve))

    def set_note(self, text):
        """Estado, disponibilidade ou qualidade acima das guias; a faixa some sem aviso."""
        if not text:
            self.note.config(text="")
            self.notice.pack_forget()
            return
        lowered = text.casefold()
        if "indisponível" in lowered or "indisponíveis" in lowered:
            kind = "unavailable"
        elif "erro" in lowered or "não foi possível" in lowered:
            kind = "error"
        elif "calculando" in lowered or "procurando" in lowered:
            kind = "loading"
        elif "nenhuma operação" in lowered or "ainda não tem operações" in lowered:
            kind = "empty"
        elif "amostra pequena" in lowered or "sem horário de entrada" in lowered or "sem calendário" in lowered:
            kind = "partial"
        else:
            kind = "info"
        style = NOTE_STYLE[kind]
        self.notice.configure(style=f"{style}.Notice.TFrame")
        self.notice.badge.set_status(NOTE_LABEL[kind], kind=kind)
        self.note.configure(text=text, style=f"{style}.Notice.TLabel")
        self._note_kind = kind
        if self._current_tab_name() == RELATIONS_TAB:
            self.notice.pack_forget()
        else:
            self.notice.pack(fill=tk.X, pady=(0, SPACING["sm"]), before=self.tabs)

    def _build_filters(self):
        bar = FilterBar(self.frame)
        bar.pack(fill=tk.X, pady=(0, SPACING["sm"]))
        self.filter_bar = bar
        cells = [ttk.Frame(bar, style="Surface.TFrame") for _ in range(4)]
        self.filter_cells = cells
        mode, selection, period_cell, news_cell = cells
        ttk.Label(mode, text="Modalidade:", style="Surface.TLabel").pack(side=tk.LEFT)
        self.lens = tk.StringVar(value=LENS_ROBOT)
        for value, label, hint in ((LENS_ROBOT, "Por robô", "Em quais ativos, horários e condições este robô vai "
                                                             "melhor?"),
                                   (LENS_ASSET, "Por ativo", "Que robôs exploram melhor este ativo?")):
            w = ttk.Radiobutton(mode, text=label, value=value, variable=self.lens, style="Surface.TRadiobutton",
                                command=self._lens_changed)
            w.pack(side=tk.LEFT, padx=(8, 0))
            Tooltip(w, hint, self.theme)
        self.selector_label = ttk.Label(selection, text="Robô:", style="Surface.TLabel")
        self.selector_label.pack(side=tk.LEFT, padx=(0, 4))
        self.selected = tk.StringVar()
        self.selector = ttk.Combobox(selection, textvariable=self.selected, state="readonly", width=28)
        self.selector.pack(side=tk.LEFT)
        self.selector.bind("<<ComboboxSelected>>", lambda e: self.load_lab())
        ttk.Label(period_cell, text="Período:", style="Surface.TLabel").pack(side=tk.LEFT, padx=(0, 4))
        self.period = tk.StringVar(value=PERIODS[0][0])
        period = ttk.Combobox(period_cell, textvariable=self.period, state="readonly", width=16,
                              values=[p[0] for p in PERIODS])
        period.pack(side=tk.LEFT)
        period.bind("<<ComboboxSelected>>", lambda e: self.apply_filters())
        self.hide_news = tk.BooleanVar(value=False)
        news = ttk.Checkbutton(news_cell, text="Ocultar notícias de impacto alto", variable=self.hide_news,
                               style="Surface.TCheckbutton",
                               command=self.apply_filters)
        news.pack(side=tk.LEFT)
        Tooltip(news, "Tira as operações com evento de impacto alto na janela de [TRADE_ANALYTICS] "
                      "event_window_before_min / after_min. Calendário: mt5x trades calendar import.", self.theme)

        self._filter_columns = None
        bar.bind("<Configure>", self._layout_filters, add="+")

    def _layout_filters(self, event):
        widths = [cell.winfo_reqwidth() for cell in self.filter_cells]
        gap = SPACING["sm"]
        available = max(1, event.width - 2 * gap)
        columns = 4 if sum(widths) + 3 * gap <= available else (
            2 if max(widths[0], widths[2]) + max(widths[1], widths[3]) + gap <= available else 1)
        if columns == self._filter_columns:
            return
        self._filter_columns = columns
        for index in range(4):
            self.filter_bar.columnconfigure(index, weight=1 if index < columns else 0)
        for index, cell in enumerate(self.filter_cells):
            cell.grid(row=index // columns, column=index % columns, sticky="w",
                      padx=(0 if index % columns == 0 else gap, 0),
                      pady=(0 if index < columns else SPACING["xs"], 0))

    def _build_kpis(self):
        row = ttk.Frame(self.results_body)
        self.kpi_row = row
        row.pack(fill=tk.X, pady=(0, SPACING["sm"]))
        self.kpi = {}
        self.metric_cards = {}
        for k, (key, title, hint) in enumerate(KPIS):
            row.columnconfigure(k, weight=1, uniform="kpi")
            metric = MetricCard(row, title, value="—", hint=hint, theme=self.theme, padding=SPACING["sm"])
            metric.grid(row=0, column=k, sticky="nsew", padx=(0 if k == 0 else SPACING["sm"], 0))
            self.metric_cards[key] = metric
            self.kpi[key] = (metric.value_label, metric.detail_label)

    def _chart(self, tab, key, row, col, rowspan=1, colspan=1, height=3.0, parent=None):
        chart = self._charts.get(key)
        if chart is None:
            chart_parent = parent or self.tab_frames[tab]
            panel = ChartPanel(chart_parent, padding=SPACING["xs"])
            panel.grid(row=row, column=col, rowspan=rowspan, columnspan=colspan, sticky="nsew",
                       padx=SPACING["xs"], pady=SPACING["xs"])
            chart = charts.Chart(panel.chart_area, self.theme, height)
            chart.widget.pack(fill=tk.BOTH, expand=True)
            self._chart_panels[key] = panel
            chart_parent.rowconfigure(row, weight=1)
            chart_parent.columnconfigure(col, weight=1)
            self._charts[key] = chart
        return chart

    def _overview_view(self, key, title):
        if not hasattr(self, "overview_notebook"):
            self.overview_notebook = ttk.Notebook(self.tab_frames["Visão geral"], style="Compact.TNotebook")
            self.overview_notebook.pack(fill=tk.BOTH, expand=True)
            self.overview_views = {}
        if key not in self.overview_views:
            frame = ttk.Frame(self.overview_notebook, padding=SPACING["xs"], style="Surface.TFrame")
            self.overview_notebook.add(frame, text=title)
            self.overview_views[key] = frame
        return self.overview_views[key]

    def _analysis_view(self, tab, key, title):
        """Dá altura e largura úteis a cada gráfico de análises com várias séries."""
        if tab not in self._analysis_notebooks:
            notebook = ttk.Notebook(self.tab_frames[tab], style="Compact.TNotebook")
            notebook.pack(fill=tk.BOTH, expand=True)
            self._analysis_notebooks[tab] = notebook
            self._analysis_views[tab] = {}
        views = self._analysis_views[tab]
        if key not in views:
            frame = ttk.Frame(self._analysis_notebooks[tab], padding=SPACING["xs"], style="Surface.TFrame")
            self._analysis_notebooks[tab].add(frame, text=title)
            views[key] = frame
        return views[key]

    # --- leitura -----------------------------------------------------------------------

    def _db(self):
        db = getattr(self.ui.app, "db_manager", None)
        return db if db is not None and db.is_connected() else None

    def _config(self):
        return getattr(self.ui.app, "config", None)

    def on_show(self):
        if not self.loaded:
            self.reload()

    def reload(self):
        db = self._db()
        if db is None:
            self.set_note("Base de dados indisponível.")
            return
        self.loaded = True
        from mt5_extracao.trade_analytics import ledger

        def done(runs, error):
            if error is not None:
                self.set_note(f"Não foi possível ler o livro de operações: {error}")
                return
            self.runs = runs or []
            self._fill_selector()
        self.ui.run_bg(lambda: ledger.runs(db), done)

    def _options(self):
        key = "robot_key" if self.lens.get() == LENS_ROBOT else "symbol"
        return sorted({r[key] for r in self.runs if r.get(key)})

    def _fill_selector(self):
        options = self._options()
        self.selector.configure(values=options)
        self.selector_label.config(text="Robô:" if self.lens.get() == LENS_ROBOT else "Ativo:")
        if not options:
            self.selected.set("")
            self.lab = self.view = None
            self._empty("O Laboratório ainda não tem operações. Rode um backtest na aba Robôs ou em Minha base, ou "
                        "clique em \"Importar backtests antigos\".")
            return
        if self.selected.get() not in options:
            self.selected.set(options[0])
        self.load_lab()

    def _lens_changed(self):
        self.selected.set("")
        self._fill_selector()

    def load_lab(self):
        db, config, choice = self._db(), self._config(), self.selected.get()
        if db is None or not choice:
            return
        self._request += 1
        request = self._request
        lens = self.lens.get()
        self.set_note("Calculando o contexto das operações...")
        from mt5_extracao.trade_analytics import aggregate

        def work():
            filters = {"robot_key": choice} if lens == LENS_ROBOT else {"symbol": choice}
            return aggregate.frame(db, config, **filters)

        def done(lab, error):
            if request != self._request:
                return                  # o usuário já escolheu outra coisa
            if error is not None:
                self.set_note(f"Erro ao montar o Laboratório: {error}")
                return
            self.lab = lab
            self.apply_filters()
        self.ui.run_bg(work, done)

    def apply_filters(self):
        if self.lab is None:
            return
        view = self.lab
        days = dict(PERIODS).get(self.period.get())
        if days and not view.empty:
            view = view[view["day"] >= view["day"].max() - timedelta(days=days)]
        if self.hide_news.get() and "event_impact" in view:
            view = view[pd.to_numeric(view["event_impact"], errors="coerce").fillna(0) < 3]
        self.view = view
        self._drawn = set()
        self.mode_results, self.mode_metrics, self.evaluation = {}, {}, None
        self._show_kpis()
        self.render_visible()

    # --- desenho -----------------------------------------------------------------------

    def _empty(self, text):
        for key in ("n", "win_rate", "profit_factor", "expectancy", "max_drawdown_pct"):
            self.metric_cards[key].set_value("—", "", style="Value.TLabel")
        self.set_note(text)
        self._drawn = set()
        for chart in self._charts.values():
            chart.message(text)

    def _show_kpis(self):
        from mt5_extracao.trade_analytics import aggregate
        view = self.view
        if view is None or view.empty:
            self._empty("Nenhuma operação neste recorte.")
            return
        m = aggregate.summarize(view, aggregate._initial_equity(self._config()), aggregate.min_trades(self._config()))
        self.metric_cards["n"].set_value(fmt_int(m["n"]), f"{view['run_id'].nunique()} run(s)")
        self.metric_cards["win_rate"].set_value(fmt_num(m["win_rate"] * 100, 1, "%"), "")
        self.metric_cards["profit_factor"].set_value(fmt_num(m["profit_factor"], 2), "")
        expectancy_style = ("Good.TLabel" if m["ci_low"] is not None and m["ci_low"] > 0 else
                            "Bad.TLabel" if m["ci_high"] is not None and m["ci_high"] < 0 else "Value.TLabel")
        expectancy_detail = (f"90%: {charts.money(m['ci_low'])} a {charts.money(m['ci_high'])}"
                             if m["ci_low"] is not None else "intervalo: poucos dias")
        self.metric_cards["expectancy"].set_value(charts.money(m["expectancy"]), expectancy_detail,
                                                   style=expectancy_style)
        self.metric_cards["max_drawdown_pct"].set_value(fmt_num(m["max_drawdown_pct"] * 100, 1, "%"), "")
        partial = int((view["completeness"] == "parcial").sum())
        notes = []
        if m["small"]:
            notes.append(f"Amostra pequena ({m['n']} operações): use como pista, não como conclusão.")
        if partial:
            notes.append(f"{partial} operação(ões) sem horário de entrada (robôs MQL5 antes da T16.2b): entram só "
                         "nas análises de calendário, pela saída.")
        self.set_note(" ".join(notes))

    def render_visible(self):
        name = self.tabs.tab(self.tabs.select(), "text")
        self._sync_tab_context(name)
        if name == RELATIONS_TAB:
            if self.relations_panel is None:
                from mt5_extracao.ui.relations import RelationsPanel
                self.relations_panel = RelationsPanel(self.tab_frames[RELATIONS_TAB], self.theme)
                self.relations_panel.frame.pack(fill=tk.BOTH, expand=True)
            if name not in self._drawn:
                self.relations_panel.render()
                self._drawn.add(name)
            return
        if self.view is None or self.view.empty:
            return
        if name in self._drawn or name in DISABLED_TABS:
            return
        try:
            {"Visão geral": self._overview, "Tempo e calendário": self._time, "Microestrutura": self._micro,
             "Comparativo": self._compare, "Execução": self._execution_refresh,
             "Insights": self._insights_refresh}[name]()
            self._drawn.add(name)
        except ImportError:
            self.set_note("Gráficos indisponíveis: instale o matplotlib (pip install matplotlib).")
        except Exception as e:
            log.exception("Erro ao desenhar o Laboratório")
            self.set_note(f"Erro ao desenhar: {e}")

    def _theme_changed(self):
        self._drawn = set()
        self.render_visible()

    def _current_tab_name(self):
        try:
            selected = self.tabs.select()
            return self.tabs.tab(selected, "text") if selected else ""
        except (AttributeError, tk.TclError):
            return ""

    def _sync_tab_context(self, name):
        """Não exibe filtros/KPIs de operações como se filtrassem as relações de indicadores."""
        if name == RELATIONS_TAB:
            self.filter_bar.pack_forget()
            self.kpi_row.pack_forget()
            self.notice.pack_forget()
            return
        if not self.kpi_row.winfo_manager():
            self.kpi_row.pack(fill=tk.X, pady=(0, SPACING["sm"]), before=self.notice if self.notice.winfo_manager()
                              else self.tabs)
        if not self.filter_bar.winfo_manager():
            self.filter_bar.pack(fill=tk.X, pady=(0, SPACING["sm"]), before=self.kpi_row)
        text = self.note.cget("text")
        if text and not self.notice.winfo_manager():
            self.set_note(text)

    def _crosstab(self, view, row, col, row_order=None):
        from mt5_extracao.trade_analytics import aggregate
        table = aggregate.crosstab(view, row, col, config=self._config())
        if row_order and not table["value"].empty:
            order = [r for r in row_order if r in table["value"].index]
            table = {k: v.reindex(order) for k, v in table.items()}
        return table

    def _heat(self, chart, ax, view, row, col, title, col_labels=None, row_labels=None, row_order=None):
        table = self._crosstab(view, row, col, row_order)
        charts.heatmap(chart, ax, table["value"], table["n"], table["small"], title, row_labels, col_labels)
        self._sample_notice(chart, table["small"])

    def _sample_notice(self, chart, flags):
        """Explica a hachura sem recalcular o limite de amostra definido pelo motor analítico."""
        panel = next((self._chart_panels[key] for key, candidate in self._charts.items() if candidate is chart), None)
        if panel is None:
            return
        has_small = flags is not None and bool(np.asarray(flags, dtype=bool).any())
        is_heatmap = any(axis.images for axis in chart.figure.axes)
        if not has_small:
            text = None
        elif is_heatmap:
            text = ("Cinza pontilhado = sem operações; hachura diagonal = amostra positiva abaixo do mínimo; "
                    "n = operações neste grupo.")
        else:
            text = "Hachura diagonal = amostra positiva abaixo do mínimo; n = operações neste grupo."
        panel.set_notice(text, kind="partial", theme=self.theme)

    def _temporal_context(self, view):
        """Descreve a referência recebida do enriquecimento, sem inferir entradas."""
        refs = set(view["time_ref"].dropna())
        if refs == {"entrada"}:
            return "Entrada"
        if refs == {"saida"}:
            return "Saída (entrada indisponível)"
        if refs == {"entrada", "saida"}:
            return "Referência mista: entrada quando disponível; saída nas demais"
        return "Referência temporal indisponível"

    def _overview_matrix(self, parent, view):
        """Matriz com atenção pelo sinal, sem atribuir confiança à cor do resultado."""
        table = self._crosstab(view, "time_bucket", "day_of_week")
        values = table["value"].reindex(columns=range(7))
        counts = table["n"].reindex(index=values.index, columns=range(7), fill_value=0).fillna(0)
        small = table["small"].reindex(index=values.index, columns=range(7), fill_value=False).fillna(False)
        if not hasattr(self, "overview_heat_table"):
            self.overview_heat_table = ContentPanel(
                parent, "Expectativa por faixa horária × dia", subtitle="Moeda da conta", padding=SPACING["xs"])
            self.overview_heat_table.grid(row=0, column=0, sticky="nsew", padx=SPACING["xs"], pady=SPACING["xs"])
            self.overview_heat_notice = Notice(self.overview_heat_table.body, "", kind="neutral", theme=self.theme)
            self.overview_heat_notice.badge.set_status("Contexto", kind="neutral")
            self.overview_heat_notice.pack(fill=tk.X, pady=(0, SPACING["xs"]))
            self.overview_heat_matrix = MatrixTable(
                self.overview_heat_table.body, ("Faixa", *(WEEKDAYS[day] for day in range(7))),
                (80, 150, 150, 150, 150, 150, 150, 150), theme=self.theme,
                height=9, row_height=max(32, self.theme.font.metrics("linespace") + 12))
            self.overview_heat_matrix.pack(fill=tk.BOTH, expand=True)
            parent.rowconfigure(0, weight=1)
            parent.columnconfigure(0, weight=1)
        excluded = len(self.view) - len(view)
        population = f"{self._temporal_context(view)} · {fmt_int(len(view))} operações na matriz."
        if excluded:
            population += f" {fmt_int(excluded)} operações sem entrada ficam fora desta matriz; KPIs incluem o recorte inteiro."
        self.overview_heat_notice.message.configure(
            text=population + " Azul suave: resultado positivo; tom discreto: negativo; neutro: zero/sem operações. "
                 "— = sem operações (n=0); n=operações; * = pouca amostra. Cor indica sinal, não validação.")
        self.overview_heat_notice.message.bind(
            "<Configure>", lambda event: self.overview_heat_notice.message.configure(wraplength=max(180, event.width)))
        rows = []
        for hour in values.index:
            row = []
            for day in range(7):
                value = values.loc[hour, day]
                count = int(counts.loc[hour, day])
                marker = "*" if count > 0 and bool(small.loc[hour, day]) else ""
                amount = ("+" if pd.notna(value) and value > 0 else "") + charts.money(value)
                background, foreground = result_attention_colors(value if count else None, self.theme.colors)
                text = f"{amount} · n={count}{marker}" if count else "—"
                row.append((text, background, foreground))
            rows.append(row)
        self.overview_heat_matrix.set_rows(values.index, rows)
        for i, cells in enumerate(self.overview_heat_matrix.cells):
            for j, cell in enumerate(cells):
                cell.configure(font=self.theme.font_small if not counts.iloc[i, j] or pd.isna(values.iloc[i, j]) else self.theme.font)

    def _overview(self):
        from mt5_extracao.trade_analytics import aggregate
        view, tab = self.view, "Visão geral"
        heat_frame = self._overview_view("heat", "Horários")
        curve_frame = self._overview_view("curves", "Curvas")
        news_frame = self._overview_view("news", "Calendário")
        timed = view[view["time_ref"] == "entrada"]
        self._overview_matrix(heat_frame, timed if not timed.empty else view)
        by = "symbol" if self.lens.get() == LENS_ROBOT else "robot_key"
        chart = self._chart(tab, "ov_curves", 0, 0, colspan=2, height=3.2, parent=curve_frame)
        chart.clear()
        charts.overlay(chart, chart.figure.add_subplot(111), aggregate.equity_curves(view, by),
                       "Resultado acumulado por " + ("ativo" if by == "symbol" else "robô"))
        self._sample_notice(chart, None)
        chart.figure.tight_layout()
        chart.draw()
        chart = self._chart(tab, "ov_news", 0, 0, colspan=2, height=3.2, parent=news_frame)
        chart.clear()
        news = view.assign(impact=pd.to_numeric(view["event_impact"], errors="coerce"))
        table = aggregate.by_dimension(news, ["impact"], self._config(), ci=False)
        ax = chart.figure.add_subplot(111)
        if table.empty:
            chart.message("Sem calendário econômico para estas operações. Importe com `mt5x trades calendar import` "
                          "(CSV) ou exporte do MT5 (`mt5x trades calendar export`).")
        else:
            labels = [f"{IMPACTS.get(int(i), i)}\n{fmt_num(w * 100, 0, '%')} acerto"
                      for i, w in zip(table["impact"], table["win_rate"])]
            charts.bars(chart, ax, labels, table["expectancy"], table["n"], table["small"],
                        "Notícias do calendário × expectativa por operação")
            self._sample_notice(chart, table["small"])
            chart.figure.tight_layout()
            chart.draw()

    def _time(self):
        from mt5_extracao.trade_analytics import aggregate
        view, tab, cfg = self.view, "Tempo e calendário", self._config()
        days = aggregate.by_dimension(view, ["day_of_week"], cfg, ci=False)
        chart = self._chart(tab, "t_week", 0, 0, height=4.0,
                            parent=self._analysis_view(tab, "week", "Dia da semana"))
        chart.clear()
        charts.bars(chart, chart.figure.add_subplot(111), [WEEKDAYS.get(int(d), d) for d in days["day_of_week"]],
                    days["expectancy"], days["n"], days["small"], "Dia da semana: expectativa por operação")
        self._sample_notice(chart, days["small"])
        chart.figure.tight_layout()
        chart.draw()
        month = aggregate.by_dimension(view, ["day_of_month"], cfg, ci=False)
        chart = self._chart(tab, "t_month", 0, 0, height=4.0,
                            parent=self._analysis_view(tab, "month", "Dia do mês"))
        chart.clear()
        charts.bars(chart, chart.figure.add_subplot(111), [str(int(d)) for d in month["day_of_month"]],
                    month["expectancy"], month["n"], month["small"], "Dia do mês: expectativa por operação")
        self._sample_notice(chart, month["small"])
        chart.figure.tight_layout()
        chart.draw()
        hours = aggregate.by_dimension(view, ["time_bucket"], cfg, ci=False)
        chart = self._chart(tab, "t_hours", 0, 0, height=4.0,
                            parent=self._analysis_view(tab, "hours", "Expectativa e acerto"))
        chart.clear()
        ax = chart.figure.add_subplot(111)
        charts.bars(chart, ax, list(hours["time_bucket"]), hours["expectancy"], hours["n"], hours["small"],
                    "Faixa horária: expectativa (barras) e acerto % (linha)")
        self._sample_notice(chart, hours["small"])
        twin = ax.twinx()
        twin.plot(range(len(hours)), hours["win_rate"] * 100, color=self.theme.colors["ink2"], linewidth=1.2,
                  marker="o", markersize=3)
        twin.set_ylabel("Acerto (%)", color=self.theme.colors["muted"], fontsize=TYPOGRAPHY["chart_label"])
        twin.set_ylim(0, 100)
        twin.tick_params(colors=self.theme.colors["muted"], labelsize=TYPOGRAPHY["chart_tick"], length=0)
        for side in ("top", "right", "left", "bottom"):
            twin.spines[side].set_visible(False)
        chart.figure.tight_layout()
        chart.draw()
        chart = self._chart(tab, "t_winloss", 0, 0, height=4.0,
                            parent=self._analysis_view(tab, "wins_losses", "Ganhos, perdas e zeradas"))
        chart.clear()
        ax = chart.figure.add_subplot(111)
        charts.style_axis(ax, self.theme, "Resultado das operações por faixa horária")
        counts = aggregate.outcome_counts(view, "time_bucket")
        x = np.arange(len(counts))
        c = self.theme.colors
        bottom = np.zeros(len(counts))
        for key, label, color, alpha, hatch in (
                ("wins", "ganhos (>0)", c["accent"], .85, None),
                ("losses", "perdas (<0)", c["ink2"], .35, ".."),
                ("flat", "zeradas (=0)", c["border"], 1.0, None),
                ("unknown", "indisponíveis", c["surface"], 1.0, "//")):
            if counts[key].any():
                ax.bar(x, counts[key], bottom=bottom, color=color, alpha=alpha, width=.65,
                       label=label, linewidth=.5 if key == "unknown" else 0,
                       edgecolor=c["muted"], hatch=hatch)
            bottom = bottom + counts[key].to_numpy()
        totals = counts["n"]
        for index, total in enumerate(totals):
            crowded = len(counts) > 14
            ax.text(index, total, f"n={int(total)}", ha="center", va="bottom", rotation=90 if crowded else 0,
                    fontsize=TYPOGRAPHY["chart_tick"], color=c["ink"])
        ax.set_ylabel("Operações por faixa horária", color=c["muted"], fontsize=TYPOGRAPHY["chart_label"])
        ax.margins(y=0.2)
        ax.set_xticks(x)
        crowded = len(x) > 12
        ax.set_xticklabels(["Faixa indisponível" if pd.isna(value) else str(value) for value in counts["time_bucket"]],
                           fontsize=TYPOGRAPHY["chart_tick"], rotation=45 if crowded else 0,
                           ha="right" if crowded else "center")
        legend = ax.legend(fontsize=TYPOGRAPHY["chart_tick"], frameon=False, loc="upper left",
                           bbox_to_anchor=(1.02, 1.0), borderaxespad=0)
        for text in legend.get_texts():
            text.set_color(c["ink2"])
        chart.figure.tight_layout()
        chart.draw()
        self._sample_notice(chart, None)
        context = self._temporal_context(view) + f" · {fmt_int(len(view))} operações no recorte."
        for key in ("t_week", "t_month", "t_hours", "t_winloss"):
            panel = self._chart_panels[key]
            sample = panel.notice.message.cget("text") if panel.notice is not None else ""
            panel.set_notice(context + (" " + sample if sample else ""), kind="info", theme=self.theme)
            panel.notice.message.bind(
                "<Configure>", lambda event, message=panel.notice.message:
                message.configure(wraplength=max(180, event.width)))

    def _micro(self):
        from mt5_extracao.trade_analytics import aggregate
        view, tab, cfg = self.view, "Microestrutura", self._config()
        complete = view[view["time_ref"] == "entrada"]
        if complete.empty or pd.to_numeric(complete["atr_pct"], errors="coerce").isna().all():
            missing = ("Sem horário de entrada ou sem barras na base para estas operações: a microestrutura precisa "
                       "dos dois (extraia o ativo no timeframe do backtest).")
            for key, view_key, title in (("m_scatter", "scatter", "Volatilidade × resultado"),
                                         ("m_vol", "volatility", "Faixa de volatilidade"),
                                         ("m_volume", "volume", "Volume na entrada"),
                                         ("m_heat", "heat", "Volatilidade × horário")):
                chart = self._chart(tab, key, 0, 0, height=4.0,
                                    parent=self._analysis_view(tab, view_key, title))
                chart.message(missing)
            return
        chart = self._chart(tab, "m_scatter", 0, 0, height=4.0,
                            parent=self._analysis_view(tab, "scatter", "Volatilidade × resultado"))
        chart.clear()
        charts.scatter_win_loss(chart, chart.figure.add_subplot(111), pd.to_numeric(complete["atr_pct"]) * 100,
                                complete["pnl"], "Volatilidade na entrada (ATR %) × resultado", "ATR % do preço")
        chart.figure.tight_layout()
        chart.draw()
        order = {"baixa": 0, "média": 1, "alta": 2}
        vol = aggregate.by_dimension(complete, ["volatility_bucket"], cfg, ci=False)
        vol = vol.assign(o=vol["volatility_bucket"].map(order)).sort_values("o")
        chart = self._chart(tab, "m_vol", 0, 0, height=4.0,
                            parent=self._analysis_view(tab, "volatility", "Faixa de volatilidade"))
        chart.clear()
        charts.bars(chart, chart.figure.add_subplot(111),
                    [f"{b}\n{fmt_num(w * 100, 0, '%')} acerto" for b, w in zip(vol["volatility_bucket"], vol["win_rate"])],
                    vol["expectancy"], vol["n"], vol["small"], "Faixa de volatilidade (percentil nas 250 barras anteriores)")
        self._sample_notice(chart, vol["small"])
        chart.figure.tight_layout()
        chart.draw()
        ranked = complete.assign(volume_band=pd.cut(pd.to_numeric(complete["volume_rank"], errors="coerce"),
                                                    [0, 0.25, 0.5, 0.75, 1.0], include_lowest=True,
                                                    labels=["baixo", "médio-baixo", "médio-alto", "alto"]))
        volume = aggregate.by_dimension(ranked.dropna(subset=["volume_band"]).assign(
            volume_band=lambda d: d["volume_band"].astype(str)), ["volume_band"], cfg, ci=False)
        order = {"baixo": 0, "médio-baixo": 1, "médio-alto": 2, "alto": 3}
        volume = volume.assign(o=volume["volume_band"].map(order)).sort_values("o")
        chart = self._chart(tab, "m_volume", 0, 0, height=4.0,
                            parent=self._analysis_view(tab, "volume", "Volume na entrada"))
        chart.clear()
        charts.bars(chart, chart.figure.add_subplot(111),
                    [f"{b}\n{fmt_num(w * 100, 0, '%')} acerto" for b, w in zip(volume["volume_band"], volume["win_rate"])],
                    volume["expectancy"], volume["n"], volume["small"], "Volume na entrada (quartil nas 20 barras anteriores)")
        self._sample_notice(chart, volume["small"])
        chart.figure.tight_layout()
        chart.draw()
        chart = self._chart(tab, "m_heat", 0, 0, height=4.0,
                            parent=self._analysis_view(tab, "heat", "Volatilidade × horário"))
        chart.clear()
        self._heat(chart, chart.figure.add_subplot(111), complete, "volatility_bucket", "time_bucket",
                   "Volatilidade × faixa horária", row_order=("baixa", "média", "alta"))
        chart.figure.tight_layout()
        chart.draw()

    def _compare(self):
        from mt5_extracao.trade_analytics import aggregate
        tab, view, cfg = "Comparativo", self.view, self._config()
        table_view = self._analysis_view(tab, "items", "Ativos e timeframes")
        if not hasattr(self, "compare_tree"):
            table_panel = TablePanel(
                table_view, "Comparação por ativo e timeframe",
                ("Item", "Timeframe", "Operações", "Acerto", "Fator de lucro", "Expectativa", "Intervalo 90%",
                 "Queda máxima"), (170, 80, 80, 70, 100, 100, 170, 100),
                ("w", "w", "e", "e", "e", "e", "w", "e"), height=8, padding=SPACING["xs"])
            table_panel.grid(row=0, column=0, columnspan=2, sticky="nsew", padx=SPACING["xs"], pady=SPACING["xs"])
            table_view.rowconfigure(0, weight=1)
            table_view.columnconfigure(0, weight=1)
            self.compare_panel = table_panel
            self.compare_tree = table_panel.tree
            self.compare_tree.tag_configure("small", foreground=self.theme.colors["muted"])
        table = aggregate.robot_view(view, cfg) if self.lens.get() == LENS_ROBOT else aggregate.asset_view(view, cfg)
        key = "symbol" if self.lens.get() == LENS_ROBOT else "robot_key"
        tree = self.compare_tree
        tree.delete(*tree.get_children())
        for _, r in table.iterrows():
            interval = f"{charts.money(r['ci_low'])} a {charts.money(r['ci_high'])}" if pd.notna(r["ci_low"]) else "—"
            tree.insert("", tk.END, values=(r[key], r["timeframe"], fmt_int(r["n"]), fmt_num(r["win_rate"] * 100, 1, "%"),
                                            fmt_num(r["profit_factor"], 2), charts.money(r["expectancy"]), interval,
                                            fmt_num(r["max_drawdown_pct"] * 100, 1, "%")),
                        tags=("small",) if r["small"] else ())
        chart = self._chart(tab, "c_corr", 0, 0, height=4.0,
                            parent=self._analysis_view(tab, "correlation", "Correlação"))
        if self.lens.get() == LENS_ASSET:
            corr = aggregate.daily_correlation(view, "robot_key")
            if corr.shape[0] >= 2:
                chart.clear()
                n = pd.DataFrame(np.ones(corr.shape, dtype=int), index=corr.index, columns=corr.columns)
                charts.heatmap(chart, chart.figure.add_subplot(111), corr, n * 0, corr.isna(),
                               "Correlação dos resultados diários entre robôs (positiva = perdas que se somam)",
                               metric_name="Correlação", show_counts=False)
                self._chart_panels["c_corr"].set_notice(
                    "Células cinza pontilhadas = pares sem dados em comum; escala fixa de −1 a 1.",
                    kind="partial", theme=self.theme)
                chart.figure.tight_layout()
                chart.draw()
                return
            chart.message("A correlação entre robôs precisa de pelo menos dois robôs com 20 dias em comum neste ativo.")
        else:
            chart.message("Na lente por robô, a tabela acima compara o robô em cada ativo e timeframe. A correlação "
                          "entre robôs aparece na lente por ativo.")

    # --- Execução (T16.6 a T16.8) --------------------------------------------------------

    def _build_execution_tab(self):
        tab = self.tab_frames["Execução"]
        bar = ttk.Frame(tab, style="Surface.TFrame")
        bar.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 6))
        ttk.Label(bar, text="Run:", style="Surface.TLabel").pack(side=tk.LEFT)
        self.run_var = tk.StringVar()
        self.run_combo = ttk.Combobox(bar, textvariable=self.run_var, state="readonly", width=38)
        self.run_combo.pack(side=tk.LEFT, padx=(6, 12))
        self.mode_vars = {}
        self.modes_box = ttk.Frame(tab, style="Surface.TFrame")      # linha própria: quebra quando não cabe
        self.modes_box.grid(row=1, column=0, columnspan=2, sticky="w", pady=(0, 4))
        self.compare_button = ttk.Button(bar, text="Comparar modos", style="Accent.TButton", command=self.compare_modes)
        self.compare_button.pack(side=tk.RIGHT)
        Tooltip(self.compare_button, "Re-simula as MESMAS entradas do run em cada modo marcado, sobre as barras da "
                                     "base, com os custos do backtester. Martingale só aparece para robô que o suporta, "
                                     "com as travas dele.", self.theme)
        self.exec_note = ttk.Label(tab, text="Escolha um run e os modos e clique em \"Comparar modos\".",
                                   style="Muted.TLabel", wraplength=1000, justify=tk.LEFT)
        self.exec_note.grid(row=2, column=0, columnspan=2, sticky="w")
        self.mode_panel = TablePanel(
            tab, "Resultados por modo",
            ("Modo", "Operações", "Acerto", "Fator de lucro", "Payoff", "Expectativa", "Queda máxima",
             "Melhor faixa", "Observação"), (210, 80, 70, 100, 70, 100, 100, 100, 260),
            ("w", "e", "e", "e", "e", "e", "e", "w", "w"), height=6, padding=SPACING["xs"])
        self.mode_panel.grid(row=3, column=0, columnspan=2, sticky="nsew", pady=(SPACING["sm"], 0))
        self.mode_tree = self.mode_panel.tree
        self.mode_tree.tag_configure("study", foreground=self.theme.colors["warn"])
        tab.columnconfigure(0, weight=1)
        tab.columnconfigure(1, weight=1)

    def _fill_runs(self):
        view = self.view
        if view is None or view.empty:
            return
        runs = view.groupby("run_id").agg(symbol=("symbol", "first"), timeframe=("timeframe", "first"),
                                          robot=("robot_key", "first"), n=("pnl", "size"),
                                          complete=("completeness", lambda c: (c == "completo").all()))
        self._run_labels = {f"{r.robot} · {r.symbol} {r.timeframe} · {r.n} op." + ("" if r.complete else " (parcial)"):
                            run_id for run_id, r in runs.iterrows()}
        self.run_combo.configure(values=list(self._run_labels))
        if self.run_var.get() not in self._run_labels and self._run_labels:
            self.run_var.set(next(iter(self._run_labels)))

    def _fill_modes(self, robot=None):
        from mt5_extracao.trade_analytics.execution import modes
        for w in self.modes_box.winfo_children():
            w.destroy()
        # as variáveis do Tk são reaproveitadas (nunca descartadas): descartá-las deixa lixo do Tk que o coletor pode
        # destruir numa thread de segundo plano, e o Tk só aceita a thread principal
        self._all_mode_vars = getattr(self, "_all_mode_vars", {})
        self.mode_vars = {}
        per_row = 5
        for k, (key, mode) in enumerate(modes.available(robot).items()):
            if key not in self._all_mode_vars:      # cria só uma vez (setdefault criaria e descartaria a cada chamada)
                self._all_mode_vars[key] = tk.BooleanVar(
                    value=key in ("ponto_fixo", "trailing_atr", "gradiente_dinamico"))
            var = self._all_mode_vars[key]
            self.mode_vars[key] = var
            w = ttk.Checkbutton(self.modes_box, text=mode.name.split(" (")[0], variable=var)
            w.grid(row=k // per_row, column=k % per_row, sticky="w", padx=(0, 10))
            Tooltip(w, mode.help, self.theme)

    def _execution_refresh(self):
        self._fill_runs()
        if not self.mode_vars:
            self._fill_modes()
        if self.mode_metrics:
            self._show_modes()

    def compare_modes(self):
        db, config = self._db(), self._config()
        run_id = getattr(self, "_run_labels", {}).get(self.run_var.get())
        wanted = [k for k, v in self.mode_vars.items() if v.get()]
        if db is None or not run_id or not wanted:
            self.exec_note.config(text="Escolha um run na lista." if not run_id else "Marque ao menos um modo.")
            return
        if "ponto_fixo" not in wanted:
            wanted.insert(0, "ponto_fixo")
        self.compare_button.state(["disabled"])
        self.exec_note.config(text="Re-simulando... (pode levar alguns segundos por modo)")
        base = self.view[self.view["run_id"] == run_id]
        lab = base.set_index("seq")
        robot_key = str(base["robot_key"].iloc[0])

        def work():
            from mt5_extracao.robots import catalog
            from mt5_extracao.trade_analytics.execution import runner
            robot = None
            try:
                robot = catalog.get_robot(catalog.load_catalog(config, db), robot_key)
            except Exception:
                robot = None
            table = runner.compare(db, config, run_id, wanted, robot=robot)
            frames = {}
            entries = base[["seq", "entry_time"]].dropna().set_index("entry_time")["seq"]
            for key, item in table.items():
                if item["result"] is not None:
                    rows = [{"run_id": run_id, "seq": entries.get(t.entry_time), "pnl": t.pnl,
                             "exit_time": t.exit_time, "entry_time": t.entry_time} for t in item["result"].trades]
                else:
                    seq = item["sequence"]["trades"]
                    rows = [{"run_id": run_id, "seq": entries.get(t.entry_time), "pnl": t.pnl,
                             "exit_time": t.exit_time, "entry_time": t.entry_time}
                            for t in seq[~seq["skipped"]].itertuples()]
                frames[key] = pd.DataFrame(rows).dropna(subset=["seq"])
            return robot, table, frames

        def done(result, error):
            self.compare_button.state(["!disabled"])
            if error is not None:
                self.exec_note.config(text=f"Não foi possível re-simular: {error}")
                return
            robot, table, frames = result
            self._fill_modes(robot)
            self.mode_results, self.mode_metrics = frames, table
            self._mode_lab = lab
            self._show_modes()
        self.ui.run_bg(work, done)

    def _show_modes(self):
        tree = self.mode_tree
        tree.delete(*tree.get_children())
        lab = getattr(self, "_mode_lab", None)
        buckets = lab["time_bucket"] if lab is not None and "time_bucket" in lab else None
        curves, heat = {}, {}
        for key, item in self.mode_metrics.items():
            m, frame = item["metrics"], self.mode_results.get(key)
            best = "—"
            if frame is not None and not frame.empty and buckets is not None:
                rows_key = frame["seq"].map(buckets)
                if rows_key.nunique() > 16:            # muitas faixas: agrupa por hora para o heatmap caber
                    rows_key = rows_key.str[:2] + "h"
                heat[SHORT_MODES.get(key, key)] = frame.assign(b=rows_key).groupby("b")["pnl"].mean()
                by = frame.assign(b=frame["seq"].map(buckets)).groupby("b")["pnl"]
                counts = by.size()
                ok = by.mean()[counts >= 5]
                best = ok.idxmax() if not ok.empty else "—"
                ordered = frame.sort_values("exit_time")
                curves[item["name"].split(" (")[0]] = pd.Series(ordered["pnl"].cumsum().values,
                                                                index=pd.to_datetime(ordered["exit_time"]))
            note = []
            if item.get("study_only"):
                note.append("só estudo (a bateria reprova para conta real)")
                trig = (item.get("sequence") or {}).get("triggers", {})
                fired = ", ".join(f"{k}: {v}" for k, v in trig.items() if v)
                if fired:
                    note.append("travas: " + fired)
            if item.get("adds"):
                note.append(f"{item['adds']} entradas adicionais; exposição máx. {fmt_num(item['max_volume'], 2)}")
            tree.insert("", tk.END, values=(item["name"], fmt_int(m["n"]), fmt_num(m["win_rate"] * 100, 1, "%"),
                                            fmt_num(m["profit_factor"], 2), fmt_num(m["payoff"], 2),
                                            charts.money(m["expectancy"]), fmt_num(m["max_drawdown_pct"] * 100, 1, "%"),
                                            best, "; ".join(note)),
                        tags=("study",) if item.get("study_only") else ())
        chart = self._chart("Execução", "e_curves", 4, 0, height=2.8)
        chart.clear()
        charts.overlay(chart, chart.figure.add_subplot(111), curves, "Mesmas entradas, modos diferentes: resultado acumulado")
        chart.draw()
        chart = self._chart("Execução", "e_heat", 4, 1, height=2.8)
        chart.clear()
        if heat:
            value = pd.DataFrame(heat).sort_index()
            n = value.notna().astype(int) * 0
            charts.heatmap(chart, chart.figure.add_subplot(111), value, n, value.isna(),
                           "Expectativa: faixa horária × modo", show_counts=False)
            chart.figure.tight_layout()
        chart.draw()
        base = self.mode_metrics.get("ponto_fixo", {}).get("metrics")
        if base:
            self.exec_note.config(text=f"Base: {fmt_int(base['n'])} operações. Re-simulação sobre barras: a ordem "
                                       "dentro da barra é suposta (stop antes do alvo). Para a ordem exata, valide no "
                                       "Strategy Tester com ticks reais.")

    # --- Insights e simulador (T16.9 e T16.10) ---------------------------------------------

    def _build_insights_tab(self):
        tab = self.tab_frames["Insights"]
        top = ttk.Frame(tab, style="Surface.TFrame")
        top.grid(row=0, column=0, columnspan=2, sticky="ew")
        self.find_button = ttk.Button(top, text="Procurar insights", style="Accent.TButton", command=self.find_insights)
        self.find_button.pack(side=tk.LEFT)
        Tooltip(self.find_button, "Procura dias, horários, notícias e faixas de volatilidade em que o robô vai pior, "
                                  "só fora da reserva final; corrige pelo número de combinações testadas e só chama "
                                  "de \"validado\" o que se confirma na reserva final.", self.theme)
        self.insight_note = ttk.Label(top, text="", style="Muted.TLabel", wraplength=800, justify=tk.LEFT)
        self.insight_note.pack(side=tk.LEFT, padx=(12, 0))
        self.insight_panel = TablePanel(tab, "Insights encontrados", ("Usar", "Estado", "Insight"),
                                        (50, 170, 900), ("center", "w", "w"), height=4,
                                        selectmode="extended", padding=SPACING["xs"])
        self.insight_panel.grid(row=1, column=0, columnspan=2, sticky="nsew",
                                pady=(SPACING["sm"], SPACING["sm"]))
        self.insight_tree = self.insight_panel.tree
        self.insight_tree.tag_configure("validated", foreground=self.theme.colors["good"])
        self.insight_tree.bind("<Button-1>", self._insight_clicked, add="+")
        self.use_insight = set()
        manual = ttk.LabelFrame(tab, text="Regras manuais")
        manual.grid(row=2, column=0, sticky="nsew", padx=(0, 6))
        self.day_vars = {d: tk.BooleanVar(value=False) for d in range(5)}
        row = ttk.Frame(manual)
        row.pack(anchor="w")
        ttk.Label(row, text="Não operar:").pack(side=tk.LEFT)
        for d, var in self.day_vars.items():
            ttk.Checkbutton(row, text=WEEKDAYS[d], variable=var).pack(side=tk.LEFT, padx=(4, 0))
        row = ttk.Frame(manual)
        row.pack(anchor="w", pady=(4, 0))
        self.window_on = tk.BooleanVar(value=False)
        ttk.Checkbutton(row, text="Operar só entre", variable=self.window_on).pack(side=tk.LEFT)
        self.window_from, self.window_to = tk.StringVar(value="10:00"), tk.StringVar(value="12:00")
        ttk.Entry(row, textvariable=self.window_from, width=6).pack(side=tk.LEFT, padx=4)
        ttk.Label(row, text="e").pack(side=tk.LEFT)
        ttk.Entry(row, textvariable=self.window_to, width=6).pack(side=tk.LEFT, padx=4)
        self.no_news = tk.BooleanVar(value=False)
        ttk.Checkbutton(manual, text="Não operar com notícia de impacto alto na janela", variable=self.no_news).pack(
            anchor="w", pady=(4, 0))
        self.no_high_vol = tk.BooleanVar(value=False)
        ttk.Checkbutton(manual, text="Não operar com volatilidade alta", variable=self.no_high_vol).pack(anchor="w")
        actions = ttk.Frame(manual)
        actions.pack(anchor="w", pady=(8, 0))
        self.simulate_button = ttk.Button(actions, text="Simular", style="Accent.TButton", command=self.simulate)
        self.simulate_button.pack(side=tk.LEFT)
        self.export_button = ttk.Button(actions, text="Exportar proposta", command=self.export_proposal)
        self.export_button.pack(side=tk.LEFT, padx=(8, 0))
        Tooltip(self.export_button, "Grava um arquivo com as regras e os números (procura, provas e reserva final). "
                                    "É uma proposta: para virar preset, passa por mt5x validate, bateria e paridade.",
                self.theme)
        self.sim_text = ttk.Label(tab, text="", style="Surface.TLabel", wraplength=520, justify=tk.LEFT)
        self.sim_text.grid(row=3, column=0, sticky="nw", pady=(6, 0))
        tab.columnconfigure(0, weight=1)
        tab.columnconfigure(1, weight=1)

    def _insights_refresh(self):
        pass

    def find_insights(self):
        view, config = self.view, self._config()
        if view is None or view.empty:
            return
        self.find_button.state(["disabled"])
        self.insight_note.config(text="Procurando... (cada combinação testada entra na conta)")
        modes = dict(self.mode_results)

        def work():
            from mt5_extracao.trade_analytics import insights
            found = insights.discover(view, config)
            base = modes.pop("ponto_fixo", None)
            if modes and base is not None:
                found += insights.mode_insights(view[view["run_id"].isin(base["run_id"])], modes, config)
            return found

        def done(found, error):
            self.find_button.state(["!disabled"])
            if error is not None:
                self.insight_note.config(text=f"Erro na procura: {error}")
                return
            from mt5_extracao.trade_analytics import insights
            self.found = found
            self.use_insight = set()
            tree = self.insight_tree
            tree.delete(*tree.get_children())
            for k, i in enumerate(found):
                tree.insert("", tk.END, iid=str(k), values=(self._mark(False) if i.rule else "", i.state, i.text),
                            tags=("validated",) if i.state == insights.VALIDATED else ())
            n_ok = sum(i.state == insights.VALIDATED for i in found)
            tested = found[0].tested if found else 0
            self.insight_note.config(text=f"{len(found)} candidato(s), {n_ok} validado(s) fora da amostra; "
                                          f"{tested} combinação(ões) testadas. Exploratório = pista, não conclusão.")
        self.ui.run_bg(work, done)

    def _mark(self, on):
        return self.theme.glyphs("☑" if on else "☐")

    def _insight_clicked(self, event):
        tree = self.insight_tree
        if tree.identify_column(event.x) != "#1":
            return
        iid = tree.identify_row(event.y)
        if not iid or not self.found[int(iid)].rule:
            return
        self.use_insight ^= {iid}
        tree.set(iid, "Usar", self._mark(iid in self.use_insight))
        return "break"

    def selected_rules(self):
        chosen = [self.found[int(k)].rule for k in sorted(self.use_insight, key=int)]
        days = [d for d, v in self.day_vars.items() if v.get()]
        if days:
            chosen.append({"tipo": "excluir_dia", "dias": days})
        if self.window_on.get():
            chosen.append({"tipo": "janela_horaria", "inicio": self.window_from.get().strip(),
                           "fim": self.window_to.get().strip()})
        if self.no_news.get():
            chosen.append({"tipo": "excluir_noticia", "impacto_min": 3})
        if self.no_high_vol.get():
            chosen.append({"tipo": "excluir_volatilidade", "faixas": ["alta"]})
        return chosen

    def simulate(self):
        view, config, chosen = self.view, self._config(), self.selected_rules()
        if view is None or view.empty:
            return
        if not chosen:
            self.sim_text.config(text="Marque uma regra (insight ou manual) para simular.")
            return
        needs = {r["modo"] for r in chosen if r.get("tipo") == "trocar_modo"}
        missing = needs - set(self.mode_results)
        if missing:
            self.sim_text.config(text=f"Compare antes os modos {', '.join(sorted(missing))} na guia Execução.")
            return
        lab = view[view["run_id"].isin(next(iter(self.mode_results.values()))["run_id"])] if needs else view
        self.simulate_button.state(["disabled"])
        modes = dict(self.mode_results)

        def work():
            from mt5_extracao.trade_analytics import rules
            return rules.evaluate(lab, chosen, config, modes), rules.apply(lab, chosen, modes), lab

        def done(result, error):
            self.simulate_button.state(["!disabled"])
            if error is not None:
                self.sim_text.config(text=f"Não foi possível simular: {error}")
                return
            self.evaluation, after, lab = result
            self._chosen = chosen
            ev = self.evaluation
            line = lambda part: (f"expectativa {charts.money(ev[part]['before']['expectancy'])} → "  # noqa: E731
                                 f"{charts.money(ev[part]['after']['expectancy'])}; fator de lucro "
                                 f"{fmt_num(ev[part]['before']['profit_factor'], 2)} → "
                                 f"{fmt_num(ev[part]['after']['profit_factor'], 2)}; queda máx. "
                                 f"{fmt_num(ev[part]['before']['max_drawdown_pct'] * 100, 1, '%')} → "
                                 f"{fmt_num(ev[part]['after']['max_drawdown_pct'] * 100, 1, '%')}")
            text = ["Regras: " + "; ".join(ev["rules"]),
                    f"Procura: {line('search')}",
                    f"Provas do walk-forward: melhorou em {ev['folds_improved']} de {len(ev['folds'])}",
                    f"Reserva final (não usada em nada antes): {line('holdout')}",
                    f"{ev['removed']} operação(ões) removida(s)."]
            if ev["weak"] or ev["note"]:
                text.append(ev["note"])
            if ev["holdout"]["after"]["expectancy"] <= ev["holdout"]["before"]["expectancy"]:
                text.append("Atenção: na reserva final as regras não melhoraram o resultado.")
            self.sim_text.config(text="\n".join(text))
            chart = self._chart("Insights", "i_curves", 2, 1, rowspan=2, height=3.0)
            chart.clear()
            curves = {}
            for name, frame in (("antes", lab), ("depois", after)):
                ordered = frame.sort_values("exit_time")
                curves[name] = pd.Series(ordered["pnl"].cumsum().values, index=pd.to_datetime(ordered["exit_time"]))
            ax = chart.figure.add_subplot(111)
            charts.overlay(chart, ax, curves, "Curva antes × depois das regras (a reserva final começa na linha)")
            from mt5_extracao.trade_analytics import rules as rules_mod
            plan = rules_mod.split(lab, config)
            if plan.holdout.any():
                start = pd.to_datetime(lab["entry_time"].fillna(lab["exit_time"]))[plan.holdout].min()
                ax.axvline(start, color=self.theme.colors["muted"], linestyle="--", linewidth=1)
            chart.draw()
        self.ui.run_bg(work, done)

    def export_proposal(self):
        if self.evaluation is None:
            self.sim_text.config(text="Simule as regras antes de exportar.")
            return
        from mt5_extracao.trade_analytics import export
        scope = ("robot:" if self.lens.get() == LENS_ROBOT else "symbol:") + self.selected.get()
        tested = self.found[0].tested if self.found else 0
        try:
            path = export.proposal(scope, self._chosen, self.evaluation, tested)
        except OSError as e:
            self.sim_text.config(text=f"Não foi possível gravar a proposta: {e}")
            return
        self.ui.log(f"Laboratório: proposta exportada em {path}")
        self.sim_text.config(text=self.sim_text.cget("text") + f"\n\nProposta gravada em {path}.")

    # --- ações -------------------------------------------------------------------------

    def import_runs(self):
        db = self._db()
        if db is None:
            return
        from mt5_extracao.trade_analytics import importers

        def done(result, error):
            if error is not None:
                self.set_note(f"Importação falhou: {error}")
                return
            self.ui.log(f"Laboratório: {result['runs']} backtest(s) importado(s), {result['trades']} operação(ões).")
            self.reload()
        self.ui.run_bg(lambda: importers.import_existing_runs(db), done)
