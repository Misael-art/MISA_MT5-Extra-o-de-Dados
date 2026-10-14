"""
Aba "Laboratório" (Fase 16, T16.11): onde os robôs ganham e perdem, pelas duas lentes.

- Por robô: o robô escolhido em todos os ativos e timeframes em que foi testado.
- Por ativo: todos os robôs testados no ativo escolhido.

Guias desta entrega: Visão geral, Tempo e calendário, Microestrutura e Comparativo. Execução e Insights dependem da
re-simulação (T16.6 a T16.10) e aparecem desabilitadas, com o motivo. Os números vêm de mt5_extracao/trade_analytics
(sem Tkinter); a leitura roda em segundo plano e só a guia visível é desenhada.
"""
import logging
import tkinter as tk
from datetime import timedelta
from tkinter import ttk

import numpy as np
import pandas as pd

from mt5_extracao.ui import charts
from mt5_extracao.ui.theme import Tooltip, card, fmt_int, fmt_num, scrolled_tree

log = logging.getLogger(__name__)

LENS_ROBOT, LENS_ASSET = "robot", "asset"
PERIODS = (("Tudo", None), ("Último ano", 365), ("Últimos 6 meses", 182), ("Últimos 3 meses", 91))
TABS = ("Visão geral", "Tempo e calendário", "Microestrutura", "Comparativo", "Execução", "Insights")
DISABLED_TABS = {"Execução": "chega com a re-simulação de execução (T16.6 a T16.8)",
                 "Insights": "chega com o simulador de regras e os insights validados (T16.9 e T16.10)"}
WEEKDAYS = {0: "Seg", 1: "Ter", 2: "Qua", 3: "Qui", 4: "Sex", 5: "Sáb", 6: "Dom"}
IMPACTS = {0: "Sem evento", 1: "Baixo", 2: "Médio", 3: "Alto"}
KPIS = (("n", "Operações", "Quantas operações há no recorte."),
        ("win_rate", "Acerto", "Percentual de operações com resultado positivo."),
        ("profit_factor", "Fator de lucro", "Soma dos ganhos dividida pela soma das perdas. Acima de 1, o robô ganhou "
                                            "mais do que perdeu."),
        ("expectancy", "Expectativa", "Resultado médio por operação, com o intervalo de 90% (bootstrap por dias)."),
        ("max_drawdown_pct", "Queda máxima", "Maior queda da curva de resultado a partir de um topo."))


class LaboratoryPanel:
    def __init__(self, parent, ui):
        self.ui = ui
        self.theme = ui.theme
        self.frame = ttk.Frame(parent, padding=12)
        self.runs = []
        self.lab = None            # livro enriquecido da seleção (sem filtros de tela)
        self.view = None           # com os filtros de tela
        self.loaded = False
        self._request = 0
        self._charts = {}
        self._drawn = set()        # guias já desenhadas com o recorte atual

        head = ttk.Frame(self.frame)
        head.pack(fill=tk.X, pady=(0, 8))
        ttk.Label(head, text="Laboratório", style="Page.Title.TLabel").pack(side=tk.LEFT)
        ttk.Label(head, text="operações dos robôs: onde ganham, onde perdem e como executar melhor",
                  style="Page.Muted.TLabel").pack(side=tk.LEFT, padx=(12, 0), pady=(6, 0))
        self.refresh_button = ttk.Button(head, text="Atualizar", command=self.reload)
        self.refresh_button.pack(side=tk.RIGHT)
        self.import_button = ttk.Button(head, text="Importar backtests antigos", command=self.import_runs)
        self.import_button.pack(side=tk.RIGHT, padx=(0, 8))
        Tooltip(self.import_button, "Traz para o Laboratório os backtests gravados antes dele existir. Os de robôs "
                                    "MQL5 entram sem o horário de entrada (análise só por horário de saída).", self.theme)

        self._build_filters()
        self._build_kpis()
        self.note = ttk.Label(self.frame, text="", style="Muted.TLabel", wraplength=1000, justify=tk.LEFT)
        self.tabs = ttk.Notebook(self.frame, style="Compact.TNotebook")
        self.tabs.pack(fill=tk.BOTH, expand=True)
        self.tab_frames = {}
        for name in TABS:
            tab = ttk.Frame(self.tabs, padding=8, style="Surface.TFrame")
            self.tabs.add(tab, text=name, state="disabled" if name in DISABLED_TABS else "normal")
            self.tab_frames[name] = tab
            if name in DISABLED_TABS:
                ttk.Label(tab, text=DISABLED_TABS[name], style="Muted.TLabel").pack(pady=20)
        self.tabs.bind("<<NotebookTabChanged>>", lambda e: self.render_visible(), add="+")
        self.theme.on_change(self._theme_changed)

    # --- montagem ----------------------------------------------------------------------

    def set_note(self, text):
        """Avisos acima das guias; a faixa some quando não há aviso."""
        self.note.config(text=text)
        if text:
            self.note.pack(fill=tk.X, pady=(0, 6), before=self.tabs)
        else:
            self.note.pack_forget()

    def _build_filters(self):
        bar = ttk.Frame(self.frame)
        bar.pack(fill=tk.X, pady=(0, 8))
        ttk.Label(bar, text="Modalidade:").pack(side=tk.LEFT)
        self.lens = tk.StringVar(value=LENS_ROBOT)
        for value, label, hint in ((LENS_ROBOT, "Por robô", "Em quais ativos, horários e condições este robô vai "
                                                             "melhor?"),
                                   (LENS_ASSET, "Por ativo", "Que robôs exploram melhor este ativo?")):
            w = ttk.Radiobutton(bar, text=label, value=value, variable=self.lens, command=self._lens_changed)
            w.pack(side=tk.LEFT, padx=(8, 0))
            Tooltip(w, hint, self.theme)
        self.selector_label = ttk.Label(bar, text="Robô:")
        self.selector_label.pack(side=tk.LEFT, padx=(18, 4))
        self.selected = tk.StringVar()
        self.selector = ttk.Combobox(bar, textvariable=self.selected, state="readonly", width=28)
        self.selector.pack(side=tk.LEFT)
        self.selector.bind("<<ComboboxSelected>>", lambda e: self.load_lab())
        ttk.Label(bar, text="Período:").pack(side=tk.LEFT, padx=(18, 4))
        self.period = tk.StringVar(value=PERIODS[0][0])
        period = ttk.Combobox(bar, textvariable=self.period, state="readonly", width=16,
                              values=[p[0] for p in PERIODS])
        period.pack(side=tk.LEFT)
        period.bind("<<ComboboxSelected>>", lambda e: self.apply_filters())
        self.hide_news = tk.BooleanVar(value=False)
        news = ttk.Checkbutton(bar, text="Ocultar notícias de impacto alto", variable=self.hide_news,
                               command=self.apply_filters)
        news.pack(side=tk.LEFT, padx=(18, 0))
        Tooltip(news, "Tira as operações com evento de impacto alto na janela de [TRADE_ANALYTICS] "
                      "event_window_before_min / after_min. Calendário: mt5x trades calendar import.", self.theme)

    def _build_kpis(self):
        row = ttk.Frame(self.frame)
        row.pack(fill=tk.X, pady=(0, 6))
        self.kpi = {}
        for k, (key, title, hint) in enumerate(KPIS):
            row.columnconfigure(k, weight=1, uniform="kpi")
            body = card(row, title, padding=10)
            body.master.grid(row=0, column=k, sticky="nsew", padx=(0 if k == 0 else 8, 0))
            value = ttk.Label(body, text="—", style="Value.TLabel")
            value.pack(anchor="w")
            sub = ttk.Label(body, text="", style="Muted.TLabel")
            sub.pack(anchor="w")
            Tooltip(body.master, hint, self.theme)
            self.kpi[key] = (value, sub)

    def _chart(self, tab, key, row, col, rowspan=1, colspan=1, height=3.0):
        chart = self._charts.get(key)
        if chart is None:
            chart = charts.Chart(self.tab_frames[tab], self.theme, height)
            chart.widget.grid(row=row, column=col, rowspan=rowspan, columnspan=colspan, sticky="nsew", padx=4,
                              pady=4)
            self.tab_frames[tab].rowconfigure(row, weight=1)
            self.tab_frames[tab].columnconfigure(col, weight=1)
            self._charts[key] = chart
        return chart

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
        self._show_kpis()
        self.render_visible()

    # --- desenho -----------------------------------------------------------------------

    def _empty(self, text):
        for key in ("n", "win_rate", "profit_factor", "expectancy", "max_drawdown_pct"):
            self.kpi[key][0].config(text="—")
            self.kpi[key][1].config(text="")
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
        self.kpi["n"][0].config(text=fmt_int(m["n"]))
        self.kpi["n"][1].config(text=f"{view['run_id'].nunique()} run(s)")
        self.kpi["win_rate"][0].config(text=fmt_num(m["win_rate"] * 100, 1, "%"))
        self.kpi["profit_factor"][0].config(text=fmt_num(m["profit_factor"], 2))
        self.kpi["expectancy"][0].config(text=charts.money(m["expectancy"]),
                                         style="Good.TLabel" if m["ci_low"] is not None and m["ci_low"] > 0 else
                                         "Bad.TLabel" if m["ci_high"] is not None and m["ci_high"] < 0 else
                                         "Value.TLabel")
        self.kpi["expectancy"][1].config(text=f"90%: {charts.money(m['ci_low'])} a {charts.money(m['ci_high'])}"
                                         if m["ci_low"] is not None else "intervalo: poucos dias")
        self.kpi["max_drawdown_pct"][0].config(text=fmt_num(m["max_drawdown_pct"] * 100, 1, "%"))
        partial = int((view["completeness"] == "parcial").sum())
        notes = []
        if m["small"]:
            notes.append(f"Amostra pequena ({m['n']} operações): use como pista, não como conclusão.")
        if partial:
            notes.append(f"{partial} operação(ões) sem horário de entrada (robôs MQL5 antes da T16.2b): entram só "
                         "nas análises de calendário, pela saída.")
        if "event_impact" in view and pd.to_numeric(view["event_impact"], errors="coerce").isna().all():
            notes.append("Sem calendário econômico: importe com `mt5x trades calendar import`.")
        self.set_note(" ".join(notes))

    def render_visible(self):
        if self.view is None or self.view.empty:
            return
        name = self.tabs.tab(self.tabs.select(), "text")
        if name in self._drawn or name in DISABLED_TABS:
            return
        try:
            {"Visão geral": self._overview, "Tempo e calendário": self._time, "Microestrutura": self._micro,
             "Comparativo": self._compare}[name]()
            self._drawn.add(name)
        except ImportError:
            self.set_note("Gráficos indisponíveis: instale o matplotlib (pip install matplotlib).")
        except Exception as e:
            log.exception("Erro ao desenhar o Laboratório")
            self.set_note(f"Erro ao desenhar: {e}")

    def _theme_changed(self):
        self._drawn = set()
        self.render_visible()

    def _heat(self, chart, ax, view, row, col, title, col_labels=None, row_labels=None, row_order=None):
        from mt5_extracao.trade_analytics import aggregate
        table = aggregate.crosstab(view, row, col, config=self._config())
        if row_order and not table["value"].empty:
            order = [r for r in row_order if r in table["value"].index]
            table = {k: v.reindex(order) for k, v in table.items()}
        charts.heatmap(chart, ax, table["value"], table["n"], table["small"], title, row_labels, col_labels)

    def _overview(self):
        from mt5_extracao.trade_analytics import aggregate
        view, tab = self.view, "Visão geral"
        timed = view[view["time_ref"] == "entrada"]
        chart = self._chart(tab, "ov_heat", 0, 0, height=3.4)
        chart.clear()
        self._heat(chart, chart.figure.add_subplot(111), timed if not timed.empty else view, "time_bucket",
                   "day_of_week", "Expectativa: faixa horária × dia (n por célula; tramado = menos de 30)",
                   WEEKDAYS)
        chart.figure.tight_layout()
        chart.draw()
        by = "symbol" if self.lens.get() == LENS_ROBOT else "robot_key"
        chart = self._chart(tab, "ov_curves", 0, 1, height=3.4)
        chart.clear()
        charts.overlay(chart, chart.figure.add_subplot(111), aggregate.equity_curves(view, by),
                       "Resultado acumulado por " + ("ativo" if by == "symbol" else "robô"))
        chart.draw()
        chart = self._chart(tab, "ov_news", 1, 0, colspan=2, height=2.4)
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
            chart.figure.tight_layout()
            chart.draw()

    def _time(self):
        from mt5_extracao.trade_analytics import aggregate
        view, tab, cfg = self.view, "Tempo e calendário", self._config()
        days = aggregate.by_dimension(view, ["day_of_week"], cfg, ci=False)
        chart = self._chart(tab, "t_week", 0, 0)
        chart.clear()
        charts.bars(chart, chart.figure.add_subplot(111), [WEEKDAYS.get(int(d), d) for d in days["day_of_week"]],
                    days["expectancy"], days["n"], days["small"], "Dia da semana: expectativa por operação")
        chart.figure.tight_layout()
        chart.draw()
        month = aggregate.by_dimension(view, ["day_of_month"], cfg, ci=False)
        chart = self._chart(tab, "t_month", 0, 1)
        chart.clear()
        charts.bars(chart, chart.figure.add_subplot(111), [str(int(d)) for d in month["day_of_month"]],
                    month["expectancy"], month["n"], month["small"], "Dia do mês: expectativa por operação")
        chart.figure.tight_layout()
        chart.draw()
        hours = aggregate.by_dimension(view, ["time_bucket"], cfg, ci=False)
        chart = self._chart(tab, "t_hours", 1, 0)
        chart.clear()
        ax = chart.figure.add_subplot(111)
        charts.bars(chart, ax, list(hours["time_bucket"]), hours["expectancy"], hours["n"], hours["small"],
                    "Faixa horária: expectativa (barras) e acerto % (linha)")
        twin = ax.twinx()
        twin.plot(range(len(hours)), hours["win_rate"] * 100, color=self.theme.colors["ink2"], linewidth=1.2,
                  marker="o", markersize=3)
        twin.set_ylim(0, 100)
        twin.tick_params(colors=self.theme.colors["muted"], labelsize=7, length=0)
        for side in ("top", "right", "left", "bottom"):
            twin.spines[side].set_visible(False)
        chart.figure.tight_layout()
        chart.draw()
        chart = self._chart(tab, "t_winloss", 1, 1)
        chart.clear()
        ax = chart.figure.add_subplot(111)
        charts.style_axis(ax, self.theme, "Vencedoras × perdedoras por faixa horária")
        counts = view.groupby(["time_bucket", "win_flag"]).size().unstack(fill_value=0) \
            .reindex(columns=[1, 0], fill_value=0)
        x = np.arange(len(counts))
        c = self.theme.colors
        ax.bar(x, counts[1], color=c["series"][0], width=0.65, label="vencedoras", linewidth=0)
        ax.bar(x, counts[0], bottom=counts[1], color=c["series"][1], width=0.65, label="perdedoras", linewidth=0,
               hatch="..")
        ax.set_xticks(x)
        crowded = len(x) > 12
        ax.set_xticklabels(counts.index, fontsize=7 if crowded else 8, rotation=45 if crowded else 0,
                           ha="right" if crowded else "center")
        legend = ax.legend(fontsize=7, frameon=False)
        for text in legend.get_texts():
            text.set_color(c["ink2"])
        chart.figure.tight_layout()
        chart.draw()

    def _micro(self):
        from mt5_extracao.trade_analytics import aggregate
        view, tab, cfg = self.view, "Microestrutura", self._config()
        complete = view[view["time_ref"] == "entrada"]
        if complete.empty or pd.to_numeric(complete["atr_pct"], errors="coerce").isna().all():
            for key, r, col in (("m_scatter", 0, 0), ("m_vol", 0, 1), ("m_volume", 1, 0), ("m_heat", 1, 1)):
                self._chart(tab, key, r, col).message(
                    "Sem horário de entrada ou sem barras na base para estas operações: a microestrutura precisa dos "
                    "dois (extraia o ativo no timeframe do backtest).")
            return
        chart = self._chart(tab, "m_scatter", 0, 0)
        chart.clear()
        charts.scatter_win_loss(chart, chart.figure.add_subplot(111), pd.to_numeric(complete["atr_pct"]) * 100,
                                complete["pnl"], "Volatilidade na entrada (ATR %) × resultado", "ATR % do preço")
        chart.figure.tight_layout()
        chart.draw()
        order = {"baixa": 0, "média": 1, "alta": 2}
        vol = aggregate.by_dimension(complete, ["volatility_bucket"], cfg, ci=False)
        vol = vol.assign(o=vol["volatility_bucket"].map(order)).sort_values("o")
        chart = self._chart(tab, "m_vol", 0, 1)
        chart.clear()
        charts.bars(chart, chart.figure.add_subplot(111),
                    [f"{b}\n{fmt_num(w * 100, 0, '%')} acerto" for b, w in zip(vol["volatility_bucket"], vol["win_rate"])],
                    vol["expectancy"], vol["n"], vol["small"], "Faixa de volatilidade (percentil nas 250 barras anteriores)")
        chart.figure.tight_layout()
        chart.draw()
        ranked = complete.assign(volume_band=pd.cut(pd.to_numeric(complete["volume_rank"], errors="coerce"),
                                                    [0, 0.25, 0.5, 0.75, 1.0], include_lowest=True,
                                                    labels=["baixo", "médio-baixo", "médio-alto", "alto"]))
        volume = aggregate.by_dimension(ranked.dropna(subset=["volume_band"]).assign(
            volume_band=lambda d: d["volume_band"].astype(str)), ["volume_band"], cfg, ci=False)
        order = {"baixo": 0, "médio-baixo": 1, "médio-alto": 2, "alto": 3}
        volume = volume.assign(o=volume["volume_band"].map(order)).sort_values("o")
        chart = self._chart(tab, "m_volume", 1, 0)
        chart.clear()
        charts.bars(chart, chart.figure.add_subplot(111),
                    [f"{b}\n{fmt_num(w * 100, 0, '%')} acerto" for b, w in zip(volume["volume_band"], volume["win_rate"])],
                    volume["expectancy"], volume["n"], volume["small"], "Volume na entrada (quartil nas 20 barras anteriores)")
        chart.figure.tight_layout()
        chart.draw()
        chart = self._chart(tab, "m_heat", 1, 1)
        chart.clear()
        self._heat(chart, chart.figure.add_subplot(111), complete, "volatility_bucket", "time_bucket",
                   "Volatilidade × faixa horária", row_order=("baixa", "média", "alta"))
        chart.figure.tight_layout()
        chart.draw()

    def _compare(self):
        from mt5_extracao.trade_analytics import aggregate
        tab, view, cfg = "Comparativo", self.view, self._config()
        frame = self.tab_frames[tab]
        if not hasattr(self, "compare_tree"):
            box, self.compare_tree = scrolled_tree(
                frame, ("Item", "Timeframe", "Operações", "Acerto", "Fator de lucro", "Expectativa", "Intervalo 90%",
                        "Queda máxima"), (170, 80, 80, 70, 100, 100, 170, 100),
                ("w", "w", "e", "e", "e", "e", "w", "e"), height=8)
            box.grid(row=0, column=0, columnspan=2, sticky="nsew", padx=4, pady=4)
            frame.rowconfigure(0, weight=1)
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
        chart = self._chart(tab, "c_corr", 1, 0, colspan=2, height=2.8)
        if self.lens.get() == LENS_ASSET:
            corr = aggregate.daily_correlation(view, "robot_key")
            if corr.shape[0] >= 2:
                chart.clear()
                n = pd.DataFrame(np.ones(corr.shape, dtype=int), index=corr.index, columns=corr.columns)
                charts.heatmap(chart, chart.figure.add_subplot(111), corr, n * 0, corr.isna(),
                               "Correlação dos resultados diários entre robôs (positiva = perdas que se somam)",
                               metric_name="Correlação")
                chart.figure.tight_layout()
                chart.draw()
                return
            chart.message("A correlação entre robôs precisa de pelo menos dois robôs com 20 dias em comum neste ativo.")
        else:
            chart.message("Na lente por robô, a tabela acima compara o robô em cada ativo e timeframe. A correlação "
                          "entre robôs aparece na lente por ativo.")

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
