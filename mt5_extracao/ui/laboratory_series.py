"""Consumidor Tk do Laboratório de séries; cálculo e diagnóstico ficam no SDK."""
import json
import tkinter as tk
from tkinter import filedialog, ttk

import numpy as np
import pandas as pd

from mt5_extracao import timeframes
from mt5_extracao.laboratory import service
from . import charts
from .components import DetailsPanel, Notice
from .theme import Tooltip

PARAMETER_LABELS = {
    "fast": "Média curta", "slow": "Média longa", "trend": "Média de tendência",
    "stop_atr": "Stop (ATR)", "trail_atr": "Trailing (ATR)", "entry_period": "Canal de entrada",
    "exit_period": "Canal de saída", "adx_min": "ADX mínimo", "adx_max": "ADX máximo",
    "atr_avg": "Média ATR", "bb_period": "Período Bollinger", "bb_k": "Desvio Bollinger",
    "rsi_period": "Período RSI", "rsi_low": "RSI inferior", "max_bars": "Prazo (barras)",
    "range_minutes": "Janela (minutos)", "target_mult": "Alvo (multiplicador)",
    "min_range_atr": "Amplitude mínima (ATR)", "rsi_pullback": "RSI de recuo", "swing_bars": "Swing (barras)",
    "target_r": "Alvo (R)", "period": "Período", "kc_mult": "Desvio Keltner", "lookback": "Retrospecto",
    "warmup_bars": "Aquecimento (barras)", "stop_points": "SL (pontos)", "target_points": "TP (pontos; 0 desliga)",
    "k_period": "Período K", "slowing": "Desaceleração K", "d_period": "Período D", "ema_period": "Período EMA",
    "upper_level": "Nível superior", "lower_level": "Nível inferior", "stop_offset_points": "Offset SL (pontos)",
    "entry_band_pct": "Banda entrada (%)", "target_band_pct": "Banda alvo (%; 0 central)",
    "volume_source": "Volume (0 auto / 1 ticks / 2 real)", "first_period": "Primeira média (barras)",
    "period_step": "Passo entre médias", "distance_atr": "Afastamento (ATR)",
    "distance_points": "Afastamento (pontos; 0 ATR)", "trail_line": "Média de gestão (1–10)",
    "offset_atr": "Offset SL (ATR)", "hilo_period": "Período HiLo", "hilo_offset": "Deslocamento HiLo (0)",
    "stop_buffer_points": "Folga SL (pontos)",
}


def timeline_bins(channels, max_columns):
    """Redução apenas gráfica: conserva eventos raros e distingue conflito de neutro."""
    count = len(channels[0]["directions"])
    boundaries = np.unique(np.linspace(0, count, min(count, max_columns) + 1).astype(int))
    data = []
    for channel in channels:
        row = []
        for start, end in zip(boundaries[:-1], boundaries[1:]):
            values = {value for value in channel["directions"][start:end] if value is not None}
            row.append(np.nan if not values else 2 if {-1, 1} <= values else 1 if 1 in values else -1 if -1 in values else 0)
        data.append(row)
    return np.array(data), boundaries


def empty_channels(report):
    return [row for row in report["channels"] if row.get("diagnosis")]


def empty_summary(report, fallback):
    rows = empty_channels(report)
    if not rows:
        return fallback
    return "\n".join(f"{row['id']}: {row['diagnosis']['message']}" for row in rows[:4]) + "\n" + rows[0]["diagnosis"]["action"]


def monitor_geometry(widget):
    """Consulta o monitor DA janela, incluindo coordenadas negativas e dock removido."""
    from mt5_extracao.robots.session_window import Placement
    placement = None
    try:
        point = (widget.winfo_rootx() + widget.winfo_width() // 2,
                 widget.winfo_rooty() + widget.winfo_height() // 2)
        placement = Placement(anchor=point)
        monitor = placement.monitor
        return monitor.x, monitor.y, monitor.width, monitor.height
    except (OSError, AttributeError, RuntimeError):
        return 0, 0, widget.winfo_screenwidth(), widget.winfo_screenheight()
    finally:
        if placement is not None:
            placement.close()


class AnalysisChart:
    """O detalhe amplia o mesmo DTO; fechar restaura a região e a seleção originais."""
    def __init__(self, parent, theme, title, renderer):
        self.theme, self.renderer, self.title = theme, renderer, title
        self.frame = ttk.Frame(parent, style="Surface.TFrame", padding=8)
        header = ttk.Frame(self.frame, style="Surface.TFrame")
        header.pack(fill=tk.X)
        ttk.Label(header, text=title, style="Heading.TLabel").pack(side=tk.LEFT)
        self.expand_button = ttk.Button(header, text="Ampliar ↗", command=self.expand)
        self.expand_button.pack(side=tk.RIGHT)
        self.chart = charts.Chart(self.frame, theme, height=3.2)
        self.chart.widget.configure(height=315)
        self.chart.widget.pack(fill=tk.BOTH, expand=True)
        self.popup = None
        self._resize_jobs = {}
        self.chart.canvas.mpl_connect("resize_event", lambda _: self._resized(self.chart))
        self.frame.bind("<Destroy>", self._destroyed, add="+")

    def _resized(self, chart):
        # Matplotlib já redimensiona os demais gráficos. Só o mapa temporal
        # precisa refazer a redução conservadora conforme os pixels disponíveis.
        if self.renderer.__name__ != "_timeline":
            return
        job = self._resize_jobs.pop(chart, None)
        if job is not None:
            self.frame.after_cancel(job)
        def render():
            self._resize_jobs.pop(chart, None)
            if chart.widget.winfo_exists():
                self.renderer(chart)
        self._resize_jobs[chart] = self.frame.after(120, render)

    def _destroyed(self, event):
        if event.widget is self.frame:
            for job in self._resize_jobs.values():
                self.frame.after_cancel(job)
            self._resize_jobs.clear()

    def redraw(self):
        self.renderer(self.chart)
        if self.popup is not None:
            self.renderer(self.detail)

    def expand(self):
        if self.popup is not None:
            self.restore()
            return
        from matplotlib.backends.backend_tkagg import NavigationToolbar2Tk
        popup = self.popup = tk.Toplevel(self.frame)
        popup.title(self.title + " · Laboratório")
        popup.transient(self.frame.winfo_toplevel())
        x, y, width, height = monitor_geometry(self.frame.winfo_toplevel())
        # Tk distingue "-300" (distância da borda direita) de "+-300"
        # (coordenada absoluta negativa num monitor à esquerda/acima).
        popup.geometry(f"{width}x{max(300, height - 50)}+{x}+{y}")
        popup.protocol("WM_DELETE_WINDOW", self.restore)
        popup.bind("<Escape>", lambda _: self.restore())
        ttk.Button(popup, text="↙ Restaurar região", command=self.restore).pack(anchor="e", padx=8, pady=4)
        self.detail = charts.Chart(popup, self.theme, height=6)
        self.detail.widget.pack(fill=tk.BOTH, expand=True)
        self.detail.canvas.mpl_connect("resize_event", lambda _: self._resized(self.detail))
        NavigationToolbar2Tk(self.detail.canvas, popup).update()
        self.expand_button.configure(text="Restaurar ↙")
        self.renderer(self.detail)

    def restore(self):
        if self.popup is not None:
            job = self._resize_jobs.pop(self.detail, None)
            if job is not None:
                self.frame.after_cancel(job)
            self.popup.destroy()
            self.popup = None
            if self.expand_button.winfo_exists():
                self.expand_button.configure(text="Ampliar ↗")


class SeriesLaboratoryPanel:
    def __init__(self, parent, ui):
        self.ui, self.theme = ui, ui.theme
        self.frame = ttk.Frame(parent, padding=4)
        self.report = None
        self._request = 0
        self._selection_revision = 0
        self._columns = None
        self._inventory = []
        self.pending = False
        self._display_initialized = False
        self.catalog = service.catalog()
        self._build_controls()
        self.status = ttk.Label(self.frame, text="Escolha uma base, os TFs e os indicadores para congelar a análise.",
                                style="Muted.TLabel", wraplength=1000)
        self.status.pack(fill=tk.X, pady=4)
        self.notice = Notice(self.frame, "Reconstrução exploratória por fechamento nominal · sem prova de "
            "disponibilidade histórica · correlação não é acerto nem resultado de trades.", theme=self.theme)
        self.notice.pack(fill=tk.X, pady=4)
        self.notice.message.configure(wraplength=1000)
        self.canvas = tk.Canvas(self.frame, highlightthickness=0, height=580)
        scroll_x = ttk.Scrollbar(self.frame, orient=tk.HORIZONTAL, command=self.canvas.xview)
        self.canvas.configure(xscrollcommand=scroll_x.set)
        scroll_x.pack(side=tk.BOTTOM, fill=tk.X)
        self.canvas.pack(fill=tk.BOTH, expand=True)
        self.body = ttk.Frame(self.canvas)
        self._body_id = self.canvas.create_window(0, 0, anchor="nw", window=self.body)
        self.panels = {}
        for key, title, render in (("price", "Preço e indicador no mesmo relógio", self._price),
                ("timeline", "Estratégia entre tempos gráficos", self._timeline),
                ("relations", "Indicadores × estratégia × TF", self._relations),
                ("lags", "Defasagens e confluências", self._lags),
                ("coverage", "Cobertura e disponibilidade", self._coverage)):
            self.panels[key] = AnalysisChart(self.body, self.theme, title, render)
        self.canvas.bind("<Configure>", self._layout)
        self.body.bind("<Configure>", lambda _: self.canvas.configure(scrollregion=self.canvas.bbox("all")))
        self.frame.bind("<Destroy>", self._destroyed, add="+")
        # LaboratoryPanel já é o dono da atualização do tema da guia visível.
        self.redraw()

    def _build_controls(self):
        actions = ttk.Frame(self.frame)
        actions.pack(fill=tk.X)
        self.analyze_button = ttk.Button(actions, text="Congelar e analisar", command=self.analyze, style="Accent.TButton")
        self.analyze_button.pack(side=tk.LEFT)
        ttk.Button(actions, text="Atualizar fontes", command=self.reload).pack(side=tk.LEFT, padx=4)
        self.extended = tk.BooleanVar(value=False)
        ttk.Checkbutton(actions, text="Visão ampliada", variable=self.extended,
                        command=self._layout).pack(side=tk.LEFT, padx=4)
        ttk.Button(actions, text="Exportar JSON", command=self.export).pack(side=tk.RIGHT)
        ttk.Button(actions, text="Gerar preset…", command=self.open_presets).pack(side=tk.RIGHT, padx=4)
        action_widgets = list(actions.winfo_children())
        for widget in action_widgets:
            widget.pack_forget()
        def layout_actions(event):
            columns = 5 if event.width > 1050 else 3 if event.width > 650 else 2
            for index, widget in enumerate(action_widgets):
                widget.grid(row=index // columns, column=index % columns, sticky="w", padx=3, pady=3)
        actions.bind("<Configure>", layout_actions)
        selection = DetailsPanel(self.frame, "Base, estratégia e cruzamentos", expanded=True, theme=self.theme)
        selection.pack(fill=tk.X, pady=4)
        self.selection_panel = selection
        self.controls = ttk.Frame(selection.body)
        self.controls.pack(fill=tk.X)
        self.control_cells = [ttk.Frame(self.controls) for _ in range(3)]
        source, strategy, features = self.control_cells
        ttk.Label(source, text="Ativo / timeframes da base").pack(anchor="w")
        self.symbol = tk.StringVar()
        self.symbol_box = ttk.Combobox(source, textvariable=self.symbol, state="readonly", width=24)
        self.symbol_box.pack(fill=tk.X)
        self.symbol_box.bind("<<ComboboxSelected>>", self._symbol_changed)
        self.tf_list = tk.Listbox(source, selectmode=tk.MULTIPLE, exportselection=False, height=5)
        self.tf_list.pack(fill=tk.X)
        self.tf_list.bind("<<ListboxSelect>>", self._changed)
        ttk.Label(strategy, text="Estratégia (mesmos parâmetros em todos os TFs)").pack(anchor="w")
        self.strategy = tk.StringVar(value="ema_atr")
        self.strategy_box = ttk.Combobox(strategy, textvariable=self.strategy, state="readonly",
                                       values=["Sem estratégia", *service.REGISTRY], width=26)
        self.strategy_box.pack(fill=tk.X)
        self.strategy_box.bind("<<ComboboxSelected>>", self._strategy_changed)
        self.params = tk.StringVar(value=json.dumps(service.get_strategy("ema_atr").params))
        self.parameters_frame = ttk.Frame(strategy)
        self.parameters_frame.pack(fill=tk.X, pady=4)
        self.parameter_values = {}
        self._params_error = None
        self._build_parameters()
        dates = ttk.Frame(strategy)
        dates.pack(fill=tk.X)
        self.start, self.end = tk.StringVar(), tk.StringVar()
        for label, variable in (("De (broker)", self.start), ("Até (broker)", self.end)):
            cell = ttk.Frame(dates)
            cell.pack(side=tk.LEFT, fill=tk.X, expand=True)
            ttk.Label(cell, text=label).pack(anchor="w")
            ttk.Entry(cell, textvariable=variable, width=14).pack(fill=tk.X)
        ttk.Label(strategy, text="Janela vazia = toda a série (máx. 20 mil barras/TF).",
                  style="Muted.TLabel", wraplength=330).pack(anchor="w")
        ttk.Label(features, text=f"Indicadores do inventário ({len(self.catalog['indicators'])} calculáveis) · "
                  "seleção múltipla").pack(anchor="w")
        self.feature_query = tk.StringVar()
        search = ttk.Entry(features, textvariable=self.feature_query)
        search.pack(fill=tk.X, pady=(2, 2))
        Tooltip(search, "Buscar por nome, id, família, unidade ou parâmetro; a seleção é mantida ao filtrar.")
        self.feature_hint = ttk.Label(features, text="", style="Muted.TLabel", wraplength=330)
        self.feature_hint.pack(anchor="w")
        feature_frame = ttk.Frame(features)
        feature_frame.pack(fill=tk.BOTH, expand=True)
        self.feature_list = tk.Listbox(feature_frame, selectmode=tk.MULTIPLE, exportselection=False, height=7)
        self.feature_list.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar = ttk.Scrollbar(feature_frame, command=self.feature_list.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.feature_list.configure(yscrollcommand=scrollbar.set)
        self._chosen_features = {item["feature_id"] for item in self.catalog["indicators"]
                                 if item["feature_id"] in service.DEFAULT_FEATURES}
        self._filter_features()
        self.feature_query.trace_add("write", self._filter_features)
        self.feature_list.bind("<<ListboxSelect>>", self._features_selected)
        detail = ttk.Frame(self.frame)
        detail.pack(fill=tk.X, pady=4)
        ttk.Label(detail, text="Detalhar no gráfico:").pack(side=tk.LEFT)
        self.detail_channel = tk.StringVar()
        self.channel_box = ttk.Combobox(detail, textvariable=self.detail_channel, state="readonly", width=52)
        self.channel_box.pack(side=tk.LEFT, padx=4, fill=tk.X, expand=True)
        self.channel_box.bind("<<ComboboxSelected>>", self._channel_changed)
        self.channel_details = ttk.Label(self.frame, text="", style="Muted.TLabel", wraplength=1000)
        self.channel_details.pack(fill=tk.X, pady=4)
        self.frame.bind("<Configure>", self._wrap_details, add="+")
        for variable in (self.params, self.start, self.end):
            variable.trace_add("write", self._changed)
        self.controls.bind("<Configure>", self._layout_controls)

    def _filter_features(self, *_):
        from mt5_extracao.laboratory import inventory
        found = inventory.search(query=self.feature_query.get(), page_size=inventory.MAX_PAGE_SIZE)
        calculable = {item["feature_id"] for item in self.catalog["indicators"]}
        self._visible_features = [row["feature_id"] for row in found["items"] if row["feature_id"] in calculable]
        names = {row["feature_id"]: row for row in found["items"]}
        self.feature_list.delete(0, tk.END)
        for index, ident in enumerate(self._visible_features):
            row = names[ident]
            self.feature_list.insert(tk.END, f"{row['name']} · {row['family']}")
            if ident in self._chosen_features:
                self.feature_list.selection_set(index)
        self.feature_hint.configure(text=found["reason"] or
            f"{len(self._visible_features)} de {len(calculable)} · {len(self._chosen_features)} selecionados")

    def _features_selected(self, *_):
        selected = {self._visible_features[i] for i in self.feature_list.curselection()}
        self._chosen_features = (self._chosen_features - set(self._visible_features)) | selected
        self.feature_hint.configure(text=f"{len(self._visible_features)} de {len(self.catalog['indicators'])} · "
                                         f"{len(self._chosen_features)} selecionados")
        self._changed()

    def _wrap_details(self, event):
        if event.widget is not self.frame:
            return
        self.channel_details.configure(wraplength=max(250, event.width - 20))
        if hasattr(self, "status"):
            self.status.configure(wraplength=max(250, event.width - 20))
        if hasattr(self, "notice"):
            self.notice.message.configure(wraplength=max(200, event.width - 120))

    def _layout_controls(self, event):
        columns = 3 if event.width >= 1100 else 2 if event.width >= 720 else 1
        for i in range(3):
            self.controls.columnconfigure(i, weight=1 if i < columns else 0)
        for i, cell in enumerate(self.control_cells):
            cell.grid(row=i // columns, column=i % columns, sticky="nsew", padx=4, pady=4)

    def _changed(self, *_):
        self._selection_revision += 1
        if self.report:
            self.status.configure(text="Seleção alterada. O diagnóstico exibido conserva a seleção congelada; analise novamente.")

    def _strategy_changed(self, *_):
        self.params.set("{}" if self.strategy.get() == "Sem estratégia" else
                        json.dumps(service.get_strategy(self.strategy.get()).params))
        self._build_parameters()
        self._changed()

    def _build_parameters(self):
        for widget in self.parameters_frame.winfo_children():
            widget.destroy()
        self.parameter_values = {}
        self._params_error = None
        if self.strategy.get() == "Sem estratégia":
            return
        strategy = service.get_strategy(self.strategy.get())
        specs = service.STRATEGY_PARAMETERS[strategy.key]
        for index, (key, value) in enumerate(strategy.params.items()):
            cell = ttk.Frame(self.parameters_frame)
            cell.grid(row=index // 2, column=index % 2, sticky="ew", padx=(0, 5), pady=2)
            self.parameters_frame.columnconfigure(index % 2, weight=1)
            ttk.Label(cell, text=PARAMETER_LABELS.get(key, key), style="Muted.TLabel", wraplength=170).pack(anchor="w")
            variable = tk.StringVar(value=str(value))
            self.parameter_values[key] = variable
            entry = ttk.Entry(cell, textvariable=variable, width=13)
            entry.pack(fill=tk.X)
            spec = specs[key]
            Tooltip(entry, f"Unidade: {spec.unit}. Mínimo {'>' if spec.exclusive_minimum else '≥'}{spec.minimum}; "
                    f"máximo {spec.maximum if spec.maximum is not None else 'conforme estratégia'}.", self.theme)
            variable.trace_add("write", self._parameters_edited)

    def _parameters_edited(self, *_):
        specs = service.STRATEGY_PARAMETERS[self.strategy.get()]
        values = {}
        self._params_error = None
        for key, variable in self.parameter_values.items():
            try:
                text = variable.get().strip().replace(",", ".")
                value = int(text) if specs[key].kind == "int" else float(text)
                if not specs[key].accepts(value):
                    raise ValueError
                values[key] = value
            except (ValueError, OverflowError):
                self._params_error = f"Revise {PARAMETER_LABELS.get(key, key)}: valor fora do tipo ou intervalo permitido."
                break
        if self._params_error is None:
            self.params.set(json.dumps(values))
        else:
            self._changed()

    def reload(self):
        def done(items, error):
            if not self.frame.winfo_exists():
                return
            if error:
                self.status.configure(text=str(error))
                return
            self._inventory = items
            symbols = sorted({item["symbol"] for item in items})
            self.symbol_box.configure(values=symbols)
            if self.symbol.get() not in symbols:
                self.symbol.set(symbols[0] if symbols else "")
            self._symbol_changed()
            if not symbols:
                self.status.configure(text="Base sem séries compatíveis com esta captura OHLCV. "
                    "Confira inventário/Analytics antes de extrair mais dados.")
        self.ui.run_bg(lambda: service.sources(self.ui.app.db_manager), done)

    def _symbol_changed(self, *_):
        previous = {self._tfs[i] for i in self.tf_list.curselection()} if hasattr(self, "_tfs") else set()
        self._tfs = [item["timeframe"] for item in self._inventory if item["symbol"] == self.symbol.get()]
        self.tf_list.delete(0, tk.END)
        chosen = previous & set(self._tfs) or set(self._tfs[:3])
        for index, tf in enumerate(self._tfs):
            self.tf_list.insert(tk.END, tf)
            if tf in chosen:
                self.tf_list.selection_set(index)
        self._changed()

    def analyze(self):
        from mt5_extracao.automation.protocol import loads
        try:
            if self._params_error:
                raise ValueError(self._params_error)
            params = loads(self.params.get())
            if not isinstance(params, dict):
                raise ValueError("Parâmetros exigem objeto JSON.")
            symbol = self.symbol.get()
            tfs = [self._tfs[i] for i in self.tf_list.curselection()]
            feature_ids = [item["feature_id"] for item in self.catalog["indicators"]
                           if item["feature_id"] in self._chosen_features]
            strategy = None if self.strategy.get() == "Sem estratégia" else self.strategy.get()
            if not symbol or not tfs:
                raise ValueError("Escolha ativo e pelo menos um timeframe da base.")
            if not feature_ids and strategy is None:
                raise ValueError("Escolha indicadores ou uma estratégia para analisar.")
            if strategy is not None:
                service.base_composition(strategy, symbol=symbol, timeframe=tfs[0], params=params)
            if (len(feature_ids) + bool(strategy)) * len(tfs) > service.MAX_CHANNELS:
                raise ValueError("Máximo de 32 canais; reduza indicadores ou TFs para este recorte.")
        except (ValueError, AttributeError) as error:
            self.status.configure(text=str(error))
            return
        self._request += 1
        request, revision = self._request, self._selection_revision
        start, end = self.start.get() or None, self.end.get() or None
        self.pending = True
        self.analyze_button.state(["disabled"])
        self.status.configure(text="Congelando a seleção pela API de snapshots e calculando os cruzamentos…")
        def work():
            snapshots = service.capture_selection(self.ui.app.db_manager, symbol=symbol,
                timeframes_selected=tfs, start=start, end=end)
            return service.analyze(snapshots, feature_ids=feature_ids, strategy_key=strategy,
                strategy_params=params, as_of=pd.Timestamp.now(tz="UTC").isoformat())
        def done(report, error):
            if not self.frame.winfo_exists() or request != self._request:
                return
            self.pending = False
            self.analyze_button.state(["!disabled"])
            if revision != self._selection_revision:
                self.status.configure(text="Seleção mudou durante o cálculo; resultado antigo não publicado. Analise a seleção atual.")
            elif error:
                self.status.configure(text=f"Análise indisponível: {error}")
            else:
                self.set_report(report)
        self.ui.run_bg(work, done)

    def set_report(self, report):
        self.report = report
        ids = [channel["id"] for channel in report["channels"]]
        self.channel_box.configure(values=ids)
        if self.detail_channel.get() not in ids:
            self.detail_channel.set(ids[0])
        self.status.configure(text=f"{report['symbol']} · {len(report['populations'])} TFs · "
            f"{len(report['channels'])} canais · {len(report['times'])} fechamentos {report['grid_timeframe']} · "
            f"estudo {report['analysis_id'][:12]} · corte {report['selection']['as_of']}"
            + "".join(f"\n{row['id']}: {row['diagnosis']['message']} {row['diagnosis']['action']}"
                      for row in empty_channels(report)[:3])
            + (f"\n… e mais {len(empty_channels(report)) - 3} canais sem dados (ver Cobertura)."
               if len(empty_channels(report)) > 3 else ""))
        self._channel_changed(redraw=False)
        self._layout()
        self.redraw()

    def _channel_changed(self, *_, redraw=True):
        if self.report is None:
            return
        row = next((row for row in self.report["channels"] if row["id"] == self.detail_channel.get()), self.report["channels"][0])
        check = row["eligibility"]
        self.channel_details.configure(text=f"{row['id']} · {row['unit']} · aquecimento {row['warmup']} barras · "
            f"cobertura {row['available']}/{row['total']} · {check['status']} · "
            + (f"{row['diagnosis']['message']} {row['diagnosis']['action']} · " if row.get("diagnosis") else "")
            + ("; ".join(check["reasons"] + check["limitations"]) or "Dados compatíveis com este cálculo exploratório."))
        if redraw and hasattr(self, "panels"):
            for key in ("price", "lags"):
                self.panels[key].redraw()

    def _layout(self, _=None):
        extended = self.extended.get()
        width = max(self.canvas.winfo_width(), 2000 if extended else 700)
        columns = 2 if extended else 1
        if columns != self._columns:
            self._columns = columns
            for column in range(2):
                self.body.columnconfigure(column, weight=1 if column < columns else 0)
            for index, panel in enumerate(self.panels.values()):
                panel.frame.grid(row=index // columns, column=index % columns, sticky="nsew", padx=4, pady=4)
        self.canvas.itemconfigure(self._body_id, width=width)
        for key, panel in self.panels.items():
            height = 460 if extended else 340
            if key == "coverage" and self.report:
                height = max(height, len(self.report["channels"]) * 17 + 80)
            panel.chart.widget.configure(height=height)
        self.canvas.configure(height=1600 if extended else 2000)
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))

    def on_show(self):
        if not self._display_initialized:
            self._display_initialized = True
            _, _, width, height = monitor_geometry(self.frame.winfo_toplevel())
            window = self.frame.winfo_toplevel()
            self.extended.set(width > 1920 and height > 1080 and
                              window.winfo_width() > 1920 and window.winfo_height() > 1080)
            self._layout()
        if not self._inventory:
            self.reload()

    def redraw(self):
        if not self.frame.winfo_exists():
            return
        colors = self.theme.colors
        self.canvas.configure(background=colors["page"])
        for widget in (self.tf_list, self.feature_list):
            widget.configure(background=colors["field"], foreground=colors["ink"],
                selectbackground=colors["select"], selectforeground=colors["ink"],
                highlightbackground=colors["border"], highlightthickness=1, borderwidth=0,
                font=self.theme.font, activestyle="none")
        for panel in self.panels.values():
            panel.redraw()

    def _ready(self, chart):
        if not self.report:
            chart.message("Selecione séries reais e use Congelar e analisar.\nNenhum dado demonstrativo é apresentado nesta visão.")
            return False
        chart.clear()
        return True

    def _timeline(self, chart):
        if not self._ready(chart):
            return
        report = self.report
        times = pd.to_datetime(report["times"], utc=True)
        channels = [row for row in report["channels"] if row["rule"] == "candidate"] or report["channels"][:8]
        if all(row.get("diagnosis") for row in channels):
            chart.message("Nenhuma intenção/direção disponível nos canais desta visão.\n"
                          + empty_summary(report, ""))
            return
        ax = chart.figure.add_subplot(111)
        from matplotlib.colors import ListedColormap, BoundaryNorm
        from matplotlib.patches import Patch
        columns = max(100, min(2000, int(chart.figure.bbox.width) - 220))
        data, boundaries = timeline_bins(channels, columns)
        cmap = ListedColormap([self.theme.colors["bad"], self.theme.colors["surface"],
                              self.theme.colors["good"], "#A274C4"]).with_extremes(bad=self.theme.colors["grid"])
        # Grade por instante nominal; datas mantidas nas dicas e nos detalhes do relatório.
        ax.imshow(np.ma.masked_invalid(data), aspect="auto", interpolation="nearest", cmap=cmap,
                  norm=BoundaryNorm([-1.5, -.5, .5, 1.5, 2.5], 4))
        ax.set_yticks(range(len(channels)), [row["timeframe"] + " · " + row["name"] for row in channels])
        positions = np.unique(np.linspace(0, data.shape[1] - 1, min(5, data.shape[1])).astype(int))
        ax.set_xticks(positions, [times[boundaries[i]].strftime("%d/%m %H:%M") for i in positions])
        charts.style_axis(ax, self.theme, "Intenções/direções por intervalo UTC · agregação somente visual", grid_axis=None)
        ax.legend(handles=[Patch(color=color, label=label) for color, label in
                  ((self.theme.colors["good"], "+1"), (self.theme.colors["bad"], "−1"),
                   ("#A274C4", "ambos no intervalo"), (self.theme.colors["grid"], "indisponível"))],
                  loc="upper right", fontsize=8, ncol=4, labelcolor=self.theme.colors["ink2"],
                  facecolor=self.theme.colors["surface"], edgecolor=self.theme.colors["border"])
        def hover(event):
            if event.inaxes != ax or event.xdata is None or event.ydata is None:
                return None
            i, j = int(round(event.ydata)), int(round(event.xdata))
            if 0 <= i < len(channels) and 0 <= j < data.shape[1]:
                start, end = boundaries[j:j + 2]
                return f"{times[start].isoformat()} a {times[end-1].isoformat()} · {channels[i]['id']}\n" \
                       f"{end-start} barras da grade · estado agregado={data[i,j]} · valores originais no JSON"
            return None
        chart.hover.append(hover)
        chart.draw()

    def _price(self, chart):
        if not self._ready(chart):
            return
        from matplotlib.collections import LineCollection
        from matplotlib.dates import AutoDateLocator, ConciseDateFormatter, date2num
        report = self.report
        channel = next((item for item in report["channels"] if item["id"] == self.detail_channel.get()), report["channels"][0])
        times = pd.to_datetime(report["times"], utc=True)
        x = date2num(times.to_pydatetime())
        price = np.array([[np.nan if v is None else v for v in row] for row in report["prices"]])
        ax, feature = chart.figure.subplots(2, 1, sharex=True, gridspec_kw=dict(height_ratios=[2, 1]))
        colors = [self.theme.colors["good"] if close >= op else self.theme.colors["bad"] for op, _, _, close in price]
        ax.add_collection(LineCollection([[(at, low), (at, high)] for at, (_, high, low, _) in zip(x, price)],
                                         colors=colors, linewidths=.7))
        ax.add_collection(LineCollection([[(at, op), (at, close)] for at, (op, _, _, close) in zip(x, price)],
                                         colors=colors, linewidths=2.2))
        ax.autoscale_view()
        values = np.array([np.nan if v is None else v for v in channel["values"]], dtype=float)
        if channel.get("diagnosis"):
            feature.text(.5, .5, f"{channel['diagnosis']['message']}\n{channel['diagnosis']['action']}",
                         transform=feature.transAxes, ha="center", va="center", wrap=True, fontsize=8,
                         color=self.theme.colors["ink2"])
        grid_tf = timeframes.parse(report["grid_timeframe"])
        expected = times[:-1] + (pd.offsets.MonthBegin(1) if grid_tf.name == "MN1" else pd.Timedelta(minutes=grid_tf.minutes))
        splits = np.flatnonzero(times[1:] != expected) + 1
        for positions in np.split(np.arange(len(times)), splits):
            # Segmento de uma barra não forma linha; o marcador evita sumir com o valor.
            marker = "o" if np.isfinite(values[positions]).sum() == 1 else None
            feature.plot(times[positions], values[positions], color=self.theme.colors["accent"], linewidth=1.1,
                         marker=marker, markersize=3)
            if channel["rule"] == "price":
                ax.plot(times[positions], values[positions], color=self.theme.colors["accent"], linewidth=1.2,
                        marker=marker, markersize=3)
        if channel["rule"] == "candidate":
            first_source = ~pd.Series(channel["source_ends"]).duplicated().to_numpy()
            for side, marker in ((1, "^"), (-1, "v")):
                selected = (values == side) & first_source
                ax.scatter(x[selected], price[selected, 3], marker=marker,
                           color=self.theme.colors["good" if side == 1 else "bad"], s=18)
        locator = AutoDateLocator(minticks=3, maxticks=6)
        feature.xaxis.set_major_locator(locator)
        feature.xaxis.set_major_formatter(ConciseDateFormatter(locator))
        charts.style_axis(ax, self.theme, f"{report['symbol']} {report['grid_timeframe']} · candles da base")
        charts.style_axis(feature, self.theme, f"{channel['name']} {channel['timeframe']} · {channel['unit']}")
        chart.draw()

    def _relations(self, chart):
        if not self._ready(chart):
            return
        report = self.report
        ids = [row["id"] for row in report["channels"]]
        indices = {key: i for i, key in enumerate(ids)}
        values = np.full((len(ids), len(ids)), np.nan)
        pair_lookup = {}
        for pair in report["pairs"]:
            if pair["lag_bars"] == 0:
                i, j = indices[pair["left"]], indices[pair["right"]]
                values[i, j] = values[j, i] = pair["correlation"] if pair["correlation"] is not None else np.nan
                pair_lookup[i, j] = pair_lookup[j, i] = pair
        if np.isnan(values).all():
            chart.message("Nenhum par com correlação calculável nesta seleção.\n"
                          + empty_summary(self.report, "Amplie a janela, reduza TFs altos ou escolha canais com valores."))
            return
        ax = chart.figure.add_subplot(111)
        cmap = charts.diverging_cmap(self.theme).with_extremes(bad=self.theme.colors["grid"])
        im = ax.imshow(np.ma.masked_invalid(values), vmin=-1, vmax=1, cmap=cmap, interpolation="nearest")
        labels = [f"{i + 1} · {row['timeframe']}" for i, row in enumerate(report["channels"])]
        ax.set_yticks(range(len(ids)), labels)
        ax.set_xticks(range(len(ids)), [str(i + 1) for i in range(len(ids))])
        colorbar = chart.figure.colorbar(im, ax=ax, label="Correlação direcional (−1 a +1)")
        colorbar.ax.tick_params(colors=self.theme.colors["ink2"])
        colorbar.ax.yaxis.label.set_color(self.theme.colors["ink2"])
        charts.style_axis(ax, self.theme, "Direção canônica, não níveis brutos · detalhes ao apontar", grid_axis=None)
        def hover(event):
            if event.inaxes != ax or event.xdata is None or event.ydata is None:
                return None
            pair = pair_lookup.get((int(round(event.ydata)), int(round(event.xdata))))
            if pair:
                return f"{pair['left']} × {pair['right']}\nr={pair['correlation']} · n={pair['n_pairs']} · " \
                       f"pares de origem={pair['unique_source_pairs']} · {pair['reason'] or 'exploratório'}"
            return None
        chart.hover.append(hover)
        chart.draw()

    def _lags(self, chart):
        if not self._ready(chart):
            return
        ax = chart.figure.add_subplot(111)
        # Não ordenar pelo melhor efeito: ordem do catálogo/TF, seleção descritiva explícita.
        selected = self.detail_channel.get()
        pairs = [pair for pair in self.report["pairs"] if pair["lag_bars"] == 1 and
                 pair["left"] == selected and pair["left"] != pair["right"]][:5]
        for i, pair in enumerate(pairs):
            rows = [row for row in self.report["pairs"] if (row["left"], row["right"]) == (pair["left"], pair["right"])]
            zero = next((row for row in self.report["pairs"] if row["lag_bars"] == 0 and
                         {row["left"], row["right"]} == {pair["left"], pair["right"]}), None)
            if zero is not None and not any(row["lag_bars"] == 0 for row in rows):
                rows = [zero, *rows]
            ax.plot([row["lag_bars"] for row in rows],
                    [np.nan if row["correlation"] is None else row["correlation"] for row in rows],
                    marker="o", linewidth=1.4, label=f"{pair['left']} → {pair['right']}")
        ax.axhline(0, color=self.theme.colors["muted"], linewidth=.7)
        ax.set_ylim(-1.05, 1.05)
        ax.set_xticks([0, 1, 3])
        ax.set_xlabel("Defasagem do preditor (barras da grade)")
        ax.xaxis.label.set_color(self.theme.colors["ink2"])
        if pairs:
            ax.legend(fontsize=7, loc="best", labelcolor=self.theme.colors["ink2"],
                      facecolor=self.theme.colors["surface"], edgecolor=self.theme.colors["border"])
        charts.style_axis(ax, self.theme, "Preditor selecionado → primeiros 5 canais · sem ranking")
        chart.draw()

    def _coverage(self, chart):
        if not self._ready(chart):
            return
        channels = self.report["channels"]
        ax = chart.figure.add_subplot(111)
        labels = [f"{i+1} · {row['timeframe']} · {row['name']}" for i, row in enumerate(channels)]
        ax.barh(range(len(channels)), [row["coverage"] * 100 for row in channels], color=self.theme.colors["accent"])
        ax.set_yticks(range(len(channels)), labels, fontsize=8)
        for i, row in enumerate(channels):
            if row.get("diagnosis"):
                ax.text(1, i, row["diagnosis"]["code"] + " · " + row["diagnosis"]["action"], va="center",
                        fontsize=7, color=self.theme.colors["ink2"])
        ax.invert_yaxis()
        ax.set_xlim(0, 100)
        ax.set_xlabel("Barras da grade com valor disponível (%)")
        ax.xaxis.label.set_color(self.theme.colors["ink2"])
        charts.style_axis(ax, self.theme, "Cobertura de cálculo · não certifica a fonte", grid_axis="x")
        chart.draw()

    def export(self):
        if self.report is None:
            self.status.configure(text="Congele uma análise antes de exportar.")
            return
        path = filedialog.asksaveasfilename(parent=self.frame, defaultextension=".json",
                                           initialfile="laboratorio-" + self.report["analysis_id"][:12] + ".json")
        if path:
            try:
                with open(path, "x", encoding="utf-8") as handle:
                    json.dump(self.report, handle, ensure_ascii=False, allow_nan=False)
            except (OSError, ValueError) as error:
                self.status.configure(text=f"Exportação recusada; escolha um arquivo novo: {error}")

    def open_presets(self):
        """Conecta ao gerador Default/Manual existente; nenhuma inferência financeira na UI."""
        from mt5_extracao.presets import NATIVE_PRESET_KEYS
        if self.report is None:
            self.status.configure(text="Congele uma análise para levar seu ativo/TF ao gerador de presets.")
            return
        robots_panel = getattr(self.ui, "robots_panel", None)
        robots = [robot for robot in getattr(robots_panel, "robots", []) if robot.key in NATIVE_PRESET_KEYS]
        if not robots:
            self.status.configure(text="Nenhum perfil nativo no catálogo atual. Atualize a aba Robôs; "
                "somente perfis com fonte/pin/inputs compatíveis geram presets operáveis.")
            return
        context = self.report
        window = tk.Toplevel(self.frame)
        window.title("Gerador padronizado · Default / Manual")
        window.transient(self.frame.winfo_toplevel())
        body = ttk.Frame(window, padding=16)
        body.pack(fill=tk.BOTH, expand=True)
        ttk.Label(body, text=f"Estudo {context['analysis_id'][:12]} · {context['symbol']}", style="Heading.TLabel").pack(anchor="w")
        ttk.Label(body, text="O editor existente valida o perfil, conserva revisões e exporta .set + manifesto. "
            "Revise os parâmetros: correlações não viram regras ou aprovação automática.",
            style="Muted.TLabel", wraplength=560).pack(fill=tk.X, pady=8)
        chosen = tk.StringVar(value=robots[0].key)
        ttk.Combobox(body, textvariable=chosen, values=[r.key for r in robots], state="readonly", width=45).pack(fill=tk.X)
        tfs = [row["timeframe"] for row in context["populations"]]
        tf = tk.StringVar(value=tfs[0])
        ttk.Combobox(body, textvariable=tf, values=tfs, state="readonly").pack(fill=tk.X, pady=8)
        def open_editor():
            robot = next(robot for robot in robots if robot.key == chosen.get())
            controls = robots_panel.controls
            previous = controls.chosen.get(robot.key)
            controls.chosen[robot.key] = context["symbol"], tf.get(), None
            editor = controls.open_presets(robot)  # revalida fonte antes de construir o editor.
            if editor is None:
                if previous is None:
                    controls.chosen.pop(robot.key, None)
                else:
                    controls.chosen[robot.key] = previous
            else:
                window.destroy()
        ttk.Button(body, text="Abrir gerador Default / Manual", command=open_editor, style="Accent.TButton").pack(anchor="e")
        return window

    def _destroyed(self, event):
        if event.widget is self.frame:
            self._request += 1
            for panel in self.panels.values():
                panel.restore()
