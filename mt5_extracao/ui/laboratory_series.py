"""Consumidor Tk do Laboratório de séries; cálculo e diagnóstico ficam no SDK."""
import json
import threading
import tkinter as tk
from tkinter import filedialog, ttk

import numpy as np
import pandas as pd

from mt5_extracao import timeframes
from mt5_extracao.laboratory import derived, service
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


def _draw_candles(ax, times, price, theme):
    """Cada candle é um segmento próprio. Um buraco de tempo não vira linha entre barras."""
    from matplotlib.collections import LineCollection
    from matplotlib.dates import date2num
    stamps = pd.to_datetime(times)
    x = date2num(stamps.to_pydatetime())
    ohlc = np.array([[np.nan if value is None else value for value in row] for row in price], dtype=float)
    colors = [theme.colors["good"] if close >= op else theme.colors["bad"] for op, _, _, close in ohlc]
    ax.add_collection(LineCollection([[(at, low), (at, high)] for at, (_, high, low, _) in zip(x, ohlc)],
                                     colors=colors, linewidths=.7))
    ax.add_collection(LineCollection([[(at, op), (at, close)] for at, (op, _, _, close) in zip(x, ohlc)],
                                     colors=colors, linewidths=2.2))
    span = (x[-1] - x[0]) if len(x) > 1 else 1 / 1440
    pad_x = max(span * 0.02, 1 / 1440)
    ax.set_xlim(x[0] - pad_x, x[-1] + pad_x)
    low, high = np.nanmin(ohlc[:, 2]), np.nanmax(ohlc[:, 1])
    pad = (high - low) * 0.05 or 1e-5
    ax.set_ylim(low - pad, high + pad)
    return x, ohlc


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


VERDICTS = {"HOLDOUT_CONSISTENT": "consistente fora", "HOLDOUT_SIGN_FLIP": "inverte fora",
            "HOLDOUT_WEAK": "fraca fora", "WITHIN_NULL": "dentro do acaso", "CI_INCLUDES_ZERO": "IC inclui zero", "NO_GAIN_OVER_PERSISTENCE": "não supera persistência", "HOLDOUT_INSUFFICIENT": "amostra curta", "FIT_WEAK": "fraca no ajuste",
            "INSUFFICIENT_PAIRS": "sem pares", "CONSTANT_SERIES": "série constante", "UNAVAILABLE": "indisponível",
            "NO_DIRECTION_RULE": "sem regra de direção"}
TOOLS = ("Navegar", "Selecionar/mover", "Linha horizontal", "Linha vertical", "Região", "Nota")
# Orçamento explícito de renderização: até 8 subjanelas (110 px cada) rolam na página;
# acima disso a figura fica ilegível e lenta no handheld, então o excedente é listado.
MAX_PANES = 8
PANE_HEIGHT = 110
UNIT_LABELS = {"points_0_100": "Oscilador 0–100", "fraction": "Fração do preço", "channel_fraction": "Posição no canal (0–1)",
               "price_units": "Unidades de preço", "ratio": "Razão", "candidate_-1_0_1": "Intenção −1/0/+1"}
NOTICE_TEXT = ("Reconstrução exploratória por fechamento nominal · sem prova de "
               "disponibilidade histórica · correlação não é acerto nem resultado de trades.")
STORED_NOTICE_TEXT = ("Exploração na grade de fechamentos · canais armazenados usam fatos/versões congelados "
    "e disponibilidade declarada em cada decisão. Preços e fórmulas locais continuam descritivos. "
    "Correlação não é acerto, lucro nem regra de compra/venda.")


def stored_choice_key(ident, version):
    return "stored:" + json.dumps([ident, version], ensure_ascii=False, separators=(",", ":"))


def comparison_arguments(strategy_key, params, extra_keys):
    """Lista vazia preserva a chamada de uma estratégia. Extras usam o padrão canônico de cada uma."""
    extras = []
    for key in extra_keys:
        if key == strategy_key or key in extras:
            continue
        extras.append(key)
    if not extras:
        return dict(strategy_key=strategy_key, strategy_params=params)
    items = [dict(key=strategy_key, params=params or {})] if strategy_key is not None else []
    items.extend(extras)
    return dict(strategies=items)


def intention_channels(report, selected_id, removed=(), hidden=()):
    """Intenções da grade e a selecionada de outro TF. Uma por estratégia; sem operação nem execução."""
    chosen, seen = [], set()
    for channel in report["channels"]:
        if channel.get("rule") != "candidate" or channel["id"] in removed:
            continue
        if channel["id"] in hidden and channel["id"] != selected_id:
            continue
        if channel["timeframe"] != report["grid_timeframe"] and channel["id"] != selected_id:
            continue
        if channel["id"] in seen:
            continue
        seen.add(channel["id"])
        chosen.append(channel)
    return chosen


def chart_groups(channels, hidden, selected_id, removed=()):
    """Unidade de preço sobrepõe candles; demais unidades em subjanelas; ocultas/removidas ficam fora."""
    overlay, panes, hidden_rows = [], {}, []
    for channel in channels:
        if channel["rule"] == "candidate" or channel["id"] in removed:
            continue
        if channel["id"] in hidden:
            hidden_rows.append(channel)
        elif channel["rule"] == "price":
            overlay.append(channel)
        else:
            panes.setdefault(channel["unit"], []).append(channel)
    ordered = sorted(panes.items(), key=lambda item: not any(row["id"] == selected_id for row in item[1]))
    return dict(overlay=overlay, panes=ordered[:MAX_PANES], hidden=hidden_rows,
                omitted=[unit for unit, _ in ordered[MAX_PANES:]])


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



def _events_text(events):
    if not events or not events["usable"]:
        return ""
    delay = events["delay_bars"]
    latest = events.get("latest")
    return (f"{events['confirmations']} pivôs confirmados, valor só na confirmação"
            + (f" (atraso {delay['min']}–{delay['max']} barras após a origem)" if delay else "")
            + (f", último há {events['bars_since_last_confirmation']} barras" if delay else "")
            + (f" · origem {latest['origin_bar_open_utc']}, confirmação {latest['confirmation_bar_open_utc']}, "
               f"disponível no fechamento nominal {latest['available_at_nominal_utc']} (recebimento não comprovado)"
               if latest else "") + " · ")


def event_sample_text(sample):
    if not sample:
        return ""
    counts = [f"{name}={sample[key]}" for name, key in (("esquerda", "left_events"), ("direita", "right_events"))
              if sample.get(key) is not None]
    return " · eventos distintos (confirmações representadas): " + ", ".join(counts) + " · contagem de barras não mede eventos independentes"

class AnalysisChart:
    """O detalhe amplia o mesmo DTO; fechar restaura a região e a seleção originais."""
    def __init__(self, parent, theme, title, renderer):
        self.theme, self.renderer, self.title = theme, renderer, title
        self.frame = ttk.Frame(parent, style="Surface.TFrame", padding=8)
        header = ttk.Frame(self.frame, style="Surface.TFrame")
        header.pack(fill=tk.X)
        self.heading = ttk.Label(header, text=title, style="Heading.TLabel")
        self.heading.pack(side=tk.LEFT)
        self.expand_button = ttk.Button(header, text="Ampliar ↗", command=self.expand)
        self.expand_button.pack(side=tk.RIGHT)
        self.chart = charts.Chart(self.frame, theme, height=3.2)
        self.chart.widget.configure(height=315)
        self.chart.widget.pack(fill=tk.BOTH, expand=True)
        self.popup = None
        self._resize_jobs = {}
        self.chart.canvas.mpl_connect("resize_event", lambda _: self._resized(self.chart))
        self.frame.bind("<Destroy>", self._destroyed, add="+")

    def set_heading(self, text):
        self.heading.configure(text=text)
        self.title = text

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
        self.derived_read = None
        self._price_heading = "Preço e indicador no mesmo relógio"
        self._request = 0
        self._selection_revision = 0
        self._columns = None
        self._inventory = []
        self.pending = False
        self._analysis_cancel=threading.Event()
        self._display_initialized = False
        self.catalog = service.catalog()
        self._build_controls()
        self.status = ttk.Label(self.frame, text="Escolha uma base, os TFs e os indicadores para congelar a análise.",
                                style="Muted.TLabel", wraplength=1000)
        self.status.pack(fill=tk.X, pady=4)
        self.notice = Notice(self.frame, NOTICE_TEXT, theme=self.theme)
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
        for key, title, render in (("price", self._price_heading, self._price),
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
        self.linked_selections=[];self.linked_target=None;self.timeline_basis="received";self.links_dialog=None
        actions = ttk.Frame(self.frame)
        actions.pack(fill=tk.X)
        self.analyze_button = ttk.Button(actions, text="Congelar e analisar", command=self.analyze, style="Accent.TButton")
        self.analyze_button.pack(side=tk.LEFT)
        self.reload_button = ttk.Button(actions, text="Atualizar fontes", command=self.reload)
        self.reload_button.pack(side=tk.LEFT, padx=4)
        self.observations_button=ttk.Button(actions,text='Séries por janela…',command=self.open_observations)
        self.observations_button.pack(side=tk.LEFT,padx=4)
        self.links_button=ttk.Button(actions,text="Fontes, candle e relatório…",command=self.open_links)
        self.links_button.pack(side=tk.LEFT,padx=4)
        ttk.Button(actions,text="Cancelar análise",command=self._analysis_cancel.set).pack(side=tk.LEFT,padx=4)
        self.extended = tk.BooleanVar(value=False)
        ttk.Checkbutton(actions, text="Visão ampliada", variable=self.extended,
                        command=self._layout).pack(side=tk.LEFT, padx=4)
        ttk.Button(actions, text="Exportar JSON", command=self.export).pack(side=tk.RIGHT)
        ttk.Button(actions, text="Abrir análise…", command=self.open_analysis).pack(side=tk.RIGHT, padx=4)
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
        ttk.Label(strategy, text="Comparar outras estratégias no mesmo ativo", ).pack(anchor="w", pady=(6, 0))
        ttk.Label(strategy, text="Cada marcada usa os parâmetros padrão. A de cima usa os campos editados. "
                  "Vazio compara só a escolhida. As marcas são intenção, não operação nem execução.",
                  style="Muted.TLabel", wraplength=330).pack(anchor="w")
        self.compare_list = tk.Listbox(strategy, selectmode=tk.MULTIPLE, exportselection=False, height=4)
        self._compare_keys = [row["key"] for row in self.catalog["strategies"]]
        for row in self.catalog["strategies"]:
            self.compare_list.insert(tk.END, f"{row['name']} · padrão")
        self.compare_list.pack(fill=tk.X)
        self.compare_list.bind("<<ListboxSelect>>", self._changed)
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
            ttk.Entry(cell, textvariable=variable, width=22).pack(fill=tk.X)
        ttk.Label(strategy, text="Janela vazia = toda a série (máx. 20 mil barras/TF).",
                  style="Muted.TLabel", wraplength=330).pack(anchor="w")
        ttk.Label(features, text="Indicadores do inventário · seleção múltipla").pack(anchor="w")
        self.feature_query = tk.StringVar()
        search = ttk.Entry(features, textvariable=self.feature_query)
        search.pack(fill=tk.X, pady=(2, 2))
        Tooltip(search, "Buscar por nome, id, família, unidade ou parâmetro; a seleção é mantida ao filtrar.")
        origin = ttk.Frame(features)
        origin.pack(fill=tk.X)
        self.feature_origin = tk.StringVar(value="Todas as origens")
        origin_box = ttk.Combobox(origin, textvariable=self.feature_origin, state="readonly", width=22,
            values=("Todas as origens", "Cálculo local", "Armazenados"))
        origin_box.pack(side=tk.LEFT, fill=tk.X, expand=True)
        origin_box.bind("<<ComboboxSelected>>", self._filter_features)
        self.clear_features_button = ttk.Button(origin, text="Limpar seleção", command=self._clear_features)
        self.clear_features_button.pack(side=tk.RIGHT)
        self.feature_hint = ttk.Label(features, text="", style="Muted.TLabel", wraplength=330)
        self.feature_hint.pack(anchor="w")
        feature_frame = ttk.Frame(features)
        feature_frame.pack(fill=tk.BOTH, expand=True)
        self.feature_list = tk.Listbox(feature_frame, selectmode=tk.MULTIPLE, exportselection=False, height=7)
        self.feature_list.grid(row=0, column=0, sticky="nsew")
        scrollbar = ttk.Scrollbar(feature_frame, command=self.feature_list.yview)
        scrollbar.grid(row=0, column=1, sticky="ns")
        horizontal = ttk.Scrollbar(feature_frame, orient=tk.HORIZONTAL, command=self.feature_list.xview)
        horizontal.grid(row=1, column=0, sticky="ew")
        feature_frame.columnconfigure(0, weight=1)
        feature_frame.rowconfigure(0, weight=1)
        self.feature_list.configure(yscrollcommand=scrollbar.set, xscrollcommand=horizontal.set)
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
        tools = ttk.Frame(self.frame)
        tools.pack(fill=tk.X, pady=2)
        inputs = ttk.Frame(tools)
        inputs.pack(fill=tk.X)
        ttk.Label(inputs, text="Ferramenta:").pack(side=tk.LEFT)
        self.tool = tk.StringVar(value=TOOLS[0])
        ttk.Combobox(inputs, textvariable=self.tool, state="readonly", width=16, values=TOOLS).pack(side=tk.LEFT, padx=4)
        ttk.Label(inputs, text="Texto:").pack(side=tk.LEFT)
        self.note_text = tk.StringVar()
        ttk.Entry(inputs, textvariable=self.note_text, width=24).pack(side=tk.LEFT, padx=4, fill=tk.X, expand=True)
        buttons = ttk.Frame(tools)
        buttons.pack(fill=tk.X)
        self.study_buttons = []
        for text, command in (("Aplicar texto", self.edit_selected_annotation),
                              ("Excluir anotação", self.remove_selected_annotation),
                              ("Desfazer", self.undo_annotation), ("Remover série", self.remove_detail_series),
                              ("Restaurar séries", self.restore_series), ("Exportar estudo", self.export_study),
                              ("Abrir estudo", self.open_study)):
            self.study_buttons.append(ttk.Button(buttons, text=text, command=command))

        def layout_study_buttons(event):
            # Refluxo: em tela estreita nenhum comando do estudo fica fora da área visível.
            columns = max(1, min(len(self.study_buttons), event.width // 150))
            for index, widget in enumerate(self.study_buttons):
                widget.grid(row=index // columns, column=index % columns, sticky="ew", padx=2, pady=1)
        buttons.bind("<Configure>", layout_study_buttons)
        self.study = None
        self._region_start = None
        self._drag = None
        self.channel_details = ttk.Label(self.frame, text="", style="Muted.TLabel", wraplength=1000)
        self.channel_details.pack(fill=tk.X, pady=4)
        self.frame.bind("<Configure>", self._wrap_details, add="+")
        for variable in (self.params, self.start, self.end):
            variable.trace_add("write", self._changed)
        self.controls.bind("<Configure>", self._layout_controls)

    def open_observations(self):
        existing=getattr(self,'observations',None)
        if existing is not None and existing.alive():existing.window.lift();return
        from mt5_extracao.ui.series_observations import SeriesObservations
        self.observations=SeriesObservations(self.frame,self.ui)

    def _filter_features(self, *_):
        from mt5_extracao.laboratory import inventory
        choices = [(row["feature_id"], row, False) for row in self.catalog["indicators"]]
        choices += [(stored_choice_key(row["feature_id"], row["algorithm_version"]), row, True)
                    for row in self.catalog.get("stored_indicators", [])]
        self._stored_choices = {key:(row["feature_id"], row["algorithm_version"]) for key, row, stored in choices if stored}
        origin = self.feature_origin.get()
        found = [(key, row, stored) for key, row, stored in choices if inventory._matches(row, self.feature_query.get())
                 and (origin == "Todas as origens" or stored == (origin == "Armazenados"))]
        self._visible_features = [key for key, row, stored in found]
        self.feature_list.delete(0, tk.END)
        for index, (key, row, stored) in enumerate(found):
            name = "Nome não verificável" if stored and row.get("name_status") == "UNVERIFIED_LEGACY_NAME" else row["name"]
            label = (f"{name} · {row['feature_id']} · versão {row['algorithm_version']} · {row['unit'] or 'unidade não declarada'} · armazenado"
                     if stored else f"{name} · {row['family']} · cálculo local")
            self.feature_list.insert(tk.END, label)
            if key in self._chosen_features:
                self.feature_list.selection_set(index)
        self.feature_hint.configure(text=("Nenhum indicador corresponde à busca/origem. " if not found else "") +
            f"{len(found)} de {len(choices)} opções · {len(self._chosen_features)} selecionados")

    def _clear_features(self):
        self._chosen_features.clear()
        self._filter_features()
        self._changed()

    def _features_selected(self, *_):
        selected = {self._visible_features[i] for i in self.feature_list.curselection()}
        self._chosen_features = (self._chosen_features - set(self._visible_features)) | selected
        self.feature_hint.configure(text=f"{len(self._visible_features)} opções visíveis · "
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
        self._reload_request = getattr(self, "_reload_request", 0) + 1
        request = self._reload_request
        self._loading_sources = True
        self.reload_button.configure(text="Lendo fontes…")
        self.reload_button.state(["disabled"])

        def work():
            db = self.ui.app.db_manager
            return dict(sources=derived.chart_sources(db), catalog=service.catalog(db))
        def done(result, error):
            if request != self._reload_request or not self.frame.winfo_exists():
                return
            self._loading_sources = False
            self.reload_button.configure(text="Atualizar fontes")
            self.reload_button.state(["!disabled"])
            if error:
                self.status.configure(text=str(error))
                return
            self.catalog = result["catalog"]
            self._filter_features()
            items = result["sources"]
            self._inventory = items
            symbols = sorted({item["symbol"] for item in items})
            self.symbol_box.configure(values=symbols)
            if self.symbol.get() not in symbols:
                self.symbol.set(symbols[0] if symbols else "")
            self._symbol_changed()
            if not symbols:
                self.status.configure(text="Base sem séries compatíveis com esta captura OHLCV. "
                    "Confira inventário/Analytics antes de extrair mais dados.")
        self.ui.run_bg(work, done)

    def _fitting_timeframes(self):
        """Seleção inicial: tempos nativos cuja série inteira cabe no congelamento."""
        native = [item for item in self._tf_items if not item.get("synthetic")]
        return [item["timeframe"] for item in native if int(item.get("rows") or 0) <= service.MAX_BARS][:3]

    def _tf_label(self, item):
        label = item["timeframe"] + (" · sintético" if item.get("synthetic") else "")
        rows = int(item.get("rows") or 0)
        if rows > service.MAX_BARS:
            label += f" · {rows} barras"
        return label

    def _symbol_changed(self, *_):
        previous = {self._tfs[i] for i in self.tf_list.curselection()} if hasattr(self, "_tfs") else set()
        self._tf_items = [item for item in self._inventory if item["symbol"] == self.symbol.get()]
        self._tfs = [item["timeframe"] for item in self._tf_items]
        self.tf_list.delete(0, tk.END)
        chosen = previous & set(self._tfs) or set(self._fitting_timeframes())
        for index, item in enumerate(self._tf_items):
            self.tf_list.insert(tk.END, self._tf_label(item))
            if item["timeframe"] in chosen:
                self.tf_list.selection_set(index)
        if chosen != previous:
            self._changed()
            skipped = [item for item in self._tf_items
                       if not item.get("synthetic") and int(item.get("rows") or 0) > service.MAX_BARS
                       and item["timeframe"] not in chosen]
            if skipped and not self.report:
                names = ", ".join(f"{item['timeframe']} ({item['rows']})" for item in skipped)
                verb = "ficaram" if len(skipped) > 1 else "ficou"
                self.status.configure(
                    text=f"{names} {verb} de fora da seleção inicial: a série inteira passa de "
                         f"{service.MAX_BARS} barras. Os tempos marcados cabem na janela. "
                         "Para incluir o tempo longo, marque-o e congele: o início das últimas "
                         f"{service.MAX_BARS} barras entra no campo De.")

    def analyze(self):
        from mt5_extracao.automation.protocol import loads
        try:
            symbol = self.symbol.get()
            if self.linked_selections and self.linked_target != symbol:
                raise ValueError("Os vínculos foram escolhidos para outro ativo. Retire-os ou refaça a associação explicitamente.")
            chosen = [self._tf_items[i] for i in self.tf_list.curselection()] if hasattr(self, "_tf_items") else []
            plan = derived.selection_plan(chosen)
            tfs = [item["timeframe"] for item in chosen]
            comparing = plan == "derived" and len(tfs) > 1
            if self._params_error and (plan != "derived" or comparing):
                raise ValueError(self._params_error)
            if not symbol or plan == "empty":
                raise ValueError("Escolha ativo e pelo menos um timeframe da base.")
            if plan == "mixed":
                raise ValueError(
                    "M7 e M9 não entram na captura nativa. Leia o sintético sozinho ou escolha só tempos do MT5.")
            stored_features = [pair for key, pair in self._stored_choices.items() if key in self._chosen_features]
            if self.linked_selections and plan != "native":
                raise ValueError("Vínculos usam snapshots de TFs nativos; M7/M9 continuam no percurso sintético próprio.")
            if stored_features and plan != "native":
                raise ValueError("Indicadores armazenados usam snapshots nativos nesta etapa; escolha TFs nativos "
                                 "ou consulte o contexto/Analytics do tempo sintético.")
            if plan == "native" and not (self.start.get().strip() or self.end.get().strip()):
                oversized = [item for item in chosen if int(item.get("rows") or 0) > service.MAX_BARS]
                if oversized:
                    item = max(oversized, key=lambda row: int(row.get("rows") or 0))
                    bound = derived.earliest_of_last(self.ui.app.db_manager, symbol, item["timeframe"])
                    if bound:
                        self.start.set(bound)
                    raise ValueError(
                        f"{item['timeframe']}: {int(item['rows'])} barras na janela; o limite é {service.MAX_BARS}. "
                        + (f"O campo De foi preenchido com {bound} (últimas {service.MAX_BARS} barras). "
                           "Confira e congele de novo." if bound else
                           f"Informe um início mais recente ou retire {item['timeframe']} da seleção."))
            feature_ids, request_kwargs = [], {}
            if plan == "native" or comparing:
                params = loads(self.params.get())
                if not isinstance(params, dict):
                    raise ValueError("Parâmetros exigem objeto JSON.")
                feature_ids = [item["feature_id"] for item in self.catalog["indicators"]
                               if item["feature_id"] in self._chosen_features]
                strategy = None if self.strategy.get() == "Sem estratégia" else self.strategy.get()
                extras = [self._compare_keys[index] for index in self.compare_list.curselection()]
                request_kwargs = comparison_arguments(strategy, params, extras)
                n_strategies = (len(request_kwargs["strategies"]) if "strategies" in request_kwargs
                                else bool(request_kwargs.get("strategy_key")))
                if not feature_ids and not stored_features and not n_strategies and not self.linked_selections:
                    raise ValueError("Escolha indicadores ou uma estratégia para analisar.")
                gate = "M1" if comparing else tfs[0]
                if strategy is not None:
                    service.base_composition(strategy, symbol=symbol, timeframe=gate, params=params)
                for key in request_kwargs.get("strategies", []):
                    if isinstance(key, str):
                        service.base_composition(key, symbol=symbol, timeframe=gate,
                                                 params=service.get_strategy(key).params)
                if (len(feature_ids) + len(stored_features) + n_strategies) * len(tfs) + sum(len(s["features"]) for s in self.linked_selections) > service.MAX_CHANNELS:
                    raise ValueError("Máximo de 32 canais; reduza indicadores ou TFs para este recorte.")
        except (ValueError, AttributeError) as error:
            self.status.configure(text=str(error))
            return
        self._request += 1
        request, revision = self._request, self._selection_revision
        start, end = self.start.get() or None, self.end.get() or None
        linked_selections=json.loads(json.dumps(self.linked_selections));basis=self.timeline_basis
        self.pending = True
        self._analysis_cancel.clear()
        self.analyze_button.state(["disabled"])
        if plan == "derived" and len(tfs) > 1:
            self.status.configure(text="Comparando as barras já gravadas na grade do tempo mais curto. "
                                  "Esta leitura não cria candles nem congela snapshot.")
        elif plan == "derived":
            self.status.configure(text="Lendo as barras já gravadas. Esta leitura não agrega candles nem congela snapshot.")
        else:
            self.status.configure(text="Congelando a seleção pela API de snapshots e calculando os cruzamentos…")
        def work():
            from mt5_extracao.analytics.indicator_materializer import Cancelled
            def guard():
                if self._analysis_cancel.is_set():raise Cancelled("Análise cancelada; snapshots confirmados preservados.")
            guard()
            if plan == "derived" and len(tfs) == 1:
                return derived.read_stored(self.ui.app.db_manager, symbol, tfs, start=start, end=end)
            if plan == "derived":
                return derived.compare_stored(self.ui.app.db_manager, symbol, tfs, start=start, end=end,
                    feature_ids=feature_ids, as_of=pd.Timestamp.now(tz="UTC").isoformat(), **request_kwargs)
            snapshots = service.capture_selection(self.ui.app.db_manager, symbol=symbol,
                timeframes_selected=tfs, start=start, end=end, stored_features=stored_features,guard=guard)
            linked=service.capture_linked_sources(self.ui.app.db_manager,target_symbol=symbol,selections=linked_selections,
                                                  cancel_check=self._analysis_cancel.is_set)
            return service.analyze(snapshots, feature_ids=feature_ids,
                as_of=pd.Timestamp.now(tz="UTC").isoformat(), stored_features=stored_features,
                linked_sources=linked,timeline_basis=basis,cancel_check=self._analysis_cancel.is_set,**request_kwargs)
        def done(report, error):
            if not self.frame.winfo_exists() or request != self._request:
                return
            self.pending = False
            self.analyze_button.state(["!disabled"])
            if self._analysis_cancel.is_set():
                self.status.configure(text="Análise cancelada; resultado anterior e snapshots confirmados preservados.")
            elif revision != self._selection_revision:
                self.status.configure(text="Seleção mudou durante o cálculo; resultado antigo não publicado. Analise a seleção atual.")
            elif error:
                self.status.configure(text=f"Análise indisponível: {error}")
            elif report.get("kind") == "stored_derived":
                self.set_derived(report)
            else:
                self.set_report(report)
        self.ui.run_bg(work, done)

    def set_report(self, report):
        from mt5_extracao.laboratory.annotations import Study
        self.derived_read = None
        self.panels["price"].set_heading(
            "Comparação sintética · fechamento do tempo mais curto" if report.get("synthetic") else self._price_heading)
        if self.study is None or self.study.analysis_id != report["analysis_id"]:
            self.study = Study.for_report(report)  # anotações nunca migram para outra análise
        self.report = report
        ids = [channel["id"] for channel in report["channels"]]
        self.channel_box.configure(values=ids)
        if self.detail_channel.get() not in ids:
            self.detail_channel.set(ids[0])
        comparison = report["selection"].get("comparison")
        compared = ""
        if comparison:
            names = ", ".join(row["key"] for row in report["selection"]["strategies"])
            compared = (f"\nIntenções comparadas ({names}) no {comparison['scope']}. "
                        f"{comparison['operation']}. {comparison['execution']}.")
        synthetic = ""
        if report.get("synthetic"):
            synthetic = (" " + report["note"] + " Comparação sincronizada no fechamento nominal do tempo "
                         "mais curto. Um intervalo sem grupo completo não vira candle.")
        self.notice.message.configure(text=(STORED_NOTICE_TEXT if report["selection"].get("stored_features") else NOTICE_TEXT) + synthetic + (
            " Comparação de intenções no mesmo recorte; operação e execução não entram neste estudo."
            if comparison else ""))
        self.status.configure(text=f"{report['symbol']} · {len(report['populations'])} TFs · "
            f"{len(report['channels'])} canais · {len(report['times'])} fechamentos {report['grid_timeframe']} · "
            f"estudo {report['analysis_id'][:12]} · corte {report['selection']['as_of']}"
            + compared
            + "".join(f"\n{row['id']}: {row['diagnosis']['message']} {row['diagnosis']['action']}"
                      for row in empty_channels(report)[:3])
            + (f"\n… e mais {len(empty_channels(report)) - 3} canais sem dados (ver Cobertura)."
               if len(empty_channels(report)) > 3 else ""))
        self._channel_changed(redraw=False)
        self._layout()
        self.redraw()

    def set_derived(self, read):
        """Desenha barras já gravadas. Não é análise, não calcula indicador e não cria candle."""
        self.report = None
        self.study = None
        self.derived_read = read
        self.panels["price"].set_heading("Barras gravadas · sintético · não é tempo do MT5")
        self.channel_box.configure(values=[])
        self.detail_channel.set("")
        names = ", ".join(f"{item['timeframe']} {item['rows']}" for item in read["series"])
        gaps = sum(item["gaps"] for item in read["series"])
        self.notice.message.configure(text=read["note"] +
            ". Leitura das barras já gravadas. Sem indicador, sem relação e sem operação.")
        self.status.configure(text=(
            f"{read['symbol']} · {names} barras completas · {gaps} intervalos sem grupo completo · "
            "leitura independente, sem comparação sincronizada · " + read["note"]))
        self.channel_details.configure(text="")
        self._layout()
        self.redraw()

    def _channel_changed(self, *_, redraw=True):
        if self.report is None:
            return
        row = next((row for row in self.report["channels"] if row["id"] == self.detail_channel.get()), self.report["channels"][0])
        check = row["eligibility"]
        role = ("intenção do gerador · operação ausente · execução ausente · "
                if row["rule"] == "candidate" else "")
        role += ("fatos congelados · disponibilidade declarada em cada decisão · "
                 if row.get("source_kind") == "stored" else "")
        presence = (f"fatos disponíveis {row['stored_available']}/{row['total']} decisões · "
                    f"{row['source_available']} observações usadas · {row['frozen_revision_count']} revisões congeladas · "
                    if row.get("source_kind") == "stored" else "")
        self.channel_details.configure(text=f"{row['id']} · {role}{row['unit'] or 'unidade não declarada'} · aquecimento "
            f"{row['warmup'] if row['warmup'] is not None else 'não declarado'} · "
            + presence + f"cobertura numérica {row['available']}/{row['total']} · {check['status']} · "
            + (f"{row['diagnosis']['message']} {row['diagnosis']['action']} · " if row.get("diagnosis") else "")
            + _events_text(row.get("events"))
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
            if key == "price" and self.report:
                panes = chart_groups(self.report["channels"], self.study.hidden, "", self.study.removed)["panes"]
                height += PANE_HEIGHT * len(panes)
            if key == "price" and self.derived_read:
                height += PANE_HEIGHT * max(0, len(self.derived_read["series"]) - 1)
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
        if not self._inventory and not getattr(self, "_loading_sources", False):
            self.reload()

    def redraw(self):
        if not self.frame.winfo_exists():
            return
        colors = self.theme.colors
        self.canvas.configure(background=colors["page"])
        for widget in (self.tf_list, self.feature_list, self.compare_list):
            widget.configure(background=colors["field"], foreground=colors["ink"],
                selectbackground=colors["select"], selectforeground=colors["ink"],
                highlightbackground=colors["border"], highlightthickness=1, borderwidth=0,
                font=self.theme.font, activestyle="none")
        for panel in self.panels.values():
            panel.redraw()

    def _ready(self, chart):
        if self.derived_read is not None and not self.report:
            chart.message("Leitura das barras gravadas em _bars_derived.\n"
                          "Sintético, âncora 00:00 da corretora, não é tempo do MT5.\n"
                          "Indicadores, relações e estratégias não entram neste recorte.")
            return False
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
        if all(row.get("source_kind") == "stored" for row in channels):
            chart.message("Canais armazenados não declaram regra de direção.\n"
                          "Nenhuma intenção de compra/venda foi inferida.\nVeja os valores em Preço e indicador.")
            return
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
        ax.set_yticks(range(len(channels)), [
            row["timeframe"] + " · " + row["name"] + (" · intenção" if row["rule"] == "candidate" else "")
            for row in channels])
        positions = np.unique(np.linspace(0, data.shape[1] - 1, min(5, data.shape[1])).astype(int))
        ax.set_xticks(positions, [times[boundaries[i]].strftime("%d/%m %H:%M") for i in positions])
        charts.style_axis(ax, self.theme, "Intenções do gerador por intervalo UTC · não são operação nem execução · "
                          "agregação somente visual", grid_axis=None)
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
        if self.derived_read is not None and not self.report:
            self._price_derived(chart)
            return
        if not self._ready(chart):
            return
        from matplotlib.dates import AutoDateLocator, ConciseDateFormatter
        report = self.report
        selected = next((item for item in report["channels"] if item["id"] == self.detail_channel.get()),
                        report["channels"][0])
        times = pd.to_datetime(report["times"], utc=True)
        groups = chart_groups(report["channels"], self.study.hidden, selected["id"], self.study.removed)
        ratios = [3] + [1] * len(groups["panes"])
        axes = np.atleast_1d(chart.figure.subplots(len(ratios), 1, sharex=True, gridspec_kw=dict(height_ratios=ratios)))
        ax = axes[0]
        x, price = _draw_candles(ax, times, report["prices"], self.theme)
        for period in service.source_periods(report,selected["id"]):
            begin,finish=pd.Timestamp(period["start"]),pd.Timestamp(period["end"])
            if finish<times[0] or begin>times[-1]:continue
            ax.axvspan(max(begin,times[0]),min(finish,times[-1]),color=self.theme.colors["accent"],alpha=.12,
                       hatch="////" if period["value"]=="recession" else None,zorder=.2)
            label={"recession":"Recessão","expansion":"Expansão"}.get(period["value"],period["value"])
            ax.text(max(begin,times[0]),.96,label+(" · referência retrospectiva" if period["retrospective"] else " · conhecido no recebimento"),transform=ax.get_xaxis_transform(),
                    va="top",fontsize=8,color=self.theme.colors["ink2"],clip_on=True)
        grid_tf = None if report.get("synthetic") else timeframes.parse(report["grid_timeframe"])
        if grid_tf is not None and grid_tf.name == "MN1":
            step = pd.offsets.MonthBegin(1)
        else:
            minutes = int(report["grid_minutes"]) if report.get("grid_minutes") else grid_tf.minutes
            step = pd.Timedelta(minutes=minutes)
        expected = times[:-1] + step
        splits = np.flatnonzero(times[1:] != expected) + 1
        palette = [self.theme.colors["accent"], *self.theme.colors["series"], self.theme.colors["warn"], self.theme.colors["ink2"]]
        chart._legend_lines = {}

        def plot(target, channel, color):
            values = np.array([np.nan if v is None else v for v in channel["values"]], dtype=float)
            first = None
            for positions in np.split(np.arange(len(times)), splits):
                # Segmento de uma barra não forma linha; o marcador evita sumir com o valor.
                marker = "o" if np.isfinite(values[positions]).sum() == 1 else None
                line, = target.plot(times[positions], values[positions], color=color, linewidth=1.1,
                                    marker=marker, markersize=3, label=None if first else f"{channel['timeframe']} · {channel['name']}")
                first = first or line
            return first, values
        for index, channel in enumerate(groups["overlay"]):
            line, _ = plot(ax, channel, palette[index % len(palette)])
            chart._legend_lines[line.get_label()] = channel["id"]
        for pane, (unit, channels) in zip(axes[1:], groups["panes"]):
            for index, channel in enumerate(channels):
                line, values = plot(pane, channel, palette[(index + 1) % len(palette)])
                chart._legend_lines[line.get_label()] = channel["id"]
                if channel.get("diagnosis") and not np.isfinite(values).any():
                    pane.text(.5, .5, f"{channel['diagnosis']['message']}\n{channel['diagnosis']['action']}",
                              transform=pane.transAxes, ha="center", va="center", wrap=True, fontsize=8,
                              color=self.theme.colors["ink2"])
            title = (unit or "Unidade não declarada") if any(c.get("source_kind") == "stored" for c in channels) \
                else UNIT_LABELS.get(unit, unit or "Unidade não declarada")
            charts.style_axis(pane, self.theme, title)
        intentions = intention_channels(report, selected["id"], self.study.removed, self.study.hidden)
        unmarked = []
        for index, channel in enumerate(intentions):
            values = np.array([np.nan if v is None else v for v in channel["values"]], dtype=float)
            first_source = ~pd.Series(channel["source_ends"]).duplicated().to_numpy()
            labeled = False
            for side, marker in ((1, "^"), (-1, "v")):
                chosen = (values == side) & first_source
                if not chosen.any():
                    continue
                color = (palette[index % len(palette)] if len(intentions) > 1
                         else self.theme.colors["good" if side == 1 else "bad"])
                ax.scatter(x[chosen], price[chosen, 3], marker=marker, color=color, s=18, zorder=4,
                           label=None if labeled else f"intenção · {channel['timeframe']} · {channel['name']}")
                labeled = True
            if not labeled and channel.get("diagnosis"):
                unmarked.append(channel)
        if unmarked:
            ax.text(0, 1.02, "Sem intenção utilizável: " + " · ".join(
                f"{channel['timeframe']} {channel['name']}: {channel['diagnosis']['message']}"
                for channel in unmarked[:2]), transform=ax.transAxes, ha="left", va="bottom", fontsize=7,
                color=self.theme.colors["warn"], clip_on=False)
        for target in axes:
            handles, labels = target.get_legend_handles_labels()
            if handles:
                legend = target.legend(fontsize=7, loc="upper left", labelcolor=self.theme.colors["ink2"],
                    facecolor=self.theme.colors["surface"], edgecolor=self.theme.colors["border"])
                for handle in legend.get_lines():
                    handle.set_picker(6)
        if groups["omitted"]:
            ax.text(0, 1.01, f"Acima de {MAX_PANES} subjanelas: " + ", ".join(groups["omitted"]) + " · oculte ou remova séries",
                    transform=ax.transAxes, ha="left", va="bottom", fontsize=7, color=self.theme.colors["muted"])
        if groups["hidden"] or self.study.removed:
            ax.text(1, 1.01, f"{len(groups['hidden'])} oculta(s) · {len(self.study.removed)} removida(s) · clique na legenda alterna",
                    transform=ax.transAxes, ha="right", va="bottom", fontsize=7, color=self.theme.colors["muted"])
        self._draw_annotations(ax)
        locator = AutoDateLocator(minticks=3, maxticks=6)
        axes[-1].xaxis.set_major_locator(locator)
        axes[-1].xaxis.set_major_formatter(ConciseDateFormatter(locator))
        if report.get("synthetic"):
            price_title = (f"{report['symbol']} {report['grid_timeframe']} · sintético · âncora 00:00 · "
                           "grade sincronizada · não é tempo do MT5")
        else:
            price_title = f"{report['symbol']} {report['grid_timeframe']} · candles da base"
        if intentions:
            price_title += " · marcas = intenção; operação e execução ausentes"
        charts.style_axis(ax, self.theme, price_title)
        if not getattr(chart, "_study_events", False):
            chart._study_events = True
            chart.canvas.mpl_connect("pick_event", lambda event, c=chart: self._legend_picked(c, event))
            chart.canvas.mpl_connect("button_press_event", lambda event, c=chart: self._chart_clicked(c, event))
            chart.canvas.mpl_connect("button_release_event", lambda event, c=chart: self._chart_released(c, event))
            chart.canvas.mpl_connect("motion_notify_event", lambda event, c=chart: self._cursor_moved(c, event))
        chart._price_axis = ax
        view = self.study.view
        if "xlim" in view:
            ax.set_xlim(*view["xlim"])  # eixo compartilhado: subjanelas seguem o mesmo zoom
        chart._cursors = [target.axvline(view.get("cursor", x[0]), color=self.theme.colors["muted"], linewidth=.7,
                                         visible="cursor" in view) for target in axes]
        chart.draw()
        ax.callbacks.connect("xlim_changed", self._zoom_changed)

    def _price_derived(self, chart):
        from matplotlib.dates import AutoDateLocator, ConciseDateFormatter
        read = self.derived_read
        chart.clear()
        axes = np.atleast_1d(chart.figure.subplots(len(read["series"]), 1, sharex=False))
        for ax, item in zip(axes, read["series"]):
            _draw_candles(ax, item["times"], item["prices"], self.theme)
            charts.style_axis(ax, self.theme,
                f"{read['symbol']} {item['timeframe']} · {read['note']} · "
                f"{item['rows']} barras · {item['gaps']} intervalos sem grupo · horário da corretora")
            locator = AutoDateLocator(minticks=3, maxticks=6)
            ax.xaxis.set_major_locator(locator)
            ax.xaxis.set_major_formatter(ConciseDateFormatter(locator))
        chart.draw()

    def _draw_annotations(self, ax):
        for item in self.study.items if self.study else ():
            color = self.theme.colors["accent" if item["id"] == self.study.selected else "warn"]
            if item["kind"] == "hline":
                ax.axhline(item["y"], color=color, linewidth=1, linestyle="--")
            elif item["kind"] == "vline":
                ax.axvline(item["x"], color=color, linewidth=1, linestyle="--")
            elif item["kind"] == "region":
                ax.axvspan(item["x0"], item["x1"], color=color, alpha=.12, linewidth=0)
            else:
                ax.annotate(item["text"], (item["x"], item["y"]), xytext=(6, 6), textcoords="offset points",
                            fontsize=8, color=self.theme.colors["ink"],
                            bbox=dict(boxstyle="round,pad=.2", fc=self.theme.colors["surface"], ec=color))
            if item["text"] and item["kind"] != "note":
                position = (item.get("x", item.get("x0")), item.get("y")) if item["kind"] != "hline" else None
                if item["kind"] == "hline":
                    ax.annotate(item["text"], (0, item["y"]), xycoords=("axes fraction", "data"), fontsize=7,
                                color=color, va="bottom")
                else:
                    ax.annotate(item["text"], (position[0], 1), xycoords=("data", "axes fraction"), fontsize=7,
                                color=color, va="top", rotation=90)

    def _legend_picked(self, chart, event):
        ident = getattr(chart, "_legend_lines", {}).get(event.artist.get_label())
        if ident is None or self.study is None:
            return
        self.study.toggle_series(ident)
        self.panels["price"].redraw()

    def _zoom_changed(self, ax):
        if self.study is not None:
            self.study.view["xlim"] = [float(value) for value in ax.get_xlim()]
            if self.panels["price"].popup is not None:
                # Ampliado e original mostram a mesma janela temporal.
                for chart in (self.panels["price"].chart, self.panels["price"].detail):
                    target = getattr(chart, "_price_axis", None)
                    if target is not None and target is not ax and tuple(target.get_xlim()) != tuple(ax.get_xlim()):
                        target.set_xlim(*ax.get_xlim())
                        chart.canvas.draw_idle()

    def _cursor_moved(self, chart, event):
        if self.study is None or event.inaxes is None or event.xdata is None or event.inaxes not in chart.figure.axes:
            return
        if self._drag is not None and self._drag[0] is chart:
            return
        self.study.view["cursor"] = float(event.xdata)
        for line in getattr(chart, "_cursors", ()):
            line.set_xdata([event.xdata, event.xdata])
            line.set_visible(True)
        chart.canvas.draw_idle()

    def _chart_clicked(self, chart, event):
        if getattr(event,"dblclick",False) and event.xdata is not None and self.report is not None:
            from matplotlib.dates import num2date
            dates=pd.to_datetime(self.report["times"],utc=True)
            index=int(np.argmin(abs(dates-pd.Timestamp(num2date(event.xdata)))))
            self.open_links(candle=index)
            return
        tool = self.tool.get()
        if self.study is None or tool == TOOLS[0] or event.inaxes is not getattr(chart, "_price_axis", None) \
                or event.xdata is None or event.ydata is None:
            return
        if tool == "Selecionar/mover":
            (x0, x1), (y0, y1) = event.inaxes.get_xlim(), event.inaxes.get_ylim()
            # Tolerância de ~1,5% da vista: independe do zoom e da unidade do eixo.
            ident = self.study.hit(event.xdata, event.ydata, abs(x1 - x0) * .015, abs(y1 - y0) * .03)
            self.study.select(ident)
            edge = ident is not None and self.study.edge(ident, event.xdata, abs(x1 - x0) * .015)
            self._drag = (chart, ident, event.xdata, event.ydata, edge or None) if ident is not None else None
            self.status.configure(text="Arraste para mover; Aplicar texto/Excluir agem na selecionada."
                                  if ident is not None else "Nenhuma anotação neste ponto.")
            self.panels["price"].redraw()
            return
        try:
            if tool == "Linha horizontal":
                self.study.add("hline", y=event.ydata, text=self.note_text.get())
            elif tool == "Linha vertical":
                self.study.add("vline", x=event.xdata, text=self.note_text.get())
            elif tool == "Nota":
                self.study.add("note", x=event.xdata, y=event.ydata, text=self.note_text.get())
            elif self._region_start is None:
                self._region_start = event.xdata
                self.status.configure(text="Região: clique no fim do intervalo.")
                return
            else:
                start, self._region_start = self._region_start, None
                self.study.add("region", x0=start, x1=event.xdata, text=self.note_text.get())
        except ValueError as error:
            self.status.configure(text=f"Anotação recusada: {error}")
            return
        self.panels["price"].redraw()

    def _chart_released(self, chart, event):
        drag, self._drag = self._drag, None
        if drag is None or drag[0] is not chart or event.xdata is None or event.ydata is None \
                or event.inaxes is not getattr(chart, "_price_axis", None):
            return
        _, ident, x, y, edge = drag
        if (event.xdata, event.ydata) == (x, y):
            return
        if edge:  # borda da região: redimensiona; o interior move a região inteira
            self._study_changed(lambda: self.study.resize(ident, edge, event.xdata))
        else:
            self._study_changed(lambda: self.study.move(ident, event.xdata - x, event.ydata - y))

    def _target_annotation(self):
        if not self.study.items:
            raise ValueError("Nenhuma anotação.")
        return self.study.selected if self.study.selected is not None else self.study.items[-1]["id"]

    def _study_changed(self, action):
        if self.study is None:
            self.status.configure(text="Congele uma análise antes de anotar.")
            return
        try:
            action()
        except ValueError as error:
            self.status.configure(text=str(error))
            return
        self._layout()  # remover/restaurar série muda o número de subjanelas
        self.panels["price"].redraw()

    def edit_selected_annotation(self):
        self._study_changed(lambda: self.study.edit(self._target_annotation(), text=self.note_text.get()))

    def remove_selected_annotation(self):
        self._study_changed(lambda: self.study.remove(self._target_annotation()))

    def remove_detail_series(self):
        """Tira a série da vista sem recongelar: o relatório e o analysis_id ficam intactos."""
        self._study_changed(lambda: self.study.remove_series(self.detail_channel.get()))

    def restore_series(self):
        self._study_changed(self.study.restore_series)

    def undo_annotation(self):
        self._study_changed(lambda: self.study.undo() or (_ for _ in ()).throw(ValueError("Nada a desfazer.")))

    def export_study(self, path=None):
        if self.study is None:
            self.status.configure(text="Congele uma análise antes de exportar o estudo.")
            return
        path = path or filedialog.asksaveasfilename(defaultextension=".json", filetypes=[("Estudo", "*.json")])
        if not path:
            return
        try:
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(self.study.to_json())
        except OSError as error:
            self.status.configure(text=f"Estudo não exportado: {error}")
            return
        self.status.configure(text=f"Estudo {self.study.analysis_id[:12]} exportado: {path}")

    def open_study(self, path=None):
        from mt5_extracao.laboratory.annotations import Study
        if self.report is None:
            self.status.configure(text="Congele a mesma análise antes de reabrir o estudo.")
            return
        path = path or filedialog.askopenfilename(filetypes=[("Estudo", "*.json")])
        if not path:
            return
        try:
            with open(path, encoding="utf-8") as handle:
                self.study = Study.from_json(handle.read(), self.report["analysis_id"],
                                             [channel["id"] for channel in self.report["channels"]])
        except (OSError, ValueError, KeyError, TypeError) as error:
            self.status.configure(text=f"Estudo não reaberto: {error}")
            return
        self.status.configure(text=f"Estudo reaberto com {len(self.study.items)} anotações.")
        self.panels["price"].redraw()

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
            if any(row.get("source_kind") == "stored" for row in report["channels"]):
                chart.message("Sem correlação direcional nesta seleção.\nCanais armazenados não declaram regra de compra/venda.\n"
                              "Veja valores e unidades no gráfico; Spearman dos valores em Relações e aderência.")
                return
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
                       f"pares de origem={pair['unique_source_pairs']} · {pair['reason'] or 'exploratório'}" \
                       + event_sample_text(pair.get("event_sample"))
            return None
        chart.hover.append(hover)
        chart.draw()

    def _lags(self, chart):
        if not self._ready(chart):
            return
        selected = self.detail_channel.get()
        if any(row["id"] == selected and row.get("source_kind") == "stored" for row in self.report["channels"]):
            chart.message("Sem regra de direção para o canal armazenado selecionado.\n"
                          "Defasagens direcionais não são inferidas;\nconsulte Spearman dos valores em Relações e aderência.")
            return
        ax = chart.figure.add_subplot(111)
        # Não ordenar pelo melhor efeito: ordem do catálogo/TF, seleção descritiva explícita.
        pairs = [pair for pair in self.report["pairs"] if pair["lag_bars"] == 1 and
                 pair["left"] == selected and pair["left"] != pair["right"]][:5]
        palette = [*self.theme.colors["series"], self.theme.colors["ink2"], self.theme.colors["warn"]]
        shown = []
        for i, pair in enumerate(pairs):
            rows = [row for row in self.report["pairs"] if (row["left"], row["right"]) == (pair["left"], pair["right"])]
            zero = next((row for row in self.report["pairs"] if row["lag_bars"] == 0 and
                         {row["left"], row["right"]} == {pair["left"], pair["right"]}), None)
            if zero is not None and not any(row["lag_bars"] == 0 for row in rows):
                rows = [zero, *rows]
            color = palette[i % len(palette)]
            descriptive = [row for row in rows if row["lag_bars"] == 0]
            predictive = [row for row in rows if row["lag_bars"] > 0]
            # Descritiva (defasagem 0) como quadrado isolado; preditiva só pelo corte temporal.
            ax.scatter([0] * len(descriptive), [np.nan if row["correlation"] is None else row["correlation"]
                       for row in descriptive], marker="s", color=color, s=22)
            ax.plot([row["lag_bars"] for row in predictive],
                    [np.nan if (row.get("out_of_sample") or {}).get("fit_correlation") is None
                     else row["out_of_sample"]["fit_correlation"] for row in predictive],
                    marker="o", linewidth=1.4, color=color, label=f"{pair['left']} → {pair['right']}")
            ax.plot([row["lag_bars"] for row in predictive],
                    [np.nan if (row.get("out_of_sample") or {}).get("holdout_correlation") is None
                     else row["out_of_sample"]["holdout_correlation"] for row in predictive],
                    marker="o", markerfacecolor="none", linestyle="--", linewidth=1, color=color)
            for row in predictive:
                interval = (row.get("out_of_sample") or {}).get("holdout_ci_95")
                if interval:
                    ax.vlines(row["lag_bars"] + .06 * i, *interval, color=color, linewidth=.8, alpha=.7)
            for row in predictive:
                verdict = (row.get("out_of_sample") or {}).get("verdict") or row.get("reason") or "UNAVAILABLE"
                ax.annotate(VERDICTS.get(verdict, verdict), (row["lag_bars"], -1.0), fontsize=6,
                            color=color, ha="center", va="bottom", xytext=(0, 8 * i), textcoords="offset points")
            shown.extend(rows)
        ax.axhline(0, color=self.theme.colors["muted"], linewidth=.7)
        ax.axvline(.5, color=self.theme.colors["border"], linewidth=.7)
        ax.set_ylim(-1.05, 1.05)
        ax.set_xticks([0, *service.PREDICTIVE_LAGS], ["0\ndescritiva", *(f"{lag}\npreditiva" for lag in service.PREDICTIVE_LAGS)])
        ax.set_xlabel("Defasagem do preditor (barras da grade) · ● ajuste 70% · ○ fora da amostra 30% · barra = IC 95% em blocos")
        ax.xaxis.label.set_color(self.theme.colors["ink2"])
        if pairs:
            ax.legend(fontsize=7, loc="upper right", labelcolor=self.theme.colors["ink2"],
                      facecolor=self.theme.colors["surface"], edgecolor=self.theme.colors["border"])
        else:
            ax.text(.5, .5, "Sem pares para o canal selecionado; escolha outro em Detalhar no gráfico.",
                    transform=ax.transAxes, ha="center", va="center", fontsize=8, color=self.theme.colors["ink2"])

        def hover(event):
            if event.inaxes is not ax or event.xdata is None:
                return None
            lag = min((0, *service.PREDICTIVE_LAGS), key=lambda value: abs(value - event.xdata))
            lines = []
            for row in shown:
                if row["lag_bars"] != lag:
                    continue
                split = row.get("out_of_sample")
                def r(value):
                    return "—" if value is None else f"{value:+.3f}"
                detail = (f"ajuste r={r(split['fit_correlation'])} n={split['fit_pairs']} · fora r="
                          f"{r(split['holdout_correlation'])} "
                          f"[{', '.join(r(v) for v in split.get('holdout_ci_95') or (None, None))}] n={split['holdout_pairs']} · acaso ±"
                          f"{'—' if split.get('null_band_95') is None else format(split['null_band_95'], '.3f')} · "
                          f"alvo defasado r={r(split.get('naive_holdout_correlation'))} · {VERDICTS[split['verdict']]}"
                          if split else f"r={r(row['correlation'])} n={row['n_pairs']} · descritiva")
                lines.append(f"{row['left']} → {row['right']}: {detail}"
                             + event_sample_text(row.get("event_sample"))
                             + ("\nFora da amostra" + event_sample_text(split.get("event_sample"))
                                if split and split.get("event_sample") else ""))
            return "\n".join(lines[:6]) or None
        chart.hover.append(hover)
        charts.style_axis(ax, self.theme, "Preditor selecionado → primeiros 5 canais · descritiva × preditiva · sem ranking")
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

    def open_links(self, candle=None):
        from .laboratory_links import LaboratoryLinks
        if self.links_dialog is None or not self.links_dialog.alive():
            self.links_dialog=LaboratoryLinks(self)
        else:self.links_dialog.window.lift()
        if candle is not None:
            self.links_dialog.tabs.select(self.links_dialog.candle_tab)
            self.links_dialog.show_candle(candle)

    def export(self):
        if self.report is None:
            if self.derived_read is not None:
                self.status.configure(text="Esta leitura não é uma análise congelada e não se exporta como estudo.")
            else:
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

    def open_analysis(self, path=None):
        """Reabre a análise exportada como foi congelada (mesmo analysis_id); não recongela nem recalcula."""
        path = path or filedialog.askopenfilename(parent=self.frame, filetypes=[("Análise JSON", "*.json")])
        if not path:
            return
        try:
            with open(path, encoding="utf-8") as handle:
                report = service.load_report(handle.read())
        except (OSError, ValueError) as error:
            self.status.configure(text=f"Análise não reaberta: {error}")
            return
        self._request += 1  # descarta cálculo em andamento: a análise aberta passa a ser a atual
        self._analysis_cancel.set()
        if report.get("laboratory_version") == 2:
            self.linked_selections=[dict(manifest_id=s["manifest_id"],features=s["features"],relationship=s["relationship"])
                                    for s in report["selection"]["linked_sources"]]
            self.linked_target=report["symbol"];self.timeline_basis=report["selection"]["timeline_basis"]
        self.pending = False
        self.analyze_button.state(["!disabled"])
        self.set_report(report)
        if report["formula_source_sha256"] != service.formula_source_hash():
            self.status.configure(text=self.status.cget("text") + " · fórmulas mudaram desde o congelamento; "
                                  "valores exibidos são os congelados.")

    def open_presets(self):
        """Conecta ao gerador Default/Manual existente; nenhuma inferência financeira na UI."""
        from mt5_extracao.presets import NATIVE_PRESET_KEYS
        if self.report is None or self.report.get("synthetic"):
            if self.derived_read is not None or (self.report or {}).get("synthetic"):
                self.status.configure(text="A leitura sintética não vai ao gerador de presets. Congele uma análise nativa.")
            else:
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
