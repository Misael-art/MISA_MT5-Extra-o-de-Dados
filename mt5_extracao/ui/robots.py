"""Aba "Robôs": catálogo em cards (MQL5 e Python) com tipo, risco, estatísticas e estágio; clique abre o painel."""
import logging
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from mt5_extracao import robots as robots_core
from mt5_extracao.robots import catalog, risk
from mt5_extracao.ui import icons
from mt5_extracao.ui.components import FilterBar, DetailsPanel, ScrollableContent
from mt5_extracao.ui.robot_detail import RISK_STYLES, RobotDetail, draw_equity
from mt5_extracao.ui.robot_controls import CardControls
from mt5_extracao.ui.theme import Tooltip, fmt_int, fmt_num
from mt5_extracao.ui.visual_tokens import result_attention_colors

log = logging.getLogger(__name__)

ALL = "Todos"
CARD_MIN_WIDTH = 310


def result_tiles(headline):
    """(valor, legenda) das estatísticas do card, a partir de Robot.headline."""
    return ((fmt_num(headline["profit_factor"]), "profit factor"),
            (fmt_int(headline["trades"]), "operações"),
            (fmt_num(headline["max_drawdown"] * 100 if headline["max_drawdown"] is not None else None, 1, "%"), "drawdown"),
            (fmt_num(headline["extra"] * 100 if headline["extra"] is not None else None, 0, "%"), headline["extra_label"]))


class RobotsPanel:
    def __init__(self, parent, ui):
        self.ui = ui
        self.theme = ui.theme
        self.robots = []
        self.visible = []        # robôs que passam nos filtros, na ordem dos cards
        self._asset_icons = {}   # referências vivas; imagens Tk permanecem na thread principal
        self.cards = {}          # chave do robô -> frame do card
        self.selected_key = None
        self._card_details = {}
        self._columns = 0
        self.frame = ttk.Frame(parent, padding=12)
        self.page_viewport = ScrollableContent(self.frame, self.theme)
        self.page_viewport.pack(fill=tk.BOTH, expand=True)
        page = self.page_viewport.content
        self.controls = CardControls(self)

        bar = FilterBar(page, padding=10)
        bar.pack(fill=tk.X)
        for column, weight in enumerate((0, 1, 1, 0)):
            bar.columnconfigure(column, weight=weight)
        self.import_button = ttk.Menubutton(bar, text="Importar robô ▾", style="Accent.TButton")
        menu = tk.Menu(self.import_button, tearoff=0)
        self.theme.style_menu(menu)
        menu.add_command(label="Arquivo do robô (.mq5 ou .ex5)...", command=self.import_file)
        menu.add_command(label="Pasta de robôs...", command=self.import_folder)
        menu.add_separator()
        menu.add_command(label="Reler o catálogo", command=self.reload)
        self.import_button["menu"] = menu
        self.import_button.grid(row=0, column=0, sticky="w")
        Tooltip(self.import_button, "Robôs MQL5 vêm de um arquivo ou de uma pasta no formato do MT5 (Experts, Include, "
                                    "Presets), como o clone do projeto HADOUKEN. Nada é copiado: o programa só lê.",
                self.theme)
        self.panel_button = ttk.Button(bar, text="Painel de resultados", style="Accent.TButton",
                                       command=ui.open_results_panel)
        self.panel_button.grid(row=0, column=3, sticky="e")
        Tooltip(self.panel_button, "Abre no navegador a visão de conjunto: todos os ativos com base, os timeframes e o "
                                   "resultado de cada robô ou estratégia em cada combinação. Só leitura.", self.theme)
        self.search_var = tk.StringVar()
        self.search_var.trace_add("write", lambda *_: self.render())
        search = ttk.Frame(bar, style="Surface.TFrame")
        self.filterbar = bar
        self.filterbar_search = search
        search.grid(row=0, column=1, columnspan=2, sticky="ew", padx=12)
        ttk.Label(search, text="Buscar", style="Muted.TLabel").pack(side=tk.LEFT, padx=(0, 6))
        search_entry = ttk.Entry(search, textvariable=self.search_var, width=18)
        search_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self.filters = {}
        self.filter_cells = []
        for index, (key, label, values, width) in enumerate((
                ("kind", "Tipo", (ALL, "MQL5", "Python"), 8),
                ("risk", "Risco", (ALL,) + risk.LEVELS, 13),
                ("stage", "Estágio", (ALL,) + tuple(catalog.STAGE_LABELS.values()), 11),
                ("regime", "Regime", (ALL,), 14))):
            cell = ttk.Frame(bar, style="Surface.TFrame")
            cell.grid(row=1, column=index, sticky="ew", padx=(0 if index == 0 else 8, 0), pady=(8, 0))
            self.filter_cells.append(cell)
            cell.columnconfigure(1, weight=1)
            ttk.Label(cell, text=label, style="Muted.TLabel").grid(row=0, column=0, sticky="w", padx=(0, 6))
            var = tk.StringVar(value=ALL)
            combo = ttk.Combobox(cell, textvariable=var, values=values, state="readonly", width=width)
            combo.grid(row=0, column=1, sticky="w")
            combo.bind("<<ComboboxSelected>>", lambda e: self.render())
            self.filters[key] = (var, combo)
        self._filterbar_stacked = None
        bar.bind("<Configure>", self._responsive_filterbar, add="+")

        self.summary = ttk.Label(page, text="Lendo o catálogo...", style="Page.Muted.TLabel")
        self.summary.pack(anchor="w", pady=(8, 6))

        body_view = ttk.Frame(page)
        body_view.pack(fill=tk.BOTH, expand=True)
        self.body_canvas = tk.Canvas(body_view, highlightthickness=0, borderwidth=0,
                                     background=self.theme.colors["page"])
        self.body_scrollbar = ttk.Scrollbar(body_view, orient="vertical", command=self.body_canvas.yview)
        self.body_canvas.configure(yscrollcommand=self.body_scrollbar.set)
        self.body_canvas.pack(fill=tk.BOTH, expand=True)
        body = ttk.Frame(self.body_canvas)
        self.body = body
        self._body_window = self.body_canvas.create_window(0, 0, window=body, anchor="nw")
        body.bind("<Configure>", self._sync_body_viewport, add="+")
        self.body_canvas.bind("<Configure>", self._sync_body_viewport, add="+")
        body.columnconfigure(0, weight=3, uniform="robots")
        body.columnconfigure(1, weight=2, uniform="robots")
        body.rowconfigure(0, weight=1)

        grid_frame = ttk.Frame(body)
        grid_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        self.canvas = tk.Canvas(grid_frame, highlightthickness=0, borderwidth=0,
                                background=self.theme.colors["page"])
        scrollbar = ttk.Scrollbar(grid_frame, orient="vertical", command=self.canvas.yview)
        self.canvas.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.inner = ttk.Frame(self.canvas)
        self._window = self.canvas.create_window(0, 0, window=self.inner, anchor="nw")
        self.inner.bind("<Configure>", lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all")))
        self.canvas.bind("<Configure>", self._resized)
        self._bind_wheel(self.canvas)

        self.detail = RobotDetail(body, ui)
        self.detail.source_combo.bind("<<ComboboxSelected>>", self._detail_preset_changed, add="+")
        self.detail.frame.grid(row=0, column=1, sticky="nsew")
        self._body_stacked = None
        body.bind("<Configure>", self._responsive_body, add="+")
        self.theme.on_change(self._theme_changed)

    def _sync_body_viewport(self, _event=None):
        canvas_width = self.body_canvas.winfo_width()
        canvas_height = self.body_canvas.winfo_height()
        content_height = self.body.winfo_reqheight()
        if canvas_width <= 1 or canvas_height <= 1:
            return
        needs_scroll = self._body_stacked and content_height > canvas_height + 4
        height = max(canvas_height, content_height) if needs_scroll else canvas_height
        self.body_canvas.itemconfigure(self._body_window, width=canvas_width, height=height)
        self.body_canvas.configure(scrollregion=(0, 0, canvas_width, height))
        if needs_scroll:
            if not self.body_scrollbar.winfo_manager():
                self.body_scrollbar.pack(side=tk.RIGHT, fill=tk.Y, before=self.body_canvas)
        elif self.body_scrollbar.winfo_manager():
            self.body_scrollbar.pack_forget()

    def _responsive_filterbar(self, event):
        stacked = event.width < 850
        if stacked == self._filterbar_stacked:
            return
        self._filterbar_stacked = stacked
        if stacked:
            for column in range(4):
                self.filterbar.columnconfigure(column, weight=1)
            self.filterbar_search.grid_configure(row=1, column=0, columnspan=4, padx=0, pady=(8, 0))
            for index, cell in enumerate(self.filter_cells):
                cell.grid_configure(row=2 + index // 2, column=(index % 2) * 2, columnspan=2,
                                    padx=(0, 8 if index % 2 == 0 else 0), pady=(8, 0))
        else:
            for column, weight in enumerate((0, 1, 1, 0)):
                self.filterbar.columnconfigure(column, weight=weight)
            self.filterbar_search.grid_configure(row=0, column=1, columnspan=2, padx=12, pady=0)
            for index, cell in enumerate(self.filter_cells):
                cell.grid_configure(row=1, column=index, columnspan=1,
                                    padx=(0 if index == 0 else 8, 0), pady=(8, 0))

    def _responsive_body(self, event):
        stacked = event.width < 980
        if stacked == self._body_stacked:
            return
        self._body_stacked = stacked
        if stacked:
            self.body.columnconfigure(0, weight=1, uniform="")
            self.body.columnconfigure(1, weight=0, uniform="")
            self.body.rowconfigure(0, weight=3, minsize=250)
            self.body.rowconfigure(1, weight=2, minsize=300)
            self.detail.frame.grid_propagate(False)
            self.canvas.master.grid_configure(row=0, column=0, columnspan=2, sticky="nsew", padx=0, pady=(0, 8))
            self.detail.frame.grid_configure(row=1, column=0, columnspan=2, sticky="nsew")
        else:
            self.body.columnconfigure(0, weight=3, uniform="robots")
            self.body.columnconfigure(1, weight=2, uniform="robots")
            self.body.rowconfigure(0, weight=1, minsize=0)
            self.body.rowconfigure(1, weight=0, minsize=0)
            self.detail.frame.grid_propagate(True)
            self.canvas.master.grid_configure(row=0, column=0, columnspan=1, sticky="nsew", padx=(0, 10), pady=0)
            self.detail.frame.grid_configure(row=0, column=1, columnspan=1, sticky="nsew")
        self._sync_body_viewport()

    # --- dados ------------------------------------------------------------------------

    def reload(self):
        """Lê o catálogo em segundo plano (arquivos e banco) e redesenha os cards."""
        config = getattr(self.ui.app, "config", None)
        db = getattr(self.ui.app, "db_manager", None)
        self.summary.config(text="Lendo o catálogo...")
        self.ui.run_bg(lambda: robots_core.load_catalog(config, db), self._loaded)

    def _loaded(self, items, error):
        if error:
            self.summary.config(text=f"Não foi possível ler o catálogo de robôs: {error}")
            return
        # Robôs do MT5 primeiro (são os que operam); a ordem de leitura é mantida dentro de cada tipo
        self.robots = sorted(items, key=lambda robot: robot.kind != catalog.MQL5)
        regimes = sorted({r.regime for r in items if r.regime})
        self.filters["regime"][1].configure(values=(ALL,) + tuple(regimes))
        self.render()

    def _matches(self, robot):
        text = self.search_var.get().strip().lower()
        if text and text not in f"{robot.name} {robot.key} {robot.description} {robot.markets}".lower():
            return False
        wanted = {key: var.get() for key, (var, _combo) in self.filters.items()}
        return (wanted["kind"] in (ALL, robot.kind_label) and wanted["risk"] in (ALL, robot.risk.level)
                and wanted["stage"] in (ALL, robot.stage_label) and wanted["regime"] in (ALL, robot.regime))

    # --- cards ------------------------------------------------------------------------

    def render(self):
        for child in self.inner.winfo_children():
            child.destroy()
        self.cards = {}
        self.controls.widgets = {}
        self.visible = [r for r in self.robots if self._matches(r)]
        mql5 = sum(r.kind == catalog.MQL5 for r in self.robots)
        text = (f"{len(self.robots)} robô(s): {mql5} MQL5 e {len(self.robots) - mql5} Python. "
                f"Mostrando {len(self.visible)}. Clique em um card para ver os detalhes.")
        if not mql5:
            text += " Para trazer robôs do MT5, use \"Importar robô\"."
        self.summary.config(text=text)
        if not self.visible:
            ttk.Label(self.inner, text="Nenhum robô com esses filtros.", style="Page.Muted.TLabel").grid(
                row=0, column=0, padx=8, pady=20, sticky="w")
        for robot in self.visible:
            self.cards[robot.key] = self._build_card(robot)
        self._columns = 0
        self._layout()
        keys = [r.key for r in self.visible]
        if self.selected_key not in keys:
            self.selected_key = keys[0] if keys else None
        self._mark_selected()
        self.detail.show(next((r for r in self.visible if r.key == self.selected_key), None))
        self._sync_detail_selection()
        self.canvas.after_idle(lambda: self._reveal(self.selected_key))

    def _build_card(self, robot):
        outer = ttk.Frame(self.inner, style="Card.TFrame", padding=12)
        wrap = CARD_MIN_WIDTH - 40

        head = ttk.Frame(outer, style="Surface.TFrame")
        head.pack(fill=tk.X)
        ttk.Label(head, text=robot.kind_label, style="Badge.TLabel").pack(side=tk.RIGHT, anchor="n")
        ttk.Label(head, text=robot.name, style="Heading.TLabel", wraplength=wrap - 60, justify=tk.LEFT).pack(
            side=tk.LEFT, anchor="w")
        facts = [f"v{robot.version}" if robot.version else "", robot.regime, ", ".join(robot.timeframes)]
        ttk.Label(outer, text=" · ".join(f for f in facts if f) or "tipo de estratégia não informado",
                  style="Muted.TLabel", wraplength=wrap, justify=tk.LEFT).pack(anchor="w", pady=(2, 8))

        context = self.controls.context(robot) if robot.kind == catalog.MQL5 else None
        if context is not None:
            self.controls.build(outer, robot)
        rating = context["risk"] if context else robot.risk
        icon, label, style = RISK_STYLES[rating.level]
        ttk.Label(outer, text=f"{icon} {label}", style=style).pack(anchor="w")
        reasons = rating.summary
        if robot.preset_risks:
            levels = [r.level for r in robot.preset_risks.values()]
            ordered = [level for level in risk.LEVELS if level in levels]
            span = ordered[0] if len(ordered) == 1 else f"{ordered[0]} a {ordered[-1]}"
            reasons += f"\n{len(levels)} preset(s): risco {span}"
        ttk.Separator(outer).pack(fill=tk.X, pady=(0, 8))
        headline = context["headline"] if context else robot.headline
        if headline:
            tiles = ttk.Frame(outer, style="Surface.TFrame")
            tiles.pack(fill=tk.X)
            Tooltip(tiles, "Validação: fora da amostra e com custos (profit factor, operações, drawdown no cenário "
                           "ruim do Monte Carlo e janelas walk-forward com lucro). Backtest: um único período, "
                           "serve para estudar, não aprova a estratégia.", self.theme)
            for column, (value, caption) in enumerate(result_tiles(headline)):
                tiles.columnconfigure(column, weight=1, uniform="tile")
                ttk.Label(tiles, text=value, style="Heading.TLabel").grid(row=0, column=column, sticky="w")
                short = {"profit factor": "PF", "operações": "Oper.", "drawdown": "Queda"}.get(caption, caption)
                label = ttk.Label(tiles, text=short, style="Muted.TLabel", wraplength=70, justify=tk.LEFT)
                label.grid(row=1, column=column, sticky="nw")
                Tooltip(label, {"profit factor": "Fator de lucro", "drawdown": "Queda máxima"}.get(caption, caption), self.theme)
            shown_run = context["run"] if context else (robot.runs[0] if robot.runs else None)
            if shown_run and len(shown_run.get("equity") or []) > 2:
                spark = tk.Canvas(outer, height=30, highlightthickness=0, borderwidth=0,
                                  background=self.theme.colors["surface"])
                spark.pack(fill=tk.X, pady=(6, 0))
                spark.bind("<Configure>", lambda e, c=spark, run=shown_run: draw_equity(
                    c, run["equity"], self.theme, labels=False))
                if headline["kind"] != "backtest":      # os números acima vêm da validação, a curva não
                    last = robot.runs[0]
                    ttk.Label(outer, text=f"curva: último backtest em {last['symbol']} {last['timeframe']}",
                              style="Muted.TLabel", wraplength=wrap, justify=tk.LEFT).pack(anchor="w")
            where = f"em {headline['symbol']} {headline['timeframe']}"
            more = f" · mais {headline['count'] - 1} teste(s)" if headline["count"] > 1 else ""
            if headline["approved"] is None:
                ttk.Label(outer, text=f"Último backtest {where}{more}", style="Muted.TLabel", wraplength=wrap,
                          justify=tk.LEFT).pack(anchor="w", pady=(6, 0))
            else:
                verdict = "✓ aprovada" if headline["approved"] else "✕ reprovada"
                ttk.Label(outer, text=f"{verdict} {where}{more}",
                          style="Good.TLabel" if headline["approved"] else "Bad.TLabel").pack(anchor="w", pady=(6, 0))
        else:
            ttk.Label(outer, text="Sem teste desta seleção" if context and robot.runs else "Sem teste gravado",
                      style="Surface.TLabel").pack(anchor="w")
            ttk.Label(outer, text="Escolha uma configuração testada ou execute BACKTEST." if context and robot.runs
                      else "As estatísticas aparecem aqui depois do primeiro teste.", style="Muted.TLabel",
                      wraplength=wrap, justify=tk.LEFT).pack(anchor="w")

        self._asset_summary(outer, robot, wrap)
        detail = DetailsPanel(outer, "Risco e estágio", expanded=self._card_details.get(robot.key, False),
                              padding=5, information=True,
                              on_toggle=lambda value, key=robot.key: self._card_details.__setitem__(key, value))
        detail.pack(fill=tk.X, pady=(8, 0))
        ttk.Label(detail.body, text=reasons, style="Muted.TLabel", wraplength=wrap, justify=tk.LEFT).pack(
            anchor="w", pady=(1, 8))
        trail = tk.Canvas(detail.body, height=16, highlightthickness=0, borderwidth=0,
                          background=self.theme.colors["surface"])
        trail.pack(fill=tk.X)
        trail.bind("<Configure>", lambda e, r=robot, c=trail: self._draw_trail(c, r))
        step = f" · falta: {robot.next_step}" if robot.next_step else ""
        position = robot.stages.index(robot.stage) + 1 if robot.stage in robot.stages else 1
        ttk.Label(detail.body, text=f"Estágio {position} de {len(robot.stages)}: {robot.stage_label}{step}",
                  style="Muted.TLabel", wraplength=wrap, justify=tk.LEFT).pack(anchor="w", pady=(4, 0))
        if robot.notes:
            ttk.Label(outer, text="⚠ " + robot.notes[0], style="Muted.TLabel", wraplength=wrap,
                      justify=tk.LEFT).pack(anchor="w", pady=(4, 0))
        detail.body.bind("<Configure>", lambda e: [w.configure(wraplength=max(80, e.width - 8)) for w in detail.body.winfo_children()
                                                    if w.winfo_class() == "TLabel"], add="+")

        # O texto acompanha a largura do card (a grade muda o número de colunas com a janela)
        wrapped = [w for w in outer.winfo_children() if w.winfo_class() == "TLabel" and str(w.cget("wraplength")) != ""
                   and int(str(w.cget("wraplength")) or 0) > 0]
        outer.bind("<Configure>", lambda e: [w.configure(wraplength=max(120, e.width - 30)) for w in wrapped], add="+")
        self._bind_tree(outer, lambda e, key=robot.key: self.select(key))
        return outer

    def _asset_summary(self, parent, robot, wrap):
        summary = robot.backtest_summary
        if not summary["tested"]:
            if robot.runs:
                ttk.Label(parent, text="Ativos dos backtests não informados", style="Muted.TLabel",
                          wraplength=wrap).pack(anchor="w", pady=(6, 0))
            return
        total = summary["tested"]
        noun = "ativo testado" if total == 1 else "ativos testados"
        heading = ttk.Label(parent, text=f"{fmt_int(total)} {noun}", style="Surface.TLabel", wraplength=wrap)
        heading.pack(anchor="w", pady=(8, 0))
        ttk.Label(parent, text="Último backtest por ativo", style="Muted.TLabel", wraplength=wrap).pack(anchor="w")
        Tooltip(heading, "Cada ativo conta uma vez, pelo último backtest gravado, incluindo todos os timeframes e "
                        "presets. Lucro/prejuízo seguem o saldo e os custos desse teste; não são aprovação nem "
                        "validação fora da amostra.", self.theme)
        if summary["positive"] or summary["negative"]:
            row = ttk.Frame(parent, style="Surface.TFrame")
            row.pack(fill=tk.X, pady=(3, 0))
            labels = []
            size = max(12, int(self.theme.font.metrics("linespace") * .8))
            for column, (state, singular, plural, value) in enumerate((
                    ("positive", "positivo", "positivos", 1), ("negative", "negativo", "negativos", -1))):
                count = summary[state]
                fill, foreground = result_attention_colors(value if count else None, self.theme.colors)
                color = self.theme.colors["accent"] if state == "positive" and count else self.theme.colors["ink2"]
                key = (state, color, size)
                if key not in self._asset_icons:
                    self._asset_icons[key] = icons.result_direction(state, color, size)
                label = ttk.Label(row, text=f"{fmt_int(count)} {singular if count == 1 else plural}",
                                  image=self._asset_icons[key], compound=tk.LEFT, style="Surface.TLabel",
                                  foreground=foreground, background=fill, padding=3, wraplength=wrap // 2 - 24)
                label.grid(row=0, column=column, sticky="ew", padx=(0 if column == 0 else 4, 0))
                row.columnconfigure(column, weight=1, uniform="asset-result")
                labels.append(label)
            row.bind("<Configure>", lambda event: [label.configure(wraplength=max(40, event.width // 2 - 30))
                                                   for label in labels], add="+")
        extra = []
        if summary["flat"]:
            extra.append(f"{fmt_int(summary['flat'])} zerado(s)")
        if summary["unavailable"]:
            extra.append(f"{fmt_int(summary['unavailable'])} sem resultado")
        if extra:
            ttk.Label(parent, text=" · ".join(extra), style="Muted.TLabel", wraplength=wrap).pack(anchor="w")

    def _draw_trail(self, canvas, robot):
        """Trilha do ciclo de vida: pontos cheios até o estágio atual, vazados daí em diante."""
        c = self.theme.colors
        canvas.delete("all")
        stages = robot.stages
        width, radius, y = max(canvas.winfo_width(), 60), 5, 8
        current = stages.index(robot.stage) if robot.stage in stages else 0
        xs = [radius + 2 + i * (width - 2 * radius - 4) / (len(stages) - 1) for i in range(len(stages))]
        for i in range(len(stages) - 1):
            canvas.create_line(xs[i] + radius, y, xs[i + 1] - radius, y, width=2,
                               fill=c["accent"] if i < current else c["border"])
        for i, x in enumerate(xs):
            done = i <= current
            canvas.create_oval(x - radius, y - radius, x + radius, y + radius, width=2,
                               outline=c["accent"] if done else c["axis"], fill=c["accent"] if done else c["surface"])

    def _bind_tree(self, widget, handler):
        widget.bind("<Button-1>", handler, add="+")
        self._bind_wheel(widget)
        for child in widget.winfo_children():
            self._bind_tree(child, handler)

    def _bind_wheel(self, widget):
        widget.bind("<MouseWheel>", lambda e: self.canvas.yview_scroll(-1 if e.delta > 0 else 1, "units"), add="+")
        widget.bind("<Button-4>", lambda e: self.canvas.yview_scroll(-1, "units"), add="+")
        widget.bind("<Button-5>", lambda e: self.canvas.yview_scroll(1, "units"), add="+")

    def _resized(self, event):
        self.canvas.itemconfigure(self._window, width=event.width)
        self._layout(event.width)

    def _layout(self, width=None):
        width = width or self.canvas.winfo_width()
        columns = max(1, width // CARD_MIN_WIDTH)
        if columns == self._columns:
            return
        for column in range(max(columns, self._columns)):
            self.inner.columnconfigure(column, weight=1 if column < columns else 0,
                                       uniform="card" if column < columns else "")
        self._columns = columns
        for index, robot in enumerate(self.visible):
            self.cards[robot.key].grid(row=index // columns, column=index % columns, sticky="nsew",
                                       padx=(0, 10), pady=(0, 10))

    def select(self, key):
        self.selected_key = key
        self._mark_selected()
        self._reveal(key)
        self.detail.show(next((r for r in self.robots if r.key == key), None))
        self._sync_detail_selection()

    def _sync_detail_selection(self):
        robot = self.detail.robot
        if robot and robot.kind == catalog.MQL5:
            from mt5_extracao.ui.robot_detail import SOURCE_DEFAULTS
            preset = self.controls.chosen[robot.key][2]
            self.detail.source_var.set(f"Preset {preset}" if preset else SOURCE_DEFAULTS)
            self.detail._show_values()
            session = self.controls.sessions.get(robot.key)
            self.detail.source_combo.configure(state="disabled" if session and session.active else "readonly")

    def _detail_preset_changed(self, _event=None):
        robot = self.detail.robot
        if robot and robot.kind == catalog.MQL5:
            source = self.detail.source_var.get()
            preset = source[len("Preset "):] if source.startswith("Preset ") else None
            symbol, timeframe, _ = self.controls.chosen[robot.key]
            self.controls.chosen[robot.key] = symbol, timeframe, preset
            self.render()

    def _reveal(self, key):
        """Rola a grade até o card, se ele estiver fora da área visível."""
        frame = self.cards.get(key)
        total = self.inner.winfo_height()
        if frame is None or total <= 1:
            return
        top, bottom = frame.winfo_y(), frame.winfo_y() + frame.winfo_height()
        first, last = (fraction * total for fraction in self.canvas.yview())
        if top < first or bottom > last:
            self.canvas.yview_moveto(max(0, top - 6) / total)

    def _mark_selected(self):
        for key, frame in self.cards.items():
            frame.configure(style="Selected.Card.TFrame" if key == self.selected_key else "Card.TFrame")

    def _theme_changed(self):
        self.canvas.configure(background=self.theme.colors["page"])
        self.body_canvas.configure(background=self.theme.colors["page"])
        self.render()

    # --- importação -------------------------------------------------------------------

    def _database(self):
        db = getattr(self.ui.app, "db_manager", None)
        if not db or not db.is_connected():
            messagebox.showerror("Robôs", "Banco de dados não disponível: não é possível gravar a importação.")
            return None
        return db

    def import_file(self):
        db = self._database()
        path = db and filedialog.askopenfilename(
            title="Escolha o robô", filetypes=[("Robô do MetaTrader 5", "*.mq5 *.ex5"), ("Todos os arquivos", "*.*")])
        if path:
            self.run_import(lambda: [robots_core.import_robot(db, path)])

    def import_folder(self):
        db = self._database()
        folder = db and filedialog.askdirectory(title="Pasta com robôs (Experts, Include, Presets)")
        if folder:
            self.run_import(lambda: robots_core.import_library(db, folder))

    def run_import(self, work):
        def done(found, error):
            if error:
                messagebox.showerror("Robôs", str(error))
                return
            self.ui.log("Robô(s) no catálogo: " + ", ".join(r.name for r in found))
            for var, _combo in self.filters.values():
                var.set(ALL)
            self.search_var.set("")
            self.selected_key = found[0].key      # depois de limpar a busca, que redesenha os cards
            self.reload()
        self.ui.run_bg(work, done)
