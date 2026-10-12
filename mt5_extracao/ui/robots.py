"""Aba "Robôs": catálogo em cards (MQL5 e Python) com tipo, risco, estatísticas e estágio; clique abre o painel."""
import logging
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from mt5_extracao import robots as robots_core
from mt5_extracao.robots import catalog, risk
from mt5_extracao.ui.robot_detail import RISK_STYLES, RobotDetail
from mt5_extracao.ui.theme import Tooltip, card, fmt_int, fmt_num

log = logging.getLogger(__name__)

ALL = "Todos"
CARD_MIN_WIDTH = 310


def result_tiles(result):
    """(valor, legenda) das estatísticas do card, a partir do melhor teste gravado."""
    return ((fmt_num(result["profit_factor"]), "profit factor"),
            (fmt_int(result["trades"]), "operações"),
            (fmt_num(result["max_drawdown"] * 100, 1, "%"), "drawdown"),
            (fmt_num(result["positive_windows"] * 100, 0, "%"), "janelas com lucro"))


class RobotsPanel:
    def __init__(self, parent, ui):
        self.ui = ui
        self.theme = ui.theme
        self.robots = []
        self.visible = []        # robôs que passam nos filtros, na ordem dos cards
        self.cards = {}          # chave do robô -> frame do card
        self.selected_key = None
        self._columns = 0
        self.frame = ttk.Frame(parent, padding=12)

        bar = card(self.frame, padding=10)
        bar.master.pack(fill=tk.X)
        self.import_button = ttk.Menubutton(bar, text="Importar robô ▾", style="Accent.TButton")
        menu = tk.Menu(self.import_button, tearoff=0)
        self.theme.style_menu(menu)
        menu.add_command(label="Arquivo do robô (.mq5 ou .ex5)...", command=self.import_file)
        menu.add_command(label="Pasta de robôs...", command=self.import_folder)
        menu.add_separator()
        menu.add_command(label="Reler o catálogo", command=self.reload)
        self.import_button["menu"] = menu
        self.import_button.pack(side=tk.LEFT)
        Tooltip(self.import_button, "Robôs MQL5 vêm de um arquivo ou de uma pasta no formato do MT5 (Experts, Include, "
                                    "Presets), como o clone do projeto HADOUKEN. Nada é copiado: o programa só lê.",
                self.theme)
        self.search_var = tk.StringVar()
        self.search_var.trace_add("write", lambda *_: self.render())
        ttk.Label(bar, text="Buscar", style="Muted.TLabel").pack(side=tk.LEFT, padx=(16, 6))
        ttk.Entry(bar, textvariable=self.search_var, width=18).pack(side=tk.LEFT)
        self.filters = {}
        for key, label, values, width in (
                ("kind", "Tipo", (ALL, "MQL5", "Python"), 8),
                ("risk", "Risco", (ALL,) + risk.LEVELS, 13),
                ("stage", "Estágio", (ALL,) + tuple(catalog.STAGE_LABELS.values()), 11),
                ("regime", "Regime", (ALL,), 14)):
            ttk.Label(bar, text=label, style="Muted.TLabel").pack(side=tk.LEFT, padx=(14, 6))
            var = tk.StringVar(value=ALL)
            combo = ttk.Combobox(bar, textvariable=var, values=values, state="readonly", width=width)
            combo.pack(side=tk.LEFT)
            combo.bind("<<ComboboxSelected>>", lambda e: self.render())
            self.filters[key] = (var, combo)

        self.summary = ttk.Label(self.frame, text="Lendo o catálogo...", style="Page.Muted.TLabel")
        self.summary.pack(anchor="w", pady=(8, 6))

        body = ttk.Frame(self.frame)
        body.pack(fill=tk.BOTH, expand=True)
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
        self.detail.frame.grid(row=0, column=1, sticky="nsew")
        self.theme.on_change(self._theme_changed)

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

        icon, label, style = RISK_STYLES[robot.risk.level]
        ttk.Label(outer, text=f"{icon} {label}", style=style).pack(anchor="w")
        reasons = robot.risk.summary
        if robot.preset_risks:
            levels = [r.level for r in robot.preset_risks.values()]
            ordered = [level for level in risk.LEVELS if level in levels]
            span = ordered[0] if len(ordered) == 1 else f"{ordered[0]} a {ordered[-1]}"
            reasons += f"\n{len(levels)} preset(s): risco {span}"
        ttk.Label(outer, text=reasons, style="Muted.TLabel", wraplength=wrap, justify=tk.LEFT).pack(
            anchor="w", pady=(1, 8))

        ttk.Separator(outer).pack(fill=tk.X, pady=(0, 8))
        result = robot.result
        if result:
            tiles = ttk.Frame(outer, style="Surface.TFrame")
            tiles.pack(fill=tk.X)
            Tooltip(tiles, "Melhor validação gravada, fora da amostra e com custos: profit factor, número de "
                           "operações, drawdown no cenário ruim do Monte Carlo e parcela das janelas walk-forward "
                           "que deram lucro.", self.theme)
            for column, (value, caption) in enumerate(result_tiles(result)):
                tiles.columnconfigure(column, weight=1, uniform="tile")
                ttk.Label(tiles, text=value, style="Heading.TLabel").grid(row=0, column=column, sticky="w")
                ttk.Label(tiles, text=caption, style="Muted.TLabel", wraplength=70, justify=tk.LEFT).grid(
                    row=1, column=column, sticky="nw")
            verdict = "✓ aprovada" if result["approved"] else "✕ reprovada"
            more = f" · mais {len(robot.results) - 1} teste(s)" if len(robot.results) > 1 else ""
            ttk.Label(outer, text=f"{verdict} em {result['symbol']} {result['timeframe']}{more}",
                      style="Good.TLabel" if result["approved"] else "Bad.TLabel").pack(anchor="w", pady=(6, 0))
        else:
            ttk.Label(outer, text="Sem teste gravado", style="Surface.TLabel").pack(anchor="w")
            ttk.Label(outer, text="As estatísticas aparecem aqui depois do primeiro teste.", style="Muted.TLabel",
                      wraplength=wrap, justify=tk.LEFT).pack(anchor="w")

        ttk.Separator(outer).pack(fill=tk.X, pady=(8, 8))
        trail = tk.Canvas(outer, height=16, highlightthickness=0, borderwidth=0,
                          background=self.theme.colors["surface"])
        trail.pack(fill=tk.X)
        trail.bind("<Configure>", lambda e, r=robot, c=trail: self._draw_trail(c, r))
        step = f" · falta: {robot.next_step}" if robot.next_step else ""
        position = robot.stages.index(robot.stage) + 1 if robot.stage in robot.stages else 1
        ttk.Label(outer, text=f"Estágio {position} de {len(robot.stages)}: {robot.stage_label}{step}",
                  style="Muted.TLabel", wraplength=wrap, justify=tk.LEFT).pack(anchor="w", pady=(4, 0))
        if robot.notes:
            ttk.Label(outer, text="⚠ " + robot.notes[0], style="Muted.TLabel", wraplength=wrap,
                      justify=tk.LEFT).pack(anchor="w", pady=(4, 0))

        # O texto acompanha a largura do card (a grade muda o número de colunas com a janela)
        wrapped = [w for w in outer.winfo_children() if w.winfo_class() == "TLabel" and str(w.cget("wraplength")) != ""
                   and int(str(w.cget("wraplength")) or 0) > 0]
        outer.bind("<Configure>", lambda e: [w.configure(wraplength=max(120, e.width - 30)) for w in wrapped], add="+")
        self._bind_tree(outer, lambda e, key=robot.key: self.select(key))
        return outer

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
