"""
Aba "Minha base": o que já foi extraído (uma linha por ativo x timeframe), com favorito, volume, resultado dos
robôs, situação da base e benchmark; marcação em lote e a bateria de backtests em segundo plano.
"""
import logging
import tkinter as tk
from tkinter import messagebox, simpledialog, ttk

from mt5_extracao import asset_table
from mt5_extracao.asset_table import COL_FAVORITE, COL_SELECTED, COLUMNS
from mt5_extracao.robots import catalog, runner
from mt5_extracao.ui.components import configure_result_tags, FilterBar
from mt5_extracao.ui.backtest_dialog import BacktestDialog
from mt5_extracao.ui.visual_tokens import SPACING, result_attention_role
from mt5_extracao.ui.theme import Tooltip, card, fmt_int, scrolled_tree

log = logging.getLogger(__name__)

EXPORT_FORMATS = (("CSV", "csv"), ("Excel", "excel"), ("Parquet", "parquet"), ("DuckDB", "duckdb"))
WIDTHS = (34, 110, 90, 120, 180, 90, 170, 70)
ANCHORS = ("center", "w", "e", "w", "w", "w", "w", "center")


class InventoryPanel:
    def __init__(self, parent, ui):
        self.ui = ui
        self.rows = {}        # iid da linha (nome da tabela) -> item do inventário
        self.checked = set()  # iids marcados para o backtest
        self.runner = None
        self._sort = (None, False)
        self._inventory = []
        self.frame = ttk.Frame(parent, padding=12)

        body = card(self.frame, "Minha base de dados", "tudo o que já foi extraído e está pronto para estudo")
        body.master.pack(fill=tk.BOTH, expand=True)
        self.summary = ttk.Label(body, text="Lendo a base...", style="Muted.TLabel")
        self.summary.pack(anchor="w", pady=(0, 8))
        Tooltip(self.summary, "Volume: média diária dos últimos 30 dias de dados da base (volume real quando a "
                              "corretora informa, senão o de ticks). Robô Lucrativo: maior retorno entre os robôs "
                              "cujo backtest mais recente nesta série deu lucro; lucro em um teste não é aprovação. "
                              "Status da Base: pela data da última barra gravada.", ui.theme)

        quick = FilterBar(body, padding=6)
        quick.pack(fill=tk.X, pady=(0, 8))
        ttk.Button(quick, text=ui.theme.glyphs("☑ Selecionar Todos"), command=self.check_all).pack(side=tk.LEFT)
        ttk.Button(quick, text=ui.theme.glyphs("☐ Deselecionar Todos"), command=self.uncheck_all).pack(
            side=tk.LEFT, padx=(8, 0))
        self.count_label = ttk.Label(quick, text="", style="Muted.TLabel")
        self.count_label.pack(side=tk.LEFT, padx=(12, 0))

        actions = FilterBar(body, padding=6)
        actions.pack(fill=tk.X, side=tk.BOTTOM, pady=(10, 0))
        self.backtest_button = ttk.Button(actions, text="Iniciar Backtest", style="Accent.TButton",
                                          command=self.start_backtest)
        self.backtest_button.pack(side=tk.LEFT)
        Tooltip(self.backtest_button, "Marque as séries na coluna Seleção e escolha os robôs: cada robô é testado em "
                                      "cada série, em segundo plano. Um backtest isolado não aprova a estratégia.",
                ui.theme)
        self.analyze_button = ttk.Button(actions, text="Analisar", command=self.analyze)
        self.analyze_button.pack(side=tk.LEFT, padx=(8, 0))
        self.select_button = ttk.Button(actions, text="Adicionar à seleção", command=self.add_to_selection)
        self.select_button.pack(side=tk.LEFT, padx=(8, 0))
        Tooltip(self.select_button, "Coloca o ativo na seleção da aba Ativos, para atualizar ou extrair mais dados.",
                ui.theme)
        self.benchmark_button = ttk.Button(actions, text="Benchmark...", command=self.edit_benchmark)
        self.benchmark_button.pack(side=tk.LEFT, padx=(8, 0))
        Tooltip(self.benchmark_button, "Vincula ao ativo o índice de referência (ex.: IBOV para ações da B3). "
                                       "Deixe em branco para desvincular.", ui.theme)
        self.export_button = ttk.Menubutton(actions, text="Exportar ▾", style="TButton")
        menu = tk.Menu(self.export_button, tearoff=0)
        ui.theme.style_menu(menu)
        for label, fmt in EXPORT_FORMATS:
            menu.add_command(label=label, command=lambda f=fmt: self.export(f))
        self.export_button["menu"] = menu
        self.export_button.pack(side=tk.LEFT, padx=(8, 0))
        ttk.Button(actions, text="Atualizar lista", command=ui.refresh_inventory).pack(side=tk.RIGHT)

        # Andamento da bateria de backtests (aparece só enquanto ela roda)
        self.progress_frame = ttk.Frame(body, style="Surface.TFrame")
        self.progress_var = tk.DoubleVar(value=0)
        self.cancel_button = ttk.Button(self.progress_frame, text="Cancelar", command=self.cancel_backtest)
        self.cancel_button.pack(side=tk.RIGHT, padx=(8, 0))
        ttk.Progressbar(self.progress_frame, variable=self.progress_var, mode="determinate").pack(
            fill=tk.X, pady=(0, 4))
        self.progress_label = ttk.Label(self.progress_frame, text="", style="Muted.TLabel")
        self.progress_label.pack(anchor="w")

        self.result_legend = ttk.Label(
            body, text="Retorno histórico: azul suave = positivo · discreto = negativo · 0,0% = zero · N/A = indisponível. Não indica aprovação.",
            style="Muted.TLabel", wraplength=1000)
        self.result_legend.pack(fill=tk.X, pady=(0, 8))
        self.result_legend.bind("<Configure>", lambda event: self.result_legend.configure(wraplength=max(180, event.width)))
        self.tree_frame, self.tree = scrolled_tree(body, COLUMNS, WIDTHS, ANCHORS, height=14, xscroll=True)
        self.tree.column(COL_FAVORITE, stretch=False)
        self.tree.column(asset_table.COL_PROFIT, anchor="e")
        self._apply_attention_theme()
        ui.theme.on_change(self._apply_attention_theme)
        self.tree.heading(COL_FAVORITE, text=ui.theme.glyphs(COL_FAVORITE))
        self.tree_frame.pack(fill=tk.BOTH, expand=True)
        for col in COLUMNS:
            self.tree.heading(col, command=lambda c=col: self.sort_by(c))
        self.tree.bind("<Button-1>", self._clicked)
        self.tree.bind("<Double-Button-1>", self._double_clicked)
        self.tree.bind("<space>", lambda e: self.toggle_checked(self.tree.selection()))
        self.tree.bind("<<TreeviewSelect>>", lambda e: self._update_buttons())
        self._update_buttons()

    # --- tabela -----------------------------------------------------------------------

    def _apply_attention_theme(self):
        configure_result_tags(self.tree, self.ui.theme)
        # O destaque de retorno não pode cortar a informação independente de qualidade.
        status_width = max(self.ui.theme.font.measure(self.ui.theme.glyphs(asset_table.fmt_status(state)))
                           for state in (None, False, True)) + 2 * SPACING["sm"]
        self.tree.column(asset_table.COL_STATUS, minwidth=status_width)

    def sort_by(self, column):
        self._sort = (column, not self._sort[1] if self._sort[0] == column else False)
        self.show(self._inventory)

    def show(self, inventory):
        self._inventory = list(inventory)
        ui = self.ui
        by_table = {item["table"]: item for item in self._inventory}
        self.checked &= set(by_table)
        frame = asset_table.sort_rows(asset_table.build_rows(
            ((item["table"], item["symbol"], [item]) for item in self._inventory), ui.favorite_symbols, ui.volumes,
            ui.profit_cells, ui.benchmarks, self.checked, per_series=True), *self._sort)
        selection = self.tree.selection()
        self.tree.delete(*self.tree.get_children())
        self.rows = {}
        for row in frame.itertuples():
            self.tree.insert("", tk.END, iid=row.key, values=ui.cell_texts(row),
                             tags=(f"result.{result_attention_role(row.profit)}",))
            self.rows[row.key] = by_table[row.key]
        if selection and selection[0] in self.rows:
            self.tree.selection_set(selection[0])
        if inventory:
            symbols = len({item["symbol"] for item in inventory})
            self.summary.config(text=f"{symbols} ativo(s) · {len(inventory)} série(s) · "
                                     f"{fmt_int(sum(item['rows'] for item in inventory))} barras. Clique no "
                                     "cabeçalho para ordenar, na estrela para favoritar e na caixa para marcar.")
        else:
            self.summary.config(text="A base ainda está vazia. Na aba Ativos, escolha um ativo e clique em "
                                     "\"Extrair histórico\".")
        self._update_buttons()

    def _clicked(self, event):
        if self.tree.identify_region(event.x, event.y) != "cell":
            return None
        iid, column = self.tree.identify_row(event.y), self._column_at(event.x)
        if not iid:
            return None
        if column == COL_FAVORITE:
            self.ui.toggle_favorite(self.rows[iid]["symbol"])
            return "break"
        if column == COL_SELECTED:
            self.toggle_checked([iid])
            return "break"
        return None

    def _column_at(self, x):
        index = int(self.tree.identify_column(x).lstrip("#") or 0) - 1
        return COLUMNS[index] if 0 <= index < len(COLUMNS) else None

    def _double_clicked(self, event):
        if self._column_at(event.x) in (COL_FAVORITE, COL_SELECTED):
            return "break"
        self.analyze()
        return None

    # --- marcação ---------------------------------------------------------------------

    def toggle_checked(self, iids):
        for iid in iids:
            self.checked.symmetric_difference_update({iid})
        self._refresh_checks()

    def check_all(self):
        self.checked = set(self.tree.get_children())
        self._refresh_checks()

    def uncheck_all(self):
        self.checked = set()
        self._refresh_checks()

    def checked_items(self):
        return [self.rows[iid] for iid in self.tree.get_children() if iid in self.checked]

    def _refresh_checks(self):
        for iid in self.tree.get_children():
            self.tree.set(iid, COL_SELECTED, self.ui.theme.glyphs(
                asset_table.CHECK_ON if iid in self.checked else asset_table.CHECK_OFF))
        self._update_buttons()

    def selected(self):
        selection = self.tree.selection()
        return self.rows.get(selection[0]) if selection else None

    def _update_buttons(self):
        state = ["!disabled"] if self.selected() else ["disabled"]
        for button in (self.analyze_button, self.select_button, self.export_button, self.benchmark_button):
            button.state(state)
        count = len(self.checked)
        symbols = len({self.rows[iid]["symbol"] for iid in self.checked if iid in self.rows})
        self.count_label.config(text=f"{symbols} ativo(s) selecionado(s)" +
                                (f" em {count} série(s)" if count != symbols else ""))
        busy = self.runner is not None and self.runner.running
        self.backtest_button.config(text=f"Iniciar Backtest ({count} selecionado{'s' if count != 1 else ''})"
                                    if count else "Iniciar Backtest")
        self.backtest_button.state(["!disabled"] if count and not busy else ["disabled"])

    def profit_changed(self, symbol):
        """Um backtest do ativo terminou: atualiza só a coluna Robô Lucrativo das linhas dele."""
        from mt5_extracao import timeframes
        for iid, item in self.rows.items():
            if item["symbol"] == symbol:
                tf = timeframes.parse(item["timeframe"])
                value = asset_table.profit_of(self.ui.profit_cells, symbol, tf.name if tf else None)
                self.tree.set(iid, asset_table.COL_PROFIT, self.ui.theme.glyphs(asset_table.fmt_profit(value)))
                self.tree.item(iid, tags=(f"result.{result_attention_role(value)}",))

    # --- ações da linha em destaque ---------------------------------------------------

    def analyze(self):
        item = self.selected()
        if item:
            self.ui.open_analysis(item["symbol"], item["timeframe"])

    def add_to_selection(self):
        item = self.selected()
        if item:
            self.ui.select_symbols([item["symbol"]])

    def export(self, fmt):
        item = self.selected()
        if item:
            self.ui.export_table(item["table"], fmt)

    def edit_benchmark(self):
        item = self.selected()
        if not item:
            return
        symbol = item["symbol"]
        answer = simpledialog.askstring(
            "Benchmark", f"Índice de referência de {symbol} (em branco = nenhum):",
            initialvalue=self.ui.benchmarks.get(symbol, ""), parent=self.frame)
        if answer is not None:
            self.ui.set_benchmark(symbol, answer.strip())

    # --- bateria de backtests ---------------------------------------------------------

    def start_backtest(self):
        series = self.checked_items()
        if not series or (self.runner is not None and self.runner.running):
            return
        robots = self.ui.robots_panel.robots
        self.dialog = BacktestDialog(self.ui, series, robots, lambda chosen: self.run_backtests(series, chosen))

    def run_backtests(self, series, robots, governor=None):
        """Monta as combinações (série x robô) e as roda em segundo plano. Devolve o executor (None se nada roda)."""
        from mt5_extracao.robots import mt5_tools
        ui = self.ui
        db, config = ui.app.db_manager, getattr(ui.app, "config", None)
        if not db or not db.is_connected():
            messagebox.showerror("Backtest", "Banco de dados não disponível: não é possível gravar os testes.",
                                 parent=self.frame)
            return None
        setup = None
        if any(r.kind == catalog.MQL5 for r in robots):
            setup, robots = self._mql5_setup(config, robots, mt5_tools)
        jobs = [runner.Job(robot, item["symbol"], item["timeframe"]) for item in series for robot in robots]
        if not jobs:
            return None
        self.progress_var.set(0)
        self.progress_label.config(text=f"Preparando {len(jobs)} backtest(s)...")
        self.cancel_button.state(["!disabled"])
        self.progress_frame.pack(fill=tk.X, side=tk.BOTTOM, pady=(10, 0), before=self.tree_frame)
        ui.log(f"Bateria de backtests iniciada: {len(series)} série(s) × {len(robots)} robô(s) = {len(jobs)} teste(s).")
        self.runner = runner.BatchRunner(db, config, jobs, governor=governor, mql5_setup=setup,
                                         on_event=lambda event: ui.call_in_ui(self._batch_event, event))
        ui.log(f"Checkpoint do lote: {self.runner.batch_id}. Retomada pela CLI com --batch-id.")
        self._update_buttons()
        self.runner.start()
        self._update_buttons()
        return self.runner

    def _mql5_setup(self, config, robots, mt5_tools):
        """Robôs MQL5 precisam do MT5 fechado: pergunta antes; sem o aceite, ficam só as estratégias Python."""
        if not mt5_tools.terminal_running(config):
            return None, robots
        if not messagebox.askyesno(
                "Backtest", "Os robôs MQL5 rodam no Strategy Tester e o MetaTrader 5 precisa estar fechado.\n\n"
                            "Fechar o MetaTrader 5 agora? (Coleta e extração em andamento serão interrompidas; "
                            "depois, use \"Verificar conexão\" para reconectar.)\n\n"
                            "Respondendo Não, só as estratégias Python são testadas.", parent=self.frame):
            return None, [r for r in robots if r.kind != catalog.MQL5]

        def setup():
            bridge_was_running = mt5_tools.release_terminal(config)
            return mt5_tools.restart_bridge if bridge_was_running else None
        return setup, robots

    def cancel_backtest(self):
        if self.runner is not None:
            self.runner.cancel()
            self.cancel_button.state(["disabled"])
            self.progress_label.config(text="Cancelando: Python para entre blocos; o Tester nativo retorna antes de liberar o terminal...")

    def _batch_event(self, event):
        ui, kind = self.ui, event["kind"]
        if kind == runner.FINISHED:
            self.progress_frame.pack_forget()
            partial = bool(self.runner and (self.runner.budget.reason or self.runner.error))
            label = "Backtests interrompidos" if partial else "Backtests concluídos"
            text = f"{label}: {event['ok']} testado(s), {event['failed']} com falha"
            if event["canceled"]:
                text += f", {event['canceled']} cancelado(s)"
            if partial:
                text += ": " + (self.runner.error or self.runner.budget.reason)
            ui.update_status(text + ".")
            ui.log(text + ". Veja o resultado na coluna Robô Lucrativo e na aba Robôs.")
            self._update_buttons()
            ui.robots_panel.reload()
            return
        job = event["job"]
        usage = f"(CPU: {event['cpu']:.0f}% | RAM: {event['ram']:.0f}%)"
        if kind == runner.STARTED:
            text = f"Executando Backtest {event['index']}/{event['total']}: {job.symbol} + {job.robot.name} {usage}"
            ui.update_status(text)
            self.progress_label.config(text=text + f" · {event['workers']} em paralelo")
        elif kind == runner.DONE:
            self.progress_var.set(100.0 * event["completed"] / max(1, event["total"]))
            if event["run"] is None:
                ui.log(f"Backtest de {job.robot.name} em {job.symbol} ({job.timeframe}) não rodou: {event['error']}")
            else:
                ui.backtest_recorded(event["run"])
