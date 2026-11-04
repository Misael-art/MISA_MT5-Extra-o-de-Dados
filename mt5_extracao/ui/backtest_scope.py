"""Revisão e acompanhamento do plano de TFs; execução na fila existente."""
from datetime import datetime, timedelta
from queue import Empty, Queue
import threading
import tkinter as tk
from tkinter import ttk

from mt5_extracao.robots import backtest_scope, budget, runner
from .components import ScrollableContent
from .theme import fmt_int, fmt_num


class BacktestScopeDialog:
    def __init__(self, ui, robot, symbol, *, preset=None, start=None, end=None,
                 manifest_path=None, is_current=lambda: True, completed=lambda batch: None):
        from .robot_controls import _modal_when_mapped
        self.ui, self.robot, self.symbol, self.preset = ui, robot, symbol, preset
        self.manifest_path, self.is_current, self.completed = manifest_path, is_current, completed
        self.batch, self.plan, self.pending = None, None, False
        self.records, self.entries = {}, []
        self.stop, self.events = threading.Event(), Queue()
        self.closing, self._poll_id = False, None
        self.window = tk.Toplevel(ui.root)
        self.window.title(f"Ativos e tempos gráficos · {robot.name}")
        self.window.geometry("980x680"); self.window.minsize(600, 430)
        self.window.transient(ui.root); _modal_when_mapped(self.window)
        self.window.protocol("WM_DELETE_WINDOW", self.close)
        self.window.bind("<Destroy>", self._destroyed, add="+")
        shell = ttk.Frame(self.window, padding=16); shell.pack(fill=tk.BOTH, expand=True)
        self.viewport = ScrollableContent(shell, ui.theme)
        self.viewport.pack(fill=tk.BOTH, expand=True)
        outer = self.viewport.content
        self.heading = ttk.Label(outer, text=f"{robot.name} · Ativos × todos os tempos gráficos",
                                 style="Heading.TLabel", wraplength=850)
        self.heading.pack(fill=tk.X)
        self.heading.bind("<Configure>", lambda e: self.heading.configure(wraplength=max(80, e.width-4)))
        assets = ttk.LabelFrame(outer, text="Ativos da base · Ctrl/Shift para selecionar vários", padding=6)
        assets.pack(fill=tk.X, pady=(8, 0))
        self.assets = ttk.Treeview(assets, columns=("symbol",), show="headings", selectmode="extended", height=4)
        self.assets.heading("symbol", text="Ativo (elegibilidade verificada ao preparar)")
        self.assets.column("symbol", width=240)
        self.assets.grid(row=0, column=0, sticky="nsew")
        assets.columnconfigure(0, weight=1)
        scrollbar = ttk.Scrollbar(assets, command=self.assets.yview)
        scrollbar.grid(row=0, column=1, sticky="ns"); self.assets.configure(yscrollcommand=scrollbar.set)
        for value in sorted(set(getattr(ui, "symbol_series", {})) | {symbol}):
            self.assets.insert("", tk.END, iid=value, values=(value,))
        self.assets.selection_set(symbol)
        self.all_assets_button = ttk.Button(assets, text="Selecionar todos da base", command=self.select_all_assets)
        self.all_assets_button.grid(row=1, column=0, sticky="w", pady=(4, 0))
        self.assets.bind("<<TreeviewSelect>>", lambda e: self.invalidate() if not self.active else None)
        for sequence in ("<Button-1>", "<KeyPress>"):
            self.assets.bind(sequence, lambda e: "break" if self.active else None)
        fields = ttk.Frame(outer); fields.pack(fill=tk.X, pady=10)
        today = datetime.now()
        self.start = tk.StringVar(value=str(start or (today-timedelta(days=90)).date()))
        self.end = tk.StringVar(value=str(end or today.date()))
        fields.columnconfigure(1, weight=1)
        for row, (label, var) in enumerate((("De (ISO)", self.start), ("Até (ISO)", self.end))):
            ttk.Label(fields, text=label).grid(row=row, column=0, sticky="w", padx=(0, 6), pady=3)
            entry = ttk.Entry(fields, textvariable=var, width=22)
            entry.grid(row=row, column=1, sticky="ew", padx=(0, 12), pady=3)
            self.entries.append(entry)
            if manifest_path:
                entry.state(["disabled"])
            var.trace_add("write", lambda *_: self.invalidate())
        self.prepare_button = ttk.Button(fields, text="Preparar plano", command=self.prepare)
        self.prepare_button.grid(row=0, column=2, rowspan=2, sticky="e")
        self.summary = ttk.Label(outer, text="Prepare o plano para conferir amostra, preset e exclusões antes de executar.",
                                 style="Muted.TLabel", wraplength=900, justify=tk.LEFT)
        self.summary.pack(fill=tk.X, pady=(0, 10))
        self.summary.bind("<Configure>", lambda e: self.summary.configure(wraplength=max(80, e.width-4)))
        table = ttk.Frame(outer); table.pack(fill=tk.BOTH, expand=True)
        self.tree = ttk.Treeview(table, columns=("tf", "bars", "status", "reason"), show="headings", height=14)
        for name, text, width in (("tf", "Ativo · TF", 160), ("bars", "Barras locais", 100), ("status", "Estado", 110),
                                  ("reason", "Motivo ou ressalva", 580)):
            self.tree.heading(name, text=text); self.tree.column(name, width=width, stretch=name == "reason")
        vertical = ttk.Scrollbar(table, command=self.tree.yview)
        horizontal = ttk.Scrollbar(table, orient=tk.HORIZONTAL, command=self.tree.xview)
        self.tree.configure(yscrollcommand=vertical.set, xscrollcommand=horizontal.set)
        table.columnconfigure(0, weight=1); table.rowconfigure(0, weight=1)
        self.tree.grid(row=0, column=0, sticky="nsew"); vertical.grid(row=0, column=1, sticky="ns")
        horizontal.grid(row=1, column=0, sticky="ew")
        self.detail = ttk.Label(outer, text="", style="Muted.TLabel", wraplength=900, justify=tk.LEFT)
        self.detail.pack(fill=tk.X, pady=(6, 0))
        self.detail.bind("<Configure>", lambda e: self.detail.configure(wraplength=max(80, e.width-4)))
        self.tree.bind("<<TreeviewSelect>>", self.show_detail)
        bottom = ttk.Frame(shell); bottom.pack(side=tk.BOTTOM, fill=tk.X, pady=(12, 0), before=self.viewport)
        bottom.columnconfigure(0, weight=1)
        self.status = ttk.Label(bottom, text="Aguardando plano", style="Muted.TLabel", wraplength=300, justify=tk.LEFT)
        self.status.grid(row=0, column=0, sticky="ew")
        self.status.bind("<Configure>", lambda e: self.status.configure(wraplength=max(80, e.width-4)))
        self.cancel_button = ttk.Button(bottom, text="Fechar", command=self.cancel_or_close)
        self.cancel_button.grid(row=0, column=2, sticky="ne")
        self.run_button = ttk.Button(bottom, text="Executar plano", style="Accent.TButton", command=self.execute)
        self.run_button.grid(row=0, column=1, sticky="ne", padx=8); self.run_button.state(["disabled"])
        self._poll_id = self.window.after(150, self.poll)

    @property
    def active(self):
        return self.pending or bool(self.batch and self.batch.running)

    def exists(self):
        return bool(self.window.winfo_exists())

    def invalidate(self):
        self.plan = None
        self.run_button.state(["disabled"])
        self.summary.configure(text="Seleção ou janela alterada; prepare outro plano antes de executar.")

    def select_all_assets(self):
        if not self.active:
            local = set(getattr(self.ui, "symbol_series", {}))
            self.assets.selection_set([value for value in self.assets.get_children() if value in local])
            self.invalidate()

    def _row_id(self, row):
        return (row["timeframe"] if len(self.plan.get("symbols", [self.symbol])) == 1 else
                budget.job_key(self.robot.key, row["symbol"], row["timeframe"]))

    def _guard(self):
        if self.stop.is_set():
            raise ValueError("Preparação cancelada.")

    def prepare(self):
        if self.active:
            return
        if not self.is_current():
            self.status.configure(text="Seleção mudou; reabra o plano."); return
        self.stop.clear(); self.pending = True; self.plan = None
        self.events, self.records = Queue(), {}
        self.prepare_button.state(["disabled"]); self.run_button.state(["disabled"])
        self.cancel_button.configure(text="Cancelar"); self.status.configure(text="Preparando revisões e elegibilidade…")
        requested = (self.start.get(), self.end.get(), tuple(self.assets.selection()))
        self.all_assets_button.state(["disabled"])

        def done(plan, error):
            self.pending = False
            if not self.exists():
                return
            if self.closing:
                self.window.destroy(); return
            self.prepare_button.state(["!disabled"]); self.all_assets_button.state(["!disabled"])
            self.cancel_button.configure(text="Fechar")
            if self.stop.is_set():
                self.status.configure(text="Preparação cancelada."); return
            if error:
                self.status.configure(text=str(error)); return
            if requested != (self.start.get(), self.end.get(), tuple(self.assets.selection())) or not self.is_current():
                self.status.configure(text="Seleção mudou durante a preparação; prepare outro plano."); return
            self.plan = plan
            self.tree.delete(*self.tree.get_children())
            for row in plan["rows"]:
                self.tree.insert("", tk.END, iid=self._row_id(row), values=(f"{row['symbol']} · {row['timeframe']}", row["bars"],
                    "Elegível" if row["eligible"] else "Excluído", row["reason"] or " · ".join(row["limitations"])))
            identity = plan["preset_identity"]
            from mt5_extracao.robots.mt5_tools import TESTER_MODELS
            model = "Barras da base local" if plan["model"] == "python_bar_backtest" else TESTER_MODELS[plan["model"]]
            self.summary.configure(text=f"{plan['eligible']} combinação(ões) elegível(is) · {plan['excluded']} excluída(s) · plano {plan['batch_id']}\n"
                f"Ativos: {', '.join(plan['symbols'])}\n"
                f"{plan['start']} a {plan['end']} · {identity.get('label') or identity['id']} · revisão {identity['revision'] or 'por conteúdo'}\n"
                f"Hash {identity['content_hash']}\nCapital simulado {fmt_num(plan['settings']['initial_equity'])} · modelo {model}\n"
                + (plan["native_download_notice"] or "Dados e parâmetros congelados neste plano; atualização da base exige outro plano.")
                + f"\nAté {plan['resource_limits_at_review']['max_workers']} execução(ões) Python; MQL5 uma por vez. "
                "Fila regulada por CPU/RAM e orçamento de tempo/disco.")
            self.status.configure(text="Plano pronto" if plan["eligible"] else "Nenhuma combinação elegível; confira as exclusões.")
            self.run_button.state(["!disabled"] if plan["eligible"] else ["disabled"])
            self.run_button.configure(text="Executar plano")
        def work():
            try:
                return backtest_scope.preview(self.ui.app.db_manager, self.ui.app.config, self.robot,
                    symbols=requested[2], start=requested[0], end=requested[1], preset=self.preset,
                    manifest_path=self.manifest_path, guard=self._guard)
            except ValueError:
                if not self.stop.is_set():
                    raise
        self.ui.run_bg(work, done)

    def show_detail(self, event=None):
        selected = self.tree.selection()
        if selected and self.plan:
            row = next(r for r in self.plan["rows"] if self._row_id(r) == selected[0])
            self.detail.configure(text="\n".join([row["reason"]] + row["limitations"]).strip() or "Elegível no recorte informado.")
            record = self.records.get(selected[0])
            if record:
                self.detail.configure(text=self.detail.cget("text") + f"\nRun {record['run_id']} · {record['symbol']} {record['timeframe']}\n"
                    f"Operações {fmt_int(record['metrics'].get('trades'))} · resultado {fmt_num(record['metrics'].get('net_profit'))} "
                    + (record['metrics'].get('currency') or '(moeda não informada)'))

    def execute(self):
        if self.active or not self.plan:
            return
        if tuple(self.assets.selection()) != tuple(self.plan["symbols"]):
            self.invalidate(); return
        if not self.is_current():
            self.status.configure(text="Seleção mudou; reabra o plano."); self.run_button.state(["disabled"]); return
        if self.robot.kind == "mql5" and getattr(self.ui.app, "offline", False):
            self.status.configure(text="Reabra sem --offline para executar o Tester."); return
        plan = self.plan
        self.stop.clear(); self.pending = True
        self.prepare_button.state(["disabled"]); self.run_button.state(["disabled"])
        self.all_assets_button.state(["disabled"])
        for entry in self.entries:
            entry.state(["disabled"])
        self.cancel_button.configure(text="Cancelar"); self.status.configure(text="Conferindo plano e iniciando fila…")

        def work():
            batch = backtest_scope.create_runner(self.ui.app.db_manager, self.ui.app.config, self.robot,
                                                 plan["batch_id"], on_event=self.events.put)
            self.batch = batch
            if self.stop.is_set():
                batch.cancel()
            batch.start()
            while not batch.join(.25):
                if self.stop.is_set():
                    batch.cancel()
            return batch

        def done(batch, error):
            self.pending = False
            if not self.exists():
                return
            self.cancel_button.configure(text="Fechar")
            self.prepare_button.state(["!disabled"])
            self.all_assets_button.state(["!disabled"])
            if not self.manifest_path:
                for entry in self.entries:
                    entry.state(["!disabled"])
            if error:
                self.status.configure(text=str(error))
            else:
                ok = sum(item["run"] is not None for item in batch.results)
                self.status.configure(text=f"{ok}/{plan['eligible']} concluído(s)" + (f" · {batch.error}" if batch.error else ""))
                self.completed(batch)
                if ok < plan["eligible"]:
                    self.run_button.configure(text="Retomar plano"); self.run_button.state(["!disabled"])
                else:
                    self.run_button.configure(text="Concluído")
            if self.closing:
                self.window.destroy()
        self.ui.run_bg(work, done)

    def poll(self):
        self._poll_id = None
        try:
            while True:
                event = self.events.get_nowait()
                job = event.get("job")
                row_id = self._row_id(dict(symbol=job.symbol, timeframe=job.timeframe)) if job and self.plan else None
                if row_id and self.tree.exists(row_id):
                    if event.get("run") is not None:
                        self.records[row_id] = event["run"]
                    values = list(self.tree.item(row_id, "values"))
                    values[2] = "Executando" if event["kind"] == runner.STARTED else "Concluído" if event.get("run") is not None else "Interrompido" if event.get("interrupted") else "Falhou"
                    if event.get("error"):
                        values[3] = event["error"]
                    self.tree.item(row_id, values=values)
                if event["kind"] == runner.DONE:
                    self.status.configure(text=f"{event['completed']}/{event['total']} processado(s)")
        except Empty:
            pass
        self._poll_id = self.window.after(150, self.poll)

    def cancel(self):
        self.stop.set()
        if self.batch and self.batch.running:
            self.batch.cancel()
        if self.exists():
            self.status.configure(text="Cancelando; aguardando a liberação do trabalho próprio…")

    def close(self):
        if self.active:
            self.closing = True; self.cancel()
        else:
            self.window.destroy()

    def cancel_or_close(self):
        if self.active:
            self.cancel()
        else:
            self.close()

    def _destroyed(self, event):
        if event.widget == self.window:
            self.stop.set()
            if self.batch and self.batch.running:
                self.batch.cancel()
            if self._poll_id is not None:
                self.window.after_cancel(self._poll_id); self._poll_id = None
