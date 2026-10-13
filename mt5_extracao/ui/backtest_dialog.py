"""Diálogo do botão "Iniciar Backtest": escolhe os robôs que serão testados nas séries marcadas da base."""
import tkinter as tk
from tkinter import ttk

from mt5_extracao.robots import catalog


class BacktestDialog:
    """
    Lista os robôs e as estratégias do catálogo com uma caixa de seleção cada. `on_run(robôs)` é chamado ao
    confirmar. `series` são os itens do inventário (ativo x timeframe) marcados em "Minha base".
    """

    def __init__(self, ui, series, robots, on_run):
        self.ui, self.series, self.robots, self.on_run = ui, list(series), list(robots), on_run
        self.vars = {}           # chave do robô -> BooleanVar
        self.window = tk.Toplevel(ui.root)
        self.window.title("Iniciar backtest")
        self.window.geometry("560x520")
        self.window.minsize(460, 380)
        self.window.transient(ui.root)

        outer = ttk.Frame(self.window, padding=14)
        outer.pack(fill=tk.BOTH, expand=True)
        symbols = list(dict.fromkeys(item["symbol"] for item in self.series))
        ttk.Label(outer, text=f"{len(self.series)} série(s) marcada(s): " + ", ".join(symbols[:6]) +
                  ("..." if len(symbols) > 6 else ""), style="Page.Muted.TLabel", wraplength=520,
                  justify=tk.LEFT).pack(anchor="w")
        ttk.Label(outer, text="Escolha os robôs e as estratégias. Cada um é testado em cada série marcada.",
                  style="Page.Muted.TLabel", wraplength=520, justify=tk.LEFT).pack(anchor="w", pady=(2, 8))

        quick = ttk.Frame(outer)
        quick.pack(fill=tk.X, pady=(0, 6))
        ttk.Button(quick, text="Selecionar todos os robôs", command=lambda: self.set_all(True)).pack(side=tk.LEFT)
        ttk.Button(quick, text="Limpar seleção", command=lambda: self.set_all(False)).pack(side=tk.LEFT, padx=(8, 0))

        bottom = ttk.Frame(outer)
        bottom.pack(fill=tk.X, side=tk.BOTTOM, pady=(10, 0))
        self.run_button = ttk.Button(bottom, style="Accent.TButton", command=self.confirm)
        self.run_button.pack(side=tk.RIGHT)
        ttk.Button(bottom, text="Cancelar", command=self.window.destroy).pack(side=tk.RIGHT, padx=(0, 8))
        self.note = ttk.Label(outer, text="", style="Page.Muted.TLabel", wraplength=520, justify=tk.LEFT)
        self.note.pack(side=tk.BOTTOM, anchor="w", pady=(8, 0))

        box = ttk.Frame(outer, style="Card.TFrame", padding=8)
        box.pack(fill=tk.BOTH, expand=True)
        canvas = tk.Canvas(box, highlightthickness=0, borderwidth=0, background=ui.theme.colors["surface"])
        bar = ttk.Scrollbar(box, orient="vertical", command=canvas.yview)
        canvas.configure(yscrollcommand=bar.set)
        bar.pack(side=tk.RIGHT, fill=tk.Y)
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        inner = ttk.Frame(canvas, style="Surface.TFrame")
        window = canvas.create_window(0, 0, window=inner, anchor="nw")
        inner.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.bind("<Configure>", lambda e: canvas.itemconfigure(window, width=e.width))
        if not self.robots:
            ttk.Label(inner, text="Nenhum robô no catálogo. Importe um na aba Robôs.", style="Muted.TLabel").pack(
                anchor="w", padx=4, pady=8)
        for robot in self.robots:
            var = tk.BooleanVar(value=False)
            self.vars[robot.key] = var
            ttk.Checkbutton(inner, text=f"{robot.name}  ({robot.kind_label})", variable=var, style="Surface.TCheckbutton",
                            command=self._update).pack(anchor="w", padx=4, pady=1)
        self._update()

    def set_all(self, value):
        for var in self.vars.values():
            var.set(value)
        self._update()

    def chosen(self):
        return [robot for robot in self.robots if self.vars[robot.key].get()]

    def _update(self):
        robots = self.chosen()
        total = len(robots) * len(self.series)
        self.run_button.config(text=f"Executar {total} Backtest(s)" if total else "Executar Backtests")
        self.run_button.state(["!disabled"] if total else ["disabled"])
        mql5 = sum(r.kind == catalog.MQL5 for r in robots)
        self.note.config(text=f"{len(self.series)} série(s) × {len(robots)} robô(s). " +
                         ("Robôs MQL5 rodam no Strategy Tester, um por vez, com o MetaTrader 5 fechado."
                          if mql5 else ""))

    def confirm(self):
        robots = self.chosen()
        if robots:
            self.window.destroy()
            self.on_run(robots)
