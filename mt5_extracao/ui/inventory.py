"""Aba "Minha base": o que já foi extraído (ativo x timeframe), período coberto e se está em dia."""
import tkinter as tk
from tkinter import ttk

from mt5_extracao import study, timeframes
from mt5_extracao.ui.theme import Tooltip, card, fmt_int, scrolled_tree

COLUMNS = ("Ativo", "Timeframe", "Barras", "Primeiro registro", "Último registro", "Atualização")
EXPORT_FORMATS = (("CSV", "csv"), ("Excel", "excel"), ("Parquet", "parquet"), ("DuckDB", "duckdb"))


class InventoryPanel:
    def __init__(self, parent, ui):
        self.ui = ui
        self.rows = {}        # iid da linha -> item do inventário
        self._sort = ("Ativo", False)
        self.frame = ttk.Frame(parent, padding=12)

        body = card(self.frame, "Minha base de dados", "tudo o que já foi extraído e está pronto para estudo")
        body.master.pack(fill=tk.BOTH, expand=True)
        self.summary = ttk.Label(body, text="Lendo a base...", style="Muted.TLabel")
        self.summary.pack(anchor="w", pady=(0, 8))

        actions = ttk.Frame(body, style="Surface.TFrame")
        actions.pack(fill=tk.X, side=tk.BOTTOM, pady=(10, 0))
        self.analyze_button = ttk.Button(actions, text="Analisar", style="Accent.TButton", command=self.analyze)
        self.analyze_button.pack(side=tk.LEFT)
        self.select_button = ttk.Button(actions, text="Adicionar à seleção", command=self.add_to_selection)
        self.select_button.pack(side=tk.LEFT, padx=(8, 0))
        Tooltip(self.select_button, "Coloca o ativo na seleção da aba Ativos, para atualizar ou extrair mais dados.",
                ui.theme)
        self.export_button = ttk.Menubutton(actions, text="Exportar ▾", style="TButton")
        menu = tk.Menu(self.export_button, tearoff=0)
        ui.theme.style_menu(menu)
        for label, fmt in EXPORT_FORMATS:
            menu.add_command(label=label, command=lambda f=fmt: self.export(f))
        self.export_button["menu"] = menu
        self.export_button.pack(side=tk.LEFT, padx=(8, 0))
        ttk.Button(actions, text="Atualizar lista", command=ui.refresh_inventory).pack(side=tk.RIGHT)

        tree_frame, self.tree = scrolled_tree(body, COLUMNS, (150, 110, 110, 170, 170, 130),
                                              ("w", "w", "e", "w", "w", "w"), height=14)
        tree_frame.pack(fill=tk.BOTH, expand=True)
        for col in COLUMNS:
            self.tree.heading(col, command=lambda c=col: self.sort_by(c))
        self.tree.bind("<Double-Button-1>", lambda e: self.analyze())
        self.tree.bind("<<TreeviewSelect>>", lambda e: self._update_buttons())
        self._update_buttons()

    def _sort_key(self, item, column):
        tf = timeframes.parse(item["timeframe"])
        return {"Ativo": (item["symbol"], tf.minutes if tf else 0),
                "Timeframe": (tf.minutes if tf else 0, item["symbol"]),
                "Barras": item["rows"],
                "Primeiro registro": str(item["start"] or ""),
                "Último registro": str(item["end"] or ""),
                "Atualização": str(item["end"] or "")}[column]

    def sort_by(self, column):
        self._sort = (column, not self._sort[1] if self._sort[0] == column else False)
        self.show(list(self.rows.values()))

    def show(self, inventory):
        column, reverse = self._sort
        inventory = sorted(inventory, key=lambda item: self._sort_key(item, column), reverse=reverse)
        self.tree.delete(*self.tree.get_children())
        self.rows = {}
        for item in inventory:
            tf = timeframes.parse(item["timeframe"])
            fresh = study.freshness(item["end"], tf.minutes if tf else 1)
            iid = self.tree.insert("", tk.END, values=(
                item["symbol"], item["timeframe"], fmt_int(item["rows"]),
                item["start"].strftime("%d/%m/%Y %H:%M") if item["start"] else "—",
                item["end"].strftime("%d/%m/%Y %H:%M") if item["end"] else "—",
                "✓ em dia" if fresh == "em dia" else f"⚠ {fresh}"))
            self.rows[iid] = item
        if inventory:
            symbols = len({item["symbol"] for item in inventory})
            self.summary.config(text=f"{symbols} ativo(s) · {len(inventory)} série(s) · "
                                     f"{fmt_int(sum(item['rows'] for item in inventory))} barras. "
                                     "Duplo clique abre a análise; ⚠ indica série desatualizada "
                                     "(use \"Atualizar até agora\" em Extrair histórico).")
        else:
            self.summary.config(text="A base ainda está vazia. Na aba Ativos, escolha um ativo e clique em "
                                     "\"Extrair histórico\".")
        self._update_buttons()

    def selected(self):
        selection = self.tree.selection()
        return self.rows.get(selection[0]) if selection else None

    def _update_buttons(self):
        state = ["!disabled"] if self.selected() else ["disabled"]
        for button in (self.analyze_button, self.select_button, self.export_button):
            button.state(state)

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
