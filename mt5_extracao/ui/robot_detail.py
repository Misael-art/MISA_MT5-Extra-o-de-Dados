"""Painel do robô (aba "Robôs"): visão geral, testes, bateria de segurança e tempo real."""
import os
import tkinter as tk
from tkinter import ttk

from mt5_extracao.robots import catalog, risk
from mt5_extracao.ui.theme import card, fmt_int, fmt_num, scrolled_tree

# nível -> (ícone, texto, estilo): cor sempre acompanhada de ícone e texto
RISK_STYLES = {risk.LOW: ("✓", "Risco baixo", "Good.TLabel"),
               risk.MODERATE: ("⚠", "Risco moderado", "Warn.TLabel"),
               risk.HIGH: ("✕", "Risco alto", "Bad.TLabel"),
               risk.UNKNOWN: ("?", "Risco não avaliado", "Surface.TLabel")}

SOURCE_DEFAULTS = "Padrão do código"

BATTERY_TEXT = (
    "A bateria de segurança confere três grupos de itens e explica cada reprovação:\n\n"
    "• Código e preset: compila sem erros, stop obrigatório, sem martingale, grid ou preço médio, toda ordem "
    "passando pela camada de segurança, sem DLL externa, risco e limites dentro dos tetos.\n"
    "• Estatística: pelo menos 200 operações, profit factor acima de 1,3, expectativa positiva com custos, "
    "drawdown de Monte Carlo dentro do limite, teste fora da amostra, robustez de ±20% e estresse de custos.\n"
    "• Operação em demo: dias rodando, desvio em relação ao backtest e teste do botão de parada.\n\n"
    "Ainda não disponível nesta versão.")

LIVE_TEXT_MQL5 = (
    "Quem envia as ordens é o robô, dentro do MetaTrader 5. Este programa instala, inicia, acompanha e para.\n\n"
    "Para iniciar em demo: robô compilado sem erros e bateria de segurança aprovada.\n"
    "Para iniciar em conta real: tudo isso, mais um período em demo com resultado dentro do esperado, risco por "
    "operação dentro do teto e confirmação digitando o número da conta. Conta real vem desligada de fábrica.\n\n"
    "Ainda não disponível nesta versão.")

LIVE_TEXT_PYTHON = (
    "Esta é uma estratégia de pesquisa: roda backtest com a sua base e não opera no MetaTrader 5.\n\n"
    "Para operar, ela precisa de uma versão em MQL5 sobre a camada de segurança do HADOUKEN. "
    "Este programa nunca envia ordens.")


class RobotDetail:
    def __init__(self, parent, ui):
        self.ui = ui
        self.theme = ui.theme
        self.robot = None
        body = card(parent, padding=14)
        self.frame = body.master

        self.title = ttk.Label(body, text="Robôs", style="Title.TLabel", wraplength=420, justify=tk.LEFT)
        self.title.pack(anchor="w")
        self.subtitle = ttk.Label(body, text="Clique em um card para ver os detalhes.", style="Muted.TLabel",
                                  wraplength=420, justify=tk.LEFT)
        self.subtitle.pack(anchor="w", pady=(2, 8))

        self.tabs = ttk.Notebook(body, style="Compact.TNotebook")
        self.tabs.pack(fill=tk.BOTH, expand=True)
        overview = ttk.Frame(self.tabs, style="Surface.TFrame", padding=10)
        tests = ttk.Frame(self.tabs, style="Surface.TFrame", padding=10)
        battery = ttk.Frame(self.tabs, style="Surface.TFrame", padding=10)
        live = ttk.Frame(self.tabs, style="Surface.TFrame", padding=10)
        for tab, text in ((overview, "Visão geral"), (tests, "Backtest"), (battery, "Bateria"), (live, "Tempo real")):
            self.tabs.add(tab, text=text)

        # Visão geral
        self.description = ttk.Label(overview, text="", style="Surface.TLabel", wraplength=400, justify=tk.LEFT)
        self.description.pack(anchor="w")
        self.notes = ttk.Label(overview, text="", style="Warn.TLabel", wraplength=400, justify=tk.LEFT)
        self.notes.pack(anchor="w", pady=(6, 0))
        values_row = ttk.Frame(overview, style="Surface.TFrame")
        values_row.pack(fill=tk.X, pady=(10, 4))
        ttk.Label(values_row, text="Valores de", style="Muted.TLabel").pack(side=tk.LEFT)
        self.source_var = tk.StringVar(value=SOURCE_DEFAULTS)
        self.source_combo = ttk.Combobox(values_row, textvariable=self.source_var, state="readonly", width=24)
        self.source_combo.pack(side=tk.LEFT, padx=(6, 0))
        self.source_combo.bind("<<ComboboxSelected>>", lambda e: self._show_values())
        self.risk_label = ttk.Label(overview, text="", style="Surface.TLabel")
        self.risk_label.pack(anchor="w")
        self.risk_reasons = ttk.Label(overview, text="", style="Muted.TLabel", wraplength=400, justify=tk.LEFT)
        self.risk_reasons.pack(anchor="w", pady=(0, 8))
        actions = ttk.Frame(overview, style="Surface.TFrame")
        actions.pack(fill=tk.X, side=tk.BOTTOM, pady=(8, 0))
        self.folder_button = ttk.Button(actions, text="Abrir a pasta do robô", command=self.open_folder)
        self.folder_button.pack(side=tk.LEFT)
        self.files_label = ttk.Label(actions, text="", style="Muted.TLabel")
        self.files_label.pack(side=tk.LEFT, padx=(10, 0))
        tree_frame, self.inputs_tree = scrolled_tree(overview, ("Valor", "Descrição"), (150, 220), height=6)
        self.inputs_tree.configure(show="tree headings")
        self.inputs_tree.heading("#0", text="Parâmetro", anchor="w")
        self.inputs_tree.column("#0", width=170, stretch=False)
        self.inputs_tree.column("Valor", stretch=False)
        tree_frame.pack(fill=tk.BOTH, expand=True)

        # Backtest
        self.tests_text = ttk.Label(tests, text="", style="Surface.TLabel", wraplength=400, justify=tk.LEFT)
        self.tests_text.pack(anchor="w")
        self.validate_button = ttk.Button(tests, text="Validar estratégias...", style="Accent.TButton",
                                          command=lambda: self.ui.open_strategy_dialog("validate"))
        tests_frame, self.tests_tree = scrolled_tree(
            tests, ("Ativo", "Timeframe", "Veredito", "Profit factor", "Operações", "Drawdown"),
            (80, 80, 90, 80, 70, 70), ("w", "w", "w", "e", "e", "e"), height=8)
        self.tests_frame = tests_frame

        # Bateria e tempo real
        ttk.Label(battery, text=BATTERY_TEXT, style="Surface.TLabel", wraplength=400, justify=tk.LEFT).pack(anchor="w")
        self.live_text = ttk.Label(live, text="", style="Surface.TLabel", wraplength=400, justify=tk.LEFT)
        self.live_text.pack(anchor="w")

        self._wrapped = (self.title, self.subtitle, self.description, self.notes, self.risk_reasons,
                         self.tests_text, self.live_text, battery.winfo_children()[0])
        body.bind("<Configure>", self._rewrap)
        self.show(None)

    def _rewrap(self, event):
        for label in self._wrapped:
            label.configure(wraplength=max(220, event.width - 30))

    def show(self, robot):
        self.robot = robot
        if robot is None:
            self.title.config(text="Robôs")
            self.subtitle.config(text="Clique em um card para ver os detalhes.")
            self.tabs.pack_forget()
            return
        self.tabs.pack(fill=tk.BOTH, expand=True)
        self.title.config(text=robot.name)
        facts = [robot.kind_label, f"versão {robot.version}" if robot.version else "", robot.regime,
                 ", ".join(robot.timeframes), robot.markets, f"estágio: {robot.stage_label}"]
        self.subtitle.config(text=" · ".join(f for f in facts if f))
        self.description.config(text=robot.description or "Sem descrição.")
        self.notes.config(text="\n".join("⚠ " + note for note in robot.notes))
        sources = [SOURCE_DEFAULTS] + [f"Preset {name}" for name in robot.presets]
        self.source_combo.configure(values=sources)
        if self.source_var.get() not in sources:
            self.source_var.set(SOURCE_DEFAULTS)
        self._show_values()
        self.folder_button.state(["!disabled"] if robot.kind == catalog.MQL5 and robot.path else ["disabled"])
        count = len(robot.files)
        self.files_label.config(text=f"{count} arquivo(s): fonte, includes e presets" if count > 1 else "")
        self._show_tests()
        self.live_text.config(text=LIVE_TEXT_PYTHON if robot.kind == catalog.PYTHON else LIVE_TEXT_MQL5)

    def _show_values(self):
        """Parâmetros agrupados, com os valores do código ou do preset escolhido, e o risco correspondente."""
        robot = self.robot
        source = self.source_var.get()
        preset = source[len("Preset "):] if source.startswith("Preset ") else None
        values = robot.presets.get(preset, {})
        rating = robot.preset_risks.get(preset, robot.risk)
        icon, label, style = RISK_STYLES[rating.level]
        self.risk_label.config(text=f"{icon} {label}", style=style)
        self.risk_reasons.config(text=rating.summary + "\nA classe vem dos parâmetros; não é garantia.")
        tree = self.inputs_tree
        tree.delete(*tree.get_children())
        parents = {}
        for item in robot.inputs:
            parent = ""
            if item.group:
                if item.group not in parents:
                    parents[item.group] = tree.insert("", tk.END, text=item.group, open=True)
                parent = parents[item.group]
            shown = robot.display_value(item, values.get(item.name))
            changed = shown.split(" (")[0] != robot.display_value(item).split(" (")[0]
            tree.insert(parent, tk.END, text=item.name, values=(shown + (" *" if changed else ""), item.comment))
        if not robot.inputs:
            tree.insert("", tk.END, text="Sem parâmetros legíveis", values=("", "robô sem código-fonte"))

    def _show_tests(self):
        robot = self.robot
        self.tests_tree.delete(*self.tests_tree.get_children())
        self.validate_button.pack_forget()
        self.tests_frame.pack_forget()
        if robot.kind == catalog.MQL5:
            self.tests_text.config(text="O backtest de robôs MQL5 roda no Strategy Tester do MetaTrader 5 e a análise "
                                        "(Monte Carlo, checklist) é feita aqui.\n\nAinda não disponível nesta versão.")
            return
        if robot.results:
            self.tests_text.config(text="Validações gravadas (fora da amostra, com custos). Backtest não garante "
                                        "resultado futuro.")
        else:
            self.tests_text.config(text="Nenhuma validação gravada para esta estratégia. A validação usa a sua base: "
                                        "walk-forward, robustez de ±20% e Monte Carlo.")
        self.validate_button.pack(anchor="w", pady=(8, 8))
        self.tests_frame.pack(fill=tk.BOTH, expand=True)
        for r in robot.results:
            self.tests_tree.insert("", tk.END, values=(
                r["symbol"], r["timeframe"], "✓ aprovada" if r["approved"] else "✕ reprovada",
                fmt_num(r["profit_factor"]), fmt_int(r["trades"]), fmt_num(r["max_drawdown"] * 100, 1, "%")))

    def open_folder(self):
        if self.robot and self.robot.path:
            self.ui._open_path(os.path.dirname(self.robot.path))
