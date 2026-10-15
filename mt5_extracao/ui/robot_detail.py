"""Painel do robô (aba "Robôs"): visão geral, testes, bateria de segurança e tempo real."""
import os
import tkinter as tk
from tkinter import ttk

from mt5_extracao import study
from mt5_extracao.robots import backtest, catalog, risk
from mt5_extracao.ui.theme import Tooltip, card, fmt_int, fmt_num, scrolled_tree

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
    "• Operação em demo: dias rodando, desvio em relação ao backtest e teste do botão de parada "
    "(ainda não disponível).\n\n"
    "Por enquanto, pela linha de comando:")


def battery_text(robot) -> str:
    """Texto da aba Bateria: a explicação e o comando ou, se já rodou, o resultado da bateria mais recente."""
    slug = robot.key.split(":", 1)[1]
    command = f"  mt5x robots battery {slug}"
    if not robot.checks:
        return f"{BATTERY_TEXT}\n\n{command}"
    check = robot.checks[0]
    lines = [("✓ Aprovada" if check["approved"] else "✗ Reprovada") + f" em {check['created_at'][:16]}" +
             (f", preset {check['preset']}" if check["preset"] else "") +
             f" · código e preset: {check['static_status']} · estatística: {check['statistical_status']}"]
    if check["approved"] and not check["allows_real"]:
        lines.append("Sem a parte de código conferida, o robô não é liberado para conta real.")
    for item in check["items"]:
        lines.append(f"{item['icon']} {item['name']}: {item['detail']}")
        if item["fix"] and item["status"] != "ok":
            lines.append(f"      {item['fix']}")
    lines += ["", "Aprovação não é garantia de resultado. Para rodar de novo:", command]
    return "\n".join(lines)

LIVE_TEXT_MQL5 = (
    "Quem envia as ordens é o robô, dentro do MetaTrader 5. Este programa instala, inicia, acompanha e para.\n\n"
    "Para iniciar em demo: robô compilado sem erros e bateria de segurança aprovada.\n"
    "Para iniciar em conta real: tudo isso, mais um período em demo com resultado dentro do esperado, risco por "
    "operação dentro do teto e confirmação digitando o número da conta. Conta real vem desligada de fábrica "
    "e ainda não está disponível.\n\n"
    "Conta demo, por enquanto pela linha de comando:\n\n"
    "  mt5x robots start <robô> --symbol <ativo> --tf <timeframe> --close-terminal\n"
    "  mt5x robots monitor <robô>\n"
    "  mt5x robots stop        (PARAR TUDO)")

LIVE_TEXT_PYTHON = (
    "Esta é uma estratégia de pesquisa: roda backtest com a sua base e não opera no MetaTrader 5.\n\n"
    "Para operar, ela precisa de uma versão em MQL5 sobre a camada de segurança do HADOUKEN. "
    "Este programa nunca envia ordens.")


# (legenda, chave em metrics, formato, dica)
RUN_TILES = (("Operações", "trades", "int", "Operações fechadas no período."),
             ("Acerto", "win_rate", "pct", "Parcela das operações que terminaram com lucro."),
             ("Profit factor", "profit_factor", "num", "Soma dos lucros dividida pela soma das perdas. Acima de 1 ganha "
                                                       "mais do que perde; a validação exige mais de 1,3."),
             ("Expectativa", "expectancy_r", "r", "Resultado médio por operação, em múltiplos do risco inicial (R)."),
             ("Resultado", "return_pct", "pct", "Resultado sobre o capital inicial, já com spread, slippage e corretagem."),
             ("Queda máxima", "max_drawdown_pct", "pct", "Maior queda do capital desde um pico, nas operações fechadas."))


def format_metric(value, kind):
    if value is None:
        return "—"
    if kind == "int":
        return fmt_int(value)
    if kind == "pct":
        return fmt_num(value * 100, 1, "%")
    return fmt_num(value, 2, " R" if kind == "r" else "")


def draw_equity(canvas, points, theme, labels=True):
    """
    Curva de capital das operações fechadas: uma linha, a referência do capital inicial tracejada e,
    com labels=True, os valores mínimo, máximo e final. Devolve a função que converte x em ponto (para o cursor).
    """
    c = theme.colors
    canvas.delete("all")
    width, height = max(canvas.winfo_width(), 40), max(canvas.winfo_height(), 20)
    values = [p[1] for p in points]
    if len(values) < 2:
        canvas.create_text(8, height / 2, anchor="w", fill=c["muted"], font=theme.font_small,
                           text="Nenhuma operação no período.")
        return None
    low, high = min(values), max(values)
    span = (high - low) or 1.0
    left, right = 6, (86 if labels else 6)
    top, bottom = (10, 10) if labels else (3, 3)

    def x_of(i):
        return left + i * (width - left - right) / (len(values) - 1)

    def y_of(v):
        return top + (high - v) * (height - top - bottom) / span

    canvas.create_line(left, y_of(values[0]), width - right, y_of(values[0]), fill=c["axis"], dash=(3, 3))
    coords = [n for i, v in enumerate(values) for n in (x_of(i), y_of(v))]
    canvas.create_line(*coords, fill=c["series"][0], width=2, joinstyle="round")
    if labels:
        x = width - right + 6
        marks = {"máx.": high, "mín.": low, "final": values[-1]}
        used = []
        for name, value in sorted(marks.items(), key=lambda kv: -kv[1]):
            y = min(max(y_of(value), 8), height - 8)
            while any(abs(y - other) < 13 for other in used):     # rótulos não se sobrepõem
                y += 13
            used.append(y)
            canvas.create_text(x, y, anchor="w", fill=c["ink2"], font=theme.font_small,
                               text=f"{name} {fmt_int(value)}")
        canvas.create_oval(x_of(len(values) - 1) - 4, y_of(values[-1]) - 4, x_of(len(values) - 1) + 4,
                           y_of(values[-1]) + 4, fill=c["series"][0], outline=c["surface"], width=2)

    def locate(x):
        i = round((x - left) * (len(values) - 1) / max(1, width - left - right))
        i = min(max(i, 0), len(values) - 1)
        return i, x_of(i), y_of(values[i])
    return locate


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
        tree_frame, self.inputs_tree = scrolled_tree(overview, ("Valor", "Descrição"), (95, 180), height=6,
                                                     xscroll=True)
        self.inputs_tree.configure(show="tree headings")
        self.inputs_tree.heading("#0", text="Parâmetro", anchor="w")
        self.inputs_tree.column("#0", width=110, stretch=False)
        self.inputs_tree.column("Valor", width=95, stretch=False)
        tree_frame.pack(fill=tk.BOTH, expand=True)

        # Backtest
        self.tests_text = ttk.Label(tests, text="", style="Surface.TLabel", wraplength=400, justify=tk.LEFT)
        self.tests_text.pack(anchor="w")
        self.run_box = ttk.Frame(tests, style="Surface.TFrame")
        controls = ttk.Frame(self.run_box, style="Surface.TFrame")
        controls.pack(fill=tk.X, pady=(8, 0))
        self.symbol_var, self.timeframe_var = tk.StringVar(), tk.StringVar()
        self.period_var = tk.StringVar(value=study.PERIODS[-1][0])
        for column, (label, var, width) in enumerate((("Ativo", self.symbol_var, 10),
                                                      ("Timeframe", self.timeframe_var, 10),
                                                      ("Período", self.period_var, 8))):
            ttk.Label(controls, text=label, style="Muted.TLabel").grid(row=0, column=column, sticky="w")
            combo = ttk.Combobox(controls, textvariable=var, state="readonly", width=width)
            combo.grid(row=1, column=column, sticky="w", padx=(0, 8))
            setattr(self, ("symbol", "timeframe", "period")[column] + "_combo", combo)
        self.period_combo.configure(values=[label for label, _days in study.PERIODS])
        self.symbol_combo.bind("<<ComboboxSelected>>", lambda e: self._symbol_changed())
        self.run_button = ttk.Button(controls, text="Rodar backtest", style="Accent.TButton", command=self.run_backtest)
        self.run_button.grid(row=1, column=3, sticky="w")
        self.run_status = ttk.Label(self.run_box, text="", style="Muted.TLabel", wraplength=400, justify=tk.LEFT)
        self.run_status.pack(anchor="w", pady=(6, 4))
        tiles = ttk.Frame(self.run_box, style="Surface.TFrame")
        tiles.pack(fill=tk.X)
        self.run_tiles = {}
        for index, (label, key, _kind, tip) in enumerate(RUN_TILES):
            tiles.columnconfigure(index % 3, weight=1, uniform="run")
            cell = ttk.Frame(tiles, style="Surface.TFrame")
            cell.grid(row=index // 3, column=index % 3, sticky="w", pady=(0, 6))
            value = ttk.Label(cell, text="—", style="Heading.TLabel")
            value.pack(anchor="w")
            ttk.Label(cell, text=label, style="Muted.TLabel").pack(anchor="w")
            Tooltip(cell, tip, self.theme)
            self.run_tiles[key] = value
        self.equity_canvas = tk.Canvas(self.run_box, height=130, highlightthickness=0, borderwidth=0,
                                       background=self.theme.colors["surface"])
        self.equity_canvas.pack(fill=tk.BOTH, expand=True, pady=(2, 0))
        self.equity_canvas.bind("<Configure>", lambda e: self._draw_run())
        self.equity_canvas.bind("<Motion>", self._hover)
        self.equity_canvas.bind("<Leave>", lambda e: self._hover(None))
        self.equity_readout = ttk.Label(self.run_box, text="", style="Muted.TLabel")
        self.equity_readout.pack(anchor="w")
        self.shown_run = None
        self._locate = None

        self.validations_box = ttk.Frame(tests, style="Surface.TFrame")
        head = ttk.Frame(self.validations_box, style="Surface.TFrame")
        head.pack(fill=tk.X, pady=(8, 4))
        ttk.Label(head, text="Validações gravadas", style="Heading.TLabel").pack(side=tk.LEFT)
        self.validate_button = ttk.Button(head, text="Validar estratégias...",
                                          command=lambda: self.ui.open_strategy_dialog("validate"))
        self.validate_button.pack(side=tk.RIGHT)
        Tooltip(self.validate_button, "Walk-forward, robustez de ±20% e Monte Carlo com a sua base. É a validação, "
                                      "e não um backtest isolado, que aprova ou reprova a estratégia.", self.theme)
        tests_frame, self.tests_tree = scrolled_tree(
            self.validations_box, ("Ativo", "Timeframe", "Veredito", "Profit factor", "Operações", "Drawdown"),
            (70, 70, 90, 80, 70, 70), ("w", "w", "w", "e", "e", "e"), height=3)
        tests_frame.pack(fill=tk.X)

        # Bateria e tempo real
        self.battery_text = ttk.Label(battery, text="", style="Surface.TLabel", wraplength=400, justify=tk.LEFT)
        self.battery_text.pack(anchor="w")
        self.live_text = ttk.Label(live, text="", style="Surface.TLabel", wraplength=400, justify=tk.LEFT)
        self.live_text.pack(anchor="w")

        self._wrapped = (self.title, self.subtitle, self.description, self.notes, self.risk_reasons,
                         self.tests_text, self.run_status, self.live_text, self.battery_text)
        self.theme.on_change(self._theme_changed)
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
        self.battery_text.config(text=battery_text(robot))
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
        self.run_box.pack_forget()
        self.validations_box.pack_forget()
        if robot.kind == catalog.MQL5:
            slug = robot.key.split(":", 1)[1]
            text = ("O backtest de robôs MQL5 roda no Strategy Tester do MetaTrader 5, com o MT5 fechado. "
                    f"Por enquanto, pela linha de comando:\n\n  mt5x robots compile {slug}\n"
                    f"  mt5x robots backtest {slug} --symbol <ativo> --tf <timeframe>")
            if robot.runs:
                last, m = robot.runs[0], robot.runs[0]["metrics"]
                text += (f"\n\nÚltimo teste ({last['created_at'][:10]}, {last['symbol']} {last['timeframe']}): "
                         f"{fmt_int(m['trades'])} operações, profit factor {fmt_num(m['profit_factor'])}, resultado "
                         f"{fmt_num(m['return_pct'] * 100, 1, '%')}, queda máxima "
                         f"{fmt_num(m['max_drawdown_pct'] * 100, 1, '%')}.")
            self.tests_text.config(text=text)
            return
        self.tests_text.config(text="Backtest com as barras da sua base, custos e lote pelo risco. "
                                    "Não garante resultado futuro.")
        self.run_box.pack(fill=tk.BOTH, expand=True)
        self.validations_box.pack(fill=tk.X)
        series = self.ui.symbol_series
        symbols = sorted(series)
        self.symbol_combo.configure(values=symbols)
        last = robot.runs[0] if robot.runs else None
        if self.symbol_var.get() not in symbols:
            self.symbol_var.set(last["symbol"] if last and last["symbol"] in symbols else (symbols[0] if symbols else ""))
        self._symbol_changed()
        self.run_button.state(["!disabled"] if symbols else ["disabled"])
        if symbols:
            self.show_run(last)
        else:
            self.show_run(last, "A base está vazia: extraia o histórico de um ativo na aba Ativos.")
        for r in robot.results:
            self.tests_tree.insert("", tk.END, values=(
                r["symbol"], r["timeframe"], "✓ aprovada" if r["approved"] else "✕ reprovada",
                fmt_num(r["profit_factor"]), fmt_int(r["trades"]), fmt_num(r["max_drawdown"] * 100, 1, "%")))
        if not robot.results:
            self.tests_tree.insert("", tk.END, values=("—", "", "nenhuma ainda", "", "", ""))

    def inventory_changed(self):
        """A base mudou (nova extração): atualiza os ativos e timeframes oferecidos para o backtest."""
        if self.robot is not None and self.robot.kind == catalog.PYTHON:
            self._show_tests()

    def _symbol_changed(self):
        """Timeframes que o ativo escolhido tem na base (o indicado pela estratégia vem primeiro, se existir)."""
        labels = [item["timeframe"] for item in self.ui.symbol_series.get(self.symbol_var.get(), [])]
        self.timeframe_combo.configure(values=labels)
        if self.timeframe_var.get() not in labels:
            from mt5_extracao import timeframes
            wanted = set(self.robot.timeframes) if self.robot else set()
            preferred = [label for label in labels if getattr(timeframes.parse(label), "name", "") in wanted]
            self.timeframe_var.set((preferred or labels or [""])[-1])

    def run_backtest(self):
        robot, symbol, timeframe = self.robot, self.symbol_var.get(), self.timeframe_var.get()
        if not robot or not symbol or not timeframe:
            return
        item = next((i for i in self.ui.symbol_series.get(symbol, []) if i["timeframe"] == timeframe), None)
        days = dict(study.PERIODS).get(self.period_var.get())
        start = study.period_start(item["end"], days) if item else None
        db, config = self.ui.app.db_manager, getattr(self.ui.app, "config", None)
        self.run_button.state(["disabled"])
        self.run_status.config(text=f"Rodando {robot.name} em {symbol} ({timeframe})...")

        def done(run, error):
            self.run_button.state(["!disabled"])
            if error:
                self.run_status.config(text=f"Não foi possível rodar: {error}")
                return
            self.ui.log(f"Backtest de {robot.name} em {symbol} {run['timeframe']}: {run['metrics']['trades']} operações.")
            if self.robot is robot:
                self.show_run(run)
            self.ui.robots_panel.reload()      # o card passa a mostrar o resultado
        self.ui.run_bg(lambda: backtest.run_python(db, config, robot, symbol, timeframe, start), done)

    def show_run(self, run, empty_text="Escolha o ativo e o período e clique em \"Rodar backtest\"."):
        self.shown_run = run
        for _label, key, kind, _tip in RUN_TILES:
            self.run_tiles[key].config(text=format_metric(run["metrics"].get(key), kind) if run else "—")
        if run:
            m = run["metrics"]
            notes = [f"{run['symbol']} {run['timeframe']} · {fmt_int(run['bars'])} barras · "
                     f"{run['data_start'][:10]} a {run['data_end'][:10]} · rodado em {run['created_at'][:16]}"]
            if m.get("skipped_min_lot"):
                notes.append(f"{m['skipped_min_lot']} sinal(is) ignorado(s): lote para o risco abaixo do mínimo.")
            if not m.get("spec_known", True):
                notes.append("Especificação do contrato ausente: valores em dinheiro são aproximados (mt5x specs).")
            if m.get("trades", 0) < 30:
                notes.append("Poucas operações: o resultado diz pouco. Use um período maior e a validação.")
            self.run_status.config(text="\n".join(notes))
        else:
            self.run_status.config(text=empty_text)
        self._draw_run()

    def _draw_run(self):
        self.equity_readout.config(text="")
        points = self.shown_run["equity"] if self.shown_run else []
        if not self.shown_run:
            self.equity_canvas.delete("all")
            self._locate = None
            return
        self._locate = draw_equity(self.equity_canvas, points, self.theme)

    def _hover(self, event):
        """Cursor sobre a curva: mostra a data e o capital do ponto mais próximo."""
        canvas = self.equity_canvas
        canvas.delete("hover")
        if event is None or not self._locate or not self.shown_run:
            self.equity_readout.config(text="")
            return
        index, x, y = self._locate(event.x)
        c = self.theme.colors
        canvas.create_line(x, 0, x, canvas.winfo_height(), fill=c["axis"], tags="hover")
        canvas.create_oval(x - 4, y - 4, x + 4, y + 4, fill=c["series"][0], outline=c["surface"], width=2, tags="hover")
        when, value = self.shown_run["equity"][index]
        initial = self.shown_run["equity"][0][1]
        change = (value / initial - 1) * 100 if initial else 0.0
        self.equity_readout.config(text=f"{when[:16]} · capital {fmt_int(value)} ({fmt_num(change, 1, '%')})")

    def _theme_changed(self):
        self.equity_canvas.configure(background=self.theme.colors["surface"])
        self._draw_run()

    def open_folder(self):
        if self.robot and self.robot.path:
            self.ui._open_path(os.path.dirname(self.robot.path))
