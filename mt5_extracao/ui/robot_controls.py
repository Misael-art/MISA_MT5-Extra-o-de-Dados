"""Controles dos cards e revisão de PLAY. Cálculos/autorização ficam no núcleo."""
import time
import tkinter as tk
from datetime import datetime, timedelta
from tkinter import messagebox, ttk

from mt5_extracao.robots import catalog, controls, portfolio, sessions
from mt5_extracao.ui import icons
from mt5_extracao.ui.components import DetailsPanel, ScrollableContent
from mt5_extracao.ui.theme import Tooltip, fmt_int, fmt_num


class CardControls:
    def __init__(self, panel):
        self.panel, self.ui, self.theme = panel, panel.ui, panel.theme
        self.style = ttk.Style(panel.frame)
        self.chosen, self.sessions, self.widgets, self.images = {}, {}, {}, {}
        self._pulse = None
        self._bright = False
        panel.frame.bind("<Destroy>", self._destroyed, add="+")
        self.ui.root.protocol("WM_DELETE_WINDOW", self._request_close)

    def context(self, robot):
        if robot.key not in self.chosen:
            last = robot.runs[0] if robot.runs else {}
            symbols = list(getattr(self.ui.app, "symbols", []) or [])
            self.chosen[robot.key] = (last.get("symbol") or (symbols[0] if symbols else ""),
                                      last.get("timeframe") or (robot.timeframes[0] if robot.timeframes else "H1"),
                                      (last.get("params") or {}).get("preset") or None)
        symbol, timeframe, preset = self.chosen[robot.key]
        if preset and preset not in robot.presets:
            preset = None
            self.chosen[robot.key] = symbol, timeframe, preset
        return controls.selection(robot, symbol, timeframe, preset)

    def _icon(self, action, accent=False, disabled=False):
        color = self.theme.colors["border"] if disabled else self.theme.colors["accent"] if accent else self.theme.colors["ink2"]
        size = max(14, int(self.theme.font_small.metrics("linespace")*.8))
        key = action, color, size
        if key not in self.images:
            self.images[key] = icons.robot_action(action, color, size)
        return self.images[key]

    def build(self, parent, robot):
        symbol, timeframe, preset = self.chosen[robot.key]
        active = self.sessions.get(robot.key)
        busy = bool(active and active.active)
        box = ttk.Frame(parent, style="Surface.TFrame")
        box.pack(fill=tk.X, pady=(8, 0))
        series_row = ttk.Frame(box, style="Surface.TFrame")
        series_row.pack(fill=tk.X)
        series_row.columnconfigure(0, weight=1)
        series_row.columnconfigure(1, weight=1)
        self.style.configure("Robot.Command.TButton", font=self.theme.font_small, padding=(5, 6))
        for column, (kind, text) in enumerate((("asset", "Ativo / TF"), ("preset", "Preset"))):
            selector = ttk.Button(series_row, text=text, image=self._icon("selector"), compound=tk.LEFT,
                                  style="Robot.Command.TButton", command=lambda k=kind: SelectionDialog(self, robot, k))
            selector.grid(row=0, column=column, sticky="ew", padx=(0 if column == 0 else 4, 0))
            if busy:
                selector.state(["disabled"])
        chosen = ttk.Label(box, text=f"{symbol or 'Escolha um ativo'} · {timeframe}\n{preset or controls.DEFAULT_PRESET}",
                           style="Muted.TLabel", justify=tk.LEFT, wraplength=260)
        chosen.pack(anchor="w", fill=tk.X, pady=(4, 0))
        chosen.bind("<Configure>", lambda e: chosen.configure(wraplength=max(40, e.width)))
        row = ttk.Frame(box, style="Surface.TFrame")
        row.pack(fill=tk.X, pady=(5, 0))
        buttons = {}
        for column, (mode, label) in enumerate((("play", "PLAY"), ("replay", "REPLAY"), ("backtest", "BACKTEST"))):
            button = ttk.Button(row, text=label, image=self._icon(mode, mode == "play"), compound=tk.LEFT,
                                style="Robot.Command.TButton", command=lambda m=mode: self.launch(robot, m))
            button.grid(row=0, column=column, sticky="ew", padx=(0 if column == 0 else 3, 0))
            row.columnconfigure(column, weight=1, uniform="commands")
            buttons[mode] = button
        layout_state = [None]

        def fit_actions(event):
            widths = {mode: button.winfo_reqwidth() for mode, button in buttons.items()}
            pair_width = max(widths["play"], 2*max(widths["replay"], widths["backtest"])+3)
            layout = ("inline" if event.width >= sum(widths.values())+6 else
                      "pair" if event.width >= pair_width else "column")
            if layout == layout_state[0]:
                return
            layout_state[0] = layout
            for column in range(3):
                used = layout == "inline" or column == 0 or layout == "pair" and column == 1
                row.columnconfigure(column, weight=int(used), uniform="commands" if layout == "pair" and used else "")
            positions = {"inline": ((0, 0, 1), (0, 1, 1), (0, 2, 1)),
                         "pair": ((0, 0, 2), (1, 0, 1), (1, 1, 1)),
                         "column": ((0, 0, 1), (1, 0, 1), (2, 0, 1))}[layout]
            for button, (line, column, span) in zip(buttons.values(), positions):
                button.grid_configure(row=line, column=column, columnspan=span,
                                      padx=(3 if column else 0, 0), pady=(3 if line else 0, 0))
        row.bind("<Configure>", fit_actions, add="+")
        context = self.context(robot)
        offline = bool(getattr(self.ui.app, "offline", False))
        if not context["run"] or busy or offline:
            buttons["play"].state(["disabled"])
            buttons["play"].configure(image=self._icon("play", disabled=True))
        for mode in ("replay", "backtest"):
            if busy or offline:
                buttons[mode].state(["disabled"])
                buttons[mode].configure(image=self._icon(mode, disabled=True))
        reason = ("Sessão em andamento · seleção bloqueada até encerrar." if busy else
                  "Análise local: reabra sem --offline para usar o MT5." if offline else context["reason"])
        hint = ttk.Label(box, text=reason or "PLAY disponível para revisão · duas confirmações", style="Muted.TLabel",
                         wraplength=260, justify=tk.LEFT)
        hint.pack(anchor="w", fill=tk.X, pady=(4, 0))
        hint.bind("<Configure>", lambda e: hint.configure(wraplength=max(40, e.width)))
        status = ttk.Label(box, text="Inativo", style="Muted.TLabel", wraplength=260)
        if active:
            status.pack(anchor="w", fill=tk.X, pady=(5, 0))
        status.bind("<Configure>", lambda e: status.configure(wraplength=max(40, e.width)))
        stop = ttk.Button(box, text="STOP · encerrar esta sessão", image=self._icon("stop"), compound=tk.LEFT,
                          command=lambda: self.stop(robot.key), style="Robot.Command.TButton")
        if busy:
            stop.pack(fill=tk.X, pady=(4, 0))
        elif active and active.report.is_file():
            ttk.Button(box, text="Ver relatório", command=lambda: ReportDialog(self.ui, active),
                       style="Robot.Command.TButton").pack(anchor="w", pady=(4, 0))
        self.widgets[robot.key] = {"buttons": buttons, "status": status, "stop": stop, "hint": hint}
        Tooltip(buttons["play"], "Revisa conta, carteira, preset e risco. Histórico negativo pede confirmação adicional.", self.theme)
        Tooltip(buttons["replay"], "Strategy Tester visual: cada tick baseado em ticks reais. Simulação histórica.", self.theme)
        Tooltip(buttons["backtest"], "Strategy Tester em sessão própria oculta/minimizada, sem fechar seu MT5.", self.theme)
        self._schedule()

    def _select(self, robot, label, value):
        selected = list(self.chosen[robot.key])
        selected[{"SELETOR · ativo": 0, "Timeframe": 1, "SELETOR · preset": 2}[label]] = (
            None if label == "SELETOR · preset" and value == controls.DEFAULT_PRESET else value)
        self.chosen[robot.key] = tuple(selected)
        self.panel.selected_key = robot.key
        self.panel.render()

    def _account(self):
        if getattr(self.ui.app, "offline", False):
            raise ValueError("O modo offline não executa terminais ou contas.")
        connector = self.ui.app.mt5_connector
        if not connector.is_initialized:
            raise ValueError("Conecte o MT5 antes de executar PLAY.")
        from mt5_extracao.mt5_backend import get_mt5
        return controls.account_identity(get_mt5())

    def launch(self, robot, mode):
        if getattr(self.ui.app, "offline", False):
            messagebox.showinfo("MT5", "Reabra sem --offline para executar o MT5.", parent=self.ui.root)
            return
        if self.sessions.get(robot.key) and self.sessions[robot.key].active:
            return
        symbol, timeframe, preset = self.chosen[robot.key]
        if not symbol:
            messagebox.showinfo("Seleção", "Escolha um ativo antes de continuar.", parent=self.ui.root)
            return
        if mode == "play":
            def read():
                from mt5_extracao.mt5_backend import get_mt5
                account = self._account()
                backend = get_mt5()
                terminal = backend.terminal_info()
                if not terminal or not terminal.connected or backend.positions_get() is None or backend.orders_get() is None:
                    raise ValueError("A conta/carteira não pôde ser lida por inteiro. Verifique a conexão.")
                snapshot = portfolio.snapshot(backend, self.panel.robots)
                if any(snapshot[k] != account[k] for k in ("login", "server", "demo")):
                    raise ValueError("A conta mudou durante a leitura. Revise novamente.")
                return account, snapshot

            def done(result, error):
                if error:
                    messagebox.showerror("PLAY", str(error), parent=self.ui.root)
                else:
                    ExecutionDialog(self, robot, mode, *result)
            self.ui.run_bg(read, done)
        else:
            ExecutionDialog(self, robot, mode)

    def start(self, robot, mode, symbol, timeframe, preset, start=None, end=None, account=None, consent=None):
        if self.sessions.get(robot.key) and self.sessions[robot.key].active:
            return
        session = sessions.Session(self.ui.app.db_manager, self.ui.app.config, robot, symbol, timeframe, preset,
                                   mode, start, end, account, consent)
        self.sessions[robot.key] = session
        session.state = "preparing"  # feedback antes de a thread começar
        self.panel.render()

        def done(result, error):
            self.panel.reload()
            if self.ui.root.winfo_exists():
                ReportDialog(self.ui, session)
        self.ui.run_bg(lambda: session.run(read_account=self._account if mode == "play" else None), done)

    def stop(self, key):
        session = self.sessions.get(key)
        if session and session.active:
            session.cancel()

    def _schedule(self):
        if self._pulse is None:
            self._pulse = self.panel.frame.after(650, self._tick)

    def _tick(self):
        self._pulse = None
        self._bright = not self._bright
        colors = self.theme.colors
        for key, session in self.sessions.items():
            widgets, frame = self.widgets.get(key), self.panel.cards.get(key)
            if not widgets or frame is None or not frame.winfo_exists():
                continue
            (state, text), count, profit = session.telemetry()
            if session.state == "queued":
                text = "Na fila · aguardando a passagem histórica em andamento"
            elif session.state == "preparing":
                text = "Preparando MT5 · sessão própria"
            elif session.state == "stopping":
                text = "Parando esta sessão…"
            elif not session.active:
                text = {"completed": "Concluído · relatório disponível", "failed": "Falhou · veja o relatório",
                        "cancelled": "Interrompido pelo usuário"}.get(session.state, "Inativo")
            else:
                text = f"{session.mode.upper()} · {session.symbol} {session.timeframe} · {text}"
                if count:
                    text += f" · {fmt_num(profit)} {getattr(session, 'currency', '') or '(moeda da sessão)'}"
            widgets["status"].configure(text=text)
            color = {"waiting": colors["accent"], "profit": colors["good"], "loss": colors["bad"]}.get(state, colors["border"])
            if session.active:
                name = f"Robot.Session.{state}.{int(self._bright)}.TFrame"
                self.style.configure(name, background=colors["surface"], relief="solid", borderwidth=2,
                                           bordercolor=color if self._bright else colors["border"])
                frame.configure(style=name)
        self._schedule()

    def _destroyed(self, event):
        if event.widget is self.panel.frame and self._pulse is not None:
            self.panel.frame.after_cancel(self._pulse)
            self._pulse = None

    def _request_close(self):
        active = [s for s in self.sessions.values() if s.active]
        if active:
            if not messagebox.askyesno("Sessões MT5 em andamento",
                    "Encerrar estas sessões antes de fechar a aplicação?\n\n"
                    "STOP impede novas ações dos Experts destas sessões; posições e ordens existentes "
                    "continuam na conta e precisam de acompanhamento.", parent=self.ui.root, default="no"):
                return
            for session in active:
                session.cancel()
            self.ui.root.after(200, self._close_when_stopped)
        else:
            self.ui.root.destroy()

    def _close_when_stopped(self):
        if any(s.active for s in self.sessions.values()):
            self.ui.root.after(200, self._close_when_stopped)
        else:
            self.ui.root.destroy()


class SelectionDialog:
    """Seletores pesquisáveis: o universo da corretora não ocupa o card nem cria 12 mil widgets."""
    def __init__(self, controller, robot, kind):
        self.controller, self.robot, self.kind = controller, robot, kind
        self.symbol, self.timeframe, self.preset = controller.chosen[robot.key]
        self.window = tk.Toplevel(controller.ui.root)
        self.window.title("Selecionar ativo e timeframe" if kind == "asset" else "Selecionar preset")
        self.window.geometry("640x540")
        self.window.minsize(420, 360)
        self.window.transient(controller.ui.root)
        self.window.grab_set()
        body = ttk.Frame(self.window, padding=16)
        body.pack(fill=tk.BOTH, expand=True)
        ttk.Label(body, text=robot.name, style="Heading.TLabel").pack(anchor="w")
        ttk.Label(body, text="Buscar ativo" if kind == "asset" else "Buscar preset", style="Muted.TLabel").pack(anchor="w", pady=(10, 2))
        self.search = tk.StringVar()
        entry = ttk.Entry(body, textvariable=self.search)
        entry.pack(fill=tk.X)
        self.count = ttk.Label(body, text="", style="Muted.TLabel")
        self.count.pack(anchor="w", pady=5)
        footer = ttk.Frame(body)
        footer.pack(side=tk.BOTTOM, fill=tk.X, pady=(10, 0))
        self.tf = tk.StringVar(value=self.timeframe)
        if kind == "asset":
            ttk.Label(footer, text="Timeframe", style="Muted.TLabel").pack(side=tk.LEFT)
            ttk.Combobox(footer, textvariable=self.tf, state="readonly", width=6,
                         values=tuple(dict.fromkeys(robot.timeframes + ("M1", "M5", "M15", "H1", "H4", "D1")))).pack(side=tk.LEFT, padx=6)
        ttk.Button(footer, text="Aplicar", style="Accent.TButton", command=self.apply).pack(side=tk.RIGHT)
        table = ttk.Frame(body)
        table.pack(fill=tk.BOTH, expand=True)
        self.tree = ttk.Treeview(table, columns=("choice",), show="headings", height=9, selectmode="browse")
        self.tree.heading("choice", text="Ativo" if kind == "asset" else "Preset")
        self.tree.column("choice", width=280, stretch=True)
        bar = ttk.Scrollbar(table, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=bar.set)
        bar.pack(side=tk.RIGHT, fill=tk.Y)
        self.tree.pack(fill=tk.BOTH, expand=True)
        self.values = (sorted(set(getattr(controller.ui.app, "symbols", []) or []) |
                              {r["symbol"] for r in robot.runs if r.get("symbol")}) if kind == "asset" else
                       [controls.DEFAULT_PRESET] + list(robot.presets))
        self.search.trace_add("write", lambda *_: self.filter())
        self.tree.bind("<Double-Button-1>", lambda e: self.apply())
        self.tree.bind("<Return>", lambda e: self.apply())
        self.window.bind("<Escape>", lambda e: self.window.destroy())
        self.filter()
        entry.focus_set()

    def filter(self):
        query = self.search.get().strip().casefold()
        found = [value for value in self.values if query in value.casefold()]
        shown = found[:500]
        current = self.symbol if self.kind == "asset" else self.preset or controls.DEFAULT_PRESET
        if current in found and current not in shown:
            shown = [current] + shown[:499]
        self.tree.delete(*self.tree.get_children())
        for index, value in enumerate(shown):
            self.tree.insert("", tk.END, iid=str(index), values=(value,))
            if value == current:
                self.tree.selection_set(str(index))
                self.tree.see(str(index))
        self.count.configure(text=f"{len(found)} encontrado(s)" + (" · mostrando 500; refine a busca" if len(found) > 500 else ""))

    def apply(self):
        chosen = self.tree.selection()
        if not chosen:
            return
        active = self.controller.sessions.get(self.robot.key)
        if active and active.active:
            return
        value = self.tree.item(chosen[0], "values")[0]
        selected = ((value, self.tf.get(), self.preset) if self.kind == "asset" else
                    (self.symbol, self.timeframe, None if value == controls.DEFAULT_PRESET else value))
        self.controller.chosen[self.robot.key] = selected
        self.controller.panel.selected_key = self.robot.key
        self.window.destroy()
        self.controller.panel.render()


class ExecutionDialog:
    def __init__(self, controller, robot, mode, account=None, snapshot=None):
        self.controller, self.ui, self.robot, self.mode = controller, controller.ui, robot, mode
        self.symbol, self.timeframe, self.preset = controller.chosen[robot.key]
        self.account, self.snapshot = account, snapshot
        self.context = controls.selection(robot, self.symbol, self.timeframe, self.preset)
        self.window = tk.Toplevel(self.ui.root)
        self.window.title(f"{mode.upper()} · {robot.name}")
        self.window.geometry("760x680")
        self.window.minsize(480, 420)
        self.window.transient(self.ui.root)
        self.window.grab_set()
        self.stage = 1
        self.consent = None
        self.body = ttk.Frame(self.window, padding=16)
        self.body.pack(fill=tk.BOTH, expand=True)
        bottom = ttk.Frame(self.body)
        bottom.pack(side=tk.BOTTOM, fill=tk.X, pady=(12, 0))
        ttk.Button(bottom, text="Cancelar", command=self.window.destroy).pack(side=tk.LEFT)
        self.button = ttk.Button(bottom, text="Continuar · 1/2" if mode == "play" else f"Iniciar {mode.upper()}",
                                 style="Accent.TButton", command=self.confirm)
        self.button.pack(side=tk.RIGHT)
        viewport = ScrollableContent(self.body, self.ui.theme)
        self.viewport = viewport
        viewport.pack(fill=tk.BOTH, expand=True)
        self.content = viewport.content
        self.labels = []
        self._label(f"{mode.upper()} · {robot.name}", "Title.TLabel")
        self._label(f"{self.symbol} · {self.timeframe} · {self.preset or controls.DEFAULT_PRESET}", "Heading.TLabel")
        if mode == "play":
            real = account.get("demo") is False
            self._label("CONTA REAL · DINHEIRO REAL EM RISCO" if real else "CONTA DEMO · operação na conta de demonstração",
                        "Bad.TLabel" if real else "Info.TLabel")
            if real:
                self.labels[-1].configure(background=self.ui.theme.colors["bad"], foreground="#ffffff", padding=10)
            self._label(f"Conta {account['login']} · {account['server']}")
            totals = snapshot["totals"]
            self._label(f"Carteira atual · {totals['positions']} posições · {totals['orders']} ordens pendentes\n"
                        f"Saldo {fmt_num(totals['balance'])} {snapshot['currency']} · patrimônio {fmt_num(totals['equity'])}\n"
                        f"Risco até os stops {fmt_num(totals['risk_pct'])}% · "
                        f"{totals['without_stop']} sem stop · margem livre {fmt_num(totals['margin_free'])}")
            if not totals["risk_complete"]:
                self._label("Risco da carteira incompleto: existem posições sem stop ou sem dados.", "Warn.TLabel")
            self._label(f"Risco configurado · {self.context['risk'].level}\n{self.context['risk'].summary}", "Warn.TLabel")
            budget = controls.risk_budget(robot, self.preset, account)
            lines = []
            for role, label in (("risk_pct", "Por operação"), ("daily_loss_pct", "Limite diário"),
                                ("max_drawdown_pct", "Queda máxima configurada")):
                pct = budget["facts"].get(role)
                if pct is not None:
                    amount = budget["amounts"].get(role)
                    lines.append(f"{label}: {fmt_num(pct)}%" + (f" · {fmt_num(amount)} {budget['currency']} sobre o saldo atual" if amount is not None else ""))
            if lines:
                self._label("\n".join(lines) + "\nSão limites configurados; execução e gaps podem produzir perda maior.", "Warn.TLabel")
            issues = controls.play_issues(self.ui.app.config, robot, self.symbol, self.timeframe, self.preset, account,
                                          self.ui.app.db_manager)
            if issues:
                self._label("PLAY bloqueado\n" + "\n".join(issues), "Bad.TLabel")
                self.button.state(["disabled"])
            run = self.context["run"]
            if run:
                m = run["metrics"]
                self._label(f"Backtest de referência · {run['data_start'][:10]} a {run['data_end'][:10]}\n"
                            f"{fmt_int(m.get('trades'))} operações · PF {fmt_num(m.get('profit_factor'))} · "
                            f"resultado {fmt_num(m.get('net_profit'))} {m.get('currency') or '(moeda do teste não identificada)'} · "
                            f"queda {fmt_num(m['max_drawdown_pct']*100 if m.get('max_drawdown_pct') is not None else None)}%\n"
                            f"Saldo inicial {fmt_num(m.get('initial_equity'))} · método: {m.get('tester_model') or 'não registrado'}")
            self.negative = controls.loss_scenario(run)
            if self.negative:
                self._label(f"HISTÓRICO NEGATIVO · cenário de perda\nSe a média histórica se repetisse, 100 operações "
                            f"representariam perda de {fmt_num(self.negative['loss_100'])} {self.negative['currency']}.\n"
                            + self.negative["basis"], "Bad.TLabel")
            self._label("O Expert opera pela Safety. STOP encerra esta sessão e impede novas ações deste Expert; "
                        "posições/ordens já existentes continuam na conta e exigem acompanhamento.", "Warn.TLabel")
            detail = DetailsPanel(self.content, "Preset e carteira · detalhes", expanded=False,
                                  information=True)
            detail.pack(fill=tk.X, pady=(8, 0))
            details = "\n".join(f"{k} = {v}" for k, v in controls.effective_values(robot, self.preset).items())
            details += "\n\n" + "\n".join(f"{p['owner']} · {p['symbol']} · {p['volume']:g} lote(s) · "
                                          f"resultado {p['profit']:.2f}" for p in snapshot["positions"])
            details += "\n" + "\n".join(f"Ordem pendente · {p['owner']} · {p['symbol']} · {p['volume']:g} lote(s)"
                                         for p in snapshot.get("orders", []))
            label = ttk.Label(detail.body, text=details, style="Muted.TLabel", justify=tk.LEFT, wraplength=600)
            label.pack(fill=tk.X)
            self.labels.append(label)
            digest = controls.context_digest(robot, self.symbol, self.timeframe, self.preset, account, run)
            self.consent = controls.Consent(digest, time.monotonic())
        else:
            self._label("SIMULAÇÃO HISTÓRICA · não envia ordens à conta", "Info.TLabel")
            self._label("Cada tick baseado em ticks reais · agentes locais · " +
                        ("Modo Visual ligado. Pause ou pare no MT5; STOP também está no card." if mode == "replay" else
                         "Modo Visual desligado. Terminal próprio oculto no Linux e minimizado no Windows."))
            run = self.context["run"] or (robot.runs[0] if robot.runs else {})
            today = datetime.now()
            self.dates = []
            for label, value in (("De (AAAA-MM-DD)", str(run.get("data_start") or (today-timedelta(days=90)).date())[:10]),
                                 ("Até (AAAA-MM-DD)", str(run.get("data_end") or today.date())[:10])):
                self._label(label, "Muted.TLabel")
                var = tk.StringVar(value=value)
                ttk.Entry(self.content, textvariable=var).pack(fill=tk.X, pady=(0, 6))
                self.dates.append(var)
            self._label("O primeiro teste pode demorar enquanto o MT5 baixa os ticks da corretora. "
                        "Ao terminar, os resultados e limites estarão no relatório e na base.", "Muted.TLabel")
        self.content.bind("<Configure>", self._wrap, add="+")
        self.window.bind("<Escape>", lambda e: self.window.destroy())
        self.button.focus_set()

    def _label(self, text, style="Surface.TLabel"):
        label = ttk.Label(self.content, text=text, style=style, wraplength=680, justify=tk.LEFT)
        label.pack(anchor="w", fill=tk.X, pady=(0, 9))
        self.labels.append(label)

    def _wrap(self, event):
        for label in self.labels:
            label.configure(wraplength=max(80, event.width - 8))

    def confirm(self):
        start = end = None
        if self.controller.chosen[self.robot.key] != (self.symbol, self.timeframe, self.preset):
            messagebox.showerror("Seleção alterada", "A seleção mudou. Feche e revise novamente.", parent=self.window)
            return
        if self.mode == "play":
            if self.stage == 1:
                self.consent.reviewed = True
                self.stage = 2
                self.button.configure(text="Confirmar PLAY · 2/2")
                confirmation = ttk.Frame(self.body)
                confirmation.pack(side=tk.BOTTOM, fill=tk.X, before=self.viewport, pady=(8, 0))
                prompt = ttk.Label(confirmation, text="CONFIRMAÇÃO 2 DE 2 · digite o número da conta para autorizar este lançamento.",
                                   style="Heading.TLabel", wraplength=680)
                prompt.pack(fill=tk.X)
                prompt.bind("<Configure>", lambda e: prompt.configure(wraplength=max(80, e.width - 8)))
                self.account_entry = ttk.Entry(confirmation)
                self.account_entry.pack(fill=tk.X)
                self.account_entry.focus_set()
                if not self.account.get("demo"):
                    self.real_var = tk.BooleanVar(value=False)
                    ttk.Checkbutton(confirmation, text="Autorizo esta execução com dinheiro REAL", variable=self.real_var).pack(anchor="w")
                return
            if self.account_entry.get().strip() != str(self.account["login"]):
                messagebox.showerror("Confirmação", "O número da conta não confere.", parent=self.window)
                return
            if not self.account.get("demo") and not self.real_var.get():
                messagebox.showerror("Conta REAL", "Marque a autorização específica deste lançamento.", parent=self.window)
                return
            if self.negative and not messagebox.askyesno(
                    "CONFIRMAÇÃO ADICIONAL · HISTÓRICO NEGATIVO",
                    f"Cenário histórico: perda de {fmt_num(self.negative['loss_100'])} {self.negative['currency']} "
                    "em 100 operações, no dimensionamento do teste. Não é previsão.\n\n"
                    "Deseja mesmo iniciar este Expert nesta conta?", parent=self.window, default="no", icon="warning"):
                return
            self.consent.confirmed = True
            self.consent.negative_confirmed = bool(self.negative)
            self.consent.real_authorized = not self.account.get("demo")
        else:
            try:
                start, end = (datetime.strptime(v.get().strip(), "%Y-%m-%d") for v in self.dates)
                if start >= end:
                    raise ValueError()
            except ValueError:
                messagebox.showerror("Período", "Informe datas AAAA-MM-DD, com início anterior ao fim.", parent=self.window)
                return
        self.window.destroy()
        self.controller.start(self.robot, self.mode, self.symbol, self.timeframe, self.preset, start, end,
                              self.account, self.consent)


class ReportDialog:
    def __init__(self, ui, session):
        window = tk.Toplevel(ui.root)
        window.title(f"Relatório · {session.robot.name}")
        window.geometry("700x530")
        window.transient(ui.root)
        viewport = ScrollableContent(window, ui.theme)
        viewport.pack(fill=tk.BOTH, expand=True, padx=16, pady=16)
        body = viewport.content
        state = {"completed": "Concluído", "failed": "Falhou", "cancelled": "Interrompido"}.get(session.state, "Em andamento")
        parts = [f"{session.mode.upper()} · {state}",
                 f"{session.robot.name} · {session.symbol} {session.timeframe} · {session.preset or controls.DEFAULT_PRESET}",
                 f"Tempo total da sessão: {fmt_num((getattr(session, 'finished', None) or time.time()) - session.started, 1)} s"]
        if session.error:
            parts.append(session.error)
        if session.record:
            run, m = session.record, session.record["metrics"]
            parts += [f"Período: {run['data_start']} a {run['data_end']}",
                      f"{fmt_int(m.get('trades'))} operações · acerto {fmt_num(m['win_rate']*100 if m.get('win_rate') is not None else None)}%",
                      f"Resultado {fmt_num(m.get('net_profit'))} {m.get('currency') or '(moeda não identificada)'} · PF {fmt_num(m.get('profit_factor'))}",
                      f"Queda máxima {fmt_num(m['max_drawdown_pct']*100 if m.get('max_drawdown_pct') is not None else None)}% · "
                      f"saldo inicial {fmt_num(m.get('initial_equity'))}",
                      f"Expectativa por operação {fmt_num(m.get('expectancy'))} {m.get('currency') or '(moeda não identificada)'} · "
                      f"{fmt_int(m.get('losses'))} perdas",
                      "Custos registrados: " + (" · ".join(f"{key} {fmt_num(value)}" for key, value in m["costs"].items())
                                                  if m.get("costs") else "consulte o relatório nativo"),
                      "Método: cada tick baseado em ticks reais. Consulte a cobertura efetiva no relatório/diário nativo."]
        elif session.mode != "play":
            parts.append("Não há métricas finais: a passagem não foi concluída. O diário preserva o trabalho executado.")
        parts += ["Histórico não garante resultado futuro. Custos/moeda e posições dependem do relatório nativo.",
                  "STOP não liquida posições nem cancela ordens já existentes.", f"Arquivo: {session.report}"]
        label = ttk.Label(body, text="\n\n".join(parts), style="Surface.TLabel", justify=tk.LEFT, wraplength=640)
        label.pack(fill=tk.X)
        body.bind("<Configure>", lambda e: label.configure(wraplength=max(80, e.width - 8)))
        actions = ttk.Frame(window, padding=10)
        actions.pack(side=tk.BOTTOM, fill=tk.X)
        if session.report.exists():
            ttk.Button(actions, text="Abrir arquivos", command=lambda: ui._open_path(str(session.directory))).pack(side=tk.LEFT)
        if session.record and session.record["metrics"].get("report"):
            ttk.Button(actions, text="Relatório MT5", command=lambda: ui._open_path(session.record["metrics"]["report"])).pack(side=tk.LEFT, padx=(6, 0))
        ttk.Button(actions, text="Fechar", command=window.destroy).pack(side=tk.RIGHT)
