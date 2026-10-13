"""
Diálogo "Configurações": a configuração em uso e as opções que se ajustam pela interface. Hoje, a organização da
janela do MetaTrader 5 ao iniciar um robô (`robots/layout.py`). Cada opção é gravada na hora em `config.ini`, só na
linha da chave (comentários e o resto do arquivo ficam como estão).
"""
import logging
import tkinter as tk
from pathlib import Path
from tkinter import ttk

from mt5_extracao.bootstrap import config_builder
from mt5_extracao.robots import layout, live, mt5_tools
from mt5_extracao.ui.theme import Tooltip, card

log = logging.getLogger(__name__)

WRAP = 640


def ui_icon(dialog):
    """O ícone de "abrir visão MT5", guardado no diálogo (o Tk descarta a imagem sem uma referência viva)."""
    from mt5_extracao.ui import icons
    dialog._view_icon = icons.mt5_view(dialog.ui.theme.colors["on_accent"])
    return dialog._view_icon


class SettingsDialog:
    def __init__(self, ui):
        self.ui = ui
        self.app = ui.app
        self.config = ui.app.config
        self.config_file = Path(getattr(ui.app, "config_path", "config/config.ini")).resolve()
        self.data_dir = None         # pasta de dados do MT5 (lida em segundo plano)
        self.common = None           # Common\Files (onde o painel grava o desligamento pelo botão TELA)
        self.state = None            # layout.status(...) da última leitura
        self.vars = {}               # chave de [MT5] -> BooleanVar
        self.checks = {}             # chave de [MT5] -> Checkbutton
        self.panel_names = {label: name for name, (label, _code) in layout.PANEL_LAYOUTS.items()}

        self.window = tk.Toplevel(ui.root)
        self.window.title("Configurações")
        self.window.transient(ui.root)
        self.window.minsize(640, 640)
        outer = ttk.Frame(self.window, padding=14)
        outer.pack(fill=tk.BOTH, expand=True)

        buttons = ttk.Frame(outer)
        buttons.pack(fill=tk.X, side=tk.BOTTOM, pady=(12, 0))
        ttk.Button(buttons, text="Abrir pasta de configuração",
                   command=lambda: ui._open_path(self.config_file.parent)).pack(side=tk.LEFT)
        ttk.Button(buttons, text="Atalhos...", command=ui.open_shortcuts_dialog).pack(side=tk.LEFT, padx=(8, 0))
        ttk.Button(buttons, text="Diagnóstico do ambiente...", command=ui.show_doctor).pack(side=tk.LEFT, padx=8)
        ttk.Button(buttons, text="Fechar", command=self.window.destroy).pack(side=tk.RIGHT)

        self._build_summary(outer)
        self._build_layout(outer)
        self.refresh()

    # --- o que está em uso ------------------------------------------------------------

    def _build_summary(self, parent):
        body = card(parent, "Em uso")
        body.master.pack(fill=tk.X)
        cfg = self.config
        info = self.app.mt5_connector.account_summary() if self.ui._mt5_connected() else None
        account = (f"conta {info['login']} em {info['server']}" + (" (demonstração)" if info["demo"] else "")
                   if info else "sem conexão")
        grid = ttk.Frame(body, style="Surface.TFrame")
        grid.pack(fill=tk.X)
        grid.columnconfigure(1, weight=1)
        rows = (("Conta", account),
                ("MetaTrader 5", cfg.get("MT5", "path", fallback="não configurado")),
                ("Banco de dados", f"{cfg.get('DATABASE', 'type', fallback='sqlite')} - "
                                   f"{cfg.get('DATABASE', 'path', fallback='database/mt5_data.db')}"),
                ("Arquivo de configuração", str(self.config_file)))
        for row, (name, value) in enumerate(rows):
            ttk.Label(grid, text=name, style="Muted.TLabel").grid(row=row, column=0, sticky="nw", padx=(0, 14), pady=1)
            ttk.Label(grid, text=value, style="Surface.TLabel", wraplength=WRAP - 170, justify=tk.LEFT).grid(
                row=row, column=1, sticky="w", pady=1)
        ttk.Label(body, style="Muted.TLabel", wraplength=WRAP, justify=tk.LEFT,
                  text="Para trocar de conta (ex.: corretora com ativos da B3), preencha MT5_LOGIN, MT5_PASSWORD e "
                       "MT5_SERVER no arquivo .env da pasta do projeto: o aplicativo entra nessa conta sozinho. As "
                       "opções que não estão nesta janela ficam no arquivo de configuração (reinicie o aplicativo "
                       "depois de editar).").pack(anchor="w", pady=(8, 0))

    # --- janela do MT5 ----------------------------------------------------------------

    def _build_layout(self, parent):
        body = card(parent, "Janela do MetaTrader 5", "como o programa arruma o MT5 ao abri-lo")
        body.master.pack(fill=tk.BOTH, expand=True, pady=(10, 0))

        head = ttk.Frame(body, style="Surface.TFrame")
        head.pack(fill=tk.X)
        self.status_label = ttk.Label(head, text="Lendo...", style="Muted.TLabel")
        self.status_label.pack(side=tk.LEFT)
        self.panel_button = ttk.Button(head, text="Voltar a ligar", command=self.turn_panel_on)
        Tooltip(self.panel_button, "O botão TELA do painel do HADOUKEN desligou a organização. Isto desfaz esse "
                                   "desligamento (o botão do painel passa a mostrar ligado).", self.ui.theme)

        self.vars[layout.MASTER_KEY] = tk.BooleanVar(value=True)
        master = ttk.Checkbutton(body, text="Organizar a janela sempre que o programa abrir o MT5", style="Surface.TCheckbutton",
                                 variable=self.vars[layout.MASTER_KEY],
                                 command=lambda: self.save(layout.MASTER_KEY))
        master.pack(anchor="w", pady=(8, 2))
        self.checks[layout.MASTER_KEY] = master
        Tooltip(master, "Deixa o MetaTrader 5 só com o gráfico do robô. Desligado, nada é mexido e o arranjo que "
                        "você tinha volta na próxima abertura.", self.ui.theme)
        parts = ttk.Frame(body, style="Surface.TFrame")
        parts.pack(fill=tk.X, padx=(22, 0))
        for key, _attr, label, hint in layout.OPTION_KEYS:
            self.vars[key] = tk.BooleanVar(value=True)
            check = ttk.Checkbutton(parts, text=label, style="Surface.TCheckbutton", variable=self.vars[key],
                                    command=lambda k=key: self.save(k))
            check.pack(anchor="w", pady=1)
            Tooltip(check, hint, self.ui.theme)
            self.checks[key] = check

        panel_row = ttk.Frame(body, style="Surface.TFrame")
        panel_row.pack(fill=tk.X, pady=(10, 0))
        ttk.Label(panel_row, text="Disposição do painel do HADOUKEN", style="Surface.TLabel").pack(side=tk.LEFT)
        self.panel_var = tk.StringVar()
        self.panel_combo = ttk.Combobox(panel_row, textvariable=self.panel_var, state="readonly", width=38,
                                        values=[label for label, _code in layout.PANEL_LAYOUTS.values()])
        self.panel_combo.pack(side=tk.LEFT, padx=(10, 0))
        self.panel_combo.bind("<<ComboboxSelected>>", lambda e: self.save_panel())
        Tooltip(self.panel_combo, "Tamanho e arrumação do painel dentro do gráfico. Automática escolhe pelo tamanho "
                                  "do gráfico. Vale para o painel que o programa inicia e, em \"Aplicar e abrir o "
                                  "MT5 agora\", para o painel que já está em um gráfico do perfil próprio.",
                self.ui.theme)

        self.block_var = tk.BooleanVar(value=False)
        self.block_check = ttk.Checkbutton(
            body, text="Manter o painel bloqueado mesmo em conta de demonstração (PARAR TUDO)",
            style="Surface.TCheckbutton", variable=self.block_var, command=self.save_block)
        self.block_check.pack(anchor="w", pady=(8, 0))
        Tooltip(self.block_check, "Desmarcado (padrão): em conta de demonstração o painel de operação manual abre "
                                  "liberado para comprar e vender. Marcado: ele abre só para visualização, com o "
                                  "PARAR TUDO acionado. Conta real nunca entra por aqui, e um robô que opera sozinho "
                                  "posto como painel abre sempre bloqueado.", self.ui.theme)
        self.stop_row = ttk.Frame(body, style="Surface.TFrame")
        self.stop_label = ttk.Label(self.stop_row, style="Bad.TLabel", wraplength=WRAP - 230, justify=tk.LEFT,
                                    text="PARAR TUDO acionado: nenhum robô do HADOUKEN opera e o painel aparece "
                                         "bloqueado.")
        self.stop_label.pack(side=tk.LEFT)
        self.release_button = ttk.Button(self.stop_row, text="Liberar o PARAR TUDO", command=self.release_stop)
        self.release_button.pack(side=tk.RIGHT)
        Tooltip(self.release_button, "Remove a parada de emergência. Os robôs do HADOUKEN que estiverem em gráficos "
                                     "voltam a poder operar; o painel deixa de aparecer bloqueado.", self.ui.theme)

        apply_row = ttk.Frame(body, style="Surface.TFrame")
        self.apply_row = apply_row
        apply_row.pack(fill=tk.X, pady=(12, 0))
        self.apply_button = ttk.Button(apply_row, text="Aplicar e abrir o MT5 agora", style="Accent.TButton",
                                       image=ui_icon(self), compound=tk.LEFT, command=self.apply_now)
        self.apply_button.pack(side=tk.LEFT)
        ttk.Label(apply_row, style="Muted.TLabel", wraplength=WRAP - 250, justify=tk.LEFT,
                  text="As opções valem sempre que o programa abrir o MetaTrader 5: ao iniciar um robô ou por este "
                       "botão (o mesmo \"Abrir visão MT5\" da janela principal). Com o MT5 já aberto, ele pergunta "
                       "antes de fechar e abrir de novo.").pack(side=tk.LEFT, padx=(12, 0))

        ttk.Separator(body).pack(fill=tk.X, pady=10)
        bottom = ttk.Frame(body, style="Surface.TFrame")
        bottom.pack(fill=tk.X)
        self.restore_button = ttk.Button(bottom, text="Devolver o arranjo anterior agora", command=self.restore)
        self.restore_button.pack(side=tk.RIGHT, padx=(10, 0))
        Tooltip(self.restore_button, "Traz de volta as janelas e barras como estavam antes da primeira organização. "
                                     "Só com o MetaTrader 5 fechado: aberto, ele regravaria o arranjo ao fechar.",
                self.ui.theme)
        self.arrangement_label = ttk.Label(bottom, text="", style="Muted.TLabel", wraplength=WRAP - 260,
                                           justify=tk.LEFT)
        self.arrangement_label.pack(side=tk.LEFT, anchor="w")
        self.feedback = ttk.Label(body, text="", style="Muted.TLabel", wraplength=WRAP, justify=tk.LEFT)
        self.feedback.pack(anchor="w", pady=(8, 0))

    def refresh(self):
        """Lê (em segundo plano) a pasta do MT5 e o estado da organização, e redesenha o bloco."""
        config = self.config

        def work():
            try:
                data = mt5_tools.data_dir(config)
            except Exception as e:           # sem a pasta de dados ainda dá para ajustar as opções
                log.debug(f"Pasta de dados do MT5 não encontrada: {e}")
                data = None
            common = mt5_tools.common_files_dir(config)
            state = layout.status(config, data, common)
            try:
                state["stopped"] = live.stop_active(config)
            except Exception as e:
                log.debug(f"Estado do PARAR TUDO não lido: {e}")
                state["stopped"] = False
            return data, common, state
        self.ui.run_bg(work, self._loaded)

    def _loaded(self, result, error):
        if not self.window.winfo_exists():
            return
        if error:
            self.status_label.config(text=f"Não foi possível ler o estado da janela do MT5: {error}",
                                     style="Bad.TLabel")
            return
        self.data_dir, self.common, self.state = result
        self._show()

    def _show(self):
        state = self.state
        for key, var in self.vars.items():
            var.set(state["enabled"] if key == layout.MASTER_KEY else state["options"][key])
        self.panel_var.set(layout.PANEL_LAYOUTS[state["panel_layout"]][0])
        self.block_var.set(state["panel_blocked"])
        self.stop_row.pack_forget()
        if state.get("stopped"):
            self.stop_row.pack(fill=tk.X, pady=(8, 0), before=self.apply_row)
        for key, check in self.checks.items():
            if key != layout.MASTER_KEY:
                check.state(["!disabled"] if state["enabled"] else ["disabled"])
        self.panel_button.pack_forget()
        if state["panel_off"]:
            self.status_label.config(text="Desligada pelo botão TELA do painel do HADOUKEN.", style="Bad.TLabel")
            self.panel_button.pack(side=tk.RIGHT)
        elif not state["enabled"]:
            self.status_label.config(text="Desligada: o programa não mexe na janela do MetaTrader 5.",
                                     style="Muted.TLabel")
        else:
            parts = sum(state["options"].values())
            self.status_label.config(
                text="Ligada." if parts else "Ligada, mas sem nenhuma parte marcada: nada será arrumado.",
                style="Good.TLabel" if parts else "Bad.TLabel")
        if self.data_dir is None:
            self.arrangement_label.config(text="Pasta de dados do MetaTrader 5 não encontrada: confira [MT5] path "
                                               "(Diagnóstico do ambiente mostra o que falta).")
        elif state["has_arrangement"]:
            hidden = ", ".join(state["hidden_now"]) or "nada"
            self.arrangement_label.config(text=f"Há um arranjo anterior guardado. Escondido agora: {hidden}.")
        else:
            self.arrangement_label.config(text="Nenhum arranjo guardado: a janela do MT5 está como você deixou.")
        self.restore_button.state(["!disabled"] if state["has_arrangement"] else ["disabled"])

    def save(self, key):
        """Grava a opção marcada ou desmarcada em config.ini (só a linha dela) e na configuração em memória."""
        value = "true" if self.vars[key].get() else "false"
        if not self._write(key, value):
            self.vars[key].set(not self.vars[key].get())
            return
        self.state = layout.status(self.config, None, self.common) | {
            "has_arrangement": self.state["has_arrangement"], "hidden_now": self.state["hidden_now"],
            "has_profile": self.state["has_profile"], "stopped": self.state.get("stopped", False)}
        self._show()
        self.feedback.config(style="Good.TLabel",
                             text=f"Salvo em {self.config_file.name}. Vale na próxima vez que o programa abrir o "
                                  "MetaTrader 5; para valer agora, clique em \"Aplicar e abrir o MT5 agora\".")

    def _write(self, key, value) -> bool:
        """Grava [MT5] chave = valor no arquivo e na configuração em memória. False (com a mensagem na tela) se falhar."""
        try:
            config_builder.set_config_value(self.config_file, "MT5", key, value)
        except (OSError, ValueError) as e:
            log.error(f"Erro ao gravar [MT5] {key} em {self.config_file}: {e}")
            self.feedback.config(style="Bad.TLabel",
                                 text=f"Não foi possível gravar em {self.config_file.name}: {e}. Confira se o arquivo "
                                      "não está aberto em outro programa ou protegido contra gravação.")
            return False
        if not self.config.has_section("MT5"):
            self.config.add_section("MT5")
        self.config.set("MT5", key, value)
        self.ui.log(f"Configuração gravada: [MT5] {key} = {value}")
        return True

    def save_panel(self):
        """Grava a disposição do painel escolhida na lista."""
        name = self.panel_names[self.panel_var.get()]
        if self._write(layout.PANEL_KEY, name):
            self.state["panel_layout"] = name
            self.feedback.config(style="Good.TLabel",
                                 text=f"Salvo em {self.config_file.name}. Para valer agora, clique em \"Aplicar e "
                                      "abrir o MT5 agora\".")
        else:
            self.panel_var.set(layout.PANEL_LAYOUTS[self.state["panel_layout"]][0])

    def save_block(self):
        """Grava se o painel manual abre bloqueado mesmo em conta de demonstração."""
        blocked = self.block_var.get()
        if not self._write(layout.PANEL_BLOCK_KEY, "true" if blocked else "false"):
            self.block_var.set(not blocked)
            return
        self.state["panel_blocked"] = blocked
        self.feedback.config(
            style="Good.TLabel",
            text=f"Salvo em {self.config_file.name}. " + (
                "O painel abre só para visualização na próxima vez que o programa abrir o MetaTrader 5."
                if blocked else
                "Em conta de demonstração o painel abre liberado para operar na próxima vez que o programa abrir o "
                "MetaTrader 5." + (" O PARAR TUDO continua acionado: libere no botão acima." if self.state.get("stopped")
                                   else "")))

    def release_stop(self):
        """Remove a parada de emergência, por decisão de quem clicou."""
        try:
            live.release_stop(self.config)
        except OSError as e:
            self.feedback.config(style="Bad.TLabel", text=f"Não foi possível liberar o PARAR TUDO: {e}")
            return
        self.ui.log("PARAR TUDO liberado pelo diálogo de Configurações.")
        self.feedback.config(style="Good.TLabel",
                             text="PARAR TUDO liberado. Um painel que já está no gráfico deixa de aparecer bloqueado "
                                  "em instantes.")
        self.refresh()

    def apply_now(self):
        """O mesmo que "Abrir visão MT5" da janela principal, com o resultado mostrado aqui."""
        self.apply_button.state(["disabled"])
        self.feedback.config(style="Muted.TLabel", text="Abrindo o MetaTrader 5 com a visão escolhida...")

        def done(result):
            if not self.window.winfo_exists():
                return
            self.apply_button.state(["!disabled"])
            if result is None:
                self.feedback.config(style="Muted.TLabel", text="Nada foi alterado no MetaTrader 5.")
            else:
                from mt5_extracao.robots import view
                self.feedback.config(style="Good.TLabel", text=view.describe(result))
            self.refresh()
        self.ui.open_mt5_view(parent=self.window, on_done=done)

    def turn_panel_on(self):
        try:
            layout.set_panel_off(self.common, False)
        except OSError as e:
            self.feedback.config(style="Bad.TLabel", text=f"Não foi possível religar: {e}")
            return
        self.feedback.config(style="Good.TLabel", text="Organização religada (o botão TELA do painel volta a "
                                                       "mostrar ligado).")
        self.refresh()

    def restore(self):
        """Devolve o arranjo anterior da janela do MT5, se o terminal estiver fechado."""
        config, data = self.config, self.data_dir
        if data is None:
            return
        self.restore_button.state(["disabled"])
        self.feedback.config(style="Muted.TLabel", text="Conferindo se o MetaTrader 5 está fechado...")

        def work():
            if mt5_tools.terminal_running(config):
                return None
            return layout.restore(data)

        def done(restored, error):
            if not self.window.winfo_exists():
                return
            if error:
                self.feedback.config(style="Bad.TLabel", text=f"Não foi possível devolver o arranjo: {error}")
            elif restored is None:
                self.feedback.config(style="Bad.TLabel",
                                     text="O MetaTrader 5 está aberto: ele regravaria o arranjo ao fechar. Feche o "
                                          "MetaTrader 5 e clique de novo.")
            else:
                text = "Arranjo anterior devolvido." if restored else "Não havia arranjo guardado."
                if restored and self.state and self.state["active"]:
                    text += " A organização continua ligada: o próximo início de um robô arruma a janela de novo."
                self.feedback.config(style="Good.TLabel", text=text)
                self.ui.log(text)
            self.refresh()
        self.ui.run_bg(work, done)
