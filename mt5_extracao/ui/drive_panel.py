"""
Guia "Google Drive" da aba Recursos: um botão conecta este computador ao Drive pelo navegador, sem arquivos manuais.

A rede e a espera do consentimento rodam numa thread própria (o consentimento pode levar minutos e não deve ocupar a
fila de trabalho da UI); o resultado volta à thread do Tk por ``root.after``. A lógica fica em
``mt5_extracao.resources.cloud_drive``.
"""
import threading
import tkinter as tk
import webbrowser
from tkinter import filedialog, messagebox, simpledialog, ttk

from mt5_extracao.resources import cloud_drive, drive_backup
from mt5_extracao.ui.theme import Tooltip

POLL_MS = 100


class DrivePanel:
    def __init__(self, notebook, ui, link_factory=cloud_drive.DriveLink, db_getter=lambda: None):
        self.ui = ui
        self.db_getter = db_getter
        self.link_factory = link_factory
        self.cancel = None
        self.busy = False
        tab = ttk.Frame(notebook, padding=10, style="Surface.TFrame")
        notebook.add(tab, text="Google Drive")
        self.frame = tab
        ttk.Label(tab, text="Cópia de segurança no Google Drive", style="Surface.TLabel",
                  font=("TkDefaultFont", 11, "bold")).pack(anchor="w")
        ttk.Label(tab, style="Muted.TLabel", wraplength=760, justify=tk.LEFT, text=(
            "Clique em Conectar: o navegador abre na sua conta Google e você autoriza. O App só enxerga a pasta "
            "\"MT5X Backup\" que ele mesmo cria. O acesso fica no cofre de senhas do sistema; nada é gravado em "
            "arquivo. As cópias são cifradas neste computador antes de sair; guarde o código de recuperação "
            "fora do Drive.")).pack(anchor="w", pady=(4, 10))
        self.state_label = ttk.Label(tab, text="Verificando...", style="Surface.TLabel", wraplength=760,
                                     justify=tk.LEFT)
        self.state_label.pack(anchor="w")
        self.detail_label = ttk.Label(tab, text="", style="Muted.TLabel", wraplength=760, justify=tk.LEFT)
        self.detail_label.pack(anchor="w", pady=(4, 10))
        row = ttk.Frame(tab, style="Surface.TFrame")
        row.pack(anchor="w")
        self.connect_button = ttk.Button(row, text="Conectar Google Drive", style="Accent.TButton",
                                         command=self.connect)
        self.connect_button.pack(side=tk.LEFT)
        Tooltip(self.connect_button, "Procura a credencial do Google neste computador e abre o consentimento no "
                                     "navegador.", ui.theme)
        self.cancel_button = ttk.Button(row, text="Cancelar", command=self.cancel_connect)
        self.verify_button = ttk.Button(row, text="Verificar destino", command=self.verify)
        self.verify_button.pack(side=tk.LEFT, padx=(8, 0))
        self.disconnect_button = ttk.Button(row, text="Desconectar", command=self.disconnect)
        self.disconnect_button.pack(side=tk.LEFT, padx=(8, 0))
        self.client_button = ttk.Button(row, text="Escolher credencial...", command=self.choose_client)
        self.client_button.pack(side=tk.LEFT, padx=(8, 0))
        self.console_button = ttk.Button(row, text="Criar credencial no Google", command=self.open_console)
        self.console_button.pack(side=tk.LEFT, padx=(8, 0))
        Tooltip(self.console_button, "Abre o Console do Google. Crie um \"ID do cliente OAuth\" do tipo \"App para "
                                     "computador\" com a Google Drive API ativa e baixe o JSON; o App o encontra "
                                     "sozinho na pasta Downloads.", ui.theme)
        row2 = ttk.Frame(tab, style="Surface.TFrame")
        row2.pack(anchor="w", pady=(8, 0))
        self.send_button = ttk.Button(row2, text="Enviar cópia agora", command=self.send)
        self.send_button.pack(side=tk.LEFT)
        Tooltip(self.send_button, "Copia a base ativa com ela em uso, cifra, envia ao Drive e confere o MD5 que o "
                                  "Google recebeu.", ui.theme)
        self.drill_button = ttk.Button(row2, text="Ensaiar restauração", command=self.drill)
        self.drill_button.pack(side=tk.LEFT, padx=(8, 0))
        Tooltip(self.drill_button, "Baixa a cópia mais nova para uma pasta temporária, decifra e confere a "
                                   "integridade. A base em uso não é tocada.", ui.theme)
        self.code_button = ttk.Button(row2, text="Código de recuperação...", command=self.show_code)
        self.code_button.pack(side=tk.LEFT, padx=(8, 0))
        self.restore_key_button = ttk.Button(row2, text="Já tenho um código...", command=self.restore_key)
        self.restore_key_button.pack(side=tk.LEFT, padx=(8, 0))
        Tooltip(self.restore_key_button, "Num computador novo: digite o código guardado para decifrar as cópias "
                                         "já enviadas.", ui.theme)
        self.refresh()

    # --- execução fora da thread do Tk -----------------------------------------------------------------------------

    def _run(self, work, done):
        self._set_busy(True)
        box = {}

        def target():
            try:
                box["result"] = work(self.link_factory())
            except cloud_drive.DriveError as exc:
                box["error"] = exc
            except Exception:
                box["error"] = cloud_drive.DriveError("unexpected", "Falha inesperada na operação do Drive. Tente novamente.")

        thread = threading.Thread(target=target, name="mt5x-drive", daemon=True)
        thread.start()

        def poll():
            if thread.is_alive():
                self.ui.root.after(POLL_MS, poll)
                return
            self._set_busy(False)
            done(box.get("result"), box.get("error"))

        self.ui.root.after(POLL_MS, poll)

    def _set_busy(self, busy):
        self.busy = busy
        for b in (self.connect_button, self.verify_button, self.disconnect_button, self.client_button,
                  self.send_button, self.drill_button, self.code_button, self.restore_key_button):
            b.state(["disabled"] if busy else ["!disabled"])

    def _show(self, result, error):
        if error is not None:
            self.state_label.config(text=error.message, style="Warn.TLabel")
            self.detail_label.config(text="")
            self.ui.log("Google Drive: " + error.message)
            return
        state = result.get("state")
        self.state_label.config(text=result.get("message") or "", style="Surface.TLabel")
        detail = ""
        if state == "connected":
            detail = f"Conta {result.get('account')} · pasta \"{result.get('folder_name')}\""
            if result.get("verified_at"):
                detail += f" · conferido em {result['verified_at'].replace('T', ' ')[:16]} UTC: "
                detail += "OK" if result.get("ok") else "; ".join(result.get("problems") or [])
            if result.get("free_bytes") is not None:
                from mt5_extracao.resources.storage import human_bytes
                detail += f" · {human_bytes(result['free_bytes'])} livres"
            if result.get("ok") is False:
                self.state_label.config(style="Warn.TLabel")
        elif state == "no_client":
            cands = result.get("candidates") or []
            detail = ("Credencial: " + cands[0]["path"]) if cands else (
                "Use \"Criar credencial no Google\" e baixe o JSON; depois clique em Conectar.")
        elif state == "unavailable":
            self.state_label.config(style="Warn.TLabel")
        if result.get("warning"):
            detail = (detail + "\n" if detail else "") + result["warning"]
        self.detail_label.config(text=detail)
        connected = state == "connected"
        self.connect_button.config(text="Conectar de novo" if connected else "Conectar Google Drive")
        self.verify_button.state(["!disabled"] if connected and not self.busy else ["disabled"])
        self.disconnect_button.state(["!disabled"] if connected and not self.busy else ["disabled"])
        self.send_button.state(["!disabled"] if connected and not self.busy else ["disabled"])
        self.drill_button.state(["!disabled"] if connected and not self.busy else ["disabled"])

    # --- ações -----------------------------------------------------------------------------------------------------

    def refresh(self):
        self._run(lambda link: link.status(), self._show)

    def connect(self):
        if self.busy:
            return
        self.cancel = threading.Event()
        cancel = self.cancel
        self.state_label.config(text="Aguardando a autorização no navegador...", style="Surface.TLabel")
        self.detail_label.config(text="Conclua o consentimento na aba que abriu. Se nada abriu, confira o navegador "
                                      "padrão.")
        self.cancel_button.pack(side=tk.LEFT, padx=(8, 0), after=self.connect_button)

        def done(result, error):
            self.cancel_button.pack_forget()
            self._show(result, error)
            if error is None:
                self.ui.log(f"Google Drive: conectado a {result.get('account')}.")

        self._run(lambda link: link.connect(cancel=cancel), done)

    def cancel_connect(self):
        if self.cancel is not None:
            self.cancel.set()

    def verify(self):
        self._run(lambda link: link.verify(), self._show)

    def disconnect(self):
        if not messagebox.askyesno("Desconectar Google Drive", "Revogar o acesso do MT5X ao seu Google Drive? "
                                   "Os arquivos já enviados continuam no Drive.", parent=self.frame):
            return
        self._run(lambda link: link.disconnect(), lambda r, e: (self._show(r, e), self.refresh() if e is None else None))

    def choose_client(self):
        path = filedialog.askopenfilename(parent=self.frame, title="Credencial OAuth do Google (JSON)",
                                          filetypes=[("JSON", "*.json"), ("Todos", "*")])
        if not path:
            return

        def work(link):
            link.use_client_file(path)
            return link.status()

        self._run(work, self._show)

    def open_console(self):
        webbrowser.open(cloud_drive.CONSOLE_URL)

    # --- cópias ----------------------------------------------------------------------------------------------------

    def _show_line(self, result, error, ok_text):
        if error is not None:
            self._show(None, error)
            return
        self.state_label.config(text=ok_text(result), style="Surface.TLabel" if result.get("ok", True)
                                else "Warn.TLabel")
        self.ui.log("Google Drive: " + ok_text(result))

    def show_code(self):
        def done(state, error):
            if error is not None:
                self._show(None, error)
                return
            if state["confirmed"]:
                messagebox.showinfo("Código de recuperação", "Seu código (já confirmado):\n\n" + state["code"],
                                    parent=self.frame)
                return
            messagebox.showinfo("Guarde este código", "Anote ou imprima este código e guarde FORA do Google Drive. "
                                "Sem ele ninguém decifra as cópias, nem você em outro computador.\n\n" + state["code"],
                                parent=self.frame)
            typed = simpledialog.askstring("Confirmar código", "Digite o código para confirmar que o guardou:",
                                           parent=self.frame, show="*")
            if typed:
                self._run(lambda link: drive_backup.Keys(link.store).confirm(typed) or {"ok": True},
                          lambda r, e: self._show_line(r, e, lambda _: "Código confirmado; envio liberado."))

        self._run(lambda link: drive_backup.Keys(link.store).ensure(), done)

    def restore_key(self):
        typed = simpledialog.askstring("Código de recuperação", "Digite o código guardado:", parent=self.frame, show="*")
        if typed:
            self._run(lambda link: drive_backup.Keys(link.store).restore(typed) or {"ok": True},
                      lambda r, e: self._show_line(r, e, lambda _: "Chave restaurada a partir do código."))

    def send(self):
        db = self.db_getter()
        path = getattr(db, "db_path", None) if getattr(db, "db_type", None) == "sqlite" else None
        if not path:
            messagebox.showwarning("Enviar cópia", "A base em uso não é SQLite; use pg_dump para PostgreSQL.",
                                   parent=self.frame)
            return

        def done(result, error):
            if error is not None and error.code == "key_unconfirmed":
                self._show(None, error)
                self.show_code()
                return
            self._show_line(result, error, drive_backup.receipt_message)

        self.state_label.config(text="Preparando, cifrando e enviando a cópia...", style="Surface.TLabel")
        self._run(lambda link: drive_backup.DriveBackup(link).send(path, scope="ativa"), done)

    def drill(self):
        self.state_label.config(text="Baixando e conferindo a cópia mais nova...", style="Surface.TLabel")
        self._run(lambda link: drive_backup.DriveBackup(link).drill(), lambda r, e: self._show_line(
            r, e, lambda x: ("Ensaio de restauração OK: " if x["ok"] else "Ensaio FALHOU: ") +
            f"{x['name']} ({x['tables']} tabelas, integridade {x['integrity']})."))
