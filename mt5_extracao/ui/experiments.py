"""Consulta de metadados do journal canônico; não executa avaliações."""
import json
import tkinter as tk
from tkinter import ttk

from mt5_extracao.experiments import store

STATES = {"requested": "Registrado", "running": "Em execução", "run_recorded": "Execução gravada",
          "reused": "Resultado reutilizado", "cached": "Resultado já existente", "rejected": "Recusado", "refused": "Recusado",
          "discarded": "Descartado", "failed": "Falhou", "cancelled": "Cancelado"}
ROLES = {"train": "Treino", "validation": "Validação", "holdout": "Reserva"}
PURPOSES = {"evaluation": "Avaliação", "tuning": "Ajuste", "proposal_after_access": "Proposta após acesso"}
EXPOSURE = {"used_for_tuning": "Usada para ajuste", "access_recorded": "Acesso registrado",
            "no_recorded_access": "Sem acesso registrado"}


class ExperimentsDialog(tk.Toplevel):
    def __init__(self, ui):
        super().__init__(ui.root)
        self.ui, self.db = ui, ui.app.db_manager
        self.title("Registro de estudos")
        self.geometry("1000x760")
        self.minsize(640, 520)
        self.transient(ui.root)
        self._generation = 0
        self.protocol_id = None
        self.snapshot = None
        self.busy = False
        self.items = {}
        body = ttk.Frame(self, padding=12)
        body.pack(fill=tk.BOTH, expand=True)
        tools = ttk.Frame(body)
        tools.pack(fill=tk.X)
        ttk.Label(tools, text="Registro de estudos", style="Page.Title.TLabel").pack(side=tk.LEFT)
        self.refresh_button = ttk.Button(tools, text="Atualizar", command=self.reload)
        self.refresh_button.pack(side=tk.RIGHT)
        self.note = ttk.Label(body, text="", justify=tk.LEFT)
        self.note.pack(fill=tk.X, pady=8)
        self.note.bind("<Configure>", lambda e: self.note.configure(wraplength=max(1, e.width - 4)))
        self.protocols = self._tree(body, ("name", "revision", "created"), ("Protocolo", "Revisão", "Registro"), 4)
        self.protocols.bind("<<TreeviewSelect>>", self.select_protocol)
        self.tabs = ttk.Notebook(body)
        self.tabs.pack(fill=tk.BOTH, expand=True, pady=(8, 0))
        self.pages = {}
        for name in ("Pedidos", "Dados e reserva", "Protocolo", "Eventos"):
            page = ttk.Frame(self.tabs, padding=8)
            self.tabs.add(page, text=name)
            self.pages[name] = page
        self.requests = self._tree(self.pages["Pedidos"], ("state", "role", "origin", "purpose"),
                                   ("Estado", "Dados", "Origem", "Finalidade"), 5)
        self.requests.bind("<<TreeviewSelect>>", self.select_request)
        self.request_detail = self._text(self.pages["Pedidos"])
        self.exposure_text = self._text(self.pages["Dados e reserva"])
        self.protocol_text = self._text(self.pages["Protocolo"])
        self.events_text = self._text(self.pages["Eventos"])
        ttk.Button(body, text="Fechar", command=self.destroy).pack(anchor="e", pady=(8, 0))
        self.reload()

    @staticmethod
    def _tree(parent, columns, headings, height):
        frame = ttk.Frame(parent)
        frame.pack(fill=tk.X)
        tree = ttk.Treeview(frame, columns=columns, show="headings", height=height, selectmode="browse")
        vertical = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=vertical.set)
        tree.pack(side=tk.LEFT, fill=tk.X, expand=True)
        vertical.pack(side=tk.RIGHT, fill=tk.Y)
        for name, heading in zip(columns, headings):
            tree.heading(name, text=heading)
            tree.column(name, width=150, minwidth=70, stretch=True)
        return tree

    def _text(self, parent):
        frame = ttk.Frame(parent)
        frame.pack(fill=tk.BOTH, expand=True, pady=(6, 0))
        text = self.ui.theme.classic(tk.Text(frame, height=6, wrap="word", padx=8, pady=8, state="disabled"))
        bar = ttk.Scrollbar(frame, command=text.yview)
        text.configure(yscrollcommand=bar.set)
        text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        bar.pack(side=tk.RIGHT, fill=tk.Y)
        return text

    @staticmethod
    def _put(widget, value):
        widget.configure(state="normal")
        widget.delete("1.0", tk.END)
        widget.insert("1.0", value)
        widget.configure(state="disabled")

    def _job(self, work, done):
        self._generation += 1
        generation = self._generation
        self.busy = True
        def finish(value, error):
            if not self.winfo_exists() or generation != self._generation:
                return
            self.busy = False
            if error is not None:
                self.snapshot = None
                self.note.configure(text=f"Não foi possível consultar o registro: {error}. Clique em Atualizar para tentar novamente.")
                return
            done(value)
        self.ui.run_bg(work, finish)

    def _clear(self):
        self.snapshot = None
        self.items = {}
        self.requests.delete(*self.requests.get_children())
        for widget in (self.request_detail, self.exposure_text, self.protocol_text, self.events_text):
            self._put(widget, "")

    def reload(self):
        selected = self.protocol_id
        self._clear()
        self.note.configure(text="Consultando protocolos registrados...")
        def done(rows):
            self.protocols.delete(*self.protocols.get_children())
            for row in rows:
                self.protocols.insert("", tk.END, iid=row["protocol_id"], values=(row["name"], row["revision"], row["created_at"]))
            if not rows:
                self.protocol_id = None
                self.note.configure(text="Nenhum protocolo registrado. Registre um protocolo pelo serviço ou CLI de experimentos; a consulta não cria avaliações.")
                return
            choice = selected if selected and self.protocols.exists(selected) else rows[0]["protocol_id"]
            self.protocols.selection_set(choice)
            self.protocols.see(choice)
        self._job(lambda: store.list_protocols(self.db), done)

    def select_protocol(self, event=None):
        selected = self.protocols.selection()
        if not selected:
            return
        self.protocol_id = choice = selected[0]
        self._clear()
        self.note.configure(text="Consultando pedidos e exposição dos dados...")
        self._job(lambda: store.status(self.db, choice), self._show)

    def _show(self, snapshot):
        self.snapshot = snapshot
        p = snapshot["protocol"]
        self.note.configure(text="Consulta de metadados. Execução gravada indica persistência do resultado; livro, aprovação estatística e reserva final exigem validação própria.")
        self.items = {item["request_id"]: item for item in snapshot["requests"]}
        for key, item in self.items.items():
            payload = item["payload"]
            self.requests.insert("", tk.END, iid=key, values=(STATES.get(item["state"], item["state"]),
                ROLES.get(payload["role"], payload["role"]), "Manual" if payload["origin"] == "manual" else "Automática",
                PURPOSES.get(payload["purpose"], payload["purpose"])))
        if self.items:
            self.requests.selection_set(next(iter(self.items)))
        else:
            self._put(self.request_detail, "Nenhum pedido registrado neste protocolo.")
        exposure = ["Exposição externa anterior: desconhecida. Ausência de acesso registrado não certifica reserva intocada.",
                    "A contagem inclui intervalos sobrepostos da mesma origem em outros protocolos.",
                    "Reserva final qualificada: não. Aprovação estatística: não. Livro qualificado: não."]
        for role, value in snapshot["exposure"].items():
            exposure.append(f"{ROLES.get(role, role)}: {EXPOSURE.get(value['status'], value['status'])}; {value['recorded_accesses']} acesso(s) registrado(s).")
        self._put(self.exposure_text, "\n\n".join(exposure))
        definition = p["definition"]
        self._put(self.protocol_text, f"{definition['name']} — revisão {definition['revision']}\nID: {p['protocol_id']}\n\n"
            + "Protocolo congelado (critérios declarados, sem qualificação estatística):\n"
            + json.dumps(definition, indent=2, ensure_ascii=False)
            + "\n\nIntervalos efetivos e origem registrados:\n" + json.dumps(p["context"], indent=2, ensure_ascii=False))
        events = snapshot["events"]
        lines = [f"Últimos {min(100, len(events))} de {len(events)} eventos deste protocolo; histórico completo preservado no journal."]
        for item in events[-100:]:
            lines.append(f"{item['event_seq']} · {item['created_at']} · {item['kind']}\nPedido: {item['request_id'] or '—'}\n"
                         + json.dumps(item["payload"], ensure_ascii=False))
        self._put(self.events_text, "\n\n".join(lines))

    def select_request(self, event=None):
        selected = self.requests.selection()
        if not selected or selected[0] not in self.items:
            return
        item = self.items[selected[0]]
        self._put(self.request_detail,
            f"Estado: {STATES.get(item['state'], item['state'])}\nPedido: {item['request_id']}\n"
            f"Alternativa: {item['alternative_id']}\nExecução vinculada: {item['run_id'] or '—'}\n"
            f"Motivo: {item['error'] or 'Sem erro registrado'}\n\nConfiguração efetiva congelada:\n"
            + json.dumps(item["payload"], indent=2, ensure_ascii=False))
