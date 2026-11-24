"""Abre pelo Laboratório os mesmos recibos e o relatório usados pelo agente."""
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, ttk
import webbrowser

from ..laboratory import ultra_report


def open_receipts(panel):
    db = panel._db()
    if db is None:
        panel.set_note("Abra a base de pesquisa para consultar as carteiras Ultra.")
        return
    window = tk.Toplevel(panel.frame)
    window.title("Carteiras Ultra")
    body = ttk.Frame(window, padding=12)
    body.pack(fill=tk.BOTH, expand=True)
    ttk.Label(body, text="Carteira").pack(anchor="w")
    receipt = ttk.Combobox(body, state="readonly", width=44)
    receipt.pack(fill=tk.X)
    ttk.Label(body, text="Referência para comparação").pack(anchor="w", pady=(8, 0))
    reference = ttk.Combobox(body, state="readonly")
    reference.pack(fill=tk.X)
    note = ttk.Label(body, text="Lendo recibos…", wraplength=340)
    note.pack(anchor="w", pady=8)
    offset = 0

    def load():
        def done(rows, error):
            if not window.winfo_exists():
                return
            values = [r["receipt"] for r in rows or [] if r["status"] == "complete"]
            receipt.configure(values=values)
            receipt.set(values[0] if values else "")
            reference.configure(values=["", *values])
            reference.set("")
            note.configure(text=str(error) if error else f"{len(values)} carteiras concluídas nesta página.")
            previous.configure(state="normal" if offset else "disabled")
            following.configure(state="normal" if len(rows or []) == 50 else "disabled")
            submit.configure(state="normal" if values else "disabled")
        panel.ui.run_bg(lambda: ultra_report.list_receipts(db, offset), done)

    def page(step):
        nonlocal offset
        offset = max(0, offset + step)
        for button in (previous, following, submit):
            button.configure(state="disabled")
        load()

    def show():
        path = filedialog.asksaveasfilename(parent=window, title="Salvar relatório da carteira",
                                            defaultextension=".html", filetypes=[("Relatório HTML", "*.html")])
        if not path:
            return
        selected, other = receipt.get(), reference.get() or None
        submit.configure(state="disabled")
        def done(result, error):
            if not window.winfo_exists():
                return
            submit.configure(state="normal")
            if error:
                note.configure(text=str(error))
            else:
                note.configure(text="Relatório salvo em " + path)
                webbrowser.open(Path(result["file"]).resolve().as_uri())
        panel.ui.run_bg(lambda: ultra_report.export(db, selected, path, reference=other), done)

    nav = ttk.Frame(body)
    nav.pack(fill=tk.X)
    previous = ttk.Button(nav, text="Anterior", state="disabled", command=lambda: page(-50))
    following = ttk.Button(nav, text="Próxima", state="disabled", command=lambda: page(50))
    previous.pack(side=tk.LEFT)
    following.pack(side=tk.LEFT)
    submit = ttk.Button(body, text="Salvar e abrir linha do tempo…", state="disabled", command=show)
    submit.pack(anchor="w", pady=8)
    load()
