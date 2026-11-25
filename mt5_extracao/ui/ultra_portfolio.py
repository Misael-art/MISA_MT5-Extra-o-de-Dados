"""Abre pelo Laboratório os mesmos recibos e o relatório usados pelo agente."""
import json
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, ttk
import webbrowser

from ..laboratory import ultra_catalog, ultra_market, ultra_report, ultra_tester
from ..laboratory.ultra_cycle_report import write_report


def show_catalog(anchor):
    window = tk.Toplevel(anchor)
    window.title("Catálogo operacional do Ultra")
    body = ttk.Frame(window, padding=12)
    body.pack(fill=tk.BOTH, expand=True)
    catalog = ultra_catalog.catalog()
    table = ttk.Frame(body)
    table.pack(fill=tk.BOTH, expand=True)
    rows = ttk.Treeview(table, columns=("tester",), show="tree headings", height=10, selectmode="browse")
    rows.column("#0", width=365, minwidth=170)
    rows.column("tester", width=165, minwidth=135, stretch=False)
    rows.heading("#0", text="Estratégia")
    rows.heading("tester", text="Expert concorrente")
    scroll = ttk.Scrollbar(table, orient="vertical", command=rows.yview)
    rows.configure(yscrollcommand=scroll.set)
    detail = ttk.Label(body, wraplength=520, justify=tk.LEFT)
    by_key = {row["key"]: row for row in catalog["strategies"]}
    account_modes = {"hedging": "hedging", "netting_exclusive_cycle": "netting com ciclo exclusivo"}
    for key, row in by_key.items():
        state = "Recorte M1 sintético" if row["ultra_scope"] == "bounded_m1_synthetic" else "Pendente"
        rows.insert("", tk.END, iid=key, text=row["name"], values=(state,))

    def select(_event=None):
        selected = rows.selection()
        if not selected:
            return
        row = by_key[selected[0]]
        modes = ", ".join(account_modes[mode] for mode in row["supported_account_modes"])
        lines = [row["indicators"], row["warmup"], row["reference_rules"],
                 f"Modos de conta do recorte no Tester · {modes}" if modes else None,
                 row["evidence"], row["market_technical_control"], row["eurusd_data_constraint"]]
        detail.configure(text="\n\n".join(line for line in lines if line))

    rows.bind("<<TreeviewSelect>>", select)
    rows.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
    scroll.pack(side=tk.RIGHT, fill=tk.Y)
    detail.pack(fill=tk.X, pady=(8, 0))
    if by_key:
        rows.selection_set(next(iter(by_key)))
        select()


def capture_market(panel, anchor, note):
    dialog = tk.Toplevel(anchor)
    dialog.title("Congelar cotações da DEMO")
    form = ttk.Frame(dialog, padding=12)
    form.pack(fill=tk.BOTH, expand=True)
    form.columnconfigure(1, weight=1)
    entries = []
    for index, (label, default) in enumerate((
            ("Símbolo exato", "EURUSD"), ("Servidor DEMO", "MetaQuotes-Demo"),
            ("Início no relógio do MT5", ""), ("Fim exclusivo no relógio do MT5", ""))):
        ttk.Label(form, text=label).grid(row=index, column=0, sticky="w", padx=(0, 6), pady=3)
        value = ttk.Entry(form, width=29)
        value.insert(0, default)
        value.grid(row=index, column=1, sticky="ew", pady=3)
        entries.append(value)
    ttk.Label(form, text="Abra a DEMO no terminal configurado. Use AAAA-MM-DDTHH:MM:SS.",
              wraplength=360).grid(row=4, column=0, columnspan=2, sticky="w", pady=8)

    def submit_capture():
        path = filedialog.asksaveasfilename(parent=dialog, title="Nome da nova pasta da fita")
        if not path:
            return
        symbol, server, start, end = [value.get().strip() for value in entries]
        config, config_path = panel._config(), panel.ui.app.config_path
        draw.configure(state="disabled")
        def finished(result, error):
            if not dialog.winfo_exists():
                return
            draw.configure(state="normal")
            if error:
                help_text.configure(text=str(error))
                return
            if anchor.winfo_exists():
                note.configure(text=f'{symbol} · {result["coverage"]["rows"]} ticks em {path}')
            dialog.destroy()
        panel.ui.run_bg(lambda: ultra_market.capture_configured(
            config, config_path, symbol, start, end, path, expected_server=server), finished)

    draw = ttk.Button(form, text="Congelar fita…", command=submit_capture)
    draw.grid(row=5, column=0, columnspan=2, sticky="w")
    help_text = ttk.Label(form, wraplength=360)
    help_text.grid(row=6, column=0, columnspan=2, sticky="w", pady=(6, 0))


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

    def show_capabilities():
        package = filedialog.askopenfilename(parent=window, title="Escolher pacote congelado", filetypes=[("JSON", "*.json")])
        if not package:
            return
        def inspect():
            return ultra_tester.package_capabilities(db, json.loads(Path(package).read_text(encoding="utf-8-sig")))
        def finished(result, error):
            if not window.winfo_exists():
                return
            capabilities.configure(state="normal")
            if error:
                note.configure(text=str(error))
            elif result["status"] == "capability_absent":
                note.configure(text=result["symbol"] + " · " + result["reason"])
            else:
                signals = ", ".join(f"{owner} ({signal})" for owner, signal in result["owner_signals"].items())
                if not signals:
                    note.configure(text=f'{result["symbol"]} · Carteira em caixa · Nenhuma estratégia selecionada.')
                    return
                note.configure(text=f'{result["symbol"]} · M1 UTC · {signals}'
                               + " · pendente com stop e alvo · cancelamento e breakeven."
                               + " Netting exige ciclo exclusivo. Aceite exige journal do agente."
                               )
        capabilities.configure(state="disabled")
        panel.ui.run_bg(inspect, finished)

    def show_platform():
        package = filedialog.askopenfilename(parent=window, title="Escolher pacote congelado", filetypes=[("JSON", "*.json")])
        if not package:
            return
        journal = filedialog.askopenfilename(parent=window, title="Escolher journal do agente do Tester")
        if not journal:
            return
        path = filedialog.asksaveasfilename(parent=window, title="Salvar trilha por ciclo em arquivo novo",
                                            defaultextension=".html")
        if not path:
            return
        def compare():
            frozen = json.loads(Path(package).read_text(encoding="utf-8-sig"))
            raw = Path(journal).read_bytes()
            result = ultra_tester.compare(db, frozen, raw, origin=ultra_tester.physical_origin(raw))
            return write_report(result, path)
        def finished(file, error):
            if window.winfo_exists():
                platform.configure(state="normal")
                note.configure(text=str(error) if error else "Trilha salva em " + file)
                if not error:
                    webbrowser.open(Path(file).as_uri())
        platform.configure(state="disabled")
        panel.ui.run_bg(compare, finished)

    def show_market():
        folder = filedialog.askdirectory(parent=window, title="Escolher fita congelada de mercado")
        if not folder:
            return
        def finished(result, error):
            if not window.winfo_exists():
                return
            market.configure(state="normal")
            if error:
                note.configure(text=str(error))
                return
            quotes = result["coverage"]["executable"]
            source = result["source"]
            note.configure(text=f'{result["symbol"]} · {source["server"]} · {quotes} cotações conferidas. '
                                "Relógio, sessões e custos aguardam evidência histórica.")
        market.configure(state="disabled")
        panel.ui.run_bg(lambda: ultra_market.verify(folder), finished)

    def run_market_probe():
        from ..robots.ultra_market_runner import run

        snapshot = filedialog.askdirectory(parent=window, title="Escolher fita congelada de mercado")
        if not snapshot:
            return
        output = filedialog.askdirectory(parent=window, title="Salvar a execução do Tester em")
        if not output:
            return
        config = panel._config()
        def finished(result, error):
            if not window.winfo_exists():
                return
            probe.configure(state="normal")
            note.configure(text=str(error) if error else f'{result["status"]} · {result["receipt_path"]}')
        probe.configure(state="disabled")
        panel.ui.run_bg(lambda: run(config, snapshot, output), finished)

    nav = ttk.Frame(body)
    nav.pack(fill=tk.X)
    previous = ttk.Button(nav, text="Anterior", state="disabled", command=lambda: page(-50))
    following = ttk.Button(nav, text="Próxima", state="disabled", command=lambda: page(50))
    previous.pack(side=tk.LEFT)
    following.pack(side=tk.LEFT)
    submit = ttk.Button(body, text="Salvar e abrir linha do tempo…", state="disabled", command=show)
    submit.pack(anchor="w", pady=8)
    ttk.Button(body, text="Catálogo operacional…", command=lambda: show_catalog(window)).pack(anchor="w", pady=4)
    ttk.Button(body, text="Coletar ticks da DEMO…", command=lambda: capture_market(panel, window, note)).pack(anchor="w", pady=4)
    market = ttk.Button(body, text="Verificar fita de mercado…", command=show_market)
    market.pack(anchor="w", pady=4)
    probe = ttk.Button(body, text="Rodar sonda de ticks no Tester…", command=run_market_probe)
    probe.pack(anchor="w", pady=4)
    capabilities = ttk.Button(body, text="Capacidades do pacote…", command=show_capabilities)
    capabilities.pack(anchor="w", pady=4)
    platform = ttk.Button(body, text="Conferir pacote e journal do Tester…", command=show_platform)
    platform.pack(anchor="w", pady=4)
    load()
