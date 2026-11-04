"""Formulário de pares congelados; cálculo e publicação pertencem ao SDK existente."""
import threading
from datetime import datetime, timezone
import tkinter as tk
from tkinter import ttk

from mt5_extracao import sdk
from mt5_extracao.ui.components import DetailsPanel, ScrollableContent
from mt5_extracao.ui.visual_tokens import SPACING


class RelationCreator:
    def __init__(self, parent, theme, *, db, run_bg, captures, as_of=None, settings=None,
                 synthetic_fixture=False, write_guard=None, write_guard_factory=None, on_saved=None, open_series=None):
        self.db, self.run_bg, self.write_guard = db, run_bg, write_guard
        self.on_saved, self.open_series = on_saved, open_series
        self.write_guard_factory = write_guard_factory
        self.pending = self.loading = self.close_after = False
        self._request = 0
        self._cancel = threading.Event()
        self._contracts, self._wrap_labels = {}, []
        self.saved_receipt = None
        settings = settings or {}
        self._preferred_pairs = [tuple(settings[key]) if settings.get(key) else None for key in ("left", "right")]
        self.window = tk.Toplevel(parent)
        self.window.title("Criar relação entre séries congeladas")
        self.window.transient(parent.winfo_toplevel())
        owner = parent.winfo_toplevel()
        width, height = min(720, owner.winfo_screenwidth()-40), min(680, owner.winfo_screenheight()-80)
        x = max(0, min(owner.winfo_screenwidth()-width, owner.winfo_rootx()+(owner.winfo_width()-width)//2))
        y = max(0, min(owner.winfo_screenheight()-height, owner.winfo_rooty()+(owner.winfo_height()-height)//2))
        self.window.geometry(f"{width}x{height}+{x}+{y}")
        self.window.minsize(560, 440)
        self.window.protocol("WM_DELETE_WINDOW", lambda: self.close(cancel=True))
        self.window.bind("<Escape>", self._escape)
        self.window.bind("<Destroy>", self._destroyed, add="+")
        self.window.bind("<Configure>", self._resize, add="+")
        body = ScrollableContent(self.window, theme)
        body.pack(fill=tk.BOTH, expand=True)
        area = ttk.Frame(body.content, style="Surface.TFrame", padding=SPACING["sm"])
        area.pack(fill=tk.X)
        area.columnconfigure(1, weight=1)
        self.area = area
        ttk.Label(area, text="Escolha as versões da captura", font=theme.font_section).grid(
            row=0, column=0, columnspan=2, sticky="w", pady=SPACING["xs"])
        self.guide = self._label(area,
            "A captura vem da análise congelada em Séries ou do resultado selecionado. O par será salvo nessa mesma base.")
        self.guide.grid(row=1, column=0, columnspan=2, sticky="ew", pady=SPACING["xs"])
        self._captures = {}
        for item in captures:
            ident = item["manifest_id"]
            label = f"{item.get('symbol', 'Captura')} / {item.get('timeframe', '—')} · {ident[:12]}"
            self._captures[label] = ident
        self.capture = tk.StringVar()
        self.capture_box = self._combo(area, 2, "Captura:", self.capture, tuple(self._captures))
        self.capture_box.bind("<<ComboboxSelected>>", lambda _e: self.load_capture())
        self.scope = self._label(area, "Carregue a captura para conferir período, fonte e contratos.")
        self.scope.grid(row=3, column=0, columnspan=2, sticky="ew", pady=SPACING["xs"])
        self.left, self.right = tk.StringVar(), tk.StringVar()
        self.left_box = self._combo(area, 4, "Esquerda:", self.left, ())
        self.right_box = self._combo(area, 5, "Direita:", self.right, ())
        self.method = tk.StringVar(value=settings.get("method", "pearson"))
        self.method_box = self._combo(area, 6, "Método:", self.method, ("pearson", "spearman"))
        self.lag = tk.StringVar(value=str(settings.get("lag_bars", 0)))
        self.lag_entry = self._entry(area, 7, "Defasagem:", self.lag)
        ttk.Label(area, text="A esquerda antecede a direita em N períodos nominais; lacunas não são comprimidas.",
                  style="Muted.TLabel", wraplength=620).grid(row=8, column=0, columnspan=2, sticky="w")
        self.as_of = tk.StringVar(value=as_of or datetime.now(timezone.utc).isoformat())
        self.cut_entry = self._entry(area, 9, "Corte com fuso:", self.as_of)
        self.synthetic = tk.BooleanVar(value=synthetic_fixture)
        self.synthetic_check = ttk.Checkbutton(area, text="Dados de referência sintéticos", variable=self.synthetic)
        self.synthetic_check.grid(row=10, column=0, columnspan=2, sticky="w", pady=SPACING["xs"])
        advanced = DetailsPanel(area, "Amostra e recursos", expanded=False, information=True,
                                theme=theme, padding=SPACING["xs"])
        advanced.grid(row=11, column=0, columnspan=2, sticky="ew")
        advanced.body.columnconfigure(1, weight=1)
        self.options, self.option_entries, self._option_titles = {}, [], {}
        fields = (("minimum_pairs", "Mínimo de pares:", 30), ("minimum_sessions", "Mínimo de sessões:", 10),
                  ("bootstrap_replicates", "Réplicas:", 999), ("seed", "Semente:", 7),
                  ("block_length_sessions", "Sessões por bloco:", ""), ("tests_counted", "Testes na família:", ""),
                  ("timeout_seconds", "Tempo máximo (s):", 120), ("max_memory_mb", "Memória máxima (MiB):", 2048))
        for row, (key, title, default) in enumerate(fields):
            value = settings.get(key, default)
            variable = tk.StringVar(value="" if value is None else str(value))
            self.options[key] = variable
            self._option_titles[key] = title.rstrip(":")
            self.option_entries.append(self._entry(advanced.body, row, title, variable))
        self.compatibility = self._label(area, "")
        self.compatibility.grid(row=12, column=0, columnspan=2, sticky="ew", pady=SPACING["xs"])
        self._label(area, "Resultado exploratório, sem causalidade ou utilidade financeira comprovadas.").grid(
            row=13, column=0, columnspan=2, sticky="ew")
        footer = ttk.Frame(self.window, padding=SPACING["sm"])
        footer.pack(fill=tk.X)
        self.status = self._label(footer, "Carregando contratos — aguarde…")
        self.status.pack(fill=tk.X, pady=(0, SPACING["xs"]))
        self.save_button = ttk.Button(footer, text="Calcular e salvar", style="Accent.TButton", command=self.calculate)
        self.save_button.pack(side=tk.LEFT)
        self.cancel_button = ttk.Button(footer, text="Cancelar cálculo", command=self.cancel)
        self.cancel_button.pack(side=tk.LEFT, padx=SPACING["xs"])
        self.close_button = ttk.Button(footer, text="Fechar", command=lambda: self.close(cancel=True))
        self.close_button.pack(side=tk.RIGHT)
        self.series_button = None
        if open_series is not None:
            self.series_button = ttk.Button(footer, text="Selecionar em Séries", command=self.select_in_series)
            self.series_button.pack(side=tk.RIGHT)
        self._inputs = (self.capture_box, self.left_box, self.right_box, self.method_box, self.lag_entry,
                        self.cut_entry, self.synthetic_check, *self.option_entries)
        for variable in (self.left, self.right, self.lag):
            variable.trace_add("write", self._validate)
        self.cancel_button.state(["disabled"])
        self.save_button.state(["disabled"])
        if self._captures:
            self.capture.set(next(iter(self._captures)))
            self.window.after_idle(self.load_capture)
        else:
            self.status.configure(text="Congele uma seleção em Séries e confluências com as entradas do par, ou selecione uma relação salva.")
        self.window.grab_set()
        self.window.after_idle(self._focus_first)

    def _focus_first(self):
        if self.alive():
            target = self.capture_box if self._captures else self.series_button or self.close_button
            target.focus_set()

    def _escape(self, _event):
        self.close(cancel=True)
        return "break"

    def _label(self, parent, text):
        label = ttk.Label(parent, text=text, style="Muted.TLabel", wraplength=620, justify=tk.LEFT)
        self._wrap_labels.append(label)
        return label

    @staticmethod
    def _entry(parent, row, title, variable):
        ttk.Label(parent, text=title).grid(row=row, column=0, sticky="w", padx=(0, SPACING["xs"]), pady=SPACING["2xs"])
        entry = ttk.Entry(parent, textvariable=variable)
        entry.grid(row=row, column=1, sticky="ew", pady=SPACING["2xs"])
        return entry

    @staticmethod
    def _combo(parent, row, title, variable, values):
        ttk.Label(parent, text=title).grid(row=row, column=0, sticky="w", padx=(0, SPACING["xs"]), pady=SPACING["2xs"])
        combo = ttk.Combobox(parent, textvariable=variable, values=values, state="readonly", width=42)
        combo.grid(row=row, column=1, sticky="ew", pady=SPACING["2xs"])
        return combo

    def _resize(self, event):
        if event.widget is not self.window:
            return
        width = max(320, event.width-56)
        if width != getattr(self, "_wrap_width", None):
            self._wrap_width = width
            for label in self._wrap_labels:
                label.configure(wraplength=width)

    def alive(self):
        try:
            return bool(self.window.winfo_exists())
        except tk.TclError:
            return False

    def load_capture(self):
        if self.pending:
            return
        ident = self._captures.get(self.capture.get())
        if ident is None:
            return
        self._request += 1
        request = self._request
        self.loading = True
        self._contracts = {}
        self.left_box.configure(values=());self.right_box.configure(values=())
        self.scope.configure(text="Conferindo o recorte desta captura — aguarde…")
        self.left.set("");self.right.set("")
        self.save_button.state(["disabled"])
        self.status.configure(text="Carregando os contratos desta captura — aguarde…")
        def work():return sdk.inspect_feature_snapshot(self.db, ident)
        def done(result, error):
            if not self.alive() or request != self._request:
                return
            self.loading = False
            if error or result["status"] != "ok":
                self.scope.configure(text="Não foi possível conferir este recorte; selecione outra captura ou recupere a origem.")
                self.status.configure(text=str(error) if error else result["errors"][0]["message"])
                return
            scope = result["scope"]
            origin, clock = scope["source"], scope["time"]
            offset = clock.get("broker_utc_offset_hours")
            offset_text = "não declarado" if offset is None else f"UTC{float(offset):+g}"
            rows_kind='fatos/revisões de séries; sem barras de mercado' if scope.get('snapshot_schema_version')==5 else 'barras da captura'
            self.scope.configure(text=f"{scope['symbol']} / {scope['timeframe']} · {scope['period']['rows']} {rows_kind}\n"
                f"{scope['period']['first']} a {scope['period']['last']} · relógio da captura {clock.get('consumer_basis') or 'não declarado'}\n"
                f"Fonte: {origin.get('provider') or 'não declarada'} · servidor: {origin.get('server') or 'não declarado'} · offset da captura: {offset_text}")
            for item in result["features"]:
                name = item["definition"].get("name") or item["feature_id"]
                label = f"{name} · {item['feature_id']}@{item['algorithm_version']} [{item['unit'] or 'unidade não declarada'}]"
                self._contracts[label] = item
            labels = tuple(self._contracts)
            self.left_box.configure(values=labels);self.right_box.configure(values=labels)
            compatible = [label for label,item in self._contracts.items() if item["numeric_contract_compatible"]]
            for index,variable in enumerate((self.left,self.right)):
                preferred = next((label for label,item in self._contracts.items() if
                    (item["feature_id"],item["algorithm_version"])==self._preferred_pairs[index]),None)
                variable.set(preferred or (compatible[min(index,len(compatible)-1)] if compatible else ""))
            self.status.configure(text="Escolha as entradas e confira o corte antes de calcular." if labels else "; ".join(result["reasons"]))
            self._validate()
        self.run_bg(work, done)

    def _validate(self, *_args):
        reason = None
        chosen = [self._contracts.get(variable.get()) for variable in (self.left, self.right)]
        if self.loading or not all(chosen):
            reason = "A captura precisa conter as duas versões armazenadas do par."
        elif any(not item["numeric_contract_compatible"] for item in chosen):
            reason = "Escolha saídas numéricas com unidade declarada. Códigos categóricos e booleanos não são medidas contínuas."
        else:
            try:
                lag = int(self.lag.get())
                if not 0 <= lag <= 10000:
                    raise ValueError
                if self.left.get()==self.right.get() and lag==0:
                    reason = "Escolha séries diferentes ou defasagem maior que zero."
            except ValueError:
                reason = "Informe uma defasagem inteira de0 a10000 períodos."
        self.compatibility.configure(text=reason or "Disponibilidade, calendário, amostra e origens serão conferidos no cálculo.")
        if not self.loading and not self.pending and reason != getattr(self, "_validation_reason", None):
            self.status.configure(text=reason or "Escolha as entradas e confira o corte antes de calcular.")
        self._validation_reason = reason
        self.save_button.state(["disabled"] if reason or self.pending else ["!disabled"])
        return reason

    def calculate(self):
        if self.pending or self._validate():
            return
        try:
            options = {}
            for key, variable in self.options.items():
                raw = variable.get().strip()
                optional = key in ("block_length_sessions", "tests_counted")
                if not raw and not optional:
                    raise ValueError(f"Informe {self._option_titles[key].lower()}.")
                try:
                    options[key] = (None if not raw else int(raw)) if key not in ("timeout_seconds", "max_memory_mb") else float(raw)
                except ValueError as exc:
                    kind = "um número" if key in ("timeout_seconds", "max_memory_mb") else "um número inteiro"
                    raise ValueError(f"{self._option_titles[key]} precisa ser {kind}.") from exc
            if options["seed"] is None or options["seed"]<0:
                raise ValueError("Use uma semente inteira não negativa.")
            left, right = (self._contracts[variable.get()] for variable in (self.left, self.right))
            arguments = dict(left=(left["feature_id"],left["algorithm_version"]),right=(right["feature_id"],right["algorithm_version"]),
                as_of=self.as_of.get().strip(),method=self.method.get(),lag_bars=int(self.lag.get()),
                synthetic_fixture=bool(self.synthetic.get()),**options)
            ident = self._captures[self.capture.get()]
        except (ValueError,KeyError) as exc:
            self.status.configure(text="Confira amostra e recursos: "+str(exc))
            return
        self.pending = True
        self._cancel.clear()
        self.close_after = False
        for widget in self._inputs:widget.state(["disabled"])
        self.save_button.state(["disabled"]);self.cancel_button.state(["!disabled"])
        if self.series_button is not None:self.series_button.state(["disabled"])
        self.status.configure(text="Calculando e salvando o par — aguarde…")
        def work():
            guard = self.write_guard_factory() if self.write_guard_factory is not None else self.write_guard
            return sdk.persist_feature_relation(self.db,ident,**arguments,write_guard=guard,cancel_check=self._cancel.is_set)
        def done(receipt,error):
            if not self.alive():return
            self.pending = False
            self.cancel_button.state(["disabled"])
            if self.series_button is not None:self.series_button.state(["!disabled"])
            for widget in self._inputs:widget.state(["!disabled"])
            for combo in (self.capture_box,self.left_box,self.right_box,self.method_box):combo.configure(state="readonly")
            self._validate()
            if error:
                self.status.configure(text=str(error))
                if self.close_after:self.window.destroy()
                return
            self.saved_receipt = receipt
            try:
                if self.on_saved is not None:self.on_saved(receipt)
            except Exception as exc:
                self.status.configure(text=f"Resultado salvo ({receipt['analysis_id']}), mas não foi possível exibi-lo: {exc}")
                return
            self.window.destroy()
        self.run_bg(work,done)

    def cancel(self):
        if self.pending:
            self._cancel.set()
            self.cancel_button.state(["disabled"])
            self.status.configure(text="Cancelando — aguarde a confirmação do processamento.")

    def close(self, cancel=False):
        if self.pending:
            self.close_after = True
            self.cancel()
        else:
            self.window.destroy()

    def select_in_series(self):
        if self.pending:
            return
        self.window.destroy()
        if self.open_series is not None:self.open_series()

    def _destroyed(self, event):
        if event.widget is self.window:
            self._cancel.set()
            self._request += 1
