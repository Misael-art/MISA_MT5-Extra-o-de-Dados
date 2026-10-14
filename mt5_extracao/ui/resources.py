"""
Aba "Recursos" (Fase 15): indicadores de espaço, maiores tabelas, higienização com arquivo reversível, arquivos
restauráveis, automação opt-in e histórico. Toda leitura e toda ação rodam em segundo plano (ui.run_bg); a aba só
lê a base na primeira vez que é aberta e quando o usuário pede.

Os números e as regras vêm de mt5_extracao/resources (sem Tkinter).
"""
import configparser
import logging
import tkinter as tk
from datetime import datetime, timedelta
from pathlib import Path
from tkinter import messagebox, simpledialog, ttk

from mt5_extracao.bootstrap import config_builder
from mt5_extracao.resources import actions, automation, files, hygiene, storage
from mt5_extracao.resources import policy as policy_mod
from mt5_extracao.ui.theme import Tooltip, card, fmt_int, scrolled_tree

log = logging.getLogger(__name__)

hb = storage.human_bytes
TICK_MS = 60_000            # o agendador confere a cada minuto se um ciclo está vencido
CHECK_ON, CHECK_OFF = "☑", "☐"

SPACE_COLUMNS = ("Tabela", "Tipo", "Ativo", "Timeframe", "Linhas", "Tamanho", "% da base", "Última barra")
SPACE_WIDTHS = (190, 70, 90, 90, 90, 90, 80, 110)
SPACE_ANCHORS = ("w", "w", "w", "w", "e", "e", "e", "w")
CLEAN_COLUMNS = ("✓", "Ativo", "Timeframe", "Tamanho", "Última barra", "Motivos")
CLEAN_WIDTHS = (34, 90, 90, 90, 110, 420)
CLEAN_ANCHORS = ("center", "w", "w", "e", "w", "w")
ARCHIVE_COLUMNS = ("Tabela", "Ativo", "Linhas", "Tamanho", "Arquivada em", "Motivo")
ARCHIVE_WIDTHS = (190, 90, 90, 90, 140, 320)
ARCHIVE_ANCHORS = ("w", "w", "e", "e", "w", "w")
ACTION_COLUMNS = ("Quando", "Ação", "Alvo", "Origem", "Resultado", "Espaço")
ACTION_WIDTHS = (140, 90, 190, 70, 90, 160)

# Opções da automação: (chave, rótulo, dica)
AUTO_FLAGS = (
    ("auto_vacuum", "Otimizar a base sozinho", "Roda o VACUUM quando o espaço livre dentro da base passar do limite "
                                               "abaixo. Precisa de disco livre do tamanho da base."),
    ("auto_clean_orphans", "Limpar registros órfãos sozinho", "Apaga registros de controle de tabelas que não "
                                                              "existem mais. Não toca em dados de ativos."),
    ("auto_archive", "Arquivar sozinho as séries das regras marcadas", "Arquivar é reversível: a série vai para um "
                     "arquivo e volta com Restaurar. Favoritos, seleção, benchmarks e ativos com benchmark positivo "
                     "nunca são arquivados."),
    ("auto_backup", "Fazer cópia de segurança da base sozinho", "Cópia conferida do arquivo SQLite no intervalo "
                                                               "abaixo, guardando as mais novas. Precisa de disco do "
                                                               "tamanho da base."),
    ("auto_trim", "Aparar sozinho o histórico antigo (retenção)", "Arquiva e retira as linhas mais velhas que os dias de "
                                                                 "retenção abaixo (0 = guarda tudo). Reversível pela "
                                                                 "guia Arquivo."),
    ("auto_clean_files", "Apagar logs e exportações antigos sozinho", "Usa os dias abaixo (0 = nunca). O log em uso "
                                                                       "nunca é apagado."),
)
AUTO_NUMBERS = (
    ("monitor_interval_minutes", "Intervalo entre ciclos (min)", 5, 1440),
    ("alert_db_gb", "Avisar quando a base passar de (GB)", 0, 100000),
    ("alert_disk_free_pct", "Avisar quando o disco livre cair abaixo de (%)", 0, 99),
    ("vacuum_min_mb", "Otimizar quando houver recuperável acima de (MB)", 1, 1000000),
    ("obsolete_days", "Série obsoleta após (dias sem barra nova)", 1, 36500),
    ("backup_interval_hours", "Cópia de segurança a cada (horas)", 1, 8760),
    ("backup_keep", "Cópias guardadas", 1, 1000),
    ("logs_keep_days", "Apagar logs com mais de (dias; 0 = nunca)", 0, 36500),
    ("exports_keep_days", "Apagar exportações com mais de (dias; 0 = nunca)", 0, 36500),
    ("retention_intraday_days", "Reter barras intradiárias por (dias; 0 = tudo)", 0, 36500),
    ("retention_ticks_days", "Reter ticks e book por (dias; 0 = tudo)", 0, 36500),
    ("max_series_mb", "Avisar quando uma série passar de (MB; 0 = não)", 0, 10000000),
)


def _fmt_date(value):
    if not value:
        return "—"
    if isinstance(value, str):
        return value.replace("T", " ")[:16]
    return value.strftime("%d/%m/%Y")


class ResourcesPanel:
    def __init__(self, parent, ui):
        self.ui = ui
        self.frame = ttk.Frame(parent, padding=12)
        self.report = None
        self.scan_result = None
        self.archives = []
        self.checked = set()
        self.loaded = False
        self.busy = False
        self.last_cycle = None
        self._cycle_running = False
        self.rule_vars = {code: tk.BooleanVar(value=True) for code in hygiene.RULES}
        self.show_protected = tk.BooleanVar(value=False)

        self._build_header()
        self._build_kpis()
        self.alert_label = ttk.Label(self.frame, text="", style="Warn.TLabel", wraplength=900, justify=tk.LEFT)
        self.tabs = ttk.Notebook(self.frame, style="Compact.TNotebook")
        self.tabs.pack(fill=tk.BOTH, expand=True)
        self._build_space_tab()
        self._build_clean_tab()
        self._build_archive_tab()
        self._build_files_tab()
        self._build_auto_tab()
        self._build_history_tab()
        self.frame.bind("<Configure>", self._resized, add="+")
        ui.theme.on_change(self._draw_history)
        ui.root.after(TICK_MS, self._tick)

    # --- montagem ----------------------------------------------------------------------

    def _build_header(self):
        head = ttk.Frame(self.frame)
        head.pack(fill=tk.X, pady=(0, 10))
        ttk.Label(head, text="Gestão de recursos", style="Page.Title.TLabel").pack(side=tk.LEFT)
        self.measured_label = ttk.Label(head, text="ainda não medido", style="Page.Muted.TLabel")
        self.measured_label.pack(side=tk.LEFT, padx=(12, 0), pady=(6, 0))
        self.refresh_button = ttk.Button(head, text="Medir agora", style="Accent.TButton", command=self.refresh)
        self.refresh_button.pack(side=tk.RIGHT)
        self.progress = ttk.Progressbar(head, mode="indeterminate", length=120)
        Tooltip(self.refresh_button, "Mede a base, as tabelas, o disco e as pastas, e refaz a análise de "
                                     "higienização. Grava um retrato para o histórico.", self.ui.theme)

    def _kpi(self, parent, column, title, hint):
        body = card(parent, title, padding=12)
        body.master.grid(row=0, column=column, sticky="nsew", padx=(0 if column == 0 else 8, 0))
        value = ttk.Label(body, text="—", style="Value.TLabel")
        value.pack(anchor="w")
        sub = ttk.Label(body, text="", style="Muted.TLabel", wraplength=220, justify=tk.LEFT)
        sub.pack(anchor="w", pady=(2, 0))
        Tooltip(body.master, hint, self.ui.theme)
        return body, value, sub

    def _build_kpis(self):
        row = ttk.Frame(self.frame)
        row.pack(fill=tk.X, pady=(0, 8))
        for i in range(4):
            row.columnconfigure(i, weight=1, uniform="kpi")
        _, self.kpi_db, self.kpi_db_sub = self._kpi(
            row, 0, "Base de dados", "Tamanho do arquivo da base (com o diário de gravação) e o ritmo de crescimento "
                                     "medido pelos retratos.")
        body, self.kpi_disk, self.kpi_disk_sub = self._kpi(
            row, 1, "Disco livre", "Espaço livre no disco onde a base está.")
        self.disk_bar = ttk.Progressbar(body, maximum=100, length=160)
        self.disk_bar.pack(fill=tk.X, pady=(6, 0))
        body, self.kpi_reclaim, self.kpi_reclaim_sub = self._kpi(
            row, 2, "Recuperável", "Páginas livres dentro da base (sobras de dados apagados). \"Otimizar\" devolve "
                                   "esse espaço ao disco.")
        self.vacuum_button = ttk.Button(body, text="Otimizar", command=self.vacuum)
        self.vacuum_button.pack(anchor="w", pady=(6, 0))
        _, self.kpi_clean, self.kpi_clean_sub = self._kpi(
            row, 3, "Candidatas à limpeza", "Séries que caem em alguma regra de higienização e não estão "
                                            "protegidas. Veja a guia Higienização.")

    def _build_space_tab(self):
        tab = ttk.Frame(self.tabs, padding=10, style="Surface.TFrame")
        self.tabs.add(tab, text="Espaço")
        row = ttk.Frame(tab, style="Surface.TFrame")
        row.pack(fill=tk.X, side=tk.BOTTOM, pady=(8, 0))
        self.trim_button = ttk.Button(row, text="Aparar série...", command=self.trim_selected)
        self.trim_button.pack(side=tk.LEFT)
        Tooltip(self.trim_button, "Mantém só os últimos N dias da série selecionada: as linhas mais velhas vão para "
                                  "o arquivo (reversível) e saem da base. Bom para M1 e ticks, que crescem rápido.",
                self.ui.theme)
        self.dirs_label = ttk.Label(row, text="", style="Muted.TLabel")
        self.dirs_label.pack(side=tk.LEFT, padx=(12, 0))

        limits = ttk.Frame(tab, style="Surface.TFrame")
        limits.pack(fill=tk.X, side=tk.BOTTOM, pady=(8, 0))
        p = policy_mod.load(self._config())
        ttk.Label(limits, text="Parar extrações com disco livre abaixo de (%)", style="Surface.TLabel").pack(
            side=tk.LEFT)
        self.block_disk_var = tk.StringVar(value=self._num_text(p.block_disk_free_pct))
        ttk.Spinbox(limits, from_=0, to=99, textvariable=self.block_disk_var, width=5).pack(side=tk.LEFT, padx=(6, 14))
        ttk.Label(limits, text="ou base acima de (GB)", style="Surface.TLabel").pack(side=tk.LEFT)
        self.block_db_var = tk.StringVar(value=self._num_text(p.block_db_gb))
        ttk.Spinbox(limits, from_=0, to=100000, textvariable=self.block_db_var, width=6).pack(side=tk.LEFT,
                                                                                           padx=(6, 14))
        ttk.Label(limits, text="Retenção por ativo", style="Surface.TLabel").pack(side=tk.LEFT)
        self.overrides_var = tk.StringVar(value=", ".join(f"{s}:{d}" for s, d in p.retention_overrides.items()))
        overrides = ttk.Entry(limits, textvariable=self.overrides_var, width=22)
        overrides.pack(side=tk.LEFT, padx=(6, 8), fill=tk.X, expand=True)
        Tooltip(overrides, "ATIVO:dias separados por vírgula (ex.: WIN$N:30, PETR4:0). Vale sobre a retenção da guia "
                           "Automação; 0 = guarda tudo desse ativo. 0 nos dois limites = nunca para a extração.",
                self.ui.theme)
        ttk.Button(limits, text="Salvar limites", command=self.save_limits).pack(side=tk.LEFT)
        frame, self.space_tree = scrolled_tree(tab, SPACE_COLUMNS, SPACE_WIDTHS, SPACE_ANCHORS, height=12)
        frame.pack(fill=tk.BOTH, expand=True)
        self.space_tree.tag_configure("internal", foreground=self.ui.theme.colors["muted"])
        for col in SPACE_COLUMNS:
            self.space_tree.heading(col, command=lambda c=col: self._sort_space(c))
        self._space_sort = ("Tamanho", True)

    def _build_clean_tab(self):
        tab = ttk.Frame(self.tabs, padding=10, style="Surface.TFrame")
        self.tabs.add(tab, text="Higienização")
        filters = ttk.Frame(tab, style="Surface.TFrame")
        filters.pack(fill=tk.X, pady=(0, 8))
        ttk.Label(filters, text="Regras:", style="Surface.TLabel").pack(side=tk.LEFT)
        for code, var in self.rule_vars.items():
            ttk.Checkbutton(filters, text=hygiene.LABELS[code], variable=var, command=self._show_scan).pack(
                side=tk.LEFT, padx=(8, 0))
        ttk.Checkbutton(filters, text="Mostrar protegidas", variable=self.show_protected,
                        command=self._show_scan).pack(side=tk.RIGHT)

        actions_row = ttk.Frame(tab, style="Surface.TFrame")
        actions_row.pack(fill=tk.X, side=tk.BOTTOM, pady=(8, 0))
        self.archive_button = ttk.Button(actions_row, text="Arquivar marcadas", style="Accent.TButton",
                                         command=self.archive_checked)
        self.archive_button.pack(side=tk.LEFT)
        Tooltip(self.archive_button, "Cada série marcada é copiada para um arquivo, conferida e só então retirada "
                                     "da base. Volta pela guia Arquivo.", self.ui.theme)
        ttk.Button(actions_row, text=self.ui.theme.glyphs("☑ Marcar todas"), command=self.check_all).pack(
            side=tk.LEFT, padx=(8, 0))
        ttk.Button(actions_row, text=self.ui.theme.glyphs("☐ Desmarcar"), command=self.uncheck_all).pack(
            side=tk.LEFT, padx=(8, 0))
        self.orphans_button = ttk.Button(actions_row, text="Limpar registros órfãos", command=self.clean_orphans)
        self.orphans_button.pack(side=tk.RIGHT)
        self.clean_summary = ttk.Label(actions_row, text="", style="Muted.TLabel")
        self.clean_summary.pack(side=tk.LEFT, padx=(12, 0))

        frame, self.clean_tree = scrolled_tree(tab, CLEAN_COLUMNS, CLEAN_WIDTHS, CLEAN_ANCHORS, height=11,
                                               selectmode="extended")
        frame.pack(fill=tk.BOTH, expand=True)
        self.clean_tree.tag_configure("protected", foreground=self.ui.theme.colors["muted"])
        self.clean_tree.bind("<Button-1>", self._clean_clicked, add="+")
        self.clean_tree.bind("<space>", lambda e: self._toggle(self.clean_tree.selection()))

    def _build_archive_tab(self):
        tab = ttk.Frame(self.tabs, padding=10, style="Surface.TFrame")
        self.tabs.add(tab, text="Arquivo")
        row = ttk.Frame(tab, style="Surface.TFrame")
        row.pack(fill=tk.X, side=tk.BOTTOM, pady=(8, 0))
        self.restore_button = ttk.Button(row, text="Restaurar selecionado", command=self.restore_selected)
        self.restore_button.pack(side=tk.LEFT)
        ttk.Button(row, text="Abrir pasta do arquivo", command=self.open_archive_dir).pack(side=tk.LEFT, padx=(8, 0))
        self.archive_summary = ttk.Label(row, text="", style="Muted.TLabel")
        self.archive_summary.pack(side=tk.LEFT, padx=(12, 0))
        frame, self.archive_tree = scrolled_tree(tab, ARCHIVE_COLUMNS, ARCHIVE_WIDTHS, ARCHIVE_ANCHORS, height=12)
        frame.pack(fill=tk.BOTH, expand=True)
        self.archive_tree.bind("<Double-1>", lambda e: self.restore_selected())

    def _build_files_tab(self):
        tab = ttk.Frame(self.tabs, padding=10, style="Surface.TFrame")
        self.tabs.add(tab, text="Cópias e arquivos")
        row = ttk.Frame(tab, style="Surface.TFrame")
        row.pack(fill=tk.X, side=tk.BOTTOM, pady=(8, 0))
        self.backup_button = ttk.Button(row, text="Fazer cópia agora", style="Accent.TButton", command=self.backup)
        self.backup_button.pack(side=tk.LEFT)
        Tooltip(self.backup_button, "Cópia conferida da base inteira, feita com ela em uso. Para voltar a uma cópia: "
                                    "feche o programa e copie o arquivo por cima da base.", self.ui.theme)
        ttk.Button(row, text="Abrir pasta das cópias", command=self.open_backup_dir).pack(side=tk.LEFT, padx=(8, 0))
        self.back_button = ttk.Button(row, text="Voltar a esta cópia...", command=self.restore_backup)
        self.back_button.pack(side=tk.LEFT, padx=(8, 0))
        self.files_button = ttk.Button(row, text="Apagar arquivos antigos", command=self.clean_files)
        self.files_button.pack(side=tk.RIGHT)
        self.files_summary = ttk.Label(tab, text="", style="Muted.TLabel", wraplength=760, justify=tk.LEFT)
        self.files_summary.pack(fill=tk.X, side=tk.BOTTOM, pady=(8, 0))
        frame, self.backup_tree = scrolled_tree(tab, ("Cópia", "Tamanho", "Criada em"), (420, 100, 140),
                                                ("w", "e", "w"), height=10)
        frame.pack(fill=tk.BOTH, expand=True)
        self.backups, self.old_files = [], []

    def _show_files(self):
        tree = self.backup_tree
        tree.delete(*tree.get_children())
        for b in self.backups:
            tree.insert("", tk.END, iid=b["path"], values=(Path(b["path"]).name, hb(b["bytes"]), f"{b['created_at']:%d/%m/%Y %H:%M}"))
        size = sum(f["bytes"] for f in self.old_files)
        text = (f"{len(self.backups)} cópia(s), {hb(sum(b['bytes'] for b in self.backups))}. " if self.backups
                else "Nenhuma cópia de segurança ainda. ")
        text += (f"Arquivos antigos de logs e exportações: {len(self.old_files)} ({hb(size)})." if self.old_files
                 else "Nenhum arquivo antigo de logs ou exportações pelos dias da guia Automação.")
        self.files_summary.config(text=text)
        if not self.old_files:
            self.files_button.state(["disabled"])

    def backup(self):
        db, config = self._db(), self._config()
        if not db:
            return

        def done(result):
            if result["ok"]:
                self.ui.log(f"Recursos: cópia de segurança criada em {result['path']} ({hb(result['bytes'])}).")
            else:
                messagebox.showerror("Cópia de segurança", result["error"], parent=self.frame)
        self._run_action("copiando a base", lambda: files.backup(db, config, "ui"), done)

    def clean_files(self):
        db, config = self._db(), self._config()
        if not db or not self.old_files:
            return
        by_folder = {}
        for f in self.old_files:
            by_folder[f["folder"]] = by_folder.get(f["folder"], 0) + 1
        if not messagebox.askyesno("Apagar arquivos antigos", "Apagar " + ", ".join(
                f"{n} de {k.lower()}" for k, n in by_folder.items()) + f" ({hb(sum(f['bytes'] for f in self.old_files))})?"
                "\n\nEstes arquivos não vão para o arquivo: somem de vez.", parent=self.frame):
            return

        def done(result):
            self.ui.log(f"Recursos: {result['removed']} arquivo(s) antigo(s) apagado(s), {hb(result['bytes'])}.")
        self._run_action("apagando arquivos antigos", lambda: files.clean_files(db, config, "ui"), done)

    def restore_backup(self):
        db, config = self._db(), self._config()
        selection = self.backup_tree.selection()
        if not db or not selection:
            messagebox.showinfo("Voltar a uma cópia", "Selecione a cópia na lista.", parent=self.frame)
            return
        path = selection[0]
        if not messagebox.askyesno(
                "Voltar a uma cópia", f"Trocar a base inteira pela cópia {Path(path).name}?\n\nTudo o que foi "
                "gravado depois dela some da base. Antes, o estado atual é guardado numa cópia nova (se isso falhar, "
                "nada muda).\n\nPare extrações e backtests antes de continuar.", icon="warning", parent=self.frame):
            return

        def done(result):
            if result["ok"]:
                self.ui.log(f"Recursos: base trocada pela cópia {Path(path).name}; estado anterior em "
                            f"{result['safety_copy']}.")
                self.ui.refresh_inventory()
            else:
                messagebox.showerror("Voltar a uma cópia", result["error"], parent=self.frame)
        self._run_action("voltando à cópia", lambda: files.restore_backup(db, config, path, "ui"), done)

    def trim_selected(self):
        db, config = self._db(), self._config()
        selection = self.space_tree.selection()
        item = next((t for t in (self.report or {}).get("tables", []) if selection and t["table"] == selection[0]),
                    None)
        if not db or item is None or item["kind"] == storage.KIND_INTERNAL:
            messagebox.showinfo("Aparar série", "Selecione na lista uma série de barras, ticks ou book.",
                                parent=self.frame)
            return
        days = simpledialog.askinteger("Aparar série", f"{item['table']}: manter os últimos quantos dias?",
                                       parent=self.frame, minvalue=1, maxvalue=36500, initialvalue=365)
        if not days:
            return
        if not messagebox.askyesno("Aparar série", f"As linhas de {item['table']} com mais de {days} dias vão para o "
                                                   "arquivo e saem da base. Você pode devolvê-las pela guia Arquivo."
                                                   "\n\nContinuar?", parent=self.frame):
            return

        def done(results):
            r = results[0]
            if r["ok"]:
                self.ui.log(f"Recursos: {r['table']}: {r['rows']} linha(s) aparada(s).")
                self.ui.refresh_inventory()
            else:
                messagebox.showerror("Aparar série", r["error"], parent=self.frame)
        self._run_action("aparando", lambda: actions.trim_tables(db, config, {item["table"]: days}, "ui"), done)

    def open_backup_dir(self):
        folder = files.backup_dir(self._db(), policy_mod.load(self._config()))
        folder.mkdir(parents=True, exist_ok=True)
        self.ui._open_path(folder)

    def _build_auto_tab(self):
        tab = ttk.Frame(self.tabs, padding=12, style="Surface.TFrame")
        self.tabs.add(tab, text="Automação")
        p = policy_mod.load(self._config())
        self.auto_vars = {"auto_enabled": tk.BooleanVar(value=p.auto_enabled)}
        master = ttk.Checkbutton(tab, text="Ligar monitoramento e automação (opt-in)",
                                 variable=self.auto_vars["auto_enabled"], command=self._auto_toggled)
        master.pack(anchor="w")
        Tooltip(master, "Desligado, nada roda sozinho. Ligado, a cada intervalo o programa mede a base, avisa pelos "
                        "pop-ups do sistema e executa só as ações marcadas abaixo.", self.ui.theme)
        self.auto_widgets = []
        flags = ttk.Frame(tab, style="Surface.TFrame")
        flags.pack(fill=tk.X, pady=(8, 0), padx=(22, 0))
        for key, label, hint in AUTO_FLAGS:
            self.auto_vars[key] = tk.BooleanVar(value=getattr(p, key))
            w = ttk.Checkbutton(flags, text=label, variable=self.auto_vars[key])
            w.pack(anchor="w", pady=1)
            Tooltip(w, hint, self.ui.theme)
            self.auto_widgets.append(w)
            if key == "auto_archive":            # as regras ficam logo abaixo da opção a que pertencem
                rules = ttk.Frame(flags, style="Surface.TFrame")
                rules.pack(anchor="w", padx=(22, 0))
        self.auto_rule_vars = {}
        for code in hygiene.RULES:
            var = tk.BooleanVar(value=code in p.auto_archive_rules)
            self.auto_rule_vars[code] = var
            w = ttk.Checkbutton(rules, text=hygiene.LABELS[code], variable=var)
            w.pack(side=tk.LEFT, padx=(0, 10))
            self.auto_widgets.append(w)

        numbers = ttk.Frame(tab, style="Surface.TFrame")
        numbers.pack(fill=tk.X, pady=(12, 0))
        self.auto_numbers = {}
        half = (len(AUTO_NUMBERS) + 1) // 2      # duas colunas: cabe em telas baixas
        for index, (key, label, low, high) in enumerate(AUTO_NUMBERS):
            row, col = index % half, (index // half) * 2
            ttk.Label(numbers, text=label, style="Surface.TLabel").grid(row=row, column=col, sticky="w", pady=2,
                                                                         padx=(0 if col == 0 else 28, 0))
            var = tk.StringVar(value=self._num_text(getattr(p, key)))
            spin = ttk.Spinbox(numbers, from_=low, to=high, textvariable=var, width=8)
            spin.grid(row=row, column=col + 1, sticky="w", padx=(12, 0), pady=2)
            self.auto_numbers[key] = var

        last = len(AUTO_NUMBERS) % half or half      # o formato ocupa a vaga livre da segunda coluna
        ttk.Label(numbers, text="Formato do arquivo de séries", style="Surface.TLabel").grid(
            row=last if len(AUTO_NUMBERS) % 2 else half, column=2, sticky="w", pady=2, padx=(28, 0))
        self.format_var = tk.StringVar(value=p.archive_format)
        fmt_box = ttk.Combobox(numbers, textvariable=self.format_var, values=policy_mod.ARCHIVE_FORMATS,
                               state="readonly", width=7)
        fmt_box.grid(row=last if len(AUTO_NUMBERS) % 2 else half, column=3, sticky="w", padx=(12, 0), pady=2)
        Tooltip(fmt_box, "sqlite: abre em qualquer ferramenta. parquet: bem menor (exige o pyarrow).", self.ui.theme)

        buttons = ttk.Frame(tab, style="Surface.TFrame")
        buttons.pack(fill=tk.X, pady=(12, 0), side=tk.BOTTOM, before=master)   # sempre visível, mesmo em tela baixa
        ttk.Button(buttons, text="Salvar", style="Accent.TButton", command=self.save_automation).pack(side=tk.LEFT)
        self.cycle_button = ttk.Button(buttons, text="Rodar um ciclo agora", command=lambda: self.run_cycle("ui"))
        self.cycle_button.pack(side=tk.LEFT, padx=(8, 0))
        self.auto_feedback = ttk.Label(buttons, text="", style="Muted.TLabel", wraplength=640, justify=tk.LEFT)
        self.auto_feedback.pack(side=tk.LEFT, padx=(12, 0))
        Tooltip(self.cycle_button, "Sem a interface aberta, agende no sistema: \"mt5x resources auto\" (cron no "
                                   "Linux, Agendador de Tarefas no Windows). Ele segue as mesmas opções.",
                self.ui.theme)
        self._auto_toggled()

    def _build_history_tab(self):
        tab = ttk.Frame(self.tabs, padding=10, style="Surface.TFrame")
        self.tabs.add(tab, text="Histórico")
        self.history_canvas = tk.Canvas(tab, height=150, highlightthickness=0,
                                        background=self.ui.theme.colors["surface"])
        self.history_canvas.pack(fill=tk.X)
        self.history_canvas.bind("<Configure>", lambda e: self._draw_history())
        frame, self.actions_tree = scrolled_tree(tab, ACTION_COLUMNS, ACTION_WIDTHS, height=8)
        frame.pack(fill=tk.BOTH, expand=True, pady=(8, 0))
        self.snapshots = []

    # --- leitura -----------------------------------------------------------------------

    def _config(self):
        return getattr(self.ui.app, "config", None)

    def _db(self):
        db = getattr(self.ui.app, "db_manager", None)
        return db if db is not None and db.is_connected() else None

    def on_show(self):
        """Chamado quando a aba é aberta: só a primeira vez lê a base."""
        if not self.loaded:
            self.refresh()

    def _set_busy(self, busy, text=None):
        self.busy = busy
        state = ["disabled"] if busy else ["!disabled"]
        for button in (self.refresh_button, self.vacuum_button, self.archive_button, self.orphans_button,
                       self.restore_button, self.cycle_button, self.backup_button, self.files_button, self.trim_button,
                       self.back_button):
            button.state(state)
        if busy:
            self.progress.pack(side=tk.RIGHT, padx=(0, 10))
            self.progress.start(12)
            if text:
                self.measured_label.config(text=text)
        else:
            self.progress.stop()
            self.progress.pack_forget()

    def refresh(self):
        db = self._db()
        if db is None:
            self.measured_label.config(text="base de dados indisponível")
            return
        if self.busy:
            return
        self.loaded = True
        self._set_busy(True, "medindo...")
        config, selected = self._config(), list(getattr(self.ui.app, "selected_symbols", []) or [])

        def work():
            policy = policy_mod.load(config)
            report = storage.measure(db, policy_mod.archive_dir(db, policy), config=config)
            storage.record_snapshot(db, report, policy.keep_snapshots_days)
            sizes = {t["table"]: t["bytes"] for t in report["tables"]}
            return {"report": report, "policy": policy,
                    "scan": hygiene.scan(db, policy, selected, sizes),
                    "archives": actions.list_archives(db, config),
                    "backups": files.list_backups(db, config),
                    "old_files": files.old_files(db, config),
                    "snapshots": storage.load_snapshots(db, 365),
                    "actions": actions.load_actions(db, 200),
                    "last_cycle": automation.last_cycle(db)}
        self.ui.run_bg(work, self._loaded)

    def _loaded(self, result, error):
        self._set_busy(False)
        if error is not None:
            self.measured_label.config(text=f"falha ao medir: {error}")
            return
        self.report, self.scan_result = result["report"], result["scan"]
        self.archives, self.snapshots = result["archives"], result["snapshots"]
        self.last_cycle = result["last_cycle"]
        self.backups, self.old_files = result["backups"], result["old_files"]
        policy = result["policy"]
        self.measured_label.config(text=f"medido às {self.report['measured_at']:%H:%M:%S}")
        self._show_kpis(policy)
        self._show_space()
        self._show_scan()
        self._show_archives()
        self._show_files()
        self._show_actions(result["actions"])
        self._draw_history()
        self._show_auto_status(policy)

    # --- exibição ----------------------------------------------------------------------

    def _show_kpis(self, policy):
        r = self.report
        trend = storage.growth(self.snapshots)
        self.kpi_db.config(text=hb(r["db_bytes"]))
        n_data = sum(1 for t in r["tables"] if t["kind"] != storage.KIND_INTERNAL)
        sub = f"{fmt_int(n_data)} tabela(s) de dados"
        if trend:
            sub += f" · {'+' if trend['bytes_per_day'] >= 0 else ''}{hb(trend['bytes_per_day'])}/dia"
        self.kpi_db_sub.config(text=sub)
        disk = r["disk"]
        if disk:
            self.kpi_disk.config(text=hb(disk["free"]),
                                 style="Bad.TLabel" if disk["free_pct"] < policy.alert_disk_free_pct else
                                 "Value.TLabel")
            used = 100 - disk["free_pct"]
            self.disk_bar.config(value=used)
            full = trend["days_to_full"] if trend else None
            days = f" · cheio em ~{full:.0f} dias" if full is not None and full < 3650 else ""
            self.kpi_disk_sub.config(text=f"{disk['free_pct']:.1f}% livre de {hb(disk['total'])}{days}".replace(
                ".", ",", 1))
        else:
            self.kpi_disk.config(text="—", style="Value.TLabel")
            self.kpi_disk_sub.config(text="disco não medido (banco remoto)")
        reclaim = r["reclaimable_bytes"]
        self.kpi_reclaim.config(text=hb(reclaim) if reclaim is not None else "—")
        share = (reclaim or 0) / r["db_bytes"] * 100 if r["db_bytes"] else 0
        self.kpi_reclaim_sub.config(text=f"{share:.1f}% da base" if reclaim is not None else "não informado")
        cands = self.scan_result["candidates"]
        self.kpi_clean.config(text=hb(sum(c["bytes"] or 0 for c in cands)))
        self.kpi_clean_sub.config(text=f"{len(cands)} série(s) · {len(self.scan_result['protected'])} protegida(s)")
        found = automation.alerts(r, policy) + [{"level": "aviso", "text": w} for w in r["warnings"]] + \
            [{"level": "aviso", "text": e} for e in policy.errors]
        self.alert_label.config(text="\n".join(("⚠ " if a["level"] == "aviso" else "⛔ ") + a["text"] for a in found),
                                style="Bad.TLabel" if any(a["level"] == "crítico" for a in found) else "Warn.TLabel")
        if found:
            self.alert_label.pack(fill=tk.X, pady=(0, 6), before=self.tabs)
        else:
            self.alert_label.pack_forget()

    def _show_space(self):
        tree = self.space_tree
        tree.delete(*tree.get_children())
        total = self.report["db_bytes"] or 0
        col, reverse = self._space_sort
        keys = {"Tabela": lambda t: t["table"], "Tipo": lambda t: t["kind"], "Ativo": lambda t: t["symbol"] or "",
                "Timeframe": lambda t: t["timeframe"] or "", "Linhas": lambda t: t["rows"],
                "Tamanho": lambda t: t["bytes"] or 0, "% da base": lambda t: t["bytes"] or 0,
                "Última barra": lambda t: t["end"] or datetime.min}
        for t in sorted(self.report["tables"], key=keys[col], reverse=reverse):
            size = hb(t["bytes"]) + (" *" if t["estimated"] else "")
            share = f"{(t['bytes'] or 0) / total * 100:.1f}%" if total else "—"
            tree.insert("", tk.END, iid=t["table"], values=(t["table"], t["kind"], t["symbol"] or "", t["timeframe"] or "",
                                            fmt_int(t["rows"]), size, share, _fmt_date(t["end"])),
                        tags=("internal",) if t["kind"] == storage.KIND_INTERNAL else ())
        dirs = self.report["dirs"]
        self.dirs_label.config(text="Pastas: " + " · ".join(f"{k} {hb(v['bytes'])}" for k, v in dirs.items()) +
                               ("   (* tamanho estimado pelas linhas)" if any(t["estimated"]
                                                                            for t in self.report["tables"]) else ""))

    def _sort_space(self, column):
        col, reverse = self._space_sort
        self._space_sort = (column, not reverse if col == column else column in ("Linhas", "Tamanho", "% da base"))
        if self.report:
            self._show_space()

    def _visible_candidates(self):
        active = {code for code, var in self.rule_vars.items() if var.get()}
        rows = [(c, False) for c in self.scan_result["candidates"] if active & set(c["reasons"])]
        if self.show_protected.get():
            rows += [(p, True) for p in self.scan_result["protected"] if active & set(p["reasons"])]
        return rows

    def _show_scan(self):
        if not self.scan_result:
            return
        tree = self.clean_tree
        tree.delete(*tree.get_children())
        visible = self._visible_candidates()
        self.checked &= {c["table"] for c, protected in visible if not protected}
        for c, protected in visible:
            reasons = "; ".join(f"{hygiene.LABELS.get(k, k)}: {v}" for k, v in c["reasons"].items())
            if protected:
                reasons = "Protegida (" + ", ".join(c["protections"]) + ") · " + reasons
            mark = "" if protected else self._mark(c["table"] in self.checked)
            tree.insert("", tk.END, iid=c["table"], values=(mark, c["symbol"], c["timeframe"], hb(c["bytes"]),
                                                            _fmt_date(c["end"]), reasons),
                        tags=("protected",) if protected else ())
        orphans = self.scan_result["orphans"]
        n_orphans = len(orphans["symbol_tables"]) + len(orphans["extraction_log"])
        self.orphans_button.config(text=f"Limpar registros órfãos ({n_orphans})")
        if n_orphans == 0:
            self.orphans_button.state(["disabled"])
        self._update_clean_summary()

    def _update_clean_summary(self):
        by_table = {c["table"]: c for c in self.scan_result["candidates"]} if self.scan_result else {}
        size = sum(by_table[t]["bytes"] or 0 for t in self.checked if t in by_table)
        self.clean_summary.config(text=f"{len(self.checked)} marcada(s) · {hb(size)} a liberar" if self.checked
                                  else "marque as séries na primeira coluna")
        self.archive_button.state(["!disabled"] if self.checked and not self.busy else ["disabled"])

    def _clean_clicked(self, event):
        if self.clean_tree.identify_region(event.x, event.y) != "cell":
            return
        if self.clean_tree.identify_column(event.x) != "#1":
            return
        iid = self.clean_tree.identify_row(event.y)
        if iid:
            self._toggle([iid])
            return "break"

    def _toggle(self, iids):
        candidates = {c["table"] for c in self.scan_result["candidates"]} if self.scan_result else set()
        for iid in iids:
            if iid not in candidates:
                continue
            self.checked ^= {iid}
            self.clean_tree.set(iid, "✓", self._mark(iid in self.checked))
        self._update_clean_summary()

    def _mark(self, on):
        return self.ui.theme.glyphs(CHECK_ON if on else CHECK_OFF)

    def check_all(self):
        self.checked = {c["table"] for c, protected in self._visible_candidates() if not protected}
        self._show_scan()

    def uncheck_all(self):
        self.checked = set()
        self._show_scan()

    def _show_archives(self):
        tree = self.archive_tree
        tree.delete(*tree.get_children())
        for a in self.archives:
            reason = "; ".join(hygiene.LABELS.get(k, k) for k in (a["reasons"] or {})) or (a["error"] or "manual")
            if a.get("kind") == "trim":
                reason = f"Histórico aparado (mantidos {a.get('keep_days')} dias)"
            if a.get("format") == "parquet":
                reason += " · parquet"
            tree.insert("", tk.END, iid=a["path"], values=(a["table"], a["symbol"] or "",
                                                           fmt_int(a["rows"]) if a["rows"] is not None else "—",
                                                           hb(a["bytes"]), _fmt_date(a["archived_at"]), reason))
        self.archive_summary.config(text=f"{len(self.archives)} arquivo(s), "
                                         f"{hb(sum(a['bytes'] for a in self.archives))} no disco")

    def _show_actions(self, rows):
        tree = self.actions_tree
        tree.delete(*tree.get_children())
        for a in rows:
            space = f"{hb(a['bytes_before'])} → {hb(a['bytes_after'])}" if a.get("bytes_after") else \
                hb(a.get("bytes_before")) if a.get("bytes_before") else ""
            tree.insert("", tk.END, values=(a["at"], a["action"], a["target"] or "", a["origin"],
                                            "ok" if a["ok"] else "falhou", space))

    def _draw_history(self):
        canvas = self.history_canvas
        colors = self.ui.theme.colors
        canvas.configure(background=colors["surface"])
        canvas.delete("all")
        w, h = max(canvas.winfo_width(), 200), max(canvas.winfo_height(), 120)
        points = [(s["taken_at"], s["db_bytes"]) for s in self.snapshots]
        if len(points) < 2:
            canvas.create_text(w / 2, h / 2, fill=colors["muted"], font=self.ui.theme.font_small,
                               text="O gráfico do tamanho da base aparece a partir do segundo retrato "
                                    "(cada \"Medir agora\" ou ciclo da automação grava um).")
            return
        pad_l, pad_r, pad_t, pad_b = 70, 12, 14, 22
        t0, t1 = points[0][0], points[-1][0]
        span = max((t1 - t0).total_seconds(), 1)
        low, high = min(v for _, v in points), max(v for _, v in points)
        if high == low:
            low, high = low * 0.95, high * 1.05 + 1
        def x(t):
            return pad_l + (t - t0).total_seconds() / span * (w - pad_l - pad_r)
        def y(v):
            return pad_t + (high - v) / (high - low) * (h - pad_t - pad_b)
        for frac in (0, 0.5, 1):
            value = low + (high - low) * frac
            canvas.create_line(pad_l, y(value), w - pad_r, y(value), fill=colors["grid"])
            canvas.create_text(pad_l - 6, y(value), text=hb(value), anchor="e", fill=colors["muted"],
                               font=self.ui.theme.font_small)
        coords = [c for t, v in points for c in (x(t), y(v))]
        canvas.create_line(*coords, fill=colors["accent"], width=2)
        last_t, last_v = points[-1]
        canvas.create_oval(x(last_t) - 3, y(last_v) - 3, x(last_t) + 3, y(last_v) + 3, fill=colors["accent"],
                           outline="")
        canvas.create_text(pad_l, h - 4, text=f"{t0:%d/%m %H:%M}", anchor="sw", fill=colors["muted"],
                           font=self.ui.theme.font_small)
        canvas.create_text(w - pad_r, h - 4, text=f"{t1:%d/%m %H:%M}", anchor="se", fill=colors["muted"],
                           font=self.ui.theme.font_small)

    def _resized(self, event):
        wrap = max(400, event.width - 40)
        self.alert_label.config(wraplength=wrap)

    # --- ações -------------------------------------------------------------------------

    def _run_action(self, label, work, done):
        if self.busy:
            return
        self._set_busy(True, label + "...")
        self.ui.log(f"Recursos: {label}...")

        def finished(result, error):
            self._set_busy(False)
            if isinstance(error, actions.Busy):
                messagebox.showinfo("Gestão de recursos", str(error), parent=self.frame)
            elif error is not None:
                messagebox.showerror("Gestão de recursos", f"{label} falhou: {error}", parent=self.frame)
            else:
                done(result)
            self.refresh()
        self.ui.run_bg(work, finished)

    def archive_checked(self):
        db, config = self._db(), self._config()
        by_table = {c["table"]: c for c in self.scan_result["candidates"]} if self.scan_result else {}
        tables = [t for t in self.checked if t in by_table]
        if not db or not tables:
            return
        size = sum(by_table[t]["bytes"] or 0 for t in tables)
        folder = policy_mod.archive_dir(db, policy_mod.load(config))
        listing = "\n".join(f"• {by_table[t]['symbol']} {by_table[t]['timeframe']}" for t in sorted(tables)[:12])
        more = f"\n... e mais {len(tables) - 12}" if len(tables) > 12 else ""
        if not messagebox.askyesno(
                "Arquivar séries",
                f"Arquivar {len(tables)} série(s) ({hb(size)})?\n\n{listing}{more}\n\nCada série é copiada para "
                f"{folder}, conferida e retirada da base. Você pode restaurá-la depois pela guia Arquivo.\n\n"
                "Depois, use \"Otimizar\" para devolver o espaço ao disco.", parent=self.frame):
            return
        reasons = {t: by_table[t]["reasons"] for t in tables}

        def done(results):
            ok = [r for r in results if r["ok"]]
            failed = [r for r in results if not r["ok"]]
            self.checked -= {r["table"] for r in ok}
            self.ui.log(f"Recursos: {len(ok)} série(s) arquivada(s)" + (
                f"; {len(failed)} com falha: " + "; ".join(f"{r['table']} ({r['error']})" for r in failed)
                if failed else "."))
            if failed:
                messagebox.showwarning("Arquivar séries", f"{len(failed)} série(s) não foram arquivadas. "
                                                          "Veja a aba Atividade.", parent=self.frame)
            self.ui.refresh_inventory()
        self._run_action("arquivando", lambda: actions.archive_tables(db, config, tables, "ui", reasons), done)

    def restore_selected(self):
        db, config = self._db(), self._config()
        selection = self.archive_tree.selection()
        if not db or not selection:
            return
        path = selection[0]
        item = next((a for a in self.archives if a["path"] == path), None)
        if item is None or not messagebox.askyesno(
                "Restaurar série", f"Devolver {item['table']} ({fmt_int(item['rows'] or 0)} linhas) à base?",
                parent=self.frame):
            return

        def done(result):
            if result["ok"]:
                self.ui.log(f"Recursos: {result['table']} restaurada ({result['rows']} linhas).")
                self.ui.refresh_inventory()
            else:
                messagebox.showerror("Restaurar série", f"Não foi restaurada: {result['error']}", parent=self.frame)
        self._run_action("restaurando", lambda: actions.restore_archive(db, config, path, "ui"), done)

    def vacuum(self):
        db, config = self._db(), self._config()
        if not db:
            return
        if not messagebox.askyesno(
                "Otimizar a base", "Otimizar reescreve a base para devolver ao disco o espaço livre dentro dela. "
                                   "Pode levar alguns minutos em bases grandes e, enquanto roda, extrações e "
                                   "backtests esperam.\n\nContinuar?", parent=self.frame):
            return

        def done(result):
            if result["ok"]:
                freed = (result["bytes_before"] or 0) - (result["bytes_after"] or 0)
                self.ui.log(f"Recursos: base otimizada, {hb(freed)} devolvidos ao disco.")
            else:
                messagebox.showerror("Otimizar a base", result["error"], parent=self.frame)
        self._run_action("otimizando", lambda: actions.vacuum(db, config, "ui"), done)

    def clean_orphans(self):
        db, config = self._db(), self._config()
        if not db:
            return

        def done(result):
            self.ui.log(f"Recursos: registros órfãos removidos: {result['removed']}.")
        self._run_action("limpando registros órfãos", lambda: actions.clean_orphans(db, config, "ui"), done)

    def open_archive_dir(self):
        db = self._db()
        folder = policy_mod.archive_dir(db, policy_mod.load(self._config()))
        folder.mkdir(parents=True, exist_ok=True)
        self.ui._open_path(folder)

    # --- automação ---------------------------------------------------------------------

    @staticmethod
    def _num_text(value):
        return str(int(value)) if float(value).is_integer() else str(value)

    def _auto_toggled(self):
        state = ["!disabled"] if self.auto_vars["auto_enabled"].get() else ["disabled"]
        for w in self.auto_widgets:
            w.state(state)

    def save_limits(self):
        values = {}
        for key, var, high in (("block_disk_free_pct", self.block_disk_var, 99),
                               ("block_db_gb", self.block_db_var, 100000)):
            try:
                number = float(var.get().strip().replace(",", "."))
                if not 0 <= number <= high:
                    raise ValueError
            except ValueError:
                messagebox.showerror("Limites", f"Use um número entre 0 e {high}.", parent=self.frame)
                return
            values[key] = self._num_text(number)
        cfg = configparser.ConfigParser(interpolation=None)
        cfg[policy_mod.SECTION] = {"retention_overrides": self.overrides_var.get()}
        errors = []
        parsed = policy_mod._overrides(cfg, errors)
        if errors:
            messagebox.showerror("Retenção por ativo", "\n".join(errors), parent=self.frame)
            return
        values["retention_overrides"] = ", ".join(f"{s}:{d}" for s, d in parsed.items())
        if self._write_values(values):
            self.ui.log("Limites e retenção por ativo gravados. Valem nas próximas extrações.")

    def _write_values(self, values) -> bool:
        config = self._config()
        path = Path(getattr(self.ui.app, "config_path", "config/config.ini")).resolve()
        try:
            for key, value in values.items():
                config_builder.set_config_value(path, policy_mod.SECTION, key, value)
        except (OSError, ValueError) as e:
            log.error(f"Erro ao gravar [{policy_mod.SECTION}] em {path}: {e}")
            messagebox.showerror("Gestão de recursos", f"Não foi possível gravar em {path.name}: {e}",
                                 parent=self.frame)
            return False
        if config is not None:
            if not config.has_section(policy_mod.SECTION):
                config.add_section(policy_mod.SECTION)
            for key, value in values.items():
                config.set(policy_mod.SECTION, key, value)
        return True

    def save_automation(self):
        config = self._config()
        path = Path(getattr(self.ui.app, "config_path", "config/config.ini")).resolve()
        values = {key: "true" if var.get() else "false" for key, var in self.auto_vars.items()}
        values["archive_format"] = self.format_var.get()
        values["auto_archive_rules"] = ",".join(c for c, v in self.auto_rule_vars.items() if v.get())
        for key, label, low, high in AUTO_NUMBERS:
            raw = self.auto_numbers[key].get().strip().replace(",", ".")
            try:
                number = float(raw)
                if not low <= number <= high:
                    raise ValueError
            except ValueError:
                self.auto_feedback.config(style="Bad.TLabel", text=f"\"{label}\": use um número entre {low} e {high}.")
                return
            values[key] = self._num_text(number)
        try:
            for key, value in values.items():
                config_builder.set_config_value(path, policy_mod.SECTION, key, value)
        except (OSError, ValueError) as e:
            log.error(f"Erro ao gravar [{policy_mod.SECTION}] em {path}: {e}")
            self.auto_feedback.config(style="Bad.TLabel", text=f"Não foi possível gravar em {path.name}: {e}")
            return
        if config is not None:
            if not config.has_section(policy_mod.SECTION):
                config.add_section(policy_mod.SECTION)
            for key, value in values.items():
                config.set(policy_mod.SECTION, key, value)
        self.ui.log(f"Configuração gravada: [{policy_mod.SECTION}] " + ", ".join(f"{k} = {v}" for k, v in values.items()))
        self._show_auto_status(policy_mod.load(config), saved=path.name)

    def _show_auto_status(self, policy, saved=None):
        parts = [f"Salvo em {saved}."] if saved else []
        if not policy.auto_enabled:
            parts.append("Automação desligada: nada roda sozinho.")
        else:
            acting = [label for key, label, _ in AUTO_FLAGS if getattr(policy, key)]
            parts.append(f"Ligada: ciclo a cada {policy.monitor_interval_minutes} min; " +
                         ("ações: " + "; ".join(acting).lower() if acting else "só mede e avisa") + ".")
            if self.last_cycle:
                nxt = self.last_cycle + timedelta(minutes=policy.monitor_interval_minutes)
                parts.append(f"Último ciclo: {self.last_cycle:%d/%m %H:%M}; próximo por volta de {nxt:%H:%M}.")
            else:
                parts.append("Nenhum ciclo ainda: o primeiro roda em até um minuto.")
        self.auto_feedback.config(style="Muted.TLabel" if not saved else "Good.TLabel", text=" ".join(parts))

    def _tick(self):
        """Agendador: confere a cada minuto se a automação está ligada e um ciclo venceu."""
        try:
            policy = policy_mod.load(self._config())
            if policy.auto_enabled and not self._cycle_running and not self.busy and self._db() is not None:
                if self.last_cycle is None:
                    self.last_cycle = automation.last_cycle(self._db())
                if automation.is_due(policy, self.last_cycle):
                    self.run_cycle("auto")
        except Exception as e:
            log.error(f"Agendador de recursos: {e}")
        try:
            self.ui.root.after(TICK_MS, self._tick)
        except tk.TclError:
            pass

    def run_cycle(self, origin):
        db, config = self._db(), self._config()
        if db is None or self._cycle_running:
            return
        self._cycle_running = True
        selected = list(getattr(self.ui.app, "selected_symbols", []) or [])
        if origin == "ui":
            self._set_busy(True, "ciclo de monitoramento...")

        def work():
            from mt5_extracao import notify
            return automation.run_cycle(db, config, origin=origin if origin == "auto" else "ui",
                                        selected=selected,
                                        notify_fn=lambda t, x, level: notify.send(config, t, x, level))

        def done(result, error):
            self._cycle_running = False
            if origin == "ui":
                self._set_busy(False)
            if error is not None:
                log.error(f"Ciclo de recursos falhou: {error}")
                return
            self.last_cycle = result["ran_at"] if policy_mod.load(config).auto_enabled else self.last_cycle
            for a in result["alerts"]:
                self.ui.log(f"Recursos [{a['level']}]: {a['text']}")
            for d in result["actions"]:
                self.ui.log(f"Recursos: ação automática '{d['action']}' executada.")
            if any(d["action"] == "arquivar" for d in result["actions"]):
                self.ui.refresh_inventory()
            if self.loaded or origin == "ui":
                self.refresh()
        self.ui.run_bg(work, done)
