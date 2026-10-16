import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, filedialog
import gc
import logging
import datetime
import traceback
import pandas as pd
import time
import threading
import queue
import os
from pathlib import Path
import json

# Importar módulos necessários para exportação
from mt5_extracao import asset_table, study, timeframes
from mt5_extracao.data_exporter import DataExporter
from mt5_extracao.error_handler import with_error_handling, ExportError
from mt5_extracao.ui.components import ContentPanel, FilterBar, PageHeader
from mt5_extracao.ui.analysis import AnalysisPanel
from mt5_extracao.ui.inventory import InventoryPanel
from mt5_extracao.ui.laboratory import LaboratoryPanel
from mt5_extracao.ui.resources import ResourcesPanel
from mt5_extracao.ui.robots import RobotsPanel
from mt5_extracao.ui.theme import Theme, Tooltip, card, fmt_int, fmt_num, scrolled_tree


# Handlers configurados em mt5_extracao.logging_setup (pontos de entrada)
log = logging.getLogger(__name__)

SYMBOL_COLUMNS = asset_table.COLUMNS


class UIManager:
    """
    Gerencia a interface gráfica (Tkinter) da aplicação MT5 Extração.

    Janela principal em abas: Ativos (escolher e extrair), Minha base (o que já existe),
    Análise (gráficos e números de estudo), Robôs, Laboratório (operações dos robôs), Recursos (espaço e higienização) e Atividade (log).
    Os painéis ficam em mt5_extracao/ui/.
    """
    def __init__(self, app_instance):
        """
        Inicializa o gerenciador da UI.

        Args:
            app_instance: A instância principal da aplicação (MT5Extracao)
                          para acessar dados e métodos.
        """
        self.app = app_instance
        self.root = app_instance.root # Usa a janela raiz da aplicação principal

        # Referências aos widgets que precisam ser acessados por outros métodos
        self.status_label = None
        self.log_text = None
        self.symbols_tree = None
        self.selected_listbox = None
        self.start_button = None
        self.stop_button = None
        self.search_var = tk.StringVar() # Variável para busca de símbolos
        self.filter_var = tk.StringVar(value="all")  # all | fav | data
        self._filter_job = None
        self._ui_queue = queue.Queue()   # chamadas vindas de outras threads (ver call_in_ui)
        self._terminal_lock = threading.Lock()   # uma abertura do terminal por vez (ver _open_organized_terminal)

        # O que existe na base (preenchido por refresh_inventory)
        self.inventory = []
        self.symbol_series = {}          # símbolo -> séries (itens do inventário) com barras
        self.existing_symbols_map = {}   # símbolo -> tabela M1 (usado pelo destaque e por código antigo)
        self.symbols_with_data = set()
        self.detail_symbol = None
        self.volumes = {}                # símbolo -> volume médio diário (asset_table.volumes)
        self.profit_cells = {}           # (símbolo, timeframe) -> último backtest de cada robô
        self.benchmarks = {}             # símbolo -> índice de referência vinculado
        self._symbol_sort = (None, False)

        # Favoritos ficam no banco (tabela _asset_prefs); o arquivo é a reserva sem banco e a origem da migração.
        # Preferências da interface ficam na pasta config/ (fora do controle de versão)
        config_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config")
        os.makedirs(config_dir, exist_ok=True)
        self.favorites_file = os.path.join(config_dir, "favorites.json")
        self.prefs_file = os.path.join(config_dir, "ui_prefs.json")
        self.favorite_symbols = self._load_json(self.favorites_file, []) or []
        self.prefs = self._load_json(self.prefs_file, {})

        log.info("UIManager inicializado.")

    # --- threads ----------------------------------------------------------------------

    def call_in_ui(self, fn, *args):
        """
        Executa fn(*args) na thread da interface. O Tkinter não pode ser usado por outras threads
        (extração e coleta rodam em threads próprias): elas enfileiram e a interface executa.
        """
        if threading.current_thread() is threading.main_thread():
            fn(*args)
        else:
            self._ui_queue.put((fn, args))

    def run_bg(self, work, done):
        """Roda work() fora da thread da interface e entrega done(resultado, erro) de volta nela."""
        def target():
            try:
                result, error = work(), None
            except Exception as e:
                log.error(f"Erro em tarefa de segundo plano: {traceback.format_exc()}")
                result, error = None, e
            self._ui_queue.put((done, (result, error)))
        threading.Thread(target=target, daemon=True).start()

    def open_results_panel(self, opener=None):
        """
        Abre o painel de resultados no navegador: a visão de conjunto ativos x timeframes x robôs. Sobe o painel
        vivo (servidor local, só nesta máquina e com chave), que se atualiza sozinho e tem botões de ação; se o
        servidor não puder subir, gera o arquivo exports/painel_resultados.html e abre a fotografia.
        """
        import webbrowser
        from mt5_extracao.bootstrap import paths
        from mt5_extracao.robots import matrix, panel, panel_server
        db, config = self.app.db_manager, getattr(self.app, "config", None)
        opener = opener or webbrowser.open
        server = getattr(self, "_panel_server", None)
        if server is None:
            try:
                from mt5_extracao import notify
                server = panel_server.PanelServer(
                    db, config, notifier=lambda title, text, level=None: notify.send(config, title, text,
                                                                                     level or notify.INFO),
                    connect=lambda: self.app.mt5_connector).start()
                self._panel_server = server
            except OSError as e:
                log.warning(f"O painel vivo não pôde subir ({e}); abrindo a fotografia.")
        if server is not None:
            opener(server.url)
            self.log("Painel de resultados aberto no navegador (vivo: atualiza sozinho e tem botões de ação). "
                     "Ele funciona enquanto este programa estiver aberto.")
            return
        target = paths.PROJECT_ROOT / "exports" / "painel_resultados.html"
        self.log("Gerando o painel de resultados...")

        def work():
            data = matrix.build(db, config)
            return panel.write(data, target), len(data["cells"])

        def done(result, error):
            if error is not None:
                self.log(f"Não foi possível gerar o painel: {error}")
                messagebox.showerror("Painel de resultados", f"Não foi possível gerar o painel:\n{error}",
                                     parent=self.root)
                return
            path, cells = result
            opener(Path(path).resolve().as_uri())
            self.log(f"Painel de resultados aberto no navegador ({cells} combinação(ões) testada(s)): {path}")
        self.run_bg(work, done)

    def _pump_ui_queue(self):
        try:
            while True:
                fn, args = self._ui_queue.get_nowait()
                try:
                    fn(*args)
                except tk.TclError as e:      # janela já fechada
                    log.debug(f"Atualização de interface ignorada: {e}")
                except Exception:
                    log.error(f"Erro ao atualizar a interface: {traceback.format_exc()}")
        except queue.Empty:
            pass
        # Lixo do Tk (variáveis, imagens) só pode ser destruído na thread principal; se ficar acumulado, o coletor do
        # Python pode rodar numa thread de segundo plano (run_bg) e o Tk derruba o processo. Coleta aqui a cada ~2 s.
        self._gc_ticks = getattr(self, "_gc_ticks", 0) + 1
        if self._gc_ticks >= 20:
            self._gc_ticks = 0
            gc.collect()
        try:
            self.root.after(100, self._pump_ui_queue)
        except tk.TclError:
            pass

    # --- preferências -----------------------------------------------------------------

    @staticmethod
    def _load_json(path, default):
        try:
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    return json.load(f)
        except Exception as e:
            log.error(f"Erro ao ler {path}: {e}")
        return default

    def save_prefs(self):
        """Guarda tema e seleção de ativos para a próxima abertura."""
        self.prefs["selected"] = list(self.app.selected_symbols)
        self.prefs["dark"] = bool(self.theme.dark)
        try:
            with open(self.prefs_file, "w", encoding="utf-8") as f:
                json.dump(self.prefs, f, ensure_ascii=False)
        except Exception as e:
            log.error(f"Erro ao salvar preferências da interface: {e}")

    def _prefs_db(self):
        db = getattr(self.app, "db_manager", None)
        return db if db and db.is_connected() else None

    def load_favorites(self):
        """Carrega os favoritos do banco (ou do arquivo, se não houver banco ou eles ainda não estiverem nele)."""
        try:
            stored = asset_table.load_prefs(self._prefs_db())
        except Exception as e:
            log.error(f"Erro ao ler os favoritos do banco: {e}")
            stored = None
        return stored["favorites"] if stored is not None else self._load_json(self.favorites_file, [])

    def save_favorites(self):
        """Salva a lista de símbolos favoritos no banco (sem banco, no arquivo)"""
        db = self._prefs_db()
        try:
            if db is not None:
                asset_table.save_favorites(db, self.favorite_symbols)
                return
            with open(self.favorites_file, 'w') as f:
                json.dump(self.favorite_symbols, f)
            log.info(f"Favoritos salvos em {self.favorites_file}")
        except Exception as e:
            log.error(f"Erro ao salvar favoritos: {e}")

    def _sync_favorites(self):
        """Na abertura: lê os favoritos do banco; na primeira vez, leva para lá os do arquivo antigo."""
        db = self._prefs_db()
        if db is None:
            return
        try:
            stored = asset_table.load_prefs(db)
            if stored is not None:
                self.favorite_symbols = stored["favorites"]
                self.benchmarks = stored["benchmarks"]
            elif self.favorite_symbols:
                asset_table.save_favorites(db, self.favorite_symbols)
                log.info(f"{len(self.favorite_symbols)} favorito(s) do arquivo levados para o banco.")
        except Exception as e:
            log.error(f"Erro ao ler os favoritos do banco: {e}")

    # --- montagem da janela -----------------------------------------------------------

    def setup_ui(self):
        """Configura a interface do usuário completa."""
        self.root = self.app.root
        # Tk só aceita a thread principal, inclusive para DESTRUIR variáveis e imagens. Com a coleta automática ligada,
        # o coletor de ciclos pode rodar dentro de uma thread de segundo plano (run_bg, extração) e destruir lixo do Tk
        # ali, derrubando o processo. Desligada, a contagem de referências segue liberando quase tudo na hora e os
        # ciclos são coletados por _pump_ui_queue, na thread principal, a cada ~2 s.
        gc.disable()
        self.root.after(100, self._pump_ui_queue)
        self.theme = Theme(self.root, dark=bool(self.prefs.get("dark")))
        self.style = ttk.Style()

        outer = ttk.Frame(self.root, padding=(14, 12, 14, 8))
        outer.pack(fill=tk.BOTH, expand=True)

        # Cabeçalho: nome do aplicativo e conexão com o MetaTrader 5
        header = ttk.Frame(outer)
        header.pack(fill=tk.X, pady=(0, 10))
        ttk.Label(header, text="MT5 Extração", style="Page.Title.TLabel").pack(side=tk.LEFT)
        self.check_button = ttk.Button(header, text="Verificar conexão", command=self.check_mt5_status)
        self.check_button.pack(side=tk.RIGHT)
        from mt5_extracao.ui import icons
        self._view_icon = icons.mt5_view(self.theme.colors["on_accent"])
        self.mt5_view_button = ttk.Button(header, text="Abrir visão MT5", image=self._view_icon, compound=tk.LEFT,
                                          style="Accent.TButton", command=self.open_mt5_view)
        self.mt5_view_button.pack(side=tk.RIGHT, padx=(0, 8))
        view_hint = ("Conexão desativada nesta sessão. Reabra o aplicativo sem --offline para conectar ao MT5."
                     if self._offline_mode() else
                     "Abre o MetaTrader 5 com a janela organizada como você escolheu em Arquivo > "
                     "Configurações (janelas laterais, barras de ferramentas, perfil de gráficos e "
                     "disposição do painel). Se ele já estiver aberto, pergunta antes de fechar e abrir de novo.")
        Tooltip(self.mt5_view_button, view_hint, self.theme)
        if self._offline_mode():
            Tooltip(self.check_button, view_hint, self.theme)
        # Privilégios de administrador só existem (e só importam) no Windows
        if os.name == "nt":
            self.mt5_admin_button = ttk.Button(header, text="Iniciar MT5 como administrador",
                                               command=self.launch_mt5_as_admin)
            self.mt5_admin_button.pack(side=tk.RIGHT, padx=(0, 8))
            if self._offline_mode():
                Tooltip(self.mt5_admin_button, view_hint, self.theme)
        # Mostra a conta e o servidor em uso (ex.: "Conectado: conta 123 em MetaQuotes-Demo (demonstração)")
        self.mt5_status_value = ttk.Label(header, text="Verificando...")
        self.mt5_status_value.pack(side=tk.RIGHT, padx=(6, 12))
        self.mt5_status_dot = ttk.Label(header, text="●")
        self.mt5_status_dot.pack(side=tk.RIGHT)
        # Por último: em janela estreita é o subtítulo que encolhe, não a conta nem os botões
        ttk.Label(header, text="monte, mantenha e estude a sua base de dados do MetaTrader 5",
                  style="Page.Muted.TLabel").pack(side=tk.LEFT, padx=(12, 0), pady=(6, 0))

        # Barra de status
        status_bar = ttk.Frame(outer)
        status_bar.pack(fill=tk.X, side=tk.BOTTOM, pady=(8, 0))
        self.status_label = ttk.Label(status_bar, text="Status: Iniciando...", style="Page.Muted.TLabel")
        self.status_label.pack(side=tk.LEFT)
        self.base_label = ttk.Label(status_bar, text="", style="Page.Muted.TLabel")
        self.base_label.pack(side=tk.RIGHT)

        self.notebook = ttk.Notebook(outer)
        self.notebook.pack(fill=tk.BOTH, expand=True)
        symbols_tab = ttk.Frame(self.notebook, padding=12)
        self.notebook.add(symbols_tab, text="Ativos")
        self.inventory_panel = InventoryPanel(self.notebook, self)
        self.notebook.add(self.inventory_panel.frame, text="Minha base")
        self.analysis_panel = AnalysisPanel(self.notebook, self)
        self.notebook.add(self.analysis_panel.frame, text="Análise")
        self.robots_panel = RobotsPanel(self.notebook, self)
        self.notebook.add(self.robots_panel.frame, text="Robôs")
        self.laboratory_panel = LaboratoryPanel(self.notebook, self)
        self.notebook.add(self.laboratory_panel.frame, text="Laboratório")
        # Laboratório só lê a base na primeira vez que a aba é aberta
        self.notebook.bind("<<NotebookTabChanged>>", lambda e: self.notebook.select() == str(
            self.laboratory_panel.frame) and self.laboratory_panel.on_show(), add="+")
        self.resources_panel = ResourcesPanel(self.notebook, self)
        self.notebook.add(self.resources_panel.frame, text="Recursos")
        log_tab = ttk.Frame(self.notebook, padding=12)
        self.log_tab = log_tab
        self.notebook.add(log_tab, text="Atividade")
        # Recursos só lê a base na primeira vez que a aba é aberta
        self.notebook.bind("<<NotebookTabChanged>>", lambda e: self.notebook.select() == str(
            self.resources_panel.frame) and self.resources_panel.on_show(), add="+")

        self._build_symbols_tab(symbols_tab)
        self._build_log_tab(log_tab)

        # Restaura a seleção da última vez
        for symbol in self.prefs.get("selected", []):
            if symbol not in self.app.selected_symbols:
                self.app.selected_symbols.append(symbol)
        self._refresh_selected_list()

        self.log("Interface gráfica configurada.")
        self._sync_favorites()
        self.filter_symbols()
        self.refresh_inventory()
        self.robots_panel.reload()

        # Menu (Arquivo, Exibir, Estratégias, Ajuda)
        self.create_menu()
        self.theme.on_change(self._theme_changed)
        self.root.bind("<Control-f>", lambda e: self.search_entry.focus_set())
        self.root.bind("<F5>", lambda e: self.refresh_inventory())

        # A janela aparece primeiro; o MT5 é aberto (já organizado) e conectado em segundo plano
        self.refresh_mt5_status()
        if not self._offline_mode() and not self._mt5_connected() and hasattr(self.app.mt5_connector, "initialize"):
            self.root.after(200, lambda: self.connect_mt5(startup=True))

    def _build_symbols_tab(self, tab):
        tab.rowconfigure(0, weight=1)
        self.symbols_canvas = tk.Canvas(tab, highlightthickness=0, borderwidth=0,
                                        background=self.theme.colors["page"])
        self.symbols_scrollbar = ttk.Scrollbar(tab, orient="vertical", command=self.symbols_canvas.yview)
        self.symbols_canvas.configure(yscrollcommand=self.symbols_scrollbar.set)
        self.symbols_canvas.pack(fill=tk.BOTH, expand=True)
        content = ttk.Frame(self.symbols_canvas)
        self._symbols_content = content
        self._symbols_window = self.symbols_canvas.create_window(0, 0, window=content, anchor="nw")
        content.bind("<Configure>", self._sync_symbols_viewport, add="+")
        self.symbols_canvas.bind("<Configure>", self._sync_symbols_viewport, add="+")
        for sequence, direction in (("<MouseWheel>", "wheel"), ("<Button-4>", "up"), ("<Button-5>", "down")):
            self.symbols_canvas.bind(sequence, lambda event, d=direction: self._scroll_symbols(event, d), add="+")

        # Esquerda: ativos da conta
        content.columnconfigure(0, weight=2, uniform="col")
        content.columnconfigure(1, weight=1, uniform="col")
        content.rowconfigure(0, weight=1)
        left = card(content, "Ativos da conta")
        left.master.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        self._symbols_left_outer = left.master
        self.symbols_label = ttk.Label(left, text="", style="Muted.TLabel")
        self.symbols_label.pack(anchor="w", pady=(0, 8))

        search_frame = FilterBar(left, padding=6)
        search_frame.pack(fill=tk.X, pady=(0, 8))
        ttk.Label(search_frame, text="Buscar", style="Muted.TLabel").pack(side=tk.LEFT, padx=(0, 6))
        self.search_var.trace_add("write", self._schedule_filter)
        self.search_entry = ttk.Entry(search_frame, textvariable=self.search_var)
        self.search_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self.search_entry.focus_set()
        Tooltip(self.search_entry, "Digite parte do código (ex.: WIN, PETR, EUR). Atalho: Ctrl+F.", self.theme)
        for text, value, tip in (("Todos", "all", "Todos os ativos que a conta oferece."),
                                 ("★ Favoritos", "fav", "Só os ativos que você marcou como favoritos."),
                                 ("Na base", "data", "Só os ativos que já têm dados extraídos.")):
            radio = ttk.Radiobutton(search_frame, text=self.theme.glyphs(text), value=value, variable=self.filter_var,
                                    style="Toolbutton", command=self.filter_symbols)
            radio.pack(side=tk.LEFT, padx=(8 if value == "all" else 1, 0))
            Tooltip(radio, tip, self.theme)

        actions = ttk.Frame(left, style="Surface.TFrame")
        actions.pack(fill=tk.X, side=tk.BOTTOM, pady=(10, 0))
        ttk.Button(actions, text="Adicionar à seleção →", command=self.add_symbols).pack(side=tk.LEFT)
        ttk.Button(actions, text=self.theme.glyphs("★ Favoritar / desfavoritar"), command=self.toggle_favorites).pack(
            side=tk.LEFT, padx=(8, 0))
        ttk.Label(actions, text="Duplo clique adiciona à seleção", style="Muted.TLabel").pack(side=tk.RIGHT)

        quick = ttk.Frame(left, style="Surface.TFrame")
        quick.pack(fill=tk.X, pady=(0, 8))
        ttk.Button(quick, text=self.theme.glyphs("☑ Selecionar Todos"), command=self.select_all_visible).pack(
            side=tk.LEFT)
        ttk.Button(quick, text=self.theme.glyphs("☐ Deselecionar Todos"), command=self.clear_selection).pack(
            side=tk.LEFT, padx=(8, 0))
        self.selected_count_label = ttk.Label(quick, text="", style="Muted.TLabel")
        self.selected_count_label.pack(side=tk.LEFT, padx=(12, 0))

        tree_frame, self.symbols_tree = scrolled_tree(
            left, SYMBOL_COLUMNS, (26, 78, 70, 88, 135, 65, 108, 58),
            ("center", "w", "e", "w", "w", "w", "w", "center"), height=18, selectmode="extended", xscroll=True)
        self.symbols_tree.column(asset_table.COL_FAVORITE, stretch=False)
        self.symbols_tree.heading(asset_table.COL_FAVORITE, text=self.theme.glyphs(asset_table.COL_FAVORITE))
        tree_frame.pack(fill=tk.BOTH, expand=True)
        for col in SYMBOL_COLUMNS:
            heading = {asset_table.COL_VOLUME: "Vol.", asset_table.COL_PROFIT: "Robô +",
                       asset_table.COL_STATUS: "Status", asset_table.COL_BENCHMARK: "Bench.",
                       asset_table.COL_DETAILS: "Na base", asset_table.COL_SELECTED: "Sel."}.get(col, col)
            self.symbols_tree.heading(col, text=self.theme.glyphs(heading),
                                      command=lambda c=col: self.sort_symbols(c))
        self.symbols_tree.bind("<<TreeviewSelect>>", self.on_symbol_select)
        self.symbols_tree.bind("<Button-1>", self._symbol_clicked)
        self.symbols_tree.bind("<Double-Button-1>", self._symbol_double_clicked)
        self.symbols_tree.bind("<Return>", lambda e: self.add_symbols())

        # Direita: seleção, detalhes e ações
        right = ttk.Frame(content)
        self._symbols_right_outer = right
        right.grid(row=0, column=1, sticky="nsew")
        right.columnconfigure(0, weight=1)
        right.rowconfigure(0, weight=1)

        selected = card(right, "Seleção", "ativos que serão extraídos ou coletados")
        selected.master.grid(row=0, column=0, sticky="nsew")
        selected_actions = ttk.Frame(selected, style="Surface.TFrame")
        selected_actions.pack(fill=tk.X, side=tk.BOTTOM, pady=(8, 0))
        selected_buttons = ttk.Frame(selected_actions, style="Surface.TFrame")
        selected_buttons.pack(fill=tk.X)
        ttk.Button(selected_buttons, text="Remover", command=self.remove_symbols).pack(side=tk.LEFT)
        ttk.Button(selected_buttons, text="Limpar", command=self.clear_selection).pack(side=tk.LEFT, padx=(8, 0))
        self.selection_label = ttk.Label(selected_actions, text="", style="Muted.TLabel")
        self.selection_label.pack(anchor="e", pady=(4, 0))
        self.selected_listbox = self.theme.classic(tk.Listbox(selected, selectmode=tk.EXTENDED, height=5,
                                                              exportselection=False, activestyle="none"))
        self.selected_listbox.pack(fill=tk.BOTH, expand=True)
        self.selected_listbox.bind('<<ListboxSelect>>', self.on_selected_symbol_select)
        self.selected_listbox.bind('<Delete>', lambda e: self.remove_symbols())

        details = card(right, "Detalhes do ativo")
        details.master.grid(row=1, column=0, sticky="ew", pady=10)
        self.symbol_details_frame = details.master
        self.detail_title = ttk.Label(details, text="Selecione um ativo para ver os detalhes", style="Surface.TLabel")
        self.detail_title.pack(anchor="w")
        grid = ttk.Frame(details, style="Surface.TFrame")
        grid.pack(fill=tk.X, pady=(6, 0))
        grid.columnconfigure(1, weight=1)
        self.detail_values = {}
        for row, (key, label, tip) in enumerate((
                ("price", "Cotação", "Último negócio (ou a oferta de compra, em Forex/CFD) informado pelo MetaTrader 5."),
                ("spread", "Spread", study.GLOSSARY["Spread médio"]),
                ("base", "Na base", "Séries já extraídas deste ativo, com o período coberto."))):
            name = ttk.Label(grid, text=label, style="Muted.TLabel")
            name.grid(row=row, column=0, sticky="nw", padx=(0, 12), pady=2)
            value = ttk.Label(grid, text="—", style="Surface.TLabel", justify=tk.LEFT, wraplength=420)
            value.grid(row=row, column=1, sticky="w", pady=2)
            self.detail_values[key] = value
            Tooltip(name, tip, self.theme)
        # Compatibilidade: código antigo escreve o texto dos detalhes neste rótulo
        self.symbol_details_content = self.detail_values["base"]

        control = card(right, "O que fazer com a seleção")
        control.master.grid(row=2, column=0, sticky="ew")
        row1 = ttk.Frame(control, style="Surface.TFrame")
        row1.pack(fill=tk.X)
        self.get_history_button = ttk.Button(row1, text="Extrair histórico...", style="Accent.TButton",
                                             command=self.extract_historical_data)
        self.get_history_button.pack(side=tk.LEFT)
        Tooltip(self.get_history_button, "Baixa as barras do período escolhido (ou só o que falta desde a última "
                                         "extração) e grava na base. É o primeiro passo para estudar um ativo.",
                self.theme)
        self.show_stats_button = ttk.Button(row1, text="Analisar", command=self.show_statistics)
        self.show_stats_button.pack(side=tk.LEFT, padx=(8, 0))
        Tooltip(self.show_stats_button, "Abre o gráfico de candles, os indicadores e os números de estudo do ativo "
                                        "(precisa ter dados na base).", self.theme)
        row2 = ttk.Frame(control, style="Surface.TFrame")
        row2.pack(fill=tk.X, pady=(8, 0))
        self.start_button = ttk.Button(row2, text="Iniciar coleta em tempo real", command=self.start_collection)
        self.start_button.pack(side=tk.LEFT)
        Tooltip(self.start_button, "Grava a barra de 1 minuto mais recente de cada ativo da seleção enquanto o "
                                   "aplicativo estiver aberto.", self.theme)
        self.stop_button = ttk.Button(row2, text="Parar coleta", command=self.stop_collection, state=tk.DISABLED)
        self.stop_button.pack(side=tk.LEFT, padx=(8, 0))

        # Progresso da coleta em tempo real (aparece só durante a coleta)
        self.progress_frame = ttk.Frame(control, style="Surface.TFrame")
        self.progress_var = tk.DoubleVar(value=0)
        self.progress_bar = ttk.Progressbar(self.progress_frame, variable=self.progress_var,
                                            style="Collection.Horizontal.TProgressbar", mode='determinate')
        self.progress_bar.pack(fill=tk.X, pady=(10, 4))
        info = ttk.Frame(self.progress_frame, style="Surface.TFrame")
        info.pack(fill=tk.X)
        self.collection_time_label = ttk.Label(info, text="Tempo: 00:00:00", style="Muted.TLabel")
        self.collection_time_label.grid(row=0, column=0, sticky="w", padx=(0, 14))
        self.collection_count_label = ttk.Label(info, text="Registros: 0", style="Muted.TLabel")
        self.collection_count_label.grid(row=0, column=1, sticky="w", padx=(0, 14))
        self.collection_errors_label = ttk.Label(info, text="Erros: 0", style="Muted.TLabel")
        self.collection_errors_label.grid(row=0, column=2, sticky="w")
        self.last_symbol_label = ttk.Label(info, text="Último símbolo: -", style="Muted.TLabel")
        self.last_symbol_label.grid(row=1, column=0, sticky="w", padx=(0, 14))
        self.last_time_label = ttk.Label(info, text="Último registro: -", style="Muted.TLabel")
        self.last_time_label.grid(row=1, column=1, columnspan=2, sticky="w")
        self.symbols_progress_frame = ttk.Frame(self.progress_frame, style="Surface.TFrame")
        self.symbols_progress_frame.pack(fill=tk.X, pady=(4, 0))
        self.symbols_progress_frame.columnconfigure(0, weight=1)
        self._symbols_stacked = None
        tab.bind("<Configure>", self._responsive_symbols_tab, add="+")

    def _responsive_symbols_tab(self, event):
        stacked = event.width < 1050
        if stacked == self._symbols_stacked:
            return
        self._symbols_stacked = stacked
        if stacked:
            self._symbols_content.columnconfigure(0, weight=1, uniform="")
            self._symbols_content.columnconfigure(1, weight=0, uniform="")
            self._symbols_content.rowconfigure(0, weight=2)
            self._symbols_content.rowconfigure(1, weight=3)
            self._symbols_left_outer.grid_configure(row=0, column=0, columnspan=2, sticky="nsew",
                                                    padx=0, pady=(0, 8))
            self._symbols_right_outer.grid_configure(row=1, column=0, columnspan=2, sticky="nsew")
        else:
            self._symbols_content.columnconfigure(0, weight=2, uniform="col")
            self._symbols_content.columnconfigure(1, weight=1, uniform="col")
            self._symbols_content.rowconfigure(0, weight=1)
            self._symbols_content.rowconfigure(1, weight=0)
            self._symbols_left_outer.grid_configure(row=0, column=0, columnspan=1, sticky="nsew",
                                                    padx=(0, 10), pady=0)
            self._symbols_right_outer.grid_configure(row=0, column=1, columnspan=1, sticky="nsew")

    def _sync_symbols_viewport(self, _event=None):
        canvas_height = self.symbols_canvas.winfo_height()
        canvas_width = self.symbols_canvas.winfo_width()
        content_height = self._symbols_content.winfo_reqheight()
        if canvas_height <= 1 or canvas_width <= 1:
            return
        needs_scroll = self._symbols_stacked and content_height > canvas_height + 4
        height = max(canvas_height, content_height) if needs_scroll else canvas_height
        self.symbols_canvas.itemconfigure(self._symbols_window, width=canvas_width, height=height)
        self.symbols_canvas.configure(scrollregion=(0, 0, canvas_width, height))
        if needs_scroll:
            if not self.symbols_scrollbar.winfo_manager():
                self.symbols_scrollbar.pack(side=tk.RIGHT, fill=tk.Y, before=self.symbols_canvas)
        elif self.symbols_scrollbar.winfo_manager():
            self.symbols_scrollbar.pack_forget()

    def _scroll_symbols(self, event, direction):
        if direction == "up":
            delta = -1
        elif direction == "down":
            delta = 1
        else:
            delta = -1 if event.delta > 0 else 1
        self.symbols_canvas.yview_scroll(delta, "units")
        return "break"

    def _build_log_tab(self, tab):
        frame = ttk.Frame(tab, padding=12)
        frame.pack(fill=tk.BOTH, expand=True)
        header = PageHeader(frame, "Atividade", "O que o aplicativo fez nesta sessão")
        header.pack(fill=tk.X, pady=(0, 8))
        header.add_action("Limpar", lambda: self.log_text.delete("1.0", tk.END))
        header.add_action("Abrir pasta de logs",
                          lambda: self._open_path(Path(self._project_root()) / "logs"))
        panel = ContentPanel(frame, title="Registro da sessão", subtitle="Mensagens desta execução", padding=10)
        panel.pack(fill=tk.BOTH, expand=True)
        body = panel.body
        log_scroll = ttk.Scrollbar(body)
        log_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.log_text = self.theme.classic(tk.Text(body, height=10, wrap=tk.WORD, padx=10, pady=8,
                                                   yscrollcommand=log_scroll.set))
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        log_scroll.config(command=self.log_text.yview)

    @staticmethod
    def _project_root():
        return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    def _theme_changed(self):
        self.create_menu()
        self.symbols_canvas.configure(background=self.theme.colors["page"])
        self.symbols_tree.tag_configure("data", foreground=self.theme.colors["good"])
        self.refresh_mt5_status(quiet=True)
        self.save_prefs()

    def toggle_theme(self):
        self.theme.toggle()

    # --- inventário da base -----------------------------------------------------------

    def refresh_inventory(self):
        """Lê (em segundo plano) o que existe na base e atualiza as abas."""
        db = self.app.db_manager
        if not db or not db.is_connected():
            log.warning("Não foi possível ler a base: banco de dados não conectado")
            self.base_label.config(text="Base de dados indisponível")
            return
        def work():
            from mt5_extracao.robots import store
            inventory = db.data_inventory()
            extra = {}
            try:        # as colunas de volume e de robôs são um complemento: sem elas a base ainda aparece
                extra = {"volumes": asset_table.volumes(db, inventory),
                         "cells": asset_table.profit_cells(store.load_runs(db)),
                         "prefs": asset_table.load_prefs(db)}
            except Exception as e:
                log.error(f"Não foi possível ler volume, backtests ou benchmark da base: {e}")
            return {"inventory": inventory, **extra}
        self.run_bg(work, self._inventory_loaded)

    # Nome antigo, ainda chamado ao fim de uma extração
    load_existing_symbols_data = refresh_inventory

    def _inventory_loaded(self, loaded, error):
        if error:
            self.log(f"Não foi possível ler a base de dados: {error}")
            return
        inventory = loaded["inventory"]
        if "cells" in loaded:
            self.volumes, self.profit_cells = loaded["volumes"], loaded["cells"]
            if loaded["prefs"] is not None:
                self.benchmarks = loaded["prefs"]["benchmarks"]
        self.inventory = inventory
        self.symbol_series = {}
        for item in inventory:
            if item["rows"]:
                self.symbol_series.setdefault(item["symbol"], []).append(item)
        for series in self.symbol_series.values():
            series.sort(key=lambda item: getattr(timeframes.parse(item["timeframe"]), "minutes", 0))
        self.symbols_with_data = set(self.symbol_series)
        self.existing_symbols_map = {item["symbol"]: item["table"] for item in inventory
                                     if item["rows"] and item["timeframe"] == timeframes.Timeframe.M1.label}
        self.inventory_panel.show(inventory)
        self.analysis_panel.set_inventory(inventory)
        self.robots_panel.detail.inventory_changed()
        self.filter_symbols()
        total = sum(item["rows"] for item in inventory)
        self.base_label.config(text=f"Base: {len(self.symbols_with_data)} ativo(s), {fmt_int(total)} barras"
                               if inventory else "Base vazia: extraia o histórico de um ativo para começar")
        if self.detail_symbol:
            self.update_symbol_details(self.detail_symbol)

    def update_symbols_with_data(self, symbols_with_data=None):
        """Mantido por compatibilidade com app.py: a interface lê o inventário em refresh_inventory()."""

    # --- lista de ativos --------------------------------------------------------------

    def _schedule_filter(self, *args):
        """A busca espera uma pausa na digitação (a conta pode ter milhares de símbolos)."""
        if self._filter_job:
            self.root.after_cancel(self._filter_job)
        self._filter_job = self.root.after(180, self.filter_symbols)

    def filter_symbols(self, *args):
        """Filtra a lista de símbolos com base no texto de busca e no filtro (todos/favoritos/na base)."""
        self._filter_job = None
        tree = self.symbols_tree
        if not tree: # Verifica se o widget existe
            return
        account = list(dict.fromkeys(getattr(self.app, 'symbols', None) or []))
        mode = self.filter_var.get()
        universe = account
        if mode == "data":  # inclui ativos extraídos em outra conta
            universe = account + sorted(self.symbols_with_data - set(account))
        elif mode == "fav":
            universe = [sym for sym in account if sym in self.favorite_symbols]
        search_text = self.search_var.get().strip().lower()
        matches = [sym for sym in universe if (not search_text or search_text in sym.lower())
                   and (mode != "data" or sym in self.symbols_with_data)]
        # Favoritos no topo; depois, por padrão, os de maior volume (ou a coluna clicada no cabeçalho)
        rows = asset_table.sort_rows(asset_table.build_rows(
            ((sym, sym, self.symbol_series.get(sym, [])) for sym in dict.fromkeys(matches)), self.favorite_symbols,
            self.volumes, self.profit_cells, self.benchmarks, self.app.selected_symbols), *self._symbol_sort)

        tree.delete(*tree.get_children())
        tree.tag_configure("data", foreground=self.theme.colors["good"])
        for row in rows.itertuples():
            tree.insert("", tk.END, iid=row.key, tags=("data",) if row.key in self.symbols_with_data else (),
                        values=self.cell_texts(row))

        total = len(account)
        if not total:
            text = "Nenhum ativo disponível: clique em \"Verificar conexão\" para conectar ao MetaTrader 5."
        elif search_text and not matches:
            text = f"Nenhum ativo contém \"{self.search_var.get().strip()}\" ({total} na conta)."
        elif len(matches) != total:
            text = f"{len(matches)} de {total} ativos. Em verde: já têm dados na base."
        else:
            text = f"{total} ativos na conta. Em verde: já têm dados na base."
        self.symbols_label.config(text=text)

    def highlight_symbols_with_data(self):
        """Redesenha a lista (favoritos, dados na base e seleção são marcados em filter_symbols)."""
        self.filter_symbols()

    def cell_texts(self, row):
        """Os textos de uma linha das tabelas de ativos, como este Tk consegue desenhar (ver Theme.glyphs)."""
        return tuple(self.theme.glyphs(text) for text in asset_table.display(row))

    def sort_symbols(self, column):
        """Clique no cabeçalho: ordena por essa coluna (segundo clique inverte). Favoritos seguem no topo."""
        self._symbol_sort = (column, not self._symbol_sort[1] if self._symbol_sort[0] == column else False)
        self.filter_symbols()

    def _symbol_column_at(self, x):
        index = int(self.symbols_tree.identify_column(x).lstrip("#") or 0) - 1
        return SYMBOL_COLUMNS[index] if 0 <= index < len(SYMBOL_COLUMNS) else None

    def _symbol_clicked(self, event):
        """Clique em ★ alterna o favorito; clique na caixa da coluna Seleção põe ou tira o ativo da seleção."""
        tree = self.symbols_tree
        symbol = tree.identify_row(event.y)
        if tree.identify_region(event.x, event.y) != "cell" or not symbol:
            return None
        column = self._symbol_column_at(event.x)
        if column == asset_table.COL_FAVORITE:
            self.toggle_favorite(symbol)
            return "break"
        if column == asset_table.COL_SELECTED:
            if symbol in self.app.selected_symbols:
                self.unselect_symbols([symbol])
            else:
                self.select_symbols([symbol])
            return "break"
        return None

    def _symbol_double_clicked(self, event):
        if self._symbol_column_at(event.x) in (asset_table.COL_FAVORITE, asset_table.COL_SELECTED):
            return "break"
        self.add_symbols()
        return None

    def _tree_symbols(self):
        return list(self.symbols_tree.selection()) if self.symbols_tree else []

    def on_symbol_select(self, event=None):
        """Manipula o evento de seleção na lista de símbolos disponíveis."""
        symbols = self._tree_symbols()
        if symbols:
            self.update_symbol_details(symbols[0])

    def on_selected_symbol_select(self, event=None):
        """Callback quando um símbolo é selecionado na lista de selecionados."""
        selection = self.selected_listbox.curselection()
        if selection:
            self.update_symbol_details(self.selected_listbox.get(selection[0]))

    def update_symbol_details(self, symbol):
        """Atualiza o painel de detalhes com informações do símbolo selecionado."""
        if not symbol:
            return
        self.detail_symbol = symbol
        self.detail_title.config(text=symbol, style="Heading.TLabel")
        series = self.symbol_series.get(symbol, [])
        if series:
            lines = []
            for item in series:
                tf = timeframes.parse(item["timeframe"])
                lines.append(f"{item['timeframe']}: {fmt_int(item['rows'])} barras, de {item['start']:%d/%m/%Y} a "
                             f"{item['end']:%d/%m/%Y} ({study.freshness(item['end'], tf.minutes if tf else 1)})")
            self.detail_values["base"].config(text="\n".join(lines))
        else:
            self.detail_values["base"].config(text="Ainda sem dados. Adicione à seleção e clique em "
                                                   "\"Extrair histórico\".")
        if not self._mt5_connected():
            self.detail_values["price"].config(text="MetaTrader 5 não conectado")
            self.detail_values["spread"].config(text="—")
            return
        self.detail_values["price"].config(text="...")
        self.detail_values["spread"].config(text="...")

        def done(quote, error):
            if self.detail_symbol != symbol:
                return
            price, spread = quote if quote else ("indisponível", "—")
            self.detail_values["price"].config(text=price)
            self.detail_values["spread"].config(text=spread)

        self.run_bg(lambda: (self.get_symbol_price(symbol), self.get_symbol_spread(symbol)), done)

    def get_symbol_spread(self, symbol):
        """Obtém o spread atual do símbolo via MT5."""
        if not self._mt5_connected():
            return "N/A (MT5 não conectado)"
        try:
            symbol_info = self.app.mt5_connector.get_symbol_info(symbol)
            if symbol_info and hasattr(symbol_info, 'spread'):
                return f"{symbol_info.spread} pontos"
            return "N/A"
        except Exception as e:
            log.error(f"Erro ao obter spread para {symbol}: {e}")
            return "Erro ao obter spread"

    def get_symbol_price(self, symbol):
        """Obtém a cotação atual do símbolo via MT5."""
        if not self._mt5_connected():
            return "N/A (MT5 não conectado)"
        try:
            symbol_info = self.app.mt5_connector.get_symbol_info(symbol)
            if not symbol_info:
                return "N/A"
            # Forex/CFD não têm "último negócio": nesses casos vale a oferta de compra (bid)
            price = getattr(symbol_info, 'last', 0) or getattr(symbol_info, 'bid', 0)
            if not price:
                return "sem cotação no momento"
            return f"{price:.{getattr(symbol_info, 'digits', 5)}f}"
        except Exception as e:
            log.error(f"Erro ao obter cotação para {symbol}: {e}")
            return "Erro ao obter cotação"

    # --- seleção ----------------------------------------------------------------------

    def _refresh_selected_list(self):
        self.selected_listbox.delete(0, tk.END)
        if self.app.selected_symbols:
            self.selected_listbox.insert(tk.END, *self.app.selected_symbols)
        count = len(self.app.selected_symbols)
        self.selection_label.config(text=f"{count} ativo(s)" if count else "nenhum ativo selecionado")
        self.selected_count_label.config(text=f"{count} ativo(s) selecionado(s)")
        chosen = set(self.app.selected_symbols)
        for sym in self.symbols_tree.get_children():
            self.symbols_tree.set(sym, asset_table.COL_SELECTED, self.theme.glyphs(
                asset_table.CHECK_ON if sym in chosen else asset_table.CHECK_OFF))

    def select_symbols(self, symbols):
        """Acrescenta ativos à seleção (sem repetir). Retorna quantos entraram."""
        added = [s for s in dict.fromkeys(symbols) if s not in self.app.selected_symbols]
        if added:
            self.app.selected_symbols.extend(added)
            self._refresh_selected_list()
            self.save_prefs()
            self.log(f"{len(added)} ativo(s) adicionado(s) à seleção: {', '.join(added[:8])}"
                     + ("..." if len(added) > 8 else ""))
        return len(added)

    def add_symbols(self):
        """Adiciona símbolos selecionados à lista de monitoramento"""
        symbols = self._tree_symbols()
        if not symbols:
            self.update_status("Clique em um ativo da lista antes de adicionar à seleção.")
            return
        if not self.select_symbols(symbols):
            self.update_status("Os ativos escolhidos já estão na seleção.")

    def select_all_visible(self):
        """Marca todas as linhas que estão na lista (respeita a busca e o filtro)."""
        if not self.select_symbols(self.symbols_tree.get_children()):
            self.update_status("Todos os ativos da lista já estão na seleção.")

    def unselect_symbols(self, removed):
        """Tira ativos da seleção."""
        removed = [s for s in removed if s in self.app.selected_symbols]
        if not removed:
            return
        self.app.selected_symbols[:] = [s for s in self.app.selected_symbols if s not in removed]
        self._refresh_selected_list()
        self.save_prefs()
        self.log(f"Removido(s) da seleção: {', '.join(removed[:8])}" + ("..." if len(removed) > 8 else ""))

    def remove_symbols(self):
        """Remove símbolos da lista de monitoramento"""
        if not self.selected_listbox:
            return
        self.unselect_symbols([self.selected_listbox.get(i) for i in self.selected_listbox.curselection()])

    def clear_selection(self):
        if self.app.selected_symbols:
            self.app.selected_symbols[:] = []
            self._refresh_selected_list()
            self.save_prefs()

    # --- favoritos --------------------------------------------------------------------

    def add_to_favorites(self):
        """Adiciona os símbolos selecionados aos favoritos"""
        symbols = self._tree_symbols()
        if not symbols:
            messagebox.showinfo("Favoritos", "Selecione pelo menos um símbolo para adicionar aos favoritos.")
            return
        added = [s for s in symbols if s not in self.favorite_symbols]
        if added:
            self.favorite_symbols.extend(added)
            self.log(f"{len(added)} símbolo(s) adicionado(s) aos favoritos.")
            self._favorites_changed()

    def remove_from_favorites(self):
        """Remove os símbolos selecionados dos favoritos"""
        symbols = self._tree_symbols()
        if not symbols:
            messagebox.showinfo("Favoritos", "Selecione pelo menos um símbolo para remover dos favoritos.")
            return
        removed = [s for s in symbols if s in self.favorite_symbols]
        if removed:
            self.favorite_symbols[:] = [s for s in self.favorite_symbols if s not in removed]
            self.log(f"{len(removed)} símbolo(s) removido(s) dos favoritos.")
            self._favorites_changed()

    def _favorites_changed(self):
        """Grava os favoritos e redesenha as duas tabelas, mantendo a posição da rolagem."""
        self.save_favorites()
        top = self.symbols_tree.yview()[0]
        self.filter_symbols()
        self.symbols_tree.yview_moveto(top)
        self.inventory_panel.show(self.inventory)

    def toggle_favorite(self, symbol):
        """Clique na estrela: favorita ou desfavorita um ativo."""
        if symbol in self.favorite_symbols:
            self.favorite_symbols[:] = [s for s in self.favorite_symbols if s != symbol]
        else:
            self.favorite_symbols.append(symbol)
        self._favorites_changed()

    def set_benchmark(self, symbol, benchmark):
        """Vincula (ou, em branco, desvincula) o índice de referência do ativo e atualiza as tabelas."""
        db = self._prefs_db()
        if db is None:
            messagebox.showerror("Benchmark", "Banco de dados não disponível: não é possível gravar o benchmark.")
            return
        try:
            asset_table.set_benchmark(db, symbol, benchmark)
        except Exception as e:
            log.error(f"Erro ao gravar o benchmark de {symbol}: {e}")
            messagebox.showerror("Benchmark", f"Não foi possível gravar o benchmark de {symbol}:\n{e}")
            return
        if benchmark:
            self.benchmarks[symbol] = benchmark
        else:
            self.benchmarks.pop(symbol, None)
        self.log(f"Benchmark de {symbol}: {benchmark or 'nenhum'}.")
        self.filter_symbols()
        self.inventory_panel.show(self.inventory)

    def backtest_recorded(self, run):
        """Um backtest acabou de ser gravado: a coluna Robô Lucrativo do ativo muda na hora, nas duas abas."""
        asset_table.add_run(self.profit_cells, run)
        symbol = run["symbol"]
        if self.symbols_tree.exists(symbol):
            self.symbols_tree.set(symbol, asset_table.COL_PROFIT, self.theme.glyphs(
                asset_table.fmt_profit(asset_table.profit_of(self.profit_cells, symbol))))
        self.inventory_panel.profit_changed(symbol)

    def toggle_favorites(self):
        """Favorita os ativos marcados; se todos já são favoritos, desfavorita."""
        symbols = self._tree_symbols()
        if not symbols:
            self.update_status("Clique em um ativo da lista para favoritar.")
        elif all(s in self.favorite_symbols for s in symbols):
            self.remove_from_favorites()
        else:
            self.add_to_favorites()

    def show_only_favorites(self):
        """Mostra apenas os símbolos marcados como favoritos"""
        self.filter_var.set("fav")
        self.filter_symbols()

    def show_all_symbols(self):
        """Mostra todos os símbolos, favoritos primeiro"""
        self.filter_var.set("all")
        self.search_var.set("")
        self.filter_symbols()

    # --- análise e exportação ---------------------------------------------------------

    def open_analysis(self, symbol, timeframe_label=None):
        """Abre a aba Análise no ativo pedido (avisa se ele ainda não tem dados)."""
        if not self.analysis_panel.open(symbol, timeframe_label):
            messagebox.showinfo(
                "Análise",
                f"{symbol} ainda não tem dados na base.\n\nAdicione o ativo à seleção e clique em "
                "\"Extrair histórico\"; depois a análise fica disponível.")
            return False
        self.notebook.select(self.analysis_panel.frame)
        return True

    def show_statistics(self):
        """Abre a análise do ativo em destaque (ou do primeiro da seleção)."""
        candidates = [self.detail_symbol] + list(self.app.selected_symbols)
        symbol = next((s for s in candidates if s in self.symbols_with_data), None) \
            or next((s for s in candidates if s), None)
        if symbol:
            self.open_analysis(symbol)
        else:
            self.notebook.select(self.analysis_panel.frame)

    def export_table(self, table_name, format_type, filters=None, on_success=None):
        """Pergunta onde salvar e exporta a tabela em segundo plano (csv, excel, parquet ou duckdb)."""
        ext = {"csv": "csv", "excel": "xlsx", "xlsx": "xlsx", "parquet": "parquet",
               "duckdb": "duckdb"}[format_type.lower()]
        file_path = filedialog.asksaveasfilename(
            defaultextension=f".{ext}", initialfile=f"{table_name}.{ext}",
            filetypes=[(f"{ext.upper()}", f"*.{ext}"), ("Todos os arquivos", "*.*")])
        if not file_path:
            return
        if not hasattr(self, 'data_exporter'):
            self.data_exporter = DataExporter(self.app.db_manager)
        exporter = self.data_exporter

        def work():
            if ext == "csv":
                return exporter.export_to_csv(table_name, caminho_arquivo=file_path, filtros=filters or None,
                                              adicionar_timestamp=False)
            if ext == "xlsx":
                return exporter.export_to_excel(table_name, caminho_arquivo=file_path, filtros=filters or None,
                                                adicionar_timestamp=False)
            if ext == "parquet":
                return exporter.export_to_parquet(table_name, caminho_arquivo=file_path, filtros=filters or None,
                                                  adicionar_timestamp=False)
            return exporter.export_to_duckdb([table_name], caminho_arquivo=file_path, filtros=filters or None)

        def done(result_path, error):
            if error:
                self.update_status("Status: falha na exportação")
                messagebox.showerror("Erro", f"Falha ao exportar: {error}")
            elif result_path:
                self.update_status(f"Status: exportado para {result_path}")
                self.log(f"Tabela {table_name} exportada para {result_path}")
                messagebox.showinfo("Sucesso", f"Dados exportados com sucesso para:\n{result_path}")
                if on_success:
                    on_success()
            else:
                messagebox.showwarning("Aviso", "Nenhum dado encontrado para exportar")

        self.update_status(f"Status: exportando {table_name}...")
        self.run_bg(work, done)

    def show_glossary(self):
        """Explica, em linguagem simples, os números e indicadores mostrados na aba Análise."""
        dialog = tk.Toplevel(self.root)
        dialog.title("Glossário")
        dialog.geometry("720x560")
        dialog.transient(self.root)
        text = self.theme.classic(scrolledtext.ScrolledText(dialog, wrap=tk.WORD, padx=14, pady=12))
        text.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        text.tag_configure("term", font=self.theme.font_bold, spacing1=10)
        for term, meaning in study.GLOSSARY.items():
            text.insert(tk.END, term + "\n", "term")
            text.insert(tk.END, meaning + "\n")
        text.config(state=tk.DISABLED)
        ttk.Button(dialog, text="Fechar", command=dialog.destroy).pack(pady=(0, 10))

    # --- coleta em tempo real ---------------------------------------------------------

    def update_collection_progress(self, total_collected, total_success, total_errors,
                                  elapsed_time, symbols_status, is_running):
        """
        Atualiza a interface com o progresso da coleta.

        Args:
            total_collected (int): Total de registros processados
            total_success (int): Total de registros salvos com sucesso
            total_errors (int): Total de erros ocorridos
            elapsed_time (float): Tempo decorrido em segundos
            symbols_status (dict): Status detalhado por símbolo
            is_running (bool): Se a coleta ainda está em andamento
        """
        if threading.current_thread() is not threading.main_thread():
            self.call_in_ui(self.update_collection_progress, total_collected, total_success, total_errors,
                            elapsed_time, symbols_status, is_running)
            return
        if not hasattr(self, 'progress_frame') or not hasattr(self, 'progress_var'):
            log.warning("Interface de progresso não inicializada.")
            return
        if not self.progress_frame.winfo_ismapped():
            self.progress_frame.pack(fill=tk.X)

        # Barra principal: taxa de registros gravados com sucesso
        if total_collected > 0:
            success_rate = (total_success / total_collected) * 100
            self.progress_var.set(success_rate)
            self.progress_bar.configure(style="Error.Horizontal.TProgressbar" if success_rate < 50
                                        else "Collection.Horizontal.TProgressbar")
        else:
            self.progress_var.set(0)

        hours, remainder = divmod(int(elapsed_time), 3600)
        minutes, seconds = divmod(remainder, 60)
        self.collection_time_label.config(text=f"Tempo: {hours:02d}:{minutes:02d}:{seconds:02d}")
        self.collection_count_label.config(text=f"Registros: {total_success} de {total_collected}")
        self.collection_errors_label.config(text=f"Erros: {total_errors}")

        last_symbol = "-"
        last_time = "-"
        for symbol, status in symbols_status.items():
            if status['last_time'] is not None:
                last_symbol = symbol
                if isinstance(status['last_time'], datetime.datetime):
                    last_time = status['last_time'].strftime("%H:%M:%S")
                elif isinstance(status['last_time'], str):
                    last_time = status['last_time']
        self.last_symbol_label.config(text=f"Último símbolo: {last_symbol}")
        self.last_time_label.config(text=f"Último registro: {last_time}")

        for widget in self.symbols_progress_frame.winfo_children():
            widget.destroy()
        row = 0
        for symbol, status in list(symbols_status.items())[:5]:  # Limita a 5 símbolos para não sobrecarregar a UI
            if status['total'] > 0:
                symbol_frame = ttk.Frame(self.symbols_progress_frame, style="Surface.TFrame")
                symbol_frame.grid(row=row, column=0, sticky="ew", pady=2)
                symbol_frame.columnconfigure(1, weight=1)
                ttk.Label(symbol_frame, text=f"{symbol}:", width=10, anchor="w", style="Muted.TLabel").grid(
                    row=0, column=0, padx=(0, 5))
                symbol_progress = ttk.Progressbar(symbol_frame, value=(status['success'] / status['total']) * 100)
                symbol_progress.grid(row=0, column=1, sticky="ew")
                status_text = f"{status['success']}/{status['total']}"
                if status['last_error']:
                    status_text += f" (Erro: {status['last_error']})"
                    symbol_progress.configure(style="Error.Horizontal.TProgressbar")
                ttk.Label(symbol_frame, text=status_text, anchor="e", style="Muted.TLabel").grid(
                    row=0, column=2, padx=(5, 0))
                row += 1

        if not is_running:
            self.progress_frame.pack_forget()
            self.refresh_inventory()

    # --- log e status -----------------------------------------------------------------

    def update_log_widget(self, message):
        """Atualiza o widget de log (text) com uma nova mensagem."""
        if threading.current_thread() is not threading.main_thread():
            self.call_in_ui(self.update_log_widget, message)
            return
        if self.log_text:
            timestamp = datetime.datetime.now().strftime("%H:%M:%S")
            self.log_text.insert(tk.END, f"[{timestamp}] {message}\n")
            # Manter o texto visível no final
            self.log_text.see(tk.END)
        else:
            log.warning(f"Tentativa de log na UI sem widget log_text: {message}")

    def log(self, message):
        """Atualiza o widget de log na UI com uma nova mensagem."""
        if threading.current_thread() is not threading.main_thread():
            self.call_in_ui(self.log, message)
            return
        # Registra no logger do sistema
        log.info(message)
        # Atualiza a UI
        self.update_log_widget(message)

    def update_status(self, status_text):
        """Atualiza o texto de status principal."""
        if threading.current_thread() is not threading.main_thread():
            self.call_in_ui(self.update_status, status_text)
            return
        # Sempre registrar o status no log
        log.info(f"Status: {status_text}")

        # Atualizar widget apenas se estiver disponível
        if hasattr(self, 'status_label') and self.status_label:
            try:
                self.status_label.config(text=status_text)
            except Exception as e:
                log.warning(f"Erro ao atualizar widget de status: {e}")
        else:
            # Apenas registrar para debug
            log.debug("Widget status_label não disponível para atualização.")

    def toggle_collection_buttons(self, collecting):
        """Habilita/desabilita os botões de iniciar/parar coleta."""
        if self.start_button and self.stop_button:
            if collecting:
                self.start_button.config(state=tk.DISABLED)
                self.stop_button.config(state=tk.NORMAL)
            else:
                self.start_button.config(state=tk.NORMAL)
                self.stop_button.config(state=tk.DISABLED)
        else:
            log.warning("Tentativa de alternar botões sem widgets start/stop_button.")

    def start_collection(self):
        """Inicia a coleta de dados (chamado pelo botão)."""
        # Obter e verificar os símbolos selecionados primeiro
        if not self.app.selected_symbols:
            messagebox.showerror("Erro", "Nenhum símbolo selecionado para coleta.")
            return

        # Verificar status real do MT5 independente da flag armazenada
        mt5_realmente_conectado = False
        if self.app.mt5_connector:
            # Verificar se o processo está rodando
            processo_rodando = self.app.mt5_connector._is_mt5_running()
            
            # Verificar se consegue acessar funcionalidades básicas do MT5
            symbols_acessiveis = False
            if processo_rodando and hasattr(self.app.mt5_connector, 'get_symbols_count'):
                try:
                    num_symbols = self.app.mt5_connector.get_symbols_count()
                    if num_symbols > 0:
                        symbols_acessiveis = True
                        log.info(f"Conexão MT5 confirmada: {num_symbols} símbolos acessíveis")
                except Exception as e:
                    log.warning(f"Erro ao verificar símbolos do MT5: {e}")
                    
            # Se processo existe mas não consegue acessar os símbolos, a conexão não está completa
            mt5_realmente_conectado = processo_rodando and symbols_acessiveis
            
            # Atualizar o estado na aplicação para refletir a realidade
            self.app.mt5_initialized = mt5_realmente_conectado
            if self.app.mt5_connector:
                self.app.mt5_connector.is_initialized = mt5_realmente_conectado
        
        # Se MT5 não está realmente conectado, tentar reconectar
        if not mt5_realmente_conectado:
            self.log("MT5 não está corretamente inicializado. Tentando reconectar...")
            
            # Perguntar ao usuário se deseja tentar reconectar
            response = messagebox.askyesno(
                "Reconexão MT5", 
                "O MT5 não está corretamente conectado para iniciar a coleta.\n\n"
                "Deseja tentar reconectar ao MT5 agora?",
            )
            
            if not response:
                return  # Usuário optou por não reconectar
            
            # Mostrar mensagem de processamento
            self.update_status("Status: Tentando reconectar ao MT5...")
            self.log("Iniciando tentativa de reconexão...")
            
            # Tentar conexão agressiva se disponível
            if hasattr(self.app.mt5_connector, 'force_connection_check'):
                log.info("Tentando forçar verificação de conexão...")
                reconnected = self.app.mt5_connector.force_connection_check(recursion_count=0)
            else:
                reconnected = False
            if reconnected:
                self.app.mt5_initialized = True
                self.log("Conexão reestabelecida com sucesso via verificação agressiva!")
                self.update_status("Status: MT5 conectado!")
            else:
                # Tentar método normal como fallback
                self.log("Verificação agressiva falhou. Tentando método normal...")
                reconnected = self.app.mt5_connector.initialize(force_restart=False, recursion_count=0)
                
                
            # Verificar resultado da tentativa de reconexão
            if reconnected or self.app.mt5_connector.is_initialized:
                self.app.mt5_initialized = True
                status = self.app.mt5_connector.get_connection_status()
                self.log(f"MT5 conectado com sucesso. Modo: {status.get('mode', 'N/A')}")
                self.update_status(f"Status: MT5 conectado. Modo: {status.get('mode', 'N/A')}")
            else:
                # Se falhar na reconexão, não permite continuar
                self.log("ERRO: Falha ao conectar ao MT5. Não é possível prosseguir.")
                self.update_status("Status: Falha na conexão com MT5")
                messagebox.showerror(
                    "Erro MT5", 
                    "Não foi possível estabelecer conexão com o MT5.\n"
                    "A coleta de dados não pode ser iniciada.\n\n"
                    "Dicas:\n"
                    "1. Verifique se o MT5 está em execução\n"
                    "2. Tente reiniciar o MT5 como administrador\n"
                    "3. Verifique se o MT5 não está bloqueado por alguma operação"
                )
                return
        
        # Atualiza UI
        self.toggle_collection_buttons(collecting=True)
        self.update_status("Status: Coletando dados...")
        self.log("Coleta de dados iniciada")

        # Chama a lógica de negócio na instância principal da app
        self.app.start_collection_logic()

    def stop_collection(self):
        """Para a coleta de dados (chamado pelo botão)."""
        # Atualiza UI
        self.toggle_collection_buttons(collecting=False)
        self.update_status("Status: Coleta parada")
        self.log("Coleta de dados interrompida")

        # Chama a lógica de negócio na instância principal da app
        self.app.stop_collection_logic()

    def handle_uncaught_exception(self, exc_type, exc_value, exc_traceback):
        """Trata exceções não capturadas na interface (chamado pelo root)."""
        error_msg = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
        log.error(f"Exceção não tratada na UI: {error_msg}") # Usa o logger do UIManager
        messagebox.showerror("Erro Inesperado",
                           f"Ocorreu um erro inesperado na interface:\n{str(exc_value)}\n\n"
                           "Consulte logs/mt5_extracao.log para mais detalhes.") # Aspas corrigidas
    def log_error(self, exception, message="Erro"):
        """Registra erro no log e exibe mensagem de erro na UI."""
        error_details = str(exception)
        log.error(f"{message} (UI): {error_details}") # Log específico da UI
        log.debug(traceback.format_exc()) # Mantém o traceback no log
        # Garante que a f-string está corretamente formatada e terminada
        messagebox.showerror(message, f"{message}:\n{error_details}")

    def extract_historical_data(self):
        """Abre a janela de configuração e inicia a extração de dados históricos."""
        # Verifica pré-condições (acessa atributos da app)
        if not self.app.mt5_initialized or not self.app.mt5_connector or not self.app.mt5_connector.is_initialized:
            # Verifica se podemos reconectar ao MT5
            response = messagebox.askyesno("MT5 Desconectado", "O MT5 está desconectado. Deseja tentar reconectar para continuar com a extração?")
            if response:
                # Avisa o usuário que irá tentar conectar ao MT5
                self.log("MT5 desconectado. Tentando reconectar...")
                self.update_status("Status: Reconectando ao MT5...")
                
                # Tenta inicializar o MT5
                if self.app.mt5_connector and self.app.mt5_connector.initialize(recursion_count=0):
                    self.app.mt5_initialized = True
                    status = self.app.mt5_connector.get_connection_status()
                    logging.info(f"Conexão MT5 reestabelecida. Modo: {status.get('mode', 'N/A')}")
                    self.log(f"MT5 conectado com sucesso. Modo: {status.get('mode', 'N/A')}")
                    self.update_status("Status: MT5 reconectado!")
                    messagebox.showinfo("Conexão MT5", "Conexão com MT5 estabelecida com sucesso. Continuando com a extração.")
                else:
                    # Se falhar na reconexão, não permite continuar
                    logging.error("Falha ao reconectar ao MT5. Não é possível prosseguir com a extração.")
                    self.log("ERRO: Falha ao reconectar ao MT5. Não é possível prosseguir.")
                    self.update_status("Status: Falha na conexão com MT5")
                    messagebox.showerror("Erro MT5", "Não foi possível estabelecer conexão com o MT5. A extração de dados não pode ser iniciada.")
                    return
            else:
                # Usuário optou por não reconectar
                return
        
        # Verifica se o MT5 está rodando como administrador
        # Só no Windows: no Linux o terminal roda no Wine, onde "administrador" não existe, e a
        # verificação (que lista todos os símbolos pela ponte) só atrasava a abertura da janela.
        if (os.name == "nt" and hasattr(self.app.mt5_connector, 'is_mt5_running_as_admin')
                and self.app.mt5_connector._is_mt5_running()):
            if not self.app.mt5_connector.is_mt5_running_as_admin():
                logging.warning("MT5 está executando sem privilégios de administrador.")
                response = messagebox.askyesno(
                    "Permissões Insuficientes",
                    "O MetaTrader 5 está em execução sem permissões de administrador.\n\n"
                    "Isso pode limitar o acesso a dados históricos e causar falhas na extração.\n\n"
                    "Deseja reiniciar o MT5 com privilégios de administrador agora?",
                )
                if response:
                    self.log("Reiniciando MT5 com permissões de administrador...")
                    self.update_status("Status: Reiniciando MT5 como administrador...")
                    
                    try:
                        if self.app.mt5_connector.launch_mt5_as_admin(wait_for_user=False):
                            # Dar tempo para o MT5 iniciar e tentar reconectar
                            time.sleep(5)
                            
                            # Tentar reconectar
                            if self.app.mt5_connector.initialize(recursion_count=0):
                                # Verificar novamente se agora está rodando como admin
                                if self.app.mt5_connector.is_mt5_running_as_admin():
                                    self.log("MT5 reiniciado com permissões de administrador com sucesso.")
                                    self.update_status("Status: MT5 iniciado com permissões de administrador.")
                                    messagebox.showinfo("MT5", "MetaTrader 5 reiniciado com permissões de administrador. Continuando com a extração.")
                                else:
                                    self.log("AVISO: MT5 reiniciado, mas ainda sem permissões de administrador.")
                                    self.update_status("Status: MT5 sem permissões de administrador")
                                    response = messagebox.askyesno(
                                        "Permissões Limitadas", 
                                        "O MT5 foi reiniciado, mas ainda parece estar sem permissões de administrador.\n\n"
                                        "Isso pode limitar as funcionalidades de extração de dados.\n\n"
                                        "Deseja continuar mesmo assim?"
                                    )
                                    if not response:
                                        return
                            else:
                                self.log("ERRO: Falha ao reconectar ao MT5 após reinício.")
                                messagebox.showerror("Erro de Conexão", "Falha ao reconectar ao MT5 após reinício. A extração pode falhar.")
                        else:
                            self.log("ERRO: Falha ao reiniciar MT5 como administrador.")
                            messagebox.showerror("Erro", "Não foi possível reiniciar o MT5 com permissões de administrador. A extração pode falhar.")
                    except Exception as admin_err:
                        self.log(f"ERRO: {str(admin_err)}")
                        logging.error(f"Erro ao tentar reiniciar MT5 como administrador: {traceback.format_exc()}")
                        messagebox.showerror("Erro", f"Erro ao tentar reiniciar MT5 como administrador: {str(admin_err)}")
                else:
                    # Usuário optou por continuar sem permissões de admin
                    response = messagebox.askyesno(
                        "Confirmação", 
                        "Continuar sem permissões de administrador pode resultar em:\n\n"
                        "- Falhas na obtenção de dados históricos\n"
                        "- Erros de conexão ao tentar acessar certos símbolos\n"
                        "- Limitações nas funcionalidades do MT5\n\n"
                        "Tem certeza que deseja continuar?"
                    )
                    if not response:
                        return
                
        if not self.app.selected_symbols:
            messagebox.showerror("Erro", "Nenhum símbolo selecionado para extração.")
            return

        # --- Criação da Janela de Configuração (Lógica da UI) ---
        config_window = tk.Toplevel(self.root)
        config_window.title("Configurar Extração de Dados Históricos")
        config_window.geometry("760x520")
        config_window.minsize(700, 480)
        config_window.transient(self.root)
        config_window.grab_set()  # Tornar modal

        main_frame = ttk.Frame(config_window, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # Período
        period_frame = ttk.LabelFrame(main_frame, text="Período")
        period_frame.pack(fill=tk.X, pady=10)
        ttk.Label(period_frame, text="Timeframe:").grid(row=0, column=0, padx=5, pady=5, sticky="w")
        timeframe_combo = ttk.Combobox(period_frame, state="readonly")
        timeframe_combo.grid(row=0, column=1, padx=5, pady=5, sticky="ew")
        timeframe_combo['values'] = [tf[0] for tf in self.app.timeframes] # Usa timeframes da app
        timeframe_combo.current(0)

        # Data Inicial / Final (simplificado para brevidade, a lógica completa está no original)
        ttk.Label(period_frame, text="Data Inicial:").grid(row=1, column=0, padx=5, pady=5, sticky="w")
        start_date_entry = ttk.Entry(period_frame) # Simplificado
        start_date_entry.grid(row=1, column=1, padx=5, pady=5, sticky="ew")
        start_date_entry.insert(0, (datetime.datetime.now() - datetime.timedelta(days=30)).strftime('%Y-%m-%d'))

        # Adicionar opções de auto-detecção de data
        auto_detect_var = tk.BooleanVar(value=True)
        config_window.auto_detect_var = auto_detect_var  # mantém a referência (sem ela a caixa aparece indefinida)
        ttk.Checkbutton(period_frame, text="Auto-detectar data mais antiga", variable=auto_detect_var).grid(row=1, column=2, padx=5, pady=5, sticky="w")

        # Botão para detectar a data mais antiga (com debug)
        detect_button = ttk.Button(period_frame, text="Detectar Agora", 
                                 command=lambda: detect_oldest_date())
        detect_button.grid(row=1, column=3, padx=5, pady=5, sticky="ew")
        # Garantir que o botão esteja visível
        detect_button.lift()
        
        # Botão cancelar detecção (inicialmente invisível)
        cancel_detect_button = ttk.Button(period_frame, text="Cancelar Detecção", 
                                        command=lambda: cancel_detection())
        cancel_detect_button.grid(row=1, column=4, padx=5, pady=5, sticky="ew")
        cancel_detect_button.grid_remove()  # Esconder inicialmente

        ttk.Label(period_frame, text="Data Final:").grid(row=2, column=0, padx=5, pady=5, sticky="w")
        end_date_entry = ttk.Entry(period_frame) # Simplificado
        end_date_entry.grid(row=2, column=1, padx=5, pady=5, sticky="ew")
        end_date_entry.insert(0, datetime.datetime.now().strftime('%Y-%m-%d'))

        # Opções
        options_frame = ttk.LabelFrame(main_frame, text="Opções de Extração")
        options_frame.pack(fill=tk.X, pady=10)
        include_indicators_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(options_frame, text="Calcular Indicadores", variable=include_indicators_var).pack(anchor="w")
        overwrite_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(options_frame, text="Sobrescrever Dados", variable=overwrite_var).pack(anchor="w")
        max_bars_var = tk.StringVar(value="10000")  # sem efeito na extração por blocos; campo removido da tela

        # Símbolos Selecionados (Display)
        symbols_frame = ttk.LabelFrame(main_frame, text=f"Símbolos Selecionados ({len(self.app.selected_symbols)})")
        symbols_frame.pack(fill=tk.BOTH, expand=True, pady=10)
        symbols_text = self.theme.classic(tk.Text(symbols_frame, height=4, wrap=tk.WORD, padx=8, pady=6))
        symbols_text.pack(fill=tk.BOTH, expand=True)
        symbols_text.insert(tk.END, ", ".join(self.app.selected_symbols))
        symbols_text.config(state=tk.DISABLED)

        # Controle (Progresso e Botões)
        control_frame = ttk.Frame(main_frame)
        control_frame.pack(fill=tk.X, pady=10)
        progress_var = tk.DoubleVar()
        ttk.Progressbar(control_frame, variable=progress_var).pack(fill=tk.X)
        status_var = tk.StringVar(value="Pronto")
        ttk.Label(control_frame, textvariable=status_var).pack(anchor="w")
        
        
        # Log para debug
        log.info("Configuração de extração histórica concluída - botões criados")
        
        # --- Lógica de Detecção de Data mais Antiga ---
        def detect_oldest_date():
            if not self.app.selected_symbols:
                messagebox.showerror("Erro", "Nenhum símbolo selecionado para detecção.")
                return
            
            if not self.app.mt5_initialized or not self.app.mt5_connector:
                response = messagebox.askyesno("MT5 Desconectado", 
                                             "O MT5 está desconectado. Deseja tentar reconectar para detectar a data mais antiga?")
                if response:
                    self.log("Tentando reconectar ao MT5...")
                    if not self.app.mt5_connector.initialize(recursion_count=0):
                        messagebox.showerror("Erro", "Falha ao conectar ao MT5. Não é possível detectar a data mais antiga.")
                        return
                else:
                    return
            
            # Modificar interface para indicar processamento
            status_var.set("Detectando data mais antiga...")
            detect_button.config(state=tk.DISABLED)
            cancel_detect_button.grid() # Mostrar botão de cancelar
            cancel_detect_button.lift()
            
            # Criar flag de cancelamento
            detection_canceled = [False]
            
            # Função para cancelar detecção
            def cancel_detection():
                detection_canceled[0] = True
                status_var.set("Detecção cancelada pelo usuário")
                cancel_detect_button.grid_remove()
                detect_button.config(state=tk.NORMAL)
            
            # Ligar o botão de cancelar à função
            cancel_detect_button.config(command=cancel_detection)
            
            # Lógica de detecção em thread separada
            def detection_thread():
                selected_timeframe = timeframe_combo.get()
                # Obter o valor numérico do timeframe
                timeframe_index = [tf[0] for tf in self.app.timeframes].index(selected_timeframe)
                timeframe_value = self.app.timeframes[timeframe_index][1]
                
                oldest_dates = {}
                
                try:
                    # Verificar cada símbolo
                    for symbol in self.app.selected_symbols:
                        if detection_canceled[0]:
                            break
                        
                        self.call_in_ui(status_var.set, f"Detectando data mais antiga para {symbol}...")
                        
                        try:
                            oldest = self.app.mt5_connector.get_oldest_available_date(
                                symbol, timeframe_value, force_refresh=True
                            )
                            
                            if oldest:
                                oldest_dates[symbol] = oldest
                                self.log(f"Data mais antiga para {symbol}: {oldest}")
                            else:
                                self.log(f"Não foi possível determinar data mais antiga para {symbol}")
                        except Exception as e:
                            self.log(f"Erro ao detectar data para {symbol}: {str(e)}")
                    
                    # Se cancelado, não atualiza UI
                    if detection_canceled[0]:
                        return
                        
                    # Determinar a data mais antiga entre todos os símbolos
                    if oldest_dates:
                        formatted_date = min(oldest_dates.values()).strftime('%Y-%m-%d')
                        self.log(f"Data mais antiga entre todos os símbolos: {formatted_date}")
                        self.call_in_ui(show_detected, f"Data mais antiga detectada: {formatted_date}", formatted_date)
                    else:
                        self.call_in_ui(show_detected, "Não foi possível determinar a data mais antiga", None)
                except Exception as e:
                    self.log(f"Erro na detecção de data mais antiga: {str(e)}")
                    self.call_in_ui(show_detected, f"Erro: {str(e)}", None)
                finally:
                    self.call_in_ui(restore_detect_buttons)

            def show_detected(message, formatted_date):
                if not config_window.winfo_exists():
                    return
                status_var.set(message)
                if formatted_date:
                    start_date_entry.delete(0, tk.END)
                    start_date_entry.insert(0, formatted_date)

            def restore_detect_buttons():
                if config_window.winfo_exists():
                    detect_button.config(state=tk.NORMAL)
                    cancel_detect_button.grid_remove()

            # Iniciar thread de detecção
            detection_thread = threading.Thread(target=detection_thread, daemon=True)
            detection_thread.start()
        
        def cancel_detection():
            """Não é necessário implementar aqui, uma vez que agora é definido localmente
            dentro da função detect_oldest_date."""
            pass
        
        def show_progress(progress, message):
            progress_var.set(progress)
            status_var.set(message)

        def finished(ok, failed, canceled):
            """Fim da extração/atualização: libera os botões, resume o resultado e marca os ativos com dados."""
            extract_button.config(state=tk.NORMAL)
            update_button.config(state=tk.NORMAL)
            cancel_button.config(text="Fechar")
            parts = [f"{ok} ativo(s) com sucesso"]
            if failed:
                parts.append(f"{failed} com falha (veja a aba Atividade)")
            if canceled:
                parts.append(f"{canceled} cancelado(s)")
            summary = "Concluído: " + ", ".join(parts) + "."
            progress_var.set(100)
            status_var.set(summary)
            self.log(summary)
            self.load_existing_symbols_data()

        # --- Lógica de Extração (será chamada pelo botão) ---
        def start_extraction_logic():
            # Obter valores da UI
            try:
                timeframe_idx = timeframe_combo.current()
                timeframe_name, timeframe_val = self.app.timeframes[timeframe_idx]
                # TODO: Usar DateEntry ou similar para datas
                start_date = datetime.datetime.strptime(start_date_entry.get(), '%Y-%m-%d')
                end_date = datetime.datetime.strptime(end_date_entry.get(), '%Y-%m-%d')
                end_date = end_date.replace(hour=23, minute=59, second=59) # Inclui todo o dia final
                max_bars = int(max_bars_var.get())
                include_indicators = include_indicators_var.get()
                overwrite = overwrite_var.get()

                if end_date < start_date:
                    messagebox.showerror("Erro", "Data final anterior à inicial.")
                    return
                if max_bars <= 0:
                    messagebox.showerror("Erro", "Máximo de barras deve ser positivo.")
                    return

            except Exception as config_err:
                messagebox.showerror("Erro de Configuração", f"Erro ao ler configurações: {config_err}")
                return

            # Desabilita botão e inicia thread (lógica movida para app)
            extract_button.config(state=tk.DISABLED)
            update_button.config(state=tk.DISABLED)
            cancel_button.config(text="Cancelar")
            status_var.set("Iniciando extração...")
            progress_var.set(0)

            # Chama a lógica de negócio na app principal
            # Passa os parâmetros obtidos da UI e callbacks para atualizar a UI
            # Chama a lógica de negócio na nova classe HistoricalExtractor
            self.app.historical_extractor.extract_data(
                symbols=self.app.selected_symbols,
                timeframe_val=timeframe_val,
                timeframe_name=timeframe_name, # Passar o nome descritivo
                start_date=start_date,
                end_date=end_date,
                # max_bars não é mais um parâmetro direto para extract_data
                include_indicators=include_indicators,
                overwrite=overwrite,
                # auto_detect_oldest_date é gerenciado internamente pelo extractor
                # max_workers pode ser configurado ou padrão
                update_progress_callback=lambda p, msg: self.call_in_ui(show_progress, p, msg),
                finished_callback=lambda s, f, c: self.call_in_ui(finished, s, f, c)
            )

        # --- Atualização incremental: do último registro salvo até agora ---
        def update_extraction_logic():
            try:
                timeframe_idx = timeframe_combo.current()
                timeframe_name, timeframe_val = self.app.timeframes[timeframe_idx]
            except Exception as config_err:
                messagebox.showerror("Erro de Configuração", f"Erro ao ler o timeframe: {config_err}")
                return
            if not self.app.selected_symbols:
                messagebox.showerror("Erro", "Nenhum símbolo selecionado.")
                return

            extract_button.config(state=tk.DISABLED)
            update_button.config(state=tk.DISABLED)
            cancel_button.config(text="Cancelar")
            status_var.set("Atualizando a partir do último registro salvo...")
            progress_var.set(0)

            self.app.historical_extractor.update_symbols(
                symbols=self.app.selected_symbols,
                timeframe_val=timeframe_val,
                timeframe_name=timeframe_name,
                include_indicators=include_indicators_var.get(),
                update_progress_callback=lambda p, msg: self.call_in_ui(show_progress, p, msg),
                finished_callback=lambda s, f, c: self.call_in_ui(finished, s, f, c)
            )

        def cancel_extraction_logic():
             if extract_button.cget('state') == tk.DISABLED:
                 # Chama a lógica de cancelamento na app principal
                 self.app.historical_extractor.cancel_extraction()
                 status_var.set("Cancelando...")
             else:
                 config_window.destroy()

        # Botões
        buttons_frame = ttk.Frame(control_frame)
        buttons_frame.pack(fill=tk.X, pady=5)
        extract_button = ttk.Button(buttons_frame, text="Iniciar Extração", style="Accent.TButton",
                                    command=start_extraction_logic)
        extract_button.pack(side=tk.LEFT, padx=5)
        update_button = ttk.Button(buttons_frame, text="Atualizar até agora", command=update_extraction_logic)
        update_button.pack(side=tk.LEFT, padx=5)
        cancel_button = ttk.Button(buttons_frame, text="Cancelar", command=cancel_extraction_logic)
        cancel_button.pack(side=tk.RIGHT, padx=5)

    def launch_mt5_as_admin(self):
        """Inicia o MT5 como administrador (se suportado)."""
        if self._offline_mode():
            self.refresh_mt5_status()
            return
        try:
            if not hasattr(self.app.mt5_connector, 'launch_mt5_as_admin'):
                messagebox.showerror("Função não suportada", "Lançamento como administrador não implementado nesta versão.")
                return

            # Avisa o usuário
            self.log("Tentando iniciar MT5 como administrador...")
            self.update_status("Status: Iniciando MT5 como administrador...")

            # Executa o método do MT5Connector
            if self.app.mt5_connector.launch_mt5_as_admin():
                self.log("Comando para iniciar MT5 como administrador enviado com sucesso.")
                self.log("Aguarde até que o MT5 inicie completamente e tente reconectar.")
                messagebox.showinfo("MT5", "Comando para iniciar MT5 como administrador enviado. Por favor, aguarde a inicialização e confirme quaisquer prompts de UAC.")
            else:
                self.log("Falha ao tentar iniciar MT5 como administrador.")
                messagebox.showerror("Erro", "Não foi possível iniciar o MT5 como administrador. Verifique o log para mais detalhes.")
        except Exception as e:
            log.debug(traceback.format_exc())
            messagebox.showerror("Erro", f"Erro ao tentar iniciar MT5 como administrador: {str(e)}")

    def create_menu(self):
        """Cria o menu da aplicação."""
        menu_bar = tk.Menu(self.root)
        self.theme.style_menu(menu_bar)
        
        # Menu File
        file_menu = tk.Menu(menu_bar, tearoff=0)
        file_menu.add_command(label="Configurações", command=self.open_settings)
        file_menu.add_command(label="Atalhos na área de trabalho e no menu...", command=self.open_shortcuts_dialog)
        file_menu.add_separator()
        
        # Submenu de exportação
        export_menu = tk.Menu(file_menu, tearoff=0)
        export_menu.add_command(label="Exportar para CSV", command=lambda: self.export_data("csv"))
        export_menu.add_command(label="Exportar para Excel", command=lambda: self.export_data("excel"))
        export_menu.add_command(label="Exportar para Parquet", command=lambda: self.export_data("parquet"))
        export_menu.add_command(label="Exportar para DuckDB", command=lambda: self.export_data("duckdb"))
        export_menu.add_separator()
        export_menu.add_command(label="Exportar Múltiplas Tabelas", command=self.export_multiple_tables)
        
        file_menu.add_cascade(label="Exportar Dados", menu=export_menu)
        file_menu.add_separator()
        
        # Opção para gerenciar favoritos
        favorites_menu = tk.Menu(file_menu, tearoff=0)
        favorites_menu.add_command(label="Adicionar Selecionados aos Favoritos", command=self.add_to_favorites)
        favorites_menu.add_command(label="Remover Selecionados dos Favoritos", command=self.remove_from_favorites)
        favorites_menu.add_separator()
        favorites_menu.add_command(label="Mostrar Apenas Favoritos", command=self.show_only_favorites)
        favorites_menu.add_command(label="Mostrar Todos", command=self.show_all_symbols)
        
        file_menu.add_cascade(label="Gerenciar Favoritos", menu=favorites_menu)
        file_menu.add_separator()
        
        file_menu.add_command(label="Sair", command=self.root.quit)
        
        # Menu Exibir
        view_menu = tk.Menu(menu_bar, tearoff=0)
        self._dark_var = tk.BooleanVar(value=self.theme.dark)
        view_menu.add_checkbutton(label="Tema escuro", variable=self._dark_var, command=self.toggle_theme)
        view_menu.add_separator()
        view_menu.add_command(label="Ativos", command=lambda: self.notebook.select(0))
        view_menu.add_command(label="Minha base", command=lambda: self.notebook.select(1))
        view_menu.add_command(label="Análise", command=lambda: self.notebook.select(2))
        view_menu.add_command(label="Robôs", command=lambda: self.notebook.select(self.robots_panel.frame))
        view_menu.add_command(label="Laboratório", command=lambda: self.notebook.select(self.laboratory_panel.frame))
        view_menu.add_command(label="Recursos", command=lambda: self.notebook.select(self.resources_panel.frame))
        view_menu.add_command(label="Atividade", command=lambda: self.notebook.select(self.log_tab))
        view_menu.add_separator()
        view_menu.add_command(label="Painel de resultados (abre no navegador)", command=self.open_results_panel)
        view_menu.add_command(label="Atualizar a base (F5)", command=self.refresh_inventory)

        # Menu Ajuda
        help_menu = tk.Menu(menu_bar, tearoff=0)
        help_menu.add_command(label="Documentação", command=self.open_documentation)
        help_menu.add_command(label="Glossário (indicadores e números da análise)", command=self.show_glossary)
        help_menu.add_command(label="Verificar conexão com o MetaTrader 5", command=self.check_mt5_status)
        help_menu.add_command(label="Diagnóstico do ambiente...", command=self.show_doctor)
        help_menu.add_separator()
        help_menu.add_command(label="Sobre", command=self.show_about)
        
        # Menu Estratégias (pesquisa com os dados do banco; não envia ordens)
        strategy_menu = tk.Menu(menu_bar, tearoff=0)
        strategy_menu.add_command(label="Triagem de ativos (Filtros A e B)...",
                                  command=lambda: self.open_strategy_dialog("screen"))
        strategy_menu.add_command(label="Validar estratégias (Filtro C)...",
                                  command=lambda: self.open_strategy_dialog("validate"))
        strategy_menu.add_command(label="Gerar relatório e abrir no navegador...",
                                  command=lambda: self.open_strategy_dialog("report"))
        strategy_menu.add_command(label="Painel de resultados: ativos x timeframes x robôs...",
                                  command=self.open_results_panel)
        strategy_menu.add_separator()
        strategy_menu.add_command(label="Sobre as estratégias", command=self.show_strategies_info)

        # Adicionar menus à barra de menu
        for menu in (file_menu, export_menu, favorites_menu, view_menu, strategy_menu, help_menu):
            self.theme.style_menu(menu)
        menu_bar.add_cascade(label="Arquivo", menu=file_menu)
        menu_bar.add_cascade(label="Exibir", menu=view_menu)
        menu_bar.add_cascade(label="Estratégias", menu=strategy_menu)
        menu_bar.add_cascade(label="Ajuda", menu=help_menu)
        
        # Configurar a barra de menu
        self.root.config(menu=menu_bar)
    
    def show_doctor(self):
        """Mostra o diagnóstico do ambiente (o mesmo do comando 'mt5x doctor'), com as dicas de correção."""
        from mt5_extracao.bootstrap import doctor, paths
        dialog = tk.Toplevel(self.root)
        dialog.title("Diagnóstico do ambiente")
        dialog.geometry("820x520")
        dialog.transient(self.root)
        text = self.theme.classic(scrolledtext.ScrolledText(dialog, wrap=tk.WORD, padx=10, pady=8))
        text.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        text.insert(tk.END, "Verificando o ambiente, aguarde...")
        text.config(state=tk.DISABLED)
        ttk.Button(dialog, text="Fechar", command=dialog.destroy).pack(pady=(0, 10))
        result = {}

        def work():
            try:
                result["text"] = doctor.format_report(doctor.run_checks(paths.PROJECT_ROOT))
            except Exception as e:
                result["text"] = f"Não foi possível concluir o diagnóstico: {e}"

        def show():
            if not dialog.winfo_exists():
                return
            if "text" not in result:
                dialog.after(300, show)
                return
            text.config(state=tk.NORMAL)
            text.delete("1.0", tk.END)
            text.insert(tk.END, result["text"])
            text.config(state=tk.DISABLED)

        threading.Thread(target=work, daemon=True).start()
        dialog.after(300, show)

    def export_data(self, format_type):
        """
        Exibe diálogo para exportar dados de uma tabela para CSV ou Excel.
        
        Args:
            format_type (str): Tipo de formato ('csv' ou 'excel')
        """
        if not hasattr(self.app, 'db_manager'):
            messagebox.showerror("Erro", "Banco de dados não inicializado")
            return
            
        # Criar janela de diálogo
        export_dialog = tk.Toplevel(self.root)
        export_dialog.title(f"Exportar para {format_type.upper()}")
        export_dialog.geometry("500x350")
        export_dialog.grab_set()  # Modal
        
        # Frame principal
        main_frame = ttk.Frame(export_dialog, style="TFrame")
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Obter lista de tabelas
        tables = self.app.db_manager.get_all_tables()
        
        # Área de seleção de tabela
        ttk.Label(main_frame, text="Selecione a tabela para exportar:").pack(anchor=tk.W)
        
        # Frame para a lista e barra de rolagem
        list_frame = ttk.Frame(main_frame)
        list_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        
        # Scrollbar
        scrollbar = ttk.Scrollbar(list_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Lista de tabelas
        table_listbox = self.theme.classic(tk.Listbox(list_frame, yscrollcommand=scrollbar.set, height=10))
        table_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.config(command=table_listbox.yview)
        
        # Preencher a lista
        for table in tables:
            table_listbox.insert(tk.END, table)
            
        # Campo para filtros (WHERE)
        ttk.Label(main_frame, text="Filtros (cláusula WHERE - opcional):").pack(anchor=tk.W, pady=(10, 0))
        filters_entry = ttk.Entry(main_frame, width=50)
        filters_entry.pack(fill=tk.X, pady=5)
        
        # Exemplo de filtro
        ttk.Label(main_frame, text="Exemplo: time > '2023-01-01' AND time < '2023-12-31'", 
                 font=("", 8)).pack(anchor=tk.W)
                 
        # Frame para botões
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=10)
        
        def do_export():
            selected_idx = table_listbox.curselection()
            if not selected_idx:
                messagebox.showwarning("Aviso", "Selecione uma tabela para exportar")
                return
                
            self.export_table(table_listbox.get(selected_idx[0]), format_type, filters_entry.get(),
                              on_success=export_dialog.destroy)

        # Botões
        ttk.Button(button_frame, text="Exportar", style="Accent.TButton", command=do_export).pack(side=tk.RIGHT, padx=5)
        ttk.Button(button_frame, text="Cancelar", command=export_dialog.destroy).pack(side=tk.RIGHT)
    
    def export_multiple_tables(self):
        """Exibe diálogo para exportar múltiplas tabelas"""
        if not hasattr(self.app, 'db_manager'):
            messagebox.showerror("Erro", "Banco de dados não inicializado")
            return
            
        # Criar janela de diálogo
        export_dialog = tk.Toplevel(self.root)
        export_dialog.title("Exportar Múltiplas Tabelas")
        export_dialog.geometry("600x400")
        export_dialog.grab_set()  # Modal
        
        # Frame principal
        main_frame = ttk.Frame(export_dialog)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Obter lista de tabelas
        tables = self.app.db_manager.get_all_tables()
        
        # Label
        ttk.Label(main_frame, text="Selecione as tabelas para exportar:").pack(anchor=tk.W)
        
        # Frame para listas
        lists_frame = ttk.Frame(main_frame)
        lists_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        
        # Frame para lista esquerda (todas as tabelas)
        left_frame = ttk.LabelFrame(lists_frame, text="Tabelas Disponíveis")
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 5))
        
        # Frame para a lista e barra de rolagem (esquerda)
        left_list_frame = ttk.Frame(left_frame)
        left_list_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        
        # Scrollbar
        left_scrollbar = ttk.Scrollbar(left_list_frame)
        left_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Lista de tabelas disponíveis
        available_tables = self.theme.classic(tk.Listbox(left_list_frame, yscrollcommand=left_scrollbar.set,
                                                         selectmode=tk.EXTENDED))
        available_tables.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        left_scrollbar.config(command=available_tables.yview)
        
        # Preencher a lista
        for table in tables:
            available_tables.insert(tk.END, table)
            
        # Frame para botões de transferência
        transfer_frame = ttk.Frame(lists_frame)
        transfer_frame.pack(side=tk.LEFT, fill=tk.Y, padx=5)
        
        # Frame para lista direita (tabelas selecionadas)
        right_frame = ttk.LabelFrame(lists_frame, text="Tabelas Selecionadas")
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=(5, 0))
        
        # Frame para a lista e barra de rolagem (direita)
        right_list_frame = ttk.Frame(right_frame)
        right_list_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        
        # Scrollbar
        right_scrollbar = ttk.Scrollbar(right_list_frame)
        right_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Lista de tabelas selecionadas
        selected_tables = self.theme.classic(tk.Listbox(right_list_frame, yscrollcommand=right_scrollbar.set,
                                                        selectmode=tk.EXTENDED))
        selected_tables.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        right_scrollbar.config(command=selected_tables.yview)
        
        # Funções para os botões
        def add_selected():
            selected = available_tables.curselection()
            for i in selected:
                table = available_tables.get(i)
                if table not in selected_tables.get(0, tk.END):
                    selected_tables.insert(tk.END, table)
        
        def remove_selected():
            selected = selected_tables.curselection()
            for i in reversed(selected):  # Reversed para não afetar os índices
                selected_tables.delete(i)
        
        def add_all():
            selected_tables.delete(0, tk.END)
            for table in tables:
                selected_tables.insert(tk.END, table)
        
        def remove_all():
            selected_tables.delete(0, tk.END)
        
        # Botões de transferência
        ttk.Button(transfer_frame, text=">>", command=add_all).pack(pady=5)
        ttk.Button(transfer_frame, text=">", command=add_selected).pack(pady=5)
        ttk.Button(transfer_frame, text="<", command=remove_selected).pack(pady=5)
        ttk.Button(transfer_frame, text="<<", command=remove_all).pack(pady=5)
        
        # Opções de formato
        format_var = tk.StringVar(value="xlsx")
        
        # Frame para opções
        options_frame = ttk.Frame(main_frame)
        options_frame.pack(fill=tk.X, pady=10)
        
        # Opções de formato
        format_frame = ttk.LabelFrame(options_frame, text="Formato de Exportação")
        format_frame.pack(fill=tk.X, padx=5)
        
        ttk.Radiobutton(format_frame, text="Excel (XLSX)", variable=format_var, value="xlsx").pack(side=tk.LEFT, padx=20)
        ttk.Radiobutton(format_frame, text="CSV (múltiplos arquivos)", variable=format_var, value="csv").pack(side=tk.LEFT, padx=20)
        
        # Frame para botões
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=10)
        
        def do_multi_export():
            selected = selected_tables.get(0, tk.END)
            if not selected:
                messagebox.showwarning("Aviso", "Selecione pelo menos uma tabela para exportar")
                return
                
            # Criar instância do exportador se não existir
            if not hasattr(self, 'data_exporter'):
                self.data_exporter = DataExporter(self.app.db_manager)
                
            format_type = format_var.get()
                
            # Solicitar local para salvar
            if format_type == 'xlsx':
                file_path = filedialog.asksaveasfilename(
                    defaultextension=".xlsx",
                    filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")],
                    initialfile="multi_export.xlsx"
                )
            else:  # csv
                # Para CSV, selecionamos um diretório
                directory = filedialog.askdirectory(
                    title="Selecione o diretório para exportar os arquivos CSV"
                )
                if directory:
                    file_path = os.path.join(directory, f"export_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}")
                else:
                    file_path = None
                
            if file_path:
                try:
                    result_path = self.data_exporter.export_multiple_tables(
                        list(selected), 
                        caminho_arquivo=file_path, 
                        formato=format_type
                    )
                    
                    if result_path:
                        messagebox.showinfo("Sucesso", f"Dados exportados com sucesso para:\n{result_path}")
                        export_dialog.destroy()
                    else:
                        messagebox.showwarning("Aviso", "Nenhum dado encontrado para exportar")
                except Exception as e:
                    messagebox.showerror("Erro", f"Falha ao exportar: {str(e)}")
        
        # Botões
        ttk.Button(button_frame, text="Exportar", command=do_multi_export).pack(side=tk.RIGHT, padx=5)
        ttk.Button(button_frame, text="Cancelar", command=export_dialog.destroy).pack(side=tk.RIGHT)

    def open_settings(self):
        """Abre as configurações: o que está em uso e as opções ajustáveis pela interface (janela do MT5)."""
        from mt5_extracao.ui.settings import SettingsDialog
        self.settings_dialog = SettingsDialog(self)
        return self.settings_dialog

    def open_mt5_view(self, parent=None, on_done=None):
        """
        Abre (ou, com a sua confirmação, fecha e reabre) o MetaTrader 5 com a organização da janela escolhida nas
        configurações. `on_done(resultado ou None)` é chamado ao fim, na thread da interface.
        """
        if self._offline_mode():
            self.refresh_mt5_status()
            if on_done:
                on_done(None)
            return
        from mt5_extracao.robots import mt5_tools, view
        config = self.app.config
        parent = parent or self.root
        self.mt5_view_button.state(["disabled"])
        self.update_status("Status: preparando a visão do MetaTrader 5...")

        def finish(result):
            self.mt5_view_button.state(["!disabled"])
            if on_done:
                on_done(result)

        def opened(result, error):
            if error:
                self.update_status("Status: não foi possível abrir a visão do MetaTrader 5")
                self.log(f"Não foi possível abrir a visão do MetaTrader 5: {error}")
                messagebox.showerror("Abrir visão MT5", f"Não foi possível abrir o MetaTrader 5:\n\n{error}\n\n"
                                     "Confira [MT5] path em Arquivo > Configurações > Diagnóstico do ambiente.",
                                     parent=parent)
                finish(None)
                return
            text = view.describe(result)
            self.log("Visão do MetaTrader 5: " + text)
            self.update_status("Status: MetaTrader 5 aberto com a visão escolhida" +
                               (" (clique em \"Verificar conexão\" para reconectar)" if result["reopened"] else ""))
            finish(result)

        def checked(running, error):
            if error:
                opened(None, error)
                return
            if running and not messagebox.askyesno(
                    "Abrir visão MT5",
                    "O MetaTrader 5 está aberto. A organização da janela só é gravada com ele fechado.\n\n"
                    "Fechar e abrir de novo agora?\n\n"
                    "Um robô que esteja no gráfico para enquanto o terminal reinicia e volta com ele; a coleta e a "
                    "extração em andamento neste programa são interrompidas (depois, use \"Verificar conexão\").",
                    parent=parent):
                self.update_status("Status: visão do MetaTrader 5 não alterada")
                finish(None)
                return
            self.update_status("Status: " + ("fechando e abrindo" if running else "abrindo") + " o MetaTrader 5...")
            # se o perfil de gráficos próprio ainda não existe, ele nasce com o gráfico do ativo em foco
            # o ativo em foco (o clicado na lista); sem ele, a visão usa o gráfico que a pessoa já tinha
            symbol = self.detail_symbol or ""
            account = self.app.mt5_connector.account_summary() if self._mt5_connected() else self.prefs.get("account")
            self.run_bg(lambda: view.open_view(config, close_terminal=running, symbol=symbol,
                                               db=self.app.db_manager, account=account,
                                               confirm=mt5_tools.confirm_startup), opened)

        self.run_bg(lambda: mt5_tools.terminal_running(config), checked)

    def open_shortcuts_dialog(self):
        """Cria ou remove os atalhos do aplicativo na área de trabalho e no menu de aplicativos / Iniciar."""
        from mt5_extracao.bootstrap import paths, shortcuts
        menu_label = "Menu Iniciar" if os.name == "nt" else "Menu de aplicativos"
        dialog = tk.Toplevel(self.root)
        dialog.title("Atalhos do aplicativo")
        dialog.transient(self.root)
        frame = ttk.Frame(dialog, padding=15)
        frame.pack(fill=tk.BOTH, expand=True)
        ttk.Label(frame, text="Onde você quer um atalho para abrir o aplicativo?").pack(anchor="w", pady=(0, 8))
        desktop_var = tk.BooleanVar(value=True)
        menu_var = tk.BooleanVar(value=True)
        dialog.shortcut_vars = (desktop_var, menu_var)  # mantém as referências das caixas
        ttk.Checkbutton(frame, text="Área de trabalho", variable=desktop_var).pack(anchor="w")
        ttk.Checkbutton(frame, text=menu_label, variable=menu_var).pack(anchor="w")
        status_var = tk.StringVar()
        ttk.Label(frame, textvariable=status_var, wraplength=560, justify=tk.LEFT).pack(anchor="w", pady=(10, 0))

        def show_existing():
            found = shortcuts.existing()
            status_var.set("Atalhos existentes:\n" + "\n".join(f"  {p}" for p in found) if found
                           else "Nenhum atalho criado ainda.")

        def create():
            if not (desktop_var.get() or menu_var.get()):
                status_var.set("Marque pelo menos um dos dois lugares.")
                return
            results = shortcuts.create(paths.PROJECT_ROOT, desktop_var.get(), menu_var.get())
            lines = [f"Não foi possível criar em {p}: {err}" if err else f"Atalho criado: {p}" for p, err in results]
            status_var.set("\n".join(lines))
            self.log("; ".join(lines))

        def remove():
            removed = shortcuts.remove(desktop_var.get(), menu_var.get())
            status_var.set("\n".join(f"Atalho removido: {p}" for p in removed) if removed
                           else "Não havia atalho para remover nos lugares marcados.")

        buttons = ttk.Frame(frame)
        buttons.pack(fill=tk.X, pady=(15, 0))
        ttk.Button(buttons, text="Criar atalhos", command=create).pack(side=tk.LEFT)
        ttk.Button(buttons, text="Remover atalhos", command=remove).pack(side=tk.LEFT, padx=10)
        ttk.Button(buttons, text="Fechar", command=dialog.destroy).pack(side=tk.RIGHT)
        show_existing()

    def _open_path(self, path):
        """Abre uma pasta/arquivo no programa padrão do sistema."""
        import subprocess
        try:
            if os.name == "nt":
                os.startfile(str(path))  # noqa: só existe no Windows
            else:
                subprocess.Popen(["xdg-open", str(path)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception as e:
            messagebox.showinfo("Abrir", f"Não foi possível abrir automaticamente.\nCaminho: {path}\n({e})")
        
    def show_about(self):
        """Exibe informações sobre o aplicativo"""
        about_dialog = tk.Toplevel(self.root)
        about_dialog.title("Sobre MT5 Extração")
        about_dialog.geometry("560x320")
        about_dialog.transient(self.root)
        about_dialog.grab_set()  # Modal
        
        # Frame principal
        main_frame = ttk.Frame(about_dialog, padding=15)
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Título
        title_label = ttk.Label(main_frame, text="MT5 Extração", style="Page.Title.TLabel")
        title_label.pack(pady=(10, 5))
        
        # Versão
        try:
            from importlib.metadata import version as _pkg_version
            versao = _pkg_version("mt5-extracao")
        except Exception:
            # Rodando da pasta do projeto, sem instalar o pacote: lê a versão do pyproject.toml
            import re
            try:
                pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
                versao = re.search(r'^version\s*=\s*"([^"]+)"', pyproject.read_text(encoding="utf-8"), re.M).group(1)
            except Exception:
                versao = "desenvolvimento"
        version_label = ttk.Label(main_frame, text=f"Versão {versao}", style="Page.Muted.TLabel")
        version_label.pack(pady=(0, 10))
        
        # Descrição
        description = (
            "Ferramenta para extração e análise de dados do MetaTrader 5.\n\n"
            "Monta e mantém bases históricas (barras, indicadores e ticks) em banco de dados local, "
            "exporta para CSV, Excel, Parquet e DuckDB e faz pesquisa de estratégias. Não envia ordens."
        )
        
        desc_label = ttk.Label(main_frame, text=description, justify=tk.CENTER,
                               wraplength=500)
        desc_label.pack(pady=10)
        
        # Copyright
        
        # Botão de fechar
        close_button = ttk.Button(main_frame, text="Fechar", command=about_dialog.destroy)
        close_button.pack(side=tk.BOTTOM, pady=10)
        
    # --- Estratégias -----------------------------------------------------------------

    def show_strategies_info(self):
        from mt5_extracao.strategies import AVOID, REGISTRY
        lines = [f"• {c.info.name} ({c.info.regime}): acerto {c.info.win_rate}, R:R {c.info.reward_risk}, "
                 f"{', '.join(c.info.timeframes)}" for c in REGISTRY.values()]
        lines += ["", "Evite no início:"] + [f"• {n}: {why}" for n, why in AVOID]
        lines += ["", "Backtest não garante resultado futuro. Guia completo: docs/estrategias.md"]
        messagebox.showinfo("Estratégias", "\n".join(lines))

    def open_strategy_dialog(self, mode):
        """Diálogo único para triagem, validação e relatório (roda em segundo plano)."""
        from mt5_extracao import services, timeframes
        from mt5_extracao.strategies import REGISTRY
        if not getattr(self.app, "db_manager", None) or not self.app.db_manager.is_connected():
            messagebox.showerror("Estratégias", "Banco de dados não disponível.")
            return
        titles = {"screen": "Triagem de ativos", "validate": "Validar estratégias (Filtro C)",
                  "report": "Relatório de estratégias"}
        dialog = tk.Toplevel(self.root)
        dialog.title(titles[mode])
        dialog.geometry("820x520")
        frame = ttk.Frame(dialog, padding=10)
        frame.pack(fill=tk.BOTH, expand=True)

        ttk.Label(frame, text="Timeframe:").grid(row=0, column=0, sticky="w")
        tf_var = tk.StringVar(value="D1")
        ttk.Combobox(frame, textvariable=tf_var, width=6, state="readonly",
                     values=[t.name for t in timeframes.MAIN]).grid(row=0, column=1, sticky="w")
        ttk.Label(frame, text="Símbolos (vazio = todos com dados):").grid(row=1, column=0, sticky="w", pady=4)
        symbols_var = tk.StringVar(value=",".join(self.app.selected_symbols or []))
        ttk.Entry(frame, textvariable=symbols_var, width=60).grid(row=1, column=1, columnspan=3, sticky="we")

        strategy_vars = {}
        if mode in ("validate", "report"):
            box = ttk.LabelFrame(frame, text="Estratégias a validar" if mode == "validate"
                                 else "Validar antes de gerar (opcional; pode levar minutos)")
            box.grid(row=2, column=0, columnspan=4, sticky="we", pady=6)
            for i, (key, cls) in enumerate(REGISTRY.items()):
                strategy_vars[key] = tk.BooleanVar(value=(mode == "validate"))
                ttk.Checkbutton(box, text=cls.info.name, variable=strategy_vars[key]).grid(
                    row=i // 3, column=i % 3, sticky="w", padx=6)

        status = tk.StringVar(value="Escolha o timeframe e clique em Executar.")
        ttk.Label(frame, textvariable=status, wraplength=780).grid(row=3, column=0, columnspan=4, sticky="w", pady=4)
        columns = ("Ativo", "Válido", "Regime", "Estratégia", "Score", "Observação")
        tree = ttk.Treeview(frame, columns=columns, show="headings", height=12)
        for col, width in zip(columns, (90, 60, 90, 170, 50, 340)):
            tree.heading(col, text=col)
            tree.column(col, width=width, anchor="w")
        tree.grid(row=4, column=0, columnspan=4, sticky="nsew")
        frame.rowconfigure(4, weight=1)
        frame.columnconfigure(3, weight=1)
        run_button = ttk.Button(frame, text="Executar")
        run_button.grid(row=0, column=3, sticky="e")

        # A thread de trabalho nunca toca no Tkinter: envia (função, args) para a fila,
        # que a thread principal esvazia a cada 100 ms.
        pending = queue.Queue()

        def ui(fn, *args):
            pending.put((fn, args))

        def poll():
            if not dialog.winfo_exists():
                return
            while True:
                try:
                    fn, args = pending.get_nowait()
                except queue.Empty:
                    break
                fn(*args)
            if "disabled" in run_button.state() or not pending.empty():
                dialog.after(100, poll)

        def show_rows(rows):
            tree.delete(*tree.get_children())
            for r in rows:
                obs = "; ".join(r["notes"] + r["warnings"] + ([r["validated"]] if r["validated"] else [])) \
                    if r["valid"] else "; ".join(r["reasons"])
                tree.insert("", tk.END, values=(r["symbol"], ("⚠" if r["warnings"] else "✅") if r["valid"] else "❌",
                                                r["regime"], r["strategy_name"] if r["valid"] else "—",
                                                f"{r['score']:.0f}", obs))

        def show_verdicts(verdicts):
            tree.delete(*tree.get_children())
            for v in verdicts:
                tree.insert("", tk.END, values=(v["symbol"], "✅" if v["approved"] else "❌", "",
                                                REGISTRY[v["strategy"]].info.name, "", v["status"]))

        def finish(message):
            status.set(message)
            run_button.state(["!disabled"])
            if mode != "screen":      # a validação grava vereditos: os cards dos robôs mostram o resultado
                self.robots_panel.reload()

        def work(tf, symbols, keys):
            from mt5_extracao.strategies import research
            progress = lambda msg: ui(status.set, msg)
            try:
                config = services.load_config()
            except FileNotFoundError:
                config = None
            try:
                db = self.app.db_manager
                symbols = symbols or research.symbols_with_data(db, tf)
                if not symbols:
                    ui(finish, f"Nenhum símbolo com dados em {tf.name}. Extraia dados primeiro.")
                    return
                if mode == "screen":
                    rows, missing = research.run_screen(db, symbols, tf, config, progress)
                    ui(show_rows, rows)
                    msg = f"{sum(r['valid'] for r in rows)} de {len(rows)} ativo(s) aptos."
                elif mode == "validate":
                    verdicts, missing = research.run_validation(db, symbols, tf, keys, config, progress)
                    ui(show_verdicts, verdicts)
                    msg = f"{sum(v['approved'] for v in verdicts)} de {len(verdicts)} combinação(ões) aprovada(s)."
                else:
                    if keys:
                        research.run_validation(db, symbols, tf, keys, config, progress)
                    path, rows, missing = research.build_report(db, symbols, tf, config, progress=progress)
                    ui(show_rows, rows)
                    import webbrowser
                    webbrowser.open(Path(os.path.abspath(path)).as_uri())
                    msg = f"Relatório salvo em {os.path.abspath(path)} e aberto no navegador."
                if missing:
                    msg += " " + research.missing_message(missing, tf)
                ui(finish, msg)
            except Exception as e:  # mostrado ao usuário; detalhes no log
                log.exception("Erro na pesquisa de estratégias")
                ui(finish, f"Erro: {e}")

        def run():
            tf = timeframes.parse(tf_var.get())
            symbols = [x.strip() for x in symbols_var.get().split(",") if x.strip()]
            keys = [k for k, var in strategy_vars.items() if var.get()]
            if mode == "validate" and not keys:
                messagebox.showwarning("Estratégias", "Marque ao menos uma estratégia.", parent=dialog)
                return
            run_button.state(["disabled"])
            status.set("Processando...")
            threading.Thread(target=work, args=(tf, symbols, keys), daemon=True).start()
            dialog.after(100, poll)

        run_button.configure(command=run)

    def open_documentation(self):
        """Abre a pasta da documentação (docs/) no gerenciador de arquivos."""
        self._open_path(Path(self._project_root()) / "docs")

    def _offline_mode(self):
        return bool(getattr(self.app, "offline", False))

    def _show_offline_status(self, quiet=False):
        self.mt5_status_value.config(text="Análise local (offline)")
        self.mt5_status_dot.config(foreground=self.theme.colors["ink2"])
        if not quiet:
            self.update_status("Reabra sem --offline para conectar.")
        for name in ("mt5_view_button", "check_button", "mt5_admin_button"):
            button = getattr(self, name, None)
            if button is not None:
                button.state(["disabled"])
        return False

    def _mt5_connected(self):
        if self._offline_mode():
            return False
        connector = self.app.mt5_connector
        return bool(connector and self.app.mt5_initialized and connector.is_initialized)

    @staticmethod
    def _missing_account():
        """O MetaTrader 5 instalado ainda não tem nenhuma conta cadastrada?"""
        from mt5_extracao.bootstrap import demo_account, paths
        cfg = demo_account.read_settings(paths.PROJECT_ROOT)
        return bool(cfg["mt5_dir"] and os.path.isdir(cfg["mt5_dir"]) and not demo_account.has_account(
            cfg["mt5_dir"], cfg["wine_prefix"] or None))

    def bring_to_front(self):
        """Traz a janela do programa para a frente (o MetaTrader 5, ao abrir, fica por cima dela)."""
        try:
            self.root.deiconify()
            self.root.lift()
            self.root.attributes("-topmost", True)
            self.root.after(600, lambda: self.root.attributes("-topmost", False))
            self.root.focus_force()
        except tk.TclError as e:
            log.debug(f"Não foi possível trazer a janela para a frente: {e}")

    def _remember_account(self):
        """Guarda a conta conferida nesta conexão: é ela que a visão usa para pôr o painel com o MT5 ainda fechado."""
        try:
            info = self.app.mt5_connector.account_summary()
        except Exception as e:
            log.debug(f"Conta não lida: {e}")
            return
        if info and info.get("login"):
            self.prefs["account"] = {"login": info["login"], "server": info.get("server", ""),
                                     "demo": bool(info.get("demo"))}
            self.save_prefs()

    def _open_organized_terminal(self):
        """
        Se o MetaTrader 5 está fechado, abre-o já com a organização escolhida e o painel. É chamado antes de toda
        tentativa de conexão (inclusive a reconexão automática depois que a pessoa fecha o MT5), porque sem isso é
        a própria conexão que abre o terminal, sem nada. Um pedido por vez; falha aqui não impede a conexão.
        """
        from mt5_extracao.robots import mt5_tools, view
        if not self._terminal_lock.acquire(blocking=False):
            return
        try:
            config = self.app.config
            if not mt5_tools.terminal_running(config):
                opened = view.open_view(config, db=self.app.db_manager, account=self.prefs.get("account"),
                                        confirm=mt5_tools.confirm_startup)
                log.info("Visão do MetaTrader 5 ao conectar: " + view.describe(opened))
        except Exception as e:
            log.warning(f"Não foi possível abrir o MetaTrader 5 já organizado ({e}). Seguindo com a conexão.")
        finally:
            self._terminal_lock.release()

    def _install_terminal_hook(self):
        connector = self.app.mt5_connector
        if hasattr(connector, "prepare_terminal") and connector.prepare_terminal is None:
            connector.prepare_terminal = self._open_organized_terminal

    def connect_mt5(self, startup=False):
        """
        Abre o MetaTrader 5 (com a janela organizada como nas configurações, se ele estiver fechado) e conecta,
        tudo em segundo plano: a janela do programa continua utilizável. No fim atualiza a lista de ativos e, se
        não conectou, diz o motivo e o que fazer.
        """
        if self._offline_mode():
            return self._show_offline_status()
        if getattr(self, "_connecting", False):
            return
        from mt5_extracao import connection_help
        from mt5_extracao.robots import mt5_tools, view
        self._connecting = True
        app, config = self.app, self.app.config
        self.mt5_status_value.config(text="Abrindo e conectando ao MetaTrader 5...")
        self.mt5_status_dot.config(foreground=self.theme.colors["ink2"])
        self.check_button.state(["disabled"])
        self.update_status("Status: abrindo e conectando ao MetaTrader 5 (a base já extraída segue disponível)...")

        def work():
            mt5_tools.use_config(config)
            self._install_terminal_hook()       # as reconexões automáticas do conector também passam por ele
            self._open_organized_terminal()     # terminal fechado: abre já organizado, antes de conectar
            connected = bool(app.mt5_connector.initialize(recursion_count=0))
            if connected:
                app.mt5_initialized = True
                app.load_symbols()
                return None
            return connection_help.diagnose(mt5_tools.terminal_running(config), connection_help.internet_ok())

        def done(reason, error):
            self._connecting = False
            self.check_button.state(["!disabled"])
            if error is not None:
                reason = connection_help.NOT_OPEN
            self.filter_symbols()
            self.bring_to_front()
            if self.refresh_mt5_status():
                self.log("Conectado ao MetaTrader 5.")
                self._remember_account()
                return
            app.mt5_initialized = False
            text = connection_help.explain(reason, windows=os.name == "nt")
            self.log("Sem conexão com o MetaTrader 5: " + text.replace("\n\n", " ").replace("\n", " "))
            self.update_status("Status: sem conexão com o MetaTrader 5 (" + reason + ")")
            if reason != connection_help.NO_INTERNET and self._missing_account():
                self._offer_demo_account()      # terminal sem conta nenhuma: oferece abrir a de demonstração
            else:
                messagebox.showwarning("MetaTrader 5 sem conexão", text, parent=self.root)

        self.run_bg(work, done)

    def refresh_mt5_status(self, quiet=False):
        """Atualiza o cabeçalho com a conta e o servidor em uso (sem perguntas)."""
        if self._offline_mode():
            return self._show_offline_status(quiet)
        colors = self.theme.colors
        if not self._mt5_connected():
            if not quiet:
                self.update_status("Status: sem conexão com o MetaTrader 5")
            self.mt5_status_value.config(text="MetaTrader 5 desconectado")
            self.mt5_status_dot.config(foreground=colors["bad"])
            return False
        info = self.app.mt5_connector.account_summary()
        if info:
            kind = " (demonstração)" if info["demo"] else ""
            text = f"Conectado: conta {info['login']} em {info['server']}{kind}"
        else:
            text = "Conectado"
        if not quiet:
            self.update_status("Status: pronto")
        self.mt5_status_value.config(text=text)
        self.mt5_status_dot.config(foreground=colors["up"])
        return True

    def _reconnect_mt5(self):
        """Conecta de novo e recarrega a lista de símbolos da conta."""
        if self._offline_mode():
            return self._show_offline_status()
        self.update_status("Status: conectando ao MetaTrader 5...")
        self.root.update_idletasks()
        self.app.mt5_initialized = bool(self.app.mt5_connector.initialize(recursion_count=0))
        if self.app.mt5_initialized:
            self.app.load_symbols()
            self.filter_symbols()
        return self.refresh_mt5_status()

    def _offer_demo_account(self):
        """MT5 sem conta: oferece abrir a conta de demonstração gratuita e conecta em seguida."""
        if self._offline_mode():
            return self._show_offline_status()
        from mt5_extracao.bootstrap import demo_account, paths
        data = demo_account.DEMO_DEFAULTS
        if not messagebox.askyesno(
                "Abrir conta de demonstração",
                "O MetaTrader 5 ainda não tem uma conta, por isso não conecta.\n\n"
                "Posso abrir agora a conta de demonstração gratuita (dinheiro virtual, servidor "
                "MetaQuotes-Demo) e preencher o cadastro com dados do projeto:\n"
                f"  {data['first']} {data['last']} - {data['email']}\n\n"
                f"{demo_account.TERMS_NOTICE}\n\nAbrir a conta agora?"):
            return
        self.update_status("Status: abrindo a conta de demonstração (cerca de 1 minuto)...")
        result = {}

        def work():
            try:
                result["code"] = demo_account.open_demo_account(paths.PROJECT_ROOT, log=log.info)
            except Exception as e:
                log.error(f"Erro ao abrir a conta de demonstração: {e}")
                result["code"] = 1

        def wait():
            if "code" not in result:
                self.root.after(500, wait)
            elif result["code"] == 0 and self._reconnect_mt5():
                messagebox.showinfo("Conta de demonstração", "Conta aberta e MetaTrader 5 conectado.")
            else:
                self.refresh_mt5_status()
                messagebox.showwarning(
                    "Conta de demonstração",
                    "Não consegui abrir a conta sozinho.\n\nAbra pelo MetaTrader 5 (Arquivo > Abrir uma conta) "
                    "e depois clique em \"Verificar conexão\".")

        threading.Thread(target=work, daemon=True).start()
        self.root.after(500, wait)

    def check_mt5_status(self):
        """Verifica a conexão com o MT5 e, se não houver, oferece a correção adequada."""
        try:
            if self._offline_mode():
                return self._show_offline_status()
            if not self.app.mt5_connector:
                self.update_status("Status: MT5 não inicializado")
                return
            if self.refresh_mt5_status():
                if os.name == "nt":
                    self._check_admin_mode()
                return

            if self._missing_account():
                self._offer_demo_account()
                return
            self.connect_mt5()          # em segundo plano; se não conectar, diz o motivo e o que fazer
        except tk.TclError as e:        # a janela foi fechada no meio da verificação: nada a atualizar
            log.debug(f"Verificação de conexão interrompida: {e}")
        except Exception as e:
            log.error(f"Erro ao verificar status do MT5: {e}")
            try:
                self.update_status(f"Erro ao verificar status: {str(e)}")
                self.mt5_status_value.config(text="Erro ao verificar a conexão")
                self.mt5_status_dot.config(foreground=self.theme.colors["bad"])
            except tk.TclError:
                pass
    
    def _check_admin_mode(self):
        """Verifica se o MT5 está em modo de administrador."""
        if self._offline_mode():
            return self._show_offline_status()
        if not hasattr(self.app.mt5_connector, 'is_mt5_running_as_admin'):
            return
            
        try:
            if self.app.mt5_connector.is_mt5_running_as_admin():
                self.update_status("Status: MT5 em execução como administrador")
                messagebox.showinfo("MT5", "O MetaTrader 5 está em execução com privilégios de administrador.")
            else:
                self.update_status("Status: MT5 em execução sem privilégios de administrador")
                
                response = messagebox.askyesno(
                    "Permissões limitadas",
                    "O MetaTrader 5 está em execução sem privilégios de administrador.\n\n"
                    "Isso pode limitar o acesso a dados históricos.\n\n"
                    "Deseja reiniciar o MT5 com privilégios de administrador?"
                )
                
                if response:
                    self.launch_mt5_as_admin()
        except Exception as e:
            log.error(f"Erro ao verificar modo de administrador: {e}")
            self.update_status(f"Erro ao verificar modo de administrador: {str(e)}")

# Exemplo de como seria chamado em app.py (não executar este arquivo diretamente)
# if __name__ == "__main__":
#     # Este bloco não deve ser executado diretamente,
#     # UIManager depende de uma instância de MT5Extracao.
#     pass
