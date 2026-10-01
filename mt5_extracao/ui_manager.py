import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, filedialog
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
from mt5_extracao.data_exporter import DataExporter
from mt5_extracao.error_handler import with_error_handling, ExportError

# Garantir que o diretório de logs existe
os.makedirs("logs", exist_ok=True)

# Configuração de logging (pode ser centralizada depois)
log = logging.getLogger(__name__)
if not log.handlers:
    log.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    # Adicionar um handler de console para depuração inicial
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    log.addHandler(ch)
    # Adicionar um handler de arquivo
    fh = logging.FileHandler("logs/ui_manager.log", encoding="utf-8")
    fh.setFormatter(formatter)
    log.addHandler(fh)

class UIManager:
    """
    Gerencia a interface gráfica (Tkinter) da aplicação MT5 Extração.
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
        self.symbols_listbox = None
        self.selected_listbox = None
        self.start_button = None
        self.stop_button = None
        self.search_var = tk.StringVar() # Variável para busca de símbolos

        # Inicializar variáveis para símbolos com dados
        self.existing_symbols_map = {}
        self.symbols_with_data = set()

        # Adicionar atributos para favoritos
        self.favorites_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config", "favorites.json")
        self.favorite_symbols = self.load_favorites() or []
        
        # Configurar diretório de config se não existir
        config_dir = os.path.dirname(self.favorites_file)
        if not os.path.exists(config_dir):
            os.makedirs(config_dir, exist_ok=True)

        log.info("UIManager inicializado.")

    def setup_ui(self):
        """Configura a interface do usuário completa."""
        self.root = self.app.root
        
        # Configurar o estilo ttk
        self.style = ttk.Style()
        self.style.configure("TButton", font=("Segoe UI", 10))
        self.style.configure("TLabel", font=("Segoe UI", 10))
        self.style.configure("TFrame", background="#f0f0f0")
        
        # Configurar Progress Bar Styles
        self.style.configure("Collection.Horizontal.TProgressbar", 
                        foreground='#4CAF50', background='#4CAF50')
        self.style.configure("Error.Horizontal.TProgressbar", 
                        foreground='#F44336', background='#F44336')
                      
        # Frame principal para organizar conteúdo
        main_frame = ttk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Frame para busca de símbolos e status
        top_frame = ttk.Frame(main_frame)
        top_frame.pack(fill=tk.X, expand=False, pady=(0, 5))
        
        # Frame de status
        status_frame = ttk.Frame(top_frame)
        status_frame.pack(side=tk.LEFT, fill=tk.Y)
        
        # Label de status geral
        self.status_label = ttk.Label(status_frame, text="Status: Iniciando...")
        self.status_label.pack(anchor="w", pady=2)
        
        # Label de status MT5
        mt5_status_frame = ttk.Frame(status_frame)
        mt5_status_frame.pack(fill=tk.X, pady=5)
        
        mt5_status_label = ttk.Label(mt5_status_frame, text="Status MT5:", width=10)
        mt5_status_label.pack(side=tk.LEFT)
        
        self.mt5_status_value = ttk.Label(mt5_status_frame, text="Desconectado", width=15)
        self.mt5_status_value.pack(side=tk.LEFT)
        
        # Botão de administrador para MT5
        self.mt5_admin_button = ttk.Button(mt5_status_frame, text="Iniciar MT5 Admin", 
                                     command=self.launch_mt5_as_admin)
        self.mt5_admin_button.pack(side=tk.LEFT, padx=5)
        
        # Verificar status como administrador
        check_mt5_admin_status_btn = ttk.Button(mt5_status_frame, text="Verificar Status", 
                                          width=15, command=self.check_mt5_status)
        check_mt5_admin_status_btn.pack(side=tk.LEFT, padx=5)
        
        # Frame de busca
        search_frame = ttk.Frame(top_frame)
        search_frame.pack(side=tk.RIGHT, fill=tk.Y)
        
        search_label = ttk.Label(search_frame, text="Buscar:")
        search_label.pack(side=tk.LEFT, padx=(0, 5))
        
        self.search_var = tk.StringVar()
        self.search_var.trace("w", self.filter_symbols)
        search_entry = ttk.Entry(search_frame, textvariable=self.search_var, width=30)
        search_entry.pack(side=tk.LEFT)
        
        # Frame do meio que contém as listboxes
        middle_frame = ttk.Frame(main_frame)
        middle_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        middle_frame.columnconfigure(0, weight=1)
        middle_frame.columnconfigure(1, weight=0)
        middle_frame.columnconfigure(2, weight=1)
        
        # Frame esquerdo com lista de todos os símbolos
        left_frame = ttk.Frame(middle_frame)
        left_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 5))
        
        symbols_label = ttk.Label(left_frame, text="Símbolos Disponíveis:")
        symbols_label.pack(fill=tk.X, pady=(0, 5))
        
        # Adicionando opções para favoritos
        favorites_frame = ttk.Frame(left_frame)
        favorites_frame.pack(fill=tk.X, pady=(0, 5))
        
        add_favorite_btn = ttk.Button(favorites_frame, text="★ Adicionar aos Favoritos", 
                                 command=self.add_to_favorites)
        add_favorite_btn.pack(side=tk.LEFT, padx=(0, 5))
        
        remove_favorite_btn = ttk.Button(favorites_frame, text="☆ Remover dos Favoritos", 
                                    command=self.remove_from_favorites)
        remove_favorite_btn.pack(side=tk.LEFT)
        
        # Lista de símbolos disponíveis
        self.symbols_listbox = tk.Listbox(left_frame, selectmode=tk.EXTENDED, height=20, exportselection=False)
        self.symbols_listbox.pack(fill=tk.BOTH, expand=True)
        self.symbols_listbox.bind('<<ListboxSelect>>', self.on_symbol_select)
        
        # Scrollbar para a lista de símbolos
        symbols_scrollbar = ttk.Scrollbar(left_frame, orient="vertical", command=self.symbols_listbox.yview)
        symbols_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.symbols_listbox.config(yscrollcommand=symbols_scrollbar.set)
        
        # Criar botões para mover entre listas
        buttons_frame = ttk.Frame(middle_frame)
        buttons_frame.grid(row=0, column=1, sticky="ns")
        
        add_btn = ttk.Button(buttons_frame, text="➡", command=self.add_symbols)
        add_btn.pack(pady=5)
        
        remove_btn = ttk.Button(buttons_frame, text="⬅", command=self.remove_symbols)
        remove_btn.pack(pady=5)
        
        self._botoes_adicionados = True
        
        # Frame direito (controles e monitoramento)
        right_frame = ttk.Frame(middle_frame)
        right_frame.grid(row=0, column=2, sticky="nsew", padx=(5, 0))

        # Frame para símbolos selecionados
        selected_frame = ttk.LabelFrame(right_frame, text="Símbolos Selecionados")
        selected_frame.pack(fill=tk.BOTH, expand=False, pady=5)

        selected_list_frame = ttk.Frame(selected_frame)
        selected_list_frame.pack(fill=tk.BOTH, expand=True)

        scrollbar2 = ttk.Scrollbar(selected_list_frame)
        scrollbar2.pack(side=tk.RIGHT, fill=tk.Y)

        # self.selected_listbox é atributo do UIManager
        self.selected_listbox = tk.Listbox(selected_list_frame, selectmode=tk.EXTENDED, yscrollcommand=scrollbar2.set)
        self.selected_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar2.config(command=self.selected_listbox.yview)
        # Preenche com símbolos já selecionados (se houver, ao iniciar)
        for symbol in self.app.selected_symbols:
             self.selected_listbox.insert(tk.END, symbol)
        
        # Adicionar binding para mostrar detalhes dos dados quando um símbolo selecionado é clicado
        self.selected_listbox.bind('<<ListboxSelect>>', self.on_selected_symbol_select)

        # Frame para detalhes do símbolo selecionado
        self.symbol_details_frame = ttk.LabelFrame(right_frame, text="Detalhes do Símbolo")
        self.symbol_details_frame.pack(fill=tk.X, expand=False, pady=5)
        
        # Conteúdo inicial do frame de detalhes
        self.symbol_details_content = ttk.Label(self.symbol_details_frame, 
                                              text="Selecione um símbolo para ver detalhes",
                                              wraplength=500, justify="left")
        self.symbol_details_content.pack(fill=tk.X, padx=10, pady=10)

        # Frame de progresso da coleta (inicialmente oculto)
        self.progress_frame = ttk.LabelFrame(right_frame, text="Progresso da Coleta")
        
        # Criar elementos para o progresso
        self.progress_var = tk.DoubleVar(value=0)
        self.progress_bar = ttk.Progressbar(self.progress_frame, variable=self.progress_var, 
                                           style="Collection.Horizontal.TProgressbar",
                                           length=100, mode='determinate')
        self.progress_bar.pack(fill=tk.X, padx=10, pady=(10, 0))
        
        # Status da coleta
        self.collection_status_frame = ttk.Frame(self.progress_frame)
        self.collection_status_frame.pack(fill=tk.X, padx=10, pady=5)
        
        # Informações da coleta (lado esquerdo)
        self.collection_info_frame = ttk.Frame(self.collection_status_frame)
        self.collection_info_frame.pack(side=tk.LEFT, fill=tk.Y)
        
        self.collection_time_label = ttk.Label(self.collection_info_frame, text="Tempo: 00:00:00")
        self.collection_time_label.pack(anchor="w")
        
        self.collection_count_label = ttk.Label(self.collection_info_frame, text="Registros: 0")
        self.collection_count_label.pack(anchor="w")
        
        self.collection_errors_label = ttk.Label(self.collection_info_frame, text="Erros: 0")
        self.collection_errors_label.pack(anchor="w")
        
        # Último status (lado direito)
        self.last_status_frame = ttk.Frame(self.collection_status_frame)
        self.last_status_frame.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.last_symbol_label = ttk.Label(self.last_status_frame, text="Último símbolo: -")
        self.last_symbol_label.pack(anchor="e")
        
        self.last_time_label = ttk.Label(self.last_status_frame, text="Último registro: -")
        self.last_time_label.pack(anchor="e")
        
        # Progresso por símbolo (lista)
        self.symbols_progress_frame = ttk.Frame(self.progress_frame)
        self.symbols_progress_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))
        
        # A lista de progresso por símbolo será criada dinamicamente
        # Esconder o frame de progresso inicialmente - será mostrado durante coleta
        # self.progress_frame.pack_forget()

        # Controles de coleta
        control_frame = ttk.LabelFrame(right_frame, text="Controle de Coleta")
        control_frame.pack(fill=tk.BOTH, expand=False, pady=5)

        # self.start_button e self.stop_button são atributos do UIManager
        # commands chamam métodos do UIManager
        self.start_button = ttk.Button(control_frame, text="Iniciar Coleta", command=self.start_collection)
        self.start_button.grid(row=0, column=0, padx=5, pady=5, sticky="ew")

        self.stop_button = ttk.Button(control_frame, text="Parar Coleta", command=self.stop_collection, state=tk.DISABLED)
        self.stop_button.grid(row=0, column=1, padx=5, pady=5, sticky="ew")

        # command chama método do UIManager
        self.show_stats_button = ttk.Button(control_frame, text="Mostrar Estatísticas", command=self.show_statistics)
        self.show_stats_button.grid(row=0, column=2, padx=5, pady=5, sticky="ew")

        # command chama método do UIManager
        self.get_history_button = ttk.Button(control_frame, text="Extrair Dados Históricos", command=self.extract_historical_data)
        self.get_history_button.grid(row=1, column=0, columnspan=3, padx=5, pady=5, sticky="ew")
        
        # Frame para logs e status
        log_frame = ttk.LabelFrame(right_frame, text="Log de Atividades")
        log_frame.pack(fill=tk.BOTH, expand=True, pady=5)

        log_scroll = ttk.Scrollbar(log_frame)
        log_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        # self.log_text é atributo do UIManager
        self.log_text = tk.Text(log_frame, height=10, width=50, yscrollcommand=log_scroll.set)
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        log_scroll.config(command=self.log_text.yview)

        # Configurar largura das colunas do main_frame
        main_frame.columnconfigure(0, weight=1) # Frame esquerdo
        main_frame.columnconfigure(1, weight=3) # Frame direito

        # Inicializar log (usando método do UIManager)
        self.log("Interface gráfica configurada.")
        
        # Carregar símbolos na lista imediatamente
        self.filter_symbols()
        
        # Verificar e carregar dados existentes para destaque visual
        self.load_existing_symbols_data()

        # --- Fim da configuração da UI ---

    def load_existing_symbols_data(self):
        """Carrega informações sobre símbolos que já têm dados no banco para destacar na UI."""
        if not self.app.db_manager or not self.app.db_manager.is_connected():
            log.warning("Não foi possível verificar símbolos existentes: Banco de dados não conectado")
            return
            
        try:
            # Obter lista de tabelas existentes do banco
            existing_tables = self.app.db_manager.get_existing_symbols()
            log.info(f"Encontradas {len(existing_tables)} tabelas com dados no banco")
            
            # Lista para mapear entre nomes normalizados e originais
            self.existing_symbols_map = {}
            # Inicializar symbols_with_data como um conjunto para pesquisa rápida
            self.symbols_with_data = set()
            
            # Para cada símbolo disponível, verificar se há dados
            for symbol in self.app.symbols:
                # Verificar para o timeframe M1 (1 minuto)
                table_name = self.app.db_manager.get_table_name_for_symbol(symbol, "1 minuto")
                
                if table_name in existing_tables:
                    self.existing_symbols_map[symbol] = table_name
                    self.symbols_with_data.add(symbol)
            
            log.info(f"Identificados {len(self.existing_symbols_map)} símbolos com dados existentes")
            
            # Atualizar a interface com destaques visuais
            self.highlight_symbols_with_data()
        except Exception as e:
            log.error(f"Erro ao verificar símbolos existentes: {e}")
            log.debug(traceback.format_exc())
            # Garantir que essas propriedades existam mesmo em caso de erro
            if not hasattr(self, "existing_symbols_map"):
                self.existing_symbols_map = {}
            if not hasattr(self, "symbols_with_data"):
                self.symbols_with_data = set()
        
    def highlight_symbols_with_data(self):
        """Destaca símbolos que já possuem dados no banco de dados."""
        if not self.symbols_listbox or not hasattr(self, "symbols_with_data"):
            return
            
        # Atualiza formatação para destacar os que têm dados
        for i in range(self.symbols_listbox.size()):
            symbol_text = self.symbols_listbox.get(i)
            # Remove o prefixo de favorito se existir
            if symbol_text.startswith("★ "):
                symbol = symbol_text[2:]
                is_favorite = True
            else:
                symbol = symbol_text
                is_favorite = False
                
            # Configurações padrão
            bg_color = 'white'
            fg_color = 'black'
            
            # Definir cores com base nas condições
            if is_favorite and symbol in self.symbols_with_data:
                # Favorito com dados - Destaque azul com fundo verde claro
                bg_color = '#E8F5E9'  # Verde claro
                fg_color = '#1976D2'  # Azul
            elif is_favorite:
                # Apenas favorito - Azul
                fg_color = '#1976D2'  # Azul
            elif symbol in self.symbols_with_data:
                # Apenas com dados - Fundo verde claro
                bg_color = '#E8F5E9'  # Verde claro
            
            # Aplicar as cores
            self.symbols_listbox.itemconfig(i, {'bg': bg_color, 'fg': fg_color})
        
        # Destacar símbolos na listbox de selecionados
        for i in range(self.selected_listbox.size()):
            symbol = self.selected_listbox.get(i)
            if symbol in self.existing_symbols_map:
                self.selected_listbox.itemconfig(i, {'fg': 'green', 'bg': '#f0f8f0'})
            
    def on_symbol_select(self, event):
        """Manipula o evento de seleção na lista de símbolos disponíveis."""
        if not self.symbols_listbox:
            return
        try:
            selection = self.symbols_listbox.curselection()
            if selection:
                symbol_text = self.symbols_listbox.get(selection[0])
                # Remove o prefixo de favorito se existir
                if symbol_text.startswith("★ "):
                    symbol = symbol_text[2:]
                else:
                    symbol = symbol_text
                
                self.update_symbol_details(symbol)
        except Exception as e:
            log.error(f"Erro ao selecionar símbolo: {e}")
        
    def on_selected_symbol_select(self, event):
        """Callback quando um símbolo é selecionado na lista de selecionados."""
        try:
            selection = self.selected_listbox.curselection()
            if selection:
                symbol_text = self.selected_listbox.get(selection[0])
                # Remove o prefixo de favorito se existir
                if symbol_text.startswith("★ "):
                    symbol = symbol_text[2:]
                else:
                    symbol = symbol_text
                
                self.update_symbol_details(symbol)
        except Exception as e:
            log.error(f"Erro ao selecionar símbolo: {e}")
        
    def update_symbol_details(self, symbol):
        """Atualiza o painel de detalhes com informações do símbolo selecionado."""
        if not hasattr(self, 'symbol_details_content') or not symbol:
            return
            
        try:
            # Verificar se o símbolo tem dados existentes
            has_data = False
            table_name = None
            
            if hasattr(self, 'existing_symbols_map'):
                if symbol in self.existing_symbols_map:
                    table_name = self.existing_symbols_map[symbol]
                    has_data = True
            else:
                # Se não tiver o mapa, tentar obter o nome da tabela diretamente
                if self.app.db_manager:
                    table_name = self.app.db_manager.get_table_name_for_symbol(symbol, "1 minuto")
                    # Verificar se a tabela existe
                    if table_name in self.app.db_manager.get_existing_symbols():
                        has_data = True
            
            # Se tem dados, mostrar resumo
            if has_data and table_name and self.app.db_manager:
                try:
                    summary = self.app.db_manager.get_symbol_data_summary(table_name)
                    
                    if summary:
                        # Formatar texto com informações resumidas
                        detail_text = f"""Símbolo: {symbol}
                        
Dados Existentes:
• Período: {summary.get('data_inicio', 'N/A')} até {summary.get('data_fim', 'N/A')}
• Total de registros: {summary.get('total_registros', 0)}
• Intervalo médio: {summary.get('intervalo_medio_minutos', 'N/A')} minutos
• Completude dos dados: {summary.get('completude', 0)}%

Meta Trader 5:
• Spread: {self.get_symbol_spread(symbol)}
• Cotação: {self.get_symbol_price(symbol)}

Status: Disponível para coleta e análise
"""
                        self.symbol_details_content.config(text=detail_text)
                        self.symbol_details_frame.config(text=f"Detalhes de {symbol} (Dados Existentes)")
                        return
                except Exception as e:
                    log.error(f"Erro ao obter resumo para {symbol}: {e}")
                    log.debug(traceback.format_exc())
            
            # Se não tiver dados ou houver erro, mostrar informações básicas
            detail_text = f"""Símbolo: {symbol}

Meta Trader 5:
• Spread: {self.get_symbol_spread(symbol)}
• Cotação: {self.get_symbol_price(symbol)}

Status: Sem dados no banco. Disponível para coleta.
"""
            self.symbol_details_content.config(text=detail_text)
            self.symbol_details_frame.config(text=f"Detalhes de {symbol}")
        except Exception as e:
            log.error(f"Erro ao atualizar detalhes do símbolo {symbol}: {e}")
            log.debug(traceback.format_exc())
            # Em caso de erro, mostrar mensagem simples
            self.symbol_details_content.config(text=f"Símbolo: {symbol}\n\nErro ao carregar detalhes.")
            self.symbol_details_frame.config(text=f"Detalhes de {symbol} (Erro)")

    def get_symbol_spread(self, symbol):
        """Obtém o spread atual do símbolo via MT5."""
        if (not self.app.mt5_initialized or not self.app.mt5_connector or 
            not self.app.mt5_connector.is_initialized):
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
        if (not self.app.mt5_initialized or not self.app.mt5_connector or 
            not self.app.mt5_connector.is_initialized):
            return "N/A (MT5 não conectado)"
            
        try:
            symbol_info = self.app.mt5_connector.get_symbol_info(symbol)
            if symbol_info and hasattr(symbol_info, 'last'):
                return f"{symbol_info.last:.5f}"
            return "N/A"
        except Exception as e:
            log.error(f"Erro ao obter cotação para {symbol}: {e}")
            return "Erro ao obter cotação"
            
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
        # Garantir que existe uma área na interface para mostrar o progresso
        if not hasattr(self, 'progress_frame') or not hasattr(self, 'progress_var'):
            log.warning("Interface de progresso não inicializada.")
            return
            
        # Garantir que o frame de progresso esteja visível
        if not self.progress_frame.winfo_ismapped():
            # Tentar localizar o frame após o qual inserir
            after_frame = None
            if hasattr(self, 'symbol_details_frame') and self.symbol_details_frame.winfo_ismapped():
                after_frame = self.symbol_details_frame
            elif hasattr(self, 'control_frame') and getattr(self, 'control_frame', None):
                after_frame = self.control_frame
                
            if after_frame:
                self.progress_frame.pack(fill=tk.X, expand=False, pady=5, after=after_frame)
            else:
                self.progress_frame.pack(fill=tk.X, expand=False, pady=5)
            
        # Atualizar barra de progresso principal (baseado na taxa de sucesso)
        if total_collected > 0:
            success_rate = (total_success / total_collected) * 100
            self.progress_var.set(success_rate)
            
            # Atualizar estilo baseado na taxa de sucesso
            if success_rate < 50:
                self.progress_bar.configure(style="Error.Horizontal.TProgressbar")
            else:
                self.progress_bar.configure(style="Collection.Horizontal.TProgressbar")
        else:
            self.progress_var.set(0)
            
        # Atualizar labels de informação
        # Formato de tempo hh:mm:ss
        hours, remainder = divmod(int(elapsed_time), 3600)
        minutes, seconds = divmod(remainder, 60)
        time_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        self.collection_time_label.config(text=f"Tempo: {time_str}")
        
        self.collection_count_label.config(text=f"Registros: {total_success} de {total_collected}")
        self.collection_errors_label.config(text=f"Erros: {total_errors}")
        
        # Encontrar último símbolo processado e último timestamp
        last_symbol = "-"
        last_time = "-"
        
        for symbol, status in symbols_status.items():
            if status['last_time'] is not None:
                last_symbol = symbol
                if isinstance(status['last_time'], (str, datetime.datetime)):
                    if isinstance(status['last_time'], datetime.datetime):
                        last_time = status['last_time'].strftime("%H:%M:%S")
                    else:
                        last_time = status['last_time']
                        
        self.last_symbol_label.config(text=f"Último símbolo: {last_symbol}")
        self.last_time_label.config(text=f"Último registro: {last_time}")
        
        # Limpar e recriar a área de progresso por símbolo
        for widget in self.symbols_progress_frame.winfo_children():
            widget.destroy()
            
        # Criar barras de progresso por símbolo (max 5 símbolos)
        row = 0
        for symbol, status in list(symbols_status.items())[:5]:  # Limita a 5 símbolos para não sobrecarregar a UI
            if status['total'] > 0:
                # Frame para o símbolo
                symbol_frame = ttk.Frame(self.symbols_progress_frame)
                symbol_frame.grid(row=row, column=0, sticky="ew", pady=2)
                symbol_frame.columnconfigure(1, weight=1)
                
                # Label do símbolo
                symbol_label = ttk.Label(symbol_frame, text=f"{symbol}:", width=10, anchor="w")
                symbol_label.grid(row=0, column=0, padx=(0, 5))
                
                # Barra de progresso
                symbol_progress_var = tk.DoubleVar(value=(status['success'] / status['total']) * 100 if status['total'] > 0 else 0)
                symbol_progress = ttk.Progressbar(symbol_frame, variable=symbol_progress_var, length=100)
                symbol_progress.grid(row=0, column=1, sticky="ew")
                
                # Texto de status
                status_text = f"{status['success']}/{status['total']}"
                if status['last_error']:
                    status_text += f" (Erro: {status['last_error']})"
                    symbol_progress.configure(style="Error.Horizontal.TProgressbar")
                
                status_label = ttk.Label(symbol_frame, text=status_text, width=20, anchor="e")
                status_label.grid(row=0, column=2, padx=(5, 0))
                
                row += 1
                
        # Se não estiver mais rodando, desabilitar alguns elementos
        if not is_running:
            # Manter a interface visível para mostrar o resultado final
            self.progress_frame.pack_forget()

    def filter_symbols(self, *args):
        """Filtra a lista de símbolos com base no texto de busca"""
        if not self.symbols_listbox: # Verifica se o widget existe
            return
            
        # Primeiro, verificar se temos símbolos disponíveis
        if not hasattr(self.app, 'symbols') or not self.app.symbols:
            log.warning("Nenhum símbolo disponível para filtrar")
            messagebox.showwarning("Aviso", "Nenhum símbolo disponível para exibir.")
            return
            
        search_text = self.search_var.get().lower()
        self.symbols_listbox.delete(0, tk.END)
        
        # Primeiro adicionar favoritos
        favorites_count = 0
        for symbol in self.app.symbols:
            if symbol in self.favorite_symbols and (search_text == '' or search_text in symbol.lower()):
                self.symbols_listbox.insert(tk.END, f"★ {symbol}")
                favorites_count += 1
        
        # Depois adicionar o resto dos símbolos
        other_count = 0
        for symbol in self.app.symbols:
            if symbol not in self.favorite_symbols and (search_text == '' or search_text in symbol.lower()):
                self.symbols_listbox.insert(tk.END, symbol)
                other_count += 1
        
        log.info(f"Filtro aplicado: {favorites_count} favoritos e {other_count} outros símbolos exibidos")
                
        # Destacar símbolos com dados existentes
        self.highlight_symbols_with_data()
        
        # Se não houver símbolos exibidos, mostrar mensagem
        if self.symbols_listbox.size() == 0:
            messagebox.showinfo("Busca", "Nenhum símbolo corresponde ao filtro de busca.")

    def add_symbols(self):
        """Adiciona símbolos selecionados à lista de monitoramento"""
        if not self.symbols_listbox or not self.selected_listbox:
            log.error("Widgets necessários não foram inicializados")
            return
            
        selected_indices = self.symbols_listbox.curselection()
        
        if not selected_indices:
            messagebox.showinfo("Seleção", "Selecione pelo menos um símbolo para adicionar.")
            return
            
        added_count = 0
        for i in selected_indices:
            symbol_text = self.symbols_listbox.get(i)
            # Remove o prefixo de favorito se existir
            if symbol_text.startswith("★ "):
                symbol = symbol_text[2:]
            else:
                symbol = symbol_text
                
            # Modifica a lista na instância principal
            if symbol not in self.app.selected_symbols:
                self.app.selected_symbols.append(symbol)
                # Adicionar o símbolo com o prefixo de favorito se ele for um favorito
                if symbol in self.favorite_symbols:
                    self.selected_listbox.insert(tk.END, f"★ {symbol}")
                else:
                    self.selected_listbox.insert(tk.END, symbol)
                added_count += 1
                self.log(f"Símbolo adicionado: {symbol}")
                
        if added_count > 0:
            self.log(f"{added_count} símbolo(s) adicionado(s) à lista de selecionados")
            # Aplicar destaque para símbolos com dados
            if hasattr(self, 'symbols_with_data'):
                for i in range(self.selected_listbox.size()):
                    symbol_text = self.selected_listbox.get(i)
                    # Remove o prefixo de favorito se existir
                    if symbol_text.startswith("★ "):
                        symbol = symbol_text[2:]
                    else:
                        symbol = symbol_text
                        
                    if symbol in self.symbols_with_data:
                        self.selected_listbox.itemconfig(i, {'fg': 'green', 'bg': '#f0f8f0'})
        else:
            self.log("Nenhum novo símbolo adicionado (já existem na lista)")
            
        # Criar botões para adicionar/remover símbolos se não existirem
        if not hasattr(self, '_botoes_adicionados') or not self._botoes_adicionados:
            self._adicionar_botoes_selecao()
            
    def _adicionar_botoes_selecao(self):
        """Adiciona botões para mover símbolos entre as listas"""
        # Obter o frame do meio que contém as listboxes
        middle_frame = None
        for child in self.root.winfo_children():
            if isinstance(child, ttk.Frame):
                for grandchild in child.winfo_children():
                    if isinstance(grandchild, ttk.Frame) and len(grandchild.winfo_children()) >= 3:
                        middle_frame = grandchild
                        break
                if middle_frame:
                    break
                    
        if not middle_frame:
            log.error("Não foi possível encontrar o frame do meio para adicionar botões")
            return
            
        # Criar frame para os botões
        buttons_frame = ttk.Frame(middle_frame)
        buttons_frame.grid(row=0, column=1, sticky="ns")
        
        # Botão para adicionar símbolo selecionado
        add_btn = ttk.Button(buttons_frame, text="➡", command=self.add_symbols)
        add_btn.pack(pady=5)
        
        # Botão para remover símbolo selecionado
        remove_btn = ttk.Button(buttons_frame, text="⬅", command=self.remove_symbols)
        remove_btn.pack(pady=5)
        
        self._botoes_adicionados = True

    def remove_symbols(self):
        """Remove símbolos da lista de monitoramento"""
        if not self.selected_listbox:
            return
        selected_indices = self.selected_listbox.curselection()

        # Remove em ordem reversa para não afetar os índices
        for i in sorted(selected_indices, reverse=True):
            symbol_text = self.selected_listbox.get(i)
            # Remove o prefixo de favorito se existir
            if symbol_text.startswith("★ "):
                symbol = symbol_text[2:]
            else:
                symbol = symbol_text
                
            # Modifica a lista na instância principal
            if symbol in self.app.selected_symbols:
                 self.app.selected_symbols.remove(symbol) # Linha duplicada removida
                 # Linhas 181-182 movidas para cá:
                 self.selected_listbox.delete(i)
                 self.log(f"Símbolo removido: {symbol}") # Usa self.log

    def update_log_widget(self, message):
        """Atualiza o widget de log (text) com uma nova mensagem."""
        if self.log_text:
            # Garantir que estamos no fim do log
            self.log_text.see(tk.END)
            # Adicionar timestamp e mensagem
            timestamp = datetime.datetime.now().strftime("%H:%M:%S")
            self.log_text.insert(tk.END, f"[{timestamp}] {message}\n")
            # Manter o texto visível no final
            self.log_text.see(tk.END)
        else:
            log.warning(f"Tentativa de log na UI sem widget log_text: {message}")
            
    def log(self, message):
        """Atualiza o widget de log na UI com uma nova mensagem."""
        # Registra no logger do sistema
        log.info(message)
        # Atualiza a UI
        self.update_log_widget(message)

    def update_status(self, status_text):
        """Atualiza o texto de status principal."""
        # Sempre registrar o status no log
        log.info(f"Status: {status_text}")
        
        # Atualizar widget apenas se estiver disponível
        if hasattr(self, 'status_label') and self.status_label:
            try:
                self.status_label.config(text=status_text)
            except Exception as e:
                log.warning(f"Erro ao atualizar widget de status: {e}")
                # Não vamos exibir a mensagem de aviso sobre widget ausente, apenas logar
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
                           "Consulte o arquivo mt5_app.log para mais detalhes.") # Aspas corrigidas
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
        if hasattr(self.app.mt5_connector, 'is_mt5_running_as_admin') and self.app.mt5_connector._is_mt5_running():
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
        config_window.geometry("500x450")
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
        ttk.Checkbutton(period_frame, text="Auto-detectar data mais antiga", variable=auto_detect_var).grid(row=1, column=2, padx=5, pady=5, sticky="w")

        # Botão para detectar a data mais antiga (com debug)
        log.info("Criando botão 'Detectar Agora'")  # Debug log
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
        ttk.Label(options_frame, text="Máximo de Barras:").pack(anchor="w")
        max_bars_var = tk.StringVar(value="10000")
        ttk.Entry(options_frame, textvariable=max_bars_var, width=10).pack(anchor="w")

        # Símbolos Selecionados (Display)
        symbols_frame = ttk.LabelFrame(main_frame, text=f"Símbolos Selecionados ({len(self.app.selected_symbols)})")
        symbols_frame.pack(fill=tk.BOTH, expand=True, pady=10)
        symbols_text = tk.Text(symbols_frame, height=4)
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
        
        # Botões de ação na parte inferior
        buttons_frame = ttk.Frame(main_frame)
        buttons_frame.pack(fill=tk.X, pady=10)
        
        start_button = ttk.Button(buttons_frame, text="Iniciar Extração", 
                               command=lambda: start_extraction_logic())
        start_button.pack(side=tk.LEFT, padx=5)
        
        cancel_button = ttk.Button(buttons_frame, text="Cancelar", 
                                command=lambda: cancel_extraction_logic())
        cancel_button.pack(side=tk.RIGHT, padx=5)
        
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
                        
                        status_var.set(f"Detectando data mais antiga para {symbol}...")
                        config_window.update_idletasks()
                        
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
                        global_oldest = min(oldest_dates.values())
                        formatted_date = global_oldest.strftime('%Y-%m-%d')
                        
                        self.log(f"Data mais antiga entre todos os símbolos: {formatted_date}")
                        status_var.set(f"Data mais antiga detectada: {formatted_date}")
                        
                        # Atualizar o campo de data inicial
                        start_date_entry.delete(0, tk.END)
                        start_date_entry.insert(0, formatted_date)
                    else:
                        status_var.set("Não foi possível determinar a data mais antiga")
                except Exception as e:
                    self.log(f"Erro na detecção de data mais antiga: {str(e)}")
                    status_var.set(f"Erro: {str(e)}")
                finally:
                    # Restaurar interface
                    detect_button.config(state=tk.NORMAL)
                    cancel_detect_button.grid_remove()
            
            # Iniciar thread de detecção
            detection_thread = threading.Thread(target=detection_thread, daemon=True)
            detection_thread.start()
        
        def cancel_detection():
            """Não é necessário implementar aqui, uma vez que agora é definido localmente
            dentro da função detect_oldest_date."""
            pass
        
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
                update_progress_callback=lambda p, msg: self.root.after_idle(lambda: (progress_var.set(p), status_var.set(msg))),
                # Ajustar callback para 3 args (success, failed, canceled)
                finished_callback=lambda s, f, c: self.root.after_idle(lambda: (extract_button.config(state=tk.NORMAL), cancel_button.config(text="Fechar")))
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

            def finished(s, f, c):
                extract_button.config(state=tk.NORMAL)
                update_button.config(state=tk.NORMAL)
                cancel_button.config(text="Fechar")

            self.app.historical_extractor.update_symbols(
                symbols=self.app.selected_symbols,
                timeframe_val=timeframe_val,
                timeframe_name=timeframe_name,
                include_indicators=include_indicators_var.get(),
                update_progress_callback=lambda p, msg: self.root.after_idle(lambda: (progress_var.set(p), status_var.set(msg))),
                finished_callback=lambda s, f, c: self.root.after_idle(lambda: finished(s, f, c))
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
        extract_button = ttk.Button(buttons_frame, text="Iniciar Extração", command=start_extraction_logic)
        extract_button.pack(side=tk.LEFT, padx=5)
        update_button = ttk.Button(buttons_frame, text="Atualizar até agora", command=update_extraction_logic)
        update_button.pack(side=tk.LEFT, padx=5)
        cancel_button = ttk.Button(buttons_frame, text="Cancelar", command=cancel_extraction_logic)
        cancel_button.pack(side=tk.RIGHT, padx=5)

    def show_statistics(self):
        """Cria uma nova janela para exibir estatísticas e gráficos."""
        if not self.app.selected_symbols:
            messagebox.showerror("Erro", "Nenhum símbolo selecionado para exibir estatísticas.")
            return

        # Verifica se o DatabaseManager está disponível
        if not self.app.db_manager or not self.app.db_manager.is_connected():
             messagebox.showerror("Erro", "Conexão com banco de dados não disponível.")
             return
             
        # Verifica se o MT5 está conectado (caso precise de dados em tempo real)
        mt5_conectado = False
        if (not self.app.mt5_initialized or not self.app.mt5_connector or 
            not self.app.mt5_connector.is_initialized):
            response = messagebox.askyesno(
                "MT5 Desconectado", 
                "O MT5 está desconectado. Alguns dados em tempo real podem não estar disponíveis. "
                "Deseja tentar reconectar ao MT5?"
            )
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
                    messagebox.showinfo("Conexão MT5", "Conexão com MT5 estabelecida com sucesso.")
                    mt5_conectado = True
                else:
                    # Se falhar na reconexão, continua apenas com dados do banco
                    logging.warning("Falha ao reconectar ao MT5. Mostrando apenas dados armazenados.")
                    self.log("AVISO: Falha ao reconectar ao MT5. Mostrando apenas dados armazenados.")
                    self.update_status("Status: Visualizando dados armazenados apenas")
            # Continua mesmo sem MT5 conectado, apenas com dados do banco
        else:
            mt5_conectado = True

        stats_window = tk.Toplevel(self.root)
        stats_window.title("Estatísticas")
        stats_window.geometry("800x600")
        
        # Adicionar barra de status na janela de estatísticas
        status_frame = ttk.Frame(stats_window)
        status_frame.pack(fill=tk.X, side=tk.BOTTOM, padx=5, pady=5)
        status_label = ttk.Label(status_frame, text="Carregando estatísticas...")
        status_label.pack(side=tk.LEFT)
        
        # Inicializar o notebook antes
        notebook = ttk.Notebook(stats_window)
        notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        dados_encontrados = False
        
        # Criamos uma função para atualizar dinamicamente o status
        def update_stats_status(text):
            status_label.config(text=text)
            stats_window.update_idletasks()
        
        # Processamos cada símbolo
        for symbol in self.app.selected_symbols:
            try:
                update_stats_status(f"Carregando dados para {symbol}...")
                
                # Verificar nome da tabela
                try:
                    table_name = self.app.db_manager.get_table_name_for_symbol(symbol, "1 minuto")
                    if not table_name:
                        self.log(f"Nome de tabela inválido para {symbol}")
                        update_stats_status(f"Erro: Nome de tabela inválido para {symbol}")
                        continue
                except Exception as table_name_error:
                    self.log(f"Erro ao obter nome da tabela para {symbol}: {str(table_name_error)}")
                    update_stats_status(f"Erro ao obter nome da tabela para {symbol}")
                    continue
                    
                # Verificar se a tabela existe
                try:
                    query_exists = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
                    table_exists = pd.read_sql(query_exists, self.app.db_manager.engine)
                    if table_exists.empty:
                        self.log(f"Tabela {table_name} não existe - sem dados para mostrar estatísticas")
                        update_stats_status(f"Sem dados para {symbol}")
                        continue
                except Exception as exists_error:
                    self.log(f"Erro ao verificar existência da tabela {table_name}: {str(exists_error)}")
                    update_stats_status(f"Erro ao verificar tabela {symbol}")
                    continue
                
                # Obter dados
                try:
                    query_data = f"SELECT * FROM {table_name} ORDER BY time DESC LIMIT 1000"
                    df = pd.read_sql(query_data, self.app.db_manager.engine)
                    
                    if df.empty:
                        self.log(f"Nenhum dado encontrado para {symbol}")
                        update_stats_status(f"Nenhum dado encontrado para {symbol}")
                        continue
                except Exception as data_error:
                    self.log(f"Erro ao ler dados da tabela {table_name}: {str(data_error)}")
                    update_stats_status(f"Erro ao ler dados de {symbol}")
                    continue
                
                dados_encontrados = True
                update_stats_status(f"Processando dados de {symbol}...")
                
                # Converter coluna de tempo
                try:
                    df['time'] = pd.to_datetime(df['time'])
                except Exception as e:
                    self.log(f"Erro ao converter coluna de tempo para {symbol}: {str(e)}")
                
                # Criar aba para este símbolo
                tab = ttk.Frame(notebook)
                notebook.add(tab, text=symbol)
                
                # Divisão em duas partes: texto e gráfico
                panel = ttk.PanedWindow(tab, orient=tk.VERTICAL)
                panel.pack(fill=tk.BOTH, expand=True)
                
                # Painel de texto
                text_frame = ttk.Frame(panel)
                panel.add(text_frame, weight=1)
                
                stats_text = tk.Text(text_frame, height=15, width=60)
                stats_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
                
                # Adicionar scrollbar
                text_scroll = ttk.Scrollbar(text_frame, command=stats_text.yview)
                text_scroll.pack(side=tk.RIGHT, fill=tk.Y)
                stats_text.config(yscrollcommand=text_scroll.set)

                # Calcular estatísticas
                try:
                    # Obter preço em tempo real se MT5 conectado
                    preço_atual = "N/A"
                    if mt5_conectado:
                        try:
                            symbol_info = self.app.mt5_connector.get_symbol_info(symbol)
                            if symbol_info and hasattr(symbol_info, 'last'):
                                preço_atual = symbol_info.last
                        except Exception as price_err:
                            log.warning(f"Erro ao obter preço atual de {symbol}: {price_err}")
                    
                    # Usar dados históricos se não conseguiu em tempo real
                    if preço_atual == "N/A" and not df.empty:
                        preço_atual = df['close'].iloc[0]
                    
                    stats = {
                        "Intervalo de Datas": f"{df['time'].min().strftime('%Y-%m-%d')} a {df['time'].max().strftime('%Y-%m-%d')}",
                        "Total de Registros": len(df),
                        "Preço Atual": preço_atual,
                        "Preço Máximo": df['high'].max(),
                        "Preço Mínimo": df['low'].min(),
                        "Média de Preço (Fechamento)": df['close'].mean(),
                    }
                    
                    # Adicionar estatísticas de indicadores se estiverem disponíveis
                    for column in ['atr', 'rsi', 'ma_20', 'volume']:
                        if column in df.columns:
                            if column == 'atr':
                                stats["Volatilidade (ATR médio)"] = df['atr'].mean()
                            elif column == 'rsi':
                                stats["RSI Atual"] = df['rsi'].iloc[0]
                            elif column == 'ma_20':
                                stats["Média Móvel 20 (atual)"] = df['ma_20'].iloc[0]
                            elif column == 'volume':
                                stats["Volume Total"] = df['volume'].sum()
                                stats["Volume Médio"] = df['volume'].mean()
                    
                    # Verificar padrões de candle se disponíveis
                    if 'candle_pattern' in df.columns:
                        patterns = df['candle_pattern'].value_counts().to_dict()
                        for pattern, count in patterns.items():
                            if pattern and pattern != "No Pattern":
                                stats[f"Padrão {pattern}"] = count
                except Exception as stats_error:
                    log.error(f"Erro ao calcular estatísticas para {symbol}: {stats_error}")
                    log.debug(traceback.format_exc())
                    stats = {"Erro": f"Falha ao calcular estatísticas: {str(stats_error)}"}

                # Exibir estatísticas
                stats_text.insert(tk.END, "ESTATÍSTICAS DO SÍMBOLO\n")
                stats_text.insert(tk.END, "=====================\n\n")
                for key, value in stats.items():
                    if isinstance(value, float):
                        value = f"{value:.5f}"
                    stats_text.insert(tk.END, f"{key}: {value}\n")

                # Painel do gráfico
                chart_frame = ttk.Frame(panel)
                panel.add(chart_frame, weight=2)

                # Gráfico (requer matplotlib e FigureCanvasTkAgg)
                try:
                    update_stats_status(f"Gerando gráfico para {symbol}...")
                    
                    try:
                        from matplotlib.figure import Figure
                        from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
                    except ImportError:
                        self.log("Matplotlib não instalado. Gráficos indisponíveis.")
                        stats_text.insert(tk.END, "\n\nAVISO: Matplotlib não instalado. Gráficos indisponíveis.")
                        continue

                    fig = Figure(figsize=(10, 4), dpi=100)
                    ax = fig.add_subplot(111)

                    plot_df = df.iloc[::-1].copy()
                    plot_df.set_index('time', inplace=True)

                    # Plotando preço de fechamento
                    ax.plot(plot_df.index, plot_df['close'], label='Preço de Fechamento')
                    
                    # Adicionar indicadores se disponíveis
                    for indicator, params in [
                        ('ma_20', {'label': 'MA 20', 'linestyle': '--', 'color': 'blue'}),
                        ('bollinger_upper', {'label': 'Bollinger Sup.', 'linestyle': ':', 'color': 'green'}),
                        ('bollinger_lower', {'label': 'Bollinger Inf.', 'linestyle': ':', 'color': 'red'})
                    ]:
                        if indicator in plot_df.columns:
                            ax.plot(
                                plot_df.index, 
                                plot_df[indicator], 
                                label=params['label'], 
                                linestyle=params['linestyle'],
                                color=params['color']
                            )

                    ax.set_title(f'Preço de {symbol}')
                    ax.set_ylabel('Preço')
                    ax.legend()
                    fig.autofmt_xdate()

                    canvas = FigureCanvasTkAgg(fig, master=chart_frame)
                    canvas.draw()
                    canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
                except Exception as plot_err:
                    self.log(f"Erro ao gerar gráfico para {symbol}: {str(plot_err)}")
                    log.error(f"Erro detalhado no gráfico: {traceback.format_exc()}")
            except Exception as e:
                self.log(f"Erro ao gerar estatísticas para {symbol}: {str(e)}")
                logging.error(f"Erro detalhado para {symbol}: {traceback.format_exc()}")

        # Atualização final de status
        if dados_encontrados:
            update_stats_status("Estatísticas carregadas com sucesso")
        else:
            messagebox.showinfo(
                "Sem dados",
                "Não foram encontrados dados para os símbolos selecionados.\n\n"
                "Execute a coleta de dados primeiro ou selecione outros símbolos."
            )
            stats_window.destroy()
    
    def launch_mt5_as_admin(self):
        """Inicia o MT5 como administrador (se suportado)."""
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
        
        # Menu File
        file_menu = tk.Menu(menu_bar, tearoff=0)
        file_menu.add_command(label="Configurações", command=self.open_settings)
        file_menu.add_separator()
        
        # Submenu de exportação
        export_menu = tk.Menu(file_menu, tearoff=0)
        export_menu.add_command(label="Exportar para CSV", command=lambda: self.export_data("csv"))
        export_menu.add_command(label="Exportar para Excel", command=lambda: self.export_data("excel"))
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
        
        # Menu Ajuda
        help_menu = tk.Menu(menu_bar, tearoff=0)
        help_menu.add_command(label="Documentação", command=self.open_documentation)
        help_menu.add_command(label="Sobre", command=self.show_about)
        
        # Menu Estratégias (pesquisa com os dados do banco; não envia ordens)
        strategy_menu = tk.Menu(menu_bar, tearoff=0)
        strategy_menu.add_command(label="Triagem de ativos (Filtros A e B)...",
                                  command=lambda: self.open_strategy_dialog("screen"))
        strategy_menu.add_command(label="Validar estratégias (Filtro C)...",
                                  command=lambda: self.open_strategy_dialog("validate"))
        strategy_menu.add_command(label="Gerar relatório e abrir no navegador...",
                                  command=lambda: self.open_strategy_dialog("report"))
        strategy_menu.add_separator()
        strategy_menu.add_command(label="Sobre as estratégias", command=self.show_strategies_info)

        # Adicionar menus à barra de menu
        menu_bar.add_cascade(label="Arquivo", menu=file_menu)
        menu_bar.add_cascade(label="Estratégias", menu=strategy_menu)
        menu_bar.add_cascade(label="Ajuda", menu=help_menu)
        
        # Configurar a barra de menu
        self.root.config(menu=menu_bar)
    
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
        
        # Estilo
        s = ttk.Style()
        s.configure("TFrame", padding=10)
        s.configure("TButton", padding=5)
        s.configure("TLabel", padding=5)
        
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
        table_listbox = tk.Listbox(list_frame, yscrollcommand=scrollbar.set, height=10)
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
                
            table_name = table_listbox.get(selected_idx[0])
            filters = filters_entry.get()
            
            # Criar instância do exportador se não existir
            if not hasattr(self, 'data_exporter'):
                self.data_exporter = DataExporter(self.app.db_manager)
                
            # Solicitar local para salvar
            if format_type.lower() == 'csv':
                file_path = filedialog.asksaveasfilename(
                    defaultextension=".csv",
                    filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
                    initialfile=f"{table_name}.csv"
                )
                
                if file_path:
                    try:
                        result_path = self.data_exporter.export_to_csv(
                            table_name, 
                            caminho_arquivo=file_path, 
                            filtros=filters if filters else None,
                            adicionar_timestamp=False
                        )
                        
                        if result_path:
                            messagebox.showinfo("Sucesso", f"Dados exportados com sucesso para:\n{result_path}")
                            export_dialog.destroy()
                        else:
                            messagebox.showwarning("Aviso", "Nenhum dado encontrado para exportar")
                    except Exception as e:
                        messagebox.showerror("Erro", f"Falha ao exportar: {str(e)}")
            
            elif format_type.lower() in ['excel', 'xlsx']:
                file_path = filedialog.asksaveasfilename(
                    defaultextension=".xlsx",
                    filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")],
                    initialfile=f"{table_name}.xlsx"
                )
                
                if file_path:
                    try:
                        result_path = self.data_exporter.export_to_excel(
                            table_name, 
                            caminho_arquivo=file_path, 
                            filtros=filters if filters else None,
                            adicionar_timestamp=False
                        )
                        
                        if result_path:
                            messagebox.showinfo("Sucesso", f"Dados exportados com sucesso para:\n{result_path}")
                            export_dialog.destroy()
                        else:
                            messagebox.showwarning("Aviso", "Nenhum dado encontrado para exportar")
                    except Exception as e:
                        messagebox.showerror("Erro", f"Falha ao exportar: {str(e)}")
        
        # Botões
        ttk.Button(button_frame, text="Exportar", command=do_export).pack(side=tk.RIGHT, padx=5)
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
        available_tables = tk.Listbox(left_list_frame, yscrollcommand=left_scrollbar.set, selectmode=tk.EXTENDED)
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
        selected_tables = tk.Listbox(right_list_frame, yscrollcommand=right_scrollbar.set, selectmode=tk.EXTENDED)
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
        """Abre a janela de configurações"""
        messagebox.showinfo("Configurações", "Funcionalidade em desenvolvimento")
        
    def show_about(self):
        """Exibe informações sobre o aplicativo"""
        about_dialog = tk.Toplevel(self.root)
        about_dialog.title("Sobre MT5 Extração")
        about_dialog.geometry("400x300")
        about_dialog.grab_set()  # Modal
        
        # Estilo
        s = ttk.Style()
        s.configure("About.TLabel", font=("Arial", 12))
        
        # Frame principal
        main_frame = ttk.Frame(about_dialog, padding=15)
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Título
        title_label = ttk.Label(main_frame, text="MT5 Extração", font=("Arial", 16, "bold"))
        title_label.pack(pady=(10, 5))
        
        # Versão
        version_label = ttk.Label(main_frame, text="Versão 1.0.0", font=("Arial", 10))
        version_label.pack(pady=(0, 10))
        
        # Descrição
        description = (
            "Ferramenta para extração e análise de dados do MetaTrader 5.\n\n"
            "Esta aplicação permite coletar dados de mercado em tempo real\n"
            "e armazená-los em banco de dados local para análise posterior."
        )
        
        desc_label = ttk.Label(main_frame, text=description, justify=tk.CENTER, style="About.TLabel")
        desc_label.pack(pady=10)
        
        # Copyright
        copyright_label = ttk.Label(main_frame, text="© 2025 - Todos os direitos reservados", font=("Arial", 8))
        copyright_label.pack(side=tk.BOTTOM, pady=10)
        
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
        """Abre a documentação"""
        doc_path = Path("docs/manual.html")
        
        if doc_path.exists():
            # Abrir no navegador padrão
            import webbrowser
            webbrowser.open(doc_path.absolute().as_uri())
        else:
            messagebox.showinfo(
                "Documentação",
                "A documentação está disponível no diretório 'docs/' do projeto.\n\n"
                "Se o diretório não existir, crie-o e adicione a documentação."
            )
    
    def update_symbols_with_data(self, symbols_with_data):
        """
        Atualiza a interface para marcar os símbolos que já têm dados no banco.
        
        Args:
            symbols_with_data (set): Conjunto de símbolos que já têm dados no banco
        """
        if not self.symbols_listbox:
            log.warning("Não foi possível atualizar símbolos com dados: symbols_listbox não inicializado")
            return
            
        # Marcar símbolos com dados na listbox principal
        for i in range(self.symbols_listbox.size()):
            symbol = self.symbols_listbox.get(i)
            
            # Remover marcador existente, se houver
            if symbol.endswith(" ✓"):
                symbol = symbol[:-2].strip()
                self.symbols_listbox.delete(i)
                self.symbols_listbox.insert(i, symbol)
            
            # Verificar se o símbolo (sem marcador) tem dados
            if symbol.upper() in symbols_with_data:
                self.symbols_listbox.delete(i)
                self.symbols_listbox.insert(i, f"{symbol} ✓")
        
        # Atualizar também os símbolos selecionados, se houver
        if self.selected_listbox:
            for i in range(self.selected_listbox.size()):
                symbol = self.selected_listbox.get(i)
                
                # Remover marcador existente, se houver
                if symbol.endswith(" ✓"):
                    symbol = symbol[:-2].strip()
                    self.selected_listbox.delete(i)
                    self.selected_listbox.insert(i, symbol)
                
                # Verificar se o símbolo (sem marcador) tem dados
                if symbol.upper() in symbols_with_data:
                    self.selected_listbox.delete(i)
                    self.selected_listbox.insert(i, f"{symbol} ✓")

    def load_favorites(self):
        """Carrega a lista de símbolos favoritos do arquivo"""
        try:
            if os.path.exists(self.favorites_file):
                with open(self.favorites_file, 'r') as f:
                    return json.load(f)
            return []
        except Exception as e:
            log.error(f"Erro ao carregar favoritos: {e}")
            return []
    
    def save_favorites(self):
        """Salva a lista de símbolos favoritos no arquivo"""
        try:
            with open(self.favorites_file, 'w') as f:
                json.dump(self.favorite_symbols, f)
            log.info(f"Favoritos salvos em {self.favorites_file}")
        except Exception as e:
            log.error(f"Erro ao salvar favoritos: {e}")

    def add_to_favorites(self):
        """Adiciona os símbolos selecionados aos favoritos"""
        if not self.symbols_listbox:
            return
            
        selected_indices = self.symbols_listbox.curselection()
        if not selected_indices:
            messagebox.showinfo("Favoritos", "Selecione pelo menos um símbolo para adicionar aos favoritos.")
            return
            
        added = 0
        for i in selected_indices:
            symbol_text = self.symbols_listbox.get(i)
            # Remove o prefixo se já for um favorito
            if symbol_text.startswith("★ "):
                symbol = symbol_text[2:]
            else:
                symbol = symbol_text
                
            if symbol not in self.favorite_symbols:
                self.favorite_symbols.append(symbol)
                added += 1
                
        if added > 0:
            self.save_favorites()
            self.log(f"{added} símbolo(s) adicionado(s) aos favoritos.")
            # Atualizar a interface
            self.filter_symbols()
        else:
            messagebox.showinfo("Favoritos", "Os símbolos selecionados já são favoritos.")
    
    def remove_from_favorites(self):
        """Remove os símbolos selecionados dos favoritos"""
        if not self.symbols_listbox:
            return
            
        selected_indices = self.symbols_listbox.curselection()
        if not selected_indices:
            messagebox.showinfo("Favoritos", "Selecione pelo menos um símbolo para remover dos favoritos.")
            return
            
        removed = 0
        for i in selected_indices:
            symbol_text = self.symbols_listbox.get(i)
            # Remove o prefixo se for um favorito
            if symbol_text.startswith("★ "):
                symbol = symbol_text[2:]
            else:
                symbol = symbol_text
                
            if symbol in self.favorite_symbols:
                self.favorite_symbols.remove(symbol)
                removed += 1
                
        if removed > 0:
            self.save_favorites()
            self.log(f"{removed} símbolo(s) removido(s) dos favoritos.")
            # Atualizar a interface
            self.filter_symbols()
        else:
            messagebox.showinfo("Favoritos", "Nenhum dos símbolos selecionados é favorito.")

    # Adicionar funções para mostrar apenas favoritos ou todos os símbolos
    def show_only_favorites(self):
        """Mostra apenas os símbolos marcados como favoritos"""
        if not self.symbols_listbox:
            return
            
        self.symbols_listbox.delete(0, tk.END)
        
        # Adicionar apenas os favoritos
        for symbol in self.app.symbols:
            if symbol in self.favorite_symbols:
                self.symbols_listbox.insert(tk.END, f"★ {symbol}")
                
        # Destacar símbolos com dados existentes
        self.highlight_symbols_with_data()
        self.log("Exibindo apenas símbolos favoritos")
    
    def show_all_symbols(self):
        """Mostra todos os símbolos, favoritos primeiro"""
        # Simplesmente chama filter_symbols com a busca vazia
        self.search_var.set("")
        self.filter_symbols()
        self.log("Exibindo todos os símbolos")

    def check_mt5_status(self):
        """Verifica e atualiza o status de conexão do MT5."""
        try:
            if not self.app.mt5_connector or not hasattr(self.app.mt5_connector, '_is_mt5_running'):
                self.update_status("Status: MT5 não inicializado")
                return
                
            # Verificar se o MT5 está em execução
            is_running = self.app.mt5_connector._is_mt5_running()
            
            if not is_running:
                self.update_status("Status: MT5 não está em execução")
                self.mt5_status_value.config(text="Desconectado", foreground="red")
                return
                
            # Verificar se está conectado
            if not self.app.mt5_initialized or not self.app.mt5_connector.is_initialized:
                self.update_status("Status: MT5 em execução, mas desconectado")
                self.mt5_status_value.config(text="Desconectado", foreground="orange")
                
                # Tentar reconectar
                response = messagebox.askyesno(
                    "Reconectar",
                    "O MT5 está em execução, mas a aplicação não está conectada.\n\nDeseja tentar conectar agora?"
                )
                
                if response:
                    if self.app.mt5_connector.initialize(recursion_count=0):
                        self.app.mt5_initialized = True
                        self.update_status("Status: MT5 conectado com sucesso")
                        self.mt5_status_value.config(text="Conectado", foreground="green")
                        
                        # Verificar modo de administrador
                        self._check_admin_mode()
                    else:
                        self.update_status("Status: Falha ao conectar com MT5")
                        messagebox.showerror("Erro", "Não foi possível conectar ao MT5. Verifique se o terminal está aberto e funcionando corretamente.")
                
                return
                
            # Se chegou aqui, está em execução e conectado
            self.update_status("Status: MT5 conectado")
            self.mt5_status_value.config(text="Conectado", foreground="green")
            
            # Verificar modo de administrador
            self._check_admin_mode()
            
        except Exception as e:
            log.error(f"Erro ao verificar status do MT5: {e}")
            self.update_status(f"Erro ao verificar status: {str(e)}")
            self.mt5_status_value.config(text="Erro", foreground="red")
    
    def _check_admin_mode(self):
        """Verifica se o MT5 está em modo de administrador."""
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
