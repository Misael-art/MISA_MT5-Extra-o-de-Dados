import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, filedialog
import logging
import datetime
import traceback
import pandas as pd
import time
import threading
import os
from pathlib import Path

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
        self.selected_symbols_listbox = None
        self.start_button = None
        self.stop_button = None
        self.search_var = tk.StringVar() # Variável para busca de símbolos

        log.info("UIManager inicializado.")

    def setup_ui(self):
        """
        Configura todos os widgets da interface gráfica.
        (Este método será preenchido movendo a lógica de app.py)
        """
        log.info("Configurando a interface gráfica...")
        
        # Criar a barra de menu
        self.create_menu()
        
        try:
            # Configuração da janela principal
            self.root.title("MT5 Extração - Coleta de Dados")
            self.root.geometry("1200x800")
            self.root.minsize(800, 600)

            # Estilo
            style = ttk.Style()
            style.configure("TButton", font=("Arial", 10), padding=5)
            style.configure("TLabel", font=("Arial", 10))
            
            # Estilos para símbolos na listbox
            style.configure("SymbolWithData.TLabel", foreground="green")
            style.configure("SymbolNoData.TLabel", foreground="black")
            
            # Estilo para barras de progresso
            style.configure("Collection.Horizontal.TProgressbar", 
                           troughcolor='#E0E0E0', 
                           background='#4CAF50',  # Verde para barras de sucesso
                           borderwidth=0, 
                           thickness=22)
            
            style.configure("Error.Horizontal.TProgressbar", 
                          background='#F44336')  # Vermelho para barras de erro

            # Frame principal
            main_frame = ttk.Frame(self.root, padding="10")
            main_frame.pack(fill=tk.BOTH, expand=True)

            # Frame superior
            top_frame = ttk.Frame(main_frame)
            top_frame.pack(fill=tk.X, pady=5)

            # Label de status no topo (agora atributo do UIManager)
            self.status_label = ttk.Label(top_frame, text="Status: Desconectado")
            self.status_label.pack(side=tk.LEFT, padx=5)

            # Atualiza o status do MT5 (acessa atributos/métodos da app principal)
            if self.app.mt5_initialized:
                self.status_label.config(text="Status: MT5 conectado")
                if hasattr(self.app, 'symbols') and self.app.symbols:
                    self.log(f"Conectado ao MetaTrader 5 - {len(self.app.symbols)} símbolos disponíveis") # Usa self.log
                else:
                    self.log("Conectado ao MetaTrader 5 - carregando símbolos...") # Usa self.log
                    self.app.load_symbols() # Chama método da app
                    if self.app.symbols:
                        self.log(f"Símbolos carregados - {len(self.app.symbols)} disponíveis") # Usa self.log
                    else:
                        self.log("Não foi possível carregar os símbolos. Funcionalidade limitada.") # Usa self.log
            else:
                self.status_label.config(text="Status: MT5 desconectado")
                self.log("MetaTrader 5 não está conectado. Funcionalidade limitada.") # Usa self.log

            # Frame esquerdo (seleção de símbolos)
            left_frame = ttk.LabelFrame(main_frame, text="Símbolos Disponíveis", padding="5")
            left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=False, padx=5, pady=5)

            # Lista de símbolos
            symbols_frame = ttk.Frame(left_frame)
            symbols_frame.pack(fill=tk.BOTH, expand=True, pady=5)

            # self.search_var já é atributo do UIManager
            self.search_var.trace("w", self.filter_symbols) # command chama método do UIManager
            search_entry = ttk.Entry(symbols_frame, textvariable=self.search_var)
            search_entry.pack(fill=tk.X, pady=5)
            ttk.Label(symbols_frame, text="Buscar símbolo:").pack(before=search_entry)

            symbols_list_frame = ttk.Frame(symbols_frame)
            symbols_list_frame.pack(fill=tk.BOTH, expand=True)

            scrollbar = ttk.Scrollbar(symbols_list_frame)
            scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

            # self.symbols_listbox é atributo do UIManager
            self.symbols_listbox = tk.Listbox(symbols_list_frame, selectmode=tk.EXTENDED, yscrollcommand=scrollbar.set)
            self.symbols_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            scrollbar.config(command=self.symbols_listbox.yview)
            
            # Adicionar binding para mostrar detalhes dos dados quando um símbolo é selecionado
            self.symbols_listbox.bind('<<ListboxSelect>>', self.on_symbol_select)

            # Preencher a lista de símbolos (chama método do UIManager)
            self.filter_symbols()

            # Botões de controle dos símbolos
            symbols_buttons_frame = ttk.Frame(left_frame)
            symbols_buttons_frame.pack(fill=tk.X, pady=5)
            # Botões Adicionar/Remover (dentro do left_frame)
            ttk.Button(symbols_buttons_frame, text="Adicionar →", command=self.add_symbols).pack(side=tk.LEFT, padx=2)
            ttk.Button(symbols_buttons_frame, text="← Remover", command=self.remove_symbols).pack(side=tk.LEFT, padx=2)

            # Frame direito (controles e monitoramento)
            right_frame = ttk.Frame(main_frame)
            right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=5, pady=5)

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
            
            # Botão para iniciar MT5 como administrador
            if hasattr(self.app.mt5_connector, 'launch_mt5_as_admin'):
                admin_frame = ttk.Frame(control_frame)
                admin_frame.grid(row=2, column=0, columnspan=3, padx=5, pady=5, sticky="ew")
                
                status_label = ttk.Label(admin_frame, text="Status do MT5:")
                status_label.pack(side=tk.LEFT, padx=5)
                
                # Função para verificar e atualizar status do MT5
                def check_mt5_admin_status():
                    if not self.app.mt5_connector._is_mt5_running():
                        status_text = "MT5 não está em execução"
                        admin_status_label.config(text=status_text, foreground="red")
                        start_admin_button.config(text="Iniciar MT5 como Admin")
                        return
                        
                    if not self.app.mt5_connector.is_mt5_running_as_admin():
                        status_text = "MT5 em execução SEM privilégios de administrador"
                        admin_status_label.config(text=status_text, foreground="orange")
                        start_admin_button.config(text="Reiniciar como Admin")
                    else:
                        status_text = "MT5 em execução COM privilégios de administrador"
                        admin_status_label.config(text=status_text, foreground="green")
                        start_admin_button.config(text="Reiniciar como Admin")
                
                # Label que mostra status de admin do MT5
                admin_status_label = ttk.Label(admin_frame, text="Verificando...", width=40)
                admin_status_label.pack(side=tk.LEFT, padx=5)
                
                # Botão para iniciar/reiniciar como admin
                start_admin_button = ttk.Button(admin_frame, text="Iniciar MT5 como Admin", 
                                              command=lambda: self.launch_mt5_as_admin())
                start_admin_button.pack(side=tk.RIGHT, padx=5)
                
                # Verificar status inicial
                check_mt5_admin_status()
                
                # Botão de refresh para verificar status
                refresh_button = ttk.Button(admin_frame, text="↻", width=3, 
                                          command=check_mt5_admin_status)
                refresh_button.pack(side=tk.RIGHT, padx=5)

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
            
            # Verificar e carregar dados existentes para destaque visual
            self.load_existing_symbols_data()

            # --- Fim da configuração da UI dentro do try ---

        # Bloco except alinhado com o try da linha 49
        except Exception as e:
            log.error(f"Erro ao configurar UI: {e}")
            # Usar o logger da app principal para exibir messagebox? Ou logar apenas?
            self.log_error(e, "Erro fatal na configuração da UI") # Usa self.log_error
            # Considerar se deve sair ou tentar continuar
            # sys.exit(1) # Evitar sys.exit dentro do UIManager
            
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
            
            # Para cada símbolo disponível, verificar se há dados
            for symbol in self.app.symbols:
                # Verificar para o timeframe M1 (1 minuto)
                table_name = self.app.db_manager.get_table_name_for_symbol(symbol, "1 minuto")
                
                if table_name in existing_tables:
                    self.existing_symbols_map[symbol] = table_name
            
            log.info(f"Identificados {len(self.existing_symbols_map)} símbolos com dados existentes")
            
            # Atualizar a interface com destaques visuais
            self.highlight_symbols_with_data()
            
        except Exception as e:
            log.error(f"Erro ao verificar símbolos existentes: {e}")
            
    def highlight_symbols_with_data(self):
        """Atualiza a interface para destacar visualmente os símbolos que já possuem dados."""
        if not hasattr(self, 'existing_symbols_map') or not self.symbols_listbox:
            return
            
        # Destacar símbolos na listbox principal
        for i in range(self.symbols_listbox.size()):
            symbol = self.symbols_listbox.get(i)
            if symbol in self.existing_symbols_map:
                # Adicionar indicação visual (cor verde ou ícone)
                self.symbols_listbox.itemconfig(i, {'fg': 'green', 'bg': '#f0f8f0'})
                
        # Destacar símbolos na listbox de selecionados
        for i in range(self.selected_listbox.size()):
            symbol = self.selected_listbox.get(i)
            if symbol in self.existing_symbols_map:
                self.selected_listbox.itemconfig(i, {'fg': 'green', 'bg': '#f0f8f0'})
            
    def on_symbol_select(self, event):
        """Callback quando um símbolo é selecionado na lista principal."""
        # Obter índice selecionado
        if not self.symbols_listbox:
            return
            
        selection = self.symbols_listbox.curselection()
        if not selection:
            return
            
        # Obter o símbolo selecionado
        symbol = self.symbols_listbox.get(selection[0])
        self.update_symbol_details(symbol)
        
    def on_selected_symbol_select(self, event):
        """Callback quando um símbolo é selecionado na lista de selecionados."""
        # Obter índice selecionado
        if not self.selected_listbox:
            return
            
        selection = self.selected_listbox.curselection()
        if not selection:
            return
            
        # Obter o símbolo selecionado
        symbol = self.selected_listbox.get(selection[0])
        self.update_symbol_details(symbol)
        
    def update_symbol_details(self, symbol):
        """Atualiza o painel de detalhes com informações do símbolo selecionado."""
        if not hasattr(self, 'symbol_details_content') or not symbol:
            return
            
        # Verificar se o símbolo tem dados existentes
        if hasattr(self, 'existing_symbols_map') and symbol in self.existing_symbols_map:
            table_name = self.existing_symbols_map[symbol]
            
            # Obter resumo dos dados
            if self.app.db_manager:
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
        
        # Se não tiver dados ou houver erro, mostrar informações básicas
        detail_text = f"""Símbolo: {symbol}

Meta Trader 5:
• Spread: {self.get_symbol_spread(symbol)}
• Cotação: {self.get_symbol_price(symbol)}

Status: Sem dados históricos armazenados
"""
        self.symbol_details_content.config(text=detail_text)
        self.symbol_details_frame.config(text=f"Detalhes de {symbol}")
        
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
        except:
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
        except:
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
        search_text = self.search_var.get().lower()
        self.symbols_listbox.delete(0, tk.END)

        # Acessa a lista de símbolos da instância principal
        for symbol in self.app.symbols:
            if search_text in symbol.lower():
                self.symbols_listbox.insert(tk.END, symbol)
                
        # Destacar símbolos com dados existentes
        self.highlight_symbols_with_data()

    def add_symbols(self):
        """Adiciona símbolos selecionados à lista de monitoramento"""
        if not self.symbols_listbox or not self.selected_listbox:
            return
        selected_indices = self.symbols_listbox.curselection()

        for i in selected_indices:
            symbol = self.symbols_listbox.get(i)
            # Modifica a lista na instância principal
            if symbol not in self.app.selected_symbols:
                self.app.selected_symbols.append(symbol)
                self.selected_listbox.insert(tk.END, symbol)
                self.log(f"Símbolo adicionado: {symbol}") # Usa self.log

    def remove_symbols(self):
        """Remove símbolos da lista de monitoramento"""
        if not self.selected_listbox:
            return
        selected_indices = self.selected_listbox.curselection()

        # Remove em ordem reversa para não afetar os índices
        for i in sorted(selected_indices, reverse=True):
            symbol = self.selected_listbox.get(i)
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
        if self.status_label:
            self.status_label.config(text=status_text)
        else:
            log.warning("Tentativa de atualizar status sem widget status_label.")

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
                self.log("Utilizando verificação agressiva de conexão...")
                reconnected = self.app.mt5_connector.force_connection_check()
                if reconnected:
                    self.app.mt5_initialized = True
                    self.log("Conexão reestabelecida com sucesso via verificação agressiva!")
                    self.update_status("Status: MT5 conectado!")
                else:
                    # Tentar método normal como fallback
                    self.log("Verificação agressiva falhou. Tentando método normal...")
                    reconnected = self.app.mt5_connector.initialize(force_restart=False)
            else:
                # Método normal se o agressivo não estiver disponível
                reconnected = self.app.mt5_connector.initialize(force_restart=False)
                
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
                if self.app.mt5_connector and self.app.mt5_connector.initialize():
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
                            if self.app.mt5_connector.initialize():
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
                    if not self.app.mt5_connector.initialize():
                        messagebox.showerror("Erro", "Falha ao conectar ao MT5. Não é possível detectar a data mais antiga.")
                        return
                else:
                    return
            
            # Verificar se o MT5 está rodando como administrador
            if hasattr(self.app.mt5_connector, 'is_mt5_running_as_admin') and self.app.mt5_connector._is_mt5_running():
                if not self.app.mt5_connector.is_mt5_running_as_admin():
                    logging.warning("MT5 está executando sem privilégios de administrador durante detecção de data.")
                    response = messagebox.askyesno(
                        "Permissões Insuficientes",
                        "O MetaTrader 5 está em execução sem permissões de administrador.\n\n"
                        "Isso pode limitar o acesso a dados históricos e causar falhas na detecção de datas.\n\n"
                        "Deseja reiniciar o MT5 com privilégios de administrador antes de continuar?",
                    )
                    if response:
                        self.log("Reiniciando MT5 com permissões de administrador...")
                        status_var.set("Reiniciando MT5 como administrador...")
                        
                        try:
                            if self.app.mt5_connector.launch_mt5_as_admin(wait_for_user=False):
                                # Dar tempo para o MT5 iniciar e tentar reconectar
                                time.sleep(5)
                                
                                # Tentar reconectar
                                if self.app.mt5_connector.initialize():
                                    # Verificar novamente se agora está rodando como admin
                                    if self.app.mt5_connector.is_mt5_running_as_admin():
                                        self.log("MT5 reiniciado com permissões de administrador com sucesso.")
                                        status_var.set("MT5 iniciado com permissões de administrador.")
                                    else:
                                        self.log("AVISO: MT5 reiniciado, mas ainda sem permissões de administrador.")
                                        status_var.set("MT5 sem permissões de administrador")
                                        response = messagebox.askyesno(
                                            "Permissões Limitadas", 
                                            "O MT5 foi reiniciado, mas ainda parece estar sem permissões de administrador.\n\n"
                                            "Isso pode limitar a detecção de datas históricas.\n\n"
                                            "Deseja continuar mesmo assim?"
                                        )
                                        if not response:
                                            return
                                else:
                                    self.log("ERRO: Falha ao reconectar ao MT5 após reinício.")
                                    messagebox.showerror("Erro de Conexão", "Falha ao reconectar ao MT5 após reinício. A detecção pode falhar.")
                            else:
                                self.log("ERRO: Falha ao reiniciar MT5 como administrador.")
                                messagebox.showerror("Erro", "Não foi possível reiniciar o MT5 com permissões de administrador. A detecção pode falhar.")
                        except Exception as admin_err:
                            self.log(f"ERRO: {str(admin_err)}")
                            logging.error(f"Erro ao tentar reiniciar MT5 como administrador: {traceback.format_exc()}")
                            messagebox.showerror("Erro", f"Erro ao tentar reiniciar MT5 como administrador: {str(admin_err)}")
                            return
                    else:
                        # Usuário optou por continuar sem permissões de admin
                        response = messagebox.askyesno(
                            "Confirmação", 
                            "Continuar sem permissões de administrador pode resultar em:\n\n"
                            "- Falhas na detecção de datas históricas\n"
                            "- Erros de conexão ao tentar acessar certos símbolos\n"
                            "- Resultados imprecisos ou incompletos\n\n"
                            "Tem certeza que deseja continuar?"
                        )
                        if not response:
                            return
            
            # Obter timeframe selecionado
            timeframe_idx = timeframe_combo.current()
            timeframe_name, timeframe_val = self.app.timeframes[timeframe_idx]
            
            # Configurar progresso e interface
            status_var.set("Iniciando detecção de data mais antiga...")
            progress_var.set(0)
            detect_button.config(state=tk.DISABLED)
            cancel_detect_button.grid()  # Mostrar botão de cancelamento
            cancel_detect_button.config(state=tk.NORMAL)
            
            # Armazenar flag de detecção ativa
            self.detection_running = True
            
            def detection_thread():
                try:
                    oldest_dates = []
                    total_symbols = len(self.app.selected_symbols)
                    
                    for i, symbol in enumerate(self.app.selected_symbols):
                        if not hasattr(self, 'detection_running') or not self.detection_running:
                            self.root.after_idle(lambda: status_var.set("Detecção cancelada pelo usuário"))
                            break
                            
                        # Dividir em sub-etapas para responsividade da interface
                        for sub_step in range(4):
                            if not hasattr(self, 'detection_running') or not self.detection_running:
                                break
                                
                            # Atualizar progresso mais frequentemente
                            sub_progress = ((i * 4 + sub_step) / (total_symbols * 4)) * 100
                            sub_msg = f"Analisando {symbol} - etapa {sub_step+1}/4 ({i+1}/{total_symbols})"
                            
                            # Thread-safe UI update
                            self.root.after_idle(lambda p=sub_progress, m=sub_msg: (
                                progress_var.set(p), 
                                status_var.set(m)
                            ))
                            time.sleep(0.1)  # Pequena pausa para resposta da UI
                        
                        # Atualizar progresso principal
                        progress = ((i + 1) / total_symbols) * 100
                        msg = f"Detectando data para {symbol} ({i+1}/{total_symbols})"
                        
                        # Atualizar UI thread-safe
                        self.root.after_idle(lambda p=progress, m=msg: (
                            progress_var.set(p), 
                            status_var.set(m)
                        ))
                        
                        try:
                            # Obter data mais antiga
                            oldest_date = self.app.mt5_connector.get_oldest_available_date(symbol, timeframe_val)
                            if oldest_date:
                                oldest_dates.append(oldest_date)
                                self.log(f"Data mais antiga para {symbol}: {oldest_date.strftime('%Y-%m-%d')}")
                            else:
                                self.log(f"Não foi possível detectar data mais antiga para {symbol}")
                        except Exception as sym_err:
                            self.log(f"Erro ao detectar data para {symbol}: {sym_err}")
                            logging.error(f"Erro detalhado para {symbol}: {traceback.format_exc()}")
                        
                        # Pequena pausa entre símbolos para não sobrecarregar o MT5
                        time.sleep(0.2)
                        
                    # Processar resultados
                    if oldest_dates and self.detection_running:
                        min_date = min(oldest_dates)
                        date_str = min_date.strftime('%Y-%m-%d')
                        
                        # Atualizar UI thread-safe
                        self.root.after_idle(lambda d=date_str: (
                            start_date_entry.delete(0, tk.END),
                            start_date_entry.insert(0, d),
                            status_var.set(f"Data mais antiga detectada: {d}"),
                            messagebox.showinfo("Detecção Concluída", f"Data mais antiga encontrada: {d}")
                        ))
                    elif self.detection_running:
                        self.root.after_idle(lambda: (
                            status_var.set("Nenhuma data antiga encontrada para os símbolos selecionados"),
                            messagebox.showinfo("Detecção Concluída", "Não foi possível encontrar dados históricos para os símbolos selecionados.")
                        ))
                        
                except Exception as e:
                    error_msg = str(e)
                    logging.error(f"Erro na detecção de data mais antiga: {error_msg}")
                    logging.error(traceback.format_exc())
                    
                    self.root.after_idle(lambda msg=error_msg: (
                        status_var.set(f"Erro na detecção: {msg}"),
                        messagebox.showerror("Erro de Detecção", f"Erro ao detectar data mais antiga: {msg}")
                    ))
                
                finally:
                    self.detection_running = False
                    # Restaurar UI
                    self.root.after_idle(lambda: (
                        detect_button.config(state=tk.NORMAL),
                        cancel_detect_button.grid_remove()
                    ))
            
            # Iniciar thread de detecção
            threading.Thread(target=detection_thread, daemon=True).start()
            
        def cancel_detection():
            if hasattr(self, 'detection_running') and self.detection_running:
                self.detection_running = False
                status_var.set("Cancelando detecção...")
                cancel_detect_button.config(state=tk.DISABLED)
                self.log("Detecção de data mais antiga cancelada pelo usuário")
        
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
            self.app.start_historical_extraction_logic(
                symbols=self.app.selected_symbols,
                timeframe_val=timeframe_val,
                timeframe_name=timeframe_name,
                start_date=start_date,
                end_date=end_date,
                max_bars=max_bars, # Passa max_bars se necessário na lógica
                include_indicators=include_indicators,
                overwrite=overwrite,
                auto_detect_oldest_date=auto_detect_var.get(),  # Passar o valor da checkbox
                # Callbacks para atualizar a UI a partir da thread da app
                update_progress_callback=lambda p, msg: self.root.after_idle(lambda: (progress_var.set(p), status_var.set(msg))),
                finished_callback=lambda s, f: self.root.after_idle(lambda: (extract_button.config(state=tk.NORMAL), cancel_button.config(text="Fechar")))
            )

        def cancel_extraction_logic():
             if extract_button.cget('state') == tk.DISABLED:
                 # Chama a lógica de cancelamento na app principal
                 self.app.cancel_historical_extraction_logic()
                 status_var.set("Cancelando...")
             else:
                 config_window.destroy()

        # Botões
        buttons_frame = ttk.Frame(control_frame)
        buttons_frame.pack(fill=tk.X, pady=5)
        extract_button = ttk.Button(buttons_frame, text="Iniciar Extração", command=start_extraction_logic)
        extract_button.pack(side=tk.LEFT, padx=5)
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
                if self.app.mt5_connector and self.app.mt5_connector.initialize():
                    self.app.mt5_initialized = True
                    status = self.app.mt5_connector.get_connection_status()
                    logging.info(f"Conexão MT5 reestabelecida. Modo: {status.get('mode', 'N/A')}")
                    self.log(f"MT5 conectado com sucesso. Modo: {status.get('mode', 'N/A')}")
                    self.update_status("Status: MT5 reconectado!")
                    messagebox.showinfo("Conexão MT5", "Conexão com MT5 estabelecida com sucesso.")
                else:
                    # Se falhar na reconexão, continua apenas com dados do banco
                    logging.warning("Falha ao reconectar ao MT5. Mostrando apenas dados armazenados.")
                    self.log("AVISO: Falha ao reconectar ao MT5. Mostrando apenas dados armazenados.")
                    self.update_status("Status: Visualizando dados armazenados apenas")
            # Continua mesmo sem MT5 conectado, apenas com dados do banco

        stats_window = tk.Toplevel(self.root)
        stats_window.title("Estatísticas")
        stats_window.geometry("800x600")

        notebook = ttk.Notebook(stats_window)
        notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        dados_encontrados = False

        for symbol in self.app.selected_symbols:
            try:
                # TODO: Refatorar para usar um método db_manager.get_data(symbol, timeframe, limit)
                table_name = f"{symbol.lower()}_1min"
                query_exists = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
                query_data = f"SELECT * FROM {table_name} ORDER BY time DESC LIMIT 1000"

                try:
                    # Usa o engine diretamente (temporário)
                    table_exists = pd.read_sql(query_exists, self.app.db_manager.engine)
                    if table_exists.empty:
                        self.log(f"Tabela {table_name} não existe - sem dados para mostrar estatísticas")
                        continue
                    df = pd.read_sql(query_data, self.app.db_manager.engine)
                except Exception as table_error:
                    self.log(f"Erro ao ler tabela {table_name}: {str(table_error)}")
                    continue

                if df.empty:
                    self.log(f"Nenhum dado encontrado para {symbol}")
                    continue

                dados_encontrados = True
                df['time'] = pd.to_datetime(df['time'])

                tab = ttk.Frame(notebook)
                notebook.add(tab, text=symbol)

                stats_text = tk.Text(tab, height=15, width=60)
                stats_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

                # TODO: Mover cálculo de estatísticas para classe/módulo separado?
                stats = {
                    "Intervalo de Datas": f"{df['time'].min()} a {df['time'].max()}",
                    "Total de Registros": len(df),
                    "Preço Atual": df['close'].iloc[0] if not df.empty else 'N/A',
                    "Preço Máximo": df['high'].max(),
                    "Preço Mínimo": df['low'].min(),
                    "Média de Preço (Fechamento)": df['close'].mean(),
                    "Volatilidade (ATR médio)": df['atr'].mean() if 'atr' in df.columns else "N/A",
                    # "Volume Total": df['volume'].sum(), # 'volume' não parece estar no DF padrão
                    # "Volume Médio": df['volume'].mean(),
                    "RSI Atual": df['rsi'].iloc[0] if 'rsi' in df.columns and not df.empty else "N/A",
                }

                if 'candle_pattern' in df.columns:
                    pattern_counts = df['candle_pattern'].value_counts().to_dict()
                    for pattern, count in pattern_counts.items():
                        if pattern != "No Pattern":
                            stats[f"Padrão {pattern}"] = count

                stats_text.insert(tk.END, "ESTATÍSTICAS DO SÍMBOLO\n")
                stats_text.insert(tk.END, "=====================\n\n")
                for key, value in stats.items():
                    stats_text.insert(tk.END, f"{key}: {value}\n")

                # Gráfico (requer matplotlib e FigureCanvasTkAgg)
                try:
                    from matplotlib.figure import Figure
                    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

                    fig = Figure(figsize=(10, 4), dpi=100)
                    ax = fig.add_subplot(111)

                    plot_df = df.iloc[::-1].copy()
                    plot_df.set_index('time', inplace=True)

                    ax.plot(plot_df.index, plot_df['close'], label='Preço de Fechamento')
                    if 'ma_20' in plot_df.columns:
                        ax.plot(plot_df.index, plot_df['ma_20'], label='MA 20', linestyle='--')
                    if 'bollinger_upper' in plot_df.columns and 'bollinger_lower' in plot_df.columns:
                        ax.plot(plot_df.index, plot_df['bollinger_upper'], label='Bollinger Sup.', linestyle=':')
                        ax.plot(plot_df.index, plot_df['bollinger_lower'], label='Bollinger Inf.', linestyle=':')

                    ax.set_title(f'Preço de {symbol}')
                    ax.set_ylabel('Preço')
                    ax.legend()
                    fig.autofmt_xdate()

                    canvas = FigureCanvasTkAgg(fig, master=tab)
                    canvas.draw()
                    canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
                except ImportError:
                    log.warning("Matplotlib não instalado. Gráficos não serão exibidos.")
                    stats_text.insert(tk.END, "\n\nAVISO: Matplotlib não instalado. Gráficos indisponíveis.")
                except Exception as plot_err:
                    log.error(f"Erro ao gerar gráfico para {symbol}: {plot_err}")
                    self.log(f"Erro ao gerar gráfico para {symbol}: {plot_err}")
                 
            except Exception as e:
                self.log(f"Erro ao gerar estatísticas para {symbol}: {str(e)}")
                logging.error(f"Erro detalhado para {symbol}: {traceback.format_exc()}")

        if not dados_encontrados:
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
            log.error(f"Erro ao tentar iniciar MT5 como administrador: {e}")
            log.debug(traceback.format_exc())
            messagebox.showerror("Erro", f"Erro ao tentar iniciar MT5 como administrador: {str(e)}")

    def create_menu(self):
        """Cria a barra de menu principal"""
        menubar = tk.Menu(self.root)
        self.root.config(menu=menubar)
        
        # Menu Arquivo
        file_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Arquivo", menu=file_menu)
        file_menu.add_command(label="Configurações", command=self.open_settings)
        file_menu.add_separator()
        file_menu.add_command(label="Sair", command=self.root.quit)
        
        # Menu Dados
        data_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Dados", menu=data_menu)
        data_menu.add_command(label="Histórico do Símbolo", command=self.extract_historical_data)
        
        # Submenu de Exportação
        export_menu = tk.Menu(data_menu, tearoff=0)
        data_menu.add_cascade(label="Exportar Dados", menu=export_menu)
        export_menu.add_command(label="Exportar para CSV", command=lambda: self.export_data("csv"))
        export_menu.add_command(label="Exportar para Excel", command=lambda: self.export_data("excel"))
        export_menu.add_command(label="Exportar Múltiplas Tabelas", command=self.export_multiple_tables)
        
        # Menu Ajuda
        help_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Ajuda", menu=help_menu)
        help_menu.add_command(label="Sobre", command=self.show_about)
        help_menu.add_command(label="Documentação", command=self.open_documentation)
        
        log.info("Menu da aplicação criado")
    
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
        if self.selected_symbols_listbox:
            for i in range(self.selected_symbols_listbox.size()):
                symbol = self.selected_symbols_listbox.get(i)
                
                # Remover marcador existente, se houver
                if symbol.endswith(" ✓"):
                    symbol = symbol[:-2].strip()
                    self.selected_symbols_listbox.delete(i)
                    self.selected_symbols_listbox.insert(i, symbol)
                
                # Verificar se o símbolo (sem marcador) tem dados
                if symbol.upper() in symbols_with_data:
                    self.selected_symbols_listbox.delete(i)
                    self.selected_symbols_listbox.insert(i, f"{symbol} ✓")

# Exemplo de como seria chamado em app.py (não executar este arquivo diretamente)
# if __name__ == "__main__":
#     # Este bloco não deve ser executado diretamente,
#     # UIManager depende de uma instância de MT5Extracao.
#     pass