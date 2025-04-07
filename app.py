import os
import sys
import configparser
import sqlite3
import json
import datetime
import traceback
import tkinter as tk
from tkinter import ttk, messagebox
import logging
import psutil
import pandas as pd
import time
from mt5_extracao.mt5_connector import MT5Connector
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.indicator_calculator import IndicatorCalculator
from mt5_extracao.ui_manager import UIManager
from mt5_extracao.data_collector import DataCollector
from mt5_extracao.data_exporter import DataExporter
from mt5_extracao.security import CredentialManager
from mt5_extracao.error_handler import with_error_handling, ErrorHandler
from mt5_extracao.integrated_services import IntegratedServices
from mt5_extracao.enhanced_calculation_service import EnhancedCalculationService
from mt5_extracao.performance_optimizer import PerformanceOptimizer
from mt5_extracao.historical_extractor import HistoricalExtractor # Adicionado
from mt5_extracao.external_data_source import ExternalDataSource, DummyExternalSource # Adicionado para Fallback
from typing import Optional # Adicionado para type hint
# Garantir que o diretório de logs existe
os.makedirs("logs", exist_ok=True)

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/mt5_app.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# Função para verificar dependências críticas antes de continuar
def verificar_dependencias_criticas():
    """Verifica se as dependências críticas estão disponíveis"""
    dependencias_faltantes = []

    # Verificar psutil para detecção de processos
    try:
        import psutil
    except ImportError:
        try:
            logging.info("Instalando psutil (necessário para verificar processos)...")
            import subprocess
            subprocess.check_call([sys.executable, "-m", "pip", "install", "psutil"])
            import psutil
            logging.info("✓ psutil instalado com sucesso")
        except Exception as e:
            logging.error(f"Erro ao instalar psutil: {str(e)}")

    # MetaTrader5 é essencial
    try:
        import MetaTrader5
    except ImportError:
        dependencias_faltantes.append("MetaTrader5")

    # Outras dependências críticas
    try:
        import pandas as pd
    except ImportError:
        dependencias_faltantes.append("pandas")

    try:
        import numpy as np
    except ImportError:
        dependencias_faltantes.append("numpy")

    try:
        from sqlalchemy import create_engine
    except ImportError:
        dependencias_faltantes.append("sqlalchemy")

    if dependencias_faltantes:
        logging.error(f"Dependências críticas faltando: {', '.join(dependencias_faltantes)}")
        return False, dependencias_faltantes

    return True, []

# Verificar e importar demais dependências
try:
    import pandas as pd
    import numpy as np
    from sqlalchemy import create_engine
    import MetaTrader5 as mt5

    # Verificação especial para pandas_ta devido a problemas conhecidos
    try:
        # Patch para compatibilidade com numpy 2.x
        import numpy
        if not hasattr(numpy, 'NaN'):
            numpy.NaN = float('nan')

        import pandas_ta as ta
    except Exception as e:
        # Não interrompe a execução, vai usar funcionalidades alternativas
        logging.warning(f"Problema ao importar pandas_ta: {str(e)}")
        ta = None

        # Implementação básica de indicadores para substituir pandas_ta
        class BasicIndicators:
            def __init__(self):
                pass

            def rsi(self, close, length=14):
                """Implementação básica de RSI"""
                try:
                    delta = close.diff()
                    gain = delta.where(delta > 0, 0)
                    loss = -delta.where(delta < 0, 0)
                    avg_gain = gain.rolling(window=length).mean()
                    avg_loss = loss.rolling(window=length).mean()
                    rs = avg_gain / avg_loss
                    rsi = 100 - (100 / (1 + rs))
                    return rsi
                except Exception as e:
                    logging.error(f"Erro ao calcular RSI: {str(e)}")
                    return pd.Series(float('nan'), index=close.index)

        # Usar implementação básica como fallback
        ta = BasicIndicators()

    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
    from threading import Thread
except ImportError as e:
    # Verificar se o arquivo verificador.py existe
    if os.path.exists("verificador.py"):
        root = tk.Tk()
        root.withdraw()
        executar_verificador = messagebox.askyesno(
            "Erro de Importação",
            f"Não foi possível importar o módulo: {str(e)}.\n"
            "Deseja executar o verificador para corrigir este problema?")

        if executar_verificador:
            logging.info("Iniciando verificador...")
            try:
                import subprocess
                subprocess.Popen([sys.executable, "verificador.py"])
            except Exception as exec_error:
                messagebox.showerror("Erro", f"Não foi possível iniciar o verificador: {str(exec_error)}")
        root.destroy()
        sys.exit(1)
    else:
        # Fallback para mensagem simples
        messagebox.showerror("Erro",
            f"Erro ao importar dependências: {str(e)}\n\n"
            "Execute install.py para instalar as dependências necessárias.")
        sys.exit(1)

# Função verificar_mt5_em_execucao() removida - Lógica movida para MT5Connector
class MT5Extracao:
    def __init__(self, root):
        """
        Inicializa a aplicação principal.
        
        Args:
            root: janela raiz do Tkinter
        """
        self.root = root
        self.config = None
        self.mt5_connector = None
        self.db_manager = None
        self.indicator_calculator = None
        self.ui_manager = None
        self.data_collector = None
        self.data_exporter = None  # Novo atributo para o DataExporter
        self.integrated_services = None  # Novo atributo para os serviços integrados
        self.historical_extractor = None # Novo atributo para o HistoricalExtractor
        
        # Configurar a janela principal
        self.setup_root_window()
        
        # Configurar gerenciador de erros
        self.error_handler = ErrorHandler()
        self.error_handler.install_global_handler()
        
        try:
            # Verificar dependências críticas
            if not verificar_dependencias_criticas():
                messagebox.showerror("Erro de Dependências", 
                                   "Uma ou mais dependências críticas não estão instaladas.\n\n"
                                   "Execute pip install -r requirements.txt e tente novamente.")
                root.after(100, root.destroy)
                return
                
            # Carregar configurações e inicializar banco de dados
            if not self.load_config_and_db():
                root.after(100, root.destroy)
                return
            
            # Inicializar conexão com MT5
            logging.info("Inicializando conexão MT5 via Connector...")
            self.mt5_connector = MT5Connector(config_path=self.config_path)
            
            # Tentar conectar ao MT5
            if self.mt5_connector.initialize():
                logging.info("Conexão com MT5 inicializada com sucesso")
                self.mt5_initialized = True  # Atualiza o status de inicialização
            else:
                logging.warning("Falha ao inicializar conexão MT5. Modo: Erro de Conexão")
                self.mt5_initialized = False  # Garante que o status está como falso em caso de erro
            
            # Inicializar outros componentes
            logging.info("Instanciando IndicatorCalculator...")
            self.indicator_calculator = IndicatorCalculator()
            
            # Carregar lista de símbolos
            self.load_symbols()
            
            # Configurar tipos de timeframes disponíveis
            self.setup_timeframes()
            
            # Inicializar serviços avançados integrados se disponíveis
            try:
                logging.info("Inicializando serviços avançados de cálculo...")
                
                # Obter configurações avançadas ou usar padrões
                advanced_config = {}
                if hasattr(self, 'config') and 'advanced' in self.config:
                    advanced_config = self.config['advanced']
                
                # Criar instância do serviço integrado
                self.integrated_services = IntegratedServices()
                
                # Verificar inicialização bem sucedida
                if self.integrated_services.initialized:
                    logging.info("Serviços avançados inicializados com sucesso")
                    
                    # Iniciar serviço de cálculo
                    if hasattr(self.integrated_services, 'calculation_service') and self.integrated_services.calculation_service:
                        self.integrated_services.calculation_service.start()
                        logging.info("Serviço de cálculo iniciado")
                else:
                    logging.warning("Inicialização dos serviços avançados incompleta")
                    if self.integrated_services.initialization_errors:
                        logging.warning(f"Erros de inicialização: {', '.join(self.integrated_services.initialization_errors)}")
            except Exception as e:
                logging.warning(f"Não foi possível inicializar serviços avançados: {str(e)}")
                logging.debug(traceback.format_exc())
                self.integrated_services = None
            
            # Criar o gerenciador de UI
            logging.info("Instanciando UIManager...")
            self.ui_manager = UIManager(self)
            
            # Criar o coletor de dados
            logging.info("Instanciando DataCollector...")
            self.data_collector = DataCollector(
                connector=self.mt5_connector,
                db_manager=self.db_manager,
                indicator_calculator=self.indicator_calculator,
                ui_manager=self.ui_manager
            )
            
            # Inicializar o exportador de dados
            logging.info("Instanciando DataExporter...")
            self.data_exporter = DataExporter(self.db_manager)

            # --- Configuração do Fallback M1 ---
            external_data_source_instance: Optional[ExternalDataSource] = None
            try:
                fallback_enabled = self.config.getboolean('FALLBACK', 'external_source_m1_fallback_enabled', fallback=False)
                fallback_type = self.config.get('FALLBACK', 'external_source_m1_type', fallback=None)

                if fallback_enabled:
                    if fallback_type and fallback_type.strip().lower() == 'dummy':
                        logging.info("Fallback M1 habilitado. Usando DummyExternalSource.")
                        external_data_source_instance = DummyExternalSource()
                    # TODO: Adicionar 'elif fallback_type.lower() == 'api_x':' para futuras fontes
                    else:
                        logging.warning(f"Fallback M1 habilitado na configuração, mas o tipo '{fallback_type}' não é reconhecido ou está vazio. Fallback permanecerá inativo.")
                else:
                    logging.info("Fallback M1 para fontes externas está desabilitado na configuração.")
            except configparser.Error as cfg_err:
                 logging.error(f"Erro ao ler configurações de fallback do config.ini: {cfg_err}. Fallback desativado.")
            except Exception as e:
                 logging.error(f"Erro inesperado ao configurar fallback: {e}. Fallback desativado.")
                 logging.debug(traceback.format_exc())
            # --- Configuração do Chunking Dinâmico ---
            chunk_config = {
                'm1': self.config.getint('EXTRACTION', 'chunk_days_m1', fallback=30),
                'm5_m15': self.config.getint('EXTRACTION', 'chunk_days_m5_m15', fallback=90),
                'default': self.config.getint('EXTRACTION', 'chunk_days_default', fallback=365)
            }
            logging.info(f"Configuração de Chunking lida: M1={chunk_config['m1']}d, M5/M15={chunk_config['m5_m15']}d, Default={chunk_config['default']}d")

            # --- Inicializar o extrator histórico ---
            logging.info("Instanciando HistoricalExtractor...")
            self.historical_extractor = HistoricalExtractor(
                connector=self.mt5_connector,
                db_manager=self.db_manager,
                indicator_calculator=self.indicator_calculator,
                external_source=external_data_source_instance, # Passa a instância (ou None)
                chunk_config=chunk_config # Passa a configuração de chunking
            )
            # REMOVIDA LINHA EXTRA ')'
            
            # Configurar a UI
            self.ui_manager.setup_ui()
            
            # Atualizar informações da tabela
            self.update_table_info()
            
            # Configure um manipulador de exceção global para encerrar o aplicativo com elegância
            tk.Tk.report_callback_exception = self.handle_uncaught_exception
        
        # Bloco except alinhado com o try da linha 186
        except Exception as e:
            logging.critical(f"Erro durante a inicialização: {str(e)}")
            logging.critical(traceback.format_exc())
            messagebox.showerror("Erro Fatal de Inicialização", 
                               f"Erro durante inicialização: {str(e)}\n\n"
                               "Mais detalhes disponíveis no arquivo de log.")
            root.after(100, root.destroy)

    def load_config_and_db(self):
        """Carrega configurações e inicializa o DatabaseManager."""
        self.config = configparser.ConfigParser()
        config_path = "config/config.ini"
        self.config_path = config_path

        if not os.path.exists(config_path):
            logging.error("Arquivo de configuração não encontrado")
            messagebox.showerror("Erro de Configuração",
                               "Arquivo 'config/config.ini' não encontrado.\n\n"
                               "Execute 'install.py' ou 'verificador.py' para configurar.")
            return False # Indica falha

        try:
            self.config.read(config_path)
            logging.info(f"Configuração lida de {config_path}")

            # Obter configurações do banco de dados
            db_type = self.config.get('DATABASE', 'type', fallback='sqlite')
            db_path = self.config.get('DATABASE', 'path', fallback='database/mt5_data.db')

            # Instanciar DatabaseManager
            logging.info(f"Instanciando DatabaseManager (Tipo: {db_type}, Path: {db_path})...")
            self.db_manager = DatabaseManager(db_type=db_type, db_path=db_path)

            if not self.db_manager.is_connected():
                logging.error("Falha ao conectar ao banco de dados via DatabaseManager.")
                messagebox.showerror("Erro de Banco de Dados",
                                   "Não foi possível conectar ao banco de dados configurado.\n"
                                   "Verifique as configurações e o log 'mt5_app.log'.")
                return False # Indica falha

            logging.info("DatabaseManager inicializado com sucesso.")
            return True # Indica sucesso

        except configparser.Error as e:
            logging.error(f"Erro ao ler arquivo de configuração '{config_path}': {e}")
            messagebox.showerror("Erro de Configuração", f"Erro ao ler '{config_path}':\n{e}")
            return False
        except Exception as e:
            logging.error(f"Erro inesperado ao carregar configuração ou inicializar DB: {e}")
            logging.error(traceback.format_exc())
            messagebox.showerror("Erro Inesperado", f"Erro ao carregar configuração/DB:\n{e}")
            return False
    # Método initialize_mt5() removido - Lógica movida para MT5Connector
    def load_symbols(self):
        """Carrega símbolos disponíveis: MT5 -> JSON -> Fallback."""
        try:
            # Lista mínima de fallback caso tudo falhe
            fallback_minimo = ["WIN$N", "WDO$N", "EURUSD", "PETR4", "AAPL"]
            default_symbols_path = "config/default_symbols.json"
            simbolos_do_json = []

            # Tenta carregar do JSON primeiro para ter uma base
            try:
                with open(default_symbols_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # Combina todas as listas de símbolos do JSON em uma única lista
                    for category in data:
                        if isinstance(data[category], list):
                            simbolos_do_json.extend(data[category])
                    # Remove duplicatas e ordena
                    simbolos_do_json = sorted(list(set(simbolos_do_json)))
                    logging.info(f"Carregados {len(simbolos_do_json)} símbolos do arquivo JSON: {default_symbols_path}")
            except FileNotFoundError:
                logging.warning(f"Arquivo de símbolos padrão '{default_symbols_path}' não encontrado. Usará fallback se MT5 falhar.")
                simbolos_do_json = fallback_minimo # Usa o mínimo se JSON não existe
            except json.JSONDecodeError as e:
                logging.error(f"Erro ao decodificar JSON de símbolos '{default_symbols_path}': {e}. Usará fallback se MT5 falhar.")
                simbolos_do_json = fallback_minimo # Usa o mínimo se JSON inválido
            except Exception as e:
                 logging.error(f"Erro inesperado ao carregar símbolos do JSON '{default_symbols_path}': {e}. Usará fallback se MT5 falhar.")
                 simbolos_do_json = fallback_minimo # Usa o mínimo em erro genérico

            # Lógica de fallback DB removida - Simplificando para MT5 -> JSON -> Mínimo
            # A lógica de fallback DB pode ser reintroduzida se necessário,
            # mas agora focaremos em carregar do MT5 ou do JSON.

            # Se não estamos em modo fallback, verifica se o conector está inicializado
            # A verificação self.mt5_connector já garante que o objeto existe
            # Se MT5 não inicializado, usa a lista do JSON (ou fallback mínimo se JSON falhou)
            if not self.mt5_initialized:
                logging.warning("MT5 não inicializado. Usando símbolos do JSON ou fallback mínimo.")
                self.symbols = simbolos_do_json if simbolos_do_json else fallback_minimo
                self.setup_timeframes() # Configura timeframes mesmo offline
                return

            # Tenta obter símbolos usando o conector
            symbols_data = None
            self.symbols = [] # Limpa a lista antes de tentar preencher

            try:
                # Abordagem 1: get_symbols() padrão
                logging.info("Tentando obter símbolos via connector.get_symbols()...") # Log backend
                symbols_data = self.mt5_connector.get_symbols()
                if symbols_data and len(symbols_data) > 0:
                    logging.info(f"Obtidos {len(symbols_data)} símbolos via get_symbols()") # Log backend
                    self.symbols = [s.name for s in symbols_data]

                # Abordagem 2: get_symbols(group="*")
                # Verifica se a primeira tentativa falhou E se há símbolos totais > 0
                elif not self.symbols and self.mt5_connector.get_total_symbols() > 0:
                     logging.info("Tentando obter símbolos via connector.get_symbols(group='*')...") # Log backend
                     symbols_data = self.mt5_connector.get_symbols(group="*")
                     if symbols_data and len(symbols_data) > 0:
                         logging.info(f"Obtidos {len(symbols_data)} símbolos via get_symbols(group=*)") # Log backend
                         self.symbols = [s.name for s in symbols_data]

                # Abordagem 3: Tentar obter informações de símbolos predefinidos um a um
                # Abordagem 3 removida - Se get_symbols falhou, usamos a lista do JSON
                # A validação individual pode ser muito lenta.

                # Se MT5 falhou em obter símbolos, usa a lista do JSON (ou fallback mínimo)
                if not self.symbols:
                    logging.warning("Falha ao obter símbolos do MT5. Usando lista do JSON ou fallback mínimo.")
                    self.symbols = simbolos_do_json if simbolos_do_json else fallback_minimo

                # Garante que a lista não esteja vazia (redundante, mas seguro)
                if not self.symbols:
                     logging.critical("Lista de símbolos está vazia após todas as tentativas! Usando fallback mínimo.")
                     self.symbols = fallback_minimo # Garante que não fique vazia

                # Ordena a lista final
                self.symbols.sort()
                logging.info(f"Símbolos carregados (final): {len(self.symbols)}")

            except Exception as e:
                # Log backend
                logging.error(f"Erro durante carregamento de símbolos do MT5: {e}")
                logging.debug(traceback.format_exc())
                self.symbols = simbolos_do_json if simbolos_do_json else fallback_minimo
                logging.info(f"Utilizando {len(self.symbols)} símbolos predefinidos devido a erro.") # Log backend

            # Configura os timeframes
            self.setup_timeframes()

        except Exception as e:
            # Log backend
            logging.error(f"Erro geral fatal em load_symbols: {e}")
            logging.debug(traceback.format_exc())
            self.symbols = simbolos_do_json if simbolos_do_json else fallback_minimo
            self.timeframes = [] # Limpa timeframes em caso de erro grave aqui

    def setup_timeframes(self):
        """Configura os timeframes disponíveis usando o MT5Connector."""
        if not self.mt5_connector:
            logging.error("MT5Connector não instanciado ao chamar setup_timeframes.")
            # Define um fallback mínimo absoluto
            self.timeframes = [("1 minuto", 1), ("5 minutos", 5), ("15 minutos", 15)]
            return

        try:
            # Obtém a lista de timeframes do conector
            self.timeframes = self.mt5_connector.get_available_timeframes()
            if not self.timeframes: # Garante que não seja None ou vazio
                 logging.warning("get_available_timeframes retornou vazio. Usando fallback mínimo.")
                 self.timeframes = [("1 minuto", 1), ("5 minutos", 5), ("15 minutos", 15)]

            logging.info(f"Timeframes configurados: {len(self.timeframes)} opções.") # Log backend

        except Exception as e:
            # Log backend
            logging.error(f"Erro ao obter timeframes do conector: {e}")
            logging.debug(traceback.format_exc())
            self.timeframes = [("1 minuto", 1), ("5 minutos", 5), ("15 minutos", 15)]

    # Método setup_ui removido - Lógica será movida para UIManager

    # Métodos filter_symbols, add_symbols, remove_symbols movidos para UIManager
            self.log(f"Símbolo removido: {symbol}")

    # Método log removido - A atualização da UI é feita pelo UIManager.
    # O log para arquivo/console pode ser feito diretamente onde necessário
    # usando logging.info() ou similar.
        # Adiciona ao widget de texto se disponível
        if hasattr(self, 'log_text') and self.log_text is not None:
            try:
                self.log_text.insert(tk.END, log_message)
                self.log_text.see(tk.END)
            except Exception as e:
                logging.error(f"Erro ao adicionar mensagem ao log_text: {e}")
                # Não propaga a exceção para não interromper a execução

    # Métodos start_collection e stop_collection movidos para UIManager
    # A lógica de negócio será chamada a partir do UIManager

    # Métodos de lógica de negócio correspondentes (a serem chamados pelo UIManager)
    def start_collection_logic(self):
        """Inicia a lógica de coleta de dados usando o DataCollector."""
        if not self.data_collector:
            logging.error("DataCollector não inicializado.")
            if self.ui_manager: self.ui_manager.log("ERRO: Coletor de dados não inicializado.")
            return
        if not self.selected_symbols:
             logging.warning("Nenhum símbolo selecionado para iniciar a coleta.")
             if self.ui_manager: self.ui_manager.log("AVISO: Nenhum símbolo selecionado.")
             return

        # Verifica se o MT5 está conectado
        if not self.mt5_initialized or not self.mt5_connector or not self.mt5_connector.is_initialized:
            logging.info("MT5 não está conectado. Tentando reconectar...")
            if self.ui_manager: 
                # Avisa o usuário que irá tentar conectar ao MT5
                messagebox.showinfo("Conexão MT5", "O MT5 está desconectado. Iremos conectar ao MT5 para continuar com a extração.")
                self.ui_manager.log("MT5 desconectado. Tentando reconectar...")
                self.ui_manager.update_status("Status: Reconectando ao MT5...")
            
            # Tenta inicializar o MT5
            if self.mt5_connector and self.mt5_connector.initialize():
                self.mt5_initialized = True
                status = self.mt5_connector.get_connection_status()
                logging.info(f"Conexão MT5 reestabelecida. Modo: {status.get('mode', 'N/A')}")
                if self.ui_manager:
                    self.ui_manager.log(f"MT5 conectado com sucesso. Modo: {status.get('mode', 'N/A')}")
                    self.ui_manager.update_status("Status: MT5 reconectado!")
                    messagebox.showinfo("Conexão MT5", "Conexão com MT5 estabelecida com sucesso. Continuando com a extração.")
            else:
                # Se falhar na reconexão, não permite continuar
                logging.error("Falha ao reconectar ao MT5. Não é possível prosseguir com a coleta.")
                if self.ui_manager:
                    self.ui_manager.log("ERRO: Falha ao reconectar ao MT5. Não é possível prosseguir.")
                    self.ui_manager.update_status("Status: Falha na conexão com MT5")
                    self.ui_manager.toggle_collection_buttons(collecting=False)
                    messagebox.showerror("Erro MT5", "Não foi possível estabelecer conexão com o MT5. A coleta de dados não pode ser iniciada.")
                return

        logging.info(f"Configurando e iniciando DataCollector para: {self.selected_symbols}")
        self.data_collector.set_symbols(self.selected_symbols)
        self.data_collector.start()
        # A UI já foi atualizada pelo UIManager que chamou este método

    def stop_collection_logic(self):
        """Para a lógica de coleta de dados usando o DataCollector."""
        if not self.data_collector:
            logging.error("DataCollector não inicializado ao tentar parar.")
            return
        logging.info("Parando DataCollector...")
        self.data_collector.stop()
        # A UI já foi atualizada pelo UIManager que chamou este método

    # Métodos collection_loop e fetch_and_save_data movidos para DataCollector

    # Métodos calculate_indicators e calculate_variations removidos
    # Lógica movida para IndicatorCalculator

    def get_dom_data(self, symbol):
        """Obtém dados de profundidade de mercado (DOM) usando o conector"""
        if not self.mt5_initialized or not self.mt5_connector:
             logging.warning(f"Não é possível obter DOM para {symbol}. Conexão MT5 não inicializada.")
             if self.ui_manager: self.ui_manager.log(f"Não é possível obter DOM para {symbol}. Conexão MT5 não inicializada.")
             return None

        try:
            # Chama o método do conector que encapsula a lógica
            book = self.mt5_connector.get_market_book(symbol)

            if book:
                # Processa o book retornado (tupla de MarketBookInfo)
                dom_levels = {"bids": [], "asks": []}
                # A lógica de ordenação e extração de 5 níveis pode ser feita aqui
                # ou movida para o conector se for reutilizada.
                # Mantendo aqui por enquanto para refletir a lógica original.

                # Separa bids e asks
                bids = sorted([item for item in book if item.type == 0], key=lambda x: x.price, reverse=True) # type 0 = BID
                asks = sorted([item for item in book if item.type == 1], key=lambda x: x.price) # type 1 = ASK

                # Pega os 5 melhores níveis
                dom_levels["bids"] = [{"price": b.price, "volume": b.volume} for b in bids[:5]]
                dom_levels["asks"] = [{"price": a.price, "volume": a.volume} for a in asks[:5]]

                # Retorna apenas se houver dados
                return dom_levels if dom_levels["bids"] or dom_levels["asks"] else None
            else:
                # O conector já logou o erro
                logging.warning(f"Não foi possível obter DOM para {symbol} via conector.")
                if self.ui_manager: self.ui_manager.log(f"Não foi possível obter DOM para {symbol} via conector.")
                return None
        except Exception as e:
            logging.error(f"Erro ao processar DOM para {symbol} após obter do conector: {str(e)}")
            if self.ui_manager: self.ui_manager.log(f"Erro ao processar DOM para {symbol} após obter do conector: {str(e)}")
            logging.debug(traceback.format_exc())
            return None

    def detect_candle_pattern(self, row):
        """Detecta padrões de candle básicos"""
        open_price = row['open']
        close = row['close']
        high = row['high']
        low = row['low']

        body_size = abs(open_price - close)
        total_size = high - low

        # Doji (corpo muito pequeno)
        if body_size <= 0.1 * total_size:
            return "Doji"

        # Martelo (cauda inferior longa, corpo pequeno no topo)
        if (close > open_price and  # Candle de alta
            (high - close) < 0.3 * body_size and  # Sombra superior pequena
            (open_price - low) > 2 * body_size):  # Sombra inferior longa
            return "Hammer"

        # Shooting Star (cauda superior longa, corpo pequeno embaixo)
        if (open_price > close and  # Candle de baixa
            (high - open_price) > 2 * body_size and  # Sombra superior longa
            (close - low) < 0.3 * body_size):  # Sombra inferior pequena
            return "Shooting Star"

        # Engulfing de alta
        # Precisaria de dados do candle anterior

        # Engulfing de baixa
        # Precisaria de dados do candle anterior

        # Candle grande de alta
        if close > open_price and body_size > 0.7 * total_size:
            return "Strong Bull"

        # Candle grande de baixa
        if open_price > close and body_size > 0.7 * total_size:
            return "Strong Bear"

        return "No Pattern"

    def simulate_trader_sentiment(self, df):
        """Simula o sentimento do mercado com base no volume e direção do preço"""
        try:
            # Obter últimos 10 candles ou menos se não houver 10
            last_n = min(10, len(df))
            recent_df = df.iloc[-last_n:]

            # Contar candles de alta e baixa
            up_candles = sum(recent_df['close'] > recent_df['open'])
            down_candles = sum(recent_df['close'] < recent_df['open'])

            # Calcular volume em candles de alta vs. baixa
            up_volume = recent_df[recent_df['close'] > recent_df['open']]['tick_volume'].sum()
            down_volume = recent_df[recent_df['close'] < recent_df['open']]['tick_volume'].sum()

            total_volume = up_volume + down_volume

            if total_volume > 0:
                bull_percentage = (up_volume / total_volume) * 100
            else:
                bull_percentage = 50  # Neutro se não há volume

            return bull_percentage
        except:
            return 50  # Valor neutro em caso de erro

    def fetch_and_save_fallback_data(self, symbol):
        """Obtém dados do banco de dados de fallback e salva no banco de dados principal"""
        try:
            # Conecta ao banco de dados de fallback
            fallback_conn = sqlite3.connect(self.fallback_db_path)

            # Obtém dados da última hora (60 candles de 1 minuto)
            table_name = f"{symbol.lower()}_1min"
            query = f"SELECT * FROM {table_name} ORDER BY time DESC LIMIT 60"

            try:
                df = pd.read_sql_query(query, fallback_conn)
                if df.empty:
                    logging.warning(f"Aviso: Sem dados para {symbol} no banco de dados de fallback")
                    if self.ui_manager: self.ui_manager.log(f"Aviso: Sem dados para {symbol} no banco de dados de fallback")
                    fallback_conn.close()
                    return

                logging.info(f"Obtidos {len(df)} registros de {symbol} do banco de dados de fallback")
                if self.ui_manager: self.ui_manager.log(f"Obtidos {len(df)} registros de {symbol} do banco de dados de fallback")

                # Converter timestamp para datetime
                df['time'] = pd.to_datetime(df['time'])

                # Salvar no banco de dados principal
                self.save_to_database(symbol, df)

                # Atualizar último preço na interface
                if hasattr(self, 'price_labels') and symbol in self.price_labels:
                    last_price = df['close'].iloc[0] if not df.empty else 0
                    self.price_labels[symbol].config(text=f"{last_price:.5f}")

            except Exception as e:
                logging.error(f"Erro ao obter dados de fallback para {symbol}: {str(e)}")
                if self.ui_manager: self.ui_manager.log(f"Erro ao obter dados de fallback para {symbol}: {str(e)}")
            finally:
                fallback_conn.close()

        except Exception as e:
            logging.error(f"Erro no modo fallback para {symbol}: {str(e)}")
            if self.ui_manager: self.ui_manager.log(f"Erro no modo fallback para {symbol}: {str(e)}")

    # Método extract_historical_data movido para UIManager

#    def start_historical_extraction_logic(self, symbols, timeframe_val, timeframe_name,
#                                          start_date, end_date, max_bars,
#                                          include_indicators, overwrite,
#                                          auto_detect_oldest_date=True,  # Novo parâmetro
#                                          update_progress_callback=None, finished_callback=None):
#        """Inicia a lógica de extração de dados históricos em background."""
#        if not self.mt5_initialized:
#            logging.error("Tentativa de iniciar extração histórica sem MT5 inicializado.")
#            if finished_callback:
#                finished_callback(0, len(symbols)) # Indica falha total
#            return
#
#        if hasattr(self, 'extraction_running') and self.extraction_running:
#            logging.warning("Tentativa de iniciar extração histórica já em andamento.")
#            return
#
#        logging.info(f"Iniciando extração histórica para {len(symbols)} símbolos...")
#        self.extraction_running = True
#
#        def extraction_thread():
#            total_symbols = len(symbols)
#            successful_symbols = 0
#            failed_symbols = 0
#
#            for i, symbol in enumerate(symbols):
#                if not self.extraction_running:
#                    logging.info("Extração histórica cancelada pelo usuário.")
#                    break
#
#                progress = (i / total_symbols) * 100
#                message = f"Extraindo {symbol} ({i+1}/{total_symbols})"
#                if update_progress_callback:
#                    update_progress_callback(progress, message)
#
#                try:
#                    # Obter dados históricos usando o conector com o método mais robusto
#                    df = self.mt5_connector.get_historical_data(
#                        symbol,
#                        timeframe_val,
#                        start_dt=start_date,
#                        end_dt=end_date
#                    )
#
#                    # Se não encontrou dados e auto-detecção está ativada
#                    if (df is None or df.empty) and auto_detect_oldest_date:
#                        msg = f"Sem dados para {symbol} no período solicitado. Tentando detectar data mais antiga..."
#                        logging.info(msg)
#                        if update_progress_callback:
#                            update_progress_callback(progress, msg)
#
#                        # Pausa para atualizar a interface
#                        time.sleep(0.1)
#
#                        # Tentar obter data mais antiga
#                        oldest_date = self.mt5_connector.get_oldest_available_date(symbol, timeframe_val)
#
#                        if oldest_date:
#                            # Usar a data mais antiga detectada, mas manter a data final original
#                            adjusted_start = oldest_date
#                            msg = f"Data mais antiga para {symbol}: {adjusted_start}. Ajustando extração..."
#                            logging.info(msg)
#                            if update_progress_callback:
#                                update_progress_callback(progress, msg)
#
#                            # Pausa para atualizar a interface
#                            time.sleep(0.1)
#
#                            # Tentar novamente com a data ajustada
#                            df = self.mt5_connector.get_historical_data(
#                                symbol,
#                                timeframe_val,
#                                start_dt=adjusted_start,
#                                end_dt=end_date
#                            )
#
#                    if df is None:
#                        logging.warning(f"Falha ao buscar dados históricos para {symbol} via connector.")
#                        failed_symbols += 1
#                        continue
#
#                    if df.empty:
#                        logging.info(f"Sem dados históricos para {symbol} no período selecionado via connector.")
#                        failed_symbols += 1
#                        continue
#
#                    # O método get_historical_data já converte timestamp para datetime
#                    # então não precisamos fazer essa conversão
#
#                    # Calcular indicadores técnicos se solicitado
#                    if include_indicators:
#                        df = self.indicator_calculator.calculate_technical_indicators(df)
#
#                    # Obter spread médio (simulação, pode não ser preciso para histórico)
#                    symbol_info = self.mt5_connector.get_symbol_info(symbol)
#                    if symbol_info:
#                        df['spread'] = symbol_info.spread
#
#                    # Salvar no banco de dados
#                    # TODO: Implementar lógica de 'overwrite' no DatabaseManager
#                    if overwrite:
#                        logging.info(f"Opção 'Sobrescrever' para {symbol} ({timeframe_name})... (Implementação Pendente)")
#                        # self.db_manager.delete_data(symbol, timeframe_name, start_date, end_date) # Exemplo
#
#                    try:
#                        success = self.db_manager.save_ohlcv_data(symbol, timeframe_name, df)
#                        if success:
#                            logging.info(f"Extraídos e salvos {len(df)} registros de {symbol} ({timeframe_name})")
#                            successful_symbols += 1
#                        else:
#                            logging.warning(f"Falha ao salvar dados históricos de {symbol} ({timeframe_name}) via DB Manager.")
#                            failed_symbols += 1
#                    except Exception as db_err:
#                        logging.error(f"Erro inesperado ao salvar dados históricos de {symbol} ({timeframe_name}): {db_err}")
#                        logging.debug(traceback.format_exc())
#                        failed_symbols += 1
#
#                except Exception as e:
#                    logging.error(f"Erro ao extrair dados de {symbol}: {str(e)}")
#                    logging.error(f"Erro detalhado para {symbol}: {traceback.format_exc()}")
#                    failed_symbols += 1
#
#            # Finalização
#            final_message = f"Extração concluída. Sucesso: {successful_symbols}, Falhas: {failed_symbols}"
#            logging.info(final_message)
#            if update_progress_callback:
#                update_progress_callback(100, final_message)
#            if finished_callback:
#                finished_callback(successful_symbols, failed_symbols)
#            self.extraction_running = False # Reseta o estado
#
#        # Iniciar thread
#        self.historical_thread = Thread(target=extraction_thread)
#        self.historical_thread.daemon = True
#        self.historical_thread.start()

#    def cancel_historical_extraction_logic(self):
#        """Sinaliza para parar a extração histórica em andamento."""
#        if hasattr(self, 'extraction_running') and self.extraction_running:
#            logging.info("Sinalizando cancelamento da extração histórica.")
#            self.extraction_running = False
#        else:
#            logging.warning("Tentativa de cancelar extração histórica que não está em andamento.")

    def setup_root_window(self):
        """Configura a janela principal da aplicação"""
        self.root.title("MT5 Extração de Dados")
        
        # Aumentar o tamanho em 40%
        self.root.geometry("1680x980")  # 1200 * 1.4 = 1680, 700 * 1.4 = 980
        
        # Configuração para melhor exibição em monitores de alta resolução
        try:
            from ctypes import windll
            windll.shcore.SetProcessDpiAwareness(1)
        except:
            pass
            
        # Configuração de visualização de pandas
        pd.set_option('display.max_columns', 500)
        pd.set_option('display.width', 1500)
        
        # Inicializa variáveis importantes para evitar erros
        self.symbols = []
        self.timeframes = []
        self.selected_symbol = tk.StringVar()
        self.selected_timeframe = tk.StringVar()
        self.start_date = None
        self.end_date = None
        self.selected_symbols = []
        self.mt5_initialized = False  # Inicialmente, o MT5 não está inicializado
    
    def update_table_info(self):
        """Atualiza informações sobre tabelas existentes no banco de dados"""
        if not hasattr(self, 'db_manager') or not self.db_manager:
            logging.warning("Não foi possível atualizar info das tabelas: DatabaseManager não inicializado")
            return
            
        try:
            tables = self.db_manager.get_all_tables()
            if tables:
                logging.info(f"Encontradas {len(tables)} tabelas com dados no banco")
                
                # Extrair símbolos únicos das tabelas
                symbols_with_data = set()
                for table in tables:
                    # Assumindo formato de tabela symbol_timeframe
                    parts = table.split('_')
                    if len(parts) >= 2:
                        symbol = parts[0]
                        symbols_with_data.add(symbol.upper())
                
                logging.info(f"Identificados {len(symbols_with_data)} símbolos com dados existentes")
                
                # Se temos UIManager, atualizar informações na interface
                if hasattr(self, 'ui_manager') and self.ui_manager:
                    self.ui_manager.update_symbols_with_data(symbols_with_data)
        except Exception as e:
            logging.error(f"Erro ao atualizar informações de tabelas: {e}")
    
    def handle_uncaught_exception(self, exc_type, exc_value, exc_traceback):
        """
        Manipulador para exceções não tratadas.
        Registra o erro e encerra os recursos necessários antes de fechar a aplicação.
        """
        # Log da exceção
        logging.critical("Exceção não tratada!")
        logging.critical(f"Tipo: {exc_type.__name__}")
        logging.critical(f"Mensagem: {exc_value}")
        
        # Obter e registrar a pilha de chamadas formatada
        tb_lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        log_traceback = ''.join(tb_lines)
        logging.critical(f"Traceback:\n{log_traceback}")
        
        # Encerrar conexões e recursos
        try:
            logging.info("Encerrando conexões devido a erro...")
            
            # Encerrar serviços integrados
            if hasattr(self, 'integrated_services') and self.integrated_services:
                try:
                    self.integrated_services.shutdown()
                    logging.info("Serviços avançados encerrados")
                except Exception as e:
                    logging.error(f"Erro ao encerrar serviços avançados: {str(e)}")
            
            # Encerrar conexão MT5
            if hasattr(self, 'mt5_connector') and self.mt5_connector:
                try:
                    self.mt5_connector.shutdown()
                    logging.info("Conexão MT5 encerrada")
                except Exception as e:
                    logging.error(f"Erro ao encerrar conexão MT5: {str(e)}")
                
            # Encerrar conexão DB
            if hasattr(self, 'db_manager') and self.db_manager:
                try:
                    self.db_manager.close()
                    logging.info("Conexão com banco de dados encerrada")
                except Exception as e:
                    logging.error(f"Erro ao encerrar conexão com banco: {str(e)}")
        except Exception as cleanup_error:
            logging.critical(f"Erro durante encerramento de emergência: {str(cleanup_error)}")
        
        # Mostrar mensagem de erro para o usuário
        messagebox.showerror("Erro Fatal", 
                           f"Ocorreu um erro fatal:\n{exc_type.__name__}: {exc_value}\n\n"
                           "A aplicação será encerrada. Consulte o log para mais detalhes.")
        
        # Encerrar a aplicação após um breve delay
        self.root.after(100, self.root.destroy)

if __name__ == "__main__":
    try:
        root = tk.Tk()
        app = MT5Extracao(root)
        root.mainloop()
    finally:
        # Desconectar MT5 ao sair
        mt5.shutdown()