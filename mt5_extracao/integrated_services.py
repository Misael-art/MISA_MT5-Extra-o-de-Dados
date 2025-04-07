import logging
import pandas as pd
import numpy as np
import time
import traceback
from typing import Dict, List, Callable, Any, Optional, Union, Tuple
from pathlib import Path
import os
import json
from datetime import datetime, timedelta
import threading
import queue

# Importação dos módulos internos
from mt5_extracao.enhanced_indicators import EnhancedIndicatorCalculator
from mt5_extracao.market_data_analyzer import MarketDataAnalyzer
from mt5_extracao.enhanced_calculation_service import EnhancedCalculationService
from mt5_extracao.performance_optimizer import PerformanceOptimizer
from mt5_extracao.mt5_connector import MT5Connector
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.data_collector import DataCollector

# Configuração de logging
log = logging.getLogger(__name__)
if not log.handlers:
    log.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    log.addHandler(ch)

class IntegratedServices:
    """
    Classe que integra todos os serviços avançados de cálculo, análise e 
    otimização de performance para facilitar o uso e minimizar redundâncias.
    
    Atua como uma fachada (Facade) para os diversos serviços,
    simplificando o acesso às funcionalidades do sistema.
    """
    
    def __init__(self, config_path: str = None):
        """
        Inicializa o gerenciador de serviços integrados.
        
        Args:
            config_path: Caminho para o arquivo de configuração (opcional)
        """
        # Carrega configurações
        self.config = self._load_configuration(config_path)
        
        # Status da inicialização
        self.initialized = False
        self.initialization_errors = []
        
        # Serviços principais
        self.performance_optimizer = None
        self.calculation_service = None
        self.market_analyzer = None
        self.indicator_calculator = None
        self.mt5_connector = None
        self.database_manager = None
        self.data_collector = None
        
        # Configurações de recursos
        self.max_workers = self.config.get('max_workers', None)
        self.cache_size = self.config.get('cache_size', 100)
        self.batch_size = self.config.get('batch_size', 1000)
        
        # Inicializa serviços
        self._initialize_services()
        
        log.info("IntegratedServices inicializado")
        
    def _load_configuration(self, config_path: str = None) -> Dict:
        """
        Carrega configurações a partir de um arquivo JSON ou usa padrões.
        
        Args:
            config_path: Caminho para o arquivo de configuração
            
        Returns:
            Dicionário com as configurações
        """
        default_config = {
            'max_workers': None,  # Autodetectar
            'cache_size': 100,
            'batch_size': 1000,
            'optimize_memory': True,
            'use_market_context': True,
            'required_indicators': ['rsi', 'macd', 'stochastic'],
            'db_connection_string': 'sqlite:///mt5_data.db'
        }
        
        if config_path and os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    user_config = json.load(f)
                    # Mescla configurações
                    config = {**default_config, **user_config}
                    log.info(f"Configurações carregadas de {config_path}")
                    return config
            except Exception as e:
                log.error(f"Erro ao carregar configurações: {str(e)}")
                log.debug(traceback.format_exc())
                
        return default_config
        
    def _initialize_services(self):
        """Inicializa todos os serviços necessários."""
        try:
            # Inicializa o otimizador de performance primeiro
            self.performance_optimizer = PerformanceOptimizer(
                target_cpu_usage=self.config.get('target_cpu_usage', 80.0),
                memory_threshold=self.config.get('memory_threshold', 75.0)
            )
            
            # Ajusta recursos com base nas recomendações do otimizador
            if self.max_workers is None:
                self.max_workers = self.performance_optimizer.recommend_parallel_workers()
                
            # Ajusta tamanho do lote se necessário
            if self.performance_optimizer.should_optimize():
                self.batch_size = self.performance_optimizer.recommended_batch_size(self.batch_size)
            
            # Inicializa calculadoras
            self.indicator_calculator = EnhancedIndicatorCalculator()
            self.market_analyzer = MarketDataAnalyzer()
            
            # Inicializa o serviço de cálculo
            self.calculation_service = EnhancedCalculationService(
                max_workers=self.max_workers,
                cache_size=self.cache_size
            )
            
            # Inicia o serviço de cálculo
            self.calculation_service.start()
            
            # Inicializa os serviços de dados, se as configurações necessárias existirem
            if self.config.get('mt5_init', False):
                # Inicializa MT5
                self.mt5_connector = MT5Connector(
                    server=self.config.get('mt5_server', None),
                    login=self.config.get('mt5_login', None),
                    password=self.config.get('mt5_password', None),
                    path=self.config.get('mt5_path', None)
                )
                
            # Inicializa banco de dados
            if self.config.get('db_connection_string'):
                db_conn = self.config.get('db_connection_string')
                if db_conn.startswith('sqlite:///'):
                    db_path = db_conn.replace('sqlite:///', '')
                    self.database_manager = DatabaseManager(
                        db_type='sqlite',
                        db_path=db_path
                    )
                else:
                    log.warning(f"String de conexão não suportada: {db_conn}")
                
            # Inicializa coletor de dados se temos MT5 e DB
            if self.mt5_connector and self.database_manager:
                self.data_collector = DataCollector(
                    mt5_connector=self.mt5_connector,
                    database_manager=self.database_manager
                )
                
            self.initialized = True
            
        except Exception as e:
            log.error(f"Erro ao inicializar serviços: {str(e)}")
            log.debug(traceback.format_exc())
            self.initialization_errors.append(str(e))
            self.initialized = False
            
    def shutdown(self):
        """Encerra todos os serviços de forma segura."""
        log.info("Encerrando serviços integrados...")
        
        # Para o serviço de cálculo
        if self.calculation_service:
            try:
                self.calculation_service.stop()
                log.info("Serviço de cálculo encerrado")
            except Exception as e:
                log.error(f"Erro ao encerrar serviço de cálculo: {str(e)}")
                
        # Desconecta do MT5
        if self.mt5_connector:
            try:
                self.mt5_connector.shutdown()
                log.info("Conexão MT5 encerrada")
            except Exception as e:
                log.error(f"Erro ao encerrar conexão MT5: {str(e)}")
                
        # Fecha conexões com o banco de dados
        if self.database_manager:
            try:
                self.database_manager.close()
                log.info("Conexões com banco encerradas")
            except Exception as e:
                log.error(f"Erro ao encerrar conexões com banco: {str(e)}")
                
        log.info("Todos os serviços foram encerrados")
        
    def get_status(self) -> Dict:
        """
        Retorna o status atual de todos os serviços.
        
        Returns:
            Dicionário com status dos serviços e recursos
        """
        status = {
            'initialized': self.initialized,
            'initialization_errors': self.initialization_errors,
            'system_status': None,
            'calculation_service': None,
            'mt5_connector': None,
            'database_manager': None
        }
        
        # Performance e recursos
        if self.performance_optimizer:
            status['system_status'] = self.performance_optimizer.get_performance_report()
            
        # Serviço de cálculo
        if self.calculation_service:
            status['calculation_service'] = {
                'running': self.calculation_service.running,
                'workers': len(self.calculation_service.workers),
                'queue_size': self.calculation_service.job_queue.qsize(),
                'cache_size': len(self.calculation_service.cache)
            }
            
        # MT5
        if self.mt5_connector:
            status['mt5_connector'] = {
                'connected': self.mt5_connector.is_connected(),
                'connection_errors': self.mt5_connector.get_connection_stats().get('errors', 0),
                'last_error': self.mt5_connector.last_error
            }
            
        # Banco de dados
        if self.database_manager:
            status['database_manager'] = {
                'connected': self.database_manager.is_connected(),
                'tables': len(self.database_manager.get_table_names())
            }
            
        return status
        
    # ===== FUNCIONALIDADES DE DADOS =====
    
    def fetch_market_data(self, symbol: str, timeframe: str = '1m', 
                        num_bars: int = 1000) -> pd.DataFrame:
        """
        Obtém dados de mercado para um símbolo e timeframe.
        
        Args:
            symbol: Símbolo a buscar
            timeframe: Timeframe dos dados
            num_bars: Número de barras a buscar
            
        Returns:
            DataFrame com os dados
        """
        if not self.mt5_connector:
            log.error("MT5Connector não inicializado")
            return pd.DataFrame()
            
        try:
            # Obtém dados do mercado
            df = self.mt5_connector.get_rates(symbol, timeframe, num_bars)
            
            # Otimiza o DataFrame se necessário
            if self.config.get('optimize_memory', True) and self.performance_optimizer:
                df = self.performance_optimizer.optimize_dataframe(df)
                
            return df
            
        except Exception as e:
            log.error(f"Erro ao buscar dados: {str(e)}")
            log.debug(traceback.format_exc())
            return pd.DataFrame()
            
    def get_historical_data(self, symbol: str, timeframe: str, 
                         start_date: Union[str, datetime],
                         end_date: Union[str, datetime] = None) -> pd.DataFrame:
        """
        Obtém dados históricos para um período específico.
        
        Args:
            symbol: Símbolo a buscar
            timeframe: Timeframe dos dados
            start_date: Data inicial
            end_date: Data final (se None, usa data atual)
            
        Returns:
            DataFrame com os dados históricos
        """
        if not self.mt5_connector:
            log.error("MT5Connector não inicializado")
            return pd.DataFrame()
            
        try:
            # Converte datas se necessário
            if isinstance(start_date, str):
                start_date = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
                
            if end_date is None:
                end_date = datetime.now()
            elif isinstance(end_date, str):
                end_date = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")
                
            # Obtém dados históricos
            df = self.mt5_connector.get_historical_data(
                symbol=symbol,
                timeframe=timeframe,
                from_date=start_date,
                to_date=end_date
            )
            
            # Otimiza o DataFrame se necessário
            if self.config.get('optimize_memory', True) and self.performance_optimizer:
                df = self.performance_optimizer.optimize_dataframe(df)
                
            return df
            
        except Exception as e:
            log.error(f"Erro ao buscar dados históricos: {str(e)}")
            log.debug(traceback.format_exc())
            return pd.DataFrame()
            
    def search_db_data(self, symbol: str, timeframe: str, 
                     limit: int = 1000) -> pd.DataFrame:
        """
        Busca dados armazenados no banco de dados.
        
        Args:
            symbol: Símbolo a buscar
            timeframe: Timeframe dos dados
            limit: Limite de registros
            
        Returns:
            DataFrame com os dados do banco
        """
        if not self.database_manager:
            log.error("DatabaseManager não inicializado")
            return pd.DataFrame()
            
        try:
            # Converte timeframe para formato da tabela
            table_name = f"{symbol.lower().replace('$', '_')}_{timeframe}"
            
            # Verifica se a tabela existe
            if not self.database_manager.table_exists(table_name):
                log.warning(f"Tabela {table_name} não encontrada no banco")
                return pd.DataFrame()
                
            # Busca dados
            df = self.database_manager.fetch_data(table_name, limit=limit)
            
            # Otimiza o DataFrame se necessário
            if self.config.get('optimize_memory', True) and self.performance_optimizer:
                df = self.performance_optimizer.optimize_dataframe(df)
                
            return df
            
        except Exception as e:
            log.error(f"Erro ao buscar dados do banco: {str(e)}")
            log.debug(traceback.format_exc())
            return pd.DataFrame()
            
    def save_data_to_db(self, df: pd.DataFrame, table_name: str, if_exists: str = 'append') -> bool:
        """
        Salva dados no banco de dados.
        
        Args:
            df: DataFrame com os dados
            table_name: Nome da tabela
            if_exists: Ação caso a tabela exista ('append', 'replace', 'fail')
            
        Returns:
            True se salvou com sucesso, False em caso de erro
        """
        if not self.database_manager:
            log.error("DatabaseManager não inicializado")
            return False
            
        try:
            # Otimiza o DataFrame antes de salvar
            if self.config.get('optimize_memory', True) and self.performance_optimizer:
                df = self.performance_optimizer.optimize_dataframe(df)
                
            # Salva os dados
            self.database_manager.save_data(df, table_name, if_exists=if_exists)
            return True
            
        except Exception as e:
            log.error(f"Erro ao salvar dados no banco: {str(e)}")
            log.debug(traceback.format_exc())
            return False
            
    # ===== FUNCIONALIDADES DE CÁLCULO =====
    
    def process_data(self, df: pd.DataFrame, symbol: str = None, 
                   include_market_context: bool = None,
                   include_advanced_stats: bool = True,
                   use_async: bool = False,
                   timeout: Optional[float] = 60.0) -> Union[pd.DataFrame, str]:
        """
        Processa um DataFrame com cálculo completo de indicadores.
        
        Args:
            df: DataFrame com dados OHLCV
            symbol: Símbolo associado aos dados (para análise de mercado)
            include_market_context: Incluir contexto de mercado
            include_advanced_stats: Incluir estatísticas avançadas
            use_async: Usar processamento assíncrono
            timeout: Tempo máximo de espera para processamento síncrono
            
        Returns:
            DataFrame processado ou job_id (se assíncrono)
        """
        if df is None or df.empty:
            log.warning("DataFrame vazio recebido para processamento")
            return df
            
        # Usa valor da configuração se não especificado
        if include_market_context is None:
            include_market_context = self.config.get('use_market_context', True)
            
        # Se o DataFrame for grande, usa processamento em lotes
        if len(df) > self.batch_size and self.calculation_service:
            if use_async:
                log.info(f"Processando dataset grande ({len(df)} linhas) de forma assíncrona")
                return self.calculation_service.process_large_dataset(
                    df, batch_size=self.batch_size, symbol=symbol)
            else:
                log.info(f"Processando dataset grande ({len(df)} linhas) em lotes")
                return self.calculation_service.process_large_dataset(
                    df, batch_size=self.batch_size, symbol=symbol)
        
        # Para DataFrames menores, usa o serviço de cálculo diretamente
        if self.calculation_service:
            if use_async:
                return self.calculation_service.calculate_indicators_async(
                    df,
                    include_market_context=include_market_context,
                    include_advanced_stats=include_advanced_stats,
                    symbol=symbol
                )
            else:
                return self.calculation_service.calculate_indicators_sync(
                    df,
                    include_market_context=include_market_context,
                    include_advanced_stats=include_advanced_stats,
                    symbol=symbol,
                    timeout=timeout
                )
        
        # Fallback para cálculo direto se o serviço não estiver disponível
        try:
            result_df = df.copy()
            
            # Cálculo de indicadores técnicos
            result_df = self.indicator_calculator.calculate_all_indicators(
                result_df,
                include_market_context=False,
                include_advanced_stats=include_advanced_stats
            )
            
            # Análise de mercado (opcional)
            if include_market_context and symbol and self.market_analyzer:
                result_df = self.market_analyzer.analyze_market_data(result_df, symbol)
                
            return result_df
            
        except Exception as e:
            log.error(f"Erro no processamento de dados: {str(e)}")
            log.debug(traceback.format_exc())
            return df
            
    def get_job_status(self, job_id: str) -> Dict:
        """
        Verifica o status de um job de processamento assíncrono.
        
        Args:
            job_id: ID do job
            
        Returns:
            Dicionário com status e resultados
        """
        if not self.calculation_service:
            return {'status': 'error', 'error': 'Serviço de cálculo não disponível'}
            
        return self.calculation_service.get_job_result(job_id)
        
    def wait_for_result(self, job_id: str, timeout: Optional[float] = None) -> Dict:
        """
        Aguarda o resultado de um processamento assíncrono.
        
        Args:
            job_id: ID do job
            timeout: Tempo máximo de espera em segundos
            
        Returns:
            Dicionário com resultado ou erro
        """
        if not self.calculation_service:
            return {'status': 'error', 'error': 'Serviço de cálculo não disponível'}
            
        return self.calculation_service.wait_for_job(job_id, timeout)
        
    # ===== FUNCIONALIDADES AVANÇADAS =====
    
    def analyze_symbol(self, symbol: str, timeframe: str = '1m', 
                      num_bars: int = 500, use_db: bool = False) -> pd.DataFrame:
        """
        Realiza uma análise completa de um símbolo, buscando dados e processando-os.
        
        Args:
            symbol: Símbolo a analisar
            timeframe: Timeframe para análise
            num_bars: Número de barras a analisar
            use_db: Usar dados do banco em vez de buscar do MT5
            
        Returns:
            DataFrame com dados processados e análises
        """
        # Obtém os dados
        if use_db and self.database_manager:
            table_name = f"{symbol.lower().replace('$', '_')}_{timeframe}"
            df = self.search_db_data(symbol, timeframe, limit=num_bars)
        elif self.mt5_connector:
            df = self.fetch_market_data(symbol, timeframe, num_bars)
        else:
            log.error("Nenhuma fonte de dados disponível para análise")
            return pd.DataFrame()
            
        if df.empty:
            log.warning(f"Não foi possível obter dados para {symbol} no timeframe {timeframe}")
            return df
            
        # Processa os dados
        result = self.process_data(df, symbol)
        
        return result
        
    def schedule_data_collection(self, symbols: List[str], 
                              timeframes: List[str], 
                              interval_seconds: int = 60,
                              max_runtime_minutes: Optional[int] = None) -> Dict:
        """
        Agenda coleta periódica de dados para os símbolos e timeframes especificados.
        
        Args:
            symbols: Lista de símbolos
            timeframes: Lista de timeframes
            interval_seconds: Intervalo entre coletas em segundos
            max_runtime_minutes: Tempo máximo de execução em minutos (None = infinito)
            
        Returns:
            Dicionário com status da operação
        """
        if not self.data_collector or not self.mt5_connector:
            return {
                'status': 'error',
                'message': 'Data Collector ou MT5 Connector não inicializados'
            }
            
        # Valida parâmetros
        if not symbols or not timeframes:
            return {
                'status': 'error',
                'message': 'É necessário especificar pelo menos um símbolo e um timeframe'
            }
            
        # Inicia a coleta em uma thread separada
        collection_thread = threading.Thread(
            target=self._collection_worker,
            args=(symbols, timeframes, interval_seconds, max_runtime_minutes),
            daemon=True
        )
        
        collection_thread.start()
        
        return {
            'status': 'started',
            'message': f'Coleta de dados iniciada para {len(symbols)} símbolos em {len(timeframes)} timeframes',
            'symbols': symbols,
            'timeframes': timeframes,
            'interval': interval_seconds
        }
        
    def _collection_worker(self, symbols: List[str], timeframes: List[str], 
                         interval_seconds: int, max_runtime_minutes: Optional[int]):
        """
        Worker de coleta de dados em thread separada.
        
        Args:
            symbols: Lista de símbolos
            timeframes: Lista de timeframes
            interval_seconds: Intervalo entre coletas
            max_runtime_minutes: Duração máxima da coleta
        """
        start_time = datetime.now()
        log.info(f"Iniciando coleta programada para {len(symbols)} símbolos")
        
        while True:
            try:
                # Verifica se atingimos o tempo máximo
                if max_runtime_minutes:
                    elapsed_minutes = (datetime.now() - start_time).total_seconds() / 60
                    if elapsed_minutes >= max_runtime_minutes:
                        log.info(f"Coleta encerrada após {elapsed_minutes:.1f} minutos")
                        break
                        
                # Coleta dados para cada símbolo e timeframe
                for symbol in symbols:
                    for timeframe in timeframes:
                        try:
                            self.data_collector.collect_symbol_data(symbol, timeframe)
                        except Exception as e:
                            log.error(f"Erro ao coletar {symbol}/{timeframe}: {str(e)}")
                            
                # Aguarda o próximo intervalo
                time.sleep(interval_seconds)
                
            except Exception as e:
                log.error(f"Erro no worker de coleta: {str(e)}")
                log.debug(traceback.format_exc())
                # Pausa curta antes de tentar novamente
                time.sleep(5)
                
    def export_processed_data(self, symbol: str, timeframe: str, 
                           format: str = 'csv',
                           output_dir: str = './data',
                           num_bars: int = 1000) -> Dict:
        """
        Exporta dados processados para arquivos CSV ou Excel.
        
        Args:
            symbol: Símbolo a exportar
            timeframe: Timeframe dos dados
            format: Formato de saída ('csv' ou 'excel')
            output_dir: Diretório para salvar arquivos
            num_bars: Número de barras a exportar
            
        Returns:
            Dicionário com status e caminho do arquivo
        """
        # Verifica pré-requisitos
        if not os.path.exists(output_dir):
            try:
                os.makedirs(output_dir)
            except Exception as e:
                return {
                    'status': 'error',
                    'message': f'Erro ao criar diretório: {str(e)}'
                }
                
        # Obtém e processa os dados
        df = self.analyze_symbol(symbol, timeframe, num_bars)
        
        if df.empty:
            return {
                'status': 'error',
                'message': f'Não foi possível obter dados para {symbol} no timeframe {timeframe}'
            }
            
        # Define o nome do arquivo
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{symbol.replace('$', '_')}_{timeframe}_{timestamp}"
        
        # Salva no formato especificado
        try:
            file_path = None
            
            if format.lower() == 'csv':
                file_path = os.path.join(output_dir, f"{filename}.csv")
                df.to_csv(file_path, index=False)
            elif format.lower() == 'excel':
                file_path = os.path.join(output_dir, f"{filename}.xlsx")
                df.to_excel(file_path, index=False)
            else:
                return {
                    'status': 'error',
                    'message': f'Formato não suportado: {format}'
                }
                
            return {
                'status': 'success',
                'message': f'Dados exportados com sucesso: {len(df)} registros',
                'file_path': file_path,
                'rows': len(df),
                'columns': len(df.columns)
            }
            
        except Exception as e:
            log.error(f"Erro ao exportar dados: {str(e)}")
            log.debug(traceback.format_exc())
            return {
                'status': 'error',
                'message': f'Erro ao exportar dados: {str(e)}'
            }

# Teste unitário
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    log.info("Testando IntegratedServices")
    
    # Configuração de teste
    test_config = {
        'max_workers': 2,
        'cache_size': 50,
        'optimize_memory': True,
        'use_market_context': True,
        'required_indicators': ['rsi', 'macd'],
        'mt5_init': False  # Não inicializa MT5 no teste
    }
    
    # Cria o serviço integrado
    integrated = IntegratedServices()
    
    try:
        # Verifica status
        status = integrated.get_status()
        log.info(f"Status da inicialização: {status['initialized']}")
        
        if status['initialized']:
            # Testes com dados sintéticos já que não temos MT5 no teste
            log.info("Criando dados sintéticos para teste...")
            
            # Cria dados de teste
            dates = pd.date_range(end=pd.Timestamp.now(), periods=100, freq='1min')
            data = {
                'time': dates,
                'open': np.random.normal(100, 1, 100),
                'high': None,
                'low': None,
                'close': None,
                'volume': np.random.randint(100, 10000, 100)
            }
            
            # Processamento dos dados para torná-los realistas
            df = pd.DataFrame(data)
            for i in range(len(df)):
                # Preços realistas com alguma tendência
                if i > 0:
                    # Adiciona uma pequena tendência
                    trend = 0.1 * np.sin(i / 10) + np.random.normal(0, 0.2)
                    df.loc[i, 'open'] = df.loc[i-1, 'close'] * (1 + trend * 0.01)
                else:
                    df.loc[i, 'open'] = 100.0
                
                # Gera high, low e close realistas
                high_offset = abs(np.random.normal(0, 0.5))
                low_offset = abs(np.random.normal(0, 0.5))
                close_trend = np.random.normal(0, 0.3)
                
                df.loc[i, 'high'] = df.loc[i, 'open'] * (1 + high_offset / 100)
                df.loc[i, 'low'] = df.loc[i, 'open'] * (1 - low_offset / 100)
                df.loc[i, 'close'] = df.loc[i, 'open'] * (1 + close_trend / 100)
                
                # Garante que high >= open >= low e high >= close >= low
                df.loc[i, 'high'] = max(df.loc[i, 'high'], df.loc[i, 'open'], df.loc[i, 'close'])
                df.loc[i, 'low'] = min(df.loc[i, 'low'], df.loc[i, 'open'], df.loc[i, 'close'])
            
            # Testa processamento síncrono
            log.info("Testando processamento síncrono...")
            processed_df = integrated.process_data(df, symbol='TEST', use_async=False)
            
            if not processed_df.empty:
                log.info(f"Processamento concluído: {processed_df.shape} com {len(processed_df.columns)} colunas")
                # Lista alguns indicadores calculados
                indicators = [col for col in processed_df.columns if col not in df.columns]
                log.info(f"Indicadores calculados: {len(indicators)}")
                if indicators:
                    log.info(f"Exemplos: {indicators[:5]}")
            
            # Testa processamento assíncrono
            log.info("Testando processamento assíncrono...")
            job_id = integrated.process_data(df, symbol='TEST', use_async=True)
            
            if job_id:
                log.info(f"Job agendado com ID: {job_id}")
                
                # Aguarda resultado
                result = integrated.wait_for_result(job_id, timeout=10.0)
                
                if result['status'] == 'completed':
                    async_df = result['result']
                    log.info(f"Resultado assíncrono: {async_df.shape}")
                    
        # Teste de exportação de dados
        if 'processed_df' in locals() and not processed_df.empty:
            log.info("Testando exportação de dados...")
            
            export_result = integrated.export_processed_data(
                symbol='TEST', 
                timeframe='1m',
                format='csv',
                output_dir='./test_data',
                num_bars=100
            )
            
            log.info(f"Resultado da exportação: {export_result['status']}")
            if export_result['status'] == 'success':
                log.info(f"Arquivo exportado: {export_result['file_path']}")
    
    finally:
        # Encerra os serviços
        log.info("Encerrando serviços...")
        integrated.shutdown()
        
    log.info("Teste do IntegratedServices concluído.") 