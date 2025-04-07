import logging
import time
import traceback
from threading import Thread, Lock
import pandas as pd

from mt5_extracao.enhanced_indicators import EnhancedIndicatorCalculator

# Configuração de logging
log = logging.getLogger(__name__)
if not log.handlers:
    log.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    log.addHandler(ch)

class DataCollector:
    """
    Responsável pela coleta contínua de dados em tempo real (M1).
    """
    def __init__(self, connector, db_manager, indicator_calculator, ui_manager=None):
        """
        Inicializa o coletor de dados.

        Args:
            connector: Instância de MT5Connector.
            db_manager: Instância de DatabaseManager.
            indicator_calculator: Instância de IndicatorCalculator.
            ui_manager: Instância opcional de UIManager (para logging na UI).
        """
        self.connector = connector
        self.db_manager = db_manager
        self.indicator_calculator = indicator_calculator
        self.ui_manager = ui_manager # Para logar mensagens na UI

        self.running = False
        self.collection_thread = None
        self.symbols_to_collect = []
        self.lock = Lock() # Para acesso thread-safe à lista de símbolos
        
        # Status de coleta para cada símbolo
        self.collection_status = {}  # {symbol: {'total': n, 'success': n, 'last_time': datetime, 'errors': n}}
        self.collection_start_time = None
        
        # Controle de reconexão automática
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 3
        self.reconnect_interval = 60  # segundos entre tentativas de reconexão
        
        # Controle de batch para extração em lotes
        self.batch_size = 1000  # número máximo de registros por lote
        self.parallel_symbols = 1  # número de símbolos processados simultaneamente (a ser implementado)

        # Inicializa o calculador avançado de indicadores
        self.enhanced_calculator = EnhancedIndicatorCalculator()

        log.info("DataCollector inicializado.")

    def set_symbols(self, symbols_list):
        """Define a lista de símbolos para coletar."""
        with self.lock:
            self.symbols_to_collect = list(symbols_list)
            
            # Inicializa o status de coleta para cada símbolo
            for symbol in self.symbols_to_collect:
                if symbol not in self.collection_status:
                    self.collection_status[symbol] = {
                        'total': 0,
                        'success': 0,
                        'errors': 0,
                        'last_time': None,
                        'last_error': None,
                        'data_size': 0  # em bytes
                    }
            
            log.info(f"Símbolos para coleta definidos: {self.symbols_to_collect}")

    def start(self):
        """Inicia o loop de coleta em uma thread separada."""
        if self.running:
            log.warning("Coleta de dados já está em andamento.")
            return
        if not self.connector or not self.connector.is_initialized:
             log.error("Não é possível iniciar a coleta: MT5Connector não inicializado.")
             if self.ui_manager: self.ui_manager.log("ERRO: Conexão MT5 não iniciada.")
             return
        if not self.db_manager or not self.db_manager.is_connected():
             log.error("Não é possível iniciar a coleta: DatabaseManager não conectado.")
             if self.ui_manager: self.ui_manager.log("ERRO: Banco de dados não conectado.")
             return
        if not self.symbols_to_collect:
             log.warning("Não é possível iniciar a coleta: Nenhum símbolo definido.")
             if self.ui_manager: self.ui_manager.log("AVISO: Nenhum símbolo selecionado para coleta.")
             return

        log.info("Iniciando loop de coleta de dados...")
        self.running = True
        self.collection_start_time = time.time()
        self.reconnect_attempts = 0
        
        # Resetar estatísticas de coleta
        with self.lock:
            for symbol in self.symbols_to_collect:
                self.collection_status[symbol] = {
                    'total': 0,
                    'success': 0,
                    'errors': 0,
                    'last_time': None,
                    'last_error': None,
                    'data_size': 0
                }
        
        self.collection_thread = Thread(target=self._collection_loop)
        self.collection_thread.daemon = True
        self.collection_thread.start()
        if self.ui_manager: 
            self.ui_manager.log("Coleta de dados iniciada.")
            # Atualiza a interface com status inicial
            self.update_collection_status()

    def stop(self):
        """Sinaliza para parar o loop de coleta."""
        log.info("Parando loop de coleta de dados...")
        self.running = False
        # Espera um pouco pela thread terminar? Opcional.
        # if self.collection_thread and self.collection_thread.is_alive():
        #     self.collection_thread.join(timeout=2)
        if self.ui_manager: 
            self.ui_manager.log("Coleta de dados interrompida.")
            # Atualizar a interface com status final
            self.update_collection_status(final=True)

    def _log_ui(self, message):
        """Envia mensagem para o log da UI, se disponível."""
        if self.ui_manager:
            self.ui_manager.log(message)
        else:
            # Fallback para log normal se UI não estiver disponível
            log.info(f"(UI LOG): {message}")

    def update_collection_status(self, final=False):
        """Atualiza o status da coleta na interface de usuário."""
        if not self.ui_manager or not hasattr(self.ui_manager, 'update_collection_progress'):
            return
            
        try:
            # Calcular estatísticas gerais
            total_collected = sum(s['total'] for s in self.collection_status.values())
            total_success = sum(s['success'] for s in self.collection_status.values())
            total_errors = sum(s['errors'] for s in self.collection_status.values())
            
            # Tempo decorrido
            elapsed_time = time.time() - self.collection_start_time if self.collection_start_time else 0
            
            # Prepara dados de status detalhados por símbolo
            symbols_status = {symbol: status for symbol, status in self.collection_status.items()}
            
            # Enviar atualização para a UI
            self.ui_manager.update_collection_progress(
                total_collected=total_collected,
                total_success=total_success,
                total_errors=total_errors,
                elapsed_time=elapsed_time,
                symbols_status=symbols_status,
                is_running=self.running and not final
            )
        except Exception as e:
            log.error(f"Erro ao atualizar status de coleta na UI: {e}")

    def _collection_loop(self):
        """Loop principal de coleta de dados (executado na thread)."""
        log.info("Thread de coleta iniciada.")
        last_status_update = time.time()
        status_update_interval = 2  # segundos entre atualizações de status
        
        while self.running:
            try:
                with self.lock:
                    current_symbols = list(self.symbols_to_collect) # Copia para evitar problemas de concorrência

                if not current_symbols:
                     log.debug("Nenhum símbolo para coletar, aguardando...")
                     time.sleep(5) # Espera antes de verificar novamente
                     continue

                log.debug(f"Iniciando ciclo de coleta para: {current_symbols}")
                for symbol in current_symbols:
                    if not self.running: break # Verifica cancelamento dentro do loop
                    self._fetch_and_save_data(symbol)
                    
                    # Atualiza status na UI periodicamente (não a cada símbolo para não sobrecarregar a UI)
                    if time.time() - last_status_update > status_update_interval:
                        self.update_collection_status()
                        last_status_update = time.time()

                if not self.running: break # Verifica cancelamento após o loop

                # Intervalo entre ciclos de coleta (ex: 60 segundos para dados M1)
                # Ajustar conforme necessário
                wait_time = 60
                log.debug(f"Ciclo de coleta concluído. Aguardando {wait_time} segundos...")
                
                # Atualiza status antes de aguardar
                self.update_collection_status()
                last_status_update = time.time()
                
                for _ in range(wait_time):
                    if not self.running: break
                    time.sleep(1)

            except Exception as e:
                error_msg = f"Erro crítico no loop de coleta: {e}"
                log.error(error_msg)
                log.debug(traceback.format_exc())
                self._log_ui(f"ERRO: {error_msg}")
                
                # Tentativa de reconexão automática
                if "connection" in str(e).lower() or "not initialized" in str(e).lower():
                    if self.reconnect_attempts < self.max_reconnect_attempts:
                        self.reconnect_attempts += 1
                        retry_time = self.reconnect_interval * self.reconnect_attempts
                        log.info(f"Tentando reconexão automática ({self.reconnect_attempts}/{self.max_reconnect_attempts}) em {retry_time} segundos...")
                        self._log_ui(f"Tentando reconexão automática em {retry_time} segundos...")
                        
                        # Aguarda tempo para reconexão
                        time.sleep(retry_time)
                        
                        # Tenta reconectar ao MT5
                        if self.connector and hasattr(self.connector, 'initialize'):
                            if self.connector.initialize():
                                log.info("Reconexão automática bem-sucedida!")
                                self._log_ui("Conexão MT5 restabelecida!")
                                continue  # Volta ao início do loop
                            else:
                                log.warning("Falha na tentativa de reconexão automática.")
                                self._log_ui("Falha na tentativa de reconexão. Aguardando próximo ciclo...")
                    else:
                        log.error(f"Máximo de tentativas de reconexão ({self.max_reconnect_attempts}) atingido.")
                        self._log_ui("Máximo de tentativas de reconexão atingido. Coleta continuará quando a conexão for restaurada.")
                
                # Espera um pouco antes de tentar novamente para evitar spam de erros
                time.sleep(30)
                
        log.info("Thread de coleta finalizada.")

    def _fetch_and_save_data(self, symbol):
        """
        Recupera dados para o símbolo especificado e salva no banco de dados.
        
        Args:
            symbol: Símbolo para o qual buscar dados
            
        Returns:
            bool: True se os dados foram salvos com sucesso, False caso contrário
        """
        try:
            if not self.connector or not self.connector.is_initialized:
                 log.error(f"MT5 não inicializado ao tentar coletar dados para {symbol}")
                 return False

            # Obtém últimas barras do símbolo (retorna as duas últimas)
            count = 2  # Pega duas barras para garantir que temos o último candle completo
            df = self.connector.get_last_bars(symbol, count=count, timeframe='1min')

            if df is None or df.empty:
                log.warning(f"Nenhum dado M1 recente retornado para {symbol} via connector.")
                self.collection_status[symbol]['errors'] += 1
                self.collection_status[symbol]['last_error'] = "Sem dados retornados do MT5"
                return False

            # Pega apenas o último candle completo (índice 1 se count=2)
            if len(df) < count:
                 log.warning(f"Menos de {count} candles retornados para {symbol}. Aguardando próximo ciclo.")
                 self.collection_status[symbol]['errors'] += 1
                 self.collection_status[symbol]['last_error'] = f"Dados insuficientes ({len(df)}/{count})"
                 return False
            last_data = df.iloc[-1:].copy() # Pega a última linha como DataFrame

            if last_data.empty:
                 log.warning(f"DataFrame da última barra vazio para {symbol}.")
                 self.collection_status[symbol]['errors'] += 1
                 self.collection_status[symbol]['last_error'] = "DataFrame vazio"
                 return False

            log.debug(f"Dados obtidos para {symbol}: {last_data['time'].iloc[0]}")
            
            # Atualizar estatísticas antes de processamento
            self.collection_status[symbol]['total'] += 1
            self.collection_status[symbol]['last_time'] = last_data['time'].iloc[0]

            # Calcular indicadores avançados usando o EnhancedIndicatorCalculator
            try:
                # Tenta obter dados históricos para cálculos mais precisos
                historical_data = self._get_historical_context(symbol)
                
                if historical_data is not None and not historical_data.empty:
                    # Concatena o candle atual com os dados históricos
                    combined_data = pd.concat([historical_data, last_data], ignore_index=True)
                    
                    # Calcula todos os indicadores no conjunto combinado
                    enriched_data = self.enhanced_calculator.calculate_all_indicators(
                        combined_data,
                        include_market_context=True,
                        include_advanced_stats=True,
                        include_candle_patterns=True,
                        include_volume_analysis=True,
                        include_trend_analysis=True,
                        include_support_resistance=True
                    )
                    
                    # Extrai apenas o último candle com todos os indicadores calculados
                    last_data_with_indicators = enriched_data.iloc[-1:].copy()
                else:
                    # Fallback para cálculos básicos caso não haja contexto histórico
                    last_data_with_indicators = self.enhanced_calculator.calculate_technical_indicators(last_data)
                    last_data_with_indicators = self.enhanced_calculator.calculate_price_variations(last_data_with_indicators)
            except Exception as calc_err:
                log.error(f"Erro ao calcular indicadores avançados para {symbol}: {calc_err}")
                log.debug(traceback.format_exc())
                self.collection_status[symbol]['last_error'] = f"Erro no cálculo de indicadores: {str(calc_err)[:50]}..."
                
                # Tenta usar o calculador básico como fallback
                try:
                    last_data_with_indicators = self.indicator_calculator.calculate_technical_indicators(last_data)
                    last_data_with_indicators = self.indicator_calculator.calculate_price_variations(last_data_with_indicators)
                except Exception as basic_err:
                    log.error(f"Erro no fallback para indicadores básicos: {basic_err}")
                    last_data_with_indicators = last_data  # Usa dados sem indicadores

            # Salvar no banco de dados
            try:
                table_name = self.db_manager.get_table_name_for_symbol(symbol, '1_minuto')
                saved = self.db_manager.save_data(last_data_with_indicators, table_name, symbol)
                
                if saved:
                    log.debug(f"Dados salvos com sucesso para {symbol} na tabela '{table_name}'.")
                    self.collection_status[symbol]['success'] += 1
                    return True
                else:
                    log.warning(f"Falha ao salvar dados para {symbol}.")
                    self.collection_status[symbol]['errors'] += 1
                    self.collection_status[symbol]['last_error'] = "Erro ao salvar no banco"
                    return False
                    
            except Exception as e:
                log.error(f"Erro ao salvar dados para {symbol}: {e}")
                log.debug(traceback.format_exc())
                self.collection_status[symbol]['errors'] += 1
                self.collection_status[symbol]['last_error'] = f"Erro no banco: {str(e)[:50]}..."
                return False
                
        except Exception as e:
            log.error(f"Erro não tratado ao coletar dados para {symbol}: {e}")
            log.debug(traceback.format_exc())
            self.collection_status[symbol]['errors'] += 1
            self.collection_status[symbol]['last_error'] = f"Erro geral: {str(e)[:50]}..."
            return False

    def _get_historical_context(self, symbol, bars=100):
        """
        Obtém dados históricos para fornecer contexto para os cálculos de indicadores.
        
        Args:
            symbol: Símbolo para o qual buscar dados históricos
            bars: Número de barras para contexto histórico
            
        Returns:
            DataFrame com dados históricos ou None se não for possível obter
        """
        try:
            # Tenta obter do banco de dados primeiro (mais eficiente)
            table_name = self.db_manager.get_table_name_for_symbol(symbol, '1_minuto')
            recent_data = self.db_manager.get_recent_data(table_name, limit=bars)
            
            # Se tiver dados suficientes no banco, usa eles
            if recent_data is not None and len(recent_data) >= 30:  # Mínimo de 30 barras para contexto
                return recent_data
                
            # Se não houver dados suficientes no banco, busca do MT5
            historical_data = self.connector.get_historical_data(
                symbol,
                timeframe='1min',
                bars=bars,
                start_dt=None,  # Sem data específica
                end_dt=None     # Até o presente
            )
            
            return historical_data
            
        except Exception as e:
            log.warning(f"Não foi possível obter contexto histórico para {symbol}: {e}")
            return None

# Exemplo de uso (requer instâncias de Connector, DBManager, etc.)
# if __name__ == "__main__":
#     pass