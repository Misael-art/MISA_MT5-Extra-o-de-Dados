import logging
import time
import traceback
from threading import Thread, Lock
import pandas as pd

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
        """Obtém dados M1 recentes para um símbolo e salva no banco."""
        log.debug(f"Processando símbolo: {symbol}")
        try:
            # Inicializa status se não existir
            if symbol not in self.collection_status:
                self.collection_status[symbol] = {
                    'total': 0, 
                    'success': 0, 
                    'errors': 0,
                    'last_time': None,
                    'last_error': None,
                    'data_size': 0
                }
                
            # Verifica disponibilidade do símbolo (usando conector)
            symbol_info = self.connector.get_symbol_info(symbol)
            if symbol_info is None:
                self._log_ui(f"Aviso: Símbolo {symbol} inválido ou não disponível.")
                self.collection_status[symbol]['errors'] += 1
                self.collection_status[symbol]['last_error'] = "Símbolo inválido ou indisponível"
                return

            # Obtém o último candle M1 (ou últimos N candles)
            # Usaremos copy_rates_from_pos para pegar o último candle fechado
            # timeframe = mt5.TIMEFRAME_M1 # Obter do conector?
            timeframe_val = 1 # M1
            count = 2 # Pega os 2 últimos para garantir que temos o último fechado
            start_pos = 0

            df = self.connector.get_rates(symbol, timeframe_val, start_pos, count)

            if df is None or df.empty:
                log.warning(f"Nenhum dado M1 recente retornado para {symbol} via connector.")
                self.collection_status[symbol]['errors'] += 1
                self.collection_status[symbol]['last_error'] = "Sem dados retornados do MT5"
                return

            # Pega apenas o último candle completo (índice 1 se count=2)
            if len(df) < count:
                 log.warning(f"Menos de {count} candles retornados para {symbol}. Aguardando próximo ciclo.")
                 self.collection_status[symbol]['errors'] += 1
                 self.collection_status[symbol]['last_error'] = f"Dados insuficientes ({len(df)}/{count})"
                 return
            last_data = df.iloc[-1:].copy() # Pega a última linha como DataFrame

            if last_data.empty:
                 log.warning(f"DataFrame da última barra vazio para {symbol}.")
                 self.collection_status[symbol]['errors'] += 1
                 self.collection_status[symbol]['last_error'] = "DataFrame vazio"
                 return

            log.debug(f"Dados obtidos para {symbol}: {last_data['time'].iloc[0]}")
            
            # Atualizar estatísticas antes de processamento
            self.collection_status[symbol]['total'] += 1
            self.collection_status[symbol]['last_time'] = last_data['time'].iloc[0]

            # Calcular indicadores e variações (usando indicator_calculator)
            try:
                last_data = self.indicator_calculator.calculate_technical_indicators(last_data)
                last_data = self.indicator_calculator.calculate_price_variations(last_data) # Variações podem precisar de mais dados
            except Exception as calc_err:
                log.error(f"Erro ao calcular indicadores/variações para {symbol}: {calc_err}")
                self.collection_status[symbol]['last_error'] = f"Erro de cálculo: {str(calc_err)[:50]}..."
                # Continua mesmo assim? Ou retorna? Por enquanto, continua.

            # Adiciona outras informações úteis se disponíveis
            try:
                last_data['spread'] = symbol_info.spread
                # last_data['last'] = symbol_info.last # 'last' pode não estar no candle fechado
                # last_data['trading_hours'] = symbol_info.trade_mode
            except Exception as info_err:
                log.warning(f"Erro ao obter info adicional (spread, etc.) para {symbol}: {info_err}")

            # Estimar tamanho dos dados coletados (aproximado)
            try:
                data_size = last_data.memory_usage(index=True, deep=True).sum()
                self.collection_status[symbol]['data_size'] += data_size
            except:
                pass  # Ignora erros na estimativa de tamanho

            # Salvar no banco de dados (usando db_manager)
            try:
                timeframe_name = "1 minuto" # Assumindo M1
                success = self.db_manager.save_ohlcv_data(symbol, timeframe_name, last_data)
                if success:
                    log.debug(f"Dados de {symbol} ({timeframe_name}) salvos para {last_data['time'].iloc[0]}")
                    self.collection_status[symbol]['success'] += 1
                    self.collection_status[symbol]['last_error'] = None
                else:
                    self._log_ui(f"Falha ao salvar dados de {symbol} ({timeframe_name})")
                    self.collection_status[symbol]['last_error'] = "Falha ao salvar no banco"
            except Exception as db_err:
                log.error(f"Erro inesperado ao salvar dados para {symbol}: {db_err}")
                self._log_ui(f"ERRO ao salvar dados de {symbol}")
                self.collection_status[symbol]['last_error'] = f"Erro de banco: {str(db_err)[:50]}..."

        except Exception as e:
            log.error(f"Erro ao processar {symbol} em fetch_and_save_data: {e}")
            log.debug(traceback.format_exc())
            self._log_ui(f"ERRO ao processar {symbol}")
            
            # Atualizar estatísticas
            if symbol in self.collection_status:
                self.collection_status[symbol]['errors'] += 1
                self.collection_status[symbol]['last_error'] = str(e)[:100]

# Exemplo de uso (requer instâncias de Connector, DBManager, etc.)
# if __name__ == "__main__":
#     pass