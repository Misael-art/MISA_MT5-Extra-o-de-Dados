import logging
import pandas as pd
import numpy as np
import traceback
import threading
import time
import multiprocessing as mp
import queue
from typing import Dict, List, Union, Optional, Tuple, Callable

from mt5_extracao.enhanced_indicators import EnhancedIndicatorCalculator
from mt5_extracao.market_data_analyzer import MarketDataAnalyzer

# Configuração de logging
log = logging.getLogger(__name__)
if not log.handlers:
    log.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    log.addHandler(ch)

class EnhancedCalculationService:
    """
    Serviço que gerencia e otimiza o cálculo de indicadores avançados
    e análises de mercado. Implementa cache, processamento paralelo
    e escalonamento inteligente de recursos.
    """
    
    def __init__(self, max_workers: int = None, cache_size: int = 100):
        """
        Inicializa o serviço de cálculo avançado.
        
        Args:
            max_workers: Número máximo de workers para processamento paralelo (None = auto)
            cache_size: Tamanho do cache para resultados intermediários
        """
        # Determina automaticamente número de workers baseado no sistema
        if max_workers is None:
            # Usa metade dos CPUs disponíveis, pelo menos 1
            self.max_workers = max(1, mp.cpu_count() // 2)
        else:
            self.max_workers = max_workers
            
        self.cache_size = cache_size
        self.indicator_calculator = EnhancedIndicatorCalculator()
        self.market_analyzer = MarketDataAnalyzer()
        
        # Cache de resultados
        self.cache = {}
        self.cache_order = []  # Para implementar LRU (Least Recently Used)
        
        # Fila de trabalhos e controle de threads
        self.job_queue = queue.Queue()
        self.results = {}
        self.workers = []
        self.running = False
        
        log.info(f"EnhancedCalculationService inicializado com {self.max_workers} workers")
        
    def start(self):
        """Inicia o pool de workers para processamento assíncrono."""
        if self.running:
            log.warning("Serviço de cálculo já está em execução.")
            return
            
        self.running = True
        
        # Inicia workers
        for i in range(self.max_workers):
            worker = threading.Thread(
                target=self._worker_loop,
                args=(i,),
                daemon=True
            )
            worker.start()
            self.workers.append(worker)
            
        log.info(f"Pool de {self.max_workers} workers iniciado.")
        
    def stop(self):
        """Para todos os workers e limpa as filas."""
        if not self.running:
            return
            
        self.running = False
        
        # Aguarda finalização dos workers
        for worker in self.workers:
            if worker.is_alive():
                worker.join(timeout=1.0)
                
        # Limpa as filas
        while not self.job_queue.empty():
            try:
                self.job_queue.get_nowait()
                self.job_queue.task_done()
            except queue.Empty:
                break
                
        self.workers = []
        log.info("Serviço de cálculo parado.")
        
    def _worker_loop(self, worker_id: int):
        """
        Loop principal do worker para processamento de jobs.
        
        Args:
            worker_id: ID do worker para identificação nos logs
        """
        log.debug(f"Worker {worker_id} iniciado.")
        
        while self.running:
            try:
                # Tenta pegar um job da fila
                try:
                    job = self.job_queue.get(timeout=1.0)
                except queue.Empty:
                    continue
                    
                # Processa o job
                job_id, func, args, kwargs = job
                
                try:
                    log.debug(f"Worker {worker_id} processando job {job_id}.")
                    result = func(*args, **kwargs)
                    self.results[job_id] = {
                        'status': 'completed',
                        'result': result,
                        'error': None
                    }
                    log.debug(f"Job {job_id} concluído com sucesso.")
                    
                except Exception as e:
                    log.error(f"Erro ao processar job {job_id}: {str(e)}")
                    log.debug(traceback.format_exc())
                    self.results[job_id] = {
                        'status': 'error',
                        'result': None,
                        'error': str(e)
                    }
                    
                finally:
                    # Marca o job como concluído
                    self.job_queue.task_done()
                    
            except Exception as e:
                log.error(f"Erro no worker {worker_id}: {str(e)}")
                log.debug(traceback.format_exc())
                
        log.debug(f"Worker {worker_id} finalizado.")
        
    def _add_to_cache(self, key: str, value):
        """
        Adiciona um resultado ao cache, implementando LRU.
        
        Args:
            key: Chave para o cache
            value: Valor a ser armazenado
        """
        # Se a chave já existe, atualiza a ordem (movendo para o final)
        if key in self.cache:
            self.cache_order.remove(key)
        elif len(self.cache) >= self.cache_size:
            # Se o cache está cheio, remove o item menos recentemente usado
            oldest_key = self.cache_order.pop(0)
            del self.cache[oldest_key]
            
        # Adiciona/atualiza o valor e a ordem
        self.cache[key] = value
        self.cache_order.append(key)
        
    def _get_from_cache(self, key: str) -> Optional[pd.DataFrame]:
        """
        Recupera um resultado do cache.
        
        Args:
            key: Chave do cache
            
        Returns:
            Valor do cache ou None se não existir
        """
        if key in self.cache:
            # Atualiza a ordem (movendo para o final)
            self.cache_order.remove(key)
            self.cache_order.append(key)
            return self.cache[key]
        return None
        
    def _generate_cache_key(self, df: pd.DataFrame, 
                          operation: str, 
                          parameters: Dict = None) -> str:
        """
        Gera uma chave única para o cache.
        
        Args:
            df: DataFrame fonte
            operation: Nome da operação
            parameters: Parâmetros adicionais
            
        Returns:
            String que representa a chave do cache
        """
        if df is None or df.empty:
            return None
            
        # Usa informação temporal para a chave
        if 'time' in df.columns:
            # Identifica o intervalo de tempo dos dados
            time_range = f"{df['time'].min().strftime('%Y%m%d%H%M')}-{df['time'].max().strftime('%Y%m%d%H%M')}"
            # Quantidade de registros
            count = len(df)
            # Representação de parâmetros
            params_str = ""
            if parameters:
                params_str = "_".join([f"{k}={v}" for k, v in sorted(parameters.items()) 
                                     if not callable(v) and not isinstance(v, pd.DataFrame)])
            return f"{operation}_{time_range}_{count}_{params_str}"
        return None
        
    def calculate_indicators_async(self, df: pd.DataFrame, 
                                 include_market_context: bool = True,
                                 include_advanced_stats: bool = True,
                                 include_candle_patterns: bool = True,
                                 include_volume_analysis: bool = True,
                                 include_trend_analysis: bool = True,
                                 include_support_resistance: bool = True,
                                 period: int = 20,
                                 symbol: str = None) -> str:
        """
        Agenda o cálculo assíncrono de indicadores avançados.
        
        Args:
            df: DataFrame com dados OHLCV
            include_*: Flags para incluir diferentes tipos de indicadores
            period: Período para os cálculos
            symbol: Símbolo do ativo para análises específicas
            
        Returns:
            ID do job para verificação posterior
        """
        # Verifica pré-requisitos
        if df is None or df.empty:
            log.warning("DataFrame vazio recebido para cálculo assíncrono de indicadores.")
            return None
            
        # Tenta usar o cache primeiro
        parameters = {
            'include_market_context': include_market_context,
            'include_advanced_stats': include_advanced_stats,
            'include_candle_patterns': include_candle_patterns,
            'include_volume_analysis': include_volume_analysis,
            'include_trend_analysis': include_trend_analysis,
            'include_support_resistance': include_support_resistance,
            'period': period,
            'symbol': symbol
        }
        cache_key = self._generate_cache_key(df, 'calculate_all', parameters)
        
        if cache_key:
            cached_result = self._get_from_cache(cache_key)
            if cached_result is not None:
                log.debug(f"Usando resultado em cache para {cache_key}")
                # Cria um ID de job "falso" para resultados em cache
                job_id = f"cache_{cache_key}"
                self.results[job_id] = {
                    'status': 'completed',
                    'result': cached_result,
                    'error': None,
                    'from_cache': True
                }
                return job_id
        
        # Registra o job
        job_id = f"job_{time.time()}_{np.random.randint(10000)}"
        self.results[job_id] = {'status': 'pending', 'result': None, 'error': None}
        
        # Função para executar o cálculo completo
        def calculate_full():
            result_df = df.copy()
            
            # 1. Cálculo de indicadores técnicos
            result_df = self.indicator_calculator.calculate_all_indicators(
                result_df,
                include_market_context=False,  # Faremos isso separadamente
                include_advanced_stats=include_advanced_stats,
                include_candle_patterns=include_candle_patterns,
                include_volume_analysis=include_volume_analysis,
                include_trend_analysis=include_trend_analysis,
                include_support_resistance=include_support_resistance,
                period=period
            )
            
            # 2. Análise de mercado (opcional)
            if include_market_context and symbol:
                result_df = self.market_analyzer.analyze_market_data(result_df, symbol)
                
            # Guarda no cache
            if cache_key:
                self._add_to_cache(cache_key, result_df)
                
            return result_df
            
        # Adiciona o job à fila
        self.job_queue.put((job_id, calculate_full, [], {}))
        
        return job_id
        
    def get_job_result(self, job_id: str) -> Dict:
        """
        Verifica o status e recupera o resultado de um job.
        
        Args:
            job_id: ID do job retornado por calculate_indicators_async
            
        Returns:
            Dicionário com status, resultado e erro (se houver)
        """
        if job_id not in self.results:
            return {'status': 'not_found', 'result': None, 'error': 'Job não encontrado'}
            
        return self.results[job_id].copy()
        
    def wait_for_job(self, job_id: str, timeout: Optional[float] = None) -> Dict:
        """
        Aguarda a conclusão de um job e retorna seu resultado.
        
        Args:
            job_id: ID do job
            timeout: Tempo máximo de espera em segundos
            
        Returns:
            Dicionário com status, resultado e erro (se houver)
        """
        if job_id not in self.results:
            return {'status': 'not_found', 'result': None, 'error': 'Job não encontrado'}
            
        start_time = time.time()
        
        while self.results[job_id]['status'] == 'pending':
            # Verifica timeout
            if timeout is not None and (time.time() - start_time) > timeout:
                return {'status': 'timeout', 'result': None, 'error': 'Timeout ao aguardar resultado'}
                
            # Pequena pausa para não sobrecarregar a CPU
            time.sleep(0.1)
            
        return self.results[job_id].copy()
        
    def calculate_indicators_sync(self, df: pd.DataFrame, 
                                 include_market_context: bool = True,
                                 include_advanced_stats: bool = True,
                                 include_candle_patterns: bool = True,
                                 include_volume_analysis: bool = True,
                                 include_trend_analysis: bool = True,
                                 include_support_resistance: bool = True,
                                 period: int = 20,
                                 symbol: str = None,
                                 timeout: Optional[float] = None) -> pd.DataFrame:
        """
        Calcula indicadores de forma síncrona (aguarda o resultado).
        
        Args:
            df: DataFrame com dados OHLCV
            include_*: Flags para incluir diferentes tipos de indicadores
            period: Período para os cálculos
            symbol: Símbolo do ativo para análises específicas
            timeout: Tempo máximo de espera em segundos
            
        Returns:
            DataFrame com os indicadores calculados
        """
        job_id = self.calculate_indicators_async(
            df,
            include_market_context=include_market_context,
            include_advanced_stats=include_advanced_stats,
            include_candle_patterns=include_candle_patterns,
            include_volume_analysis=include_volume_analysis,
            include_trend_analysis=include_trend_analysis,
            include_support_resistance=include_support_resistance,
            period=period,
            symbol=symbol
        )
        
        if not job_id:
            log.warning("Falha ao criar job para cálculo síncrono")
            return df
            
        result = self.wait_for_job(job_id, timeout)
        
        if result['status'] == 'completed':
            return result['result']
        else:
            log.error(f"Erro ao calcular indicadores: {result['error']}")
            return df
            
    def process_data_batch(self, data_frames: List[pd.DataFrame], 
                          symbol: str = None,
                          parameters: Dict = None) -> List[pd.DataFrame]:
        """
        Processa um lote de DataFrames em paralelo.
        
        Args:
            data_frames: Lista de DataFrames para processamento
            symbol: Símbolo associado aos dados
            parameters: Parâmetros para o cálculo de indicadores
            
        Returns:
            Lista com os DataFrames processados
        """
        if not data_frames:
            return []
            
        # Define parâmetros padrão se não especificados
        if parameters is None:
            parameters = {
                'include_market_context': True,
                'include_advanced_stats': True,
                'include_candle_patterns': True,
                'include_volume_analysis': True,
                'include_trend_analysis': True,
                'include_support_resistance': True,
                'period': 20
            }
            
        # Agenda jobs para cada DataFrame
        job_ids = []
        for df in data_frames:
            job_id = self.calculate_indicators_async(
                df,
                symbol=symbol,
                **parameters
            )
            if job_id:
                job_ids.append(job_id)
                
        # Aguarda resultados
        results = []
        for job_id in job_ids:
            result = self.wait_for_job(job_id)
            if result['status'] == 'completed':
                results.append(result['result'])
            else:
                log.error(f"Erro ao processar lote: {result['error']}")
                # Adiciona o DataFrame original em caso de erro
                idx = job_ids.index(job_id)
                if idx < len(data_frames):
                    results.append(data_frames[idx])
                    
        return results
        
    def process_large_dataset(self, df: pd.DataFrame, 
                             batch_size: int = 1000,
                             symbol: str = None,
                             parameters: Dict = None) -> pd.DataFrame:
        """
        Processa um DataFrame grande dividindo-o em lotes menores.
        
        Args:
            df: DataFrame a ser processado
            batch_size: Tamanho dos lotes para processamento
            symbol: Símbolo associado aos dados
            parameters: Parâmetros para o cálculo
            
        Returns:
            DataFrame completo processado
        """
        if df is None or df.empty:
            return df
            
        # Se o DataFrame for menor que o tamanho do lote, processa diretamente
        if len(df) <= batch_size:
            return self.calculate_indicators_sync(df, symbol=symbol, **(parameters or {}))
            
        # Divide em lotes
        batches = []
        for i in range(0, len(df), batch_size):
            batches.append(df.iloc[i:i+batch_size].copy())
            
        log.info(f"Processando dataset grande ({len(df)} linhas) em {len(batches)} lotes")
        
        # Processa os lotes
        processed_batches = self.process_data_batch(batches, symbol, parameters)
        
        # Combina os resultados
        result = pd.concat(processed_batches, ignore_index=True)
        
        return result
        
    def cleanup_jobs(self, max_age_hours: float = 24.0):
        """
        Limpa jobs antigos para liberar memória.
        
        Args:
            max_age_hours: Idade máxima (em horas) para manter jobs
        """
        current_time = time.time()
        jobs_to_remove = []
        
        for job_id, job_info in self.results.items():
            # Ignora jobs em cache
            if job_id.startswith('cache_'):
                continue
                
            # Extrai o timestamp do job_id
            try:
                job_time = float(job_id.split('_')[1])
                age_hours = (current_time - job_time) / 3600
                
                if age_hours > max_age_hours:
                    jobs_to_remove.append(job_id)
            except (IndexError, ValueError):
                # Formato de job_id inválido, ignora
                pass
                
        # Remove os jobs antigos
        for job_id in jobs_to_remove:
            del self.results[job_id]
            
        log.info(f"Cleanup: removidos {len(jobs_to_remove)} jobs antigos")
        
    def cleanup_cache(self, max_items: int = None):
        """
        Limpa o cache para liberar memória.
        
        Args:
            max_items: Número máximo de itens para manter (None = mantém o padrão)
        """
        if max_items is None:
            max_items = self.cache_size // 2
            
        while len(self.cache) > max_items:
            oldest_key = self.cache_order.pop(0)
            del self.cache[oldest_key]
            
        log.info(f"Cleanup: cache reduzido para {len(self.cache)} itens")

# Teste unitário
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    log.info("Testando EnhancedCalculationService")
    
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
    
    # Inicializa o serviço
    service = EnhancedCalculationService(max_workers=2)
    service.start()
    
    try:
        # Teste assíncrono
        log.info("Agendando cálculo assíncrono...")
        job_id = service.calculate_indicators_async(df, symbol='WIN$N')
        
        log.info(f"Job agendado com ID: {job_id}")
        log.info("Aguardando resultado...")
        
        result = service.wait_for_job(job_id)
        log.info(f"Status do job: {result['status']}")
        
        if result['status'] == 'completed':
            df_result = result['result']
            log.info(f"Resultado obtido: {df_result.shape} com {len(df_result.columns)} colunas")
            
        # Teste síncrono
        log.info("Executando cálculo síncrono...")
        df_sync = service.calculate_indicators_sync(df, symbol='WIN$N')
        log.info(f"Resultado síncrono: {df_sync.shape} com {len(df_sync.columns)} colunas")
        
        # Teste de cache
        log.info("Testando cache...")
        start_time = time.time()
        df_cached = service.calculate_indicators_sync(df, symbol='WIN$N')
        elapsed = time.time() - start_time
        log.info(f"Tempo com cache: {elapsed:.4f} segundos")
        
        # Teste batch
        log.info("Testando processamento em lotes...")
        batches = [df.iloc[:30].copy(), df.iloc[30:60].copy(), df.iloc[60:].copy()]
        batch_results = service.process_data_batch(batches, symbol='WIN$N')
        log.info(f"Resultados em lote: {len(batch_results)} lotes processados")
        
        # Teste dataset grande
        log.info("Testando dataset grande...")
        big_df = pd.concat([df] * 10, ignore_index=True)  # 1000 linhas
        big_result = service.process_large_dataset(big_df, batch_size=250, symbol='WIN$N')
        log.info(f"Resultado grande: {big_result.shape} linhas processadas")
        
    finally:
        # Para o serviço ao finalizar
        service.stop()
        
    log.info("Teste do EnhancedCalculationService concluído.") 