import logging
import pandas as pd
import numpy as np
import psutil
import os
import time
import traceback
from typing import Dict, List, Callable, Any, Optional, Union, Tuple
from functools import lru_cache, wraps
from datetime import datetime

# Configuração de logging
log = logging.getLogger(__name__)
if not log.handlers:
    log.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    log.addHandler(ch)

class PerformanceOptimizer:
    """
    Classe responsável por otimizar a performance do sistema através
    de monitoramento de recursos, cache inteligente e estratégias
    adaptativas de processamento.
    """
    
    def __init__(self, target_cpu_usage: float = 80.0, 
                memory_threshold: float = 75.0,
                monitoring_interval: float = 5.0):
        """
        Inicializa o otimizador de performance.
        
        Args:
            target_cpu_usage: Porcentagem alvo de uso de CPU (default: 80%)
            memory_threshold: Porcentagem de memória após a qual começamos a liberar recursos (default: 75%)
            monitoring_interval: Intervalo em segundos para monitoramento (default: 5s)
        """
        self.target_cpu_usage = target_cpu_usage
        self.memory_threshold = memory_threshold
        self.monitoring_interval = monitoring_interval
        
        # Métricas e informações de performance
        self.metrics = {
            'execution_times': {},    # Tempos de execução de funções
            'memory_usage': [],       # Histórico de uso de memória
            'cpu_usage': []           # Histórico de uso de CPU
        }
        
        # Status do sistema
        self.system_status = {
            'load_level': 'normal',   # 'low', 'normal', 'high', 'critical'
            'last_check': datetime.now(),
            'available_resources': {}
        }
        
        # Inicializa o monitoramento
        self._update_system_status()
        
        log.info(f"PerformanceOptimizer iniciado com alvo de CPU: {target_cpu_usage}%, "
                f"limite de memória: {memory_threshold}%")
    
    def _update_system_status(self):
        """Atualiza o status do sistema e recursos disponíveis."""
        # Coleta métricas
        cpu_percent = psutil.cpu_percent(interval=0.1)
        memory = psutil.virtual_memory()
        memory_percent = memory.percent
        
        # Atualiza histórico
        self.metrics['cpu_usage'].append((datetime.now(), cpu_percent))
        self.metrics['memory_usage'].append((datetime.now(), memory_percent))
        
        # Limita o tamanho do histórico (últimas 100 medições)
        if len(self.metrics['cpu_usage']) > 100:
            self.metrics['cpu_usage'].pop(0)
        if len(self.metrics['memory_usage']) > 100:
            self.metrics['memory_usage'].pop(0)
        
        # Determina o nível de carga
        if memory_percent > 90 or cpu_percent > 95:
            load_level = 'critical'
        elif memory_percent > self.memory_threshold or cpu_percent > self.target_cpu_usage:
            load_level = 'high'
        elif memory_percent < self.memory_threshold/2 and cpu_percent < self.target_cpu_usage/2:
            load_level = 'low'
        else:
            load_level = 'normal'
            
        # Atualiza status
        self.system_status['load_level'] = load_level
        self.system_status['last_check'] = datetime.now()
        self.system_status['available_resources'] = {
            'cpu_available': max(0, 100 - cpu_percent),
            'memory_available': max(0, 100 - memory_percent),
            'memory_free_gb': memory.available / (1024 * 1024 * 1024)
        }
        
        log.debug(f"Status do sistema: {load_level}, CPU: {cpu_percent}%, Memória: {memory_percent}%")
    
    def should_optimize(self) -> bool:
        """Verifica se devemos otimizar com base no estado atual do sistema."""
        # Atualiza status se necessário
        now = datetime.now()
        if (now - self.system_status['last_check']).total_seconds() > self.monitoring_interval:
            self._update_system_status()
            
        # Determina se é necessário otimizar
        return self.system_status['load_level'] in ['high', 'critical']
    
    def recommended_batch_size(self, default_size: int = 1000) -> int:
        """
        Recomenda um tamanho de lote adequado com base no uso de recursos.
        
        Args:
            default_size: Tamanho padrão do lote
            
        Returns:
            Tamanho de lote recomendado
        """
        self._update_system_status()
        load_level = self.system_status['load_level']
        
        # Ajusta o tamanho do lote com base na carga
        if load_level == 'critical':
            return max(50, default_size // 10)
        elif load_level == 'high':
            return max(100, default_size // 4)
        elif load_level == 'low':
            return min(10000, default_size * 2)
        else:
            return default_size
    
    def recommend_parallel_workers(self, default_workers: int = None) -> int:
        """
        Recomenda o número de workers paralelos com base na carga do sistema.
        
        Args:
            default_workers: Número padrão de workers (None = cpu_count)
            
        Returns:
            Número recomendado de workers
        """
        if default_workers is None:
            default_workers = os.cpu_count()
            
        self._update_system_status()
        load_level = self.system_status['load_level']
        
        # Ajusta o número de workers com base na carga
        if load_level == 'critical':
            return max(1, default_workers // 4)
        elif load_level == 'high':
            return max(1, default_workers // 2)
        elif load_level == 'low':
            return default_workers
        else:
            # No caso normal, usa 75% dos CPUs disponíveis
            return max(1, int(default_workers * 0.75))
    
    def record_execution_time(self, function_name: str, execution_time: float):
        """
        Registra o tempo de execução de uma função para análise.
        
        Args:
            function_name: Nome da função monitorada
            execution_time: Tempo de execução em segundos
        """
        if function_name not in self.metrics['execution_times']:
            self.metrics['execution_times'][function_name] = []
            
        self.metrics['execution_times'][function_name].append(
            (datetime.now(), execution_time)
        )
        
        # Limita o histórico para as últimas 100 execuções
        if len(self.metrics['execution_times'][function_name]) > 100:
            self.metrics['execution_times'][function_name].pop(0)
    
    def get_performance_report(self) -> Dict:
        """
        Gera um relatório de performance do sistema.
        
        Returns:
            Dicionário com métricas de performance
        """
        self._update_system_status()
        
        # Calcula estatísticas das métricas
        execution_stats = {}
        for func_name, times in self.metrics['execution_times'].items():
            if not times:
                continue
                
            # Extrai apenas os tempos de execução
            exec_times = [t[1] for t in times]
            
            execution_stats[func_name] = {
                'avg': np.mean(exec_times),
                'min': min(exec_times),
                'max': max(exec_times),
                'median': np.median(exec_times),
                'count': len(exec_times),
                'total': sum(exec_times)
            }
        
        # Obtém médias de CPU e memória
        cpu_values = [cpu[1] for cpu in self.metrics['cpu_usage']]
        memory_values = [mem[1] for mem in self.metrics['memory_usage']]
        
        return {
            'system_status': self.system_status['load_level'],
            'cpu': {
                'current': cpu_values[-1] if cpu_values else None,
                'avg': np.mean(cpu_values) if cpu_values else None,
                'max': max(cpu_values) if cpu_values else None
            },
            'memory': {
                'current': memory_values[-1] if memory_values else None,
                'avg': np.mean(memory_values) if memory_values else None,
                'max': max(memory_values) if memory_values else None,
                'free_gb': self.system_status['available_resources'].get('memory_free_gb')
            },
            'execution_stats': execution_stats
        }
    
    def optimize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Otimiza um DataFrame para reduzir uso de memória.
        
        Args:
            df: DataFrame para otimizar
            
        Returns:
            DataFrame otimizado
        """
        if df is None or df.empty:
            return df
            
        result = df.copy()
        start_mem = result.memory_usage().sum() / 1024**2
        
        # Para cada coluna, converte para o tipo de dado mais eficiente
        for col in result.columns:
            col_type = result[col].dtype
            
            # Otimiza números inteiros
            if col_type != object and pd.api.types.is_integer_dtype(col_type):
                c_min = result[col].min()
                c_max = result[col].max()
                
                # Seleciona o tipo mais eficiente
                if c_min >= 0:
                    if c_max < 256:
                        result[col] = result[col].astype(np.uint8)
                    elif c_max < 65536:
                        result[col] = result[col].astype(np.uint16)
                    elif c_max < 4294967296:
                        result[col] = result[col].astype(np.uint32)
                    else:
                        result[col] = result[col].astype(np.uint64)
                else:
                    if c_min > -128 and c_max < 128:
                        result[col] = result[col].astype(np.int8)
                    elif c_min > -32768 and c_max < 32768:
                        result[col] = result[col].astype(np.int16)
                    elif c_min > -2147483648 and c_max < 2147483648:
                        result[col] = result[col].astype(np.int32)
                    else:
                        result[col] = result[col].astype(np.int64)
                        
            # Otimiza números de ponto flutuante
            elif col_type != object and pd.api.types.is_float_dtype(col_type):
                # Testa se float32 é suficiente
                result[col] = pd.to_numeric(result[col], downcast='float')
                
            # Otimiza objetos / strings
            elif col_type == object:
                # Converte para categoria se houver poucos valores únicos
                num_unique = result[col].nunique()
                num_total = len(result[col])
                
                if num_unique / num_total < 0.5:  # Se menos de 50% são valores únicos
                    result[col] = result[col].astype('category')
        
        # Calcula a memória economizada
        end_mem = result.memory_usage().sum() / 1024**2
        savings = (start_mem - end_mem) / start_mem * 100
        
        log.debug(f"Otimização de memória: {start_mem:.2f} MB -> {end_mem:.2f} MB ({savings:.1f}% economia)")
        
        return result
    
    def select_optimal_columns(self, df: pd.DataFrame, 
                              required_columns: List[str] = None) -> List[str]:
        """
        Seleciona colunas ótimas para processamento com base na carga.
        
        Args:
            df: DataFrame fonte
            required_columns: Colunas que devem ser sempre incluídas
            
        Returns:
            Lista de colunas recomendadas
        """
        self._update_system_status()
        
        # Se não temos restrições de memória, retorna todas as colunas
        if self.system_status['load_level'] in ['low', 'normal']:
            return list(df.columns)
            
        # Garante que as colunas requeridas são incluídas
        if required_columns is None:
            required_columns = []
            
        # Criticalidade: se estamos em estado crítico, apenas colunas essenciais
        if self.system_status['load_level'] == 'critical':
            return required_columns
            
        # Estado de alta carga: inclui apenas colunas essenciais + algumas importantes
        # Estratégia: preferir colunas numéricas que ocupam menos memória
        columns_info = []
        for col in df.columns:
            if col in required_columns:
                continue  # Já temos estas
                
            # Calcula o uso de memória da coluna
            mem_usage = df[col].memory_usage() / 1024**2  # MB
            is_numeric = pd.api.types.is_numeric_dtype(df[col].dtype)
            
            columns_info.append({
                'name': col,
                'memory': mem_usage,
                'is_numeric': is_numeric,
                'score': mem_usage * (0.5 if is_numeric else 1.0)  # Prefere numéricas
            })
            
        # Ordena por score (menor = melhor)
        columns_info.sort(key=lambda x: x['score'])
        
        # Seleciona as melhores colunas até usar 50% da memória disponível
        available_mem = self.system_status['available_resources']['memory_free_gb'] * 1024  # MB
        target_mem = available_mem * 0.5  # Usamos até 50% da memória disponível
        
        selected = required_columns.copy()
        current_mem = 0
        
        for col_info in columns_info:
            if current_mem + col_info['memory'] > target_mem:
                break
                
            selected.append(col_info['name'])
            current_mem += col_info['memory']
            
        log.debug(f"Seleção de colunas: {len(selected)}/{len(df.columns)} colunas selecionadas")
        return selected
    
    # Decoradores para uso com funções
    def timeit(self, func):
        """
        Decorador para monitorar o tempo de execução de uma função.
        
        Args:
            func: Função a ser monitorada
            
        Returns:
            Função decorada
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                return func(*args, **kwargs)
            finally:
                execution_time = time.time() - start_time
                self.record_execution_time(func.__name__, execution_time)
        return wrapper
    
    def adaptive_cache(self, max_size: int = 128):
        """
        Decorador que implementa cache adaptativo baseado na carga do sistema.
        
        Args:
            max_size: Tamanho máximo do cache
            
        Returns:
            Decorador para função
        """
        def decorator(func):
            # Configura um cache LRU
            cached_func = lru_cache(maxsize=max_size)(func)
            
            @wraps(func)
            def wrapper(*args, **kwargs):
                self._update_system_status()
                
                # Se a carga é crítica, podemos desabilitar o cache
                if self.system_status['load_level'] == 'critical':
                    if self.system_status['available_resources']['memory_available'] < 10:
                        # Memória muito baixa, não use cache
                        return func(*args, **kwargs)
                
                # Do contrário, use o cache
                return cached_func(*args, **kwargs)
            
            # Adiciona uma função para limpar o cache
            wrapper.cache_clear = cached_func.cache_clear
            wrapper.cache_info = cached_func.cache_info
            
            return wrapper
        return decorator

# Teste unitário
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    log.info("Testando PerformanceOptimizer")
    
    # Cria o otimizador
    optimizer = PerformanceOptimizer()
    
    # 1. Teste de monitoramento
    log.info("Status atual do sistema:")
    log.info(f"Carga: {optimizer.system_status['load_level']}")
    log.info(f"CPU disponível: {optimizer.system_status['available_resources'].get('cpu_available', 'N/A')}%")
    log.info(f"Memória disponível: {optimizer.system_status['available_resources'].get('memory_available', 'N/A')}%")
    log.info(f"Memória livre: {optimizer.system_status['available_resources'].get('memory_free_gb', 'N/A'):.2f} GB")
    
    # 2. Teste de otimização de DataFrame
    log.info("\nTestando otimização de DataFrame...")
    
    # Cria um DataFrame de teste
    n_rows = 100000
    df = pd.DataFrame({
        'id': range(n_rows),
        'value_int': np.random.randint(-1000, 1000, n_rows),
        'value_uint': np.random.randint(0, 1000, n_rows),
        'small_int': np.random.randint(-10, 10, n_rows),
        'large_float': np.random.random(n_rows) * 10000,
        'small_float': np.random.random(n_rows),
        'category': np.random.choice(['A', 'B', 'C', 'D'], n_rows),
        'text': ['Text ' + str(i % 100) for i in range(n_rows)]
    })
    
    # Mede o uso de memória inicial
    mem_before = df.memory_usage().sum() / 1024**2
    log.info(f"Memória inicial: {mem_before:.2f} MB")
    
    # Otimiza o DataFrame
    df_optimized = optimizer.optimize_dataframe(df)
    
    # Mede o uso de memória após otimização
    mem_after = df_optimized.memory_usage().sum() / 1024**2
    log.info(f"Memória após otimização: {mem_after:.2f} MB")
    log.info(f"Economia: {mem_before - mem_after:.2f} MB ({(mem_before - mem_after) / mem_before * 100:.1f}%)")
    
    # 3. Teste do decorador de tempo
    log.info("\nTestando decorador de tempo...")
    
    @optimizer.timeit
    def slow_function(n):
        time.sleep(n)
        return n * 2
    
    # Executa algumas vezes
    slow_function(0.1)
    slow_function(0.2)
    slow_function(0.1)
    
    # Verifica as métricas
    report = optimizer.get_performance_report()
    log.info(f"Tempo médio de execução: {report['execution_stats']['slow_function']['avg']:.4f}s")
    
    # 4. Teste do cache adaptativo
    log.info("\nTestando cache adaptativo...")
    
    @optimizer.adaptive_cache(max_size=128)
    def cached_function(n):
        time.sleep(0.1)  # Simula processamento
        return n * 2
    
    # Executa algumas vezes e mede o tempo
    start = time.time()
    for i in range(5):
        cached_function(10)
    time_uncached = time.time() - start
    
    # Deve ser rápido na segunda vez por causa do cache
    start = time.time()
    for i in range(5):
        cached_function(10)
    time_cached = time.time() - start
    
    log.info(f"Tempo sem cache: {time_uncached:.4f}s")
    log.info(f"Tempo com cache: {time_cached:.4f}s")
    log.info(f"Cache info: {cached_function.cache_info()}")
    
    # 5. Teste de seleção de colunas
    log.info("\nTestando seleção de colunas...")
    
    # Recomendações com base na carga
    optimal_columns = optimizer.select_optimal_columns(df, required_columns=['id', 'value_int'])
    log.info(f"Colunas selecionadas: {len(optimal_columns)}/{len(df.columns)}")
    log.info(f"Colunas: {optimal_columns}")
    
    # 6. Teste de recomendações adaptativas
    log.info("\nTestando recomendações adaptativas...")
    
    batch_size = optimizer.recommended_batch_size()
    workers = optimizer.recommend_parallel_workers()
    
    log.info(f"Tamanho de lote recomendado: {batch_size}")
    log.info(f"Número de workers recomendado: {workers}")
    
    log.info("\nTeste do PerformanceOptimizer concluído.") 