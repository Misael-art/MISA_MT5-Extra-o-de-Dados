import logging
import pandas as pd
import numpy as np
import traceback
from typing import Dict, List, Union, Optional, Tuple

from mt5_extracao.indicator_calculator import IndicatorCalculator
from mt5_extracao.advanced_indicators import AdvancedIndicators

# Configuração de logging
log = logging.getLogger(__name__)
if not log.handlers:
    log.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    log.addHandler(ch)

class EnhancedIndicatorCalculator:
    """
    Integra os indicadores básicos com os avançados, proporcionando uma interface unificada
    para cálculo e processamento de indicadores técnicos, estatísticas e análises de mercado.
    
    Esta classe atua como uma camada de integração, delegando os cálculos para as classes
    especializadas e unindo os resultados.
    """
    
    def __init__(self):
        """Inicializa o calculador de indicadores avançados."""
        log.info("Inicializando EnhancedIndicatorCalculator")
        self.basic_calculator = IndicatorCalculator()
        self.advanced_calculator = AdvancedIndicators()
        
    def calculate_all_indicators(self, df: pd.DataFrame, 
                               include_market_context: bool = True,
                               include_advanced_stats: bool = True,
                               include_candle_patterns: bool = True,
                               include_volume_analysis: bool = True,
                               include_trend_analysis: bool = True,
                               include_support_resistance: bool = True,
                               period: int = 20) -> pd.DataFrame:
        """
        Calcula todos os indicadores disponíveis e adiciona ao DataFrame.
        
        Args:
            df: DataFrame com colunas OHLCV
            include_market_context: Se deve incluir análises contextuais
            include_advanced_stats: Se deve incluir estatísticas avançadas
            include_candle_patterns: Se deve incluir padrões de candles
            include_volume_analysis: Se deve incluir análise de volume
            include_trend_analysis: Se deve incluir análise de tendência
            include_support_resistance: Se deve incluir suportes e resistências
            period: Período base para cálculos
            
        Returns:
            DataFrame com todos os indicadores calculados
        """
        if df is None or df.empty:
            log.warning("DataFrame vazio recebido para cálculo de indicadores.")
            return df
            
        required_columns = ['open', 'high', 'low', 'close', 'time']
        if not all(col in df.columns for col in required_columns):
            log.error("DataFrame não contém colunas necessárias para indicadores.")
            missing = [col for col in required_columns if col not in df.columns]
            log.error(f"Colunas ausentes: {missing}")
            return df
            
        try:
            log.info(f"Calculando indicadores para DataFrame com {len(df)} linhas...")
            
            # 1. Calcular indicadores básicos primeiro
            df = self.basic_calculator.calculate_technical_indicators(df.copy())
            df = self.basic_calculator.calculate_price_variations(df)
            
            # 2. Calcular indicadores avançados
            # Stochastic Oscillator
            stoch = self.advanced_calculator.stochastic_oscillator(
                df['high'], df['low'], df['close'])
            for key, value in stoch.items():
                df[key] = value
                
            # ADX
            adx_result = self.advanced_calculator.adx(
                df['high'], df['low'], df['close'])
            for key, value in adx_result.items():
                df[key] = value
                
            # CCI
            df['cci'] = self.advanced_calculator.cci(
                df['high'], df['low'], df['close'])
                
            # Fibonacci levels (apenas para o último candle para não sobrecarregar)
            # Note: Os níveis são estáticos para toda a série, então salvamos apenas a última linha
            if len(df) > 0:
                fib_levels = self.advanced_calculator.fibonacci_levels(
                    df['high'], df['low'], trend='auto', period=period)
                for key, value in fib_levels.items():
                    df[key] = value.iloc[-1]
                
            # 3. Calcular estatísticas avançadas conforme opções
            if include_advanced_stats:
                stats = self.advanced_calculator.calculate_statistics(
                    df['close'], period=period)
                for key, value in stats.items():
                    df[key] = value
                    
            # 4. Identificar padrões de candle
            if include_candle_patterns:
                patterns = self.advanced_calculator.candle_patterns(
                    df['open'], df['high'], df['low'], df['close'])
                for key, value in patterns.items():
                    df[f'pattern_{key}'] = value
                    
            # 5. Análise de volume se disponível
            if include_volume_analysis and 'volume' in df.columns:
                volume_metrics = self.advanced_calculator.volume_analysis(
                    df['close'], df['volume'], period=period)
                for key, value in volume_metrics.items():
                    df[key] = value
                    
            # 6. Análise de tendência
            if include_trend_analysis:
                trend_metrics = self.advanced_calculator.trend_analysis(
                    df['close'], period_short=period, period_long=period*2)
                for key, value in trend_metrics.items():
                    df[key] = value
                    
            # 7. Suportes e resistências
            if include_support_resistance:
                sr_levels = self.advanced_calculator.support_resistance(
                    df['high'], df['low'], df['close'], period=period)
                for key, value in sr_levels.items():
                    df[key] = value
                    
            # 8. Contexto de mercado
            if include_market_context:
                context = self.advanced_calculator.market_context(df)
                for key, value in context.items():
                    df[key] = value
            
            log.info(f"Cálculo completo - DataFrame agora contém {len(df.columns)} colunas.")
            return df
            
        except Exception as e:
            log.error(f"Erro ao calcular indicadores avançados: {str(e)}")
            log.debug(traceback.format_exc())
            return df
            
    def calculate_technical_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Mantém compatibilidade com a interface original do IndicatorCalculator.
        Calcula apenas os indicadores técnicos básicos.
        
        Args:
            df: DataFrame com colunas OHLC
            
        Returns:
            DataFrame com indicadores básicos calculados
        """
        return self.basic_calculator.calculate_technical_indicators(df)
        
    def calculate_price_variations(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Mantém compatibilidade com a interface original do IndicatorCalculator.
        Calcula apenas as variações percentuais.
        
        Args:
            df: DataFrame com colunas 'time', 'open', 'high', 'low', 'close'
            
        Returns:
            DataFrame com variações percentuais calculadas
        """
        return self.basic_calculator.calculate_price_variations(df)
        
    def calculate_advanced_indicators(self, df: pd.DataFrame, 
                                   indicators: List[str] = None,
                                   period: int = 20) -> pd.DataFrame:
        """
        Calcula indicadores avançados específicos.
        
        Args:
            df: DataFrame com colunas OHLCV
            indicators: Lista de indicadores a calcular (None = todos)
            period: Período para os cálculos
            
        Returns:
            DataFrame com indicadores avançados calculados
        """
        if df is None or df.empty:
            log.warning("DataFrame vazio recebido para cálculo de indicadores avançados.")
            return df
            
        required_columns = ['open', 'high', 'low', 'close']
        if not all(col in df.columns for col in required_columns):
            log.error("DataFrame não contém colunas OHLC necessárias para indicadores avançados.")
            missing = [col for col in required_columns if col not in df.columns]
            log.error(f"Colunas ausentes: {missing}")
            return df
            
        available_indicators = {
            'stochastic': lambda df: self.advanced_calculator.stochastic_oscillator(
                df['high'], df['low'], df['close']),
            'adx': lambda df: self.advanced_calculator.adx(
                df['high'], df['low'], df['close']),
            'cci': lambda df: {'cci': self.advanced_calculator.cci(
                df['high'], df['low'], df['close'])},
            'fibonacci': lambda df: self.advanced_calculator.fibonacci_levels(
                df['high'], df['low'], trend='auto', period=period),
            'statistics': lambda df: self.advanced_calculator.calculate_statistics(
                df['close'], period=period),
            'candle_patterns': lambda df: self.advanced_calculator.candle_patterns(
                df['open'], df['high'], df['low'], df['close']),
            'trend': lambda df: self.advanced_calculator.trend_analysis(
                df['close'], period_short=period, period_long=period*2),
            'support_resistance': lambda df: self.advanced_calculator.support_resistance(
                df['high'], df['low'], df['close'], period=period),
        }
        
        # Volume é adicional, requer coluna específica
        if 'volume' in df.columns:
            available_indicators['volume'] = lambda df: self.advanced_calculator.volume_analysis(
                df['close'], df['volume'], period=period)
        
        # Contexto de mercado requer coluna time
        if 'time' in df.columns:
            available_indicators['market_context'] = lambda df: self.advanced_calculator.market_context(df)
        
        # Se nenhum indicador específico for solicitado, calcular todos disponíveis
        if indicators is None:
            indicators = list(available_indicators.keys())
        
        try:
            for indicator in indicators:
                if indicator in available_indicators:
                    result = available_indicators[indicator](df)
                    if isinstance(result, dict):
                        for key, value in result.items():
                            df[key] = value
                    else:
                        df[indicator] = result
                else:
                    log.warning(f"Indicador '{indicator}' não encontrado ou não implementado.")
            
            return df
        
        except Exception as e:
            log.error(f"Erro ao calcular indicadores avançados específicos: {str(e)}")
            log.debug(traceback.format_exc())
            return df
            
    def analyze_price_patterns(self, df: pd.DataFrame) -> Dict[str, List[Dict]]:
        """
        Analisa padrões de preço complexos identificados na série.
        
        Args:
            df: DataFrame com colunas OHLCV
            
        Returns:
            Dicionário com padrões identificados e suas características
        """
        if df is None or df.empty or len(df) < 10:
            log.warning("DataFrame insuficiente para análise de padrões.")
            return {}
            
        try:
            # Adiciona os indicadores necessários para identificação de padrões
            df_with_indicators = self.calculate_all_indicators(
                df.copy(), 
                include_market_context=False,
                include_support_resistance=False
            )
            
            patterns = {}
            
            # Identifica padrões de candle
            candle_patterns = {}
            pattern_cols = [col for col in df_with_indicators.columns if col.startswith('pattern_')]
            
            for col in pattern_cols:
                pattern_name = col.replace('pattern_', '')
                pattern_indices = df_with_indicators.index[df_with_indicators[col] == True].tolist()
                
                if pattern_indices:
                    candle_patterns[pattern_name] = []
                    for idx in pattern_indices:
                        candle_patterns[pattern_name].append({
                            'time': df_with_indicators.loc[idx, 'time'],
                            'price': df_with_indicators.loc[idx, 'close'],
                            'strength': 'strong' if (
                                df_with_indicators.loc[idx, 'volume'] > df_with_indicators['volume'].mean()
                                if 'volume' in df_with_indicators.columns else True
                            ) else 'normal'
                        })
            
            patterns['candle_patterns'] = candle_patterns
            
            # Identifica padrões de tendência
            if 'trend_direction' in df_with_indicators.columns:
                trends = {
                    'uptrend': [],
                    'downtrend': [],
                    'sideways': []
                }
                
                # Encontrar sequências de tendência
                current_trend = 0
                trend_start = 0
                
                for i in range(1, len(df_with_indicators)):
                    direction = df_with_indicators['trend_direction'].iloc[i]
                    
                    # Nova tendência ou mudança de tendência
                    if direction != current_trend:
                        # Registra a tendência anterior se tiver duração mínima
                        if i - trend_start >= 5 and current_trend != 0:
                            trend_type = 'uptrend' if current_trend == 1 else (
                                'downtrend' if current_trend == -1 else 'sideways')
                            
                            trends[trend_type].append({
                                'start_time': df_with_indicators['time'].iloc[trend_start],
                                'end_time': df_with_indicators['time'].iloc[i-1],
                                'duration': i - trend_start,
                                'start_price': df_with_indicators['close'].iloc[trend_start],
                                'end_price': df_with_indicators['close'].iloc[i-1],
                                'change_pct': ((df_with_indicators['close'].iloc[i-1] / 
                                             df_with_indicators['close'].iloc[trend_start]) - 1) * 100
                            })
                        
                        # Inicia nova tendência
                        current_trend = direction
                        trend_start = i
                
                # Registra a última tendência
                if current_trend != 0 and len(df_with_indicators) - trend_start >= 5:
                    trend_type = 'uptrend' if current_trend == 1 else (
                        'downtrend' if current_trend == -1 else 'sideways')
                    
                    trends[trend_type].append({
                        'start_time': df_with_indicators['time'].iloc[trend_start],
                        'end_time': df_with_indicators['time'].iloc[-1],
                        'duration': len(df_with_indicators) - trend_start,
                        'start_price': df_with_indicators['close'].iloc[trend_start],
                        'end_price': df_with_indicators['close'].iloc[-1],
                        'change_pct': ((df_with_indicators['close'].iloc[-1] / 
                                     df_with_indicators['close'].iloc[trend_start]) - 1) * 100
                    })
                
                patterns['trends'] = trends
            
            # Identificar suportes e resistências significativos testados
            if any(col.startswith('support_') or col.startswith('resistance_') for col in df_with_indicators.columns):
                sr_tests = {
                    'support_tests': [],
                    'resistance_tests': []
                }
                
                # Suportes
                support_cols = [col for col in df_with_indicators.columns if col.startswith('support_')]
                for col in support_cols:
                    level = df_with_indicators[col].iloc[-1]
                    if not pd.isna(level):
                        # Verifica se o preço se aproximou do suporte
                        tests = []
                        for i in range(len(df_with_indicators)):
                            # Suporte é testado quando o preço chega a 0.1% do nível
                            if abs(df_with_indicators['low'].iloc[i] / level - 1) < 0.001:
                                tests.append(i)
                        
                        if tests:
                            sr_tests['support_tests'].append({
                                'level': level,
                                'tests_count': len(tests),
                                'last_test': df_with_indicators['time'].iloc[tests[-1]] if tests else None,
                                'strength': 'strong' if len(tests) > 2 else 'moderate'
                            })
                
                # Resistências
                resistance_cols = [col for col in df_with_indicators.columns if col.startswith('resistance_')]
                for col in resistance_cols:
                    level = df_with_indicators[col].iloc[-1]
                    if not pd.isna(level):
                        # Verifica se o preço se aproximou da resistência
                        tests = []
                        for i in range(len(df_with_indicators)):
                            # Resistência é testada quando o preço chega a 0.1% do nível
                            if abs(df_with_indicators['high'].iloc[i] / level - 1) < 0.001:
                                tests.append(i)
                        
                        if tests:
                            sr_tests['resistance_tests'].append({
                                'level': level,
                                'tests_count': len(tests),
                                'last_test': df_with_indicators['time'].iloc[tests[-1]] if tests else None,
                                'strength': 'strong' if len(tests) > 2 else 'moderate'
                            })
                
                patterns['sr_tests'] = sr_tests
            
            # Identificar divergências com indicadores
            if 'rsi' in df_with_indicators.columns:
                divergences = {
                    'bullish_divergences': [],
                    'bearish_divergences': []
                }
                
                # Procura por divergências em janelas de 10 candles
                for i in range(10, len(df_with_indicators)):
                    window = df_with_indicators.iloc[i-10:i+1]
                    
                    # Divergência de baixa: preço faz alta mais alta, RSI faz alta mais baixa
                    if window['close'].iloc[-1] > window['close'].max():
                        if window['rsi'].iloc[-1] < window['rsi'].max():
                            divergences['bearish_divergences'].append({
                                'time': window['time'].iloc[-1],
                                'price': window['close'].iloc[-1],
                                'indicator': 'rsi',
                                'indicator_value': window['rsi'].iloc[-1]
                            })
                    
                    # Divergência de alta: preço faz baixa mais baixa, RSI faz baixa mais alta
                    if window['close'].iloc[-1] < window['close'].min():
                        if window['rsi'].iloc[-1] > window['rsi'].min():
                            divergences['bullish_divergences'].append({
                                'time': window['time'].iloc[-1],
                                'price': window['close'].iloc[-1],
                                'indicator': 'rsi',
                                'indicator_value': window['rsi'].iloc[-1]
                            })
                
                patterns['divergences'] = divergences
            
            return patterns
            
        except Exception as e:
            log.error(f"Erro ao analisar padrões de preço: {str(e)}")
            log.debug(traceback.format_exc())
            return {}

# Teste unitário
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    log.info("Testando EnhancedIndicatorCalculator")
    
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
    
    # Inicializa o calculador e processa o DataFrame
    calculator = EnhancedIndicatorCalculator()
    result = calculator.calculate_all_indicators(df)
    
    log.info(f"DataFrame original: {df.shape}, com indicadores: {result.shape}")
    log.info(f"Novas colunas: {set(result.columns) - set(df.columns)}")
    
    # Testa a análise de padrões
    patterns = calculator.analyze_price_patterns(df)
    log.info(f"Padrões detectados: {patterns.keys()}")
    
    log.info("Teste concluído com sucesso") 