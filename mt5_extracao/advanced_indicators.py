import logging
import pandas as pd
import numpy as np
import traceback
from scipy import stats
from typing import Dict, List, Union, Optional, Tuple

# Configuração de logging
log = logging.getLogger(__name__)
if not log.handlers:
    log.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    log.addHandler(ch)

class AdvancedIndicators:
    """
    Implementa indicadores técnicos avançados e análises estatísticas
    para complementar os indicadores básicos.
    """
    
    def __init__(self):
        log.info("Inicializando módulo de indicadores avançados")
    
    def stochastic_oscillator(self, high: pd.Series, low: pd.Series, close: pd.Series, 
                              k_period: int = 14, d_period: int = 3, smooth_k: int = 3) -> Dict[str, pd.Series]:
        """
        Calcula o Oscilador Estocástico (%K e %D).
        
        Args:
            high: Série de preços máximos
            low: Série de preços mínimos
            close: Série de preços de fechamento
            k_period: Período para cálculo de %K
            d_period: Período para cálculo de %D
            smooth_k: Período para suavização de %K
            
        Returns:
            Dicionário com séries %K e %D
        """
        try:
            # Encontra máximos e mínimos no período
            lowest_low = low.rolling(window=k_period).min()
            highest_high = high.rolling(window=k_period).max()
            
            # Calcula %K (não suavizado)
            k_raw = 100 * ((close - lowest_low) / (highest_high - lowest_low))
            
            # Aplica suavização em %K se solicitado
            k = k_raw.rolling(window=smooth_k).mean() if smooth_k > 1 else k_raw
            
            # Calcula %D (média móvel de %K)
            d = k.rolling(window=d_period).mean()
            
            return {
                'stoch_k': k,
                'stoch_d': d
            }
        except Exception as e:
            log.error(f"Erro ao calcular Oscilador Estocástico: {str(e)}")
            log.debug(traceback.format_exc())
            return {
                'stoch_k': pd.Series(np.nan, index=close.index),
                'stoch_d': pd.Series(np.nan, index=close.index)
            }
    
    def adx(self, high: pd.Series, low: pd.Series, close: pd.Series, 
            period: int = 14) -> Dict[str, pd.Series]:
        """
        Calcula o Average Directional Index (ADX), +DI e -DI.
        
        Args:
            high: Série de preços máximos
            low: Série de preços mínimos
            close: Série de preços de fechamento
            period: Período para cálculo do ADX
            
        Returns:
            Dicionário com séries ADX, +DI e -DI
        """
        try:
            # Calcular True Range
            tr1 = high - low
            tr2 = abs(high - close.shift(1))
            tr3 = abs(low - close.shift(1))
            tr = pd.DataFrame({
                'tr1': tr1,
                'tr2': tr2,
                'tr3': tr3
            }).max(axis=1)
            
            # Calcular +DM e -DM
            up_move = high - high.shift(1)
            down_move = low.shift(1) - low
            
            # +DM ocorre quando há um movimento para cima e é maior que o movimento para baixo
            plus_dm = ((up_move > down_move) & (up_move > 0)) * up_move
            # -DM ocorre quando há um movimento para baixo e é maior que o movimento para cima
            minus_dm = ((down_move > up_move) & (down_move > 0)) * down_move
            
            # Calcular as médias móveis exponenciais
            tr_period = tr.ewm(alpha=1/period, min_periods=period).mean()
            plus_di = 100 * (plus_dm.ewm(alpha=1/period, min_periods=period).mean() / tr_period)
            minus_di = 100 * (minus_dm.ewm(alpha=1/period, min_periods=period).mean() / tr_period)
            
            # Calcular o índice direcional (DX) e o ADX
            dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
            adx = dx.ewm(alpha=1/period, min_periods=period).mean()
            
            return {
                'adx': adx,
                'plus_di': plus_di,
                'minus_di': minus_di
            }
        except Exception as e:
            log.error(f"Erro ao calcular ADX: {str(e)}")
            log.debug(traceback.format_exc())
            return {
                'adx': pd.Series(np.nan, index=close.index),
                'plus_di': pd.Series(np.nan, index=close.index),
                'minus_di': pd.Series(np.nan, index=close.index)
            }
    
    def cci(self, high: pd.Series, low: pd.Series, close: pd.Series, 
            period: int = 20) -> pd.Series:
        """
        Calcula o Commodity Channel Index (CCI).
        
        Args:
            high: Série de preços máximos
            low: Série de preços mínimos
            close: Série de preços de fechamento
            period: Período para cálculo do CCI
            
        Returns:
            Série com valores do CCI
        """
        try:
            # Preço típico (TP)
            tp = (high + low + close) / 3
            
            # Média móvel simples do preço típico
            sma_tp = tp.rolling(window=period).mean()
            
            # Desvio absoluto médio
            mad = abs(tp - sma_tp).rolling(window=period).mean()
            
            # Fator constante (tradicionalmente 0.015)
            constant = 0.015
            
            # Cálculo do CCI
            cci = (tp - sma_tp) / (constant * mad)
            
            return cci
        except Exception as e:
            log.error(f"Erro ao calcular CCI: {str(e)}")
            log.debug(traceback.format_exc())
            return pd.Series(np.nan, index=close.index)
    
    def fibonacci_levels(self, high: pd.Series, low: pd.Series, trend: str = 'auto', 
                         period: int = 20) -> Dict[str, pd.Series]:
        """
        Calcula níveis de Fibonacci com base nos máximos e mínimos recentes.
        
        Args:
            high: Série de preços máximos
            low: Série de preços mínimos
            trend: 'up', 'down' ou 'auto' para detectar automaticamente
            period: Período para identificar máximos e mínimos
            
        Returns:
            Dicionário com séries para os níveis de Fibonacci
        """
        try:
            # Determinar a tendência se for 'auto'
            if trend == 'auto':
                # Usa a diferença entre as médias para determinar a tendência
                ma_short = high.rolling(window=5).mean()
                ma_long = high.rolling(window=20).mean()
                trend = 'up' if ma_short.iloc[-1] > ma_long.iloc[-1] else 'down'
            
            # Encontrar máximo e mínimo no período
            max_price = high.rolling(window=period).max().iloc[-1]
            min_price = low.rolling(window=period).min().iloc[-1]
            
            # Definir níveis de Fibonacci comuns
            fib_levels = [0, 0.236, 0.382, 0.5, 0.618, 0.786, 1, 1.272, 1.618]
            result = {}
            
            # Calcular níveis com base na tendência
            if trend == 'up':
                # Tendência de alta: retrações de máximo para mínimo
                range_val = max_price - min_price
                for level in fib_levels:
                    level_name = f'fib_{int(level*1000)}'
                    # Para tendência de alta, os níveis partem do máximo para baixo
                    level_value = max_price - (range_val * level)
                    result[level_name] = pd.Series(level_value, index=high.index)
            else:
                # Tendência de baixa: retrações de mínimo para máximo
                range_val = max_price - min_price
                for level in fib_levels:
                    level_name = f'fib_{int(level*1000)}'
                    # Para tendência de baixa, os níveis partem do mínimo para cima
                    level_value = min_price + (range_val * level)
                    result[level_name] = pd.Series(level_value, index=high.index)
            
            return result
        except Exception as e:
            log.error(f"Erro ao calcular níveis de Fibonacci: {str(e)}")
            log.debug(traceback.format_exc())
            return {f'fib_{int(level*1000)}': pd.Series(np.nan, index=high.index) 
                    for level in [0, 0.236, 0.382, 0.5, 0.618, 0.786, 1]}
    
    def calculate_statistics(self, close: pd.Series, period: int = 20) -> Dict[str, pd.Series]:
        """
        Calcula métricas estatísticas como Skewness e Kurtosis.
        
        Args:
            close: Série de preços de fechamento
            period: Período para cálculo das estatísticas
            
        Returns:
            Dicionário com séries para as métricas estatísticas
        """
        try:
            result = {}
            
            # Retornos logarítmicos (mais adequados para análise estatística)
            returns = np.log(close / close.shift(1))
            
            # Calcular skewness (assimetria)
            # Positivo: cauda para direita (retornos positivos extremos)
            # Negativo: cauda para esquerda (retornos negativos extremos)
            result['skewness'] = returns.rolling(window=period).apply(
                lambda x: stats.skew(x, nan_policy='omit'), raw=True
            )
            
            # Calcular kurtosis (curtose)
            # > 3: distribuição com caudas pesadas (fat tails)
            # < 3: distribuição com caudas leves
            # Pandas usa a definição de Fisher que subtrai 3, então 0 é o valor de referência
            result['kurtosis'] = returns.rolling(window=period).apply(
                lambda x: stats.kurtosis(x, nan_policy='omit'), raw=True
            )
            
            # Volatilidade (desvio padrão dos retornos)
            result['volatility'] = returns.rolling(window=period).std() * np.sqrt(252)  # Anualizado
            
            return result
        except Exception as e:
            log.error(f"Erro ao calcular estatísticas: {str(e)}")
            log.debug(traceback.format_exc())
            return {
                'skewness': pd.Series(np.nan, index=close.index),
                'kurtosis': pd.Series(np.nan, index=close.index),
                'volatility': pd.Series(np.nan, index=close.index)
            }
    
    def volume_analysis(self, close: pd.Series, volume: pd.Series, 
                      period: int = 20) -> Dict[str, pd.Series]:
        """
        Realiza análise de volume em relação ao preço.
        
        Args:
            close: Série de preços de fechamento
            volume: Série de volumes
            period: Período para cálculo das métricas
            
        Returns:
            Dicionário com séries para as métricas de volume
        """
        try:
            result = {}
            
            # Cálculo do On-Balance Volume (OBV)
            obv = pd.Series(0, index=close.index)
            for i in range(1, len(close)):
                if close.iloc[i] > close.iloc[i-1]:
                    obv.iloc[i] = obv.iloc[i-1] + volume.iloc[i]
                elif close.iloc[i] < close.iloc[i-1]:
                    obv.iloc[i] = obv.iloc[i-1] - volume.iloc[i]
                else:
                    obv.iloc[i] = obv.iloc[i-1]
            
            result['obv'] = obv
            
            # Volume médio
            result['volume_sma'] = volume.rolling(window=period).mean()
            
            # Rácio do volume atual / volume médio
            result['volume_ratio'] = volume / result['volume_sma']
            
            # Volume ponderado pelo preço (VWAP) intradiário
            # Isso é uma aproximação simplificada que funciona para dados intradiários
            result['vwap'] = ((close * volume).cumsum() / volume.cumsum())
            
            # Price-Volume Trend (PVT)
            percent_change = close.pct_change()
            result['pvt'] = (percent_change * volume).cumsum()
            
            return result
        except Exception as e:
            log.error(f"Erro ao calcular métricas de volume: {str(e)}")
            log.debug(traceback.format_exc())
            return {
                'obv': pd.Series(np.nan, index=close.index),
                'volume_sma': pd.Series(np.nan, index=close.index),
                'volume_ratio': pd.Series(np.nan, index=close.index),
                'vwap': pd.Series(np.nan, index=close.index),
                'pvt': pd.Series(np.nan, index=close.index)
            }
    
    def candle_patterns(self, open_: pd.Series, high: pd.Series, low: pd.Series, 
                      close: pd.Series) -> Dict[str, pd.Series]:
        """
        Identifica padrões de candlestick comuns.
        
        Args:
            open_: Série de preços de abertura
            high: Série de preços máximos
            low: Série de preços mínimos
            close: Série de preços de fechamento
            
        Returns:
            Dicionário com séries booleanas para cada padrão identificado
        """
        try:
            result = {}
            
            # Tamanho do corpo do candle
            body = abs(close - open_)
            
            # Tamanho da sombra superior
            upper_shadow = high - np.maximum(close, open_)
            
            # Tamanho da sombra inferior
            lower_shadow = np.minimum(close, open_) - low
            
            # Tamanho total do candle
            total_range = high - low
            
            # Valor médio do range para normalização
            avg_range = total_range.rolling(window=14).mean()
            
            # Doji (corpo muito pequeno)
            result['doji'] = body < (0.1 * avg_range)
            
            # Martelo (candle de baixa com sombra inferior longa)
            hammer_condition = (
                (lower_shadow > (2 * body)) &  # Sombra inferior pelo menos 2x o corpo
                (upper_shadow < (0.2 * body)) &  # Sombra superior pequena
                (body > 0)  # Assegura que não é um doji
            )
            result['hammer'] = hammer_condition
            
            # Martelo invertido (candle de alta com sombra superior longa)
            inv_hammer_condition = (
                (upper_shadow > (2 * body)) &  # Sombra superior pelo menos 2x o corpo
                (lower_shadow < (0.2 * body)) &  # Sombra inferior pequena
                (body > 0)  # Assegura que não é um doji
            )
            result['inverted_hammer'] = inv_hammer_condition
            
            # Engolfo de alta (bullish engulfing)
            bullish_engulfing = (
                (close.shift(1) < open_.shift(1)) &  # Candle anterior é de baixa
                (close > open_) &  # Candle atual é de alta
                (open_ <= close.shift(1)) &  # Abertura atual <= fechamento anterior
                (close >= open_.shift(1))  # Fechamento atual >= abertura anterior
            )
            result['bullish_engulfing'] = bullish_engulfing
            
            # Engolfo de baixa (bearish engulfing)
            bearish_engulfing = (
                (close.shift(1) > open_.shift(1)) &  # Candle anterior é de alta
                (close < open_) &  # Candle atual é de baixa
                (open_ >= close.shift(1)) &  # Abertura atual >= fechamento anterior
                (close <= open_.shift(1))  # Fechamento atual <= abertura anterior
            )
            result['bearish_engulfing'] = bearish_engulfing
            
            # Estrela da Manhã (Morning Star) - padrão de três candles
            morning_star = (
                (close.shift(2) < open_.shift(2)) &  # Primeiro candle é de baixa
                (abs(close.shift(1) - open_.shift(1)) < (0.3 * avg_range.shift(1))) &  # Segundo candle pequeno
                (close > open_) &  # Terceiro candle é de alta
                (close > ((open_.shift(2) + close.shift(2)) / 2))  # Fechamento > média do primeiro candle
            )
            result['morning_star'] = morning_star
            
            # Estrela da Noite (Evening Star) - padrão de três candles
            evening_star = (
                (close.shift(2) > open_.shift(2)) &  # Primeiro candle é de alta
                (abs(close.shift(1) - open_.shift(1)) < (0.3 * avg_range.shift(1))) &  # Segundo candle pequeno
                (close < open_) &  # Terceiro candle é de baixa
                (close < ((open_.shift(2) + close.shift(2)) / 2))  # Fechamento < média do primeiro candle
            )
            result['evening_star'] = evening_star
            
            return result
        except Exception as e:
            log.error(f"Erro ao identificar padrões de candlestick: {str(e)}")
            log.debug(traceback.format_exc())
            empty_series = pd.Series(False, index=close.index)
            return {
                'doji': empty_series.copy(),
                'hammer': empty_series.copy(),
                'inverted_hammer': empty_series.copy(),
                'bullish_engulfing': empty_series.copy(),
                'bearish_engulfing': empty_series.copy(),
                'morning_star': empty_series.copy(),
                'evening_star': empty_series.copy()
            }

    def support_resistance(self, high: pd.Series, low: pd.Series, close: pd.Series,
                         period: int = 20, sensitivity: float = 1.0) -> Dict[str, pd.Series]:
        """
        Identifica níveis de suporte e resistência.
        
        Args:
            high: Série de preços máximos
            low: Série de preços mínimos
            close: Série de preços de fechamento
            period: Período para identificação de pivôs
            sensitivity: Sensibilidade para detecção (ajusta o 'ruído' permitido)
            
        Returns:
            Dicionário com séries para níveis de suporte e resistência
        """
        try:
            result = {}
            
            # Cria uma janela deslizante para identificar pivôs (máximos e mínimos locais)
            def is_pivot_high(window):
                center = len(window) // 2
                if all(window[center] >= window[i] for i in range(len(window)) if i != center):
                    return window[center]
                return np.nan
            
            def is_pivot_low(window):
                center = len(window) // 2
                if all(window[center] <= window[i] for i in range(len(window)) if i != center):
                    return window[center]
                return np.nan
            
            # Identifica pivôs
            pivot_highs = high.rolling(window=period, center=True).apply(
                is_pivot_high, raw=True)
            pivot_lows = low.rolling(window=period, center=True).apply(
                is_pivot_low, raw=True)
            
            # Ajusta sensibilidade
            min_distance = sensitivity * close.rolling(window=period).std()
            
            # Processamento dos pivôs para identificar suportes e resistências
            resistances = []
            supports = []
            
            # Identifica resistências
            for i in range(len(pivot_highs)):
                if not np.isnan(pivot_highs.iloc[i]):
                    # Verifica se não há uma resistência próxima já identificada
                    is_new = True
                    for r in resistances:
                        if abs(r - pivot_highs.iloc[i]) < min_distance.iloc[i]:
                            is_new = False
                            break
                    if is_new:
                        resistances.append(pivot_highs.iloc[i])
            
            # Identifica suportes
            for i in range(len(pivot_lows)):
                if not np.isnan(pivot_lows.iloc[i]):
                    # Verifica se não há um suporte próximo já identificado
                    is_new = True
                    for s in supports:
                        if abs(s - pivot_lows.iloc[i]) < min_distance.iloc[i]:
                            is_new = False
                            break
                    if is_new:
                        supports.append(pivot_lows.iloc[i])
            
            # Converte para séries
            for i, level in enumerate(sorted(resistances)):
                result[f'resistance_{i+1}'] = pd.Series(level, index=close.index)
            
            for i, level in enumerate(sorted(supports, reverse=True)):
                result[f'support_{i+1}'] = pd.Series(level, index=close.index)
            
            return result
        except Exception as e:
            log.error(f"Erro ao calcular suportes e resistências: {str(e)}")
            log.debug(traceback.format_exc())
            return {
                'support_1': pd.Series(np.nan, index=close.index),
                'resistance_1': pd.Series(np.nan, index=close.index)
            }
    
    def trend_analysis(self, close: pd.Series, period_short: int = 20, 
                      period_long: int = 50) -> Dict[str, Union[pd.Series, float]]:
        """
        Analisa a tendência do preço usando múltiplas técnicas.
        
        Args:
            close: Série de preços de fechamento
            period_short: Período curto para análise
            period_long: Período longo para análise
            
        Returns:
            Dicionário com métricas de análise de tendência
        """
        try:
            result = {}
            
            # Médias móveis
            ma_short = close.rolling(window=period_short).mean()
            ma_long = close.rolling(window=period_long).mean()
            
            # Direção da tendência
            # 1: alta, -1: baixa, 0: sem tendência definida
            trend_direction = np.where(ma_short > ma_long, 1, 
                               np.where(ma_short < ma_long, -1, 0))
            result['trend_direction'] = pd.Series(trend_direction, index=close.index)
            
            # Força da tendência (razão entre as médias)
            result['trend_strength'] = ma_short / ma_long
            
            # Duração da tendência atual (dias)
            crossover = (ma_short > ma_long) != (ma_short.shift(1) > ma_long.shift(1))
            last_crossover = 0
            for i in reversed(range(len(crossover))):
                if crossover.iloc[i]:
                    last_crossover = i
                    break
            
            result['trend_duration'] = pd.Series(
                np.arange(len(close)) - last_crossover, index=close.index
            )
            
            # Linear regression (usando numpy polyfit)
            x = np.arange(period_long)
            window_close = close.iloc[-period_long:].values if len(close) >= period_long else close.values
            if len(window_close) > 1:  # Precisa de ao menos 2 pontos para a regressão
                slope, intercept = np.polyfit(x[:len(window_close)], window_close, 1)
                result['trend_slope'] = pd.Series(slope, index=close.index)
                
                # R-squared (coeficiente de determinação)
                y_pred = intercept + slope * x[:len(window_close)]
                ss_total = np.sum((window_close - np.mean(window_close))**2)
                ss_residual = np.sum((window_close - y_pred)**2)
                r_squared = 1 - (ss_residual / ss_total)
                result['trend_r_squared'] = pd.Series(r_squared, index=close.index)
            else:
                result['trend_slope'] = pd.Series(np.nan, index=close.index)
                result['trend_r_squared'] = pd.Series(np.nan, index=close.index)
            
            return result
        except Exception as e:
            log.error(f"Erro ao analisar tendência: {str(e)}")
            log.debug(traceback.format_exc())
            return {
                'trend_direction': pd.Series(np.nan, index=close.index),
                'trend_strength': pd.Series(np.nan, index=close.index),
                'trend_duration': pd.Series(np.nan, index=close.index),
                'trend_slope': pd.Series(np.nan, index=close.index),
                'trend_r_squared': pd.Series(np.nan, index=close.index)
            }
            
    def market_context(self, df: pd.DataFrame, df_index: Optional[pd.DataFrame] = None) -> Dict[str, pd.Series]:
        """
        Adiciona informações contextuais do mercado.
        
        Args:
            df: DataFrame com dados OHLCV
            df_index: DataFrame opcional com dados de índice de referência
            
        Returns:
            Dicionário com métricas de contexto de mercado
        """
        try:
            result = {}
            
            # Extrai informações temporais
            if 'time' in df.columns:
                # Dia da semana (0=segunda, 6=domingo)
                result['day_of_week'] = df['time'].dt.dayofweek
                
                # Hora do dia
                result['hour_of_day'] = df['time'].dt.hour
                
                # Sessão de negociação
                # Simplificado - pode ser customizado para diferentes mercados
                result['trading_session'] = pd.Series(0, index=df.index)  # 0=indefinido
                # Exemplo para mercado brasileiro: 
                # 0=pre-market, 1=regular, 2=after
                mask_regular = (df['time'].dt.hour >= 10) & (df['time'].dt.hour < 17)
                result['trading_session'].loc[mask_regular] = 1
                mask_premarket = (df['time'].dt.hour >= 9) & (df['time'].dt.hour < 10)
                result['trading_session'].loc[mask_premarket] = 0
                mask_aftermarket = (df['time'].dt.hour >= 17) & (df['time'].dt.hour < 18)
                result['trading_session'].loc[mask_aftermarket] = 2
            
            # Volatilidade de mercado
            if 'high' in df.columns and 'low' in df.columns:
                # Volatilidade intradiária (High-Low Range / Close)
                result['intraday_volatility'] = (df['high'] - df['low']) / df['close']
            
            # Correlação com índice, se disponível
            if df_index is not None and 'close' in df_index.columns and 'close' in df.columns:
                # Certifica-se de ter os mesmos timestamps
                common_index = df['time'].intersection(df_index['time'])
                if len(common_index) > 0:
                    df_aligned = df[df['time'].isin(common_index)]
                    df_index_aligned = df_index[df_index['time'].isin(common_index)]
                    
                    # Calcula correlação com janela móvel
                    window = min(20, len(common_index))
                    corr = pd.Series(np.nan, index=df.index)
                    
                    for i in range(window-1, len(df_aligned)):
                        if i >= window:
                            corr_window = np.corrcoef(
                                df_aligned['close'].iloc[i-window+1:i+1], 
                                df_index_aligned['close'].iloc[i-window+1:i+1]
                            )[0, 1]
                            corr.iloc[df_aligned.index[i]] = corr_window
                    
                    result['index_correlation'] = corr
            
            return result
        except Exception as e:
            log.error(f"Erro ao calcular contexto de mercado: {str(e)}")
            log.debug(traceback.format_exc())
            return {} 