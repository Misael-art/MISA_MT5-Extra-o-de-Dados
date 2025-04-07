import logging
import pandas as pd
import numpy as np
import traceback

# Configuração de logging (pode ser centralizada depois)
log = logging.getLogger(__name__)
if not log.handlers:
    log.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    # Adicionar um handler de console para depuração inicial
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    log.addHandler(ch)
    # TODO: Configurar handler de arquivo se necessário, ou usar config central

# Tenta importar pandas_ta e define fallback
try:
    # Patch para compatibilidade com numpy 2.x (se necessário)
    if not hasattr(np, 'NaN'):
        np.NaN = float('nan')
    import pandas_ta as ta
    log.info("pandas_ta importado com sucesso.")
except ImportError:
    log.warning("Módulo pandas_ta não encontrado. Indicadores serão limitados.")
    ta = None
except Exception as e:
    log.warning(f"Erro ao importar pandas_ta: {e}. Indicadores serão limitados.")
    ta = None

# Implementação básica de indicadores para substituir pandas_ta (fallback)
class BasicIndicators:
    """Implementações básicas de indicadores como fallback para pandas_ta."""
    def __init__(self):
        pass # Nenhuma inicialização necessária por enquanto

    def rsi(self, close, length=14):
        """Implementação básica de RSI"""
        try:
            delta = close.diff()
            gain = delta.where(delta > 0, 0)
            loss = -delta.where(delta < 0, 0)
            # Usa Simple Moving Average (SMA) para média, como pandas_ta faz por padrão
            avg_gain = gain.rolling(window=length, min_periods=length).mean()
            avg_loss = loss.rolling(window=length, min_periods=length).mean()
            rs = avg_gain / avg_loss
            rsi = 100.0 - (100.0 / (1.0 + rs))
            return rsi
        except Exception as e:
            log.error(f"Erro ao calcular RSI (básico): {str(e)}")
            return pd.Series(np.nan, index=close.index)

    def sma(self, close, length=20):
        """Implementação básica de Média Móvel Simples (SMA)"""
        try:
            return close.rolling(window=length, min_periods=length).mean()
        except Exception as e:
            log.error(f"Erro ao calcular SMA (básico): {str(e)}")
            return pd.Series(np.nan, index=close.index)

    def true_range(self, high, low, close):
        """Implementação básica de True Range (TR)"""
        try:
            high_low = high - low
            high_close_prev = np.abs(high - close.shift(1))
            low_close_prev = np.abs(low - close.shift(1))
            tr = np.maximum(high_low, np.maximum(high_close_prev, low_close_prev))
            return tr
        except Exception as e:
            log.error(f"Erro ao calcular True Range (básico): {str(e)}")
            return pd.Series(np.nan, index=close.index)

    def atr(self, high, low, close, length=14):
        """Implementação básica de Average True Range (ATR)"""
        try:
            tr = self.true_range(high, low, close)
            atr = tr.rolling(window=length, min_periods=length).mean() # Usa SMA para média
            return atr
        except Exception as e:
            log.error(f"Erro ao calcular ATR (básico): {str(e)}")
            return pd.Series(np.nan, index=close.index)

    # TODO: Adicionar implementações básicas para MACD e Bollinger Bands se necessário


class IndicatorCalculator:
    """
    Calcula indicadores técnicos e variações percentuais para DataFrames OHLCV.
    """
    def __init__(self):
        self.use_pandas_ta = ta is not None
        if not self.use_pandas_ta:
            log.warning("Usando implementações básicas de indicadores (pandas_ta indisponível).")
            self.basic_indicators = BasicIndicators()
        else:
            self.basic_indicators = None # Não necessário se pandas_ta estiver disponível

    def calculate_technical_indicators(self, df):
        """
        Calcula e adiciona colunas de indicadores técnicos ao DataFrame.

        Args:
            df (pd.DataFrame): DataFrame com colunas 'open', 'high', 'low', 'close'.

        Returns:
            pd.DataFrame: DataFrame original com colunas de indicadores adicionadas.
        """
        if df is None or df.empty:
            log.warning("DataFrame vazio recebido para cálculo de indicadores.")
            return df
        if not all(col in df.columns for col in ['open', 'high', 'low', 'close']):
             log.error("DataFrame não contém colunas OHLC necessárias para indicadores.")
             return df

        log.debug(f"Calculando indicadores para DataFrame com {len(df)} linhas...")
        try:
            if self.use_pandas_ta:
                log.debug("Usando pandas_ta para indicadores.")
                # RSI
                df['rsi'] = ta.rsi(df['close'], length=14)
                # MACD
                macd = ta.macd(df['close'])
                if macd is not None and not macd.empty:
                    df['macd_line'] = macd.get('MACD_12_26_9')
                    df['macd_signal'] = macd.get('MACDs_12_26_9')
                    df['macd_histogram'] = macd.get('MACDh_12_26_9')
                # MA 20
                df['ma_20'] = ta.sma(df['close'], length=20)
                # Bollinger Bands
                bbands = ta.bbands(df['close'])
                if bbands is not None and not bbands.empty:
                    df['bollinger_upper'] = bbands.get('BBU_20_2.0')
                    df['bollinger_lower'] = bbands.get('BBL_20_2.0')
                # ATR
                df['atr'] = ta.atr(df['high'], df['low'], df['close'], length=14)
                # True Range (pode ser útil)
                df['true_range'] = ta.true_range(df['high'], df['low'], df['close'])
            else:
                # Usa implementações básicas
                log.debug("Usando BasicIndicators (fallback).")
                df['rsi'] = self.basic_indicators.rsi(df['close'], length=14)
                df['ma_20'] = self.basic_indicators.sma(df['close'], length=20)
                df['true_range'] = self.basic_indicators.true_range(df['high'], df['low'], df['close'])
                df['atr'] = self.basic_indicators.atr(df['high'], df['low'], df['close'], length=14)
                # MACD e Bollinger não implementados no fallback básico por enquanto
                df['macd_line'] = np.nan
                df['macd_signal'] = np.nan
                df['macd_histogram'] = np.nan
                df['bollinger_upper'] = np.nan
                df['bollinger_lower'] = np.nan

            log.debug("Cálculo de indicadores concluído.")
            return df

        except Exception as e:
            log.error(f"Erro ao calcular indicadores técnicos: {e}")
            log.debug(traceback.format_exc())
            # Retorna o DataFrame original em caso de erro para não quebrar o fluxo
            return df

    def calculate_price_variations(self, df):
        """
        Calcula e adiciona colunas de variação percentual ao DataFrame.

        Args:
            df (pd.DataFrame): DataFrame com colunas 'time', 'open', 'high', 'low', 'close'.
                               Deve estar ordenado por 'time'.
        Returns:
            pd.DataFrame: DataFrame original com colunas de variação adicionadas.
        """
        if df is None or df.empty:
            log.warning("DataFrame vazio recebido para cálculo de variações.")
            return df
        if not all(col in df.columns for col in ['time', 'open', 'high', 'low', 'close']):
             log.error("DataFrame não contém colunas necessárias para variações.")
             return df

        log.debug(f"Calculando variações para DataFrame com {len(df)} linhas...")
        try:
            # Garante ordenação por tempo (importante para cálculos baseados em iloc)
            df = df.sort_values('time')

            # Último preço de fechamento (usado como referência)
            last_close = df['close'].iloc[-1]

            # Variação vs abertura dos últimos 5 minutos
            df_5min = df.iloc[-5:]
            if len(df_5min) > 0:
                open_5min = df_5min['open'].iloc[0]
                df['var_pct_open_5min'] = (last_close - open_5min) / open_5min * 100 if open_5min else np.nan

            # Variação vs fechamento dos últimos 15 minutos
            if len(df) >= 15:
                close_15min = df['close'].iloc[-15]
                df['var_pct_close_15min'] = (last_close - close_15min) / close_15min * 100 if close_15min else np.nan

            # Variação vs máxima dos últimos 30 minutos
            df_30min = df.iloc[-30:]
            if len(df_30min) > 0:
                high_30min = df_30min['high'].max()
                df['var_pct_high_30min'] = (last_close - high_30min) / high_30min * 100 if high_30min else np.nan

            # Variação vs mínima dos últimos 60 minutos
            df_60min = df.iloc[-60:]
            if len(df_60min) > 0:
                low_60min = df_60min['low'].min()
                df['var_pct_low_60min'] = (last_close - low_60min) / low_60min * 100 if low_60min else np.nan

            # Variação vs abertura do dia
            today = pd.Timestamp.now().normalize() # Data de hoje sem hora
            df_today = df[df['time'] >= today]
            if not df_today.empty:
                open_daily = df_today['open'].iloc[0]
                df['var_pct_open_daily'] = (last_close - open_daily) / open_daily * 100 if open_daily else np.nan

            # Variação vs fechamento do dia anterior (requer dados do dia anterior)
            # TODO: Implementar lógica para buscar/usar fechamento do dia anterior se necessário.
            df['var_pct_close_prev_day'] = np.nan

            log.debug("Cálculo de variações concluído.")
            return df

        except Exception as e:
            log.error(f"Erro ao calcular variações: {e}")
            log.debug(traceback.format_exc())
            # Retorna o DataFrame original em caso de erro
            return df


# Exemplo de uso (para teste)
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    log.info("Testando IndicatorCalculator...")

    # Cria um DataFrame de exemplo
    data = {
        'time': pd.date_range(end=pd.Timestamp.now(), periods=100, freq='min'),
        'open': np.random.rand(100) * 10 + 100,
        'high': lambda df: df['open'] + np.random.rand(100) * 2,
        'low': lambda df: df['open'] - np.random.rand(100) * 2,
        'close': lambda df: df['open'] + np.random.rand(100) - 0.5,
    }
    # Avalia lambdas para criar colunas dependentes
    sample_df = pd.DataFrame({'time': data['time'], 'open': data['open']})
    sample_df['high'] = data['high'](sample_df)
    sample_df['low'] = data['low'](sample_df)
    sample_df['close'] = data['close'](sample_df)


    calculator = IndicatorCalculator()

    log.info("Calculando indicadores técnicos...")
    df_with_indicators = calculator.calculate_technical_indicators(sample_df.copy())
    print("\nDataFrame com Indicadores (primeiras 5 linhas):")
    print(df_with_indicators.head().to_string())
    print("\nDataFrame com Indicadores (últimas 5 linhas):")
    print(df_with_indicators.tail().to_string())

    log.info("\nCalculando variações de preço...")
    df_with_variations = calculator.calculate_price_variations(df_with_indicators.copy())
    print("\nDataFrame com Variações (últimas 5 linhas):")
    # Seleciona colunas relevantes para visualização
    variation_cols = [col for col in df_with_variations.columns if col.startswith('var_pct')]
    print(df_with_variations[['time', 'close'] + variation_cols].tail().to_string())

    log.info("\nTeste do IndicatorCalculator concluído.")