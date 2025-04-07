import logging
import pandas as pd
import numpy as np
import traceback
from typing import Dict, List, Union, Optional, Tuple
import datetime
import pytz

# Configuração de logging
log = logging.getLogger(__name__)
if not log.handlers:
    log.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    log.addHandler(ch)

class MarketDataAnalyzer:
    """
    Analisa dados de mercado para identificar informações contextuais,
    eventos econômicos, volatilidade e outros fatores que afetam o mercado.
    
    Esta classe complementa os indicadores técnicos, proporcionando uma
    visão mais ampla do contexto do mercado para análise de dados.
    """
    
    def __init__(self, timezone: str = 'America/Sao_Paulo'):
        """
        Inicializa o analisador de dados de mercado.
        
        Args:
            timezone: Fuso horário para análise de sessões de mercado
        """
        self.timezone = pytz.timezone(timezone)
        log.info(f"MarketDataAnalyzer inicializado com timezone {timezone}")
        self._initialize_economic_calendars()
        
    def _initialize_economic_calendars(self):
        """Inicializa os calendários econômicos internos."""
        # Eventos econômicos importantes - um exemplo simplificado
        # Em produção isto deveria vir de uma API ou ser alimentado externamente
        self.economic_events = {
            # Data (Y-m-d): [Lista de eventos]
            '2025-04-01': [
                {'time': '10:00', 'event': 'PIB Brasil', 'importance': 'high'},
                {'time': '15:30', 'event': 'Decisão COPOM', 'importance': 'high'}
            ],
            '2025-04-06': [
                {'time': '09:30', 'event': 'Inflação IPCA', 'importance': 'medium'}
            ]
        }
        
        # Datas de vencimento de contratos futuros
        self.futures_expirations = {
            'WIN': ['2025-04-15', '2025-06-17', '2025-08-19'],
            'DOL': ['2025-04-01', '2025-05-01', '2025-06-03'],
            'IND': ['2025-04-15', '2025-06-17', '2025-08-19']
        }
    
    def identify_market_session(self, timestamp: pd.Timestamp) -> str:
        """
        Identifica a sessão de mercado com base no horário.
        
        Args:
            timestamp: O timestamp a ser analisado
            
        Returns:
            String com o nome da sessão de mercado
        """
        # Converte para o timezone local
        if timestamp.tzinfo is None:
            local_time = self.timezone.localize(timestamp)
        else:
            local_time = timestamp.astimezone(self.timezone)
            
        # Extrai hora e dia da semana
        hour = local_time.hour
        minute = local_time.minute
        weekday = local_time.weekday()  # 0=Segunda, 6=Domingo
        
        # Mercado brasileiro (B3)
        if weekday >= 0 and weekday <= 4:  # Segunda a Sexta
            if hour == 9 and minute < 30:
                return "pre_market_b3"
            elif (hour == 9 and minute >= 30) or (hour > 9 and hour < 16):
                return "regular_b3"
            elif hour == 16 and minute < 30:
                return "after_market_b3"
            elif hour >= 17 or hour < 9:
                return "closed_b3"
        else:
            return "weekend_closed"
            
        return "unknown"
    
    def days_to_expiration(self, symbol: str, timestamp: pd.Timestamp) -> Optional[int]:
        """
        Calcula dias até expiração para contratos futuros.
        
        Args:
            symbol: Símbolo do contrato
            timestamp: Data de referência
            
        Returns:
            Número de dias até a expiração ou None se não for encontrado
        """
        # Extrai o símbolo base (remover sufixos como mês/ano)
        base_symbol = symbol.split('$')[0] if '$' in symbol else symbol
        base_symbol = base_symbol.split('@')[0] if '@' in symbol else base_symbol
        
        # Procura datas de expiração para o símbolo
        if base_symbol in self.futures_expirations:
            # Encontra a próxima expiração
            reference_date = timestamp.date()
            for expiration_str in sorted(self.futures_expirations[base_symbol]):
                expiration_date = datetime.datetime.strptime(expiration_str, '%Y-%m-%d').date()
                if expiration_date >= reference_date:
                    return (expiration_date - reference_date).days
        
        return None
    
    def find_economic_events(self, timestamp: pd.Timestamp, 
                           window_hours: int = 24) -> List[Dict]:
        """
        Encontra eventos econômicos próximos ao timestamp.
        
        Args:
            timestamp: Data/hora de referência
            window_hours: Janela de horas para procurar eventos (antes e depois)
            
        Returns:
            Lista de eventos econômicos no período
        """
        # Normaliza para o fuso horário correto
        if timestamp.tzinfo is None:
            ts = self.timezone.localize(timestamp)
        else:
            ts = timestamp.astimezone(self.timezone)
            
        date_str = ts.strftime('%Y-%m-%d')
        
        # Eventos do dia
        events = []
        if date_str in self.economic_events:
            events.extend(self.economic_events[date_str])
            
        # Se a janela é maior que 24h, verificar dias adjacentes
        if window_hours > 24:
            days_to_check = window_hours // 24 + 1
            for i in range(1, days_to_check):
                # Dia anterior
                prev_date = (ts - datetime.timedelta(days=i)).strftime('%Y-%m-%d')
                if prev_date in self.economic_events:
                    events.extend(self.economic_events[prev_date])
                    
                # Dia seguinte
                next_date = (ts + datetime.timedelta(days=i)).strftime('%Y-%m-%d')
                if next_date in self.economic_events:
                    events.extend(self.economic_events[next_date])
                    
        return events
    
    def calculate_volatility_regimes(self, df: pd.DataFrame, 
                                   lookback_periods: List[int] = [5, 20, 50]) -> Dict[str, pd.Series]:
        """
        Calcula e identifica regimes de volatilidade em diferentes períodos.
        
        Args:
            df: DataFrame com dados OHLCV
            lookback_periods: Lista de períodos para análise
            
        Returns:
            Dicionário com séries de volatilidade
        """
        if 'high' not in df.columns or 'low' not in df.columns:
            log.warning("Dados de high e low necessários para cálculo de volatilidade")
            return {}
            
        result = {}
            
        try:
            # Volatilidade diária (High-Low Range normalizado pelo Close)
            daily_volatility = (df['high'] - df['low']) / df['close']
            
            # Calcula volatilidade para diferentes períodos
            for period in lookback_periods:
                # Média móvel da volatilidade
                vol_ma = daily_volatility.rolling(window=period).mean()
                result[f'volatility_{period}'] = vol_ma
                
                # Desvio padrão da volatilidade
                vol_std = daily_volatility.rolling(window=period).std()
                result[f'volatility_std_{period}'] = vol_std
                
                # Classificação do regime (3 níveis - baixo, médio, alto)
                # Utilizando quantis para classificação 
                vol_quantile = vol_ma.rolling(window=period*3).quantile(0.70)
                vol_quantile_low = vol_ma.rolling(window=period*3).quantile(0.30)
                
                regime = pd.Series(1, index=df.index)  # 1 = médio (padrão)
                regime.loc[vol_ma > vol_quantile] = 2  # 2 = alta volatilidade 
                regime.loc[vol_ma < vol_quantile_low] = 0  # 0 = baixa volatilidade
                
                result[f'volatility_regime_{period}'] = regime
            
            return result
            
        except Exception as e:
            log.error(f"Erro ao calcular regimes de volatilidade: {str(e)}")
            log.debug(traceback.format_exc())
            return {}
            
    def detect_market_hours_volatility(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """
        Detecta períodos de alta/baixa volatilidade por hora do dia.
        
        Args:
            df: DataFrame com dados OHLCV incluindo coluna 'time'
            
        Returns:
            Dicionário com séries de volatilidade por hora
        """
        if 'time' not in df.columns:
            log.warning("Coluna 'time' necessária para análise horária de volatilidade")
            return {}
            
        if 'high' not in df.columns or 'low' not in df.columns:
            log.warning("Dados de high e low necessários para cálculo de volatilidade")
            return {}
            
        try:
            # Extrai hora do dia
            hours = df['time'].dt.hour
            
            # Volatilidade por candle
            candle_vol = (df['high'] - df['low']) / df['close']
            
            # Agrupa volatilidade por hora
            hour_volatility = {}
            
            for hour in range(24):
                mask = hours == hour
                if any(mask):
                    hour_volatility[hour] = candle_vol[mask].mean()
            
            # Identifica horas de alta/baixa volatilidade
            if hour_volatility:
                mean_vol = np.mean(list(hour_volatility.values()))
                std_vol = np.std(list(hour_volatility.values()))
                
                high_vol_hours = [h for h, v in hour_volatility.items() if v > mean_vol + 0.5*std_vol]
                low_vol_hours = [h for h, v in hour_volatility.items() if v < mean_vol - 0.5*std_vol]
                
                # Cria séries para indicar alta/baixa volatilidade
                high_vol_flag = pd.Series(False, index=df.index)
                high_vol_flag.loc[hours.isin(high_vol_hours)] = True
                
                low_vol_flag = pd.Series(False, index=df.index)
                low_vol_flag.loc[hours.isin(low_vol_hours)] = True
                
                return {
                    'high_volatility_hour': high_vol_flag,
                    'low_volatility_hour': low_vol_flag
                }
            
            return {}
            
        except Exception as e:
            log.error(f"Erro ao detectar volatilidade por hora: {str(e)}")
            log.debug(traceback.format_exc())
            return {}
    
    def analyze_market_data(self, df: pd.DataFrame, symbol: str = None) -> pd.DataFrame:
        """
        Realiza uma análise completa dos dados de mercado.
        
        Args:
            df: DataFrame com dados OHLCV
            symbol: Símbolo do ativo (opcional)
            
        Returns:
            DataFrame com análises adicionadas
        """
        if df is None or df.empty:
            log.warning("DataFrame vazio recebido para análise de mercado.")
            return df
            
        required_columns = ['time', 'close']
        if not all(col in df.columns for col in required_columns):
            log.error("DataFrame não contém colunas necessárias para análise de mercado.")
            missing = [col for col in required_columns if col not in df.columns]
            log.error(f"Colunas ausentes: {missing}")
            return df
            
        try:
            # Cria cópia para não modificar o original
            result_df = df.copy()
            
            # 1. Identifica sessão de mercado
            result_df['market_session'] = result_df['time'].apply(self.identify_market_session)
            
            # 2. Dias até expiração (para futuros)
            if symbol is not None:
                result_df['days_to_expiration'] = result_df['time'].apply(
                    lambda x: self.days_to_expiration(symbol, x))
                
                # Flag para último dia de expiração
                result_df['last_trading_day'] = result_df['days_to_expiration'] == 0
            
            # 3. Eventos econômicos
            result_df['has_economic_event'] = False
            for i, row in result_df.iterrows():
                events = self.find_economic_events(row['time'], window_hours=24)
                if events:
                    result_df.at[i, 'has_economic_event'] = True
            
            # 4. Análise de volatilidade
            if 'high' in df.columns and 'low' in df.columns:
                # Regimes de volatilidade
                vol_regimes = self.calculate_volatility_regimes(result_df)
                for key, value in vol_regimes.items():
                    result_df[key] = value
                
                # Volatilidade por hora
                hour_vol = self.detect_market_hours_volatility(result_df)
                for key, value in hour_vol.items():
                    result_df[key] = value
            
            # 5. Características temporais
            result_df['day_of_week'] = result_df['time'].dt.dayofweek
            result_df['hour_of_day'] = result_df['time'].dt.hour
            result_df['is_end_of_month'] = result_df['time'].dt.is_month_end
            result_df['is_end_of_week'] = result_df['time'].dt.dayofweek == 4  # Sexta-feira
            
            return result_df
            
        except Exception as e:
            log.error(f"Erro ao analisar dados de mercado: {str(e)}")
            log.debug(traceback.format_exc())
            return df
            
    def update_economic_events(self, events_dict: Dict[str, List[Dict]]):
        """
        Atualiza o calendário de eventos econômicos.
        
        Args:
            events_dict: Dicionário de eventos no formato {data: [evento1, evento2, ...]}
        """
        self.economic_events.update(events_dict)
        log.info(f"Calendário de eventos econômicos atualizado. Total: {len(self.economic_events)} dias")
        
    def update_futures_expirations(self, expirations_dict: Dict[str, List[str]]):
        """
        Atualiza o calendário de vencimentos de contratos futuros.
        
        Args:
            expirations_dict: Dicionário no formato {símbolo: [data1, data2, ...]}
        """
        self.futures_expirations.update(expirations_dict)
        log.info(f"Calendário de vencimentos atualizado. Símbolos: {list(self.futures_expirations.keys())}")

# Teste unitário
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    log.info("Testando MarketDataAnalyzer")
    
    # Cria dados de teste
    dates = pd.date_range(
        start='2025-04-01 09:00:00', 
        end='2025-04-06 17:00:00', 
        freq='1H'
    )
    
    data = {
        'time': dates,
        'open': np.random.normal(100, 1, len(dates)),
        'high': None,
        'low': None,
        'close': None,
        'volume': np.random.randint(100, 10000, len(dates))
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
    
    # Inicializa o analisador e processa o DataFrame
    analyzer = MarketDataAnalyzer()
    result = analyzer.analyze_market_data(df, symbol='WIN$N')
    
    log.info(f"DataFrame original: {df.shape}, com análises de mercado: {result.shape}")
    log.info(f"Novas colunas: {set(result.columns) - set(df.columns)}")
    
    # Verifica quantidade de sessões de mercado
    sessions = result['market_session'].value_counts()
    log.info(f"Sessões de mercado detectadas: {sessions}")
    
    log.info("Teste concluído com sucesso") 