import os
import logging
import pandas as pd
import numpy as np # <--- ADICIONADO IMPORT
import traceback
from sqlalchemy import create_engine, text, inspect # Adicionado inspect
from sqlalchemy.exc import SQLAlchemyError

from mt5_extracao.error_handler import with_error_handling, DatabaseError, DataTypeError

# Garantir que o diretório de logs existe
os.makedirs("logs", exist_ok=True)

# Configuração de logging (pode ser centralizada depois)
log = logging.getLogger(__name__)
if not log.handlers:
    log.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    # Adicionar um handler de console para depuração inicial
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    log.addHandler(ch)
    # Adicionar um handler de arquivo
    fh = logging.FileHandler("logs/database_manager.log", encoding="utf-8")
    fh.setFormatter(formatter)
    log.addHandler(fh)

class DatabaseManager:
    """
    Gerencia a conexão e as operações com o banco de dados.
    """
    def __init__(self, db_type='sqlite', db_path='database/mt5_data.db'):
        """
        Inicializa o gerenciador do banco de dados.

        Args:
            db_type (str): Tipo do banco ('sqlite', futuramente 'postgresql', etc.).
            db_path (str): Caminho para o arquivo do banco SQLite ou string de conexão.
        """
        self.db_type = db_type
        self.db_path = db_path
        self.engine = None
        self._connect()

    def _connect(self):
        """Estabelece a conexão com o banco de dados."""
        log.info(f"Configurando conexão com banco de dados: Tipo={self.db_type}, Path={self.db_path}")
        try:
            if self.db_type == 'sqlite':
                # Garante que o diretório do banco existe
                db_dir = os.path.dirname(self.db_path)
                if db_dir and not os.path.exists(db_dir):
                    os.makedirs(db_dir)
                    log.info(f"Diretório do banco de dados criado: {db_dir}")
                connection_string = f'sqlite:///{self.db_path}'
                self.engine = create_engine(connection_string)
                # Testa a conexão inicial
                with self.engine.connect() as connection:
                    log.info(f"Conexão com banco de dados SQLite estabelecida: {self.db_path}")
            # TODO: Adicionar suporte para outros tipos de banco (PostgreSQL/TimescaleDB)
            # elif self.db_type == 'postgresql':
            #     # connection_string = f'postgresql://user:password@host:port/database'
            #     # self.engine = create_engine(connection_string)
            #     log.warning("Suporte a PostgreSQL ainda não implementado.")
            #     self.engine = None
            else:
                log.error(f"Tipo de banco de dados não suportado: {self.db_type}")
                self.engine = None
        except SQLAlchemyError as e:
            log.error(f"Erro ao conectar ao banco de dados ({self.db_type}): {e}")
            log.debug(traceback.format_exc())
            self.engine = None
        except Exception as e:
            log.error(f"Erro inesperado ao configurar banco de dados: {e}")
            log.debug(traceback.format_exc())
            self.engine = None

    def is_connected(self):
        """Verifica se a conexão com o banco de dados está ativa."""
        return self.engine is not None

    # Mapeamento de nomes de colunas para tipos SQL (simplificado)
    # Usado para criar a tabela com todas as colunas possíveis de uma vez
    _FULL_COLUMN_DEFINITIONS = {
        "time": "TIMESTAMP PRIMARY KEY",
        "open": "REAL",
        "high": "REAL",
        "low": "REAL",
        "close": "REAL",
        "tick_volume": "INTEGER",
        "spread": "INTEGER",
        "real_volume": "INTEGER",
        "rsi": "REAL",
        "macd_line": "REAL",
        "macd_signal": "REAL",
        "macd_histogram": "REAL",
        "ma_20": "REAL", # Exemplo, pode haver outras MAs
        "bollinger_upper": "REAL",
        "bollinger_lower": "REAL",
        "atr": "REAL",
        "true_range": "REAL",
        "var_pct_open_5min": "REAL",
        "var_pct_close_15min": "REAL",
        "var_pct_high_30min": "REAL",
        "var_pct_low_60min": "REAL",
        "var_pct_open_daily": "REAL",
        "var_pct_close_prev_day": "REAL",
        "stoch_k": "REAL",
        "stoch_d": "REAL",
        "adx": "REAL",
        "plus_di": "REAL",
        "minus_di": "REAL",
        "cci": "REAL",
        "fib_0": "REAL",
        "fib_236": "REAL",
        "fib_382": "REAL",
        "fib_500": "REAL",
        "fib_618": "REAL",
        "fib_786": "REAL",
        "fib_1000": "REAL",
        "fib_1272": "REAL",
        "fib_1618": "REAL",
        "skewness": "REAL",
        "kurtosis": "REAL",
        "volatility": "REAL",
        "pattern_doji": "INTEGER",
        "pattern_hammer": "INTEGER",
        "pattern_inverted_hammer": "INTEGER",
        "pattern_bullish_engulfing": "INTEGER",
        "pattern_bearish_engulfing": "INTEGER",
        "pattern_morning_star": "INTEGER",
        "pattern_evening_star": "INTEGER",
        "volume_obv": "REAL", # Assumindo que análise de volume adiciona estes
        "volume_cmf": "REAL",
        "volume_fi": "REAL",
        "volume_vpt": "REAL",
        "volume_nvi": "REAL",
        "volume_pvi": "REAL",
        "trend_direction": "INTEGER",
        "trend_strength": "REAL",
        "trend_duration": "INTEGER",
        "trend_slope": "REAL",
        "trend_r_squared": "REAL",
        "resistance_1": "REAL",
        "resistance_2": "REAL",
        "resistance_3": "REAL", # Adicionado R3/S3 por precaução
        "support_1": "REAL",
        "support_2": "REAL",
        "support_3": "REAL",
        "day_of_week": "INTEGER",
        "hour_of_day": "INTEGER",
        "trading_session": "INTEGER", # Assumindo INTEGER
        "intraday_volatility": "REAL",
    }

    def _create_table_if_not_exists(self, table_name):
        """Cria a tabela com o schema completo se ela não existir."""
        if not self.is_connected():
            log.error(f"Não é possível criar tabela '{table_name}': Banco de dados não conectado.")
            return False

        try:
            with self.engine.connect() as connection:
                inspector = inspect(self.engine)
                if not inspector.has_table(table_name):
                    log.info(f"Tabela '{table_name}' não encontrada. Criando com schema completo...")
                    column_defs = [f"{name} {sql_type}" for name, sql_type in self._FULL_COLUMN_DEFINITIONS.items()]
                    # Citar o nome da tabela para segurança
                    create_sql = f'CREATE TABLE "{table_name}" ({", ".join(column_defs)})'
                    connection.execute(text(create_sql))
                    # Adicionar commit explícito após DDL
                    if hasattr(connection, 'commit'):
                        connection.commit()
                    log.info(f"Tabela '{table_name}' criada com sucesso.")
                    return True
                else:
                    # log.debug(f"Tabela '{table_name}' já existe.")
                    return True # Tabela já existe, sucesso
        except SQLAlchemyError as e:
            log.error(f"Erro SQLAlchemy ao criar/verificar tabela '{table_name}': {e}")
            log.debug(traceback.format_exc())
            return False
        except Exception as e:
            log.error(f"Erro inesperado ao criar/verificar tabela '{table_name}': {e}")
            log.debug(traceback.format_exc())
            return False

    def save_ohlcv_data(self, symbol, timeframe_name, df):
        """
        Salva ou atualiza dados OHLCV no banco de dados.
        Garante que a tabela exista com o schema completo antes de salvar.

        Args:
            symbol (str): Nome do símbolo (ex: 'WIN$N').
            timeframe_name (str): Nome legível do timeframe (ex: '1 minuto').
            df (pd.DataFrame): DataFrame contendo os dados OHLCV com coluna 'time'.
        """
        if not self.is_connected():
            log.error("Não é possível salvar dados: Banco de dados não conectado.")
            return False
        if df is None or df.empty:
            log.warning(f"DataFrame vazio para {symbol} {timeframe_name}. Nada a salvar.")
            return True # Considera sucesso, pois não há erro

        table_name = self.get_table_name_for_symbol(symbol, timeframe_name)
        log.info(f"Preparando para salvar {len(df)} registros para {symbol} ({timeframe_name}) na tabela '{table_name}'...")

        # 1. Garantir que a tabela exista com o schema completo
        if not self._create_table_if_not_exists(table_name):
            log.error(f"Falha ao garantir a existência/schema da tabela '{table_name}'. Abortando salvamento.")
            return False

        # 2. Preparar DataFrame para salvar
        try:
            df_converted = df.copy()
            # Garantir que 'time' seja datetime
            if 'time' in df_converted.columns:
                 df_converted['time'] = pd.to_datetime(df_converted['time'])
            else:
                 log.error("DataFrame não contém a coluna 'time'.")
                 return False

            # Definir 'time' como índice
            df_to_save = df_converted.set_index('time')

            # Remover colunas do DataFrame que NÃO existem na definição completa
            # Isso evita erros se o cálculo de indicadores gerar colunas inesperadas
            # Usar os nomes das chaves diretamente, pois removemos as aspas
            valid_columns = [col for col in self._FULL_COLUMN_DEFINITIONS.keys() if col != 'time'] # Usar nomes diretamente
            cols_to_drop = [col for col in df_to_save.columns if col not in valid_columns]
            if cols_to_drop:
                log.warning(f"Removendo colunas não definidas no schema de '{table_name}': {cols_to_drop}")
                df_to_save = df_to_save.drop(columns=cols_to_drop)

            # Garantir que colunas FALTANTES no DF (mas presentes no schema) sejam adicionadas com NaN
            # Isso evita erro no INSERT se um indicador não foi calculado
            for col_name in self._FULL_COLUMN_DEFINITIONS.keys(): # Iterar diretamente sobre as chaves
                 if col_name != 'time' and col_name not in df_to_save.columns:
                      log.debug(f"Adicionando coluna faltante '{col_name}' com NaN ao DataFrame antes de salvar em '{table_name}'.")
                      df_to_save[col_name] = np.nan # Adiciona com NaN

            # Reordenar colunas do DataFrame para coincidir com a ordem da definição (boa prática)
            ordered_columns = [col for col in self._FULL_COLUMN_DEFINITIONS.keys() if col != 'time']
            df_to_save = df_to_save[ordered_columns]

        except Exception as e:
            log.error(f"Erro ao preparar DataFrame para salvar em '{table_name}': {e}")
            log.debug(traceback.format_exc())
            return False

        # 3. Salvar no banco de dados
        try:
            log.debug(f"Tentando salvar dados na tabela '{table_name}'...")
            # Usar 'append'. A chave primária 'time' deve lidar com duplicatas se o SQLite estiver configurado corretamente
            # ou se a lógica de 'overwrite' for usada antes desta chamada.
            # Deixar pandas/SQLAlchemy lidar com a citação do nome da tabela
            df_to_save.to_sql(table_name, self.engine, if_exists='append', index=True, index_label='time')

            log.info(f"Dados para {symbol} ({timeframe_name}) salvos com sucesso em '{table_name}'.")
            return True
        except SQLAlchemyError as e:
            # Log detalhado do erro SQL
            log.error(f"Erro SQLAlchemy ao salvar dados para {symbol} em '{table_name}': {e}")
            if hasattr(e, 'params'): log.error(f"Parâmetros: {e.params}")
            if hasattr(e, 'statement'): log.error(f"Statement: {e.statement}")
            log.debug(traceback.format_exc())
            return False
        except Exception as e:
            log.error(f"Erro inesperado ao salvar dados para {symbol} em '{table_name}': {e}")
            log.debug(traceback.format_exc())
            return False

    def get_existing_symbols(self):
        """
        Retorna uma lista de símbolos que já possuem dados no banco.
        
        Returns:
            list: Lista de símbolos normalizados (ex: ["win_n_1_minuto", "petr4_diario"]) ou lista vazia
        """
        if not self.is_connected():
            log.error("Não é possível obter símbolos: Banco de dados não conectado.")
            return []
        
        try:
            # Consulta todas as tabelas no banco de dados SQLite
            with self.engine.connect() as conn:
                if self.db_type == 'sqlite':
                    # Para SQLite
                    result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'"))
                    tables = [row[0] for row in result]
                # Adicionar suporte para PostgreSQL depois
                # elif self.db_type == 'postgresql':
                #     result = conn.execute(text("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'"))
                #     tables = [row[0] for row in result]
                else:
                    tables = []
            
            # Filtra apenas tabelas que seguem o padrão de nome de símbolos
            # Assume um padrão como symbol_timeframe (ex: win_n_1_minuto)
            log.info(f"Encontradas {len(tables)} tabelas no banco de dados.")
            return tables
            
        except SQLAlchemyError as e:
            log.error(f"Erro ao consultar tabelas: {e}")
            return []
        except Exception as e:
            log.error(f"Erro inesperado ao listar tabelas: {e}")
            return []
    
    def get_symbol_data_summary(self, table_name):
        """
        Retorna um resumo dos dados existentes para um símbolo/timeframe específico.
        
        Args:
            table_name (str): Nome da tabela normalizado (ex: 'win_n_1_minuto')
            
        Returns:
            dict: Resumo dos dados (data_inicio, data_fim, total_registros, ultimo_update) ou None se erro
        """
        if not self.is_connected():
            log.error(f"Não é possível obter resumo de {table_name}: Banco de dados não conectado.")
            return None
            
        try:
            with self.engine.connect() as conn:
                # Verifica se a tabela existe
                if self.db_type == 'sqlite':
                    result = conn.execute(text(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"))
                    if not result.scalar():
                        log.warning(f"Tabela {table_name} não encontrada no banco de dados.")
                        return None
                
                # Informações básicas (contagem, data mais antiga, data mais recente)
                count_query = f"SELECT COUNT(*) FROM {table_name}"
                result = conn.execute(text(count_query))
                total_registros = result.scalar() or 0
                
                if total_registros == 0:
                    log.info(f"Tabela {table_name} existe mas está vazia.")
                    return {"total_registros": 0}
                    
                # Data mais antiga
                min_date_query = f"SELECT MIN(time) FROM {table_name}"
                result = conn.execute(text(min_date_query))
                data_inicio = result.scalar()
                
                # Data mais recente
                max_date_query = f"SELECT MAX(time) FROM {table_name}"
                result = conn.execute(text(max_date_query))
                data_fim = result.scalar()
                
                # Verificar intervalo de tempo (média de tempo entre registros)
                if total_registros > 1:
                    # Calcula média de tempo entre registros (para detectar timeframe)
                    interval_query = f"""
                    SELECT 
                        (JULIANDAY(MAX(time)) - JULIANDAY(MIN(time))) * 24 * 60 / (COUNT(*) - 1) as avg_minutes
                    FROM {table_name}
                    """
                    result = conn.execute(text(interval_query))
                    intervalo_medio_minutos = result.scalar() or 0
                else:
                    intervalo_medio_minutos = 0
                
                # Formatação das datas para string
                data_inicio_str = data_inicio.strftime('%Y-%m-%d %H:%M:%S') if data_inicio else 'N/A'
                data_fim_str = data_fim.strftime('%Y-%m-%d %H:%M:%S') if data_fim else 'N/A'
                
                # Verifica a completude dos dados
                if total_registros > 10 and data_inicio and data_fim and intervalo_medio_minutos > 0:
                    # Calcula o número esperado de barras se os dados forem contínuos
                    periodo_total_minutos = ((data_fim - data_inicio).total_seconds() / 60)
                    barras_esperadas = periodo_total_minutos / intervalo_medio_minutos
                    completude = min(100, (total_registros / barras_esperadas) * 100 if barras_esperadas > 0 else 0)
                else:
                    completude = 0
                
                return {
                    "total_registros": total_registros,
                    "data_inicio": data_inicio_str,
                    "data_fim": data_fim_str, 
                    "intervalo_medio_minutos": round(intervalo_medio_minutos, 1),
                    "completude": round(completude, 1)
                }
                
        except SQLAlchemyError as e:
            log.error(f"Erro SQL ao obter resumo de {table_name}: {e}")
            return None
        except Exception as e:
            log.error(f"Erro inesperado ao obter resumo de {table_name}: {e}")
            return None
    
    def get_table_name_for_symbol(self, symbol, timeframe_name):
        """
        Retorna o nome normalizado da tabela para um símbolo e timeframe.
        
        Args:
            symbol (str): Nome do símbolo (ex: 'WIN$N')
            timeframe_name (str): Nome do timeframe (ex: '1 minuto')
            
        Returns:
            str: Nome normalizado da tabela
        """
        # Normaliza o nome da tabela (ex: WIN$N_1_minuto -> win_n_1_minuto)
        table_name = f"{symbol}_{timeframe_name}".lower()
        table_name = ''.join(c if c.isalnum() else '_' for c in table_name)
        # Remove múltiplos underscores
        table_name = '_'.join(filter(None, table_name.split('_')))
        return table_name
    
    def optimize_database(self):
        """
        Executa otimizações no banco de dados para melhorar a performance.
        
        Returns:
            bool: True se as otimizações foram aplicadas com sucesso, False caso contrário
        """
        if not self.is_connected():
            log.error("Não é possível otimizar: Banco de dados não conectado.")
            return False
            
        try:
            with self.engine.connect() as conn:
                # Executa VACUUM para otimizar espaço (apenas SQLite)
                if self.db_type == 'sqlite':
                    log.info("Iniciando otimização de banco de dados (VACUUM)...")
                    conn.execute(text("VACUUM"))
                    log.info("Otimização VACUUM concluída.")
                    
                    # Cria índices para melhorar a performance de consultas por data
                    tables = self.get_existing_symbols()
                    indexed_tables = 0
                    
                    for table in tables:
                        try:
                            # Verifica se o índice já existe
                            index_check = conn.execute(text(f"SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='{table}' AND name='idx_{table}_time'"))
                            if not index_check.scalar():
                                # Cria índice de tempo para consultas mais rápidas
                                log.info(f"Criando índice de tempo para a tabela {table}...")
                                conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{table}_time ON {table} (time)"))
                                indexed_tables += 1
                        except Exception as idx_err:
                            log.warning(f"Erro ao criar índice para {table}: {idx_err}")
                    
                    log.info(f"Criados índices para {indexed_tables} tabelas.")
                    return True
                    
                # Lógica para PostgreSQL pode ser adicionada no futuro
                return True
                
        except SQLAlchemyError as e:
            log.error(f"Erro SQL ao otimizar banco de dados: {e}")
            return False
        except Exception as e:
            log.error(f"Erro inesperado ao otimizar banco de dados: {e}")
            return False

    @with_error_handling(error_type=DatabaseError)
    def execute_query(self, query, params=None):
        """
        Executa uma consulta SQL e retorna os resultados como DataFrame.
        
        Args:
            query (str): Consulta SQL a ser executada
            params (dict, optional): Parâmetros para a consulta
            
        Returns:
            pandas.DataFrame: Resultados da consulta
            
        Raises:
            DatabaseError: Se ocorrer um erro na execução da consulta
        """
        if not self.is_connected():
            self._connect()
            if not self.is_connected():
                raise DatabaseError("Falha ao conectar ao banco de dados")
        
        log.info(f"Executando consulta: {query}")
        
        try:
            # Executar a consulta usando pandas
            df = pd.read_sql_query(query, self.engine, params=params)
            log.info(f"Consulta retornou {len(df)} registros")
            return df
        except SQLAlchemyError as e:
            log.error(f"Erro SQLAlchemy ao executar consulta: {e}")
            raise DatabaseError(f"Erro na consulta SQL: {str(e)}", query=query)
        except Exception as e:
            log.error(f"Erro inesperado ao executar consulta: {e}")
            log.error(traceback.format_exc())
            raise DatabaseError(f"Erro ao executar consulta: {str(e)}", query=query)
            
    @with_error_handling(error_type=DatabaseError)
    def get_all_tables(self):
        """
        Retorna uma lista com todos os nomes de tabelas no banco de dados.
        
        Returns:
            list: Lista de nomes de tabelas
        """
        if not self.is_connected():
            self._connect()
            if not self.is_connected():
                return []
                
        try:
            # Para SQLite
            if self.db_type == 'sqlite':
                query = "SELECT name FROM sqlite_master WHERE type='table'"
                result = pd.read_sql_query(query, self.engine)
                tables = result['name'].tolist()
                log.info(f"Encontradas {len(tables)} tabelas no banco de dados.")
                return tables
                
            # Para outros bancos (PostgreSQL, MySQL)
            # Implementar conforme necessário
                
            return []
        except Exception as e:
            log.error(f"Erro ao listar tabelas: {e}")
            return []
            
    @with_error_handling(error_type=DatabaseError)
    def get_table_summary(self, table_name):
        """
        Retorna um resumo dos dados de uma tabela (data mais antiga, mais recente, quantidade).
        
        Args:
            table_name (str): Nome da tabela
            
        Returns:
            dict: Resumo da tabela com data inicial, final e contagem
        """
        if not self.is_connected():
            self._connect()
            if not self.is_connected():
                return None
                
        try:
            # Assumindo que todas as tabelas têm um campo de data/timestamp na primeira coluna
            query = f"""
            SELECT 
                MIN(time) as data_inicial,
                MAX(time) as data_final,
                COUNT(*) as total_registros
            FROM {table_name}
            """
            
            df = pd.read_sql_query(query, self.engine)
            if df.empty:
                return {
                    "data_inicial": None,
                    "data_final": None,
                    "total_registros": 0
                }
                
            # Converter para dicionário
            summary = df.iloc[0].to_dict()
            
            # Converter timestamps para datetime se forem strings
            for key in ["data_inicial", "data_final"]:
                if isinstance(summary[key], str):
                    try:
                        summary[key] = pd.to_datetime(summary[key])
                    except:
                        # Se falhar, manter como string
                        pass
                        
            log.info(f"Resumo obtido para {table_name}: {summary}")
            return summary
            
        except Exception as e:
            log.error(f"Erro inesperado ao obter resumo de {table_name}: {e}")
            return None

    @with_error_handling(error_type=DatabaseError)
    def get_recent_data(self, table_name, limit=100):
        """
        Obtém os dados mais recentes de uma tabela específica.
        
        Args:
            table_name (str): Nome da tabela
            limit (int): Número máximo de registros a serem retornados
            
        Returns:
            pandas.DataFrame: DataFrame com os dados mais recentes ou None em caso de erro
        """
        if not self.is_connected():
            log.error(f"Não é possível obter dados recentes de {table_name}: Banco de dados não conectado.")
            return None
            
        try:
            # Verifica se a tabela existe
            if self.db_type == 'sqlite':
                with self.engine.connect() as conn:
                    result = conn.execute(text(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"))
                    if not result.scalar():
                        log.warning(f"Tabela {table_name} não encontrada no banco de dados.")
                        return None
            
            # Consulta os dados mais recentes
            query = f"""
            SELECT * FROM {table_name}
            ORDER BY time DESC
            LIMIT {limit}
            """
            
            df = pd.read_sql_query(query, self.engine)
            
            if df.empty:
                log.warning(f"Nenhum dado encontrado na tabela {table_name}.")
                return None
                
            # Reordena o DataFrame cronologicamente (do mais antigo para o mais recente)
            df = df.sort_values(by='time').reset_index(drop=True)
            
            log.info(f"Obtidos {len(df)} registros recentes da tabela {table_name}.")
            return df
            
        except SQLAlchemyError as e:
            log.error(f"Erro SQL ao obter dados recentes de {table_name}: {e}")
            return None
        except Exception as e:
            log.error(f"Erro inesperado ao obter dados recentes de {table_name}: {e}")
            log.debug(traceback.format_exc())
            return None
            
    @with_error_handling(error_type=DatabaseError)
    def save_data(self, df, table_name, symbol=None):
        """
        Salva os dados no banco de dados.
        
        Args:
            df (pandas.DataFrame): DataFrame contendo os dados a serem salvos
            table_name (str): Nome da tabela
            symbol (str, optional): Símbolo para fins de log
            
        Returns:
            bool: True se os dados foram salvos com sucesso, False caso contrário
        """
        if not self.is_connected():
            log.error(f"Não é possível salvar dados em {table_name}: Banco de dados não conectado.")
            return False
            
        if df is None or df.empty:
            symbol_info = f" para {symbol}" if symbol else ""
            log.warning(f"DataFrame vazio{symbol_info}. Nada a salvar em {table_name}.")
            return False
            
        try:
            # Converte tipos de dados para formatos compatíveis com SQLite
            df_to_save = df.copy()
            
            # Verifica se 'time' está no DataFrame e converte para tipo correto
            if 'time' in df_to_save.columns:
                # Garante que 'time' é datetime
                df_to_save['time'] = pd.to_datetime(df_to_save['time'])
                
                # Define 'time' como índice para a operação funcionar corretamente
                df_to_save = df_to_save.set_index('time')
            
            # Converte inteiros grandes para evitar erros de SQLite
            for col in df_to_save.select_dtypes(include=['int64', 'uint64']).columns:
                df_to_save[col] = df_to_save[col].astype('int32')
            
            # Salva no banco de dados
            df_to_save.to_sql(table_name, self.engine, if_exists='append', index=True)
            
            symbol_info = f" para {symbol}" if symbol else ""
            log.info(f"Dados{symbol_info} salvos com sucesso em {table_name} ({len(df)} registros).")
            return True
            
        except SQLAlchemyError as e:
            symbol_info = f" para {symbol}" if symbol else ""
            log.error(f"Erro SQL ao salvar dados{symbol_info} em {table_name}: {e}")
            return False
        except Exception as e:
            symbol_info = f" para {symbol}" if symbol else ""
            log.error(f"Erro inesperado ao salvar dados{symbol_info} em {table_name}: {e}")
            log.debug(traceback.format_exc())
            return False


    @with_error_handling(error_type=DatabaseError)
    def delete_data_periodo(self, table_name, start_date, end_date):
        """
        Deleta dados de uma tabela dentro de um período específico.

        Args:
            table_name (str): Nome da tabela normalizado.
            start_date (datetime): Data inicial do período a ser deletado.
            end_date (datetime): Data final do período a ser deletado.

        Returns:
            bool: True se a deleção foi bem-sucedida (ou nenhum dado existia), False caso contrário.
        """
        if not self.is_connected():
            log.error(f"Não é possível deletar dados de {table_name}: Banco de dados não conectado.")
            return False

        log.info(f"Deletando dados da tabela '{table_name}' entre {start_date} e {end_date}...")

        try:
            with self.engine.connect() as connection:
                # Usar transação para garantir atomicidade
                with connection.begin():
                    # Construir a query DELETE com parâmetros seguros
                    query = text(f"DELETE FROM {table_name} WHERE time >= :start AND time <= :end")
                    result = connection.execute(query, {"start": start_date, "end": end_date})
                    log.info(f"{result.rowcount} registros deletados da tabela '{table_name}'.")
            return True
        except SQLAlchemyError as e:
            log.error(f"Erro SQLAlchemy ao deletar dados de '{table_name}': {e}")
            log.debug(traceback.format_exc())
            # Levanta a exceção para ser capturada pelo decorator @with_error_handling
            raise DatabaseError(f"Erro SQL ao deletar dados de '{table_name}': {str(e)}", query=str(query))
        except Exception as e:
            log.error(f"Erro inesperado ao deletar dados de '{table_name}': {e}")
            log.debug(traceback.format_exc())
            raise DatabaseError(f"Erro inesperado ao deletar dados de '{table_name}': {str(e)}")

# Exemplo de uso (para teste)
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    log.info("Testando DatabaseManager...")

    # Cria um DB de teste
    test_db_path = "database/test_mt5_data.db"
    if os.path.exists(test_db_path):
        os.remove(test_db_path)

    db_manager = DatabaseManager(db_path=test_db_path)

    if db_manager.is_connected():
        log.info("Conexão de teste bem-sucedida.")

        # Cria um DataFrame de exemplo
        data = {
            'time': pd.to_datetime(['2024-01-01 10:00:00', '2024-01-01 10:01:00']),
            'open': [100, 101],
            'high': [102, 101.5],
            'low': [99, 100.5],
            'close': [101, 100.8],
            'tick_volume': [1000, 1200],
            'spread': [5, 6],
            'real_volume': [500, 600]
        }
        sample_df = pd.DataFrame(data)

        log.info("Tentando salvar dados de exemplo...")
        success = db_manager.save_ohlcv_data("TEST$X", "1 minuto", sample_df)
        if success:
            log.info("Dados de exemplo salvos com sucesso.")
            # Verifica se a tabela foi criada (SQLite)
            try:
                with db_manager.engine.connect() as conn:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM test_x_1_minuto")).scalar()
                    log.info(f"Verificação: Tabela 'test_x_1_minuto' contém {result} registros.")
            except Exception as e:
                log.error(f"Erro ao verificar tabela: {e}")
        else:
            log.error("Falha ao salvar dados de exemplo.")

        # Tenta salvar novamente (teste append)
        log.info("Tentando salvar os mesmos dados novamente (teste append)...")
        success_append = db_manager.save_ohlcv_data("TEST$X", "1 minuto", sample_df)
        if success_append:
             try:
                with db_manager.engine.connect() as conn:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM test_x_1_minuto")).scalar()
                    log.info(f"Verificação pós-append: Tabela 'test_x_1_minuto' contém {result} registros.") # Deve ser 4
             except Exception as e:
                log.error(f"Erro ao verificar tabela pós-append: {e}")

    else:
        log.error("Falha ao conectar ao banco de dados de teste.")

    log.info("Teste do DatabaseManager concluído.")
    # Limpa o banco de teste
    # if os.path.exists(test_db_path):
    #     os.remove(test_db_path)