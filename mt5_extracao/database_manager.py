import os
import logging
import pandas as pd
import traceback
from sqlalchemy import create_engine, text
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

    def save_ohlcv_data(self, symbol, timeframe_name, df):
        """
        Salva ou atualiza dados OHLCV no banco de dados.

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
            return False

        # Normaliza o nome da tabela (ex: WIN$N_1_minuto -> win_n_1_minuto)
        # Remove caracteres inválidos e converte para minúsculas
        table_name = f"{symbol}_{timeframe_name}".lower()
        table_name = ''.join(c if c.isalnum() else '_' for c in table_name)
        # Remove múltiplos underscores
        table_name = '_'.join(filter(None, table_name.split('_')))

        log.info(f"Salvando {len(df)} registros para {symbol} ({timeframe_name}) na tabela '{table_name}'...")

        try:
            # Converte tipos de dados para formatos compatíveis com SQLite
            df_converted = df.copy()
            
            # Converte inteiros de 64 bits não assinados para inteiros de 32 bits 
            # (SQLite não suporta inteiros de 64 bits não assinados)
            if 'tick_volume' in df_converted:
                df_converted['tick_volume'] = df_converted['tick_volume'].astype('int32')
            if 'real_volume' in df_converted:
                df_converted['real_volume'] = df_converted['real_volume'].astype('int32')
            if 'spread' in df_converted:
                df_converted['spread'] = df_converted['spread'].astype('int32')
                
            # Usa 'replace' para inserir novos dados e sobrescrever existentes (baseado no índice 'time')
            # Define 'time' como índice para a operação de replace funcionar corretamente
            df_to_save = df_converted.set_index('time')

            # Salva no banco de dados
            # if_exists='append' é mais seguro para dados de série temporal para não apagar tudo
            # Mas 'replace' pode ser útil se quisermos garantir que apenas os dados mais recentes existam
            # Vamos usar 'append' e garantir a unicidade com um índice ou chave primária depois
            # TODO: Adicionar tratamento de chave primária (time, symbol) para evitar duplicatas com 'append'
            df_to_save.to_sql(table_name, self.engine, if_exists='append', index=True)

            log.info(f"Dados para {symbol} ({timeframe_name}) salvos com sucesso em '{table_name}'.")
            return True
        except SQLAlchemyError as e:
            log.error(f"Erro SQLAlchemy ao salvar dados para {symbol} em '{table_name}': {e}")
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