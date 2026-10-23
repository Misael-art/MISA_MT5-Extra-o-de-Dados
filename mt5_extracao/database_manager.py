import os
from pathlib import Path
import logging
import pandas as pd
import numpy as np # <--- ADICIONADO IMPORT
import hashlib
import json
import threading
import traceback
from datetime import datetime
from contextlib import contextmanager
from sqlalchemy import bindparam, create_engine, text, inspect, MetaData, Table # Adicionado inspect
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError

from mt5_extracao.error_handler import with_error_handling, DatabaseError, DataTypeError


# Handlers configurados em mt5_extracao.logging_setup (pontos de entrada)
log = logging.getLogger(__name__)

class DatabaseManager:
    """
    Gerencia a conexão e as operações com o banco de dados.
    """
    TIME_BASES = ("broker", "utc")

    # Valor numérico usado pelo código legado quando o offset não foi declarado; NÃO é informação
    # conhecida: time_context() o expõe como ``offset_state='unknown'`` e leituras causais recusam.
    _PRESUMED_OFFSET = -3.0

    def __init__(self, db_type='sqlite', db_path='database/mt5_data.db', time_basis='broker',
                 broker_utc_offset=None, broker_timezone=None, read_only=False):
        """
        Inicializa o gerenciador do banco de dados.

        Args:
            db_type (str): 'sqlite' ou 'postgresql' (TimescaleDB é usado se a extensão existir).
            db_path (str): Caminho do arquivo SQLite ou URL SQLAlchemy do PostgreSQL
                           (ex.: postgresql+psycopg://usuario:senha@host:5432/banco).
            time_basis (str): 'broker' grava o horário do servidor da corretora como o MT5 devolve
                              (padrão, comportamento histórico); 'utc' converte para UTC ao gravar.
            broker_utc_offset (float | None): fuso fixo da corretora em horas. ``None`` = desconhecido
                (o legado ainda calcula com -3, mas o estado fica explícito como desconhecido).
            broker_timezone (str | None): zona IANA da corretora (ex.: Europe/Athens). Exige
                time_basis='utc': converte o relógio local para UTC com horário de verão; a hora
                repetida é inferida pela ordem e a inexistente é recusada.
        """
        if time_basis not in self.TIME_BASES:
            raise ValueError(f"time_basis inválido: {time_basis!r} (use 'broker' ou 'utc')")
        self.db_type = db_type
        self.db_path = db_path
        self.time_basis = time_basis
        self.offset_declared = broker_utc_offset is not None and str(broker_utc_offset).strip() != ""
        self.broker_utc_offset = float(broker_utc_offset) if self.offset_declared else self._PRESUMED_OFFSET
        self.broker_timezone = (broker_timezone or "").strip() or None
        if self.broker_timezone:
            import pytz  # base de fusos própria: funciona no Windows sem o pacote tzdata
            try:
                pytz.timezone(self.broker_timezone)
            except pytz.UnknownTimeZoneError as exc:
                raise ValueError(f"broker_timezone inválido: {self.broker_timezone!r}; use um nome IANA "
                                 "como Europe/Athens.") from exc
            if time_basis != "utc":
                raise ValueError("broker_timezone exige time_basis = utc (o horário local repete no fim do "
                                 "horário de verão e não pode ser chave).")
        self._time_basis_checked = False
        # Somente leitura: não cria pasta, arquivo nem tabelas de controle; o próprio banco recusa escrita.
        self.read_only = bool(read_only)
        self.engine = None
        self._table_lock = threading.Lock()
        self._table_cache = {}
        self._connect()

    def _connect(self):
        """Estabelece a conexão com o banco de dados."""
        log.info(f"Configurando conexão com banco de dados: Tipo={self.db_type}, Path={self._safe_location()}")
        try:
            if self.db_type == 'sqlite':
                # Garante que o diretório do banco existe
                db_dir = os.path.dirname(self.db_path)
                if self.read_only:
                    if not os.path.isfile(self.db_path):
                        raise SQLAlchemyError(f"Banco inexistente para leitura: {self._safe_location()}")
                    self.engine = create_engine(f"sqlite:///file:{Path(self.db_path).resolve().as_posix()}?mode=ro&uri=true",
                                                connect_args={'timeout': 30})
                    with self.engine.connect():
                        log.info("Conexão SQLite somente leitura estabelecida.")
                    return
                if db_dir and not os.path.exists(db_dir):
                    os.makedirs(db_dir)
                    log.info(f"Diretório do banco de dados criado: {db_dir}")
                connection_string = f'sqlite:///{self.db_path}'
                # timeout: extrações em paralelo gravam no mesmo arquivo (SQLite serializa escritas)
                self.engine = create_engine(connection_string, connect_args={'timeout': 30})
                # Testa a conexão inicial
                with self.engine.connect() as connection:
                    log.info(f"Conexão com banco de dados SQLite estabelecida: {self.db_path}")
                self._ensure_control_tables()
            elif self.db_type == 'postgresql':
                url = self.read_only_pg_url(self.db_path) if self.read_only else self.db_path
                self.engine = create_engine(url, pool_pre_ping=True)
                with self.engine.connect() as connection:
                    self._timescale = bool(connection.execute(text(
                        "SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'")).scalar())
                log.info(f"Conexão com PostgreSQL estabelecida: {self._safe_location()}"
                         + (" (TimescaleDB disponível)" if self._timescale else ""))
                if not self.read_only:
                    self._ensure_control_tables()
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

    _timescale = False

    def close(self):
        """Libera as conexões com o banco (chamado ao encerrar o aplicativo). Pode ser chamado mais de uma vez."""
        if self.engine is not None:
            self.engine.dispose()

    @contextmanager
    def consistent_read(self):
        """Uma imagem de leitura para barras e metadados, sem mudar o isolamento global."""
        if not self.is_connected():
            raise ValueError("Banco indisponível; confira a conexão antes de capturar os dados.")
        with self.engine.connect() as conn:
            if self.dialect == "postgresql":
                conn = conn.execution_options(isolation_level="REPEATABLE READ")
            with conn.begin():
                if self.dialect == "sqlite":
                    # sqlite3 legado não inicia transação em SELECT; SQLAlchemy.begin é lógico.
                    if not conn.connection.driver_connection.in_transaction:
                        conn.exec_driver_sql("BEGIN")
                elif self.dialect == "postgresql":
                    conn.exec_driver_sql("SET TRANSACTION READ ONLY")
                yield conn

    def _safe_location(self):
        """Caminho/URL para logs, sem a senha."""
        if self.db_type == 'sqlite':
            return self.db_path
        try:
            return make_url(self.db_path).render_as_string(hide_password=True)
        except Exception:
            return "<url inválida>"

    @property
    def dialect(self):
        return self.engine.dialect.name if self.engine is not None else self.db_type

    def _data_tables(self):
        """Tabelas de dados de símbolos (as internas começam com '_')."""
        names = inspect(self.engine).get_table_names()
        return sorted(n for n in names if not n.startswith('_') and not n.startswith('sqlite_'))

    @staticmethod
    def _as_datetime(value):
        """Horário lido do banco: texto no SQLite, datetime no PostgreSQL."""
        if isinstance(value, datetime):
            return value
        return pd.Timestamp(str(value)).to_pydatetime()

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
        "tick_volume": "BIGINT",   # BIGINT: no PostgreSQL INTEGER tem 32 bits (no SQLite é igual)
        "spread": "INTEGER",
        "real_volume": "BIGINT",
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
                    if self._timescale:
                        # TimescaleDB: particiona por tempo (consultas por período mais rápidas)
                        connection.execute(text("SELECT create_hypertable(:t, 'time', if_not_exists => TRUE, "
                                                "migrate_data => TRUE)"), {"t": table_name})
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

    def save_ohlcv_data(self, symbol, timeframe_name, df, *, received_at=None, source=None):
        """
        Salva ou atualiza dados OHLCV no banco de dados.
        Garante que a tabela exista com o schema completo antes de salvar.

        Args:
            symbol (str): Nome do símbolo (ex: 'WIN$N').
            timeframe_name (str): Nome legível do timeframe (ex: '1 minuto').
            df (pd.DataFrame): DataFrame contendo os dados OHLCV com coluna 'time'.
            received_at: instante UTC em que ESTE sistema recebeu o payload (padrão: agora).
                Um retry deve repassar o instante original; payload mais antigo que o vigente
                não prevalece (contrato de recebimento em ``_ohlcv_receipts``).
            source: fonte do payload ({provider, server[, derivation]}). Fonte primária passa por
                ``datasets.declare_source``: incompatível com a base é recusada sem gravar. Com
                ``derivation='fallback_m1'`` (fallback configurado) a base não muda de fonte e cada
                recebimento registra a origem real.
        """
        if received_at is not None:
            self._utc_text(received_at, "recebimento (received_at)")  # erro de chamada: não vira False
        origin = "write:unknown"
        if source:
            from mt5_extracao.analytics import datasets
            provider = source.get("provider") or "unknown"
            if source.get("derivation") == "fallback_m1":
                datasets._source(source, "validation")
                origin = f"fallback:{provider}"
            else:
                try:
                    declared = datasets.declare_source(self, source)
                except ValueError as exc:
                    log.error(f"Gravação recusada para {symbol} {timeframe_name}: {exc}")
                    return False
                origin = f"write:{declared['provider']}@{declared['server'] or 'servidor-desconhecido'}"
        if not self.is_connected():
            log.error("Não é possível salvar dados: Banco de dados não conectado.")
            return False
        if df is None or df.empty:
            log.warning(f"DataFrame vazio para {symbol} {timeframe_name}. Nada a salvar.")
            return True # Considera sucesso, pois não há erro

        table_name = self.get_table_name_for_symbol(symbol, timeframe_name)
        log.info(f"Preparando para salvar {len(df)} registros para {symbol} ({timeframe_name}) na tabela '{table_name}'...")

        # 0. A base de tempo gravada no banco precisa ser a mesma configurada
        if not self._check_time_basis():
            return False

        # 1. Garantir que a tabela exista com o schema completo
        if not self._create_table_if_not_exists(table_name):
            log.error(f"Falha ao garantir a existência/schema da tabela '{table_name}'. Abortando salvamento.")
            return False

        # 2. Preparar DataFrame para salvar
        try:
            df_converted = df.copy()
            # Garantir que 'time' seja datetime
            if 'time' in df_converted.columns:
                 df_converted['time'] = self._to_storage(pd.to_datetime(df_converted['time']))
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
            self._upsert_dataframe(table_name, df_to_save, received_at=received_at, origin=origin)

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

    # Limite seguro de linhas por INSERT: SQLite aceita até 32766 parâmetros;
    # a tabela OHLCV tem ~70 colunas -> 200 linhas = ~14 mil parâmetros.
    _UPSERT_CHUNK_ROWS = 200

    # Contrato de recebimento do bruto OHLCV (T1.1/BD04), aditivo:
    #   _ohlcv_versions: cada CONTEÚDO distinto de uma barra (hash do OHLCV/volumes/spread);
    #   _ohlcv_receipts: cada EVENTO de recebimento (conteúdo + instante em que ESTE sistema o recebeu).
    # Prevalece o conteúdo do recebimento mais recente; empate com conteúdos distintos é conflito,
    # resolvido pelo menor hash (independe da ordem de chegada) e sinalizado. Legado gravado antes do
    # contrato recebe um recebimento com instante NULL (desconhecido), nunca um horário inventado.
    VERSIONS_TABLE = "_ohlcv_versions"
    RECEIPTS_TABLE = "_ohlcv_receipts"
    _REVISION_FIELDS = ("open", "high", "low", "close", "tick_volume", "spread", "real_volume")

    def _ensure_receipts(self, conn):
        conn.execute(text(
            f"CREATE TABLE IF NOT EXISTS {self.VERSIONS_TABLE} (table_name TEXT NOT NULL, time TEXT NOT NULL, "
            "content_sha256 TEXT NOT NULL, payload_json TEXT NOT NULL, "
            "PRIMARY KEY (table_name, time, content_sha256))"))
        conn.execute(text(
            f"CREATE TABLE IF NOT EXISTS {self.RECEIPTS_TABLE} (table_name TEXT NOT NULL, time TEXT NOT NULL, "
            "content_sha256 TEXT NOT NULL, received_at_utc TEXT NOT NULL, recorded_at_utc TEXT NOT NULL, "
            "origin TEXT NOT NULL, PRIMARY KEY (table_name, time, content_sha256, received_at_utc))"))

    _UNKNOWN_RECEIPT = ""  # legado: chegada desconhecida (texto vazio ordena antes de qualquer instante)

    @staticmethod
    def _utc_text(value, label="instante"):
        """UTC canônico; horário sem fuso é recusado em vez de presumido (como no feature store)."""
        stamp = pd.Timestamp(value)
        if pd.isna(stamp) or stamp.tzinfo is None:
            raise ValueError(f"O {label} precisa declarar fuso horário; horário sem fuso não é presumido UTC.")
        return stamp.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    def _market_payload(self, row):
        payload = {}
        for name in self._REVISION_FIELDS:
            value = row.get(name)
            payload[name] = None if value is None or (isinstance(value, float) and np.isnan(value)) else float(value)
        text_payload = json.dumps(payload, sort_keys=True)
        return text_payload, hashlib.sha256(text_payload.encode()).hexdigest()

    @staticmethod
    def _winner(receipts):
        """(hash vencedor, recebimento, conflito) entre recebimentos (hash, instante)."""
        latest = max(received for _, received in receipts)
        tied = sorted({digest for digest, received in receipts if received == latest})
        return tied[0], latest, len(tied) > 1

    def _receive(self, conn, table_name, records, received_at, origin="write:unknown"):
        """Registra os recebimentos e devolve só os registros cujo conteúdo prevalece.

        Barras cujo vencedor não é o conteúdo recebido agora têm o bruto reconciliado com o vencedor.
        """
        if not records:
            return records
        self._ensure_receipts(conn)
        incoming = self._utc_text(received_at if received_at is not None else pd.Timestamp.now(tz="UTC"),
                                  "recebimento (received_at)")
        recorded = self._utc_text(pd.Timestamp.now(tz="UTC"))
        key = lambda value: pd.Timestamp(value).strftime(self._TIME_FMT)
        stamps = sorted({key(r["time"]) for r in records})
        bind = stamps if self.dialect == "sqlite" else [pd.Timestamp(v).to_pydatetime() for v in stamps]
        columns = ", ".join(["time", *self._REVISION_FIELDS])
        current, receipts = {}, {}
        for begin in range(0, len(stamps), 500):
            rows = conn.execute(text(f'SELECT {columns} FROM "{table_name}" WHERE time IN :times').bindparams(
                bindparam("times", expanding=True)), {"times": bind[begin:begin + 500]}).mappings()
            current.update({key(row["time"]): dict(row) for row in rows})
            for row in conn.execute(text(
                    f"SELECT time, content_sha256, received_at_utc FROM {self.RECEIPTS_TABLE} "
                    "WHERE table_name=:t AND time IN :times").bindparams(bindparam("times", expanding=True)),
                    {"t": table_name, "times": stamps[begin:begin + 500]}):
                receipts.setdefault(row[0], []).append((row[1], row[2]))
        add_version = text(f"INSERT INTO {self.VERSIONS_TABLE} VALUES (:t,:time,:h,:payload) "
                           "ON CONFLICT (table_name, time, content_sha256) DO NOTHING")
        add_receipt = text(f"INSERT INTO {self.RECEIPTS_TABLE} VALUES (:t,:time,:h,:rcv,:rec,:origin) "
                           "ON CONFLICT (table_name, time, content_sha256, received_at_utc) DO NOTHING")
        by_stamp = {}
        for record in records:
            stamp = key(record["time"])
            payload, digest = self._market_payload(record)
            by_stamp.setdefault(stamp, {})[digest] = record
            if stamp not in receipts and stamp in current:  # bruto anterior ao contrato
                legacy_payload, legacy_digest = self._market_payload(current[stamp])
                conn.execute(add_version, {"t": table_name, "time": stamp, "h": legacy_digest, "payload": legacy_payload})
                conn.execute(add_receipt, {"t": table_name, "time": stamp, "h": legacy_digest,
                                           "rcv": self._UNKNOWN_RECEIPT, "rec": recorded, "origin": "legacy"})
                receipts[stamp] = [(legacy_digest, self._UNKNOWN_RECEIPT)]
            conn.execute(add_version, {"t": table_name, "time": stamp, "h": digest, "payload": payload})
            conn.execute(add_receipt, {"t": table_name, "time": stamp, "h": digest, "rcv": incoming,
                                       "rec": recorded, "origin": origin})
            receipts.setdefault(stamp, [])
            if (digest, incoming) not in receipts[stamp]:
                receipts[stamp].append((digest, incoming))
        apply = []
        for stamp, candidates in by_stamp.items():
            digest, _, conflict = self._winner(receipts[stamp])
            if conflict:
                log.warning("Recebimentos empatados com conteúdos distintos em %s %s; prevalece o menor hash.",
                            table_name, stamp)
            if digest in candidates:
                apply.append(candidates[digest])
                continue
            if stamp in current and self._market_payload(current[stamp])[1] == digest:
                continue
            winner = json.loads(conn.execute(text(
                f"SELECT payload_json FROM {self.VERSIONS_TABLE} WHERE table_name=:t AND time=:time "
                "AND content_sha256=:h"), {"t": table_name, "time": stamp, "h": digest}).scalar_one())
            log.warning("Recebimento anterior ao vigente em %s %s não sobrescreve o bruto.", table_name, stamp)
            if stamp not in current:
                apply.append({**next(iter(candidates.values())), **winner})
        return apply

    def _upsert_dataframe(self, table_name, df, *, received_at=None, origin="write:unknown"):
        """Insere ou atualiza linhas pela chave primária 'time'. df indexado por 'time'.

        Tudo numa transação: falha em qualquer bloco desfaz os anteriores e os recebimentos.
        """
        table = Table(table_name, MetaData(), autoload_with=self.engine)
        table_cols = [c.name for c in table.columns]
        data = df.reset_index() if 'time' not in df.columns else df
        data = data[[c for c in table_cols if c in data.columns]]
        # NaN -> None (NULL) e Timestamp -> datetime (formato de gravação do SQLAlchemy)
        data = data.astype(object).where(pd.notna(data), None)
        records = data.to_dict(orient='records')
        for r in records:
            if isinstance(r['time'], pd.Timestamp):
                r['time'] = r['time'].to_pydatetime()
        with self.engine.begin() as conn:
            records = self._receive(conn, table_name, records, received_at, origin)
            for i in range(0, len(records), self._UPSERT_CHUNK_ROWS):
                insert = postgresql_insert if self.dialect == 'postgresql' else sqlite_insert
                stmt = insert(table).values(records[i:i + self._UPSERT_CHUNK_ROWS])
                update_cols = {c: stmt.excluded[c] for c in data.columns if c != 'time'}
                conn.execute(stmt.on_conflict_do_update(index_elements=['time'], set_=update_cols))
        return len(records)

    def load_ohlcv_as_of(self, symbol, timeframe_name, as_of, *, mode="causal", start=None, end=None):
        """Barras como eram conhecidas em ``as_of`` (com fuso), sem recorrer ao vigente.

        ``causal``: só barras já FECHADAS e já RECEBIDAS no corte, com o conteúdo do último recebimento
        até lá; barras de chegada desconhecida (legado) ficam de fora. ``inspect``: inclui o legado com
        ``arrival_state='unknown'`` para leitura humana — não é elegível para backtest/decisão.
        Colunas extras: ``known_since_utc``, ``arrival_state``, ``conflict``, ``content_sha256``.
        """
        if mode not in ("causal", "inspect"):
            raise ValueError("mode deve ser 'causal' ou 'inspect'.")
        cutoff = self._utc_text(as_of, "corte (as_of)")
        if self.time_context()["offset_state"] == "unknown":
            raise ValueError("Leitura causal exige o fuso do corretor declarado (broker_utc_offset ou "
                             "broker_timezone); com fuso desconhecido não se sabe quando a barra fechou em UTC.")
        from mt5_extracao import timeframes
        tf = timeframes.parse(timeframe_name)
        if tf is None:
            raise ValueError(f"Timeframe desconhecido: {timeframe_name}")
        table_name = self.get_table_name_for_symbol(symbol, timeframe_name)
        with self.consistent_read() as conn:
            bars = self.load_ohlcv(symbol, timeframe_name, start, end, connection=conn)
            history = pd.DataFrame(columns=["time", "content_sha256", "received_at_utc", "payload_json"])
            if not bars.empty and inspect(conn).has_table(self.RECEIPTS_TABLE):
                history = pd.read_sql(text(
                    f"SELECT r.time, r.content_sha256, r.received_at_utc, v.payload_json "
                    f"FROM {self.RECEIPTS_TABLE} r JOIN {self.VERSIONS_TABLE} v ON v.table_name=r.table_name "
                    "AND v.time=r.time AND v.content_sha256=r.content_sha256 WHERE r.table_name=:t "
                    "AND r.time >= :first AND r.time <= :last"), conn, params={
                        "t": table_name,
                        "first": self._to_storage(pd.Series([pd.Timestamp(bars.time.min())])).dt.strftime(self._TIME_FMT)[0],
                        "last": self._to_storage(pd.Series([pd.Timestamp(bars.time.max())])).dt.strftime(self._TIME_FMT)[0]})
        extra = ["known_since_utc", "arrival_state", "conflict", "content_sha256"]
        if bars.empty:
            return bars.assign(**{name: None for name in extra})
        starts = pd.to_datetime(bars["time"])
        step = pd.DateOffset(months=1) if tf.name == "MN1" else pd.Timedelta(minutes=tf.minutes)
        if self.time_basis == "utc" or self.broker_timezone:
            ends_utc = (pd.to_datetime(self._to_storage(starts)) + step).dt.tz_localize("UTC")
        else:
            ends_utc = (starts + step - pd.Timedelta(hours=float(self.broker_utc_offset))).dt.tz_localize("UTC")
        frame = bars.assign(_stamp=self._to_storage(starts).dt.strftime(self._TIME_FMT),
                            _closed=ends_utc.dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ") <= cutoff)
        known = history[(history.received_at_utc != self._UNKNOWN_RECEIPT) & (history.received_at_utc <= cutoff)]
        known = known.sort_values(["time", "received_at_utc", "content_sha256"], ascending=[True, False, True])
        top = known.groupby("time").head(1).set_index("time")
        ties = known.merge(top[["received_at_utc"]].reset_index(), on=["time", "received_at_utc"])
        conflicts = set(ties.groupby("time")["content_sha256"].nunique().loc[lambda n: n > 1].index)
        legacy_only = history.groupby("time")["received_at_utc"].agg(lambda r: (r == self._UNKNOWN_RECEIPT).all())
        legacy = history[history.received_at_utc == self._UNKNOWN_RECEIPT].drop_duplicates("time").set_index("time")
        rows = []
        for row in frame.to_dict(orient="records"):
            stamp, closed = row.pop("_stamp"), row.pop("_closed")
            if closed and stamp in top.index:
                version = top.loc[stamp]
                row.update(json.loads(version.payload_json))
                row.update(known_since_utc=pd.Timestamp(version.received_at_utc), arrival_state="received",
                           conflict=stamp in conflicts, content_sha256=version.content_sha256)
                rows.append(row)
            elif mode == "inspect" and (stamp not in legacy_only.index or legacy_only[stamp]):
                if stamp in legacy.index:
                    row.update(json.loads(legacy.loc[stamp].payload_json))
                row.update(known_since_utc=None, arrival_state="unknown", conflict=False,
                           content_sha256=legacy.loc[stamp].content_sha256 if stamp in legacy.index else None)
                rows.append(row)
        result = pd.DataFrame(rows, columns=[*bars.columns, *extra])
        return result.reset_index(drop=True)

    def _has_time_primary_key(self, table_name):
        """True se a tabela existe e sua chave primária é exatamente a coluna 'time'."""
        inspector = inspect(self.engine)
        if not inspector.has_table(table_name):
            return False
        return inspector.get_pk_constraint(table_name).get('constrained_columns') == ['time']

    # Formato de data usado nas tabelas de controle (texto ordenável, com microssegundos)
    _TIME_FMT = '%Y-%m-%d %H:%M:%S.%f'

    @staticmethod
    def read_only_pg_url(url):
        """URL PostgreSQL somente leitura: ACRESCENTA às options existentes (ex.: search_path do schema)."""
        parsed = make_url(url)
        current = parsed.query.get("options", "")
        current = " ".join(current) if isinstance(current, tuple) else current
        merged = " ".join(part for part in (current, "-c default_transaction_read_only=on") if part)
        return parsed.update_query_dict({"options": merged}).render_as_string(hide_password=False)

    def time_context(self):
        """Contexto temporal da base: base de gravação, zona ou offset, e se o offset é conhecido."""
        state = "zone" if self.broker_timezone else "declared" if self.offset_declared else "unknown"
        return {"time_basis": self.time_basis, "broker_timezone": self.broker_timezone,
                "broker_utc_offset": self.broker_utc_offset if state == "declared" else None, "offset_state": state}

    def require_fixed_offset(self, consumer):
        """Consumidores que ainda convertem com offset fixo recusam base com zona, sem resultado errado."""
        if self.broker_timezone:
            raise ValueError(f"{consumer} ainda usa offset fixo e não suporta base com zona "
                             f"({self.broker_timezone}); use uma base sem broker_timezone para esse consumo.")

    def _to_storage(self, times):
        """Horário da corretora (como o MT5 devolve) -> horário gravado no banco."""
        if self.broker_timezone:
            local = pd.DatetimeIndex(pd.to_datetime(times))
            try:
                aware = local.tz_localize(self.broker_timezone, ambiguous="infer", nonexistent="raise")
            except Exception as exc:
                raise ValueError(f"Horário local incompatível com {self.broker_timezone}: hora inexistente ou "
                                 f"repetida sem ordem que a desambigue ({exc}).") from exc
            result = aware.tz_convert("UTC").tz_localize(None)
            return pd.Series(result, index=getattr(times, "index", None)) if isinstance(times, pd.Series) else result
        if self.time_basis == "utc":
            return times - pd.Timedelta(hours=self.broker_utc_offset)
        return times

    def _from_storage(self, times):
        """Horário gravado no banco -> horário da corretora."""
        if self.broker_timezone:
            utc = pd.DatetimeIndex(pd.to_datetime(times)).tz_localize("UTC").tz_convert(self.broker_timezone)
            result = utc.tz_localize(None)
            return pd.Series(result, index=getattr(times, "index", None)) if isinstance(times, pd.Series) else result
        if self.time_basis == "utc":
            return times + pd.Timedelta(hours=self.broker_utc_offset)
        return times

    def get_metadata(self, key):
        with self.engine.connect() as conn:
            row = conn.execute(text("SELECT value FROM _metadata WHERE key = :k"), {"k": key}).fetchone()
        return row[0] if row else None

    def _check_time_basis(self):
        """
        Grava a base de tempo na primeira gravação e recusa gravar se o banco já usa outra
        (misturar horários da corretora e UTC corromperia as séries).
        """
        if self._time_basis_checked:
            return True
        with self.engine.begin() as conn:
            row = conn.execute(text("SELECT value FROM _metadata WHERE key = 'time_basis'")).fetchone()
            if row is None:
                conn.execute(text("INSERT INTO _metadata (key, value) VALUES ('time_basis', :v)"),
                             {"v": self.time_basis})
            zone = conn.execute(text("SELECT value FROM _metadata WHERE key = 'broker_timezone'")).fetchone()
            if zone is None and row is None and self.broker_timezone:
                conn.execute(text("INSERT INTO _metadata (key, value) VALUES ('broker_timezone', :v)"),
                             {"v": self.broker_timezone})
            elif (zone[0] if zone else None) != self.broker_timezone and row is not None:
                log.error(f"Este banco foi gravado com zona {zone[0] if zone else 'nenhuma'!s}, mas a configuração pede "
                          f"{self.broker_timezone or 'nenhuma'}. Nada foi gravado: não se reinterpreta uma base existente.")
                return False
            if row is not None and row[0] != self.time_basis:
                log.error(f"Este banco grava horários na base '{row[0]}', mas o config.ini pede '{self.time_basis}'. "
                          f"Nada foi gravado. Ajuste [APP] time_basis = {row[0]} ou use outro arquivo de banco.")
                return False
        self._time_basis_checked = True
        return True

    def _ensure_control_tables(self):
        """Cria as tabelas internas de controle, se não existirem."""
        with self.engine.begin() as conn:
            conn.execute(text(
                "CREATE TABLE IF NOT EXISTS _extraction_log ("
                " table_name  TEXT NOT NULL,"
                " block_start TIMESTAMP NOT NULL,"
                " block_end   TIMESTAMP NOT NULL,"
                " rows        INTEGER NOT NULL,"
                " status      TEXT NOT NULL,"   # 'ok' | 'empty' | 'failed'
                " source      TEXT NOT NULL,"   # 'mt5' | nome da fonte externa
                " updated_at  TIMESTAMP NOT NULL,"
                " PRIMARY KEY (table_name, block_start, block_end))"))
            conn.execute(text("CREATE TABLE IF NOT EXISTS _metadata (key TEXT PRIMARY KEY, value TEXT)"))
            conn.execute(text(
                "CREATE TABLE IF NOT EXISTS _symbol_tables ("
                " symbol     TEXT NOT NULL,"
                " timeframe  TEXT NOT NULL,"    # timeframe normalizado (ex.: 1_minuto)
                " table_name TEXT NOT NULL UNIQUE,"
                " PRIMARY KEY (symbol, timeframe))"))
            conn.execute(text(
                "CREATE TABLE IF NOT EXISTS _symbol_specs ("
                " symbol TEXT PRIMARY KEY, point REAL, digits INTEGER, tick_size REAL, tick_value REAL,"
                " volume_min REAL, volume_step REAL, volume_max REAL, contract_size REAL,"
                " currency_profit TEXT, spread REAL, updated_at TEXT)"))
            # Colunas adicionadas depois (bases antigas recebem via ALTER TABLE)
            cols = {c["name"] for c in inspect(conn).get_columns("_extraction_log")}
            if "quality_json" not in cols:
                conn.execute(text("ALTER TABLE _extraction_log ADD COLUMN quality_json TEXT"))
            cols = {c["name"] for c in inspect(conn).get_columns("_symbol_specs")}
            for name, kind in (("swap_long", "REAL"), ("swap_short", "REAL"), ("swap_mode", "INTEGER"),
                               ("swap_rollover3days", "INTEGER"), ("spread_profile", "TEXT")):
                if name not in cols:
                    conn.execute(text(f"ALTER TABLE _symbol_specs ADD COLUMN {name} {kind}"))

    def record_block(self, table_name, start, end, rows, status, source, quality=None):
        """Registra (ou atualiza) o resultado da extração de um bloco (quality: relatório de data_quality)."""
        with self.engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO _extraction_log (table_name, block_start, block_end, rows, status, source, updated_at, "
                "quality_json) VALUES (:t, :s, :e, :r, :st, :src, :u, :q) "
                "ON CONFLICT(table_name, block_start, block_end) DO UPDATE SET "
                "rows = excluded.rows, status = excluded.status, source = excluded.source, "
                "updated_at = excluded.updated_at, quality_json = excluded.quality_json"),
                {"t": table_name, "s": start.strftime(self._TIME_FMT), "e": end.strftime(self._TIME_FMT),
                 "r": int(rows), "st": status, "src": source, "u": datetime.now().strftime(self._TIME_FMT),
                 "q": json.dumps(quality, ensure_ascii=False) if quality is not None else None})

    def quality_reports(self, table_name):
        """Relatórios de qualidade gravados para a tabela: lista de (block_start, dict)."""
        with self.engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT block_start, quality_json FROM _extraction_log "
                "WHERE table_name = :t AND quality_json IS NOT NULL ORDER BY block_start"), {"t": table_name}).fetchall()
        return [(self._as_datetime(a).strftime(self._TIME_FMT), json.loads(q)) for a, q in rows]

    def completed_blocks(self, table_name):
        """Blocos (inicio, fim) já concluídos (status 'ok' ou 'empty') para a tabela."""
        with self.engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT block_start, block_end FROM _extraction_log "
                "WHERE table_name = :t AND status IN ('ok', 'empty')"), {"t": table_name}).fetchall()
        return {(self._as_datetime(a), self._as_datetime(b)) for a, b in rows}

    def failed_blocks(self, table_name, until):
        """Intervalos falhos já encerrados, isolados por tabela, para retomada exata."""
        with self.engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT block_start, block_end FROM _extraction_log "
                "WHERE table_name = :t AND status = 'failed' AND block_end <= :end "
                "ORDER BY block_start, block_end"),
                {"t": table_name, "end": until.strftime(self._TIME_FMT)}).fetchall()
        return [(self._as_datetime(a), self._as_datetime(b)) for a, b in rows]

    TICK_FIELDS = ("time", "time_msc", "seq", "bid", "ask", "last", "volume", "volume_real", "flags")

    def save_ticks(self, table_name, df):
        """
        Grava ticks com upsert. Chave (time_msc, seq): o MT5 pode devolver vários ticks no mesmo
        milissegundo, e seq (ordem dentro do milissegundo) os mantém distintos e reprocessáveis.
        Retorna o número de linhas gravadas.
        """
        if df is None or df.empty:
            return 0
        if not self._check_time_basis():
            raise RuntimeError("Base de tempo do banco diferente da configurada; ticks não gravados.")
        data = df.sort_values("time_msc", kind="stable").reset_index(drop=True)
        data["seq"] = data.groupby("time_msc").cumcount()
        times = self._to_storage(pd.to_datetime(data["time_msc"], unit="ms"))
        data["time"] = pd.Series(times).dt.strftime(self._TIME_FMT).values
        for col in self.TICK_FIELDS:
            if col not in data.columns:
                data[col] = 0
        rows = data[list(self.TICK_FIELDS)].astype(object).where(data[list(self.TICK_FIELDS)].notna(), None)
        records = rows.to_dict("records")
        cols = ", ".join(self.TICK_FIELDS)
        params = ", ".join(f":{c}" for c in self.TICK_FIELDS)
        updates = ", ".join(f"{c} = excluded.{c}" for c in self.TICK_FIELDS if c not in ("time_msc", "seq"))
        with self.engine.begin() as conn:
            conn.execute(text(
                f'CREATE TABLE IF NOT EXISTS "{table_name}" (time TEXT NOT NULL, time_msc BIGINT NOT NULL, '
                f'seq INTEGER NOT NULL, bid REAL, ask REAL, last REAL, volume BIGINT, volume_real REAL, '
                f'flags INTEGER, PRIMARY KEY (time_msc, seq))'))
            sql = text(f'INSERT INTO "{table_name}" ({cols}) VALUES ({params}) '
                       f'ON CONFLICT(time_msc, seq) DO UPDATE SET {updates}')
            for i in range(0, len(records), 500):
                conn.execute(sql, records[i:i + 500])
        return len(records)

    BOOK_FIELDS = ("time_utc", "time_msc", "type", "price", "volume")

    def save_book(self, table_name, rows):
        """
        Grava níveis do book (lista de dicts com BOOK_FIELDS). Chave (time_msc, type, price):
        horário do coletor em UTC (o MT5 não informa o horário do book). Retorna as linhas gravadas.
        """
        if not rows:
            return 0
        cols = ", ".join(self.BOOK_FIELDS)
        params = ", ".join(f":{c}" for c in self.BOOK_FIELDS)
        with self.engine.begin() as conn:
            conn.execute(text(
                f'CREATE TABLE IF NOT EXISTS "{table_name}" (time_utc TEXT NOT NULL, time_msc BIGINT NOT NULL, '
                f'type INTEGER NOT NULL, price REAL NOT NULL, volume REAL, PRIMARY KEY (time_msc, type, price))'))
            conn.execute(text(f'INSERT INTO "{table_name}" ({cols}) VALUES ({params}) '
                              f'ON CONFLICT(time_msc, type, price) DO UPDATE SET volume = excluded.volume'), rows)
        return len(rows)

    SPEC_FIELDS = ("point", "digits", "tick_size", "tick_value", "volume_min", "volume_step", "volume_max",
                   "contract_size", "currency_profit", "spread", "swap_long", "swap_short", "swap_mode",
                   "swap_rollover3days")

    def save_spread_profile(self, symbol, profile):
        """Grava o spread medido nos ticks (dict de specs.spread_profile). O símbolo precisa ter especificação."""
        with self.engine.begin() as conn:
            done = conn.execute(text("UPDATE _symbol_specs SET spread_profile = :p WHERE symbol = :s"),
                                {"p": json.dumps(profile, ensure_ascii=False), "s": symbol})
            if done.rowcount:
                from mt5_extracao.analytics import instruments
                from datetime import timezone
                row = self.get_symbol_spec(symbol, connection=conn)
                values = {key: row.get(key) for key in (*self.SPEC_FIELDS, "spread_profile")}
                instruments.record(self, symbol, "specification", values, source="tick_spread_calibration",
                                   known_at=datetime.now(timezone.utc), connection=conn)
        return done.rowcount > 0

    def load_tick_spreads(self, table_name):
        """Ticks com bid e ask de uma tabela <símbolo>_ticks: DataFrame time_msc, bid, ask (vazio se não há)."""
        if not inspect(self.engine).has_table(table_name):
            return pd.DataFrame(columns=["time_msc", "bid", "ask"])
        with self.engine.connect() as conn:
            return pd.read_sql(text(f'SELECT time_msc, bid, ask FROM "{table_name}" WHERE bid > 0 AND ask > 0 '
                                    "ORDER BY time_msc"), conn)

    def save_symbol_spec(self, symbol, spec, *, source="local_observation", known_at=None):
        """Grava (ou atualiza) a especificação do símbolo (dict com as chaves de SPEC_FIELDS)."""
        values = {k: spec.get(k) for k in self.SPEC_FIELDS}
        cols = ", ".join(self.SPEC_FIELDS)
        params = ", ".join(f":{k}" for k in self.SPEC_FIELDS)
        updates = ", ".join(f"{k} = excluded.{k}" for k in self.SPEC_FIELDS)
        with self.engine.begin() as conn:
            from mt5_extracao.analytics import instruments
            from datetime import timezone
            instruments.record(self, symbol, "specification", values, source=source,
                               known_at=known_at or datetime.now(timezone.utc), connection=conn)
            conn.execute(text(
                f"INSERT INTO _symbol_specs (symbol, {cols}, updated_at) VALUES (:symbol, {params}, :updated_at) "
                f"ON CONFLICT(symbol) DO UPDATE SET {updates}, updated_at = excluded.updated_at"),
                {"symbol": symbol, **values, "updated_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')})

    def get_symbol_spec(self, symbol, *, connection=None):
        """Especificação gravada do símbolo (dict, inclui updated_at) ou None."""
        if connection is None:
            with self.engine.connect() as conn:
                return self.get_symbol_spec(symbol, connection=conn)
        row = connection.execute(text("SELECT * FROM _symbol_specs WHERE symbol = :s"), {"s": symbol}).mappings().fetchone()
        return dict(row) if row else None

    def load_market_frame(self, symbol, family, start_utc, end_utc, *, connection=None):
        """Ticks/DOM do intervalo UTC [início,fim), sem converter seu epoch pelo fuso do broker."""
        from mt5_extracao.analytics.data_eligibility import TICK_FIELDS, BOOK_FIELDS
        if family not in ("ticks", "book"):
            raise ValueError("Fonte de mercado inválida; use ticks ou book.")
        if connection is None:
            with self.consistent_read() as conn:
                return self.load_market_frame(symbol, family, start_utc, end_utc, connection=conn)
        fields = TICK_FIELDS if family == "ticks" else BOOK_FIELDS
        table = self.find_table_for_symbol(symbol, family, connection=connection)
        if table is None:
            return pd.DataFrame(columns=[*fields, "symbol", "event_time_utc"])
        available = {col["name"] for col in inspect(connection).get_columns(table)}
        if "time_msc" not in available:
            raise ValueError("Fonte de mercado sem time_msc; confira o schema antes de consumir.")
        columns = [col for col in fields if col in available]
        begin, end = pd.Timestamp(start_utc), pd.Timestamp(end_utc)
        if begin.tzinfo is None or end.tzinfo is None or end <= begin:
            raise ValueError("Intervalo de mercado precisa de fuso explícito e fim posterior ao início.")
        quote = self.engine.dialect.identifier_preparer.quote
        ordering = ("time_msc", "seq") if family == "ticks" else ("time_msc", "type", "price")
        order = ",".join(col for col in ordering if col in available)
        frame = pd.read_sql(text(f"SELECT {','.join(columns)} FROM {quote(table)} "
                                 f"WHERE time_msc>=:start AND time_msc<:end ORDER BY {order}"), connection,
                            params={"start": int(begin.value // 1_000_000), "end": int(end.value // 1_000_000)})
        frame["event_time_utc"] = pd.to_datetime(frame.time_msc, unit="ms", utc=True)
        frame["symbol"] = symbol
        frame = frame[[*columns, "symbol", "event_time_utc"]]
        return frame

    def find_table_for_symbol(self, symbol, timeframe_name, *, connection=None):
        """Tabela existente do símbolo/timeframe, sem registrar nada (None se ainda não foi extraído)."""
        if not self.is_connected():
            return None
        if connection is None:
            with self.engine.connect() as conn:
                return self.find_table_for_symbol(symbol, timeframe_name, connection=conn)
        timeframe_key = self._normalize_name(timeframe_name)
        row = connection.execute(text("SELECT table_name FROM _symbol_tables WHERE symbol = :s AND timeframe = :t"),
                                 {"s": symbol, "t": timeframe_key}).fetchone()
        name = row[0] if row else self._normalize_name(f"{symbol}_{timeframe_name}")
        return name if inspect(connection).has_table(name) else None

    def symbols_with_data(self, symbols, timeframe_name):
        """
        {símbolo: tabela} dos símbolos que já têm tabela nesse timeframe. Uma leitura só e nenhuma
        gravação: serve para a interface marcar os ativos mesmo com milhares de símbolos na conta.
        """
        if not self.is_connected():
            return {}
        timeframe_key = self._normalize_name(timeframe_name)
        existing = set(self._data_tables())
        with self.engine.connect() as conn:
            rows = conn.execute(text("SELECT symbol, table_name FROM _symbol_tables WHERE timeframe = :t"),
                                {"t": timeframe_key}).fetchall()
        registered = {r[0]: r[1] for r in rows}
        owners = {table: symbol for symbol, table in registered.items()}
        result = {}
        for symbol in symbols:
            name = registered.get(symbol)
            if name is None:
                # Base antiga sem registro: vale o nome padrão, se não pertencer a outro símbolo
                name = self._normalize_name(f"{symbol}_{timeframe_name}")
                if owners.get(name, symbol) != symbol:
                    continue
            if name in existing:
                result[symbol] = name
        return result

    def data_inventory(self):
        """
        O que existe na base: uma linha por tabela de barras, com
        {symbol, timeframe (rótulo, ex.: '1 minuto'), table, rows, start, end}. Só leitura.
        Tabelas de bases antigas, sem registro em _symbol_tables, entram com o símbolo deduzido do nome.
        """
        if not self.is_connected():
            return []
        from mt5_extracao import timeframes
        labels = {self._normalize_name(tf.label): tf.label for tf in timeframes.Timeframe}
        inspector = inspect(self.engine)
        with self.engine.connect() as conn:
            registered = {r[2]: (r[0], r[1]) for r in conn.execute(
                text("SELECT symbol, timeframe, table_name FROM _symbol_tables")).fetchall()}
            result = []
            for table in self._data_tables():
                if "close" not in {c["name"] for c in inspector.get_columns(table)}:
                    continue  # ticks e book não são barras
                symbol, key = registered.get(table, (None, None))
                if symbol is None:
                    key = next((k for k in sorted(labels, key=len, reverse=True) if table.endswith("_" + k)), None)
                    symbol = (table[:-len(key) - 1] if key else table).upper()
                rows, start, end = conn.execute(
                    text(f'SELECT COUNT(*), MIN(time), MAX(time) FROM "{table}"')).fetchone()
                result.append({
                    "symbol": symbol, "timeframe": labels.get(key, key or ""), "table": table, "rows": int(rows or 0),
                    "start": self._from_storage(pd.to_datetime(start)).to_pydatetime() if start is not None else None,
                    "end": self._from_storage(pd.to_datetime(end)).to_pydatetime() if end is not None else None})
        return result

    def load_ohlcv(self, symbol, timeframe_name, start=None, end=None, *, connection=None):
        """
        Barras OHLCV (time, open, high, low, close, tick_volume, spread, real_volume) do símbolo, em
        ordem de tempo, no horário da corretora. DataFrame vazio se não houver dados.
        """
        wanted = ["time", "open", "high", "low", "close", "tick_volume", "spread", "real_volume"]
        if not self.is_connected():
            return pd.DataFrame(columns=wanted)
        if connection is None:
            with self.engine.connect() as conn:
                return self.load_ohlcv(symbol, timeframe_name, start, end, connection=conn)
        table = self.find_table_for_symbol(symbol, timeframe_name, connection=connection)
        if table is None:
            return pd.DataFrame(columns=wanted)
        present = {c["name"] for c in inspect(connection).get_columns(table)}
        cols = [c for c in wanted if c in present]
        where, params = [], {}
        if start is not None:
            where.append("time >= :start")
            params["start"] = self._to_storage(pd.Timestamp(start)).strftime(self._TIME_FMT)
        if end is not None:
            where.append("time <= :end")
            params["end"] = self._to_storage(pd.Timestamp(end)).strftime(self._TIME_FMT)
        quoted = ", ".join(f'"{c}"' for c in cols)
        sql = f'SELECT {quoted} FROM {connection.dialect.identifier_preparer.quote(table)}'
        if where:
            sql += " WHERE " + " AND ".join(where)
        df = pd.read_sql(text(sql + " ORDER BY time"), connection, params=params)
        if not df.empty:
            df["time"] = self._from_storage(pd.to_datetime(df["time"]))
        return df

    def get_last_timestamp(self, table_name):
        """Maior valor de time da tabela (datetime) ou None se a tabela não existir ou estiver vazia."""
        if not self.is_connected() or not inspect(self.engine).has_table(table_name):
            return None
        with self.engine.connect() as conn:
            value = conn.execute(text(f'SELECT MAX(time) FROM "{table_name}"')).scalar()
        return self._from_storage(pd.to_datetime(value)).to_pydatetime() if value is not None else None

    def get_rows_before(self, table_name, before, limit, columns=None):
        """
        Últimas `limit` linhas com time < `before`, em ordem crescente de tempo.
        Retorna None se a tabela não existir. Usado para aquecer indicadores entre blocos.
        """
        if not inspect(self.engine).has_table(table_name):
            return None
        cols = ", ".join(f'"{c}"' for c in columns) if columns else "*"
        query = text(f'SELECT {cols} FROM "{table_name}" WHERE time < :t ORDER BY time DESC LIMIT :n')
        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"t": self._to_storage(pd.Timestamp(before)).strftime(self._TIME_FMT),
                                                  "n": int(limit)})
        if not df.empty and 'time' in df.columns:
            df['time'] = self._from_storage(pd.to_datetime(df['time']))
        return df.iloc[::-1].reset_index(drop=True)

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
            tables = self._data_tables()
            
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
                if not inspect(conn).has_table(table_name):
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
                # Texto no SQLite, datetime no PostgreSQL: converte (e volta para a base de tempo configurada)
                data_inicio = self._from_storage(pd.to_datetime(data_inicio)) if data_inicio is not None else None
                data_fim = self._from_storage(pd.to_datetime(data_fim)) if data_fim is not None else None
                
                # Verificar intervalo de tempo (média de tempo entre registros)
                if total_registros > 1 and data_inicio is not None and data_fim is not None:
                    # Média de tempo entre registros (para detectar o timeframe)
                    intervalo_medio_minutos = (data_fim - data_inicio).total_seconds() / 60 / (total_registros - 1)
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
    
    @staticmethod
    def _normalize_name(text):
        """Minúsculas; tudo que não é alfanumérico vira '_' (sem '_' repetidos nas pontas/meio)."""
        text = ''.join(c if c.isalnum() else '_' for c in str(text).lower())
        return '_'.join(filter(None, text.split('_')))

    def get_table_name_for_symbol(self, symbol, timeframe_name):
        """
        Retorna o nome da tabela para um símbolo e timeframe.

        O nome base é o mesmo de sempre (ex.: WIN$N + '1 minuto' -> win_n_1_minuto). Como símbolos
        diferentes podem gerar o mesmo nome (WIN$N e WIN_N), o par símbolo/timeframe é registrado em
        _symbol_tables; se o nome base já pertence a outro símbolo, acrescenta-se um sufixo de hash.
        Tabelas existentes nunca são renomeadas: o primeiro símbolo a usar o nome base fica com ele.
        """
        timeframe_key = self._normalize_name(timeframe_name)
        base = self._normalize_name(f"{symbol}_{timeframe_name}")
        key = (symbol, timeframe_key)
        if key in self._table_cache:
            return self._table_cache[key]
        if not self.is_connected():
            return base
        with self._table_lock:
            with self.engine.begin() as conn:
                row = conn.execute(text("SELECT table_name FROM _symbol_tables WHERE symbol = :s AND timeframe = :t"),
                                   {"s": symbol, "t": timeframe_key}).fetchone()
                if row:
                    name = row[0]
                else:
                    taken = conn.execute(text("SELECT 1 FROM _symbol_tables WHERE table_name = :n"),
                                         {"n": base}).fetchone()
                    name = base
                    if taken:
                        name = f"{base}_{hashlib.sha1(symbol.encode('utf-8')).hexdigest()[:6]}"
                        log.warning(f"Nome de tabela '{base}' já pertence a outro símbolo; '{symbol}' usará '{name}'.")
                    conn.execute(text("INSERT INTO _symbol_tables (symbol, timeframe, table_name) VALUES (:s, :t, :n)"),
                                 {"s": symbol, "t": timeframe_key, "n": name})
            self._table_cache[key] = name
        return name

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
                # VACUUM (SQLite) / VACUUM ANALYZE (PostgreSQL) não rodam dentro de transação
                conn = conn.execution_options(isolation_level="AUTOCOMMIT")
                log.info("Iniciando otimização de banco de dados (VACUUM)...")
                conn.execute(text("VACUUM" if self.dialect == 'sqlite' else "VACUUM ANALYZE"))
                log.info("Otimização VACUUM concluída.")

                # Cria índices para melhorar a performance de consultas por data
                indexed_tables = 0
                for table in self.get_existing_symbols():
                    try:
                        existing = {ix["name"] for ix in inspect(conn).get_indexes(table)}
                        if f"idx_{table}_time" not in existing:
                            log.info(f"Criando índice de tempo para a tabela {table}...")
                            conn.execute(text(f'CREATE INDEX IF NOT EXISTS "idx_{table}_time" ON "{table}" (time)'))
                            indexed_tables += 1
                    except Exception as idx_err:
                        log.warning(f"Erro ao criar índice para {table}: {idx_err}")

                log.info(f"Criados índices para {indexed_tables} tabelas.")
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
            tables = self._data_tables()
            log.info(f"Encontradas {len(tables)} tabelas no banco de dados.")
            return tables
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
            if not inspect(self.engine).has_table(table_name):
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
                # Garante que 'time' é datetime (na base de tempo configurada)
                if not self._check_time_basis():
                    return False
                df_to_save['time'] = self._to_storage(pd.to_datetime(df_to_save['time']))
                
                # Define 'time' como índice para a operação funcionar corretamente
                df_to_save = df_to_save.set_index('time')
            
            # Converte inteiros grandes para evitar erros de SQLite
            for col in df_to_save.select_dtypes(include=['int64', 'uint64']).columns:
                df_to_save[col] = df_to_save[col].astype('int32')
            
            # Salva no banco de dados (upsert quando a tabela já existe com PK 'time')
            if self._has_time_primary_key(table_name):
                self._upsert_dataframe(table_name, df_to_save)
            else:
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
                    # Mesmo formato de texto gravado na coluna time (comparação de texto no SQLite)
                    result = connection.execute(query, {
                        "start": self._to_storage(pd.Timestamp(start_date)).strftime(self._TIME_FMT),
                        "end": self._to_storage(pd.Timestamp(end_date)).strftime(self._TIME_FMT)})
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
