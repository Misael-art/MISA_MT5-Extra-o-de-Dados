import logging
import time
import traceback
import pandas as pd
from threading import Thread, Lock
from typing import Optional # Adicionado
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import MetaTrader5 as mt5 # Adicionado para constantes de timeframe

# Importar componentes necessários (ajustar caminhos se necessário)
from .mt5_connector import MT5Connector
from .database_manager import DatabaseManager
from .indicator_calculator import IndicatorCalculator
from .external_data_source import ExternalDataSource # Adicionado

log = logging.getLogger(__name__)

class HistoricalExtractor:
    """
    Responsável pela extração robusta e eficiente de dados históricos do MT5.
    Implementa extração em blocos, retries com backoff e paralelização.
    """
    def __init__(self, connector: MT5Connector, db_manager: DatabaseManager, indicator_calculator: IndicatorCalculator,
                 external_source: Optional[ExternalDataSource] = None,
                 chunk_config: Optional[dict] = None): # Adicionado chunk_config
        """
        Inicializa o extrator histórico.

        Args:
            connector: Instância de MT5Connector.
            db_manager: Instância de DatabaseManager.
            indicator_calculator: Instância de IndicatorCalculator.
            external_source (Optional[ExternalDataSource]): Instância opcional de uma fonte de dados externa para fallback M1.
            chunk_config (Optional[dict]): Dicionário com configuração de chunking (ex: {'m1': 30, 'm5_m15': 90, 'default': 365}).
        """
        self.connector = connector
        self.db_manager = db_manager
        self.indicator_calculator = indicator_calculator
        self.external_source = external_source
        # Define um chunk_config padrão se não for fornecido
        self.chunk_config = chunk_config or {'m1': 30, 'm5_m15': 90, 'default': 365}
        self.extraction_running = False
        self.cancel_requested = False
        self._lock = Lock() # Para controle de estado thread-safe

    def cancel_extraction(self):
        """Sinaliza o cancelamento da extração em andamento."""
        with self._lock:
            if self.extraction_running:
                log.info("Solicitação de cancelamento da extração histórica recebida.")
                self.cancel_requested = True

    def extract_data(self, symbols: list, timeframe_val: int, timeframe_name: str,
                     start_date: datetime, end_date: datetime,
                     include_indicators: bool, overwrite: bool,
                     max_workers: int = 4, # Número de workers paralelos
                     update_progress_callback=None, finished_callback=None):
        """
        Inicia a extração de dados históricos em uma thread separada.

        Args:
            symbols: Lista de símbolos para extrair.
            timeframe_val: Valor do timeframe MT5 (ex: mt5.TIMEFRAME_H1).
            timeframe_name: Nome descritivo do timeframe (ex: "1_hora").
            start_date: Data inicial da extração.
            end_date: Data final da extração.
            include_indicators: Se True, calcula indicadores técnicos.
            overwrite: Se True, deleta dados existentes antes de salvar.
            max_workers: Número máximo de threads para paralelização por símbolo.
            update_progress_callback: Função chamada para atualizar o progresso (progresso, mensagem).
            finished_callback: Função chamada ao finalizar (sucesso, falha).
        """
        with self._lock:
            if self.extraction_running:
                log.warning("Tentativa de iniciar extração histórica já em andamento.")
                return
            self.extraction_running = True
            self.cancel_requested = False

        log.info(f"Iniciando extração histórica para {len(symbols)} símbolos com {max_workers} workers...")
        log.info(f"Período: {start_date} a {end_date}, Timeframe: {timeframe_name}, Indicadores: {include_indicators}, Sobrescrever: {overwrite}")

        # Iniciar a thread principal de extração
        extraction_thread = Thread(target=self._run_extraction,
                                   args=(symbols, timeframe_val, timeframe_name, start_date, end_date,
                                         include_indicators, overwrite, max_workers,
                                         update_progress_callback, finished_callback))
        extraction_thread.daemon = True
        extraction_thread.start()

    def _run_extraction(self, symbols: list, timeframe_val: int, timeframe_name: str,
                        start_date: datetime, end_date: datetime,
                        include_indicators: bool, overwrite: bool, max_workers: int,
                        update_progress_callback=None, finished_callback=None):
        """Lógica principal da extração executada na thread."""
        total_symbols = len(symbols)
        successful_symbols = 0
        failed_symbols = 0
        results = {} # Armazena o resultado por símbolo (sucesso/falha/cancelado)

        try:
            # Usar ThreadPoolExecutor para paralelizar por símbolo
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(self._process_symbol, symbol, timeframe_val, timeframe_name,
                                           start_date, end_date, include_indicators, overwrite): symbol
                           for symbol in symbols}

                processed_count = 0
                for future in as_completed(futures):
                    symbol = futures[future]
                    processed_count += 1
                    progress = (processed_count / total_symbols) * 100

                    if self.cancel_requested:
                        log.info(f"Cancelamento detectado durante processamento de {symbol}. Tentando cancelar futuros...")
                        # Tentar cancelar futuros restantes (pode não funcionar se já iniciaram)
                        for f in futures:
                            if not f.done():
                                f.cancel()
                        results[symbol] = "cancelado" # Marca o atual como cancelado se ainda não terminou
                        # Os que já terminaram terão seu resultado preservado
                        continue # Pula para o próximo futuro concluído

                    try:
                        success = future.result() # Obtém o resultado (True/False)
                        if success:
                            successful_symbols += 1
                            results[symbol] = "sucesso"
                            message = f"Sucesso: {symbol} ({processed_count}/{total_symbols})"
                        else:
                            failed_symbols += 1
                            results[symbol] = "falha"
                            message = f"Falha: {symbol} ({processed_count}/{total_symbols})"
                        log.info(message)
                        if update_progress_callback:
                            update_progress_callback(progress, message)
                    except Exception as exc:
                        failed_symbols += 1
                        results[symbol] = "erro"
                        message = f"Erro ao processar {symbol}: {exc} ({processed_count}/{total_symbols})"
                        log.error(message)
                        log.debug(traceback.format_exc())
                        if update_progress_callback:
                            update_progress_callback(progress, message)

        except Exception as e:
            log.error(f"Erro inesperado no pool de extração: {e}")
            log.debug(traceback.format_exc())
            # Marcar todos os símbolos restantes como falha em caso de erro no pool
            failed_symbols = total_symbols - successful_symbols

        finally:
            # Contabilizar cancelados se houver
            canceled_symbols = sum(1 for res in results.values() if res == "cancelado")
            # Ajustar contagem de falhas se houve cancelamento
            if canceled_symbols > 0:
                 failed_symbols = total_symbols - successful_symbols - canceled_symbols

            final_message = f"Extração concluída. Sucesso: {successful_symbols}, Falhas: {failed_symbols}, Cancelados: {canceled_symbols}"
            log.info(final_message)
            if update_progress_callback:
                update_progress_callback(100, final_message)
            if finished_callback:
                finished_callback(successful_symbols, failed_symbols, canceled_symbols)

            with self._lock:
                self.extraction_running = False
                self.cancel_requested = False # Resetar estado

    def _process_symbol(self, symbol: str, timeframe_val: int, timeframe_name: str,
                        start_date: datetime, end_date: datetime,
                        include_indicators: bool, overwrite: bool) -> bool:
        """
        Processa a extração de dados para um único símbolo, incluindo blocos e retries.

        Returns:
            bool: True se a extração para este símbolo foi bem-sucedida, False caso contrário.
        """
        log.info(f"Iniciando processamento para {symbol}...")

        # 1. Lógica de Sobrescrita (Overwrite) - Chamada ANTES de buscar blocos
        if overwrite:
            try:
                # Obter o nome normalizado da tabela
                table_name = self.db_manager.get_table_name_for_symbol(symbol, timeframe_name)
                log.info(f"[{symbol}] Opção 'Sobrescrever' ativa. Deletando dados existentes da tabela '{table_name}' entre {start_date.date()} e {end_date.date()}...")
                
                # Chamar o método para deletar os dados no período
                delete_success = self.db_manager.delete_data_periodo(table_name, start_date, end_date)
                
                if not delete_success:
                    # Log já é feito dentro de delete_data_periodo em caso de erro SQL
                    log.warning(f"[{symbol}] Falha ao deletar dados antigos da tabela '{table_name}'. A extração continuará, mas pode haver duplicatas.")
                    # Não retorna False aqui para permitir que a extração continue mesmo se a deleção falhar
            except Exception as del_err:
                log.error(f"[{symbol}] Erro inesperado durante a operação de sobrescrita (delete): {del_err}")
                log.debug(traceback.format_exc())
                # Considerar se a extração deve parar aqui. Por ora, continua.

        # 2. Lógica de Extração em Blocos (Chunking Dinâmico)
        # Define o tamanho do bloco com base no timeframe e na configuração
        if timeframe_val == mt5.TIMEFRAME_M1:
            chunk_days = self.chunk_config.get('m1', 30) # Padrão 30 dias para M1
        elif timeframe_val in [mt5.TIMEFRAME_M5, mt5.TIMEFRAME_M15]:
            chunk_days = self.chunk_config.get('m5_m15', 90) # Padrão 90 dias para M5/M15
        else:
            chunk_days = self.chunk_config.get('default', 365) # Padrão 365 dias para outros

        block_delta = timedelta(days=chunk_days)
        log.info(f"[{symbol}] Usando blocos de {chunk_days} dias para timeframe {timeframe_name}.")
        current_start = start_date
        all_symbol_data = []
        symbol_success = True # Assume sucesso até que um bloco falhe

        while current_start < end_date:
            if self.cancel_requested:
                log.info(f"[{symbol}] Cancelamento solicitado durante processamento de blocos.")
                return False # Indica falha devido ao cancelamento

            block_end = min(current_start + block_delta, end_date)
            log.debug(f"[{symbol}] Processando bloco: {current_start.date()} a {block_end.date()}")

            # 3. Lógica de Retry com Backoff para buscar o bloco
            max_retries = 3
            retry_delay = 1 # segundos iniciais
            block_df = None

            for attempt in range(max_retries):
                 if self.cancel_requested: return False # Verifica cancelamento antes de cada tentativa

                 try:
                     # Usar copy_rates_from para M1 (via bars) e copy_rates_range para outros
                     if timeframe_val == mt5.TIMEFRAME_M1:
                         # Solicitar um número grande de barras a partir da data inicial
                         # A API MT5 limitará ao máximo disponível se 200k for excessivo
                         bars_to_request = 200000
                         block_df = self.connector.get_historical_data(
                             symbol,
                             timeframe_val,
                             start_dt=current_start,
                             bars=bars_to_request,
                             end_dt=None # Força o uso de copy_rates_from ou copy_rates_from_pos
                         )
                         # Filtrar dados que podem vir antes de current_start se copy_rates_from_pos for usado
                         if block_df is not None and not block_df.empty:
                              block_df = block_df[block_df['time'] >= current_start]
                         # Filtrar dados que podem vir depois de block_end (menos provável com _from/_from_pos)
                         if block_df is not None and not block_df.empty:
                              block_df = block_df[block_df['time'] <= block_end]
                     else:
                         # Para outros timeframes, usar o range
                         block_df = self.connector.get_historical_data(
                             symbol,
                             timeframe_val,
                             start_dt=current_start,
                             end_dt=block_end,
                             bars=None
                         )

                     if block_df is not None: # Pode retornar DataFrame vazio se não houver dados, o que não é erro
                         log.debug(f"[{symbol}] Bloco {current_start.date()}-{block_end.date()} obtido com {len(block_df)} barras.")
                         break # Sucesso, sai do loop de retry
                     else:
                         # Se retornou None, é um erro na API/Conexão
                         error = self.connector.mt5.last_error() if hasattr(self.connector, 'mt5') else "N/A"
                         log.warning(f"[{symbol}] Tentativa {attempt+1}/{max_retries}: Falha ao obter bloco {current_start.date()}-{block_end.date()}. Erro MT5: {error}")

                 except Exception as e:
                     log.warning(f"[{symbol}] Tentativa {attempt+1}/{max_retries}: Exceção ao obter bloco {current_start.date()}-{block_end.date()}: {e}")
                     log.debug(traceback.format_exc())

                 # Espera antes da próxima tentativa (backoff exponencial simples)
                 if attempt < max_retries - 1:
                     log.info(f"[{symbol}] Aguardando {retry_delay}s antes da próxima tentativa...")
                     time.sleep(retry_delay)
                     retry_delay *= 2 # Dobra o tempo de espera

            # Verifica se o bloco foi obtido com sucesso após retries via MT5
            if block_df is None:
                log.warning(f"[{symbol}] Falha ao obter bloco {current_start.date()}-{block_end.date()} via MT5 após {max_retries} tentativas.") # Mudado para warning

                # --- INÍCIO: LÓGICA DE FALLBACK PARA M1 ---
                if timeframe_val == mt5.TIMEFRAME_M1 and self.external_source and self.external_source.is_configured():
                    log.info(f"[{symbol}] Tentando fallback com fonte externa ({self.external_source.__class__.__name__}) para o bloco M1 {current_start.date()}-{block_end.date()}...")
                    try:
                        external_block_df = self.external_source.get_historical_m1_data(symbol, current_start, block_end)
                        if external_block_df is not None:
                            # Validação básica das colunas esperadas (ajustar se necessário)
                            expected_cols = {'time', 'open', 'high', 'low', 'close', 'real_volume'}
                            if expected_cols.issubset(external_block_df.columns):
                                log.info(f"[{symbol}] Fallback bem-sucedido! Obtido {len(external_block_df)} barras M1 da fonte externa.")
                                block_df = external_block_df # Usa os dados do fallback
                            else:
                                log.warning(f"[{symbol}] Fonte externa ({self.external_source.__class__.__name__}) retornou dados M1, mas colunas esperadas ({expected_cols}) não encontradas. Ignorando fallback.")
                                block_df = None # Garante que block_df permaneça None
                        else:
                            log.warning(f"[{symbol}] Fonte externa ({self.external_source.__class__.__name__}) não retornou dados (None) para o bloco M1.")
                            # block_df continua None
                    except Exception as ext_err:
                        log.error(f"[{symbol}] Erro ao tentar buscar dados M1 da fonte externa ({self.external_source.__class__.__name__}): {ext_err}")
                        log.debug(traceback.format_exc())
                        # block_df continua None
                # --- FIM: LÓGICA DE FALLBACK PARA M1 ---

                # Se ainda for None após tentativa de fallback (ou se não era M1/sem fallback), marca falha definitiva
                if block_df is None:
                    log.error(f"[{symbol}] Falha definitiva ao obter bloco {current_start.date()}-{block_end.date()}.")
                    symbol_success = False
                    break # Falha em um bloco, interrompe a extração para este símbolo

            # Adiciona dados do bloco (se não estiver vazio)
            if not block_df.empty:
                all_symbol_data.append(block_df)

            # Avança para o próximo bloco
            # Adiciona 1 segundo para evitar sobreposição exata se MT5 incluir a data final
            current_start = block_end + timedelta(seconds=1)

        # 4. Consolidação e Salvamento dos Dados do Símbolo (se todos os blocos tiveram sucesso)
        if symbol_success and all_symbol_data:
            final_df = pd.concat(all_symbol_data, ignore_index=True)
            final_df = final_df.drop_duplicates(subset=['time'], keep='first').sort_values(by='time') # Garante unicidade e ordem
            log.info(f"[{symbol}] Total de {len(final_df)} barras únicas obtidas após concatenação dos blocos.")

            # Calcular indicadores se solicitado
            if include_indicators:
                try:
                    log.debug(f"[{symbol}] Calculando indicadores...")
                    final_df = self.indicator_calculator.calculate_technical_indicators(final_df)
                    # Adicionar outros cálculos se necessário (ex: spread simulado)
                    symbol_info = self.connector.get_symbol_info(symbol)
                    if symbol_info:
                        final_df['spread'] = symbol_info.spread # Spread atual, não histórico
                except Exception as ind_err:
                    log.error(f"[{symbol}] Erro ao calcular indicadores: {ind_err}")
                    # Decide se continua sem indicadores ou falha
                    # Por enquanto, continua sem indicadores

            # Salvar no banco de dados
            try:
                log.debug(f"[{symbol}] Salvando {len(final_df)} barras no banco de dados...")
                # Usar save_ohlcv_data que pode ser mais otimizado
                saved = self.db_manager.save_ohlcv_data(symbol, timeframe_name, final_df)
                if saved:
                    log.info(f"[{symbol}] Dados salvos com sucesso no banco.")
                    return True # Sucesso para este símbolo
                else:
                    log.error(f"[{symbol}] Falha ao salvar dados no banco (método save_ohlcv_data retornou False).")
                    return False # Falha para este símbolo
            except Exception as db_err:
                log.error(f"[{symbol}] Erro ao salvar dados no banco: {db_err}")
                log.debug(traceback.format_exc())
                return False # Falha para este símbolo

        elif symbol_success and not all_symbol_data:
            log.info(f"[{symbol}] Nenhum dado encontrado no período solicitado após processar todos os blocos.")
            return True # Considera sucesso, pois não houve erro, apenas sem dados
        else:
            # Se symbol_success é False, significa que um bloco falhou
            log.error(f"[{symbol}] Extração falhou devido a erro em um dos blocos.")
            return False # Falha para este símbolo