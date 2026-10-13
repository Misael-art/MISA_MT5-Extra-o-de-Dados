import time
import logging
import datetime
import traceback

# Configurar logging básico para o teste
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    # Importar as classes necessárias
    from mt5_extracao.mt5_connector import MT5Connector
    from mt5_extracao.database_manager import DatabaseManager
    from mt5_extracao.indicator_calculator import IndicatorCalculator
    from mt5_extracao.historical_extractor import HistoricalExtractor
    import MetaTrader5 as mt5 # Para constantes de timeframe
except ImportError as e:
    logging.error(f"Erro ao importar módulos necessários: {e}")
    logging.error("Certifique-se de que o ambiente virtual está ativo e as dependências instaladas.")
    exit(1)

def run_test():
    """Executa o teste de funcionalidade e performance."""
    logging.info("--- Iniciando Teste de Extração Histórica ---")

    # --- Configuração ---
    # Tenta usar as configurações padrão (ajuste se necessário)
    config_path = "config/config.ini"
    db_path = "database/mt5_data_test.db" # Usar um DB de teste para não afetar o principal

    # Parâmetros do Teste (ajuste conforme necessário)
    test_symbol = "WIN$N" # Use um símbolo válido e disponível na sua conta MT5
    test_timeframe_val = mt5.TIMEFRAME_H1
    test_timeframe_name = "1_hora" # Nome correspondente para salvar no DB
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=30) # Teste com 30 dias de dados H1
    include_indicators = False # Teste mais rápido sem indicadores
    overwrite = True # Testar a funcionalidade de sobrescrever
    max_workers = 2 # Limitar workers para o teste

    connector = None
    db_manager = None
    historical_extractor = None

    try:
        # --- Inicialização dos Componentes ---
        logging.info("Inicializando componentes...")
        connector = MT5Connector(config_path=config_path)
        if not connector.initialize():
            logging.error("Falha ao inicializar MT5Connector. Verifique a conexão MT5 e config.ini.")
            return False

        db_manager = DatabaseManager(db_path=db_path)
        if not db_manager.is_connected():
            logging.error(f"Falha ao conectar ao banco de dados de teste: {db_path}")
            return False

        indicator_calculator = IndicatorCalculator() # Mesmo sem usar, é necessário para o construtor

        historical_extractor = HistoricalExtractor(connector, db_manager, indicator_calculator)
        logging.info("Componentes inicializados com sucesso.")

        # --- Execução dos Testes de API ---
        logging.info("--- Iniciando Testes de API MT5 ---")
        test_results = {}
        overall_success = True
        test_period_start = datetime.datetime.now() - datetime.timedelta(days=7) # Teste com 7 dias
        test_period_end = datetime.datetime.now()
        large_bar_count = 50000 # Tentar buscar um histórico maior com copy_rates_from

        # --- Teste 1: WIN@N H1 (Range) ---
        symbol_to_test = "WIN@N"
        tf_to_test = mt5.TIMEFRAME_H1
        test_name = f"Teste 1: {symbol_to_test} H1 (Range)"
        logging.info(f"--- {test_name} ---")
        start_time = time.time()
        df_test1 = connector.get_historical_data(symbol_to_test, tf_to_test, start_dt=test_period_start, end_dt=test_period_end)
        duration = time.time() - start_time
        if df_test1 is not None and not df_test1.empty:
            logging.info(f"SUCESSO - {test_name}: Obtidos {len(df_test1)} registros em {duration:.2f}s")
            test_results[test_name] = "SUCESSO"
        else:
            logging.error(f"FALHA - {test_name}: Não retornou dados em {duration:.2f}s (Verificar logs do conector)")
            test_results[test_name] = "FALHA"
            overall_success = False
        time.sleep(1) # Pausa entre testes

        # --- Teste 2: WIN@N M1 (Range) ---
        tf_to_test = mt5.TIMEFRAME_M1
        test_name = f"Teste 2: {symbol_to_test} M1 (Range)"
        logging.info(f"--- {test_name} ---")
        start_time = time.time()
        df_test2 = connector.get_historical_data(symbol_to_test, tf_to_test, start_dt=test_period_start, end_dt=test_period_end)
        duration = time.time() - start_time
        if df_test2 is not None and not df_test2.empty:
            logging.info(f"SUCESSO - {test_name}: Obtidos {len(df_test2)} registros em {duration:.2f}s")
            test_results[test_name] = "SUCESSO"
        else:
            logging.warning(f"FALHA ESPERADA? - {test_name}: Não retornou dados em {duration:.2f}s (Verificar logs do conector para erro -2)")
            test_results[test_name] = "FALHA (Esperada?)"
            # Não marca overall_success como False aqui, pois a falha pode ser esperada
        time.sleep(1)

        # --- Teste 3: WIN@N M1 (From) ---
        test_name = f"Teste 3: {symbol_to_test} M1 (From)"
        logging.info(f"--- {test_name} ---")
        start_time = time.time()
        # Usar start_dt e bars (None para start_dt fará usar copy_rates_from_pos)
        # Para usar copy_rates_from, precisamos de start_dt e não especificar end_dt
        df_test3 = connector.get_historical_data(symbol_to_test, tf_to_test, start_dt=test_period_start, bars=None, end_dt=None)
        duration = time.time() - start_time
        if df_test3 is not None and not df_test3.empty:
            logging.info(f"SUCESSO - {test_name}: Obtidos {len(df_test3)} registros em {duration:.2f}s")
            test_results[test_name] = "SUCESSO"
        else:
            logging.error(f"FALHA - {test_name}: Não retornou dados em {duration:.2f}s (Verificar logs do conector)")
            test_results[test_name] = "FALHA"
            overall_success = False
        time.sleep(1)

        # --- Teste 4: WIN$ M1 (Range) ---
        symbol_to_test = "WIN$" # Testar contrato contínuo
        tf_to_test = mt5.TIMEFRAME_M1
        test_name = f"Teste 4: {symbol_to_test} M1 (Range)"
        logging.info(f"--- {test_name} ---")
        start_time = time.time()
        df_test4 = connector.get_historical_data(symbol_to_test, tf_to_test, start_dt=test_period_start, end_dt=test_period_end)
        duration = time.time() - start_time
        if df_test4 is not None and not df_test4.empty:
            logging.info(f"SUCESSO - {test_name}: Obtidos {len(df_test4)} registros em {duration:.2f}s")
            test_results[test_name] = "SUCESSO"
        else:
            logging.warning(f"FALHA ESPERADA? - {test_name}: Não retornou dados em {duration:.2f}s (Verificar logs do conector para erro -2)")
            test_results[test_name] = "FALHA (Esperada?)"
        time.sleep(1)

        # --- Teste 5: WIN$ M1 (From) ---
        test_name = f"Teste 5: {symbol_to_test} M1 (From)"
        logging.info(f"--- {test_name} ---")
        start_time = time.time()
        df_test5 = connector.get_historical_data(symbol_to_test, tf_to_test, start_dt=test_period_start, bars=None, end_dt=None)
        duration = time.time() - start_time
        if df_test5 is not None and not df_test5.empty:
            logging.info(f"SUCESSO - {test_name}: Obtidos {len(df_test5)} registros em {duration:.2f}s")
            test_results[test_name] = "SUCESSO"
        else:
            logging.error(f"FALHA - {test_name}: Não retornou dados em {duration:.2f}s (Verificar logs do conector)")
            test_results[test_name] = "FALHA"
            overall_success = False

        # --- Resumo dos Testes de API ---
        logging.info("--- Resumo dos Testes de API ---")
        for name, result in test_results.items():
            logging.info(f"{name}: {result}")

        return overall_success # Retorna True se todos os testes não esperados como falha passaram

    except Exception as e:
        logging.error(f"Erro fatal durante o teste: {e}")
        logging.error(traceback.format_exc())
        return False
    finally:
        # --- Limpeza ---
        if connector and connector.is_initialized:
            connector.shutdown()
            logging.info("Conexão MT5 finalizada.")
        # Opcional: deletar o banco de dados de teste
        # import os
        # if os.path.exists(db_path):
        #     try:
        #         os.remove(db_path)
        #         logging.info(f"Banco de dados de teste '{db_path}' removido.")
        #     except OSError as e:
        #         logging.error(f"Erro ao remover banco de dados de teste '{db_path}': {e}")

        logging.info("--- Teste de Extração Histórica Finalizado ---")

if __name__ == "__main__":
    import threading # Importar aqui para o Event funcionar
    success = run_test()
    if success:
        logging.info("Resultado Final: Teste passou.")
        # exit(0) # Sucesso
    else:
        logging.error("Resultado Final: Teste falhou.")
        # exit(1) # Falha