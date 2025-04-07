import os
import sys
import logging
import configparser
import time
import traceback
import datetime
import pandas as pd
from tkinter import messagebox  # Temporário? Idealmente, remover dependência da UI.
import json
import subprocess
from pathlib import Path

# Import dos módulos criados
from mt5_extracao.security import CredentialManager
from mt5_extracao.error_handler import (
    with_error_handling, 
    check_mt5_error, 
    MT5Error, 
    MT5ConnectionError, 
    MT5IPCError
)

try:
    import MetaTrader5 as mt5
except ImportError:
    logging.error("Módulo MetaTrader5 não encontrado. Instale-o com: pip install MetaTrader5")
    mt5 = None

try:
    import psutil
except ImportError:
    logging.warning("Módulo psutil não encontrado. Verificação de processo MT5 desativada.")
    psutil = None

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
    fh = logging.FileHandler("logs/mt5_connector.log", encoding="utf-8")
    fh.setFormatter(formatter)
    log.addHandler(fh)

DEFAULT_CONFIG_PATH = "config/config.ini"

class MT5Connector:
    """
    Gerencia a conexão com a plataforma MetaTrader 5.
    """
    def __init__(self, config_path=DEFAULT_CONFIG_PATH):
        self.config_path = config_path
        self.mt5_path = None
        self.is_initialized = False
        self.connection_mode = "Desconectado" # Ex: Conectado, Compatibilidade, Limitado, Fallback
        self._load_config()

    def _load_config(self):
        """Carrega o caminho do MT5 do arquivo de configuração."""
        config = configparser.ConfigParser()
        if not os.path.exists(self.config_path):
            log.error(f"Arquivo de configuração não encontrado em: {self.config_path}")
            # Idealmente, lançar uma exceção aqui ou retornar um estado de erro
            return
        try:
            config.read(self.config_path)
            self.mt5_path = config.get('MT5', 'path', fallback=None)
            if not self.mt5_path:
                log.error("Caminho do MT5 não definido no arquivo de configuração.")
            elif not os.path.exists(self.mt5_path):
                log.error(f"Caminho do MT5 configurado não existe: {self.mt5_path}")
                self.mt5_path = None # Invalida o caminho se não existir
        except Exception as e:
            log.error(f"Erro ao ler configuração do MT5: {e}")
            log.debug(traceback.format_exc())
            self.mt5_path = None

    def _is_mt5_running(self):
        """
        Verifica se o processo terminal64.exe está em execução.
        Utiliza múltiplas abordagens para maior confiabilidade.
        
        Returns:
            bool: True se o MT5 está em execução, False caso contrário
        """
        if not psutil:
            log.warning("psutil não disponível, usando método alternativo para verificar se MT5 está em execução.")
            try:
                # Tenta usar o comando tasklist como alternativa
                result = subprocess.run(["tasklist", "/FI", "IMAGENAME eq terminal64.exe"], 
                                        capture_output=True, text=True)
                return "terminal64.exe" in result.stdout
            except Exception as e:
                log.error(f"Erro ao verificar processo MT5 via tasklist: {e}")
                return False  # Assume que não está rodando em caso de erro
                
        try:
            # Primeira abordagem: verificar por processo terminal64.exe via psutil
            for proc in psutil.process_iter(['name', 'exe']):
                try:
                    # Verifica tanto o nome quanto o caminho do executável
                    if (proc.info['name'] and 'terminal64.exe' in proc.info['name'].lower()) or \
                       (proc.info['exe'] and 'terminal64.exe' in proc.info['exe'].lower()):
                        log.info("Processo terminal64.exe encontrado em execução.")
                        return True
                        
                    # Também verifica por alternativas como metatrader5.exe
                    if (proc.info['name'] and 'metatrader5' in proc.info['name'].lower()) or \
                       (proc.info['exe'] and 'metatrader5' in proc.info['exe'].lower()):
                        log.info("Processo metatrader5 encontrado em execução.")
                        return True
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    continue
                    
            # Segunda abordagem: verificar conexão ao MT5 via API
            if mt5:
                try:
                    # Verificar se consegue obter informações básicas do terminal
                    # como forma de verificar se o MT5 está disponível
                    # Usamos uma função que sabemos que existe na biblioteca
                    terminal_info = getattr(mt5, 'terminal_info', None)
                    if terminal_info and terminal_info():
                        log.info("MT5 está disponível via API.")
                        return True
                except Exception as e:
                    log.debug(f"Erro ao verificar disponibilidade do MT5 via API: {e}")
            
            # Terceira abordagem: verificar portas de comunicação do MT5
            try:
                for conn in psutil.net_connections():
                    # MT5 normalmente usa portas específicas para comunicação
                    if conn.laddr.port in [443, 8222, 8228]:
                        log.info(f"Detectada conexão em porta típica do MT5: {conn.laddr.port}")
                        return True
            except (psutil.AccessDenied, Exception) as e:
                log.debug(f"Erro ao verificar conexões de rede do MT5: {e}")
            
            # Não encontrou o processo
            log.info("Processo terminal64.exe não encontrado em execução.")
            return False
            
        except Exception as e:
            log.error(f"Erro ao verificar processo do MT5: {e}")
            return False  # Assume que não está rodando em caso de erro

    @with_error_handling(error_type=MT5ConnectionError)
    def initialize(self, mt5_path=None, auto_login=True, force_restart=False, recursion_count=0):
        """
        Inicializa a conexão com o MetaTrader 5.
        
        Args:
            mt5_path (str, optional): Caminho para o terminal64.exe. Se None, usa config.ini
            auto_login (bool): Se True, tenta fazer login com credenciais (se disponíveis)
            force_restart (bool): Se True, força reinicialização mesmo se já estiver conectado
            recursion_count (int): Contador de recursão para evitar chamadas infinitas
            
        Returns:
            bool: True se a inicialização for bem-sucedida
            
        Raises:
            MT5ConnectionError: Se não for possível inicializar a conexão
        """
        # Evitar recursão infinita
        if recursion_count > 2:  # Limitar a 3 tentativas (0, 1, 2)
            log.error("Limite de recursão atingido em initialize. Abortando.")
            return False
            
        # Verifica se o módulo MT5 está disponível
        if not mt5:
            log.error("Módulo MetaTrader5 não está disponível ou não foi importado corretamente")
            return False
            
        # Verifica o caminho do MT5
        if not mt5_path:
            mt5_path = self.mt5_path
        if not mt5_path:
            log.error("Caminho do MT5 não configurado")
            return False
            
        if not os.path.exists(mt5_path):
            log.error(f"Caminho configurado para o MT5 não existe: {mt5_path}")
            return False
            
        # Verifica se o MT5 já está em execução
        is_running = self._is_mt5_running()
        log.info(f"MT5 está em execução? {is_running}")
        
        # Se não estiver rodando ou forçar reinício, tenta iniciar
        if not is_running:
            if self._start_mt5_if_not_running(recursion_count):
                is_running = True
        
        # Verifica se está rodando como administrador (apenas uma vez)
        if is_running:
            # Lista de estratégias de conexão para tentar
            connection_strategies = [
                # Estratégia 1: Conexão padrão
                {"description": "Padrão", "params": {"path": mt5_path, "timeout": 30000}},
                
                # Estratégia 2: Modo portátil
                {"description": "Portátil", "params": {"path": mt5_path, "timeout": 30000, "portable": True}},
                
                # Estratégia 3: Servidor local
                {"description": "Servidor local", "params": {"server": "127.0.0.1", "timeout": 30000}},
                
                # Estratégia 4: Apenas timeout maior
                {"description": "Timeout longo", "params": {"timeout": 60000}},
                
                # Estratégia 5: Caminho alternativo (pasta pai)
                {"description": "Caminho pai", "params": {"path": os.path.dirname(mt5_path), "timeout": 30000}},
                
                # Estratégia 6: Caminho direto para terminal64.exe
                {"description": "Terminal direto", "params": {"path": os.path.join(mt5_path, "terminal64.exe"), "timeout": 30000}}
            ]
            
            # Tenta cada estratégia até que uma funcione
            for strategy in connection_strategies:
                try:
                    log.info(f"Tentando estratégia de conexão: {strategy['description']}")
                    # Desconecta primeiro para garantir uma inicialização limpa
                    try:
                        if hasattr(mt5, 'shutdown'):
                            mt5.shutdown()
                            time.sleep(1)
                    except:
                        pass
                    
                    # Tenta inicializar com a estratégia atual
                    result = mt5.initialize(**strategy["params"])
                    
                    if result:
                        # Testa a conexão tentando acessar alguma funcionalidade básica
                        try:
                            account_info = mt5.account_info()
                            if account_info is not None:
                                log.info(f"Conexão confirmada, informações da conta obtidas")
                            else:
                                log.warning("Inicialização bem-sucedida, mas falha ao obter dados da conta")
                        except Exception as account_err:
                            log.warning(f"Inicialização bem-sucedida, mas erro ao verificar conta: {account_err}")
                            
                        # Verifica se consegue obter símbolos também
                        try:
                            symbols = mt5.symbols_get()
                            if symbols:
                                log.info(f"Acesso a {len(symbols)} símbolos confirmado")
                        except Exception as symbols_err:
                            log.warning(f"Erro ao verificar símbolos: {symbols_err}")
                            
                        log.info(f"Conexão com MT5 inicializada com sucesso (estratégia: {strategy['description']})")
                        self.is_initialized = True
                        self.connection_mode = f"Conectado ({strategy['description']})"
                        return True
                    else:
                        # Obtém e registra detalhes do erro
                        error = mt5.last_error()
                        error_description = f"Código do erro: {error[0]}, Descrição: {error[1]}"
                        log.error(f"Falha na estratégia {strategy['description']}: {error_description}")
                        
                        # Verifica se é o erro IPC específico (código -10003)
                        if error[0] == -10003 and "IPC initialize failed" in error[1]:
                            log.warning("Detectado erro IPC específico. Tentando resolver...")
                            # Tenta corrigir o erro IPC
                            if self._fix_ipc_error():
                                log.info("Erro IPC corrigido com sucesso!")
                                self.is_initialized = True
                                self.connection_mode = "Conectado (Após correção IPC)"
                                return True
                except Exception as e:
                    log.error(f"Exceção na estratégia {strategy['description']}: {e}")
                    log.debug(traceback.format_exc())
                
                # Pausa breve entre tentativas
                time.sleep(0.5)
            
            # Se chegou aqui, todas as estratégias falharam
            log.error("Todas as estratégias de conexão falharam")
        else:
            log.error("MT5 não está em execução após tentativas de inicialização")
        
        # Se chegou aqui, não conseguiu inicializar
        self.is_initialized = False
        self.connection_mode = "Erro de Conexão"
        return False

    def get_symbols(self, group="*"):
        """
        Retorna uma lista de objetos de símbolos disponíveis no MT5.
        Opcionalmente filtrando por grupo.
        
        Args:
            group (str): Grupo de símbolos (ex: "*", "FX*", etc.)
            
        Returns:
            list: Lista de objetos symbol_info ou None em caso de erro
        """
        if not self.is_initialized:
            log.error("MT5 não inicializado ao tentar obter símbolos")
            return None
            
        try:
            log.info(f"Tentando obter símbolos do grupo: '{group}'")
            symbols = mt5.symbols_get(group)
            
            if not symbols or len(symbols) == 0:
                log.warning(f"Nenhum símbolo retornado para o grupo: '{group}'")
                
                # Verificação detalhada do status MT5
                self._log_mt5_status()
                
                # Tentar métodos alternativos para obter símbolos
                log.info("TENTATIVA ALTERNATIVA 1: Obtendo símbolos sem especificar grupo...")
                alt_symbols = mt5.symbols_get()
                if alt_symbols and len(alt_symbols) > 0:
                    log.info(f"SUCESSO ALTERNATIVO 1: {len(alt_symbols)} símbolos obtidos sem especificar grupo")
                    return alt_symbols
                else:
                    log.warning("FALHA ALTERNATIVA 1: Tentativa de obter símbolos sem grupo também falhou")
                    
                # Tentar alguns grupos específicos comuns
                common_groups = ["FX*", "FOREX*", "Forex*", "CRYPTO*", "Crypto*", "FUTURES*", "Futures*", "*USD*", "B3*", "*Shares*", "*Índices*"]
                for i, alt_group in enumerate(common_groups):
                    log.info(f"TENTATIVA ALTERNATIVA {i+2}: Obtendo símbolos do grupo '{alt_group}'...")
                    alt_symbols = mt5.symbols_get(alt_group)
                    if alt_symbols and len(alt_symbols) > 0:
                        log.info(f"SUCESSO ALTERNATIVO {i+2}: Grupo '{alt_group}' retornou {len(alt_symbols)} símbolos")
                        return alt_symbols
                    else:
                        log.warning(f"FALHA ALTERNATIVA {i+2}: Grupo '{alt_group}' não retornou símbolos")
                
                # Tentar obter os símbolos visíveis na Market Watch
                log.info("TENTATIVA ALTERNATIVA MERCADO: Obtendo símbolos visíveis na Market Watch...")
                watch_symbols = mt5.symbols_get(group="#")
                if watch_symbols and len(watch_symbols) > 0:
                    log.info(f"SUCESSO ALTERNATIVO MERCADO: Obtidos {len(watch_symbols)} símbolos da Market Watch")
                    return watch_symbols
                else:
                    log.warning("FALHA ALTERNATIVA MERCADO: Não foi possível obter símbolos da Market Watch")
                
                # Último recurso: criar símbolos a partir de uma lista fixa mais abrangente
                fallback_symbols = [
                    "EURUSD", "USDJPY", "GBPUSD", "AUDUSD", "USDCAD", "USDCHF", "NZDUSD", 
                    "EURGBP", "EURJPY", "WIN$N", "WDO$N", "DOL$N", "IND$N", "BTCUSD", 
                    "PETR4", "VALE3", "ITUB4", "BBDC4", "B3SA3", "ABEV3", "GGBR4"
                ]
                log.info("TENTATIVA ÚLTIMA CHANCE: Obtendo informações de símbolos comuns individualmente...")
                manual_symbols = []
                for sym in fallback_symbols:
                    info = mt5.symbol_info(sym)
                    if info is not None:
                        manual_symbols.append(info)
                        log.info(f"Símbolo adicionado manualmente: {sym}")
                
                if manual_symbols and len(manual_symbols) > 0:
                    log.info(f"SUCESSO ÚLTIMA CHANCE: Obtidos {len(manual_symbols)} símbolos pelo método de fallback individual")
                    return manual_symbols
                
                log.error("FALHA CRÍTICA: Todos os métodos para obter símbolos falharam!")
                
                # Tentar fornecer informações sobre os erros do MT5
                last_error = mt5.last_error()
                if last_error:
                    log.error(f"Último erro MT5: Código {last_error[0]}, Mensagem: {last_error[1]}")
                else:
                    log.error("MT5 não reportou erros específicos, mas falhou em obter símbolos")
                
                return None
            
            log.info(f"SUCESSO: Obtidos {len(symbols)} símbolos para o grupo: '{group}'")
            return symbols
            
        except Exception as e:
            log.error(f"ERRO CRÍTICO ao obter símbolos: {e}")
            log.debug(traceback.format_exc())
            return None
            
    def _log_mt5_status(self):
        """
        Registra informações detalhadas do status do MT5 para diagnóstico
        """
        try:
            log.info("----- DIAGNÓSTICO MT5 -----")
            
            # Total de símbolos
            total_symbols = mt5.symbols_total()
            log.info(f"Total de símbolos reportado pelo MT5: {total_symbols}")
            
            # Status da terminal_info
            terminal = mt5.terminal_info()
            if terminal is not None:
                log.info(f"Terminal conectado: {terminal.connected}")
                log.info(f"Caminho do terminal: {terminal.path}")
                log.info(f"Versão do terminal: {terminal.version}")
                log.info(f"Modo de operação: {'Permitido' if terminal.trade_allowed else 'Não permitido'}")
                log.info(f"Comunidade: {'Conectado' if terminal.community_connected else 'Desconectado'}")
                log.info(f"Sinais: {'Permitido' if terminal.signals_allowed else 'Não permitido'}")
                
            else:
                log.warning("Não foi possível obter informações do terminal")
                
            # Status do account_info
            account = mt5.account_info()
            if account is not None:
                log.info(f"Conta logada: {account.login}")
                log.info(f"Nome da conta: {account.name}")
                log.info(f"Servidor: {account.server}")
                log.info(f"Moeda: {account.currency}")
                log.info(f"Alavancagem: {account.leverage}")
                log.info(f"Tipo de conta: {account.margin_so_mode}")
            else:
                log.warning("Não foi possível obter informações da conta")
                
            # Verifica últimos erros
            last_error = mt5.last_error()
            if last_error:
                log.error(f"Último erro MT5: Código {last_error[0]}, Descrição: {last_error[1]}")
            else:
                log.info("Sem erros MT5 reportados recentemente")
                
            # Verificar estado da conexão
            if terminal and terminal.connected:
                # Tentar obter alguns símbolos de forma aleatória para testar
                sample_symbols = ["EURUSD", "USDJPY", "GBPUSD"]
                for sym in sample_symbols:
                    sym_info = mt5.symbol_info(sym)
                    if sym_info:
                        log.info(f"Símbolo de teste {sym}: Disponível ({sym_info.visible})")
                    else:
                        log.warning(f"Símbolo de teste {sym}: Não disponível")
                        
            log.info("----- FIM DIAGNÓSTICO MT5 -----")
        except Exception as e:
            log.error(f"Erro ao obter status do MT5: {e}")
            log.debug(traceback.format_exc())

    def get_total_symbols(self):
        """Encapsula mt5.symbols_total()"""
        if not self.is_initialized or not mt5:
            log.warning("Tentativa de obter total de símbolos sem conexão MT5 inicializada.")
            return 0
        try:
            total = mt5.symbols_total()
            return total if total is not None else 0
        except Exception as e:
            log.error(f"Erro ao obter total de símbolos: {e}")
            log.debug(traceback.format_exc())
            return 0

    def get_symbol_info(self, symbol, retry_count=0, max_retries=2):
        """
        Obtém informações detalhadas sobre um símbolo.
        
        Args:
            symbol (str): Nome do símbolo
            retry_count (int): Contador de tentativas internas
            max_retries (int): Número máximo de tentativas
            
        Returns:
            Symbol_Info object ou None em caso de erro
        """
        if not self.is_initialized and retry_count == 0:
            log.warning(f"MT5 não inicializado ao tentar obter informações do símbolo {symbol}. Tentando inicializar...")
            if not self.initialize(recursion_count=retry_count):
                log.error(f"Falha ao inicializar MT5 para obter informações do símbolo {symbol}")
                return None
        
        try:
            # Aplicar autocorreção ao símbolo antes de buscar informações, mas apenas na primeira tentativa
            if retry_count == 0:
                original_symbol = symbol
                symbol = self.auto_correct_symbol(symbol)
                if original_symbol != symbol:
                    log.debug(f"Símbolo corrigido: {original_symbol} -> {symbol}")
                    
            # Proteção contra símbolos malformados
            if not symbol or len(symbol) < 1 or len(symbol) > 32:
                log.error(f"Nome de símbolo inválido: {symbol}")
                return None
                
            # Proteção contra recursão ao tentar obter informação
            if retry_count > max_retries:
                log.error(f"Excedido número máximo de tentativas para obter informações do símbolo {symbol}")
                return None
            
            # Tentar obter informações do símbolo
            log.debug(f"Tentando obter informações do símbolo: {symbol}")
            symbol_info = mt5.symbol_info(symbol)
            
            if not symbol_info:
                error = mt5.last_error()
                log.warning(f"Erro ao obter informações do símbolo {symbol}: {error}")
                
                # Se o erro for por falta de conexão, tentar reinicializar
                if retry_count < max_retries:
                    log.info(f"Tentando reinicializar MT5 e buscar símbolo novamente (tentativa {retry_count+1}/{max_retries})")
                    self.shutdown()
                    time.sleep(1)
                    if self.initialize(recursion_count=retry_count):
                        # Chamada recursiva com incremento do contador
                        return self.get_symbol_info(symbol, retry_count + 1, max_retries)
                    else:
                        log.error(f"Falha ao reinicializar MT5 para nova tentativa do símbolo {symbol}")
                
                return None
            
            return symbol_info
            
        except Exception as e:
            log.error(f"Erro ao obter informações do símbolo {symbol}: {e}")
            return None

    def get_rates(self, symbol, timeframe, start_pos, count):
        """Encapsula mt5.copy_rates_from_pos()"""
        if not self.is_initialized or not mt5:
            log.warning(f"Tentativa de obter rates para {symbol} sem conexão MT5 inicializada.")
            return None
        try:
            rates = mt5.copy_rates_from_pos(symbol, timeframe, start_pos, count)
            if rates is None:
                log.error(f"Erro ao obter rates para {symbol} (copy_rates_from_pos retornou None). Erro MT5: {mt5.last_error()}")
                return None
            # Converter para DataFrame para consistência (opcional, mas útil)
            rates_df = pd.DataFrame(rates)
            # Converter timestamp para datetime
            rates_df['time'] = pd.to_datetime(rates_df['time'], unit='s')
            return rates_df
        except Exception as e:
            log.error(f"Erro ao obter rates para {symbol}: {e}")

    def get_rates_from(self, symbol, timeframe, date_from, count):
        """Encapsula mt5.copy_rates_from()"""
        if not self.is_initialized or not mt5:
            log.warning(f"Tentativa de obter rates para {symbol} (from date) sem conexão MT5 inicializada.")
            return None
        try:
            rates = mt5.copy_rates_from(symbol, timeframe, date_from, count)
            if rates is None:
                log.error(f"Erro ao obter rates para {symbol} (copy_rates_from retornou None). Erro MT5: {mt5.last_error()}")
                return None
            # Converter para DataFrame para consistência
            rates_df = pd.DataFrame(rates)
            # Converter timestamp para datetime
            rates_df['time'] = pd.to_datetime(rates_df['time'], unit='s')
            return rates_df
        except Exception as e:
            log.error(f"Erro ao obter rates para {symbol} (from date): {e}")

    def get_market_book(self, symbol):
        """Encapsula mt5.market_book_get()"""
        if not self.is_initialized or not mt5:
            log.warning(f"Tentativa de obter market book para {symbol} sem conexão MT5 inicializada.")
            return None
        try:
            # É necessário adicionar o símbolo ao MarketWatch antes de obter o book
            if not mt5.market_book_add(symbol):
                log.error(f"Falha ao adicionar {symbol} ao MarketWatch. Erro: {mt5.last_error()}")
                # Não retorna None aqui, pois market_book_get pode funcionar mesmo assim em alguns casos

            # Espera um pouco para garantir que o book seja atualizado (pode ser necessário ajustar)
            time.sleep(0.1)

            book = mt5.market_book_get(symbol)
            if book:
                # Opcional: Remover o símbolo após obter o book para não poluir o MarketWatch?
                # mt5.market_book_release(symbol)
                return book
            else:
                log.error(f"Erro ao obter market book para {symbol}. Erro: {mt5.last_error()}")
                # mt5.market_book_release(symbol) # Tenta remover mesmo em caso de erro
                return None
        except Exception as e:
            log.error(f"Erro ao obter market book para {symbol}: {e}")
            log.debug(traceback.format_exc())
            # Tentar remover em caso de exceção
            # try:
            #     mt5.market_book_release(symbol)

    def get_rates_range(self, symbol, timeframe, date_from, date_to):
        """Encapsula mt5.copy_rates_range()"""
        if not self.is_initialized or not mt5:
            log.warning(f"Tentativa de obter rates para {symbol} (range) sem conexão MT5 inicializada.")
            return None
        try:
            # Garante que date_from e date_to sejam objetos datetime
            if not isinstance(date_from, datetime.datetime):
                log.error("date_from deve ser um objeto datetime")
                return None
            if not isinstance(date_to, datetime.datetime):
                log.error("date_to deve ser um objeto datetime")
                return None

            rates = mt5.copy_rates_range(symbol, timeframe, date_from, date_to)
            if rates is None:
                log.error(f"Erro ao obter rates para {symbol} (copy_rates_range retornou None). Erro MT5: {mt5.last_error()}")
                return None
            # Converter para DataFrame para consistência
            rates_df = pd.DataFrame(rates)
            # Converter timestamp para datetime
            rates_df['time'] = pd.to_datetime(rates_df['time'], unit='s')
            return rates_df
        except Exception as e:
            log.error(f"Erro ao obter rates para {symbol} (range): {e}")
            log.debug(traceback.format_exc())
            return None

            # except:

    def get_available_timeframes(self):
        """Retorna a lista de timeframes disponíveis.

        Retorna uma lista de tuplas (nome_legivel, valor_mt5).
        Usa os valores do módulo mt5 se inicializado, caso contrário, usa padrões.
        """
        # Valores padrão (caso mt5 não esteja disponível ou inicializado)
        default_timeframes = [
            ("1 minuto", 1),
            ("5 minutos", 5),
            ("15 minutos", 15),
            ("30 minutos", 30),
            ("1 hora", 60),
            ("4 horas", 240),
            ("1 dia", 1440),
            ("1 semana", 10080),
            ("1 mês", 43200)
        ]

        if self.is_initialized and mt5:
            try:
                # Tenta usar os valores do MT5
                return [
                    ("1 minuto", mt5.TIMEFRAME_M1),
                    ("5 minutos", mt5.TIMEFRAME_M5),
                    ("15 minutos", mt5.TIMEFRAME_M15),
                    ("30 minutos", mt5.TIMEFRAME_M30),
                    ("1 hora", mt5.TIMEFRAME_H1),
                    ("4 horas", mt5.TIMEFRAME_H4),
                    ("1 dia", mt5.TIMEFRAME_D1),
                    ("1 semana", mt5.TIMEFRAME_W1),
                    ("1 mês", mt5.TIMEFRAME_MN1)
                ]
            except AttributeError as e:
                log.warning(f"Erro ao acessar constantes de timeframe do MT5 ({e}). Usando padrões.")
                return default_timeframes
            except Exception as e:
                 log.error(f"Erro inesperado ao obter timeframes do MT5: {e}")
                 return default_timeframes
        else:
            # Retorna os padrões se não estiver conectado
            log.info("MT5 não inicializado. Usando timeframes padrão.")
            return default_timeframes

            #     pass
            return None

            log.debug(traceback.format_exc())
            return None

            log.debug(traceback.format_exc())
            self.is_initialized = False
            self.connection_mode = "Erro Crítico"
            return False

    def shutdown(self):
        """Encerra a conexão com o MetaTrader 5."""
        if self.is_initialized and mt5:
            try:
                mt5.shutdown()
                log.info("Conexão MT5 encerrada.")
                self.is_initialized = False
                self.connection_mode = "Desconectado"
            except Exception as e:
                log.error(f"Erro ao encerrar conexão MT5: {e}")

    def get_connection_status(self):
        """Retorna o status atual da conexão com o MT5."""
        return {
            "is_initialized": self.is_initialized,
            "mode": self.connection_mode,
            "path": self.mt5_path
        }
        
    def is_admin(self):
        """Verifica se o programa está sendo executado como administrador."""
        try:
            import ctypes
            return ctypes.windll.shell32.IsUserAnAdmin() != 0
        except:
            log.warning("Não foi possível verificar permissões de administrador.")
            return False
            
    def is_mt5_running_as_admin(self):
        """
        Verifica se o MetaTrader 5 está em execução com permissões de administrador.
        
        Returns:
            bool: True se o MT5 está executando como administrador, False caso contrário
        """
        if not self._is_mt5_running():
            log.info("MT5 não está em execução, não é possível verificar permissões")
            return False
            
        try:
            # Verificação de compatibilidade
            if not psutil:
                log.warning("Biblioteca psutil não disponível para verificar permissões admin")
                # Assume que está rodando como admin para evitar loops
                return True
            
            # Método alternativo mais confiável: verificar se MT5 consegue carregar valores
            try:
                if mt5 and hasattr(mt5, 'symbol_info_tick'):
                    # Se conseguir obter informações básicas, provavelmente tem permissões adequadas
                    try:
                        # Tenta inicializar brevemente para verificar permissões
                        if not self.is_initialized:
                            mt5.initialize(path=self.mt5_path, timeout=10000)
                            
                        # Tentamos obter símbolos e verificar permissões
                        symbols = mt5.symbols_get()
                        if symbols and len(symbols) > 0:
                            # Se conseguir obter símbolos, provavelmente está com permissões adequadas
                            log.info(f"MT5 tem acesso a {len(symbols)} símbolos")
                            # Desconecta se inicializamos só para o teste
                            if not self.is_initialized:
                                mt5.shutdown()
                            return True
                        else:
                            log.warning("MT5 conectado mas sem acesso a símbolos")
                            if not self.is_initialized:
                                mt5.shutdown()
                    except Exception as api_err:
                        log.debug(f"Erro ao verificar acesso a símbolos: {api_err}")
                        if not self.is_initialized:
                            try:
                                mt5.shutdown()
                            except:
                                pass
            except Exception as e:
                log.debug(f"Erro ao verificar conexão MT5: {e}")
            
            # Método tradicional: verifica permissões de processo
            # Este método não é 100% confiável no Windows 10+, mas mantemos como fallback
            for proc in psutil.process_iter(['name', 'exe', 'pid']):
                try:
                    if 'terminal64.exe' in proc.info['name'].lower() or 'metatrader' in proc.info['name'].lower():
                        log.info(f"Processo MT5 encontrado: {proc.info['name']}")
                        
                        # No Windows 10+, retorna True para evitar loops infinitos
                        # Esta é uma solução de contorno para o problema conhecido no Windows 10+
                        # onde a verificação de permissões nem sempre funciona corretamente
                        import platform
                        if platform.system() == 'Windows' and int(platform.release()) >= 10:
                            log.info(f"Windows 10+ detectado, considerando MT5 como admin")
                            return True
                        
                        # Em outros sistemas, tenta verificações adicionais
                        try:
                            # Tenta verificar se o processo tem permissões de administrador
                            # Nota: Isso nem sempre é confiável no Windows 10+
                            proc_with_info = psutil.Process(proc.info['pid'])
                            user = proc_with_info.username()
                            log.info(f"MT5 executando como usuário: {user}")
                            return 'admin' in user.lower() or self.is_admin()
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            log.warning("Acesso negado ao verificar permissões do processo MT5")
                            # Assume permissão para evitar loops
                            return True
                except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
                    log.warning(f"Erro ao verificar processo MT5: {e}")
                    continue

            # Não encontrou o MT5 ou falhou ao verificar permissões
            log.warning("Não foi possível verificar permissões do MT5 com certeza")
            return False
            
        except Exception as e:
            log.error(f"Erro ao verificar permissões do MT5: {e}")
            log.debug(traceback.format_exc())
            # Em caso de erro, retorna True para evitar loops
            return True

    def launch_mt5_as_admin(self, wait_for_user=True):
        """
        Inicia o MetaTrader 5 com permissões de administrador.
        
        Args:
            wait_for_user (bool): Se True, exibe mensagem pedindo confirmação do usuário
            
        Returns:
            bool: True se o processo foi iniciado, False caso contrário
        """
        if not self.mt5_path:
            log.error("Caminho do MT5 não configurado. Impossível iniciar.")
            return False
            
        terminal_exe = os.path.join(self.mt5_path, "terminal64.exe")
        if not os.path.exists(terminal_exe):
            log.error(f"Executável terminal64.exe não encontrado em: {terminal_exe}")
            return False
            
        # Verifica se o MT5 já está em execução
        if self._is_mt5_running():
            # Se estiver rodando, verifica se já tem permissões adequadas
            if self.is_mt5_running_as_admin():
                log.info("MT5 já está em execução com permissões adequadas.")
                return True
                
            # Está rodando sem permissões adequadas, pergunta se quer fechar
            if wait_for_user:
                # Importa aqui para evitar dependência circular
                try:
                    from tkinter import messagebox
                    resposta = messagebox.askquestion(
                        "MT5 sem permissões adequadas",
                        "O MetaTrader 5 está em execução, mas sem permissões adequadas. "
                        "Para melhor funcionamento, é recomendável fechá-lo e reabri-lo como administrador.\n\n"
                        "Deseja fechar o MT5 atual e reabri-lo como administrador?"
                    )
                    if resposta != 'yes':
                        log.info("Usuário optou por não reiniciar o MT5 como administrador.")
                        return False
                except ImportError:
                    # Se não conseguir importar tkinter, continua sem perguntar
                    pass
                        
            # Fecha o MT5 atual usando diversas abordagens
            try:
                killed = False
                # Abordagem 1: Usando taskkill para garantir que todos os processos sejam encerrados
                try:
                    subprocess.run(["taskkill", "/F", "/IM", "terminal64.exe"], 
                                  capture_output=True, text=True)
                    killed = True
                except Exception as e:
                    log.warning(f"Erro ao encerrar MT5 via taskkill: {e}")
                
                # Abordagem 2: Usando psutil caso taskkill falhe
                if not killed and psutil:
                    try:
                        for proc in psutil.process_iter(['name']):
                            try:
                                if proc.info['name'] and 'terminal64.exe' in proc.info['name'].lower():
                                    proc.kill()
                            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                                continue
                        killed = True
                    except Exception as e:
                        log.warning(f"Erro ao encerrar MT5 via psutil: {e}")
                
                if killed:
                    log.info("Processos do MT5 encerrados.")
                    # Espera para garantir que o processo foi encerrado completamente
                    for i in range(10):
                        if not self._is_mt5_running():
                            break
                        time.sleep(0.5)
                else:
                    log.warning("Não foi possível encerrar o MT5. Tentando iniciar mesmo assim.")
            except Exception as e:
                log.error(f"Erro ao tentar encerrar o MT5: {e}")
                # Continua mesmo se não conseguir encerrar
                
        # Inicia o MT5 como administrador
        try:
            import ctypes
            
            if wait_for_user and not self._is_mt5_running():
                try:
                    from tkinter import messagebox
                    resposta = messagebox.askquestion(
                        "Iniciar MT5 como Administrador",
                        "Para garantir o funcionamento correto, o MetaTrader 5 precisa ser iniciado com permissões "
                        "de administrador.\n\n"
                        "Deseja iniciar o MT5 como administrador agora?"
                    )
                    if resposta != 'yes':
                        log.info("Usuário optou por não iniciar o MT5 como administrador.")
                        return False
                except ImportError:
                    # Se não conseguir importar tkinter, continua sem perguntar
                    pass
            
            # Se já tentou fechar mas ainda está rodando, avisa
            if self._is_mt5_running():
                log.warning("MT5 ainda está em execução. Tentando iniciar mesmo assim.")
            
            # Usa o ShellExecute para invocar o UAC
            if hasattr(ctypes.windll.shell32, 'ShellExecuteW'):
                log.info(f"Iniciando MT5 como administrador: {terminal_exe}")
                result = ctypes.windll.shell32.ShellExecuteW(
                    None,                   # Handle para a janela pai
                    "runas",                # Verbo (runas = executar como administrador)
                    terminal_exe,           # Caminho do executável
                    None,                   # Parâmetros 
                    self.mt5_path,          # Diretório de trabalho
                    1                       # nShowCmd (1 = SW_NORMAL)
                )
                if result > 32:  # Se for > 32, a operação foi bem-sucedida
                    log.info("Comando para iniciar MT5 como administrador enviado com sucesso.")
                    
                    # Espera até o MT5 iniciar, com timeout
                    started = False
                    for i in range(20):  # Espera até 10 segundos (20*0.5s)
                        if self._is_mt5_running():
                            started = True
                            break
                        time.sleep(0.5)
                    
                    if started:
                        log.info("MT5 iniciado com sucesso.")
                        # Dá um tempo adicional para o MT5 inicializar completamente
                        time.sleep(2)
                        return True
                    else:
                        log.error("MT5 não parece ter iniciado após 10 segundos.")
                        return False
                else:
                    log.error(f"Falha ao iniciar MT5 como administrador. Código: {result}")
                    return False
            else:
                # Fallback se ShellExecuteW não estiver disponível (improvável no Windows)
                log.warning("ShellExecuteW não disponível, tentando método alternativo...")
                try:
                    subprocess.Popen(
                        ["runas", "/user:Administrator", f"\"{terminal_exe}\""],
                        shell=True,
                        creationflags=subprocess.CREATE_NO_WINDOW
                    )
                    
                    # Espera até o MT5 iniciar, com timeout
                    started = False
                    for i in range(20):  # Espera até 10 segundos (20*0.5s)
                        if self._is_mt5_running():
                            started = True
                            break
                        time.sleep(0.5)
                    
                    if started:
                        log.info("MT5 iniciado com sucesso (método alternativo).")
                        time.sleep(2)
                        return True
                    else:
                        log.error("MT5 não parece ter iniciado após 10 segundos (método alternativo).")
                        return False
                except Exception as alt_err:
                    log.error(f"Erro ao iniciar MT5 usando método alternativo: {alt_err}")
                    return False
                
        except Exception as e:
            log.error(f"Erro ao tentar iniciar MT5 como administrador: {e}")
            log.debug(traceback.format_exc())
            return False

    def ensure_mt5_running_with_admin(self, auto_start=False):
        """
        Garante que o MT5 esteja em execução com permissões de administrador.
        
        Args:
            auto_start (bool): Se True, tenta iniciar o MT5 automaticamente sem perguntar ao usuário
            
        Returns:
            tuple: (is_running, is_admin) indicando se está rodando e se tem permissões
        """
        is_running = self._is_mt5_running()
        
        if not is_running:
            if auto_start:
                # Inicia sem perguntar ao usuário
                success = self.launch_mt5_as_admin(wait_for_user=False)
                if success:
                    return (True, True)
                # Se falhar, retorna o estado real
                return (self._is_mt5_running(), False)
            else:
                # Inicia perguntando ao usuário
                success = self.launch_mt5_as_admin(wait_for_user=True)
                if success:
                    return (True, True)
                # Se falhar, retorna o estado real
                return (self._is_mt5_running(), False)
        else:
            # Já está rodando, verifica se é admin
            is_admin = self.is_mt5_running_as_admin()
            
            if not is_admin and auto_start:
                # Tenta reiniciar como admin
                success = self.launch_mt5_as_admin(wait_for_user=False)
                if success:
                    return (True, True)
            
            return (True, is_admin)

    def handle_symbol_error(self, symbol, error):
        """
        Trata erros específicos relacionados a símbolos.
        
        Args:
            symbol (str): Nome do símbolo que gerou o erro
            error (Exception): Erro ocorrido
            
        Returns:
            bool: True se o erro foi tratado e pode tentar novamente, False caso contrário
        """
        error_str = str(error)
        
        # Mapear erros comuns para ações
        if "invalid symbol" in error_str.lower():
            log.warning(f"Símbolo {symbol} é inválido.")
            return False
            
        elif "not initialized" in error_str.lower():
            log.warning("Conexão MT5 não inicializada. Tentando reconectar...")
            if self.initialize():
                log.info("Reconexão bem-sucedida ao MT5")
                return True  # Pode tentar novamente
            return False
            
        elif "not enough" in error_str.lower() and "data" in error_str.lower():
            log.warning(f"Dados insuficientes para {symbol}. Considere um período mais recente.")
            return False
            
        # Erro desconhecido
        log.error(f"Erro não tratado para {symbol}: {error}")
        return False
            
    def validate_symbol(self, symbol):
        """
        Verifica se um símbolo é válido e está disponível no MT5.
        
        Args:
            symbol (str): O símbolo a ser validado
            
        Returns:
            bool: True se o símbolo for válido, False caso contrário
        """
        if not self.is_initialized or not mt5:
            log.warning(f"Não é possível validar o símbolo {symbol}: MT5 não inicializado")
            return False
            
        try:
            # Verifica se o símbolo existe no MT5
            symbol_info = self.get_symbol_info(symbol)
            if symbol_info is None:
                log.warning(f"Símbolo {symbol} não encontrado no MT5")
                return False
                
            return True
        except Exception as e:
            log.error(f"Erro ao validar símbolo {symbol}: {e}")
            return False
            
    def auto_correct_symbol(self, symbol):
        """
        Tenta corrigir o nome do símbolo para o formato aceito pelo MT5.
        
        Args:
            symbol (str): Nome do símbolo original
            
        Returns:
            str: Nome do símbolo corrigido ou o original se não for possível corrigir
        """
        if not symbol:
            return symbol
            
        # Conversão básica para maiúsculas
        corrected = symbol.upper()
        
        # Substitui caracteres comuns que podem causar problemas
        corrected = corrected.replace(' ', '')
        
        # Algumas correções específicas para símbolos brasileiros
        if corrected.endswith('F') and len(corrected) > 5:  # Possível ação futura
            parts = corrected.split('F', 1)
            if parts[0]:
                corrected = parts[0] + '$F'
                
        # Corrige WIN para formato padrão
        if corrected == 'WIN' or corrected == 'WINFUT':
            corrected = 'WIN$'
            
        # Verifica se o símbolo corrigido existe no MT5
        if self.is_initialized and mt5:
            try:
                # Usar mt5.symbol_info diretamente para evitar recursão
                if mt5.symbol_info(corrected) is None:
                    # Tenta variações comuns
                    variations = [
                        corrected + '$',      # Adiciona $
                        corrected + '$N',     # Adiciona $N para índices futuros  
                        corrected.replace('$', ''),  # Remove $ se existir
                        corrected + 'USD',    # Para criptomoedas
                        'WIN$N' if corrected in ['WIN', 'WINFUT', 'WIN$'] else corrected,
                        'DOL$N' if corrected in ['DOL', 'DOLFUT', 'DOL$'] else corrected,
                        'IND$N' if corrected in ['IND', 'INDFUT', 'IND$'] else corrected
                    ]
                    
                    for var in variations:
                        if mt5.symbol_info(var) is not None:
                            log.info(f"Símbolo corrigido: {symbol} -> {var}")
                            return var
            except Exception as e:
                log.debug(f"Erro ao tentar autocorrigir símbolo {symbol}: {e}")
                
        return corrected
            
    @with_error_handling(error_type=MT5ConnectionError)
    def get_oldest_available_date(self, symbol, timeframe, force_refresh=False, max_cache_age_days=30):
        """
        Detecta a data mais antiga disponível para um símbolo específico.
        Utiliza sistema de cache com expiração e implementa throttling para evitar sobrecarga do MT5.
        
        Args:
            symbol (str): Nome do símbolo
            timeframe (int ou str): Valor do timeframe (ex: mt5.TIMEFRAME_D1) ou string (ex: '1d')
            force_refresh (bool): Ignorar cache e forçar nova busca
            max_cache_age_days (int): Idade máxima do cache em dias
            
        Returns:
            datetime.datetime: Data mais antiga disponível ou None se não for possível determinar
        """
        # Verificar conexão MT5
        if not self.is_initialized or not mt5:
            log.warning(f"Tentativa de obter data mais antiga para {symbol} sem conexão MT5 inicializada.")
            return None
        
        # Converter o timeframe para o formato do MT5 se for string
        mt5_timeframe = timeframe
        if not isinstance(timeframe, int) or timeframe not in [
            mt5.TIMEFRAME_M1, mt5.TIMEFRAME_M2, mt5.TIMEFRAME_M3, mt5.TIMEFRAME_M4, 
            mt5.TIMEFRAME_M5, mt5.TIMEFRAME_M6, mt5.TIMEFRAME_M10, mt5.TIMEFRAME_M12, 
            mt5.TIMEFRAME_M15, mt5.TIMEFRAME_M20, mt5.TIMEFRAME_M30, 
            mt5.TIMEFRAME_H1, mt5.TIMEFRAME_H2, mt5.TIMEFRAME_H3, mt5.TIMEFRAME_H4, 
            mt5.TIMEFRAME_H6, mt5.TIMEFRAME_H8, mt5.TIMEFRAME_H12, 
            mt5.TIMEFRAME_D1, mt5.TIMEFRAME_W1, mt5.TIMEFRAME_MN1
        ]:
            mt5_timeframe = self._convert_timeframe_to_mt5(timeframe)
            if mt5_timeframe is None:
                log.error(f"Timeframe inválido: {timeframe}")
                return None
            log.debug(f"Timeframe convertido: {timeframe} -> {mt5_timeframe}")
        
        # Chave de cache baseada no symbol e timeframe convertido
        cache_key = f"{symbol}_{mt5_timeframe}"
            
        # Tentar autocorrigir o símbolo
        original_symbol = symbol
        symbol = self.auto_correct_symbol(symbol)
        if original_symbol != symbol:
            log.info(f"Usando símbolo corrigido: {symbol} (original: {original_symbol})")
            # Atualizar a chave de cache para usar o símbolo corrigido
            cache_key = f"{symbol}_{mt5_timeframe}"
            
        # Verificar se o símbolo é válido
        if not self.validate_symbol(symbol):
            log.warning(f"Símbolo {symbol} inválido ou indisponível. Pulando detecção de data.")
            return None
            
        # Verificar no cache primeiro se não for forçada atualização
        if not force_refresh:
            cache_data = self._load_oldest_dates_cache()
            
            # Verificar se temos dados em cache válidos para este símbolo/timeframe
            if cache_key in cache_data:
                cached_date_str = cache_data[cache_key]
                
                # Verificar data de atualização do cache
                if (f"{cache_key}_updated" in cache_data and 
                    isinstance(cache_data[f"{cache_key}_updated"], str)):
                    try:
                        cache_update_date = datetime.datetime.fromisoformat(
                            cache_data[f"{cache_key}_updated"]
                        )
                        age_days = (datetime.datetime.now() - cache_update_date).days
                        
                        # Se cache estiver atualizado, usar valor
                        if age_days <= max_cache_age_days:
                            try:
                                cached_date = datetime.datetime.fromisoformat(cached_date_str)
                                log.info(f"Data em cache válida para {symbol} (atualizada há {age_days} dias)")
                                return cached_date
                            except ValueError:
                                log.warning(f"Formato de data inválido no cache: {cached_date_str}")
                                
                        else:
                            log.info(f"Cache para {symbol} expirado ({age_days} dias > {max_cache_age_days})")
                    except ValueError:
                        log.warning(f"Formato de data de atualização inválido: {cache_data[f'{cache_key}_updated']}")
        
        # --- Detecção de data mais antiga ---
        try:
            # Configuração de throttling
            request_delay = 0.5  # Tempo de espera entre requisições (500ms)
            
            # Verificar novamente se o símbolo existe
            symbol_info = self.get_symbol_info(symbol)
            if not symbol_info:
                log.warning(f"Símbolo {symbol} não encontrado. Não é possível determinar data mais antiga.")
                return None
            
            # Estratégia: começar com 5 anos e ir retrocedendo
            today = datetime.datetime.now()
            oldest_date = None
            found_data = False
            
            log.info(f"Iniciando detecção de data mais antiga para {symbol} (timeframe {mt5_timeframe})")
            
            # Períodos a tentar, do mais recente para o mais antigo
            periods = [
                {"name": "5 anos", "years": 5},
                {"name": "10 anos", "years": 10},
                {"name": "15 anos", "years": 15},
                {"name": "20 anos", "years": 20},
                {"name": "25 anos", "years": 25}
            ]
            
            for period in periods:
                if found_data:
                    break
                    
                years_back = period["years"]
                period_name = period["name"]
                
                # Esperar entre períodos para não sobrecarregar
                time.sleep(request_delay)
                    
                search_start = today - datetime.timedelta(days=years_back*365)
                search_end = search_start + datetime.timedelta(days=30)  # Janela de 30 dias
                
                log.info(f"Buscando dados para {symbol} de {period_name} atrás: {search_start.date()} a {search_end.date()}")
                
                try:
                    # Obter dados usando o método mais robusto get_historical_data
                    df = self.get_historical_data(symbol, mt5_timeframe, start_dt=search_start, end_dt=search_end)
                    
                    if df is not None and not df.empty:
                        log.info(f"Encontrados {len(df)} registros para {symbol} no período de {period_name} atrás")
                        found_data = True
                        current_oldest = df['time'].min()
                        
                        if oldest_date is None or current_oldest < oldest_date:
                            oldest_date = current_oldest
                            
                        # Tentar um período ainda mais antigo para refinar
                        older_start = search_start - datetime.timedelta(days=365)
                        older_end = search_start
                        
                        # Esperar antes da próxima requisição
                        time.sleep(request_delay)
                        
                        log.info(f"Refinando busca para {symbol}: {older_start.date()} a {older_end.date()}")
                        df_older = self.get_historical_data(symbol, mt5_timeframe, start_dt=older_start, end_dt=older_end)
                        
                        if df_older is not None and not df_older.empty:
                            log.info(f"Encontrados mais {len(df_older)} registros antigos para {symbol}")
                            even_older = df_older['time'].min()
                            if even_older < oldest_date:
                                oldest_date = even_older
                                
                                # Continuar refinando se encontrou dados mais antigos
                                step_back = 2
                                while step_back <= 5:  # Limitar a 5 tentativas adicionais
                                    # Throttling entre requisições
                                    time.sleep(request_delay)
                                    
                                    extra_start = older_start - datetime.timedelta(days=365 * step_back)
                                    extra_end = extra_start + datetime.timedelta(days=365)
                                    
                                    log.info(f"Busca adicional {step_back} para {symbol}: {extra_start.date()} a {extra_end.date()}")
                                    df_extra = self.get_historical_data(symbol, mt5_timeframe, start_dt=extra_start, end_dt=extra_end)
                                    
                                    if df_extra is not None and not df_extra.empty:
                                        log.info(f"Encontrados mais {len(df_extra)} registros na busca adicional {step_back}")
                                        extra_oldest = df_extra['time'].min()
                                        if extra_oldest < oldest_date:
                                            oldest_date = extra_oldest
                                            step_back += 1
                                        else:
                                            # Não encontrou data mais antiga, interromper
                                            break
                                    else:
                                        # Sem dados neste período, interromper
                                        break
                except Exception as search_err:
                    log.warning(f"Erro na busca de {period_name} para {symbol}: {search_err}")
                    # Continuar com o próximo período mesmo em caso de erro
            
            # Salvar resultado no cache se encontrou dados
            if found_data and oldest_date:
                cache_data = self._load_oldest_dates_cache()
                cache_data[cache_key] = oldest_date.isoformat()
                cache_data[f"{cache_key}_updated"] = datetime.datetime.now().isoformat()
                self._save_oldest_dates_cache(cache_data)
                
                log.info(f"Data mais antiga para {symbol} (timeframe {mt5_timeframe}): {oldest_date}")
                return oldest_date
            else:
                log.warning(f"Nenhum dado histórico encontrado para {symbol} em nenhum período testado")
                return None
                
        except Exception as e:
            log.error(f"Erro ao buscar data mais antiga para {symbol}: {e}")
            log.debug(traceback.format_exc())
            return None

    def _start_mt5_if_not_running(self, recursion_count=0):
        """
        Tenta iniciar o MT5 se não estiver em execução.
        
        Args:
            recursion_count (int): Contador de recursão para evitar ciclos infinitos
            
        Returns:
            bool: True se o MT5 foi iniciado com sucesso ou já estava em execução
        """
        # Verificar se o MT5 já está em execução
        is_running = self._is_mt5_running()
        if is_running:
            log.info("MT5 já está em execução.")
            return True
            
        # Se chegou até aqui, precisamos iniciar o MT5
        if not self.mt5_path:
            log.error("Caminho do MT5 não configurado. Impossível iniciar.")
            return False
            
        log.info(f"Tentando iniciar MT5 de: {self.mt5_path}")
        
        # Construir caminho para o terminal64.exe
        exe_path = os.path.join(self.mt5_path, "terminal64.exe")
        if not os.path.exists(exe_path):
            # Se não encontrar, verificar se o path já é o executável
            if os.path.basename(self.mt5_path).lower() == "terminal64.exe":
                exe_path = self.mt5_path
            else:
                log.error(f"Executável terminal64.exe não encontrado em: {self.mt5_path}")
                return False
                
        # Tentar iniciar o executável
        try:
            # Iniciar o processo
            subprocess.Popen(exe_path)
            log.info(f"Comando para iniciar MT5 enviado: {exe_path}")
            
            # Aguardar inicialização
            max_wait = 30  # segundos
            for i in range(max_wait):
                time.sleep(1)
                if self._is_mt5_running():
                    log.info(f"MT5 iniciado com sucesso após {i+1} segundos")
                    # Removendo a chamada recursiva a initialize para evitar ciclos infinitos
                    # Apenas retorna sucesso para que o processo de inicialização continue na função chamadora
                    return True
                    
            log.warning(f"MT5 iniciado, mas não detectado após {max_wait} segundos")
            return False
            
        except Exception as e:
            log.error(f"Erro ao iniciar MT5: {e}")
            return False

    def _fix_ipc_error(self):
        """
        Tenta resolver o erro IPC (Inter-Process Communication) comum ao conectar com MT5.
        Especificamente trata o erro -10003 (IPC initialize failed).
        
        Returns:
            bool: True se conseguiu corrigir o erro, False caso contrário
        """
        log.info("Tentando resolver erro IPC...")
        
        try:
            # 1. Verifica se o MT5 está realmente rodando
            if not self._is_mt5_running():
                log.error("MT5 não está em execução, não é possível corrigir erro IPC")
                return False
                
            # 2. Tenta encerrar e reiniciar o MT5
            log.info("Encerrando o MT5 para resolver problema de comunicação...")
            try:
                # Tenta usar taskkill para garantir que o processo termina
                subprocess.run(["taskkill", "/F", "/IM", "terminal64.exe"], 
                              capture_output=True, text=True)
                              
                # Espera 3 segundos para o processo encerrar
                time.sleep(3)
                
                # Verifica se realmente encerrou
                if self._is_mt5_running():
                    log.warning("Não foi possível encerrar o MT5 para reinicialização")
                    return False
                    
                # Reinicia o MT5 como administrador
                log.info("Reiniciando MT5 como administrador para corrigir erro IPC...")
                result = self.launch_mt5_as_admin(wait_for_user=False)
                
                if result:
                    log.info("MT5 reiniciado com sucesso, esperando inicialização completa...")
                    # Espera um tempo maior para garantir inicialização completa
                    time.sleep(5)
                    
                    # Verifica primeiro se já não está conectado antes de tentar inicializar novamente
                    if mt5.terminal_info():
                        log.info("Conexão já estabelecida automaticamente após reinicialização")
                        return True
                        
                    # Tenta conectar em modo portátil com timeout reduzido para evitar bloqueios longos
                    try:
                        # Inicialização com parâmetros específicos para evitar recursão
                        if mt5.initialize(path=self.mt5_path, timeout=15000, portable=True):
                            log.info("Conexão estabelecida com sucesso após correção de erro IPC")
                            
                            # Verifica se a conexão está funcionando obtendo informações básicas
                            try:
                                symbols = mt5.symbols_get()
                                if symbols:
                                    log.info(f"Acesso a {len(symbols)} símbolos confirmado após correção IPC")
                                    return True
                                else:
                                    log.warning("Não foi possível acessar símbolos após correção de IPC")
                            except Exception as e:
                                log.error(f"Erro ao verificar símbolos após correção IPC: {e}")
                        else:
                            error = mt5.last_error()
                            log.error(f"Erro ao conectar mesmo após reinício: {error}")
                    except Exception as init_error:
                        log.error(f"Erro durante tentativa de inicialização após correção IPC: {init_error}")
                else:
                    log.error("Falha ao reiniciar MT5")
            except Exception as e:
                log.error(f"Erro ao tentar reiniciar MT5: {e}")
                
            # 3. Verifica permissões do diretório MT5
            try:
                log.info("Verificando permissões do diretório MT5...")
                import ctypes
                if ctypes.windll.shell32.IsUserAnAdmin() == 0:
                    log.warning("Script não está rodando como administrador, pode não ter permissão para acessar o diretório do MT5")
                
                # Tenta acessar o diretório para verificar permissões
                files = os.listdir(self.mt5_path)
                log.info(f"Acesso ao diretório MT5 bem-sucedido: {len(files)} arquivos encontrados")
            except Exception as perm_error:
                log.error(f"Erro de permissão no diretório MT5: {perm_error}")
                return False
                
            # 4. Tenta estratégias alternativas de conexão
            # Aqui podemos adicionar mais estratégias específicas para resolver esse erro
                
            return False
        except Exception as e:
            log.error(f"Erro ao tentar corrigir problema IPC: {e}")
            return False

    def _load_oldest_dates_cache(self):
        """
        Carrega o cache de datas mais antigas de um arquivo JSON.
        
        Returns:
            dict: Dicionário com as datas em cache, vazio se o arquivo não existir
        """
        cache_file = os.path.join(os.path.dirname(self.config_path), "oldest_dates_cache.json")
        cache_data = {}
        
        if os.path.exists(cache_file):
            try:
                with open(cache_file, 'r') as f:
                    cache_data = json.load(f)
                log.debug(f"Cache de datas mais antigas carregado: {len(cache_data)} entradas")
            except Exception as e:
                log.warning(f"Erro ao carregar cache de datas: {e}")
                # Em caso de erro, retorna dicionário vazio para forçar nova detecção
                cache_data = {}
        else:
            log.debug("Arquivo de cache de datas não encontrado, será criado na próxima detecção")
            
        return cache_data
        
    def _save_oldest_dates_cache(self, cache_data):
        """
        Salva o cache de datas mais antigas em um arquivo JSON.
        
        Args:
            cache_data (dict): Dicionário com as datas em cache
        """
        if not cache_data:
            log.warning("Tentativa de salvar cache vazio, ignorando")
            return
            
        cache_file = os.path.join(os.path.dirname(self.config_path), "oldest_dates_cache.json")
        
        try:
            # Garantir que o diretório existe
            os.makedirs(os.path.dirname(cache_file), exist_ok=True)
            
            with open(cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
            log.info(f"Cache de datas salvo: {len(cache_data)} entradas")
        except Exception as e:
            log.error(f"Erro ao salvar cache de datas: {e}")
            log.debug(traceback.format_exc())
    
    # --- Lógica de detecção de data mais antiga (continuação) ---

    def get_symbols_count(self):
        """
        Retorna o número de símbolos disponíveis no MT5.
        
        Esta função é útil para verificações rápidas de conexão com o MT5,
        pois qualquer terminal MT5 deve ter acesso a pelo menos alguns símbolos.
        
        Returns:
            int: Número de símbolos disponíveis ou 0 se houver falha.
        """
        foi_inicializado_agora = False
        
        try:
            # Verificar se MT5 já está inicializado
            if not mt5.terminal_info():
                log.info("MT5 não inicializado para contagem de símbolos. Tentando inicializar...")
                
                # Tentativa rápida de inicialização
                if not mt5.initialize():
                    log.warning("Falha ao inicializar MT5 para contagem de símbolos")
                    return 0
                else:
                    foi_inicializado_agora = True
                    log.info("MT5 inicializado temporariamente para contagem de símbolos")
            
            # Obter a contagem de símbolos
            symbols = mt5.symbols_get()
            count = len(symbols) if symbols is not None else 0
            
            if count == 0:
                log.warning("MT5 não retornou nenhum símbolo disponível")
            else:
                log.info(f"MT5 tem {count} símbolos disponíveis")
                
            return count
                
        except Exception as e:
            log.error(f"Erro ao obter contagem de símbolos do MT5: {e}")
            return 0
            
        finally:
            # Se inicializamos apenas para esta operação, fechar a conexão
            if foi_inicializado_agora:
                log.info("Fechando conexão temporária com MT5 após contagem de símbolos")
                mt5.shutdown()
                
    def force_connection_check(self, recursion_count=0):
        """
        Realiza uma verificação agressiva da conexão com o MT5 e tenta restabelecer se necessário.
        Esta função é mais invasiva que is_initialized e deve ser usada apenas quando necessário.
        
        Args:
            recursion_count (int): Contador de recursão para evitar chamadas infinitas
            
        Returns:
            bool: True se a conexão for estabelecida com sucesso, False caso contrário
        """
        # Evitar recursão infinita
        if recursion_count > 2:  # Limitar a 3 tentativas (0, 1, 2)
            log.error("Limite de recursão atingido em force_connection_check. Abortando.")
            return False
            
        log.info(f"Iniciando verificação agressiva de conexão com MT5... (tentativa {recursion_count + 1}/3)")
        
        # Verifica se o processo do MT5 está em execução
        is_running = self._is_mt5_running()
        if not is_running:
            log.warning("MT5 não está em execução. Tentando iniciar...")
            # Tentar iniciar o MT5
            started = self._start_mt5_if_not_running()
            if not started:
                log.error("Não foi possível iniciar o MT5.")
                return False
            # Aguardar um pouco para o MT5 iniciar
            time.sleep(3)
        
        # Verifica se já está inicializado
        if self.is_initialized:
            # Testar acesso às funções básicas
            try:
                symbols = mt5.symbols_get()
                if symbols and len(symbols) > 0:
                    log.info(f"Conexão com MT5 já estabelecida e funcionando ({len(symbols)} símbolos disponíveis)")
                    return True
            except Exception as e:
                log.warning(f"MT5 marcado como inicializado mas falhou ao verificar símbolos: {e}")
                
        # Tentar reiniciar a conexão
        log.info("Tentando encerrar conexão atual e reconectar...")
        try:
            # Forçar desconexão para limpar qualquer estado
            if hasattr(mt5, 'shutdown'):
                mt5.shutdown()
                time.sleep(1)
        except Exception as e:
            log.warning(f"Erro ao desconectar MT5: {e}")
            
        # Tentar cada estratégia de conexão
        for strategy in [
            {"description": "Padrão", "params": {"path": self.mt5_path}},
            {"description": "Timeout longo", "params": {"path": self.mt5_path, "timeout": 30000}},
            {"description": "Portátil", "params": {"path": self.mt5_path, "portable": True}},
            {"description": "Servidor local", "params": {"server": "localhost"}},
        ]:
            log.info(f"Tentando estratégia: {strategy['description']}")
            try:
                result = mt5.initialize(**strategy["params"])
                if result:
                    # Verificar acesso a funcionalidades básicas
                    try:
                        symbols = mt5.symbols_get()
                        if symbols and len(symbols) > 0:
                            log.info(f"Conexão estabelecida com sucesso! ({len(symbols)} símbolos)")
                            self.is_initialized = True
                            return True
                        else:
                            log.warning("MT5 inicializado mas sem acesso a símbolos")
                    except Exception as symbols_err:
                        log.warning(f"Erro ao verificar símbolos: {symbols_err}")
                else:
                    error_code = mt5.last_error()
                    log.warning(f"Falha na estratégia {strategy['description']}: {error_code}")
            except Exception as e:
                log.warning(f"Erro na estratégia {strategy['description']}: {e}")
            
            # Breve pausa antes da próxima tentativa
            time.sleep(1)
            
        # Se chegou aqui, todas as estratégias falharam
        log.error("Todas as estratégias de conexão falharam")
        self.is_initialized = False
        return False

    @with_error_handling(error_type=MT5ConnectionError)
    def get_last_bars(self, symbol, count=1, timeframe='1min'):
        """
        Obtém as últimas barras de um símbolo específico.
        
        Args:
            symbol (str): Símbolo para o qual obter as barras
            count (int): Número de barras a serem obtidas
            timeframe (str ou int): Timeframe dos dados (ex: '1min', '5min', '1h', '1d') ou valor do timeframe MT5
            
        Returns:
            pandas.DataFrame: DataFrame com os dados das barras ou None em caso de erro
        """
        if not self.is_initialized or not mt5:
            log.warning(f"Tentativa de obter as últimas barras para {symbol} sem conexão MT5 inicializada.")
            return None
            
        try:
            # Corrigir o símbolo automaticamente
            original_symbol = symbol
            symbol = self.auto_correct_symbol(symbol)
            if original_symbol != symbol:
                log.info(f"Símbolo corrigido para obter últimas barras: {original_symbol} -> {symbol}")
            
            # Converter o timeframe para o formato do MT5
            mt5_timeframe = timeframe
            if not isinstance(timeframe, int) or timeframe not in [
                mt5.TIMEFRAME_M1, mt5.TIMEFRAME_M2, mt5.TIMEFRAME_M3, mt5.TIMEFRAME_M4, 
                mt5.TIMEFRAME_M5, mt5.TIMEFRAME_M6, mt5.TIMEFRAME_M10, mt5.TIMEFRAME_M12, 
                mt5.TIMEFRAME_M15, mt5.TIMEFRAME_M20, mt5.TIMEFRAME_M30, 
                mt5.TIMEFRAME_H1, mt5.TIMEFRAME_H2, mt5.TIMEFRAME_H3, mt5.TIMEFRAME_H4, 
                mt5.TIMEFRAME_H6, mt5.TIMEFRAME_H8, mt5.TIMEFRAME_H12, 
                mt5.TIMEFRAME_D1, mt5.TIMEFRAME_W1, mt5.TIMEFRAME_MN1
            ]:
                mt5_timeframe = self._convert_timeframe_to_mt5(timeframe)
                if mt5_timeframe is None:
                    log.error(f"Timeframe inválido: {timeframe}")
                    return None
                    
            # Validar símbolo
            if not self.validate_symbol(symbol):
                log.warning(f"Símbolo {symbol} inválido ou indisponível para obter últimas barras.")
                return None
                
            # Obter as barras usando copy_rates_from_pos com sistema de retry
            log.debug(f"Obtendo {count} últimas barras para {symbol} no timeframe {timeframe} ({mt5_timeframe})")
            
            # Implementar sistema de retry para robustez
            max_retries = 3
            retry_count = 0
            rates = None
                
            while retry_count < max_retries:
                try:
                    rates = mt5.copy_rates_from_pos(symbol, mt5_timeframe, 0, count)
                    
                    # Se obteve dados com sucesso, sai do loop
                    if rates is not None and len(rates) > 0:
                        break
                        
                    # Se não obteve dados, registra erro e tenta novamente
                    error = mt5.last_error()
                    log.warning(f"Tentativa {retry_count+1}/{max_retries}: Falha ao obter últimas barras para {symbol}. Erro MT5: {error}")
                    
                    # Esperar antes de tentar novamente
                    time.sleep(0.5)
                    retry_count += 1
                    
                except Exception as retry_error:
                    log.warning(f"Tentativa {retry_count+1}/{max_retries}: Exceção ao obter últimas barras para {symbol}: {retry_error}")
                    time.sleep(0.5)
                    retry_count += 1
            
            # Verificar se conseguiu obter dados após as tentativas
            if rates is None or len(rates) == 0:
                error = mt5.last_error()
                log.warning(f"Nenhum dado retornado para {symbol} no timeframe {timeframe} após {max_retries} tentativas. Erro MT5: {error}")
                return None
                
            # Converter para DataFrame
            df = pd.DataFrame(rates)
            
            # Converter timestamp para datetime
            df['time'] = pd.to_datetime(df['time'], unit='s')
            
            log.debug(f"Obtidas {len(df)} barras para {symbol} no timeframe {timeframe}")
            return df
            
        except Exception as e:
            log.error(f"Erro ao obter as últimas barras para {symbol}: {e}")
            log.debug(traceback.format_exc())
            return None

    def _convert_timeframe_to_mt5(self, timeframe_str):
        """
        Converte uma string de timeframe (ex: '1min') ou valor inteiro para o valor correspondente do MT5.
        
        Args:
            timeframe_str (str ou int): String representando o timeframe ou valor inteiro diretamente
            
        Returns:
            int: Valor do timeframe do MT5 ou None se não for possível converter
        """
        if not self.is_initialized or not mt5:
            log.warning("MT5 não inicializado ao tentar converter timeframe")
            return None

        # Se já for um dos valores numéricos do MT5, retorna diretamente
        if isinstance(timeframe_str, int):
            # Verifica se é um dos valores válidos do MT5
            valid_timeframes = [
                mt5.TIMEFRAME_M1, mt5.TIMEFRAME_M2, mt5.TIMEFRAME_M3, mt5.TIMEFRAME_M4, 
                mt5.TIMEFRAME_M5, mt5.TIMEFRAME_M6, mt5.TIMEFRAME_M10, mt5.TIMEFRAME_M12, 
                mt5.TIMEFRAME_M15, mt5.TIMEFRAME_M20, mt5.TIMEFRAME_M30, 
                mt5.TIMEFRAME_H1, mt5.TIMEFRAME_H2, mt5.TIMEFRAME_H3, mt5.TIMEFRAME_H4, 
                mt5.TIMEFRAME_H6, mt5.TIMEFRAME_H8, mt5.TIMEFRAME_H12, 
                mt5.TIMEFRAME_D1, mt5.TIMEFRAME_W1, mt5.TIMEFRAME_MN1
            ]
            if timeframe_str in valid_timeframes:
                return timeframe_str
                
            # É um inteiro, mas não é um valor direto do MT5, tenta interpretar como minutos
            log.warning(f"Valor de timeframe {timeframe_str} não é diretamente um valor MT5, tentando interpretar como minutos")
            # Continua com a conversão abaixo

        # Mapeamento de strings para valores do MT5
        timeframe_map = {
            'm1': mt5.TIMEFRAME_M1,
            'm5': mt5.TIMEFRAME_M5,
            'm15': mt5.TIMEFRAME_M15,
            'm30': mt5.TIMEFRAME_M30,
            'h1': mt5.TIMEFRAME_H1,
            'h4': mt5.TIMEFRAME_H4,
            'd1': mt5.TIMEFRAME_D1,
            'w1': mt5.TIMEFRAME_W1,
            'mn1': mt5.TIMEFRAME_MN1,
            # Mais aliases para flexibilidade
            '1m': mt5.TIMEFRAME_M1,
            '5m': mt5.TIMEFRAME_M5,
            '15m': mt5.TIMEFRAME_M15,
            '30m': mt5.TIMEFRAME_M30,
            'h': mt5.TIMEFRAME_H1,
            '4hour': mt5.TIMEFRAME_H4,
            'day': mt5.TIMEFRAME_D1,
            'week': mt5.TIMEFRAME_W1,
            'month': mt5.TIMEFRAME_MN1,
            # Aliases em português
            'minuto': mt5.TIMEFRAME_M1,
            '1min': mt5.TIMEFRAME_M1,
            '5min': mt5.TIMEFRAME_M5,
            '15min': mt5.TIMEFRAME_M15,
            '30min': mt5.TIMEFRAME_M30,
            'hora': mt5.TIMEFRAME_H1,
            '4horas': mt5.TIMEFRAME_H4,
            'dia': mt5.TIMEFRAME_D1,
            'diario': mt5.TIMEFRAME_D1,
            'semana': mt5.TIMEFRAME_W1,
            'semanal': mt5.TIMEFRAME_W1,
            'mes': mt5.TIMEFRAME_MN1,
            'mensal': mt5.TIMEFRAME_MN1
        }
        
        # Normaliza a string para lowercase e sem espaços
        if isinstance(timeframe_str, str):
            normalized = timeframe_str.lower().replace(' ', '')
            
            if normalized in timeframe_map:
                return timeframe_map[normalized]
                
            # Tratar casos como '1', '5', etc.
            try:
                # Se for apenas um número, assume que são minutos
                minutes = int(normalized)
                if minutes == 1:
                    return mt5.TIMEFRAME_M1
                elif minutes == 5:
                    return mt5.TIMEFRAME_M5
                elif minutes == 15:
                    return mt5.TIMEFRAME_M15
                elif minutes == 30:
                    return mt5.TIMEFRAME_M30
                elif minutes == 60:
                    return mt5.TIMEFRAME_H1
                elif minutes == 240:
                    return mt5.TIMEFRAME_H4
                elif minutes == 1440:
                    return mt5.TIMEFRAME_D1
                elif minutes == 10080:
                    return mt5.TIMEFRAME_W1
                elif minutes == 43200:
                    return mt5.TIMEFRAME_MN1
            except ValueError:
                # Não é um número puro
                pass
        else:
            # Se não for string nem um valor válido do MT5, tenta interpretar como minutos
            minutes = int(timeframe_str)
            if minutes == 1:
                return mt5.TIMEFRAME_M1
            elif minutes == 5:
                return mt5.TIMEFRAME_M5
            elif minutes == 15:
                return mt5.TIMEFRAME_M15
            elif minutes == 30:
                return mt5.TIMEFRAME_M30
            elif minutes == 60:
                return mt5.TIMEFRAME_H1
            elif minutes == 240:
                return mt5.TIMEFRAME_H4
            elif minutes == 1440:
                return mt5.TIMEFRAME_D1
            elif minutes == 10080:
                return mt5.TIMEFRAME_W1
            elif minutes == 43200:
                return mt5.TIMEFRAME_MN1
            
        log.warning(f"Timeframe não reconhecido: {timeframe_str}, usando padrão TIMEFRAME_M1")
        return mt5.TIMEFRAME_M1

    @with_error_handling(error_type=MT5ConnectionError)
    def get_historical_data(self, symbol, timeframe='1min', bars=None, start_dt=None, end_dt=None):
        """
        Obtém dados históricos para um símbolo específico.
        
        Args:
            symbol (str): Símbolo para o qual obter os dados históricos
            timeframe (str ou int): Timeframe dos dados (ex: '1min', '5min', '1h', '1d') ou valor do timeframe MT5
            bars (int, optional): Número de barras a serem obtidas. Se None, usa start_dt e end_dt.
            start_dt (datetime, optional): Data inicial para obter dados
            end_dt (datetime, optional): Data final para obter dados
            
        Returns:
            pandas.DataFrame: DataFrame com os dados históricos ou None em caso de erro
        """
        if not self.is_initialized or not mt5:
            log.warning(f"Tentativa de obter dados históricos para {symbol} sem conexão MT5 inicializada.")
            return None
            
        try:
            # Corrigir o símbolo automaticamente
            original_symbol = symbol
            symbol = self.auto_correct_symbol(symbol)
            # Log para depuração da correção do símbolo
            log.debug(f"get_historical_data: Símbolo original='{original_symbol}', Símbolo corrigido='{symbol}'")
            if original_symbol != symbol:
                log.info(f"Símbolo corrigido para obter dados históricos: {original_symbol} -> {symbol}")
            
            # Converter o timeframe para o formato do MT5
            mt5_timeframe = timeframe
            if not isinstance(timeframe, int) or timeframe not in [
                mt5.TIMEFRAME_M1, mt5.TIMEFRAME_M2, mt5.TIMEFRAME_M3, mt5.TIMEFRAME_M4, 
                mt5.TIMEFRAME_M5, mt5.TIMEFRAME_M6, mt5.TIMEFRAME_M10, mt5.TIMEFRAME_M12, 
                mt5.TIMEFRAME_M15, mt5.TIMEFRAME_M20, mt5.TIMEFRAME_M30, 
                mt5.TIMEFRAME_H1, mt5.TIMEFRAME_H2, mt5.TIMEFRAME_H3, mt5.TIMEFRAME_H4, 
                mt5.TIMEFRAME_H6, mt5.TIMEFRAME_H8, mt5.TIMEFRAME_H12, 
                mt5.TIMEFRAME_D1, mt5.TIMEFRAME_W1, mt5.TIMEFRAME_MN1
            ]:
                mt5_timeframe = self._convert_timeframe_to_mt5(timeframe)
                if mt5_timeframe is None:
                    log.error(f"Timeframe inválido: {timeframe}")
                    return None
            
            # Validar símbolo
            if not self.validate_symbol(symbol):
                log.warning(f"Símbolo {symbol} inválido ou indisponível para obter dados históricos.")
                return None
                
            # Registrar detalhes da solicitação para depuração
            if bars is not None:
                log.debug(f"Obtendo {bars} barras históricas para {symbol} no timeframe {timeframe} ({mt5_timeframe})")
            elif start_dt is not None and end_dt is not None:
                log.debug(f"Obtendo dados históricos para {symbol} de {start_dt} a {end_dt} no timeframe {timeframe} ({mt5_timeframe})")
            elif start_dt is not None:
                log.debug(f"Obtendo dados históricos para {symbol} a partir de {start_dt} no timeframe {timeframe} ({mt5_timeframe})")
            else:
                log.debug(f"Obtendo dados históricos padrão para {symbol} no timeframe {timeframe} ({mt5_timeframe})")
                
            # Implementar sistema de retry para robustez
            max_retries = 3
            retry_count = 0
            rates = None
                
            while retry_count < max_retries:
                try:
                    # Determinar o método e parâmetros de obtenção de dados
                    params_str = "N/A" # Valor padrão
                    if bars is not None:
                        # Obter um número específico de barras (mais recentes)
                        params_str = f"copy_rates_from_pos(symbol='{symbol}', timeframe={mt5_timeframe}, start_pos=0, count={bars})"
                        log.debug(f"Tentando obter dados via: {params_str}")
                        rates = mt5.copy_rates_from_pos(symbol, mt5_timeframe, 0, bars)
                    elif start_dt is not None and end_dt is not None:
                        # Obter dados em um intervalo específico
                        params_str = f"copy_rates_range(symbol='{symbol}', timeframe={mt5_timeframe}, date_from={start_dt}, date_to={end_dt})"
                        log.debug(f"Tentando obter dados via: {params_str}")
                        rates = mt5.copy_rates_range(symbol, mt5_timeframe, start_dt, end_dt)
                    elif start_dt is not None:
                        # Obter dados a partir de uma data específica (até o presente)
                        count = 5000 # Usar o máximo de barras padrão
                        params_str = f"copy_rates_from(symbol='{symbol}', timeframe={mt5_timeframe}, date_from={start_dt}, count={count})"
                        log.debug(f"Tentando obter dados via: {params_str}")
                        rates = mt5.copy_rates_from(symbol, mt5_timeframe, start_dt, count)
                    else:
                        # Se nenhum parâmetro específico foi fornecido, usa um número padrão de barras
                        default_bars = 1000
                        params_str = f"copy_rates_from_pos(symbol='{symbol}', timeframe={mt5_timeframe}, start_pos=0, count={default_bars})"
                        log.debug(f"Tentando obter dados via: {params_str}")
                        rates = mt5.copy_rates_from_pos(symbol, mt5_timeframe, 0, default_bars)
                    
                    # Se obteve dados com sucesso, sai do loop
                    if rates is not None and len(rates) > 0:
                        break
                        
                    # Se não obteve dados, registra erro e tenta novamente
                    error = mt5.last_error()
                    log.warning(f"Tentativa {retry_count+1}/{max_retries}: Falha ao obter dados para {symbol} usando {params_str}. Erro MT5: {error}")
                    
                    # Esperar antes de tentar novamente
                    time.sleep(0.5)
                    retry_count += 1
                    
                except Exception as retry_error:
                    log.warning(f"Tentativa {retry_count+1}/{max_retries}: Exceção ao obter dados para {symbol} usando {params_str}: {retry_error}")
                    time.sleep(0.5)
                    retry_count += 1
            
            # Verificar se conseguiu obter dados após as tentativas
            if rates is None or len(rates) == 0:
                error = mt5.last_error()
                # Usar params_str da última tentativa
                log.warning(f"Nenhum dado histórico retornado para {symbol} no timeframe {timeframe} após {max_retries} tentativas (última tentativa com {params_str}). Erro MT5: {error}")
                return None
                
            # Converter para DataFrame
            df = pd.DataFrame(rates)
            
            # Converter timestamp para datetime
            df['time'] = pd.to_datetime(df['time'], unit='s')
            
            log.debug(f"Obtidas {len(df)} barras históricas para {symbol} no timeframe {timeframe}")
            return df
            
        except Exception as e:
            log.error(f"Erro ao obter dados históricos para {symbol}: {e}")
            log.debug(traceback.format_exc())
            return None

# Exemplo de uso (para teste)
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    log.info("Testando MT5Connector...")

    # Garante que o diretório config e o arquivo existam para teste
    if not os.path.exists("config"):
        os.makedirs("config")
    if not os.path.exists(DEFAULT_CONFIG_PATH):
        log.warning(f"Arquivo de configuração {DEFAULT_CONFIG_PATH} não encontrado. Criando um exemplo.")
        with open(DEFAULT_CONFIG_PATH, "w") as f:
            f.write("[MT5]\n")
            # Tenta encontrar um caminho válido ou deixa em branco
            mt5_test_path = r"C:\Program Files\MetaTrader 5" # Exemplo comum
            if not os.path.exists(os.path.join(mt5_test_path, "terminal64.exe")):
                 mt5_test_path = "" # Deixa em branco se o exemplo não for válido
            f.write(f"path = {mt5_test_path}\n\n")
            f.write("[DATABASE]\n")
            f.write("type = sqlite\n")
            f.write("path = database/mt5_data.db\n")

    connector = MT5Connector()
    status = connector.get_connection_status()
    log.info(f"Status inicial: {status}")

    if connector.mt5_path: # Só tenta inicializar se o caminho foi carregado
        log.info("Tentando inicializar...")
        success = connector.initialize()
        status = connector.get_connection_status()
        log.info(f"Resultado da inicialização: {success}")
        log.info(f"Status após inicialização: {status}")

        if success:
            # Exemplo: Obter informações da conta se conectado
            try:
                if mt5 and connector.is_initialized:
                    account_info = mt5.account_info()
                    if account_info:
                        log.info(f"Informações da conta: Login={account_info.login}, Servidor={account_info.server}")
                    else:
                         log.warning("Não foi possível obter informações da conta.")
            except Exception as e:
                log.error(f"Erro ao obter informações da conta: {e}")

            log.info("Tentando encerrar conexão...")
            connector.shutdown()
            status = connector.get_connection_status()
            log.info(f"Status após shutdown: {status}")
        else:
            log.warning("Inicialização falhou.")
    else:
        log.error("Não foi possível iniciar o teste pois o caminho do MT5 não está configurado.")

    log.info("Teste do MT5Connector concluído.")