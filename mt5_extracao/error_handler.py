import logging
import traceback
import functools
import time
import os
from pathlib import Path
import sys
import datetime
from tkinter import messagebox

# Configuração de logging
log = logging.getLogger(__name__)
if not log.handlers:
    log.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    # Adicionar um handler de console para depuração inicial
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    log.addHandler(ch)
    # Adicionar um handler de arquivo
    os.makedirs("logs", exist_ok=True)
    fh = logging.FileHandler("logs/error_handler.log", encoding="utf-8")
    fh.setFormatter(formatter)
    log.addHandler(fh)

# Definição de exceções personalizadas para o projeto
class MT5Error(Exception):
    """Exceção base para erros relacionados ao MetaTrader 5"""
    def __init__(self, message, error_code=None, details=None):
        self.error_code = error_code
        self.details = details
        super().__init__(message)

class MT5ConnectionError(MT5Error):
    """Exceção para erros de conexão com o MetaTrader 5"""
    pass

class MT5IPCError(MT5ConnectionError):
    """Exceção específica para erros de IPC no MetaTrader 5"""
    pass

class MT5SymbolError(MT5Error):
    """Exceção para erros relacionados a símbolos no MetaTrader 5"""
    pass

class MT5DataError(MT5Error):
    """Exceção para erros relacionados a dados no MetaTrader 5"""
    pass

class DatabaseError(Exception):
    """Exceção base para erros relacionados ao banco de dados"""
    def __init__(self, message, table=None, query=None, details=None):
        self.table = table
        self.query = query
        self.details = details
        super().__init__(message)

class DataTypeError(DatabaseError):
    """Exceção para erros de tipo de dados no banco de dados"""
    pass

class ExportError(Exception):
    """Exceção para erros na exportação de dados"""
    def __init__(self, message, format=None, file_path=None, details=None):
        self.format = format
        self.file_path = file_path
        self.details = details
        super().__init__(message)

def with_error_handling(error_type=None, retry_count=0, retry_delay=1, log_level=logging.ERROR):
    """
    Decorador para tratamento padronizado de erros.
    
    Args:
        error_type (Exception): Tipo de exceção a ser lançada em caso de falha
        retry_count (int): Número de retentativas em caso de falha
        retry_delay (int): Tempo em segundos entre retentativas
        log_level (int): Nível de log para erros (padrão: logging.ERROR)
    
    Returns:
        callable: Decorador configurado
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            attempts = retry_count + 1
            last_error = None
            func_name = func.__name__
            
            # Obter nome da classe se for um método
            if args and hasattr(args[0], '__class__'):
                class_name = args[0].__class__.__name__
                func_name = f"{class_name}.{func_name}"
            
            for attempt in range(attempts):
                try:
                    if attempt > 0:
                        log.info(f"Tentativa {attempt+1}/{attempts} para {func_name}")
                    return func(*args, **kwargs)
                except Exception as e:
                    last_error = e
                    error_cls = e.__class__.__name__
                    
                    # Formato do log: nome da função, erro, detalhes
                    log_message = f"Erro na função {func_name}: [{error_cls}] {str(e)}"
                    
                    if log_level >= logging.ERROR:
                        log.error(log_message)
                        log.error(traceback.format_exc())
                    elif log_level >= logging.WARNING:
                        log.warning(log_message)
                    else:
                        log.info(log_message)
                    
                    # Registrar a exceção em um arquivo específico para diagnóstico
                    _log_exception_to_file(func_name, e)
                    
                    if attempt < retry_count:
                        log.info(f"Aguardando {retry_delay}s antes da próxima tentativa...")
                        time.sleep(retry_delay)
                    else:
                        log.warning(f"Todas as {attempts} tentativas para {func_name} falharam")
            
            # Se chegou aqui, todas as tentativas falharam
            if error_type:
                raise error_type(f"Falha após {attempts} tentativas: {last_error}")
            raise last_error
        
        return wrapper
    return decorator

def _log_exception_to_file(func_name, exception):
    """
    Registra detalhes de uma exceção em um arquivo específico para diagnóstico
    
    Args:
        func_name (str): Nome da função que gerou a exceção
        exception (Exception): A exceção capturada
    """
    try:
        # Garantir que o diretório de logs existe
        error_log_dir = Path("logs/exceptions")
        os.makedirs(error_log_dir, exist_ok=True)
        
        # Nome do arquivo: YYYY-MM-DD_exceptions.log
        from datetime import datetime
        date_str = datetime.now().strftime("%Y-%m-%d")
        log_file = error_log_dir / f"{date_str}_exceptions.log"
        
        # Registrar a exceção com timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(f"\n{'='*80}\n")
            f.write(f"TIMESTAMP: {timestamp}\n")
            f.write(f"FUNÇÃO: {func_name}\n")
            f.write(f"EXCEÇÃO: {exception.__class__.__name__}: {str(exception)}\n")
            f.write(f"TRACEBACK:\n{traceback.format_exc()}\n")
            f.write(f"{'='*80}\n")
    except Exception as e:
        # Em caso de falha no registro, apenas log no console
        log.error(f"Erro ao registrar exceção em arquivo: {e}")

def safe_call(func, default_return=None, **kwargs):
    """
    Função utilitária para chamar uma função com tratamento de erro.
    
    Args:
        func (callable): Função a ser chamada
        default_return: Valor padrão a ser retornado em caso de erro
        **kwargs: Argumentos para passar para a função
    
    Returns:
        O resultado da função ou default_return em caso de erro
    """
    try:
        return func(**kwargs)
    except Exception as e:
        log.warning(f"Erro ao chamar {func.__name__}: {e}")
        return default_return

def check_mt5_error(mt5_result, operation_name="operação MT5"):
    """
    Verifica o resultado de uma operação do MT5 e lança exceção apropriada se houver erro.
    
    Args:
        mt5_result: Resultado da operação MT5
        operation_name (str): Nome da operação para mensagem de erro
    
    Returns:
        O próprio resultado, se não houver erro
    
    Raises:
        MT5Error: Exceção apropriada para o tipo de erro
    """
    import MetaTrader5 as mt5
    
    if mt5_result is None or (isinstance(mt5_result, (bool, int)) and not mt5_result):
        # Obter o último erro do MT5
        error_code = mt5.last_error()[0] if hasattr(mt5, 'last_error') else -1
        error_msg = str(mt5.last_error()) if hasattr(mt5, 'last_error') else "Erro desconhecido"
        
        # Mapear para exceções específicas
        if error_code == -10003:  # IPC initialize failed
            raise MT5IPCError(f"Erro de comunicação IPC com MT5: {error_msg}", error_code)
        elif error_code in [-2, -10000, -10001, -10002]:  # Erros de conexão
            raise MT5ConnectionError(f"Erro de conexão com MT5: {error_msg}", error_code)
        else:
            raise MT5Error(f"Erro na {operation_name}: {error_msg}", error_code)
    
    return mt5_result 

class ErrorHandler:
    """
    Gerenciador centralizado de erros para a aplicação.
    
    Fornece tratamento padronizado para diferentes tipos de erros,
    incluindo logging, exibição de mensagens ao usuário e criação
    de relatórios de erros detalhados.
    """
    
    def __init__(self, app_name="MT5Extracao"):
        """
        Inicializa o tratador de erros
        
        Args:
            app_name (str): Nome da aplicação para identificação nos logs
        """
        self.app_name = app_name
        self.logger = logging.getLogger(f"{app_name}.error_handler")
        
        # Configura o diretório para logs de exceções não tratadas
        self.exceptions_dir = Path("logs/exceptions")
        self.ensure_log_directory()
    
    def ensure_log_directory(self):
        """Garante que o diretório de logs existe"""
        try:
            self.exceptions_dir.mkdir(parents=True, exist_ok=True)
            self.logger.debug(f"Diretório de logs de exceções configurado: {self.exceptions_dir}")
        except Exception as e:
            self.logger.error(f"Não foi possível criar o diretório de logs: {str(e)}")
    
    def handle_exception(self, exc_type, exc_value, exc_traceback):
        """
        Manipula exceções não tratadas globalmente.
        
        Args:
            exc_type: Tipo da exceção
            exc_value: Valor/mensagem da exceção
            exc_traceback: Traceback da exceção
        """
        # Evita handling de KeyboardInterrupt
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        
        # Formata a exceção para log
        exception_text = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
        
        # Gera um ID único para a exceção
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        exception_id = f"{timestamp}_{exc_type.__name__}"
        
        # Registra a exceção no log
        self.logger.critical(f"Exceção não tratada ({exception_id}): {exc_value}")
        self.logger.critical(exception_text)
        
        # Salva detalhes da exceção em arquivo
        self.save_exception_details(exception_id, exception_text)
        
        # Exibe mensagem para o usuário
        self.show_error_message(
            title="Erro Inesperado",
            message=f"Ocorreu um erro inesperado: {exc_value}\n\n"
                    f"O erro foi registrado com ID: {exception_id}\n"
                    f"Consulte os logs para mais detalhes.",
            details=exception_text
        )
    
    def save_exception_details(self, exception_id, exception_text):
        """
        Salva detalhes de uma exceção em arquivo.
        
        Args:
            exception_id (str): Identificador único da exceção
            exception_text (str): Texto completo da exceção
        """
        try:
            # Garante que o diretório existe
            self.ensure_log_directory()
            
            # Cria o arquivo de log da exceção
            exception_file = self.exceptions_dir / f"{exception_id}.log"
            
            with open(exception_file, "w", encoding="utf-8") as f:
                # Adiciona cabeçalho com informações do sistema
                f.write(f"=== Relatório de Erro: {self.app_name} ===\n")
                f.write(f"Data e Hora: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Sistema Operacional: {sys.platform}\n")
                f.write(f"Versão Python: {sys.version}\n")
                f.write("="*50 + "\n\n")
                
                # Adiciona o traceback completo
                f.write(exception_text)
                
                # Adiciona informações das variáveis de ambiente relevantes
                f.write("\n\n=== Variáveis de Ambiente Relevantes ===\n")
                for key, value in os.environ.items():
                    if key.startswith(("PYTHON", "PATH", "MT5_")):
                        # Oculta senhas e dados sensíveis
                        if "PASSWORD" in key or "SENHA" in key or "KEY" in key or "SECRET" in key:
                            value = "********"
                        f.write(f"{key}={value}\n")
            
            self.logger.info(f"Detalhes da exceção salvos em: {exception_file}")
            
        except Exception as e:
            self.logger.error(f"Não foi possível salvar os detalhes da exceção: {str(e)}")
    
    def handle_mt5_error(self, error_code, operation, symbol="", timeframe=""):
        """
        Manipula erros específicos do MetaTrader 5.
        
        Args:
            error_code (int): Código de erro do MT5
            operation (str): Operação que causou o erro
            symbol (str, optional): Símbolo envolvido na operação
            timeframe (str, optional): Timeframe envolvido na operação
            
        Returns:
            str: Mensagem de erro formatada
        """
        import MetaTrader5 as mt5
        
        # Obtém a descrição do erro
        error_description = mt5.last_error()
        
        # Formata os detalhes da operação
        operation_details = f"Operação: {operation}"
        if symbol:
            operation_details += f", Símbolo: {symbol}"
        if timeframe:
            operation_details += f", Timeframe: {timeframe}"
        
        # Monta a mensagem completa
        error_message = f"Erro MT5 {error_code}: {error_description}. {operation_details}"
        
        # Registra no log
        self.logger.error(error_message)
        
        return error_message
    
    def handle_database_error(self, exception, operation):
        """
        Manipula erros específicos de banco de dados.
        
        Args:
            exception (Exception): A exceção de banco de dados
            operation (str): Operação que causou o erro
            
        Returns:
            str: Mensagem de erro formatada
        """
        # Formata a mensagem de erro
        error_message = f"Erro de banco de dados durante {operation}: {str(exception)}"
        
        # Registra no log com detalhes se disponíveis
        self.logger.error(error_message)
        self.logger.debug(f"Detalhes da exceção: {traceback.format_exc()}")
        
        # Trata tipos específicos de erros de banco de dados
        if "no such table" in str(exception).lower():
            suggestion = "A tabela pode não existir. Verifique se o nome está correto."
            error_message += f"\nSugestão: {suggestion}"
        elif "unique constraint" in str(exception).lower():
            suggestion = "Registro duplicado. Os dados já existem no banco de dados."
            error_message += f"\nSugestão: {suggestion}"
        elif "disk" in str(exception).lower() and ("full" in str(exception).lower() or "space" in str(exception).lower()):
            suggestion = "Possível falta de espaço em disco. Verifique o espaço disponível."
            error_message += f"\nSugestão: {suggestion}"
        
        return error_message
    
    def show_error_message(self, title, message, details=None):
        """
        Exibe uma mensagem de erro para o usuário.
        
        Args:
            title (str): Título da mensagem
            message (str): Mensagem principal
            details (str, optional): Detalhes adicionais
        """
        try:
            messagebox.showerror(title, message)
            
            # Se há detalhes e são extensos, registra no log
            if details and len(details) > 500:
                self.logger.debug(f"Detalhes adicionais do erro: {details}")
        except Exception as e:
            # Fallback para console se a GUI falhar
            self.logger.error(f"Não foi possível exibir mensagem de erro: {str(e)}")
            print(f"\nERRO: {title}\n{message}\n")
            if details:
                print(f"Detalhes: {details}\n")
    
    def show_warning(self, title, message):
        """
        Exibe uma mensagem de aviso para o usuário.
        
        Args:
            title (str): Título do aviso
            message (str): Mensagem de aviso
        """
        try:
            messagebox.showwarning(title, message)
        except Exception as e:
            # Fallback para console se a GUI falhar
            self.logger.error(f"Não foi possível exibir mensagem de aviso: {str(e)}")
            print(f"\nAVISO: {title}\n{message}\n")
    
    def show_info(self, title, message):
        """
        Exibe uma mensagem informativa para o usuário.
        
        Args:
            title (str): Título da mensagem
            message (str): Mensagem informativa
        """
        try:
            messagebox.showinfo(title, message)
        except Exception as e:
            # Fallback para console se a GUI falhar
            self.logger.error(f"Não foi possível exibir mensagem informativa: {str(e)}")
            print(f"\nINFO: {title}\n{message}\n")
    
    def install_global_handler(self):
        """
        Instala este handler como o manipulador global de exceções não tratadas.
        """
        sys.excepthook = self.handle_exception
        self.logger.info("Handler de exceções globais instalado") 