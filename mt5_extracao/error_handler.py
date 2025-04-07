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
        # Ignora KeyboardInterrupt (Ctrl+C)
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        
        # Gera um ID único para esta exceção baseado no timestamp
        exception_id = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        
        # Formata a mensagem de log
        exception_text = f"Exceção não tratada ({exception_id}):\n"
        exception_text += "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
        
        # Registra no log
        self.logger.critical(exception_text)
        
        # Salva detalhes em arquivo
        self.save_exception_details(exception_id, exception_text)
        
        # Exibe mensagem para o usuário
        try:
            error_message = str(exc_value)
            error_title = f"{self.app_name} - Erro ({exc_type.__name__})"
            detailed_message = (
                f"Ocorreu um erro inesperado: {error_message}\n\n"
                f"O erro foi registrado com ID: {exception_id}\n"
                f"Consulte os logs para mais informações."
            )
            messagebox.showerror(error_title, detailed_message)
        except Exception:
            # Se falhar ao exibir a mensagem, apenas registra no log
            self.logger.error("Não foi possível exibir a mensagem de erro ao usuário")
    
    def save_exception_details(self, exception_id, exception_text):
        """
        Salva detalhes completos da exceção em um arquivo.
        
        Args:
            exception_id (str): ID único da exceção
            exception_text (str): Texto completo da exceção com traceback
        """
        try:
            # Garantir que o diretório existe
            os.makedirs(self.exceptions_dir, exist_ok=True)
            
            # Nome do arquivo baseado no ID da exceção
            filename = f"exception_{exception_id}.log"
            error_file = self.exceptions_dir / filename
            
            # Informações do sistema para diagnóstico
            system_info = {
                "Python": sys.version,
                "Platform": sys.platform,
                "Time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "App": self.app_name
            }
            
            # Escrever no arquivo
            with open(error_file, "w", encoding="utf-8") as f:
                f.write("="*80 + "\n")
                f.write("INFORMAÇÕES DO SISTEMA:\n")
                for key, value in system_info.items():
                    f.write(f"{key}: {value}\n")
                f.write("="*80 + "\n\n")
                f.write(exception_text)
            
            self.logger.info(f"Detalhes da exceção salvos em: {error_file}")
        except Exception as e:
            self.logger.error(f"Erro ao salvar detalhes da exceção: {str(e)}")
    
    def handle_mt5_error(self, error_code, operation, symbol="", timeframe=""):
        """
        Trata erros específicos do MetaTrader 5 de forma padronizada.
        
        Args:
            error_code (int): Código de erro do MT5
            operation (str): Operação que estava sendo realizada
            symbol (str): Símbolo envolvido (opcional)
            timeframe (str): Timeframe envolvido (opcional)
        
        Returns:
            dict: Informações sobre o erro e ações sugeridas
        """
        # Mapeamento de códigos de erro para mensagens e sugestões
        error_map = {
            -10003: {
                "tipo": "Erro de comunicação IPC",
                "mensagem": "Falha na inicialização da comunicação com o MetaTrader 5",
                "sugestão": "Reinicie o MetaTrader 5 com privilégios de administrador"
            },
            -10000: {
                "tipo": "Erro de inicialização",
                "mensagem": "Falha ao inicializar terminal MetaTrader 5",
                "sugestão": "Verifique se o MetaTrader 5 está instalado corretamente"
            },
            -2: {
                "tipo": "Erro de parâmetros",
                "mensagem": "Parâmetros inválidos na operação",
                "sugestão": "Verifique os parâmetros da operação"
            }
        }
        
        # Obter informações do erro ou usar genérico
        error_info = error_map.get(error_code, {
            "tipo": "Erro do MetaTrader 5",
            "mensagem": f"Código de erro: {error_code}",
            "sugestão": "Consulte a documentação do MetaTrader 5"
        })
        
        # Adicionar detalhes da operação
        context = operation
        if symbol:
            context += f" (Símbolo: {symbol}"
            if timeframe:
                context += f", Timeframe: {timeframe}"
            context += ")"
        
        error_info["contexto"] = context
        
        # Registrar no log
        self.logger.error(
            f"{error_info['tipo']} - {error_info['mensagem']} - {context}"
        )
        
        return error_info
    
    def handle_database_error(self, exception, operation):
        """
        Trata erros relacionados ao banco de dados de forma padronizada.
        
        Args:
            exception (Exception): A exceção capturada
            operation (str): Operação que estava sendo realizada
            
        Returns:
            dict: Informações sobre o erro e ações sugeridas
        """
        # Informações sobre o erro
        error_info = {
            "tipo": exception.__class__.__name__,
            "mensagem": str(exception),
            "contexto": operation,
            "sugestão": "Verifique a integridade do banco de dados"
        }
        
        # Adicionar detalhes específicos para DatabaseError
        if isinstance(exception, DatabaseError):
            if hasattr(exception, 'table') and exception.table:
                error_info["tabela"] = exception.table
            if hasattr(exception, 'query') and exception.query:
                error_info["query"] = exception.query
            if hasattr(exception, 'details') and exception.details:
                error_info["detalhes"] = exception.details
        
        # Registrar no log
        log_message = f"{error_info['tipo']} - {error_info['mensagem']} - {operation}"
        if 'tabela' in error_info:
            log_message += f" - Tabela: {error_info['tabela']}"
        self.logger.error(log_message)
        
        return error_info
    
    def show_error_message(self, title, message, details=None):
        """
        Exibe uma mensagem de erro para o usuário.
        
        Args:
            title (str): Título da mensagem
            message (str): Texto da mensagem
            details (str, optional): Detalhes adicionais
            
        Returns:
            bool: True se a mensagem foi exibida, False em caso contrário
        """
        try:
            display_message = message
            if details:
                display_message += f"\n\nDetalhes: {details}"
            
            messagebox.showerror(title, display_message)
            return True
        except Exception as e:
            self.logger.error(f"Erro ao exibir mensagem de erro: {str(e)}")
            return False
    
    def show_warning(self, title, message):
        """
        Exibe uma mensagem de aviso para o usuário.
        
        Args:
            title (str): Título da mensagem
            message (str): Texto da mensagem
            
        Returns:
            bool: True se a mensagem foi exibida, False em caso contrário
        """
        try:
            messagebox.showwarning(title, message)
            return True
        except Exception as e:
            self.logger.error(f"Erro ao exibir mensagem de aviso: {str(e)}")
            return False
    
    def show_info(self, title, message):
        """
        Exibe uma mensagem informativa para o usuário.
        
        Args:
            title (str): Título da mensagem
            message (str): Texto da mensagem
            
        Returns:
            bool: True se a mensagem foi exibida, False em caso contrário
        """
        try:
            messagebox.showinfo(title, message)
            return True
        except Exception as e:
            self.logger.error(f"Erro ao exibir mensagem informativa: {str(e)}")
            return False
    
    def install_global_handler(self):
        """
        Instala o manipulador de exceções global para a aplicação.
        
        Returns:
            bool: True se o manipulador foi instalado com sucesso
        """
        try:
            # Salvar o manipulador original para referência
            self.original_excepthook = sys.excepthook
            
            # Instalar o novo manipulador
            sys.excepthook = self.handle_exception
            
            # Registrar a instalação no log
            self.logger.info("Handler de exceções globais instalado")
            return True
        except Exception as e:
            self.logger.error(f"Erro ao instalar handler de exceções: {str(e)}")
            return False 