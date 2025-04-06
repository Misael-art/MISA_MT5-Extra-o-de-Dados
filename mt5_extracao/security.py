import os
import logging
import json
from pathlib import Path
from base64 import b64encode, b64decode
from datetime import datetime
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from dotenv import load_dotenv, set_key

try:
    from dotenv import load_dotenv
    DOTENV_AVAILABLE = True
except ImportError:
    DOTENV_AVAILABLE = False

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
    fh = logging.FileHandler("logs/security.log", encoding="utf-8")
    fh.setFormatter(formatter)
    log.addHandler(fh)

class CredentialManager:
    """
    Gerencia o armazenamento e recuperação segura de credenciais.
    Suporta armazenamento via arquivo .env e criptografia para dados sensíveis.
    """
    
    def __init__(self, app_name="MT5Extracao"):
        """
        Inicializa o gerenciador de credenciais.
        
        Args:
            app_name (str): Nome da aplicação para identificação das credenciais
        """
        self.app_name = app_name
        self.logger = logging.getLogger(f"{self.app_name}.security")
        
        # Caminho para o arquivo .env
        self.env_path = Path(".env")
        
        # Caminho para o arquivo de credenciais criptografadas
        self.cred_path = Path("credentials.json")
        
        # Caminho para o arquivo de chave
        self.key_path = Path(f"{self.app_name.lower()}.key")
        
        # Carrega as variáveis de ambiente do arquivo .env se existir
        self._load_env()
        
        # Inicializa a chave Fernet para criptografia
        self._initialize_crypto()
    
    def _load_env(self):
        """Carrega variáveis de ambiente do arquivo .env"""
        if self.env_path.exists():
            load_dotenv(self.env_path)
            self.logger.debug("Arquivo .env carregado")
        else:
            self.logger.debug("Arquivo .env não encontrado")
    
    def _initialize_crypto(self):
        """Inicializa ou carrega a chave de criptografia"""
        try:
            if self.key_path.exists():
                with open(self.key_path, "rb") as key_file:
                    key = key_file.read()
                self.fernet = Fernet(key)
                self.logger.debug("Chave de criptografia carregada")
            else:
                # Gera uma nova chave
                key = Fernet.generate_key()
                with open(self.key_path, "wb") as key_file:
                    key_file.write(key)
                self.fernet = Fernet(key)
                self.logger.debug("Nova chave de criptografia gerada")
                
                # Define permissões restritas
                os.chmod(self.key_path, 0o600)
        except Exception as e:
            self.logger.error(f"Erro ao inicializar criptografia: {str(e)}")
            self.fernet = None
    
    def _derive_key(self, password, salt=None):
        """
        Deriva uma chave de criptografia a partir de uma senha.
        
        Args:
            password (str): Senha para derivar a chave
            salt (bytes, optional): Salt para a derivação. Se None, gera um novo.
            
        Returns:
            tuple: (chave derivada, salt usado)
        """
        if salt is None:
            salt = os.urandom(16)
        
        password_bytes = password.encode('utf-8')
        
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )
        
        key = base64.urlsafe_b64encode(kdf.derive(password_bytes))
        return key, salt
    
    def get_mt5_credentials(self):
        """
        Obtém as credenciais do MT5 armazenadas.
        
        Returns:
            dict: Um dicionário com as credenciais (login, password, server)
            ou None se não encontradas
        """
        # Primeiro tenta obter das variáveis de ambiente
        login = os.getenv("MT5_LOGIN")
        password = os.getenv("MT5_PASSWORD")
        server = os.getenv("MT5_SERVER")
        
        if login and password:
            return {
                "login": login,
                "password": password,
                "server": server if server else ""
            }
        
        # Se não encontrar no .env, tenta no arquivo de credenciais
        if self.fernet and self.cred_path.exists():
            try:
                with open(self.cred_path, "r") as f:
                    encrypted_data = json.load(f)
                
                if "mt5" in encrypted_data:
                    encrypted_creds = encrypted_data["mt5"]
                    decrypted_data = self.fernet.decrypt(
                        encrypted_creds.encode('utf-8')
                    ).decode('utf-8')
                    
                    mt5_creds = json.loads(decrypted_data)
                    return mt5_creds
            except Exception as e:
                self.logger.error(f"Erro ao recuperar credenciais do MT5: {str(e)}")
        
        return None
    
    def save_mt5_credentials(self, login, password, server="", use_env=True):
        """
        Salva as credenciais do MT5.
        
        Args:
            login (str): Login do MT5
            password (str): Senha do MT5
            server (str, optional): Servidor do MT5
            use_env (bool): Se True, salva no arquivo .env
        
        Returns:
            bool: True se salvou com sucesso, False caso contrário
        """
        try:
            # Dados a serem salvos
            mt5_creds = {
                "login": str(login),
                "password": password,
                "server": server
            }
            
            # Salva no arquivo .env se solicitado
            if use_env:
                set_key(self.env_path, "MT5_LOGIN", str(login))
                set_key(self.env_path, "MT5_PASSWORD", password)
                if server:
                    set_key(self.env_path, "MT5_SERVER", server)
                self.logger.debug("Credenciais do MT5 salvas no arquivo .env")
            
            # Adicionalmente, salva no arquivo de credenciais criptografado
            if self.fernet:
                all_creds = {}
                
                # Carrega credenciais existentes, se houver
                if self.cred_path.exists():
                    try:
                        with open(self.cred_path, "r") as f:
                            all_creds = json.load(f)
                    except:
                        pass
                
                # Criptografa as credenciais do MT5
                mt5_json = json.dumps(mt5_creds)
                encrypted_mt5 = self.fernet.encrypt(mt5_json.encode('utf-8')).decode('utf-8')
                
                # Atualiza o dicionário de credenciais
                all_creds["mt5"] = encrypted_mt5
                
                # Salva no arquivo
                with open(self.cred_path, "w") as f:
                    json.dump(all_creds, f)
                
                # Define permissões restritas
                os.chmod(self.cred_path, 0o600)
                
                self.logger.debug("Credenciais do MT5 salvas no arquivo criptografado")
            
            return True
        
        except Exception as e:
            self.logger.error(f"Erro ao salvar credenciais do MT5: {str(e)}")
            return False
    
    def clear_credentials(self):
        """
        Remove todas as credenciais armazenadas.
        
        Returns:
            bool: True se limpou com sucesso, False caso contrário
        """
        try:
            # Remove variáveis do arquivo .env
            if self.env_path.exists():
                # Tenta preservar outras variáveis não relacionadas a credenciais
                env_content = {}
                with open(self.env_path, "r") as f:
                    for line in f:
                        if "=" in line and not line.startswith("#"):
                            key, value = line.strip().split("=", 1)
                            if not key.startswith("MT5_"):
                                env_content[key] = value
                
                # Reescreve o arquivo sem as credenciais
                with open(self.env_path, "w") as f:
                    for key, value in env_content.items():
                        f.write(f"{key}={value}\n")
            
            # Remove o arquivo de credenciais criptografadas
            if self.cred_path.exists():
                os.remove(self.cred_path)
            
            self.logger.info("Todas as credenciais foram removidas")
            return True
            
        except Exception as e:
            self.logger.error(f"Erro ao limpar credenciais: {str(e)}")
            return False
    
    def test_mt5_connection(self, login=None, password=None, server=None):
        """
        Testa a conexão com o MT5 usando as credenciais fornecidas ou armazenadas.
        
        Args:
            login (str, optional): Login para testar. Se None, usa as credenciais armazenadas.
            password (str, optional): Senha para testar. Se None, usa as credenciais armazenadas.
            server (str, optional): Servidor para testar. Se None, usa as credenciais armazenadas.
        
        Returns:
            bool: True se conectou com sucesso, False caso contrário
        """
        try:
            import MetaTrader5 as mt5
            
            # Se não forneceu credenciais, tenta usar as armazenadas
            if login is None or password is None:
                stored_creds = self.get_mt5_credentials()
                if not stored_creds:
                    self.logger.error("Não há credenciais armazenadas para testar")
                    return False
                
                login = stored_creds["login"]
                password = stored_creds["password"]
                server = stored_creds.get("server", "")
            
            # Inicializa o MT5
            if not mt5.initialize():
                self.logger.error(f"Falha ao inicializar MT5: {mt5.last_error()}")
                return False
            
            # Tenta fazer login
            login_result = mt5.login(
                login=int(login),
                password=password,
                server=server
            )
            
            # Encerra o MT5
            mt5.shutdown()
            
            if login_result:
                self.logger.info("Teste de conexão com MT5 bem-sucedido")
                return True
            else:
                self.logger.error(f"Falha no login: {mt5.last_error()}")
                return False
                
        except Exception as e:
            self.logger.error(f"Erro ao testar conexão com MT5: {str(e)}")
            return False

# Classe simples para obfuscação básica de dados sensíveis
# Nota: Isto não é criptografia de alto nível, apenas obfuscação básica 
# para evitar armazenamento de credenciais em texto puro
class SimpleObfuscator:
    """
    Fornece obfuscação básica para dados sensíveis.
    Não use para segurança de alto nível - é apenas para evitar
    armazenamento de texto puro em arquivos de configuração.
    """
    
    @staticmethod
    def obfuscate(text, key=None):
        """
        Obfusca um texto usando uma chave simples.
        
        Args:
            text (str): Texto a ser ofuscado
            key (str, optional): Chave para ofuscar. Padrão: timestamp atual
            
        Returns:
            str: Texto ofuscado em base64
        """
        if not text:
            return None
            
        if not key:
            # Usar timestamp como chave padrão
            key = str(datetime.now().timestamp())
            
        # Algoritmo simples: XOR entre texto e chave
        result = []
        key_bytes = key.encode()
        key_len = len(key_bytes)
        text_bytes = text.encode()
        
        for i, char in enumerate(text_bytes):
            key_char = key_bytes[i % key_len]
            result.append(char ^ key_char)
            
        # Codificar em base64 para armazenamento/transmissão
        return b64encode(bytes(result)).decode()
    
    @staticmethod
    def deobfuscate(obfuscated_text, key=None):
        """
        Recupera um texto ofuscado usando a mesma chave.
        
        Args:
            obfuscated_text (str): Texto ofuscado a ser recuperado
            key (str): Chave usada para ofuscar
            
        Returns:
            str: Texto original
        """
        if not obfuscated_text:
            return None
            
        if not key:
            # Se não foi fornecida chave, tenta usar timestamp atual 
            # (normalmente não vai funcionar a menos que seja o mesmo usado para ofuscar)
            key = str(datetime.now().timestamp())
            
        try:
            # Decodificar da base64
            data = b64decode(obfuscated_text)
            
            # Aplicar o mesmo algoritmo XOR para reverter
            result = []
            key_bytes = key.encode()
            key_len = len(key_bytes)
            
            for i, char in enumerate(data):
                key_char = key_bytes[i % key_len]
                result.append(char ^ key_char)
                
            return bytes(result).decode()
        except Exception as e:
            log.error(f"Erro ao desobfuscar: {e}")
            return None

def create_gitignore_entry():
    """
    Adiciona entradas relacionadas à segurança no arquivo .gitignore
    
    Returns:
        bool: True se foi adicionado com sucesso
    """
    try:
        # Verificar se o arquivo .gitignore existe
        gitignore_file = Path(".gitignore")
        if not gitignore_file.exists():
            # Criar arquivo
            with open(gitignore_file, "w", encoding="utf-8") as f:
                f.write("# Arquivos de credenciais e segurança\n")
                f.write(".env\n")
                f.write("*.key\n")
                f.write("credentials.json\n")
            log.info("Arquivo .gitignore criado com entradas de segurança.")
            return True
        
        # Se o arquivo já existe, verificar se já tem as entradas relacionadas à segurança
        with open(gitignore_file, "r", encoding="utf-8") as f:
            content = f.read()
            
        # Verificar se já contém as entradas
        entries_to_add = []
        if ".env" not in content:
            entries_to_add.append(".env")
        if "*.key" not in content:
            entries_to_add.append("*.key")
        if "credentials.json" not in content:
            entries_to_add.append("credentials.json")
            
        # Se não tem nenhuma entrada para adicionar, retorna
        if not entries_to_add:
            log.info("Arquivo .gitignore já contém entradas de segurança.")
            return True
            
        # Adicionar entradas ao final do arquivo
        with open(gitignore_file, "a", encoding="utf-8") as f:
            f.write("\n# Arquivos de credenciais e segurança\n")
            for entry in entries_to_add:
                f.write(f"{entry}\n")
                
        log.info(f"Entradas adicionadas ao .gitignore: {entries_to_add}")
        return True
    except Exception as e:
        log.error(f"Erro ao modificar .gitignore: {e}")
        return False 