import os
import sys
import subprocess
import winreg
import tkinter as tk
from tkinter import filedialog, messagebox
import logging
import traceback

# Garantir que o diretório de logs existe
os.makedirs("logs", exist_ok=True)

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/mt5_instalacao.log"),
        logging.StreamHandler()
    ]
)

def check_python_version():
    """Verifica se a versão do Python é compatível (3.7+)"""
    if sys.version_info < (3, 7):
        logging.error("Versão do Python incompatível. Requer Python 3.7 ou superior.")
        messagebox.showerror("Erro", "Esta aplicação requer Python 3.7 ou superior.")
        return False
    logging.info(f"Versão do Python compatível: {sys.version}")
    return True

def install_dependencies():
    """Instala as dependências necessárias via pip"""
    try:
        logging.info("Atualizando pip...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "pip"], 
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        logging.info("Instalando dependências...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-e", "."], 
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        # Verifica se setuptools está instalado (para pkg_resources)
        try:
            logging.info("Verificando setuptools...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", "setuptools"], 
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except subprocess.CalledProcessError:
            logging.warning("Erro ao instalar setuptools")
        
        logging.info("Dependências instaladas com sucesso")
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"Erro ao instalar dependências: {e}")
        messagebox.showerror("Erro", 
                            f"Falha ao instalar dependências.\nErro: {str(e)}\n\n"
                            "Tente executar manualmente:\n"
                            f"{sys.executable} -m pip install -e .")
        return False
    except Exception as e:
        logging.error(f"Erro inesperado ao instalar dependências: {e}")
        logging.error(traceback.format_exc())
        messagebox.showerror("Erro", f"Erro inesperado ao instalar dependências: {str(e)}")
        return False

def find_mt5_installation():
    """Tenta localizar a instalação do MetaTrader 5 automaticamente nos registros do Windows"""
    logging.info("Procurando instalação do MetaTrader 5...")
    
    try:
        # Possíveis caminhos de registro para o MT5
        registry_paths = [
            r"SOFTWARE\MetaQuotes\Terminal",
            r"SOFTWARE\WOW6432Node\MetaQuotes\Terminal",
        ]
        
        for path in registry_paths:
            try:
                logging.info(f"Verificando registro: {path}")
                with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, path) as key:
                    # Enumera todas as subchaves (terminais instalados)
                    i = 0
                    while True:
                        try:
                            subkey_name = winreg.EnumKey(key, i)
                            with winreg.OpenKey(key, subkey_name) as subkey:
                                try:
                                    mt5_path, _ = winreg.QueryValueEx(subkey, "Path")
                                    terminal_path = os.path.join(mt5_path, "terminal64.exe")
                                    if os.path.exists(mt5_path) and os.path.exists(terminal_path):
                                        logging.info(f"MT5 encontrado via registro: {mt5_path}")
                                        return mt5_path
                                except (FileNotFoundError, OSError) as e:
                                    logging.debug(f"Erro ao ler valor da chave: {e}")
                            i += 1
                        except OSError:
                            break
            except (FileNotFoundError, OSError) as e:
                logging.debug(f"Erro ao acessar chave de registro {path}: {e}")
                continue
    except Exception as e:
        logging.error(f"Erro ao buscar no registro: {e}")
    
    # Caminhos padrão conhecidos
    default_paths = [
        os.path.join(os.environ.get("PROGRAMFILES", "C:\\Program Files"), "MetaTrader 5"),
        os.path.join(os.environ.get("PROGRAMFILES(X86)", "C:\\Program Files (x86)"), "MetaTrader 5"),
        "C:\\Program Files\\MetaTrader 5",
        "C:\\Program Files (x86)\\MetaTrader 5"
    ]
    
    for path in default_paths:
        terminal_path = os.path.join(path, "terminal64.exe")
        if os.path.exists(path) and os.path.exists(terminal_path):
            logging.info(f"MT5 encontrado em caminho padrão: {path}")
            return path
    
    logging.warning("MT5 não encontrado automaticamente")
    return None

def select_mt5_path_manually():
    """Solicita ao usuário para selecionar manualmente o diretório do MT5"""
    root = tk.Tk()
    root.withdraw()  # Esconde a janela principal
    
    messagebox.showinfo("Seleção do MetaTrader 5", 
                       "Não foi possível encontrar automaticamente o MetaTrader 5.\n"
                       "Por favor, selecione o diretório de instalação manualmente.\n\n"
                       "Se o MT5 não está instalado, faça o download em:\n"
                       "https://www.metatrader5.com/pt/download")
    
    mt5_path = filedialog.askdirectory(title="Selecione o diretório de instalação do MetaTrader 5")
    
    if not mt5_path:
        logging.warning("Usuário cancelou a seleção do diretório")
        return None
    
    terminal_path = os.path.join(mt5_path, "terminal64.exe")
    if os.path.exists(terminal_path):
        logging.info(f"MT5 selecionado manualmente: {mt5_path}")
        return mt5_path
    else:
        logging.error(f"Diretório inválido selecionado: {mt5_path}")
        messagebox.showerror("Erro", "Diretório inválido ou terminal64.exe não encontrado.")
        
        retry = messagebox.askyesno("Tentar novamente?", 
                                   "Diretório inválido ou terminal64.exe não encontrado.\n\n"
                                   "Deseja tentar selecionar novamente?")
        if retry:
            return select_mt5_path_manually()
        return None

def create_config_file(mt5_path):
    """Cria arquivo de configuração com o caminho do MT5"""
    try:
        config_dir = "config"
        os.makedirs(config_dir, exist_ok=True)
        
        config_path = os.path.join(config_dir, "config.ini")
        with open(config_path, "w") as f:
            f.write("[MT5]\n")
            f.write(f"path = {mt5_path}\n")
            f.write("\n[DATABASE]\n")
            f.write("type = sqlite\n")
            f.write("path = database/mt5_data.db\n")
        
        # Cria diretório para o banco de dados
        os.makedirs("database", exist_ok=True)
        
        logging.info(f"Arquivo de configuração criado em {config_path}")
        return True
    except Exception as e:
        logging.error(f"Erro ao criar arquivo de configuração: {e}")
        logging.error(traceback.format_exc())
        messagebox.showerror("Erro", f"Falha ao criar arquivo de configuração: {str(e)}")
        return False

def check_mt5_module():
    """Verifica se o módulo MetaTrader5 está instalado"""
    try:
        logging.info("Verificando módulo MetaTrader5...")
        import MetaTrader5
        logging.info("Módulo MetaTrader5 encontrado")
        return True
    except ImportError:
        logging.warning("Módulo MetaTrader5 não encontrado. Tentando instalar...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "MetaTrader5"], 
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            logging.info("Módulo MetaTrader5 instalado com sucesso")
            return True
        except subprocess.CalledProcessError as e:
            logging.error(f"Erro ao instalar MetaTrader5: {e}")
            messagebox.showerror("Erro", 
                                f"Falha ao instalar o módulo MetaTrader5.\n"
                                "Execute o verificador.py para ajudar a resolver este problema.")
            return False

def main():
    logging.info("Iniciando instalação da aplicação MT5 Extração...")
    print("===== MT5 Extração - Instalação =====")
    
    # Verifica versão do Python
    if not check_python_version():
        sys.exit(1)
    
    # Instala dependências
    print("Instalando dependências...")
    if not install_dependencies():
        print("Falha ao instalar dependências. Verifique o log para mais detalhes.")
        
        # Pergunta se deseja continuar mesmo assim
        root = tk.Tk()
        root.withdraw()
        if not messagebox.askyesno("Continuar?", "Houve problemas na instalação de dependências.\nDeseja continuar mesmo assim?"):
            sys.exit(1)
        root.destroy()
    
    # Verifica módulo MetaTrader5
    if not check_mt5_module():
        print("Aviso: Módulo MetaTrader5 não instalado ou não pôde ser importado.")
        print("Use o verificador.py para resolver este problema.")
    
    # Localiza instalação do MT5
    print("Buscando instalação do MetaTrader 5...")
    mt5_path = find_mt5_installation()
    
    if not mt5_path:
        print("MT5 não encontrado automaticamente. Solicitando seleção manual...")
        mt5_path = select_mt5_path_manually()
    
    if not mt5_path:
        print("Falha ao localizar o MetaTrader 5.")
        print("A instalação será concluída sem a configuração do MT5.")
        print("Execute novamente este script ou use o verificador.py para configurar o MT5 mais tarde.")
        
        root = tk.Tk()
        root.withdraw()
        messagebox.showwarning("Aviso", 
                             "Instalação concluída sem configuração do MT5.\n"
                             "Execute o verificador.py para completar a configuração.")
        root.destroy()
        sys.exit(1)
    
    print(f"MetaTrader 5 encontrado em: {mt5_path}")
    
    # Cria arquivo de configuração
    if create_config_file(mt5_path):
        print("\nInstalação concluída com sucesso!")
        
        # Verifica se o verificador.py existe
        if os.path.exists("verificador.py"):
            print("\nPara verificar a instalação, execute: python verificador.py")
            
        print("\nPara iniciar a aplicação, execute: python app.py")
        
        root = tk.Tk()
        root.withdraw()
        messagebox.showinfo("Sucesso", 
                         "Instalação concluída com sucesso!\n\n"
                         "Execute python app.py para iniciar a aplicação.\n"
                         "Ou python verificador.py para verificar a instalação.")
        root.destroy()
    else:
        print("\nOcorreram erros durante a instalação. Verifique o log para mais detalhes.")
        sys.exit(1)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.critical(f"Erro fatal durante a instalação: {e}")
        logging.critical(traceback.format_exc())
        
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror("Erro Fatal", 
                           f"Ocorreu um erro inesperado durante a instalação:\n{str(e)}\n\n"
                           "Verifique o arquivo de log mt5_instalacao.log para mais detalhes.")
        root.destroy()
        sys.exit(1) 