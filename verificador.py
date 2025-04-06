import os
import sys
import subprocess
import platform
import tkinter as tk
from tkinter import messagebox, ttk
import configparser
import logging
import json
import importlib
import time
import shutil

# Garantir que o diretório de logs existe
os.makedirs("logs", exist_ok=True)

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/mt5_diagnostico.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

class MT5Verificador:
    def __init__(self, voltar_callback=None):
        self.voltar_callback = voltar_callback
        self.dependencias_obrigatorias = [
            "MetaTrader5",
            "pandas",
            "numpy",
            "matplotlib",
            "sqlalchemy",
            "PIL"
        ]
        
        # Configurar diretório de instalação
        self.config_path = "config/config.ini"
        self.resultado = {
            "sistema": self.verificar_sistema(),
            "dependencias": {},
            "mt5": {"instalado": False, "caminho": "", "inicializado": False},
            "problemas": []
        }
        
        # Referência à janela principal
        self.root = None
    
    def verificar_sistema(self):
        """Verifica informações do sistema operacional"""
        return {
            "os": platform.system(),
            "versao": platform.version(),
            "arquitetura": platform.architecture()[0],
            "python": platform.python_version(),
            "executavel": sys.executable,
            "ambiente_virtual": "VIRTUAL_ENV" in os.environ
        }
    
    def verificar_dependencias(self):
        """Verifica se todas as dependências estão instaladas"""
        logging.info("Verificando dependências...")
        
        for pacote in self.dependencias_obrigatorias:
            try:
                # Casos especiais
                if pacote == "PIL":
                    importlib.import_module("PIL")
                    self.resultado["dependencias"][pacote] = True
                else:
                    importlib.import_module(pacote)
                    self.resultado["dependencias"][pacote] = True
                logging.info(f"✓ {pacote} encontrado")
            except ImportError:
                self.resultado["dependencias"][pacote] = False
                logging.warning(f"✗ {pacote} não encontrado")
                self.resultado["problemas"].append(f"Dependência não encontrada: {pacote}")
        
        # Verificação especial para pandas_ta que está dando problemas
        try:
            # Verifica se pandas_ta está instalado, sem importá-lo diretamente
            result = subprocess.run(
                [sys.executable, "-m", "pip", "show", "pandas_ta"],
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE,
                text=True,
                check=False
            )
            if result.returncode == 0:
                self.resultado["dependencias"]["pandas_ta"] = True
                logging.info(f"✓ pandas_ta encontrado (instalado via pip)")
            else:
                raise subprocess.CalledProcessError(result.returncode, result.args)
        except subprocess.CalledProcessError:
            self.resultado["dependencias"]["pandas_ta"] = False
            logging.warning(f"✗ pandas_ta não encontrado")
            self.resultado["problemas"].append("Dependência não encontrada: pandas_ta")
        
        # Verificação do psutil
        try:
            importlib.import_module("psutil")
            self.resultado["dependencias"]["psutil"] = True
            logging.info(f"✓ psutil encontrado")
        except ImportError:
            self.resultado["dependencias"]["psutil"] = False
            logging.warning(f"✗ psutil não encontrado")
            self.resultado["problemas"].append("Dependência não encontrada: psutil")
    
    def verificar_mt5(self):
        """Verifica se o MetaTrader 5 está instalado e configurado"""
        logging.info("Verificando instalação do MetaTrader 5...")
        
        # Verifica se o módulo está disponível
        if not self.resultado["dependencias"].get("MetaTrader5", False):
            logging.warning("Módulo MetaTrader5 não instalado")
            return
        
        # Verifica configuração
        if os.path.exists(self.config_path):
            try:
                config = configparser.ConfigParser()
                config.read(self.config_path)
                mt5_path = config.get('MT5', 'path', fallback=None)
                
                if mt5_path and os.path.exists(mt5_path):
                    self.resultado["mt5"]["instalado"] = True
                    self.resultado["mt5"]["caminho"] = mt5_path
                    logging.info(f"✓ MetaTrader 5 encontrado em: {mt5_path}")
                    
                    # Verifica se o MT5 já está em execução
                    try:
                        import psutil
                        mt5_running = False
                        for proc in psutil.process_iter(['name']):
                            try:
                                if proc.info['name'] and 'terminal64.exe' in proc.info['name'].lower():
                                    mt5_running = True
                                    logging.info("MetaTrader 5 já está em execução")
                                    break
                            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                                pass
                        
                        # Tenta inicializar apenas se não estiver em execução ou mesmo assim para verificação
                        try:
                            import MetaTrader5 as mt5
                            if mt5.initialize(path=mt5_path):
                                self.resultado["mt5"]["inicializado"] = True
                                logging.info(f"✓ MetaTrader 5 inicializado com sucesso")
                                mt5.shutdown()
                            else:
                                error_code = mt5.last_error()
                                if "IPC initialize failed" in str(error_code) and mt5_running:
                                    # MT5 já está em execução e não permite nova inicialização
                                    logging.info(f"MetaTrader 5 já está em execução. Isso é normal e esperado.")
                                    self.resultado["mt5"]["inicializado"] = True
                                else:
                                    self.resultado["problemas"].append(f"Não foi possível inicializar o MT5. Código: {error_code}")
                                    logging.warning(f"✗ Não foi possível inicializar o MT5. Código: {error_code}")
                        except Exception as e:
                            self.resultado["problemas"].append(f"Erro ao inicializar MT5: {str(e)}")
                            logging.error(f"Erro ao inicializar MT5: {str(e)}")
                    except ImportError:
                        logging.warning("psutil não disponível para verificar se MT5 está em execução")
                        # Continua sem o psutil
                        try:
                            import MetaTrader5 as mt5
                            if mt5.initialize(path=mt5_path):
                                self.resultado["mt5"]["inicializado"] = True
                                logging.info(f"✓ MetaTrader 5 inicializado com sucesso")
                                mt5.shutdown()
                            else:
                                error_code = mt5.last_error()
                                self.resultado["problemas"].append(f"Não foi possível inicializar o MT5. Código: {error_code}")
                                logging.warning(f"✗ Não foi possível inicializar o MT5. Código: {error_code}")
                        except Exception as e:
                            self.resultado["problemas"].append(f"Erro ao inicializar MT5: {str(e)}")
                            logging.error(f"Erro ao inicializar MT5: {str(e)}")
                else:
                    self.resultado["problemas"].append("Caminho do MT5 inválido no arquivo de configuração")
                    logging.warning("✗ Caminho do MT5 inválido no arquivo de configuração")
            except Exception as e:
                self.resultado["problemas"].append(f"Erro ao ler arquivo de configuração: {str(e)}")
                logging.error(f"Erro ao ler arquivo de configuração: {str(e)}")
        else:
            self.resultado["problemas"].append("Arquivo de configuração não encontrado")
            logging.warning("✗ Arquivo de configuração não encontrado")
    
    def instalar_dependencias(self, auto_instalar=False):
        """Instala as dependências faltantes"""
        logging.info("Instalando dependências...")
        
        # Lista para rastrear instalações bem-sucedidas
        dependencias_instaladas = []
        dependencias_faltantes = []
        falhas = []
        
        # Se for instalação automática, primeiro verificamos o que está faltando
        if auto_instalar and self.resultado["dependencias"]:
            for pacote, instalado in self.resultado["dependencias"].items():
                if not instalado:
                    if pacote == "PIL":
                        dependencias_faltantes.append("Pillow")
                    elif pacote in ["pandas_ta", "psutil"]:
                        # Tratados separadamente
                        continue
                    else:
                        dependencias_faltantes.append(pacote)
            
            # Tenta psutil primeiro se estiver faltando
            if not self.resultado["dependencias"].get("psutil", True):
                try:
                    logging.info("Instalando psutil (necessário para verificar processos)...")
                    subprocess.check_call([sys.executable, "-m", "pip", "install", "psutil"], 
                                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    logging.info("✓ psutil instalado com sucesso")
                    dependencias_instaladas.append("psutil")
                except subprocess.CalledProcessError as e:
                    logging.error(f"Erro ao instalar psutil: {str(e)}")
                    falhas.append("psutil")
            
            # Tenta instalar pandas_ta separadamente
            if not self.resultado["dependencias"].get("pandas_ta", True):
                try:
                    logging.info("Tentando instalar pandas_ta...")
                    subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "--force-reinstall", "pandas_ta"],
                                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    logging.info("✓ pandas_ta instalado com sucesso")
                    dependencias_instaladas.append("pandas_ta")
                except subprocess.CalledProcessError as e:
                    logging.error(f"Erro ao instalar pandas_ta: {str(e)}")
                    falhas.append("pandas_ta")
        else:
            # Instalação manual, apenas as que o usuário selecionou
            for pacote, instalado in self.resultado["dependencias"].items():
                if not instalado:
                    if pacote == "PIL":
                        dependencias_faltantes.append("Pillow")
                    elif pacote == "psutil":
                        try:
                            logging.info("Instalando psutil (necessário para verificar processos)...")
                            subprocess.check_call([sys.executable, "-m", "pip", "install", "psutil"],
                                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                            logging.info("✓ psutil instalado com sucesso")
                            dependencias_instaladas.append("psutil")
                        except subprocess.CalledProcessError as e:
                            logging.error(f"Erro ao instalar psutil: {str(e)}")
                            falhas.append("psutil")
                    elif pacote == "pandas_ta":
                        try:
                            logging.info("Tentando instalar pandas_ta...")
                            subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "--force-reinstall", "pandas_ta"],
                                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                            logging.info("✓ pandas_ta instalado com sucesso")
                            dependencias_instaladas.append("pandas_ta")
                        except subprocess.CalledProcessError as e:
                            logging.error(f"Erro ao instalar pandas_ta: {str(e)}")
                            falhas.append("pandas_ta")
                    else:
                        dependencias_faltantes.append(pacote)
        
        # Instala as dependências restantes
        if dependencias_faltantes:
            try:
                logging.info(f"Instalando dependências: {', '.join(dependencias_faltantes)}")
                subprocess.check_call([sys.executable, "-m", "pip", "install"] + dependencias_faltantes,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                logging.info(f"✓ Dependências instaladas com sucesso: {', '.join(dependencias_faltantes)}")
                dependencias_instaladas.extend(dependencias_faltantes)
            except subprocess.CalledProcessError as e:
                logging.error(f"Erro ao instalar dependências em lote: {str(e)}")
                # Tentar instalar uma a uma
                for dep in dependencias_faltantes:
                    try:
                        logging.info(f"Tentando instalar {dep} individualmente...")
                        subprocess.check_call([sys.executable, "-m", "pip", "install", dep],
                                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                        logging.info(f"✓ {dep} instalado com sucesso")
                        dependencias_instaladas.append(dep)
                    except subprocess.CalledProcessError as e2:
                        logging.error(f"Erro ao instalar {dep}: {str(e2)}")
                        falhas.append(dep)
        
        # Tenta instalar usando o setup.py se disponível (para o modo de desenvolvimento)
        if falhas and os.path.exists("setup.py"):
            try:
                logging.info("Tentando instalar via setup.py (modo de desenvolvimento)...")
                subprocess.check_call([sys.executable, "-m", "pip", "install", "-e", "."],
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                logging.info("✓ Pacote instalado em modo de desenvolvimento")
                return True
            except subprocess.CalledProcessError as e:
                logging.error(f"Erro ao instalar via setup.py: {str(e)}")
        
        return len(dependencias_instaladas) > 0
    
    def corrigir_problemas(self, auto_correcao=False):
        """Tenta corrigir os problemas detectados automaticamente"""
        logging.info("Tentando corrigir problemas...")
        problemas_resolvidos = []
        
        # Se for autocorreção, tenta instalar todas as dependências faltantes
        if auto_correcao:
            if any(["Dependência não encontrada" in p for p in self.resultado["problemas"]]):
                if self.instalar_dependencias(auto_instalar=True):
                    problemas_resolvidos.extend([p for p in self.resultado["problemas"] if "Dependência não encontrada" in p])
        
        # Processa cada problema individual
        for problema in self.resultado["problemas"]:
            if "Dependência não encontrada" in problema and not auto_correcao:
                if self.instalar_dependencias():
                    problemas_resolvidos.append(problema)
            elif "Arquivo de configuração não encontrado" in problema:
                # Tenta executar o instalador
                try:
                    logging.info("Executando instalador...")
                    subprocess.check_call([sys.executable, "install.py"])
                    if os.path.exists(self.config_path):
                        problemas_resolvidos.append(problema)
                        logging.info("✓ Arquivo de configuração criado com sucesso")
                except subprocess.CalledProcessError as e:
                    logging.error(f"Erro ao executar instalador: {str(e)}")
        
        # Remove problemas resolvidos da lista
        for problema in problemas_resolvidos:
            if problema in self.resultado["problemas"]:
                self.resultado["problemas"].remove(problema)
        
        return len(problemas_resolvidos) > 0
    
    def gerar_relatorio(self):
        """Gera um relatório completo do diagnóstico"""
        self.verificar_dependencias()
        self.verificar_mt5()
        
        # Salva o relatório em JSON
        with open("mt5_diagnostico.json", "w", encoding="utf-8") as f:
            json.dump(self.resultado, f, ensure_ascii=False, indent=4)
        
        return self.resultado
    
    def mostrar_interface(self, auto_corrigir=False):
        """Mostra uma interface gráfica com os resultados do diagnóstico"""
        if auto_corrigir:
            # Corrige problemas automaticamente antes de mostrar a interface
            self.corrigir_problemas(auto_correcao=True)
            # Atualiza o relatório
            self.resultado = {
                "sistema": self.verificar_sistema(),
                "dependencias": {},
                "mt5": {"instalado": False, "caminho": "", "inicializado": False},
                "problemas": []
            }
            self.gerar_relatorio()
        
        # Cria janela principal
        self.root = tk.Tk()
        self.root.title("MT5 Extração - Diagnóstico")
        self.root.geometry("600x550")
        self.root.minsize(600, 550)
        
        # Estilo
        style = ttk.Style()
        style.configure("TLabel", font=("Arial", 10))
        style.configure("Header.TLabel", font=("Arial", 12, "bold"))
        style.configure("Success.TLabel", foreground="green")
        style.configure("Error.TLabel", foreground="red")
        
        # Frame principal
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Título
        ttk.Label(main_frame, text="Diagnóstico da Aplicação MT5 Extração", style="Header.TLabel").pack(pady=10)
        
        # Informações do Sistema
        sys_frame = ttk.LabelFrame(main_frame, text="Informações do Sistema", padding="5")
        sys_frame.pack(fill=tk.X, pady=5)
        
        sistema = self.resultado["sistema"]
        ttk.Label(sys_frame, text=f"Sistema: {sistema['os']} {sistema['versao']} ({sistema['arquitetura']})").pack(anchor="w")
        ttk.Label(sys_frame, text=f"Python: {sistema['python']}").pack(anchor="w")
        ttk.Label(sys_frame, text=f"Ambiente Virtual: {'Sim' if sistema['ambiente_virtual'] else 'Não'}").pack(anchor="w")
        
        # Dependências
        dep_frame = ttk.LabelFrame(main_frame, text="Dependências", padding="5")
        dep_frame.pack(fill=tk.X, pady=5)
        
        for pacote, instalado in self.resultado["dependencias"].items():
            if instalado:
                ttk.Label(dep_frame, text=f"✓ {pacote}", style="Success.TLabel").pack(anchor="w")
            else:
                ttk.Label(dep_frame, text=f"✗ {pacote}", style="Error.TLabel").pack(anchor="w")
        
        # MetaTrader 5
        mt5_frame = ttk.LabelFrame(main_frame, text="MetaTrader 5", padding="5")
        mt5_frame.pack(fill=tk.X, pady=5)
        
        mt5_info = self.resultado["mt5"]
        if mt5_info["instalado"]:
            ttk.Label(mt5_frame, text=f"✓ Instalado: {mt5_info['caminho']}", style="Success.TLabel").pack(anchor="w")
        else:
            ttk.Label(mt5_frame, text="✗ Não encontrado", style="Error.TLabel").pack(anchor="w")
        
        if mt5_info["inicializado"]:
            ttk.Label(mt5_frame, text="✓ Inicialização teste bem-sucedida", style="Success.TLabel").pack(anchor="w")
        elif mt5_info["instalado"]:
            ttk.Label(mt5_frame, text="✗ Não foi possível inicializar", style="Error.TLabel").pack(anchor="w")
        
        # Problemas
        prob_frame = ttk.LabelFrame(main_frame, text="Problemas Detectados", padding="5")
        prob_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        
        if self.resultado["problemas"]:
            for problema in self.resultado["problemas"]:
                ttk.Label(prob_frame, text=f"• {problema}", style="Error.TLabel").pack(anchor="w")
            
            # Botões para ações
            botoes_frame = ttk.Frame(prob_frame)
            botoes_frame.pack(pady=10)
            
            # Botão para corrigir problemas
            ttk.Button(botoes_frame, text="Corrigir Problemas Automaticamente", 
                       command=self.corrigir_e_atualizar_ui).pack(pady=5, padx=5, side=tk.LEFT)
            
            # Botão para voltar ao menu principal (se callback fornecido)
            if self.voltar_callback:
                ttk.Button(botoes_frame, text="Voltar ao Menu Principal", 
                          command=self.voltar_ao_menu).pack(pady=5, padx=5, side=tk.LEFT)
        else:
            ttk.Label(prob_frame, text="Não foram detectados problemas!", style="Success.TLabel").pack(anchor="w")
            
            # Botões para ações
            botoes_frame = ttk.Frame(prob_frame)
            botoes_frame.pack(pady=10)
            
            # Botão para iniciar aplicação
            ttk.Button(botoes_frame, text="Iniciar Aplicação", 
                      command=lambda: self.iniciar_aplicacao()).pack(pady=5, padx=5, side=tk.LEFT)
            
            # Botão para voltar ao menu principal (se callback fornecido)
            if self.voltar_callback:
                ttk.Button(botoes_frame, text="Voltar ao Menu Principal", 
                          command=self.voltar_ao_menu).pack(pady=5, padx=5, side=tk.LEFT)
        
        # Botão de fechar no final da janela
        ttk.Button(main_frame, text="Fechar", command=self.root.destroy).pack(pady=10)
        
        self.root.mainloop()
    
    def voltar_ao_menu(self):
        """Fecha a janela atual e retorna ao menu principal"""
        if self.root:
            self.root.destroy()
        
        if self.voltar_callback:
            self.voltar_callback()
    
    def corrigir_e_atualizar_ui(self):
        """Corrige problemas e atualiza a interface"""
        if self.corrigir_problemas(auto_correcao=True):
            messagebox.showinfo("Correção", "Alguns problemas foram corrigidos. O diagnóstico será executado novamente.")
            if self.root:
                self.root.destroy()
            
            self.resultado = {
                "sistema": self.verificar_sistema(),
                "dependencias": {},
                "mt5": {"instalado": False, "caminho": "", "inicializado": False},
                "problemas": []
            }
            self.gerar_relatorio()
            # Recarrega a interface
            self.mostrar_interface()
        else:
            messagebox.showerror("Correção", "Não foi possível corrigir todos os problemas automaticamente.")
    
    def iniciar_aplicacao(self):
        """Inicia a aplicação principal"""
        if self.root:
            self.root.destroy()
        
        try:
            logging.info("Iniciando aplicação principal...")
            subprocess.Popen([sys.executable, "app.py"])
        except Exception as e:
            logging.error(f"Erro ao iniciar aplicação: {str(e)}")
            messagebox.showerror("Erro", f"Erro ao iniciar aplicação: {str(e)}")

def main():
    print("===== MT5 Extração - Verificador de Ambiente =====")
    print("Analisando seu sistema e procurando por problemas...")
    
    try:
        # Verificar se psutil está instalado, para detecção de processos
        import psutil
    except ImportError:
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "psutil"])
            print("Instalando biblioteca auxiliar (psutil)... Concluído.")
            import psutil
        except:
            print("Aviso: Não foi possível instalar psutil. Algumas verificações podem ser limitadas.")
    
    verificador = MT5Verificador()
    resultado = verificador.gerar_relatorio()
    
    print("\nDiagnóstico concluído!")
    
    # Atualiza o encoding do console para lidar com caracteres unicode
    if os.name == 'nt':  # Windows
        try:
            os.system('chcp 65001 > NUL')  # Muda para UTF-8
        except:
            pass
    
    if resultado["problemas"]:
        print(f"\nForam encontrados {len(resultado['problemas'])} problemas:")
        for i, problema in enumerate(resultado["problemas"], 1):
            print(f"  {i}. {problema}")
        
        print("\nExibindo interface gráfica para correção...")
        
        # Perguntar ao usuário se deseja correção automática
        resposta = input("Deseja tentar corrigir problemas automaticamente? (S/N): ").strip().lower()
        auto_corrigir = resposta == 's' or resposta == 'sim'
        
        verificador.mostrar_interface(auto_corrigir=auto_corrigir)
    else:
        print("\nNenhum problema encontrado! O sistema está pronto para uso.")
        verificador.mostrar_interface()

if __name__ == "__main__":
    main() 