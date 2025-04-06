#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Inicializador Simplificado para o MT5 Extração
Este script detecta automaticamente o estado da instalação e 
guia o usuário pelas etapas necessárias para executar a aplicação.
"""

import os
import sys
import subprocess
import tkinter as tk
from tkinter import ttk, messagebox
import logging
import threading
import time
from PIL import Image, ImageTk
import ctypes

# Garantir que o diretório de logs existe
os.makedirs("logs", exist_ok=True)

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/mt5_execucao.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# Configuração para melhor DPI awareness no Windows
try:
    ctypes.windll.shcore.SetProcessDpiAwareness(1)
except:
    pass

# Função para mostrar uma tela de splash
def mostrar_splash():
    """Exibe uma tela de splash enquanto o aplicativo carrega"""
    splash_root = tk.Tk()
    splash_root.overrideredirect(True)  # Sem borda
    
    # Tamanho e posição
    width, height = 500, 300
    screen_width = splash_root.winfo_screenwidth()
    screen_height = splash_root.winfo_screenheight()
    x = (screen_width - width) // 2
    y = (screen_height - height) // 2
    splash_root.geometry(f"{width}x{height}+{x}+{y}")
    
    # Fundo
    splash_frame = tk.Frame(splash_root, bg="#2c3e50", width=width, height=height)
    splash_frame.pack(fill="both", expand=True)
    
    # Título da aplicação
    title_label = tk.Label(
        splash_frame, 
        text="MT5 Extração", 
        font=("Arial", 24, "bold"), 
        fg="white", 
        bg="#2c3e50"
    )
    title_label.pack(pady=(50, 10))
    
    # Subtítulo
    subtitle_label = tk.Label(
        splash_frame, 
        text="Ferramenta de Coleta de Dados Financeiros", 
        font=("Arial", 12), 
        fg="white", 
        bg="#2c3e50"
    )
    subtitle_label.pack(pady=(0, 30))
    
    # Mensagem de carregamento
    message_var = tk.StringVar()
    message_var.set("Inicializando aplicação...")
    message_label = tk.Label(
        splash_frame, 
        textvariable=message_var, 
        font=("Arial", 10), 
        fg="white", 
        bg="#2c3e50"
    )
    message_label.pack(pady=10)
    
    # Barra de progresso
    progress = ttk.Progressbar(
        splash_frame, 
        orient="horizontal", 
        length=400, 
        mode="indeterminate"
    )
    progress.pack(pady=20)
    progress.start(10)
    
    # Versão
    version_label = tk.Label(
        splash_frame, 
        text="v1.0.0", 
        font=("Arial", 8), 
        fg="white", 
        bg="#2c3e50"
    )
    version_label.pack(side=tk.BOTTOM, pady=10)
    
    # Função para atualizar a mensagem de carregamento
    def update_message(text):
        message_var.set(text)
        splash_root.update()
    
    # Função para fechar a tela de splash
    def close_splash():
        splash_root.destroy()
    
    return splash_root, update_message, close_splash

# Função para instalar setuptools imediatamente (evita problemas com pandas_ta)
def instalar_setuptools(update_message=None):
    """Instala setuptools para evitar problemas com pkg_resources"""
    try:
        if update_message:
            update_message("Verificando setuptools...")
            
        logging.info("Verificando setuptools...")
        try:
            import pkg_resources
            logging.info("pkg_resources já está disponível")
            return True
        except ImportError:
            if update_message:
                update_message("Instalando setuptools...")
                
            logging.warning("pkg_resources não encontrado, instalando setuptools...")
            subprocess.call(
                [sys.executable, "-m", "pip", "install", "--upgrade", "setuptools"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            try:
                import pkg_resources
                logging.info("setuptools instalado com sucesso")
                return True
            except ImportError:
                logging.error("Falha ao instalar setuptools")
                return False
    except Exception as e:
        logging.error(f"Erro ao instalar setuptools: {e}")
        return False

def verificar_ambiente(update_message=None):
    """Verifica se o ambiente está corretamente configurado"""
    if update_message:
        update_message("Verificando componentes do sistema...")
        
    files_to_check = ["verificador.py", "install.py", "app.py"]
    missing_files = []
    
    for file in files_to_check:
        if not os.path.exists(file):
            missing_files.append(file)
    
    if missing_files:
        logging.error(f"Arquivos essenciais faltando: {', '.join(missing_files)}")
        return False, missing_files
    
    return True, []

def executar_verificador(voltar_callback=None):
    """Executa o verificador de ambiente"""
    logging.info("Iniciando verificador de ambiente...")
    try:
        # Cria uma nova janela para mostrar o progresso
        janela_progresso = tk.Toplevel()
        janela_progresso.title("Verificação em Andamento")
        janela_progresso.geometry("400x150")
        janela_progresso.resizable(False, False)
        janela_progresso.transient()
        janela_progresso.grab_set()
        
        # Centraliza a janela
        janela_progresso.update_idletasks()
        width = janela_progresso.winfo_width()
        height = janela_progresso.winfo_height()
        x = (janela_progresso.winfo_screenwidth() // 2) - (width // 2)
        y = (janela_progresso.winfo_screenheight() // 2) - (height // 2)
        janela_progresso.geometry('{}x{}+{}+{}'.format(width, height, x, y))
        
        # Adiciona um label com a mensagem
        tk.Label(janela_progresso, text="Verificando ambiente...", font=("Arial", 12)).pack(pady=20)
        tk.Label(janela_progresso, text="Analisando dependências e configurações. Aguarde.", font=("Arial", 10)).pack()
        
        # Barra de progresso indeterminada
        progresso = ttk.Progressbar(janela_progresso, mode="indeterminate")
        progresso.pack(fill=tk.X, padx=20, pady=20)
        progresso.start(10)
        
        # Função para executar o verificador em uma thread separada
        def executar_em_thread():
            try:
                # Importar diretamente o verificador
                print("Iniciando verificador de ambiente...")
                
                try:
                    sys.path.append(os.getcwd())
                    from verificador import MT5Verificador
                    verificador = MT5Verificador(voltar_callback=voltar_callback)
                    
                    # Auto-correção de problemas
                    resultado = verificador.gerar_relatorio()
                    
                    # Fecha a janela de progresso
                    janela_progresso.destroy()
                    
                    # Mostra a interface do verificador
                    verificador.mostrar_interface(auto_corrigir=True)
                except ImportError as e:
                    logging.error(f"Erro ao importar verificador: {e}")
                    janela_progresso.destroy()
                    messagebox.showerror("Erro", f"Erro ao importar verificador: {e}")
                    
                    # Fallback para execução como subprocess
                    subprocess.Popen([sys.executable, "verificador.py"])
                    if voltar_callback:
                        voltar_callback()
            except Exception as e:
                logging.error(f"Erro ao executar verificador: {e}")
                try:
                    janela_progresso.destroy()
                except:
                    pass
                messagebox.showerror("Erro", f"Erro ao executar verificador: {e}")
                if voltar_callback:
                    voltar_callback()
        
        # Inicia a thread para não bloquear a interface
        thread = threading.Thread(target=executar_em_thread, daemon=True)
        thread.start()
        
        # Mantém a janela de progresso visível enquanto a thread executa
        return True
    except Exception as e:
        logging.error(f"Erro ao executar verificador: {e}")
        messagebox.showerror("Erro", f"Erro ao executar verificador: {e}")
        if voltar_callback:
            voltar_callback()
        return False

def executar_instalador(voltar_callback=None):
    """Executa o instalador"""
    logging.info("Iniciando instalador...")
    try:
        # Cria uma nova janela para mostrar o progresso
        janela_progresso = tk.Toplevel()
        janela_progresso.title("Instalação em Andamento")
        janela_progresso.geometry("400x200")
        janela_progresso.resizable(False, False)
        janela_progresso.transient()  # Torna a janela transitória
        
        # Centraliza a janela
        janela_progresso.update_idletasks()
        width = janela_progresso.winfo_width()
        height = janela_progresso.winfo_height()
        x = (janela_progresso.winfo_screenwidth() // 2) - (width // 2)
        y = (janela_progresso.winfo_screenheight() // 2) - (height // 2)
        janela_progresso.geometry('{}x{}+{}+{}'.format(width, height, x, y))
        
        # Adiciona um label com a mensagem
        tk.Label(janela_progresso, text="Instalação em andamento...", font=("Arial", 12)).pack(pady=10)
        tk.Label(janela_progresso, text="Isso pode levar alguns minutos. Por favor, aguarde.", font=("Arial", 10)).pack()
        
        # Informação de status
        status_label = tk.Label(janela_progresso, text="Iniciando...", font=("Arial", 10))
        status_label.pack(pady=5)
        
        # Barra de progresso indeterminada
        progresso = ttk.Progressbar(janela_progresso, mode="indeterminate")
        progresso.pack(fill=tk.X, padx=20, pady=10)
        progresso.start(10)
        
        # Botão de cancelar com estado inicial desativado
        cancelar_btn = ttk.Button(janela_progresso, text="Cancelar (indisponível)", state="disabled")
        cancelar_btn.pack(pady=10)
        
        # Função para instalar dependências críticas manualmente
        def instalar_dependencias_basicas():
            try:
                status_label.config(text="Instalando setuptools (pkg_resources)...")
                janela_progresso.update()
                
                # Instala setuptools para resolver o problema do pkg_resources
                subprocess.call(
                    [sys.executable, "-m", "pip", "install", "--upgrade", "setuptools"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                )
                
                # Instala outras dependências críticas
                status_label.config(text="Instalando dependências críticas...")
                janela_progresso.update()
                
                # Lista de pacotes críticos que precisam ser instalados
                pacotes_criticos = [
                    "wheel",
                    "psutil",
                    "pandas",
                    "pandas_ta",
                    "MetaTrader5"
                ]
                
                for pacote in pacotes_criticos:
                    try:
                        status_label.config(text=f"Instalando {pacote}...")
                        janela_progresso.update()
                        
                        subprocess.call(
                            [sys.executable, "-m", "pip", "install", "--upgrade", pacote],
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL
                        )
                    except Exception as e:
                        logging.warning(f"Erro ao instalar {pacote}: {e}")
                
                return True
            except Exception as e:
                logging.error(f"Erro ao instalar dependências básicas: {e}")
                return False
        
        # Função para executar o instalador diretamente sem subprocess
        def executar_direto():
            try:
                # Antes de tudo, garante que as dependências críticas estão instaladas
                status_label.config(text="Verificando dependências críticas...")
                janela_progresso.update()
                
                instalar_dependencias_basicas()
                
                # Atualiza status
                status_label.config(text="Importando módulos...")
                janela_progresso.update()
                
                # Adiciona o diretório atual ao path
                sys.path.append(os.getcwd())
                
                # Executa o install.py diretamente
                import install
                
                # Execução explícita da função main do módulo
                install.main()
                
                # Verifica novamente as dependências críticas após a instalação
                status_label.config(text="Finalizando instalação...")
                janela_progresso.update()
                
                # Tenta resolver o problema do pkg_resources ao final da instalação
                try:
                    import pkg_resources
                except ImportError:
                    status_label.config(text="Instalando setuptools final...")
                    subprocess.call(
                        [sys.executable, "-m", "pip", "install", "--upgrade", "setuptools"],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL
                    )
                
                # Fecha janela após conclusão
                janela_progresso.destroy()
                
                # Exibe mensagem de sucesso
                messagebox.showinfo("Instalação", "Instalação concluída com sucesso!\nFerramentas e dependências instaladas.")
                
                # Retorna ao menu principal
                if voltar_callback:
                    voltar_callback()
                
            except Exception as e:
                logging.error(f"Erro ao executar instalador: {str(e)}")
                try:
                    janela_progresso.destroy()
                except:
                    pass
                messagebox.showerror("Erro", f"Erro ao instalar: {str(e)}")
                if voltar_callback:
                    voltar_callback()
        
        # Função para executar o instalador como processo separado (plano B)
        def executar_como_processo():
            try:
                # Primeiro instala as dependências críticas
                instalar_dependencias_basicas()
                
                # Atualiza status
                status_label.config(text="Executando instalador...")
                janela_progresso.update()
                
                # Executa em modo separado mas com redirecionamento
                with open("install_output.log", "w") as f_out:
                    try:
                        processo = subprocess.Popen(
                            [sys.executable, "install.py"],
                            stdout=f_out,
                            stderr=subprocess.STDOUT,
                            creationflags=subprocess.CREATE_NO_WINDOW
                        )
                    except AttributeError:
                        # Fallback para plataformas onde CREATE_NO_WINDOW não está disponível
                        processo = subprocess.Popen(
                            [sys.executable, "install.py"],
                            stdout=f_out,
                            stderr=subprocess.STDOUT
                        )
                
                # Verifica o status a cada 500ms
                def verificar_status():
                    if processo.poll() is None:
                        # Ainda está em execução
                        status_label.config(text="Instalando componentes...")
                        janela_progresso.after(500, verificar_status)
                    else:
                        # Processo terminou
                        if processo.returncode == 0:
                            # Tenta instalar setuptools novamente para garantir
                            try:
                                subprocess.call(
                                    [sys.executable, "-m", "pip", "install", "--upgrade", "setuptools"],
                                    stdout=subprocess.DEVNULL,
                                    stderr=subprocess.DEVNULL
                                )
                            except:
                                pass
                                
                            status_label.config(text="Instalação concluída")
                            janela_progresso.destroy()
                            messagebox.showinfo("Instalação", "Instalação concluída com sucesso!")
                        else:
                            status_label.config(text=f"Erro: código {processo.returncode}")
                            janela_progresso.destroy()
                            messagebox.showerror("Erro", f"Falha na instalação (código {processo.returncode})")
                        
                        # Retorna ao menu principal
                        if voltar_callback:
                            voltar_callback()
                
                # Inicia a verificação
                verificar_status()
                
            except Exception as e:
                logging.error(f"Erro ao executar instalador como processo: {str(e)}")
                try:
                    janela_progresso.destroy()
                except:
                    pass
                messagebox.showerror("Erro", f"Erro ao instalar: {str(e)}")
                if voltar_callback:
                    voltar_callback()
        
        # Decide o método de execução com base na disponibilidade
        try:
            # Tenta verificar se podemos importar install
            import importlib.util
            spec = importlib.util.find_spec("install")
            if spec is not None:
                # O módulo pode ser importado, usar método direto
                thread = threading.Thread(target=executar_direto, daemon=True)
            else:
                # O módulo não pode ser importado, usar método de processo
                status_label.config(text="Usando método alternativo...")
                thread = threading.Thread(target=executar_como_processo, daemon=True)
        except:
            # Em caso de erro, usar método de processo
            status_label.config(text="Usando método alternativo...")
            thread = threading.Thread(target=executar_como_processo, daemon=True)
        
        # Inicia a thread
        thread.start()
        
        # Configuração de protocolo de fechamento
        def ao_fechar():
            messagebox.showinfo("Instalação", "A instalação continuará em segundo plano.\nA interface principal será restaurada.")
            if voltar_callback:
                voltar_callback()
            janela_progresso.destroy()
        
        janela_progresso.protocol("WM_DELETE_WINDOW", ao_fechar)
        
        return True
    except Exception as e:
        logging.error(f"Erro ao preparar instalador: {e}")
        messagebox.showerror("Erro", f"Erro ao preparar instalador: {e}")
        if voltar_callback:
            voltar_callback()
        return False

def executar_aplicacao(voltar_callback=None):
    """Executa a aplicação principal"""
    logging.info("Iniciando aplicação principal...")
    try:
        # Verificar se o MetaTrader 5 já está em execução
        mt5_running = False
        try:
            import psutil
            for proc in psutil.process_iter(['name']):
                try:
                    if proc.info['name'] and 'terminal64.exe' in proc.info['name'].lower():
                        mt5_running = True
                        break
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    pass
        except ImportError:
            pass
        
        if not mt5_running:
            resposta = messagebox.askquestion(
                "MT5 não está em execução", 
                "O MetaTrader 5 não parece estar em execução. É recomendável abri-lo antes de iniciar a aplicação.\n\n"
                "Deseja continuar mesmo assim?"
            )
            if resposta != 'yes':
                if voltar_callback:
                    voltar_callback()
                return False
        
        # Cria uma nova janela para mostrar o progresso
        janela_progresso = tk.Toplevel()
        janela_progresso.title("Iniciando Aplicação")
        janela_progresso.geometry("400x150")
        janela_progresso.resizable(False, False)
        janela_progresso.transient()
        janela_progresso.grab_set()
        
        # Centraliza a janela
        janela_progresso.update_idletasks()
        width = janela_progresso.winfo_width()
        height = janela_progresso.winfo_height()
        x = (janela_progresso.winfo_screenwidth() // 2) - (width // 2)
        y = (janela_progresso.winfo_screenheight() // 2) - (height // 2)
        janela_progresso.geometry('{}x{}+{}+{}'.format(width, height, x, y))
        
        # Adiciona um label com a mensagem
        tk.Label(janela_progresso, text="Iniciando aplicação principal...", font=("Arial", 12)).pack(pady=20)
        tk.Label(janela_progresso, text="Conectando ao MetaTrader 5 e inicializando. Aguarde.", font=("Arial", 10)).pack()
        
        # Barra de progresso indeterminada
        progresso = ttk.Progressbar(janela_progresso, mode="indeterminate")
        progresso.pack(fill=tk.X, padx=20, pady=20)
        progresso.start(10)
        
        # Função para executar a aplicação em uma thread separada
        def executar_em_thread():
            try:
                # Executa a aplicação como um processo separado
                processo = subprocess.Popen([sys.executable, "app.py"])
                
                # Aguarda um pouco para garantir que o processo iniciou
                time.sleep(2)
                
                # Fecha a janela de progresso depois de um curto período
                janela_progresso.destroy()
                
                # Verifica se o processo está rodando
                if processo.poll() is None:  # None significa que ainda está executando
                    logging.info("Aplicação principal iniciada com sucesso")
                    if voltar_callback:
                        voltar_callback()
                else:
                    # Se o processo já terminou, algo deu errado
                    logging.error(f"Aplicação encerrou prematuramente com código: {processo.returncode}")
                    messagebox.showerror("Erro", "A aplicação foi iniciada mas encerrou prematuramente.")
                    if voltar_callback:
                        voltar_callback()
            except Exception as e:
                logging.error(f"Erro ao iniciar aplicação: {e}")
                try:
                    janela_progresso.destroy()
                except:
                    pass
                messagebox.showerror("Erro", f"Erro ao iniciar aplicação: {str(e)}")
                if voltar_callback:
                    voltar_callback()
        
        # Inicia a thread para não bloquear a interface
        thread = threading.Thread(target=executar_em_thread, daemon=True)
        thread.start()
        
        # Mantém a janela de progresso visível enquanto a thread executa
        return True
    except Exception as e:
        logging.error(f"Erro ao executar aplicação: {e}")
        messagebox.showerror("Erro", f"Erro ao executar aplicação: {e}")
        if voltar_callback:
            voltar_callback()
        return False

def mostrar_gui_simples():
    """Mostra uma interface gráfica simples para o usuário escolher a ação"""
    # Cria a janela principal
    root = tk.Tk()
    root.title("MT5 Extração - Menu Principal")
    root.geometry("480x380")
    root.resizable(False, False)
    
    # Configuração de estilo
    style = ttk.Style()
    style.configure("TButton", font=("Arial", 11), padding=8)
    style.configure("TLabel", font=("Arial", 11))
    style.configure("Title.TLabel", font=("Arial", 16, "bold"))
    style.configure("Subtitle.TLabel", font=("Arial", 11))
    
    # Centraliza a janela
    root.update_idletasks()
    width = root.winfo_width()
    height = root.winfo_height()
    x = (root.winfo_screenwidth() // 2) - (width // 2)
    y = (root.winfo_screenheight() // 2) - (height // 2)
    root.geometry('{}x{}+{}+{}'.format(width, height, x, y))
    
    # Frame principal com padding
    main_frame = ttk.Frame(root, padding=20)
    main_frame.pack(fill=tk.BOTH, expand=True)
    
    # Título
    ttk.Label(main_frame, text="MT5 Extração", style="Title.TLabel").pack(pady=(0, 5))
    ttk.Label(main_frame, text="Ferramenta de Coleta de Dados Financeiros", style="Subtitle.TLabel").pack(pady=(0, 20))
    
    # Separador
    ttk.Separator(main_frame).pack(fill=tk.X, pady=10)
    
    # Verifica o status atual do ambiente
    ambiente_ok, _ = verificar_ambiente()
    
    # Frame para status do ambiente
    status_frame = ttk.LabelFrame(main_frame, text="Status do Ambiente")
    status_frame.pack(fill=tk.X, pady=15)
    
    if ambiente_ok:
        status_text = "Todos os componentes estão disponíveis."
        status_color = "green"
    else:
        status_text = "Alguns componentes estão faltando. Use Verificar Ambiente."
        status_color = "red"
    
    status_label = ttk.Label(status_frame, text=status_text)
    status_label.pack(pady=10, padx=10)
    
    # Frame para botões com grid layout
    button_frame = ttk.Frame(main_frame)
    button_frame.pack(fill=tk.BOTH, expand=True, pady=15)
    
    # Configura as colunas para centralizar
    button_frame.columnconfigure(0, weight=1)
    
    # Botões de ação
    verificar_btn = ttk.Button(
        button_frame, 
        text="Verificar Ambiente",
        command=executar_verificador
    )
    verificar_btn.grid(row=0, column=0, pady=5, sticky="ew")
    
    instalar_btn = ttk.Button(
        button_frame, 
        text="Instalar/Configurar",
        command=executar_instalador
    )
    instalar_btn.grid(row=1, column=0, pady=5, sticky="ew")
    
    iniciar_btn = ttk.Button(
        button_frame, 
        text="Iniciar Aplicação",
        command=executar_aplicacao
    )
    iniciar_btn.grid(row=2, column=0, pady=5, sticky="ew")
    
    # Rodapé
    footer_frame = ttk.Frame(main_frame)
    footer_frame.pack(fill=tk.X, pady=(15, 0))
    
    ttk.Label(
        footer_frame, 
        text="Para assistência, consulte a documentação ou envie um e-mail para suporte@mt5extracao.com",
        font=("Arial", 8)
    ).pack(side=tk.BOTTOM)
    
    # Inicia o loop de eventos
    root.mainloop()

def main():
    """Função principal"""
    print("===== MT5 Extração - Inicialização =====")
    print("Verificando ambiente de execução...")
    
    # Mostra a tela de splash
    splash_root, update_message, close_splash = mostrar_splash()
    splash_root.update()
    
    # Thread de inicialização
    def inicializacao_thread():
        try:
            # Verificar dependências críticas
            update_message("Verificando dependências críticas...")
            instalar_setuptools(update_message)
            
            # Verificar ambiente
            update_message("Verificando arquivos do sistema...")
            ambiente_ok, arquivos_faltantes = verificar_ambiente(update_message)
            
            # Finaliza o splash e executa a interface principal
            update_message("Iniciando interface principal...")
            time.sleep(1)  # Curta pausa para o usuário ver o último status
            
            # Fecha o splash e continua com a inicialização principal
            splash_root.after(0, close_splash)
            
            # Verificar se todos os arquivos necessários estão presentes
            if not ambiente_ok:
                print(f"ERRO: Arquivos necessários faltando: {', '.join(arquivos_faltantes)}")
                messagebox.showerror("Erro", 
                    f"Arquivos necessários não encontrados: {', '.join(arquivos_faltantes)}\n\n"
                    "Verifique se você está executando a aplicação no diretório correto.")
                return
            
            # Mostra interface gráfica
            splash_root.after(100, mostrar_gui_simples)
            
        except Exception as e:
            logging.error(f"Erro durante a inicialização: {e}")
            print(f"ERRO: {e}")
            
            try:
                splash_root.after(0, close_splash)
                messagebox.showerror("Erro", f"Erro durante a inicialização: {e}")
            except:
                pass
    
    # Iniciar thread em segundo plano
    init_thread = threading.Thread(target=inicializacao_thread)
    init_thread.daemon = True
    init_thread.start()
    
    # Mainloop do splash
    splash_root.mainloop()

if __name__ == "__main__":
    main() 