# Instalação automática

A instalação baixa e configura tudo sozinha: Python, dependências, MetaTrader 5
e a configuração inicial. Pode ser executada quantas vezes quiser: etapas já
concluídas são puladas, e o seu `config/config.ini` é preservado.

## Windows 10/11

1. Baixe o projeto (botão **Code → Download ZIP** no GitHub) e extraia numa pasta
   sem acentos, de preferência (ex.: `C:\MT5Extracao`).
2. Dê **duplo clique em `install.bat`**.
3. Responda às perguntas (ou apenas Enter para aceitar o padrão). Se o Windows
   pedir permissão de administrador para o MetaTrader 5, clique em **Sim**.
4. No final, use o atalho **MT5 Extração** na Área de Trabalho (ou `run.bat`).

O instalador:

| Etapa | O que acontece |
|---|---|
| Python | Procura um Python 3.9–3.13 com tkinter. Se não houver, instala o Python 3.11 via `winget`; se o winget não existir, usa o instalador oficial do python.org (somente para o usuário atual) |
| Dependências | Cria `.venv` e instala `requirements.txt` (inclui o pacote `MetaTrader5`) |
| MetaTrader 5 | Procura instalações existentes, incluindo as de corretoras. Se não houver, baixa o `mt5setup.exe` oficial e instala em modo automático (`/auto`) |
| Configuração | Gera `config\config.ini`, `config\default_symbols.json` e, se você quiser, o `.env` com as credenciais |
| Final | Cria o atalho e roda o diagnóstico |

Opções (pelo terminal): `install.bat -Yes -NoMT5 -NoShortcut -MT5Path "C:\Program Files\Minha Corretora MT5"`

## Linux (Ubuntu, Debian, Mint, Fedora, Arch, openSUSE)

```bash
git clone <url-do-repositorio> mt5-extracao && cd mt5-extracao
./install.sh
```

O MetaTrader 5 e o pacote Python `MetaTrader5` só existem para Windows. No Linux, o
instalador monta este arranjo automaticamente:

```
 Linux                                     Wine (prefixo dedicado)
┌───────────────────────────┐   RPyC    ┌──────────────────────────────────┐
│ app.py (Python do Linux,  │◄─────────►│ Python 3.11 do Windows           │
│ .venv)                    │ 127.0.0.1 │  └ scripts/mt5_bridge_server.py  │
└───────────────────────────┘  :18812   │     └ pacote MetaTrader5 ──► terminal64.exe
                                        └──────────────────────────────────┘
```

| Etapa | O que acontece |
|---|---|
| Pacotes do sistema | `python3`, `venv`, `tkinter`, `curl`, `Xvfb` e Wine 8+ (no Debian/Ubuntu, do repositório oficial WineHQ). Usa `sudo` |
| Python | Cria `.venv` (prefere um Python que tenha tkinter) e instala `requirements.txt` |
| Wine + MT5 | Cria o prefixo `~/.local/share/mt5-extracao/wine` (Windows 10), baixa e instala o MT5 com `/auto` |
| Ponte | Instala o Python 3.11 do Windows dentro do Wine, mais `MetaTrader5` e `rpyc` |
| Configuração | Gera `config/config.ini` (com `[MT5] wine_prefix` e `[BRIDGE]`), `.env` opcional |
| Final | Atalho no menu de aplicativos, inicia a ponte e roda o diagnóstico |

Sem interface gráfica (servidor), o instalador usa um display virtual (Xvfb).

Opções: `./install.sh --yes --no-mt5 --skip-system-packages --wine-prefix DIR --python python3.11`

Ponte: `./scripts/mt5-bridge.sh start|stop|restart|status|foreground` (log em `logs/mt5_bridge.log`).

> **Estado atual no Linux:** o aplicativo acessa o MT5 pela ponte (`mt5_extracao/mt5_backend.py`)
> sempre que `[BRIDGE] enabled = true`; o `run.sh` inicia a ponte antes de abrir o app. O fluxo está
> coberto por testes com uma ponte real e um MetaTrader5 simulado; a validação com o terminal real no
> Wine é a tarefa **T0.3/T2.6** do [plano de trabalho](PLANO_DE_TRABALHO.md).

## Diagnóstico

```bash
.venv/bin/python -m mt5_extracao.bootstrap doctor        # Linux
.venv\Scripts\python -m mt5_extracao.bootstrap doctor    # Windows
```

Cada item mostra `[ OK ]`, `[AVISO]` ou `[FALHA]`, e cada falha vem com a dica de correção.
`--json` gera uma saída legível por máquina.

## Reconfigurar

```bash
python -m mt5_extracao.bootstrap configure                  # assistente interativo
python -m mt5_extracao.bootstrap configure --mt5-path "C:\Program Files\Minha Corretora MT5"
python -m mt5_extracao.bootstrap locate-mt5                 # lista instalações encontradas
```

Credenciais: nunca passe a senha na linha de comando. Use o assistente
(a senha é digitada de forma oculta) ou `--password-env NOME_DA_VARIAVEL`.
Elas ficam em `.env`, com permissão 600 no Linux, e o arquivo está no `.gitignore`.

## Problemas comuns

| Sintoma | Solução |
|---|---|
| `tkinter` ausente | Debian/Ubuntu: `sudo apt install python3-tk`. No Windows, reinstale o Python marcando "tcl/tk". Depois rode o instalador de novo |
| MT5 de corretora não encontrado | `configure --mt5-path "<pasta que contém terminal64.exe>"` |
| Linux: ponte não inicia | `./scripts/mt5-bridge.sh foreground` mostra o erro. Veja também `logs/mt5_bridge.log` |
| Arch: falha ao instalar o Wine | Habilite `[multilib]` em `/etc/pacman.conf` |
| Proxy corporativo | Defina `HTTPS_PROXY` antes de rodar o instalador |

Logs da instalação: `logs/install-windows.log` e `logs/install-linux.log`.
