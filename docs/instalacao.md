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

## Linha de comando (sem interface gráfica)

Tudo o que a interface faz para extração também pode ser feito pela linha de comando, útil em
servidores e para agendar atualizações. Use pelo lançador do projeto:

```bash
./run.sh --cli <comando> ...        # Linux
run.bat --cli <comando> ...         # Windows
```

| Comando | Exemplo |
|---|---|
| Diagnóstico | `./run.sh --cli doctor` |
| Símbolos do MT5 | `./run.sh --cli symbols --group "*WIN*"` |
| Tabelas do banco | `./run.sh --cli tables` |
| Extração histórica | `./run.sh --cli extract --symbols 'WIN$N,WDO$N' --tf M1 --from 2024-01-01 --to 2024-06-30 --indicators` |
| Atualizar até agora | `./run.sh --cli update --symbols 'WIN$N' --tf M1` |
| Ticks (bid/ask/last) | `./run.sh --cli ticks --symbols 'WIN$N' --from 2024-06-03 --to 2024-06-07` |
| Book de ofertas (DOM) | `./run.sh --cli book --symbols 'WIN$N' --interval 1 --duration 3600` (horário do computador, em UTC) |
| Exportar | `./run.sh --cli export --table win_n_1_minuto --format csv --out win.csv` (também `excel`, `parquet`, `duckdb`) |

Timeframes aceitos: `M1 M5 M15 M30 H1 H4 D1 W1 MN1` (também `1min`, `1 hora`...). No Linux, coloque
símbolos com `$` entre aspas simples (`'WIN$N'`). Códigos de saída: 0 sucesso, 1 falha, 2 uso incorreto.

### Agendamento (manter a base atualizada)

```bash
./run.sh --cli schedule --every 15m --symbols 'WIN$N,WDO$N' --tf M1
```

O comando **mostra** (não instala) a linha pronta para o `cron` (Linux) ou o comando `schtasks`
(Windows). Por padrão, das 9h às 18h, de segunda a sexta (`--hours 9-18 --days 1-5`). Revise e cole:
no Linux, com `crontab -e`; no Windows, no Prompt de Comando. A saída do cron vai para `logs/cron.log`.

### Estratégias e triagem de ativos

| Comando | Exemplo |
|---|---|
| Lista das estratégias | `./run.sh --cli strategies` |
| Especificações do contrato | `./run.sh --cli specs --symbols 'WIN$N,WDO$N'` |
| Triagem (Filtros A e B) | `./run.sh --cli screen --tf D1` |
| Validação (Filtro C) | `./run.sh --cli validate --tf D1 --strategies all` |
| Backtest isolado | `./run.sh --cli backtest --symbol 'WIN$N' --tf M15 --strategy orb` |
| Relatório HTML | `./run.sh --cli report --tf D1 --open` |

Guia completo: [estrategias.md](estrategias.md).

## Banco PostgreSQL / TimescaleDB (opcional)

O padrão é SQLite (um arquivo, nada a instalar). Para bases grandes ou acesso por várias máquinas:

1. Crie o banco no servidor (ex.: `createdb mt5`) e instale o driver: `.venv/bin/pip install "psycopg[binary]"`
   (o instalador já tenta instalá-lo junto com os opcionais).
2. Em `config/config.ini`:
   ```ini
   [DATABASE]
   type = postgresql
   url = postgresql://usuario@localhost:5432/mt5
   ```
3. No arquivo `.env`: `DB_PASSWORD=sua_senha` (a senha nunca vai no `config.ini`).
4. Confira com `./run.sh --cli tables`.

Se a extensão TimescaleDB existir no banco (`CREATE EXTENSION timescaledb;`), as tabelas de barras
novas são criadas como hypertables automaticamente.

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

Logs da instalação: `logs/install-windows.log` e `logs/install-linux.log`. Log do programa (interface e
linha de comando): `logs/mt5_extracao.log` (gira a cada 5 MB, guarda 5 arquivos). Na linha de comando,
`-v` também mostra os detalhes no console.
