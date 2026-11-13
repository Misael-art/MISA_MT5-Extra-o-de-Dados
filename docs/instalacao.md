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
| Python | Procura um Python 3.11–3.13 com tkinter (CI cobre 3.11; 3.12 e 3.13 são aceitos sem cobertura do CI). Se não houver, instala o Python 3.11 via `winget`; se o winget não existir, usa o instalador oficial do python.org (somente para o usuário atual) |
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

**MT5 oculto.** Para o MetaTrader 5 não ocupar a tela, ligue `[MT5] hidden = true` em `config/config.ini` e
reinicie a ponte (`./scripts/mt5-bridge.sh restart`). O terminal, o MetaEditor e o Strategy Tester passam a rodar
em uma tela virtual (é preciso o pacote do Xvfb). Para uma execução só: `MT5X_HIDDEN=1 mt5x ...`.
`mt5x mt5 status` diz se há um terminal aberto fora da vista e `mt5x mt5 show` grava uma imagem do que ele está
mostrando. Um terminal oculto continua operando: não o esqueça aberto com um robô.

**Avisos.** Com `[APP] notifications = true`, cada ação concluída (extração, compilação, teste, bateria, início e
parada de robô) mostra um aviso do sistema. É o par do modo oculto: você não vê o MT5, mas fica sabendo do que
aconteceu. Em um `config.ini` antigo, acrescente a chave à seção `[APP]`.

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

## Google Drive (cópia na nuvem, opcional)

Na aba **Recursos → Google Drive**, clique em **Conectar Google Drive**. O App procura sozinho uma credencial
OAuth do tipo "App para computador" (cofre do sistema, pasta de configuração `~/.config/mt5x` ou `%APPDATA%\mt5x`,
e arquivos `client_secret*.json` dos últimos 30 dias em Downloads, Área de Trabalho e Documentos), abre o
navegador no consentimento do Google e recebe a resposta em `127.0.0.1` (PKCE). Sem credencial, o botão
**Criar credencial no Google** abre o Console: crie o projeto, ative a Google Drive API, crie um "ID do cliente
OAuth" do tipo "App para computador" e baixe o JSON; não é preciso mover o arquivo.

- Escopo único `drive.file`: o App só vê a pasta **MT5X Backup** que ele cria.
- Credencial, token de renovação e vínculo ficam no cofre de senhas do sistema (pacote `keyring`, em
  `requirements-optional.txt`); sem cofre disponível o App recusa e explica, nunca grava o token em arquivo.
- **Verificar destino** confere, só lendo, conta, pasta, lixeira, compartilhamento, permissão de gravação e espaço.
- **Desconectar** revoga no Google e apaga o token do cofre.
- Linha de comando: `mt5x resources drive status|connect|verify|disconnect [--client-file ARQ]`.

- **Enviar cópia agora**: copia a base ativa com ela em uso (`quick_check`), cifra com AES-256-GCM neste
  computador, envia de forma retomável e confere o MD5 recebido pelo Google. Envio bloqueado se a verificação do
  destino acusar problema. Nada é apagado no Drive e não há envio automático.
- **Código de recuperação**: criado no primeiro envio; o App só envia depois que você o redigita. Guarde-o fora do
  Drive. Num computador novo, **Já tenho um código...** recria a chave.
- **Ensaiar restauração**: baixa a cópia mais nova para uma pasta temporária, confere MD5, decifra, compara o
  SHA-256 e roda `PRAGMA integrity_check`; a base em uso não é tocada.
- Cofre bloqueado (KWallet/Chaveiro): a guia avisa sem abrir janela; Conectar pede o desbloqueio.
- CLI: `mt5x resources drive key|confirm-key|restore-key` exige terminal interativo; o código é exibido
  explicitamente no terminal e digitado com entrada oculta. Nunca passe o código em argumentos, scripts ou logs.
  Na interface, a confirmação e a recuperação usam campos ocultos.
- `mt5x resources drive send [--scope ativa|pesquisa|qa] [--source ARQ]`, `list` e `drill [--file-id ID]`
  mantêm a saída estruturada sem códigos de recuperação. No Linux, `./run.sh --offline --cli ...` executa
  operações locais sem Wine/ponte; no Windows, use `run.bat --cli ...`.
- A mesma cópia tem ID remoto pré-gerado persistido no cofre; retry e retomada após reiniciar o processo
  reutilizam esse ID. Em falha, o arquivo cifrado fica no preparo local e a URL de sessão fica só no cofre.
  Preserve ambos para retomar. Nada apaga a última cópia boa.
- `download-plan [--file-id ID]` informa origem, bytes, MD5 e argumentos de conferência sem transferir.
  Downloads acima de 70 MB são manuais, incluindo o total de partes; depois use
  `drill --file-id ID --downloaded-file ARQ`. O ensaio confere também as contagens de tabelas da cópia nova.
- O recibo separa `snapshot_state`, `upload_state`, `remote_verification_state` e `restore_state`. Um envio com
  MD5 remoto conferido ainda tem restauração `not_tested`; checksum ausente fica `pending`, com `ok=false`.
  A restauração só muda para `verified` depois do ensaio. MD5 remoto não é uma prova de SHA-256 remoto.
- A capacidade vem de `storageQuota` quando conhecida e dos erros da API, sem quota fixa no App.
  Limites por projeto da API continuam válidos, inclusive com várias contas selecionadas pelo usuário.
- Referências oficiais: [OAuth desktop](https://developers.google.com/identity/protocols/oauth2/native-app),
  [drive.file](https://developers.google.com/workspace/drive/api/guides/api-specific-auth),
  [limites](https://developers.google.com/workspace/drive/api/guides/limits),
  [upload retomável](https://developers.google.com/workspace/drive/api/guides/manage-uploads) e
  [download parcial](https://developers.google.com/workspace/drive/api/guides/manage-downloads).
- Consentimento e transferência com conta real aguardam a qualificação sintética da composição completa.
  PostgreSQL continua com aceite separado por `pg_dump`/`pg_restore`.

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

## Conta do MetaTrader 5 (login automático)

Você **não precisa** informar login, senha ou servidor: o app usa a conta em que o terminal MT5
estiver logado. As credenciais no `.env` são opcionais.

Um MT5 recém-instalado ainda não tem conta. Nesse caso o instalador do Linux oferece abrir a **conta de
demonstração gratuita** (dinheiro virtual, servidor `MetaQuotes-Demo`) e preenche o assistente do MT5
sozinho; você só autoriza. Para fazer isso depois, ou no Windows:

```bash
.venv/bin/python -m mt5_extracao.bootstrap demo-account          # pergunta antes
.venv/bin/python -m mt5_extracao.bootstrap demo-account --yes    # sem perguntas
```

- O cadastro usa dados do projeto (não os seus): nome "Projeto Extracao", e-mail
  `mt5-extracao@example.com`, celular `11912345678`. Troque com `--first`, `--last`, `--email`,
  `--phone` e `--birth AAAA-MM-DD`.
- Abrir a conta significa aceitar os termos e a política de proteção de dados da MetaQuotes.
- A conta `MetaQuotes-Demo` tem Forex e CFDs (ex.: `EURUSD`), não os ativos da B3 (veja abaixo).
- Limitações: validado no Linux (Wine) com o MT5 build 6257; no Windows o comando existe, mas ainda não
  foi testado. Supõe datas com o ano por último e o código de país que o MT5 sugere; se o formulário
  recusar algum dado, o comando avisa e você completa na janela do MT5.

### Ativos da B3 (WIN, WDO, ações)

`WIN$N`, `WDO$N`, `PETR4` etc. só existem nos servidores de corretoras brasileiras. Se você pedir um deles
na conta de demonstração da MetaQuotes, o app avisa que o servidor não tem o símbolo.

1. Abra uma conta (demo ou real) em uma corretora que ofereça MT5 com B3. Esta é a única etapa manual:
   o cadastro é seu, com a corretora.
2. Instale o MT5 da corretora e aponte o app para ele:
   `.venv/bin/python -m mt5_extracao.bootstrap configure --mt5-path "<pasta do MT5 da corretora>"`
3. Preencha `MT5_LOGIN`, `MT5_PASSWORD` e `MT5_SERVER` no `.env` (o `configure` pergunta). A partir daí o
   app entra nessa conta sozinho a cada conexão. O nome do servidor deve ser igual ao exibido no MT5.

Símbolos personalizados criados dentro do MT5 não são criados pelo app (a API Python do MT5 não permite) e
não recebem cotações sozinhos. Para usar histórico de outra origem, veja a fonte externa CSV em
[plano_fallback_m1.md](plano_fallback_m1.md).

## Atalhos (área de trabalho e menu)

O instalador do Linux cria um atalho com ícone na área de trabalho e outro no menu de aplicativos, ambos
para o `run.sh`. Para criar, recriar ou remover depois:

- no aplicativo: **Arquivo → Atalhos na área de trabalho e no menu...** (também em Configurações);
- pelo terminal: `.venv/bin/python -m mt5_extracao.bootstrap shortcuts` (opções `--no-desktop`, `--no-menu`, `--remove`).

No Windows a mesma opção cria os atalhos na Área de Trabalho e no menu Iniciar (para o `run.bat`); essa parte
ainda não foi testada em um Windows. O ícone é gerado por `scripts/make_icon.py`.
