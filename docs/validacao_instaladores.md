# Validação dos instaladores com o MT5 real (T0.3 e T2.6)

Registro das execuções manuais que o CI não cobre (download e instalação do MT5, Wine,
ponte com o MT5 real). Roteiro em [PLANO_DE_TRABALHO.md](PLANO_DE_TRABALHO.md), seções T0.3 e T2.6.

## T0.3 — Instaladores

| Ambiente | Data | Comando | Resultado |
|---|---|---|---|
| BigLinux/Manjaro (família Arch), KDE Wayland, Wine 11.17, máquina de uso (não é VM limpa) | 2026-10-12 | `./install.sh --yes` | ✅ com as correções abaixo. `doctor`: 20 OK, 1 aviso (`pandas-ta` opcional), 0 falhas; ponte ATIVA; MetaTrader5 5.0.6231, terminal build 6257 |
| Reexecução (mesma máquina) | 2026-10-12 | `./install.sh --yes` (2ª e 3ª vez) | ✅ etapas puladas, `config.ini` preservado |
| Windows 11 (usuário comum) | — | `install.bat` | ⏳ pendente |
| Windows 10 sem winget | — | `install.bat` | ⏳ pendente |
| Windows com MT5 de corretora | — | `install.bat` | ⏳ pendente |
| Ubuntu 24.04 desktop | — | `./install.sh` | ⏳ pendente |
| Ubuntu 22.04 servidor (sem X) | — | `./install.sh --yes` | ⏳ pendente |
| Debian 12 | — | `./install.sh` | ⏳ pendente |
| Fedora 40 | — | `./install.sh` | ⏳ pendente |

### Falhas encontradas e corrigidas (Linux, 2026-10-12)

1. **Download do MT5 dava 404.** A URL `.../metaquotes.software.corp/metatrader5/mt5setup.exe` não existe;
   a correta é `.../metaquotes.software.corp/mt5/mt5setup.exe`. Corrigido em `linux.sh` e `windows.ps1`
   (no Windows só a URL foi conferida; o instalador não foi executado).
2. **Pacotes do sistema sem terminal.** Sem TTY o `sudo` não tinha como pedir a senha. O instalador agora
   só pede permissão se faltar pacote (Arch) e, sem terminal, usa a janela de senha do sistema (`pkexec`).
   Também deixou de usar `pacman -Sy` (atualização parcial).
3. **Conexão: 5 tentativas erradas antes da certa.** No Wine, `initialize(path=<pasta>)` falha com
   `IPC initialize failed, Process create failed`; só o caminho do `terminal64.exe` funciona. Pela ponte,
   essa estratégia agora é a primeira.
4. **Terminal novo não tem conta.** O MT5 build 6257 abre um assistente em vez de criar a conta demo, e
   `initialize` fica em `IPC timeout`. Novo comando `python -m mt5_extracao.bootstrap demo-account`
   (chamado pelo instalador) abre a conta de demonstração gratuita; o `doctor` ganhou o item "Conta no MT5".
   Testado de ponta a ponta partindo de um terminal sem conta: 20 s.
5. **Reexecutar o instalador com o MT5 aberto travava** em `wineserver -w` (que espera todos os processos
   do Wine). Agora não espera quando o terminal ou a ponte estão em execução.

6. **Preços de outro ativo gravados como `WIN$N` (integridade).** `WIN$N` não existe na `MetaQuotes-Demo`,
   mas a autocorreção de nomes removia o `$` e achava `WINN` (ação dos EUA, ~US$ 35): a extração gravava
   esses preços em `win_n_1_minuto` e informava "Sucesso". A variação que remove o `$` foi retirada; agora
   a extração falha e a CLI explica que o servidor não tem o símbolo e o que fazer.
7. **Credenciais do `.env` nunca eram usadas.** O parâmetro `auto_login` do conector não fazia nada. Agora,
   com `MT5_LOGIN`/`MT5_PASSWORD`/`MT5_SERVER` preenchidos, o app entra nessa conta sozinho; se o login
   falhar, a conexão falha (não segue na conta errada). Validado só com o MT5 falso: falta testar com a
   conta de uma corretora.

### Interface gráfica (validada em 2026-10-12 com o MT5 real, em display virtual Xvfb)

Todas as telas foram abertas e os fluxos de extração histórica e coleta em tempo real executados pela GUI.

| Problema encontrado | Correção |
|---|---|
| A GUI não abria com Tk 9 (`trace("w", ...)` foi removido): "Erro Fatal de Inicialização" | `trace_add("write", ...)` |
| Abertura congelava por minutos com 12.306 símbolos: uma consulta e uma gravação em `_symbol_tables` por símbolo | `DatabaseManager.symbols_with_data`: uma leitura, nenhuma gravação |
| O menu (Exportar, Estratégias, Ajuda) nunca era criado | `create_menu()` passou a ser chamado |
| A janela de extração nunca mostrava o fim (threads mexiam no Tkinter) | fila `call_in_ui`; resumo "Concluído: N ativo(s) com sucesso" |
| Coleta em tempo real: erros nos indicadores (`time` como texto) | conversão para datetime |
| Janela fixa em 1680x980 cortava o conteúdo em telas menores; listas não acompanhavam a altura | tamanho limitado à tela; listas expansíveis; busca acima da lista |
| "Status MT5: Desconectado" mesmo conectado; botão de administrador no Linux | mostra conta e servidor em uso; botão só no Windows |
| MT5 sem conta / desconectado | a GUI oferece abrir a conta de demonstração ou reconectar |
| "Configurações" era só "em desenvolvimento"; "Sobre" cortado; estatísticas com `None`/`nan` | tela de configuração em uso, diagnóstico em Ajuda, textos corrigidos |

Não validado: aparência em um desktop real com gerenciador de janelas (as capturas foram em Xvfb), a GUI no
Windows e o diálogo de abertura da conta demo pela GUI (o comando por trás dele foi testado).

### Extração real (conta MetaQuotes-Demo, pela ponte)

| Verificação | Resultado |
|---|---|
| `mt5x symbols --group "*EURUSD*"` | ✅ (12.306 símbolos na conta) |
| `mt5x extract --symbols EURUSD --tf M1 --from 2026-10-05 --to 2026-10-09` | ✅ 7.200 barras em 14 s |
| Reextrair o mesmo período | ✅ continua 7.200 linhas, sem duplicatas |
| `mt5x update --symbols EURUSD --tf M1` | ✅ base avançou até o minuto atual (7.581 linhas) |

Não validado: símbolos da B3 (`WIN$N`, `WDO$N`, `PETR4`...). Conferido no MT5 real que a `MetaQuotes-Demo`
não tem nenhum deles; é preciso a conta de uma corretora brasileira (veja docs/instalacao.md).

## T2.6 — Paridade Windows × Linux

Script: `scripts/parity_check.py` (requer `pyarrow`).

| Lado | Data | Comando | Resultado |
|---|---|---|---|
| Linux (ponte) | 2026-10-12 | `parity_check.py extract --symbol EURUSD --date 2026-10-09 --out linux.parquet` | ✅ 1.440 barras |
| Windows | — | mesmo comando, `--out windows.parquet`, mesma conta/corretora | ⏳ pendente |
| Comparação | — | `parity_check.py compare linux.parquet windows.parquet` | ⏳ pendente |

## Pendências de validação no Windows

O desenvolvimento segue no host atual (Linux, MT5 no Wine). Esta lista junta o que só o Windows real confirma e é
executada de uma vez na tarefa **T10.W** (veja "Regra da pendência do Windows", Fase 10 do
[PLANO_DE_TRABALHO.md](PLANO_DE_TRABALHO.md)). Cada tarefa nova acrescenta aqui as suas linhas. As pendências mais
antigas (instaladores e paridade da T2.6) estão nas tabelas acima e entram na mesma execução.

Use um EA de exemplo do próprio MT5 (`Examples\Moving Average`) e uma conta de demonstração.

| Origem | O que conferir | Comando | Resultado esperado | Data | Situação |
|---|---|---|---|---|---|
| T8.4 | Pasta de dados fora do modo portátil | `mt5x robots install <robô>` | Arquivos em `%APPDATA%\MetaQuotes\Terminal\<id>\MQL5` (achada por `origin.txt`), não na pasta do programa | — | ⏳ pendente |
| T8.4 | Compilação sem o `.bat` do Wine | `mt5x robots compile <robô>` | `.ex5` gerado; erros e avisos lidos do log; caminho com espaço funciona | — | ⏳ pendente |
| T8.4 | Código de saída do MetaEditor | o mesmo | Resultado correto mesmo com o código de saída invertido (1 = compilou) | — | ⏳ pendente |
| T8.5 | Strategy Tester sem `/portable` | `mt5x robots backtest <robô> --symbol EURUSD --tf H1` | Relatório `.htm` na pasta de dados; métricas iguais às do MT5 | — | ⏳ pendente |
| T8.5 | Idioma do relatório | o mesmo, com o terminal em inglês e em português | Mesmas operações lidas nos dois idiomas | — | ⏳ pendente |
| T8.5 | `Common\Files` | backtest de um robô com `InpExportTrades` | `HDK_trades_<magic>.csv` lido de `%APPDATA%\MetaQuotes\Terminal\Common\Files` | — | ⏳ pendente |
| T8.5 | Terminal aberto | o mesmo comando com o MT5 aberto, sem e com `--close-terminal` | Sem a opção: para e explica; com ela: fecha, testa (não há ponte para religar) | — | ⏳ pendente |
| T8.6 | Bateria | `mt5x robots battery <robô>` | Mesmo resultado que no Linux para o mesmo backtest | — | ⏳ pendente |
| T8.7 | Início em demo | `mt5x robots start <robô> --symbol EURUSD --tf H1 --close-terminal` | Só depois de validada no Linux (lista da T8.7 no plano): robô no gráfico, na conta conferida | — | ⏳ pendente |
| T8.7 | PARAR TUDO | `mt5x robots stop` e `--release` | `HDK_KILL.txt` criado e removido em `Common\Files` | — | ⏳ pendente |
| T9.1 | Saída `--json` no console do Windows | `mt5x --json status` | Um único objeto JSON, acentos corretos (cp1252 × UTF-8) | — | ⏳ pendente |
| T9.2 | Auditoria | `mt5x audit` | Roda; o item de permissão do `.env` é coerente com o Windows | — | ⏳ pendente |
| T10.2 | Biblioteca de segurança | `mt5x robots compile <robô que declara safety_lib>` com `safety_lib_dir` apontando para o clone | Biblioteca em `MQL5\\Include\\MT5X\\Safety` da pasta de dados; robô compila | — | ⏳ pendente |
| Exatidão | Só o terminal desta instalação é fechado | com dois MT5 instalados e abertos, `mt5x robots backtest <robô> ... --close-terminal` | Fecha só o de `[MT5] path`; o outro continua aberto | — | ⏳ pendente |
| Exatidão | Ticks reais | `mt5x robots backtest <robô> --symbol EURUSD --tf H1` com `tester_model = 4` | Relatório gerado; "Qualidade do histórico" conferida à mão | — | ⏳ pendente |
| T11.3 | MT5 minimizado no modo oculto | com `[MT5] hidden = true`, `mt5x robots backtest <robô> --symbol EURUSD --tf H1` | O terminal abre minimizado, sem tomar o foco, e o teste termina normalmente | — | ⏳ pendente |
| T11.1 | Timeframe montado | `mt5x resample --symbols EURUSD --base M1 --tf M5,H1` | Barras iguais às extraídas do MT5 | — | ⏳ pendente |
| Geral | Suíte de testes | `.venv\Scripts\python -m pytest -q` | Mesmos aprovados do Linux, menos os específicos de Linux | — | ⏳ pendente |

A ponte restrita (T9.3) é só do Linux e não entra nesta lista.
