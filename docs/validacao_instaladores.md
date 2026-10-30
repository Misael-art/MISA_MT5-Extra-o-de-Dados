# Validação dos instaladores com o MT5 real (T0.3 e T2.6)

Pendência Recursos (2026-10-21): conferir em Windows físico a leitura incompleta
de pastas, com backups/restauração acessíveis e limpeza indisponível.49 regressões,
24 testes de retenção, três de interação UI e cinco próprios finais passaram no
Linux (seleções sobrepostas); recuperação/exportação foram QA em SQLite privado,
com fonte candidata. Rede/I/O bloqueado e duração total de operações não recebem
garantia pelo prazo cooperativo da leitura opcional. CI final próprio pendente.

Pendência T19.34 (2026-10-21): conferir o contexto de Robôs em Windows físico,
após concluir também o novo CI: rodada66571c4 falhou no alcance do viewport em
900×620/144DPI; ensaio passou a usar posições internas antes do mapeamento.
com96/144DPI, fontes ampliadas, dois temas e rolagem/teclado em900×620. O runner
Windows da ponta8056fd6 teve uma falha de mapeamento no ensaio144DPI; o ensaio
passou a percorrer ambas as rolagens e aguardar geometria por prazo limitado,
preservando os limites e acrescentando visibilidade. Novo CI obrigatório ainda
pendente; Tk/Xvfb local e CI não substituem este checklist físico.

Registro das execuções manuais que o CI não cobre (download e instalação do MT5, Wine,
ponte com o MT5 real). Roteiro em [PLANO_DE_TRABALHO.md](PLANO_DE_TRABALHO.md), seções T0.3 e T2.6.

## T0.3 — Instaladores

Pendência específica Windows — correção REPLAY de 21/10/2026: abrir o painel
HADOUKEN pelo card, operar abas/pausa, encerrar só a própria sessão e conferir
backtest minimizado. Caminhos/visibilidade foram simulados em testes Linux;
isso não comprova execução em Windows físico. [Contrato](sdk/replay_painel_manual.md).

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
| T7.2 | Símbolos das tabelas | abrir as abas Ativos e Minha base | ★/☆, ☑/☐ e 🟢 🔴 ⚪ 🎯 ❌ aparecem desenhados (no Windows o Tk usa as fontes do sistema) | — | ⏳ pendente |
| T7.2 | Bateria pela interface | marcar duas séries em Minha base, "Iniciar Backtest", escolher duas estratégias Python | Janela não trava; barra de status mostra o andamento com CPU e RAM; a coluna Robô Lucrativo muda ao fim de cada teste | — | ⏳ pendente |
| T7.2 | Robô MQL5 pela interface | o mesmo, com um robô MQL5 compilado e o MT5 aberto | Pergunta antes de fechar o MT5; testa; "Verificar conexão" reconecta (não há ponte para religar) | — | ⏳ pendente |
| T12.L1 | Organização por partes | `mt5x mt5 layout`; desligar `layout_hide_toolbars` e iniciar um robô; `mt5x mt5 restore-layout` com o MT5 fechado | O estado bate com a tela; as barras ficam; a devolução traz as janelas de volta sem mudar outras opções do terminal | — | ⏳ pendente |
| T12.L2 | Opções na interface | Arquivo > Configurações: desmarcar e marcar as caixas da "Janela do MetaTrader 5" | `config.ini` muda só na linha da chave (comentários ficam); "Devolver o arranjo anterior agora" recusa com o MT5 aberto | — | ⏳ pendente |
| T12.L3 | Abrir visão MT5 | botão "Abrir visão MT5" (ou `mt5x mt5 view --close-terminal`) com o MT5 fechado e com ele aberto | Abre com as janelas e barras como marcado; aberto, pergunta antes; não há ponte para religar; "Verificar conexão" reconecta | — | ⏳ pendente |
| T12.L3 | Disposição do painel | escolher "Ampla" em Configurações e iniciar o painel pelo programa; depois trocar e "Aplicar e abrir o MT5 agora" | O painel abre na disposição escolhida nas duas situações | — | ⏳ pendente |
| T12.4 | Ordens pendentes no executor | compilar e rodar `Tests/HDK_PendingOrders_Test.mq5` do HADOUKEN no Strategy Tester (EURUSD H1, cada tick) | `RESUMO passou=50 falhou=0 concluido=sim` em `Common\Files\HDK_Tests` | — | ⏳ pendente |
| T12.4 | Boleta de ordem pendente no painel | em conta **demo**, com `mt5x mt5 view`: aba Boleta, afastar o preço (`»»`), enviar uma compra limite e uma venda stop, cancelar com `×` e com "Cancelar todas"; repetir e clicar em PAUSAR | as ordens aparecem na lista e em `mt5x portfolio` com stop; os cancelamentos tiram as ordens da conta; a aba cabe no painel sem cobrir o FECHAR TUDO | — | ⏳ pendente |
| T12.L4 | Painel liberado em demo | "Abrir visão MT5" em conta demo; marcar "Manter o painel bloqueado..." e abrir de novo | Na primeira o painel abre liberado; na segunda, bloqueado, e "Liberar o PARAR TUDO" o desbloqueia | — | ⏳ pendente |
| Correção | Abertura do programa | abrir pelo atalho com o MT5 fechado; repetir sem internet | A janela aparece em poucos segundos; o MT5 abre organizado e conecta; sem internet, a mensagem diz isso e o que fazer | — | ⏳ pendente |
| T11.1 | Timeframe montado | `mt5x resample --symbols EURUSD --base M1 --tf M5,H1` | Barras iguais às extraídas do MT5 | — | ⏳ pendente |
| T11.4 | Aviso do sistema | com `[APP] notifications = true`, `mt5x extract --symbols EURUSD --tf H1 --from <data>` | Um balão "Extração concluída" aparece ao terminar | — | ⏳ pendente |
| T11.9 | Painel vivo | `mt5x panel --serve --open`, clicar em "Testar o que falta" de uma estratégia Python | A página mostra o progresso e a matriz se atualiza sozinha | — | ⏳ pendente |
| T15 | Gestão de recursos | aba Recursos: "Medir agora", arquivar uma série obsoleta, restaurá-la, "Otimizar"; depois `mt5x resources auto` no Agendador de Tarefas com `[RESOURCES] auto_enabled = true` | Disco livre e tamanho batem com o Explorador; a série some e volta com as mesmas linhas; a trava `.lock` é apagada ao fim; o agendador roda sem a janela aberta; "Fazer cópia agora" com o programa extraindo gera cópia que abre; "Aparar série..." e "Voltar a esta cópia..." com o programa aberto (arquivo travado pelo Windows?) | — | ⏳ pendente |
| T17.1–T17.5 | Fonte, escala e telas da interface | abrir claro/escuro em 1280×800 e 1920×1080, usar escala de exibição de 125%/150%, reduzir e maximizar a janela; navegar com Tab | Fonte sem serifa disponível ou fallback do Tk; textos, controles e gráficos sem cortes; foco visível; amostra insuficiente, dados parciais e estados de segurança continuam explícitos | — | ⏳ pendente (Windows real) |
| T17.24 | Bateria histórica ligada a PF indefinido | Selecionar o card do painel HADOUKEN, abrir Bateria e percorrer texto longo pela barra e Page Down, em claro/escuro e 1280×800/1920×1080 | Sem erro fatal; reprovação, motivo final e ação de refazer acessíveis; consulta preserva registros e aprovações históricas gravadas | — | ⏳ pendente (Windows real) |
| T17.25 | Microestrutura e comparação com contexto completo/parcial | Abrir quatro gráficos e comparar modos, trocar seleção durante cálculo e redimensionar em claro/escuro | População explícita, dados completos preenchidos, parcial com ação útil, identidades/contagens coerentes e figuras percorríveis | — | ⏳ pendente (Windows real com MT5); CI Windows é distinto |
| T17.26 | Ativos e sinal nos cards | Conferir último backtest por ativo, negativo, zero e sem resultado; alternar tema e escala, clicar no ícone | Contagens distintas, setas acompanhadas de texto e seleção funcionando sem promover aprovação | — | ⏳ pendente (Windows real) |
| T17.27 | Layout no DPI nativo | Alternar gráficos/tema e redimensionar nas duas resoluções/DPI; reabrir página após ocultar | Legendas completas fora dos dados e altura estável pela fonte, sem crescimento acumulado | — | ⏳ pendente (Windows físico com MT5); CI é distinto |
| T17.28 | Comandos dos cards no MT5 nativo Windows | Em DEMO: selecionar ativo/preset, revisar PLAY em duas etapas e alerta negativo, parar; REPLAY visual/ticks reais e STOP; BACKTEST minimizado com outro terminal aberto | Contexto/relatório coerentes, pulso acompanha posições, apenas a instalação da sessão é encerrada e terminal original preservado | — | ⏳ pendente (Windows físico); caminhos simulados/CI são distintos |
| T20.2 | Referências financeiras dos motores Python | `.venv\Scripts\python -m pytest -q tests/test_financial_references.py tests/test_backtester.py tests/test_real_costs.py tests/test_trade_resim.py` | Contas manuais de lote/teto/passo, bid/ask, comissões, swap por parcela/lote e recusa de PnL inválido; sem exigir MT5 | — | ⏳ pendente (Windows físico); cálculos sem ramificações por sistema, referências verificadas no Linux |
| T20.3 | Patrimônio flutuante e legado | `.venv\Scripts\python -m pytest -q tests/test_floating_equity.py` e `.venv\Scripts\python tests/fixtures/equity_reference/verify.py` | Curvas BID/ASK, parcial/swap, sobreposição, pico não positivo, fluxos por cotas e legado preservado; fixture nativo conciliado | — | ⏳ pendente (Windows físico); Linux/Wine observado separadamente |
| T20.4 | Manifestos e replay de dados | `.venv\Scripts\python -m pytest -q tests/test_dataset_snapshots.py tests/test_sdk_api.py` | Mesma revisão após correção/importação, ausência explícita, serialização e caminhos portáveis | — | ⏳ pendente (Windows físico); SQLite/Linux e PostgreSQL 16 isolado observados |
| T20.5 | Vigência de instrumentos e legado | `.venv\Scripts\python -m pytest -q tests/test_instrument_history.py tests/test_symbol_specs.py tests/test_dataset_snapshots.py` | Conhecimento tardio/vigência, replay histórico, universo descontinuado, contrato/série de estudo, importação formato 1 e backup | — | ⏳ pendente (Windows físico); Linux e PostgreSQL 16 isolado observados |
| T20.6 | Qualidade por consumidor e formatos 1/2/3 | `.venv\Scripts\python -m pytest -q tests/test_data_eligibility.py tests/test_ticks.py tests/test_book_collector.py tests/test_historical_extractor.py tests/test_dataset_snapshots.py` | Volume real/ticks distintos, fonte ausente/gerada/observada, origem por hashes, UTC/causalidade, campo de qualidade ausente e legados preservados | — | ⏳ pendente (Windows físico); Linux e PostgreSQL16 isolado observados |
| T20.24 | Orçamento, cancelamento e retomada da fila | `.venv\Scripts\python -m pytest -q tests/test_batch_budget.py tests/test_robots_batch.py tests/test_robots_runner.py tests/test_sdk_api.py` | Checkpoints/publicação atômicos, orçamento, cancelamento responsivo, legados e caminhos simulados; conferir ciclo nativo/recursos no Windows físico | — | ⏳ pendente (Windows físico); Linux 1200 aprovados e PostgreSQL16 isolado 8 aprovados |
| Geral | Suíte de testes | `.venv\Scripts\python -m pytest -q` | Mesmos aprovados do Linux, menos os específicos de Linux | — | ⏳ pendente |

A ponte restrita (T9.3) é só do Linux e não entra nesta lista.

### Pendência Windows físico — T20.25

- [ ] Conferir posse de um byte com `msvcrt` entre processos, reentrada e queda do
  controlador com MetaEditor/Tester/launcher ainda vivo; só liberar após conferir
  executável e identidade da sessão, sem tomar posse por idade/PID.
- [ ] Executar o backtest padrão com perfil privado, mantendo o terminal principal
  aberto; conferir report/SET/livro próprios, custos reconciliados e origem intacta.
- [ ] Verificar STOP/timeout e espera de filhos próprios, referências protegidas,
  quarentena e restauração sem sobrescrever destino, inclusive locks de arquivos.

Linux/Wine e caminhos Windows simulados não substituem esses ensaios físicos.

### T19.14 — Consulta temporal (pendência Windows físico)

- [ ] Em Windows real, publicar backtest Python e consultar `mt5x --json trades trajectory --run ID`,
  conferindo relógio da fonte, parciais/ciclos e corte `--as-of`. Não equivale a validar fills nativos.
- [ ] Conferir base antiga sem diário e recusa de resultado adulterado, preservando ledger/snapshot.

Linux: suíte final 1381 aprovados/12 pulos/3 avisos, saída 0; PostgreSQL
isolado: 10 aprovados, container removido; doctor:16 OK/2 avisos/0 falhas.
Essas provas não substituem Windows físico.


### T13.3 — Perfis e regimes (pendência Windows físico)

- [ ] Em Windows físico, executar os testes de features/perfis, inclusive UTC
  explícito, limite de nanossegundos, invalidação, legado e contextos M15/H1.
- [ ] Em cópia de uma base antiga, conferir `analytics features compute`,
  `list --all-versions` e `values` por versão/TF, preservando zero/ausência,
  histórico, configurações e credenciais. Conferir os caminhos do instalador.

CI Windows: 1421 aprovados/33 ignorados/5 avisos, cobertura 73,09%, no código
a1c2a77. Linux local: 1438/16/3, cobertura 77,44%; PostgreSQL real isolado: 20
aprovados. Esses resultados não substituem Windows físico nem MT5 conectado;
a T13.3 não introduz leitor MQL5 ou autorização de negociação.

### EXTR-01 integrada — pendência específica de Windows físico

- [ ] Em Windows físico com MT5 DEMO comprovado, reproduzir a jornada curta de
  extração/atualização/repetição/reabertura/consulta/exportação e Cancelar/retomada
  da UI, sem ordens e em banco de teste próprio. Não usar conta REAL.

Linux/Wine no master a92cd8a: sete casos dirigidos aprovados; conector DEMO e UI
reais passaram. CI Windows aprovado na PR117 foi reaproveitado por igualdade de
código; não equivale à prova física acima. Relatório: EXTR-01_VALIDACAO_NATIVA.md.

### EXTR-02 — gerenciador Linux/Wine

- [ ] Windows físico: confirmar que a conexão nativa/instalador continuam usando
  o caminho Windows; a ponte de processos Linux não é usada nessa plataforma.

No Linux, teste com caminhos de espaços e simulação de paths.is_windows. Prova
Wine real de start/status/stop/repetição e consulta curta DEMO; nenhum sinal a
terminal ou wineserver. Veja docs/extrator/EXTR-02_PONTE_BACKGROUND.md.

### T18.1 — registry tipado/DTO (pendência Windows físico)

- [ ] Em cópia de base antiga no Windows físico, registrar o core e os produtores tipados;
  conferir consulta por versão/timeframe, coexistência com definições 1.2.1 e reconstrução
  do contrato 1.3 sem alterar fatos antigos. Conferir repetição e reabertura.
- [ ] Serializar a fixture de DTO v1 com ausência explícita e sem NaN; não apresentar
  a fixture como relação calculada nem ligar a UI a serviço inexistente.

Linux PR124:1501/26/3, saída0, cobertura77,64%; PostgreSQL dedicado aprovado.
O pytest Windows da PR124 passou no ecfe057, na execução existente. CI e Linux são evidências distintas dessa prova física.
T18.1 não muda os caminhos/instaladores nem exige conexão MT5.

### T19.3 — consumidor de indicadores (pendência Windows físico)

- [ ] Em base de teste no Windows físico, materializar M5, repetir sem novas linhas,
  reabrir e consultar/exportar indicadores versionados; confirmar destino preservado em cancelamento.
- [ ] Laboratório: ação Indicadores (CSV), dois temas, DPI96/144, janela estreita/larga;
  conferir cabeçalho completo, foco e diálogo nativo. Relações continua sintética e ação desabilitada.
- [ ] Legado sem relógio e run fixado sem materialização vinculada exibem motivo de ausência.

Linux:27 testes focados, oito capturas/casos Tk sob Xvfb e CLI com 600 barras DEMO arquivadas
→120 M5. Isso não comprova desktop físico Windows. Instaladores/caminhos não alterados;
nenhum terminal/credencial ou processo de terceiros modificado por esta tarefa.

Aceite Linux final T19.3:1392 Ubuntu +132 UI Xvfb +4 UI compatíveis reaproveitados =1528 casos
únicos, cobertura77,64%; check Windows CI aprovado na execução existente do eaec23b; prova física continua pendente.

### T19.4 — Alligator (pendência Windows físico)

- [ ] Em base de teste no Windows físico, materializar Alligator M5, consultar/reabrir e
  exportar CSV; conferir semente/aquecimento, estados de ausência, âncora e offset separados.
- [ ] Confirmar diálogo/caminho com espaço e valores do CSV; não usar offset visual para
  antecipar disponibilidade. Não há consumidor operacional H exposto por esta ficha.

Linux:62 focados e prova CLI em120 M5 de DEMO arquivada; doctor21/1/0. Sem alterações na UI,
instaladores/caminhos; evidência gráfica anterior compatível reaproveitada. CI ainda pendente.

### EXTR-02 — confirmação de saída do processo (follow-up)

Correção Linux-only em Bridge.stop: simulação do caminho Windows nativo passou no teste
existente; esse caminho não foi modificado. A prova física anterior permanece pendente,
sem novo stop da ponte real ou sessão de terceiros.17 testes Linux e doctor21/1/0.

CI da PR127/63e717f: seis checks aprovados;1497 testes Windows CI/64 skips/5avisos.
Isso não encerra a conferência no Windows físico; bootstrap nativo permanece preservado.

Aceite final T19.4/70b2191: seis checks aprovados, incluindo1514 Windows CI/66 skips/5avisos.
Os291 arquivos de produção/testes correspondem ao código final; isso não substitui Windows físico.

### T19.3 — consistência do snapshot (follow-up)

- [ ] No Windows físico, repetir a atualização interrompida por correção da fonte e retomar,
  conferindo ausência de COMPLETE/SKIPPED obsoleto e preservação dos lotes confirmados.

135 casos locais distintos compatíveis e doctor21/1/0; novos casos SQLite e wrappers PG.
CI Windows deste follow-up ainda pendente; não é prova de Windows físico. Ponte não reiniciada.

CI final do follow-up T19.3/1039690: seis checks aprovados,1519 Windows CI/71 skips/5avisos.
SQLite e PostgreSQL exercitaram geração/retomada e retries em lote; Windows físico segue pendente.


### T19.5 — AO/AC (pendência Windows físico)

- [ ] Em Windows físico, consultar/exportar AO/AC materializados pelo serviço existente,
  incluindo aquecimento, ausência e arquivo CSV em pasta com espaço.

Linux:78 casos focados distintos compatíveis e doctor21OK/1opcional/0falhas.32 definições
no registry, interface de exportação preservada; quatro testes atuais da ação da UI aprovados.
CI Windows desta ficha ainda pendente; resultados locais não são prova de Windows físico.

Composição corrigida de T19.5 após129:88 focados/doctor21/1/0; nova prova CLI/store/CSV
120M5/960fatos com assinatura04f94b5b. CI final deste runtime pendente; físico não encerrado.

Aceite final T19.5/11b6c53: seis checks aprovados,1540 Windows CI/73 skips/5avisos,43PG.
293 fontes/testes correspondem ao código final;88 focados e prova CLI04f94b5b preservados.
CI virtual não substitui a validação Windows físico que permanece no checklist acima.

## T19.6 — Pivôs causais

Windows CI aprovado na fonte6a9d580:1569 testes/78 skips/5 avisos; cobertura73,36%. Windows físico continua pendente: instalar/abrir banco existente, materializar pivôs e consultar/exportar origem/confirmação pelo serviço; sem ordens. Não bloqueia o aceite Linux. [Escopo](analytics/T19.6_PIVOS_CAUSAIS.md).


Pendência delimitada T19.26: no Windows físico, conferir publicação por hardlink no
volume de exportação, mensagens em volume sem suporte/caminho longo, .set UTF-16,
filename canônico, repetição sem overwrite e importação. CI Windows/fakes não
substituem esse ensaio; o recorte pode fechar funcionalmente no host Linux/Wine.


T19.29 — Windows físico pendente: validar Session/INI e .set canônicos, flag Visual
real, cancelamento só do processo próprio, diário/recibo, fechamento final e
reconciliação do Manual entre backtest e replay. Sem operação REAL; ensaio sem
negócios só cobre contratos exercitados. O CI/fake e Linux/Wine não substituem isso.


### T20.22 inicial — editor Default/Manual e resultados nativos

- [ ] Windows físico: abrir pelo card S09, nome/revisão/seleção persistida, rolagem em DPI alto, NTFS/Unicode, Cancelar somente sessão própria, inputs realmente carregados e visual nativo com negócios reconciliados.
- CI e ensaio Wine não substituem essa pendência específica; o recorte não altera instaladores.


### T19.16 inicial — ER com idade no S09 v1.40

Aceite dirigido Linux/Wine: Manual nomeado, salvar/reabrir/exportar, 32 inputs
carregados; EURUSD/M5, 14→15/10/2026, backtest e replay conciliados com quatro
saídas ER por execução. Doze processos próprios encerrados, ponte/principal
preservados. Recibo: `docs/evidencias/T19_16_ER_20261020.json`. O check automatizado
Windows valida código com provedores simulados e não substitui o host físico.

- [ ] Windows físico: conferir campos idade mínima/limiar ER, leitura da janela e
  instante conhecido, 32 inputs do S09 v1.40 e resultado persistido no Tester nativo.
- [ ] Windows físico: conferir rolagem/DPI, exportação Unicode/NTFS e encerramento
  somente dos processos próprios, mantendo ponte e terminal principal.

Este checklist não autoriza execução ao vivo DEMO nem herda aceite para Pullback ER,
carteira ou gradientes. S09 permanece restrito ao Tester; cache inelegível.


### T19.7 inicial — publicação Donchian

- [ ] Windows físico: exportar pelo Laboratório contexto Donchian materializado com
  relógio declarado, conferir 14 IDs/estados e destinos Unicode/NTFS. Teste Xvfb com
  fonte controlada não comprova este host. Este recorte não habilita TM06 ou live.


### T19.16/TM06 — S09 v1.50

- [ ] Windows físico: configurar prazo Donchian no Manual, salvar/reabrir/exportar,
  conferir 33 inputs e contexto/barra versus instante conhecido no journal/aba.
- [ ] Windows físico: demonstrar rompimento confirmado, retorno, saída e replay
  conciliados, reinício sem reset e encerramento só dos processos próprios.
  Não libera live DEMO, Pullback temporal, carteira ou cache.

- [ ] TM07/S09v1.60 em Windows físico: configurar prazo ATR base/mínimo/teto,
  carregar os35 inputs, conferir semente/base imutáveis e contexto conhecido,
  backtest/replay com negócios e reconciliação, liberação somente dos processos
  próprios. Linux/Wine e CI Windows não substituem este aceite.

TM07: CI Windows do workflow39488078713 aprovado em Mf5583f7; não equivale
a executar o terminal/Tester no Windows físico. Checklist manual acima permanece
pendente. Ensaio real realizado no Linux/Wine com ponte e terminal preservados.

### T19.20 inicial — filtro de entrada e cota (Windows físico pendente)

- [ ] Carregar S09v1.70/43inputs, configurar janela/exclusão/dias/âncora/cota1/3/N
  em Manual, salvar/reabrir/exportar e conferir campos e horários do servidor.
- [ ] No Tester, reconciliar recusas/primeiros fills, contador após parcial/reinício,
  SL/saídas fora da janela e liberação somente dos processos próprios; preservar ponte/principal.
- [ ] Rejeitar histórico/identidade ausentes sem devolver cota nem suspender proteções;
  registrar limite de DST/calendário futuro, sem inferir do relógio do host.

CI Windows do workflow39499390366 aprovado no runtime372b762; não substitui este
aceite manual do terminal no Windows físico. Par nativo efetivo exercitado em Wine.


T19.20/02 — Windows físico: conferir Manual S09v1.80/44inputs, salvar/reabrir/exportar
cooldown, tick milissegundo/fechamento total versus parcial, replay com recusa e nova
entrada, persistência/consulta e liberação dos processos próprios preservando ponte/
principal. Linux/Wine e CI não substituem este aceite. S09 somente Tester.


Complemento T8.7/DEMO (20/10/2026): Windows físico deve conferir sincronização do perfil privado, seleção EMA+ATR/ativo/TF/preset e inputs reais, identidade DEMO a cada etapa, PLAY/STOP por sessão, eventos/SL/fills/resultados próprios. A preparação Linux/Wine ficou parcial antes do replay por falta de sincronização; nenhum aceite ao vivo foi promovido. S09 e adaptador piloto continuam somente Tester.


- [ ] T19.39/Madrid: validar em Windows físico editor Default/Manual com seis campos,
  fonte1.20/pin870c0f8db71c3d5a/13 inputs e Canvas padrão, exportação e recibo do Tester.
  Tk/Xvfb Linux e compilação6276Wine0/0 não substituem esse aceite; CI final separado.


- [ ] T19.40/HA-HiLo: Windows físico com fonte1.20/pin5dad3bbabab0343f/10inputs,
  Canvas padrão, Default/Manual3campos, exportação/recibo, semTP e SL obrigatório.
  Wine/Tk simulado não prova reversão, histórico do servidor ou comportamento físico.

- [ ] F2/T2.5 sessão existente: executar no Windows físico reuso local por diretório/EXE,
  correspondência de conta/servidor/data_path, fechamento local sem SDKshutdown e
  recusa após identidade alterada. Linux/Wine real e caminhos local/Windows simulados
  estão no recorte de2026-10-21; isso não conclui paridade física T2.6 nem concorrência.

- [ ] T20.12 consulta UI: verificar no Windows físico menu Exibir → Registro de estudos,
  temas/teclado/rolagem, callbacks após fechar e ausência de escrita por consulta.
  Tk/Xvfb Linux e teste PostgreSQL do job têm escopos próprios, sem aceite físico.

- [ ] T19.34 altura: conferir Windows físico com fontes ampliadas e página rolável;
  corpo acessível via scroll externo, contexto por scroll do detalhe, filtros/seleção
  preservados. Escala1,5 em900×620 ainda tem cortes na navegação/resumo/status global;
  este recorte não os qualifica nem substitui o CI Windows final.

## T19.34 — navegação e status (2026-10-21)

- [x] Linux/Tk/Xvfb: nomes completos, foco/teclado, mensagens integrais, duas paletas,
  quatro tamanhos,96/144DPI simulados e fonte1,0/1,5;10 finais aprovados131,60s.
- [x] Histórico privado reaberto sem recálculo; banco owner/MT5 não utilizados;
  fonte e scrollbar do detalhe corrigidas após inspeção de captura.
- [ ] CI próprio da ponta e instalação serial depois das PRs172–175.
- [ ] Windows físico: fonte/DPI/troca de área/Detalhes com teclado.

Não altera instaladores nem qualifica acessibilidade completa, cálculos ou operação.

## T19.34 — resumo/erro do catálogo (2026-10-21)

- [x] Linux/Tk/Xvfb:11 testes finais, resumo/erro completos na largura real, seleção
  preservada, temas/fonte/DPI simulados e quatro tamanhos; histórico privado sem
  recálculo, capturas normais e mensagem de QA inspecionadas. Busca inalterada.
- [ ] CI da ponta e instalação serial depois de176.
- [ ] Windows físico: quebra do resumo/erro e alcance pela rolagem da página.

Doctor20/1opcional/0 reaproveitado da mesma base/ambiente com bootstrap idêntico.
Sem alteração de instalador ou qualificação financeira/nativa/operacional.

## G19.5 — notas/fotografia no gráfico (2026-10-22)

- [x] Linux/Wine: caminho Common/Files e percurso MT5→banco→reabertura, Unicode/múltiplas linhas e fotografia imutável.
- [x] Descoberta do Common via APPDATA com paths.is_windows simulado, além de prefixo Wine.
- [x] Suíte completa App2911/52/3,27 dirigidos e doctor16/2/0 no perfil isolado.
- [ ] Windows físico: cursor de pena, foco/edição/atalhos, DPI e rename atômico entre MT5 e serviço.
- [ ] Distribuição final App/HADOUKEN/Safety e instalação na prévia do proprietário.

## G19.6 — ativo de coloração (2026-10-22)

- [x] Descoberta/fingerprint e caminho MQL5/Indicators nos testes sem terminal.
- [x] Falha do indicador impede compilação do Expert; pacote legado preservado.
- [x] Adaptador manual e ativo compilados no perfil Linux/Wine isolado.
- [x] Ensaio nativo96/144 DPI com App consumindo séries reais.
- [ ] Windows físico: instalação, MetaEditor, coloração, DPI e lápis.

## G19.7 — séries gráficas (2026-10-22)

- [x] Dois ativos no fingerprint/plano; ausência rejeitada antes de instalar.
- [x] Biblioteca G19.6 continua exigindo somente candles.
- [ ] Composição final: adaptador SDK com ambos os ativos no Linux/Wine.
- [ ] Windows físico: compilação, +, Agrupar, janela e remoção.

## G19.8 — composição/ativos Safety (2026-10-22)

- [x] Ativos opcionais: hashes, caminhos contidos, validação antes de copiar, idempotência e ordem de compilação.
- [x] Fingerprint real do painel com ambos os ativos; alteração de Series retira a exceção manual.
- [ ] Instalação/compilação da composição exata no Linux/Wine e atalho existente.
- [ ] Windows físico: instalação, compilação e estudo/agrupamento.

## Monitor PLAY/REPLAY — 2026-10-23

- [x] Linux/Wine6281: replay do painel em1280×800 real e1920×1080 virtual,
  candles/indicadores/avanço; cancelamento e limpeza do runtime próprios.
- [x] Escolha/padrão PLAY, REPLAY obrigatório e monitor retirado em testes;
  caminhos Linux e Windows simulados, sem alterar confirmações financeiras.
- [ ] Windows físico: dois monitores com DPI diferente, monitor negativo, escolha
  do lançamento, maximização e PLAY minimizado/exibido, STOP próprio.
- [ ] Retorno físico do dock após desconexão e escalas distintas em Linux.
- [x] PLAY manual DEMO visível e oculto, telemetria, STOP próprio e carteira
  inalterada sem ordens. Primeira leitura sem API conectada recusou corretamente;
  o fluxo normal de conexão permitiu o QA. Não qualifica gestão/negociação.

[Contrato e limites](sdk/play_replay_monitores.md).

## G19.10 — retorno do dock/área Wine (2026-10-23)

- [x] Duas telas físicas conectadas: eDP-1 1280×800 e DP-1 3456×1458 (áreas
  informadas pelo sistema). PLAY no monitor maior preenche terminal/gráfico;
  REPLAY no handheld desenha/avança e conclui o período. ComposiçãoApp0b2fda3.
- [x] Modos internos preparados no registro privado; original/ponte/host preservados.
- [x] CorridaBadWindow: subprocesso real antes sai1/depois sai0; handler anterior
  preservado para Tk/outras conexões. UI normal sobrevive à retirada do compilador.
- [ ] Windows físico e troca de DPI entre monitores continuam pendentes.
- [ ] Migração automática de uma sessão já aberta por hotplug não é entregue.

Evidência finalSuperATR1.02/dezcurvas/cinzaclaro tem recorte próprioG19.9.

## G19.16 — alvo probabilístico (2026-10-23)

- [x] Linux/Wine6281: painel7.10 e script sintético compilados0/0; mostrador,
  cores/neutralidade, timer independente e recriação:28 verificações nativas.
  Monitor atual1280×800, gráfico1181×666; sem mudar resolução global ou enviar ordens.
- [x] Corpos reais do modelo/feed/política e geometria executados com plataforma
  simulada; fechamento observado não é inferido do bool do executor.
- [ ] Windows físico: MetaEditor, monitor/DPI, Canvas, timer e leitura inline.
- [ ] Tester/servidor DEMO: recibo de parciais/final, netting/hedging, restart e
  concorrência. Gestão padrão desligada; nenhuma qualificação REAL/financeira.

[Contrato](sdk/alvo_probabilistico.md). Não modifica o instalador nem distribui
novo payload Safety; a closure integral precisa acompanhar o Expert.

## AIA-01 — workflows de agentes (2026-10-23)

- [ ] Windows físico: abrir JSONL pelo Python instalado sem Tk; consultar catálogo
  completo, Default/Manual/exportar/reabrir e pacote nativo preservando recibos.
- [ ] Windows físico: PLAY/REPLAY visível em monitores distintos por anchor, aguardar
  callback ready, adicionar/remover somente indicadores próprios, capturar PNG,
  encerrar sessão sem tocar processo externo ou confundir STOP com zeragem.
- [ ] Windows físico: testar EOF/Ctrl-C/retorno após falha e paths com espaços.
- [ ] Tester pausado: controle gráfico pode aguardar callbacks; play/pause do próprio
  Tester ainda não é API disponível. Não automatizar mouse como substituto.

Linux usa os mesmos Session/Consent e paths existentes; simulações de paths Windows
não constituem os aceites físicos acima. Sem alteração de instaladores neste recorte.
[Contrato SDK/CLI](agente.md).

## T20.30-HOST1 — retomada sem disco antigo ou SD (2026-10-24)

[Roteiro e fontes](repasse/NOVO_HOST_20261024.md). Bash e PowerShell em modo plano,
helpers stdlib, clones Git temporários, paths Linux/Windows, recusa de WIP e
restore do snapshot real foram exercitados no Linux existente. Isto não equivale
a reinstalar o SO nem a rodar o instalador num Windows físico.

- [ ] Linux limpo: baixar launcher sem Git/Python, instalar com o gerenciador real,
  conferir `.venv`, toolchain, três fontes/pins, logs e `state/setup.json`.
- [ ] Windows físico: Git/winget, Python oficial, WinLibs/g++, UTF-8 e paths com
  espaços/acentos; repetir preservando configuração e dados existentes.
- [ ] Destino novo: recuperar partes pela conta ChatGPT sem SSD/SD, conferir SHA,
  extrair em pasta nova, comparar48 tabelas/contagens e abrir os históricos.
- [ ] Atalho central e menu no mesmo runtime; UI offline e, após instalar a plataforma,
  nova conexão DEMO/PLAY/REPLAY com monitor/DPI e gates próprios.
- [ ] Falha/interrupção: preservar recibo e WIP, não deixar `development_ready`
  após falha, não restaurar tokens/PIDs/locks e não apagar recursos alheios.

Compilação, pacotes instalados ou clone restaurado não qualificam perfis bloqueados,
operação REAL, fonte histórica ou as pendências funcionais do Laboratório.


## BD16-WINDOWS — janelas referenciais (2026-10-29)

O recorte foi exercitado pelo launcher Linux em QA sintética isolada. Não altera
instaladores, pins ou dependências. [Contrato](sdk/api.md#janelas-móveis-de-pares-congelados-bd16-windows).

- [ ] Windows físico: abrir Janelas móveis por uma relação salva, conferir gráfico,
  tabela, teclado e rolagem em janela menor e nas escalas de DPI utilizadas.
- [ ] Windows físico: aguardar/cancelar cálculo, alterar parâmetros, salvar a seleção
  e exportar/importar DTO3 em caminho com espaços/acentos; preservar DTO1/2.

CI Windows e simulações não substituem esse percurso no host físico; não há
qualificação financeira ou integração MT5 implícita no teste da interface.


## MEN-01 — identidade de séries de menções (2026-10-29)

- [ ] Windows físico: conferir contrato/registro e consulta CLI das três versões
  em base própria, com paths/labels Unicode; zero/ausência e revisões tardias
  devem coincidir com o SDK. Sem mudança de instaladores/GUI neste incremento.

CI Windows não equivale a esse percurso físico; fonte/classificador reais
permanecem nos incrementos MEN-03/04.


## MEN-02 — ingestão de lotes (2026-10-29)

- [ ] Windows físico: JSON UTF-8/BOM e paths com espaços/acentos, ingestão repetida,
  cancelamento por Ctrl-C e retorno sem duplicar Q/P/N/cursor; ler pelo SDK/CLI.

Os testes multiplataforma do CI e o launcher Linux não substituem esse aceite.
Nenhuma alteração de instalador ou dependência neste incremento.


## MEN-03 — API pública limitada (2026-10-29)

- [ ] Windows físico: captura pelo launcher/CLI, TLS/proxy público configurado,
  timeout/retentativa/429 e Ctrl-C; conferir retomada do mesmo roster e cursor.
- [ ] Windows físico: encerramento abrupto de processo próprio, prova de morte
  e reconciliação sem assumir morte por idade ou tocar processo externo.

Transporte injetado no CI não certifica rede pública nesse host físico.


MEN-04 — pendência Windows físico: instalar VADER/langdetect pelo instalador
oficial, conferir identidade local/recibo (diferença de bytes exige nova
qualificação), capturar Q com modelo declarado, classificar lote explícito,
conferir P/N tardios, abstenções e leitura sem inferência. Linux foi exercitado
pelo launcher com QA10/3/2, SIGINT, retomada e fonte pública; não qualifica MT5,
Wine, hardware Windows nem precisão geral do corpus/idioma português.
