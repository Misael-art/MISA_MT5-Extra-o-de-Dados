# Continuidade: harmonização de 2026-10-14

## Retomada T18.8 — 2026-10-15

A apresentação local da T18.8 foi implementada. A aba Laboratório tem as visões Matriz (Indicador × indicador e Indicador × alvo),
Linha do tempo, Ganhos e perdas, Convergências e Candidatos. As subvisões usam a fixture do componente e estão
identificadas como sintéticas; veja as capturas em [docs/img/relations/T18.8](img/relations/T18.8/).

A revisão com Analytics confirmou que T18.1 (DTO/fixture versionada) e T18.6 (serviço de consulta) ainda não estão
presentes nesta base. Não integrar ao ledger F16 nem inferir estados até receber esse contrato. A UI não recalcula
estatísticas, não altera Experts, presets ou ordens, e não oferece ação operacional para candidatos. O layout reusa
tema, tokens e componentes T17; subvisões ficam roláveis em janelas menores e a linha temporal suaviza só a exibição
entre pontos recebidos, preservando marcadores e lacunas.

Validação reportada na worktree original da T18.8 (que também continha branches locais de Analytics): suíte completa
**900 aprovados, 10 ignorados** em 666,12 s; `doctor` **16 OK, 2 avisos, 0 falhas**
(`pandas-ta` opcional ausente e ponte MT5 desativada); `git diff --check` limpo. Capturas foram geradas em Linux/Xvfb
nos temas claro e escuro, em 1280×800, 1920×1080, 820×800 e escala Tk simulada de 150%. Windows real não foi executado.
Na revisão posterior, as faixas da linha do tempo passaram a usar tons neutros para reservar as cores semânticas aos
marcadores; legenda, seletor de ranking e avisos empilham/quebram texto em janelas estreitas. As quatro capturas da
linha do tempo foram renovadas depois desses ajustes.

Revisão da cadeia isolada para publicação (master + T17.1–T17.5 + T18.0 + T18.8): instalação Linux com
`./install.sh --yes --no-mt5` e `requirements-dev.txt`; primeira suíte completa **835 aprovados, 9 ignorados,
2 falhas** em 616,75 s (`test_settings_restore_the_arrangement_only_with_the_terminal_closed` e
`test_no_internet_is_said_plainly_with_what_to_do`). Ambos passaram quando repetidos isoladamente. Segunda suíte
completa: **836 aprovados, 9 ignorados, 1 falha** em 594,69 s; repetiu apenas
`test_settings_restore_the_arrangement_only_with_the_terminal_closed`, também aprovado isoladamente. A espera
assíncrona deste teste já tinha sido registrada como intermitente durante T17.3; nenhum teste foi enfraquecido nem
alterado para esconder o resultado. `doctor`: **16 OK, 2 avisos, 0 falhas** (`pandas-ta` opcional ausente e ponte
MT5 desativada). Os testes focados da apresentação T18.8: **15 aprovados**. `git diff --check` passou; nenhum script
shell mudou. Windows real não foi executado.

## Nova frente documental F18 — 2026-10-15

Pedido do dono para relações entre indicadores, preço futuro, ganhos/perdas e aderência temporal dos Experts.
T18.0 registra a [especificação](propostas/RELACOES_INDICADORES_SETUP.md), a [matriz específica da F18](analytics/MATRIZ_REQUISITOS_F18.md),
as tarefas no plano/roadmap e os prompts separados de [Analytics](analytics/PROMPT_F18_ANALYTICS.md) e [UI](analytics/PROMPT_F18_UI.md).
T18.1–T18.9 estavam inicialmente planejadas; a apresentação T18.8 foi adicionada depois. T18.1 (DTO/fixture versionada)
e T18.6 (serviço de consulta) ainda não estão nesta base, então a tela usa uma fixture sintética e identificada como provisória.
Pesquisa/serviço local independem de MQL5; aplicação operacional mantém dependências e autorizações próprias de T13.7.
Nenhum cálculo analítico, preset, Expert ou ordem foi alterado pela apresentação. Cada implementação segue uma tarefa por PR.

Verificação do registro: instalador sem MT5 e dependências dev executados; 5 testes focados de documentação/integração
aprovados; 11 novos links locais conferidos; `git diff --check` sem erros. Doctor do ambiente existente:
16 OK, 2 avisos (pandas-ta opcional ausente e ponte desligada), 0 falhas.
A primeira linha de base sob Xvfb foi interrompida por falta de espaço no tmpfs `/tmp` (Errno 28).
A repetição em diretório exclusivo no disco terminou com 828 aprovados e 47 ignorados em 402,66 s; o Xvfb encerrou
com erro de limpeza (`kill: No such process`, saída 1), portanto **não é validação integral da UI**.
Janela principal repetida isoladamente: 34 aprovados em 154,36 s. Não se somam essas execuções como suíte única.
Nenhum teste foi alterado/pulado por esta tarefa; não foram apagados temporários de outros agentes.
Não houve ShellCheck porque nenhum script shell mudou. Windows e MT5 real não foram validados nesta entrega.

**Leia isto antes de retomar qualquer tarefa.** Em 2026-10-14 os agentes pararam por limite de crédito com várias
frentes abertas em branches locais. A pedido do dono do projeto, todas foram integradas ao `master` numa única
harmonização (branch `integracao/harmonizacao-2026-10-14`), testadas juntas e registradas aqui. O estado de cada
tarefa continua no painel de [PLANO_DE_TRABALHO.md](PLANO_DE_TRABALHO.md).

## O que entrou no master

| Frente | Branch de origem | Commits | O que traz |
|---|---|---|---|
| T12.1 (registros) | `tarefa/T12.1-safety-ampliada` | `d7f0e5f`, `bf55ad8` | Evidências da T12.1 e a divergência de dependências T12.1 ↔ T12.3 (só docs) |
| T12.6a | `tarefa/T12.6a-painel-v7` | `013c723` | Validação do painel v7 concluída no Linux (só docs: plano e ROADMAP) |
| T10.11 | `tarefa/T10.11-preparacao-market` | `ccd51b2`, `b2dc33c` | Pré-checagem e pacote local do Market (`robots/market.py`, comandos no `cli.py`, testes) |
| F-03 / T12.3 | `tarefa/F-03-exposicao` (o mesmo commit está em `codex/t12-3-orcamento-global`) | `db6ca9f` | Exposição consolidada por ativo e grupo em `robots/portfolio.py`, `mt5x-safety` 1.4.0, chave nova de config, docs do SDK |
| Fase 15 (T15 a T15.9) | `tarefa/T15-gestao-recursos` → `T15.7` → `T15.8` → `T15.9-limites-retencao` (PRs #51 a #54) | 6 commits | Aba "Recursos", `mt5x resources`, arquivo reversível, cópias de segurança, retenção, limites que bloqueiam a gravação, automação opt-in. Plano: [propostas/GESTAO_RECURSOS.md](propostas/GESTAO_RECURSOS.md) |

Ordem das mesclagens: T12.1 → T12.6a → T10.11 → F-03 → Fase 15. Cada uma é um commit de merge próprio
("merge: ... (harmonização 2026-10-14)"), então dá para ver ou reverter uma frente sem mexer nas outras.

## Conflitos e como foram resolvidos

Só houve conflito em `docs/PLANO_DE_TRABALHO.md` (F-03 × T12.1/T12.6a). Código não conflitou.

1. **Painel de status:** ficou **T12.6a ✅ concluída** (da branch T12.6a) e **T12.3 ⏳ em implementação autorizada**
   (da F-03). As duas eram mudanças de linhas diferentes da mesma tabela.
2. **Seção da T12.1:** os dois textos foram mantidos. Primeiro vêm as evidências da T12.1; depois, como "Atualização da
   frente F-03", a `mt5x-safety` 1.4.0 e a tabela de paridade dos seis robôs; por fim, a divergência de dependências.
   "Cinco robôs" virou "seis robôs", como na F-03, que é a mais recente.
3. **Decisão em aberto, não tomada na harmonização:** a T12.1 registrou um ciclo de dependências (T12.1 precisa da
   T12.3, que depende da T12.2, que depende da T12.1). A F-03 já trata a T12.3 como "em implementação autorizada".
   **Nada foi reordenado.** A ordem formal precisa ser confirmada pelo dono do projeto (nota no fim da seção T12.1).

## Correções feitas durante a harmonização (pelo CI do PR #55)

- `resources/guard.py` (T15.9): o cache do guarda começava em 0 no relógio `time.monotonic()`, que conta desde o boot.
  Numa máquina recém-ligada a primeira checagem não rodava. Agora a primeira chamada sempre mede (teste novo).
- `tests/test_ui_main_window.py`: o fixture esperava fixos 0,6 s pela leitura da base em segundo plano; no runner do
  Windows não bastava (`test_backtest_from_the_robot_card`). Agora espera a base carregar, até 10 s. Nenhuma
  asserção mudou e nenhum teste foi pulado.

## O que NÃO entrou, e por quê

- `claude/adoring-hopper-qdtf34` (PR #1, commit `ce83918`): era um `docs/continuidade.md` antigo, da época da T8.3,
  que mandava fazer checkout daquela branch. Mesclá-lo apontaria os agentes para um estado velho. Este arquivo o
  substitui; o PR #1 foi fechado com essa explicação.

## Verificação feita na base integrada

- `pytest`: **701 aprovados, 8 pulados** (antes da harmonização o master tinha 648).
- Importação da interface e dos módulos novos; `mt5x --help` lista `portfolio`, `resources` e `robots` (com `market`).
- Não foi feito: MT5 real, Windows, PostgreSQL ou TimescaleDB reais. Seguem as pendências já registradas em cada tarefa
  e no checklist de [validacao_instaladores.md](validacao_instaladores.md).

## Como cada agente retoma

1. `git fetch origin && git switch master && git pull`.
2. **Não continue numa branch antiga sem antes trazer o master**: `git switch <sua-branch> && git merge origin/master`.
   As branches listadas acima já estão inteiras no master; para trabalho novo, crie uma branch nova a partir dele.
3. Rode `.venv/bin/python -m pytest -q` (esperado: 701 aprovados, mais os testes que você acrescentar) e o `doctor`.
4. Regras novas que valem para todos (detalhes em [propostas/GESTAO_RECURSOS.md](propostas/GESTAO_RECURSOS.md)):
   - O menu Exibir seleciona as abas pelo widget, não pelo índice: a aba "Recursos" fica entre "Robôs" e "Atividade".
   - O extrator tem `write_guard`: o caminho que grava barras precisa consultá-lo antes de gravar.
   - Para tirar dados de usuário da base, use `resources.actions.archive_tables` ou `trim_tables` (arquivo
     reversível), nunca DROP ou DELETE direto.
   - "Benchmark positivo" = algum backtest com lucro no ativo, em qualquer robô, configuração ou data. Esses ativos nunca
     são candidatos à limpeza.
   - Tabelas internas novas, todas aditivas: `_resource_snapshots` e `_resource_actions`.
5. Ao terminar a tarefa, atualize o painel do PLANO_DE_TRABALHO.md e, se mudar o estado geral, este arquivo.

## Retomada após a harmonização — T13.1

- Branch local atual: `tarefa/T13.1-calendario-sessoes`, criada a partir de `master` em `2226bf5`.
- A T13.1 implementa calendário observado em M1, captura de metadados estáveis de `symbol_info`, barras derivadas
  versionadas e o modo `mt5x resample --session-aware`. As regras e limitações estão em
  [T13.1_CALENDARIO_OBSERVADO.md](analytics/T13.1_CALENDARIO_OBSERVADO.md).
- Verificação: testes focados `.venv/bin/python -m pytest -q tests/test_analytics_calendar.py tests/test_resample.py`
  — **20 aprovados**; `doctor` — 16 OK, 2 avisos, 0 falhas. Duas repetições integrais ficaram inconclusivas: a
  primeira terminou aos 79% com 413 aprovados, 8 pulados, 85 falhas e 98 erros após `OSError: [Errno 28] No space
  left on device` no tmpfs; a segunda chegou à bateria de UI/conector, emitiu marcadores `E` e encerrou com status
  143 após cerca de 32 minutos, sem resumo final. Os temporários próprios foram removidos; o tmpfs tinha 604 MB
  livres ao fim da segunda execução. A base EURUSD foi lida sem escrita e sem conectar ao MT5. O modo legado
  `resample` não mudou.
- A branch é local; não houve push nem PR. A T13.1 fica pronta para revisão, com a regressão global inconclusiva;
  a T13.2 continua sendo a próxima tarefa da Fase 13 após integrar esta dependência.
- Worktree `../MISA_MT5-analytics` permanece ativo e não foi removido; na última conferência estava na branch
  `tarefa/T16.11b-execucao-insights`.

## Harmonização 2 (2026-10-14): Fase 16 e T13.1 no master

Branch `integracao/fase16-t13.1`, mesclada no `master` (veja o PR dela). Entraram:

| Frente | O que traz | Registro |
|---|---|---|
| Fase 16 — Laboratório (T16.0 a T16.12; PRs #56 a #70, fechados como substituídos) | Aba "Laboratório", livro de operações `_trade_ledger`, calendário econômico de várias fontes, enriquecimento, re-simulação de execução, insights validados, `mt5x trades` | [propostas/ANALYTICS_OPERACOES.md](propostas/ANALYTICS_OPERACOES.md), seção 10 |
| T13.1 — calendário de sessões (branch `tarefa/T13.1-calendario-sessoes`, commit `05506ee`) | Calendário observado em M1, barras derivadas versionadas, `mt5x resample --session-aware` | [analytics/T13.1_CALENDARIO_OBSERVADO.md](analytics/T13.1_CALENDARIO_OBSERVADO.md) |

**Ajuste feito na T13.1 durante a harmonização:** as tabelas viraram `_market_calendar` e `_bars_derived`. Sem o `_`,
o `DatabaseManager` as trataria como séries de ativo: `bars_derived` tem `close` e entraria no inventário, e o
`MIN(time)` do inventário quebraria ("Minha base", Laboratório e Recursos fora do ar). Nenhuma base real tinha as
tabelas. Teste de regressão: `tests/test_harmonizacao_frentes.py`.

**Sobre a suíte "inconclusiva" da T13.1:** as duas causas eram do ambiente, não do código:
1. o tmpfs `/tmp` estava com 3,8 GB de pastas antigas do pytest (`/tmp/pytest-of-misael/pytest-*`); foram apagadas;
2. o display `:0` fica inacessível para o shell em alguns momentos, e cada teste de interface espera minutos pelo X
   antes de desistir (os `E` e o status 143 depois de 32 min). **Rode a suíte com `xvfb-run -a`** (cerca de 6 min).

**Regra nova para todos:** tabela interna (que não é série de ativo) começa com `_`. O teste de regressão acima
protege as das Fases 13, 15 e 16.

**Próximas tarefas:** T13.2 (Fase 13) pode partir do `master`. Pendências da Fase 16 na seção 10 do plano dela
(T16.2b no HADOUKEN, exportação do calendário com o MT5 conectado, Windows real).

**Queda "Illegal instruction"/"Segmentation fault" na suíte (era a mesma "regressão inconclusiva" da T13.1):** o coletor de
lixo do Python rodava dentro de threads de segundo plano e destruía variáveis e imagens do Tk, que só aceita a thread
principal. Corrigido: a janela desliga a coleta automática (`ui_manager.setup_ui`) e coleta na thread principal a cada
~2 s (`_pump_ui_queue`); os testes religam e coletam depois de cada teste (`tests/conftest.py`). Com isso, a suíte
completa: **802 aprovados, 9 pulados, ~7,5 min** com `xvfb-run -a`.

## Retomada da T13.2 — 2026-10-14

- Implementação na branch `codex/t13.2-feature-store`, ainda não integrada: registry imutável por versão, valores
  append-only com disponibilidade UTC e revisão, consulta *as-of*, propagação seletiva de dependências e CLI
  `mt5x analytics features list/show/values/affected`. Detalhes em
  [T13.2_FEATURE_STORE.md](analytics/T13.2_FEATURE_STORE.md).
- Decisão da primeira fase: SQLite, com SQLAlchemy e DDL portátil. A base local lida somente em metadados tinha
  329,6 MB e 23 tabelas. A exportação do retrato para o MT5 permanece para a T13.5. Nenhum valor de característica
  foi calculado ou gravado na base real.
- Verificação: testes focados 20 aprovados, 2 pulados; suíte completa **809 aprovados, 10 pulados em 521,99 s**;
  `doctor` 16 OK, 2 avisos, 0 falhas. Os dois testes PostgreSQL da frente foram pulados localmente porque
  `MT5X_TEST_PG_URL` não está configurada; um teste de round-trip *as-of* foi adicionado ao job PostgreSQL do CI.
  Windows real, PostgreSQL local e MT5 real não foram testados.

## Retomada da Fase 17 — 2026-10-14

- **T17.1 concluída em 2026-10-15** no Linux; validação Windows continua pendente.
- A Fase 17 foi criada para a modernização visual do mt5x, separada das frentes de Analytics. O plano completo está em
  [propostas/INTERFACE_VISUAL.md](propostas/INTERFACE_VISUAL.md); T17.1 a T17.5 têm entregas independentes.
- T17.1 foi concluída no Linux na worktree `../MISA_MT5-Extra-o-de-Dados-T17.1`, branch `tarefa/T17.1-sistema-visual`, baseada no
  commit `e8657fe`. A worktree original da T13.3 contém alterações Analytics não commitadas e foi preservada.
- Escopo T17.1: `ui/visual_tokens.py` centraliza paletas, escala, tipografia, espaçamento, dimensões e contraste;
  `ui/theme.py` agora escolhe uma família sans instalada, aplica tamanhos mínimos de 11/10 pt, suporta ajuste de
  leitura sem duplicar o DPI e usa tokens de espaço nas tabelas, controles e abas. `tests/test_ui_theme.py` cobre
  contraste, fallback, tamanhos e notificação de redesenho. Nenhum motor, cálculo, schema ou conexão MT5 foi alterado.
- Linha de base da T17.1: `install.sh --yes --no-mt5`, `pip install -r requirements-dev.txt` e suíte antes das
  alterações (**809 aprovados, 10 pulados em 492,63 s**, com `xvfb-run -a`). A linha de base realista usa ledger e
  barras sintéticos e está em `docs/img/interface-visual/T17.1/mt5x-before-light.png` e
  `mt5x-before-dark.png`; não houve conexão ao MT5. Após a mudança, capturas com a mesma fixture estão em
  `mt5x-after-light.png` e `mt5x-after-dark.png`.
- No host Linux, a família Tk disponível é Lato; Inter/Noto Sans não aparecem entre as famílias anunciadas ao Tk.
  A escala física medida no Xvfb foi ~100 DPI e `tk scaling` 1,389; o Tk aplica essa escala aos pontos. Contraste
  textual inicial medido: `muted` 3,15:1 e `accent` 3,87:1 no fundo claro. A nova paleta é testada a pelo menos
  4,5:1 para os papéis de texto e 3:1 para séries.
- Verificação posterior: testes focados de tema/janela/Laboratório **43 aprovados em 250,42 s**; teste focado de
  tokens/tema **5 aprovados em 0,43 s**; suíte completa final **814 aprovados, 10 pulados em 718,50 s** usando
  `TMPDIR` e `--basetemp` dedicados após um primeiro run interrompido por `Errno 28` de armazenamento temporário.
  `doctor`: **16 OK, 2 avisos, 0 falhas** (opcional
  `pandas-ta` ausente e ponte desligada por `--no-mt5`). `git diff --check` passou.
- Capturas de referência do usuário não estavam no diretório de anexos. O pedido de reenvio está pendente; usar as
  capturas reais quando disponíveis na T17.3. A validação em Windows com DPI 125%/150% está pendente em
  [validacao_instaladores.md](validacao_instaladores.md); nenhum resultado Windows foi declarado.
- T17.2 concluída em 2026-10-15 na worktree `../MISA_MT5-Extra-o-de-Dados-T17.2`, branch
  `tarefa/T17.2-componentes-apresentacao`, baseada no commit `a1ad9fe`. `ui/components.py` agora reúne cabeçalho,
  faixa de filtros, painel/cartão de métrica, badge semântico, aviso, painel de estado, gráfico, tabela, progresso,
  resumo comparativo e painel recolhível de detalhes. `ui/theme.py` configura estilos de status e fallback textual
  para glifos ausentes. As ações visíveis exigem callback; a camada não calcula analytics e as telas não foram
  integradas ainda.
- T17.2: baseline **814 aprovados, 10 pulados em 723,22 s**; testes focados **18 aprovados**; suíte final
  **827 aprovados, 10 pulados em 790,90 s**; `doctor` **16 OK, 2 avisos esperados, 0 falhas**; `git diff --check`
  passou. Avisos: `pandas-ta` opcional ausente e ponte MT5 desligada por `--no-mt5`. Windows real segue pendente.
- Verificação de preservação: a worktree original começou esta frente em `tarefa/T13.3-perfis-regimes` com
  alterações locais Analytics. Na checagem final estava limpa em `tarefa/T16.3b-enriquecimento-temporal`, HEAD
  `880bf18`; o reflog contém o commit `bde3652 feat(T13.3): add profile and regime features` antes da troca de
  branch. Nenhum comando desta tarefa alterou o worktree original.
- T17.3 concluída no Linux; próxima tarefa: T17.4 (demais abas), depois T17.5
  (validação visual integrada).

## Retomada T17.3 — 2026-10-15

- T17.3 iniciada na worktree `../MISA_MT5-Extra-o-de-Dados-T17.3`, branch `tarefa/T17.3-laboratorio-graficos`,
  baseada no commit `0d81adb` da T17.2. Instalação Linux com `./install.sh --yes --no-mt5` e dependências de
  desenvolvimento concluídas; `doctor` do instalador informou 16 OK, 2 avisos esperados, 0 falhas.
- Baseline antes das mudanças: **826 aprovados, 10 pulados e 1 falha em 660,14 s** no teste
  `test_settings_restore_the_arrangement_only_with_the_terminal_closed`; a mensagem de conclusão não chegou em
  0,6 s. Repetição isolada: **1 aprovado em 6,43 s**. Não alteramos a tela de Configurações, fora do escopo da T17.3.
- As cinco capturas originais do pedido não estavam disponíveis na worktree nem foram reenviadas; a comparação da
  T17.3 usa fixture sintética e fica registrada para incorporar as referências reais quando disponíveis.

## Conclusão da T17.3 — 2026-10-15

- Na worktree `../MISA_MT5-Extra-o-de-Dados-T17.3`, branch `tarefa/T17.3-laboratorio-graficos`, baseada em `0d81adb`,
  a visão geral do Laboratório passou a usar uma tabela rolável de 24 horários com valor, unidade, `n` e estado de
  amostra explícitos. Células sem operações mostram `— · n=0`; o asterisco indica operações positivas abaixo do mínimo.
- Horários, microestrutura e comparação agora oferecem subtelas para ler um gráfico por vez. Gráficos de barras e
  heatmaps mostram unidade, contagem e qualidade; heatmaps distinguem ausência de dados, e a correlação mantém a
  escala fixa −1…1 com legenda.
- Capturas em 1280×800, com MT5 desconectado, estão em `docs/img/interface-visual/T17.3/`: visão geral antes/depois
  em claro e escuro e análise temporal antes/depois em claro. Foram inspecionadas também as vistas Curvas, Calendário,
  Microestrutura indisponível e Correlação com dois robôs.
- Testes focados: **27 aprovados em 16,89 s**. Suíte completa: **831 aprovados, 10 pulados, 1 falha em 560,91 s**;
  a falha foi a mesma intermitência fora de escopo observada na baseline, em
  `test_settings_restore_the_arrangement_only_with_the_terminal_closed`; repetição isolada: **1 aprovado em 5,84 s**.
  `doctor`: **16 OK, 2 avisos esperados, 0 falhas**; `git diff --check` passou.
- As cinco capturas originais do pedido não foram reenviadas. Nenhum MT5 real foi conectado. Windows e DPI alto ficam
  pendentes para a T17.5/checklist; não se declara validação Windows.
- Próxima tarefa: T17.4, aplicação progressiva às abas restantes.

## Conclusão da T17.4 — 2026-10-15

- T17.4 concluída no Linux na worktree `../MISA_MT5-Extra-o-de-Dados-T17.4`, branch
  `tarefa/T17.4-aplicacao-abas`, baseada no commit T17.3 `ccde9ce`. Instalação com
  `./install.sh --yes --no-mt5` e dependências de desenvolvimento concluídas antes do trabalho; baseline: **832 aprovados,
  10 pulados em 618,76 s**.
- As abas Ativos, Minha base, Análise, Robôs, Recursos e Atividade agora reutilizam os componentes visuais existentes
  e reorganizam filtros, métricas, tabelas e detalhes para janelas menores. Ativos e Robôs mantêm os controles inferiores
  alcançáveis por rolagem; a tabela de parâmetros de robô pode ser rolada horizontalmente. Nenhum cálculo, contrato ou
  regra operacional foi alterado.
- Capturas antes/depois das seis telas, com MT5 desconectado e fixture sintética, estão em
  `docs/img/interface-visual/T17.4/`: antes/depois em claro a 1280×800, depois em escuro a 1280×800 e depois em claro a
  900×850. As vistas inferiores de Ativos e Robôs também foram capturadas a 900×620. As cinco imagens de referência
  originais citadas no pedido não estavam disponíveis e seguem pendentes.
- Verificação: teste responsivo focado **1 aprovado**; suíte completa final **833 aprovados, 10 pulados em 653,73 s**;
  `doctor` **16 OK, 2 avisos esperados, 0 falhas** (`pandas-ta` opcional ausente e ponte desativada por `--no-mt5`);
  `py_compile` e `git diff --check` passaram. A primeira suíte completa parou porque o `tmpfs` de sessão do pytest
  encheu; a repetição usou diretórios temporários em `/var/tmp` e terminou normalmente.
- Nenhum MT5 real foi conectado. A validação em Windows, DPI alto e 1920×1080 segue pendente para T17.5/checklist;
  não se declara validação Windows. Próxima tarefa: T17.5, validação visual integrada e documentação final da fase.

## Conclusão da T17.5 — 2026-10-15

- T17.5 concluída no Linux na worktree `../MISA_MT5-Extra-o-de-Dados-T17.5`, branch
  `tarefa/T17.5-validacao-visual`, baseada no commit T17.4 `c3f625b`. A instalação e as dependências de desenvolvimento
  foram preparadas antes da baseline; esta entrega fecha a documentação e as evidências sem alteração de cálculo ou API.
- As seis telas foram capturadas em claro/escuro a 1920×1080, com Xvfb em 96 DPI (`tk scaling=1,333`) e 144 DPI
  (`tk scaling=1,998`), em `docs/img/interface-visual/T17.5/`. Há capturas adicionais de Análise com 10 barras e de
  Ativos com cotação sintética grande. Os pares antes/depois 1280×800 e as capturas narrow 900×850/900×620 estão em
  `docs/img/interface-visual/T17.4/`.
- Fixture local sem MT5 real: 46 símbolos, 600 barras M1, 480 M15 e uma série curta de 10 barras. A Análise recusou o
  ativo sem dados e abriu as séries existentes. `Ctrl+F`, busca, Tab e resize foram exercitados; em 900×620 Ativos
  empilha os painéis e expõe rolagem, e em 1920×1080 volta ao leiaute lado a lado. O display Xvfb não oferece estado
  de maximização de um gerenciador real. As imagens originais do pedido continuam indisponíveis.
- Na baseline direta do desktop: **831 aprovados, 10 pulados, 2 falhas intermitentes** (teste de desempenho de agregação
  em 1,59 s frente ao limite de 1,5 s; teste responsivo sem conseguir reduzir a janela desktop de 1280 px). As duas
  repetições isoladas passaram, com a responsiva em Xvfb. Suíte final completa em Xvfb 1280×800: **833 aprovados,
  10 pulados em 674,22 s**. `doctor`: **16 OK, 2 avisos esperados, 0 falhas** (`pandas-ta` opcional ausente e ponte
  MT5 desligada); `git diff --check` passou.
- A Fase 17 está concluída no Linux. Windows real, DPI de 125%/150%, maximização pelo sistema e glifos/fontes do Tk
  continuam na pendência T17.1–T17.5 de `docs/validacao_instaladores.md`; não se declara validação Windows.

## T17.7 — referência temporal e população (2026-10-16)

- Implementação em branch `codex/t17-7-contexto-temporal`, PR #81 draft, base direta
  master. Sem merge. PRs #79/#80 permanecem separados e sem autorização de integração.
- Horários identifica entrada/saída e população; seleção mista explicita saídas
  excluídas da matriz sem alterar os KPIs. Visões temporais identificam referência
  mista. Sem mudança de agregação, valores, risco ou ordens.
- Baseline 837 aprovados/9 ignorados em 726,65 s; primeira suíte posterior
  838 aprovados/9 ignorados em 583,13 s; testes focados finais 6 aprovados.
  Revalidação no commit 061f751: **838 aprovados, 9 ignorados em 705,84 s**, saída 0.
- Dados reais em cópia consistente: 263 operações parciais, temas claro/escuro,
  1280×800/1920×1080, exportação UI de 12405 barras conferida; encerramento saída 0.
  Doctor principal aprovado; worktree deliberadamente offline acusa caminho MT5 vazio.
  Nenhum shell alterado; Windows real pendente.
- DPI144 desde a criação do Tk revelou cortes no cabeçalho/filtros e matriz sem
  altura em 1280×800: problema crítico ainda aberto, T17.9 proposta. T17.8 proposta
  remove suavização existente em Relações. WIP T17.6 preservada byte a byte.
- Auditoria e evidências em [AUDITORIA_UI_20261016.md](propostas/AUDITORIA_UI_20261016.md).
  Aceite visual integral e campanha PF ≥ 1,9 permanecem pendentes.

## T17.9 — responsividade do Laboratório (2026-10-16)

- Branch `codex/t17-9-laboratorio-responsivo`, PR #82 draft, empilhado sobre #81.
  Código em 972658c. Sem merge. Contexto de seleção fixo, cabeçalho/filtros
  reorganizados, resultados roláveis com área útil, colunas medidas pela fonte
  e rolagem horizontal; componente reutilizado de Relações, sem alteração analítica.
- Baseline 838 aprovados/9 ignorados em 697,62 s. Focados 31 aprovados em 44,07 s.
  Suíte completa no commit 972658c: **840 aprovados, 9 ignorados em 733,11 s**, saída 0.
- Inspeção real iterativa com 263 operações parciais: DPI144 desde o início do Tk,
  1280×800/1920×1080, claro/escuro e rolagem; nova rodada em DPI96 incluiu 900×620.
  Cortes de aviso e valores encontrados na primeira rodada foram corrigidos na
  segunda. Capturas em `docs/img/interface-visual/T17.9/`.
- Doctor isolado 15 OK/2 avisos/1 falha por caminho MT5 deliberadamente vazio;
  principal validado separadamente. Windows real pendente, nenhum shell alterado.
- Pedido posterior do dono: atenção discreta a positivos/negativos/ausências em
  Horários e Minha base. T17.12 planejada reutilizando seletivamente T17.6
  preservada, sem importar suavização. Pré-requisito T17.12a preserva métricas
  ausentes e distingue zero/perda/sem resultado na apresentação compartilhada.
  Aceite visual global e campanha PF ≥ 1,9 continuam pendentes.

### Revalidação CI da T17.9 no Windows

O job Windows de uma branch descendente retornou duas colunas após o teste pedir
1920 px; a asserção exigia quatro. O teste foi corrigido na tarefa de origem
para verificar todos os controles mapeados e inteiros dentro dos limites reais
do painel, mantendo a checagem dos valores. A causa precisa da diferença de
fontes/DPI/tamanho efetivo no runner não foi medida. Não se exige um número fixo
de colunas quando o conteúdo precisa refluír. Nenhuma asserção de acesso, valor,
seleção ou rolagem foi retirada. Windows manual continua pendente.

## T17.12a — disponibilidade e apresentação dos resultados (2026-10-16)

- PR #83 draft, branch `codex/t17-12a-disponibilidade-resultados`, base #82.
  `asset_table` preserva métricas ausentes/não finitas como indisponíveis, mantém
  o run mais recente e mostra zero e percentuais negativos explicitamente.
  Nenhuma fórmula financeira, banco, risco ou ordem muda.
- Baseline: 840 aprovados/9 ignorados em 745,70 s. Focados: 14 aprovados em 1,87 s.
  Primeira suíte posterior: 847 aprovados/1 falha/9 ignorados em 553,43 s: feedback
  de conexão ainda em andamento após espera fixa de 0,8 s no teste de falta de
  internet, já registrado como intermitente. Repetição isolada passou em 5,64 s;
  repetição completa no mesmo a886351: **848 aprovados, 9 ignorados em 556,06 s**,
  saída 0. Nenhum teste foi removido, pulado ou enfraquecido.
- 271 runs completos/13 séries reais mantêm células e retornos iguais; UI Minha
  base inspecionada, com zero/perda existentes agora visíveis. Capturas
  `docs/img/interface-visual/T17.12a/`. Doctor offline: 15 OK/2 avisos/1 falha
  por caminho MT5 vazio deliberado; principal validado separadamente.
- T17.12 aplica a hierarquia discreta de atenção pedida pelo dono. Uma prévia
  privada com dados reais confirmou o estilo; o código visual ainda não foi
  incorporado nesta tarefa. WIP T17.6 preservada. Sem merge; PF pendente do aceite.

## T17.12 — hierarquia discreta de atenção (2026-10-16)

- Branch `codex/t17-12-hierarquia-atencao`, dependente de #83. Horários recebe
  destaque azul suave nos positivos, perdas sóbrias e ausência neutra. Minha base
  usa a mesma semântica, com seleção nativa destacada e qualidade legível.
- Sem mudança de cálculos ou dados: n e asterisco de pouca amostra permanecem;
  traço sem operações remete a n=0 na legenda. Positivo não significa validação.
- MatrixTable reutilizada seletivamente do WIP T17.6, preservado integralmente;
  nenhum gradiente por magnitude ou suavização importado. Teclado e cabeçalho fixo
  validados. 30 testes focados aprovados; suíte completa no 5388ec8: 852 aprovados,
  9 ignorados em 598,30 s, saída 0. PR #84 draft.
- Operação real com cópia consistente da base, sem terminal, em ambos os temas,
  DPI96/144 e resoluções 1280×800/1920×1080; capturas em
  `docs/img/interface-visual/T17.12/`. Doctor offline tem falha deliberada de path
  MT5; Windows visual manual e aceite global pendentes. #83 CI completo verde.
- Nenhum PR foi mesclado. Campanha PF ≥1,9 continua posterior ao aceite visual
  e à contabilidade de custos; nenhum resultado financeiro novo foi produzido.

- Revalidação do host principal após T17.12: doctor 21 OK/1 aviso/0 falhas.
  T17.6 preservada, incluindo 22 arquivos não rastreados e diffs staged/unstaged.

## T17.8 — traço observado em Relações (2026-10-16)

- Branch `codex/t17-8-traco-observado`, base T17.12/#84. Remove interpolação
  cúbica da timeline: agora um ponto por janela recebida, com NaN preservando
  lacunas; traço reto explicitado na legenda. Estados, marcadores, tabela, seleção
  e gestão de callbacks permanecem. Fonte continua fixture sintética declarada,
  sem vínculo com base real/serviço F18; não há resultado financeiro nesta entrega.
- Ambiente próprio instalado e dev. Baseline própria: 851 aprovados/1 falha/9
  ignorados em 653,29 s, teste de conexão com espera fixa de 0,8 s já intermitente;
  repetição isolada passou (1 em 6,04 s). Supervisor terminou 143 antes do filho,
  que escreveu o resumo; exit do filho não capturado. Não chamar baseline verde.
  A base de código foi aprovada separadamente na T17.12: 852/9, saída 0.
- Focados: 11 aprovados em 23,10 s; cenário de lacuna interna/seleção nos dois
  temas repetido após a última asserção: 1 aprovado em 2,85 s. Suíte completa do
  código b9fd90a: 852 aprovados/1 falha/9 ignorados em 652,79 s, saída 1.
  A falha repetiu o mesmo teste de conexão/espera fixa da baseline; não é suíte
  global aprovada. Correção da sincronização exige tarefa separada.
- Aplicação aberta com base vazia de auditoria, terminal desativado, Relações
  explicitamente sintética, ambos os temas, DPI96/144 e 1280×800/1920×1080.
  Capturas em `docs/img/interface-visual/T17.8/`; doctor offline 15 OK/2 avisos/
  1 falha deliberada por MT5.path vazio. Host principal: 21 OK/1 aviso/0 falhas.
- Auditoria encontrou espaço vertical ocioso abaixo da área analítica enquanto
  a timeline exige rolagem interna, especialmente em 1920×1080. Próximo recorte
  de ocupação da altura deve preservar mínimos/rolagem e ser testado separadamente.
- Sem merge, operação em conta ou campanha PF. Revisão/integração pendentes.

## T17.15 — sincronização do teste de conexão (2026-10-16)

- PR #86 draft, branch `codex/t17-15-sincronizacao-conexao`, base #85;
  ambiente próprio preparado e baseline completa da T17.8 registrada com a
  mesma falha repetida de espera fixa. Somente teste/documentação nesta tarefa.
- O cenário de internet/conta/recuperação aguarda `_connecting` terminar com
  prazo de10 s, mantendo todas as asserções e verificando botão disponível.
  Parametrizado com worker imediato e worker de1 s para ultrapassar os0,8 s
  anteriores. Nenhuma mudança de conector, runtime ou negociação.
- Focados: 5 aprovados/32 não selecionados em23,75 s. Suíte completa20765d8
  aprovada: **854 aprovados/9 ignorados em765,21 s, saída0**. Doctor do mesmo runtime
  offline já registrado15 OK/2 avisos/1 falha deliberada de MT5.path vazio.
- Operação adicional de UI sobre cópia real: diálogo15séries×15robôs, limpar/
  selecionar/cancelar sem iniciar; lote histórico EURUSD H1 com6estratégias
  Python e1worker, quatro concluídos/zero falhas/dois pendentes cancelados.
  Resultados negativos preservados, dados marcados como expostos à pesquisa.
  Não é campanha PF19 nem reserva independente. Sem ordem ou terminal.
- T17.14 preparada em worktree próprio, baseline rodando antes de modificar
  layout. T17.16 proposta após seletor H4 mostrar resultado H1 anterior e botão
  cortado no detalhe do robô; sem mudança de cálculos. Nenhum merge realizado.

## T17.14 — altura da área analítica (2026-10-16)

- Branch `codex/t17-14-altura-analitica`, baseT17.15/#86, ambiente próprio
  instalado/dev e baseline854 aprovados/9 ignorados em768,23 s, saída0.
- ScrollableContent preenche altura disponível sem suprimir altura requerida;
  cálculo desconta borda e usa layout estabilizado para textos com quebra de
  linhas. Callback único, cancelado ao destruir o componente. Sem dados/cálculos.
- Rodadas: primeiro teste detectou2px de rolagem artificial e foi corrigido;
  teste de limite passou. Rodada seguinte34 focados aprovados em44,07 s; inspeção
  de resize detectou atraso na altura após wrap, corrigido e coberto por novo
  teste. Três casos de rolagem/wrap/Destroy passaram em0,79 s. Focados finais:
  35 aprovados em52,74 s; suíte40853f2:856 aprovados/9 ignorados
  em713,62 s, saída0. Suíte anterior60d8cc2 foi interrompida143 por
  código supersedido, sem afirmar sucesso.
- Operação antes/depois clara/escura,DPI96/144,1280×800/1920×1080 e ciclo de
  resize: contêiner maior preenche830 dos832px em96DPI (2px são borda); viewport
  interno sobe267→468px. Medidas exatas preservadas após estreitar/alargar,
  gráficos com altura mínima preservada. Capturasdocs/img/interface-visual/T17.14.
  Relações continua explicitamente sintética, sem vínculo à base local vazia.
- Limites independentes: detalhe lateral143px emDPI144(T17.17), seletor/resultado
  e botão de Backtest(T17.16), retornozero emAnálise(T17.13), empates/lacunas
  temporais(T17.10/T17.11). Aceite global, revisão/merge e PF19 pendentes.

- Doctor deste recorte:15 OK/2 avisos/1 falha porMT5.path vazio deliberado
  e ponte desativada; não é diagnóstico verde. Host principal segue diagnóstico
  separado21 OK/1 aviso/0 falhas. PR #87 draft, sem merge.

## T17.17 — detalhe legível em Relações (2026-10-16)

- Branch `codex/t17-17-detalhe-legivel`, baseT17.14/#87. `_split_view` mede
  reserva de texto pela fonte real e mantém proporção das colunas; se não
  couber, empilha e conserva rolagem e mínimos dos gráficos/tabelas. Nenhuma
  mudança de dados/seleção/estados/engine; fonte continua sintética declarada.
- Ambiente próprio instalado/dev; baselinead31f06:856 aprovados/9 ignorados
  em732,12 s, saída0. Focados finais12 aprovados em30,05 s. Suíte completa
  e8bdb11:857 aprovados/9 ignorados em808,56 s, saída0.
  O resultado não substitui a revisão independente nem oCI noSHAfinal.
- Aplicação operada emdois temas,DPI96/144,1920×1080/1280×800/820×800 e
  retorno à janela grande. Em144DPI, detalhe143→441px (mínimo388) em1920;
  em1280e820 empilha1154/774px; gráfico344px preservado. Capturas
  `docs/img/interface-visual/T17.17/`. Nenhum resultado sintético chamado real.
- Próximos problemas:T17.16 contexto/backtestbotão;T17.13 retornozeroemAnálise;
  T17.10/T17.11 semântica deempates/lacunas exige núcleo. Revisão/integração
  ainda pendentes; #79/#80 fora da autorização nova solicitada, nenhuma conta.

- Inspeção adicionalDPI144/1280: PageDown pelo teclado e rolagem levam ao
  detalhe empilhado; seleção OOS atualiza o detalhe. Sessão própria encerrada0.
  Doctoroffline15 OK/2 avisos/1 falha deliberada deMT5.path vazio; hostprincipal
  comdiagnóstico separado21 OK/1 aviso/0 falhas. PR #88 draft, semmerge.
- CI da dependência #86: primeiratentativaWindowstimeout60 s emfixtureSQL
  inalterada; uma reexecução no mesmo8246686 passou, todoschecksverdes. Falha
  inicial preservada nos logs e descrição, sem alterar timeout ou testes.

- CI Windows do 2706c66 revelou duas falhas reais: painel interno recebia
  novamente os 4 px já reservados pelo split. Removido espaçamento duplicado
  de Linha do tempo, Distribuição e Convergência, preservando as exigências de
  largura dos testes. Log inicial preservado; validação da correção em execução.

- Correção de espaçamento: 13 testes de Relações aprovados em58,77 s;
  operação96/144DPI e ciclo1920→1280→820→1920, ambos temas, confirmou
  detalhe acima do mínimo. DPI144:445px(min388) em1920, empilha nas menores;
  DPI96:453px em1920 e292px(min276) em1280. Interfaces encerradas0.
  Capturas de demonstração identificadas SYNTHETIC; suíte completa
  final b35f274:858 aprovados/9 ignorados em1118,60 s, saída0. Windows
  do mesmo commit aprovado em20m47s.

- Primeira repetição local do ajuste:784 aprovados/83 ignorados, saída0;
  não aceita para QA porque74 testes gráficos ficaram sem display após falha
  do Xvfb no roteiro privado. Mantido log da tentativa. Roteiro corrigido
  (-noreset e displayfd aberto), repetido mesmo SHA:858/9, sem exclusões
  gráficas. Os nove ignorados esperados são integrações opcionais separadas
  e são executadas em seus jobs do CI. Nenhum teste alterado para contornar
  a falha do ambiente. Doctor:15 OK/2 avisos/1 falha deliberada MT5.path vazio.

## T17.16 — contexto e controles do Backtest (2026-10-16)

- Branch `codex/t17-16-contexto-backtest`, baseT17.17/#88; ambiente próprio
  instalado/dev, baseline9f40e96:857 aprovados/9 ignorados em823,04 s, saída0.
- Seleção para próximo teste separada do resultado armazenado; após teste desta
  sessão, a proveniência da seleção/base identifica compatibilidade ou resultado
  anterior. Histórico não recebe equivalência de período presumida. Erro não
  apaga resultado/curva, pedido duplicado não inicia, callback de outro robô não
  muda o detalhe atual. Hook existente atualizaAtivos/MinhaBase imediatamente.
- Controles refluem4/2/1colunas eBacktest usaScrollableContent daT17.14.
  Três focados aprovados em21,68 s, incluindo erro/seleção/troca de robô; geometria
  DPI144:1 aprovado em11,82 s. Suíte58cb114:859 aprovados/9 ignorados
  em937,37 s, saída0. PR #89 draft.
- Operação real: cópia privada consistente da base, dois temas,DPI96/144 e
  1280×800/1920×1080; botão inteiro, PageDownacessa resultado. Novo testePython
  EURUSDH1 concluído, texto da seleção atual confirmado e muda paraanterior após
  alterarperíodo. Dados expostos registrados, semOOS/campanhaPF19, nenhuma conta
  outerminal. Evidênciasdocs/img/interface-visual/T17.16 e logsprivados.
- Semmerge; aceitevisualglobal/revisão/integração pendentes.

- DoctorofflineT17.16:15 OK/2 avisos/1 falha deliberadaMT5.pathvazio;
  principal preservada comdiagnóstico separado21 OK/1 aviso/0 falhas.

## T17.13 — retorno de mercado com atenção coerente (2026-10-16)

- Branch `codex/t17-13-retorno-mercado`, base T17.16/#89. Ambiente próprio
  instalado e baseline 859 aprovados/9 ignorados, saída 0, antes do código.
- Retorno positivo recebe azul suave, negativo discreto, zero neutro e ausência
  explícita. Sinais textuais preservam a leitura sem depender da cor. Legenda
  distingue retorno de preço e lucro de robô. Mesmos tokens de T17.12; sem
  mudança em fórmulas, candles ou banco.
- Oito testes focados aprovados em 42,24 s; incluem zero, ausência, NaN/Inf,
  tema, limpeza e contraste mínimo 4,5. Suíte final 65b13d0: 865 aprovados/9 ignorados em1251,92 s, saída0. PR #90 draft.
- Interface operada em cópia consistente da base real EURUSD H1: resumo
  idêntico ao anterior após normalizar somente datas para JSON. Dois temas,
  96/144 DPI, 1280×800/1920×1080; cenários zero/ausência rotulados CONTROLLED.
  Capturas em docs/img/interface-visual/T17.13. Tentativas do roteiro com
  serialização incorreta/rodapé antigo foram retidas nos logs e substituídas
  por novas capturas, sem editar imagens.
- Doctor isolado: 15 OK/2 avisos/1 falha de MT5.path vazio deliberado.
  Nenhuma operação em conta, nenhum merge. Clipping preexistente da Análise
  em 144 DPI/1280 permanece achado separado, sem ampliar este recorte.

## T17.10 — operações zeradas distintas de perdas (2026-10-16)

- Branch `codex/t17-10-resultados-zerados`, base T17.13/#90. Ambiente próprio
  instalado; baseline no88cbe6c:865 aprovados/9 ignorados em1114,07 s, saída0,
  antes do código. Correção de espaçamento da dependência T17.17 reconciliada.
- Reprodução real BBRSI:10.777 operações,4.071 ganhos,6.366 perdas,340 zeradas;
  o gráfico anterior mostrava6.706 perdedoras. Novo `aggregate.outcome_counts`
  classifica o PnL no núcleo (>0/<0/=0/ausente ou inválido), preservando a
  dimensão ausente e a população. A UI renderiza as séries prontas e omite
  categorias sem operações da legenda. Ganhos em azul, perdas discretas com
  trama, zeradas neutras. Não muda PF, expectativa, equity ou ledger/schema.
- Quatorze testes do núcleo aprovados em3,00 s e oito de Laboratório em31,62 s;
  incluem zero, ausentes/não finitos/booleanos, categoria vazia, dimensão ausente,
  métricas preservadas, dois temas e atualização da seleção sem série antiga.
  Suíte completa no85b81ef:875 aprovados/9 ignorados em944,54 s, saída0; PR #91 draft.
- Aplicação real em cópia consistente, ambos temas,96/144 DPI e1280/1920:
  barras reconciliadas com4.071/6.366/340 e soma10.777. Ledger antes/depois
  com hash idêntico; quick_check dos dois bancos OK. Contagens CSV geradas pela API do núcleo e gráfico
  exportado conferidos nos artefatos privados. PageDown e rolagem alcançam a
  base do gráfico em144 DPI; sessões próprias encerradas0. Capturas em
  docs/img/interface-visual/T17.10. Dados existentes expostos, sem novo backtest
  ou campanha financeira; esses agregados não são evidência OOS.
- Doctor isolado:15 OK/2 avisos/1 falha deliberada de MT5.path vazio. Principal
  integrada até #87 tem diagnóstico separado21 OK/1 aviso/0 falhas, ponte
  consultada em modo somente leitura; nenhuma operação em conta.
- Pendências separadas observadas: aviso parcial atribui origem só a MQL5 mesmo
  na seleção Python; densidade dos rótulos horários em DPI alto e contexto
  completo nos exports de gráficos exigem recortes próprios. Não é aceite
  visual global, nem demonstração da meta financeira.

## T17.11 — grade temporal e lacunas explícitas (2026-10-16)

- Branch `codex/t17-11-lacunas-horarias`, base T17.10/#91. Ambiente próprio
  instalado; baseline no 1851b1d: 875 aprovados/9 ignorados em 917,57 s, saída 0,
  antes do código. Pai e5ec314 reconciliado, com documentação adicional.
- Reprodução real MQLExemplo: 263 operações parciais, 37 faixas; 00:00 e 02:30
  apareciam em posições consecutivas e conectadas. Novo `hourly_summary` fornece
  grade de 24 h pela configuração do enriquecimento. Faixas com n=0 têm medidas
  ausentes. Metadados separam horário inválido e PnL indisponível; as medidas
  observadas continuam vindo de `by_dimension`.
- O componente de barras preserva None/nonfinite como ausência. A linha quebra
  em NaN; o espaçamento dos rótulos do eixo preserva todas as faixas e medidas.
  n=0 é distinto de amostra pequena e retorno zero. Ledger, cache, fórmulas
  financeiras e parâmetros foram preservados.
- 29 testes de núcleo/desenho aprovados em 7,78 s e 9 de UI em 38,51 s, incluindo
  grades configuráveis, vazio, exclusões, zero observado, segmentos e temas.
  Suíte completa no 3b4e6d1: 886 aprovados/9 ignorados em 879,42 s, saída 0. PR #92 draft.
- Operação real nos dois temas, 96/144 DPI e 1280×800/1920×1080: 48 posições,
  37 medidas, 11 lacunas e 4 segmentos. Expectativa, acerto, n e marca de amostra
  dos grupos observados comparados com a tabela original e preservados. Rótulos
  sem sobreposição medidos no renderer; PageDown/rolagem exercitados. CSV de
  verificação da API e gráfico exportado conferidos. Referência permanece Saída,
  sem inferir entrada. Capturas em docs/img/interface-visual/T17.11.
- Sessões encerradas com saída 0. A primeira gravação da auditoria falhou ao
  serializar um escalar NumPy; erro preservado e normalização corrigida no
  roteiro, mantendo valores e capturas. Doctor isolado: 15 OK/2 avisos/1 falha
  deliberada de MT5.path vazio. Não houve novo backtest, campanha PF19 ou
  operação em conta nesta tarefa.

## T17.20 — restauração com sincronização observável (2026-10-16)

- Branch `codex/t17-20-sincronizacao-arranjo`, base da principal após #89.
  Ambiente próprio instalado; baseline b9e54c9: 860 aprovados/9 ignorados em
  868,84 s, saída 0, antes do código.
- Duas execuções Windows do a893f54 falharam na espera fixa de 0,6 s do teste
  antigo. Logs e identidade da fixture/runtime preservados. A primeira correção
  local revelou outra etapa: feedback encerrado antes do refresh que reabilita
  o botão. Esperar apenas a mensagem permitia ignorar a segunda invocação.
- O teste agora aguarda callback e estado esperado do diálogo com prazo finito,
  incluindo o primeiro refresh. Cenários de restauração imediata e I/O falso
  atrasado em 1 s; nenhum MT5 real. Todas as verificações originais de proteção
  e bytes UTF-16 mantidas, com conferência por AST; novas verificações confirmam
  nenhuma restauração com terminal aberto e uma única chamada quando fechado.
- Quatro focados aprovados em 18,68 s. A tentativa local anterior, com 3 aprovações
  e 1 falha, permanece registrada. Suíte completa no cc13d0e em execução; resultado
  final pendente. Diff de runtime vazio: não houve alteração da aplicação.
- Doctor isolado: 15 OK/2 avisos/1 falha deliberada de MT5.path vazio. Nenhuma
  conta, configuração ou arquivo do terminal do usuário foi alterado.

## ORG05 — estado da integração visual (2026-10-16)

A principal eed5f34 reúne os PRs #81–#89 e #93, integrados separadamente após revisão
técnica do agente executor, checks no head final e smoke no módulo instalado com
configuração/dados privados. O trabalho original T17.6, as configurações e as
bases do usuário foram preservados. Nenhum agente adicional foi acionado.

Estado por PR, commits de merge, verificações no host, falhas preservadas e
pendências estão em [Continuidade da integração visual](propostas/CONTINUIDADE_INTEGRACAO_UI.md).
O painel do plano foi reconciliado com essas integrações. #90 recebeu a correção
separada do teste assíncrono (#93) e aguarda novo CI; #91/#92 possuem implementação, testes e
operação real registrados, mas ainda dependem da cadeia de integração.

O smoke histórico do #89 manteve as 129 operações, PF 0,637667 e resultado
negativo da execução anterior; ledger reconciliado e contexto anterior após
alterar período. São dados previamente expostos, sem campanha/OOS. A meta
financeira não foi demonstrada. Aceite global da UI, dados/custos e pesquisa
permanecem frentes explícitas. #79/#80, releases e contas seguem separados.

ORG05 altera somente documentação. Baseline a13c2c6: 856 aprovados/9 ignorados,
saída 0; base depois reconciliada com b9e54c9. Suíte final c3d3785: 860 aprovados/9
ignorados em 591,95 s, saída 0, após repetir a execução interrompida por reboot.
Depois recebeu #93, mantendo diff próprio somente documental. Doctor atual da
principal após restaurar apenas o servidor RPC: 21 OK/1 aviso/0 falhas. Pedido
de informações recolhíveis registrado em T17.21, ainda sem código neste snapshot.
## T17.18 — Análise e cabeçalho respeitam a fonte (2026-10-16)

- Ambiente isolado instalado/dev; baseline a893f54 repetida após reboot com
  866 aprovados/9 ignorados em623,39 s, saída0. Recebeu correção test-only #93.
- Filtros agrupados e cartões refluem pelo tamanho pedido pela fonte e valor;
  resultados em ScrollableContent existente, leitura OHLC quebrada e figuras
  com altura mínima em contêiner próprio. O contêiner evita que o backend
  Matplotlib/DPI sobrescreva o tamanho pedido do canvas. Cabeçalho mantém
  nome/conexão/ações legíveis; a legenda secundária só aparece quando cabe.
- Primeira tentativa entrou em ciclo de geometria; execução interrompida e
  logs preservados. Chave de layout e retirada do reset transitório de wrap
  estabilizaram a atualização. Dez focados existentes passaram em47,44 s;
  cinco de geometria em40,48 s; após contêiner,14 em74,99 s; fonte maior e
  número extenso:cinco em43,92 s. Assertivas nativas de largura, bounds e
  não sobreposição, PageDown, dados inalterados e cancelamento ao destruir.
- Aplicação operada com EURUSD H1 real,1.537 barras e resumo idêntico: dois
  temas,96/144 DPI,1920/1280/900 e leitura OHLC. Na escala real o backend
  reduziu o requisito do canvas; a correção do contêiner manteve a altura.
  O primeiro roteiro de hover sintético perdeu o readout num Leave nativo;
  repetido em144 DPI com movimento real do mouse, verificação aprovada.
  As duas sessões finais encerraram0. Nenhuma fórmula/candle/indicador mudou.
- Roteiros passaram a aguardar a base disponível antes de abrir o ativo;
  abrir cedo acionava o diálogo informativo previsto, não falha no produto.
  Suíte completa/doctor/CI e integração ainda pendentes neste snapshot.

- A revisão dos valores de geometria revelou aumento de1347→2051 px após
  redimensionar/alternar tema em DPI real. O full076053b foi interrompido com
  saída143 antes de alterar código; esse resultado não é aprovação. Contêiner
  agora conserva requisito próprio e desliga propagação do grid, evitando
  realimentação do requisito do backend. Teste verifica limites mínimo E máximo.
- Cinco focados com fonte maior, valor extenso e limites passaram em29,24 s.
  Operação real repetida:144 DPI ficou em600 px nos dois temas e1920/1280/900;
  96 DPI em480 px nas janelas menores e609 px quando há área. Texto/candles e
  resumo das1.537 barras idênticos; hover por mouse real, teclado e rolagem
  conferidos. Duas sessões encerradas0. Capturas finais substituem somente as
  imagens geradas nesta tarefa; tentativas anteriores mantidas nos artefatos.
- Resultado da suíte final/doctor/CI e revisão é atualizado no PR; não houve
  operação em conta, nova pesquisa financeira ou modificação de base do usuário.

## T17.21 — informação recolhível sem perder a seleção (2026-10-16)

- Ambiente próprio instalado/dev; baseline eed5f34 com 861 aprovados/9 ignorados
  em653,15 s, saída0, antes do código. Reutiliza DetailsPanel, sem dependências.
- Variante de informação com botão ℹ e texto de ação, Enter/Espaço e Escape;
  recolher conserva o texto e devolve foco ao botão quando necessário. Seis
  detalhes de seleção começam fechados; mudança de seleção atualiza o conteúdo
  sem expansão automática. O split devolve largura/altura à análise e conserva
  a largura de leitura de T17.17 quando aberto.
- Critérios extensos dos estados e explicação do traçado ficam sob informação;
  contexto primário e avisos sintéticos/custos/ausência/validade permanecem
  expostos. Recorte avançado mantém seus dados e passa ao mesmo botão informativo.
- Trinta testes existentes de componentes/Relações passaram em23,42 s; com
  os novos contratos de teclado, foco, texto fechado, seleção e ganho de área,
  33 passaram em28,97 s. Verificações anteriores de detalhe legível permanecem
  exercitadas com o painel explicitamente aberto; assertivas do componente
  original conferidas por AST. Suíte final/operação visual/doctor pendentes.
- As referências são a descrição do usuário e as telas atuais; anexos temporários
  ausentes não foram alegados como inspecionados. Relações continua demonstração
  sintética, sem ligação ao DTO/serviço F18. Não houve cálculo ou mudança de dados.

- Suíte no43a14b5 concluída: 864 passed, 9 skipped in 609.05s (0:10:09), saída0. Nas capturas da
  operação visual, texto/seleção/gráfico/tabelas permaneceram idênticos; clique
  real por mouse e Escape/foco funcionaram em96/144 DPI e três larguras.
  O glifo ℹ, apesar de presente no texto, não apareceu na fonte do host. Antes
  do aceite final, será substituído pelo ícone desenhado do sistema existente
  (`ui/icons.py`), com imagem não vazia e atualização por tema verificadas.
  A suíte anterior permanece evidência da versão anterior, não da correção.

- Ícone desenhado implementado no sistema Pillow/PhotoImage existente, com
  tamanho pela fonte, contraste do tema e rótulo acessível. Trinta e três
  focados passaram em27,72 s; tentativa anterior teve NameError na variável
  do próprio teste, corrigida sem mudar o contrato. Fotos novas96/144 DPI
  mostram o ícone; clique real, Escape/foco, paridade de texto/gráfico/tabela
  e seis perfis por escala passaram. As duas sessões encerraram0.
- Validação final após a correção e reconciliação com a principal é registrada
  no PR desta tarefa; a suíte864/9 anterior não substitui essa execução.

## Retomada T17.18 — altura nativa em DPI alto

- Validação combinada d05106f:896 aprovados/9 ignorados em742,33s, saída0.
  O CI Windows encontrou uma falha nova no limite superior da figura:892px
  para teto864px com fonte maior. Sem repetição cega ou tolerância adicional: o
  código deixa de usar a altura requisitada inicial do backend, que também pode
  incorporar DPI, e usa480px ou24linhas da fonte. Novo teste injeta pedido
  inicial de1920px e conserva as mesmas exigências de limite mínimo/máximo em
  duas escalas. As execuções anteriores ficam preservadas; resultado final do
  novo código e novo CI serão registrados antes da integração.

- O CI Windows do acb617f ainda encontrou excesso de altura:989px na regressão
  injetada e892px no resize, para teto864px. O diagnóstico local confirmou que
  o pack podia expandir a figura pela área atribuída à página do Notebook.
  Agora somente a largura vem do pack; a altura é calculada pela fonte e pelo
  espaço útil do viewport, descontando cartões, legenda, abas e leitura OHLC.
  Abas ocultas conservam o mínimo e a seleção/resize recalcula a aba visível.
  A regressão também força uma página de2048px: o fonte imutável acb617f falha
  nas duas escalas no host (2009>550 e2002>744); o novo código passa nos mesmos
  cenários. Cinco focados finais:38,37s. O primeiro roteiro de reprodução teve
  erro de importação de fakes (saída4), corrigido somente no harness; não foi
  contado como demonstração da regressão. Nenhum limite foi relaxado.

## T13.9 — PF definido e compatibilidade histórica (2026-10-16)

Implementação em PR97, sem campanha financeira. Um contrato comum remove tetos
99/999, representa vazio/sem perdas/inválido como null com motivo e conserva
razões finitas. Validação/bateria não aprovam por indefinição; consumidores
aceitam ausência. Projeções antigas recalculam PF quando o livro está completo
e mantêm os registros originais, inclusive aprovações de bateria que precisam
ser refeitas quando vinculadas a PF indefinido. Banco antigo continua abrindo.

Ambiente instalado/dev e baseline887/9 anteriores ao código. Focados156, depois
93 e56 nas revisões; suíte240f92a com926/9 em681,75s, saída0. O head final recebe
ajuste de proveniência JSON/ordenação, com validação específica e integral no
PR. Uma suíte anterior foi encerrada241 para incorporar a revisão, não contada
como aprovação. Doctor isolado15/2/1deliberado; principal21/1/0 separado.

Cópia pré-exposta:276 execuções e chaves únicas,14 vazias com PF0 sem denominador;
276 livros e4.140 valores de outras métricas preservados. UI real263operações
continua PF0,87; testes visuais controlados identificados verificam sem perdas
e razão2000, com motivo legível em144DPI. Não demonstram meta financeira.
Detalhes de escopo, limites e compatibilidade em
[PF definido](propostas/T13_9_PF_DEFINIDO.md).

## T17.22 — análise local explícita no ponto de entrada (2026-10-16)

Opção `--offline` implementada, sem persistência no INI. O app usa a lista
local de símbolos, inventário e análises; o conector dispensa SDK e caminho de
terminal nessa sessão. Inicialização/encerramento/erro não alcançam o SDK.
Cabeçalho neutro identifica análise local, ações de conexão/abertura/admin são
desativadas e explicam como reabrir; chamadas diretas também são guardadas,
inclusive a oferta de conta demo. O modo padrão permanece disponível. O fim
do programa encerra apenas seu próprio conector, em lugar de resolver o SDK
global incondicionalmente. Não altera estratégia, dados ou risco.

Baseline 8bacadf:890 aprovados/9 ignorados em810,86s, saída0, antes do código.
Focados iniciais13/12,14s; após contagem explícita das chamadas bloqueadas,
parser/default/help/cleanup e guardas de UI:8 aprovados em16,56s. Subprocesso
com a classe real/main abriu600 barras sintéticas de teste, desenhou análises
e encerrou sem chamada ao SDK/diálogos, mantendo o INI idêntico. Testes de
conector simulam Windows/Linux e preservam outro backend já existente.
Recebeu a cadeia T13.9/T17.18, com diff próprio restrito ao início local;
15 focados combinados em53,78s. Suíte final, CI, operação instalada pelo
lançador e diagnóstico serão registrados no PR. Windows real permanece
pendente; nenhum terminal/conta do usuário foi iniciado ou encerrado.

- A inspeção nativa do modo local encontrou a lista de símbolos configurados
  ainda chamada de “ativos da conta” e ações de extração/coleta habilitadas.
  Rótulos agora identificam a lista local; extração/coleta e chamadas diretas
  ficam desativadas, com a ação de reabrir explicada. Análises/importação
  continuam disponíveis. Oito focados passaram em16,47s após esse ajuste.
  A suíte56f73b2 foi interrompida241 antes da revisão, sem alegar aprovação;
  novo head tem suíte/CI próprios. Primeira inspeção offline1537barras saiu0.
