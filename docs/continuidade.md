# Continuidade: harmonização de 2026-10-14

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

- Branch de origem: `tarefa/T13.1-calendario-sessoes`, criada a partir de `master` em `2226bf5` e integrada no PR
  #71 (merge `880bf18`).
- A T13.1 implementa calendário observado em M1, captura de metadados estáveis de `symbol_info`, barras derivadas
  versionadas e o modo `mt5x resample --session-aware`. As regras e limitações estão em
  [T13.1_CALENDARIO_OBSERVADO.md](analytics/T13.1_CALENDARIO_OBSERVADO.md).
- Verificação: testes focados `.venv/bin/python -m pytest -q tests/test_analytics_calendar.py tests/test_resample.py`
  — **20 aprovados**; `doctor` — 16 OK, 2 avisos, 0 falhas. As duas primeiras repetições integrais ficaram
  inconclusivas por espaço temporário e display inacessível; isso foi resolvido na harmonização com `xvfb-run -a`.
  A suíte integrada do master foi repetida com `/tmp`: **802 aprovados, 9 pulados em 510 s**. A base EURUSD foi lida
  sem escrita e sem conectar ao MT5. O modo legado `resample` não mudou.
- A branch foi integrada no PR #71. O worktree `../MISA_MT5-analytics` foi removido a pedido em 2026-10-14 após
  confirmar que estava limpo e que sua branch já era ancestral do master; o banco ignorado continha apenas tabelas
  internas vazias. A referência da branch local foi mantida.

## Harmonização 2 (2026-10-14): Fase 16 e T13.1 no master

Branch `integracao/fase16-t13.1`, mesclada no `master` pelo PR #71 (merge `880bf18`). Entraram:

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

**Próximas tarefas:** T13.2 foi implementada na branch local `codex/t13.2-feature-store`; após revisão e integração,
T13.3 pode começar. Pendências da Fase 16 na seção 10 do plano dela (T16.2b no HADOUKEN, exportação do calendário
com o MT5 conectado, Windows real).

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

## Retomada da T13.3 — 2026-10-15

- Branch local: `tarefa/T13.3-perfis-regimes`, com T13.2 como ancestral local; T13.2 e T13.3 ainda não foram integradas
  ao `master`. A implementação e as limitações da T13.3 estão em
  [T13.3_PERFIS_REGIMES.md](analytics/T13.3_PERFIS_REGIMES.md).
- Verificação após as alterações: testes focados T13.3/T13.1/T13.2 — **42 aprovados em 23,54 s**; suíte completa —
  **833 aprovados, 10 ignorados em 676,66 s**; `doctor` — **16 OK, 2 avisos, 0 falhas**. O único aviso opcional é
  `pandas-ta`; a ponte Wine está desativada pela configuração atual.
- A base `database/mt5_data.db` foi aberta somente em modo SQLite `mode=ro`; nenhum cálculo foi gravado nela e o MT5
  não foi conectado. EURUSD e AGG atenderam à validação de dois mercados descrita no relatório da T13.3.
- Decisão 5 permanece pendente do dono: a confiança proposta para regime é uma fração de concordância entre ADX,
  eficiência e R²; para RVOL é a quantidade de sessões usadas na mediana. A T13.4 pode avançar; a T13.5 aguarda a
  decisão conforme o plano. Windows real, PostgreSQL local, MT5 real e DST não foram validados.
- Worktree separado `/home/misael/Projects/MISA_MT5-Extra-o-Dados-T17.1` permanece intocado. Não houve push nem PR.
- Próxima tarefa elegível registrada nesta retomada: T13.4. T16.3b também é independente e pode ser tratada em branch
  separado; manter cada tarefa em sua própria branch.

## Retomada da T16.3b e T13.4 — 2026-10-15

### T16.3b — enriquecimento temporal do Laboratório

- Branch local independente `tarefa/T16.3b-enriquecimento-temporal`, commit `5d85cf3`, sobre o `master`; ainda não
  integrada. Corrige o uso de barra M1 aberta para entradas no meio da barra, a mistura barra a barra entre volume
  real e ticks, notícia posterior à entrada e MAE/MFE que incorporava a barra de saída inteira.
- Testes focados de enriquecimento/cache/calendário: **22 aprovados**. Suíte daquela branch: **806 aprovados, 9
  ignorados**; doctor **16 OK, 2 avisos, 0 falhas**. Limites de reconstrução intrabar e disponibilidade de notícias
  estão registrados em `docs/propostas/ANALYTICS_OPERACOES.md`. A alteração é local; sem push/PR.

### T13.4 — hipóteses e avaliação temporal

- Trabalho na branch local `tarefa/T13.4-hipoteses-walk-forward`, baseada na T13.3 (`bde3652`), com checkpoints
  `61c8326` e `7935b30`; ainda não integrada ao `master`. Arquivos de implementação: `hypotheses.py`,
  `pattern_engine.py`, `validation.py`, catálogo JSON
  H01–H16 e `tests/test_analytics_hypotheses.py`; relatório: [T13.4_RELATORIO.md](analytics/T13.4_RELATORIO.md).
- H01–H05 têm builders e foram avaliadas em memória sobre EURUSD e AGG, lendo `database/mt5_data.db` somente com
  SQLite `mode=ro` e `PRAGMA query_only=1`. Hashes, período, custos ausentes, amostras sem condição, resultados
  negativos e a única estimativa com amostra informativa constam no relatório. Nenhuma reserva foi gravada e nenhuma
  hipótese foi aprovada.
- H06–H16 são definições declarativas `DRAFT`, sem builders nem métricas avaliadas. Continuam pendentes a estabilidade
  agregada entre mercados comparáveis, tempo de recuperação e construtores dessas hipóteses. Avaliações agora gravam
  `RUNNING` e consomem a reserva antes do cálculo; exceções ficam como `FAILED`. Uma interrupção abrupta deixa a linha
  `RUNNING` visível e não libera a reserva. Decisão 5 continua pendente; não impede o relatório
  informativo H01–H05.
- Verificação após auditoria de tentativas: focada **81 aprovados em 38,32 s** (T13.4, calendário, feature store,
  perfis/regimes e validação estatística); completa **848 aprovados, 10 ignorados em 572,38 s**; `doctor` **16 OK, 2
  avisos, 0 falhas**. Avisos:
  dependência opcional `pandas-ta` ausente e ponte Wine desativada por configuração. Nenhum `.sh` foi alterado; não
  se aplica ShellCheck. Windows real, PostgreSQL e MT5 real não foram testados.
- Para T13.5 a dependência T12.1 segue **parcial** no plano. A decisão #6 resolveu SQLite/SQLAlchemy para o banco,
  mas o formato do snapshot ainda depende de medição; seguem pendentes a política de validade (#7) e o comportamento
  do leitor no Strategy Tester. T13.7a segue proposta pendente de decisão. Não iniciar ações no HADOUKEN, publicar snapshots,
  fazer pilotos ou tocar em contas. T17.1 e os worktrees T17.2/T17.3 permanecem fora do escopo e intocados.

## Atualização T13.4 — builders H06–H16 e validação de retomada — 2026-10-15

- Continuação na branch `tarefa/T13.4-hipoteses-walk-forward`, sobre a base `ccf5316`, com checkpoint local `0ff2df5`.
  `pattern_extensions.py`
  implementa H06–H16; H14/H15 exigem `trade_events`, H16 exige `signal_events`. Todos esses padrões seguem `DRAFT`;
  o motor não os deixa elegíveis mesmo com efeito sintético. `summarize_market_stability` publica apenas um resumo
  descritivo por grupo previamente declarado, hipótese, versão e timeframe.
- H06–H16 foram exercitadas por fixtures. O catálogo inclui horário sem fuso, resolução M1, entrada/saída e os
  limites de resolução de H14–H16. H06 alinha e só usa M15 fechada; H09/H10 rejeitam lacunas nos rótulos; H10 respeita
  a janela de recuperação; H12 preserva ambiguidade/ausência; H14/H15 usam apenas barras fechadas disponíveis.
- Leitura real somente leitura (`mode=ro`, `query_only=1`): H06–H13 produziram contagens EURUSD/AGG descritas em
  `docs/analytics/T13.4_RELATORIO.md`; não foram feitos testes de efeito ou reserva OOS. Volume real EURUSD conhecido
  zero manteve H08 sem amostra; AGG produziu H08. A `_trade_ledger` local não tem entrada, preço e direção simultâneos
  e não existe tabela de sinais, então H14–H16 não têm eventos reais para avaliar. EURUSD e AGG não foram declarados
  comparáveis.
- Sweep read-only H06–H13: **37,80 s**, RSS máximo **275,5 MiB**, 99.611 barras EURUSD + 8.181 AGG; SQLite permaneceu
  em **353.746.944 bytes** antes/depois. Suíte T13.4: **26 aprovados**. Suíte completa: **859 aprovados, 10 ignorados
  em 648,97 s**. `doctor`: **16 OK, 2 avisos, 0 falhas** (`pandas-ta` opcional ausente; ponte Wine desativada).
  Nenhum `.sh` mudou; não se aplica ShellCheck. Windows real, PostgreSQL, horário de verão e terminal MT5 não foram
  testados.
- A T13.4 permanece **parcial**: nenhum H06–H16 foi avaliado OOS; H14–H16 precisam de eventos reais válidos; não há
  grupo de mercados comparáveis declarado. Próximo limite operacional: T13.5 segue dependente de T12.1 e dos detalhes
  externos #6/#7/Strategy Tester; T13.7a ainda requer confirmação do dono. Nenhuma operação externa foi feita.

## Reconciliação da matriz e extensões temporais — 2026-10-15

- O checkpoint T13.4 de código é `0ff2df5`; `6ce45bf` atualiza o ponteiro de continuidade e `41bdef7` reconcilia
  estado da matriz/propostas. Ambos são locais e não integrados. A matriz agora classifica os itens sem dados reais
  OOS, sem mercado comparável, sem eventos de trade e
  sem jobs incrementais como pendentes, com o dado ou contrato que falta explícito.
- A linha T16.3b estava atrasada em `PLANO_DE_TRABALHO.md` e `propostas/ANALYTICS_OPERACOES.md`: o trabalho existe em
  `tarefa/T16.3b-enriquecimento-temporal`, commit `5d85cf3`, com 22 testes focados, suíte 806/9 e doctor 16/2/0; a
  aproximação M1/OHLC segue declarada. Não foi integrada.
- A revisão encontrou duas lacunas que não estavam detalhadas como tarefas: T13.3b para checkpoint, execução
  incremental, recuperação e medição dos produtores; T16.3c para disponibilidade/revisões econômicas *as-of*. As
  propostas foram registradas no plano. T16.3a armazena `event_time` e `imported_at`, mas não prova o momento em que
  `actual` foi recebido nem preserva versões anteriores; nenhuma data de recepção será inferida.
- A próxima tarefa técnica independente é T13.3b numa branch própria, derivada da T13.3; primeiro executar a linha de
  base do branch com `/tmp` e Xvfb, depois desenhar retomada/idempotência contra a recomputação integral. A T13.7a
  continua aguardando resposta do dono; T13.5 segue dependente de T12.1 e decisões de validade/Strategy Tester.

## Checkpoint T13.4 — completude do modelo de custos — 2026-10-15

- Na branch existente `tarefa/T13.4-hipoteses-walk-forward`, a auditoria encontrou preenchimento implícito de custos
  ausentes com zero na sensibilidade e ausência de bloqueio por componentes de custo desconhecidos. A sensibilidade
  agora exige custo disponível para cada entrada e não publica expectativa líquida incompleta; o gate OOS para
  `VALIDATING` verifica todos os componentes declarados, `cost_bps` e spread observado quando exigido pelo modelo.
- H01–H16 ainda declaram comissão e slippage ausentes; nenhuma hipótese pode ficar elegível até que esses componentes
  sejam medidos. Nenhuma base real foi escrita e nenhuma reserva OOS foi consumida.
- Verificação Linux: testes T13.4 focados **27 aprovados**; suíte completa **860 aprovados, 10 ignorados em 533,64 s**;
  `doctor` **16 OK, 2 avisos, 0 falhas** (módulo opcional `pandas-ta` ausente; ponte Wine desativada). Nenhum script
  `.sh` foi alterado. A T13.4 permanece parcial: H06–H16 não têm avaliação OOS real, H14–H16 ainda dependem de eventos
  reais válidos e não há comparabilidade declarada entre mercados.
