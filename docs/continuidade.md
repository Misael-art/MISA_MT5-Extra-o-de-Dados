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

## T16.3b — correção temporal do Laboratório (2026-10-15)

- Branch local `tarefa/T16.3b-enriquecimento-temporal`, criada sobre o `master` em `880bf18`. A tarefa concluiu a
  correção de barras abertas, disponibilidade da surpresa, fonte única de volume, rótulo de resolução de MAE/MFE e
  migração aditiva do cache. Evidências e escopo estão no andamento da
  [proposta do Laboratório](propostas/ANALYTICS_OPERACOES.md).
- Verificação: baseline **802 aprovados, 9 ignorados em 717,57 s**; focados **22 aprovados em 17,29 s**; suíte após
  alterações **806 aprovados, 9 ignorados em 531,31 s**; `doctor` **16 OK, 2 avisos, 0 falhas**. Avisos: pandas-ta
  opcional ausente e ponte Wine desativada por configuração.
- MAE/MFE usa barras OHLC sobrepostas ao período da operação; sem leitura de ticks, os extremos de barras parciais
  podem não coincidir com a ordem intrabar real. Isso fica exposto em `mae_mfe_resolution`.
- T13.2/T13.3 seguem fora do `master` em branches locais; T13.3 foi concluída em
  `tarefa/T13.3-perfis-regimes` (`bde3652`) e sua continuidade está registrada na branch própria. O worktree separado
  `/home/misael/Projects/MISA_MT5-Extra-o-Dados-T17.1` não foi alterado. Nenhum push ou PR foi feito.

## T16.3c — proveniência point-in-time do calendário (2026-10-15)

- Worktree local `/home/misael/Projects/MISA_MT5-Extra-o-de-Dados-T16.3c`, branch
  `tarefa/T16.3c-calendario-point-in-time`, empilhada sobre a dependência local T16.3b (`5d85cf3`, baseada em
  `master` `880bf18`).
- Implementação em validação: colunas aditivas de disponibilidade em `_economic_events`, tabela de revisões
  `_economic_event_revisions`, `load_revisions`/`load_asof`/`merged_asof`, e enriquecimento que só expõe surpresa após
  disponibilidade registrada. Reimportação idêntica permanece idempotente; eventos antigos sem recibo não ganham
  disponibilidade a partir do evento ou de `imported_at`.
- Baseline antes das alterações: `test_trade_calendar.py`, `test_analytics_calendar.py` e `test_trade_enrich.py`,
  **29 aprovados**. A primeira suíte integral parou por `ENOSPC` (85 falhas, 415 aprovados, 8 ignorados, 98 erros
  antes do `INTERNALERROR`); a segunda teve **4 falhas, 802 aprovados, 9 ignorados**, todas com erros de espaço.
  Sem tocar em `/run/user/1000`, a reprodução isolada das quatro falhas passou **4/4** com
  `XDG_RUNTIME_DIR`, `TMPDIR` e `SQLITE_TMPDIR` em diretórios privados de `/tmp`.
- Validação final: focados **28 aprovados** (`test_trade_calendar.py`, `test_trade_enrich.py`, `test_trade_cache.py`);
  suíte integral **812 aprovados, 9 ignorados em 601,71 s**; `doctor` **16 OK, 2 avisos, 0 falhas**. Avisos: `pandas-ta`
  opcional ausente e ponte Wine desativada por configuração. `git diff --check` limpo.
- Evidência de replay com dados de mercado permanece pendente até existir histórico real de recibos/revisões da fonte.
  Auditoria local somente leitura em 2026-10-15 confirmou que `[SCREENER] events_file` está vazio e que `Common\Files`
  não contém exportação `MT5X_calendar_*.csv`; não há fonte local para o replay. O importador CSV aceita `source_received_at`
  explícito com fuso e o normaliza para UTC. Nesta continuação, o exportador MQL5 passou a gravar `observed_at_utc` após
  cada consulta bem-sucedida; o importador guarda esse horário como observação da versão, sem preencher
  `source_received_at`. A API não declara a hora de recebimento da fonte. O exportador recusa o Strategy Tester para não
  usar seu relógio simulado como UTC; a hora depende do relógio local do terminal. Como o arquivo corrente é substituído,
  cada exportação precisa ser importada antes da seguinte para preservar revisões intermediárias. O replay real ainda
  exige uma exportação feita no MT5 conectado ou CSV com recibos explícitos. Testes focados calendário/enriquecimento/cache:
  **29 aprovados**; suíte completa: **813 aprovados, 9
  ignorados em 644,74 s**; `doctor`: **16 OK, 2 avisos, 0 falhas**. A compilação/exportação em terminal MT5, Windows e
  PostgreSQL não foram exercitados. `event_time` continua em horário do servidor; o offset exportado é apenas o atual e
  não resolve conversão histórica sob DST. Worktrees da T17 permanecem intocados; não houve push ou PR.
