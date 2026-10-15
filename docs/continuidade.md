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
  `TMPDIR` e `--basetemp` dedicados em `/home` após um primeiro run interrompido por `Errno 28` de armazenamento
  temporário. `doctor`: **16 OK, 2 avisos, 0 falhas** (opcional
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
