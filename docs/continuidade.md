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

## Continuidade do ciclo Analytics — checkpoint 2026-10-15

O objetivo ativo é continuar a Fase 13 em ordem de dependência, avançando trabalho independente enquanto decisões ou
dados externos não chegam. Entregas locais deste ciclo, cada uma no seu worktree: T13.3b (`tarefa/T13.3b-producers-incrementais`,
`3921af5`), T13.4 (`tarefa/T13.4-hipoteses-walk-forward`, `ae30450`) e T16.3c
(`tarefa/T16.3c-calendario-point-in-time`, `fc24bde`). Nenhuma foi publicada ou mesclada.

T16.11c está na branch `tarefa/T16.11c-interacoes-laboratorio`, separada dos worktrees T17.1–T17.5. A entrega inclui
filtro cruzado por clique em heatmaps de contexto e atalho do painel de detalhes do robô. A suíte completa passou com
**789 aprovados e 9 pulados**; os 40 testes focados de Laboratório e janela principal também passaram. O doctor reportou
16 OK, 2 avisos (`pandas-ta` opcional ausente e ponte Wine desativada) e 0 falhas. A validação real do Tk/Matplotlib no
Windows segue pendente na lista de [validacao_instaladores.md](validacao_instaladores.md).

### Pendências e decisões

- **T13.5:** aguarda a conclusão parcial da T12.1 e as decisões externas sobre política de frescor (#7) e comportamento
  do Strategy Tester.
- **T13.7a:** foi solicitada confirmação para adicionar o modo shadow, apenas informativo e sem ordens. A resposta do
  dono ainda está pendente; essa confirmação não autoriza piloto nem conta real.
- **T13.4:** evidência fora da amostra ainda depende de custos e calendário confiáveis e de mercados comparáveis reais.
- **T16.3c:** ainda falta replay de recibos de uma fonte real para validar a história de disponibilidade no tempo.

Não alterar os worktrees T17.1–T17.5 ao retomar. Sem resposta do dono, continuar somente com tarefas independentes que
tenham entrada, aceite e testes determinísticos.

### Continuidade do ciclo Analytics — checkpoint T16.12 (2026-10-15)

T16.12 foi implementada localmente na branch `tarefa/T16.12-cli-relatorio`, HEAD `e99becd` (implementação inicial
`34f43d7`), em worktree própria e baseada na entrega T16.11c. Ela preserva `compare`, `report --by`, simulação de filtros e `insights --save` do CLI já publicado, além de
adicionar `enrich`, relatório completo, re-simulação por run e `export --rules-file`. Os comandos usam os agregadores e
motores existentes; re-simulação de execução e persistência de insights exigem `--save`. `--json` mantém o envelope
global do CLI. A exportação gera uma proposta para revisão, sem aplicar regras.

Antes de qualquer alteração, a suíte completa passou com **789 aprovados e 9 pulados**. Após a implementação, os testes
focados de CLI (com compatibilidade), importadores e exportação passaram (**15 aprovados**), e a suíte completa passou
com **794 aprovados e 9 pulados** em tela virtual. O doctor reportou **16 OK, 2 avisos opcionais e 0 falhas** (`pandas-ta` ausente
e ponte Wine desativada). A validação real da CLI/JSON no Windows continua pendente no checklist de
[validacao_instaladores.md](validacao_instaladores.md).

T13.5, T13.7a, T13.4 e T16.3c seguem com as pendências e decisões descritas acima. Os worktrees T17.1–T17.5 continuam
separados e não foram alterados.
