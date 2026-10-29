# Regras para agentes (IA ou humanos)

Leia este arquivo inteiro antes de alterar o projeto. O que fazer está em
[docs/ROADMAP.md](docs/ROADMAP.md) e, passo a passo, em
[docs/PLANO_DE_TRABALHO.md](docs/PLANO_DE_TRABALHO.md). Estado atual, frentes integradas e como
retomar: [docs/continuidade.md](docs/continuidade.md) (harmonização de 2026-10-14).

## Fluxo obrigatório

1. **Uma tarefa do plano por PR** (ex.: T1.1). Não misture tarefas nem faça "melhorias" extras.
2. Reutilize o ambiente preparado pelo wrapper de `CONTINUAR_AQUI.md`; diagnostique antes de instalar novamente. Se estiver ausente, use o instalador oficial e `requirements-dev.txt` no único `.venv`, respeitando os limites de download e privilégio.
3. Conheça o baseline dos checks afetados antes de mudar código. Execute a suíte pertinente e os gates obrigatórios ao fechar o lote; documentação isolada exige revisão de diff/referências, sem repetir uma suíte pesada sem necessidade.
4. Siga os passos da tarefa na ordem. Se um passo não bater com o código (ex.: a linha citada
   não existe mais), investigue a divergência no código atual e registre no PR. Preserve o contrato; prossiga no escopo comprovado e nas partes independentes, solicitando decisão somente quando indispensável.
5. Rode de novo: `pytest`, `shellcheck` (se mexeu em `.sh`) e o `doctor`.
6. Atualize o painel de status em `docs/PLANO_DE_TRABALHO.md`.

## Regras técnicas

- **Windows e Linux são plataformas de primeira classe.** Use `pathlib`/`os.path`; nunca fixe `C:\`
  nem `/home` em código (exceto nos valores padrão já centralizados em `mt5_extracao/bootstrap/paths.py`).
- **Validação no Windows é pendência, não bloqueio.** O host de desenvolvimento é Linux (MT5 no Wine): conclua a
  tarefa verificando nele, teste os dois caminhos simulando `paths.is_windows`, e registre o que só o Windows real
  confirma no checklist de `docs/validacao_instaladores.md`. Nunca afirme que algo funciona no Windows sem ter
  rodado lá. Regra completa: "Regra da pendência do Windows", Fase 10 do PLANO_DE_TRABALHO.md.
- **`mt5_extracao/bootstrap/` usa só a biblioteca padrão.** Ele roda antes das dependências existirem.
- **Não importe `MetaTrader5` fora de `mt5_extracao/mt5_backend.py`** (a partir de T2.2).
  Enquanto T2.2 não for concluída, não adicione novos `import MetaTrader5`.
- **Não importe `tkinter` no núcleo** (`mt5_extracao/*.py`, exceto `ui_manager.py`).
- **Banco de dados:** nunca apague nem renomeie tabelas existentes de usuários; mudanças de schema
  são aditivas (novas colunas/tabelas). Bases antigas devem continuar abrindo.
- **Configuração:** chaves novas vão para `DEFAULTS` em `mt5_extracao/bootstrap/config_builder.py`
  **e** para `config/config.ini.example`. O código deve funcionar com `fallback=` quando a chave faltar.
- **Segredos:** senhas só no `.env` (nunca em `config.ini`, logs ou argumentos de linha de comando).
- **Dependências:** adicione em `requirements.txt` somente se usadas pelo código em execução.
  Opcionais vão em `requirements-optional.txt`. Não fixe versões sem necessidade.
- **Valores duplicados entre arquivos** (versão do rpyc, pasta do Python no Wine, porta da ponte,
  faixa de Python): veja a tabela em PLANO_DE_TRABALHO.md (T0.1) e altere todos juntos.
- **Instaladores:** devem continuar idempotentes (rodar 2× não quebra nem sobrescreve o que o
  usuário configurou) e com mensagens amigáveis em português, cada erro com a ação de correção.
- **Testes:** todo comportamento novo tem teste em `tests/`. Testes não podem exigir o MT5 real
  (use o provedor falso de T4.3 ou a ponte com um `MetaTrader5.py` falso, como em
  `tests/test_bootstrap_doctor.py`). Nunca desative, pule ou apague testes para o CI passar.
- **Estilo:** siga o código ao redor (logs com `log = logging.getLogger(__name__)`, mensagens em português).

## Não versionar

`config/config.ini`, `.env`, `*.db`, `logs/`, `exports/`, `.venv/`, credenciais e caminhos pessoais
(o `.gitignore` já cobre, então não force com `git add -f`).

## Prévia de desenvolvimento do proprietário

Por solicitação explícita de 2026-10-21, cada avanço testável deve ficar disponível
no atalho habitual da área de trabalho e no menu. Manter ambos apontando para
`mt5x/app-development/run.sh` na pasta de dados do usuário, com uma composição
coerente e identificada por commit, conforme [a prévia](docs/TESTAR_DESENVOLVIMENTO.md).
Publicar o fonte no GitHub e atualizar essa prévia após a verificação pertinente;
não deixar o atalho em checkout antigo enquanto o desenvolvimento está em outro.
Isso autoriza disponibilizar a prévia para teste; não transforma rascunhos ou
falhas conhecidas em entregas aprovadas nem altera a referência aceita.
Reutilizar ambiente/dados, conservar retorno e remover somente temporários próprios.

## Comandos úteis

| Objetivo | Comando |
|---|---|
| Testes | `.venv/bin/python -m pytest -q` |
| Diagnóstico | `.venv/bin/python -m mt5_extracao.bootstrap doctor` |
| Reconfigurar | `.venv/bin/python -m mt5_extracao.bootstrap configure` |
| Lint dos scripts | `shellcheck install.sh run.sh scripts/*.sh scripts/install/*.sh` |
| Ponte (Linux) | `./scripts/mt5-bridge.sh start\|stop\|status\|foreground` |

## Coleção Projects: continuidade e organização

- Política comum: [../../../AGENTS.md](../../../AGENTS.md), especialmente autonomia, testes, GitHub e recursos (§§5–10); raiz/branch em [registro central](../../../.workspace/projects.json). A orientação atual de raiz única substitui recomendações históricas de criar worktrees/clones por tarefa; os contratos técnicos e gates deste projeto permanecem obrigatórios. Em outro host, preserve este mesmo método em um único checkout registrado, sem recriar caminhos antigos.
- Antes de implementar, leia `CONTINUAR_AQUI.md`, `docs/continuidade.md`, `docs/ROADMAP.md` e `docs/PLANO_DE_TRABALHO.md`, identifique causa, owner, dependências e aceite. Procure a solução oficial existente e integre nela; não introduza soluções concorrentes, aliases ambíguos ou refatorações fora do pedido. Neste host, a única raiz é `/home/misael/Projects/MT5X`.
- Execute o lote até implementar, testar, revisar e preservar a continuidade. Resolva decisões reversíveis pelo contexto; não pare em planos nem peça confirmação para editar/testar o que já está autorizado. Comunique descobertas, bloqueios e entrega, sem narrar cada comando ou pequeno avanço.
- Antes de operações pesadas, confira espaço do destino, RAM e carga; estime saídas e retenção. Reutilize ferramentas/ambientes compatíveis, limite jobs e encerre somente processos temporários próprios. Não mate trabalhos alheios, limpe caches compartilhados ou apague evidência para obter um gate verde.
- A raiz da composição é `MT5X/`: App somente em `repos/app`, HADOUKEN em `repos/hadouken`, Safety em `repos/safety`; um ambiente App em `repos/app/.venv`, insumos/recibos nos destinos do wrapper. Respeite os commits fixados; não altere pins nem crie outro motor de dados, cache, scheduler ou executor para contornar integração.
- Faça os checks pertinentes a toda alteração; para documentos, revise diff, referências e coerência. Para código, cubra comportamento/regressão e execute os gates aplicáveis, sem desativá-los. Use o launcher/atalho central para validar GUI e CLI no host atual, preservando os dados. O doctor offline é `doctor --development --no-probe`; não qualifica MT5/Wine/Windows nem contas. Integração nativa exige ambiente permitido, ownership/lease e evidência real; nunca usar negociação REAL como teste.
- Reutilize a autenticação válida e os helpers existentes. Nunca peça senha/token no chat, copie sessões ou grave credenciais em código/log. Se faltar acesso indispensável, solicite uma única ação pelo fluxo oficial, preserve o estado e avance no trabalho independente; não repita permissões já concedidas.
- Confira remoto, PRs e checks do SHA antes do lote e ao publicar; reutilize o PR da mesma branch/objetivo. Revise arquivos explícitos, segredos e tamanho, faça commit/push do incremento autorizado e confirme o SHA remoto. Sem stage global de resgates, push forçado, merge de pesquisas ou publicação de dados privados. Não faça polling contínuo de CI.
- PR cabe ao incremento revisável; release somente a marco elegível com versão/artefatos/notas e gates completos, dentro da autorização de publicação vigente. Não gerar release por manutenção destas regras. Preserve limites de merge/deploy e falhas preexistentes; acesso autenticado não amplia autorização.
- Fontes e receitas ficam no Git; bancos, ROMs, assets privados, SDKs, builds, caches e pacotes de resgate seguem owner/destino próprios e recuperação verificável. `.gitignore` não é backup. Novo arquivo acima de 10 MiB exige revisão; downloads acima de 70 MB são manuais, inclusive o pacote dividido, com origem/tamanho/hash/destino preparados.
- Ao fechar, mantenha apenas os gerados necessários e atualize o handoff existente com SHA, testes no host, limites de validação, bloqueio e próximo passo. Não declare produto pronto quando faltar evidência do alvo real, nem deixe um único exemplar de trabalho útil sem proteção identificada.
