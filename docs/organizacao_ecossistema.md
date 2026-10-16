# Organização do ecossistema (frente ORG)

> **Situação: registrada em 2026-10-15.** Unidades administrativas ORG-01 a ORG-10, de preservação, integração e
> limpeza do trabalho espalhado entre os três repositórios do conjunto (mt5x, HADOUKEN e mt5x-safety). A ordem é
> **preservar e integrar antes de limpar**. Nada aqui altera o comportamento do programa, presets, Experts ou regras
> operacionais: cada mudança de código continua saindo de uma tarefa do [PLANO_DE_TRABALHO](PLANO_DE_TRABALHO.md),
> uma por PR. O painel das unidades está no próprio plano; este documento guarda as regras permanentes: fontes
> canônicas, fila de integração, política de Git e os gates de preservação antes da limpeza.

A rastreabilidade vem da auditoria local de 2026-10-15 (não publicada: contém dados operacionais e caminhos
locais). Ela fez o inventário de refs, worktrees e estados, e a preservação Git em ORG-01: bundles por repositório,
restauração verificada em repositórios separados, patches e tar do WIP da T17.6. **Nenhuma pasta, branch ou tag foi
removida, movida ou publicada nessa rodada.**

## Fontes canônicas

| Peça | Fonte canônica | Papel | Estado em 2026-10-15 |
|---|---|---|---|
| mt5x | `MISA_MT5-Extra-o-de-Dados` | Programa (extração, pesquisa e validação, SDK de robôs, Laboratório), planos e fila de integração | `master` = `origin/master`, árvore limpa |
| HADOUKEN | `projeto-indicadores-mt5` | Framework MQL5, painel, Experts e a camada `Safety/` (origem) | branch de trabalho `codex/t12-3-orcamento-global` à frente de `origin/master` |
| Safety | `mt5x-safety` | Distribuição versionada da camada `Safety/` | `origin/main` e tag `v1.3.0` publicados; 1.4.0 e 1.5.0 existem em branches locais, em revisão, **sem release estável** |
| Reservado | `HADOUKEN-MQL5-Framework` | Nome reservado para uma migração futura | Vazio (sem commits e sem refs). **Não é destino de implementação**: nenhuma tarefa da fila aponta para lá até o ADR do ORG-10 |

Regras de propriedade:

- **Uma tarefa, um repositório responsável.** Migrar nome ou pasta não resolve contrato, histórico divergente nem
  dados ausentes; por isso o framework continua no HADOUKEN atual até uma migração deliberada.
- **Safety:** a origem é a camada do HADOUKEN; o `mt5x-safety` é a cópia versionada e verificada (arquivo, hash,
  `source_commit` e contrato registrados a cada versão). O consumidor declara a versão em `robot.json`
  (`safety_lib`) e só é publicado depois do provedor disponível na versão declarada.
- **Ligação local:** `[ROBOTS] library_dirs` aponta para o clone do HADOUKEN e `safety_lib_dir` para o clone do
  `mt5x-safety`. Não alterar esses caminhos numa arrumação sem migração e teste.
- **T16.2b** (colunas novas no CSV de trades) é feita no repositório HADOUKEN populado, com compatibilidade
  antiga/nova e fixture consumida pelo mt5x — não depende de mudança de nome nem do repositório reservado.

## Fila de integração

**Como ler:** todo recorte do mt5x parte do `master` atual (`git fetch` antes de começar; o SHA exato é fixado no PR
do recorte) e não faz merge em bloco da branch mais nova — reaplica apenas os deltas ainda ausentes, preservando a
autoria e justificando conflitos. Os HEADs são o retrato de 2026-10-15; revalidar antes de agir. Branch do recorte e
PR são por tarefa; PR empilhado tem base explícita e teste do conjunto.

### mt5x (este repositório)

| # | Tarefa | Responsável | Branch do recorte (HEAD) | PR | Estado e próxima ação |
|---|---|---|---|---|---|
| 1 | Isolamento de schemas PostgreSQL | integração | `codex/postgresql-test-schema-isolation` (`80fd2f3`) | a abrir | Primeiro da fila: habilita testes PG concorrentes seguros. A branch carrega ancestrais Analytics: extrair só a mudança de isolamento ou empilhar PR com base explícita. |
| 2 | T13.2 (guarda de características) e T13.3 (perfis/regimes; a branch inclui correção de invalidação) | integração + Analytics | `tarefa/T13.3-perfis-regimes` (`3f9e333`) | a abrir | Separar em dois recortes/PRs, na ordem. |
| 3 | T13.3b (produtores incrementais) | integração | `tarefa/T13.3b-producers-incrementais` (`00f6770`) | a abrir | Depois da base T13.3. |
| 4 | T16.3b (enriquecimento temporal) | integração | `tarefa/T16.3b-enriquecimento-temporal` (`af6281a`) | a abrir | Integrar as correções de vazamento/volume; a aproximação M1/OHLC segue declarada. |
| 5 | T16.3c (calendário point-in-time) | integração + Analytics | `tarefa/T16.3c-calendario-point-in-time` (`6d9de5f`) | a abrir | Depois das fronteiras do T16.3b; manter as limitações de recibos reais. |
| 6 | T13.4 (hipóteses/walk-forward) | Analytics | `tarefa/T13.4-hipoteses-walk-forward` (`827afab`) | a abrir | Integrar a ferramenta; o status segue **parcial** por custos, ausência de eventos e OOS — integrar não aprova hipótese. |
| 7 | T16.12 (CLI/relatório), adaptador T13.6 e T16.11c (interação do Laboratório) | UI + Analytics | `tarefa/T16.12-cli-relatorio` (`adff6eb`), T13.6 (`dba30d7`), `tarefa/T16.11c-interacoes-laboratorio` (`701f4fd`) | a abrir | Respeitar as dependências efetivas; a UI nova já integrada é preservada — não restaurar tela antiga nem fazer checkout de arquivo inteiro antigo. |
| 8 | T18.1 (registry/DTO) e T18.2 (associações); ligação do T18.8 ao serviço | Analytics + UI | `tarefa/T18.1-registry-indicadores` (`f45618f`), `tarefa/T18.2-associacoes` (`76404a1`) | a abrir | Depois de reconciliar as dependências Analytics; o T18.8 hoje usa fixture sintética identificada, que não vira real sem o contrato/serviço. |
| 9 | T13.5 (publicador candidato) e T13.7a (especificação sombra) | integração + Analytics | `tarefa/T13.5-publicador-candidato` (`ab5b8a9`), `tarefa/T13.7a-especificacao-sombra` (`2525ab4`) | a abrir | Conforme decisões/dependências; candidato não é leitor MQL5 pronto e a especificação não autoriza operação. |

Fora da fila, com tratamento próprio:

- **T17.6** (worktree `../MISA_MT5-Extra-o-Dados-T17.6`, branch `tarefa/T17.6-laboratorio-legibilidade`): **sujo**,
  com alterações de código, testes, docs e imagens não rastreadas. Pertence ao responsável, que conclui o checkpoint
  (ORG-03). **Não remover, não substituir e não limpar por cima.**
- **T17.1** (worktree `../MISA_MT5-Extra-o-Dados-T17.1`, branch `tarefa/T17.1-sistema-visual`): commit original
  divergente do integrado (T17.1–T17.5 entraram por outra cadeia). Candidato a arquivo só depois de provar a
  equivalência e com refs arquivados (ORG-09).

### HADOUKEN / Safety

- Documentos **T12.1** (`d2ba9c3`) e **T12.6a** (`fe1bdd1`): integrar preservando as evidências posteriores.
- **G-02** (painel modular, `8d06844`) tem registro de divergência de escopo em `7710059`: ler e resolver antes de
  concluir o PR.
- **F-03** (exposição, `225fa7b`) e **T12.3** (orçamento global, `480d310`) são recortes separados, cada um com
  dependências e critérios de aceite próprios.
- **Safety 1.4.0** (`fc93bba`) e **1.5.0** (`d45257b`): um PR por versão, com manifesto de origem e comparação dos
  arquivos. Release estável só depois da revisão da política global de risco, dos testes de concorrência/reservas e
  dos aceites pendentes; **não retaggear a 1.3.0**.
- **T16.2b**: no repositório HADOUKEN populado, com decisão de fonte registrada; evoluir o CSV com compatibilidade e
  fixture consumida pelo mt5x.
- **Fonte Safety → distribuição:** política única (origem no HADOUKEN; cópia gerada/verificada no `mt5x-safety`),
  registrando arquivo, hash, `source_commit` e contrato; evitar edições manuais concorrentes.
- **Fronteira de `OrderSend`:** existe chamada legada em
  `Include/Framework/HADOUKEN/Plugins/Trading/HDK_RiskManager_Plugin.mqh` (fora de `Safety/`). Antes de afirmar
  alcance, mapear o grafo de includes e o legado suportado. O enforcement no CI deve distinguir chamada real de
  comentário/string, cobrir `OrderSendAsync` e wrappers, permitir somente fronteiras aprovadas explicitamente e ter
  teste negativo — sem exclusão ampla de `Plugins/`.

## Política permanente de commits, PRs e merges

1. **Uma tarefa por branch e por PR.** `master`/`main` são base de execução e integração; não recebem feature direto.
2. **Cada agente em worktree próprio**, com proprietário e tarefa explícitos. Uma fila de integração com um
   responsável; no máximo duas frentes de features ativas por repositório enquanto a dívida é reduzida.
3. Ciclo de cada tarefa:
   1. registrar tarefa/escopo/arquivos, dependências e critérios; `git fetch` da base e branch nova;
   2. preparar ambiente e baseline conforme as instruções do repositório;
   3. implementar e revisar os diffs, incluindo não rastreados; nunca `git add .` sem revisar;
   4. commit pequeno e coerente, em português; sem `config.ini`, `.env`, banco, logs, `.venv` nem caminhos pessoais;
   5. publicar checkpoint de branch autorizado e abrir PR draft cedo; trabalho parcial não é chamado de pronto;
   6. o PR informa problema/comportamento, escopo, dependências, commits, testes por plataforma, riscos e pendências;
      PR empilhado tem base explícita e teste do conjunto; a mesma branch não serve a várias tarefas;
   7. reconciliar a base atual; resolver conflito por conteúdo, nunca aceitando um lado inteiro;
   8. executar os checks no **SHA exato** a mesclar; alteração posterior invalida o check anterior relevante;
   9. revisão independente, CI exigido e critérios de aceite atendidos; falha intermitente fica registrada, não é
      desativada nem convertida em verde sem evidência; Windows real pendente conforme a regra do projeto;
   10. merge pela política do repositório, sem bypass, force-push ou merge em lote; a decisão de squash/merge é
       registrada (após squash, ID não ancestral exige mapa de equivalência);
   11. confirmar o SHA remoto, atualizar o principal com fast-forward seguro, rodar smoke e atualizar
       plano/continuidade/contratos; "commitado", "publicado", "mesclado", "compilado", "validado" e "aprovado para
       operar" são estados distintos;
   12. só então liberar worktree/branch, com os dados preservados e um relatório antes/depois.
4. **Release:** tag imutável apontando para commit revisado/mesclado, versão consistente no manifesto e nos exemplos,
   changelog, hash/origem e evidências de teste. Asserções de segurança ou paridade são evidência por cenário — não
   decorrem de ter release, nem de CI estático verde.

## Preservação antes de limpar

Classificação antes de remover qualquer worktree:

- **A.** Integrado por ancestralidade, limpo, sem agente ativo e sem dados únicos: candidato, após backup.
- **B.** Equivalente/reaplicado, sem ancestralidade: candidato só com prova de equivalência e refs arquivados.
- **C.** Pendente/único: preservar e enviar à fila de integração.
- **D.** Sujo, ativo ou com dados de usuário: preservar; nunca remoção automática.

Para **todos**: conferir staged/unstaged/untracked e ignorados — `git status` limpo não prova que a pasta está vazia.
Bancos SQLite exigem backup consistente (API de backup ou aplicação fechada; não copiar o `.db` com WAL ativo) com
restauração/`integrity_check` conferidos. Configuração e `.env` ficam privados, com permissões restritas. Não remover
runtime do MT5/Wine, `Common Files`, kill flags ou estado de robô como se fossem "cache".

A remoção usa `git worktree remove` **sem `--force`**, somente com os gates satisfeitos; um erro é impedimento, não
convite a `rm -rf`. `git worktree prune` não substitui remoção/preservação e não roda em lote sem inspeção. Branches
podem permanecer em refs/tags de arquivo até segunda revisão (`--merged` não detecta squash/reaplicação) e branches
remotas não são apagadas automaticamente. Antes de qualquer limpeza, criar a segunda cópia verificada em
armazenamento separado, com acesso restrito; o arquivo privado da auditoria não vai para o repositório.

## Critério de retomada organizada

- fontes canônicas registradas (este documento);
- checkpoint seguro de cada tarefa e WIP (ORG-03 conclui a T17.6);
- fila e dependências sem merge implícito;
- isolamento dos testes/dados (ORG-04);
- comunicação de contrato Analytics → DTO → UI;
- UI real distinguida de fixture;
- fonte Safety → distribuição → consumidor com versão rastreável;
- cada tarefa sai da fila por PR revisado, sem acumular uma nova branch "integração final" com tudo.

É possível seguir em tarefas independentes enquanto a fila é integrada; não congelar o projeto indefinidamente.
Evitar abrir frente nova dependente de três branches não revisadas e não iniciar migração grande junto com novas
features, limpeza e mudança de modelo de risco.
