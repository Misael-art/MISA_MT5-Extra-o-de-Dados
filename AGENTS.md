# Regras para agentes (IA ou humanos)

Leia este arquivo inteiro antes de alterar o projeto. O que fazer está em
[docs/ROADMAP.md](docs/ROADMAP.md) e, passo a passo, em
[docs/PLANO_DE_TRABALHO.md](docs/PLANO_DE_TRABALHO.md). Estado atual, frentes integradas e como
retomar: [docs/continuidade.md](docs/continuidade.md) (harmonização de 2026-10-14).

## Fluxo obrigatório

1. **Uma tarefa do plano por PR** (ex.: T1.1). Não misture tarefas nem faça "melhorias" extras.
2. Antes de começar, instale o ambiente: `./install.sh --yes --no-mt5` (Linux) ou
   `install.bat -Yes -NoMT5` (Windows). Depois: `pip install -r requirements-dev.txt` no `.venv`.
3. Rode os testes **antes** de mudar qualquer coisa, para conhecer o estado inicial:
   `.venv/bin/python -m pytest -q`.
4. Siga os passos da tarefa na ordem. Se um passo não bater com o código (ex.: a linha citada
   não existe mais), **pare e registre no PR** o que encontrou. Não invente outra solução.
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

- Este projeto tem um único checkout ativo por componente, sob a raiz de projetos escolhida pelo proprietário. Neste host, use somente `/home/misael/Projects/MT5X`. Resgates antigos são arquivos de consulta, não outros ambientes de desenvolvimento.
- Leia também as instruções do diretório pai e o registro central `.workspace/projects.json`, quando presentes. A instrução atual de raiz única prevalece sobre sugestões históricas de criar clones ou worktrees por agente/tarefa. Não crie worktrees nem variantes paralelas sem pedido explícito do usuário.
- Preserve alterações locais. Faça mudanças pequenas no checkout existente, valide, revise arquivos/segredos/tamanho, faça commit e push do lote autorizado e confira o SHA remoto antes de declarar sincronização. Não faça stage global de material resgatado, push forçado ou merge automático de pesquisas.
- Mantenha código, documentos e receitas no Git. SDKs, caches, dependências, builds, ROMs, bancos, pacotes de migração, sessões e dados privados usam os destinos próprios, com manifestos e recuperação verificável; `.gitignore` não é backup. Novo arquivo acima de 10 MiB exige revisão e armazenamento apropriado.
- Continue autonomamente até a entrega verificável. Evite confirmações rotineiras e relatos de pequenos avanços. Interrompa apenas o que depender de informação, acesso ou autorização indispensável; registre causa e próxima ação, mantendo as partes independentes em andamento.
- Resgate e testes históricos não aprovam funcionamento neste host. Preserve critérios e evidências de cada produto. A autorização de continuidade não permite publicar conteúdo privado em repositório público nem fazer merge/release/deploy do produto.
