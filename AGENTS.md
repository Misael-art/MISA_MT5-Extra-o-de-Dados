# Regras para agentes (IA ou humanos)

Leia este arquivo inteiro antes de alterar o projeto. O que fazer está em
[docs/ROADMAP.md](docs/ROADMAP.md) e, passo a passo, em
[docs/PLANO_DE_TRABALHO.md](docs/PLANO_DE_TRABALHO.md).

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

## Comandos úteis

| Objetivo | Comando |
|---|---|
| Testes | `.venv/bin/python -m pytest -q` |
| Diagnóstico | `.venv/bin/python -m mt5_extracao.bootstrap doctor` |
| Reconfigurar | `.venv/bin/python -m mt5_extracao.bootstrap configure` |
| Lint dos scripts | `shellcheck install.sh run.sh scripts/*.sh scripts/install/*.sh` |
| Ponte (Linux) | `./scripts/mt5-bridge.sh start\|stop\|status\|foreground` |
