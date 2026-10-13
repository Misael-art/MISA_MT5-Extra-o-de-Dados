# Continuidade do trabalho

Registro para retomar o desenvolvimento em qualquer máquina (Claude Code local, outro agente ou
pessoa) sem perder o histórico. O estado detalhado de cada tarefa fica no painel de
[PLANO_DE_TRABALHO.md](PLANO_DE_TRABALHO.md); este arquivo só diz **onde estamos e como continuar**.

## Onde está o código

- Branch: `claude/adoring-hopper-qdtf34`
- PR: [Misael-art/MISA_MT5-Extra-o-de-Dados#1](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/1) (rascunho, base `master`)
- Último commit registrado aqui: `1ca7306` (T8.3), CI verde nos 6 checks (Ubuntu, Windows, PostgreSQL,
  instaladores Linux/Windows e lint), sem conflitos e sem revisões pendentes.

## O que já foi feito

| Fase | Situação |
|---|---|
| 0 a 6 (instalação, integridade, MT5 no Linux, CLI, limpeza, ticks/book/PostgreSQL/colunar, estratégias) | ✅ feitas na sessão do Claude Code na nuvem |
| T0.3 / T2.6 (validação com MT5 real) | ⏳ parcial, feita no host: Linux (família Arch) validado; falta o lado Windows e outras distros ([validacao_instaladores.md](validacao_instaladores.md)) |
| 7 (interface em abas) e T8.0 a T8.3 (robôs) | ✅ feitas no host |

## Próximos passos

Na ordem do painel do plano:

1. **T8.4**: ferramentas MT5 (instalar arquivos, compilar, ler o log do MetaEditor).
2. **T8.5**: backtest MQL5 pelo Strategy Tester (depende da T8.4 e do HADOUKEN v7 compilando).
3. **T8.6** a **T8.9**: bateria de segurança, tempo real em demo/real e robôs de atuação ([robos.md](robos.md)).
4. Fechar **T0.3/T2.6** no Windows: rodar `scripts/parity_check.py` lá e comparar com o lado Linux.
5. Pendência opcional: testar a hypertable do TimescaleDB num servidor com a extensão (T5.2).

## Como retomar no Claude Code local

```bash
git clone https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados.git   # ou: git fetch && git pull
cd MISA_MT5-Extra-o-de-Dados
git checkout claude/adoring-hopper-qdtf34
./install.sh                         # Windows: install.bat
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m pytest -q
.venv/bin/python -m mt5_extracao.bootstrap doctor
claude
```

Mensagem inicial sugerida para o agente:

> Leia AGENTS.md, docs/continuidade.md e docs/PLANO_DE_TRABALHO.md. Estamos no branch
> claude/adoring-hopper-qdtf34 (PR #1). Continue pela próxima tarefa pendente do painel
> (T8.4), seguindo os passos do plano, e atualize o painel e este arquivo ao terminar.

## Regras para não perder progresso

- Faça commit e push no mesmo branch ao fim de cada tarefa; o CI do PR #1 valida Linux, Windows e PostgreSQL.
- Antes de começar em outra máquina: `git pull`.
- Nunca versione `.env`, `config/config.ini`, `*.db`, `logs/` nem `exports/` (o `.gitignore` já cobre).
- Ao terminar uma sessão, atualize a seção "Onde está o código" e "Próximos passos" deste arquivo.
