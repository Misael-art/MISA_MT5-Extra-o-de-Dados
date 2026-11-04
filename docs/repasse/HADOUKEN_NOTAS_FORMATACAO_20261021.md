# Notas preservadas da frente HADOUKEN T20/T21

Transcrição normalizada dos dois documentos encontrados no WIP dessa frente em21/10/2026. Os originais exatos e seus hashes estão no pacote privado. Estas notas preservam contexto de autoria; não substituem o [roteiro atual](RECUPERACAO_POS_FORMATACAO_20261021.md) nem representam revisão/execução independente das ferramentas citadas.

A referência operacional do integrador foi build6276; o build6258 e o erroIPC citados abaixo pertencem ao relato dessa frente. A ponte/principal usados pelo integrador não foram declarados genericamente quebrados. Não executar scripts de host, reiniciar a ponte ou copiar perfis por causa dessa transcrição. Os comandos de instalação do roteiro atual usam o bootstrap do projeto e preservam a separação entre código, dados privados e aceites pendentes.

## Fonte: `Documentation/HADOUKEN_2/CONTINUIDADE_FORMATACAO_20261021.md`

# Continuidade após a formatação do host (2026-10-21)

O host de desenvolvimento (Manjaro, Wine, MT5 build 6258) vai ser reinstalado. Este arquivo diz
onde está cada coisa no GitHub, o que **não** está e como retomar sem perder trabalho.

## 1. Repositórios

| Repositório | Visibilidade | Papel |
|---|---|---|
| `Misael-art/projeto-indicadores-mt5` | privado | Este repositório: Experts, motor `HDK_StrategyRobot`, `Safety/`, painel, contratos |
| `Misael-art/MISA_MT5-Extra-o-de-Dados` | **público** | `mt5x`: extrator, backtester Python, sessões no Tester, ponte, UI. Não enviar credencial nem dado de conta |
| `Misael-art/mt5x-safety` | privado | Distribuição da camada Safety/painel para o mt5x |
| `Misael-art/HADOUKEN-MQL5-Framework` | privado | Framework legado; sem trabalho pendente |

## 2. Trabalho que estava só neste disco e foi enviado

Branches com commits que nunca tinham sido enviados foram publicados com o próprio nome. Se o nome
já existia no GitHub com outro conteúdo, foram para `backup/formatacao-20261021/<branch>`.

Mudanças **não commitadas** de cada worktree foram gravadas como um commit de snapshot, sem tocar
no diretório de trabalho, em `backup/formatacao-20261021/wip/<branch>`. Ficaram de fora `.venv`,
`database/`, `*.db`, `*.sqlite` e arquivos acima de 50 MB. Esses snapshots são de várias frentes
(muitas de outros agentes); **não integre direto**: cada dono confere e decide.

Lista completa: ver a seção 8 (gerada pelo script de backup).

Para listar no GitHub:

```bash
git ls-remote origin 'refs/heads/backup/formatacao-20261021/*'
```

## 3. O que NÃO está no GitHub (backup externo ou regenerar)

| Item | Onde ficava | Tamanho | Decisão |
|---|---|---|---|
| Terminal MT5 + Wine (histórico de ticks, Tester, cópias de terminal isoladas) | `~/.local/share/mt5-extracao/` | 112 GB | Regenerável: reinstalar o MT5 e baixar o histórico. Copiar só se quiser evitar o download |
| Banco local do mt5x | `MISA_MT5-Extra-o-de-Dados/database/mt5_data.db` | 456 MB | Copiar para disco externo (regenerável pelo extrator, mas leva tempo) |
| Evidências e sessões do mt5x | `MISA_MT5-Extra-o-de-Dados/logs/` (ignorado pelo git) | 43 GB | Provas JSON das campanhas T8.7/T19/M19. Copiar se a auditoria for necessária; os PRs já citam os resultados |
| Configuração com credenciais | `MISA_MT5-Extra-o-de-Dados/config/config.ini` | — | **Copiar manualmente** (login DEMO, servidor, caminhos). Nunca commitar |
| Wine padrão | `~/.wine` | 1,1 GB | Descartável |
| Memória do agente Claude | `~/.claude/projects/<PROJECT_CONTEXT>/` | 14 MB | O conteúdo útil está transcrito na seção 6 |

## 4. Reinstalação: passo a passo

1. Instalar `git`, `gh` (e `gh auth login`), `python3`, `g++`, `wine`, `xvfb`, `flock`, `iconv`.
2. Clonar:
   ```bash
   mkdir -p ~/Projects && cd ~/Projects
   gh repo clone Misael-art/projeto-indicadores-mt5
   gh repo clone Misael-art/MISA_MT5-Extra-o-de-Dados
   gh repo clone Misael-art/mt5x-safety
   ```
3. Validar este repositório (não precisa de MT5):
   ```bash
   cd projeto-indicadores-mt5
   python3 Tools/check_repo.py
   python3 -m unittest discover -s Tools/tests
   ```
   Os testes de referência das estratégias compilam o corpo MQL com `g++`; se `g++` faltar, eles
   falham, e isso não é regressão.
4. mt5x: criar `.venv`, `pip install -r requirements.txt`, restaurar `config/config.ini` do backup
   e seguir `docs/continuidade.md` daquele repositório.
5. MT5 no Wine: instalar em `~/.local/share/mt5-extracao/wine` (é o caminho que o `config.ini`
   espera) e entrar **somente na conta DEMO**.
6. Compilação isolada: ajustar `SRC` em `Tools/agente/hdk_compile.sh` (ver `Tools/agente/README.md`).
7. Worktrees: recriar só quando for trabalhar, com `Tools/agente/t21_new.sh` ou
   `git worktree add -b <branch> ../_worktrees/hadouken/<dir> origin/<branch>`.

## 5. Estado do trabalho

### 5.1 Frente Estratégia (T20/T21): PRs abertos, CI verde, sem merge

| PR | Entrega | Estado |
|---|---|---|
| #49 | `HDK_Tsmom` | compilado (0/0), teste de referência |
| #50 | `HDK_IntradayMomentum` | idem; precisa de ativo B3 |
| #51 | preset Donchian H4 + `T20_SUGESTOES_EXPERTS.md` | preset |
| #52 | `HDK_TurtleSoup` | compilado, testado |
| #53 | `HDK_Rsi2Trend` | compilado, testado |
| #54 | preset `HDK_Orb_FaixaAsia.set` | preset |
| #55 | `HDK_Nr7Breakout` | compilado, testado |
| #56 | `HDK_GapFade` | idem; precisa de B3 |
| #57 | `HDK_ValueArea` | idem; precisa de B3 |
| #58 | `HDK_OvernightDrift` | compilado, testado |
| #59 | classificador de regime (`HDK_RegimeReference.mqh`) | referência sem consumidor |
| #60 | registro: `propostas/T21_COBERTURA_E_PROPOSTAS.md`, Épico C C-13..C-24 | docs |

Nenhum tem replay no Strategy Tester, par Python no mt5x ou aprovação. Não declarar lucro.

**Integração:** cada PR de Expert muda a contagem de consumidores B05 de 10 para 11
(`Tools/tests/test_position_policy.py` e `Tests/Contracts/HDK_PositionPolicy_Test.mq5`). Integrar
um por vez e regenerar com `Tools/agente/b05_add.py` (termina em 18). Magics reservados:
20262001–20262003 e 20262101–20262109.

### 5.2 Outras frentes deste repositório

- Painel compacto PM-12 (abas Operar/Risco/Cestas/Análise/Visual): ver
  `propostas/PM12_PAINEL_COMPACTO.md`. Pendente: aceite do `HDK_PanelHost` num Expert real e
  conferência em terminal com janela (DEMO); Windows físico não validado.
- Branches locais publicados no backup: `codex/t12-3-orcamento-global`, `codex/t16-2b-csv-completo`,
  `codex/t19-move-guard-integracao`, `tarefa/G-02-painel-modular`, `tarefa/T12.1-safety-ampliada`,
  `tarefa/F-03-exposicao`, `tarefa/T12.6a-painel-v7`. Conferir se ainda têm algo fora do master
  antes de abrir PR.
- `ESTUDOS/` (não versionado na raiz): dois arquivos de estudo do usuário, guardados no snapshot
  `backup/formatacao-20261021/wip/codex/t12-3-orcamento-global`.

### 5.3 Bloqueios que continuam

1. Ponte mt5x sem IPC (`IPC send failed`): sem Tester, sem histórico multiativo. Depois de
   reinstalar, **uma** sondagem de sincronização com o lease existente; não repetir sem mudança.
2. Ativos B3 ausentes na conta DEMO (C-16, C-17, C-20).
3. Par Python no mt5x para cada Expert novo (exigido pelo Épico C).
4. Validação externa das memórias de cálculo (pedido escrito nos PRs, não enviado).
5. Contrato de publicação do regime (C-21) com o dono do Épico F.
6. C-22 notícia, C-23 fluxo e C-24 pares: bloqueados por infraestrutura do motor.

## 6. Regras de trabalho que valem para qualquer agente

- Nunca operar conta REAL. Ao vivo só em DEMO e com aceite próprio. S09 é só Tester.
- Não instalar automaticamente sobre o terminal principal; não encerrar Wine, terminal, ponte
  ou processos de terceiros.
- Não alterar Pullback (Expert, perfil, evidências) nem, sozinho, `HDK_StrategyRobot`, executor,
  RiskGuard, políticas de posição, contratos centrais, sessão/runner/ponte ou registros centrais:
  propor e coordenar.
- Uma tarefa por PR, branch `codex/`, worktree própria; nada de reset/clean destrutivo ou stash
  global; não trocar a branch da worktree de outro agente.
- Usar o lease/lock existente (`native-tester.lock`); não criar outro sistema de locks.
- Distinguir sempre: implementado / compilado / validado em replay / integrado.
- Não enviar mensagens externas sem autorização.

## 7. Ferramentas

`Tools/agente/` traz os scripts de compilação isolada, criação de worktree, publicação de PR e
ajuste B05 (ver o README de lá).

## 8. Referências de backup publicadas

## Fonte: `Tools/agente/README.md`

# Ferramentas de agente (frente Estratégia T20/T21)

Scripts usados para produzir os PRs #49–#60. São utilitários de **host**, não do produto: dependem
de caminhos locais e precisam ser ajustados depois de uma reinstalação.

| Script | Faz | Depende de |
|---|---|---|
| `hdk_compile.sh <worktree> <rel.mq5>...` | Compila numa árvore MQL5 isolada com prefixo Wine próprio, sob o lock cooperativo `~/.local/state/mt5-extracao/native-tester.lock` | `SRC` = pasta de um terminal MT5 (MetaEditor64.exe + `MQL5/Include` da biblioteca padrão); `wine`, `winepath`, `iconv`, `flock` |
| `t21_new.sh <dir> <branch>` | Cria worktree a partir de `origin/master` em `~/Projects/_worktrees/hadouken/<dir>` | caminho do clone principal |
| `t21_ship.sh <dir> <mq5> <msg> <título> <corpo> [nob05]` | Sobe a contagem B05, roda `unittest` e `check_repo`, compila, faz commit, push e abre o PR | os dois acima e `gh` autenticado |
| `b05_add.py <worktree>` | Soma 1 ao número de consumidores em `Tools/tests/test_position_policy.py` e regenera `Tests/Contracts/HDK_PositionPolicy_Test.mq5` | — |

Depois de reinstalar, ajuste `SRC` em `hdk_compile.sh` para a pasta do MetaTrader 5 instalada (por
exemplo, `~/.local/share/mt5-extracao/wine/drive_c/Program Files/MetaTrader 5`) e os caminhos fixos
`<HOME>/...`.
