# Fase 15 — Gestão de recursos (espaço, higienização e automação)

**Pedido do dono do projeto (2026-10-14):** abrir uma frente paralela ao roadmap com uma aba de gestão de recursos
na interface. O primeiro problema é o espaço: a base pode ficar muito grande. A aba deve monitorar a base e o disco,
higienizar, permitir limpar ativos sem resultado positivo, não usados em estudos ou obsoletos, e oferecer
monitoramento e automação programada **somente se o usuário ligar** (opt-in). Deve ser robusta, resiliente e
preparada para novas frentes (logs, exportações, cópias de segurança).

Branch: `tarefa/T15-gestao-recursos`, em worktree separado, para não interferir nos agentes do roadmap. Os arquivos
novos ficam em `mt5_extracao/resources/` e `mt5_extracao/ui/resources.py`; nos arquivos compartilhados as mudanças
são pontuais (registro da aba, do subcomando e das chaves de configuração).

## Princípios

1. **Nada some sem volta.** "Limpar" um ativo é **arquivar**: a tabela é copiada para um arquivo SQLite próprio em
   `database/arquivo/`, a cópia é conferida (mesmo número de linhas) e só então a tabela sai da base. Restaurar
   devolve a tabela e o registro dela. A regra do AGENTS.md ("nunca apague tabelas de usuários") vale para
   mudanças de schema; aqui a remoção é uma **ação explícita do usuário**, sempre precedida do arquivo.
2. **Protegidos nunca são candidatos:** favoritos, ativos na seleção, ativos com benchmark vinculado, ativos usados
   como benchmark de outro, e séries com backtest lucrativo.
3. **Automação é opt-in e conservadora.** Com `[RESOURCES] auto_enabled = false` (padrão) nada roda sozinho. Ligada,
   cada ação automática tem a sua chave própria, também desligada por padrão, e a automação só arquiva (reversível).
4. **Medir é barato e não trava a interface.** Toda leitura roda em segundo plano; medição pesada (tamanho por
   tabela) usa `dbstat` do SQLite quando disponível e cai para estimativa por linhas quando não.
5. **Uma ação por vez:** trava de arquivo (`database/arquivo/.lock`) impede que a interface e o agendador do sistema
   (cron / Agendador de Tarefas) mexam na base ao mesmo tempo.
6. **Tudo auditado:** cada ação grava em `_resource_actions` (quando, o quê, origem ui/cli/auto, bytes antes e
   depois). Retratos periódicos ficam em `_resource_snapshots` (mudança aditiva: bases antigas continuam abrindo).
7. **Extensível:** as regras de higienização e os "módulos" de medição ficam em registros (`hygiene.RULES`,
   `storage.EXTRA_DIRS`); uma frente nova acrescenta uma entrada, sem mexer na aba.

## Tarefas

| Tarefa | O quê | Estado |
|---|---|---|
| T15.0 | Este plano | ✅ |
| T15.1 | Medição: tamanho da base e de cada tabela, espaço recuperável, disco livre, pastas (logs, exportações, arquivo); retratos e projeção de crescimento | ✅ |
| T15.2 | Higienização: regras `sem_lucro`, `sem_estudo`, `obsoleta`, `vazia` e registros órfãos, com proteções | ✅ |
| T15.3 | Ações seguras: arquivar (com conferência), restaurar, otimizar (VACUUM com checagem de disco), limpar órfãos; auditoria e trava | ✅ |
| T15.4 | `mt5x resources status | scan | archive | restore | archives | vacuum | orphans | auto | history` | ✅ |
| T15.5 | Automação opt-in: política em `[RESOURCES]`, ciclo (medir → alertar → agir), alertas pelo `notify`, agendador na interface e comando para o cron | ✅ |
| T15.6 | Aba "Recursos" na interface: indicadores, Espaço, Higienização, Arquivo, Automação e Histórico | ✅ (vista em tela virtual no Linux; Windows não visto) |
| T15.7 | Próximas frentes: rotação de logs, limpeza de exportações antigas, cópia de segurança agendada, compactação em Parquet, retenção por particionamento no PostgreSQL/TimescaleDB, limites por ativo | proposta |

## Regras de higienização (T15.2)

| Regra | Quando a série é candidata |
|---|---|
| `sem_lucro` | Houve backtest na série e nenhum robô teve lucro no mais recente |
| `sem_estudo` | Nenhum backtest no ativo (em nenhum timeframe) |
| `obsoleta` | Última barra mais velha que `[RESOURCES] obsolete_days` (padrão 180) |
| `vazia` | Tabela sem linhas |

Órfãos: linhas de `_symbol_tables` e `_extraction_log` que apontam para tabelas que não existem mais.

## Configuração (`[RESOURCES]`)

| Chave | Padrão | Uso |
|---|---|---|
| `auto_enabled` | `false` | Liga o monitoramento automático (opt-in) |
| `monitor_interval_minutes` | `60` | Intervalo entre ciclos |
| `alert_db_gb` | `10` | Avisa quando a base passar disso (0 = desligado) |
| `alert_disk_free_pct` | `10` | Avisa quando o disco livre cair abaixo disso (%) |
| `auto_vacuum` | `false` | Otimiza sozinho quando o recuperável passar de `vacuum_min_mb` |
| `vacuum_min_mb` | `200` | Limiar da otimização automática |
| `auto_clean_orphans` | `false` | Remove registros órfãos sozinho |
| `auto_archive` | `false` | Arquiva sozinho as séries das regras em `auto_archive_rules` |
| `auto_archive_rules` | `obsoleta,vazia` | Regras que a automação pode arquivar |
| `obsolete_days` | `180` | Idade para a regra `obsoleta` |
| `archive_dir` | vazio | Pasta do arquivo (vazio = `arquivo/` ao lado da base) |
| `keep_snapshots_days` | `365` | Quanto histórico de medições guardar |

## Validação

- Testes em `tests/test_resources.py` (medição, regras, arquivar/restaurar ida e volta, trava, automação, CLI).
- Windows: só simulado. Pendências no checklist de `docs/validacao_instaladores.md` (T10.W).
