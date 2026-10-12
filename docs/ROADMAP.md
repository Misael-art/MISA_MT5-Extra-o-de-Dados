# Roadmap: construtor de bases MT5 completo (Windows + Linux)

Visão: com um duplo clique (Windows) ou `./install.sh` (Linux), o usuário instala
tudo e constrói e mantém bases históricas confiáveis (OHLCV, indicadores, ticks)
do MetaTrader 5, pela interface gráfica ou por linha de comando agendada.

O detalhamento de cada tarefa (arquivos, passos, código de referência, critérios
de aceite e testes) está em [PLANO_DE_TRABALHO.md](PLANO_DE_TRABALHO.md).
As regras para agentes estão em [../AGENTS.md](../AGENTS.md).

## Fases

```
F0 Fundação ──► F1 Integridade dos dados ──► F3 CLI/automação ──► F5 Escala e novos dados
    │                                             ▲
    └─────────► F2 Núcleo desacoplado + Linux ────┘   F6 Estratégias e triagem (usa a base de F1)
                         │
                         └────────► F4 Qualidade e manutenção (contínua)
```

| Fase | Objetivo | Tarefas | Depende de | Status |
|---|---|---|---|---|
| **F0** Fundação | Instalação automática Win/Linux, configuração inicial, diagnóstico, CI | T0.1–T0.3 | — | T0.1 ✅, T0.2 ✅, T0.3 ⏳ (validação manual) |
| **F1** Integridade | Reextrair sem erro, retomar extrações, atualização incremental, dados corretos | T1.1–T1.8 | F0 | ✅ |
| **F2** Núcleo + Linux | Núcleo sem MetaTrader5/Tkinter no import; o app funciona no Linux via ponte | T2.1–T2.6 | F0 | T2.1–T2.5 ✅, T2.6 ⏳ (paridade com MT5 real) |
| **F3** CLI e automação | `mt5x extract/update/export/doctor`, agendamento, `pyproject.toml` | T3.1–T3.4 | F1, F2 | ✅ (agendamento: cron e schtasks) |
| **F4** Qualidade | Limpeza de arquivos, logging central, testes com provedor falso, docs | T4.1–T4.5 | F0 (pode ir em paralelo) | ✅ |
| **F5** Escala | Parquet/DuckDB, PostgreSQL/TimescaleDB, ticks, book, fonte externa real | T5.1–T5.6 | F1, F3 | ✅ (TimescaleDB: hypertable automática, ainda não testada em servidor com a extensão) |
| **F6** Estratégias | Seis estratégias, backtest com custos, walk-forward/Monte Carlo (Filtro C), triagem de ativos (Filtros A e B), relatório HTML, menu na GUI | T6.1–T6.8 | F1, F3 | ✅ |
| **F7** Interface | Janela principal em abas (Ativos, Minha base, Análise, Atividade), tema claro/escuro, gráficos e números de estudo | T7.1 | F1, F6 | ✅ |
| **F8** Robôs | Catálogo de robôs (MQL5 e Python) em cards, backtest, bateria de segurança e operação em demo/real pelo robô MQL5 | T8.0–T8.9 | F6, F7 | T8.0–T8.6 ✅; T8.7 ⏳ (só simulada, falta validar com o MT5 real em demo); demais a fazer |
| **F9** Agentes e segurança | Operação sem tela (`--json`, `status`, `analyze`), auditoria `mt5x audit` e ponte do Linux restrita a funções de leitura | T9.1–T9.3 | F3, F8 (T8.3) | ✅ |

## Marcos (cada marco = versão publicável)

| Marco | Versão | Critério de pronto |
|---|---|---|
| M0 — Instala sozinho | 0.2.0 | `install.bat` e `install.sh` concluem em máquinas limpas (checklist T0.3); CI verde |
| M1 — Base confiável | 0.3.0 | T1.1–T1.6 concluídas; reextrair o mesmo período não duplica nem falha; extração interrompida retoma de onde parou |
| M2 — Linux de verdade | 0.4.0 | T2.1–T2.5; extração M1 de 1 mês no Linux gera o mesmo resultado que no Windows (teste de paridade) |
| M3 — Sem cliques | 0.5.0 | T3.1–T3.3; `mt5x update` agendado via cron / Agendador de Tarefas mantém a base em dia |
| M4 — Manutenível | 0.6.0 | T4.1–T4.4; cobertura de testes ≥ 60% no núcleo (`database_manager`, `historical_extractor`, `mt5_backend`) |
| M5 — Escala | 1.0.0 | T5.1–T5.4; exportação Parquet, TimescaleDB opcional, ticks |

## Ordem recomendada de execução (uma tarefa = um PR)

1. T0.3 (validação manual dos instaladores; pode ser feita por um humano)
2. T1.1 → T1.5 → T1.4 → T1.2 → T1.3 → T1.6 → T1.7 → T1.8
3. T2.1 → T2.2 → T2.3 → T2.4 → T2.5 → T2.6
4. T4.3 (provedor falso; facilita testar as fases seguintes) → T4.1 → T4.2
5. T3.3 → T3.1 → T3.2 → T3.4
6. T4.4, T4.5
7. T5.x conforme a necessidade
8. F6 (T6.1 → T6.8) já concluída; novas estratégias seguem o roteiro em docs/estrategias.md ("Para desenvolvedores")
9. F7 (T7.1) já concluída; novos painéis entram em `mt5_extracao/ui/`
10. F8: T8.0 → T8.1 → T8.2 → T8.3 → T8.4 → T8.5 → T8.6 → T8.7 → T8.8 (T8.9 em paralelo depois da T8.5)
11. F9 (T9.1 → T9.3) já concluída

As fases F1 e F2 são independentes e podem ser executadas em paralelo por
agentes diferentes, **desde que não editem os mesmos arquivos ao mesmo tempo**
(conflitos esperados: `mt5_connector.py`, `historical_extractor.py`).

## Fora do escopo (por ora)

- macOS: o pacote MetaTrader5 não existe e o Wine no macOS é instável. A arquitetura da ponte (F2) permitiria suportá-lo no futuro.
- Envio de ordens **pelo Python**: o código deste projeto nunca chama `order_send`. A partir da F8 o aplicativo
  instala, inicia, monitora e para robôs MQL5, e quem opera é o robô dentro do MT5, pela camada `Safety/` do
  HADOUKEN (veja [robos.md](robos.md)). Conta real fica desligada por padrão.
- Bases em nuvem gerenciadas (AWS RDS etc.): basta apontar T5.2 para o host.
