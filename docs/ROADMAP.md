# Roadmap: construtor de bases MT5 completo (Windows + Linux)

Visão: com um duplo clique (Windows) ou `./install.sh` (Linux), o usuário instala
tudo e constrói e mantém bases históricas confiáveis (OHLCV, indicadores, ticks)
do MetaTrader 5, pela interface gráfica ou por linha de comando agendada.

A partir da Fase 10 o projeto é também um **SDK para desenvolver robôs MQL5 de alto padrão**: um contrato
escrito, um ponto de partida (`mt5x robots new`), um ciclo de desenvolvimento em um comando e uma API estável.
O aplicativo amigável (automação, análise e gestão) continua sendo o mesmo; o SDK é uma camada a mais.

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
| **F10** SDK de robôs | Contrato do robô, biblioteca de segurança MQL5 versionada, `robots new`, ciclo `robots check`, bateria completa para MQL5, paridade Python × MQL5, API pública, documentação e pacote; como funções opcionais, geração de MQL5 a partir de uma estratégia declarada em Python e preparação para o MQL5 Market | T10.0–T10.12 + T10.W | F8 (T8.4–T8.6), F9 | T10.0–T10.8 e T10.10 ✅; demais a fazer (T10.9 pacote, T10.11 Market, T10.12 custo do backtester, T10.W Windows). Lacunas conhecidas em docs/sdk/exatidao.md; a do custo do backtester Python virou a T10.12. Opcionais: T10.10 (gerar MQL5 a partir de Python) e T10.11 (preparar para o MQL5 Market). **T10.W (validação no Windows) é pendência: não bloqueia o desenvolvimento no host atual** |
| **F11** Testes amplos e operação discreta | Montar um timeframe maior a partir de uma base menor (M5 de M1...), backtest em vários ativos de uma vez, MT5 oculto (sem ocupar a tela) e avisos em pop-up no computador a cada ação executada | T11.0–T11.4 | F1, F8, F10 | T11.0 ✅; demais a fazer |

## Marcos (cada marco = versão publicável)

| Marco | Versão | Critério de pronto |
|---|---|---|
| M0 — Instala sozinho | 0.2.0 | `install.bat` e `install.sh` concluem em máquinas limpas (checklist T0.3); CI verde |
| M1 — Base confiável | 0.3.0 | T1.1–T1.6 concluídas; reextrair o mesmo período não duplica nem falha; extração interrompida retoma de onde parou |
| M2 — Linux de verdade | 0.4.0 | T2.1–T2.5; extração M1 de 1 mês no Linux gera o mesmo resultado que no Windows (teste de paridade) |
| M3 — Sem cliques | 0.5.0 | T3.1–T3.3; `mt5x update` agendado via cron / Agendador de Tarefas mantém a base em dia |
| M4 — Manutenível | 0.6.0 | T4.1–T4.4; cobertura de testes ≥ 60% no núcleo (`database_manager`, `historical_extractor`, `mt5_backend`) |
| M5 — Escala | 1.0.0 | T5.1–T5.4; exportação Parquet, TimescaleDB opcional, ticks |
| M6 — SDK no host atual | 1.1.0 | T10.1–T10.8 concluídas e verificadas no Linux (Wine); um robô criado com `mt5x robots new` passa por `mt5x robots check` do zero |
| M7 — SDK publicado | 1.2.0 | M6 + T10.W (checklist do Windows cumprido) + T10.9 (pacote publicado) |

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
12. F10, em três trilhas que podem andar em paralelo (arquivos diferentes):
    - ciclo e qualidade: T10.1 → T10.4 → T10.5
    - lado MQL5: T10.2 → T10.3 → T10.6
    - superfície pública: T10.7 → T10.8 → T10.9 (a T10.9 só depois da T10.W)
    - opcionais, depois das trilhas: T10.10 (depende de T10.3 e T10.6) e T10.11 (depende de T10.4)
    - F11, pedida pelo dono do projeto antes da T10.11: T11.1 → T11.2 → T11.3 → T11.4
    - T10.W quando houver uma máquina Windows; até lá, cada tarefa acrescenta o que falta conferir ao
      checklist em [validacao_instaladores.md](validacao_instaladores.md)

As fases F1 e F2 são independentes e podem ser executadas em paralelo por
agentes diferentes, **desde que não editem os mesmos arquivos ao mesmo tempo**
(conflitos esperados: `mt5_connector.py`, `historical_extractor.py`).

## Fora do escopo (por ora)

- macOS: o pacote MetaTrader5 não existe e o Wine no macOS é instável. A arquitetura da ponte (F2) permitiria suportá-lo no futuro.
- Envio de ordens **pelo Python**: o código deste projeto nunca chama `order_send`. A partir da F8 o aplicativo
  instala, inicia, monitora e para robôs MQL5, e quem opera é o robô dentro do MT5, pela camada `Safety/` do
  HADOUKEN (veja [robos.md](robos.md)). Conta real fica desligada por padrão. O SDK (F10) não muda isso:
  ele ajuda a escrever o robô MQL5; o envio de ordens continua sendo só do robô.
- Tradução de código Python **livre** para MQL5: a T10.10 gera MQL5 só a partir de estratégias **declaradas**
  (indicadores e condições de um conjunto suportado). `gerar_sinais` escrito à mão continua sendo portado por quem
  desenvolve.
- Envio automático ao MQL5 Market e licenciamento: a T10.11 confere e empacota; o envio é feito pela pessoa, na
  área do vendedor do MQL5.com.
- Bases em nuvem gerenciadas (AWS RDS etc.): basta apontar T5.2 para o host.
