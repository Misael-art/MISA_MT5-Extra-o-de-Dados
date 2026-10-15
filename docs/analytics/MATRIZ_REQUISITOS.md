# Matriz de requisitos — ciclo de pesquisa de contexto (Fases 13 e 16)

Levantamento inicial de 2026-10-14, feito sobre o `master` em `880bf18` mais a T13.2 local (`codex/t13.2-feature-store`).
Serve para não duplicar componentes: cada requisito aponta para o que já existe e só abre tarefa para a lacuna.
As tarefas citadas estão em [PLANO_DE_TRABALHO.md](../PLANO_DE_TRABALHO.md) (Fase 13) e em
[propostas/ANALYTICS_OPERACOES.md](../propostas/ANALYTICS_OPERACOES.md) (Fase 16).

Atualização de 2026-10-15: T13.3 está na branch local `tarefa/T13.3-perfis-regimes` (`bde3652`), T13.3b na
`tarefa/T13.3b-producers-incrementais` (`4ede7fa`, documentação final `4ecbaca`), e T13.4 na
`tarefa/T13.4-hipoteses-walk-forward` (`ae30450`). T16.3b está em `tarefa/T16.3b-enriquecimento-temporal`
(`5d85cf3`) e T16.3c em `tarefa/T16.3c-calendario-point-in-time` (`fc24bde`). Nenhuma dessas branches foi integrada
ao `master`. T13.3b passou a suíte final com 848 aprovados e 10 ignorados; T13.4 segue parcial por custos e dados
reais ausentes. T16.3c implementa as revisões, mas replay com recibos de fonte real está pendente. Evidências:
[T13.3_PERFIS_REGIMES.md](T13.3_PERFIS_REGIMES.md), [T13.4_RELATORIO.md](T13.4_RELATORIO.md) e
[continuidade.md](../continuidade.md). A Decisão 5 permanece aberta.

Ciclo: dados → características → hipóteses → validação → comparação de filtros → publicação informativa →
modo sombra → pilotos autorizados → monitoramento.

## 1. Dados e tempo

| Requisito | Implementação existente | Lacuna | Tarefa | Dependência | Critério de aceite | Estado nesta retomada (evidência ou pendência) |
|---|---|---|---|---|---|---|
| Barras por sessão, sem barras fantasmas | `analytics/calendar.py`, `analytics/resampling.py` (`_market_calendar`, `_bars_derived`) | Calendário é inferido do M1; offset fixo, sem horário de verão | T13.1 ✅ | — | Testes 1–3 e 5 da especificação | **Entregue no Linux**; reconstrução, feriados, DST, Windows e MT5 real continuam como limites registrados em T13.1. |
| Fato com observação, disponibilidade, validade, revisão | `analytics/feature_store.py` e `save_values` | — | T13.2/T13.3 | T13.2 | Contrato, lote atômico e duplicatas testados | **Entregue localmente** nas branches T13.2/T13.3; PostgreSQL e Windows real pendentes. |
| Consulta *as-of* | `query_as_of`, `query_history`, `asof_join` | — | T13.2 | T13.2 | Revisões preservadas; decisões só veem `available_at <= t` | **Entregue localmente**; testes T13.2 registrados na continuidade; não integrado ao `master`. |
| Barra superior aberta ou incompleta não conta como fechada | `latest_bars(closed_only=True)` e `available_at = bar_end_utc` | — | T13.2 | T13.2 | H1 não aparece antes de fechar; parcial, cobertura incompleta e barra futura excluídas | **Entregue localmente** e coberto por testes point-in-time; não integrado ao `master`. |
| Correção tardia e proveniência das características | Revisões em `_bars_derived`, janelas por sessão e fingerprint `source_revision` | Memória de Wilder é aproximada; estado/duração do regime dependem do prefixo histórico | T13.3 | T13.1, T13.2 | Disponibilidade não antecipa correção; limite e propagação testados | **Entregue com limite documentado** na T13.3; a aproximação não é garantia de memória exata. |
| Volume real ≠ volume de ticks | `_bars_derived.real_volume_known`; enriquecimento temporal corrigido | — | T13.3/T16.3b | — | Fonte declarada; séries nunca misturam fontes | **Entregue localmente** em T13.3 e T16.3b; testes das duas frentes registrados, branches ainda não integradas. |
| Contexto do Laboratório só com informação anterior | Enriquecimento só usa barras fechadas e eventos conhecidos até a entrada | Resolução M1 ainda limita ordem intrabar e precisão de MAE/MFE | T16.3b | — | Armadilhas de entrada no meio da barra e notícia posterior | **Entregue localmente** em `5d85cf3`; aproximações intrabar permanecem explícitas; branch não integrada. |
| Fato × divulgação × recebimento × revisão (economia) | T16.3c mantém disponibilidade observada e histórico aditivo de revisões | A base histórica não contém recibos confiáveis; importações antigas não reconstroem a disponibilidade original | T16.3c | T16.3a/T16.3b | Usar `actual` só após disponibilidade comprovada; preservar revisões e desconhecidos | **Implementação entregue localmente** em `fc24bde`; testes sintéticos cobrem `load_asof` e revisões. Replay real segue pendente até haver fonte/exportação com timestamps confiáveis. Não inferir recepção de `event_time` ou de importação histórica posterior. |
| Série contínua ajustada nunca executada | Decisão em aberto 2 do plano | Sem ativos B3 contínuos na base | Decisão 2 | Dono do projeto | Nunca executar em preço ajustado | **Pendente de decisão e dados**; não há ativo B3 contínuo disponível para validar. |

## 2. Características (T13.3)

| Requisito | Existente | Lacuna | Tarefa | Critério de aceite | Estado nesta retomada (evidência ou pendência) |
|---|---|---|---|---|---|
| Retornos, gap, pregão anterior, movimento após abertura | `study.seasonality` e `session_structure.py` | Reconstrução usa calendário observado | T13.3 | Gap disponível só no fim da 1ª barra da sessão | **Entregue localmente**; coberto por testes sintéticos e base EURUSD/AGG; limitações em T13.3. |
| Volume: médias/medianas 5–60 sessões, inclinação, percentil | `volume_profiles.py` | — | T13.3 | Só sessões anteriores completas; sintético e AGG medidos | **Entregue localmente**; zero real conhecido permanece separado de volume ausente. |
| RVOL no mesmo horário | `volume_profiles.py` | — | T13.3 | Mediana de sessões anteriores no mesmo minuto; aquecimento/denominador zero = ausente | **Entregue localmente**; evidência sintética e EURUSD/AGG consta em T13.3. |
| Volatilidade, compressão/expansão | `strategies.indicators`, `market_regimes.py` | — | T13.3 | Percentil causal e correção tardia testados | **Entregue localmente**; limite de memória do indicador está documentado. |
| Regime TREND/RANGE/TRANSITION/UNCERTAIN | `asset_screener`, `market_regimes.py` | — | T13.3 | Paridade para duração 1; duração mínima 3 e aquecimento incerto | **Entregue localmente**; evidência e incerteza da Decisão 5 registradas em T13.3. |
| Faixa inicial, distância de níveis anteriores | `session_structure.py` | — | T13.3 | Disponibilidade após N minutos; cruzamento de duas pontas ambíguo | **Entregue localmente**; sessões/lacunas seguem calendário observado, não calendário oficial. |
| Sazonalidade (dia, semana do mês, hora, minuto da sessão) | `enrich.calendar_columns`, `calendar_rows` | `study.seasonality` agrega a amostra inteira e não é causal | T13.3 | Colunas reutilizadas; resumo global só descritivo | **Entregue localmente**; agregados não causais não são usados como sinal point-in-time. |
| "Confiança" por característica | Proposta registrada nos parâmetros | Decisão em aberto 5 | T13.3 | Definição explícita; dono confirma | **Pendente de decisão do dono**; nenhuma medida foi tratada como probabilidade. |

## 3. Hipóteses e validação (T13.4)

| Requisito | Existente | Lacuna | Tarefa | Critério de aceite | Estado nesta retomada (evidência ou pendência) |
|---|---|---|---|---|---|
| Hipótese declarativa (universo, sessão, fuso, condição, gatilho, entrada, saída, custos, parâmetros, versão, validação) | `hypotheses.py`, catálogo JSON H01–H16, `pattern_engine.py`, `pattern_extensions.py` | Limiares e resultados OOS H06–H16 não medidos; H14/H15 têm apenas eventos candidatos em relatórios reutilizados sem base temporal declarada; H16 não tem sinais | T13.4 | Arquivos JSON validados; sem coluna por descoberta | **Pendente de evidência OOS e eventos válidos**: H01–H16 têm builders; H06–H16 seguem `DRAFT`, sem elegibilidade. H06–H13 têm só contagens descritivas EURUSD/AGG. Os relatórios atuais contêm eventos candidatos para H14/H15, mas 58 referências resolvem para 9 arquivos compartilhados sem snapshot por run nem base temporal declarada; 161 entradas EURUSD alinham por minuto e preço com M1 sob hipótese não confirmada e geram somente 156/132 observações exploratórias. Custos de comissão/slippage seguem ausentes; H16 não tem sinais explícitos. |
| Separação cronológica, purging/embargo | `rules.split` usa duração máxima de `exit_time - entry_time` | Sem prova de sobreposição arbitrária além da duração declarada/observada | T13.4 | Teste sintético com horizonte sobreposto | **Entregue para H01–H05**; teste verifica que o fim do rótulo precede a janela de checagem. |
| Seleção de parâmetros dentro do treino | Seleção por maior expectativa líquida na partição de busca | — | T13.4 | Reserva nunca toca a seleção | **Entregue**; teste altera resultados da reserva e confirma seleção invariável. |
| Inferência com dependência temporal | Bootstrap móvel por blocos de dias e wild bootstrap centrado na hipótese nula | Incerteza de blocos com poucas sessões pode permanecer grande | T13.4 | Bootstrap temporal e taxa-base | **Entregue**; ruído e amostras pequenas não são tratados como suficiência ou probabilidade de lucro. |
| Múltiplas comparações | Bonferroni sobre candidatos/avaliações; `_pattern_evaluations` registra o início antes do cálculo, exceções como `FAILED` e rejeições concluídas | Interrupção abrupta deixa estado `RUNNING`; precisa de reconciliação manual; família é conservadora | T13.4 | Contagem de tentativas exibida | **Entregue para auditoria de tentativas**; erro Python é persistido e reserva consumida antes do cálculo. Processo encerrado à força deixa `RUNNING` visível e não libera a reserva. |
| Reserva final protegida contra reutilização | `_pattern_reservations` registra primeira avaliação por hipótese/versão/mercado/TF | Histórico de estado/aprovador pertence à governança posterior | T13.4/T13.8 | Segunda avaliação é marcada reutilizada | **Entregue para avaliação**; reuso não produz segundo resultado OOS nem permite aprovação. Trilha de aprovação fica para T13.8. |
| Custos, atraso, perturbação de parâmetros | Sensibilidade em validação a multiplicadores de custo, atrasos 0/1/3 barras e grade de parâmetros | Comissão/slippage ausentes; `real_spread` em `_robot_runs` é marcador booleano, e não custo monetário; atraso limitado às amostras com barra executável | T13.4 | Tabela de sensibilidade | **Entregue com limites publicados** em T13.4_RELATORIO.md; o perfil de spread disponível não completa os custos por operação nem permite expectativa líquida completa. |
| Estabilidade por período e mercado; concentração | Métricas por ano e participação dos 5% maiores resultados; `summarize_market_stability` | Nenhum grupo real de mercados comparáveis declarado; EURUSD e AGG não foram combinados | T13.4 | Estabilidade e concentração | **Pendente de avaliações reais em mercados comparáveis**: o agregador foi testado sinteticamente e resume apenas hipótese/versão/timeframe iguais; não estima significância nem aprovação. A base local não fornece grupo comparável declarado para preenchê-lo. |
| Testes contra falsos positivos | Geração nula determinística por semente e portão de aprovação | Amostra sintética não prova taxa zero nem impossibilidade de falso positivo | T13.4 | 40 repetições nulas; limite de tolerância para falso sinal ajustado e elegibilidade | **Entregue em teste sintético**; com custos completos, o teste limita cada contagem a 6/40 sob α nominal de 5%. Isso não estabelece taxa de falso positivo zero. |
| MAE/MFE, recuperação, atraso de entrada | T16.3b corrige disponibilidade/limite temporal; H14/H15 recebem `trade_events`; H16 recebe `signal_events` e calcula atrasos 0/1/3 | Ledger sem entrada/direção; snapshots `_robot_runs` e exports MQL5 legados sem entrada; relatórios Tester sem snapshot por run/base temporal declarada; sem sinais explícitos; resolução limitada a M1 | T13.4/T16.3b | Resolução de saída e recuperação declaradas | **Pendente de eventos válidos para avaliação**: builders e contratos passaram em fixtures sintéticas. Relatórios Tester atuais fornecem candidatos para H14/H15, mas 58 referências compartilham 9 caminhos; sob alinhamento temporal não confirmado, 161 eventos EURUSD coincidem por minuto/preço com M1 e geram 156/132 observações exploratórias. Ledger e exports não completam a proveniência; custos de comissão/slippage faltam e H16 segue sem sinais. A aproximação intrabarra permanece limitada a M1/OHLC; as hipóteses seguem `DRAFT`. |
| Risco simultâneo entre estratégias | `aggregate.daily_correlation`, `robots/portfolio.py` (exposição F-03) | Sobreposição de posições abertas | T13.7 | — | **Pendente** de T13.7; correlação diária e orçamento não substituem exposição simultânea intraposição. |

## 4. Comparação de filtros

| Requisito | Existente | Lacuna | Tarefa | Critério de aceite | Estado nesta retomada (evidência ou pendência) |
|---|---|---|---|---|---|
| Original × filtrado (líquido, expectativa, drawdown, exposição, custos, n, estabilidade) | `rules.evaluate`, guia Insights do Laboratório | Contrato integrado e comparação visual | T13.6 | Rótulo "simulado" em todo resultado filtrado | **Pendente** de T13.6 e T13.5; o relatório T13.4 declara que não simula execução nem reexecução de carteira. |
| Filtro que muda estado/capital/sinais seguintes exige re-execução completa | `rules` filtra um livro fixo | Reexecução completa pelo backtester | T13.7 | Só afirmar equivalência após reexecução | **Pendente** de T13.7; filtragem de ledger fixo não é equivalente. |
| Registro de todos os sinais, inclusive bloqueados | — | `_signal_log` e integração do leitor | T13.7a | Instante, estratégia, direção, contexto, versão, decisão e motivo | **Pendente de decisão do dono** para incluir T13.7a e da T13.5; nenhuma estratégia é alterada. |

## 5. Publicação, sombra, pilotos, monitoramento

| Requisito | Existente | Lacuna | Tarefa | Dependência | Critério de aceite | Estado nesta retomada (evidência ou pendência) |
|---|---|---|---|---|---|---|
| Snapshot versionado, procedência, validade, hash, troca atômica, rejeição | `mt5_tools.common_files_dir` | Ainda sem publicador/leitor | T13.5 | T13.3, T12.1 | Testes 9, 11, 14 | **Pendente**: T12.1 e decisões de formato/Strategy Tester/validade precisam ser resolvidas antes da implementação MQL5. |
| Analytics desligado preserva comportamento | — | Teste e integração do robô | T13.7 | T13.5 | Teste 10 | **Pendente** de T13.5/T13.7; nenhum código de robô foi alterado. |
| Modo sombra (decide, não age) | — | — | T13.7a | T13.5 | Nenhuma ordem afetada; sinais registrados | **Pendente de decisão** sobre adicionar T13.7a ao plano e dependente de T13.5. |
| Piloto opt-in que só reduz risco | `mt5x-safety` (guardião, orçamento) | Política e evidência Strategy Tester | T13.7 | T13.5, T13.7a, T12.3, robô aprovado, autorização específica | Só reduz; bateria sem regressão | **Pendente** de dependências e autorização específica; conta real não autorizada. |
| Monitoramento e rebaixamento | — | Implementação completa, trilha, retenção e backup | T13.8 | T13.4, T13.7 | Motivo e trilha gravados | **Pendente** de T13.7; ainda sem trilha de ator/motivo de transição ou política de backup específica. |
| Jobs idempotentes, checkpoint, recursos | `producer_jobs.py`, checkpoint transacional por família, cursor + sequência da fonte, limites e runs medidos | PostgreSQL e Windows reais não foram executados | T13.3b ✅ (branch local) | T13.3, T15.9 | Falha e retomada sem duplicar/pular; correção tardia iguala recomputação completa; novas barras leem só delta/cauda limitada; métricas em [relatório](T13.3B_PRODUTORES_INCREMENTAIS.md) | **Entregue localmente** em `4ede7fa`; testes de falha/retomada e correção tardia passaram; suíte final 848/10 e doctor 16 OK/2 avisos/0 falhas. PostgreSQL/Windows reais pendentes. |

## Questões para o dono do projeto (não bloqueiam T13.3 a T13.4)

1. **Decisão 5 (confiança):** **pendente do dono**; a proposta T13.3 mede concordância dos indicadores para regime e
   número de sessões para RVOL. Nenhuma medida é probabilidade. Não bloqueia o núcleo de T13.4.
2. **T13.7a (modo sombra):** **pendente do dono** para confirmar a nova tarefa entre T13.5 e T13.7.
3. **Pilotos (T13.7):** **pendentes de autorização específica** para cada piloto demo; conta real não autorizada.
