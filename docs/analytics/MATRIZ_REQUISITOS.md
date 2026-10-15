# Matriz de requisitos — ciclo de pesquisa de contexto (Fases 13 e 16)

Levantamento de 2026-10-14, feito sobre o `master` em `880bf18` mais a T13.2 local (`codex/t13.2-feature-store`).
Serve para não duplicar componentes: cada requisito aponta para o que já existe e só abre tarefa para a lacuna.
As tarefas citadas estão em [PLANO_DE_TRABALHO.md](../PLANO_DE_TRABALHO.md) (Fase 13) e em
[propostas/ANALYTICS_OPERACOES.md](../propostas/ANALYTICS_OPERACOES.md) (Fase 16).

Ciclo: dados → características → hipóteses → validação → comparação de filtros → publicação informativa →
modo sombra → pilotos autorizados → monitoramento.

## 1. Dados e tempo

| Requisito | Implementação existente | Lacuna | Tarefa | Dependência | Critério de aceite |
|---|---|---|---|---|---|
| Barras por sessão, sem barras fantasmas | `analytics/calendar.py`, `analytics/resampling.py` (`_market_calendar`, `_bars_derived`) | Calendário é inferido do M1 (reconstrução histórica); offset fixo, sem horário de verão | T13.1 ✅ (limitações registradas) | — | Testes 1–3 e 5 da especificação |
| Fato com observação, disponibilidade, validade, revisão | `analytics/feature_store.py` (T13.2) | Gravação em lote (uma transação por valor é lenta para séries longas) | T13.3 acrescenta `save_values` | T13.2 | Mesmo contrato de `save_value`, testado |
| Consulta *as-of* | `feature_store.query_as_of` | Junção *as-of* de uma série de instantes de decisão (só há consulta por um instante) | T13.3 (`asof_join`) | T13.2 | Cada decisão vê só `available_at <= t`; teste entre timeframes |
| Barra superior aberta não conta como fechada | `available_at = bar_end_utc` nos produtores | — | T13.3 | T13.2 | Teste: H1 das 10:00 não aparece às 10:30 |
| Volume real ≠ volume de ticks | `_bars_derived.real_volume_known` | `trade_analytics/enrich.py` mistura real e tick barra a barra e trata real `0` como ausente | **T16.3b** | — | Fonte do volume declarada; nunca misturada na mesma série |
| Contexto do Laboratório só com informação anterior | `enrich.py` usa `time < entry_time` | Usa a barra **aberta** quando a entrada não cai na abertura da barra; surpresa de notícia posterior à entrada vaza | **T16.3b** | — | Testes de vazamento com entrada no meio da barra e com notícia depois da entrada |
| Fato × divulgação × recebimento × revisão (economia) | `_economic_events` guarda horário do evento e `actual` | Não guarda quando o `actual` foi recebido nem revisões | Registrado; entra quando houver característica econômica (fora da T13.3) | T16.3a | `actual` só usado após o recebimento |
| Série contínua ajustada nunca executada | Decisão em aberto 2 do plano | Sem ativos B3 contínuos na base | Fica na decisão 2 | Dono do projeto | — |

## 2. Características (T13.3)

| Requisito | Existente | Lacuna | Tarefa | Critério de aceite |
|---|---|---|---|---|
| Retornos, gap, pregão anterior, movimento após abertura | `study.seasonality` (agregado, sem *as-of*) | Por sessão, com disponibilidade | T13.3 | Gap disponível só no fim da 1ª barra da sessão |
| Volume: médias/medianas 5–60 sessões, inclinação, percentil | — | Tudo | T13.3 | Só sessões anteriores; série sintética de resposta conhecida |
| RVOL no mesmo horário | Fórmula na §5 da especificação | Tudo | T13.3 | Mediana das sessões anteriores no mesmo minuto da sessão; amostra insuficiente = ausente, nunca 1,0 |
| Volatilidade, compressão/expansão | `strategies/indicators.py` (ATR, Bollinger, percentil) | Percentil só com janela anterior | T13.3 (reutiliza `indicators`) | Percentil causal |
| Regime TREND/RANGE/TRANSITION/UNCERTAIN | `asset_screener` (ADX 25/20 na última barra) | Confiança, duração mínima, estado incerto | T13.3 | Mesmo cálculo da triagem com duração 1; diferença documentada |
| Faixa inicial, distância de níveis anteriores | — | Tudo | T13.3 (faixa inicial e máx./mín. da sessão anterior) | Disponível só após N minutos |
| Sazonalidade (dia, semana do mês, hora, minuto da sessão) | `enrich.calendar_columns`, `study.seasonality` | Minuto da sessão pelo calendário observado | T13.3 reutiliza `calendar_columns` | — |
| "Confiança" por característica | Decisão em aberto 5 | Definição | T13.3 propõe por característica; **dono confirma** | Texto de cada definição registrado nos `parameters` |

## 3. Hipóteses e validação (T13.4)

| Requisito | Existente | Lacuna | Tarefa | Critério de aceite |
|---|---|---|---|---|
| Hipótese declarativa (universo, sessão, fuso, condição, gatilho, entrada, saída, custos, parâmetros, versão, validação) | `trade_analytics/rules.py` (regras sobre operações, não sobre mercado) | Formato declarativo sobre características | T13.4 (`hypotheses.py`) | Arquivo JSON validado; sem coluna por descoberta |
| Separação cronológica, purging/embargo | `rules.split` (folga = duração máxima) | Embargo por horizonte do rótulo | T13.4 reutiliza `rules.split` e acrescenta embargo | Teste sintético com horizonte sobreposto |
| Seleção de parâmetros dentro do treino | `insights.discover` (cada prova só vê a sua procura) | Para hipóteses de mercado | T13.4 | Reserva nunca tocada na seleção (teste) |
| Inferência com dependência temporal | `trade_analytics/stats.py` (bootstrap em blocos de dias) | — | T13.4 reutiliza | — |
| Múltiplas comparações | `insights` (Benjamini-Hochberg) | Registro persistente de **todas** as tentativas | T13.4 (`_pattern_evaluations` com cada tentativa) | Contagem de tentativas exibida |
| Reserva final protegida contra reutilização | `rules.split` reserva final | Trava de uso: reserva consumida fica registrada | T13.4 | Segunda avaliação na mesma reserva é marcada "reutilizada" |
| Custos, atraso, perturbação de parâmetros | Re-simulação T16.6 (custos), `rules` | Atraso de entrada e grade de perturbação para hipóteses | T13.4 | Tabela de sensibilidade |
| Estabilidade por período e mercado; concentração | `stats` (por ano no Laboratório) | Concentração (participação dos maiores resultados) | T13.4 | — |
| Testes contra falsos positivos | `insights` (60 sorteios de ruído) | Para hipóteses de mercado | T13.4 | Ruído puro não aprova |
| MAE/MFE, recuperação, atraso de entrada | `enrich` (MAE/MFE) | Tempo de recuperação; MAE/MFE com a barra da saída inteira (superestima) | T16.3b documenta; tempo de recuperação na T13.4 | — |
| Risco simultâneo entre estratégias | `aggregate.daily_correlation`, `robots/portfolio.py` (exposição F-03) | Sobreposição de posições abertas | T13.7 | — |

## 4. Comparação de filtros

| Requisito | Existente | Lacuna | Tarefa | Critério de aceite |
|---|---|---|---|---|
| Original × filtrado (líquido, expectativa, drawdown, exposição, custos, n, estabilidade) | `rules.evaluate`, guia Insights do Laboratório | Exposição e custos lado a lado; marcação "contrafactual simulado" | T13.6 (tela) | Rótulo "simulado" em todo resultado filtrado |
| Filtro que muda estado/capital/sinais seguintes exige re-execução completa | `rules` filtra um livro fixo | Avisar que filtrar livro fixo ≠ re-executar; re-execução pelo backtester | T13.7 | Comparação só apresentada como equivalente após re-execução |
| Registro de todos os sinais, inclusive bloqueados | — | Tudo (`_signal_log`) | **T13.7a** (nova, modo sombra) | Instante, estratégia, direção, contexto, versão da regra, decisão, motivo |

## 5. Publicação, sombra, pilotos, monitoramento

| Requisito | Existente | Lacuna | Tarefa | Dependência | Critério de aceite |
|---|---|---|---|---|---|
| Snapshot versionado, procedência, validade, hash, troca atômica, rejeição | `mt5_tools.common_files_dir` | Tudo | T13.5 | T13.3, T12.1 | Testes 9, 11, 14 |
| Analytics desligado preserva comportamento | — | — | T13.7 | T13.5 | Teste 10 |
| Modo sombra (decide, não age) | — | Tudo | **T13.7a** | T13.5 | Nenhuma ordem afetada; sinais registrados |
| Piloto opt-in que só reduz risco | `mt5x-safety` (guardião, orçamento) | Política | T13.7 | T13.5, T12.3, robô aprovado, **autorização do dono** | Só reduz; bateria sem regressão |
| Monitoramento e rebaixamento | — | Tudo | T13.8 | T13.4, T13.7 | Motivo e trilha gravados |
| Jobs idempotentes, checkpoint, recursos | Fase 15 (`write_guard`, retenção, arquivo) | Produtores da T13.3 consultam o guarda | T13.3 | — | Escrita bloqueada pelo guarda é recusada com mensagem |

## Questões para o dono do projeto (não bloqueiam T13.3 a T13.4)

1. **Decisão 5 (confiança):** a T13.3 propõe — regime: fração dos três indicadores (ADX, eficiência, R²) que
   concordam com o estado; RVOL: número de sessões na mediana. Nenhuma é probabilidade. Confirmar ou trocar.
2. **T13.7a (modo sombra) como tarefa nova** entre T13.5 e T13.7: confirmar a inclusão no plano.
3. **Pilotos (T13.7):** qualquer piloto em conta demo exige autorização específica; conta real não está autorizada.
