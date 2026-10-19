# Matriz de requisitos — ciclo de pesquisa de contexto (Fases 13 e 16)

> Registro histórico canônico da origem `e8657fe`/`bde3652`, incorporado na
> integração T13.3 de 2026-10-18. As tabelas descrevem a base e as verificações
> datadas abaixo. O estado atual da composição F19/F20 e seu aceite estão no
> PLANO_DE_TRABALHO, na continuidade e no contrato T13.3_PERFIS_REGIMES.
> Os marcadores históricos desta matriz não certificam a integração atual.

Levantamento de 2026-10-14, feito sobre o `master` em `880bf18` mais a T13.2 local (`codex/t13.2-feature-store`).
Serve para não duplicar componentes: cada requisito aponta para o que já existe e só abre tarefa para a lacuna.
As tarefas citadas estão em [PLANO_DE_TRABALHO.md](../PLANO_DE_TRABALHO.md) (Fase 13) e em
[propostas/ANALYTICS_OPERACOES.md](../propostas/ANALYTICS_OPERACOES.md) (Fase 16).

Atualização de 2026-10-15: as lacunas da T13.3 foram implementadas e verificadas na branch local
`tarefa/T13.3-perfis-regimes`, dependente da T13.2 local; ainda não foram integradas ao `master`. Evidências e
limitações da base estão em [T13.3_PERFIS_REGIMES.md](T13.3_PERFIS_REGIMES.md). A Decisão 5 permanece aberta.

Ciclo: dados → características → hipóteses → validação → comparação de filtros → publicação informativa →
modo sombra → pilotos autorizados → monitoramento.

## 1. Dados e tempo

| Requisito | Implementação existente | Lacuna | Tarefa | Dependência | Critério de aceite |
|---|---|---|---|---|---|
| Barras por sessão, sem barras fantasmas | `analytics/calendar.py`, `analytics/resampling.py` (`_market_calendar`, `_bars_derived`) | Calendário é inferido do M1 (reconstrução histórica); offset fixo, sem horário de verão | T13.1 ✅ (limitações registradas) | — | Testes 1–3 e 5 da especificação |
| Fato com observação, disponibilidade, validade, revisão | `analytics/feature_store.py` (T13.2) e `save_values` (T13.3) | — | T13.3 ✅ | T13.2 | Mesmo contrato de `save_value`, lote atômico e duplicatas no lote testados |
| Consulta *as-of* | `query_as_of`, `query_history`, `asof_join` | — | T13.3 ✅ | T13.2 | Revisões anteriores preservadas; séries separadas por símbolo, TF e versão; decisões só veem `available_at <= t` |
| Barra superior aberta ou incompleta não conta como fechada | `latest_bars(closed_only=True)` e `available_at = bar_end_utc` | — | T13.3 ✅ | T13.2 | H1 não aparece antes de fechar; parcial, cobertura incompleta e barra futura excluídas |
| Correção tardia e proveniência das características | Revisões em `_bars_derived`, janelas por sessão e fingerprint `source_revision` | Memória de Wilder é aproximada; estado/duração do regime dependem do prefixo histórico | T13.3 ✅ | T13.1, T13.2 | Disponibilidade nunca antecipa correção; limite de decaimento e propagação do estado testados |
| Volume real ≠ volume de ticks | `_bars_derived.real_volume_known` | `trade_analytics/enrich.py` mistura real e tick barra a barra e trata real `0` como ausente | **T16.3b** | — | Fonte do volume declarada; nunca misturada na mesma série |
| Contexto do Laboratório só com informação anterior | `enrich.py` usa `time < entry_time` | Usa a barra **aberta** quando a entrada não cai na abertura da barra; surpresa de notícia posterior à entrada vaza | **T16.3b** | — | Testes de vazamento com entrada no meio da barra e com notícia depois da entrada |
| Fato × divulgação × recebimento × revisão (economia) | `_economic_events` guarda horário do evento e `actual` | Não guarda quando o `actual` foi recebido nem revisões | Registrado; entra quando houver característica econômica (fora da T13.3) | T16.3a | `actual` só usado após o recebimento |
| Série contínua ajustada nunca executada | Decisão em aberto 2 do plano | Sem ativos B3 contínuos na base | Fica na decisão 2 | Dono do projeto | — |

## 2. Características (T13.3)

| Requisito | Existente | Lacuna | Tarefa | Critério de aceite |
|---|---|---|---|---|
| Retornos, gap, pregão anterior, movimento após abertura | `study.seasonality` (agregado, sem *as-of*) | Implementação causal por sessão | T13.3 ✅ | Gap disponível só no fim da 1ª barra da sessão |
| Volume: médias/medianas 5–60 sessões, inclinação, percentil | `volume_profiles.py` | — | T13.3 ✅ | Só sessões anteriores completas; sintético e AGG medidos |
| RVOL no mesmo horário | `volume_profiles.py` | — | T13.3 ✅ | Mediana das sessões anteriores no mesmo minuto; amostra insuficiente ou denominador zero = ausente |
| Volatilidade, compressão/expansão | `strategies.indicators` (ATR, Bollinger) e `market_regimes.py` | — | T13.3 ✅ | Percentil causal e correção tardia testados |
| Regime TREND/RANGE/TRANSITION/UNCERTAIN | `asset_screener` e `market_regimes.py` (ADX 25/20) | — | T13.3 ✅ | Paridade com a triagem para duração 1; duração mínima 3 e aquecimento incerto |
| Faixa inicial, distância de níveis anteriores | `session_structure.py` | — | T13.3 ✅ | Disponibilidade após N minutos; rompimento de duas pontas fica marcado ambíguo |
| Sazonalidade (dia, semana do mês, hora, minuto da sessão) | `enrich.calendar_columns`, `calendar_rows` | `study.seasonality` é agregado sobre a amostra inteira e não é causal | T13.3 ✅ | Colunas do calendário reutilizadas; resumo global fica só na validação descritiva |
| "Confiança" por característica | Proposta registrada nos parâmetros | Decisão em aberto 5 | T13.3 ✅ (proposta) | Texto de cada definição registrado; dono ainda confirma |

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

## Integração T18.1 em 2026-10-19

Registro tipado, alvos forward e DTO v1 adotados do commit canônico 80fd887 por cherry-pick revisado.
Migração somente aditiva; contratos antigos preservados; novos produtores com versão 1.3 e escopo
concreto por timeframe/configuração de T13.3b. Testes atuais cobrem tipagem, tombstones e transição de checkpoint;
aceite Linux1501/26/3, saída0, cinco checks aprovados e pytest Windows ainda em execução.
Resultados históricos875/10 não substituem a composição atual.
Associações, serviço e UI real permanecem tarefas posteriores.
