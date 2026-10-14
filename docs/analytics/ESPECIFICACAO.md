# MT5x Analytics — Especificação funcional e técnica v1.0

> **Situação:** intenção registrada em 2026-10-13. A entrega A1/T13.1 (calendário observado e barras derivadas)
> foi implementada; A2 em diante continuam planejadas. O plano de implantação está na Fase 13 de
> [PLANO_DE_TRABALHO.md](../PLANO_DE_TRABALHO.md), e as regras entregues estão em
> [T13.1_CALENDARIO_OBSERVADO.md](T13.1_CALENDARIO_OBSERVADO.md). Onde este texto e o código atual divergem,
> a Fase 13 diz qual dos dois vale.

**Escopo:** projeto Python `mt5x` / `MT5 Extração de Dados`, com interface de consulta para Experts MQL5 do HADOUKEN.
**Natureza:** proposta de evolução; fora da T13.1, os módulos indicados como NOVO ainda não foram implementados.
**Princípio:** Python coleta, agrega, estuda, valida e publica dados; MQL5 avalia as informações publicadas, respeita
os próprios sinais e a camada Safety; Python **nunca envia ordens**.

## 1. Visão e objetivos

Transformar a base histórica do mt5x em um catálogo de evidências sobre ativos, usando barras M1 como granularidade
temporal principal, além de ticks e snapshots do book quando disponíveis. Entregar indicadores históricos e
probabilidades condicionais pré-calculados e versionados, com controle de validade temporal, qualidade e
significância estatística.

**Objetivos mensuráveis**

- Ressample de M1 para qualquer múltiplo inteiro de minuto permitido pelo calendário, incluindo
  2/3/5/10/15/30/60/240 minutos; suportar sessão diária e janelas móveis de 24h separadamente.
- Calcular características históricas por ativo, sessão, dia da semana, faixa horária e regime.
- Testar hipóteses com histórico temporal e custos plausíveis; distinguir descoberta, validação e liberação.
- Publicar um contrato de leitura leve para Experts, sem carregar milhões de barras ou exigir conexão com banco remoto.
- Recomputar incrementalmente o mínimo necessário após coleta ou correção tardia.

## 2. Limites arquiteturais e propriedade dos dados

1. `mt5x`: extração, indicadores, base histórica, análise, experiência de pesquisa, versionamento e publicação.
2. HADOUKEN: consome *facts* e sinalizações analíticas como filtros adicionais configuráveis, mantendo
   `StrategySignal`, `StrategyRules`, motor e `Safety/` intactos por padrão.
3. `HDK_HADOUKEN_Painel`: visão de análises do ativo e parâmetros de consulta; entradas manuais permanecem manuais.
4. `HDK_RiskGuard`: continua aprovando/negando volume/exposição. Nenhum dado analítico concede permissão para
   contornar limites.
5. A integração com o futuro Guardian Global e Gradiente Linear será opcional e versionada.
6. A coleta M1 NÃO recria fluxo de ordens, microestrutura ou sequência intraminuto. Para análises dependentes deles,
   utilizar ticks/DOM reais e registrar cobertura.

## 3. Pipeline de dados

```
MT5 ─► Extrator OHLCV M1 / Ticks / DOM ─► Validação e Catálogo de Sessões
                                             │
                                             ▼
                                     Armazenamento bruto
                                             │
                            ┌────────────────┴─────────────────┐
                            ▼                                  ▼
                  Barras derivadas por sessão          Indicadores de qualidade
                            │                                  │
                            └────────────────┬─────────────────┘
                                             ▼
                                  Feature Engineering
                                             ▼
                          Catálogo de Hipóteses e Pattern Mining
                                             ▼
                   Validação temporal, robustez, custos e evidências
                                             ▼
                            Feature Store / Publicações
                                   ┌─────────┴─────────┐
                                   ▼                   ▼
                         Painel pesquisa mt5x     Snapshot MT5
                                                       ▼
                                             HDK_AnalyticsReader
                                                       ▼
                                           Experts / Painel / Safety
```

## 4. Calendário, tempo e reconstrução OHLCV

### 4.1 Fonte canônica

- O M1 bruto deve preservar: `broker`, `account/server_id` quando pertinente, `raw_symbol`,
  `canonical_instrument_id`, `time_utc`, `time_broker`, `session_id`, OHLC, `tick_volume`, `real_volume`, `spread`
  se disponível, `data_quality`, `ingest_batch`, `revision` e `observed_at`.
- Registrar `available_at_utc`: momento em que a informação estava disponível para o sistema; isso é distinto do
  timestamp de mercado.
- No caso de contratos futuros, preservar a série de contrato individual e a metodologia explícita de
  rolagem/ajuste para série contínua; não misturar bases ajustadas com preços negociáveis.
- No caso de ações, versões com ajustes corporativos devem ser distinguidas de preços de execução originais.

### 4.2 Agregação

Para cada janela `[inicio, fim)` alinhada a sessão/instrumento:

- `open`: primeira abertura válida; `high`: máxima das máximas; `low`: mínima das mínimas; `close`: último
  fechamento válido.
- `tick_volume` e `real_volume`: somas, mantidas separadas; distinguir desconhecido de zero.
- `bar_count` e `coverage_ratio` guardados junto à barra; `quality_status`: `COMPLETE`, `PARTIAL`, `NO_TRADES`,
  `MISSING`, `CORRECTED`.
- `spread`: por exemplo média ponderada ou último valor — definir explicitamente a métrica; **não** somar spreads.
- `VWAP` verdadeiro requer negócios com preço e volume, ou agregados que contenham numerador/soma exatos. Não chamar
  média de preços M1 de VWAP verdadeiro.
- Barras parciais são armazenadas com status e **não** entram em análises que exijam fechamento.
- Sessões que atravessam a meia-noite, pausas de negociação, feriados, horários especiais e DST fazem parte da
  tabela de calendário.
- Timeframe `1D` significa sessão de negociação; janela `1440min` representa duração de 24h e é um conceito distinto.
- Para intervalos que não dividem perfeitamente a sessão, aplicar política declarada
  (`partial_last_bar=keep_marked|discard`) sem mesclar pregões.

### 4.3 Atualização

- Materializações M2/M3/M5/M15/M30/H1/H4/D1 mais usadas; os demais períodos sob demanda, com cache.
- Em cada ingestão, invalidar apenas buckets afetados e características dependentes de sua janela de retrospectiva.
- Em correção retroativa, registrar nova revisão, recomputar dependentes, preservar relatórios históricos de
  pesquisa e emitir nova publicação.
- Nunca alterar silenciosamente snapshots antigos usados em backtests.

## 5. Famílias de características pré-calculadas

| Família | Exemplos de features | Aplicação potencial |
|---|---|---|
| Retornos / gaps | Retorno D-1, gap de abertura, retorno após abertura, variações por horários | Gaps, reversão, continuidade |
| Volume | média e mediana 5/10/20/60 sessões, inclinação 3 semanas, percentil histórico | Liquidez e validação de contexto |
| Volume intradiário | curva esperada por minuto/faixa, RVOL *até agora* por hora/dia | Comparar com mesma hora de sessões anteriores |
| Volatilidade | ATR, amplitude, volatilidade realizada, range percentil, compressão e expansão | Estratégias direcionais vs laterais |
| Tendência/lateralidade | ADX compatível, eficiência de percurso, inclinação, autocorrelação, choppiness | Classificador de regime |
| Sazonalidade | dia da semana, semana do mês, vencimento, feriado, sessão, hora | Efeitos calendáricos |
| Estrutura intradiária | range inicial, máximo/mínimo da sessão, rompimento/reteste, extensão do movimento | Contexto operacional |
| Liquidez/microestrutura | spread, profundidade, desequilíbrio, slippage estimado | **Somente** se ticks/DOM suficientes |
| Relações entre ativos | correlações móveis, defasagens, dispersão, volatilidade relativa | Contexto multiativo sem supor causalidade |
| Saúde das estratégias | expectativa histórica condicionada a regime por robô, amostra e estabilidade | Compatibilidade estratégia × mercado |

`RVOL_at_time = volume_cumulativo_da_sessao_ate_t / mediana_historica_do_volume_cumulativo_ate_o_mesmo_t`; segmentar
por calendário e dia da semana para não confundir sazonalidade intradiária com mudança estrutural.

Queda persistente de volume: comparar janelas equivalentes, tendência robusta (por exemplo inclinação e
intervalos), dispersão e mínimo de sessões; evitar inferência baseada em três observações ou em volumes não
comparáveis.

## 6. Biblioteca de hipóteses — exemplos de partida

### H01. Alta no dia anterior → gap negativo

- Condição: `(close[D-1]/close[D-2]-1) >= 0,015`.
- Desfecho A: `(open[D]/close[D-1]-1) <= -0,005`.
- Desfecho B, testável em negociação: retorno `open[D] → close[D+5min]` de uma venda executável após a abertura,
  com custos e slippage.
- Não confundir um **gap ocorrido antes da entrada** com lucro alcançável por operação iniciada na abertura.
- Medir taxa condicional, taxa-base, ganho esperado, dispersão, amostra, intervalo de confiança e resultado fora da
  amostra.

### H02. +1,15% até 17:25 → +0,40% adicional até fechamento

- Condição: variação acumulada desde a abertura da sessão até 17:25 ≥ 1,15%, considerando barra fechada disponível
  nesse instante.
- Desfecho: `(close_sessao / preco_entrada_apos_17:25 -1) >= 0,004`.
- Excluir pregões cujo horário de encerramento não permite o estudo. Valores de horário devem vir do calendário por
  símbolo, não ser fixados a 18:00.
- Custos, liquidez e viés de execução de fim de sessão são materiais.

### H03. Queda de volume por três semanas

- Comparar volume de sessões fechadas vs padrão equivalente de 20–60 sessões, normalizado por dia da semana e
  duração da sessão.
- Gerar `VOLUME_DECLINING` apenas se amplitude/robustez exceder limiares validados; anexar intensidade e confiança.
- Testar se esse estado realmente piora a estratégia de rompimento antes de promover para bloqueio.

### H04. Volume atípico de segunda-feira ou baixo de terça-feira

- Verificar `volume/duracao` por dia da semana, comparado ao baseline apropriado, corrigindo feriados, volatilidade
  e vencimento.
- Testar se a variação de volume está associada a maior sucesso, **não assumir** que volume alto eleva
  automaticamente acerto.

### H05. Regime de lateralização e propensão a rompimento

- Medir `range/ATR`, eficiência do movimento, ADX, expansão/contração de volatilidade e RVOL intradiário.
- Definir estados `TREND`, `RANGE`, `TRANSITION`, `UNCERTAIN` com confiança e duração mínima.
- Avaliar o desempenho de cada uma das seis estratégias condicionado a regimes, usando histórico fora da amostra.

### Extensões priorizadas

- `GAP_CONTINUATION_VS_FADE`: continuação/reversão de gaps conforme amplitude, direção D-1 e volume.
- `OPENING_RANGE_BREAKOUT`: rompimento da máxima/mínima dos primeiros N minutos e comportamento subsequente.
- `VOLATILITY_COMPRESSION_EXPANSION`: probabilidade de expansão após compressão, por horário.
- `VOLUME_PRICE_DIVERGENCE`: retorno crescente com RVOL declinante versus crescente.
- `LAST_HOUR_EFFECT`: comportamento da última hora, por tipo de sessão/vencimento.
- `DOW_REGIME_INTERACTION`: dia da semana condicionado ao regime; não usar dia isoladamente sem amostra adequada.
- `EXPIRY_ROLLOVER`: comportamento ao redor de vencimentos, rolagem e troca de contrato.
- `CROSS_ASSET_CONTEXT`: correlação de WIN/WDO com variáveis acessíveis no mesmo instante; atenção a fusos de bolsas
  diferentes.
- `LIQUIDITY_DROPOUT`: aumento de spread e deterioração de liquidez (requer cotações/ticks/DOM reais).
- `STRATEGY_FITNESS`: oportunidade/risco previsto para cada estratégia por regime, com estudos independentes.

## 7. Motor de descoberta de padrões

### 7.1 Separar hipótese registrada de exploração

- **Declarativo:** regras históricas específicas descritas em YAML/JSON, com condição, janela, desfecho, universo e
  custos.
- **Exploratório:** busca de correlações e relações condicionais, sempre marcadas `EXPLORATORY` e não liberadas para
  operação automática.
- Catálogo mantém todas as hipóteses testadas, inclusive rejeitadas, para controlar seleção enviesada.
- Não permitir que padrões descobertos no conjunto de teste sejam reutilizados como se fossem hipóteses pré-definidas.

### 7.2 Métricas por hipótese

`n_condicao`, `n_sucesso`, `p_condicional`, `p_base`, `lift`, `expectativa_liquida`, `intervalo_confianca`,
`incerteza`, `pior_resultado`, `drawdown`, `estabilidade_por_ano`, `custo`, `OOS`, `status`, `version`.

Regra deve ser visível ao usuário com amostra, limitações e validade temporal. Probabilidade alta isolada não
basta: considerar payoff negativo nas exceções.

### 7.3 Validação

- Separação cronológica treino / validação / teste; walk-forward e janela móvel.
- Purging/embargo quando horizontes dos rótulos se sobrepõem; nenhuma variável posterior ao momento decisório pode
  vazar para o presente.
- Correção para múltiplos testes (por exemplo FDR) e, quando adequado, Deflated Sharpe Ratio / PBO em seleção de
  estratégias.
- Intervalos de confiança com dependência temporal (bootstrap em blocos, quando aplicável); amostra mínima por
  evento e janela.
- Sensibilidade a custos, spread, atraso, valores-limite, símbolos e subperíodos.
- Modelo de execução conservador para ambiguidades intraminuto; usar ticks reais quando ordem exata dos eventos
  importar.
- Evidência fora da amostra é requisito para `APPROVED`; aprovação pode expirar com degradação estatística.

### 7.4 Estados

`DRAFT → EXPLORATORY → VALIDATING → APPROVED → MONITORING → RETIRED`.

`APPROVED` **não** significa recomendação de compra/venda, apenas que há evidência de utilidade no contexto definido.

## 8. Dados analíticos no banco

Preservar tabelas brutas existentes; adicionar tabelas normalizadas, sem mudar sem necessidade a convenção atual de
tabelas símbolo/timeframe.

```text
instrument_catalog(instrument_id, broker_symbol, contract_month, ...)
market_calendar(session_id, instrument_id, open_utc, close_utc, segments_json, ...)
bars_derived(instrument_id, tf, bar_start_utc, bar_end_utc, OHLCV..., quality, revision)
feature_definitions(feature_id, name, algorithm_version, parameters, dependencies)
feature_values(instrument_id, tf, observation_time, available_at_utc,
               feature_id, numeric_value, categorical_value, quality, revision)
pattern_definitions(pattern_id, title, condition_json, target_json, version, status)
pattern_evaluations(pattern_id, asset_id, train_from, train_to, test_from,
                    test_to, n_events, p_condition, p_base, lift, expectancy_net,
                    confidence_json, performance_json, validated_at_utc)
strategy_regime_stats(strategy_id, instrument_id, regime, window, n_trades,
                      net_expectancy, confidence, model_version, available_at_utc)
feature_publications(publication_id, account_scope, symbols_json, schema_version,
                     generated_at_utc, data_cutoff_utc, valid_until_utc, checksum)
```

Índices por `(instrument_id, tf, bar_start_utc)`, `(instrument_id, feature_id, available_at_utc)` e
`(pattern_id, evaluated_at)` conforme plano real de consultas. PostgreSQL/TimescaleDB é alternativa quando o volume
ou a simultaneidade justificar, sem obrigatoriedade na primeira fase.

### Regra de *point-in-time*

Cada fato analítico terá `observation_time`, `available_at`, `valid_until`, `source_revision` e versão de algoritmo.
Backtest só pode ver fatos com `available_at <= simulation_time`. Indicadores que incorporam a barra diária completa
só ficam disponíveis após o fechamento correspondente.

## 9. Contrato de publicação para MT5

### 9.1 Preferência inicial: snapshot compacto

Gerar CSV com esquema estável para `MQL5/Files/HADOUKEN/Analytics/` ou na pasta comum do terminal, por símbolo e
contexto. Publicar via arquivo temporário + verificação e substituição atômica, e manifest com hash/versão, para
evitar leitura parcial. No Linux/Wine, resolver a pasta via configuração e validar as permissões de acesso, sem
assumir mesmo caminho do host.

Campos obrigatórios:

```text
schema_version,publication_id,broker,account_scope,symbol,timeframe,
feature_id,value,feature_type,quality,confidence,n_observations,
observed_at_utc,available_at_utc,valid_until_utc,
algorithm_version,source_revision
```

Exemplo representativo de linhas:

```csv
schema_version,publication_id,broker,account_scope,symbol,timeframe,feature_id,value,feature_type,quality,confidence,n_observations,observed_at_utc,available_at_utc,valid_until_utc,algorithm_version,source_revision
1,pub_072,EXEMPLO,demo,WIN,M5,RVOL_SAME_TIME,0.72,float,VALID,0.82,78,2026-10-13T14:25:00Z,2026-10-13T14:26:00Z,2026-10-13T14:40:00Z,1.0,583
1,pub_072,EXEMPLO,demo,WIN,D1,REGIME,RANGE,enum,VALID,0.71,120,2026-10-13T14:25:00Z,2026-10-13T14:26:00Z,2026-10-13T14:40:00Z,1.0,583
```

**Os valores são fictícios.** Nível de `confidence` precisa ter definição calibrada por feature; não usar como
probabilidade de lucro.

### 9.2 MQL5 adapter

```text
Include/Framework/HADOUKEN/Analytics/
  HDK_AnalyticsTypes.mqh       // enums, DTO, validade e status
  HDK_AnalyticsReader.mqh      // leitura, cache, rejeição de snapshot inválido
  HDK_AnalyticsPolicy.mqh      // política opcional por estratégia
  HDK_AnalyticsPanel.mqh       // painel visual de diagnósticos
```

O `HDK_AnalyticsReader` somente **lê**, não envia ordem. Se não houver snapshot válido, retornar `UNAVAILABLE` ou
`STALE`, nunca preencher com falso valor neutro. No painel, mostrar idade e fonte dos dados. No robô, comportamento
ante `STALE` é política explícita e independente por estratégia (advisory, bloqueio conservador ou não utilização).
A entrada aprovada continua obrigada a passar por `HDK_RiskGuard` e `HDK_SafeExecutor`.

### 9.3 Alternativa futura: SQLite nativo do MQL5

MQL5 disponibiliza `DatabaseOpen` com abertura somente leitura e pasta comum, portanto uma cópia materializada de um
banco analítico pequeno também é possível. Evitar compartilhamento de conexão de escrita com o Python em operação;
preferir snapshot/cópia versionada. Medir latência na máquina real e no Strategy Tester antes de escolher.

## 10. Política de sinalização e decisão

Estados recomendados: `FAVORABLE`, `NEUTRAL`, `CAUTION`, `UNFAVORABLE`, `INSUFFICIENT_DATA`, `STALE`.

Nunca converter uma evidência geral (`segunda tem volume alto`) diretamente em ordem ou aumento de contratos.
Exemplo de políticas:

- `VISUAL_ONLY`: colore o estado e mostra explicação.
- `ENTRY_VALIDATOR`: uma estratégia **opt-in** utiliza evidência validada para permitir/bloquear sua própria entrada.
- `RISK_SCALER`: redução proporcional configurada, conservadora, nunca acima de orçamento liberado pelos guardas.
  Qualquer aumento exige validação específica e autorização prévia.
- `NO_ACTION`: ignorar informação que não tenha evidência estatística suficiente.

Modos iniciais: `VISUAL_ONLY` e `NO_ACTION`. Qualquer ação automatizada será etapa posterior, validada por parity,
battery e walk-forward.

## 11. Interface mt5x e integração HADOUKEN

### Tela "Inteligência de Ativos"

- Filtros: ativo, contrato/contínuo, timeframe, janela temporal, dia, horário, estratégia, regime, tipo de dado.
- Indicadores: volume relativo, tendência do volume, volatilidade, regime, probabilidade condicionada, amostra,
  confiança, validade, qualidade.
- Visuais: calendário semanal e heatmap por hora, distribuição de retornos condicionais, sazonalidade de volume,
  evolução por regime, resultado de estratégia por ambiente.
- Painel de hipóteses: criar/editar, executar pesquisa, comparar taxa-base, custos e resultados OOS.
- Catálogo: aprovação/reprovação, deriva, histórico de versões e evidências.

### Aba HADOUKEN "Inteligência do Ativo"

- Cartões de contexto: regime, volume esperado vs atual, sazonalidade, alerta de mudança e qualidade dos dados.
- Lista de regras vigentes com explicação curta (`por quê`, observação, amostra, fonte e validade).
- Alerta sobre dados obsoletos; configurações por estratégia para consumo somente após aprovação.
- Sem sobreposição ao painel de estatísticas operacionais: este é **histórico do mercado e contexto**, não PnL
  realizado.

## 12. Organização sugerida do código Python

```text
mt5_extracao/
  analytics/
    __init__.py
    calendar.py             # sessão, fuso, disponibilidade
    resampling.py           # build/verify barras derivadas
    features.py             # registry de indicadores históricos
    volume_profiles.py      # sazonalidade, RVOL, tendência
    market_regimes.py       # direcional, lateral, transição
    hypotheses.py           # DSL e catálogo
    pattern_engine.py       # descoberta e avaliação
    validation.py           # walk-forward, custos, amostra, testes
    strategy_fitness.py     # estratégia × regime × ativo
    feature_store.py        # persistência e point-in-time
    publisher.py            # exportação atômica para MQL5
    monitoring.py           # drift, stale, qualidade
  robots/                   # sistema atual, sem envio de ordens
```

Novos comandos sugeridos (após auditoria dos existentes):

```bash
mt5x resample --symbols 'WIN$N' --base M1 --tf M2,M3,M5,M15,M30,H1,D1
mt5x analytics features --symbols 'WIN$N' --update
mt5x analytics hypotheses run --id H01 --symbol 'WIN$N'
mt5x analytics validate --id H01 --walk-forward
mt5x analytics publish --symbol 'WIN$N' --destination mt5-common
mt5x analytics report --symbol 'WIN$N' --open
```

Os nomes são proposta de contrato CLI, não comandos existentes.

## 13. Sequência de entregas e dependências

| Fase | Entrega | Critério de aceite |
|---|---|---|
| A0 | Auditoria do código `mt5x resample`, qualidade, schemas, AGENTS.md, integrações MQL5 | Inventário confirmado e lacunas registradas |
| A1 | Calendário de sessões e agregador M1 → N minutos | OHLCV correto, DST, pausas, lacunas, barras incompletas |
| A2 | Feature Store temporal e incrementos | Reprocessamento por dependência, rastreabilidade e snapshot point-in-time |
| A3 | Perfis de volume, sazonalidade, regimes | Algoritmos testados em séries sintéticas e mercados distintos |
| A4 | Hipóteses H01–H05 e biblioteca parametrizável | Evidências com amostra, baseline, custos, OOS e versão |
| A5 | Publicador + leitor MQL5 de somente leitura | Snapshots atômicos, tratamento de `STALE`, teste Windows/Linux/Wine |
| A6 | Interface de pesquisa mt5x + aba HADOUKEN | Filtros, gráficos, explicação e proveniência |
| A7 | Estratégia × regime e pilotos opt-in | Nenhuma regressão na battery, parity, Safety, custo e walk-forward |
| A8 | Monitoramento de deriva, alertas e governança de regras | Auto-rebaixamento de regras instáveis, trilha de auditoria |

## 14. Testes essenciais

1. Três candles M1 sintéticos com valores pré-definidos produzem OHLCV M3 exato, idempotente.
2. Sessão atravessando meia-noite: M5 não cruza a fronteira de sessão indevidamente.
3. Dia com feriado ou fechamento antecipado: não cria barras fantasmas.
4. Ingestão tardia de M1 altera apenas a barra agregada afetada, indicadores dependentes e publicação subsequente.
5. Volumes reais `0` e `NULL` não são tratados como a mesma observação.
6. Um evento observado às 17:25 nunca usa close das 18:00 em qualquer feature disponível às 17:25.
7. Backtest as-of reproduz exatamente os valores disponíveis na data simulada, incluindo revisão de histórico.
8. Hipóteses de dias de semana reportam amostra e baseline, e controles de múltiplas comparações.
9. Snapshot malformado/expirado é rejeitado sem alterar a autorização da camada Safety.
10. Estratégia automática não sofre mudança de decisão quando analytics está desativado.
11. Snapshot publicado com segurança continua legível no MT5 nativo e no MT5 Wine (pasta comum).
12. Classificadores de regime e métricas de volume possuem paridade entre pesquisa offline e publicação.
13. Rollovers de contratos futuros preservam preço negociável e registram corretamente ajuste da série contínua.
14. Conta demo / testador: nenhuma dependência de arquivo externo não versionado compromete reprodutibilidade.

## 15. Riscos e decisões em aberto

- Definir calendário e timezone para cada corretora/símbolo; evitar horas fixas de exemplo sem validação.
- Decidir se a série contínua de WIN/WDO será apenas para estudos ou também para features de execução; nunca
  executar em preço ajustado.
- Diagnosticar a qualidade do volume real fornecido pela corretora para cada ativo; volume tick não equivale a
  contratos negociados.
- Estabelecer um vocabulário por estratégia para regime e validação de eficiência; não existe um único regime
  universal que garanta resultado.
- Estabelecer prazo de validade de snapshots por frequência e comportamento diante de falha do Python.
- Criar políticas explícitas de consentimento para alterar limites operacionais conforme analítica.
- Benchmarks de processamento/memória/latência, SQLite vs PostgreSQL e formato dos snapshots, a decidir mediante
  medições reais.
- Definir retenção/versionamento e backups de features e hipóteses aprovadas.

## 16. Referências técnicas para implementação

- pandas `DataFrame.resample`: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.resample.html
- MQL5 `FileOpen`, sandbox e pasta comum: https://www.mql5.com/en/docs/files/fileopen
- MQL5 `DatabaseOpen` e SQLite nativo: https://www.mql5.com/en/docs/database/databaseopen
- Bailey & López de Prado, *Deflated Sharpe Ratio*: https://doi.org/10.3905/jpm.2014.40.5.094
- Harvey & Liu, *Backtesting* (múltiplos testes): https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2345489

**Nota de implementação:** usar o README fornecido como visão de negócio, porém ler os arquivos reais `AGENTS.md`,
`docs/arquitetura.md`, `docs/PLANO_DE_TRABALHO.md`, `docs/estrategias.md`, `docs/robos.md`, `docs/sdk/README.md`
antes de mudar código. Não renomear ou substituir componentes existentes sem necessidade. Todo exemplo de
estatística é ilustrativo; padrões precisam ser medidos e validados com dados reais.
