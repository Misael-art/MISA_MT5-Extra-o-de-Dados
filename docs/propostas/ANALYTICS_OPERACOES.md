# Fase 16 — Laboratório: analytics de operações, contexto, execução e otimização

**Pedido do dono do projeto (2026-10-14):** evoluir a análise para uma aba (chamada **Laboratório**, decisão de 2026-10-14) que transforme extração e
backtest em inteligência e otimização de estratégias. A aba deve:

- enriquecer cada operação com calendário, horário, notícias, volume e volatilidade;
- mostrar onde o robô ganha e perde (heatmaps, curvas por horário, dispersões);
- re-simular a mesma entrada sob outros modos de execução (ponto fixo, gradiente fixo, dinâmico ou direcional,
  martingale, trailing stop);
- sugerir regras de filtragem com um simulador que recalcula a curva de patrimônio;
- ter duas lentes: **por robô** (em que ativos e condições ele funciona) e **por ativo** (que robôs e execuções
  exploram melhor aquele ativo).

**Estado: plano registrado. Nada implementado.** Este documento diz o que fazer, em que ordem, onde no código e como
cada passo é aceito. Cada tarefa vira um PR (regra do AGENTS.md).

---

## 1. Como esta fase convive com o que já existe

Esta fase não pode duplicar nem atropelar outras frentes. As decisões abaixo foram tomadas lendo o código e os
planos em 2026-10-14.

| Já existe ou está planejado | Relação com a Fase 16 | Decisão |
|---|---|---|
| **Fase 13 — MT5x Analytics** ([ESPECIFICACAO](../analytics/ESPECIFICACAO.md)): hipóteses sobre o **mercado** (gap, volume, regime), pacote `mt5_extracao/analytics/` e comando `mt5x analytics` | A especificação diz: "sem sobreposição ao painel de estatísticas operacionais: este é histórico do mercado e contexto, não PnL realizado". A Fase 16 é justamente o PnL realizado dos robôs | **Pacote próprio: `mt5_extracao/trade_analytics/`. Comando próprio: `mt5x trades`.** O nome `analytics` fica para a Fase 13. Quando a T13.1 (calendário de sessões) e a T13.2 (características *as-of*) existirem, a Fase 16 passa a consumi-las em vez de calcular por conta própria (ver T16.3) |
| **Aba "Análise"** (`ui/analysis.py`): candles, indicadores e sazonalidade **do ativo** | Assunto diferente: preço, não operações | Continua como está. A aba nova se chama **"Laboratório"** (decisão do dono, 2026-10-14) e fica logo depois de "Robôs" (é lá que se olha o resultado dos robôs). O subtítulo da aba deixa claro: "operações dos robôs: onde ganham, onde perdem e como executar melhor" |
| **T12.6A — Estatísticas** (livro por operação e por grade, no HADOUKEN e no mt5x) | As duas precisam de um **livro de operações** com entrada, saída e contexto | **Um livro só:** a tabela `_trade_ledger` da T16.1 é o esquema que a T12.6A também usa. Quem fizer primeiro cria; o outro reaproveita |
| **Épico I** ([EPICO_I](EPICO_I_PESQUISA_VALIDACAO.md)): I-02 (execução independente do sinal), I-03 (walk-forward), I-09 (sobreajuste), I-10 (estratégia × regime), I-11 (otimização) | O simulador de execução e as regras otimizadas são exatamente esse território, com o maior risco do épico: **o melhor do passado vendido como resultado** | As regras do Épico I valem aqui (seção 6): separação dentro/fora da amostra, contagem de tentativas, amostra mínima de 30 operações por célula, preenchimento conservador |
| **Bateria de segurança** (`robots/battery.py`, `robots/risk.py`): reprova martingale, grid e preço médio em robô real | O Laboratório simula martingale e gradiente | Simular é estudo, não operação. **Martingale só aparece para o robô que o suporta**, com os limites de segurança do próprio robô (seção 4.4); fora disso não é oferecido. Proposta exportada com martingale é marcada como reprovável pela bateria. Gradiente é exportado como parâmetros do Gradiente Linear do HADOUKEN (T12.4/T12.5), que roda dentro da camada de segurança |
| **Calendário econômico:** `[SCREENER] events_file` (CSV `time,symbol,impact`) já lido por `asset_screener.load_events` | Fonte de "notícias de impacto" | **Várias fontes, com enriquecimento** (decisão de 2026-10-14): o CSV continua; entra a exportação do calendário do próprio MT5 por MQL5; as fontes são unidas numa tabela só (seção 3.3) |
| **Custos reais** (T10.12) e `SymbolSpec` (`strategies/backtester.py`) | A re-simulação precisa de custo, lote e valor do ponto | A re-simulação usa o mesmo `SymbolSpec` e o mesmo `BacktestConfig` do backtester. Nenhum modelo de custo novo |
| **Gestão de recursos** (Fase 15) | Tabelas novas crescem | As tabelas novas entram no inventário da aba Recursos. Retenção e arquivo seguem as regras de lá |
| **Interface:** Tkinter + matplotlib (já usado na aba Análise), tema claro e escuro (`ui/theme.py`) | O pedido citou PyQt, pyqtgraph, Plotly e seaborn | **Mantém Tkinter + matplotlib.** Trocar o toolkit quebraria a aplicação inteira e os testes de interface. Zoom e dicas ao parar o mouse vêm do matplotlib embutido (barra de navegação e eventos `motion_notify`). Uma visão HTML interativa opcional pode reaproveitar o painel vivo (T11.9) depois (T16.12) |

---

## 2. Arquitetura

```text
mt5_extracao/trade_analytics/        (sem Tkinter; só pandas/numpy, como o núcleo)
  ledger.py        T16.1  livro de operações (_trade_ledger): gravar e ler, por robô ou por ativo
  importers.py     T16.2  operações do backtester Python e do HDK_trades.csv (MQL5) para o livro
  enrich.py        T16.3  colunas de contexto por operação (calendário, horário, notícia, volume, volatilidade)
  calendar/          T16.3a calendário econômico de várias fontes (CSV, exportação MQL5, futuras) em _economic_events
  aggregate.py     T16.4  agregações por dimensão e cruzamentos (robô → ativos; ativo → robôs)
  stats.py         T16.4  métricas, intervalos de confiança (bootstrap), amostra mínima
  execution/       T16.6–T16.8  re-simulação de execução sobre as barras M1 da base
    engine.py      motor barra a barra com regra conservadora intrabarra
    modes.py       ponto fixo, trailing, break-even, gradiente fixo/dinâmico/direcional, (anti)martingale
  rules.py         T16.9  regras de filtro (dia, horário, notícia, volatilidade) e troca de modo
  insights.py      T16.10 descoberta de regras com validação fora da amostra e contagem de tentativas
  cache.py         T16.5  cache das colunas enriquecidas por execução (_trade_features)
mt5_extracao/ui/laboratory.py        T16.11 aba "Laboratório" (Tkinter + matplotlib)
mt5_extracao/ui/charts.py            T16.11 gráficos reutilizáveis (heatmap, overlay, dispersão, barras)
mt5x trades ...                      T16.12 o mesmo pelo terminal, com --json para agentes
```

**Fluxo:** backtest (Python ou MQL5) → `importers` grava no `_trade_ledger` → `enrich` calcula o contexto de cada
operação usando **só barras fechadas antes da entrada** e guarda no `_trade_features` → `aggregate` e `stats`
respondem às perguntas das duas lentes → `execution` re-simula sob outros modos → `rules` e `insights` propõem e
testam filtros → a aba e o `mt5x trades` desenham.

**Regras técnicas herdadas do AGENTS.md:** sem `tkinter` no núcleo; sem `import MetaTrader5` (a Fase 16 não fala com
o MT5; ela lê a base); tabelas novas só aditivas; chaves novas em `DEFAULTS` e no `config.ini.example` com
`fallback=`; Windows e Linux de primeira classe; mensagens em português; todo comportamento com teste, sem MT5 real.

---

## 3. Dados

### 3.1 Livro de operações — `_trade_ledger` (T16.1)

Uma linha por operação de qualquer backtest ou conta. Mesmo esquema para a T12.6A.

| Coluna | Tipo | Observação |
|---|---|---|
| `run_id` | TEXT | liga ao `_robot_runs` (backtest) ou a uma importação de conta |
| `seq` | INTEGER | ordem da operação no run; chave (`run_id`, `seq`) |
| `robot_key`, `symbol`, `timeframe` | TEXT | as duas lentes filtram por aqui (índices) |
| `source` | TEXT | `python`, `mql5_tester`, `conta_demo`, `conta_real`, `resim:<modo>` |
| `direction` | INTEGER | +1 compra, -1 venda |
| `entry_time`, `exit_time` | TIMESTAMP | horário da base (`time_basis` vale como no resto) |
| `entry_price`, `exit_price`, `stop_price`, `target_price` | REAL | stop e alvo iniciais; podem ser nulos |
| `volume`, `pnl`, `r_multiple`, `bars`, `exit_reason` | — | `exit_reason`: stop, alvo, sinal, tempo, fim |
| `mae`, `mfe` | REAL | maior contra e a favor durante a operação, em pontos (calculado pelo enriquecimento quando a fonte não traz) |
| `params_hash` | TEXT | parâmetros/preset do run, para comparar configurações |
| `completeness` | TEXT | `completo` ou `parcial` (ex.: MQL5 antigo sem horário de entrada) |

**Lacuna de hoje (conferida no código):** `robots/backtest.py` grava em `_robot_runs.trades_json` só
`[saída, resultado, volume]`, e o `HDK_trades.csv` do HADOUKEN tem só `time;symbol;type;volume;result`. O
backtester Python já tem tudo em `Trade` (`entry_time`, `direction`, preços, `r_multiple`, `exit_reason`): basta
gravar. Para os robôs MQL5 é preciso estender o CSV no HADOUKEN (T16.2b, tarefa cruzada). Até lá, essas operações
entram como `parcial` e a aba mostra só o que dá para mostrar com o horário de saída.

### 3.2 Contexto por operação — `_trade_features` (T16.3, T16.5)

Calculado uma vez e guardado; `feature_version` muda quando a fórmula muda (e invalida o cache).

| Grupo | Colunas |
|---|---|
| Calendário | `day_of_week` (0=seg), `day_of_month`, `week_of_month` (1–5), `month`, `is_month_end` |
| Horário | `hour`, `bucket_15m`, `bucket_30m`, `minutes_from_open` (pela sessão do dia; T13.1 quando existir) |
| Notícias | `event_impact` (nenhum, baixo, médio, alto), `minutes_to_event` (sinal: antes ou depois), `event_name` |
| Microestrutura | `entry_volume` (volume real, senão de ticks, da barra anterior à entrada), `volume_rank` (percentil em 20 dias), `atr14`, `atr_pct`, `bb_width`, `prior_return_pct` (variação nas N barras anteriores), `volatility_bucket` (baixa, média, alta por tercis do próprio ativo) |
| Resultado | `win_flag`, `pnl`, `r_multiple`, `duration_min`, `mae`, `mfe` |

**Sem olhar o futuro:** toda característica usa só barras com `time < entry_time` (a barra da entrada ainda não
fechou). Isso tem teste próprio, com uma série armadilha (T16.3). É a mesma regra *point-in-time* da Fase 13.

### 3.3 Calendário econômico de várias fontes (T16.3a)

Decisão de 2026-10-14: o calendário usa **várias fontes** e é **enriquecido**; o CSV continua e entra a exportação do
calendário do próprio MT5.

| Fonte | Como entra | O que traz |
|---|---|---|
| CSV manual (`[SCREENER] events_file`, como hoje) | leitor atual, com colunas opcionais novas | `time,symbol,impact` e, se houver, `name,country,currency` |
| **Calendário do MT5** (`CalendarValueHistory`, `CalendarEventById`, `CalendarCountryById` no MQL5) | script MQL5 `MT5X_CalendarExport.mq5`, instalado e rodado pelo mt5x como os demais scripts (`robots/mt5_tools.py`), que grava `Common\Files\MT5X_calendar_<servidor>.csv`; o mt5x importa | nome do evento, país, moeda, importância (nenhuma, baixa, média, alta), valores real, previsto e anterior, horário do servidor |
| Fontes futuras | um importador por fonte, no mesmo registro | — |

Tudo vai para a tabela aditiva **`_economic_events`** (`event_time`, `event_id`, `name`, `country`, `currency`,
`impact`, `actual`, `forecast`, `previous`, `source`, `imported_at`). Enriquecimento ao unir as fontes:

- o mesmo evento de duas fontes (mesma moeda, horário a até 1 minuto e nome parecido) vira **uma linha**, com as
  fontes listadas; a importância do MT5 prevalece sobre a do CSV, mas a do CSV vale quando a pessoa a marcou à mão
  (coluna `override`);
- **surpresa** = real − previsto, quando os dois existem (uma dimensão a mais para o Laboratório);
- **ligação com ativos:** cada evento vale para os ativos cuja moeda de lucro ou de base é a do evento (lida de
  `_symbol_specs`), mais os mapeamentos de `[TRADE_ANALYTICS] event_symbols` (ex.: `BRL: WIN$N, WDO$N`);
- `mt5x trades calendar import | status` mostra de onde veio cada evento, a cobertura de datas e as lacunas.

Uma operação fica "com notícia" quando há evento ligado ao ativo dentro da janela
`[TRADE_ANALYTICS] event_window_before_min` / `event_window_after_min` em torno da entrada. Sem nenhuma fonte, a
dimensão aparece desabilitada na aba, com a explicação de como preencher.

---

## 4. Re-simulação de execução (T16.6 a T16.8)

### 4.1 O que é e o que não é

Pega os **pontos de entrada** de um run (horário, direção, stop inicial) e refaz a **gestão da posição** sobre as
barras M1 da base, sob outro modo. Não roda o MT5. Não muda o sinal. Responde: "com estas mesmas entradas, como
teria ido com trailing, com gradiente, com martingale?".

### 4.2 Regras do motor (`execution/engine.py`)

- **Barras M1 da base** como trajetória; se a série do run for maior que M1 e existir M1, usa M1. Sem M1, usa a do run e
  marca o resultado como "resolução grosseira".
- **Ambiguidade intrabarra conservadora:** se stop e alvo cabem na mesma barra, o **stop vem primeiro**. Gap: a ordem
  é preenchida na abertura da barra, não no nível. Escorregamento e comissão vêm do `BacktestConfig` (mesmos
  custos do backtester).
- **Paridade obrigatória:** o modo "ponto fixo" com os parâmetros do run tem que reproduzir as operações do
  backtester Python (mesmas entradas e saídas; tolerância declarada no teste). Sem isso, nenhum outro modo é
  mostrado.
- **Limites de segurança da simulação:** exposição máxima em lotes e número máximo de níveis por modo; operação que
  estoura o limite é encerrada e marcada. Martingale tem teto de multiplicações e mostra a **ruína**: perda máxima e
  maior sequência.

### 4.3 Modos (`execution/modes.py`, um registro aberto como os da Fase 15)

| Modo | Parâmetros | Observação |
|---|---|---|
| Ponto fixo (base) | alvo e stop em pontos ou em R | referência de comparação |
| Break-even | gatilho em R, deslocamento | |
| Trailing por ATR (Chandelier) | período, múltiplo | |
| Trailing por máxima/mínima de N barras | N | |
| Gradiente linear fixo | espaçamento em pontos, níveis, lote por nível, a favor ou contra | mesmos nomes de parâmetros do Gradiente Linear do HADOUKEN (T12.4) |
| Gradiente dinâmico | espaçamento = k × ATR no momento da montagem | alinhado à T12.5 |
| Gradiente direcional (piramidação) | adiciona só a favor; stop do conjunto sobe | |
| Martingale / anti-martingale | os do próprio robô (multiplicador, teto, reinício) | **só para robô que o suporta**, com os limites de segurança dele (seção 4.4) |

Cada modo devolve um resultado no mesmo formato do backtester (lista de `Trade` + curva), gravado no
`_trade_ledger` com `source = resim:<modo>` e o `params_hash` do modo. Assim tudo o que vale para operações reais
(enriquecimento, agregação, heatmaps) vale igual para as simuladas.

### 4.4 Martingale: só quando o robô suporta, e com as travas dele

Decisão de 2026-10-14: o Laboratório mostra o comportamento da operação com martingale **se a estratégia e o robô o
suportam**, respeitando todos os pontos de segurança daquele robô.

- **Quem suporta:** o robô que tem parâmetro de papel `martingale` (o mesmo detector de `robots/risk.py`, que já lê
  os parâmetros de cada robô) e que não tem a proteção `block_averaging` ligada no preset. Estratégias Python e robôs
  sem esse parâmetro **não** recebem o modo martingale: a opção não aparece.
- **Travas do robô na simulação:** os parâmetros do preset com papel `risk_pct`, `daily_loss_pct`,
  `max_drawdown_pct`, `stop_points`/`stop_mult` e o teto do multiplicador são aplicados na re-simulação como o robô
  os aplicaria: atingiu a perda diária, para no dia; atingiu o drawdown máximo, para; o lote nunca passa do teto de
  risco por operação nem do `volume_max` do `_symbol_specs`. Se o robô não declara um desses limites, a simulação
  usa o teto global (`[ROBOTS] max_real_risk_pct`) e avisa.
- **Na tela:** ao lado do resultado, a ruína (maior sequência de perdas, maior exposição, perda máxima) e quantas vezes
  cada trava disparou. A bateria de segurança continua reprovando martingale para operação real; o Laboratório diz
  isso junto do resultado.

---

## 5. A aba "Laboratório" (T16.11)

### 5.1 Barra superior (sempre visível)

```text
MODALIDADE: (•) Por robô  ( ) Por ativo   | Robô [▼] ou Ativo [▼] | Comparar: [x] WIN$N [x] WDO$N ...
Período [de .. até] | Execução [Ponto fixo ▼] | Notícias: [ ] ocultar impacto alto | Amostra: dentro / fora | ⟳
```

- **Por robô:** o robô escolhido, em todos os ativos e timeframes em que foi testado.
- **Por ativo:** todos os robôs testados no ativo escolhido.
- **Amostra:** "descoberta", "validação" ou "toda" (seção 6.1). Toda tela mostra de que amostra são os números.

### 5.2 Guias internas (cada uma é um dos protótipos do pedido)

| Guia | Conteúdo | Protótipo |
|---|---|---|
| **Visão geral** | Indicadores no topo (operações, acerto, fator de lucro, expectativa, queda máxima, com intervalo de confiança); heatmap Horário × Dia da semana; curvas de patrimônio sobrepostas (ativos na lente Robô, robôs na lente Ativo, ou modos de execução); tabela de notícias × desempenho | 1 e lente dupla |
| **Tempo e calendário** | Barras por dia da semana (acerto e resultado), heatmap do dia do mês (1–31), curva de acerto e resultado ao longo do pregão (faixas de 15/30/60 min), semana do mês, vencedoras × perdedoras por faixa de 30 min | pilar 1 |
| **Microestrutura** | Dispersão Volatilidade (ATR) × resultado (vencedora e perdedora em cores distintas), barras por faixa de volume com acerto, heatmap Volatilidade × Faixa horária | pilar 2 |
| **Execução** | Seleção dos modos para comparar; curvas sobrepostas; matriz Modo × (acerto, fator de lucro, payoff, queda máxima, melhores horários); super-heatmap Contexto × Modo; dispersão Acerto × Fator de lucro por [contexto + modo] | 2 |
| **Insights e simulador** | Lista de insights com amostra, intervalo e estado; checklist de regras (excluir dia, janela horária, notícia; trocar de modo por volatilidade ou horário); antes × depois **fora da amostra**; "Exportar proposta" | 3 |
| **Comparativo** (só na lente Ativo) | Revezamento: qual robô domina cada faixa horária ou dia; correlação dos resultados diários entre robôs (se as perdas se somam ou se compensam) | lente Ativo |

### 5.3 Regras visuais (pedagogia e honestidade)

- Paleta **divergente acessível** (azul ↔ laranja, não verde ↔ vermelho puro) com a escala sempre visível; tema
  claro e escuro pelos tokens de `ui/theme.py`. Respeita o pedido "estilo terminal profissional" no tema escuro sem
  criar um terceiro tema.
- Toda célula mostra **n** (número de operações). Célula com **n < 30** fica hachurada e cinza, com a dica "amostra
  pequena: não conclua nada daqui" (regra I-10).
- Ao parar o mouse: dica com n, acerto, expectativa e intervalo de 90%. Clicar numa célula filtra as outras guias
  (cruzamento interativo).
- Cada gráfico tem um "?" com a explicação curta (como o glossário da aba Análise).
- Nada é chamado de "recomendado". Insight mostra estado (`exploratório`, `validado fora da amostra`), amostra e
  quantas alternativas foram testadas para encontrá-lo.

### 5.4 Desempenho

- Leitura, enriquecimento e agregação em segundo plano (`ui.run_bg`), como as outras abas. A aba só lê a base quando
  é aberta.
- Enriquecimento calculado **uma vez por run** e guardado (`_trade_features`); trocar filtro só re-agrega em memória.
- Metas, medidas em teste de desempenho com 50 mil operações sintéticas:

  | Operação | Meta |
  |---|---|
  | Re-agregar e redesenhar a guia visível | < 300 ms |
  | Simulador de regras (sem re-simular execução) | < 500 ms |
  | Re-simular um modo | < 5 s para 10 mil operações em M1, com progresso e cancelamento |

- Gráficos reaproveitam a `Figure` e redesenham só a guia visível. Curvas longas são reduzidas (como o
  `downsample` dos backtests). O simulador espera 250 ms depois da última mudança antes de recalcular.

---

## 6. Otimização sem enganar ninguém (T16.9, T16.10)

### 6.1 Como os dados são separados (decisão de 2026-10-14: o mais abrangente e seguro que for viável)

A ideia em uma frase: **uma regra só vale se continuar funcionando num período que não foi usado para encontrá-la.**
Duas proteções juntas:

1. **Walk-forward ancorado (várias provas seguidas).** O histórico de cada run é cortado em blocos cronológicos. A
   regra é procurada no bloco 1 e conferida no bloco 2; depois procurada nos blocos 1–2 e conferida no 3; e assim por
   diante. Entre a parte de procura e a de conferência fica uma folga (embargo) do tamanho da operação mais longa,
   para uma operação não "vazar" de um lado para o outro. A regra precisa funcionar na maioria das provas, não em
   uma só.
2. **Reserva final intocada.** Os últimos 20% do histórico ficam guardados e **não entram em nenhuma procura nem
   conferência**. Só são usados uma vez, no fim, para o veredito mostrado ao usuário.

Quando não há operações suficientes (cada bloco precisa de pelo menos `min_trades_cell` operações), o Laboratório
reduz o número de blocos até o mínimo de dois; se nem assim der, fica só a reserva final e o insight é marcado
"evidência fraca". Chaves: `[TRADE_ANALYTICS] wf_folds = 4`, `holdout_fraction = 0.2`, `min_trades_cell = 30`.

### 6.2 Regras obrigatórias

É a parte mais perigosa: com dezenas de dias, horários, faixas e modos, sempre existe uma combinação que "teria dado
lucro". Regras obrigatórias, do Épico I e da especificação da Fase 13:

1. **Descoberta × validação (6.1).** Regras são descobertas só nas partes de procura. O "resultado projetado" do
   simulador é mostrado nas provas do walk-forward e na reserva final, ao lado do de procura. Se só melhora na procura,
   a tela diz isso.
2. **Contagem de tentativas.** Cada insight informa quantas combinações foram testadas para achá-lo. A significância
   é corrigida por múltiplos testes (FDR de Benjamini-Hochberg). Quando a I-09 existir, entra o Deflated Sharpe.
3. **Amostra mínima.** Regra que deixa menos de 30 operações em qualquer lado não vira insight.
4. **Intervalo de confiança** por bootstrap em blocos (dias), não só a média.
5. **Teste do ruído:** em operações de resultado aleatório, o motor de insights não pode produzir nenhum insight
   "validado". Esse é um teste automático da T16.10.
6. **Exportar é propor, não aplicar.** "Exportar proposta" grava um arquivo
   (`exports/propostas/<robô>_<data>.json`) com as regras, a amostra, os números de dentro e de fora e o número de
   tentativas. Para virar preset de robô passa pelo fluxo de sempre: `mt5x validate`, bateria e paridade.
   Proposta com martingale sai marcada "reprovável pela bateria para operação real". Regras de gradiente saem como
   parâmetros do Gradiente Linear do HADOUKEN.
7. Insights têm o mesmo ciclo de estados da Fase 13 (`exploratório → validando → validado → monitorando →
   aposentado`), guardados em `_trade_insights` (aditiva).

---

## 7. Tarefas

Cada tarefa é um PR. "Aceite" é o que o revisor confere; "Testes" é o mínimo em `tests/`.

### T16.0 — Plano registrado
Este documento, as linhas no painel e a seção da Fase 16 no PLANO_DE_TRABALHO.md. **Aceite:** revisão do dono.

### T16.1 — Livro de operações (`_trade_ledger`)
- **Fazer:** `trade_analytics/ledger.py` com `save_trades(db, run_id, trades, meta)`, `load(db, robot=None,
  symbol=None, timeframe=None, source=None, start=None, end=None)` → DataFrame; índices por robô, ativo e horário
  de entrada; criação sob demanda (aditiva), SQLite e PostgreSQL.
- **Aceite:** gravar e ler devolve as mesmas operações; base antiga abre; nenhuma tabela existente muda.
- **Testes:** ida e volta; filtros das duas lentes; PostgreSQL pelo job de CI que já existe.

### T16.2 — Importadores (Python e MQL5)
- **Fazer:** (a) `robots/backtest.py` passa a gravar também no livro as operações completas do `Trade`
  (mantendo `trades_json` como está, por compatibilidade); importador para os runs antigos (`mt5x trades import`)
  que gera linhas `parcial`. (b) **Tarefa cruzada no HADOUKEN (T16.2b):** acrescentar ao `HDK_trades.csv` as colunas
  `entry_time;entry_price;exit_price;sl;tp;direction;exit_reason`. O leitor `read_hdk_trades` aceita os dois formatos.
- **Aceite:** backtest novo de estratégia Python aparece completo no livro; run MQL5 antigo aparece `parcial`;
  CSV novo vira `completo`.
- **Testes:** o backtest grava; CSV antigo e novo; nada muda em `_robot_runs`.

### T16.3a — Calendário econômico de várias fontes
- **Fazer:** `trade_analytics/calendar/` (registro de importadores: CSV e MT5); script `MT5X_CalendarExport.mq5`
  (período, países e importância como parâmetros; grava CSV em `Common\Files`); instalação e execução pelo
  `robots/mt5_tools.py`, como os outros scripts; tabela `_economic_events`; união e enriquecimento da seção 3.3;
  `mt5x trades calendar import --source csv|mt5 | status`.
- **Aceite:** as duas fontes entram; o mesmo evento das duas vira uma linha com as duas fontes; surpresa calculada;
  eventos ligados aos ativos pela moeda.
- **Testes:** CSV antigo (só `time,symbol,impact`) e novo; arquivo do MT5 de exemplo (gerado no formato do script);
  união sem duplicar; mapeamento moeda → ativo. O script MQL5 compila pelo `mt5x robots compile`, conferido no host
  com o MT5.

### T16.3 — Enriquecimento
- **Fazer:** `enrich.py` (colunas da seção 3.2), usando os eventos da T16.3a com a janela configurável.
  Sessão e minutos desde a abertura: pela dedução das barras do dia (mesma ideia da T13.1); quando a T13.1 existir,
  troca a fonte sem mudar a saída.
- **Chaves novas `[TRADE_ANALYTICS]`:** `event_window_before_min = 30`, `event_window_after_min = 30`,
  `time_bucket_min = 30`, `volatility_period = 14`, `wf_folds = 4`, `holdout_fraction = 0.2`, `min_trades_cell = 30`,
  `event_symbols = ` (mapeamentos moeda → ativos).
- **Aceite:** cada operação recebe as colunas; nenhuma usa barra com `time >= entry_time`.
- **Testes:** série armadilha (um salto de preço na barra da entrada não pode aparecer no ATR da operação); dia da
  semana e semana do mês em datas conhecidas; operação sem barras anteriores fica com contexto nulo, sem erro.

### T16.3b — Correção temporal do enriquecimento (auditoria de 2026-10-14)
Achados da auditoria da integração temporal entre características e Laboratório
([analytics/MATRIZ_REQUISITOS.md](../analytics/MATRIZ_REQUISITOS.md)):
1. `enrich` escolhe a última barra com `time < entry_time`, mas `time` é a **abertura** da barra. Uma entrada às
   10:03 em M5 usa a barra das 10:00, que só fecha às 10:05 (máxima, mínima e fechamento ainda não existiam). Só
   está certo quando a entrada cai exatamente na abertura da barra (backtester Python); operações do Strategy
   Tester entram a qualquer momento. Correção: barra com `time + duração <= entry_time`.
2. A notícia mais forte na janela inclui eventos **depois** da entrada com `event_surprise` (real − previsto), que
   só se conhece após a divulgação. Correção: surpresa só para eventos divulgados antes da entrada; o horário e o
   impacto agendados continuam (são conhecidos antes).
3. `vol` usa o volume real quando > 0 e ticks quando não: mistura as duas fontes na mesma série e trata volume real
   `0` como ausente. Correção: uma fonte por série (real só se conhecida em toda a janela), declarada na saída.
4. MAE/MFE incluem a barra da saída inteira (preços depois da saída) e excluem o pedaço da barra da entrada depois
   dela: são resultado, não contexto, mas ficam aproximados. Correção: documentar a aproximação; ticks quando houver.
- **Aceite:** testes de vazamento com entrada no meio da barra e com notícia depois da entrada; `FEATURE_VERSION`
  sobe para 2 (o cache da T16.5 se invalida sozinho); resultados antigos não mudam no livro.
- **Estado local (2026-10-15):** entregue em `tarefa/T16.3b-enriquecimento-temporal`, commit `5d85cf3`.
  Testes focados: 22 aprovados; suíte: 806 aprovados, 9 ignorados; `doctor`: 16 OK, 2 avisos, 0 falhas.
  MAE/MFE continua aproximado à resolução M1/OHLC. A branch não foi integrada.

### T16.3c — Calendário econômico point-in-time: disponibilidade observada e revisões (extensão de 2026-10-15)

- **Lacuna confirmada:** T16.3a guarda `event_time`, valores `actual`/`forecast`/`previous` e `imported_at`. O primeiro
  é o horário do evento informado pelo calendário (sem separar previsto de divulgação efetiva); o segundo é quando
  esta base foi carregada. Nenhum dos dois prova quando o valor real ficou disponível à estratégia, e o `save` atual
  substitui a mesma linha quando o evento recebe correção, sem guardar a versão anterior. A T16.3b evita usar eventos
  agendados depois da entrada, mas não reconstrói disponibilidade nem revisões.
- **Objetivo:** preservar proveniência e revisões sem inventar timestamps. Em captura ao vivo, `first_observed_at`
  registra quando o mt5x recebeu a informação; se a fonte trouxer um `source_received_at` confiável, guardá-lo à parte.
  Consultas *as-of* só usam valores quando uma dessas disponibilidades documentadas for `<= decision_time`. Horários
  ausentes ficam desconhecidos; não usar esses campos em características point-in-time.
- **Persistência:** adicionar armazenamento interno de revisões por fonte/evento, mantendo `_economic_events` e bancos
  antigos legíveis. Reimportar deve ser idempotente sem apagar revisões históricas. `imported_at`, `first_observed_at`
  e `source_received_at` devem ter significados distintos.
- **Aceite:** fixtures cobrem um evento com duas revisões observadas em instantes distintos e consultas antes/depois da
  segunda revisão; reimportação não duplica nem apaga histórico; evento histórico sem timestamp de disponibilidade
  fica desconhecido para contexto *as-of*; nenhum código deduz disponibilidade pelo horário do evento ou por uma
  importação posterior.
- **Dependências/limites:** T16.3a e T16.3b. Implementação e testes sintéticos são independentes, mas avaliação real
  point-in-time fica pendente até existir uma fonte/exportação com timestamps confiáveis de recebimento e revisão.

### T16.4 — Agregações e estatística (as duas lentes)
- **Fazer:** `aggregate.py`: `by_dimension(df, dims)`, `crosstab(df, row, col, metric)`, `robot_view(db, robot)`,
  `asset_view(db, symbol)`, `daily_correlation(df)`; `stats.py`: métricas do backtester (`compute_metrics`), bootstrap
  em blocos por dia, marcação de amostra pequena.
- **Aceite:** os números batem com `compute_metrics` no total; cada célula traz n e intervalo.
- **Testes:** dados sintéticos com efeito plantado (perde às sextas, ganha das 10h às 11h) que tem de aparecer; célula
  com n < 30 marcada.

### T16.5 — Cache e desempenho
- **Fazer:** `_trade_features` com `feature_version`; recalcula só os runs novos ou com versão velha; entra no
  inventário da Fase 15 (tamanho, retenção).
- **Aceite:** abrir a aba pela segunda vez não recalcula; metas da seção 5.4 medidas.
- **Testes:** invalidação por versão; teste de desempenho com 50 mil operações (marcado, roda no CI).

### T16.6 — Motor de re-simulação e modo ponto fixo
- **Fazer:** `execution/engine.py` com as regras da seção 4.2 e o modo base.
- **Aceite:** **paridade** com o backtester Python nas seis estratégias (mesmas entradas e saídas, tolerância
  declarada); stop antes do alvo na barra ambígua; gap preenchido na abertura.
- **Testes:** paridade; barra ambígua; gap; custo e escorregamento iguais aos do backtester.

### T16.7 — Modos de saída (break-even e trailings)
- **Aceite:** cada modo com caso sintético de resultado conhecido.
- **Testes:** trilha sobe e nunca desce; break-even só depois do gatilho.

### T16.8 — Modos de dimensionamento (gradientes e martingale)
- **Fazer:** gradiente fixo, dinâmico (ATR) e direcional, com os nomes de parâmetro do Gradiente Linear do HADOUKEN
  (T12.4/T12.5); martingale e anti-martingale só para robô que os suporta, com as travas dele (seção 4.4). Limites de
  exposição e níveis.
- **Aceite:** exposição nunca passa do teto; martingale não aparece para robô sem suporte; cada trava do robô
  (perda diária, drawdown, risco por operação, `volume_max`) dispara na simulação como dispararia no robô.
- **Testes:** preço médio e resultado de grade em trajetória conhecida; teto respeitado; sequência de perdas leva ao
  limite e encerra; robô sem parâmetro de martingale não recebe o modo; preset com `block_averaging` ligado também não.

### T16.9 — Regras e simulador
- **Fazer:** `rules.py`: regras como dados (`{"tipo": "excluir_dia", "dias": [4]}`, `janela_horaria`,
  `excluir_noticia`, `trocar_modo_se`), aplicadas ao livro enriquecido; resultado antes × depois em dentro e fora.
- **Aceite:** aplicar e remover uma regra devolve o original; simulador dentro da meta de tempo.
- **Testes:** regra de excluir sexta tira exatamente as operações de sexta; troca de modo usa o resultado re-simulado
  daquele modo.

### T16.10 — Insights automáticos
- **Fazer:** `insights.py` varre as dimensões (dia, faixa horária, notícia, surpresa da notícia, volatilidade, volume,
  modo) só nas partes de procura; confere nas provas do walk-forward e na reserva final (seção 6.1); corrige por múltiplos testes; texto pedagógico pronto ("Sextas: expectativa
  -R$ 14,20 por operação em 212 operações; fora da amostra: -R$ 9,80 em 61"); grava em `_trade_insights`.
- **Aceite:** regras da seção 6; cada insight mostra n, intervalo, tentativas e estado.
- **Testes:** efeito plantado é achado e validado; **ruído puro não produz insight validado** (mil sorteios); insight
  que só existe dentro da amostra fica "exploratório".

### T16.11 — Aba "Laboratório"
- **Fazer:** `ui/laboratory.py` e `ui/charts.py` (heatmap, overlay, dispersão, barras agrupadas, com hachura de
  amostra pequena e dicas). Entra no notebook depois de "Robôs", no menu Exibir pelo widget, não pelo índice.
  Barra superior e guias da seção 5. Ligação com a aba Robôs: "Abrir no Laboratório" no painel de cada robô.
- **Aceite:** abre com a base vazia (mensagem de como começar), com runs parciais (o que falta e por quê) e com dados
  completos; temas claro e escuro; 1100×720 sem cortar controles.
- **Testes:** os de interface do padrão de `tests/test_ui_main_window.py` (com espera pela leitura em segundo plano,
  como corrigido na harmonização): troca de lente, filtro, clique na célula, simulador.

### T16.12 — `mt5x trades` e relatório
- **Fazer:** `mt5x trades import | enrich | report --robot X | --symbol Y | simulate --mode ... | insights | export`,
  todos com `--json`. Relatório HTML opcional pelo mesmo gerador do painel de resultados (T11.7), com gráficos
  interativos no navegador.
- **Aceite:** os mesmos números da aba.
- **Testes:** CLI com base de teste, como os de `tests/test_cli.py`.

### Ordem e dependências

```text
T16.0 → T16.1 → T16.2 → T16.3a → T16.3 → T16.4 → T16.5 ─┬→ T16.11 (guias Visão geral, Tempo, Microestrutura)
                         │                      └→ T16.12 (report)
                         └→ T16.6 → T16.7 → T16.8 → T16.9 → T16.10 → T16.11 (Execução, Insights) → T16.12 (simulate, insights)
T16.2b (HADOUKEN) em paralelo; sem ela os robôs MQL5 ficam "parciais"
```

A primeira entrega útil para o usuário é **T16.1 a T16.5 mais a T16.11 parcial**: a lente dupla com tempo, calendário e
microestrutura, sem re-simulação. A re-simulação e o simulador vêm depois, porque dependem da paridade da T16.6.

---

## 8. Decisões do dono do projeto (2026-10-14)

1. **Nome da aba:** "Laboratório".
2. **Calendário econômico:** várias fontes, com enriquecimento. O CSV continua, e entra a exportação do calendário do
   próprio MT5 por MQL5 (seção 3.3, T16.3a).
3. **Martingale:** o Laboratório mostra o comportamento da operação com martingale quando a estratégia e o robô o
   suportam, respeitando todas as travas de segurança daquele robô (seção 4.4).
4. **Separação dos dados para validar regras:** a forma mais abrangente e segura que for viável: walk-forward ancorado
   com embargo, mais uma reserva final intocada, com redução automática quando há poucas operações (seção 6.1).

## 9. O que este plano não promete

- Re-simular sobre M1 não é o mesmo que o Strategy Tester com ticks reais: a ordem dentro do minuto é suposta
  (conservadora). Quando a ordem exata importar, a validação final é pelo Strategy Tester (I-08).
- Insight validado fora da amostra é **evidência**, não recomendação de operação.
- Robôs MQL5 só terão análise completa depois da T16.2b no HADOUKEN.

---

## 10. Andamento (atualize a cada tarefa: é o ponto de passagem entre agentes)

**Como continuar daqui:** cada tarefa é uma branch empilhada sobre a anterior (`tarefa/T16.<n>-...`), com PR para a
branch de baixo. Antes de começar, leia a última linha desta tabela, faça `git fetch` e crie a sua branch a partir da
branch da última tarefa concluída (ou do `master`, se ela já foi mesclada). Só mexa em arquivos da Fase 16
(`mt5_extracao/trade_analytics/`, `mt5_extracao/ui/laboratory.py`, `mt5_extracao/ui/charts.py`, `tests/test_trade_*`)
e, nos arquivos compartilhados, só nos pontos citados na tarefa. Testes do PostgreSQL do Laboratório vão em
`tests/test_*_pg.py` (o job de PostgreSQL do CI já roda esse padrão).

| Tarefa | Branch | PR | Estado | O que ficou pronto e o que falta |
|---|---|---|---|---|
| T16.0 | `tarefa/T16.0-plano-analytics` | #56 | ✅ | Plano e decisões do dono |
| T16.1 | `tarefa/T16.1-livro-operacoes` | (ver PR da branch) | ✅ | `trade_analytics/ledger.py`: `_trade_ledger` com `save_trades`, `load` (lentes robô e ativo, período), `runs`, `delete_run`; colunas acrescentadas em base antiga; `completo`/`parcial`. Testes: `tests/test_trade_ledger.py` e `tests/test_trade_analytics_pg.py`. Falta: nada; quem grava no livro é a T16.2 |
| T16.2 | `tarefa/T16.2-importadores` | (ver PR da branch) | ✅ (lado mt5x) | `trade_analytics/importers.py`: backtest Python grava operações completas no livro (`robots/backtest.run_python`, 2 linhas); run do Strategy Tester grava pelo `HDK_trades.csv` nos dois formatos (`run_mql5`, 3 linhas; a leitura antiga do card não mudou); `import_existing_runs` traz runs antigos como `parcial`; bateria e paridade não entram; falha no livro nunca derruba o backtest. `mt5x trades import` e `mt5x trades runs` (`trade_analytics/commands.py`, registrado no `cli.py` junto do `resources`). Testes: `tests/test_trade_importers.py`. **Falta a T16.2b no HADOUKEN** (abaixo) |
| T16.2b | (repositório HADOUKEN) | — | ⏳ aguardando checkout com código | Acrescentar ao `HDK_trades_<magic>.csv` as colunas `entry_time;entry_price;exit_price;sl;tp;direction;exit_reason`, **depois** das atuais `time;symbol;type;volume;result` (nessa ordem; separador `;`; horário `AAAA.MM.DD HH:MM`; `direction` = `buy`/`sell`; `sl`/`tp` = 0 quando não houver; `exit_reason` = `sl`, `tp`, `sinal`, `tempo` ou `fim`). O leitor do mt5x já aceita os formatos antigo e novo (`read_hdk_trades_full`). Checkout `HADOUKEN-MQL5-Framework` vazio; `origin` não tem branches nem commits; não alterado. |
| T16.3a | `tarefa/T16.3a-calendario` | (ver PR da branch) | ✅ (Linux) | `trade_analytics/calendar/`: tabela `_economic_events`; fontes `csv` (o `events_file` de sempre, com colunas opcionais `name,country,currency,actual,forecast,previous,override`) e `mt5` (script `trade_analytics/mql5/MT5X_CalendarExport.mq5`, só leitura, grava `Common\Files\MT5X_calendar_<servidor>.csv`, **compilado no MetaEditor do host**); `merged` une o mesmo evento das fontes (horário a 1 min, moeda, nome parecido), escolhe o impacto (manual > MT5 > maior) e calcula a surpresa; `events_for` liga eventos ao ativo (moeda de lucro, moedas no nome do par, `event_symbols`, `*` do CSV). `mt5x trades calendar import/status/install/export`. Chaves novas em `[TRADE_ANALYTICS]`. Testes: `tests/test_trade_calendar.py`. **Não visto:** a exportação rodando no terminal conectado (o script compila; executar exige o MT5 aberto e conectado, e o terminal do host é compartilhado com outro agente) — próximo agente: rodar `mt5x trades calendar export` com o MT5 fechado e depois `import --source mt5` |
| T16.3 | `tarefa/T16.3-enriquecimento` | (ver PR da branch) | ✅ | `trade_analytics/enrich.py`: `enrich(db, config, trades)` devolve, por operação, calendário (dia da semana, do mês, semana do mês, mês, fim de mês, hora, faixa), minutos desde a abertura do dia, notícia mais forte na janela (impacto, minutos até o evento, nome, surpresa), volume e percentil, ATR, ATR%, largura de Bollinger, retorno anterior, percentil e faixa de volatilidade **point-in-time**, ganho/perda, duração, MAE e MFE. Operação parcial recebe só calendário pela saída. Chaves `time_bucket_min` e `volatility_period`. `FEATURE_VERSION = 1` (a T16.5 usa para o cache). Testes: `tests/test_trade_enrich.py` (inclui a armadilha de olhar o futuro) |
| T16.3b | `tarefa/T16.3b-enriquecimento-temporal` | — | ✅ local (`acc1872`) | Barra aberta, notícia posterior à entrada, volume real × ticks e limite temporal de MAE/MFE; reauditoria focada 23. Suíte 806/9 é evidência anterior; `doctor` 16/2/0. M1/OHLC permanece aproximado; não integrada |
| T16.3c | `tarefa/T16.3c-calendario-point-in-time` | — | ✅ local (`20f8a87`) | Guarda primeira observação e revisões de eventos econômicos; `load_asof`/`merged_asof`, cache e `source_received_at` explícito cobertos em 30 focados. Suíte 813/9 é anterior ao teste novo; `doctor` 16/2/0. Replay com recibos reais e validação Windows pendentes; não integrada |
| T16.4 | `tarefa/T16.4-agregacoes` | (ver PR da branch) | ✅ | `trade_analytics/stats.py` (métricas com as fórmulas de `compute_metrics`, conferidas em teste; bootstrap em blocos de dias) e `trade_analytics/aggregate.py` (`frame` = livro + enriquecimento; `by_dimension`, `crosstab` com valor/n/pequena, `robot_view`, `asset_view`, `equity_curves`, `daily_correlation`). Chave `min_trades_cell`. Testes: `tests/test_trade_aggregate.py` (efeitos plantados — sexta negativa, 10h positiva — aparecem; célula pequena marcada) |
| T16.5 | `tarefa/T16.5-cache` | (ver PR da branch) | ✅ | `trade_analytics/cache.py`: `_trade_features` + `_trade_features_runs` (assinatura por run: versão das fórmulas, chaves de `[TRADE_ANALYTICS]` que entram na conta, operações do run e calendário). `aggregate.frame` passa a usar o cache. Teste de desempenho com 50 mil operações (crosstab + by_dimension abaixo de 1,5 s no CI). As tabelas novas aparecem sozinhas no inventário da aba Recursos (Fase 15). Testes: `tests/test_trade_cache.py` |
| T16.11a | `tarefa/T16.11a-aba-laboratorio` | (ver PR da branch) | ✅ (Linux, tela virtual, temas claro e escuro) | Primeira parte da aba: `ui/charts.py` (heatmap divergente azul↔laranja com n por célula e trama nas células com n < 30, barras, curvas sobrepostas, dispersão vencedoras × perdedoras com forma e cor, dicas ao parar o mouse; `$` de WIN$N escapado) e `ui/laboratory.py` (lentes por robô e por ativo, período, ocultar notícias de impacto alto, indicadores com intervalo de 90%, guias Visão geral, Tempo e calendário, Microestrutura e Comparativo; Execução e Insights desabilitadas com o motivo; botão "Importar backtests antigos"; avisos de amostra pequena, operações parciais e calendário vazio). A aba entra depois de "Robôs" (`ui_manager.py`: registro e menu Exibir, pelo widget). Testes: `tests/test_ui_laboratory.py`. T16.11b (Execução/Insights) foi entregue no master; T16.11c foi implementada localmente em `tarefa/T16.11c-interacoes-laboratorio` (HEAD `701f4fd`), com 40 testes de UI focados, suíte registrada 789/9 e `doctor` 16/2/0; ainda fora do master. |
| T16.6 | `tarefa/T16.6-resimulacao` | (ver PR da branch) | ✅ | `trade_analytics/execution/engine.py` (motor barra a barra com as regras do backtester: ordem intrabarra, stop antes do alvo, gap na abertura, bid/ask, spread por barra, escorregamento, comissão, swap; saídas não ligadas a preço mantidas do run original) e `execution/runner.py` (`inputs`, `base_policy`, `resimulate`, grava como `resim:<modo>`). **Paridade com o backtester nas seis estratégias Python**, com custos. No backtester (aditivo): `Trade` ganhou `stop_price`/`target_price` e o spread virou `execution_spreads` (sem mudar o comportamento). `ledger.load`/`runs` excluem `resim:*` por padrão (as lentes não contam em dobro). Testes: `tests/test_trade_resim.py` |
| T16.7 | `tarefa/T16.7-modos-saida` | (ver PR da branch) | ✅ | `execution/modes.py`: registro aberto de modos (`ponto_fixo`, `alvo_r`, `break_even`, `trailing_atr`, `trailing_barras`), cada um com nome, explicação e parâmetros; o motor ganhou `breakeven_r` e `trailing_bars`. `runner.compare` re-simula o mesmo run em vários modos sobre as mesmas entradas (barras lidas uma vez). Testes: `tests/test_trade_modes.py` |
| T16.8 | `tarefa/T16.8-dimensionamento` | (ver PR da branch) | ✅ | Motor com **gradiente** na mesma operação (`grid_*` na `ExitPolicy`): contra/preço médio com espaçamento fixo ou k × ATR medido antes da entrada, a favor/piramidação com stop do conjunto subindo; entrada adicional no ask (compra) ou bid (venda), nível tocado junto com o stop conta como preenchido antes (pior caso), teto de exposição. Modos `gradiente_fixo`, `gradiente_dinamico`, `gradiente_direcional`. **Martingale e anti-martingale** (`execution/sizing.py`): só para robô com parâmetro de martingale e sem `block_averaging` (`martingale_support`), com as travas do robô (`robot_safety`: risco por operação, perda diária, drawdown, `volume_max`; o que o robô não declara vem do teto global e é anotado), teto de multiplicações, ruína (maior sequência, maior lote, maior perda) e contagem de cada trava; `runner.compare` recusa martingale para robô sem suporte. Testes: `tests/test_trade_sizing.py` |
| T16.9 | `tarefa/T16.9-regras-simulador` | (ver PR da branch) | ✅ | `trade_analytics/rules.py`: regras como dados (`excluir_dia`, `janela_horaria`, `excluir_noticia`, `excluir_volatilidade`, `trocar_modo` com o resultado re-simulado), `describe` (texto da regra), `split` (walk-forward ancorado com folga do tamanho da operação mais longa + reserva final; menos provas com pouca amostra; "evidência fraca" quando nem duas cabem) e `evaluate` (antes × depois na procura, em cada prova e na reserva). Chaves `wf_folds` e `holdout_fraction`. Testes: `tests/test_trade_rules.py` (reserva nunca usada na procura; efeito real melhora em todas as provas e na reserva; ruído não dá salto) |
| T16.10 | `tarefa/T16.10-insights` | (ver PR da branch) | ✅ | `trade_analytics/insights.py`: `discover` (dia, faixa horária, notícia, volatilidade; Welch, Benjamini-Hochberg a 1% sobre todas as combinações; walk-forward em que cada prova só vê a sua procura; **efeito confirmado na reserva final com teste próprio a 5%**; texto pedagógico com n, tentativas e reserva) e `mode_insights` (modo × contexto, teste pareado nas mesmas operações; regra `trocar_modo`). `_trade_insights` (aditiva). `rules` ganhou `excluir_janela`. **Teste do ruído: 60 sorteios de ruído puro, nenhum insight validado** (a 1ª versão validava 2 — critérios endurecidos e documentados). Testes: `tests/test_trade_insights.py` |
| T16.11b | `tarefa/T16.11b-execucao-insights` | (ver PR da branch) | ✅ (Linux, tela virtual) | Guias **Execução** (escolher run e modos; martingale só aparece para robô que o suporta; tabela modo × acerto, fator de lucro, payoff, expectativa, queda, melhor faixa e observações — entradas adicionais, exposição, travas disparadas, "só estudo"; curvas sobrepostas; heatmap faixa horária × modo) e **Insights** (procura com estado e texto; marcar insights e regras manuais; simular com procura, provas e reserva final, curva antes × depois com a linha do início da reserva; aviso quando a reserva não melhora; exportar proposta). `trade_analytics/export.py` grava `exports/propostas/<escopo>_<data>.json` com regras, números, tentativas e avisos (martingale reprovável, gradiente só via HADOUKEN, evidência fraca). Correção junto: teste do card de backtest espera o resultado em vez de tempo fixo (falhava no Windows). Testes: `tests/test_ui_laboratory.py`, `tests/test_trade_export.py`. **Ambiente:** neste host, o display `:0` às vezes fica inacessível para o shell e os testes de interface ficam minutos esperando o X; rode a suíte com `xvfb-run -a` (assim: 787 aprovados, 9 pulados, ~6 min; uma rodada anterior caiu uma vez com "instrução ilegal" num módulo nativo e não se repetiu — se acontecer, rode de novo com `python -X faulthandler`). T16.11c implementada localmente em `tarefa/T16.11c-interacoes-laboratorio` (HEAD `701f4fd`); 40 testes de UI focados, suíte registrada 789/9, `doctor` 16/2/0; não integrada. |
| T16.12 | `tarefa/T16.12-cli-relatorio` | — | ✅ local (HEAD `adff6eb`, código `e99becd`) | Preserva `compare`, `report --by`, filtros de simulação e `insights --save`; adiciona `enrich`, relatório detalhado, re-simulação por run e `export --rules-file`. `--json` segue o envelope global. Persistência da re-simulação exige `--save`; `export` produz proposta não aplicável. Testes focados 15; suíte 794/9; `doctor` 16 OK/2 avisos/0 falhas. Validação Windows pendente; HTML opcional não feito; não integrada |

**Estado da Fase 16 (2026-10-14):** T16.0 a T16.12 entregues em PRs empilhados (#56 a #70), na ordem da tabela acima. Merge na ordem: #56 → #57 → ... → #70 (cada PR aponta para a branch de baixo). Pendências: T16.2b (colunas novas no `HDK_trades.csv`, no HADOUKEN), exportação do calendário com o MT5 conectado (o script compila), validação no Windows, TimescaleDB/PostgreSQL real na re-simulação e relatório HTML. T16.11c, que era opcional, foi implementada em branch local e continua fora do master.
