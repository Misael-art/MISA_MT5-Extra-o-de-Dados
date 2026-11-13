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
| T16.2b | (repositório HADOUKEN) | — | a fazer | Acrescentar ao `HDK_trades_<magic>.csv` as colunas `entry_time;entry_price;exit_price;sl;tp;direction;exit_reason`, **depois** das atuais `time;symbol;type;volume;result` (nessa ordem; separador `;`; horário `AAAA.MM.DD HH:MM`; `direction` = `buy`/`sell`; `sl`/`tp` = 0 quando não houver; `exit_reason` = `sl`, `tp`, `sinal`, `tempo` ou `fim`). O leitor do mt5x já aceita esse formato e o antigo (`read_hdk_trades_full`); nada mais muda no mt5x |
| T16.3a | `tarefa/T16.3a-calendario` | (ver PR da branch) | ✅ (Linux) | `trade_analytics/calendar/`: tabela `_economic_events`; fontes `csv` (o `events_file` de sempre, com colunas opcionais `name,country,currency,actual,forecast,previous,override`) e `mt5` (script `trade_analytics/mql5/MT5X_CalendarExport.mq5`, só leitura, grava `Common\Files\MT5X_calendar_<servidor>.csv`, **compilado no MetaEditor do host**); `merged` une o mesmo evento das fontes (horário a 1 min, moeda, nome parecido), escolhe o impacto (manual > MT5 > maior) e calcula a surpresa; `events_for` liga eventos ao ativo (moeda de lucro, moedas no nome do par, `event_symbols`, `*` do CSV). `mt5x trades calendar import/status/install/export`. Chaves novas em `[TRADE_ANALYTICS]`. Testes: `tests/test_trade_calendar.py`. **Não visto:** a exportação rodando no terminal conectado (o script compila; executar exige o MT5 aberto e conectado, e o terminal do host é compartilhado com outro agente) — próximo agente: rodar `mt5x trades calendar export` com o MT5 fechado e depois `import --source mt5` |
| T16.3 | `tarefa/T16.3-enriquecimento` | (ver PR da branch) | ✅ | `trade_analytics/enrich.py`: `enrich(db, config, trades)` devolve, por operação, calendário (dia da semana, do mês, semana do mês, mês, fim de mês, hora, faixa), minutos desde a abertura do dia, notícia mais forte na janela (impacto, minutos até o evento, nome, surpresa), volume e percentil, ATR, ATR%, largura de Bollinger, retorno anterior, percentil e faixa de volatilidade **point-in-time**, ganho/perda, duração, MAE e MFE. Operação parcial recebe só calendário pela saída. Chaves `time_bucket_min` e `volatility_period`. `FEATURE_VERSION = 1` (a T16.5 usa para o cache). Testes: `tests/test_trade_enrich.py` (inclui a armadilha de olhar o futuro) |
| T16.4 | `tarefa/T16.4-agregacoes` | (ver PR da branch) | ✅ | `trade_analytics/stats.py` (métricas com as fórmulas de `compute_metrics`, conferidas em teste; bootstrap em blocos de dias) e `trade_analytics/aggregate.py` (`frame` = livro + enriquecimento; `by_dimension`, `crosstab` com valor/n/pequena, `robot_view`, `asset_view`, `equity_curves`, `daily_correlation`). Chave `min_trades_cell`. Testes: `tests/test_trade_aggregate.py` (efeitos plantados — sexta negativa, 10h positiva — aparecem; célula pequena marcada) |
| T16.5 | `tarefa/T16.5-cache` | (ver PR da branch) | ✅ | `trade_analytics/cache.py`: `_trade_features` + `_trade_features_runs` (assinatura por run: versão das fórmulas, chaves de `[TRADE_ANALYTICS]` que entram na conta, operações do run e calendário). `aggregate.frame` passa a usar o cache. Teste de desempenho com 50 mil operações (crosstab + by_dimension abaixo de 1,5 s no CI). As tabelas novas aparecem sozinhas no inventário da aba Recursos (Fase 15). Testes: `tests/test_trade_cache.py` |
| T16.11a | `tarefa/T16.11a-aba-laboratorio` | (ver PR da branch) | ✅ (Linux, tela virtual, temas claro e escuro) | Primeira parte da aba: `ui/charts.py` (heatmap divergente azul↔laranja com n por célula e trama nas células com n < 30, barras, curvas sobrepostas, dispersão vencedoras × perdedoras com forma e cor, dicas ao parar o mouse; `$` de WIN$N escapado) e `ui/laboratory.py` (lentes por robô e por ativo, período, ocultar notícias de impacto alto, indicadores com intervalo de 90%, guias Visão geral, Tempo e calendário, Microestrutura e Comparativo; Execução e Insights desabilitadas com o motivo; botão "Importar backtests antigos"; avisos de amostra pequena, operações parciais e calendário vazio). A aba entra depois de "Robôs" (`ui_manager.py`: registro e menu Exibir, pelo widget). Testes: `tests/test_ui_laboratory.py`. **Falta (T16.11b):** guias Execução e Insights, clique na célula filtrando as outras guias, "Abrir no Laboratório" no painel do robô |
| T16.6 | `tarefa/T16.6-resimulacao` | (ver PR da branch) | ✅ | `trade_analytics/execution/engine.py` (motor barra a barra com as regras do backtester: ordem intrabarra, stop antes do alvo, gap na abertura, bid/ask, spread por barra, escorregamento, comissão, swap; saídas não ligadas a preço mantidas do run original) e `execution/runner.py` (`inputs`, `base_policy`, `resimulate`, grava como `resim:<modo>`). **Paridade com o backtester nas seis estratégias Python**, com custos. No backtester (aditivo): `Trade` ganhou `stop_price`/`target_price` e o spread virou `execution_spreads` (sem mudar o comportamento). `ledger.load`/`runs` excluem `resim:*` por padrão (as lentes não contam em dobro). Testes: `tests/test_trade_resim.py` |
| T16.7 | `tarefa/T16.7-modos-saida` | (ver PR da branch) | ✅ | `execution/modes.py`: registro aberto de modos (`ponto_fixo`, `alvo_r`, `break_even`, `trailing_atr`, `trailing_barras`), cada um com nome, explicação e parâmetros; o motor ganhou `breakeven_r` e `trailing_bars`. `runner.compare` re-simula o mesmo run em vários modos sobre as mesmas entradas (barras lidas uma vez). Testes: `tests/test_trade_modes.py` |
| T16.8 | `tarefa/T16.8-dimensionamento` | (ver PR da branch) | ✅ | Motor com **gradiente** na mesma operação (`grid_*` na `ExitPolicy`): contra/preço médio com espaçamento fixo ou k × ATR medido antes da entrada, a favor/piramidação com stop do conjunto subindo; entrada adicional no ask (compra) ou bid (venda), nível tocado junto com o stop conta como preenchido antes (pior caso), teto de exposição. Modos `gradiente_fixo`, `gradiente_dinamico`, `gradiente_direcional`. **Martingale e anti-martingale** (`execution/sizing.py`): só para robô com parâmetro de martingale e sem `block_averaging` (`martingale_support`), com as travas do robô (`robot_safety`: risco por operação, perda diária, drawdown, `volume_max`; o que o robô não declara vem do teto global e é anotado), teto de multiplicações, ruína (maior sequência, maior lote, maior perda) e contagem de cada trava; `runner.compare` recusa martingale para robô sem suporte. Testes: `tests/test_trade_sizing.py` |
| T16.9 | `tarefa/T16.9-regras-simulador` | (ver PR da branch) | ✅ | `trade_analytics/rules.py`: regras como dados (`excluir_dia`, `janela_horaria`, `excluir_noticia`, `excluir_volatilidade`, `trocar_modo` com o resultado re-simulado), `describe` (texto da regra), `split` (walk-forward ancorado com folga do tamanho da operação mais longa + reserva final; menos provas com pouca amostra; "evidência fraca" quando nem duas cabem) e `evaluate` (antes × depois na procura, em cada prova e na reserva). Chaves `wf_folds` e `holdout_fraction`. Testes: `tests/test_trade_rules.py` (reserva nunca usada na procura; efeito real melhora em todas as provas e na reserva; ruído não dá salto) |
| T16.10 | `tarefa/T16.10-insights` | (ver PR da branch) | ✅ | `trade_analytics/insights.py`: `discover` (dia, faixa horária, notícia, volatilidade; Welch, Benjamini-Hochberg a 1% sobre todas as combinações; walk-forward em que cada prova só vê a sua procura; **efeito confirmado na reserva final com teste próprio a 5%**; texto pedagógico com n, tentativas e reserva) e `mode_insights` (modo × contexto, teste pareado nas mesmas operações; regra `trocar_modo`). `_trade_insights` (aditiva). `rules` ganhou `excluir_janela`. **Teste do ruído: 60 sorteios de ruído puro, nenhum insight validado** (a 1ª versão validava 2 — critérios endurecidos e documentados). Testes: `tests/test_trade_insights.py` |
| T16.11b | `tarefa/T16.11b-execucao-insights` | (ver PR da branch) | ✅ (Linux, tela virtual) | Guias **Execução** (escolher run e modos; martingale só aparece para robô que o suporta; tabela modo × acerto, fator de lucro, payoff, expectativa, queda, melhor faixa e observações — entradas adicionais, exposição, travas disparadas, "só estudo"; curvas sobrepostas; heatmap faixa horária × modo) e **Insights** (procura com estado e texto; marcar insights e regras manuais; simular com procura, provas e reserva final, curva antes × depois com a linha do início da reserva; aviso quando a reserva não melhora; exportar proposta). `trade_analytics/export.py` grava `exports/propostas/<escopo>_<data>.json` com regras, números, tentativas e avisos (martingale reprovável, gradiente só via HADOUKEN, evidência fraca). Correção junto: teste do card de backtest espera o resultado em vez de tempo fixo (falhava no Windows). Testes: `tests/test_ui_laboratory.py`, `tests/test_trade_export.py`. **Ambiente:** neste host, o display `:0` às vezes fica inacessível para o shell e os testes de interface ficam minutos esperando o X; rode a suíte com `xvfb-run -a` (assim: 787 aprovados, 9 pulados, ~6 min; uma rodada anterior caiu uma vez com "instrução ilegal" num módulo nativo e não se repetiu — se acontecer, rode de novo com `python -X faulthandler`). **Falta (T16.11c, opcional):** clique numa célula filtrando as outras guias; "Abrir no Laboratório" no painel do robô |
| T16.12 | `tarefa/T16.12-cli` | (ver PR da branch) | ✅ | `mt5x trades report --robot X | --symbol Y [--by ...]`, `compare --run R [--modes ...] [--preset P]` (martingale recusado para robô sem suporte), `insights --robot|--symbol [--save]`, `simulate --robot|--symbol [--exclude-days] [--window HH:MM-HH:MM] [--no-news] [--no-high-vol] [--rules JSON] [--export]`; todos com `--json`. Testes: `tests/test_trade_cli.py`. **Não feito:** relatório HTML (era opcional no plano; o painel vivo da T11.9 é o lugar natural) |

**Estado da Fase 16 (2026-10-14):** T16.0 a T16.12 entregues em PRs empilhados (#56 a #70), na ordem da tabela acima. Merge na ordem: #56 → #57 → ... → #70 (cada PR aponta para a branch de baixo). Pendências: T16.2b (colunas novas no `HDK_trades.csv`, no HADOUKEN), exportação do calendário com o MT5 conectado (o script compila), validação no Windows, TimescaleDB/PostgreSQL real na re-simulação, T16.11c opcional (clique na célula filtrando as guias; "Abrir no Laboratório" no painel do robô) e relatório HTML.

---

## 11. Pesquisa de estratégias, excursões, fidelidade de ticks e mitigação de perdas — 09/11/2026

**Estado: objetivos e hipóteses registrados para continuidade; investigação/validação específica pendente.**
Este adendo é a fonte canônica do pedido de 09/11/2026. Qualquer agente deve lê-lo antes de
aprofundar uma frente, consultar os contratos indicados e manter aqui conclusões, limitações e
próximos passos. Não há autorização para ativar negociação real, modificar os controles de risco,
repinar HADOUKEN/Safety ou implementar a estratégia operacional nesta etapa documental.

**Gatilho do estudo:** vídeo ["MARTINGALE vs LOTE FIXO: A Diferença é ASSUSTADORA!"](https://www.youtube.com/watch?v=ddyNmgahFn8)
(No Risk No Gain — Robôs Para Day Trade). A transcrição integral, os parâmetros e os resultados
quantitativos do vídeo **ainda precisam de auditoria independente**: título e hipótese motivadora
não constituem evidência de que uma abordagem vença ou perca em determinada amostra. A análise
deve comparar explicitamente o que o vídeo demonstra, o que o MT5X já calcula, as diferenças de
fonte/método e as lacunas, sem atribuir ao vídeo números não verificados.

### 11.1 Pergunta central e escopo das quatro frentes

Avaliar se sinais causais e mensuráveis de reversão, surgidos quando uma operação está
desfavorável e perto do stop, permitem **mitigar a perda líquida** com gerenciamento de saída
ou uma adição limitada de posição, sem piorar de maneira inaceitável a cauda de perdas, o
capital exigido e o risco de ruína. A investigação separa quatro entregas:

1. **Estratégias e dimensionamento:** formalizar lote fixo, Martingale entre operações,
   Anti-Martingale, gradientes, preço médio na mesma posição, adição condicional ao sinal e
   redução/encerramento antecipado. Contrastar matemática de exposição, expectativa, custo,
   chance de recuperação, drawdown, risco extremo e suportes reais do robô, em vez de usar
   "Martingale" indiscriminadamente para qualquer aumento de lote.
2. **Indicadores e análises:** auditar MAE/MFE, momento e duração dos extremos, trajetórias
   antes/depois do gatilho, máximo lucro/perda flutuante, recuperação, tempo em risco,
   exposição máxima, utilização de margem, eficiência de captura e apresentação em CLI/UI;
   indicar quais métricas têm evidência suficiente por fonte.
3. **Ticks reais e qualidade do backtest:** percorrer MT5 → conector → extrator → banco →
   qualificador → snapshot imutável → consumidor → motor/Testador → relatório. Distinguir
   `COPY_TICKS_ALL`, percentuais reportados pelo Tester, origem observada, **cobertura integral**,
   ordem dos eventos e execuções efetivas. Determinar quais simulações reproduzem de fato
   Bid/Ask, spread, custos, gaps e eventos intrabarra.
4. **Mitigação condicional da perda:** definir sinais observáveis de reversão, gatilhos,
   restrições de adição, invalidação, saídas e comparadores. Primeiro estudar em dados
   históricos qualificados e QA isolada; nenhuma mudança automática no executor real.

### 11.2 Definições para impedir conclusões enganosas

| Medida | Definição e cuidado obrigatório |
|---|---|
| `MFE` / excursão favorável máxima | Maior deslocamento **a favor** observado entre entrada e saída (compra: valorização; venda: queda), em preço/pontos; para resultado monetário, usar preço efetivamente liquidável, direção, posição no instante e custos. |
| `MAE` / excursão adversa máxima | Maior deslocamento **contra** observado no mesmo intervalo. Guardar também o instante, idade da operação, duração aproximada e fonte/resolução; um máximo observado em OHLC não certifica sequência de ticks. |
| `mfe_time` / `mae_time` | Primeiro extremo **observado**, com empate/ordem declarados quando determináveis; em OHLC devolver intervalo ou desconhecido. Não transformar timestamp da barra em instante preciso de toque. |
| `floating_pnl_min/max` | Extremos de PnL ainda aberto, com marcação pelo Bid para compra e Ask para venda, volume vigente, comissões/swap/spread e convenção explícita de custos; sinal e unidades padronizados. |
| `max_exposure` / `max_margin` | Maior volume nocional/contratos e margem exigida simultaneamente, com horários e duração. **Exposição máxima ganhadora/perdedora** deve identificar se o vídeo se refere a excursão monetária, volume ou margem; não fundir conceitos. |
| `recovery_time` / `rebound` | Tempo desde um fundo adverso até nível predefinido (zero flutuante líquido, entrada, alvo parcial ou outro), com censura se não houve recuperação até a saída. Só contar eventos vistos depois do sinal. |
| `capture_efficiency` | Lucro líquido capturado / MFE líquida comparável, quando MFE positiva e denominador confiável; negativos, zero, adições e operações parciais precisam regra explícita. |
| `R` | Resultado líquido dividido pelo risco monetário inicial **comprovado** e positivo; não recalcular retroativamente usando o stop movido nem confundir R inicial com risco extra após adição. |
| `drawdown` / `risk_of_ruin` | Queda da curva de patrimônio, perdas máximas e probabilidade de romper limite de capital sob hipóteses declaradas; métricas dependem de trajetória, tamanho de posição e custos, não só da taxa de acerto. |

Para comparar uma operação com e sem adição, produzir **duas lentes**: (a) trajetória do
sinal/preço com **volume inicial constante** e (b) trajetória da carteira com volume que muda
em cada fill. Caso contrário, o aparente MAE/MFE pode melhorar só por mudar o denominador.
Preservar preço médio ponderado por quantidade, comissão, spread, slippage, multiplicador,
moeda e margem; distinguir stop no gráfico de preço executável e de perda monetária global.
Agregações por ativo/robô precisam sessão, timezone de origem, ponto/tick-size e vigência
do contrato, com resultados em pontos, moeda e R somente quando elegíveis.

### 11.3 Estratégia proposta em termos verificáveis

**Exemplo de compra:** posição comprada anda em direção ao stop; antes de tocá-lo, surge
um sinal de reversão **conhecido naquele instante**. Hipótese: comprar um lote adicional
(ou multiplicador limitado), deslocar o preço médio ponderado e sair em um repique que
reduza o prejuízo, permita break-even **líquido** ou recupere parte do movimento. Se o
sinal perder validade e o preço retomar a direção adversa, reduzir/zerar conforme um
plano predefinido. **Venda:** espelhar direções e lados Bid/Ask. A adição deve ser na
mesma direção da posição original para reduzir seu preço médio de entrada; uma ordem no
sentido oposto é hedge/redução de exposição e exige **experimento separado**, sobretudo
por diferenças entre contas *netting* e *hedging*.

Especificar **antes** da procura estatística: definição de proximidade do stop (pontos,
ATR ou fração de `R`), sinais candidatos (desaceleração, estrutura de preço, regime,
retorno à média, fluxo quando comprovado), momento em que cada sinal fica disponível,
volume permitido, máximo de adições, intervalo entre entradas, faixa temporal, teto de
risco agregado, stop/saída de invalidação e take-profit/saída de oportunidade. Congelar
cada configuração por experimento, registrando custos e cenários adversos. Não usar
o próprio repique futuro para gerar o gatilho (*look-ahead*).

**Alerta matemático:** baixar o preço médio **não** reduz automaticamente a perda
monetária nem o risco. Uma adição eleva a derivada do PnL em relação ao preço e pode
ampliar muito a perda se o movimento continuar; o ponto de equilíbrio melhora em preço,
mas piora a exigência de margem e pode superar o orçamento de perda original. Qualquer
hipótese que permita ultrapassar o stop inicial deve reportar explicitamente **perda
máxima de cenário**, capital adicional exigido, distância ao novo limite, travas e
probabilidade/gravidade de gaps. Sem limites demonstrados, a hipótese não é elegível.

### 11.4 Grupos experimentais comparáveis

| ID | Variante | Pergunta isolada |
|---|---|---|
| H0 | Entrada e stop originais, lote fixo, sem intervenção | Qual o baseline econômico e de risco? |
| H1 | Mesmo lote; saída antecipada quando sinal de continuação adversa confirma | Quanto se mitiga apenas saindo mais cedo, sem capital extra? |
| H2 | Uma única adição limitada após sinal causal de reversão | O repique compensa custos e a exposição extra? |
| H3 | Adição proporcional limitada, multiplicador predefinido | Sensibilidade do resultado e da cauda de perdas ao volume adicional. |
| H4 | H2 com stop global, parcial, break-even/trailing e invalidação objetivos | Quanto a gestão posterior acrescenta? |
| H5 | Lote fixo, Martingale entre operações, Anti-Martingale e gradientes existentes | A suposta vantagem decorre da regra ou somente do dimensionamento? |
| H6 | Encerrar integralmente a posição perdida e abrir uma operação de reversão independente | O **sinal** tem mérito mesmo sem carregar e reforçar uma posição perdedora? |

Usar exatamente os mesmos sinais de entrada, instantes, ativos, sessão, custos,
condições de mercado e conjuntos de teste para variantes pareadas. Uma operação
original dá origem a **um caso de comparação**, mesmo se houver várias ordens,
fills, saídas parciais ou hipóteses; não inflar o tamanho da amostra. Comparar
expectativa líquida por entrada/ciclo e por unidade de risco, distribuições,
caudas, maior perda, drawdown, margem máxima, duração, custo, liquidação forçada,
contagem de intervenções e concentração de ganhos em poucos casos. Não escolher
vencedor por taxa de acerto, preço médio ou retorno bruto isolados.

### 11.5 Cobertura inicial MT5X: evidência, não promessa

| Capacidade | Evidência existente e contrato reutilizável | Pendência específica do estudo |
|---|---|---|
| Livro, MAE/MFE, re-simulação e modos | F16: `trade_analytics/ledger.py`, `enrich.py`, `execution/engine.py`, `modes.py`, `sizing.py`, `runner.py`; histórico de tarefas nesta página | Confirmar na versão/execução alvo cálculo por origem e disponibilidade de métricas para H0–H6; MAE/MFE básicos não implicam as duas lentes monetárias. |
| Extremos e ciclo temporal | [T19.14](../analytics/T19.14_TRAJETORIAS_TEMPORAIS.md), consulta `mt5x --json trades trajectory --run ID`, sidecar `_trade_trajectories` | Medir consistência, tempos, empates, censura, adições/parciais, vínculo a ledger, cobertura efetiva e apresentação; nativo pode faltar eventos. |
| Coleta de ticks | `mt5_connector.py` usa `copy_ticks_range(..., COPY_TICKS_ALL)`; `tick_extractor.py` captura em blocos com retomada | Auditar armazenamento real, intervalos, timezones, flags, Bid/Ask/Last, `volume_real`, duplicação, monotonicidade, lacunas e reconciliação de volumes/tempos sem descartar eventos legítimos. |
| Elegibilidade de ticks | [T20.6](../analytics/T20.6_QUALIDADE_CAPACIDADES.md), `analytics/data_eligibility.py`, snapshots/revisões T20.4 | `complete_real_ticks` pode ficar desconhecido sem denominador/atestado; obter evidência por ativo/intervalo; verificar consumidor e relatório do Tester no mesmo run. |
| Padrões e validação temporal | [Descoberta T13.4](../analytics/DESCOBERTA_PADROES.md), [Épico I](EPICO_I_PESQUISA_VALIDACAO.md), F16 `rules.py`/`insights.py` | Formalizar sinais de reversão *as-of*, contagem de buscas, comparadores pareados, folds e reserva intocada. |
| Segurança e execução | `robots/risk.py`, `robots/battery.py`; manifestos e travas E14/HADOUKEN/Safety | **Martingale, grid e preço médio são bloqueados para robôs reais** pelas regras vigentes; pesquisa em QA não autoriza mudança de gate ou contrato de execução. |

O motor barra a barra da F16 tem hipóteses conservadoras sobre a ordem intrabarra.
Um replay com ticks *observados* também não reconstrói, sozinho, prioridade de fila,
latência, execução no broker, liquidez disponível, slippage efetivo nem histórico
completo. O Strategy Tester pode oferecer relatório/modelagem adicional, mas
"100%" em um indicador de qualidade não certifica todas essas dimensões. Usar
classes explícitas de confiança: `OHLC/ordem desconhecida`, `ticks observados
parciais`, `ticks com cobertura reportada/conciliada`, `execução/fills nativos
comprovados`; nunca rotular classe inferior como equivalente à superior.

### 11.6 Auditoria de dados antes da primeira conclusão econômica

Para cada `symbol`/janela/`run_id`, guardar identificador de origem, timezone e
offset, UTC `time_msc`, tipo/flags, quantidade de ticks, Bid/Ask/Last, `volume`
e `volume_real` observados (sem substituir valores nulos/zero), primeiro e
último timestamp, janelas vazias, transições de horário, spread anômalo, ticks
sem preço válido, timestamps iguais com sequência/flags diferentes, gaps e
contagem de duplicatas comprovadas. Confrontar o que veio do MT5 com o que foi
persistido e com o snapshot congelado, via contagens, checksums e amostras
reproduzíveis; reportar rejeições e dados classificados como desconhecidos.

Conferir se o consumidor usa esses **mesmos ticks** na avaliação dos gatilhos,
dos extremos e das saídas, e se o teste nativo efetivamente selecionou modo de
"cada tick baseado em ticks reais" para aquele símbolo e intervalo. Registrar
o percentual/relatório extraído da própria execução e as diferenças contra
OHLC/M1 e ticks gerados. Reconstruir fills com lado certo do spread, custos,
ordem de eventos e política conservadora em casos simultâneos. Não inferir
integralidade a partir do sucesso da API, ausência visual de buracos ou número
absoluto de eventos. Testes de estresse e paridade: gaps, spread variável,
alta volatilidade, inversão rápida, mercado sem liquidez, adição seguida de
stop no mesmo intervalo, dados parciais, timezone/DST, netting e hedging.

### 11.7 Método experimental e gates de aceite

1. **Inventário e reprodução:** inspecionar código ativo, contrato de dados,
   runs reais qualificados e versões; gerar casos controlados em **base de QA**
   isolada. Registrar comandos, hashes, parâmetros, fonte, timezone, conta
   sintética/DEMO quando autorizada, data, custos e limitações no recibo já
   existente. Não usar dados privados como fixtures versionadas.
2. **Pré-registro das hipóteses:** fixar universo de ativos, período, tamanho de
   amostra, horário, regra causal, filtros, limites, custos e política de saída;
   contabilizar todas as combinações tentadas, inclusive fracassos. Respeitar
   os próprios critérios de significância e amostra mínima do Épico I/F16.
3. **Separação temporal:** procura somente no treinamento; validação em
   walk-forward com embargo que cobre a duração das posições; reserva final
   intocada até escolha da regra, sem reutilizar resultado para reotimizar.
   Comparar por bootstrap de blocos/sessões e por regime/ativo, com intervalos
   e tratamento de múltiplas hipóteses. Publicar fragilidade quando faltar amostra.
4. **Risco de cauda e viabilidade:** simular custos pessimistas, slippage/gap,
   spread, comissões, swaps, ordem conservadora, margem e risco agregado;
   executar sensibilidade em stop, multiplicador, janela e atraso de execução.
   Uma média melhor com drawdown/capital inaceitáveis **reprova** a variante.
5. **Confronto nativo e relatório:** onde houver permissão e dados adequados,
   confrontar trajetórias e fills com o Tester/MT5; diferenças permanecem
   registradas, sem promover mock, barra, ticks parciais ou DEMO a execução REAL.

**Aceite documental desta etapa:** quatro frentes definidas, H0–H6 separadas,
fontes e métricas identificadas, lacunas honestas, método de ensaio, gates e
ordem de trabalho recuperáveis a partir deste documento e dos handoffs.
**Não aceitos nesta etapa:** fidelidade de ticks, superioridade estatística,
melhoria de perdas, adequação ao capital, implementação da estratégia ou
autorização de operação. Resultados negativos devem ser guardados como achados.

### 11.8 Ordem de aprofundamento e repasse entre agentes

| Ordem | Entrega esperada | Critério para avançar |
|---|---|---|
| A — Ticks | Mapa completo da cadeia, campos, cobertura, divergências e elegibilidade por motor/run; relatório de lacunas por fonte | Reconstrução auditável do que cada teste consumiu, inclusive o que **não** se sabe |
| B — Métricas | Matriz UI/CLI/banco de MAE/MFE, tempos de extremos, PnL flutuante, exposição/margem e custo | Definições, cálculos e disponibilidade comparados contra trajetórias controladas |
| C — Sinais e política | Especificação causal dos gatilhos de reversão e invalidação, limites e saídas para compra/venda e netting/hedging | Sem olhar o futuro; teto de perda/exposição e cenários-limite declarados |
| D — Ensaio | H0–H6 com custos, experimentos pareados e recibos reproduzíveis na QA | Resultado líquido e risco apresentados **juntos**, inclusive falhas |
| E — Robustez | Validação temporal, sensibilidade, cauda, exigência de capital e parecer de viabilidade | Reserva final e condições nativas qualificadas; nenhuma promoção automática à operação |

**Ao retomar:** iniciar por **A — auditoria de ticks** e registrar aqui data,
branch/PR/SHA, entradas testadas, o que ficou provado, o que permanece
desconhecido e próximo passo, com links aos recibos apropriados. Manter
`CONTINUAR_AQUI.md` e `docs/continuidade.md` apontando para esta seção.
Uma frente por PR; preservar o checkout único e trabalhos concorrentes.

### 11.9 Auditoria de código e da base local — 10/11/2026

Inspeção do checkout `codex/launcher-classes`/`550b6d5` (pacote `trade_analytics` idêntico ao
`master` `06b78eb`) e leitura somente leitura de `database/mt5_data.db`. Substitui a coluna
"pendência" de 11.5 pelo que foi **observado**; nada aqui é aceite econômico.

| # | Achado | Evidência | Consequência |
|---|---|---|---|
| A1 | A base local **não tem nenhuma tabela de ticks**. Só EURUSD: M1 com 102.683 barras (22/07–30/10/2026, 8 blocos `ok`) e M5…D1 | `sqlite_master`, `_extraction_log` | Todo backtest/re-simulação local é classe **D** (barras). Nenhum estudo de repique intrabarra é certificável neste host hoje |
| A2 | O caminho de ticks existe e é íntegro em desenho: `copy_ticks_range(COPY_TICKS_ALL)` → `TickExtractor` (blocos, retomada, prova SHA-256 por minuto) → `save_ticks` (chave `time_msc`+`seq`) → `load_market_frame` → snapshot T20.4 | `mt5_connector.py:816`, `tick_extractor.py`, `database_manager.py:752/854`, `data_eligibility.acquisition` | Capacidade de coleta presente; **cobertura integral nunca é afirmada** (`complete_real_ticks` fica `unknown` por construção) |
| A3 | O backtester Python e o motor de re-simulação **não consomem ticks**: decidem em OHLC do TF do run; ticks só calibram spread por hora (`specs --calibrate`) e alimentam trajetórias T19.14 quando o snapshot os congelou | `strategies/backtester.py`, `execution/engine.py`, `trajectories/service.py:99-120` | "Ticks reais no backtest" hoje só existe no Strategy Tester nativo (modelo 4), cujo relatório é lido por `data_eligibility.tester_report` |
| A4 | Não havia rótulo único de fidelidade: `assess`/`evaluate` devolvem capacidades, mas nenhum consumidor recebe a classe A–E de 11.5 | `analytics/data_eligibility.py` | Implementar a classificação (T16.14) e anexá-la a todo estudo |
| A5 | `_symbol_specs` está vazia e o spread médio das barras EURUSD M15 é ≈0,01 ponto | consulta à base | Custos locais quase nulos: qualquer resultado econômico local é **otimista** e precisa do estresse de custo |
| B1 | Colunas `mae`/`mfe` do `_trade_ledger` existem e estão **100% vazias** (9.635 operações, 126 runs) | consulta à base | O livro não é fonte de excursões; usar `_trade_features` (T16.3) ou trajetórias (T19.14) |
| B2 | `enrich` calcula MAE/MFE só em preço, sem instante, e usa máximas **bid** para vendas (sem spread) | `trade_analytics/enrich.py:169-175` | Subestima o MAE de vendas; não serve às duas lentes de 11.2 |
| B3 | T19.14 mede extremos com instante/janela e limitações explícitas, mas em OHLC exige colunas `ask_*` para vendas; as barras gravadas não as têm | `trajectories/paths.py:92-98` | Vendas ficam sem trajetória de preço em barras; só com quotes capturados |
| B4 | Exposição: o motor só devolvia `max_volume` global e contagem de adições. Não havia PnL flutuante mínimo por caso, tempo até o extremo, recuperação após o MAE nem eficiência de captura | `engine.SimResult` | Acrescentar o registro por caso (T16.15) |
| B5 | Margem: não há margem inicial/manutenção na especificação (`SymbolSpec`) | `backtester.py:41-60` | "Margem máxima" fica **indisponível**; reportar volume e perda flutuante, sem inventar margem |
| C1 | Dimensionamento já coberto: lote fixo (base), gradiente contra fixo/ATR (preço médio **cego**), piramidação, martingale/anti-martingale sequenciais com travas | `execution/modes.py`, `sizing.py` | H0 e H5 prontos. Faltavam H1, H2, H3, H4 e H6: nenhum modo condiciona a adição a um **sinal** |
| C2 | Bateria e catálogo reprovam martingale/preço médio para conta real; HADOUKEN/Safety fixados | `robots/risk.py`, `battery.py` | Tudo desta frente é `study_only`; nenhum gate muda |
| C3 | Netting × hedging só aparece em carteira/sessão ao vivo; o motor modela um conjunto de lotes FIFO por operação | `robots/portfolio.py`, `engine.close_part` | Adição na mesma direção é equivalente nas duas contas; ordem oposta (virada de mão) **não** é modelada |
| D1 | Catálogo Python: `ema_atr`, `donchian`, `bb_rsi`, `orb`, `pullback`, `squeeze` (Bollinger×Keltner), `s09`, `stoch_ema9`, `vwap_bands`, `madrid`, `ha_hilo`. **Não há estratégia MACD nem Keltner pura** | `strategies/__init__.py` | O exemplo "entra por MACD, depois Bollinger, depois Keltner" depende de novas definições de setup (F19), não desta frente |
| D2 | Ultra está só especificado (8.5 dos pilares; T19.52–54 planejadas). Não existe seletor condicional nem piso de fator de lucro por política | `PILARES_DADOS_OPERACAO_PRESETS.md` §8.5 | Recorte de estudo T19.52a (11.12) |

### 11.10 Classificação de fidelidade A–E (T16.14)

Uma classe por conjunto de dados efetivamente consumido, derivada do relatório T20.6
(`data_eligibility.assess`) e, quando houver, do relatório nativo do Tester. Nunca sobe de
classe por inferência.

| Classe | Critério verificável | O que certifica |
|---|---|---|
| **A** | Ticks capturados, origem observada comprovada pelos hashes por minuto, auditoria sem defeitos **e** cobertura integral declarada pelo relatório do Tester da mesma execução (100% ticks reais) | Ordem intrabarra, Bid/Ask e spread por evento |
| **B** | Ticks observados com limitação conhecida: cobertura integral não provada, lacunas, timestamps iguais sem ordem do provedor ou cotações inválidas | Extremos e lado do mercado onde há ticks; não a completude |
| **C** | Ticks presentes com origem gerada, mista ou desconhecida (sequência reconstruída/híbrida) | Nada além do que as barras certificam |
| **D** | Só barras OHLC válidas | Decisões no **fechamento** e execução na abertura seguinte; ordem intrabarra desconhecida (stop antes do alvo) |
| **E** | Sem dados válidos ou procedência desconhecida | Nada |

A auditoria de ticks (`tick_fidelity.audit`) mede o que 11.6 exige: contagem, primeiro/último
instante, regressão de `time_msc`, multiplicidade no mesmo milissegundo, cotações inválidas
(`ask < bid`, preço ≤ 0), presença de Bid/Ask/Last/volume real, histograma de flags, spread
(mediana, p99, máximo, anomalias > 10× a mediana), maiores intervalos sem ticks e ticks por dia.
Regra de consumo: política que decide **dentro** da barra (toque, repique antes do stop) exige
A ou B; a recuperação condicional desta passagem decide no fechamento e executa na abertura
seguinte, então roda em D com o rótulo "ordem intrabarra desconhecida", sem certificação de
fidelidade (seção 4.4 do pedido).

### 11.11 Recuperação condicional: especificação implementada (T16.15)

**Estados de um caso** (uma operação do run = um caso; 11.4): `aberta` → `em zona` (perda no
fechamento ≥ `zona_r` × R e < 1 R, stop ainda o inicial, sem parcial) → decisão causal no
fechamento da barra → ordem na **abertura da barra seguinte** → `recuperando` → saída por
equilíbrio/entrada/alvo original, stop global, invalidação ou saídas originais do run. Se a
abertura seguinte já atravessou o stop, vale o stop (gap); nenhuma adição ocorre.

**Sinais de reversão candidatos** (`execution/reversal.py`; só barras fechadas ≤ i; espelhados
para venda): `rsi` (RSI 14 sai da sobrevenda < 30 virando para cima), `bollinger` (fecha de
volta dentro da banda 20/2 após fechar fora), `estrutura` (fecha acima da máxima anterior
depois de dois fechamentos em queda), `falso_rompimento` (fura a mínima de 10 barras e fecha
acima dela) e `exaustao` (pavio de rejeição ≥ 60% de uma barra com amplitude ≥ 0,8 ATR,
fechando no terço favorável). Combinação k-de-n na mesma barra (`min_sinais`). **Continuação
adversa** (H1): fechamento abaixo da mínima das 10 barras anteriores sem nenhum sinal de
reversão. Nenhum sinal é presumido preditivo: o estudo mede.

| Hipótese | Modo (`execution/modes.py`) | Regra |
|---|---|---|
| H0 | `ponto_fixo` | Baseline do run |
| H1 | `saida_antecipada` | Em zona + continuação adversa → sai na abertura seguinte, sem capital extra |
| H2 | `recuperacao_unica` | Em zona + reversão → **uma** adição de 1× o lote; alvo no equilíbrio líquido do conjunto |
| H3 | `recuperacao_proporcional` | Como H2 com `fator_lote` 2; com uma única adição a exposição fica limitada a 3× o lote do caso |
| H4 | `recuperacao_gerida` | Como H2; ao tocar o equilíbrio o stop sobe para ele e o alvo passa a ser a entrada original; fechamento além do extremo da barra do sinal invalida e zera na abertura seguinte |
| H5 | `gradiente_dinamico` (existente) | Preço médio **sem** sinal, comparador convencional; martingale sequencial segue restrito a robô que o declara |
| H6 | `reversao_independente` | Fecha a posição original na abertura seguinte e abre operação nova de `fator_lote` × lote, com o mesmo stop e os mesmos níveis de saída de H2 |

**Matemática e limites.** Equilíbrio no lado que fecha a posição:
`L = Σ(pᵢ·vᵢ)/Σvᵢ + d·(comissões pagas + comissão de saída)/(valor do ponto·Σvᵢ)`. O stop do
conjunto **permanece o inicial**: a adição é feita perto dele, então o risco incremental é
`f·|preço da adição − stop|` por lote e a perda máxima de cenário sem gap é
`1R + f·(1 − z)·R` mais custos (z = fração de R em que a adição ocorreu). Com gap a perda
escala com `1 + f`. Teto de adições (`max_adicoes`, padrão 1) e de exposição (`teto_lotes`).
Compra adiciona no ask, venda no bid, com escorregamento contra; comissão por lote e por lado.

**H6 e o custo afundado.** Manter 1 lote e adicionar f é economicamente igual a fechar e reabrir
`1 + f` lotes no mesmo preço, menos custos. H6 usa só `f` lotes: a diferença H2 − H6 mede o
que o lote **carregado** rendeu no repique; se H6 não é positivo, o sinal não tem mérito
próprio e H2 só parece bom por carregar exposição.

**Virada de mão (H7, não implementada).** Encerrar e abrir no sentido oposto exige direção
variável dentro do caso, stop/alvo próprios e distinção netting/hedging; fica especificada como
experimento separado, depois de H1/H6 mostrarem se a continuação adversa é previsível.

**Registro por caso** (`SimResult.cases`), nas duas lentes de 11.2: (a) volume inicial
constante — `mae_price`/`mfe_price` desde a entrada original, em preço e em R, barras até cada
extremo, recuperação observada depois do MAE e eficiência de captura; (b) carteira —
`floating_min`/`floating_max` em dinheiro com o volume vigente, `peak_volume`, intervenção,
barra, volume adicionado. Na barra de saída só o preço executado entra (ordem intrabarra
desconhecida). Margem: indisponível (B5).

**Estudo pareado** (`runner.recovery_study`, `mt5x trades recovery --run ID`): mesmas entradas,
barras, custos e sinais para todas as variantes; por variante, métricas do Laboratório e, contra
H0 no mesmo caso: diferença líquida e média com IC 90% por bootstrap em blocos de dias, perda
média, quantis p1/p5 e pior caso, queda máxima, exposição máxima, casos com intervenção,
**recuperados × agravados**, custo incremental, diferença na procura × reserva final
(`holdout_fraction`) e sob estresse de escorregamento. **Parecer** `aprofundar` só com n de
intervenções ≥ `min_trades_cell`, limite inferior do IC > 0, reserva e estresse positivos e
cauda (p5) e queda não piores que H0; caso contrário `reprovada` ou `amostra insuficiente`, com
os motivos. Frequência de recuperação nunca aprova sozinha (5.6 do pedido). Todo resultado é
`study_only` e carrega a classe de fidelidade.

### 11.12 Coesão com o Ultra: carteira de setups por ativo e piso de fator de lucro (T19.52a)

O pedido de 10/11 acrescenta três exigências à política Ultra de 8.5 dos
[pilares](PILARES_DADOS_OPERACAO_PRESETS.md): (1) um ativo não fica preso a um preset — cada
oportunidade pode vir de um setup diferente, o que o histórico mostrar mais assertivo naquele
contexto; (2) durante a posição, outro sinal **historicamente validado** pode mudar o alvo ou
antecipar a saída; (3) fator de lucro mínimo de 2.

Como isso entra sem contrariar 8.5: (1) é o seletor de regras por contexto, com prioridade
congelada no treino e alternativa padrão que **bloqueia** a entrada; (2) é gestão intraposição
como capacidade específica — os modos de 11.11 (saída antecipada, recuperação gerida) são os
primeiros candidatos, e só viram regra do Ultra depois do parecer `aprofundar` em reserva;
(3) **PF ≥ 2,0 é piso de elegibilidade, não garantia**: medido na validação e confirmado na
reserva final, com n mínimo; política que não alcança o piso não é emitida ("sem candidato
elegível"), e o piso nunca é atingido removendo retrospectivamente as perdas.

Recorte de estudo implementado (`presets/conditional_policy.py`, `mt5x trades router`): sobre
runs **já gravados no livro** de vários setups do mesmo ativo/timeframe, (a) separa procura e
reserva por tempo; (b) na procura, para cada contexto conhecido na entrada (faixa horária ×
regime de volatilidade *point-in-time*), ordena os setups por fator de lucro com n mínimo e
congela a prioridade; (c) percorre a reserva em ordem de entrada como uma conta de posição
única: aceita a operação do setup prioritário do contexto se não houver posição aberta, senão
bloqueia; (d) compara com o melhor setup estático escolhido na procura e com o oráculo
retrospectivo (rotulado não executável); (e) aplica o piso de PF. O resultado é diagnóstico:
não gera `.set`, não altera HADOUKEN e não substitui T19.52–54 (predicados tipados, pacote,
execução nativa, UI).

### 11.13 Resultados no host e continuidade

**Implementado em 10/11/2026** (T16.14, T16.15, T19.52a): `analytics/tick_fidelity.py`,
`execution/reversal.py`, recuperação e registro por caso em `execution/engine.py`, modos em
`execution/modes.py`, `execution/study.py`, `presets/conditional_policy.py` e os comandos
`mt5x trades fidelity | recovery | router`. Os cinco modos novos aparecem sozinhos na guia
Execução do Laboratório, marcados "só estudo".

**Verificação.** 21 testes novos (`test_trade_recovery_study.py`, `test_tick_fidelity.py`,
`test_conditional_policy.py`); regressão de motor, livro, trajetórias, elegibilidade, backtester,
presets e UI do Laboratório (Linux/Xvfb): 277 aprovados. Percurso pelo launcher oficial
(`./run.sh --cli`, classe local, sem ponte) em **base de QA isolada**: barras reais EURUSD M15
(6.443, 22/07–23/10/2026), contrato declarado para a QA, escorregamento de 5 pontos e comissão
de 3,5 por lote e lado. [Recibo](../evidencias/T1615_RECUPERACAO_ROTEADOR_EURUSD_M15_QA_20261110.json).
Nível exercitado: re-simulação em barras, **classe D**. Não é Tester/MT5, DEMO, conta real nem
Windows.

**Achados no host (EURUSD M15, três meses; evidência fraca por amostra):**

- **Fidelidade:** classe D; decisão intrabarra não certificável. Nenhuma conclusão abaixo vale
  para repique dentro da barra.
- **Excursões (base):** nas quatro estratégias medidas, metade das vencedoras suportou de 0,34 a
  0,42 R contra e 90% até 0,77–0,82 R. Quem fecha uma barra além de 0,6 R contra já está, na
  maioria, entre as perdedoras: a zona de recuperação é território de perda, não de ruído.
  Perdedoras chegaram a mostrar ganho relevante antes de perder (p90 do MFE entre 0,78 e
  1,67 R), o que aponta para gestão de **saída** (break-even/parcial) como frente mais
  promissora que a adição.
- **Preço médio sem sinal (H5):** reprovado nos quatro runs; piora líquido (−6 mil a −21 mil),
  cauda, queda e exposição (até 2× o lote). É o risco do martingale/preço médio do vídeo,
  reproduzido com os dados locais.
- **Adição condicionada (H2/H3/H4):** só 3 a 9 intervenções por run → **amostra insuficiente**
  em todos. Onde há algum sinal ele é desfavorável: em `ema_atr` 0 de 7 casos melhoraram e o
  intervalo de 90% da diferença fica inteiro abaixo de zero; em `bb_rsi` H2 e H4 idem. H3
  aumenta o pior caso em 29–85%.
- **Reentrada independente (H6):** negativa em três de quatro runs; os sinais de reversão
  testados não mostraram mérito próprio nesta amostra.
- **Saída antecipada (H1):** em `ema_atr` 4 de 5 casos melhoraram (+709, IC 90% acima de zero),
  mas são 5 casos; em `bb_rsi` 37 casos, 27 melhoraram em frequência e o líquido **piorou**
  (−2.246) — exemplo direto de que frequência de recuperação não aprova.
- **Política condicional (T19.52a):** sem candidato elegível. Com custos, só `ema_atr` fica
  perto do equilíbrio (PF 1,00 na procura; 1,28 na reserva); nenhum setup alcança PF > 1 com
  30 operações em algum contexto, então a política bloqueia tudo. O oráculo retrospectivo
  mostra PF 2,5 na reserva: é a ilusão que o rótulo "não executável" evita.
  **Nenhuma combinação local sustenta o piso de PF 2.**

**Lacunas abertas, em ordem:** (1) extrair ticks (`mt5x ticks`) e medir custos
(`mt5x specs --calibrate`) dos ativos de interesse, para sair da classe D; (2) mais história e
mais ativos — três meses de um par não sustentam parecer; (3) o livro local guarda cada run
Python em triplicata (mesmo `params_hash`), e as lentes por robô/ativo somam as três: n inflado
e intervalos estreitos demais (corrigir em ficha própria); (4) `enrich` subestima o MAE de
vendas (B2) e as colunas `mae`/`mfe` do livro seguem vazias (B1); (5) H7 (virada de mão) e
margem (B5) não modeladas; (6) estratégias MACD e Keltner não existem no catálogo (D1);
(7) Ultra completo segue em T19.52–54; (8) confronto com o Tester nativo em ticks reais e
auditoria do vídeo (transcrição e números) continuam pendentes.

**Próximo passo recomendado:** com ticks e mais história, repetir `mt5x trades recovery` por
run e estudar H1 e os modos de saída (break-even, parcial) antes de qualquer adição; só uma
variante com parecer `aprofundar` em reserva vira candidata a regra intraposição do Ultra.

## Auditoria de ticks versão 2 — 12/11/2026

Owner Dados/Laboratório, fila DIAG-TICKS, continuação da PR304.
Foram reproduzidas 15 falhas em flags não finitas, fracionárias ou fora do
intervalo, sequência inválida/duplicada e auditoria vazia com promoção de classe.
A versão 2 registra metadados inválidos e mantém a captura original. A classe A
exige auditoria atual com linhas, e a certificação intrabarra exige sequência
sem defeitos bloqueantes. Point inválido ou conversão que exceda o número finito
é diagnosticado, conservando o spread em unidades de preço. Arrays inteiros
nullable têm seus valores ausentes contabilizados. O corretivo de timestamps
inválidos e seus seis testes já presentes no checkout foram revisados e preservados
neste incremento.

Regressão Linux em lote único com elegibilidade e recuperação teve 80 aprovados.
Após cobrir booleanos e inteiros nullable, o recorte final teve 34 aprovados em
3,47 s. Contagens sobrepostas. Doctor offline teve 16 OK e dois avisos.
O percurso CLI dedicado e a GUI aguardam uma janela sem a VM de outra tarefa,
que passou a ocupar cerca de 2 GiB com pressão de I/O. A suíte Windows da ponta
anterior falha no lote de cancelamento SQLite, item DIAG-CI304. O CI da nova
ponta terá estado próprio. Windows físico, Tester e DEMO conservam seus aceites.

## Excursões nas lentes e no cache — 12/11/2026

Owner Dados/Laboratório, fila DIAG-EXCURSOES, branch `codex/lab-excursoes`.
As lentes agora preenchem MAE/MFE ausentes com o enriquecimento e preservam
valores registrados no livro. Vendas usam o ask modelado pela regra de spread
do backtester, incluindo o piso calibrado. Falta ou invalidade do point mantém
as excursões indisponíveis. O campo `excursion_basis` identifica livro,
combinação de fontes, envelope bid/ask ou ask indisponível. A versão de cálculo 2
invalida caches anteriores e acrescenta a coluna pelo mecanismo aditivo existente.
As amplitudes continuam em unidades de preço sobre envelopes de barras. A ordem
dos eventos intrabarra e o instante dos extremos da barra de saída permanecem desconhecidos.

Reprodução inicial com quatro falhas. No Linux, lotes separados tiveram 38 testes
aprovados e 21 de cache/livro/importação/CLI aprovados, incluindo 50 mil operações.
Após ajustar tipos e cobrir cache legado, o lote final teve 13 aprovados e um teste
PostgreSQL pendente por ausência de `MT5X_TEST_PG_URL`. As contagens se sobrepõem.
O lote de cache levou 56,46 s e atingiu 400 MiB de RSS. A medição durante o lote
registrou RAM disponível acima de 7,9 GiB, swap inativo e ACPI até 59 °C nas amostras.
Doctor offline aprovado com 16 OK e dois avisos. Launcher oficial CLI com SQLite
sintético confirmou MAE 4, MFE 2 e `ask_bar_envelope`, conservando o livro.
CI do novo SHA, PostgreSQL real, Windows físico e percurso GUI desta correção
permanecem pendentes. Evidência local em `state/qa-tmp/diagnostico-20261112`.

## Vídeos complementares

Análise de 13/11/2026 solicitada pelo proprietário. Sete anexos correspondem
a seis vídeos. Os dois relatórios Linda são idênticos por SHA-256
`bd54c2917e904d44fe1e01b151619fc4277a9c9899aea1e349587fcd309a7d60`.
As descrições e reconstruções de código são evidência fornecida pelo agente
que examinou os vídeos. Fonte original completo, capturas binárias, ticks e
livros das demonstrações continuam ausentes. Esta passagem conferiu código do
projeto e documentação oficial, com ensaios pequenos em memória.

### Correções prioritárias adicionais

Os dois itens abaixo entram em P0, antes dos novos Experts que dependam
desses cálculos. Estado, posse, branch/PR e SHA ficam na fila única do handoff.
A implementação e a publicação corretivas permanecem pendentes.

#### IND-CCI-01 · Desvio absoluto médio e aquecimento do CCI

- **Owner de componente** Dados e indicadores App, com integração aos consumidores.
- **Fonte** `mt5_extracao/advanced_indicators.py`, método `AdvancedIndicators.cci`.
  A média móvel de `abs(tp - sma_tp)` reúne desvios calculados contra médias
  diferentes. O desvio da referência usa a mesma média da janela em avaliação.
- **Reprodução executada** no `.venv` existente, Python com `-B`, um processo e
  `OPENBLAS_NUM_THREADS=1`, `OMP_NUM_THREADS=1`, `MKL_NUM_THREADS=1`. OHLC
  sintético com27 barras, close1…27, high=close+0,5, low=close−0,5 e período14.
  O método atual retornou66,6666666667 no último ponto. O desvio absoluto
  médio da janela final é3,5, com referência123,8095238095. O primeiro valor
  atual surge no índice26, enquanto a janela de14 barras fica completa no13.
  O caso muda a comparação do indicador com o nível100.
- **Alcance conferido** `EnhancedIndicatorCalculator.calculate_all_indicators`
  chama esse método; `DataCollector` usa o calculador e salva o resultado.
  `app.py` instancia e inicia esse coletor. O módulo da prévia
  `development-2b3b274da6de` é idêntico ao fonte por hash. A coleta e a base
  pessoal ficaram fora do ensaio. A quantidade de dados afetados exige inventário.
- **Próxima ação** regressão de fórmula/aquecimento, correção no calculador
  canônico e reconciliação de consumidores, versões, caches e materializações.
  Preservar resultados históricos/snapshots e planejar recomputação aditiva
  identificada pelo produtor existente, com posse do pilar de Dados.
- **Aceite** referência independente, série constante/ausente/insuficiente,
  subida/queda, janela parcial, limiares e atualização incremental. Validar
  saída do calculador integrado e percurso de coleta com QA própria. A
  paridade com iCCI/Profit demanda valores e execução nativa próprios.

Reprodução mínima do método, com dados sintéticos, a partir da raiz do App

```bash
rtk env OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 .venv/bin/python -B - <<'PY'
import pandas as pd
from mt5_extracao.advanced_indicators import AdvancedIndicators
close = pd.Series(range(1, 28), dtype=float)
actual = AdvancedIndicators().cci(close + 0.5, close - 0.5, close, period=14)
window = close.iloc[-14:]
center = window.mean()
reference = (close.iloc[-1] - center) / (0.015 * (window - center).abs().mean())
print(actual.iloc[-1], reference, actual.first_valid_index())
PY
```

#### IND-AGGR-01 · Índices incrementais do AggrVolBalance

- **Owner de componente** indicadores HADOUKEN, com composição pelo integrador.
- **Fonte** `Indicators/HADOUKEN/Volume/HDK_AggrVolBalance.mq5` configura
  buffers e arrays como `AS_SERIES=true`. A atualização usa
  `start_bar=prev_calculated-1` e laço crescente até `rates_total`.
- **Reprodução** inspeção estática e ensaio aritmético dos índices. Com
  rates_total100/prev_calculated100, visita apenas99. Com101/100, visita99 e100.
  A barra corrente é0. A execução MQL nativa e o alcance no artefato instalado
  permanecem por qualificar. A infraestrutura de sinais contém consumidores
  desse buffer, cujo vínculo no perfil efetivo exige conferência.
- **Próxima ação** reproduzir no indicador, corrigir a seleção incremental e
  comparar com reconstrução completa sobre os mesmos dados, preservando o
  contrato e reconciliando os pins por incremento revisado.
- **Aceite** primeiro cálculo, tick na mesma barra, nova barra, histórico
  expandido/revisado, dados inválidos e leitura por consumidor. Resultados
  incrementais devem corresponder à reconstrução no mesmo corte.

O indicador existente calcula uma aproximação por candle/volume. O novo
fluxo com direção observada por flags exige contrato e qualidade próprios.
Os rótulos e o registry devem preservar essa distinção de origem e fórmula.

### Avaliação dos seis vídeos e encaixe no projeto

| Referência | Incremento aproveitável | Reuso e lacunas | Aceite proposto |
|---|---|---|---|
| CCI/SMA9/21/ATR10, `_glnf5eXLuM` | Uma entrada por episódio direcional, filtro de distância e SL/TP por volatilidade, com sinais visuais | SMA/EMA/ATR, scaffold, risco e painel existem. Gerador declarado aceita SMA/EMA/RSI; CCI/filtro de distância e estado de rearme precisam de extensão tipada após IND-CCI-01. ATR do relato usa média aritmética | Sequências com sinal persistente, retirada do sinal, reinício, recusa/incerteza e stop. Definir snapshot ATR versus atualização e medir custos. |
| Pressão de agressão, `g7xWx5n0WhQ` | Indicador de pressão e variação em janela curta, cobertura de ticks e cooldown | Captura/auditoria de ticks e indicadores de aproximação já existem. Pressão observada por flags, sua série temporal e consumidor exigem produtor qualificado | Janela completa W+intervalo comparado, unidade de volume fixa, limites de leitura diagnosticados, ticks repetidos/fora de ordem e sinais simultâneos. Paridade incremental/lote antes de avaliar mérito econômico. |
| Pullback atribuído a Linda Raschke, `Oj_YwoevrnE` | Regime→rompimento→pullback→ordem, validade e invalidação visíveis | Donchian/EMA/ADX e políticas temporais existem; sua presença não comprova o setup. ADX com dois períodos exige equivalência definida | Ordem temporal estrita, expiração, referência de candle/plano versionada, cancelamento observado, corrida cancelamento/fill e recuperação. |
| GradienteFull3.9, `roUb-meZZjI` | Exportação diária com extrema/horários/custos, atalhos com prévia, distância ATR congelada e filtros de regime | Ledger/exportação, pendentes Safety e gradientes de pesquisa existem. Simulação em QA segue os gates de risco vigentes | Exportação idempotente, bloqueio de arquivo diagnosticado, revalidação de atalhos, exposição executada+reservada e estudo pareado com custos. |
| Renko embarcado, `Sg2zQZdw6vI` | Série derivada causal de ticks, seed/gap explícitos, timestamp e sequência de boxes, visual opcional | Composition atualmente exige timeframe MT5 e declara Renko como contrato próprio. Usar o pilar de Dados e motor financeiro existentes | Convenção de tamanho/reversão/pavios/fonte, múltiplos boxes por tick, lacunas, corte as-of, restart e paridade de série/eventos, com negociação em cotações executáveis. |
| Vela de volume, `rqkVwLhvLFc` | Zonas de volume financeiro com proveniência e validade, candidato a estudo de breakout/reteste | Volume real/tick já tem unidades e ausência explícitas no registry. Volume financeiro equivalente ao Profit precisa de contrato. A entrega relatada é indicador | Recorde conhecido em cada instante, revisão de zonas, sessões, dia atual/empates/ausência, índices válidos e comparação fora da amostra com referências simples. |

### Ajustes críticos na interpretação dos relatórios

1. **Extremos diários e suas horas são insuficientes para otimizar travas.**
   Ensaio em memória com curvas A=`[0,100,-100,200,-200,50]` e
   B=`[0,-100,100,200,-200,50]` tem máximo200 no índice3, mínimo−200 no4
   e fechamento50 em ambas. Com gain/loss80, A toca gain primeiro e B toca
   loss primeiro. A curva/eventos com cobertura qualificada e o encerramento
   contrafactual precisam ser reavaliados no motor existente. O serviço de
   trajetórias atual já identifica os limites de cruzamento absoluto.
2. **Offset e desvio de execução têm contratos distintos.** Stop-limit pode
   continuar sem execução após o disparo. A propriedade deviation do MT5
   depende do modo de execução. Qualificar parcial, recusa e cancelamento.
   DAY/GTC tratam validade de ordem; zeragem é um processo observado.
3. **Variação de pressão é uma diferença finita.** Definir comparação fixa,
   extremo na janela ou taxa por segundo. Expiração de vendas antigas pode
   elevar compra60%→100% com zero compras novas. A sequência50→70→60 e
   comparação contra mínimos da janela produz impulso10p.p. nos dois lados.
   A prioridade do sinal e os filtros de quantidade/liquidez exigem contrato.
4. **Estado de estratégia e efeito no broker exigem vínculos.** O reset da
   flag Linda no código reconstruído não apresenta cancelamento explícito
   da ordem. O trecho combina Close corrente com referências[1], e pode
   registrar rompimento e aproveitar pullback anterior na mesma avaliação.
   LastBarOnChart identifica a última barra; a política de fechamento é própria.
   Definir referência fixa ou móvel e custo/risco reais após os fills.
5. **Portabilidade de volume e Renko exige equivalência.** Volume financeiro
   do Profit, volume de contratos e contagem de ticks têm unidades distintas.
   Sufixo.TR e seleção de ticks reais no Tester exigem prova de origem/cobertura.
   Convenção20R=19ticks é específica do relato. Vários boxes gerados por um
   tick compartilham o evento e o preço executável. A hipótese de atuação
   institucional e a alegação de aderência100% permanecem sem prova abrangente.
6. **O código reconstruído das zonas tem inconsistência de limites.** Arrays
   Max/Min/MaiorVolume/Hora são declarados1…50 e o loop percorre0…50. Omitir
   a plotagem0 deixa esse acesso a investigar no original. Maior candle do dia
   só pode orientar decisões depois de conhecido. Preservar zonas históricas
   e versões evita inserir o vencedor retrospectivo no começo da sessão.
7. **Preço médio, martingale e compressão de alvo exigem avaliação conjunta
   de cauda, capital, risco e custos.** Acerto100% em fechamentos apresentados
   deixa em aberto exposição e perdas flutuantes. O perfil do vídeo com SL0
   conflita com os gates atuais. Os modos e H0–H6 existentes dão o caminho
   de pesquisa. Seu ganho incremental depende de teste pareado elegível.

### Painel, operação e método

- **Painel por capacidades** reutilizar PanelHost/GuardianPanel, explicando
  variante/preset, fonte/unidade/cobertura, barra ou box confirmado, estado
  de rearme/etapa/expiração e pedido/fill/cancelamento observados. DPI,
  contraste, teclado e sobreposição de rótulos têm aceite físico próprio.
- **Atalhos e controle manual** aproveitar a boleta/PlacePending/ModifyPending
  e validações existentes. Prévia, foco/armamento, repetição de tecla, idade
  do cursor/cotação e risco precisam de revalidação. Pausar entradas, cancelar
  pendentes e zerar possuem ações e acompanhamento identificados.
- **Proteção financeira** definir escopo/custos/âncora e fórmula de retenção
  de lucro, incluindo a diferença entre percentual protegido e devolução
  permitida. A zeragem permanece sujeita ao mercado e à reconciliação.
- **Exportação** derivar do ledger/journal e manter schema, run, conta/servidor
  saneados, Magic integral, símbolo/contrato, revisão, moeda e estado provisório
  ou final. Um arquivo comum resultado_diario.csv exige isolamento por execução
  e tratamento de concorrência. Conferir lock, interrupção e retomada com QA própria.
- **Desempenho** leituras incrementais, buffers limitados com truncamento
  diagnosticado, atualização visual desacoplada e objetos próprios. CopyTicks
  pode aguardar sincronização no EA; callbacks exigem medição e limites.
  As três bandas/produtores/visuais compartilham contratos, assim como as
  novas séries; estudo e execução consomem o cálculo canônico qualificado.
- **Experimentos** corrigir cálculos primeiro, qualificar dados/custos,
  comparar componentes sob condições iguais, congelar regras, registrar
  tentativas e reservar período fora da amostra. Replay, compilação e alguns
  trades demonstrados têm alcance próprio. IA reutiliza schema/compiler/
  scaffold e contratos existentes, com ferramentas nas permissões vigentes.
- **Sequência sugerida** P0 atuais e novos, Triplo já priorizado, capacidades
  de rearme/estados/inspeção, candidatos CCI e pullback, zonas de volume,
  fluxo e Renko após suas dependências de dados. Configurador/dashboard
  mantêm prioridade Alta após operações funcionais, com arquitetura futura
  avaliada no marco já definido pelo proprietário.

Esta passagem fez análise, registro local e ensaios em memória, incluindo
execução do método CCI atual. Antes desse ensaio, MemAvailable7285MiB e PSI
de memória0 foram observados. As amostras têm27 barras e curvas sintéticas,
com um processo e OPENBLAS/OMP/MKL configurados para1. Correções do
produto, regressões permanentes, CI, Tester, conta e observação prolongada dos
incrementos continuam pendentes. A análise não qualifica rentabilidade.

Fontes dos vídeos

- [CCI e volatilidade](https://www.youtube.com/watch?v=_glnf5eXLuM).
- [Pressão de agressão](https://www.youtube.com/watch?v=g7xWx5n0WhQ).
- [Setup atribuído a Linda Raschke](https://www.youtube.com/watch?v=Oj_YwoevrnE).
- [Gradiente Linear](https://www.youtube.com/watch?v=roUb-meZZjI).
- [Renko integrado](https://www.youtube.com/watch?v=Sg2zQZdw6vI).
- [Zonas de volume financeiro](https://www.youtube.com/watch?v=rqkVwLhvLFc).

Referências primárias para contratos e cálculos

- [CCI e fórmula](https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/cci).
- [Manual NTSL](https://downloadserver-cdn.nelogica.com.br/content/profit/manual_ntsl/ManualNTSL.pdf).
- [CopyTicks](https://www.mql5.com/en/docs/series/copyticks).
- [Dados e flags MqlTick](https://www.mql5.com/en/docs/constants/structures/mqltick).
- [Indexação AS_SERIES](https://www.mql5.com/en/docs/array/arraysetasseries).
- [Solicitação e desvio de preço](https://www.mql5.com/en/docs/constants/structures/mqltraderequest).
- [Execução e Stop Offset](https://ajuda.nelogica.com.br/hc/pt-br/articles/360053623191-Compreendendo-o-Pulo-de-Ordens).
- [Corrida de execução/cancelamento OCO](https://ajuda.nelogica.com.br/hc/pt-br/articles/360041299231-Ordens-OCO-Editor-de-Estrat%C3%A9gias-de-Negocia%C3%A7%C3%A3o).
- [Ticks reais e gerados](https://www.metatrader5.com/en/terminal/help/algotrading/tick_generation).

## Triagem de canais e auditoria do relatório

Registro de análise em 13/11/2026, a partir do relatório entregue pelo
proprietário sobre 28 vídeos distintos de dez canais. SHA256 do anexo
`cd486221cb5bd09ddcaac1fb28fc674a2bf0560a67afaa61e12bf2e3d924802d`.
O relatório declara acesso a transcrições e metadados e ausência de acesso
ao repositório. Seus exemplos são material de pesquisa com autoria técnica
a esclarecer. Faltam evidências visuais, intervalos de cobertura, timestamps
por afirmação, código original e separação entre observação e reconstrução.

Uma consulta leve ao oEmbed oficial do YouTube confirmou título e canal de
VYRyK6SoaaM, NLBXgSmRBgU, Ls1-_SpEfmo e LNNQ1t2bsGk. Datas, durações,
fórmulas e fidelidade das descrições seguem dependentes da inspeção do conteúdo.
A conferência local usou App `550b6d5`, HADOUKEN `d4d389e` e Safety `27b8599`.
As alterações locais preexistentes foram preservadas. Esta passagem acrescenta
análise documental e exemplos aritméticos pequenos ao registro existente.

### Prioridade e reconciliação com a fila

A ordem corretiva vigente continua E14-CALC-01 → IND-CCI-01 → IND-AGGR-01 →
E14-METRIC-01 → E14-SCOPE-01 → E14-EXEC-01. Depois vêm SA-01-CONTRATO e
SA-01-PLOT. Os achados de cálculo já reproduzidos precisam de regressão,
correção, identificação dos resultados afetados e qualificação dos consumidores
antes das comparações financeiras dependentes. A fila de continuidade mantém
seus 28 itens e seus estados. Este estudo fornece propostas e critérios para
os responsáveis existentes. [Fila única](../../CONTINUAR_AQUI.md).

| ID citado no relatório | Responsabilidade canônica | Destino da proposta externa |
|---|---|---|
| E14-CALC-01 | Comissões, fills, parciais e contagem de ciclos | Permutação/Martin Ratio entram como proposta metodológica de F13/F19, após dados e métricas elegíveis |
| E14-SCOPE-01 | Escopos de conta/Magic/símbolo em métricas, travas e zeragem | ONNX fica como evolução futura com contrato e owner próprios |
| E14-EXEC-01 | Intenção durável, parcial, resultado incerto, reinício e reversão nos consumidores vigentes | A proposta de parcial deve qualificar o fluxo existente e seus perfis de conta |

E14-CALC-01b, E14-RISK-01 e E14-PANEL-01 são rótulos sugeridos pelo relatório.
Sua eventual adoção exige reconciliação com a fila e com os componentes já
presentes. O configurador guiado e o dashboard consolidado mantêm prioridade
Alta após as correções aplicáveis e as operações funcionais do Expert.
A avaliação futura da arquitetura da interface permanece no marco registrado.

### Correções necessárias no material recebido

1. **Parcial em netting e confirmação.** O relatório recomenda
   `CTrade::PositionClosePartial` para netting. A documentação oficial define
   ambas as sobrecargas para hedging. O retorno booleano confirma verificações
   básicas e exige consulta do resultado. O exemplo ainda move SL mesmo quando
   o volume parcial é inviável ou a solicitação falha, e mantém conclusão em
   uma flag volátil. A fonte atual oferece NativePartialPolicy → ReduceOwn →
   SafeExecutor, com intenção, persistência, comparação de volume/histórico
   e BE condicionado à confirmação persistida. O perfil atual bloqueia
   netting/exchange em NativeBaseAdapter, SafeExecutor e suas operações de
   gestão enquanto a posse por contribuição estiver sem qualificação.
   Compatibilidade B3 netting depende de contrato de contribuição, escopo e
   recuperação em E14-SCOPE-01/E14-EXEC-01. Preservar esse guarda durante a
   investigação. Uma ordem oposta pendente também exige reserva, reconciliação
   e tratamento de corrida para impedir redução excessiva ou inversão.
   [Contrato de PositionClosePartial](https://www.mql5.com/en/docs/standardlibrary/tradeclasses/ctrade/ctradepositionclosepartial).
2. **Lote mínimo pode exceder o orçamento.** O helper externo devolve lote
   mínimo quando faltam especificações e eleva volumes pequenos ao mínimo.
   Com orçamento1, risco100 por lote, mínimo0,1 e passo0,01, o volume calculado
   0,01 vira0,1 e o risco sobe para10. Esse é um contraexemplo do snippet
   externo. HDK_RiskGuard já valida especificações, usa OrderCalcProfit na
   moeda da conta, rejeita lote abaixo do mínimo e recalcula risco final.
   Reutilizar esse caminho, acrescentando os cenários pertinentes à sua
   qualificação. Casas decimais fixas, moedaBRL presumida e arredondamento
   posterior ao limite máximo exigem correção no exemplo.
3. **Preço, envio e resultado possuem contratos distintos.** Fallback para
   dígitos com tick desconhecido deixa sem prova a validade do preço.
   A normalização precisa considerar lado, tipo da operação, distância e risco
   depois do ajuste. Tipo de preenchimento depende da especificação do símbolo
   e do modo de execução. OrderCheck e um código de retorno isolado deixam
   pendente a reconciliação de fills, volume e resultado final. OnTradeTransaction
   pode entregar várias transações por pedido, com ordem de chegada variável.
   O handler precisa ser curto e apoiar a consulta/recuperação persistida.
   [Transações de negociação](https://www.mql5.com/en/docs/event_handlers/ontradetransaction).
4. **APTR exige calendário de sessões.** Multiplicar índices por24h/TF
   pressupõe barras contínuas durante todo o dia. Barras ausentes, sessões,
   feriados, DST e contratos impedem assumir o mesmo minuto do pregão.
   Definir timezone, sessão, referência de fechamento, tratamento de gaps,
   amostra mínima e cobertura. Zero e média parcial silenciosa tornam o
   resultado ambíguo. APTR é uma hipótese adicional de volatilidade, com
   versão própria e comparação pareada. O relato de diferença de volatilidade
   em ordens de grandeza e os horários fixos carecem de evidência específica
   do ativo e período. O calendário e os produtores existentes são os pontos
   de integração. A referência sazonal de volume já registrada em
   indicator_registry.py ajuda a reutilizar relógios e disponibilidade.
5. **Permutação precisa de hipótese nula e p-valor válido.** O helper externo
   retorna p=0 com zero excedências e100 sorteios. Uma estimativa conservadora
   usual para o teste aleatório inclui a observação e usa(b+1)/(N+1), dando
   1/101 nesse exemplo. Empates, repetições e desenho do sorteio exigem
   definição. Scores ausentes, NaN ou infinitos precisam de estado inválido
   explícito. Com score real NaN e scores permutados finitos, as comparações
   do helper são falsas e também produzem p=0. A permutação externa preserva
   colunas derivadas antigas
   em df.copy(), enquanto modifica OHLC, e sorteia gaps e intrabarras
   separadamente. Recalcular features/sinais e justificar dependências,
   sessões, volume, spread e custos sob a hipótese nula. Reexecutar seleção e
   otimização em cada amostra quando a pergunta envolver viés da seleção.
   Um parâmetro congelado responde a uma pergunta diferente. O Monte Carlo
   atual de strategies/validation.py já reamostra retornos com reposição.
   Registrar separadamente a permutação proposta, com seed, universo de
   tentativas, incerteza e período reservado. Os limites0,01/0,05 e N100
   permanecem propostas a justificar metodologicamente. [Phipson e Smyth](https://gksmyth.github.io/pubs/PermPValuesPreprint.pdf).
6. **ONNX depende do schema real do modelo.** O snippet supõe três saídas e
   usa vector double com conversão desabilitada. É preciso conferir quantidade,
   nomes, dtypes, shapes, ordem de features, normalização treinada, operadores
   suportados e valores finitos. Um modelo float requer tensores compatíveis.
   Registrar versão/hash, paridade de decisão Python/MQL5, falhas e orçamento
   de tempo/memória antes de integrar ao filtro de entrada. Latência<1ms
   precisa de medição no modelo e hardware escolhidos. O operador matricial@
   consta na documentação atual, com tipos e dimensões compatíveis. Sua adoção
   exige medir o benefício sobre o cálculo concreto. [OnnxRun](https://www.mql5.com/en/docs/onnx/onnxrun), [produto matricial](https://www.mql5.com/en/docs/basis/operators/matmul).
7. **Timer, renderização e handles já têm integração.** PanelHost já evita
   criar o painel na otimização, e StrategyRobot possui OnTimer. A fila de
   eventos de cada programa serializa os callbacks. Migrar trabalho pesado
   para OnTimer continua consumindo essa fila e pode descartar novos eventos
   de timer. Medir duração, limitar trabalho e consumir snapshots existentes.
   Handles precisam de ownership e ciclo de vida explícitos. IndicatorRelease
   tem uma exceção documentada no Strategy Tester, onde sua execução é
   suprimida. O ganho fixo2,5x proposto precisa de benchmark pareado.
   [OnTimer](https://www.mql5.com/en/docs/event_handlers/ontimer), [IndicatorRelease](https://www.mql5.com/en/docs/series/indicatorrelease).
8. **Regimes e portabilidade precisam de evidência causal.** As regras
   ATR/EMA e slope descritas ainda exigem fonte para a classificação como
   modelo de Markov. Limiar1,8/0,6, sessões e faixa de pullback são hipóteses
   de estratégia. Já existe market_regimes.py com estados confirmados,
   duração, aquecimento UNCERTAIN e disponibilidade por barra fechada.
   Aproveitar esse catálogo para comparar variantes. MQL5 ter MTF/book/ticks
   oferece APIs, e cada uso ainda precisa provar cobertura, relógio e
   disponibilidade. Sinal+1/0/−1 exige estado inválido separado, timestamp,
   revisão e distinção entre estado persistente e novo evento. Extremos
   confirmados exigem atraso explícito e available_at.

### Aproveitamento por área e aceites propostos

As linhas abaixo são propostas de integração. Os responsáveis devem ligá-las
às tarefas oficiais quando houver escopo executável, conservando ID, estado,
dependências e evidência na fila única.

| Área / responsável | Integração e benefício concreto | Dependências e aceite |
|---|---|---|
| Painel / Expert e F19 | Estender PanelHost/GuardianPanel com variante, revisão, qualidade/frescor do dado, motivo de bloqueio, sinal, solicitação e fill observado. Tornar o plot opcional uma apresentação do mesmo cálculo consumido pelo Expert | E14-METRIC/SCOPE/EXEC e SA-01-PLOT. Comparar decisões/journal com visual habilitado e desabilitado. Conferir leitura, foco, teclado, DPI e acessibilidade em interface real |
| Configurador guiado / presets e interface | Explicar unidade, intervalo válido, dependência entre campos e impacto no risco. Exibir comparação da revisão anterior, resumo e validação antes de aplicar | Operações funcionais e contratos de preset. Valores antigos continuam legíveis. Aplicação tem intenção estável, estado observado e recuperação. Arquitetura futura da interface é decisão do marco existente |
| Qualidade de operação / Expert e Safety | Reutilizar pausa, cancelamento e encerramento com escopo visível, pedido acompanhado e motivo de estado incerto. Mostrar contribuição compartilhada ou perfil de conta sem qualificação | E14-SCOPE/EXEC. Ensaios próprios de timeout, fill parcial, reinício e mudança de conta. Nenhum efeito fora do mandato e nenhuma repetição cega após resultado desconhecido |
| Expert e indicadores / HADOUKEN e Dados | Fechar primeiro Stop ATR Triplo, seed, fórmulas, entrada/saída, rearme e compatibilidade de proteção. Qualificar handles, CopyBuffer completo e disponibilidade MTF. Reutilizar HDK_StrategyRobot, NativeBaseAdapter e produtor SuperATR do header fixado | P0, SA-01-CONTRATO/PLOT e composição revisada. Regressores de limites, paridade temporal e ensaio nativo próprio com perfil autorizado. Revisões dos pins exigem reconciliação correspondente |
| Metodologia e otimização / F13 e F19 | Acrescentar permutação como experimento registrado e Martin/Ulcer como métricas complementares. Preservar walk-forward, custos, janelas causais, universo histórico e histórico de tentativas | Correções contábeis/indicadores e dados elegíveis. Definir hipótese nula e estatística, tratar denominador zero, reotimização, múltiplas tentativas e incerteza. Usar amostras pequenas para validar o método antes de orçamento maior |
| APTR e regimes / Dados e indicadores | Registrar nova feature/produtor com referência por sessão anterior, amostra/cobertura e parâmetros versionados. Comparar contra o cálculo de referência nas mesmas condições | Calendário, timezone, preço anterior, fonte e revisão. Provar ausência de futuro, reação a faltantes e igualdade temporal entre consumidores. Benefício financeiro depende de avaliação fora da amostra |
| Dashboard e inspetor / F19 e Analytics | Reunir líquido, drawdown, exposição, MAE/MFE e distribuições com filtros compartilhados por ativo, período e revisão. Permitir inspecionar sinal, confirmações, entrada, parcial, stop e saída com seus motivos | P0 e operação funcional. Derivar do ledger, trajetórias e journal oficiais. Distinguir provisório/final e frescor. Extremos diários isolados deixam sem prova a ordem intradiária de gatilhos. Conferir seleção e navegação na interface real |
| ONNX e matrizes / pesquisa e integração | Investigar inferência nativa como extensão futura do fluxo oficial, com modelo reproduzível, explicação do filtro e orçamento medido | Dados/execução qualificados, features causais e pipeline versionado. Paridade de transformação e decisão, testes de corrupção/schema/falha, medição no alvo e integração do resultado indisponível |

### Próxima coleta de evidências

Priorizar a inspeção completa dos vídeos de APTR, permutação, parcial/eventos
e regimes. Para cada regra, trazer URL com início/fim, transcrição curta,
captura relevante de fórmula/parâmetros/painel e vínculo ao código original
com versão/licença. Rotular observação visual, declaração do autor, inferência
e proposta do agente. Explicitar trechos sem cobertura e divergências entre
tela, fala e exemplo. Para ONNX, acrescentar modelo, schema, dtype e requisitos
de runtime. Esse material permite fechar contratos e aceites com procedência.

A revisão local confirmou ausência dos seis basenames principais propostos
no conjunto de arquivos rastreados do App. Os caminhos oficiais inspecionados
incluem robots/super_atr_research.py, analytics/super_atr.py,
strategies/validation.py, analytics/market_regimes.py, indicator_registry.py
e os módulos HADOUKEN acima. HDK_SuperATR.mq5 declara dez buffers/plots.
A conferência do projeto deve prevalecer sobre as contagens hipotéticas do relatório.

Os únicos ensaios novos foram aritmética em memória do snippet de lote e do
p-valor, com biblioteca padrão no ambiente existente. A qualificação desta
proposta depende das regressões, CI, interface real, Tester, conta, sessões
e observação prolongada aplicáveis a cada incremento. Os cinco pregões e
os horários fixos do checklist externo seguem como sugestões a dimensionar
pelo cenário e pelo orçamento do host. Fonte inspecionada e documentação
oficial sustentam os contratos apontados. Mérito financeiro de APTR, regimes,
parciais ou ONNX permanece uma hipótese a avaliar.

## Encaminhamento para atuação e cruzamentos

Pedido do proprietário em 13/11/2026 para preservar todos os achados e
incrementos discutidos nesta análise e permitir atuação coordenada dos agentes.
O mapa abaixo vincula os temas às fichas existentes. A atribuição de uma ficha
identifica o ponto de integração e o responsável por área. O estado efetivo
de cada capacidade exige conferir fonte, composição e evidência atuais.
Posse, estado, branch/PR, SHA, bloqueio e próxima ação continuam na fila de
CONTINUAR_AQUI. Os temas futuros devem ampliar suas fichas quando assumidos,
conservando o histórico e os aceites já demonstrados.

### Mapa de atuação por capacidade

| Tema | Ficha e responsável por área | Próxima investigação, implementação ou aceite |
|---|---|---|
| Comissões, taxas, fills, parciais e ciclos | E14-CALC-01, T20.2 / HADOUKEN e Analytics | Reproduzir o custo de entrada repetido, fixar atribuição por fill/ciclo e reconciliar resultado líquido, PF, contagem e curvas nos consumidores |
| Fórmula e aquecimento do CCI | IND-CCI-01, T19.3/21 / Dados e indicadores | Corrigir o desvio da mesma janela, provar aquecimento e fronteiras, inventariar cálculos e resultados derivados afetados e conferir paridade nativa |
| Atualização incremental de agressão | IND-AGGR-01, T19.11 / indicadores HADOUKEN | Comparar lote/incremental com AS_SERIES, barra atual/nova barra e revisão de histórico, depois qualificar consumidores e artefato |
| Histórico, frescor e ausência de métricas | E14-METRIC-01 / painel e Analytics | Reproduzir falha/leitura parcial, preservar estado explícito e cobertura, distinguir zero válido e indisponível e qualificar recuperação/cache |
| Escopo de conta, Magic, símbolo e contribuição | E14-SCOPE-01, T19.23, T20.7 / Safety e Expert | Explicitar identidade e mandato de métricas, limites, cancelamento e encerramento. Para netting, qualificar posse por contribuição e interferência manual antes de ampliar o perfil |
| Intenção durável, parcial, incerteza, reinício e reversão | E14-EXEC-01, T20.20 / Expert e Safety | Exercitar pedido aceito sem fill conhecido, reconciliação, repetição e recuperação. Ligar decisões e efeitos a IDs persistidos e proteger o saldo após parcial |
| Lote, moeda, preço, margem e filling | T19.56, T20.5/8/9 / Safety e integração | Reutilizar RiskGuard/SafeExecutor. Cobrir mínimo maior que orçamento, passos arbitrários, tick ausente, arredondamento direcional e risco/margem depois da normalização |
| SL/TP, BE, trailing e stop por barras | T19.19/57, T20.11 / Expert e pesquisa | Definir âncora e cadência, offset, prioridade e monotonicidade. Reconciliar SL observado, pedidos próprios e intervenção externa. Confirmar parcial antes da mudança dependente de stop |
| Sessão, limite diário e retenção de lucro | T19.20/48, T20.3 / Safety, Dados e Analytics | Definir sessão/relógio, floating inicial, depósitos/saques, máximo observado e devolução permitida. Simular gatilhos sobre trajetórias com ordem temporal qualificada |
| Stop ATR Triplo e plot opcional | SA-01-CONTRATO/PLOT / Expert, indicadores e painel | Fechar fórmula, seed, três bandas no mesmo TF ou variante contratada, consenso, rearme, saída e compatibilidade SL/TP. Comparar cálculo e decisões com plot habilitado/desabilitado |
| CCI com médias e filtro de distância | T19.17/21 / Expert e indicadores | Depois de IND-CCI-01, especificar episódio direcional, retirada do sinal, cooldown, snapshot ATR e uma entrada por evento, incluindo recusa e reinício |
| Setup sequencial de rompimento/pullback | T19.17/21, T20.11 / Expert e pesquisa | Definir regime, rompimento, pullback, plano pendente, expiração e invalidação em ordem estrita. Testar corrida cancelamento/fill e recuperação do plano |
| Pressão, derivada, CVD e book | T19.11/12/13, T20.6 / Dados e fluxo | Qualificar unidade, flags/proxy, janela W+intervalo, fonte, disponibilidade e truncamento. Expiração de negócios antigos precisa de tratamento distinto de agressão nova. Conferir lote/incremental |
| Gradiente, grid, preço médio e martingale | T19.22/43/56, T20.8/9 / pesquisa e Safety | Medir exposição executada/reservada, risco agregado, margem, perdas abertas e cauda. Fixar distância ATR e contrato dos níveis. A hipótese fica sujeita aos gates do módulo e à comparação elegível |
| Renko derivado de ticks | T19.41, T20.5/6 / Dados e integração | Definir box, reversão, pavio, seed, gaps e sequência por tick. Preservar relógios e preços executáveis quando um evento gera vários boxes. Provar retomada e paridade |
| Zonas de volume financeiro | T19.3/17, T20.4/5 / Dados e indicadores | Confirmar equivalência da unidade ao relato, regra de recorde, empates e validade. Preservar o que era conhecido em cada instante e revisar limites dos arrays do exemplo original |
| APTR por horário/sessão | T19.3, T20.5 / Dados e indicadores | Alinhar observações por sessão anterior elegível, declarar amostra/cobertura e fechar a referência do true range. Comparar com ATR sob os mesmos dados, custos e risco |
| Regimes, volatilidade e sazonalidade | T13.3, T19.21 / Analytics e indicadores | Reutilizar o catálogo de regimes, declarar atraso/histerese/estado incerto e avaliar ATR_ratio/slope como variantes. Justificar limiares e rótulo Markov com fonte e método |
| Contrato de sinal, MTF e handles | T19.2/3/33, T20.26 / SDK, Dados e Expert | Versionar buffers e estados, barra confirmada, available_at, revisão, evento consumido e disponibilidade. Conferir CopyBuffer completo, ownership de handles e liberação no ciclo suportado |
| Walk-forward, permutação e seleção | T19.27, T20.12/13/14/15 / pesquisa | Congelar hipótese nula, universo, tentativas, custos e holdout. Tratar NaN/denominadores/empates, reexecutar seleção quando pertinente e medir incerteza. Acrescentar Martin/Ulcer como métricas definidas |
| Viés temporal, sobrevivência e frequência | T20.4/5/12/13, T19.20 / Dados e pesquisa | Versionar universo histórico, contratos e ajustes conhecidos à época. Especificar custo/preço de execução, instante do sinal e cooldown. Verificar causalidade além do shift aplicado ao retorno |
| ONNX, modelos e matrizes | T19.2/23, T20.26/27 / pesquisa e integração | Tratar como evolução futura. Validar artefato/licença, schema, dtype/shape, feature order, transformações treinadas, hash e runtime. Medir latência/memória e conferir decisão indisponível e paridade |
| Painel operacional e desempenho visual | E14-METRIC-01, SA-01-PLOT, T19.34 / painel e Expert | Reutilizar PanelHost/GuardianPanel e bypass do otimizador. Mostrar frescor, motivo de bloqueio, revisão e resultado observado. Medir callbacks e equivalência das decisões com renderização opcional |
| Atalhos, boleta e controle manual | T19.30, T20.20/22 / interface e Safety | Definir foco/armamento, prévia, repetição de tecla, cotação/cursor válidos e revalidação no ato. Acompanhar pausa, cancelamento, encerramento e resultado incerto no mesmo protocolo |
| Configurador guiado | LAB-09, T19.25/26/30 / presets e interface | Prioridade Alta após operações funcionais. Exibir unidades, limites, campos condicionais, explicação de efeito, comparação de revisão e resumo. Conferir inputs salvos, compilados e observados no EA |
| Dashboard consolidado e inspetor | LAB-05/06, T20.22/23 / Analytics e interface | Prioridade Alta após operações funcionais. Compartilhar filtros e seleção temporal, líquido/DD/exposição/MAE/MFE/distribuições. Navegar da operação até sinal, confirmação, pedido, fill, proteção e saída com motivos |
| Exportação e pacote reproduzível | T16.12, T20.16/25 / Analytics e presets | Exportar pelo ledger/journal, com schema, identidade saneada, revisão, moeda e estado provisório/final. Qualificar concorrência, lock, interrupção, retomada e retenção do arquivo |
| Recursos, cache e cancelamento | T20.24/25 / donos dos produtores e integração | Reutilizar fila/cache, invalidar por revisão, limitar buffers com diagnóstico e permitir cancelamento acompanhado. Medir orçamento antes de cargas e manter uma carga pesada por vez |
| Desenvolvimento assistido e artefatos de terceiros | T19.2/23, T20.26/27 / engenharia e integrador | Especificar casos e limites antes da geração, revisar código/licença/origem, qualificar integração CLI/GUI e manter permissões e evidências por composição |
| Aceites operacionais e gráficos futuros | ACEITE-NATIVO/UI/M1, LAB-10 / QA, operador e integração | Separar compilação, simulação, compositor real, entrada física, acessibilidade, login, MT5/Tester, DEMO e observação. Registrar dependência exata. Avaliar arquitetura gráfica no marco posterior solicitado |

As fichas e seus contratos permanecem nas fontes de planejamento existentes.
Os detalhes dos achados ficam nas seções anteriores deste documento e no
complemento do Super ATR. [F19](PLANO_F19_PILARES_PRESETS.md),
[F20](PLANO_F20_ROBUSTEZ.md),
[backlog Super ATR](LAB_SUPER_ATR_PENDENCIAS_20261024.md).

### Cruzamentos de análise a preservar

| Cruzamento | Pergunta e comparação requeridas | Evidência para interpretar o resultado |
|---|---|---|
| Contabilidade × gestão × excursões | Como parcial, BE, trailing, custos e lote alteram líquido, DD e MAE/MFE por ciclo? | Mesmo conjunto de operações/dados elegíveis, custos por fill reconciliados, trajetórias e ordem dos gatilhos conhecidas ou limitação explícita |
| ATR/Triplo/APTR × regime × sessão | Qual componente acrescenta informação depois de controlar horário, volatilidade, spread e risco? | Banda isolada, pares e triplo, depois variante sazonal e filtro, por ablação com protocolo/holdout congelados |
| Fluxo × volume × liquidez × latência | A variação de pressão acompanha negócios novos, janela expirada ou mudança de cobertura? | Fonte e unidade fixas, janela completa, eventos repetidos/fora de ordem diagnosticados e preço executável observável |
| Setup × MTF/Renko × relógios | O sinal já estava disponível quando o plano ou a ordem foi produzido? | event_time, available_at, observado em, revisão e ID do evento/box ligados ao pedido, incluindo múltiplos boxes no mesmo tick |
| Gradiente/parciais × conta compartilhada × risco | Que exposição e reserva permanecem após fills, cancelamentos ou interferência de outro owner? | Escopo de conta/Magic/símbolo/contribuição, posição e pendentes reconciliados, margem/custos elegíveis e recuperação de incerteza |
| Preset × otimização × múltiplas tentativas | O resultado sobrevive à seleção, ao período reservado e à comparação com referências simples? | Todas as tentativas registradas, p-valor/metodologia válidos, custos, incerteza, estabilidade e benefício incremental fora da amostra |
| Painel/plot × executor × journal | O que o usuário vê corresponde ao parâmetro aplicado, ao sinal consumido e ao efeito observado? | Mesmo run/revisão/ID, teste pareado com visual ligado/desligado e estados de pedido/fill/falha/recuperação verificáveis |
| Recursos × cobertura × confiança | Truncamento, atraso ou cancelamento afetaram o dado e a decisão? | Orçamento e retenção medidos, cobertura/frescor explícitos, invalidade propagada e replay da primeira divergência com insumos preservados |

### Protocolo de assunção e encerramento

1. Ler a fila única, conferir owner, fonte efetiva, pins e PR. Escolher a ficha
   correspondente e registrar estado, branch/PR e próximo passo antes da escrita.
2. Distinguir falha reproduzida no produto, hipótese, erro de exemplo externo
   e capacidade já presente. Registrar comando, ambiente, insumos saneados,
   resultado esperado/observado, alcance e aceite pertinente ao recorte.
3. Integrar a menor correção ou extensão aos produtores, contratos, serviços,
   CLI/GUI e executor oficiais afetados. Acrescentar regressão do limite e
   verificar dependências e resultados antigos que precisam de requalificação.
4. Validar em lotes proporcionais e sequenciais. Revisar arquivos, segredos,
   diff e documentação. Publicar commit/PR com SHA remoto confirmado e
   evidências associadas. Atualizar estado e próximo passo na fila.
5. Quando faltar conta, hardware, sessão ou dados, registrar bloqueio preciso
   e avançar no recorte independente. Um item concluído precisa de evidência
   correspondente ao seu aceite e de publicação conferida.

Esta demanda publica o registro de auditoria e desenvolvimento futuro. Os oito
itens corretivos/contratuais conservam seus estados atuais na fila. Os
demais temas ampliam as fichas por capacidade quando assumidos. Resultados
financeiros e prontidão operacional exigem as evidências dos marcos respectivos.
