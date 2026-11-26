# Preset técnico EURUSD no Strategy Tester

Abra o MT5 no perfil portátil DEMO autorizado e confirme o servidor
MetaQuotes-Demo e a conta hedging em USD. Encerre o terminal antes de rodar o
Tester pelo aplicativo. Use a configuração Linux do projeto e o ambiente
gerenciado. As credenciais permanecem no terminal.

Congele a seleção no Laboratório e execute o mesmo arquivo no Tester.
`FITA` identifica uma pasta com o manifesto e os blocos de ticks
congelados em dias inteiros. `HIPOTESE` identifica o JSON com
`definition`, `apply_invalidation`, `stop_points` e `target_points`.
Os 12 módulos nativos são selecionados na ordem do catálogo quando
`--strategy` não é informado. Com `--pattern`, a hipótese ocupa o 13º lugar.
Use `--strategy` repetido para definir a prioridade em controles específicos.

```bash
.venv/bin/python -m mt5_extracao.cli laboratory ultra-market-preset-freeze \
  --snapshot "$FITA" --pattern "$HIPOTESE" --out "$EVIDENCIAS/eurusd-v1.json"
.venv/bin/python -m mt5_extracao.cli laboratory ultra-market-tester-run \
  --mode portfolio --snapshot "$FITA" --preset "$EVIDENCIAS/eurusd-v1.json" \
  --out "$EVIDENCIAS/tester" --timeout 900
```

Na interface do Laboratório, abra **Carteiras Ultra**, use **Gerar preset
EURUSD…** e selecione o JSON em **Executar preset no Tester…**. A execução
mostra o nome, a versão, o prefixo do hash, a etapa e o caminho do recibo.
Ao concluir, abre o relatório de conciliação. Durante a execução,
`receipt.json` registra a etapa e a última etapa concluída. Antes de
repetir, confira a atividade do terminal e as mudanças de data e tamanho
do recibo e dos logs.

## Revisões e controles

Para congelar ORB com uma janela diferente, passe um arquivo
`--parameters` contendo `{"orb":{"range_minutes":15}}`. Use
`--previous eurusd-v1.json` para derivar a versão seguinte sobre a mesma
fita. Por exemplo, um arquivo com `{"orb":{"range_minutes":45}}` altera
só a janela ORB. Uma nova versão também pode receber uma revisão da
hipótese via `--pattern`. Preserve o JSON original, altere somente
`apply_invalidation` e rode as duas versões sobre a mesma fita para
isolar o efeito da invalidação.

A hipótese executa condição e contexto direcionais no fechamento do
M1, com OHLC, `prior_close` e literais na grade de preço. Aceita
`lt`, `lte`, `gt`, `gte`, `and`, `or`, `not` e invalidação
`cancel_before_entry`. A entrada pode ocorrer no próximo tick
executável. O modo aceita `repetition_policy=all` e relógio
`mt5_raw_clock`. Features externas, outros timeframes e edições de
grade, pausa ou reativação ficam fora deste contrato técnico.

## Recibos e decisões

- `preset-frozen.json` reproduz a seleção. `receipt.json` guarda os
  proprietários, vínculos, entradas, vetos, proteções e saldo conciliado.
- `report.htm` é o relatório nativo. `carteira.html` apresenta a
  conciliação, com os tickets atribuídos a cada proprietário.
- O journal registra `PRESET`, `INPUTS`, `RATE`, `RAW` e `BAR`.
  As duas últimas linhas por decisão mostram os sinais antes e depois
  da validação do stop. `ENTRY` informa a resposta ou o veto da ordem.
  `RATE` preserva os valores binários das barras do Tester na
  representação decimal de 17 algarismos significativos. No Bollinger/RSI,
  a comparação de preço absorve diferenças de cálculo de até
  `10⁻¹²` vezes a banda central. O Squeeze aplica o mesmo limite no
  momentum quando o preço coincide com a média. Os dois limites ficam
  congelados no preset.
- Use `laboratory ultra-market-compare-signals --receipt RECEIPT_JSON`
  para confrontar as estratégias Python com os prefixos de barras do
  journal. O recibo principal confronta a hipótese declarativa com a
  fita. A comparação das curvas do Super ATR usa a referência canônica
  e tem sua própria evidência.

O perfil congelado destina-se ao Tester. A qualificação econômica exige
outro período, ainda inédito, com custos, calendário e critérios
congelados antes da abertura da reserva.

## Pesquisa de mercado e política portátil

Abra **Carteiras Ultra → Pesquisar candidatos no Tester…** e escolha a
fita de treino, o plano JSON e a nova pasta de resultados. Aplique a
política produzida em **Executar preset no Tester…** e escolha uma fita
posterior. A interface usa o mesmo fluxo de pesquisa e execução da CLI.

```bash
.venv/bin/python -m mt5_extracao.cli --json laboratory ultra-market-research \
  --training "$TREINO" --plan "$PLANO" --out "$EVIDENCIAS/pesquisa" --timeout 900
.venv/bin/python -m mt5_extracao.cli --json laboratory ultra-market-tester-run \
  --mode portfolio --snapshot "$FITA_POSTERIOR" \
  --preset "$EVIDENCIAS/pesquisa/policy.json" --out "$EVIDENCIAS/execucao" --timeout 900
```

Exemplo de plano técnico para comparar duas médias em treino. O piso
negativo permite examinar alternativas de desempenho negativo.

```json
{
  "max_attempts": 2,
  "max_modules": 1,
  "min_cycles": 1,
  "min_net_cents": -2000,
  "candidates": [
    {"id": "ema-10", "strategy": "ema_atr", "parameters": {"fast": 10}},
    {"id": "ema-30", "strategy": "ema_atr", "parameters": {"fast": 30}}
  ]
}
```

Cada ensaio usa um único módulo no mesmo histórico do Tester. O
Laboratório ordena os candidatos elegíveis por lucro líquido de treino
e resolve empates pela ordem registrada no plano. Seleciona até
`max_modules`, com uma alternativa por módulo. `selection.json` inclui
os resultados, os vetos do critério, o período e os caminhos dos recibos.
Uma seleção vazia termina com `no_candidate`. `progress.json` mostra a
etapa, os ensaios concluídos e o recibo em andamento. A pontuação isolada
sustenta a seleção técnica. A carteira conjunta tem recibo próprio.

`policy.json` fixa parâmetros, prioridades, gestão, requisitos de
mercado, fontes e origem do treino. O recibo de cada execução vincula a
mesma política à fita, janela, especificações observadas, regra de
aquecimento, fontes geradas, EX5 e relatório. A política portátil exige
EURUSD M1, dias completos no relógio bruto do servidor, ambiente
MetaQuotes-Demo hedging em USD e as especificações congeladas de
preço, volume e execução. O formato preso à janela permanece verificável.
Use `ultra-market-preset-freeze --portable` para controles técnicos
pré-definidos.

### Parâmetros da pesquisa

Os valores vão de `candidates[].parameters` a `profile.modules[].inputs`
e aos valores literais da referência MQL5 que compõe o Expert. Consulte
`composition.registry.STRATEGY_PARAMETERS` para tipos, unidades e limites.
A validação também confere combinações incompatíveis e a presença de
volume de negócios quando solicitado pelo VWAP.

| Módulo | Parâmetro → campo nativo |
|---|---|
| `ema_atr` | `fast` → `InpFastPeriod`, `slow` → `InpSlowPeriod`, `trend` → `InpTrendPeriod`, `stop_atr` → `InpStopAtrMult`, `trail_atr` → `InpTrailAtrMult` |
| `donchian` | `entry_period` → `InpEntryPeriod`, `exit_period` → `InpExitPeriod`, `adx_min` → `InpAdxMin`, `atr_avg` → `InpAtrAvgPeriod`, `stop_atr` → `InpStopAtrMult` |
| `bb_rsi` | `bb_period` → `InpBbPeriod`, `bb_k` → `InpBbDeviation`, `rsi_period` → `InpRsiPeriod`, `rsi_low` → `InpRsiLow`, `adx_max` → `InpAdxMax`, `stop_atr` → `InpStopAtrMult`, `max_bars` → `InpMaxBars` |
| `orb` | `range_minutes` → `InpRangeMinutes`, `target_mult` → `InpTargetMult`, `min_range_atr` → `InpMinRangeAtr` |
| `pullback` | `trend` → `InpTrendPeriod`, `adx_min` → `InpAdxMin`, `rsi_period` → `InpRsiPeriod`, `rsi_pullback` → `InpRsiPullback`, `swing_bars` → `InpSwingBars`, `target_r` → `InpTakeProfitR` |
| `squeeze` | `period` → `InpPeriod`, `bb_k` → `InpBbDeviation`, `kc_mult` → `InpKeltnerMult`, `lookback` → `InpLookback`, `stop_atr` → `InpStopAtrMult`, `trail_atr` → `InpTrailAtrMult` |
| `s09_reference` | `fast` → `InpFastPeriod`, `slow` → `InpSlowPeriod`, `warmup_bars` → `InpWarmupBars`, `stop_points` → `InpStopPoints`, `target_points` → `InpTargetPoints` |
| `stoch_ema9` | `k_period` → `InpKPeriod`, `slowing` → `InpSlowing`, `d_period` → `InpDPeriod`, `ema_period` → `InpEmaPeriod`, `upper_level` → `InpUpperLevel`, `lower_level` → `InpLowerLevel`, `stop_offset_points` → `InpStopOffsetPoints`, `target_points` → `InpTargetPoints` |
| `vwap_bands` | `entry_band_pct` → `InpEntryBandPct`, `target_band_pct` → `InpTargetBandPct`, `stop_offset_points` → `InpStopPoints`, `volume_source` → `InpVolumeSource` |
| `madrid` | `first_period` → `InpFirstPeriod`, `period_step` → `InpPeriodStep`, `distance_atr` → `InpDistanceAtr`, `distance_points` → `InpDistancePoints`, `trail_line` → `InpTrailLine`, `offset_atr` → `InpOffsetAtr` |
| `ha_hilo` | `hilo_period` → `InpHiLoPeriod`, `hilo_offset` → `InpHiLoOffset`, `stop_buffer_points` → `InpStopBufferPoints` |
| `super_atr_triple` | `seed_start_broker` → `InpSeedStart`, `period` → `InpPeriod`, `mean` → `InpMean`, `deviation1` → `InpDeviation1`, `deviation2` → `InpDeviation2`, `deviation3` → `InpDeviation3`, `initial_side` → `InpInitialSide`, `entries_enabled` → `InpEntriesEnabled` |
| `pattern_definition` | `pattern.definition` → expressão MQL5 gerada, `apply_invalidation` → condição, `stop_points` → `InpStopPoints`, `target_points` → `InpTargetPoints` |

A hipótese usa as modalidades OHLC M1 e `cancel_before_entry` documentadas
acima. Auto e ticks são modalidades de VWAP válidas em fitas com cotações.
O modo Real exige volume de negócios registrado. O Super ATR usa a
primeira barra observada da janela como âncora padrão e admite uma
âncora explícita no relógio do broker.

### Limites locais, gestão e revisão

Cada candidato pode ter `owner_rules.risk` com `daily_loss_cents`,
`daily_profit_cents`, `max_attempts` ou `max_losses`. Uma tentativa
é uma ordem de abertura com pelo menos um preenchimento no dia do
servidor. O limite de perdas conta ciclos encerrados com resultado
líquido negativo e atribui a perda ao dia da última saída. Uma saída
parcial altera o resultado diário realizado e conserva o ciclo até
zerar o volume. O realizado usa lucro, comissão, tarifa e swap do
histórico de cada posição. Uma trava atingida permanece válida até a
virada do dia. As posições continuam sob a gestão do seu módulo e
as entradas também passam pelos limites globais de exposição, margem
e perda. O recibo confronta `LOCAL` com os negócios por identificador e
ticket. `owner_rules.management` admite `partial_r`, `partial_fraction`,
`breakeven_after_partial` e `trailing_atr` sobre os gatilhos do
motor canônico.

No mesmo passe, acrescente `--successor SEGUNDO_JSON --switch-at-msc TICK`
para instalar um sucessor direto de gestão no tick ou no próximo
disponível. Uma posição incumbente conserva a política de origem até o
encerramento. Cada módulo passa ao sucessor quando está livre e
conciliado. O journal registra `ROTATE` por módulo. `--restore-at-msc`
exercita a reidratação do executor e a conciliação do livro no próprio
passe, registrando `RECOVER`. A reabertura do MT5 com posições herdadas
exige evidência no ambiente de destino. O plano de pesquisa técnico e o
observador da DEMO não habilitam negociação prospectiva.

O plano pareado EURUSD registra a hipótese de reversão e seus cinco braços. O botão de pesquisa aceita como destino a pasta que contém o plano já congelado para retomar uma tentativa interrompida. A interface sugere o último preset selecionado ao abrir o diálogo de execução. A CLI aceita a publicação posterior de política no recibo de um passe preparado para recebê-la. [Protocolo, comandos e limites](repasse/ULTRA_EURUSD_CONCILIACAO.md)
