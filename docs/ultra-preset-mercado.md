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
  `10⁻¹²` vezes a banda central. O limite fica congelado no preset.
- Use `laboratory ultra-market-compare-signals --receipt RECEIPT_JSON`
  para confrontar as estratégias Python com os prefixos de barras do
  journal. O recibo principal confronta a hipótese declarativa com a
  fita. A comparação das curvas do Super ATR usa a referência canônica
  e tem sua própria evidência.

O perfil congelado destina-se ao Tester. A qualificação econômica exige
outro período, ainda inédito, com custos, calendário e critérios
congelados antes da abertura da reserva.
