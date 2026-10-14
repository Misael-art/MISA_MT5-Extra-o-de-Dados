# ExemploCruzamento

Robô MQL5 criado com `mt5x robots new`. Toda ordem passa pela biblioteca de segurança `mt5x-safety` 1.4.0.
O esqueleto **não opera** até você escrever o sinal.

## O que editar

Abra `ExemploCruzamento.mq5` e escreva a sua estratégia entre os marcadores:

| Marcador | Para quê |
|---|---|
| `MT5X:PARAMETROS` | `input` da estratégia (períodos, filtros) |
| `MT5X:GLOBAIS` | handles de indicadores |
| `MT5X:INIT` | criar os indicadores (`iMA`, `iRSI`...) |
| `MT5X:SINAL` | devolver +1 (comprar), -1 (vender) ou 0 |

Não envie ordens direto (`OrderSend`, `CTrade`): use só o sinal. O stop, o lote pelo risco, os limites de perda e a
parada de emergência já estão no esqueleto, e a bateria de segurança reprova ordem fora da biblioteca.

Preencha também `ExemploCruzamento.robot.json` (descrição, regime, timeframes) e acrescente a `tunable` os parâmetros
numéricos da sua estratégia.

## Ciclo

```bash
mt5x robots validate exemplocruzamento                                  # contrato
mt5x robots compile exemplocruzamento                                   # instala e compila no MetaEditor
mt5x robots backtest exemplocruzamento --symbol <ativo> --tf <timeframe>   # Strategy Tester
mt5x robots battery exemplocruzamento                                   # bateria de segurança
```

Magic number: 175355. Não repita em outro robô.
