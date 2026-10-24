# BD07 — Reconciliar calculadores (1º recorte, 23/10/2026) — PARCIAL

Vinculação T19.3/T6.1/T4.5. Depende de [BD06](BD06_CONTRATO.md).

## Mudança de produção
`IndicatorCalculator` (legado das colunas OHLCV) sem `pandas-ta` gravava MACD e Bollinger como
colunas inteiras NaN, sem sinal. Agora declara `unavailable_outputs` (atributo e
`DataFrame.attrs`) com o motivo e avisa uma vez nos logs. As colunas continuam NaN (sem fórmula
substituta não comprovada) e o comportamento com `pandas-ta` presente não muda.

## Variantes de retorno (família do piloto) — nomeadas, não equivalentes
| Saída | Definição | Dono |
|---|---|---|
| `price.return_1/5/20` (registry) | close[t]/close[t−N] − 1, N em **barras**, fração | T19.3 — piloto BD08a |
| `var_pct_*` (legado) | variação % em janelas de **relógio** (5/15/30/60 min, diária, dia anterior) | T6.1, colunas legadas |
| `session.return` (produtor T13.3) | retorno da **sessão** observada | T13.3 |
Nenhuma é oráculo da outra; o piloto usa só a definição do registry.

## Provas
`tests/test_bd07_calculators.py` (3): sem pandas-ta as saídas faltantes são declaradas; com
pandas-ta (stub) nada é declarado faltante; contrato do retorno do piloto.

## Pendente
`AdvancedIndicators` (coleta em tempo real) e cálculos privados de estratégias (VWAP, estocástico
lento, Heiken-Ashi, Madrid) continuam fora do registry; semente de Wilder (BD08a/BD08b).


A semente das quatro saídas RSI/ATR do registry foi explicitada no
[recorte BD08b](BD08B_WILDER.md), sem alterar as funções legadas de estratégia.
Essa qualificação não cobre os calculadores privados nem as outras famílias.
