# Criptomoedas pela API pública da Binance

A conta de demonstração da MetaQuotes não tem criptomoedas: seus grupos são Nasdaq, Forex, índices e
metais. `mt5x crypto` usa a API pública de mercado da Binance, que dispensa conta e chave.

```bash
mt5x crypto list --quote BRL
mt5x crypto list --symbols BTC,ETH
mt5x crypto extract --symbols BTCUSDT,ETHBRL --tf M1 --from 2026-01-01
mt5x crypto extract --symbols BTCUSDT,ETHBRL --tf M1
mt5x --db database/cripto_binance.db backtest --symbol BTCUSDT --tf M1 --strategy bb_rsi
```

- **Primeira extração e continuação.** A primeira extração pede `--from`. Sem `--from`, a extração
  continua da última vela gravada.
- **Banco próprio.** Os dados vão para `database/cripto_binance.db` (ou o `--db` informado), com
  fonte `binance@api.binance.com`. Uma base aceita uma única fonte, então a Binance não se mistura
  com MT5 nem com a B3.
- **Velas.** São as do mercado à vista, de M1 a MN1, desde a listagem do par, com horário em UTC.
  Só velas fechadas são gravadas.
- **Volumes.** `tick_volume` é o número de negócios. `real_volume` é o volume financeiro na moeda de
  cotação (USDT, BRL…), arredondado, porque a coluna do banco é inteira.
- **Especificação.** Na primeira extração de cada par, o app grava a especificação a partir dos
  filtros públicos: passo de preço, lote mínimo e passo de lote, contrato de 1 unidade da moeda
  base e lucro na moeda de cotação.
- **Custos.** O spread fica 0 e a taxa da corretora não entra no cálculo, então o custo do backtest
  é otimista. Ajuste o custo antes de tirar conclusões.
- **Limite de uso da API.** Quando a API responde 429 ou 418, o comando espera e tenta de novo.
