# Estratégias, backtest e triagem de ativos

Este módulo usa a base que você extraiu para responder, com dados, a três perguntas:

1. **Este ativo é operável?** (Filtro A: custo, liquidez, qualidade dos dados, histórico, lote)
2. **Há oportunidade agora?** (Filtro B: regime de mercado e score de 0 a 100)
3. **Esta estratégia funciona neste ativo e timeframe?** (Filtro C: backtest com walk-forward, robustez e Monte Carlo)

Só a combinação que passa nos três merece estudo mais aprofundado.

> **Importante:** resultados de backtest **não garantem** resultados futuros. O objetivo é
> descartar o que não funciona e priorizar estudos, não recomendar investimentos.
> O programa **não envia ordens**.

## Passo a passo

```bash
# 0. Tenha dados: D1/H4 com 3+ anos ou intradiário com 1+ ano
mt5x extract --symbols WIN$N,WDO$N,PETR4 --tf D1 --from 2018-01-01

# 1. Especificações de contrato (tick, valor do tick, lote mínimo) — uma vez por símbolo
mt5x specs --symbols WIN$N,WDO$N,PETR4

# 2. Triagem: quem está apto e o que fazer agora
mt5x screen --tf D1

# 3. Validação (Filtro C) — demora alguns minutos por ativo
mt5x validate --tf D1 --strategies all

# 4. Relatório completo no navegador
mt5x report --tf D1 --open
```

Sem `--symbols`, os comandos usam todos os símbolos que já têm dados no timeframe.
No Linux use `./run.sh --cli <comando>`; no Windows `run.bat --cli <comando>` (se `mt5x` não estiver no PATH).

Na interface gráfica: menu **Estratégias** → *Triagem de ativos*, *Validar estratégias* ou
*Gerar relatório e abrir no navegador*. Os símbolos selecionados na tela principal vêm preenchidos.

## As seis estratégias

| Chave | Estratégia | Regime | Acerto típico | Risco:retorno | Complexidade | Timeframes |
|---|---|---|---|---|---|---|
| `ema_atr` | Tendência EMA+ATR | tendência | 35–45% | 1:2 ou mais | baixa | H1, H4, D1 |
| `donchian` | Donchian/Turtle | tendência | 30–40% | 1:2,5 ou mais | baixa | H4, D1 |
| `bb_rsi` | Reversão BB/RSI | lateral | 60–70% | ≈ 1:1 | baixa | M15–D1 |
| `orb` | Opening Range Breakout | rompimento | 40–50% | 1:1,5 a 1:2 | média | M5, M15 |
| `pullback` | Pullback em momentum | tendência | 45–55% | 1:2 | média | H1, H4 |
| `squeeze` | Squeeze BB/Keltner | compressão → rompimento | 40–50% | 1:2 ou mais | média | H1, H4 |

Taxas de acerto e risco:retorno são referências típicas da literatura, **não promessas**.

- **Tendência EMA+ATR:** compra quando a EMA20 cruza acima da EMA50 com o preço acima da EMA200; stop de 2×ATR, stop móvel de 3×ATR; sai no cruzamento contrário.
- **Donchian/Turtle:** compra no rompimento da máxima de 20 barras; sai no rompimento da mínima de 10. Só opera com ADX > 20 ou ATR acima da média. Commodities, índices e FX em D1/H4.
- **Reversão BB/RSI:** com ADX < 20 (mercado lateral), compra quando a mínima toca a banda inferior de Bollinger e o RSI(2) < 10; sai na banda central; stop de 1,5×ATR ou 10 barras.
- **Opening Range Breakout:** faixa = máxima/mínima dos primeiros 30 minutos do dia; compra no fechamento acima da faixa; stop do outro lado; alvo de 1,5× a faixa; **sempre zera no fim do dia**. WIN/WDO, DAX, US500/NAS100 em M5/M15.
- **Pullback em momentum:** preço acima da EMA200 e ADX > 25; compra quando o RSI(14) recua abaixo de 40 e vira para cima; stop abaixo do último fundo; realiza metade em 1R (e move o stop para o preço de entrada) e o resto em 2R.
- **Squeeze:** quando as Bandas de Bollinger ficam dentro do Canal de Keltner (compressão), compra no rompimento da banda a favor do momentum; stop de 1,5×ATR e stop móvel de 2,5×ATR. Ouro, índices e cripto em H1/H4.

Vendas são simétricas. Para testar outro parâmetro: `mt5x backtest --symbol WIN$N --tf M15 --strategy orb --param range_minutes=15`.

## Evite no início

- **Martingale:** dobrar a posição após perdas — uma sequência ruim zera a conta.
- **Grid sem stop:** acumula posições contra o movimento sem limite de perda.
- **Scalping de poucos pontos:** custos (spread, corretagem, slippage) consomem a vantagem.
- **Arbitragem de latência:** exige infraestrutura dedicada; no varejo o resultado é ilusório.
- **ML caixa-preta:** sem entender o porquê, é fácil ajustar ao ruído (overfitting).

## Filtro A — o ativo é operável? (semanal)

Basta falhar em um critério para o ativo ficar **❌ inapto**; o motivo aparece por extenso.

| Critério | Corte padrão | Chave em `[SCREENER]` |
|---|---|---|
| Spread / ATR(14) | < 10% | `max_spread_atr` |
| Liquidez (volume mediano) | ≥ percentil 30 entre os ativos analisados (com 3+ ativos) | `min_liquidity_percentile` |
| Barras com problema (OHLC inválido, lacunas no pregão, gaps > 5×ATR) | < 1% | `max_bad_bars` |
| Histórico | ≥ 3 anos (H4 e maiores) ou ≥ 1 ano (H1 e menores) | `min_years_daily`, `min_years_intraday` |
| Picos de spread (> 3× a média) | ≤ 2% das barras | `spike_multiple`, `max_spike_share` |
| Lote para 1% de risco com stop de 2×ATR | ≥ lote mínimo | `[STRATEGY] risk_per_trade`, `initial_equity` |

Sem `mt5x specs`, spread/ATR e lote não podem ser verificados: o ativo aparece com **⚠ verificar**.

## Filtro B — há oportunidade agora? (diário)

| Componente | Como é medido | Efeito |
|---|---|---|
| Regime | ADX(14): > 25 tendência, < 20 lateral | escolhe a família de estratégia |
| Força | inclinação da EMA50 / ATR e R² da regressão de 50 barras | tendência mais limpa = score maior |
| Volatilidade | percentil do ATR nas últimas 100 barras | muito baixa: aguardar squeeze; muito alta: reduzir o lote |
| Custo | spread / ATR | mais caro = score menor |
| Eficiência | Kaufman Efficiency Ratio (20) | movimento limpo favorece tendência; ruído favorece reversão |
| Evento | CSV em `[SCREENER] events_file` | evento de alto impacto nas próximas `event_window_hours` horas **bloqueia** |
| Correlação | com os símbolos de `[SCREENER] portfolio` | acima de 0,7 é penalizado |

Arquivo de eventos (opcional), por exemplo `config/eventos.csv`:

```csv
time,symbol,impact
2025-03-07 10:30,USD,alto
2025-03-19 18:30,BRL,alto
2025-03-20 09:00,*,medio
```

`symbol` é comparado como trecho do nome do ativo (`USD` afeta `EURUSD` e `USDJPY`; `*` afeta todos).

## Filtro C — a estratégia funciona aqui?

| Critério | Corte padrão | Chave em `[STRATEGY]` |
|---|---|---|
| Profit factor fora da amostra | > 1,3 | `min_profit_factor` |
| Operações fora da amostra | ≥ 100 | `min_trades` |
| Janelas walk-forward com lucro | > 50% | — |
| Robustez: cada parâmetro ±20% | todas as variações com PF > 1 | — |
| Drawdown máximo no Monte Carlo (percentil 95) | ≤ 20% | `max_drawdown` |

Como é feito:

- **Walk-forward:** os dados são divididos em 7 partes. Em 5 janelas, os parâmetros são escolhidos em 2 partes e testados na parte seguinte, que a otimização nunca viu.
- **Robustez:** cada parâmetro é alterado em −20% e +20%. Estratégia que só funciona em um valor exato provavelmente foi ajustada ao acaso.
- **Monte Carlo:** 1000 reordenações (com reposição) das operações fora da amostra mostram o drawdown que você deve esperar num cenário ruim, não só no histórico que aconteceu.

O resultado fica gravado no banco (tabela interna `_strategy_validation`) e aparece na matriz do relatório
e na coluna *Observação* da triagem (✅ validada / ❌ reprovada / não validada).

## Premissas do backtest

- Barras do MT5 são de **bid**: a compra paga o spread da própria barra (coluna `spread`); a venda a descoberto paga na recompra.
- Sinal no fechamento da barra → execução na abertura da barra seguinte (mais `slippage_points`).
- Se stop e alvo são tocados na mesma barra, conta o **stop** (premissa pessimista). Gap além do stop executa na abertura.
- Tamanho da posição: `capital × risco ÷ perda por lote no stop`, arredondado para baixo no passo de lote. Se ficar abaixo do lote mínimo, a operação é **ignorada** e contada no resumo.
- Corretagem: `commission_per_lot` por lote e por lado.
- Drawdown calculado sobre a curva das operações fechadas.

## Configuração (`config/config.ini`)

```ini
[STRATEGY]
initial_equity = 100000
risk_per_trade = 0.01
commission_per_lot = 0
slippage_points = 0
max_drawdown = 0.20
min_profit_factor = 1.3
min_trades = 100

[SCREENER]
max_spread_atr = 0.10
min_liquidity_percentile = 30
max_bad_bars = 0.01
min_years_daily = 3
min_years_intraday = 1
spike_multiple = 3
max_spike_share = 0.02
max_correlation = 0.7
portfolio = WIN$N,WDO$N
events_file = config/eventos.csv
event_window_hours = 4
```

Todas as chaves são opcionais; sem elas valem os padrões acima.

## Para desenvolvedores

Nova estratégia: crie `mt5_extracao/strategies/<nome>.py` com uma subclasse de `Strategy`
(`key`, `info`, `default_params`, `param_grid` com até 12 combinações, `_signals(df, out)` e,
se precisar, `exit_rules()`), registre em `strategies/__init__.py` e acrescente os testes de
contrato e de "sem olhar o futuro" em `tests/test_strategies.py`. Detalhes em
[PLANO_DE_TRABALHO.md](PLANO_DE_TRABALHO.md) (Fase 6).
