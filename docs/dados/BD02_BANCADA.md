# BD02 — Bancada compacta, referências independentes e orçamento

Recorte de 2026-10-22, vinculado a F15/T20.4/T20.24/T20.25 (infraestrutura de testes existente).
Depende de [BD01](BD01_INVENTARIO.md). Não altera runtime, schema nem dados do usuário.

## O que entra

[tests/bench.py](../../tests/bench.py) gera, em memória, barras no formato `_bars_derived`
(o mesmo dos produtores T13.3 e do registry). Toda massa leva `origin=synthetic:bd02` e
`synthetic=True`; nada é gravado em disco. Cenários: tendência, lateralidade, preço constante
(empates e denominador zero), passeio aleatório, lacuna, duplicidade, fora de ordem, revisão
tardia (revisão 2 com instante de recebimento), close ausente, sessões que cruzam a meia-noite
UTC, semana com mudança de horário europeia, volume real zero × ausente × parcialmente
desconhecido, três escalas de preço (1e-4, 1, 1e5) e **duas bases com o mesmo símbolo**
(`EURUSD`) e dados/identidades distintos (`base_id`, `provider`).

[tests/bench_reference.py](../../tests/bench_reference.py) implementa, em Python puro e sem
importar `strategies.indicators`, pandas ou numpy, as definições escritas de retorno, true
range, suavização de Wilder (duas sementes), RSI, ER de Kaufman, largura de Bollinger
(desvio populacional) e Donchian atual/anterior. São oráculos independentes do código testado.

## O que foi verificado

[tests/test_bd02_bench.py](../../tests/test_bd02_bench.py), 12 casos:

- determinismo por SHA-256 do CSV canônico; rótulo sintético em todos os cenários; cada cenário
  de preço tem pelo menos duas vezes o maior aquecimento do registry (39 barras);
- defeitos visíveis (lacuna, duplicata, ordem, revisão, ausência, sessão noturna);
- volume zero conhecido, ausente e desconhecido permanecem distintos;
- registry = referência independente em seis cenários para `price.return_1/20`,
  `trend.efficiency_ratio_20`, `volatility.bb_20_width`, Donchian upper atual/anterior,
  `volatility.atr_14` e `momentum.rsi_14`. Tolerância relativa 1e-9 (ordem distinta de somas
  em ponto flutuante), 1e-12 para máximos (sem aritmética); ausência deve coincidir exatamente;
- orçamento medido (abaixo).

## Achado para BD08a — semente de Wilder

O contrato do registry diz “Wilder RMA(14)”, mas `strategies.indicators.rma` usa
`ewm(alpha=1/n, adjust=False, min_periods=n)`, que semeia na **primeira observação**. O Wilder
clássico semeia com a média simples das n primeiras; a convenção dos nativos MT5 não foi
comparada neste recorte (BD11).
O teste `test_wilder_seed_divergence_is_measured_not_hidden` prova que o registry coincide com
a semente “first”, difere da semente SMA na barra 14 e converge após ~200 barras. Isso afeta
`volatility.atr_14`, `volatility.atr_pct_14`, `momentum.rsi_14*`, `momentum.macd_histogram_atr_14`
e `trend.ema20_distance_atr14`. Não foi corrigido aqui: a decisão (renomear a variante no
contrato ou criar versão com semente SMA, preservando a anterior) pertence a BD08a.

## Orçamento

| Item | Teto | Medido |
|---|---:|---:|
| Fixtures persistentes (`tests/fixtures/`) | 100 MiB | 30 111 B (nada acrescentado) |
| Temporários da bancada (CSV canônico de todos os cenários) | 250 MiB | 559 009 B, em memória |
| Geração + impressão digital | 30 s | < 1 s |

Os tetos não incluem a instalação Python/MT5/Wine nem artefatos nativos, que têm orçamentos
próprios. Nenhum download é necessário. Limpeza: a bancada não cria arquivos.

## Impressões digitais (SHA-256 do CSV canônico, 16 primeiros dígitos)

| Cenário | Barras | Bytes | SHA-256 |
|---|---:|---:|---|
| trend | 240 | 32211 | `9dbfed6cfb29047a` |
| lateral | 240 | 33692 | `adb78d34ec21d8c3` |
| flat | 240 | 27974 | `5a78526899964a90` |
| random_walk | 240 | 37521 | `143f88d0a0e19637` |
| gap | 235 | 31548 | `efb93920f1a8e6d1` |
| duplicate | 241 | 32344 | `1a1acecc9d6ebef5` |
| out_of_order | 240 | 32211 | `f38930d96a0d87c3` |
| revision | 240 | 32206 | `6c898d393f42e7ee` |
| missing_close | 240 | 32208 | `42c68f16b4b6070d` |
| overnight | 144 | 22485 | `2d5f1597952b0001` |
| dst | 240 | 37656 | `6be06c45b18721d3` |
| volume_zero_known | 48 | 7589 | `ebb5c254d8267bec` |
| volume_absent | 48 | 7445 | `a1c3c91cbe9772b3` |
| volume_partial_unknown | 48 | 7539 | `670b4b9d90bdafc3` |
| scale_0.0001 | 240 | 41737 | `96cd9c06c3fb7568` |
| scale_1 | 240 | 38351 | `297533fe925c2043` |
| scale_100000 | 240 | 37909 | `ae442662b18c71f4` |
| base_a | 240 | 32451 | `8017bf07e142380f` |
| base_b | 240 | 33932 | `2d9a7d8f9e36dfaf` |

## Limites

Referências cobrem oito saídas do registry; as demais famílias recebem referência em suas
fichas (BD08b/BD09). A bancada não prova fonte real, calendário de corretora ou PostgreSQL.
