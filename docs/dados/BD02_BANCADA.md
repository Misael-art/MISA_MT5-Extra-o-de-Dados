# BD02 — Bancada compacta, referências independentes e orçamento

Recorte de 2026-10-22, vinculado a F15/T20.4/T20.24/T20.25 (infraestrutura de testes existente).
Depende de [BD01](BD01_INVENTARIO.md). Não altera runtime, schema nem dados do usuário.

## O que entra

[tests/bench.py](../../tests/bench.py) gera, em memória, barras no formato `_bars_derived`
(o mesmo dos produtores T13.3 e do registry). Toda massa leva `origin=synthetic:bd02` e
`synthetic=True`; nada é gravado em disco. Cenários: tendência, lateralidade, preço constante
(empates e denominador zero), passeio aleatório, lacuna, duplicidade, fora de ordem, revisão
tardia (revisão 2 com instante de recebimento), close ausente, sessões que cruzam a meia-noite
UTC, transição real de fim de horário de verão em fuso local (Europe/Athens, 25/10/2026: offset +3→+2 e hora local 03:00 repetida), volume real zero × ausente × parcialmente
desconhecido, três escalas de preço (1e-4, 1, 1e5) e **duas bases com o mesmo símbolo**
(`EURUSD`) e dados/identidades distintos (`base_id`, `provider`).

[tests/bench_reference.py](../../tests/bench_reference.py) implementa, em Python puro e sem
importar `strategies.indicators`, pandas ou numpy, as definições escritas de retorno, true
range, suavização de Wilder (duas sementes), RSI, ER de Kaufman, largura de Bollinger
(desvio populacional) e Donchian atual/anterior. São oráculos independentes do código testado.

## O que foi verificado

[tests/test_bd02_bench.py](../../tests/test_bd02_bench.py), 15 casos:

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

## Correções após revisão independente (V03/V04, 23/10/2026)

A primeira versão (8f63786) tinha dois defeitos, preservados aqui como histórico:

- **V03:** `computed_at_utc` era fixo em 22/10/2026 e o cenário “dst” ia até 31/10 em UTC puro:
  216/240 barras tinham computação anterior ao fechamento, e não havia transição de offset local.
  Agora `computed_at_utc = bar_end_utc + 2 s` em toda massa válida, testado em todos os cenários
  (`test_every_valid_scenario_is_computed_after_its_bar_closes`); o cenário `dst` usa fuso real
  com fold verificado. Massa impossível existe só em `invalid_computed_before_close()`, fora de
  `scenarios()`, marcada `valid=False`; quem a consumir deve rejeitá-la (resposta do sistema: BD05).
- **V04:** 559 009 B era o tamanho somado do CSV canônico, não memória. As métricas agora são
  separadas e nomeadas pelo escopo.

## Orçamento (medição de 23/10/2026, Python do venv, Linux)

| Métrica | Escopo | Teto | Medido |
|---|---|---:|---:|
| Fixtures persistentes | arquivos em `tests/fixtures/` | 100 MiB | 30 111 B (nada acrescentado) |
| CSV canônico | soma dos CSV serializados, **nunca gravados** | 250 MiB | 536 402 B |
| DataFrames | `memory_usage(deep=True)` somado | 250 MiB | 1 010 300 B |
| Pico de alocação da geração | `tracemalloc` durante `scenarios()` | 250 MiB | 1 130 821 B |
| Duração da geração | `perf_counter` | 30 s | 0,26 s |

Nenhuma dessas métricas é o pico RSS de um job de materialização; essa medição pertence a BD14.
A bancada não grava temporários em disco. Instalação Python/MT5/Wine e artefatos nativos têm
orçamentos próprios. Nenhum download é necessário.

## Impressões digitais (SHA-256 completo do CSV canônico)

| Cenário | Barras | Bytes CSV | SHA-256 |
|---|---:|---:|---|
| trend | 240 | 32211 | `2e3f34ecfb5cad681dbe94992755e6b1a0a8d9ca328a63dc26861a4fb49a6a5a` |
| lateral | 240 | 33692 | `007425e1351f232c59489c3a69f83abaf1eb657709c769ca53311b11dcd73e89` |
| flat | 240 | 27974 | `6e6f56f096ad50386e914695f97886ca3cdca1b443b098bdaf7574b8a2570030` |
| random_walk | 240 | 37521 | `a88366b71ff523e21f3abef11b491496c4cd14be467d45d42209f7bdba7728fe` |
| gap | 235 | 31548 | `acdc8b1606e9789000c2a288067c2751c8e3e154b97ad9706e271a527d7ef676` |
| duplicate | 241 | 32344 | `393ba2bcc4091d454ca3407c47b4f7c552e4dcdec9556c69c0da33e62c0882c0` |
| out_of_order | 240 | 32211 | `8521fa8e754a5f127bdcfe4b5f7fda1189df8ff5a6834744e366c6e06991aca0` |
| revision | 240 | 32206 | `f4bc91b9517a246cce0ff3d7282c5a60368bb6982e4cedabf1f95db58dc5dd2a` |
| missing_close | 240 | 32208 | `022d3afd124a311f1075f0c874ae53cf795784f5b3088b5f155f04ff06effa10` |
| overnight | 144 | 22485 | `df7b06a91738b883f1b1c0ddf2917f2ca52835718b8939a8e165010599b4c9d5` |
| dst | 96 | 15049 | `c29f5cca042c48ec5af3c7a192a5c19f00f79c5280bdf144f8ba9e42d44a7711` |
| volume_zero_known | 48 | 7589 | `e0d1c1015dc99f25354a93caf8a7335d38249e8f90bc12af3ea3bfafc453e221` |
| volume_absent | 48 | 7445 | `12d1bf93d14730f8bac989594d05b14ef3ab82f949c1476e42a5ddffd7910bc2` |
| volume_partial_unknown | 48 | 7539 | `96b2d694fd1c8e55a304f296cea42f733eca673a52d0c0a00a212a5e9d54c65d` |
| scale_0.0001 | 240 | 41737 | `55e9867e4ddd5de8d4d1ad3b9feafe759bddeaf788e75891893e6eaba48dfedb` |
| scale_1 | 240 | 38351 | `1b099cda9297b009628180128453644a92000ab88ff9322c24eb2f09c81c74d1` |
| scale_100000 | 240 | 37909 | `379eada601fbbb3efc73740aa06a45cf0ed09dd37a0df99f87284e40448e1175` |
| base_a | 240 | 32451 | `30e6e06bc02722fb8ad49ebfb5a2dab3216a722823dd41efef73392f8a3f6c1b` |
| base_b | 240 | 33932 | `1a0ca6b78877a5614065e54d702e26aa93f45cac0ea123c1c5eace00bbb24efd` |

## Limites

Referências cobrem oito saídas do registry; as demais famílias recebem referência em suas
fichas (BD08b/BD09). A bancada não prova fonte real, calendário de corretora ou PostgreSQL.


## Resolução parcial do achado em BD08b (23/10/2026)

O [recorte RSI/ATR](BD08B_WILDER.md) preserva e explicita a semente na primeira
observação para as quatro saídas RSI14/slope3/ATR14/ATR14-close; acrescenta
reinício e aquecimento após ausência. O teste de divergência SMA permanece.
Os normalizadores de MACD/EMA e Keltner mantêm qualificação própria pendente.
A descrição acima conserva a fotografia em que o achado foi levantado.
