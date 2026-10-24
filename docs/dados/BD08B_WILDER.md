# BD08b — RSI/ATR: primeiro recorte (T19.3), parcial

Continuação da [sexta revisão](BD18A_CONSULTA.md), sobre #202/d1218ec.
BD08b continua parcial: este recorte cobre somente `momentum.rsi_14`,
`momentum.rsi_14_slope_3`, `volatility.atr_14` e `volatility.atr_pct_14`.
MACD, canais, ER, Alligator/AO/AC/pivôs e produtores T13.3 mantêm suas
qualificações próprias. Não fecha M4, BD12–14 ou o pilar de dados.

## Problema e decisão do recorte

O achado de semente da BD02 permitia explicitar a variante vigente ou criar
outra com SMA. Este recorte **preserva a semente na primeira observação**.
Não troca a fórmula de todos os usuários por SMA nem afirma paridade nativa.

Sondas adicionais na ponta aceita reproduziram ATR negativo como `AVAILABLE`
com mínima maior que máxima, e RSI disponível imediatamente após fechamento
ausente, ainda usando o estado anterior. O caminho de materialização também
removia barras inelegíveis antes do cálculo, comprimindo a sequência conhecida.
Esses problemas pertencem à família ainda não qualificada; não reabrem V5-01/02.

Regra fechada para a variante canônica: entrada ausente/não finita ou barra
conhecida inelegível interrompe a recorrência; a próxima sequência válida
semeia novamente e cumpre todo o aquecimento. ATR exige `low <= close <= high`
finito; RSI exige fechamento finito. Falta de coluna é `SOURCE_UNAVAILABLE`;
entrada inválida é `SOURCE_VALUE_MISSING`; prefixo insuficiente é `WARMUP`.
As funções legadas de estratégias continuam com seu comportamento anterior.

## Contrato numérico

| Saída | Variante | Primeiro valor por sequência | Unidade |
|---|---|---|---|
| RSI14 | ganho/perda com RMA: semente no primeiro delta; `s = (13*s + x)/14` | 15ª barra (14 deltas) | pontos 0–100 |
| RSI14 slope3 | RSI[t] − RSI[t−3], mesma sequência | 18ª barra | pontos por 3 barras |
| ATR14 | TR inicial = high−low; depois máximo entre amplitude e gaps ao fechamento anterior; RMA first-seeded | 14ª barra | preço |
| ATR14/close | mesmo ATR, divisão por fechamento estritamente positivo | 14ª barra | fração |

Sem perda e com ganho, RSI=100; sem ganho nem perda, RSI=50. Mercado constante
pode produzir ATR=0 e slope=0 válidos. Denominador não positivo na razão gera
`INVALID_DENOMINATOR`, nunca infinito. Deslocamento de desenho não entra no cálculo.
Lacunas de calendário sem barra observada não criam candles nem são confundidas
com uma barra presente e inelegível.

A recorrência de primeira observação corresponde ao kernel
[`ewm(adjust=False)` do pandas](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.ewm.html).
O teste conserva também a contraprova com semente SMA da bancada. A fórmula
[RSI publicada pela MetaQuotes](https://www.mql5.com/en/code/47) não é usada
como prova de equivalência de sementes, gaps ou valores capturados do terminal.

## Implementação e compatibilidade

O registry aplica `strategies.indicators.rsi/atr` às sequências válidas; não
cria outro calculador nem usa fórmula de widget. O materializador propaga a
interrupção usando a posição da barra no histórico bruto, como já faz para
Donchian/pivôs. O hash causal inclui barras inelegíveis; sua revisão limita a
disponibilidade da cauda. A consulta continua exclusivamente leitura.

Parâmetros gravados declaram `seed=first_observation`, `adjust=false` e
`missing_policy=reset_and_reseed`. A assinatura global vigente muda ao editar
registry/materializador/indicadores: a nova versão exige materialização explícita;
versões anteriores e definições persistidas continuam consultáveis. O custo
dessa invalidação ampla permanece em BD13; não se promete invalidação seletiva.
Normalizadores de outras famílias que usam ATR (Keltner, MACD/ATR e distância
EMA/ATR) não recebem esta qualificação e conservam o caminho anterior.

## Evidências e limites

`tests/test_bd08b_wilder.py` usa recorrência escalar independente, sem EWM nem
funções do produto no oráculo. Exercita tendências, lateralidade, constância,
escalas, NaN/infinito, OHLC inconsistente, ausência de coluna, prefixo futuro,+lotes, cancelamento/retomada, revisão tardia, barra inelegível e consulta pública.
A tolerância `1e-11` cobre a ordem diferente das operações em float64; estados,
revisões e prefixos preservados exigem igualdade exata. Não aproxima hashes.

São 450 M1 sintéticas por percurso principal, sem download nem histórico DEMO.
Serviço, SDK e CLI conservam identidade, versão, fotografia e `knowledge_horizon`;
paginação após revisão conserva o cursor anterior. O corte coberto é preservado;
nova consulta acima da fronteira permanece limitada e pode avançar depois.

`tests/test_bd08b_wilder_pg.py` repete a cadeia e a invalidação em schema isolado.
Windows físico e comparação MT5 não foram exercitados. Banco principal não é
entrada dos testes; composição/instalação têm seu dono BD21. Resultado final das
suítes e recursos será registrado na continuidade e no recibo deste recorte.
