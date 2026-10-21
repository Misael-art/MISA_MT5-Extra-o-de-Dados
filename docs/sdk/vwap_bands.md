# S01 — Bandas percentuais da VWAP, T19.36 Python

## Fonte e recorte

`vwap_bands` implementa cálculo Python e usa `Strategy`, `ExitRules`,
`BaseAdapter`, sizing, custos e Backtester existentes. Não cria executor ou gestor
por estratégia. Base exata da aplicação: `ce09fe669f0dec60d79c97c90a83640ff3c8cb6c`.
O PLANO/continuidade centrais e os consumidores nativos/UI ficam sob posse da
integração. Este documento e a [evidência](../evidencias/T19_36_VWAP_PYTHON_20261021.json)
registram somente este recorte Python.

Fonte funcional: item1 de
`projeto-indicadores-mt5/ESTUDOS/estrategias_trading_algoritmico_especificacoes_tecnicas_md.md`.
Variante corrigida: HADOUKEN `855baec99a80a8c320897dd63ab3154e37251d0e`, integrada
em `de9018d768dfc28411d66613639e8dc6d6c66c7e`, especialmente
`Include/Framework/HADOUKEN/Strategies/HDK_VwapBandsReference.mqh` e
`Documentation/HADOUKEN_2/propostas/T19_36_VWAP_BANDS.md`.
A introdução do estudo menciona desvio padrão; a regra implementada e declarada
usa **bandas percentuais**. Não se infere equivalência com outra plataforma.

A VWAP encontrada em `advanced_indicators.py` calcula `close × volume` acumulado,
sem reset diário. Essa semântica não atende a HLC3 diária; por isso este cálculo
fica no módulo específico, reaproveitando a infraestrutura de sinais/execução.

## Parâmetros e dados

| Nome | Padrão | Tipo/unidade | Limites |
|---|---:|---|---|
| `entry_band_pct` | 1.0 | número / percentual | `0 < valor <= 10` |
| `target_band_pct` | 0.5 | número / percentual | `0 <= valor < entry_band_pct` |
| `stop_offset_points` | 200 | inteiro / pontos de cotação | 1–2147483647 |
| `volume_source` | 0 | inteiro / fonte | 0 Auto, 1 ticks, 2 real |

Booleanos, números não finitos e parâmetros desconhecidos são recusados. A
composição declara os mesmos tipos/unidades. A relação entre os percentuais é
validada ao instanciar a estratégia, inclusive pelo `BaseAdapter.strategy()`;
um rascunho JSON não equivale à execução validada.

O frame requer `time`, `open`, `high`, `low`, `close` e os volumes da fonte:

| Fonte | Colunas obrigatórias de volume |
|---|---|
| Auto | `real_volume` **e** `tick_volume` |
| ticks | `tick_volume` |
| real | `real_volume` |

Não substitui coluna ausente por zero nem deduz volume de preço ou número de
barras. `required_columns` é declarado por instância conforme a fonte. `spread`
continua opcional segundo o motor existente. Integridade geométrica das barras
(por exemplo `low <= close <= high`), abertura executável, periodicidade e
completude pertencem ao contrato de qualidade dos dados, sem reparo silencioso.

`time` deve conter horários **naive**, válidos, estritamente crescentes e únicos,
no calendário do servidor **declarado pelo chamador**. O cálculo não converte UTC,
infere fuso/DST, ordena barras, preenche buracos ou presume leilão. Epoch numérico,
horários com timezone, NaT, repetição e regressão são recusados; converta a fonte
explicitamente antes de montar o frame. As barras são as observações fechadas;
a decisão correspondente só pode atuar na abertura seguinte.

## Cálculo e causalidade

Para a data corrente e a fonte selecionada, incluindo a barra fechada `i`:

```text
typical_j = (high_j + low_j + close_j) / 3
VWAP_i = soma(typical_j × volume_j) / soma(volume_j)
entrada inferior/superior = VWAP_i × (1 ∓ entry_band_pct/100)
alvo inferior/superior    = VWAP_i × (1 ∓ target_band_pct/100)
```

A soma segue a ordem cronológica em `float64`. H/L/C não finito, não positivo ou
`EMPTY_VALUE` (`double` máximo), volume selecionado negativo/não finito/sentinela,
produto/soma/resultado não finito ou resultado não positivo **envenenam aquela
data até a próxima transição**. Não preenche nem reinicia no meio da data.
Volume zero é válido: conserva a média já conhecida; antes do primeiro volume
positivo, a média permanece indefinida. H/L/C é validado também quando volume=0.

Se o primeiro registro está no meio da data, toda essa primeira data fica
indisponível. Início às 00:00 ou uma transição de data observada habilitam uma
aproximação desde a **primeira barra disponível** da nova data. Uma transição com
buraco de dados também não comprova abertura da sessão ou histórico completo.

Auto escolhe uma série inteira por **todo o prefixo conhecido**: real depois de
qualquer real finito positivo conhecido; antes disso, ticks. O marcador
`has_real` **não reinicia na troca da data**, embora os acumulados reiniciem.
Dois acumulados diários independentes permitem que, no primeiro real positivo,
o valor passe a usar todo o acumulado **real** daquela data conhecida, sem ticks
misturados. Valores anteriores publicados não são reescritos pelo futuro.
Uma invalidade de volume da série não selecionada não invalida a série usada;
ao selecionar real, uma invalidade real anterior da mesma data continua valendo.

`daily_vwap(frame, volume_source)` retorna `vwap` e a fonte efetiva 1/2 em cada
prefixo, preservando o índice. Sinais usam o fechamento da própria barra para a
VWAP: `low <= banda inferior` compra, `high >= banda superior` vende; tocar ambas
na mesma barra é neutro. Igualdade com uma única banda conta como toque.
Não usa a VWAP da barra anterior, nem confirma toque de tick dentro da barra.
Primeira barra elegível: índice299 (300 barras), sem barra extra.

Python mantém o prefixo fixo do frame; o nativo carrega uma janela deslizante de
até1500 barras e escolhe uma única série para toda a janela. Ao sair o real
positivo antigo da janela nativa, Auto pode voltar a ticks; o prefixo fixo Python
conserva real. O começo da primeira data também pode ser diferente. Comparação
futura deve reconstruir cada janela, registrar a fonte real escolhida e fixar
volumes, OHLC, relógio e especificações; não comparar apenas o nome da estratégia.

## Sinais, bracket e overrides

`SIGNAL_COLUMNS` permanece intacto. Em barra com toque único elegível, as colunas
`stop_long/short` contêm os **preços brutos das bandas de entrada** e
`target_long/short` os **preços das bandas de alvo**. Não embute um point FX no
produtor. Nas demais barras, esses níveis ficam indefinidos. Não há saída por
sinal oposto, ordem limite, parcial, BE, trailing, Move Guard ou alvo móvel nos
defaults desta estratégia. SL e TP ficam fixados a partir do sinal aceito.

Reaproveita `signal_stop_offset_points` do S10. O Backtester converte o offset com
`SymbolSpec.point` conhecido: SL = banda inferior − offset na compra / banda
superior + offset na venda. Quantiza o nível absoluto no tick mais próximo,
empate positivo para cima, antes de calcular distância de risco/lote. O offset
pode não ser múltiplo do tick. O filtro comum de sinais adia a verificação de
lado do **stop bruto** quando há offset, pois a banda sozinha pode estar além do
close e o SL final ser válido. O motor valida o SL final tanto no fechamento do
sinal quanto no BID/ASK da abertura seguinte, antes do lote.

Único campo aditivo de `ExitRules` neste recorte:
`absolute_signal_target: bool = False`. VWAP o liga; S10 e legados preservam o
default desligado. Ligado, normaliza o preço absoluto do alvo do sinal no tick e
recusa a entrada inteira se alvo inválido ou já atravessado pelo preço executado,
**antes do lote**. O mesmo vale para SL inválido/atravessado nesse caminho com
offset; as recusas incrementam `skipped_gap`. Não abre posição sem o TP exigido.
`target_band_pct=0` significa **VWAP central com TP**, nunca TP desligado.

Precedência de overrides explícitos existentes:

- `profit_exit=fixed_points`: TP a partir do **fill**, substitui banda absoluta.
- `profit_exit=fixed_r`: TP a partir do **fill e risco inicial**, substitui banda.
- `protection=fixed_points`: SL a partir do fill, **sem duplicar offset** da VWAP.

Os mesmos overrides tipados de `ExitRules` preservam essas âncoras. Uma regra não
pode declarar simultaneamente dois padrões de alvo (`absolute_signal_target`
e `signal_target_points`). Desligado o novo campo, o legado mantém inclusive sua
semântica anterior de alvos brutos e não recebe recusa geral nova de target gap.

## Evidências, integração e limites

Testes próprios usam referência Decimal que recalcula cada prefixo/dia desde
zero, sem helpers do produto, além de casos de volume, causalidade, limites,
igualdades, datas, invalidez/overflow e exercícios do Backtester/BaseAdapter reais
nos dois sentidos, tick/point/lote, gaps e overrides. Foram preservados todos os
asserts de negociação e equivalência do teste genérico de composição; somente a
fixture VWAP passou a intradiária determinística. A fixture diária800 anterior
produziu legitimamente70 sinais long/81 short,151 recusas de bracket e0 trades;
não se alterou regra de produção para forçar negócios.

Antes de alterar:181 testes focados passaram. Fechamento:110 testes novos VWAP
aprovados e240 testes de composição/regressões aprovados. A seleção focada
intermediária teve331 aprovados e a única falha da fixture genérica descrita
acima; essa comparação foi aprovada na seleção final após o ajuste de dados,
sem nova mudança funcional. Não se somam as seleções como testes únicos.
Doctor:20 OK,1 aviso (`pandas-ta` opcional ausente),0 falhas.
Comandos, contagens, hashes e doctor constam da evidência vinculada. Reuso do
ambiente preparado foi autorizado; sem reinstalar globalmente ou reescrever
atalhos/configuração. Doctor `--no-probe` usa somente arquivos/stat: não conecta
ponte, abre banco ou inicia processo nativo. Testes puros Linux não qualificam
Windows físico. Os erros iniciais de fixture/expectativa estão registrados.

O cache permanece **inelegível**: o gate existente de identidade de `run_python`
recusa o estado adicional `required_columns` da instância; este recorte não altera
o gate. Reuso futuro exige identidade verificável de dados, volumes/fonte,
calendário, janela, parâmetros, especificações e código/execução, com invalidação.
Não há paridade financeira/externa/nativa, stops/freeze de broker, prova de fills,
Tester/replay, aceitação operacional, aprovação estatística ou autorização live.
`SymbolSpec` não contém stops/freeze nem recibos do executor; o modelo de custos
e fills segue as limitações existentes do Backtester.

Não altera UI, presets, pins, `composition/native.py`, S10/consumidores do dono,
`HDKSignal`, Safety, executáveis ou instalação. **Não exige distribuição Safety**
para Python. A integração comporá esta branch por Git; consumidor nativo/UI,
registro de fonte por janela e aceites correspondentes são recortes separados.
Nenhum MT5, MetaEditor, Tester, lease ou processo nativo foi usado nesta frente.
