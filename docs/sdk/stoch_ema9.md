# S10 — Estocástico lento + EMA9, T19.38 Python

## Estado e fonte

`stoch_ema9` acrescenta cálculo Python real ao registry existente e usa o mesmo
`Strategy`, `ExitRules`, `BaseAdapter`, sizing, backtester e exportação de trades.
Não cria executor nem gestor por estratégia. Implementação nesta frente sobre
`3addcad548b3537b353bf8991027784558531dc0`; integração e consumidores nativos/UI
pertencem à frente de integração e continuam com aceites próprios.

Fonte funcional: item10 de
`projeto-indicadores-mt5/ESTUDOS/estrategias_trading_algoritmico_especificacoes_tecnicas_md.md`
(SHA256 `8228e6f406d5fc9bc16fc7298bd457941a7227c02b5c0e7eeb946f1a65351702`).
A variante matemática declarada e suas correções numéricas vêm do HADOUKEN
`b40e5bef`, integrado por `1e09f7f5541598de584d8c126efd7f0d50e74b00`:
`Include/Framework/HADOUKEN/Strategies/HDK_StochEma9Reference.mqh` e
`Documentation/HADOUKEN_2/propostas/T19_38_ESTOCASTICO_EMA9.md`.
O item original não fixa todas as convenções abaixo. Os testes certificam esta
definição local; não certificam equivalência com Profit, `iStochastic` ou fills MT5.

## Parâmetros tipados

| Nome Python | Padrão | Tipo / unidade | Limites |
|---|---:|---|---|
| `k_period` | 14 | inteiro / barras | 2–1000 |
| `slowing` | 3 | inteiro / barras | 1–100 |
| `d_period` | 3 | inteiro / barras | 1–100 |
| `ema_period` | 9 | inteiro / barras | 1–1000 |
| `upper_level` | 75.0 | número / índice 0–100 | `lower_level < upper_level <= 100` |
| `lower_level` | 25.0 | número / índice 0–100 | `0 <= lower_level < upper_level` |
| `stop_offset_points` | 50 | inteiro / pontos de cotação | 0–2147483647 |
| `target_points` | 0 | inteiro / pontos de cotação | 0 desliga; até 2147483647 |

Booleanos não são números de período/pontos. Parâmetros desconhecidos, frações de
período/pontos e níveis não finitos são recusados. O registry da composição declara
os mesmos tipos/unidades; a estratégia também valida a relação entre os níveis.
Os defaults dos seis robôs anteriores e S09 são preservados; S10 é acrescentado ao
fim do registry. Escolhas de mercado/TF não são qualificação de adequação.

## Cálculo e disponibilidade

Para cada barra fechada `j`, `LL_j` e `HH_j` são o mínimo/máximo das últimas
`k_period` barras, incluindo `j`. Na barra `i`:

```text
K_i = 100 × soma(C_j − LL_j) / soma(HH_j − LL_j)
      j = i − slowing + 1 ... i
D_i = média simples dos últimos d_period valores K
EMA_0 = C_0
EMA_i = alpha × C_i + (1 − alpha) × EMA_(i−1), alpha = 2/(ema_period+1)
```

K é **razão de somas**, sem substituir por média dos K brutos nem limitar
artificialmente o resultado. Denominador zero/não positivo, observação inválida
em qualquer janela H/L/C ou agregação não finita deixam a janela indefinida.
`EMPTY_VALUE` MQL5 (`double` máximo) é ausência, embora seja um número finito.
K/D podem recuperar depois que a observação inválida sai de todas as suas janelas.

A EMA conserva a âncora no primeiro fechamento do frame Python. OHLC não finito
ou `EMPTY_VALUE` invalida o restante desse prefixo, sem preencher, reiniciar ou
publicar uma EMA aparentemente válida. A verificação de `open` é uma recusa
conservadora adicional desta simulação; o cálculo K usa H/L/C. Uma fonte corrigida
ou outra janela de aquecimento exige novo frame e nova identidade de execução.

Primeiro K no índice `k_period + slowing − 2`, primeiro D em
`k_period + slowing + d_period − 3`. Um cruzamento precisa de D atual e anterior:
`k_period + slowing + d_period − 1` barras. O gate da estratégia usa o maior desse
total e 300, sem barra extra. Não exige uma SMA inicial da EMA.

Compra: antes `K <= D`, agora `K > D`, `K > upper_level` e `close > EMA`.
Venda: antes `K >= D`, agora `K < D`, `K < lower_level` e `close < EMA`.
Igualdade atual com D, nível ou EMA não entra; igualdade anterior permite cruzar.
Sinal fechado executa na abertura seguinte pelo motor existente. Não acrescenta
saída por sinal oposto, reversão, parcial, BE ou trailing ATR aos defaults S10.

`SIGNAL_COLUMNS` permanece intacto. Após o gate, `stop_long` e `stop_short`
publicam a EMA em **preço**, também nas barras sem nova entrada. Os targets nas
colunas continuam NaN: falta `SymbolSpec.point` ao produtor de sinais.

### Janela fixa Python e janela nativa

Python calcula a partir do início do frame recebido e acrescentar futuro não
altera o passado. O motor nativo considerado carrega até 1500 barras fechadas e
recalcula a EMA a partir do começo dessa janela, que desliza. Essas sementes não
são universalmente equivalentes, sobretudo com EMA longa ou histórico curto.
Nenhuma equivalência financeira, externa ou de reinício é inferida dos testes
Decimal. Uma comparação futura precisa fixar a mesma janela e semente, dados,
especificações e execução; não basta comparar o nome S10.

## Regras compartilhadas de saída

`ExitRules` recebe somente campos aditivos:

| Campo | Padrão legado | Uso |
|---|---|---|
| `signal_stop_offset_points` | `0.0` | offset não negativo e finito em pontos |
| `move_guard_from_signal_stop` | `False` | booleano explícito; liga avanço pelos níveis do sinal |
| `signal_target_points` | `None` | TP padrão positivo e finito a partir do fechamento do sinal |

S10 liga o guard, copia seu offset e traduz `target_points=0` para `None`.
O backtester converte pontos com o `point` **conhecido** do símbolo. Não fabrica
`0.00001`/pip FX e recusa point/tick inválido mesmo com guard ligado e offset zero.

Sem override fixo de proteção, SL inicial = EMA menos offset em compra / mais
offset em venda. O nível absoluto é normalizado no tick mais próximo, com empate
positivo para cima, **antes** de medir distância de risco e calcular lote.
O offset pode não ser múltiplo de tick, pois a EMA também pode estar entre ticks.
O TP S10 é `close_sinal ± target_points × point`, normalizado no tick. Gap que
atravessa SL/TP ou torna o target inválido para o preço executado recusa a entrada
inteira e incrementa `skipped_gap`; não abre posição sem essa proteção/target.

No fechamento, depois dos toques intrabar, parcial/BE e trailing ATR existentes,
o mesmo bloco de gestão compartilhada considera o guard EMA ± offset. Só aceita
nível finito/positivo, diferente do sentinela, no lado válido do BID de fechamento
da compra ou ASK da venda, e estritamente mais protetor que o SL corrente.
Recusa/igualdade/afastamento não mudam SL, TP, volume nem criam ordens. A decisão
só afeta barras seguintes: não usa um low/high já ocorrido para acionar o novo SL.
Um stop movido usa o motivo existente `stop móvel`; `Trade.stop_price` conserva o
SL **inicial**, e `target_price` o TP. Não há recibo novo de modificação do broker.

Overrides existentes continuam explícitos: `ExitRules.target_points` tem a
âncora no fill e `target_r` usa risco inicial; ambos substituem o padrão
`signal_target_points`. A composição atual pode aplicar esses overrides sem perder
seus contratos. `stop_points` substitui a proteção inicial no fill, enquanto o
guard S10 continua usando a EMA e seu offset depois. Não afastará um SL de ATR/BE
mais apertado. Com as novas opções em seus defaults, o caminho legado preserva
suas regras anteriores e não passa a exigir especificação conhecida do símbolo.

`SymbolSpec` não contém stops level/freeze level, nem retorno de `ModifySL`.
Recusas locais de preço/monotonia são propriedades do modelo Python, não simulação
de aceite/rejeição do servidor. Spread, slippage, comissão, swap e sizing seguem o
backtester existente; não qualificam causalidade de ticks ou equivalência econômica
com a corretora. A aceitação operacional nativa continua separada.

## Composição, persistência e repasse

`get_strategy("stoch_ema9", ...)` e `base_composition("stoch_ema9", ...)` funcionam
no backend Python existente; roundtrip JSON, unidades e overrides são testados.
As novas regras serializadas são aditivas; regras antigas sem esses campos mantêm
defaults. Identidades calculadas com fontes/regras novas devem ser distintas.
O cache S10 permanece **inelegível** no serviço atual de `run_python`: o gate de
identidade recusa estado de instância adicional (`min_bars` calculado). Este
recorte não altera esse gate nem permite reutilizar cache por semelhança histórica.
Reutilização futura exige identidade verificável de dados, especificações,
parâmetros, janela/semente e código/execução, com invalidação apropriada.

Não altera UI, presets, `composition/native.py`, pins, `HDKSignal`, headers,
consumidores nativos, Safety nem executáveis. Portanto não exige distribuição
Safety para o cálculo Python e não autoriza nova versão nativa ou live. O dono da
integração fará os mapeamentos/pins/recibos/UI em sua frente. `parity_with` do Expert
não é habilitado aqui. Defaults só de execução Python não concedem capacidade nativa.
O PLANO/continuidade centrais ficam sob posse da integração; este documento e a
[evidência S10](../evidencias/T19_38_S10_PYTHON_20261021.json) registram o recorte.

## Verificação e limites

Antes de alterar: 142 testes focados existentes aprovados. Novos testes usam um
oráculo Decimal independente, dados determinísticos, prefixos/nondefault index,
razão de somas diferente de SMA(K bruto), D, semente EMA, gate300/305, igualdades,
faixa plana, NaN/±Inf/EMPTY_VALUE nos quatro campos, recuperação K/D e overflow.
Exercícios reais do backtester cobrem os dois sentidos, offset/point/tick/lote,
guard zero-offset/on/off, recusa/igualdade/afastamento, BID/ASK, não retroatividade,
ATR, parcial/BE, TP preservado e no fechamento do sinal, gaps e overrides da
composição. Resultado final: **240 testes focados aprovados**, incluindo os96 novos
casos S10/guard e os casos existentes; **171 regressões ampliadas aprovadas**
(validação/walk-forward, CLI, screener, robôs, geração, equity, parcial, lote,
políticas temporais, sessão e cooldown). A última recusa de SL não positivo só
afeta o caminho opt-in, coberto pela seleção final; o caminho legado exercitado
pela seleção ampliada não foi alterado por esse ajuste. Não são testes de regex
nem repetição do QA visual. Comandos e identidades constam da evidência vinculada.

Ambiente virtual próprio com dependências de desenvolvimento já preparadas e
verificadas via pip, sem modificar o venv compartilhado. Instalador global não foi
reexecutado para preservar atalhos/configuração/recursos da integração. Doctor
`--no-probe` usa a configuração existente **só para leitura de arquivos/stat**:
20 OK, 1 aviso (`pandas-ta` opcional ausente), 0 falhas. Não conecta ponte/MT5 nem
abre o banco. Nenhum Tester/MetaEditor, lease ou processo nativo nesta frente.
Sem suíte integral/workflow duplicado. Windows físico, paridade e aceite
operacional/estatístico permanecem pendentes; não há promoção ao vivo.
