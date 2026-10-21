# HA/HiLo — T19.40 Python, variante declarada

## Fonte e recorte

`ha_hilo` reutiliza `Strategy`, `ExitRules`, `BaseAdapter` e o Backtester.
São três arquivos de runtime neste recorte: a estratégia nova e seus registros
em `strategies/__init__.py` e `composition/registry.py`. Motor, adapter, execução,
risco, UI, perfis nativos, presets, dados e configuração do proprietário não mudam.

Pai exato: `43234c6a9786a089e767c27862dadbfe760849da`.
Fonte funcional: item11 de
`projeto-indicadores-mt5/ESTUDOS/estrategias_trading_algoritmico_especificacoes_tecnicas_md.md`.
Referência lida: HADOUKEN `3c5fb55ec5b4a5ab06c69e2f37c64d564ff94a85`,
`HDK_HaHiLoReference.mqh` e wrapper1.10, integrada em `c48ac239`.
Isso identifica a variante, sem alegar paridade externa, Profit ou aceite nativo.

## Contrato

| Campo | Padrão | Tipo, unidade e limites |
|---|---|---|
| `hilo_period` | 10 | inteiro, barras, 2..500 |
| `hilo_offset` | 0 | inteiro, barras, somente0 |
| `stop_buffer_points` | 0 | inteiro, pontos de cotação, 0..2147483647 |

Booleanos, números fracionários, strings e valores fora dos limites são recusados.
`min_bars=300` é fixo: primeira decisão possível no índice299. Não aumenta conforme
o período: HiLo500 precisa de pelo menos501 barras e de rompimento para definir
uma linha; nem300 nem1500 barras garantem estado disponível.

Entrada de dados: `time/open/high/low/close`; volume não é necessário. As barras
precisam estar em ordem temporal crescente, com geometria OHLC válida, preços
positivos e colunas de mesmo tamanho. O recorte não repara geometria, ordena
barras, converte fuso nem verifica completude de sessão.

O helper `ha_hilo_context(df, hilo_period=10, hilo_offset=0)` retorna
`ha_open`, `ha_close`, `line` e `state`, preservando o índice:

- HAclose = (O+H+L+C)/4; HAopen inicial = (O0+C0)/2, depois metade da soma de
  HAopen e HAclose anteriores.
- Estado inicial0, linha indefinida. Na barra i>=período, close real estritamente
  acima da SMA de máximas até i−1 define alta; estritamente abaixo da SMA de
  mínimas até i−1 define baixa; entre as duas ou na igualdade conserva o estado.
- A linha usa a SMA **atual** de mínimas na alta ou de máximas na baixa.
  Atualiza mesmo sem mudança de estado e mesmo na igualdade. O indicador gráfico
  `HDK_HiloActivator.mq5` usa inicialização/igualdade diferentes; não é substituído.
- NaN, ±Inf, `EMPTY_VALUE` (maior double), preço zero/negativo ou overflow de uma
  agregação tornam indefinido o restante do contexto, sem re-seed. O helper
  conserva a parte anterior válida e recusa conservadoramente o contexto conjunto
  quando HA ou HiLo falham. O ATR comum também fica indisponível nesse sufixo.

Python usa o prefixo fixo entregue. O nativo recalcula uma janela móvel de até1500
barras, podendo perder a virada que definiu o HiLo e recomeçar indefinido. A semente
HA também muda ao deslocar a janela. O peso de uma diferença inicial cai por2 a
cada atualização; não há limite absoluto de erro independente da origem nem seed
global/persistida. Não se afirma equivalência entre essas janelas.

## Sinal, proteção e execução existentes

Compra: HAclose>line e HAclose>HAopen. Venda: ambas as desigualdades invertidas.
É condição de nível, não cruzamento: pode repetir após um SL. Neutro não pede
fechamento. Compra também pede `exit_short`, venda pede `exit_long`.
Os stops das duas colunas são a linha **bruta**, apenas onde existe sinal raw.
Não há alvo padrão. `SIGNAL_COLUMNS` permanece intacto.

`ExitRules.signal_stop_offset_points=stop_buffer_points`, Move Guard desligado,
sem trailing, parcial, BE ou saída temporal padrão. Não se cria campo, executor ou
gestor. O SL da entrada permanece fixo; a atualização posterior do HiLo não o move.

Com buffer>0, o motor exige `SymbolSpec` conhecido, converte N×point, normaliza ao
tick e valida a proteção contra close real do sinal e preço de abertura antes do
lote. Não se presume point FX, nem que point=tick. Com buffer0/MoveGuard desligado
segue o caminho existente de nível bruto, sem acrescentar normalização nova.
Distância mínima, freeze, margem real, slippage efetivo e aceite do broker não são
demonstrados pelo backtest.

HAclose não é fill. O fechamento pendente ocorre na abertura seguinte, antes da
tentativa de entrada oposta. Se o SL ou lote não permitir a nova entrada, a saída
continua válida e o modelo pode ficar sem posição. A validação da entrada mantém
`exit_*`: HAclose e close real podem estar em lados distintos da linha. Não há
ReversePosition atômico, promessa de fechamento total no servidor ou garantia de
sempre posicionado. O modelo OHLC conserva as prioridades e os custos existentes.

Overrides Python explícitos continuam com a prioridade existente:

- `profit_exit.fixed_points`: alvo desde o fill, N×point; `fixed_r`: fill±R×risco
  inicial. Substituem somente o TP padrão, que aqui é ausente, sem apagar exits.
- `protection.fixed_points`: SL desde o fill, aplicado antes do lote; substitui
  a linha/folga inicial sem duplicar o offset. Pode tornar operável uma condição
  HA cujo stop bruto estava do lado errado; mantém os pedidos de saída.

Isso não libera overrides no nativo nem modifica os seis legados, S09, S10, VWAP
ou Madrid. O contrato tipado não concede capacidades nativas por sidecar.

## Re-simulação, evidências e limites

O motor de re-simulação existente pode usar o SL inicial já gravado e preservar
as saídas originais por sinal (`keep_original_exits=True`), sem contexto de Move
Guard porque ele está desligado. O teste próprio confere esse caminho com buffer
não zero; a regressão geral também inclui a nova chave sem editar engine ou seu
teste. Essa projeção **não recalcula sinais nem gera novas entradas inversas**;
alterar a proteção/saída do livro não reconstrói a sequência de decisões do robô.

Testes próprios usam Decimal independente, prefixos sem futuro, sementes,
inicialização/igualdade, dados inválidos/overflow, limite300/501, ambos os lados,
offset point≠tick, recusa antes do lote, SL fixo e overrides pelo adapter real.
Não usam MT5, contas ou banco do proprietário. A evidência JSON registra comandos,
resultados e hashes. O SDK/PLANO/continuidade centrais ficam para composição serial.

Cache continua decidido pelo gate genérico existente: identidade completa e
verificável de código/dados/regras/spec/runtime pode resultar em elegibilidade.
Não há waiver ou estado artificial para esta estratégia, nem qualificação de
reuso operacional, paridade nativa, Windows físico ou execução ao vivo.
