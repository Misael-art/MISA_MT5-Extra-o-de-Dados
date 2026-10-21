# Madrid — T19.39 Python, variante fechada

## Recorte e fonte

`madrid` usa `Strategy`, `ExitRules`, `BaseAdapter` e o Backtester existentes.
Não cria gestor, executor, FC/FV persistente ou Move Guard paralelo. Pai exato
da frente: `d10e2b0b89d5bcd4f60551355cde7e300c2c83ed`; composição de corretivos
da integração por Git fica registrada na evidência, preservando esse ancestral.

Fonte funcional: item5 de
`projeto-indicadores-mt5/ESTUDOS/estrategias_trading_algoritmico_especificacoes_tecnicas_md.md`.
Fundação nativa lida: `94e3de07f55dc929f010db3ee9e558f0dd0687c5`, especialmente
`HDK_MadridReference.mqh`, wrapper `HDK_Madrid.mq5` e `T19_39_MADRID.md`.
O corretivo1.10 `f7357c61adc18023b116399486c457c2bbe04909` foi revisado
estaticamente, com limites de variante/prefixo/números especiais; sua integração
H `1305bc9635bf44b9e093081af09139a7e813beaf` não é aceite operacional Python.
O corretivo nativo, seus pins e consumidor/UI são da integração; nenhum binário
ou distribuição Safety é necessário para este cálculo Python.

O estudo exige dez EMAs em progressão, rompimento fechado de M1, afastamento,
stop em média selecionada com offset e TP móvel. A variante escolhe progressão
aritmética5..50, escalas ATR configuráveis e **TP fixado no sinal**. Não entrega
TP móvel. M1 é a primeira média, não o timeframe de um minuto.

## Contrato tipado

| Parâmetro | Default | Tipo/unidade | Limite |
|---|---:|---|---|
| `first_period` | 5 | inteiro / barras | >=1; último período <=1000 |
| `period_step` | 5 | inteiro / barras | >=1; `first_period + 9×step <= 1000` |
| `distance_atr` | 1.0 | número / ATR14 | positivo, finito |
| `distance_points` | 0 | inteiro / pontos de cotação | 0–2147483647; 0 escolhe ATR |
| `trail_line` | 10 | inteiro / índice da média | 1–10 |
| `offset_atr` | 0.25 | número / ATR14 | não negativo, finito |

Booleanos e frações de parâmetros inteiros, sentinelas/não finitos, períodos
excessivos e parâmetros desconhecidos são recusados. A restrição conjunta dos
períodos é validada ao instanciar a estratégia, inclusive pelo adapter. Não há
conversão implícita de pontos/pips FX. O warmup é **300 fixo**, mesmo com M10=1000;
não se inventa período extra nem se promete convergência da média.

## Cálculo e sinais

`madrid_averages(frame, first_period, period_step)` devolve `m1..m10` e `atr` com
o índice original. Para cada período `p`, EMA0=C0 e
`EMA_i = alpha×C_i + (1-alpha)×EMA_(i-1)`, alpha=2/(p+1).
TR0=H0−L0; depois `TR_i=max(H_i−L_i,abs(H_i−C_(i-1)),abs(L_i−C_(i-1)))`.
ATR0=TR0 e ATR_i=(1/14)×TR_i+(1−1/14)×ATR_(i-1), sem SMA inicial.
O módulo legado de EMA mascara os primeiros `p−1` valores; isso esconderia
períodos longos na barra300. A implementação específica conserva as mesmas
sementes recursivas explícitas da variante H, sem alterar o indicador legado.

H/L/C devem ser finitos, positivos e diferentes de `EMPTY_VALUE` (`double`
máximo). Uma invalidade ou agregação recursiva não finita bloqueia **todo o
restante do prefixo**, sem reinício ou reparo silencioso. ATR zero é um estado
matemático válido em faixa plana: não permite sinal/guard até ficar positivo.
Integridade geométrica OHLC, aberturas executáveis, horários e grade continuam
requisitos do contrato de dados; o cálculo não inventa observações nem calendário.

Alinhamento é estrito: M1>M2>...>M10 para compra; espelho para venda. Igualdade
desalinha. Cruzamento compra: antes C<=M1 e agora C>M1; venda: antes C>=M1 e
agora C<M1. Distância máxima é inclusiva. Na escala ATR é calculada na própria
barra fechada: `abs(C−M1) <= distance_atr×ATR14`.
Com `first_period=1`, EMA1=C e nenhum cruzamento estrito ocorre: configuração
válida sem operações, não corrigida para produzir trades.

Esse reset implícito aceita retorno à igualdade; não equivale universalmente
aos flags FC/FV do estudo. Não há estado persistido de ciclo, trava de reentrada
após reinício ou promessa de equivalência de ciclos entre processos.

`SIGNAL_COLUMNS` permanece intacto. Após o warmup e com contexto válido,
`stop_long/short` publicam **preços** `Mtrail ∓ offset_atr×ATR14`, também sem novo
cruzamento ou leque alinhado. O offset ATR já está nesses níveis; não recebe
offset em pontos adicional. Produtos não finitos tornam os níveis daquela barra
indisponíveis. O Move Guard único do Backtester decide se o nível aperta o stop.
Não se cria saída por sinal oposto, reversão, parcial, BE ou trailing ATR extra.

## Referência em pontos, alvo e precedência

Modo ATR: `target_long/short` são preços M1±distance_atr×ATR14;
`absolute_signal_target=True` reutiliza normalização/recusa antes do lote. Um
TP padrão não positivo/não finito não significa TP desligado: o motor recusa a
entrada inteira. Os flags de sinal são candidatos; a execução ainda verifica
stop/target, símbolo, lote, gap e overrides. Um override explícito pode substituir
um alvo padrão inválido, mantendo o filtro de afastamento.

Modo pontos: as colunas `target_long/short` contêm **M1 em preço, não o TP**.
A estratégia publica candidatos pelo cruzamento/alinhamento, pois ainda não tem
`SymbolSpec.point`. O campo aditivo
`ExitRules.signal_reference_distance_points: Optional[float] = None` declara
essa semântica ao motor. Exige valor positivo e finito quando ligado; é
mutuamente exclusivo com os outros padrões de alvo do sinal.

Antes do lote, o Backtester exige point/tick conhecidos e converte D=N×point.
Confere referência e fechamento finitos/positivos e `abs(close_sinal−M1)<=D`.
TP padrão=M1±D, normalizado no tick mais próximo (empate positivo para cima),
com validação de lado no fill. D não precisa ser múltiplo do tick: o alvo final
é preço absoluto. Referência ausente, distância excedida ou bracket já
atravessado recusa toda a entrada; o contador existente é `skipped_gap`.

Overrides preservam a prioridade e a âncora:

- lucro `fixed_points`: TP a partir do **fill**;
- lucro `fixed_r`: TP a partir do **fill e risco inicial**;
- proteção `fixed_points`: troca **apenas** o SL inicial pelo nível desde o fill;
  guard continua vindo de Mtrail±offset ATR e só aperta.

No modo pontos, trocar o TP **não desliga** o filtro de distância nem permite
referência inválida. Por isso a posse foi ampliada para um sexto arquivo de
runtime: somente o `if` de limpeza de targets em `composition/adapter.py` passa
a conservar M1 quando o novo campo está ligado. Legados/S10/VWAP, com campo None,
mantêm a limpeza anterior em overrides. Nenhum nome de estratégia entra no motor
para escolher gestão; ele consome somente regras tipadas.

## Causalidade, evidência e limites

O frame Python é um prefixo fixo: acrescentar futuro não altera o passado. O
nativo recalcula uma janela de até1500 barras e suas sementes mudam ao deslizar.
Comparação futura precisa fixar exatamente OHLC/janela/semente/especificações;
não se alega paridade com Profit, indicador embutido ou resultado da corretora.
SL/TP normalizados, spread/custos e fills seguem o modelo existente, sem dados
de stops/freeze do broker ou recibo de `ModifySL` em `SymbolSpec`.

A [evidência própria](../evidencias/T19_39_MADRID_PYTHON_20261021.json) registra
baseline, testes e hashes. Os testes exercitam sementes Decimal independentes,
prefixos, limites, pontos/tick, guard monotônico, proteção contra mesmo-barra/gaps
e o `BaseAdapter.run` real com overrides válidos e referências inválidas.
Baseline222 aprovados; foco340 e seleção ampliada288 aprovados. Após os últimos
casos de sentinela/tipos e fronteiras exatas,129 casos Madrid estão cobertos na
seleção final257 aprovada (catálogo/CLI/re-simulação/composição). Três testes GUI
afetados pelo novo card também passaram em Tk/Xvfb. Seleções se sobrepõem; não
somar como casos únicos. Doctor20OK/1aviso opcional `pandas-ta`/0falhas.

O corretivo da integração `6981fd65b3492941284e3045433b2d98e9f4d7a2` foi
composto por merge `ca823defc304179ef04bbf4061a025056c170618`, preservando o pai
exato d10. Seu `ExitPolicy.from_rules` recusa genericamente Move Guard dependente
de série de sinais: re-simulação T16.6 continua indisponível também para Madrid,
sem projetar gestão incompleta. Testes provam recusa pelo engine e Runner, em
vez de excluir o cenário. A única alteração própria em fixtures desse recorte
foi acrescentar Madrid à `PYTHON_KEYS` explícita (10), mantendo legados e VWAP.
Documentos e evidências anteriores congelados qualificam seus respectivos
commits; os hashes Madrid são os desta nova evidência, sem reescrever o passado.

Não foram iniciados MT5, MetaEditor, Tester, lease, instalação ou nova campanha
nativa. Windows físico, replay/aceite operacional e aprovação estatística são
pendências; este recorte não autoriza live. UI, presets e centrais permanecem
sob posse da integração.

Este recorte não modifica o gate de cache nem concede waiver de identidade.
Madrid conserva apenas `params` na instância: o gate genérico existente pode
registrar `frozen_reference/cache_eligible=True` com snapshot/código/regras/spec/
runtime hashados. Isso não é qualificação nova do reuso Madrid, do broker ou live.
Um protocolo futuro de reuso precisa conservar dados, janela/semente, parâmetros,
especificações e execução verificáveis, com invalidação; não basta histórico
semelhante. A distribuição Safety não é alterada pelo cálculo Python.
