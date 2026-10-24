# BD08b — MACD canônico (T19.3), parcial

Base: PR206/`6e7b30e`, código anterior `f6d8a81`. Continuação após o
[aceite delimitado RSI/ATR](BD08B_WILDER.md). Uma tarefa canônica: quatro
saídas existentes `momentum.macd_line_12_26_9`, `momentum.macd_signal_12_26_9`,
`momentum.macd_histogram_12_26_9` e `momentum.macd_histogram_atr_14`.
Não acrescenta indicadores nem conclui BD08b, E12, M4 ou o pilar.

## Contrato e problema reproduzido

Na base, ausência de fechamento permitia anunciar MACD disponível na barra
seguinte ainda com o estado anterior. OHLC inconsistente também podia produzir
histograma/ATR disponível. A remoção de barra conhecida inelegível no
materializador comprimira a sequência. Contraprovas em fixture foram guardadas
antes da alteração:10 falhas no recorte de ausência, reinício e metadados.

| Saída | Variante e entrada | Primeiro valor por sequência | Unidade |
|---|---|---|---|
| Linha | EMA12(close) − EMA26(close) | 26ª barra de fechamento finito | preço |
| Sinal | EMA9 da linha visível, semente na primeira linha (26ª barra) | 34ª barra | preço |
| Histograma | linha − sinal | 34ª barra | preço |
| Histograma/ATR14 | histograma / ATR14 estritamente positivo, mesma barra | 34ª barra e dependências maduras | unidades de ATR |

EMAs usam alpha=2/(n+1), adjust=false, semente na primeira observação; o sinal
é **EMA**, não SMA. A semente SMA e o sinal SMA têm contraprovas separadas.
Recorrência de EMA: s←s+alpha×(x−s). A operação vetorizada preserva o kernel
existente, cuja definição está na [documentação pandas](https://pandas.pydata.org/docs/reference/api/pandas.Series.ewm.html).
Não afirma equivalência MT5, pandas-ta, calculadores privados ou legado.

Ausência/não finito de fechamento interrompe as EMAs; a próxima sequência
válida reinicia o aquecimento26/34. O normalizador reusa ATR1.1 qualificado:
primeira TR=high−low, RMA alpha1/14, semente na primeira TR,14 barras; OHLC deve
ser finito e low≤close≤high. Suas sequências são independentes: OHLC inválido
com fechamento válido reinicia ATR, mas mantém MACD de fechamento; a razão
aguarda os14 novos OHLC válidos e a maturidade do histograma. Fechamento
inválido e barra conhecida inelegível reiniciam ambas as dependências.
Lacuna de calendário sem observação não cria barra nem reinício.

Falta de coluna é SOURCE_UNAVAILABLE; entrada inválida é SOURCE_VALUE_MISSING;
aquecimento incompleto é WARMUP. ATR zero/não positivo não vira divisão válida:
INVALID_DENOMINATOR. MACD zero é valor válido, inclusive com fechamento zero ou
negativo. Resultado aritmético não finito é INVALID_RESULT. Deslocamento de
desenho não entra no valor; disponibilidade segue a barra de cálculo fechada
e revisões recebidas, nunca o instante de uma barra futura.

## Implementação e versões

O registry usa o kernel `macd` já existente em segmentos finitos. A razão usa
a mesma avaliação ATR1.1 de RSI/ATR. Não altera o kernel legado, a dependência
opcional pandas-ta nem as regras de Keltner/distância EMA/ATR. O materializador
propaga a interrupção de barras inelegíveis e seus hashes/instantes causais;
consulta continua somente leitura, sem cálculo ou migração implícitos.

As quatro definições MACD passam de1.0 para1.1; materialização usa1.1+hash.
O registro direto idempotente conserva definições, parâmetros, unidades e fatos
1.0. Dados anteriores não são substituídos. O hash global ainda versiona todos
os IDs materializados quando muda módulo compartilhado: invalidação seletiva
permanece BD13. Reuso local de dependências não implementa o grafo BD12.

## Provas e precisão

`tests/test_bd08b_macd.py::_expected` usa recorrência decimal de60 dígitos a
partir da representação float exata das entradas; não chama EWM nem produtor.
Compara seis massas240 barras (tendência, lateralidade, constante, passeio e
escalas), ausências, OHLC inválido, colunas faltantes, reinícios, índice temporal,
prefixo sem futuro, sementes alternativas e zero/denominador inválido.

Antes da comparação foi definido orçamento absoluto64×epsilon_float64×máximo
|close| da fixture; para a razão, esse orçamento é dividido pelo ATR esperado.
Tolerância relativa adicional1e-12. O orçamento considera arredondamento e
cancelamento das EMAs, sem prometer precisão relativa uniforme quando o
histograma tende a zero. Estados, ausências, revisões, hashes e prefixos
preservados são comparados exatamente; tolerância numérica não aproxima hashes.

SQLite e PostgreSQL exercitam append, revisão tardia, lotes, invalidação de
barra inelegível e migração. Cancelamento/retomada conserva lotes e revisões.
Serviço/SDK/CLI são conciliados com identidade, versão, fotografia e horizonte:
mesmo cursor e corte coberto estáveis; novo pedido acima da fronteira pode
avançar, sempre com aviso. `known_at` continua marca lógica pós-commit dos dados,
sem medição exata da visibilidade física do commit do carimbo.

Recursos medem o processo de testes e os temporários ao final; não são pico de
disco ou custo isolado do materializador. Massas compactas, sem download DEMO,
MT5 ou banco principal. Windows físico e paridade nativa pendentes. Suíte geral,
CI, medidas e tentativas são vinculadas ao SHA do código no recibo final.

## Continuidade

M2 permanece primeiro percurso demonstrado/parcial, E31 planejado e E32.1 CLI
sem exportação. As três falhas gerais da entrega anterior mantêm diagnóstico
próprio, sem correção alheia nesta família. Demais famílias/produtores BD08b
continuam pendentes. BD12→BD13/14 segue o grafo; BD18b ainda precisa limitar a
leitura na origem:200 mil fatos pós-leitura não garantem memória. Composição e
ativação são BD21; este recorte não altera canais do host nem integra a pilha.


## Evidências da ponta b1f0328

27 testes da família aprovados em22,46s;167 dirigidos aprovados em224,45s.
Doctor:16 OK,2 avisos,0 falhas (pandas-ta opcional ausente e ponte desativada
somente no perfil próprio). Sem alteração de scripts: shellcheck local não se
aplica; o lint dos instaladores permanece na CI.

[Recibo com campos nomeados](recibos/recibo_bd08b_macd_20261024.json) e
[precisão por massa](recibos/precisao_bd08b_macd_b1f0328.json). Medição do processo
inteiro da família:164064 KiB de pico RSS,4513990 bytes temporários ao final,
23,57s de parede. Não é custo isolado do produtor nem pico de disco.
As32 comparações cenário×saída da sonda numérica são separadas dos27 testes.

| Saída | Maior erro absoluto observado | Maior erro relativo sobre todo esperado não zero |
|---|---:|---:|
| Linha, preço | 4,58e-11 | 1,16e-10 |
| Sinal, preço | 3,45e-11 | 6,43e-11 |
| Histograma, preço | 2,41e-11 | 5,42e-6 |
| Histograma/ATR | 4,53e-13 | 5,42e-6 |

O erro relativo do histograma próximo de zero reforça o limite declarado;
não se esconde uma faixa de magnitude nem se afirma precisão relativa uniforme.
Os valores são observações das oito fixtures, não um limite universal.

A primeira regressão preservada teve165 aprovados e2 falhas: a palavra
independentes na descrição acionava o detector textual de pendências e um
novo motivo de aquecimento quebrava compatibilidade. A descrição foi ajustada
e REQUIRES_34_BARS conservado sem afrouxar sua regressão. Uma primeira seleção
PG teve erro de instrumentação por dois nomes de arquivos; nenhum teste rodou.
A seleção corrigida passou5 testes em35,25s antes do ajuste final de textos;
seu resultado não é atribuído ao SHA final. Contêiner próprio removido.

[PR209 em rascunho](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/209),
sobre PR206/6e7b30e. Os seis checks técnicos passaram no próprio b1f0328; logs conferidos:
PostgreSQL73 aprovados,0 pulados,0 falhas,0 avisos em108,06s; Ubuntu2937 aprovados,
259 pulados,0 falhas,0 avisos em1603,93s; Windows3091 aprovados,104 pulados,
0 falhas,5 avisos em3403,74s, além da etapa separada de contexto3 aprovados,
0 pulados,0 falhas,0 avisos em43,97s. Lint e ambos os instaladores aprovados.
Esses checks são do código b1f0328; o commit documental posterior usa
[skip ci] e não tem execução própria. Windows físico continua pendente.

Baseline integral em6e7b30e:3100 aprovados,66 pulados,0 falhas,2 avisos de layout
em4340,17s, em uma execução. Os três casos antigos passaram nessa baseline,
sem reclassificar sua tentativa original nem provar causa comum.

Geral final local b1f0328: encerramento143 em cerca de97%, sem resumo;3104
marcadores observados (3035 aprovados,69 pulados,0 falhas observadas; número de
avisos indisponível). O processo não estava vivo na inspeção seguinte; causa
não estabelecida. Os92 casos restantes passaram em processo novo, com0 pulados,
0 falhas e0 avisos em31,00s, mantendo os limites originais. Todos os3196 casos
tiveram execução observada entre processos; isso **não é aprovação integral
local em execução única**. A tentativa interrompida permanece preservada.
O encerramento exige diagnóstico próprio de ciclo de processos/recursos,
sem correção alheia nesta família nem atribuição automática ao host.

A fotografia final encontrou Appfd6062885e30/T20.22-LAB1 em desenvolvimento,
Hb730ed4a8df7 e Safetyc5a3e2162cac. O App e os bytes dos atalhos avançaram
em relação ao início, preservando app-development/run.sh. Os preparos desta
frente haviam preservado os bytes reais imediatamente após cada instalação;
não se alega igualdade durante toda a execução. Estado atual conservado;
nenhuma ativação/merge desta frente. Raiz/WIP e stat do banco principal iguais.



Para reproduzir a comparação decimal e seu orçamento na raiz instalada:

```bash
rtk proxy .venv/bin/python -m pytest -q tests/test_bd08b_macd.py
```

Para inspecionar o erro sem corte de magnitude (exemplo com uma massa):

```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path("tests").resolve()))
from decimal import Decimal
from tests.test_bd08b_macd import IDS, _expected
from mt5_extracao.analytics import indicator_registry
import bench
frame = bench.scenarios()["trend"]
expected, _ = _expected(frame)
for row in indicator_registry.iter_core(frame, feature_ids=IDS):
    want = expected[row["feature_id"]][row["bar_index"]]
    if want is not None:
        error = abs(Decimal.from_float(row["value"]) - want)
        print(row["feature_id"], error, error / abs(want) if want else None)
```
