# Madrid — Default, Manual e conferência nativa

T19.39/S05: variante declarada em [madrid.md](madrid.md). O consumidor acrescenta
Default preservado, Manual nomeado/revisionado, seis parâmetros e orçamento até
0,5% do saldo, exportação e conferência no serviço de backtest/replay existente.
Não oferece lotes fixos, parciais, BE, filtros ou saídas temporais deste perfil.

## Fonte e comportamento

Madrid1.20, fonte HADOUKEN `6677bce1a4d08f6f92d51ad6caccd078d2e33ef5`,
integrada pelo PR47 em `87469b3d3d066b02863acdfac414b6888a8594e4`, fingerprint
`870c0f8db71c3d5a`. Versão, fingerprint, nomes/tipos/quantidade exata dos13 inputs
e closure são exigidos antes de abrir o editor e novamente na exportação.
A única dependência externa permitida é `Canvas/Canvas.mqh`, cuja biblioteca
padrão precisa existir no destino. O observador compartilhado acompanha a closure;
não requer novo release Safety. Pins S09/Pullback/S10/VWAP permanecem intactos.

São dez EMAs em progressão aritmética; M1 é a primeira média. O gatilho fechado
admite igualdade anterior e exige cruzamento atual estrito com leque alinhado.
Não implementa flags FC/FV persistentes nem impede reavaliação após restart.
Períodos5..50 e escalas ATR são escolhas da variante, sem paridade externa.
Warmup300 fixo, EMA inicial C0 e ATR14 inicial H0−L0 seguido de RMA1/14.
Cada janela nativa de até1500 barras tem sua própria semente; o frame Python
fixo não é tomado como equivalente à janela deslizante do terminal.

`distance_points=0` escolhe ATR14, mantendo TP obrigatório. Com pontos positivos,
usa N×point do símbolo. TP=M1±afastamento da barra do sinal e fica fixo;
TP móvel do estudo continua pendente. Stop e guard vêm da média selecionada
±offset ATR. O Move Guard compartilhado só tenta apertar, pelas proteções do
executor; o trace não comprova que a modificação foi aplicada.

OnInit exige Tester+DEMO antes do motor. Uma posição, saldo composto, SL obrigatório,
limites diária2%/semanal4%/DD10%, buffer20% e bloqueio de averaging permanecem.
Não existe outro gestor ou caminho de envio. Risco acima de0,5% ou override nativo
de alvo/SL é recusado; as composições Python possuem seu contrato próprio.

## Conferência do diário

O leitor estrito compartilhado exige consumo integral de cada marcador reservado,
janelas completas ou incrementais com count anterior/anterior+1 e clocks coerentes.
O consumidor usa o cálculo Python existente por cada janela observada e aplica a
sequência nativa de validação. No modo pontos, as targetcols Python são referência
M1: converte a distância pelo point registrado e calcula os TPs brutos.

Guards são comparados mesmo sem entrada ou leque alinhado. Sem sinal do wrapper,
SL/TP propostos são zero. TP escolhido inválido também zera os níveis do sinal,
preservando guards válidos já propostos; depois vem o filtro do lado do stop do
motor. Um SL inválido pode recusar a intenção conservando propostas brutas no
contexto. Não comparar essas propostas a preços já normalizados pelo executor.
Replay compara a referência de janelas além do livro/sinais/saldo.

ACCEPTED/PARTIAL/UNCERTAIN travam novas entradas nessa instância e não suspendem
gestão. PARTIAL com opened0 não significa ausência de execução. Observação final
da chamada não relata todas as retentativas; restart, reconciliação assíncrona e
concorrência não são qualificados. Diário sem contador/hash encadeado pode perder
pares inteiros em janela constante sem detecção. Manifesto/recibo fixam contexto,
magic, símbolo, TF e parâmetros. Recibo real, .set/INI carregado, DEMO, isolamento,
fills e saldo continuam exigidos. Fonte, ModifySL, stops/freeze e execução broker
não são certificados pelo recálculo. Aceite operacional e live permanecem falsos.

## Evidência e limites

Núcleo Python `111ae036` composto por Git preservando os pais d10/6981 e todos os
seis runtimes originais. Evidências do núcleo:129 Madrid finais,257 pós-composição
e três GUI afetados; seleções anteriores sobrepõem, não são somadas.
Integração:165 testes com Tk/Xvfb, incluindo consumidores anteriores, catálogo,
re-simulação, conferência de propostas, níveis inválidos e resposta truncada.
Seleção adicional95 testes cobre CLI/UI/backtest/painel e os mesmos consumidores; há sobreposição. 38 dirigidos finais cobrem a semente na rolagem1500 e o modo pontos pelo serviço.
Session simulada não é execução no terminal. Re-simulação T16.6 continua recusando
genericamente Move Guard sem o contexto necessário, inclusive Madrid.

Expert e wrapper de recibo gerado pela aplicação: MetaEditor6276/Wine0erros/0avisos;
contexto liberado, nenhum processo próprio remanescente observado e principal
preservado. Fonte real/pin e13 inputs conferidos em Default/Manual exportado/relido
numa base isolada. Compilação/exportação não demonstram inputs carregados no Tester.

Cópia consistente da base real, EURUSD/H1 setembro2026, operação desde15/09,
risco0,25%:528barras,10operações, resultado105,10, salvo/reaberto e livro conciliado.
Esse resultado não é aprovação financeira/paridade; especificação histórica é
desconhecida. O gate genérico pode marcar identidade Python como
`frozen_reference/cache_eligible=true` por hashes completos; ele foi preservado,
sem qualificar reuso operacional, evidência nativa ou live. O serviço nativo mantém
cache inelegível. Resultados negativos S10/EMA anteriores permanecem preservados.

CI final, instalação do conjunto correspondente, Tester/replay real, painel,
Windows físico e live DEMO possuem aceites próprios ainda pendentes. A instalação
do proprietário recebe apenas código revisado com checks finais verdes.
