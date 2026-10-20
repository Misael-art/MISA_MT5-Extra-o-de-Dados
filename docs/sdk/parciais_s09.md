# T19.57 inicial — parciais e saldo

Em preparação, dependente de T19.56/H24 e M149. S09 1.20 exclusivamente Tester DEMO,
com perfis anteriores1.00/1.10 preservados. UI/Manual: agenda desabilitada, simples
ou duas etapas; gatilhos em R inicial, uma unidade comum em lotes/fração por agenda,
base inicial/saldo por etapa e BE após confirmação. Misturar unidades na mesma agenda,
grids, adições, reversões e trailing não estão habilitados. Pullback segue bloqueado.

Normalizar redução para baixo, preservar mínimo no saldo e validar agenda antes de
abrir. Nenhum envio/retorno bool equivale a confirmação. Intenção persistida antes do
envio; ownership/volume/histórico/SL devem confirmar a mesma redução. Pendente/fill
parcial/incerto congela novas tentativas inclusive após restart. BE somente após
confirmação, sem afrouxar SL; falha de alteração mantém proteção anterior observável.
O R usa âncora inicial persistida, sem recalcular a partir do SL movido.

O defeito legado Python/H foi reproduzido com meia unidade de lote indivisível nos
dois sentidos e corrigido: sem redução válida não há confirmação nem BE. A identidade
de execução existente já inclui o código do backtester, invalidando evidências de
cache incompatíveis; não qualifica dados ausentes nem certifica histórico da corretora.

Eventos S09_PARTIAL distinguem pedido/confirmado/volumes/SL/BE/preço/custos. S09_DEAL
observa fills próprios ao término. O serviço existente soma volumes/custos por posição,
confere o CSV/livro/saldo e compara os eventos sem usar tickets transitórios como
identidade entre Testers. Continua uma operação por posição, sem inflar acerto/PF
contando reduções como operações. Abas próprias mostram etapas e custos por fill.

Referência Decimal independente e testes de recusa/ausência/duplicação/ownership,
fill parcial/custos/saldo/BE. Compilação final e aceite UI/backtest/replay ainda devem
concluir; ensaio sem parcial confirmada não qualifica essa capacidade. Cache nativo
inelegível, S09 somente Tester. Ao vivo DEMO e Windows físico com aceites específicos.
