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
fill parcial/custos/saldo/BE. Prova consumidora final abaixo; ensaio sem parcial confirmada não qualifica essa capacidade. Cache nativo
inelegível, S09 somente Tester. Ao vivo DEMO e Windows físico com aceites específicos.

## Prova consumidora final, 2026-10-20

Fonte H89139c8134fca00461ada8850257a0dbd8d7763d, fingerprint60bf18181739612d,
Safety1.7 e aplicação8a08e4c8156b989ed3288eef037b54cf6841172e identificadas.
UI real: Default somente leitura, Manual nomeado, duas etapas em lotes/R,
reabertura/exportação, recusa de etapa que zeraria o saldo, consulta após reabrir.
Backtest e replay visual finais: duas posições, três reduções confirmadas,
sete fills e22inputs em cada; CSV/livro/custos/saldo e eventos idênticos.
Um negócio percorreu3→2→1→0; no outro, BE encerrou o saldo antes da segunda etapa.
BE só aparece confirmado após redução e SL efetivamente observado. Os custos
observados nesse símbolo/recorte são zero; testes independentes incluem custos não
nulos. Não se converte essa observação local em custo universal ou meta financeira.

Ensaio QA distinto reinitializou o controlador dentro do Tester depois de reduzir
3→2, preservou marca confirmada sem reenviar, injetou intenção pendente e reabriu:
volume2/saídas1/SL mantidos. Também recusou NaN/zero/negativo antes do executor.
Não é prova de reinício do processo MT5/Windows físico. Patch do Expert QA e hashes
ficam junto do registro para revisão; não é estratégia instalada nem aceite Manual.

Compilação final instrumentada:0erros/avisos; os seis consumidores legados alterados
pelo include comum também compilaram isoladamente. PIDs próprios observados foram
encerrados; ponte e terminal principal preservaram PID/criação e conexão DEMO.
A revisão negativa reproduziu duas falhas no leitor (redução sem eventos e BE
sem ack), corrigiu e reaplicou o leitor aos artefatos, sem novo Tester por essa
mudança. A guarda nativa não finita mudou binário/fingerprint e por isso exigiu
novo ensaio dirigido da fonte final. Campanha histórica aceita não foi repetida.

Verificações locais:162focados na preparação,14 do leitor estrito e39 do vínculo
final;21Tools/115arquivos check_repo. Doctor20OK/1opcionalpandas-ta/0falhas com
--root explícito para a configuração preservada; a primeira invocação apontava
para a árvore privada sem config e foi corrigida sem gerar ou alterar configuração.
CI existente M7e1b cobre lógica Python inalterada; somente literais de revisão/
fingerprint mudaram para8a08, cobertos pelos39 e prova final. Checks nativos H891
aprovados. Aguardar checks obrigatórios M antes de integrar; uma tarefa por PR.

Registro T19_57_PARCIAIS_20261020.json. Cache inelegível; S09 somente Tester.
Pullback permanece bloqueado no adaptador até seu aceite específico. Ao vivo DEMO,
restart de processo e Windows físico mantêm suas coberturas próprias.
