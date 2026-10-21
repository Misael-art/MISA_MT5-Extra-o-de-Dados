# Consulta do registro de estudos na aplicação

Recorte complementar T20.12 registrado antes do código, sobre174/c0e0f9c.
O registro inicial e a avaliação composta existem; seus protocolos/pedidos e
exposição só são consultáveis pela CLI. Esta entrega acrescenta Exibir → Registro
de estudos, lendo o MESMO store canônico, sem novo banco/journal/cache ou motor.

Escopo: listagem de nomes/revisões/IDs; status de um protocolo, pedidos/recusas,
origem/finalidade, execuções vinculadas, eventos e exposição compartilhada da
reserva. O status existente não entrega métricas financeiras; consulta de metadados
não registra falso acesso a resultados. A UI não cria protocolo/pedido nem calcula
avaliação ou aprova reserva. Snapshot, critérios, orçamento por pedido e flags de
qualificação vêm do store; não há matemática financeira em widgets.

Aceite: base antiga sem tabelas abre vazia sem migração de escrita; journal e run
não mudam por consulta; erro tem ação Atualizar; callback antigo/fechamento não
substitui seleção recente; nomes/IDs e recusas persistidos são visíveis, incluindo
usado para ajuste através de outro protocolo. Interface rolável e por teclado.
Windows físico, demais consumidores e T20.13/27/28 permanecem pendentes.


Abertura: menu **Exibir → Registro de estudos...**. Escolha o protocolo pelo nome
/revisão; guias Pedidos, Dados e reserva, Protocolo e Eventos mostram os registros
persistidos. Atualizar preserva a seleção quando ela ainda existe. Os textos são
selecionáveis, roláveis e acompanham o tema; resultados reutilizados têm estado
próprio e não representam nova execução independente. Mostramos os últimos100
eventos do protocolo, com contagem explícita; o histórico canônico completo permanece.
Criação/avaliação continua pelo serviço/CLI atual; a tela consulta metadados.

Validação: baseline4aprovados16,90s;5próprios31,95s e seleção30aprovados/1PGlocal
não executado/64deselecionados44,70s (contagens sobrepostas);6próprios finais21,31s
incluindo rótulo de reutilização. Teste PG novo roda no job existente. Snapshot de
onze runtimes protegidos idênticos ao pai; o store anterior é prefixo byte-idêntico
mais uma consulta de índice. Doctor com configuração existente20/1opcional/0.

Prova candidata usa cópia SOMENTE das cinco tabelas de journal, lidas de origem
SQLite mode=ro, sem copiar a base inteira nem criar avaliação. Aplicação real em
análise local abriu o item do menu:2protocolos/4pedidos/2execuções/17eventos, mesmo
runbf4d, snapshot de status e configuração/preferências originais preservados.
Capturas de pedidos/reserva inspecionadas; expõe treino usado para ajuste e reserva
externa desconhecida, qualified_final_holdout=False. O primeiro wrapper Xvfb saiu1
na limpeza de seu servidor já encerrado, após prova Python completa; permaneceu
registrado. Retomada somente de leitura da cópia existente com -d terminou0, sem
reclonar nem recalcular. Falha intermediária do helper por diretório config existente
foi corrigida no helper, sem mudança de dados/runtime financeiro. CI e instalação
são etapas próprias ainda pendentes; não há aprovação estatística/financeira.
