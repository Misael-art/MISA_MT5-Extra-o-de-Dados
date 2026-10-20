# Manual nomeado e exportação — T19.26 inicial

`presets.manual.save_as_manual` clona o Default (ou Manual) com ID próprio, nome
obrigatório, referência ao Default de origem e revisão1. Escopo robô/ativo/TF é
preservado; novo contexto parte de seu Default. O Default e fontes não são editados.
Nomes iguais após acentos/caixa/slug são recusados neste contexto, sem fundir IDs.

O Manual inicial é **rascunho compilado, aguardando conferência nativa**: salvar não
aprova nem executa. Este recorte aceita somente parâmetros suportados pelo perfil
S09 aprovado. Escolha inválida/sem suporte é recusada, preservando a revisão salva;
rascunho livre de capacidades ainda não executáveis permanece fora deste recorte.

`save_revision` preserva anteriores e muda revisão/identidade de execução quando
parâmetros mudam. Repetir Salvar com a mesma composição é idempotente. Edição stale
é recusada para não perder trabalho. Nome de apresentação usa tabela lateral;
`rename_manual` conserva snapshot original, ID/revisão, .set e identidade de execução.
Reabrir e listar recupera o rótulo atual sem regravar a definição histórica.

Exportação usa nome T19.50 no `.set` e no manifesto fechado, conservando leitura dos
pacotes anteriores com `parameters.set` interno. Recompila o contrato/fonte atuais;
não atualiza fonte/versão silenciosamente. Contexto opcional de teste é explícito;
se mudar, a identidade da execução muda, mantendo o hash operacional da composição.
Subdiretório por execução separa identidades/contextos com mesmo basename. Publica
cada arquivo completo atomicamente sem substituir destino; manifesto por último
marca pacote completo. Reexportação idêntica não altera bytes. Divergência/link ou
falha de publicação/caminho exige outro destino; parcial próprio pode ser retomado.

Importação inicial recebe manifesto + .set local canônico, confere tipos/inputs,
versão, fonte, identidade e checksum pelo compilador existente. Preserva ID, nome,
Default de origem, contexto e estado não aprovado. Mesma definição/revisão é
idempotente, sem sobrescrever seleção ou rótulo atual; recibo lateral conserva o
pacote importado/contexto. Mesmo ID/revisão com outros parâmetros é recusado.
Nome ocupado por outra identidade também é recusado. Default efetivo é criado
pelo serviço local, não substituído por importação. `.set` legado sem pacote segue
nos leitores/fluxos legados, sem conversão automática ou aprovação presumida.

Schema é aditivo: `_preset_labels`/`_preset_imports`; dados/defaults/runs anteriores
permanecem intactos. Não instala Expert, altera limites, inicia replay ou envia
ordem.71 testes focados e quatro verificações pertinentes após integrar exportação
do Default/CAS passaram. PostgreSQL no job existente; hardlink/publicação e caminhos
reais do Windows físico continuam pendentes específicos. UI, carregamento real do
preset, negócios e reconciliação são consumidores seguintes. Pullback bloqueado.
