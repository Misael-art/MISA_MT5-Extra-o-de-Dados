# T19.20 inicial — horários, dias e ciclos por dia de sessão

Filtro tipado `session_v1` na lista `composition.filters` v1 existente; não muda
o hash nem o significado de composições antigas. Novo perfil S09v1.70,43inputs.
Perfis anteriores continuam abrindo e recusam o filtro que não implementam.

Parâmetros: `start_minute`, `end_minute`, `exclude_start_minute`,
`exclude_end_minute`, `weekdays`, `anchor_minute`, `max_cycles`.
Minutos0..1439; dias em bits seg0..dom6 (1..127); cota0..1000000 (0 sem cota).
Início/fim iguais permitem24h na janela e desabilitam exclusão. Intervalos
`[início,fim)`, incluindo os noturnos, usam horário do servidor da corretora.

O dia lógico é `[âncora,âncora+24h)` no relógio declarado, com o dia da âncora.
Consome a convenção de sessão ancorada do calendário observado T13.1; segmentos
observados/gaps não geram previsão de pregão nem nova vaga de cota diária.
Sem inferência de offset, UTC real, DST ou feriados futuros. O motor Python exige
`entry_time_basis="broker"`; `BaseAdapter.run` recebe `time_basis="broker"`.
Fonte com fuso embutido ou relógio repetido/regredido é recusada, sem conversão.

A entrada é avaliada antes do dimensionamento/envio. O primeiro fill confirmado
consome um ciclo; redução/saída, rejeição e cancelamento sem fill não consomem
outro. Histórico por posição/ordem e identidade persistida recuperam a contagem
após reinicialização. Histórico ausente/inconsistente, persistência falha ou
identidade trocada não viram zero. Adição/reversão ficam indisponíveis.
SL, parciais e saídas continuam sendo gerenciados fora da janela e com cota cheia.

UI: Default preservado → Manual nomeado, ativar filtro, editar horários/âncora,
dias/cota, salvar revisão, reabrir e exportar pelo serviço existente. Dias/horários
inválidos bloqueiam execução. Aba Sessão mostra decisões, motivos e primeiros fills;
resultado só aprova o filtro quando há negócios, recusas observadas e replay
conciliado. Ausência de negócios continua sendo apenas ensaio de contrato.

Recibo nativo: `HDK_SESSION` precisa corresponder ao preset realmente carregado,
sinais, fills/posições, CSV e livro. A referência independente usa datetime e
conjuntos de primeiros fills, sem chamar o predicado de produção. Bloco ausente,
duplicado, truncado, contador/motivo adulterado ou envio após recusa impede aceite.
Resultado e eventos usam persistência/consulta/exportação existentes.

Estado: implementação/testes dirigidos e compilação isolada aprovados; ensaio
nativo funcional e checks finais ainda pendentes. Não declara aceite operacional.
S09 somente Tester; cache inelegível. Cooldown/fechamento por horário, Pullback
neste filtro, live DEMO, carteira/gradientes, DST e Windows físico têm aceites próprios.
