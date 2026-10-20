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

Estado: aceite funcional dirigido e revisão concluídos conforme registro abaixo;
checks existentes da aplicação pendentes. Não declara aceite ao vivo.
S09 somente Tester; cache inelegível. Cooldown/fechamento por horário, Pullback
neste filtro, live DEMO, carteira/gradientes, DST e Windows físico têm aceites próprios.


## Aceite funcional dirigido da revisão final

Runtimes H8a3b9d284b716e4a14a3d1bcaf95670a1544330a /
M372b762615abf4955b9198a574e88da1cb3193e4, fingerprintad7a304b82ff8084.
EURUSD/M5 13→15/10/2026; quarta,09–12h/exclusão09:15–09:25/âncora00h/cota1;
lote0,25/SL500/TP1000/teto600s. Backtest e replay:1 posição/2fills e10 eventos
de sessão cada. Oito recusas:5 por dia,1 por intervalo excluído,1 por cota e1
fora da janela. Eventos, fills, livro/CSV/saldo e resultado2,00USD coincidiram.
43inputs efetivos,26includes por execução e wrapper/INI/binários conferidos.
No conjunto com a compilação:14PIDs próprios encerrados,3perfis vazios,
ponte/principal com identidades preservadas; nenhum marcador nativo restante.
UI consultou, reabriu resultados e exportou depois do iniciador terminar, sem
outra execução. Cotas3/7, reinício/parcial e falhas estão nos testes do header real;
o par nativo exerceu a cota1 e não forçou corrupção/reinicialização. Proteções
fora da janela são preservadas pela ordem do controlador e pelos testes do motor.

Revisão técnica final concluída, sem pendência impeditiva; dois checks H aprovados.
Consumidor M#161 aguarda as suítes Ubuntu/Windows do workflow39499390366 existente;
outros quatro checks passaram. Recibo no extrator: docs/evidencias/T19_20_SESSAO_20261020.json.
Alterações posteriores aos runtimes acima são apenas documentais. Este é um
aceite funcional deste cenário/perfil, sem aprovação estatística ou ao vivo.
