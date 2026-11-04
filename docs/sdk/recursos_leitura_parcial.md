# Recursos: leitura de pastas e acesso às cópias

A abertura de Recursos inclui uma leitura opcional das pastas e dos arquivos
antigos. Em uma instalação com muitos arquivos, essa leitura mantinha toda a
aba ocupada e impedia usar a lista de cópias. A prova na cópia privada da base
real excedeu45 e90segundos antes de criar um backup; a thread estava percorrendo
pastas. Nenhum banco do proprietário foi restaurado nesse ensaio.

A interface passa um prazo cooperativo compartilhado de1segundo às duas leituras
opcionais. O relatório do SQLite, o disco, a lista de backups e os comandos
existentes continuam disponíveis quando essa parte não termina. O tamanho da
pasta interrompida fica indisponível (`None`), com aviso de leitura incompleta.
Um total parcial não vira zero nem total completo.

Se a lista de arquivos antigos não terminar, seu prefixo é descartado. A tela
explica a leitura incompleta e mantém a limpeza indisponível, inclusive se o
callback for chamado diretamente. Isso evita confirmar uma limpeza com uma
prévia parcial. A implementação da limpeza e as proteções dos artefatos mantêm
o percurso existente; chamadas de medição/CLI sem callback continuam completas.

O prazo é conferido entre operações do filesystem. Uma operação individual de
I/O ou a enumeração interna de uma pasta pode ultrapassá-lo. Ele não é um limite
absoluto de duração da tela, das consultas SQL ou da cópia/restauração do SQLite.
Não cria scheduler, cache, contador de jobs ou configuração financeira.

Validação proporcional:45 testes existentes passaram no baseline; quatro
regressões novas falharam antes da alteração. A seleção final dos quatro casos
mais os45anteriores passou49testes em59,05s. Mais24testes de retenção de sessões
e artefatos passaram em12,65s. As contagens de baseline/final se sobrepõem.
O percurso privado pela interface e os checks finais têm evidência própria;
fonte candidata, integração e instalação são estados distintos.

Após compor por Git o layout66571c4, três casos de interação entre Recursos e o
detalhe de Robôs passaram em38,09s. Um caso adicional força interrupção após
medir o primeiro arquivo: os cinco casos próprios finais passaram em11,81s.
O prefixo do tamanho e o prefixo dos candidatos não são publicados como completos.

Na fonte candidata, a interface criou a cópia verificada de455,6MB, guardou o
estado anterior e restaurou a cópia escolhida. Exportou12.405barras EURUSD/H1 e
conferiu todas as colunas com a base original aberta só para leitura. O run da
composição permaneceu igual. A releitura exibiu três cópias, backup/restauração
disponíveis e limpeza bloqueada com aviso. Foram usados SQLite privado e
confirmação controlada de QA; o banco do proprietário não foi restaurado.
Um prazo inicial de90s do ensaio interrompeu a verificação após a cópia, antes da
auditoria final. Esse resultado não foi aceito; a retomada privada com prazo300s
concluiu a operação e a auditoria. As cópias e os diagnósticos foram preservados.

Windows físico, unidades de rede, filesystem bloqueado e escalabilidade geral
de todas as consultas conservam validação própria. Não há novo aceite de MT5,
operação, estratégia ou desempenho financeiro.

Composição posterior do ensaio de alcance1b0ba510: três casos de interação UI
passaram42,93s. Os três runtimes de Recursos permanecem byte-idênticos a8aef19c;
a prova SQLite privada foi reaproveitada por esses hashes, sem repetir restauração.
A evidência original conserva seus hashes do snapshot8aef19c, por ancestralidade.


Composição da correção de altura172/bfc422 por Git:4testes de interação passaram
em51,11s (Recursos +contexto +escala1,5). Os três runtimes de Recursos permanecem
byte-idênticos ao snapshot8aef e à prova privada completa. Não se repetiu backup/
restore de455,6MiB por mudança da página de Robôs. CI anterior e070 falhou no mesmo
mapeamento144DPI de172 e está preservado; novaCI da composição ainda necessária.
O processo visual Windows isolado de172/bfc passou, sem substituir sua suíte completa.
