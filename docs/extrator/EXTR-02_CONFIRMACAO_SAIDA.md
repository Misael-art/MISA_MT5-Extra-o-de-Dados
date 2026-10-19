# EXTR-02 — confirmar saída pela identidade do processo

Correção pontual do impedimento encontrado no Ubuntu da PR126/T19.4, sobre master2b27b1f.
Uma tarefa por PR: esta frente trata somente o encerramento da ponte; Alligator permanece
na PR126, com sua referência, custo e evidências preservados.

## Causa reproduzida

O stop anterior esperava enquanto owned(record) fosse verdadeiro. O ambiente de /proc pode
ser desmontado durante a saída antes de a identidade desaparecer ou tornar-se zumbi.
Nesse intervalo, perder o token fazia stop retornar sucesso/remover registro com o PID ainda
identificável. Não era falha de SMMA nem de ctypes. O teste existente da libc expôs essa corrida.

Teste determinístico antes da mudança: falhou porque stop não aguardou nenhuma vez e anunciou
parada com a identidade ainda viva. Agora a espera usa identidade completa (PID/nascimento/boot/UID).
Os prazos5s/2s são preservados; cada sinal continua exigindo posse fresca e pidfd. Se a posse for
perdida e o processo não sair, a escalada é recusada e o registro permanece para ação do usuário.
Identidade substituta nunca recebe sinal. Nenhum Wine, terminal, display ou sessão externa é parado.

## Verificação proporcional

17 testes da ponte aprovados em3,92s, incluindo três casos determinísticos novos: ambiente
sumiu antes da identidade; identidade foi reutilizada; token sumiu sem saída e não pode haver
escalada. O teste original libc também passou, sem alterar sua asserção ou inserir espera nele.
Doctor21OK/1opcional/0falhas. Não há shell alterado nem nova dependência/configuração.
Nenhuma materialização/campanha de Alligator foi repetida; não houve chamada de ordens.

Mudança de produção restrita a Bridge.stop, caminho Linux/Wine. O caminho Windows nativo da
entrada principal está byte a byte inalterado e foi simulado no teste existente; testes deste
arquivo já são Linux-only. Evidência Windows da base #125 permanece compatível com esse
caminho inalterado; Windows físico permanece pendência delimitada. CI Linux e revisão final
aguardados antes da integração. A ponte real da Root é preservada em segundo plano.

Artefatos privados: logs/extr-02-confirmacao-saida-20261019/{blocker.json,regression-before.log,
focused-bridge.log,doctor-result.json}. O diário da PR126 registra a falha original Ubuntu;
sua integração só prossegue após incorporar esta correção por Git, preservando trabalho local.

## Aceite final dos checks existentes

Os seis checks do run39278788779, commit63e717f, concluíram com sucesso, incluindo o Windows
já existente. Ubuntu:1395 aprovados/166 skips em959,19s, cobertura77,64%; PostgreSQL:34
aprovados em26,24s; Windows CI:1497 aprovados/64 skips/5avisos em2856,50s, cobertura73,36%.
Lint e ambos os instaladores aprovados. Revisão sem pendências. O teste libc original e os
três casos novos passaram; nenhuma asserção foi relaxada. O fechamento modifica só documentos,
com fontes/testes idênticos ao commit aprovado e [skip ci], preservando os checks existentes.

A PR126 já incorpora esta correção por merge Git; composição70b2191 validada com34 testes
Alligator+ponte e doctor21/1/0. Seu novo CI é justificado pela mudança de runtime e pela falha
Ubuntu anterior. Integração em master respeita127 antes de126; SHA real fica no diário da
integração. A ponte própria permanece em segundo plano; Windows físico continua pendente.
