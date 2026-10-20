# T8.7 — complemento de aceite operacional ao vivo DEMO

O proprietário solicitou execução ao vivo DEMO em20/10/2026. Esse aceite é próprio:
S09, Pullback Nativo e o adaptador piloto continuam com bloqueios explícitos de
Strategy Tester DEMO. Nenhuma dessas proteções foi removida. As provas históricas
anteriores e a mecânica T8.7 já registrada permanecem preservadas; ensaio sem negócios
não é aceite operacional de uma estratégia.

## Contexto preparado e limite atual

Expert existente EMA+ATR, EURUSD/H1, preset legado `HDK_EmaAtr_Conservador`:
risco0,25% por operação, perda diária1%, semanal2% e drawdown5%, sem mudar limites.
O caminho PLAY e seus arquivos de execução/UI são idênticos aos da master integrada
46f31b6; nenhum runtime foi alterado nesta avaliação. Safety usada: snapshot aprovado
92c5208/1.7, com fonte/12includes/wrappers/binários isolados e identificados.
Nenhuma biblioteca de branch de trabalho foi recompilada ou instalada no principal.

A UI configurou ativo/TF/preset e executou **um** backtest dirigido do contexto
que ainda não tinha referência compatível:13→15/10/2026. Uma posição encerrada,
sem entrada desprotegida, resultado-250,92USD e caixa/CSV conciliados. Não é campanha
estatística, otimização ou recomendação de desempenho. O resultado negativo deve
continuar visível na revisão do PLAY, sem fabricar uma aprovação.

Os12inputs reais foram confrontados com o diário do agente e preset/defaults.
As verificações atuais do PLAY passaram após esse backtest. A leitura da conta
confirmou DEMO/permissão para Experts, cotações atualizando e nenhuma posição/ordem
no instante verificado; essa leitura não autoriza outra conta/contexto.

**Ao vivo não foi iniciado.** O replay próprio foi interrompido ANTES de iniciar o
Tester: terminal privado não confirmou sincronização em90s. O catálogo nativo da
mesma conta DEMO/build estava preparado; portanto essa mitigação, que permitiu o
replay do cooldown, não é uma recuperação universal. A causa final da intermitência
não foi comprovada. Não desligar proteções, trocar credenciais ou reiniciar o
principal/ponte para contornar a pré-condição.

14PIDs próprios observados ausentes,0zombies/0reutilizados,2perfis vazios e nenhuma
reserva nativa remanescente. Ponte/principal preservados e conectados à DEMO.
Recibo [T8_7_DEMO_PARCIAL_20261020.json](../evidencias/T8_7_DEMO_PARCIAL_20261020.json).

## Próxima ação concreta

Recuperar sincronização estável do perfil próprio e registrar confirmação nativa;
só depois concluir o replay faltante, preservando o backtest se código/parâmetros/
plataforma continuarem compatíveis. Revalidar conta DEMO/contexto/carteira/proteções
e realizar a observação ao vivo pela UI. Sem alteração da pré-condição, não repetir
sessões dependentes. PLAY/STOP sem negócios só comprovam os contratos exercitados.

O preset usado aqui é o legado conservador existente. Não comprova carregamento ao
vivo de pacote Default/Manual do piloto, nem libera S09/Pullback, carteira/gradientes,
cache ou Windows físico. STOP existente desanexa a sessão própria e não liquida
posições; esse limite precisa ser considerado antes de encerrar uma sessão com saldo.
