# Exibição do painel no REPLAY — operação manual ainda pendente

Correção T17.28 solicitada em 21/10/2026. **Exibição corrigida no ensaio Linux;
operação manual completa ainda não aprovada. PR em draft, sem instalação.**

Atualização do proprietário: o código está incluído na
[prévia de desenvolvimento para teste](../TESTAR_DESENVOLVIMENTO.md), disponibilizada
pelo atalho habitual. A referência aceita permanece preservada e o aceite da
interação manual continua pendente.

## Correção de exibição

A sessão anterior compilava no Xvfb e abria o replay no desktop, podendo ainda
trocar de driver X11 para Wayland. No host Plasma/Wayland, Wine11.17/MT56276,
dois ensaios não chegaram a um painel utilizável e encerraram sem relatório,
com `BadWindow/X_UnmapWindow` no log de processo. Remover apenas a variável
Wayland não resolveu o ensaio intermediário.

Compilação e execução de PLAY/REPLAY agora usam o mesmo DISPLAY do aplicativo,
via X11/XWayland. A variável de modo oculto é explicitamente desligada no filho.
O backtest continua no Xvfb; no Windows a compilação/backtest continuam
minimizados, e PLAY/REPLAY mantêm o comportamento nativo de janela visível.
O ambiente global do usuário não é alterado.

O replay do `HDK_HADOUKEN_Painel` em EURUSD/H1,14→15/10/2026, exibiu o painel
com essa mudança, inclusive antes da experiência de adaptação dos cliques.
Uma execução posterior com essa mesma configuração de display e observador
experimental terminou com relatório importado, zero trades e saldo simulado
preservado. Não é qualificação financeira ou aprovação do observador experimental,
nem prova de causa interna universal no Wine.

## Lacuna manual comprovada e preservada

O [livro oficial MQL5](https://www.mql5.com/en/book/applications/events/events_onchartevent)
registra a ausência de eventos interativos `OnChartEvent` no Tester visual.
Foi ensaiada uma consulta ao estado dos botões `HDK7_<magic>_` para encaminhar
cliques ao handler original. Compilou0/0, mas a aba Risco não mudou no ensaio
físico; portanto a adaptação **foi retirada do código final**. O commit anterior
`ba59485` conserva a proposta, sem promovê-la a capacidade aprovada.

A captura inicialmente indicava o objeto de fundo `HDKV_<magic>_BG` sob o cursor.
Uma experiência privada ocultou só essa camada: o cursor passou a identificar o
botão correto, mas a aba ainda não mudou. Ocultar o fundo também não entrou no
runtime final. Não atribuir a falha somente a essa camada ou afirmar que polling
resolve a interação. Um ensaio de reuso foi invalidado por LiveUpdate do perfil
privado para6281; a tentativa posterior usou novamente a origem6276.

**Próximo recorte necessário:** instrumentar em QA o estado real do botão e o
recebimento no handler, confrontando o clique nativo com a visualização do Tester;
resolver a entrega de ações pela integração existente e repetir apenas esse aceite.
Preservar o mesmo RiskGuard/executor, os pins Safety e a base do proprietário.
Validar abas, pausa/retomada e só depois ações financeiras simuladas conciliadas.
Arraste/teclado e Windows físico têm verificações próprias. Até passar, o painel
não está qualificado para o uso manual essencial solicitado pelo proprietário.

## Validação e preservação

Baseline56 testes aprovados. Código final:84 testes pertinentes de controles,
modo oculto, UI, presets e encerramento; doctor20OK/1pandas-ta opcional/0falhas.
O wrapper original compilou0/0 no ensaio de exibição. Regressões exercitam Linux
e Windows simulados, incluindo compilação e backtest oculto; Windows físico pendente.

Ensaios em bancos privados de QA e perfis próprios; dados/configuração/WIP do
proprietário preservados. Falhas, capturas e recibos privados permanecem fora do
Git. Limpar só cópias próprias após arquivar e conferir ausência de processos,
incluindo filhos tardios do atualizador no perfil privado. Não houve ordens ao vivo,
alteração de risco/executor ou autorização REAL. A instalação aceita continua intacta.
