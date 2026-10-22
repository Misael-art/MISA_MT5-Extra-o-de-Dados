# Painel de gestão acessível no REPLAY

Correção T17.28 solicitada em 21/10/2026. O painel HADOUKEN precisa aparecer e
receber comandos manuais no Strategy Tester visual.

## Exibição no Linux/Wine

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

Com essa correção, o replay real do `HDK_HADOUKEN_Painel` em EURUSD/H1,
14→15/10/2026, exibiu o painel e terminou com relatório importado, zero trades
e saldo simulado preservado. Isso demonstra o percurso observado, sem atribuir
uma causa interna universal ao Wine ou qualificar todas as combinações de host.

## Botões no Tester

O [livro oficial MQL5](https://www.mql5.com/en/book/applications/events/events_onchartevent)
registra a ausência de eventos interativos `OnChartEvent` no Tester visual.
O observador da sessão consulta o estado dos botões e encaminha o clique ao
handler original do Expert, antes de OnTick/OnTimer. Preserva o timer existente.

- Só no Tester visual, fora de otimização; não executa esse encaminhamento na conta.
- Só objetos `OBJ_BUTTON` do gráfico atual com prefixo `HDK7_<magic>_` do próprio Expert.
- Libera o estado do botão antes do encaminhamento e processa um clique por consulta.
- A consulta tem intervalo mínimo de 50ms de parede; não altera o relógio histórico.
- Risco, ordens, proteções e confirmação continuam no mesmo handler/host/executor.
  Não cria um segundo gestor nem implementa compra/venda no Python.

Não promete eventos de arraste, teclado ou edição de objetos no Tester; este
recorte cobre botões do painel. Pausar a simulação no visualizador também suspende
os eventos da estratégia; retomar o tempo permite processar seus comandos.

## Evidência e limites

Baseline: 56 testes aprovados antes da alteração. Regressões de ambiente
reproduzem a diferença na implementação antiga, incluindo compilação e sessão
interativa. Seleção final de controles/modo oculto/UI/presets/encerramento:
85 testes aprovados. Doctor `--no-probe`:20OK/1pandas-ta opcional/0falhas.
Wrapper com adaptação dos botões compilado no MT5:0erros/0avisos.

Os ensaios usam banco privado de QA e perfil próprio. A base e a configuração do
proprietário permanecem preservadas. Capturas, logs e recibos privados ficam fora
do Git. Windows físico, drag/teclado, execução financeira manual e operação DEMO
ao vivo mantêm aceites próprios. Não há promoção de risco, preset ou autorização REAL.
