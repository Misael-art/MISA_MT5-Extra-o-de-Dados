# T17.28 — comandos e contexto operacional dos cards MT5

Pedido do proprietário em 2026-10-17, registrado antes do código. Uma tarefa/PR:
o fluxo completo de seleção → revisão → execução → acompanhamento → relatório.
Depende de T8.5/T8.7, T11.3, T12.2/T12.L e T17.26. Não inclui campanha PF19.

## Requisitos e aceite

1. Cada card MQL5 recebe ícones desenhados e rótulos PLAY, REPLAY e BACKTEST,
   seletores de ativo/timeframe e preset. Python conserva seu fluxo de pesquisa.
2. O contexto selecionado governa números, curva e elegibilidade. Exigir teste
   nativo do mesmo ativo/timeframe, código e valores efetivos de preset. Testes
   de bateria/paridade não são testes comuns. Ausência/desatualização desabilita
   PLAY com motivo visível. Seleção persiste durante atualização do catálogo.
3. PLAY revisa preset/risco, macro histórico, conta e carteira atual em duas
   etapas deliberadas. Conta REAL tem tarja vermelha e conta digitada. Histórico
   negativo exige terceira confirmação: média histórica ×100, na unidade e no
   dimensionamento do teste. Sem moeda comprovada, dizer unidade do teste; sem
   frequência confiável, não calcular semana. Nunca prometer perda/ganho futuro.
4. A instrução atual autoriza demo negativa após consentimento; substitui apenas
   a exigência estatística do PLAY demo novo. Segurança estática, stop, limites,
   compilação e identidade continuam exigidos. CLI demo existente não muda.
   REAL conserva bateria completa, teto e período demonstrado em demo, opt-in
   e consentimento por lançamento; ensaio REAL somente com provedores falsos.
5. REPLAY usa Strategy Tester, Model=4, Visual=1, sem dupla confirmação; permite
   escolher período e assistir, pausar/parar pelo Tester e STOP na aplicação.
   É histórico, independentemente do tipo de conta conectada. BACKTEST usa
   Visual=0 e modo oculto/minimizado. Só agentes locais; nunca nuvem paga.
6. Sessões portáteis próprias preservam o terminal aberto, seus gráficos e ponte.
   Nada de taskkill global, apagar Common/CSV prévio ou alterar presets originais.
   Cancelamento encerra somente processo pertencente à sessão; relatório cancelado
   não é tratado como backtest concluído/elegível.
7. Contorno pulsa azul aguardando posição, vermelho em perda e verde em lucro,
   com texto redundante e estado desconhecido para telemetria ausente/antiga.
   Posições são do magic/símbolo/conta/run selecionados. Não usar saldo agregado.
8. Relatório final durável contém contexto, parâmetros, período, método, custos,
   números e comportamento disponíveis, conclusão/cancelamento/erro e limitações.
   Reusar importador/metrics/store existentes; nenhum cálculo financeiro no widget.
9. Testar autorização vinculada ao contexto, mudanças de conta/preset/código,
   negativo/zero/ausente, STOP, erros e lifecycle; temas e DPI alto nas duas
   resoluções, teclado/rolagem; MT5 nativo demo e lançador instalado. Windows
   real pendente, caminhos simulados e CI separados.

## Fontes oficiais verificadas

- [Tester e ticks reais](https://www.metatrader5.com/pt/terminal/help/algotrading/testing).
- [INI, Model=4, Visual, Experts Account/Profile](https://www.metatrader5.com/en/terminal/help/start_advanced/start).

Configuração de ticks reais não prova cobertura perfeita: registrar o que o
relatório e diário efetivamente demonstram. Confirmar resultado financeiro
continua separado de autorizar conta. Nenhuma operação REAL está autorizada
para o agente nesta tarefa; os testes DEMO foram autorizados pelo proprietário.

## Baseline e preservação

Principal f49851f limpo. Instalador --yes --no-mt5 e requirements-dev em ambiente
3.11 próprio, XDG/atalhos/prefixo isolados. Árvore e todos os pacotes idênticos
à suíte 99fb95b: 1008 aprovados, 9 ignorados existentes, 3 avisos, saída0.
Prova privada t1728-baseline-reuse.json. Não reutilizar após alterações.

Implementação e aceites finais serão registrados aqui e no PR, com limitações
declaradas; planejamento não equivale a validação.
