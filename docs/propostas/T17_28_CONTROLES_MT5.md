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

## Implementação

Cards MQL5 têm seletores pesquisáveis, contexto visível, comandos responsivos,
ícones desenhados e detalhes de risco/estágio recolhíveis. A revisão do PLAY
mostra carteira, orçamento de risco e histórico exato. Consentimento expira em
três minutos e é vinculado a código, valores, teste, identidade e exposição.
Conta e fontes são relidas depois da compilação. Sem backtest correspondente,
o PLAY permanece desabilitado; seleção não transforma teste em aprovação.

Cada execução usa instalação portátil própria. BACKTEST fica no display oculto
Linux; REPLAY mantém controles nativos e gráfico de velas. Model=4, Visual=1/0,
agentes locais e porta própria são explícitos. STOP, prazo e fechamento da UI
alcançam somente os processos dessa instalação. A presença do aplicativo é
revalidada no Expert em conta; ausência prolongada impede novas ações.

O observador é somente leitura: acompanha magic/ativo, exporta posições com
custos de entrada/saída e inclui fechamento final do Tester com magic zero.
Livro e variação do saldo nativo precisam reconciliar antes da gravação.
Cancelamento não habilita PLAY. Relatório JSON e arquivos nativos ficam na
pasta da sessão, inclusive quando há erro ou interrupção após sua preparação.

## Ensaios e correções encontrados no host

- Compilação nativa do wrapper: respeitar callbacks em includes e usar nome
  próprio, porque a recompilação automática pode substituir o binário original.
- Reconciliar a última posição, fechada pela plataforma com magic zero: o livro
  completo tem 16 posições, resultado líquido -2.342,53 USD, PF0,47995551 no
  cenário EURUSD H1, 01/09–01/10/2026. Isso é QA de software, sem evidência OOS.
- Um ensaio inicial regravou um CSV compartilhado porque o executor legado
  forçava exportação. O arquivo original foi restaurado de backup verificado;
  todos os 14 CSVs originais voltaram a coincidir byte a byte. O fluxo novo
  desliga essa exportação depois dos defaults e isola File/Folder Common do
  histórico por UUID. Regressão preserva sentinela e recusa CSV de sessão já
  existente. Ensaios corrigidos preservaram todos os 23 arquivos monitorados.
- PLAY real no terminal DEMO iniciou o Expert e publicou estado sem posição;
  STOP encerrou somente a sessão, mantendo zero posições/ordens e saldo. Por
  estar fora do mercado, esse ensaio não demonstra abertura/fechamento ao vivo.
- A inspeção 1280/1920 levou seletores para diálogos pesquisáveis e botões para
  o topo. Métricas ausentes em histórico antigo ficam desconhecidas, sem zero
  inventado. Testes falsos cobrem conta REAL, consentimentos e lucro/perda.
- REPLAY iniciado pelo card concluiu sem intervenção para fechar o visualizador:
  livro de 16 posições e variação de saldo diferem só por arredondamento
  binário (<0,01 USD); relatório apareceu na aplicação. Capturas comprovaram
  contorno verde em lucro e vermelho em perda, com resultado/moeda em texto.
  STOP pelo card interrompeu uma outra passagem sem gravar resultado elegível.
- PLAY iniciado pelo card percorreu revisão, conta digitada e confirmação
  adicional negativa no DEMO nativo. A telemetria publicou sem posição e STOP
  encerrou somente a cópia. Campo de confirmação fica fixo acima dos botões,
  fora da rolagem de detalhes. Reabrir relatório conserva a duração final.
- A extensão opcional do executor foi documentada em [SDK](../sdk/api.md), com
  teste dos defaults anteriores preservados. Não remover a guarda de API para
  aceitar a nova assinatura sem atualizar sua referência.

A suíte integral, revisão do patch, CI no SHA final, integração e lançador
instalado serão registrados no PR e na continuidade após sua conclusão.

## Limites

Nenhuma conta REAL foi operada. Windows físico permanece pendente: caminhos e
ownership são simulados nos testes; CI não substitui MT5 nativo Windows. Não há
liquidação automática ao parar: posições/ordens já existentes continuam na conta.
Telemetria sem atualização é desconhecida. A observação de posições é por magic
e ativo: instâncias externas com os mesmos identificadores precisam ser evitadas.
O observador não substitui Safety, e um teste isolado não prova rentabilidade.
O gate REAL conserva bateria completa, teto e pelo menos sete dias acompanhados
em demo, configuráveis em ROBOTS.min_demo_days. Não houve campanha financeira.
