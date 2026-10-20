# Painel modular HADOUKEN — cobertura e atividades

**Registro de 2026-10-20. Design, organização e estrutura aprovados pelo proprietário.**
Esta entrega documenta requisitos, cobertura encontrada e recortes de desenvolvimento.
A aprovação visual não declara os serviços implementados nem autoriza negociação real.
“Hadoop”, no pedido, foi interpretado como **HADOUKEN**, conforme o contexto do projeto.

## 1. Decisão de produto preservada

Um único painel flutuante e arrastável por gráfico, iniciado **compacto**, com
informações principais, ativo/TF, identificação do Expert/Magic, estado, quantidade,
SL/TP rápido e os comandos **Comprar, Vender, Finalizar e Zerar tudo** alcançáveis.
As cinco pequenas abas de fichário — **Operação, Risco, Cestas, Análise e Visual** —
expandem o corpo do mesmo painel. O `+` revela parâmetros dentro desse corpo;
abrir uma aba ou expandir um módulo não inicia uma estratégia ou ordem.

[Mockup aprovado, três estados do mesmo painel](../img/painel-modular/mockup-aprovado.png).
As três representações da imagem são estados alternativos, não três janelas simultâneas.
A cor de cada aba identifica sua função; a cor discreta de cada gráfico identifica
sua instância. Esses dois códigos visuais têm papéis distintos.

O núcleo do painel pode avançar com as capacidades integradas. Serviços faltantes
entram nas atividades abaixo; não tornam a construção de todo o painel dependente
de carteira concorrente, cestas, Monte Carlo ou drift. Um módulo sem suporte mostra
o requisito ausente e seu estado; não simula disponibilidade. Esta é a interpretação
de trabalho da frase final ambígua do pedido; não houve início de implementação.

## 2. Base da avaliação e limites

Inspeção estática das bases remotas integradas, sem promover WIP dos clones principais:

| Repositório | Base inspecionada | Alcance |
|---|---|---|
| mt5x / extrator | `b139f04f1f884f4606c0742bbb6945989e2655a2` (`origin/master`) | Dados, carteira, estatísticas, pesquisas, presets e abertura dos gráficos |
| HADOUKEN | `596f2c8ce132789095d6b718bd75bd46cecabd08` (`origin/master`) | Painel v7, Expert manual, estratégias, executor e planos F/G/H |
| mt5x-safety | `dbff0d2f12b42e5777d245007e5136bba4e416cf` (`origin/main`, VERSION 1.7.0) | Fonte distribuída da execução; não confundir com o checkout local antigo 1.5.0 |

O blob de `HDK_SafeExecutor.mqh` nas bases H/S acima é
`780dc953342269200ccfe2e17afa688215dbf5ef`, nos respectivos caminhos de distribuição.
Não se utilizou o comportamento de uma biblioteca local antiga como aceite atual.
As referências históricas dos épicos não substituem a inspeção do código integrado.
Essa base S é documental: a coordenação informou que seus executáveis continuam no
snapshot Safety 1.7 previamente aprovado `92c5208`. Não houve recompilação, troca de
biblioteca ou promoção de `dbff0d2` para executáveis por causa desta avaliação.

A frente de integração confirmou que lotes/parciais/BE após confirmação,
Pullback histórico e S09 temporal/ER/Donchian integrados não qualificam carteira
concorrente, cestas, equity isolada ao vivo ou os recursos probabilísticos pedidos.
TM07 ATR (H#31/M#159) estava em revisão, fora destas bases; não é capacidade entregue
por este documento. Nenhum MT5, compilador ou Tester foi usado nesta avaliação.

Há diferenças materiais entre as fontes reutilizáveis: `live.status` soma custos dos
deals de saída selecionados e não inclui `fee` nessa projeção; o exportador de sessão
já agrega comissão/swap/fee por posição; `HDK_TradeStats` conta saídas e associa comissão
de entrada pelo identificador da posição. Isso não equivale à contabilidade reconciliada
de operações com várias parciais ou vários donos. A atividade PM-01 deve tratar essas
diferenças com testes de reconciliação, sem renomear o resultado atual como cobertura total.

Os IDs **PM-00–PM-12** abaixo identificam requisitos/recortes desta cobertura.
Não criam um segundo framework, ledger, registry, executor ou conjunto de épicos.
Cada recorte entra na ficha de seu dono F/G/H/I/F19/F20; uma tarefa por PR.

## 3. Organização das abas

| Área | Conteúdo e administração |
|---|---|
| Compacto permanente | Identidade, estado, resultado aberto/fechado com escopo, boleta e saídas; alerta resumido com acesso ao motivo |
| Operação | Entrada/orientação, tipos de ordem, gradiente fixo/dinâmico/direcional, lotes e parciais, ganhos, stops/proteção e filtros |
| Risco | Limites, exposição/margem, mandatos, travas, regras de prop firm e diagnóstico de desvio de risco/lote |
| Cestas | Seleção e membros da cesta, preço médio por ativo/direção, resultado líquido, TP/SL financeiro, progresso e fechamentos autorizados |
| Análise | Recortes EA/Magic/comentário, P&L/equity identificada, custos, Monte Carlo e comparação com referência de drift |
| Visual | Favoritos e navegação, cores de status, linhas/arraste, temas, degradê/marca d'água e disposição por gráfico |

Posições, pendentes, log, alertas, filtros, pausar/retomar, linhas e organização da
janela permanecem alcançáveis. Risco tem uma administração única; a boleta exibe
apenas resumo/prévia da mesma fonte. Não duplicar regras em cada aba.

## 4. Matriz de cobertura encontrada

**Parcial** significa infraestrutura reutilizável ou uma versão limitada existente,
não a funcionalidade integral pedida. **A desenvolver** significa ausência de
evidência integrada no caminho ativo examinado; não afirma inexistência em todo legado.

| ID | Requisito | Cobertura atual | Lacuna / dono |
|---|---|---|---|
| PM-00 | Todos os pilares operacionais no painel aprovado | Parcial: mercado, boleta limite/stop, stops e gestão existentes; F19 acrescenta capacidades por perfil | Catálogo completo na UI e ligação por capacidade; G-02/03/05/06/12, T19.17–23/30/56/57 |
| PM-01 | P&L por EA/Magic/comentário | Parcial: `robots/live.status`, `portfolio.snapshot` e H `HDK_TradeStats` recortam Magic; comentário não é dimensão integrada dessas projeções | Identidade, comentário, custos e reconciliação; H-01/02/03/11/14 e F-02/10/11; apresentação G-09 |
| PM-02 | Equity isolada observada ao vivo | Parcial: `strategies/equity.py` e T20.3 tratam curvas de backtest/re-simulação; não há aceite de coletor live por múltiplos EAs | Série observada e capital atribuído por escopo; H-09/10/11, T20.3/18 e G-09 |
| PM-03 | Basket TP/SL líquido e preço médio ponderado | Parcial: leitura de carteira, fechamento próprio e fechamento supervisionado existem; sem motor financeiro de cesta qualificado | Composição/custos/monitor/encerramento; E-12, F-06/08/10/12, H-01/02/13 e G-08 |
| PM-04 | Fechamento por contraposição / Close By | A desenvolver no executor integrado examinado | Operação hedge com ambos os tickets autorizados e contabilização própria; extensão de E-12/F-08/F-11/H-01, distribuição Safety |
| PM-05 | Barras de progresso TP/SL de cesta | A desenvolver no painel ativo | Consumo do estado PM-03, sem recálculo financeiro no widget; G-08 |
| PM-06 | Monte Carlo para meta versus violação de limite | Parcial: `validation.monte_carlo` reamostra retornos e calcula DD; `ruin_probability` atual usa DD ≥ 50%, não regras de challenge | Barreiras, horizonte, calendário e amostra por escopo; I-04/T20.13 e apresentação G-09 |
| PM-07 | Alerta de drift operacional | Planejado: I-12, T20.18/19 e T13.8 possuem donos relacionados; sem aceite deste detector ao vivo | Referência por versão, regras de risco/lote e alertas calibrados; T20.19/I-12 com G-13 |
| PM-08 | Favoritos e troca rápida no gráfico | Parcial: `_asset_prefs`, `asset_table` e GUI já persistem favoritos | Disponibilizar no MT5 e navegar sem perder gestão das posições; G-04/11/14 e integração `robots/layout`/`view` |
| PM-09 | Cores dinâmicas de P&L/BE | Parcial: painel usa cores de estado/P&L; linhas de entrada/SL/TP têm cores fixas e fechamentos usam verde/vermelho | Estado dinâmico, faixa de BE líquido e legenda acessível; G-11/14 |
| PM-10 | TP/SL arrastável com prévia financeira | Parcial: `CHDKChartMarks` desenha linhas, explicitamente não selecionáveis | Prévia, validação e alteração pelo executor, com retorno/rollback; G-11, E-01/B-05 e Safety |
| PM-11 | Degradê e marca d'água por gráfico/Expert | A desenvolver no caminho ativo v7 | Identidade visual estável, camadas, preferências e desempenho; G-14 |
| PM-12 | Mesmo painel como padrão de cada Expert | Parcial: componente v7 compartilhável e painel manual; estratégias nativas não comprovam essa integração completa | Adaptador de apresentação/eventos por Expert e ciclo de vida; G-02/03/04, contrato de capacidades e dono do StrategyRobot |

## 5. Atividades e critérios de aceite

### PM-00 — Preservação operacional e ligação do catálogo

- **Prioridade/dono:** P0 para inventário e G-02; apresentação G-03/05/06/12 sobre as capacidades F19 integradas.
- **Passos:** mapear cada ação/parâmetro atual para o novo lugar → modularizar sem mudar
  comportamento → aplicar compacto/abas/expansão → ligar cada editor à capacidade declarada.
- **Cobertura obrigatória:** mercado/limite/stop/stop-limit; nível fixo/Fibonacci/reteste;
  gradiente fixo/dinâmico e direção a favor/contra; volume fixo/por risco, adições e parciais;
  ganho fixo/R/ATR/indicador/sinal/tempo; stop fixo/ATR/dinâmico, trailing, BE e saída temporal;
  horário/dias/notícias/frequência/cooldown/spread/direção e orientação normal/inversa quando suportada.
- **Aceite:** nenhuma função antiga desaparece; presets e unidades conservam significado;
  abrir o editor não executa ordens; suporte ausente explica o motivo. Stop inicial,
  trailing e saída temporal são papéis distintos, componíveis conforme o contrato.
  Saída temporal não remove o stop protetor obrigatório.
- **Provas:** mapa antes/depois e cliques nas três disposições/dois temas; providers falsos,
  paridade dos robôs e replay nativo somente no recorte integrado usado.

### PM-01 — Identidade e analytics reconciliados por dono

- **Prioridade/dono:** P1; H-01/02/03/11/14 com F-02/10/11; G-09 consome o resultado.
- **Dados:** servidor/conta/ambiente, EA/versão, instância, Magic, símbolo/TF,
  comentário original, regra de agrupamento versionada, ordem/deal/posição/operação/ciclo,
  timestamps, moeda e custos. Comentário é rótulo/filtro; não concede autorização.
  Magic 0, colisão, comentário ausente/truncado e donos externos ficam identificados.
- **Passos:** ampliar o ledger/ingestão existentes aditivamente → preservar eventos brutos
  e vínculo à posição → publicar recortes únicos para painel, aplicação e relatórios.
  Ingestão e deduplicação usam identidade de conta/servidor e identificadores nativos;
  reprocessamento/reconexão não duplicam deals. Compartilhar a coleta por conta, em vez
  de cada painel reconstruir isoladamente todo o histórico. O ledger atual orientado
  a runs não é, por sua existência, um livro completo de operações ao vivo.
- **Aceite:** realizado, flutuante, custos e fluxos externos separados; parciais não viram
  trades adicionais. Comissão de entrada/saída, swap e `DEAL_FEE` são conciliados sem dupla
  contagem. Custo sem atribuição comprovada permanece não atribuído, não zero.
  Soma por dono, externos e valores não atribuídos tem reconciliação explicável com a conta.
- **Provas:** dois EAs no mesmo ativo, vários ativos por Magic, manual por comentário,
  parcial/reversão/Close By, reconexão, histórico incompleto, troca de conta e netting.
  Em netting, ler só `POSITION_MAGIC` não prova a contribuição econômica de cada EA.

### PM-02 — Série observada por EA e mini curva no painel

- **Prioridade/dono:** P1; H-09/10/11, reuso T20.3; coletor/DTO e depois G-09.
- **Passos:** definir capital alocado ao escopo → observar posições e histórico conciliados
  → persistir amostras por identidade → disponibilizar curva no módulo Análise, expansível.
- **Aceite:** sem capital atribuído a série é chamada **P&L acumulado**, não patrimônio
  independente. Com capital atribuído, equity virtual = capital do escopo + realizado líquido
  + flutuante apropriado, com fluxos de alocação separados. Não copiar `ACCOUNT_EQUITY`
  como se fosse equity de cada EA. Moeda, frequência, origem, último dado e início da
  observação visíveis; lacunas anteriores não são interpoladas como equity observada.
  A coleta continua com a UI recolhida; o gráfico da curva pode suspender só o desenho.
  Cadência/orçamento e retenção são explícitos; falha na ponte não produz amostras zero.
  Definir e medir a meta de atualização ao vivo (referência inicial de até um segundo
  em cenário declarado), sem confundir idade da cotação com atraso de renderização.
- **Provas:** perda aberta recuperada antes do fechamento, aportes/saques, posições
  simultâneas, custos, reinício, amostra perdida/atrasada e desenho recolhido/expandido.
  Curvas modeladas por barras continuam identificadas e distintas das observadas ao vivo.

### PM-03 — Cesta financeira com custos e mandato

- **Prioridade/dono:** P1 após PM-01 e permissões/reconciliação; E-12/F-06/08/10/12,
  H-02/13; a UI G-08 apenas administra a configuração e os resultados desse serviço.
- **Passos:** definir membros e ciclo → calcular P&L líquido atual → definir TP/SL em moeda
  e/ou percentual com base explícita → monitorar → cancelar pendentes e encerrar somente
  os membros autorizados → reconciliar cada resposta e recuperar após reinício.
- **Aceite:** incluir custos efetivos acumulados, sem contar novamente swap já realizado;
  custo futuro de fechamento é estimativa separada, com método e disponibilidade.
  “Ponto exato” é reformulado como **nível/resultado estimado**, sujeito à execução.
  Preço médio ponderado existe por símbolo e direção; cesta multissímbolo usa P&L
  consolidado, sem inventar um preço único. Para exposições opostas, mostrar os dois lados
  e o resultado líquido. Nenhum alvo financeiro virtual substitui os stops protetores.
- **Provas:** swaps/comissões/taxas, moedas e especificações suportadas, volume desigual,
  preenchimento durante zeragem, recusa/fechamento parcial, queda/reinício e mandato revogado.
  Vários EAs exigem adesão/mandato e decisão única de fechamento, não disputa entre painéis.
- **Referência para o cálculo:** em um instrumento hipotético de tick 1 e valor 1 por
  tick/lote, compra de 2 lotes a 100, comissão efetiva −2 e swap −1, o resultado a 106
  é 9. Uma meta líquida de 15 corresponde a 109 nesse modelo simples; acrescentar
  comissão futura estimada −2 desloca a prévia para 110. Exibir a diferença entre
  custos já incorridos e hipótese futura; não generalizar essa fórmula a outros modos
  de cálculo/moeda sem a especificação e o motor financeiro correspondentes.
- **Dependência a detalhar:** E-12 referencia E-11 no plano histórico. Ao abrir a ficha,
  confirmar com o dono se o recorte de cesta sem grade pode ser separado do motor de
  gradiente; não considerar essa dependência resolvida por este documento.

### PM-04 — Close By protegido e contabilizado

- **Prioridade/dono:** P2; extensão de execução E-12/F-08/F-11, com dono da Safety e H-01.
  Depende de PM-01/03 para escopo, membros e resultado. PR do serviço precede a ação G-08.
- **Passos:** detectar conta hedge e permissão do símbolo → selecionar pares opostos do
  mesmo símbolo → revalidar ambos os tickets e seus mandatos → executar pelo SafeExecutor
  → verificar retorno do servidor e exposição residual → reconciliar custos/resultado.
- **Aceite:** respeitar volume remanescente, FIFO/restrições aplicáveis e autorizações de
  ambos os donos. Sem suporte, a ação explica o motivo. Não abrir posição contrária apenas
  para forçar essa operação nem trocar silenciosamente por outro fechamento.
  A economia de comissão não é garantida; registrar custo efetivo da corretora.
  A atribuição do resultado entre EAs é uma política explícita do ledger, não inferência
  ingênua do deal de saída: o lançamento nativo dos dois lados é assimétrico.
- **Provas:** pares iguais/desiguais, mesmo Magic/dois donos autorizados, revogação,
  flags sem suporte, netting, retorno positivo sem execução confirmada, rejeição e reinício.
  A documentação oficial confirma a possibilidade de economizar um spread no Close By,
  a exigência da flag do símbolo e o volume compensado pelo menor dos dois lados.
  [Semântica nativa](https://www.mql5.com/en/book/automation/experts/experts_closeby).
  O retorno booleano da biblioteca não substitui a conferência do resultado do servidor.
  [PositionCloseBy](https://www.mql5.com/en/docs/standardlibrary/tradeclasses/ctrade/ctradepositioncloseby).

### PM-05 — Progresso de TP e SL da cesta

- **Prioridade/dono:** P2; G-08 após DTO PM-03; prévia somente leitura pode entrar antes
  do fechamento automático, desde que identificada como prévia e sem botão operacional.
- **Passos:** consumir baseline/resultado/limites/estado e timestamps → desenhar duas
  barras pequenas, uma para TP e outra para risco consumido → expandir composição/custos.
- **Aceite:** base do ciclo e unidades explícitas; ganho em direção ao TP e perda em direção
  ao SL não são a mesma porcentagem. Denominador zero/limite ausente retorna N/D.
  Saturar o desenho não oculta o valor financeiro excedido. 100% significa limiar atingido,
  não fechamento confirmado; exibir atingido, encerrando, parcial, falhou ou encerrado.
- **Provas:** zero, limites negativos/positivos, gap além do limite, dados atrasados,
  cesta vazia, composição alterada e fechamento com remanescentes.

### PM-06 — Monte Carlo de metas e limites

- **Prioridade/dono:** P2; I-04/T20.13 com H-01/14; G-09 apresenta, sem motor paralelo.
- **Passos:** selecionar amostra encerrada/identificada → fixar cenário, horizonte e seed
  → reamostrar unidades econômicas corretas → avaliar qual barreira é atingida primeiro
  → publicar probabilidade de meta, violação e nenhuma barreira até o horizonte.
- **Aceite:** distinguir a métrica atual de DD ≥ 50% das novas regras. Declarar capital,
  meta, limites diário/global/trailing, fuso/reset, custos, número de trajetórias/amostra
  e incerteza. Sem trajetória intradiária, não declarar probabilidade de violar regra
  patrimonial intradiária a partir de somente trades fechados. Um ciclo de grade não
  vira observações independentes; dependência entre EAs precisa de amostragem compatível.
  Sem base suficiente, mostrar insuficiência, não probabilidade zero nem garantia.
- **Operação:** cálculo no serviço Python em segundo plano, cache por amostra/regra/versão,
  atualização ao fechar novas operações ou sob solicitação; nenhum Monte Carlo por tick
  ou em cada painel simultaneamente. O resultado estatístico não muda o risco sozinho.
- **Provas:** casos determinísticos de meta/perda/horizonte, seed reprodutível,
  reset/limite móvel, amostra curta, ciclos, custos já líquidos sem dupla dedução,
  cancelamento e resposta vencida descartada.

### PM-07 — Drift de execução e risco/lote

- **Prioridade/dono:** P2; T20.19/I-12 após referência T20.18 e PM-01; G-13 mostra o alerta.
- **Passos:** fixar referência por estratégia/preset/versão e escopo → observar volume,
  risco monetário/%/R, frequência, duração e custos disponíveis → separar violação
  determinística da configuração de desvio estatístico → alertar com motivo/evidência.
- **Aceite:** comparar unidades equivalentes por ativo/especificação, não lote bruto de
  símbolos diferentes. Alteração autorizada de preset muda a referência de forma registrada;
  não apaga histórico nem adapta silenciosamente a referência ao desvio.
  Mostrar amostra, janela, medida, limite, idade do dado e severidade; usar mínimo amostral,
  cooldown/histerese e verificar falsos positivos. Drift sozinho não prova perda de vantagem
  nem aciona encerramento/troca de preset; as travas de risco continuam independentes.
- **Provas:** lote/risco exagerado, volume normal com volatilidade distinta, parcial/scale-in,
  mudança de preset, amostra insuficiente, gaps e comparação de dois EAs.

### PM-08 — Favoritos e navegação segura por ativo

- **Prioridade/dono:** P1; G-04/11/14 e integração de abertura/layout.
- **Passos:** reutilizar favoritos existentes e exportação/consulta versionada → lista
  compacta no painel → focar o gráfico correspondente ou usar gráfico de análise dedicado
  → refletir ativo/TF/Expert/Magic e identidade visual após a navegação.
- **Aceite:** um clique para o percurso permitido, sem substituir o ativo de um EA com
  posições e abandonar sua gestão. Troca de símbolo/TF reinicializa o EA; o serviço precisa
  preservar contexto e capacidades ou impedir a troca nesse gráfico com motivo claro.
  Reutilizar preferências, sem segundo cadastro divergente. Símbolo inexistente, sufixo
  da corretora, indisponibilidade e atualização assíncrona possuem resposta visível.
- **Provas:** conta com símbolos diferentes, posição/pendente em gestão, gráfico já aberto,
  reconexão, alteração de TF, reinício e nenhuma nova ordem por mera navegação.
  [Comportamento de ChartSetSymbolPeriod](https://www.mql5.com/en/docs/chart_operations/chartsetsymbolperiod).

### PM-09 — Semântica de cores por estado

- **Prioridade/dono:** P1; G-11/14, dados financeiros PM-01/03 quando forem mostrados.
- **Passos:** definir tokens/legenda → verde para P&L líquido positivo, vermelho negativo,
  amarelo para faixa configurada perto do BE líquido → aplicar a marcadores/estado/linhas
  pertinentes, com identificação textual e valores; manter as cores de ação C/V reconhecíveis.
- **Aceite:** ação Comprar/Vender não troca de significado pela cor do lucro.
  Perto do BE inclui custos declarados e tolerância explícita; dado ausente/atrasado
  não fica verde. Tema, daltonismo, seleção, alerta e identidade do gráfico não se confundem.
- **Provas:** passagem pelo BE com custos, empate, P&L indisponível, tema claro/escuro,
  estabilização visual perto do limiar e várias posições/ordens do mesmo gráfico.

### PM-10 — Arraste de linhas com prévia e execução validada

- **Prioridade/dono:** P2; G-11/E-01/B-05 e dono do executor; dados PM-01/03 conforme escopo.
- **Passos:** tornar somente os objetos próprios apropriados arrastáveis → calcular prévia
  financeira identificada → ao soltar, confirmar/aplicar conforme política explícita
  → serviço revalida preço/tick/stops/freeze/volume/permissão → conferir servidor e atualizar.
  Durante o arraste, um rótulo na própria linha mostra preço, volume, ganho/perda
  monetária estimada e a base de custos usada; os detalhes completos continuam expansíveis.
- **Aceite:** mover o mouse não envia uma ordem por evento. Prévia distingue resultado
  estimado de custo efetivo/futuro. Recusa restaura o nível confirmado e informa o motivo;
  posição encerrada ou alterada durante o arraste invalida a intenção antiga.
  Respeitar monotonia/proteção e contrato de override do Expert; não contornar suas políticas.
  Não mover objetos do usuário/terceiros e não desenhar sucesso antes da confirmação.
- **Provas:** compra/venda, parcial, atualização de mercado, step e moeda, freeze/stops level,
  rejeição, desconexão, mandato revogado e conflito com trailing/BE automático.

### PM-11 — Identidade visual do gráfico: degradê e marca d'água

- **Prioridade/dono:** P1 para identidade/viabilidade visual e G-14; não depende de PM-02–07.
- **Passos:** escolher identidade persistente → derivar paleta suave → provar camadas
  de fundo atrás dos candles → desenhar degradê discreto e marca d'água → persistir/restaurar.
- **Identidade:** servidor/conta/ambiente, EA/Magic e instância durável, símbolo/TF.
  `ChartID` é endereço de runtime, não identidade persistente suficiente. Instâncias do
  mesmo EA podem compartilhar família de cor com variação controlada; cada gráfico
  precisa continuar distinguível. Permitir ajuste/restauração pelo usuário.
- **Aceite visual:** fundo com pouca saturação/contraste, coerente com o tema; marca d'água
  grande e discreta com **símbolo + TF**, identificação secundária de **EA + Magic** e estado
  legível mesmo com corpo recolhido. Não esconder candles, escalas, indicadores ou ordens;
  não disputar mouse/foco, nem confundir cor de identidade com lucro/prejuízo.
- **Viabilidade:** não presumir uma propriedade nativa de cor com degradê. Validar solução
  de canvas/bitmap/objetos e sua ordem de desenho no Wine/Windows; se o caminho não atender
  legibilidade/custo, registrar a limitação e a alternativa para aprovação do requisito,
  sem declarar degradê entregue por uma simples cor de fundo.
- **Provas:** janelas lado a lado com mesmo/diferente EA/Magic/ativo/TF, redimensionamento,
  DPI/tema, zoom, reinício, screenshots legíveis, limpeza só de objetos próprios e restauração
  do fundo anterior ao desativar. Medir memória/CPU/clique antes/depois com vários gráficos;
  não recalcular pixels do fundo por tick se nada mudou.

### PM-12 — Painel padrão incorporado aos Experts e ciclo de vida

- **Prioridade/dono:** P1; G-02/03/04 e dono do StrategyRobot/adaptadores; integração serial.
- **Arquitetura:** um componente de apresentação padrão incorporado ao Expert. O MT5
  aceita um EA por gráfico; anexar outro EA de painel removeria o primeiro. Para Expert
  externo sem fonte, eventual observador é percurso separado e somente leitura até haver
  integração e mandato próprios. Não alegar painel universal de controle de terceiros.
  [Limite oficial por gráfico](https://www.metatrader5.com/en/terminal/help/algotrading/trade_robots_indicators).
- **Passos:** identificar snapshots/ações por instância → integrar eventos/timer sem
  trocar o sinal ou duplicar ordens → iniciar compacto → persistir posição/tamanho/aba
  e preferência de fundo → preservar gerenciamento ao recolher/trocar contexto/reiniciar.
- **Aceite:** recolher a UI não pausa o robô nem suspende SL/TP/trailing/travas. Comprar/Vender,
  Finalizar e Zerar tudo ficam visíveis; ação indisponível explica sua capacidade/permissão.
  Comando manual tem intenção e autorização próprias, não dispensa risco e não substitui
  o sinal das entradas automáticas. **Finalizar** encerra a posição/ciclo selecionado conforme
  seu contrato; **Zerar tudo** cancela pendentes, fecha o escopo autorizado e pausa, preservando
  o comportamento atual e a confirmação existente. Escopo maior exige mandato explícito.
  O fechamento próprio atual é por Magic e pode abranger mais de um símbolo. Não
  relabelar silenciosamente isso como “só este gráfico”: exibir o alcance efetivo.
  Se várias instâncias compartilham Magic, é necessário distinguir leitura agregada de
  controle por instância e respeitar as guardas de exclusividade já existentes.
- **Multigráfico:** abertura/configuração no mt5x não deve apagar gráficos/desenhos dos demais
  Experts nem forçar toda abertura de volta a um único gráfico. Reconciliar essa necessidade
  com `layout_single_chart`/perfil/organização atuais; cada instância tem preferência própria.
  Editar as fontes compartilhadas somente após combinar diff e ordem com seu dono.
- **Provas:** dois/muitos EAs, Magic repetido detectado, recolher/expandir/arrastar, proteção
  durante recolhimento, troca de conta/ativo/TF, perfil reaberto, reconexão e idempotência.

## 6. Contratos de apresentação e persistência

Os campos abaixo são **requisitos para ampliar os contratos existentes**, não um DTO
paralelo já implementado: identidade/escopo e ambiente; versão do serviço/capacidade;
instante/fuso/idade; origem observada/modelada; completude e motivos; moeda/unidades;
realizado/flutuante/custos; membros e revisão da cesta; estado da intenção/execução;
permissões e motivo de recusa; hash/versão da configuração aplicada.

O painel lê dados e solicita ações; serviço/guardas revalidam permissões e execução.
Python/ponte permanece no seu contrato de leitura, sem novo `order_send` ou import de
MetaTrader5 fora do backend. Não criar serviço HTTP de negociação por causa deste painel.
Persistência é aditiva, compatível com bases antigas; novas chaves de configuração,
quando implementadas, entram em DEFAULTS e no exemplo, com fallback. Segredos não entram
no gráfico, parâmetros expostos, logs ou configuração versionada.

Representar estados distintos: **Disponível**, **Bloqueado com motivo**, **Dados insuficientes**,
**A desenvolver**, **Calculando**, **Dado vencido**, **Executando**, **Parcial/Falhou** e
**Confirmado**. Ausência não é zero; expansão não é habilitação; observar não é gerir.

## 7. Ordem de execução sem bloquear o painel inteiro

1. **Fundação:** PM-00/G-02, inventário de preservação e contratos de UI/capacidades.
   G-02 conserva seu aceite sem mudança de comportamento; a aplicação do design vem em
   tarefa posterior, sem misturar modularização, nova execução e nova contabilidade.
2. **Painel aprovado e identidade:** compacto/abas/expansão, PM-12 e PM-11 por recortes
   próprios; ligar somente ações atuais e os resumos confiáveis. PM-08/09 podem seguir
   após seus aceites de navegação e semântica. Não esperar cestas/Monte Carlo/drift.
3. **Analytics isolada:** PM-01 → PM-02. Apresentação pode usar fixtures identificadas
   para QA, sem chamá-las de integração ao vivo.
4. **Cestas:** PM-01 + permissões/reconciliação → PM-03 → PM-05; PM-04 tem validação e PR
   próprios. Prévia/observação da cesta não concede sua execução automática.
5. **Probabilidade e diagnóstico:** PM-01 + referência válida → PM-06/07 nos respectivos
   donos estatísticos. As duas atividades não são pré-requisito da nova boleta.
6. **Arraste operacional:** PM-10 após contrato de override/executor e dados do escopo.
   Avaliar o percurso próprio antes de estender para cestas e mandatos de terceiros.

Cada PR identifica base M/H/S e dono, arquivos, requisito PM coberto e provas compatíveis.
Reutilizar evidência existente quando a fonte/capacidade forem as mesmas; não duplicar
workflow/testes nativos. MT5/compilação/Tester usam a coordenação e `mt5_tools.tester_lease()`
existentes, sessão privada e somente encerramento de processos próprios identificados.
Windows físico permanece pendência registrada em `docs/validacao_instaladores.md`, sem
bloquear aceites Linux/Wine nem ser declarado validado por simulação ou CI.

## 8. Referências de implementação inspecionadas

- M: [carteira](../../mt5_extracao/robots/portfolio.py),
  [status por Magic](../../mt5_extracao/robots/live.py),
  [ledger](../../mt5_extracao/trade_analytics/ledger.py),
  [exportação nativa por sessão](../../mt5_extracao/robots/templates/session_export.mqh.tmpl),
  [equity](../../mt5_extracao/strategies/equity.py),
  [Monte Carlo atual](../../mt5_extracao/strategies/validation.py),
  [favoritos](../../mt5_extracao/asset_table.py),
  [abertura](../../mt5_extracao/robots/view.py) e [organização](../../mt5_extracao/robots/layout.py).
- H, base `596f2c8`: `Safety/HDK_GuardianPanel.mqh` (`CHDKGuardianPanel`/`CHDKChartMarks`),
  `Safety/HDK_PanelUi.mqh`, `Safety/HDK_TradeStats.mqh`, `Safety/HDK_SafeExecutor.mqh`,
  `Experts/HADOUKEN/HDK_HADOUKEN_Painel.mq5` e `Strategies/HDK_StrategyRobot.mqh`.
  [Fontes Safety fixadas na base](https://github.com/Misael-art/projeto-indicadores-mt5/tree/596f2c8ce132789095d6b718bd75bd46cecabd08/Include/Framework/HADOUKEN/Safety).
- S, base `dbff0d2`, VERSION 1.7.0:
  [fonte distribuída](https://github.com/Misael-art/mt5x-safety/tree/dbff0d2f12b42e5777d245007e5136bba4e416cf/Include/MT5X/Safety).
- Planejamento existente: [pilares F19](PILARES_DADOS_OPERACAO_PRESETS.md),
  [fichas F19](PLANO_F19_PILARES_PRESETS.md), [fichas F20](PLANO_F20_ROBUSTEZ.md),
  [épico I](EPICO_I_PESQUISA_VALIDACAO.md), [índice A–J](README.md) e
  [UI da aplicação](UI_ORGANIZACAO_GRAFICOS_PRIORIDADES.md).
  Em H: `Documentation/HADOUKEN_2/propostas/EPICO_{E,F,G,H}_*.md`.

## 9. Registro e integração documental

Entrega local em worktree própria, sem alterar WIP dos clones principais ou fontes de
runtime. O mockup aprovado é preservado como referência; nenhuma imagem nova modifica
o design validado. Não há PR de implementação nem declaração de funcionalidade entregue.

### Verificação desta entrega

- Ambiente preparado com `./install.sh --yes --no-mt5` e dependências de desenvolvimento.
  Os atalhos do instalador foram apontados novamente para o clone principal, evitando
  deixar a aplicação do usuário vinculada à worktree documental.
- Suíte basal iniciada antes da documentação. Por orientação persistente do proprietário,
  transmitida pela coordenação, a repetição foi encerrada somente no processo próprio:
  **690 aprovados e 33 pendências existentes em 701,74 s**, saída 2 por SIGINT.
  Esse resultado parcial **não** é declarado como passe da suíte completa; log preservado.
- Evidência completa reutilizada, informada pelo integrador:
  [workflow 39471926683](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/actions/runs/39471926683),
  seis checks aprovados em `a9c2b66`, Ubuntu 1.895, Windows CI 2.012 e PostgreSQL 52.
  A conferência local de `git diff --name-only a9c2b66 b139f04` retornou somente sete
  documentos; o código de execução é o mesmo. Não foi criado workflow adicional.
- Após adicionar os artefatos, `pytest -q tests/test_robots_portfolio.py tests/test_sdk_docs.py`:
  **10 aprovados em 2,61 s**. Nenhum teste foi alterado, desativado ou adicionado.
- Conferidos **17 links locais**, presença das 13 fichas, identidade da imagem aprovada
  e ausência de alterações em arquivos rastreados preexistentes. SHA-256 do PNG:
  `3666c3a6882125ae3ec1f06bf04c1e6251b7f6d8e2c27813861225a4f2fb638f`.
- Doctor: **16 OK, dois avisos, zero falhas**; avisos de pandas-ta opcional ausente e
  ponte desativada no ambiente preparado com `--no-mt5`. Nenhum acesso nativo executado.
- Shellcheck não se aplica: a entrega adiciona somente Markdown e PNG. Windows físico,
  comportamento visual no terminal e funcionalidades planejadas não foram validados aqui.

### Trechos para o responsável pela integração serial

Adicionar vínculo na linha T12.6b/c do painel de status, preservando estado/dependências:

> Design do painel único aprovado em 2026-10-20; cobertura PM-00–PM-12 documentada em
> `propostas/PAINEL_MODULAR_COBERTURA.md`. G-02 continua fundação; analytics ao vivo,
> cestas/Close By, Monte Carlo de metas e drift têm recortes próprios. Runtime não iniciado.

Acrescentar na continuidade, sem reescrever aceites anteriores:

> Painel modular: layout de fichário/compacto/expansão aprovado pelo proprietário.
> Inventário estático M b139f04/H 596f2c8/S dbff0d2 e atividades PM-00–PM-12 registrados.
> Painel padrão incorporado a cada EA; identidade por gráfico com degradê e marca d'água;
> serviços faltantes não bloqueiam o núcleo. Sem MT5/compilação/runtime nesta entrega.
> Integrar este registro na fila documental, uma tarefa por PR.

No índice de propostas, incluir:

> [Painel modular — design aprovado, cobertura e atividades](PAINEL_MODULAR_COBERTURA.md).

Esses trechos são preparados para o integrador; não se apresentam como já aplicados aos
arquivos centrais compartilhados. Registro não duplica as fichas dos donos nem altera
os estados históricos de integração F19/F20.
