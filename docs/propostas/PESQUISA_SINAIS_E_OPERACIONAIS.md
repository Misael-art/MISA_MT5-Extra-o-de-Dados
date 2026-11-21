# Plano de pesquisa de sinais, operacionais e presets

Registro de 20/11/2026 com **17 fichas planejadas**. Este plano detalha os recortes de F13/F16/F19/F20 no [roadmap](../ROADMAP.md) e no [painel de trabalho](../PLANO_DE_TRABALHO.md). A fila corretiva P0 e as dependências de segurança e dados do projeto mantêm precedência. A primeira entrega desta frente é F19-PESQ-01. Cada ficha referencia a tarefa já existente que fornece sua capacidade.

## Como retomar uma ficha

1. Consulte [a diretriz de Dados](../dados/DIRETRIZ_BASE_ESTRATEGICA.md), [o plano F19](PLANO_F19_PILARES_PRESETS.md), [as fichas F20](PLANO_F20_ROBUSTEZ.md) e [o handoff](../../CONTINUAR_AQUI.md). Confira branch, revisão do App, pins HADOUKEN/Safety e aceites aplicáveis. Os resumos de vídeos fornecem hipóteses. Seus números são parâmetros de réplica ou exemplos aritméticos.
2. Localize a linha no painel de trabalho. Escolha uma ficha com pré-requisitos demonstrados. Quando houver PR ou implementação da tarefa-mãe, acrescente o caso a ela ou referencie o aceite existente. Registre estado, responsável pela mudança, identificador do protocolo, PR/SHA e lacuna remanescente na mesma linha. Uma ficha pode resultar em incrementos por repositório, seguindo uma tarefa revisável por PR.
3. Registre previamente o braço de referência, a variante e o resultado que mudaria a decisão. Use o ambiente e o executor oficiais, um job por vez e o gate de recursos do host. Use fixtures controladas e anonimizadas para regressão, base de pesquisa identificada para a hipótese e Tester DEMO isolado para execução nativa.
4. Anexe recibo com versão do código/inputs, fonte, janela e fuso, cobertura, capacidades ausentes, custos, contagens, diferença em relação à referência, incerteza e veredito. Diferencie **implementado**, **verificado no Tester**, **evidência econômica suficiente**, **inconclusivo** e **reprovado**. A reprovação bem documentada encerra a hipótese. Abrir nova tentativa exige nova versão e registro prévio.

## Mapa da evidência e dos componentes

| Origem do insight | Hipóteses a preservar | Fichas |
|---|---|---|
| Experts A, Open Strike | Banda EMA10, candle inicial M15, ordem stop, prazo e paridade entre plataformas | 01, 03, 07 |
| Experts B, ORB Pro | Caixa de abertura, quatro limites de largura/distância, direção e saídas combináveis | 04–06 |
| Experts C, FVG | Zona de três velas, meio da vela versus meio da zona, expiração e grade limitada | 10, 11 |
| Experts D, Price Action | Reteste, mudança de estrutura, comparação com níveis aleatórios, confirmação, sweep, horário, custos por timeframe e reserva | 03, 08, 09 |
| Operacional A, médio para frente | Piramidação, reposição, alvo principal e reconciliação de estado | 11, 12 |
| Operacional B, hedge | Duas pontas, agenda, modo de conta e margem conjunta | 11, 15 |
| Operacional C, harmônico | Escala logarítmica, aporte fixo, estoque remanescente e patrimônio | 16 |
| Operacional D, entrada dinâmica | Bollinger na abertura do ciclo, grade fixa depois do preenchimento | 11, 13 |
| Operacional E, assimétrico | Retorno pela banda, vetor de lotes por nível, âncora e stop iniciais | 11, 14 |
| Gerador de presets | Execução condicionada ao sinal, contrato e evidência exigida por finalidade | 02, 03, 17 |

- **Dados e padrões.** Reutilizar [estrutura da sessão](../../mt5_extracao/analytics/session_structure.py), [fidelidade dos ticks](../../mt5_extracao/analytics/tick_fidelity.py), [escopo do importador B3](../dados/B3_PUBLICO.md), [pivôs causais](../analytics/T19.6_PIVOS_CAUSAIS.md) e [fluxo de avaliação de padrões](../../mt5_extracao/analytics/pattern_flow.py).
- **Sinais e execução.** Usar [ORB Python](../../mt5_extracao/strategies/orb.py), [indicadores e Bollinger](../../mt5_extracao/strategies/indicators.py), [BB/RSI](../../mt5_extracao/strategies/bb_rsi.py), [modos do Laboratório](../../mt5_extracao/trade_analytics/execution/modes.py), [motor de execução](../../mt5_extracao/trade_analytics/execution/engine.py) e [contrato de ciclos](../analytics/T19.14_TRAJETORIAS_TEMPORAIS.md). Na composição fixada, localizar `Experts/HADOUKEN/Estrategias/HDK_Orb/HDK_Orb.mq5` no HADOUKEN e `Include/MT5X/Safety/HDK_RiskGuard.mqh` no Safety.
- **Experimentos e presets.** Reutilizar [registro de experimentos](../../mt5_extracao/experiments/service.py), [validação temporal](../../mt5_extracao/strategies/validation.py), [preset de pesquisa](../../mt5_extracao/presets/research.py), [compilador](../../mt5_extracao/presets/compiler.py) e [política condicional](../../mt5_extracao/presets/conditional_policy.py). Estender o registro de capacidades da revisão efetiva antes de exportar uma nova regra.
- **Regressões de referência.** Partir dos testes de [fidelidade](../../tests/test_tick_fidelity.py), [famílias](../../tests/test_pattern_families.py), [ciclos](../../tests/test_trajectory_cycles.py), [protocolo PF19](../../tests/test_pf19_study.py), [compilação de presets](../../tests/test_preset_compiler.py) e [Ultra condicional](../../tests/test_conditional_policy.py). Os testes de cada ficha devem exercitar seu caminho normal e preservar o comportamento dos presets anteriores.

## Fichas de execução

As dependências numéricas abaixo usam o prefixo F19-PESQ. Os identificadores T remetem às fases já existentes. A qualificação nativa de cada variante pode avançar quando a respectiva capacidade estiver disponível, sem esperar as outras famílias de estratégias.

<a id="f19-pesq-01"></a>

### F19-PESQ-01 — Dados e sessão do WIN

**Responsável** — Dados/Analytics. **Pré-requisitos locais** — nenhum. **Integração** — T13.1–3 e T20.4–6.

**Objetivo.** Distinguir sessão completa, sessão especial e lacuna por vencimento antes de construir a caixa de abertura.

**Implementação.** Inventariar fonte, contrato, rolagem, calendário vigente, fuso servidor/bolsa, leilão quando identificável e origem de bid/ask/last. Registrar início esperado/observado, cobertura M1/ticks, lacunas e motivo de exclusão. Calcular gap anterior, largura em pontos/ticks e ATR anterior. Cortes e percentis usam apenas o treino. O importador B3 agrega negócios recentes. Qualificar spread, agressor e fila separadamente.

**Aceite e evidência.** Fixture com abertura completa, primeiro M1 às 09h07, minuto interior ausente, sessão especial e troca de vencimento. A faixa 09h00–09h15 só é publicada após fechar a última barra elegível. Dia incompleto sai do teste com motivo. Entregar manifesto de cobertura por pregão, incluindo ausências.

<a id="f19-pesq-02"></a>

### F19-PESQ-02 — Livro de oportunidades e ciclos

**Responsável** — App/Analytics e Laboratório. **Pré-requisitos locais** — 01. **Integração** — T13.4, T16.1/3 e T19.14.

**Objetivo.** Medir o denominador completo de sinais e seguir cada operação até a exposição voltar a zero.

**Implementação.** Relacionar oportunidade, decisão, veto, expiração, pedido, execução parcial/total, saldo e encerramento. Guardar disponibilidade, lado, variante, exposição por perna, preço solicitado e efetivo, tempo até preencher e custo por execução. Medir MAE/MFE, instante dos extremos, perda flutuante, margem máxima, pior sessão, tempo em drawdown e concentração. Reusar o livro e o esquema de trajetórias com compatibilidade retroativa.

**Aceite e evidência.** Percorrer sinal vetado, expirado, rejeitado e executado em partes com redução e zeragem. Conciliar contagens por oportunidade, ordens e ciclos e o caixa após custos. Sessões sem trade ficam no denominador. Lacunas recebem motivo. Reprocessar os mesmos eventos produz os mesmos totais.

O percurso Python grava sinal, disponibilidade, decisão, motivo e vínculo ao ciclo
no journal de trajetórias existente. `mt5x --json trades opportunities --run <id>`
consulta o livro. Com o calendário da F19-PESQ-01, a opção `--calendar <arquivo>`
inclui os pregões sem sinal nas barras avaliadas e as sessões inelegíveis. O JSON
inclui preço de referência antes do slippage, fills modelados, comissão, swap,
exposição restante, extremos monetários observados e saldo líquido do ciclo.
Uma consulta de execução legada retorna indisponibilidade explícita se o
produtor não tiver gravado oportunidades.

Na bancada pelo launcher Linux, uma execução sintética preservou 3 oportunidades,
1 veto, 1 expiração e 1 ciclo encerrado com 3 fills e saldo de 40 na moeda do
modelo. Outra execução devolveu dois pregões com abertura elegível e zero sinais.
Rejeição nativa, entrada parcialmente preenchida, margem e exposição por perna
exigem eventos de produtor com essas capacidades. O modelo atual executa a entrada
integral e registra reduções posteriores. A consulta também informa concentração
dos cinco maiores ganhos, maior perda observada em um ciclo e pior pregão elegível
por data do sinal. A duração medida em drawdown soma intervalos entre fechamentos
submersos adjacentes da curva de patrimônio unitizada. Uma lacuna de amostragem
interrompe essa soma. A duração intrabarra continua indisponível.

<a id="f19-pesq-03"></a>

### F19-PESQ-03 — Protocolo de comparação

**Responsável** — App/Experimentos. **Pré-requisitos locais** — 01, 02. **Integração** — T13.4/10, T19.27 e T20.9/12–15/18.

**Objetivo.** Congelar hipótese, referência e decisão econômica antes de abrir a reserva.

**Implementação.** Registrar símbolo, período, versões, parâmetros, orçamento e todas as tentativas. Separar descoberta, validação móvel e reserva inédita. Purgar rótulos cruzando a fronteira e aplicar embargo justificado pelo horizonte. Parear controles por lado, sessão, volatilidade e distância. Separar efeito do sinal, filtro e saída. Reexecutar a cadeia quando custos, duração, preenchimento ou margem alterarem as entradas seguintes.

**Aceite e evidência.** Persistir protocolo e hash antes de calcular. Demonstrar rejeição de característica conhecida após a decisão e impedimento de qualificar reserva já explorada. Publicar diferenças pareadas por blocos de pregões, inclusive dias sem operação. Declarar multiplicidade, sensibilidade a custos e incerteza. Aplicar os critérios da seção de promoção.

<a id="f19-pesq-04"></a>

### F19-PESQ-04 — ORB de referência e paridade de abertura

**Responsável** — App/Estratégias e HADOUKEN. **Pré-requisitos locais** — 01. **Integração** — T13.3, T19.17/20/23 e T20.8/11.

**Objetivo.** Reproduzir a semântica do ORB atual nos dois motores e qualificar a sessão antes de mudar os filtros.

**Implementação.** Registrar primeiro fechamento fora da faixa, lado oposto como stop, alvo múltiplo da faixa, filtro mínimo ATR e zeragem. Conferir os padrões de 30 min e 1,5×. Os dois fontes partem da primeira barra presente. Delimitar janela pela sessão escolhida e escolher se histórico parcial gera inelegibilidade. Preservar a interpretação dos presets históricos e versionar a nova semântica.

**Aceite e evidência.** Fixture idêntica no Python e no MQL5 com dia completo, atrasado e sem rompimento. Comparar horário, máximas/mínimas, direção, ordem, veto de margem e caixa. Registrar a primeira divergência. Aceite nativo em Tester DEMO identificado. Diagnóstico por fixture e aceite do Tester têm registros próprios.

<a id="f19-pesq-05"></a>

### F19-PESQ-05 — ORB com caixa e distância filtradas

**Responsável** — App/Estratégias. **Pré-requisitos locais** — 02, 03, 04. **Integração** — T19.20/21/24 e T20.15.

**Objetivo.** Estimar o que os filtros contribuem sobre o ORB nas mesmas sessões e com a mesma gestão.

**Implementação.** Congelar caixa de 15 min, rompimento por fechamento M1 e primeiro evento elegível. Ensaiar largura mínima/máxima (200/750 pontos no relato) e deslocamento além da borda (10/400). Comparar fixo com normalização por ATR conhecido. Tratar 09h e 10h e as direções como variantes distintas. Definir inclusividade das bordas e se o primeiro rompimento excessivo consome o dia.

**Aceite e evidência.** Testar extremos exatos, rompimento seguido de retorno e abertura incompleta. Relatar aceitos, bloqueados, perdas evitadas, ganhos descartados e diferença de PnL por pregão elegível. O saldo de poucas semanas em OHLC não promove o filtro.

<a id="f19-pesq-06"></a>

### F19-PESQ-06 — Saídas do ORB

**Responsável** — Laboratório/Composição e HADOUKEN. **Pré-requisitos locais** — 05. **Integração** — T16.7, T19.18/19/23/56/57 e T20.11.

**Objetivo.** Isolar a contribuição de stop, alvo por faixa, parcial, break-even e trailing.

**Implementação.** Com as entradas congeladas, comparar stop oposto versus 500 pontos. Testar separadamente alvo de 1,4× a faixa, BE em 0,8×, parcial em 1× e trailing com gatilho/distância/passo de 600/500/100 pontos. Definir prioridade no mesmo tick, custo do BE, mínimo e passo do volume, zeragem às 17h para a réplica e risco monetário comparável. Reexecutar a composição completa ao final.

**Aceite e evidência.** Fixture de faixa de 500 pontos com dois WIN e saída de um a +500 e outro a +700 deve produzir R$ 240 brutos após ambas as saídas. Com ambos a +700, R$ 280. Cobrir volume indivisível, conflito intrabarra e veto de margem. Reconciliar volumes, caixa e retentativa no Expert que declare capacidade.

<a id="f19-pesq-07"></a>

### F19-PESQ-07 — Open Strike e validade da pendente

**Responsável** — App/Estratégias e HADOUKEN. **Pré-requisitos locais** — 01, 02, 03. **Integração** — T19.17/20/23/29 e T20.9/11.

**Objetivo.** Distinguir o valor do primeiro M15 do valor de aguardar o rompimento do extremo.

**Implementação.** Primeiro M15 completo e direcional acima/abaixo da Bollinger 10/1,0 com centro EMA atribuído pelo relato. Fixar preço, inicialização e definição do desvio. Confrontar valores de referência com o cálculo SMA existente. Comparar mercado no próximo preço negociável versus stop a um tick do extremo. Especificar preço de disparo, validade inicial de um candle, cancelamento na virada, uma entrada por sessão, janela 09h–17h, zeragem 17h30, alvo 600 e stop 1.000 pontos. Tratar stop-limit do Profit como variante.

**Aceite e evidência.** Testar simetria, candle neutro, igualdade na banda, virada, gap, não preenchimento e corrida entre fill e cancelamento. Conferir bandas e primeira divergência de execução antes do PF. A referência binária integral 600/1.000 tem equilíbrio bruto de 62,5%. Reconciliar diferenças de saídas por horário, slippage e operações faltantes.

<a id="f19-pesq-08"></a>

### F19-PESQ-08 — Primeiro reteste e polaridade

**Responsável** — App/Analytics e Estratégias. **Pré-requisitos locais** — 01, 02, 03. **Integração** — T13.4, T19.6/43 e T20.15.

**Objetivo.** Avaliar se o primeiro reteste de um nível rompido produz vantagem após custos.

**Implementação.** Congelar pivô confirmado, quebra, tolerância, instante da primeira revisita, contagem prévia de toques e horizonte. Parear níveis de controle por distância, horário, lado e volatilidade. Comparar entrada no toque e após confirmação, stop físico e saída ao fechamento. Registrar separadamente efeitos de horário, timeframe e sequência prévia de três dias.

**Aceite e evidência.** Deslocar um pivô futuro no fixture não pode mudar decisões passadas. Distinguir primeiro e quarto toques. Relatar risco/tempo exposto, custo do atraso e perda de cauda. Apresentar controles e resultados por mercado. Os relatos NQ/ES originam hipóteses para WIN/WDO.

<a id="f19-pesq-09"></a>

### F19-PESQ-09 — CHoCH em H4

**Responsável** — App/Analytics e Estratégias. **Pré-requisitos locais** — 01, 02, 03. **Integração** — T13.4, T19.6/43 e T20.15.

**Objetivo.** Resolver a ambiguidade entre um fechamento contrário e uma quebra de estrutura confirmada.

**Implementação.** Registrar duas regras, a primeira mudança de direção do fechamento e fechamento que rompe swing confirmado contra a estrutura anterior. Declarar o relógio H4 por mercado, disponibilidade do swing, entrada no preço posterior, hard stop além do swing e alvo de 1R. Testar com/sem sweep e os filtros de sessão e esticamento como hipóteses identificadas.

**Aceite e evidência.** Fixture em que a cor do candle muda sem cruzar swing e outra em que há rompimento. As variantes precisam divergir no evento previsto. Confrontar controles pareados e medir estabilidade entre períodos e ativos sem usar o resultado de um para calibrar a reserva do outro.

<a id="f19-pesq-10"></a>

### F19-PESQ-10 — Fair Value Gap como evento e entrada

**Responsável** — App/Analytics e Laboratório. **Pré-requisitos locais** — 01, 02, 03. **Integração** — T13.4, T19.17/21 e T20.6/9.

**Objetivo.** Testar a zona antes de combinar sua detecção com aumento de posição.

**Implementação.** Para três barras fechadas A/B/C, alta se mínima(C) > máxima(A), baixa no espelho. Distinguir meio do intervalo e meio da vela B. Para a variante do relato, exigir centro de B entre extremos, direção de B e corpo/amplitude ≥50%, com amplitude positiva. Congelar validade de cinco barras, regra da sexta, sobreposição e virada. Medir toque na borda, profundidade, tempo ao retorno, violação e preço executável. Medir microestrutura só com dados elegíveis.

**Aceite e evidência.** Fixtures de alta/baixa, amplitude zero, centro da vela fora da zona, toque sem fill e expiração. Nenhum sinal antes do fechamento de C. Comparar borda, meio da zona e meio da vela contra controle pareado. Grade dentro da zona depende de F19-PESQ-11 e requer ensaio próprio com risco total igual.

<a id="f19-pesq-11"></a>

### F19-PESQ-11 — Contrato comum de grade e risco do ciclo

**Responsável** — Laboratório/Composição, HADOUKEN e Safety. **Pré-requisitos locais** — 01, 02, 03. **Integração** — T12.4/5, T16.8, T19.22/23/56/57 e T20.7–11.

**Objetivo.** Habilitar uma comparação de entrada direta, grade fixa contra, ATR contra e direcional a favor sob o mesmo risco total.

**Implementação.** Inventariar capacidades por Expert/revisão. Definir âncora, preço em ticks inteiros, direção, espaçamento, lote por nível, número de níveis, pendentes, risco monetário do ciclo, margem e zeragem. O motor consultado multiplica o lote inicial por um fator único e condiciona parcial à ausência de grade. Declarar combinação incompatível até haver execução comprovada. Manter os tetos por ciclo, dia e carteira no Safety.

**Aceite e evidência.** Cobrir gap de vários níveis, barra ambígua em OHLC, ordem parcial ou fora de ordem, spread alargado, falta de margem e reinício. Reconciliar posição, pendentes, saldo e custo por fill em netting/hedging conforme suporte. Seis compras de um WIN a 50 pontos entre níveis acumulam R$ 150 de perda bruta no sexto nível. Mais 50 pontos contra levam a R$ 210. A margem tem verificação própria.

<a id="f19-pesq-12"></a>

### F19-PESQ-12 — Médio para frente e reposição

**Responsável** — HADOUKEN e Laboratório. **Pré-requisitos locais** — 11. **Integração** — T19.22/23/43 e T20.11.

**Objetivo.** Medir o efeito incremental de reapregoar um degrau atrás depois do preenchimento de uma ordem stop a favor.

**Implementação.** Comparar entrada única, piramidação existente e piramidação com reposição via limit. Congelar alvo principal da primeira entrada, alvo por nível e regra de interrupção das adições. Guardar extremos antes de mudar a posição selecionada. Identificar intenção por ciclo, lado, nível e geração. Relacionar ordem e negócio com preço pretendido e preço executado. Reconciliar no reinício antes de emitir outra ordem.

**Aceite e evidência.** Repetir evento, aplicar slippage e preenchimento parcial e embaralhar confirmações. Cada quantidade confirmada admite no máximo a reposição correspondente, nenhum nível além do alvo, zero pendentes residuais após encerramento confirmado. Medir ganho incremental líquido, giro e pior reversão comparados à piramidação simples.

<a id="f19-pesq-13"></a>

### F19-PESQ-13 — Banda dinâmica com âncora congelada

**Responsável** — App/Estratégias, Laboratório e HADOUKEN. **Pré-requisitos locais** — 11. **Integração** — T19.17/21/22/43.

**Objetivo.** Testar se usar a banda para abrir o ciclo melhora a entrada mantendo fixo o passo após o fill.

**Implementação.** Replicar SMA20 e bandas a dois desvios. Definir quando atualizar ou cancelar a primeira pendente, se a âncora usa banda, pedido ou primeiro fill e qual evento a congela. Aplicar o filtro da média ao início. Gestão do ciclo continua após o cruzamento da média. Avaliar exclusão da primeira barra para esta reversão. Comparar separadamente âncora móvel pré-entrada e espaçamento por ATR.

**Aceite e evidência.** Fixture move a banda entre sinal e fill, depois cruza a média com posição aberta. A âncora e o passo ficam reproduzíveis até zerar e nenhum filtro impede saída protetiva. Relatar múltiplos níveis na mesma barra como ambíguos quando a fonte não permitir ordená-los.

<a id="f19-pesq-14"></a>

### F19-PESQ-14 — Gradiente assimétrico

**Responsável** — App/Composição, Laboratório e HADOUKEN. **Pré-requisitos locais** — 11. **Integração** — T19.22/43/56/57 e T20.8–10.

**Objetivo.** Identificar se a melhora atribuída ao vetor de lotes permanece quando o risco permitido é idêntico.

**Implementação.** Formalizar o cruzamento de retorno da Bollinger nos dois lados. Guardar vetor explícito incluindo o lote inicial. A transcrição deixa a fronteira de alguns níveis 1/2/3 ambígua. Ancorar degraus e stop no preço inicial e alvo no preço médio. Unificar estado do ciclo quando a conta for netting. Calcular perda projetada com toda a grade e custos antes de admitir o ciclo.

**Aceite e evidência.** Comparar vetor uniforme e escalonado com o mesmo teto de perda e margem. Fixtures de ramo comprado/vendido, nível intermediário, posição completa, gap além do stop e reset após zeragem. Relatar frequência de grade cheia, perda média nos piores ciclos e benefício líquido após normalizar exposição.

<a id="f19-pesq-15"></a>

### F19-PESQ-15 — Hedge bidirecional

**Responsável** — HADOUKEN/Safety e Laboratório. **Pré-requisitos locais** — 11. **Integração** — T12.1/2, T19.22/48 e T20.7–11.

**Objetivo.** Avaliar duas grades simultâneas como carteira de duas pernas.

**Implementação.** Identificar o modo da conta. No modo hedging, rastrear tickets, magic e lotes por perna, limites financeiros do ciclo e da carteira, janela de abertura, perna remanescente e carregamento entre pregões. No netting, o mesmo símbolo tem posição líquida. Marcar indisponível a variante que exige duas posições opostas simultâneas. Usar a lógica auditável do projeto. A planilha de margem de BRA50/CFD e a biblioteca fechada relatadas não parametrizam WIN.

**Aceite e evidência.** Comparar só compra, só venda e as duas pontas com capital e limites iguais. Cobrir fechamento isolado de uma perna, duas requisições simultâneas, rejeição e margem correndo contra. Conciliar volumes bruto/líquido, custos de ambas as pontas e máxima perda flutuante. Sem conta compatível, manter hipótese de laboratório.

<a id="f19-pesq-16"></a>

### F19-PESQ-16 — Gradiente harmônico à vista

**Responsável** — App/Analytics de carteira e Laboratório. **Pré-requisitos locais** — 01, 02, 03. **Integração** — T19.14/56 e T20.4–9.

**Objetivo.** Comparar o patrimônio do investidor, incluindo estoque retido e caixa disponível.

**Implementação.** Para N intervalos e 0 < L < H, usar Pᵢ = H × (L/H)^(i/N), i de 0 a N. Declarar quais N níveis recebem orçamento, valor por parcela, lote negociável, regra de venda de unidades e saldo retido. Tratar execução parcial, taxas, proventos/desdobramentos, série ajustada para análise e preço de negociação. Comprar e manter e aportes parcelados usam o mesmo caixa inicial, calendário e fluxos externos.

**Aceite e evidência.** Conferir N+1 fronteiras, razão percentual constante, arredondamento sem caixa negativo e patrimônio = caixa + estoque marcado. Incluir caso abaixo do piso, iliquidez e ação corporativa. Comparar patrimônio final, drawdown, giro, caixa ocioso e unidades. Manter derivativos alavancados fora desta hipótese à vista.

<a id="f19-pesq-17"></a>

### F19-PESQ-17 — Gerador, paridade e promoção por candidato

**Responsável** — App/Presets e HADOUKEN. **Pré-requisitos locais** — 02, 03 e ficha candidata. **Integração** — T19.23–29/50/52–55 e T20.11–19.

**Objetivo.** Levar cada candidato elegível do estudo ao gerador, ao Tester e à observação prospectiva.

**Implementação.** Preservar Default como referência e Manual como definição explícita. Vincular versão do sinal, origem e revisão do Expert, modo de conta, unidades, validade, nível/âncora, custos, limites e precedência de stop/parcial/BE/trailing/zeragem. Defensivo declara o objetivo de cauda e tolerância de perda de expectativa antes da reserva. Maximalista (Fator de lucro) usa critérios econômicos declarados. Ultra compara seleção condicional à melhor política estática escolhida no treino e explicita contexto pouco amostrado.

**Aceite e evidência.** Reabrir pacote no gerador, comparar hash/inputs com o .set e reconciliar sinal, veto, ordem, fills, posição e caixa no Tester DEMO. Manter preset de pesquisa quando faltar capacidade ou evidência. Testar rejeição de versão incompatível, leitura de pacote anterior e invalidação após mudança material. Acompanhar sem ordens em dados prospectivos. Concluir separadamente por candidato, inclusive reprovado.

## Regras de avaliação econômica e promoção

- **Amostra e unidade.** Guardar oportunidades, ciclos e dias em camadas distintas. Calcular PF no conjunto de ciclos líquidos reconciliados, somando ganhos e dividindo pelo módulo da soma das perdas. Sem perdas, o PF fica indefinido. Separar PnL realizado e patrimônio marcado. Parciais de um ciclo pertencem ao mesmo risco. Comparar também expectativa por oportunidade/pregão, drawdown de patrimônio, pior perda do ciclo/dia, margem, exposição e concentração. Medir a correlação entre estratégias de abertura quando disputam o mesmo orçamento.
- **Controle da pesquisa.** Fixar regime, percentis, seleção de parâmetros e controles no treino. Ajustar para múltiplas hipóteses por família e divulgar quantas variantes foram experimentadas. Em buscas amplas, considerar Sharpe deflacionado quando a distribuição dos retornos e o número efetivo de tentativas forem estimáveis. Usar reamostragem por blocos de pregões com os mesmos dias nos braços, informar sensibilidade ao bloco, réplicas sem perdas e exclusão do melhor pregão. Dados já expostos seguem exploratórios. Se faltar reserva íntegra, abrir observação prospectiva sem ordens.
- **Critério PF19.** O [protocolo original](../pesquisa/PF19_PROTOCOLO_20261016.json) rege qualquer alegação de PF19. Exige reserva inédita, custos/moeda verificados, ao menos 200 operações fechadas, 60 sessões com operações e 30 perdas, PF ≥1,9, limite inferior do IC95% do PF >1, expectativa líquida positiva, drawdown ≤20%, participação dos cinco maiores ganhos ≤50% do ganho bruto, ao menos três trimestres positivos e estabilidade a custos, parâmetros e atraso. Considerar patrimônio intraposição e margem. Indisponibilidade limita a aprovação. Uma estratégia de uma entrada por dia precisa de pelo menos 200 pregões com execução para satisfazer a contagem. O [estudo anterior](../pesquisa/PF19_RELATORIO_20261016.md) reprovou oito casos de EURUSD EMA/ATR e Donchian. O melhor PF pontual foi 0,8758 e o respectivo cenário adverso, 0,7205. Essas observações fornecem referência de método.
- **Preço e capacidade de execução.** Usar valor do ponto, tick mínimo, volume permitido, comissão, spread e relógios do contrato vigente. Comparar OHLC, ticks gerados e ticks observados identificando a cobertura. Na falta de sequência intrabarra, medir limites favorável/adverso para stop, alvo e adição. Se a conclusão mudar, classificar como inconclusiva. Tocar uma limit não demonstra preenchimento em fila. Considerar slippage, atraso, rejeição, gaps e liquidez no lado apropriado. Reexecutar a cadeia para o cenário adverso.
- **Decisão no ciclo de vida.** Em cada comparação guardar referência, estimativa e intervalo, impactos em capital e perda de cauda, custo de oportunidade, limitações, veredito e revisão. Registrar conflitos de paridade por evento e impedir promoção automática se houver divergência material, reserva comprometida ou risco não quantificável. Os critérios do Defensivo e do Ultra devem estar fixados antes da reserva. A comparação entre os três objetivos conserva as diferenças de exposição e frequência.

### Parâmetros exploratórios e limites do relato

O material de Open Strike sugere variar validade de 1 a 5 barras, período de banda de 5 a 20 (passo 2), desvio de 0,50 a 2,00 (passo 0,30), alvo de 200 a 1.000 (passo 200) e stop de 500 a 1.500 (passo 200). Fixar previamente os valores efetivamente enumerados e o orçamento de tentativas, inclusive extremos não atingidos pelo passo. Começar com a réplica antes de qualquer varredura. Os 145–146 trades e PF 1,60–1,64 descritos não são resultados do MT5X. No WIN, converter explicitamente tick de 5 pontos e valor de R$ 0,20 por ponto. Nunca carregar esses valores para outro contrato sem verificar sua especificação.

As menções a gradientes binário e sniper não especificam gatilho, ativos, lotes nem encerramento. Registrar como ideias sem critério de implementação até existir regra reproduzível. Nos exemplos de hedge, resultados demo, CFDs, margem de planilha e metas financeiras não calibram capital para futuros. Na aplicação harmônica, o ganho em unidades e o retorno em dinheiro precisam ser apurados separadamente.

Referências primárias para a implementação — [especificação do WIN na B3](https://www.b3.com.br/pt_br/produtos-e-servicos/negociacao/renda-variavel/futuro-mini-de-ibovespa.htm), [iBands](https://www.mql5.com/en/docs/indicators/ibands), [modelos de ticks do Tester](https://www.metatrader5.com/en/terminal/help/algotrading/tick_generation), [ordenação de OnTradeTransaction](https://www.mql5.com/en/docs/event_handlers/ontradetransaction), [netting e hedging](https://www.mql5.com/en/book/automation/account/account_netting_hedge) e [seleção entre múltiplos testes](https://davidhbailey.com/dhbpapers/deflated-sharpe.pdf).
