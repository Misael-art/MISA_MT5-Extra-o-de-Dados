# F19 — Catálogo rastreável de indicadores, tempo e setups

**Planejamento de 2026-10-17.** Nenhum item se torna implementado por constar aqui.
Referência funcional: [especificação](PILARES_DADOS_OPERACAO_PRESETS.md).
Responsáveis, passos, dependências e testes: [plano F19](PLANO_F19_PILARES_PRESETS.md).

## 1. Proveniência e cobertura

A [reconciliação T19.1](INVENTARIO_F19.md) registra código integrado/branches,
donos, consumidores e lacunas de todos os IDs abaixo. Entrega documental em
revisão local; nenhum indicador/setup é promovido a implementado por esse mapa.

Fontes fornecidas pelo proprietário no repositório HADOUKEN `projeto-indicadores-mt5`,
pasta `ESTUDOS/`, lidas integralmente nesta sessão:

| Fonte | SHA-256 do conteúdo lido |
|---|---|
| `indicadores.txt` | `e701f02dac3fc3515d898cafc51cc26e31a7689209878f02672fdc7ea7bd1018` |
| `estrategias_trading_algoritmico_especificacoes_tecnicas_md.md` | `8228e6f406d5fc9bc16fc7298bd457941a7227c02b5c0e7eeb946f1a65351702` |

As duas capturas anexadas pelo proprietário mostram Execução e Insights no Laboratório:
oito modos visíveis, resultados por modo, regras manuais, exportação de proposta e curvas
antes/depois. São referências de organização e lacunas da experiência, não testes de desempenho.
Não versionar números de conta nem depender dos caminhos temporários das imagens.

O TXT repete a lista de indicadores: cada conceito abaixo é contado uma vez.
O Markdown contém 11 itens detalhados e uma seção 12 com famílias genéricas;
sua frase final sobre 48 itens e a referência a “1 a 37” não fornecem os demais setups.
Não inventar regras nem declarar que 48 robôs foram especificados.

As fórmulas abaixo são **definições propostas para implementação**, não certificação dos
materiais de origem. Fixar convenções matemáticas com fontes técnicas primárias e
paridade antes de codificar; parâmetros exemplificativos do estudo não são presets aprovados.

## 2. Regras comuns aos indicadores I01–I18

Cada indicador declara fórmula/variante, preço e volume de origem, período/parâmetros,
unidades, aquecimento exato, tratamento de zero/NaN, atualização, versão e `available_at`.
Indicador pode servir ao dado de mercado, ao contexto da operação e a filtro/sinal/saída;
essas utilizações compartilham a definição, sem copiar fórmulas no widget ou robô.

Aceite comum: cálculo puro; casos manuais e séries determinísticas; prefixo do histórico
inalterado quando dados futuros são acrescentados; paridade incremental × lote; sinais e
confirmações sem antecipação; persistência/cache/exportação; consumidor Laboratório;
par MQL5 quando selecionável operacionalmente. Ausência de dado retorna motivo.
O registro-base é T18.1; materialização/consulta e seus consumidores entram pela T19.3.

### I01 — Saldo de Agressão / Net Aggression

- **Mercado:** diferença de volumes classificados como agressão compradora e vendedora,
  por janela e acumulado da sessão; distinguir delta de volume e delta de contagem.
- **Fonte:** negócios com lado agressor ou classificador declarado. Tick de cotação,
  volume agregado de candle e book estático não provam agressor.
- **Parâmetros:** janela, unidade, reset, regra de classificação e categoria desconhecida.
- **Operações:** saldo anterior à entrada, inclinação/divergência e variação durante o trade.
- **Aceite específico:** compra/venda/indefinido, sessão noturna, reset e volume sem lado;
  classificador aproximado identificado, sem chamar de fluxo institucional comprovado.
- **Tarefa:** T19.11; captura T5.3 existente não garante os campos necessários.

### I02 — CVD / Cumulative Volume Delta

- **Mercado:** soma temporal de deltas de agressão; variantes por sessão e contínua têm IDs distintos.
- **Parâmetros:** âncora/reset, janela, tipo de volume e política em lacuna de dados.
- **Operações:** nível relativo, inclinação e divergência causal com preço.
- **Aceite específico:** partir de deltas I01 conciliados; reinício incremental e fontes diferentes
  não costuram uma série contínua falsa. Divergência exige pivôs já confirmados.
- **Tarefa:** T19.11.

### I03 — Imbalance / desequilíbrio

- **Duas definições distintas:** imbalance de agressões por preço/footprint e imbalance de
  liquidez exibida no book. Registrar IDs/variantes separados, nunca confundir as fontes.
- **Parâmetros:** comparação horizontal/diagonal para footprint, razão mínima, volume mínimo,
  profundidade e regra para denominador zero. “300% mais” e “razão 3:1” não são a mesma definição.
- **Operações:** estado disponível antes da entrada e mudanças durante o ciclo.
- **Aceite específico:** zero dos dois lados, apenas um lado, snapshot vencido, cancelamento,
  níveis faltantes e negócios sem classificação; book não substitui agressão.
- **Tarefa:** T19.12; depender da qualidade efetiva das fontes T5.3/T5.4.

### I04 — Speed of Tape

- **Mercado:** negócios e/ou volume por unidade de tempo; aceleração em janelas definidas.
- **Fonte:** timestamps de negócios com resolução suficiente, ordenação e desempate.
- **Parâmetros:** janela, unidade, normalização histórica causal e limiar.
- **Aceite específico:** rajadas, empates de timestamp, buracos e retomada; não calcular
  “velocidade da fita” apenas contando candles.
- **Tarefa:** T19.13.

### I05 — Large Trades / grandes negócios

- **Mercado:** evento de tamanho acima de limiar absoluto ou quantil de janela anterior.
- **Parâmetros:** tamanho mínimo, unidade, histórico de calibração e regra de agregação.
- **Aceite específico:** quantil sem futuro, escala/lote do ativo, fragmentação sem duplicação.
  Negócio grande não comprova a identidade ou intenção institucional do participante.
- **Tarefa:** T19.13.

### I06 — Alligator de Bill Williams

- **Mercado:** três médias suavizadas de preço mediano, com períodos e deslocamentos por linha.
- **Parâmetros:** mandíbula/dentes/lábios, método de suavização, sementes, offsets e preço.
- **Saídas:** linhas, alinhamento, separação e estado de tendência/consolidação objetivamente definido.
- **Aceite específico:** deslocamento de desenho não autoriza usar barras futuras; registrar
  tempo do cálculo e tempo da exibição separadamente; paridade com variante escolhida.
- **Contrato T19.4:** mediano `(high+low)/2`; SMMA com semente SMA de n valores finitos
  consecutivos; recorrência `(anterior*(n-1)+preço)/n`. Mandíbula13/desenho+8,
  dentes8/+5, lábios5/+3. Lacuna reinicia semente. Valores consumidos não deslocados,
  disponíveis no fechamento/recebimento; desenho usa âncora+offset, sem prever UTC futuro.
  Alinhamento estrutural ±1/0 e separação máximo−mínimo exigem as três linhas; mistura/empate
  não certifica regime de consolidação nem gera ordem. Referências primárias e limites:
  [contrato/aceite](../analytics/T19.4_ALLIGATOR.md).
- **Tarefa:** T19.4.

### I07 — Awesome Oscillator / AO

- **Mercado:** SMA5 do preço mediano menos SMA34, conforme o estudo.
- **Parâmetros:** períodos, preço, aquecimento; manter defaults de referência separados de variantes.
- **Operações:** nível, inclinação, cruzamento e aceleração, quando disponíveis.
- **Aceite específico:** série constante, aquecimento e diferenças entre cálculo/formatação.
- **Contrato implementado:** SMA por janela completa finita, mediano (high+low)/2; AO aquece em
  34 observações. Inclinação/cor e cruzamento exigem 35, segunda diferença exige 36. Cor é
  sinal da variação, com empate neutro explícito; toque em zero não cruza, saída de zero pode
  cruzar. Transformações não alteram o nível. Ausência não vira zero e disponibilidade é no
  fechamento/recebimento de revisão, sem deslocamento. [Referência e aceite](../analytics/T19.5_AO_AC.md).
- **Tarefa:** T19.5.

### I08 — Fractals

- **Variante em revisão:** [T19.6: origem, confirmação, empates e limites](../analytics/T19.6_PIVOS_CAUSAIS.md).

- **Mercado:** pivô central em sequência de cinco candles, com duas barras de cada lado.
- **Parâmetros:** largura, regras para máximas/mínimas iguais e instante de confirmação.
- **Operações:** último topo/fundo confirmado, distância e rompimento posterior.
- **Aceite específico:** pivô só fica disponível após as barras de confirmação; marca histórica
  pode estar no centro, mas filtro não recebe o sinal duas barras antes.
- **Tarefa:** T19.6.

### I09 — Accelerator/Decelerator / AC

- **Mercado:** variante de referência fixada AO menos SMA5 do mesmo AO (5/34 no mediano).
- **Parâmetros:** períodos do AO e suavização do AC, inclinação/cor como transformações separadas.
- **Aceite específico:** paridade do encadeamento e aquecimento total, sem antecipar mudança de cor.
- **Contrato implementado:** 38 observações para AC, 39 para inclinação/cor. Janelas finitas
  completas, ausência explícita, cor separada do nível e disponibilidade causal no serviço
  existente; detalhes e limite da paleta em [T19.5](../analytics/T19.5_AO_AC.md).
- **Tarefa:** T19.5.

### I10 — Donchian Channels

- **Mercado:** máxima/mínima móveis de N barras e linha central, com referência explícita.
- **Existente:** `strategies/indicators.py:donchian`; verificar o shift da variante reutilizada.
- **Parâmetros:** N, inclusão/exclusão da barra corrente e base do rompimento.
- **Operações:** distâncias, renovação de extremos, retorno ao canal e tempo sem novo extremo.
- **Aceite específico:** não usar máxima do próprio candle como canal anterior de rompimento;
  integrar ao store/exportação mesmo havendo função pronta.
- **Tarefas:** T19.7 (mercado) e T19.16 (gestão temporal).

### I11 — Envelopes

- **Mercado:** média de referência × (1 ± percentual); não usar desvio padrão por substituição.
- **Parâmetros:** período, método/preço da média, percentuais superior/inferior e unidade %.
- **Operações:** posição no envelope e distância normalizada para filtros/alvos.
- **Aceite específico:** percentual 1% não interpretado como multiplicador 1; dados insuficientes.
- **Tarefa:** T19.7.

### I12 — Canais de Regressão Linear

- **Mercado:** mínimos quadrados por janela e bandas ao redor da reta.
- **Parâmetros:** N, preço, definição do desvio (resíduo ou outra variante declarada), multiplicador.
- **Existente:** slope e R²; isso não prova que os canais completos existam.
- **Operações:** inclinação, R², posição/distância e amplitude disponíveis antes da decisão.
- **Aceite específico:** endpoint calculado só com janela passada; reta histórica recalculada
  não pode substituir o valor que estava disponível naquele instante.
- **Tarefa:** T19.7.

### I13 — McGinley Dynamic

- **Mercado:** recorrência adaptativa; proposta de variante:
  MD[t] = MD[t−1] + (P[t]−MD[t−1]) / (k × N × (P[t]/MD[t−1])^4).
- **Parâmetros:** N, k, preço, semente, limites e política para preço/denominador zero.
- **Operações:** direção, distância e uso como média/stop, somente após paridade.
- **Aceite específico:** não prometer eliminação de atraso; evitar explosão numérica e comparar
  variantes explicitamente, sem usar uma sob o nome da outra.
- **Tarefa:** T19.8.

### I14 — ZigZag

- **Variante em revisão:** [T19.6: origem, confirmação, empates e limites](../analytics/T19.6_PIVOS_CAUSAIS.md).

- **Mercado:** swings segundo desvio, profundidade e backstep definidos.
- **Saídas distintas:** desenho retrospectivo sujeito a repaint e sequência de pivôs confirmados.
- **Operações:** só pivôs com confirmação já disponível; último segmento provisório é identificado.
- **Aceite específico:** acrescentar barras futuras pode revisar o desenho provisório,
  mas não reescrever decisões anteriores; exportar tempo de confirmação e revisão.
- **Tarefa:** T19.6.

### I15 — Ultimate Oscillator

- **Mercado:** combinar razões entre soma de pressão compradora e true range em três janelas.
- **Parâmetros:** curtas/médias/longas, pesos e escala; propostas de referência 7/14/28 e 4/2/1,
  que precisam de confirmação na implementação.
- **Aceite específico:** pressão/true range com fechamento anterior, denominador zero e
  aquecimento; não combinar osciladores com fórmula distinta sem novo ID.
- **Tarefa:** T19.9.

### I16 — TRIX

- **Mercado:** taxa de variação da terceira suavização exponencial, com sinal opcional.
- **Parâmetros:** N, regra da EMA/semente, escala %/fração e período da linha de sinal.
- **Operações:** inclinação/cruzamento/zero e divergências sem pivôs futuros.
- **Aceite específico:** aquecimento encadeado e denominador zero; lote × incremental.
- **Tarefa:** T19.10.

### I17 — KST / Know Sure Thing

- **Mercado:** combinação ponderada de ROC em múltiplos períodos, cada um suavizado.
- **Parâmetros:** quatro janelas ROC, quatro suavizações, pesos e sinal.
- **Aceite específico:** variante/períodos registrados; aquecimento pelo caminho mais longo,
  dados constantes e consistência de escala.
- **Tarefa:** T19.10.

### I18 — Efficiency Ratio / ER de Kaufman

- **Mercado:** módulo da variação líquida em N períodos dividido pela soma dos módulos
  das variações no caminho. Denominador zero tem política explícita, sem infinito.
- **Existente:** `strategies/indicators.py:efficiency_ratio`; integrar sua definição sem cópia.
- **Operações:** ER na entrada e ao longo da posição, com janela causal.
- **Aceite específico:** mercado imóvel, tendência monotônica e caminho alternante; ER baixo
  é condição de regra configurável, não prova de falta de fluxo.
- **Tarefas:** T19.3 (publicação/consumo) e T19.16 (saída temporal).

## 3. Métricas temporais e gestão da operação

| ID | Conteúdo integral do estudo/pedido | Dados e saída | Tarefa |
|---|---|---|---|
| TM01 | Contador iniciado na abertura da posição | Idade em relógio, tempo negociável e barras; referência de preenchimento | T19.14 |
| TM02 | PnL × tempo, limite N e lucro mínimo | Trajetória observável; exemplo 15 barras/50 pontos é ilustrativo | T19.14/T19.15 |
| TM03 | Time de gain e time de loss | Prazos/limiares separados para ganho, perda e empate; saída com motivo | T19.15 |
| TM04 | MEP/MFE e MEN/MAE por tempo | Extremo, instante/barra do extremo, tempo até limiares e curva parcial | T19.14 |
| TM05 | ER junto com idade da operação | Falta de eficiência observável depois de uma idade mínima | T19.16 |
| TM06 | Donchian e tempo de rompimento | Tempo sem renovar extremo, retorno ao canal e encerramento condicionado | T19.16 |
| TM07 | ATR relativo ao tempo, relógio volumétrico ou por amplitude | Horizonte adaptativo limitado; relógios distintos, fonte/limiar/reset e disponibilidade; ATR primeiro | T19.16 |
| TM08 | Distribuição de duração e chance de continuação | Todas as operações, censura, grupos/IC e n em risco; não inferir só dos vencedores | T19.14/T19.31 |

MFE/MAE gerais já existem no enriquecimento; falta a dimensão temporal completa.
Preservar a convenção de sinal/unidade existente ou versionar a conversão.
Pontos, preço, R e dinheiro não são intercambiáveis sem stop inicial, especificação e moeda.
O estudo usa os nomes MEP/MEN; o catálogo mantém os equivalentes MFE/MAE para interoperabilidade.

## 4. Indicadores e ferramentas adicionais do segundo estudo/pedido

| ID | Necessidade | Reuso/definição e lacunas | Tarefas |
|---|---|---|---|
| AX01 | VWAP diária/sessão e bandas | Preço × volume de negócios; aproximação OHLCV/tick-volume rotulada; bandas % e desvio padrão separadas | T19.3/T19.36 |
| AX02 | Leque Madrid | Dez EMAs ordenadas, períodos ainda ausentes no estudo; inclinação/afastamento e ciclo | T19.39 |
| AX03 | 2MV | Média de High/Low, método/período explícitos e cor objetiva | T19.42 |
| AX04 | Estocástico lento + EMA9 | Reutilizar cálculos existentes após verificar 14/3, linha/sinal e aquecimento | T19.38 |
| AX05 | Heiken-Ashi + High-Low Activator | Fórmula, semente, preço sintético × executável, período 10/offset 0 de referência | T19.40 |
| AX06 | Fibonacci para alvo e referência de entrada | Pivôs confirmados, retração/extensão, níveis, direção, tipo de ordem e congelamento | T19.18/T19.17 |
| AX07 | Renko e gerador | Ticks → tijolos, pavios, reversão, gaps, múltiplos tijolos/tick e timestamp determinístico | T19.41 |
| AX08 | Suporte/resistência, falha e polaridade | Regras objetivas e confirmação causal; versão discricionária permanece manual | T19.43 |
| AX09 | OCO, cópia, painéis e travas | Classificar como ferramentas, não setups de sinal; preservar limites e ownership | T19.44/T19.47/T19.48 |

## 5. Catálogo dos 11 itens detalhados do segundo documento

Cada Sxx conserva a referência textual de origem e define a decomposição:
**sinal → entrada → ganho → stop → filtros**. “A definir” é uma dependência de especificação,
não liberdade para atribuir uma regra inventada ao setup original.
Valores numéricos são exemplos/referências fornecidos, sem validade universal entre ativos.

### S01 — VWAP Bands Scalper (`QTIVjQ-2mko`)

- **Sinal:** retorno à média em extremos da VWAP da sessão.
- **Entrada:** compra limite em −1%/−1,5%; venda em +1%/+1,5%.
- **Ganho:** retorno a −0,5%/+0,5% ou VWAP central, escolha explícita.
- **Stop:** 150–200 pontos além da banda extrema, normalizados ao ativo; protetor obrigatório.
- **Filtros:** sessão, spread, volume e disponibilidade da fonte; sem preço médio implícito.
- **Lacunas:** texto chama de “desvio padrão” o que lista como percentual; fixar variante.
  Não chamar re-simulação por barras de altíssima frequência/tick real.
- **Entrega:** módulo de sinal e políticas AX01; T19.36, com par Python/MQL5 e preset compatível.

### S02 — MT5 Renko Setup & Offline Generator (`u9HzRRzSIdI`)

- **Tipo:** infraestrutura de série, não estratégia autônoma.
- **Fonte:** M1 como referência de ativo; geração exigida por ticks reais.
- **Parâmetros citados:** 10R/15R/20R, atualização 500 ms/OnTick, pavios habilitados.
- **Lacunas:** definir R em pontos/ticks/preço, regra de reversão e disponibilidade do tijolo.
  Verificar mecanismo real de símbolo customizado no MT5; não copiar menu “Open Offline”
  como API garantida do terminal.
- **Entrega:** AX07 reproduzível e paridade dos tijolos; T19.41.
  Não transformar OHLC M1 em trajetória única de ticks e afirmar equivalência.

### S03 — Grid Connect / Master-Slave (`JYAqOsWL8WY`)

- **Tipo:** gerenciador de execução; sinal pertence ao master.
- **Entrada:** semente, step de referência 50 pontos, 5–10 níveis;
  fator 1,0 linear/1,5 alavancado, sem confundir lote por nível com exponencial.
- **Ganho:** financeiro consolidado da cesta; trailing dinâmico do grid.
- **Stop/filtros:** política global/ciclo, exposição/margem, validade, agenda e limites do robô.
- **Lacunas:** não “interceptar e substituir” ordem cheia já executada: integrar intenção
  antes do envio, ou protocolo de adoção explicitamente suportado e autorizado.
- **Entrega:** adaptador e orquestração, sem envio fora da Safety; T19.44.
  Liberação de grade depende da política e da reserva agregada existentes.

### S04 — Manual + Gradiente Linear (`HWA6K8msBW0`)

- **Tipo:** híbrido, com decisão manual ou variante algorítmica separadamente especificada.
- **Contexto:** tendência macro de referência M30, falhas de topo/fundo e troca de polaridade;
  evitar compras em topo, vendas em fundo e entradas no meio do range, segundo o estudo.
- **Entrada:** semente reduzida (exemplo três contratos) e grade reativa/trailing.
- **Ganho:** parciais e meta financeira conforme contexto; não presumir rentabilidade do vaivém.
- **Stop/filtros:** invalidação estrutural e stop financeiro obrigatório; preferência pós-11h
  Brasília como parâmetro, não horário fixo válido para todos os ativos.
- **Lacunas:** definições numéricas de “esticado”, “falha”, “meio do range” e mudança de meta.
  Preservar intenção/manual registrado; uma automação nova tem ID/versão próprios.
- **Entrega:** AX08, entrada manual reprodutível por eventos e variante objetiva; T19.43.

### S05 — Madrid Trend Scalper (`6UsYTUr_xXE`)

- **Sinal:** dez EMAs estritamente ordenadas; fechamento anterior cruza M1 a favor,
  distância máxima permitida e flags FC/FV livres.
- **Entrada:** execução depois da confirmação; definir tipo/preço sem usar fechamento futuro.
- **Ganho:** média M1 ± afastamento; média/target móveis com regra temporal explícita.
- **Stop:** uma das dez médias selecionada + offset, com trava de avanço.
- **Filtros:** reset exige retorno/cruzamento contrário de M1 para liberar novo ciclo.
- **Lacunas:** dez períodos, definição exata do cruzamento/igualdade e instante do reset.
- **Entrega:** AX02, políticas reutilizadas e estado de ciclo persistente; T19.39.

### S06 — 2MV Trend Scalper (`TuqMC8AOQL0`)

- **Sinal:** no Renko, alta se Close > Open e Close > média de High;
  baixa se Close < Open e Close < média de Low; neutro dentro do canal.
- **Entrada:** limite na abertura do bloco seguinte; semente exemplificada em dois contratos.
- **Ganho:** parcial no nível de fechamento de referência e adições a favor por novo bloco.
- **Stop:** perda da cor da tendência/inversão encerra tudo, além de stop protetor.
- **Parâmetros:** média de referência 20, Renko 7R/10R/15R, parcial e teto de adições.
- **Lacunas:** tipo da média, regras de parcial e preenchimento; abertura/fechamento de um
  tijolo sintético não provam preços executáveis depois de conhecido o sinal.
- **Entrega:** AX03 sobre AX07, T19.42; reservas para scale-in e recuperação obrigatórias.

### S07 — Alaska & Square (`0DMP0lmrvTY`)

- **Sinal:** somente venda, candle H1 anterior positivo.
- **Entrada Alaska:** venda stop 200 pontos abaixo do fechamento anterior; um contrato.
- **Ganho Alaska:** 100 pontos; stop global 400 pontos acima da entrada inicial.
- **Square:** reforço de três contratos após 150 pontos contra; alvo de referência 120 pontos
  desde o acionamento do médio; bloquear reentrada na mesma barra H1.
- **Lacunas:** direção do alvo +120 no texto é ambígua para venda; esclarecer se recompra a
  120 abaixo do médio, alvo por perna ou cesta, e o destino do alvo inicial.
- **Entrega:** T19.45, como sinal simples + política de uma adição; não liberar preço médio
  enquanto política/Safety/bateria/reserva e tipo de conta não suportarem.

### S08 — Round Six (`pYCEcBuEZg0`)

- **Sinal:** retorno à média nas extremidades de Bollinger ou Keltner em consolidação.
- **Entrada:** até seis rodadas; parâmetros de lote/distância ainda não fornecidos.
- **Ganho:** alvo de referência 50 pontos ou média central.
- **Stop/filtros:** limites financeiros por sessão, proteção global e filtro Black Box.
- **Lacuna impeditiva da fidelidade:** fórmula proprietária do Black Box ausente.
- **Entrega:** T19.46 primeiro documenta a lacuna; original não implementável fielmente com
  esses dados. Variante de regime transparente tem outro ID e regra aprovada antes de teste.
  Nunca chamar um ADX/ER escolhido pelo executor de Black Box original.

### S09 — Base MQL5 / Cruzamento de Médias (`Ou3NVuKTseo`)

- **Tipo:** framework/template com setup exemplo, não 48 estratégias novas.
- **Sinal:** cruzamento EMA curta/longa; reutilizar `ema_atr`/declaração onde equivalente.
- **Operacional:** SL/TP OCO fixos; break-even, trailing fixo por barra, janela e zeragem final.
- **Gestão:** perda/meta diária, painel magic/ativo/PnL diário/semanal/mensal.
- **Lacunas:** períodos e gatilhos, unidades, precedência e equivalência com robô já existente.
- **Entrega:** T19.37, piloto do caminho completo; variantes são configuração, sem cópia
  do framework e sem enfraquecer o stop obrigatório.

### S10 — Seguro Iniciantes (`a-N8cbDra3o`)

- **Sinal:** estocástico lento 14/3 cruza sua média; compra >75 com preço > EMA9;
  venda <25 com preço < EMA9.
- **Entrada:** após confirmação em barra fechada.
- **Ganho:** TP fixo opcional; saída conforme políticas selecionadas.
- **Stop:** EMA9 + offset; Move Guard só avança a favor, sem relaxar após recuo da média.
- **Lacunas:** linha exata K/D usada, suavizações e sinal em igualdade, TFs e regras de reentrada.
- **Entrega:** AX04, T19.38; nome do estudo não significa garantia ou adequação a iniciantes.

### S11 — Heiken-Ashi + High-Low Activator (`9DxBy8rLtp4`)

- **Sinal:** compra se HA Close > High-Low e HA Close > HA Open;
  venda com desigualdades inversas. Referência High-Low 10/offset 0.
- **Operacional:** fechamento e nova entrada oposta a mercado; sem TP fixo.
- **Stop/filtros:** proteção obrigatória e limites/filtros têm precedência sobre “sempre posicionado”.
- **Lacunas:** fórmula/estado do Activator, semente HA, confirmação e tratamento do neutro;
  preço sintético do HA não é preço de preenchimento.
- **Entrega:** AX05, T19.40; reversão de duas etapas conciliada, sem dobrar posição por engano.

## 6. Seção histórica sem regras individuais: cobertura explícita

| Família citada na seção 12 | Destino e tratamento |
|---|---|
| Grid/preço médio, Martingale/Fibonacci e correlação | Entradas/dimensionamento T19.22; restrições vigentes; não prometer preset operacional de estudo proibido |
| Breakout Donchian/Keltner e ORB | Reutilizar `donchian`, `orb`, `squeeze`; variante tem regra e tentativa próprias |
| Mean reversion Bollinger 2/3 desvios, VWAP e divergências RSI | Reusar `bb_rsi`, AX01 e pivôs confirmados; parâmetros não especificam um robô completo |
| OCO, copiadores, painéis e proteção patrimonial | AX09; ferramentas de suporte com contratos/ownership, não sinais novos |

T19.47 registra quais regras ainda precisam ser fornecidas, IDs conhecidos e relações
com os módulos existentes. O conteúdo disponibilizado não permite enumerar os supostos
37/48 setups. Sua ausência permanece visível no backlog, sem declarar cobertura fictícia.

## 7. Rastreabilidade do pedido além dos estudos

| Pedido | Contrato/aceite | Tarefas |
|---|---|---|
| Base enriquecida de mercado | I01–I18, AX01 e store causal versionado | T19.3–T19.13 |
| Backtest/histórico real enriquecido | Mesmo ledger, origens separadas e métricas temporais | T19.14–T19.16 |
| Entrada simples, níveis e gradientes a favor/contra | Planos tipados e risco do conjunto | T19.17/T19.22/T19.23 |
| Gestão de lotes e volume maior que um | Quantidade fixa/por risco, unidade/mínimo/passo, margem e exposição; adições separadas | T19.56 |
| Parciais simples/múltiplas e saldo protegido | Agenda por quantidade/% com base explícita, etapas confirmadas, custos e métricas por trade/ciclo | T19.57 |
| Alvos fixos/variáveis/Fibonacci | Política de ganho independente do stop | T19.18 |
| Stops por indicador e tempo | Proteção + trailing + tempo sem antecipação | T19.15/T19.16/T19.19 |
| Horários, dias e uma/três/N operações por dia | Sessão, preenchimentos/ciclos e contadores recuperáveis | T19.20 |
| Filtros de qualquer indicador catalogado | Suporte por tipo/capacidade e ausência explícita | T19.21 |
| Gerador manual, defensivo e PF | Objetivos pré-definidos, backtest completo e pacote verificável | T19.24–T19.29 |
| Nomes padronizados no Default e categorias de trabalho | Nome do Manual escolhido pelo usuário, rótulo/slug/metadados e arquivo portável; legado preservado | T19.26/T19.50 |
| Default da estratégia primeiro | Referência versionada dos padrões efetivos, anterior aos derivados; Salvar como Manual sem editar o original | T19.55 |
| Ultra: compor contextos favoráveis ao longo do calendário/mercado | Regras causais, prioridades, alternativa padrão, rastreio e avaliação da política inteira | T19.52–T19.54 |
| Inversão experimental compra↔venda | Flag desligada por padrão; transformar intenção e recalcular plano, sem reverter posição aberta | T19.51 |
| Caixa operacional organizada | Entrada, ganho, stop, filtros e resumo | T19.30 |
| UI temporal, gráficos e candidatos | Distribuições, fronteiras OOS, n/qualidade e ações claras | T19.31/T19.32 |
| Escolha de timeframe em Robôs | Reusar T17.28; verificar propagação e descoberta | T19.33 |
| UI moderna e adaptável | Componentes F17, teclado, temas, DPI e matriz de resoluções | T19.34 |
| Preset funciona nos robôs HADOUKEN | Compilador, capacidades, paridade e reconciliação nativa | T19.23/T19.25/T19.29/T19.35 |
| Setups do segundo estudo | S01–S11 individualizados, com lacunas declaradas | T19.36–T19.47 |
| Gestão da conta depois | Preservar travas existentes e robustecer módulo em etapa posterior | T19.48 |
| Frente entregue sem regressão | Aceite integrado e documentação reconciliada | T19.49 |

Um item só muda de “planejado” para “implementado/validado” com evidência vinculada à tarefa.
Impedimento por falta de dado ou especificação tem motivo e próximo passo, não equivale
a “concluído” para o cálculo ou robô correspondente.

## 8. Adendo do proprietário — nomes, Ultra e inversão

Default da estratégia como referência inicial; quatro categorias de trabalho:
**Manual, Defensivo, Maximalista (Fator de lucro) e Ultra**. A convenção de nomes,
com exemplos e identidade independente do nome, está na especificação §8.4.
Ultra (§8.5) segue as “migalhas de pão” como regras de contexto que escolhem configurações
de entrada/ganho/stop/filtros disponíveis naquele instante. Inclui calendário por hora/dia/
semana/mês, indicadores e variáveis observáveis da operação; não costura o melhor resultado
de cada intervalo escolhido depois de conhecer o desfecho. Regras e política completas
são testadas e reproduzidas no HADOUKEN, com prioridades, fallback e rastreio.

A inversão (§6.2) é outra dimensão operacional, experimental e opcional em qualquer categoria
quando suportada. Compra vira venda e venda vira compra; sem sinal continua sem sinal.
Não equivale ao stop-and-reverse S11. Entrada, tipo de ordem, stop/alvo, custos e tamanho
são recalculados para a direção efetiva, sem negar o PnL de um backtest anterior.
O Ultra pode incluir entrada Fibonacci, usando os níveis causais de AX06 e o módulo de
entrada correspondente; um alvo Fibonacci não comprova esse suporte de entrada.

Esses requisitos adicionais vêm do pedido de 2026-10-17, sem alterar a proveniência ou
contagem dos indicadores/setups dos dois estudos. T19.50–54 permanecem planejadas.

## 9. Indicador composto proposto para estudo futuro: FRP

**FRP — Fluxo Relativo do Preço**, ideia adicional do proprietário: grupos de três ou quatro
indicadores correlacionados entre si e com o movimento do preço, com concordância direcional
e relação com o Expert quando houver dados adequados. Explorar limiares 0,60/0,70/0,80/0,90,
tratados como coeficientes de correlação, não probabilidade de acerto.

[Conceito, perguntas matemáticas e backlog FRP-00–04](../pesquisa/FRP_FLUXO_RELATIVO_PRECO.md).
Estado ESTUDO FUTURO/P3; sem fórmula aprovada ou implementação. Não altera os 18 indicadores
originais, as 57 fichas planejadas nem o caminho crítico da F19. Avaliar redundância e
contribuição incremental fora da amostra antes de propor uso como filtro/sinal/preset.

## 10. Default, Manual nomeado e validação operacional ágil

Adendo de 2026-10-17: usuário informa Nome do preset no Manual; preservar o texto em
catálogo/manifesto/importação, gerar slug portável e identificar automaticamente por ID/
metadados. Default é o primeiro preset de referência da estratégia e não é sobrescrito
ao criar um Manual. Maximalista é a apresentação do perfil FatorLucro, sem duplicar categoria.
Regras detalhadas na especificação §§8.4.1/8.4.2; tarefas T19.26/50/55.

[Matriz de validação](VALIDACAO_F19.md): observar gradiente/stops e comportamento operacional
no replay visual nativo MT5, em ambiente DEMO, quando ativo indisponível online/fora de pregão.
Agente pode escolher período com aquecimento e amostras suficientes para agilizar cobertura.
Estrutura/cálculos/pesquisa usam testes/backtests pertinentes. Recorte de QA não prova lucro/OOS.
[Prompt consolidado de execução](PROMPT_F19_EXECUCAO.md).

## 11. Gestão de lotes e realização parcial

Adendo operacional: volume de entrada maior que um, lote fixo/por risco e saídas parciais
em uma/várias etapas, com quantidade absoluta ou percentual/base definida. Uma entrada com
mais volume, entradas adicionais e fechamento parcial são conceitos distintos.
Regras, exemplos aritméticos e aceites na especificação §6.3; T19.56/57 ampliam para 57 fichas.

Reutilizar SymbolSpec.lots_for_risk e parcial simples existentes; re-simulador atual não
aplica a parcial simples em grid, portanto combinação só será oferecida após extensão/prova.
Preservar volume mínimo/passo/saldo, stop, ownership, limites e custos; não contar parcelas
como operações independentes para acerto/PF. UI Lotes e parciais, preset e HADOUKEN
compartilham a mesma política; testar estrutura/backtest e observar reduções no replay nativo/DEMO.
