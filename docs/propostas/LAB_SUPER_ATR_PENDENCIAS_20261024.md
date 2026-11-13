# Laboratório e Super ATR — pendências e divisão de frentes

Registro do proprietário em **24/10/2026**, após testar pelo atalho central.
**Laboratório incompleto; aceite funcional reaberto.** Este documento registra
trabalho a executar, não entrega as correções nem inicia os agentes.
Tarefa documental LAB-SA-00; cada implementação abaixo terá seu próprio PR.

## Ponto de partida e evidência

- App: `codex/lab-multiframe`, fonte documental
  `af717fa90bd4d514fb226622862bca572ae41f88`, runtime
  `7c66feda06eb065b5cd591fd22ba5b602cffa624`,
  [PR210 em rascunho](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/210).
  Depende de AppPR207/208 e HPR75. Ainda não é master integrado.
- [Repasse anterior](../repasse/T20_22_LABORATORIO_CONTINUIDADE_20261024.md)
  conserva testes, limitações, instalação e backup. Seus testes não substituem
  os percursos reais agora reportados como falhos. A ampla anterior imprimiu
  3014/259 e terminou com código143 após o resumo; a seleção nova20/15 terminou0.
- Três imagens fornecidas pelo proprietário: sufixos `533b4e69…` (visão entre TFs
  cinza, com uma barra/NaN no detalhe), `495f1993…` (Horários, cabeçalho desalinhado)
  e `cec7624f…` (Dia do mês, rótulos verticais e cores antigas). As originais
  ficam no contexto privado; contêm identificação de conta e não são publicadas.
  Imagem vazia é sintoma: a causa precisa ser reproduzida, não deduzida da captura.
- `ui/relations.py` ainda declara uma fixture sintética, com quatro indicadores.
  Os 52 cálculos Analytics, os 38 indicadores de Estudo nativo e o inventário
  de séries realmente persistidas são conjuntos distintos. Nenhum desses números
  comprova cobertura completa da base. Serviços de consulta BD18a/BD19c1 já
  existem: consumir seus contratos, sem novo catálogo ou store concorrente.

## Backlog verificável

P1 indica percurso ausente/falho ou dependência funcional; P2, refinamento visual.
Todos permanecem **pendentes**, salvo o registro documental LAB-SA-00.

| ID / plano existente | Prioridade / dono | Trabalho e critério de aceite |
|---|---|---|
| LAB-01 · T18.6/8, BD19 | P1 · Laboratório, contrato com Dados | Indicador × indicador e Indicador × alvo devem descobrir **todo o inventário da base**, variantes/outputs/params incluídos. Busca e filtros com motivo explícito para indisponibilidade; sem lista curta fixa nem fixture apresentada como dado real. Teste de inclusão de uma nova definição sem editar o seletor, paginação e identidade da fonte. |
| LAB-02 · T20.22-LAB1 | P1 · Laboratório | Reproduzir Séries e confluências pelo atalho e corrigir a geração das visões. Identificar DB/contexto/manifestos/TFs/corte, warmup e exceções. Dados elegíveis produzem curvas e eventos; dados insuficientes produzem motivo e ação útil, nunca apenas área cinza ou sucesso com NaN. Cobrir resposta obsoleta/cancelamento e erro de fundo. |
| LAB-03 · T17.2/5 | P1 · Laboratório | Horários: cabeçalho e corpo compartilham larguras, posição horizontal e resize. Validar alinhamento de **cada coluna** após rolagem vertical/horizontal, maximizar/restaurar, DPI e mudança de monitor; não apenas captura estática inicial. |
| LAB-04 · T17.5, T20.22 | P2 · Laboratório | Modernizar Dia do mês e revisar as demais visões. Escala divergente consistente, magnitude forte em tons intensos e fraca em tons claros, zero neutro, ausente distinto. Valores horizontais e legíveis; remover `n=…` repetido sobre todas as barras, levando amostra/insuficiência para legenda contextual, tooltip ou detalhes. Preservar denominadores e limitações, com temas claro/escuro e escala visível. |
| LAB-05 · T20.22/23 | P1 · Laboratório | Visão gráfica do **mesmo ativo com estratégias/robôs diferentes**: preço, sinais/eventos e curvas comparáveis, seleção identificada e navegação temporal vinculada. Distinguir intenção, operação e execução; comparar só recortes/modelos/custos compatíveis ou explicitar incompatibilidade. |
| LAB-06 · T19.33, T20.22/24/25 | P1 · Laboratório | Visão do **mesmo robô com a mesma revisão de preset em diferentes ativos**. Mostrar hash/revisão efetiva, intervalos, especificações/unidades e custos; curvas normalizadas quando moedas/escalas diferirem. Último run por ativo não comprova mesmo preset. Reusar orçamento/fila; não disparar backtests ao abrir a tela. |
| LAB-07 · T20.22/23, T20.13 | P1 · Laboratório | Mesma estratégia entre TFs, com comparação sincronizada e grau de relação **descritiva e preditiva separados**. Fixar direção/horizonte/defasagem, elegibilidade as-of, denominadores, eventos não confirmados, controles e incerteza. Avaliação temporal fora da amostra para alegar previsão; sem ela, rotular exploratório. M7/M9 somente pelo contrato de barras derivadas, não inventados na UI. |
| LAB-08 · T20.23, G19.6/7, AIA-02 | P1 · Laboratório | Gráfico do ativo com candles e todas as séries qualificadas selecionáveis: sobreposição/subjanelas conforme unidade, múltiplas instâncias parametrizadas, legenda/ocultar/remover, cursor/zoom vinculados. Ferramentas de linha, horizontal/vertical, região e nota, editar/excluir/desfazer e exportar/reabrir o estudo com identidade. Reusar Estudo existente quando aplicável, sem limitar o laboratório a abrir uma janela MT5. |
| LAB-09 · T19.32, F19, AIA-02 | P1 · Laboratório + integração serial | Gerador de presets coeso via Default/Manual/store/compiler existentes. Corrigir a descoberta e qualificar a closure de cada perfil; os seis perfis automáticos encontrados estavam bloqueados por versão/pin/inputs. Não remover gate para habilitar botão nem criar formato paralelo. Saída do estudo é candidato rastreável, não preset aprovado por correlação. |
| LAB-10 · T20.30, AIA-02 | P1 · Integrador / cada frente | Todas as capacidades por serviço/SDK/CLI e UI cliente; parâmetros tipados, JSON, IDs, exportação, erros úteis e resultado verificável. Publicar composição imutável e testada no atalho/menu centrais, com rollback. Validar handheld, monitor amplo, DPI, coordenadas negativas e desconexão do dock; ampliar/restaurar gráfico sem perder contexto, rolagem na tela menor. |
| SA-01 · EST-OP, G19.9/13/15 | P1 · Expert Super ATR | Expert que consome a matemática existente e exige confirmação causal entre TFs. Congelar variante de entrada/saída/risco/linhas/TFs antes do código; usar somente barras fechadas disponíveis, recusando confirmação futura/ausente/vencida. Reutilizar risco/executor/host e testar pelos percursos de preset/CLI/atalho. Primeiro recorte Tester+DEMO; sem autorização REAL. |
| SA-02 · EST-IND, BD06/07/18/19 | P1 · **Agente do pilar de dados já existente** | Cálculo e persistência do Super ATR, com coluna/projeção consultável pelo usuário e série canônica identificada por parâmetros/versão/linha/seed/janela/fonte. Store aditivo, consulta e as-of/qualidade, repetição idempotente, revisão preservada e paridade declarada com MQL. Laboratório e Expert são consumidores, não donos de outro storage. |

M7/M9, em 2026-10-28: o produtor de barras derivadas grava esses tempos em
`_bars_derived` com âncora 00:00, grupos completos e rótulo `M7`/`M9`. Séries
desenha essa leitura, rotulada como sintética, sem agregar na UI. Isso não
fecha o LAB-07 nem o SA-01/SA-02.

As quatro comparações não são a mesma entrega: indicador entre TFs, estratégia
entre TFs, estratégias diferentes no mesmo ativo e preset idêntico entre ativos.
O inspetor financeiro ainda depende de eventos de execução/exposição comprovados;
não inferir close, SL aplicado ou fill a partir de uma curva do indicador.

## Divisão de posse e dependências

| Frente | Pode editar no seu recorte | Preserva / consome |
|---|---|---|
| Agente Laboratório | `laboratory/`, UI/gráficos/componentes, adaptadores de leitura, testes e docs próprios | Produtores/schema/ingestão/registry canônico da frente de dados; matemática Super ATR; motor financeiro e executor |
| Agente Expert | Novo consumidor MQL5 Super ATR, testes/contrato próprios e adaptação de perfil por commit revisado | Cálculo canônico, RiskGuard/SafeExecutor/PanelHost, fontes da outra frente do painel, dados e componentes visuais do Laboratório |
| Agente Dados existente | Cálculo/store/migração/materialização/query canônicos, incluindo SA-02 mediante seu contrato | Preserva WIPs dos consumidores; entrega série/envelope versionado para ambos |
| Integrador | Mapas centrais, contrato compartilhado e composição/publicação da prévia | Só compõe commits congelados por Git; evita edição simultânea de `cli.py`, SDK, catálogos, manifests e pins |

Cada agente cria worktree/branch próprias a partir da composição identificada.
Não usar a raiz com WIP como checkout de edição. Arquivo central compartilhado
tem **um dono por vez**; os demais entregam patch/commit para composição serial.
Teste nativo exige `tester_lease`, namespace e processos próprios. Não encerrar
Wine/MT5/ponte ou serviços globais de outro agente para obter a reserva.

### Acordo necessário com Dados — antes de consumir SA-02

O integrador e o dono de Dados congelam a definição do Super ATR e a forma do
envelope, reutilizando BD06/07/18. Campos necessários: ativo/contrato, fonte e
dataset/manifesto, TF/calendário/fuso, abertura/fim da barra, `available_at`, revisão,
ID/versão da fórmula, período/tipo de média/desvio/lado inicial, seed/início da
janela, nível/estado/ATR/virada e qualidade/warmup. Os nomes concretos seguem o DTO
canônico: esta lista é requisito, não um schema implementado por este documento.

O pedido de **coluna do indicador Super ATR na base** precisa resultar em algo
visível e consultável, inclusive exportação. Não sobrescrever uma coluna sem
identidade ao mudar parâmetros nem criar cinquenta colunas ambíguas por consumidor.
Fechamento nominal não comprova instante histórico de recebimento. Ausente,
provisório, inválido, zero válido, warmup e revisão não são o mesmo estado.
Sem disponibilidade histórica certificada, a UI/estudo deve declarar o limite.

Enquanto esse contrato estiver pendente, os agentes podem implementar UI e
testes com fixtures **declaradas**, e matemática/consumidor com séries congeladas.
Não declarar integração à base ou paridade ponta a ponta concluída por isso.

## Ordem e entrega

1. Laboratório inicia pelo reproduzível LAB-02/03 e catálogo real LAB-01, em PRs
   próprios; após isso LAB-04/08 e comparações LAB-05/06/07. Presets/SDK/atalho
   acompanham cada recorte, sem esperar o fim para testar a integração.
2. Em paralelo, Expert lê a memória e congela a variante SA-01; Dados mantém SA-02.
   Não esperar o redesenho visual para testar causalidade do consumidor.
3. Integrar serialmente bibliotecas, pins e leitores pelos contratos aprovados.
   Publicar fonte/commit/PR/push e prévia identificada; não promover canal aceito
   nem fazer merge automático por um gráfico parecer correto.
4. Aceite de cada item registra comandos, exit codes, identidade do runtime
   realmente lançado pelo atalho, seleção/dados sem segredos, capturas saneadas,
   limites e pendências. Aplicar testes negativos de warmup/NaN/lacunas/revisão,
   não esconder indisponibilidade para preencher a tela. Windows físico é pendência
   explícita; instalação/compilação/QA sintética não qualificam operação financeira.

Prompts completos, prontos para agentes independentes:

- [Laboratório, ferramentas e modernização](../repasse/PROMPT_LABORATORIO_GRAFICOS_20261024.md).
- [Expert Super ATR com confirmação entre TFs](../repasse/PROMPT_EXPERT_SUPER_ATR_MTF_20261024.md).

## Requisitos do proprietário e análise de integração — 13/11/2026

O proprietário priorizou o Expert funcional e solicitou o registro de duas
demandas de interface para discussão posterior às operações funcionais. A
referência é o vídeo `5ABRHXAaEKU`, com configurações aproximadamente entre
08:00–11:00, dashboard entre 16:30–17:40 e acompanhamento gráfico em 20:23.
O relatório textual do agente com acesso ao vídeo foi recebido em 13/11/2026.
Ele descreve áudio, parâmetros e capturas, sem entregar imagens anexadas,
fonte do EA, ticks ou livro de execuções. A análise abaixo cruza esse relato
com inspeção estática dos fontes atuais e os limites da evidência disponível.
[Vídeo](https://www.youtube.com/watch?v=5ABRHXAaEKU&t=1223s)

### Prioridade e sequência solicitadas

| Vinculação existente | Prioridade / dono | Marco desta demanda | Objetivo, dependência e aceite |
|---|---|---|---|
| LAB-09 · configurador guiado de presets | **Alta** · Laboratório e Presets | depende de operações funcionais | Discussão futura após operações funcionais do Expert Stop ATR Triplo desta demanda. Explicações, unidades, limites, agrupamento por sinal/gestão/risco e resumo das alterações. Aceite futuro inclui validação tipada e correspondência entre configuração exibida, revisão salva e parâmetros efetivamente utilizados. |
| LAB-05/06 · T20.22/23 · visão consolidada por experimento | **Alta** · Laboratório e integração | depende de operações funcionais | Discussão futura após operações funcionais do Expert desta demanda. Resultado líquido, drawdown, exposição, MAE/MFE e distribuição das operações, com filtros compartilhados por ativo, período e revisão de preset. Incluir avaliação de uma futura arquitetura de interface e eventual substituição tecnológica mencionada pelo proprietário como substituição de Qt. A fotografia atual do App usa Tkinter/ttk/Matplotlib e painéis HTML. A escolha de arquitetura pertence à discussão posterior. |
| SA-01 · nova variante Stop ATR Triplo e plot opcional | **P1** · Expert/HADOUKEN, com Dados e integração App | análise registrada, execução a assumir | Implementação pendente. O relato descreve três curvas no mesmo TF, período20 e desvios1/2/3, avaliadas após fechamento, com saída por consenso oposto. O candidato multi-TF existente possui outro contrato. Definir identidade própria para a variante e reaproveitar risco/execução. O painel deve alternar a apresentação do indicador. Fórmula, seed, reentrada, SL e compatibilidade do perfil ainda exigem fechamento de contrato. |
| LAB-08 · inspetor das decisões do Expert | **Alta** · Expert e Laboratório | análise registrada | Análise vinculada ao Expert. Selecionar uma operação e acompanhar sinal, confirmações, entrada, proteção e saída, com motivos e vínculo aos registros de execução. Distinguir a identidade da decisão, do pedido, dos fills e da posição. |

O marco de operações funcionais deve demonstrar sinal elegível, avaliação de
risco, envio, resultado observado, gestão e encerramento/reconciliação em
ambiente autorizado. A qualificação econômica e os aceites por plataforma
continuam com seus critérios específicos. A prioridade alta dos dois itens de
interface conserva a dependência temporal expressamente solicitada.

### Fontes conferidas e implicações para o Expert

| Componente | Fato observado no código | Implicação para a implementação |
|---|---|---|
| `robots/templates/SuperAtrResearch.mq5.tmpl` | Consome `HDK_SuperATR.mqh`, calcula barras fechadas e preenche `HDKSignal`. A definição do gerador exige TFs distintos e crescentes. A variante SA-01 usa o nível da curva como stop e sai na virada oposta da base mesmo sem confirmação. | Reaproveitar a composição e versionar a variante do vídeo com três parametrizações no mesmo TF e saída por consenso oposto. Manter a referência SA-REF-01 congelada. |
| `robots/super_atr_research.py` | Gera o pacote, declara inputs, seed, identidade e presets, com entradas de pesquisa inicialmente desligadas. | Incluir os novos campos e capacidades nos contratos, no pacote e na auditoria de inputs efetivos. |
| `analytics/super_atr.py` | Executa o header fixado através do adaptador existente e publica no store canônico. | Dados, Expert e visualização devem compartilhar fórmula, parâmetros, seed, revisão e semântica temporal. |
| HADOUKEN `HDK_StrategyRobot.mqh` | `StrategySignal` chega a `TryEnter`, `RiskGuard` e `SafeExecutor.OpenObserved`. O host oferece painel e gestão existentes. | Há um caminho de abertura já implementado. Examinar o vínculo durável da sessão e a recuperação de resultados incertos no perfil escolhido. |
| HADOUKEN `HDK_NativeBaseAdapter.mqh` | A entrada vinculada usa intenção persistida. O perfil atual exige Tester/DEMO, SL/TP positivos e constrói preços a partir das distâncias à cotação. | Comparar o contrato com stop absoluto na curva, stop ancorado no fechamento e saída por virada. Esse perfil requer adequação explícita caso a estratégia tenha TP ausente ou outra âncora de stop. |
| HADOUKEN `HDK_StudyGraphs.mqh` e `Indicators/HADOUKEN/Study/HDK_SuperATR.mq5` | O catálogo adiciona o indicador visual pelos defaults, com dez curvas no timeframe do gráfico. | Vincular o plot às três curvas, parâmetros e seed efetivamente utilizados pelo novo Expert. |
| `robots/parity.py`, `native_journal.py` e `session_audit.py` | Existem leitores e reconciliação de decisões, operações e journal nativo. | Reutilizar esses consumidores para o inspetor e ampliar o registro necessário à variante SA-01, incluindo decisões recusadas antes do envio. |

### Arquitetura proposta para discussão

1. Formalizar uma configuração versionada da variante do vídeo, com seed,
   relógio, aquecimento, gatilhos, stop, alvo e gestão. A referência SA-REF-01
   conserva sua identidade congelada e serve como comparação identificada.
2. Consumir o kernel Super ATR existente e produzir uma decisão identificada
   para cada oportunidade, com os valores e fechamentos utilizados.
3. Encaminhar a decisão pelo host e executor existentes, com intenção durável,
   revalidação de risco e acompanhamento do resultado. Conferir a compatibilidade
   do perfil de execução antes de adaptar os parâmetros da estratégia.
4. Hospedar o painel no próprio Expert e acrescentar o estado de apresentação
   do indicador. Armar, pausar e gerir posições seguem os controles operacionais
   próprios. A camada visual consome os dados e a configuração identificados
   da estratégia.
5. Renderizar as curvas operacionais, seus TFs, parâmetros e marcas de decisão.
   As três curvas pertencem ao mesmo TF no relato recebido. Valores confirmados
   e valores provisórios devem carregar seu estado temporal.
   A posse de objetos/handles deve permitir preservar estudos adicionados pelo
   usuário ao alternar a apresentação.
6. Vincular decisão, plano, resultado observado e posição aos registros existentes.
   O inspetor apresenta essa trilha por operação e consulta os serviços do App
   através do worker, com atualização de tela e I/O nos caminhos já adotados.

### Próximos passos e critérios de aceite

Por determinação posterior do proprietário, começar pelos itens corretivos
P0 detalhados em [Correções prioritárias](#correções-prioritárias). Esta ordem
antecede as ampliações do Expert e da interface nos caminhos afetados.

| Ordem | Entrega | Dependência e prova requerida |
|---|---|---|
| 1 | E14-CALC-01 → IND-CCI-01 → IND-AGGR-01 → E14-METRIC-01 · P0 | Reproduzir e corrigir contabilização, contagem, fórmulas, atualização incremental e indisponibilidade, com regressões e apuração reconciliada. |
| 2 | E14-SCOPE-01 e E14-EXEC-01 · P0 | Qualificar escopos e recuperação, corrigindo lacunas reproduzidas na execução existente. |
| 3 | SA-01-CONTRATO · P1 | Relatório recebido descreve três curvas no mesmo TF, período20 e fatores1/2/3. Fechar média ATR, base de preço, ratchet, seed, warmup, igualdade, gaps, reentrada após stop/retomada e prioridades de saída. Sem fonte ou valores de referência suficientes, declarar a equivalência ao vídeo como hipótese. |
| 4 | Encaixe no perfil de execução | Comparar o contrato auditado com `StrategyRobot` e o adaptador vinculado. Resolver âncora do stop e TP através do motor existente, usando os aceites corretivos aplicáveis. |
| 5 | Expert integrado | Exercitar compras, vendas, recusas de risco/dados e gestão pelos pontos oficiais do App. Reconciliar decisões e operações com registros identificados. |
| 6 | SA-01-PLOT · P1 | Provar correspondência das curvas confirmadas com o sinal e equivalência de decisões/ordens em ensaio pareado com plot ligado/desligado. Exercitar alternância e ciclo de vida dos objetos próprios. |
| 7 | Inspetor de decisões | Selecionar uma operação e recuperar seu sinal, confirmações, parâmetros, motivos, execução e saída pelo mesmo run e revisão. Estados incompletos devem permanecer identificados. |
| 8 | Qualificação operacional | Testes focados de causalidade, warmup, revisão, gaps, deduplicação e recuperação, seguidos de compilação, Tester visual e percurso DEMO autorizado. Registrar recursos e executar uma carga por vez. |
| 9 | Discussão da interface futura | Após as correções aplicáveis e o marco de operações funcionais, retomar o configurador guiado, o dashboard consolidado e a avaliação de arquitetura gráfica com os contratos de serviço existentes. |

Este adendo registra requisitos e análise de integração. As evidências nativas
do novo perfil e a fidelidade à estratégia do vídeo dependem das entregas
indicadas. A continuidade desta demanda fica neste backlog existente, sob os
owners e contratos já adotados pelo projeto.

### Avaliação por área após recebimento do relatório

As prioridades abaixo pertencem a este complemento. A execução futura deve
reconferir posse, branch/PR e composição. O diagnóstico foi feito no checkout
App `550b6d5`, com WIP preservado, HADOUKEN `d4d389e` e Safety `27b8599`.
Os caminhos são relativos ao componente indicado. O artefato instalado e os
aceites nativos desta variante permanecem por verificar.

| Área / vínculo existente | Reaproveitamento comprovado nos fontes | Melhoria, dependência e aceite propostos |
|---|---|---|
| Sinal / SA-01 | `robots/super_atr_research.py`, template e kernel HADOUKEN existentes | Três curvas no mesmo TF, consenso confirmado e manutenção durante divergência, com identidade versionada. Aceite por sequência de barras e decisões esperadas, incluindo ausência, revisão, warmup, igualdade, gaps e attach com consenso já formado. Divergência entre bandas é estado do indicador. Seu benefício como filtro depende de pesquisa. |
| Matemática / SA-02 | `analytics/super_atr.py` executa o header fixado pelo adaptador existente e publica no store canônico | Kernel oferece SMA/EMA, seed e ratchet definidos. Confirmar equivalência à fórmula do vídeo antes de alegar réplica. Expert, produtor e plot devem usar o mesmo contrato, prefixo, parâmetros e corte. Mudança de fórmula requer versão e reconciliação dos componentes. |
| Entrada / Expert e integração | Host `HDK_StrategyRobot.mqh`, `HDK_SafeExecutor.mqh` e adaptador `HDK_NativeBaseAdapter.mqh` | Resolver a compatibilidade de stop absoluto/TP ausente com o adaptador vinculado que exige distâncias SL/TP positivas. Reutilizar intenção durável, revalidação, reserva e journal existentes. Aceite inclui repetição, reinício, envio aceito sem confirmação, parcial e recusa, conservando identidade e resultado consultável. |
| Reversão / Expert e Safety | Saída por sinal e reconciliação de reservas no adaptador nativo | Definir sequência de fechamento, consulta até comprovar posição encerrada, nova avaliação de risco e abertura oposta. Incerteza ou parcial mantém acompanhamento e impede nova abertura. O perfil atual declara ausência de reversão na mesma barra. Alterar esse comportamento exige contrato próprio e testes nos modos netting/hedging suportados. |
| SL/TP / proteção comum E14 | SL obrigatório no manifesto de pesquisa, validação de risco e regras de proteção existentes | Preservar SL protetivo no broker como proposta para o novo perfil. A referência do vídeo usa SL0 por padrão, portanto resultados dessa adaptação precisam de rótulo próprio. Definir âncora, tick, stops/freeze, volume e saída estratégica, incluindo TP ausente se contratado. |
| BE/TS / E14 | `HDK_PositionPolicy.mqh` já combina BE e trailing sem afrouxar SL. BE usa R inicial, trailing nativo é amostrado por barra fechada | Acrescentar offset, passo e eventual cadência por tick somente em versão explícita. O relato usa pontos e trailing intrabar. Aceite compara gatilho, preço executável, rounding, custos, stop confirmado e persistência. O aumento300→600 foi uma demonstração. Sua superioridade econômica depende de validação. |
| Stop por N barras / E14 e pesquisa | `trade_analytics/execution/modes.py` possui `trailing_barras` | Qualificar a tradução para proteção nativa compartilhada. Usar N barras fechadas, avanço monotônico e um controlador do SL. Comparar saída com o modelo de pesquisa e conferir preço executável, gaps e histórico incompleto. |
| Intervenção externa / Safety | Executor restringe alterações inválidas e proteção combina candidatos que melhoram SL | Diferença de `POSITION_SL` só comprova mudança observada. Rastrear último SL confirmado, pedidos próprios em voo e identidade da posição antes de atribuir origem. Definir suspensão/retomada da gestão e tratamento de remoção/alargamento segundo contrato de risco. |
| Limites financeiros / E14 | `HDK_RiskGuard.mqh` já usa equity da conta, âncoras persistidas, limite diário/semanal, DD, margem e risco aberto/pendente | Meta de ganho em dinheiro e escopo por estratégia exigem extensão identificada. Mostrar separadamente P&L da conta e do robô. Qualificar início da sessão, carregamento de posições, depósitos/saques e rollover. Persistir bloqueio e acompanhar zeragem incompleta. Limiares disparam pedidos e podem ser ultrapassados durante execução. |
| Sessão / políticas nativas | Políticas de entrada e saída por sessão existentes | Distinguir início, limite de novas entradas e zeragem. Revalidar calendário/servidor, contrato vencido, ausência de tick e mercado fechado. Pausa de entrada conserva gestão das posições. Exibir posição restante e motivo quando o fechamento estiver impedido. |
| Posse e portfólio / Safety | Guardas de identidade/exposição/reservas e `robots/portfolio.py` | Usar Magic integral compatível com o protocolo e identidade de conta/servidor/símbolo/instância. O snapshot sinaliza contribuição netting não resolvida. Comprovar posse antes de gerir exposição compartilhada. Validar especificações do símbolo e custos pelo contrato, sem tabela B3/Forex improvisada. |
| Pânico / painel e Safety | `HDK_PanelHost.mqh` pausa, cancela pendentes do Magic, fecha posições e consulta remanescentes | Tornar o escopo explícito, pois a zeragem atual alcança símbolos do mesmo Magic. Vincular pedidos/resultado/recuperação ao mecanismo durável. Aceite com pendente que executa durante cancelamento, posição parcial, indisponibilidade e proteção de posições de outros owners. |
| Painel operacional / E14 | Estado de conexão/permissão/pausa, capacidades, histórico, relógio e adaptação DPI existentes | Acrescentar consenso3/3, barra confirmada, motivo de recusa, idade da cotação, perfil/preset efetivos e estado de execução incerta. Separar posição aberta, elegibilidade de entrada e gestão disponível. Validar legibilidade, escala, teclado, estados e falhas de conexão no alvo real. |
| Plot opcional / SA-01 e LAB-08 | Catálogo de Estudo e indicador nativo com dez curvas | `HDK_StudyGraphs.mqh` chama `iCustom` pelos defaults. Passar configuração/seed efetivas e limitar apresentação às curvas contratadas. Hospedar no mesmo EA, preservar objetos/estudos/tema alheios e desenhar gatilhos BE/TS confirmados. Aceite pareado com plot ligado/desligado e histórico prefixado idêntico. |
| Contabilização / E14 e analytics | Estatísticas DIA/SEM/MÊS no painel e ledger/agrupamentos no App | Revisar comissões por fill, parciais, `INOUT`, taxas e contagem de ciclos. Em `HDK_TradeStats.Refresh`, cada saída soma a primeira comissão de entrada encontrada para a posição. Exemplo em memória com comissão−2 e lucros de duas saídas10/20 resulta26, enquanto custo único resulta28. A rotina também conta duas saídas. É reprodução aritmética derivada do fonte, com validação nativa pendente. |
| Indisponibilidade das métricas / E14 e analytics | Analytics distingue estados ausentes e deduplica runs no agregado | `TradeStats.Refresh` zera acumuladores antes de `HistorySelect`, que pode falhar. Implementação futura precisa apresentar falha/frescura/cobertura. Revisar rótulos PF indefinido, aberto/fechado, custo conhecido e denominador por operação. |
| Extremos e exposição / Dados e analytics | Serviço `trade_analytics/trajectories` preserva cobertura e recusa alegar caminho contínuo sem prova | Distinguir máximo/mínimo do P&L diário, MAE/MFE por ciclo, drawdown, exposição nocional, margem e risco até SL. Medir na fonte qualificada e renderizar por timer. Preservar âncora da sessão e floating inicial para posições carregadas, além de atribuição de taxas. |
| Inspetor / T20.23 e LAB-08 | `parity.py`, `native_journal.py`, `session_audit.py`, ledger e trajetórias | Ligar run/revisão/decisão/plano/pedido/fills/posição/proteção/saída, incluindo recusas e registros incompletos. Cursor temporal mostra somente informação disponível no instante. Aceite com parciais, reinício, resultado incerto e divergência identificada entre pesquisa e execução. |
| Pesquisa e paridade / SA-01 e validação | Motor, snapshots, custos, reconciliação e header compartilhados existentes | Testar componentes separadamente sob as mesmas condições, com protocolo e orçamento congelados, custos conhecidos e período fora da amostra. Paridade compara decisões, pedidos, fills, posição, proteção e P&L. Dois extremos próximos fornecem uma observação limitada. A causa das diferenças do vídeo permanece hipótese. |
| Desempenho e host / owners de cada componente | Timer/render, produtor com orçamento/cancelamento e fila existentes | Reduzir redraw, consultas e reprocessamento repetido mantendo seed/revisão/causalidade. `OnTick` pode agrupar eventos de novas cotações durante processamento, portanto a cobertura tick a tick exige evidência específica. Um job por vez, medição de recursos e limites do host antes de cargas. Timer de2s pertence à apresentação. |
| Configurador / LAB-09, Alta após operações funcionais | Default/Manual, campos tipados, compiler, presets e pacote existentes | Fluxo guiado sinal→gestão→risco→revisão, unidades pontos/ticks/preço/R/moeda, campos condicionais, erros úteis e resumo de diferenças. Aceite relaciona configuração exibida, revisão salva, pacote e inputs observados no EA. |
| Dashboard / LAB-05/06, T20.22/23, Alta após operações funcionais | Agregados de robô/ativo, curvas e correlação no App | Consolidar P&L líquido, drawdown, exposição, MAE/MFE e distribuições, com filtros compartilhados e comparação elegível. What-if por exclusão é recorte histórico descritivo. Avaliar carteira futura exige re-simulação de risco, margem e regras sob seleção temporal. |
| Interface futura / LAB-10 e integração | App usa Tkinter/ttk/Matplotlib, possui HTML e servidor local de ações limitadas | Avaliar arquitetura gráfica depois do marco solicitado. `robots/panel_server.py` já tem loopback, chave por sessão, validação de origem, catálogo de ações e um trabalho. Execução por gráfico web acrescentaria contrato de autenticação, autorização, validade/intenção durável/reconciliação e cancelamento OCO sujeito a corridas, usando os serviços/execução existentes. |
| Desenvolvimento por IA / fluxo de engenharia | Instruções, gerador/scaffold, contratos e gates existentes | Especificar casos e limites antes da geração, revisar incrementos, preservar hashes e separar código compilado de comportamento validado. MCP dá acesso a ferramentas, com permissões vigentes. Qualificar os efeitos reais e a recuperação do que for gerado. |

### Limites da evidência econômica e próximos marcos

O relatório informa16 pregões, resultado bruto simulado2098→2843BRL e ajuste
gain300/loss650 sobre o mesmo recorte, sem custos/slippage e sem evidência
entregue de teste fora da amostra. Aumento de35,5% é descrição desse cenário.
O ganho futuro e a robustez do preset permanecem sem demonstração. Offset de
BE positivo e acionamento de uma trava diária continuam sujeitos a custos,
gaps, liquidez e preço de execução.

As três bandas compartilham preço, período e volatilidade. O experimento deve
comparar banda lenta isolada, pares e consenso triplo sob os mesmos dados,
custos, risco e regras. Depois mede separadamente período, saída, BE/TS e
travas diárias. Registrar todas as tentativas e selecionar antes do holdout.

Os extremos827/−41 externos e826/−31 do painel fornecem dois pontos de
comparação. A hipótese de amostragem2s explicar a divergência exige dados e
rastro de cálculo. As descrições do vídeo também fornecem horas de servidor,
mas seus exemplos não certificam calendário, disponibilidade histórica ou
equivalência de execução do MT5X.

1. **HADOUKEN-E14, painel e Analytics** executam E14-CALC-01 e
   E14-METRIC-01 em P0, com regressão e correção dos cálculos e estados.
2. **Safety e Expert** executam E14-SCOPE-01 e E14-EXEC-01 em P0,
   qualificando escopo e recuperação e corrigindo lacunas demonstradas.
3. **Expert, Dados e integrador** fecham a variante e o perfil de execução.
   Aceite inclui fórmula/seed, causalidade, SL/TP, reentrada e reversão.
4. **Expert e Safety** integram entrada/gestão/saída e recuperação consultável.
   Aceite negativo cobre repetição, parcial, incerteza, reinício e posse.
5. **Painel e Expert** vinculam plot opcional, gatilhos e estados aos dados
   efetivos. Contabilização precisa de regressão de fills/custos e cobertura.
6. **QA e operador** qualificam compilação, Tester e DEMO autorizada pelos
   caminhos oficiais, com evidências por alvo e recursos registrados.
7. **Pesquisa e Dados** comparam componentes e custos fora da amostra,
   preservando períodos, tentativas e motivos de reprovação.
8. **Laboratório e integração** retomam os dois itens Alta de interface após
   operações funcionais e avaliam arquitetura gráfica com contratos existentes.

Esta passagem entregou avaliação e atualização local deste complemento.
O ensaio em memória usa números sintéticos. Código executável, pins, índice
Git e processos foram preservados. Compilação, suíte, CI, MT5/Tester e conta
DEMO desta variante ficaram fora desta passagem de análise.

### Correções prioritárias

Determinação do proprietário em 13/11/2026. **A atuação corretiva tem
prioridade P0**, incluindo análise, reprodução, implementação integrada e
regressão. Os resultados produzidos pelo caminho afetado precisam de
reconciliação após a correção antes de sustentar seleção de presets ou aceite
das métricas. Os registros históricos conservam o escopo dos ensaios originais.

O estado operacional e a posse pertencem à fila de `CONTINUAR_AQUI.md`.
Os seis itens foram registrados como livres, com branch/PR e último SHA
pendentes. Os responsáveis abaixo designam componentes. O agente executor
assume sua identidade na fila antes da escrita, após conferir código, WIP,
owner e PR atuais. Cada implementação deve preservar pins e reconciliar a
composição pelo integrador. A priorização documental deixa a correção do
produto pendente de implementação e evidência.

#### E14-CALC-01 · P0 · Comissões, fills, parciais e ciclos

- **Responsáveis** HADOUKEN-E14 e Analytics, com integração App/Safety.
- **Evidência** `HDK_TradeStats.mqh`, `Refresh`, reaplica a primeira comissão
  encontrada para `position_id` em cada saída e incrementa a contagem a cada
  deal de saída. Exemplo sintético com comissão−2 e lucros10/20 produz26,
  com resultado esperado28 ao cobrar a entrada uma vez. A reprodução feita
  foi aritmética em memória. A qualificação no consumidor nativo está pendente.
- **Próxima ação** reproduzir pelo consumidor oficial, definir o contrato de
  atribuição de custos e contagem, criar a regressão e corrigir a rotina comum
  e seus consumidores afetados, incluindo o resultado exibido no painel.
- **Aceite** comissões/taxas por fill atribuídas uma vez, múltiplas entradas
  agregadas corretamente, parciais e `INOUT` conciliados, recortes DIA/SEM/MÊS
  coerentes e contagem de deals/ciclos identificada. Cobrir posição atravessando
  períodos e histórico da entrada fora da janela. Reconciliar líquido,
  taxa de acerto e PF com casos próprios de resultado conhecido. Qualificar
  sem operações, sem perdas e custos ausentes. Registrar diferenças nos
  relatórios dependentes. Este item é distinto de DIAG-EXCURSOES/DIAG-AMOSTRA.

#### E14-METRIC-01 · P0 · Histórico indisponível e estado das métricas

- **Responsáveis** painel HADOUKEN e Analytics.
- **Evidência** `HDK_TradeStats.Refresh` zera acumuladores e arrays antes de
  `HistorySelect`; a falha retorna com os valores zerados. Falha nativa e
  propagação visual precisam de reprodução dirigida. Usa o contexto contábil
  de E14-CALC-01 para qualificar as métricas resultantes.
- **Próxima ação** reproduzir falha e leitura incompleta, integrar estado de
  disponibilidade/cobertura/frescor e tratar atualização recuperada no serviço
  existente. Qualificar o último valor válido quando ele for apresentado.
- **Aceite** distinguir zero válido, ausência de operações, leitura parcial,
  indisponibilidade e valor antigo. Cobrir falha antes/depois de uma leitura
  válida, recuperação e atualização do painel. A seleção e as ações afetadas
  dependem de contexto qualificado e revalidação no ato.

#### E14-SCOPE-01 · P0 · Escopo de métricas, travas e zeragem

- **Responsáveis** Safety, painel e Analytics.
- **Evidência** `RiskGuard.DayPL` deriva da equity da conta; `TradeStats` filtra
  Magic; `PanelHost.CloseEverything` alcança os símbolos do Magic. O código
  comprova os escopos. Seu enquadramento como defeito depende da política e
  dos rótulos efetivos, que precisam de qualificação.
- **Próxima ação** conferir contrato e UI, reproduzir ambiguidades e corrigir
  rótulos, parâmetros e integração dos controles afetados. Identificar
  moeda, conta/servidor, Magic/símbolos e âncora da sessão.
- **Aceite** resultado e trava com alcance declarado, tratamento de posições
  carregadas e depósitos/saques conforme política, bloqueios persistidos e
  ação de emergência com alcance explícito. Separar extremos do P&L diário,
  MAE/MFE por ciclo, DD, margem e risco até SL. Validar preservação de posições
  alheias e estados desconhecidos, inclusive contribuição netting não resolvida.

#### E14-EXEC-01 · P0 · Incerteza, reinício, parcial e reversão

- **Responsáveis** Expert-HADOUKEN e Safety.
- **Evidência** intenção durável e reconciliação existem no adaptador e executor.
  A cobertura nos consumidores vigentes StrategyRobot/NativeBaseAdapter e sua
  recuperação precisa de reconferência. O perfil atual declara ausência de
  reversão na mesma barra. Este P0 qualifica o comportamento vigente e corrige
  lacunas comprovadas. A compatibilidade do futuro Triplo é SA-01-CONTRATO.
- **Próxima ação** reconferir vínculo do consumidor ao executor, reproduzir
  aceitação sem confirmação, parcial, interrupção e reinício com IDs estáveis,
  e corrigir falhas demonstradas reutilizando journal e reservas existentes.
- **Aceite** intenção persistida antes do efeito, consulta de resultado e
  reconciliação sem duplicação. Aplicar a política de saída/reversão contratada
  pelo consumidor, incluindo a recusa quando a reversão estiver indisponível.
  Reversão permitida depende de encerramento observado e nova avaliação de
  risco. Dados ou estado incompletos conservam o resultado
  como incerto. Provar os limites com processos e árvores próprios e registrar
  separadamente compilação, simulação, Tester, DEMO autorizada e Windows físico.

#### SA-01-CONTRATO e SA-01-PLOT · P1 · Integração após correções aplicáveis

- **Responsáveis** Expert, Dados e integração para o contrato; Expert e painel
  para o plot. As fichas e aceites anteriores deste adendo permanecem aplicáveis.
- **Evidência** consumidor atual multi-TF/saída pela base, relato com três bandas
  no mesmo TF/consenso oposto, fórmula do vídeo sem comprovação suficiente,
  perfil vinculado com SL/TP positivos e plot atual pelos defaults/dez curvas.
- **Próxima ação** fechar a variante e a compatibilidade do perfil, depois
  vincular as três curvas e parâmetros/seed efetivos ao plot opcional.
- **Aceite** contrato versionado, correspondência matemática declarada,
  referência SA-REF-01 identificada, SL protetivo e TP conforme contrato,
  reentrada/reversão definidas e ensaio pareado com visual ligado/desligado.
  Aplicar os aceites corretivos P0 aos caminhos usados pela variante.

#### Orientação de execução e encerramento para agentes

1. Conferir ponta, PR, owner, working tree e se o achado já foi corrigido no
   fluxo atual antes de assumir o ID. Registrar evidência quando já atendido.
2. Executar o primeiro recorte P0 viável, com um escritor no checkout e uma
   carga pesada por vez, medindo recursos antes e durante a carga pertinente.
3. Registrar reprodução, ambiente, comandos, causa, regressão e correção mínima
   no consumidor compartilhado. Propagar aos caminhos reais afetados.
4. Registrar testes focados, gates e diferença entre QA simulada e nativa,
   além de branch/PR/SHA e composição que efetivamente recebeu a correção.
5. Encerrar como concluído somente o recorte comprovado. Uma dependência
   externa recebe estado bloqueado com causa e próxima ação exatas. Seguir
   para o próximo item independente e revisar os bloqueios ao fim.

Configurador guiado e dashboard consolidado continuam com prioridade Alta,
após as correções aplicáveis e as operações funcionais do Expert.

Referências técnicas oficiais consultadas

- [Eventos OnTick](https://www.mql5.com/en/docs/event_handlers/ontick).
- [Transações e ordem de chegada](https://www.mql5.com/en/docs/event_handlers/ontradetransaction).
- [Especificações do símbolo](https://www.mql5.com/en/docs/constants/environment_state/marketinfoconstants).
- [Otimização e validação forward](https://www.metatrader5.com/en/terminal/help/algotrading/strategy_optimization).
