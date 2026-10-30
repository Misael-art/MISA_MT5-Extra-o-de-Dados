# Plano de implementação — estrutura estratégica de dados e consumidores

Data: 22/10/2026. Revisão2. Estado: planejamento revisado para execução; não representa implementação concluída.

Esta revisão corrige a ordem por capacidades, explicita os contratos de migração/consumo e acrescenta gates verificáveis. As seções6 e10–17 são a referência operacional para as fichas; os documentos anteriores continuam preservados.

## 1. Direção definida pelo proprietário

O alvo principal é construir o projeto, seus mecanismos e suas ferramentas:
criação/atualização de bases, indicadores, cálculos compostos, análises,
relacionamentos e consumo. Os ativos exportáveis pela conta DEMO são uma
bancada de construção e exercícios, sem compromisso de uso futuro.

Não haverá campanha para homologar o EURUSD ou outro ativo da bancada,
acumular históricos extensos sem necessidade ou selecionar uma estratégia
vencedora nesta frente. Eficiência financeira observada num ativo não se
transfere para outro ativo, fonte, contrato ou período.

Qualificar nesta frente significa demonstrar que o MECANISMO reconhece a
qualidade das entradas, calcula corretamente, representa suas limitações e
entrega resultados consistentes aos consumidores. A presença de uma lacuna
na fonte DEMO deve produzir um estado explícito e testável; não exige coletar
mais mercado para liberar o desenvolvimento das outras capacidades.

## 2. Resultado esperado

Uma cadeia reutilizável e extensível:

fontes/bases identificadas → ingestão validada → dados normalizados → catálogo
e cálculos simples → dependências/cálculos compostos → materialização versionada
→ relações/contexto → serviço de consulta → Laboratório, análises, exportação,
Estudo e futura API.

Uma expansão deve percorrer essa cadeia por contratos e dependências. Adicionar
um indicador ou base não pode exigir editar listas manuais em todos os
consumidores, invalidar indiscriminadamente o acervo ou confundir identidades.
O algoritmo e os metadados específicos da extensão continuam exigindo definição.

## 3. Base de execução e reuso

A fotografia inicial descreve App321272f0b24d, HADOUKEN6fb02374cd65 e
Safetyc5a3e2162cac. Ela permanece como fotografia histórica. A primeira ficha
conferirá novamente as bases Git, branches, entregas integradas e fontes
instaladas; seus estados documentais não serão usados como prova de runtime.

Reusar, corrigir ou completar as estruturas existentes:

| Responsabilidade | Base existente |
|---|---|
| Captura e persistência | mt5_connector, historical_extractor, tick_extractor, book_collector, database_manager |
| Fontes/instrumentos/qualidade | analytics.datasets, instruments, data_eligibility |
| Tempo e sessões | timeframes, analytics.calendar, resampling |
| Definições e valores | analytics.indicator_registry, features, feature_store, target_registry |
| Cálculo e atualização | strategies.indicators, analytics.producers, producer_jobs, indicator_materializer |
| Contexto de operações | trade_analytics.ledger, enrich, cache e trajectories |
| Contrato de relações | analytics.relation_dto e frentes F18 já existentes |
| Consumidores | SDK, CLI, Laboratório, UI Relações, exportadores e chart_study |
| Recursos | resources, write_guard e orçamento/checkpoints existentes |
| Nativo, quando atingido | HADOUKEN/Study, adaptadores; distribuição Safety apenas quando necessária |

As fichas BD abaixo organizam a execução desta orientação. Não criam um segundo
roadmap, registry, store, ledger, scheduler ou motor estatístico. Cada ficha
deverá ser vinculada ao dono T13/T18/T19/T20/F15 correspondente. Uma mudança
implementada em outra frente será consumida e verificada, sem reimplementação.

## 4. Regras transversais

- Bancada compacta, determinística e descartável; fontes reais pequenas apenas
  quando acrescentarem evidência técnica. Nenhuma extração ampla automática.
- Reutilizar uma amostra em vários testes, sem copiar a base principal por run.
- Corrigir a arquitetura, sem tentar preencher todas as colunas antigas de um
  ativo só para aparentar completude. Preservar compatibilidade do legado.
- Schema aditivo; dados anteriores e versões antigas continuam consultáveis.
- Identidade inclui base/fonte/instrumento/contexto. Mesmo símbolo textual em
  duas bases não prova que os instrumentos ou feeds sejam equivalentes.
- Ausência, zero, inválido, aquecimento, revisão, expiração e falta de capacidade
  são estados distintos. Aproveitar estados existentes; mapear sem inventar
  enums concorrentes.
- Hora da observação, confirmação, disponibilidade e captura são distintas.
  Horário desconhecido continua desconhecido; sem conversão UTC presumida.
- Cada variante de fórmula declara parâmetros, semente, janela, unidades e
  disponibilidade. Mesmo nome não concede equivalência Python/MT5/legado.
- Revisões afetam somente dependências alcançadas. Resultados históricos não
  são reescritos nem promovidos para outra versão silenciosamente.
- Orçamento de disco/memória/tempo e cancelamento são parte do funcionamento.
- Limites de risco e políticas de ativação continuam em seus pilares; esta
  frente fornece dados/contratos, sem alterar permissões ou operar contas.

## 5. Ordem e fichas de implementação

### Fase A — fechar o inventário técnico e a bancada

#### BD01 — matriz única de lacunas e consumidores

**Vinculação:** T19.1/T20.1 e mapa existente. **Depende de:** orientação atual.

Conferir cada indicador, fonte, cálculo composto e consumidor: definição,
implementação, persistência, atualização, exposição e teste. Incluir as52
definições hoje registradas, os38 nativos, famílias Analytics, indicadores
legados/avançados e fichas I01–I18/AX pertinentes. Separar indicadores de setups
e ferramentas; a contagem52 não é teto nem prova de cobertura integral.

**Aceite:** cada item tem ID, dono, variante, dependências, estágio, lacuna,
teste necessário e consumidor. Toda lacuna descoberta recebe ficha vinculada;
nenhum item some por ser difícil. Código já pronto será marcado para reuso.

#### BD02 — bancada compacta e política de artefatos

**Vinculação:** F15/T20.4/24/25, infraestrutura de testes existente. **Depende de:** BD01.

Montar massas pequenas: tendência, lateralidade, empate, gaps, duplicidade,
entrada fora de ordem, revisão, ausência, volume zero/ausente, preços em
escalas diferentes, contratos/lotes distintos e calendário/fuso. Usar duas
bases isoladas com símbolos iguais e dados diferentes. Criar exemplos de
negócios/agressor e book quando a regra exigir, sempre rotulados como sintéticos.

Proposta inicial de teto:100MiB para fixtures persistentes e250MiB para dados
temporários de teste, medidos e ajustados na ficha se houver justificativa.
Esses tetos não incluem a instalação compartilhada de Python/MT5/Wine, nem
autorizam apagar arquivos do usuário. Artefatos nativos têm orçamento próprio.
Massas de carga são geradas temporariamente, sem guardar históricos extensos.

**Aceite:** casos reproduzíveis, valores esperados independentes, inventário de
bytes/artefatos, limpeza somente dos temporários próprios e ausência de downloads
necessários à suíte. As amostras precisam cobrir aquecimentos reais dos algoritmos.

### Fase B — robustecer o mecanismo de múltiplas bases

#### BD03 — identidade e capacidades de fontes/bases

**Vinculação:** T20.4/5/6. **Depende de:** BD01/02.

Estender os contratos existentes para distinguir origem/base, provedor/feed,
instrumento, símbolo/alias, calendário, revisão, unidade e capacidade. Conferir
onde hoje o contexto usa apenas símbolo/TF e impede isolamento adequado.
Qualquer extensão de chave/schema terá migração aditiva e leitura do legado.

**Aceite:** duas bases com o mesmo símbolo não misturam valores, cache,
correlações, notas ou contexto; uma terceira fonte pode usar o contrato sem
alterar os consumidores. Capacidades ausentes produzem diagnóstico previsível.

#### BD04 — ingestão idempotente e qualidade por capacidade

**Vinculação:** F1/F4, T20.6. **Depende de:** BD03.

Conferir captura por blocos, falhas/retomada, duplicidade, revisão tardia,
gravação parcial e validação dos campos necessários. Distinguir volume real de
ticks, cotação de negócio e book de fila. Padronizar origem e motivos no contrato
existente de qualidade; integrar leitores externos pelo mesmo mecanismo.

**Aceite:** repetição não duplica; correção conserva histórico de revisão;
falha não anuncia sucesso; estado incompleto/ilegível não vira zero; mudanças
de capacidade da fonte são propagadas aos consumidores.

#### BD05 — tempo, sessões e derivação entre timeframes

**Vinculação:** T13.1/T11.1, T20.5/6. **Depende de:** BD03/04.

Qualificar observação/disponibilidade, barras abertas/fechadas, sessões e
revisões. Alinhar timeframes por informação já disponível. Semanal/mensal
terão regra explícita de calendário/provedor; separar novas subfichas se faltarem
implementações, sem reduzi-los silenciosamente a minutos fixos. Operar quando
o offset for desconhecido exige capacidade/restrição explícita por consumidor.

**Aceite:** M5 não usa H1 ainda aberto; pivô não aparece antes da confirmação;
calendários noturnos, feriados/lacunas e mudanças de fuso são casos de teste;
limitações do provedor não contaminam os demais módulos.

### Fase C — completar e harmonizar indicadores

#### BD06 — contrato completo do catálogo

**Vinculação:** T18.1/T19.3. **Depende de:** BD01/03/05.

Completar ID, fórmula/variante, parâmetros tipados, unidades, entrada/saída,
tipo/buffers, aquecimento, alinhamento, regra de ausência, revisão e consumidores.
Acrescentar metadados para apresentação/plotagem quando necessários. Gerar o
catálogo dos consumidores a partir desse contrato; não criar listas paralelas.

**Aceite:**100% do inventário possui ficha de contrato e estado de suporte.
Ausência de fórmula é pendência explícita; não adotar regra por adivinhação.
Alterações incompatíveis produzem versão identificada e conservam a anterior.

#### BD07 — reconciliar calculadores existentes

**Vinculação:** T19.3 e base T6.1. **Depende de:** BD06.

Confrontar IndicatorCalculator, Advanced/EnhancedIndicators, funções puras,
registry e nativos. Definir implementações canônicas e variantes que precisam
ser conservadas; fazer os caminhos de coleta/consumo delegarem quando houver
equivalência comprovada. Resolver dependência opcional que muda silenciosamente
a cobertura. A indisponibilidade deve ser explícita e testada.

**Aceite:** mesma variante/entrada gera resultado coerente nos consumidores;
colunas antigas continuam legíveis; cada diferença legítima é nomeada; testes
com e sem dependência opcional não produzem sucesso falso.

#### BD08 — qualificar o conjunto já calculável

**Vinculação:** T19.3–7/T13.3, conforme a família. **Depende de:** BD06/07, por família. BD08a qualifica a primeira família e BD08b completa as demais.

Reusar e verificar RSI/ATR/MACD/canais/retornos/ER/Alligator/AO/AC/pivôs e
produtores de volume/regime/estrutura/calendário. Tratar warmup, denominador,
NaN, semente, precisão, deslocamento e disponibilidade. Cada família divergente
vira seu recorte por PR; o plano não obriga reescrever algoritmos corretos.

**Aceite:** referências independentes, prefixos históricos estáveis, limites
e versões cobertos. Os critérios medem exatidão funcional, sem exigir PF/lucro.

#### BD09 — completar famílias faltantes do catálogo

**Vinculação:** fichas restantes T19.7–10 e AX correspondentes. **Depende de:** BD06/07 e o recorte pertinente de BD08; não depende de todas as outras famílias.

Completar Envelopes/regressão, McGinley, Ultimate, TRIX/KST e demais cálculos
identificados como faltantes. Ficha repetível por família, com regras e unidades
fechadas antes do algoritmo. Incluir estado/projeção/consumo, não apenas função.

**Aceite:** cada item fecha o percurso cálculo→store→consulta→consumidor, ou
permanece explicitamente pendente quando sua especificação ainda não existe.
O marco “catálogo completo” não fecha enquanto houver algoritmo definido faltante.

#### BD10 — fluxo, book e indicadores dependentes de fonte específica

**Vinculação:** T19.11–13 e AX de fonte. **Depende de:** BD03/04/06.

Implementar/testar agressão, CVD, imbalance, velocidade da fita e grandes
negócios conforme contratos e dados exigidos. Book imbalance e agressão são
capacidades distintas; snapshots não fornecem identidade/prioridade de fila.
Regras relativas de tamanho não provam identidade institucional.

**Aceite:** algoritmos com entradas válidas funcionam em fixtures referenciais;
fontes sem a capacidade devolvem motivo explícito; uma fonte compatível nova
habilita o cálculo sem reescrever Laboratório/API. Validação em fonte real
permanece identificada quando a DEMO não permite essa prova.

#### BD11 — integrar os indicadores nativos e adicionais pertinentes

**Vinculação:** T19.3, G19.5–7, fichas AX. **Depende de:** BD06/07.

Conferir contrato dos38 nativos/buffers, parâmetros, seeds/offsets e captura.
Definir quais valores são calculados offline e quais requerem MT5; persistir
capturas identificadas quando o consumo offline depender delas. Tratar demais
indicadores aprovados do inventário por subficha. FRP/Black Box/regras ainda
sem especificação permanecem pendências próprias, não implementações fictícias.

**Aceite:** terminal ausente não provoca dado inventado ou quebra geral; valores
capturados conservam fonte/versão/disponibilidade; equivalência com Python só
é afirmada nos recortes efetivamente comparados.

### Fase D — cálculos compostos, revisões e resiliência

#### BD12 — dependências e execução de cálculos compostos

**Vinculação:** T13.2/3 e T19.3/21. **Depende de:** BD05/06/BD08a; compostos adicionais dependem somente de seus pais.

Estender o grafo existente de dependências para executar racionais compostos
com entradas tipadas, alinhamento causal e ordem definida. Conferir ciclos,
dependência ausente, unidades incompatíveis e maturidade. Evitar motor paralelo
de expressões; qualquer extensão de expressividade terá contrato e testes.

**Aceite:** composição de pelo menos dois níveis; ausência de um pai produz
estado coerente nos descendentes; adicionar um ramo não altera outro; nenhum
resultado usa dado disponível somente no futuro.

**Adendo BD12-SNAPSHOT (29/10/2026):** `difference` e `absolute` usam o grafo e
executor existentes sobre contratos/pais de um snapshot4, sem completar entradas
com dados atuais. Dois níveis conferidos por aritmética independente, revisões
tardias, expiração, ausência e ramo independente; versões explícitas, unidades
fraction/price_units e mesma observação. A CLI/SDK preserva o contexto após
exportação/importação; não grava valores filhos nem infere direção operacional.
107 testes dirigidos e launcher oficial com fixture própria; [contrato e
limites](../sdk/api.md#composições-congeladas-bd12-snapshot). O aceite deste recorte
não encerra outras operações/tipos, granularidade BD13, materialização BD14 ou
consumidores completos BD19. A assinatura ainda cobre módulos inteiros.

#### BD13 — revisão, cache e invalidação por dependência

**Vinculação:** T20.17/25, T13.2/3b. **Depende de:** BD12.

Rever a assinatura global que hoje pode versionar muitos indicadores quando
um arquivo do registry muda. Introduzir granularidade por algoritmo/contrato e
dependências, com migração identificada e leitura das versões antigas. Incluir
fonte, parâmetros, calendário, contexto e alinhamento nas identidades aplicáveis.

**Aceite:** correção de A recalcula descendentes de A, sem invalidar B sem
dependência; adicionar indicador não altera séries anteriores sem motivo;
consulta histórica preserva o que era conhecido. Tolerâncias não aproximam hashes.

**Adendo BD13-RETURNS (29/10/2026):** primeira redução de revisões no materializador
existente: retornos usam fingerprints dos extremos, com recibos da janela ordinal
para não antecipar remoção/restauração de barras. [Contrato, baseline e limites](BD08A_RETORNO.md#bd13-returns--revisões-por-dependência-29102026).
Não encerra a assinatura global, a execução seletiva nem as demais famílias.

**Adendo BD13-SIGNATURES (29/10/2026):** os três retornos qualificados usam
contrato por ID e componentes AST de fórmula/revisão/orquestração, com identidades
próprias por horizonte. Contexto padrão escolhe a versão por ID; fatos antigos
continuam legíveis explicitamente, sem fallback. [Contrato e limites](BD06_CONTRATO.md#ordem-das-assinaturas-resposta-ao-r05).
Outras49 definições e funções compartilhadas conservam granularidade mais ampla;
não há novo motor/store/cache ou conclusão de BD13 integral.

**Adendo BD13-COMPOSITION (29/10/2026):** identidade de cada receita congelada
liga operador, contrato e pais; proveniência/source_revision incluem o hash de
cálculo. Somente componentes usados entram na assinatura; helpers compartilhados
permanecem conservadores. Envelope2 conserva receita1, sem escrever fatos filhos
nem alterar versões qualificadas dos retornos. [Contrato e migração](../sdk/api.md#composições-congeladas-bd12-snapshot).
Materialização/recuperação BD14 e demais famílias continuam na sequência própria.

#### BD14 — materialização e recuperação

**Vinculação:** T19.3/T13.3b/T20.24/25. **Depende de:** BD04/12/13.

Verificar batch, append, correção anterior ao checkpoint, revisão durante
consulta/gravação, concorrência, interrupção e indisponibilidade de disco.
Estados recursivos devem ser exatos; otimização não pode alterar a semente.
Reusar orçamento, fila e write_guard existentes, com medições de recursos.

**Aceite:** retomada equivale ao processamento completo de referência; não
publica COMPLETE sobre fonte antiga; falhas não duplicam fatos; memória e disco
ficam dentro do orçamento declarado; nenhum consumidor recebe sucesso parcial.

**Adendo BD14-OBSERVATION (29/10/2026):** o contexto congelado agora pode fixar
uma observação e reconstruir suas revisões, preservando a leitura padrão da
observação mais recente. Recibos tardios e vencimentos vigentes têm escopo
explícito; não ampliam a cobertura de mercado. Validade dos pais é propagada
pelos dois níveis. [API e limites](../sdk/api.md#revisões-de-uma-observação-congelada-bd14-observation).
É uma preparação para materializar revisões reais, sem transformar pontos de
uma grade em eventos novos; ainda não grava fatos filhos. A gravação precisa
deduplicar revisões já existentes e respeitar a monotonicidade de recebimento
do store, além dos gates de interrupção/concorrência acima.

**Adendo BD14-INTERRUPTION (29/10/2026):** os dois produtores existentes agora
encerram o medidor RSS também quando a execução sai por `KeyboardInterrupt`,
`SystemExit` ou outra `BaseException`; a interrupção original é relançada.
Cancelamento grava `CANCELLED`, outras falhas gravam `FAILED`, sem avançar
checkpoint fora da transação de fatos. O fechamento normal interrompe o medidor
antes de medir disco ou atualizar o recibo. Se o recibo não puder ser gravado,
o erro original é preservado e o diagnóstico identifica o run cujo estado não
foi persistido; esse registro pode continuar `RUNNING`, sem alegação de aceite.
Lotes já confirmados são a referência para retomada. Morte abrupta do processo
e reconciliação automática de runs órfãos, materialização de compostos e os
demais gates BD14 continuam pendentes.

**Adendo BD14-MATERIALIZE (29/10/2026):** primeira cadeia congelada persistida
pelo store/journal/grafo existentes, com eventos por observação e corte UTC
explícito. As receitas permanecem imutáveis; saídas ganham versões próprias,
proveniência e validade. Duas passagens verificam o histórico/resultado antes
de confirmar lotes e checkpoint. A repetição preserva revisões e manifesto do
prefixo; novas entradas precisam conter os pais já usados pelas origens
anteriores. Namespace, geração concorrente, monotonicidade, corrupção e
interrupção não são convertidos em sucesso parcial. Configuração por run é
aditiva no journal; não há nova fila, armazenamento ou executor.
[API, orçamento e limites](../sdk/api.md#materialização-da-cadeia-congelada-bd14-materialize).
O aceite é de snapshot/corte declarados, sem certificação da fonte live ou
qualificação nativa/financeira. SIGKILL/runs órfãos e o restante de BD14
permanecem abertos.

**Adendo BD14-ORPHAN (29/10/2026):** a recuperação do journal passa a distinguir
proprietário local encerrado de processo ativo ou não verificável. A identidade
é aditiva, compartilhando o helper de propriedade já usado pelo orçamento de
Robôs; não há novo executor. Reconciliação condicional marca `INTERRUPTED` sem
alterar fatos/checkpoints nem inventar métricas finais. Início de novo run usa
o mesmo mecanismo no escopo ativo/timeframe/família; CLI/SDK permitem inspeção
e reconciliação explícita. Legados, outra máquina/boot/namespace ou acesso
negado são preservados. [Contrato e limites](../sdk/api.md#recuperação-de-jobs-órfãos-bd14-orphan).
Esse recorte não encerra avaliação em lote eficiente, demais famílias ou gates
nativos/financeiros/Windows físico.

**Adendo BD14-BATCH (29/10/2026):** blocos de até256 eventos atravessam
observações usando o mesmo contexto, executor e as-of canônicos. Escopo e
ausências permanecem por observação; o estado mantido não é reiniciado na
fronteira do bloco. Séries de identidade completa usam junção vetorizada;
expansão parcial/heterogênea mantém o contrato existente. Guardas conferem
também troca da função/código do operador antes da gravação/conclusão. Duas
passagens, limites e lote SQL permanecem; não há novo motor ou cache retido.
Identidades acompanham o código, preservando as versões/fatos anteriores.
[Contrato](../sdk/api.md#avaliação-de-eventos-em-lote-bd14-batch).
Outras famílias, relações completas e gates nativos/financeiros seguem abertos.

### Fase E — relacionamentos efetivos e contexto

#### BD15 — serviço real de relações entre indicadores

**Vinculação:** T18.2/6 e dependências específicas T13.4. **Depende de:** BD08a/BD12 para BD15a (cálculo referencial); BD14/BD15a para BD15b (serviço persistido).

Inspecionar e consumir a implementação de associações já existente em branch,
integrando só o recorte necessário. Implementar pares, métodos/defasagens,
amostra, unidades/transformações e ausência/constância. Persistir identidades e
reusar relation_dto. Etapas estatísticas adicionais mantêm seus donos. A correção do serviço pode ser demonstrada em dados referenciais; não exigir uma estratégia lucrativa ou uma campanha financeira OOS para concluir esse mecanismo.

**Aceite:** relações conhecidas positivas, negativas, independentes e degeneradas
reproduzidas; sinais de coeficiente não viram ganho financeiro; operações
sem informação suficiente retornam inconclusão e motivo.

**Adendo BD15a (29/10/2026):** o módulo canônico `associations.py` da branch
T18.2 (`76404a1`) foi integrado, sem trazer a infraestrutura de pesquisa inteira.
Pearson/Spearman, diagnósticos, bootstrap temporal e correção de família são
reutilizados. A consulta `sdk.query_feature_relation`/CLI lê versões explícitas
do snapshot4, calendário conhecido no corte e, para compostos, manifests de
origem imutáveis. Não há fallback para fatos/contratos live ou calendário civil
inferido. Defasagem nominal não comprime lacunas nem atravessa sessões.
DTO2 distingue séries pareadas de alvos futuros; DTO1 continua compatível.
Tipos, ausência/constância, população, exclusões e fontes distintas são explícitos.
Textos numéricos/booleanos misturados e identidades incompletas não entram como
números contínuos. O resultado é exploratório, sem utilidade financeira ou
causalidade certificada. [API e limites](../sdk/api.md#relações-referenciais-congeladas-bd15a).
Não persiste relações nem substitui tentativas/OOS, BD15b, cruzamento entre bases
ou adaptação completa da UI. O mecanismo referencial não depende de negociação.

**Adendo BD15b (29/10/2026):** persistência referencial no `feature_store`
existente, com tabela aditiva, identidade compartilhada pelo cálculo/DTO,
hashes e referências de snapshots confirmados atomicamente. `relation --persist`
e `relation-result`, também no SDK, publicam/leem o resultado original.
Repetição/concorrência preserva o primeiro recibo; conflito e falta de
integridade/origem são recusados. Método, defasagem, população e revisões
ficam preservados. Consulta sem `--persist` continua somente leitura.
Recursos/relógio ficam no primeiro recibo, fora da identidade semântica.
Não cria outro store/executor nem certifica o resultado financeiramente.
Catálogo/paginação de pares, UI completa e demais alvos seguem nas suas fichas.
[Contrato](../sdk/api.md#persistência-de-relações-referenciais-bd15b).

#### BD16 — expansão das relações e bases cruzadas

**Vinculação:** T18.5/6, isolamento T20.25. **Depende de:** BD03/05/13/BD15b.

Incluir janelas móveis, atualização dos pares afetados e cruzamento explícito
de fontes/bases compatíveis. Exigir identidade/alinhamento antes de juntar
ativos/feeds; não unir pelo símbolo textual. Gerar pares sob seleção/orçamento,
sem materializar automaticamente todas as combinações possíveis.

**Aceite:** novo indicador entra nas relações elegíveis; nova base fica isolada
até cruzamento explícito; revisão invalida só relações alcançadas; paginação e
orçamento evitam crescimento quadrático descontrolado.

**Adendo BD16-WINDOWS (29/10/2026):** a consulta de janelas usa o serviço
canônico, prepara a captura/dois canais uma vez e mantém a matemática existente.
Janela/avanço contam fechamentos observados; defasagem continua nominal e ambos
os fechamentos precisam estar dentro do recorte UTC. População, exclusões,
aquecimento, cauda, sobreposição e família Bonferroni são explícitos. Não salva
campanhas automaticamente; salvar a seleção publica um DTO3 pelo store e
transação existentes. DTO1/2 continuam legíveis, leitor antigo recusa DTO3.
[API, fluxo de tela e orçamento](../sdk/api.md#janelas-móveis-de-pares-congelados-bd16-windows).
O corte é fixo e retrospectivo; elegibilidade da captura inteira permanece
obrigatória. Recapturar muda as identidades: invalidação incremental por trecho,
expansão de pares e cruzamento explícito de bases seguem pendentes. Este recorte
não conclui os aceites completos de BD16 nem qualifica causalidade/utilidade.

#### BD17 — ligar contexto de estratégias e operações

**Vinculação:** T16.3/5/T19.14/T18.3/4. **Depende de:** BD05/14/BD15b e recorte do ledger; cruzamento entre bases só quando usado.

Vincular sinal/configuração/execução/resultado às versões dos dados e cálculos.
Conferir direção, parciais/ciclos, MAE/MFE e população usada por cada análise.
Para alvos futuros, respeitar maturidade e separar rótulos de pesquisa de inputs
operacionais. Convergências seguem a infraestrutura temporal validada existente.

**Aceite:** exemplo pequeno rastreável de ponta a ponta; não duplica resultado
por parcial; a análise usa o contexto conhecido na decisão; número do gráfico
confere com o mesmo universo consultado no serviço.

### Fase F — consumidores e preparação da API

#### BD18 — serviço de consulta único e contrato público

**Vinculação:** T18.6/9, SDK existente/T13.6 conforme escopo. **Depende de:** BD06/BD08a para BD18a (catálogo e séries do piloto); BD14/BD15b/BD16 para BD18b (contrato completo).

Definir consultas de catálogo, fontes/capacidades, séries/qualidade, valores
históricos/as-of, compostos e relações. Contratos incluem versão, unidades,
proveniência, recorte, frescor, motivos e paginação. JSON sem NaN/infinito e
erros previsíveis. API de aplicação existente permanece compatível.

**Aceite:** a mesma seleção retorna os mesmos fatos no serviço, SDK e
serialização; limites/paginação e versões antigas têm comportamento definido;
consulta funciona sem UI e sem MT5 quando os valores necessários estão persistidos.
Pronto para futura API exige serviço executável, consultas e serialização testados. Não exige publicar servidor HTTP nesta frente. BD18a permite integrar um consumidor cedo; BD18b acrescenta compostos/relações e reconcilia o contrato integral.

#### BD19 — integrar consumidores pelo mesmo contrato

**Vinculação:** T18.8/T13.6/T19.3, G19.5–7 e F5. **Depende de:** BD18a para BD19c1 (primeira consulta/CLI); BD18b e a capacidade utilizada para os demais consumidores.

Subfichas separadas por consumidor: BD19a Laboratório/análises (usa BD17); BD19b Relações
com dados efetivos (usa BD15b); BD19c1 primeira consulta/CLI e BD19c2 CLI/exportação
com contrato completo; BD19d Estudo/notas/gráficos (usa BD11). Reusar
design atual. Estados sintéticos continuam apenas no modo de demonstração/teste.

**Aceite:** novo indicador aparece quando compatível e recebe motivo quando
não; consultas/export/gráfico reconciliam valores, unidades e versões; cálculo
fica fora dos widgets. Nenhuma lista fixa exige alteração por consumidor.

**Adendo BD19b-RELATIONS (29/10/2026):** primeiro consumidor de DTO2 persistidos
no Laboratório atual, em Matriz → Relações salvas. O índice paginado reutiliza
`query_service`, SDK/CLI e cursores; payloads são lidos somente para a página,
um por vez. Seleção confere as origens por `load_feature_relation`, sem calcular
nos widgets. Coeficiente, versões/unidades, amostra/exclusões e selo sintético
ficam visíveis; detalhes guardam identificadores e inferência. Respostas antigas
não substituem filtro/seleção atuais. Usa `run_bg`/barra existentes e recolhe o
contexto específico de Séries enquanto o par salvo tem seu próprio recorte.
Não encerra BD18b, criação visual de pares, outros tipos/alvos ou exportação.
[Contrato](../sdk/api.md#consulta-de-relações-salvas-no-laboratório-bd19b).

**Adendo BD19b-CREATE (29/10/2026):** Criar par usa as capturas do relatório
congelado em Séries ou do resultado selecionado. O SDK expõe somente os contratos
dessa captura, sem completar com versões atuais. Formulário numérico com corte,
amostra e recursos explícitos; `run_bg`, cancelamento e política de escrita
obtida no início da operação reutilizam o mecanismo existente. O ID confirmado
é selecionado diretamente, mesmo fora da primeira página. Troca/erro de captura
limpa contexto e opções; callbacks antigos são descartados. Códigos categóricos
congelados não viram medida contínua por terem declaração legada NUMBER; cálculo
e DTO usam a mesma regra. Não reescreve artefatos, reduz mínimos automaticamente,
ativa Expert ou concede evidência financeira. Outras famílias/alvos e contrato
completo continuam nas fichas próprias.
[Contrato](../sdk/api.md#seleção-congelada-para-criar-pares-bd19b-create).

**Adendo BD19c2-RELATIONS (29/10/2026):** SDK/CLI transferem o recibo original
e todos os snapshots declarados da relação referencial, pelos stores existentes.
O destino valida hashes, contratos, contexto e conjunto de origens antes de
publicar snapshots/artefato/referências numa transação. Não recalcula, promove
qualificação, completa com fatos live, altera a fonte ativa do destino ou
sobrescreve conflitos. Repetição preserva o primeiro recibo local e reexportação
desse recibo conserva os bytes. Resultados materializados e indisponíveis
mantêm ancestralidade e motivos. Publicação de arquivo reutiliza o mecanismo
atômico do pacote Manual. Pacote limitado a64MiB/32 snapshots; timeout, memória,
cancelamento e política de escrita reutilizam Analytics/Recursos.
O Laboratório lê os resultados importados mesmo sem barras/fatos live da
origem. Outras famílias/alvos, janelas móveis e cruzamento explícito de fontes
continuam nas suas fichas; a transferência não encerra BD18b/BD19 completos.
[Contrato](../sdk/api.md#transferência-de-relações-congeladas-bd19c2-relations).

**Adendo BD19b-TRANSFER (29/10/2026):** o Laboratório passa a escolher arquivos
para o mesmo SDK de transferência. Ações em linha própria, exportação somente
da seleção conferida, importação sem exigir dados ativos da origem e foco no
ID confirmado. Filtros/seleção/novas ações ficam suspensos durante o trabalho;
`run_bg`, espera e cancelamento existentes são reutilizados. Mudança de contexto
não aplica uma resposta antiga; a política de escrita fica vinculada ao destino
escolhido antes do dispatch. O fechamento sinaliza cancelamento sem desfazer um
commit confirmado. Erros e repetição mantêm recibos, arquivos e qualificação.
[Contrato](../sdk/api.md#transferência-visual-no-laboratório-bd19b-transfer).

### Fase G — demonstrar expansão e fechar a composição

#### BD20 — provas de expansão

**Vinculação:** T20.26/29 e contratos anteriores. **Depende de:** BD08b/09/10/11/16/17/BD18b e todos os recortes BD19.

Executar testes completos de: indicador numérico novo; composto dependente;
saída categórica/multibuffer quando aplicável; nova base com mesmo símbolo;
novo provedor/capacidade; revisão de fórmula/fonte e desativação de versão.

**Aceite:** extensão registrada percorre cálculo→store→relações→consulta→
consumidores sem adaptações dispersas; resultados anteriores são preservados;
o recálculo é limitado às dependências; recursos são medidos. Cada consumidor
incompatível explica sua restrição sem impedir os outros.

#### BD21 — composição, regressão e instalação

**Vinculação:** T19.23/29, T20.29 e distribuição existente. **Depende de:** BD20.

Reconciliar fontes, capacidades e fingerprints reais dos perfis afetados.
As seis recusas de presets registradas na fotografia inicial recebem correção
própria vinculada, sem aprovação cega de hashes. Quando H/S mudarem, compilar
consumidores/ativos e comparar fontes efetivamente instaladas.

**Aceite:** biblioteca real passa nos contratos de composição aplicáveis,
legado abre, fluxos anteriores permanecem utilizáveis e a versão validada pode
ser instalada pelo mesmo atalho com retorno. Nenhuma negociação ao vivo é
necessária para demonstrar o mecanismo de dados.

#### BD22 — fotografia final e encerramento por evidência

**Vinculação:** continuidade/status existentes e este espelho. **Depende de:** BD01–21.

Publicar matriz por capacidade: planejada, implementada, verificada em fixture,
verificada em fonte real disponível e integrada aos consumidores. Listar fontes
não oferecidas pela DEMO e pendências de especificação separadamente. Guardar
artefatos pequenos, hashes, recursos e resultados da prova de expansão.

**Aceite:** nenhum item do inventário sem estado/justificativa/dono; todas as
fichas de runtime declaradas concluídas têm evidência e consumidores; pendência
de especificação/algoritmo definido faltante impede declarar catálogo integral
concluído. Restrição de feed não impede declarar o mecanismo correto no escopo
comprovado, mantendo a prova externa pendente.

## 6. Dependências e entregas intermediárias — ordem revisada

O fluxo é por CAPACIDADE. Não há uma barreira que exija concluir todos os
indicadores antes de testar um consumidor. Os marcos agregam evidência, sem
impedir uma cadeia vertical pequena de funcionar cedo.

| Marco | Conteúdo | Critério de passagem |
|---|---|---|
| M1 — mapa e bancada | BD01/02 | Inventário com donos, massas referenciais e orçamento |
| M2 — núcleo e primeiro consumo | BD03–07, BD08a, BD18a, BD19c1 | Fonte→valor canônico→consulta/CLI reproduzível |
| M3 — primeira cadeia completa | BD12–14, BD15a/b, recorte BD16, BD18b, BD19b | Simples→composto→relação→consulta→tela com dados de bancada |
| M4 — catálogo ampliado | BD08b/09/10/11 | Todas as famílias do escopo com contrato e evidência própria |
| M5 — consumidores reconciliados | BD16/17, BD18b, BD19a/b/c2/d | Seleções equivalentes, estados e resultados coerentes |
| M6 — expansão e falhas | BD20, bateria transversal | Expansão, migração, isolamento e recuperação comprovados |
| M7 — composição disponível | BD21/22 | Fontes instaladas verificadas, atalho e fotografia final |

A lista exata de nós/dependências está no [grafo de execução](grafo_execucao_dados_20261022_rev2.json). Há22 fichas
lógicas e29 nós de execução, pois BD08/15/18/19 têm recortes explícitos. Famílias
e repositórios poderão exigir PRs adicionais, sempre com dono e escopo delimitado.
BD21 não impede antecipar uma correção de compatibilidade necessária ao piloto.

**Adendo de execução (23/10/2026, R05):** o piloto BD06→BD07→BD08a→BD18a→BD19c1 roda com a
assinatura global vigente como limitação temporária explícita; o grafo não muda e BD13 continua
após BD12. Metadados de contrato (BD06) ficam fora do hash de implementação. Ver
[BD06](BD06_CONTRATO.md).

Quando uma fonte real não existir na DEMO, comprovar o algoritmo com referência
apropriada e manter a prova externa pendente. Quando faltar a própria regra,
registrar impedimento de especificação e continuar capacidades independentes.
Nenhuma dessas situações autoriza marcar funcionalidade ausente como concluída.

## 7. Testes estruturais obrigatórios

| Caso | O que precisa demonstrar |
|---|---|
| Mesmo símbolo em duas bases | Isolamento de valores, contexto, caches e relações |
| Inserção repetida | Idempotência sem perda de revisão |
| Revisão tardia | Invalidação/recomputação dos descendentes, preservação as-of |
| Futuro acrescentado | Prefixo conhecido não muda indevidamente |
| M5 com confirmação H1 | Usa somente a informação H1 disponível |
| Fonte sem volume real/agressor | Limitação explícita; não substitui por ticks sem regra |
| Indicador recursivo em lotes | Estado/semente coerentes com referência completa |
| Cancelar/falhar/retomar | Resultado final equivalente, sem sucesso parcial |
| Duas escritas concorrentes | Identidade/revisão/checkpoint consistentes |
| Dependência ausente/cíclica | Falha explicada e localizada |
| Saída sem valor/constante | Zero, ausência, aquecimento e correlação indefinida distintos |
| Novo indicador/novo composto | Aparece na cadeia e atualiza dependências elegíveis |
| Leitura antiga | Versões e bancos anteriores permanecem utilizáveis |
| Interface versus consulta/export | Mesmo recorte e valores conciliados |
| Restrição de disco/memória | Interrupção controlada e retomada íntegra |

Referências numéricas devem ser independentes do algoritmo testado; tolerâncias
justificadas por fórmula e escala. Testes de engenharia não exigem corretora nem
resultado financeiro positivo. Provas nativas só onde necessárias, em perfil
isolado e sem ordens ao vivo. SQLite e PostgreSQL/caminhos Windows são verificados
conforme o recorte; Windows físico permanece pendência própria quando não medido.

## 8. Disciplina de execução

1. Antes do primeiro código: reconciliar tarefa/dono/base, ler instruções dos
   repositórios e preservar o WIP existente. Não trabalhar diretamente sobre
   alterações alheias da raiz.
2. Usar branch/worktree isolada codex/, ambiente obrigatório e baseline de testes
   conforme AGENTS.md. Uma tarefa do plano canônico por PR.
3. Se a ficha não corresponder ao código, registrar a divergência e ajustar o
   planejamento do recorte antes de implementar, conforme o fluxo do projeto.
4. Rodar testes pertinentes, suíte/doctor exigidos e shellcheck se houver SH.
   Não repetir campanhas numéricas/financeiras sem uma questão técnica concreta.
5. Atualizar status/continuidade e matriz de evidências na entrega. IDs BD são
   referência desta priorização; não substituem responsáveis existentes.
6. Instalar apenas composições validadas no canal de desenvolvimento, preservando
   dados/preferências/versões aceitas e o caminho do atalho usado para testar.
7. Não apagar bases/exports do usuário para cumprir orçamento. Limpeza desta
   frente limita-se a temporários próprios; qualquer política de retenção nova
   terá escopo explícito, preservando referências e usando recursos existentes.

## 9. Definição de conclusão desta frente

- A estrutura recebe e distingue bases/fontes diferentes por contrato.
- O catálogo aprovado tem suas fórmulas implementadas, testadas e expostas, com
  limitações de feed explicitamente tratadas; não há lacunas ocultas por coluna/lista.
- Indicadores simples e compostos têm tempo, qualidade e revisões coerentes.
- Relações e cruzamentos funcionam sobre os dados identificados e atualizam com
  expansão/revisão, dentro de orçamento.
- Laboratório, análises, exportadores e Estudo consomem os mesmos contratos.
- A camada de consulta está pronta para uma futura API, sem precisar de UI.
- Uma expansão completa foi demonstrada, incluindo preservação do que já existia.
- O armazenamento e as provas permanecem compactos e o avanço está fotografado.

A homologação de ativos produtivos, rentabilidade de estratégias, ativação
adaptativa de robôs e publicação externa da API serão frentes posteriores,
apoiadas nesta estrutura e em evidências específicas de seus contextos.

## 10. Catálogo de fechamento e rastreabilidade

BD01 gera um inventário versionado, sem limite artificial de52/38. Para cada
item: ID estável; origem no catálogo/estudo; variante; dono canônico; capacidade
de entrada; unidades; regra; dependências; implementação; persistência; consulta;
consumidores; testes/evidências; limitação de fonte; falta de especificação.

Para cada lacuna, abrir um recorte com comportamento esperado e referência que
reproduza a falha. Não preencher uma matriz com “suportado” só porque há código,
coluna, mock ou item de menu. Não desconsiderar uma família por exigir fonte nova.

O catálogo de escopo terá três classes: (A) regras definidas, implementar e
verificar integralmente; (B) algoritmo definido, fonte real indisponível na
DEMO, provar mecanismo e manter prova externa identificada; (C) regra ainda
indefinida, explicitar a decisão faltante. ItensC não são exclusões silenciosas:
impedem declarar o catálogo original integral concluído até resolver sua definição.
Testes sintéticos não transformam um itemB em validado com feed real.

Definir e validar também tipos numéricos, booleanos, categóricos e multibuffer.
Desativar uma definição conserva leitura da versão e das evidências antigas.
Novos tipos precisam de suporte genérico próprio; descoberta automática não
inventa fórmula, regra visual ou método estatístico.

## 11. Contratos mínimos e múltiplas bases

Contrato de entrada: identidade estável da base/fonte e do instrumento;
família de dados; timezone/calendário conhecidos ou explicitamente desconhecidos;
especificação aplicável; revisão/origem; distinção entre observado e sintético.

Contrato de valor: identidade de contexto; definição/variante/versão; parâmetros;
janela; unidade; valor/tipo; observação/confirmação/disponibilidade/captura;
qualidade e motivo; revisão da fonte e linhagem; estado de atualização.

Contrato composto: pais e versões; alinhamento temporal; fórmula/expressão
permitida; unidade resultante; aquecimento; regra para ausência/revisão de pais;
custo de processamento; método de recálculo e referência independente.

Contrato de relação: identidades das séries e seleção; transformação/unidades;
método/defasagem; população; janela; n_pares/sessões; coeficiente/efeito;
estado para constância/insuficiência; versões; disponibilidade dos rótulos;
proveniência e frescor. Reusar relation_dto e o serviço canônico existente.

Contrato de consulta: seleção explícita por base/fonte/instrumento/TF/versão/
intervalo/as-of; paginação com ordem estável; projeção de colunas; limite;
envelope de metadados; erros estruturados; comportamento legado documentado.
Transformações e normalizações são explícitas; não modificar escalas para fazer
correlações ou gráficos parecerem coerentes.

Adaptar essas semânticas aos contratos existentes. Nomes acima são requisitos,
não ordens para criar módulos/tabelas/colunas duplicadas. Evitar o produto
cartesiano de todas as combinações e não iniciar cálculos caros/escritas
silenciosas numa consulta de leitura.

## 12. Migração e compatibilidade

Antes de alterar chaves, mapear todos os leitores/escritores atingidos,
incluindo snapshots/notas, ledger/contexto, cache e exports. A identidade de
namespace do legado deve ser estável entre reinícios. Não inventar feed ou
instrumento antigo desconhecido; conservar sua limitação sem misturar bases.

Migração aditiva, testada em banco pequeno de versão anterior: abertura,
leituras/escritas pertinentes, repetição da migração, interrupção e retomada.
Não apagar/renomear tabelas do usuário. Migração grande tem plano de lote,
índices e espaço temporário medidos. Testar fonteA/baseA contra fonteB/baseB
com nomes idênticos e parâmetros diferentes; defaults não podem cruzar contextos.

A volta de versão usa código/links anteriores compatíveis e preserva extensões
aditivas; não executa DROP para desfazer trabalho. Não escrever na base principal
para provar migração. Banco da bancada basta para validar o mecanismo.

Assinaturas devem separar mudança de cálculo de alteração meramente visual.
Código compartilhado que afeta várias fórmulas invalida seus dependentes reais;
metadado de apresentação não deve provocar recálculo numérico sem necessidade.
Incluir bibliotecas/configurações que alterem a semântica. Hashes continuam
exatos, mesmo quando a referência numérica usa tolerância justificada.

## 13. Significado do aceite de relações e futura API

O mecanismo de relações deve ser matematicamente correto e lidar com dados
constantes, poucos pares, ausências, defasagens e revisões. Demonstrar relação
conhecida em fixture não demonstra utilidade financeira. OOS, seleção de
vencedores e ativação dos robôs não são pré-requisito para fechar uma consulta
descritiva correta; inferências que utilizem esses rótulos mantêm requisitos
e estados próprios, sem exibir aprovação que a evidência não sustenta.

A API futura terá como fundamento um serviço executável testado hoje, com
serialização, versões, isolamento, limites e erros verificáveis. “Pronto para
API” não significa apenas um documento de endpoints. Não publicar servidor
nem criar autenticação/transporte externo como escopo lateral desta frente.

Indicadores dependentes do MT5 podem ser consultados offline a partir de
capturas materializadas, com origem/idade/limites explícitos. Sem captura válida,
responder indisponibilidade. Não recriar todos os indicadores nativos em Python
apenas para simular disponibilidade, nem exigir terminal para ler dados já salvos.

## 14. Bateria de aceite por mecanismo

A [matriz de aceites](matriz_aceites_dados_20261022_rev2.json) contém36 casos E01–E36 e seu dono BD. Cada evidência
deverá apontar commit, teste, referência, entrada/hash, parâmetros, resultado,
limitação e recursos. Estados: planejado; em execução; passou; falhou; bloqueado
por especificação; prova externa pendente. Não chamar passagem de stub de integração.

Nenhum percentual substitui casos. Tolerâncias por algoritmo serão justificadas
antes da comparação. Avaliar lotes/recursão, causalidade, type/unit checking,
source isolation, retries/concorrência, rollback, paginação, frescor e ausência.
SQLite e PostgreSQL usam seus caminhos reais de transação no ambiente disponível;
simular is_windows cobre caminhos, não comprova MT5 em Windows físico.

O aceite agregado exige a cadeia completa: fonte→valor simples→composto→relação
→consulta/DTO→consumidor/export. Depois repetir com novo indicador e outra base,
preservando resultados anteriores. Os testes de extensão devem falhar se listas
manuais ou caches incorretos impedirem a propagação.

## 15. Execução por PR e fotografia

Cada recorte começa com ficha canônica vinculada, base Git e baseline; termina
com testes pertinentes, suíte/doctor exigidos, revisão, evidência e status. Não
misturar famílias sob donos distintos ou consertar indiscriminadamente o WIP.
Primeira documentação canônica do plano será incorporada pelo recorteBD01 em
docs/, retirando caminhos pessoais. Artefatos deste pacote são locais/ignorados.

Se um teste inicial falhar: registrar e atribuir à versão; distinguir falha
preexistente da introduzida. Não enfraquecer teste para obter verde, não declarar
suíte completa a partir de testes dirigidos. Investigar primeiro a causa; reparar
o pré-requisito no recorte correto antes de atribuir aceite ao dependente.

Fotografias incrementais preservam: contexto/revisão; contrato; capacidade;
estado anterior/atual; prova; consumo efetivo; migração/rollback; recursos;
pendência e primeira ficha elegível. Não substituir a fotografia inicial por uma
versão mais favorável. Handoffs devem registrar processos próprios, arquivos,
worktrees, PRs e comando de retomada, sem segredos.

## 16. Instalação, preservação do host e falhas

A raiz original está em branch antiga com WIP amplo; releases instaladas não
são checkouts para editar. Criar worktrees a partir de fontes Git reconciliadas.
Os instaladores podem tocar atalhos: fotografar os dois atalhos e os links antes
da preparação, conferir e restaurar apenas o que o próprio preparo alterou.
Não sobrescrever uma alteração concorrente do usuário.

Antes da ativação, revalidar a composição exata, guardar retorno, arquivos
originais e identidades; conferir manifests, schemas compatíveis, catálogo,
consulta e GUI. O mesmo atalho deve apontar ao canal de desenvolvimento validado.
Não promover canais aceitos por inferência. Versões de dados/cliente/algoritmo
precisam permanecer coerentes no rollback.

Nunca encerrar Wine/MT5/processos alheios por nome. Se a troca de release exigir
trocar o dono canônico da ponte, usar a API da revisão original com posse
comprovada, depois iniciar a nova; não editar tokens/PIDs para adotá-la.
Se houver falha, preservar recibo e voltar o canal anterior pelo caminho próprio.

Não reutilizar cegamente scripts antigos de ativação com hashes, nomes de
diretórios e contagens fixas da G19. Reusar APIs e adaptar gates à nova fonte.
Temporários são identificados por owner/escopo; bancos, logs e exports do usuário
não viram candidatos a limpeza apenas para satisfazer um teto.

## 17. Encerramento sem esconder pendências

Entregar o quadro do catálogo inteiro e todos os consumidores prometidos. Uma
primeira cadeia funcional é marco intermediário, não conclusão integral.
Classificar separadamente: algoritmo/contrato faltante; erro de implementação;
integração faltante; dados do feed ausentes; validação externa/Windows físico.
Os três primeiros impedem dizer que a funcionalidade correspondente foi entregue.

Uma declaração final válida identifica precisamente o que está implementado,
testado, integrado e instalado, e as provas externas que permanecem pendentes.
Não usar “robusto”, “resiliente”, “todos” ou “pronto para API” sem a matriz de
evidências que os sustenta. A avaliação financeira de ativos futuros, política
adaptativa de risco e operação produtiva permanecem em seus próprios pilares.

## 18. Menções na internet — quantidade, positivo e negativo

Registro: 29/10/2026. **Estado: plano solicitado pelo proprietário; implementação
pendente.** Identificador da extensão: **MEN**. Vinculação: BD03/04/05/06/14/18/19,
T13.2, T16.3, F18 e T20.4/6/12/22/23. Este capítulo estende o plano canônico;
não institui outro roadmap, motor estatístico, catálogo ou armazenamento.
Prompt de execução: [MEN — implementação integrada](../repasse/PROMPT_MENCOES_MERCADO_20261029.md).

### 18.1. Decisão do proprietário e fronteira funcional

Capturar a presença de ativos, empresas e temas na internet e disponibilizar
**três medidas quantitativas: quantidade, positivo e negativo**. São entradas
para os mecanismos existentes de indicadores, médias, relações e estratégias.
Uma menção ou mudança de sentimento não valida estratégia nem autoriza operação.

A média e o comportamento em torno dela serão calculados pelos indicadores
existentes, com suas janelas, parâmetros e versões. Desvios, razões, percentuais,
saldo de sentimento, normalizações, correlações e conclusões pertencem ao
Laboratório/Analytics e à composição existente. Não impor mediana como referência,
criar um índice proprietário de atenção ou acrescentar essas derivações como
novas medidas obrigatórias da coleta. Média, relevância estatística e eventual
utilidade financeira continuam conceitos distintos no resultado das análises.

O proprietário chama o ambiente de cruzamentos de **Strator**. Neste plano esse
nome identifica o fluxo de análise descrito por ele; localizar a superfície
oficial correspondente antes de editar. Não criar um módulo/produto homônimo.

Ficam fora deste incremento: descoberta automática de ativos a partir de temas,
coluna de previsão de alta/baixa, recomendação de compra/venda, indicador de
alcance, reputação de autor, score de estratégia, execução financeira e painel
analítico paralelo. Os adaptadores podem preservar metadados necessários para
qualificar a origem sem transformar todo atributo disponível em indicador.

### 18.2. Contrato das três medidas

As colunas de apresentação serão **Quantidade**, **Positivo**, **Negativo**.
Aliases técnicos propostos: `mentions.quantity`, `mentions.positive`,
`mentions.negative`, sujeitos à validação de colisões/convenções em MEN-01.
Implementar como três características numéricas no catálogo/store existentes;
uma projeção com três colunas não exige três novas colunas em tabelas OHLCV.
Chaves, tempos, fonte, estados e proveniência acompanham o envelope existente.

| Medida | Definição | Unidade |
|---|---|---|
| Quantidade (Q) | Número de publicações elegíveis distintas que mencionam o objeto na janela e no universo declarado | publicação por janela |
| Positivo (P) | Número dessas publicações classificadas positivamente em relação ao objeto | publicação por janela |
| Negativo (N) | Número dessas publicações classificadas negativamente em relação ao objeto | publicação por janela |

Regras obrigatórias:

1. Uma publicação com dez ocorrências do nome conta uma vez por objeto. Um
   documento com dois objetos pode contar uma vez para cada um, com classificação
   própria. Não somar ativos para inferir número de publicações únicas globais.
2. No contrato inicial, P e N são mutuamente exclusivos. Neutro, misto e
   indeterminado permanecem fora de ambos e dentro de Q se a menção for elegível.
   Logo, `0 <= P`, `0 <= N`, `P + N <= Q`; não exigir `P + N = Q`. O restante não
   pode ser chamado automaticamente de neutro. Esses rótulos e cobertura ficam
   em metadados de classificação, sem quarta medida obrigatória.
3. Contagens são inteiras, finitas e não negativas; o transporte numérico atual
   pode exigir representação compatível. Validar limite de representação exata
   antes de converter para float; rejeitar perda silenciosa de precisão.
4. Positivo/negativo descrevem avaliação textual **do objeto**, não variação de
   preço ou expectativa de lucro. Uma frase sobre crise não é automaticamente
   negativa para todos os ativos citados. Não inferir uma recomendação financeira.
5. Política inicial para conectores de documentos: contar publicações originais,
   comentários e respostas como itens distintos; excluir repostagem simples e
   duplicação de transporte. Publicação com comentário próprio/quote pode contar
   como item distinto. Declarar tipos por fonte. Agregados com política diferente
   conservam outro escopo/versão, sem serem normalizados por suposição.
6. Q cobre o universo efetivamente recuperado ou explicitamente contado pela
   fonte. Não é censo da internet. P e N têm que pertencer ao mesmo universo de Q.
   Contagem total do provedor e sentimento de amostra não formam uma tripla
   completa. Publicar Q total com P/N indisponíveis; se houver uma amostra útil,
   publicar outra série, com Q/P/N dessa amostra e identidade inequívoca. Não
   extrapolar proporções para criar contagens positivas/negativas aparentes.
7. Zero exige coleta bem-sucedida e cobertura declarada da janela. Coleta ausente,
   truncada, interrompida, não autorizada ou sem capacidade tem estado/motivo;
   não preencher com zero, interpolar ou copiar a janela anterior. Q pode estar
   disponível enquanto P/N aguardam classificação; isso não autoriza P=N=0.
8. Q/P/N disponíveis como tripla devem compartilhar população e revisão de
   entrada. Publicar lotes consistentes na transação existente; não expor P/N
   novos contra Q de outra população. A disponibilidade de cada medida continua
   explícita quando os estágios de captura e classificação terminam em instantes
   diferentes.

### 18.3. Identidade, universo e filtros

A série precisa distinguir base/namespace, objeto, fonte/provedor e conjunto de
fontes subjacentes, consulta e versão, idioma/região quando conhecidos, política
de inclusão/deduplicação, resolução, janela e versão da classificação. Fixar
representação desse escopo no contrato canônico antes de escrever valores.

`symbol` igual não basta para juntar duas fontes. O store atual tem chave de
valor por símbolo/TF/feature/versão/observação/revisão, sem coluna independente de
fonte nessa chave: MEN-01 deve demonstrar como a identidade completa cabe no
registry e na seleção, sem colisões. Preferir o mecanismo existente de definições
e parâmetros; se houver lacuna, extensão aditiva central e teste de isolamento.
Não trocar o provedor global da base OHLCV para representar a fonte de menções.

Objetos recebem identidade explícita: instrumento, companhia/marca ou tema.
Relacionar, por configuração versionada, aliases e termos de inclusão/exclusão
ao instrumento da base. Exemplos: ouro metal versus medalha; Vale empresa versus
verbo; Ibovespa versus ETF/contrato futuro. Não tratar instrumentos relacionados
como idênticos. Temas como zumbis e K-pop podem ter séries sem vínculo financeiro;
um vínculo futuro com empresa/setor exige definição própria, não inferência oculta.

Filtros por autores públicos, canais ou comunidades são seleções opcionais
quando a fonte os suporta, com identidade de consulta nova. Não supor que
"utilizadores" autoriza coletar informações privadas: consumidores do App usam
o controle de acesso já existente; seleção de autor público é outra dimensão.

Mudança de aliases, fontes, acesso, paginação, idioma ou política de inclusão
precisa de nova identidade/revisão rastreável. Não concatenar essas populações
como se o aumento de cobertura fosse aumento de menções. Agregação multifonte
fica em composição explícita: não somar API direta e fornecedor que já inclui
os mesmos documentos. Se não houver IDs para deduplicação, manter séries separadas.

### 18.4. Tempo, disponibilidade e intervalos

Usar janelas `[início, fim)` em UTC com fuso explícito, preservando a regra de
calendário/sessão usada para projeção no ativo. Guardar separadamente hora de
publicação, primeira observação/captura, fechamento da janela, processamento e
disponibilidade. Campo desconhecido permanece desconhecido.

- A contagem da janela só fica final após fechamento e cobertura suficiente
  declarada. Janela em andamento é parcial e não entra como contagem final.
- `available_at` não antecede os dados necessários nem a produção/recebimento
  real daquele resultado. P/N podem ficar disponíveis depois de Q. Revisão por
  postagem atrasada, edição, exclusão ou reclassificação é anexada; snapshot
  anterior mantém o resultado que conhecia, sujeito aos direitos de retenção.
- Exemplo de teste: publicação às10h, recebimento às10h20; decisão às10h05 não
  pode enxergá-la. Classificação pronta às10h22 não aparece às10h20.
- Backfill recuperado hoje não recebe disponibilidade histórica presumida.
  Histórico point-in-time certificado pelo fornecedor exige evidência e modo de
  pesquisa explícitos; não equivale à latência real da instalação do usuário.
- Modelo treinado/publicado depois do período estudado identifica análise
  retrospectiva. Não certifica reprodução de um classificador disponível então.
- A resolução é a comprovada pela origem. Janelas menores podem compor uma maior
  por soma somente com cobertura, alinhamento e revisões consistentes. Não
  dividir uma contagem diária em M1/H1, nem somar acumulados sobrepostos. Uma
  projeção causal do último dado conhecido preserva resolução/idade e não cria
  novas observações independentes ou amostras estatísticas.
- Sazonalidade, médias e desvios continuam cálculos dos indicadores existentes;
  persistir só quando solicitado pelo fluxo canônico de materialização.

### 18.5. Fontes e ferramentas — referências para implementação

Referências públicas consultadas em29/10/2026. **Consulta documental não prova
acesso contratado, cobertura brasileira, precisão ou funcionamento no host.**
Revalidar termos, quotas, endpoints, retenção e capacidades no início de cada
adaptador. Não fixar preços ou SLAs não contratados. Nenhuma contratação, coleta
ampla ou aquisição de modelo foi feita neste lote documental.

| ID | Fonte / referência oficial | Capacidade a aproveitar | Condição para o contrato Q/P/N |
|---|---|---|---|
| F01 | [X — Post Counts](https://docs.x.com/x-api/posts/counts/introduction) e [documentação da API](https://docs.x.com/x-api) | Contagens recentes/históricas por consulta; documentação descreve minuto/hora/dia; textos por capacidade de busca/stream separada | Counts não retorna textos. Habilitar P/N somente com corpus correspondente ou agregado compatível; respeitar acesso efetivo e política de repostagens |
| F02 | [Bluesky — Jetstream](https://github.com/bluesky-social/jetstream) e [referência HTTP](https://endpoints.bsky.app/) | Fluxo de registros públicos e capacidades de recuperação documentadas | Candidato inicial para coleta própria; filtrar coleções/objetos, guardar cursor, detectar lacunas/reconexão e não presumir histórico integral |
| F03 | [YouTube — commentThreads.list](https://developers.google.com/youtube/data/reference/rest/v3/commentThreads/list) e [comments.list](https://developers.google.com/youtube/v3/docs/comments/list) | Comentários por vídeo/canal, paginação e respostas pelo recurso apropriado | Vídeos/canais selecionados são o painel observado. Conferir respostas completas, comentários desabilitados, quotas, edições e seleção; não chamar o painel de todo o YouTube |
| F04 | [Reddit — Data API Terms](https://redditinc.com/policies/data-api-terms) e [acesso à API](https://support.reddithelp.com/hc/en-us/articles/14945211791892-Developer-Platform-Accessing-Reddit-Data) | Publicações e comentários de comunidades elegíveis | Uso comercial depende de acordo. Registrar acesso pendente sem improvisar scraping substituto; manter restante do trabalho independente |
| F05 | Sites, blogs e fóruns — API/feed/documentação de cada publicador a cadastrar em MEN-02 | Publicações de um painel explícito; RSS/Atom podem ajudar a descoberta | Não há API universal nem lista de sites aprovada. Cadastrar URL, regras, IDs/URLs canônicas, conteúdo efetivamente acessível e paginação; snippets/truncamentos não equivalem a texto integral |
| F06 | [Brandwatch — Data Retrieval](https://developers.brandwatch.com/docs/data-retrieval), [APIs](https://www.brandwatch.com/products/apis/) e [Consumer Research](https://www.brandwatch.com/products/consumer-research/features/) | Menções e agregados, incluindo volume e distribuição de sentimento | Candidato para ampliar fontes mediante contrato. Verificar consultas equivalentes, universo dos agregados, idioma, categorias, backfill e direitos de exportação; índice/sentimento médio não é contagem |
| F07 | [LSEG MarketPsych — Analytics and Models](https://www.lseg.com/en/data-analytics/market-data/quantitative-economic-data-solutions/marketpsych-analytics-and-models) e [MarketPsych](https://www.marketpsych.com/home) | Feeds derivados de notícias, redes e documentos financeiros; entidades/tópicos/sentimento | Avaliar amostra/esquema e licença. Só integrar como Q/P/N se entregar contagens semanticamente equivalentes; scores proprietários ficam em família distinta, sem renomeá-los como contagens |
| F08 | [Google Trends — definição dos dados](https://support.google.com/trends/answer/4365533?hl=pt-BR) | Interesse relativo de busca; complemento da pesquisa | Não preencher Q/P/N: busca não é publicação e escala0–100 não é número de menções. Eventual integração pertence a série externa distinta, sem bloquear MEN |
| F09 | [GDELT — DOC2.0](https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/) | Repercussão jornalística, linhas de volume/tom e referências de artigos | Reusar a frente de notícias. Volume relativo/tom não são contagens P/N. Documentos elegíveis podem ser classificados sob contrato separado de notícias; não substituem cobertura social |
| C01 | [FinBERT — pesquisa](https://arxiv.org/abs/1908.10063) e [implementação ProsusAI](https://github.com/ProsusAI/finBERT) | Referência de classificador financeiro para comparação | Não presumir português/gírias nem equivalência de variantes; fixar modelo/tokenizador/licença/versão e medir em amostra revisada. Modelo é ferramenta, não fonte de menções |

Todas as fontes discutidas permanecem rastreadas, mas implementar conectores
indiscriminadamente não é critério de conclusão. O núcleo deve funcionar com
uma fonte elegível ponta a ponta e receber uma segunda sem alterar consumidores.
F01–F07 são candidatas à ingestão de menções; F08/F09 são complementos de famílias
distintas. Ausência de contrato de um fornecedor opcional não bloqueia o núcleo,
nem permite declarar aquele fornecedor integrado.

### 18.6. Captura, classificação e persistência

**Captura.** Adaptador entrega documentos elegíveis ou contagens agregadas, com
modo, escopo, janela, revisão, cobertura e cursor. Reusar clientes/credenciais
válidos e a infraestrutura oficial de jobs, limites, cancelamento e checkpoints.
Não prender conexão/transação SQL durante requisição remota. Tratar paginação,
429/Retry-After, timeout e reconexão com orçamento finito, retomada e motivo
explícito. Retry de leitura não pode duplicar fatos. Nenhuma consulta do
Laboratório deve disparar rede ou classificador implicitamente.

**Origem.** Manifesto de coleta referencia provedor, consulta saneada, objetos,
período, tipos de publicação, resolução, contagem recebida, cobertura, amostra,
licença/retenção, hashes e cursor saneado. Não expor credenciais ou consultas
privadas em recibos públicos. IDs/hashes de documento e revisão sustentam
idempotência e auditoria, sem criar um catálogo de usuários.

**Classificação.** Separar elegibilidade/objeto de sentimento. Usar texto e
contexto efetivamente disponíveis, com regras para negação, ironia, mistura,
publicidade e múltiplos objetos. Não usar preço futuro para escolher rótulo.
Salvar modelo/provedor, versão do prompt/regras, parâmetros e idioma suportado;
resultado recebido fica imutável por revisão. Não depender de reexecutar um LLM
mutável para reproduzir o resultado passado. Comparar classificador local e
serviço acessível pelo custo/qualidade observados; não impor GPU ou LLM pago.

**Qualificação.** Amostra manual de desenvolvimento separada da avaliação, com
exemplos em português/inglês quando suportados, ambiguidades e casos fora do
domínio. Relatar matriz de confusão, precisão/recall por classe e abstenções.
Fixar critérios de promoção antes da avaliação; não inventar precisão nem usar
lucro de backtest como rótulo correto. Classificador não qualificado pode servir
à bancada sintética; não promover P/N real como capacidade validada.

**Armazenamento.** Reusar `database_manager`, `analytics.datasets`,
`feature_store`, `features`, `producers`, `producer_jobs`, consultas e snapshots.
Uma tabela aditiva de entradas/recibos só cabe se inventário comprovar lacuna;
ela não substitui o feature_store. Três séries canônicas saem do mesmo serviço.
Inspecionar chaves de jobs/checkpoints: duas consultas/fontes do mesmo símbolo
não podem compartilhar cursor por coincidência de família. Estender a chave
canônica se necessário, sem segundo journal ou scheduler.

**Retenção e recuperação.** Dados/textos seguem licença e destino próprio;
persistir somente o permitido, com minimização e política de exclusão/edição
compatível. Se a licença impedir replay de texto, declarar o limite e conservar
agregados/recibos permitidos. Exportação de snapshot não inclui corpus bruto por
conveniência. Manifesto e proteção independente de insumos ignorados seguem o
fluxo existente; `.gitignore` não é backup. Fixtures são próprias/sintéticas ou
amostras públicas com uso permitido; nunca dados privados do proprietário.

### 18.7. Integração e posse

| Responsabilidade | Dono / base canônica a estender |
|---|---|
| Fontes, objetos, escopo, entrada e revisões | Pilar de Dados; `analytics.datasets`, `instruments` no que seu contrato comportar, `database_manager` e ingestão existente |
| Definições Q/P/N, agregação e disponibilidade | Pilar de Dados; `feature_store`, `features`, `producers`, `producer_jobs` e materializador existentes |
| Consulta, snapshots e projeção | Pilar de Dados; `query_service`, `datasets`, `indicator_context`, SDK/CLI oficiais |
| Médias, derivações e relações | Analytics/Strator/Laboratório existentes; `strategies.indicators`, `associations`, `relation_dto` e fluxos F18; não replicar fórmulas no coletor |
| Seleção de séries, gráfico e cruzamentos | Laboratório, após contrato de Dados; `ui/laboratory_series.py`, `ui/relations.py` e componentes existentes |
| Contexto de estratégias/usuários e operações | Contratos BD16/17, ledger/runs e acesso existentes; respeitar bases e revisões de presets |
| Consumo futuro por Expert | Adaptador/contrato operacional próprio, somente após dados/consumidor; sem editar HADOUKEN/Safety nesta frente por conveniência |

Esses módulos foram localizados no checkout de referência; presença não prova
que todo cruzamento esteja implementado ou integrado. Uma análise entre bases,
ativos, utilizadores, estratégias ou indicadores deve declarar as identidades,
população, permissões, janela e corte usados. Dados não reimplementa capacidades
ausentes dos consumidores para aparentar conclusão. Vincular cada lacuna ao
dono BD/F18/LAB existente, com contrato e aceite próprios.

### 18.8. Execução por incrementos

Cada linha abaixo é um incremento revisável, uma tarefa por PR. Subtarefas MEN
permanecem neste capítulo e no painel do plano; não criar backlog concorrente.

| ID | Entrega e passos | Dependências | Aceite verificável |
|---|---|---|---|
| MEN-00 | Registrar decisão, fontes, plano e prompt nos documentos canônicos | Pedido29/10 | Revisão documental e links; não marca implementação concluída |
| MEN-01 | Inventariar fonte/store/consumidores; fechar contrato Q/P/N, identidade de objeto/fonte/consulta e representação nas chaves existentes; desenhar migração somente se necessária | BD03/04/06 e contratos §§11–12 | Dois provedores e duas consultas para o mesmo símbolo não colidem; estados e unidades definidos; mapa de leitores/escritores e baseline de testes |
| MEN-02 | Cadastrar capacidades e acessos de F01–F09; escolher fonte inicial por acesso real; contrato de adaptador de documentos/agregados, orçamento, cursor e manifestos; importar fixture pelo serviço oficial | MEN-01 | Fixture determinística percorre ingestão/consulta sem rede; total versus amostra e fonte ausente são explícitos; cadastro não é conector implementado |
| MEN-03 | Implementar primeiro adaptador real com paginação/reconexão, limites e retomada; início candidato F02 se elegível, ou outro acesso já válido | MEN-02 + acesso real | Pequena captura pública no host, IDs/horários/escopo conferidos; repetição sem duplicação, falha parcial sem zero artificial e retomada pelo mesmo job |
| MEN-04 | Implementar e qualificar classificação por objeto e contagem de P/N, incluindo abstenção e revisão | MEN-01/02; MEN-03 para aceite real | Oráculo sintético exato, avaliação manual separada, versão persistida, falhas não viram sentimento; nenhuma previsão de preço agregada |
| MEN-05 | Materializar Q/P/N por janela e expor pelo catálogo/serviço/SDK/CLI/snapshot existentes; conferir exportação/importação | MEN-03/04 + BD14/18/19 utilizados | Igualdade consulta/SDK/CLI, chaves isoladas, revisão causal por decisão, publicação consistente e snapshot antigo preservado após atualização |
| MEN-06 | Disponibilizar as três séries no Laboratório e exercitar indicadores/médias e relações existentes; documentar cada cruzamento suportado e pendente | MEN-05 + consumidor BD19/F18 correspondente | Seleção pela descoberta do catálogo, sem lista fixa/rede ao abrir; percurso real pelo launcher com série de menções e indicador/preço compatível, estados e população visíveis |
| MEN-07 | Integrar segunda fonte independente pelo mesmo adaptador/contrato; fornecedores adicionais em subtarefas próprias com acesso e orçamento | MEN-05; MEN-06 para aceite visual | Consumidores intactos, fontes separadas e cobertura comprovada; nenhuma soma de corpus sobreposto nem população ampliada silenciosamente |
| MEN-08 | Reconciliar composição, proteção dos insumos, instalação/prévia, regressão e handoff; registrar capacidades não acessíveis | MEN-06/07 e gates pertinentes | Commit/push/PR/SHAs e recibos; migração/retorno preservam dados; quadro do que funciona, do que não foi integrado e dos aceites externos pendentes |

MEN-04 pode ser desenvolvido com fixture enquanto o acesso de MEN-03 está
pendente. Essa independência não transforma teste offline em certificação da
fonte. Em MEN-06, primeiro demonstrar uma relação descritiva já suportada; preço,
indicador, estratégia, ativo, utilizador e TF são percursos distintos. Não
prometer todos por demonstrar um único par. Se falta uma capacidade central,
implementar na sua frente canônica ou registrar dependência concreta.

### 18.9. Bateria de aceite obrigatória

| Caso controlado | Resultado esperado |
|---|---|
| Dez publicações elegíveis:3 positivas,2 negativas,5 neutras/mistas/indeterminadas | Q=10,P=3,N=2; nenhum saldo/score/compra-venda criado pelo coletor |
| Palavra repetida no mesmo item e dois objetos com avaliações opostas | Uma menção por objeto; P/N específicos do objeto; repetição de transporte não altera resultado |
| Mesmo símbolo em duas fontes, consultas, bases ou perfis autorizados | Históricos, checkpoints e resultados isolados; nenhum cruzamento implícito |
| Zero observado, fonte fora do ar, paginação truncada e classificador indisponível | Zero somente no caso comprovado; estados/motivos e P/N ausentes nos demais |
| Counts total1000 e100 documentos amostrados com30 positivos/20 negativos | Q total1000 sem P/N total; amostra separada Q100/P30/N20, sem projeção para300/200 |
| Score0,7 ou porcentagem de sentimento recebida de fornecedor | Recusa como contagem; nenhuma multiplicação implícita pela quantidade |
| Janela fechada versus parcial e evento na fronteira final | `[início,fim)` respeitado; evento na borda entra na próxima; parcial não é final |
| Publicação/captura/classificação em instantes diferentes | Cada decisão enxerga somente o que já estava disponível; fuso desconhecido recusado ou marcado |
| Edição, exclusão permitida por contrato, chegada tardia e reclassificação | Revisões e política de retenção respeitadas; snapshot anterior não lê resultado atual ocultamente |
| Backfill e modelo posterior ao período | Proveniência retrospectiva explícita, sem disponibilidade histórica fabricada |
| Reagregação de duas janelas completas e uma ausente | Soma correta das completas; ausência preservada, sem inventar cobertura/zerar lacuna |
| Rede429/timeout, cancelamento, quota e morte comprovada do processo | Backoff/limites, checkpoint e retomada consistentes; sem job eterno ou publicação parcial |
| Base antiga e segunda aplicação da migração | Dados anteriores preservados, abertura/repetição corretas; SQLite e PG nos caminhos afetados |
| Consulta e abertura de relatório/exportação | Nenhuma rede/classificação/escrita cara inesperada; versões e recibos equivalentes |
| Série constante, pouca amostra e repetição de dado diário em M1 | Estados de relação existentes; repetições não contam como novos eventos independentes |
| Correlação sintética forte e correlação nula | Ambos são resultados legítimos; nenhum limiar financeiro requerido para aceitar a coleta |

Verificação documental: diff, links/âncoras, IDs, coerência dos três campos e
matriz de fontes. Implementação: testes pertinentes a cada incremento, gates
obrigatórios do repositório, doctor pelo wrapper e jornada oficial da superfície
afetada com base de QA separada. Para integração externa, recibo de chamada real
pequena sem segredos; mocks não a substituem. Para UI, inspecionar seleção,
ausência/espera/erro/cancelamento e restauração do contexto no launcher oficial.
Não usar negociação REAL, contas ou bancos privados como fixtures.

### 18.10. Recursos, publicação e encerramento

Manter raiz única `/home/misael/Projects/MT5X`, App em `repos/app`, um `.venv`,
wrapper/caches oficiais, dados de pesquisa/QA separados e pins dos companions
preservados. Não criar clone/worktree, ambiente, scheduler, executor ou lake
paralelo em nome de big data. Antes de crescer, medir documentos/bytes por
janela, custo de classificação, RAM/disco/latência e retenção. Escala é requisito
de capacidade medido; não uma obrigação de instalar uma stack distribuída.

Uma suíte/build pesada por vez; downloads/pacotes/modelos acima70MB são
preparados para execução manual pelo usuário, sem fragmentar para contornar o
limite. Credenciais somente no fluxo oficial existente; acesso faltante produz
uma solicitação objetiva nesse fluxo enquanto a parte independente continua.
Nova assinatura paga/coleta onerosa exige a autorização aplicável; este plano
não é contratação. Não pedir segredos no chat nem instalar serviços recorrentes.

Consultar branch/PRs/SHAs/checks antes do lote e publicação. Preservar WIP alheio,
versionar somente arquivos revisados e confirmar SHA remoto; sem force push,
merge automático ou release por documentação. Atualizar este capítulo, painel
e `docs/continuidade.md` por incremento. Registro administrativo permanece em
`Projects/.workspace`, sem listas concorrentes na raiz da composição.

Concluir o mecanismo significa: primeira fonte real, classificação qualificada,
Q/P/N persistidos/consultáveis/congeláveis, consumo por indicadores/relações
existentes e segunda fonte integrada sem reescrever consumidores, com provas
no host. Acessos opcionais e cruzamentos ainda ausentes ficam discriminados.
Nenhum resultado de correlação, preço futuro ou estratégia vencedora é condição
de sucesso da captura. Não certificar utilidade financeira ou operação de Expert
a partir do aceite técnico deste mecanismo.


### 18.11. Inventário e contrato executável MEN-01

Inventário do App após PR235/42c044c e plano MEN-00:

| Papel | Implementação localizada / decisão |
|---|---|
| Entrada/namespace/base | `database_manager`, `analytics.datasets.identity/declare_source/fork`; manter provedor de preços, não trocar pela rede social |
| Objetos financeiros | `analytics.instruments` aceita fatos de especificação/série/ajuste, não é cadastro de documentos ou temas; não inventar instrumentos para K-pop/zumbis |
| Definições/valores | `features.register`, `feature_store.register_definition/save_values/query_history/query_as_of`; chaves já isolam versões, parâmetros já são imutáveis |
| Jobs/checkpoints | `producer_jobs`: chave `(symbol,tf,family)`; `mentions:<hash completo do escopo>` já oferece isolamento, sem migração/segundo journal |
| Consulta/captura/transporte | `query_service`, `datasets`, `indicator_context`, SDK/CLI existentes; catálogo distingue todas as versões |
| Consumo visual/associação | `ui/laboratory_series.py`, `ui/relations.py`, `analytics.associations/relation_dto`; são superfícies existentes para a função descrita como Strator, sem módulo homônimo localizado |
| Adaptador externo existente | `external_data_source.ExternalDataSource.get_historical_m1_data` tem contrato OHLCV/M1; não recebe documentos ou agregados MEN. Reusar seu papel de fallback de preços, sem deformá-lo como coletor de menções |
| Notícias/sentimento encontrados | Calendário econômico em `trade_analytics.enrich` qualifica operações; `app.py.simulate_trader_sentiment` declara simulação por preço/volume, sem chamador localizado. Nenhum é um corpus de menções ou classificador de texto |
| GDELT | Busca no código atual não localizou cliente/ingestor GDELT; F09 é referência documental, não integração comprovada. Não atribuir existência de coletor ao rótulo notícias |
| Entradas/classificações/recibos de documentos | Não há esquema/caminho canônico para esses registros; eventual tabela aditiva só após contrato de adaptador MEN-02. MEN-01 não cria nenhuma |

Contrato1 implementado em `analytics.mentions`: três IDs estáveis, tipoNUMBER e
unidadepublication/window. A versão inclui hash integral de namespace, objeto,
vínculo, fonte/corpus/acesso, consulta/filtros/idioma, resolução, política,
universo e classificador. Duas fontes e duas consultas do mesmo símbolo recebem
quatro versões/famílias de checkpoint distintas. Fonte da base não muda.
Definições guardam fingerprints e identidade, sem expressão/autores/aliases
privados; textos não integram valores/snapshots por esse caminho.

Estados: zero exige cobertura completa; cobertura parcial/ausente tem três
valoresUNAVAILABLE e motivo. Classificação pendente não vira P=N=0. Q e P/N têm
relógios próprios: completar P/N não reescreve a metadata histórica de Q. Limite
inteiro seguro2^53−1; float/score/porcentagem não são contagens. Janelas UTC
fechadas `[início,fim)`, resolução fixa atéD1 e precisão de microssegundos.
[Assinatura e contrato detalhado](../sdk/api.md#identidade-e-valores-de-menções-men-01).

Baseline antes do código:34 testes de features/BD03/BD06 aprovados no host.
A prova usa registros determinísticos próprios no store/journal existentes;
fonte real e classificador qualificado continuam MEN-03/04, não certificados
por fixtures. Não há migração necessária das chaves; versões/colunas anteriores
permanecem. MEN-02 deve provar elegibilidade/deduplicação/corpus/manifestos e
qualificar a lacuna de recibos antes de publicar capturas de documentos.


### 18.12. Adaptador de lote e ingestão canônica MEN-02

`mention_ingestion` recebe lote limitado de documentos ou agregado; não abre
rede. `source_capabilities` rastreia F01–F09 como DOCUMENTED_ONLY, sem afirmar
conectores integrados. A busca pública Bluesky devolveu403 no host; a API oficial
Hacker News/newstories devolveu200/JSON. Recibos de sondas são privados, sem
corpus retido. F05/Hacker News é candidato por acesso demonstrado, não conector
qualificado por essa sonda. [API oficial](https://github.com/HackerNews/API).

Não foi necessária tabela de entradas: texto é efêmero e as contagens/recibos
mínimos cabem em `feature_store.metadata_json`; hashes de publicações/revisões
sustentam população/deduplicação, sem catálogo de autores ou cópia de corpus.
Cursor/estado/run ficam em `producer_jobs`, no escopo MEN-01. O guard do store
confere namespace e recursos; três valores e checkpoint confirmam juntos.
Tentativa incompleta tem três ausências/motivo e preserva o cursor completo.
Falha/cancelamento não confirma prefixo parcial de uma janela.

Filtros de objeto/consulta são literais NFKC/casefold, com aliases, inclusões,
exclusões e metadados declarados. Não inventa resolução de entidades geral.
A fixture separa ouro/medalha, Vale/verbo, índice/ETF/futuro e avaliações opostas
por objeto. Palavra repetida e duplicação de transporte não multiplicam Q.
Limites1000 documentos/64KiB por texto/4MiB por entrada. Revisões conflitantes no
mesmo lote são recusadas; exclusão/edição recebida gera outra população.

Anotações por objeto só têm aceite de fixture (`qa.`), nunca de classificador
real. Total1000 e amostra100/P30/N20 usam escopos diferentes; P/N do total ficam
indisponíveis. Nenhum percentual/score é multiplicado por Q. Q mantém seu
relógio/metadata ao chegar classificação posterior; repetir mantém primeiro
recebimento. As consultas e o catálogo não chamam rede nem classificador.
[SDK/CLI e formato do lote](../sdk/api.md#ingestão-limitada-de-menções-men-02).

Fonte real/reconexão/limites de rede/retomada são MEN-03; qualificação textual é
MEN-04; snapshots/consumo visual completo e segunda fonte continuam MEN-05…08.
Não concluir captura real pela ingestão determinística de documentos próprios.


### 18.13. Primeira captura pública MEN-03 — painel Hacker News

F05/Hacker News usa API oficial fixa, universo `sample` e painel deN IDs únicos
(1…64, padrão24), discriminado no escopo/manifesto. Novos membros do painel
podem mudar contagens: não é censo do HN/internet nem prova de crescimento de
menções. Texto lido é título + conteúdo local, sem buscar artigo apontado por
URL. Não há fonte universal de sites implementada.

`mention_sources` reusa `producer_jobs`, guardas e ingestão MEN-02. Roster é
fixado no run por `attach_run_inputs`, uma única vez enquanto ativo, e em
checkpoint pendente com proprietário/hash; input finalizado não é reescrito.
Rede fica fora de SQL. Requisições têm timeout/512KiB/três tentativas/backoff e
Retry-After finitos; erro/item desconhecido não vira zero. Retomada conserva o
roster pendente, e painel novo é ação explícita. Segundo coletor não rouba run
ativo. Tombstone conserva horário desconhecido e exclusão, sem timestamp falso.
[API, limites e CLI](../sdk/api.md#captura-pública-limitada-de-menções-men-03).

Validação real do host:um run foi morto após fixar o roster público, antes de
ler/contar itens; nenhuma contagem confirmada. Nova execução pelo launcher
reconciliou INTERRUPTED e recuperou os mesmos24 IDs;24 GETs completos,
Q=1 conferido por tokenizador Unicode independente. A janela da demonstração
foi escolhida retrospectivamente após sonda de um tema fixo AI; não é estudo
pré-registrado. Outra coleta refletiu mudança de membros do painel como revisão,
com corte anterior preservado e disponibilidade neste sistema, sem backdating.
SIGINT após item real recebido deixou zero fatos e o mesmo roster para retomar.
Fonte de preços e base principal foram preservadas; somente QA própria foi usada.

90 testes pertinentes aprovados/3 PG pendentes no host. Mocks exercitam falhas,
cancelamento, concorrência de proprietário, namespace e contrato; fonte real é
comprovada pelo recibo separado, não pela fixture de rede. Texto não persiste;
manifestos/hashes/cursor ficam nas tabelas existentes, sem novo journal/store.
P/N reais continuamUNAVAILABLE/PENDING:MEN-04 qualificará o classificador.
Bluesky/searchPosts403 continua acesso pendente; fornecedores opcionais não são
chamados de integrados. Consumidores/segunda fonte/instalação final seguem
MEN-04…08. [API oficial HN](https://github.com/HackerNews/API).


### 18.14 Classificação MEN-04 — domínio controlado em inglês

O serviço `mention_classification` anexa P/N somente a Q completo da mesma
população já recebida. `mention_classifier` usa VADER3.3.2/MIT e
langdetect1.0.9/Apache-2.0 locais, sem GPU/rede/serviço pago. Código, léxicos e
perfis têm fingerprint; sua identidade é declarada no contrato antes da captura.
Não é FinBERT, não qualifica português nem todo o corpus Hacker News. Apenas
avaliações explícitas do objeto em inglês têm domínio certificado pelo benchmark
controlado; sintaxe/idioma/contexto ambíguos abstêm. Não confundir polaridade com
preço. Corpus todo fora do idioma deixa P/N indisponíveis; abstenções conhecidas
no domínio ficam explicitadas em `label_counts`, jamais convertidas em neutro.

Protocolo fixado antes do teste:44 casos autorais separados dos exemplos de
desenvolvimento;≥8 exemplos de cada polaridade, precisão P/N≥90%, recall≥70%,
zero polaridades nos12 casos de abstenção esperada. Fingerprint do protocolo:
`d1262870f76392b2c6a84bf0bf60bea87b6818e5b43e309f75a6d35ded8d512d`.
Anotação semântica por Codex, sem revisão humana independente ou inferência de
representatividade. Não houve ajuste após o teste nem uso de resultado financeiro.
Matriz e rótulos esperados autorais reproduzíveis em
`tests/test_mention_classifier_evaluation.py`; recibo runtime em
`mention_qualification.REPORT`. Precisão/recall: P90,9%/100%; N100%/90%;
neutro100%/100%; misto75%/75%; abstenção100%/100%,12/44 abstenções.
Os dois erros são preservados: um negativo como misto e um misto como positivo.
Esse limite permanece visível nos serviços/recibos, sem selo de precisão geral.

As saídas persistidas por item têm hash da publicação/revisão, objeto, idioma,
rótulo e motivo; nenhum texto bruto. Consulta/replay usa os valores versionados
sem executar um modelo atual. Q conserva primeiro recebimento e metadados sem
rótulos futuros; P/N têm disponibilidade da classificação efetiva. Recaptura
idêntica não rebaixa P/N para pendente. Edição muda população e invalida P/N;
modificação concorrente de geração durante CPU rejeita publicação antiga.
Classificação usa família distinta no mesmo journal, sem apagar o painel/cursor
de captura. Falha/cancelamento preserva Q e os rótulos anteriores confirmados.

[SDK/CLI e limites](../sdk/api.md#classificação-explícita-de-menções-por-objeto-men-04).
Acesso/conversa geral/idiomas adicionais exigem nova avaliação; MEN-05…08 mantêm
aceites próprios de catálogo/snapshot, Laboratório, segunda fonte e proteção.


### 18.15 Catálogo/consulta e snapshot MEN-05

As três medidas são definições NUMBER com unidade `publication/window`, fonte,
consulta/versionamento e população próprios no catálogo/store canônicos. Leitura
com corte e contexto não coleta/classifica, e valores ausentes não viram zero.
Captura/reclassificação seguem MEN-02…04, não uma materialização por preço.

A lacuna comprovada foi `datasets.capture` exigir OHLCV mesmo para temas. O
mesmo serviço/store ganha modo opt-in `feature_only=True`, formato5: somente
contratos/histórico de características, janela `[início,fim)` UTC explícita e
fechada, sem OHLCV, calendário ou especificação inventados. Não depende do fuso
do corretor. Formatos1…4 e padrão com barras preservados. Mesmos blocos,
hashes, transação, deduplicação, limites de seleção, guardas e transporte;
nenhuma tabela/store/serializador paralelo. Cancelamento antes do commit
reverte blocos/manifesto, importação não publica fatos nem muda namespace do
receptor. O snapshot mantém origem histórica própria, inclusive fontes de
menções por definição/recibo, sem trocar provedor global de preços.

SDK `capture_feature_snapshot`, `export_feature_snapshot`,
`import_feature_snapshot` e inspeção/contexto existentes; CLI
`snapshot-capture/export/import`. Sem barras, consumo que exige mercado é
recusado ou fica sem grade; não se simula cruzamento de um tema com preço.
Para explorar recibo tardio, contexto com observação fixa e corte até o último
recibo congelado; snapshot anterior não completa P/N com a base/modelo atual.
Lotes/snapshots de QA demonstram10/3/2 e alterações posteriores; transporte
real de agregados públicos conserva Q8/P0/N0/8 abstenções e cinco revisões
(Q e pendências + classificação posterior), sem texto bruto. Erros de orçamento
na CLI agora retornam JSON/ação de correção, sem traceback. Persistem os limites
de qualificação de MEN-04 e cruzamentos BD16/17/F18/LAB; MEN-06…08 prosseguem.


### 18.16 Consumo MEN-06 no Laboratório existente

Laboratório → Séries e confluências → **Séries por janela…** é o consumidor de
snapshots/fatos por observação, inclusive temas sem OHLCV. Não existe novo
produto Strator. A tela usa `stored_series_sources/query_stored_observations`
do SDK; disponibilidade e revisão vêm do `feature_store.asof_join` por janela
exata e a média vem de `strategies.indicators.sma`, com período/hash/proveniência.
Não se criou fórmula no Tk, indicador primário de atenção, store ou executor.
Sem reamostragem, zero inventado ou retenção entre janelas; lacuna interrompe
média. O eixo de publicação é retrospectivo no corte, não conhecimento histórico
ou sinal operacional. Valores primários, unidade, fonte, consulta/versão,
estado, população e disponibilidade são inspecionáveis. Q/P/N de populações
diferentes não são reunidos nos cartões; tabela/gráfico preservam cada variante.

Números com separadores e cartões grandes; abstenções e domínio inglês restrito
aparecem junto a P/N. Ausência permanece travessão. Espera tem barra indeterminada
local e usa `run_bg` atual; leitura/cálculo canceláveis e erro preservam resultado
anterior. Exportação/importação aguardam conclusão atômica sem promessa falsa
de cancelamento. Abrir snapshot não publica fatos no receptor. Interface limita
seis séries por gráfico para leitura; fonte grande começa nas últimas20mil
janelas nominais explicitamente, com intervalo ajustável. Detalhe técnico é
expansível, gráfico usa a ampliação/navegação existentes.

Criar relação abre o formulário canônico `RelationCreator`, sem segundo motor.
Sem grade de mercado, formato5 não recebe barras fictícias; o resultado pode
ser insuficiente. Snapshot com mercado e contratos numéricos usa os mesmos
cálculos já suportados. QA de contagens sintéticas demonstrou relação+1 e nula0
com indicador armazenado:160 pares/20 sessões em testes; no launcher, relação+1
salva com88 pares/11 sessões na janela escolhida. Não qualifica conta, fonte
nativa, utilidade financeira nem cruzamentos além de BD16/17/F18/LAB atuais.

No host, fonte pública congelada Q8/P0/N0 com8 abstenções foi reaberta no
Laboratório sem nenhuma chamada de rede/modelo. Outro recorte QA, claramente
sintético, mostrou1.000.001/250.000/50.000, média, lacunas, ampliação, tela menor,
espera, cancelamento/erro e recuperação. Os2.540 fatos de entrada e namespace
foram preservados; somente snapshots/artefato de relação foram acrescentados.
MEN-07/08 mantêm aceite de segunda fonte, regressão, proteção e reconciliação.


### 18.17 MEN-07 — segunda fonte, Stack Overflow via Stack Exchange API

F05 tem dois publicadores independentes: HN e perguntas públicas do Stack
Overflow (`stackexchange.stackoverflow.questions.panel.v1`). Não são o mesmo
corpus revendendo contagens. O mesmo coletor/journal/store/SDK/CLI recebe painel
explícito de1…64 perguntas criadas na janela; primeiroN em criação descendente,
corpo/título locais completos, lotes de até8 IDs. Sem respostas/comentários,
sem censo e sem converter votos/score em P/N. Retomada usa IDs congelados;
ID omitido/texto incompleto licencia insuficiente falha sem contagem menor/zero.
Filtro regional sem prova era ignorado no preparador; agora é recusado, assim
como idiomas/canais/comunidades não certificados nos dois adaptadores.

Quota/backoff/Retry-After e cooldown da mesma URL≥60s ficam em checkpoints do
journal atual, separados da família dos fatos, com serialização SQLite/PG.
Espera fora de SQL, orçamento/cancelamento existentes; quota zero bloqueia até
próxima virada UTC por política local conservadora, sem prometer reset da quota
do servidor nesse horário; erro HTTP também conserva recibos de quota. Sem cache de
respostas, novo scheduler/store/executor ou assinatura. Controle por base;
quota real continua compartilhada pelo IP, sem governar outros aplicativos.

Licença observada pela API e inferida pela política pública são distinguídas.
Filtro pré-calculado pede `content_license` e corpo. Se o campo falta e a criação
é≥02/05/2018, a política oficial autoriza atribuir CC BY-SA4.0 a essa contribuição;
recibo registra inferência, quantidade, referência/data/versão. Não presume
licença de revisões antigas nem substitui campo conflitante. Sem texto bruto
ou nomes de autores em store/snapshots/Git; IDs públicos só no journal privado
para retomada. Fonte/consulta/aliases/licença/versões permanecem explícitos.

No host, pequena captura real de16 perguntas após SIGKILL do roster foi retomada
pelos mesmos IDs; Q1 para o filtro AI, P0/N0 e uma abstenção. Oráculo independente
confirmou Q; os5 fatos/revisões HN anteriores, namespace e provedor de preços
foram preservados no mesmo símbolo. Repetição cumpriu intervalo real da API e
preservou corte anterior. SIGINT durante cooldown confirmado preservou10 fatos
confirmados e marcou CANCELLED. Catálogo/snapshot e leitor do Laboratório
aceitaram a segunda fonte sem mudança de consumidor. Qualificador ainda é
somente benchmark inglês controlado, não precisão geral da conversa na fonte.
[SDK e fontes primárias](../sdk/api.md#segundo-publicador-independente-men-07).
MEN-08 prossegue com proteção/regressão/reconciliação final.


### 18.18 Aceite integrado, proteção e continuidade MEN-08

MEN-00…07 estão preservados em PRs236…243, cada frente com contrato, testes e
recibo próprio. MEN-08 fecha proteção, regressão e continuidade na branch
`codex/mencoes-aceite`. São incrementos em cadeia aberta, sem merge em master.
Código MEN-07: `72850fb7a5810932c89e87bbb691e3f2ff6dcb40`,286 arquivos de runtime
verificados; HADOUKEN/Safety permanecem detached nos pins do manifesto.

| Capacidade | Aceite observado e limite |
|---|---|
| Q/P/N e identidade | Três NUMBER inteiros por publicação distinta/objeto/fonte/consulta/janela; zero, ausência, total/amostra e versões separados; sem índice obrigatório/direção de preço |
| Captura | HN painel24/64 e Stack Overflow painel16 por APIs oficiais, oráculo independente de Q, SIGKILL/roster retomado e SIGINT/rollback. Painéis limitados, sem censo |
| Classificação | Local, versão/hash/idioma fixados; disponibilidade posterior a Q, histórico preservado.44 casos próprios separados do desenvolvimento; P precisão90,9%/recall100%, N100%/90%;2 erros e12 abstenções. Qualificação **somente inglês controlado**; sem amostra humana independente ou representativa das fontes |
| Fonte real e sentimento | HN Q8/P0/N0/8 abstenções; SO Q1/P0/N0/1 abstenção. São saídas reais persistidas; nenhum suporte real P/N nessas amostras para estimar precisão por fonte |
| Store e snapshots | Store/journal canônicos, SQLite e PG, namespace/provedor de preço intactos; formato5 sem preço opt-in, histórico e transporte idempotentes; antigos1–4 preservados |
| Laboratório | Séries e confluências → Séries por janela…: seleção por catálogo, valores/gráfico/tabela, ausências/abstenções, SMA existente, leitura offline e espera/cancelamento/erro. Relação forte+1 e nula0 com indicador por engine atual; prova visual de relação+1 com88 pares/11 sessões sintéticas |
| Produtos/consumidores | Mesmos SDK/CLI/Analytics/Laboratório; segunda fonte não exigiu reescrever consumidor nem criar módulo Strator. Coleta é ação explícita; ler/importar/exportar não chama rede/modelo |
| Proteção | Complemento privado MEN na página de recuperação existente:9 partes, ZIP3.621.472bytes, SHA-256 `232ff78e9c97cebdfb82d5ba9bb6261c16576d42c6d3f8b5bda91e2013a5ba20`. Relido pela internet e restaurado:160 arquivos/30 bases QA com quick_check; sem texto bruto, credenciais ou base do proprietário |

O manifesto privado registra origem, tamanhos/hashes de cada parte, inventário
interno, nomes de download/referências e retenção. Usa o mesmo
`scripts/recover_cloud_backup.py`: conferir todas as partes antes de recompor,
extrair somente em pasta nova e conferir os hashes internos. Preservar enquanto
os recibos forem referência, até haver sucessor independente conferido. Bases
sintéticas e capturas públicas agregadas permanecem em QA; não sobrescrever base
principal nem copiar configuração/autenticação/locks. `.gitignore` não é backup.
O pacote principal de recuperação do proprietário não foi alterado.

Uso pelo SDK/CLI: declarar seleção explícita, `mentions-describe`,
`mentions-collect --classify` (ação com rede), `series`, `snapshot-capture`,
`snapshot-export`/`snapshot-import`, `stored-sources`/`stored-observations`.
[Contrato e exemplos](../sdk/api.md#segundo-publicador-independente-men-07).
`mention-classifiers` expõe o escopo restrito; leitura congelada é passiva.
Painel/filtros/alias/objeto e janela UTC são decisões explícitas do usuário.

Pendências discriminadas, sem bloquear o núcleo delimitado:

- **F01 X, F03 YouTube, F04 Reddit, F06 Brandwatch e F07 MarketPsych:** sem
  credencial/contrato/corpus/licença/quotas demonstrados neste incremento;
  não integrados. Nenhuma assinatura/coleta onerosa autorizada.
- **F02 Bluesky:** pesquisa HTTP pública recusou acesso403 neste host; Jetstream
  exige adaptação/qualificação própria do protocolo e continuidade. Não integrado,
  sem contornar a recusa por scraping ou presumir histórico integral.
- **F05:** somente HN e perguntas SO certificados nos painéis descritos; demais
  sites/feeds, respostas/comentários SO e censo/histórico amplo não integrados.
- **F08 Trends:** buscas relativas0–100 não são Q/P/N. **F09 GDELT/notícias:**
  frente existente, sem mapear tom/volume relativo para essas contagens nem
  duplicar corpus. Não certificados como novos adaptadores MEN.
- **Classificador:** português/gírias, avaliação humana independente, amostra
  representativa com P/N e qualificação geral de cada fonte continuam pendentes.
  Não apresentar o benchmark controlado como validação geral de mercado.
- **BD16/17/F18/LAB:** cruzamentos genéricos com preço sem contexto compatível,
  múltiplos ativos/TFs, operações/utilizadores e demais relatórios conservam
  contratos/gates dos donos; a janela de fatos sem OHLCV não inventa mercado.
  **Expert/SA:** consumo operacional/estratégia/risco e Tester/DEMO próprios.
- **Nativo/financeiro:** host Linux com launcher oficial e bases de QA; doctor
  offline não certifica MT5/Wine, Windows físico, conta, latência operacional,
  estratégia ou utilidade financeira. Sem negociação REAL.


Regressão integrada no host: **221 aprovados/18 casos PostgreSQL não executados**,
347,57s, cobrindo contrato/lotes/fontes/classificação/snapshots/Laboratório,
formatos antigos, janelas de referência e relações persistidas. São testes
pertinentes, sem alegação de suíte completa. PG do código MEN-07: **96 aprovados**,
110,85s, incluindo segunda fonte/retomada e rollback de quota; job123843555241.
Doctor offline **16 OK/2 avisos/0 falhas**. Recibos próprios, comparações da
prévia e restore ficam no destino de QA existente e no registro administrativo
central; artefatos privados/referências de acesso não entram no Git público.


O gate PG da primeira ponta documental MEN-08 expôs uma corrida preexistente no
cadastro comum de definições (95 aprovados/1 falha em materialização composta):
duas chamadas liam chave ausente antes de INSERT. MEN-08 inclui correção no
`feature_store.register_definition`: advisory lock transacional PostgreSQL por
ID/versão antes da leitura; repetir definição idêntica conserva o primeiro
registro, e metadados divergentes continuam recusados sem sobrescrever. Não muda
schema, valores, modelo nem versões de preço. Dois casos concorrentes reproduziram
UniqueViolation antes da correção; ambos e os3 casos de materialização PG passaram
após a correção. Os recibos finais discriminam a regressão anterior e os gates
executados depois deste ajuste; não apresentar o95/1 como aceite aprovado.


Após a correção de cadastro, a bateria completa PostgreSQL do workflow também
foi executada no host, em contêiner próprio com PostgreSQL16.15 já disponível:
**98 aprovados em359,48s**, incluindo os dois novos casos e todos os caminhos MEN.
Sem download de imagem, negociação ou acesso a banco do proprietário. O servidor
de QA é temporário; schemas próprios são removidos pelo helper de testes.

Regressão após a correção: **196 aprovados/18 PG não executados no comando
SQLite**,213,95s, cobrindo MEN, definições e materialização composta; casos PG
foram executados separadamente no gate98/0 acima. Rodadas sobrepostas, não somar
como casos distintos. Doctor repetido após o ajuste:16 OK/2 avisos/0 falhas.


### 18.19 Cruzamentos no candle e relatório — BD16-LAB-LINKS

Pedido posterior ao MEN-08: integrar menções, nível de atividade, indicadores,
preço histórico, TFs, cronologia e compartilhamento no Laboratório existente.
Vínculo explícito aponta snapshot/objeto/TF/versões para o ativo estudado e registra
uma descrição da associação; o nome textual não prova equivalência econômica.
Sem duplicar store, motor de correlação ou executor, nem consultar rede ao ler.

Duas perspectivas distintas: **recebimento** coloca cada mudança disponível no
primeiro fechamento observado que a conhece, sem repetir contagem nos candles
seguintes; **publicação retrospectiva** usa o fim da janela descrita e somente
revisões conhecidas no corte, mostrando o recibo posterior. A segunda não é input
historicamente disponível nem evidência preditiva. Nenhuma divide D1 em M1 ou
preenche lacuna com zero. Colisões de recibos e revisões ficam no detalhe; não
somar observações de populações distintas para caber no desenho.

Todos os pares da seleção elegível, inclusive TFs diferentes, usam as matrizes
numéricas e controles existentes, com limites de canais/células, ausência,
constância e correlação nula explícitas. Volume real/ticks e Q/P/N mantêm suas
unidades. Defasagens/horizontes não saltam buracos da grade nominal. Relatório,
UI/SDK/CLI e exportação compartilham os mesmos dados congelados; o candle expõe
janela, captura, disponibilidade, revisão, fonte e limitações.

Sessões de negociação só usam calendário congelado disponível; fases de ciclo
macroeconômico exigem fonte/observações explícitas, sem inferir recessão/expansão
por preço ou volume. Categorias ficam como informação temporal, sem converter
rótulos em correlações numéricas. Correlação não determina motivo do preço.
Aceite inclui dados fortes/nulos, chegada/classificação tardias, multiobjeto/TF,
semântica de zero/lacuna, revisão/corte/export/reabertura, cancelamento/limites e
percurso pelo launcher oficial em QA separado. A etapa não qualifica Expert,
negociação, causalidade, precisão do classificador ou mercado nativo.

Implementação validada em30/10/2026 na branch `codex/cruzamentos-mencoes`,
sobre MEN-08/PR244. UI: **Séries e confluências → Fontes, candle e relatório…**;
fontes escolhidas são congeladas pela ação explícita **Congelar e analisar**.
SDK/CLI de análise exige `manifest_id` existente, sem captura implícita.
[Uso e contratos](../sdk/laboratorio_series.md#fontes-vinculadas-candle-e-relatório-portátil).

Regressão pertinente:102 aprovados/1 não executado por condição de ambiente;
após o ajuste de leitura,2 testes direcionados aprovados; após a rolagem da
matriz,2 percursos Tk aprovados. Rodadas se sobrepõem. PostgreSQL16.15
isolado:3 novos casos aprovados; baseline MEN-08 tem98 casos no host eCI
próprios. Doctor de desenvolvimento offline:16 OK/2 avisos/0 falhas.
Launcher oficial com base/preferências próprias:78 candles,11 canais M1/M3,
24 janelas, correlação forte+1 e nula0, categoria datada, recibos Q/P/N
distintos, espera/cancelamento preservando resultado, vínculo de outro ativo
recusado e HTML local; fatos/namespace preservados, zero chamadas de rede/modelo.
CLI oficial confirmou candle/cruzamentos/exportação e hash da base intacto.
HTML percorrido no navegador em820×680, sem overflow da página, com candle,
lag0/1/3/6 e estabilidade de quatro blocos. Ensaios são sintéticos declarados;
não há qualificação financeira, Windows físico ou disponibilidade nativa.

Versão2 congela matrizes, alvos brutos e estabilidade no momento da análise;
reabrir/ler/exportar verifica integridade e lê o resultado, sem recalcular
estatísticas/fórmulas. Versão1 permanece compatível. Limites32 canais/8 TFs,
20 mil barras/200 mil células e revisões,120s/1024MiB; fontes W1/MN1 e
M7/M9 sintéticos não entram neste vínculo. Captura cancelada preserva os
snapshots já confirmados. Categorias permanecem fora de matrizes numéricas.
A cadeia segue emPRs abertas; não representa merge, release ou aceite dos
demais itens BD16/17/18/LAB/Expert.


### 18.20 Painéis sociais explícitos — MEN-SOCIAL

Reddit (originais de uma comunidade), X (busca recente, sem repostagens simples)
e YouTube (comentários de primeiro nível de um vídeo) usam o mesmo coletor,
store e journal MEN. Transporte: Composio CLI local com conta autorizada,
somente GET em endpoints fixos; não se armazena token nem resposta bruta.
Cada painel congela IDs antes de hidratar. Retomada consulta os mesmos IDs;
ID ausente/erro de autenticação/JSON parcial não publica Q0. Corpo/notas longas
X são consumidos quando presentes, sem confundir snippets/vídeos/likes com Q.
Respostas YouTube e comentários Reddit continuam outro painel, não presumidos.

Contrato: provider `reddit`/`twitter`/`youtube`; corpora respectivamente
`reddit.community.originals.panel.v1`, `x.recent-search.posts.panel.v1`,
`youtube.video.top-level-comments.panel.v1`; documents/sample e perfil
`composio-read-approved`. `panel_size`1…64 e `account` opcional são parâmetros
públicos; YouTube exige `video_id` de11 caracteres, Reddit exatamente uma
`query.communities`. Região/idioma/canal sem prova são recusados. O filtro
literal do objeto permanece no ingest canônico. Busca recente X recusa janela
fora de7 dias; Reddit/YouTube observam painel atual, não histórico/censo.

Checkpoint HTTP compartilhado e separado por provedor conserva prazo de acesso;
ausência de headers/quota na CLI continua desconhecida, sem inventar Retry-After.
Erro não é repetido automaticamente; atraso mínimo registrado para retomada.
Isso não certifica quotas/continuidade de cada plano contratado. Reddit requer
aprovação para o uso permitido, X depende do acesso efetivo e YouTube da conta
e quota; a descoberta local não sonda nem afirma acesso. Nenhuma compra/coleta
onerosa ou aprovação comercial foi feita.

Validação:77 testes de regressão no host,4 PostgreSQL isolados. Cobrem captura
com transporte próprio declarado, perda de documento, roster retomado, origem
de preço preservada, ausência de texto nos fatos, separação de cooldowns,
cancelamento e encerramento do subprocesso próprio. As três conexões reais
estavam ausentes e foram solicitadas no fluxo oficial; validação de captura
real permanece pendente de autorização/acesso. Não apresentar os testes como
precisão do classificador ou certificação das fontes.

Referências conferidas30/10/2026: [Reddit API](https://www.reddit.com/dev/api/)
e [acesso/termos](https://support.reddithelp.com/hc/en-us/articles/14945211791892-Developer-Platform-Accessing-Reddit-Data);
[X recent search](https://docs.x.com/x-api/posts/search-recent-posts);
[YouTube commentThreads](https://developers.google.com/youtube/data/reference/rest/v3/commentThreads/list).


### 18.21 Português e avaliação reproduzível — MEN-PT

`object-evaluation-pt` é um classificador próprio, opt-in e versionado de
avaliação explícita de objeto em português. Usa aliases obrigatórios, detector
já instalado e regras/léxico próprios para elogio/crítica e gírias declaradas
(top/show/brabo/curto/lixo/bugado). Preço/direção, ironia, pergunta, autoria
indireta, negação ambígua e sujeito não resolvido abstêm. Não traduz VADER nem
altera o ID/artefato inglês. Os resultados antigos permanecem na própria versão.

36 exemplos próprios de desenvolvimento e30 de avaliação separados. Na
avaliação: P e N com8 casos cada, precisão100% e recall87,5%; um falso neutro
positivo e um negativo preservados. Critério controlado: suportePN≥8,
precisão≥85%, recall≥75%. Este recibo não é revisão humana independente,
amostra representativa de fonte, validação de português amplo ou utilidade
financeira. Nunca usar a métrica pequena como precisão geral nas redes.

`mention-classifiers --classifier object-evaluation-pt` inspeciona o artefato
e seu recibo congelado. A seleção MEN usa exatamente o descriptor retornado.
`mentions-classify`/`mentions-collect --classify` selecionam essa versão pelo
contrato existente; Q continua com relógio próprio, P/N chegam depois. A
interface mostra “português restrito”, sem executar modelo na consulta.

`mention-evaluate --input cases.json --classifier object-evaluation-pt --out
metrics.json` recebe `cases` e `protocol`. Casos têm id/text/aliases/language/
expected; protocolo declara annotation_origin e development_ids. IDs de teste
duplicados, documento/objeto repetido e sobreposição com desenvolvimento são
recusados. Saída: métricas/confusão/erros com índices/hashes, identidades do
modelo/protocolo/avaliação, sem texto. Declarar revisão independente mantém
`DECLARED_NOT_VERIFIED`: o programa não inventa quem revisou ou representatividade.
Revisão humana e amostra real elegível continuam necessárias para outro domínio.

API: `sdk.mention_classifier_capabilities(classifier_id)` e
`sdk.evaluate_mention_classifier(cases,classifier_id=...,protocol=...)`.
A avaliação não faz rede/grava corpus e recusa sobrescrever recibo CLI.
Regressão28 casos aprovados no host e1 PG isolado; launcher oficial confirmou
Q10/P3/N2 em documentos próprios, classificação tardia e os2 erros da avaliação.
Doctor offline16 OK/2 avisos/0 falhas. English object-vader-en e versões dos
retornos permanecem preservados. Nenhuma qualificação de captura/conta/Expert.
