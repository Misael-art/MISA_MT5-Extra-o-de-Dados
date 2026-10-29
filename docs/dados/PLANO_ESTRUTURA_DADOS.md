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
