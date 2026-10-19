# FRP — Fluxo Relativo do Preço: hipótese de pesquisa futura

**Registrado em 2026-10-17 a pedido do proprietário. Estado: ESTUDO FUTURO, prioridade P3.**
FRP-00 entrega somente este registro conceitual. Não há fórmula definitiva, indicador
implementado, backtest executado ou evidência de segurança operacional nesta entrega.
Consolidar a base antes da pesquisa; esta proposta não entra no caminho crítico da F19.

Referências internas: [relações F18](../propostas/RELACOES_INDICADORES_SETUP.md),
[pilares F19](../propostas/PILARES_DADOS_OPERACAO_PRESETS.md),
[catálogo dos estudos](../propostas/CATALOGO_F19_INDICADORES_SETUPS.md) e
[plano F19](../propostas/PLANO_F19_PILARES_PRESETS.md).

**Complemento de 2026-10-17:** a [F20](../propostas/ROBUSTEZ_PESQUISA_EXECUCAO.md)
qualifica dados/protocolo/reprodução e T20.15 oferece infraestrutura de ablação e
contribuição incremental para uso futuro. Não antecipa FRP-01–04, não define fórmula
e não torna FRP dependência de presets/Ultra ou do primeiro ciclo F19/F20.

## 1. Ideia original e objetivo

No mesmo ativo, identificar grupos de **três ou quatro indicadores** que tenham:

1. correlação elevada entre si;
2. correlação elevada com o movimento/fluxo do preço;
3. concordância direcional observável no instante da decisão;
4. relação útil com a estratégia do Expert, quando houver sinais/ledger suficientes.

A hipótese inicial do proprietário é que, com esses indicadores concordantes e fluxo
do preço ascendente, o conjunto possa produzir um novo indicador de confirmação,
chamado **FRP — Fluxo Relativo do Preço**, útil para estudar entradas e filtros.
Uma variante descendente é pesquisa separada; não pressupor simetria de compras/vendas.

Explorar coeficientes **0,60 / 0,70 / 0,80 / 0,90**, como exemplos de limiares configuráveis,
sem escolher um default por este registro. São valores de correlação; não significam
60%/70%/80%/90% de probabilidade de acerto, confiança estatística ou segurança do trade.
O objetivo futuro é verificar a hipótese e conceber a matemática com o proprietário.

## 2. O que significa “fluxo do preço” nesta proposta

Definir antes do experimento o movimento considerado: retorno passado por janela,
inclinação causal, direção normalizada pela volatilidade ou outra transformação explícita.
Não usar nível bruto de preço em tendência como confirmação suficiente.

O FRP é uma proposta sobre comportamento do preço e indicadores. Não é sinônimo de
fluxo de ordens/agressão, CVD ou desequilíbrio de book. Uma extensão com negócios/book
exige fontes qualificadas e uma hipótese própria; não é pré-requisito da ideia inicial.

Separar duas perguntas:

| Pergunta | Comparação | Significado do resultado |
|---|---|---|
| Descritiva | Indicadores × movimento de preço já observado na mesma janela | Concordância atual/passada; não comprova antecipação |
| Preditiva | Indicadores disponíveis em t × retorno/direção posterior t→t+h | Hipótese de utilidade futura, avaliada fora da amostra |
| Do Expert | FRP antes do sinal/entrada × resultados posteriores do Expert | Utilidade incremental para uma estratégia/versão/preset determinados |

As três perguntas têm populações e alvos próprios. Não transportar uma correlação
descritiva para uma conclusão preditiva ou operacional sem o teste correspondente.

## 3. Estrutura matemática a conceber posteriormente

Esta seção registra possibilidades, não uma fórmula aprovada.

- **Universo:** indicadores/transformações do registry, no mesmo ativo/contrato/corretora
  e timeframes explicitamente alinhados por disponibilidade.
- **Grupo:** G com 3 ou 4 componentes; não escolher o grupo olhando o resultado do teste.
- **Janela:** tamanho, frequência de recalibração, sessões e amostra válida previamente definidos.
- **Método:** Pearson ou Spearman conforme a pergunta/tipo de variável; não alternar para
  o que der maior número depois de ver a reserva.
- **Dois limiares:** tau_ind para relações entre componentes e tau_preco para a relação
  de cada componente/conjunto com o alvo de preço. Não presumir que ambos devem ser iguais.
- **Sinal:** definir se exige correlação positiva ou alinhamento de componentes inversos;
  usar módulo da correlação não pode apagar silenciosamente uma direção contrária.

Uma hipótese estrita de grupo seria exigir que **todos os pares** de G superem tau_ind
e que cada componente tenha associação com o alvo acima de tau_preco, após orientação
definida no treino. Uma hipótese menos restritiva poderia exigir associação do conjunto
ao alvo; deve ter versão e contagem de tentativas separadas.

Representar as relações como grafo pode ajudar a procurar grupos, mas a escolha do grupo,
dos períodos, dos limiares e da orientação é parte da busca e precisa ser contabilizada.
Não confundir existência de um grupo altamente conectado com informação independente.

Para combinar os componentes, estudar normalizações causais e concordância direcional:
por exemplo, transformações s_i(t) comparáveis e média ponderada com pesos não negativos
que somem 1. A direção do preço observado pode atuar como condição adicional.
Escolher entre média, votação, mínimo de concordância ou outra regra somente no protocolo.
Score de concordância não recebe o nome de “probabilidade” sem calibração e teste próprios.

Componentes, pesos, normalizações e limiares são ajustados no treino e congelados para
validação. Se usar janelas móveis, recalibração tem agenda e usa apenas passado disponível.
Alvos futuros entram na estimação histórica somente depois de maduros; não entram no
cálculo do FRP em uma decisão passada antes de terem ocorrido.

## 4. Perguntas que o agente deve ajudar a resolver

1. Como medir “fluxo relativo”: relativo à volatilidade, ao movimento recente, à sessão
   ou a uma referência? Qual unidade/escala e significado econômico de cada saída?
2. Quais transformações dos indicadores representam direção, e não apenas intensidade?
3. Quanto da alta correlação vem de fórmulas parecidas calculadas sobre o mesmo preço?
4. O conjunto acrescenta algo ao melhor componente ou apenas repete a mesma informação?
5. Qual horizonte h e qual ação serão avaliados: confirmação, filtro, entrada ou gestão?
6. Qual correlação importa: simultânea, defasada, por regime ou com retorno futuro?
7. O efeito sobrevive a custos, spreads, mudanças de regime e janelas realmente posteriores?
8. O FRP melhora uma estratégia concreta ou só descreve movimentos que já aconteceram?
9. Como detectar grupo instável, dado vencido, pouca amostra ou perda de relação no tempo?

Essas decisões serão concebidas em estudo futuro com o proprietário. Não preencher lacunas
com parâmetros escolhidos informalmente para produzir um indicador aparentemente seguro.

## 5. Pré-requisitos da pesquisa aplicada

- Base histórica com qualidade/cobertura, calendário e timeframes reconciliados.
- Store/registry F13/F18 integrados, com fórmulas, versões, aquecimento e available_at.
- Relações reais de T18.2–T18.4 e consulta T18.6 verificadas, sem confundir fixture sintética
  da UI com estatística calculada sobre o ativo.
- Para a trilha do Expert: identidade de código/preset/execução, direção/entrada e custos;
  ausência desses dados permite somente a trilha de mercado, não inventar uma relação do robô.
- Motor de avaliação cronológica, controle de tentativas e métricas/PF com estados definidos.

Cumprir esses pré-requisitos por capacidade comprovada; não exigir gestão global de conta
para pesquisa offline nem introduzir o FRP como dependência do primeiro preset ou do Ultra.

## 6. Protocolo futuro de validação

1. Registrar hipótese, ativo/TF, transformações, alvos/horizontes, janelas, tau_ind/tau_preco,
   famílias de comparação, limites de busca e critérios de aceitação antes dos resultados.
2. Separar treino, validação cronológica e reserva final; controlar maturidade dos alvos,
   sobreposição temporal, embargo e seleção repetida de grupo/parâmetro.
3. Selecionar grupos e estimar pesos somente no treino; congelar as regras de cálculo.
4. Comparar com direção/momentum do preço, cada componente isolado, o melhor componente
   escolhido no treino e o Expert original. Testar contribuição incremental do conjunto.
5. Avaliar direção ascendente e, se proposta, descendente em trilhas separadas; por regime
   e período, com n/sessões, incerteza, estabilidade e cobertura explícitos.
6. Para uso como filtro/sinal, reexecutar a estratégia completa com capital, frequência,
   custos e risco equivalentes; ledger filtrado sozinho é evidência contrafactual.
7. Testar séries sem sinal, componentes duplicados, correlação alta sem poder preditivo,
   componentes anticorrelacionados, quebras de regime e dados com vazamento proposital.
8. Reportar também resultados negativos ou insuficientes. Correlação acima de um limiar
   não é critério suficiente de aprovação; nenhuma meta de acerto é imposta por este registro.

Usar os mesmos princípios F18 de redundância, alvos maduros, bootstrap temporal e controle
de múltiplas tentativas. O estudo de 3/4 fatores é uma extensão futura: não amplia o primeiro
ciclo da F18, que limita as interações iniciais a dois fatores.

## 7. Saídas candidatas e integração posterior

Estudar saída com direção (ascendente/descendente/neutra), força de concordância, grupo/
componentes/versões, método/janela, correlações/limiares, amostra/sessões, qualidade/frescor
e motivo de indisponibilidade. Dado ausente ou aquecimento não vira score neutro/zero.
Separar direção atual, evidência de utilidade histórica/OOS e autorização operacional.

Se houver utilidade demonstrada, registrar o FRP como indicador derivado composto no mesmo
registry/store, sem um banco paralelo. Poderá então ser pesquisado no Laboratório e como
filtro/contexto de presets, inclusive Ultra, sem virar gatilho obrigatório.
Consumo no HADOUKEN exige contrato/capacidades, paridade, versões e validação da composição;
correlação não habilita ordens ou altera risco automaticamente.

## 8. Backlog reservado, fora do caminho crítico

| ID | Etapa futura | Dependências e aceite | Estado |
|---|---|---|---|
| FRP-00 | Registrar ideia e limites do estudo | Este documento e links nos índices | REGISTRADO; somente documentação |
| FRP-01 | Conceber hipótese e matemática com o proprietário | Definições das seções 2–4; protocolo escrito, variantes e decisões abertas | ESTUDO FUTURO / P3 |
| FRP-02 | Protótipo offline de grupos e score | FRP-01 e base/registry/relações qualificados; cálculo causal reproduzível | ESTUDO FUTURO / P3 |
| FRP-03 | Avaliar utilidade incremental OOS | FRP-02; comparações, incerteza/custos/estabilidade e resultados negativos preservados | ESTUDO FUTURO / P3 |
| FRP-04 | Decidir integração no produto/Expert | FRP-03 com evidência suficiente e decisão de escopo; contratos, paridade e UI | ESTUDO FUTURO / P3 |

Não iniciar protótipo, mineração de grupos ou integração como efeito deste registro.
Uma tarefa por PR quando cada etapa for promovida a PLANEJADO; os IDs da F19 não mudam.
Decisões sobre limiares/fórmula podem ser exploradas conceitualmente sem alegar validação.

## 9. Verificação do registro documental

SDK docs: 3 testes aprovados antes/3 depois (0,81/0,75 s). Doctor: 16 OK/2 avisos/0 falhas.
Links do conjunto documental conferidos (44), numeração das 54 fichas F19 preservada e
git diff --check limpo. Runtime/testes/dependências não foram alterados. A suíte integral
anterior da sessão não foi repetida nem apresentada como validação desta hipótese.
Nenhuma análise de mercado, fórmula, resultado financeiro ou ambiente Windows foi validado
por este registro; permanece uma proposta para concepção e investigação futuras.
