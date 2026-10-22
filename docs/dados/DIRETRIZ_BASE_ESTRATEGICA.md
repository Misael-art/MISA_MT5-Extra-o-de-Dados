# Diretriz 02 — base estratégica de dados e cálculos

Data: 22/10/2026. Revisão: 2.
Origem: orientação do proprietário, após a fotografia inicial dos pilares.
Prioridade: principal prioridade de desenvolvimento neste momento.
Estado: necessidade e critérios de estruturação registrados; execução a desdobrar nos pilares existentes.

## 1. Objetivo

Constituir uma estrutura estratégica reutilizável de bases e cálculos, na qual os
racionais, indicadores e cálculos compostos sejam executados corretamente e
possam sustentar o Laboratório, as análises e, posteriormente, uma API.

O resultado pretendido é uma cadeia coerente entre fonte, dado armazenado,
cálculo simples, cálculo composto, contexto e consumo. A base deve permitir
investigar as estratégias e suas condições com rastreabilidade suficiente para
comparar fotografias e reproduzir conclusões.

“Completa” requer um contrato de cobertura: universo de ativos, períodos,
timeframes, fontes e capacidades necessárias. Não significa declarar completo
o mercado inteiro, preencher ausências com dados inventados ou exigir uma
fonte que a corretora não disponibiliza. Lacunas devem permanecer visíveis.

## 2. Estrutura a desenvolver e verificar

| Camada | O que deve ser estruturado | Evidência esperada |
|---|---|---|
| Universo e cobertura | Ativos, períodos, timeframes e requisitos de cada estudo | Matriz de cobertura por série e capacidade, com lacunas e atualização |
| Fontes primárias | Candles, ticks, volumes, spread e especificações; demais fontes conforme o estudo | Origem e revisão identificadas, sem confundir observação, derivação e simulação |
| Tempo e instrumentos | Calendário, fuso, sessões e vigência de contratos/especificações | Regras temporais explícitas e correspondência com o instrumento correto |
| Qualidade | Validade, ausência, duplicidade, continuidade e disponibilidade | Diagnóstico por série/período e elegibilidade por consumidor |
| Indicadores simples | Fórmulas, parâmetros, sementes, unidades e aquecimento | Referências e testes de cálculo, com estados para dados insuficientes |
| Racionais e cálculos compostos | Dependências entre séries, indicadores e condições | Dependências explícitas, alinhamento temporal e regra de composição reproduzível |
| Persistência analítica | Valores, versões, qualidade, revisões e instante conhecido | Materialização consultável, retomável e rastreável |
| Contexto de operações | Relação entre mercado, sinal, execução, gestão e resultado | Identidades conciliadas entre dados, estratégias, presets e operações |
| Consumidores | Laboratório, análises e contrato de consulta para futura API | Mesma semântica, versões, unidades e estados de qualidade |

As linhas são categorias de trabalho e critérios a detalhar. Não declaram uma
lista fechada de indicadores nem antecipam a implementação de todos os estudos.

## 3. Contrato mínimo de um racional ou cálculo composto

Cada cálculo deverá declarar, conforme aplicável:

- Pergunta que procura responder e significado do resultado.
- Séries e indicadores de entrada, com fonte e versão.
- Fórmula/regra, parâmetros, unidade e domínio do resultado.
- Janela, aquecimento, sessão e timeframe de cada dependência.
- Alinhamento entre timeframes e instante em que cada informação está disponível.
- Tratamento de ausência, atraso, histórico insuficiente e revisão da fonte.
- Forma de persistir, atualizar e invalidar resultados dependentes.
- Consumidores previstos e requisitos para que possam usar o resultado.
- Evidência de correção numérica, causalidade e reprodução.

Um indicador calculável não equivale a um histórico materializado e qualificado.
Uma coluna existente não comprova preenchimento. Um resultado retrospectivo não
deve ser apresentado como informação conhecida antes de sua disponibilidade.

## 4. Critérios para acompanhar avanço

1. Cobertura requerida e cobertura efetiva identificadas por ativo/TF/período.
2. Dados obtidos, ausentes ou não suportados classificados de forma explícita.
3. Cálculos simples e compostos com regras, dependências e testes verificáveis.
4. Materialização e consumo verificados numa bancada compacta, com versão e qualidade.
5. Atualização, revisão de fonte, retomada e reprocessamento coerentes.
6. Laboratório e análises consumindo dados efetivos e identificados.
7. Consultas preparadas para a API futura, com contratos e semântica estáveis.
8. Evidência de recursos/tempo de processamento adequada ao volume utilizado.

Valores, limites e metas quantitativas ainda serão definidos por entrega.
O registro de uma necessidade ou a existência de código não constitui aceite.

## 5. Laboratório, análises e API

O Laboratório deve receber dados que permitam avaliar estratégia base,
confirmações, filtros e gestão operacional em contextos identificados.
As análises devem distinguir resultado conhecido, indisponibilidade e hipótese.
Exemplos sintéticos de apresentação não devem ser contabilizados como análises
da base real.

A futura API deverá consumir essa mesma base e suas definições, evitando
resultados divergentes causados por cálculos ou interpretações paralelos.
Preparar contratos de consulta faz parte desta direção; publicar um servidor
ou ligar consumidores externos fica para a etapa correspondente.

## 6. Relação com risco e ativação

A política de risco poderá mudar a elegibilidade de uma oportunidade, a
alocação e a simultaneidade de robôs. Ela não muda retroativamente os dados ou
a evidência da pesquisa. Resultados devem identificar a política utilizada.

A etapa atual prioriza o alicerce de dados/cálculos para subsidiar essas decisões.
As lacunas operacionais da fotografia inicial permanecem registradas, sem
remover suas travas ou alterar configurações de risco durante este registro.

## 7. Próximo desdobramento de planejamento

Confrontar inventário real, cobertura e produtores existentes com o escopo da
base estratégica; organizar entregas incrementais nos pilares e tarefas já
existentes, com critérios de aceite e evidências por fotografia.

Esta orientação altera a prioridade do desenvolvimento: a base estratégica
passa à frente de uma busca imediata por estratégias vencedoras ou de uma
ativação adaptativa ainda sem dados e cálculos qualificados.

## 8. Esclarecimento vinculante do proprietário — estrutura, bases de exercício e expansão

O principal alvo é o MECANISMO do projeto e suas ferramentas, não a homologação
financeira ou o abastecimento extensivo dos ativos exportáveis pela conta DEMO.
Esses ativos constituem bancada de construção e exercícios. As bases futuras
serão diversas; um resultado eficiente em um ativo não se transfere para outro.

A qualificação desta prioridade diz respeito à estrutura: ingestão, identidade,
qualidade reconhecida, indicadores, cálculos compostos, atualização e consumo.
Não gastar tempo ou disco tentando tornar a base DEMO um universo produtivo
presumido. Usar amostras mínimas que comprovem capacidades e cenários técnicos.
A configuração de cobertura descrita nas seções anteriores é um contrato do
mecanismo e do teste; não é uma campanha de extração/homologação desses ativos.

A estrutura deverá ficar robusta, funcional, qualificada e resiliente para
expansão, com todo o catálogo aprovado tratado, cálculos corretos e contratos
prontos para a futura API. Uma expansão deverá alcançar cruzamentos,
relacionamentos, dependências e consumidores elegíveis. Limitação do feed e
lacuna de implementação são estados distintos, ambos explícitos.

O plano de implementação20261022 organiza sete marcos e fichasBD01–BD22,
vinculadas aos planos/donos existentes, sem criar motores concorrentes. A cadeia
vertical inicial será compacta; a expansão do catálogo e seus consumidores será
comprovada por testes específicos e fotografias sucessivas. Implementação desta
estrutura não equivale a liberação de robôs ou publicação externa da API.
