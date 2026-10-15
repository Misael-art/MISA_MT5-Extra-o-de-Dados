# Fase 18 — Relações entre indicadores e aderência temporal dos setups

Pedido do dono em 2026-10-15. **PLANEJADO; nenhum cálculo, coluna, tela, preset ou API foi implementado por este documento.**
Registro T18.0; implementação em T18.1–T18.9, uma tarefa por PR. Os comandos e schemas abaixo são contratos propostos.
Integração com F13 (características/hipóteses), F16 (operações), F17 (apresentação) e F15 (recursos).
Este plano não substitui validação, bateria, paridade, guardião nem autoriza operações financeiras.

## 1. Objetivo e significado

Pré-calcular relações por ativo, corretora/contrato, timeframe, Expert/versão/preset e direção, para:
1. identificar indicadores redundantes ou complementares;
2. estudar relação entre contexto disponível e movimentos futuros;
3. explicar ganhos, perdas e risco dos Experts;
4. acompanhar estabilidade/descaracterização ao longo do histórico;
5. produzir candidatos a filtros/presets com evidência e rastreabilidade;
6. permitir leitura local e, depois, API somente leitura.

Correlação é associação, não causalidade, taxa de acerto ou garantia de lucro.
Ausência de correlação linear não prova ausência de relação não linear nem que um Expert não funciona.
Correlação entre indicadores pode significar informação repetida, não confirmação independente.
Uma associação negativa pode ser útil (por exemplo, volume baixo associado a losses); seu sinal não é aprovação/reprovação.
Aderência do setup deve separar **presença da configuração**, **evidência histórica de utilidade**, **qualidade/frescor**
e **autorização operacional**. A tela não pode reduzir esses quatro eixos a um semáforo de compra/venda.
Presença do setup exige uma regra explícita ou sinal emitido pelo Expert; sem esse contrato, fica UNKNOWN.
Não inferir o que o código do Expert considera setup apenas pela correlação dos seus resultados.

## 2. Famílias de análise

| Família | Unidade e alvo | Resultado |
|---|---|---|
| Indicador × indicador | Mesmas observações point-in-time | Pearson/Spearman, redundância e estabilidade; sem linguagem de lucro |
| Indicador × movimento | Característica em t × retorno futuro t→t+h | Efeito por horizonte e direção, não correlação com nível bruto do preço |
| Indicador × resultado do Expert | Contexto antes da entrada × resultado após encerramento | Acerto líquido, loss, resultado líquido/R, caudas, MAE/MFE quando disponíveis |
| Conjunto × alvo | Regras conjuntas limitadas, pré-definidas ou selecionadas só no treino | Ganho incremental contra regra simples e Expert original |
| Aderência no tempo | Configuração congelada × janelas posteriores | Presença, utilidade OOS, deterioração, inversão e indisponibilidade separadas |

Rótulos de sucesso: lucro líquido > 0; loss: lucro líquido < 0; zero/empate separado.
Custos incompletos impedem chamar o alvo de líquido. Custos zero só se conhecidos e justificados.
Não correlacionar com uma coluna constante contendo apenas trades vencedores. Incluir vencedoras, perdedoras e empates,
com denominador e população explícitos. Comparações de distribuição só dentro dos vencedores/perdedores são descritivas.
Não duplicar como “duas descobertas” associações complementares com win e loss quando o alvo for estritamente binário.

Resultados de Expert exigem identidade de código/preset/execução e entradas/direções válidas. O ledger local possui
limitações registradas na F13: não inferir eventos de entrada ausentes a partir da saída.
Relação do mercado pode ser calculada sem operações; relação do Expert não pode ser fabricada sem ledger/sinais.
Distinguir trades executados, todos os sinais, sinais bloqueados e contrafactuais. Sem sinais bloqueados o estudo
descreve a população selecionada pelo Expert, não todos os momentos do mercado.

## 3. Indicadores e transformações

T18.1 inventaria e reutiliza os cálculos existentes. Cobrir RSI/IFR, ATR, Bollinger, Keltner, MACD, volume/RVOL,
tendência, retornos e contexto temporal. Indicador indisponível aparece como tal; não substituí-lo por zero.
Cada definição informa fórmula, parâmetros, unidade, aquecimento, fonte, timeframe, versão e disponibilidade.

Exemplos de variáveis comparáveis:
- RSI: nível, inclinação e cruzamento de limiar;
- MACD: linha, sinal, histograma, inclinação e distâncias normalizadas;
- Bollinger/Keltner: largura, posição no canal, distância em ATR, compressão e cruzamentos;
- ATR: percentual do preço, percentil causal e variação;
- volume: real OU ticks, RVOL no mesmo momento da sessão, inclinação e percentil causal;
- preço: retornos e distância de níveis, nunca preço bruto em tendência como evidência preditiva.

Normalização, seleção de limiares e parâmetros são ajustadas no treino e congeladas na validação.
Eventos discretos, categorias e variáveis contínuas usam métodos apropriados; Pearson não vira método universal.
MACD tem caminhos opcionais que hoje podem produzir NaN: testar disponibilidade real sem dependência opcional,
reutilizar ou implementar fórmula pura testada, sem deixar resultado depender silenciosamente de pandas-ta.

## 4. Tempo, população e prevenção de vazamento

- Features devem estar disponíveis antes ou no instante da decisão; barra superior aberta fica excluída.
- Movimentos futuros são rótulos, com início, fim e disponibilidade próprios, nunca features.
- Acerto/PnL só fica disponível após encerramento e recebimento do resultado; janelas em t usam só alvos já maduros.
- Custos, revisões e recepções tardias entram via as-of; separar reconstrução histórica e histórico recebido.
- Célula do tempo t jamais usa acontecimentos posteriores a t; não pintar o passado com um ranking escolhido no fim.
- Sessões, pausas, fusos, offset fixo/DST e séries contínuas preservam as limitações documentadas da origem.
- Compras e vendas são separadas; comparações agregadas exigem ponderação e composição declaradas.
- Não juntar corretoras, contratos, estratégias, presets ou timeframes diferentes como observações equivalentes.
- Alvos sobrepostos e posições simultâneas exigem embargo e inferência por blocos/sessões, não n trades independentes.
- Censura: operações abertas e retornos cujo horizonte ainda não terminou não são losses nem zeros.

Modos do histórico:
**descritivo retrospectivo** pode cobrir toda a base, mas nunca afirma simulação disponível no passado;
**replay OOS** mostra aquecimento cinza, seleção no treino e janelas futuras sem acesso ao desfecho.
O ranking inicial usa apenas a janela inicial de treino. “Melhor por janela” é visão distinta, identificando mudanças
do indicador/regra e seu orçamento de busca; não costurar vencedores retrospectivos em uma curva supostamente executável.

## 5. Estatística e descoberta

Primeira versão:
- Pearson e Spearman para pares contínuos; coeficiente, intervalo, n de pares válidos, sessões e período;
- variável binária × contínua: associação ponto-bisserial e comparação condicional com baseline;
- win/loss, payoff, expectativa líquida/R e perdas extremas apresentados em conjunto;
- candidatos não lineares por faixas/limiares simples, definidos no treino;
- interações de dois fatores com orçamento explícito. Triples/modelos complexos ficam fora do primeiro ciclo.

Coeficientes de família diferente não são diretamente comparáveis em um ranking universal.
Constantes, variância quase nula, aquecimento e dados ausentes produzem null + reason.
Não remover silenciosamente outliers: publicar regra de tratamento e resultado de sensibilidade.

Reutilizar a infraestrutura de T13.4 quando suas capacidades forem comprovadas:
seleção aninhada no treino, purging/embargo, reserva final, múltiplas comparações, bootstrap temporal e registro de tentativas.
Auditar famílias de testes: indicadores × versões × ativos × direções × horizontes × janelas × interações.
Definir família/correção antes da busca; registrar testes descartados e exposições repetidas ao mesmo holdout.
p ajustado não é probabilidade de lucro; n mínimo não substitui análise de incerteza e poder.
Experimentos de ruído verificam taxa de falso positivo com tolerâncias justificadas, não promessa de zero falsos positivos.

Teste incremental de volume (exemplo): Expert original versus filtro de volume com mesma avaliação OOS e custos.
Controlar horário/regime e redundância com indicadores do setup antes de atribuir utilidade ao volume.
Quando filtro altera capital, posição ou sinais seguintes, exigir reexecução completa; ledger filtrado é contrafactual.
Convergência de dois indicadores deve superar cada um isolado e o baseline em OOS antes de recomendar pesquisa de preset.
Não alterar critérios existentes de aprovação/bateria.

## 6. Persistência: colunas adicionais sem explosão de schema

As novas **colunas analíticas consumíveis** ficam em views/consultas e exportações largas versionadas, apoiadas por
tabelas internas normalizadas. Não adicionar uma coluna física por par de indicadores em cada tabela OHLCV.
Preservar tabelas de usuário; dados brutos, features, rótulos e avaliações ficam separados.
Reutilizar _feature_definitions/_feature_values e contratos de F13/F16; tabelas abaixo só para entidades ainda inexistentes.

Modelo lógico a confirmar em T18.1:
- _association_definitions: id, versão, família, feature(s)/versões/params_hash, alvo/horizonte, população, método,
  plano de janela, folds, política de custos, regras de qualidade, search_budget;
- _association_runs: run_id, definição, escopo, dataset_hash, source_revision, as_of_utc, estado, cursor/checkpoint,
  versão do código, semente, tentativas, tempo/RSS/bytes, erro;
- _association_values: run_id, chave estável da célula, janela/segmento, feature(s), alvo, direção, estimator,
  effect/interval/p_raw/p_adjusted, n_pairs/n_sessions/n_wins/n_losses/n_flat, baseline, delta_OOS, custo/qualidade,
  evidence_state, available_at_utc, valid_until_utc, source_revision;
- _setup_alignment: definição/preset congelados, janela, presence_state, evidence_state, freshness_state,
  support_state, utility_state, reason_codes, relation_ids, comparação baseline e métricas de risco;
- _research_candidates: referências às hipóteses/avaliações existentes, configuração proposta, validade e estado;
  não duplicar catálogo de T13.4 se ele representar a entidade.

Features em t pertencem ao feature store; avaliações maduras pertencem a associações. Campos extensíveis devem manter
versão e tipos verificáveis, sem JSON opaco como única forma de consultar métricas essenciais.
Chaves incluem contrato/corretora, símbolo, timeframe, Expert/versão/preset/run quando aplicável e direção.
Índices por escopo, definição/versão, janela e disponibilidade; uniqueness para retries e revisões append-only.
Escolha física e migrações aditivas são entregues e testadas em T18.1; nenhum DDL foi executado neste registro.

Exemplo de colunas projetadas para Laboratório/agentes:
indicator_id, indicator_version, target_id, method, effect, effect_ci_low, effect_ci_high,
window_start_utc, window_end_utc, n_pairs, n_sessions, p_adjusted, oos_delta_net,
presence_state, utility_state, evidence_state, freshness_state, reason, available_at_utc, valid_until_utc, source_revision.
Valores desconhecidos são null; effect negativo não muda automaticamente o estado para vermelho.

## 7. Pré-processamento, recursos e serviço de leitura

Acoplar a producer_jobs da T13.3b, consultando write_guard da F15:
- execução opt-in/manual ou agendada com limites de pares, janelas, CPU, memória e armazenamento;
- cálculo em lotes por ativo/família, cache por hash de parâmetros/dataset e revisão;
- append lê delta/cauda necessária; correção tardia invalida janelas dependentes e descendentes;
- checkpoint transacional, cancelamento/retomada, idempotência, log de RUNNING interrompido e política de recuperação;
- não materializar todos os pares × todas as barras por padrão;
- separar base de pesquisa do limite de latência de consumo; medir cenários e documentar hardware/amostra.

Primeiro serviço Python/CLI somente leitura e views documentadas; agentes não escrevem tabelas por SQL ad hoc.
Contratos propostos, **ainda inexistentes**:
`mt5x analytics relations compute|list|show|timeline|export`
e `mt5x analytics relations candidates`. Queries exigem escopo, as-of e versão ou política explícita de última versão.
DTO próprio (relation_schema_version) preserva envelope/proveniência de T13.6 sem quebrar schema_version=1 existente.

Depois API local, opt-in, vinculada a loopback, somente leitura, paginação e limites de consulta, validação de input,
sem SQL arbitrário, execução de código ou controle de ordens. Reutilizar servidor existente se compatível, sem expor
rotas operacionais por acidente. Acesso remoto exige desenho de autenticação/TLS/autorização específico.
Endpoints propostos: GET /api/v1/relations, /api/v1/relations/timeline, /api/v1/relations/candidates.
Sem rota de apply, auto-tune ou order. Política de acesso deve cobrir clientes web maliciosos, origem/Host e token local
quando aplicável. Não expor contas/credenciais. Campos/filtros allowlist e exportação CSV protegida contra fórmulas.

## 8. Mapa temporal e linguagem visual

Subaba do Laboratório: **Relações e aderência**. Navegação interna:
**Matriz**, **Linha do tempo**, **Ganhos e perdas**, **Convergências**, **Candidatos**.

Eixo horizontal sempre tempo cronológico do início ao fim da base selecionada. Linhas são indicadores/regra ×
Expert/ativo, com compra/venda separadas. Janela, passo, horizonte e versão ficam visíveis.
Retângulos representam intervalos medidos, não interpolação de lucro. Mostrar limites de treino/OOS e rótulos maduros.
Zoom/pan, seleção de intervalo, detalhe clicável, ordenação e exportação reproduzível.
Ordenação “melhor” exige métrica e período de ranking explícitos; opção padrão ordena no treino congelado.

Duas legendas distintas:
1. **associação assinada**: escala divergente fixa de -1 a +1 para correlação; zero ao centro, sem associação
   visual com “funciona/não funciona”. Escalas de outros estimadores não são reaproveitadas.
2. **aderência/evidência**: azul vibrante = utilidade favorável sustentada no escopo OOS definido;
   amarelo opaco = deterioração detectada contra referência congelada, com suporte para comparação;
   vermelho opaco = evidência de incompatibilidade/efeito adverso sob critério pré-definido.
   “Sem evidência útil detectada nesta janela” deve ser distinguido de efeito adverso; nenhum deles prova
   impossibilidade permanente do Expert. Dados insuficientes/constantes/aquecimento = cinza/padrão + motivo;
   vencido = badge STALE; exploratório = badge próprio, não azul de aprovação.

A solicitação “azul = setup ativo” é atendida por **indicador de presença separado** (texto/ícone/configuração
atendida). Não confundir configuração presente com associação validada. “Nenhum fator correlacionado” não vira
vermelho por default: pode haver relação não linear, baixa amostra ou baixa potência estatística.
Categorias sem evidência favorável, sem deterioração demonstrada nem efeito adverso ficam inconclusivas/neutras.

Contrato semântico mínimo: presence_state = PRESENT/ABSENT/UNKNOWN; utility_state =
FAVORABLE/DETERIORATING/ADVERSE/INCONCLUSIVE; evidence_state = EXPLORATORY/VALIDATED/INSUFFICIENT;
freshness_state = CURRENT/STALE/UNAVAILABLE; support_state informa qualidade/amostra e motivo.
Uma evidência validada pode ser adversa. Azul de utilidade requer estado favorável, evidência validada e suporte/frescor
adequados; nenhuma cor resolve autorização operacional. Descrever separadamente indisponibilidade, não significância
e evidência de efeito abaixo do mínimo útil, esta última apenas com teste/limite de equivalência pré-especificado.

Não definir aderência como média de correlações nem porcentagem de win. Primeira entrega usa estados explicáveis
com regras/limiares congelados no treino; score numérico futuro exige definição/calibração e versão próprias.
Nenhuma célula azul autoriza ordem.

## 9. Requisitos visuais

Reutilizar F17 (Tkinter/ttk, matplotlib, tokens, temas e componentes); não migrar toolkit.
- Fontes e valores legíveis em tamanho real, texto sobre cor com contraste medido; usar texto escuro em amarelo claro.
- Legenda fixa próxima do gráfico, amostras de cor rotuladas, sinal/unidade e aviso de baixa amostra visíveis.
- Tooltip e painel de detalhe: período, janela, escopo, método, efeito/IC, n/sessões, custos, tentativa/ajuste,
  baseline/OOS, disponibilidade e motivo do estado. Informações essenciais também por teclado/clique.
- Matriz indicador × indicador simétrica quando o método for simétrico; defasagens/direcionalidade não são simétricas.
- Ganhos/perdas lado a lado, mesma unidade, população e escalas comparáveis, com empate e distribuição/caudas.
- Comparação de várias séries não cobre dados com legenda; limitar seleção e usar painéis alinhados.
- Não apresentar ranking comum somando coeficientes incompatíveis; clusterização é exploratória e explicada.
- Resize empilha painéis; paginar/virtualizar linhas; rasterização/agregação de desenho não altera métricas exportadas.
- Carregando, cancelado, vazio, incompleto, erro e stale possuem estados próprios. Sem métricas inventadas.
- Testar claro/escuro, 1280×800, 1920×1080, DPI alto, teclado, contraste e muitas janelas/linhas.
- Fixture sintética conhecida permite começar UI em paralelo; selo “DADOS SINTÉTICOS” permanente e nenhuma decisão operacional.

## 10. Tarefas e critérios de aceite

| ID | Entrega | Depende de | Aceite principal |
|---|---|---|---|
| T18.0 | Especificação, roadmap, matriz e prompts | Pedido do dono | Registro coeso; implementação não declarada pronta |
| T18.1 | Registry de indicadores/transformações, alvos e contrato físico/DTO | T13.2, T13.3 | Fórmulas determinísticas, MACD sem NaN silencioso, disponibilidade, tipos, schema aditivo e fixture DTO |
| T18.2 | Indicador × indicador e × movimento futuro | T18.1, T13.4 (infraestrutura validada) | Pearson/Spearman e alvo/horizonte, constantes/missing, maturidade, temporalidade e inferência testadas |
| T18.3 | Indicador × resultado de Expert, compra/venda e ganho/loss | T18.2, T16.1, T16.3b | Ledger válido, custo, população e baseline; sem winners-only; dado ausente impede avaliação indevida |
| T18.4 | Convergências limitadas e utilidade incremental | T18.3, T13.4 | Duplas superam univariadas/baseline em OOS, busca registrada, sem seleção no teste |
| T18.5 | Janelas móveis, aderência e jobs incrementais | T18.2; T18.3/4 para trilhas do Expert/conjunto; T13.3b, T15.9 | Ranking congelado, nenhuma célula usa futuro, revisões/retomada e estados separados |
| T18.6 | Serviço local de consulta, CLI, projeções de colunas e DTO | T18.2, T18.5; T18.3/4 para resultados respectivos | as-of, JSON sem NaN, paginação, views/exports tipados e parity cálculo×consulta |
| T18.7 | Candidatos a filtros/presets para otimização | T18.4, T18.6 | Manifesto com proveniência, comparação/reexecução, nenhum auto-apply; critérios Safety preservados |
| T18.8 | UI Relações e aderência integrada ao Laboratório | T18.1 fixture; T18.6 para integração; T17.1–T17.5 | Contrato visual, tempo correto, legendas/contraste, interação e capturas; sem recálculo no widget |
| T18.9 | API local somente leitura | T18.6 | Contrato versionado, limites/acesso seguros, as-of, nenhuma escrita/ordem e equivalência com serviço local |

**Passos de cada tarefa:** ler dependências e esta especificação; auditar código disponível/branches e definir arquivos;
testar baseline; implementar o recorte; executar testes indicados abaixo e regressão exigida pelo AGENTS; medir/inspecionar
quando aplicável; atualizar painel e continuidade com evidências; entregar uma tarefa por PR.
T18.1 deve detalhar arquivos de schema/registry e congelar fixture/DTO antes de T18.8 começar a implementação.
T18.2–T18.7 têm prioridade analítica P1; T18.8 é P1 em paralelo; T18.9 é P2 após consumo local validado.

Implementação local iniciada em 2026-10-15: ver [T18.1_REGISTRY_DTO.md](../analytics/T18.1_REGISTRY_DTO.md) para
o schema aditivo, escopo de timeframe, unidades, regras de maturidade e fixture v1. T18.2–T18.9 não são concluídas
por esse contrato; nenhuma estatística real nem serviço da UI está disponível ainda.

**Locais previstos, a confirmar por auditoria:** analytics/indicator_registry.py, analytics/associations.py,
analytics/association_store.py, analytics/association_jobs.py, analytics/setup_alignment.py,
analytics/research_candidates.py; adaptadores de trade_analytics reutilizam ledger/enrich; UI em
ui/relations.py + charts/components existentes; CLI importa serviços, nunca Tkinter.
Nomes são proposta; não duplicar produtor ou store já existente.
Novas chaves de config, se necessárias, vão para DEFAULTS e example com fallback; nenhuma chave adicionada neste plano.

**Testes obrigatórios de implementação:**
- séries com relações conhecidas positiva, negativa, nula e não monotônica; preço tendencial espúrio;
- constante, NaN, volume zero/desconhecido, custos incompletos, classe única e nenhuma operação;
- mudar dados futuros não altera célula já publicada, seleção nem ranking passado;
- outcome só entra após maturidade/encerramento; correção tardia respeita as-of;
- redundância de indicadores não gera falsa confirmação independente;
- duplas sem efeito versus efeito incremental conhecido, busca/holdout auditados;
- compras/vendas, presets e moedas não se misturam; ledger faltante não vira amostra zero “válida”;
- replay completo × incremental, cancelamento/retomada, revisão e write_guard;
- DTO/CLI/API iguais, JSON null sem NaN, exportação segura e consultas limitadas;
- UI todos os estados, cores/legendas e teclado, temas/resoluções, seleção/zoom e muitas linhas;
- nenhuma ordem ou mudança de risco, analytics desativado preserva comportamento existente.

Performance: publicar baseline de pares×janelas, barras/trades, CPU/RSS/disco e latência de consulta/desenho.
Fixar orçamento mensurável a partir do host e do dataset, sem prometer latência arbitrária.
Testes unitários não exigem MT5 real; Windows real é pendência registrada, não bloqueio das entregas Linux.

## 11. Presets, Experts e agentes

Consumir inicialmente pelo serviço/CLI/projeções locais; posteriormente T18.9 fornece API.
Candidato de preset inclui Expert/hash, símbolo/contrato/corretora, TF/direção, regra e parâmetros, versões de
features, dataset/revisão, treino/OOS/holdout, custos, tentativas, métricas, validade, limitações e motivo.
É **proposta de pesquisa**, não preset aplicado/aprovado. Não aumentar risco nem reescrever preset vigente.
O otimizador registra orçamento e tentativas e valida OOS; nenhuma busca pode usar o mesmo holdout reiteradamente
como se fosse independente. Aplicação/piloto continua dependente de T13.7, guardião, bateria, paridade e autorização.
Publicação para MQL5 reutiliza T13.5 quando pronta; pesquisa local e UI offline não dependem de ligar o terminal.

## 12. Coordenação e integração

T18.0 é somente documentação em worktree separado, baseada no checkpoint local T13.6.
Outras branches analíticas e F17 não estão automaticamente integradas só por existirem; verificar ancestralidade,
diff e contratos antes de implementar. Não fazer merge amplo por conveniência.
A entrega documental deve chegar à branch dos agentes por integração revisada; deixar arquivo em worktree não o coloca no master.
Analytics possui cálculos/schema/DTO/CLI; UI possui apresentação e consome fixture/serviço.
Schema/DTO congelado e versionado; nenhum agente altera unilateralmente o contrato do outro.
T18.8 pode prototipar com fixture após T18.1 e implementar por etapa; tela integrada só conclui após serviço real.
Planos existentes T13.5–T13.8 mantêm dependências; este pedido autoriza planejar/implementar F18, não resolve
implicitamente decisões antigas de validade, confiança ou pilotos.

## 13. Referências

- [SciPy Spearman](https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.spearmanr.html):
  relação monotônica, constante indefinida; não confundir com inexistência de relação geral.
- [Bailey e López de Prado — Deflated Sharpe Ratio](https://www.davidhbailey.com/dhbpapers/deflated-sharpe.pdf):
  seleção, múltiplos testes e inflação de resultados de backtest.
- [MQL5 CopyRates](https://www.mql5.com/en/docs/series/copyrates):
  posição zero é barra atual, que pode estar incompleta.
