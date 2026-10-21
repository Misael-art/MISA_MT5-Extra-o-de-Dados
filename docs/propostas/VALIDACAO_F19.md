# F19 — Matriz de validação e uso de replay nativo

**Retificação documental de 2026-10-17, solicitada pelo proprietário.** Aplica-se às tarefas
F19; [especificação](PILARES_DADOS_OPERACAO_PRESETS.md) e [plano](PLANO_F19_PILARES_PRESETS.md).
O [prompt único de execução](PROMPT_CONSOLIDADO_DESENVOLVIMENTO_MT5.md) incorpora estas regras,
incluindo a retificação do anexo que antes exigia operação ao vivo para todo aceite.
O complemento [F20/Q01–Q16](ROBUSTEZ_PESQUISA_EXECUCAO.md) usa a mesma distinção de
modalidades; [as fichas F20](PLANO_F20_ROBUSTEZ.md) acrescentam critérios por capacidade.
Para UI, aplicar [prioridades e aceites específicos](UI_ORGANIZACAO_GRAFICOS_PRIORIDADES.md):
UI-A01–A10 atuais por capacidade; UI-P01/P02 posteriores e UI-X01 experimental não são
pré-requisitos do aceite visual atual. UI/DTO/replay/financeiro mantêm provas separadas.

## 1. Princípio

Escolher a forma de prova adequada a cada requisito. Não exigir operação ao vivo para
aceitar cálculo, schema ou nomeação; não aceitar apenas mocks como demonstração visual
do funcionamento do gradiente ou dos stops no MT5.

O proprietário autoriza ensaios operacionais em **conta DEMO** e aceita **replay nativo do
MT5/Strategy Tester visual** quando o ativo estiver indisponível online ou fora de pregão.
O replay deve executar o Expert no terminal MT5 de verdade, no ambiente da conta DEMO;
re-simulação Python e fixture de UI são provas complementares, com identificação própria.
Ordens históricas do Tester não são ordens executadas na conta DEMO ao vivo.

## 2. Matriz por tipo de requisito

| Requisito | Evidência principal | Complementos/limites |
|---|---|---|
| Fórmula, unidade, causalidade, tempo, custos, métricas | Casos determinísticos/manuais, testes de núcleo e backtests adequados | Comparação independente e paridade Python/MQL5 quando aplicável |
| Schema, contrato, migração, persistência, cache, nomeação e IDs | Testes estruturais/de integração e ida e volta de arquivos/banco | Banco/preset antigo abre; não exige operação em mercado |
| Performance | Medições antes/depois, mesmo cenário, memória/tempo/I/O/resposta | Não relaxar precisão/aceites para obter velocidade |
| Utilidade estatística e seleção automática | Backtest completo, treino/validação/reserva, custos e risco equivalentes | Replay de recorte escolhido para QA não é OOS financeiro |
| UI moderna/adaptativa | Operar UI real em temas/DPI/resoluções/teclado, comparar DTO/valores | Testes de widgets e capturas apoiam, sem cálculo no widget |
| Entradas, gradientes, stops, trailing, alvos, tempo, filtros e inversão | Observação do Expert no MT5 nativo: replay visual ou ensaio ao vivo em DEMO disponível | Eventos/relatório reconciliados; cobrir as variantes implementadas |
| Default, Manual nomeado, preset/política Ultra no Expert | Testar valores efetivos; para efeito operacional, replay nativo/DEMO | Identidade, nome/metadados, regra/transição e parâmetros conferidos |
| Lotes e parciais | Casos conhecidos de quantidade/risco, mínimo/passo/saldo/custos e métricas; observar volume maior que um e reduções no MT5 nativo por replay/DEMO | Reconciliar entrada, parcelas e saldo; não confundir execução parcial com trade novo; preservar proteção |
| Comunicação, execução ao vivo, permissões/conta e condições que o Tester não reproduz | Ensaio específico em DEMO quando necessário, mais fakes de erro | Limitação/pendência delimitada, sem exigir disponibilidade ao vivo dos demais aceites |
| Patrimônio flutuante, margem/reserva e exposição conjunta | Referências independentes, sequências conhecidas e comparação nativa no recorte suportado | Saldo encerrado não prova risco patrimonial; declarar frequência de marcação e limite intrabarra |
| Dados/specs e pacote reproduzíveis, cache/invalidação | Snapshot consistente, hashes/revisões e round-trip isolado com legado | Reprodução indisponível fica explícita; não usar propriedade atual como dado histórico |
| Protocolo estatístico, múltiplas tentativas e ablação | Experimentos controlados, vazamento plantado, efeito conhecido e calibração em ruído | Medir falsos positivos/intervalos/poder, sem prometer zero universal ou ordenar todo Monte Carlo |
| Eventos, restart, duplicação e falhas | Injeção determinística e reconciliação com estado/ordens/deals próprios no MT5 | Tester só prova falhas que reproduz; rede/fill ao vivo mantêm prova DEMO específica |
| Observação futura e degradação | Decisões registradas antes do desfecho e acompanhamento prospectivo; séries controladas para o algoritmo | Replay não é forward; PnL virtual não é extrato, drift isolado não prova perda de vantagem |
| Orçamento/cancelamento/isolamento e backup/restauração | Limites/falhas induzidas, retomada e restore em destino de teste, sem ordens | Não apagar origem/terceiros; backup criado não basta; registrar integridade e tempo observado |
| Release e relatórios/builds/idiomas | Instalação repetida e fixtures/formato nativo dos ambientes suportados | Parsing desconhecido falha explicitamente; Windows simulado não é Windows físico |

Não flexibiliza Safety, bateria, contratos ou gates de grade/risco. Capacidade operacional
ainda sem suporte/política permanece impedida, mesmo se o mercado estiver aberto.

## 3. Replay com período escolhido para agilidade e cobertura

O agente pode escolher ativo/TF/período com histórico suficiente para exercitar a tarefa.
Usar o menor recorte que cubra os eventos relevantes e o aquecimento; ampliar ou usar
mais de um recorte quando uma passagem não os contenha. Não repetir toda a base sem motivo.

Antes do ensaio, listar os eventos esperados e a razão da escolha do período:

- Gradiente: semente, níveis adicionais, espaçamento, direção, limite de volume/exposição,
  pendentes/cancelamento e fechamento do ciclo; variantes fixas/dinâmicas/direcionais aplicáveis.
- Stops: stop inicial, acionamento, avanço do trailing/break-even, trava de recuo, gaps e
  coexistência com alvo/tempo conforme a tarefa. Um desenho na tela não prova execução.
- Filtros/tempo: bloqueio e liberação nas bordas, frequência/cota e saída temporal observável.
- Inversão/Ultra: direção original/efetiva, regra/prioridade/fallback e transição suportada.
- Presets: Default e Manual/derivados com valores efetivamente carregados e contexto correto.
- Lotes/parciais: quantidade inicial >1 na unidade do ativo, uma/várias reduções, volume
  remanescente e proteção; observar custos/resultado por evento, encerramento final e
  ausência de repetição da mesma etapa. Cobrir indivisibilidade/mínimo/passo por testes
  estruturais e agenda impossível com motivo; adições e netting/hedging só quando suportados.

Recortes são casos de QA, inclusive negativos/perdedores. Não escolher apenas os de lucro
nem tratar otimização para aparecer um evento como desempenho do preset original.
Variantes de teste isoladas preservam limites e têm parâmetros/identidade registrados.

Preferir ticks reais quando a sequência intrabarra afetar o comportamento e houver fonte
adequada; registrar o modelo, a qualidade/cobertura realmente demonstrados e as aproximações.
Não usar OHLC para afirmar a ordem exata de eventos intrabarra sem evidência.
Mesmo o modo de ticks reais pode substituir minutos por ticks gerados. Registrar origem/
cobertura/substituições demonstráveis; se faltarem metadados, qualidade desconhecida não
vira cobertura integral. Ver [documentação MT5](https://www.mql5.com/en/docs/runtime/testing).

Se o ativo não estiver disponível online, usar o replay com histórico/especificação
disponíveis no terminal. Se precisar de outro ativo compatível, declarar o escopo dessa
prova; nunca trocar símbolo/corretora silenciosamente nem atribuir o aceite ao ativo original.
Ausência de histórico ou de evento exigido impede somente aquele ensaio, com motivo registrado.

## 4. Evidência mínima e conclusão

Registrar modalidade (estrutura/backtest/replay/DEMO ao vivo), SHA/Expert/Safety, preset/ID/
valores efetivos, símbolo/corretora/TF, período, modelo/qualidade, eventos cobertos, logs,
capturas pertinentes e reconciliação do histórico produzido pelo MT5 com a aplicação.
Para Ultra, incluir o rastro regra/contexto/decisão; para gradiente, ciclo/níveis/exposição.
Não versionar credenciais, número pessoal de conta ou caminhos do host.

Replay nativo com cobertura e reconciliação satisfaz o aceite funcional histórico das
capacidades que reproduz; mercado fechado não bloqueia sua conclusão.
Não afirmar validação ao vivo ou de latência/preenchimento real da corretora a partir dele.
Manter pendente apenas o que de fato exige outro ambiente/condição, com próximo ensaio claro.

Ao operar ao vivo, comprovar/revalidar conta DEMO, respeitar limites e ownership e encerrar
apenas sessões/operações próprias de teste. Nunca operar conta REAL, aumentar limites para
passar um ensaio ou fechar posições/processos de terceiros.

Este documento retifica a exigência genérica anterior de testar tudo com mercado aberto.
Não altera a arquitetura nem certifica presets/retornos; implementação e evidências
continuam sendo entregues por tarefa.

## 5. Registro inicial de experimentos ([T20.12](../sdk/experiments.md))

Separar protocolo, alternativa, pedido deliberado e execução técnica. Uma alternativa
manual também é registrada; uma nova consulta deliberada não desaparece por usar cache,
e retentativa ou reexecução idêntica não vira amostra independente. O orçamento inicial
é por `evaluation_request`, incluindo suas retentativas; o teto agregado de campanha
permanece fora deste recorte.

Registrar o acesso aos dados de reserva antes do cálculo ou da entrega de resultado,
inclusive em cache. Falha desse registro impede o acesso. Consulta para ajuste e
exposição de intervalos compartilhados não podem ser apagadas por nova revisão ou
novo protocolo. Dados legados não ganham condição de reserva intocada retroativamente.
O registro inicial mantém `qualified_final_holdout=False`; ele não calibra critérios,
não seleciona candidatos e não substitui T20.13/T19.27.

A publicação atômica do run, manifesto e journal recebe o estado `run_recorded`.
Importação posterior do livro não faz parte dessa garantia e exige conferência própria.
Testes de migração, concorrência, idempotência, falhas, acesso e retomada provam esse
contrato de persistência; não requerem MT5 nem constituem aprovação estatística ou
operacional.
