# F20 — Robustez da pesquisa, execução e ciclo de estratégias

**Pedido aceito pelo proprietário em 2026-10-17. T20.0 entrega somente documentação;
T20.1–T20.30 são tarefas planejadas.** Nenhum cálculo, Expert, preset ou banco foi
alterado por este registro. A aprovação incorpora as dezesseis sugestões da avaliação
ao desenvolvimento; não certifica capacidades existentes nem autoriza conta REAL.

Referências: [roadmap](../ROADMAP.md), [plano principal](../PLANO_DE_TRABALHO.md),
[fichas F20](PLANO_F20_ROBUSTEZ.md), [prompt único](PROMPT_CONSOLIDADO_DESENVOLVIMENTO_MT5.md),
[F19](PILARES_DADOS_OPERACAO_PRESETS.md), [épico I](EPICO_I_PESQUISA_VALIDACAO.md)
e [validação proporcional](VALIDACAO_F19.md).

## 1. Propósito e limites

Permitir construir, comparar, explicar, reproduzir e acompanhar estratégias/Experts
MT5 com cálculos conferidos e limites de evidência explícitos. Quantidade de indicadores,
testes aprovados ou lucro em um backtest não certificam utilidade futura.

F20 qualifica os pilares de dados, operação, estratégia, presets e UI. Não cria outro
backtester, store, ledger, catálogo de presets, executor, sistema visual ou gerenciador
de recursos. As fases F13/F15/F16/F17/F18/F19 e os épicos B–J conservam suas funções.
O [inventário T19.1](PLANO_F19_PILARES_PRESETS.md) recebe a extensão T20.1 no mesmo mapa.
O estado histórico de cada documento deve ser confrontado com código/PR/evidência.

Uma tarefa por PR. A ficha T20 detalha uma extensão verificável ou uma qualificação
da entrega anterior; não manda reimplementar seu pré-requisito. Se a mesma mudança
já tiver dono em uma tarefa existente, registrar esse ID como executor e T20 como
cobertura/consumo, sem executar dois PRs para a mesma alteração. Não concluir T20
apenas por existir uma implementação sem as provas adicionais exigidas.

## 2. Rastreabilidade das dezesseis sugestões aceitas

| ID | Sugestão | Donos/base existentes | Desdobramento F20 |
|---|---|---|---|
| Q01 | Certificação de cálculos e risco flutuante | T6.3/4, T13.9, F16/H, I-04/I-09 | T20.2/3/13 |
| Q02 | Dados e especificações reproduzíveis no tempo | F1/F5, T13.1/2, A-01/A-04, T19.3 | T20.4/5 |
| Q03 | Qualidade condicionando o uso dos dados | T1.8, T13.1/2, T18.1, I-08, indicadores F19 | T20.6 |
| Q04 | Compatibilidade ativo/corretora/conta | T6.5, contrato B-11, T19.2/23/29, F-11 | T20.7 |
| Q05 | Fidelidade econômica e capacidade por volume | I-07/I-08, T19.24/56/57, A-09 | T20.8/9/10 |
| Q06 | Reconciliação por evento Python × MT5 | I-06, H-01, T10.6, T16.2b, T19.29 | T20.11 |
| Q07 | Registro de tentativas e uso da reserva | T13.4, I-03/I-09, T19.27/52 | T20.12/13 |
| Q08 | Seleção com vários objetivos e compromissos | T19.28/54, PF canônico T13.9 | T20.14 |
| Q09 | Contribuição incremental de cada componente | I-02/I-05, F18, pesquisa FRP futura | T20.15 |
| Q10 | Pacote reproduzível e ciclo de evidências | B-11, T10.4, T19.25/50/55, publicação F13 | T20.16/17 |
| Q11 | Acompanhamento e diagnóstico de degradação | T13.8, I-12, J-05/J-08, monitor existente | T20.18/19 |
| Q12 | Recuperação e falhas operacionais | T12.3, D/E/F, ownership e sessões T17.28 | T20.20 |
| Q13 | Pesquisa de carteira com exposição simultânea | F12, épico F, A-11, T19.48 | T20.21, etapa posterior |
| Q14 | UI orientada à investigação e decisão | F17, T13.6, T18.8, T19.30–34 | T20.22/23 |
| Q15 | Orçamento, retomada e isolamento de tarefas | T11.5/6, F15, runner/cache/sessões | T20.24/25 |
| Q16 | Extensibilidade, segurança e restauração | SDK/B-09/B-11, T9.2/3, J-09, F15 | T20.26–29 |

T20.1 reconcilia cobertura e T20.30 aceita um percurso completo do recorte entregue.
Os IDs Q são requisitos, não notas de maturidade nem probabilidades de sucesso.

## 3. Dados e cálculos certificados

### 3.1 Referência matemática independente — Q01

Manter pequenos casos com entradas, contas e saídas esperadas revisáveis, independentes
do algoritmo testado. Cobrir pontos/ticks/preço, unidade de volume, moedas, PnL bruto e
líquido, comissão por execução, bid/ask, spread, slippage, swap suportado, risco, margem,
arredondamento, parcelas, saldo e agrupamento trade/ciclo. Não usar o próprio motor como
único oráculo; conferir identidades e conciliação com referências nativas pertinentes.

Identificar fórmula/variante, precisão, erro tolerado e fundamento. Tolerância não pode
ocultar direção errada, volume inválido ou evento faltante. PF zero, ausente e indefinido
seguem T13.9; não introduzir infinito, teto ou fórmula alternativa de PF.

Acrescentar patrimônio marcado a mercado em instantes definidos, distinguindo saldo,
resultado flutuante, custos, moeda de conta, pico e drawdown. Declarar marcação por bid/ask
e frequência: uma curva amostrada em barras não certifica o extremo ocorrido entre elas.
Preservar a métrica histórica de operações encerradas com rótulo inequívoco e nova métrica
aditiva; não reescrever resultados antigos silenciosamente. Débitos/créditos externos
precisam de tratamento explícito ao avaliar retorno e drawdown, sem virar lucro do robô.

### 3.2 Versões históricas e cobertura — Q02/Q03

Manifesto imutável identifica fonte, corretora/servidor sem dados pessoais, símbolo/contrato,
timeframe, período, fuso/sessão, revisões, hash de conteúdo canônico, versão de derivação e
limitações. Snapshot usa exportação consistente ou mecanismo de snapshot do backend;
hash do arquivo de um banco em gravação não comprova consistência. Não exigir cópia integral
da base a cada execução: usar referências verificáveis e retenção compatível com F15.

Correção gera revisão nova; experimento antigo conserva a referência anterior ou declara
que a reprodução não está disponível. `observed_at`, `available_at` e vigência têm funções
distintas. Dados externos/notícias revisadas não podem aparecer antes de sua disponibilidade.
Alinhamento de ativos e timeframes usa consulta causal; barra superior ainda aberta,
pivô não confirmado e rótulo futuro não alimentam decisões passadas.

Especificações do símbolo têm vigência e fonte: preço/tick, volume mínimo/passo/máximo,
moedas, sessões, margem e propriedades relevantes. Não atribuir retrospectivamente o
valor atual como fato histórico conhecido. Ajustes de ações, universo incluindo ativos
descontinuados, vencimentos e rolagens de futuros recebem política explícita; série
contínua de estudo não substitui preço do contrato executável. Na falta de eventos ou
especificação histórica, apresentar a limitação, sem inventar dados nem contratar fonte paga.

Qualidade é dimensão separada do resultado financeiro: faltas, duplicatas, timestamps,
volume real/ticks, bid/ask, book e origem observada/gerada. Ausência é desconhecida, não zero.
Feature de agressão/CVD/footprint exige fonte suficiente; snapshots espaçados do book não
reconstroem fila. O gate bloqueia somente a capacidade incompatível e informa o motivo.
Model4 por si só não prova cobertura integral de ticks reais: registrar substituições
quando demonstráveis e `desconhecida` quando o terminal/relatório não fornecer evidência.

## 4. Execução fiel e reconciliável

### 4.1 Capacidades e economia — Q04/Q05

Matriz por Expert/versão, módulos, ativo, corretora, conta netting/hedging e ambiente.
Registrar tipos de ordem/filling, lote, stops/freeze, margem, moeda, sessão e combinações
suportadas. Separar `suportado pelo código`, `simulado`, `validado no Tester`, `observado em
DEMO`, `não avaliado` e `incompatível`; evidência não se transfere entre contas/ativos.

Ampliar I-07 no motor canônico: exposição simultânea, margem comprometida/reservada por
política, tratamento de pendentes, preços e câmbio conhecidos no instante, perdas flutuantes
e condições de liquidação suportadas. Não recalcular o passado consultando margem atual.
`OrderCalcMargin` isolado não considera posições/pendentes existentes; não somar margens
ingenuamente em netting/hedging. `OrderCheck` não é confirmação de execução. Valores da
corretora servem como referência contextual; modelo histórico e limites ficam declarados.

Calibrar custos por ativo/fonte/período e medir erro remanescente. Testar gaps, spreads
adversos, atraso, rejeição, execução parcial e ordens simultâneas com mecanismos suportados.
Sem evidência de fila/liquidez, não prometer fill por mero toque nem simulação de impacto
precisa. Separar fechamento parcial voluntário de preenchimento parcial pela corretora.

Curva de capacidade avalia volumes e cenários de custo/participação, sempre respeitando
mínimo/passo/margem. Hipóteses de impacto são cenários, não capacidade certificada. Comparar
retorno líquido, patrimônio/drawdown, exposição e elegibilidade para diferentes volumes.

### 4.2 Eventos e recuperação — Q06/Q12

Rastrear intenção, filtros, plano, decisão de risco, solicitação, resposta, ordem, negócio,
posição, etapa de parcial e ciclo, com IDs, timestamps, versão e motivo. Não presumir
relação 1:1 nem ordem fixa de chegada entre solicitação e transações MT5. Processamento
leve e reconciliação com estado/histórico nativo impedem duplicação de efeitos; não prometer
entrega exatamente uma vez pelo servidor. Evento sem associação permanece pendente/explicado.

Comparador relata a primeira divergência material e seus valores, distinguindo lógica,
fonte, relógio, custo, arredondamento e aproximação intrabarra. Preservar os seis pares
atuais como regressão, com tolerâncias previamente justificadas por campo.

Injetar queda de conexão/ponte, reinício do Expert/terminal/app, resposta tardia, evento
duplicado/fora de ordem, dado vencido e falha de persistência. Ao retomar, reconciliar
ownership/ordens/posições antes de novas ações; não reenviar por timeout sem verificar o
resultado anterior. Proteger saldo após parcial e evitar etapa repetida. Separar suspensão
de entradas, cancelamento de pendentes próprias e liquidação de posições próprias: política
e autorização aplicáveis são explícitas. Não tocar sessões/arquivos/posições de terceiros.

## 5. Pesquisa que mede sua própria incerteza

### 5.1 Tentativas e protocolo — Q07

Registro único de experimentos referencia composição, fonte, código, folds, semente, custos,
capital, objetivo, orçamento e protocolo congelado. Contar alternativas manuais/automáticas,
inclusive filtros, indicadores, horários e regras Ultra, e conservar falhas/descartes com
motivo. Repetição técnica idêntica referencia a tentativa original sem apagar falhas nem
inflar amostra; uma nova alternativa de pesquisa permanece tentativa mesmo quando usa cache.

Registrar consultas à reserva e decisões posteriores. Se o resultado reservado orienta
ajustes, sua função passa a validação e deixa de ser teste final intocado; novo protocolo
precisa de amostra futura/independente válida. Não tentar resolver isso recortando novamente
o mesmo histórico já examinado. Janelas cronológicas, purga/embargo quando cabíveis e
dependência entre ciclos/rótulos têm justificativa, não tamanho universal arbitrário.

Revisar matematicamente os critérios I-04/I-09/I-11: preservar unidade/dependência na
reamostragem; não exigir que todo Monte Carlo por ciclo tenha drawdown maior que o de
operações. Em ruído, avaliar taxa de falsos positivos com intervalo e poder em efeito
conhecido; não prometer zero aprovações em toda realização aleatória. DSR/PBO e métodos
afins trazem hipóteses/limitações e dados necessários, sem serem percentuais de garantia.
A revisão documental não muda a bateria em execução: alteração de critério é versionada,
testada e rastreada em I-13, sem afrouxar proteção para fazer o teste passar.

### 5.2 Objetivos e contribuição — Q08/Q09

Default, Manual, Defensivo, Maximalista (`FatorLucro`) e Ultra conservam a semântica F19.
Não criar sexta categoria. Comparar compromissos de retorno líquido, risco flutuante,
atividade/amostra, estabilidade, custos e complexidade entre candidatos elegíveis.
Fronteira de Pareto é apoio visual; não trocar silenciosamente o objetivo PF por score.
Restrições, desempates e penalização de complexidade são fixados no treino/protocolo.
Estratégia sem operar e PF indefinido não viram vencedores por conveniência numérica.

Ultra compara sua política causal inteira com Default e soluções simples, incluindo custo
das transições, limites de regras e amostra por contexto. Não pressupor superioridade nem
costurar máximos conhecidos depois do fato. Perfil Defensivo declara frequência da marcação
e limitações de risco; não usar apenas saldo encerrado para alegar menor risco patrimonial.

Ablação mede remoção/adição/substituição de sinal, filtro, entrada e gestão usando protocolo,
dados, risco e custos comparáveis. Acrescentar controles/baselines simples pertinentes e
sensibilidade de parâmetros; isolar, quando possível, mérito do sinal e da execução.
Mudança que altera ocupação/capital/sinais exige backtest completo, não edição de PnL.
Contribuição incremental é associação experimental na amostra, não prova de causalidade.
FRP permanece ESTUDO FUTURO/P3; consome essa infraestrutura depois de consolidada.

## 6. Pacotes, estados e acompanhamento

### 6.1 Identidade e ciclo de evidências — Q10

Estender o pacote F19, sem catálogo paralelo: preset legível e canônico, composição,
fontes e binários identificados, versões/framework/Safety/contratos, build do MT5,
dependências efetivas, dados/especificações, protocolo/folds/sementes e hashes dos artefatos.
Exportação respeita licença e pode referenciar dados externos indistribuíveis, declarando
o necessário à reprodução. Credenciais e identificadores pessoais ficam fora do pacote.

Estados por dimensão: integridade do pacote, compatibilidade, cálculo, estatística,
paridade, replay funcional, DEMO ao vivo e plataforma validada. Cada evidência aponta
escopo/versão/resultado/data; não resumir tudo em uma nota de confiança nem transformar
aprovação estatística em permissão de operar. Mudança de dado/código/política invalida os
consumidores afetados pelo grafo de dependências; mudança só de rótulo preserva identidade
conforme a convenção Manual. Histórico antigo permanece legível, sem apagar reprovações.

### 6.2 Observação futura e degradação — Q11

Modo de observação registra intenções e bloqueios com os dados disponíveis no momento,
sem enviar ordens. Se houver PnL virtual, identificar modelo de execução e custos como
simulação; não atribuir fills virtuais à corretora nem ao extrato DEMO.

Comparar posteriormente decisões e execuções DEMO a expectativas congeladas, distinguindo
falha operacional/dado vencido, custo, mudança de distribuição e variação amostral. Usar
limites/persistência de alerta definidos antes, cobertura/amostra e controle de alarmes
repetidos. Drift de uma feature isolada não prova perda de vantagem. Diagnóstico aponta
eventos e incerteza; reotimização/substituição de preset não ocorre silenciosamente.

### 6.3 Carteira, em segundo momento — Q13

Pesquisar curvas síncronas com patrimônio flutuante, capital e margem compartilhados,
conversão monetária, sinais concorrentes, exposição por ativo/moeda/fator e dependência
das perdas em condições adversas. Somar resultados independentes não demonstra execução
conjunta possível. Testar pesos/limites em protocolo temporal sem realocar com o futuro.
T20.21 é um piloto de pesquisa de carteira; não implementa todo o guardião nem adia
F-04/F-05 e demais travas necessárias às múltiplas ordens da F19.

## 7. UI, performance e operação da plataforma

### 7.1 Percurso e investigação — Q14

Detalhamento e prioridade: [UI-A01–A10 atuais, UI-P01/P02 posteriores e UI-X01
experimental](UI_ORGANIZACAO_GRAFICOS_PRIORIDADES.md). Não altera o número de fichas F20.
Os requisitos atuais entram junto de cada capacidade F19/F20; renderizador especializado
não é dependência da UI atual nem do primeiro preset. Permanecem Tkinter/ttk/Matplotlib.

Hipótese → dados/qualidade → composição → protocolo/experimento → comparação → preset →
validação → acompanhamento. Usar navegação atual e apresentação progressiva básica/avançada,
sem exigir preenchimento de módulos opcionais. Mostrar unidades, incompatibilidades,
orçamento de busca, andamento/cancelamento, amostra, limitações e próximos passos.

Gráficos vinculam preço, entradas, parcelas, stops/alvos, exposição e patrimônio no mesmo
tempo; seleção revela regra/versão/contexto/motivo e evento fonte. Alternância saldo/equity
e comparação de versões declaram denominador, moeda e frequência. Preservar pontos reais,
lacunas, zero e ausência; agregação visual não altera extremos nem métricas. Contrastes,
teclado, foco, fonte e DPI seguem F17/F19, inclusive 900×620 e janela estreita. Widgets
consomem DTOs/serviços reais; não recalculam estatística nem fabricam dados em produção.

### 7.2 Orquestração — Q15

Ampliar fila retomável T11.5 e gestão F15: orçamento de alternativas, CPU/RAM/disco/slots,
pré-checagem de temporários, progresso e cancelamento cooperativo. Estados de tarefa têm
checkpoint e dono; interrupção não publica resultado parcial como completo. Filas e cache
usam código/dados/especificações/protocolo/seed/custo completos na chave.

Isolar terminais/Wine/perfis/artefatos quando necessário e verificar que encerramento atinge
somente processos próprios. Não paralelizar cenários no mesmo terminal sem suporte provado.
Retenção protege artefatos referenciados por experimentos; coleta de lixo é reversível/opt-in
conforme F15. Medir tempo/memória/I/O/UI em cenários comparáveis; não trocar backend nem
introduzir nuvem paga sem necessidade medida e autorização aplicável.

### 7.3 Contratos de extensão, segurança e restauração — Q16

Cada módulo declara versão, tipos/unidades/limites, aquecimento, disponibilidade, estado,
capacidades, compatibilidade e cenários de referência. Conformidade deve testar efeito e
legado, não apenas presença de atributos. Registrar módulos no SDK/Composer existente;
não carregar código arbitrário pelo conteúdo de um preset ou criar outro executor.

Experts/fontes de terceiros têm origem e hashes, revisão de includes/dependências e
permissões explícitas para DLL, rede e arquivos. Compilar não equivale a confiar/executar.
Preservar ponte somente leitura, segredos no .env, auditoria e privilégios mínimos. Não
executar automaticamente artefato desconhecido no terminal pessoal. Respeitar licenças.

Estender backups F15 para pacote coerente de pesquisa: banco/snapshot, presets, manifesto,
experimentos e referências de evidências. Restaurar em ambiente isolado, validar integridade,
schema antigo, vínculos e retomada sem enviar ordens. Falha de cópia/restauração conserva a
origem; credenciais não são incluídas em pacotes portáveis. Registrar duração e perda máxima
de dados observada, sem prometer recuperação instantânea.

Qualificar release/instaladores idempotentes e leitores de relatório MT5 por variantes de
build/idioma suportadas: erro de parsing vira falha explícita, não resultado zero. Testar
Linux/Wine e caminhos Windows simulados; Windows físico continua pendência delimitada até
execução real. Dependências efetivas ficam registradas para reprodução, sem fixação de
versões arbitrária ou alteração de runtime nesta entrega documental.

## 8. Portões de aceite e ordem de investimento

| Marco | Evidência mínima | Permite avançar |
|---|---|---|
| M20-A | Cobertura reconciliada, contas de referência, patrimônio e fonte/especificação identificáveis | Comparar composições com limitações explícitas |
| M20-B | Capacidades, margem/custos e divergências por evento conferidas para o recorte | Qualificar fidelidade operacional daquele recorte |
| M20-C | Registro de tentativas, reserva protegida e protocolo auditado | Seleção automática e pesquisa de Ultra, sem promessa de ganho |
| M20-D | Pacote e invalidação, UI ligada a serviços e replay nativo reconciliado | Primeiro ciclo reproduzível até preset validado |
| M20-E | Observação/DEMO, falhas/retomada, recursos e restauração comprovados | Acompanhamento e recuperação no ambiente ensaiado |
| M20-F | Pesquisa síncrona da carteira sobre infraestrutura e guardas integrados | Avaliar combinações no segundo momento |

Marcos são por capacidade e ambiente, sem versão comercial/data prometida. O piloto F19
Default → Manual → backtest completo → preset → replay/UI não espera todos os indicadores,
Ultra, book, carteira ou FRP. Antes de concluir alegação de risco defensivo, aplicar Q01;
antes de seleção automática, Q07; antes de declarar fidelidade operacional, Q04/Q05/Q06.
Protótipos intermediários conservam o rótulo e não pulam os gates já existentes.

Agentes autônomos que criam/promovem estratégias, aprendizado por reforço, reotimização
contínua, alta frequência e expansão indiscriminada de indicadores permanecem posteriores/
P3. A aceitação das sugestões não os promove ao caminho crítico nem inicia FRP agora.

## 9. Validação e fontes técnicas

Aplicar a [matriz F19](VALIDACAO_F19.md): contas conhecidas e testes estruturais para
cálculos/contratos; protocolo completo para inferência; UI real para apresentação; Expert
em replay MT5 visual/DEMO para comportamento operacional. Mercado fechado permite replay
com período curto e eventos suficientes; isso não é amostra OOS nem execução DEMO ao vivo.
Rede, condições de preenchimento ao vivo e permissões específicas conservam ensaio DEMO
próprio quando necessário. Nunca ensaiar conta REAL por este plano.

Fontes primárias consultadas em 2026-10-17, como fundamento, sem substituir a medição local:

- [MT5: modos de teste, ticks reais e limitações](https://www.mql5.com/en/docs/runtime/testing).
- [MT5: transações e ordem de eventos](https://www.mql5.com/en/docs/event_handlers/ontradetransaction).
- [MT5: cálculo de margem isolado](https://www.mql5.com/en/docs/trading/ordercalcmargin)
  e [checagem de solicitação](https://www.mql5.com/en/docs/trading/ordercheck).
- [Bailey e López de Prado: Deflated Sharpe Ratio](https://www.davidhbailey.com/dhbpapers/deflated-sharpe.pdf).

As fontes justificam cuidados e hipóteses; não comprovam implementação ou rentabilidade.
