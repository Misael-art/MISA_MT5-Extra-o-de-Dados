# Prompt — agente de Analytics: Fase 18

Continue a frente analítica existente e implemente a Fase 18 de relações entre indicadores, movimentos,
resultados dos Experts e aderência temporal. Não abandone nem reinicie o trabalho atual.

## Retomada e escopo

1. Leia AGENTS.md, RTK.md, docs/continuidade.md, ROADMAP, PLANO_DE_TRABALHO e
   docs/propostas/RELACOES_INDICADORES_SETUP.md integralmente.
2. Leia MATRIZ_REQUISITOS.md, contratos T13.6, feature store, hipóteses, producer_jobs e ledger.
3. Confira branches, worktrees, arquivos locais e ancestralidade. F13/F16/F17 têm entregas em branches separadas:
   existência não significa integração. Preserve tudo.
4. Este plano foi criado em tarefa/T18.0-plano-associacoes, em worktree separado. Se os arquivos não estiverem na
   sua branch, inspecione o diff e integre somente a entrega documental por procedimento revisável, preservando
   versões mais recentes do plano/continuidade. Não mescle todas as frentes por conveniência.
5. Instale ambiente e execute baseline conforme AGENTS. Confira espaço de temporários e display; prefira TMPDIR
   em volume com espaço e Xvfb quando necessário. Não apague arquivos de outro agente.
6. Confirme requisitos e arquivos por tarefa; uma tarefa do plano por PR. Se passo divergir do código, cumpra a
   regra de parada/registro. Não substitua silenciosamente o plano.

## Ordem

Conclua tarefas anteriores elegíveis; onde houver bloqueio externo, avance em F18 independente:
T18.1 → T18.2 → T18.3 → T18.4; T18.5 inicia após T18.2 e adiciona trilhas conforme 3/4;
T18.6 fornece serviço local/CLI/DTO; T18.7 exporta propostas; T18.9 adiciona API somente leitura.
T18.8 é responsabilidade da UI. Não dependa de MT5 conectado para pesquisa local e serviço offline.
Não dispense dependências de validação ou autorização de pilotos.

## Requisitos fundamentais

- Registry de indicadores e transformações versionadas: RSI/IFR, ATR, Bollinger, Keltner, MACD, volume/RVOL,
  tendência/retornos. Reutilize fórmulas existentes e trate indisponibilidade sem NaN/zero silencioso.
- Calcule separadamente indicador×indicador, indicador×retorno futuro e indicador×resultado do Expert.
- Preço bruto tendencial não é alvo preditivo; use retornos/horizontes e distâncias normalizadas.
- Compras, vendas, versões/presets, contratos e feeds têm escopos distintos.
- Win/loss/empate exige denominador completo; nunca correlacione só vencedores com sucesso constante.
- Custos incompletos não produzem aprovação ou expectativa líquida fabricada.
- Sem entrada/direção/resultado válidos, sinalize a relação do Expert como indisponível. Cálculo de mercado pode seguir.
- Alvos amadurecem após horizonte/saída e recebimento. Nunca usar rótulo ainda não conhecido na célula histórica.
- Revisões e disponibilidade as-of preservam passado; reconstrução histórica é identificada.
- Redundância entre indicadores não conta como confirmação independente.
- Duplas precisam agregar utilidade OOS além dos fatores isolados e do Expert original.
- Não expanda combinatória sem orçamento de busca e registro de todas as tentativas.

## Evidência e temporalidade

Reutilize e audite T13.4: treino/validação/reserva, seleção só no treino, embargo/purging, dependência temporal,
múltiplos testes e holdout consumido. Não promova resultado por p isolado, taxa de acerto ou n mínimo.
Avalie baseline, expectativa líquida, payoff, caudas, drawdown, exposição, custos e estabilidade.

Ranking congelado no treino para replay. Não escolha o melhor indicador usando toda a base para pintar o passado.
Visão retrospectiva e ranking móvel exploratório têm rótulos próprios.
Alterar futuro deve deixar prefixo publicado e seleção passada invariantes.
Quando filtro muda estado/capital/ordens seguintes, exigir reexecução completa; ledger filtrado é contrafactual.

## Banco, jobs e leitura

Preserve barras e tabelas de usuário. Reutilize feature store/ledger e crie tabelas normalizadas aditivas com _.
Projeções largas/views/exportações oferecem as colunas extras; não uma coluna física por cada par em cada ativo.
Documente tipos, índices, chave de revisão, unidade, estimator, escopo, janela, n, intervalo, ajuste de testes,
baseline/OOS, available_at, validade, origem e reason.

Use producer_jobs/write_guard e F15: incremental, cache, invalidação por revisão, checkpoint, cancelamento/retomada,
limites e logs. Meça CPU/RSS/disco e consultas; não calcule todos os pares por barra por padrão.

Primeiro serviço Python/CLI de leitura; depois API opt-in loopback com autenticação/origem quando aplicável,
allowlist de campos, limites, paginação e sem SQL arbitrário ou rotas de escrita/ordem.
Preserve DTO v1 da T13.6; publique extensão versionada e fixture serializável sem NaN.

## Estados para UI

Forneça presença do setup, evidência de utilidade, qualidade/frescor e autorização separadamente.
Não pinte coeficiente positivo como azul nem negativo como vermelho.
Azul: utilidade favorável OOS no contexto definido; amarelo: deterioração demonstrada;
vermelho: incompatibilidade/efeito adverso com suporte; cinza: insuficiente/indefinido.
Ausência de relação detectada não prova que o Expert deixou de funcionar.
Defina critérios/limiares no treino, explique motivos; não invente score agregado de “confiança”.

## Candidatos a presets

Manifesto versionado, hashes de Expert/preset/dataset, regras/limiares, features, treino/OOS, custos, tentativas,
métricas, validade e limitações. Proposta para pesquisa/otimização, nunca aplicação automática.
Sem aumentar risco, reescrever preset vigente, contornar Safety ou mudar bateria.
Modo sombra/piloto e publicação MQL5 obedecem dependências próprias. Conta real e operações não estão autorizadas.

## Coordenação com UI

Congele contrato/fixture em T18.1 e documente exemplos válidos/insuficientes/stale.
UI trabalha em worktree próprio e não recalcula métricas. Combine propriedade de presentation/cli/plano antes de editar.
Inspecione integração final serviço→DTO→tela/exportação; não entregue somente módulos desconectados.

## Ciclo e aceite

Implemente → testes conhecidos/armadilhas temporais/ruído/revisão/recuperação → revisão do diff →
medições → regressão e doctor conforme AGENTS → documentação e checkpoint → próxima tarefa elegível.

Nenhum teste desativado para passar; nenhuma evidência real inventada.
Sem múltiplos mercados/ledger/custos válidos, entregue infraestrutura e limite explícito, sem aprovar relação.
Windows real fica no checklist; não bloqueia desenvolvimento Linux.
Não faça merge, push/publicação ou operações financeiras por este prompt.

Mantenha checklist requisito→tarefa→teste/evidência→status. Cada checkpoint registra commits, arquivos não
rastreados, testes, pendências externas e passo seguinte. Não encerre com “tudo pronto” havendo itens pendentes.
