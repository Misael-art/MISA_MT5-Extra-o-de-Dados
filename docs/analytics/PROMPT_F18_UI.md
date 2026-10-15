# Prompt — agente de UI: Relações e aderência (T18.8)

Evolua a interface do Laboratório para apresentar relações entre indicadores e a aderência temporal dos setups.
Reutilize integralmente o sistema visual F17; não reinicie a modernização nem mude arquitetura/toolkit.

## Início

Leia AGENTS.md/RTK.md, continuidade, plano/roadmap e docs/propostas/RELACOES_INDICADORES_SETUP.md.
Leia o plano INTERFACE_VISUAL.md da branch F17 e o contrato T13.6. Verifique quais branches realmente foram
integradas, inclusive T17.6: não presuma que nomes de tarefas significam aceite/merge.
Trabalhe em worktree isolado; preserve outras frentes, uma tarefa por PR.
Prepare ambiente/baseline conforme AGENTS e confira temporários/display.

Sua responsabilidade é T18.8 (apresentação). Analytics entrega T18.1 fixture/DTO e T18.6 serviço.
Se o contrato não existir, prepare a proposta visual e fixture explicitamente provisória com o agente analítico;
não cristalize schema concorrente. Integração real e aceite final exigem serviço.
Não dependa de publicador MQL5 para a tela de pesquisa offline.

## Experiência

Subaba “Relações e aderência” no Laboratório, com vistas:
- Matriz: indicador×indicador e indicador×alvo, método/escala explícitos.
- Linha do tempo: começo da base à esquerda e fim atualizado à direita.
- Ganhos e perdas: distribuição/efeito por direção e contexto, denominadores e empates.
- Convergências: fatores isolados versus dupla e baseline, evidência OOS.
- Candidatos: propostas para pesquisa/preset, nunca botão de aplicar ao Expert.

Filtros claros: ativo/feed/contrato, Expert/versão/preset, timeframe, direção, alvo/horizonte, método, período,
janela/passo e modo retrospectivo/replay OOS. Recolha filtros avançados sem esconder contexto ativo.

Indicadores/regra nas linhas e tempo nas colunas. Zoom/pan, seleção de intervalo e célula, detalhe persistente,
exportação e navegação por teclado. Mostrar intervalos de treino/validação/OOS e aquecimento.
Ordenação exibe métrica e período de ranking. Padrão de replay é ranking congelado no treino.
Não costure retrospectivamente o “melhor de cada janela” como uma estratégia realizável.

## Cor e significado

Duas escalas independentes:
1. Correlação assinada: divergente -1…+1, zero central, legenda e valor/método; sinal não significa lucro.
2. Aderência:
   azul vibrante = utilidade favorável OOS segundo critérios do produtor;
   amarelo opaco = deterioração demonstrada;
   vermelho opaco = incompatibilidade/efeito adverso com evidência;
   cinza/padrão + texto = sem dados, insuficiente, indefinido ou aquecimento.

Sem evidência detectada não equivale a prova de inutilidade. Distinga inconclusivo de adverso.
Dados vencidos têm STALE, exploratórios têm selo próprio. Não reutilize azul de aprovação para mock/exploração.
“Setup presente/ativo” é texto/ícone separado; não significa autorização de operar.
Não invente score ou estado no widget e não recalcule significância/aderência.

Preserve as preferências azul/amarelo/vermelho do usuário com saturação moderada em áreas grandes e cores fortes
nos destaques. Fontes têm contraste medido nos fundos; amarelo claro pede texto escuro. Nenhuma informação só por cor.

## Legibilidade e visualização

- Tokens e componentes F17, temas claro/escuro, tipografia de dados, espaçamento e superfícies consistentes.
- Legenda amigável fixa, amostras de cor com nomes e definição curta; não depender de tooltip.
- Heatmap mostra ausência de dados diferente de zero; não interpolar lacunas nem pintar antes da disponibilidade.
- Intervalo, n de pares/sessões, método, unidade, baseline e estado acessíveis em detalhe por clique/teclado.
- Tooltip complementar: feature/parâmetros, alvo/horizonte, compra/venda, IC, ajuste de testes/tentativas,
  custos, janela, fonte/revisão, data de disponibilidade/validade e reason.
- Correlação simétrica só quando método/defasagem forem simétricos.
- Ganhos/perdas usam escalas comparáveis e identificação da população.
- Legendas não cobrem dados; seleção de séries, painéis alinhados e rolagem evitam poluição.
- Gráficos respondem ao tamanho da janela; evitar miniaturas ilegíveis para caber todas as linhas.
- Muitos dados: paginação/virtualização e agregação visual explícita; exportar resolução original.
- Dados sintéticos têm selo permanente; não apresentá-los como descoberta do ativo real.

## Estados e interação

Carregando, vazio, sem ledger, custo incompleto, dados parciais, erro recuperável, cancelamento, stale e insuficiente.
Feedback visível e razões compreensíveis. Sem métricas ausentes convertidas em zero.
Resultados antigos após mudar filtros ficam identificados e não sobrepõem silenciosamente consulta mais recente.
Preserve ordenação, seleção e zoom quando fizer sentido; limpar contexto quando mudá-lo evitar comparação inválida.

Threads/filas e gerenciamento de figuras usam padrões existentes; Tk só na thread principal.
Não tocar nas correções de GC/vida de imagens. Debounce de resize, descarte de callbacks e cancelamento seguro.
UI consome DTO/serviço, não consulta tabelas nem recalcula análise.

## Verificação e refinamento

Teste fixtures de efeito positivo, negativo útil, efeito adverso, deterioração, não linear, ausente, constante,
baixa amostra, classes únicas, muitas linhas/janelas, nomes longos, erros e stale.
Verifique 1280×800, 1920×1080, DPI alto, temas, resize, teclado e fonte real.
Capture antes/depois, inspecione ao tamanho real e refine até não haver cortes, legendas confusas ou sobreposição.
Não conclua com screenshot apenas: exercite seleção, filtros, zoom, exportação e paridade com DTO.

Coordene apresentação com Analytics; não altere cálculos, schema, presets, permissões ou decisões financeiras.
Execute testes e doctor conforme AGENTS; Windows real é pendência declarada.
Documente evidências, contratos, limites e integração. Não declare F18 implementada só porque uma fixture renderiza.
Sem merge/publicação ou operação financeira por este prompt.
