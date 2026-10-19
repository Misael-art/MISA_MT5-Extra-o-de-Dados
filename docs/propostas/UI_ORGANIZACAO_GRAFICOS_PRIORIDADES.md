# UI — organização, gráficos e prioridades de implementação

**Registro solicitado pelo proprietário em 2026-10-17. Somente documentação.**
As sugestões específicas de UI complementam Q14/F20 e os requisitos visuais F19.
Não significam interface implementada, novo motor gráfico aprovado ou ensaio MT5 realizado.

Referências: [F17 e seus limites](INTERFACE_VISUAL.md),
[composição/UI F19](PILARES_DADOS_OPERACAO_PRESETS.md),
[plano F19](PLANO_F19_PILARES_PRESETS.md),
[robustez Q14](ROBUSTEZ_PESQUISA_EXECUCAO.md),
[plano F20](PLANO_F20_ROBUSTEZ.md), [roadmap](../ROADMAP.md) e
[validação](VALIDACAO_F19.md).

Execução pelo [prompt único F19/F20/UI/MT5](PROMPT_CONSOLIDADO_DESENVOLVIMENTO_MT5.md),
sem concatenar orientações parciais do chat ou dos prompts antigos.

## 1. Decisão de escopo

**AGORA/P1** significa requisito da capacidade que já será implementada em F19/F20,
quando seu serviço/dado estiver integrado. Não significa implementar todas as telas
antes do primeiro preset. **POSTERIOR/P2** significa refinamento após o percurso básico.
**EXPERIMENTO/P3** significa proposta técnica sem início/adoção nesta fila.

Permanecem Tkinter/ttk, Matplotlib, tokens, componentes e navegação existentes. Preservar
as abas principais e ações alcançáveis; os modos abaixo organizam o conteúdo do Laboratório,
sem apagar funções nem criar outra aplicação. Ganho visual deve acompanhar clareza,
correção dos dados, responsividade e desempenho medido.

Os IDs UI-A/UI-P/UI-X são requisitos e cobertura; não acrescentam tarefas concorrentes.
T19.30/31/32/33/34, T20.22/23 e seus serviços continuam donos. Uma alteração aparece em
um só PR da tarefa correspondente, com vínculo ao requisito. Não renumerar as 57 fichas
F19 nem as 30 F20. O registro é adendo documental T20.0, sem reabrir a entrega histórica F17.

## 2. O que entra agora no contexto das implementações

| ID | Requisito AGORA/P1 | Dono e dependência | Aceite principal |
|---|---|---|---|
| UI-A01 | Modos Analisar, Compor, Validar e comparar | T20.22 sobre telas existentes; serviço de cada ação | Percurso descobrível, funções antigas alcançáveis, sem duplicar abas/ações/contexto |
| UI-A02 | Contexto compacto e gráfico com área principal | T20.22; T19.30/33/34 | Estratégia/ativo/TF/preset/período e origem/modalidade claros; painéis recolhem sem esconder informação essencial |
| UI-A03 | Editor operacional por grupos e resumos | T19.30, capacidades T19.2/17–23/51/56/57 | Entradas/orientação, lotes/parciais, alvos, stops/proteção, filtros com unidades e conflitos explícitos |
| UI-A04 | Hierarquia visual e legibilidade consistentes | T19.34/T20.22, componentes/tokens F17 | Dois temas, tipografia/contraste/espaçamentos coerentes, ação principal identificável, estados financeiros e de evidência distintos |
| UI-A05 | Gráficos essenciais e catálogo por capacidade | T20.23, T19.31 e donos F13/F16/F18/T20 | Curvas/eventos iguais aos DTOs; gráfico indisponível explica o dado/serviço ausente; sem cálculo financeiro no widget |
| UI-A06 | Seleção coordenada e inspetor temporal | T20.23 após eventos T20.11 e dados T20.3 | Clique na tabela/gráfico aponta a mesma operação/regra/instante; cursores/zoom compatíveis; sem causa inventada |
| UI-A07 | Tabelas legíveis e insights com resumo e detalhe | T20.22 sobre F16/F18, T19.32 quando aplicável | Unidades, alinhamento, ordenação e amostra; seleção da linha distinta de habilitar regra; texto completo acessível |
| UI-A08 | Estado de edição, continuidade e recuperação de contexto | T19.26/30/32 e T20.22, fila T20.24 se usada | Resultado associado à configuração efetivamente calculada; seleção preservada, progresso/cancelamento e retorno claro ao padrão visual |
| UI-A09 | Adaptação, acessibilidade e interação fluida | T19.34/T20.22/23 | Teclado/foco/rolagem, matriz de tamanhos/DPI, tarefas fora da thread visual e descarte de resposta antiga |
| UI-A10 | Uma tela de referência e aceite do percurso simples | Layout/contexto T20.22; gráficos/inspetor T20.23; aceite T20.30 | Validar um percurso antes da expansão; provas de UI/DTO/replay distintas, sem exigir todos os indicadores/Ultra |

Concluir o dono quando seu escopo e dependências estiverem cobertos. Um requisito já
atendido pelo código atual exige comprovação, não uma segunda implementação.

### 2.1 Modos de trabalho e disposição — UI-A01/UI-A02

- **Analisar:** resultados, horários, duração, custos, indicadores e risco.
- **Compor:** editar módulos e inspecionar a composição efetiva; edição não opera a conta.
- **Validar e comparar:** protocolo, execução/andamento, resultados, evidências e presets.

Primeiro mapear subvisões atuais para esses modos. Evitar transportar ou duplicar
funcionalidades já presentes em Análise, Robôs e Recursos. Contexto selecionado é único
por percurso; modos não compartilham resultado de outro ativo/TF/revisão por acidente.

| Região | Conteúdo | Em janela estreita/baixa |
|---|---|---|
| Cabeçalho compacto | Estratégia, ativo, TF, preset e período; ação principal da etapa | Reflow sem perder contexto; reusar Ativo / TF existente |
| Faixa de contexto | Fonte, modalidade, qualidade e estado da evidência | Resumo explícito e detalhe acessível, sem ocultar limitação material |
| Painel de composição | Grupos operacionais e resumo | Recolhível ou visão própria; não forçar três colunas |
| Área analítica central | Um gráfico principal, complementos escolhidos | Altura útil e ampliar/restaurar; não virar faixa ilegível |
| Inspetor sob demanda | Operação, regra, parâmetros/contexto e evento fonte | Abrir ao selecionar, com caminho claro de retorno |
| Área inferior | Tabela de operações/eventos/resultados | Recolhível/rolável, sem competir permanentemente com o gráfico |

A largura deriva de fonte/DPI/espaço disponível; não fixar dimensões que só funcionem
no monitor do desenvolvedor. Cabeçalho não pode consumir a altura necessária aos resultados.

### 2.2 Editor, estados e tabelas — UI-A03/UI-A07/UI-A08

Editor: **Entrada e orientação → Lotes e parciais → Alvos → Stops e proteção → Filtros**.
Estratégia/sinal é contexto independente da política operacional. Grupos fechados mostram
resumo legível da configuração, com unidades, parâmetros ativos e limitações. Orientação
invertida é experimental e desligada por padrão. Lotes não se confundem com número de ciclos/dia.

Usar seleção única quando as alternativas forem exclusivas. Políticas coexistentes,
como stop inicial/break-even/trailing, mostram ordem, ativação e prioridade efetivas.
Saídas temporais são saídas; janelas/dias/cotas de entrada são filtros. Composição inviável
aponta motivo e ação possível, sem aumentar lote/risco ou desabilitar proteção para viabilizá-la.

Preservar configuração em edição separada daquela que produziu o resultado exibido.
Mostrar alteração pendente; ação Aplicar/Simular/Executar experimento tem efeito claro,
sem recalcular tudo a cada tecla nem iniciar ordens. Manter resultado anterior identificado
durante a execução, progresso/cancelamento e recuperação do rascunho quando suportada.

Preservar seleção/filtros/colunas na atualização e, no escopo suportado, preferências
básicas por contexto. Não copiar estado entre símbolos/TFs/presets incompatíveis; oferecer
restaurar disposição padrão sem restaurar parâmetros financeiros ou substituir preset.
Persistência usa a infraestrutura existente e regras DEFAULTS/example/fallback do AGENTS.

Tabelas: números à direita, unidade no cabeçalho, precisão adequada, ordenação visível,
colunas necessárias ao contexto e detalhe para conteúdo extenso. Insight mostra condição,
efeito, n, incerteza/estado e comparação de referência; o texto e método completos ficam
acessíveis no inspetor. Checkbox de uso não é seleção de linha. Resultado exploratório
não ganha aparência de candidato validado; ausência, zero e indefinido são distintos.

### 2.3 Gráficos e interações — UI-A05/UI-A06

| Visualização | Prioridade no percurso | Fonte e condição de disponibilidade |
|---|---|---|
| Preço/candles com entradas, parciais, stops/alvos e gradiente | Essencial ao inspecionar operação; apresentar só políticas suportadas | Barras/ticks e eventos identificados; T20.11/T19.29/57 conforme módulo |
| Patrimônio e saldo, drawdown em painel temporal associado | Essencial quando as métricas forem qualificadas | T20.3; sem intratrade suficiente rotular risco/frequência/ausência, sem fabricar equity |
| Calendário e mapa dia × horário | Entra com análise temporal existente/entregue | F16/calendário; referência entrada/saída, fuso, cobertura, n e amostra pequena explícitos |
| Distribuição de resultado e duração | Entra quando sua análise for entregue | F16/T19.14/31; unidade, população e incerteza compatíveis com o serviço |
| Sensibilidade de parâmetros | Condicional à entrega da pesquisa correspondente, não pré-requisito do Manual | I-05/T20.15 e protocolo, custos/amostra/experimentos identificados |
| Risco × retorno/compromissos entre presets | Condicional à seleção/comparação implementada | T19.28/T20.14; candidatos elegíveis, sem score PF oculto ou superioridade presumida de Ultra |
| Matriz de relações entre indicadores | Condicional ao DTO/serviço real F18 | Fixture continua identificada até integração; correlação não representa segurança operacional |

O catálogo não obriga mostrar sete gráficos simultaneamente nem implementar seus motores
na tarefa visual. No primeiro Manual, usar preço/eventos e o resultado/risco realmente
disponível. Gráficos condicionais entram junto de seus serviços, sem bloquear o piloto.

Interações essenciais: cursor temporal compartilhado entre séries alinháveis, seleção
bidirecional tabela/gráfico, zoom/deslocamento coordenados quando comparáveis, detalhes de
evento/regra/versão/contexto, ampliar/restaurar e exportar o que foi apresentado. Recurso
sem dado/sem suporte tem motivo, não callback vazio. Oferecer alternativa por teclado/tabela
para detalhes importantes. Campo sem registro não recebe explicação inferida.

Eixo mostra unidade/moeda/fuso e referência temporal; comparar saldo/equity e versões
com capital/risco/período declarados. Seleção exploratória de intervalo não redefine o
protocolo/OOS nem gera/aplica preset automaticamente. Não apresentar resultado de liquidação
como se estivesse disponível na entrada. IDs/tempo/fonte sustentam a associação de eventos.

Preservar pontos reais, extremos, gaps e dados ausentes. Redução visual mantém os dados
e métricas originais; hover informa resolução/agregação quando relevante. Não suavizar
para esconder perdas, criar preço intermediário negociável ou falsear drawdown. Desenho
de stop no gráfico não comprova acionamento no MT5.

### 2.4 Aparência, adaptação e desempenho — UI-A04/UI-A09

Usar paleta, fontes, escala e componentes F17; não criar outro tema. Diminuir bordas
competindo com o conteúdo, padronizar espaçamentos, alinhar valores e destacar uma ação
principal por etapa. Ganho/perda, direção, qualidade/compatibilidade e validação têm
papéis distintos; preservar a semântica acessível azul/laranja e estados já definidos.
Texto/ícone complementam cor. A apresentação confortável e legível é a referência inicial.

Conferir 1920×1080, 1280×800, 900×800, 900×620 e 820×800; DPI96/144 e escalas 125/150%
quando disponíveis, dois temas, fonte ampliada, teclado, listas extensas e números grandes.
Painéis recolhidos não impedem atingir controles nem perdem seleção. Não somar fatores
de escala duas vezes. Windows simulado não substitui inspeção Windows física.

Medir interação/redesenho/resize/hover, tempo/memória e resposta da UI em cenários
reproduzíveis pequenos/médios/grandes. Fixar orçamento verificável no recorte antes de
otimizar; não prometer FPS sem medição. Usar atualizações parciais quando trouxerem ganho
comprovado, preservando invalidação por resize/DPI/tema/dados e descarte de callbacks antigos.
Análise pesada fica fora da thread da UI; operações Tk seguem na thread principal.

## 3. Refinamentos posteriores — POSTERIOR/P2

| ID | Proposta | Condição para entrar | Limite |
|---|---|---|---|
| UI-P01 | Alternância confortável/compacta | Percurso básico validado e demanda por maior densidade medida | Não diminuir fonte/contraste/acesso para acomodar mais controles; confortável já é requisito atual |
| UI-P02 | Vários perfis completos de disposição/área de trabalho | Preferências básicas e restauração padrão estáveis | Não confundir perfil visual com preset financeiro; migração/recuperação próprias antes de adoção |

Não bloquear T19.35/T20.30 por esses refinamentos. Quando escolhidos, detalhar uma tarefa
por PR sobre os mesmos componentes, mantendo separado o aceite do percurso básico.

## 4. Experimento técnico futuro — EXPERIMENTO/P3

### UI-X01 — Componente financeiro especializado, candidato Lightweight Charts

**Estado: registrado, não iniciado.** A hipótese é obter uma experiência financeira mais
fluida para preço/operações. Não há decisão de adoção, instalação de dependência, troca
de toolkit nem comparação de performance executada por esta documentação.

Avaliar depois da tela de referência/medição da base, ou diante de limitação demonstrada
do renderer atual. Registrar ficha própria antes de entrar na fila, fora do caminho crítico
F19/F20. A restrição F17 continua válida; eventual exceção pertence a decisão arquitetural
explícita futura, com justificativa e versão, não a uma interpretação do agente executor.

Escopo da prova de conceito: uma visualização isolada com os mesmos DTOs e dados do gráfico
atual, sem refazer cálculos e sem operar conta. Comparar instalação/empacotamento, offline,
tempo/memória/interação, integração com temas/DPI/teclado, sincronização/eventos, exportação,
ciclo de vida dos recursos e ambientes Windows/Linux. Identificar superfície web necessária,
dependências efetivas e licença/atribuição da versão avaliada. Recursos/indicadores/desenhos
precisam de implementação e testes; nome da biblioteca não entrega toda a experiência.

Critério de saída: relatório comparável de ganhos, perdas e ambientes não ensaiados; decisão
de adotar, limitar a um tipo de gráfico ou descartar. Preservar renderização atual como
referência até decisão/integração testada. Nenhuma meta de FPS, compatibilidade ou vantagem
é presumida. Matplotlib permanece a base dos gráficos atuais/estatísticos/relatórios;
uma adoção híbrida só entra se justificada. Migração integral da aplicação para web/Qt/
Electron não é escopo deste registro nem consequência automática do experimento.

Fontes primárias consultadas na avaliação de 2026-10-17:

- [Matplotlib integrado ao Tk](https://matplotlib.org/stable/gallery/user_interfaces/embedding_in_tk_sgskip.html).
- [Atualização parcial de renderização Matplotlib](https://matplotlib.org/stable/users/explain/animations/blitting.html).
- [Lightweight Charts](https://tradingview.github.io/lightweight-charts/) e
  [recursos/exemplos](https://tradingview.github.io/lightweight-charts/tutorials).

## 5. Sequência e prova da entrega atual

1. **Mapear e desenhar:** T20.22 identifica a navegação atual, contexto, estados e regiões
   da tela de referência. Fixture provisória é identificada; não comprova Analytics integrado.
2. **Entregar a capacidade:** T19.30/31/32 e serviços correspondentes entregam seus recortes
   com UI, sem gráficos apoiados em cálculos inventados. Reusar o mapa e componentes.
3. **Ligar inspeção:** T20.23 conecta preço/eventos/patrimônio/seleção/inspetor conforme dados
   efetivos; não iniciar todos os módulos para desenhar uma tela.
4. **Validar o percurso:** T20.30 aceita o Manual simples. Depois ampliar para objetivos
   automáticos/Ultra/catálogo na ordem de dependências e pelo mesmo padrão validado.
5. **Reavaliar refinamentos/experimento:** UI-P01/P02 e UI-X01 conservam fila posterior,
   sem dependência do primeiro percurso. Não começar prova de conceito por ler seu registro.

T20.22 pode planejar a referência antes de integrar o inspetor T20.23; sua aceitação não
exige a conclusão de T20.23, que já depende dela. O aceite específico de inspeção pertence
a T20.23. Preservar uma tarefa por PR em vez de somar toda a tela em uma alteração monolítica.

Verificação do executor: DTO × tabela × gráfico × exportação; operação real da UI nos
temas/tamanhos/DPI/teclado; ausência/zero/indefinido, cancelamento e troca de contexto;
medições antes/depois e regressão do legado. Prova de preço/eventos operacionais é
reconciliada com replay MT5 nativo/DEMO quando aplicável. Seleção de recorte curto serve
a QA funcional e não prova utilidade OOS, forward ou fill DEMO ao vivo. Fonte/qualidade e
limitações continuam explícitas; aplicar VALIDACAO_F19 e AGENTS, sem conta REAL.

Entrega deste registro: documentação/priorização e vínculos no plano/roadmap/prompt.
Nenhuma tela foi desenhada/implementada/validada, nenhum benchmark ou experimento iniciado.
