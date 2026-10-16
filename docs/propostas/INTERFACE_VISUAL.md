# Fase 17 — Modernização visual e de usabilidade do mt5x

**Pedido do dono do projeto (2026-10-14):** tornar a interface Tkinter existente mais legível, consistente e profissional, com estética de aplicativo SaaS desktop. Preservar arquitetura, cálculos, dados, callbacks e regras de segurança. Este plano organiza a frente em tarefas pequenas; cada uma corresponde a uma entrega revisável.

## 1. Limites e convivência

- Permanecem Tkinter/ttk e matplotlib; sem migração para web, Electron, Qt ou toolkit novo.
- A Fase 17 apresenta dados existentes. Não altera schemas, modelos financeiros, critérios estatísticos, permissões, custos, sinais, operações ou APIs públicas.
- O pacote visual fica em `mt5_extracao/ui/`; nenhum `tkinter` novo entra no núcleo.
- Reutilizar `ui/theme.py`, `ui/icons.py` e os componentes existentes. Extrair componentes apenas a partir de usos concretos.
- Fase 13 calcula contexto de mercado; Fase 16 calcula analytics de operações. A Fase 17 não importa nem duplica esses motores.
- Dados ausentes, parciais e amostras insuficientes continuam identificados. Se reduzir hachuras, usar texto, ícone ou padrão alternativo com a mesma clareza.
- As operações de Tk continuam na thread principal. Gráficos, callbacks, coleta de lixo, filas e ciclo de vida das imagens mantêm as proteções existentes.

## 2. Sequência de entregas

### T17.1 — Tokens, tipografia, escala e temas

- **Objetivo:** centralizar valores visuais e melhorar a legibilidade sem alterar layout ou semântica das telas.
- **Arquivos:** `mt5_extracao/ui/visual_tokens.py`, `mt5_extracao/ui/theme.py`, `tests/test_ui_theme.py`, capturas em `docs/img/interface-visual/T17.1/`, este plano e o checklist Windows.
- **Passos:**
  1. Medir a família de fonte e o DPI que o Tk realmente enxerga; registrar captura do Laboratório com fixture sintética nos temas claro e escuro.
  2. Centralizar paletas, papéis tipográficos, escala, espaçamento, dimensões e tamanhos de texto dos gráficos.
  3. Escolher fonte instalada com preferência por Inter e fallbacks por plataforma; usar a fonte padrão do Tk só quando nenhuma preferência existir. Não baixar nem distribuir fontes.
  4. Aplicar tokens ao `Theme`, preservar a assinatura `Theme(root, dark=...)` e disponibilizar escala de leitura sem multiplicar o DPI já aplicado pelo Tk.
  5. Medir contraste das cores textuais e de séries e acrescentar testes estáveis para paletas, fonte disponível e escala.
- **Aceite:** fontes pequenas ficam com pelo menos 10 pt e texto base com pelo menos 11 pt (ou preserva tamanho maior do sistema); texto comum atinge 4,5:1 nos dois temas; elementos gráficos essenciais atingem 3:1; seleção, foco, estados, menus e tooltips continuam acompanhando o tema; Tk conserva o dimensionamento de DPI do sistema.
- **Verificação:** `tests/test_ui_theme.py`, suíte completa, `doctor`, captura 1280×800 com fixture; a validação adicional 1920×1080, DPI alto, resize e Windows real fica registrada na T17.5.
- **Estado:** concluída no Linux; Windows pendente. O Tk escolheu Lato no host; Inter e Noto Sans não estão entre as famílias expostas pelo X atual. Capturas comparáveis estão na seção 3.

### T17.2 — Componentes reutilizáveis de apresentação

- **Objetivo:** consolidar padrões usados pelas telas sem criar um framework genérico.
- **Arquivos:** novo `mt5_extracao/ui/components.py`, `mt5_extracao/ui/theme.py` e `tests/test_ui_components.py`; as integrações
  em arquivos de tela ficam na T17.3/T17.4.
- **Componentes:** cabeçalho, barra de filtros, cartão de métrica, status com texto e ícone, avisos, estados
  vazio/carregando/parcial/indisponível, painel de gráfico, tabela, capacidade/progresso, resumo de comparação e
  painel de detalhes.
- **Aceite:** componentes recebem dados por argumentos explícitos, respeitam os dois temas, têm foco/teclado visíveis e não contêm cálculo de analytics; nenhum controle aparenta funcionar sem callback.
- **Verificação:** baseline **814 aprovados, 10 pulados em 723,22 s**; foco de temas/componentes **18 aprovados**; suíte final **827 aprovados, 10 pulados em 790,90 s**. `doctor`: **16 OK, 2 avisos esperados, 0 falhas**; `git diff --check` passou.
- **Estado:** concluída no Linux. `Theme` fornece variantes semânticas para badge e aviso nos dois temas; símbolos têm fallback para Tk sem glifos ricos. Nenhuma tela foi integrada nesta tarefa. Windows real pendente.

### T17.3 — Laboratório e gráficos

- **Objetivo:** organizar primeiro a tela de maior densidade analítica e tornar os gráficos legíveis.
- **Arquivos previstos:** `mt5_extracao/ui/laboratory.py`, `mt5_extracao/ui/charts.py`, testes e capturas.
- **Ordem visual:** título/contexto, filtros, métricas, avisos de disponibilidade/qualidade, guias, gráficos/tabelas e detalhes sob demanda.
- **Aceite:** manter amostra, intervalo e estado visíveis; separar conexão, qualidade, validação e resultado; indicar unidade e período; não conectar dados ausentes; não cobrir dados com legendas; manter zoom, seleção e dicas. A hachura pode diminuir apenas se outra indicação igualmente clara continuar presente.
- **Baseline Linux:** **826 aprovados, 10 pulados, 1 falha** em 660,14 s. O teste de restauração de layout esperou uma resposta assíncrona por 0,6 s; a repetição isolada passou (**1 aprovado em 6,43 s**). Nenhuma alteração foi feita para essa ocorrência, que fica registrada como intermitência preexistente até a verificação final.
- **Entrega Linux:** integra `PageHeader`, `FilterBar`, `MetricCard`, `Notice`, `ChartPanel` e `TablePanel`. A visão geral usa tabela rolável para as 24 faixas horárias, com resultado, `n` e asterisco apenas para amostra positiva abaixo do mínimo; `n=0` aparece como sem operações. Curvas, calendário, horários, microestrutura e comparação usam subtelas para manter um gráfico por área de leitura. Gráficos mostram unidades, contagens e estado de amostra; heatmaps têm escala de cor, padrão distinto para dados ausentes e correlação fixa em −1…1. Os cálculos e seus motores não mudaram.
- **Inspeção visual:** capturas da visão geral em claro/escuro e 1280×800 em `docs/img/interface-visual/T17.3/mt5x-before-light.png`, `mt5x-before-dark.png`, `mt5x-after-light.png` e `mt5x-after-dark.png`. A análise temporal em claro, mesma resolução e fixture, está em `mt5x-before-tempo-light.png` e `mt5x-after-tempo-light.png`. Também foram inspecionadas as vistas Curvas, Calendário, Microestrutura indisponível e Correlação com dois robôs; o MT5 permaneceu desconectado.
- **Verificação final:** testes focados de tema/componentes/gráficos/Laboratório **27 aprovados em 16,89 s**. Suíte completa **831 aprovados, 10 pulados, 1 falha em 560,91 s**. A única falha foi novamente `test_settings_restore_the_arrangement_only_with_the_terminal_closed`, fora do escopo: o feedback assíncrono não chegou em 0,6 s; repetição isolada **1 aprovado em 5,84 s**. `doctor`: **16 OK, 2 avisos esperados, 0 falhas**; `git diff --check` passou. Não se alterou Configurações.
- **Estado:** concluída no Linux na worktree `../MISA_MT5-Extra-o-de-Dados-T17.3`, branch `tarefa/T17.3-laboratorio-graficos`, baseada no commit `0d81adb`. As cinco capturas originais mencionadas no pedido continuam indisponíveis; a inspeção usou fixture sintética e o MT5 desconectado. Windows permanece pendente para a T17.5/checklist.

### T17.4 — Aplicação gradual às outras abas

- **Telas:** Ativos, Minha base, Análise, Robôs, Recursos e Atividade.
- **Arquivos previstos:** `mt5_extracao/ui_manager.py` e módulos de tela existentes (`inventory.py`, `analysis.py`, `robots.py`, `robot_detail.py`, `resources.py`), com testes por tela.
- **Levantamento e propriedade antes da implementação (2026-10-15):** Ativos e Atividade ficam em `ui_manager.py` (`_build_symbols_tab`, `_build_log_tab`); Minha base em `ui/inventory.py`; Análise em `ui/analysis.py`; Robôs em `ui/robots.py` e `ui/robot_detail.py`; Recursos em `ui/resources.py`. A apresentação é responsabilidade desta tarefa. `ui/components.py` e `ui/theme.py` serão reutilizados; `ui/laboratory.py` e motores de analytics/backtest ficam fora do escopo. `ui_manager.py` também concentra navegação e callbacks assíncronos, então os ajustes nele serão limitados à disposição e estilo dos widgets das telas nomeadas. `docs/PLANO_DE_TRABALHO.md`, `docs/ROADMAP.md`, esta proposta e `docs/continuidade.md` são arquivos compartilhados e só serão atualizados por esta entrega ao final. O trabalho ocorre na worktree `../MISA_MT5-Extra-o-de-Dados-T17.4`, branch `tarefa/T17.4-aplicacao-abas`, baseada no commit T17.3 `ccde9ce`; não há edição concorrente desses arquivos no worktree.
- **Fronteira com Analytics:** não alterar funções de cálculo, critérios, schema, segurança dos robôs nem contratos. Tabelas, progressos, estados e detalhes recebem valores/callbacks já preparados pelas telas. A coordenação de implementação fica nesta worktree isolada; os componentes visuais isolados já foram entregues na T17.2.
- **Baseline Linux:** instalador e dependências de desenvolvimento concluídos; suíte completa **832 aprovados, 10 pulados em 618,76 s**, antes das alterações visuais.
- **Aceite:** manter nomes, navegação, ações e dados atuais; melhorar hierarquia, densidade, tabelas e estados; empilhar conteúdo em janelas menores; foco e ordem de Tab permanecem utilizáveis. Segurança/aprovação segue separada do resultado financeiro; capacidade desconhecida não vira percentual inventado.
- **Entrega Linux (2026-10-15):** Ativos e Minha base usam `FilterBar` para busca, seleção e ações; Ativos encurta apenas os títulos exibidos na tabela, preserva as chaves internas e permite rolar a tela em janelas menores. Análise e Recursos reorganizam filtros e cartões de métricas conforme a largura. Robôs reorganiza filtros e detalhes e oferece rolagem vertical no layout empilhado; a tabela de parâmetros mantém rolagem horizontal. Atividade agora usa cabeçalho e painel de conteúdo compartilhados. A apresentação foi ajustada sem alterar cálculos, dados, navegação ou callbacks.
- **Inspeção visual:** capturas de referência sintética antes/depois para as seis telas em `docs/img/interface-visual/T17.4/`. As capturas `mt5x-before-*-light.png` e as versões `mt5x-after-*-light.png` são 1280×800; há também versões depois em tema escuro na mesma resolução. As capturas narrow em claro são 900×850, com vistas adicionais 900×620 da parte inferior de Ativos e Robôs para conferir acesso aos controles. A fixture usa barras sintéticas, banco local e MT5 desconectado. O leiaute foi conferido em tema claro e escuro, em tamanho amplo e reduzido. As cinco imagens de referência originais mencionadas no pedido não estavam disponíveis e não foram reenviadas.
- **Verificação final:** teste responsivo focado **1 aprovado**; suíte completa **833 aprovados, 10 pulados em 653,73 s**, executada com diretório temporário dedicado em `/var/tmp`; `doctor` **16 OK, 2 avisos esperados, 0 falhas** (dependência opcional `pandas-ta` ausente e ponte MT5 desativada por `--no-mt5`); `py_compile` dos módulos alterados e `git diff --check` passaram. A primeira tentativa da suíte completa foi interrompida porque o `tmpfs` de sessão encheu; a repetição completa terminou sem esse erro.
- **Estado:** concluída no Linux na worktree `../MISA_MT5-Extra-o-de-Dados-T17.4`, branch `tarefa/T17.4-aplicacao-abas`, baseada no commit T17.3 `ccde9ce`. Windows real, DPI alto e resolução 1920×1080 permanecem pendentes para T17.5 e o checklist; não se declara validação Windows.

### T17.5 — Validação visual integrada e documentação

- **Objetivo:** conferir a aplicação completa e fechar evidências da fase.
- **Cobertura:** 1280×800, 1920×1080, condição de DPI alto, janela reduzida/maximizada, claro/escuro, resize, teclado, dados completos/parciais/ausentes, amostra insuficiente, números grandes, listas extensas e muitas séries.
- **Aceite:** capturas comparáveis antes/depois; nenhuma sobreposição, corte ou perda de informação; números idênticos aos motores; estados de qualidade explícitos; `pytest` e `doctor` passam; pendências que exigem Windows real estão no checklist sem alegação de validação não executada.
- **Validação Linux (2026-10-15):** executada na worktree `../MISA_MT5-Extra-o-de-Dados-T17.5`, branch `tarefa/T17.5-validacao-visual`, baseada no commit T17.4 `c3f625b`. A tela ocupou o display virtual 1920×1080 em 96 DPI (`tk scaling=1,333`) e 144 DPI (`tk scaling=1,998`); as seis telas do T17.4 também têm capturas comparáveis 1280×800 e 900×850. Em 900×620, Ativos empilha os painéis e mostra a barra de rolagem; em 1920×1080 retorna ao leiaute lado a lado. O Xvfb não executa estado de maximização de um gerenciador de janelas real.
- **Interação e dados:** `Ctrl+F` levou o foco à busca; Tab avançou para o filtro; buscar `SYNTH001` deixou uma linha. A fixture local continha 46 símbolos, 600 barras M1, 480 M15 e uma série curta de 10 barras; a Análise recusou abrir `PETR4` sem dados e abriu as séries com dados. O painel de detalhes foi exercitado com cotação sintética `1234567890.12`, com precisão preservada e sem corte. Nenhum terminal ou MT5 real foi conectado; a captura de número grande usa provedor MT5 falso.
- **Capturas:** `docs/img/interface-visual/T17.5/` contém as seis telas em claro/escuro a 1920×1080 nos dois perfis Xvfb (96 e 144 DPI), além de Análise com 10 barras e Ativos com número grande a 144 DPI. A revisão visual não encontrou cortes ou sobreposições nas telas examinadas; barras e listas longas permanecem acessíveis por rolagem. O comparativo antes/depois 1280×800 está em `docs/img/interface-visual/T17.4/`. As cinco referências originais do pedido não foram anexadas novamente.
- **Verificação:** baseline antes de alterações: **831 aprovados, 10 pulados e 2 falhas** na execução direta do desktop: a medição `test_aggregation_speed_on_50k_cached_trades` levou 1,59 s para um limite de 1,5 s; `test_dense_tabs_reflow_and_assets_remain_reachable_in_a_small_window` não conseguiu reduzir a janela porque o desktop manteve 1280 px. As repetições isoladas passaram (a responsiva no Xvfb); a suíte completa final no Xvfb 1280×800 passou: **833 aprovados, 10 pulados em 674,22 s**. `doctor`: **16 OK, 2 avisos esperados, 0 falhas** (`pandas-ta` opcional ausente e ponte desligada); `git diff --check` passou.
- **Estado:** concluída no Linux; T17.1–T17.5 foram verificadas no Linux e a Fase 17 está fechada nessa plataforma. Windows real, escalas de exibição de 125%/150%, DPI do sistema e maximização com gerenciador de janelas permanecem pendentes na linha T17.1–T17.5 de `docs/validacao_instaladores.md`; não se declara validação Windows.

## 3. Evidência de linha de base da T17.1

As cinco capturas citadas no pedido não estavam disponíveis no diretório de anexos ao iniciar esta tarefa. Para não bloquear a inspeção, a linha de base foi capturada no aplicativo real com ledger e barras sintéticos, sem conexão ao MT5. `docs/img/interface-visual/T17.1/mt5x-before-light.png` e `mt5x-before-dark.png` mostram o antes; `mt5x-after-light.png` e `mt5x-after-dark.png` mostram o estado após os tokens da T17.1, todos em 1280×800. As capturas originais do usuário devem ser anexadas à T17.3 para orientar e completar a comparação visual.

Problemas observados na linha de base: a configuração Tk do host podia cair na fonte bitmap `fixed` de 7 pt; a hierarquia entre texto de apoio e conteúdo principal era fraca; o tema claro tinha contraste `muted` 3,15:1 e `accent` 3,87:1 em alguns fundos; a grade de amostra pequena mostrava hachura em todas as células da fixture. A hachura e o estado de amostra pequena devem continuar identificáveis.

O teste inicial da worktree Linux antes das alterações foi **809 aprovados, 10 pulados, em 492,63 s**, sob `xvfb-run -a`. Depois das alterações, os testes de interface focados tiveram **43 aprovados** e a suíte final **814 aprovados, 10 pulados, em 718,50 s**, com `TMPDIR` e `--basetemp` próprios. Um run completo anterior terminou por falta transitória de espaço. `doctor`: 16 OK, 2 avisos esperados, 0 falhas. O Xvfb local pode expor um conjunto diferente de fontes do X real; por isso, teste automatizado e inspeção visual da fonte instalada são verificações distintas.

## T17.7 — Referência temporal e população do Laboratório

Registro anterior à implementação, 2026-10-16. Prioridade do dono: auditoria real e
refinamento visual antes da campanha PF ≥ 1,9. PRs #79/#80 continuam separados e sem merge autorizado.

- **Problema observado:** Horários mostra “Entrada” com 263 operações reais parciais
  enriquecidas pela saída. Na seleção mista, a matriz usa somente entradas, enquanto
  os KPIs usam a seleção inteira, sem explicitar essa diferença de população.
- **Dependências:** T16.3 (`time_ref`), T17.3 e componentes existentes, presentes no master.
- **Arquivos:** `ui/laboratory.py`, `tests/test_ui_laboratory.py`, este plano, painel de
  status, continuidade e evidências deste recorte. T17.6 permanece preservada separadamente.
- **Passos:** baseline; identificar populações recebidas; rotular matriz e contexto
  temporal; testar completo/parcial/misto e troca; inspecionar temas/resoluções/DPI;
  suíte e doctor; commit/PR próprio.
- **Aceite:** referência entrada/saída/mista explícita; exclusão de saídas na matriz
  mista visível; números e seleção inalterados; testes de regressão e capturas reais.
  Não suavizar curvas. Windows real permanece pendente. O aceite integral da nova
  auditoria continua separado deste recorte.
- **Baseline:** instalador Linux sem MT5 e dependências dev concluídos; 837 aprovados,
  9 ignorados em 726,65 s, saída 0, Xvfb exclusivo e Python 3.11.15.

- **Entrega:** PR #81 draft. Suíte no commit 061f751: 838 aprovados, 9 ignorados,
  saída 0; 6 testes focados aprovados. Doctor do principal aprovado. Worktree
  offline acusa caminho MT5 vazio. Capturas reais e pendências críticas em
  [auditoria](AUDITORIA_UI_20261016.md). Sem merge; sem aceite visual global.

## T17.9 — Laboratório responsivo e resultados roláveis

Registro anterior à implementação. Dependências T17.1–T17.3/T17.7, PR próprio
empilhado sobre #81, sem merge. Baseline instalada no Linux e suíte aprovada antes
de alterações. Evidência real em DPI144/1280×800: botão Atualizar cortado, notícias
fora da largura, matriz sem altura.

- **Arquivos:** ui/laboratory.py, ui/components.py, ui/relations.py (extração do
  contêiner rolável existente), testes de componentes/Laboratório/Relações, plano,
  continuidade e capturas.
- **Passos:** extrair área rolável existente para componente compartilhado; manter
  filtros de seleção fora dela; reorganizar cabeçalho e filtros por largura real;
  permitir rolagem da área de KPIs/avisos/resultados; testar resize/teclado/temas;
  operar no host em DPI96/144, 1280×800/1920×1080; corrigir após inspeção; suíte e
  doctor; commit/PR próprio.
- **Aceite:** todos os botões/filtros acessíveis, resultados com altura útil por
  rolagem, contexto selecionado visível, valores/callbacks/Tk GC preservados; nenhuma
  alteração de cálculo, fixture ou suavização (T17.8 separada). Windows real pendente.

## T17.12a — Ausência, zero e perda na preparação das tabelas

Recorte registrado antes da implementação, necessário para T17.12 (hierarquia de
atenção pedida pelo dono). A auditoria anterior às cores encontrou `add_run`
substituindo métricas ausentes por zero. A base real auditada tem 271 runs com
ambas as métricas presentes; a lacuna afeta dados incompletos e precisa de teste
próprio. Sem nova estatística dentro de widgets.

- **Dependência:** T17.9/PR #82, base 972658c. Ambiente instalado e baseline antes
  das alterações: suíte completa aprovada, evidência privada t1712-baseline.
- **Arquivos:** asset_table.py, tests/test_asset_table.py, este plano, painel,
  continuidade e evidências do recorte. Nenhum widget/colorização nesta tarefa.
- **Passos:** preservar ausência/não finitos na entrada do adaptador; considerar
  somente métricas disponíveis na apresentação; impedir fallback a runs antigos
  quando o mais recente está incompleto; exibir retorno negativo e zero existentes
  distintamente de N/A; testar latest/ausência/zero/perda/resultado conhecido junto
  de desconhecido; operar UI/CLI e conferir que os registros completos mantêm seus
  valores; suíte e doctor; commit/PR próprio.
- **Aceite:** ausência não vira zero; resultado negativo conhecido continua
  disponível; nenhuma fórmula/seleção de runs/regra de risco muda; texto percentual
  revela os valores já calculados. T17.12 aplica as cores depois deste suporte.

- **Evidência:** 14 testes focados aprovados. Comparação de 271 runs completos
  reais, cobrindo 13 séries: células e retornos idênticos antes/depois; somente
  apresentação textual de zero/perda mudou. Captura real em
  `docs/img/interface-visual/T17.12a/`. Suíte completa posterior pendente.

## T17.12 — Hierarquia discreta de atenção

Pedido do dono com capturas: positivos destacados suavemente, negativos discretos
e ausências sem competir por atenção. Recorte registrado antes da implementação.

- **Dependências:** T17.9 e T17.12a presentes; ambiente 3.11.15 preparado e
  dependências dev instaladas. Baseline de código a886351: 848 aprovados/9
  ignorados em 556,06 s; commit posterior 35321b3 alterou somente documentação.
- **Arquivos:** ui/visual_tokens.py, ui/components.py, ui/laboratory.py,
  ui/inventory.py; testes de tokens/componentes/Laboratório/Minha base; plano,
  continuidade e capturas.
- **Passos:** extrair seletivamente MatrixTable do WIP T17.6 preservado, sem
  suavização, gradientes ou ancestrais; centralizar cores de atenção pelo sinal;
  aplicar a Horários e Minha base, preservar contagens/sinais/ausências/baixa amostra;
  garantir teclado/rolagem/seleção/ordenação/tema/atualização; operar claro/escuro
  e DPI96/144,1280×800/1920×1080; medir contraste; suíte/doctor; commit/PR próprio.
- **Aceite:** positivo azul suave, negativo sóbrio e ausência neutra, contraste
  textual >=4,5:1, sinal e n visíveis, zero distinto de ausência, sem inferir
  aprovação/significância/autorização; métricas inalteradas. Cores representam
  sinal, não magnitude. Outras telas recebem aplicação progressiva com semântica
  coerente, sem confundir verde de dados na base em Ativos com retorno positivo.
  Windows real pendente; esta tarefa não declara aceite visual integral.

- **Implementação:** tokens compartilhados pelo sinal, sem gradiente de magnitude;
  MatrixTable com foco único por Tab, setas/Home/End, rolagem e cabeçalho fixo;
  ausência sem operações mostra traço discreto, com n=0 explicado na legenda;
  zero e contagens presentes continuam explícitos. Minha base mantém ordenação,
  seleção e estados de qualidade independentes do retorno.
- **Validação inicial:** 30 testes focados aprovados (duração no log privado t1712-focused-final); contraste >=4,5:1
  nos dois temas; base real com 263 operações parciais e 15 séries, em 96/144 DPI
  e 1280×800/1920×1080. Capturas em docs/img/interface-visual/T17.12/.
  Suíte completa no código 5388ec8: 852 aprovados/9 ignorados, 598,30 s, saída 0. Doctor offline: 15 OK, 2 avisos,
  1 falha esperada por caminho MT5 deliberadamente vazio; não é diagnóstico verde.
  A dependência #83 passou Linux/Windows/PostgreSQL/instaladores/lint no CI.

## T17.8 — linha do tempo com coordenadas observadas

Recorte registrado antes da implementação: retirar interpolação cúbica existente
na timeline de Relações, conforme instrução de não suavizar resultados.

- Dependência: T17.12/PR #84, código igual ao 5388ec8 validado integralmente
  (852 aprovados/9 ignorados); ambiente próprio instalado com dev. Baseline própria
  53484c2: 851 aprovados/1 falha/9 ignorados em 653,29 s, falha já intermitente de
  conexão que aguarda somente 0,8 s; supervisor terminou 143 antes do filho, que
  completou e escreveu o resumo. Código de saída do filho não foi capturado.
- Arquivos: ui/relations.py, tests/test_ui_relations.py, plano, proposta,
  continuidade e capturas. Fixture sintética continua explicitamente identificada.
- Passos: remover helper de suavização, desenhar somente centros/valores recebidos
  com NaN nas lacunas; ajustar legenda; verificar coordenadas exatas/ausências
  internas/temas/seleção; operar UI e capturar; suíte completa/doctor; PR próprio.
- Aceite: um ponto por janela recebida, sem pontos inventados; gaps não conectados,
  estados/marcadores/tabelas preservados. Sem cálculos, engine, contas ou integração
  F18; esta apresentação não demonstra resultado financeiro nem validação.

- Implementado no b9fd90a: helper de suavização removido; dados da linha iguais
  aos centros/valores recebidos, com NaN nas ausências. 11 testes focados aprovados
  (23,10 s); nova asserção de seleção na lacuna aprovada em rodada de 2,85 s.
  Base vazia local, fixture de Relações explicitamente sintética, dois temas,
  DPI96/144 e 1280×800/1920×1080. Suíte posterior b9fd90a: 852 aprovados/1 falha/9 ignorados em652,79 s, saída1; mesma espera fixa de conexão da baseline.
  Limite de ocupação vertical registrado para outro recorte; sem aceite global.

## T17.15 — sincronização do teste de recuperação da conexão

Recorte registrado antes do código. Falha repetida na baseline e pós-T17.8:
`test_no_internet_is_said_plainly_with_what_to_do` consulta a UI 0,8 s após cada
trabalho em background. Em carga, o último callback ainda está pendente; isolado
passou em6,04 s. Não há evidência de defeito do conector nesta reprodução.

- Base: T17.8/#85; ambiente próprio instalado/dev. Baseline completa b9fd90a:
  852 aprovados/1 falha/9 ignorados em652,79 s; nenhuma alteração de runtime desde.
- Arquivos: tests/test_ui_main_window.py, proposta/painel/continuidade.
- Passos: aguardar conclusão observável de cada tentativa com prazo limitado;
  manter todas as asserções de mensagens, estado, conta simulada e botão; testar
  cenário com worker deliberadamente mais lento que0,8 s; exigir falha explícita ao vencer o prazo; rodar
  focados, suíte e doctor; PR próprio. Nenhuma conexão/conta real necessária.
- Aceite: teste não depende de velocidade incidental da máquina; não aumentar
  somente sleep, não retirar asserção nem ignorar falha; timeout falha claramente.
  Não mexer em outras esperas ou runtime fora deste recorte.

- Focados: 5 aprovados em23,75 s, incluindo worker0/1 s e casos adjacentes
  de abertura/terminal existente/falha de organização. Todas as asserções de
  resultado preservadas; nenhuma mudança de runtime. Suíte completa20765d8:854 aprovados/9 ignorados
  em765,21 s, saída0.

## T17.14 — altura disponível da área analítica

Recorte registrado antes do código. Auditoria real em1920×1080/DPI96 mede832px
no viewport externo e629px no conteúdo, deixando203px ociosos; a timeline
permanece com267px de viewport e575px de conteúdo. O componente compartilhado
ScrollableContent ajusta largura, mas não altura da janela Canvas.

- Dependências: T17.15 (teste), T17.8 e T17.12. Ambiente próprio instalado/dev,
  baseline completa20765d8:854 aprovados/9 ignorados em768,23 s, saída0.
- Arquivos: ui/components.py, tests/test_ui_components.py e, se necessário ao
  aceite, teste de Laboratório; proposta/painel/continuidade/capturas.
- Passos: definir altura do conteúdo como máximo entre requisito e viewport;
  atualizar scrollregion sem ciclos; testar curto/alto e resize grande→pequeno→
  grande; operar ambos os temas,1280×800/1920×1080,DPI96/144; suíte/doctor/PR.
- Aceite: sem espaço vertical ocioso no contêiner quando o conteúdo cabe;
  conteúdo maior continua rolável, gráficos/tabelas não comprimidos, teclado e
  callbacks preservados. Nenhuma alteração de dados ou cálculos.
- EmDPI144 o conteúdo já excede o viewport; a largura de detalhe de143px é
  problema independente da divisão em colunas e foi separado comoT17.17.

- Implementado no40853f2: conteúdo preenche max(requisito,altura interna),
  sincronizado após cálculo de layout do Tk, com callback cancelado noDestroy.
  Primeiro teste detectou2px de rolagem artificial; corrigida a borda e testado
  o limite. Inspeção seguinte detectou altura antiga após quebra de linha; novo
  teste de wrap/resize/destruição passou (3 focados em0,79 s).
- Medição posteriorDPI96/1920: conteúdo830px/viewport832px com2px de borda,
  viewport interno468px em vez de267px. Resize1280→1920 restaura alturas exatas
  nos doisDPIs, sem comprimir gráficos. Largura do detalheDPI144 continua143px
  (T17.17). Fonte de Relações é sintética declarada, base local vazia de auditoria.
- Primeira suíte posterior60d8cc2 interrompida por alteração necessária (exit143),
  não é aprovação. Nova suíte completa40853f2:856 aprovados/9 ignorados em713,62 s, saída0. Capturas antes/depois
  em docs/img/interface-visual/T17.14/. Aceite visual global continua pendente.

- Focados finais:35 aprovados em52,74 s, incluindoLaboratório/Relações/componentes.

## T17.17 — detalhe legível nas divisões de Relações

Registrado antes da implementação. Medição real da UI emDPI144 encontra detalhe
com143px em1920×1080 e1280×800, por pesos de colunas sem reserva de largura;
texto quebra por palavra. FonteLato11 mede364px para28 dígitos, ou388px com
preenchimento. T17.14 resolve altura, não resolve essa largura independente.

- Dependência:T17.14/#87 e ancestrais. Ambiente próprio instalado/dev, baseline
  ad31f06:856 aprovados/9 ignorados em732,12 s, saída0, antes do código.
- Arquivos:ui/relations.py, tests/test_ui_relations.py, proposta/painel/
  continuidade/capturas. Não mudar valores, semântica de estados,engine ou fonte.
- Passos: largura mínima de detalhe pela fonte real, divisão proporcional que
  respeita a reserva ou empilhamento quando não couber; testar posições/medidas
  reais após resize1920→1280→820→1920, nos doisDPIs/temas; preservar seleção,
  rolagem e mínimos de gráficos/tabelas; suíte/doctor/PR próprio.
- Aceite: texto do detalhe tem largura legível emduascolunas; caso contrário
  empilha e permanece rolável. Sem fixar número de colunas por resolução
  nominal quando font/DPI/largura efetiva exigirem reflow. A fonte continua
  explicitamente sintética, não há conexão aoDTO/serviçoF18 ainda pendente.

- Implementado noe8bdb11: colunas proporcionais com reserva mínima pela fonte
  (28dígitos+preenchimento), incluindo o espaço entre áreas; empilha quando
  as proporções não cabem. Sem fixarDPI/resolução dentro da regra.
- Focados finais:12 aprovados em30,05 s. Operação em ambos os temas,DPI96/144,
  resize1920→1280→820→1920. EmDPI144/1920, detalhe143→441px (mínimo388);
  em1280e820, empilhado1154/774px. Gráfico mantém344px de altura em144DPI.
  Capturasdocs/img/interface-visual/T17.17; fonte sintética declarada.
- Baseline856/9,732,12 s, saída0; suíte completae8bdb11 em andamento.
  Ainda não constitui aceite visual global nem integração aoF18real.

## T17.16 — seleção e resultado de Backtest no detalhe do robô

Registrado antes de implementar, após reprodução com dados reais: seletor
EURUSD/4horas/Tudo e resultadoEURUSDH1; título informaH1, porém não diferencia
explicitamente o resultado armazenado da seleção para o próximo teste. Botão
Rodar backtest aparece cortado em1280×800.

- Dependência:T17.17/#88 e anteriores; ambiente próprio instalado/dev, baseline
  9f40e96 em andamento antes do código. Evidência privada do lote históricoUI.
- Arquivos:ui/robot_detail.py, tests/test_ui_main_window.py, proposta/painel/
  continuidade/capturas. Sem alteração de engine, métricas, banco ou risco.
- Passos: controles refluem pela largura real; seleção para próximo teste e
  resultado exibido têm textos distintos; mudar filtro marca resultado anterior,
  sem inventar equivalência de período para runs históricos; processamento não
  bloqueiaTk nem permite duplicar pedido; erro conserva resultado anterior
  identificado; callback não atribui teste a outro robô.
- A leitura do código encontrou ausência de rolagem no conteúdo da abaBacktest
  do detalhe. Será usada ScrollableContent daT17.14 no conteúdo já existente,
  preservando gráficos/validações e evitando compressão após o reflow. Nenhuma
  tela/estatística paralela. Esse refinamento foi registrado antes do código.
- Aceite: controles/botão visíveis e acionáveis em1280/1920,DPI96/144, teclado,
  seleção e resultados preservados; estado de carregamento/erro/anterior claro;
  runs históricos não chamados atuais sem comprovação do recorte; testes semMT5
  real e operação históricaPython somente emcópia privada, exposição registrada.

- Leitura anterior ao código confirmou que o callback do detalhe não chama
  `UIManager.backtest_recorded`, embora a execução em lote use esse hook para
  atualizar Ativos/Minha base. O recorte reutilizará o hook existente ao concluir
  o teste, com regressão da atualização imediata; não criará cálculos na UI.
