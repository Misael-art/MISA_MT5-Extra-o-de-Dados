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
- **Arquivos previstos:** novo `mt5_extracao/ui/components.py` e testes; integrações em arquivos de tela ficam na T17.3/T17.4.
- **Componentes:** cabeçalho, barra de filtros, cartão de métrica, status com texto e ícone, avisos, estados vazio/carregando/parcial/indisponível, painel de gráfico, tabela, capacidade/progresso, resumo de comparação e painel de detalhes.
- **Aceite:** componentes recebem dados por argumentos explícitos, respeitam os dois temas, têm foco/teclado visíveis e não contêm cálculo de analytics; nenhum controle aparenta funcionar sem callback.

### T17.3 — Laboratório e gráficos

- **Objetivo:** organizar primeiro a tela de maior densidade analítica e tornar os gráficos legíveis.
- **Arquivos previstos:** `mt5_extracao/ui/laboratory.py`, `mt5_extracao/ui/charts.py`, testes e capturas.
- **Ordem visual:** título/contexto, filtros, métricas, avisos de disponibilidade/qualidade, guias, gráficos/tabelas e detalhes sob demanda.
- **Aceite:** manter amostra, intervalo e estado visíveis; separar conexão, qualidade, validação e resultado; indicar unidade e período; não conectar dados ausentes; não cobrir dados com legendas; manter zoom, seleção e dicas. A hachura pode diminuir apenas se outra indicação igualmente clara continuar presente.

### T17.4 — Aplicação gradual às outras abas

- **Telas:** Ativos, Minha base, Análise, Robôs, Recursos e Atividade.
- **Arquivos previstos:** `mt5_extracao/ui_manager.py` e módulos de tela existentes (`inventory.py`, `analysis.py`, `robots.py`, `robot_detail.py`, `resources.py`), com testes por tela.
- **Aceite:** manter nomes, navegação, ações e dados atuais; melhorar hierarquia, densidade, tabelas e estados; empilhar conteúdo em janelas menores; foco e ordem de Tab permanecem utilizáveis. Segurança/aprovação segue separada do resultado financeiro; capacidade desconhecida não vira percentual inventado.

### T17.5 — Validação visual integrada e documentação

- **Objetivo:** conferir a aplicação completa e fechar evidências da fase.
- **Cobertura:** 1280×800, 1920×1080, condição de DPI alto, janela reduzida/maximizada, claro/escuro, resize, teclado, dados completos/parciais/ausentes, amostra insuficiente, números grandes, listas extensas e muitas séries.
- **Aceite:** capturas comparáveis antes/depois; nenhuma sobreposição, corte ou perda de informação; números idênticos aos motores; estados de qualidade explícitos; `pytest` e `doctor` passam; pendências que exigem Windows real estão no checklist sem alegação de validação não executada.

## 3. Evidência de linha de base da T17.1

As cinco capturas citadas no pedido não estavam disponíveis no diretório de anexos ao iniciar esta tarefa. Para não bloquear a inspeção, a linha de base foi capturada no aplicativo real com ledger e barras sintéticos, sem conexão ao MT5. `docs/img/interface-visual/T17.1/mt5x-before-light.png` e `mt5x-before-dark.png` mostram o antes; `mt5x-after-light.png` e `mt5x-after-dark.png` mostram o estado após os tokens da T17.1, todos em 1280×800. As capturas originais do usuário devem ser anexadas à T17.3 para orientar e completar a comparação visual.

Problemas observados na linha de base: a configuração Tk do host podia cair na fonte bitmap `fixed` de 7 pt; a hierarquia entre texto de apoio e conteúdo principal era fraca; o tema claro tinha contraste `muted` 3,15:1 e `accent` 3,87:1 em alguns fundos; a grade de amostra pequena mostrava hachura em todas as células da fixture. A hachura e o estado de amostra pequena devem continuar identificáveis.

O teste inicial da worktree Linux antes das alterações foi **809 aprovados, 10 pulados, em 492,63 s**, sob `xvfb-run -a`. Depois das alterações, os testes de interface focados tiveram **43 aprovados** e a suíte final **814 aprovados, 10 pulados, em 718,50 s**, com `TMPDIR` e `--basetemp` próprios. Um run completo anterior terminou por falta transitória de espaço. `doctor`: 16 OK, 2 avisos esperados, 0 falhas. O Xvfb local pode expor um conjunto diferente de fontes do X real; por isso, teste automatizado e inspeção visual da fonte instalada são verificações distintas.
