# T17.27 — Layout após mudança de DPI da figura

## Observação antes do código

Na instalação principal, ./run.sh, base real e MT5 demo, a legenda de três modos de Execução/Curvas ultrapassou a borda direita em DPI144. A captura instalada `15-execution-curves.png` registra o corte. Uma reprodução com os mesmos nomes e Tk/Matplotlib nativos mediu figura1280px e legenda até1420px. Um novo cálculo de layout, após a figura assumir o DPI nativo, coloca a legenda inteira até1247px, sem mudar os dados.

O backend Tk atualiza DPI e tamanho físico ao mapear o canvas e emite `resize_event`; esse caminho pode ocorrer sem um novo Configure do widget. O Laboratório hoje recalcula somente pelo Configure Tk. Inspecionado o código do backend instalado, não uma hipótese sobre outra versão.

## Ambiente e baseline

Instalador --yes --no-mt5 e requirements-dev executados com Python3.11 em XDG/Wine/Desktop exclusivos, sem mexer na instalação operacional. O master37182d2 tem árvore exatamente idêntica à composição4f15880 testada:998 aprovados,9 pulos existentes,1aviso, saída0. Todos os pacotes dos dois ambientes foram comparados e são idênticos; evidência de equivalência preservada antes de alterar qualquer arquivo rastreado. A suíte completa será repetida após a alteração.

## Escopo e passos

1. Registrar este recorte e baseline antes da implementação.
2. Reproduzir em teste a primeira renderização/mapeamento com DPI100/144,1280×800/1920×1080 e claro/escuro; verificar geometricamente legenda e área de dados, além de dados e rótulos intactos.
3. Ligar o recálculo compartilhado de Chart ao resize da própria figura, inclusive DPI ao mapear, sem duplicar bindings do Laboratório. Preservar ciclo de vida, thread Tk e draw_idle. Se o diagnóstico divergir, parar e registrar no PR.
4. Testes focados, inspeção real com cópia consistente, suíte completa, doctor, CI no SHA exato e revisão própria identificada; merge normal e conferência instalada com mesma DEMO.

## Aceite

- [x] Legenda inteira dentro da figura após o primeiro mapeamento em DPI100/144 e nos dois tamanhos/temas.
- [x] Resize de ida/volta recalcula layout; nenhuma série/valor/nome perdido; legenda fora da área de dados e largura útil preservada.
- [x] Nenhum callback pendente ou recálculo em laço ao destruir/redesenhar.
- [ ] Suíte, CI, doctor e aplicação instalada registrados no PR.

Não muda cálculos, filtros, população, hipóteses de execução, schema, configuração, número de séries nem métodos financeiros. Não redesenha mapas nem inclui mudanças nos cards. A densidade vertical pode continuar exigindo rolagem e fica registrada como limite, sem corte de dados. Sem ordens, sem campanha PF. Windows físico com MT5 continua pendente; CI Windows é evidência distinta.

## Segundo ciclo observado — altura realimentada pelo backend

Os primeiros32 testes focados passaram e16 casos com cópia real confirmaram as legendas inteiras em DPI144. Porém a própria inspeção registrou figuras de911→2784px ao alternar quatro visões. Isso não é aceite visual: corrigir a legenda não resolveu a ocupação. Registra-se antes de alterar: o tamanho solicitado pelo Canvas/DPI se propaga ao Notebook/viewport, que expande os outros canvas. Reutilizar o princípio já adotado em Análise/T17.18: chart_area com altura própria pela fonte, propagação desligada e somente largura expandida; o backend conserva liberdade para redimensionar a figura dentro dessa área. Nada de modificar backend ou reduzir fontes.

Extensão deste mesmo recorte de layout: regressão de limites mínimo e máximo ao alternar abas/tema/resize, inclusive Notebook com altura artificialmente grande; conferir todos os gráficos de microestrutura e execução com dados reais. Manter rolagem quando os controles e o gráfico legível excedem a janela. O primeiro lote de capturas fica preservado como diagnóstico, não aceite da altura.

## Verificação antes da suíte final

34 focados de gráficos/Laboratório aprovados, saída0, incluindo8cenários de mapeamento/DPI/resize/tema e2de altura com página artificial de2000px. Antes da correção, o limite superior reproduziu1918px para teto482px; os registros de falha foram preservados. O primeiro teste de altura demonstrou o mínimo insuficiente, e a ordem foi ajustada para exercitar primeiro o excesso observado.

Cópia consistente da base real, em DPI144:32casos aprovados nos dois temas/tamanhos, com quatro gráficos micro, curvas/horários de Execução e duas legendas de outras visões. Todas as alturas ficaram em600px; os três modos mantiveram119operações/identidades. Capturas e exportações de figuras completas foram inspecionadas; não são dados sintéticos nem operação instalada. O primeiro lote com alturas excessivas permanece diagnóstico. DPI100, suíte, CI, revisão, doctor e operação instalada serão registrados no PR. O aviso Matplotlib de margens do teste legado permanece visível e não foi suprimido.
