# Navegação e status com fonte ampliada

Recorte T19.34 sobre a composição175/e174cc7, separado do corretivo da página172.
A captura em900×620,144DPI simulado e escala de leitura1,5 expôs nomes das áreas e
status/base cortados. O contexto já tinha rolagem; faltava um caminho legível global.

Quando os sete nomes não cabem, o seletor **Área** mostra os mesmos nomes completos
e controla o Notebook existente. Seleção pelo menu ou por código mantém o seletor
coerente; ampliar a janela restaura as abas e transfere o foco do seletor ocultado.
As páginas não são recriadas e a troca não executa ações financeiras.

Status e informação da base usam duas linhas quando necessário. Se uma mensagem
exceder a largura ou tiver várias linhas, a barra apresenta um resumo com reticências
e o botão **Detalhes**. O detalhe preserva o texto integral do momento da abertura,
selecionável e rolável, com a fonte de leitura escolhida. Espaço ativa o botão,
Ctrl+End chega ao final e Escape fecha a janela. Mensagens curtas continuam iguais;
os modelos e o log integral permanecem. A atualização síncrona anterior é preservada.
Não há nova configuração, cálculo, perfil, fonte de dados, cache ou mudança nativa.

## Verificação

Baseline dirigida:3 testes anteriores aprovados20,05s; duas novas regressões falharam
antes do ajuste19,70s. Durante o desenvolvimento, uma assert nova supôs que o ttk
retornaria lista vazia para abas ocultas; este Tcl representa isso com elemento
`null`. A assert foi corrigida para exigir ausência do rótulo da aba, conservando a
verificação da visibilidade, foco e nomes completos. Essa falha foi do teste novo.
A captura identificou e corrigiu fonte pequena e scrollbar sem espaço no detalhe.

Seleção final:10 aprovados131,60s, incluindo7 novos casos e3 de contexto/página.
Matriz:1920×1000,1280×800,900×620,820×800; dois temas,96/144DPI simulados e escalas
1,0/1,5; retorno repetido, seleção preservada, largura e foco. Seleção do entorno
teve15 aprovados e a assert ttk acima falhou; após corrigi-la,6 dirigidos passaram
52,37s incluindo conexão, mensagens de erro e backtest pela UI. Contagens se
sobrepõem; a matemática não foi modificada. Doctor sem probe:20OK/1aviso opcional/0.

A candidata reabriu o histórico privado existente48fede,3operações/111,0875, sem
recalcular:24 medidas, dois temas/quatro tamanhos e captura extra de fonte máxima.
Modelo, run e resultado permaneceram. A mensagem longa da captura de Detalhes é
QA de legibilidade identificado, não uma falha real de conexão. Nenhum banco do
proprietário foi aberto nesse ensaio; nenhum MT5/Tester foi iniciado.

CI da ponta e instalação dependem da fila172→173→174→175 e dos seis checks próprios.
A repetição seletiva do Windows172bfc trata um timeout em commit SQLite anterior à
UI; não concede aceite a este recorte. Windows físico, acessibilidade completa,
resumo/cartões/gráficos em todos os perfis e T19.34 ampla continuam pendentes.
Não há aceite financeiro, nativo, reserva estatística ou operação ao vivo.
