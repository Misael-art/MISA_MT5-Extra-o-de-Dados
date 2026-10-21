# T19.34 inicial — contexto do robô dentro do viewport

Os textos de descrição, contexto, execução e resultado do detalhe de Robôs
agora quebram linhas pela largura alocada ao próprio rótulo. Antes usavam a
largura do cartão externo, maior que a área útil da aba com rolagem; parte da
data, parâmetros e avisos ficava cortada mesmo havendo espaço para mais linhas.

A correção preenche a largura disponível e reage ao `Configure` do rótulo. A
mesma largura não é reaplicada quando só a altura muda, evitando ciclos de
geometria. Conteúdo, métricas, seleção, cálculos e botões não mudam. A rolagem
vertical e Page Down existentes continuam dando acesso ao restante do resultado.

Regressão dirigida reproduziu 688 pixels de quebra de linha para um rótulo com
594 disponíveis. Após o ajuste, foram conferidos 1920, 1280, 900 e 820 pixels,
incluindo 900×620, dois temas e escalas Tk simulando 96/144 DPI, com fonte ampliada.
Textos longos, troca de tema, resize repetido, seleção e resultado preservados
entram nas verificações. Três testes anteriores do detalhe permanecem aprovados.

Uma prova Tk/Xvfb separada reabriu o mesmo run histórico isolado da composição
T19.24, sem novo cálculo, conferindo os limites dos rótulos e quatro capturas.
Em altura curta, o conteúdo vertical é percorrido por rolagem; a captura não
significa que todos os elementos cabem simultaneamente. O resultado histórico
não recebe qualificação financeira por essa apresentação.

Este recorte não conclui T19.34. Editor/gerador e gráficos ainda não disponíveis,
acessibilidade completa, DPI físico e Windows físico conservam validação própria.
Não há mudança de fonte/binário nativo nem novo aceite operacional.

O CI Windows da ponta8056fd6 revelou um limite do ensaio: em144DPI, um rótulo
não estava mapeado após a espera fixa de150ms (2.753 testes passaram, um falhou).
O ensaio agora percorre as duas áreas de rolagem e aguarda, por no máximo5s,
o rótulo ficar mapeado, visível e dentro da largura disponível. Confere também
a interseção vertical com ambos os viewports; não exige que todos os textos
longos apareçam simultaneamente. Preserva texto, métricas, temas, tamanhos,
escalas e Page Down. Nenhuma asserção anterior foi removida e o runtime continua
byte a byte igual à ponta8056fd6. A causa exata do mapeamento no runner não foi
reproduzida no Linux;12 testes locais passaram após o ajuste do ensaio. O novo
CI Windows continua obrigatório; isso não qualifica Windows/DPI físicos.

A rodada39589919928 da ponta66571c4 também falhou no alcance em900×620/144DPI
(2.753 passaram/85skips). O diagnóstico mostrou largura válida835/892px, mas
rótulo desmapeado e interseção vertical vazia563..293. O ensaio usava a posição
global do próprio rótulo ainda desmapeado para revelar o viewport externo. Agora
calcula a posição do viewport dentro do conteúdo pelos seus ancestrais e consulta
coordenadas globais apenas depois do mapeamento, para verificar visibilidade.
Preserva prazo5s, larguras, texto, duas rolagens, temas e escalas.12 testes locais
passaram em32,04s; a confirmação Windows depende da nova ponta, sem assumir que
esse diagnóstico local reproduz integralmente o gerenciador de janelas do runner.
