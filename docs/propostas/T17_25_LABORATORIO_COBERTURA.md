# T17.25 — Dados e comparação do Laboratório

Pedido de 2026-10-17: gráficos de microestrutura vazios e comparação sem resultados. Este recorte trata o caminho dos dados até essas duas visões. Contagens e ícones dos cards ficam na T17.26, em PR próprio.

## Observação e diagnóstico antes da implementação

- As 161 operações do Donchian na base instalada são registros antigos, sem entrada/preço/stop. Não permitem reconstruir a microestrutura nem refazer gestão. Na execução selecionada da captura, o motor ignora 81 de 81 operações; a UI tenta acessar `seq` numa lista vazia e mostra esse erro.
- O cache do enriquecimento não considera o conteúdo das barras. Um caso controlado confirmou contexto vazio mesmo depois de adicionar o histórico; a invalidação manual o recuperou.
- A associação dos resultados usa horário, que muda nas entradas aproximadas à resolução das barras e também pode se repetir. A identidade do livro deve permanecer estável.
- O instalador sem MT5 e dependências de desenvolvimento foram executados em ambiente isolado. Baseline do SHA `0c5f74b`: **967 aprovados, 9 pulos existentes, saída 0**, em 1340,41 s. Três regressões privadas falharam pelos motivos acima antes do código.

## Escopo e ordem

1. Incluir o conteúdo OHLCV na assinatura do cache, compartilhando a leitura por série entre runs. Mudança dentro do mesmo intervalo e chegada de barras devem invalidar o contexto afetado; dados inalterados continuam usando cache.
2. Acrescentar ao resultado do motor a sequência original, sem mudar preço, custo ou regra de execução. O runner entrega frames com essa identidade também nos modos de dimensionamento.
3. Recusar comparação sem entradas/dados utilizáveis com motivo e ação em português. Preservar o contrato dos frames vazios; limpar resultados antigos e impedir que callback de seleção anterior repinte outra seleção.
4. Identificar runs sem colisão de rótulos, preferir um completo quando ainda não há seleção válida e tornar explícita a cobertura da microestrutura, exclusões e aproximações da simulação.
5. Validar núcleo/GUI, suíte geral, doctor, temas e resoluções; então CI no SHA final, revisão própria identificada, integração normal, confirmação remota e operação pelo lançador instalado.
6. Incorporar como **novas execuções** as exportações completas nativas já preservadas, com hash, relatório, parâmetros e timeframe reais. Preservar linhas originais e não reconstruir entradas antigas. A tentativa EMA que não concluiu com sucesso é excluída. Dados importados continuam retrospectivos; não são avaliação OOS nem nova execução do Tester.

## Aceite

- [x] Contexto se atualiza quando chegam barras ou ocorre correção dentro da mesma faixa; segunda consulta sem alteração usa cache.
- [x] Run antigo não gera `KeyError`, tabela vazia sem explicação ou zeros apresentados como resultado calculado.
- [x] Operações simultâneas, aproximadas e exclusões preservam a identidade original e a concordância entre tabela e curvas.
- [x] Trocar run/filtro durante cálculo não mantém resultados anteriores ou aceita callback antigo.
- [x] Os quatro gráficos usam operações com contexto disponível, com população e limites explícitos.
- [ ] Testes novos, suíte completa, doctor, CI exato, revisão e integração documentados no PR.
- [ ] Lançador principal com base real e DEMO existente: gráficos e comparação exercitados; registros/configuração e terminal preservados.

## Limites

Não muda a metodologia de custos, a resolução usada pelo motor nem as regras financeiras. Não preenche entradas antigas por inferência, não aprova estratégia por lucro observado, não envia ordens e não repete campanha financeira. Windows físico com MT5 permanece no checklist; CI Windows é uma evidência distinta.

Uma primeira cópia privada da importação de manutenção gerou curva em formato incorreto. O teste foi encerrado, a evidência de falha foi preservada e o driver foi corrigido para o contrato `[horário, patrimônio]`. A base instalada não foi alterada por esse ensaio. Só a repetição corrigida pode servir como aceite.


## Refinamento observado na inspeção com dados completos

Na primeira renderização funcional, a tabela exibiu três modos com 119 operações e os frames mantiveram todas as 119 identidades. Os quatro gráficos de microestrutura foram preenchidos e a cobertura explicitou as 161 operações antigas sem entrada. Entretanto, as duas figuras de execução ficaram comprimidas abaixo da tabela. Antes de implementar o refinamento, fica registrado: reutilizar o notebook de subvisões existente para **Resultados, Curvas e Horários**, mantendo seleção e modos acima. Testar e inspecionar área útil dos gráficos em ambas as resoluções; sem mudar os cálculos.

A inspeção das capturas também mostrou margem excessiva da legenda após redimensionar: o layout matplotlib tinha sido calculado antes do tamanho final do canvas. Registrar antes da alteração: redesenhar o layout dos gráficos do Laboratório no Configure existente, com regressão de largura útil. A rolagem continua disponível em janelas pequenas.


## Verificação da implementação

35 testes iniciais do núcleo e 55 testes focados passaram; as regressões adicionais da área útil, das contagens reais do mapa e do resize também passaram. A inspeção final da aplicação sobre cópia consistente da base concluiu **28 casos** (quatro gráficos de microestrutura e três visões de execução, dois temas e 1280×800/1920×1080), saída 0. No Donchian: 119 operações com contexto, entre 280 no recorte; três modos com 119 resultados/identidades cada. O teste antigo de 81 operações voltou com motivo e ação, sem `seq`. A altura dos gráficos é percorrível pela rolagem em janelas pequenas.

Na cópia, as 12 baterias, 271 execuções e 39.261 linhas originais do livro permaneceram idênticas. Somente as cinco novas execuções verificadas e suas 769 operações foram acrescidas. O importador de manutenção usa parâmetros/intervalo/timeframe do relatório/INI preservados, hash dos artefatos e curva no contrato do produtor existente. A tentativa EMA com estado FAILED não entrou.

Nenhum teste foi removido ou desativado. Nenhum script shell ou dependência mudou. Suíte final, CI no SHA exato, revisão própria, confirmação remota e teste instalado com a DEMO são gates rastreados no PR. As capturas privadas offline complementam esse teste instalado. A comparação mantém as hipóteses existentes do motor sobre barras; não demonstra paridade do PnL com o Tester nativo.

A revisão própria acrescentou a regressão da troca de robô enquanto a comparação antiga ainda termina. A população, métricas, avisos e comparação são limpos no início da leitura; a conclusão antiga é descartada, e um run previamente escolhido é recuperado quando ainda existe. A suíte final que havia acabado de iniciar foi interrompida antes dessa correção e não conta como aceite. A repetição deve usar o novo SHA publicado.
