# T13.9 — fator de lucro definido e sem teto artificial

## Objetivo e evidência

Consolidar somente o contrato do fator de lucro (PF). O Laboratório e o card
limitam razões a 99; a validação converte infinito em 999; a bateria pode aprovar
amostras sem perdas. A auditoria somente de leitura da cópia histórica encontrou
276 execuções, todas com livro finito reconciliável, das quais 14 tinham número
de PF sem denominador de perdas. Isso não é uma campanha de pesquisa nem prova
de custos líquidos completos ou de amostra independente.

## Passos, nesta ordem

1. Preparar ambiente próprio e executar a suíte antes da alteração. Concluído:
   baseline `24faa35`, 887 aprovados/9 ignorados, 687,11 s, saída 0. Inspeção dos
   produtores, consumidores e tabelas concluída; o walk-forward escolhe por soma
   de R e continuará assim.
2. Criar uma função compartilhada, sem dependências novas: soma dos PnLs positivos
   dividida pelo módulo da soma dos negativos, sem limitar razões finitas. Gravar
   também versão, estado, ganhos/perdas brutos e quantidade de perdas. Com livro
   vazio, sem perdas ou valores inválidos, PF é `None`/JSON `null`, com motivo.
   Só perdas é PF zero definido. Outras fórmulas e o livro permanecem iguais.
3. Consumir o contrato no backtester, card, Laboratório, validação e bateria.
   PF indefinido não satisfaz limiares. Formatação e ordenação devem aceitar
   ausência, colocar valores desconhecidos depois dos conhecidos e não converter
   `null` em zero. UI mantém o motivo acessível junto ao indicador.
4. Compatibilidade somente na projeção de leitura, sem reescrever registros ou
   schema: quando o livro antigo completo está disponível, recalcular só PF e
   acrescentar sua proveniência. Sem livro, preservar o número original em campo
   próprio; os antigos sentinelas/tetos 99/999 e valores não finitos ficam
   não verificados, sem aprovação projetada. Outros números antigos permanecem
   identificados como relatados, sem alegação de denominador auditado. Validações
   novas persistem o contrato no JSON já existente. Uma aprovação histórica
   armazenada fica preservada mesmo quando a leitura exige nova validação.
5. Testar razões >99 e >999, só ganhos/perdas, zeros/vazio, inválidos, JSON
   estrito, consumidores e ordenação, falsa aprovação, banco antigo e ausência
   de mutação. Comparar PnL/equity e métricas alheias ao PF; executar focados,
   suíte completa, doctor, revisão técnica, CI e PR próprio.

## Limites e aceite

Não muda risco, custos, estratégia, escolha de parâmetros, thresholds existentes,
ou autorizações financeiras. PF definido não é sinônimo de amostra suficiente.
O requisito específico da campanha PF19 continua incluindo perdas, sessões,
OOS independente, custos e intervalo de confiança. Os valores desta correção
não podem ser apresentados como novo resultado OOS.

O relatório do MT5 define a razão ganhos/perdas brutos. A API nativa usa
`DBL_MAX` quando não há perdas: esse sentinela deve ser distinguido da política
de elegibilidade da pesquisa, que exige denominador e amostra adequados.

Referências consultadas em 2026-10-16:

- https://www.metatrader5.com/en/terminal/help/algotrading/testing_report
- https://www.mql5.com/en/docs/constants/environment_state/statistics
