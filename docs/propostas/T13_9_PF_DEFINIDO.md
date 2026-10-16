# T13.9 — fator de lucro definido e sem teto artificial

## Objetivo e evidência

Consolidar somente o contrato do fator de lucro (PF). O Laboratório e o card
limitam razões a 99; a validação converte infinito em 999; a bateria pode aprovar
amostras sem perdas. A auditoria somente de leitura da cópia histórica encontrou
276 execuções, todas com livro finito reconciliável, das quais 14 estavam vazias
e tinham PF zero sem denominador de perdas. Isso não é uma campanha de pesquisa nem prova
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

## Implementação e verificação

O contrato comum está em `mt5_extracao/profit_factor.py`. O livro antigo é
reconciliado somente na leitura, com valor relatado e origem preservados quando
representáveis em JSON estrito. Um valor antigo não finito não reaparece como
`Infinity` no campo de proveniência; o registro original permanece no banco.
Baterias antigas vinculadas a uma execução com PF indefinido também exigem
nova validação na projeção, conservando flags e itens armazenados. Catálogo e
matriz reutilizam os runs carregados. A correção não valida aprovações antigas
sem vínculo de execução nem certifica custos que não foram registrados.

Focados iniciais:156 aprovados em53,87s. Após a revisão das baterias legadas:
93 aprovados em14,96s; contrato/live/UI:56 em27,52s. Suíte240f92a:926 aprovados
/9 ignorados em681,75s, saída0. O ajuste final de JSON/ordenação tem verificação
própria e novo head; a evidência integral correspondente é registrada no PR97.
A suíte5576195 foi encerrada para incorporar a revisão (241), sem alegação
de aprovação. Doctor isolado:15OK/2avisos/1falha deliberada por terminal não
configurado e ponte desativada. A principal foi diagnosticada separadamente:
21OK/1aviso/0falhas; Windows com MT5 real permanece pendente.

Na cópia histórica,4.140 valores alheios ao PF foram idênticos entre os dois
produtores comparados; os276 hashes de livros permaneceram iguais, assim como
o arquivo SQLite. Sessão nativa em144DPI:263 operações reais mantêm PF0,87;
cenários controlados explicitamente sintéticos mostram ausência com motivo e
PF2000 inteiro. O motivo curto cabe no cartão (106px requisitados/216px úteis),
após a inspeção detectar corte da descrição longa. Sessão encerrada com saída0.
Nenhuma estratégia nova, ordem, backtest ou resultado OOS foi produzido por
essa verificação. As informações de custos/moeda ausentes continuam pendência
explícita antes de qualificar qualquer campanha PF líquido≥1,9.
