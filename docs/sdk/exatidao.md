# Exatidão: o que está provado, o que foi corrigido e o que ainda é lacuna

Este documento existe para que nenhuma decisão de operar se apoie em um número mais bonito do que a realidade.
Ele separa três coisas: o que foi **medido** no host, o que foi **corrigido** por causa dessas medições e o que
**continua sendo lacuna**, com a consequência prática de cada uma. Revise-o a cada tarefa que mexer em backtest,
bateria ou tempo real.

Medições de 2026-10-12, host Linux, MT5 build 6258 no Wine, conta MetaQuotes-Demo, EURUSD H1, de 2025-10-03 a
2026-09-30, robô gerado de um cruzamento de médias (10 × 40, stop 1,5 × ATR, alvo 2R, risco 1% sobre 100.000).

## Corrigido

| Problema | Consequência se ficasse | Correção |
|---|---|---|
| No modelo "OHLC de 1 minuto" do Strategy Tester, um stop atravessado por gap de fim de semana é preenchido no preço do stop: −1,00R, contra −1,66R com "cada tick" e com ticks reais (medido em 6 operações) | Backtest e bateria subestimavam perdas | O padrão passou a ser ticks reais (`tester_model = 4`). A bateria **reprova** backtest feito nos modelos 1 e 2 |
| O backtest rodava o `.ex5` que estava na pasta do MT5 mesmo com o fonte alterado depois | A bateria lia o código novo e as estatísticas eram do código antigo | O backtest recusa se o fonte ou um include difere do instalado. Cada execução grava a impressão digital do código; a bateria reprova se ela não é a do disco |
| A aprovação da bateria continuava valendo depois de o código mudar | Robô alterado seguia "aprovado" e liberado para demo | A aprovação guarda a impressão digital. Se o código muda, o estágio "Bateria" cai e a trava de demo fecha |
| A bateria de um preset usava o último backtest, qualquer que fosse a configuração dele | Aprovar o preset A com as estatísticas do preset B, ou de um `--param` solto | Novo item: o backtest tem de ser do mesmo preset e sem parâmetro alterado à mão |
| "Stop obrigatório" vinha só da leitura dos parâmetros | Um robô podia declarar o stop e não enviá-lo | O backtest conta, no relatório, as entradas cuja ordem saiu sem S/L; uma só reprova a bateria |
| `--close-terminal` fechava **qualquer** MetaTrader 5 aberto na máquina | Derrubar um terminal de outra instalação com um robô operando | Só o terminal desta instalação é fechado (caminho do executável no Windows, `WINEPREFIX` no Wine). Um terminal não identificado nunca é fechado: o comando para e pede que a pessoa feche. Vale também para o reinício pelo conector |
| O terminal aberto não era detectado no Wine (o processo se chama `main`) | O teste abria uma segunda instância em vez de parar | Detecção pela linha de comando (PR da correção anterior) |
| Duas execuções no mesmo segundo podiam trocar de ordem | "O backtest mais recente" não era necessariamente o último | Horário gravado com microssegundos |
| Número com vírgula de milhar e ponto decimal ("1,234.56") seria lido errado | Resultado de operação errado, em silêncio | Leitura trata os dois separadores; quando há os dois, o último é o decimal |
| Na paridade, o robô media o stop a partir do preço de entrada e o Python a partir do fechamento da barra do sinal; e o robô operava no período de aquecimento | Lotes e saídas diferentes entre os dois lados | O esqueleto passou a fixar o stop no fechamento da barra do sinal e a calcular lote e alvo pela distância real. Os dois lados começam no mesmo instante, com o mesmo capital |

## Paridade medida depois das correções

| Modelo do Tester | Operações (Python × MQL5) | Casadas | Pares com resultado até 5% um do outro | Diferença do resultado total |
|---|---|---|---|---|
| Cada tick | 144 × 144 | 141 (97,9%) | 136 de 141 (96,5%) | 12,2% do líquido; 0,59% do giro bruto |
| Ticks reais | 144 × 144 | 140 (97,2%) | 134 de 140 (95,7%) | 43,6% do líquido; 2,09% do giro bruto |

Em todos os pares o sinal do resultado é o mesmo. O critério da T10.6 (≥ 95% casadas **e** diferença ≤ 5% do
resultado líquido) **não é cumprido** na segunda parte. O percentual sobre o líquido é alto porque o líquido é
pequeno perto do giro (−9.139 sobre 190.777), não porque as operações divirjam.

## Lacunas que continuam

Cada uma com o que ela significa para quem vai operar.

1. **O backtester Python é otimista em custos.** Ele trabalha com barras: usa o spread gravado na barra (média de
   0,57 ponto nas barras H1 medidas) e não vê o spread alargar. Com ticks reais, o mesmo robô perdeu 43,6% a mais
   (2,09% do giro). Parte vem de operações que, com o spread real, saem em outro ponto. **Consequência:** a
   validação das estratégias Python (`mt5x validate`) e o lado Python da paridade favorecem a estratégia. Antes de
   operar, a palavra final é a do Strategy Tester com ticks reais. Mitigação disponível: preencher
   `[STRATEGY] slippage_points` e `commission_per_lot` (a auditoria já acusa quando estão em zero).
2. **Dentro de uma barra, o Python não sabe se o stop ou o alvo veio primeiro** e assume o stop. É conservador, mas
   desloca a saída de algumas operações (3 de 144).
3. **Queda máxima e Monte Carlo usam só operações fechadas.** A queda com a posição aberta não entra.
   **Consequência:** o drawdown real pode ser maior do que o mostrado.
4. **Os limites de perda (diário, semanal, drawdown) não são provados por teste.** A bateria confere os valores
   declarados e, no teste, só a presença do stop. No Strategy Tester a parada por arquivo é ignorada pela própria
   biblioteca (`!m_is_tester`), então também não é testável ali.
5. **A qualidade do histórico do Tester não é lida.** Se a corretora não tem ticks reais do ativo, o Tester gera
   os ticks e o programa não avisa. Confira "Qualidade do histórico" no relatório `.htm`.
6. **A paridade casa pelo horário de saída, sem a direção**, que não é gravada com as operações.
7. **Tempo real em demo (T8.7) nunca rodou em um terminal de verdade.** As chaves do `.ini` de início vieram da
   documentação. `mt5x robots stop` não fecha posições e só é obedecido por robôs com a biblioteca de segurança.
8. **A biblioteca de segurança só foi exercitada no Strategy Tester.** Em conta, demo ou real, nenhum robô rodou
   com ela.
9. **Nada disto foi validado no Windows** (checklist em [validacao_instaladores.md](../validacao_instaladores.md)).
10. **A leitura do relatório depende da forma das tabelas** (13 células por negócio, 11 por ordem), conferida no
    build 6258 em português. Uma mudança de formato do MT5 faria o backtest ler zero operações; o comando
    `robots check` reprova nesse caso, mas o `robots backtest` só avisa.
