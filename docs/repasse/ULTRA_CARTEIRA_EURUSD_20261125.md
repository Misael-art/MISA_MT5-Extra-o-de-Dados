# Ultra Preset · carteira EURUSD no Strategy Tester

Continuação da PR 412 sobre o App `69c17a8ea5cf65698457ea89364e0ba3f427ba30`, com HADOUKEN `d3dd3a5e9a680d0c97d58cf160c60c4e58e88a3f` e Safety `27b85992af4b10f4cd7687f370142ac88f515375`. Os arquivos gerados incluem SHA-256 da fonte, das dependências usadas, dos inputs efetivos e do EX5. O código novo está no App.

## Execução do catálogo

A CLI, a UI e o gerador consomem as chaves do catálogo do Laboratório. Por padrão, o Expert inclui as 11 referências e o Super ATR Triplo. `--strategy` seleciona módulos na ordem de avaliação e `--pattern` pode incluir uma definição declarativa. Cada estratégia tem seu próprio magic, parâmetros, sinal, estado e executor. A mesma instância do Tester hospeda o limite agregado de 1% e os limites de 0,5% por estratégia. O preço, a quantidade, a proteção, o ticket e a resposta continuam a passar pelo executor canônico.

| Módulo | Gestão da referência na carteira |
| --- | --- |
| EMA+ATR | Filtro de tendência, stop e trailing por ATR, saída no cruzamento inverso |
| Donchian | Rompimento filtrado, stop por ATR e saída no canal oposto |
| Bollinger/RSI | Stop por ATR, retorno à média e tempo máximo |
| ORB | Stop do outro lado da faixa, alvo por amplitude e saída de sessão |
| Pullback | Stop no swing, alvo em R, parcial em 1R e breakeven após confirmação |
| Squeeze | Compressão Bollinger/Keltner, confirmação por momentum, stop e trailing por ATR |
| S09 | EMA de prefixo ancorado, entrada na barra seguinte e proteção fixa em pontos |
| Estocástico/EMA9 | Cruzamento extremo alinhado à EMA, stop móvel e alvo opcional |
| VWAP | Volume de ticks M1 do MT5, stop de banda e alvo congelado no sinal |
| Madrid | Alinhamento de dez EMAs, stop móvel na média selecionada e alvo |
| HA HiLo | Stop no HiLo e saída pela condição oposta |
| Super ATR Triplo | Prefixo fechado ancorado no primeiro M1 observado, consenso, SL na primeira curva e saída por consenso oposto |
| Hipótese de preço | Comparações OHLC/prior_close na grade, lógica ternária, invalidação anterior ao envio e SL/TP do ensaio |

O catálogo informa aquecimento, dados requeridos, modos de conta e recorte de evidência por chave. A variante gráfica do Super ATR mantém o mesmo cálculo e tem aceite visual separado. O modo novo suporta EURUSD M1, moeda USD, conta DEMO hedging e dias completos no relógio do servidor. A hipótese deste modo exige AST de preço M1, repetição `all`, universo EURUSD e `timezone_calendar_versions.clock` igual a `mt5_raw_clock`. A proteção S09 usa a configuração técnica 5/12, aquecimento de 30 barras e SL/TP de 20/30 pontos. A VWAP usa fonte 1 de volume, entrada em 0,04% e alvo em 0,02%. Essas escolhas foram fixadas para exercitar a integração. O protocolo econômico permanece separado.

## Fita, execução e conciliação

| Evidência | Resultado |
| --- | --- |
| Fita | 23/11/2026 das 00:00 até 24/11/2026 às 00:00, fim exclusivo, relógio bruto do MT5 |
| Manifesto | `ec95b9554d7a20ce98730765647f5dd0aed6c124486721a740a4f019862a51ae` |
| Fluxo | 260.178 cotações, 1.439 minutos com cotação, 949 ocorrências consecutivas no mesmo milissegundo e uma repetição idêntica preservada |
| Comparação | Horários, sequência, bid, ask, last e volumes idênticos entre broker e `OnTick`. O bit `0x400` de flags mudou em 260.175 registros. |
| Primeira execução com 11 módulos | `5971241b251841948daa488bf7d1debb`. A execução e o HTML fecharam. A chamada de auditoria exigia o contexto de um dono. |
| Repetição com auditoria | `1444343cb028419881448d8040148c5a`. Build 6327, MetaEditor sem erros ou avisos, duas posições simultâneas, dez ordens e dez negócios. |
| Encerramento | Saldo e patrimônio de USD 980,12 sobre USD 1.000,00. Ordens e posições abertas zeradas. Journal persistido `d3b27253e953cc10acd33ca5a146af42db9864555cddca551abc6357ea142ad2`. |
| Passe com 13 módulos | `f5eb4270fc374b7599ae5969ac9cb7cb`. Compilação sem erros ou avisos, 18.707 avaliações de barra, duas posições simultâneas, dez ordens, dez negócios e saldo final de USD 980,12. |
| Hipótese de preço M1 | 663 ocorrências idênticas em Python e MQL5, das quais 116 foram invalidadas antes da ordem. |
| Recibo corrigido | A primeira auditoria dos 13 módulos parou na conversão de preços da DLL, que podem estar a um ULP do decimal. A repetição da auditoria sobre a mesma fita e o mesmo journal converteu os preços com a regra de grade já usada na coleta e conciliou o passe. |

As ordens de entrada trazem o SL da estratégia e o alvo aplicável. A conciliação usa tick, passo de lote e centavo de USD como unidades. O HTML do MT5 é cruzado ticket a ticket com os negócios do agente. O pico de duas posições aparece no `OnTick` e na reconstrução cronológica dos negócios. O primeiro passe e sua divergência de auditoria ficam preservados para revisão.

No passe com 13 módulos, Donchian, S09, VWAP e HA HiLo registraram negócios. Cada módulo avaliou 1.439 barras. A amostra exercitou fechamento por sinal e por stop. O recibo inclui 19 planos, 7 respostas de transação e 10 notificações de negócio. A hipótese executou sua decisão e seus vetos no mesmo Expert, sem preenchimento no recorte.

O passe `2b269e7d3bab48ae963cf3260b43342b` de 16 a 20/11 conciliou 989.333 ticks, 32 ordens e 32 negócios. Duas admissões de donos distintos ocorreram no mesmo tick em dois momentos. O saldo e o patrimônio fecharam em USD 956,99 e o journal registrou 72 vetos de orçamento global. A hipótese teve 3.299 ocorrências idênticas no confronto com Python, com 725 invalidações antes de envio.

O controle de gestão de 16/11 registrou saída parcial do Pullback de 0,31 lote a partir de 0,62, seguida de breakeven em 1,10245. O relatório mantém a ordem do journal quando duas ações chegam no mesmo milissegundo. O primeiro ensaio diário conciliou os negócios, mas o `ToDate` do Tester deixou de fora três cotações do último segundo. A primeira correção recuperou a fita completa e o encerramento antecipado do Tester devolveu um HTML sem as linhas de negócios. A versão seguinte encerra a janela financeira na última cotação congelada e deixa o Tester terminar a geração do relatório. O gate compara toda a fita e exige a carteira zerada na fronteira.

O reteste `5407c7d9718840df896954b76998805e` no build 6331 conciliou os 165.488 ticks, os 21 negócios no HTML do MT5 e USD 979,94 de saldo e patrimônio. O controle isolado do Squeeze `de8d085ce7a74ecaacdb869136a85e98` conciliou os mesmos 165.488 ticks, 24 negócios, 45 alterações de trailing e USD 1.010,27 de saldo e patrimônio. Entre a carteira completa e esses controles, houve preenchimentos para todos os 13 módulos do recorte.

O Forex da amostra teve volume negociado zero. A VWAP usa o volume de cotações entregue nas barras M1 do MT5. O relógio histórico e os custos econômicos seguem com o alcance do [protocolo de EURUSD](ULTRA_EURUSD_PROTOCOLO_20261125.md).

## Reprodução no Linux

Use o perfil portátil UltraQA com a DEMO MetaQuotes-Demo salva e o terminal fechado. O launcher lê os parâmetros usuais do perfil. A autenticação pertence ao terminal. Rode pelo Fôlego com uma carga local por vez.

```sh
./run.sh --cli --json laboratory ultra-tester-capabilities
./run.sh --cli --json laboratory ultra-market-verify --snapshot "$FITA"
./run.sh --cli --json laboratory ultra-market-tester-run --mode portfolio \
  --snapshot "$FITA" --out "$RECIBOS" --timeout 600
```

Para incluir a via declarativa, passe `--pattern "$HIPOTESE"`. O JSON exige `definition`, `apply_invalidation`, `stop_points` e `target_points`. A definição é o contrato de hipótese T13.4. No Laboratório, abra **Carteiras Ultra**, **Rodar carteira EURUSD no Tester** e selecione as estratégias e a fita. Cada tentativa preserva as fontes geradas, parâmetros, relatório, journal do agente, recibo e o relatório navegável `carteira.html`. A matriz de cada dono distingue barras avaliadas, sinais, entradas, recusas, saídas, parciais e mudanças de proteção.

## Alcance dos aceites

- A carteira avalia o catálogo de estratégias executáveis indicado acima. A apresentação do Super ATR, a cobertura dos demais operadores declarativos e a paridade de indicadores e gestão de cada referência com Python precisam de aceites próprios.
- A execução EURUSD comprovou ordens em concorrência, posição identificada por dono, proteção inicial, encerramentos e reconciliação financeira no recorte observado. Parciais de plataforma, troca de preset com posição aberta, netting compartilhado e recuperação de processo preservam seus recortes de evidência anteriores.
- O ensaio é técnico. A elegibilidade econômica e a reserva inédita permanecem no protocolo EURUSD.
- A observação prospectiva anterior no gráfico UltraQA manteve caixa e emitiu zero ordens. Os tickets deste repasse pertencem ao Strategy Tester.
