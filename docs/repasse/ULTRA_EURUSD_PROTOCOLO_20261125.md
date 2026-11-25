# Ultra Preset · protocolo EURUSD de 25/11/2026

Protocolo congelado antes de ler resultados do EURUSD. Os limites são de início inclusivo e fim exclusivo, no relógio bruto do MT5. A coleta deve provar o mapeamento desse relógio e das sessões para UTC antes de aplicar qualquer regra baseada em horário.

| Uso | Início | Fim |
|---|---|---|
| Treino | 28/09/2026 00:00 | 07/11/2026 00:00 |
| Validação | 09/11/2026 00:00 | 14/11/2026 00:00 |
| Reserva inédita | 16/11/2026 00:00 | 21/11/2026 00:00 |
| Ensaio técnico de importação e relógio | 23/11/2026 08:00 | 23/11/2026 10:00 |

O ensaio técnico não integra os cortes econômicos. A busca terá até 11 tentativas individuais, uma por estratégia registrada com parâmetros padrão. Hipóteses declarativas novas exigem protocolo próprio. O comparador estático usa o melhor resultado líquido de treino entre módulos elegíveis com expectativa positiva, escolhendo pela chave em caso de empate. A validação é aplicada uma vez. A reserva é aberta após registrar o resultado da validação, a composição e seus hashes. Carteira condicional, carteira estática, caixa, remoções individuais, invalidações e revisão fixa/7/14/15 dias usam o mesmo calendário e orçamento. Se as regras da carteira condicional carecerem de configuração congelada, o braço fica inconclusivo.

Limites máximos para o ensaio de pesquisa e a avaliação DEMO, sujeitos aos limites mais restritivos do Safety e do perfil autorizado, são 0,5% de risco por ciclo e 1% agregado sobre o patrimônio. Todos os braços usam as mesmas cotações executáveis e as mesmas especificações válidas. A contabilização inclui spread observado, tarifas, slippage medido ou hipótese explícita, swap e conversão aplicáveis, e margem demonstrada. Ausência de custo indispensável, história incompleta ou modo de conta incompatível suspende a promoção. A reserva também suspende versões com resultado líquido ou expectativa não positivos, margem insuficiente ou drawdown de patrimônio superior a 5%. Preserve o resultado de caixa, a amostra e a incerteza mesmo quando nenhum módulo passar.

Para conferir mecanismo e tickets, o ensaio técnico deve comparar preço pela grade de um tick, volume pela grade de um passo e caixa pela menor unidade de depósito. A igualdade é exata depois de normalizar. Diferenças de tempo, direção, sequência, transição e titularidade ficam no recibo com o primeiro campo divergente. Campos sem comprovação histórica continuam desconhecidos, mesmo quando o terminal informa valores atuais.

O controle de ciclo desta janela usa um único S09 M1, EMA 5/12, aquecimento de 30
fechamentos desde o primeiro minuto da fita, uma tentativa na primeira condição,
0,01 lote, SL de 20 ticks, TP de 30 ticks, spread máximo de cinco ticks e limite
de 0,5% de risco. O depósito do Tester é USD 1.000. Esse perfil técnico foi fixado
antes de calcular as entradas da janela. As EMAs usam apenas o bid do último tick
disponível na barra anterior. A hipótese de comparação para esta bancada é custo
explícito e atraso zero no Tester. Qualquer custo observado fica no recibo.
