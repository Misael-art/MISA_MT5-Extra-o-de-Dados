# T19.15 inicial — saídas temporais

Política tipada temporal_v1 no slot já existente conditional_policy; disabled e os
hashes/campos antigos permanecem idênticos. Não é Ultra, otimizador ou estratégia nova.
S09 v1.30 tem perfil fechado com30 inputs; requer HADOUKEN runtime09068ad e
Safety1.7 aprovados. S09 anteriores/Pullback mantêm perfis, consultas e restrições próprios.
Dependências M: confirmação da parcial na re-simulação (PR153) e varredura segura
de processos/isolamento da fixture nativa (PR155). H#27 integrada em7759344.

Prazos em segundos, desde o primeiro fill, incluindo gaps de relógio: absoluto,
progresso abaixo de lucro mínimo e ganho/perda/empate. Idade>=prazo é inclusiva;
igualdade ao lucro mínimo não é falta de progresso. Ganho acima da faixa; perda
abaixo do negativo da faixa; empate nas duas bordas inclusive. Não se reinicia
idade ao mudar de região, realizar parcial ou recuperar a mesma configuração.
Relógio da fonte explícito, sem assumir fuso do host, calendário/sessão/DST ou
converter barras/Renko em segundos. max_bars legado continua no mesmo fechamento.

Moeda da conta Tester: limiares não negativos em casas explícitas, conferidas no
Expert. HALF_UP em unidades mínimas da moeda. PnL observado inclui custos e lucro
já lançados nos fills, mais flutuante/swap do saldo; não inventa custo futuro da
saída. Ausência é distinta de zero: limite absoluto pode agir sem PnL, condições
financeiras não. SL físico permanece obrigatório, sem aumento de risco/limites.

No OHLC, decisão no fechamento só agenda a próxima abertura. Na abertura, um
SL/TP já cruzado pelo gap precede a ordem temporal. Não consulta high/low futuros
para preencher retroativamente. No nativo, avalia somente ticks disponíveis e
confirma saldo zero/histórico de saída. São bases observacionais distintas, sem
alegar paridade financeira Python×MT5. Os dois motores Python usam a mesma decisão
pura; preservam comissões, swap, parcial, execução e trajetórias existentes.

UI Default/Manual: ativar explicitamente, editar prazos, lucro/faixa e casas da
moeda; salvar/reabrir/exportar. Nova aba mostra instante de decisão/confirmação,
motivo, idade desde o fill, PnL observado e saldo. A causa temporal fica no recibo,
ligada aos fills reais; não reescreve motivo bruto do broker nem resultados antigos.
Perfil/inputs carregados, fonte/includes e executáveis devem ser conferidos antes
do ensaio. Sem negócio ou sem saída temporal confirmada não há aceite dessa capacidade.

Provas automáticas: N−1/N/N+1; igualdade, moeda, ausência, long/short, comissões,
swap, parcial sem reset, precedência de gaps/SL/TP, execução posterior ao fechamento,
max_bars legado e comparação dos motores. Controlador MQL real com plataforma falsa
cobre reinit, hash diferente, âncora ausente, intenção pendente, aceite sem fill,
fill parcial e histórico ausente, sem reenviar. Header real contra Decimal independente.
Isso não comprova reinício físico do processo/OS.

Aceite dirigido Linux/Wine: absoluto, gain/loss, lucro mínimo e empate, cada qual
com quatro posições fechadas no backtest e quatro no replay,30 inputs realmente
carregados e oito fills. Decisão/idade/PnL/redução/caixa conciliados pelo serviço
existente; todos os binários compilados com0 erros/0 avisos e fontes iguais à árvore
identificada. UI Manual salvo/reaberto, prazo inválido bloqueado, consulta de oito
resultados após reabrir banco.42 PIDs próprios observados ausentes; scanner final
vazio nos oito perfis, ponte/principal preservados. Um backtest de progresso terminou
antes de SIGTERM do coordenador: relatório/CSV/inputs reais foram importados sem
reexecutar Tester. Nesse caso só se afirma ausência de recursos após a interrupção,
sem inventar PIDs ou instante anterior. Causa do SIGTERM não comprovada.
Recibo: [T19_15_TEMPORAL_20261020.json](../evidencias/T19_15_TEMPORAL_20261020.json).
São controles funcionais, sem aprovação estatística ou meta universal de tempo;
custos não nulos/falhas/reinit têm cobertura automatizada, sem inferir reinício físico.

Cache inelegível. S09 somente Tester. DEMO ao vivo, Windows físico, carteira e
gradientes mantêm seus aceites próprios; tempo com ER/Donchian/ATR é outra ficha.
