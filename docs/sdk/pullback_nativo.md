# Pullback — perfil próprio no percurso histórico

O Expert novo `HDK_Pullback_Nativo` consome o adaptador, dimensionamento e mecanismo
de parcial já integrados; o legado `HDK_Pullback` permanece separado e não é promovido.
UI reutilizada: Default imutável, Manual nomeado, parâmetros EMA/ADX/RSI/swing/alvoR,
lote por risco ou fixo sob o limite0,5%, exportação e carregamento verificável.
Agenda limitada a metade do volume inicial em1R, BE após confirmação; sem editor de
outras agendas neste perfil. A viabilidade pelo mínimo/passo/saldo é conferida antes
de enviar a entrada; não habilita parcial indivisível ou BE sem redução confirmada.

SL no swing fechado, arredondamento para fora na grade do símbolo; alvo emR do SL
inicial. O risco depende do swing e da cotação, portanto a UI não fabrica uma prévia
de SL fixo. Lote fixo já permite prévia da grade/viabilidade da metade; margem e risco
reais são conferidos no Tester. Dados atuais ausentes não viram especificação histórica.

O serviço existente persiste inputs reais, sinais, posições, reduções, fills/custos,
resultado e primeira divergência do replay. Para Pullback, confere decisões e preços
contra a implementação Python existente sobre o mesmo prefixo de barras fechadas
registradas pelo Expert. Tolerâncias entre runtimes: EMA/RSI1e-7, ADX1e-4 absoluto na
escala0..100; junto ao limiar ADX, recusa aceite. Isso não declara igualdade bit a bit.
Barra sem mudança de fechamento não é virada RSI; teste Decimal independente do header.

S09 v1.00/1.10/1.20 e seus presets/recibos continuam preservados, sem migração automática.
Os marcadores S09_* são o formato compartilhado de eventos v1; perfil/fingerprint,
magic e recibo identificam inequivocamente o consumidor Pullback. Não são estratégia S09.

Aceite próprio ainda pendente do ensaio final alinhado e dos checks. Mesmo após
aprovação, a prova cobre somente o perfil/contexto/volume e cenários exercitados;
não otimiza a estratégia nem comprova performance, carteira, gradientes ou live.
Cache inelegível; S09 somente Tester; DEMO ao vivo e Windows físico específicos.
