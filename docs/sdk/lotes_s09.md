# T19.56 inicial — lotes S09 no Tester

Em preparação, dependente do corretivo #148 e da HADOUKEN #24. Não é aceite
operacional por aprovação de registry ou compilação. A prova consumidora com
negócios, inputs carregados e eventos em backtest/replay ainda está pendente.

O editor Default/Manual acrescenta modo Por risco ou Volume fixo, volume em lotes
e orçamento em percentual do saldo, até 0,5%. O Default é imutável; o Manual
conserva nome e revisões, exporta o .set efetivo e pode ser reaberto. Quantidade
maior que um significa volume da mesma entrada, nunca número de ordens.

A prévia usa a especificação lida ao reabrir o símbolo e o capital simulado inicial.
Mostra solicitado/normalizado, mínimo/passo/máximo e risco estimado/orçamento.
Não certifica a especificação histórica nem afirma conhecer a margem. Ausência de
prévia não exige resolver IPC para iniciar o Tester: a execução nativa terá de
confirmar os dados reais antes de qualquer entrada. Valores inválidos e recusas
calculadas pela especificação disponível continuam explícitos na interface.

O perfil S09 1.10 usa InpLotMode=1 para risco por saldo ou 0 para lote fixo;
InpFixedLot fica inativo no modo por risco. InpRiskPct é orçamento também no modo
fixo, sem elevar o teto existente. O volume fixo é quantizado para baixo, sem
promover quantidade abaixo do mínimo. O NativeBaseAdapter reusa CalcLot/CheckEntry
além do orçamento do plano. OrderCalcMargin deve confirmar o cálculo e preservar
a reserva de 20%; indisponibilidade nunca significa margem ilimitada.

O recibo confere os doze inputs reais e os flags Tester/DEMO/Visual. S09_SIZING
registra pedido, plano, risco/orçamento, margem e mínimo/máximo/passo efetivos;
S09_SIGNAL confirma a entrada e seu preenchimento. O serviço confere os eventos,
a quantização por Decimal, o CSV/livro/saldo e a primeira divergência com o replay.
A aba Lotes e risco exibe esses fatos; ausência não vira valor preenchido ou
margem zero. Valores de margem no diário têm precisão de oito casas; a conferência
admite apenas 2e-8 de representação, sem alterar a decisão nativa de risco/margem.

Compatibilidade: perfil1.00/fingerprint91ff0ec8a7bba77e permanece reconhecido;
perfil1.10/fingerprint8fcc5dfacc9118fa corresponde à fonte H6120466 e Safety1.7.
Presets de fonte anterior ficam preservados e consultáveis pela identidade,
revisão e fonte originais, sem conversão automática ou execução sob outra fonte.
Não se altera seleção salva silenciosamente; escolha o novo Default para criar
outro Manual. Exportação/execução da revisão anterior exige sua fonte correspondente.

Este recorte é nativo. O adaptador Python mantém seus modos anteriores e recusa
fixed_volume explicitamente; não há alegação de paridade financeira entre motores.
EMA/SL/TP/limites do piloto e o limite de uma posição permanecem. Parciais,
BE/trailing, grids e adições continuam indisponíveis, com Pullback bloqueado até
corrigir e validar sua parcial. Próxima T19.57, reusando o observador que já agrega
saídas/custos por posição para não contar parciais como novas operações.

Cache nativo continua inelegível por falta de revisão histórica verificável.
S09 é exclusivamente Tester DEMO. Execução ao vivo DEMO e Windows físico mantêm
aceites próprios; nenhum resultado histórico autoriza operação ao vivo.
