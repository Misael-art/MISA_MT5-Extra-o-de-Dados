# T19.19 — Stop fixo inicial

Recorte inicial necessário ao S09, dependente de T19.18 inicial. Não conclui
T19.19 integral: novos trailing fixo/barras/média, trava de avanço e High-Low
continuam nas dependências respectivas. Trailing ATR existente é preservado.
Break-even/parcial não são liberados no novo adaptador, e Pullback permanece
bloqueado até a confirmação de fechamento T19.57.

`ExitRules.stop_points` define distância positiva/finita em pontos de cotação.
Fonte: especificação conhecida point/tick; distância deve ser múltipla do tick.
O nível de stop nasce **no fill**, após gap/spread/slippage e antes do lote:
compra entrada - distância, venda entrada + distância. O risco até esse nível
alimenta o dimensionamento existente; não há fórmula nova de lote/PnL/custos.
Sem regra fixa tipada, entrada sem stop válido continua inoperável.

Uma intenção de entrada pode existir na barra fechada sem nível de stop, quando
ExitRules tipado garante que a proteção será construída na execução. `gerar_sinais`
não desloca preço futuro nem fabrica stop retrospectivo: suas colunas podem
permanecer ausentes. O motor valida a especificação e resolve o nível antes de
qualquer posição. Override tipado opcional é transmitido ao gerador; fornecendo
sinais prontos, o chamador conserva seus flags, não recupera entradas antes
filtradas por outra política. Sem override, o fluxo anterior permanece válido.

BaseAdapter aceita protection.fixed_points(`stop_points`, unidade point), sozinho
ou com profit_exit em pontos/R. Módulo de proteção substitui só o stop inicial;
alvo do sinal/regra continua até escolha explícita de ganho. Trailing ATR, tempo,
fim do dia e sinal conservam precedência anterior; trailing só avança a favor.
Alvo/stop na mesma barra seguem stop_first. Stop inicial efetivo continua em
Trade.stop_price mesmo quando o trailing avança depois.

Referência independente: compra preenche113, stop10pontos/point1 fica103 antes
de arriscar10unidades com lote1; alvo123. Venda preenche89, stop99 e alvo79.
Conferidos ausência, sinal sem nível futuro, gap/spread/slip, perda no stop/lote,
precisão/tick, CSV, prefixo após acrescentar futuro, override e Pullback bloqueado.
Trailing115-2×2=111 não recua para100 quando ATR aumenta: fechamento111-slip=110,
com stop inicial103 preservado. São preços de teste, não metas financeiras.

142 testes focados aprovados: referências novas, T19.18, backtester, estratégias,
composição e SDK. CI obrigatório final ainda pendente. Catálogo/SDK1/artefatos
anteriores preservados, sem mudar config/dep/instaladores/dados do usuário.
Consumidor H22 já tem contrato nativo de SL/TP fixos inteiros com guardas próprias;
serviço T19.23/perfil S09/carregamento/Default/Manual/UI continuam pendentes.
Este módulo Python não habilita execução nativa por sidecar. Broker stops/freeze
são checados no executor nativo; o modelo Python não os certifica. Windows físico
é pendência específica, sem ordens ao vivo ou campanha numérica nova.
