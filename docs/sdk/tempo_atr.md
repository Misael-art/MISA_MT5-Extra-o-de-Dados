# T19.16 / TM07 — Horizonte ATR limitado (variante inicial)

Variante explicitada do estudo: ATR14 Wilder canônico (`ewm(alpha=1/14,
adjust=False,min_periods=14)`). Na entrada, congelar a semente e ATR base das
250 barras fechadas imediatamente anteriores ao primeiro fill. Primeiro TR é
high-low, seguintes max(high-low,abs(high-close anterior),abs(low-close anterior)).
A semente é identificada, não equivale a todo o histórico da corretora nem a um
ATR universal. Não altera o registry nem rematerializa dados antigos.

Horizonte = clamp(mínimo, teto absoluto, ceil(base_segundos × ATR_base/ATR_atual)).
Banda de 1 ns antes de ceil evita ruído na igualdade. ATR dobrado contrai o prazo
pela metade, sujeito ao mínimo. ATR menor pode ampliar apenas até o teto absoluto
obrigatório. Todos os prazos contam desde o primeiro fill, nunca da atualização.
Recalcular o ATR atual desde a mesma semente, usando somente barras fechadas
conhecidas naquele instante. No nativo, nova barra é observada imediatamente; correções da mesma barra são
amostradas no máximo uma vez por segundo e só influenciam após serem observadas.
Atingir o prazo fecha independentemente do PnL; as
saídas absoluta/progresso/gain/loss/tie/ER/Donchian mantêm prioridade anterior.

Sem ATR base ou atual positivo/finito, aquecimento/cobertura insuficiente ou
âncora perdida: não presumir ATR nem reiniciar idade. SL e prazos anteriores
permanecem. A base é imutável após parcial/reinício/correção da fonte. Uma correção
recebida produz novo contexto atual, sem reescrever as decisões já registradas.
Adições/gradientes, relógios volumétricos/por amplitude não são suportados neste
recorte. Relógio é segundos reais incluindo gaps, não conversão implícita de volume.

Novo perfil S09, Manual opt-in e recibo próprios. Perfis anteriores preservados;
Pullback temporal não é liberado. S09 somente Tester; live DEMO, Windows físico,
carteira e gradientes exigem aceites próprios. Cache continua inelegível.

Estado inicial: implementação/validação em andamento; sem aceite operacional TM07.
