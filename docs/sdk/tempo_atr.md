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

Estado: aceite funcional dirigido corrigido concluído; checks da aplicação/integração pendentes.

Correção do ensaio preliminar: snapshots ATR extensos não apareceram no journal
real do agente, embora a saída tenha ocorrido. O consumidor recusou o aceite e
o replay dependente não iniciou. Fonte passa a usar cabeçalho + blocos limitados
a800 caracteres de dados, com posição/instante/campo/offset/contagem completos;
o recibo rejeita bloco ausente/repetido/truncado ou de outra observação.
O ensaio preliminar é preservado sem aceite. O par final será repetido somente
pela mudança concreta deste protocolo de runtime.

TM07 — aceite dirigido corrigido concluído no perfil1.60/db930f548c8e10d0,
H runtime8b40cac e M runtime295c323. Mesmo EURUSD/M5 14→15/10/2026,
base600/min60/teto3600s; lote0,25/SL500/TP1000 fixados antes do ensaio.
Backtest e replay:4 posições/8fills cada,4 saídas ATR,9 contextos e35inputs
carregados por execução; horizontes588–626s observados, contexto/livro/fills/
caixa7,75USD conciliados. Fonte/24includes/wrapper/binários conferidos.
22PIDs próprios observados encerrados,5 perfis vazios (inclui preliminar/compilações),
ponte/principal preservados. UI: Manual salvo/reaberto, consulta e exportação
conferidas sem novo ensaio. Preliminar recusado preservado sem aceite.
H#31: dois checks corrigidos aprovados; M#159: aguardar workflow39485700757
existente no código295c323. CI92d6aac cancelado após mudança concreta de runtime.
Recibo docs/evidencias/T19_16_ATR_20261020.json. Sem liberação live, Pullback
temporal, carteira/gradientes ou relógios volumétricos; Windows físico pendente.
