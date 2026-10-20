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

Journal: cabeçalho HDK_ATR e fonte HDK_ATR_SOURCE em blocos de até800
caracteres de dados, identificados por posição/instante/campo/offset/contagem.
O recibo exige todos os blocos completos e ordenados; rejeita ausência, repetição,
truncamento ou associação a outra observação.

Novo perfil S09, Manual opt-in e recibo próprios. Perfis anteriores preservados;
Pullback temporal não é liberado. S09 somente Tester; live DEMO, Windows físico,
carteira e gradientes exigem aceites próprios. Cache continua inelegível.

## Evidência final e limites

Revisão final: HADOUKEN `52c10013365cf3c73682cc433b82bb82fefa957d`, aplicação
`f5583f7306e395a381fb646279108114689a7391`; S09v1.60, fingerprint `4afcfe4bcef17c18`.
Na mesma janela EURUSD/M5 de14→15/10/2026, lote0,25/SL500/TP1000 e
base600/mínimo60/teto3600s, backtest e replay encerraram4 posições/8fills cada,
com4 saídas ATR,9 contextos e35inputs carregados por execução. Contextos,
livro/fills e caixa7,75USD conciliados; horizontes observados588–626s.
É um aceite funcional do cenário exercitado, sem aprovação estatística.

Fontes,24includes por execução, wrapper e executáveis foram conferidos por hash.
No conjunto das execuções preservadas e da final,36PIDs próprios foram observados
encerrados e8 perfis ficaram vazios. Ponte e terminal principal permaneceram
intactos; não restou marcador nativo ativo. Manual salvo/reaberto, consulta do
resultado persistido e exportação da UI foram conferidos sem outro ensaio.

Dois defeitos encontrados antes da integração justificaram novas verificações:
snapshots extensos ausentes do journal passaram a blocos limitados e completos;
perda isolada da base ATR deixou de recusar todo o controlador. O consumidor
recusou a evidência incompleta e não iniciou o replay dependente. As versões
anteriores e seus resultados permanecem preservados; não substituem o aceite
do fonte final. Testes com header MQL real/plataforma falsa cobrem perda de base,
confirmação falha, fronteiras e reinício. O par nativo final exercitou o caminho
normal, sem provocar corrupção ou crash.

Estado: HADOUKEN #31 integrada em `fe0e6e9`; seis checks obrigatórios da aplicação
aprovados no runtime `f5583f7`, workflow39488078713. Revisão final concluída;
delta posterior apenas documental. Integração do consumidor segue a dependência
H#31 → M#159, conferindo a base/árvore final. Nenhuma campanha adicional.

Recibo público no extrator: `docs/evidencias/T19_16_ATR_20261020.json`.
S09 permanece **somente Tester**. Pullback temporal, live DEMO, Windows físico,
carteira concorrente e gradientes mantêm aceites próprios. Cache inelegível.
Relógios volumétricos/por amplitude não fazem parte desta variante.
