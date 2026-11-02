# T19.18 — Ganho fixo inicial em pontos e R

Recorte necessário ao S09; não conclui o catálogo inteiro T19.18. Percentual,
ATR/médias/VWAP/canais, Fibonacci, agendas de reduções e escolha de módulos na UI
continuam nas respectivas dependências. Não habilita estratégia/preset nativo.

`ExitRules.target_points` é opcional, positivo/finito, em **pontos de cotação**.
Requer especificação conhecida de símbolo, point e tick, com distância múltipla
do tick. Não infere unidade de preço por um nome de ativo nem toma dinheiro por
pontos. Um dado ausente/inválido produz erro com ação de correção. Pontos e R
não podem ser duas fontes simultâneas em ExitRules.

Âncora: preço efetivo de entrada do motor, na próxima abertura, **depois de spread
e slippage**. O alvo passa a existir nesse evento; não é nível disponível na
barra do sinal. Compra: entrada + distância; venda: entrada - distância. Gap
não transforma o close do sinal em preço futuro de entrada. Preço/nível efetivo
ficam em Trade.target_price, usados pela representação/exportação existente.
O lote, moeda, custos e PnL continuam no backtester/SymbolSpec já existentes.

BaseAdapter aceita profit_exit `fixed_points` (`target_points`, unidade point)
e `fixed_r` (`target_r`, unidade R). O modo explícito substitui o alvo do sinal
ou da regra original, por override tipado no motor existente, sem outro simulador.
Proteção/stop, trailing, tempo, fim do dia e sinais de saída continuam com sua
precedência original e podem encerrar antes do alvo. Empate intrabar segue
stop_first. Não se mistura uma escolha de ganho com alteração do stop.
`strategy_default` mantém caminho anterior, sem override ou mudança de cálculos.
Pullback continua bloqueado no novo adaptador até a confirmação da parcial/BE.

O registry fechado1 ganhou um modo declarado, sem mudar estrutura/campos/unidades
anteriores. Os seis retratos base antigos mantêm os mesmos hashes e roundtrip;
leitor antigo recusa o modo novo, preservando artefatos e sem migração silenciosa.
SDK1/capabilities1 permanecem válidos; sidecar externo não habilita MQL5. O adaptador
nativo do aplicativo ainda depende T19.23 e do perfil real; H22 fornece só o
consumidor inicial Tester DEMO de SL/TP fixos inteiros, não todo esse catálogo.

Referência independente: sinal fecha100, compra preenche113 após gap/spread/slip,
10pontos com point1 resultam alvo123, mantendo stop95. Venda preenche89 e alvo79.
Alvo em2R com risco inicial18 resulta149. Conferidos long/short, prioridade do
stop, ausência/tick, números inválidos, conflito, alvo original substituído,
reabertura canônica, prefixo após acrescentar futuro e igualdade no DataFrame/CSV
existentes. São preços artificiais de teste, não metas ou campanha financeira.

Verificação focada de backtester/estratégias/composição/SDK e referências novas
aprovada. A expectativa antiga de alvoR indisponível foi atualizada conservando
os sete casos e verificando suporte por backend; os sete passaram. Fonte runtime
não mudou após a execução focada, demais aprovações foram reaproveitadas. CI
obrigatório da fonte final ainda pendente. Bootstrap/instaladores/config/dependências
não mudaram; evidências compatíveis preservadas, sem repetir doctor/campanhas.
Windows físico permanece específico; nenhuma ordem foi enviada nesta tarefa.

A assinatura pública `sdk.run_backtest` do SDK 1 permanece inalterada. O override
opcional de regras pertence ao motor interno consumido pelo serviço de composição;
o SDK encaminha os argumentos anteriores ao mesmo motor, sem duplicar cálculos.

## E14 — TP planejado e alvo virtual do novo perfil nativo

S09 v2.10, em qualificação e bloqueado para ativação, mantém TP físico em pontos
e acrescenta dois modos explícitos. O `fixed_r` Python acima conserva seu
contrato pós-fill; não recebe suporte nativo por equivalência de nome.

`profit_exit.planned_r_v1` aceita `target_r` positivo, unidade **planned_R**.
O TP físico é calculado com a distância do SL inicial planejado antes do
envio. Slippage/gaps podem alterar o R realizado. Não é o R usado pelos
gatilhos de parcial, trailing e BE: esses usam fills e SL original do broker.

`profit_exit.dynamic_atr_v1` aceita `atr_multiplier` (ATR), `minimum_r` e
`maximum_r` (R real inicial), todos positivos e mínimo≤máximo. Exemplo:

```json
{"mode":"dynamic_atr_v1","params":{"atr_multiplier":2,"minimum_r":2,"maximum_r":4}}
```

O alvo **virtual** é `entrada ± clamp(ATR fechado × multiplicador,
mínimoR × R inicial, máximoR × R inicial)`. ATR14 segue a janela fechada descrita
em [proteção comum](protecao_inicial.md#e14--proteção-comum-nativa-em-qualificação),
recalculada por barra; não se apresenta um ATR congelado na entrada como alvo
que acompanha novas barras. Cotação executável cruzando o nível solicita close
por contribuição no executor/journal existentes. O preço de saída pode diferir.
SL e TP físico de contingência permanecem no broker; esse TP usa o máximo em
R **planejado** antes do fill. O broker pode encerrá-los antes da saída virtual.
Falta de ATR preserva essas proteções, sem fabricar nível ou probabilidade.

Parciais devem anteceder o menor alvo virtual em R. Risco/sessão/tempo têm
prioridade sobre o alvo virtual; este precede sinal oposto, parcial e trailing.
Modificar SL preserva TP. As59 entradas efetivas, fonte, binário, revisão e
sessão são conferidos pelo vínculo F04.6. Testes simulados não qualificam o
percurso nativo, a corretora, restart real ou Windows físico.
