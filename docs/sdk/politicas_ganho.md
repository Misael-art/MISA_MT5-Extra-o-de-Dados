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
