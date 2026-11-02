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

## E14 — proteção comum nativa em qualificação

O novo S09 v2.10 acrescenta `protection.managed_v1`, separado dos perfis
congelados. O perfil exige pacote/sessão vinculados e continua bloqueado até
qualificação nativa. O adaptador Python genérico recusa esse modo; a declaração
do schema ou um sidecar importado não concede suporte.

Exemplo do objeto `protection`, para usuário/agente preparar um Manual:

```json
{"mode":"managed_v1","params":{"stop_mode":1,"stop_points":0,"stop_atr":2,
"trailing_mode":2,"trailing_points":0,"trailing_atr":1.5,"trailing_trigger_r":0.5,
"breakeven_trigger_r":1,"exit_on_opposite":true,"exit_on_session_end":false}}
```

`stop_mode`:0 pontos,1 ATR; `trailing_mode`:0 desligado,1 pontos,2 ATR.
Distância ativa deve ser positiva; distâncias inativas devem ser zero. Trailing
desligado exige gatilho zero. O ATR14 de Wilder usa as últimas250 barras
fechadas, com semente na mais antiga da janela; atualiza somente na nova barra.
Distâncias iniciais são quantizadas para fora do tick **antes** do lote/risco.
Distância abaixo do mínimo nativo, especificação ou ATR ausentes recusam entrada.

Trailing amostra cotação uma vez por barra fechada e só melhora SL. BE por
gatilho usa R real inicial e cobre o preço médio ponderado no primeiro nível
negociável favorável; não cobre custos nem garante execução exata. Parcial/BE
existentes conservam sua confirmação por saldo/histórico. R nunca é recalculado
pelo stop movido. Toda alteração conserva o TP físico e usa intenção estável
do mesmo journal; resultado incerto não autoriza reenvio.

`exit_on_session_end=true` exige `filters.session_v1` explícito. Janela, dias e
exclusão podem solicitar saída; a cota de novos ciclos não solicita fechamento.
Saída por sinal usa a orientação configurada e não reverte na mesma barra.
Precedência: risco → sessão → tempo → alvo virtual → sinal oposto → parcial/BE
confirmados → trailing/BE por gatilho. Pausa mantém a gestão; STOP não prova
liquidação. Reinício só recupera exposição com prova do mesmo vínculo/preset.

Na UI: criar um Manual a partir do novo Default, ativar **Configurar SL, TP,
trailing e BE**, escolher os modos e salvar revisão. Campos da estratégia que
ficaram inativos são desabilitados; valores carregados continuam conferidos.
Orientação pode ser normal/invertida sem ativar trailing ou BE. A API usa a
mesma composição/compilador. Revisão salva vale na próxima sessão; a janela em
execução mantém seus inputs. A prévia de lote com SL ATR fica indisponível até
haver contexto nativo; não substitui risco e margem efetivos do Tester.
