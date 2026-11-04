# SDK para agentes e engenheiros — AIA-01

O [Laboratório de séries](sdk/laboratorio_series.md) oferece
`laboratory catalog|sources|freeze|analyze`, descobertos por `commands` e
utilizáveis por `cli.run`. O mesmo diagnóstico v1 é consumido pelo Tk e pelo SDK,
com snapshots/qualidade/corte explícitos; não precisa abrir a tela para calcular
cruzamentos. Presets continuam nos métodos Default/Manual descritos abaixo.

O CLI é a entrada operacional sem Tk, cliques ou automação de teclado. Descoberta e
recibos são JSON; a interface gráfica continua consumindo os mesmos serviços.
Esta entrega compõe BD19c1 e a prévia gráfica/probabilística sem alterar seus cálculos,
store de dados, Budget, journal estatístico, risco ou executor.

## Começar e descobrir

Use o ambiente instalado do projeto. Em Linux, substitua `python` por `.venv/bin/python`;
em Windows, por `.venv\Scripts\python.exe`. Não é necessário abrir o aplicativo Tk.

```sh
python -m mt5_extracao.cli --json commands
python -m mt5_extracao.cli --json workflow methods
python -m mt5_extracao.cli --config config/config.ini --json robots list
python -m mt5_extracao.cli --config config/config.ini --json workflow serve
```

`commands` percorre o argparse real: subcomandos, argumentos posicionais/opcionais,
tipos, escolhas, padrões e grupos mutuamente exclusivos. `robots` conserva sua ação
posicional com `choices`; não são subparsers artificiais. `workflow methods` e
`commands` não abrem banco, MT5 ou GUI. Consulte o catálogo antes de formar argumentos;
os comandos existentes continuam disponíveis diretamente.

`workflow serve` recebe uma linha JSON por pedido em stdin e devolve um recibo por
linha em stdout; diagnósticos vão para stderr/log. Não abre porta HTTP/TCP nem exige
permissão para operar a tela. Uma sessão gráfica visível naturalmente precisa de
display; consulta, pesquisa e backtest Python não precisam de desktop.

```json
{"id":"catalogo","method":"commands"}
{"id":"estado","method":"session.status"}
{"id":"dados","method":"cli.run","params":{"arguments":["tables"]}}
```

Recibo: `{id,ok,data,errors,error_code?}`. Um ID repetido com conteúdo igual na mesma
instância devolve o recibo; para uma nova leitura/poll use ID novo; conteúdo diferente com o mesmo ID é recusado. IDs não
substituem os registros duráveis de experimentos/execução. Não há deduplicação geral
após reinício do controller. Chamadas de uma mesma instância Python são
serializadas, como JSONL; use sessions/consulta de status para operações em segundo plano. Nova tentativa deliberada usa ID novo e, quando aplicável,
o journal de experimentos existente.

## PLAY, REPLAY e vários robôs

`session.review` recebe `robot`, `symbol`, `timeframe` e `preset` opcional. Retorna
fonte/identidade, conta/carteira, risco, cenário negativo, impedimentos e revisão com
prazo de 180 segundos. Não inicia o terminal. Para PLAY, leia esse recibo antes de
confirmar; a conta é novamente consultada no `session.open` e pelo Session existente.

```json
{"id":"revisar","method":"session.review","params":{"robot":"mql5:hdk_hadouken_painel","symbol":"EURUSD","timeframe":"M5"}}
```

Depois use o `review` e o login efetivamente recebidos:

```json
{"id":"iniciar","method":"session.open","params":{"mode":"play","review":"ID_RECEBIDO","accept_review":true,"confirm_account":123,"visible":true,"anchor":[100,100]}}
```

O número acima é ilustrativo, não uma conta padrão. `accept_negative=true` é exigido
quando há cenário negativo; REAL exige `authorize_real=true` **e todos os gates
existentes**, incluindo configuração/bateria/evidência. Estes campos não autorizam
um agente a negociar fora da instrução do proprietário. Exemplos e QA desta entrega
são visuais em DEMO, com gestão probabilística desligada e sem enviar ordens.
A seleção não pode ser substituída depois da revisão; fonte/conta/preset alterados
ou revisão vencida recusam antes da abertura. Painel manual só é reconhecido pelos
pins revisados. Robôs automáticos continuam exigindo suas evidências.

Cada `session.open` devolve `session`, utilizado em `session.status`, `session.wait`
(`until=running|ready|finished`, `timeout` até120s) e `session.stop`. O controlador mantém
até16 sessões próprias e recusa PLAY concorrente com o mesmo magic entre elas.
`session.wait` com prazo esgotado ou estado falho devolve `ok=false`, preservando
o status e o código `wait_timeout|session_not_ready`; novas consultas usam IDs novos.
Um manifesto interrompe os passos seguintes. `session.status` é a consulta sem espera.
Não certifica concorrência de carteira entre outros controllers/terminais.

REPLAY exige visibilidade. PLAY automático é discreto por padrão; painel manual
reconhecido é visível por padrão. `visible` pode ser explicitado. `anchor=[x,y]`
seleciona o monitor; a resolução vem do monitor atual pelo Session/window existente,
com fallback quando o dock é removido. BACKTEST não fornece canal de gráfico visual.

Para fonte MQL comum, histórico exige `mode=replay|backtest`, `robot`, `symbol`,
`timeframe`, `start` e `end` ISO, com fim posterior. Para perfis nativos registrados,
use **pacote verificado**: `mode`, `robot`, `manifest` (caminho do manifesto exportado),
sem substituir contexto/inputs. O serviço chama `PresetExecution`: contrato, pin,
recibo carregado, reconciliação e restrições continuam sendo os mesmos.

EOF/Ctrl-C/context manager encerra somente as sessões iniciadas por essa instância.
`session.stop` encerra a sessão/Expert; **não liquida posições**. Não use STOP como
zeragem de carteira. Encerramento incerto exige conservar o perfil e consultar os
artefatos antes de limpar. Nenhum processo externo é sinalizado pelo controlador.

## Gráficos, indicadores e captura

Aguarde `session.wait` com `until=ready` e confira `chart_control.ready`: processo
`running` ainda pode estar iniciando o MT5. O ACK de prontidão vem de um callback
do Expert, não da existência do processo. Prontidão não certifica histórico sincronizado;
confira barras em chart.list e o PNG. Com a sessão visível pronta, envie `session` em todos os métodos abaixo.
`chart` é ID decimal **textual de64bits**, nunca float; `"0"` significa principal.

| Método | Parâmetros adicionais | Efeito |
|---|---|---|
| `chart.list` | — | Gráficos próprios, símbolo/TF, dimensões, barras e indicadores adicionados pelo controller |
| `chart.open` | `symbol`, `timeframe` | Auxiliar sem Expert, até16 gráficos ativos por sessão |
| `chart.select` | `chart` | Traz o gráfico próprio para frente |
| `chart.close` | `chart` | Fecha somente auxiliar próprio; principal usa session.stop |
| `chart.style` | `style=light|dark|light_gray`, `chart?` | Estilo nomeado; principal requer API de apresentação do Expert |
| `chart.snapshot` | `chart?`, `width?`, `height?` | PNG no sandbox próprio → canal comum UUID → captures da sessão, com ACK |
| `indicator.add` | `kind`, `period?`, `mean?`, `deviation?`, `chart?` | Adiciona, aguarda cálculo/nome final e devolve ID próprio para remoção |
| `indicator.remove` | `indicator`, `chart?` | Remove somente indicador criado pelo controller, com identidade inequívoca |
| `panel.set` | `field`, `value` | Atribui preferência visual idempotente pelo renderizador existente |
| `panel.get` | — | Lê preferências efetivas do painel |
| `chart.receipt` | — | Consulta ACK tardio sem reenviar o efeito |

`kind`: `sma`, `ema`, `atr`, `rsi`, `adx`, `super_atr_10`. Período2..500, média0=SMA/1=EMA
para SuperATR, desvio positivo até100. SuperATR usa DEZ curvas, multiplicadores
`deviation × 1..10`, o asset revisado já existente, sem nova fórmula ou estratégia.
`calculated` informa barras calculadas e `value_available` exige valor finito na
última barra fechada de todos os buffers esperados (dez no SuperATR). Um histórico
insuficiente pode ter cálculo concluído e valores indisponíveis; não é plotagem qualificada.
Limite de64 tentativas de criação por sessão, sem reutilizar ID removido.
O MT5 pode compartilhar IDs com outros componentes do mesmo EA. Por isso o canal
não invalida handles com IndicatorRelease: remove só a apresentação própria e
conserva esses recursos até encerrar a instância privada. O limite inclui recusas
do MT5 e impede retenção ilimitada; retomar libera recursos pelo Session existente.
SMA/EMA ficam sobre os candles; ATR/RSI/ADX nativos usam subjanela. SuperATR exige
asset presente na closure revisada; não se procura binário arbitrário por nome.
O recibo inclui `parameters` lidos por IndicatorParameters no terminal, `inputs_verified`
e `parameters_status`. No PLAY, SuperATR exige que os31 valores matemáticos lidos
coincidam com os solicitados. No Tester ensaiado, IndicatorParameters retornou
indisponível: permite somente a apresentação, com `inputs_verified=false` e
`parameters_status=unavailable_in_tester`, sem afirmar leitura nativa desses inputs.
Isso não altera o recibo carregado obrigatório do **robô** em PresetExecution,
nem qualifica resultados financeiros ou calibração pelo simples gráfico. A chamada
parametrizada de SuperATR inclui os slots das onze declarações input group do
asset1.02, além dos31 valores; isto não altera o contrato/serialização de inputs de EAs.
A ordem foi investigada a partir de [reprodução no fórum MQL5](https://www.mql5.com/en/forum/461183)
e deve ser conferida no readback nativo, sem inferir que compilar prova binding.
[AIA-02a](sdk/study_cli.md) expõe o catálogo nativo de Estudo por `study.catalog` e
`study.indicator.add`, com defaults da própria closure (38 entradas em HPR75),
metadados/readback e remoção pelo mesmo ownership. Indicadores de terceiros e a
projeção gráfica das séries Python continuam extensões próprias, sem fallback por mouse.

`panel.set`: `theme` inteiro0..15; `timer`, `probability`, `gradient`, `watermark`,
`clean`, `bars` booleanos reais. Tema15 é Cinza Claro;7 Porcelana;0 escuro. Atribuição
repetida conserva estado. Não aceita nomes de botões/ações de ordem. A configuração
de probabilidade aqui é somente exibição, não altera `InpProbManage` nem parâmetros
financeiros. Sem API explícita do Expert, o SDK recusa mudar o principal.

```json
{"id":"cinza","method":"panel.set","params":{"session":"ID_SESSAO","field":"theme","value":15}}
{"id":"atr","method":"indicator.add","params":{"session":"ID_SESSAO","kind":"super_atr_10","period":20,"mean":0,"deviation":1}}
{"id":"imagem","method":"chart.snapshot","params":{"session":"ID_SESSAO","width":1280,"height":800}}
```

O canal opcional é incluído somente com `chart_control=True` (SDK de workflows o
solicita). Sessões legadas permanecem sem esse canal. Não há `OrderSend` nele.
PLAY usa captura nativa do chart, copiada ao canal comum UUID e preservada em
captures da sessão. Em REPLAY o backend padrão é `owned_window`: captura pela API
do SO somente a janela de metatester comprovada por PID/prefixo da sessão, incluindo
controles nativos, sem cliques/teclas. Retorna resolução REAL; omita width/height e
chart (ou use0). `backend=native` no Tester é recusado: no build ensaiado o MT5
aceitou ChartScreenShot sem produzir PNG. O recibo identifica explicitamente o
backend; não se chama captura de janela de captura só do chart. X11 usa ImageMagick
(import), já usado pelo diagnóstico; Windows usa PrintWindow e tem aceite físico
pendente. Janela ambígua/alterada ou ferramenta ausente recusam com erro estruturado.
Pedidos são TSV atômicos e respostas JSON em `Common/Files`, separados por UUID da
sessão. Há um pedido pendente por sessão. Timeout não significa recusa: bloqueia
novo comando até ACK tardio conciliado por `chart.receipt` ou recuperação explícita.
Não reenvie cegamente. Gráficos alheios, remoção ambígua e fechamento do principal
são recusados. Esse canal local não é fronteira contra outro processo malicioso com
a mesma conta de SO. Não modifica arquivos/Experts de gráficos externos.

No Tester, comandos dependem de callbacks do Expert; se a reprodução estiver
pausada, a resposta pode esperar retomada. Não há comando de play/pause do próprio
Tester nesta entrega. [Captura nativa](https://www.mql5.com/en/docs/chart_operations/chartscreenshot)
e [anexação de indicadores](https://www.mql5.com/en/docs/chart_operations/chartindicatoradd)
seguem o sandbox e o contexto símbolo/TF do MT5. PNG não certifica execução financeira.

## Presets sem editor gráfico

`presets.default/list` recebem robot/symbol/timeframe. `presets.get` recebe preset_id
e revision opcional. `presets.save` recebe robot/source_id/name e composição opcional;
`presets.revise` recebe robot/preset_id/composition/expected_revision;
`presets.rename` muda só o nome do Manual. `presets.select` fixa revisão no contexto.
`presets.export` recebe robot/preset_id/directory e test_context opcional
(`start`, `end`, `deposit`, `model`); `presets.import` verifica manifesto/.set/fonte.
Usam Default/Manual/store/compiler atuais, sem liberar capabilities inexistentes.

## Manifestos reproduzíveis e SDK Python

```json
{"version":1,"steps":[
 {"id":"lista","method":"commands"},
 {"id":"dados","method":"cli.run","params":{"arguments":["tables"]}}
]}
```

```sh
python -m mt5_extracao.cli --config config/config.ini --json workflow run --file workflow.json
```

Até128 passos, IDs únicos, JSON estrito (sem campos duplicados/NaN/Infinity).
`{"$ref":"iniciar.session"}` referencia um campo de resultado anterior, preservando
seu tipo. Não executa expressão, shell ou interpolação de texto. Falha interrompe os
passos seguintes; recibos anteriores permanecem e não se alega rollback de operações
já concluídas. `run` fecha sessões no fim; use `serve` para mantê-las interativas.

```python
from mt5_extracao.sdk import open_workflow
with open_workflow(db, config, read_account=read_account,
                   config_path="config/config.ini") as workflow:
    receipt = workflow.call({"id":"estado", "method":"session.status"})
```

O callback read_account é do backend/connector existente. Sem callback, o SDK
usa o backend já conectado; o CLI inicializa seu connector com a configuração explícita. `cli.run` requer
config_path explícito e chama somente o CLI público deste Python por argv sem shell;
não permite workflow recursivo. Preserva envelope/código de saída.

## Matriz de workflow e continuidade

| Necessidade | Entrada existente reutilizada / nova |
|---|---|
| Fonte, inputs, risco, catálogo | `robots list/show/params/risk`, `commands` |
| Gráfico/estilo/vários gráficos/indicadores/captura | `workflow serve`, métodos chart/indicator/panel |
| Vários robôs/PLAY/REPLAY/STOP | métodos session; Session/Consent/PresetExecution existentes |
| Default/Manual/pacotes | métodos presets, compiler/store existentes |
| Estudo no laboratório | `trades trajectory/calendar/report/insights/simulate/compare` |
| Bases/indicadores armazenados | extract/update/quality/tables/export e `analytics features ...` BD19c1 |
| Protocolo, alternativas e avaliação registrada | `experiments create/request/evaluate/status/holdout_access` |
| Backtest e validação | `backtest`, `validate`, `screen`, `analyze`, `report` |
| Compilar/instalar/conferir implantação local | `robots install/compile/check` + revisão/start pelo workflow |
| Empacotar/publicar | `robots market` e contrato manual Market existente, sem publicação automática |
| Retenção/recuperação/orçamento | `resources ...`, Budget e stores existentes; sem nova fila/cache |

As ações de robots acima são escolhas posicionais; confira argumentos/ações realmente
disponíveis em `commands`. Esta matriz não redefine orçamento nem admite execução
REAL sem instrução. Operação de lote/experimentos continua no serviço dono, não em
um segundo scheduler de workflows. Manifesto não transforma execuções em amostras
estatísticas independentes.

Próximas frentes registradas, **fora desta implementação**: indicador estratégico e
sua série em banco, maturação do OMNI (contrato ainda a fechar), estratégia operacional,
expansão tipada dos demais estudos/indicadores e controle de reprodução do Tester.
O algoritmo/aceite financeiro desses trabalhos não é inferido pela disponibilidade
CLI. Windows físico, concorrência entre instâncias, lifecycle completo de todos os
Experts e regressão gráfica com vários monitores reais permanecem qualificações próprias.

## Verificações e pendência herdada

AIA-01 foi exercitado pelo CLI real no Linux/Wine: PLAY DEMO (40 pedidos,
incluindo recusas esperadas) e REPLAY (15), PNG1280×800, sem ordens. SuperATR
20/SMA/1..10 e14/EMA/.5..5 tiveram31 valores matemáticos conferidos no readback43
no PLAY; o Tester declarou sua indisponibilidade de introspecção. Testes100
(dirigidos finais) e237 (regressão, sobrepostos); framework136/checker166,
MetaEditor0 erros/0 avisos, doctor20/1opcional/0. [Evidência por hash](evidencias/AIA_01_SDK_AGENTES_20261023.json).

A execução ampla inicial teve2893pass/251skip/3timeouts60s. Na rechecagem,
parity passou; persistiram `tests/test_robots_backtest.py::test_newest_run_first_and_period_start`
e `tests/test_robots_runner.py::test_real_python_backtests_are_recorded`.
O primeiro timeout foi reproduzido no Git congelado da base1d5ce229, anterior ao
AIA-01. Stacks passam por snapshot/decodificação Pandas e pelo runner/monitor;
a causa de desempenho não foi fechada. Não afrouxar testes nem promover isso a
falha atribuída ao SDK. Nova execução ampla passou2952/251skip em2971.93s,
sem DISPLAY e com BLAS/OMP=1; começou antes dos últimos refinamentos, cobertos
pelos100 dirigidos finais. Isso não fecha a causa dos timeouts nem substitui CI
do último HEAD. O delta SDK não altera esses produtores.

[Repasse para outro host/agente](repasse/AIA_01_CONTINUIDADE_20261023.md):
fontes/PRs, instalação, limpeza própria e limites de continuidade.
