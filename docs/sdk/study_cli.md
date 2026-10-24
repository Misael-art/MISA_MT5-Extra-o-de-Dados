# AIA-02a — catálogo nativo de Estudo por CLI

Extensão do [SDK para agentes](../agente.md), sem automação de mouse/teclado.
Usa a mesma sessão, ownership, limites, IDs de pedido e ACK da AIA-01. Não muda
fórmulas, estratégia, risco, execução, schema, ingestão ou produtores de dados.

## Descobrir e plotar

Com uma sessão visual própria já em `session.wait until=ready`, envie por
`workflow serve`, `workflow run` ou `sdk.open_workflow`:

```json
{"id":"catalogo","method":"study.catalog","params":{"session":"ID_SESSAO"}}
{"id":"bandas","method":"study.indicator.add","params":{"session":"ID_SESSAO","catalog_id":"native:8","timeout":120}}
{"id":"remover","method":"indicator.remove","params":{"session":"ID_SESSAO","indicator":{"$ref":"bandas.indicator"}}}
```

A referência `$ref` do exemplo é resolvida por um manifesto `workflow run`; em
JSONL/SDK, use o ID textual efetivamente devolvido em `data.indicator`.
Para outro gráfico próprio, acrescente `chart` em `study.indicator.add`.
O exemplo `native:8` corresponde a Bands(20,2) na closure HPR75; consulte o
catálogo da sessão antes de escolher, em vez de presumir uma posição universal.

`study.catalog` exige somente `session`. Não cria indicadores nem handles.
Retorna `available`, `catalog_sha256` dos bytes do header da closure do Expert,
`parameter_mode=catalog_defaults`, `source` e `entries`. Cada entrada tem
`catalog_id`, `name`, `scale`, `buffers`, `warmup` e `placement=main|separate`.
HPR75/b730 contém38 entradas. A quantidade e os defaults vêm de
`HDK_StudyCatalog.mqh`, sem uma segunda tabela Python. Sem esse catálogo na
closure, a resposta declara `available=false`, lista vazia e motivo.

`study.indicator.add` exige `session` e `catalog_id`; aceita `chart` e `timeout`.
IDs são textos `native:N` sem zeros à esquerda. A existência do índice é conferida
no catálogo do Expert antes de criar um handle. `period`, `mean`, `deviation`,
`parameters` e caminhos arbitrários de iCustom são recusados: esta entrada usa
exatamente os defaults da fábrica existente. Para SMA/EMA/ATR/RSI/ADX/SuperATR
parametrizados, a API `indicator.add` da AIA-01 continua disponível.

## Recibo e apresentação

A aquisição participa das leases existentes do catálogo. O SDK não chama
IndicatorRelease em aliases compartilhados; mantém o limite de64 tentativas por
sessão e libera os recursos com o processo privado. O indicador é colocado no
gráfico principal quando a escala canônica é preço, ou em uma nova subjanela.
Remoção continua em `indicator.remove`, somente para a identidade criada pelo
controller; `chart.list` informa também seu `catalog_id`.

Além de nome, ID, janela e barras calculadas, o ACK inclui os parâmetros lidos
por IndicatorParameters e `buffer_values` para TODOS os buffers declarados pelo
catálogo, em `buffer_shift=1`. Cada buffer traz índice, quantidade copiada e
valor finito ou null. `value_available` significa que todos tinham valor nessa
barra fechada: Fractals e outros indicadores esparsos podem legitimamente não ter.
Isso não certifica confirmação histórica de pivô, ausência de repaint, paridade
externa ou disponibilidade passada do valor observado agora.

Em PLAY, `parameters_status=observed_catalog_defaults` informa a observação dos
parâmetros, não uma segunda validação de binding: `inputs_verified=false` é
preservado. No Tester, a indisponibilidade de introspecção continua explícita.
Nenhum desses campos substitui o recibo obrigatório do robô em PresetExecution.

O SDK aguarda o marcador de limpeza inicial do layout antes de criar um gráfico
do mesmo símbolo/TF do painel: o host ainda poderia confundi-lo com o auxiliar
de arranque e fechá-lo. Usa o timer do Expert, sem remover o marcador nem editar
o host. Progresso `initial_layout` declara `chart_created=false`; use timeout
suficiente, como120s no primeiro `chart.open`. Em timeout, consulte `chart.receipt`
sem reenviar o efeito. Se o host nunca concluir a limpeza, o pedido permanece
incerto até conciliação ou encerramento da sessão própria.

## Fronteiras de trabalho

Esta tarefa não altera o pilar de dados nem os fontes H do agente do painel.
A closure usada é HPR75/b730, manual7.11/pin45e31481977d09f1; outros perfis mantêm
seus snapshots e pins. Fonte alterada não recebe qualificação automática.

Extensões ainda próprias: parâmetros tipados dos38 presets, indicadores de
terceiros, projeção gráfica das séries Python, anotações/lápis e controle da
reprodução do Tester. OMNI, novo indicador estratégico, armazenamento e estratégia
operacional permanecem contratos separados, com a integração de dados coordenada
pela frente responsável. Windows físico e aceites financeiros/operacionais não
são demonstrados por testes simulados, compilação ou PNG.
