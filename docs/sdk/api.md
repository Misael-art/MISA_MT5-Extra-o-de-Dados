# API Python do SDK (`mt5_extracao.sdk`)

Importe sempre de `mt5_extracao.sdk`. O que está aqui segue versão semântica; o resto do pacote é interno.

```python
import configparser
from mt5_extracao import sdk
from mt5_extracao.database_manager import DatabaseManager

config = configparser.ConfigParser()
config.read("config/config.ini", encoding="utf-8")
db = DatabaseManager(db_type="sqlite", db_path="database/mt5_data.db")

robots = sdk.load_catalog(config, db)
robot = sdk.get_robot(robots, "meurobo")
result = sdk.check(db, config, robot, robots, "EURUSD", "H1")
print(result.ok, [(s.name, s.status) for s in result.steps])
```

As funções de análise não enviam ordens. `open_workflow` pode iniciar Experts via PLAY com os mesmos consentimentos/gates de Session; o Expert mantém seu executor. O backtest MQL5 usa o Strategy Tester e sua reserva exclusiva.

## Versões

| Nome | Significado |
|---|---|
| `API_VERSION` | Versão desta API. Sobe quando algo muda de forma incompatível |
| `CONTRACT_VERSION` | Versão do contrato do robô ([contrato.md](contrato.md)) |

**Política de mudança:** uma função ou parâmetro só é removido ou muda de significado em versão **maior** do pacote,
depois de pelo menos uma versão emitindo `DeprecationWarning`. Parâmetros novos entram no fim, com valor padrão.

## Consulta de dados analíticos (BD18a)

Leitura sem UI e sem MT5 dos fatos já materializados no store; não calcula nem altera fatos ou
definições. A identidade pública pode inicializar o namespace da base (`datasets.identity`, R04).

- `sdk.query_series(db, symbol=..., timeframe=..., feature_ids=[...], as_of=..., start=None, end=None,
  algorithm_version=None, page_size=500, cursor=None)` → envelope `query_schema_version=1` com `base`
  (namespace, linhagem, fonte), `time_context` (estado do fuso), `definitions` (contrato BD06),
  `values` (valor/categoria, qualidade, revisão, `available_at`, versão), `page.next_cursor` (ordem
  estável feature/observação/versão), `freshness`, `reasons` e `errors` estruturados
  (`NAIVE_TIME`, `UNKNOWN_FEATURE`, `BAD_PAGE_SIZE`, `BAD_CURSOR`, `CURSOR_MISMATCH`,
  `SELECTION_TOO_LARGE`, `MISSING_SELECTION`, `BAD_FEATURES`, `BAD_TIME`, `BAD_RANGE`,
  `BAD_VERSION`, `BASE_UNAVAILABLE`, `INVALID_DEFINITION`, `MISSING_DEFINITION`, `STALE_CURSOR`).
  JSON sem NaN/infinito. Cada definição inclui `variants`, por versão, com os metadados registrados;
  campos de resumo só são preenchidos quando comuns às versões selecionadas. Não há conversão
  implícita de unidades nem escolha automática da versão mais recente. Tipos não numéricos
  permanecem em `category`, sem codificação numérica inventada.
- `sdk.query_catalog(db=None)` → contratos BD06 do registry; com banco inclui todas as definições
  e versões persistidas, além da identidade e contexto temporal. A chamada antiga sem argumentos
  permanece válida. `origin`, `computable`, `metadata_state`, `metadata_conflicts` e `name_status`
  distinguem fórmula local, metadados incompletos, divergência e nomes legados não verificáveis.
  Metadados ausentes do produtor permanecem ausentes; o contrato local é exposto separadamente.

O cursor é opaco e temporário: vincula seleção, base, contexto, metadados e conteúdo dos fatos
no corte as-of. Ao receber `STALE_CURSOR`, descarte as páginas anteriores e recomece sem cursor.
Alterações futuras ao corte histórico
não invalidam a paginação desse corte. Cursores v3 substituem v1/v2 (reinicie sem cursor após
atualizar). A página usa limite e chave no SQL; a geração percorre a seleção em lotes e preserva
o teto de200.000 fatos, sem carregá-los todos em um DataFrame. Metadados/fingerprint/página
compartilham uma imagem de leitura. [Contrato e limites](../dados/BD18A_CONSULTA.md#paginação-sql-por-chave-bd18a-sql-29102026).
Compostos e relações permanecem em BD18b.

Não há servidor HTTP: a futura API publicará este mesmo serviço.

## Assinaturas por indicador (BD13-SIGNATURES)

O materializador existente passa a identificar separadamente os três retornos
qualificados `price.return_1/5/20`. Cada versão depende do contrato matemático
do próprio ID, componentes de cálculo/revisão e versões Python/pandas/NumPy;
um calculador alheio não muda essas identidades. Código compartilhado assinado
permanece conservador. Outras famílias ainda usam a assinatura por módulos;
isso não declara todo o catálogo granularizado. [Contrato e limites](../dados/BD06_CONTRATO.md#ordem-das-assinaturas-resposta-ao-r05).

`analytics features context` sem `--algorithm-version` e o contexto de trades
selecionam a versão atual **por ID**. Não se deve aplicar o hash de um retorno
a todos os demais. Se a versão nova ainda não foi materializada, o contexto
declara ausência; não preenche com uma versão antiga. A opção explícita continua
lendo as versões anteriores. Dados, definições, fatos e snapshots antigos não
são renomeados nem sobrescritos.

`sdk.query_catalog(db)` continua descobrindo as versões persistidas. Para
`sdk.query_series`, informe o ID e a versão desejada na seleção; horizontes podem
exigir chamadas com versões diferentes. Sem filtro de versão, o contrato existente
continua retornando todas as versões selecionadas, sem inferir uma versão atual
universal. Capturas usam os pares ID/versão explícitos já disponíveis. Não há
migração automática dos consumidores para fatos recém-calculados nem promoção
de qualidade histórica ou aceite operacional.

## Composições congeladas (BD12-SNAPSHOT)

Adições compatíveis ao SDK1, independentes das políticas de composição de estratégias/trades:

- `sdk.define_feature_composite(db, feature_id, name, algorithm_version, *, operation, inputs)`
  registra uma receita imutável no catálogo existente. `inputs` é uma lista **ordenada** de pares
  `(feature_id, algorithm_version)` já registrados. Retorna `True` se criou e `False` para repetição
  idêntica; alterar receita/nome/unidade na mesma versão é recusado. Não grava valores calculados.
- `sdk.query_composite_context(db, manifest_id, decision_times, *, targets, observation_time=None)` carrega o snapshot
  existente e avalia os alvos, lista de pares ID/versão, nos instantes UTC explícitos de
  `decision_times` (lista/tupla). Nenhum fato ou definição da base atual completa o snapshot.
  CLI e SDK delegam ao mesmo `features.compose_snapshot`, que usa o executor de dependências
  existente; não há motor de expressões ou execução de código fornecido pelo usuário.

Esta etapa oferece `difference` (duas entradas, esquerda menos direita) e `absolute` (uma
entrada). Entradas e saídas precisam ter a mesma unidade declarada, `fraction` ou `price_units`.
Outras unidades/escala restrita, inclusive percentuais, exigem outro contrato: diferença entre
percentuais não é silenciosamente rotulada como a unidade original. Categorias/booleanos,
códigos direcionais categóricos guardados em NUMBER e pivôs com eventos não qualificados são
recusados. Não se transforma resultado numérico em intenção de compra/venda.

Congele **todas** as receitas e versões dos pais usados, inclusive ancestrais. O grafo é validado
antes do primeiro cálculo; receita/ciclo/versão ausente, metadados incompatíveis ou operador
desconhecido geram erro. Limites:32 nós e200.000 pares nó/decisão, incluindo os pais. Cada ID
tem uma versão por avaliação; cenários com versões diferentes do mesmo ID usam chamadas
separadas. É um ativo/TF por snapshot; não há alinhamento implícito entre ativos ou TFs.

Os pais usam o `asof_join` canônico. Só valores AVAILABLE da **mesma observação** podem ser
combinados; estados ausentes/expirados e observações desencontradas tornam o descendente
UNAVAILABLE, com motivo. Zero conhecido continua zero. Disponibilidade do resultado é o máximo
declarado dos pais em cada decisão; uma revisão tardia de observação antiga não substitui a
observação mais recente. Há proveniência por pai/versão/revisão/valor e avaliação de dois níveis.

O envelope atual tem `composition_version=2`, `signature_scheme=frozen_node_components_v1`,
`evaluation_id`, `implementation_sha256`, manifest, alvos, contratos, ordem `executed`, `context`,
`window_utc` e `qualification`. **Receitas continuam no schema1** e snapshots anteriores são lidos
sem reescrever contratos/fatos. Reavaliar com este esquema gera identidades novas; não renomeia
resultados exportados no esquema anterior.

Cada receita calculada tem `computation_sha256` nas definições, canal e proveniência. A identidade
liga contrato próprio, operador nomeado e contratos/identidades dos pais; a de uma folha representa
somente seu contrato congelado, sem certificar o cálculo ou a aquisição na fonte. A proveniência
e `source_revision` do nó incluem sua identidade, mesmo se uma implementação diferente coincidir
no valor numérico. Alterar um operador muda seus descendentes; acrescentar ramo/operador não usado
conserva a identidade dos nós originais. O hash global identifica a união do grafo selecionado.

Componentes AST do grafo, contexto congelado, as-of, representação de Snapshot e regras de tempo
usadas entram na identidade, com Python/pandas/NumPy. Comentários/finais de linha/caminhos e funções
alheias ficam fora. Funções e representação compartilhadas permanecem conservadoras. Componentes
ausentes falham explicitamente. `evaluation_id` também liga manifest, alvos, ordem das decisões,
versão do envelope e janela UTC efetiva. Anotações das decisões não são identidade matemática.

É **contexto exploratório calculado**, sem materialização ou comprovação de sinal recebido
historicamente. `materialized=False` e `operational_acceptance=False`; qualidade desconhecida
da fonte não é promovida. Registro de receitas não faz seus valores aparecerem automaticamente
nas séries armazenadas ou na UI. Materialização/recuperação ficam em BD14 e consumidores completos
nas fichas BD19 correspondentes; os fatos originais e formatos de snapshot permanecem preservados.

```sh
./run.sh --cli --json analytics features define-composite --id qa.diff --name "Diferença" \
  --algorithm-version v1 --operation difference --input qa.left v1 --input qa.right v1
./run.sh --cli --json analytics features define-composite --id qa.absolute --name "Módulo" \
  --algorithm-version v1 --operation absolute --input qa.diff v1
./run.sh --cli --json laboratory freeze --symbol QA --tf M1 \
  --stored-feature qa.left v1 --stored-feature qa.right v1 \
  --stored-feature qa.diff v1 --stored-feature qa.absolute v1
./run.sh --cli --json analytics features compose --manifest MANIFEST_ID \
  --target qa.absolute v1 --as-of 2025-01-01T00:01:00Z
```

As entradas do exemplo precisam já existir, tipadas e com fatos; os comandos não inventam dados.

### Revisões de uma observação congelada (BD14-OBSERVATION)

O padrão acompanha a observação mais recente disponível em cada decisão. Para
reconstruir as revisões de uma observação antiga, informe `observation_time` com
fuso; a CLI oferece `features compose --observation UTC` e
`features context --manifest ID --observation UTC`. A consulta permanece somente
nos fatos congelados. A observação precisa existir nesse histórico e estar na
janela original de barras. Não há fallback para outra observação ou para a base atual.

Esse modo fixa a observação e permite decisões até o fim da janela de barras,
último recibo congelado dessa observação ou vencimento vigente declarado, o que
for posterior. Um vencimento substituído por revisão posterior não estende esse
limite. A janela cobre o histórico conhecido dessa observação, **sem ampliar
cobertura de mercado**. O padrão continua recusando decisões fora das barras.
Recebimento tardio de observação antiga pode mudar seu cálculo sem substituir a
observação mais recente na consulta padrão.

O resultado expõe `context_scope` com modo, observação e janela efetiva. O
DataFrame de `indicator_context.from_snapshot(..., observation_time=...)` guarda
o mesmo escopo em `attrs['snapshot_context_scope']`; o contexto CLI também o
retorna no JSON. `selected_observation_time` distingue a consulta explícita do
valor ausente. Escopo/decisões entram na avaliação; a identidade matemática do
grafo não depende da observação ou do instante escolhidos.

`valid_until_utc` acompanha os pais e os dois níveis de composição; o filho
herda o menor vencimento conhecido. No limite exclusivo de validade o resultado
fica indisponível, sem converter ausência em zero. Um recibo posterior pode
restabelecer o valor. Ainda não grava fatos filhos: esta leitura prepara a
materialização por observação/revisão, evitando usar uma grade de decisões como
se cada valor mantido fosse um evento novo.

### Materialização da cadeia congelada (BD14-MATERIALIZE)

`sdk.materialize_composite_snapshot(db, manifest_id, *, targets, as_of,
batch_size=5000, write_guard=None, cancel_check=None, timeout_seconds=600,
max_memory_mb=2048)` grava os alvos e seus ancestrais compostos no store existente.
`targets` contém pares ID/versão de receitas presentes no snapshot4 junto com
todas as entradas. A CLI delega ao mesmo produtor:

```bash
./run.sh --cli analytics features materialize-composite \
  --manifest MANIFEST_ID --target example.absolute v1 \
  --as-of 2026-01-01T00:00:00Z --batch-size 100
```

`as_of` exige fuso e um instante já ocorrido. O job enumera recebimentos/revisões
e vencimentos vigentes conhecidos até esse corte, por observação. Não usa uma
grade de decisões nem troca uma observação antiga pela mais recente. Prazos
posteriores continuam em `valid_until_utc`, sem gravar antecipadamente uma
expiração que um recibo futuro ainda pode substituir. Entradas totalmente
ausentes não inventam observações. O armazenamento exige precisão exata em
microssegundos; o produtor recusa horários que seriam arredondados.

Receita1 permanece imutável. Cada saída ganha outra versão, com sufixo
`+mat.<hash>`, família `materialized_composite`, identidade completa de cálculo/
projeção e pais com versões explícitas. A versão depende do código/contratos,
sem incorporar manifesto ou corte. Uma saída armazenada pode ser congelada e
consumida como entrada tipada de outra receita. Mantêm-se NUMBER,
fraction/price_units, alinhamento por observação e ausência sem conversão em zero;
nenhuma direção operacional é inferida.
O contexto conserva as versões das receitas avaliadas; os metadados de
materialização também listam as versões de saída dos ancestrais. Eles são
avaliados no mesmo grafo congelado, sem completar entradas com fatos atuais.

O grafo é preparado uma vez por job; execução e as-of são os mesmos da consulta.
Contextos usam a imagem congelada e eventos são avaliados em blocos256. Um
primeiro percurso verifica o histórico e prepara hashes; o segundo confere o
mesmo resultado antes da gravação em lotes. Permanecem os limites32 nós,
200.000 pares nó/evento e200.000 revisões armazenadas na janela; timeout/memória
e `write_guard`/cancelamento usam os mecanismos existentes. O custo de dois
percursos é explícito, sem promessa de aceleração ou escala ilimitada.

Repetir ou retomar reconhece as revisões antigas, preserva seus hashes e seu
manifesto original. Uma captura posterior pode acrescentar correções tardias
sem regravar o prefixo. Antes de acrescentar fatos, o produtor verifica que a
entrada ainda contém os fatos pais já usados pelas origens anteriores. Uma
captura antiga não publica vencimentos sobre uma entrada mais recente; um novo
evento que retroceda o recebimento exige reconciliação explícita. Hashes,
proveniência, revisões duplicadas ou lacunas divergentes não são reparados
silenciosamente. Disponibilidade empatada usa recebimento e depois revisão,
como a consulta SQL canônica.

Fatos/checkpoint são confirmados na mesma transação, com conferência da
identidade da base e da geração de saídas antes e depois dos valores, sob a
exclusão de escrita do store. Esse protocolo presume gravações
append-only pelo store; alterações SQL externas não são um protocolo de edição.
No PostgreSQL, a identidade permanece protegida até o commit, inclusive contra
uma troca de namespace iniciada depois da conferência.
Um escritor concorrente provoca conflito e nova tentativa, sem duplicatas.
Cancelamento/interrupção mantém somente os lotes confirmados; a retomada compara
os fatos, sem confiar apenas num cursor. Importação em outra base continua
legível, mas a escrita exige o namespace original. Nunca há fallback para a
base atual na avaliação congelada.

O run referencia seu manifesto pelo vínculo existente de datasets. Corte,
alvos, versões e limites ficam em `configuration_json`, coluna opcional aditiva
no journal existente; recibos antigos permanecem sem configuração conhecida.
O checkpoint guarda o mesmo manifesto/corte. `source_sequence=0` não declara
uma geração live. O resultado explicita `input_basis='frozen_snapshot'`,
`as_of`, `limits` e `qualification.latest_live_source_certified=False`:
COMPLETE refere-se à entrada congelada e ao corte escolhidos. Não certifica
fonte live, disponibilidade histórica nativa, estratégia ou operação financeira.
SIGKILL e reconciliação de runs órfãos têm o recorte BD14-ORPHAN abaixo;
propriedade não verificável, outros operadores/famílias e qualificação física
continuam nas respectivas frentes do roadmap.

### Avaliação de eventos em lote (BD14-BATCH)

O bloco de cálculo agora pode conter até256 eventos de observações diferentes,
na ordem observação/recebimento. Cada decisão conserva a observação escolhida
e sua própria janela de recibos/validade. Um recibo tardio de A não estende a
janela de B; uma folha ausente não desloca linhas nem usa outra barra. O estado
mantido entre eventos é preservado ao atravessar a fronteira do bloco.

O mesmo contexto e `asof_join` canônicos processam séries já identificadas em
uma junção vetorizada; a expansão parcial/heterogênea conserva o contrato
anterior. Revisões permanecem inteiras, inclusive com linhas ausentes, sem
arredondar identidades maiores que2^53. Grafo/executor/store são os existentes,
sem cache retido, fila ou processo paralelo. `batch_size` continua sendo o
lote de gravação SQL; não muda o bloco interno de cálculo nem os limites.

As duas passagens e os guardas continuam obrigatórios. A implementação do
operador é conferida antes de gravar e concluir; trocar sua função/código no
meio do job aborta, mesmo quando os valores do bloco já foram calculados.
Hashes acompanham o código selecionado: novas versões materializadas não
reescrevem fatos/contratos das anteriores. A receita1 e as assinaturas dos três
retornos qualificados são preservadas. A otimização não amplia a cobertura,
o orçamento ou os aceites financeiros/nativos.

## Workflows de agentes (AIA-01)

`open_workflow(db, config, *, read_account=None, config_path=None, db_override=None)`
é uma API aditiva (`API_VERSION=1`). Devolve context manager com `call`, `run`,
`methods` e `close`: sessões, gráficos, indicadores, presets e adaptação do CLI
existente por JSON. Não importa Tk nem MetaTrader5 ao importar o SDK.
PLAY inicia o Expert existente somente após revisão e confirmação do contexto;
encerrar a sessão não liquida posições. EOF/close encerra somente sessões próprias.
[Guia completo, exemplos e limites](../agente.md).

## Catálogo

| Nome | O que faz |
|---|---|
| `Robot` | Um robô do catálogo: tipo, parâmetros, presets, risco, estágio, `manifest` (o `robot.json`) |
| `load_catalog(config=None, db=None)` | Estratégias Python, pastas de `[ROBOTS] library_dirs` e robôs importados |
| `get_robot(robots, key)` | Um robô pela chave (`"meurobo"` ou `"mql5:meurobo"`). `ValueError` se não existir |
| `robot_from_file(path)` | Lê um `.mq5` ou `.ex5` sem registrá-lo |
| `import_library(db, folder)` | Registra uma pasta de robôs no catálogo |

## Contrato e ponto de partida

| Nome | O que faz |
|---|---|
| `validate_contract(robot)` | Lista de `Problem` (vazia = contrato válido) |
| `Problem` | `field` e `message` de um problema no `robot.json` |
| `new_robot(name, folder, robots=(), config=None, magic=None)` | Cria o esqueleto de um robô (o mesmo que `mt5x robots new`). Devolve os arquivos criados |

## MetaTrader 5: instalar e compilar

| Nome | O que faz |
|---|---|
| `install_robot(config, robot, ...)` | Copia o robô, includes e presets para a pasta do MT5, e a biblioteca de segurança se o robô a declara |
| `compile_robot(config, robot, ...)` | Instala e compila no MetaEditor. Devolve `(arquivos, CompileResult)` |
| `CompileResult` | `ok`, `errors`, `warnings`, `messages`, `summary`, `compiled` |

## Backtest

| Nome | O que faz |
|---|---|
| `run_python(db, config, robot, symbol, timeframe, start=None, end=None, params=None, ..., real_spread=True, cashflows=None, data_manifest_id=None, control=None)` | Backtest de uma estratégia Python com revisão fixada das barras. `real_spread=False` cobra o spread das barras em vez do medido nos ticks (é o que a paridade usa) |
| `run_mql5(db, config, robot, symbol, timeframe, start=None, end=None, preset=None, params=None, ...)` | Backtest de um robô MQL5 pelo Strategy Tester. O MT5 precisa estar fechado |

Os dois gravam a execução e devolvem um dicionário com `metrics`, `equity` e `trades`. Pela CLI, `--symbols A,B,C`
roda o mesmo teste em vários ativos.

Desde T20.3, `run_python` e `run_backtest` aceitam `cashflows=None` ao final.
O padrão é ausência de fluxos, preservando chamadas posicionais e padrões
anteriores (`API_VERSION=1`). Um vetor fornece uma quantia por fechamento de
barra, na moeda da conta; Series com índice temporal precisa corresponder às
barras do período selecionado. Depósitos são positivos e saques negativos.
Fluxos alteram caixa e o risco composto da próxima barra, sem entrar no PnL.
Valores inválidos, patrimônio/cotas não positivos no fluxo ou resgate integral
produzem `ValueError`, sem gravar um run parcialmente calculado.

O retorno Python acrescenta `account_balance`, `floating_equity`,
`performance_equity`, `account_cashflows` e `valuation`. No dicionário gravado,
curvas são pares horário/valor; fluxos são eventos esparsos (lista vazia significa
sem fluxo). A série de desempenho usa cotas para o TWR e para o novo drawdown;
`floating_drawdown_basis` identifica se o valor monetário é do patrimônio da
conta ou equivalente normalizado ao capital inicial. `equity` e
`max_drawdown_pct` antigos mantêm significado de operações encerradas, e
`return_pct` antigo continua PnL sobre capital inicial, distinto do novo
`time_weighted_return_pct`. A marcação é no fechamento, sem observar risco
intrabarra; status/limitações ficam em `valuation`. Run legado sem trajetória
retorna `None` nos novos campos e motivo, sem regravação. Veja
[marcação, fluxos e prova nativa T20.3](../analytics/T20.3_PATRIMONIO_FLUTUANTE.md).

Desde T20.4, `run_python` acrescenta ao final `data_manifest_id=None`. Por padrão,
captura barras/especificação/calendários numa imagem consistente e fixa sua
identidade no run. Com um ID anterior, lê aquela revisão, sem usar dados atuais;
ativo/TF e limites explicitamente informados devem corresponder à seleção do
manifesto. As grafias equivalentes de data/timeframe são normalizadas.
Os retornos acrescentam `data_manifest_id` e `data_provenance_status`. A origem
local e servidor desconhecido ficam explícitos; hash não certifica corretora,
qualidade, causalidade ou estratégia. Referência ausente/corrompida gera erro.
Runs legados sem revisão continuam identificados como `legacy_current_data`.
Veja [manifestos e reprodução dos dados](../analytics/T20.4_MANIFESTOS_DADOS.md).
O contrato 1 permanece compatível: o argumento novo é opcional e vem após os
anteriores. A reprodução de código/custos/configuração/protocolo é tratada nos
donos T20.12/16/25, além desta identidade de dados.

Desde T17.28, `run_mql5` aceita ao final seis opções compatíveis com chamadas
anteriores: `visual=None`, `read_csv=True`, `trades_csv_override=None`,
`compiled_override=None`, `export_trades=True`, `tester_port=None`. Sem essas
opções, preserva o fluxo anterior. `visual=True/False` explicita modo visual e
somente agentes locais; `tester_port` escolhe porta local de 1024 a 65535.
`read_csv=False` desliga a leitura do CSV legado; `export_trades=False` impede
que o parâmetro de exportação reconhecido seja forçado a true.

As sessões dos cards passam seu binário instrumentado em `compiled_override`
e seu CSV exclusivo em `trades_csv_override`, dentro da instalação portátil
preparada. CSV já existente é preservado e recusado; CSV ausente ou livro que
não reconcilia com a variação do saldo nativo impede gravar o resultado. Essas
opções não preparam/isolam terminais por si mesmas nem autorizam contas.
`API_VERSION=1` permanece: nenhum parâmetro anterior mudou de posição, padrão
ou significado. Fonte, parâmetros e binário devem corresponder ao mesmo Expert.

## Bateria e ciclo completo

| Nome | O que faz |
|---|---|
| `run_battery(db, config, robot, robots=(), preset=None, deep=False, ...)` | Bateria de segurança sobre o último backtest. Com `deep=True` (robô MQL5), repete o Strategy Tester para medir fora da amostra e robustez. Devolve `BatteryReport` |
| `BatteryReport` | `items`, `approved`, `allows_real`, `static_status`, `statistical_status`, `to_dict()` |
| `BatteryItem` | `group`, `id`, `name`, `status`, `detail`, `fix` |
| `check(db, config, robot, robots, symbol, timeframe, ...)` | Contrato, compilação, backtest e bateria, parando na primeira falha. Devolve `CheckResult` |
| `CheckResult` | `steps`, `ok`, `report` (a bateria), `to_dict()` |
| `write_junit(result, path)` | Grava o `CheckResult` em JUnit XML |

## Relações referenciais congeladas (BD15a)

`sdk.query_feature_relation(db, manifest_id, *, left, right, as_of, lag_bars=0,
method="pearson", minimum_pairs=30, minimum_sessions=10, bootstrap_replicates=999,
seed=7, block_length_sessions=None, tests_counted=None, synthetic_fixture=False,
timeout_seconds=120, max_memory_mb=2048, cancel_check=None)` consulta dois pares
`(feature_id, algorithm_version)` presentes no mesmo snapshot4. CLI equivalente:

```bash
./run.sh --cli --json analytics features relation --manifest MANIFEST_ID \
  --left example.x v1 --right example.y v1 --as-of 2026-01-01T00:00:00Z \
  --method spearman --lag-bars 0 --seed 7
```

Usa somente fatos/contratos congelados e o contexto causal existente. Composto
materializado precisa de seus manifests de origem imutáveis; ausência ou
proveniência não verificável produz inconclusão, sem completar com tabelas live.
Pivôs armazenados sem qualificação de eventos ficam fora das relações. Categorias,
booleanos e tipos não declarados não recebem coeficiente contínuo por conversão.

Defasagem positiva liga a esquerda ao fechamento nominal anterior indicado,
exigindo barra de referência e mesma sessão; não comprime lacunas. Sessões vêm
somente do calendário congelado conhecido no corte, nunca da data civil inferida.
População, exclusões exclusivas e fontes distintas são informadas; contagem de
fontes distintas não certifica amostras independentes. As-of exige fuso e corte
já ocorrido. Limites:100.000 decisões,32 snapshots de linhagem, até5.000 réplicas,
com os guardas existentes de tempo/memória e cancelamento, sem resultado parcial.

O DTO2 `FEATURE_PAIR_RELATION` conserva descritores esquerdo/direito, unidade,
versão, método, defasagem, população, hashes e proveniência. Não inventa horizonte
de alvo futuro; o DTO1 e sua fixture continuam compatíveis. Pearson/Spearman e
inferência temporal reaproveitam o módulo canônico de associações T18.2. P-valores
não são promoção de evidência: o resultado permanece exploratório, utilidade
inconclusiva e sem causalidade/aceite operacional. Bonferroni só é aplicado quando
`tests_counted` declara a família; não substitui o registro de tentativas/OOS.
Não persiste relações, executa estratégias ou envia ordens; BD15b e consumidores
completos continuam nas frentes próprias.

## Recuperação de jobs órfãos (BD14-ORPHAN)

`sdk.recover_producer_jobs(db, *, symbol=None, timeframe=None, family=None)`
reconcilia o journal existente. CLI equivalente:
`analytics features recover-jobs [--symbol QA] [--tf M1] [--family família]`.
O SDK recebe o timeframe canônico do journal (por exemplo `M1`); o CLI normaliza
os nomes aceitos. Sem filtros, examina todas as famílias da base escolhida.
Retorna escopo, quantidade examinada, IDs recuperados, quantidade preservada e
instante da reconciliação. Não executa cálculos nem modifica fatos/checkpoints.

Novos runs guardam identidade local: hostname, escopo da máquina e geração do
PID. O escopo é um hash, sem identificadores brutos: Linux inclui máquina,
boot e namespace de PID; Windows usa MachineGuid e a geração do PID. Outra
máquina/escopo, acesso negado, informação inválida ou ausente preservam o run.
Registros legados sem escopo verificável também permanecem intocados. O tempo
decorrido não é prova de morte. Mudança de boot/namespace Linux mantém o aceite
de recuperação pendente; não há roubo de uma execução remota por timeout.

Somente `RUNNING` com proprietário local comprovadamente encerrado vira
`INTERRUPTED`, por atualização condicional que preserva término concorrente.
`recovered_at_utc` informa a descoberta; `finished_at_utc` e duração não são
inventados. Contadores SQL legados `NOT NULL` são preservados como placeholders;
`producer_jobs.latest_run` apresenta suas métricas finais como `None` e
`metrics_state=UNAVAILABLE`. A recuperação não certifica quantos fatos o job
gravou: os lotes/checkpoints confirmados continuam sendo a referência.

Ao iniciar outro run, o mecanismo reconcilia automaticamente apenas seu
ativo/timeframe/família. A retomada continua no produtor original, com suas
guardas de origem, geração e deduplicação. Não existe nova fila, scheduler,
executor, takeover de processo ativo ou encerramento de terminal.

## Paridade Python × MQL5

| Nome | O que faz |
|---|---|
| `compare_parity(python_trades, mql5_trades, tolerance_minutes=60)` | Casa as operações pelo horário de saída e devolve `ParityReport` |
| `run_parity(db, config, robot, strategy_robot, symbol, timeframe, ...)` | Roda os dois backtests no mesmo período e compara. Devolve `report`, `python_run` e `mql5_run` |
| `ParityReport` | `matched`, `matched_share`, `pairs_close_share`, `difference_of_gross`, `median_ratio`, `only_python`, `only_mql5`, `same_sign`, `result_difference` (informativo), `ok` |

## Estratégia declarada e geração do robô MQL5

| Nome | O que faz |
|---|---|
| `SignalSpec` | A regra de entrada como dados: indicadores (`sma`, `ema`, `rsi`), condições, stop em ATR e alvo em R. `SignalSpec.from_dict(...)` valida |
| `DeclaredStrategy` | Executa uma `SignalSpec` no backtester Python (mesma interface das outras estratégias) |
| `generate_robot(spec, name, folder, robots=(), config=None, magic=None)` | Cria o robô MQL5 com o sinal gerado (o mesmo que `mt5x robots generate`) |

Não é um tradutor de Python: só o que está na declaração é gerado. A prova de cada robô gerado é a paridade.

## Visão de conjunto

| Nome | O que faz |
|---|---|
| `build_matrix(db, config=None, robots=None)` | Ativos com base, timeframes, robôs e uma célula por (robô, ativo, timeframe) com a execução mais recente, a origem do número e a situação (`aprovado`, `reprovado`, `promissor`, `poucas operações`, `negativo`). Só leitura |
| `matrix_positives(matrix, robot_key)` | Onde o robô teve resultado positivo, os aprovados primeiro |

"Promissor" é lucro com 30 ou mais operações **sem** bateria nem validação: não é aprovação. É o que o painel
(`mt5x panel`) mostra.

## Estratégias Python

| Nome | O que faz |
|---|---|
| `Strategy`, `StrategyInfo` | Classe base de uma estratégia e a sua ficha ([estrategias.md](../estrategias.md), "Para desenvolvedores") |
| `get_strategy(key, **params)` | Instância de uma das estratégias registradas |
| `BacktestConfig` | Capital inicial, risco por operação, slippage e corretagem |
| `run_backtest(df, strategy, spec=None, config=None, start_index=0, signals=None, cashflows=None, check=None, observation_timeframe=None)` | Backtest com custos e lote pelo risco; `start_index` e `signals` servem à validação, e `cashflows` identifica fluxos no fechamento |
| `validate_strategy(df, key, spec=None, config=None, criteria=None, params=None)` | Walk-forward, robustez e Monte Carlo (Filtro C) |


T20.5 qualifica a especificação consumida com `instrument_context`,
`spec_historical_known` e `series_role` nas métricas e metadados de avaliação.
`spec_known` conserva disponibilidade da especificação, sem comprovar vigência
histórica. Manifestos novos usam formato 2, preservando leitura/exportação do
formato 1. APIs públicas e chamadas posicionais permanecem as mesmas; metadados
opcionais foram acrescentados à dataclass interna `SymbolSpec`.
[Regras de vigência, fontes e limitações](../analytics/T20.5_VIGENCIA_INSTRUMENTOS.md).


T20.6 acrescenta `data_quality` às métricas Python/MQL e à avaliação de
re-simulação. Capacidade de dados, requisitos do consumidor e limitações de
origem/cobertura são explícitos; relatório nativo sem requisitos do Expert
não o qualifica automaticamente. `run_python` mantém os argumentos anteriores e exige
OHLCV para o motor, além das `required_columns` opcionais da estratégia.
`Strategy.required_columns` tem o padrão anterior; componentes de fechamento
podem ser qualificados separadamente de OHLCV completo.

Ferramentas avançadas: `analytics.data_eligibility.requirements/evaluate/as_of`
consomem os contratos/provas existentes, sem outro registry ou feature store.
`datasets.capture(..., families=["ticks", "book"])` captura fontes extras
explicitamente; `Snapshot.quality/market_frame` permitem inspeção da revisão.
Formato3 lê/exporta1/2 sem converter metadados livres antigos em prova atual.
[Regras e fronteiras reais](../analytics/T20.6_QUALIDADE_CAPACIDADES.md).


## Orçamento cooperativo de lotes (T20.24)

`robots.backtest.run_batch` e `robots.runner.BatchRunner` usam o mesmo
`robots.budget.Budget`, sem outro agendador. `batch_id` identifica o checkpoint
operacional. `Budget.prepare` fixa o plano e os manifests Python; `checkpoint`
consulta estado/histórico/medições. A retomada recupera os run_ids publicados,
sem reexecutar nem regravar o resultado. Falhas permanecem no histórico. Esse
checkpoint não é um registro estatístico de alternativas/consultas à reserva.

`run_python(..., control=None)` e `run_mql5(..., control=None)` acrescentam um
`JobBudget` opcional; `run_backtest(..., check=None)` aceita callback cooperativo
que pode levantar uma interrupção. Os argumentos anteriores continuam na mesma
ordem e todos os novos defaults são `None`. A publicação controlada em
`store.save_run(..., control=...)` grava o resultado e o checkpoint do item na
mesma transação, com o run_id reservado. Cancelamento não publica um resultado
parcial. O limite de admissões permite terminar os já admitidos; tempo/CPU/RAM
ou cancelamento explícito interrompem no próximo ponto cooperativo.

O motor confere antes/depois dos sinais e entre blocos de até 256 barras; uma
chamada atômica de indicador não é interrompida internamente. O executor nativo padrão usa a sessão privada T20.25, confere o controle
durante a espera e encerra somente seus processos. Executores nativos injetados
continuam sujeitos ao próprio contrato: a fila confere o orçamento após o retorno,
sem promessa de interrupção interna de uma chamada arbitrária. Executores injetados pelo argumento legado `run` do `BatchRunner` não
recebem o callback novo; precisam adotar o protocolo controlado para publicação
recuperável. A fila nunca certifica efeitos externos de um executor arbitrário.

Limites `[ROBOTS]`, seus defaults e a distinção da concorrência estão em
[T20.24](../analytics/T20.24_ORCAMENTO_FILA.md). Orçamento e status aparecem na
CLI/UI; Windows físico continua no checklist de validação.


## Cache e perfis privados (T20.25)

`trade_analytics.cache.features` mantém a API anterior e verifica identidade e
integridade antes de reutilizar contexto. O schema acrescenta colunas de checksum
e contagem, sem apagar tabelas existentes. `robots.backtest.already_tested` aceita
configuração, período e manifest para conferir a identidade Python; legado sem
prova retorna ausência de hit. Resultado nativo não vira cache entre experimentos.
A recuperação do mesmo checkpoint permanece disponível e informa `reuse_basis`.

`run_mql5` mantém a assinatura e passa pelo executor `Session` existente, com
terminal/perfil privado por tentativa. Metadados distinguem `run_id`, `artifact_id`,
`artifact_directory`, `native_profile_isolated` e captura por observador ou
relatório. `is_running` injetado continua sendo uma recusa explícita do executor;
o terminal operacional aberto não é impedimento automático no caminho padrão.

`resources.files.list_sessions(db, config)` inspeciona os perfis canônicos;
`move_session(db, config, session_id, restore=False, origin="ui")` move para
quarentena ou restaura, retornando `ok/path/error`. Nenhuma dessas APIs apaga um
perfil. Referências, ownership, encerramento, links e processos são conferidos;
a movimentação reversível usa a auditoria F15. A CLI correspondente está em
`mt5x resources sessions --help`.

[Contratos, critérios de reuso, limites de binários e validação](../analytics/T20.25_CACHE_ISOLAMENTO.md).


A preparação privada aceita o callback cooperativo existente de orçamento e o
consulta com volume de destino/reserva antes de copiar arquivos. O cancelamento
é observado entre arquivos; a espera por processos mantém suas checagens atuais.
A posse começa antes da preparação, permitindo recolher uma cópia recusada antes
da compilação quando nenhum processo permaneceu ativo.


O manifesto da sessão contém `owner_store`, vínculo sem URI/senha ao banco criador.
`list_sessions/move_session` preservam perfis de outro banco e perfis antigos sem
vínculo verificável, mesmo quando dois bancos usam o mesmo prefixo Wine.

## T19.14 — Observações e consulta de trajetórias

`run_backtest(..., observation_timeframe="M15")` e
`execution.engine.simulate(..., observation_timeframe="M15")` acrescentam
`result.trajectory`. O argumento é opcional no motor bruto; a publicação normal
Python e re-simulações salvas já registram o diário. Preenchimentos e cálculos
legados são preservados. O diário modelado distingue abertura, fechamento
nominal e intervalo intrabarra; não é exportação de ticks reais.

`trade_analytics.trajectories.service.query(db, run_id, seq=None, as_of=None,
unit="money", thresholds=(), ages=())` é a consulta de serviço do CLI
`mt5x trades trajectory`. Ela não executa estratégias nem ordens.
[Contrato e estado do recorte](../analytics/T19.14_TRAJETORIAS_TEMPORAIS.md).
O contrato MQL5 1 permanece o mesmo. `observation_timeframe=None` é opcional
no fim da assinatura de `sdk.run_backtest`; chamadas posicionais anteriores
continuam válidas e `API_VERSION=1` é preservada.

T19.24 mantém também a assinatura publicada de `sdk.run_python` por um wrapper
explícito, como em `sdk.run_backtest`. O argumento interno `composition` não
é promovido ao SDK 1; todos os quinze argumentos e defaults anteriores continuam
encaminhados ao mesmo produtor. O novo consumidor composto usa o serviço/CLI
versionado `composition_v1`, descrito em [backtest composto](composed_backtest.md).


### Persistência de relações referenciais (BD15b)

`sdk.persist_feature_relation(db, manifest_id, *, left, right, as_of, lag_bars=0,
method="pearson", minimum_pairs=30, minimum_sessions=10, bootstrap_replicates=999,
seed=7, block_length_sessions=None, tests_counted=None, synthetic_fixture=False,
timeout_seconds=120, max_memory_mb=2048, cancel_check=None, write_guard=None)`
calcula pelo serviço BD15a e publica um artefato no `analytics.feature_store`.
Retorna `status` (`INSERTED`/`UNCHANGED`), `analysis_id`, `payload_sha256`,
`created_at_utc` e o DTO original em `relation`.
`sdk.load_feature_relation(db, analysis_id)` lê esse DTO sem recalcular.

CLI: acrescente `--persist` a `analytics features relation`. Para recuperar,
use `analytics features relation-result --analysis-id ID`. Os mesmos argumentos
globais `--config`/`--db`/`--json` selecionam a base e a apresentação existentes.
Sem `--persist`, a consulta continua somente leitura e não cria a tabela.

`_feature_relations` é uma tabela aditiva no armazenamento canônico. Resultado,
hash e referências de todos os manifests ancestrais são confirmados na mesma
transação; falha/cancelamento deixa nenhum artefato parcial. Escritores repetidos
ou concorrentes preservam o primeiro resultado e seu relógio de geração. Mesmo
ID com conteúdo semântico diferente é recusado, sem sobrescrever o original.
Método, população, parâmetros, defasagem, seed, versões, revisão congelada e
implementação compõem a identidade; recursos e relógio da execução ficam no
primeiro recibo e não abrem outra identidade. A repetição sob orçamento diferente
devolve explicitamente o recibo original. A extração do helper de identidade
altera a assinatura da implementação de pares; resultados anteriores não são
reescritos nem promovidos para a nova assinatura.

O carregamento confere hashes, índices, contrato e referências/origens imutáveis.
Origem ausente exige restauração/importação; não há complemento com dados ativos.
Hashes verificam consistência dos bytes, não autenticidade criptográfica de
um banco alterado por terceiros. O limite do artefato é16MiB e32 manifests;
limites de cálculo BD15a, cancelamento e guarda de escrita são preservados.
Persistir um resultado insuficiente preserva sua inconclusão. Isto não cria
registro de tentativas, aceitação OOS/financeira, novos jobs ou adaptação da UI.


### Consulta de relações salvas no Laboratório (BD19b)

`sdk.query_saved_relations(db, *, symbol=None, timeframe=None, analysis_id=None, page_size=50,
cursor=None)` consulta o índice de DTO2 persistidos pelo serviço BD15b. Retorna
envelope `status`, `items`, `matched`, `selection`, `base`, `page` e `errors`.
CLI: `analytics features relation-list --symbol QA --tf M1 --page-size 25`;
`--cursor` percorre a próxima página. Filtros são identidades exatas; vazios na
GUI mostram todos os recortes da base selecionada.

A consulta reutiliza os cursores/geração do serviço canônico, aplica `LIMIT`
antes de ler os payloads e processa um artefato por vez. Até200 itens por página
e200.000 metadados por seleção; acima disso, filtre ativo/timeframe. Base,
conteúdo ou filtros alterados invalidam o cursor, sem misturar páginas. Não
cria schema nem executa cálculos. O resumo confere o hash/contrato do artefato;
a seleção pelo `load_feature_relation` confere também os snapshots de origem.
Falha de integridade retorna erro sem resultados parciais.

No Laboratório → Relações e aderência → Matriz → Relações salvas, consulte,
selecione e leia o coeficiente em destaque, séries/versões/unidades, amostra,
exclusões e qualificação. Detalhes expõem identificação, corte, intervalo e
origens. Carregamento usa `run_bg`/indicador de processamento existentes;
respostas antigas não substituem um novo filtro ou seleção. A subvisão é
somente leitura de resultados persistidos, conserva o selo sintético e não
recalcula nem promove evidência, lucro, causalidade ou autorização operacional.
Criação visual de novos pares, demais alvos e exportação completa permanecem
nas frentes próprias.


### Seleção congelada para criar pares (BD19b-CREATE)

`sdk.inspect_feature_snapshot(db, manifest_id)` retorna os contratos exatos da
captura, seu recorte e compatibilidade numérica declarada. Não busca definições
ou versões atuais e não calcula indicadores. Falta de captura/integridade é
erro explícito; captura sem versões armazenadas orienta nova seleção em Séries.
Compatibilidade do contrato não certifica disponibilidade, ancestralidade,
calendário ou amostra; essas verificações continuam no cálculo BD15a.

Códigos categóricos não entram como medida contínua mesmo quando um contrato
legado informa `value_type=NUMBER`: o tipo de saída congelado também é conferido.
O cálculo e o DTO compartilham a regra. A mudança altera a assinatura dos pares;
artefatos numéricos válidos anteriores continuam legíveis, sem reescrita. Se um
artefato antigo contradizer o próprio tipo categórico congelado, sua apresentação
como coeficiente é recusada e os bytes são preservados para diagnóstico.

`query_saved_relations(..., analysis_id=ID)` e `relation-list --analysis-id ID`
localizam exatamente um resultado sem exigir percorrer todas as páginas. O
identificador integra o recorte do cursor; cursores da versão anterior precisam
ser reiniciados. `relation --persist` passa o guarda de escrita da configuração
selecionada, como os outros produtores. Nenhum limite é alterado para liberar
uma gravação.


No Laboratório → Relações e aderência → Matriz → Relações salvas, **Criar par**
abre o formulário. Capturas vêm da análise em Séries e confluências ou do
resultado salvo selecionado. Sem captura, **Selecionar em Séries** leva à
seleção existente: escolha versões armazenadas e congele a análise.
O formulário mostra o período/relógio da origem e os contratos de cada entrada;
método, defasagem e corte com fuso são explícitos. **Amostra e recursos** contém
mínimos de pares/sessões, réplicas, semente, bloco, família de testes e orçamento.
Códigos categóricos/booleanos e unidades ausentes não habilitam o cálculo
contínuo. A seleção e os metadados de uma captura anterior são limpos ao trocar,
e respostas atrasadas não restauram o contexto antigo.

**Calcular e salvar** usa `persist_feature_relation` em `run_bg`, com a política
de escrita obtida no início da operação e sinal de cancelamento. As entradas
ficam desabilitadas durante o trabalho, a barra compartilhada indica espera e
**Cancelar cálculo** solicita interrupção. Fechar/Escape durante o trabalho
solicita cancelamento e espera a confirmação. Se a transação já confirmou,
o resultado salvo é preservado e exibido; cancelamento não desfaz commits.
Sucesso seleciona exatamente o ID retornado, mesmo quando ele ficaria fora da
primeira página do acervo. **Consultar** retira esse foco e volta ao recorte
completo. Repetição conserva o recibo original; não gera duplicatas.
O sinalizador de referência sintética é explícito e conservado no artefato.
Nada ativa Expert, ordens, utilidade financeira ou qualificação da fonte.
