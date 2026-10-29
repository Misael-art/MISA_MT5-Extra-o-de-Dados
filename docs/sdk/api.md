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
