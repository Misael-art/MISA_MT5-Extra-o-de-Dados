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

Nada aqui envia ordens. O backtest de robô MQL5 usa o Strategy Tester do MetaTrader 5, que precisa estar fechado.

## Versões

| Nome | Significado |
|---|---|
| `API_VERSION` | Versão desta API. Sobe quando algo muda de forma incompatível |
| `CONTRACT_VERSION` | Versão do contrato do robô ([contrato.md](contrato.md)) |

**Política de mudança:** uma função ou parâmetro só é removido ou muda de significado em versão **maior** do pacote,
depois de pelo menos uma versão emitindo `DeprecationWarning`. Parâmetros novos entram no fim, com valor padrão.

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
| `run_python(db, config, robot, symbol, timeframe, start=None, end=None, params=None, ..., real_spread=True, cashflows=None)` | Backtest de uma estratégia Python com as barras da base. `real_spread=False` cobra o spread das barras em vez do medido nos ticks (é o que a paridade usa) |
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
| `run_backtest(df, strategy, spec=None, config=None, start_index=0, signals=None, cashflows=None)` | Backtest com custos e lote pelo risco; `start_index` e `signals` servem à validação, e `cashflows` identifica fluxos no fechamento |
| `validate_strategy(df, key, spec=None, config=None, criteria=None, params=None)` | Walk-forward, robustez e Monte Carlo (Filtro C) |
