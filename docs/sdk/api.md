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
| `run_python(db, config, robot, symbol, timeframe, start=None, end=None, params=None)` | Backtest de uma estratégia Python com as barras da base |
| `run_mql5(db, config, robot, symbol, timeframe, start=None, end=None, preset=None, params=None, ...)` | Backtest de um robô MQL5 pelo Strategy Tester. O MT5 precisa estar fechado |

Os dois gravam a execução e devolvem um dicionário com `metrics`, `equity` e `trades`.

## Bateria e ciclo completo

| Nome | O que faz |
|---|---|
| `run_battery(db, config, robot, robots=(), preset=None, deep=False, ...)` | Bateria de segurança sobre o último backtest. Com `deep=True` (robô MQL5), repete o Strategy Tester para medir fora da amostra e robustez. Devolve `BatteryReport` |
| `BatteryReport` | `items`, `approved`, `allows_real`, `static_status`, `statistical_status`, `to_dict()` |
| `BatteryItem` | `group`, `id`, `name`, `status`, `detail`, `fix` |
| `check(db, config, robot, robots, symbol, timeframe, ...)` | Contrato, compilação, backtest e bateria, parando na primeira falha. Devolve `CheckResult` |
| `CheckResult` | `steps`, `ok`, `report` (a bateria), `to_dict()` |
| `write_junit(result, path)` | Grava o `CheckResult` em JUnit XML |

## Estratégias Python

| Nome | O que faz |
|---|---|
| `Strategy`, `StrategyInfo` | Classe base de uma estratégia e a sua ficha ([estrategias.md](../estrategias.md), "Para desenvolvedores") |
| `get_strategy(key, **params)` | Instância de uma das estratégias registradas |
| `BacktestConfig` | Capital inicial, risco por operação, slippage e corretagem |
| `run_backtest(df, strategy, spec=None, config=None, ...)` | Backtest com custos e lote pelo risco (os dois últimos parâmetros, `start_index` e `signals`, servem à validação) |
| `validate_strategy(df, key, spec=None, config=None, criteria=None, params=None)` | Walk-forward, robustez e Monte Carlo (Filtro C) |
