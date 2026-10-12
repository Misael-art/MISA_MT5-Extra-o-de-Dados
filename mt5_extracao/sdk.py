"""
API pública do SDK de robôs: o único lugar de onde código de fora deve importar.

    from mt5_extracao import sdk

    robots = sdk.load_catalog(config, db)
    robot = sdk.get_robot(robots, "meurobo")
    problems = sdk.validate_contract(robot)
    result = sdk.check(db, config, robot, robots, "EURUSD", "H1")

O que está em `__all__` segue versão semântica: só muda de forma incompatível em versão maior do pacote, depois de
um ciclo de aviso (`DeprecationWarning`). `API_VERSION` sobe a cada mudança incompatível. Todo o resto do pacote é
interno e pode mudar sem aviso. Referência: docs/sdk/api.md.

Este módulo não importa `tkinter` nem `MetaTrader5`. Nada aqui envia ordens.
"""
from mt5_extracao.robots.backtest import run_mql5, run_python
from mt5_extracao.robots.battery import Item as BatteryItem
from mt5_extracao.robots.battery import Report as BatteryReport
from mt5_extracao.robots.battery import run_battery
from mt5_extracao.robots.catalog import Robot, get_robot, import_library, load_catalog, robot_from_file
from mt5_extracao.robots.contract import CONTRACT_VERSION, Problem
from mt5_extracao.robots.contract import validate as validate_contract
from mt5_extracao.robots.generate import create as generate_robot
from mt5_extracao.robots.matrix import build as build_matrix
from mt5_extracao.robots.matrix import positives as matrix_positives
from mt5_extracao.robots.mt5_tools import CompileResult, compile_robot, install_robot
from mt5_extracao.robots.parity import ParityReport
from mt5_extracao.robots.parity import compare as compare_parity
from mt5_extracao.robots.parity import run as run_parity
from mt5_extracao.robots.pipeline import CheckResult, check, write_junit
from mt5_extracao.robots.scaffold import create as new_robot
from mt5_extracao.strategies import Strategy, StrategyInfo, get_strategy
from mt5_extracao.strategies.backtester import BacktestConfig, run_backtest
from mt5_extracao.strategies.declared import DeclaredStrategy, SignalSpec
from mt5_extracao.strategies.validation import validate as validate_strategy

API_VERSION = 1

__all__ = [
    "API_VERSION", "CONTRACT_VERSION",
    # catálogo
    "Robot", "load_catalog", "get_robot", "robot_from_file", "import_library",
    # contrato e ponto de partida
    "Problem", "validate_contract", "new_robot",
    # MT5: instalar e compilar
    "CompileResult", "install_robot", "compile_robot",
    # backtest
    "run_python", "run_mql5",
    # bateria e ciclo completo
    "BatteryItem", "BatteryReport", "run_battery", "CheckResult", "check", "write_junit",
    # paridade Python x MQL5
    "ParityReport", "compare_parity", "run_parity",
    # estratégia declarada e geração do robô MQL5
    "SignalSpec", "DeclaredStrategy", "generate_robot",
    # visão de conjunto: ativo x timeframe x robô
    "build_matrix", "matrix_positives",
    # estratégias Python
    "Strategy", "StrategyInfo", "get_strategy", "BacktestConfig", "run_backtest", "validate_strategy",
]
