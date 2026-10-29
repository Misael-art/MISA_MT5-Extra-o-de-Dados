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
from typing import Optional

from mt5_extracao.robots.backtest import run_mql5, run_python as _run_python
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
from mt5_extracao.strategies.backtester import BacktestConfig, run_backtest as _run_backtest
from mt5_extracao.strategies.declared import DeclaredStrategy, SignalSpec
from mt5_extracao.strategies.validation import validate as validate_strategy


def run_python(db, config, robot, symbol: str, timeframe: str, start=None, end=None,
               params: Optional[dict] = None, strategy=None, source: str = "backtest", trade_from=None,
               real_spread: bool = True, cashflows=None, data_manifest_id=None, control=None) -> dict:
    """Delega ao produtor existente com a assinatura publicada do SDK 1.

    Composições explícitas pertencem ao serviço/CLI interno, sem ampliar o SDK.
    """
    return _run_python(db, config, robot, symbol, timeframe, start, end, params, strategy,
                       source, trade_from, real_spread, cashflows, data_manifest_id, control)


def run_backtest(df, strategy, spec=None, config=None, start_index=0, signals=None,
                 cashflows=None, check=None, observation_timeframe=None):
    """Executa o motor existente com a assinatura publicada do SDK 1.

    Políticas de composição são extensões internas do serviço de composição.
    """
    return _run_backtest(df, strategy, spec, config, start_index, signals, cashflows,
                         check, observation_timeframe)


API_VERSION = 1


def open_workflow(db, config, *, read_account=None, config_path=None, db_override=None):
    """Controlador local de sessões/presets/gráficos; fechar o contexto encerra só suas sessões."""
    from .automation import open_workflow as create
    return create(db, config, read_account=read_account, config_path=config_path, db_override=db_override)

def query_series(db, **selection):
    """Consulta pública de fatos do store (BD18a); ver ``analytics.query_service.series``."""
    from mt5_extracao.analytics import query_service
    return query_service.series(db, **selection)


def query_catalog(db=None):
    """Contratos do registry e, com db, todas as versões registradas (BD06/BD18a)."""
    from mt5_extracao.analytics import query_service
    return query_service.catalog(db)


def define_feature_composite(db, feature_id, name, algorithm_version, *, operation, inputs):
    """Registra receita aritmética tipada e imutável; não grava valores calculados."""
    from mt5_extracao.analytics import features
    return features.register_composite(db, feature_id, name, algorithm_version, operation=operation, inputs=inputs)


def query_composite_context(db, manifest_id, decision_times, *, targets, observation_time=None):
    """Avalia o grafo congelado pelo executor existente; contexto exploratório, sem materialização."""
    import pandas as pd
    from mt5_extracao.analytics import datasets, features
    if not isinstance(decision_times, (list, tuple)):
        raise ValueError("Informe decision_times como uma lista de instantes com fuso.")
    return features.compose_snapshot(datasets.load(db, manifest_id), pd.DataFrame(dict(decision_time=decision_times)),
                                     targets=targets, observation_time=observation_time)


def materialize_composite_snapshot(db, manifest_id, *, targets, as_of, batch_size=5000, write_guard=None, cancel_check=None,
                                   timeout_seconds=600, max_memory_mb=2048):
    """Grava revisões da cadeia congelada pelo store/journal existentes; não certifica fonte live."""
    from mt5_extracao.analytics import features
    return features.materialize_snapshot(db, manifest_id, targets=targets, as_of=as_of, batch_size=batch_size,
        write_guard=write_guard, cancel_check=cancel_check, timeout_seconds=timeout_seconds, max_memory_mb=max_memory_mb)


def query_feature_relation(db, manifest_id, *, left, right, as_of, lag_bars=0, method="pearson", minimum_pairs=30,
                           minimum_sessions=10, bootstrap_replicates=999, seed=7, block_length_sessions=None,
                           tests_counted=None, synthetic_fixture=False, timeout_seconds=120, max_memory_mb=2048,
                           cancel_check=None):
    """Relação referencial de duas versões congeladas; sem persistência ou certificação financeira."""
    from mt5_extracao.analytics import associations
    return associations.query_frozen_pair(db, manifest_id, left=left, right=right, as_of=as_of,
        lag_bars=lag_bars, method=method, minimum_pairs=minimum_pairs, minimum_sessions=minimum_sessions,
        bootstrap_replicates=bootstrap_replicates, seed=seed, block_length_sessions=block_length_sessions,
        tests_counted=tests_counted, synthetic_fixture=synthetic_fixture, timeout_seconds=timeout_seconds,
        max_memory_mb=max_memory_mb, cancel_check=cancel_check)


def recover_producer_jobs(db, *, symbol=None, timeframe=None, family=None):
    """Reconcilia apenas runs de proprietários locais encerrados; fatos e checkpoint são preservados."""
    from mt5_extracao.analytics import producer_jobs
    return producer_jobs.reconcile_orphan_runs(db, symbol=symbol, timeframe=timeframe, family=family)


__all__ = [
    "API_VERSION", "CONTRACT_VERSION",
    "open_workflow",
    # consulta de dados analíticos (futura API, sem servidor)
    "query_series", "query_catalog", "define_feature_composite", "query_composite_context", "materialize_composite_snapshot",
    "recover_producer_jobs",
    "query_feature_relation",
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
