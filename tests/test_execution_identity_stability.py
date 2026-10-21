"""Identidade por código/valores, independente de detalhes de memória do processo."""
import os
import subprocess
import sys

import pytest

from mt5_extracao.robots import backtest


@pytest.mark.parametrize("seed", ["1", "2", "3"])
def test_execution_code_identity_survives_interning_and_fresh_process(seed):
    from mt5_extracao.strategies import indicators
    before = backtest._execution_code_digest()
    constant = next(value for value in indicators.smma.__code__.co_consts[1:]
                    if type(value) is str and not value.isidentifier())
    held = sys.intern(constant)
    assert held == constant
    assert backtest._execution_code_digest() == before
    script = "from mt5_extracao.robots.backtest import _execution_code_digest; print(_execution_code_digest())"
    child = subprocess.run([sys.executable, "-c", script],
                           env={**os.environ, "PYTHONHASHSEED": seed},
                           capture_output=True, text=True, check=True, timeout=30)
    assert child.stdout.strip() == before
