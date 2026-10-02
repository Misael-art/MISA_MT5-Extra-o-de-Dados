"""
Módulo "MetaTrader5" falso para o servidor da ponte nos testes de ponta a ponta.
Delega tudo a uma instância de FakeMT5 (tests/fakes.py).
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from fakes import FakeMT5  # noqa: E402

_fake = FakeMT5()
__version__ = _fake.__version__


def __getattr__(name):
    return getattr(_fake, name)
