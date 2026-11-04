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


def order_calc_profit(action, symbol, volume, price_open, price_close):
    """Contrato fictício de10 unidades por preço; só transporte da ponte."""
    _fake.calls.append(("order_calc_profit", (action, symbol, volume, price_open, price_close), {}))
    if symbol not in _fake.symbols or action not in (0, 1):
        return None
    return (price_close-price_open) * (1 if action == 0 else -1) * volume * 10


def order_calc_margin(action, symbol, volume, price):
    _fake.calls.append(("order_calc_margin", (action, symbol, volume, price), {}))
    return volume * price * 10 if symbol in _fake.symbols and action in (0, 1) else None
