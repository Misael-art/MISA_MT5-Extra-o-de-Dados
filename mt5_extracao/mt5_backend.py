"""
Acesso ao MetaTrader 5: módulo local (Windows) ou ponte RPyC (Linux/Wine).

Este é o ÚNICO módulo que importa o pacote MetaTrader5. O restante do código usa:

    from mt5_extracao.mt5_backend import get_mt5
    mt5 = get_mt5(config_path)   # None se o MT5 estiver indisponível

No Linux, o pacote MetaTrader5 roda no Python do Windows dentro do Wine
(scripts/mt5_bridge_server.py) e é acessado por RPyC. As datas são enviadas como
inteiros epoch (segundos, datetime sem fuso = UTC) e os resultados voltam como
tipos puros do Python e são reconstruídos aqui (arrays numpy e namedtuples).
"""
import calendar
import collections
import configparser
import datetime as _dt
import json
import logging
import threading

import numpy as np

log = logging.getLogger(__name__)

BRIDGE_PROTOCOL = "mt5x-bridge/2"


def _to_epoch(v):
    """datetime -> epoch em segundos (sem fuso = UTC); outros valores inalterados."""
    if isinstance(v, _dt.datetime):
        if v.tzinfo is not None:
            v = v.astimezone(_dt.timezone.utc).replace(tzinfo=None)
        return calendar.timegm(v.timetuple())
    return v


_struct_types = {}


def _decode(v):
    """Reconstrói arrays numpy estruturados e namedtuples a partir do formato da ponte."""
    if isinstance(v, dict) and v.get("__ndarray__"):
        return np.array([tuple(r) for r in v["rows"]], dtype=[tuple(d) for d in v["descr"]])
    if isinstance(v, dict) and "__struct__" in v:
        fields = {k: _decode(x) for k, x in v["fields"].items()}
        key = (v["__struct__"], tuple(fields))
        if key not in _struct_types:
            _struct_types[key] = collections.namedtuple(v["__struct__"], list(fields))
        return _struct_types[key](**fields)
    if isinstance(v, list):
        return tuple(_decode(x) for x in v)
    return v


class RemoteMT5:
    """
    Mesma API do módulo MetaTrader5, executada no Python do Wine pela ponte restrita
    (scripts/mt5_bridge_server.py): só funções de leitura, argumentos e resultados em JSON.
    """

    def __init__(self, host="127.0.0.1", port=18812):
        import rpyc
        self._lock = threading.Lock()  # uma conexão RPyC não deve ser usada por várias threads ao mesmo tempo
        self._conn = rpyc.connect(host, port, keepalive=True,
                                  config={"sync_request_timeout": 300, "allow_pickle": False})
        try:
            protocol = str(self._conn.root.info())
        except Exception as e:
            self._conn.close()
            raise RuntimeError("a ponte em execução é de uma versão antiga (modo aberto). "
                               "Reinicie-a: ./scripts/mt5-bridge.sh restart") from e
        if protocol != BRIDGE_PROTOCOL:
            self._conn.close()
            raise RuntimeError(f"ponte fala '{protocol}', esperado '{BRIDGE_PROTOCOL}'. "
                               "Reinicie-a: ./scripts/mt5-bridge.sh restart")
        self._consts = {}

    def __getattr__(self, name):
        if name.startswith("_") and name != "__version__":
            raise AttributeError(name)
        if name.isupper() or name == "__version__":
            if name not in self._consts:
                with self._lock:
                    self._consts[name] = self._conn.root.const(name)
            return self._consts[name]

        def fn(*args, **kwargs):
            payload = json.dumps([[_to_epoch(a) for a in args], {k: _to_epoch(v) for k, v in kwargs.items()}])
            with self._lock:
                return _decode(json.loads(self._conn.root.call(name, payload)))
        fn.__name__ = name
        return fn


_instance = None
_instance_lock = threading.Lock()


def get_mt5(config_path="config/config.ini"):
    """
    Retorna o módulo MetaTrader5 (Windows) ou um RemoteMT5 (Linux, [BRIDGE] enabled = true).
    Retorna None se o MT5 estiver indisponível (pacote ausente ou ponte fora do ar).
    """
    global _instance
    with _instance_lock:
        if _instance is not None:
            return _instance
        cfg = configparser.ConfigParser(interpolation=None)
        cfg.read(config_path, encoding="utf-8")
        if cfg.getboolean("BRIDGE", "enabled", fallback=False):
            host = cfg.get("BRIDGE", "host", fallback="127.0.0.1")
            port = cfg.getint("BRIDGE", "port", fallback=18812)
            try:
                _instance = RemoteMT5(host, port)
                log.info(f"MetaTrader5 acessado pela ponte em {host}:{port}.")
            except Exception as e:
                log.error(f"Ponte MT5 indisponível em {host}:{port}: {e}. "
                          f"Inicie-a com ./scripts/mt5-bridge.sh start")
                return None
        else:
            try:
                import MetaTrader5 as mt5
                _instance = mt5
            except ImportError:
                log.error("Pacote MetaTrader5 não encontrado (só existe para Windows; no Linux use a ponte).")
                return None
        return _instance


def reset():
    """Descarta a instância atual (usado em testes e ao reconfigurar)."""
    global _instance
    with _instance_lock:
        _instance = None
