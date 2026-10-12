"""
Servidor da ponte MT5 para Linux.

Roda com o Python do WINDOWS dentro do Wine (onde o pacote MetaTrader5 funciona) e atende o app,
que roda no Python do Linux (mt5_extracao/mt5_backend.py).

Segurança: a ponte é RESTRITA. Só responde às funções de leitura listadas em ALLOWED; não executa
código enviado pelo cliente, não expõe módulos e não aceita pickle. Funções que enviam ou
simulam ordens ficam de fora de propósito: quem opera é o robô MQL5 dentro do MT5, nunca o Python.
Argumentos e resultados trafegam como texto JSON. Mesmo assim, não exponha à rede: use 127.0.0.1.

Iniciado/parado por scripts/mt5-bridge.sh.
"""
import argparse
import json
import logging
import sys

PROTOCOL = "mt5x-bridge/2"

# Leitura de dados e de estado, mais conexão (initialize/login/shutdown). Nada que envie ordem.
ALLOWED = frozenset({
    "initialize", "login", "shutdown", "version", "last_error", "account_info", "terminal_info",
    "symbols_total", "symbols_get", "symbol_info", "symbol_info_tick", "symbol_select",
    "market_book_add", "market_book_get", "market_book_release",
    "copy_rates_from", "copy_rates_from_pos", "copy_rates_range", "copy_ticks_from", "copy_ticks_range",
    "positions_total", "positions_get", "orders_total", "orders_get",
    "history_orders_total", "history_orders_get", "history_deals_total", "history_deals_get",
})


def encode(v):
    """Converte os resultados do MetaTrader5 em tipos que o JSON carrega (o cliente reconstrói)."""
    if hasattr(v, "dtype") and getattr(v.dtype, "names", None):
        return {"__ndarray__": True, "descr": [[n, v.dtype[n].str] for n in v.dtype.names], "rows": v.tolist()}
    if hasattr(v, "_asdict"):
        return {"__struct__": type(v).__name__, "fields": {k: encode(x) for k, x in v._asdict().items()}}
    if isinstance(v, (tuple, list)):
        return [encode(x) for x in v]
    if hasattr(v, "item") and not isinstance(v, (int, float, str, bool)):   # escalar do numpy
        return v.item()
    return v


def make_service(rpyc, mt5, allowed=ALLOWED):
    class MT5ReadOnlyService(rpyc.Service):
        def exposed_info(self):
            return PROTOCOL

        def exposed_const(self, name):
            name = str(name)
            if not (name.isupper() or name == "__version__"):
                raise PermissionError("A ponte só informa constantes do MetaTrader5: %s" % name)
            value = getattr(mt5, name)
            if not isinstance(value, (int, float, str, bool)):
                raise PermissionError("Constante não suportada: %s" % name)
            return value

        def exposed_call(self, name, payload):
            name = str(name)
            if name not in allowed:
                raise PermissionError("Função não permitida pela ponte: %s (a ponte é somente leitura)" % name)
            args, kwargs = json.loads(payload)
            return json.dumps(encode(getattr(mt5, name)(*args, **kwargs)))
    return MT5ReadOnlyService


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Ponte restrita para o MetaTrader5 (rodar no Python do Wine)")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18812)
    parser.add_argument("--check", action="store_true",
                        help="Só verifica se rpyc e MetaTrader5 importam e sai")
    parser.add_argument("--allow", action="append", default=[], metavar="FUNCAO",
                        help="Libera uma função extra (só para testes; nunca libere funções de ordem)")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    log = logging.getLogger("mt5_bridge")

    try:
        import rpyc
        from rpyc.utils.server import ThreadedServer
    except ImportError as e:
        log.error("rpyc não instalado no Python do Wine: %s", e)
        return 1
    try:
        import MetaTrader5  # noqa: F401  (pré-carrega para falhar cedo)
        log.info("MetaTrader5 %s disponível", getattr(MetaTrader5, "__version__", "?"))
    except ImportError as e:
        log.error("MetaTrader5 não instalado no Python do Wine: %s", e)
        return 1
    if args.check:
        return 0
    extra = [name for name in args.allow if name.startswith("order")]
    if extra:
        log.error("Funções de ordem não podem ser liberadas: %s", ", ".join(extra))
        return 1
    if args.host not in ("127.0.0.1", "localhost", "::1"):
        log.warning("Ponte escutando em %s: qualquer máquina que alcance este endereço lê os dados da sua conta. "
                    "Use 127.0.0.1.", args.host)

    server = ThreadedServer(
        make_service(rpyc, MetaTrader5, ALLOWED | frozenset(args.allow)),
        hostname=args.host,
        port=args.port,
        reuse_addr=True,
        protocol_config={
            "allow_public_attrs": False,
            "allow_all_attrs": False,
            "allow_pickle": False,
            "sync_request_timeout": 300,  # extrações longas de M1
        },
    )
    log.info("Ponte MT5 (restrita, %d funções de leitura) escutando em %s:%s (Ctrl+C para parar)",
             len(ALLOWED) + len(args.allow), args.host, args.port)
    try:
        server.start()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
