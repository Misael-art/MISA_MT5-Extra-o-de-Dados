"""
Servidor da ponte MT5 para Linux.

Roda com o Python do WINDOWS dentro do Wine (onde o pacote MetaTrader5
funciona) e expõe os módulos Python via RPyC "classic" em 127.0.0.1.
O app, rodando no Python do Linux, conecta com:

    import rpyc
    conn = rpyc.classic.connect("127.0.0.1", 18812)
    mt5 = conn.modules["MetaTrader5"]

Iniciado/parado por scripts/mt5-bridge.sh. Não execute exposto à rede:
o modo classic permite executar código arbitrário, por isso o bind padrão
é 127.0.0.1.
"""
import argparse
import logging
import sys


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Ponte RPyC para o MetaTrader5 (rodar no Python do Wine)")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18812)
    parser.add_argument("--check", action="store_true",
                        help="Só verifica se rpyc e MetaTrader5 importam e sai")
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

    server = ThreadedServer(
        rpyc.SlaveService,
        hostname=args.host,
        port=args.port,
        reuse_addr=True,
        protocol_config={
            "allow_public_attrs": True,
            "allow_pickle": True,
            "sync_request_timeout": 300,  # extrações longas de M1
        },
    )
    log.info("Ponte MT5 escutando em %s:%s (Ctrl+C para parar)", args.host, args.port)
    try:
        server.start()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
