"""
DESCONTINUADO: check_mt5_config.py foi substituído por:
    python -m mt5_extracao.bootstrap doctor

Mantido apenas por compatibilidade: executa o comando novo.
"""
import sys

from mt5_extracao.bootstrap.__main__ import main

if __name__ == "__main__":
    print("AVISO: 'check_mt5_config.py' está descontinuado. Use: python -m mt5_extracao.bootstrap doctor", file=sys.stderr)
    sys.exit(main(["doctor"] + sys.argv[1:]))
