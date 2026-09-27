"""
DESCONTINUADO: install.py foi substituído por:
    install.bat (Windows) ou ./install.sh (Linux); para só reconfigurar: python -m mt5_extracao.bootstrap configure

Mantido apenas por compatibilidade: executa o comando novo.
"""
import sys

from mt5_extracao.bootstrap.__main__ import main

if __name__ == "__main__":
    print("AVISO: 'install.py' está descontinuado. Use: install.bat (Windows) ou ./install.sh (Linux); para só reconfigurar: python -m mt5_extracao.bootstrap configure", file=sys.stderr)
    sys.exit(main(["configure"] + sys.argv[1:]))
