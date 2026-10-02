"""
DESCONTINUADO: executar_mt5.py foi substituído pelos lançadores:
    run.bat (Windows) ou ./run.sh (Linux)

Mantido apenas por compatibilidade: executa o app.py.
"""
import os
import runpy
import sys

if __name__ == "__main__":
    print("AVISO: 'executar_mt5.py' está descontinuado. Use run.bat (Windows) ou ./run.sh (Linux).", file=sys.stderr)
    root = os.path.dirname(os.path.abspath(__file__))
    os.chdir(root)
    sys.path.insert(0, root)
    runpy.run_path(os.path.join(root, "app.py"), run_name="__main__")
