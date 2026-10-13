#!/usr/bin/env bash
# Inicia o MT5 Extração no Linux (criado para ser usado após ./install.sh).
set -euo pipefail
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_ROOT"

if [[ ! -x .venv/bin/python ]]; then
    echo "O ambiente ainda não foi instalado. Execute primeiro: ./install.sh"
    exit 1
fi
if [[ ! -f config/config.ini ]]; then
    .venv/bin/python -m mt5_extracao.bootstrap configure
fi

# No Linux o MT5 roda no Wine: garante que a ponte esteja ativa
./scripts/mt5-bridge.sh start >/dev/null || echo "Aviso: ponte MT5 indisponível (veja logs/mt5_bridge.log)." >&2

# ./run.sh --cli <comando> ...  -> linha de comando (mt5x); sem --cli -> interface gráfica
if [[ "${1:-}" == "--cli" ]]; then
    shift
    exec .venv/bin/python -m mt5_extracao.cli "$@"
fi
exec .venv/bin/python app.py "$@"
