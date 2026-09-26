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
./scripts/mt5-bridge.sh start || echo "Aviso: ponte MT5 indisponível (veja logs/mt5_bridge.log)."

exec .venv/bin/python app.py "$@"
