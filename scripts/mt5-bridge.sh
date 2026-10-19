#!/usr/bin/env bash
# Mesmo gerenciador Linux/Wine; ciclo de vida e identidade em bootstrap.bridge.
# Uso: ./scripts/mt5-bridge.sh start|stop|restart|status|foreground
# Configuração: config/config.ini. Estado e log: logs/mt5_bridge*.
set -Eeuo pipefail
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PY="$PROJECT_ROOT/.venv/bin/python"
[[ -x "$VENV_PY" ]] || { echo "Ambiente não instalado. Execute ./install.sh" >&2; exit 1; }
export PYTHONPATH="$PROJECT_ROOT${PYTHONPATH:+:$PYTHONPATH}"
cd "$PROJECT_ROOT"
exec "$VENV_PY" -m mt5_extracao.bootstrap.bridge --root "$PROJECT_ROOT" "${1:-status}"
