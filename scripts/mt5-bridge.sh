#!/usr/bin/env bash
# =============================================================================
# Controla a ponte MT5 (Linux): Python do Windows no Wine servindo o pacote
# MetaTrader5 via RPyC em 127.0.0.1 (scripts/mt5_bridge_server.py).
#
# Uso: ./scripts/mt5-bridge.sh start|stop|restart|status|foreground
# Lê [MT5] wine_prefix e [BRIDGE] host/port/wine_python de config/config.ini.
# Log: logs/mt5_bridge.log   PID: logs/mt5_bridge.pid
# =============================================================================
set -Eeuo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PY="$PROJECT_ROOT/.venv/bin/python"
LOG_FILE="$PROJECT_ROOT/logs/mt5_bridge.log"
PID_FILE="$PROJECT_ROOT/logs/mt5_bridge.pid"
XVFB_PID_FILE="$PROJECT_ROOT/logs/mt5_bridge_xvfb.pid"
SERVER_SCRIPT="$PROJECT_ROOT/scripts/mt5_bridge_server.py"
mkdir -p "$PROJECT_ROOT/logs"

[[ -x "$VENV_PY" ]] || { echo "Ambiente não instalado. Execute ./install.sh" >&2; exit 1; }

# Lê a configuração com o próprio Python do projeto (sem parser em bash)
read_config() {
    "$VENV_PY" - "$PROJECT_ROOT" <<'EOF'
import configparser, shlex, sys
from pathlib import Path
root = Path(sys.argv[1])
cfg = configparser.ConfigParser(interpolation=None)
cfg.read(root / "config" / "config.ini", encoding="utf-8")
vals = {
    "CFG_PREFIX": cfg.get("MT5", "wine_prefix", fallback=""),
    "CFG_HOST": cfg.get("BRIDGE", "host", fallback="127.0.0.1"),
    "CFG_PORT": cfg.get("BRIDGE", "port", fallback="18812"),
    "CFG_WINE_PY": cfg.get("BRIDGE", "wine_python", fallback=r"C:\Python311\python.exe"),
    "CFG_ENABLED": cfg.get("BRIDGE", "enabled", fallback="false"),
}
for k, v in vals.items():
    print(f"{k}={shlex.quote(v)}")
EOF
}
eval "$(read_config)"

port_open() {
    "$VENV_PY" -c "import socket,sys; socket.create_connection(('$CFG_HOST', int('$CFG_PORT')), 1.5)" 2>/dev/null
}

is_running() {
    [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null
}

start_display() {
    # O terminal MT5 (iniciado por mt5.initialize) precisa de um display X
    if [[ -n "${DISPLAY:-}" ]]; then return; fi
    if ! command -v Xvfb >/dev/null 2>&1; then
        echo "Aviso: sem DISPLAY e sem Xvfb; o terminal MT5 pode não abrir." >&2
        return
    fi
    local n=99
    while [[ -e "/tmp/.X11-unix/X$n" || -e "/tmp/.X$n-lock" ]]; do n=$((n + 1)); done
    nohup Xvfb ":$n" -screen 0 1280x800x24 -nolisten tcp >/dev/null 2>&1 &
    echo $! > "$XVFB_PID_FILE"
    export DISPLAY=":$n"
    sleep 1
}

cmd_start() {
    if [[ "$CFG_ENABLED" != "true" ]]; then
        echo "Ponte desativada ([BRIDGE] enabled != true). Nada a fazer."; return 0
    fi
    if is_running && port_open; then echo "Ponte já está em execução (PID $(cat "$PID_FILE"))."; return 0; fi
    [[ -n "$CFG_PREFIX" ]] || { echo "[MT5] wine_prefix vazio em config/config.ini. Execute ./install.sh" >&2; return 1; }
    command -v wine >/dev/null 2>&1 || { echo "Wine não encontrado. Execute ./install.sh" >&2; return 1; }
    export WINEPREFIX="$CFG_PREFIX" WINEDEBUG=-all
    start_display
    local server_win
    server_win="$(winepath -w "$SERVER_SCRIPT" 2>/dev/null)"
    echo "===== $(date '+%Y-%m-%d %H:%M:%S') iniciando ponte =====" >> "$LOG_FILE"
    nohup wine "$CFG_WINE_PY" "$server_win" --host "$CFG_HOST" --port "$CFG_PORT" >> "$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    local _
    for _ in $(seq 1 60); do
        if port_open; then echo "Ponte MT5 iniciada em $CFG_HOST:$CFG_PORT (PID $(cat "$PID_FILE"))."; return 0; fi
        if ! is_running; then break; fi
        sleep 1
    done
    echo "Falha ao iniciar a ponte. Últimas linhas de $LOG_FILE:" >&2
    tail -n 20 "$LOG_FILE" >&2 || true
    return 1
}

cmd_stop() {
    if is_running; then
        kill "$(cat "$PID_FILE")" 2>/dev/null || true
    fi
    # O processo python.exe do Wine pode sobreviver ao lançador 'wine'
    pkill -f "mt5_bridge_server.py" 2>/dev/null || true
    rm -f "$PID_FILE"
    if [[ -f "$XVFB_PID_FILE" ]]; then
        kill "$(cat "$XVFB_PID_FILE")" 2>/dev/null || true
        rm -f "$XVFB_PID_FILE"
    fi
    echo "Ponte MT5 parada."
}

cmd_status() {
    if port_open; then
        echo "Ponte MT5: ATIVA em $CFG_HOST:$CFG_PORT"
        return 0
    fi
    echo "Ponte MT5: PARADA"
    return 3
}

cmd_foreground() {
    export WINEPREFIX="$CFG_PREFIX" WINEDEBUG=-all
    start_display
    exec wine "$CFG_WINE_PY" "$(winepath -w "$SERVER_SCRIPT")" --host "$CFG_HOST" --port "$CFG_PORT"
}

case "${1:-status}" in
    start) cmd_start ;;
    stop) cmd_stop ;;
    restart) cmd_stop; cmd_start ;;
    status) cmd_status ;;
    foreground) cmd_foreground ;;
    *) echo "Uso: $0 start|stop|restart|status|foreground" >&2; exit 2 ;;
esac
