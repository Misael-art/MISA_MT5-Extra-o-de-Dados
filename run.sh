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

# Drive é uma operação local explícita: não inicia Wine nem a ponte.
if [[ "${1:-}" == "--offline" && "${2:-}" == "--cli" ]]; then
    shift 2
    offline_arguments=("$@")
    for ((offline_index=0; offline_index<${#offline_arguments[@]}; offline_index++)); do
        case "${offline_arguments[offline_index]}" in
            --config|--db) offline_index=$((offline_index + 1)) ;;
            --json|-v|--verbose|--config=*|--db=*) ;;
            resources)
                if [[ "${offline_arguments[offline_index+1]:-}" == "drive" ]]; then
                    MT5X_LOCAL_ONLY=1 exec .venv/bin/python -m mt5_extracao.cli "$@"
                fi
                break ;;
            *) break ;;
        esac
    done
    echo "Use --offline --cli somente para resources drive."
    exit 2
fi

# Dashboard é leitura local: não abrir ponte/terminal para servir HTML.
# Respeita também as opções globais da CLI antes do nome do comando.
if [[ "${1:-}" == "--cli" ]]; then
    cli_arguments=("$@")
    for ((argument_index=1; argument_index<${#cli_arguments[@]}; argument_index++)); do
        case "${cli_arguments[argument_index]}" in
            --config|--db) argument_index=$((argument_index + 1)) ;;
            --json|-v|--verbose) ;;
            dashboard)
                shift
                exec .venv/bin/python -m mt5_extracao.cli "$@"
                ;;
            *) break ;;
        esac
    done
fi

# No Linux o MT5 roda no Wine: garante que a ponte esteja ativa
export MT5X_BRIDGE_START_ERROR=""
if [[ "${1:-}" != "--offline" ]]; then
    if bridge_output=$(./scripts/mt5-bridge.sh start 2>&1); then
        :
    else
        export MT5X_BRIDGE_START_ERROR="$bridge_output"
        mkdir -p logs
        printf '%s\n' "$bridge_output" >>logs/mt5_bridge.log
        printf 'Aviso: ponte MT5 indisponível: %s\n' "$bridge_output" >&2
    fi
fi

# ./run.sh --cli <comando> ...  -> linha de comando (mt5x); sem --cli -> interface gráfica
if [[ "${1:-}" == "--cli" ]]; then
    shift
    exec .venv/bin/python -m mt5_extracao.cli "$@"
fi
exec .venv/bin/python app.py "$@"
