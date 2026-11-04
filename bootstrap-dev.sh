#!/usr/bin/env bash
# Entrada independente: pode ser baixada pelo navegador antes de haver Git/Python.
set -euo pipefail
workspace="${HOME}/Projects/MT5X"
apply=0
native=0
force_plan=0
while (($#)); do
    case "$1" in
        --workspace) workspace="${2:?Informe a pasta de desenvolvimento}"; shift ;;
        --yes) apply=1 ;;
        --with-mt5) native=1 ;;
        --plan|--help|-h) force_plan=1 ;;
        *) echo "Opção inválida: $1" >&2; exit 2 ;;
    esac
    shift
done
app="$workspace/repos/app"
echo "MT5X: Git + instalador oficial + dependências de desenvolvimento."
echo "Pasta: $workspace; um ambiente Python e três repositórios."
echo "Wine/MT5: $native. Nenhuma base privada é substituída."
if ((apply == 0 || force_plan)); then
    echo "Use --yes para preparar; --with-mt5 também instala a plataforma nativa."
    exit 0
fi
as_root() {
    if [[ $EUID -eq 0 ]]; then "$@"
    elif command -v sudo >/dev/null; then sudo "$@"
    else echo "Instale sudo ou peça ao administrador os pacotes indicados e repita." >&2; exit 1
    fi
}
if ! command -v git >/dev/null; then
    if command -v apt-get >/dev/null; then
        as_root apt-get update
        as_root apt-get install -y git
    elif command -v pacman >/dev/null; then
        as_root pacman -S --needed --noconfirm git
    elif command -v dnf >/dev/null; then
        as_root dnf install -y git
    elif command -v zypper >/dev/null; then
        as_root zypper --non-interactive install git
    else
        echo "Instale Git pelo gerenciador do sistema e repita este comando." >&2
        exit 1
    fi
fi
if [[ ! -e "$app" ]]; then
    mkdir -p "$workspace/repos"
    git clone --config core.autocrlf=false --branch codex/novo-host-20261024 \
        https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados.git "$app"
fi
[[ "$(git -C "$app" rev-parse --show-toplevel)" == "$(cd "$app" && pwd -P)" ]] || exit 1
origin="$(git -C "$app" remote get-url origin)"
[[ "${origin%.git}" == https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados ]] || {
    echo "A pasta app pertence a outro repositório. Escolha outra --workspace." >&2; exit 1;
}
[[ -z "$(git -C "$app" status --porcelain)" ]] || {
    echo "App contém alterações. Preserve-as; use uma pasta nova, sem reset/clean." >&2; exit 1;
}
[[ -f "$app/scripts/setup_development.py" ]] || {
    echo "Este checkout não contém o wrapper. Busque a branch codex/novo-host-20261024 em outra pasta." >&2; exit 1;
}
if [[ ! -x "$app/.venv/bin/python" ]] || ((native)); then
    args=(--yes)
    if ((native == 0)); then args+=(--no-mt5); fi
    bash "$app/install.sh" "${args[@]}"
fi
if ! command -v g++ >/dev/null || ! command -v shellcheck >/dev/null; then
    if command -v apt-get >/dev/null; then as_root apt-get install -y g++ shellcheck
    elif command -v pacman >/dev/null; then as_root pacman -S --needed --noconfirm gcc shellcheck
    elif command -v dnf >/dev/null; then as_root dnf install -y gcc-c++ ShellCheck
    elif command -v zypper >/dev/null; then as_root zypper --non-interactive install gcc-c++ ShellCheck
    else echo "Instale g++ e shellcheck e repita o wrapper." >&2; exit 1
    fi
fi
exec "$app/.venv/bin/python" "$app/scripts/setup_development.py" --workspace "$workspace"
