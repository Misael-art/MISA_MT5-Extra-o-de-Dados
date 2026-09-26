#!/usr/bin/env bash
# =============================================================================
# MT5 Extração - instalador automático para Linux
#
# O que faz (idempotente: pode ser executado várias vezes):
#   1. Instala pacotes do sistema: Python 3 + venv + tkinter, curl, Xvfb, Wine
#   2. Cria o ambiente virtual .venv e instala as dependências Python
#   3. Cria um prefixo Wine dedicado, baixa e instala o MetaTrader 5 (/auto)
#   4. Instala o Python do Windows dentro do Wine + MetaTrader5 + rpyc (ponte)
#   5. Gera config/config.ini e .env (python -m mt5_extracao.bootstrap configure)
#   6. Cria atalho no menu de aplicativos, inicia a ponte e roda o diagnóstico
#
# Uso:  ./install.sh [--yes] [--no-mt5] [--skip-system-packages]
#                    [--wine-prefix DIR] [--python BIN]
# Log:  logs/install-linux.log
# =============================================================================
set -Eeuo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LOG_DIR="$PROJECT_ROOT/logs"
LOG_FILE="$LOG_DIR/install-linux.log"
VENV_DIR="$PROJECT_ROOT/.venv"
VENV_PY="$VENV_DIR/bin/python"
CACHE_DIR="${XDG_CACHE_HOME:-$HOME/.cache}/mt5-extracao"

# --- Versões/URLs (altere aqui, em um único lugar) ---------------------------
MT5_SETUP_URL="https://download.mql5.com/cdn/web/metaquotes.software.corp/metatrader5/mt5setup.exe"
WIN_PYTHON_VERSION="3.11.9"
WIN_PYTHON_URL="https://www.python.org/ftp/python/${WIN_PYTHON_VERSION}/python-${WIN_PYTHON_VERSION}-amd64.exe"
WIN_PYTHON_DIR_WIN='C:\Python311'          # deve bater com paths.WINE_PYTHON_WINDOWS_DIR
WIN_PYTHON_DIR_UNIX_REL="drive_c/Python311"
RPYC_VERSION="6.0.2"                       # deve bater com requirements.txt
WINE_MIN_MAJOR=8
MIN_PY_MINOR=9                             # Python 3.9+

# --- Opções -------------------------------------------------------------------
ASSUME_YES=0
INSTALL_MT5=1
SYSTEM_PACKAGES=1
PYTHON_BIN=""
DOCTOR_OK=0
WINEPREFIX="${MT5X_WINEPREFIX:-${XDG_DATA_HOME:-$HOME/.local/share}/mt5-extracao/wine}"

usage() {
    sed -n '3,15p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -y|--yes) ASSUME_YES=1 ;;
        --no-mt5) INSTALL_MT5=0 ;;
        --skip-system-packages) SYSTEM_PACKAGES=0 ;;
        --wine-prefix) WINEPREFIX="${2:?--wine-prefix requer um diretório}"; shift ;;
        --python) PYTHON_BIN="${2:?--python requer um executável}"; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Opção desconhecida: $1" >&2; usage; exit 2 ;;
    esac
    shift
done

mkdir -p "$LOG_DIR" "$CACHE_DIR"
exec > >(tee -a "$LOG_FILE") 2>&1
echo "===== $(date '+%Y-%m-%d %H:%M:%S') instalação iniciada =====" >> "$LOG_FILE"

# --- Saída amigável -----------------------------------------------------------
if [[ -t 1 ]]; then C_B=$'\033[1m'; C_G=$'\033[32m'; C_Y=$'\033[33m'; C_R=$'\033[31m'; C_0=$'\033[0m'
else C_B=""; C_G=""; C_Y=""; C_R=""; C_0=""; fi
STEP=0
TOTAL_STEPS=6
step() { STEP=$((STEP + 1)); echo; echo "${C_B}[${STEP}/${TOTAL_STEPS}] $*${C_0}"; }
info() { echo "  - $*"; }
ok()   { echo "  ${C_G}✔${C_0} $*"; }
warn() { echo "  ${C_Y}!${C_0} $*"; }
die()  { echo; echo "${C_R}✖ ERRO:${C_0} $*"; echo "  Detalhes em: $LOG_FILE"; exit 1; }

on_error() {
    local code=$? line=$1
    echo "${C_R}✖ Falha inesperada (linha $line, código $code).${C_0} Veja $LOG_FILE"
    echo "  Você pode rodar ./install.sh novamente: as etapas já concluídas são puladas."
}
trap 'on_error $LINENO' ERR

confirm() {
    # confirm "pergunta" -> 0 = sim. Com --yes ou sem terminal, assume sim.
    [[ $ASSUME_YES -eq 1 || ! -t 0 ]] && return 0
    local reply
    read -r -p "  $1 [S/n] " reply || true
    [[ -z "$reply" || "$reply" =~ ^[sSyY] ]]
}

SUDO=""
if [[ $EUID -ne 0 ]]; then
    if command -v sudo >/dev/null 2>&1; then SUDO="sudo"; fi
else
    warn "Executando como root. Recomendado: usuário comum (o sudo será usado quando necessário)."
fi
as_root() {
    if [[ $EUID -eq 0 ]]; then "$@"
    elif [[ -n "$SUDO" ]]; then $SUDO "$@"
    else die "É necessário privilégio de administrador para: $* (instale o sudo ou rode as etapas manualmente)"; fi
}

download() {
    # download URL DESTINO - com retentativas; reaproveita arquivo já baixado
    local url=$1 dest=$2
    if [[ -s "$dest" ]]; then info "Usando arquivo em cache: $dest"; return 0; fi
    info "Baixando $url"
    curl -fL --retry 4 --retry-delay 2 --connect-timeout 30 -o "$dest.part" "$url" \
        || die "Falha ao baixar $url (verifique a conexão/proxy)."
    mv "$dest.part" "$dest"
}

# --- 1. Pacotes do sistema ----------------------------------------------------
DISTRO_ID=""; DISTRO_LIKE=""; DISTRO_CODENAME=""; UBUNTU_CODENAME_VAL=""
detect_distro() {
    if [[ -r /etc/os-release ]]; then
        # shellcheck disable=SC1091
        . /etc/os-release
        DISTRO_ID="${ID:-}"; DISTRO_LIKE="${ID_LIKE:-}"
        DISTRO_CODENAME="${VERSION_CODENAME:-}"; UBUNTU_CODENAME_VAL="${UBUNTU_CODENAME:-}"
    fi
    case " $DISTRO_ID $DISTRO_LIKE " in
        *" debian "*|*" ubuntu "*) PKG_FAMILY="debian" ;;
        *" fedora "*|*" rhel "*|*" centos "*) PKG_FAMILY="fedora" ;;
        *" arch "*) PKG_FAMILY="arch" ;;
        *" suse "*|*" opensuse "*) PKG_FAMILY="suse" ;;
        *) PKG_FAMILY="unknown" ;;
    esac
}

wine_major() {
    command -v wine >/dev/null 2>&1 || { echo 0; return; }
    wine --version 2>/dev/null | sed -E 's/^wine-([0-9]+).*/\1/;t;s/.*/0/'
}

install_wine_debian() {
    if [[ "$(wine_major)" -ge $WINE_MIN_MAJOR ]]; then ok "Wine $(wine --version) já instalado"; return; fi
    local repo_os="debian" codename="$DISTRO_CODENAME"
    if [[ "$DISTRO_ID" == "ubuntu" || " $DISTRO_LIKE " == *" ubuntu "* ]]; then
        repo_os="ubuntu"; codename="${UBUNTU_CODENAME_VAL:-$DISTRO_CODENAME}"
    fi
    info "Instalando Wine estável do repositório oficial WineHQ ($repo_os/$codename)"
    as_root dpkg --add-architecture i386
    as_root mkdir -pm755 /etc/apt/keyrings
    local tmp_key tmp_src
    tmp_key="$(mktemp)"; tmp_src="$(mktemp)"
    if [[ -n "$codename" ]] \
        && curl -fsSL --retry 3 -o "$tmp_key" https://dl.winehq.org/wine-builds/winehq.key \
        && curl -fsSL --retry 3 -o "$tmp_src" "https://dl.winehq.org/wine-builds/$repo_os/dists/$codename/winehq-$codename.sources"; then
        as_root install -m 644 "$tmp_key" /etc/apt/keyrings/winehq-archive.key
        as_root install -m 644 "$tmp_src" "/etc/apt/sources.list.d/winehq-$codename.sources"
        as_root apt-get update
        if as_root apt-get install -y --install-recommends winehq-stable; then
            rm -f "$tmp_key" "$tmp_src"; ok "Wine $(wine --version) instalado"; return
        fi
        warn "winehq-stable indisponível; usando o Wine da distribuição"
    else
        warn "Repositório WineHQ indisponível para '$codename'; usando o Wine da distribuição"
    fi
    rm -f "$tmp_key" "$tmp_src"
    as_root apt-get update
    as_root apt-get install -y wine wine64 || as_root apt-get install -y wine
}

install_system_packages() {
    local label="Python, tkinter, curl, Xvfb"
    if [[ $INSTALL_MT5 -eq 1 ]]; then label="$label, Wine"; fi
    step "Pacotes do sistema ($label)"
    if [[ $SYSTEM_PACKAGES -eq 0 ]]; then warn "Pulado (--skip-system-packages)"; return; fi
    detect_distro
    info "Distribuição: ${DISTRO_ID:-desconhecida} (família $PKG_FAMILY)"
    case "$PKG_FAMILY" in
        debian)
            export DEBIAN_FRONTEND=noninteractive
            as_root apt-get update
            as_root apt-get install -y python3 python3-venv python3-pip python3-tk curl ca-certificates xvfb
            if [[ $INSTALL_MT5 -eq 1 ]]; then install_wine_debian; fi ;;
        fedora)
            as_root dnf install -y python3 python3-pip python3-tkinter curl xorg-x11-server-Xvfb
            if [[ $INSTALL_MT5 -eq 1 ]]; then as_root dnf install -y wine; fi ;;
        arch)
            as_root pacman -Sy --needed --noconfirm python python-pip tk curl xorg-server-xvfb
            if [[ $INSTALL_MT5 -eq 1 ]]; then
                as_root pacman -S --needed --noconfirm wine \
                    || die "Falha ao instalar o Wine. Habilite o repositório [multilib] em /etc/pacman.conf e rode novamente."
            fi ;;
        suse)
            as_root zypper --non-interactive install python3 python3-pip python3-tk curl xvfb-run
            if [[ $INSTALL_MT5 -eq 1 ]]; then as_root zypper --non-interactive install wine; fi ;;
        *)
            warn "Distribuição não reconhecida. Instale manualmente: python3 (3.9+), venv, tkinter, curl, Xvfb e Wine $WINE_MIN_MAJOR+."
            warn "Depois rode: ./install.sh --skip-system-packages" ;;
    esac
    ok "Pacotes do sistema verificados"
}

# --- 2. Ambiente Python -------------------------------------------------------
py_ok() {
    # py_ok BIN [tk] -> 0 se BIN é Python 3.MIN_PY_MINOR+ (e tem tkinter, se pedido)
    local code="import sys; sys.exit(0 if sys.version_info >= (3, $MIN_PY_MINOR) else 1)"
    [[ "${2:-}" == "tk" ]] && code="import tkinter; $code"
    command -v "$1" >/dev/null 2>&1 && "$1" -c "$code" >/dev/null 2>&1
}

pick_python() {
    if [[ -n "$PYTHON_BIN" ]]; then echo "$PYTHON_BIN"; return; fi
    local c want
    # 1ª passada: exige tkinter (interface gráfica); 2ª: qualquer Python compatível
    for want in tk any; do
        for c in python3.11 python3.12 python3.10 python3.13 python3.9 python3; do
            if py_ok "$c" "$want"; then echo "$c"; return; fi
        done
    done
    echo ""
}

setup_python_env() {
    step "Ambiente virtual Python e dependências"
    local py
    py="$(pick_python)"
    [[ -n "$py" ]] || die "Python 3.$MIN_PY_MINOR+ não encontrado. Instale python3 e rode novamente."
    info "Usando $("$py" --version 2>&1) ($(command -v "$py"))"
    if [[ -x "$VENV_PY" ]] && ! py_ok "$VENV_PY" tk && py_ok "$py" tk; then
        warn "O .venv atual não tem tkinter; recriando com $py"
        rm -rf "$VENV_DIR"
    fi
    if [[ ! -x "$VENV_PY" ]]; then
        "$py" -m venv "$VENV_DIR" \
            || die "Falha ao criar o .venv. Debian/Ubuntu: sudo apt install python3-venv"
        ok "Ambiente virtual criado em .venv"
    else
        ok "Ambiente virtual já existe (.venv)"
    fi
    "$VENV_PY" -m pip install --upgrade pip --quiet
    info "Instalando dependências (pode levar alguns minutos)..."
    "$VENV_PY" -m pip install -r "$PROJECT_ROOT/requirements.txt" \
        || die "Falha ao instalar dependências de requirements.txt"
    if "$VENV_PY" -m pip install -r "$PROJECT_ROOT/requirements-optional.txt" --quiet 2>/dev/null; then
        ok "Dependências opcionais instaladas"
    else
        warn "Dependências opcionais (pandas-ta) indisponíveis; o app usará indicadores básicos"
    fi
    ok "Dependências Python instaladas"
}

# --- Display para o Wine (Xvfb quando não há sessão gráfica) ------------------
XVFB_PID=""
ensure_display() {
    if [[ -n "${DISPLAY:-}" ]]; then return; fi
    if command -v Xvfb >/dev/null 2>&1; then
        local n=99
        while [[ -e "/tmp/.X11-unix/X$n" || -e "/tmp/.X$n-lock" ]]; do n=$((n + 1)); done
        Xvfb ":$n" -screen 0 1280x800x24 -nolisten tcp >/dev/null 2>&1 &
        XVFB_PID=$!
        export DISPLAY=":$n"
        sleep 2
        info "Sem sessão gráfica: usando display virtual Xvfb $DISPLAY"
    else
        warn "Sem DISPLAY e sem Xvfb; instaladores do Wine podem falhar."
    fi
}
cleanup() {
    if [[ -n "$XVFB_PID" ]]; then
        kill "$XVFB_PID" 2>/dev/null || true
        XVFB_PID=""
        unset DISPLAY
    fi
}
trap cleanup EXIT

wine_env() {
    export WINEPREFIX WINEARCH=win64 WINEDEBUG=-all
    # Evita diálogos de instalação do Mono/Gecko (instalação sem interação)
    export WINEDLLOVERRIDES="mscoree,mshtml="
}

# --- 3. Wine + MetaTrader 5 ---------------------------------------------------
mt5_installed() {
    "$VENV_PY" -m mt5_extracao.bootstrap locate-mt5 --wine-prefix "$WINEPREFIX" >/dev/null 2>&1
}

setup_wine_and_mt5() {
    step "Wine e MetaTrader 5"
    if [[ $INSTALL_MT5 -eq 0 ]]; then warn "Pulado (--no-mt5)"; return; fi
    command -v wine >/dev/null 2>&1 || die "Wine não encontrado. Rode sem --skip-system-packages ou instale o Wine $WINE_MIN_MAJOR+."
    [[ "$(wine_major)" -ge $WINE_MIN_MAJOR ]] || warn "Wine $(wine --version) é antigo; recomendado $WINE_MIN_MAJOR+."
    wine_env
    ensure_display
    if [[ ! -f "$WINEPREFIX/system.reg" ]]; then
        info "Criando prefixo Wine em $WINEPREFIX"
        mkdir -p "$WINEPREFIX"
        wineboot --init >/dev/null 2>&1 || true
        wineserver -w || true
        wine reg add 'HKCU\Software\Wine' /v Version /d win10 /f >/dev/null 2>&1 || true
        wineserver -w || true
        ok "Prefixo Wine criado (Windows 10)"
    else
        ok "Prefixo Wine já existe: $WINEPREFIX"
    fi

    if mt5_installed; then
        ok "MetaTrader 5 já instalado no prefixo"
        return
    fi
    confirm "Baixar e instalar o MetaTrader 5 agora?" || { warn "Instalação do MT5 pulada pelo usuário"; return; }
    local setup="$CACHE_DIR/mt5setup.exe"
    download "$MT5_SETUP_URL" "$setup"
    info "Executando o instalador do MT5 em modo automático (/auto)..."
    wine "$setup" /auto >/dev/null 2>&1 || true
    local waited=0
    until mt5_installed; do
        if [[ $waited -ge 600 ]]; then die "O MT5 não foi instalado em 10 minutos. Tente: WINEPREFIX=$WINEPREFIX wine $setup"; fi
        sleep 5; waited=$((waited + 5))
    done
    # O instalador abre o terminal ao final; fecha tudo para seguir a instalação
    sleep 5
    wineserver -k || true
    ok "MetaTrader 5 instalado"
}

# --- 4. Python do Windows no Wine (ponte) -------------------------------------
setup_wine_python() {
    step "Ponte Linux <-> MT5 (Python do Windows no Wine)"
    if [[ $INSTALL_MT5 -eq 0 ]]; then warn "Pulado (--no-mt5)"; return; fi
    wine_env
    ensure_display
    local wine_py_unix="$WINEPREFIX/$WIN_PYTHON_DIR_UNIX_REL/python.exe"
    local wine_py_win="$WIN_PYTHON_DIR_WIN\\python.exe"
    if [[ ! -f "$wine_py_unix" ]]; then
        local installer="$CACHE_DIR/python-$WIN_PYTHON_VERSION-amd64.exe"
        download "$WIN_PYTHON_URL" "$installer"
        info "Instalando Python $WIN_PYTHON_VERSION (Windows) no Wine..."
        wine "$installer" /quiet InstallAllUsers=0 "TargetDir=$WIN_PYTHON_DIR_WIN" PrependPath=0 \
            Include_test=0 Include_doc=0 Include_launcher=0 Include_tcltk=0 >/dev/null 2>&1 || true
        wineserver -w || true
        [[ -f "$wine_py_unix" ]] || die "Python do Windows não foi instalado no Wine ($wine_py_unix)."
        ok "Python do Windows instalado no Wine"
    else
        ok "Python do Windows já instalado no Wine"
    fi
    info "Instalando MetaTrader5 e rpyc no Python do Wine..."
    wine "$wine_py_win" -m pip install --upgrade pip --quiet --disable-pip-version-check 2>/dev/null || true
    wine "$wine_py_win" -m pip install --quiet --disable-pip-version-check MetaTrader5 "rpyc==$RPYC_VERSION" \
        || die "Falha ao instalar MetaTrader5/rpyc no Python do Wine."
    local server_win
    server_win="$(winepath -w "$PROJECT_ROOT/scripts/mt5_bridge_server.py" 2>/dev/null)"
    wine "$wine_py_win" "$server_win" --check >/dev/null 2>&1 \
        || die "A ponte não conseguiu importar MetaTrader5 no Wine. Veja $LOG_FILE"
    wineserver -w || true
    ok "Ponte pronta (MetaTrader5 + rpyc $RPYC_VERSION no Wine)"
}

# --- 5. Configuração ----------------------------------------------------------
configure_app() {
    step "Configuração inicial (config/config.ini e .env)"
    local args=(configure --wine-prefix "$WINEPREFIX" --no-doctor)
    if [[ $INSTALL_MT5 -eq 0 ]]; then args+=(--no-bridge); fi
    if [[ $ASSUME_YES -eq 1 || ! -t 0 ]]; then args+=(--non-interactive); fi
    "$VENV_PY" -m mt5_extracao.bootstrap "${args[@]}" || die "Falha na configuração inicial."
    chmod +x "$PROJECT_ROOT/run.sh" "$PROJECT_ROOT/scripts/mt5-bridge.sh" 2>/dev/null || true
    ok "Configuração concluída"
}

# --- 6. Atalho, ponte e diagnóstico ------------------------------------------
finish() {
    step "Atalho, ponte e diagnóstico"
    local apps_dir="${XDG_DATA_HOME:-$HOME/.local/share}/applications"
    if mkdir -p "$apps_dir" 2>/dev/null; then
        cat > "$apps_dir/mt5-extracao.desktop" <<EOF
[Desktop Entry]
Type=Application
Name=MT5 Extração de Dados
Comment=Extração de dados do MetaTrader 5
Exec="$PROJECT_ROOT/run.sh"
Path=$PROJECT_ROOT
Terminal=false
Categories=Office;Finance;
EOF
        ok "Atalho criado no menu de aplicativos"
    fi
    cleanup
    if [[ $INSTALL_MT5 -eq 1 ]] && [[ -x "$PROJECT_ROOT/scripts/mt5-bridge.sh" ]]; then
        "$PROJECT_ROOT/scripts/mt5-bridge.sh" start || warn "Não foi possível iniciar a ponte agora (veja logs/mt5_bridge.log)"
    fi
    echo
    if "$VENV_PY" -m mt5_extracao.bootstrap doctor; then DOCTOR_OK=1; else DOCTOR_OK=0; fi
}

main() {
    echo "${C_B}MT5 Extração - instalação automática (Linux)${C_0}"
    info "Projeto: $PROJECT_ROOT"
    info "Prefixo Wine: $WINEPREFIX"
    install_system_packages
    setup_python_env
    setup_wine_and_mt5
    setup_wine_python
    configure_app
    finish
    echo
    if [[ $DOCTOR_OK -eq 1 ]]; then
        echo "${C_G}${C_B}Instalação concluída!${C_0}"
    else
        echo "${C_Y}${C_B}Instalação concluída com pendências.${C_0} Corrija os itens [FALHA] acima (cada um traz a dica)"
        echo "  e rode ./install.sh novamente - as etapas prontas são puladas."
    fi
    echo "  Iniciar o aplicativo:   ./run.sh   (ou pelo menu: 'MT5 Extração de Dados')"
    echo "  Diagnóstico:            .venv/bin/python -m mt5_extracao.bootstrap doctor"
    echo "  Ponte MT5 (Wine):       ./scripts/mt5-bridge.sh start|stop|status"
    echo "  Log da instalação:      $LOG_FILE"
}

main
