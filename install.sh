#!/usr/bin/env bash
# Instalação automática no Linux. Veja scripts/install/linux.sh --help
exec bash "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/scripts/install/linux.sh" "$@"
