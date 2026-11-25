"""Classe de cada comando da CLI para o launcher: precisa da ponte MT5 ou só lê dados locais.

Fonte única usada por ``run.sh --cli``. Comandos locais abrem sem iniciar a ponte/terminal e rodam
com ``MT5X_LOCAL_ONLY=1``; se um deles tentar conectar ao MT5, a CLI recusa com erro explícito em vez
de falhar em silêncio. Comando desconhecido ou que pode usar o MT5 continua iniciando a ponte.
Sem importações pesadas: o launcher consulta este módulo a cada execução.
"""
import sys

# Leitura/análise sobre bases, recibos e arquivos locais; nenhum chama o conector MT5.
LOCAL_COMMANDS = frozenset({
    "dashboard", "tables", "quality", "export", "strategies", "analyze", "status", "validate", "screen",
    "commands", "report", "laboratory", "analytics", "trades", "experiments", "resources", "b3", "crypto",
})
# Opções globais da CLI que consomem o argumento seguinte.
_GLOBAL_WITH_VALUE = frozenset({"--config", "--db"})
_GLOBAL_FLAGS = frozenset({"--json", "-v", "--verbose"})
LOCAL_ENV = "MT5X_LOCAL_ONLY"


def _command_args(argv):
    """Primeiro argumento que não é opção global (o comando), ou ``None``."""
    index = 0
    while index < len(argv):
        token = argv[index]
        if token in _GLOBAL_WITH_VALUE:
            index += 2
        elif token in _GLOBAL_FLAGS or token.startswith(("--config=", "--db=")):
            index += 1
        else:
            return argv[index:]
    return []


def command_name(argv):
    args = _command_args(argv)
    return args[0] if args else None


def needs_bridge(argv):
    args = _command_args(argv)
    if args[:2] == ["laboratory", "ultra-market-capture"]:
        return True
    return not (args and (args[0] in LOCAL_COMMANDS or args[:2] == ["workflow", "methods"]))


def main(argv=None):
    """Código de saída 0 = comando local (sem ponte); 1 = iniciar a ponte."""
    return 1 if needs_bridge(sys.argv[1:] if argv is None else argv) else 0


if __name__ == "__main__":
    sys.exit(main())
