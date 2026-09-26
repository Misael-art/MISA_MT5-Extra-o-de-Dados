"""
CLI de configuração inicial e diagnóstico.

    python -m mt5_extracao.bootstrap configure [opções]
    python -m mt5_extracao.bootstrap doctor [--json] [--no-probe]
    python -m mt5_extracao.bootstrap locate-mt5 [--json]

Códigos de saída: 0 = sucesso, 1 = falha, 2 = uso incorreto.
"""
import argparse
import getpass
import json
import os
import sys
from pathlib import Path

from . import config_builder, doctor, mt5_locator, paths


def _print(msg: str = "") -> None:
    print(msg, flush=True)


def _ask(prompt: str, default: str = "") -> str:
    suffix = f" [{default}]" if default else ""
    try:
        answer = input(f"{prompt}{suffix}: ").strip()
    except EOFError:
        return default
    return answer or default


def _choose_installation(found, interactive: bool):
    if not found:
        return None
    if len(found) == 1 or not interactive:
        return found[0]
    _print("Foram encontradas várias instalações do MetaTrader 5:")
    for i, inst in enumerate(found, 1):
        _print(f"  {i}) {inst.directory}")
    while True:
        answer = _ask("Escolha o número", "1")
        if answer.isdigit() and 1 <= int(answer) <= len(found):
            return found[int(answer) - 1]
        _print("Opção inválida.")


def cmd_locate(args) -> int:
    prefixes = [Path(args.wine_prefix)] if args.wine_prefix else None
    found = mt5_locator.find_installations(wine_prefixes=prefixes)
    if args.json:
        _print(json.dumps([f.to_dict() for f in found], ensure_ascii=False, indent=2))
    elif not found:
        _print("Nenhuma instalação do MetaTrader 5 encontrada.")
    else:
        for f in found:
            _print(f"{f.directory}  ({f.windows_path})")
    return 0 if found else 1


def cmd_configure(args) -> int:
    root = Path(args.root).resolve()
    interactive = not args.non_interactive and sys.stdin.isatty()
    wine_prefix = Path(args.wine_prefix).expanduser() if args.wine_prefix else None

    config_builder.ensure_work_dirs(root)

    # 1. Localizar o MT5
    installation = None
    if args.mt5_path:
        installation = mt5_locator.installation_from_path(args.mt5_path, wine_prefix)
        if installation is None:
            _print(f"ERRO: terminal64.exe não encontrado em '{args.mt5_path}'.")
            return 1
    else:
        prefixes = [wine_prefix] if wine_prefix else None
        found = mt5_locator.find_installations(wine_prefixes=prefixes)
        installation = _choose_installation(found, interactive)
        if installation is None and interactive:
            manual = _ask("MT5 não encontrado. Informe a pasta do MetaTrader 5 (Enter para pular)")
            if manual:
                installation = mt5_locator.installation_from_path(manual, wine_prefix)
                if installation is None:
                    _print("Caminho inválido; o MT5 poderá ser configurado depois.")

    # 2. Montar overrides
    overrides = {"MT5": {}, "BRIDGE": {}}
    if installation is not None:
        overrides["MT5"]["path"] = str(installation.directory)
        overrides["MT5"]["windows_path"] = installation.windows_path
        if installation.wine_prefix:
            overrides["MT5"]["wine_prefix"] = str(installation.wine_prefix)
        _print(f"MetaTrader 5: {installation.directory}")
    else:
        _print("AVISO: MetaTrader 5 não localizado. Configure depois com --mt5-path.")

    if not paths.is_windows():
        overrides["BRIDGE"]["enabled"] = "false" if args.no_bridge else "true"
        overrides["BRIDGE"]["wine_python"] = args.wine_python or (paths.WINE_PYTHON_WINDOWS_DIR + "\\python.exe")
        if wine_prefix and "wine_prefix" not in overrides["MT5"]:
            overrides["MT5"]["wine_prefix"] = str(wine_prefix)
    if args.bridge_port:
        overrides["BRIDGE"]["port"] = str(args.bridge_port)
    if args.db_path:
        overrides["DATABASE"] = {"path": args.db_path}

    # 3. Escrever config.ini (preservando valores existentes)
    cfg_path = paths.config_path(root)
    existing = config_builder.load_config(cfg_path) if cfg_path.exists() and not args.reset else None
    cfg = config_builder.build_config(existing, overrides)
    config_builder.write_config(cfg_path, cfg)
    _print(f"Configuração salva em {cfg_path}")

    if config_builder.ensure_symbols_file(paths.symbols_path(root)):
        _print(f"Lista de símbolos padrão criada em {paths.symbols_path(root)}")

    # 4. Credenciais (opcionais) -> .env
    login = args.login or os.environ.get("MT5_LOGIN") or None
    server = args.server or os.environ.get("MT5_SERVER") or None
    password = os.environ.get(args.password_env) if args.password_env else None
    if interactive and not args.skip_credentials and not login:
        _print("")
        _print("Credenciais da conta MT5 são opcionais (o terminal pode usar a conta já logada).")
        if _ask("Deseja salvar login/servidor/senha agora? (s/N)", "n").lower().startswith("s"):
            login = _ask("Login (número da conta)") or None
            server = _ask("Servidor (ex.: XPMT5-DEMO)") or None
            password = getpass.getpass("Senha (não será exibida): ") or None
    if not args.skip_credentials and config_builder.write_env_credentials(
            paths.env_path(root), login, password, server):
        _print(f"Credenciais salvas em {paths.env_path(root)} (arquivo privado, fora do git)")

    # 5. Diagnóstico rápido
    if not args.no_doctor:
        _print("")
        checks = doctor.run_checks(root, probe_bridge_conn=False)
        _print(doctor.format_report(checks, color=sys.stdout.isatty()))
    return 0


def cmd_doctor(args) -> int:
    checks = doctor.run_checks(Path(args.root).resolve(), probe_bridge_conn=not args.no_probe)
    if args.json:
        _print(json.dumps(doctor.checks_to_dicts(checks), ensure_ascii=False, indent=2))
    else:
        _print(doctor.format_report(checks, color=sys.stdout.isatty()))
    return 1 if any(c.status == doctor.FAIL for c in checks) else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m mt5_extracao.bootstrap",
                                     description="Configuração inicial e diagnóstico do MT5 Extração.")
    parser.add_argument("--root", default=str(paths.PROJECT_ROOT), help="Raiz do projeto (padrão: automático)")
    sub = parser.add_subparsers(dest="command")

    p = sub.add_parser("configure", help="Gera config/config.ini, .env e pastas de trabalho")
    p.add_argument("--mt5-path", help="Pasta do MetaTrader 5 (ou caminho do terminal64.exe)")
    p.add_argument("--wine-prefix", help="Prefixo Wine onde o MT5 está (Linux)")
    p.add_argument("--wine-python", help=r"Python do Windows no Wine (padrão C:\Python311\python.exe)")
    p.add_argument("--bridge-port", type=int, help="Porta da ponte RPyC (padrão 18812)")
    p.add_argument("--no-bridge", action="store_true",
                   help="Linux: desativa a ponte Wine (instalação sem MT5)")
    p.add_argument("--db-path", help="Caminho do banco SQLite (padrão database/mt5_data.db)")
    p.add_argument("--login", help="Login da conta MT5 (opcional)")
    p.add_argument("--server", help="Servidor da corretora (opcional)")
    p.add_argument("--password-env", metavar="VAR",
                   help="Nome da variável de ambiente que contém a senha (nunca passe a senha na linha de comando)")
    p.add_argument("--skip-credentials", action="store_true", help="Não perguntar/gravar credenciais")
    p.add_argument("--non-interactive", action="store_true", help="Não fazer perguntas (usa padrões)")
    p.add_argument("--reset", action="store_true", help="Ignora o config.ini existente e recria com padrões")
    p.add_argument("--no-doctor", action="store_true", help="Não executar o diagnóstico ao final")
    p.set_defaults(func=cmd_configure)

    p = sub.add_parser("doctor", help="Verifica o ambiente e aponta correções")
    p.add_argument("--json", action="store_true")
    p.add_argument("--no-probe", action="store_true", help="Não tenta conectar na ponte MT5 (Linux)")
    p.set_defaults(func=cmd_doctor)

    p = sub.add_parser("locate-mt5", help="Lista instalações do MetaTrader 5 encontradas")
    p.add_argument("--wine-prefix", help="Procurar somente neste prefixo Wine (Linux)")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_locate)
    return parser


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "command", None):
        # Sem subcomando: assistente interativo completo
        args = parser.parse_args(["--root", args.root, "configure"])
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
