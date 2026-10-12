"""
Auditoria de segurança: do projeto (segredos, ponte, dependências), patrimonial (o que pode mexer
na conta) e de gestão do capital (risco, critérios de aprovação, custos dos testes).

    findings = run_audit(root, config, db)      # lista de Finding; é o que 'mt5x audit' mostra

Só lê arquivos, configuração e banco. Não lê o conteúdo do .env (só as permissões), não fala com o
MT5 (a conta é informada por quem chama) e não corrige nada sozinha: cada achado diz o que fazer.
"""
import os
import re
import stat
import subprocess
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

CRITICAL, HIGH, MEDIUM, LOW, INFO = "crítico", "alto", "médio", "baixo", "info"
SEVERITIES = (CRITICAL, HIGH, MEDIUM, LOW, INFO)
PROJECT, ASSETS, CAPITAL = "projeto", "patrimonial", "capital"
FAIL, OK, SKIPPED = "falha", "ok", "não verificado"

# Versão mínima sem falhas conhecidas (alertas do Dependabot em 2026-10). Não substitui o Dependabot
# nem o pip-audit: é a rede de segurança para instalações antigas.
SAFE_VERSIONS = {"pillow": "12.3.0", "cryptography": "49.0.0", "requests": "2.33.0", "python-dotenv": "1.2.2"}

_SECRET_FILES = re.compile(r"(^|/)(\.env|credentials\.json|.*\.key|.*\.pem|config/config\.ini)$|\.(db|sqlite)$")
_SECRET_IN_LOG = re.compile(r"(password|senha|passwd|token)\s*[=:]\s*['\"]?[^\s'\"*]{4,}", re.I)
_ORDER_CALL = re.compile(r"\border_(send|send_async)\b")


@dataclass
class Finding:
    id: str
    area: str
    severity: str
    status: str
    title: str
    detail: str = ""
    fix: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


def _version(text: str):
    return tuple(int(n) for n in re.findall(r"\d+", text)[:4])


def _get(config, section, key, fallback):
    try:
        return config.getfloat(section, key, fallback=fallback) if config is not None else fallback
    except ValueError:
        return fallback


# --- projeto --------------------------------------------------------------------------

def check_tracked_secrets(root: Path) -> Finding:
    f = Finding("P1", PROJECT, CRITICAL, OK, "Segredos e bases fora do controle de versão")
    if not (root / ".git").exists():
        f.status, f.detail = SKIPPED, "pasta sem repositório git"
        return f
    try:
        tracked = subprocess.run(["git", "ls-files"], cwd=str(root), capture_output=True, text=True, timeout=20)
    except (OSError, subprocess.SubprocessError) as e:
        f.status, f.detail = SKIPPED, f"git indisponível: {e}"
        return f
    bad = [line for line in tracked.stdout.splitlines() if _SECRET_FILES.search(line) and not line.endswith(".example")]
    if bad:
        f.status, f.detail = FAIL, "versionados: " + ", ".join(bad[:10])
        f.fix = "git rm --cached <arquivo>, troque as senhas expostas e confira o .gitignore."
    return f


def check_env_permissions(root: Path) -> Finding:
    f = Finding("P2", PROJECT, HIGH, OK, "Arquivo .env legível só pelo dono")
    env = root / ".env"
    if not env.exists():
        f.status, f.detail = SKIPPED, "não há .env (login automático desligado)"
    elif os.name != "posix":
        f.status, f.detail = SKIPPED, "permissões POSIX não se aplicam ao Windows; confira a ACL da pasta"
    elif stat.S_IMODE(env.stat().st_mode) & 0o077:
        f.status, f.detail = FAIL, f"permissões {stat.S_IMODE(env.stat().st_mode):o}: outros usuários podem ler a senha"
        f.fix = "chmod 600 .env"
    return f


def check_config_secrets(config) -> Finding:
    f = Finding("P3", PROJECT, HIGH, OK, "Sem senha no config.ini")
    found = []
    if config is not None:
        for section in config.sections():
            for key, value in config.items(section):
                if re.search(r"pass|senha|secret|token", key, re.I) and value.strip():
                    found.append(f"[{section}] {key}")
                if key == "url" and re.search(r"://[^/@\s:]+:[^/@\s]+@", value):
                    found.append(f"[{section}] url (senha na URL)")
    if found:
        f.status, f.detail = FAIL, "preenchidos: " + ", ".join(found)
        f.fix = "Mova para o .env (MT5_PASSWORD, DB_PASSWORD) e apague do config.ini."
    return f


def check_logs(root: Path, max_bytes: int = 2_000_000) -> Finding:
    f = Finding("P4", PROJECT, HIGH, OK, "Sem senha ou token nos logs")
    hits = []
    for path in sorted((root / "logs").glob("*.log*")) if (root / "logs").is_dir() else []:
        try:
            with open(path, "rb") as handle:
                handle.seek(max(0, path.stat().st_size - max_bytes))
                text = handle.read().decode("utf-8", errors="replace")
        except OSError:
            continue
        if _SECRET_IN_LOG.search(text):
            hits.append(path.name)
    if hits:
        f.status, f.detail = FAIL, "padrão de senha/token em: " + ", ".join(hits[:8])
        f.fix = "Apague esses logs, troque a senha e corrija a mensagem que a registrou."
    return f


def check_bridge(root: Path, config) -> List[Finding]:
    enabled = config is not None and config.get("BRIDGE", "enabled", fallback="false").strip().lower() == "true"
    host = config.get("BRIDGE", "host", fallback="127.0.0.1").strip() if config is not None else "127.0.0.1"
    exposed = Finding("P5", PROJECT, CRITICAL, OK, "Ponte do MT5 só na própria máquina")
    if not enabled:
        exposed.status, exposed.detail = SKIPPED, "ponte desligada (Windows ou MT5 local)"
    elif host not in ("127.0.0.1", "localhost", "::1"):
        exposed.status = FAIL
        exposed.detail = f"[BRIDGE] host = {host}: qualquer máquina da rede alcança o MT5 logado na sua conta"
        exposed.fix = "Use host = 127.0.0.1 e reinicie a ponte (./scripts/mt5-bridge.sh restart)."
    open_service = Finding("A1", ASSETS, HIGH, OK, "Ponte do MT5 só aceita funções de leitura")
    server = root / "scripts" / "mt5_bridge_server.py"
    if not enabled:
        open_service.status, open_service.detail = SKIPPED, "ponte desligada"
    elif server.is_file() and "SlaveService" in server.read_text(encoding="utf-8", errors="replace"):
        open_service.status = FAIL
        open_service.detail = ("a ponte usa o modo aberto do RPyC: qualquer programa deste computador pode executar "
                               "código no Python do Wine e enviar ordens pela conta logada no MT5")
        open_service.fix = "Atualize para a ponte restrita (lista de funções de leitura) e reinicie a ponte."
    return [exposed, open_service]


def check_panel() -> Finding:
    """O painel vivo executa ações: só pode escutar na própria máquina e precisa de chave."""
    from mt5_extracao.robots import panel_server
    f = Finding("P7", PROJECT, CRITICAL, OK, "Painel vivo só na própria máquina e com chave")
    if panel_server.HOST not in ("127.0.0.1", "localhost", "::1"):
        f.status = FAIL
        f.detail = f"o painel escutaria em {panel_server.HOST}: qualquer máquina da rede poderia acionar os botões"
        f.fix = "Volte panel_server.HOST para 127.0.0.1."
    elif not getattr(panel_server.PanelApp, "check_key", None):
        f.status, f.detail = FAIL, "o painel não confere a chave da sessão"
        f.fix = "Restaure PanelApp.check_key: sem ela, qualquer página aberta no navegador dispara ações."
    return f


def check_python_orders(root: Path) -> Finding:
    f = Finding("A2", ASSETS, CRITICAL, OK, "O Python não envia ordens")
    files = list((root / "mt5_extracao").rglob("*.py")) + list((root / "scripts").rglob("*.py")) + [root / "app.py"]
    bad = []
    for path in files:
        if path.is_file() and path.name != "audit.py" and \
                _ORDER_CALL.search(path.read_text(encoding="utf-8", errors="replace")):
            bad.append(str(path.relative_to(root)))
    if bad:
        f.status, f.detail = FAIL, "chamadas de envio de ordem em: " + ", ".join(bad)
        f.fix = "Remova: quem opera é o robô MQL5 dentro do MT5, pela camada de segurança (docs/robos.md)."
    return f


def check_dependencies(installed: Optional[dict] = None) -> Finding:
    f = Finding("P6", PROJECT, HIGH, OK, "Dependências sem falhas conhecidas")
    if installed is None:
        from importlib import metadata
        installed = {}
        for name in SAFE_VERSIONS:
            try:
                installed[name] = metadata.version(name)
            except metadata.PackageNotFoundError:
                pass
    old = [f"{name} {installed[name]} (mínimo {minimum})" for name, minimum in SAFE_VERSIONS.items()
           if name in installed and _version(installed[name]) < _version(minimum)]
    if old:
        f.status, f.detail = FAIL, "; ".join(old)
        f.fix = "pip install -U -r requirements.txt (no .venv)."
    return f


# --- patrimonial ----------------------------------------------------------------------

def check_account(account: Optional[dict], config) -> List[Finding]:
    real = Finding("A3", ASSETS, HIGH, OK, "Conta conectada é de demonstração")
    if account is None:
        real.status, real.detail = SKIPPED, "use 'mt5x audit --mt5' para consultar a conta conectada"
    elif not account.get("demo"):
        real.status = FAIL
        real.detail = f"conta {account.get('login')} em {account.get('server')} é REAL"
        real.fix = "Para pesquisa e testes, conecte uma conta demo (python -m mt5_extracao.bootstrap demo-account)."
    else:
        real.detail = f"conta {account.get('login')} em {account.get('server')}"
    allow = Finding("A4", ASSETS, HIGH, OK, "Operação em conta real desligada")
    if config is not None and config.get("ROBOTS", "allow_real", fallback="false").strip().lower() == "true":
        allow.status, allow.detail = FAIL, "[ROBOTS] allow_real = true"
        allow.fix = "Deixe false até um robô cumprir a bateria e o período em demo; ligar é decisão de uma pessoa."
    return [real, allow]


def check_robots(catalog) -> List[Finding]:
    """Robôs MQL5 do catálogo: risco alto, DLL, sem fonte e ordens fora da camada de segurança."""
    from mt5_extracao.robots import mql
    findings = []
    for robot in (r for r in catalog if r.kind == "mql5"):
        label = f"{robot.name} ({robot.key})"
        if not robot.has_source:
            findings.append(Finding("A5", ASSETS, MEDIUM, FAIL, f"Robô sem código-fonte: {label}",
                                    "não dá para verificar stop, limites nem o que ele envia",
                                    "Não rode em conta real; peça o .mq5 ou trate como caixa-preta só em demo."))
            continue
        if robot.dll_imports:
            findings.append(Finding("A6", ASSETS, HIGH, FAIL, f"Robô usa DLL externa: {label}",
                                    ", ".join(robot.dll_imports) + ": código fora do MT5, com acesso ao computador",
                                    "Só permita DLL de origem conhecida; no MT5, deixe 'Permitir DLL' desligado."))
        risky = {name: r for name, r in {"padrão do código": robot.risk, **robot.preset_risks}.items()
                 if r.level == "Alto"}
        for name, rating in risky.items():
            findings.append(Finding("C5", CAPITAL, HIGH, FAIL, f"Risco alto em {label}, {name}",
                                    rating.summary, "Use um preset de risco baixo ou moderado, com stop e limite diário."))
        safety = [p for p in robot.files if "/safety/" in p.replace("\\", "/").lower()]
        if safety:        # robô com camada de segurança: nada fora dela pode enviar ordem
            outside = [p for p in robot.files if p not in safety and p.lower().endswith((".mq5", ".mqh"))]
            loose = [Path(p).name for p in outside
                     if mql.ORDER_CALL.search(re.sub(r"//[^\n]*", "", mql.read_text(p)))]
            if loose:
                findings.append(Finding("A7", ASSETS, HIGH, FAIL, f"Ordem fora da camada de segurança: {label}",
                                        "envio direto em: " + ", ".join(loose),
                                        "Toda ordem deve passar pelo executor seguro (TryEnter / SafeExecutor)."))
    if not any(f.id in ("A5", "A6", "A7") for f in findings):
        findings.append(Finding("A5", ASSETS, MEDIUM, OK, "Robôs MQL5 com fonte, sem DLL e sem ordem fora da camada de segurança"))
    return findings


# --- capital --------------------------------------------------------------------------

def check_capital(config) -> List[Finding]:
    findings = []
    risk = _get(config, "STRATEGY", "risk_per_trade", 0.01)
    f = Finding("C1", CAPITAL, HIGH if risk > 0.02 else MEDIUM, OK, "Risco por operação até 1% do capital",
                f"risk_per_trade = {risk:g}")
    if risk > 0.01:
        f.status = FAIL
        f.detail += f": {risk * 100:g}% por operação; dez perdas seguidas levam {(1 - (1 - risk) ** 10) * 100:.0f}% do capital"
        f.fix = "Em [STRATEGY], use risk_per_trade = 0.01 ou menos."
    findings.append(f)

    loose = []
    if _get(config, "STRATEGY", "max_drawdown", 0.20) > 0.20:
        loose.append(f"max_drawdown = {_get(config, 'STRATEGY', 'max_drawdown', 0.20):g} (padrão 0.20)")
    if _get(config, "STRATEGY", "min_profit_factor", 1.3) < 1.3:
        loose.append(f"min_profit_factor = {_get(config, 'STRATEGY', 'min_profit_factor', 1.3):g} (padrão 1.3)")
    if _get(config, "STRATEGY", "min_trades", 100) < 100:
        loose.append(f"min_trades = {_get(config, 'STRATEGY', 'min_trades', 100):g} (padrão 100)")
    f = Finding("C2", CAPITAL, MEDIUM, OK, "Critérios de aprovação não foram afrouxados")
    if loose:
        f.status, f.detail = FAIL, "; ".join(loose)
        f.fix = "Volte aos padrões em [STRATEGY]: critério frouxo aprova estratégia que não se sustenta."
    findings.append(f)

    f = Finding("C3", CAPITAL, MEDIUM, OK, "Testes consideram corretagem e slippage")
    if _get(config, "STRATEGY", "commission_per_lot", 0.0) == 0 and _get(config, "STRATEGY", "slippage_points", 0.0) == 0:
        f.status = FAIL
        f.detail = "commission_per_lot = 0 e slippage_points = 0: só o spread entra no custo, o resultado fica otimista"
        f.fix = "Preencha em [STRATEGY] a corretagem por lote da sua corretora e um slippage realista."
    findings.append(f)
    return findings


def check_validations(db, now: Optional[datetime] = None, max_age_days: int = 90) -> List[Finding]:
    """Aprovações antigas ou sem especificação de contrato não devem sustentar decisão de capital."""
    from mt5_extracao.strategies import specs
    from mt5_extracao.strategies.validation import load_validations
    f = Finding("C4", CAPITAL, MEDIUM, OK, "Aprovações recentes e com especificação de contrato")
    if db is None or getattr(db, "engine", None) is None:
        f.status, f.detail = SKIPPED, "banco indisponível"
        return [f]
    approved = load_validations(db)
    approved = approved[approved["approved"] == 1] if len(approved) else approved
    now = now or datetime.now()
    problems = []
    for row in approved.itertuples():
        name = f"{row.strategy} em {row.symbol} {row.timeframe}"
        try:
            if now - datetime.strptime(str(row.created_at)[:19], "%Y-%m-%d %H:%M:%S") > timedelta(days=max_age_days):
                problems.append(f"{name}: validada em {str(row.created_at)[:10]}")
        except ValueError:
            pass
        if not specs.load_spec(db, row.symbol).known:
            problems.append(f"{name}: sem especificação de contrato")
    if problems:
        f.status, f.detail = FAIL, "; ".join(problems[:8])
        f.fix = "Rode 'mt5x specs' e valide de novo com dados atuais ('mt5x update' e 'mt5x validate')."
    elif not len(approved):
        f.detail = "nenhuma estratégia aprovada ainda"
    return [f]


# --- orquestração ---------------------------------------------------------------------

def run_audit(root, config=None, db=None, account: Optional[dict] = None, catalog=None,
              installed: Optional[dict] = None) -> List[Finding]:
    root = Path(root)
    if catalog is None:
        from mt5_extracao import robots
        catalog = robots.load_catalog(config, db)
    findings = [check_tracked_secrets(root), check_env_permissions(root), check_config_secrets(config),
                check_logs(root), *check_bridge(root, config), check_panel(), check_python_orders(root),
                check_dependencies(installed), *check_account(account, config), *check_robots(catalog),
                *check_capital(config), *check_validations(db)]
    order = {FAIL: 0, SKIPPED: 1, OK: 2}
    return sorted(findings, key=lambda f: (order[f.status], SEVERITIES.index(f.severity), f.id))


def failures(findings: List[Finding], minimum: str = HIGH) -> List[Finding]:
    """Achados reprovados com gravidade igual ou maior que `minimum`."""
    limit = SEVERITIES.index(minimum)
    return [f for f in findings if f.status == FAIL and SEVERITIES.index(f.severity) <= limit]


def summarize(findings: List[Finding]) -> dict:
    failed = [f for f in findings if f.status == FAIL]
    return {"total": len(findings), "failed": len(failed), "ok": sum(f.status == OK for f in findings),
            "skipped": sum(f.status == SKIPPED for f in findings),
            "by_severity": {s: sum(f.severity == s for f in failed) for s in SEVERITIES},
            "by_area": {a: sum(f.area == a for f in failed) for a in (PROJECT, ASSETS, CAPITAL)}}
