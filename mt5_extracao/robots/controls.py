"""Contexto dos comandos MT5: evidência, consentimento e projeções, sem UI ou envio de ordens."""
import hashlib
import json
import math
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from . import catalog, contract

DEFAULT_PRESET = "Padrão do código"


def account_identity(mt5):
    """Leitura fresca; modo desconhecido/contest e carteiras ilegíveis não viram conta REAL."""
    terminal, account = mt5.terminal_info(), mt5.account_info()
    if not terminal or not terminal.connected or account is None:
        raise ValueError("A conta não está mais conectada ao MT5.")
    mode = getattr(account, "trade_mode", None)
    if mode not in (0, 2):
        raise ValueError("Tipo de conta não suportado ou não identificado; PLAY foi bloqueado.")
    balance = float(account.balance)
    if not account.login or not account.server or not account.currency or not math.isfinite(balance) or balance <= 0:
        raise ValueError("Identidade, moeda ou saldo da conta inválidos; PLAY foi bloqueado.")
    if not getattr(account, "trade_allowed", True):
        raise ValueError("A conta não tem permissão para negociar; PLAY foi bloqueado.")
    positions, orders = mt5.positions_get(), mt5.orders_get()
    if positions is None or orders is None:
        raise ValueError("A carteira não pôde ser lida por inteiro; PLAY foi bloqueado.")
    exposure = sorted((kind, int(item.ticket), str(item.symbol), int(item.magic),
                       str(getattr(item, "volume", getattr(item, "volume_current", ""))),
                       str(getattr(item, "price_open", "")), str(item.sl), str(item.tp))
                      for kind, items in (("position", positions), ("order", orders)) for item in items)
    signature = hashlib.sha256(json.dumps(exposure).encode()).hexdigest()
    return {"login": int(account.login), "server": str(account.server), "demo": mode == 0,
            "currency": str(account.currency), "balance": balance, "portfolio_signature": signature}


def effective_values(robot, preset=None):
    if preset and preset not in robot.presets:
        raise ValueError(f"Preset desconhecido: {preset}")
    values = {i.name: str(i.default) for i in robot.inputs}
    values.update({k: str(v) for k, v in robot.presets.get(preset, {}).items() if not k.startswith("__")})
    for item in robot.inputs:
        values[item.name] = _input_value(robot, item, values[item.name])
    return values


def _input_value(robot, item, value):
    text = str(value).split("||", 1)[0].strip()
    members = robot.enums.get(item.type, {})
    if text in members:
        return str(members[text])
    if item.type == "bool" and text.lower() in ("true", "false", "1", "0"):
        return "true" if text.lower() in ("true", "1") else "false"
    if item.type == "ENUM_TIMEFRAMES" and text.startswith("PERIOD_"):
        from mt5_extracao import timeframes
        tf = timeframes.parse(text[7:])
        if text == "PERIOD_CURRENT":
            return "0"
        if tf:
            return str(tf.mt5)
    return text


def native_values(robot, preset=None):
    """Só overrides no .set: defaults simbólicos/cores/datas ficam a cargo do compilador MQL5."""
    known = {i.name: i for i in robot.inputs}
    effective_values(robot, preset)  # valida a existência do preset
    values = {k: v for k, v in robot.presets.get(preset, {}).items() if not k.startswith("__")}
    unknown = set(values) - set(known)
    if unknown:
        raise ValueError("Preset contém parâmetros que não existem neste robô: " + ", ".join(sorted(unknown)))
    return {name: _input_value(robot, known[name], value) for name, value in values.items()}


def _canonical(value):
    text = str(value).split("||", 1)[0].strip()
    if text.lower() in ("true", "false"):
        return text.lower()
    try:
        number = Decimal(text)
        return str(number.normalize()) if number.is_finite() else text
    except InvalidOperation:
        return text


def matching_runs(robot, symbol, timeframe, preset=None):
    """Mesmo código e parâmetros efetivos; nunca aprovar outro preset ou teste parcial/cancelado."""
    expected = effective_values(robot, preset)
    fingerprint = catalog.fingerprint(robot)
    export = contract.input_name(robot, "export_trades") or "InpExportTrades"
    result = []
    for run in robot.runs:
        m = run.get("metrics") or {}
        try:
            count, net = float(m["trades"]), float(m["net_profit"])
            valid = (not isinstance(m["trades"], bool) and not isinstance(m["net_profit"], bool)
                     and math.isfinite(count) and count >= 0 and count.is_integer() and math.isfinite(net))
        except (KeyError, ValueError, TypeError, OverflowError):
            valid = False
        if (run.get("source") != "strategy tester" or run.get("symbol") != symbol
                or run.get("timeframe") != timeframe or m.get("source_fingerprint") != fingerprint
                or m.get("session_state", "completed") != "completed" or not valid):
            continue
        used = run.get("params") or {}
        # Nome mais valores: presets distintos não herdam elegibilidade um do outro.
        if (used.get("preset") or "") != (preset or ""):
            continue
        if any(k not in expected and k not in ("preset", export) and not k.startswith("__") for k in used):
            continue
        if any(_canonical(_input_value(robot, i, used.get(k, i.default))) != _canonical(expected[k])
               for i in robot.inputs for k in (i.name,) if k != export):
            continue
        result.append(run)
    return sorted(result, key=lambda r: (str(r.get("created_at", "")), str(r.get("run_id", ""))), reverse=True)


def selection(robot, symbol, timeframe, preset=None):
    runs = matching_runs(robot, symbol, timeframe, preset)
    headline = None
    if runs:
        run, metrics = runs[0], runs[0]["metrics"]
        headline = {"kind": "backtest", "symbol": run["symbol"], "timeframe": run["timeframe"],
                    "profit_factor": metrics.get("profit_factor"), "trades": metrics["trades"],
                    "max_drawdown": metrics.get("max_drawdown_pct"), "extra": metrics.get("win_rate"),
                    "extra_label": "acerto", "approved": None, "count": len(runs)}
    rating = robot.preset_risks.get(preset, robot.risk)
    return {"run": runs[0] if runs else None, "headline": headline,
            "risk": rating, "reason": "" if runs else "PLAY indisponível: faça backtest deste ativo, timeframe e preset no código atual."}


def loss_scenario(run):
    """Cenário histórico na escala do teste, sem converter moeda, saldo ou frequência."""
    metrics = (run or {}).get("metrics") or {}
    try:
        net, trades = float(metrics["net_profit"]), float(metrics["trades"])
        if not math.isfinite(net) or not math.isfinite(trades) or trades <= 0 or not trades.is_integer():
            return None
    except (KeyError, TypeError, ValueError, OverflowError):
        return None
    if net >= 0:
        return None
    return {"per_trade": net / trades, "loss_100": -net / trades * 100,
            "currency": str(metrics.get("currency") or "unidades monetárias do teste"),
            "trades": int(trades), "initial_equity": metrics.get("initial_equity"),
            "basis": "Média histórica × 100, mesmos lotes/risco, saldo e custos do teste. "
                     "Não é previsão nem valor convertido para sua conta. Frequência semanal não certificada."}


def risk_budget(robot, preset=None, account=None):
    rating = robot.preset_risks.get(preset, robot.risk)
    facts = dict(rating.facts, **contract.facts(robot, effective_values(robot, preset)))
    balance = (account or {}).get("balance")
    currency = str((account or {}).get("currency") or "")
    amounts = {}
    if isinstance(balance, (int, float)) and not isinstance(balance, bool) and math.isfinite(balance) and balance > 0 and currency:
        for role in ("risk_pct", "daily_loss_pct", "max_drawdown_pct"):
            pct = facts.get(role)
            if isinstance(pct, (int, float)) and not isinstance(pct, bool) and math.isfinite(pct) and pct > 0:
                amounts[role] = balance * pct / 100
    return {"facts": facts, "amounts": amounts, "currency": currency}


def context_digest(robot, symbol, timeframe, preset, account, run):
    values = {"robot": robot.key, "source": catalog.fingerprint(robot), "values": effective_values(robot, preset),
              "preset": preset or "", "symbol": symbol, "timeframe": timeframe,
              "login": (account or {}).get("login"), "server": (account or {}).get("server"),
              "demo": (account or {}).get("demo"), "currency": (account or {}).get("currency"),
              "balance": (account or {}).get("balance"), "portfolio": (account or {}).get("portfolio_signature"),
              "run": (run or {}).get("run_id")}
    return hashlib.sha256(json.dumps(values, sort_keys=True).encode()).hexdigest()


@dataclass
class Consent:
    digest: str
    issued_at: float
    reviewed: bool = False
    confirmed: bool = False
    negative_confirmed: bool = False
    real_authorized: bool = False

    def validate(self, digest, negative=False, real=False, now=None):
        now = time.monotonic() if now is None else now
        if digest != self.digest or not 0 <= now - self.issued_at <= 180:
            raise ValueError("A seleção/conta mudou ou a confirmação expirou. Revise o PLAY novamente.")
        if not (self.reviewed and self.confirmed) or (negative and not self.negative_confirmed):
            raise ValueError("Confirmações do PLAY incompletas.")
        if real and not self.real_authorized:
            raise ValueError("Conta REAL exige autorização específica deste lançamento.")


def play_issues(config, robot, symbol, timeframe, preset, account, db=None):
    """Demo pode ter prejuízo; isso não dispensa as proteções operacionais do Expert."""
    from . import battery, live
    issues = []
    context = selection(robot, symbol, timeframe, preset)
    native_values(robot, preset)
    if robot.kind != catalog.MQL5:
        return ["PLAY é exclusivo de Experts MQL5."]
    if context["reason"]:
        issues.append(context["reason"])
    if not account or not account.get("login") or type(account.get("demo")) is not bool:
        issues.append("Conecte e identifique a conta no MT5 antes de operar.")
    values = effective_values(robot, preset)
    facts = dict(context["risk"].facts, **contract.facts(robot, values))
    run_metrics = (context["run"] or {}).get("metrics", {})
    if (facts.get("mandatory_stop") is None and run_metrics.get("entries", 0) > 0
            and run_metrics.get("entries_without_stop") == 0):
        facts["mandatory_stop"] = True
    if not facts.get("mandatory_stop"):
        issues.append("Stop obrigatório não comprovado para este preset.")
    for role in ("risk_pct", "daily_loss_pct", "max_drawdown_pct"):
        value = facts.get(role)
        if not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
            issues.append(f"Proteção {role} não comprovada ou sem limite.")
    checks = [c for c in robot.checks if c.get("fingerprint") == catalog.fingerprint(robot)
              and (c.get("preset") or "") == (preset or "")]
    static = battery.static_items(robot, config, catalog.load_catalog(config), preset)
    failures = [item for item in static if item.status in (battery.FAIL, battery.NA)]
    if failures:
        issues += [f"Segurança: {item.name} · {item.detail}" for item in failures]
    if live.stop_active(config):
        issues.append("PARAR TUDO está ativo. Sua liberação exige decisão explícita.")
    if account and account.get("demo") is False:
        if not checks or not checks[0].get("allows_real") or checks[0].get("symbol") != symbol or checks[0].get("timeframe") != timeframe:
            issues.append("Conta REAL exige bateria completa aprovada.")
        maximum = config.getfloat("ROBOTS", "max_real_risk_pct", fallback=1.0)
        if facts.get("risk_pct", math.inf) > maximum:
            issues.append(f"Risco excede o teto REAL de {maximum:g}% por operação.")
        # Dias acompanhados, sem inferir que o estágio Demo comprova operação.
        minimum = max(1, config.getint("ROBOTS", "min_demo_days", fallback=7))
        from . import store
        meta = store.load_records(db).get(robot.key, {}).get("meta", {}) if db is not None else {}
        evidence = meta.get("demo_sessions", [])
        identity = context_digest(robot, symbol, timeframe, preset, {}, None)
        seconds = sum(max(0, float(item.get("observed_seconds", 0))) for item in evidence
                      if item.get("identity") == identity and item.get("verified_demo") is True)
        if seconds / 86400 < minimum:
            issues.append(f"Conta REAL exige {minimum} dias em demo comprovados na bateria operacional.")
    return issues


def activity_state(active, positions=None, profit=None, fresh=True):
    if not active:
        return "idle", "Inativo"
    if not fresh or positions is None or profit is None or not math.isfinite(profit):
        return "unknown", "Ativo · aguardando telemetria atual"
    if not positions:
        return "waiting", "Ativo · sem posição"
    if profit < 0:
        return "loss", "Em operação · perda"
    if profit > 0:
        return "profit", "Em operação · lucro"
    return "waiting", "Em operação · resultado zero"
