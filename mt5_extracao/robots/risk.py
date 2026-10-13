"""
Classificação de risco de um robô a partir dos parâmetros de entrada e do preset.

É uma leitura do que está configurado, com o motivo sempre explícito. Não mede o risco real da
estratégia (isso é papel do backtest e da bateria de segurança) e não é garantia.
"""
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional

LOW, MODERATE, HIGH, UNKNOWN = "Baixo", "Moderado", "Alto", "Não avaliado"
LEVELS = (LOW, MODERATE, HIGH, UNKNOWN)

LOW_RISK_PCT = 0.5        # até aqui, com as demais proteções, o risco é "Baixo"
MODERATE_RISK_PCT = 1.0   # acima disso é "Alto"

# Perfis nomeados (ex.: HDK_PROFILE_CONSERVATIVE): quando o robô usa um perfil, os valores de risco
# ficam dentro do código; o nome do perfil é a única pista sem um robot.json.
_PROFILES = ((re.compile(r"conserv", re.I), LOW, "Conservador"),
             (re.compile(r"moder", re.I), MODERATE, "Moderado"),
             (re.compile(r"ag+ress", re.I), HIGH, "Agressivo"))
_CUSTOM = re.compile(r"custom|personal", re.I)

_PATTERNS = {
    "profile": r"profile|perfil",
    "risk_pct": r"risk.*(pct|percent)|risco por (trade|opera)|riskper",
    "daily_loss_pct": r"daily.*loss|perda m[aá]xima di[aá]ria|maxdailyloss",
    "max_drawdown_pct": r"drawdown|maxdd",
    "stop_mult": r"stop.*(atr|mult)|sl.*atr",
    "stop_points": r"stop.*(points|pontos|pips)|stoploss|^inpsl$|^sl$",
    "block_averaging": r"block.*averag|proibir pre[cç]o m[eé]dio",
    "martingale": r"martingale|lot.*multipl|multiplicador.*lote|grid(?!.*(color|cor))",
}


@dataclass
class RiskRating:
    level: str
    reasons: List[str] = field(default_factory=list)
    facts: Dict[str, object] = field(default_factory=dict)

    @property
    def summary(self) -> str:
        return " · ".join(self.reasons)


def _number(value) -> Optional[float]:
    try:
        return float(str(value).replace(",", "."))
    except (TypeError, ValueError):
        return None


def _truthy(value) -> bool:
    return str(value).strip().lower() in ("true", "1")


def _pct(value: float) -> str:
    return f"{value:g}".replace(".", ",") + "%"


_NEGATION = re.compile(r"block|proibi|\bsem\b|\bno\b|n[aã]o", re.I)


def find_inputs(inputs) -> Dict[str, object]:
    """Associa cada papel (risco por trade, limite diário...) ao primeiro parâmetro que o descreve."""
    found = {}
    for role, pattern in _PATTERNS.items():
        rx = re.compile(pattern, re.I)
        candidates = inputs
        if role == "martingale":      # "Proibir preço médio (sem martingale/grid)" é proteção, não martingale
            candidates = [i for i in inputs if not _NEGATION.search(i.name) and not _NEGATION.search(i.comment)]
        match = next((i for i in candidates if rx.search(i.name)), None) \
            or next((i for i in candidates if rx.search(i.comment)), None)
        if match is not None:
            found[role] = match
    return found


def rate(inputs, values: Optional[Dict[str, str]] = None, enums=None, has_source: bool = True,
         manifest: Optional[dict] = None) -> RiskRating:
    """
    inputs: lista de MqlInput; values: valores do preset (sobrepõem os padrões do fonte);
    manifest: seção "risk" de um robot.json ({"risk_pct": 1.0, "daily_loss_pct": 3.0, ...}), que tem prioridade.
    """
    from .mql import enum_member
    if not has_source and not manifest:
        return RiskRating(UNKNOWN, ["sem código-fonte: não dá para verificar as proteções"])
    values, enums, manifest = values or {}, enums or {}, manifest or {}
    roles = find_inputs(inputs)

    def value(role):
        item = roles.get(role)
        return None if item is None else values.get(item.name, item.default)

    facts, reasons, worst = {}, [], LOW
    order = {LOW: 0, MODERATE: 1, HIGH: 2}

    def raise_to(level):
        nonlocal worst
        if order[level] > order[worst]:
            worst = level

    # Perfil nomeado: vale no lugar dos parâmetros numéricos, a menos que seja o "personalizado"
    profile_level = None
    if "profile" in roles and not manifest:
        item = roles["profile"]
        member = enum_member(enums, item.type, str(value("profile"))) or str(value("profile"))
        facts["profile"] = member
        if not _CUSTOM.search(member):
            for rx, level, label in _PROFILES:
                if rx.search(member):
                    profile_level = level
                    raise_to(level)
                    reasons.append(f"perfil {label}")
                    break

    risk = _number(manifest.get("risk_pct", value("risk_pct") if profile_level is None else None))
    if risk is not None:
        facts["risk_pct"] = risk
        reasons.append(f"{_pct(risk)}/trade")
        raise_to(HIGH if risk > MODERATE_RISK_PCT else MODERATE if risk > LOW_RISK_PCT else LOW)

    daily = _number(manifest.get("daily_loss_pct", value("daily_loss_pct") if profile_level is None else None))
    if daily is not None:
        facts["daily_loss_pct"] = daily
        if daily <= 0:
            reasons.append("sem limite de perda diário")
            raise_to(HIGH)
        else:
            reasons.append(f"limite diário {_pct(daily)}")

    drawdown = _number(manifest.get("max_drawdown_pct", value("max_drawdown_pct") if profile_level is None else None))
    if drawdown is not None:
        facts["max_drawdown_pct"] = drawdown

    stops = [_number(value(r)) for r in ("stop_mult", "stop_points") if r in roles]
    has_stop = manifest.get("mandatory_stop", any(s is not None and s > 0 for s in stops) if stops else None)
    if has_stop is not None:
        facts["mandatory_stop"] = bool(has_stop)
        reasons.insert(0, "stop obrigatório" if has_stop else "sem stop")
        if not has_stop:
            raise_to(HIGH)
    else:
        reasons.append("stop não identificado nos parâmetros")
        raise_to(MODERATE)

    if "martingale" in roles:
        raw = value("martingale")
        number = _number(raw)
        if _truthy(raw) or (number is not None and number > 1):
            facts["averaging"] = True
            reasons.append(f"aumenta a posição após perda ({roles['martingale'].name})")
            raise_to(HIGH)
    if "block_averaging" in roles:
        blocked = _truthy(value("block_averaging"))
        facts["averaging"] = facts.get("averaging", False) or not blocked
        reasons.append("sem preço médio" if blocked else "permite preço médio")
        if not blocked:
            raise_to(HIGH)

    if risk is None and profile_level is None:
        if worst == HIGH:      # já há um sinal claro de risco alto
            return RiskRating(HIGH, reasons, facts)
        return RiskRating(UNKNOWN, reasons + ["não encontrei o risco por operação nos parâmetros"], facts)
    return RiskRating(worst, reasons, facts)


def rate_python(risk_per_trade: float) -> RiskRating:
    """Estratégias Python: o stop é exigido por Strategy.gerar_sinais e o risco vem de [STRATEGY] risk_per_trade."""
    pct = risk_per_trade * 100
    level = HIGH if pct > MODERATE_RISK_PCT else MODERATE if pct > LOW_RISK_PCT else LOW
    return RiskRating(level, ["stop obrigatório", f"{_pct(pct)}/trade (configuração)", "só pesquisa, não opera"],
                      {"risk_pct": pct, "mandatory_stop": True})
