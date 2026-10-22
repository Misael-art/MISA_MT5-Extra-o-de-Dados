"""Perfil manual reconhecido por identidade de fontes, sem inferir pelo número de trades."""
import math

from . import catalog, mql

ROBOT_KEY = "mql5:hdk_hadouken_painel"
# Árvores revisadas do painel manual: G18 e G19.2. Não conceder a exceção só pelo nome.
APPROVED_SOURCE_FINGERPRINTS = frozenset({"0958d874e9f55cb4", "20e8d6b940a652a4"})
PROFILE_LIMITS = {
    "HDK_PROFILE_CONSERVATIVE": (0.5, 2.0, 10.0),
    "HDK_PROFILE_MODERATE": (1.0, 3.0, 15.0),
    "HDK_PROFILE_AGGRESSIVE": (1.5, 4.0, 20.0),
}


def recognized(robot):
    """Só o painel manual revisado, com fonte; Expert automático continua exigindo backtest."""
    return (robot.kind == catalog.MQL5 and robot.key == ROBOT_KEY and robot.has_source
            and catalog.fingerprint(robot) in APPROVED_SOURCE_FINGERPRINTS)


def facts(robot, values):
    """Limites efetivos do BuildConfig do fonte identificado; não altera os inputs."""
    if not recognized(robot):
        return {}
    inputs = {item.name: item for item in robot.inputs}

    def value(name):
        return values.get(name, inputs[name].default if name in inputs else None)

    def member(name):
        item = inputs.get(name)
        raw = value(name)
        return (mql.enum_member(robot.enums, item.type, str(raw)) or str(raw)) if item else ""

    def number(name):
        try:
            parsed = float(str(value(name)).split("|")[0])
            return parsed if math.isfinite(parsed) else None
        except (TypeError, ValueError):
            return None

    profile = member("InpProfile")
    limits = PROFILE_LIMITS.get(profile)
    if limits is None and profile == "HDK_PROFILE_CUSTOM":
        limits = tuple(number(name) for name in ("InpRiskPct", "InpDailyLossPct", "InpMaxDrawdownPct"))
    result = dict(zip(("risk_pct", "daily_loss_pct", "max_drawdown_pct"), limits)) if limits else {}
    stop_mode = member("InpStopMode")
    stop = number("InpStopAtrMult" if stop_mode == "HDK_STOP_ATR" else "InpStopPoints")
    result["mandatory_stop"] = stop_mode in ("HDK_STOP_ATR", "HDK_STOP_POINTS") and stop is not None and stop > 0
    return result
