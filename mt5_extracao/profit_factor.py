"""Contrato do PF: razão finita sem teto; ausência de perdas não é aprovação."""
import math
from typing import Optional

VERSION = 1


def finite(value) -> Optional[float]:
    """Número finito não negativo; None/NaN/infinito não viram zero."""
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) and number >= 0 else None


def fields(pnl) -> dict:
    """Metadados auditáveis na mesma unidade dos PnLs recebidos, sem inferir custos."""
    values = list(pnl)
    result = {"profit_factor": None, "profit_factor_version": VERSION,
              "profit_factor_status": "invalid_pnl", "gross_profit": None,
              "gross_loss": None, "losses": None}
    try:
        if any(isinstance(x, bool) or x is None for x in values):
            return result
        values = [float(x) for x in values]
        if not all(math.isfinite(x) for x in values):
            return result
        gains = math.fsum(x for x in values if x > 0)
        loss = -math.fsum(x for x in values if x < 0)
        ratio = gains / loss if loss > 0 else None
        if ratio is not None and not math.isfinite(ratio):
            return result
    except (TypeError, ValueError, OverflowError):
        return result
    result.update(profit_factor=ratio, gross_profit=gains, gross_loss=loss,
                  losses=sum(x < 0 for x in values),
                  profit_factor_status="defined" if loss > 0 else "no_losses" if values else "no_trades")
    return result


def passes(value, threshold: float) -> bool:
    number = finite(value)
    return number is not None and number > threshold


def sort_key(value):
    """Ordem decrescente com ausentes ao fim, inclusive NaN vindo de DataFrame."""
    number = finite(value)
    return number is None, -number if number is not None else 0.0


def text(value, digits: int = 2) -> str:
    number = finite(value)
    return f"{number:.{digits}f}".replace(".", ",") if number is not None else "indefinido"


def reason(metrics: dict, compact: bool = False) -> str:
    if compact:
        return {"no_losses": "Sem perdas", "no_trades": "Sem operações",
                "invalid_pnl": "PnL inválido/ausente", "legacy_unverified": "PF legado não verificado",
                "legacy_reported": "PF legado relatado"}.get(metrics.get("profit_factor_status"), "")
    return {"no_losses": "Sem perdas no denominador", "no_trades": "Sem operações",
            "invalid_pnl": "Resultado ausente ou inválido",
            "legacy_unverified": "PF legado sem denominador verificável",
            "legacy_reported": "PF legado relatado; denominador não auditado"}.get(
                metrics.get("profit_factor_status"), "")


def read_fields(metrics: dict, pnl=None, expected_count=None) -> dict:
    """Projeção de leitura: nunca altera o registro nem inventa o livro antigo."""
    original = metrics.get("profit_factor")
    if pnl is not None:
        values = list(pnl)
        if expected_count is not None and len(values) == expected_count:
            return {**fields(values), "profit_factor_recorded": original if finite(original) is not None else None,
                    "profit_factor_origin": "ledger_recomputed"}
    if metrics.get("profit_factor_version") == VERSION:
        gains, loss = finite(metrics.get("gross_profit")), finite(metrics.get("gross_loss"))
        value = gains / loss if gains is not None and loss is not None and loss > 0 else None
        return {**{k: metrics.get(k) for k in fields([])},
                "profit_factor": finite(value),
                "profit_factor_status": "defined" if finite(value) is not None else
                    metrics.get("profit_factor_status") if metrics.get("profit_factor_status") in
                    ("no_trades", "no_losses", "invalid_pnl") else "invalid_pnl"}
    number = finite(original)
    unverified = number is None or number in (99.0, 999.0)
    return {"profit_factor": None if unverified else number,
            "profit_factor_recorded": original if finite(original) is not None else None,
            "profit_factor_version": 0,
            "profit_factor_status": "legacy_unverified" if unverified else "legacy_reported",
            "gross_profit": None, "gross_loss": None, "losses": None,
            "profit_factor_origin": "legacy_reported"}
