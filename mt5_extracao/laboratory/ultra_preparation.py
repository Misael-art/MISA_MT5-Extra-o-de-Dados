"""Materializa o contrato sintético usado pelo Tester antes da amostra congelada."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import re
from string import Template

from ..experiments.schema import digest

PRIMER_MINUTES = 128


def render(template, package, symbol, frames):
    first, last = (frames[i]["mark"]["key"][0] for i in (0, -1))
    day = datetime.fromtimestamp(first / 1000, timezone.utc).replace(hour=9, minute=0, second=0, microsecond=0)
    prior = day - timedelta(days=1)
    while prior.weekday() >= 5:
        prior -= timedelta(days=1)
    start = int(prior.timestamp()) * 1000
    if start <= 0:
        raise ValueError("O intervalo não comporta o histórico prévio em M1.")
    spec = package["specification"]
    policy = package["policy"]
    currency = policy["currency"]
    if not re.fullmatch(r"[A-Z]{3}", currency) or spec["currency_profit"] != currency:
        raise ValueError("A bancada do Tester requer a mesma moeda no contrato e na conta.")
    values = {field: Decimal(str(spec[field])) for field in ("tick_size", "tick_value", "point", "volume_step",
                                                            "volume_min", "volume_max", "contract_size")}
    if any(not v.is_finite() or v <= 0 for v in values.values()):
        raise ValueError("O contrato do Tester exige preço e volumes positivos e conhecidos.")
    volume = values["volume_step"]
    key = package["package_hash"]
    target = "UT_" + digest([key, template, start, PRIMER_MINUTES])[:20]
    text = Template(template).substitute(
        package=key, source=symbol, target=target, currency=currency, primer=PRIMER_MINUTES, count=len(frames),
        first=first, last=last, prior=start, tick_size=values["tick_size"], tick_value=values["tick_value"],
        point=values["point"], digits=spec["digits"], step=volume, min_lot=values["volume_min"],
        max_lot=values["volume_max"], contract_size=values["contract_size"], margin=Decimal(policy["margin"])/100/volume)
    metadata = {"symbol": target, "primer_kind": "synthetic_constant_bid_100_ask_101_ticks",
                "primer_bars": PRIMER_MINUTES, "primer_first_msc": start, "sample_first_msc": first,
                "sample_last_msc": last, "tick_cash_cents_per_step": policy["tick_cash"], "margin_cents_per_step": policy["margin"]}
    return text, metadata
