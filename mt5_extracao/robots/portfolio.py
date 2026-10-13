"""
Carteira da conta (T12.2): todas as posições e ordens pendentes, de qualquer robô e as manuais, com o dono de cada
uma, o risco até o stop e os totais. SOMENTE LEITURA: este módulo nunca envia, altera nem cancela ordem.

    carteira = portfolio.snapshot(mt5, robots)        # `mt5` é o backend (ponte ou módulo); `robots`, o catálogo
    carteira["totals"]["risk_pct"], carteira["state"]

O mesmo retrato é calculado dentro do MT5 pelo HDK_AccountGuardian (HDK_PortfolioRegistry.mqh), que o grava em
Common\\Files\\HDK_portfolio_<conta>.json; `read_guardian` lê esse arquivo quando a ponte não está disponível.
"""
import json
import logging
from typing import Iterable, Optional

from . import battery, catalog

log = logging.getLogger(__name__)

HEDGING = 2                       # ACCOUNT_MARGIN_MODE_RETAIL_HEDGING
BUY_ORDER_TYPES = (0, 2, 4, 6)    # ORDER_TYPE_BUY, BUY_LIMIT, BUY_STOP, BUY_STOP_LIMIT
MANUAL = "manual ou desconhecido"
NORMAL, ALERT = "NORMAL", "ALERTA"
GUARDIAN_FILE = "HDK_portfolio_{login}.json"


def owners(robots: Iterable = ()) -> dict:
    """{magic number: nome do robô} dos robôs MQL5 do catálogo que têm código-fonte."""
    names = {}
    for robot in robots:
        if robot.kind != catalog.MQL5 or not robot.has_source:
            continue
        magic = battery.magic_number(robot)
        if magic.lstrip("-").isdigit() and int(magic) != 0:
            names.setdefault(int(magic), robot.name)
    return names


def owner_name(magic: int, names: dict) -> str:
    if not magic:
        return MANUAL
    return names.get(int(magic), f"robô não catalogado #{int(magic)}")


def _risk(info, buy: bool, volume: float, price: float, stop: float) -> Optional[float]:
    """Perda de ir do preço ao stop, na moeda da conta. None = sem stop ou sem dados do ativo; 0 = stop no lucro."""
    if not stop or info is None:
        return None
    tick_size = float(getattr(info, "trade_tick_size", 0) or getattr(info, "point", 0) or 0)
    tick_value = float(getattr(info, "trade_tick_value", 0) or 0)
    if tick_size <= 0 or tick_value <= 0:
        return None
    distance = (price - stop) if buy else (stop - price)
    return max(0.0, distance / tick_size * tick_value * float(volume))


def snapshot(mt5, robots: Iterable = (), max_risk_pct: float = 0.0) -> dict:
    """
    Retrato da conta conectada. Levanta RuntimeError, com o que fazer, se não há conta.
    `max_risk_pct` (% do saldo) só muda o campo "state": nada é bloqueado aqui.
    """
    account = mt5.account_info()
    if account is None:
        raise RuntimeError("Nenhuma conta conectada no MetaTrader 5. Abra o MT5, entre na conta e rode: mt5x doctor")
    names = owners(robots)
    infos = {}

    def info(symbol):
        if symbol not in infos:
            infos[symbol] = mt5.symbol_info(symbol)
        return infos[symbol]

    positions = []
    for p in mt5.positions_get() or ():
        buy = int(p.type) == 0
        risk = _risk(info(p.symbol), buy, p.volume, float(p.price_open), float(p.sl))
        positions.append({
            "ticket": int(p.ticket), "symbol": p.symbol, "magic": int(p.magic), "owner": owner_name(p.magic, names),
            "direction": 1 if buy else -1, "volume": float(p.volume), "price_open": float(p.price_open),
            "sl": float(p.sl), "tp": float(p.tp), "has_stop": bool(p.sl), "risk_money": risk,
            "profit": float(p.profit) + float(getattr(p, "swap", 0.0) or 0.0)})
    orders = []
    for o in mt5.orders_get() or ():
        buy = int(o.type) in BUY_ORDER_TYPES
        risk = _risk(info(o.symbol), buy, o.volume_current, float(o.price_open), float(o.sl))
        orders.append({
            "ticket": int(o.ticket), "symbol": o.symbol, "magic": int(o.magic), "owner": owner_name(o.magic, names),
            "type": int(o.type), "direction": 1 if buy else -1, "volume": float(o.volume_current),
            "price": float(o.price_open), "sl": float(o.sl), "has_stop": bool(o.sl), "risk_money": risk})

    balance = float(getattr(account, "balance", 0.0) or 0.0)
    unknown = [i for i in positions + orders if i["risk_money"] is None]
    risk_open = sum(p["risk_money"] or 0.0 for p in positions)
    risk_pending = sum(o["risk_money"] or 0.0 for o in orders)
    totals = {
        "balance": balance, "equity": float(getattr(account, "equity", 0.0) or 0.0),
        "margin": float(getattr(account, "margin", 0.0) or 0.0),
        "margin_free": float(getattr(account, "margin_free", 0.0) or 0.0),
        "floating": sum(p["profit"] for p in positions), "risk_open": risk_open, "risk_pending": risk_pending,
        "risk_pct": (risk_open + risk_pending) / balance * 100.0 if balance > 0 else 0.0,
        "risk_complete": not unknown, "positions": len(positions), "orders": len(orders),
        "without_stop": sum(1 for i in positions + orders if not i["has_stop"]),
        "manual": sum(1 for p in positions if not p["magic"])}

    by_owner = {}
    for item, kind in [(p, "positions") for p in positions] + [(o, "orders") for o in orders]:
        group = by_owner.setdefault(item["owner"], {"owner": item["owner"], "magic": item["magic"], "positions": 0,
                                                    "orders": 0, "volume": 0.0, "profit": 0.0, "risk_money": 0.0,
                                                    "symbols": []})
        group[kind] += 1
        group["risk_money"] += item["risk_money"] or 0.0
        if kind == "positions":
            group["volume"] += item["volume"]
            group["profit"] += item["profit"]
        if item["symbol"] not in group["symbols"]:
            group["symbols"].append(item["symbol"])

    hedging = int(getattr(account, "margin_mode", HEDGING) or 0) == HEDGING
    shared = {}
    for item in positions + orders:
        shared.setdefault(item["symbol"], set()).add(item["owner"])
    shared = {symbol: sorted(who) for symbol, who in shared.items() if len(who) > 1}

    reason = ""
    if unknown:
        reason = (f"{len(unknown)} posição(ões) ou ordem(ns) sem stop ou sem dados do ativo: o risco da conta não pode "
                  "ser avaliado por inteiro")
    elif max_risk_pct > 0 and totals["risk_pct"] > max_risk_pct:
        reason = f"risco até o stop de {totals['risk_pct']:.2f}% do saldo (limite {max_risk_pct:.2f}%)"
    return {"login": int(getattr(account, "login", 0) or 0), "server": getattr(account, "server", "") or "",
            "currency": getattr(account, "currency", "") or "", "demo": getattr(account, "trade_mode", None) == 0,
            "hedging": hedging, "state": ALERT if reason else NORMAL, "reason": reason, "totals": totals,
            "positions": positions, "orders": orders,
            "by_owner": sorted(by_owner.values(), key=lambda g: -g["risk_money"]), "shared_symbols": shared}


def read_guardian(config, login=None) -> Optional[dict]:
    """
    Retrato gravado pelo HDK_AccountGuardian em Common\\Files (None se não há arquivo). Com `login`, lê o daquela
    conta; sem ele, o mais recente. Serve quando a ponte não está no ar; o horário do retrato vem em "updated".
    """
    from . import mt5_tools
    try:
        folder = mt5_tools.kill_file(config).parent
    except (OSError, ValueError):
        return None
    if not folder.is_dir():
        return None
    files = [folder / GUARDIAN_FILE.format(login=login)] if login else sorted(
        folder.glob(GUARDIAN_FILE.format(login="*")), key=lambda f: f.stat().st_mtime, reverse=True)
    for path in files:
        try:
            return json.loads(path.read_text(encoding="utf-8-sig"))
        except (OSError, ValueError) as e:
            log.warning(f"Retrato do guardião ilegível em {path}: {e}")
    return None
