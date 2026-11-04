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
import math
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


def _matches_symbol(symbol: str, pattern: str) -> bool:
    """Mesmo curinga do HDK_SymbolMatches: igualdade ou prefixo antes do primeiro `*`."""
    pattern = pattern.strip()
    if not pattern:
        return False
    star = pattern.find("*")
    return symbol == pattern if star < 0 else symbol.startswith(pattern[:star])


def _exposure_groups(symbol: str, info, fixed_groups: str) -> list[str]:
    groups = []
    for index, spec in enumerate((fixed_groups or "").split(";")):
        patterns = [pattern for pattern in spec.split(",") if pattern.strip()]
        if any(_matches_symbol(symbol, pattern) for pattern in patterns):
            groups.append(f"Grupo {index + 1}")
    if groups:
        return groups
    base = str(getattr(info, "currency_base", "") or "")
    profit = str(getattr(info, "currency_profit", "") or "")
    if base:
        groups.append(base)
    if profit and profit not in groups:
        groups.append(profit)
    return groups or [f"Ativo {symbol}"]


def _number(value):
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) and abs(number) != float.fromhex("0x1.fffffffffffffp+1023") else None


def _checked_snapshot(value):
    """Nenhum total/campo exportado pode converter overflow em carteira normal."""
    if isinstance(value, float) and _number(value) is None:
        raise RuntimeError("Total ou exposição não finitos; carteira sem confirmação.")
    if isinstance(value, dict):
        for item in value.values():
            _checked_snapshot(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _checked_snapshot(item)
    return value


def _risk(mt5, symbol: str, buy: bool, volume: float, price: float, stop: float, *, pending=False, specs=None) -> Optional[float]:
    """Estimativa nativa em moeda da conta. Falha/ausência de dados permanece None."""
    volume, price, stop = (_number(v) for v in (volume, price, stop))
    if (not symbol or specs is None or any(v is None or v <= 0 for v in (volume, price, stop)) or
            any(_number(getattr(specs, field, None)) is None or float(getattr(specs, field)) <= 0
                for field in ("point", "trade_tick_size"))):
        return None
    if pending and (price - stop) * (1 if buy else -1) <= 0:
        return None
    try:
        result = _number(mt5.order_calc_profit(0 if buy else 1, symbol, volume, price, stop))
    except (AttributeError, RuntimeError, TypeError, ValueError, OSError):
        return None
    return max(0.0, -result) if result is not None else None


def _account_identity(account):
    if account is None:
        raise RuntimeError("Conta indisponível durante leitura da carteira; reconecte e atualize o retrato.")
    login = getattr(account, "login", None)
    if not isinstance(login, int) or isinstance(login, bool):
        raise RuntimeError("Identificador da conta indisponível; atualize a conexão.")
    mode, trade_mode = getattr(account, "margin_mode", None), getattr(account, "trade_mode", None)
    if isinstance(mode, bool) or isinstance(trade_mode, bool) or trade_mode not in (0, 1, 2):
        raise RuntimeError("Modalidade da conta indisponível; não inferir DEMO ou posse.")
    identity = (login, str(getattr(account, "server", "") or ""),
                str(getattr(account, "currency", "") or ""), mode, trade_mode)
    if identity[0] <= 0 or not identity[1] or not identity[2] or identity[3] not in (0, 1, HEDGING):
        raise RuntimeError("Identidade/servidor/modalidade da carteira indisponível; atualize a conexão.")
    for field in ("balance", "equity", "margin", "margin_free"):
        number = _number(getattr(account, field, None))
        if number is None or number < 0 or (field in ("balance", "equity") and number == 0):
            raise RuntimeError("Capital ou margem da conta indisponível; carteira sem confirmação.")
    return identity


def _rows(mt5, method):
    rows = getattr(mt5, method)()
    if rows is None:
        raise RuntimeError("Exposição indisponível durante leitura da carteira; não tratar como conta vazia.")
    return tuple(rows)


def _row_key(row, *, pending=False):
    fields = ("ticket", "magic", "symbol", "type", "volume_current" if pending else "volume", "price_open", "sl")
    if pending and getattr(row, "type", None) in (6, 7):
        fields += ("price_stoplimit",)
    values = tuple(getattr(row, name, None) for name in fields)
    if (not isinstance(values[0], int) or values[0] <= 0 or not isinstance(values[1], int) or values[1] < 0 or
            not values[2] or values[3] not in (range(8) if pending else (0, 1)) or
            any(_number(v) is None for v in values[4:]) or values[4] <= 0 or values[5] <= 0 or values[6] < 0):
        raise RuntimeError("Identidade, volume, preço ou stop inválidos no retrato; atualize a carteira.")
    return values


def snapshot(mt5, robots: Iterable = (), max_risk_pct: float = 0.0, exposure_groups: str = "") -> dict:
    """
    Retrato da conta conectada. Levanta RuntimeError, com o que fazer, se não há conta.
    `max_risk_pct` (% do saldo) só muda o campo "state": nada é bloqueado aqui.
    """
    max_risk_pct = _number(max_risk_pct)
    if max_risk_pct is None or not 0 <= max_risk_pct <= 100:
        raise ValueError("Limite da carteira deve ser percentual finito de 0 a 100% do saldo.")
    account = mt5.account_info()
    if account is None:
        raise RuntimeError("Nenhuma conta conectada no MetaTrader 5. Abra o MT5, entre na conta e rode: mt5x doctor")
    identity = _account_identity(account)
    native_positions, native_orders = _rows(mt5, "positions_get"), _rows(mt5, "orders_get")
    position_keys = sorted(_row_key(p) for p in native_positions)
    order_keys = sorted(_row_key(o, pending=True) for o in native_orders)
    names = owners(robots)
    infos = {}

    def info(symbol):
        if symbol not in infos:
            infos[symbol] = mt5.symbol_info(symbol)
        return infos[symbol]

    positions = []
    for p in native_positions:
        buy = int(p.type) == 0
        specs = info(p.symbol)
        risk = _risk(mt5, p.symbol, buy, p.volume, float(p.price_open), float(p.sl), specs=specs)
        positions.append({
            "ticket": int(p.ticket), "symbol": p.symbol, "magic": int(p.magic), "owner": owner_name(p.magic, names),
            "ownership_basis": "magic_label_only" if identity[3] == HEDGING else "netting_contribution_unresolved",
            "direction": 1 if buy else -1, "volume": float(p.volume), "price_open": float(p.price_open),
            "sl": float(p.sl), "tp": float(p.tp), "has_stop": bool(p.sl), "risk_money": risk,
            "base_currency": str(getattr(specs, "currency_base", "") or ""),
            "profit_currency": str(getattr(specs, "currency_profit", "") or ""),
            "profit": float(p.profit) + float(getattr(p, "swap", 0.0) or 0.0)})
    orders = []
    for o in native_orders:
        buy = int(o.type) in BUY_ORDER_TYPES
        specs = info(o.symbol)
        entry = float(o.price_stoplimit if int(o.type) in (6, 7) else o.price_open)
        risk = _risk(mt5, o.symbol, buy, o.volume_current, entry, float(o.sl), pending=True, specs=specs)
        orders.append({
            "ticket": int(o.ticket), "symbol": o.symbol, "magic": int(o.magic), "owner": owner_name(o.magic, names),
            "type": int(o.type), "direction": 1 if buy else -1, "volume": float(o.volume_current),
            "price": entry, "sl": float(o.sl), "has_stop": bool(o.sl), "risk_money": risk,
            "base_currency": str(getattr(specs, "currency_base", "") or ""),
            "profit_currency": str(getattr(specs, "currency_profit", "") or "")})

    if any(_number(p["profit"]) is None for p in positions):
        raise RuntimeError("Resultado de posição não finito; carteira sem confirmação.")
    refreshed = mt5.account_info()
    if (_account_identity(refreshed) != identity or
            float(refreshed.balance) != float(account.balance) or
            sorted(_row_key(p) for p in _rows(mt5, "positions_get")) != position_keys or
            sorted(_row_key(o, pending=True) for o in _rows(mt5, "orders_get")) != order_keys):
        raise RuntimeError("Conta ou exposição mudou durante a leitura; atualize antes de usar o retrato.")
    balance = float(account.balance)
    unknown = [i for i in positions + orders if i["risk_money"] is None]
    risk_open = sum(p["risk_money"] or 0.0 for p in positions)
    risk_pending = sum(o["risk_money"] or 0.0 for o in orders)
    totals = {
        "balance": balance, "equity": float(getattr(account, "equity", 0.0) or 0.0),
        "margin": float(getattr(account, "margin", 0.0) or 0.0),
        "margin_free": float(getattr(account, "margin_free", 0.0) or 0.0),
        "floating": sum(p["profit"] for p in positions), "risk_open": risk_open, "risk_pending": risk_pending,
        "risk_pct": (risk_open + risk_pending) / balance * 100.0 if balance > 0 else 0.0,
        "risk_complete": not unknown, "snapshot_complete": True, "capital_base": "balance", "positions": len(positions), "orders": len(orders),
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

    account_currency = str(getattr(account, "currency", "") or "")
    by_symbol = {}
    by_group = {}
    for item, kind in [(p, "positions") for p in positions] + [(o, "orders") for o in orders]:
        symbol = item["symbol"]
        exposure = by_symbol.setdefault(symbol, {
            "symbol": symbol, "base_currency": item["base_currency"], "profit_currency": item["profit_currency"],
            "currency": account_currency, "buy_positions": 0, "sell_positions": 0,
            "buy_orders": 0, "sell_orders": 0, "buy_volume": 0.0, "sell_volume": 0.0,
            "buy_risk_money": 0.0, "sell_risk_money": 0.0})
        side = "buy" if item["direction"] > 0 else "sell"
        exposure[f"{side}_{kind}"] += 1
        exposure[f"{side}_volume"] += item["volume"]
        exposure[f"{side}_risk_money"] += item["risk_money"] or 0.0
        for group_name in _exposure_groups(symbol, info(symbol), exposure_groups):
            group = by_group.setdefault(group_name, {
                "group": group_name, "currency": account_currency, "buy_positions": 0, "sell_positions": 0,
                "buy_orders": 0, "sell_orders": 0, "buy_risk_money": 0.0, "sell_risk_money": 0.0})
            group[f"{side}_{kind}"] += 1
            group[f"{side}_risk_money"] += item["risk_money"] or 0.0

    reason = ""
    if unknown:
        reason = (f"{len(unknown)} posição(ões) ou ordem(ns) sem stop ou sem dados do ativo: o risco da conta não pode "
                  "ser avaliado por inteiro")
    elif max_risk_pct > 0 and totals["risk_pct"] > max_risk_pct:
        reason = f"risco até o stop de {totals['risk_pct']:.2f}% do saldo (limite {max_risk_pct:.2f}%)"
    return _checked_snapshot({"login": int(getattr(account, "login", 0) or 0), "server": getattr(account, "server", "") or "",
            "currency": account_currency, "risk_source": "native_order_calc_profit", "demo": getattr(account, "trade_mode", None) == 0,
            "hedging": hedging, "state": ALERT if reason else NORMAL, "reason": reason, "totals": totals,
            "positions": positions, "orders": orders,
            "by_owner": sorted(by_owner.values(), key=lambda g: -g["risk_money"]), "shared_symbols": shared,
            "exposures": [by_symbol[key] for key in sorted(by_symbol)],
            "groups": [by_group[key] for key in sorted(by_group)]})


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
