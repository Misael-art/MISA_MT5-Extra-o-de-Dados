"""F19-PESQ-16: faixas geométricas e orçamento nominal de estoque à vista."""
from mt5_extracao import b3_public
from mt5_extracao.analytics import instruments
from mt5_extracao.composition.schema import _decode, _object, _version


def _integer(value, field, *, minimum=1, maximum=2**53 - 1):
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"{field} precisa ser inteiro entre {minimum} e {maximum}.")
    return value


def _floor_root(value, degree, lower, upper):
    while lower < upper:
        probe = (lower + upper + 1) // 2
        if probe**degree <= value:
            lower = probe
        else:
            upper = probe - 1
    return lower


def _boundaries(low, high, count):
    ticks = [high]
    radicand = high**count
    for _ in range(count):
        radicand = radicand // high * low
        level = _floor_root(radicand, count, low, ticks[-1])
        if level == ticks[-1]:
            raise ValueError("Fronteiras coincidem no passo de preço. Reduza os intervalos ou amplie a faixa.")
        ticks.append(level)
    return ticks


def _arm(prices, cash, budget, fee, minimum, step, maximum, mark):
    entries = []
    shares = turnover = fees = 0
    for price in prices:
        lot = min(max(0, budget - fee) // price, maximum) // step * step
        lot = lot if lot >= minimum else 0
        paid = fee if lot else 0
        use = price * lot
        entries.append({"price_cents": price, "units": lot, "fee_cents": paid,
                        "budget_cents": budget, "unspent_cents": budget - use - paid})
        shares += lot
        turnover += use
        fees += paid
    available = cash - turnover - fees
    inventory = shares * mark
    return {"allocations": entries, "units": shares, "cash_cents": available, "inventory_mark_cents": inventory,
            "equity_cents": available + inventory, "nominal_turnover_cents": turnover, "fees_cents": fees}


def report(snapshot, definition, *, as_of):
    """Orça as compras nominais e marca o estoque no mesmo preço dos controles."""
    plan = _decode(definition)
    _object(plan, ("version", "lower_cents", "upper_cents", "intervals", "cash_cents", "fee_per_buy_cents",
                   "mark_cents", "installment_prices_cents"), "harmonic_cash")
    _version(plan["version"], "harmonic_cash.version")
    low = _integer(plan["lower_cents"], "lower_cents")
    high = _integer(plan["upper_cents"], "upper_cents")
    count = _integer(plan["intervals"], "intervals", maximum=512)
    if high - low < count:
        raise ValueError("A faixa precisa comportar todos os intervalos no passo de preço.")
    cash = _integer(plan["cash_cents"], "cash_cents")
    fee = _integer(plan["fee_per_buy_cents"], "fee_per_buy_cents", minimum=0)
    mark = _integer(plan["mark_cents"], "mark_cents")
    installment = plan["installment_prices_cents"]
    if type(installment) is not list or len(installment) != count:
        raise ValueError("Informe um preço de compra à vista por parcela de comparação.")
    for price in installment:
        _integer(price, "installment_prices_cents")

    cutoff = instruments.utc(as_of)
    manifest = snapshot.manifest
    symbol = manifest["symbol"]
    source = manifest["source"]
    if any(source.get(field) != value for field, value in b3_public.SOURCE.items()) or not b3_public.is_cash_symbol(symbol):
        raise ValueError("A projeção exige um ativo à vista da B3 Pública.")
    history = snapshot.instrument_history
    fact = instruments.specification(history, cutoff) if history is not None else None
    if fact is None:
        raise ValueError("Snapshot sem especificação vigente e conhecida no corte.")
    spec = fact["payload"]
    if any(spec.get(key) != value for key, value in
           {"currency_profit": "BRL", "tick_size": .01, "tick_value": .01, "contract_size": 1}.items()):
        raise ValueError("O modelo requer ação ou ETF à vista, unidade sem alavancagem e passo de um centavo.")
    volumes = [spec.get(key) for key in ("volume_min", "volume_step", "volume_max")]
    if any(type(value) not in (int, float) or value != int(value) for value in volumes):
        raise ValueError("O modelo requer mínimo, máximo e passo de volume em unidades inteiras.")
    minimum, step, maximum = (_integer(int(value), "volume") for value in volumes)
    if minimum > maximum:
        raise ValueError("O contrato do ativo traz volume máximo menor que o mínimo.")

    ticks = _boundaries(low, high, count)
    market = (fee, minimum, step, maximum, mark)
    # Reserva a mesma parcela em todos os intervalos. A sobra inteira fica em caixa.
    allocation = cash // count
    return {"version": 1, "snapshot_id": snapshot.manifest_id, "symbol": symbol, "as_of": cutoff,
            "specification_revision": fact["revision_id"], "currency": "BRL", "initial_cash_cents": cash,
            "boundaries_cents": ticks, "ideal_adjacent_ratio": (low / high)**(1 / count),
            "harmonic_buy_levels": "lower_edge_of_each_interval", "mark_cents": mark, "below_floor": mark < low,
            "basis": "nominal_all_planned_purchases", "execution_status": "unassessed",
            "arms": {"harmonic": _arm(ticks[1:], cash, allocation, *market),
                     "buy_and_hold": _arm([high], cash, cash, *market),
                     "installments": _arm(installment, cash, allocation, *market)}}
