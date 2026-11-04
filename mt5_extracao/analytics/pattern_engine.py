"""Avaliação causal T13.4-P02: regras em barras fechadas, desfechos separados."""

from bisect import bisect_left, bisect_right, insort
from decimal import Decimal, InvalidOperation

import pandas as pd

from mt5_extracao.analytics import hypotheses


STATE_VERSION = 1
MAX_STATE_BARS = 20_000
REQUIRED_OBSERVATION = {"source_namespace", "symbol", "timeframe", "bar_start_utc",
                        "bar_end_utc", "available_at_utc", "revision", "session_id",
                        "open", "high", "low", "close"}
OPTIONAL_OBSERVATION = {"reference_price", "reference_available_at_utc", "session_minute",
                        "source_revision"}


def _instant(value, field):
    stamp = pd.Timestamp(value)
    if pd.isna(stamp) or stamp.tzinfo is None:
        raise ValueError(f"{field} exige instante com fuso explícito.")
    return stamp.tz_convert("UTC")


def _decimal(value):
    if value is None:
        return None
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("Preço ou referência inválidos.") from exc
    if not result.is_finite():
        raise ValueError("Preço ou referência precisam ser finitos.")
    return result


def _observation(raw):
    if not isinstance(raw, dict) or not REQUIRED_OBSERVATION <= set(raw) or set(raw) - REQUIRED_OBSERVATION - OPTIONAL_OBSERVATION:
        raise ValueError("Observação sem identidade, relógio ou campos permitidos.")
    row = dict(raw)
    for field in ("source_namespace", "symbol", "timeframe"):
        if not isinstance(row[field], str) or not row[field].strip():
            raise ValueError(f"{field} obrigatório na observação.")
    if type(row["revision"]) is not int or row["revision"] < 1:
        raise ValueError("Revisão da observação precisa ser inteira positiva.")
    if row["session_id"] is not None and (not isinstance(row["session_id"], str) or not row["session_id"]):
        raise ValueError("session_id precisa ser identificador ou null.")
    start, end, available = (_instant(row[key], key) for key in
                             ("bar_start_utc", "bar_end_utc", "available_at_utc"))
    if not start < end <= available:
        raise ValueError("Barra precisa fechar antes da disponibilidade; dados futuros recusados.")
    for key, value in (("bar_start_utc", start), ("bar_end_utc", end), ("available_at_utc", available)):
        row[key] = value.isoformat()
    if row.get("reference_price") is not None:
        if "reference_available_at_utc" not in row:
            raise ValueError("Referência externa exige disponibilidade própria.")
        reference_available = _instant(row["reference_available_at_utc"], "reference_available_at_utc")
        if reference_available > available:
            raise ValueError("Referência futura não pode participar da decisão.")
        row["reference_available_at_utc"] = reference_available.isoformat()
    for field in ("open", "high", "low", "close", "reference_price"):
        if field in row:
            value = _decimal(row[field])
            row[field] = format(value, "f") if value is not None else None
    if "source_revision" in row and row["source_revision"] is not None:
        revision = row["source_revision"]
        if (not isinstance(revision, (str, int)) or isinstance(revision, bool) or
                not str(revision) or len(str(revision)) > 128):
            raise ValueError("Revisão da fonte precisa ser identificador curto.")
        row["source_revision"] = str(revision)
    if all(row[key] is not None for key in ("open", "high", "low", "close")):
        low, high = _decimal(row["low"]), _decimal(row["high"])
        if low > high or any(not low <= _decimal(row[key]) <= high for key in ("open", "close")):
            raise ValueError("OHLC contraditório; não avalie a barra.")
    if row.get("session_minute") is not None and (type(row["session_minute"]) is not int or row["session_minute"] < 0):
        raise ValueError("Minuto de sessão inválido.")
    return row


def initial_state(definition, source_namespace, symbol):
    if not isinstance(definition, hypotheses.Definition):
        raise TypeError("Compile a hipótese antes de avaliar.")
    if not isinstance(source_namespace, str) or not source_namespace:
        raise ValueError("Namespace de fonte obrigatório.")
    if not isinstance(symbol, str) or not symbol:
        raise ValueError("Símbolo da fonte obrigatório.")
    if symbol not in definition.body["universe_definition"]["symbols"]:
        raise ValueError("Símbolo fora do universo congelado da hipótese.")
    return {"state_version": STATE_VERSION, "definition_hash": definition.definition_hash,
            "source_namespace": source_namespace, "symbol": symbol, "last_available_at_utc": None,
            "known": {}, "starts": [], "emitted_sessions": {}}


def _prior(state, bar_start):
    index = bisect_left(state["starts"], bar_start)
    return state["known"][state["starts"][index - 1]] if index else None


def _value(node, row, prior, state):
    op = node["op"]
    if op == "field":
        value = prior.get("close") if node["name"] == "prior_close" and prior is not None else row.get(node["name"])
        return _decimal(value)
    if op == "literal":
        return Decimal(node["value"])
    if op == "percent_change":
        value = _value(node["value"], row, prior, state)
        reference = _value(node["reference"], row, prior, state)
        return None if value is None or reference is None or reference <= 0 else value / reference - 1
    if op == "compare":
        left, right = (_value(node[key], row, prior, state) for key in ("left", "right"))
        if left is None or right is None:
            return None
        return {"gt": left > right, "gte": left >= right, "lt": left < right, "lte": left <= right}[node["comparison"]]
    if op == "crosses_above":
        if prior is None:
            return None
        before = _prior(state, prior["bar_start_utc"])
        previous_left = _value(node["left"], prior, before, state)
        previous_right = _value(node["right"], prior, before, state)
        current_left = _value(node["left"], row, prior, state)
        current_right = _value(node["right"], row, prior, state)
        if None in (previous_left, previous_right, current_left, current_right):
            return None
        return previous_left <= previous_right and current_left > current_right
    if op == "not":
        value = _value(node["term"], row, prior, state)
        return None if value is None else not value
    if op in ("and", "or"):
        values = [_value(term, row, prior, state) for term in node["terms"]]
        if op == "and":
            return False if False in values else None if None in values else True
        return True if True in values else None if None in values else False
    raise ValueError("Expressão não compilada.")


def evaluate_prefix(definition, state, raw):
    """Atualiza estado JSON após validar a chegada; devolve evento opcional."""
    if not isinstance(definition, hypotheses.Definition):
        raise TypeError("Compile a hipótese antes de avaliar.")
    if (state.get("state_version") != STATE_VERSION or state.get("definition_hash") != definition.definition_hash):
        raise ValueError("Estado incremental incompatível com versão da hipótese.")
    body = definition.body
    row = _observation(raw)
    if (row["source_namespace"] != state["source_namespace"] or row["symbol"] != state["symbol"] or
            row["timeframe"] != body["timeframe"]):
        raise ValueError("Fonte, símbolo ou timeframe divergente; não misture séries no estado.")
    last = state["last_available_at_utc"]
    if last is not None and row["available_at_utc"] < last:
        raise ValueError("Chegada fora de ordem; reexecute o prefixo em ordem de disponibilidade.")
    known = state["known"]
    existing = known.get(row["bar_start_utc"])
    if existing is not None and row["revision"] <= existing["revision"]:
        raise ValueError("Revisão repetida ou regressiva no mesmo candle.")
    if existing is not None and row["available_at_utc"] <= existing["available_at_utc"]:
        raise ValueError("Revisão exige disponibilidade posterior à anterior.")
    prior = _prior(state, row["bar_start_utc"])
    index = bisect_left(state["starts"], row["bar_start_utc"])
    following = (known[state["starts"][index + (existing is not None)]]
                 if index + (existing is not None) < len(state["starts"]) else None)
    if (prior is not None and prior["bar_end_utc"] > row["bar_start_utc"]) or (following is not None and row["bar_end_utc"] > following["bar_start_utc"]):
        raise ValueError("Barras sobrepostas; recorte temporal incompatível.")
    context = (True if body["context_expression"] is None else
               _value(body["context_expression"], row, prior, state))
    condition = _value(body["condition_expression"], row, prior, state)
    reference = _value({"op": "field", "name": body["price_reference"]}, row, prior, state)
    selected = (context is True and condition is True and
                reference is not None and all(row[key] is not None for key in ("open", "high", "low", "close")))
    if body["repetition_policy"] == "first_per_session":
        if row["session_id"] is None:
            raise ValueError("first_per_session exige sessão comprovada.")
        selected = selected and row["session_id"] not in state["emitted_sessions"]
    event = None
    if selected:
        event = {"event_id": hypotheses.digest({"definition_hash": definition.definition_hash,
                  "source_namespace": row["source_namespace"], "symbol": row["symbol"],
                  "bar_start_utc": row["bar_start_utc"], "revision": row["revision"],
                  "available_at_utc": row["available_at_utc"]}),
                 "pattern_id": body["pattern_id"], "definition_hash": definition.definition_hash,
                 "source_namespace": row["source_namespace"], "symbol": row["symbol"],
                 "timeframe": row["timeframe"], "session_id": row["session_id"],
                 "bar_start_utc": row["bar_start_utc"], "bar_end_utc": row["bar_end_utc"],
                 "available_at_utc": row["available_at_utc"], "decision_at_utc": row["available_at_utc"],
                 "revision": row["revision"], "source_revision": row.get("source_revision"),
                 "side": body["side"], "entry_policy": body["simulated_entry"],
                 "signal_values": {"close": row["close"], "prior_close": prior.get("close") if prior else None,
                                   "reference": str(reference) if reference is not None else None,
                                   "reference_available_at_utc": row.get("reference_available_at_utc"),
                                   "condition": condition, "context": context},
                 "outcome": {"status": "pending"}}
    if existing is None and len(known) >= MAX_STATE_BARS:
        raise ValueError("Estado incremental excede limite de barras; divida o recorte.")
    if existing is None:
        insort(state["starts"], row["bar_start_utc"])
    known[row["bar_start_utc"]] = row
    if event is not None and body["repetition_policy"] == "first_per_session":
        state["emitted_sessions"][row["session_id"]] = event["event_id"]
    state["last_available_at_utc"] = row["available_at_utc"]
    return state, event


def evaluate_batch(definition, source_namespace, symbol, observations):
    state = initial_state(definition, source_namespace, symbol)
    events = []
    for observation in observations:
        state, event = evaluate_prefix(definition, state, observation)
        if event is not None:
            events.append(event)
    return state, events


def resolve_outcomes(definition, events, observations, *, as_of):
    """Acrescenta labels futuros somente depois de recebidos; não altera sinais."""
    cutoff = _instant(as_of, "as_of")
    rows = sorted((_observation(raw) for raw in observations), key=lambda row: row["available_at_utc"])
    current = {}
    for row in rows:
        if _instant(row["available_at_utc"], "available_at_utc") > cutoff:
            continue
        key = (row["source_namespace"], row["symbol"], row["timeframe"], row["bar_start_utc"])
        previous = current.get(key)
        if previous is not None and (row["revision"] <= previous["revision"] or
                                     row["available_at_utc"] <= previous["available_at_utc"]):
            raise ValueError("Revisões futuras conflitantes; desfecho recusado.")
        current[key] = row
    horizon = definition.body["outcome_definition"]["horizon_bars"]
    groups = {}
    for row in current.values():
        key = (row["source_namespace"], row["symbol"], row["timeframe"])
        groups.setdefault(key, []).append(row)
    for rows_for_source in groups.values():
        rows_for_source.sort(key=lambda row: row["bar_start_utc"])
    starts = {key: [row["bar_start_utc"] for row in rows_for_source]
              for key, rows_for_source in groups.items()}
    result = []
    for event in events:
        if event["definition_hash"] != definition.definition_hash:
            raise ValueError("Evento de outra definição; desfecho recusado.")
        if _instant(event["decision_at_utc"], "decision_at_utc") > cutoff:
            raise ValueError("Evento ainda indisponível no corte; consulta futura recusada.")
        key = (event["source_namespace"], event["symbol"], event["timeframe"])
        source_starts = starts.get(key, [])
        index = max(bisect_right(source_starts, event["bar_start_utc"]),
                    bisect_left(source_starts, event["decision_at_utc"]))
        later = groups.get(key, [])[index:index + horizon]
        outcome = {"status": "censored", "reason": "future_horizon_not_received"}
        if len(later) >= horizon:
            selected = later[:horizon]
            direct_entry = selected[0]["bar_start_utc"] == event["bar_end_utc"]
            continuous = all(left["bar_end_utc"] == right["bar_start_utc"]
                             for left, right in zip(selected, selected[1:]))
            entry = _decimal(selected[0]["open"])
            close = _decimal(selected[-1]["close"])
            prices = [(_decimal(row["high"]), _decimal(row["low"])) for row in selected]
            if not direct_entry:
                outcome = {"status": "censored", "reason": "gap_or_delay_before_entry_without_calendar_proof"}
            elif not continuous:
                outcome = {"status": "censored", "reason": "gap_inside_horizon_without_calendar_proof"}
            elif entry is not None and entry > 0 and close is not None and all(None not in pair for pair in prices):
                direction = -1 if event["side"] == "short" else 1
                terminal = direction * (close / entry - 1)
                favorable = max((high / entry - 1 if direction == 1 else 1 - low / entry)
                                for high, low in prices)
                adverse = min((low / entry - 1 if direction == 1 else 1 - high / entry)
                              for high, low in prices)
                outcome = {"status": "complete", "entry_bar_start_utc": selected[0]["bar_start_utc"],
                           "end_bar_end_utc": selected[-1]["bar_end_utc"], "entry_price": str(entry),
                           "terminal_return": str(terminal), "mfe": str(favorable), "mae": str(adverse),
                           "basis": "future_bars_received_at_cutoff_not_trade_or_net_profit"}
            else:
                outcome = {"status": "censored", "reason": "invalid_or_missing_execution_price"}
        result.append({**event, "outcome": outcome})
    return result
