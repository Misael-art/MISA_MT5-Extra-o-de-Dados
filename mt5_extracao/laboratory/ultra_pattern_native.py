"""Compila o subconjunto causal de hipóteses M1 sobre preços discretos observados."""
from decimal import Decimal
from fractions import Fraction
from itertools import zip_longest
from string import Template

from ..analytics.hypotheses import compile_definition
from ..analytics.pattern_families import compile_family

FIELDS = {"open": "opening", "high": "highest", "low": "lowest", "close": "closing", "prior_close": "prior",
          "reference_price": "breakout_reference"}


def breakout_lookback(body):
    """Localiza o produtor da referência; uma referência externa arbitrária não é exportável."""
    if body["price_reference"] != "reference_price" or body["pattern_id"] != "breakout_failure":
        return None
    parameters = body["complexity"].get("parameters")
    clock = body["timezone_calendar_versions"]
    if not {"clock", "calendar"} <= clock.keys():
        raise ValueError("A referência de rompimento exige versões de relógio e calendário.")
    canonical = compile_family("breakout_failure", parameters, symbol=body["universe_definition"]["symbols"][0],
                               timeframe=body["timeframe"], clock_version=clock["clock"], calendar_version=clock["calendar"],
                               cost_model_id=body["cost_model_id"], horizon_bars=body["outcome_definition"]["horizon_bars"]).body
    if any(body[key] != canonical[key] for key in ("side", "condition_expression")):
        raise ValueError("A referência de rompimento exige o predicado e o lado vendidos da família canônica.")
    return parameters["lookback_bars"]


def _price(node, tick):
    if node["op"] == "field" and node["name"] in FIELDS:
        return FIELDS[node["name"]]
    if node["op"] == "literal" and node["unit"] == "price":
        literal = Decimal(node["value"])
        if literal.is_zero():
            return "0"
        if abs(literal.adjusted() - tick.adjusted()) > 52:
            raise ValueError("O literal excede a precisão da grade do Expert.")
        price = Fraction(literal) / Fraction(tick)
        if abs(price) < 2**52 and price.denominator == 1:
            return str(price.numerator)
    raise ValueError("O predicado nativo exige preço observado ou literal na grade do ativo.")


def _predicate(node, tick):
    op = node["op"]
    if op == "compare":
        which = {"lt": 0, "lte": 1, "gt": 2, "gte": 3}[node["comparison"]]
        left, right = node["left"], node["right"]
        if left["op"] == "percent_change" and right["op"] == "literal" and right["unit"] == "fraction":
            number = Decimal(right["value"])
            if number != 0 and (number.adjusted() < -9 or abs(number) >= 1):
                raise ValueError("Use uma fração nativa em (-1, 1) com até nove casas decimais.")
            ratio = Fraction(number)
            if ratio.denominator > 10**9:
                raise ValueError("Denominador da variação fora da precisão nativa.")
            return (f'PatternChange({_price(left["value"], tick)},{_price(left["reference"], tick)},'
                    f'{ratio.numerator},{ratio.denominator},{which})')
        return f'PatternCmp({_price(node["left"], tick)},{_price(node["right"], tick)},{which})'
    if op == "not":
        return f'PatternNot({_predicate(node["term"], tick)})'
    if op in ("and", "or"):
        function = "PatternAnd" if op == "and" else "PatternOr"
        terms = iter(node["terms"])
        expression = _predicate(next(terms), tick)
        for term in terms:
            expression = f"{function}({expression},{_predicate(term, tick)})"
        return expression
    raise ValueError(f"O operador de hipótese {op} ainda não tem implementação no Expert.")


def kernel(template, module, slot, tick, *, market=False):
    params = module["params"]
    body = compile_definition(params["definition"]).body
    if body["timeframe"] != "M1" or body["side"] not in {"long", "short"}:
        raise ValueError("O Expert aceita hipóteses direcionais no fechamento de M1.")
    if body["price_reference"] not in FIELDS or set(body["data_requirements"]) - {"ohlcv"} or body["features_and_versions"]:
        raise ValueError("O Expert aceita hipóteses de preço M1 com referência OHLC conhecida e sem features externas.")
    condition = _predicate(body["condition_expression"], tick)
    if body["context_expression"] is not None:
        condition = f'PatternAnd({_predicate(body["context_expression"], tick)},{condition})'
    checks = []
    for i, rule in enumerate(body["invalidation_rules"]):
        if rule["action"] != "cancel_before_entry":
            raise ValueError("A invalidação nativa implementa cancel_before_entry.")
        checks.append(f'         value={_predicate(rule["when"], tick)};')
        checks.append(f'         PrintFormat("MT5X_ULTRA_PREDICATE1|{slot}|%I64d|%I64d|{i}|%d",msc,available,value);')
        if params["apply_invalidation"]:
            checks.append("         invalid=PatternOr(invalid,value);")
    reference = FIELDS[body["price_reference"]]
    lookback = breakout_lookback(body)
    if lookback is None and "breakout_reference" in "\n".join([reference, condition, *checks]):
        raise ValueError("Referência externa sem produtor nativo de barras fechadas.")
    history = prepare = remember = trace = ""
    market_gate = f"   if({reference}==LONG_MIN || ({condition})!=1) return;"
    if lookback is not None and market:
        prepare = f"""   if(last<{lookback} || b.time[last-{lookback}]*1000<UltraStart) return;
   long breakout_reference=LONG_MIN;
   for(int i=last-{lookback};i<last;i++)
      breakout_reference=MathMax(breakout_reference,(long)MathRound(b.high[i]/_Point));"""
        log = ('PrintFormat("MT5X_ULTRA_BREAKOUT_BAR|%I64d|%I64d|%I64d|%I64d|%I64d|%d|%d",'
               'msc,available,breakout_reference,highest,closing,')
        market_gate = f"{prepare}\n   if(({condition})!=1) {{ {log}0,0); return; }}"
        trace = f"   {log}1,invalid);\n"
    elif lookback is not None:
        history = f"   long previous_highs[{lookback}];\n   int high_count, high_cursor;"
        prepare = f"""            long breakout_reference=LONG_MIN;
            if(high_count=={lookback})
               for(int i=0;i<{lookback};i++) breakout_reference=MathMax(breakout_reference,previous_highs[i]);"""
        remember = f"""            previous_highs[high_cursor]=highest;
            high_cursor=(high_cursor+1)%{lookback};
            if(high_count<{lookback}) high_count++;"""
    code = Template(template).substitute(slot=slot, reference=reference, condition=condition,
        session=" && (!emitted || last_day!=minute/1440)" if body["repetition_policy"] == "first_per_session" else "",
        direction=1 if body["side"] == "long" else -1, checks="\n".join(checks), variables="int value;" if checks else "",
        history=history + "\n" if history else "", prepare_reference=prepare + "\n" if prepare else "",
        remember_high=remember + "\n" if remember else "", market_gate=market_gate, trace_bar=trace)

    if "PatternChange(" in code:
        # Produtos inteiros preservam a inclusão da fronteira da fração decimal.
        code = """#ifndef MT5X_ULTRA_PATTERN_CHANGE
#define MT5X_ULTRA_PATTERN_CHANGE
int PatternChange(const long value,const long reference,const long numerator,const long denominator,const int comparison)
{
   if(value<0 || reference<=0 || denominator<=0) return -1;
   long difference=value-reference;
   if(difference>LONG_MAX/denominator || difference<-(LONG_MAX/denominator) ||
      (numerator!=0 && reference>LONG_MAX/(numerator>0 ? numerator : -numerator))) return -1;
   long a=difference*denominator, b=reference*numerator;
   return (int)(comparison==0 ? a<b : comparison==1 ? a<=b : comparison==2 ? a>b : a>=b);
}
#endif
""" + code
    return code


def audit(db, package, frames, lines):
    """Confronta cada valor ternário do journal com a avaliação causal da hipótese."""
    from ..analytics import datasets
    from . import ultra

    marker = "MT5X_ULTRA_PREDICATE1|"
    modules = package["policy"]["modules"]
    expected = []
    if any(m["strategy"] == "pattern_definition" for m in modules):
        _, scheduled = ultra._inputs(datasets.load(db, package["source_manifest"]), package["policy"])
        for index, frame in enumerate(frames):
            signals = scheduled.get(index, {})
            for slot, module in enumerate(modules):
                signal = signals.get(module["owner"])
                if module["strategy"] != "pattern_definition" or signal is None:
                    continue
                for i, test in enumerate(signal["entry_decision"]["rules"]):
                    value = -1 if test["value"] is None else int(test["value"])
                    expected.append((slot, frame["mark"]["key"][0], signal["available_msc"], i, value))
    return _compare("pattern.rule_value", marker, modules, frames, lines, expected)


def audit_signals(package, frames, lines):
    modules = package["policy"]["modules"]
    slots = {m["owner"]: i for i, m in enumerate(modules)}
    expected = [(slots[e["owner"]], e["key"][0], e["available_msc"], e["side"], e["reference"])
                for frame in frames for e in frame["events"] if e["event"] == "armed"]
    return _compare("owner.signal", "MT5X_ULTRA_SIGNAL1|", modules, frames, lines, expected)


def _compare(field, marker, modules, frames, lines, expected):
    observed = []
    malformed = None
    for line in lines:
        if marker in line:
            fields = line.partition(marker)[2].split("|")
            try:
                slot, msc, available, index, value = map(int, fields)
                valid_values = (index in (-1, 1) if field == "owner.signal" else index >= 0 and value in (-1, 0, 1))
                if not (0 <= slot < len(modules) and msc >= available > 0 and valid_values):
                    raise ValueError
            except ValueError:
                malformed = fields
                break
            observed.append((slot, msc, available, index, value))
    difference = None
    for pos, (wanted, got) in enumerate(zip_longest(expected, observed)):
        if wanted != got:
            slot, msc, available, _, _ = wanted or got
            key = next((frame["mark"]["key"] for frame in frames if frame["mark"]["key"][0] == msc), None)
            if malformed is not None and pos == len(observed):
                got = malformed
            difference = {"field": field, "key": key, "slot": slot, "available_msc": available,
                          "expected": wanted, "observed": got}
            break
    if malformed is not None and difference is None:
        difference = {"field": field, "key": None, "expected": None, "observed": malformed}
    return {"first_divergence": difference, "expected_records": len(expected), "observed": observed}
