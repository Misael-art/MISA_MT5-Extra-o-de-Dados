"""Compila o subconjunto causal de hipóteses M1 sobre preços discretos observados."""
from decimal import Decimal
from fractions import Fraction
from itertools import zip_longest
from string import Template

from ..analytics.hypotheses import compile_definition

FIELDS = {"open": "opening", "high": "highest", "low": "lowest", "close": "closing", "prior_close": "prior"}


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
    raise ValueError("O predicado nativo exige OHLC, prior_close ou preço literal na grade do ativo.")


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


def kernel(template, module, slot, tick):
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
    code = Template(template).substitute(slot=slot, reference=FIELDS[body["price_reference"]], condition=condition,
        session=" && (!emitted || last_day!=minute/1440)" if body["repetition_policy"] == "first_per_session" else "",
        direction=1 if body["side"] == "long" else -1, checks="\n".join(checks), variables="int value;" if checks else "")

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
