"""Qualidade por consumidor; não calcula indicadores nem substitui seus registries."""
import hashlib
import json
import math

import pandas as pd

from mt5_extracao.data_quality import check_quality

VERSION = 1
TICK_FIELDS = ("time_msc", "seq", "bid", "ask", "last", "volume", "volume_real", "flags")
BOOK_FIELDS = ("time_msc", "type", "price", "volume")
BARS_FIELDS = ("time", "open", "high", "low", "close", "tick_volume", "spread", "real_volume")
BAR_FAMILIES = {"time": "bar_time", "open": "price_open", "high": "price_high", "low": "price_low", "close": "price_close",
                "spread": "spread", "tick_volume": "tick_volume", "real_volume": "real_volume"}
FAMILIES = set(BAR_FAMILIES.values()) | {"ohlcv", "ohlc_prices", "hlc", "hl", "complete_bars", "observed_session_bars", "ticks", "observed_ticks",
                                       "observed_real_volume", "complete_real_ticks", "aggressor", "book_snapshots",
                                       "observed_book_snapshots", "queue"}


def requirements(definition):
    """Consome required_columns/source_id do registry T18.1, sem outro catálogo de fórmulas."""
    columns = definition.get("required_columns")
    if not isinstance(columns, (tuple, list)) or not columns:
        raise ValueError("Consumidor sem requisitos de dados; declare required_columns no contrato de origem.")
    missing = set(columns) - set(BAR_FAMILIES)
    source = str(definition.get("source_id", "bars"))
    if missing or not (source == "bars" or source.startswith("bars.")):
        raise ValueError("Fonte do consumidor exige adaptador próprio; não a substitua por OHLCV.")
    families = {BAR_FAMILIES[column] for column in columns}
    all_prices = {BAR_FAMILIES[col] for col in ("time", "open", "high", "low", "close")}
    if all_prices <= families:
        families = (families - all_prices) | {"ohlcv"}
    else:
        for columns, family in ((("open", "high", "low", "close"), "ohlc_prices"),
                                (("high", "low", "close"), "hlc"), (("high", "low"), "hl")):
            subset = {BAR_FAMILIES[col] for col in columns}
            if subset <= families:
                families = (families - subset) | {family}
                break
    return sorted(families)


def _finite(frame, columns, *, nonnegative=False):
    if frame is None or frame.empty or not set(columns) <= set(frame.columns):
        return False
    values = frame[list(columns)].apply(pd.to_numeric, errors="coerce")
    return bool(values.apply(lambda column: column.map(lambda x: pd.notna(x) and math.isfinite(float(x)))).all().all() and
                (not nonnegative or (values >= 0).all().all()))


def _canonical_row(values, family):
    fields = TICK_FIELDS if family == "ticks" else BOOK_FIELDS if family == "book" else BARS_FIELDS
    ints = {"time_msc", "seq", "flags", "type", "volume"} if family == "ticks" else {"time_msc", "type"}
    row = []
    for column, value in zip(fields, values):
        if pd.isna(value):
            row.append(None)
        elif column == "time":
            row.append(pd.Timestamp(value).isoformat())
        elif family == "bars" and column in ("tick_volume", "real_volume"):
            row.append(str(int(value)) if isinstance(value, int) or float(value).is_integer() else float(value).hex())
        elif column in ints:
            row.append(str(int(value)))
        else:
            row.append(float(value).hex())
    return row


def _digest(frame, family):
    fields = TICK_FIELDS if family == "ticks" else BOOK_FIELDS if family == "book" else BARS_FIELDS
    rows = [_canonical_row(values, family) for values in frame[list(fields)].itertuples(index=False, name=None)]
    # Independente da ordem física do SELECT; mantém multiplicidade de ticks.
    payload = json.dumps(sorted(rows, key=str), separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(payload.encode()).hexdigest()


def _bar_proofs(frame):
    for values in frame[list(BARS_FIELDS)].itertuples(index=False, name=None):
        row = _canonical_row(values, "bars")
        yield {"time": row[0], "sha256": hashlib.sha256(json.dumps([row], separators=(",", ":"), allow_nan=False).encode()).hexdigest()}


def acquisition(frame, family, *, origin="unknown", received_at=None, symbol=None, timeframe=None):
    """Provas por minuto permitem verificar subconjuntos sem confiar só no nome do provedor."""
    if family not in ("ticks", "book", "bars") or origin not in ("observed", "generated", "unknown"):
        raise ValueError("Família/origem de coleta inválida.")
    fields = TICK_FIELDS if family == "ticks" else BOOK_FIELDS if family == "book" else BARS_FIELDS
    data = frame.copy()
    reported = sorted(data.columns.tolist())
    if family == "ticks":
        data = data.sort_values("time_msc", kind="stable").reset_index(drop=True)
        data["seq"] = data.groupby("time_msc").cumcount()
    for column in fields:
        if column not in data:
            data[column] = None if family == "bars" else 0
    if family == "bars":
        duplicates = sorted({pd.Timestamp(value).isoformat() for value in data.loc[data.time.duplicated(), "time"]})
        data = data.drop_duplicates("time", keep="first")
        return {"quality_version": VERSION, "family": family, "origin": origin, "symbol": symbol, "timeframe": timeframe,
                "origin_basis": "provider_declaration_api_response_not_independent_venue_validation",
                "reported_fields": reported, "bar_proofs": list(_bar_proofs(data)),
                "duplicate_bar_times": duplicates, "received_at": received_at, "complete_market_coverage": None}
    proofs = []
    if not data.empty:
        for minute, part in data.groupby(data.time_msc // 60000, sort=True):
            proofs.append({"minute": int(minute), "rows": len(part), "sha256": _digest(part, family)})
    return {"quality_version": VERSION, "family": family, "origin": origin, "symbol": symbol,
            "origin_basis": "provider_declaration_api_response_not_independent_venue_validation",
            "reported_fields": reported, "minute_proofs": proofs,
            "received_at": received_at, "complete_market_coverage": None}


def origin_measurement(frame, family, evidence):
    counts = {"observed": 0, "generated": 0, "unknown": 0}
    if frame is None or frame.empty:
        return dict(origin="unknown", **{f"{key}_rows": value for key, value in counts.items()})
    proofs = {}
    for item in evidence:
        if item.get("quality_version") != VERSION or item.get("family") != family:
            continue
        if "symbol" in frame and set(frame.symbol) != {item.get("symbol")}:
            continue
        for proof in item.get("minute_proofs", ()):
            proofs.setdefault((proof["minute"], proof["rows"], proof["sha256"]), set()).add(item.get("origin", "unknown"))
    for minute, part in frame.groupby(pd.to_numeric(frame.time_msc) // 60000, sort=True):
        found = proofs.get((int(minute), len(part), _digest(part, family)), {"unknown"})
        kind = next(iter(found)) if len(found) == 1 and found <= {"observed", "generated"} else "unknown"
        counts[kind] += len(part)
    origin = "unknown" if counts["unknown"] else "mixed" if counts["observed"] and counts["generated"] else "observed" if counts["observed"] else "generated"
    return dict(origin=origin, **{f"{key}_rows": value for key, value in counts.items()})


def verified_origin(frame, family, evidence):
    return origin_measurement(frame, family, evidence)["origin"]


def bar_origin(frame, evidence, symbol=None, timeframe=None):
    data = frame.copy()
    for column in BARS_FIELDS:
        if column not in data:
            data[column] = None
    lookup = {}
    for item in evidence:
        if (item.get("quality_version") == VERSION and item.get("family") == "bars" and
                item.get("symbol") == symbol and item.get("timeframe") == timeframe):
            for proof in item.get("bar_proofs", ()):
                lookup.setdefault((proof["time"], proof["sha256"]), set()).add(item.get("origin", "unknown"))
    counts = {"observed": 0, "generated": 0, "unknown": 0}
    if not lookup:
        return {"origin": "unknown", "unknown_rows": len(data), "observed_rows": 0, "generated_rows": 0}
    for proof in _bar_proofs(data):
        found = lookup.get((proof["time"], proof["sha256"]), {"unknown"})
        kind = next(iter(found)) if len(found) == 1 and found <= {"observed", "generated"} else "unknown"
        counts[kind] += 1
    origin = "unknown" if counts["unknown"] else "mixed" if counts["observed"] and counts["generated"] else "observed" if counts["observed"] else "generated"
    return dict(origin=origin, **{f"{key}_rows": value for key, value in counts.items()})


def assess(bars, *, bar_minutes=1, ticks=None, book=None, evidence=(), derived=None, symbol=None, timeframe=None):
    """Mede o conteúdo capturado; None = não capturado, vazio = capturado sem linhas."""
    evidence = tuple(evidence)
    caps = {}
    def cap(name, state, reason, **extra):
        caps[name] = dict(state=state, reason=reason, **extra)

    timed = False
    diagnostic = None
    if bars is not None and not bars.empty:
        data = bars.copy()
        if "time" in data:
            data["time"] = pd.to_datetime(data.time, errors="coerce", format="ISO8601")
            timed = bool(data.time.notna().all())
        numeric = [c for c in BAR_FAMILIES if c != "time" and c in data]
        data[numeric] = data[numeric].apply(pd.to_numeric, errors="coerce")
        bars = data
    valid = bool(timed and _finite(bars, ("open", "high", "low", "close")))
    if valid:
        diagnostic = check_quality(bars, bar_minutes=bar_minutes)
        valid = not (diagnostic["invalid_ohlc"]["count"] or diagnostic["duplicate_time"]["count"])
    measured = bar_origin(bars, evidence, symbol, timeframe) if timed else {"origin": "unknown"}
    cap("ohlcv", "available" if valid else "invalid", "OHLCV finitos e horários únicos." if valid else "OHLCV ausentes, inválidos ou com horários duplicados.", **measured)
    for column in ("open", "high", "low", "close"):
        good = _finite(bars, (column,))
        cap(BAR_FAMILIES[column], "available" if good else "missing", f"Preço {column} disponível." if good else f"Falta preço {column} finito.", origin=measured["origin"])
    for family, columns in (("hl", ("high", "low")), ("hlc", ("high", "low", "close")),
                            ("ohlc_prices", ("open", "high", "low", "close"))):
        good = _finite(bars, columns)
        if good:
            prices = bars[list(columns)].apply(pd.to_numeric)
            good = (prices.high >= prices.low).all()
            for column in (set(columns) & {"open", "close"}):
                good = good and prices[column].between(prices.low, prices.high).all()
        cap(family, "available" if good else "invalid", "Preços necessários finitos e coerentes entre si." if good else "Faltam preços necessários ou há contradição entre eles.", origin=measured["origin"])
    cap("bar_time", "available" if timed else "missing", "Horário da barra não comprova seu fechamento/disponibilidade." if timed else "Falta horário válido.", origin=measured["origin"])
    for name, column in (("tick_volume", "tick_volume"), ("real_volume", "real_volume"), ("spread", "spread")):
        good = _finite(bars, (column,), nonnegative=True)
        if name == "real_volume" and good and not (pd.to_numeric(bars[column]) > 0).any():
            cap(name, "unknown", "Volume real só com zeros não comprova que a fonte o informa; tick volume não o substitui.")
        else:
            cap(name, "available" if good else "missing", f"Coluna {column} disponível." if good else f"Falta {column} válido; não há substituição por outra família.", origin=measured["origin"])
    cap("observed_real_volume", "available" if caps["real_volume"]["state"] == "available" and measured["origin"] == "observed" else "unknown",
        "Volume real exige valores próprios e prova de origem observada; tick volume não o substitui.")
    gaps = diagnostic["gaps"]["count"] if diagnostic else None
    cap("complete_bars", "unknown" if not gaps else "missing", "Completude exige grade de sessões/fonte; ausência de gap no diagnóstico não comprova cobertura integral.", diagnostic_gaps=gaps)
    matched = 0
    minimum = None
    known_real = 0
    if valid and derived is not None and not derived.empty:
        latest = derived.sort_values("revision").drop_duplicates(["session_id", "bar_start_utc"], keep="last").copy()
        latest["time"] = pd.to_datetime(latest.bar_start_broker, format="ISO8601")
        if not latest.time.duplicated().any():
            merged = bars.merge(latest, on="time", how="left", suffixes=("", "_derived"), validate="one_to_one")
            match = pd.Series(True, index=merged.index)
            for column in ("open", "high", "low", "close", "tick_volume", "real_volume"):
                if column not in bars:
                    match &= False
                else:
                    left, right = merged[column], merged[column + "_derived"]
                    match &= (left == right) | (left.isna() & right.isna())
            matched = int(match.sum())
            if matched == len(bars) and _finite(merged, ("coverage_ratio",), nonnegative=True) and (merged.coverage_ratio <= 1).all():
                minimum = float(merged.coverage_ratio.min())
                known_real = int(merged.real_volume_known.sum())
                complete = minimum == 1 and merged.quality_status.isin(("COMPLETE", "CORRECTED", "NO_TRADES")).all()
                cap("observed_session_bars", "available" if complete else "missing",
                    "Cobertura da grade de sessões observadas, com valores iguais à revisão derivada.",
                    coverage_min=minimum, matching_bars=matched, real_volume_known_bars=known_real,
                    scope="observed_sessions_not_complete_venue_calendar", origin="unknown")
    if "observed_session_bars" not in caps:
        cap("observed_session_bars", "unknown", "Falta revisão derivada que corresponda às barras; não inferir cobertura pela contagem.", matching_bars=matched)
    for family, frame in (("ticks", ticks), ("book_snapshots", book)):
        raw_family = "ticks" if family == "ticks" else "book"
        if frame is None:
            cap(family, "not_captured", "Fonte não capturada nesta revisão; capture-a para este consumidor.", origin="unknown", rows=None)
            continue
        fields = TICK_FIELDS if family == "ticks" else BOOK_FIELDS
        good = _finite(frame, fields)
        if good and not all(pd.api.types.is_numeric_dtype(frame[c]) for c in fields):
            frame = frame.copy()
            frame[list(fields)] = frame[list(fields)].apply(pd.to_numeric)
            if family == "ticks":
                ticks = frame
        integer_columns = ("time_msc", "seq", "flags", "volume") if family == "ticks" else ("time_msc", "type")
        if good:
            good = all((pd.to_numeric(frame[c]) % 1 == 0).all() for c in integer_columns)
            good = good and all(pd.to_numeric(frame[c]).between(-(2**63), 2**63 - 1).all() for c in integer_columns)
            good = good and (pd.to_numeric(frame.volume) >= 0).all()
            good = good and (pd.to_numeric(frame["flags"]).between(0, 2**31 - 1).all() and
                             (pd.to_numeric(frame.seq) >= 0).all() and (pd.to_numeric(frame.volume_real) >= 0).all()
                             if family == "ticks" else pd.to_numeric(frame.type).isin((1, 2, 3, 4)).all())
        key = ["time_msc", "seq"] if family == "ticks" else ["time_msc", "type", "price"]
        duplicates = int(frame.duplicated(key).sum()) if set(key) <= set(frame) else None
        good = good and duplicates == 0
        measured = origin_measurement(frame, raw_family, evidence) if good else {"origin": "unknown"}
        origin = measured.pop("origin")
        cap(family, "available" if good else "missing" if frame.empty else "invalid",
            "Fonte capturada, sem chaves duplicadas." if good else "Fonte vazia/inválida ou com chaves duplicadas.",
            origin=origin, rows=len(frame), duplicate_keys=duplicates, **measured)
    tick_origin = caps["ticks"]["origin"]
    cap("observed_book_snapshots", "available" if caps["book_snapshots"]["state"] == "available" and caps["book_snapshots"]["origin"] == "observed" else "unknown",
        "DOM observado exige evidência de origem consistente, sem inferir fila ou relógio da bolsa.")
    cap("observed_ticks", "available" if caps["ticks"]["state"] == "available" and tick_origin == "observed" else "unknown",
        "Origem observada declarada pelo provedor e consistente com hashes por minuto." if tick_origin == "observed" else "Origem observada não comprovada para todas as linhas capturadas.")
    cap("complete_real_ticks", "unknown", "Número de ticks/Model 4/requisições bem-sucedidas não provam cobertura integral de ticks reais.", coverage_fraction=None)
    trade_count = classified = 0
    if ticks is not None and caps["ticks"]["state"] == "available":
        flags = ticks["flags"].astype("int64")
        trade = ((flags & (8 | 16 | 32 | 64)) != 0) & ((pd.to_numeric(ticks.volume) > 0) | (pd.to_numeric(ticks.volume_real) > 0))
        side = ((flags & 32) != 0) ^ ((flags & 64) != 0)
        trade_count, classified = int(trade.sum()), int((trade & side).sum())
    cap("aggressor", "available" if trade_count and classified == trade_count and tick_origin == "observed" else "unknown",
        "Agressor exige flags inequívocas em negócios observados; não é inferido de candle, último preço ou book estático.",
        trades=trade_count, classified_trades=classified)
    cap("queue", "missing", "Snapshots DOM não informam IDs/prioridade das ordens; não provam posição na fila.")
    bar_evidence = [item for item in evidence if item.get("family") == "bars" and item.get("symbol") == symbol and item.get("timeframe") == timeframe]
    duplicates = sorted({value for item in bar_evidence for value in item.get("duplicate_bar_times", ())})
    return {"version": VERSION, "capabilities": caps, "ohlcv_diagnostic": diagnostic,
            "source_duplicate_bars_reported": len(duplicates) if bar_evidence else None,
            "coverage_basis": "captured_values_not_complete_venue_history", "source_certification": "not_independent"}


def evaluate(report, required=("ohlcv",)):
    if not required or isinstance(required, str):
        raise ValueError("Informe uma lista explícita de famílias exigidas pelo consumidor.")
    blocked, limitations = [], []
    for name in sorted(set(required)):
        if name not in report["capabilities"]:
            if name not in FAMILIES:
                raise ValueError(f"Capacidade desconhecida: {name}; declare o adaptador antes de consumir.")
            blocked.append(f"A evidência desta execução não comprova a capacidade {name}.")
            continue
        item = report["capabilities"][name]
        if item["state"] != "available":
            blocked.append(item["reason"])
        elif item.get("origin") in ("unknown", "generated", "mixed"):
            limitations.append(f"{name}: origem {item['origin']}; resultado de estudo sem certificação da fonte.")
    diagnostic = report.get("ohlcv_diagnostic")
    if "ohlcv" in required and diagnostic and diagnostic["gaps"]["count"]:
        limitations.append("Há lacunas no diagnóstico intradiário; não se afirma continuidade da série.")
    if "ohlcv" in required and report.get("source_duplicate_bars_reported"):
        limitations.append("A coleta continha barras duplicadas; a normalização não comprova qualidade da fonte original.")
    return {"version": VERSION, "required": sorted(set(required)), "eligible": not blocked,
            "status": "blocked" if blocked else "limited" if limitations else "eligible",
            "reasons": blocked, "limitations": limitations}


def as_of(frame, cutoff):
    """Seleção causal multiativo/multi-TF; revisões só aparecem depois de disponíveis."""
    required = {"symbol", "timeframe", "event_end", "available_at"}
    if not required <= set(frame):
        raise ValueError("Seleção causal exige symbol, timeframe, event_end e available_at explícitos.")
    if any(not isinstance(value, str) or not value.strip() for col in ("symbol", "timeframe") for value in frame[col]):
        raise ValueError("Seleção causal exige identificação explícita de ativo e timeframe.")
    when = pd.Timestamp(cutoff)
    if pd.isna(when) or when.tzinfo is None:
        raise ValueError("O corte causal precisa declarar fuso horário.")
    data = frame.copy()
    for column in ("event_end", "available_at"):
        if any(pd.isna(value) or pd.Timestamp(value).tzinfo is None for value in data[column]):
            raise ValueError("Disponibilidade/fechamento ausentes ou sem fuso; não infira pelo início da barra.")
        data[column] = pd.to_datetime(data[column], utc=True, format="ISO8601")
    if (data.available_at < data.event_end).any():
        raise ValueError("Disponibilidade anterior ao fechamento da barra; corrija a fonte temporal.")
    data = data[(data.event_end <= when) & (data.available_at <= when)]
    keys = ["symbol", "timeframe", "event_end"]
    if data.duplicated(keys + ["available_at"]).any():
        raise ValueError("Revisões conflitantes no mesmo instante; concilie a fonte antes do join causal.")
    return data.sort_values("available_at").drop_duplicates(keys, keep="last").sort_values(keys).reset_index(drop=True)


def summary(assessment):
    if not assessment:
        return "Qualidade dos dados não registrada nesta execução antiga."
    if "real_tick_fraction_reported" in assessment:
        if assessment.get("real_tick_coverage_status") == "model_report_conflict":
            return "Relatório e modelo solicitado divergentes; cobertura de ticks reais não qualificada."
        fraction = assessment["real_tick_fraction_reported"]
        return ("Cobertura de ticks reais não informada/conflitante no relatório." if fraction is None else
                f"Ticks reais declarados no relatório: {fraction * 100:g}%. Requisitos específicos do robô não registrados.")
    gate = assessment.get("eligibility")
    if not gate:
        return "Qualidade medida; requisitos do consumidor ainda não declarados."
    if gate.get("reasons"):
        return "Dados incompatíveis: " + " ".join(gate["reasons"])
    if gate.get("limitations"):
        return "Dados com limitações de origem/cobertura; compatíveis apenas com os requisitos declarados."
    return "Dados compatíveis com os requisitos desta execução."


def provider_origin(connector, symbol):
    value = getattr(connector, "data_origin", None)
    try:
        value = value(symbol) if callable(value) else value
    except Exception:
        value = "unknown"
    return value if value in ("observed", "generated", "unknown") else "unknown"


def tester_report(report_text, model):
    """Qualidade histórica e fração explicitamente rotulada de ticks reais são distintas."""
    import re
    import unicodedata
    from html.parser import HTMLParser

    class Cells(HTMLParser):
        def __init__(self):
            super().__init__()
            self.rows, self.row, self.cell = [], [], None
        def handle_starttag(self, tag, attrs):
            if tag in ("td", "th"):
                self.cell = []
        def handle_data(self, data):
            if self.cell is not None:
                self.cell.append(data)
        def handle_endtag(self, tag):
            if tag in ("td", "th") and self.cell is not None:
                self.row.append(" ".join(self.cell).strip())
                self.cell = None
            elif tag == "tr":
                self.rows.append(self.row)
                self.row = []
    def plain(value):
        return "".join(c for c in unicodedata.normalize("NFKD", value.lower()) if not unicodedata.combining(c)).strip(" :")
    parser = Cells()
    parser.feed(report_text)
    real, historical = [], []
    for row in parser.rows:
        for index, label in enumerate(row[:-1]):
            label, value = plain(label), plain(row[index + 1])
            known_history = label in ("history quality", "qualidade do historico", "qualidade historica")
            known_real = label in ("real ticks", "ticks reais", "real ticks percentage", "percentual de ticks reais")
            if not (known_history or known_real):
                continue
            match = re.search(r"(?<![\d.,])([0-9]+(?:[.,][0-9]+)?)\s*%", value)
            if not match:
                continue
            pct = float(match[1].replace(",", "."))
            if not 0 <= pct <= 100:
                continue
            if known_real or (known_history and ("real ticks" in value or "ticks reais" in value)):
                real.append(pct)
            elif known_history:
                historical.append(pct)
    fraction = real[0] / 100 if real and len(set(real)) == 1 else None
    conflict = str(model) in ("0", "1", "2", "3") and fraction is not None and fraction > 0
    origin = "unknown" if fraction is None or conflict else "observed" if fraction == 1 else "generated" if fraction == 0 else "mixed"
    return {"version": VERSION, "evidence_basis": "native_report_not_requested_model",
            "report_sha256": hashlib.sha256(report_text.encode("utf-8")).hexdigest(), "requested_model": str(model),
            "origin": origin, "real_tick_fraction_reported": fraction,
            "history_quality_pct_reported": historical[0] if historical and len(set(historical)) == 1 else None,
            "real_tick_coverage_status": "model_report_conflict" if conflict else "not_reported" if not real else "conflicting" if fraction is None else "reported",
            "capabilities": {"complete_real_ticks": {"state": "unknown" if conflict else "available" if fraction == 1 else "unknown" if fraction is None else "missing",
                                "reason": "Modelo solicitado e relatório divergentes; confira se pertencem à mesma execução." if conflict else
                                          "Cobertura de ticks reais explicitamente declarada no relatório." if fraction == 1 else
                                          f"Relatório informa {fraction * 100:g}% de ticks reais; este consumidor exige cobertura integral." if fraction is not None else
                                          "Cobertura de ticks reais não informada ou conflitante."},
                             "queue": {"state": "missing", "reason": "Relatório de ticks não comprova fila de ordens."}},
            "eligibility": {"status": "unqualified",
                            "eligible": None, "required": None, "reasons": [],
                            "limitations": (["Cobertura efetiva de ticks reais não informada ou conflitante."] if fraction is None else
                                            ["Parte dos ticks do teste foi gerada."] if fraction < 1 else []) +
                                            (["Modelo solicitado diverge da origem declarada no relatório."] if conflict else []) +
                                            ["Requisitos específicos do Expert não declarados no contrato atual."]}}
