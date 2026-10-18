"""Consulta das trajetórias do ledger; não executa estratégia ou ordens."""
from numbers import Integral
from mt5_extracao import timeframes
from mt5_extracao.analytics import datasets, data_eligibility
from mt5_extracao.trade_analytics import cache, ledger, stats
from . import cycles, paths, store
from .clocks import Clock


def query(db, run_id, *, seq=None, as_of=None, unit="money", thresholds=(), ages=(), minimum=stats.MIN_TRADES):
    ages = tuple(paths._number(value) for value in ages)
    thresholds = tuple(paths._number(value) for value in thresholds)
    if any(value is None or value < 0 for value in ages):
        raise ValueError("Idades devem ser minutos finitos e não negativos.")
    ages = tuple(sorted(set(ages)))
    if any(value is None or value == 0 for value in thresholds) or len(set(thresholds)) != len(thresholds):
        raise ValueError("Limiares devem ser finitos, distintos e diferentes de zero.")
    if unit not in ("money", "price", "points", "R"):
        raise ValueError("Unidade desconhecida; escolha money, price, points ou R.")
    if seq is not None and (isinstance(seq, bool) or not isinstance(seq, Integral) or seq < 0):
        raise ValueError("Seq deve ser um inteiro não negativo do ledger.")
    seq = int(seq) if seq is not None else None
    record = store.load(db, run_id)
    snapshot = datasets.for_run(db, run_id)
    frames = {}
    with db.consistent_read() as conn:
        part = ledger.load(db, run_id=run_id, connection=conn)
        if record is not None and cache._frame_digest(part) != record["ledger_sha256"]:
            raise ValueError("Ledger mudou durante a consulta; repita a leitura do contexto atual.")
        if unit != "money" and snapshot is None:
            for symbol, tf in set(zip(part.symbol, part.timeframe)):
                parsed = timeframes.parse(tf)
                if symbol and parsed:
                    frames[(symbol, tf)] = db.load_ohlcv(symbol, parsed.label, connection=conn)
    report = {"schema_version": 1, "run_id": run_id, "unit": unit,
              "data_manifest_id": snapshot.manifest_id if snapshot is not None else None,
              "status": "unavailable", "reason": None, "units": [], "n_ledger": len(part), "duration": None,
              "data_basis": "frozen_reference" if snapshot is not None else "legacy_current_data"}
    report["absolute_crossing"] = {
        "state": "unavailable", "reason": "continuous_position_path_not_proven",
        "required": ["observed_ticks", "complete_real_ticks"],
        "data_gate": data_eligibility.evaluate(snapshot.quality, ("observed_ticks", "complete_real_ticks"))
                     if snapshot is not None else None}
    report["clock_qualification"] = "declared_fixed_offset_not_independent_historical_timezone_proof"
    calendar = snapshot.market_calendar if snapshot is not None else None
    report["trading_clock"] = {
        "state": "unavailable", "reason": "calendar_coverage_not_qualified",
        "captured_calendar_sources": sorted(calendar.source.dropna().unique().tolist())
                                     if calendar is not None and not calendar.empty else []}
    report["request"] = {"seq": seq, "as_of": str(as_of) if as_of is not None else None,
                         "unit": unit, "thresholds": list(thresholds), "ages_min": list(ages)}
    if part.empty:
        report["reason"] = "no_trades"
        return report
    if seq is not None and seq not in set(part.seq):
        raise ValueError("Operação não está no run selecionado; confira o seq do ledger.")
    if unit == "money":
        if record is None:
            report["reason"] = "missing_producer_observations"
            return report
        units = cycles.summarize(record["observations"], run_id=run_id, source=record["source"],
                                 clock=Clock(**record["clock"]), as_of=as_of, thresholds=thresholds)
        report["trajectory_id"] = record["trajectory_id"]
        report["producer_identity"] = record["producer_identity"]
    else:
        clock = Clock(**record["clock"]) if record is not None else Clock(
            "broker", snapshot.manifest["time"]["broker_utc_offset_hours"] if snapshot is not None else db.broker_utc_offset)
        model_units = {u["identity"]["seq"]: u for u in cycles.summarize(
            record["observations"], run_id=run_id, source=record["source"], clock=clock, as_of=as_of)} if record is not None else {}
        units = []
        for _, trade in part.iterrows():
            if seq is not None and trade.seq != seq and not ages:
                continue
            options = {}
            effective_trade, effective_as_of = trade.to_dict(), as_of
            modeled = model_units.get(trade.seq)
            if modeled is not None and modeled["status"] == "observed":
                if modeled["right_censored"]:
                    effective_trade["exit_time"], effective_trade["exit_price"] = None, None
                    effective_as_of = paths.UTC.timestamp(modeled["end_utc"]).tz_convert(clock.zone)
                else:
                    last = modeled["points"][-1]
                    options["exit_window"] = {"start_time": last["start_time"], "end_time": last["end_time"],
                                              "available_at": last["available_at"], "price": last["fill_price"],
                                              "phase": last["phase"]}
            if unit in ("points", "R"):
                if snapshot is None or snapshot.spec.provenance.get("specification_status") != "known":
                    raise ValueError("Conversão exige especificação historicamente qualificada; consulte preço ou recupere a origem.")
                options["point"] = snapshot.spec.point
                if unit == "R":
                    if record is None:
                        raise ValueError("R exige risco inicial comprovado pelo produtor; consulte preço ou recupere a origem.")
                    options["risk_distance"] = (trade.entry_price - trade.stop_price) * trade.direction
            source = {}
            gate = None
            quote_gate = None
            resolution_reason = "legacy_current_data"
            if snapshot is not None:
                manifest = snapshot.manifest
                tf = timeframes.parse(trade.timeframe)
                if trade.symbol == manifest["symbol"] and tf is not None and tf.name == manifest["timeframe"]:
                    ticks = snapshot.market_frame("ticks") if "ticks" in manifest["derivation"].get("market_families", []) else None
                    gate = data_eligibility.evaluate(snapshot.quality, ("ticks",))
                    quote_gate = gate
                    if ticks is not None and not ticks.empty and gate["eligible"]:
                        source, resolution_reason = {"quotes": ticks}, "captured_quotes"
                    else:
                        gate = data_eligibility.evaluate(snapshot.quality, ("ohlc_prices",))
                        source = {"bars": snapshot.bars} if gate["eligible"] else {}
                        resolution_reason = "qualified_ohlc_fallback" if gate["eligible"] else "no_qualified_price_source"
            elif (trade.symbol, trade.timeframe) in frames:
                if record is None or clock.zone == Clock("broker", db.broker_utc_offset).zone:
                    source = {"bars": frames[(trade.symbol, trade.timeframe)]}
                else:
                    resolution_reason = "producer_market_clocks_not_aligned"
            item = paths.reconstruct(effective_trade, clock=clock, as_of=effective_as_of, unit=unit, thresholds=thresholds,
                                     **source, **options)
            if modeled is not None and modeled["status"] == "observed":
                item["sequence_basis"] = "modeled_execution_order"
                item["position_timing_source"] = "producer_observations"
                if "as_of_between_observations" in modeled["limitations"]:
                    item["limitations"].append("as_of_between_observations")
            if item["coverage_state"] == "execution_endpoints_only":
                resolution_reason = "execution_endpoints_only"
            if snapshot is not None:
                item["data_quality"] = snapshot.quality
            item["path_gate"], item["resolution_reason"] = gate, resolution_reason
            item["quote_gate"] = quote_gate
            units.append(item)
    for item in units:
        # O calendário canônico atual é inferido de M1, sem contrato de cobertura
        # integral de pregão. Não transformar lacuna em feriado/pausa nem 24x7.
        item.setdefault("trading_min", None)
        item["trading_clock"] = dict(report["trading_clock"])
    if ages:
        # A seleção de uma linha não transforma a população do run em apenas
        # aquele vencedor/perdedor nem trata reexecuções como novas amostras.
        enriched = []
        for item in units:
            trade = part[part.seq == item["identity"]["seq"]].iloc[0]
            context = {name: None if paths._missing(trade[name]) else
                       trade[name].item() if hasattr(trade[name], "item") else trade[name]
                       for name in ("symbol", "timeframe", "robot_key", "direction", "params_hash")}
            enriched.append({**item, "identity": {**item["identity"], **context}})
        report["duration"] = {"population": "full_run", **stats.duration_curve(enriched, ages, minimum=minimum)}
    if seq is not None:
        units = [u for u in units if u["identity"]["seq"] == seq]
    report["units"] = units
    report["status"] = "observed" if any(u["status"] == "observed" for u in units) else "unavailable"
    return report
