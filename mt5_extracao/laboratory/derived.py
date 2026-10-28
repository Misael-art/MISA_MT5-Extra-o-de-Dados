"""Leitura das barras já gravadas em ``_bars_derived``.

Não agrega, não extrai, não grava e não cria tabela. M7/M9 e os demais tempos de
dia exato saem da última revisão fechada. Tempos do enum continuam na captura
nativa de ``datasets``. Dois ou mais tempos comparam o fechamento já gravado
na grade do mais curto; um buraco não vira candle.
"""
import hashlib
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

from mt5_extracao import timeframes
from mt5_extracao.analytics import data_eligibility, indicator_registry
from mt5_extracao.analytics.resampling import latest_bars, parse_target
from mt5_extracao.chart_study import catalog as study_catalog
from mt5_extracao.laboratory import service
from mt5_extracao.laboratory.service import MAX_BARS, MAX_TIMEFRAMES, sources

NOTE = "sintético, âncora 00:00 do calendário da corretora, não é tempo do MT5"


def selection_plan(items):
    """``native``, ``derived``, ``mixed`` ou ``empty`` a partir das fontes já listadas."""
    if not items:
        return "empty"
    flags = {bool(item.get("synthetic")) for item in items}
    if flags == {True}:
        return "derived"
    if flags == {False}:
        return "native"
    return "mixed"


def _table_ready(db):
    from sqlalchemy import inspect
    return db is not None and getattr(db, "engine", None) is not None and inspect(db.engine).has_table("_bars_derived")


def _minutes(item):
    parsed = timeframes.parse(item["timeframe"])
    if parsed is not None:
        return parsed.minutes
    try:
        target = parse_target(item["timeframe"])
    except ValueError:
        return 10**9
    return target.minutes if target.minutes is not None else 10**9


def list_stored(db):
    """Séries de dia exato que já existem. Tempos do MT5 gravados na mesma tabela ficam de fora."""
    if not _table_ready(db):
        return []
    from sqlalchemy import text
    with db.engine.connect() as conn:
        pairs = conn.execute(text("SELECT DISTINCT symbol, tf FROM _bars_derived ORDER BY symbol, tf")).fetchall()
    result = []
    for symbol, label in pairs:
        try:
            target = parse_target(label)
        except ValueError:
            continue
        if target.mode != "exact_day":
            continue
        frame = latest_bars(db, symbol, target.label, closed_only=True)
        if frame.empty:
            continue
        starts = pd.to_datetime(frame["bar_start_broker"])
        result.append(dict(
            symbol=str(symbol), timeframe=target.label, rows=int(len(frame)),
            start=starts.min().isoformat(sep=" "), end=starts.max().isoformat(sep=" "),
            synthetic=True, mt5_predefined=False, day_origin="broker_00:00",
            kind="bars_derived", note=NOTE))
    return result


def earliest_of_last(db, symbol, timeframe_name, limit=MAX_BARS):
    """Início, no relógio da corretora, das últimas ``limit`` barras nativas.

    None quando a série inteira cabe no limite ou o tempo não está na base.
    Não lê OHLC: recusar a janela inteira não carrega a série que não cabe.
    """
    from sqlalchemy import text

    tf = timeframes.parse(timeframe_name)
    if db is None or getattr(db, "engine", None) is None or tf is None or limit < 1:
        return None
    table = db.find_table_for_symbol(symbol, tf.label)
    if not table:
        return None
    quote = db.engine.dialect.identifier_preparer.quote
    with db.engine.connect() as conn:
        value = conn.execute(
            text(f"SELECT time FROM {quote(table)} ORDER BY time DESC LIMIT 1 OFFSET :off"),
            {"off": int(limit) - 1}).scalar()
    if value is None:
        return None
    stamp = pd.Timestamp(db._from_storage(pd.Timestamp(value)))
    if stamp.tzinfo is not None:
        stamp = stamp.tz_localize(None)
    return stamp.isoformat()


def chart_sources(db):
    """Inventário nativo mais as séries de dia exato já gravadas. Não cria barras."""
    native = [{**item, "synthetic": False, "mt5_predefined": True, "kind": "native"} for item in sources(db)]
    return sorted([*native, *list_stored(db)], key=lambda row: (row["symbol"], _minutes(row), row["timeframe"]))


def _broker_bound(value):
    if value is None or value == "":
        return None
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is not None:
        raise ValueError("A janela da leitura sintética usa o relógio da corretora, sem fuso.")
    return stamp


def _exact_frames(db, symbol, timeframes_selected, start=None, end=None, cutoff=None):
    """Última revisão fechada de cada tempo de dia exato. Não cria a barra que falta."""
    labels = list(timeframes_selected)
    if not labels or len(labels) > MAX_TIMEFRAMES or len(set(labels)) != len(labels):
        raise ValueError("Escolha de 1 a 8 tempos sintéticos distintos já gravados.")
    start_at, end_at = _broker_bound(start), _broker_bound(end)
    loaded = []
    for label in labels:
        target = parse_target(label)
        if target.mode != "exact_day":
            raise ValueError(
                f"{label} não é leitura de dia exato. Tempos do MT5 continuam na captura nativa.")
        frame = latest_bars(db, symbol, target.label, closed_only=True)
        if not frame.empty and (start_at is not None or end_at is not None):
            stamps = pd.to_datetime(frame["bar_start_broker"])
            mask = pd.Series(True, index=frame.index)
            if start_at is not None:
                mask &= stamps >= start_at
            if end_at is not None:
                mask &= stamps <= end_at
            frame = frame.loc[mask].reset_index(drop=True)
        if cutoff is not None and not frame.empty:
            frame = frame.loc[frame["bar_end_utc"] <= cutoff].reset_index(drop=True)
        if frame.empty:
            raise ValueError(
                f"{target.label}: nenhuma barra completa de {symbol} na janela. "
                "Amplie o intervalo ou gere as barras com resample --session-aware. "
                "Esta leitura não cria candles.")
        if int(frame["bar_count"].ne(target.minutes).sum()) or int(frame["is_partial"].ne(0).sum()):
            raise ValueError(
                f"{target.label}: a leitura só desenha grupo completo. Há barra curta ou parcial gravada.")
        if len(frame) > MAX_BARS:
            suggested = pd.Timestamp(frame["bar_start_broker"].iloc[-MAX_BARS]).isoformat(sep=" ")
            raise ValueError(
                f"{target.label}: {len(frame)} barras na janela; o limite é {MAX_BARS}. "
                f"Informe início a partir de {suggested} (últimas {MAX_BARS} barras) ou um fim anterior.")
        loaded.append((target, frame))
    loaded.sort(key=lambda item: (item[0].minutes, item[0].label))
    return loaded


def _gap_count(frame, minutes):
    starts = pd.to_datetime(frame["bar_start_utc"], utc=True)
    if len(starts) < 2:
        return 0
    expected = starts.iloc[:-1] + pd.Timedelta(minutes=minutes)
    return int((starts.iloc[1:].reset_index(drop=True) != expected.reset_index(drop=True)).sum())


def read_stored(db, symbol, timeframes_selected, start=None, end=None):
    """Devolve OHLC da última revisão fechada. Recusa tempo nativo e não preenche buraco."""
    series = []
    for target, frame in _exact_frames(db, symbol, timeframes_selected, start, end):
        starts = pd.to_datetime(frame["bar_start_broker"])
        prices = [[float(row.open), float(row.high), float(row.low), float(row.close)]
                  for row in frame.itertuples(index=False)]
        series.append(dict(
            timeframe=target.label, minutes=int(target.minutes), rows=int(len(frame)),
            gaps=_gap_count(frame, target.minutes),
            start=starts.iloc[0].isoformat(sep=" "), end=starts.iloc[-1].isoformat(sep=" "),
            times=[stamp.isoformat(sep=" ") for stamp in starts], prices=prices,
            bar_count_min=int(frame["bar_count"].min()), bar_count_max=int(frame["bar_count"].max()),
            partials=0))
    return dict(kind="stored_derived", symbol=str(symbol), synthetic=True, mt5_predefined=False,
                day_origin="broker_00:00", note=NOTE, series=series)


def _derived_source_hash():
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def _align_minutes(channel, grid, minutes):
    """Junta o valor no fechamento da grade e o retém só até o fechamento seguinte da origem."""
    source = pd.DataFrame(dict(at=channel["source_end"], source_end=channel["source_end"],
        value=channel["values"].to_numpy(), direction=channel["direction"].to_numpy()))
    joined = pd.merge_asof(pd.DataFrame(dict(at=grid)), source, on="at", direction="backward")
    expiry = joined.source_end + pd.Timedelta(minutes=int(minutes))
    valid = joined.source_end.notna() & (joined["at"] < expiry)
    joined.loc[~valid, ["value", "direction"]] = np.nan
    joined.loc[~valid, "source_end"] = pd.NaT
    return joined


def _series_channels(frame, label, minutes, feature_ids, strategies, cutoff):
    ends = pd.DatetimeIndex(pd.to_datetime(frame["bar_end_utc"], utc=True))
    starts = pd.DatetimeIndex(pd.to_datetime(frame["bar_start_utc"], utc=True))
    if ends.hasnans or not ends.is_monotonic_increasing or ends.has_duplicates:
        raise ValueError(f"{label}: fechamentos exigem horário UTC estritamente crescente e sem duplicata.")
    if len(starts) > 1 and bool((starts[1:] < ends[:-1]).any()):
        raise ValueError(f"{label}: barras sobrepostas na revisão gravada; a comparação não as mistura.")
    broker_time = pd.DatetimeIndex(pd.to_datetime(frame["bar_start_broker"]))
    if broker_time.tz is not None:
        broker_time = broker_time.tz_localize(None)
    bars = pd.DataFrame({
        "time": broker_time,
        "open": pd.to_numeric(frame["open"], errors="coerce"),
        "high": pd.to_numeric(frame["high"], errors="coerce"),
        "low": pd.to_numeric(frame["low"], errors="coerce"),
        "close": pd.to_numeric(frame["close"], errors="coerce"),
    })
    if "tick_volume" in frame.columns:
        bars["tick_volume"] = pd.to_numeric(frame["tick_volume"], errors="coerce")
    eligibility = data_eligibility.assess(bars, bar_minutes=minutes, symbol=None, timeframe=label)
    prefix_quality = data_eligibility.assess(bars, bar_minutes=minutes)

    def eligibility_for(definition):
        required = data_eligibility.requirements(definition)
        check = data_eligibility.evaluate(eligibility, required)
        prefix = data_eligibility.evaluate(prefix_quality, required)
        if not prefix["eligible"]:
            check = dict(prefix, reasons=check["reasons"] + prefix["reasons"])
        return check

    selected = list(feature_ids)
    computed = pd.DataFrame(indicator_registry.iter_core(bars, feature_ids=selected)) if selected else pd.DataFrame()
    result = []
    for ident in selected:
        definition = indicator_registry.get_definition(ident)
        check = eligibility_for(definition)
        rows = computed.loc[computed.feature_id == ident].set_index("bar_index").reindex(bars.index)
        values = pd.to_numeric(rows.value, errors="coerce").where(rows.state == "AVAILABLE")
        if not check["eligible"]:
            values[:] = np.nan
        rule, legend = study_catalog.rule(definition)
        events = service._events(rows, definition) if ident.startswith("structure.") else None
        direction = service._direction(values, bars["close"], rule)
        result.append(dict(id=f"{label}:{ident}", feature=ident, name=definition["name"], timeframe=label,
            parameters=definition["parameters"], unit=definition["unit"], rule=rule, legend=legend,
            algorithm_version=definition["algorithm_version"], values=values, direction=direction,
            source_end=ends, source_start=starts, eligibility=check,
            states=dict(Counter(rows.state)), warmup=definition["warmup_count"], events=events))
    for strategy in strategies:
        check = eligibility_for(dict(required_columns=strategy.required_columns, source_id="bars"))
        values = pd.Series(np.nan, index=bars.index, dtype=float)
        if check["eligible"]:
            signals = strategy.gerar_sinais(bars)
            values[:] = np.where(signals.entry_long & signals.entry_short, np.nan,
                        np.where(signals.entry_long, 1., np.where(signals.entry_short, -1., 0.)))
            values.iloc[:strategy.min_bars - 1] = np.nan
        result.append(dict(id=f"{label}:strategy:{strategy.key}", feature="strategy:" + strategy.key,
            name=strategy.info.name, timeframe=label, parameters=strategy.params, unit="candidate_-1_0_1",
            algorithm_version="current_source", rule="candidate", legend="Compra / neutro / venda; não são trades",
            values=values, direction=values, source_end=ends, source_start=starts, eligibility=check,
            states=dict(AVAILABLE=int(values.notna().sum()), UNAVAILABLE=int(values.isna().sum())),
            warmup=strategy.min_bars, events=None))
    prices = bars[["open", "high", "low", "close"]].astype(float)
    return result, dict(symbol=None, timeframe=label, minutes=int(minutes), closed_rows=len(bars),
        first_open_utc=starts[0].isoformat(), last_close_utc=ends[-1].isoformat(),
        gaps=_gap_count(frame, minutes), nominal_closes=ends, prices=prices, cutoff=cutoff)


def compare_stored(db, symbol, timeframes_selected, *, start=None, end=None,
                   feature_ids=service.DEFAULT_FEATURES, strategy_key=None, strategy_params=None,
                   strategies=None, as_of):
    """Compara tempos de dia exato já gravados na grade do fechamento mais curto.

    Não chama ``datasets.capture``, não agrega e não preenche o intervalo recusado.
    O valor de um tempo mais longo só entra depois do fechamento gravado e sai no
    fechamento seguinte dessa mesma série.
    """
    labels = list(timeframes_selected)
    if len(labels) < 2:
        raise ValueError(
            "A comparação sincronizada pede ao menos dois tempos de dia exato. "
            "Um tempo sozinho continua na leitura, sem indicador.")
    selected = list(feature_ids)
    if len(set(selected)) != len(selected):
        raise ValueError("Indicadores duplicados; selecione cada definição uma vez.")
    for ident in selected:
        indicator_registry.get_definition(ident)
    if isinstance(strategies, (str, bytes)) or (strategies is not None and not isinstance(strategies, (list, tuple))):
        raise ValueError("A comparação de estratégias exige uma lista de chaves distintas.")
    if strategies is not None and len(strategies) == 0:
        strategies = None
    n_strategies = len(strategies) if strategies is not None else bool(strategy_key)
    if not selected and not n_strategies:
        raise ValueError("Escolha indicadores ou uma estratégia.")
    if (len(selected) + n_strategies) * len(labels) > service.MAX_CHANNELS:
        raise ValueError(f"Seleção excede {service.MAX_CHANNELS} canais; separe o estudo em recortes explícitos.")
    cutoff = service._utc(as_of)
    loaded = _exact_frames(db, symbol, labels, start, end, cutoff)
    if len(loaded) < 2:
        raise ValueError("A comparação sincronizada pede ao menos dois tempos de dia exato.")
    # O schema de preset só nomeia tempos do enum. A checagem de parâmetros usa M1 e
    # descarta esse retrato; os canais e a grade continuam com o rótulo de dia exato.
    resolved, listed = service._resolve_strategies(
        strategy_key, strategy_params, strategies, symbol, "M1")
    channels, populations = [], []
    for target, frame in loaded:
        built, population = _series_channels(frame, target.label, target.minutes, selected, resolved, cutoff)
        population["symbol"] = str(symbol)
        channels.extend(built)
        populations.append(population)
    grid = populations[0]["nominal_closes"]
    aligned = [_align_minutes(channel, grid, channel_minutes(channel, populations))
               for channel in channels]
    unconfirmed = [bool(channel["events"] and not channel["events"]["usable"]) for channel in channels]
    pairs = [dict(left=left["id"], right=right["id"],
                  **service._pair(aligned[i], aligned[j], lag, unconfirmed[i] or unconfirmed[j]))
             for i, left in enumerate(channels) for j, right in enumerate(channels)
             for lag in ((0,) if i == j else (0, *service.PREDICTIVE_LAGS) if j > i else service.PREDICTIVE_LAGS)]
    prices = [[float(value) if pd.notna(value) else None for value in row]
              for row in populations[0]["prices"].itertuples(index=False, name=None)]
    grid_minutes = populations[0]["minutes"]
    gap_count = populations[0]["gaps"]
    for population in populations:
        population.pop("nominal_closes")
        population.pop("prices")
        population.pop("cutoff")
    output_channels = []
    for channel, frame in zip(channels, aligned):
        summary = {key: value for key, value in channel.items() if key not in
                   {"values", "direction", "source_start", "source_end"}}
        summary.update(available=int(frame.value.notna().sum()), total=len(frame),
            source_rows=len(channel["values"]), source_available=int(channel["values"].notna().sum()),
            candidate_counts=dict(long=int(channel["values"].eq(1).sum()), short=int(channel["values"].eq(-1).sum()))
                if channel["rule"] == "candidate" else None,
            coverage=float(frame.value.notna().mean()) if len(frame) else 0.0,
            values=[float(value) if pd.notna(value) and np.isfinite(value) else None for value in frame.value],
            directions=[int(value) if pd.notna(value) else None for value in frame.direction],
            source_ends=[value.isoformat() if pd.notna(value) else None for value in frame.source_end])
        summary["diagnosis"] = service.diagnose(channel, source_rows=summary["source_rows"],
            source_available=summary["source_available"], available=summary["available"])
        output_channels.append(summary)
    selection = dict(kind="stored_exact_day", manifests=[], feature_ids=selected,
        timeframes=[item[0].label for item in loaded], day_origin="broker_00:00",
        derived_source_sha256=_derived_source_hash(),
        strategy=dict(key=resolved[0].key, params=resolved[0].params) if resolved and not listed else None,
        as_of=cutoff.isoformat())
    if listed:
        selection["strategies"] = [dict(key=item.key, params=item.params, role="intention") for item in resolved]
        selection["comparison"] = dict(
            scope="mesmo ativo, origem gravada, relógio e corte",
            intention="sinais do gerador; não são trades, fills nem R",
            operation="operação ausente neste relatório",
            execution="execução ausente neste relatório",
            costs="custos não aplicados; a curva não é PnL")
    source_hash = service.formula_source_hash()
    limitations = list(service.LIMITATIONS) + [
        "Barras de dia exato já gravadas, âncora 00:00 da corretora. A grade é o fechamento do tempo "
        "mais curto. Um intervalo sem grupo completo não vira candle, linha nem par imputado.",
    ]
    return dict(laboratory_version=service.VERSION, kind="stored_exact_day_comparison", synthetic=True,
        mt5_predefined=False, day_origin="broker_00:00", note=NOTE,
        analysis_id=service._hash(dict(version=service.VERSION, source_hash=source_hash, **selection)),
        grid_gap_count=gap_count, grid_minutes=grid_minutes,
        formula_source_sha256=source_hash, price_columns=["open", "high", "low", "close"], prices=prices,
        selection=selection, symbol=str(symbol), grid_timeframe=populations[0]["timeframe"],
        times=[stamp.isoformat() for stamp in grid], channels=output_channels, pairs=pairs,
        populations=populations, alignment="nominal_closed_bar_backward_join_expiry_one_source_period",
        qualification=dict(exploratory=True, historical_availability_proven=False,
                           independent_observations=False, operational_acceptance=False),
        limitations=limitations)


def channel_minutes(channel, populations):
    for population in populations:
        if population["timeframe"] == channel["timeframe"]:
            return population["minutes"]
    raise ValueError(f"{channel['timeframe']}: minutos da série gravada ausentes na comparação.")


def public_comparison(report):
    """Resumo da comparação, sem a série de preços nem os valores de cada barra."""
    channels = [dict(id=row["id"], timeframe=row["timeframe"], name=row["name"],
                     available=row["available"], total=row["total"],
                     diagnosis=row.get("diagnosis")) for row in report["channels"]]
    pairs = [dict(left=row["left"], right=row["right"], lag_bars=row["lag_bars"],
                  relation=row["relation"], correlation=row["correlation"], n_pairs=row["n_pairs"],
                  reason=row["reason"],
                  verdict=(row.get("out_of_sample") or {}).get("verdict"))
             for row in report["pairs"]]
    return dict(kind=report["kind"], symbol=report["symbol"], synthetic=True, note=report["note"],
                day_origin=report["day_origin"], grid_timeframe=report["grid_timeframe"],
                grid_minutes=report["grid_minutes"], grid_gap_count=report["grid_gap_count"],
                bars=len(report["times"]), analysis_id=report["analysis_id"],
                channels=channels, pairs=pairs, limitations=report["limitations"])


def public_summary(read, head=0):
    """O CLI não imprime milhares de preços. ``head`` pede só as primeiras barras."""
    shown = max(0, int(head))
    series = []
    for item in read["series"]:
        summary = {key: value for key, value in item.items() if key not in {"times", "prices"}}
        if shown:
            summary["head"] = [dict(time=item["times"][index], open=item["prices"][index][0],
                                     high=item["prices"][index][1], low=item["prices"][index][2],
                                     close=item["prices"][index][3])
                               for index in range(min(shown, item["rows"]))]
        series.append(summary)
    return {**{key: value for key, value in read.items() if key != "series"}, "series": series}
