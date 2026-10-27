"""T20.22-LAB1: comparação exploratória reproduzível entre séries e timeframes.

Não materializa fatos point-in-time nem substitui o query_service BD18a. Reconstrói
fórmulas canônicas na revisão selecionada e alinha pelo fechamento NOMINAL. A
disponibilidade histórica de recebimento/revisão continua desconhecida.
"""
from collections import Counter
from dataclasses import asdict
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from mt5_extracao import timeframes
from mt5_extracao.analytics import data_eligibility, datasets, indicator_registry
from mt5_extracao.chart_study import catalog as study_catalog
from mt5_extracao.composition import base_composition
from mt5_extracao.composition.registry import STRATEGY_PARAMETERS
from mt5_extracao.strategies import REGISTRY, get_strategy

VERSION = 1
MAX_CHANNELS = 32
MAX_BARS = 20_000
MAX_TIMEFRAMES = 8
DEFAULT_FEATURES = ("momentum.rsi_14", "volatility.atr_pct_14", "volatility.bb_20_position",
                    "volatility.kc_20_position", "trend.efficiency_ratio_20")
LIMITATIONS = (
    "Exploração descritiva; correlação não é acerto, causalidade, lucro ou probabilidade calibrada.",
    "Fechamento nominal não comprova recebimento histórico nem ausência de revisão/repaint.",
    "Janelas, sementes, parâmetros e origem estão congelados nos manifests; sem reamostragem implícita.",
    "Pares com valores retidos e autocorrelação não são observações independentes; sem p-valor ou ranking automático.",
    "Defasagem 0 é descritiva; defasagem > 0 só é candidata preditiva com corte temporal 70/30 fixo, consistente "
    "fora da amostra, acima da faixa nula por deslocamento circular e da persistência do próprio alvo, ainda sem calibração, custos ou validação de estratégia.",
    "Intenções da estratégia não são fills, reversões confirmadas nem aceite operacional do robô.",
)


def _hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                    separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def formula_source_hash():
    # Identifica o código de cálculo desta exploração, sem criar uma identidade de execução/cache.
    root = Path(__file__).resolve().parents[1]
    paths = [*sorted((root / "strategies").glob("*.py")),
             root / "analytics" / "indicator_registry.py", root / "chart_study" / "catalog.py", Path(__file__)]
    return _hash({path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths})


def catalog():
    """Descobre todas as fórmulas/estratégias realmente registradas, sem banco/Tk/MT5."""
    features = indicator_registry.definitions()
    strategies = []
    for key, cls in REGISTRY.items():
        instance = get_strategy(key)
        strategies.append(dict(key=key, name=cls.info.name, params=instance.params,
            parameters={name: asdict(spec) for name, spec in STRATEGY_PARAMETERS[key].items()},
            warmup=instance.min_bars, required_columns=list(instance.required_columns),
            basis="candidate_signals_not_trades"))
    return dict(laboratory_version=VERSION, indicators=features, strategies=strategies,
                limits=dict(channels=MAX_CHANNELS, bars_per_timeframe=MAX_BARS, timeframes=MAX_TIMEFRAMES),
                views=["timeframes", "indicator_relations", "timeline", "lags", "coverage", "price_indicator"],
                limitations=list(LIMITATIONS))


def sources(db):
    result = []
    for item in db.data_inventory():
        tf = timeframes.parse(item["timeframe"])
        if tf is not None:
            result.append(dict(symbol=item["symbol"], timeframe=tf.name, rows=item["rows"],
                start=str(item["start"]) if item["start"] is not None else None,
                end=str(item["end"]) if item["end"] is not None else None))
    return sorted(result, key=lambda row: (row["symbol"], timeframes.parse(row["timeframe"]).minutes))


def capture_selection(db, *, symbol, timeframes_selected, start=None, end=None):
    """Congela somente uma seleção explícita pela API datasets existente."""
    tfs = [timeframes.parse(tf) for tf in timeframes_selected]
    if not tfs or len(tfs) > MAX_TIMEFRAMES or None in tfs or len(set(tfs)) != len(tfs):
        raise ValueError("Escolha de 1 a 8 timeframes distintos do inventário.")
    # Recusa antes de persistir um snapshot desnecessariamente grande. A captura
    # continua dona da leitura consistente; a análise reconfere o limite depois.
    for tf in tfs:
        frame = db.load_ohlcv(symbol, tf.label, start, end)
        if frame.empty:
            raise ValueError(f"{tf.name}: nenhuma barra de {symbol} na janela escolhida; amplie início/fim ou extraia este TF.")
        if len(frame) > MAX_BARS:
            suggested = pd.Timestamp(frame.time.iloc[-MAX_BARS]).isoformat()
            raise ValueError(f"{tf.name}: {len(frame)} barras na janela; o limite é {MAX_BARS}. "
                             f"Informe início a partir de {suggested} (últimas {MAX_BARS} barras) ou um fim anterior.")
    return [datasets.capture(db, symbol, tf.name, start, end) for tf in sorted(tfs, key=lambda tf: tf.minutes)]


def _utc(value):
    stamp = pd.Timestamp(value)
    if pd.isna(stamp) or stamp.tzinfo is None:
        raise ValueError("as_of exige data ISO com fuso explícito; exemplo: 2026-10-24T12:00:00Z.")
    return stamp.tz_convert("UTC")


def _bounds(bars, tf, offset):
    starts = pd.DatetimeIndex(pd.to_datetime(bars.time))
    if starts.hasnans or starts.tz is not None or not starts.is_monotonic_increasing or starts.has_duplicates:
        raise ValueError("Barras exigem relógio broker sem fuso, estritamente crescente e sem duplicatas.")
    ends = starts + (pd.offsets.MonthBegin(1) if tf.name == "MN1" else pd.Timedelta(minutes=tf.minutes))
    if len(starts) > 1 and (starts[1:] < ends[:-1]).any():
        raise ValueError("Barras sobrepostas para o timeframe declarado; revise a fonte antes de comparar.")
    return ((starts - pd.Timedelta(hours=offset)).tz_localize("UTC"),
            (ends - pd.Timedelta(hours=offset)).tz_localize("UTC"))


def _direction(values, close, rule):
    if rule == "price":
        difference = close - values
    elif rule == "midpoint_50":
        difference = values - 50
    elif rule == "midpoint_half":
        difference = values - .5
    elif rule == "zero":
        difference = values
    else:
        difference = values.diff()
    return np.sign(difference).where(np.isfinite(difference))


def _channels(snapshot, feature_ids, strategy, cutoff):
    manifest = snapshot.manifest
    tf = timeframes.parse(manifest["timeframe"])
    offset = manifest["time"].get("broker_utc_offset_hours")
    if manifest["time"].get("offset_state") != "declared" or type(offset) not in (int, float) or not np.isfinite(offset):
        raise ValueError("Snapshot sem offset broker conhecido; não é possível alinhar os relógios.")
    bars = snapshot.bars.reset_index(drop=True)
    if len(bars) > MAX_BARS:
        raise ValueError(f"Mais de {MAX_BARS} barras no snapshot; congele uma janela menor explicitamente.")
    starts, ends = _bounds(bars, tf, offset)
    # Nunca incluir uma barra cujo fechamento nominal ultrapasse o corte.
    keep = ends <= cutoff
    bars, starts, ends = bars.loc[keep].reset_index(drop=True), starts[keep], ends[keep]
    if bars.empty:
        raise ValueError(f"{tf.name}: nenhuma barra fechada no corte informado.")
    eligibility = snapshot.quality
    prefix_quality = data_eligibility.assess(bars, bar_minutes=tf.minutes)
    def eligibility_for(definition):
        required = data_eligibility.requirements(definition)
        check = data_eligibility.evaluate(eligibility, required)
        prefix = data_eligibility.evaluate(prefix_quality, required)
        if not prefix["eligible"]:
            check = dict(prefix, reasons=check["reasons"] + prefix["reasons"])
        return check
    result = []
    selected = list(feature_ids)
    computed = pd.DataFrame(indicator_registry.iter_core(bars, feature_ids=selected)) if selected else pd.DataFrame()
    for ident in selected:
        definition = indicator_registry.get_definition(ident)
        check = eligibility_for(definition)
        rows = computed.loc[computed.feature_id == ident].set_index("bar_index").reindex(bars.index)
        values = pd.to_numeric(rows.value, errors="coerce").where(rows.state == "AVAILABLE")
        if not check["eligible"]:
            values[:] = np.nan
        rule, legend = study_catalog.rule(definition)
        direction = _direction(values, pd.to_numeric(bars.close, errors="coerce"), rule)
        result.append(dict(id=f"{tf.name}:{ident}", feature=ident, name=definition["name"], timeframe=tf.name,
            parameters=definition["parameters"], unit=definition["unit"], rule=rule, legend=legend,
            algorithm_version=definition["algorithm_version"], values=values, direction=direction,
            source_end=ends, source_start=starts, eligibility=check,
            states=dict(Counter(rows.state)), warmup=definition["warmup_count"]))
    if strategy is not None:
        check = eligibility_for(dict(required_columns=strategy.required_columns, source_id="bars"))
        values = pd.Series(np.nan, index=bars.index, dtype=float)
        if check["eligible"]:
            signals = strategy.gerar_sinais(bars)
            values[:] = np.where(signals.entry_long & signals.entry_short, np.nan,
                        np.where(signals.entry_long, 1., np.where(signals.entry_short, -1., 0.)))
            values.iloc[:strategy.min_bars - 1] = np.nan
        result.append(dict(id=f"{tf.name}:strategy:{strategy.key}", feature="strategy:" + strategy.key,
            name=strategy.info.name, timeframe=tf.name, parameters=strategy.params, unit="candidate_-1_0_1",
            algorithm_version="current_source", rule="candidate", legend="Compra / neutro / venda; não são trades",
            values=values, direction=values, source_end=ends, source_start=starts, eligibility=check,
            states=dict(AVAILABLE=int(values.notna().sum()), UNAVAILABLE=int(values.isna().sum())),
            warmup=strategy.min_bars))
    return result, dict(manifest_id=snapshot.manifest_id, symbol=manifest["symbol"], timeframe=tf.name,
        source=manifest["source"], clock=manifest["time"], quality=eligibility,
        selected_rows=len(snapshot.bars), closed_rows=len(bars),
        first_open_utc=starts[0].isoformat(), last_close_utc=ends[-1].isoformat(),
        seed_basis="first_bar_of_selected_snapshot", nominal_closes=ends,
        prices=bars[["open", "high", "low", "close"]].astype(float).replace([np.inf, -np.inf], np.nan))


def _align(channel, grid):
    source = pd.DataFrame(dict(at=channel["source_end"], source_end=channel["source_end"],
        value=channel["values"].to_numpy(), direction=channel["direction"].to_numpy()))
    joined = pd.merge_asof(pd.DataFrame(dict(at=grid)), source, on="at", direction="backward")
    tf = timeframes.parse(channel["timeframe"])
    expiry = joined.source_end + (pd.offsets.MonthBegin(1) if tf.name == "MN1" else pd.Timedelta(minutes=tf.minutes))
    # Retenção só até o próximo fechamento nominal. Buraco de origem nunca vira linha contínua.
    valid = joined.source_end.notna() & (joined["at"] < expiry)
    joined.loc[~valid, ["value", "direction"]] = np.nan
    joined.loc[~valid, "source_end"] = pd.NaT
    return joined


def diagnose(channel, *, source_rows, source_available, available):
    """Motivo e ação para canal vazio/escasso; nunca deixar só área cinza."""
    check, warmup = channel["eligibility"], int(channel["warmup"] or 0)
    tf = channel["timeframe"]
    if not check["eligible"]:
        return dict(code="INELIGIBLE_DATA", message=f"{tf}: dados não atendem ao cálculo: "
                    + ("; ".join(check["reasons"]) or check["status"]),
                    action="Revise qualidade/colunas desta série no inventário ou escolha outro TF.")
    if source_available == 0 and source_rows < warmup:
        return dict(code="WARMUP_EXCEEDS_WINDOW", message=f"{tf}: {source_rows} barras fechadas; o cálculo exige "
                    f"aquecimento de {warmup}.", action=f"Antecipe o início para incluir ao menos {warmup} barras {tf} "
                    "ou remova este TF.")
    if source_available == 0:
        return dict(code="NO_VALUES", message=f"{tf}: o cálculo não produziu valores nas {source_rows} barras.",
                    action="Confira os detalhes/estados do canal; amplie a janela ou troque parâmetros.")
    if available == 0:
        return dict(code="NO_OVERLAP", message=f"{tf}: valores não coincidem com fechamentos da grade.",
                    action="Escolha TFs com sobreposição temporal ou outra janela.")
    return None


# LAB-07: corte cronológico fixo (não otimizado) para separar descrição de previsão.
FIT_FRACTION = 0.7
MIN_HOLDOUT_PAIRS = 30
MIN_HOLDOUT_EFFECT = 0.1


def _correlation(x, y):
    if len(x) < 3 or x.nunique() < 2 or y.nunique() < 2:
        return None
    return float(np.clip(x.corr(y), -1, 1))


def _null_band(x, y, min_shift):
    """Controle: |r| que deslocamentos circulares do preditor produzem por acaso (quantil 95%).

    O deslocamento preserva autocorrelação e retenção de cada série, ao contrário de
    embaralhar barras; todas as defasagens saem de uma correlação cruzada por FFT.
    """
    a, b = np.asarray(x, float), np.asarray(y, float)
    n = len(a)
    if n < 2 * min_shift + 10 or a.std() == 0 or b.std() == 0:
        return None
    a, b = (a - a.mean()) / a.std(), (b - b.mean()) / b.std()
    cross = np.fft.irfft(np.conj(np.fft.rfft(a)) * np.fft.rfft(b), n) / n
    shifts = cross[min_shift:n - min_shift]
    return float(np.quantile(np.abs(shifts), 0.95))


def _temporal_split(x, y, naive=None):
    """Correlação no trecho inicial (ajuste) e no final (fora da amostra), na ordem do tempo.

    `naive` é o próprio alvo com a mesma defasagem: o preditor só acrescenta algo se
    superar o que a persistência do alvo já antecipa.
    """
    cut = int(len(x) * FIT_FRACTION)
    fit, holdout = _correlation(x.iloc[:cut], y.iloc[:cut]), _correlation(x.iloc[cut:], y.iloc[cut:])
    n_holdout = len(x) - cut
    naive_holdout = None if naive is None else _correlation(naive.iloc[cut:].astype(float), y.iloc[cut:])
    # Deslocamento mínimo de 10% da amostra afasta a defasagem real e vizinhas retidas.
    band = _null_band(x.iloc[cut:], y.iloc[cut:], max(5, n_holdout // 10)) if n_holdout >= MIN_HOLDOUT_PAIRS else None
    if n_holdout < MIN_HOLDOUT_PAIRS or fit is None or holdout is None:
        verdict = "HOLDOUT_INSUFFICIENT"
    elif abs(fit) < MIN_HOLDOUT_EFFECT:
        verdict = "FIT_WEAK"  # sem relação no ajuste, não há o que confirmar fora
    elif np.sign(fit) != np.sign(holdout):
        verdict = "HOLDOUT_SIGN_FLIP"
    elif abs(holdout) < MIN_HOLDOUT_EFFECT:
        verdict = "HOLDOUT_WEAK"
    elif band is None or abs(holdout) <= band:
        verdict = "WITHIN_NULL"  # efeito do tamanho do acaso sob a mesma autocorrelação
    elif naive_holdout is not None and abs(holdout) <= abs(naive_holdout):
        verdict = "NO_GAIN_OVER_PERSISTENCE"
    else:
        verdict = "HOLDOUT_CONSISTENT"
    return dict(fit_fraction=FIT_FRACTION, fit_pairs=cut, fit_correlation=fit,
                holdout_pairs=n_holdout, holdout_correlation=holdout, null_band_95=band, naive_holdout_correlation=naive_holdout,
                control="circular_shift_of_predictor_in_holdout", verdict=verdict)


def _pair(left, right, lag):
    a, b = left.direction.shift(lag), right.direction
    valid = a.notna() & b.notna()
    x, y = a[valid].astype(float), b[valid].astype(float)
    n = len(x)
    unique = len(pd.DataFrame(dict(a=left.source_end.shift(lag)[valid], b=right.source_end[valid])).drop_duplicates())
    reason = "INSUFFICIENT_PAIRS" if n < 3 else "CONSTANT_SERIES" if x.nunique() < 2 or y.nunique() < 2 else None
    effect = None if reason else float(np.clip(x.corr(y), -1, 1))
    active = (x != 0) & (y != 0)
    return dict(lag_bars=lag, n_pairs=n, unique_source_pairs=unique, correlation=effect,
        left_source_bars=int(left.source_end.shift(lag)[valid].nunique()),
        right_source_bars=int(right.source_end[valid].nunique()),
        agreement=float((x == y).mean()) if n else None, active_pairs=int(active.sum()),
        active_agreement=float((x[active] == y[active]).mean()) if active.any() else None,
        state="UNAVAILABLE" if reason else "EXPLORATORY", reason=reason,
        # Defasagem 0 só descreve co-movimento; com defasagem o preditor antecede o alvo e
        # exige o corte temporal. Consistência fora da amostra não é previsão calibrada.
        relation="DESCRIPTIVE" if lag == 0 else "PREDICTIVE_CANDIDATE",
        out_of_sample=None if lag == 0 or reason else _temporal_split(x, y, right.direction.shift(lag)[valid]))


def analyze(snapshots, *, feature_ids=DEFAULT_FEATURES, strategy_key=None, strategy_params=None, as_of):
    """Mesmo relatório para SDK/CLI/Tk; não escreve base nem inicia backtests."""
    snapshots = list(snapshots)
    if not 1 <= len(snapshots) <= MAX_TIMEFRAMES:
        raise ValueError("Informe de 1 a 8 snapshots canônicos.")
    selected = list(feature_ids)
    if len(set(selected)) != len(selected):
        raise ValueError("Indicadores duplicados; selecione cada definição uma vez.")
    for ident in selected:
        indicator_registry.get_definition(ident)
    if not selected and strategy_key is None:
        raise ValueError("Escolha indicadores ou uma estratégia.")
    if (len(selected) + bool(strategy_key)) * len(snapshots) > MAX_CHANNELS:
        raise ValueError(f"Seleção excede {MAX_CHANNELS} canais; separe o estudo em recortes explícitos.")
    manifests = [item.manifest for item in snapshots]
    scopes = {(m["symbol"], m["source"]["namespace"], m["source"]["provider"], m["source"]["server"],
               m["time"].get("broker_utc_offset_hours")) for m in manifests}
    if len(scopes) != 1 or len({m["timeframe"] for m in manifests}) != len(manifests):
        raise ValueError("Compare o mesmo ativo/origem/relógio e somente um snapshot por timeframe.")
    cutoff = _utc(as_of)
    snapshots.sort(key=lambda item: timeframes.parse(item.manifest["timeframe"]).minutes)
    strategy = None
    if strategy_key is not None:
        # O schema existente valida limites/tipos e o construtor valida relações entre parâmetros.
        strategy = get_strategy(strategy_key, **(strategy_params or {}))
        base_composition(strategy_key, symbol=manifests[0]["symbol"], timeframe=manifests[0]["timeframe"],
                         params=strategy.params)
    elif strategy_params:
        raise ValueError("Parâmetros exigem estratégia explícita.")
    channels, populations = [], []
    for item in snapshots:
        built, population = _channels(item, selected, strategy, cutoff)
        channels.extend(built)
        populations.append(population)
    grid = populations[0]["nominal_closes"]
    aligned = [_align(channel, grid) for channel in channels]
    pairs = [dict(left=left["id"], right=right["id"], **_pair(aligned[i], aligned[j], lag))
             for i, left in enumerate(channels) for j, right in enumerate(channels)
             for lag in ((0,) if i == j else (0, 1, 3) if j > i else (1, 3))]
    prices = [[float(value) if pd.notna(value) else None for value in row]
              for row in populations[0]["prices"].itertuples(index=False, name=None)]
    for population in populations:
        population.pop("nominal_closes")
        population.pop("prices")
    output_channels = []
    for channel, frame in zip(channels, aligned):
        summary = {key: value for key, value in channel.items() if key not in
                   {"values", "direction", "source_start", "source_end"}}
        summary.update(available=int(frame.value.notna().sum()), total=len(frame),
            source_rows=len(channel["values"]), source_available=int(channel["values"].notna().sum()),
            candidate_counts=dict(long=int(channel["values"].eq(1).sum()), short=int(channel["values"].eq(-1).sum()))
                if channel["rule"] == "candidate" else None,
            coverage=float(frame.value.notna().mean()),
            values=[float(value) if pd.notna(value) and np.isfinite(value) else None for value in frame.value],
            directions=[int(value) if pd.notna(value) else None for value in frame.direction],
            source_ends=[value.isoformat() if pd.notna(value) else None for value in frame.source_end])
        summary["diagnosis"] = diagnose(channel, source_rows=summary["source_rows"],
            source_available=summary["source_available"], available=summary["available"])
        output_channels.append(summary)
    selection = dict(manifests=[item.manifest_id for item in snapshots], feature_ids=selected,
        strategy=dict(key=strategy.key, params=strategy.params) if strategy else None, as_of=cutoff.isoformat())
    source_hash = formula_source_hash()
    grid_tf = timeframes.parse(populations[0]["timeframe"])
    expected = grid[:-1] + (pd.offsets.MonthBegin(1) if grid_tf.name == "MN1" else pd.Timedelta(minutes=grid_tf.minutes))
    gap_count = int((grid[1:] != expected).sum())
    return dict(laboratory_version=VERSION, analysis_id=_hash(dict(version=VERSION, source_hash=source_hash, **selection)),
        grid_gap_count=gap_count,
        formula_source_sha256=source_hash, price_columns=["open", "high", "low", "close"], prices=prices,
        selection=selection, symbol=manifests[0]["symbol"], grid_timeframe=populations[0]["timeframe"],
        times=[stamp.isoformat() for stamp in grid], channels=output_channels, pairs=pairs,
        populations=populations, alignment="nominal_closed_bar_backward_join_expiry_one_source_period",
        qualification=dict(exploratory=True, historical_availability_proven=False,
                           independent_observations=False, operational_acceptance=False),
        limitations=list(LIMITATIONS))
