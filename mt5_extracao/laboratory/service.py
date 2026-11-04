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
    "Pivôs (fractais/ZigZag) entram só na barra de confirmação, nunca na origem; a saída provisória é desenho, "
    "fica fora das relações e o atraso origem→confirmação fica registrado por canal.",
    "Barras que retêm um pivô não são novos eventos. Contagens de eventos nos pares são distintas das barras; "
    "o corte 70/30 e seus controles continuam por barras, sem validação estatística por eventos independentes.",
    "Intenções da estratégia não são fills, reversões confirmadas nem aceite operacional do robô.",
    "Estratégias comparadas no mesmo ativo são intenções do gerador no preço congelado; operação, execução, "
    "custos e PnL continuam ausentes. Ativo, origem ou relógio diferentes são recusados, não misturados.",
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


CLOCK_UNKNOWN = (
    "Fuso do servidor não declarado na configuração ([APP] broker_utc_offset/broker_timezone vazios); sem ele "
    "não se sabe quando cada barra fechou em UTC. Ação: declare broker_utc_offset somente se o servidor não "
    "muda de horário dentro da janela analisada; servidor com horário de verão exige broker_timezone com "
    "time_basis = utc, que a captura de snapshots ainda não aceita. Sua seleção foi mantida.")


def capture_selection(db, *, symbol, timeframes_selected, start=None, end=None, stored_features=()):
    """Congela somente uma seleção explícita pela API datasets existente."""
    tfs = [timeframes.parse(tf) for tf in timeframes_selected]
    if not tfs or len(tfs) > MAX_TIMEFRAMES or None in tfs or len(set(tfs)) != len(tfs):
        raise ValueError("Escolha de 1 a 8 timeframes distintos do inventário.")
    clock = db.time_context() if hasattr(db, "time_context") else {"offset_state": "declared"}
    if clock["offset_state"] == "unknown":
        # Recusa antes de congelar: sem relógio do servidor não há fechamento UTC nem corte as-of.
        raise ValueError(CLOCK_UNKNOWN)
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
    return [datasets.capture(db, symbol, tf.name, start, end, stored_features=stored_features)
            for tf in sorted(tfs, key=lambda tf: tf.minutes)]


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


def _events(rows, definition, starts=None, ends=None):
    """Eventos de pivô: valor só existe a partir da confirmação; provisório nunca vira relação."""
    if definition["parameters"]["output"] == "provisional":
        return dict(basis="provisional_drawing", usable=False, confirmations=0, delay_bars=None,
                    bars_since_last_confirmation=None)
    confirmed = [(position, meta) for position, meta in enumerate(rows.pivot_metadata)
                 if isinstance(meta, dict) and meta.get("new_confirmation")]
    delays = [meta["pivot_confirmation_position"] - meta["pivot_origin_position"] for _, meta in confirmed]
    latest = None
    if confirmed and starts is not None and ends is not None:
        _, meta = confirmed[-1]
        origin, confirmation = int(meta["pivot_origin_position"]), int(meta["pivot_confirmation_position"])
        latest = dict(origin_bar_open_utc=starts[origin].isoformat(),
            confirmation_bar_open_utc=starts[confirmation].isoformat(),
            available_at_nominal_utc=ends[confirmation].isoformat(), historical_receipt_proven=False)
    return dict(basis="confirmation_bar_close", usable=True, confirmations=len(confirmed), latest=latest,
        delay_bars=dict(min=min(delays), median=float(np.median(delays)), max=max(delays)) if delays else None,
        bars_since_last_confirmation=len(rows) - 1 - confirmed[-1][0] if confirmed else None)


def _event_times(rows, ends):
    """Identidade do evento retido: fechamento nominal da confirmação, nunca barra corrente/origem."""
    return pd.to_datetime([ends[int(meta["pivot_confirmation_position"])]
        if isinstance(meta, dict) and meta.get("pivot_confirmation_position") is not None else pd.NaT
        for meta in rows.pivot_metadata], utc=True)


def _channels(snapshot, feature_ids, strategies, cutoff):
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
        events = _events(rows, definition, starts, ends) if ident.startswith("structure.") else None
        direction = _direction(values, pd.to_numeric(bars.close, errors="coerce"), rule)
        result.append(dict(id=f"{tf.name}:{ident}", feature=ident, name=definition["name"], timeframe=tf.name,
            parameters=definition["parameters"], unit=definition["unit"], rule=rule, legend=legend,
            algorithm_version=definition["algorithm_version"], values=values, direction=direction,
            source_end=ends, source_start=starts, eligibility=check,
            states=dict(Counter(rows.state)), warmup=definition["warmup_count"], events=events))
        if events is not None:
            result[-1]["event_at"] = _event_times(rows, ends)
    for strategy in strategies:
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
            warmup=strategy.min_bars, events=None))
    return result, dict(manifest_id=snapshot.manifest_id, symbol=manifest["symbol"], timeframe=tf.name,
        source=manifest["source"], clock=manifest["time"], quality=eligibility,
        selected_rows=len(snapshot.bars), closed_rows=len(bars),
        first_open_utc=starts[0].isoformat(), last_close_utc=ends[-1].isoformat(),
        seed_basis="first_bar_of_selected_snapshot", nominal_closes=ends,
        prices=bars[["open", "high", "low", "close"]].astype(float).replace([np.inf, -np.inf], np.nan))


def _align(channel, grid):
    source = pd.DataFrame(dict(at=channel["source_end"], source_end=channel["source_end"],
        value=channel["values"].to_numpy(), direction=channel["direction"].to_numpy()))
    if "event_at" in channel:
        source["event_at"] = channel["event_at"]
    joined = pd.merge_asof(pd.DataFrame(dict(at=grid)), source, on="at", direction="backward")
    tf = timeframes.parse(channel["timeframe"])
    expiry = joined.source_end + (pd.offsets.MonthBegin(1) if tf.name == "MN1" else pd.Timedelta(minutes=tf.minutes))
    # Retenção só até o próximo fechamento nominal. Buraco de origem nunca vira linha contínua.
    valid = joined.source_end.notna() & (joined["at"] < expiry)
    joined.loc[~valid, ["value", "direction"]] = np.nan
    joined.loc[~valid, "source_end"] = pd.NaT
    if "event_at" in joined:
        joined.loc[~valid, "event_at"] = pd.NaT
    return joined


def diagnose(channel, *, source_rows, source_available, available):
    """Motivo e ação para canal vazio/escasso; nunca deixar só área cinza."""
    check, warmup = channel["eligibility"], int(channel["warmup"] or 0)
    tf = channel["timeframe"]
    if channel.get("events") and not channel["events"]["usable"]:
        return dict(code="UNCONFIRMED_EVENT", message=f"{tf}: saída provisória de pivô é só desenho; "
                    "não confirmada, não entra em relações.", action="Use a saída top/bottom confirmada deste pivô.")
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
PREDICTIVE_LAGS = (1, 3, 6)   # barras da grade; horizonte fixo, nunca escolhido pelo melhor efeito
BOOTSTRAP_RESAMPLES = 200
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


def _block_ci(x, y):
    """IC 95% de r por bootstrap de blocos móveis: blocos preservam a dependência serial."""
    a, b = np.asarray(x, float), np.asarray(y, float)
    n = len(a)
    block = max(5, n // 20)
    if n < 2 * block:
        return None
    rng = np.random.default_rng(0)  # semente fixa: mesmo relatório para a mesma seleção
    starts = rng.integers(0, n - block + 1, size=(BOOTSTRAP_RESAMPLES, -(-n // block)))
    index = (starts[:, :, None] + np.arange(block)).reshape(BOOTSTRAP_RESAMPLES, -1)[:, :n]
    sa, sb = a[index], b[index]
    sa, sb = sa - sa.mean(1, keepdims=True), sb - sb.mean(1, keepdims=True)
    denominator = np.sqrt((sa ** 2).sum(1) * (sb ** 2).sum(1))
    valid = denominator > 0
    if valid.sum() < BOOTSTRAP_RESAMPLES * .9:
        return None
    r = (sa * sb).sum(1)[valid] / denominator[valid]
    low, high = np.quantile(r, [.025, .975])
    return [float(low), float(high)]


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
    # IC só onde ainda decide algo: amostra curta ou ajuste fraco não chegam à confirmação.
    interval = None if verdict in ("HOLDOUT_INSUFFICIENT", "FIT_WEAK") else _block_ci(x.iloc[cut:], y.iloc[cut:])
    if verdict == "HOLDOUT_CONSISTENT" and (interval is None or interval[0] <= 0 <= interval[1]):
        verdict = "CI_INCLUDES_ZERO"
    return dict(fit_fraction=FIT_FRACTION, fit_pairs=cut, fit_correlation=fit,
                holdout_pairs=n_holdout, holdout_correlation=holdout, null_band_95=band, naive_holdout_correlation=naive_holdout,
                holdout_ci_95=interval,
                control="circular_shift_of_predictor_in_holdout", verdict=verdict)


def _pair(left, right, lag, unconfirmed=False):
    a, b = left.direction.shift(lag), right.direction
    valid = a.notna() & b.notna()
    x, y = a[valid].astype(float), b[valid].astype(float)
    n = len(x)
    unique = len(pd.DataFrame(dict(a=left.source_end.shift(lag)[valid], b=right.source_end[valid])).drop_duplicates())
    reason = ("UNCONFIRMED_EVENT" if unconfirmed else "INSUFFICIENT_PAIRS" if n < 3
              else "CONSTANT_SERIES" if x.nunique() < 2 or y.nunique() < 2 else None)
    effect = None if reason else float(np.clip(x.corr(y), -1, 1))
    active = (x != 0) & (y != 0)
    event_sample = _event_sample(left, right, lag, valid)
    split = None if lag == 0 or reason else _temporal_split(x, y, right.direction.shift(lag)[valid])
    if split is not None and event_sample is not None:
        split["event_sample"] = _event_sample(left, right, lag, valid, holdout=True)
    return dict(lag_bars=lag, n_pairs=n, unique_source_pairs=unique, correlation=effect,
        event_sample=event_sample,
        left_source_bars=int(left.source_end.shift(lag)[valid].nunique()),
        right_source_bars=int(right.source_end[valid].nunique()),
        agreement=float((x == y).mean()) if n else None, active_pairs=int(active.sum()),
        active_agreement=float((x[active] == y[active]).mean()) if active.any() else None,
        state="UNAVAILABLE" if reason else "EXPLORATORY", reason=reason,
        # Defasagem 0 só descreve co-movimento; com defasagem o preditor antecede o alvo e
        # exige o corte temporal. Consistência fora da amostra não é previsão calibrada.
        relation="DESCRIPTIVE" if lag == 0 else "PREDICTIVE_CANDIDATE",
        out_of_sample=split)


def _event_sample(left, right, lag, valid, *, holdout=False):
    """Conta apenas eventos representados nos pares válidos, inclusive depois do corte temporal."""
    if "event_at" not in left and "event_at" not in right:
        return None
    events = {}
    for side, frame, shift in (("left", left, lag), ("right", right, 0)):
        if "event_at" in frame:
            values = frame.event_at.shift(shift)[valid]
            events[side] = values.iloc[int(len(values) * FIT_FRACTION):] if holdout else values
    return dict(left_events=int(events["left"].nunique()) if "left" in events else None,
        right_events=int(events["right"].nunique()) if "right" in events else None,
        unique_event_pairs=int(pd.DataFrame(events).dropna().drop_duplicates().shape[0]) if len(events) == 2 else None,
        basis="distinct_confirmation_closes_in_valid_pairs", independent_events_proven=False)


def _report_events(channel, count):
    values = channel.get("event_confirmation_ends")
    return (pd.DataFrame(dict(event_at=pd.to_datetime(values, utc=True))) if values is not None
            else pd.DataFrame(index=range(count)))


def _resolve_strategies(strategy_key, strategy_params, strategies, symbol, timeframe):
    """Uma estratégia preserva o relatório antigo. A lista compara intenções, sem operação nem execução."""
    if strategies is not None:
        if strategy_key is not None or strategy_params:
            raise ValueError("Informe uma estratégia com parâmetros ou a lista de comparação, não as duas.")
        resolved, seen = [], []
        for item in strategies:
            if isinstance(item, str):
                key, params = item, {}
            elif isinstance(item, dict) and isinstance(item.get("key"), str) and (
                    item.get("params") is None or isinstance(item.get("params"), dict)):
                key, params = item["key"], item.get("params") or {}
            else:
                raise ValueError("Cada estratégia da comparação precisa de uma chave registrada.")
            if key in seen:
                raise ValueError(f"Estratégia repetida na comparação: {key}.")
            seen.append(key)
            strategy = get_strategy(key, **params)
            base_composition(key, symbol=symbol, timeframe=timeframe, params=strategy.params)
            resolved.append(strategy)
        return resolved, True
    if strategy_key is not None:
        strategy = get_strategy(strategy_key, **(strategy_params or {}))
        base_composition(strategy_key, symbol=symbol, timeframe=timeframe, params=strategy.params)
        return [strategy], False
    if strategy_params:
        raise ValueError("Parâmetros exigem estratégia explícita.")
    return [], False


def analyze(snapshots, *, feature_ids=DEFAULT_FEATURES, strategy_key=None, strategy_params=None,
            strategies=None, as_of):
    """Mesmo relatório para SDK/CLI/Tk; não escreve base nem inicia backtests."""
    snapshots = list(snapshots)
    if not 1 <= len(snapshots) <= MAX_TIMEFRAMES:
        raise ValueError("Informe de 1 a 8 snapshots canônicos.")
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
    if (len(selected) + n_strategies) * len(snapshots) > MAX_CHANNELS:
        raise ValueError(f"Seleção excede {MAX_CHANNELS} canais; separe o estudo em recortes explícitos.")
    manifests = [item.manifest for item in snapshots]
    scopes = {(m["symbol"], m["source"]["namespace"], m["source"]["provider"], m["source"]["server"],
               m["time"].get("broker_utc_offset_hours")) for m in manifests}
    if len(scopes) != 1 or len({m["timeframe"] for m in manifests}) != len(manifests):
        raise ValueError("Compare o mesmo ativo/origem/relógio e somente um snapshot por timeframe.")
    cutoff = _utc(as_of)
    snapshots.sort(key=lambda item: timeframes.parse(item.manifest["timeframe"]).minutes)
    # O schema existente valida limites/tipos e o construtor valida relações entre parâmetros.
    resolved, listed = _resolve_strategies(strategy_key, strategy_params, strategies,
                                           manifests[0]["symbol"], manifests[0]["timeframe"])
    channels, populations = [], []
    for item in snapshots:
        built, population = _channels(item, selected, resolved, cutoff)
        channels.extend(built)
        populations.append(population)
    grid = populations[0]["nominal_closes"]
    aligned = [_align(channel, grid) for channel in channels]
    unconfirmed = [bool(channel["events"] and not channel["events"]["usable"]) for channel in channels]
    pairs = [dict(left=left["id"], right=right["id"],
                  **_pair(aligned[i], aligned[j], lag, unconfirmed[i] or unconfirmed[j]))
             for i, left in enumerate(channels) for j, right in enumerate(channels)
             for lag in ((0,) if i == j else (0, *PREDICTIVE_LAGS) if j > i else PREDICTIVE_LAGS)]
    prices = [[float(value) if pd.notna(value) else None for value in row]
              for row in populations[0]["prices"].itertuples(index=False, name=None)]
    for population in populations:
        population.pop("nominal_closes")
        population.pop("prices")
    output_channels = []
    for channel, frame in zip(channels, aligned):
        summary = {key: value for key, value in channel.items() if key not in
                   {"values", "direction", "source_start", "source_end", "event_at"}}
        summary.update(available=int(frame.value.notna().sum()), total=len(frame),
            source_rows=len(channel["values"]), source_available=int(channel["values"].notna().sum()),
            candidate_counts=dict(long=int(channel["values"].eq(1).sum()), short=int(channel["values"].eq(-1).sum()))
                if channel["rule"] == "candidate" else None,
            coverage=float(frame.value.notna().mean()),
            values=[float(value) if pd.notna(value) and np.isfinite(value) else None for value in frame.value],
            directions=[int(value) if pd.notna(value) else None for value in frame.direction],
            source_ends=[value.isoformat() if pd.notna(value) else None for value in frame.source_end])
        if "event_at" in frame:
            summary["event_confirmation_ends"] = [value.isoformat() if pd.notna(value) else None for value in frame.event_at]
        summary["diagnosis"] = diagnose(channel, source_rows=summary["source_rows"],
            source_available=summary["source_available"], available=summary["available"])
        output_channels.append(summary)
    selection = dict(manifests=[item.manifest_id for item in snapshots], feature_ids=selected,
        strategy=dict(key=resolved[0].key, params=resolved[0].params) if resolved and not listed else None,
        as_of=cutoff.isoformat())
    if listed:
        selection["strategies"] = [dict(key=item.key, params=item.params, role="intention") for item in resolved]
        selection["comparison"] = dict(
            scope="mesmo ativo, origem, relógio e corte",
            intention="sinais do gerador; não são trades, fills nem R",
            operation="operação ausente neste relatório",
            execution="execução ausente neste relatório",
            costs="custos não aplicados; a curva não é PnL")
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


_REPORT_KEYS = ("analysis_id", "selection", "symbol", "grid_timeframe", "times", "channels", "pairs", "populations",
                "formula_source_sha256", "qualification")


def load_report(text):
    """Reabre uma análise exportada sem recalcular: identidade e valores são os congelados."""
    try:
        report = json.loads(text)
    except ValueError as error:
        raise ValueError(f"Arquivo não é uma análise JSON do Laboratório: {error}") from error
    if not isinstance(report, dict) or report.get("laboratory_version") != VERSION:
        raise ValueError(f"Análise incompatível: exige laboratory_version {VERSION}.")
    missing = [key for key in _REPORT_KEYS if key not in report]
    if missing:
        raise ValueError("Análise incompleta; faltam: " + ", ".join(missing))
    expected = _hash(dict(version=VERSION, source_hash=report["formula_source_sha256"], **report["selection"]))
    if expected != report["analysis_id"]:
        raise ValueError("analysis_id não corresponde à seleção e às fórmulas declaradas; arquivo alterado.")
    return report


MATRIX_REASONS = {
    "UNCONFIRMED_EVENT": "Saída provisória de pivô: desenho não confirmado, fora das relações.",
    "SAME_SERIES": "Mesma série com defasagem: é persistência do próprio canal, não relação entre indicadores.",
    "NO_OVERLAP": "Sem fechamentos comuns com valor nos dois canais (warmup, lacuna ou TF sem sobreposição).",
    "INSUFFICIENT_PAIRS": "Menos de 3 pares alinhados; nenhum efeito é estimado.",
    "CONSTANT_SERIES": "Um dos lados não varia nos pares alinhados; correlação indefinida (não é zero).",
}


def relation_matrix(report, *, lag=0):
    """Matriz indicador × indicador derivada do relatório congelado (mesma função para SDK/CLI/UI).

    Efeito = Spearman dos valores alinhados na grade (preditor deslocado ``lag`` barras); a correlação
    de direção, o par e o veredito fora da amostra vêm do próprio relatório. Nada é recalculado na UI.
    """
    if lag not in (0, *PREDICTIVE_LAGS):
        raise ValueError(f"Defasagem {lag} fora das fixas do relatório: {(0, *PREDICTIVE_LAGS)}.")
    channels = report["channels"]
    pairs = {(p["left"], p["right"], p["lag_bars"]): p for p in report["pairs"]}
    values = {c["id"]: pd.Series([np.nan if v is None else v for v in c["values"]], dtype=float) for c in channels}
    event_frames = {c["id"]: _report_events(c, len(values[c["id"]])) for c in channels}
    blocked = {c["id"]: ("UNCONFIRMED_EVENT" if (c.get("events") or {}).get("usable") is False
                         else (c.get("diagnosis") or {}).get("code")) for c in channels}
    cells = []
    for left in channels:
        row = []
        for right in channels:
            a, b = left["id"], right["id"]
            pair = pairs.get((a, b, lag)) or (pairs.get((b, a, 0)) if lag == 0 else None)
            x, y = values[a].shift(lag), values[b]
            valid = x.notna() & y.notna()
            reason = ("UNCONFIRMED_EVENT" if "UNCONFIRMED_EVENT" in (blocked[a], blocked[b])
                      else "SAME_SERIES" if a == b and lag else None)
            if reason is None:
                # Canal vazio herda o diagnóstico (warmup, inelegível…), distinto de falta de sobreposição.
                reason = (blocked[a] or blocked[b] or "NO_OVERLAP" if not valid.any()
                          else "INSUFFICIENT_PAIRS" if valid.sum() < 3
                          else "CONSTANT_SERIES" if x[valid].nunique() < 2 or y[valid].nunique() < 2 else None)
            effect = None if reason else float(np.clip(x[valid].corr(y[valid], method="spearman"), -1, 1))
            oos = (pair or {}).get("out_of_sample") or {}
            row.append(dict(left=a, right=b, lag_bars=lag, effect=effect, n_pairs=int(valid.sum()),
                unique_source_pairs=(pair or {}).get("unique_source_pairs"),
                event_sample=_event_sample(event_frames[a], event_frames[b], lag, valid),
                direction_correlation=None if reason else (pair or {}).get("correlation"),
                relation="DESCRIPTIVE" if lag == 0 else "PREDICTIVE_CANDIDATE",
                verdict=oos.get("verdict"), holdout_ci_95=oos.get("holdout_ci_95"),
                state="UNAVAILABLE" if reason else "EXPLORATORY", reason=reason,
                reason_text=MATRIX_REASONS.get(reason) or next((c["diagnosis"]["message"] for c in (left, right)
                                                                if c.get("diagnosis")), None)))
        cells.append(row)
    return dict(analysis_id=report["analysis_id"], symbol=report["symbol"], grid_timeframe=report["grid_timeframe"],
        as_of=report["selection"]["as_of"], lag_bars=lag, method="spearman_values_on_nominal_grid",
        source=[dict(manifest_id=p["manifest_id"], timeframe=p["timeframe"], source=p["source"],
                     first_open_utc=p["first_open_utc"], last_close_utc=p["last_close_utc"]) for p in report["populations"]],
        formula_source_sha256=report["formula_source_sha256"],
        channels=[dict(id=c["id"], name=c["name"], timeframe=c["timeframe"], unit=c["unit"],
                       parameters=c["parameters"], algorithm_version=c["algorithm_version"],
                       coverage=c["coverage"], available=c["available"], total=c["total"],
                       blocked=blocked[c["id"]], events=c.get("events")) for c in channels],
        cells=cells, qualification=report["qualification"],
        limitations=["Correlação de valores no fechamento nominal; não comprova disponibilidade histórica nem lucro.",
                     "O alvo de retorno do preço é target_matrix, horizonte fixo, sobre estes mesmos preços; não é PnL nem o produtor T18.6."])


def _numeric(values):
    return pd.Series([np.nan if value is None else value for value in values], dtype=float)


def _closes(report):
    prices = report.get("prices")
    if not prices:
        raise ValueError("Análise sem preços congelados; não há retorno para cruzar.")
    closes = []
    for row in prices:
        if not row or len(row) < 4 or row[3] is None:
            closes.append(np.nan)
        else:
            closes.append(float(row[3]))
    series = pd.Series(closes, dtype=float)
    if len(series) != len(report["times"]):
        raise ValueError("Preços e grade temporal com tamanhos diferentes.")
    return series


def forward_close_return(closes, horizon):
    """Retorno bruto do fechamento `horizon` barras à frente. A barra sem futuro fica ausente, nunca zero."""
    if horizon not in PREDICTIVE_LAGS:
        raise ValueError(f"Horizonte {horizon} fora dos fixos {PREDICTIVE_LAGS}.")
    future = closes.shift(-horizon)
    base = closes.where(closes > 0)
    out = future / base - 1.0
    return out.where(np.isfinite(out))


def _rank_effect(x, y):
    """Spearman só onde os dois lados existem. Constante não vira correlação zero."""
    valid = x.notna() & y.notna() & np.isfinite(x) & np.isfinite(y)
    n = int(valid.sum())
    left, right = x[valid], y[valid]
    if n < 3:
        return n, None, "INSUFFICIENT_PAIRS", left, right
    if left.nunique() < 2 or right.nunique() < 2:
        return n, None, "CONSTANT_SERIES", left, right
    effect = float(left.corr(right, method="spearman"))
    if not np.isfinite(effect):
        return n, None, "CONSTANT_SERIES", left, right
    return n, float(np.clip(effect, -1, 1)), None, left, right


def _channel_block(channel):
    events = channel.get("events") or {}
    if events.get("usable") is False:
        return "UNCONFIRMED_EVENT"
    return (channel.get("diagnosis") or {}).get("code")


def target_matrix(report, *, horizon=1):
    """Indicador × retorno bruto do preço no horizonte fixo. Não é trade, R nem produtor T18.6."""
    closes = _closes(report)
    target = forward_close_return(closes, horizon)
    target_id = f"price.forward_return:{horizon}"
    cells = []
    for channel in report["channels"]:
        blocked = _channel_block(channel)
        n, effect, reason, xs, ys = _rank_effect(_numeric(channel["values"]), target)
        if blocked:
            reason, effect = blocked, None
        oos = None if reason else _temporal_split(xs.reset_index(drop=True), ys.reset_index(drop=True))
        valid = _numeric(channel["values"]).notna() & target.notna()
        event_sample = _event_sample(_report_events(channel, len(target)), pd.DataFrame(index=target.index), 0, valid)
        cells.append(dict(left=channel["id"], right=target_id, lag_bars=horizon, effect=effect, n_pairs=n,
            event_sample=event_sample,
            unique_source_pairs=n, direction_correlation=None,
            relation="PREDICTIVE_CANDIDATE", verdict=(oos or {}).get("verdict"),
            holdout_ci_95=(oos or {}).get("holdout_ci_95"),
            state="UNAVAILABLE" if reason else "EXPLORATORY", reason=reason,
            reason_text=MATRIX_REASONS.get(reason) or (
                f"Spearman do valor contra o retorno do fechamento {horizon} barras à frente. "
                "Não é resultado de operação.")))
    return dict(analysis_id=report["analysis_id"], symbol=report["symbol"],
        grid_timeframe=report["grid_timeframe"], as_of=report["selection"]["as_of"], lag_bars=horizon,
        method="spearman_value_vs_forward_close_return", target=target_id,
        formula_source_sha256=report["formula_source_sha256"],
        channels=[dict(id=c["id"], name=c["name"], timeframe=c["timeframe"]) for c in report["channels"]],
        source=[dict(first_open_utc=report["times"][0], last_close_utc=report["times"][-1])],
        cells=[cells], qualification=report.get("qualification"),
        limitations=["Retorno bruto do preço; sem custo, slippage ou fill.",
                     "A última barra do horizonte não tem alvo e fica de fora, não entra como zero.",
                     "Consistência fora da amostra não é previsão calibrada nem preset."])


def stability_windows(report, left_id, right_id, *, lag=0, blocks=4):
    """Quatro blocos cronológicos iguais. Bloco sem pares fica ausente; não é interpolado nem zerado."""
    if blocks < 2:
        raise ValueError("A linha do tempo precisa de ao menos dois blocos.")
    channels = {c["id"]: c for c in report["channels"]}
    if left_id not in channels or right_id not in channels:
        raise ValueError("Canal ausente na análise congelada.")
    times = pd.to_datetime(report["times"])
    x = _numeric(channels[left_id]["values"]).shift(lag)
    y = _numeric(channels[right_id]["values"])
    if len(x) != len(times) or len(y) != len(times):
        raise ValueError("Canal e grade temporal com tamanhos diferentes.")
    edges = np.linspace(0, len(times), blocks + 1).astype(int)
    cut = int(len(times) * FIT_FRACTION)
    windows = []
    for start, end in zip(edges[:-1], edges[1:]):
        n, effect, reason, _xs, _ys = _rank_effect(x.iloc[start:end], y.iloc[start:end])
        opening, closing = times[start], times[max(start, end - 1)]
        stamp = "%Y-%m-%d %H:%M"
        windows.append(dict(
            start=opening.strftime(stamp), end=closing.strftime(stamp),
            center=times[(start + max(start, end - 1)) // 2].strftime(stamp),
            segment="Ajuste" if end <= cut else "Fora da amostra",
            value=effect, value_text="—" if effect is None else f"{effect:+.2f}".replace(".", ","),
            ci="IC da janela não é recalculado; o corte 70/30 do par está no relatório.",
            n=f"n={n}", n_pairs=n, reason=reason,
            utility_state="INCONCLUSIVE",
            evidence_state="EXPLORATORY" if effect is not None else "INSUFFICIENT",
            freshness_state="CURRENT", presence_state="UNKNOWN",
            support_state="bloco cronológico fixo",
            reason_text=MATRIX_REASONS.get(reason, "Spearman no bloco; ausência não é correlação zero.")))
    return dict(left=left_id, right=right_id, lag_bars=lag, blocks=blocks, windows=windows,
                method="spearman_equal_chronological_blocks")


def direction_outcomes(report, channel_id, *, horizon=1):
    """Conta o sinal do retorno futuro quando a intenção do canal é compra ou venda.

    As três faixas somam o denominador. Não há trade, R nem custo.
    """
    channel = next((c for c in report["channels"] if c["id"] == channel_id), None)
    if channel is None:
        raise ValueError("Canal ausente na análise congelada.")
    directions = _numeric(channel["directions"])
    returns = forward_close_return(_closes(report), horizon)
    if len(directions) != len(returns):
        raise ValueError("Intenções e preços com tamanhos diferentes.")
    rows = []
    for sign, label in ((1.0, "Intenção de compra"), (-1.0, "Intenção de venda")):
        chosen = returns[directions.eq(sign) & returns.notna()]
        gains, losses = int((chosen > 0).sum()), int((chosen < 0).sum())
        flats = int((chosen == 0).sum())
        total = int(len(chosen))
        if gains + losses + flats != total:
            raise RuntimeError("As faixas de retorno não fecham com o denominador.")
        rows.append(dict(label=label, sign=int(sign), wins=gains, losses=losses, flats=flats,
            censored=0, n_total=total, n_mature=total, presence_state="UNKNOWN", freshness_state="CURRENT",
            utility_state="INCONCLUSIVE", evidence_state="EXPLORATORY" if total else "INSUFFICIENT",
            reason="Retorno bruto do fechamento futuro condicionado à intenção. Não é operação."))
    return dict(channel_id=channel_id, horizon_bars=horizon, directions=rows,
        population=f"{report['symbol']} {report['grid_timeframe']} · retorno bruto em {horizon} barras",
        cost_status="custos não aplicados; expectativa líquida de trades não é calculada",
        unit="contagem de fechamentos; ganho = retorno > 0, perda = retorno < 0, empate = retorno 0")


def holdout_comparison(report):
    """ρ fora da amostra já gravado no par, menos a persistência do alvo. A UI não recalcula."""
    rows = []
    for pair in report.get("pairs") or []:
        oos = pair.get("out_of_sample") or {}
        if pair.get("relation") != "PREDICTIVE_CANDIDATE" or not oos:
            continue
        effect, naive = oos.get("holdout_correlation"), oos.get("naive_holdout_correlation")
        delta = None if effect is None or naive is None else float(effect) - float(naive)
        ci = oos.get("holdout_ci_95")
        rows.append(dict(
            label=f"{pair['left']} → {pair['right']} +{pair['lag_bars']}",
            effect=None if effect is None else float(effect),
            effect_text="—" if effect is None else f"{effect:+.2f}".replace(".", ","),
            ci=("IC não calculado" if not ci else f"IC 95%: {ci[0]:+.2f} a {ci[1]:+.2f}".replace(".", ",")),
            n=f"n={oos.get('holdout_pairs')}", delta=delta,
            delta_text="—" if delta is None else f"{delta:+.2f}".replace(".", ","),
            verdict=oos.get("verdict") or "—",
            utility_state="INCONCLUSIVE", evidence_state="EXPLORATORY", freshness_state="CURRENT",
            presence_state="UNKNOWN", support_state="corte 70/30 do serviço",
            reason="Diferença de ρ contra a persistência do alvo. Não é R por operação."))
    return rows


def research_candidates(report):
    """Pares que o próprio relatório marcou consistentes fora da amostra. A tela não os reclassifica."""
    rows = []
    for pair in report.get("pairs") or []:
        verdict = (pair.get("out_of_sample") or {}).get("verdict")
        if verdict != "HOLDOUT_CONSISTENT":
            continue
        holdout = pair["out_of_sample"].get("holdout_correlation")
        rows.append(dict(
            id=pair["left"] + "→" + pair["right"],
            proposal=f"{pair['left']} antecede {pair['right']} em {pair['lag_bars']} barras",
            scope=f"{report['symbol']} · grade {report['grid_timeframe']}",
            window=f"ajuste {int(FIT_FRACTION * 100)}% · fora {100 - int(FIT_FRACTION * 100)}%",
            evidence_state="EXPLORATORY", freshness_state="CURRENT", presence_state="UNKNOWN",
            utility_state="INCONCLUSIVE", support_state="corte temporal do serviço",
            validity="pesquisa; sem ação de aplicar preset ou Expert",
            reason=(f"ρ fora da amostra {holdout:+.3f}. Consistência exploratória não autoriza operação."
                    if holdout is not None else "Consistente fora da amostra, sem efeito numérico.")))
    return rows
