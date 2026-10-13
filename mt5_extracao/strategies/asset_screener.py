"""
Triagem de ativos.

Filtro A — o ativo é operável? (eliminatório; recalcule semanalmente)
Filtro B — há oportunidade agora? (score 0–100; recalcule diariamente ou por sessão)

    linhas = screen({"WIN$N": df_win, "EURUSD": df_eur}, specs, Timeframe.H1, ScreenerConfig())
    print(format_table(linhas))

Toda reprovação ou penalidade vem com o motivo em português (ex.: "spread/ATR alto (14,0%)").
"""
import logging
from dataclasses import dataclass, fields
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from .. import data_quality
from . import REGISTRY
from . import indicators as ind
from .backtester import SymbolSpec

log = logging.getLogger(__name__)

INTRADAY_LIMIT_MINUTES = 240   # abaixo de H4 vale o histórico mínimo intradiário


@dataclass
class ScreenerConfig:
    # Filtro A
    max_spread_atr: float = 0.10
    min_liquidity_percentile: float = 30.0
    max_bad_bars: float = 0.01
    min_years_daily: float = 3.0
    min_years_intraday: float = 1.0
    spike_multiple: float = 3.0
    max_spike_share: float = 0.02
    stop_atr: float = 2.0
    # Risco (de [STRATEGY])
    initial_equity: float = 100_000.0
    risk_per_trade: float = 0.01
    # Filtro B
    max_correlation: float = 0.7
    event_window_hours: float = 4.0
    session_start: str = "09:00"
    session_end: str = "18:30"

    @classmethod
    def from_config(cls, config) -> "ScreenerConfig":
        """Lê [SCREENER], [STRATEGY] e [APP] do config.ini; chave ausente = padrão."""
        values = {}
        for f in fields(cls):
            section = "STRATEGY" if f.name in ("initial_equity", "risk_per_trade") else \
                "APP" if f.name.startswith("session_") else "SCREENER"
            if config is None or not config.has_option(section, f.name):
                continue
            raw = config.get(section, f.name)
            values[f.name] = raw.strip() if f.type in (str, "str") else float(raw)
        return cls(**values)


def _pct(x: float) -> str:
    return f"{x * 100:.1f}%".replace(".", ",")


def _check(name, passed, text, value=None):
    return {"name": name, "passed": bool(passed), "text": text, "value": value}


# --- Filtro A ---------------------------------------------------------------------------------------

def filter_a(df: pd.DataFrame, spec: SymbolSpec, bar_minutes: int, cfg: ScreenerConfig) -> dict:
    """Critérios eliminatórios que dependem só do próprio ativo (a liquidez relativa é aplicada em screen)."""
    checks, warnings, metrics = [], [], {}
    if df is None or len(df) < 120:
        n = 0 if df is None else len(df)
        return {"checks": [_check("Dados suficientes", False, f"poucos dados ({n} barras; mínimo 120)")],
                "warnings": [], "metrics": {"bars": n}, "liquidity": np.nan}

    data = df.reset_index(drop=True)
    atr = ind.atr(data, 14)
    recent = data.tail(100)
    atr_recent = float(atr.tail(100).median())
    metrics["bars"] = len(data)
    metrics["atr"] = float(atr.iloc[-1])

    # 1) Custo: spread / ATR
    if "spread" in data.columns and data["spread"].notna().any():
        spread_pts = data["spread"].fillna(0).astype(float)
    else:
        spread_pts = pd.Series(spec.spread_points, index=data.index, dtype=float)
    if spec.known:
        spread_atr = float(spread_pts.tail(100).median() * spec.point / atr_recent) if atr_recent > 0 else np.inf
        metrics["spread_atr"] = spread_atr
        checks.append(_check("Spread/ATR", spread_atr < cfg.max_spread_atr,
                             f"spread/ATR {'ok' if spread_atr < cfg.max_spread_atr else 'alto'} ({_pct(spread_atr)}; "
                             f"máximo {_pct(cfg.max_spread_atr)})", spread_atr))
    else:
        metrics["spread_atr"] = np.nan
        warnings.append("especificação ausente: rode 'mt5x specs' (spread/ATR e lote não verificados)")

    # 2) Liquidez (comparada entre ativos em screen)
    vol_col = "real_volume" if "real_volume" in data.columns and data["real_volume"].tail(20).sum() > 0 \
        else "tick_volume"
    liquidity = float(data[vol_col].tail(20).median()) if vol_col in data.columns else np.nan
    metrics["liquidity"] = liquidity

    # 3) Barras com problema: OHLC inválido, lacunas no pregão (intradiário) e gaps anômalos de preço
    quality = data_quality.check_quality(data, bar_minutes, cfg.session_start, cfg.session_end)
    gap_px = (data["open"] - data["close"].shift(1)).abs()
    anomalous = int((gap_px > 5 * atr.shift(1)).sum())
    bad = quality["invalid_ohlc"]["count"] + quality["duplicate_time"]["count"] + quality["gaps"]["count"] + anomalous
    bad_share = bad / len(data)
    metrics["bad_bars"] = bad_share
    checks.append(_check("Barras com problema", bad_share < cfg.max_bad_bars,
                         f"{_pct(bad_share)} de barras com problema (máximo {_pct(cfg.max_bad_bars)}): "
                         f"{quality['invalid_ohlc']['count']} OHLC inválido, {quality['gaps']['count']} lacunas, "
                         f"{anomalous} gaps > 5×ATR", bad_share))

    # 4) Histórico mínimo
    times = pd.to_datetime(data["time"])
    years = (times.iloc[-1] - times.iloc[0]).days / 365.25
    intraday = bar_minutes < INTRADAY_LIMIT_MINUTES
    need = cfg.min_years_intraday if intraday else cfg.min_years_daily
    metrics["years"] = years
    checks.append(_check("Histórico", years >= need,
                         f"histórico de {years:.1f} anos (mínimo {need:g} {'intradiário' if intraday else 'D1/H4'})"
                         .replace(".", ","), years))

    # 5) Picos de spread
    window = spread_pts.tail(500)
    mean = float(window.mean())
    spikes = float((window > cfg.spike_multiple * mean).mean()) if mean > 0 else 0.0
    metrics["spread_spikes"] = spikes
    checks.append(_check("Picos de spread", spikes <= cfg.max_spike_share,
                         f"{_pct(spikes)} das barras com spread > {cfg.spike_multiple:g}× a média "
                         f"(máximo {_pct(cfg.max_spike_share)})", spikes))

    # 6) Lote para risco de 1% com stop de 2×ATR
    if spec.known:
        stop = cfg.stop_atr * float(atr.iloc[-1])
        lots = spec.lots_for_risk(cfg.initial_equity * cfg.risk_per_trade, stop)
        metrics["lots"] = lots
        checks.append(_check("Lote mínimo", lots > 0,
                             f"lote para {_pct(cfg.risk_per_trade)} de risco: {lots:g} (mínimo {spec.volume_min:g})"
                             if lots > 0 else
                             f"lote para {_pct(cfg.risk_per_trade)} de risco abaixo do mínimo ({spec.volume_min:g}): "
                             "aumente o capital ou escolha outro ativo", lots))
    return {"checks": checks, "warnings": warnings, "metrics": metrics, "liquidity": liquidity}


# --- Filtro B ---------------------------------------------------------------------------------------

def load_events(path: Optional[str]) -> pd.DataFrame:
    """CSV com colunas time,symbol,impact (impacto 'alto'/'high'/'3' bloqueia). Vazio se não houver."""
    cols = ["time", "symbol", "impact"]
    if not path:
        return pd.DataFrame(columns=cols)
    try:
        events = pd.read_csv(path)
    except FileNotFoundError:
        log.warning(f"Arquivo de eventos não encontrado: {path}. Continuando sem eventos.")
        return pd.DataFrame(columns=cols)
    missing = [c for c in cols if c not in events.columns]
    if missing:
        raise ValueError(f"Arquivo de eventos {path} sem as colunas: {', '.join(missing)} "
                         "(esperado: time,symbol,impact)")
    events["time"] = pd.to_datetime(events["time"])
    return events


def _high_impact(value) -> bool:
    return str(value).strip().lower() in ("alto", "high", "3", "alta")


def upcoming_event(symbol: str, events: pd.DataFrame, now: datetime, hours: float) -> Optional[str]:
    if events is None or events.empty:
        return None
    window = events[(events["time"] >= now) & (events["time"] <= now + timedelta(hours=hours))]
    for _, ev in window.iterrows():
        key = str(ev["symbol"]).strip().upper()
        if _high_impact(ev["impact"]) and (key == "*" or key in symbol.upper()):
            return f"evento de alto impacto ({ev['symbol']}) em {ev['time']:%d/%m %H:%M}"
    return None


def suggest_strategy(regime: str, atr_pct: float, bar_minutes: int, er: float) -> Optional[str]:
    """Família de estratégia coerente com o regime atual (ver tabela em docs/estrategias.md)."""
    if bar_minutes <= 30 and regime != "lateral":
        return "orb"
    if np.isfinite(atr_pct) and atr_pct < 20:
        return "squeeze"
    if regime == "tendência":
        if bar_minutes >= INTRADAY_LIMIT_MINUTES:
            return "donchian" if er >= 0.3 else "ema_atr"
        return "pullback"
    if regime == "lateral":
        return "bb_rsi"
    return "squeeze" if np.isfinite(atr_pct) and atr_pct < 35 else None


def filter_b(df: pd.DataFrame, bar_minutes: int, cfg: ScreenerConfig, spread_atr: float = np.nan,
             correlation: float = np.nan, event: Optional[str] = None) -> dict:
    data = df.reset_index(drop=True)
    close = data["close"]
    atr = ind.atr(data, 14)
    adx = float(ind.adx(data, 14)["adx"].iloc[-1])
    ema50 = ind.ema(close, 50)
    last_atr = float(atr.iloc[-1])
    slope = float((ema50.iloc[-1] - ema50.iloc[-11]) / 10 / last_atr) if last_atr > 0 else 0.0
    r2 = float(ind.linreg_r2(close, 50).iloc[-1])
    atr_pct = float(ind.rolling_percentile(atr, 100).iloc[-1])
    er = float(ind.efficiency_ratio(close, 20).iloc[-1])
    regime = "tendência" if adx > 25 else "lateral" if adx < 20 else "transição"

    notes = []
    if regime == "tendência":
        regime_fit, quality = min(1.0, (adx - 20) / 20), 0.5 * r2 + 0.5 * er
    elif regime == "lateral":
        regime_fit, quality = float(np.clip((25 - adx) / 15, 0, 1)), 1 - er
    else:
        regime_fit, quality = 0.3, 0.3
        notes.append("regime indefinido (ADX entre 20 e 25)")
    if atr_pct < 20:
        vol_score = 0.5
        notes.append("volatilidade muito baixa: aguarde o rompimento (squeeze)")
    elif atr_pct > 95:
        vol_score = 0.3
        notes.append("volatilidade extrema: reduza o lote")
    elif atr_pct > 85:
        vol_score = 0.6
        notes.append("volatilidade alta: reduza o lote")
    else:
        vol_score = 1.0
    cost = float(np.clip(1 - spread_atr / cfg.max_spread_atr, 0, 1)) if np.isfinite(spread_atr) else 0.5
    if np.isfinite(correlation) and correlation > cfg.max_correlation:
        corr_score = float(np.clip(1 - (correlation - cfg.max_correlation) / (1 - cfg.max_correlation), 0, 1))
        notes.append(f"correlação {correlation:.2f} com a carteira".replace(".", ","))
    else:
        corr_score = 1.0
    score = 100 * (0.30 * regime_fit + 0.25 * float(np.clip(quality, 0, 1)) + 0.15 * vol_score
                   + 0.20 * cost + 0.10 * corr_score)
    if event:
        score = 0.0
        notes.insert(0, event + ": bloqueado")
    return {"regime": regime, "adx": adx, "direction": "alta" if slope > 0 else "baixa",
            "trend_strength": abs(slope), "r2": r2, "atr_percentile": atr_pct, "efficiency": er,
            "spread_atr": spread_atr, "correlation": correlation, "event": event,
            "score": round(float(score), 1), "notes": notes,
            "strategy": None if event else suggest_strategy(regime, atr_pct, bar_minutes, er)}


# --- pipeline ---------------------------------------------------------------------------------------

def _correlations(data: Dict[str, pd.DataFrame], portfolio: List[str]) -> Dict[str, float]:
    """Maior |correlação| dos retornos (últimas 100 barras) de cada ativo com os ativos da carteira."""
    if not portfolio:
        return {}
    returns = {}
    for sym, df in data.items():
        if df is not None and len(df) > 2:
            returns[sym] = df.set_index(pd.to_datetime(df["time"]))["close"].pct_change().tail(100)
    table = pd.DataFrame(returns)
    result = {}
    for sym in returns:
        others = [p for p in portfolio if p in returns and p != sym]
        if others:
            result[sym] = float(table[others].corrwith(table[sym]).abs().max())
    return result


def screen(data: Dict[str, pd.DataFrame], specs: Dict[str, SymbolSpec], bar_minutes: int,
           cfg: Optional[ScreenerConfig] = None, events: Optional[pd.DataFrame] = None,
           portfolio: Optional[List[str]] = None, validations: Optional[pd.DataFrame] = None,
           timeframe_name: str = "", now: Optional[datetime] = None) -> List[dict]:
    """Aplica os Filtros A e B a todos os ativos e devolve as linhas ordenadas (válidos por score)."""
    cfg = cfg or ScreenerConfig()
    rows = []
    for symbol, df in data.items():
        spec = specs.get(symbol) or SymbolSpec(symbol=symbol, known=False)
        a = filter_a(df, spec, bar_minutes, cfg)
        rows.append({"symbol": symbol, "a": a})

    # Liquidez relativa (precisa de ao menos 3 ativos para fazer sentido)
    liq = pd.Series({r["symbol"]: r["a"]["liquidity"] for r in rows}, dtype=float)
    if liq.notna().sum() >= 3:
        # 0 = menos líquido, 100 = mais líquido (posição entre os ativos analisados)
        pct = (liq.rank(method="min") - 1) / (liq.notna().sum() - 1) * 100
        for r in rows:
            p = pct.get(r["symbol"], np.nan)
            ok = bool(np.isfinite(p) and p >= cfg.min_liquidity_percentile)
            r["a"]["checks"].append(_check(
                "Liquidez", ok, f"liquidez {'ok' if ok else 'baixa'} (percentil {p:.0f} dos ativos analisados; "
                f"mínimo {cfg.min_liquidity_percentile:g})", p))

    corr = _correlations(data, portfolio or [])
    result = []
    for r in rows:
        symbol, a = r["symbol"], r["a"]
        df = data[symbol]
        failed = [c["text"] for c in a["checks"] if not c["passed"]]
        row = {"symbol": symbol, "valid": not failed, "reasons": failed, "warnings": list(a["warnings"]),
               "filter_a": a, "regime": "—", "strategy": None, "strategy_name": "—", "score": 0.0,
               "validated": "", "notes": []}
        if df is not None and len(df) >= 120:
            when = now or pd.to_datetime(df["time"]).iloc[-1].to_pydatetime()
            event = upcoming_event(symbol, events, when, cfg.event_window_hours)
            b = filter_b(df, bar_minutes, cfg, a["metrics"].get("spread_atr", np.nan), corr.get(symbol, np.nan),
                         event)
            row.update(regime=b["regime"], score=b["score"] if not failed else 0.0, notes=b["notes"], filter_b=b,
                       strategy=b["strategy"])
        row["strategy"], row["validated"] = _pick_validated(row["strategy"], symbol, timeframe_name, validations)
        row["strategy_name"] = REGISTRY[row["strategy"]].info.name if row["strategy"] else "aguardar"
        result.append(row)
    result.sort(key=lambda x: (not x["valid"], -x["score"], x["symbol"]))
    return result


def _pick_validated(suggested, symbol, timeframe_name, validations):
    """Prefere a estratégia sugerida se aprovada no Filtro C; senão, outra aprovada do mesmo regime."""
    if validations is None or validations.empty or not suggested:
        return suggested, "não validada (rode 'mt5x validate')" if suggested else ""
    mine = validations[(validations["symbol"] == symbol) &
                       ((validations["timeframe"] == timeframe_name) if timeframe_name else True)]
    approved = set(mine.loc[mine["approved"].astype(bool), "strategy"])
    if suggested in approved:
        return suggested, "✅ validada (Filtro C)"
    regime = REGISTRY[suggested].info.regime
    for key in approved:
        if REGISTRY[key].info.regime == regime:
            return key, "✅ validada (Filtro C)"
    if suggested in set(mine["strategy"]):
        return suggested, "❌ reprovada no Filtro C"
    return suggested, "não validada (rode 'mt5x validate')"


def format_table(rows: List[dict]) -> str:
    header = ("Ativo", "Válido", "Regime", "Estratégia sugerida", "Score", "Observação")
    lines = []
    for r in rows:
        if r["valid"]:
            valid = "⚠" if r["warnings"] else "✅"
            obs = "; ".join(r["notes"] + r["warnings"] + ([r["validated"]] if r["validated"] else []))
        else:
            valid = "❌"
            obs = "; ".join(r["reasons"])
        lines.append((r["symbol"], valid, r["regime"], r["strategy_name"] if r["valid"] else "—",
                      f"{r['score']:.0f}", obs))
    widths = [max(len(h), *(len(l[i]) for l in lines)) if lines else len(h) for i, h in enumerate(header[:-1])]
    out = ["  ".join(h.ljust(w) for h, w in zip(header[:-1], widths)) + "  " + header[-1]]
    for l in lines:
        out.append("  ".join(v.ljust(w) for v, w in zip(l[:-1], widths)) + "  " + l[-1])
    return "\n".join(out)
