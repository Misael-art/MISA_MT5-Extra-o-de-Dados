"""Tradução fechada dos parâmetros do catálogo para o Expert de mercado."""
from datetime import datetime, timezone
from decimal import Decimal

from ..composition.registry import STRATEGY_PARAMETERS


INPUTS = {
    "ema_atr": dict(fast="FastPeriod", slow="SlowPeriod", trend="TrendPeriod", stop_atr="StopAtrMult",
                    trail_atr="TrailAtrMult"),
    "donchian": dict(entry_period="EntryPeriod", exit_period="ExitPeriod", adx_min="AdxMin",
                     atr_avg="AtrAvgPeriod", stop_atr="StopAtrMult"),
    "bb_rsi": dict(bb_period="BbPeriod", bb_k="BbDeviation", rsi_period="RsiPeriod", rsi_low="RsiLow",
                   adx_max="AdxMax", stop_atr="StopAtrMult", max_bars="MaxBars"),
    "orb": dict(range_minutes="RangeMinutes", target_mult="TargetMult", min_range_atr="MinRangeAtr"),
    "pullback": dict(trend="TrendPeriod", adx_min="AdxMin", rsi_period="RsiPeriod", rsi_pullback="RsiPullback",
                     swing_bars="SwingBars", target_r="TakeProfitR"),
    "squeeze": dict(period="Period", bb_k="BbDeviation", kc_mult="KeltnerMult", lookback="Lookback",
                    stop_atr="StopAtrMult", trail_atr="TrailAtrMult"),
    "s09_reference": dict(fast="FastPeriod", slow="SlowPeriod", warmup_bars="WarmupBars",
                          stop_points="StopPoints", target_points="TargetPoints"),
    "stoch_ema9": dict(k_period="KPeriod", slowing="Slowing", d_period="DPeriod", ema_period="EmaPeriod",
                       upper_level="UpperLevel", lower_level="LowerLevel", stop_offset_points="StopOffsetPoints",
                       target_points="TargetPoints"),
    "vwap_bands": dict(entry_band_pct="EntryBandPct", target_band_pct="TargetBandPct",
                      stop_offset_points="StopPoints", volume_source="VolumeSource"),
    "madrid": dict(first_period="FirstPeriod", period_step="PeriodStep", distance_atr="DistanceAtr",
                   distance_points="DistancePoints", trail_line="TrailLine", offset_atr="OffsetAtr"),
    "ha_hilo": dict(hilo_period="HiLoPeriod", hilo_offset="HiLoOffset", stop_buffer_points="StopBufferPoints"),
    "super_atr_triple": dict(seed_start_broker="SeedStart", period="Period", mean="Mean",
                             deviation1="Deviation1", deviation2="Deviation2", deviation3="Deviation3",
                             initial_side="InitialSide", entries_enabled="EntriesEnabled"),
}


def edits(parameters):
    if not isinstance(parameters, dict):
        raise ValueError("parameters: esperado objeto por módulo.")
    translated = {}
    for key, values in parameters.items():
        if key not in INPUTS or not isinstance(values, dict):
            raise ValueError(f"parameters.{key}: esperado módulo nativo e seus parâmetros.")
        translated[key] = {}
        for name, value in values.items():
            field = f"parameters.{key}.{name}"
            spec = STRATEGY_PARAMETERS[key].get(name)
            if spec is None or not spec.accepts(value):
                raise ValueError(f"{field}: valor fora do contrato tipado da estratégia.")
            if name == "range_minutes" and (type(value) is not int or not 1 <= value <= 240):
                raise ValueError(f"{field}: MQL5 requer 1 a 240 minutos inteiros.")
            if name == "initial_side" and value not in (-1, 1):
                raise ValueError(f"{field}: informe -1 ou 1.")
            if spec.kind == "int" and abs(value) > 2147483647:
                raise ValueError(f"{field}: valor fora do intervalo de int do MQL5.")
            if spec.unit == "bar" and key != "super_atr_triple" and value >= 1500:
                raise ValueError(f"{field}: o prefixo do Expert contém no máximo 1500 barras.")
            if spec.kind == "broker_datetime":
                seconds = int(datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp())
                literal = f"(datetime){seconds}"
            elif spec.kind == "bool":
                literal = str(value).lower()
            else:
                literal = format(Decimal(str(value)), "f")
            translated[key]["Inp" + INPUTS[key][name]] = literal
    return translated


def validate_effective(key, inputs):
    def number(name):
        return Decimal(inputs["Inp" + INPUTS[key][name]])

    if key in ("ema_atr", "s09_reference") and number("fast") >= number("slow"):
        raise ValueError(f"parameters.{key}.fast: deve ser menor que slow.")
    if key == "s09_reference" and number("warmup_bars") <= number("slow"):
        raise ValueError("parameters.s09_reference.warmup_bars: deve exceder slow.")
    if key == "madrid" and number("first_period") + 9 * number("period_step") > 1000:
        raise ValueError("parameters.madrid.period_step: a décima média não pode exceder 1000 barras.")
    if key == "stoch_ema9" and number("lower_level") >= number("upper_level"):
        raise ValueError("parameters.stoch_ema9.lower_level: deve ser menor que upper_level.")
    if key == "vwap_bands" and number("target_band_pct") >= number("entry_band_pct"):
        raise ValueError("parameters.vwap_bands.target_band_pct: deve ser menor que entry_band_pct.")
