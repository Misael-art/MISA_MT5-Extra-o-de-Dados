"""Fotografia imutável: buffers MT5 e fórmulas existentes sobre barras congeladas."""
import hashlib
import json
import math
import re
from datetime import datetime, timezone

import pandas as pd

from mt5_extracao.analytics import indicator_registry

MAX_BARS = 2000
MAX_OBSERVATIONS = 4096


def json_text(value):
    def default(item):
        if isinstance(item, (datetime, pd.Timestamp)):
            return item.isoformat()
        if hasattr(item, "item"):
            return item.item()
        raise TypeError(f"Tipo não serializável: {type(item).__name__}")
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False, default=default)


def utc(value):
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        raise ValueError("O horário da captura precisa indicar o fuso.")
    return stamp.tz_convert("UTC").isoformat()


def native_clock(epoch):
    return pd.Timestamp(epoch, unit="s").isoformat()


def _number(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError("Valor numérico inválido na captura.")
    return value


def validate_capture(capture):
    if not isinstance(capture, dict) or capture.get("contract") != 1:
        raise ValueError("Contrato de captura de estudo inválido.")
    if not isinstance(capture.get("symbol"), str) or not capture["symbol"].strip() or len(capture["symbol"]) > 128:
        raise ValueError("Ativo inválido na captura.")
    from mt5_extracao.timeframes import parse
    timeframe = capture.get("timeframe", "")
    if parse(timeframe) is None:
        if not isinstance(timeframe, str) or not re.fullmatch(r"(?:M[1-9][0-9]*|H[1-9][0-9]*|[1-9][0-9]*min)", timeframe):
            raise ValueError("Timeframe inválido na captura.")
    bar = _number(capture.get("bar_epoch"))
    if int(bar) != bar or bar <= 0:
        raise ValueError("Candle inválido na captura.")
    _number(capture.get("anchor_price"))
    rates, observations = capture.get("rates", []), capture.get("native", [])
    if not isinstance(rates, list) or len(rates) > MAX_BARS:
        raise ValueError("A captura excedeu o limite de barras do Estudo.")
    if not isinstance(observations, list) or len(observations) > MAX_OBSERVATIONS:
        raise ValueError("A captura excedeu o limite de indicadores do Estudo.")
    previous = 0
    for row in rates:
        if not isinstance(row, dict):
            raise ValueError("Barra inválida na captura.")
        at = _number(row.get("time"))
        if int(at) != at or not previous < at <= bar:
            raise ValueError("As barras precisam estar ordenadas, sem duplicatas nem dados posteriores ao candle.")
        for name in ("open", "high", "low", "close"):
            _number(row.get(name))
        if row["high"] < max(row["open"], row["close"], row["low"]) or row["low"] > min(row["open"], row["close"]):
            raise ValueError("OHLC inconsistente na captura.")
        for name in ("tick_volume", "real_volume", "spread"):
            if name in row and _number(row[name]) < 0:
                raise ValueError("Volume/spread inválido na captura.")
        previous = at
    if rates and previous != bar:
        raise ValueError("O prefixo congelado precisa terminar no candle escolhido.")
    seen = set()
    for row in observations:
        if not isinstance(row, dict) or not isinstance(row.get("id"), str) or not row["id"] or row["id"] in seen:
            raise ValueError("Identificador de indicador nativo inválido ou duplicado.")
        seen.add(row["id"])
        if row.get("value") is not None:
            _number(row["value"])
        if row.get("state") == "AVAILABLE" and row.get("value") is None:
            raise ValueError("Indicador disponível precisa trazer seu valor; zero não substitui ausência.")
    json_text(capture)
    return capture


def build(capture, captured_at):
    """Reutiliza o registry; não cria fatos financeiros nem retrodata disponibilidade.

    O prefixo OHLC é copiado pelo MT5 no clique. O cálculo do extrator pode
    ocorrer depois, sobre exatamente esses bytes, e registra seu próprio horário.
    Não consulta barras novas nem recalcula a fotografia ao reabrir a nota.
    """
    validate_capture(capture)
    definitions = indicator_registry.definitions()
    rates = pd.DataFrame(capture.get("rates", []))
    if not rates.empty:
        # MQL datetime codifica o relógio do servidor. Não inventar UTC: VWAP
        # diário exige exatamente esse calendário naive, explicitamente decodificado.
        rates["time"] = pd.to_datetime(rates["time"], unit="s")
    latest = {}
    if not rates.empty:
        last = rates.index[-1]
        for row in indicator_registry.iter_core(rates):
            if row["bar_index"] == last:
                latest[row["feature_id"]] = row
    computed_at = datetime.now(timezone.utc).isoformat()
    values = []
    for spec in definitions:
        row = latest.get(spec["feature_id"], {})
        values.append({"id": spec["feature_id"], "name": spec["name"], "value": row.get("value"),
                       "state": row.get("state", "SOURCE_UNAVAILABLE"),
                       "reason": row.get("reason", "NO_FROZEN_RATES"), "unit": spec["unit"],
                       "parameters": spec["parameters"], "algorithm_version": spec["algorithm_version"],
                       "formula": spec["formula"], "source": "extractor_frozen_capture",
                       "computed_at_utc": computed_at,
                       "details": {k: v for k, v in row.items() if k not in ("bar_index", "value", "state", "reason")}})
    result = {"contract": 1, "symbol": capture["symbol"], "timeframe": capture["timeframe"],
              "bar_time_native": native_clock(capture["bar_epoch"]),
              "bar_time_utc": None,
              "native_time_basis": capture.get("native_time_basis", "mt5_datetime_epoch"),
              "captured_at_utc": utc(captured_at), "computed_at_utc": computed_at,
              "native": capture.get("native", []), "extractor": values,
              "source_sha256": hashlib.sha256(json_text(capture).encode()).hexdigest(),
              "rates_sha256": hashlib.sha256(json_text(capture.get("rates", [])).encode()).hexdigest(),
              "frozen_bar_count": len(rates),
              "basis": "MT5 buffers observed at capture; MQL candle clock is server calendar with unknown UTC offset; extractor formulas use only the frozen prefix; not historical signal availability"}
    json_text(result)
    return result
