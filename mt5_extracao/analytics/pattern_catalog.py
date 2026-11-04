"""Catálogo T13.4-P01 de séries locais para pesquisa, sem coletar nem negociar.

O namespace vem de ``datasets``; símbolo e timeframe sozinhos não identificam
uma fonte. A qualificação é por recorte e família, sem inferir calendário,
disponibilidade de execução ou certificação econômica de uma tabela OHLCV.
"""

import pandas as pd
from sqlalchemy import text

from mt5_extracao import timeframes
from mt5_extracao.analytics import data_eligibility, datasets, instruments


FAMILY_REQUIREMENTS = {
    "reversal": ("ohlcv",),
    "continuation_time": ("ohlcv", "observed_session_bars"),
    "gap": ("ohlcv", "observed_session_bars"),
    "breakout_failure": ("ohlcv",),
}
MAX_CAPTURE_BARS = 20_000


def _window_rows(db, conn, symbol, timeframe, start, end):
    """Conta antes de materializar, com as mesmas fronteiras do leitor canônico."""
    table = db.find_table_for_symbol(symbol, timeframe, connection=conn)
    if table is None:
        return 0
    predicates, params = [], {}
    for key, value, operator in (("start", start, ">="), ("end", end, "<=")):
        if value is not None:
            predicates.append(f"time {operator} :{key}")
            params[key] = db._to_storage(pd.Timestamp(value)).strftime(db._TIME_FMT)
    quoted = conn.dialect.identifier_preparer.quote(table)
    query = f"SELECT COUNT(*) FROM {quoted}"
    if predicates:
        query += " WHERE " + " AND ".join(predicates)
    return int(conn.execute(text(query), params).scalar() or 0)


def sources(db):
    """Descobre somente tabelas locais; não trata o catálogo do MT5 como histórico."""
    origin = datasets.identity(db)
    clock = db.time_context()
    series = []
    for item in db.data_inventory():
        tf = timeframes.parse(item["timeframe"])
        if tf is None:
            continue
        series.append({"source_namespace": origin["namespace"], "symbol": item["symbol"],
                       "timeframe": tf.name, "rows": item["rows"],
                       "first_broker": item["start"].isoformat() if item["start"] is not None else None,
                       "last_broker": item["end"].isoformat() if item["end"] is not None else None})
    return {"source": origin, "clock": clock, "series": sorted(series, key=lambda row:
            (row["symbol"], timeframes.parse(row["timeframe"]).minutes))}


def qualify(db, *, symbol, timeframe, family, start=None, end=None):
    """Qualifica uma série e um recorte explícitos, sem escrita ou recaptura."""
    if family not in FAMILY_REQUIREMENTS:
        raise ValueError(f"Família desconhecida: {family}; escolha uma família de pesquisa declarada.")
    tf = timeframes.parse(timeframe)
    if tf is None:
        raise ValueError(f"Timeframe inválido: {timeframe}")
    available = sources(db)
    matching = [row for row in available["series"] if row["symbol"] == symbol and row["timeframe"] == tf.name]
    if len(matching) != 1:
        raise ValueError("Série ausente ou ambígua no inventário local; escolha uma origem e um timeframe únicos.")
    with db.consistent_read() as conn:
        origin = datasets.identity(db, connection=conn)
        count = _window_rows(db, conn, symbol, tf.label, start, end)
        if count > MAX_CAPTURE_BARS:
            raise ValueError(f"Recorte tem {count} barras; limite de leitura P01: {MAX_CAPTURE_BARS}. "
                             "Escolha início e fim menores.")
        bars = db.load_ohlcv(symbol, tf.label, start, end, connection=conn)
        facts = instruments.history(db, symbol, connection=conn)
        derived = datasets._derived_evidence(symbol, tf, conn)
        acquisition = None
        if not bars.empty and available["clock"]["offset_state"] == "declared":
            first_utc, end_utc = datasets._window_utc(bars, tf.name, db.broker_utc_offset)
            acquisition = datasets._acquisition_evidence(
                db, symbol, (tf.label,), conn, first_utc, end_utc, bars)
    quality = data_eligibility.assess(bars, bar_minutes=tf.minutes or 1440,
                                      symbol=symbol, timeframe=tf.name, derived=derived,
                                      evidence=datasets._evidence_items(acquisition) if acquisition is not None else ())
    gate = data_eligibility.evaluate(quality, FAMILY_REQUIREMENTS[family])
    reasons = list(gate["reasons"])
    clock = available["clock"]
    if clock["offset_state"] != "declared":
        reasons.append("Relógio do broker sem offset fixo comprovado neste capturador; fechamento UTC e snapshot recusados.")
    if bars.empty:
        reasons.append("Nenhuma barra no recorte pedido; ausência não equivale a retorno zero.")
    return {"source_namespace": origin["namespace"], "source": origin,
            "symbol": symbol, "timeframe": tf.name, "family": family,
            "selection": {"start": str(start) if start is not None else None,
                          "end": str(end) if end is not None else None},
            "rows": len(bars), "first_broker": bars.time.iloc[0].isoformat() if len(bars) else None,
            "last_broker": bars.time.iloc[-1].isoformat() if len(bars) else None,
            "instrument_fact_rows": len(facts), "clock": clock, "quality": quality,
            "required": gate["required"], "eligible": not reasons,
            "status": "blocked" if reasons else gate["status"],
            "reasons": reasons, "limitations": [*gate["limitations"],
                "Universo histórico e especificação econômica não certificados por uma tabela de barras."],
            "economic_execution": "not_qualified"}


def freeze(db, *, symbol, timeframe, family, start=None, end=None):
    """Congela um recorte compacto no store canônico após qualificação."""
    report = qualify(db, symbol=symbol, timeframe=timeframe, family=family, start=start, end=end)
    if not report["eligible"]:
        raise ValueError("Série inelegível: " + " ".join(report["reasons"]))
    if report["rows"] > MAX_CAPTURE_BARS:
        raise ValueError(f"Recorte tem {report['rows']} barras; limite de captura P01: {MAX_CAPTURE_BARS}.")
    snapshot = datasets.capture(db, symbol, timeframe, start, end)
    if (snapshot.manifest["source"]["namespace"] != report["source_namespace"] or
            len(snapshot.bars) > MAX_CAPTURE_BARS or
            not data_eligibility.evaluate(snapshot.quality, FAMILY_REQUIREMENTS[family])["eligible"]):
        raise ValueError("Fonte, tamanho ou qualidade mudaram durante a captura; qualifique a nova revisão.")
    return {"source_namespace": report["source_namespace"], "manifest_id": snapshot.manifest_id,
            "family": family, "rows": len(snapshot.bars),
            "quality": snapshot.quality, "economic_execution": "not_qualified"}
