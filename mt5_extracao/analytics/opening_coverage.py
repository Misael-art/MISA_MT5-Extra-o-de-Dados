"""F19-PESQ-01: cobertura da abertura nominal por pregão e contrato WIN."""
import hashlib
import json
import re
from datetime import date, time
from math import isfinite

import pandas as pd

from mt5_extracao.analytics import data_eligibility, instruments, tick_fidelity
from mt5_extracao.strategies.indicators import atr


VENUE_TZ = "America/Sao_Paulo"
MINUTE = pd.Timedelta(minutes=1)
OPENING_MINUTES = 15
ATR_PERIOD = 14
WIN_CONTRACT = re.compile(r"WIN[FGHJKMNQUVXZ][0-9]{2}")


def _schedule(calendar):
    """Calendário declarado por um publicador, com rolagem escolhida no próprio roster."""
    if not isinstance(calendar, dict) or set(calendar) != {"source", "sessions"}:
        raise ValueError("Informe um calendário com source e sessions.")
    source, sessions = calendar["source"], calendar["sessions"]
    if not isinstance(source, str) or not source.strip() or not isinstance(sessions, list) or not sessions:
        raise ValueError("O calendário exige a referência da fonte e pelo menos um pregão.")
    result = {}
    for row in sessions:
        fields = {"date", "contract", "kind", "known_at", "open", "close", "auction_open"}
        if not isinstance(row, dict) or set(row) - fields or not all(isinstance(v, str) for v in row.values()):
            raise ValueError("Pregão inválido no calendário declarado.")
        day = date.fromisoformat(row.get("date", "")).isoformat()
        kind, contract = row.get("kind"), row.get("contract", "")
        if day in result or kind not in ("regular", "special", "closed", "unknown") or not WIN_CONTRACT.fullmatch(contract):
            raise ValueError("Informe um único contrato WIN e tipo de sessão por data.")
        opening, closing, auction = (row.get(key) for key in ("open", "close", "auction_open"))
        if kind in ("regular", "special"):
            if not (opening and closing and opening < closing) or (auction and auction > opening):
                raise ValueError("Revise os horários de abertura, fechamento e pré-abertura do pregão.")
        elif any((opening, closing, auction)):
            raise ValueError("Sessão fechada ou desconhecida não declara horários de negociação.")

        instants = []
        for clock in (opening, closing, auction):
            if clock is None:
                instants.append(None)
                continue
            if not re.fullmatch(r"[0-9]{2}:[0-9]{2}", clock):
                raise ValueError("Use HH:MM no relógio da bolsa para os horários de sessão.")
            local = pd.Timestamp.combine(date.fromisoformat(day), time.fromisoformat(clock))
            instants.append(local.tz_localize(VENUE_TZ).tz_convert("UTC"))
        result[day] = (contract, kind, pd.Timestamp(instruments.utc(row.get("known_at"))), *instants)
    return sorted(result.items())


def _coverage(bars, start, end):
    expected = pd.date_range(start, end, freq="min", inclusive="left")
    missing = expected.difference(bars.index)
    aligned = bars.index.difference(expected).empty and bars.index.is_unique
    quality = data_eligibility.assess(bars)["capabilities"]["ohlcv"]["state"]
    return {"expected": len(expected), "observed": len(expected) - len(missing),
            "missing_utc": [stamp.isoformat() for stamp in missing], "valid_ohlc": aligned and quality == "available"}


def _ticks(snapshot, start, end, cutoff):
    caps = snapshot.quality["capabilities"] if snapshot else {}
    tick_cap = caps.get("ticks", {})
    result = {"state": tick_cap.get("state", "not_captured"), "origin": tick_cap.get("origin", "unknown"),
              "coverage_state": "unverified", "rows": None, "minutes_with_ticks": None,
              "prices": None, "spread": None, "aggressor": "unknown", "queue": "missing"}
    if not snapshot or "ticks" not in snapshot.manifest["frames"]:
        return result
    ticks = snapshot.market_frame("ticks")
    times = pd.to_datetime(ticks.time_msc, unit="ms", utc=True, errors="coerce")
    window = times.ge(start) & times.lt(end) & times.le(cutoff)
    audit = tick_fidelity.audit(ticks.loc[window], point=1)
    result.update(rows=audit["rows"], minutes_with_ticks=int(times[window].dt.floor("min").nunique()),
                  prices={"bid_ask_rows": audit.get("quoted_rows", 0), "last_rows": audit.get("last_rows", 0)},
                  spread=audit.get("spread"), defects=audit["defects"])
    if caps.get("observed_ticks", {}).get("state") == "available" and audit["rows"]:
        sides = data_eligibility.assess(None, ticks=ticks.loc[window])["capabilities"]["aggressor"]
        if sides["trades"] and sides["trades"] == sides["classified_trades"]:
            result["aggressor"] = "observed_flags"
    if not audit["rows"]:
        result["state"] = "missing"
    return result


def report(snapshots, calendar, *, as_of):
    """Lê snapshots congelados. A janela de 15 min usa o relógio declarado da bolsa."""
    schedule = _schedule(calendar)
    cutoff = pd.Timestamp(instruments.utc(as_of))
    by_contract = {}
    source = None
    for snapshot in snapshots:
        manifest = snapshot.manifest
        contract, clock = manifest["symbol"], manifest["time"]
        if (not WIN_CONTRACT.fullmatch(contract) or manifest["timeframe"] != "M1" or
                "bars" not in manifest["frames"] or clock.get("offset_state") != "declared" or
                clock.get("consumer_basis") != "broker"):
            raise ValueError("Use snapshots de mercado M1 de contratos WIN com relógio declarado.")
        if contract in by_contract or (source is not None and manifest["source"] != source):
            raise ValueError("Use um snapshot por contrato, da mesma fonte e base.")
        source = manifest["source"]
        bars = snapshot.bars
        times = pd.to_datetime(bars.time, format="ISO8601").dt.tz_localize("UTC")
        times -= pd.Timedelta(hours=clock["broker_utc_offset_hours"])
        bars.index = pd.DatetimeIndex(times)
        by_contract[contract] = (snapshot, bars.sort_index(), snapshot.instrument_history)
    if not by_contract:
        raise ValueError("Escolha ao menos um snapshot de mercado M1.")

    rows, previous, previous_close = [], None, None
    for day, (contract, kind, known, opening, closing, auction) in schedule:
        snapshot, bars, history = by_contract.get(contract, (None, None, None))
        row = {"date": day, "contract": contract, "session_kind": kind,
               "snapshot": snapshot.manifest_id if snapshot else None,
               "broker_utc_offset_hours": snapshot.manifest["time"]["broker_utc_offset_hours"] if snapshot else None,
               "calendar_known_at_utc": known.isoformat(), "status": "excluded", "reasons": [],
               "expiration": None, "roll_from": previous if previous != contract else None,
               "expected_open_utc": None, "observed_open_utc": None, "expected_close_utc": None,
               "auction_open_utc": auction.isoformat() if auction is not None else None,
               "auction_observation": "not_classified", "m1": None, "ticks": None, "range": None,
               "gap_points": None, "prior_atr_points": None, "prior_session": None}
        rows.append(row)
        if opening is None:
            row["reasons"].append("closed_session" if kind == "closed" else "unknown_schedule")
            if kind == "unknown":
                previous_close = None
            continue
        row.update(expected_open_utc=opening.isoformat(), expected_close_utc=closing.isoformat())
        reasons = row["reasons"]
        if kind == "special":
            reasons.append("special_session")
        knowledge_cutoff = min(opening, cutoff)
        if known > knowledge_cutoff:
            reasons.append("calendar_not_known_at_open")
        window_end = opening + OPENING_MINUTES * MINUTE
        if window_end > closing:
            reasons.append("window_outside_session")
        if cutoff < window_end:
            reasons.append("window_not_closed")
        fact = instruments.expiration(history, contract, known_at=knowledge_cutoff) if history is not None else None
        if fact:
            row["expiration"] = {"at_utc": fact["valid_from"], "revision_id": fact["revision_id"], "source": fact["source"]}
            if window_end > pd.Timestamp(fact["valid_from"]):
                reasons.append("expiration_before_window_close")
        else:
            reasons.append("expiration_unknown_at_open")
        if snapshot is None:
            reasons.append("snapshot_missing")
            previous, previous_close = contract, None
            continue

        session = bars.loc[(bars.index >= opening) & (bars.index < closing) & (bars.index + MINUTE <= cutoff)]
        if not session.empty:
            row["observed_open_utc"] = session.index[0].isoformat()
        window = session.loc[session.index < window_end]
        row["m1"] = _coverage(window, opening, window_end)
        row["ticks"] = _ticks(snapshot, opening, min(closing, window_end), cutoff)
        if row["m1"]["missing_utc"] and cutoff >= window_end:
            reasons.append("no_opening_bars" if window.empty else "incomplete_opening_m1")
        if not window.empty and not row["m1"]["valid_ohlc"]:
            reasons.append("invalid_opening_m1")

        if previous == contract and previous_close is not None:
            prior_day, prior_price, prior_atr = previous_close
            row.update(prior_session=prior_day, prior_atr_points=prior_atr)
            if not reasons:
                row["gap_points"] = float(window.iloc[0].open - prior_price)
        previous, previous_close = contract, None
        if (cutoff >= closing and known <= opening and not session.empty and fact and
                closing <= pd.Timestamp(fact["valid_from"])):
            full = _coverage(session, opening, closing)
            if not full["missing_utc"] and full["valid_ohlc"]:
                value = atr(session, ATR_PERIOD).iloc[-1]
                previous_close = (day, session.iloc[-1].close, float(value) if isfinite(value) else None)
        if reasons:
            continue
        high, low = float(window.high.max()), float(window.low.min())
        spec = instruments.specification(history, opening) if history is not None else None
        tick_size = spec["payload"].get("tick_size") if spec else None
        row.update(status="eligible", range={"start_utc": opening.isoformat(), "available_at_utc": window_end.isoformat(),
                                             "high": high, "low": low, "width_points": high - low,
                                             "width_ticks": (high - low) / tick_size if tick_size and tick_size > 0 else None,
                                             "specification_revision": spec["revision_id"] if spec else None})

    manifest = {"version": 1, "source": source, "calendar_source": calendar["source"], "timezone": VENUE_TZ,
                "opening_minutes": OPENING_MINUTES, "prior_atr_period": ATR_PERIOD, "as_of": cutoff.isoformat(),
                "eligible": sum(row["status"] == "eligible" for row in rows), "sessions": rows}
    manifest["manifest_id"] = hashlib.sha256(json.dumps(manifest, sort_keys=True, separators=(",", ":"),
                                                        allow_nan=False).encode("utf-8")).hexdigest()
    return manifest
