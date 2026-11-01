"""Reconciliação detalhada um a um (decisão → posição → saída), sem sobrescrever colisões."""
from mt5_extracao.robots import parity


def py(entry, direction=1, reason="stop", exit_time="2026-10-01 10:00:00", volume=1.0, stop=1.1, target=1.3):
    return dict(entry_time=entry, exit_time=exit_time, direction=direction, exit_reason=reason, volume=volume,
                stop_price=stop, target_price=target, entry_price=1.2, exit_price=stop, pnl=-100.0)


def nat(entry, direction="buy", reason="sl", time="2026.10.01 10:00:12", volume="1.00", sl="1.10000", tp="1.30000", pid="7"):
    return dict(entry_time=entry, time=time, direction=direction, exit_reason=reason, volume=volume, sl=sl, tp=tp,
                entry_price="1.2", exit_price="1.1", result="-100", commission="0", swap="0", fee="0", position_id=pid)


def test_identical_pair_links_decision_and_position():
    report = parity.reconcile_trades([py("2026-10-01 08:30:00")], [nat("2026.10.01 08:30:05")],
        [dict(decision="2026.10.01 08:30", side="1", eligible="1")])
    row = report["rows"][0]
    assert report["summary"]["identical"] == 1 and row["flags"] == [] and row["position_id"] == "7"
    assert row["decision"]["eligible"] == "1" and report["summary"]["native_positions_linked_to_decision"] == 1


def test_same_minute_repetition_is_reported_not_overwritten():
    report = parity.reconcile_trades([py("2026-10-01 08:30:00")],
                                     [nat("2026.10.01 08:30:05", pid="7"), nat("2026.10.01 08:30:40", pid="8")])
    assert report["summary"]["collisions"] and report["summary"]["paired"] == 1
    assert report["only_native"][0]["position_id"] == "8"                 # a duplicata fica visível


def test_each_divergence_is_flagged_and_empty_reason_is_not_guessed():
    report = parity.reconcile_trades(
        [py("2026-10-01 08:30:00", reason="fim do dia", volume=2.0)],
        [nat("2026.10.01 08:30:05", reason="", time="2026.10.01 10:03:00", volume="1.50", sl="1.10010", tp="1.30020")])
    row = report["rows"][0]
    assert set(row["flags"]) == {"reason", "exit_minute", "sl", "tp", "volume"}
    assert row["native_reason"] == "(vazio)" and report["summary"]["empty_native_reason"] == 1
    assert row["volume_ratio"] == 0.75


def test_unmatched_sides_and_decision_lines():
    report = parity.reconcile_trades([py("2026-10-01 08:30:00", direction=-1)], [nat("2026.10.01 08:30:05")])
    assert report["summary"]["only_python"] == 1 and report["summary"]["only_native"] == 1
    lines = parity.parse_decision_lines("x\tSAREF_DECISION decision=2026.10.01 08:30 side=-1 close=1.2 eligible=1 orders=1\n")
    assert lines == [dict(decision="2026.10.01 08:30", side="-1", close="1.2", eligible="1", orders="1")]


def test_empty_csv_reason_is_filled_only_from_host_log_of_that_position():
    rows = [nat("2026.10.01 08:30:05", reason="", pid="16"), nat("2026.10.01 09:30:05", reason="", pid="17")]
    trades = [py("2026-10-01 08:30:00", reason="fim do dia"), py("2026-10-01 09:30:00", reason="fim do dia")]
    closes = parity.parse_close_lines("x Posição #16 fechada (fim do dia)\ny Posição #99 fechada (fim do dia)\n")
    report = parity.reconcile_trades(trades, rows, host_closes=closes)
    by_pid = {r["position_id"]: r for r in report["rows"]}
    assert by_pid["16"]["native_reason"] == "fim do dia" and by_pid["16"]["native_reason_source"] == "log_host"
    assert by_pid["17"]["native_reason"] == "(vazio)" and by_pid["17"]["native_reason_source"] is None   # sem prova, segue vazio
