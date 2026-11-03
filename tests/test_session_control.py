"""Canal de encerramento sem interface: fila, respostas do wrapper, veredito e serviço da sessão."""
from types import SimpleNamespace

import pytest

from mt5_extracao.robots import session_control, sessions

SID = "a" * 32


def ack(seq, command, state, own=(0, 0), ambiguous=(0, 0), foreign=(0, 0), attempts=0, actions=""):
    return f"{seq};{command};{state};{own[0]};{own[1]};{ambiguous[0]};{ambiguous[1]};{foreign[0]};{foreign[1]};{attempts};{actions}\n"


def test_queue_is_atomic_and_rejects_unknown_commands(tmp_path):
    path = session_control.write_commands(tmp_path, SID, [("PAUSE", 0), ("FLATTEN", 5)])
    assert path.read_text() == "1;PAUSE;0\n2;FLATTEN;5\n" and not list(tmp_path.glob("*.tmp"))
    with pytest.raises(ValueError):
        session_control.write_commands(tmp_path, SID, [("CLOSE_ALL", 0)])


def test_closed_only_when_flatten_reports_zero_own_and_zero_ambiguous():
    flat = session_control.parse_ack(ack(2, "FLATTEN", "flat", foreign=(1, 1), attempts=1, actions="remove:4:10009,close:2:10009"))
    verdict = session_control.closure(flat)
    assert verdict["closed"] and verdict["preserved_foreign"] == dict(positions=1, orders=1)
    assert [a["kind"] for a in verdict["last"]["actions"]] == ["remove", "close"]
    ambiguous = session_control.closure(session_control.parse_ack(ack(2, "FLATTEN", "ambiguous", ambiguous=(1, 0))))
    assert not ambiguous["closed"] and "ambígua" in ambiguous["reason"]
    exposed = session_control.closure(session_control.parse_ack(ack(2, "FLATTEN", "exposed", own=(1, 0), attempts=5)))
    assert not exposed["closed"] and "restante: 1" in exposed["reason"]
    assert not session_control.closure(session_control.parse_ack(ack(1, "PAUSE", "paused")))["closed"]


def test_incomplete_ack_is_an_error_not_an_empty_state():
    with pytest.raises(ValueError):
        session_control.parse_ack("2;FLATTEN;flat;0;0\n")


def test_channel_liquidate_waits_for_real_answers(tmp_path):
    channel = session_control.Channel(tmp_path, SID)
    answers = iter([ack(1, "PAUSE", "paused"), ack(2, "FLATTEN", "liquidating", own=(1, 0), attempts=1),
                    ack(2, "FLATTEN", "flat", attempts=2, actions="close:7:10009")])
    target = tmp_path / session_control.filenames(SID)["ack"]

    def sleep(_):
        with target.open("a") as f:
            f.write(next(answers, ""))
    clock = iter(range(1000)).__next__
    verdict = channel.liquidate(30, poll=0, clock=clock, sleep=sleep)
    assert verdict["closed"] and verdict["responded"] and verdict["last"]["attempts"] == 2
    assert (tmp_path / session_control.filenames(SID)["control"]).read_text() == "1;PAUSE;0\n2;FLATTEN;0\n"


def test_channel_without_answer_is_not_closed(tmp_path):
    verdict = session_control.Channel(tmp_path, SID).liquidate(3, poll=0, clock=iter(range(1000)).__next__, sleep=lambda _: None)
    assert not verdict["closed"] and not verdict["responded"]


def test_wrapper_gates_opening_and_defines_owner_by_session_start():
    prelude, body = session_control.prelude(), session_control.source(SID, "InpMagicNumber", tester_queue=True)
    assert "#define OrderSend MT5X_OrderSend" in prelude and "#define OrderCheck MT5X_OrderCheck" in prelude and "TRADE_RETCODE_TRADE_DISABLED" in prelude
    assert "r.action==TRADE_ACTION_PENDING) return true" in prelude and "r.position!=0) return false" in prelude
    assert "opened>=MT5X_CtlStart ? 1 : 2" in body and "magic!=InpMagicNumber" in body
    assert "TRADE_ACTION_REMOVE" in body and "r.position=ticket" in body and "{{" not in body
    assert "PositionClose" not in body and "CloseAll" not in body                     # nada global


def test_controller_close_out_keeps_session_when_exposure_remains(tmp_path, monkeypatch):
    from mt5_extracao.automation import sessions as automation
    from mt5_extracao.robots import mt5_tools
    monkeypatch.setattr(mt5_tools, "common_files_dir", lambda _: tmp_path)
    controller = automation.SessionController(None, {}, None)
    cancelled = []
    job = SimpleNamespace(id=SID, mode="play", config={}, cancel=lambda: cancelled.append(1))
    controller.jobs[SID] = job
    monkeypatch.setattr(controller, "status", lambda params: dict(session=SID))
    monkeypatch.setattr(session_control.Channel, "liquidate",
                        lambda self, timeout: dict(closed=False, reason="exposição própria restante: 1"))
    result = controller.close_out({"session": SID, "timeout": 5})
    assert not result["stopped"] and not cancelled and result["supervision"].startswith("mantida")
    monkeypatch.setattr(session_control.Channel, "liquidate", lambda self, timeout: dict(closed=True, reason=""))
    assert controller.close_out({"session": SID, "timeout": 5})["stopped"] and cancelled == [1]
    job.mode = "backtest"
    controller.controls.clear()
    with pytest.raises(ValueError):
        controller.pause({"session": SID})


def test_indicator_cache_reuses_only_identical_sources_and_compiler(tmp_path):
    from mt5_extracao.robots import mt5_tools
    root = tmp_path / "s1" / "MQL5"
    (root / "Indicators").mkdir(parents=True)
    (root / "Include").mkdir()
    indicator, header = root / "Indicators" / "Study.mq5", root / "Include" / "Lib.mqh"
    indicator.write_text("#include <Lib.mqh>")
    header.write_text("int x;")
    editor = tmp_path / "MetaEditor64.exe"
    editor.write_bytes(b"v1")
    items = [SimpleNamespace(source=indicator, target=indicator), SimpleNamespace(source=header, target=header)]
    calls = []

    def run(command, cwd, prefix, timeout):
        calls.append(command)
        indicator.with_suffix(".ex5").write_bytes(b"binary-%d" % len(calls))
        indicator.with_suffix(".log").write_text("Result: 0 errors, 0 warnings, 1 ms elapsed")
        return 0
    cache = tmp_path / ".compile-cache"
    assert mt5_tools.compile_indicator_dependencies(items, editor, None, run, cache=cache) is None and len(calls) == 1
    indicator.with_suffix(".ex5").unlink()
    assert mt5_tools.compile_indicator_dependencies(items, editor, None, run, cache=cache) is None and len(calls) == 1
    assert indicator.with_suffix(".ex5").read_bytes() == b"binary-1"                        # reaproveitado
    header.write_text("int y;")                                                               # qualquer fonte muda a chave
    mt5_tools.compile_indicator_dependencies(items, editor, None, run, cache=cache)
    editor.write_bytes(b"v2")                                                                 # outro compilador também
    mt5_tools.compile_indicator_dependencies(items, editor, None, run, cache=cache)
    assert len(calls) == 3 and not list(cache.glob("*.tmp"))
