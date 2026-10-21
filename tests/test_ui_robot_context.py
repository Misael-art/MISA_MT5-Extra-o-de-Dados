"""Textos do resultado cabem no viewport real, inclusive com fonte ampliada."""
from copy import deepcopy

import pytest

from mt5_extracao.composition import base_composition
from tests.test_ui_main_window import ui, _set_ui_scaling


@pytest.mark.parametrize("scaling", [96 / 72, 144 / 72])
def test_backtest_context_wraps_inside_actual_viewport_and_remains_scrollable(ui, scaling, request):
    previous_scaling = float(ui.root.tk.call("tk", "scaling"))
    request.addfinalizer(lambda: ui.root.tk.call("tk", "scaling", previous_scaling))
    _set_ui_scaling(ui, scaling)
    ui.notebook.select(ui.robots_panel.frame)
    ui.robots_panel.select("python:donchian")
    detail = ui.robots_panel.detail
    detail.tabs.select(1)
    composition = base_composition("donchian", symbol="WIN$N", timeframe="M1",
                                   risk_fraction=.0025, compound=False).to_dict()
    composition["protection"] = {"mode": "fixed_points", "params": {"stop_points": 100}}
    composition["profit_exit"] = {"mode": "fixed_r", "params": {"target_r": 1.5}}
    run = {"run_id": "long-context", "symbol": "WIN$N", "timeframe": "M1", "bars": 600,
           "data_start": "2024-01-02", "data_end": "2024-01-03", "created_at": "2024-02-01T12:34",
           "metrics": {"trades": 2, "net_profit": -10, "profit_factor": .5,
                       "max_drawdown_pct": .1, "win_rate": .5,
                       "spec_known": False, "composition": composition},
           "equity": [["2024-01-02", 100], ["2024-01-03", 90]]}
    original = deepcopy(run)
    # A troca de tema redesenha o card a partir do modelo, como no histórico
    # persistido; a fixture precisa registrar o resultado também nesse modelo.
    detail.robot.runs = [run]
    detail.show_run(run)
    error = "Não foi possível concluir a consulta desta seleção. O resultado anterior foi preservado. " * 3
    detail.execution_status.configure(text=error)
    labels = (detail.tests_text, detail.result_context, detail.execution_status, detail.run_status)
    text = [label.cget("text") for label in labels]
    for _ in range(2):
        for width, height in ((1920, 1000), (1280, 800), (900, 620), (820, 800), (1920, 1000)):
            ui.root.geometry(f"{width}x{height}")
            ui.pump(.15)
            for label in labels:
                assert label.winfo_ismapped()
                assert label.winfo_reqwidth() <= label.master.winfo_width(), (
                    width, height, str(label), label.winfo_reqwidth(), label.master.winfo_width())
                assert int(float(label.cget("wraplength"))) <= label.winfo_width()
            assert [label.cget("text") for label in labels] == text
        ui.toggle_theme()
    ui.root.geometry("900x620")
    ui.pump(.15)
    canvas = detail.tests_viewport.canvas
    canvas.focus_force()
    for _ in range(12):
        canvas.event_generate("<Next>")
        ui.pump(.02)
    assert canvas.yview()[1] >= .999
    assert detail.shown_run is run and run == original
