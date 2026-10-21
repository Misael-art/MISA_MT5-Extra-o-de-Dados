"""Textos do resultado cabem no viewport real, inclusive com fonte ampliada."""
from copy import deepcopy
import json
import time

import pytest

from mt5_extracao.composition import base_composition
from tests.test_ui_main_window import ui, _set_ui_scaling


def _layout_y(widget, ancestor):
    """Posição dentro do conteúdo, sem coordenadas globais de janela desmapeada."""
    y = 0
    while widget is not ancestor:
        assert widget.master is not None, "O viewport deixou de pertencer ao detalhe."
        y += widget.winfo_y()
        widget = widget.master
    return y



def _ancestor_layout(widget):
    result = []
    while widget is not None:
        result.append(dict(widget=str(widget), mapped=widget.winfo_ismapped(),
            allocated=(widget.winfo_width(), widget.winfo_height()),
            requested=(widget.winfo_reqwidth(), widget.winfo_reqheight())))
        widget = widget.master
    return result


def _reach_label(ui, detail, label):
    # Em janela empilhada, o detalhe fica depois dos cards. Percorre as duas
    # áreas, como o usuário, antes de exigir o mapeamento do rótulo. O Tk pode
    # concluir o resize/map em eventos posteriores; 150 ms não é esse contrato.
    outer = ui.robots_panel.body_canvas
    inner = detail.tests_viewport.canvas
    deadline = time.monotonic() + 5
    while True:
        content = detail.tests_viewport.content
        inner.yview_moveto(label.winfo_y() / max(1, content.winfo_height()))
        ui.pump(.02)
        body = ui.robots_panel.body
        # Primeiro revela o próprio viewport. O rótulo pode estar desmapeado
        # pelo Canvas; sua posição global não serve de entrada para a rolagem.
        outer.yview_moveto(_layout_y(inner, body) / max(1, body.winfo_height()))
        ui.pump(.05)
        page = ui.robots_panel.page_viewport
        page.canvas.yview_moveto(_layout_y(outer, page.content) / max(1, page.content.winfo_height()))
        ui.pump(.05)
        mapped = label.winfo_ismapped() and label.winfo_viewable()
        top = bottom = None
        if mapped:
            top = max(label.winfo_rooty(), inner.winfo_rooty(), outer.winfo_rooty(), page.canvas.winfo_rooty())
            bottom = min(label.winfo_rooty() + label.winfo_height(),
                         inner.winfo_rooty() + inner.winfo_height(),
                         outer.winfo_rooty() + outer.winfo_height(),
                         page.canvas.winfo_rooty() + page.canvas.winfo_height())
        if (mapped
                and top < bottom
                and label.winfo_reqwidth() <= label.master.winfo_width()
                and int(float(label.cget("wraplength"))) <= label.winfo_width()):
            return
        assert time.monotonic() < deadline, json.dumps({
            "geometry": ui.root.geometry(), "tab": detail.tabs.select(),
            "label": str(label), "mapped": label.winfo_ismapped(),
            "requested": (label.winfo_reqwidth(), label.winfo_reqheight()),
            "allocated": (label.winfo_width(), label.winfo_height()),
            "parent": (label.master.winfo_width(), label.master.winfo_height()),
            "visible_vertical_intersection": (top, bottom),
            "outer_scroll": outer.yview(), "inner_scroll": inner.yview(),
            "inner_layout_y": _layout_y(inner, body), "body_height": body.winfo_height(),
            "outer_bounds": (outer.winfo_rooty(), outer.winfo_height()),
            "inner_bounds": (inner.winfo_rooty(), inner.winfo_height()),
            "page_bounds": (page.canvas.winfo_rooty(), page.canvas.winfo_height()),
            "ancestors": _ancestor_layout(label),
        })


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
                _reach_label(ui, detail, label)
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
