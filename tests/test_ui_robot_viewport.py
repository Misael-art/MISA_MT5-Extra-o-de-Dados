"""Fonte suportada não pode deixar o corpo dos robôs sem caminho de acesso."""
import pytest
from tests.test_ui_main_window import ui, _set_ui_scaling


def test_maximum_supported_reading_scale_keeps_body_reachable(ui, request):
    previous_scaling = float(ui.root.tk.call("tk", "scaling"))
    request.addfinalizer(lambda: ui.root.tk.call("tk", "scaling", previous_scaling))
    _set_ui_scaling(ui, 2.0)
    ui.theme.set_scale(1.5)
    ui.notebook.select(ui.robots_panel.frame)
    ui.robots_panel.select("python:donchian")
    ui.root.geometry("900x620+0+0")
    ui.pump(.3)
    page = getattr(ui.robots_panel, "page_viewport", None)
    if page is not None:
        page.canvas.focus_force()
        for _ in range(12):
            page.canvas.event_generate("<Next>")
            ui.pump(.02)
    viewport = ui.robots_panel.body_canvas
    bounds = []
    node = viewport
    while node is not None:
        bounds.append((str(node), node.winfo_ismapped(), node.winfo_rooty(), node.winfo_height(), node.winfo_reqheight()))
        node = node.master
    assert viewport.winfo_viewable(), bounds
    assert max(y for name, mapped, y, h, req in bounds) < min(y+h for name, mapped, y, h, req in bounds), bounds
    assert ui.robots_panel.detail.robot.key == "python:donchian"
