import pandas as pd
import pytest

from mt5_extracao.trade_analytics import ledger
from tests.test_ui_laboratory import panel, _trades


@pytest.mark.parametrize("dark", [False, True])
def test_lab_exports_context_with_reasons_and_disables_export_in_relations(panel, tmp_path, monkeypatch, dark):
    from mt5_extracao.ui import laboratory
    if dark:
        panel.theme.toggle()
    ledger.save_trades(panel.db, "context", _trades(7, 3), robot_key="python:ctx", symbol="TEST", timeframe="5 minutos")
    panel.reload()
    panel.pump(lambda: panel.view is not None)
    assert panel.view is not None and len(panel.view) == 3
    target = tmp_path / "context.csv"
    monkeypatch.setattr(laboratory.filedialog, "asksaveasfilename", lambda **k: str(target))
    panel.indicators_button.invoke()
    panel.pump(lambda: target.exists())
    rows = pd.read_csv(target, keep_default_na=False)
    assert len(rows) == 3 and rows["trend.efficiency_ratio_20::reason"].eq("CLOCK_REQUIRED").all()
    panel.tabs.select(panel.tab_frames[laboratory.RELATIONS_TAB])
    panel.render_visible()
    panel.ui.root.update()
    assert panel.indicators_button.instate(["disabled"])
    assert panel.relations_panel is not None


def test_context_change_preserves_export_destination(panel, tmp_path, monkeypatch):
    from mt5_extracao.ui import laboratory
    ledger.save_trades(panel.db, "context", _trades(7, 3), robot_key="python:ctx", symbol="TEST", timeframe="5 minutos")
    panel.reload()
    panel.pump(lambda: panel.view is not None)
    target = tmp_path / "context.csv"
    target.write_text("anterior", encoding="utf-8")
    monkeypatch.setattr(laboratory.filedialog, "asksaveasfilename", lambda **k: str(target))
    jobs = []
    monkeypatch.setattr(panel.ui, "run_bg", lambda work, done: jobs.append((work, done)))
    panel.indicators_button.invoke()
    panel._indicator_context_revision += 1
    work, done = jobs.pop()
    with pytest.raises(Exception, match="cancelada") as caught:
        work()
    done(None, caught.value)
    assert target.read_text(encoding="utf-8") == "anterior"


def test_indicator_action_does_not_cover_title_at_large_dpi(panel):
    import tkinter.font as font
    root = panel.ui.root
    root.tk.call("tk", "scaling", 144 / 72)
    for name in font.names(root):
        current = font.nametofont(name, root)
        current.configure(size=current.cget("size"))
    panel.frame.master.pack_configure(fill="both", expand=True)
    for width in (820, 1280, 820):
        root.geometry(f"{width}x800")
        root.update()
        header = panel.indicators_button.master.master
        assert header.title_label.winfo_width() >= header.title_label.winfo_reqwidth()
        for widget in (panel.import_button, panel.refresh_button, panel.indicators_button):
            assert widget.winfo_rootx() + widget.winfo_width() <= root.winfo_rootx() + root.winfo_width()
