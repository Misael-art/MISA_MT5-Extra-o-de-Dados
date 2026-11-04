"""Registro e execução separados, espera visível e recibo no HTML."""
import json
from tests.test_ui_laboratory import panel
from tests.test_ui_laboratory_series import open_series
from tests.test_laboratory_links import linked,analyze
from tests.test_laboratory_protocol import definition


def test_ui_registers_then_evaluates_and_exports_receipt(panel,linked,tmp_path,monkeypatch):
    from mt5_extracao.ui import laboratory_links
    from tkinter import simpledialog
    panel.ui.app.db_manager=linked[0];series=open_series(panel);panel.pump(lambda:not series._loading_sources)
    report=analyze(linked);series.set_report(report);series.open_links();view=series.links_dialog;panel.pump(lambda:not view.pending)
    path=tmp_path/'protocol.json';path.write_text(json.dumps(definition(report)),encoding='utf-8')
    monkeypatch.setattr(laboratory_links.filedialog,'askopenfilename',lambda **k:str(path))
    view.register_protocol();assert view.pending;panel.pump(lambda:not view.pending,seconds=30)
    assert view.protocol_id and not getattr(view,'evaluation',None)
    monkeypatch.setattr(simpledialog,'askstring',lambda *a,**k:'ui-owned')
    view.evaluate_protocol();assert view.pending and view.progress.winfo_manager()
    panel.pump(lambda:not view.pending,seconds=30)
    assert hasattr(view,'evaluation'),view.status.cget('text')
    assert view.evaluation['tests_counted']==len(report['channels'])*2*3
    assert not view.evaluation['predictive_qualification']
    out=tmp_path/'evaluation.html';monkeypatch.setattr(laboratory_links.filedialog,'asksaveasfilename',lambda **k:str(out))
    view.export();panel.pump(lambda:not view.pending,seconds=30)
    assert view.protocol_id in out.read_text(encoding='utf-8')
