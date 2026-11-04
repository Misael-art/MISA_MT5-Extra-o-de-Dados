"""Guia Tk lê avaliação gravada e aplica filtro sem reexecutar produtor."""

from tests.test_pattern_campaign import source
from tests.test_pattern_consumers import persisted
from tests.test_ui_laboratory import panel


def test_experiment_dialog_reads_pattern_receipt_and_filters_family(panel, source):
    from mt5_extracao.ui.experiments import ExperimentsDialog

    db, _ = source
    protocol, request, manifest, result = persisted(db)
    panel.ui.app.db_manager = db
    dialog = ExperimentsDialog(panel.ui)
    try:
        panel.pump(lambda: dialog.protocols.exists(protocol), 5)
        dialog.protocols.selection_set(protocol)
        dialog.select_protocol()
        panel.pump(lambda: bool(dialog.patterns.get_children()), 5)
        assert len(dialog.patterns.get_children()) == 1
        assert result["receipt"]["source_namespace"] in dialog.pattern_detail.get("1.0", "end")
        assert request in dialog.pattern_detail.get("1.0", "end")
        assert manifest in dialog.pattern_detail.get("1.0", "end")
        dialog.pattern_family.set("gap")
        dialog._render_patterns()
        assert dialog.patterns.get_children() == ()
        assert "Nenhuma avaliação" in dialog.pattern_detail.get("1.0", "end")
        dialog.pattern_family.set("reversal")
        dialog._render_patterns()
        assert len(dialog.patterns.get_children()) == 1
    finally:
        dialog.destroy()
