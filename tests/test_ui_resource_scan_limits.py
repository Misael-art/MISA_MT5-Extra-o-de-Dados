"""O inventário de pastas pode falhar sem esconder as cópias restauráveis."""
import time

from mt5_extracao.resources import files, storage
from mt5_extracao.ui import resources
from tests.test_ui_main_window import ui


def test_incomplete_folder_read_keeps_backup_restore_available_and_blocks_cleanup(ui, tmp_path, monkeypatch):
    config = ui.app.config
    config["RESOURCES"] = {"backup_dir": str(tmp_path / "copies"), "archive_dir": str(tmp_path / "archive")}
    backup = files.backup(ui.app.db_manager, config, prune=False)
    assert backup["ok"]

    def expired(*args, **kwargs):
        raise TimeoutError("prazo da leitura opcional")

    monkeypatch.setattr(storage, "dir_bytes", expired)
    monkeypatch.setattr(files, "old_files", expired)
    panel = ui.resources_panel
    panel.refresh()
    end = time.monotonic() + 10
    while panel.busy and time.monotonic() < end:
        ui.pump(.05)
    assert not panel.busy and panel.report is not None
    assert backup["path"] in panel.backup_tree.get_children()
    assert not panel.backup_button.instate(["disabled"])
    assert not panel.back_button.instate(["disabled"])
    assert panel.old_files_complete is False and panel.old_files == []
    assert "incompleta" in panel.files_summary.cget("text")
    assert panel.files_button.instate(["disabled"])

    def forbidden(*args, **kwargs):
        raise AssertionError("limpeza ou confirmação sobre lista incompleta")

    monkeypatch.setattr(resources.messagebox, "askyesno", forbidden)
    monkeypatch.setattr(panel, "_run_action", forbidden)
    panel.clean_files()
    ui.toggle_theme()
    panel._show_files()
    assert panel.files_button.instate(["disabled"])
    assert backup["path"] in panel.backup_tree.get_children()
