"""Atalhos na área de trabalho e no menu de aplicativos (Linux; o caminho do Windows só é montado)."""
import os
from pathlib import Path

import pytest

from mt5_extracao.bootstrap import paths, shortcuts
from mt5_extracao.bootstrap.__main__ import main

pytestmark = pytest.mark.skipif(paths.is_windows(), reason="formato .desktop é do Linux")


@pytest.fixture
def home(tmp_path, monkeypatch):
    """Área de trabalho e menu dentro de uma pasta temporária."""
    desktop = tmp_path / "Área de trabalho"
    desktop.mkdir()
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "share"))
    monkeypatch.setattr(shortcuts, "desktop_dir", lambda env=None: desktop)
    return tmp_path


def test_icon_files_are_shipped():
    assert (shortcuts.ASSETS / "icon.png").is_file() and (shortcuts.ASSETS / "icon.ico").is_file()


def test_create_writes_desktop_and_menu_entries(home, tmp_path):
    root = tmp_path / "projeto com espaço"
    results = shortcuts.create(root)
    assert [err for _, err in results] == [None, None]
    desktop_file = home / "Área de trabalho" / shortcuts.LINUX_FILE
    menu_file = home / "share" / "applications" / shortcuts.LINUX_FILE
    assert {p for p, _ in results} == {desktop_file, menu_file}
    text = desktop_file.read_text(encoding="utf-8")
    assert f'Exec="{root / "run.sh"}"' in text and f"Icon={shortcuts.ASSETS / 'icon.png'}" in text
    assert "Terminal=false" in text and os.access(desktop_file, os.X_OK)
    # criar de novo não duplica nem falha
    assert shortcuts.create(root) == results and sorted(shortcuts.existing()) == sorted([desktop_file, menu_file])


def test_create_only_one_place_and_remove(home):
    shortcuts.create(Path("/x"), desktop=False)
    assert [p.parent.name for p in shortcuts.existing()] == ["applications"]
    assert len(shortcuts.remove()) == 1 and shortcuts.existing() == []


def test_shortcuts_command(home, capsys):
    assert main(["--root", str(home), "shortcuts"]) == 0
    assert capsys.readouterr().out.count("Atalho criado") == 2
    assert main(["shortcuts", "--remove"]) == 0
    assert "removido" in capsys.readouterr().out
