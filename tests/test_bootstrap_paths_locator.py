from pathlib import Path

import pytest

from mt5_extracao.bootstrap import mt5_locator, paths


def make_mt5(directory: Path) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / paths.MT5_TERMINAL_EXE).write_bytes(b"")
    return directory


def test_linux_to_wine_path_roundtrip(tmp_path):
    prefix = tmp_path / "wine"
    host = prefix / "drive_c" / "Program Files" / "MetaTrader 5"
    win = paths.linux_to_wine_path(host, prefix)
    assert win == r"C:\Program Files\MetaTrader 5"
    assert paths.wine_to_linux_path(win, prefix) == host


def test_linux_to_wine_path_outside_drive_c(tmp_path):
    with pytest.raises(ValueError):
        paths.linux_to_wine_path(tmp_path / "x", tmp_path / "wine")


def test_wine_to_linux_path_rejects_other_drives(tmp_path):
    with pytest.raises(ValueError):
        paths.wine_to_linux_path(r"D:\MT5", tmp_path)


def test_default_wine_prefix_env_override(tmp_path):
    assert paths.default_wine_prefix({"MT5X_WINEPREFIX": str(tmp_path)}) == tmp_path
    assert paths.default_wine_prefix({"XDG_DATA_HOME": str(tmp_path)}) == tmp_path / "mt5-extracao" / "wine"


def test_find_wine_installations_prefers_default_folder(tmp_path):
    prefix = tmp_path / "wine"
    pf = prefix / "drive_c" / "Program Files"
    make_mt5(pf / "Corretora XP MT5")
    make_mt5(pf / "MetaTrader 5")
    (pf / "Outro Programa").mkdir()
    found = mt5_locator.find_wine_installations([prefix])
    found.sort(key=mt5_locator._preference)
    assert [f.directory.name for f in found] == ["MetaTrader 5", "Corretora XP MT5"]
    assert found[0].windows_path == r"C:\Program Files\MetaTrader 5"
    assert found[0].wine_prefix == prefix


def test_find_wine_installations_ignores_missing_prefix(tmp_path):
    assert mt5_locator.find_wine_installations([tmp_path / "nao-existe"]) == []


def test_installation_from_path_accepts_exe_and_dir(tmp_path):
    prefix = tmp_path / "wine"
    d = make_mt5(prefix / "drive_c" / "Program Files" / "MetaTrader 5")
    by_dir = mt5_locator.installation_from_path(d, prefix)
    by_exe = mt5_locator.installation_from_path(d / paths.MT5_TERMINAL_EXE, prefix)
    assert by_dir is not None and by_exe is not None
    assert by_dir.directory == by_exe.directory == d


def test_installation_from_path_invalid(tmp_path):
    assert mt5_locator.installation_from_path(tmp_path) is None


@pytest.mark.skipif(paths.is_windows(), reason="prefixo Wine só se aplica ao Linux")
def test_installation_from_path_guesses_prefix(tmp_path):
    prefix = tmp_path / "pfx"
    d = make_mt5(prefix / "drive_c" / "Program Files" / "MetaTrader 5")
    inst = mt5_locator.installation_from_path(d)
    assert inst is not None and inst.wine_prefix == prefix


@pytest.mark.skipif(not paths.is_windows(), reason="somente Windows")
def test_find_windows_installations_scans_program_files(tmp_path):
    make_mt5(tmp_path / "MetaTrader 5")
    found = mt5_locator.find_windows_installations({"ProgramFiles": str(tmp_path)})
    assert any(f.directory == tmp_path / "MetaTrader 5" for f in found)
