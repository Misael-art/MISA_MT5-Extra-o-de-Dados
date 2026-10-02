"""T5.6: pasta de dados e caminhos independentes do diretório atual."""
import configparser
import os

import pytest

from mt5_extracao import cli, services


@pytest.fixture
def fake_project(tmp_path, monkeypatch):
    root = tmp_path / "projeto"
    (root / "config").mkdir(parents=True)
    monkeypatch.setattr(services, "project_root", lambda: str(root))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "xdg"))
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "xdg"))
    elsewhere = tmp_path / "outra_pasta"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    return root


def cfg(text=""):
    c = configparser.ConfigParser()
    c.read_string(text)
    return c


def test_relative_db_goes_to_project_not_cwd(fake_project):
    path = services.resolve_db_path(cfg("[DATABASE]\npath = database/mt5_data.db\n"))
    assert path == os.path.join(str(fake_project), "database", "mt5_data.db")
    assert services.resolve_db_path(cfg()) == path                       # sem a chave: mesmo padrão


def test_absolute_and_explicit_paths_are_kept(fake_project, tmp_path):
    absolute = str(tmp_path / "x.db")
    assert services.resolve_db_path(cfg(f"[DATABASE]\npath = {absolute}\n")) == absolute
    assert services.resolve_db_path(cfg(), db_path="rel.db") == "rel.db"


def test_data_dir_auto_and_custom(fake_project, tmp_path):
    auto = services.resolve_db_path(cfg("[APP]\ndata_dir = auto\n"))
    assert auto == os.path.join(services.user_data_dir(), "database", "mt5_data.db")
    assert services.user_data_dir().startswith(str(tmp_path / "xdg"))
    custom = services.resolve_db_path(cfg(f"[APP]\ndata_dir = {tmp_path / 'dados'}\n"))
    assert custom == os.path.join(str(tmp_path / "dados"), "database", "mt5_data.db")


def test_existing_project_db_is_kept_when_data_dir_is_set(fake_project):
    old = fake_project / "database" / "mt5_data.db"
    old.parent.mkdir()
    old.write_bytes(b"")
    assert services.resolve_db_path(cfg("[APP]\ndata_dir = auto\n")) == str(old)


def test_db_manager_uses_resolved_path(fake_project):
    db = services.create_db_manager(cfg("[DATABASE]\ntype = sqlite\npath = database/novo.db\n"))
    assert db.is_connected()
    assert (fake_project / "database" / "novo.db").exists()
    assert not os.path.exists("database")                               # nada criado no diretório atual


def test_cli_finds_project_config_from_any_folder(fake_project, capsys):
    (fake_project / "config" / "config.ini").write_text(
        "[DATABASE]\ntype = sqlite\npath = database/mt5_data.db\n", encoding="utf-8")
    assert cli.main(["tables"]) == 0
    assert "0 tabela(s)" in capsys.readouterr().out
    assert (fake_project / "database" / "mt5_data.db").exists()
