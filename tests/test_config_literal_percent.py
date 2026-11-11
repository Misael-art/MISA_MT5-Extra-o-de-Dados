"""config.ini é lido sem interpolação: '%' em URL não derruba a CLI nem difere do doctor."""
import json

import pytest

from mt5_extracao import cli, services


def test_percent_in_database_url_is_literal(tmp_path, capsys):
    config = tmp_path / "config.ini"
    config.write_text("[DATABASE]\ntype = sqlite\nurl = postgresql://u@h/db?options=-csearch_path%3Dqa\n"
                      f"path = {tmp_path / 'qa.db'}\n", encoding="utf-8")
    loaded = services.load_config(str(config))
    assert loaded.get("DATABASE", "url").endswith("search_path%3Dqa")
    assert cli.main(["--config", str(config), "--json", "tables"]) == 0
    assert json.loads(capsys.readouterr().out)["ok"] is True


@pytest.mark.parametrize("filename", ["qa%20.sqlite", "ação%cotação.sqlite"])
def test_gui_reads_literal_percent_and_utf8_without_touching_another_database(tmp_path, monkeypatch, filename):
    import app

    database = tmp_path / filename
    config = tmp_path / "qa.ini"
    config.write_text(f"[DATABASE]\ntype=sqlite\npath={database}\n"
                      "url=postgresql://u@h/db?options=-csearch_path%3Dqa\n", encoding="utf-8")
    errors = []
    monkeypatch.setattr(app.messagebox, "showerror", lambda *args: errors.append(args))
    instance = object.__new__(app.MT5Extracao)
    instance.config_path = str(config)
    try:
        assert instance.load_config_and_db(), errors
        assert instance.config.get("DATABASE", "url").endswith("search_path%3Dqa")
        assert instance.db_manager.db_path == str(database)
        assert database.exists() and not errors
    finally:
        if getattr(instance, "db_manager", None) is not None:
            instance.db_manager.close()
