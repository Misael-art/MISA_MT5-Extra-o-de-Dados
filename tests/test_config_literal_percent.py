"""config.ini é lido sem interpolação: '%' em URL não derruba a CLI nem difere do doctor."""
import json

from mt5_extracao import cli, services


def test_percent_in_database_url_is_literal(tmp_path, capsys):
    config = tmp_path / "config.ini"
    config.write_text("[DATABASE]\ntype = sqlite\nurl = postgresql://u@h/db?options=-csearch_path%3Dqa\n"
                      f"path = {tmp_path / 'qa.db'}\n", encoding="utf-8")
    loaded = services.load_config(str(config))
    assert loaded.get("DATABASE", "url").endswith("search_path%3Dqa")
    assert cli.main(["--config", str(config), "--json", "tables"]) == 0
    assert json.loads(capsys.readouterr().out)["ok"] is True
