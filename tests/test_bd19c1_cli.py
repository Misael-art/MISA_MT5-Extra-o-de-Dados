"""BD19c1: o primeiro consumidor (CLI) entrega exatamente o envelope do serviço público."""
import json

import pandas as pd

from mt5_extracao import cli, sdk
from tests.test_bd18a_query import NOW, _db, no_sleep  # noqa: F401
from tests.test_bd08a_returns import SYMBOL


def _config(tmp_path, db):
    config = tmp_path / "config.ini"
    config.write_text(f"[DATABASE]\ntype = sqlite\npath = {db.db_path}\n[APP]\nbroker_utc_offset = 0\n",
                      encoding="utf-8")
    return config


def test_cli_json_equals_sdk_envelope(connector, fake_mt5, tmp_path, capsys):
    db = _db(connector, tmp_path)
    config = _config(tmp_path, db)
    argv = ["--config", str(config), "--json", "analytics", "features", "series", "--symbol", SYMBOL, "--tf", "M5",
            "--feature", "price.return_1", "--feature", "price.return_5", "--as-of", NOW.isoformat()]
    assert cli.main(argv) == 0
    envelope = json.loads(capsys.readouterr().out)["data"]["envelope"]
    expected = sdk.query_series(db, symbol=SYMBOL, timeframe="M5", feature_ids=["price.return_1", "price.return_5"],
                                as_of=NOW)
    assert envelope["values"] == expected["values"] and envelope["base"] == expected["base"]
    assert envelope["definitions"] == json.loads(json.dumps(expected["definitions"]))


def test_cli_reports_structured_error(connector, fake_mt5, tmp_path, capsys):
    db = _db(connector, tmp_path)
    argv = ["--config", str(_config(tmp_path, db)), "analytics", "features", "series", "--symbol", SYMBOL,
            "--tf", "M5", "--feature", "price.return_1", "--as-of", "2026-01-01"]
    assert cli.main(argv) == cli.EXIT_USAGE
    assert "[NAIVE_TIME]" in capsys.readouterr().out
