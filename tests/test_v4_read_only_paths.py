"""V4-03: modo somente leitura abre exatamente o arquivo pedido, com qualquer nome válido."""
import json
import os
import sqlite3
from pathlib import Path

import pytest
from sqlalchemy import text

from mt5_extracao import cli
from mt5_extracao.analytics import query_service
from mt5_extracao.database_manager import DatabaseManager
from tests.test_v3_query_contract import FAR, SYMBOL, _prepared

NAMES = ["study #1.sqlite", "estudo ação ç.sqlite", "taxa 100%.sqlite", "q&x=1;y=2.sqlite", "dir com espaço/b#.sqlite"]
if os.name != "nt":
    NAMES.append("q?x=1.sqlite")  # '?' é válido em POSIX, não no Windows


def _opened(db):
    with db.engine.connect() as conn:
        return Path(conn.execute(text("PRAGMA database_list")).fetchone()[2]).resolve()


@pytest.mark.parametrize("name", NAMES)
def test_v4_03_read_only_opens_the_requested_file(tmp_path, name):
    path = tmp_path / name
    path.parent.mkdir(parents=True, exist_ok=True)
    _prepared(path).engine.dispose()
    before = sorted(p.name for p in path.parent.iterdir())
    reader = DatabaseManager(db_path=str(path), broker_utc_offset=0, read_only=True)
    assert _opened(reader) == path.resolve()
    with reader.engine.connect() as conn:
        with pytest.raises(Exception):
            conn.execute(text("CREATE TABLE intruso (x INTEGER)"))
        with pytest.raises(Exception):
            conn.execute(text("DELETE FROM _feature_values"))
    env = query_service.series(reader, symbol=SYMBOL, timeframe="M5", feature_ids=["price.return_1"], as_of=FAR)
    assert env["status"] == "ok" and len(env["values"]) == 18
    assert sorted(p.name for p in path.parent.iterdir()) == before   # nenhum arquivo criado


def test_v4_03_relative_path_and_missing_file(tmp_path, monkeypatch):
    path = tmp_path / "rel #2.sqlite"
    _prepared(path).engine.dispose()
    monkeypatch.chdir(tmp_path)
    reader = DatabaseManager(db_path="rel #2.sqlite", broker_utc_offset=0, read_only=True)
    assert _opened(reader) == path.resolve()
    missing = DatabaseManager(db_path=str(tmp_path / "nao existe #.sqlite"), read_only=True)
    assert not missing.is_connected() and not (tmp_path / "nao existe #.sqlite").exists()
    assert not any(p.name.startswith("nao existe") for p in tmp_path.iterdir())


def test_v4_03_cli_reads_the_requested_file(tmp_path, capsys):
    path = tmp_path / "cli #3.sqlite"
    _prepared(path).engine.dispose()
    os.chmod(path, 0o444)
    config = tmp_path / "cli.ini"
    config.write_text(f"[DATABASE]\ntype = sqlite\npath = {path}\n[APP]\nbroker_utc_offset = 0\n", encoding="utf-8")
    try:
        code = cli.main(["--config", str(config), "--json", "analytics", "features", "series", "--symbol", SYMBOL,
                         "--tf", "M5", "--feature", "price.return_1", "--as-of", FAR.isoformat()])
    finally:
        os.chmod(path, 0o644)
    out = json.loads(capsys.readouterr().out)
    assert code == 0 and len(out["data"]["envelope"]["values"]) == 18
    assert sorted(p.name for p in tmp_path.iterdir()) == sorted(["cli #3.sqlite", "cli.ini"])
