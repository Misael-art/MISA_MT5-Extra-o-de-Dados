"""Fase 15: gestão de recursos (medição, higienização, arquivo reversível, automação opt-in e CLI)."""
import configparser
import json
from datetime import datetime, timedelta

import pandas as pd
import pytest
from sqlalchemy import inspect, text

from mt5_extracao import asset_table, cli
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.resources import actions, automation, hygiene, policy, storage
from mt5_extracao.robots import store

NOW = datetime(2026, 10, 14, 12, 0)


def _bars(end, n=50):
    times = pd.date_range(end=end, periods=n, freq="D")
    return pd.DataFrame({"time": times, "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5,
                         "tick_volume": 100, "spread": 1, "real_volume": 0})


def _run(db, robot, symbol, net):
    store.save_run(db, {"run_id": f"{robot}-{symbol}", "robot_key": robot, "source": "backtest", "symbol": symbol,
                        "timeframe": "D1", "data_start": "2026-01-01", "data_end": "2026-10-01", "bars": 50,
                        "metrics": {"net_profit": net, "return_pct": net / 1000}, "equity": [],
                        "created_at": "2026-10-01 10:00:00"})


def _config(tmp_path, **resources):
    cfg = configparser.ConfigParser(interpolation=None)
    cfg.optionxform = str
    cfg["RESOURCES"] = {"archive_dir": str(tmp_path / "arquivo"), **{k: str(v) for k, v in resources.items()}}
    return cfg


@pytest.fixture
def db(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "base.db"))
    db.save_ohlcv_data("PETR4", "1 dia", _bars(NOW))           # recente, robô com lucro -> protegida
    db.save_ohlcv_data("VALE3", "1 dia", _bars(NOW))           # recente, robô sem lucro -> sem_lucro
    db.save_ohlcv_data("OLD3", "1 dia", _bars(NOW - timedelta(days=400)))   # obsoleta e sem estudo
    db.save_ohlcv_data("FAV3", "1 dia", _bars(NOW - timedelta(days=400)))   # obsoleta, mas favorita
    _run(db, "rob_a", "PETR4", 500.0)
    _run(db, "rob_a", "VALE3", -200.0)
    asset_table.save_favorites(db, ["FAV3"])
    yield db
    db.close()


def _table(db, symbol):
    return next(i["table"] for i in db.data_inventory() if i["symbol"] == symbol)


# --- política ------------------------------------------------------------------------------

def test_policy_defaults_are_all_off_and_bad_values_fall_back(tmp_path):
    p = policy.load(None)
    assert not p.auto_enabled and not p.auto_archive and not p.auto_vacuum and not p.auto_clean_orphans
    cfg = _config(tmp_path, auto_enabled="talvez", obsolete_days="-3", auto_archive_rules="obsoleta,xyz",
                  monitor_interval_minutes="1")
    p = policy.load(cfg)
    assert p.auto_enabled is False and p.obsolete_days == 180
    assert p.auto_archive_rules == ("obsoleta",)
    assert p.monitor_interval_minutes == 5            # mínimo
    assert len(p.errors) == 3


def test_new_keys_exist_in_defaults_and_example():
    from pathlib import Path
    from mt5_extracao.bootstrap import config_builder
    keys = set(config_builder.DEFAULTS["RESOURCES"])
    example = (Path(__file__).resolve().parents[1] / "config" / "config.ini.example").read_text(encoding="utf-8")
    assert "[RESOURCES]" in example
    for key in keys:
        assert f"\n{key} =" in example, key
    assert set(policy.Policy.__dataclass_fields__) - {"errors"} == keys


# --- medição -------------------------------------------------------------------------------

def test_measure_reports_sizes_kinds_disk_and_dirs(db, tmp_path):
    report = storage.measure(db, tmp_path / "arquivo")
    assert report["db_bytes"] > 0 and report["reclaimable_bytes"] >= 0
    assert report["disk"]["total"] > 0
    bars = [t for t in report["tables"] if t["kind"] == storage.KIND_BARS]
    assert {t["symbol"] for t in bars} == {"PETR4", "VALE3", "OLD3", "FAV3"}
    assert all(t["bytes"] and t["bytes"] > 0 and t["rows"] == 50 for t in bars)
    assert any(t["kind"] == storage.KIND_INTERNAL for t in report["tables"])
    assert set(report["dirs"]) >= {"Logs", "Exportações", "Arquivo"}


def test_measure_without_database_does_not_raise():
    report = storage.measure(None)
    assert report["db_bytes"] is None and report["warnings"]


def test_size_estimate_when_dbstat_is_missing():
    tables = [{"rows": 30, "bytes": None}, {"rows": 10, "bytes": None}, {"rows": 0, "bytes": 100}]
    storage._estimate_missing(tables, db_bytes=1100, reclaimable=200)
    assert tables[0]["bytes"] == 600 and tables[1]["bytes"] == 200 and tables[0]["estimated"]


def test_snapshots_and_growth(db):
    base = storage.measure(db, with_dirs=False)
    for days, extra in ((10, 0), (5, 5_000_000), (0, 10_000_000)):
        report = dict(base, measured_at=NOW - timedelta(days=days), db_bytes=base["db_bytes"] + extra,
                      disk={"free": 100_000_000, "total": 1_000_000_000})
        assert storage.record_snapshot(db, report, keep_days=3650)
    snaps = storage.load_snapshots(db)
    assert len(snaps) == 3
    g = storage.growth(snaps)
    assert g["bytes_per_day"] == pytest.approx(1_000_000)
    assert g["days_to_full"] == pytest.approx(100)
    assert storage.growth(snaps[:1]) is None


def test_human_bytes():
    assert storage.human_bytes(None) == "—"
    assert storage.human_bytes(512) == "512 B"
    assert storage.human_bytes(1536) == "1,5 KB"
    assert storage.human_bytes(3 * 1024 ** 3) == "3,0 GB"


# --- higienização --------------------------------------------------------------------------

def test_scan_finds_candidates_and_respects_protections(db, tmp_path):
    found = hygiene.scan(db, policy.load(_config(tmp_path)), now=NOW)
    by_symbol = {c["symbol"]: c for c in found["candidates"]}
    assert set(by_symbol) == {"VALE3", "OLD3"}
    assert set(by_symbol["VALE3"]["reasons"]) == {"sem_lucro"}
    assert set(by_symbol["OLD3"]["reasons"]) == {"sem_estudo", "obsoleta"}
    protected = {p["symbol"]: p for p in found["protected"]}
    assert "favorito" in protected["FAV3"]["protections"]
    assert "PETR4" not in protected            # protegida e sem nenhum motivo: nem aparece


def test_selection_and_benchmark_protect(db, tmp_path):
    asset_table.set_benchmark(db, "PETR4", "OLD3")         # OLD3 é benchmark de PETR4
    found = hygiene.scan(db, policy.load(_config(tmp_path)), selected=["VALE3"], now=NOW)
    assert found["candidates"] == []
    reasons = {p["symbol"]: p["protections"] for p in found["protected"]}
    assert "na seleção" in reasons["VALE3"]
    assert "usado como benchmark de outro ativo" in reasons["OLD3"]


def test_rules_can_be_limited_and_a_broken_rule_does_not_stop_the_scan(db, tmp_path, monkeypatch):
    monkeypatch.setitem(hygiene.RULES, "quebrada", lambda item, ctx: 1 / 0)
    found = hygiene.scan(db, policy.load(_config(tmp_path)), now=NOW, rules=("obsoleta", "quebrada"))
    assert [c["symbol"] for c in found["candidates"]] == ["OLD3"]


# --- arquivar e restaurar ------------------------------------------------------------------

def test_archive_and_restore_round_trip(db, tmp_path):
    cfg = _config(tmp_path)
    table = _table(db, "OLD3")
    with db.engine.connect() as conn:
        original = conn.execute(text(f'SELECT * FROM "{table}" ORDER BY time')).fetchall()
    [result] = actions.archive_tables(db, cfg, [table], origin="teste", reasons={table: {"obsoleta": "x"}})
    assert result["ok"] and result["rows"] == 50
    assert not inspect(db.engine).has_table(table)
    assert "OLD3" not in {i["symbol"] for i in db.data_inventory()}
    [arch] = actions.list_archives(db, cfg)
    assert arch["table"] == table and arch["rows"] == 50 and arch["symbol"] == "OLD3"
    assert arch["reasons"] == {"obsoleta": "x"}

    restored = actions.restore_archive(db, cfg, arch["path"], origin="teste")
    assert restored["ok"], restored
    with db.engine.connect() as conn:
        assert conn.execute(text(f'SELECT * FROM "{table}" ORDER BY time')).fetchall() == original
    assert db.find_table_for_symbol("OLD3", "1 dia") == table
    assert actions.list_archives(db, cfg) == []
    assert (tmp_path / "arquivo" / "restaurados").is_dir()       # o arquivo não é apagado
    log = actions.load_actions(db)
    assert [a["action"] for a in log] == ["restaurar", "arquivar"] and all(a["ok"] for a in log)


def test_restore_refuses_to_overwrite(db, tmp_path):
    cfg = _config(tmp_path)
    table = _table(db, "OLD3")
    actions.archive_tables(db, cfg, [table])
    db.save_ohlcv_data("OLD3", "1 dia", _bars(NOW))         # extraída de novo
    [arch] = actions.list_archives(db, cfg)
    result = actions.restore_archive(db, cfg, arch["path"])
    assert not result["ok"] and "já existe" in result["error"]
    assert actions.list_archives(db, cfg)                      # o arquivo continua lá


def test_archive_refuses_internal_and_missing_tables_and_keeps_going(db, tmp_path):
    results = actions.archive_tables(db, _config(tmp_path), ["_metadata", "nao_existe", _table(db, "OLD3")])
    assert [r["ok"] for r in results] == [False, False, True]


def test_failed_copy_check_removes_nothing(db, tmp_path, monkeypatch):
    table = _table(db, "OLD3")
    monkeypatch.setattr(actions, "_archive_one_sqlite", lambda *a: 49)
    [result] = actions.archive_tables(db, _config(tmp_path), [table])
    assert not result["ok"] and "nada foi removido" in result["error"]
    assert inspect(db.engine).has_table(table)
    assert not list((tmp_path / "arquivo").glob("*.db"))


def test_generic_copy_path_round_trip(db, tmp_path, monkeypatch):
    """Caminho do PostgreSQL (cópia em blocos com pandas), exercitado sobre o SQLite."""
    monkeypatch.setattr(actions, "CHUNK_ROWS", 7)
    table = _table(db, "OLD3")
    info = {"table": table, "rows": 50, "dialect": "postgresql", "controls": {}}
    target = tmp_path / "g.db"
    assert actions._archive_one_generic(db, table, target, info) == 50
    assert actions._read_info(target)["table"] == table


def test_lock_blocks_a_second_action(db, tmp_path):
    cfg = _config(tmp_path)
    with actions.exclusive(tmp_path / "arquivo", "teste"):
        with pytest.raises(actions.Busy):
            actions.archive_tables(db, cfg, [_table(db, "OLD3")])
    assert inspect(db.engine).has_table(_table(db, "OLD3"))


def test_vacuum_and_orphans(db, tmp_path):
    cfg = _config(tmp_path)
    result = actions.vacuum(db, cfg, origin="teste")
    assert result["ok"] and result["bytes_after"] is not None
    with db.engine.begin() as conn:
        conn.execute(text("INSERT INTO _symbol_tables VALUES ('X', 'diario', 'x_diario')"))
    assert hygiene.find_orphans(db)["symbol_tables"] == ["x_diario"]
    cleaned = actions.clean_orphans(db, cfg)
    assert cleaned["removed"]["symbol_tables"] == 1
    assert hygiene.find_orphans(db)["symbol_tables"] == []


def test_vacuum_refuses_without_disk(db, tmp_path, monkeypatch):
    monkeypatch.setattr(storage, "disk_usage", lambda p: {"free": 1, "total": 10, "used": 9, "free_pct": 10.0,
                                                          "path": str(p)})
    result = actions.vacuum(db, _config(tmp_path))
    assert not result["ok"] and "disco livre" in result["error"]


# --- automação -----------------------------------------------------------------------------

def test_cycle_does_nothing_without_opt_in(db, tmp_path):
    out = automation.run_cycle(db, _config(tmp_path, auto_archive="true"), now=NOW)
    assert out["actions"] == []
    assert inspect(db.engine).has_table(_table(db, "OLD3"))
    assert storage.load_snapshots(db)                         # mas mede
    assert automation.last_cycle(db) is None


def test_cycle_with_opt_in_archives_only_allowed_rules(db, tmp_path):
    sent = []
    cfg = _config(tmp_path, auto_enabled="true", auto_archive="true", auto_archive_rules="obsoleta",
                  auto_clean_orphans="true")
    out = automation.run_cycle(db, cfg, now=NOW, notify_fn=lambda *a: sent.append(a))
    archived = [r["table"] for d in out["actions"] if d["action"] == "arquivar" for r in d["result"] if r["ok"]]
    assert archived == [_table_name("OLD3")]                  # FAV3 é favorita; VALE3 não é obsoleta
    assert any("arquivada" in a[1] for a in sent)
    assert automation.last_cycle(db) == NOW
    assert not automation.is_due(policy.load(cfg), NOW, NOW + timedelta(minutes=30))
    assert automation.is_due(policy.load(cfg), NOW, NOW + timedelta(minutes=61))
    assert not automation.is_due(policy.load(_config(tmp_path)), None)


def _table_name(symbol):
    return f"{symbol.lower()}_1_dia"


def test_alerts():
    p = policy.Policy(alert_db_gb=1, alert_disk_free_pct=10, vacuum_min_mb=1)
    report = {"db_bytes": 2 * 1024 ** 3, "disk": {"free_pct": 5.0, "free": 100}, "reclaimable_bytes": 5 * 1024 ** 2}
    assert {a["code"] for a in automation.alerts(report, p)} == {"base_grande", "disco_cheio", "recuperavel"}
    assert automation.alerts({"db_bytes": 1, "disk": {"free_pct": 50.0, "free": 1}}, p) == []


# --- CLI -----------------------------------------------------------------------------------

def _cli(tmp_path, db, *args):
    cfg = tmp_path / "config.ini"
    cfg.write_text(f"[DATABASE]\ntype = sqlite\npath = {db.db_path}\n[RESOURCES]\narchive_dir = "
                   f"{tmp_path / 'arquivo'}\n", encoding="utf-8")
    return cli.main(["--config", str(cfg), "--json", "resources", *args])


def test_cli_status_scan_archive_restore(db, tmp_path, capsys):
    assert _cli(tmp_path, db, "status") == cli.EXIT_OK
    data = json.loads(capsys.readouterr().out.strip().splitlines()[-1])["data"]
    assert data["db_bytes"] > 0
    assert _cli(tmp_path, db, "scan") == cli.EXIT_OK
    data = json.loads(capsys.readouterr().out.strip().splitlines()[-1])["data"]
    assert {c["symbol"] for c in data["candidates"]} >= {"VALE3"}
    table = _table(db, "VALE3")
    assert _cli(tmp_path, db, "archive", table) == cli.EXIT_USAGE          # sem --yes não mexe
    capsys.readouterr()
    assert inspect(db.engine).has_table(table)
    assert _cli(tmp_path, db, "archive", table, "--yes") == cli.EXIT_OK
    capsys.readouterr()
    assert not inspect(db.engine).has_table(table)
    assert _cli(tmp_path, db, "archives") == cli.EXIT_OK
    [arch] = json.loads(capsys.readouterr().out.strip().splitlines()[-1])["data"]["archives"]
    assert _cli(tmp_path, db, "restore", arch["path"], "--yes") == cli.EXIT_OK
    capsys.readouterr()
    assert inspect(db.engine).has_table(table)
    assert _cli(tmp_path, db, "history") == cli.EXIT_OK


# --- cópias e arquivos antigos (T15.7) -----------------------------------------------------

def test_backup_is_checked_and_pruned(db, tmp_path):
    from mt5_extracao.resources import files
    cfg = _config(tmp_path, backup_dir=str(tmp_path / "copias"), backup_keep="2")
    paths = []
    for i in range(3):
        result = files.backup(db, cfg, origin="teste")
        assert result["ok"], result
        paths.append(result["path"])
        import os, time
        os.utime(result["path"], (time.time() - (10 - i) * 60,) * 2)    # horários distintos
    left = [b["path"] for b in files.list_backups(db, cfg)]
    assert len(left) == 2 and paths[0] not in left
    copy = DatabaseManager(db_path=left[0])
    assert {i["symbol"] for i in copy.data_inventory()} == {"PETR4", "VALE3", "OLD3", "FAV3"}
    copy.close()
    assert not files.backup_due(db, _config(tmp_path, backup_dir=str(tmp_path / "copias")))


def test_backup_refuses_without_disk(db, tmp_path, monkeypatch):
    from mt5_extracao.resources import files
    monkeypatch.setattr(storage, "disk_usage", lambda p: {"free": 1, "total": 10, "used": 9, "free_pct": 10.0,
                                                          "path": str(p)})
    result = files.backup(db, _config(tmp_path, backup_dir=str(tmp_path / "copias")))
    assert not result["ok"] and "disco livre" in result["error"]
    assert not list((tmp_path / "copias").glob("*"))


def test_old_files_keep_the_active_log_and_respect_zero(db, tmp_path, monkeypatch):
    import os
    from mt5_extracao.resources import files
    logs, exports = tmp_path / "logs", tmp_path / "exports"
    logs.mkdir(); exports.mkdir()
    for path in (logs / "mt5_extracao.log", logs / "mt5_extracao.log.1", exports / "a.csv"):
        path.write_text("x")
        old = (NOW - timedelta(days=90)).timestamp()
        os.utime(path, (old, old))
    monkeypatch.setitem(storage.EXTRA_DIRS, "Logs", lambda db, archive: logs)
    monkeypatch.setitem(storage.EXTRA_DIRS, "Exportações", lambda db, archive: exports)
    found = files.old_files(db, _config(tmp_path), now=NOW)              # exportações: 0 = nunca
    assert [f["path"] for f in found] == [str(logs / "mt5_extracao.log.1")]
    found = files.old_files(db, _config(tmp_path, exports_keep_days=30), now=NOW)
    assert len(found) == 2
    result = files.clean_files(db, _config(tmp_path, exports_keep_days=30), now=NOW)
    assert result["removed"] == 2 and (logs / "mt5_extracao.log").exists()


def test_cycle_makes_backup_only_with_opt_in(db, tmp_path):
    from mt5_extracao.resources import files
    cfg = _config(tmp_path, auto_backup="true", backup_dir=str(tmp_path / "copias"))
    automation.run_cycle(db, cfg, now=NOW)
    assert files.list_backups(db, cfg) == []
    cfg["RESOURCES"]["auto_enabled"] = "true"
    out = automation.run_cycle(db, cfg, now=NOW)
    assert [d["action"] for d in out["actions"]] == ["copiar"]
    assert len(files.list_backups(db, cfg)) == 1


def test_cli_backup_and_files(db, tmp_path, capsys):
    assert _cli(tmp_path, db, "backup") == cli.EXIT_USAGE
    assert _cli(tmp_path, db, "backups") == cli.EXIT_OK
    assert _cli(tmp_path, db, "files") == cli.EXIT_OK
