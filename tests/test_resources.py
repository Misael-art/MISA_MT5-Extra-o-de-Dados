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


def _run(db, robot, symbol, net, run_id=None, created="2026-10-01 10:00:00"):
    store.save_run(db, {"run_id": run_id or f"{robot}-{symbol}", "robot_key": robot, "source": "backtest", "symbol": symbol,
                        "timeframe": "D1", "data_start": "2026-01-01", "data_end": "2026-10-01", "bars": 50,
                        "metrics": {"net_profit": net, "return_pct": net / 1000}, "equity": [],
                        "created_at": created})


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


def test_any_positive_run_ever_is_a_positive_benchmark(db, tmp_path):
    """Benchmark positivo: qualquer backtest do ativo, de qualquer robô ou configuração, em qualquer data, com lucro."""
    _run(db, "rob_b", "VALE3", 50.0, run_id="antigo", created="2025-01-01 10:00:00")   # antigo, outro robô
    _run(db, "rob_a", "VALE3", -300.0, run_id="novo", created="2026-10-10 10:00:00")    # o mais recente perde
    found = hygiene.scan(db, policy.load(_config(tmp_path)), now=NOW)
    assert "VALE3" not in {c["symbol"] for c in found["candidates"]}
    reasons = hygiene.build_context(db, 180, now=NOW)
    assert reasons.best["VALE3"] == ("rob_b", 50.0) and reasons.tested["VALE3"] == 3


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


# --- T15.8: parquet, aparar, limite por série, voltar a uma cópia ---------------------------

def _rows(db, table):
    with db.engine.connect() as conn:
        return conn.execute(text(f'SELECT * FROM "{table}" ORDER BY time')).fetchall()


def test_parquet_archive_round_trip(db, tmp_path):
    pytest.importorskip("pyarrow")
    cfg = _config(tmp_path, archive_format="parquet")
    table = _table(db, "OLD3")
    original = _rows(db, table)
    [result] = actions.archive_tables(db, cfg, [table])
    assert result["ok"] and result["archive"].endswith(".parquet")
    [arch] = actions.list_archives(db, cfg)
    assert arch["format"] == "parquet" and arch["rows"] == 50 and arch["symbol"] == "OLD3"
    assert actions.restore_archive(db, cfg, arch["path"])["ok"]
    assert _rows(db, table) == original
    assert db.find_table_for_symbol("OLD3", "1 dia") == table


def test_trim_keeps_recent_rows_and_restores_the_rest(db, tmp_path):
    cfg = _config(tmp_path)
    table = _table(db, "PETR4")                    # 50 barras diárias até NOW
    original = _rows(db, table)
    [result] = actions.trim_tables(db, cfg, {table: 20}, origin="teste", now=NOW)
    assert result["ok"] and result["rows"] == 29          # a barra exatamente no corte fica
    assert len(_rows(db, table)) == 21
    [arch] = actions.list_archives(db, cfg)
    assert arch["kind"] == "trim" and arch["keep_days"] == 20
    assert actions.restore_archive(db, cfg, arch["path"])["ok"]
    assert _rows(db, table) == original
    # nada a aparar: ok, sem arquivo
    [again] = actions.trim_tables(db, cfg, {table: 3650}, now=NOW)
    assert again["ok"] and again["rows"] == 0 and again["archive"] is None


def test_trim_generic_path_and_parquet(db, tmp_path):
    pytest.importorskip("pyarrow")
    cfg = _config(tmp_path, archive_format="parquet")
    table = _table(db, "VALE3")
    [result] = actions.trim_tables(db, cfg, {table: 10}, now=NOW)
    assert result["ok"] and result["rows"] == 39 and len(_rows(db, table)) == 11
    assert actions.restore_archive(db, cfg, result["archive"])["ok"]
    assert len(_rows(db, table)) == 50


def test_trim_restore_needs_the_table(db, tmp_path):
    cfg = _config(tmp_path)
    table = _table(db, "OLD3")
    [trim] = actions.trim_tables(db, cfg, {table: 10}, now=NOW - timedelta(days=400))
    actions.archive_tables(db, cfg, [table])
    result = actions.restore_archive(db, cfg, trim["archive"])
    assert not result["ok"] and "restaure antes a série inteira" in result["error"]


def test_retention_plan_and_auto_trim(db, tmp_path):
    db.save_ohlcv_data("WIN", "1 minuto", _bars(NOW).assign(time=pd.date_range(end=NOW, periods=50, freq="h")))
    cfg = _config(tmp_path, retention_intraday_days="1", auto_enabled="true", auto_trim="true")
    report = storage.measure(db, with_dirs=False)
    assert automation.retention_plan(report, policy.load(cfg)) == {"win_1_minuto": 1}
    out = automation.run_cycle(db, cfg, now=NOW)
    [trim] = [d for d in out["actions"] if d["action"] == "aparar"]
    assert trim["result"][0]["rows"] > 0
    assert len(_rows(db, "win_1_minuto")) < 50


def test_series_size_alert():
    p = policy.Policy(max_series_mb=1)
    report = {"db_bytes": 1, "tables": [{"table": "x", "kind": storage.KIND_BARS, "bytes": 2 * 1024 ** 2},
                                        {"table": "_y", "kind": storage.KIND_INTERNAL, "bytes": 9 * 1024 ** 2}]}
    [alert] = automation.alerts(report, p)
    assert alert["code"] == "serie_grande" and "x (" in alert["text"] and "_y" not in alert["text"]


def test_invalid_archive_format_falls_back(tmp_path):
    p = policy.load(_config(tmp_path, archive_format="zip"))
    assert p.archive_format == "sqlite" and p.errors


def test_restore_backup_keeps_a_safety_copy(db, tmp_path):
    from mt5_extracao.resources import files
    cfg = _config(tmp_path, backup_dir=str(tmp_path / "copias"), backup_keep="1")
    first = files.backup(db, cfg)
    assert first["ok"]
    actions.archive_tables(db, cfg, [_table(db, "OLD3")])
    assert "OLD3" not in {i["symbol"] for i in db.data_inventory()}
    result = files.restore_backup(db, cfg, first["path"], origin="teste")
    assert result["ok"], result
    assert "OLD3" in {i["symbol"] for i in db.data_inventory()}
    from pathlib import Path
    assert Path(first["path"]).exists() and Path(result["safety_copy"]).exists()   # nada apagado (keep=1)


def test_restore_backup_refuses_a_broken_copy(db, tmp_path):
    from mt5_extracao.resources import files
    broken = tmp_path / "quebrada.db"
    broken.write_bytes(b"isto nao e um sqlite" * 100)
    result = files.restore_backup(db, _config(tmp_path, backup_dir=str(tmp_path / "copias")), broken)
    assert not result["ok"]
    assert "OLD3" in {i["symbol"] for i in db.data_inventory()}


def test_cli_trim_needs_yes(db, tmp_path, capsys):
    table = _table(db, "PETR4")
    assert _cli(tmp_path, db, "trim", table, "--days", "10") == cli.EXIT_USAGE
    assert len(_rows(db, table)) == 50
    assert _cli(tmp_path, db, "trim", table, "--days", "10", "--yes") == cli.EXIT_OK
    assert len(_rows(db, table)) <= 11


# --- T15.9: limites que bloqueiam, retenção por ativo, TimescaleDB ---------------------------

def test_guard_is_off_by_default_and_blocks_when_disk_is_low(db, tmp_path, monkeypatch):
    from mt5_extracao.resources import guard
    assert guard.make(db, _config(tmp_path)) is None
    monkeypatch.setattr(storage, "disk_usage", lambda p: {"free": 1, "total": 100, "used": 99, "free_pct": 1.0,
                                                          "path": str(p)})
    g = guard.make(db, _config(tmp_path, block_disk_free_pct="5"))
    assert "disco com só 1.0% livre" in g()
    assert guard.make(db, _config(tmp_path, block_db_gb="0.000001"))() .startswith("a base ocupa")
    assert guard.make(db, _config(tmp_path, block_db_gb="100"))() is None


def test_guard_is_cached(db, tmp_path, monkeypatch):
    from mt5_extracao.resources import guard
    calls = []
    monkeypatch.setattr(guard, "check", lambda db, p: calls.append(1))
    g = guard.make(db, _config(tmp_path, block_db_gb="1"), interval=3600)
    g(); g(); g()
    assert len(calls) == 1


def test_guard_checks_on_first_call_even_right_after_boot(db, tmp_path, monkeypatch):
    """time.monotonic() conta desde o boot: numa máquina recém-ligada é menor que o intervalo."""
    from mt5_extracao.resources import guard
    monkeypatch.setattr(guard.time, "monotonic", lambda: 5.0)
    monkeypatch.setattr(guard, "check", lambda db, p: "bloqueado")
    assert guard.make(db, _config(tmp_path, block_db_gb="1"), interval=30)() == "bloqueado"


def test_extractor_stops_before_writing_when_blocked(db):
    import pandas as pd
    from mt5_extracao.historical_extractor import HistoricalExtractor
    ext = HistoricalExtractor(connector=None, db_manager=db, indicator_calculator=None)
    ext.write_guard = lambda: "disco cheio"
    saved, _ = ext._save_block("NEW3", "1 dia", "new3_1_dia", _bars(NOW, 5), pd.Timestamp(NOW - timedelta(days=5)),
                               include_indicators=False)
    assert saved is None and ext.cancel_requested
    assert "NEW3" not in {i["symbol"] for i in db.data_inventory()}


def test_services_wire_the_guard(db, tmp_path):
    from mt5_extracao import services
    ext = services.create_extractor(_config(tmp_path, block_db_gb="5"), None, db, None)
    assert callable(ext.write_guard)
    assert services.create_extractor(_config(tmp_path), None, db, None).write_guard is None


def test_retention_overrides_per_asset(db, tmp_path):
    hourly = lambda: _bars(NOW).assign(time=pd.date_range(end=NOW, periods=50, freq="h"))  # noqa: E731
    db.save_ohlcv_data("WIN$N", "1 minuto", hourly())
    db.save_ohlcv_data("WDO$N", "1 minuto", hourly())
    db.save_ohlcv_data("IND$N", "1 minuto", hourly())
    with db.engine.begin() as conn:
        conn.execute(text("CREATE TABLE win_n_ticks (time TEXT, time_msc BIGINT, bid REAL)"))
    cfg = _config(tmp_path, retention_intraday_days="10", retention_ticks_days="5",
                  retention_overrides="WIN$N:2, WDO$N:0, lixo")
    p = policy.load(cfg)
    assert p.retention_overrides == {"WIN$N": 2, "WDO$N": 0} and len(p.errors) == 1
    plan = automation.retention_plan(storage.measure(db, with_dirs=False), p)
    assert plan["win_n_1_minuto"] == 2 and plan["win_n_ticks"] == 2
    assert "wdo_n_1_minuto" not in plan                     # 0 = guarda tudo desse ativo
    assert plan["ind_n_1_minuto"] == 10
    assert not any(t.endswith("_1_dia") for t in plan)      # diário nunca entra


def test_timescale_drop_chunks_is_used_only_for_hypertables(monkeypatch):
    class Conn:
        def __init__(self):
            self.sql, self.counts = [], [100, 60]
        def execute(self, stmt, params=None):
            sql = str(stmt)
            self.sql.append(sql)
            value = 1 if "hypertables" in sql else (self.counts.pop(0) if "COUNT" in sql else None)
            return type("R", (), {"scalar": lambda self: value})()
    class DB:
        dialect, _timescale = "postgresql", True
    conn = Conn()
    assert actions._drop_old_chunks(DB(), conn, "win_ticks", NOW) == 40
    assert any("drop_chunks" in s for s in conn.sql)
    DB._timescale = False
    assert actions._drop_old_chunks(DB(), Conn(), "win_ticks", NOW) == 0
