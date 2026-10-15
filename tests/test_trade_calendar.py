"""T16.3a: calendário econômico de várias fontes (CSV e exportação do MT5), unido e enriquecido."""
import configparser
import json

import pandas as pd
import pytest
from sqlalchemy import text

from mt5_extracao import cli
from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.trade_analytics import calendar as cal
from mt5_extracao.trade_analytics.calendar import mt5_export

MT5_FILE = ("# server_utc_offset_sec=7200\r\n"
            "time;event_id;name;country;currency;importance;actual;forecast;previous\r\n"
            "2026.01.09 10:30;840030016;Nonfarm Payrolls;US;USD;3;256.000000;160.000000;212.000000\r\n"
            "2026.01.09 10:30;840030005;Unemployment Rate;US;USD;3;4.100000;4.200000;;\r\n"
            "2026.01.10 09:00;76020001;IPCA;BR;BRL;2;;0.300000;0.390000\r\n")
CSV_OLD = "time,symbol,impact\n2026-01-09 10:30,USD,alto\n2026-01-12 15:00,*,baixo\n"
CSV_NEW = ("time,symbol,impact,name,currency,override\n"
           "2026-01-09 10:30,USD,medio,Nonfarm Payrolls,USD,1\n"
           "2026-01-10 09:00,WIN$N,alto,IPCA,BRL,0\n")


@pytest.fixture
def db(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / "base.db"))
    yield db
    db.close()


def _write(tmp_path, name, text):
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return path


def test_mt5_export_is_read(tmp_path):
    rows = mt5_export.read_export(_write(tmp_path, "MT5X_calendar_Demo.csv", MT5_FILE))
    assert len(rows) == 3 and rows[0]["event_time"] == pd.Timestamp("2026-01-09 10:30")
    assert rows[0]["impact"] == cal.HIGH and rows[0]["actual"] == 256.0 and rows[2]["actual"] is None
    assert rows[1]["previous"] is None and rows[2]["currency"] == "BRL"


def test_mt5_export_reads_explicit_source_received_timestamp(tmp_path):
    content = ("time;event_id;name;country;currency;importance;actual;forecast;previous;source_received_at\n"
               "2026.01.09 10:30;840030016;Nonfarm Payrolls;US;USD;3;256;160;212;"
               "2026-01-09T10:31:00+02:00\n")
    row = mt5_export.read_export(_write(tmp_path, "MT5X_calendar_Received.csv", content))[0]
    assert row["source_received_at"] == "2026-01-09T08:31:00+00:00"


def test_old_csv_keeps_working(tmp_path):
    rows = cal.read_csv_file(_write(tmp_path, "e.csv", CSV_OLD))
    assert [r["impact"] for r in rows] == [cal.HIGH, cal.LOW]
    assert rows[0]["currency"] == "USD" and rows[1]["symbol"] == "*"
    with pytest.raises(ValueError):
        cal.read_csv_file(_write(tmp_path, "ruim.csv", "time,impact\n2026-01-01,alto\n"))


def test_import_is_idempotent_per_source(db, tmp_path):
    assert cal.import_source(db, None, "mt5", _write(tmp_path, "m.csv", MT5_FILE)) == 3
    cal.import_source(db, None, "mt5", tmp_path / "m.csv")
    assert len(cal.load(db)) == 3
    cfg = configparser.ConfigParser()
    cfg["SCREENER"] = {"events_file": str(_write(tmp_path, "e.csv", CSV_OLD))}
    assert cal.import_source(db, cfg, "csv") == 2
    assert set(cal.status(db)) == {"csv", "mt5"}
    with pytest.raises(ValueError):
        cal.import_source(db, None, "site")


def test_revision_history_is_idempotent_and_queries_as_of_observation(db):
    first_seen = pd.Timestamp("2026-10-10T12:00:00Z")
    second_seen = pd.Timestamp("2026-10-11T12:00:00Z")
    later_reimport = pd.Timestamp("2026-10-12T12:00:00Z")
    event = {"event_time": pd.Timestamp("2020-01-09 10:30"), "event_id": "release-1",
             "name": "Employment report", "country": "US", "currency": "USD", "symbol": "USD",
             "impact": cal.HIGH, "actual": 256.0, "forecast": 160.0, "previous": 212.0}
    key = cal._key("csv", event)
    with pytest.raises(ValueError, match="fuso horário"):
        cal.load_asof(db, pd.Timestamp("2026-10-10 12:00:00"))

    assert cal.save(db, "csv", [event], observed_at=first_seen) == 1
    assert cal.load_asof(db, pd.Timestamp("2020-01-10T00:00:00Z")).empty
    before_revision = cal.load_asof(db, second_seen - pd.Timedelta(seconds=1))
    assert before_revision["actual"].tolist() == [256.0]
    first_revision = cal.load_revisions(db, event_key=key).iloc[0]
    assert first_revision["revision_no"] == 1 and first_revision["observed_at"] == first_seen

    corrected = {**event, "actual": 300.0}
    assert cal.save(db, "csv", [corrected], observed_at=second_seen) == 1
    assert cal.load_asof(db, second_seen - pd.Timedelta(seconds=1))["actual"].tolist() == [256.0]
    assert cal.load_asof(db, second_seen)["actual"].tolist() == [300.0]
    assert cal.merged_asof(db, second_seen - pd.Timedelta(seconds=1))["actual"].tolist() == [256.0]
    assert cal.merged_asof(db, second_seen)["actual"].tolist() == [300.0]

    cal.save(db, "csv", [corrected], observed_at=later_reimport)
    revisions = cal.load_revisions(db, event_key=key)
    assert revisions["revision_no"].tolist() == [1, 2]
    assert cal.load(db).iloc[0]["first_observed_at"] == first_seen


def test_legacy_import_time_is_not_inferred_as_availability(db):
    old_columns = {name: kind for name, kind in cal.COLUMNS.items()
                   if name not in {"first_observed_at", "source_received_at"}}
    event = {"event_time": pd.Timestamp("2020-01-09 10:30").to_pydatetime(), "event_id": "legacy-1",
             "name": "Historical report", "country": "US", "currency": "USD", "symbol": "USD",
             "impact": cal.HIGH, "actual": 1.5, "forecast": 1.0, "previous": 1.2,
             "override": 0, "source": "csv", "imported_at": "2026-01-09 12:00:00"}
    event["event_key"] = cal._key("csv", event)
    names = list(old_columns)
    with db.engine.begin() as conn:
        ddl = ", ".join(f"{name} {kind}" for name, kind in old_columns.items())
        conn.execute(text(f"CREATE TABLE {cal.TABLE} ({ddl})"))
        conn.execute(text(f"INSERT INTO {cal.TABLE} ({', '.join(names)}) "
                          f"VALUES ({', '.join(':' + name for name in names)})"), event)

    assert cal.load(db)["actual"].tolist() == [1.5]
    assert cal.load_asof(db, pd.Timestamp("2026-12-01T00:00:00Z")).empty
    assert pd.isna(cal.merged(db).iloc[0]["surprise_available_at"])

    incoming = {name: value for name, value in event.items() if name not in {"event_key", "source", "imported_at"}}
    cal.save(db, "csv", [incoming], observed_at=pd.Timestamp("2026-10-10T12:00:00Z"))
    assert cal.load(db)["actual"].tolist() == [1.5]
    assert cal.load_asof(db, pd.Timestamp("2026-12-01T00:00:00Z")).empty
    legacy_revision = cal.load_revisions(db, event_key=event["event_key"]).iloc[0]
    assert pd.isna(legacy_revision["available_at"])


def test_asof_date_filters_apply_after_latest_event_time_revision(db):
    first_seen = pd.Timestamp("2026-10-10T12:00:00Z")
    corrected_seen = pd.Timestamp("2026-10-11T12:00:00Z")
    event = {"event_time": pd.Timestamp("2020-01-09 10:30"), "event_id": "corrected-time-1",
             "name": "Rescheduled release", "country": "US", "currency": "USD", "symbol": "USD",
             "impact": cal.HIGH, "actual": 1.0, "forecast": 0.5, "previous": 0.8}
    cal.save(db, "csv", [event], observed_at=first_seen)
    moved = {**event, "event_time": pd.Timestamp("2020-02-09 10:30"), "actual": 1.5}
    cal.save(db, "csv", [moved], observed_at=corrected_seen)

    old_range = {"start": pd.Timestamp("2020-01-01"), "end": pd.Timestamp("2020-02-01")}
    new_range = {"start": pd.Timestamp("2020-02-01"), "end": pd.Timestamp("2020-03-01")}
    before_correction = cal.load_asof(db, corrected_seen - pd.Timedelta(seconds=1), **old_range)
    after_correction_old_range = cal.load_asof(db, corrected_seen, **old_range)
    after_correction_new_range = cal.load_asof(db, corrected_seen, **new_range)

    assert before_correction["event_time"].tolist() == [pd.Timestamp("2020-01-09 10:30")]
    assert after_correction_old_range.empty
    assert after_correction_new_range["event_time"].tolist() == [pd.Timestamp("2020-02-09 10:30")]


def test_csv_source_received_at_requires_timezone_and_normalizes_to_utc(tmp_path):
    aware = _write(tmp_path, "received.csv",
                    "time,symbol,impact,source_received_at\n"
                    "2020-01-09 10:30,USD,alto,2020-01-09T10:32:00-03:00\n")
    row = cal.read_csv_file(aware)[0]
    assert row["source_received_at"] == "2020-01-09T13:32:00+00:00"
    naive = _write(tmp_path, "received-naive.csv",
                   "time,symbol,impact,source_received_at\n"
                   "2020-01-09 10:30,USD,alto,2020-01-09 10:32:00\n")
    with pytest.raises(ValueError, match="fuso horário"):
        cal.read_csv_file(naive)


def test_merge_unites_sources_and_enriches(db, tmp_path):
    cal.import_source(db, None, "mt5", _write(tmp_path, "m.csv", MT5_FILE))
    cal.import_source(db, None, "csv", _write(tmp_path, "e.csv", CSV_NEW))
    events = cal.merged(db)
    nfp = events[events["name"] == "Nonfarm Payrolls"].iloc[0]
    assert nfp["sources"] == "csv,mt5"                       # o mesmo evento das duas fontes vira uma linha
    assert nfp["impact"] == cal.MEDIUM                        # marcado à mão no CSV vence o do MT5
    assert nfp["surprise"] == pytest.approx(96.0)             # real − previsto
    assert pd.Timestamp(nfp["surprise_available_at"]).tzinfo is not None  # disponibilidade tem fuso explícito
    ipca = events[events["name"] == "IPCA"].iloc[0]
    assert ipca["sources"] == "csv,mt5" and ipca["impact"] == cal.MEDIUM   # sem marcação à mão: o do MT5
    assert len(events) == 3                                    # NFP, desemprego e IPCA


def test_old_csv_without_names_merges_by_time_and_currency(db, tmp_path):
    cal.import_source(db, None, "mt5", _write(tmp_path, "m.csv", MT5_FILE))
    cal.import_source(db, None, "csv", _write(tmp_path, "e.csv", CSV_OLD))
    events = cal.merged(db)
    at_1030 = events[events["event_time"] == pd.Timestamp("2026-01-09 10:30")]
    assert any("csv" in s for s in at_1030["sources"])         # o CSV sem nome casou com um dos eventos do MT5
    assert len(events) == 4                                      # + o evento '*' do dia 12


def test_events_for_symbol(db, tmp_path):
    cal.import_source(db, None, "mt5", _write(tmp_path, "m.csv", MT5_FILE))
    events = cal.merged(db)
    mapping = cal.parse_mapping("BRL: WIN$N WDO$N; USD: WDO$N")
    assert set(cal.events_for(events, "EURUSD")["currency"]) == {"USD"}      # moedas no nome do par
    assert set(cal.events_for(events, "WIN$N", mapping=mapping)["currency"]) == {"BRL"}
    assert set(cal.events_for(events, "WDO$N", mapping=mapping)["currency"]) == {"BRL", "USD"}
    assert set(cal.events_for(events, "PETR4", profit_currency="BRL")["currency"]) == {"BRL"}
    assert cal.events_for(events, "PETR4").empty


def test_request_file_and_startup_ini(tmp_path, monkeypatch):
    from mt5_extracao.robots import mt5_tools
    monkeypatch.setattr(mt5_tools, "common_files_dir", lambda config: tmp_path)
    path = mt5_export.write_request(None, 365, 15, "BR,US", 2)
    assert "countries=BR,US" in path.read_text(encoding="ascii")
    assert "Script=MT5X\\MT5X_CalendarExport" in mt5_export.startup_ini()
    (tmp_path / "MT5X_calendar_Demo.csv").write_text(MT5_FILE, encoding="utf-8")
    assert mt5_export.find_export(None).name == "MT5X_calendar_Demo.csv"


def test_install_copies_and_compiles(tmp_path, monkeypatch):
    from mt5_extracao.robots import mt5_tools
    monkeypatch.setattr(mt5_tools, "data_dir", lambda config, mt5=None: tmp_path)
    monkeypatch.setattr(mt5_tools, "find_metaeditor", lambda config: tmp_path / "MetaEditor64.exe")
    monkeypatch.setattr(mt5_tools, "wine_prefix", lambda config: None)
    seen = {}
    monkeypatch.setattr(mt5_tools, "compile_source",
                        lambda src, me, prefix, run=None: seen.setdefault("src", src) and
                        mt5_tools.CompileResult(True, compiled=str(src.with_suffix(".ex5"))))
    result = mt5_export.install(None)
    assert result["ok"] and (tmp_path / "MQL5" / "Scripts" / "MT5X" / "MT5X_CalendarExport.mq5").is_file()


def test_script_has_no_trading_calls():
    """O script só lê o calendário: sem ordem, sem DLL (as mesmas regras da bateria)."""
    source = mt5_export.SCRIPT.read_text(encoding="utf-8")
    for forbidden in ("OrderSend", "CTrade", "PositionOpen", "#import"):
        assert forbidden not in source
    assert "CalendarValueHistory" in source and "FILE_COMMON" in source


def test_cli_calendar_import_and_status(db, tmp_path, capsys):
    cfg = tmp_path / "config.ini"
    cfg.write_text(f"[DATABASE]\ntype = sqlite\npath = {db.db_path}\n[SCREENER]\nevents_file = "
                   f"{_write(tmp_path, 'e.csv', CSV_OLD)}\n", encoding="utf-8")
    assert cli.main(["--config", str(cfg), "--json", "trades", "calendar", "import", "--source", "csv"]) == cli.EXIT_OK
    capsys.readouterr()
    assert cli.main(["--config", str(cfg), "--json", "trades", "calendar", "status"]) == cli.EXIT_OK
    data = json.loads(capsys.readouterr().out.strip().splitlines()[-1])["data"]
    assert data["sources"]["csv"]["events"] == 2
