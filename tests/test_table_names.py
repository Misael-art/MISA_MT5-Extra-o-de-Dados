"""T1.7: nomes de tabela sem colisão entre símbolos, compatível com bases antigas."""
import threading

import pandas as pd
from sqlalchemy import text

from mt5_extracao.database_manager import DatabaseManager


def _db(tmp_path, name="t.db"):
    return DatabaseManager(db_path=str(tmp_path / name))


def test_same_names_as_before(tmp_path):
    db = _db(tmp_path)
    assert db.get_table_name_for_symbol("WIN$N", "1 minuto") == "win_n_1_minuto"
    assert db.get_table_name_for_symbol("PETR4", "1 mês") == "petr4_1_mês"


def test_colliding_symbols_get_different_tables(tmp_path):
    db = _db(tmp_path)
    a = db.get_table_name_for_symbol("WIN$N", "1 minuto")
    b = db.get_table_name_for_symbol("WIN_N", "1 minuto")
    assert a == "win_n_1_minuto"
    assert b.startswith("win_n_1_minuto_") and b != a


def test_mapping_is_persistent_and_timeframe_spelling_insensitive(tmp_path):
    db = _db(tmp_path)
    db.get_table_name_for_symbol("WIN$N", "1 minuto")
    b = db.get_table_name_for_symbol("WIN_N", "1 minuto")
    db2 = _db(tmp_path)  # nova instância (sem cache) lê o mapeamento gravado
    assert db2.get_table_name_for_symbol("WIN_N", "1 minuto") == b
    # O coletor em tempo real usa '1_minuto': deve cair na mesma tabela
    assert db2.get_table_name_for_symbol("WIN$N", "1_minuto") == "win_n_1_minuto"


def test_existing_legacy_table_is_kept(tmp_path):
    db = _db(tmp_path)
    df = pd.DataFrame({"time": pd.to_datetime(["2024-01-02 09:00"]), "open": [1.0], "high": [1.0],
                       "low": [1.0], "close": [1.0], "tick_volume": [1], "spread": [1], "real_volume": [1]})
    db.save_ohlcv_data("WIN$N", "1 minuto", df)
    with db.engine.begin() as c:  # simula base anterior à T1.7: sem mapeamento
        c.execute(text("DELETE FROM _symbol_tables"))
    db2 = _db(tmp_path)
    assert db2.get_table_name_for_symbol("WIN$N", "1 minuto") == "win_n_1_minuto"
    with db2.engine.connect() as c:
        assert c.execute(text("SELECT COUNT(*) FROM win_n_1_minuto")).scalar() == 1


def test_concurrent_calls_are_consistent(tmp_path):
    db = _db(tmp_path)
    results = []
    threads = [threading.Thread(target=lambda s=s: results.append((s, db.get_table_name_for_symbol(s, "1 minuto"))))
               for s in ["WIN$N", "WIN_N"] * 5]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    names = {}
    for sym, name in results:
        names.setdefault(sym, set()).add(name)
    assert all(len(v) == 1 for v in names.values())
    assert names["WIN$N"] != names["WIN_N"]
