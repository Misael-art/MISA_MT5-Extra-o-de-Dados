"""T16.11 (parte 1): aba Laboratório — lentes, filtros, guias e estados vazios."""
import configparser
import gc
import queue
import threading
import time
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from mt5_extracao.database_manager import DatabaseManager
from mt5_extracao.strategies.backtester import Trade
from mt5_extracao.trade_analytics import ledger

tk = pytest.importorskip("tkinter")


def _trades(seed, n=120):
    rng = np.random.default_rng(seed)
    entries = pd.date_range("2025-01-06 09:05", periods=n, freq="23h")   # ~115 dias
    return [Trade(1, e, e + pd.Timedelta(minutes=30), 100, 101, 1, float(rng.normal(0, 10)) - (20 if e.dayofweek == 4
                  else 0), 0.1, 6, "alvo", 0) for e in entries]


@pytest.fixture
def panel(tmp_path):
    try:
        root = tk.Tk()
    except tk.TclError as e:
        pytest.skip(f"sem display gráfico: {e}")
    pytest.importorskip("matplotlib")
    from tkinter import ttk
    from mt5_extracao.ui.laboratory import LaboratoryPanel
    from mt5_extracao.ui.theme import Theme
    db = DatabaseManager(db_path=str(tmp_path / "lab.db"))
    jobs = queue.Queue()
    workers = []

    class UI:
        def __init__(self):
            self.root, self.theme = root, Theme(root)
            self.app = type("App", (), {"db_manager": db, "config": configparser.ConfigParser()})()
            self.logs = []

        def run_bg(self, work, done):
            def target():
                try:
                    jobs.put((done, (work(), None)))
                except Exception as e:                 # noqa: BLE001
                    jobs.put((done, (None, e)))
            worker = threading.Thread(target=target, daemon=True)
            workers.append(worker)
            worker.start()

        def log(self, text):
            self.logs.append(text)

    ui = UI()
    notebook = ttk.Notebook(root)
    notebook.pack()
    p = LaboratoryPanel(notebook, ui)
    notebook.add(p.frame, text="Laboratório")
    ui.notebook = notebook
    ui.laboratory_panel = p

    def pump(until=lambda: False, seconds=15):
        end = time.time() + seconds
        while time.time() < end:
            while not jobs.empty():
                fn, args = jobs.get()
                fn(*args)
            root.update()
            if until():
                return
            time.sleep(0.02)
    p.pump, p.db, p.root = pump, db, root
    gc_was_enabled = gc.isenabled()
    # Pandas may hit a native illegal instruction if automatic GC runs inside this worker thread.
    # Keep collection on the Tk test thread, after background work has finished.
    gc.disable()
    try:
        yield p
    finally:
        for worker in workers:
            worker.join()
        root.destroy()
        db.close()
        if gc_was_enabled:
            gc.enable()
            gc.collect()


def test_empty_book_explains_how_to_start(panel):
    panel.on_show()
    panel.pump(lambda: "ainda não tem operações" in panel.note.cget("text"))
    assert "Importar backtests antigos" in panel.note.cget("text")


def test_lenses_filters_and_tabs(panel):
    ledger.save_trades(panel.db, "a", _trades(1), robot_key="python:a", symbol="WIN$N", timeframe="M5")
    ledger.save_trades(panel.db, "b", _trades(2), robot_key="python:a", symbol="WDO$N", timeframe="M5")
    ledger.save_trades(panel.db, "c", _trades(3), robot_key="python:b", symbol="WIN$N", timeframe="M5")
    panel.on_show()
    panel.pump(lambda: panel.view is not None)
    assert panel.selected.get() == "python:a" and panel.kpi["n"][0].cget("text") == "240"
    for k, name in enumerate(("Tempo e calendário", "Microestrutura", "Comparativo")):
        panel.tabs.select(k + 1)
        panel.pump(lambda: name in panel._drawn, 10)
        assert name in panel._drawn
    assert len(panel.compare_tree.get_children()) == 2                  # o robô em dois ativos
    assert str(panel.tabs.tab(4, "state")) == "normal"                 # Execução e Insights habilitadas (T16.11b)
    panel.lens.set("asset")
    panel._lens_changed()
    panel.pump(lambda: panel.view is not None and panel.selected.get() == "WDO$N")
    panel.selected.set("WIN$N")
    panel.load_lab()
    panel.pump(lambda: panel.view is not None and set(panel.view["robot_key"]) == {"python:a", "python:b"})
    assert panel.kpi["n"][0].cget("text") == "240"
    panel.period.set("Últimos 3 meses")
    panel.apply_filters()
    assert int(panel.kpi["n"][0].cget("text").replace(".", "")) < 240


def test_heatmap_cell_filters_all_laboratory_tabs(panel):
    from matplotlib.backend_bases import MouseEvent
    from mt5_extracao.trade_analytics import aggregate

    ledger.save_trades(panel.db, "a", _trades(1), robot_key="python:a", symbol="WIN$N", timeframe="M5")
    ledger.save_trades(panel.db, "b", _trades(2), robot_key="python:a", symbol="WDO$N", timeframe="M5")
    panel.on_show()
    panel.pump(lambda: panel.view is not None)
    panel.root.update()

    chart = panel._charts["ov_heat"]
    ax = chart.figure.axes[0]
    table = aggregate.crosstab(panel.view, "time_bucket", "day_of_week", config=panel._config())
    i, j = np.argwhere(table["n"].to_numpy() > 0)[0]
    expected = {"time_bucket": table["n"].index[i], "day_of_week": table["n"].columns[j]}
    x, y = ax.transData.transform((j, i))
    event = MouseEvent("button_press_event", chart.canvas, x, y, button=1)
    chart.canvas.callbacks.process("button_press_event", event)

    assert panel.cell_filter == expected
    assert panel.view["time_bucket"].eq(expected["time_bucket"]).all()
    assert panel.view["day_of_week"].eq(expected["day_of_week"]).all()
    filtered_count = len(panel.view)
    panel.tabs.select(1)
    panel.pump(lambda: "Tempo e calendário" in panel._drawn)
    assert "Tempo e calendário" in panel._drawn
    assert len(panel.view) == filtered_count

    panel.clear_cell_filter_button.invoke()
    assert panel.cell_filter is None
    assert len(panel.view) > filtered_count


def test_robot_detail_opens_laboratory_for_robot_and_explains_missing_runs(panel):
    from mt5_extracao.ui.robot_detail import RobotDetail

    ledger.save_trades(panel.db, "a", _trades(1), robot_key="python:a", symbol="WIN$N", timeframe="M5")
    panel.on_show()
    panel.pump(lambda: panel.view is not None)
    panel.root.update()

    detail = RobotDetail(panel.frame, panel.ui)
    detail.robot = SimpleNamespace(key="python:a")
    detail.laboratory_button.invoke()
    panel.pump(lambda: panel.selected.get() == "python:a" and panel.view is not None)
    assert panel.ui.notebook.select() == str(panel.frame)
    assert panel.selected.get() == "python:a"

    detail.robot = SimpleNamespace(key="python:without-runs")
    detail.laboratory_button.invoke()
    panel.pump(lambda: panel.selected.get() == "" and "Este robô ainda não tem operações" in panel.note.cget("text"))
    assert panel.selected.get() == ""


def test_import_button_brings_old_runs(panel):
    from mt5_extracao.robots import store
    store.save_run(panel.db, {"run_id": "old", "robot_key": "mql5:x", "source": "teste", "symbol": "EURUSD",
                              "timeframe": "H1", "data_start": "", "data_end": "", "bars": 0, "metrics": {},
                              "equity": [], "trades": [["2025-01-06 10:00:00", 3.0, 1.0]] * 3,
                              "created_at": "2025-01-07 00:00:00"})
    panel.import_runs()
    panel.pump(lambda: panel.view is not None)
    assert panel.selected.get() == "mql5:x"
    assert "sem horário de entrada" in panel.note.cget("text")


def test_execution_insights_simulate_and_export(panel, tmp_path, monkeypatch):
    """T16.11b: comparar modos de um run real, procurar insights, simular uma regra e exportar a proposta."""
    from mt5_extracao import robots
    from mt5_extracao.robots import backtest
    from mt5_extracao.trade_analytics import export
    from tests.market_data import random_walk
    panel.db.save_ohlcv_data("BBB", "15 minutos", random_walk(3000, seed=4, vol=0.6, freq="15min", spread=10))
    robot = next(r for r in robots.load_catalog(None, panel.db) if r.key == "python:donchian")
    backtest.run_python(panel.db, None, robot, "BBB", "M15")
    panel.on_show()
    panel.pump(lambda: panel.view is not None)
    panel.tabs.select(4)
    panel.pump(lambda: "Execução" in panel._drawn, 10)
    assert panel.run_var.get() and "martingale" not in panel.mode_vars          # Python: sem martingale
    panel.compare_modes()
    panel.pump(lambda: bool(panel.mode_metrics), 60)
    assert {"ponto_fixo", "trailing_atr"} <= set(panel.mode_metrics)
    assert len(panel.mode_tree.get_children()) == len(panel.mode_metrics)
    panel.tabs.select(5)
    panel.find_insights()
    panel.pump(lambda: "candidato" in panel.insight_note.cget("text"), 60)
    panel.day_vars[4].set(True)
    panel.simulate()
    panel.pump(lambda: panel.evaluation is not None, 30)
    assert "Reserva final" in panel.sim_text.cget("text") and "Não operar Sex" in panel.sim_text.cget("text")
    monkeypatch.setattr(export, "_folder", lambda: tmp_path)
    panel.export_proposal()
    assert list(tmp_path.glob("robot_python_donchian_*.json"))
