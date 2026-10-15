"""T16.11 (parte 1): aba Laboratório — lentes, filtros, guias e estados vazios."""
import configparser
import queue
import threading
import time

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
            threading.Thread(target=target, daemon=True).start()

        def log(self, text):
            self.logs.append(text)

    ui = UI()
    notebook = ttk.Notebook(root)
    notebook.pack()
    p = LaboratoryPanel(notebook, ui)
    notebook.add(p.frame, text="Laboratório")

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
    p.pump, p.db = pump, db
    yield p
    root.destroy()
    # lixo do Tk tem de ser coletado AQUI, na thread principal: se sobrar, o coletor pode rodar numa thread de
    # segundo plano do próximo teste e o Tk derruba o processo ("Illegal instruction")
    import gc
    gc.collect()
    db.close()


def test_empty_book_explains_how_to_start(panel):
    panel.on_show()
    panel.pump(lambda: "ainda não tem operações" in panel.note.cget("text"))
    assert "Importar backtests antigos" in panel.note.cget("text")
    assert panel._note_kind == "empty"


def test_lenses_filters_and_tabs(panel):
    ledger.save_trades(panel.db, "a", _trades(1), robot_key="python:a", symbol="WIN$N", timeframe="M5")
    ledger.save_trades(panel.db, "b", _trades(2), robot_key="python:a", symbol="WDO$N", timeframe="M5")
    ledger.save_trades(panel.db, "c", _trades(3), robot_key="python:b", symbol="WIN$N", timeframe="M5")
    panel.on_show()
    panel.pump(lambda: panel.view is not None)
    assert panel.selected.get() == "python:a" and panel.kpi["n"][0].cget("text") == "240"
    assert [panel.overview_notebook.tab(i, "text") for i in range(3)] == ["Horários", "Curvas", "Calendário"]
    assert panel.overview_heat_table.master is panel.overview_views["heat"]
    assert len(panel.overview_heat_table.tree.get_children()) == 24
    first_cell = panel.overview_heat_table.tree.item(panel.overview_heat_table.tree.get_children()[0], "values")[1]
    assert "n=" in first_cell
    cells = [panel.overview_heat_table.tree.item(row, "values")[1:] for row in
             panel.overview_heat_table.tree.get_children()]
    empty_cells = [cell for row in cells for cell in row if "n=0" in cell]
    assert empty_cells and all(cell.endswith("n=0") and "—" in cell for cell in empty_cells)
    assert any(cell.endswith("*") and "n=0" not in cell for row in cells for cell in row)
    for k, name in enumerate(("Tempo e calendário", "Microestrutura", "Comparativo")):
        panel.tabs.select(k + 1)
        panel.pump(lambda: name in panel._drawn, 10)
        assert name in panel._drawn
    assert [panel._analysis_notebooks["Comparativo"].tab(i, "text") for i in range(2)] == [
        "Ativos e timeframes", "Correlação"]
    assert [panel._analysis_notebooks["Tempo e calendário"].tab(i, "text") for i in range(4)] == [
        "Dia da semana", "Dia do mês", "Expectativa e acerto", "Vitórias e perdas"]
    assert all(text.get_rotation() == 90 for text in panel._charts["t_winloss"].figure.axes[0].texts)
    assert [panel._analysis_notebooks["Microestrutura"].tab(i, "text") for i in range(4)] == [
        "Volatilidade × resultado", "Faixa de volatilidade", "Volume na entrada", "Volatilidade × horário"]
    assert len(panel.compare_tree.get_children()) == 2                  # o robô em dois ativos
    assert str(panel.tabs.tab(4, "state")) == "normal"                 # Execução e Insights habilitadas (T16.11b)
    panel.lens.set("asset")
    panel._lens_changed()
    panel.pump(lambda: panel.view is not None and panel.selected.get() == "WDO$N")
    panel.selected.set("WIN$N")
    panel.load_lab()
    panel.pump(lambda: panel.view is not None and set(panel.view["robot_key"]) == {"python:a", "python:b"})
    assert panel.kpi["n"][0].cget("text") == "240"
    correlation = panel._charts["c_corr"]
    assert correlation.figure.axes[0].images[0].norm.vmin == -1
    assert correlation.figure.axes[-1].get_ylabel() == "Correlação"
    assert "escala fixa de −1 a 1" in panel._chart_panels["c_corr"].notice.message.cget("text")
    panel.period.set("Últimos 3 meses")
    panel.apply_filters()
    assert int(panel.kpi["n"][0].cget("text").replace(".", "")) < 240


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
    assert panel._note_kind == "partial"


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
