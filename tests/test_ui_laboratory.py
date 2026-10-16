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


@pytest.mark.parametrize("values,shown,reason", [([1, 5], "—", "Sem perdas"),
                                                ([2000, -1], "2.000,00", "")])
def test_pf_card_keeps_undefined_reason_or_full_finite_ratio(panel, values, shown, reason):
    panel.view = pd.DataFrame({"pnl": values, "run_id": ["controlled"] * len(values),
                               "completeness": ["completo"] * len(values)})
    before = panel.view.copy(deep=True)
    panel._show_kpis()
    card = panel.metric_cards["profit_factor"]
    assert card.value_label.cget("text") == shown
    assert card.detail_label.cget("text") == reason
    pd.testing.assert_frame_equal(panel.view, before)


def test_relations_view_is_available_without_ledger_and_restores_trade_filters(panel):
    from mt5_extracao.ui.laboratory import RELATIONS_TAB

    panel.tabs.select(panel.tab_frames[RELATIONS_TAB])
    panel.render_visible()
    panel.ui.root.update()
    assert panel.relations_panel is not None
    assert RELATIONS_TAB in panel._drawn
    assert not panel.filter_bar.winfo_manager()
    assert not panel.kpi_row.winfo_manager()

    panel.tabs.select(panel.tab_frames["Visão geral"])
    panel.render_visible()
    assert panel.filter_bar.winfo_manager()
    assert panel.kpi_row.winfo_manager()


def test_lenses_filters_and_tabs(panel):
    ledger.save_trades(panel.db, "a", _trades(1), robot_key="python:a", symbol="WIN$N", timeframe="M5")
    ledger.save_trades(panel.db, "b", _trades(2), robot_key="python:a", symbol="WDO$N", timeframe="M5")
    ledger.save_trades(panel.db, "c", _trades(3), robot_key="python:b", symbol="WIN$N", timeframe="M5")
    panel.on_show()
    panel.pump(lambda: panel.view is not None)
    assert panel.selected.get() == "python:a" and panel.kpi["n"][0].cget("text") == "240"
    assert [panel.overview_notebook.tab(i, "text") for i in range(3)] == ["Horários", "Curvas", "Calendário"]
    assert panel.overview_heat_table.master is panel.overview_views["heat"]
    assert len(panel.overview_heat_matrix.cells) == 24
    cells = [[cell.cget("text") for cell in row] for row in panel.overview_heat_matrix.cells]
    assert any("n=" in cell for row in cells for cell in row)
    empty_cells = [cell for row in cells for cell in row if cell == "—"]
    assert empty_cells and "sem operações (n=0)" in panel.overview_heat_notice.message.cget("text")
    assert any(cell.endswith("*") and "n=0" not in cell for row in cells for cell in row)
    for k, name in enumerate(("Tempo e calendário", "Microestrutura", "Comparativo")):
        panel.tabs.select(k + 1)
        panel.pump(lambda: name in panel._drawn, 10)
        assert name in panel._drawn
    assert [panel._analysis_notebooks["Comparativo"].tab(i, "text") for i in range(2)] == [
        "Ativos e timeframes", "Correlação"]
    assert [panel._analysis_notebooks["Tempo e calendário"].tab(i, "text") for i in range(4)] == [
        "Dia da semana", "Dia do mês", "Expectativa e acerto", "Ganhos, perdas e zeradas"]
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


def test_temporal_population_updates_without_changing_matrix_values(panel):
    """Entrada e saída em horas distintas: rótulo e n acompanham a população existente."""
    ledger.save_trades(panel.db, "complete", _trades(5, n=3), robot_key="python:full", symbol="AAA", timeframe="H1")
    panel.on_show()
    panel.pump(lambda: panel.view is not None)
    original = panel.view.copy(deep=True)
    parent = panel.overview_views["heat"]
    panel._overview_matrix(parent, original)
    rows = [[cell.cget("text") for cell in row] for row in panel.overview_heat_matrix.cells]
    assert "Entrada · 3 operações" in panel.overview_heat_notice.message.cget("text")
    partial = original.copy(deep=True)
    partial["time_ref"] = "saida"
    panel.view = partial
    panel._overview_matrix(parent, partial)
    assert "Saída (entrada indisponível) · 3 operações" in panel.overview_heat_notice.message.cget("text")
    assert rows == [[cell.cget("text") for cell in row] for row in panel.overview_heat_matrix.cells]
    mixed = pd.concat([original, partial], ignore_index=True)
    panel.view = mixed
    panel._overview()
    text = panel.overview_heat_notice.message.cget("text")
    assert "Entrada · 3 operações" in text and "3 operações sem entrada ficam fora" in text
    assert rows == [[cell.cget("text") for cell in row] for row in panel.overview_heat_matrix.cells]
    panel._time()
    for key in ("t_week", "t_month", "t_hours", "t_winloss"):
        text = panel._chart_panels[key].notice.message.cget("text")
        assert "Referência mista" in text and "6 operações" in text
    panel._time()
    assert panel._chart_panels["t_winloss"].notice.message.cget("text").count("Referência mista") == 1
    pd.testing.assert_frame_equal(mixed, panel.view)
    panel.view = original
    panel._overview()
    assert "ficam fora" not in panel.overview_heat_notice.message.cget("text")
    pd.testing.assert_frame_equal(original, panel.view)


def test_small_window_keeps_filters_and_scrollable_results_reachable(panel):
    root = panel.ui.root
    root.geometry("900x620")
    panel.frame.master.pack(fill=tk.BOTH, expand=True)
    ledger.save_trades(panel.db, "a", _trades(1), robot_key="python:a", symbol="AAA", timeframe="H1")
    panel.on_show()
    panel.pump(lambda: panel.view is not None)
    root.update()
    assert panel._filter_columns < 4
    for cell in panel.filter_cells:
        assert cell.winfo_ismapped()
        assert cell.winfo_x() + cell.winfo_width() <= panel.filter_bar.winfo_width()
    assert panel.refresh_button.winfo_ismapped()
    viewport = panel.results_viewport
    assert viewport.scrollbar.winfo_manager() == "grid"
    assert viewport.content.winfo_height() > viewport.canvas.winfo_height()
    viewport.canvas.yview_moveto(1)
    root.update()
    assert panel.filter_bar.winfo_ismapped()
    assert panel.selector.winfo_ismapped()
    matrix = panel.overview_heat_matrix
    assert matrix.canvas.winfo_height() >= 100
    for row in matrix.cells:
        assert all(cell.winfo_width() >= cell.winfo_reqwidth() for cell in row)
    assert str(matrix.horizontal.cget("orient")) == "horizontal"
    root.geometry("1920x1080")
    root.update()
    # O tamanho pedido não fixa a largura efetiva nem as métricas das fontes no Windows.
    # Verificamos acesso real aos controles, inclusive quando o leiaute precisa de duas linhas.
    for cell in panel.filter_cells:
        assert cell.winfo_ismapped()
        assert cell.winfo_rootx() >= panel.filter_bar.winfo_rootx()
        assert cell.winfo_rootx() + cell.winfo_width() <= panel.filter_bar.winfo_rootx() + panel.filter_bar.winfo_width()
    for button in (panel.import_button, panel.refresh_button):
        assert button.winfo_ismapped()
        assert button.winfo_rootx() + button.winfo_width() <= panel.frame.winfo_rootx() + panel.frame.winfo_width()
    assert panel.kpi["n"][0].cget("text") == "120"


def test_outcome_chart_separates_zero_and_refreshes_in_both_themes(panel):
    from dataclasses import replace
    mixed = [replace(trade, pnl=value) for trade, value in zip(_trades(8, n=6), [12, -7, 0, 0, 3, -2])]
    positive = [replace(trade, pnl=1.0) for trade in _trades(9, n=3)]
    ledger.save_trades(panel.db, "mixed", mixed, robot_key="python:a", symbol="AAA", timeframe="H1")
    ledger.save_trades(panel.db, "positive", positive, robot_key="python:b", symbol="AAA", timeframe="H1")
    panel.on_show()
    panel.pump(lambda: panel.view is not None)
    original = panel.view.copy(deep=True)
    panel.tabs.select(1)
    for _ in range(2):
        panel._time()
        axis = panel._charts["t_winloss"].figure.axes[0]
        totals = {container.get_label(): sum(bar.get_height() for bar in container.patches)
                  for container in axis.containers}
        assert totals == {"ganhos (>0)": 2, "perdas (<0)": 2, "zeradas (=0)": 2}
        assert sum(totals.values()) == len(panel.view)
        assert "6 operações" in panel._chart_panels["t_winloss"].notice.message.cget("text")
        pd.testing.assert_frame_equal(panel.view, original)
        panel.theme.toggle()
    panel.selected.set("python:b")
    panel.load_lab()
    panel.pump(lambda: panel.view is not None and set(panel.view["robot_key"]) == {"python:b"})
    axis = panel._charts["t_winloss"].figure.axes[0]
    assert [container.get_label() for container in axis.containers] == ["ganhos (>0)"]
    assert sum(bar.get_height() for bar in axis.containers[0].patches) == 3


def test_hourly_curve_preserves_real_spacing_and_gaps_in_both_themes(panel):
    from dataclasses import replace
    from matplotlib.path import Path
    entries = pd.to_datetime(["2025-01-06 00:05", "2025-01-06 02:35", "2025-01-06 03:35", "2025-01-06 04:05"])
    trades = [replace(trade, entry_time=entry, exit_time=entry + pd.Timedelta(minutes=10), pnl=pnl)
              for trade, entry, pnl in zip(_trades(11, n=4), entries, [0.0, 5.0, -2.0, 2.0])]
    ledger.save_trades(panel.db, "gaps", trades, robot_key="python:gaps", symbol="AAA", timeframe="H1")
    panel.on_show()
    panel.pump(lambda: panel.view is not None)
    original = panel.view.copy(deep=True)
    panel.tabs.select(1)
    expected = np.full(48, np.nan)
    expected[[0, 5, 7, 8]] = [0, 100, 0, 100]
    for _ in range(2):
        panel._time()
        panel.ui.root.update()
        chart = panel._charts["t_hours"]
        line = chart.figure.axes[1].lines[0]
        np.testing.assert_equal(line.get_xdata(), np.arange(48))
        np.testing.assert_equal(line.get_ydata(), expected)
        codes = [code for _vertices, code in line.get_path().iter_segments()]
        assert codes.count(Path.MOVETO) == 3
        assert len(chart.figure.axes[0].texts) == 4
        assert len(chart.figure.axes[0].get_xticks()) <= 12
        assert "30 min" in chart.figure.axes[0].get_xlabel()
        notice = panel._chart_panels["t_hours"].notice.message.cget("text")
        assert "Lacunas (n=0)" in notice and "não equivalem a zero" in notice
        pd.testing.assert_frame_equal(panel.view, original)
        panel.theme.toggle()
    panel.view = original.copy(deep=True)
    panel.view["time_bucket"] = None
    panel._time()
    chart = panel._charts["t_hours"]
    assert len(chart.figure.axes) == 1 and not chart.figure.axes[0].lines
    assert "Sem operações com horário e resultado válidos" in chart.figure.axes[0].texts[0].get_text()
    assert "4 operações sem faixa horária válida ficam fora" in panel._chart_panels["t_hours"].notice.message.cget("text")
