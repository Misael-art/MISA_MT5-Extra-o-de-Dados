"""Cursor/zoom temporais e seleção da cronologia no cliente real."""
from types import SimpleNamespace
from matplotlib.dates import date2num
import pandas as pd
import pytest
from tests.test_ui_laboratory import panel
from tests.test_ui_laboratory_series import open_series
from tests.test_laboratory_links import linked,analyze
from tests.test_laboratory_financial import financial
from tests.test_mentions_contract import db


def test_month_plot_uses_frozen_broker_clock_and_does_not_compress_gaps():
    from mt5_extracao.ui.laboratory_series import _plot_periods,timeline_bins
    times=pd.DatetimeIndex(['2024-01-31T22:00Z','2024-02-29T22:00Z','2024-04-30T22:00Z'])
    r=dict(grid_timeframe='MN1',populations=[dict(clock=dict(broker_utc_offset_hours=2))])
    previous,following=_plot_periods(r,times)
    assert previous[0]==pd.Timestamp('2023-12-31T22:00Z') and following[0]==times[1]
    assert following[1]==pd.Timestamp('2024-03-31T22:00Z')
    breaks=[2];data,boundaries=timeline_bins([dict(directions=[1,1,-1])],1,breaks)
    assert list(boundaries)==[0,2,3] and data.tolist()==[[1,-1]]


def test_temporal_cursor_zoom_sync_and_receipt_row_selects_candle(panel,linked):
    panel.ui.app.db_manager=linked[0];series=open_series(panel);panel.pump(lambda:not series._loading_sources)
    report=analyze(linked);series.set_report(report)
    price=series.panels['price'].chart;timeline=series.panels['timeline'].chart
    original_matrix=series.panels['relations'].chart.figure.axes
    matrix_limits=[a.get_xlim() for a in original_matrix]
    start=date2num(pd.Timestamp(report['times'][2]).to_pydatetime());finish=date2num(pd.Timestamp(report['times'][8]).to_pydatetime())
    timeline._time_axes[0].set_xlim(start,finish)
    assert price._time_axes[0].get_xlim()==pytest.approx((start,finish))
    series._cursor_moved(timeline,SimpleNamespace(inaxes=timeline._time_axes[0],xdata=start))
    assert price._cursors[0].get_xdata()[0]==pytest.approx(start)
    series.panels['price'].expand();series.panels['timeline'].expand();panel.ui.root.update()
    for name in ('price','timeline'):
        detail=series.panels[name].detail
        assert detail._time_axes[0].get_xlim()==pytest.approx((start,finish))
        assert detail._cursors[0].get_xdata()[0]==pytest.approx(start)
    series.open_links(candle=0);view=series.links_dialog;panel.pump(lambda:not view.pending)
    quantity=next(i for i,(channel,event) in enumerate(view.event_rows) if channel['feature']=='mentions.quantity')
    view.events_tree.selection_set(str(quantity));panel.ui.root.update()
    assert view.candle_index.get()=='3'
    expected=date2num(pd.Timestamp(report['times'][3]).to_pydatetime())
    assert timeline._cursors[0].get_xdata()[0]==pytest.approx(expected)
    assert len(view.candle_values.get_children())==len(report['channels'])
    assert [a.get_xlim() for a in original_matrix]==matrix_limits
    for name in ('price','timeline'):series.panels[name].restore()
    series.select_candle(4);panel.ui.root.update()


def test_inspector_links_conciliated_run_and_exports_frozen_observations(panel,financial,tmp_path,monkeypatch):
    from mt5_extracao.ui import laboratory_links
    from tests.test_laboratory_execution import analyze as market_analysis
    from mt5_extracao.robots import backtest
    database,_,runs=financial;panel.ui.app.db_manager=database
    report=market_analysis(database,runs[0]);series=open_series(panel);panel.pump(lambda:not series._loading_sources)
    series.set_report(report);series.open_links(candle=300);view=series.links_dialog;panel.pump(lambda:not view.pending)
    monkeypatch.setattr(backtest,'run_python',lambda *a,**k:pytest.fail('inspector ran strategy'))
    view.list_executions();panel.pump(lambda:not view.pending)
    view.execution_choice.set(next(k for k,v in view.execution_options.items() if v==runs[0]['run_id']))
    view.link_execution();assert view.pending;panel.pump(lambda:not view.pending,seconds=30)
    assert view.execution['run_id']==runs[0]['run_id'],view.status.cget('text')
    assert view.execution_events.get_children()
    assert 'sequência do modelo' in view.candle_text.get('1.0','end')
    out=tmp_path/'execution.html';monkeypatch.setattr(laboratory_links.filedialog,'asksaveasfilename',lambda **k:str(out))
    view.export();panel.pump(lambda:not view.pending,seconds=30)
    assert view.execution['trajectory_id'] in out.read_text(encoding='utf-8')
