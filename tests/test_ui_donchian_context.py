import pandas as pd
from tests.test_ui_laboratory import panel, _trades
from tests.test_indicator_materialization import source, seed
from mt5_extracao.analytics import indicator_materializer
from mt5_extracao.trade_analytics import ledger


def test_existing_indicator_csv_action_exports_real_materialized_channel(panel,tmp_path,monkeypatch):
    from mt5_extracao.ui import laboratory
    panel.db.broker_utc_offset=0
    seed(panel.db,source(60))
    ids=['channel.donchian_20_previous_upper','channel.donchian_20_previous_position']
    indicator_materializer.materialize(panel.db,'TEST','M5',feature_ids=ids)
    ledger.save_trades(panel.db,'donchian',_trades(7,3),robot_key='python:ctx',symbol='TEST',timeframe='M5')
    panel.reload();panel.pump(lambda:panel.view is not None)
    # Relógio UTC explicitamente declarado pelo consumidor de teste, sem inferir host/corretora.
    panel.view['entry_time']=pd.Timestamp('2026-09-01T13:00Z')
    target=tmp_path/'Donchian pela UI.csv';monkeypatch.setattr(laboratory.filedialog,'asksaveasfilename',lambda **k:str(target))
    panel.indicators_button.invoke();panel.pump(lambda:target.exists())
    exported=pd.read_csv(target,keep_default_na=False)
    assert exported[ids[0]+'::state'].eq('AVAILABLE').all()
    assert exported[ids[0]+'::value'].eq(147.).all()
    assert exported[ids[1]+'::state'].eq('AVAILABLE').all()
    assert exported[ids[0]+'::parameters'].str.contains('"reference_shift_bars":1',regex=False).all()
