"""Ciclo categórico datado na visão do candle, sem codificá-lo como número."""
import json

import pandas as pd
import pytest

from mt5_extracao.analytics import datasets,macro_sources
from mt5_extracao.laboratory import service,report as portable
from tests.test_mentions_contract import db
from tests.test_mention_sources import Response


@pytest.fixture
def phase(db,monkeypatch):
    raw=b'observation_date,USREC\n2020-01-01,0\n2020-02-01,0\n2020-03-01,1\n'
    monkeypatch.setattr(macro_sources.requests,'get',lambda *a,**k:Response(None,raw=raw))
    source=macro_sources.collect(db,source_id='fred.usrec',start='2020-01-01',end='2020-04-01')
    snap=datasets.capture(db,source['symbol'],'D1','2020-01-01T00:00Z','2020-04-01T00:00Z',stored_features=source['features'],feature_only=True)
    prices=[100.+i*.1 for i in range(95)]
    db.save_ohlcv_data('X','1 dia',pd.DataFrame(dict(time=pd.date_range('2020-01-01',periods=95,freq='D'),
        open=prices,high=[p+.2 for p in prices],low=[p-.2 for p in prices],close=prices,tick_volume=10,real_volume=100,spread=1)))
    market=service.capture_selection(db,symbol='X',timeframes_selected=['D1'])
    link=dict(snapshot=snap,features=source['features'],target_symbol='X',relationship='QA: fase econômica EUA associada explicitamente a preço sintético X')
    def analyze(basis='publication',cut=None):return service.analyze(market,feature_ids=['price.return_1'],linked_sources=[link],
        timeline_basis=basis,as_of=cut or pd.Timestamp.now(tz='UTC').isoformat())
    return analyze


def test_macro_periods_and_candle_are_retrospective_and_not_numeric(phase):
    r=phase();c=next(c for c in r['channels'] if c.get('linked_source'))
    periods=service.source_periods(r,c['id'])
    assert [p['value'] for p in periods]==['expansion','recession'] and periods[0]['period_count']==2
    assert periods[0]['start']=='2020-01-01T00:00:00+00:00' and periods[0]['end']=='2020-03-01T00:00:00+00:00'
    assert all(p['retrospective'] for p in periods) and c['values']==[None]*95
    index=r['times'].index('2020-03-15T00:00:00+00:00');context=service.candle_context(r,index)
    assert context['source_periods'][0]['value']=='recession'
    assert not context['received_events']
    cell=service.relation_matrix(r)['cells'][0][1]
    assert cell['effect'] is None and cell['reason']=='NON_NUMERIC_STORED_FEATURE'


def test_received_and_old_cut_do_not_backfill_retrospective_cycles(phase):
    received=phase('received');old=phase(cut='2020-03-15T00:00Z')
    assert all(not values for values in received['source_periods'].values())
    assert all(not values for values in old['source_periods'].values())
    assert not service.candle_context(old,0)['source_periods']


def test_source_citation_and_periods_survive_frozen_report(phase):
    r=phase();frozen=service.load_report(json.dumps(r))
    assert frozen['source_periods']==r['source_periods']
    html=portable.render(frozen)
    assert 'id="citations"' in html and 'Federal Reserve Bank of St. Louis' in html
    assert 'https://fred.stlouisfed.org/series/USREC' in html
    assert 'R.source_periods' in html
