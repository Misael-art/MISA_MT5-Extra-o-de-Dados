"""T20.24b: produto ativo × TF no motor/store atuais, com entradas próprias."""
import json

import pytest

from mt5_extracao import cli, sdk
from mt5_extracao.robots import backtest_scope as scope, budget, store
from tests.market_data import trend
from tests.test_backtest_scope import source, empty_source, native_robot
from tests.test_batch_budget import sample


def add_assets(source, count):
    db, _, _ = source
    symbols = [f'QA{i}' for i in range(count)]
    for symbol in symbols:
        for tf, freq in [('5 minutos', '5min'), ('1 hora', 'h')]:
            # Uma barra além do warmup250; manter todas as combinações reais.
            db.save_ohlcv_data(symbol, tf, trend(251, freq=freq))
    return symbols


def preview(source, **kwargs):
    db, robot, config = source
    return sdk.preview_backtest_scope(db, config, robot, start='2020-01-01', end='2020-02-01', **kwargs)


@pytest.mark.parametrize('count', [1, 3, 6])
def test_product_real_engine_and_exact_job_context_resume_without_duplicates(empty_source, count):
    db, robot, config = empty_source
    symbols = add_assets(empty_source, count)
    plan = preview(empty_source, symbols=symbols + symbols[:1])
    assert plan['symbols'] == symbols and plan['eligible'] == count * 2
    assert len(plan['rows']) == count * 21 and not store.load_runs(db)
    assert plan['symbol'] == (symbols[0] if count == 1 else None)
    before = {(r['symbol'], r['timeframe']): r['data_manifest_id'] for r in plan['rows'] if r['eligible']}
    altered = db.load_ohlcv(symbols[-1], '1 hora'); altered.loc[0, 'close'] += .01
    db.save_ohlcv_data(symbols[-1], '1 hora', altered)
    batch = scope.create_runner(db, config, robot, plan['batch_id']).start()
    assert batch.join(40) and not batch.error, batch.error
    results = store.load_runs(db)
    assert len(results) == count * 2
    ids = {r['run_id'] for r in results}
    for record in results:
        context = record['metrics']['backtest_scope']
        assert context['symbol'] == record['symbol'] and context['timeframe'] == record['timeframe']
        assert context['symbols'] == symbols
        assert context['data_manifest_id'] == record['data_manifest_id'] == before[record['symbol'], record['timeframe']]
    again = scope.create_runner(db, config, robot, plan['batch_id']).start()
    assert again.join(40) and not again.error
    assert {r['run_id'] for r in store.load_runs(db)} == ids
    inspected = scope.inspect_plan(db, plan['batch_id'])
    assert inspected['status'] == 'completed' and len(inspected['executions']) == count * 2
    assert {r['symbol'] for r in inspected['executions']} == set(symbols)
    assert inspected['measurements']['sampled_rss_peak'] > 0


def test_all_local_freezes_inventory_and_explains_absences(source):
    db, _, _ = source
    symbols = add_assets(source, 3)
    plan = preview(source, all_symbols=True, scope='M5')
    assert plan['asset_scope'] == 'all_local' and plan['symbols'] == ['QA'] + symbols
    assert plan['eligible'] == 4
    add_assets(source, 6)
    assert scope.inspect_plan(db, plan['batch_id'])['symbols'] == ['QA'] + symbols
    empty = preview(source, symbols=['ABSENT'], scope='M5')
    assert empty['eligible'] == 0 and 'Sem série' in empty['rows'][0]['reason']


def test_distinct_names_resolving_same_table_are_not_executed_twice(source):
    plan = preview(source, symbols=['QA', 'qa'], scope='M5')
    assert plan['eligible'] == 1 and plan['excluded'] == 1
    assert 'compartilhada com QA' in plan['rows'][1]['reason']


@pytest.mark.parametrize('kwargs', [dict(), dict(symbols=[]), dict(symbols='QA'),
    dict(symbols=['QA', '']), dict(symbol='QA', symbols=['QA']), dict(symbols=['QA'], all_symbols=True)])
def test_ambiguous_or_empty_asset_requests_refused(source, kwargs):
    with pytest.raises(ValueError):
        preview(source, **kwargs)


@pytest.mark.parametrize('failure', ['cpu', 'ram', 'disk', 'cancel'])
def test_partial_product_resource_guard_and_cancel_resume_exactly_once(source, monkeypatch, failure):
    db, robot, config = source
    symbols = add_assets(source, 3)
    config['ROBOTS'].update(batch_hard_cpu_pct='80', batch_hard_ram_pct='80')
    measured = sample(None)
    monkeypatch.setattr(budget, '_sample', lambda *a, **k: dict(measured))
    plan = preview(source, symbols=symbols, scope='M5')
    def event(value):
        if value['kind'] == 'done':
            if failure == 'cancel':
                batch.cancel()
            else:
                measured[dict(cpu='cpu', ram='ram', disk='free_bytes')[failure]] = 0 if failure == 'disk' else 90
                batch.budget._sample_at = None
    batch = scope.create_runner(db, config, robot, plan['batch_id'], on_event=event)
    batch.start(); assert batch.join(30)
    first = store.load_runs(db)
    assert len(first) == 1 and scope.inspect_plan(db, plan['batch_id'])['status'] == 'interrupted'
    measured.update(sample(None))
    resumed = scope.create_runner(db, config, robot, plan['batch_id']).start()
    assert resumed.join(30) and not resumed.error
    final = store.load_runs(db)
    assert len(final) == 3 and sum(r['run_id'] == first[0]['run_id'] for r in final) == 1
    assert scope.inspect_plan(db, plan['batch_id'])['status'] == 'completed'


def test_bound_package_excludes_other_assets_without_retargeting(source, native_robot, tmp_path):
    from mt5_extracao.presets import defaults, manual
    from mt5_extracao.presets.compiler import TestContext
    db, _, config = source
    record = defaults.ensure_default(db, native_robot, symbol='QA', timeframe='M5')
    package = manual.export_preset(db, native_robot, record.identity.id, tmp_path/'package',
        test_context=TestContext('2020-01-01', '2020-02-01', deposit=100000, model='0'))
    plan = scope.preview(db, config, native_robot, symbols=['QA', 'OTHER'],
        start='2020-01-01', end='2020-02-01', manifest_path=package['manifest_path'])
    assert plan['eligible'] == 1 and plan['preset_identity']['content_hash'] == package['package'].package_hash
    assert all('outro ativo' in r['reason'] for r in plan['rows'] if r['symbol'] == 'OTHER')
    batch = scope.create_runner(db, config, native_robot, plan['batch_id'])
    assert [(j.symbol, j.timeframe) for j in batch.jobs] == [('QA', 'M5')]


def test_cli_multi_and_all_assets_use_same_review_without_execution(source, tmp_path, monkeypatch, capsys):
    db, robot, _ = source
    symbols = add_assets(source, 3)
    config = tmp_path/'qa.ini'; config.write_text('[ROBOTS]\nbatch_max_workers=1\n', encoding='utf-8')
    monkeypatch.chdir(tmp_path); monkeypatch.setattr(cli.Context, 'db', property(lambda _: db))
    args = ['--config', str(config), '--json', 'robots', 'backtest-plan', robot.key,
            '--tf', 'M5', '--from', '2020-01-01', '--to', '2020-02-01']
    for selection, expected in [(['--symbols', ','.join(symbols)], symbols), (['--all-symbols'], ['QA'] + symbols)]:
        assert cli.main(args + selection) == 0
        plan = json.loads(capsys.readouterr().out)['data']['backtest_plan']
        assert plan['symbols'] == expected and plan['eligible'] == len(expected)
    assert not store.load_runs(db)


def test_scope_auto_workers_start_small_and_explicit_ceiling_is_respected(source):
    _, _, config = source
    config['ROBOTS']['batch_max_workers'] = '0'
    governor = scope._governor(config)
    assert governor.max_workers == 2 and governor.limit == 1
    config['ROBOTS']['batch_max_workers'] = '1'
    assert scope._governor(config).max_workers == 1


def test_native_product_uses_private_session_boundary_serially(source, tmp_path, monkeypatch):
    import time
    from mt5_extracao.robots import catalog, sessions, mt5_tools
    db, _, config = source
    symbols = add_assets(source, 3)
    path = tmp_path/'Owned.mq5'; path.write_text('input int Period=20;\nvoid OnTick() {}\n', encoding='utf-8')
    robot = catalog.robot_from_file(path); robot.timeframes = ('M5', 'H1')
    config['ROBOTS']['batch_max_workers'] = '2'
    monkeypatch.setattr(mt5_tools, 'common_files_dir', lambda config:tmp_path/'common')
    active, peaks, calls = [], [], []
    class Session:
        def __init__(self, db, config, robot, symbol, timeframe, **kwargs):
            self.symbol, self.tf, self.kwargs = symbol, timeframe, kwargs
            self.exception, self.error, self.record = None, '', None
            self.trades_file, self.state = 'own.csv', 'created'
        def run(self):
            active.append(self); peaks.append(len(active)); time.sleep(.02)
            control = self.kwargs['native_options']['control']
            self.record = dict(run_id=control.run_id, robot_key=robot.key, symbol=self.symbol, timeframe=self.tf,
                source='strategy tester', bars=0, params={}, equity=[], created_at='2020-02-01 00:00:00',
                data_start='2020-01-01 00:00:00', data_end='2020-02-01 00:00:00',
                metrics={'source_fingerprint':catalog.fingerprint(robot)})
            store.save_run(db, self.record, control=control)
            calls.append((self.symbol, self.tf)); active.remove(self); self.state='completed'
    monkeypatch.setattr(sessions, 'Session', Session)
    plan = scope.preview(db, config, robot, symbols=symbols, start='2020-01-01', end='2020-02-01')
    batch = scope.create_runner(db, config, robot, plan['batch_id']).start()
    assert batch.join(30) and not batch.error
    assert max(peaks) == 1 and len(calls) == 6
    assert set(calls) == {(s, tf) for s in symbols for tf in ('M5', 'H1')}
