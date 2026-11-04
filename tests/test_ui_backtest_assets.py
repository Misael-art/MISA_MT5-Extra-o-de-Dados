"""Selecionar/revisar/executar vários ativos no diálogo oficial, sem MT5."""
from tests.test_ui_main_window import ui
from tests.test_ui_backtest_scope import wait, open_python
from tests.market_data import trend
from mt5_extracao.robots import budget, store


def test_six_asset_review_and_progress_keep_each_cell_and_detail(ui):
    symbols = ['WIN$N'] + [f'QA{i}' for i in range(5)]
    for symbol in symbols[1:]:
        ui.app.db_manager.save_ohlcv_data(symbol, '1 hora', trend(350, freq='h'))
    dialog = open_python(ui)
    wait(ui, lambda:len(ui.symbol_series) == 6)
    # O inventário já está carregado quando o diálogo nasce.
    dialog.close(); dialog = open_python(ui)
    dialog.select_all_assets(); ui.pump(.05)
    assert set(dialog.assets.selection()) == set(symbols)
    dialog.prepare(); assert dialog.active and dialog.all_assets_button.instate(['disabled'])
    wait(ui, lambda:not dialog.active)
    assert dialog.plan['eligible'] == 7 and len(dialog.tree.get_children()) == 126
    assert not store.load_runs(ui.app.db_manager)
    dialog.execute(); wait(ui, lambda:not dialog.active)
    wait(ui, lambda:len(dialog.records) == 7)
    assert len(store.load_runs(ui.app.db_manager)) == 7
    for symbol in symbols:
        key = budget.job_key(dialog.robot.key, symbol, 'H1')
        assert dialog.tree.item(key, 'values')[2] == 'Concluído'
        dialog.tree.selection_set(key); dialog.show_detail()
        assert symbol + ' H1' in dialog.detail.cget('text')
        assert dialog.records[key]['symbol'] == symbol
    dialog.close()


def test_asset_selection_invalidates_review_and_empty_selection_explained(ui):
    ui.app.db_manager.save_ohlcv_data('QA2', '1 hora', trend(350, freq='h'))
    dialog = open_python(ui)
    dialog.prepare(); wait(ui, lambda:not dialog.active)
    assert dialog.plan and not dialog.run_button.instate(['disabled'])
    dialog.assets.selection_set('QA2'); ui.pump(.05)
    assert dialog.plan is None and dialog.run_button.instate(['disabled'])
    dialog.assets.selection_set([]); ui.pump(.05)
    dialog.prepare(); wait(ui, lambda:not dialog.active)
    assert 'ao menos um ativo' in dialog.status.cget('text')
    assert not store.load_runs(ui.app.db_manager)
    dialog.close()
