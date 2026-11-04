"""CLI e SDK retornam a mesma campanha e o mesmo ID de recorte persistido."""
from copy import deepcopy
import json

from mt5_extracao import cli,sdk
from tests.test_reference_relation_windows import reference,phase,db_rows


def stable(report):
    report=deepcopy(report);report.pop('generated_at_utc')
    for item in report['windows']:item['result'].pop('generated_at_utc')
    return report


def test_cli_campaign_and_selected_window_reconcile_with_sdk(phase,tmp_path,capsys,monkeypatch):
    db,image,cutoff=phase;config=tmp_path/'qa.ini'
    config.write_text(f'[DATABASE]\ntype=sqlite\npath={db.db_path}\n[APP]\nbroker_utc_offset=0\ntime_basis=broker\n[RESOURCES]\nblock_db_gb=0\n',encoding='utf-8')
    monkeypatch.chdir(tmp_path)
    prefix=['--config',str(config),'--json','analytics','features']
    common=['--manifest',image.manifest_id,'--left','qa.x','1','--right','qa.phase','1','--as-of',cutoff,
        '--bootstrap-replicates','29','--synthetic-fixture']
    before=db_rows(db)
    assert cli.main(prefix+['relation-windows',*common,'--window-bars','80','--stride-bars','40'])==0
    reply=json.loads(capsys.readouterr().out);report=reply['data']['windows']
    expected=sdk.query_feature_relation_windows(db,image.manifest_id,left=('qa.x','1'),right=('qa.phase','1'),as_of=cutoff,
        window_bars=80,stride_bars=40,bootstrap_replicates=29,synthetic_fixture=True)
    assert stable(report)==stable(expected) and db_rows(db)==before
    window=report['windows'][-1]
    assert cli.main(prefix+['relation',*common,'--persist','--decision-start',window['decision_start_utc'],
        '--decision-end',window['decision_end_utc'],'--tests-counted','3'])==0
    saved=json.loads(capsys.readouterr().out)
    assert saved['data']['persistence']['analysis_id']==window['analysis_id']
    assert saved['data']['relation']['relation_schema_version']==3
    assert any('Janela UTC' in str(m) for m in saved['messages'])
    assert sdk.load_feature_relation(db,window['analysis_id'])==saved['data']['relation']


def test_cli_refuses_excess_windows_without_partial_output(phase,tmp_path,capsys,monkeypatch):
    db,image,cutoff=phase;config=tmp_path/'qa.ini'
    config.write_text(f'[DATABASE]\ntype=sqlite\npath={db.db_path}\n[APP]\nbroker_utc_offset=0\n',encoding='utf-8')
    monkeypatch.chdir(tmp_path);before=db_rows(db)
    result=cli.main(['--config',str(config),'--json','analytics','features','relation-windows','--manifest',image.manifest_id,
        '--left','qa.x','1','--right','qa.phase','1','--as-of',cutoff,'--window-bars','80','--stride-bars','40',
        '--max-windows','2','--bootstrap-replicates','29'])
    reply=json.loads(capsys.readouterr().out)
    assert result==2 and not reply['ok'] and 'windows' not in reply['data'] and db_rows(db)==before
