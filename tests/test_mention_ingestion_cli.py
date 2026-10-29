"""Usabilidade CLI: descrever, ingerir e reler sem buscar/classificar na leitura."""
import json

from mt5_extracao import cli, sdk
from tests.test_mentions_contract import db, selection
from tests.test_mention_ingestion import batch


def test_cli_describe_ingest_repeat_and_read_reconcile_with_sdk(db,tmp_path,capsys):
    config=tmp_path/'qa.ini';config.write_text(f'[DATABASE]\ntype=sqlite\npath={db.db_path}\n[APP]\nbroker_utc_offset=0\ntime_basis=broker\n',encoding='utf-8')
    selected=tmp_path/'seleção com espaço.json';selected.write_text(json.dumps(selection()),encoding='utf-8-sig')
    data=tmp_path/'lote.json';data.write_text(json.dumps(batch(db)),encoding='utf-8')
    prefix=['--config',str(config),'--json','analytics','features']
    assert cli.main(prefix+['mentions-describe','--selection',str(selected)])==0
    assert json.loads(capsys.readouterr().out)['data']['contract']==sdk.describe_mention_series(db,**selection())
    command=prefix+['mentions-ingest','--selection',str(selected),'--batch',str(data),'--synthetic-fixture']
    assert cli.main(command)==0
    result=json.loads(capsys.readouterr().out)['data']['capture'];assert result['counts']['inserted']==3
    assert cli.main(command)==0
    assert json.loads(capsys.readouterr().out)['data']['capture']['counts']['unchanged']==3
    assert cli.main(prefix+['series','--symbol','QA','--tf','M1','--feature','mentions.quantity','--as-of','2025-01-01T11:00Z'])==0
    assert json.loads(capsys.readouterr().out)['data']['envelope']['values'][0]['value']==10


def test_cli_refuses_large_or_non_object_input_and_reports_incomplete_capture(db,tmp_path,capsys):
    from mt5_extracao.analytics import mention_ingestion
    config=tmp_path/'qa.ini';config.write_text(f'[DATABASE]\ntype=sqlite\npath={db.db_path}\n[APP]\nbroker_utc_offset=0\n',encoding='utf-8')
    selected=tmp_path/'selection.json';selected.write_text(json.dumps(selection()),encoding='utf-8')
    data=tmp_path/'batch.json';value=batch(db);value.update(coverage='unavailable',reason='SOURCE_UNAVAILABLE',documents=[])
    value.pop('annotations');value.pop('classified_at');data.write_text(json.dumps(value),encoding='utf-8')
    prefix=['--config',str(config),'--json','analytics','features']
    assert cli.main(prefix+['mentions-ingest','--selection',str(selected),'--batch',str(data),'--synthetic-fixture'])==1
    assert json.loads(capsys.readouterr().out)['data']['capture']['job']['status']=='INCOMPLETE'
    selected.write_text('[]',encoding='utf-8')
    assert cli.main(prefix+['mentions-describe','--selection',str(selected)])==2;capsys.readouterr()
    selected.write_bytes(b' '*(mention_ingestion.MAX_INPUT_BYTES+1))
    assert cli.main(prefix+['mentions-describe','--selection',str(selected)])==2
    assert not json.loads(capsys.readouterr().out)['ok']


def test_capability_cli_does_not_probe_sources(db,tmp_path,capsys,monkeypatch):
    import requests
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(requests,'get',lambda *a,**k:(_ for _ in ()).throw(AssertionError('network read')))
    config=tmp_path/'qa.ini';config.write_text(f'[DATABASE]\ntype=sqlite\npath={db.db_path}\n',encoding='utf-8')
    assert cli.main(['--config',str(config),'--json','analytics','features','mention-sources'])==0
    assert len(json.loads(capsys.readouterr().out)['data']['sources'])==9
