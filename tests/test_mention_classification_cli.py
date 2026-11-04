"""CLI explícita: Q antes de P/N, estado/versão e consulta sem inferência."""
import json

from mt5_extracao import cli,sdk
from mt5_extracao.analytics import mention_classifier
from tests.test_mentions_contract import db
from tests.test_mention_classification import prepared
from tests.test_mention_sources import transport,document,Response,hn_selection


def test_cli_classifies_received_batch_and_reads_without_running_model(db,tmp_path,capsys,monkeypatch):
    s,b=prepared(db)
    cfg=tmp_path/'qa.ini';cfg.write_text(f'[DATABASE]\ntype=sqlite\npath={db.db_path}\n[APP]\nbroker_utc_offset=0\n')
    selected=tmp_path/'selection.json';selected.write_text(json.dumps(s))
    batch=tmp_path/'batch.json';batch.write_text(json.dumps(b))
    prefix=['--config',str(cfg),'--json','analytics','features']
    assert cli.main(prefix+['mention-classifiers'])==0
    assert json.loads(capsys.readouterr().out)['data']['classifier']['benchmark_matches']
    assert cli.main(prefix+['mentions-classify','--selection',str(selected),'--batch',str(batch),'--synthetic-fixture'])==0
    assert json.loads(capsys.readouterr().out)['data']['capture']['receipt']['label_counts']['positive']==3
    monkeypatch.setattr(mention_classifier,'classify_document',lambda *a,**k:(_ for _ in ()).throw(AssertionError('read ran model')))
    assert cli.main(prefix+['series','--symbol','QA','--tf','M1','--feature','mentions.positive','--as-of','2026-10-29T23:59:59Z'])==0
    capsys.readouterr()


def test_cli_public_capture_then_classification_uses_same_population_and_separate_runs(db,tmp_path,capsys,monkeypatch):
    s=hn_selection();s['classifier']=mention_classifier.descriptor()
    def response(url):
        if url.endswith('newstories.json'):return Response([1,2])
        item=document(int(url.rsplit('/',1)[-1].split('.')[0]));item.update(title='Empresa QA is a wonderful and useful tool for our team.',text='')
        return Response(item)
    transport(monkeypatch,response)
    cfg=tmp_path/'qa.ini';cfg.write_text(f'[DATABASE]\ntype=sqlite\npath={db.db_path}\n[APP]\nbroker_utc_offset=0\n')
    selected=tmp_path/'selection.json';selected.write_text(json.dumps(s))
    args=['--config',str(cfg),'--json','analytics','features','mentions-collect','--selection',str(selected),
          '--start','2025-01-01T10:00Z','--end','2025-01-01T10:01Z','--classify']
    assert cli.main(args)==0
    result=json.loads(capsys.readouterr().out)['data']['capture']
    assert result['receipt']['quantity']==2
    assert result['classification']['receipt']['label_counts']=={'positive':2}
    assert result['job']['run_id']!=result['classification']['job']['run_id']
    assert cli.main(args)==0
    result=json.loads(capsys.readouterr().out)['data']['capture']
    assert result['counts']['unchanged']==1 and result['classification']['counts']['unchanged']==2
