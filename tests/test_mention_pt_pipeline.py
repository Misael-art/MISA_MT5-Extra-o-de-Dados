"""Q/P/N português tardio no mesmo store; avaliação CLI/SDK sem corpus no recibo."""
import json
from datetime import datetime,timezone

import pandas as pd

from mt5_extracao import sdk,cli
from mt5_extracao.analytics import feature_store,mention_classifier_pt,mention_qualification_pt,mention_evaluation
from tests.test_mentions_contract import db,selection
from tests.test_mention_ingestion import batch
from tests.test_mention_classifier_pt_evaluation import cases,protocol


def prepare(db):
    s=selection();s['classifier']=mention_classifier_pt.descriptor();b=batch(db,s)
    b.pop('annotations');b.pop('classified_at')
    bodies=['Empresa QA é excelente.','Adoro Empresa QA.','Empresa QA é braba.','Empresa QA é péssima.',
            'Não recomendo Empresa QA.','Empresa QA é uma companhia.','Empresa QA tem dois escritórios.',
            'Empresa QA está na agenda.','Empresa QA é boa mas Empresa QA é horrível.','Empresa QA é ótima?']
    for doc,body in zip(b['documents'],bodies):doc.update(text=body,language='pt')
    sdk.ingest_mention_batch(db,selection=s,batch=b,synthetic_fixture=True)
    return s,b


def test_portuguese_classifier_reuses_late_count_revision_without_changing_q(db):
    s,b=prepare(db);cut=datetime.now(timezone.utc).isoformat();before=feature_store.query_as_of(db,cut)
    result=sdk.classify_mention_batch(db,selection=s,batch=b,synthetic_fixture=True)
    assert result['receipt']['label_counts']==dict(positive=3,negative=2,neutral=3,mixed=1,abstained=1)
    assert result['receipt']['qualification']=='CONTROLLED_PORTUGUESE_BENCHMARK_ONLY'
    pd.testing.assert_frame_equal(feature_store.query_as_of(db,cut),before)
    assert len(feature_store.query_history(db,feature_id='mentions.quantity'))==1
    now=feature_store.query_as_of(db,datetime.now(timezone.utc).isoformat())
    assert dict(zip(now.feature_id,now.numeric_value))=={'mentions.quantity':10.,'mentions.positive':3.,'mentions.negative':2.}
    assert not any(doc['text'] in str(now.metadata_json.tolist()) for doc in b['documents'])


def test_frozen_portuguese_evaluation_keeps_model_metrics_and_mismatches():
    current=mention_evaluation.evaluate(cases(),classifier_id='object-evaluation-pt',protocol=protocol())
    frozen=mention_qualification_pt.REPORT
    for key in ['classifier','metrics','confusion','mismatches','evaluation_sha256','protocol_sha256','evaluator_sha256']:
        assert current[key]==frozen[key]
    assert frozen['criteria_met'] and not frozen['independent_human_validation']


def test_cli_can_select_portuguese_and_export_evaluation_without_text(db,tmp_path,capsys):
    cfg=tmp_path/'qa.ini';cfg.write_text(f'[DATABASE]\ntype=sqlite\npath={db.db_path}\n[APP]\nbroker_utc_offset=0\n',encoding='utf-8')
    source=tmp_path/'cases.json';source.write_text(json.dumps(dict(cases=cases(),protocol=protocol()),ensure_ascii=False),encoding='utf-8')
    out=tmp_path/'metrics.json';prefix=['--config',str(cfg),'--json','analytics','features']
    assert cli.main(prefix+['mention-classifiers','--classifier','object-evaluation-pt'])==0
    assert json.loads(capsys.readouterr().out)['data']['classifier']['benchmark_matches']
    assert cli.main(prefix+['mention-evaluate','--input',str(source),'--classifier','object-evaluation-pt','--out',str(out)])==0
    assert json.loads(capsys.readouterr().out)['data']['evaluation']['cases']==30
    assert 'Orion' not in out.read_text(encoding='utf-8')
    assert cli.main(prefix+['mention-evaluate','--input',str(source),'--classifier','object-evaluation-pt','--out',str(out)])!=0
    capsys.readouterr()
