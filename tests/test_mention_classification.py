"""Classificação por objeto, disponibilidade tardia e revisão sem vazar texto."""
from copy import deepcopy
from datetime import datetime,timezone
import json

import pandas as pd
import pytest
from sqlalchemy import text

from mt5_extracao import sdk
from mt5_extracao.analytics import feature_store,mention_classifier,mention_classification,indicator_materializer
from tests.test_mentions_contract import db,selection
from tests.test_mention_ingestion import batch


def options():
    s=selection();s["subject"].update(id="company:acme",label="Acme",aliases=["Acme"])
    s["query"].update(expression="Acme",languages=[])
    s["classifier"]=mention_classifier.descriptor();return s


def prepared(db):
    s=options();b=batch(db,s);b.pop("annotations");b.pop("classified_at")
    bodies=["Acme is excellent.","I love Acme.","Acme is wonderful.","Acme is awful.","I hate Acme.",
            "Acme is available today.","Acme has a new version.","Acme is on the list.",
            "Acme is good but Acme is bad.","Is Acme actually useful?"]
    for d,body in zip(b["documents"],bodies):d.update(text=body,language="en")
    sdk.ingest_mention_batch(db,selection=s,batch=b,synthetic_fixture=True)
    return s,b


def classify(db,s,b,**kw):return sdk.classify_mention_batch(db,selection=s,batch=b,synthetic_fixture=True,**kw)


def test_qualification_artifact_and_counts_q10_p3_n2_with_late_clock_and_replay(db):
    capability=sdk.mention_classifier_capabilities();assert capability["benchmark_matches"]
    assert capability["qualification"]["independent_human_validation"] is False
    s,b=prepared(db);cut=datetime.now(timezone.utc).isoformat();before=feature_store.query_as_of(db,cut)
    result=classify(db,s,b);assert result["counts"]==dict(inserted=0,revised=2,unchanged=0)
    assert result["receipt"]["label_counts"]==dict(positive=3,negative=2,neutral=3,mixed=1,abstained=1)
    pd.testing.assert_frame_equal(feature_store.query_as_of(db,cut),before)
    now=feature_store.query_as_of(db,datetime.now(timezone.utc).isoformat())
    assert dict(zip(now.feature_id,now.numeric_value))=={"mentions.quantity":10.,"mentions.positive":3.,"mentions.negative":2.}
    assert len(feature_store.query_history(db,feature_id="mentions.quantity"))==1
    assert not any(d["text"] in str(now.metadata_json.tolist()) for d in b["documents"])
    pn=json.loads(now[now.feature_id=="mentions.positive"].iloc[0].metadata_json)
    assert len(pn["classification_outputs"])==10 and pn["classification_qualified"]
    original=feature_store.query_history(db)
    assert classify(db,s,b)["counts"]==dict(inserted=0,revised=0,unchanged=2)
    assert sdk.ingest_mention_batch(db,selection=s,batch=b,synthetic_fixture=True)["counts"]==dict(inserted=0,revised=0,unchanged=1)
    pd.testing.assert_frame_equal(feature_store.query_history(db),original)


def test_same_post_has_opposite_labels_for_two_objects_and_alias_words_count_once(db):
    s=options();b=batch(db,s);b.pop("annotations");b.pop("classified_at");b["documents"]=b["documents"][:1]
    b["documents"][0].update(text="Acme is excellent. Beta is awful. Acme is great.",language="en")
    sdk.ingest_mention_batch(db,selection=s,batch=b,synthetic_fixture=True);a=classify(db,s,b)
    t=deepcopy(s);t["subject"].update(id="company:beta",label="Beta",aliases=["Beta"]);t["query"]["expression"]="Beta"
    c=deepcopy(b);c["scope_hash"]=sdk.describe_mention_series(db,**t)["scope_hash"]
    sdk.ingest_mention_batch(db,selection=t,batch=c,synthetic_fixture=True);z=classify(db,t,c)
    assert a["receipt"]["quantity"]==z["receipt"]["quantity"]==1
    assert a["receipt"]["label_counts"]=={"positive":1} and z["receipt"]["label_counts"]=={"negative":1}


def test_population_change_invalidates_old_labels_but_preserves_old_cut(db):
    s,b=prepared(db);classify(db,s,b);cut=datetime.now(timezone.utc).isoformat();old=feature_store.query_as_of(db,cut)
    b["documents"][0].update(revision="2",text="Acme is awful.")
    b['captured_at']=datetime.now(timezone.utc).isoformat()
    sdk.ingest_mention_batch(db,selection=s,batch=b,synthetic_fixture=True)
    current=feature_store.query_as_of(db,datetime.now(timezone.utc).isoformat());assert current[current.feature_id!="mentions.quantity"].quality.tolist()==["UNAVAILABLE","UNAVAILABLE"]
    pd.testing.assert_frame_equal(feature_store.query_as_of(db,cut),old)
    classify(db,s,b);current=feature_store.query_as_of(db,datetime.now(timezone.utc).isoformat())
    assert current[current.feature_id=="mentions.negative"].iloc[0].numeric_value==3


def test_wrong_model_missing_population_incomplete_or_annotations_refused(db):
    s,b=prepared(db)
    for change in [dict(coverage="partial",reason="missing"),dict(annotations={}),dict(scope_hash="wrong")]:
        c=dict(b,**change)
        with pytest.raises(ValueError):classify(db,s,c)
    wrong=deepcopy(s);wrong["classifier"]["version"]="unknown"
    with pytest.raises(ValueError):classify(db,wrong,b)
    b["documents"][0]["revision"]="unreceived"
    with pytest.raises(ValueError,match="população"):classify(db,s,b)
    assert len(feature_store.query_history(db))==3


def test_unsupported_language_is_unavailable_instead_of_fake_pn_zero(db):
    s,b=prepared(db)
    for d in b["documents"]:d.update(language="pt",text="Acme é muito boa.",revision="pt")
    sdk.ingest_mention_batch(db,selection=s,batch=b,synthetic_fixture=True)
    result=classify(db,s,b);assert result["job"]["status"]=="INCOMPLETE"
    rows=feature_store.query_as_of(db,datetime.now(timezone.utc).isoformat())
    assert rows[rows.feature_id!="mentions.quantity"].quality.tolist()==["UNAVAILABLE","UNAVAILABLE"]
    assert result["receipt"]["label_counts"]=={"abstained":10}


def test_cancelled_classification_keeps_q_and_pending_pn_and_closes_run(db,monkeypatch):
    s,b=prepared(db);before=feature_store.query_history(db);state={"calls":0};real=mention_classifier.classify_document
    def wrapped(*a,**k):state["calls"]+=1;return real(*a,**k)
    monkeypatch.setattr(mention_classifier,"classify_document",wrapped)
    with pytest.raises(indicator_materializer.Cancelled):classify(db,s,b,cancel_check=lambda:state["calls"]>=1)
    pd.testing.assert_frame_equal(feature_store.query_history(db),before)
    with db.engine.connect() as c:assert c.execute(text("SELECT status FROM _analytics_producer_runs WHERE family LIKE :family"),{'family':'mentions:classify:%'}).scalar_one()=="CANCELLED"


def test_generation_change_during_cpu_cannot_publish_stale_pn(db,monkeypatch):
    s,b=prepared(db);real=mention_classifier.classify_document;changed=False
    def wrapped(*a,**k):
        nonlocal changed
        if not changed:
            changed=True;c=deepcopy(b);c["documents"][0].update(revision="new",text="Acme is awful.")
            sdk.ingest_mention_batch(db,selection=s,batch=c,synthetic_fixture=True)
        return real(*a,**k)
    monkeypatch.setattr(mention_classifier,"classify_document",wrapped)
    with pytest.raises(indicator_materializer.SourceChanged):classify(db,s,b)
    current=feature_store.query_as_of(db,datetime.now(timezone.utc).isoformat())
    assert current[current.feature_id!="mentions.quantity"].quality.tolist()==["UNAVAILABLE","UNAVAILABLE"]


@pytest.mark.parametrize("body,language,expected",[
 ("Acme will rise tomorrow.","en","abstained"),("Acme is great /s","en","abstained"),
 ("Is Acme good?","en","abstained"),("Buy Acme now with discount code","en","abstained"),
 ("Acme is better than Beta","en","abstained"),("Acme é boa","pt","abstained"),
 ("Beta is great and Acme was mentioned.","en","abstained"),
 ("Acme is good. Beta is awful.","en","positive"),
 ("Acme is terrible and disappointing.",None,"negative"),
 ("1234",None,"abstained")])
def test_domain_abstention_object_targeting_and_language(body,language,expected):
    assert mention_classifier.classify_document(body,["Acme"],language=language)["label"]==expected
