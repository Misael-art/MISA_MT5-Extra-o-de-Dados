"""Avaliação reproduzível de classificadores; não grava corpus nem certifica origem humana."""
from collections import Counter
import hashlib
import json
from pathlib import Path

from . import mentions

LABELS=('positive','negative','neutral','mixed','abstained')


def evaluate(cases,*,classifier_id,protocol):
    from . import mention_classifier,mention_classifier_pt
    model={mention_classifier.CLASSIFIER_ID:mention_classifier,mention_classifier_pt.CLASSIFIER_ID:mention_classifier_pt}.get(classifier_id)
    if model is None:raise ValueError('Classificador desconhecido; consulte mention-classifiers.')
    protocol=mentions._object(protocol,{'annotation_origin','development_ids'}, {'name','version'},'evaluation.protocol')
    if protocol['annotation_origin'] not in {'codex_authored','user_provided','independent_human_declared'}:
        raise ValueError('Declare a origem da anotação; declaração não comprova independência ou representatividade.')
    development=set(mentions._labels(protocol['development_ids'],'development_ids'))
    if not isinstance(cases,list) or not 1<=len(cases)<=1000 or len(json.dumps(cases,ensure_ascii=False).encode())>4*1024*1024:
        raise ValueError('Avaliação exige1…1000 casos, até4MiB, sem truncamento.')
    rows=[];seen=set();documents=set()
    for case in cases:
        case=mentions._object(case,{'id','text','aliases','language','expected'},set(),'evaluation.case')
        ident=mentions._text(case['id'],'case.id')
        if ident in seen or ident in development:raise ValueError('Casos de avaliação precisam ser distintos e separados do desenvolvimento.')
        if case['expected'] not in LABELS:raise ValueError('Rótulo esperado desconhecido.')
        seen.add(ident)
        if not isinstance(case['text'],str):raise ValueError('Caso exige texto original, não score.')
        fingerprint=mentions._hash(dict(text=case['text'],aliases=case['aliases']))
        if fingerprint in documents:raise ValueError('Documento/objeto duplicado infla a amostra de avaliação.')
        documents.add(fingerprint)
        actual=model.classify_document(case['text'],case['aliases'],language=case['language'])
        rows.append(dict(case_index=len(rows),case_sha256=mentions._hash(ident),expected=case['expected'],predicted=actual['label'],reason=actual['reason']))
    counts=Counter((r['expected'],r['predicted']) for r in rows);metrics={}
    for label in LABELS:
        support=sum(n for (a,b),n in counts.items() if a==label)
        predicted=sum(n for (a,b),n in counts.items() if b==label)
        correct=counts[label,label]
        metrics[label]=dict(support=support,predicted=predicted,
            precision=correct/predicted if predicted else None,recall=correct/support if support else None)
    return dict(classifier=model.descriptor(),cases=len(rows),metrics=metrics,
        confusion=[dict(expected=a,predicted=b,count=n) for (a,b),n in sorted(counts.items())],
        mismatches=[r for r in rows if r['expected']!=r['predicted']],
        evaluation_sha256=mentions._hash(cases),protocol_sha256=mentions._hash(protocol),
        evaluator_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),annotation_origin=protocol['annotation_origin'],
        independent_human_validation=False,representative_source_validation=False,
        annotation_independence='DECLARED_NOT_VERIFIED' if protocol['annotation_origin']=='independent_human_declared' else 'NOT_INDEPENDENT',
        qualification='CONTROLLED_EVALUATION_ONLY',raw_text_exported=False,network_required=False,
        financial_utility_certified=False)
