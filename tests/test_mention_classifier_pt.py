"""Casos próprios, não revisão humana independente nem amostra representativa."""
from collections import Counter
import json
import hashlib

import pytest

from mt5_extracao.analytics import mention_classifier,mention_classifier_pt

CASES=[
 ('p01','Empresa QA é excelente.','positive'),
 ('p02','Empresa QA é ótima para trabalhar.','positive'),
 ('p03','Empresa QA é confiável.','positive'),
 ('p04','Eu adoro a Empresa QA.','positive'),
 ('p05','Recomendo Empresa QA.','positive'),
 ('p06','Empresa QA é braba.','positive'),
 ('p07','Eu curto Empresa QA.','positive'),
 ('p08','Empresa QA é top.','positive'),
 ('n01','Empresa QA é péssima.','negative'),
 ('n02','Empresa QA é terrível.','negative'),
 ('n03','Empresa QA é bugada.','negative'),
 ('n04','Eu odeio Empresa QA.','negative'),
 ('n05','Não recomendo Empresa QA.','negative'),
 ('n06','Empresa QA é lixo.','negative'),
 ('n07','Empresa QA é lenta.','negative'),
 ('n08','Detesto Empresa QA.','negative'),
 ('z01','Empresa QA foi fundada ontem.','neutral'),
 ('z02','Empresa QA tem uma sede em Brasília.','neutral'),
 ('z03','Empresa QA está na lista.','neutral'),
 ('z04','Empresa QA é uma companhia.','neutral'),
 ('m01','Empresa QA é ótima mas Empresa QA é lenta.','mixed'),
 ('m02','Adoro Empresa QA. Odeio Empresa QA.','mixed'),
 ('m03','Empresa QA é útil e bugada.','mixed'),
 ('m04','Empresa QA é excelente; Empresa QA é terrível.','mixed'),
 ('a01','Empresa QA é excelente?','abstained'),
 ('a02','Empresa QA é ótima, só que não.','abstained'),
 ('a03','Empresa QA é excelente kkk.','abstained'),
 ('a04','Empresa QA subiu dez por cento.','abstained'),
 ('a05','Empresa QA não é ruim.','abstained'),
 ('a06','Empresa QA não é boa.','abstained'),
 ('a07','Dizem que Empresa QA é excelente.','abstained'),
 ('a08','Empresa QA é boa mas o atendimento é ruim.','abstained'),
 ('a09','Orion é terrível e Empresa QA foi citada.','abstained'),
 ('a10','Empresa QA is excellent.','abstained','en'),
 ('a11','Empresa QA es excelente.','abstained','es'),
 ('a12','Empresa QA é ótima e Orion é péssimo.','abstained'),
]


def evaluation():
    return [dict(id=c[0],text=c[1],expected=c[2],language=c[3] if len(c)>3 else 'pt',aliases=['Empresa QA']) for c in CASES]


def test_portuguese_controlled_cases_and_slang_preserve_abstentions():
    for case in evaluation():
        result=mention_classifier_pt.classify_document(case['text'],case['aliases'],language=case['language'])
        assert result['label']==case['expected'],case


def test_english_model_is_not_reinterpreted_as_portuguese():
    assert mention_classifier.descriptor()['version']=='1+0989d6273ee548d8bab7122622d0553b2032d6e1cf93def7cdde046c312ab6e1'
    assert mention_classifier_pt.descriptor()['id']!='object-vader-en'
    assert mention_classifier.classify_document('Empresa QA é excelente.',['Empresa QA'],language='pt')['label']=='abstained'


def test_portuguese_requires_explicit_alias_and_bounded_text():
    with pytest.raises(ValueError):mention_classifier_pt.classify_document('QA',[])
    with pytest.raises(ValueError):mention_classifier_pt.classify_document('x'*65537,['QA'])
    assert mention_classifier_pt.classify_document('Orion é excelente.',['Empresa QA'],language='pt')['label']=='abstained'
