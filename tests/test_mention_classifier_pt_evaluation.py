"""Avaliação própria separada dos textos de desenvolvimento; erros conservados."""
import pytest
from mt5_extracao.analytics import mention_evaluation

TEXTS=[
 ('Orion é uma ferramenta excelente.','positive'),('Orion parece ótima.','positive'),
 ('Gosto de Orion.','positive'),('Eu amo Orion.','positive'),('Orion é eficiente.','positive'),
 ('Orion é show.','positive'),('Orion funciona perfeitamente.','positive'),('Orion é muito boa.','positive'),
 ('Orion é um desastre.','negative'),('Orion é inútil.','negative'),('Orion parece péssima.','negative'),
 ('Odeio Orion.','negative'),('Orion é horrível.','negative'),('Não recomendo Orion.','negative'),
 ('Orion é problemática.','negative'),('Orion é inconfiável.','negative'),
 ('Orion tem dois escritórios.','neutral'),('Orion foi fundada em janeiro.','neutral'),
 ('Orion está na agenda.','neutral'),('Orion é um programa.','neutral'),
 ('Orion é boa mas Orion é terrível.','mixed'),('Eu amo Orion. Eu odeio Orion.','mixed'),
 ('Orion não é excelente.','abstained'),('Orion é a melhor?','abstained'),
 ('Orion é boa, disseram eles.','abstained'),('Orion é ótima /s','abstained'),
 ('Orion é top kkkk.','abstained'),('Orion caiu 5%.','abstained'),
 ('Orion é melhor que outro programa.','abstained'),('Orion é ótima, mas a equipe é horrível.','abstained'),
]


def cases():
    return [dict(id='held-pt-'+str(i),text=t,aliases=['Orion'],language='pt',expected=e) for i,(t,e) in enumerate(TEXTS)]


def protocol():
    from tests.test_mention_classifier_pt import CASES
    return dict(annotation_origin='codex_authored',development_ids=[c[0] for c in CASES],name='Português explícito controlado',version='1')


def test_evaluation_does_not_claim_human_or_source_qualification_and_preserves_errors():
    report=mention_evaluation.evaluate(cases(),classifier_id='object-evaluation-pt',protocol=protocol())
    assert report['cases']==30 and report['mismatches']
    assert not report['independent_human_validation'] and not report['representative_source_validation']
    assert report['metrics']['positive']['recall']>=.75 and report['metrics']['negative']['recall']>=.75
    assert report['raw_text_exported'] is False and not report['network_required']
    assert 'Orion' not in str(report)


def test_review_declarations_and_overlap_cannot_certify_independence():
    p=protocol();p['annotation_origin']='independent_human_declared'
    report=mention_evaluation.evaluate(cases(),classifier_id='object-evaluation-pt',protocol=p)
    assert report['annotation_independence']=='DECLARED_NOT_VERIFIED' and not report['independent_human_validation']
    p['development_ids']=[cases()[0]['id']]
    with pytest.raises(ValueError,match='separados'):mention_evaluation.evaluate(cases(),classifier_id='object-evaluation-pt',protocol=p)
    with pytest.raises(ValueError,match='duplicado'):mention_evaluation.evaluate([cases()[0],dict(cases()[0],id='other')],classifier_id='object-evaluation-pt',protocol=protocol())
