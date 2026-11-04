"""DTO2 representa pares sem inventar alvo futuro; DTO1 continua compatível."""
from copy import deepcopy
import json

import pytest

from mt5_extracao.analytics import relation_dto


def payload():
    def descriptor(ident):
        definition=dict(feature_id=ident,algorithm_version='1',unit='fraction',value_type='NUMBER',formula=None)
        return dict(**{k:definition[k] for k in ('feature_id','algorithm_version','unit','value_type')},
                    definition=definition,transform='identity')
    return dict(left=descriptor('qa.x'),right=descriptor('qa.y'),
        relation=dict(method='pearson',effect=.5,effect_ci_low=.1,effect_ci_high=.9,p_raw=.2,p_adjusted=None,
            n_pairs=4,n_sessions=2,evidence_state='EXPLORATORY',utility_state='INCONCLUSIVE',freshness_state='FROZEN_AS_OF',state='OK',reason=None),
        scope=dict(symbol='QA',timeframe='M1',lag_bars=0,lag_rule='left and right at same nominal close'),
        population=dict(n_total=6,n_eligible=4,n_distinct_source_pairs=4,exclusions={'LEFT_UNAVAILABLE':2}),
        provenance=dict(base_namespace='reference',snapshot_manifest_id='a'*64,as_of_utc='2026-01-01T00:00:00Z',
            analysis_id='b'*64,sample_hash='c'*64,implementation_sha256='d'*64),generated_at_utc='2026-01-01T00:00:00Z',synthetic_fixture=True)


def test_pair_envelope_has_no_fabricated_future_horizon_and_is_detached_from_inputs():
    kwargs=payload();dto=relation_dto.build_pair_relation_dto(**kwargs)
    assert dto['relation_schema_version']==2 and dto['kind']=='FEATURE_PAIR_RELATION' and 'target' not in dto
    assert dto['scope']['lag_bars']==0 and not dto['qualification']['financial_utility_certified']
    assert json.loads(relation_dto.dumps(dto))==dto
    kwargs['left']['definition']['formula']='changed'
    assert dto['left']['definition']['formula'] is None
    old=relation_dto.synthetic_fixture()
    assert old['relation_schema_version']==1 and old['target']['horizon_bars']==3
    assert json.loads(relation_dto.dumps(old))==old


@pytest.mark.parametrize('field,value',[('effect',1.1),('effect',float('nan')),('p_raw',True),('p_raw',1.1),
    ('n_pairs',-1),('n_sessions',10),('utility_state','BENEFICIAL'),('evidence_state','CONFIRMED')])
def test_invalid_or_overstated_relation_is_rejected(field,value):
    kwargs=payload();kwargs['relation'][field]=value
    with pytest.raises(ValueError):relation_dto.build_pair_relation_dto(**kwargs)


@pytest.mark.parametrize('field,value',[('n_total',7),('n_eligible',-1),('n_distinct_source_pairs',10),('exclusions',{'bad':True})])
def test_population_counts_and_exclusions_reconcile(field,value):
    kwargs=payload();kwargs['population'][field]=value
    with pytest.raises(ValueError):relation_dto.build_pair_relation_dto(**kwargs)


def test_category_cannot_be_labeled_as_a_continuous_coefficient_or_retyped_without_contract_change():
    kwargs=payload();kwargs['left']['value_type']='CATEGORY';kwargs['left']['definition']['value_type']='CATEGORY'
    with pytest.raises(ValueError,match='NUMBER'):relation_dto.build_pair_relation_dto(**kwargs)
    kwargs=payload();kwargs['left']['algorithm_version']='2'
    with pytest.raises(ValueError,match='diverge'):relation_dto.build_pair_relation_dto(**kwargs)


def test_unavailable_effect_does_not_carry_invented_inference():
    kwargs=payload();kwargs['relation'].update(effect=None,reason='CONSTANT_INPUT',evidence_state='INSUFFICIENT')
    with pytest.raises(ValueError,match='inferência'):relation_dto.build_pair_relation_dto(**kwargs)
    kwargs['relation'].update(effect_ci_low=None,effect_ci_high=None,p_raw=None)
    assert relation_dto.build_pair_relation_dto(**kwargs)['relation']['effect'] is None
