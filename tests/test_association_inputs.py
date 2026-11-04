"""Tipos/identidade explícitos e oráculos referenciais da integração T18.2/BD15a."""
import math

import numpy as np
import pandas as pd
import pytest

from mt5_extracao.analytics import associations
from tests.test_analytics_associations import _samples


@pytest.mark.parametrize('value',[True,np.bool_(False),'1','TREND',[1,2],complex(1,2)])
def test_mixed_object_value_does_not_become_a_continuous_number(value):
    samples=_samples(np.arange(100,dtype=float),np.arange(100,dtype=float))
    samples['x_value']=samples.x_value.astype(object);samples.at[0,'x_value']=value
    result=associations.analyze_association(samples,as_of=samples.decision_time.max(),bootstrap_replicates=9)
    assert result['status']=='UNSUPPORTED_VALUE_TYPE' and result['effect'] is None
    assert result['n_non_numeric']==1 and result['n_pairs']==99


@pytest.mark.parametrize('column',['symbol','timeframe','session_id'])
def test_missing_scope_or_session_is_not_silently_imputed(column):
    samples=_samples(np.arange(100,dtype=float),np.arange(100,dtype=float));samples.loc[0,column]=None
    with pytest.raises(ValueError):
        associations.analyze_association(samples,as_of=samples.decision_time.max(),bootstrap_replicates=9)


def test_interleaved_session_identity_cannot_claim_chronological_blocks():
    samples=_samples(np.arange(100,dtype=float),np.arange(100,dtype=float))
    samples.loc[10,'session_id']=samples.loc[0,'session_id']
    with pytest.raises(ValueError,match='intercaladas'):
        associations.analyze_association(samples,as_of=samples.decision_time.max(),bootstrap_replicates=9)


@pytest.mark.parametrize('method',['pearson','spearman'])
@pytest.mark.parametrize('slope',[-3.,3.])
def test_positive_and_negative_reference_match_independent_product_moment_oracle(method,slope):
    x=np.arange(100,dtype=float);y=slope*x+7
    centered_x=[float(v)-sum(x)/len(x) for v in x]
    centered_y=[float(v)-sum(y)/len(y) for v in y]
    expected=sum(a*b for a,b in zip(centered_x,centered_y))/math.sqrt(sum(a*a for a in centered_x)*sum(b*b for b in centered_y))
    samples=_samples(x,y)
    result=associations.analyze_association(samples,method=method,as_of=samples.decision_time.max(),bootstrap_replicates=29)
    assert result['status']=='OK' and result['effect']==pytest.approx(expected,abs=1e-12)
    assert -1<=result['ci_low']<=result['ci_high']<=1 and 0<=result['p_raw']<=1


@pytest.mark.parametrize('method',['pearson','spearman'])
def test_balanced_factorial_reference_has_zero_linear_and_rank_association(method):
    samples=_samples(np.tile([-1.,-1.,1.,1.],25),np.tile([-1.,1.,-1.,1.],25))
    result=associations.analyze_association(samples,method=method,as_of=samples.decision_time.max(),bootstrap_replicates=29)
    assert result['status']=='OK' and result['effect']==pytest.approx(0.,abs=1e-12) and result['p_raw']==1.


def test_resource_guard_cancels_inference_without_returning_partial_statistics():
    samples=_samples(np.arange(100,dtype=float),np.arange(100,dtype=float));calls=[]
    def guard():
        calls.append(True)
        if len(calls)==4:raise TimeoutError('orçamento QA')
    with pytest.raises(TimeoutError,match='orçamento QA'):
        associations.analyze_association(samples,as_of=samples.decision_time.max(),bootstrap_replicates=100,guard=guard)
    assert len(calls)==4


def test_empty_population_does_not_bypass_invalid_configuration():
    samples=_samples(np.array([]),np.array([]))
    samples=pd.DataFrame(columns=['symbol','timeframe','session_id','decision_time','x_value','y_value','x_available_at_utc','y_available_at_utc'])
    with pytest.raises(ValueError,match='minimum_pairs'):
        associations.analyze_association(samples,as_of='2026-01-01T00:00Z',minimum_pairs=True)
