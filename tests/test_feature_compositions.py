"""Receitas de dois níveis: mesmo grafo/executor, contratos congelados, sem escrita implícita."""
import json

import pandas as pd
import pytest
from sqlalchemy import text

from mt5_extracao import cli, sdk, timeframes
from mt5_extracao.analytics import datasets, feature_store, features, indicator_registry
from tests.test_bd18_stored_catalog import db

A=("qa.a","v1")
B=("qa.b","v1")
DIFF=("qa.difference","v1")
ABS=("qa.absolute","v1")
ALL=[A,B,DIFF,ABS]


@pytest.fixture
def source(db):
    db.save_ohlcv_data("QA",timeframes.parse("M1").label,pd.DataFrame(dict(
        time=pd.date_range("2025-01-01",periods=6,freq="min"),open=100.,high=101.,low=99.,close=100.,
        tick_volume=10.,real_volume=0.,spread=1.)))
    for ident,version in [A,B]:
        features.register(db,ident,ident,version,dependencies=["bars:M1"],unit="fraction",value_type="NUMBER")
    yield db


def define(source):
    features.register_composite(source,DIFF[0],"Diferença QA",DIFF[1],operation="difference",inputs=[A,B])
    features.register_composite(source,ABS[0],"Módulo QA",ABS[1],operation="absolute",inputs=[DIFF])


def put(source,pair,value,*,observation="2025-01-01T00:00Z",available="2025-01-01T00:01Z",**changes):
    return feature_store.save_value(source,"QA","M1",*pair,observation,value,
        available_at=available,observed_at=available,**changes)


def freeze(source,selection=ALL):
    return datasets.capture(source,"QA","M1",stored_features=selection)


def evaluate(snapshot,times,targets=(ABS,)):
    return features.compose_snapshot(snapshot,pd.DataFrame(dict(decision_time=times)),targets=targets)


def test_two_level_values_match_arithmetic_oracle_with_delays_corrections_and_observation_alignment(source):
    define(source)
    put(source,A,1.)
    put(source,B,3.,available="2025-01-01T00:01:30Z")
    put(source,A,9.,available="2025-01-01T00:02Z")
    put(source,B,5.,observation="2025-01-01T00:01Z",available="2025-01-01T00:03Z")
    put(source,A,10.,observation="2025-01-01T00:01Z",available="2025-01-01T00:03:10Z")
    put(source,A,999.,available="2025-01-01T00:04Z")
    snapshot=freeze(source)
    times=["2025-01-01T00:01Z","2025-01-01T00:01:30Z","2025-01-01T00:02Z",
           "2025-01-01T00:03Z","2025-01-01T00:03:10Z","2025-01-01T00:04Z"]
    result=evaluate(snapshot,times)
    rows=result["context"]
    assert [row["qa.difference::value"] for row in rows]==[None,-2.,6.,None,5.,5.]
    assert [row["qa.absolute::value"] for row in rows]==[None,2.,6.,None,5.,5.]
    assert rows[3]["qa.difference::reason"]=="PARENT_OBSERVATIONS_DIFFER"
    assert rows[0]["qa.absolute::state"]=="UNAVAILABLE"
    assert [r["feature_id"] for r in result["executed"]]==[DIFF[0],ABS[0]]
    assert result["qualification"]["materialized"] is False
    assert result["qualification"]["operational_acceptance"] is False
    for row in rows:
        if row["qa.absolute::state"]=="AVAILABLE":
            assert pd.Timestamp(row["qa.absolute::available_at_utc"])<=pd.Timestamp(row["decision_time"])
    json.dumps(result,allow_nan=False)


def test_frozen_graph_uses_existing_executor_without_current_database_or_registry(source,monkeypatch):
    define(source);put(source,A,1.);put(source,B,3.)
    snapshot=freeze(source)
    expected=evaluate(snapshot,["2025-01-01T00:02Z"])
    put(source,A,77.,available="2025-01-01T00:01:30Z")
    before=snapshot.feature_history
    calls=[]
    original=features.recalculate_affected
    def execute(*args,**kwargs):
        calls.append(kwargs["definitions"])
        return original(*args,**kwargs)
    monkeypatch.setattr(features,"recalculate_affected",execute)
    monkeypatch.setattr(feature_store,"list_definitions",lambda *a,**k:pytest.fail("não ler definições atuais"))
    monkeypatch.setattr(feature_store,"query_history",lambda *a,**k:pytest.fail("não ler fatos atuais"))
    monkeypatch.setattr(indicator_registry,"iter_core",lambda *a,**k:pytest.fail("não reconstruir fórmulas"))
    assert evaluate(snapshot,["2025-01-01T00:02Z"])==expected
    assert len(calls)==1 and len(calls[0])==2
    pd.testing.assert_frame_equal(snapshot.feature_history,before)


def test_missing_parent_and_expiry_propagate_without_turning_into_zero(source):
    define(source);put(source,A,0.,valid_until="2025-01-01T00:03Z")
    missing=evaluate(freeze(source),["2025-01-01T00:02Z"])["context"][0]
    assert missing["qa.absolute::value"] is None and missing["qa.absolute::state"]=="UNAVAILABLE"
    put(source,B,0.)
    rows=evaluate(freeze(source),["2025-01-01T00:02Z","2025-01-01T00:03Z"])["context"]
    assert rows[0]["qa.absolute::value"]==0. and rows[0]["qa.absolute::state"]=="AVAILABLE"
    assert rows[1]["qa.absolute::value"] is None and rows[1]["qa.absolute::state"]=="UNAVAILABLE"


def test_register_is_immutable_and_never_materializes_values(source):
    assert sdk.define_feature_composite(source,DIFF[0],"Diferença QA",DIFF[1],operation="difference",inputs=[A,B])
    assert not sdk.define_feature_composite(source,DIFF[0],"Diferença QA",DIFF[1],operation="difference",inputs=[A,B])
    with pytest.raises(ValueError,match="imutável|versão"):
        features.register_composite(source,DIFF[0],"Diferença QA",DIFF[1],operation="absolute",inputs=[A])
    assert feature_store.query_history(source).empty


@pytest.mark.parametrize("operation,inputs,match",[
    ("eval",[A],"desconhecida"),([], [A],"desconhecida"),("absolute",None,"exige"),
    ("difference",[A],"exige"),("absolute",[(A[0],None)],"ID/versão"),
    ("absolute",[(A[0],"missing")],"não registrado")])
def test_bad_operation_arity_or_version_rejected_before_registration(source,operation,inputs,match):
    with pytest.raises(ValueError,match=match):
        features.register_composite(source,"qa.bad","Inválido","v1",operation=operation,inputs=inputs)
    assert not any(d["feature_id"]=="qa.bad" for d in feature_store.list_definitions(source))


@pytest.mark.parametrize("kind,unit,match",[("CATEGORY","fraction","categorias"),("BOOLEAN","fraction","booleanos"),
    ("NUMBER",None,"declarados"),("NUMBER","percent","incompatíveis"),("NUMBER","price_units","incompatíveis")])
def test_types_and_units_are_not_coerced(source,kind,unit,match):
    features.register(source,"qa.typed","Typed","v1",unit=unit,value_type=kind)
    with pytest.raises(ValueError,match=match):
        features.register_composite(source,"qa.bad","Inválido","v1",operation="difference",inputs=[A,("qa.typed","v1")])


def test_every_locked_parent_must_be_captured_and_same_id_versions_cannot_be_mixed(source):
    define(source)
    with pytest.raises(ValueError,match="não congelado.*Capture"):
        evaluate(freeze(source,[DIFF,ABS]),["2025-01-01T00:02Z"])
    features.register(source,A[0],"A v2","v2",unit="fraction",value_type="NUMBER")
    features.register_composite(source,"qa.both","Duas versões","v1",operation="difference",inputs=[A,(A[0],"v2")])
    snapshot=freeze(source,[A,(A[0],"v2"),("qa.both","v1")])
    with pytest.raises(ValueError,match="Duas versões"):
        evaluate(snapshot,["2025-01-01T00:02Z"],targets=[("qa.both","v1")])


def test_adding_unrelated_live_branch_does_not_change_existing_graph_or_snapshot(source):
    define(source);put(source,A,1.);put(source,B,3.)
    snapshot=freeze(source);expected=evaluate(snapshot,["2025-01-01T00:02Z"])
    features.register_composite(source,"qa.other","Outro ramo","v1",operation="absolute",inputs=[A])
    assert evaluate(snapshot,["2025-01-01T00:02Z"])==expected
    with source.engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM _feature_values WHERE feature_id LIKE 'qa.absolute%' OR feature_id LIKE 'qa.difference%'")).scalar()==0


def test_order_duplicate_decisions_scope_reserved_columns_and_budget(source,monkeypatch):
    define(source);put(source,A,1.);put(source,B,3.)
    snapshot=freeze(source)
    times=["2025-01-01T00:02Z","2025-01-01T00:01Z","2025-01-01T00:02Z"]
    assert [r["decision_time"] for r in evaluate(snapshot,times)["context"]]==[pd.Timestamp(t).isoformat() for t in times]
    with pytest.raises(ValueError,match="fuso"):
        evaluate(snapshot,["2025-01-01T00:02"])
    with pytest.raises(ValueError,match="fora da janela"):
        evaluate(snapshot,["2025-01-01T00:07Z"])
    with pytest.raises(ValueError,match="outra origem"):
        features.compose_snapshot(snapshot,pd.DataFrame(dict(decision_time=[times[0]],symbol=["OTHER"])),targets=[ABS])
    with pytest.raises(ValueError,match="reservadas"):
        features.compose_snapshot(snapshot,pd.DataFrame({"decision_time":[times[0]],"qa.a::value":[999.]}),targets=[ABS])
    from mt5_extracao.analytics import query_service
    monkeypatch.setattr(query_service,"MAX_SELECTION_ROWS",3)
    with pytest.raises(ValueError,match="pares nó/decisão"):
        evaluate(snapshot,[times[0]])


def test_cli_definition_and_snapshot_evaluation_match_sdk(source,monkeypatch,capsys):
    monkeypatch.setattr(cli.Context,"db",property(lambda _:source))
    assert cli.main(["--json","analytics","features","define-composite","--id",DIFF[0],"--name","Diferença QA",
        "--algorithm-version","v1","--operation","difference","--input",*A,"--input",*B])==0
    assert json.loads(capsys.readouterr().out)["data"]["created"] is True
    features.register_composite(source,ABS[0],"Módulo QA",ABS[1],operation="absolute",inputs=[DIFF])
    put(source,A,1.);put(source,B,3.)
    snapshot=freeze(source)
    args=["--json","analytics","features","compose","--manifest",snapshot.manifest_id,"--target",*ABS,
          "--as-of","2025-01-01T00:02Z"]
    assert cli.main(args)==0
    output=json.loads(capsys.readouterr().out)["data"]["composition"]
    assert output==sdk.query_composite_context(source,snapshot.manifest_id,["2025-01-01T00:02Z"],targets=[ABS])


def test_export_import_keeps_original_graph_context_despite_other_database_clock(source,tmp_path):
    from mt5_extracao.database_manager import DatabaseManager
    define(source);put(source,A,1.);put(source,B,3.)
    snapshot=freeze(source)
    expected=evaluate(snapshot,["2025-01-01T00:02Z"])
    path=tmp_path/"graph.json"
    datasets.export_snapshot(source,snapshot.manifest_id,path)
    other=DatabaseManager(db_path=str(tmp_path/"other.sqlite"),broker_utc_offset=-3)
    try:
        datasets.declare_source(other,{"provider":"other","server":"Elsewhere"})
        imported=datasets.import_snapshot(other,path)
        assert evaluate(imported,["2025-01-01T00:02Z"])==expected
    finally:
        other.close()


def test_empty_decisions_and_closed_unit_contract(source):
    define(source)
    assert evaluate(freeze(source),[])["context"]==[]
    features.register(source,"qa.percent","Escala percentual","v1",unit="percent",value_type="NUMBER")
    with pytest.raises(ValueError,match="somente fraction ou price_units"):
        features.register_composite(source,"qa.abspercent","Escala não qualificada","v1",
            operation="absolute",inputs=[("qa.percent","v1")])
    features.register(source,"qa.price","Preço QA","v1",unit="price_units",value_type="NUMBER")
    features.register_composite(source,"qa.absprice","Módulo preço","v1",operation="absolute",inputs=[("qa.price","v1")])
    put(source,("qa.price","v1"),-10.)
    snap=freeze(source,[("qa.price","v1"),("qa.absprice","v1")])
    result=evaluate(snap,["2025-01-01T00:02Z"],targets=[("qa.absprice","v1")])
    assert result["context"][0]["qa.absprice::value"]==10.
    assert result["context"][0]["qa.absprice::unit"]=="price_units"


def raw_recipe(source,ident,parents,*,operation="absolute",**changes):
    recipe=dict(schema_version=1,operator_version=1,operation=operation,
        inputs=[dict(feature_id=p[0],algorithm_version=p[1]) for p in parents])
    parameters=changes.pop("parameters",dict(composition=recipe))
    spec=dict(family="composite",formula="abs(input)",unit="fraction",value_type="NUMBER",
        warmup_count=0,warmup_unit="bars",source_id="frozen_feature_context",
        availability_rule=features._COMPOSITE_AVAILABILITY)
    spec.update(changes)
    return feature_store.register_definition(source,ident,ident,"v1",parameters,
        ["feature:"+p[0] for p in parents],**spec)


def test_imported_cycle_is_rejected_before_execution(source):
    raw_recipe(source,"qa.u",[("qa.v","v1")])
    raw_recipe(source,"qa.v",[("qa.u","v1")])
    snapshot=freeze(source,[("qa.u","v1"),("qa.v","v1")])
    with pytest.raises(ValueError,match="ciclos"):
        evaluate(snapshot,["2025-01-01T00:02Z"],targets=[("qa.u","v1")])


@pytest.mark.parametrize("changes",[
    dict(operation="__import__('os').system('unexpected')"),
    dict(operation=[]),
    dict(availability_rule="other_rule"),
    dict(parameters={"composition":{"schema_version":True,"operator_version":1,
        "operation":"absolute","inputs":[{"feature_id":"qa.a","algorithm_version":"v1"}]}})])
def test_unrecognized_frozen_recipe_never_becomes_executable_code(source,changes):
    raw_recipe(source,"qa.bad",[A],**changes)
    snapshot=freeze(source,[A,("qa.bad","v1")])
    with pytest.raises(ValueError):
        evaluate(snapshot,["2025-01-01T00:02Z"],targets=[("qa.bad","v1")])


def test_graph_budget_non_text_columns_and_non_finite_result_are_explicit(source,monkeypatch):
    define(source);put(source,A,1e308);put(source,B,-1e308)
    snapshot=freeze(source)
    row=evaluate(snapshot,["2025-01-01T00:02Z"])["context"][0]
    assert row["qa.difference::reason"]=="NON_FINITE_RESULT" and row["qa.difference::value"] is None
    assert row["qa.absolute::state"]=="UNAVAILABLE"
    with pytest.raises(ValueError,match="colunas textuais"):
        features.compose_snapshot(snapshot,pd.DataFrame({"decision_time":["2025-01-01T00:02Z"],1:[0]}),targets=[ABS])
    monkeypatch.setattr(features,"MAX_COMPOSITION_NODES",3)
    with pytest.raises(ValueError,match="3 nós"):
        evaluate(snapshot,["2025-01-01T00:02Z"])


def test_stored_direction_codes_keep_categorical_semantics_and_cannot_become_arithmetic(source):
    from mt5_extracao.analytics import indicator_contracts
    spec=indicator_contracts.contract("trend.alligator_alignment")
    feature_store.register_definition(source,spec["feature_id"],spec["name"],spec["algorithm_version"],
        spec["parameters"],["bars:M1"],**{key:spec[key] for key in (
            "family","formula","unit","warmup_count","warmup_unit","availability_rule","value_type")})
    with pytest.raises(ValueError,match="categorias"):
        features.register_composite(source,"qa.encoded","Não converter","v1",operation="absolute",
            inputs=[(spec["feature_id"],spec["algorithm_version"])])


def test_evaluation_identity_tracks_decision_order_and_handles_module_path_alias(source,monkeypatch):
    from pathlib import Path
    define(source);put(source,A,1.);put(source,B,3.)
    snapshot=freeze(source)
    times=["2025-01-01T00:01Z","2025-01-01T00:02Z"]
    first=evaluate(snapshot,times)
    again=evaluate(snapshot,list(reversed(times)))
    assert len(first["implementation_sha256"])==64
    assert first["evaluation_id"]!=again["evaluation_id"]
    assert first["implementation_sha256"]==again["implementation_sha256"]
    module=Path(features.__file__).resolve()
    monkeypatch.setattr(features,"__file__",str(module.parent/".."/"analytics"/module.name))
    assert evaluate(snapshot,times)==first


def test_frozen_definitions_opt_in_does_not_read_current_graph_even_when_empty(monkeypatch):
    monkeypatch.setattr(feature_store,"list_definitions",lambda *a,**k:pytest.fail("grafo congelado vazio"))
    assert features.affected_by(None,["feature:qa.a"],definitions=[])==[]
    assert features.recalculate_affected(None,["feature:qa.a"],{},definitions=[])==[]


def test_provenance_of_a_decision_does_not_depend_on_other_rows_being_unavailable(source):
    define(source);put(source,A,1.);put(source,B,3.)
    snapshot=freeze(source)
    single=evaluate(snapshot,["2025-01-01T00:02Z"])["context"][0]
    mixed=evaluate(snapshot,["2025-01-01T00:00Z","2025-01-01T00:02Z"])["context"][1]
    assert single["qa.difference::metadata_json"]==mixed["qa.difference::metadata_json"]
    assert single["qa.absolute::source_revision"]==mixed["qa.absolute::source_revision"]


def test_cli_plain_output_shows_values_units_and_unavailable_states(source,monkeypatch,capsys):
    define(source);put(source,A,1.);put(source,B,3.)
    snapshot=freeze(source)
    monkeypatch.setattr(cli.Context,"db",property(lambda _:source))
    assert cli.main(["analytics","features","compose","--manifest",snapshot.manifest_id,"--target",*ABS,
                     "--as-of","2025-01-01T00:00Z","--as-of","2025-01-01T00:02Z"])==0
    output=capsys.readouterr().out
    assert "qa.absolute::value" in output and "2.0" in output and "fraction" in output
    assert "UNAVAILABLE" in output and "AVAILABLE" in output
    assert "NaN" not in output and "—" in output


def test_cli_discovers_the_operator_catalog_instead_of_maintaining_another_list():
    parser=cli.build_parser()
    sub=next(action for action in parser._actions if action.dest=="command")
    analytics=next(action for action in sub.choices["analytics"]._actions if action.dest=="analytics_domain")
    feature_actions=next(action for action in analytics.choices["features"]._actions if action.dest=="features_action")
    define=feature_actions.choices["define-composite"]
    operation=next(action for action in define._actions if action.dest=="operation")
    assert tuple(operation.choices)==features.composition_operations()
