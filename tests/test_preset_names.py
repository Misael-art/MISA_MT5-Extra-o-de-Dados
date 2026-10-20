"""T19.50: rótulo/slug/ID/hash e caminho portátil, sem gerar estratégia/preset."""
from dataclasses import replace

import pytest

from mt5_extracao.presets.compiler import PresetIdentity
from mt5_extracao.presets.names import make_name,portable_slug,inspect_name,category_label

HASH="a"*64


def test_default_and_named_manual_have_canonical_identity_without_discarding_label():
    default=PresetIdentity("one",category="Default")
    name=make_name("HDK_S09_Referencia","EURUSD","M5",default,HASH)
    assert name.filename=="HDK-S09-Referencia__EURUSD__M5__Default__Normal__v001__aaaaaaaaaaaa.set"
    assert name.label=="Default · Padrão do código" and name.manual_slug is None
    manual=PresetIdentity("two",category="Manual",label="São João — manhã",default_origin="one")
    own=make_name("HDK_S09_Referencia","EURUSD","M5",manual,HASH)
    assert "__Manual__Sao-Joao-manha__Normal__v001__" in own.filename
    assert own.label==manual.label and own.manual_slug=="Sao-Joao-manha"
    parsed=inspect_name(own.filename)
    assert parsed["format_version"]==1 and parsed["manual_slug"]==own.manual_slug
    assert parsed["revision"]==1 and parsed["short_config_hash"]==HASH[:12]
    assert not parsed["rename_required"]


def test_export_names_are_stable_and_parameter_revision_or_orientation_is_visible():
    identity=PresetIdentity("id",category="Manual",label="Teste")
    first=make_name("S09","EURUSD","M5",identity,HASH)
    assert first==make_name("S09","EURUSD","M5",identity,HASH)
    assert first.filename!=make_name("S09","EURUSD","M5",replace(identity,revision=2),HASH).filename
    assert first.filename!=make_name("S09","EURUSD","M5",identity,"b"*64).filename
    inverse=make_name("S09","EURUSD","M5",identity,HASH,orientation="inverted")
    assert "__Inverso__" in inverse.filename
    assert inverse.filename!=first.filename


def test_slug_case_and_accent_collisions_are_explicit_for_store_to_reject():
    a=make_name("S09","EURUSD","M5",PresetIdentity("one",category="Manual",label="João"),HASH)
    b=make_name("s09","eurusd","M5",PresetIdentity("two",category="Manual",label="joao"),HASH)
    assert a.collision_key==b.collision_key
    assert a.label!=b.label
    # Identidades distintas não ganham permissão para sobrescrever o mesmo caminho.
    assert portable_slug("A/B\\C: D.. ")=="A-B-C-D"


@pytest.mark.parametrize("name",[""," ","\n","CON","con.","AUX","NUL","PRN","COM1","LPT9","🚀"])
def test_reserved_or_empty_slug_is_actionable_on_both_platforms(name):
    with pytest.raises(ValueError):portable_slug(name)


def test_maximum_length_does_not_truncate_hash_or_silently_overwrite():
    identity=PresetIdentity("id",category="Manual",label="x"*80)
    with pytest.raises(ValueError,match="reduza o nome"):
        make_name("r"*80,"S"*30,"M5",identity,HASH)
    good=make_name("S09","EURUSD","M5",identity,HASH)
    assert len(good.filename[:-4])<=180 and good.filename.endswith(HASH[:12]+".set")


@pytest.mark.parametrize("kwargs",[dict(timeframe="TICKS"),dict(orientation="unknown"),dict(config_hash="abc123"),
                                    dict(config_hash="A"*64)])
def test_unknown_context_or_hash_cannot_create_filename(kwargs):
    values=dict(robot="S09",symbol="EURUSD",timeframe="M5",identity=PresetIdentity("id",category="Default"),config_hash=HASH)
    values.update(kwargs)
    with pytest.raises(ValueError):make_name(**values)


def test_legacy_and_maximalista_alias_are_read_without_rename_or_new_capability():
    assert category_label("Maximalista")==category_label("Fator de lucro")=="FatorLucro"
    legacy=inspect_name("Meu preset antigo.set")
    assert legacy["legacy"] and legacy["filename"]=="Meu preset antigo.set" and not legacy["rename_required"]
    alias="S09__EURUSD__M5__Maximalista__Normal__v001__aaaaaaaa.set"
    parsed=inspect_name(alias)
    assert parsed["category"]=="FatorLucro" and parsed["category_original"]=="Maximalista"
    assert parsed["filename"]==alias and not parsed["rename_required"]
    with pytest.raises(ValueError):make_name("S09","EURUSD","M5",PresetIdentity("draft"),HASH)
