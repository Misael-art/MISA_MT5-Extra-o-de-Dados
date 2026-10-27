"""LAB-08: modelo de anotações do estudo, sem Tk."""
import json

import pytest

from mt5_extracao.laboratory.annotations import Study


def test_add_edit_remove_undo_and_roundtrip_keep_identity():
    study = Study("a" * 64)
    line = study.add("hline", y=101.5, text="suporte")
    study.add("vline", x=20000.5)
    region = study.add("region", x0=20001.0, x1=20000.0)
    assert (region["x0"], region["x1"]) == (20000.0, 20001.0)
    note = study.add("note", x=20000.2, y=100.0, text="rompimento")
    study.edit(note["id"], text="rompimento falso")
    study.remove(line["id"])
    assert [item["kind"] for item in study.items] == ["vline", "region", "note"]
    assert study.undo() and study.items[0]["text"] == "suporte"
    study.hidden = {"M5:momentum.rsi_14"}
    again = Study.from_json(study.to_json(), "a" * 64)
    assert again.items == study.items and again.hidden == study.hidden
    assert again.add("hline", y=1.0)["id"] == max(item["id"] for item in study.items) + 1


def test_rejects_other_analysis_invalid_coordinates_and_empty_note():
    study = Study("a" * 64)
    with pytest.raises(ValueError, match="outra análise"):
        Study.from_json(study.to_json(), "b" * 64)
    for kind, values in (("hline", dict(y=float("nan"))), ("region", dict(x0=1.0, x1=1.0)),
                         ("note", dict(x=1.0, y=1.0, text=" ")), ("arrow", {})):
        with pytest.raises(ValueError):
            study.add(kind, **values)
    assert not study.items and not study.undo()
    json.loads(study.to_json())
