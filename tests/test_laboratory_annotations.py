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


def test_move_select_series_visibility_view_and_identity_roundtrip():
    report = dict(analysis_id="a" * 64, symbol="EURUSD", grid_timeframe="M5", laboratory_version=1,
                  selection=dict(feature_ids=["momentum.rsi_14"]),
                  channels=[dict(id="M5:momentum.rsi_14", name="RSI", timeframe="M5", unit="points_0_100",
                                 parameters=dict(period=14), algorithm_version="1", source=dict(provider="x"))])
    study = Study.for_report(report)
    region = study.add("region", x0=10.0, x1=12.0)
    note = study.add("note", x=11.0, y=100.0, text="n")
    assert study.hit(11.0, 100.0, .2, 1.0) == note["id"]  # nota vence o interior da região
    assert study.hit(10.05, 50.0, .2, 1.0) == region["id"]
    assert study.hit(30.0, 0.0, .2, 1.0) is None
    study.move(region["id"], dx=1.5, dy=99)
    assert (study.items[0]["x0"], study.items[0]["x1"]) == (11.5, 13.5)
    study.move(note["id"], dx=-1, dy=2)
    assert (study.items[1]["x"], study.items[1]["y"]) == (10.0, 102.0)
    study.remove_series("M5:momentum.rsi_14")
    study.view = dict(xlim=[10.0, 20.0], cursor=12.0)
    again = Study.from_json(study.to_json(), "a" * 64, ["M5:momentum.rsi_14"])
    assert again.removed == {"M5:momentum.rsi_14"} and again.view == study.view
    assert again.identity["series"][0]["parameters"] == {"period": 14}
    assert again.identity["series"][0]["unit"] == "points_0_100"
    with pytest.raises(ValueError, match="séries ausentes"):
        Study.from_json(study.to_json(), "a" * 64, ["M5:outra"])
    with pytest.raises(ValueError, match="EURUSD M5"):
        Study.from_json(study.to_json(), "b" * 64)
    with pytest.raises(ValueError, match="não é um estudo"):
        Study.from_json("{", "a" * 64)
    # Desfazer restaura série removida e a geometria antes do arrasto, sem tocar a vista.
    assert study.undo() and not study.removed
    assert study.undo() and study.items[1]["x"] == 11.0
    assert study.view["cursor"] == 12.0


def test_version_one_study_still_reopens():
    legacy = json.dumps(dict(study_version=1, analysis_id="a" * 64, hidden_series=["s"],
                             items=[dict(id=3, kind="hline", y=1.0, text="")]))
    study = Study.from_json(legacy, "a" * 64, ["s"])
    assert study.hidden == {"s"} and study.items[0]["id"] == 3 and study.identity is None


def test_region_edge_resizes_only_that_side_and_undo_restores():
    study = Study("a" * 64)
    region = study.add("region", x0=10.0, x1=20.0)
    assert study.edge(region["id"], 10.1, .5) == "x0" and study.edge(region["id"], 15.0, .5) is None
    study.resize(region["id"], "x1", 25.0)
    assert (study.items[0]["x0"], study.items[0]["x1"]) == (10.0, 25.0)
    with pytest.raises(ValueError, match="diferentes"):
        study.resize(region["id"], "x0", 25.0)
    assert study.undo() and study.items[0]["x1"] == 20.0
