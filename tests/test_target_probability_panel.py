"""A closure probabilística acompanha a identidade manual, sem exceção por versão."""
from dataclasses import replace

import pytest

from mt5_extracao.robots import catalog, controls, manual_panel


@pytest.mark.parametrize("dependency", ["Policy", "Feed", "View", "Model"])
def test_transitive_probability_source_is_required_for_manual_play(tmp_path, monkeypatch, dependency):
    root = tmp_path / "biblioteca"
    expert = root / "Experts/HADOUKEN/HDK_HADOUKEN_Painel.mq5"
    safety = root / "Include/Framework/HADOUKEN/Safety"
    indicators = safety.parent / "Indicators"
    safety.mkdir(parents=True)
    indicators.mkdir()
    expert.parent.mkdir(parents=True)
    expert.write_text('#property version "7.10"\n'
                      '#include <Framework/HADOUKEN/Safety/Policy.mqh>\n'
                      '#include <Framework/HADOUKEN/Safety/View.mqh>\n'
                      'input bool InpProbManage=false;\nvoid OnTick(){}\n', encoding="utf-8")
    (safety / "Policy.mqh").write_text('#include "Feed.mqh"\n', encoding="utf-8")
    (safety / "Feed.mqh").write_text('#include "../Indicators/Model.mqh"\n', encoding="utf-8")
    (safety / "View.mqh").write_text('// apresentação independente\n', encoding="utf-8")
    (indicators / "Model.mqh").write_text('double probability(){return 80;}\n', encoding="utf-8")
    robot = catalog.robot_from_file(expert)
    pin = catalog.fingerprint(robot)
    monkeypatch.setattr(manual_panel, "APPROVED_SOURCE_FINGERPRINTS", {pin})
    assert manual_panel.recognized(robot)
    assert next(x.default for x in robot.inputs if x.name == "InpProbManage") == "false"
    assert controls.selection(robot, "EURUSD", "M5")["play_available"]
    assert not manual_panel.recognized(replace(robot, has_source=False))
    assert not manual_panel.recognized(replace(robot, key="mql5:automatico"))
    changed = (indicators if dependency == "Model" else safety) / (dependency + ".mqh")
    changed.write_text(changed.read_text(encoding="utf-8") + "// revisão não reconhecida\n", encoding="utf-8")
    assert catalog.fingerprint(robot) != pin
    assert not controls.selection(robot, "EURUSD", "M5")["play_available"]
    # Copiar o mesmo número de versão não restaura a exceção.
    assert not manual_panel.recognized(catalog.robot_from_file(expert))
