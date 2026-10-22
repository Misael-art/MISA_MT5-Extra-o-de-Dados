from types import SimpleNamespace

import pytest

from mt5_extracao.robots import catalog, mt5_tools


def project(tmp_path):
    root = tmp_path / "library"
    expert = root / "Experts/HADOUKEN/Panel.mq5"
    header = root / "Include/Framework/HADOUKEN/Safety/HDK_StudyLayers.mqh"
    indicator = root / "Indicators/HADOUKEN/Study/HDK_StudyCandles.mq5"
    for path in (expert, header, indicator):
        path.parent.mkdir(parents=True, exist_ok=True)
    expert.write_text('#include <Framework/HADOUKEN/Safety/HDK_StudyLayers.mqh>\nint OnInit(){return 0;}\nvoid OnTick(){}\n')
    header.write_text('// controlador de estudo\n')
    indicator.write_text('#property indicator_chart_window\n// fonte para compilar antes do Expert\n')
    return root, expert, indicator


def test_study_indicator_source_is_reviewed_fingerprinted_and_installed_in_indicators(tmp_path):
    root, expert, indicator = project(tmp_path)
    robot = catalog.robot_from_file(expert)
    assert str(indicator) in robot.files
    old = catalog.fingerprint(robot)
    indicator.write_text(indicator.read_text()+'// alteração revisável\n')
    assert catalog.fingerprint(robot) != old
    plan = mt5_tools.install_plan(robot, tmp_path / "profile")
    assets = mt5_tools.indicator_dependencies(plan)
    assert len(assets) == 1
    assert assets[0].target == tmp_path / "profile/MQL5/Indicators/HADOUKEN/Study/HDK_StudyCandles.mq5"
    indicator.unlink()
    with pytest.raises(ValueError, match="Atualize a biblioteca"):
        catalog.robot_from_file(expert)


def test_dependencies_compile_before_main_and_failure_is_returned(tmp_path, monkeypatch):
    _, expert, _ = project(tmp_path)
    robot = catalog.robot_from_file(expert)
    items = mt5_tools.install_plan(robot, tmp_path / "profile")
    calls = []
    def compile_(source, editor, prefix, run):
        calls.append(source)
        return SimpleNamespace(ok=False, summary="Indicador precisa de correção")
    monkeypatch.setattr(mt5_tools, "compile_source", compile_)
    failure = mt5_tools.compile_indicator_dependencies(items, tmp_path / "MetaEditor.exe")
    assert not failure.ok
    assert calls == [mt5_tools.indicator_dependencies(items)[0].target]
    # Um EA legado/sem dependências preserva o caminho anterior e não precisa
    # de MetaEditor para uma dependência gráfica que não existe no seu plano.
    calls.clear()
    assert mt5_tools.compile_indicator_dependencies(items[:1], None) is None
    assert calls == []
