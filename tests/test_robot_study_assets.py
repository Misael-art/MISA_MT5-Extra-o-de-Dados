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


def test_multiple_graphs_add_series_asset_to_review_and_compile_plan(tmp_path):
    root, expert, candles = project(tmp_path)
    graphs = root / "Include/Framework/HADOUKEN/Safety/HDK_StudyGraphs.mqh"
    graphs.write_text('#include "HDK_StudyLayers.mqh"\n')
    expert.write_text(expert.read_text().replace("HDK_StudyLayers", "HDK_StudyGraphs"))
    series = candles.with_name("HDK_StudySeries.mq5")
    with pytest.raises(ValueError, match="HDK_StudySeries"):
        catalog.robot_from_file(expert)
    series.write_text('#property indicator_separate_window\n')
    robot = catalog.robot_from_file(expert)
    old = catalog.fingerprint(robot)
    series.write_text(series.read_text()+"// nova fórmula revisada\n")
    assert catalog.fingerprint(robot) != old
    items = mt5_tools.install_plan(robot, tmp_path / "profile")
    assert {item.target.name for item in mt5_tools.indicator_dependencies(items)} == {
        "HDK_StudyCandles.mq5", "HDK_StudySeries.mq5"}
    assert {str(candles), str(series)} <= set(robot.files)


def test_legacy_color_library_does_not_require_new_graph_asset(tmp_path):
    root, expert, candles = project(tmp_path)
    # A atualização da App continua aceitando a biblioteca G19.6, que só colore.
    assert not candles.with_name("HDK_StudySeries.mq5").exists()
    robot = catalog.robot_from_file(expert)
    assert [item.target.name for item in mt5_tools.indicator_dependencies(
        mt5_tools.install_plan(robot, tmp_path / "profile"))] == [candles.name]


def test_safety_distribution_uses_its_own_indicator_namespace(tmp_path):
    root = tmp_path / "safety"
    expert = root / "Experts/MT5X/Smoke.mq5"
    header = root / "Include/MT5X/Safety/HDK_StudyGraphs.mqh"
    series = root / "Indicators/MT5X/Study/HDK_StudySeries.mq5"
    for item in (expert, header, series):
        item.parent.mkdir(parents=True, exist_ok=True)
    expert.write_text('#include <MT5X/Safety/HDK_StudyGraphs.mqh>\nint OnInit(){return 0;}\nvoid OnTick(){}\n')
    header.write_text('// camada gráfica\n')
    series.write_text('#property indicator_separate_window\n')
    robot = catalog.robot_from_file(expert)
    assert str(series) in robot.files
    assert mt5_tools.indicator_dependencies(mt5_tools.install_plan(robot, tmp_path / "profile"))[0].target == (
        tmp_path / "profile/MQL5/Indicators/MT5X/Study/HDK_StudySeries.mq5")
