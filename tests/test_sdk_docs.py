"""T10.8: a documentação do SDK não pode apodrecer: o robô de exemplo é o que a geração produz hoje."""
import json
import re
from pathlib import Path

from mt5_extracao.robots import battery, catalog, contract, generate, mql
from mt5_extracao.strategies.declared import SignalSpec

DOCS = Path(__file__).resolve().parents[1] / "docs" / "sdk"
EXAMPLE = DOCS / "exemplo"
SOURCE = EXAMPLE / "Experts" / "ExemploCruzamento" / "ExemploCruzamento.mq5"


def test_example_robot_is_valid_and_never_sends_orders_directly():
    robot = catalog.robot_from_file(SOURCE)
    assert contract.validate(robot) == [] and robot.manifest["parity_with"] == generate.DECLARED
    assert list(robot.presets) == ["ExemploCruzamento_Conservador"] and robot.dll_imports == []
    items = {i.id: i for i in battery.static_items(robot, None, [robot])}
    assert items["orders"].status == battery.OK and items["limits"].status == battery.OK
    code = mql._BLOCK_COMMENT.sub("", "\n".join(line.split("//")[0] for line in mql.read_text(SOURCE).splitlines()))
    assert not mql.ORDER_CALL.search(code)


def test_example_robot_is_exactly_what_generate_produces_today(tmp_path):
    """Se o esqueleto ou o gerador mudarem, o exemplo publicado tem de ser gerado de novo (e o tutorial, refeito)."""
    spec = SignalSpec.from_dict(json.loads((EXAMPLE / "cruzamento.json").read_text(encoding="utf-8")))
    magic = int(battery.magic_number(catalog.robot_from_file(SOURCE)))
    files = generate.create(spec, "ExemploCruzamento", tmp_path, magic=magic)
    for created in files:
        published = EXAMPLE / created.relative_to(tmp_path)
        assert published.read_text(encoding="utf-8") == created.read_text(encoding="utf-8"), published.name
    assert generate.load_spec(catalog.robot_from_file(SOURCE)).to_dict() == spec.to_dict()


def test_tutorial_links_and_commands_exist():
    from mt5_extracao import cli
    parser = cli.build_parser()
    sub = next(a for a in parser._actions if hasattr(a, "choices") and a.choices and "robots" in a.choices)
    actions = next(a for a in sub.choices["robots"]._actions if a.dest == "action").choices
    for name in ("README.md", "tutorial.md"):
        text = (DOCS / name).read_text(encoding="utf-8")
        for target in re.findall(r"\]\(([^)#]+)\)", text):
            if not target.startswith("http"):
                assert (DOCS / target).exists(), f"{name}: link quebrado para {target}"
        for action in set(re.findall(r"mt5x robots (\w+)", text)):
            assert action in actions, f"{name}: 'mt5x robots {action}' não existe"
    tutorial = (DOCS / "tutorial.md").read_text(encoding="utf-8")
    assert "não é uma estratégia para operar" in tutorial and "exatidao.md" in tutorial
    assert "Paridade dentro do critério." in tutorial and "Reprovada: corrija os itens" in tutorial
