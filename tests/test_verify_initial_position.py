"""O ensaio E-13 nunca descobre inputs privados nem reutiliza destino de QA."""
from types import SimpleNamespace

import pytest

from mt5_extracao.composition import native
from scripts import verify_initial_position as qa


def public_robot(**changes):
    values = dict(key=native.ROBOT_KEY, version=native.PANEL_ROBOT_VERSION,
                  inputs=[SimpleNamespace(name=name, type=kind) for name, kind in native.PANEL_INPUT_TYPES.items()])
    values.update(changes)
    return SimpleNamespace(**values)


def test_receipt_uses_only_existing_reviewed_public_interface():
    robot = public_robot()
    assert qa.qa_public_fields(robot)["InpPresetId"] == "token"
    for altered in (public_robot(key="mql5:other"), public_robot(version="2.00"),
                    public_robot(inputs=robot.inputs + [SimpleNamespace(name="Password", type="string")]),
                    public_robot(inputs=robot.inputs[:-1])):
        with pytest.raises(ValueError, match="nenhum input novo"):
            qa.qa_public_fields(altered)


@pytest.mark.parametrize("end", ["2026-10-12", "2026-10-11", "2026-10-20"])
def test_qa_refuses_unbounded_or_invalid_window_before_reading_configuration(tmp_path, end):
    target = tmp_path / "new"
    with pytest.raises(ValueError, match="janela histórica"):
        qa.main(["--framework", str(tmp_path / "absent"), "--out", str(target),
                 "--from-date", "2026-10-12", "--to-date", end])
    assert not target.exists()


def test_existing_evidence_is_never_overwritten(tmp_path):
    marker = tmp_path / "keep"
    marker.write_text("evidence")
    with pytest.raises(FileExistsError, match="preservadas"):
        qa.main(["--framework", str(tmp_path / "absent"), "--out", str(tmp_path),
                 "--from-date", "2026-10-12", "--to-date", "2026-10-16"])
    assert marker.read_text() == "evidence"
