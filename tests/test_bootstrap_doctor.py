import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from mt5_extracao.bootstrap import config_builder, doctor, paths
from mt5_extracao.bootstrap.__main__ import main

REPO = Path(__file__).resolve().parents[1]


def test_check_python_ok():
    assert doctor.check_python().status in (doctor.OK, doctor.WARN)


def test_check_module_missing_is_fail_or_warn():
    assert doctor.check_module("modulo_que_nao_existe_xyz", "x").status == doctor.FAIL
    assert doctor.check_module("modulo_que_nao_existe_xyz", "x", required=False).status == doctor.WARN


def test_check_config_missing(tmp_path):
    checks = doctor.check_config(tmp_path)
    assert checks[0].status == doctor.FAIL
    assert "configure" in checks[0].hint


def test_check_config_bad_mt5_path(tmp_path):
    cfg = config_builder.build_config(None, {"MT5": {"path": str(tmp_path / "nao-existe")}})
    config_builder.write_config(paths.config_path(tmp_path), cfg)
    (tmp_path / "database").mkdir()
    by_name = {c.name: c for c in doctor.check_config(tmp_path)}
    assert by_name["Caminho do MT5"].status == doctor.FAIL
    assert by_name["Pasta do banco de dados"].status == doctor.OK


def test_format_report_summary():
    text = doctor.format_report([doctor.Check("a", doctor.OK, "x"),
                                 doctor.Check("b", doctor.FAIL, "y", "faça z")])
    assert "faça z" in text and "1 falha" in text


def test_doctor_json_exit_code(tmp_path, capsys):
    code = main(["--root", str(tmp_path), "doctor", "--json", "--no-probe"])
    out = capsys.readouterr().out
    assert code == 1  # sem config.ini -> FALHA
    assert '"status"' in out


def _free_port():
    import socket
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_bridge_server_and_probe(tmp_path):
    """Sobe o servidor da ponte com um MetaTrader5 falso e verifica o probe do doctor."""
    pytest.importorskip("rpyc")
    fake = tmp_path / "fake"
    fake.mkdir()
    (fake / "MetaTrader5.py").write_text('__version__ = "0.0-teste"\n', encoding="utf-8")
    port = _free_port()
    env = dict(os.environ, PYTHONPATH=str(fake))
    proc = subprocess.Popen([sys.executable, str(REPO / "scripts" / "mt5_bridge_server.py"), "--port", str(port)],
                            env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    try:
        deadline = time.time() + 20
        while not doctor._port_open("127.0.0.1", port, timeout=0.5):
            assert proc.poll() is None, proc.stdout.read().decode(errors="replace")
            assert time.time() < deadline, "ponte não iniciou"
            time.sleep(0.2)
        check = doctor.probe_bridge("127.0.0.1", port)
        assert check.status == doctor.OK, check.detail
        assert "0.0-teste" in check.detail
    finally:
        proc.terminate()
        proc.wait(timeout=10)


def test_bridge_server_check_fails_without_mt5(tmp_path):
    pytest.importorskip("rpyc")
    code = subprocess.call([sys.executable, str(REPO / "scripts" / "mt5_bridge_server.py"), "--check"],
                           env=dict(os.environ, PYTHONPATH=""), cwd=str(tmp_path),
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    # Verifica num subprocesso: o conftest injeta um MetaTrader5 falso neste processo
    has_mt5 = subprocess.call([sys.executable, "-c", "import MetaTrader5"], cwd=str(tmp_path),
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) == 0
    assert code == (0 if has_mt5 else 1)
