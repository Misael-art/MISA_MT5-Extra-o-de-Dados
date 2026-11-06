"""Classe de comando do launcher: consultas locais abrem sem a ponte MT5 pela entrada oficial."""
import argparse
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from mt5_extracao import cli, launch_policy

ROOT = Path(__file__).resolve().parents[1]


def test_global_options_are_skipped_before_the_command():
    assert launch_policy.command_name(["--config", "x.ini", "--json", "--db=y.db", "tables"]) == "tables"
    assert not launch_policy.needs_bridge(["--db", "a.db", "-v", "laboratory", "catalog"])
    assert launch_policy.needs_bridge(["extract", "--symbols", "X"])
    assert launch_policy.needs_bridge([]) and launch_policy.needs_bridge(["comando-novo"])


def test_every_local_command_exists_and_mt5_commands_stay_bridged():
    parser = cli.build_parser()
    commands = set(next(a for a in parser._actions if isinstance(a, argparse._SubParsersAction)).choices)
    assert launch_policy.LOCAL_COMMANDS <= commands
    for name in ("extract", "update", "ticks", "book", "symbols", "specs", "mt5", "audit", "portfolio",
                 "backtest", "robots", "doctor", "resample", "commands"):
        assert launch_policy.needs_bridge([name])


def test_local_command_that_asks_for_mt5_is_refused(monkeypatch, tmp_path):
    monkeypatch.setenv(launch_policy.LOCAL_ENV, "1")
    ctx = cli.Context(argparse.Namespace(config=str(tmp_path / "c.ini"), db=None))
    with pytest.raises(RuntimeError, match="consulta local"):
        ctx.connector()


@pytest.mark.skipif(os.name == "nt", reason="run.sh é o launcher Linux")
@pytest.mark.parametrize("argv, bridged", [(["tables"], False), (["--json", "laboratory", "catalog"], False),
                                           (["status"], False), (["extract", "--help"], True)])
def test_official_launcher_starts_bridge_only_for_mt5_commands(tmp_path, argv, bridged):
    app = tmp_path / "app"
    (app / "scripts").mkdir(parents=True)
    (app / "config").mkdir()
    shutil.copy(ROOT / "run.sh", app / "run.sh")
    for name in ("mt5_extracao", ".venv", "app.py"):
        (app / name).symlink_to(ROOT / name)
    (app / "config" / "config.ini").write_text(f"[DATABASE]\ntype = sqlite\npath = {tmp_path / 'qa.db'}\n")
    marker = tmp_path / "bridge-started"
    stub = app / "scripts" / "mt5-bridge.sh"
    stub.write_text(f"#!/usr/bin/env bash\ntouch '{marker}'\n")
    stub.chmod(0o755)
    env = {k: v for k, v in os.environ.items() if k != launch_policy.LOCAL_ENV}
    result = subprocess.run(["bash", str(app / "run.sh"), "--cli", *argv], cwd=tmp_path, env=env,
                            capture_output=True, text=True, timeout=300)
    assert result.returncode == 0, result.stderr[-2000:]
    assert marker.exists() is bridged
