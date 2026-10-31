"""Executa o transporte JS real: hash e números conservados no JSON baixado."""
import base64
import hashlib
import json
from pathlib import Path
import shutil
import subprocess

import pytest

from mt5_extracao.dashboard import document, generator
from tests.test_dashboard_document import fixture


def execute(encoded, *, future=False):
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node indisponível para executar o transporte JS; teste de Browser continua próprio")
    source = Path(document.ROOT, "static/js/transport.js")
    code = r"""
global.window = {};
global.crypto = require('node:crypto').webcrypto;
if (process.argv[2] === 'future') {
  const RealDate = Date;
  global.Date = class extends RealDate { static now() {return 2208988800000;} };
}
require(process.argv[1]);
window.MT5XTransport.decode(JSON.parse(require('node:fs').readFileSync(0,'utf8')))
  .then(value=>process.stdout.write(value.json))
  .catch(error=>{process.stderr.write(error.message); process.exitCode=2;});
"""
    return subprocess.run([node, "-e", code, str(source), "future" if future else "now"],
                          input=json.dumps(encoded), text=True, capture_output=True, timeout=10)


def test_js_json_round_trip_preserves_digest_numbers_and_frozen_clock():
    snapshot = fixture()
    snapshot["sections"][0]["evidence"].update(decimal=1.0, negative_zero=-0.0, exponent=1e-7,
        hostile='</script><script>alert("fixture")</script>', nested={"snapshot_id": None})
    snapshot = generator.seal(snapshot)
    for future in (False, True):
        result = execute(document._encoded(snapshot), future=future)
        assert result.returncode == 0, result.stderr
        restored = json.loads(result.stdout)
        assert generator.validate(restored) == snapshot
        assert '"negative_zero":-0.0' in result.stdout
        assert restored["as_of_utc"] == snapshot["as_of_utc"]
        assert restored["sections"][0]["display_state"] == "INSUFFICIENT"


def test_js_rejects_changed_payload_and_unknown_schema():
    snapshot = fixture()
    encoded = document._encoded(snapshot)
    encoded["content"] = base64.b64encode(b"{}").decode()
    result = execute(encoded)
    assert result.returncode == 2 and "Digest" in result.stderr
    snapshot["schema_version"] = 99
    snapshot = generator.seal(snapshot)
    result = execute(document._encoded(snapshot))
    assert result.returncode == 2 and "Schema" in result.stderr
