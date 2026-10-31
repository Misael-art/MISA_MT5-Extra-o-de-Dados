"""Um HTML congelado para HTTP/file, sem rede nem sobrescrita de arquivos."""
import base64
from datetime import datetime, timezone
import hashlib
import json
from html.parser import HTMLParser
from pathlib import Path
import re

import pytest

from mt5_extracao.dashboard import document, generator
from mt5_extracao.dashboard.server import DashboardServer


def fixture():
    snapshot = generator.generate(None, symbol='QA </script><img src=x onerror=alert(1)>', timeframe="M5",
        as_of=datetime.now(timezone.utc).isoformat(), fixture_label="Demonstração sintética · sem evidência de mercado")
    snapshot["errors"] = []
    section = snapshot["sections"][0]
    section.update(title="Surpresa de demonstração", display_state="INSUFFICIENT", n_observations=3, unit="desvio QA")
    section["visual"] = {"value": -2.5, "direction": "DOWN", "baseline": 0,
        "series": [{"time": f"2026-10-{index:02d}T00:00:00Z", "value": None if index == 15 else index/10-2}
                   for index in range(1, 31)]}
    section["details"]["reason"] = "Fixture identificada; amostra pequena; não é análise real."
    return generator.seal(snapshot)


def decode(html):
    match = re.search(r'<script id="dashboard-data" type="application/json">(.*?)</script>', html, re.S)
    encoded = json.loads(match.group(1))
    raw = base64.b64decode(encoded["content"])
    assert hashlib.sha256(raw).hexdigest() == encoded["integrity"]["digest"]
    payload = json.loads(raw)
    payload.update(snapshot_id=encoded["integrity"]["digest"], integrity=encoded["integrity"])
    generator.validate(payload)
    return payload


class Resources(HTMLParser):
    def __init__(self):
        super().__init__()
        self.external = []
        self.inline_scripts = []
        self.inside = False
    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if (tag == "script" and "src" in attrs) or tag in {"link", "iframe", "img"}:
            self.external.append((tag, attrs))
        if tag == "script" and attrs.get("type") != "application/json":
            self.inside = True
    def handle_data(self, data):
        if self.inside:
            self.inline_scripts.append(data)
    def handle_endtag(self, tag):
        if tag == "script":
            self.inside = False


def test_archive_has_same_payload_as_server_no_external_assets_and_csp(tmp_path):
    snapshot = fixture()
    server = DashboardServer(port=5055, snapshot=snapshot)
    with server.app.test_client() as client:
        response = client.get(server.url)
    path = tmp_path / "Fotografia com espaço e acento.html"
    receipt = document.export(snapshot, path)
    raw = path.read_bytes()
    assert response.data == raw
    assert decode(raw.decode()) == snapshot
    assert receipt["network_required"] is False
    assert receipt["sha256"] == hashlib.sha256(raw).hexdigest()
    parser = Resources()
    parser.feed(raw.decode())
    assert not parser.external
    code = "".join(parser.inline_scripts)
    digest = base64.b64encode(hashlib.sha256(code.encode()).digest()).decode()
    assert "'sha256-" + digest + "'" in response.headers["Content-Security-Policy"]
    assert "connect-src 'none'" in response.headers["Content-Security-Policy"]
    assert b"&lt;/script&gt;" in raw and b"<img src=x" not in raw
    assert b"Apache License" in raw and b"Apache ECharts" in raw


def test_archive_idempotent_and_preserves_existing_target_and_symlink(tmp_path):
    snapshot = fixture()
    target = tmp_path / "new.html"
    document.export(snapshot, target)
    before = target.stat().st_mtime_ns
    document.export(snapshot, target)
    assert target.stat().st_mtime_ns == before
    protected = tmp_path / "owner.html"
    protected.write_text("preservar")
    with pytest.raises(ValueError, match="existente"):
        document.export(snapshot, protected)
    assert protected.read_text() == "preservar"
    link = tmp_path / "link.html"
    try:
        link.symlink_to(protected)
    except OSError:
        return  # Windows sem privilégio de symlink: demais asserts já executados.
    with pytest.raises(ValueError, match="simbólico"):
        document.export(snapshot, link)


def test_later_renderer_change_refuses_mixed_generation(monkeypatch):
    snapshot = fixture()
    resources = document._load_resources()
    resources["css/dashboard.css"] += b"/*another generation*/"
    monkeypatch.setattr(document, "_load_resources", lambda: resources)
    with pytest.raises(ValueError, match="RENDERER_CHANGED"):
        document.render(snapshot)


def test_document_budget_and_payload_tampering_refuse(tmp_path, monkeypatch):
    snapshot = fixture()
    snapshot["sections"][0]["visual"]["value"] = 9
    with pytest.raises(ValueError, match="Digest"):
        document.export(snapshot, tmp_path / "tampered.html")
    assert not (tmp_path / "tampered.html").exists()
    snapshot = fixture()
    monkeypatch.setattr(document, "MAX_DOCUMENT_BYTES", 10)
    with pytest.raises(ValueError, match="DOCUMENT_TOO_LARGE"):
        document.export(snapshot, tmp_path / "large.html")
    assert not (tmp_path / "large.html").exists()


def test_export_remains_independent_of_database_after_generation(tmp_path, monkeypatch):
    snapshot = fixture()
    monkeypatch.setattr(generator, "generate", lambda *a, **kw: pytest.fail("não consultar dados"))
    target = tmp_path / "frozen.html"
    document.export(snapshot, target)
    archived = decode(target.read_text())
    assert archived["as_of_utc"] == snapshot["as_of_utc"]
    assert archived["sections"][0]["n_observations"] == 3
    assert archived["sections"][0]["visual"]["value"] == -2.5
