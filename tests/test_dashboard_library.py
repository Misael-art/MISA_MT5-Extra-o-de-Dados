import configparser
from dataclasses import asdict
import hashlib
import json
from html import unescape
import re

import pytest

from mt5_extracao.analytics.indicator_registry import definitions
from mt5_extracao.composition.registry import FILTER_PARAMETERS, MODULE_PARAMETERS
from mt5_extracao.dashboard import document, generator
from mt5_extracao.dashboard.server import DashboardServer
from mt5_extracao.strategies import REGISTRY


def test_library_coverage_tracks_canonical_contracts():
    """Adicionar/remover contratos exige revisar o material no mesmo incremento."""
    data = json.loads((document.ROOT / "library_content.json").read_text(encoding="utf-8"))
    described = [feature for group in data["indicators"] for feature in group["definitions"]]
    assert sorted(described) == sorted(spec["feature_id"] for spec in definitions())
    assert {key for key in data["strategies"] if key.startswith("python:")} == {f"python:{key}" for key in REGISTRY}
    assert {slot: set(guide["modes"]) for slot, guide in data["operations"].items()} == {
        slot: set(modes) for slot, modes in {**MODULE_PARAMETERS, "filters": FILTER_PARAMETERS}.items()
    }
    contracts = {
        "indicators": definitions(),
        "operations": {slot: {mode: {name: asdict(spec) for name, spec in params.items()}
                              for mode, params in modes.items()}
                       for slot, modes in {**MODULE_PARAMETERS, "filters": FILTER_PARAMETERS}.items()},
        "strategies": {key: {"params": cls.default_params, "summary": cls.info.summary} for key, cls in REGISTRY.items()},
    }
    for name, contract in contracts.items():
        encoded = json.dumps(contract, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode()
        digest = hashlib.sha256(encoded).hexdigest()
        assert data["reviewed_contracts"][name] == digest, f"Revisar fichas de {name} para o contrato {digest}."
    examples = {item["id"]: item for item in data["examples"]}
    assert len(examples) == len(data["examples"])
    for group in data["indicators"]:
        assert group["example"] in examples
        assert all(group["notes"][name] for name in ("Origem", "No gráfico", "Cenário A", "Cenário B"))
    for example in examples.values():
        assert all(len(series["values"]) == len(example["labels"]) for series in example["series"])
    for study in data["studies"]:
        assert study["sources"] and len(study["notes"]) == 4
        assert study["example"] in examples
    references = {card["id"] for key in ("indicators", "reading", "laboratory") for card in data[key]}
    for guide in data["native_guides"].values():
        assert guide["reference"] in references
        assert re.fullmatch(r"[a-f0-9]{64}", guide["source_sha256"])


def test_library_navigation_assets_and_no_database(tmp_path, monkeypatch):
    from mt5_extracao import services
    config = configparser.ConfigParser()
    config.read_dict({"ROBOTS": {"library_dirs": str(tmp_path)}})
    folder = tmp_path / "Indicators" / "HADOUKEN" / "Trend"
    folder.mkdir(parents=True)
    (folder / "Example.mq5").write_text('#property indicator_plots 1\n'
        '#property description "<b>sem HTML</b>"\n#property version "1.10"\n', encoding="utf-8")
    monkeypatch.setattr(services, "create_db_manager", lambda *args, **kwargs: pytest.fail("A biblioteca não precisa de banco."))
    server = DashboardServer(config, port=5055)
    with server.app.test_client() as client:
        page = client.get(server.url)
        href = unescape(re.search(r'href="([^"]+)"[^>]*>Biblioteca</a>', page.get_data(as_text=True))[1])
        assert href.startswith("/biblioteca?k=")
        assert client.get(server.url.split("/?")[0] + "/biblioteca").status_code == 403
        library = client.get(server.url.split("/?")[0] + href)
        assert library.status_code == 200
        body = library.get_data(as_text=True)
        assert "Do gráfico à estratégia." in body
        assert "Indicadores nativos HADOUKEN" in body
        assert "Example</strong> · Trend · v1.10" in body
        assert "&lt;b&gt;sem HTML&lt;/b&gt;" in body
        assert str(tmp_path) not in body
        assert all(f'id="{anchor}"' in body for anchor in ("sniper-trend", "operacional", "carteira", "leitura"))
        assert library.headers["Referrer-Policy"] == "no-referrer"
        for path in re.findall(r'<script src="([^"]+)"', body):
            assert client.get(server.url.split("/?")[0] + unescape(path)).status_code == 200


def test_live_snapshot_discloses_no_session_key_in_export():
    snapshot = generator.generate(None, symbol="QA", timeframe="M5", as_of="2026-11-06T12:00:00+00:00")
    server = DashboardServer(port=5055, snapshot=snapshot)
    body, _ = document.render(snapshot)
    assert 'id="library-link" href="biblioteca" hidden' in body
    with server.app.test_client() as client:
        assert client.get(server.url).get_data(as_text=True) == body
        assert client.get(server.url.replace("/?", "/biblioteca?")).status_code == 200
    assert server.url.split("k=")[1] not in body
