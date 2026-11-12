"""Um documento para HTTP e arquivo: recursos locais imutáveis, sem acesso a dados."""
import base64
import hashlib
import json
from pathlib import Path

from jinja2 import DictLoader, Environment, select_autoescape
from markupsafe import Markup

from . import generator

ROOT = Path(__file__).parent
RENDERER_VERSION = "DASH-03/2"
RESOURCES = ("css/dashboard.css", "vendor/echarts-6.0.0.min.js", "vendor/LICENSE-echarts",
             "vendor/NOTICE-echarts", "vendor/manifest.json", "js/transport.js", "js/charts/surprise.js",
             "js/charts/regime.js", "js/charts/volume.js", "js/charts/hypothesis.js", "js/charts/validation.js", "js/dashboard.js")
MAX_DOCUMENT_BYTES = 16 * 1024 * 1024
TEMPLATES = ("index.html", "sections/surpresa.html", "sections/regime.html", "sections/volume.html")


def _load_resources():
    result = {name: (ROOT / "static" / name).read_bytes() for name in RESOURCES}
    result.update({"template:" + name: (ROOT / "templates" / name).read_bytes() for name in TEMPLATES})
    return result


def _identities(resources):
    result = {name: {"sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}
              for name, raw in resources.items()}
    vendor = json.loads(resources["vendor/manifest.json"])
    if vendor["version"] != "6.0.0" or any(result["vendor/" + name] != info for name, info in vendor["files"].items()):
        raise ValueError("VENDOR_CHANGED: biblioteca/licença diverge do manifesto local.")
    return result


def resource_identity():
    return _identities(_load_resources())


def _encoded(snapshot):
    body = dict(snapshot)
    body.pop("integrity")
    body["snapshot_id"] = None
    canonical = generator._json(body).encode()
    return {"encoding": "base64-utf8-canonical-json", "content": base64.b64encode(canonical).decode(),
            "integrity": snapshot["integrity"]}


def render(snapshot):
    """Compila a fotografia validada; nenhuma consulta, cache, CDN ou arquivo de dados."""
    generator.validate(snapshot)
    raw_resources = _load_resources()
    expected = snapshot["provenance"].get("resources")
    if expected is not None and expected != _identities(raw_resources):
        raise ValueError("RENDERER_CHANGED: recursos mudaram desde a geração; gere outra fotografia.")
    resources = {name: raw.decode("utf-8") for name, raw in raw_resources.items()}
    code = "\n;\n".join(resources[name] for name in RESOURCES if name.endswith(".js"))
    code = code.replace("</script", "<\\/script")
    digest = base64.b64encode(hashlib.sha256(code.encode()).digest()).decode()
    csp = ("default-src 'none'; script-src 'sha256-" + digest + "'; style-src 'unsafe-inline'; "
           "img-src data:; object-src 'none'; base-uri 'none'; form-action 'none'; connect-src 'none'")
    environment = Environment(loader=DictLoader({name: resources["template:" + name] for name in TEMPLATES}),
                              autoescape=select_autoescape(["html"]))
    html = environment.get_template("index.html").render(snapshot=snapshot,
        css_embed=Markup(resources["css/dashboard.css"]), js_embed=Markup(code),
        encoded=Markup(generator._json(_encoded(snapshot))), csp=csp,
        license_text=resources["vendor/LICENSE-echarts"], notice_text=resources["vendor/NOTICE-echarts"])
    if len(html.encode()) > MAX_DOCUMENT_BYTES:
        raise ValueError("DOCUMENT_TOO_LARGE: HTML excede16MiB; reduza o recorte explicitamente.")
    return html, csp


def export(snapshot, destination):
    from mt5_extracao.presets.manual import _publish
    target = Path(destination).expanduser()
    content, _ = render(snapshot)
    # Reutiliza a publicação atômica sem sobrescrita do App. Mesmo destino é idempotente.
    _publish(target, content.encode())
    return {"snapshot_id": snapshot["snapshot_id"], "path": str(target), "bytes": len(content.encode()),
            "sha256": hashlib.sha256(content.encode()).hexdigest(), "network_required": False}


def library_context(config=None):
    """Projeta os contratos distribuídos sem consultar base, terminal ou execução."""
    from mt5_extracao.analytics import indicator_registry
    from mt5_extracao.composition.registry import FILTER_PARAMETERS, MODULE_PARAMETERS
    from mt5_extracao.robots import catalog, mql

    content = json.loads((ROOT / "library_content.json").read_text(encoding="utf-8"))
    definitions = {spec["feature_id"]: spec for spec in indicator_registry.definitions()}
    groups = []
    for guide in content["indicators"]:
        specs = [definitions.pop(ident) for ident in guide["definitions"] if ident in definitions]
        groups.append({**guide, "specs": specs})
    for spec in definitions.values():
        groups.append({"id": spec["feature_id"], "title": spec["name"], "family": spec["family"], "specs": [spec],
                       "summary": "Contrato disponível. Origem histórica e exemplos didáticos aguardam revisão."})

    robots = [
        {"name": robot.name, "key": robot.key, "kind": robot.kind, "description": robot.description,
         "timeframes": robot.timeframes, "regime": robot.regime,
         "guide": content["strategies"].get(robot.key) or
         content["strategies"].get(f"python:{robot.manifest.get('parity_with', '')}")}
        for robot in sorted(catalog.load_catalog(config), key=lambda item: (item.kind, item.name.casefold()))
    ]
    native = {}
    for folder in catalog.library_dirs(config):
        root = next((path for path in (folder / "Indicators/HADOUKEN", folder / "MQL5/Indicators/HADOUKEN")
                     if path.is_dir()), None)
        if root is None:
            continue
        for path in sorted(root.rglob("*.mq5")):
            try:
                text = mql.read_text(path)
                source = mql.parse_source(text)
            except (OSError, ValueError):
                continue
            if source.indicator:
                guide = content["native_guides"].get(path.stem, {})
                reviewed = guide.get("source_sha256") == hashlib.sha256(text.encode()).hexdigest()
                native.setdefault(path.stem, {"name": path.stem, "family": path.parent.name,
                                              "version": source.version, "description": " ".join(source.descriptions),
                                              "guide": guide if reviewed else None})

    operation = []
    for slot, modes in {**MODULE_PARAMETERS, "filters": FILTER_PARAMETERS}.items():
        guide = content["operations"].get(slot, {})
        operation.append({"id": slot, "title": guide.get("title", slot), "summary": guide.get("summary", ""),
                          "modes": [{"id": mode, "description": guide.get("modes", {}).get(mode),
                                     "params": [(key, spec.unit) for key, spec in params.items()]}
                                    for mode, params in modes.items()]})
    return {**content, "indicators": groups, "operations": operation, "robots": robots,
            "native_indicators": list(native.values()),
            "definition_count": sum(len(group["specs"]) for group in groups)}
