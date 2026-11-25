"""T19.25: pacote tipado do preset S09, pelo mapper e serializador existentes."""
from dataclasses import dataclass
from datetime import date
import hashlib
import json
import math
import re
from pathlib import Path
import tempfile

from ..composition.native import native_adapter
from ..composition.schema import Composition, canonical_json, _decode, _object
from ..robots import mql, mt5_tools, native_inputs

PACKAGE_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class PresetIdentity:
    id: str
    revision: int = 1
    category: str = "draft"
    label: str = ""
    default_origin: str | None = None

    def __post_init__(self):
        if not native_inputs.scalar(self.id, "token") or len(self.id)>80:
            raise ValueError("ID do preset exige 1 a 80 caracteres públicos portáteis.")
        if type(self.revision) is not int or self.revision<1:
            raise ValueError("Revisão do preset deve ser inteiro positivo.")
        if self.category not in ("draft", "Default", "Manual"):
            raise ValueError("Pacote inicial admite rascunho, Default ou Manual; categorias automáticas ainda indisponíveis.")
        if (not isinstance(self.label,str) or len(self.label)>80 or self.label!=self.label.strip()
                or any(ord(c)<32 for c in self.label) or self.category=="Manual" and not self.label):
            raise ValueError("Nome do Manual exige 1 a 80 caracteres, sem controles/espaços nas bordas.")
        if self.default_origin is not None and (not native_inputs.scalar(self.default_origin,"token") or len(self.default_origin)>80):
            raise ValueError("Origem do Default deve ser identidade pública portátil.")

    def to_dict(self):
        return dict(id=self.id,revision=self.revision,category=self.category,label=self.label,default_origin=self.default_origin)


@dataclass(frozen=True)
class TestContext:
    start: str
    end: str
    deposit: float
    model: str = "0"

    def __post_init__(self):
        try:
            start,end=date.fromisoformat(self.start),date.fromisoformat(self.end)
            if self.start!=start.isoformat() or self.end!=end.isoformat() or end<=start:
                raise ValueError
        except (ValueError,TypeError):
            raise ValueError("Contexto exige datas ISO completas e fim posterior ao início; confira a janela do Tester.") from None
        if self.model not in mt5_tools.TESTER_MODELS:
            raise ValueError("Modelo de teste desconhecido; use um modelo suportado pelo serviço existente.")
        if type(self.deposit) not in (int,float) or not math.isfinite(self.deposit) or self.deposit<=0:
            raise ValueError("Depósito simulado deve ser positivo e finito; não altera saldo/limites de conta.")

    def to_dict(self):
        return dict(start=self.start,end=self.end,model=self.model,deposit=self.deposit)


@dataclass(frozen=True)
class PresetPackage:
    _manifest_json: str
    set_bytes: bytes

    @property
    def manifest(self):
        return json.loads(self._manifest_json)

    def to_json(self):
        return self._manifest_json

    @property
    def package_hash(self):
        # Identidade do pacote/contexto; não redefine o config_hash operacional existente.
        return hashlib.sha256(self._manifest_json.encode("utf-8")+b"\0"+self.set_bytes).hexdigest()


    @property
    def execution_hash(self):
        # Nome de apresentação não muda inputs ou evidência da mesma revisão.
        m=self.manifest
        execution={key:m[key] for key in ("config_hash","native","parameters_sha256","test_context")}
        execution["revision"]=m["identity"]["revision"]
        return hashlib.sha256(canonical_json(execution).encode("utf-8")).hexdigest()


def _serialize_set(parameters):
    with tempfile.TemporaryDirectory(prefix="mt5x-preset-") as directory:
        root=Path(directory)
        name=mt5_tools.write_tester_set(root,"parameters",parameters)
        path=root/"MQL5"/"Profiles"/"Tester"/name
        data=path.read_bytes()
        if mql.parse_set(mql.read_text(path))!=parameters:
            raise ValueError("Serialização do preset não preservou os inputs; confira o perfil e o leitor .set.")
        return data


def _require_seed_before_test(composition,test_context):
    """Seed de prefixo (relógio broker) posterior ao início do Tester reprova no OnInit.

    O Expert exige InpSeedStart <= TimeCurrent(); no Tester o relógio começa em `start` 00:00 broker.
    Recusar aqui evita compilar e baixar ticks para uma sessão que já nasce recusada.
    """
    if test_context is None:
        return
    seed=composition.to_dict()["strategy"]["params"].get("seed_start_broker")
    if not isinstance(seed,str) or seed.startswith("1970-01-01"):
        return
    start=date.fromisoformat(test_context.start).isoformat()+"T00:00"
    if seed>start:
        raise ValueError(f"Seed {seed} é posterior ao início do Tester ({start}, relógio broker); "
                         "escolha uma barra real anterior ou adie o início da janela.")


def compile_package(composition, robot, identity, test_context=None, *, canonical_filename=False):
    if not isinstance(identity,PresetIdentity):
        raise ValueError("Compile com identidade tipada do preset; não adivinhar nome/versão.")
    if test_context is not None and not isinstance(test_context,TestContext):
        raise ValueError("Use contexto tipado de teste ou mantenha o preset como rascunho sem janela.")
    composition=composition if isinstance(composition,Composition) else Composition(composition)
    native=native_adapter(composition,robot).describe(identity.id)
    _require_seed_before_test(composition,test_context)
    if type(canonical_filename) is not bool:
        raise ValueError("Convenção do filename exige escolha explícita booleana.")
    filename="parameters.set"
    if canonical_filename:
        from .names import make_name
        ctx=composition.to_dict()["context"]
        filename=make_name(robot.key.split(":",1)[-1],ctx["symbol"],ctx["timeframe"],identity,
                           composition.config_hash,orientation=composition.to_dict()["orientation"]["mode"]).filename
    data=_serialize_set(native["parameters"])
    manifest={"preset_package_schema_version":PACKAGE_SCHEMA_VERSION,"identity":identity.to_dict(),
              "composition":composition.to_dict(),"config_hash":composition.config_hash,"native":native,
              "parameters_file":filename,"parameters_encoding":"utf-16",
              "parameters_sha256":hashlib.sha256(data).hexdigest(),
              "test_context":test_context.to_dict() if test_context is not None else None}
    return PresetPackage(canonical_json(manifest),data)


def load_package(manifest, set_bytes, robot):
    """Recompila o contrato local para conferir o pacote; não aceita aprovação embutida."""
    manifest=_decode(manifest)
    _object(manifest,("preset_package_schema_version","identity","composition","config_hash","native",
                      "parameters_file","parameters_encoding","parameters_sha256","test_context"),"preset_package")
    if type(manifest["preset_package_schema_version"]) is not int or manifest["preset_package_schema_version"]!=PACKAGE_SCHEMA_VERSION:
        raise ValueError("Versão do pacote desconhecida; use leitor compatível, sem migração automática.")
    _object(manifest["identity"],("id","revision","category","label","default_origin"),"identity")
    identity=PresetIdentity(**manifest["identity"])
    context=manifest["test_context"]
    if context is not None:
        _object(context,("start","end","model","deposit"),"test_context")
        context=TestContext(**context)
    expected=compile_package(manifest["composition"],robot,identity,context,
                             canonical_filename=manifest["parameters_file"]!="parameters.set")
    if type(set_bytes) is not bytes or set_bytes!=expected.set_bytes or canonical_json(manifest)!=expected.to_json():
        raise ValueError("Pacote divergente do contrato/inputs/fonte atuais; preserve o original e compile uma nova revisão.")
    return expected


def read_package(manifest_path,robot):
    """Lê os arquivos exportados reais; não instala, importa no banco ou inicia sessão."""
    manifest_path=Path(manifest_path)
    if manifest_path.is_symlink():
        raise ValueError("Importe pacote local regular; link simbólico não concede acesso a outro arquivo.")
    manifest=_decode(manifest_path.read_text(encoding="utf-8"))
    filename=manifest.get("parameters_file") if isinstance(manifest,dict) else None
    if not isinstance(filename,str) or not re.fullmatch(r"[A-Za-z0-9_-]+\.set",filename):
        raise ValueError("Pacote exige .set local com nome portátil, sem caminhos; preserve o arquivo original.")
    set_path=manifest_path.parent/filename
    if set_path.is_symlink():
        raise ValueError("Arquivo .set é link simbólico; preserve o original e importe uma cópia regular.")
    return load_package(manifest,set_path.read_bytes(),robot)


def ultra_research_ledger(db, receipt):
    """Reabre o livro ou deriva as marcas em caixa de uma política vazia já qualificada."""
    from ..analytics import datasets
    from ..composition import ultra as portfolio
    from ..laboratory import ultra

    run = ultra.inspect_receipt(db, receipt, timeline=True)
    if run["status"] == "complete":
        return run
    policy = run["policy"]
    if run["status"] != "no_candidate" or policy["modules"] or run["reasons"]:
        raise ValueError("Pacote Ultra exige livro de replay concluído ou carteira vazia qualificada.")
    if policy["engine_hash"] != ultra.source_identity():
        raise ValueError("Motor do controle em caixa diverge da revisão congelada.")
    ticks, _ = ultra._inputs(datasets.load(db, run["manifest_id"]), policy)
    state = portfolio.initial_state(policy["cash"])
    frames = []
    for tick in ticks:
        events, mark = portfolio.advance(state, policy, tick, {})
        frames.append({"events": events, "mark": mark})
    return dict(run, account=state, timeline=frames, progress=len(frames))


def compile_ultra_research(db, receipt):
    """Pacote de reprodução ligado ao livro congelado; o executor MT5 tem gate próprio."""
    from ..analytics import datasets
    from ..experiments.schema import digest
    from ..laboratory import ultra

    run = ultra_research_ledger(db, receipt)
    snapshot = datasets.load(db, run["manifest_id"])
    manifest = snapshot.manifest
    payload = {"version": 1, "kind": "ultra_research", "receipt": receipt,
               "sample_kind": run["sample_kind"], "policy": run["policy"],
               "symbol": manifest["symbol"], "feed": manifest["source"],
               "time": manifest["time"], "specification": snapshot.spec_row,
               "instrument_context": snapshot.spec.provenance,
               "capabilities": ultra.capabilities(), "source_manifest": run["manifest_id"],
               "ledger_hash": digest(run["timeline"]), "account_hash": digest(run["account"]),
               "native_status": "capability_absent", "economic_approval": False}
    if run["status"] == "no_candidate":
        payload["selection_status"] = "no_candidate"
    return dict(payload, package_hash=digest(payload))


def verify_ultra_research(db, package):
    if not isinstance(package, dict) or package.get("kind") != "ultra_research":
        raise ValueError("Pacote de pesquisa Ultra inválido.")
    actual = compile_ultra_research(db, package.get("receipt"))
    if canonical_json(actual) != canonical_json(package):
        raise ValueError("Pacote diverge do recibo, da fonte ou do livro congelado.")
    return {"status": "verified_research", "package_hash": actual["package_hash"],
            "receipt": actual["receipt"], "native_status": actual["native_status"]}
