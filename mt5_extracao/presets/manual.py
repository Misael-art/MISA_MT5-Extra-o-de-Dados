"""T19.26 inicial: Manual nomeado, revisões, importação e exportação sem overwrite."""
from dataclasses import dataclass,replace
import base64
import os
from pathlib import Path
import re
import tempfile
import uuid

from sqlalchemy import inspect,text
from sqlalchemy.exc import IntegrityError

from . import store
from .compiler import PresetIdentity,TestContext,compile_package,load_package
from .names import portable_slug
from ..composition import Composition
from ..composition.schema import canonical_json,_decode

_LABELS="""CREATE TABLE IF NOT EXISTS _preset_labels (
 preset_id TEXT PRIMARY KEY,robot_key TEXT NOT NULL,symbol TEXT NOT NULL,timeframe TEXT NOT NULL,
 label TEXT NOT NULL,manual_slug TEXT NOT NULL,
 UNIQUE(robot_key,symbol,timeframe,manual_slug))"""
_IMPORTS="""CREATE TABLE IF NOT EXISTS _preset_imports (
 package_hash TEXT PRIMARY KEY,preset_id TEXT NOT NULL,revision INTEGER NOT NULL,manifest_json TEXT NOT NULL)"""


def _schema(conn):
    store._schema(conn);conn.execute(text(_LABELS));conn.execute(text(_IMPORTS))


def _reserve_label(conn,package):
    m=package.manifest;identity=m["identity"];ctx=m["composition"]["context"]
    conn.execute(text("INSERT INTO _preset_labels(preset_id,robot_key,symbol,timeframe,label,manual_slug) "
       "VALUES(:id,:robot,:symbol,:tf,:label,:slug) ON CONFLICT(preset_id) DO UPDATE SET "
       "label=excluded.label,manual_slug=excluded.manual_slug"),
       {"id":identity["id"],"robot":m["native"]["robot"]["key"],"symbol":ctx["symbol"],"tf":ctx["timeframe"],
        "label":identity["label"],"slug":portable_slug(identity["label"]).casefold()})


def _label(db,preset):
    if not inspect(db.engine).has_table("_preset_labels"):
        return preset.identity.label
    with db.engine.connect() as conn:
        return conn.execute(text("SELECT label FROM _preset_labels WHERE preset_id=:id"),
                            {"id":preset.identity.id}).scalar_one_or_none() or preset.identity.label


@dataclass(frozen=True)
class ManualPreset:
    record: store.StoredPreset
    label: str

    @property
    def identity(self):
        return replace(self.record.identity,label=self.label)

    @property
    def composition(self):
        return self.record.composition

    @property
    def manifest(self):
        """Snapshot original preservado; apresentação atual vem de identity."""
        return self.record.manifest

    @property
    def name(self):
        from .names import make_name
        m=self.record.manifest;ctx=m["composition"]["context"]
        return make_name(m["native"]["robot"]["key"].split(":",1)[-1],ctx["symbol"],ctx["timeframe"],
                         self.identity,m["config_hash"],orientation=m["composition"]["orientation"]["mode"])

    def package(self,robot,test_context=None,*,canonical_filename=False):
        original=self.record.package(robot)  # nunca atualizar a fonte silenciosamente
        context=test_context
        if context is None and original.manifest["test_context"] is not None:
            context=TestContext(**original.manifest["test_context"])
        return compile_package(self.composition,robot,self.identity,context,canonical_filename=canonical_filename)


def get_manual(db,preset_id,revision=None):
    preset=store.get_preset(db,preset_id,revision)
    if preset is None or preset.identity.category!="Manual":
        raise ValueError("Manual/revisão não encontrado; preserve o Default e escolha um Manual salvo.")
    return ManualPreset(preset,_label(db,preset))


def list_manual(db,robot_key,symbol,timeframe):
    return [ManualPreset(item,_label(db,item)) for item in store.list_presets(db,robot_key,symbol,timeframe)
            if item.identity.category=="Manual"]


def save_as_manual(db,robot,source_id,name,*,source_revision=None,composition=None):
    store._require_db(db)
    if not isinstance(name,str):
        raise ValueError("Informe nome do Manual antes de salvar.")
    source=store.get_preset(db,source_id,source_revision)
    if source is None:
        raise ValueError("Preset de origem ausente; reabra o Default ou Manual antes de clonar.")
    source.package(robot)
    comp=source.composition if composition is None else composition if isinstance(composition,Composition) else Composition(composition)
    if comp.to_dict()["context"]!=source.composition.to_dict()["context"]:
        raise ValueError("Clone Manual conserva ativo/TF da origem; crie outro Default para mudar o contexto.")
    origin=source.identity.id if source.identity.category=="Default" else source.identity.default_origin
    identity=PresetIdentity(uuid.uuid4().hex,category="Manual",label=name.strip(),default_origin=origin)
    package=compile_package(comp,robot,identity)
    try:
        with store._LOCK,db.engine.begin() as conn:
            _schema(conn);_reserve_label(conn,package);store._insert(conn,store._row(package))
    except IntegrityError:
        raise ValueError("Nome já usado neste robô/ativo/TF ou revisão concorrente; escolha outro nome ou reabra o Manual.") from None
    return get_manual(db,identity.id,1)


def save_revision(db,robot,preset_id,composition,*,expected_revision):
    if type(expected_revision) is not int or expected_revision<1:
        raise ValueError("Revisão esperada deve ser inteiro positivo; reabra o Manual antes de salvar.")
    current=get_manual(db,preset_id)
    current.record.package(robot)
    comp=composition if isinstance(composition,Composition) else Composition(composition)
    if comp.to_dict()["context"]!=current.composition.to_dict()["context"]:
        raise ValueError("A revisão conserva ativo/TF; salve outro Manual para outro contexto.")
    if comp.to_json()==current.composition.to_json():
        return current  # repetir Salvar não cria revisão nem invalida evidência
    if current.identity.revision!=expected_revision:
        raise ValueError("Manual mudou desde a edição; reabra a revisão atual antes de salvar.")
    identity=replace(current.identity,revision=current.identity.revision+1)
    package=compile_package(comp,robot,identity)
    try:
        with store._LOCK,db.engine.begin() as conn:
            _schema(conn)
            latest=conn.execute(text("SELECT MAX(revision) FROM _preset_revisions WHERE preset_id=:id"),{"id":preset_id}).scalar()
            if latest!=expected_revision:
                raise ValueError("Manual mudou desde a edição; reabra a revisão atual antes de salvar.")
            _reserve_label(conn,package);store._insert(conn,store._row(package))
    except IntegrityError:
        raise ValueError("Nome/revisão já existe; reabra o Manual antes de salvar.") from None
    return get_manual(db,preset_id,identity.revision)


def rename_manual(db,preset_id,name):
    if not isinstance(name,str):
        raise ValueError("Informe nome do Manual antes de renomear.")
    current=get_manual(db,preset_id)
    identity=replace(current.identity,label=name.strip())
    # Só apresentação: snapshot, parâmetros, revisão e provas da execução são preservados.
    m=current.record.manifest;ctx=m["composition"]["context"]
    try:
        with store._LOCK,db.engine.begin() as conn:
            _schema(conn)
            conn.execute(text("INSERT INTO _preset_labels(preset_id,robot_key,symbol,timeframe,label,manual_slug) "
                 "VALUES(:id,:robot,:symbol,:tf,:label,:slug) ON CONFLICT(preset_id) DO UPDATE SET "
                 "label=excluded.label,manual_slug=excluded.manual_slug"),
                 {"id":preset_id,"robot":m["native"]["robot"]["key"],"symbol":ctx["symbol"],"tf":ctx["timeframe"],
                  "label":identity.label,"slug":portable_slug(identity.label).casefold()})
    except IntegrityError:
        raise ValueError("Nome já usado neste robô/ativo/TF; escolha outro nome.") from None
    return get_manual(db,preset_id)


def _publish(path,data):
    if path.is_symlink():
        raise ValueError("Destino é link simbólico; escolha diretório próprio para preservar arquivos de terceiros.")
    if path.exists():
        if not path.is_file() or path.read_bytes()!=data:
            raise ValueError("Arquivo existente difere do pacote; preserve-o e escolha outro destino/revisão.")
        return
    temporary=None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent,prefix=".mt5x-",delete=False) as handle:
            temporary=Path(handle.name);handle.write(data);handle.flush();os.fsync(handle.fileno())
        try:
            os.link(temporary,path)  # publicação atômica completa, sem substituir destino
        except FileExistsError:
            if path.is_symlink() or path.read_bytes()!=data:
                raise ValueError("Destino mudou durante a exportação; preserve-o e escolha outro diretório.") from None
    except OSError as error:
        raise ValueError("Não foi possível publicar atomicamente; escolha diretório local gravável/caminho mais curto. "+str(error)) from None
    finally:
        if temporary is not None:temporary.unlink(missing_ok=True)


def export_manual(db,robot,preset_id,directory,*,revision=None,test_context=None):
    get_manual(db,preset_id,revision)
    return export_preset(db,robot,preset_id,directory,revision=revision,test_context=test_context)


def export_preset(db,robot,preset_id,directory,*,revision=None,test_context=None):
    record=store.get_preset(db,preset_id,revision)
    if record is None:
        raise ValueError("Preset/revisão não encontrado; reabra o snapshot antes de exportar.")
    if record.identity.category=="Manual":
        package=get_manual(db,preset_id,record.identity.revision).package(robot,test_context,canonical_filename=True)
    elif record.identity.category=="Default":
        original=record.package(robot)
        context=test_context
        if context is None and original.manifest["test_context"] is not None:
            context=TestContext(**original.manifest["test_context"])
        package=compile_package(record.composition,robot,record.identity,context,canonical_filename=True)
    else:
        raise ValueError("Rascunho sem Default/Manual nomeado não gera arquivo executável.")
    base=Path(directory).expanduser().resolve()
    folder=base/package.execution_hash[:16]
    if folder.is_symlink():
        raise ValueError("Diretório do pacote é link simbólico; escolha outro destino.")
    try:
        base.mkdir(parents=True,exist_ok=True);folder.mkdir(exist_ok=True)
    except OSError as error:
        raise ValueError("Escolha diretório de exportação local gravável/caminho mais curto. "+str(error)) from None
    set_path=folder/package.manifest["parameters_file"]
    manifest_path=set_path.with_suffix(".manifest.json")
    _publish(set_path,package.set_bytes)
    _publish(manifest_path,package.to_json().encode("utf-8"))  # manifesto é marcador de pacote completo
    return {"set_path":set_path,"manifest_path":manifest_path,"package":package}


def _definition(package):
    m=package.manifest;identity=dict(m["identity"]);identity.pop("label")
    return canonical_json({"identity":identity,"composition":m["composition"],"native":m["native"],
                           "parameters_sha256":m["parameters_sha256"]})


def import_manual(db,robot,manifest_path):
    store._require_db(db)
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
    package=load_package(manifest,set_path.read_bytes(),robot)
    identity=PresetIdentity(**package.manifest["identity"])
    if identity.category!="Manual":
        raise ValueError("Importação inicial trata Manual nomeado; o Default efetivo é criado pelo serviço local, sem substituir referência.")
    existing=store.get_preset(db,identity.id,identity.revision)
    if existing is not None and _definition(existing.package(robot))!=_definition(package):
        raise ValueError("Identidade/revisão já existe com outros parâmetros; preserve-a e salve outra revisão.")
    try:
        with store._LOCK,db.engine.begin() as conn:
            _schema(conn)
            row=conn.execute(text("SELECT manifest_json,set_base64 FROM _preset_revisions WHERE preset_id=:id AND revision=:rev"),
                             {"id":identity.id,"rev":identity.revision}).mappings().one_or_none()
            if row is None:
                _reserve_label(conn,package);store._insert(conn,store._row(package))
            elif _definition(load_package(row["manifest_json"],base64.b64decode(row["set_base64"]),robot))!=_definition(package):
                raise ValueError("Identidade/revisão já existe com outros parâmetros; não sobrescrever.")
            conn.execute(text("INSERT INTO _preset_imports(package_hash,preset_id,revision,manifest_json) VALUES(:hash,:id,:rev,:manifest) "
                              "ON CONFLICT(package_hash) DO NOTHING"),
                         {"hash":package.package_hash,"id":identity.id,"rev":identity.revision,"manifest":package.to_json()})
    except IntegrityError:
        raise ValueError("Nome/revisão já usado neste contexto; preserve os dois pacotes e escolha outra identidade.") from None
    return get_manual(db,identity.id,identity.revision)
