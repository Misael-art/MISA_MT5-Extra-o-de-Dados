"""Snapshots aditivos de presets; revisão salva não é regravada nem aprovada."""
import base64
from dataclasses import dataclass
from datetime import datetime,timezone
import json
import threading

from sqlalchemy import inspect,text

from .compiler import PresetPackage,load_package
from .names import make_name

_LOCK=threading.RLock()
_CREATE="""CREATE TABLE IF NOT EXISTS _preset_revisions (
 preset_id TEXT NOT NULL, revision INTEGER NOT NULL, robot_key TEXT NOT NULL,
 symbol TEXT NOT NULL, timeframe TEXT NOT NULL, category TEXT NOT NULL,
 display_name TEXT NOT NULL, manual_slug TEXT,
 source_fingerprint TEXT NOT NULL, config_hash TEXT NOT NULL, execution_hash TEXT NOT NULL,
 scope_key TEXT UNIQUE, manifest_json TEXT NOT NULL, set_base64 TEXT NOT NULL, created_at TEXT NOT NULL,
 PRIMARY KEY(preset_id,revision))"""
_SELECTION="""CREATE TABLE IF NOT EXISTS _preset_selections (
 robot_key TEXT NOT NULL,symbol TEXT NOT NULL,timeframe TEXT NOT NULL,
 preset_id TEXT NOT NULL,revision INTEGER NOT NULL,
 PRIMARY KEY(robot_key,symbol,timeframe))"""


def _require_db(db):
    if db is None or getattr(db,"engine",None) is None:
        raise ValueError("Selecione um banco disponível para preservar presets e suas revisões.")


def _schema(conn):
    conn.execute(text(_CREATE));conn.execute(text(_SELECTION))


@dataclass(frozen=True)
class StoredPreset:
    _manifest_json: str
    set_bytes: bytes
    created_at: str

    @property
    def manifest(self):
        return json.loads(self._manifest_json)

    @property
    def identity(self):
        from .compiler import PresetIdentity
        return PresetIdentity(**self.manifest["identity"])

    @property
    def composition(self):
        from ..composition import Composition
        return Composition(self.manifest["composition"])

    @property
    def name(self):
        value=self.manifest
        return make_name(value["native"]["robot"]["key"].split(":",1)[-1],
                         value["composition"]["context"]["symbol"],value["composition"]["context"]["timeframe"],
                         self.identity,value["config_hash"],orientation=value["composition"]["orientation"]["mode"])

    def package(self,robot):
        """Reabre para execução só com o contrato/fonte compatíveis; leitura acima preserva o snapshot antigo."""
        return load_package(self._manifest_json,self.set_bytes,robot)


def _from_row(row):
    return StoredPreset(row["manifest_json"],base64.b64decode(row["set_base64"],validate=True),row["created_at"])


def _row(package,scope_key=None):
    value=package.manifest;identity=value["identity"];context=value["composition"]["context"]
    from .compiler import PresetIdentity
    name=make_name(value["native"]["robot"]["key"].split(":",1)[-1],context["symbol"],context["timeframe"],
                   PresetIdentity(**identity),value["config_hash"],orientation=value["composition"]["orientation"]["mode"])
    return dict(preset_id=identity["id"],revision=identity["revision"],robot_key=value["native"]["robot"]["key"],
                symbol=context["symbol"],timeframe=context["timeframe"],category=identity["category"],
                display_name=identity["label"],manual_slug=name.manual_slug.casefold() if name.manual_slug else None,
                source_fingerprint=value["native"]["robot"]["fingerprint"],config_hash=value["config_hash"],
                execution_hash=package.execution_hash,scope_key=scope_key,manifest_json=package.to_json(),
                set_base64=base64.b64encode(package.set_bytes).decode("ascii"),created_at=datetime.now(timezone.utc).isoformat())


def _insert(conn,row,*,ignore_default_race=False):
    fields=tuple(row)
    sql="INSERT INTO _preset_revisions ("+",".join(fields)+") VALUES ("+",".join(":"+key for key in fields)+")"
    if ignore_default_race:
        sql+=" ON CONFLICT(scope_key) DO NOTHING"
    conn.execute(text(sql),row)


def get_preset(db,preset_id,revision=None):
    _require_db(db)
    if not inspect(db.engine).has_table("_preset_revisions"):
        return None
    params={"id":preset_id};sql="SELECT * FROM _preset_revisions WHERE preset_id=:id"
    if revision is not None:
        sql+=" AND revision=:revision";params["revision"]=revision
    with db.engine.connect() as conn:
        row=conn.execute(text(sql+" ORDER BY revision DESC LIMIT 1"),params).mappings().one_or_none()
    return _from_row(row) if row is not None else None


def list_presets(db,robot_key,symbol,timeframe):
    _require_db(db)
    if not inspect(db.engine).has_table("_preset_revisions"):
        return []
    with db.engine.connect() as conn:
        rows=conn.execute(text("SELECT p.* FROM _preset_revisions p WHERE p.robot_key=:robot AND p.symbol=:symbol "
             "AND p.timeframe=:tf AND NOT EXISTS (SELECT 1 FROM _preset_revisions n "
             "WHERE n.preset_id=p.preset_id AND n.revision>p.revision) ORDER BY p.category,p.display_name,p.preset_id"),
             {"robot":robot_key,"symbol":symbol,"tf":timeframe}).mappings()
        return [_from_row(row) for row in rows]


def select_preset(db,robot_key,symbol,timeframe,preset_id,revision):
    _require_db(db)
    with _LOCK,db.engine.begin() as conn:
        _schema(conn)
        row=conn.execute(text("SELECT * FROM _preset_revisions WHERE preset_id=:id AND revision=:revision"),
                         {"id":preset_id,"revision":revision}).mappings().one_or_none()
        if row is None or (row["robot_key"],row["symbol"],row["timeframe"])!=(robot_key,symbol,timeframe):
            raise ValueError("Revisão ausente ou de outro contexto; escolha preset do mesmo robô, ativo e timeframe.")
        conn.execute(text("INSERT INTO _preset_selections(robot_key,symbol,timeframe,preset_id,revision) "
                          "VALUES(:robot,:symbol,:tf,:id,:revision) ON CONFLICT(robot_key,symbol,timeframe) "
                          "DO UPDATE SET preset_id=excluded.preset_id,revision=excluded.revision"),
                     {"robot":robot_key,"symbol":symbol,"tf":timeframe,"id":preset_id,"revision":revision})


def selected_preset(db,robot_key,symbol,timeframe):
    _require_db(db)
    if not inspect(db.engine).has_table("_preset_selections"):
        return None
    with db.engine.connect() as conn:
        row=conn.execute(text("SELECT preset_id,revision FROM _preset_selections WHERE robot_key=:robot AND symbol=:symbol AND timeframe=:tf"),
                         {"robot":robot_key,"symbol":symbol,"tf":timeframe}).mappings().one_or_none()
    if row is None:
        return None
    result=get_preset(db,row["preset_id"],row["revision"])
    if result is None:
        raise ValueError("Seleção aponta revisão ausente; preserve o banco e escolha outra revisão.")
    return result
