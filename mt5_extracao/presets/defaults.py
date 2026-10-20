"""T19.55: Default imutável dos parâmetros efetivos do perfil/versão/contexto."""
import hashlib
import uuid

from sqlalchemy import text

from ..composition.native import native_adapter,default_composition
from ..composition.schema import canonical_json
from .compiler import PresetIdentity,compile_package
from . import store


def ensure_default(db,robot,*,symbol,timeframe):
    store._require_db(db)
    composition=default_composition(robot,symbol=symbol,timeframe=timeframe)
    native=native_adapter(composition,robot).describe("default_scope")
    scope={"robot":native["robot"],"profile_id":native["profile_id"],"profile_version":native["profile_version"],
           "context":composition.to_dict()["context"],"default_config_hash":composition.config_hash}
    key=hashlib.sha256(canonical_json(scope).encode("utf-8")).hexdigest()
    identity=PresetIdentity(uuid.uuid5(uuid.NAMESPACE_URL,"mt5x:default:"+key).hex,
                            category="Default",label="Default · Padrão do código")
    package=compile_package(composition,robot,identity)
    with store._LOCK,db.engine.begin() as conn:
        store._schema(conn)
        store._insert(conn,store._row(package,key),ignore_default_race=True)
        row=conn.execute(text("SELECT * FROM _preset_revisions WHERE scope_key=:key"),{"key":key}).mappings().one()
        result=store._from_row(row)
        # Identidade/conteúdo de uma revisão existente não podem ser silenciosamente reescritos.
        if result.manifest!=package.manifest or result.set_bytes!=package.set_bytes:
            raise ValueError("Default existente diverge do perfil efetivo; preserve a revisão e revise versão/escopo antes de criar outra.")
    return result
