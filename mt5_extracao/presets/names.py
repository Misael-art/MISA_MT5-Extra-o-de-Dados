"""Nomes portáteis v1. Nome/ID/hash/evidência continuam conceitos distintos."""
from dataclasses import dataclass
import re
import unicodedata

from ..composition.registry import TIMEFRAMES
from .compiler import PresetIdentity

NAME_FORMAT_VERSION = 1
HASH_LENGTH = 12
MAX_STEM_LENGTH = 180
CATEGORIES = ("Default", "Manual", "Defensivo", "FatorLucro", "Ultra")
RESERVED = {"con","prn","aux","nul",*(f"com{i}" for i in range(1,10)),*(f"lpt{i}" for i in range(1,10))}


def category_label(category):
    if category in ("Maximalista", "Fator de lucro"):
        return "FatorLucro"
    if category not in CATEGORIES:
        raise ValueError("Categoria de preset desconhecida; preserve o legado sem renomear automaticamente.")
    return category


def portable_slug(label):
    if not isinstance(label,str) or not label.strip() or any(ord(c)<32 for c in label):
        raise ValueError("Informe nome não vazio e sem caracteres de controle.")
    ascii_text=unicodedata.normalize("NFKD",label).encode("ascii","ignore").decode()
    slug="-".join(re.findall(r"[A-Za-z0-9]+",ascii_text))
    if not slug or slug.casefold() in RESERVED:
        raise ValueError("Nome não produz identificador portátil válido ou é reservado no Windows; escolha outro nome.")
    return slug


@dataclass(frozen=True)
class PresetName:
    filename: str
    label: str
    manual_slug: str | None

    @property
    def collision_key(self):
        return self.filename.casefold()


def make_name(robot, symbol, timeframe, identity, config_hash, *, orientation="normal"):
    if not isinstance(identity,PresetIdentity) or identity.category not in ("Default","Manual"):
        raise ValueError("Nome exportável exige Default ou Manual nomeado; rascunho não gera .set executável.")
    if timeframe not in TIMEFRAMES:
        raise ValueError("Timeframe desconhecido; mantenha o contexto explícito do preset.")
    if orientation not in ("normal","inverted"):
        raise ValueError("Orientação desconhecida; normal e inversa são rótulos, não concessão de suporte nativo.")
    if not isinstance(config_hash,str) or not re.fullmatch(r"[0-9a-f]{64}",config_hash):
        raise ValueError("Informe o SHA-256 canônico completo da composição; não inventar hash pelo nome.")
    parts=[portable_slug(robot),portable_slug(symbol),timeframe,identity.category]
    manual_slug=portable_slug(identity.label) if identity.category=="Manual" else None
    if manual_slug is not None:
        parts.append(manual_slug)
    parts += ["Normal" if orientation=="normal" else "Inverso", f"v{identity.revision:03d}", config_hash[:HASH_LENGTH]]
    stem="__".join(parts)
    if len(stem)>MAX_STEM_LENGTH:
        raise ValueError("Nome canônico excede 180 caracteres; reduza o nome Manual ou use identificação curta do robô/ativo, sem truncar identidade.")
    label=identity.label if identity.category=="Manual" else "Default · Padrão do código"
    return PresetName(stem+".set",label,manual_slug)


def inspect_name(filename):
    """Reconhece convenção/alias; arquivo legado nunca é renomeado nem reescrito."""
    legacy={"format_version":None,"legacy":True,"filename":filename,"rename_required":False}
    if not isinstance(filename,str) or not filename.endswith(".set"):
        return legacy
    parts=filename[:-4].split("__")
    if len(parts) not in (7,8):
        return legacy
    robot,symbol,timeframe,category=parts[:4]
    try:
        canonical_category=category_label(category)
    except ValueError:
        return legacy
    manual=canonical_category=="Manual"
    if len(parts)!=(8 if manual else 7) or timeframe not in TIMEFRAMES:
        return legacy
    slug=parts[4] if manual else None
    orientation,revision,short_hash=parts[-3:]
    tokens=[robot,symbol]+([slug] if manual else [])
    if (any(not re.fullmatch(r"[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*",value) for value in tokens)
            or orientation not in ("Normal","Inverso") or not re.fullmatch(r"v[0-9]{3,}",revision)
            or int(revision[1:])<1 or not re.fullmatch(r"[0-9a-f]{8,64}",short_hash)):
        return legacy
    return {"format_version":NAME_FORMAT_VERSION,"legacy":False,"filename":filename,"rename_required":False,
            "robot":robot,"symbol":symbol,"timeframe":timeframe,"category":canonical_category,
            "category_original":category,"manual_slug":slug,"orientation":orientation,
            "revision":int(revision[1:]),"short_config_hash":short_hash}
