"""DTO imutável do registro inicial; declarações não são aprovação estatística."""
from dataclasses import asdict, dataclass
import hashlib
import json
import math
import re

import pandas as pd

from ..composition.schema import canonical_json
from ..robots.budget import Limits

ROLES = ("train", "validation", "holdout")


def encoded(value):
    return canonical_json(value)


def digest(value):
    return hashlib.sha256(encoded(value).encode()).hexdigest()


def token(value, label):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", value):
        raise ValueError(f"{label}: use de 1 a 128 letras, números, ponto, hífen ou sublinhado.")
    return value


def timestamp(value):
    if not isinstance(value, str):
        raise ValueError("evaluation_start deve ser um horário ISO explícito.")
    try:
        stamp = pd.Timestamp(value)
        if pd.isna(stamp):
            raise ValueError()
        return stamp.isoformat()
    except (ValueError, TypeError, OverflowError):
        raise ValueError("evaluation_start deve ser um horário ISO válido.") from None


def closed(value, fields, label):
    if not isinstance(value, dict) or set(value) != set(fields):
        raise ValueError(f"{label}: campos esperados: {', '.join(sorted(fields))}.")


@dataclass(frozen=True, init=False)
class ProtocolRevision:
    _payload: str

    def __init__(self, value):
        # Serializar primeiro também recusa NaN/Inf, sem compartilhar mutáveis.
        value = json.loads(encoded(value))
        closed(value, ("schema_version", "name", "revision", "criteria", "objective", "grid", "budget", "seed", "roles"), "Protocolo")
        if type(value["schema_version"]) is not int or value["schema_version"] != 1:
            raise ValueError("Versão de protocolo desconhecida; use schema_version=1.")
        token(value["name"], "Nome do protocolo")
        if type(value["revision"]) is not int or not 1 <= value["revision"] <= 2147483647:
            raise ValueError("Revisão deve ser um inteiro de 1 a 2147483647.")
        for key in ("criteria", "objective"):
            item = value[key]
            if not isinstance(item, dict) or not isinstance(item.get("id"), str) or not item["id"].strip() or type(item.get("version")) is not int or item["version"] < 1:
                raise ValueError(f"{key}: declare id e version inteiro positivo; não há critério implícito.")
        if not isinstance(value["grid"], list) or any(not isinstance(v, dict) for v in value["grid"]):
            raise ValueError("grid deve ser uma lista declarada de objetos; [] indica alternativas manuais.")
        if type(value["seed"]) is not int or value["seed"] < 0:
            raise ValueError("seed deve ser um inteiro não negativo declarado.")
        budget = value["budget"]
        defaults = asdict(Limits())
        closed(budget, ("scope", "limits"), "Orçamento")
        if budget["scope"] != "evaluation_request":
            raise ValueError("Orçamento inicial exige scope=evaluation_request; teto de campanha não está implementado.")
        closed(budget["limits"], defaults, "Limites do orçamento")
        for key, number in budget["limits"].items():
            try:
                valid = type(number) in (int, float) and math.isfinite(number)
            except OverflowError:
                valid = False
            if not valid:
                raise ValueError("Limites devem ser números finitos explícitos, sem booleanos.")
        if type(budget["limits"]["max_jobs"]) is not int:
            raise ValueError("max_jobs deve ser inteiro.")
        Limits(**budget["limits"])
        roles = value["roles"]
        if not isinstance(roles, dict) or not roles or set(roles)-set(ROLES):
            raise ValueError("roles deve declarar train, validation e/ou holdout, sem papéis desconhecidos.")
        for role in roles.values():
            closed(role, ("data_manifest_id", "evaluation_start"), "Papel dos dados")
            if not isinstance(role["data_manifest_id"], str) or not re.fullmatch(r"[0-9a-f]{64}", role["data_manifest_id"]):
                raise ValueError("Papel exige data_manifest_id SHA-256 de snapshot existente.")
            role["evaluation_start"] = timestamp(role["evaluation_start"])
        object.__setattr__(self, "_payload", encoded(value))

    @property
    def protocol_id(self):
        return hashlib.sha256(self._payload.encode()).hexdigest()

    def to_dict(self):
        return json.loads(self._payload)
