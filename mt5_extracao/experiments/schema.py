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


@dataclass(frozen=True, init=False)
class PatternCampaignRevision:
    """Protocolo T13.4-P03 no mesmo registro, sem alegar reserva qualificada."""

    _payload: str

    def __init__(self, value):
        from mt5_extracao import timeframes

        value = json.loads(encoded(value))
        closed(value, ("schema_version", "type", "name", "revision", "roster", "roles",
                       "endpoints", "grid", "budget", "criteria", "seed", "validation_plan",
                       "economic_plan"), "Campanha de padrões")
        if type(value["schema_version"]) is not int or value["schema_version"] != 2 or value["type"] != "pattern_campaign_v1":
            raise ValueError("Campanha exige schema_version=2 e tipo pattern_campaign_v1.")
        token(value["name"], "Nome da campanha")
        if type(value["revision"]) is not int or not 1 <= value["revision"] <= 2147483647:
            raise ValueError("Revisão da campanha inválida.")
        roster = value["roster"]
        if not isinstance(roster, list) or not roster or len(roster) > 128:
            raise ValueError("Campanha exige roster explícito de 1 a 128 fontes congeladas.")
        seen = set()
        for item in roster:
            closed(item, ("data_manifest_id", "source_namespace", "symbol", "timeframe"), "Fonte da campanha")
            if not isinstance(item["data_manifest_id"], str) or not re.fullmatch(r"[0-9a-f]{64}", item["data_manifest_id"]):
                raise ValueError("Fonte exige manifest_id SHA-256.")
            token(item["source_namespace"], "Namespace da fonte")
            if not isinstance(item["symbol"], str) or not 1 <= len(item["symbol"]) <= 128 or any(ord(ch) < 32 for ch in item["symbol"]):
                raise ValueError("Símbolo da fonte inválido.")
            tf = timeframes.parse(item["timeframe"])
            if tf is None:
                raise ValueError("Timeframe da fonte inválido.")
            item["timeframe"] = tf.name
            identity = (item["source_namespace"], item["symbol"], item["timeframe"])
            if identity in seen:
                raise ValueError("Roster contém identidade duplicada; concilie a fonte.")
            seen.add(identity)
        roles = value["roles"]
        closed(roles, ROLES, "Janelas globais")
        previous_end = None
        for role in ROLES:
            window = roles[role]
            closed(window, ("start_utc", "end_utc"), "Janela global")
            for key in ("start_utc", "end_utc"):
                try:
                    stamp = pd.Timestamp(window[key])
                except (ValueError, TypeError, OverflowError):
                    raise ValueError("Cortes globais exigem UTC explícito.") from None
                if pd.isna(stamp) or stamp.tzinfo is None:
                    raise ValueError("Cortes globais exigem UTC explícito.")
                window[key] = stamp.tz_convert("UTC").isoformat()
            if window["start_utc"] >= window["end_utc"] or (previous_end is not None and window["start_utc"] < previous_end):
                raise ValueError("Janelas train/validation/holdout precisam ser ordenadas e não sobrepostas.")
            previous_end = window["end_utc"]
        if not isinstance(value["endpoints"], list) or not value["endpoints"] or len(value["endpoints"]) > 32:
            raise ValueError("Campanha exige endpoints primários/secundários explícitos.")
        for endpoint in value["endpoints"]:
            closed(endpoint, ("id", "kind"), "Endpoint")
            token(endpoint["id"], "Endpoint")
            if endpoint["kind"] not in ("primary", "secondary"):
                raise ValueError("Endpoint exige papel primary ou secondary.")
        if sum(endpoint["kind"] == "primary" for endpoint in value["endpoints"]) != 1:
            raise ValueError("Declare exatamente um endpoint primário.")
        if not isinstance(value["grid"], list) or any(
                not isinstance(item, str) or not re.fullmatch(r"[0-9a-f]{64}", item) for item in value["grid"]):
            raise ValueError("Grade exige hashes de definição distintos.")
        if len(value["grid"]) != len(set(value["grid"])):
            raise ValueError("Grade exige hashes de definição distintos.")
        budget = value["budget"]
        closed(budget, ("scope", "max_attempts", "limits"), "Orçamento de campanha")
        if budget["scope"] != "campaign" or type(budget["max_attempts"]) is not int or not 1 <= budget["max_attempts"] <= 100_000:
            raise ValueError("Orçamento exige scope=campaign e max_attempts positivo.")
        if len(value["grid"]) > budget["max_attempts"]:
            raise ValueError("Grade excede orçamento de tentativas antes da busca.")
        closed(budget["limits"], asdict(Limits()), "Limites de recursos")
        if any(type(number) not in (int, float) or not math.isfinite(number) for number in budget["limits"].values()) or type(budget["limits"]["max_jobs"]) is not int:
            raise ValueError("Limites de recursos exigem números finitos explícitos.")
        Limits(**budget["limits"])
        criteria = value["criteria"]
        closed(criteria, ("alpha", "confidence", "minimum_effect", "maximum_drawdown"), "Critérios")
        for key, number in criteria.items():
            if type(number) not in (int, float) or not math.isfinite(number):
                raise ValueError("Critérios numéricos finitos são obrigatórios.")
        if not (0 < criteria["alpha"] < 1 and 0 < criteria["confidence"] < 1 and
                criteria["minimum_effect"] >= 0 and 0 <= criteria["maximum_drawdown"] < 1):
            raise ValueError("Critérios fora do domínio declarado.")
        if type(value["seed"]) is not int or value["seed"] < 0:
            raise ValueError("Semente não negativa obrigatória.")
        if not isinstance(value["validation_plan"], dict) or not isinstance(value["economic_plan"], dict):
            raise ValueError("Planos estatístico e econômico devem ser objetos explícitos.")
        object.__setattr__(self, "_payload", encoded(value))

    @property
    def protocol_id(self):
        return hashlib.sha256(self._payload.encode()).hexdigest()

    def to_dict(self):
        return json.loads(self._payload)
