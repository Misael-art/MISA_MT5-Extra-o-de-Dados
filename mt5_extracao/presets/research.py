"""Artefatos de reprodução de pesquisas no pilar de presets."""

import hashlib
import json


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _seal(record):
    return {**record, "preset_id": hashlib.sha256(_canonical(record).encode("utf-8")).hexdigest()}


def build_research_preset(campaign, attempt, *, data_manifest_id, source_namespace,
                          results_sha256, implementation):
    """Congela entradas e evidência para reprodução, mantendo execução inelegível."""
    if campaign["definition"].get("type") != "pattern_campaign_v1" or attempt["protocol_id"] != campaign["protocol_id"]:
        raise ValueError("Preset de pesquisa exige tentativa da campanha congelada.")
    payload = attempt["payload"]
    if not any(item["data_manifest_id"] == data_manifest_id and
               item["source_namespace"] == source_namespace for item in campaign["definition"]["roster"]):
        raise ValueError("Fonte do preset fora do roster da campanha.")
    if not isinstance(results_sha256, str) or len(results_sha256) != 64 or any(
            character not in "0123456789abcdef" for character in results_sha256):
        raise ValueError("Resultado exige SHA-256 canônico.")
    if not isinstance(implementation, dict) or not implementation or any(
            not isinstance(key, str) or not isinstance(value, str) or len(value) != 64 for key, value in implementation.items()):
        raise ValueError("Preset exige hashes de implementação por componente.")
    record = {"schema_version": 1, "kind": "pattern_research_only",
              "protocol_id": campaign["protocol_id"], "request_id": attempt["request_id"],
              "candidate_id": payload["candidate_id"], "definition_hash": payload["definition_hash"],
              "definition": payload["definition"], "data_manifest_id": data_manifest_id,
              "source_namespace": source_namespace, "role": payload["role"],
              "validation_plan": campaign["definition"]["validation_plan"],
              "economic_plan": campaign["definition"]["economic_plan"],
              "results_sha256": results_sha256, "implementation": implementation,
              "native_set_equivalent": False,
              "activation_authorized": False}
    return _seal(record)


def build_paired_research_preset(protocol, report):
    """Congela o candidato F19 e os identificadores da comparação já realizada."""
    from mt5_extracao.composition import Composition
    from mt5_extracao.experiments import f19

    definition = protocol["definition"]
    role = report.get("role")
    if (definition["objective"]["id"] != f19.OBJECTIVE or report.get("protocol_id") != protocol["protocol_id"]
            or role not in definition["roles"] or report.get("snapshot_id") != definition["roles"][role]["data_manifest_id"]):
        raise ValueError("Preset pareado exige a comparação do mesmo protocolo e snapshot.")
    arms = {item["arm"]: Composition(item["composition"]).to_dict() for item in definition["grid"]}
    return _seal({"schema_version": 1, "kind": "f19_paired_research_only",
                  "protocol_id": protocol["protocol_id"], "role": role, "arms": arms,
                  "data_manifest_id": report["snapshot_id"], "coverage_manifest_id": report["coverage_manifest_id"],
                  "source_identity": protocol["context"]["identity"], "scope": definition["roles"][role],
                  "executor_sha256": report["executor_sha256"], "runs": report["runs"],
                  "seed": definition["seed"], "objective": definition["objective"], "criteria": definition["criteria"],
                  "results_sha256": hashlib.sha256(_canonical(report).encode("utf-8")).hexdigest(),
                  "purpose": "paired_research", "economic_approval": False, "native_set_equivalent": False,
                  "activation_authorized": False})


def validate_research_preset(value):
    if (not isinstance(value, dict) or value.get("schema_version") != 1 or
            value.get("kind") not in ("pattern_research_only", "f19_paired_research_only")):
        raise ValueError("Preset de pesquisa desconhecido.")
    payload = dict(value)
    ident = payload.pop("preset_id", None)
    if ident != hashlib.sha256(_canonical(payload).encode("utf-8")).hexdigest():
        raise ValueError("Preset de pesquisa alterado.")
    if payload.get("native_set_equivalent") is not False or payload.get("activation_authorized") is not False:
        raise ValueError("Preset de pesquisa não autoriza operação nativa.")
    if payload["kind"] == "f19_paired_research_only" and payload.get("economic_approval") is not False:
        raise ValueError("Preset pareado de pesquisa exige o estado explícito de avaliação exploratória.")
    return json.loads(_canonical(value))
