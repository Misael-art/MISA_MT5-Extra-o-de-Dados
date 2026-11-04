"""Protocolo JSON local; sem eval, shell, coerção de booleanos ou números especiais."""
import hashlib
import json
import math


def loads(text):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("Chave JSON duplicada: " + key)
            result[key] = value
        return result

    def invalid(value):
        raise ValueError("Número JSON inválido: " + value)

    return json.loads(text, object_pairs_hook=pairs, parse_constant=invalid)


def fields(value, required=(), optional=()):
    if not isinstance(value, dict):
        raise ValueError("Informe um objeto JSON.")
    missing, extra = set(required) - value.keys(), value.keys() - set(required) - set(optional)
    if missing or extra:
        raise ValueError("Campos ausentes: " + ", ".join(sorted(missing)) +
                         "; desconhecidos: " + ", ".join(sorted(extra)))
    return value


def text(value, name="texto", maximum=256):
    if not isinstance(value, str) or not value.strip() or len(value) > maximum or any(c in value for c in "\t\r\n\x00"):
        raise ValueError("Valor inválido para " + name)
    return value


def number(value, name, minimum, maximum, *, integer=False):
    types = (int,) if integer else (int, float)
    if type(value) not in types or not math.isfinite(value) or not minimum <= value <= maximum:
        raise ValueError("Valor fora do contrato: " + name)
    return value


def boolean(value, name):
    if type(value) is not bool:
        raise ValueError(name + " exige true ou false, sem coerção.")
    return value


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False,
                                     separators=(",", ":")).encode()).hexdigest()


def resolve(value, results):
    """Referências estruturadas, ex.: {\"$ref\":\"abrir.session\"}; nunca texto executável."""
    if isinstance(value, dict):
        if "$ref" in value:
            fields(value, ("$ref",))
            parts = text(value["$ref"], "$ref").split(".")
            current = results
            try:
                for part in parts:
                    current = current[part]
            except (KeyError, TypeError):
                raise ValueError("Referência indisponível: " + value["$ref"]) from None
            return current
        return {key: resolve(item, results) for key, item in value.items()}
    if isinstance(value, list):
        return [resolve(item, results) for item in value]
    return value
