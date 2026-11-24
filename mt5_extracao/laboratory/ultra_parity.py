"""Conferência do livro normalizado do Tester. A procedência do journal é auditada separadamente."""
from hashlib import sha256

from ..experiments.schema import closed, digest
from ..presets.compiler import verify_ultra_research
from .ultra import inspect_receipt


def _difference(expected, observed, path):
    if type(expected) is not type(observed):
        return {"field": path, "expected": expected, "observed": observed}
    if isinstance(expected, dict):
        for key in dict.fromkeys([*expected, *observed]):
            if key not in observed or key not in expected:
                return {"field": f"{path}.{key}", "missing_from": "observed" if key in expected else "expected"}
            diff = _difference(expected[key], observed[key], f"{path}.{key}")
            if diff:
                return diff
    elif isinstance(expected, list):
        for i, (a, b) in enumerate(zip(expected, observed)):
            diff = _difference(a, b, f"{path}[{i}]")
            if diff:
                return diff
        if len(expected) != len(observed):
            return {"field": path + ".length", "expected": len(expected), "observed": len(observed)}
    elif expected != observed:
        return {"field": path, "expected": expected, "observed": observed}
    return None


def compare_trace(db, package, trace=None, *, journal=None):
    checked = verify_ultra_research(db, package)
    result = {"package_hash": checked["package_hash"], "receipt": checked["receipt"],
              "native_acceptance": "pending_independent_tester_evidence",
              "units": {"price": "integer_tick", "quantity": "integer_volume_step", "money": "currency_cent"},
              "tolerance_units": 0, "first_divergence": None, "frames_compared": 0}
    if trace is None:
        return dict(result, status="missing_tester_trace")
    closed(trace, ("kind", "package_hash", "manifest_id", "tester_build", "journal_sha256", "timeline", "account"),
           "Livro normalizado do Tester")
    if (trace["kind"] != "mt5_strategy_tester_ultra_v1" or trace["package_hash"] != checked["package_hash"]
            or trace["manifest_id"] != package["source_manifest"] or type(trace["tester_build"]) is not int
            or trace["tester_build"] <= 0 or not isinstance(trace["timeline"], list)
            or not isinstance(trace["account"], dict)):
        raise ValueError("Identidade do Tester, pacote, fonte ou livro incompatível.")
    if not journal or sha256(journal).hexdigest() != trace["journal_sha256"]:
        raise ValueError("Journal bruto ausente ou divergente do hash declarado no livro.")
    run = inspect_receipt(db, checked["receipt"], timeline=True)
    expected, observed = run["timeline"], trace["timeline"]
    for index, (left, right) in enumerate(zip(expected, observed)):
        diff = _difference(left, right, "frame")
        if diff:
            result["first_divergence"] = dict(diff, ordinal=index, expected_key=left["mark"]["key"])
            break
        result["frames_compared"] += 1
    else:
        if len(expected) != len(observed):
            result["first_divergence"] = {"ordinal": min(len(expected), len(observed)), "field": "timeline.length",
                                          "expected": len(expected), "observed": len(observed)}
        else:
            result["first_divergence"] = _difference(run["account"], trace["account"], "account")
    return dict(result, status="diverged" if result["first_divergence"] else "normalized_trace_matches",
                journal_sha256=trace["journal_sha256"], trace_sha256=digest(trace), tester_build=trace["tester_build"])
