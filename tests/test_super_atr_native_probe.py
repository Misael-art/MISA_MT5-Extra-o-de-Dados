"""Executa a sonda C++ do corpo MQL real (SA-01 e SA-REF-01) nos checks habituais.

Não é MT5/Tester: prova causalidade, revisão, deduplicação, armamento e regras de saída do fonte gerado.
"""
import contextlib
import io
import json
import shutil
from pathlib import Path

import pytest

FRAMEWORK = Path(__file__).resolve().parents[2] / "hadouken"


def test_real_mql_body_runs_both_variants():
    if not (FRAMEWORK / "Include/Framework/HADOUKEN/Indicators/HDK_SuperATR.mqh").exists():
        pytest.skip("HADOUKEN fixado ausente")
    if not (shutil.which("g++") or shutil.which("clang++")):
        pytest.skip("compilador C++ indisponível")
    from tests import super_atr_research_probe as probe
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        probe.main(str(FRAMEWORK))
    receipt = json.loads(out.getvalue())
    assert receipt["future_isolation"] and receipt["stale_missing_revision_sync_refused"] and receipt["deduplicated"]
    assert receipt["entries"] > 0 and receipt["exits"] > receipt["entries"]
    assert receipt["sa_ref_01_entries"] > 0 and receipt["sa_ref_01_opposite_flip_exits"] == 0
    assert receipt["sa_ref_01_ineligible_logged"] > 0 and receipt["mt5_qualified"] is False
