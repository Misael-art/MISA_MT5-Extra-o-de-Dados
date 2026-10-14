"""T16.11b: exportar a proposta do Laboratório (propor, não aplicar)."""
import json

from mt5_extracao.trade_analytics import export


def test_proposal_file_has_rules_numbers_and_warnings(tmp_path):
    evaluation = {"search": {"before": {"n": 10}, "after": {"n": 8}}, "folds": [], "folds_improved": 0,
                  "holdout": {"before": {"expectancy": -1.0}, "after": {"expectancy": 1.0}}, "weak": True, "removed": 2}
    rules = [{"tipo": "excluir_dia", "dias": [4]},
             {"tipo": "trocar_modo", "modo": "martingale", "quando": {"volatility_bucket": ["alta"]}}]
    path = export.proposal("robot:python:x", rules, evaluation, tested=12, folder=tmp_path)
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["regras_em_texto"][0] == "Não operar Sex" and data["alternativas_testadas"] == 12
    assert any("REPROVA" in w for w in data["avisos"]) and any("fraca" in w for w in data["avisos"])
    assert "Não é recomendação" in data["aviso"] and data["reserva_final"]["after"]["expectancy"] == 1.0
