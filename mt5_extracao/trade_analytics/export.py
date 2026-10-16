"""
Exportar a proposta do Laboratório (seção 6, item 6 do plano): um arquivo com as regras, a amostra e os números de
procura, de cada prova e da reserva final, e quantas alternativas foram testadas. Exportar é PROPOR, não aplicar: para
virar preset, a proposta passa por `mt5x validate`, bateria e paridade, como qualquer mudança de robô.

    path = proposal(scope="robot:python:donchian", rules=[...], evaluation=rules.evaluate(...), tested=12)
"""
import json
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from mt5_extracao.trade_analytics import rules as rules_mod

STUDY_ONLY = ("martingale", "anti_martingale")


def _folder() -> Path:
    from mt5_extracao.bootstrap import paths
    return Path(paths.PROJECT_ROOT) / "exports" / "propostas"


def proposal(scope: str, rules: List[dict], evaluation: dict, tested: int = 0, folder: Optional[Path] = None,
             notes: Optional[List[str]] = None, context: Optional[dict] = None) -> Path:
    folder = Path(folder) if folder else _folder()
    folder.mkdir(parents=True, exist_ok=True)
    study = [r for r in rules if r.get("tipo") == "trocar_modo" and r.get("modo") in STUDY_ONLY]
    grid = [r for r in rules if r.get("tipo") == "trocar_modo" and str(r.get("modo", "")).startswith("gradiente")]
    warnings = []
    if study:
        warnings.append("Contém martingale/anti-martingale: a bateria de segurança REPROVA para operação real. "
                        "Use só como estudo.")
    if grid:
        warnings.append("Contém gradiente: só pode ir para operação como parâmetros do Gradiente Linear do HADOUKEN "
                        "(T12.4/T12.5), dentro da camada de segurança.")
    if evaluation.get("weak"):
        warnings.append("Evidência fraca: poucas operações para o walk-forward.")
    data = {
        "gerado_em": datetime.now().isoformat(timespec="seconds"),
        "escopo": scope,
        "aviso": "Proposta para estudo. Não é recomendação de operação; aplicar exige mt5x validate, bateria e paridade.",
        "regras": rules, "regras_em_texto": [rules_mod.describe(r) for r in rules],
        "alternativas_testadas": int(tested), "avisos": warnings + list(notes or []),
        "procura": evaluation.get("search"), "provas": evaluation.get("folds"),
        "provas_que_melhoraram": evaluation.get("folds_improved"), "reserva_final": evaluation.get("holdout"),
        "operacoes_removidas": evaluation.get("removed"),
    }
    if context is not None:
        data["contexto_populacao"] = context
    safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in scope)
    path = folder / f"{safe}_{datetime.now():%Y%m%d-%H%M%S}.json"
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=float), encoding="utf-8")
    return path
