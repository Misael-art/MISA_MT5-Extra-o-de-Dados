"""LAB-08: anotações do gráfico do Laboratório amarradas à identidade da análise.

Modelo puro (sem Tk/MT5): linhas, regiões e notas com editar/excluir/desfazer e
exportação JSON. Reabrir só aplica o estudo à mesma análise congelada.
"""
import copy
import json
import math

STUDY_VERSION = 1
KINDS = ("hline", "vline", "region", "note")
MAX_ITEMS = 500


def _finite(value, name):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f"{name} exige número finito.")
    return float(value)


def _validate(item):
    kind = item.get("kind")
    if kind not in KINDS:
        raise ValueError("Anotação desconhecida; use " + ", ".join(KINDS) + ".")
    clean = dict(id=int(item["id"]), kind=kind, text=str(item.get("text", ""))[:500])
    if kind == "hline":
        clean["y"] = _finite(item.get("y"), "y")
    elif kind == "vline":
        clean["x"] = _finite(item.get("x"), "x")
    elif kind == "region":
        x0, x1 = sorted((_finite(item.get("x0"), "x0"), _finite(item.get("x1"), "x1")))
        if x0 == x1:
            raise ValueError("Região exige início e fim diferentes.")
        clean.update(x0=x0, x1=x1)
    else:
        clean.update(x=_finite(item.get("x"), "x"), y=_finite(item.get("y"), "y"))
        if not clean["text"].strip():
            raise ValueError("Nota exige texto.")
    return clean


class Study:
    """Coordenadas: x em data matplotlib (dias UTC), y na escala de preço do painel principal."""

    def __init__(self, analysis_id, items=()):
        if not analysis_id:
            raise ValueError("Estudo exige uma análise congelada.")
        self.analysis_id = analysis_id
        self.items = [_validate(item) for item in items]
        self.hidden = set()
        self._history = []
        self._next = max((item["id"] for item in self.items), default=0) + 1

    def _remember(self):
        self._history.append(copy.deepcopy(self.items))
        del self._history[:-100]

    def add(self, kind, **values):
        if len(self.items) >= MAX_ITEMS:
            raise ValueError(f"Limite de {MAX_ITEMS} anotações por estudo.")
        item = _validate(dict(values, id=self._next, kind=kind))
        self._remember()
        self.items.append(item)
        self._next += 1
        return item

    def edit(self, ident, **values):
        index = next((i for i, item in enumerate(self.items) if item["id"] == ident), None)
        if index is None:
            raise ValueError("Anotação inexistente.")
        updated = _validate(dict(self.items[index], **values, id=ident, kind=self.items[index]["kind"]))
        self._remember()
        self.items[index] = updated
        return updated

    def remove(self, ident):
        if not any(item["id"] == ident for item in self.items):
            raise ValueError("Anotação inexistente.")
        self._remember()
        self.items = [item for item in self.items if item["id"] != ident]

    def undo(self):
        if not self._history:
            return False
        self.items = self._history.pop()
        return True

    def to_json(self):
        return json.dumps(dict(study_version=STUDY_VERSION, analysis_id=self.analysis_id,
                               hidden_series=sorted(self.hidden), items=self.items),
                          ensure_ascii=False, allow_nan=False, indent=1)

    @classmethod
    def from_json(cls, text, analysis_id):
        payload = json.loads(text)
        if payload.get("study_version") != STUDY_VERSION:
            raise ValueError("Versão de estudo não suportada.")
        if payload.get("analysis_id") != analysis_id:
            raise ValueError("Estudo pertence a outra análise; congele a mesma seleção antes de reabrir.")
        study = cls(analysis_id, payload.get("items", ()))
        study.hidden = set(payload.get("hidden_series", ()))
        return study
