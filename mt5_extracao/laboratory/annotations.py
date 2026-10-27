"""LAB-08: anotações do gráfico do Laboratório amarradas à identidade da análise.

Modelo puro (sem Tk/MT5): linhas, regiões e notas com selecionar/mover/editar/
excluir/desfazer, visibilidade das séries (ocultar/remover da vista, sem recalcular
a análise), vista (zoom/cursor) e exportação JSON. O estudo grava a identidade da
análise (fonte, parâmetros, unidades) e só reabre sobre a mesma análise congelada.
"""
import copy
import json
import math

STUDY_VERSION = 2
# v1 (26/10/2026) não tinha identidade/séries removidas/vista; a leitura continua aceita
# enquanto houver estudos exportados pela prévia anterior. Remover quando a v2 for aceita.
READABLE_VERSIONS = (1, 2)
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


def identity_from_report(report):
    """Identidade verificável do estudo: o que o usuário vê depende destes campos."""
    return dict(symbol=report["symbol"], grid_timeframe=report["grid_timeframe"],
                laboratory_version=report.get("laboratory_version"), selection=report["selection"],
                series=[dict(id=row["id"], name=row["name"], timeframe=row["timeframe"], unit=row["unit"],
                             parameters=row.get("parameters"), algorithm_version=row.get("algorithm_version"),
                             source=row.get("source")) for row in report["channels"]])


def _view(view):
    clean = {}
    if view.get("xlim") is not None:
        x0, x1 = (_finite(value, "xlim") for value in view["xlim"])
        if x0 < x1:
            clean["xlim"] = [x0, x1]
    if view.get("cursor") is not None:
        clean["cursor"] = _finite(view["cursor"], "cursor")
    return clean


class Study:
    """Coordenadas: x em data matplotlib (dias UTC), y na escala de preço do painel principal."""

    def __init__(self, analysis_id, items=(), identity=None):
        if not analysis_id:
            raise ValueError("Estudo exige uma análise congelada.")
        self.analysis_id = analysis_id
        self.identity = identity
        self.items = [_validate(item) for item in items]
        self.hidden = set()      # fora do desenho, ainda na legenda
        self.removed = set()     # fora do desenho e da legenda; restaurável sem recongelar
        self.selected = None
        self.view = {}           # zoom e cursor; não entra no desfazer
        self._history = []
        self._next = max((item["id"] for item in self.items), default=0) + 1

    def _remember(self):
        self._history.append((copy.deepcopy(self.items), set(self.hidden), set(self.removed)))
        del self._history[:-100]

    def _index(self, ident):
        index = next((i for i, item in enumerate(self.items) if item["id"] == ident), None)
        if index is None:
            raise ValueError("Anotação inexistente.")
        return index

    def add(self, kind, **values):
        if len(self.items) >= MAX_ITEMS:
            raise ValueError(f"Limite de {MAX_ITEMS} anotações por estudo.")
        item = _validate(dict(values, id=self._next, kind=kind))
        self._remember()
        self.items.append(item)
        self._next += 1
        self.selected = item["id"]
        return item

    def edit(self, ident, **values):
        index = self._index(ident)
        updated = _validate(dict(self.items[index], **values, id=ident, kind=self.items[index]["kind"]))
        self._remember()
        self.items[index] = updated
        return updated

    def move(self, ident, dx=0.0, dy=0.0):
        """Desloca a geometria inteira; cada tipo só usa os eixos que possui."""
        item = self.items[self._index(ident)]
        dx, dy = _finite(dx, "dx"), _finite(dy, "dy")
        shifted = {key: item[key] + dx for key in ("x", "x0", "x1") if key in item}
        if "y" in item:
            shifted["y"] = item["y"] + dy
        return self.edit(ident, **shifted)

    def hit(self, x, y, tolerance_x, tolerance_y):
        """Anotação mais próxima do ponto dentro da tolerância (unidades de dado), ou None."""
        best = None
        for item in self.items:
            if item["kind"] == "hline":
                distance = abs(item["y"] - y) / tolerance_y
            elif item["kind"] == "vline":
                distance = abs(item["x"] - x) / tolerance_x
            elif item["kind"] == "region":
                distance = 0.5 if item["x0"] <= x <= item["x1"] else min(abs(x - item["x0"]), abs(x - item["x1"])) / tolerance_x
            else:
                distance = max(abs(item["x"] - x) / tolerance_x, abs(item["y"] - y) / tolerance_y)
            # Bordas e linhas vencem o interior de uma região sobreposta.
            if distance <= 1 and (best is None or distance < best[0]):
                best = (distance, item["id"])
        return best and best[1]

    def select(self, ident):
        if ident is not None:
            self._index(ident)
        self.selected = ident

    def remove(self, ident):
        self._index(ident)
        self._remember()
        self.items = [item for item in self.items if item["id"] != ident]
        if self.selected == ident:
            self.selected = None

    def toggle_series(self, series_id):
        self._remember()
        self.hidden ^= {series_id}

    def remove_series(self, series_id):
        if series_id in self.removed:
            raise ValueError("Série já removida da vista.")
        self._remember()
        self.removed.add(series_id)
        self.hidden.discard(series_id)

    def restore_series(self):
        if not self.removed:
            raise ValueError("Nenhuma série removida.")
        self._remember()
        self.removed.clear()

    def undo(self):
        if not self._history:
            return False
        self.items, self.hidden, self.removed = self._history.pop()
        if self.selected not in {item["id"] for item in self.items}:
            self.selected = None
        return True

    def to_json(self):
        return json.dumps(dict(study_version=STUDY_VERSION, analysis_id=self.analysis_id, identity=self.identity,
                               hidden_series=sorted(self.hidden), removed_series=sorted(self.removed),
                               view=self.view, items=self.items),
                          ensure_ascii=False, allow_nan=False, indent=1)

    @classmethod
    def for_report(cls, report):
        return cls(report["analysis_id"], identity=identity_from_report(report))

    @classmethod
    def from_json(cls, text, analysis_id, series_ids=None):
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as error:
            raise ValueError(f"Arquivo não é um estudo JSON: {error.msg}.") from error
        if not isinstance(payload, dict) or payload.get("study_version") not in READABLE_VERSIONS:
            raise ValueError("Versão de estudo não suportada; exporte novamente pelo Laboratório.")
        if payload.get("analysis_id") != analysis_id:
            identity = payload.get("identity") or {}
            origin = " ".join(str(identity.get(key, "")) for key in ("symbol", "grid_timeframe")).strip()
            raise ValueError(f"Estudo pertence a outra análise ({str(payload.get('analysis_id'))[:12]}"
                             f"{' · ' + origin if origin else ''}); congele a mesma seleção antes de reabrir.")
        hidden, removed = set(payload.get("hidden_series", ())), set(payload.get("removed_series", ()))
        if series_ids is not None and (hidden | removed) - set(series_ids):
            raise ValueError("Estudo cita séries ausentes nesta análise: " + ", ".join(sorted((hidden | removed) - set(series_ids))))
        study = cls(analysis_id, payload.get("items", ()), payload.get("identity"))
        study.hidden, study.removed = hidden, removed
        study.view = _view(payload.get("view") or {})
        return study
