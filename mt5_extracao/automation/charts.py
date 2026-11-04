"""Canal de apresentação da sessão própria; nenhuma operação de negociação."""
from pathlib import Path
import threading
import shutil
import time
import uuid

from .protocol import boolean, fields, number, text

OPERATIONS = ("chart.list", "chart.open", "chart.close", "chart.select", "chart.style", "chart.snapshot",
              "indicator.add", "indicator.remove", "study.catalog", "study.indicator.add", "panel.set", "panel.get")


def validate(method, params):
    fields(params, (), ("chart", "symbol", "timeframe", "style", "width", "height", "kind", "period",
                            "mean", "deviation", "indicator", "field", "value", "catalog_id"))
    if method not in OPERATIONS:
        raise ValueError("Operação gráfica desconhecida.")
    values = [method, "0", "", "", "", "", "", ""]
    allowed = {"chart"}
    chart = params.get("chart", "0")
    if not isinstance(chart, str) or not chart.isascii() or not chart.isdecimal() or len(chart) > 19 or int(chart) > 9223372036854775807:
        raise ValueError("chart deve ser ID decimal textual (64 bits), ou0 para o principal.")
    values[1] = chart
    if method == "study.catalog":
        allowed = set()
    elif method == "study.indicator.add":
        import re
        allowed.add("catalog_id")
        ident = params.get("catalog_id")
        if not isinstance(ident, str) or not re.fullmatch(r"native:(0|[1-9][0-9]{0,2})", ident):
            raise ValueError("catalog_id exige ID native:N retornado por study.catalog.")
        values[2] = ident
    elif method == "chart.open":
        from .. import timeframes
        allowed = {"symbol", "timeframe"}
        values[2] = text(params.get("symbol"), "symbol", 64)
        tf = timeframes.parse(params.get("timeframe", ""))
        if tf is None:
            raise ValueError("Timeframe nativo não reconhecido.")
        values[3] = str(tf.mt5)
    elif method == "chart.style":
        allowed.add("style")
        if params.get("style") not in ("light", "dark", "light_gray"):
            raise ValueError("style deve ser light, dark ou light_gray.")
        values[2] = params["style"]
    elif method == "chart.snapshot":
        allowed.update(("width", "height"))
        values[2:4] = [str(number(params.get(k, default), k, 200, 8192, integer=True))
                       for k, default in (("width", 1280), ("height", 800))]
    elif method == "indicator.add":
        allowed.update(("kind", "period", "mean", "deviation"))
        if params.get("kind") not in ("sma", "ema", "atr", "rsi", "adx", "super_atr_10"):
            raise ValueError("Indicador não suportado; consulte workflow methods.")
        if params["kind"] != "super_atr_10" and set(params) & {"mean", "deviation"}:
            raise ValueError("mean/deviation pertencem somente a super_atr_10.")
        values[2] = params["kind"]
        values[3] = str(number(params.get("period", 20), "period", 2, 500, integer=True))
        values[4] = str(number(params.get("mean", 0), "mean", 0, 1, integer=True))
        values[5] = str(number(params.get("deviation", 1.0), "deviation", .000001, 100))
    elif method == "indicator.remove":
        allowed.add("indicator")
        values[2] = text(params.get("indicator"), "indicator", 32)
        if not values[2].isdecimal():
            raise ValueError("Use o ID retornado por indicator.add.")
    elif method == "panel.set":
        allowed.update(("field", "value"))
        name = params.get("field")
        if name == "theme":
            value = number(params.get("value"), "theme", 0, 15, integer=True)
        elif name in ("timer", "probability", "gradient", "watermark", "clean", "bars"):
            value = int(boolean(params.get("value"), name))
        else:
            raise ValueError("Campo visual não suportado; ações de ordem não pertencem ao canal gráfico.")
        values[2:4] = [name, str(value)]
    if params.keys() - allowed:
        raise ValueError("Parâmetros não usados nesta operação: " + ", ".join(sorted(params.keys() - allowed)))
    return values


class Channel:
    def __init__(self, session):
        self.session = session
        self.lock = threading.Lock()
        self.unconfirmed = False
        self.last_id = None
        self.last_method = None

    def state(self):
        from ..robots import mt5_tools
        from .protocol import loads
        root = mt5_tools.common_files_dir(self.session.config)
        ready = False
        if root is not None and self.session.state == "running" and self.session.instrumented:
            path = root / ("mt5x_control_" + self.session.id + ".ready.json")
            if path.is_file():
                value = loads(path.read_text(encoding="utf-8-sig"))
                fields(value, ("session", "chart", "ready"))
                ready = value["session"] == self.session.id and value["ready"] is True
        return dict(ready=ready, unconfirmed=self.unconfirmed, request_id=self.last_id)

    def _response(self, path):
        from .protocol import loads
        result = loads(path.read_text(encoding="utf-8-sig"))
        fields(result, ("id", "ok", "data", "error"))
        text(result["id"], "id", 32)
        boolean(result["ok"], "ok")
        if not isinstance(result["data"], dict) or not isinstance(result["error"], str):
            raise ValueError("Recibo gráfico malformado.")
        return result

    def _data(self, result):
        data = dict(result["data"])
        if result["ok"] and self.last_method == "chart.snapshot":
            name = data.get("file")
            expected = "mt5xa_" + self.last_id + ".png"
            if name != expected:
                raise RuntimeError("Nome da captura diverge do pedido próprio.")
            from ..robots import mt5_tools
            common = mt5_tools.common_files_dir(self.session.config)
            if common is None:
                raise RuntimeError("Pasta comum indisponível para a captura.")
            folder = common / ("mt5x_control_" + self.session.id)
            source = folder / name
            if not folder.resolve().is_relative_to(common.resolve()) or not source.resolve().is_relative_to(folder.resolve()) or not source.is_file():
                raise RuntimeError("Captura confirmada sem arquivo próprio; preserve os artefatos.")
            with source.open("rb") as handle:
                if handle.read(8) != b"\x89PNG\r\n\x1a\n":
                    raise RuntimeError("Captura confirmada não contém PNG válido.")
            directory = Path(self.session.directory) / "captures"
            if not directory.resolve().is_relative_to(Path(self.session.directory).resolve()):
                raise RuntimeError("Pasta de captura fora da sessão.")
            directory.mkdir(exist_ok=True)
            path = directory / name
            shutil.copyfile(source, path)
            data["path"] = str(path)
        return data

    def request(self, method, params, timeout=15):
        from ..robots import mt5_tools
        values = validate(method, params)
        number(timeout, "timeout", .1, 120)
        with self.lock:
            if self.unconfirmed:
                raise RuntimeError("Resposta gráfica anterior incerta; consulte artefatos e reabra a sessão antes de outro comando.")
            if self.session.state != "running" or not self.session.instrumented:
                raise ValueError("Sessão instrumentada ainda não está rodando.")
            root = mt5_tools.common_files_dir(self.session.config)
            if root is None:
                raise ValueError("Pasta comum da sessão indisponível.")
            name = "mt5x_control_" + self.session.id
            request_id = uuid.uuid4().hex
            self.last_id = request_id
            self.last_method = method
            request = root / (name + ".request.tsv")
            response = root / (name + ".response.json")
            if request.exists():
                raise ValueError("Já existe um pedido gráfico pendente; preserve-o para conciliação.")
            temp = request.with_suffix(".tmp")
            temp.write_text("\t".join([request_id] + values) + "\n", encoding="utf-8")
            temp.replace(request)
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                if response.is_file():
                    result = None
                    try:
                        result = self._response(response)
                        if result["id"] == request_id:
                            data = self._data(result)
                            if not result["ok"]:
                                raise ValueError(result["error"] or "Comando gráfico recusado pelo terminal.")
                            return data
                    except (ValueError, TypeError, KeyError, OSError, RuntimeError):
                        # Um ACK de recusa é conclusivo; recibo ilegível/arquivo divergente é incerto.
                        if result is None or result.get("id") != request_id or result.get("ok") is not False:
                            self.unconfirmed = True
                        raise
                time.sleep(.05)
            self.unconfirmed = True
            raise RuntimeError("Tempo esgotado: resultado gráfico incerto, sem reenvio automático. Pedido " + request_id)

    def receipt(self):
        """Consulta posterior sem reenviar o efeito. Um ACK tardio permite continuar."""
        from ..robots import mt5_tools
        root = mt5_tools.common_files_dir(self.session.config)
        if root is None or self.last_id is None:
            return dict(state="no_request")
        path = root / ("mt5x_control_" + self.session.id + ".response.json")
        if path.exists():
            result = self._response(path)
            if result.get("id") == self.last_id:
                result = dict(result, data=self._data(result))
                self.unconfirmed = False
                return dict(state="responded", receipt=result)
        progress = None
        path = root / ("mt5x_control_" + self.session.id + ".progress.json")
        if path.is_file():
            from .protocol import loads
            value = loads(path.read_text(encoding="utf-8-sig"))
            fields(value, ("id", "data"))
            if value["id"] == self.last_id and isinstance(value["data"], dict):
                progress = value["data"]
        return dict(state="unconfirmed" if self.unconfirmed else "pending", request_id=self.last_id, progress=progress)


def source(session_id, *, super_atr=False, study_catalog_sha256=None):
    import re
    if not re.fullmatch(r"[0-9a-f]{32}", session_id):
        raise ValueError("ID da sessão gráfica inválido.")
    if study_catalog_sha256 is not None and not re.fullmatch(r"[0-9a-f]{64}", study_catalog_sha256):
        raise ValueError("Hash do catálogo de estudo inválido.")
    body = Path(__file__).with_name("native_charts.mqh").read_text(encoding="utf-8")
    helpers = Path(__file__).with_name("native_studies.mqh").read_text(encoding="utf-8")
    defines = "#define MT5X_AGENT_SUPER_ATR\n" if super_atr else ""
    defines += '#define MT5X_AGENT_STUDY_HASH "' + (study_catalog_sha256 or "") + '"\n'
    return defines + body.replace("// @STUDY_HELPERS@", helpers).replace("@SESSION@", session_id)
