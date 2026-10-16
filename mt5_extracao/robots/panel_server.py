"""
Painel vivo: um servidor local que entrega o painel de resultados, mantém a página atualizada e executa ações
do próprio projeto a partir de botões. Somente biblioteca padrão.

    server = PanelServer(db, config)        # escolhe uma porta livre em 127.0.0.1
    server.start()                          # atende em segundo plano
    print(server.url)                       # http://127.0.0.1:<porta>/?k=<chave>

Regras de segurança (docs/PLANO_DE_TRABALHO.md, "Painel vivo"), todas cobertas por teste:
1. Só escuta em 127.0.0.1 e recusa pedidos cujo cabeçalho Host não seja 127.0.0.1 ou localhost.
2. Uma chave aleatória por sessão: sem ela, nenhuma rota responde. Pedidos de outra origem (Origin) são recusados.
3. Lista fechada de ações (ACTIONS); cada parâmetro é conferido contra o catálogo, a base e os timeframes.
   Nada de comando livre, caminho de arquivo nem texto que vá para um interpretador.
4. Um trabalho por vez. O que fecha o MetaTrader 5 só roda com a confirmação dada na página (HTTP 428 sem ela).
5. Fora daqui, de propósito: iniciar robô em conta, liberar o PARAR TUDO e editar configuração ou código.
O Python continua sem enviar ordens: as ações são leitura da base, backtests e a gravação do arquivo de parada.
"""
import hmac
from mt5_extracao import profit_factor
import json
import logging
import secrets
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

log = logging.getLogger(__name__)

HOST = "127.0.0.1"                       # nunca outro endereço: o painel executa ações
ALLOWED_HOSTS = ("127.0.0.1", "localhost")
KEY_HEADER = "X-MT5X-Key"
MAX_BODY = 64 * 1024
MAX_ITEMS = 60                            # robôs, ativos ou timeframes por pedido
RUNNING, DONE, FAILED = "em andamento", "concluído", "falhou"


class ActionError(ValueError):
    """Pedido recusado: ação fora da lista ou parâmetro que não existe. A mensagem vai para a página."""


class NeedsConfirmation(ActionError):
    """A ação interrompe algo (fecha o MetaTrader 5): a página pede a confirmação e repete com `confirm`."""

    def __init__(self, message: str, confirm: str):
        super().__init__(message)
        self.confirm = confirm


class Busy(RuntimeError):
    """Já há um trabalho em andamento."""


class Job:
    def __init__(self, action: str, label: str):
        self.id = secrets.token_hex(6)
        self.action, self.label = action, label
        self.status, self.messages, self.result = RUNNING, [], ""
        self.started_at = datetime.now().strftime("%H:%M:%S")
        self.finished_at = ""

    def say(self, text: str) -> None:
        self.messages.append(str(text))
        del self.messages[:-40]            # só as últimas linhas ficam na página

    def to_dict(self) -> dict:
        return {"id": self.id, "action": self.action, "label": self.label, "status": self.status,
                "messages": list(self.messages), "result": self.result, "started_at": self.started_at,
                "finished_at": self.finished_at}


class PanelApp:
    """A lógica do painel vivo, separada do HTTP para ser testada direto."""

    def __init__(self, db, config, notifier: Optional[Callable] = None, connect: Optional[Callable] = None):
        self.db, self.config = db, config
        self._connect = connect            # devolve o conector do MT5 já ligado (None = painel sem acesso ao MT5)
        self.key = secrets.token_urlsafe(24)
        self._lock = threading.Lock()
        self._job: Optional[Job] = None
        self._notify = notifier

    # --- dados ------------------------------------------------------------------------

    def catalog(self) -> List:
        from . import catalog
        return catalog.load_catalog(self.config, self.db)

    def matrix(self) -> dict:
        from . import matrix
        data = matrix.build(self.db, self.config)
        # versão: muda quando muda o que a página mostra (a hora em que foi gerada não conta)
        stable = {k: v for k, v in data.items() if k != "generated_at"}
        data["version"] = format(hash(json.dumps(stable, sort_keys=True, default=str)) & 0xFFFFFFFF, "08x")
        data["actions"] = sorted(ACTIONS)
        return data

    def job(self) -> Optional[dict]:
        with self._lock:
            return self._job.to_dict() if self._job else None

    def check_key(self, given: Optional[str]) -> bool:
        return bool(given) and hmac.compare_digest(str(given), self.key)

    # --- validação dos parâmetros -----------------------------------------------------

    def _robots(self, keys, robots) -> List:
        if not isinstance(keys, list) or not keys or len(keys) > MAX_ITEMS:
            raise ActionError("Informe de 1 a %d robôs." % MAX_ITEMS)
        by_key = {r.key: r for r in robots}
        missing = [str(k)[:60] for k in keys if k not in by_key]
        if missing:
            raise ActionError("Robô que não está no catálogo: " + ", ".join(missing))
        return [by_key[k] for k in keys]

    def _symbols(self, symbols, known) -> List[str]:
        if not isinstance(symbols, list) or not symbols or len(symbols) > MAX_ITEMS:
            raise ActionError("Informe de 1 a %d ativos." % MAX_ITEMS)
        missing = [str(s)[:40] for s in symbols if s not in known]
        if missing:
            raise ActionError("Ativo sem base: " + ", ".join(missing))
        return list(symbols)

    @staticmethod
    def _timeframes(names) -> List:
        from mt5_extracao.timeframes import Timeframe
        if not isinstance(names, list) or not names or len(names) > MAX_ITEMS:
            raise ActionError("Informe ao menos um timeframe.")
        valid = {tf.name: tf for tf in Timeframe}
        missing = [str(n)[:10] for n in names if n not in valid]
        if missing:
            raise ActionError("Timeframe desconhecido: " + ", ".join(missing))
        return [valid[n] for n in names]

    # --- ações ------------------------------------------------------------------------

    def _backtest(self, params: dict, job: Job) -> str:
        from . import backtest, catalog
        data = self.matrix()
        robots = self._robots(params.get("robots"), self.catalog())
        symbols = self._symbols(params.get("symbols"), set(data["symbols"]))
        tfs = self._timeframes(params.get("tfs"))
        mql5 = any(r.kind == catalog.MQL5 for r in robots)
        names = {r.key: r.name for r in robots}

        def progress(item):
            head = f"{names[item['robot']]} {item['symbol']} {item['timeframe']}: "
            if item["run"] is None:
                job.say(head + "não testado: " + item["error"])
            else:
                m = item["run"]["metrics"]
                job.say(head + f"{m['trades']} operações, profit factor {profit_factor.text(m['profit_factor'])}" +
                        (" (já testado)" if item["skipped"] else ""))
        from . import mt5_tools
        bridge_was_running = False
        try:
            if mql5 and mt5_tools.terminal_running(self.config):      # a confirmação já foi conferida em _precheck
                job.say("Fechando o MetaTrader 5 para rodar os testes...")
                bridge_was_running = mt5_tools.release_terminal(self.config)
            results = backtest.run_batch(self.db, self.config, robots, symbols, tfs,
                                         resume=bool(params.get("resume")), progress=progress)
        finally:
            if bridge_was_running:
                mt5_tools.restart_bridge()
                job.say("Ponte religada.")
        failed = sum(1 for r in results if r["run"] is None)
        skipped = sum(1 for r in results if r["skipped"])
        return f"{len(results) - failed - skipped} testado(s), {skipped} pulado(s), {failed} sem teste"

    def _resample(self, params: dict, job: Job) -> str:
        from mt5_extracao import resample
        data = self.matrix()
        symbols = self._symbols(params.get("symbols"), set(data["symbols"]))
        base = self._timeframes([params.get("base")])[0]
        targets = self._timeframes(params.get("tfs"))
        for target in targets:
            try:
                resample.check_pair(base, target)
            except ValueError as e:
                raise ActionError(str(e)) from None
        done = 0
        for symbol in symbols:
            try:
                rows = resample.resample_symbol(self.db, symbol, base, targets)
            except (ValueError, RuntimeError) as e:
                job.say(f"{symbol}: {e}")
                continue
            done += len(rows)
            for row in rows:
                job.say(f"{symbol} {row['timeframe']}: {row['bars']} barras montadas de {base.name}")
        return f"{done} série(s) montada(s)"

    def _battery(self, params: dict, job: Job) -> str:
        from . import battery
        data = self.matrix()
        robots = self.catalog()
        robot = self._robots([params.get("robot")], robots)[0]
        symbol = self._symbols([params.get("symbol")], set(data["symbols"]) | {c["symbol"] for c in data["cells"]})[0]
        try:
            report = battery.run_battery(self.db, self.config, robot, robots, symbol=symbol)
        except ValueError as e:
            raise ActionError(str(e)) from None
        for item in report.items:
            if item.status in (battery.FAIL, battery.WARN):
                job.say(f"{item.icon} {item.name}: {item.detail}")
        return f"{robot.name} em {report.symbol or symbol}: bateria " + ("APROVADA" if report.approved else "REPROVADA")

    def _connector(self):
        if self._connect is None:
            raise ActionError("Este painel foi aberto sem acesso ao MetaTrader 5. Abra-o com 'mt5x panel --serve' ou "
                              "pelo botão da janela do programa.")
        try:
            connector = self._connect()
        except Exception as e:
            raise ActionError(f"Não foi possível conectar ao MetaTrader 5: {e}") from None
        if connector is None or not getattr(connector, "is_initialized", True):
            raise ActionError("Não foi possível conectar ao MetaTrader 5. Rode 'mt5x doctor' para diagnosticar.")
        return connector

    def _update(self, params: dict, job: Job) -> str:
        """Atualiza até agora as séries EXTRAÍDAS dos ativos e timeframes pedidos e remonta as séries montadas delas."""
        from mt5_extracao import resample, services
        from mt5_extracao.indicator_calculator import IndicatorCalculator
        data = self.matrix()
        symbols = self._symbols(params.get("symbols"), set(data["symbols"]))
        tfs = self._timeframes(params.get("tfs"))
        extractor = services.create_extractor(self.config, self._connector(), self.db, IndicatorCalculator())
        built = resample.origins(self.db)                       # {(ativo, timeframe): timeframe de origem}
        ok_total = failed_total = 0
        for tf in tfs:
            wanted = [s for s in symbols if tf.name in data["inventory"].get(s, {}) and (s, tf.name) not in built]
            if not wanted:
                continue
            done, result = threading.Event(), {}

            def finished(ok, failed, canceled):
                result.update(ok=ok, failed=failed + canceled)
                done.set()
            job.say(f"Atualizando {', '.join(wanted)} {tf.name}...")
            extractor.update_symbols(wanted, tf.value, tf.label, include_indicators=False, default_days=30,
                                     max_workers=1, update_progress_callback=lambda pct, msg: None,
                                     finished_callback=finished)
            if not done.wait(1800):
                raise ActionError(f"A atualização de {tf.name} não terminou em 30 minutos.")
            ok_total += result.get("ok", 0)
            failed_total += result.get("failed", 0)
            job.say(f"{tf.name}: {result.get('ok', 0)} ok, {result.get('failed', 0)} com falha")
        rebuilt = 0
        for (symbol, target), source in sorted(built.items()):
            if symbol in symbols and any(tf.name == source for tf in tfs):
                target_tf, source_tf = self._timeframes([target])[0], self._timeframes([source])[0]
                rebuilt += len(resample.resample_symbol(self.db, symbol, source_tf, [target_tf]))
        if rebuilt:
            job.say(f"{rebuilt} série(s) montada(s) refeita(s) com as barras novas")
        if not ok_total and not failed_total:
            return "Nada a atualizar: os timeframes pedidos são montados de outro ou não têm base."
        return f"{ok_total} série(s) atualizada(s), {failed_total} com falha" + (f", {rebuilt} remontada(s)" if rebuilt else "")

    def _specs(self, params: dict, job: Job) -> str:
        from mt5_extracao.strategies import specs
        data = self.matrix()
        symbols = self._symbols(params.get("symbols"), set(data["symbols"]))
        result = specs.fetch_and_save_specs(self._connector(), self.db, symbols)
        for symbol, status in result.items():
            job.say(f"{symbol}: " + ("especificação gravada" if status == "ok" else status))
        saved = sum(1 for v in result.values() if v == "ok")
        return f"{saved} de {len(symbols)} especificação(ões) gravada(s). Teste de novo as estratégias Python desses ativos."

    def _precheck(self, action: str, params: dict) -> None:
        """Antes de aceitar: o que fecha o MetaTrader 5 precisa de `confirm: "close_mt5"` no pedido."""
        if action != "backtest":
            return
        from . import catalog, mt5_tools
        keys = params.get("robots") if isinstance(params.get("robots"), list) else []
        by_key = {r.key: r for r in self.catalog()}
        if not any(getattr(by_key.get(k), "kind", "") == catalog.MQL5 for k in keys):
            return
        if mt5_tools.terminal_running(self.config) and params.get("confirm") != "close_mt5":
            raise NeedsConfirmation("O teste de robô MQL5 usa o Strategy Tester e precisa FECHAR o MetaTrader 5 desta "
                                    "instalação (a ponte é religada no fim). Se houver um robô operando nele, não "
                                    "confirme.", "close_mt5")

    def _stop_all(self, params: dict, job: Job) -> str:
        from . import live
        path = live.stop_all(self.config, "Acionado pelo painel.")
        job.say("Os robôs com a camada de segurança não abrem novas operações. Posições abertas NÃO foram fechadas.")
        job.say("Para liberar: mt5x robots stop --release (só pela linha de comando).")
        return f"PARAR TUDO acionado: {path}"

    def submit(self, action: str, params: dict) -> dict:
        """Confere e inicia a ação em segundo plano. ActionError se o pedido for inválido; Busy se já há um trabalho."""
        spec = ACTIONS.get(action if isinstance(action, str) else "")
        if spec is None:
            raise ActionError("Ação desconhecida. As ações do painel são: " + ", ".join(sorted(ACTIONS)))
        if not isinstance(params, dict):
            raise ActionError("Parâmetros inválidos.")
        unknown = set(params) - set(spec["params"])
        if unknown:
            raise ActionError("Parâmetro não aceito nesta ação: " + ", ".join(sorted(str(u)[:30] for u in unknown)))
        with self._lock:
            if self._job is not None and self._job.status == RUNNING:
                raise Busy(f"Já há um trabalho em andamento ({self._job.label}). Espere ele terminar.")
        self._precheck(action, params)
        with self._lock:
            if self._job is not None and self._job.status == RUNNING:
                raise Busy(f"Já há um trabalho em andamento ({self._job.label}). Espere ele terminar.")
            job = self._job = Job(action, spec["label"])
        handler = getattr(self, spec["handler"])

        def work():
            level = None
            try:
                job.result, job.status = handler(params, job), DONE
            except ActionError as e:
                job.result, job.status, level = str(e), FAILED, "aviso"
            except Exception as e:      # um erro inesperado não pode derrubar o servidor
                log.exception("Erro em ação do painel")
                job.result, job.status, level = f"{type(e).__name__}: {e}", FAILED, "aviso"
            job.finished_at = datetime.now().strftime("%H:%M:%S")
            if self._notify:
                try:
                    self._notify(f"Painel: {job.label} ({job.status})", job.result, level or
                                 ("crítico" if action == "stop_all" else None))
                except Exception:
                    pass
        threading.Thread(target=work, daemon=True, name="mt5x-panel-job").start()
        return job.to_dict()


# ação -> o método que a executa, o rótulo mostrado e os parâmetros aceitos
ACTIONS: Dict[str, dict] = {
    "backtest": {"handler": "_backtest", "label": "Backtest",
                 "params": ("robots", "symbols", "tfs", "resume", "confirm")},
    "update": {"handler": "_update", "label": "Atualizar a base", "params": ("symbols", "tfs")},
    "specs": {"handler": "_specs", "label": "Gravar especificação de contrato", "params": ("symbols",)},
    "resample": {"handler": "_resample", "label": "Montar timeframes", "params": ("symbols", "base", "tfs")},
    "battery": {"handler": "_battery", "label": "Bateria de segurança", "params": ("robot", "symbol")},
    "stop_all": {"handler": "_stop_all", "label": "PARAR TUDO", "params": ()},
}


def _handler(app: PanelApp, port_of: Callable[[], int]):
    from . import panel

    class Handler(BaseHTTPRequestHandler):
        server_version = "mt5x-panel"

        def log_message(self, fmt, *args):       # sem poluir o terminal: vai para o log do projeto
            log.debug("painel: " + fmt % args)

        def _send(self, code: int, body, content_type="application/json; charset=utf-8"):
            data = body if isinstance(body, bytes) else json.dumps(body, ensure_ascii=False, default=str).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Referrer-Policy", "no-referrer")       # a chave está no endereço: não vaza por link
            self.send_header("Content-Security-Policy",
                             "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; "
                             "connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'")
            self.end_headers()
            self.wfile.write(data)

        def _allowed(self, query: dict) -> bool:
            host = (self.headers.get("Host") or "").rsplit(":", 1)
            if host[0] not in ALLOWED_HOSTS or (len(host) == 2 and host[1] != str(port_of())):
                self._send(403, {"error": "Este painel só atende na própria máquina."})
                return False
            origin = self.headers.get("Origin")
            if origin and origin not in tuple(f"http://{h}:{port_of()}" for h in ALLOWED_HOSTS):
                self._send(403, {"error": "Pedido de outra página recusado."})
                return False
            key = self.headers.get(KEY_HEADER) or (query.get("k") or [""])[0]
            if not app.check_key(key):
                self._send(403, {"error": "Chave ausente ou inválida. Abra o painel pelo endereço que o programa mostrou."})
                return False
            return True

        def do_GET(self):
            url = urlparse(self.path)
            query = parse_qs(url.query)
            if not self._allowed(query):
                return
            if url.path == "/":
                self._send(200, panel.render(app.matrix(), live=True).encode("utf-8"), "text/html; charset=utf-8")
            elif url.path == "/api/matrix":
                self._send(200, app.matrix())
            elif url.path == "/api/job":
                self._send(200, {"job": app.job()})
            else:
                self._send(404, {"error": "Não existe."})

        def do_POST(self):
            url = urlparse(self.path)
            if not self._allowed({}):            # em POST a chave só vale no cabeçalho, nunca no endereço
                return
            if url.path != "/api/action":
                self._send(404, {"error": "Não existe."})
                return
            if (self.headers.get("Content-Type") or "").split(";")[0].strip() != "application/json":
                self._send(415, {"error": "Envie JSON."})
                return
            try:
                length = int(self.headers.get("Content-Length") or 0)
            except ValueError:
                length = -1
            if not 0 < length <= MAX_BODY:
                self._send(413, {"error": "Pedido vazio ou grande demais."})
                return
            try:
                body = json.loads(self.rfile.read(length).decode("utf-8"))
                action, params = body.get("action"), body.get("params", {})
            except (ValueError, AttributeError, UnicodeDecodeError):
                self._send(400, {"error": "JSON inválido."})
                return
            try:
                self._send(202, {"job": app.submit(action, params)})
            except NeedsConfirmation as e:
                self._send(428, {"error": str(e), "confirm": e.confirm})
            except ActionError as e:
                self._send(400, {"error": str(e)})
            except Busy as e:
                self._send(409, {"error": str(e)})

    return Handler


class PanelServer:
    """O servidor HTTP do painel vivo, sempre em 127.0.0.1."""

    def __init__(self, db, config, port: int = 0, notifier: Optional[Callable] = None,
                 connect: Optional[Callable] = None):
        self.app = PanelApp(db, config, notifier, connect)
        self.httpd = ThreadingHTTPServer((HOST, port), _handler(self.app, lambda: self.port))
        self.httpd.daemon_threads = True
        self._thread: Optional[threading.Thread] = None

    @property
    def port(self) -> int:
        return self.httpd.server_address[1]

    @property
    def url(self) -> str:
        return f"http://{HOST}:{self.port}/?k={self.app.key}"

    def start(self) -> "PanelServer":
        self._thread = threading.Thread(target=self.httpd.serve_forever, daemon=True, name="mt5x-panel")
        self._thread.start()
        return self

    def stop(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
