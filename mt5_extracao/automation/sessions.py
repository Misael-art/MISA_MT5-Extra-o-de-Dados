"""PLAY/REPLAY por serviço, usando Session/Consent existentes, sem Tk ou outro executor."""
from datetime import datetime
import threading
import time
import uuid

from .charts import Channel
from .protocol import boolean, fields, number, text
from ..robots import catalog, controls, sessions


def _context(params):
    from .. import timeframes
    symbol = text(params.get("symbol"), "symbol", 64)
    tf = timeframes.parse(params.get("timeframe", ""))
    if tf is None:
        raise ValueError("Timeframe nativo inválido.")
    preset = params.get("preset")
    if preset is not None:
        text(preset, "preset")
    return symbol, tf.name, preset


def _date(value):
    if not isinstance(value, str):
        raise ValueError("Informe data AAAA-MM-DD.")
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        raise ValueError("Informe data AAAA-MM-DD.") from None


class SessionController:
    def __init__(self, db, config, read_account):
        self.db, self.config, self.read_account = db, config, read_account
        self.reviews, self.jobs, self.threads, self.channels = {}, {}, {}, {}

    def _robot(self, key):
        return catalog.get_robot(catalog.load_catalog(self.config, self.db), text(key, "robot"))

    def review(self, params):
        fields(params, ("robot", "symbol", "timeframe"), ("preset",))
        robot = self._robot(params["robot"])
        from ..presets import NATIVE_PRESET_KEYS
        if robot.key in NATIVE_PRESET_KEYS:
            raise ValueError("Perfil nativo exige pacote e Tester DEMO; PLAY não está disponível nesse percurso.")
        symbol, tf, preset = _context(params)
        account = self.read_account()
        selection = controls.selection(robot, symbol, tf, preset)
        issues = controls.play_issues(self.config, robot, symbol, tf, preset, account, self.db)
        digest = controls.context_digest(robot, symbol, tf, preset, account, selection["run"])
        review = uuid.uuid4().hex
        self.reviews[review] = dict(robot=robot, symbol=symbol, timeframe=tf, preset=preset, account=account,
            consent=controls.Consent(digest, time.monotonic()), negative=bool(controls.loss_scenario(selection["run"])))
        return dict(review=review, digest=digest, account=account, robot=robot.key, symbol=symbol, timeframe=tf,
            preset=preset, source_fingerprint=catalog.fingerprint(robot), issues=issues,
            risk=controls.risk_budget(robot, preset, account), loss_scenario=controls.loss_scenario(selection["run"]),
            expires_in_seconds=180, can_start=not issues,
            confirmations=["accept_review=true", "confirm_account=conta revisada",
                           "accept_negative=true se houver cenário negativo", "authorize_real=true se REAL"])

    def open(self, params):
        fields(params, ("mode",), ("robot", "symbol", "timeframe", "preset", "start", "end", "manifest",
            "review", "accept_review", "confirm_account", "accept_negative", "authorize_real", "visible", "anchor"))
        mode = params["mode"]
        if mode not in ("play", "replay", "backtest"):
            raise ValueError("Modo deve ser play, replay ou backtest.")
        visible = params.get("visible")
        if visible is not None:
            boolean(visible, "visible")
        if mode == "replay" and visible is False:
            raise ValueError("REPLAY é sempre visível.")
        anchor = params.get("anchor")
        if anchor is not None:
            if not isinstance(anchor, list) or len(anchor) != 2:
                raise ValueError("anchor exige [x,y] do monitor desejado.")
            anchor = tuple(number(x, "anchor", -100000, 100000, integer=True) for x in anchor)
        if sum(j.active for j in self.jobs.values()) >= 16:
            raise ValueError("Limite de16 sessões ativas deste controlador atingido.")
        actor = None
        if mode == "play":
            if set(params) - {"mode", "review", "accept_review", "confirm_account", "accept_negative", "authorize_real", "visible", "anchor"}:
                raise ValueError("PLAY usa somente a seleção da revisão, sem substituir parâmetros.")
            review = self.reviews.get(params.get("review"))
            if review is None:
                raise ValueError("Revisão do PLAY ausente; use session.review.")
            if params.get("accept_review") is not True:
                raise ValueError("Confirme explicitamente a revisão com accept_review=true.")
            account = self.read_account()
            if type(params.get("confirm_account")) is not int or params["confirm_account"] != account["login"]:
                raise ValueError("Confirmação da conta incorreta.")
            for key in ("accept_negative", "authorize_real"):
                if key in params:
                    boolean(params[key], key)
            robot = self._robot(review["robot"].key)
            symbol, tf, preset = review["symbol"], review["timeframe"], review["preset"]
            consent = review["consent"]
            selection = controls.selection(robot, symbol, tf, preset)
            digest = controls.context_digest(robot, symbol, tf, preset, account, selection["run"])
            consent.reviewed = True
            consent.confirmed = True
            consent.negative_confirmed = params.get("accept_negative", False)
            consent.real_authorized = params.get("authorize_real", False)
            consent.validate(digest, bool(controls.loss_scenario(selection["run"])), not account.get("demo", True))
            issues = controls.play_issues(self.config, robot, symbol, tf, preset, account, self.db)
            if issues:
                raise ValueError("\n".join(issues))
            # Uma autorização não abre cópias concorrentes da mesma seleção/magic.
            magic = self._magic(robot, preset)
            for job in self.jobs.values():
                if job.mode == "play" and job.active and self._magic(job.robot, job.preset) == magic:
                    raise ValueError("Magic já pertence a uma sessão PLAY deste controlador.")
            job = sessions.Session(self.db, self.config, robot, symbol, tf, preset, mode=mode,
                account=account, consent=consent, visible=visible, window_anchor=anchor, chart_control=True)
            self.reviews.pop(params["review"])
        else:
            if set(params) & {"review", "accept_review", "confirm_account", "accept_negative", "authorize_real"}:
                raise ValueError("Confirmações de conta pertencem somente a PLAY.")
            robot = self._robot(params.get("robot"))
            if "manifest" in params:
                if set(params) & {"symbol", "timeframe", "preset", "start", "end"}:
                    raise ValueError("Pacote nativo fixa seu contexto; não substituir campos.")
                from ..presets.execution import PresetExecution
                actor = PresetExecution(self.db, self.config, robot, text(params["manifest"], "manifest", 4096), mode=mode)
                job = actor.session
                job.chart_control = True
                if visible is not None:
                    job.visible = sessions.chart_visible(robot, mode, visible)
                job.window_anchor = anchor
            else:
                from ..presets import NATIVE_PRESET_KEYS
                if robot.key in NATIVE_PRESET_KEYS:
                    raise ValueError("Use pacote exportado para preservar recibo/inputs do perfil nativo.")
                symbol, tf, preset = _context(params)
                start, end = _date(params.get("start")), _date(params.get("end"))
                if start >= end:
                    raise ValueError("A janela exige start anterior a end.")
                job = sessions.Session(self.db, self.config, robot, symbol, tf, preset, mode=mode,
                    start=start, end=end, visible=visible, window_anchor=anchor, chart_control=True)
        job.state = "preparing"
        self.jobs[job.id] = job
        self.channels[job.id] = Channel(job)
        work = actor.run if actor else lambda: job.run(read_account=self.read_account if mode == "play" else None)
        thread = threading.Thread(target=work, name="mt5x-session-" + job.id)
        self.threads[job.id] = thread
        try:
            thread.start()
        except RuntimeError:
            job.cancel()
            job.state = "failed"
            job.error = "Não foi possível iniciar a thread da sessão."
            self.threads.pop(job.id, None)
            raise
        return self.status({"session": job.id})

    @staticmethod
    def _magic(robot, preset):
        from ..robots import battery
        value = battery.magic_number(robot, controls.effective_values(robot, preset))
        if not str(value).isdigit() or int(value) <= 0:
            raise ValueError("Magic efetivo não identificado para este PLAY.")
        return str(int(value))

    def job(self, key):
        if key not in self.jobs:
            raise ValueError("Sessão não pertence a este controlador.")
        return self.jobs[key]

    def status(self, params):
        fields(params, (), ("session",))
        jobs = [self.job(params["session"])] if "session" in params else list(self.jobs.values())
        rows = []
        for job in jobs:
            activity, count, profit = job.telemetry()
            rows.append(dict(session=job.id, state=job.state, mode=job.mode, robot=job.robot.key,
                symbol=job.symbol, timeframe=job.timeframe, visible=job.visible, error=job.error,
                activity=activity, positions=count, floating=profit, report=str(job.report),
                run=job.record, chart_control=self.channels[job.id].state(), monitor=vars(job.window_monitor) if job.window_monitor is not None else None))
        return rows[0] if "session" in params else dict(sessions=rows)

    def stop(self, params):
        fields(params, ("session",))
        job = self.job(params["session"])
        job.cancel()
        return dict(**self.status(params), stop_semantics="encerra somente a sessão; não liquida posições")

    def wait(self, params):
        fields(params, ("session",), ("until", "timeout"))
        job = self.job(params["session"])
        until = params.get("until", "running")
        if until not in ("running", "ready", "finished"):
            raise ValueError("until deve ser running, ready ou finished.")
        timeout = number(params.get("timeout", 30), "timeout", .1, 120)
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if (until == "running" and job.state == "running") or (until == "ready" and self.channels[job.id].state()["ready"]) or not job.active:
                return self.status({"session": job.id})
            time.sleep(.05)
        return dict(**self.status({"session": job.id}), wait_timed_out=True)

    def close(self):
        for job in self.jobs.values():
            if job.active:
                job.cancel()
        deadline = time.monotonic() + 45
        for thread in self.threads.values():
            thread.join(timeout=max(0, deadline - time.monotonic()))
        return self.status({})
