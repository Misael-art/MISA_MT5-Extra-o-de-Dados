"""T19.29 inicial: pacote real→Session existente→recibo/livro/saldo reconciliados.

Somente histórico Tester DEMO. Não há API de operação ao vivo ou envio Python.
"""
import calendar
import configparser
import copy
from datetime import datetime,timezone
from decimal import Decimal,ROUND_FLOOR
import hashlib
import json
import math
from pathlib import Path,PureWindowsPath
import re
import threading

from sqlalchemy import inspect,text

from ..composition.schema import canonical_json
from ..robots import mql,sessions
from ..robots import store as robot_store
from ..trade_analytics import ledger
from .compiler import read_package
from . import partial_events
from .protocol import strict_events
from .manual import _publish

_EVIDENCE_LOCK=threading.Lock()
_EVIDENCE="""CREATE TABLE IF NOT EXISTS _preset_native_runs (
 run_id TEXT PRIMARY KEY, execution_hash TEXT NOT NULL, mode TEXT NOT NULL,
 status TEXT NOT NULL,evidence_json TEXT NOT NULL,created_at TEXT NOT NULL)"""
_SIGNAL=re.compile(r"S09_SIGNAL event_at=(\d+) confirmed_at=(\d+) available_at=(\d+) direction=(-?\d+) opened=([01]) "
                   r"execution_state=(\d+) volume=([^ ]+) order=(\d+) deal=(\d+) reason=(.*)")
_SIZING=re.compile(r"S09_SIZING event_at=(\d+) lot_mode=([01]) requested_volume=([^ ]+) planned_volume=([^ ]+) "
                   r"risk_money=([^ ]+) risk_budget=([^ ]+) margin_required=([^ ]+) margin_free=([^ ]+) "
                   r"volume_min=([^ ]+) volume_max=([^ ]+) volume_step=([^\s]+)")


def native_sizing(directory,signals,expected):
    rows=[]
    for path in (Path(directory)/"Tester").glob("Agent-*/logs/*.log"):
        for match in _SIZING.finditer(mql.read_text(path)):
            at,mode,*numbers=match.groups()
            values=[None if i in (4,5) and v=="unavailable" else float(v) for i,v in enumerate(numbers)]
            if not all(v is None or math.isfinite(v) for v in values):
                raise ValueError("Evento de lote contém número inválido; preserve o diário nativo.")
            requested,planned,risk,budget,margin,free,minimum,maximum,step=values
            signal=next((s for s in signals if s["event_at"]==int(at)),None)
            if (signal is None or int(mode)!=int(expected["InpLotMode"]) or budget<=0 or risk<0 or planned<0
                    or requested!=(float(expected["InpFixedLot"]) if mode=="0" else 0)):
                raise ValueError("Evento de lote não corresponde ao sinal/input carregado; confira a origem antes de executar.")
            known=margin is not None and free is not None
            spec_known=minimum>0 and maximum>=minimum and step>0
            if signal["opened"] and (planned!=signal["filled_volume"] or planned<=0 or risk<=0 or risk>budget
                    or (mode=="0" and planned>requested) or not known or margin<0 or free<=0
                    or margin>free*.8+2e-8 or not spec_known or not minimum<=planned<=maximum):
                raise ValueError("Volume/risco/margem nativos não conciliam com o preenchimento; não aprovar este resultado.")
            if signal["opened"]:
                p,s=Decimal(str(planned)),Decimal(str(step))
                normalized=(min(Decimal(str(requested)),Decimal(str(maximum)))/s).to_integral_value(rounding=ROUND_FLOOR)*s if mode=="0" else p
                if p%s or p!=normalized:
                    raise ValueError("Volume preenchido diverge da normalização independente pelo passo/máximo nativos.")
            rows.append(dict(event_at=int(at),mode="fixed_volume" if mode=="0" else "risk_fraction",
                requested_volume=requested if mode=="0" else None,planned_volume=planned,
                filled_volume=signal["filled_volume"],risk_money=risk,risk_budget=budget,
                margin_required=margin if known else None,margin_free=free if known else None,margin_known=known,
                volume_min=minimum if spec_known else None,volume_max=maximum if spec_known else None,
                volume_step=step if spec_known else None,spec_known=spec_known))
    if len(rows)!=len(signals) or len({r["event_at"] for r in rows})!=len(rows):
        raise ValueError("Agenda de lotes ausente ou duplicada no diário nativo; preserve os eventos para revisão.")
    return rows


def validate_sizing_budgets(sizing,signals,rows,*,deposit,risk_pct):
    """Uma posição por vez: saldo antes da entrada = depósito + posições já encerradas."""
    signals_by_origin={row["event_at"]:row for row in signals}
    for item in sizing:
        if item["filled_volume"] is None:continue
        entered=datetime.fromtimestamp(signals_by_origin[item["event_at"]]["available_at"],timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        previous=sum((Decimal(str(row["pnl"])) for row in rows if row["entry_time"]<entered and row["exit_time"]<=entered),Decimal(0))
        budget=(Decimal(str(deposit))+previous)*Decimal(str(risk_pct))/100
        if abs(Decimal(str(item["risk_budget"]))-budget)>Decimal("0.00000002"):
            raise ValueError("Orçamento efetivo do Tester difere do depósito/saldo e percentual do preset; confira estado persistido e contexto antes de repetir.")


def native_signals(directory, *, marker="S09_SIGNAL"):
    if marker not in ("S09_SIGNAL", "HDK_STRATEGY_SIGNAL"):
        raise ValueError("Contrato de sinal nativo desconhecido.")
    pattern=_SIGNAL if marker=="S09_SIGNAL" else re.compile(_SIGNAL.pattern.replace("S09_SIGNAL", marker))
    paths=list((Path(directory)/"Tester").glob("Agent-*/logs/*.log"))
    result=[]
    for path in paths:
        for match in strict_events(mql.read_text(path),marker,pattern):
            event,confirmed,available,direction,opened,state,volume,order,deal,reason=match.groups()
            item=dict(event_at=int(event),confirmed_at=int(confirmed),available_at=int(available),direction=int(direction),
                      opened=opened=="1",execution_state=int(state),filled_volume=float(volume) if state=="4" else None,
                      order_ticket=int(order),deal_ticket=int(deal),reason=reason.strip())
            if (item["direction"] not in (-1,1) or not item["event_at"]<item["confirmed_at"]<=item["available_at"]
                    or item["execution_state"] not in range(7)):
                raise ValueError("Diário nativo contém sinal sem confirmação/versão reconhecida; preserve-o para revisão.")
            if (item["execution_state"]==4)!=item["opened"]:
                raise ValueError("Diário diverge entre abertura e preenchimento; preserve a resposta nativa.")
            if item["opened"] and (item["execution_state"]!=4 or not math.isfinite(item["filled_volume"]) or
                                    item["filled_volume"]<=0 or item["order_ticket"]<=0 or item["deal_ticket"]<=0):
                raise ValueError("Entrada nativa não confirma preenchimento completo; parcial/incerto exige reconciliação específica.")
            if item["execution_state"] in (3,5,6):
                raise ValueError("Resposta nativa aceita/parcial/incerta não qualifica o piloto; confira histórico antes de repetir.")
            result.append(item)
    if len({(r["event_at"],r["direction"]) for r in result})!=len(result):
        raise ValueError("Diário repete intenção; preserve a sessão e confira deduplicação.")
    return result


def native_mode(directory,session_id):
    markers=[]
    for path in (Path(directory)/"Tester").glob("Agent-*/logs/*.log"):
        markers.extend(re.findall(r"MT5X_NATIVE_MODE session="+re.escape(session_id)+r" tester=([01]) visual=([01]) demo=([01])",
                                  mql.read_text(path)))
    if len(markers)!=1 or markers[0][0]!="1" or markers[0][2]!="1":
        raise ValueError("Modo nativo não confirmado no diário próprio; confira Tester DEMO e inicialização, sem presumir pelo pedido.")
    return {"tester":True,"visual":markers[0][1]=="1","demo":True}


def _semantic_signals(signals):
    return [{key:value for key,value in row.items() if key not in ("order_ticket","deal_ticket")} for row in signals]


def first_difference(reference,rows,signals,sizing=None,partials=None,deals=None,temporal=None,channel=None,atr=None,session=None,cooldown=None):
    sections=[("ledger",reference["ledger"],rows),("signals",_semantic_signals(reference["signals"]),_semantic_signals(signals))]
    if sizing is not None:sections.append(("sizing",reference.get("sizing",[]),sizing))
    if partials is not None:sections.append(("partials",partial_events.semantic(reference.get("partials",[])),partial_events.semantic(partials)))
    if deals is not None:sections.append(("deals",partial_events.semantic(reference.get("deals",[])),partial_events.semantic(deals)))
    if temporal is not None:sections.append(("temporal_events",partial_events.semantic(reference.get("temporal_events",[])),partial_events.semantic(temporal)))
    if atr is not None:sections.append(("atr_observations",partial_events.semantic(reference.get("atr_observations",[])),partial_events.semantic(atr)))
    if session is not None:sections.append(("session_events",reference.get("session_events",[]),session))
    if cooldown is not None:sections.append(("cooldown_events",reference.get("cooldown_events",[]),cooldown))
    if channel is not None:sections.append(("channel_observations",partial_events.semantic(reference.get("channel_observations",[])),partial_events.semantic(channel)))
    for section,expected,actual in sections:
        if len(expected)!=len(actual):
            return dict(section=section,field="count",expected=len(expected),observed=len(actual))
        for index,(old,new) in enumerate(zip(expected,actual)):
            for field in sorted(set(old)|set(new)):
                if old.get(field)!=new.get(field):
                    return dict(section=section,index=index,field=field,expected=old.get(field),observed=new.get(field))
    return None


def _epoch(value):
    # Mesmo relógio nativo de barra/TimeCurrent; não presumir timezone local do host.
    return calendar.timegm(value.to_pydatetime().timetuple())


def _ledger_rows(db,record,signals, *, target_required=True):
    frame=ledger.load(db,run_id=record["run_id"])
    if len(frame)!=record["metrics"]["trades"] or not frame["completeness"].eq(ledger.COMPLETE).all():
        raise ValueError("Livro da aplicação ausente/incompleto; confira importação do CSV sem alterar o resultado financeiro.")
    filled=[s for s in signals if s["opened"]]
    if len(filled)!=len(frame):
        raise ValueError("Entradas do diário não correspondem às posições encerradas; confira sinais/fechamento final.")
    output=[]
    for row in frame.itertuples():
        numbers=[row.entry_price,row.exit_price,row.stop_price,row.target_price,row.volume,row.pnl]
        if not all(value is not None and math.isfinite(value) for value in numbers) or min(numbers[:3])<=0 or row.volume<=0 or row.target_price<0 or (target_required and row.target_price==0):
            raise ValueError("Livro sem preços, volume ou SL/TP nativos completos; não aprovar por preenchimento presumido.")
        matches=[s for s in filled if s["direction"]==row.direction and s["available_at"]==_epoch(row.entry_time)
                 and s["filled_volume"]==row.volume]
        if len(matches)!=1 or row.exit_time<row.entry_time:
            raise ValueError("Posição não corresponde a uma entrada disponível após confirmação; confira relógio/volume e diário.")
        stop_valid=row.stop_price<row.entry_price if row.direction==1 else row.stop_price>row.entry_price
        target_valid=(not target_required and row.target_price==0) or (row.target_price>row.entry_price if row.direction==1 else 0<row.target_price<row.entry_price)
        if not stop_valid or not target_valid:
            raise ValueError("SL/TP histórico não protege o preenchimento observado; revisar a divergência sem ampliar limites.")
        output.append(dict(direction=int(row.direction),entry_time=row.entry_time.strftime("%Y-%m-%d %H:%M:%S"),
             exit_time=row.exit_time.strftime("%Y-%m-%d %H:%M:%S"),entry_price=float(row.entry_price),exit_price=float(row.exit_price),
             stop_price=float(row.stop_price),target_price=float(row.target_price),volume=float(row.volume),pnl=float(row.pnl),
             exit_reason=row.exit_reason))
    observed=[[r["exit_time"],r["pnl"],r["volume"]] for r in sorted(output,key=lambda r:r["exit_time"])]
    if observed!=record["trades"]:
        raise ValueError("Livro da aplicação diverge do CSV/run; preserve ambos e confira a primeira posição divergente.")
    return output


def load_evidence(db,*,execution_hash=None):
    if not inspect(db.engine).has_table("_preset_native_runs"):
        return []
    with db.engine.connect() as conn:
        query="SELECT evidence_json FROM _preset_native_runs";params={}
        if execution_hash is not None:query+=" WHERE execution_hash=:hash";params["hash"]=execution_hash
        return [json.loads(row[0]) for row in conn.execute(text(query+" ORDER BY created_at DESC,run_id DESC"),params)]


def _save_evidence(db,evidence):
    encoded=canonical_json(evidence)
    with _EVIDENCE_LOCK,db.engine.begin() as conn:
        conn.execute(text(_EVIDENCE))
        old=conn.execute(text("SELECT evidence_json FROM _preset_native_runs WHERE run_id=:id"),
                         {"id":evidence["run_id"]}).scalar_one_or_none()
        if old is not None:
            if old!=encoded:raise ValueError("Run já tem outra evidência; preserve-a e crie uma nova sessão.")
            return
        conn.execute(text("INSERT INTO _preset_native_runs(run_id,execution_hash,mode,status,evidence_json,created_at) "
                          "VALUES(:id,:hash,:mode,:status,:evidence,:created)"),
                     {"id":evidence["run_id"],"hash":evidence["execution_hash"],"mode":evidence["mode"],
                      "status":evidence["status"],"evidence":encoded,"created":evidence["created_at"]})


class PresetExecution:
    def __init__(self,db,config,robot,manifest_path,*,mode="backtest",root=None,control=None):
        if mode not in ("backtest","replay"):
            raise ValueError("Preset inicial permite somente backtest/replay no Tester DEMO; ao vivo indisponível.")
        self.db,self.robot,self.manifest_path,self.mode=db,robot,Path(manifest_path),mode
        self.package=read_package(self.manifest_path,robot)
        context=self.package.manifest["test_context"]
        if context is None:raise ValueError("Escolha janela/modelo/capital simulado explícitos e exporte o pacote antes de executar.")
        child=configparser.ConfigParser(interpolation=None);child.read_dict({s:dict(config[s]) for s in config.sections()})
        if not child.has_section("STRATEGY"):child.add_section("STRATEGY")
        child["STRATEGY"]["initial_equity"]=str(context["deposit"])
        native=self.package.manifest["native"];identity=self.package.manifest["identity"];scope=self.package.manifest["composition"]["context"]
        # Preservar a ordem/bytes do .set exportado; não substituir o arquivo por parâmetros presumidos.
        values=mql.parse_set(self.package.set_bytes.decode("utf-16"))
        actor=copy.copy(robot);actor.presets={identity["id"]:dict(values)}
        options=dict(db=db,robot=actor,symbol=scope["symbol"],preset=identity["id"],params=dict(values),
                     model=context["model"],visual=mode=="replay",read_csv=True)
        if control is not None:options["control"]=control
        self.session=sessions.Session(db,child,actor,scope["symbol"],scope["timeframe"],preset=identity["id"],mode=mode,
             start=datetime.fromisoformat(context["start"]),end=datetime.fromisoformat(context["end"]),root=root,native_options=options,
             input_receipt_fields=native["receipt_fields"],expected_native_inputs=native["expected_native_inputs"])
        self.tester_capture=None
        self.session.native_options["run"]=self._run_command
        self.status="created";self.error="";self.evidence=None;self.started=False

    def _run_command(self,command,cwd,prefix,timeout):
        if "/config:" in command.lower():
            found=re.search(r'/config:"([^"]+)"',command,re.I)
            if found is None:raise ValueError("INI do Tester não identificado; preserve a sessão para diagnóstico.")
            source=self.session.directory/PureWindowsPath(found.group(1)).name
            ini=configparser.ConfigParser(interpolation=None);ini.read_string(mql.read_text(source))
            filename=ini.get("Tester","ExpertParameters",fallback="")
            if not re.fullmatch(r"[A-Za-z0-9_-]+\.set",filename):
                raise ValueError("INI não aponta arquivo .set próprio; não iniciar teste com inputs presumidos.")
            parameter_path=self.session.directory/"MQL5"/"Profiles"/"Tester"/filename
            context=self.package.manifest["test_context"];scope=self.package.manifest["composition"]["context"]
            if (parameter_path.read_bytes()!=self.package.set_bytes or ini.get("Tester","Model")!=context["model"]
                    or ini.get("Tester","Symbol")!=scope["symbol"] or ini.get("Tester","Period")!=scope["timeframe"]
                    or ini.getfloat("Tester","Deposit")!=context["deposit"]
                    or ini.get("Tester","FromDate")!=context["start"].replace("-",".")
                    or ini.get("Tester","ToDate")!=context["end"].replace("-",".")
                    or ini.getboolean("Tester","Visual")!=(self.mode=="replay")
                    or ini.get("Tester","UseRemote")!="0" or ini.get("Tester","UseCloud")!="0"):
                raise ValueError("INI/arquivo realmente enviado ao Tester diverge do pacote; preserve os arquivos e revise o mapeamento.")
            _publish(self.session.directory/"preset_tester.ini",source.read_bytes())
            self.tester_capture={"parameters_file":filename,"parameters_sha256":hashlib.sha256(parameter_path.read_bytes()).hexdigest(),
                                 "ini_sha256":hashlib.sha256(source.read_bytes()).hexdigest(),"matches_exported_set":True}
        return self.session._run_command(command,cwd,prefix,timeout,compiling="/compile:" in command.lower())

    def cancel(self):
        self.session.cancel()

    def run(self):
        if self.started:raise ValueError("Esta sessão já foi iniciada; reabra uma sessão própria para novo ensaio.")
        self.started=True;self.status="running";called_session=False
        try:
            if read_package(self.manifest_path,self.robot)!=self.package:
                raise ValueError("Pacote mudou desde a seleção; reabra-o antes de iniciar a sessão.")
            called_session=True
            self.session.run()
            if self.session.state!="completed":
                self.status=self.session.state;self.error=self.session.error;return self
            self.session._check_cancel()
            proof=self._proof()
            _publish(self.session.directory/"preset_evidence.json",canonical_json(proof).encode("utf-8"))
            _save_evidence(self.db,proof)
            self.evidence=proof;self.status=proof["status"]
        except sessions.Cancelled as error:
            self.status="cancelled";self.error=str(error)
        except Exception as error:
            self.status="verification_failed";self.error=str(error) or type(error).__name__
        finally:
            if not called_session:
                self.session.state="cancelled" if self.status=="cancelled" else "failed"
                self.session.error=self.error
        return self

    def _proof(self):
        job=self.session;m=job.record["metrics"];package=self.package.manifest;native=package["native"]
        reopened=robot_store.load_runs(self.db,run_id=job.record["run_id"])
        receipt=job.native_input_receipt
        if (len(reopened)!=1 or not receipt or reopened[0].get("native_inputs")!=receipt or
                receipt.get("values")!=native["expected_native_inputs"] or not receipt.get("preset_verified")
                or not receipt.get("tester") or not receipt.get("demo")
                or m.get("source_fingerprint")!=native["robot"]["fingerprint"] or not m.get("native_profile_isolated")
                or m.get("native_detail_capture")!="observer" or m.get("cash_reconciled") is not True
                or m.get("entries_without_stop")!=0):
            raise ValueError("Conferência nativa incompleta: inputs/fonte/DEMO/CSV/SL/saldo não correspondem ao pacote; preserve a sessão.")
        if not self.tester_capture or not self.tester_capture["matches_exported_set"]:
            raise ValueError("Não há prova do .set/INI realmente carregado; confira o comando próprio do Tester.")
        actual_mode=native_mode(job.directory,job.id)
        if actual_mode["visual"]!=(self.mode=="replay"):
            raise ValueError("Modo Visual nativo difere do solicitado; replay continua pendente.")
        stoch=native["profile_id"]=="hadouken.stoch_ema9"
        vwap=native["profile_id"]=="hadouken.vwap_bands"
        madrid=native["profile_id"]=="hadouken.madrid"
        signals=native_signals(job.directory,marker="HDK_STRATEGY_SIGNAL" if stoch or vwap or madrid else "S09_SIGNAL")
        sizing=native_sizing(job.directory,signals,native["expected_native_inputs"]) if native["profile_version"]>=2 else None
        rows=_ledger_rows(self.db,job.record,signals,target_required=not stoch or package["composition"]["strategy"]["params"]["target_points"]>0)
        if stoch and package["composition"]["strategy"]["params"]["target_points"]==0 and any(row["target_price"]!=0 for row in rows):
            raise ValueError("S10: alvo desabilitado no preset, mas o livro informa TP; preserve a divergência.")
        if sizing is not None:
            validate_sizing_budgets(sizing,signals,rows,deposit=package["test_context"]["deposit"],
                                   risk_pct=native["expected_native_inputs"]["InpRiskPct"])
        partials=deals=None
        if native["profile_version"]>=3:
            partials,deals=partial_events.read_events(job.directory,package["composition"]["partial_exit_plan"],signals,sizing,rows)
        strategy_reference=None
        if native["profile_id"]=="hadouken.pullback":
            from .pullback_events import validate
            strategy_reference=validate(job.directory,package["composition"]["strategy"]["params"],signals,rows)
        if stoch:
            from .stoch_events import validate
            strategy_reference=validate(job.directory,package["composition"]["strategy"]["params"],signals)
        if vwap:
            from .vwap_events import validate
            strategy_reference=validate(job.directory,package["composition"]["strategy"]["params"],signals)
        if madrid:
            from .madrid_events import validate
            strategy_reference=validate(job.directory,package["composition"]["strategy"]["params"],signals)
        temporal=None;channel=[] if native["profile_version"]>=6 else None;atr=[] if native["profile_version"]>=7 else None
        if native["profile_version"]>=4:
            from .temporal_events import read_events
            temporal=read_events(job.directory,package["composition"]["conditional_policy"],signals,sizing,deals,channel_observations=channel,atr_observations=atr)
        session=None;cooldown=None
        if native["profile_id"]=="hadouken.s09_reference" and native["profile_version"]>=9:
            from .cooldown_events import read_events as read_cooldown_events
            cooldown=read_cooldown_events(job.directory,package["composition"]["filters"],signals,deals)
        if native["profile_id"]=="hadouken.s09_reference" and native["profile_version"]>=8:
            from .session_events import read_events as read_session_events
            session=read_session_events(job.directory,package["composition"]["filters"],signals,deals,{r["event_at"] for r in (cooldown or []) if not r["allowed"]})
        trades=m.get("trades");active=type(trades) is int and trades>0 and len(rows)==trades
        if active and m.get("entries")!=trades:
            raise ValueError("Entradas e posições encerradas não conciliam; conferir última posição e respostas antes de aceitar.")
        status=("reconciled_"+self.mode) if active else "contract_only"
        prior=next((p for p in load_evidence(self.db,execution_hash=self.package.execution_hash)
                    if p["mode"]=="backtest" and p["status"]=="reconciled_backtest"),None) if self.mode=="replay" else None
        difference=first_difference(prior,rows,signals,sizing,partials,deals,temporal,channel,atr,session,cooldown) if prior is not None else None
        if (stoch or vwap or madrid) and prior is not None and difference is None and prior.get("strategy_reference")!=strategy_reference:
            difference=dict(section="strategy_reference",field="observed_context",expected=prior.get("strategy_reference"),observed=strategy_reference)
        match=None if prior is None else difference is None
        if match is False:status="native_divergence"
        report=getattr(job,"report",job.directory/"relatorio.json")
        resource_report=json.loads(Path(report).read_text(encoding="utf-8")) if Path(report).is_file() else {}
        proof=dict(resources_released=resource_report.get("resources_released"),preset_native_evidence_schema_version=1,run_id=job.record["run_id"],session=job.id,
            preset=package["identity"],config_hash=package["config_hash"],execution_hash=self.package.execution_hash,
            package_hash=self.package.package_hash,set_sha256=package["parameters_sha256"],source=native["robot"],
            context=package["composition"]["context"],test_context=package["test_context"],mode=self.mode,status=status,
            inputs=receipt,tester_capture=self.tester_capture,actual_mode=actual_mode,signals=signals,ledger=rows,trades=trades,cash_reconciled=True,
            net_profit=m.get("net_profit"),native_cash_result=m.get("native_cash_result"),currency=m.get("currency"),
            tester_model=m.get("tester_model"),data_quality=m.get("data_quality"),
            clock_basis="native_server",intrabar_basis="native_MT5_ticks; stop_first is Python OHLC policy",
            reference_backtest_run_id=prior["run_id"] if prior else None,reference_match=match,first_difference=difference,
            functional_native_activity=active,functional_cycle=(active and self.mode=="replay" and match is True),
            allows_live=False,statistical_approval=False,cache_eligible=False,history_revision="unavailable",
            created_at=datetime.now(timezone.utc).isoformat())
        if sizing is not None:proof["sizing"]=sizing
        if session is not None:
            session_activity=any(not row["allowed"] for row in session if row["event"]=="check")
            proof.update(session_events=session,session_enabled=any(r["mode"]=="session_v1" for r in package["composition"]["filters"]),
                         session_acceptance=active and match is True and session_activity)
        if cooldown is not None:
            denied=[r for r in cooldown if not r["allowed"]]
            later=any(r["allowed"] and r["last_close_msc"]>0 and any(d["at_msc"]<r["at_msc"] for d in denied) and
                      any(s["event_at"]==r["event_at"] and s["opened"] for s in signals) for r in cooldown)
            proof.update(cooldown_events=cooldown,cooldown_enabled=any(r["mode"]=="cooldown_v1" for r in package["composition"]["filters"]),
                         cooldown_acceptance=active and match is True and bool(denied) and later)
        if partials is not None:proof.update(partials=partials,deals=deals,partial_activity=any(r["state"]=="confirmed" for r in partials))
        if temporal is not None:
            proof.update(temporal_events=temporal,temporal_activity=any(r['state']=='confirmed' for r in temporal),
                         temporal_enabled=package['composition']['conditional_policy']['mode']=='temporal_v1',
                         temporal_acceptance=active and match is True and any(r['state']=='confirmed' for r in temporal))
        if native["profile_version"]>=5:
            er_activity=any(r['state']=='confirmed' and r['reason']=='time_er' for r in (temporal or []))
            proof.update(efficiency_activity=er_activity,efficiency_enabled=package['composition']['conditional_policy']['mode']=='temporal_er_v1',
                         efficiency_acceptance=active and match is True and er_activity)
        if atr is not None:
            atr_activity=any(r['state']=='confirmed' and r['reason']=='time_atr' for r in (temporal or []))
            proof.update(atr_observations=atr,atr_activity=atr_activity,
                atr_enabled=package['composition']['conditional_policy']['mode']=='temporal_atr_v1',
                atr_acceptance=active and match is True and atr_activity)
        if channel is not None:
            channel_activity=any(r['state']=='confirmed' and r['reason']=='time_donchian' for r in (temporal or []))
            proof.update(channel_observations=channel,channel_activity=channel_activity,
                channel_enabled=package['composition']['conditional_policy']['mode']=='temporal_donchian_v1',
                channel_acceptance=active and match is True and channel_activity)
        if strategy_reference is not None:proof["strategy_reference"]=strategy_reference
        return proof
