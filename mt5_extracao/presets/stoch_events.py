"""S10: reconstrói cada janela OHLC realmente registrada, sem supor seed fixo no terminal.

Valida a decisão declarada; preenchimentos continuam dependendo do recibo/livro/saldo
conferidos pelo serviço de execução. Não comprova sucesso do Move Guard nem paridade externa.
"""
import hashlib
import math
import pandas as pd
from ..strategies import get_strategy
from ..composition.schema import canonical_json
from .strategy_context import read_journal, windows



def _fail():
    raise ValueError("S10: contexto/janela/decisão ausente ou divergente da variante Python; preserve o diário e confira a primeira diferença.")


def validate(directory, params, signals):
    text=read_journal(directory)
    decisions=[];digest=hashlib.sha256();strategy=get_strategy("stoch_ema9",**params)
    by_event={};count=0
    for row,window in windows(text,_fail):
        count+=1
        frame=pd.DataFrame(window,columns=["at","open","high","low","close"])
        frame["time"]=pd.to_datetime(frame["at"],unit="s")
        expected=strategy.gerar_sinais(frame).iloc[-1]
        close=window[-1][-1];point=row["point"]
        ready=len(window)>=strategy.min_bars
        ema=float(expected.stop_long) if ready else 0
        levels={"stop_long":ema-params["stop_offset_points"]*point if ready else 0,
                "stop_short":ema+params["stop_offset_points"]*point if ready else 0,
                "target_long":close+params["target_points"]*point if ready and params["target_points"] else 0,
                "target_short":close-params["target_points"]*point if ready and params["target_points"] else 0}
        levels.update(guard_long=levels["stop_long"],guard_short=levels["stop_short"])
        tolerance=max(1e-10,abs(close)*1e-12)
        if any(not math.isfinite(value) or abs(row[key]-value)>tolerance for key,value in levels.items()):_fail()
        long=bool(expected.entry_long) and 0<levels["stop_long"]<close
        short=bool(expected.entry_short) and levels["stop_short"]>close
        if row["long"]!=long or row["short"]!=short:_fail()
        by_event[row["event_at"]]=row
        digest.update(canonical_json({"context":row,"window":window}).encode("utf-8"))
        if long or short:decisions.append({"event_at":row["event_at"],"direction":1 if long else -1,**levels})
    for signal in signals:
        row=by_event.get(signal["event_at"])
        if (row is None or not row["long" if signal["direction"]==1 else "short"] or
                signal["confirmed_at"]!=row["confirmed_at"] or signal["available_at"]<row["available_at"]):_fail()
    return {"strategy":"stoch_ema9","reference":"Python variant on each observed native closed window",
            "contexts":count,"trace_sha256":digest.hexdigest(),"decisions":decisions,
            "native_window_bars":1500,"external_parity":False,"move_guard_execution_qualified":False,"source_qualified":False}
