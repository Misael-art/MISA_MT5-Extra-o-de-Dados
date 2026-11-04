"""Controle de encerramento de uma sessão, sem interface gráfica: pausar, retomar e liquidar só a exposição própria.

A App escreve comandos numa fila em Common (`mt5x_session_<id>_control.txt`, linhas `seq;comando;não_antes`);
o wrapper da sessão, dentro do próprio Expert, executa e responde em `..._control_ack.txt`. Nada passa pela
interface do terminal.

- PAUSE: recusa toda ordem nova de abertura (mercado sem `position` e pendentes) pela mesma chamada OrderSend
  do Expert; ajuste de SL/TP, remoção e fechamento seguem permitidos, então a gestão continua.
- RESUME: volta a aceitar aberturas.
- FLATTEN: pausa, remove pendentes e fecha por ticket apenas a exposição desta sessão; repete até ficar sem
  exposição própria ou esgotar as tentativas.
- STATUS: só publica a exposição.

Posse: magic e símbolo da sessão e abertura (POSITION_TIME/ORDER_TIME_SETUP) a partir do início da sessão,
gravado na primeira inicialização e relido em reinícios. Mesmo magic/símbolo de antes do início é
`ambiguous`: nunca é tocado e impede `flat`. Tudo o que é de outro magic ou símbolo é `foreign`: só contado.
"""
import os
from pathlib import Path

COMMANDS = ("PAUSE", "RESUME", "FLATTEN", "STATUS")
ACK_FIELDS = ("seq", "command", "state", "own_positions", "own_orders", "ambiguous_positions",
              "ambiguous_orders", "foreign_positions", "foreign_orders", "attempts", "actions")


def filenames(session_id):
    base = f"mt5x_session_{session_id}"
    return dict(control=f"{base}_control.txt", ack=f"{base}_control_ack.txt", start=f"{base}_control_start.txt")


def write_commands(common, session_id, commands):
    """Grava a fila inteira de forma atômica. `commands`: [(comando, não_antes_epoch_servidor)], 0 = já."""
    lines = []
    for seq, (command, not_before) in enumerate(commands, 1):
        if command not in COMMANDS:
            raise ValueError(f"Comando de sessão desconhecido: {command}")
        lines.append(f"{seq};{command};{int(not_before)}")
    path = Path(common) / filenames(session_id)["control"]
    temp = path.with_suffix(".tmp")
    temp.write_text("\n".join(lines) + "\n", encoding="ascii")
    os.replace(temp, path)
    return path


def parse_ack(text):
    """Respostas do wrapper, na ordem. Linhas incompletas são erro, não estado vazio."""
    rows = []
    for line in text.splitlines():
        if not line.strip():
            continue
        parts = line.strip().split(";", len(ACK_FIELDS) - 1)
        if len(parts) != len(ACK_FIELDS):
            raise ValueError(f"Resposta de controle incompleta: {line!r}")
        row = dict(zip(ACK_FIELDS, parts))
        for key in ACK_FIELDS[3:10]:
            row[key] = int(row[key])
        row["seq"] = int(row["seq"])
        row["actions"] = [dict(zip(("kind", "ticket", "retcode"), a.split(":"))) for a in row["actions"].split(",") if a]
        rows.append(row)
    return rows


def read_ack(common, session_id):
    path = Path(common) / filenames(session_id)["ack"]
    return parse_ack(path.read_text(encoding="ascii", errors="strict")) if path.is_file() else []


def closure(rows):
    """Veredito do encerramento pela última resposta de FLATTEN; `closed` só sem exposição própria nem ambígua."""
    flatten = [r for r in rows if r["command"] == "FLATTEN"]
    if not flatten:
        return dict(closed=False, reason="sem resposta de FLATTEN")
    last = flatten[-1]
    remaining = last["own_positions"] + last["own_orders"]
    ambiguous = last["ambiguous_positions"] + last["ambiguous_orders"]
    closed = last["state"] == "flat" and remaining == 0 and ambiguous == 0
    reason = "" if closed else ("posse ambígua: mesmo magic/símbolo anterior à sessão" if ambiguous else
                                f"exposição própria restante: {remaining}" if remaining else f"estado {last['state']}")
    return dict(closed=closed, reason=reason, last=last,
                preserved_foreign=dict(positions=last["foreign_positions"], orders=last["foreign_orders"]))


def prelude():
    """MQL antes do include do Expert: estado e portão de abertura que redefine OrderSend."""
    return '''
int MT5X_CtlState=0;            // 0 livre, 1 pausado, 2 liquidando
long MT5X_CtlSeq=0, MT5X_CtlFlattenSeq=0;
int MT5X_CtlAttempts=0;
datetime MT5X_CtlStart=0;
ulong MT5X_CtlLastPoll=0;
string MT5X_CtlQueue[];
bool MT5X_CtlLoaded=false;
string MT5X_CtlActions="";
bool MT5X_CtlInternal=false;

bool MT5X_CtlIsOpening(const MqlTradeRequest &r) {
  if(r.action==TRADE_ACTION_PENDING) return true;
  if(r.action!=TRADE_ACTION_DEAL || r.position!=0) return false;
  if(AccountInfoInteger(ACCOUNT_MARGIN_MODE)!=ACCOUNT_MARGIN_MODE_RETAIL_HEDGING && PositionSelect(r.symbol)) {
    long type=PositionGetInteger(POSITION_TYPE);
    bool reduces=(type==POSITION_TYPE_BUY && r.type==ORDER_TYPE_SELL) || (type==POSITION_TYPE_SELL && r.type==ORDER_TYPE_BUY);
    if(reduces && r.volume<=PositionGetDouble(POSITION_VOLUME)+1e-9) return false;   // redução em netting
  }
  return true;
}

bool MT5X_OrderSend(const MqlTradeRequest &request,MqlTradeResult &result) {
  if(!MT5X_CtlInternal && MT5X_CtlState!=0 && MT5X_CtlIsOpening(request)) {
    ZeroMemory(result); result.retcode=TRADE_RETCODE_TRADE_DISABLED; result.comment="MT5X sessao pausada";
    PrintFormat("MT5X_CONTROL refused_opening state=%d symbol=%s action=%d",MT5X_CtlState,request.symbol,(int)request.action);
    return false;
  }
  return OrderSend(request,result);
}

// Recusa na checagem: o executor do HADOUKEN consulta OrderCheck antes de reservar a intenção, então a
// pausa não deixa pedido vinculado "sem confirmação final". O portão de OrderSend fica como barreira.
bool MT5X_OrderCheck(const MqlTradeRequest &request,MqlTradeCheckResult &result) {
  if(!MT5X_CtlInternal && MT5X_CtlState!=0 && MT5X_CtlIsOpening(request)) {
    ZeroMemory(result); result.retcode=TRADE_RETCODE_TRADE_DISABLED; result.comment="MT5X sessao pausada";
    PrintFormat("MT5X_CONTROL refused_check state=%d symbol=%s action=%d",MT5X_CtlState,request.symbol,(int)request.action);
    return false;
  }
  return OrderCheck(request,result);
}

bool MT5X_OrderSendAsync(const MqlTradeRequest &request,MqlTradeResult &result) {
  if(!MT5X_CtlInternal && MT5X_CtlState!=0 && MT5X_CtlIsOpening(request)) {
    ZeroMemory(result); result.retcode=TRADE_RETCODE_TRADE_DISABLED; result.comment="MT5X sessao pausada";
    return false;
  }
  return OrderSendAsync(request,result);
}

#define OrderSend MT5X_OrderSend
#define OrderSendAsync MT5X_OrderSendAsync
#define OrderCheck MT5X_OrderCheck
'''


def source(session_id, magic_expr, *, tester_queue=False):
    """MQL depois do include (o magic pode ser input do Expert): posse, liquidação, resposta e fila."""
    names = filenames(session_id)
    return f'''
void MT5X_CtlInit() {{
  int f=FileOpen("{names["start"]}",FILE_READ|FILE_TXT|FILE_ANSI|FILE_COMMON|FILE_SHARE_READ);
  if(f!=INVALID_HANDLE) {{ MT5X_CtlStart=(datetime)StringToInteger(FileReadString(f)); FileClose(f); }}
  if(MT5X_CtlStart<=0) {{
    MT5X_CtlStart=TimeCurrent();
    f=FileOpen("{names["start"]}",FILE_WRITE|FILE_TXT|FILE_ANSI|FILE_COMMON);
    if(f!=INVALID_HANDLE) {{ FileWriteString(f,IntegerToString((long)MT5X_CtlStart)); FileClose(f); }}
  }}
}}

// Classe da exposição: 1 própria, 2 ambígua (mesmo magic/símbolo de antes do início), 0 alheia.
int MT5X_CtlOwner(const long magic,const string symbol,const datetime opened) {{
  if(magic!={magic_expr} || symbol!=_Symbol) return 0;
  return opened>=MT5X_CtlStart ? 1 : 2;
}}

ENUM_ORDER_TYPE_FILLING MT5X_CtlFilling() {{
  long modes=SymbolInfoInteger(_Symbol,SYMBOL_FILLING_MODE);
  if((modes&SYMBOL_FILLING_FOK)!=0) return ORDER_FILLING_FOK;
  if((modes&SYMBOL_FILLING_IOC)!=0) return ORDER_FILLING_IOC;
  return ORDER_FILLING_RETURN;
}}

void MT5X_CtlAct(const string kind,const ulong ticket,const uint retcode) {{
  if(MT5X_CtlActions!="") MT5X_CtlActions+=",";
  MT5X_CtlActions+=kind+":"+IntegerToString((long)ticket)+":"+IntegerToString((long)retcode);
}}

void MT5X_CtlLiquidate() {{
  MT5X_CtlInternal=true;
  for(int i=OrdersTotal()-1;i>=0;i--) {{
    ulong ticket=OrderGetTicket(i);
    if(ticket==0 || MT5X_CtlOwner(OrderGetInteger(ORDER_MAGIC),OrderGetString(ORDER_SYMBOL),(datetime)OrderGetInteger(ORDER_TIME_SETUP))!=1) continue;
    MqlTradeRequest r; MqlTradeResult res; ZeroMemory(r); ZeroMemory(res);
    r.action=TRADE_ACTION_REMOVE; r.order=ticket;
    OrderSend(r,res); MT5X_CtlAct("remove",ticket,res.retcode);
  }}
  for(int i=PositionsTotal()-1;i>=0;i--) {{
    ulong ticket=PositionGetTicket(i);
    if(ticket==0 || MT5X_CtlOwner(PositionGetInteger(POSITION_MAGIC),PositionGetString(POSITION_SYMBOL),(datetime)PositionGetInteger(POSITION_TIME))!=1) continue;
    bool buy=PositionGetInteger(POSITION_TYPE)==POSITION_TYPE_BUY;
    MqlTradeRequest r; MqlTradeResult res; ZeroMemory(r); ZeroMemory(res);
    r.action=TRADE_ACTION_DEAL; r.position=ticket; r.symbol=_Symbol; r.volume=PositionGetDouble(POSITION_VOLUME);
    r.type=buy ? ORDER_TYPE_SELL : ORDER_TYPE_BUY; r.price=buy ? SymbolInfoDouble(_Symbol,SYMBOL_BID) : SymbolInfoDouble(_Symbol,SYMBOL_ASK);
    r.deviation=50; r.magic={magic_expr}; r.type_filling=MT5X_CtlFilling(); r.comment="MT5X flatten";
    OrderSend(r,res); MT5X_CtlAct("close",ticket,res.retcode);
  }}
  MT5X_CtlInternal=false;
}}

void MT5X_CtlAck(const long seq,const string command) {{
  int counts[6]={{0,0,0,0,0,0}};   // própria, ambígua, alheia × posições, ordens
  for(int i=PositionsTotal()-1;i>=0;i--) {{
    if(PositionGetTicket(i)==0) continue;
    int owner=MT5X_CtlOwner(PositionGetInteger(POSITION_MAGIC),PositionGetString(POSITION_SYMBOL),(datetime)PositionGetInteger(POSITION_TIME));
    counts[owner==1 ? 0 : owner==2 ? 2 : 4]++;
  }}
  for(int i=OrdersTotal()-1;i>=0;i--) {{
    if(OrderGetTicket(i)==0) continue;
    int owner=MT5X_CtlOwner(OrderGetInteger(ORDER_MAGIC),OrderGetString(ORDER_SYMBOL),(datetime)OrderGetInteger(ORDER_TIME_SETUP));
    counts[owner==1 ? 1 : owner==2 ? 3 : 5]++;
  }}
  string state=MT5X_CtlState==0 ? "running" : MT5X_CtlState==1 ? "paused" :
               (counts[0]+counts[1]>0 ? (MT5X_CtlAttempts>=5 ? "exposed" : "liquidating") :
                counts[2]+counts[3]>0 ? "ambiguous" : "flat");
  int f=FileOpen("{names["ack"]}",FILE_READ|FILE_WRITE|FILE_TXT|FILE_ANSI|FILE_COMMON|FILE_SHARE_READ);
  if(f==INVALID_HANDLE) return;
  FileSeek(f,0,SEEK_END);
  FileWriteString(f,StringFormat("%I64d;%s;%s;%d;%d;%d;%d;%d;%d;%d;%s\\n",seq,command,state,counts[0],counts[1],counts[2],
                  counts[3],counts[4],counts[5],MT5X_CtlAttempts,MT5X_CtlActions));
  FileClose(f);
  PrintFormat("MT5X_CONTROL seq=%I64d command=%s state=%s own=%d/%d ambiguous=%d/%d foreign=%d/%d attempts=%d actions=%s",
              seq,command,state,counts[0],counts[1],counts[2],counts[3],counts[4],counts[5],MT5X_CtlAttempts,MT5X_CtlActions);
  MT5X_CtlActions="";
}}

void MT5X_CtlLoad() {{
  if(MT5X_CtlLoaded && {"true" if tester_queue else "false"}) return;
  int f=FileOpen("{names["control"]}",FILE_READ|FILE_TXT|FILE_ANSI|FILE_COMMON|FILE_SHARE_READ|FILE_SHARE_WRITE);
  if(f==INVALID_HANDLE) return;
  ArrayResize(MT5X_CtlQueue,0);
  while(!FileIsEnding(f)) {{ string line=FileReadString(f); if(line!="") {{ int n=ArraySize(MT5X_CtlQueue); ArrayResize(MT5X_CtlQueue,n+1); MT5X_CtlQueue[n]=line; }} }}
  FileClose(f); MT5X_CtlLoaded=true;
}}

void MT5X_CtlPoll() {{
  if(!MQLInfoInteger(MQL_TESTER)) {{ ulong now=GetTickCount64(); if(now-MT5X_CtlLastPoll<1000 && MT5X_CtlState!=2) return; MT5X_CtlLastPoll=now; }}
  MT5X_CtlLoad();
  for(int i=0;i<ArraySize(MT5X_CtlQueue);i++) {{
    string parts[]; if(StringSplit(MT5X_CtlQueue[i],';',parts)!=3) continue;
    long seq=StringToInteger(parts[0]);
    if(seq<=MT5X_CtlSeq || TimeCurrent()<(datetime)StringToInteger(parts[2])) continue;
    MT5X_CtlSeq=seq;
    string command=parts[1];
    if(command=="PAUSE") MT5X_CtlState=1;
    else if(command=="RESUME") MT5X_CtlState=0;
    else if(command=="FLATTEN") {{ MT5X_CtlState=2; MT5X_CtlFlattenSeq=seq; MT5X_CtlAttempts=0; continue; }}
    MT5X_CtlAck(seq,command);
  }}
  if(MT5X_CtlState==2 && MT5X_CtlAttempts<5 && MT5X_CtlFlattenSeq>0) {{
    MT5X_CtlAttempts++;
    MT5X_CtlLiquidate();
    MT5X_CtlAck(MT5X_CtlFlattenSeq,"FLATTEN");
    // Uma vez sem exposição própria, para de tentar; a fila segue podendo pedir STATUS.
    bool own=false;
    for(int i=PositionsTotal()-1;i>=0 && !own;i--) if(PositionGetTicket(i)>0 && MT5X_CtlOwner(PositionGetInteger(POSITION_MAGIC),PositionGetString(POSITION_SYMBOL),(datetime)PositionGetInteger(POSITION_TIME))==1) own=true;
    for(int i=OrdersTotal()-1;i>=0 && !own;i--) if(OrderGetTicket(i)>0 && MT5X_CtlOwner(OrderGetInteger(ORDER_MAGIC),OrderGetString(ORDER_SYMBOL),(datetime)OrderGetInteger(ORDER_TIME_SETUP))==1) own=true;
    if(!own) MT5X_CtlFlattenSeq=0;
  }}
}}
'''


class Channel:
    """Fila de comandos de uma sessão ao vivo: cada comando é acrescentado e vale já (não_antes 0)."""
    def __init__(self, common, session_id):
        self.common, self.session_id, self.commands = Path(common), session_id, []

    def send(self, command):
        self.commands.append((command, 0))
        write_commands(self.common, self.session_id, self.commands)
        return len(self.commands)

    def wait(self, seq, timeout, *, poll=.25, done=None, clock=None, sleep=None):
        """Última resposta de `seq`; `done(row)` decide quando parar. None se o Expert não respondeu no prazo."""
        import time
        clock, sleep = clock or time.monotonic, sleep or time.sleep
        end, row = clock() + timeout, None
        while True:
            rows = [r for r in read_ack(self.common, self.session_id) if r["seq"] == seq]
            row = rows[-1] if rows else row
            if row is not None and (done is None or done(row)):
                return row
            if clock() >= end:
                return row
            sleep(poll)

    def liquidate(self, timeout=60, **kwargs):
        """PAUSE e FLATTEN; devolve o veredito de `closure` sobre as respostas reais do Expert."""
        self.wait(self.send("PAUSE"), min(timeout, 15), **kwargs)
        seq = self.send("FLATTEN")
        self.wait(seq, timeout, done=lambda r: r["state"] in ("flat", "ambiguous", "exposed"), **kwargs)
        verdict = closure([r for r in read_ack(self.common, self.session_id) if r["seq"] == seq])
        verdict["responded"] = "last" in verdict
        return verdict
