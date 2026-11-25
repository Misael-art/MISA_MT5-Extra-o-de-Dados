"""Relatório offline navegável da carteira. Lê curvas confirmadas sem reexecutar pesquisa."""
from html import escape
import json
from pathlib import Path

from sqlalchemy import inspect, text

from . import ultra
from .ultra_evaluation import metrics


def list_receipts(db, offset=0, limit=50):
    if type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError("Página de recibos inválida.")
    with db.engine.connect() as conn:
        if not inspect(conn).has_table("_ultra_replays"):
            return []
        rows = conn.execute(text("SELECT receipt,status FROM _ultra_replays ORDER BY receipt LIMIT :n OFFSET :start"),
                            {"n": limit, "start": offset}).mappings()
        return [dict(row) for row in rows]


def explain(db, receipt, ordinal):
    """Consulta pelo índice do cursor, sem materializar a linha do tempo completa."""
    if type(ordinal) is not int or ordinal < 0:
        raise ValueError("O índice do tick deve ser inteiro a partir de zero.")
    summary = ultra.inspect_receipt(db, receipt)
    with db.engine.connect() as conn:
        row = conn.execute(text("SELECT events,mark FROM _ultra_frames WHERE receipt=:r AND ordinal=:i"),
                           {"r": receipt, "i": ordinal}).first()
    if row is None:
        raise ValueError("Tick ausente no recibo.")
    events = json.loads(row[0])
    mark = json.loads(row[1])
    policy = summary["policy"]
    return {"receipt": receipt, "ordinal": ordinal, "snapshot": summary["manifest_id"], "key": mark["key"],
            "mark": mark, "events": events, "version": policy["revision"],
            "rules": {m["owner"]: m for m in policy["modules"]},
            "risk_limit": policy["risk_limit"], "fill_steps_per_tick": policy["fill_steps_per_tick"],
            "source": "research_replay"}


def export(db, receipt, destination, *, reference=None):
    data = ultra.inspect_receipt(db, receipt, timeline=True)
    if data["status"] != "complete":
        raise ValueError("Selecione um recibo concluído.")
    marks = None
    if reference:
        other = ultra.inspect_receipt(db, reference, timeline=True)
        if (other["status"] != "complete" or data["manifest_id"] != other["manifest_id"]
                or any(data["policy"][k] != other["policy"][k] for k in ultra.ACCOUNT_FIELDS + ("currency",))):
            raise ValueError("Comparação exige o mesmo snapshot, caixa, liquidez, risco, moeda e custos.")
        marks = [f["mark"] for f in other["timeline"]]
        if [m["key"] for m in marks] != [f["mark"]["key"] for f in data["timeline"]]:
            raise ValueError("Relógios divergentes nos recibos.")
    result = metrics(data)
    model = {"frames": data["timeline"], "reference": marks, "initial": result["initial_cash"],
             "currency": data["policy"]["currency"], "limits": {m["owner"]: m["max_losses"] for m in data["policy"]["modules"]},
             "owners": [m["owner"] for m in data["policy"]["modules"]],
             "rules": {m["owner"]: m for m in data["policy"]["modules"]}, "risk_limit": data["policy"]["risk_limit"]}
    def money(number):
        return escape(f'{number / 100:,.2f} {model["currency"]}')
    page = (TEMPLATE.replace("__FINAL__", money(result["end_equity"]))
            .replace("__DD__", money(result["max_drawdown"])).replace("__CYCLES__", str(result["completed_cycles"]))
            .replace("__SAMPLE__", "Amostra sintética" if data["sample_kind"] == "synthetic" else "Histórico de mercado")
            .replace("__RECEIPT__", escape(receipt))
            .replace("__DATA__", json.dumps(model, ensure_ascii=False, allow_nan=False, separators=(",", ":")).replace("<", "\\u003c")))
    with Path(destination).open("x", encoding="utf-8") as out:
        out.write(page)
    return {"status": "exported", "receipt": receipt, "file": str(destination), "ticks": len(data["timeline"])}


TEMPLATE = """<!doctype html><html lang="pt-BR"><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>Carteira Ultra · Laboratório MT5X</title>
<style>
*{box-sizing:border-box}body{margin:0;background:#f5f6f3;color:#192c30;font:16px/1.45 system-ui,sans-serif}
main,header>div{max-width:1080px;margin:auto;padding:20px clamp(14px,4vw,44px)}header{background:#16383c;color:#fff}
header small{color:#c0d4ce}h1{margin:4px 0 0;font-size:clamp(24px,4vw,38px)}h2{font-size:20px;margin:0 0 12px}
.cards,.charts{display:grid;gap:12px}.cards{grid-template-columns:repeat(auto-fit,minmax(130px,1fr))}
section,article{background:#fff;border:1px solid #ccd5d1;border-radius:6px;padding:14px;margin-bottom:14px}
article strong{display:block;font-size:22px}label{display:inline-block;margin:4px 10px 4px 0}
select,input,button{font:inherit}select{max-width:100%}.cycle{display:flex;align-items:center;gap:8px;margin-right:0;min-width:0}.cycle select{min-width:0}button,select{padding:5px 9px}figure.tracks{margin:12px 0}#lanes{height:auto}.trackLegend{font-size:.85em;color:#475e61}
input[type=range]{width:min(500px,100%);accent-color:#007566}
canvas{display:block;width:100%;height:145px;touch-action:pan-y}canvas:focus-visible,button:focus-visible{outline:3px solid #a55008}
.charts{grid-template-columns:repeat(auto-fit,minmax(min(100%,300px),1fr))}figure{margin:0;min-width:0}
figcaption{font-weight:650}small,.muted{color:#475e61}code{overflow-wrap:anywhere}output{font-variant-numeric:tabular-nums}
#events{padding-left:1.3em}li{padding:.25em}details{margin:.4em 0}pre{white-space:pre-wrap;overflow-wrap:anywhere}
</style><header><div><small>MT5X / Laboratório</small><h1>Carteira Ultra</h1>__SAMPLE__ · Replay Python</div></header>
<main><div class="cards"><article>Patrimônio final<strong>__FINAL__</strong></article><article>Maior drawdown<strong>__DD__</strong></article>
<article>Ciclos encerrados<strong>__CYCLES__</strong></article></div>
<section><h2>Linha do tempo</h2><label>Métrica <select id="metric"><option value="equity">Patrimônio</option>
<option value="drawdown">Drawdown</option><option value="margin">Margem reservada</option><option value="cost">Comissões</option></select></label>
<span id="legend" class="muted"></span><div class="charts"><figure><figcaption>Preço bid em ticks</figcaption>
<canvas id="price" tabindex="0" aria-label="Preço. Use as setas para percorrer os ticks"></canvas></figure>
<figure><figcaption id="metricTitle"></figcaption><canvas id="curve" tabindex="0" aria-label="Curva financeira sincronizada"></canvas></figure></div>
<figure class="tracks"><figcaption>Ciclos por estratégia</figcaption><canvas id="lanes" tabindex="0"
 aria-label="Ciclos simultâneos. Use as setas ou toque para consultar o livro"></canvas>
 <div class="trackLegend">Cinza · pendente　 Verde · posição　 ◆ veto ou invalidação　 ● mudança de gestão</div>
 </figure><label for="cursor">Tick</label><input type="range" id="cursor" min="0" step="1"><output id="time"></output></section>
<section><h2>Decisões neste instante</h2><label>Estratégia <select id="owner"><option value="">Todas</option></select></label>
<label class="cycle">Ciclo <select id="cycle"><option value="">Todos</option></select></label>
<button id="prev">Decisão anterior</button> <button id="next">Próxima decisão</button><p id="account"></p>
<ul id="events" aria-live="polite"></ul></section><details><summary>Identidade do recibo</summary><code>__RECEIPT__</code></details></main>
<script id="frozen" type="application/json">__DATA__</script><script>
const data=JSON.parse(document.getElementById('frozen').textContent), frames=data.frames, el=id=>document.getElementById(id);
const fmt=new Intl.NumberFormat('pt-BR',{minimumFractionDigits:2,maximumFractionDigits:2});
const money=v=>fmt.format(v/100)+' '+data.currency, utc=ms=>new Date(ms).toISOString().replace('T',' ').replace('Z',' UTC');
const reasons={local_loss_streak:'Limite local de perdas consecutivas',shared_margin:'Margem compartilhada insuficiente',
 shared_risk:'Orçamento de risco compartilhado insuficiente',closed_bar:'Sinal da barra fechada',reserved:'Ordem com reserva',
 breakeven:'Stop movido ao preço da ordem',owner_cycle_open:'Já há ciclo deste dono',reference_lost:'Perda da referência do setup',
 partial:'Fill parcial',complete:'Fill completo',stop:'Saída por stop',target:'Saída por alvo',local_daily_goal:'Meta diária local',
 pending_expired:'Ordem expirada',utc_day_reset:'Reinício das cotas da sessão UTC',signal_ttl:'Sinal expirado'};
for(const owner of data.owners)el('owner').add(new Option(owner,owner));
const marks=frames.map(f=>f.mark), first=marks[0].key[0], last=marks[marks.length-1].key[0];
function addDD(rows){let peak=data.initial;for(const m of rows){peak=Math.max(peak,m.equity);m.drawdown=peak-m.equity}}
addDD(marks);if(data.reference)addDD(data.reference);
el('legend').textContent=data.reference?'Verde: carteira · Ocre: referência':'Carteira em verde';
el('cursor').max=frames.length-1;
const initial=frames.findIndex(f=>f.events.some(e=>e.reason==='local_loss_streak'));
el('cursor').value=initial<0?0:initial;
function chart(canvas,field,compare){
 const w=canvas.clientWidth,h=canvas.clientHeight,ratio=window.devicePixelRatio||1;
 canvas.width=w*ratio;canvas.height=h*ratio;const c=canvas.getContext('2d');c.scale(ratio,ratio);
 const curves=compare&&data.reference?[marks,data.reference]:[marks];let lo=Infinity,hi=-Infinity;
 for(const rows of curves)for(const row of rows){lo=Math.min(lo,row[field]);hi=Math.max(hi,row[field]);}
 const x=t=>5+(w-12)*(t-first)/Math.max(1,last-first),y=v=>h-17-(h-38)*(v-lo)/Math.max(1,hi-lo);
 c.font='12px system-ui';c.fillStyle='#42575a';c.fillText(field==='bid'?String(hi):money(hi),5,12);
 c.fillText(field==='bid'?String(lo):money(lo),5,h-2);
 curves.forEach((rows,i)=>{c.beginPath();let prior=null;c.strokeStyle=i?'#a75c19':'#00756a';c.lineWidth=1.6;
  for(const row of rows){const t=row.key[0];if(prior===null||t-prior>60000)c.moveTo(x(t),y(row[field]));
   else c.lineTo(x(t),y(row[field]));prior=t;}c.stroke()});
 const selected=marks[Number(el('cursor').value)];c.strokeStyle='#293c68';c.setLineDash([3,3]);
 c.beginPath();c.moveTo(x(selected.key[0]),15);c.lineTo(x(selected.key[0]),h-14);c.stroke();
}
const tracks=new Map(data.owners.map(owner=>[owner,{spans:[],markers:[]}]));
const active=new Map();
function change(owner,phase,index){
 const previous=active.get(owner),track=tracks.get(owner);
 if(previous&&previous.phase===phase)return;
 if(previous)track.spans.push([previous.start,index,previous.phase]);
 if(phase)active.set(owner,{start:index,phase});else active.delete(owner);
}
frames.forEach((frame,i)=>{for(const e of frame.events){
 if(!tracks.has(e.owner))continue;
 if(e.event==='order')change(e.owner,'pending',i);
 if(e.event==='fill')change(e.owner,'position',i);
 if(e.event==='exit'||(e.event==='cancel'&&active.get(e.owner)?.phase==='pending'))change(e.owner,null,i);
 if(['veto','invalidated','management'].includes(e.event))tracks.get(e.owner).markers.push([i,e.event]);
}});
for(const owner of data.owners)change(owner,null,marks.length-1);
function lanes(index){
 const canvas=el('lanes'),w=canvas.clientWidth,selected=el('owner').value;
 const owners=selected?[selected]:data.owners,h=owners.length*24+8,ratio=window.devicePixelRatio||1;
 canvas.style.height=h+'px';canvas.width=w*ratio;canvas.height=h*ratio;
 const c=canvas.getContext('2d');c.scale(ratio,ratio);c.font='13px system-ui';
 const x=i=>55+(w-62)*(marks[i].key[0]-first)/Math.max(1,last-first);
 owners.forEach((owner,row)=>{const y=17+24*row,track=tracks.get(owner);
  c.fillStyle='#243a3c';c.fillText(owner,2,y+3,48);
  for(const [start,end,phase] of track.spans){c.fillStyle=phase==='position'?'#00756a':'#a5acaf';
   c.fillRect(x(start),y-4,Math.max(2,x(end)-x(start)),7)}
  for(const [i,kind] of track.markers){const t=x(i);c.beginPath();
   if(kind==='management'){c.fillStyle='#203f78';c.arc(t,y-5,3,0,Math.PI*2)}
   else{c.fillStyle='#a34a20';c.moveTo(t,y-7);c.lineTo(t+3,y-3);c.lineTo(t,y+1);c.lineTo(t-3,y-3);c.closePath()}
   c.fill()}
 });
 c.strokeStyle='#293c68';c.setLineDash([3,3]);c.beginPath();c.moveTo(x(index),0);c.lineTo(x(index),h);c.stroke();
}
const cycleStarts=new Map();
frames.forEach((frame,index)=>{for(const e of frame.events)if(e.event==='armed')cycleStarts.set(e.cycle_id,{owner:e.owner,index})});
function setCycles(){
 const combo=el('cycle'),owner=el('owner').value;
 combo.replaceChildren(new Option('Todos',''));
 for(const [id,item] of cycleStarts)if(!owner||owner===item.owner)
  combo.add(new Option(item.owner+' · '+utc(marks[item.index].key[0])+' · '+id.slice(0,8),id));
}
setCycles();
function eventsAt(i){const owner=el('owner').value,cycle=el('cycle').value;
 return frames[i].events.filter(e=>(!owner||owner===e.owner)&&(!cycle||cycle===e.cycle_id))}

function draw(){
 const i=Number(el('cursor').value),m=marks[i],metric=el('metric');el('time').value=utc(m.key[0])+' · seq '+m.key[1];
 lanes(i);el('metricTitle').textContent=metric.selectedOptions[0].text;chart(el('price'),'bid',false);chart(el('curve'),metric.value,true);
 el('account').textContent='Patrimônio '+money(m.equity)+' · Risco reservado '+money(m.risk)+' · Margem '+money(m.margin);
 const list=el('events');list.replaceChildren();for(const e of eventsAt(i)){
  const li=document.createElement('li'),detail=document.createElement('details'),title=document.createElement('summary'),raw=document.createElement('pre');
  title.textContent=(e.owner||'Conta')+' · '+(reasons[e.reason]||e.reason)+(e.price!==undefined?' · '+e.price+' ticks':'');
  const explanation=document.createElement('p');
  if(e.reason==='local_loss_streak')explanation.textContent=e.local.losses+' perdas consecutivas encerradas. Limite do ciclo de novas entradas: '+data.limits[e.owner]+'.';
  if(e.reason==='shared_margin')explanation.textContent='Solicitação de '+money(e.requested_margin)+' com '+money(e.margin)+' já reservados e patrimônio de '+money(e.equity)+'.';
  if(e.reason==='reference_lost')explanation.textContent='Referência '+(e.reference??'do ciclo')+' · bid '+e.bid+' · ask '+e.ask+' ticks.';
  raw.textContent=JSON.stringify({event:e,rule:data.rules[e.owner],risk_limit:data.risk_limit},null,2);detail.append(title);if(explanation.textContent)detail.append(explanation);detail.append(raw);li.append(detail);list.append(li)}
 if(!list.children.length){const li=document.createElement('li');li.textContent='Sem decisão neste tick para a seleção.';list.append(li)}
}
function move(step,onlyEvents){let i=Number(el('cursor').value)+step;
 while(i>=0&&i<frames.length){if(!onlyEvents||eventsAt(i).length){el('cursor').value=i;draw();return}i+=step}}
for(const id of ['price','curve','lanes']){
 const canvas=el(id);canvas.onkeydown=e=>{if(['ArrowRight','ArrowLeft'].includes(e.key)){e.preventDefault();move(e.key==='ArrowRight'?1:-1,false)}};
 canvas.onpointerdown=e=>{const bounds=canvas.getBoundingClientRect();const left=id==='lanes'?55:5;
 const t=first+(last-first)*Math.max(0,Math.min(1,(e.clientX-bounds.left-left)/Math.max(1,bounds.width-left-7)));
 let a=0,b=marks.length-1;while(a<b){let mid=(a+b)>>1;if(marks[mid].key[0]<t)a=mid+1;else b=mid}el('cursor').value=a;draw()};
}
el('prev').onclick=()=>move(-1,true);el('next').onclick=()=>move(1,true);
el('owner').onchange=()=>{setCycles();draw()};
el('cycle').onchange=()=>{const cycle=cycleStarts.get(el('cycle').value);if(cycle)el('cursor').value=cycle.index;draw()};
el('metric').onchange=el('cursor').oninput=draw;
new ResizeObserver(draw).observe(el('price'));draw();
</script></html>"""
