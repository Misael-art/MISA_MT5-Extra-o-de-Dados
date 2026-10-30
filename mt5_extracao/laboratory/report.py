"""Relatório local portátil: os números vêm do serviço do Laboratório, sem rede."""
import html
import json
from pathlib import Path

from . import service

MAX_BYTES = 16*1024*1024


def render(report, *, cancel_check=None, timeout_seconds=120, max_memory_mb=1024):
    validated = service.load_report(json.dumps(report, allow_nan=False))
    comparisons = service.crossings(validated, cancel_check=cancel_check,
        timeout_seconds=timeout_seconds, max_memory_mb=max_memory_mb)
    payload = json.dumps(dict(analysis=validated, crossings=comparisons), ensure_ascii=False, allow_nan=False,
                         separators=(",", ":")).replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026")
    if len(payload.encode()) > MAX_BYTES:
        raise ValueError("Relatório excede16MiB; reduza canais/recorte antes de compartilhar.")
    return TEMPLATE.replace("__TITLE__", html.escape(f"Cruzamentos · {report['symbol']} · {report['grid_timeframe']}"))\
                   .replace("__PAYLOAD__", payload)


def export(report, destination, **kwargs):
    target = Path(destination).expanduser()
    content = render(report, **kwargs)
    try:
        with target.open("x", encoding="utf-8") as out:
            out.write(content)
    except OSError as exc:
        raise ValueError("Escolha um arquivo novo em uma pasta gravável: " + str(exc)) from exc
    return dict(path=str(target), analysis_id=report["analysis_id"], bytes=len(content.encode()),
                network_required=False, operational_acceptance=False)


TEMPLATE = r'''<!doctype html>
<html lang="pt-BR"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="Content-Security-Policy" content="default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; img-src data:; connect-src 'none'">
<title>__TITLE__</title>
<style>
:root{font:16px system-ui,sans-serif;color:#203149;background:#edf2f7}*{box-sizing:border-box}body{margin:0}
main{max-width:1320px;margin:auto;padding:24px}h1{font-size:1.8rem;margin:0 0 8px}h2{font-size:1.25rem;margin:0 0 14px}
p{line-height:1.5}.muted{color:#526278;font-size:.9rem;overflow-wrap:anywhere}.notice{border-left:4px solid #2465ad;background:#e6effa;padding:12px 16px}
section{background:white;border:1px solid #d0dce9;border-radius:10px;padding:18px;margin:18px 0;min-width:0}
.controls{display:flex;gap:12px;flex-wrap:wrap;align-items:center}label{display:grid;gap:4px;font-size:.9rem;max-width:100%;min-width:0}
select,input,button{font:inherit;padding:8px;border:1px solid #a9bbd0;border-radius:5px;background:white;color:inherit}
select{max-width:100%;min-width:0}button{cursor:pointer;background:#edf4fc}button:focus-visible,input:focus-visible,select:focus-visible{outline:3px solid #da8a12}
.cards{display:flex;gap:12px;flex-wrap:wrap}.card{padding:14px;background:#f1f5fa;min-width:160px;flex:1}.card b{display:block;font-size:1.5rem}
.scroll{overflow:auto;max-height:560px}table{border-collapse:collapse;font-size:.88rem;min-width:100%;font-variant-numeric:tabular-nums}
th,td{border:1px solid #d7e0ec;padding:10px;text-align:left;vertical-align:top}th{background:#edf3fa;position:sticky;top:0;z-index:1}
td.number{text-align:right;white-space:nowrap;min-width:85px}td small{display:block;color:#41516a;margin-top:4px}td.na{background:#f4f5f7;color:#647184}
#matrix th:first-child{left:0;z-index:2;min-width:210px;max-width:340px}#matrix td:first-child{position:sticky;left:0;background:white}
#matrix th:not(:first-child){width:150px;max-width:180px;overflow-wrap:anywhere}
svg{width:100%;height:300px;display:block;background:#fafcfe}#seriesplot{height:180px}#stability{height:220px}#stability text{font-size:15px}input[type=range]{flex:1;min-width:160px}
.tag{display:inline-block;background:#e6effa;padding:4px 8px;border-radius:4px;font-size:.8rem}.synthetic{background:#fff1cf}
details{margin:12px 0}pre{white-space:pre-wrap;overflow-wrap:anywhere;font-size:.8rem}a{color:#175aa1}
@media(max-width:650px){main{padding:12px}section{padding:12px}h1{font-size:1.4rem}.cards{gap:6px}.card{min-width:120px}th,td{padding:7px}}
@media print{button,input,select{display:none}.scroll{overflow:visible;max-height:none}section{break-inside:avoid}body{background:white}}
</style><main>
<h1>__TITLE__</h1><p id="scope" class="muted"></p><p id="qual" class="notice"></p>
<div class="controls"><span id="perspective" class="tag"></span><span id="synthetic" class="tag synthetic" hidden>Dados próprios de QA sintética</span><button id="download">Baixar análise JSON</button><button onclick="window.print()">Imprimir</button></div>
<section><div class="cards" id="cards"></div></section>
<section><h2>Candle, preço e informação disponível</h2>
<div class="controls"><button id="previous" aria-label="Candle anterior">←</button><input id="cursor" type="range" min="0" aria-label="Candle selecionado"><button id="next" aria-label="Próximo candle">→</button><b id="stamp"></b></div>
<p id="ohlc" class="muted"></p><svg id="price" viewBox="0 0 1000 300" role="img" aria-label="Candles do recorte congelado, seleção por clique"></svg>
<div class="controls"><label>Série vinculada ou indicador<select id="series"></select></label><span id="seriesvalue"></span></div>
<svg id="seriesplot" viewBox="0 0 1000 180" role="img" aria-label="Valores originais por candle; lacunas preservadas"></svg>
<div class="scroll"><table id="candle"></table></div>
<h3>Recebimentos neste candle</h3><div class="scroll"><table id="receipts"></table></div>
<h3>Janelas de publicação encerradas neste candle</h3><div class="scroll"><table id="publications"></table></div>
<p class="muted">Datas de publicação, captura e disponibilidade são distintas. O co-movimento não identifica o motivo do preço.</p></section>
<section><h2>Todos os cruzamentos da seleção</h2>
<div class="controls"><label>Defasagem nominal<select id="lag"></select></label>
<label>Timeframe<select id="tf"><option value="">Todos os TFs</option></select></label><label>Buscar série<input id="query" type="search" placeholder="nome, objeto ou fonte"></label></div>
<p class="muted">Spearman dos valores originais, de −1 a +1. Zero é efeito nulo; — indica ausência ou relação indefinida. n conta pares da grade, sem afirmar independência. A defasagem respeita lacunas e não certifica previsão.</p>
<div class="scroll"><table id="matrix"></table></div></section>
<section><h2>Como a relação muda na linha do tempo</h2>
<div class="controls"><label>Preditor<select id="left"></select></label><label>Resposta<select id="right"></select></label></div>
<p class="muted">Quatro blocos cronológicos do mesmo estudo, pela função de estabilidade existente. A defasagem segue a seleção da matriz. Bloco sem amostra não é zero; comparação retrospectiva sem causalidade certificada.</p>
<svg id="stability" viewBox="0 0 1000 220" role="img" aria-label="Correlação nos quatro blocos cronológicos"></svg>
<div class="scroll"><table id="windows"></table></div></section>
<section><h2>Indicadores e fontes × movimento posterior do preço</h2><p class="muted">Retorno bruto futuro em horizonte fixo, somente quando o fechamento existe no corte. Pesquisa retrospectiva, sem custos/fills ou regra de negociação.</p><div class="scroll"><table id="targets"></table></div></section>
<section><h2>Cronologia dos recibos vinculados</h2><p class="muted">Inclui revisões e observações que não cabem em um candle. Nenhuma soma ou extrapolação foi aplicada.</p><div class="controls"><label>Filtrar recibos<input id="eventquery" type="search" placeholder="fonte, data, estado"></label><button id="eventprev">Anterior</button><span id="eventpage"></span><button id="eventnext">Próxima</button></div><div class="scroll"><table id="events"></table></div></section>
<section><h2>Fonte, vínculos e limites</h2><div id="limits"></div><details><summary>Identidade e proveniência congeladas</summary><pre id="provenance"></pre></details></section>
</main><script type="application/json" id="data">__PAYLOAD__</script><script>
'use strict';
const D=JSON.parse(document.getElementById('data').textContent),R=D.analysis,X=D.crossings;
const $=id=>document.getElementById(id),fmt=v=>v===null||v===undefined?'—':typeof v==='number'?new Intl.NumberFormat('pt-BR',{maximumFractionDigits:6}).format(v):String(v);
const text=(id,v)=>$(id).textContent=v,cell=(parent,tag,value,cls)=>{const e=document.createElement(tag);e.textContent=value;if(cls)e.className=cls;parent.append(e);return e};
function table(id,heads,rows){const t=$(id);t.replaceChildren();const h=document.createElement('tr');heads.forEach(v=>cell(h,'th',v));t.append(h);for(const values of rows){const row=document.createElement('tr');values.forEach(v=>cell(row,'td',fmt(v)));t.append(row)}}
text('scope',R.symbol+' · grade '+R.grid_timeframe+' · corte '+R.selection.as_of+' · '+R.analysis_id);
text('perspective',R.selection.timeline_basis==='publication'?'Publicação retrospectiva no corte':R.selection.timeline_basis==='received'?'Recebimento · primeiro fechamento que conhece o fato':'Fechamentos nominais · recibo histórico não certificado');
text('qual','Correlação descritiva. Preços e fórmulas locais não comprovam recebimento histórico. Classificação de menções mantém o domínio do produtor; object-vader-en é restrito ao inglês controlado, sem validação representativa nas fontes.');
const EV=R.channels.flatMap(c=>(c.receipt_events||[]).map(e=>({...e,channel:c.name,channel_id:c.id}))).sort((a,b)=>a.available_at.localeCompare(b.available_at));
$('synthetic').hidden=!EV.some(e=>e.metadata&&e.metadata.synthetic_fixture);
for(const [label,value] of [['Candles',R.times.length],['Canais',R.channels.length],['Vínculos',(R.selection.linked_sources||[]).length],['Recibos',EV.length]]){const c=document.createElement('div');c.className='card';cell(c,'span',label);cell(c,'b',fmt(value));$('cards').append(c)}
R.limitations.forEach(v=>cell($('limits'),'p',v));text('provenance',JSON.stringify({selection:R.selection,populations:R.populations,qualification:R.qualification,integrity_sha256:R.integrity_sha256},null,2));
R.channels.forEach((c,i)=>{const o=document.createElement('option');o.value=i;o.textContent=c.name+' · '+c.timeframe;$('series').append(o)});
for(const id of ['left','right'])R.channels.forEach(c=>{const o=document.createElement('option');o.value=c.id;o.textContent=c.name+' · '+c.timeframe;$(id).append(o)});
if(R.channels.length>1)$('right').selectedIndex=1;
X.matrices.forEach(m=>{const o=document.createElement('option');o.value=m.lag_bars;o.textContent=m.lag_bars===0?'Mesmo fechamento':m.lag_bars+' candles';$('lag').append(o)});
[...new Set(R.channels.map(c=>c.timeframe))].forEach(v=>{const o=document.createElement('option');o.value=v;o.textContent=v;$('tf').append(o)});
$('cursor').max=R.times.length-1;let index=0,first=0,last=0,eventPage=0;
function node(parent,tag,attrs){const n=document.createElementNS('http://www.w3.org/2000/svg',tag);Object.entries(attrs).forEach(([k,v])=>n.setAttribute(k,v));parent.append(n);return n}
function chart(){const svg=$('price');svg.replaceChildren();first=Math.max(0,index-40);last=Math.min(R.times.length,first+80);first=Math.max(0,last-80);const prices=R.prices.slice(first,last),ys=prices.flatMap(p=>[p[1],p[2]]).filter(v=>v!==null),lo=Math.min(...ys),hi=Math.max(...ys),span=hi-lo||1;
 const px=i=>55+(i-first+.5)*900/Math.max(1,last-first),py=v=>260-(v-lo)*220/span;
 for(let k=0;k<=4;k++){let v=lo+k*span/4;node(svg,'line',{x1:55,x2:960,y1:py(v),y2:py(v),stroke:'#dce5ef'});const t=node(svg,'text',{x:3,y:py(v)+4,fill:'#53667e','font-size':11});t.textContent=fmt(v)}
 prices.forEach((p,j)=>{if(p.some(v=>v===null))return;const x=px(first+j),color=p[3]>=p[0]?'#087f70':'#bf3c38';node(svg,'line',{x1:x,x2:x,y1:py(p[2]),y2:py(p[1]),stroke:color,'stroke-width':1});node(svg,'line',{x1:x,x2:x,y1:py(p[0]),y2:py(p[3]),stroke:color,'stroke-width':Math.max(2,500/(last-first))})});
 node(svg,'line',{x1:px(index),x2:px(index),y1:25,y2:270,stroke:'#225dba','stroke-width':2});
 EV.filter(e=>e.receipt_bar_index!==null&&e.receipt_bar_index>=first&&e.receipt_bar_index<last).forEach(e=>node(svg,'circle',{cx:px(e.receipt_bar_index),cy:15,r:3,fill:'#b77916'}));
 const c=R.channels[Number($('series').value||0)],plot=$('seriesplot');plot.replaceChildren();const vals=c.values.slice(first,last).filter(v=>v!==null),a=vals.length?Math.min(...vals):0,b=vals.length?Math.max(...vals):1,den=b-a||1;
 c.values.slice(first,last).forEach((v,j)=>{if(v!==null){const n=node(plot,'circle',{cx:px(first+j),cy:145-(v-a)*115/den,r:3.5,fill:'#2465ad'});node(n,'title',{}).textContent=R.times[first+j]+' · '+fmt(v)}});
 const t=node(plot,'text',{x:10,y:16,fill:'#42556e','font-size':12});t.textContent=(c.unit||'unidade não declarada')+' · pontos originais; lacunas sem preenchimento';
 text('seriesvalue',fmt(c.raw_values?c.raw_values[index]:c.values[index])+' '+(c.unit||'')+' · '+(c.per_bar_states?c.per_bar_states[index]:'valor nominal'));
}
const eventHeads=['Série','Janela início','Janela fim','Captura','Disponibilidade','Revisão','Valor','Abstenções','Estado'];
const eventRows=events=>events.map(e=>[e.channel,e.observation_time,e.window_end,e.captured_at,e.available_at,e.revision,e.value,e.label_counts?e.label_counts.abstained:null,e.state]);
function candle(){index=Math.max(0,Math.min(R.times.length-1,index));$('cursor').value=index;text('stamp',R.times[index]);text('ohlc',['Abertura','Máxima','Mínima','Fechamento'].map((s,k)=>s+': '+fmt(R.prices[index][k])).join(' · ')+' · sessão: '+((R.session_ids||[])[index]||'sem calendário congelado'));
 table('candle',['Série / TF','Valor','Unidade','Observação','Disponibilidade / fechamento nominal','Estado'],R.channels.map(c=>[c.name+' · '+c.timeframe,c.raw_values?c.raw_values[index]:c.values[index],c.unit,(c.observation_times||[])[index],(c.source_ends||[])[index],c.per_bar_states?c.per_bar_states[index]:(c.diagnosis||{}).message]));
 table('receipts',eventHeads,eventRows(EV.filter(e=>e.receipt_bar_index===index)));table('publications',eventHeads,eventRows(EV.filter(e=>e.publication_bar_index===index)));chart();}
$('previous').onclick=()=>{index--;candle()};$('next').onclick=()=>{index++;candle()};$('cursor').oninput=()=>{index=Number($('cursor').value);candle()};$('series').onchange=chart;
$('price').onclick=e=>{const b=$('price').getBoundingClientRect(),x=(e.clientX-b.left)*1000/b.width;index=first+Math.max(0,Math.min(last-first-1,Math.floor((x-55)*(last-first)/900)));candle()};
function matrices(){const m=X.matrices.find(m=>m.lag_bars===Number($('lag').value)),query=$('query').value.toLocaleLowerCase(),tf=$('tf').value;
 const ids=m.channels.map((c,i)=>({c,i})).filter(({c})=>(!tf||c.timeframe===tf)&&(!query||(c.name+' '+c.timeframe).toLocaleLowerCase().includes(query))),t=$('matrix');t.replaceChildren();const h=document.createElement('tr');cell(h,'th','Preditor ↓ / resposta →');ids.forEach(({c})=>cell(h,'th',c.name+' · '+c.timeframe));t.append(h);
 ids.forEach(({c,i})=>{const row=document.createElement('tr');cell(row,'td',c.name+' · '+c.timeframe);ids.forEach(({i:j})=>{const q=m.cells[i][j],td=cell(row,'td',q.effect===null?'—':q.effect.toLocaleString('pt-BR',{minimumFractionDigits:3,maximumFractionDigits:3}),'number'+(q.effect===null?' na':''));td.title=q.reason_text||q.reason||'Spearman descritivo';const n=document.createElement('small');n.textContent='n='+q.n_pairs+' · pares distintos='+q.unique_source_pairs;td.append(n);if(q.effect!==null)td.style.background=q.effect>=0?'rgba(27,131,145,'+(.08+.25*Math.abs(q.effect))+')':'rgba(191,91,51,'+(.08+.25*Math.abs(q.effect))+')'});t.append(row)});
 const names=Object.fromEntries(R.channels.map(c=>[c.id,c.name+' · '+c.timeframe]));table('targets',['Série','Horizonte','Efeito','Pares','Estado / motivo'],X.price_targets.flatMap(m=>m.cells[0].map(c=>[names[c.left],m.lag_bars,c.effect,c.n_pairs,c.reason||c.state])));}
function stability(){const t=(X.timelines||[]).find(t=>t.left===$('left').value&&t.right===$('right').value&&t.lag_bars===Number($('lag').value)),svg=$('stability');svg.replaceChildren();
 for(const v of [-1,0,1]){node(svg,'line',{x1:55,x2:960,y1:100-v*70,y2:100-v*70,stroke:'#dce5ef'});const x=node(svg,'text',{x:10,y:105-v*70,fill:'#53667e','font-size':12});x.textContent=fmt(v)}
 const w=t?t.windows:[];w.forEach((p,i)=>{const x=90+i*270;if(p.value!==null){node(svg,'circle',{cx:x,cy:100-p.value*70,r:5,fill:'#2465ad'});if(i&&w[i-1].value!==null)node(svg,'line',{x1:x-270,x2:x,y1:100-w[i-1].value*70,y2:100-p.value*70,stroke:'#2465ad'})}const n=node(svg,'text',{x:x-55,y:200,fill:'#53667e','font-size':11});n.textContent=p.start.slice(5)+' · n='+p.n_pairs});
 table('windows',['Início UTC','Fim UTC','Spearman','Pares','Motivo'],w.map(p=>[p.start,p.end,p.value,p.n_pairs,p.reason||'Exploratória']));if(!w.length){const n=node(svg,'text',{x:170,y:115,fill:'#53667e','font-size':15});n.textContent='Sem janela elegível para este par/defasagem; veja o motivo na matriz.'}}
['lag','tf'].forEach(id=>$(id).onchange=()=>{matrices();stability()});['left','right'].forEach(id=>$(id).onchange=stability);$('query').oninput=matrices;
function events(){const q=$('eventquery').value.toLocaleLowerCase(),rows=EV.filter(e=>!q||(e.channel+' '+e.available_at+' '+e.state).toLocaleLowerCase().includes(q));eventPage=Math.max(0,Math.min(eventPage,Math.max(0,Math.ceil(rows.length/50)-1)));table('events',eventHeads,eventRows(rows.slice(eventPage*50,(eventPage+1)*50)));text('eventpage',(eventPage+1)+' / '+Math.max(1,Math.ceil(rows.length/50))+' · '+rows.length+' recibos');}
$('eventquery').oninput=()=>{eventPage=0;events()};$('eventprev').onclick=()=>{eventPage--;events()};$('eventnext').onclick=()=>{eventPage++;events()};
$('download').onclick=()=>{const a=document.createElement('a'),url=URL.createObjectURL(new Blob([JSON.stringify(R)],{type:'application/json'}));a.href=url;a.download='laboratorio-'+R.analysis_id.slice(0,12)+'.json';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000)};
candle();matrices();stability();events();
</script></html>'''
