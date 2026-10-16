"""
Painel visual de resultados: um arquivo HTML único, com os dados embutidos, sem servidor e sem nada da internet.

    path = write(matrix.build(db, config), "exports/painel_resultados.html")     # o mesmo que `mt5x panel`

Duas visões da matriz de `robots/matrix.py`: por ativo (ativo x timeframe do robô escolhido) e por robô (onde ele
teve resultado positivo). É só leitura: o painel não roda teste nenhum. As regras do que cada cor significa estão
em matrix.py e aparecem na legenda do próprio painel; nenhuma célula usa só cor (há sempre ícone e texto).
"""
import json
from pathlib import Path

_TEMPLATE = r"""<!doctype html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Painel de resultados</title>
<style>
:root {
  --bg: #f4f6f9; --surface: #ffffff; --line: #dfe4ec; --text: #17202e; --muted: #5d6b7e; --accent: #1f5fd6;
  --ok: #0f7b4b; --ok-bg: #d9f2e5; --prom: #1b6f8f; --prom-bg: #dcf0f7; --bad: #b3261e; --bad-bg: #fbe1df;
  --neg: #7a4a47; --neg-bg: #f1e9e8; --few: #8a5a00; --few-bg: #fbedcf; --none-bg: #f1f3f7; --shadow: 0 1px 2px rgba(15,25,45,.08);
}
:root[data-theme="dark"] {
  --bg: #10151d; --surface: #19212d; --line: #2b3646; --text: #e7ecf3; --muted: #9aa7b8; --accent: #7fb0ff;
  --ok: #6fdba6; --ok-bg: #143a2a; --prom: #7fcdea; --prom-bg: #14323f; --bad: #ff9a92; --bad-bg: #45201e;
  --neg: #d3aaa6; --neg-bg: #33282a; --few: #f0c36a; --few-bg: #3b2f14; --none-bg: #1e2733; --shadow: none;
}
@media (prefers-color-scheme: dark) {
  :root:not([data-theme="light"]) {
    --bg: #10151d; --surface: #19212d; --line: #2b3646; --text: #e7ecf3; --muted: #9aa7b8; --accent: #7fb0ff;
    --ok: #6fdba6; --ok-bg: #143a2a; --prom: #7fcdea; --prom-bg: #14323f; --bad: #ff9a92; --bad-bg: #45201e;
    --neg: #d3aaa6; --neg-bg: #33282a; --few: #f0c36a; --few-bg: #3b2f14; --none-bg: #1e2733; --shadow: none;
  }
}
* { box-sizing: border-box; }
[hidden] { display: none !important; }
body { margin: 0; background: var(--bg); color: var(--text); font: 15px/1.45 system-ui, -apple-system, "Segoe UI", Roboto, sans-serif; }
.wrap { max-width: 1280px; margin: 0 auto; padding: 20px 16px 48px; }
header.top { display: flex; flex-wrap: wrap; align-items: flex-end; justify-content: space-between; gap: 12px; margin-bottom: 16px; }
h1 { font-size: 24px; margin: 0; letter-spacing: -.01em; }
.sub { color: var(--muted); font-size: 13px; margin-top: 2px; }
button, select, input { font: inherit; color: inherit; }
.btn { background: var(--surface); border: 1px solid var(--line); border-radius: 8px; padding: 7px 12px; cursor: pointer; }
.btn:hover { border-color: var(--accent); }
.chips { display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 16px; }
.chip { background: var(--surface); border: 1px solid var(--line); border-radius: 10px; padding: 8px 12px; min-width: 92px; box-shadow: var(--shadow); }
.chip b { display: block; font-size: 20px; font-variant-numeric: tabular-nums; line-height: 1.15; }
.chip span { color: var(--muted); font-size: 12px; }
.bar { background: var(--surface); border: 1px solid var(--line); border-radius: 12px; padding: 12px; display: flex; flex-wrap: wrap; gap: 12px; align-items: flex-end; margin-bottom: 16px; box-shadow: var(--shadow); }
.field { display: flex; flex-direction: column; gap: 4px; min-width: 150px; }
.field label { font-size: 12px; color: var(--muted); }
.field select, .field input { background: var(--bg); border: 1px solid var(--line); border-radius: 8px; padding: 7px 9px; }
.tabs { display: inline-flex; border: 1px solid var(--line); border-radius: 9px; overflow: hidden; }
.tabs button { background: transparent; border: 0; padding: 8px 14px; cursor: pointer; white-space: nowrap; }
.tabs button[aria-selected="true"] { background: var(--accent); color: #fff; }
:root[data-theme="dark"] .tabs button[aria-selected="true"] { color: #0c1420; }
.card { background: var(--surface); border: 1px solid var(--line); border-radius: 12px; box-shadow: var(--shadow); overflow: hidden; }
.card h2 { font-size: 16px; margin: 0; padding: 14px 16px 4px; }
.card p.hint { color: var(--muted); font-size: 13px; margin: 0; padding: 0 16px 12px; }
.scroll { overflow-x: auto; }
table { border-collapse: separate; border-spacing: 0; width: 100%; }
th, td { padding: 8px; text-align: left; border-top: 1px solid var(--line); vertical-align: top; }
thead th { font-size: 12px; color: var(--muted); font-weight: 600; text-transform: uppercase; letter-spacing: .04em; border-top: 0; white-space: nowrap; }
th.sym { position: sticky; left: 0; background: var(--surface); z-index: 1; min-width: 108px; }
th.sym b { font-size: 15px; } th.sym small { display: block; color: var(--muted); font-weight: 400; font-size: 12px; }
td.cellwrap { min-width: 132px; padding: 6px; }
.cell { display: block; width: 100%; text-align: left; border: 1px solid transparent; border-radius: 10px; padding: 8px 10px; background: var(--none-bg); cursor: pointer; min-height: 74px; }
.cell:hover, .cell:focus-visible { border-color: var(--accent); outline: none; }
.cell .v { font-size: 19px; font-weight: 700; font-variant-numeric: tabular-nums; line-height: 1.15; }
.cell .d { font-size: 12px; color: var(--muted); font-variant-numeric: tabular-nums; }
.cell .s { font-size: 12px; font-weight: 600; margin-top: 3px; display: flex; gap: 5px; align-items: center; }
.cell.empty { cursor: default; border: 1px dashed var(--line); background: transparent; color: var(--muted); display: flex; align-items: center; justify-content: center; font-size: 13px; }
.cell.nodata { border-style: solid; background: var(--none-bg); opacity: .55; }
.st-aprovado { background: var(--ok-bg); } .st-aprovado .s, .st-aprovado .v { color: var(--ok); }
.st-promissor { background: var(--prom-bg); } .st-promissor .s { color: var(--prom); }
.st-reprovado { background: var(--bad-bg); } .st-reprovado .s { color: var(--bad); }
.st-negativo { background: var(--neg-bg); } .st-negativo .s, .st-negativo .v { color: var(--neg); }
.st-poucas { background: var(--few-bg); } .st-poucas .s { color: var(--few); } .st-poucas .v { opacity: .6; }
.pill { display: inline-flex; gap: 5px; align-items: center; border-radius: 999px; padding: 2px 9px; font-size: 12px; font-weight: 600; white-space: nowrap; }
.pill.st-aprovado { color: var(--ok); } .pill.st-promissor { color: var(--prom); } .pill.st-reprovado { color: var(--bad); }
.pill.st-negativo { color: var(--neg); } .pill.st-poucas { color: var(--few); }
td.num, th.num { text-align: right; font-variant-numeric: tabular-nums; white-space: nowrap; }
tr.pick { cursor: pointer; } tr.pick:hover td { background: var(--none-bg); }
tr.sel td { background: var(--none-bg); }
.grid2 { display: grid; gap: 16px; grid-template-columns: minmax(0, 1fr); }
.detail { margin-top: 16px; padding: 14px 16px; }
.detail h3 { margin: 0 0 8px; font-size: 15px; }
.detail dl { display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 10px 18px; margin: 0; }
.detail dt { font-size: 12px; color: var(--muted); } .detail dd { margin: 0; font-variant-numeric: tabular-nums; }
.legend { margin-top: 16px; padding: 14px 16px; }
.legend h3 { margin: 0 0 8px; font-size: 15px; }
.legend ul { list-style: none; margin: 0; padding: 0; display: grid; gap: 6px; }
.legend li { display: grid; grid-template-columns: 160px 1fr; gap: 10px; font-size: 13px; align-items: start; }
.warn { margin-top: 10px; font-size: 13px; color: var(--muted); border-left: 3px solid var(--few); padding-left: 10px; }
.blank { padding: 28px 16px; color: var(--muted); text-align: center; }
.live { background: var(--surface); border: 1px solid var(--line); border-radius: 12px; padding: 12px; margin-bottom: 16px; box-shadow: var(--shadow); }
.live .row { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }
.live .state { color: var(--muted); font-size: 13px; margin-right: auto; display: flex; gap: 8px; align-items: center; }
.dot { width: 9px; height: 9px; border-radius: 50%; background: var(--ok); display: inline-block; }
.dot.off { background: var(--bad); } .dot.busy { background: var(--few); }
.btn.primary { background: var(--accent); border-color: var(--accent); color: #fff; }
:root[data-theme="dark"] .btn.primary { color: #0c1420; }
.btn.danger { border-color: var(--bad); color: var(--bad); font-weight: 600; }
.btn[disabled] { opacity: .5; cursor: not-allowed; }
.job { margin-top: 10px; border-top: 1px solid var(--line); padding-top: 10px; font-size: 13px; }
.job b { font-size: 14px; } .job pre { margin: 6px 0 0; white-space: pre-wrap; color: var(--muted); font: 12px/1.45 ui-monospace, Menlo, Consolas, monospace; max-height: 150px; overflow: auto; }
.confirm { margin-top: 10px; padding: 10px; border: 1px solid var(--bad); border-radius: 8px; font-size: 13px; display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }
.acts { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 12px; }
@media (max-width: 640px) { .legend li { grid-template-columns: 1fr; } .field { min-width: 46%; flex: 1; } h1 { font-size: 20px; } }
</style>
</head>
<body>
<div class="wrap">
  <header class="top">
    <div>
      <h1>Painel de resultados</h1>
      <div class="sub" id="generated"></div>
    </div>
    <button class="btn" id="theme" type="button" aria-label="Alternar tema claro e escuro">Tema</button>
  </header>
  <div class="live" id="live" hidden>
    <div class="row">
      <div class="state"><span class="dot" id="dot"></span><span id="livestate">Painel vivo</span></div>
      <button class="btn primary" id="act-missing" type="button">Testar o que falta</button>
      <button class="btn" id="act-update" type="button" title="Busca no MetaTrader 5 as barras novas dos timeframes extraídos e refaz os montados.">Atualizar a base</button>
      <button class="btn" id="act-specs" type="button" title="Grava o contrato (ponto, valor do tick, lotes) dos ativos: resolve as células 'não confiável'.">Gravar especificações</button>
      <button class="btn" id="act-resample" type="button">Montar timeframes</button>
      <button class="btn danger" id="act-stop" type="button">PARAR TUDO</button>
    </div>
    <div class="confirm" id="confirm" hidden></div>
    <div class="job" id="job" hidden></div>
  </div>
  <div class="chips" id="chips"></div>
  <div class="bar">
    <div class="field" style="min-width:auto">
      <label id="lview">Visão</label>
      <div class="tabs" role="tablist" aria-labelledby="lview">
        <button type="button" role="tab" id="tab-asset" aria-selected="true">Por ativo</button>
        <button type="button" role="tab" id="tab-robot" aria-selected="false">Por robô</button>
      </div>
    </div>
    <div class="field"><label for="robot">Robô ou estratégia</label><select id="robot"></select></div>
    <div class="field"><label for="metric">Número em destaque</label>
      <select id="metric">
        <option value="return_pct">Resultado (%)</option>
        <option value="profit_factor">Profit factor</option>
        <option value="trades">Operações</option>
        <option value="max_drawdown_pct">Queda máxima (%)</option>
      </select>
    </div>
    <div class="field"><label for="search">Filtrar ativo</label><input id="search" type="search" placeholder="ex.: EURUSD" autocomplete="off"></div>
  </div>
  <div id="view"></div>
  <div class="card detail" id="detail" hidden></div>
  <div class="card legend">
    <h3>O que cada situação significa</h3>
    <ul id="legend"></ul>
    <p class="warn">Cada célula mostra o teste <b>mais recente</b> daquela combinação, não o melhor. Lucro em backtest não é garantia de
    resultado. Números do backtester Python são otimistas em custo: a palavra final é a do Strategy Tester com ticks reais.</p>
    <p class="warn" id="livenote" hidden>Painel vivo: os botões executam ações do programa neste computador, uma por vez. Por aqui não se
    inicia robô em conta nem se libera o PARAR TUDO: isso continua sendo feito pela linha de comando.</p>
  </div>
</div>
<script id="data" type="application/json">__DATA__</script>
<script>
(function () {
  "use strict";
  var M = JSON.parse(document.getElementById("data").textContent);
  var LIVE = __LIVE__ && location.protocol !== "file:";
  var KEY = (location.search.match(/[?&]k=([^&#]+)/) || [])[1] || "";
  var ICON = {"aprovado": "✓", "promissor": "↗", "reprovado": "×", "negativo": "▼", "poucas operações": "…", "não confiável": "⚠", "sem teste": "○", "sem base": "–"};
  var CLS = {"aprovado": "st-aprovado", "promissor": "st-promissor", "reprovado": "st-reprovado", "negativo": "st-negativo", "poucas operações": "st-poucas", "não confiável": "st-poucas"};
  var ORDER = ["aprovado", "promissor", "reprovado", "negativo", "poucas operações", "não confiável", "sem teste", "sem base"];
  var state = {view: "asset", robot: "", metric: "return_pct", search: ""};
  var $ = function (id) { return document.getElementById(id); };
  function el(tag, attrs, children) {
    var node = document.createElement(tag);
    Object.keys(attrs || {}).forEach(function (k) {
      if (k === "text") node.textContent = attrs[k]; else if (k === "class") node.className = attrs[k]; else node.setAttribute(k, attrs[k]);
    });
    (children || []).forEach(function (c) { node.appendChild(typeof c === "string" ? document.createTextNode(c) : c); });
    return node;
  }
  function num(x, digits) { return x == null || !Number.isFinite(Number(x)) ? "—" : Number(x).toLocaleString("pt-BR", {minimumFractionDigits: digits, maximumFractionDigits: digits}); }
  function pfOrder(x) { return x == null || !Number.isFinite(Number(x)) ? -Infinity : Number(x); }
  function pct(x) { return (x > 0 ? "+" : "") + num(x * 100, 1) + "%"; }
  function metric(cell, key) {
    if (key === "profit_factor") return num(cell.profit_factor, 2);
    if (key === "trades") return String(cell.trades);
    if (key === "max_drawdown_pct") return num(cell.max_drawdown_pct * 100, 1) + "%";
    return pct(cell.return_pct);
  }
  var cells = {};
  function index() {
    cells = {};
    M.cells.forEach(function (c) { cells[c.robot + "|" + c.symbol + "|" + c.timeframe] = c; });
  }
  index();
  function robotOf(key) { return M.robots.filter(function (x) { return x.key === key; })[0]; }
  function robotName(key) { var r = M.robots.filter(function (x) { return x.key === key; })[0]; return r ? r.name : key; }
  function symbols() {
    var q = state.search.trim().toLowerCase();
    return M.symbols.filter(function (s) { return !q || s.toLowerCase().indexOf(q) >= 0; });
  }
  function statusOf(robot, symbol, tf) {
    var c = cells[robot + "|" + symbol + "|" + tf];
    if (c) return c.status;
    return (M.inventory[symbol] || {})[tf] ? "sem teste" : "sem base";
  }
  function pill(status) { return el("span", {"class": "pill " + (CLS[status] || "")}, [ICON[status] + " " + status]); }

  function showDetail(cell) {
    var box = $("detail");
    box.hidden = false;
    box.textContent = "";
    box.appendChild(el("h3", {text: robotName(cell.robot) + " em " + cell.symbol + " " + cell.timeframe}));
    var rows = [["Situação", ICON[cell.status] + " " + cell.status], ["Resultado", pct(cell.return_pct) + " (" + num(cell.net_profit, 2) + ")"],
      ["Profit factor", num(cell.profit_factor, 2)], ["Operações", String(cell.trades)], ["Acerto", num(cell.win_rate * 100, 1) + "%"],
      ["Queda máxima", num(cell.max_drawdown_pct * 100, 1) + "%"], ["Período testado", cell.period], ["Testado em", cell.tested_at],
      ["Origem do número", cell.origin], ["Veredito", cell.verdict || "ainda sem bateria nem validação"]];
    if (cell.preset) rows.push(["Preset", cell.preset]);
    if (cell.profit_factor_note) rows.push(["Qualidade do PF", cell.profit_factor_note]);
    var dl = el("dl");
    rows.forEach(function (r) { dl.appendChild(el("div", {}, [el("dt", {text: r[0]}), el("dd", {text: r[1]})])); });
    box.appendChild(dl);
    box.appendChild(el("p", {"class": "warn", text: M.statuses[cell.status] + "."}));
    if (LIVE) {
      var acts = el("div", {"class": "acts"});
      var battery = el("button", {"class": "btn", type: "button", text: "Rodar a bateria neste ativo"});
      battery.addEventListener("click", function () { act("battery", {robot: cell.robot, symbol: cell.symbol}); });
      acts.appendChild(battery);
      var again = el("button", {"class": "btn", type: "button", text: "Testar de novo"});
      again.addEventListener("click", function () { act("backtest", {robots: [cell.robot], symbols: [cell.symbol], tfs: [cell.timeframe]}); });
      acts.appendChild(again);
      box.appendChild(acts);
    }
    // o detalhe fica abaixo da tabela: sem isto, clicar numa célula parecia não fazer nada em telas baixas
    if (box.scrollIntoView) box.scrollIntoView({block: "nearest", behavior: "smooth"});
  }

  function renderAsset(root) {
    var card = el("div", {"class": "card"});
    card.appendChild(el("h2", {text: robotName(state.robot) + ": ativos e timeframes"}));
    card.appendChild(el("p", {"class": "hint", text: "Uma linha por ativo com base, uma coluna por timeframe. Clique em uma célula para ver os detalhes."}));
    var list = symbols();
    if (!list.length) {
      var none = M.symbols.length ? "Nenhum ativo combina com o filtro \"" + state.search.trim() + "\". Apague o filtro para ver todos."
                                  : "Nenhum ativo com base. Extraia dados com: mt5x extract";
      card.appendChild(el("div", {"class": "blank", text: none})); root.appendChild(card); return;
    }
    var head = el("tr", {}, [el("th", {"class": "sym", text: "Ativo"})]);
    M.timeframes.forEach(function (tf) { head.appendChild(el("th", {text: tf})); });
    var body = el("tbody");
    list.forEach(function (symbol) {
      var inv = M.inventory[symbol] || {};
      var row = el("tr", {}, [el("th", {"class": "sym", scope: "row"}, [el("b", {text: symbol}),
        el("small", {text: Object.keys(inv).length + " timeframe(s) com base"})])]);
      M.timeframes.forEach(function (tf) {
        var status = statusOf(state.robot, symbol, tf), c = cells[state.robot + "|" + symbol + "|" + tf], td = el("td", {"class": "cellwrap"});
        if (c) {
          var b = el("button", {"class": "cell " + (CLS[status] || ""), type: "button",
            "aria-label": symbol + " " + tf + ": " + status + ", " + metric(c, state.metric)});
          b.appendChild(el("div", {"class": "v", text: metric(c, state.metric)}));
          b.appendChild(el("div", {"class": "d", text: c.trades + " ops · PF " + num(c.profit_factor, 2)}));
          b.appendChild(el("div", {"class": "s", text: ICON[status] + " " + status}));
          b.addEventListener("click", function () { showDetail(c); });
          td.appendChild(b);
        } else {
          var note = status === "sem teste" && inv[tf] && inv[tf].built_from ? " (base montada de " + inv[tf].built_from + ")" : "";
          td.appendChild(el("div", {"class": "cell empty" + (status === "sem base" ? " nodata" : ""), title: M.statuses[status] + note,
            text: ICON[status] + " " + status}));
        }
        row.appendChild(td);
      });
      body.appendChild(row);
    });
    card.appendChild(el("div", {"class": "scroll"}, [el("table", {}, [el("thead", {}, [head]), body])]));
    root.appendChild(card);
  }

  function count(robot) {
    var n = {total: 0}; ORDER.forEach(function (s) { n[s] = 0; });
    M.cells.forEach(function (c) { if (c.robot === robot) { n[c.status] += 1; n.total += 1; } });
    return n;
  }
  function positives(robot) {
    return M.cells.filter(function (c) { return c.robot === robot && (c.status === "aprovado" || c.status === "promissor"); })
      .sort(function (a, b) { return (a.status !== "aprovado") - (b.status !== "aprovado") || pfOrder(b.profit_factor) - pfOrder(a.profit_factor); });
  }
  function renderRobot(root) {
    var all = el("div", {"class": "card"});
    all.appendChild(el("h2", {text: "Todos os robôs e estratégias"}));
    all.appendChild(el("p", {"class": "hint", text: "Quantas combinações de ativo e timeframe cada um tem em cada situação. Clique em uma linha para escolher."}));
    var head = el("tr", {}, [el("th", {text: "Robô"}), el("th", {text: "Tipo"}), el("th", {"class": "num", text: "Testadas"})]);
    ["aprovado", "promissor", "reprovado", "negativo", "poucas operações", "não confiável"].forEach(function (s) { head.appendChild(el("th", {"class": "num", text: ICON[s] + " " + s})); });
    var body = el("tbody");
    M.robots.forEach(function (r) {
      var n = count(r.key), row = el("tr", {"class": "pick" + (r.key === state.robot ? " sel" : ""), tabindex: "0"},
        [el("td", {}, [el("b", {text: r.name})]), el("td", {text: r.kind_label}), el("td", {"class": "num", text: String(n.total)})]);
      ["aprovado", "promissor", "reprovado", "negativo", "poucas operações", "não confiável"].forEach(function (s) { row.appendChild(el("td", {"class": "num", text: n[s] ? String(n[s]) : "·"})); });
      function choose() { state.robot = r.key; $("robot").value = r.key; render(); if (LIVE) buttons(); }
      row.addEventListener("click", choose);
      row.addEventListener("keydown", function (e) { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); choose(); } });
      body.appendChild(row);
    });
    all.appendChild(el("div", {"class": "scroll"}, [el("table", {}, [el("thead", {}, [head]), body])]));
    root.appendChild(all);

    var q = state.search.trim().toLowerCase();
    var rows = positives(state.robot).filter(function (c) { return !q || c.symbol.toLowerCase().indexOf(q) >= 0; });
    var card = el("div", {"class": "card", style: "margin-top:16px"});
    card.appendChild(el("h2", {text: robotName(state.robot) + ": onde o resultado foi positivo"}));
    card.appendChild(el("p", {"class": "hint", text: "Só combinações com lucro e " + M.min_trades + " ou mais operações. As aprovadas vêm primeiro; depois, as promissoras, que ainda precisam de bateria ou validação."}));
    if (!rows.length) {
      card.appendChild(el("div", {"class": "blank", text: count(state.robot).total ? "Nenhuma combinação positiva com operações suficientes até agora." : "Este robô ainda não foi testado. Rode: mt5x robots backtest"}));
    } else {
      var h = el("tr", {}, [el("th", {text: "Ativo"}), el("th", {text: "Timeframe"}), el("th", {text: "Situação"}), el("th", {"class": "num", text: "Resultado"}),
        el("th", {"class": "num", text: "Profit factor"}), el("th", {"class": "num", text: "Operações"}), el("th", {"class": "num", text: "Queda máx."}), el("th", {text: "Origem"})]);
      var b = el("tbody");
      rows.forEach(function (c) {
        var tr = el("tr", {"class": "pick", tabindex: "0"}, [el("td", {}, [el("b", {text: c.symbol})]), el("td", {text: c.timeframe}), el("td", {}, [pill(c.status)]),
          el("td", {"class": "num", text: pct(c.return_pct)}), el("td", {"class": "num", text: num(c.profit_factor, 2)}), el("td", {"class": "num", text: String(c.trades)}),
          el("td", {"class": "num", text: num(c.max_drawdown_pct * 100, 1) + "%"}), el("td", {text: c.origin})]);
        tr.addEventListener("click", function () { showDetail(c); });
        tr.addEventListener("keydown", function (e) { if (e.key === "Enter") showDetail(c); });
        b.appendChild(tr);
      });
      card.appendChild(el("div", {"class": "scroll"}, [el("table", {}, [el("thead", {}, [h]), b])]));
    }
    root.appendChild(card);
  }

  function render() {
    var root = $("view");
    root.textContent = "";
    $("detail").hidden = true;
    $("tab-asset").setAttribute("aria-selected", String(state.view === "asset"));
    $("tab-robot").setAttribute("aria-selected", String(state.view === "robot"));
    if (!M.robots.length) { root.appendChild(el("div", {"class": "card blank", text: "Nenhum robô no catálogo."})); return; }
    (state.view === "asset" ? renderAsset : renderRobot)(root);
    try { history.replaceState(null, "", "#" + state.view + "/" + encodeURIComponent(state.robot) + "/" + state.metric); } catch (e) { /* arquivo local */ }
  }

  function header() {
    $("generated").textContent = LIVE ? "Painel vivo · dados de " + M.generated_at
      : "Gerado em " + M.generated_at + " · só leitura: este arquivo não roda testes";
    var totals = {}; ORDER.forEach(function (s) { totals[s] = 0; });
    M.cells.forEach(function (c) { totals[c.status] += 1; });
    $("chips").textContent = "";
    [[M.symbols.length, "ativos"], [M.timeframes.length, "timeframes"], [M.robots.length, "robôs e estratégias"], [M.cells.length, "combinações testadas"],
     [totals["aprovado"], "✓ aprovadas"], [totals["promissor"], "↗ promissoras"]].forEach(function (c) {
      $("chips").appendChild(el("div", {"class": "chip"}, [el("b", {text: String(c[0])}), el("span", {text: c[1]})]));
    });
    var groups = {};
    $("robot").textContent = "";
    M.robots.forEach(function (r) { (groups[r.kind_label] = groups[r.kind_label] || []).push(r); });
    Object.keys(groups).forEach(function (label) {
      var g = el("optgroup", {label: label});
      groups[label].forEach(function (r) { g.appendChild(el("option", {value: r.key, text: r.name + (r.tested ? " (" + r.tested + (r.tested === 1 ? " teste)" : " testes)") : " (sem teste)")})); });
      $("robot").appendChild(g);
    });
    if (state.robot) $("robot").value = state.robot;
  }
  header();
  ORDER.forEach(function (s) { $("legend").appendChild(el("li", {}, [pill(s), el("span", {text: M.statuses[s]})])); });

  // ----- painel vivo: só quando a página vem do servidor local do programa -----
  var lastJob = null, busy = false;
  function api(method, path, body) {
    var options = {method: method, headers: {"X-MT5X-Key": KEY}, cache: "no-store"};
    if (body) { options.headers["Content-Type"] = "application/json"; options.body = JSON.stringify(body); }
    return fetch(path, options).then(function (r) { return r.json().then(function (j) { return {ok: r.ok, data: j}; }); });
  }
  function setState(text, kind) { $("livestate").textContent = text; $("dot").className = "dot" + (kind ? " " + kind : ""); }
  function showJob(job) {
    var box = $("job");
    if (!job) { box.hidden = true; return; }
    box.hidden = false;
    box.textContent = "";
    box.appendChild(el("b", {text: job.label + ": " + job.status + (job.result ? " · " + job.result : "")}));
    if (job.messages.length) box.appendChild(el("pre", {text: job.messages.join("\n")}));
  }
  function buttons() {
    var robot = robotOf(state.robot) || {};
    ["act-missing", "act-resample", "act-update", "act-specs"].forEach(function (id) { $(id).disabled = busy; });
    $("act-missing").textContent = "Testar o que falta em " + (robot.name || "…");
    $("act-missing").title = robot.kind === "python"
      ? "Roda o backtest desta estratégia em cada ativo e timeframe com base que ainda não foi testado."
      : "Roda o Strategy Tester em cada ativo e timeframe ainda sem teste. Se o MetaTrader 5 estiver aberto, o painel pede para fechá-lo.";
  }
  function refresh() {
    return api("GET", "/api/matrix").then(function (r) {
      if (!r.ok) { setState(r.data.error || "Sem acesso ao programa", "off"); return; }
      if (r.data.version !== M.version) { M = r.data; index(); header(); render(); }
      if (!busy) setState("Painel vivo · atualizado às " + new Date().toLocaleTimeString("pt-BR"), "");
    }).catch(function () { setState("O programa não está respondendo: o painel parou de se atualizar", "off"); });
  }
  function poll() {
    api("GET", "/api/job").then(function (r) {
      if (!r.ok) return;
      var job = r.data.job, was = busy;
      busy = !!job && job.status === "em andamento";
      if (job && (busy || !lastJob || lastJob.id !== job.id || lastJob.status !== job.status)) showJob(job);
      if (busy) setState("Trabalhando: " + job.label, "busy");
      if (was && !busy) refresh();
      lastJob = job; buttons();
    }).catch(function () {});
  }
  function ask(text, yesLabel, onYes) {
    var box = $("confirm");
    box.hidden = false; box.textContent = "";
    box.appendChild(el("span", {text: text}));
    var yes = el("button", {"class": "btn danger", type: "button", text: yesLabel});
    var no = el("button", {"class": "btn", type: "button", text: "Cancelar"});
    yes.addEventListener("click", function () { box.hidden = true; onYes(); });
    no.addEventListener("click", function () { box.hidden = true; });
    box.appendChild(yes); box.appendChild(no);
  }
  function act(action, params) {
    $("confirm").hidden = true;
    api("POST", "/api/action", {action: action, params: params}).then(function (r) {
      if (!r.ok && r.data.confirm) {
        ask(r.data.error + " Confirma?", "Sim, fechar o MetaTrader 5 e testar", function () {
          var again = {}; Object.keys(params).forEach(function (k) { again[k] = params[k]; }); again.confirm = r.data.confirm;
          act(action, again);
        });
        return;
      }
      if (!r.ok) { showJob({label: "Pedido recusado", status: "não executado", result: r.data.error, messages: []}); return; }
      busy = true; showJob(r.data.job); setState("Trabalhando: " + r.data.job.label, "busy"); buttons();
    }).catch(function () { setState("O programa não está respondendo", "off"); });
  }
  function missing() {
    var symbols = {}, tfs = {};
    M.symbols.forEach(function (s) { M.timeframes.forEach(function (tf) {
      if ((M.inventory[s] || {})[tf] && !cells[state.robot + "|" + s + "|" + tf]) { symbols[s] = 1; tfs[tf] = 1; }
    }); });
    return {symbols: Object.keys(symbols), tfs: Object.keys(tfs)};
  }
  if (LIVE) {
    $("live").hidden = false; $("livenote").hidden = false;
    $("act-missing").addEventListener("click", function () {
      var m = missing();
      if (!m.symbols.length) { showJob({label: "Testar o que falta", status: "nada a fazer", result: "Todas as combinações com base já foram testadas.", messages: []}); return; }
      act("backtest", {robots: [state.robot], symbols: m.symbols, tfs: m.tfs, resume: true});
    });
    $("act-resample").addEventListener("click", function () {
      var base = M.timeframes[0], sizes = {M1: 1, M5: 5, M15: 15, M30: 30, H1: 60, H4: 240, D1: 1440};
      var targets = Object.keys(sizes).filter(function (tf) { return sizes[base] && sizes[tf] > sizes[base] && sizes[tf] % sizes[base] === 0; });
      var symbols = M.symbols.filter(function (s) { return (M.inventory[s] || {})[base]; });
      if (!targets.length || !symbols.length) { showJob({label: "Montar timeframes", status: "nada a fazer", result: "Não há um timeframe menor na base de onde montar os maiores.", messages: []}); return; }
      act("resample", {symbols: symbols, base: base, tfs: targets});
    });
    $("act-stop").addEventListener("click", function () {
      ask("PARAR TUDO: os robôs com a camada de segurança param de abrir operações. Posições abertas NÃO são fechadas. Confirma?",
          "Sim, parar tudo", function () { act("stop_all", {}); });
    });
    $("act-update").addEventListener("click", function () {
      var tfs = M.timeframes.filter(function (tf) { return M.symbols.some(function (s) { var i = (M.inventory[s] || {})[tf]; return i && !i.built_from; }); });
      if (!tfs.length) { showJob({label: "Atualizar a base", status: "nada a fazer", result: "Não há séries extraídas na base.", messages: []}); return; }
      act("update", {symbols: M.symbols, tfs: tfs});
    });
    $("act-specs").addEventListener("click", function () { act("specs", {symbols: M.symbols}); });
    setInterval(poll, 2000); setInterval(function () { if (!busy) refresh(); }, 10000);
    poll();
  }
  var best = M.robots.slice().sort(function (a, b) { return b.tested - a.tested; })[0];
  state.robot = best ? best.key : "";
  var hash = decodeURIComponent(location.hash.replace(/^#/, "")).split("/");
  if (hash[0] === "robot" || hash[0] === "asset") state.view = hash[0];
  if (hash[1] && M.robots.some(function (r) { return r.key === hash[1]; })) state.robot = hash[1];
  if (hash[2] && $("metric").querySelector('option[value="' + hash[2] + '"]')) state.metric = hash[2];
  $("robot").value = state.robot; $("metric").value = state.metric;
  $("robot").addEventListener("change", function (e) { state.robot = e.target.value; render(); if (LIVE) buttons(); });
  $("metric").addEventListener("change", function (e) { state.metric = e.target.value; render(); });
  $("search").addEventListener("input", function (e) { state.search = e.target.value; render(); });
  $("tab-asset").addEventListener("click", function () { state.view = "asset"; render(); });
  $("tab-robot").addEventListener("click", function () { state.view = "robot"; render(); });
  $("theme").addEventListener("click", function () {
    var root = document.documentElement, dark = root.getAttribute("data-theme") === "dark" ||
      (!root.getAttribute("data-theme") && window.matchMedia("(prefers-color-scheme: dark)").matches);
    root.setAttribute("data-theme", dark ? "light" : "dark");
    try { localStorage.setItem("mt5x-tema", dark ? "light" : "dark"); } catch (e) { /* sem armazenamento: só não lembra */ }
  });
  var forced = (location.search.match(/[?&]tema=(claro|escuro)/) || [])[1], saved = null;
  try { saved = localStorage.getItem("mt5x-tema"); } catch (e) { saved = null; }
  if (forced) document.documentElement.setAttribute("data-theme", forced === "escuro" ? "dark" : "light");
  else if (saved === "dark" || saved === "light") document.documentElement.setAttribute("data-theme", saved);
  render();
  if (LIVE) buttons();
})();
</script>
</body>
</html>
"""


def render(matrix: dict, live: bool = False) -> str:
    """
    O HTML completo do painel para a matriz dada (`matrix.build`). Com `live`, a página (servida pelo
    panel_server) se atualiza sozinha e mostra os botões de ação; como arquivo, é a fotografia de sempre.
    """
    data = json.dumps(matrix, ensure_ascii=False, separators=(",", ":"))
    # o JSON vai dentro de uma tag <script>: nenhum "<" pode aparecer literalmente (fecharia a tag ou abriria um
    # comentário). \u003c é um escape válido de JSON, então o texto chega inteiro ao JSON.parse.
    data = data.replace("<", "\\u003c")
    return _TEMPLATE.replace("__LIVE__", "true" if live else "false").replace("__DATA__", data)


def write(matrix: dict, path) -> Path:
    """Grava o painel em `path` (cria a pasta) e devolve o caminho."""
    path = Path(path).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render(matrix), encoding="utf-8")
    return path
