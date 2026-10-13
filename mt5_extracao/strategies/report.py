"""
Relatório HTML autocontido (sem internet, abre em qualquer navegador): triagem de ativos,
matriz ativo × estratégia com o veredito do Filtro C, resumo das estratégias e o que evitar.
"""
import html
import math
from datetime import datetime
from typing import List

import pandas as pd

from . import AVOID, REGISTRY

CSS = """
:root{--bg:#f7f7f5;--card:#fff;--text:#1d1d1b;--muted:#6b6b66;--line:#e3e2dd;--ok:#1f7a4d;--ok-bg:#e3f4ea;
--bad:#b3261e;--bad-bg:#fbe7e5;--warn:#8a5a00;--warn-bg:#fdf1d8;--accent:#2f5d9e}
@media (prefers-color-scheme:dark){:root{--bg:#161615;--card:#1f1f1d;--text:#ecebe6;--muted:#a3a29b;--line:#34342f;
--ok:#6fd39f;--ok-bg:#173326;--bad:#ff8a80;--bad-bg:#3a1a17;--warn:#f2c46b;--warn-bg:#3a2e12;--accent:#8fb3ef}}
*{box-sizing:border-box}html,body{max-width:100%;overflow-x:hidden}body{margin:0;background:var(--bg);color:var(--text);
font:15px/1.5 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif}
main{max-width:1100px;margin:0 auto;padding:24px 16px 64px}
h1{font-size:1.6rem;margin:0 0 4px;overflow-wrap:anywhere}h2{font-size:1.2rem;margin:32px 0 8px}
.sub{color:var(--muted);margin:0 0 16px}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:16px;margin:12px 0;overflow-x:auto}
table{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums}
th,td{text-align:left;padding:8px 10px;border-bottom:1px solid var(--line);vertical-align:top}
th{font-size:.8rem;text-transform:uppercase;letter-spacing:.03em;color:var(--muted)}
td.num{text-align:right}
.badge{display:inline-block;padding:1px 8px;border-radius:999px;font-size:.8rem;font-weight:600;white-space:nowrap}
.ok{color:var(--ok);background:var(--ok-bg)}.bad{color:var(--bad);background:var(--bad-bg)}
.warn{color:var(--warn);background:var(--warn-bg)}.none{color:var(--muted)}
.score{display:inline-block;width:64px;height:8px;border-radius:4px;background:var(--line);vertical-align:middle;margin-right:6px}
.score>i{display:block;height:100%;border-radius:4px;background:var(--accent)}
.notice{border-left:4px solid var(--warn);background:var(--warn-bg);padding:10px 14px;border-radius:6px}
small,.muted{color:var(--muted)}ul{margin:6px 0;padding-left:20px}
"""


def _e(x) -> str:
    return html.escape(str(x))


def _num(x, digits=2) -> str:
    if x is None or (isinstance(x, float) and (math.isnan(x))):
        return "—"
    return f"{x:.{digits}f}".replace(".", ",")


def _screen_table(rows: List[dict]) -> str:
    if not rows:
        return "<p class='muted'>Nenhum ativo com dados para analisar.</p>"
    out = ["<table><tr><th>Ativo</th><th>Válido</th><th>Regime</th><th>Estratégia sugerida</th>"
           "<th>Score</th><th>Observação</th></tr>"]
    for r in rows:
        if r["valid"]:
            badge = "<span class='badge warn'>⚠ verificar</span>" if r["warnings"] else \
                "<span class='badge ok'>✅ apto</span>"
            obs = r["notes"] + r["warnings"] + ([r["validated"]] if r["validated"] else [])
            strategy = r["strategy_name"]
        else:
            badge, obs, strategy = "<span class='badge bad'>❌ inapto</span>", r["reasons"], "—"
        score = r["score"]
        out.append(f"<tr><td><b>{_e(r['symbol'])}</b></td><td>{badge}</td><td>{_e(r['regime'])}</td>"
                   f"<td>{_e(strategy)}</td><td><span class='score'><i style='width:{score:.0f}%'></i></span>"
                   f"{score:.0f}</td><td>{'<br>'.join(_e(o) for o in obs) or '—'}</td></tr>")
    out.append("</table>")
    return "".join(out)


def _matrix(rows: List[dict], validations: pd.DataFrame) -> str:
    if validations is None or validations.empty:
        return ("<p class='muted'>Nenhuma validação gravada para este timeframe. Rode "
                "<code>mt5x validate --symbols ... --tf ...</code> (ou o menu Estratégias → Validar) "
                "para preencher esta matriz.</p>")
    symbols = [r["symbol"] for r in rows] or sorted(set(validations["symbol"]))
    head = "".join(f"<th>{_e(REGISTRY[k].info.name)}</th>" for k in REGISTRY)
    out = [f"<table><tr><th>Ativo</th>{head}</tr>"]
    for symbol in symbols:
        cells = []
        for key in REGISTRY:
            hit = validations[(validations["symbol"] == symbol) & (validations["strategy"] == key)]
            if hit.empty:
                cells.append("<td class='none'>não validada</td>")
                continue
            v = hit.iloc[0]
            cls, label = ("ok", "✅ aprovada") if bool(v["approved"]) else ("bad", "❌ reprovada")
            pf, trades = v["profit_factor_oos"], int(v["trades_oos"])
            detail = (f"PF {'∞' if pf >= 999 else _num(pf)} · {trades} op. · DD p95 {_num(v['mc_dd'] * 100, 1)}%"
                      if trades else "sem operações fora da amostra")
            cells.append(f"<td title='{_e(v['status'])}'><span class='badge {cls}'>{label}</span><br>"
                         f"<small>{detail}</small></td>")
        out.append(f"<tr><td><b>{_e(symbol)}</b></td>{''.join(cells)}</tr>")
    out.append("</table><p class='muted'>Passe o mouse sobre uma célula para ver os motivos. "
               "PF = profit factor fora da amostra; op. = operações fora da amostra; "
               "DD p95 = drawdown máximo no percentil 95 do Monte Carlo.</p>")
    return "".join(out)


def _strategies_table() -> str:
    out = ["<table><tr><th>Estratégia</th><th>Regime</th><th>Acerto típico</th><th>Risco:retorno</th>"
           "<th>Complexidade</th><th>Timeframes</th><th>Mercados</th></tr>"]
    for cls in REGISTRY.values():
        i = cls.info
        out.append(f"<tr><td><b>{_e(i.name)}</b><br><small>{_e(i.summary)}</small></td><td>{_e(i.regime)}</td>"
                   f"<td>{_e(i.win_rate)}</td><td>{_e(i.reward_risk)}</td><td>{_e(i.complexity)}</td>"
                   f"<td>{_e(', '.join(i.timeframes))}</td><td>{_e(i.markets)}</td></tr>")
    out.append("</table>")
    return "".join(out)


def render_report(rows: List[dict], validations: pd.DataFrame, tf, missing: List[str] = ()) -> str:
    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    valid = sum(1 for r in rows if r["valid"])
    missing_html = ""
    if missing:
        missing_html = (f"<p class='notice'>Sem dados em {tf.name} para: {_e(', '.join(missing))}. "
                        f"Extraia com <code>mt5x extract --symbols {_e(','.join(missing))} --tf {tf.name} "
                        "--from 2020-01-01</code>.</p>")
    avoid = "".join(f"<li><b>{_e(n)}</b>: {_e(why)}</li>" for n, why in AVOID)
    return f"""<!doctype html>
<html lang="pt-BR"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Relatório de estratégias {tf.name}</title><style>{CSS}</style></head>
<body><main>
<h1>Relatório de estratégias — {tf.name}</h1>
<p class="sub">Gerado em {now} · {len(rows)} ativo(s) analisado(s), {valid} apto(s) no Filtro A.</p>
<p class="notice"><b>Importante:</b> resultados de backtest não garantem resultados futuros. Use este relatório
para descartar o que não funciona e priorizar estudos, não como recomendação de investimento.</p>
{missing_html}
<h2>1. Triagem de ativos (Filtros A e B)</h2>
<p class="muted">Filtro A elimina ativos caros, ilíquidos, com dados ruins ou pouco histórico (recalcule semanalmente).
O score (0–100) do Filtro B mede a oportunidade agora: regime, força, volatilidade, custo, eficiência,
eventos e correlação com a carteira (recalcule diariamente).</p>
<div class="card">{_screen_table(rows)}</div>
<h2>2. Matriz ativo × estratégia (Filtro C)</h2>
<p class="muted">Aprovada = profit factor fora da amostra acima do mínimo, operações suficientes, maioria das janelas
walk-forward positivas, robusta a ±20% nos parâmetros e drawdown do Monte Carlo dentro do limite.</p>
<div class="card">{_matrix(rows, validations)}</div>
<h2>3. As seis estratégias</h2>
<p class="muted">Taxas de acerto e risco:retorno são referências típicas, não promessas.</p>
<div class="card">{_strategies_table()}</div>
<h2>4. Evite no início</h2>
<div class="card"><ul>{avoid}</ul></div>
</main></body></html>
"""
