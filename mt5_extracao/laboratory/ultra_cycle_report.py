"""Leitura por ciclo do mesmo confronto emitido pela CLI e pelo executor."""
from html import escape
import json
from pathlib import Path


def _records(title, rows):
    if not rows:
        return ""
    content = []
    for row in rows:
        fields = ((key, json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else str(value))
                  for key, value in row.items())
        content.append("<dl>" + "".join(f"<dt>{escape(k)}</dt><dd>{escape(v)}</dd>" for k, v in fields) + "</dl>")
    return f"<details><summary>{escape(title)} ({len(rows)})</summary>{''.join(content)}</details>"


def write_report(result, path):
    """Aceita somente o resultado do comparador e grava um documento novo."""
    if result.get("origin") != "strategy_tester_orders":
        raise ValueError("A trilha de ordens exige a origem strategy_tester_orders.")
    title = "Ciclos de execução no Strategy Tester"
    parts = [f"""<!doctype html><html lang="pt-BR"><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>{title}</title>
<style>body{{max-width:64rem;margin:auto;padding:1rem;color:#1b2631;font:1rem/1.5 system-ui}}
summary,a{{padding:.7rem 0;cursor:pointer;display:list-item}}details{{margin:.5rem 0}}
dl{{border-left:3px solid #678;padding-left:1rem}}dt{{font-weight:600}}
dd{{margin:0;overflow-wrap:anywhere}}:focus-visible{{outline:3px solid #166ac2}}</style><main><h1>{title}</h1>"""]
    parts.append(_records("Identificação e resultado", [{k: result[k] for k in
        ("status", "modeled_status", "native_order_parity", "package_hash", "terminal_build", "journal_sha256", "platform_error", "no_order_reason")
        if k in result}]))
    for key, label in (("modeled_first_divergence", "Primeira divergência do livro"),
                       ("platform_first_divergence", "Primeira divergência das ordens")):
        if result.get(key):
            parts.append(_records(label, [result[key]]))
    for key, label in (("policy_limits", "Limites e unidades do pacote"), ("platform_account", "Conta no início"),
                       ("platform_report_comparison", "Conferência do relatório MT5"), ("platform_final", "Conta no encerramento")):
        if result.get(key):
            parts.append(_records(label, [result[key]]))
    parts.append(_records("Eventos da carteira", result.get("global_events", [])))
    cycles = result.get("cycles", [])
    if not cycles:
        parts.append("<p>Nenhum ciclo identificado nesta comparação.</p>")
    for cycle in cycles:
        heading = f'{cycle["owner"]} · {cycle["cycle_id"]}'
        parts.append(f'<details><summary>{escape(heading)}</summary>')
        parts.append(_records("Regra congelada", [dict(cycle["rule"], cycle_version=cycle["version"])]))
        for key, label in (("events", "Decisões e orçamento no tick"), ("vetoes", "Vetos de execução"),
                           ("requests", "Solicitações e respostas"), ("transactions", "Transações recebidas"),
                           ("history", "Ordens no histórico"), ("deals", "Negócios e resultado"),
                           ("snapshots", "Posição, proteção e conta por tick")):
            parts.append(_records(label, cycle[key]))
        parts.append("</details>")
    parts.append("</main></html>")
    with Path(path).open("x", encoding="utf-8") as handle:
        handle.write("".join(parts))
    return str(Path(path).resolve())
