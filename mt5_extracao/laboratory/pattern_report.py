"""Relatório portátil das consultas T13.4 já persistidas; nenhuma avaliação aqui."""

from html import escape
import json


def render(result):
    rows = result if isinstance(result, list) else [result]
    cards = []
    for item in rows:
        receipt = item["receipt"]
        title = " · ".join(str(receipt[key]) for key in ("symbol", "timeframe", "family"))
        identity = " · ".join(str(receipt[key]) for key in
                              ("source_namespace", "protocol_id", "request_id", "data_manifest_id"))
        facts = item.get("facts")
        content = (f"<article><h2>{escape(title)}</h2><p>Exploratória · sem aprovação estatística, "
                   f"econômica ou reserva final qualificada.</p><p>{escape(identity)}</p>"
                   f"<p>Disponível desde {escape(item['available_at_utc'])}; "
                   f"{receipt['event_count']} evento(s), {receipt['negative_count']} negativo(s), "
                   f"{receipt['censored_count']} censurado(s).</p>"
                   f"<details><summary>Recibo e fontes</summary><pre>{escape(json.dumps(receipt, ensure_ascii=False, indent=2))}</pre></details>")
        if facts is not None:
            content += ("<details><summary>Fatos persistidos</summary><pre>" +
                        escape(json.dumps(facts, ensure_ascii=False, indent=2)) + "</pre></details>")
        cards.append(content + "</article>")
    body = "".join(cards) if cards else "<p>Nenhuma avaliação persistida no filtro e corte escolhidos.</p>"
    return ("<!doctype html><html lang='pt-BR'><meta charset='utf-8'><title>Avaliações T13.4</title>"
            "<style>body{font:16px system-ui;max-width:80rem;margin:2rem auto;padding:0 1rem}"
            "article{border:1px solid #aaa;border-radius:.5rem;padding:1rem;margin:1rem 0}"
            "pre{white-space:pre-wrap;overflow-wrap:anywhere}summary{cursor:pointer}</style>"
            "<h1>Avaliações T13.4 persistidas</h1><p>Consulta de pesquisa; sem ordens ou recomendação.</p>"
            + body + "</html>")
