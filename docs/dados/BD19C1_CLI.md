# BD19c1 — Primeiro consumo pela CLI (23/10/2026) — PARCIAL

Vinculação T18.6 (CLI/consulta local). Depende de [BD18a](BD18A_CONSULTA.md).

`mt5x analytics features series --symbol S --tf TF --feature ID [--feature ID…] --as-of T
[--from/--to] [--page-size N] [--cursor C]` delega a `query_service.series`, sem cálculo próprio.
Texto: base (namespace, fonte), estado do fuso, fatos e cursor. `--json`: `data.envelope` é o
envelope do serviço. Seleção inválida devolve código de saída de uso e o erro estruturado.

## Prova ([tests/test_bd19c1_cli.py](../../tests/test_bd19c1_cli.py))
Sobre a cadeia do piloto (provedor falso → extrator → M5 → store), a CLI `--json` devolve os
mesmos valores, base e definições que `sdk.query_series`; horário sem fuso → `[NAIVE_TIME]`.

## Limites
Laboratório, Relações, export e Estudo ainda não consomem o serviço (BD19a/b/c2/d).
