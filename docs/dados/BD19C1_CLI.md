# BD19c1 — Primeiro consumo pela CLI (23/10/2026) — PARCIAL

Vinculação T18.6 (CLI/consulta local). Depende de [BD18a](BD18A_CONSULTA.md).

`mt5x analytics features series --symbol S --tf TF --feature ID [--feature ID…] --as-of T
[--from/--to] [--algorithm-version V] [--page-size N] [--cursor C]` delega a
`query_service.series`, sem cálculo próprio.
Texto: base (namespace, fonte), estado do fuso, fatos e cursor. `--json`: `data.envelope` é o
envelope do serviço. Seleção inválida devolve código de saída de uso e o erro estruturado.

`mt5x analytics features catalog` expõe o catálogo unificado do mesmo serviço, incluindo
versões persistidas sem fórmula local e conflitos de metadados. `--json` devolve o mesmo
envelope de `sdk.query_catalog(db)` após serialização JSON. `features list/show` conservam
seu papel de inspeção do registro armazenado. Novos fatos/definições não são calculados pela leitura.

## Prova ([tests/test_bd19c1_cli.py](../../tests/test_bd19c1_cli.py))
Sobre a cadeia do piloto (provedor falso → extrator → M5 → store), a CLI `--json` devolve os
mesmos valores, base e definições que `sdk.query_series`; horário sem fuso → `[NAIVE_TIME]`.

## Limites
O inventário de Relações consome o leitor canônico de definições. A análise de Séries/relações,
export e Estudo ainda precisam do consumo causal dos fatos (BD19a/b/c2/d).
