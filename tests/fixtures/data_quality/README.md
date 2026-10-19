# Evidências de compatibilidade e qualidade (T20.6)

`registry_definitions.json` foi extraído por `get_definition` do registry T18.1
no commit f45618f9059feb8e0f54fa75f70f8159995891ab. Contém quatro definições reais,
com hash da fonte e sua revisão; não é outro registry de execução. O adaptador
precisa preservar a diferença entre real volume, tick volume e fechamento.

`dataset_v2.json` foi produzido por `datasets.capture/export_snapshot` do
commit 16eddef5576043ccbfc83469fcb8f1bbd42d21b9 (T20.5). O processo conferiu
VERSION=2 e a origem do módulo no checkout anterior. QA H1, duas barras sintéticas
e especificação atual; nenhuma conta, conexão, caminho ou credencial. O teste
importa e exporta os mesmos bytes sem herdar ticks que existam na base receptora.
O formato1 continua coberto pelo fixture real anterior em ../datasets/v1.json.

`native_quality_row.html` conserva somente a linha de qualidade do relatório
real do ensaio MetaTester6272/Wine da T20.3 (EURUSD H1 MetaQuotes-Demo, 2025-01-06).
A linha declara `100% real ticks`; não é uma declaração apenas do modelo4.
Não inclui conta ou demais linhas do relatório. O arquivo original em UTF16
foi lido pelo decodificador existente; o recorte é UTF8. Só prova o campo daquele
ensaio, não todos os Experts ou uma fila de ordens. Prova/limites do ensaio:
../equity_reference/README.md.

SHA-256 do relatório original (não publicado aqui):
88b3511b9c8be35d1f9c9bf9b24cb8faf6f952c0fbe53b51f0b4c8e75718e67e

SHA-256 do texto decodificado:
0ec463ebf58c847a5df0ec680c9377774d9d927057b66d068de39aeb2e0d891f

Hashes dos fixtures:

- `dataset_v2.json`: `ca8adf794669dc28b55410ecdd4e5dac4e8052fa219cffb4f4611b4942d18e6c`
- `native_quality_row.html`: `35b1285928a126442f54a6a921c6f57fed463221197173ae886d7302dc7b2904`
- `registry_definitions.json`: `dd47686328a3d7cba810fdf1fbea474bbe8c2ec48a08e98b1bcddc4735706cdf`
