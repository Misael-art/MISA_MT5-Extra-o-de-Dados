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
o contrato completo de exportação e o Estudo mantêm recortes pendentes (BD19a/b/c2/d).

## BD19c2-PERSISTED — contexto e CSV de definições persistidas

O adaptador existente `analytics.indicator_context` passa a resolver IDs e versões pelo catálogo
canônico. `features context/export --features ID --algorithm-version V` lê fatos de indicadores
sem fórmula local, com unidades e parâmetros registrados nessa versão. Sem versão explícita,
o padrão legado continua sendo a implementação local atual; um ID sem calculador exige a versão
e orienta a consultar `features catalog`. Não há escolha automática da versão mais recente.
M1/W1/MN1 são aceitos para a leitura explícita; ler esses fatos não exige produzir barras derivadas.

O contexto reutiliza `query_history` + `asof_join`: cada decisão só recebe revisões já disponíveis
naquele instante. Uma correção tardia de uma observação antiga não substitui a observação mais
recente. A projeção mantém o retrato `query_as_of`, sem preencher buracos com valores de outra
barra. Fatos órfãos não usam a fórmula local como contrato substituto.

O CSV preserva valores categóricos/booleanos, além dos numéricos: antes, a projeção usava apenas
`numeric_value`, gerando célula vazia apesar de estado AVAILABLE para categorias válidas. Ausência,
expiração e zero continuam distintos. O exportador atômico e seus guards são reutilizados;
falha na seleção não sobrescreve um destino existente. Esse recorte não encerra o contrato
completo de BD19c2, dependente de compostos/relações/BD18b, nem o consumo de snapshots pelo Laboratório.

Validação de29/10:baseline24; rodada final119 testes distintos (materialização, contexto/CSV,
Alligator, AO/AC, pivôs e Donchian), doctor offline16/2avisos/0. Launcher oficial/CLI com base
sintética própria: indicador sem calculador devolve12 percent; categoria `buy` reaparece no CSV
com estado AVAILABLE. Testes também cobrem zero, false, ordem/duplicação de decisões, expiração,
revisão tardia, versão ausente e preservação do destino. Nenhuma base do proprietário foi fixture.

Bancada sintética reutilizável de5.000 fatos/500 decisões, três chamadas aquecidas, frames
numéricos idênticos: contexto187→148ms e projeção195→167ms (medianas nesta rodada). Amostras
se sobrepõem e variam com a carga do host; não constituem promessa de aceleração. Valores são
lidos diretamente das colunas, sem converter cada linha em outro dicionário. Recibos locais
`state/qa-tmp/catalog-store/context-*`; sem aceite MT5/Wine/Windows físico ou financeiro.
