# BD03 — Identidade de bases e fontes (primeiro recorte: mapa e prova do contrato existente)

Recorte de 2026-10-22, vinculado a T20.4/T20.5/T20.6. Depende de [BD01](BD01_INVENTARIO.md) e
[BD02](BD02_BANCADA.md). **Sem schema novo**: o plano exige mapear leitores/escritores antes de
alterar chaves. Este recorte mapeia, prova o que já é garantido e delimita o que falta.

## Contrato existente (T20.4)

- **Base = arquivo de banco.** `_dataset_origin` guarda um `namespace` (UUID gerado uma vez por
  banco, slot `default`), estável entre reinícios e independente do caminho do arquivo.
- **Fonte declarada no manifesto.** `datasets.capture(..., source={provider, server, derivation})`
  grava rótulos sem caminhos, contas ou credenciais; ausente → `provider=local_database`,
  `server_status=unknown` (não inventa feed).
- **Cruzamento explícito.** `export_snapshot`/`import_snapshot` transportam o manifesto com o
  `namespace` da origem; a base que importa conserva o próprio namespace e os próprios dados.

## Provas ([tests/test_bd03_base_identity.py](../../tests/test_bd03_base_identity.py), 4 casos)

| Caso | Resultado |
|---|---|
| Mesmo símbolo `EURUSD` em dois bancos com dados diferentes | namespaces, manifestos, barras e valores do store distintos |
| Reabrir o banco | mesmo namespace |
| Importar snapshot de B em A | manifesto importado mantém namespace/provedor de B; captura local de A inalterada |
| Copiar o arquivo do banco | **a cópia herda o namespace** (risco medido, abaixo) |

## Leitores e escritores das chaves (symbol/tf) — onde o namespace não chega

| Estrutura | Chave atual | Carrega identidade de base? |
|---|---|---|
| `_feature_values` (store T13.2) | symbol, tf, observation_time, feature_id, algorithm_version, revision | não; isolamento só pelo arquivo |
| Escritores: `producers`, `indicator_materializer` | symbol/tf | não |
| Leitores: CLI `query_as_of`, `indicator_context` | symbol/tf | `indicator_context` anota `run_manifest_id` quando o run tem manifesto |
| `_bars_derived`, `_market_calendar`, `_analytics_producer_*` | symbol/tf | não |
| `relation_dto` | séries por feature_id | não |
| `chart_study` (notas/capturas) | symbol/tf | não |
| ledger `trade_analytics` | run/símbolo | indireto via manifesto do run (`robots/store`) |
| `trade_analytics/cache` | manifesto | sim (`manifest_id`) |

## Lacunas e riscos (sem correção neste recorte)

1. **Valores sem identidade no envelope.** Uma consulta ao store devolve fatos sem `namespace`;
   um consumidor que junte resultados de dois bancos não consegue rotular a origem. Dono: BD18
   (envelope de consulta) e BD19 (consumidores).
2. **Cópia de arquivo herda o namespace.** Duas cópias que divergirem não se distinguem. Proposta
   a decidir: operação explícita de “derivar base” que gere novo namespace e registre a origem.
3. **Duas bases no mesmo banco não são suportadas.** Mesmo símbolo de dois feeds no mesmo arquivo
   colide nas chaves acima. Atender exigiria acrescentar a identidade às chaves de ~10 tabelas.

## Decisão de arquitetura pendente

Manter **uma base por banco** (contrato T20.4 atual, sem migração de chaves; cruzamento por
snapshot importado e envelope com namespace) ou **várias bases por banco** (migração aditiva de
chaves). Recomendação deste recorte: manter uma base por banco e propagar o namespace aos
envelopes de consulta/DTO, porque não exige reescrever chaves nem arrisca o banco principal.
Até a decisão, BD04/BD05 seguem sobre o contrato atual, que não muda com nenhuma das opções.
