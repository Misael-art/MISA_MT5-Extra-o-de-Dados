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

## Decisão de arquitetura (engenharia, 23/10/2026)

**Uma base lógica por banco SQLite ou por schema/banco PostgreSQL**, identificada pelo
conteúdo (`_dataset_origin`), nunca pelo nome do arquivo. Fundamento: o resultado pedido é que
bases não se confundam em valores, cache, DTO e notas; as chaves por símbolo já são isoladas
pelo banco/schema, e migrar ~10 tabelas não traria garantia adicional. A identidade passa a ser
explícita nas bordas. Compatibilidade futura: se várias bases num mesmo banco forem pedidas, o
namespace já está nos envelopes e pode virar coluna de chave por migração aditiva.

## Segundo recorte — mudanças de produção (correção V01)

| Mecanismo | Arquivo | Comportamento |
|---|---|---|
| `datasets.identity(db)` | `analytics/datasets.py` | namespace, linhagem (`original`/`fork`, `parent_namespace`), fonte declarada e backend; reabrir/mover/copiar como backup não gera UUID |
| `datasets.fork(db)` | idem | cópia que vai divergir ganha namespace novo e registra a origem (slot `parent`, sem tabela nova) |
| `datasets.declare_source(db, source)` e `capture` | idem | 1ª declaração fixa provider/server da base; fonte diferente é **recusada** antes de gravar; captura sem rótulo herda a fonte declarada |
| `feature_store.with_base_identity(db, frame)` | `analytics/feature_store.py` | carimba `base_namespace/lineage/provider/server` em fatos lidos antes de juntar bancos |
| notas do Estudo | `chart_study/store.py` | nota nova congela `base` na fotografia; nota já salva mantém seus bytes |
| `relation_dto` | `analytics/relation_dto.py` | `provenance.base_namespace` obrigatório fora de fixture sintética (fixture declara `None`) |

Schema: nenhuma tabela/coluna nova; só novas linhas (slots) em `_dataset_origin`.
Cópia sem fork que diverge mantém o namespace, mas o `manifest_id` (que inclui namespace e
conteúdo) muda: cache e contexto por manifesto não colidem.

## Provas do segundo recorte (11 casos SQLite + 2 PostgreSQL)

Mesmo símbolo com dados diferentes **e iguais** consultados juntos (namespaces, versões e chaves
de cache distintos); mover/backup/reabrir conservam a identidade; fork deriva e registra origem;
cópia divergente distinguida por fork ou por revisão de conteúdo; feed/servidor incompatível
recusado sem gravar manifesto; terceira fonte (`csv-import`) pelo mesmo contrato chega carimbada
(E04); notas congelam a base; DTO exige base. `tests/test_bd03_base_identity_pg.py` repete
isolamento por schema, reabertura, recusa e fork em PostgreSQL — **executado só na CI** (sem
PostgreSQL local).

## Ainda pendente (BD03 continua parcial)

- Escritores brutos (`save_ohlcv_data`, extratores) não consultam a fonte declarada: a recusa
  acontece na captura/manifesto, não na gravação bruta. Ligar o servidor da conta ao
  `declare_source` no extrator pertence ao dono da extração (T1.x/EXTR) e é o próximo passo.
- Consumidores (CLI, Laboratório, export) ainda não chamam `with_base_identity`: BD18/BD19.
- Ledger referencia a base só via manifesto do run.
