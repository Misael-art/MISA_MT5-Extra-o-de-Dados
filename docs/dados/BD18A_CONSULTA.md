# BD18a — Consulta inicial executável — PARCIAL

## Estado atual (23/10/2026, 5ª revisão — V5-01..02)

- **V5-01:** o carimbo de conhecimento era calculado antes do UPDATE/commit da sua própria
  transação; uma nova consulta no mesmo corte mudava quando ele confirmava. Agora o carimbo é
  transação serializada pelo contador do store, com horário tomado sob o bloqueio e sempre
  posterior à fronteira confirmada (`knowledge_frontier`), que avança na mesma transação. A
  fotografia e o cursor fixam geração **e** fronteira; `knowledge` usa
  `known_at <= min(corte, fronteira)`; corte além da fronteira é limitado e declarado
  (`knowledge_horizon`). Ordem lógica = geração/fronteira; horário físico = `known_at`, que é sempre
  posterior ao commit dos dados (conservador). Legado: sem geração, nunca conhecido.
- **V5-02:** `save_value` escolhia a revisão antes do bloqueio; dois escritores colidiam com
  `IntegrityError`. Agora a exclusão (SQLite `BEGIN IMMEDIATE`; PG `FOR UPDATE` no contador) vem antes
  de ler a revisão vigente, em `save_value` e `save_values`: escritores entram em fila, retries iguais
  não duplicam, conteúdos distintos geram revisões na ordem confirmada.
- Provas: `tests/test_v5_store_contract.py` (barreiras dentro do carimbo e antes do contador; falham
  no código anterior), `tests/test_v5_query_contract.py` (páginas, mesmo corte, duas
  características, SDK, CLI, leitor sem escrita) e `_pg.py` (CI).

## 4ª revisão (23/10/2026, V4-01..03)

- **V4-01:** `save_value` gravava sem horário (NULL igual a legado); agora `save_value` e
  `save_values` usam o mesmo caminho de escrita (#201).
- **V4-02:** a fotografia por horário admitia uma revisão confirmada depois da 1ª página
  (páginas [1,2,300]). Agora cada transação reserva uma *geração* num contador serializado até o
  commit; `store_snapshot` lê a última geração confirmada; páginas e características usam a mesma
  geração e o mesmo instante; `knowledge` exige commit carimbado (carimbo perdido é reparado pelo
  próximo escritor, sempre com data posterior). Relação: escrita → commit (visibilidade) →
  carimbo `known_at` (conhecimento); `recorded_at` é só o relógio do escritor.
- **V4-03:** o modo somente leitura SQLite concatenava o caminho na URL (`study #1.sqlite` abria
  `study `, criando-o). Agora abre `Path.resolve().as_uri()?mode=ro` pelo `sqlite3`; o escritor usa
  `URL.create` (nome com `?` também quebrava). Achado fora do escopo: `resources/actions.py`
  ainda monta URL SQLite por texto (dono T15.x).
- Provas: `tests/test_v4_store_contract.py`, `tests/test_v4_query_contract.py`,
  `tests/test_v4_read_only_paths.py` e wrappers PG (CI). Limite novo: escritores do store ficam
  serializados pelo contador até o commit (concorrência de escrita reduzida, leitura não bloqueada).

## Contrato v2 (23/10/2026, 3ª revisão)

A 3ª revisão reproduziu cinco defeitos na v1 (log `v3_contraprovas_antes_fe92ed1.txt` na
fotografia): reconstrução sem distinção de conhecimento real (18 fatos, 17 VALID, onde o bruto
causal tinha 0), unidade atual aplicada a versão antiga, DDL/INSERT numa consulta (falha com
`query_only`), cursor de outra base aceito, exceções cruas e filtros duplicados.

Correções (sonda `v3_contraprovas_depois_*.txt`): `basis` research/knowledge com
`recorded_at`/`knowledge_state`/`reconstructed` e motivo (store: coluna aditiva `recorded_at_utc`,
PR próprio T13.2); definição por versão gravada separada do contrato vigente; leitura pura
(`datasets.read_identity`, `DatabaseManager(read_only=True)`, CLI somente leitura); cursor ligado à
base/basis/seleção com fotografia `recorded_through`; validação com códigos estáveis. Provas:
`tests/test_v3_query_contract.py` (11) e `_pg.py` (CI).

Limites mantidos: a seleção ainda é carregada inteira antes de paginar (teto de resposta
200 000, não garantia de memória — BD18b); `knowledge` usa a gravação do fato derivado como limite
superior das dependências (gravado depois dos recebimentos), não a cadeia detalhada de cada
fonte; fatos anteriores a `recorded_at_utc` são `recording_unknown`.

## Recorte inicial (23/10/2026, v1, preservado)


Vinculação T18.6 (consulta local/DTO) e SDK T10.7. Depende de BD06/BD08a.

`analytics/query_service.series` e `sdk.query_series`/`sdk.query_catalog` (documentados em
[docs/sdk/api.md](../sdk/api.md)): uma seleção devolve envelope `query_schema_version=1` com a
identidade da base (`datasets.identity`, R04: a camada pública passa a carimbar a base), o
contexto temporal (`offset_state`), o contrato BD06 das características, os fatos disponíveis no
corte as-of vindos do store, paginação por cursor ligada à seleção, frescor, motivos e erros
estruturados; JSON sem NaN/infinito; leitura nunca calcula nem grava.

## Provas ([tests/test_bd18a_query.py](../../tests/test_bd18a_query.py), 5 casos, cadeia do BD08a)
Envelope = store (`query_as_of`) fato a fato; base `mt5@Fake-Demo` e offset declarado; páginas de
97 concatenadas = consulta única; cursor de outra seleção recusado; as-of anterior à
disponibilidade → vazio com motivo; erros `NAIVE_TIME/UNKNOWN_FEATURE/BAD_PAGE_SIZE/BAD_CURSOR`
sem escrita; SDK = serviço.

## Limites
Só características do registry (produtores T13.3 e compostos: BD18b); relações ausentes;
seleção carregada inteira antes de paginar, com teto de 200 000 fatos (`SELECTION_TOO_LARGE`) —
paginação no banco fica para BD18b; sem servidor HTTP.
