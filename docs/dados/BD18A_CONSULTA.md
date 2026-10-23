# BD18a — Consulta inicial executável — PARCIAL

## Estado atual (23/10/2026, contrato v2 após a 3ª revisão)

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
