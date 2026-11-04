# BD18a — Consulta inicial executável (23/10/2026) — PARCIAL

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
