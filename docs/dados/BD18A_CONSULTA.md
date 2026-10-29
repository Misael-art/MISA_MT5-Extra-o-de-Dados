# BD18a — Consulta inicial executável (23/10/2026) — PARCIAL

Vinculação T18.6 (consulta local/DTO) e SDK T10.7. Depende de BD06/BD08a.

`analytics/query_service.series` e `sdk.query_series`/`sdk.query_catalog` (documentados em
[docs/sdk/api.md](../sdk/api.md)): uma seleção devolve envelope `query_schema_version=1` com a
identidade da base (`datasets.identity`, R04: a camada pública passa a carimbar a base), o
contexto temporal (`offset_state`), o contrato BD06 das características, os fatos disponíveis no
corte as-of vindos do store, paginação por cursor ligada à seleção, frescor, motivos e erros
estruturados; JSON sem NaN/infinito; leitura não calcula nem altera fatos/definições. O carimbo
de identidade acima é a escrita de metadados já prevista em R04, não materialização de valores.

## Catálogo persistido e versões (28/10/2026)

`definition_catalog(db=None)` reúne registry e todas as definições/versões persistidas, como
fonte única também para o inventário do Laboratório. Esse leitor interno não cria schema nem
namespace. `catalog(db)`/`sdk.query_catalog(db=None)` expõem o envelope público; a chamada sem
argumentos conserva a descoberta offline. Indicadores sem calculador local são consultáveis
pelos fatos e permanecem `computable=False`; ausência de fórmula não é ausência de dados.

As definições da seleção trazem `variants` com parâmetros, unidades, tipos, aquecimento,
disponibilidade, fonte e situação do nome declarados por versão. Campos ausentes não são
reconstituídos por uma fórmula de outra versão. Divergências com o registry são explícitas;
fatos sem definição registrada são recusados (`MISSING_DEFINITION`). Categorias/booleanos
mantêm a representação do store, sem conversão numérica implícita. Não se escolhe a última versão.

Cursores v2 vinculam a seleção à identidade/contexto da base, aos metadados e aos fatos no
corte as-of. Troca de base ou revisão desse conteúdo recusa a continuação (`STALE_CURSOR`);
revisão posterior ao corte não altera a seleção histórica. Reiniciar sem cursor descarta páginas
anteriores; cursores legados também precisam desse reinício. IDs repetidos são deduplicados.
O inventário verifica a geração do catálogo e aplica ativo/TF à mesma série, limpa detalhes
ao carregar/errar e descarta respostas de buscas antigas.

## Provas ([tests/test_bd18a_query.py](../../tests/test_bd18a_query.py), 5 casos, cadeia do BD08a)
Envelope = store (`query_as_of`) fato a fato; base `mt5@Fake-Demo` e offset declarado; páginas de
97 concatenadas = consulta única; cursor de outra seleção recusado; as-of anterior à
disponibilidade → vazio com motivo; erros `NAIVE_TIME/UNKNOWN_FEATURE/BAD_PAGE_SIZE/BAD_CURSOR`
sem escrita; SDK = serviço.

O incremento de28/10 acrescenta [test_bd18_stored_catalog.py](../../tests/test_bd18_stored_catalog.py):
duas versões sem fórmula local, unidades/parâmetros distintos, categorias/booleanos e códigos
de direção numéricos com semântica categórica preservada, revisão futura versus revisão no corte,
outra base, cursor malformado, definição ausente, filtro ativo/TF e compatibilidade SDK/CLI.
Rodada final:64 testes de consulta/contrato/CLI/SDK/inventário e25 Tk; doctor offline16/2avisos/0.
Launcher oficial offline com banco/preferências sintéticos: catálogo e busca vazia inspecionados,
sem exceções e sem tarefas pendentes; CLI real = SDK para catálogo e versão2 com valores12/13/14/15
em percent, enquanto a versão1 conserva fraction e parâmetros diferentes.

Bancada local de5.000 fatos, página500, três chamadas aquecidas: medianas166ms na consulta
publicada anterior e180ms na atual; valores iguais. Catálogo8ms/inventário13ms. A verificação
de geração acrescenta custo; não se declara ganho de desempenho. Recibos locais em
`state/qa-tmp/catalog-store/`; sem dados ou imagens privados no Git. UI Linux/Tk em display
virtual não qualifica Windows físico, ponte, terminal ou estratégias.

## Limites
Definições persistidas agora participam do catálogo/consulta; compostos e relações continuam ausentes;
seleção carregada inteira antes de paginar, com teto de 200 000 fatos (`SELECTION_TOO_LARGE`) —
paginação no banco fica para BD18b; sem servidor HTTP.
