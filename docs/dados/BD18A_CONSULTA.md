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

Cursores v3 (v2 na entrega de28/10, substituído pela paginação SQL abaixo) vinculam a seleção à identidade/contexto da base, aos metadados e aos fatos no
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
Prévia publicada `03d5e6a`/PR214:273 arquivos por hash, launcher repetido e detalhes das duas
versões conferidos em1280×850 após completar a rolagem da área expandida.

Bancada local de5.000 fatos, página500, três chamadas aquecidas: medianas166ms na consulta
publicada anterior e180ms na atual; valores iguais. Catálogo8ms/inventário13ms. A verificação
de geração acrescenta custo; não se declara ganho de desempenho. Recibos locais em
`state/qa-tmp/catalog-store/`; sem dados ou imagens privados no Git. UI Linux/Tk em display
virtual não qualifica Windows físico, ponte, terminal ou estratégias.

## Limites
Definições persistidas agora participam do catálogo/consulta; compostos e relações continuam ausentes;
o teto de200.000 fatos selecionados (`SELECTION_TOO_LARGE`) permanece; sem servidor HTTP.

## Paginação SQL por chave (BD18a-SQL, 29/10/2026)

A página é cortada pelo SQL com `LIMIT page_size+1`, depois da seleção da revisão disponível
e da validade, usando a chave `(feature_id, observation_time, algorithm_version)`. A próxima
consulta usa essa chave como fronteira; não há OFFSET nem DataFrame de toda a seleção antes
do corte. SQLite usa collation BINARY e PostgreSQL C, para conservar a ordem lexical do contrato
também com maiúsculas, acentos e versões como2/10. A regra de revisão/expiração é a mesma do
`feature_store.query_as_of`, cuja API de DataFrame completo permanece compatível.

Catálogo, identidade, fingerprint e página usam uma única `consistent_read` do DatabaseManager
existente. Uma gravação entre fingerprint e leitura da página não mistura fatos/contratos de
momentos diferentes. A inicialização pública da identidade R04 continua anterior à transação;
`datasets.identity(...connection=...)` apenas lê os metadados nessa imagem, sem DDL.

A geração do cursor ainda depende de **todos os fatos selecionados no corte**, inclusive fora
da página. Essa verificação percorre o resultado em lotes de256, sem reter suas linhas; considera
todas as colunas canônicas, não confia apenas em `value_hash` armazenado. Metadados por versão
abrangem a seleção inteira já na primeira página. O orçamento é conferido durante a leitura;
excedê-lo recusa o pedido antes de devolver uma página parcial. O banco ainda pode ordenar ou
materializar sua execução: o lote limita o buffer Python, não promete custo constante do SQL.

Cursores v3 identificam esse fingerprint. Formatos v1/v2 exigem reinício sem cursor (`BAD_CURSOR`);
`STALE_CURSOR` continua significando que base/contexto/contratos/conteúdo no corte mudaram.
Revisão disponível somente depois do corte não invalida a paginação histórica. A CLI e o SDK
recebem o mesmo envelope anterior, salvo o cursor opaco atualizado. Não calcula fatos, cria
outro store, altera tabelas de usuário ou completa as capacidades de compostos/relações BD18b.

Baseline53 aprovados; rodada integrada150 aprovados e1 PostgreSQL pendente no host sem servidor
dedicado. Inclui SQL limitado sem `read_sql` global, fronteira de microssegundos, ordem lexical,
metadados de versões fora da página, teto sem resposta parcial, revisão futura, expiração sem
fallback, nulos e corrupção não finita, leitura sem DDL e gravação concorrente WAL entre
fingerprint e página. O caso de ordem lexical também está no job PostgreSQL dedicado; compilação
ou SQLite não o certificam. Doctor offline16/2avisos/0 e CLI oficial: páginas1/2 retornam
0,0/0,0001/0,0002, com primeiro envelope igual ao SDK.

A mesma bancada5.000 fatos/página1,3 chamadas aquecidas, mediu178→110ms de mediana e pico de
alocações Python9.588.764→439.807bytes (medição separada por tracemalloc). Antes eram5.000 linhas
em DataFrame para devolver1; depois, nenhuma leitura `read_sql` de toda a seleção. Todos os
campos do envelope conferem, exceto o cursor atualizado. É uma medição local com a carga deste
host; não promete ganho universal nem mede a memória interna do banco. Recibos locais em
`state/qa-tmp/query-pagination/`, com bancada anterior reutilizada e sem dados privados.
