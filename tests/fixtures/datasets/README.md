# Snapshot anterior ao histórico de instrumentos

`v1.json` foi produzido pelo `datasets.capture`/`export_snapshot` da T20.4,
commit `c991b04c85029824e74a0861fb82947ade3d227c`, antes da extensão de formato 2.
O processo conferiu `datasets.VERSION == 1` e a origem do módulo no checkout
anterior. Não foi construído removendo campos de um pacote novo.

Dados sintéticos: QA H1, duas barras em 2025-01-06 e uma especificação atual.
Não há conta, caminho pessoal, conexão ou credencial no pacote. O instante de
atualização da especificação é observação local, sem vigência retroativa.

Manifesto: `141f9d8e5bf1ff94fd6961b99ee67d55a40a2b47c1b5a24854ff92cdbd86f57e`.
SHA-256 do arquivo: `120ef6c8e787fb40f2289a00f575842b04224856e46f08ffa8aa3d0d8a9d2252`.

O teste T20.5 importa o arquivo com especificação disponível/validade histórica
desconhecida, depois exporta e compara os bytes. Não atualizar esse fixture
com o produtor novo: ele prova a leitura do formato anterior.
