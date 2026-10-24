# BD06 — Contrato completo do catálogo (1º recorte, 23/10/2026) — PARCIAL

Vinculação T18.1/T19.3. Depende de BD01/BD03/BD05.

## O que muda

[`analytics/indicator_contracts.py`](../../mt5_extracao/analytics/indicator_contracts.py) estende cada uma
das 52 definições do registry com os campos que o plano exige e o registry não guardava: pergunta,
`output_kind` e `buffers` (número × categoria, com categorias nomeadas), regra de ausência,
alinhamento (deslocamento de desenho separado do valor), regra de revisão, consumidores,
referência independente e apresentação (painel/traço). A fórmula continua só no registry; família
sem contrato falha ao ser consultada.

Códigos de direção (−1/0/1 de alinhamento, cor, cruzamento) são **categorias**, embora gravados
como `NUMBER` pelo contrato de armazenamento T18.1 (mudar o `value_type` versionaria os valores).

## Ordem das assinaturas (resposta ao R05)

O grafo continua acíclico e **não muda**: BD13 depende de BD12, que depende de BD08a. O piloto
BD06→BD07→BD08a→BD18a→BD19c1 roda com a assinatura global vigente
(`indicator_materializer.implementation_signature`) como **limitação temporária explícita**:
editar `indicator_registry.py`, `strategies/indicators.py` ou o materializador versiona os 52 IDs
(as versões antigas continuam legíveis no store). Para não agravar isso, os metadados deste módulo
ficam fora do hash. Invalidação seletiva **não** é declarada pronta; continua em BD13.

## Provas

`tests/test_bd06_contracts.py` (6 casos): contrato completo para todo o registry e serializável
sem NaN; categorias explícitas; deslocamento como apresentação; metadados fora da assinatura;
referências pendentes listadas (23 de 52: KC, MACD, distância EMA/ATR, posição de BB e saídas
derivadas do Donchian — BD08b); família desconhecida falha.

## Pendente
Produtores T13.3 (60 IDs) ainda sem esse contrato; consumidores (Estudo, Laboratório) ainda leem
`indicator_registry.definitions()` em vez do contrato completo (BD19).


Atualização parcial de24/10/2026: as quatro saídas MACD passam a apontar para
[referência decimal própria](BD08B_MACD.md). A lista de23 acima é a fotografia
original; a família MACD deixa de constar como referência pendente. Isso não
qualifica automaticamente os demais IDs nem conclui BD06/BD08b.
