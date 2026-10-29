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

## BD06-NOMES — nomes imutáveis e compatibilidade do catálogo

Uma variável do loop de validação de metadados sobrescrevia o argumento `name`:
novas definições eram gravadas com o nome `value_type`, e repetir a mesma versão
com outro nome não era rejeitado. O registro agora preserva o nome recebido e
recusa sua alteração na mesma versão, mantendo a idempotência da repetição exata.

`_feature_definitions.definition_schema_version` é aditivo: registros novos usam 2,
enquanto registros anteriores recebem 1, sem reescrever nomes, datas ou fatos.
A leitura de um schema anterior reconhece o legado sem migrar tabelas. Nomes
`value_type` no schema 1 ficam `UNVERIFIED_LEGACY_NAME`; um nome intencionalmente
igual no schema 2 é `DECLARED` e continua imutável. O marcador não muda a versão
matemática nem a identidade do indicador.

Para não quebrar produtores existentes, o registro legado com nome não verificável
aceita uma repetição somente se parâmetros, dependências e todos os demais
metadados forem idênticos. Retorna sem escrever, emite aviso e conserva o rótulo
antigo como não verificável. Alterar qualquer outro metadado continua exigindo
nova versão. Não inferir nem corrigir nomes históricos silenciosamente.
A CLI de definições e o detalhe do inventário exibem essa limitação.

Regressões em `tests/test_analytics_features.py` cobrem nome, imutabilidade,
schema antigo, migração aditiva e consumo CLI/inventário. O caso de append em
`tests/test_indicator_materialization.py` simula o legado, acrescenta 20 barras
M5 nos 52 indicadores e confere preservação de todos os fatos/hashes anteriores,
sem revisões numéricas. Validação dirigida: 111 testes do núcleo/consulta/CLI e
produtores, um append legado e três testes Tk do inventário aprovados (115 casos
distintos). A reprodução anterior gravava `value_type` para `Nome correto` e
aceitava outro nome na mesma versão; o comportamento corrigido foi exercitado
também pelo launcher oficial, em banco sintético próprio. Isso não encerra BD06 completo nem a consulta de todas
as definições armazenadas: esse consumo continua na sequência BD18/BD19.
