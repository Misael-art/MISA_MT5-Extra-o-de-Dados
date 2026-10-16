# T17.23 — Contexto verificável da agregação no Laboratório

## Problema e contrato

O aviso atual atribui todos os registros parciais a robôs MQL5 anteriores à T16.2b. O ledger define completo pela presença da entrada e direção válida; importações Python antigas também podem ser parciais. Vários runs agregados exibem uma curva descritiva, sem constituir uma carteira executável. A queda usa o capital inicial configurado no resumo, e a moeda e os custos do ledger antigo não estão documentados.

Preservar todos os resultados, curvas, fórmulas, parâmetros e registros. Não inferir tecnologia/versão/causa nem moeda ou custos ausentes. O contexto deve descrever apenas a população observada. Sobreposição refere-se aos intervalos de operações observadas por run, não ao período integral do teste nem a duplicidade.

## Passos

1. Ambiente pelo instalador e requirements-dev, seguido de baseline: concluídos, 947 aprovados e 9 skips existentes, exit 0, antes deste documento/código.
2. Implementar contexto no núcleo: população/runs, parcialidade e campos ausentes, referência temporal, limites observados, capital usado no resumo e moeda/custos não documentados neste contrato. Sem estatística em widgets.
3. Compartilhar o contexto na UI, CLI JSON/texto e proposta exportada. Essencial visível; comprovação em DetailsPanel existente, inicialmente recolhido e com ícone de informação. Ocultar o contexto F16 ao abrir a apresentação sintética F18.
4. Testar parcialidade Python/MQL5 sem causa inventada, direção/entrada distintas, vazio e múltiplos runs, sobreposição observada, JSON/export e nenhuma mutação numérica. Inspecionar dados reais já expostos em temas/tamanhos/DPI e interação do detalhe.
5. Suíte completa, doctor, revisão técnica, CI no SHA exato, PR próprio, integração individual e operação instalada. Registrar limitações e atualizar continuidade.

## Aceite

Não aparece a falsa atribuição a MQL5; a agregação é identificada como descritiva e a queda informa a referência de capital. Moeda/custos ausentes continuam desconhecidos; PF calculável não certifica resultado líquido. Comprovação permanece acessível por teclado/mouse sem competir com a análise. UI/CLI/export compartilham o mesmo contexto e os números prévios permanecem idênticos.

Sem novas dependências/configuração/schema, estratégia, otimização, ordens ou autorização financeira. Dados já expostos são QA, não avaliação OOS. Base T17.22, integração própria sem PRs #79/#80.
