# BD08a — Primeira família qualificada: retorno (23/10/2026) — PARCIAL

Vinculação T19.3. Depende de [BD06](BD06_CONTRATO.md)/[BD07](BD07_CALCULADORES.md). Sem código de
produção novo: qualifica `price.return_1/5/20` pela cadeia real.

## Cadeia exercitada ([tests/test_bd08a_returns.py](../../tests/test_bd08a_returns.py), 5 casos)
Provedor MT5 falso → `HistoricalExtractor` (fonte `mt5@Fake-Demo` e recebimento por bloco) → M1 bruto
→ `resampling` M5 (offset declarado 0) → `indicator_materializer` → `_feature_values` → consulta.

| Caso | Resultado |
|---|---|
| Valores = oráculo independente (`bench_reference.returns`) sobre os closes M5 derivados | iguais (rel 1e-12), ausências coincidem |
| Acrescentar dias depois | prefixo já materializado idêntico; só novas observações |
| `batch_size` 7 × 5000 | valores idênticos |
| Denominador 0 ou ausente | ausência com estado ≠ VALID, nunca infinito |
| Correção tardia da última M1 de uma barra M5 | M5 revisada; retornos descendentes ganham revisão; as-of antes da correção devolve o valor antigo |

## Medições e achados
- Job de materialização (3 IDs, 324 barras M5): ~1 s, pico RSS do processo 172 MB (medido pelo
  job existente), crescimento do banco ~0,7 MB.
- **Invalidação ampla:** uma única barra corrigida gerou 852 revisões de fatos (`write_from = 0`,
  reavaliação de prefixo exato desde o início). Correto, mas caro; pertence a BD13/BD14.
- Correção numa M1 que não fecha a barra M5 não muda close nem retornos (comportamento correto).

## Limites
Só a família de retorno; demais famílias em BD08b. Assinatura global vigente (R05).

## BD13-RETURNS — revisões por dependência (29/10/2026)

O achado de852 revisões permanece como baseline histórico. No recorte atual,
`indicator_materializer` conserva a fórmula e a avaliação do registry existente,
mas o fingerprint de cada retorno maduro passa a usar os dois extremos do seu
horizonte, com observações explícitas na proveniência. Durante o aquecimento,
usa o prefixo conhecido que determina a maturidade. Tokens ainda representam a
barra de origem completa: corrigir outro campo em um extremo pode revisar sua
proveniência mesmo sem mudar o close. Não se aproxima ou arredonda o hash.

A disponibilidade considera correções na janela ordinal, inclusive recibos de
barras removidas/restauradas por elegibilidade. Só usar os recibos dos novos
extremos poderia antecipar uma mudança de contagem. O máximo móvel existente
recebe esses recibos agrupados com a próxima barra elegível; isso pode conservar
um recibo anterior à borda, de forma conservadora. Não altera o calendário nem
preenche barras ausentes. Correções fora dessa janela não atrasam os valores.

Na bancada compacta de80 barras M5/três horizontes, corrigir a barra30 gerava150
revisões. A regressão agora exige6: barra30 e barra30+h em cada horizonte1/5/20.
Valores são conferidos por aritmética independente; as-of anterior, ramo já
gravado, retomada após perda de recibo e remoção/restauração têm regressões.

Este recorte reduz escritas/revisões, não evita carregar e reavaliar o histórico.
Não granulariza a assinatura de implementação: editar o materializador ainda
gera uma versão nova, preservando a antiga para leitura explícita. As demais
famílias mantêm a política de prefixo, necessária nas recursivas e ainda
conservadora nas restantes. BD13 de assinaturas, execução seletiva e BD14
continuam abertos; sem novo cache, store ou executor.
