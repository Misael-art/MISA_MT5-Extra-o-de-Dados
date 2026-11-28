# EURUSD · conciliação e hipótese de reversão M1

A PR 412 usa a correção de volume do HADOUKEN. A referência canônica da correção é `b7158d22`. O replay diagnóstico reproduziu 11 negócios, saldo de USD 1.013,36 e 24 recusas. A proteção 10 da posição 10 ainda constava como reconhecida depois da saída parcial de 0,08 e do stop de 0,09. O histórico deixava 2,7755575615628914e−17 ao subtrair as duas saídas de 0,17. Na primeira recusa, em 1794812520477, PersistJournal rejeitou o volume positivo da posição encerrada durante a validação da projeção. A correção usa o volume corrente confirmado pela conta após confrontar a cadeia completa. [PR do HADOUKEN](https://github.com/Misael-art/projeto-indicadores-mt5/pull/93)

## Pesquisa pareada

O perfil contém a família, a queda entre fechamentos, a invalidação na mínima, a proteção, os cortes, as tentativas e os limites da seleção. O plano congela cinco políticas antes do primeiro passe, cada uma com depósito inicial idêntico. A carteira conjunta paga o spread bid/ask e submete as duas estratégias ao mesmo orçamento dentro de um único Expert. A referência em caixa possui resultado e exposição zero. [Plano de EURUSD](perfis/EURUSD/reversal-m1.json)

No launcher de desenvolvimento, informe uma pasta nova para a pesquisa. Repetir o comando com o mesmo destino conserva o histórico de tentativas. Use uma fita compatível posterior ao treino para repetir a política selecionada.

```bash
./run.sh --cli --json laboratory ultra-market-research --training "$TREINO" \
  --plan docs/repasse/perfis/EURUSD/reversal-m1.json --out "$PESQUISA" --timeout 900
./run.sh --cli --json laboratory ultra-market-tester-run --mode portfolio \
  --snapshot "$OUTRA_FITA" --preset "$PESQUISA/policy.json" --out "$REPLAY" --timeout 900
```

Na janela de carteira do Laboratório, selecione o plano JSON, a fita e o destino pelo botão de treino. O resultado informa o caminho da política quando houver seleção. O botão de executar preset aceita esse arquivo e abre o relatório do passe. Para acompanhar ou retomar pela CLI, consulte `progress.json`. A seleção em caixa fica em `selection.json`. O campo `economic_approval` permanece falso durante o desenvolvimento.

O [plano de falha de rompimento](perfis/EURUSD/breakout-failure-m1.json) usa venda em M1. A barra precisa superar a máxima das `lookback_bars` anteriores por pelo menos `breakout_fraction` e fechar abaixo daquela máxima. As barras anteriores vêm do prefixo da própria fita. O Expert registra a decisão de cada barra elegível no primeiro tick disponível, com a máxima anterior, a máxima e o fechamento. O recibo compara a sequência completa com Python em `comparison.pattern_parity`, inclusive quando a barra não sinaliza. O plano aceita `close_at_low` e `close_above_prior_close` em `invalidation`. Os braços sem filtro registram o predicado e deixam a entrada seguir para os guards do Tester.

## Revisão publicada com o Tester aberto

Acrescente `--policy-inbox` ao passe inicial. Enquanto o recibo registra `testing`, congele uma política sucessora com `--portable --previous "$ATIVA"`. A recepção aceita a retirada de módulos compilados e a alteração de `orb.range_minutes`. A ordem das prioridades, a proteção e o orçamento precisam permanecer compatíveis. Cada passe aceita uma publicação, identificada por vínculo, revisão, política e hash do pacote. Com `--selection`, o instalador verifica os cinco braços e a fronteira de treino, reutiliza a política vencedora e a vincula como sucessora da versão ativa. Uma seleção em caixa suspende as entradas.

```bash
./run.sh --cli --json laboratory ultra-market-install --receipt "$RECIBO" \
  --successor "$NOVA" --effective-msc "$INSTANTE"
./run.sh --cli --json laboratory ultra-market-install --receipt "$RECIBO_DE_SUSPENSAO" \
  --selection "$PESQUISA/selection.json" --effective-msc "$INSTANTE"
```

O Expert consulta a entrega na primeira cotação de cada minuto. A partir da vigência, fecha a entrada para donos com posição, ordem ou incerteza. Preserva o vínculo antigo para gestão. Depois de conciliar o encerramento, aplica a nova versão aos módulos habilitados. Uma política em caixa mantém todos fechados para novas entradas. Os eventos de recebimento, ativação, retenção e adoção ficam em `comparison.observations.delivery`.

A pasta de evidência privada nesta composição é `state/linux/ultra-plataforma/eurusd-20261126`. O contrato de um passe concluído fica em `receipt.json`, acompanhado de `report.htm`, `carteira.html`, EX5, fita binária e journal. O histórico de preflight, pausa e retomada fica em `host/`. O mercado permanece em observação até a qualificação da reserva econômica.

## Resultados de desenvolvimento em 16 de novembro de 2026

O replay corrigido preservou os 11 negócios do prefixo original. No primeiro veto anterior, tick 1794812520477, o Expert aplicou o limite de margem de USD 810,69 a um pedido que exigia USD 925,52. A ordem 13 foi executada no tick 1794812580132. O passe terminou com 43 ordens, 43 negócios, sete parciais e sete breakevens. O relatório confirmou saldo e patrimônio de USD 982,56 sem pendências. O hash SHA-256 do EX5 é `a3c41ecd4092f35db3f4f6fed3d124a85f38b004437f88c79777eaf16e24a7da`. A cópia do guard exportada pelo App conserva todas as linhas da fonte canônica. O exportador remove a marca BOM do texto.

| Braço | Líquido em USD | Ciclos | Fator de lucro | Drawdown relativo de patrimônio |
|---|---:|---:|---:|---:|
| Caixa | 0,00 | 0 | Indefinido | 0,00% |
| ORB | −4,56 | 1 | 0,000 | 0,46% |
| Reversão simples | −15,98 | 8 | 0,461 | 2,48% |
| Reversão filtrada | −15,63 | 8 | 0,472 | 2,52% |
| ORB e reversão simples | −15,64 | 8 | 0,460 | 1,96% |
| ORB e reversão filtrada | −15,89 | 8 | 0,456 | 2,03% |

Os cinco braços consumiram cinco tentativas. O Expert e o Python identificaram as mesmas 17 ocorrências e seis invalidações. No tick 1794817261009, o fechamento bid passou de 1,10204 a 1,10176 e igualou a mínima. A carteira estática executou a ordem 8 de compra de 0,19 lote. A condicional suprimiu o sinal antes de entrar. Adicionar o padrão filtrado ao ORB reduziu o resultado em USD 11,33. Aplicar o filtro à carteira estática reduziu o resultado em USD 0,25. Cada braço atingiu no máximo uma posição simultânea. Todos ficaram abaixo do piso de retorno. A seleção técnica foi caixa.

Na revisão posterior, o produtor observou 425.984 bytes de fita já gravada pelo Expert antes de produzir o sucessor. O ORB adotou a janela de 15 minutos no tick 1794810180682. O Pullback preservou a versão anterior na posição 12 até seu encerramento em 1794812102517. Na suspensão do dia 17, a posição 11 conservou a gestão de origem. O Expert registrou 16 vetos de novas entradas e concluiu a conciliação.

A presença do ORB também deslocou uma entrada da reversão filtrada no tick 1794850200004. O guard calculou USD 4,75 de risco para a ordem e USD 4,11 de capacidade diária restante no patrimônio compartilhado. A comparação de sinais apresenta o motivo integral observado no journal.

## Resultado com todos os módulos e retomada do relatório

Em 16/11 o controle dos doze módulos nativos perdeu USD 19,35. Adicionar o padrão filtrado com prioridade final produziu os mesmos cinco ciclos, máximo de duas posições e drawdown de patrimônio de 1,94%. Os onze sinais válidos da reversão exigiam USD 4,75 de risco com USD 0,65 restantes. A contribuição marginal foi USD 0,00. O controle foi registrado depois da seleção primária e antes desses dois passes.

No teste semanal de 16 a 20/11, a carteira de regressão processou 989.333 ticks, abriu e encerrou 16 posições e terminou com USD 956,99. O histórico de pesquisa inclui 5.908.294 ticks em 25 sessões de treino e 5.350.905 ticks nas 20 sessões de validação. No rótulo de um minuto com spread, a reversão filtrada somou −115 e −361 ticks nos dois períodos, respectivamente.

O terminal pode continuar o mesmo passe depois que termina a espera da CLI. O recibo guarda o prazo e a identidade da tentativa. A retomada lê o relatório final da instalação, o journal do agente e a fita vinculada. O relatório conciliado fica na pasta `reconciled` junto ao recibo inicial. Para acompanhar um passe em aberto ou conciliar depois do encerramento, use a fita original.

```bash
./run.sh --cli --json laboratory ultra-market-tester-resume \
  --receipt "$RECIBO_ORIGINAL" --snapshot "$FITA_ORIGINAL"
```

Reabrir o mesmo plano e destino no Laboratório também tenta recuperar o braço pendente, conferindo a identidade de sua política e de sua fita. Uma conciliação confirmada conserva a contagem de lançamentos. O mesmo procedimento reconhece recibos anteriores que terminaram com journal parcial e já possuem relatório nativo completo.
