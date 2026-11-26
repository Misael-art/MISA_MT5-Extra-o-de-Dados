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
