# BD04 — Ingestão idempotente, revisão e qualidade (primeiro recorte)

Recorte de 2026-10-22, vinculado a F1/F4 (T1.1/T1.2), T13.1 e T20.6. Depende de
[BD03](BD03_IDENTIDADE.md). Sem código de produção novo: consolida o aceite da cadeia
bruto → derivado → snapshot com os mecanismos existentes.

## Comportamento verificado ([tests/test_bd04_ingestion.py](../../tests/test_bd04_ingestion.py))

| Aceite do plano | Mecanismo existente | Resultado |
|---|---|---|
| Ordem de chegada irrelevante | upsert por `time`; leitura ordenada | embaralhado = ordenado no bruto e no M3 derivado |
| Repetição não duplica | upsert por `time` na tabela bruta; `_bars_derived` só grava nova revisão quando `input_hash` muda | 2 gravações = 6 linhas; 2ª derivação 0 inseridas/0 corrigidas |
| Correção conserva histórico | tabela bruta **sobrescreve**; `_bars_derived` cria revisão 2 e mantém a 1; manifesto T20.4 é imutável | M3 com revisões [1, 2] e closes [102, 250]; snapshot anterior ainda lê 102 |
| Falha não anuncia sucesso | `save_ohlcv_data` devolve `False` sem coluna `time` | nada gravado |
| Incompleto não vira zero | volume real ausente persiste `NULL`; zero conhecido persiste 0 | [0, NULL, 7] |

## Contraexemplos da revisão independente (V02) e correção de produção

O primeiro recorte (só testes) provava que derivado e snapshot **já capturados** sobrevivem.
A revisão mostrou que (a) corrigir 102→250 antes de qualquer captura perdia o 102 e (b) reaplicar
o payload antigo fazia o bruto voltar a 102. Correção aditiva no dono do upsert (T1.1),
`database_manager._upsert_dataframe`, na mesma transação da gravação:

- **`_ohlcv_revisions`** (tabela nova, aditiva): só barras cujo OHLCV/volumes/spread mudam
  ganham linhas; a versão vigente é arquivada antes de ser sobrescrita. Barra legada nunca
  corrigida não tem histórico e sua chegada fica desconhecida (`known_since_utc` NULL), sem
  história fictícia.
- **Precedência:** `save_ohlcv_data(..., received_at=)` informa quando ESTE sistema recebeu o
  payload (padrão: agora). Versão recebida depois prevalece; recebida antes do vigente vira
  `stale` (registrada, não aplicada); mesmo payload + mesmo recebimento é repetição sem efeito.
  O provedor não informa revisão/vintage: não inventamos. Um retry sem `received_at` é tratado
  como novo — por isso retries devem repassar o instante original.
- **`load_ohlcv_as_of(symbol, tf, as_of)`**: barras como eram conhecidas no instante, com
  `revision` e `known_since_utc`. `load_ohlcv` continua sendo a leitura vigente (latest ≠ as-of).

## Provas adicionais ([tests/test_bd04_ingestion.py](../../tests/test_bd04_ingestion.py), 9 casos)

| Caso | Resultado |
|---|---|
| Duas correções sem captura prévia (101→250→300) | as-of devolve 101/250/300 conforme o instante; 1ª versão com chegada desconhecida |
| Payload antigo após o novo (retry com recebimento original) | bruto continua 250; versão `stale` registrada e nunca escolhida no as-of |
| Evento repetido | sem nova revisão |
| Retry sem `received_at` | política explícita: vira a versão mais nova; o as-of anterior continua consultável |
| Falha injetada no 2º bloco | `False`; 1º bloco e histórico desfeitos; retry íntegro |
| Volume real ausente → disponível → ausente (E06) | `ohlcv` elegível nos três; `real_volume` elegível só no meio; snapshot do meio conserva 5,0 |
| Fonte indisponível no meio do lote e retomada | coberto pelo teste existente `test_extraction_resume.py::test_failed_block_does_not_block_others_and_resume_fetches_only_missing` (não repetido) |

`tests/test_bd04_ingestion_pg.py` repete revisão/stale/as-of em PostgreSQL — **executado só na CI**.

## Recursos medidos (SQLite local, 10 000 barras M1)

1ª gravação 10,66 s; regravação idêntica 11,19 s; 10 000 correções 14,7 s (arquivo 7,3 MiB).
O custo dominante é o upsert preexistente; correções em massa fazem uma consulta de histórico
por barra alterada.

## Limite importante

O bruto continua guardando só o valor vigente; o histórico de mudanças fica em
`_ohlcv_revisions`. Ticks e book não ganharam histórico neste recorte. Quem precisa do passado
usa `load_ohlcv_as_of`, snapshot ou `_bars_derived`. Extratores ainda não repassam
`received_at` de retries (passo seguinte no dono da extração).
## Pendente em BD04

- Capacidade: comprovada no nível de `data_eligibility` (E06); consumidores de UI ainda não leem
  esse estado diretamente (BD19).
- Captura por blocos interrompida e retomada já tem aceite EXTR-01/T1.2; não repetido aqui.
- PostgreSQL não exercitado neste recorte.
