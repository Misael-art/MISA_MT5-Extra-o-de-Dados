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

## Limite importante

A tabela bruta por símbolo/TF guarda só o valor mais recente. O histórico de revisão existe nas
barras derivadas e nos snapshots, não no bruto. Uma consulta as-of anterior à correção precisa
partir de snapshot ou de `_bars_derived` (revisão por `computed_at_utc`), nunca da tabela bruta.

## Pendente em BD04

- Propagação de mudança de capacidade da fonte (ex.: volume real passa a existir) até o
  consumidor, via `data_eligibility`/qualidade T20.6 — recorte seguinte.
- Captura por blocos interrompida e retomada já tem aceite EXTR-01/T1.2; não repetido aqui.
- PostgreSQL não exercitado neste recorte.
