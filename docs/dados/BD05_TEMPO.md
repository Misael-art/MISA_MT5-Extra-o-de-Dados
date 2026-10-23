# BD05 — Tempo, sessões e derivação entre timeframes (primeiro recorte)

Recorte de 23/10/2026, vinculado a T13.1/T11.1 e T20.5/6. Depende de BD03/BD04 e da bancada
corrigida ([BD02](BD02_BANCADA.md), V03). Usa os mecanismos existentes; uma correção de produção.

## Provas ([tests/test_bd05_time.py](../../tests/test_bd05_time.py), 7 casos)

| Aceite | Mecanismo | Resultado |
|---|---|---|
| M5 não usa H1 ainda aberto | `data_eligibility.as_of` | às 10:35 só a H1 das 09:00; a das 10:00 aparece às 11:00:02 (fechamento + recebimento), não às 11:00:01 |
| Revisão tardia invisível ao as-of anterior | idem | valor revisado só após `available_at` da revisão |
| Massa impossível da bancada | idem | `invalid_computed_before_close` é **rejeitada** ("anterior ao fechamento") |
| Fuso desconhecido | idem | horário sem fuso é recusado, sem UTC presumido |
| Sessão noturna e feriado | `calendar` + `resampling` (âncora 17:00) | duas sessões que atravessam a meia-noite, 6 H1 cada; dia sem pregão não gera barra |
| W1/MN1 não são minutos fixos | `resampling.parse_target`, `clocks.bar_end_in_source` | derivação recusa W1/MN1; fim de MN1 por calendário (fev/2024 = 29 dias, jan = 31) |
| Pivô só após confirmação | T19.6, já qualificado | `docs/analytics/T19.6_PIVOS_CAUSAIS.md` (não repetido) |

## Correção de produção

`resampling.parse_target("W1")` levantava `AttributeError` (chamava `Timeframe.parse`, inexistente)
em vez da recusa explicada. Passa a usar `timeframes.parse` e devolve a mensagem
"Timeframe analítico inválido … Use M<n>, H<n>, 1D ou 1440min".

## Limites medidos (não corrigidos)

1. **Offset fixo do corretor perde uma hora no fim do horário de verão.** Com `broker_utc_offset`
   fixo e a base em relógio do corretor, a hora local repetida (fold, bancada `dst`) vira o mesmo
   carimbo; o upsert por `time` guarda só uma das duas barras (95 de 96). Também não existe estado
   "offset desconhecido": `broker_utc_offset` tem padrão −3 presumido. Corrigir exige zona horária
   do corretor por base (com fold/gap) no dono T1.6/T13.1 — próximo recorte de BD05.
2. **`robots/parity.py`** soma `Timeframe.MN1.minutes` (30 dias) ao fim do período mensal;
   pertence ao dono da paridade (T10.6), registrado sem alteração.
3. Calendário observado continua com offset fixo e sem calendário oficial futuro (T13.1).
