# BD05 — Tempo, sessões e derivação entre timeframes

## Estado atual (23/10/2026, 2º recorte) — PARCIAL

**Zona do corretor (R06), dono T1.6 (base de tempo):** nova chave `[APP] broker_timezone` (IANA;
`DEFAULTS`, `config.ini.example`, fallback vazio). Com zona, a base exige `time_basis = utc`: a
gravação converte o relógio local para UTC com horário de verão; a hora repetida (fold) é
inferida pela ordem e as **duas barras reais são conservadas** (96/96 na bancada, antes 95/96);
hora inexistente (gap de primavera) ou repetida sem ordem que a resolva é **recusada** sem gravar.
A zona fica em `_metadata` na 1ª gravação; reabrir com outra zona (ou sem zona) é recusado.
Validação por `pytz` (dependência existente): funciona no Windows sem `tzdata` (testado
simulando `zoneinfo` ausente; Windows físico não exercitado).

**Offset desconhecido:** `broker_utc_offset` vazio (padrão de instalações novas) ou ausente
deixa de ser −3 conhecido: `DatabaseManager.time_context()["offset_state"]` = `unknown`
(`declared`/`zone` nos demais casos); o código legado ainda calcula com −3, mas a leitura causal
(`load_ohlcv_as_of`) recusa base com fuso desconhecido e o manifesto do snapshot grava
`offset_state`. Configurações existentes que já contêm `broker_utc_offset = -3` continuam
tratadas como declaradas — o instalador gravava esse valor como padrão; o proprietário deve
conferir se −3 é de fato o fuso do servidor dos dados.

**Consumidores ainda de offset fixo:** derivação por sessão (`resampling`) e captura de snapshot
(`datasets.capture`) recusam base com zona, com mensagem explicada, em vez de produzir UTC errado.

Provas: `tests/test_bd05_zone.py` (7 casos) e `tests/test_bd05_zone_pg.py` (CI).

### Mapa W1/MN1

| Caminho | Situação |
|---|---|
| Captura nativa (`copy_rates_range`, enum `Timeframe`) | aceita W1/MN1; `Timeframe.MN1.minutes = 43200` é rótulo, não duração |
| Derivação analítica (`resampling.parse_target`) | recusa W1/MN1 com explicação (não há agregação semanal/mensal) |
| Fim de barra: `clocks.bar_end_in_source`, `datasets.capture`, `load_ohlcv_as_of` | MN1 por calendário; W1 = 7 dias. Com zona, W1 em semana de DST tem 167/169 h reais: o fim usa 7 dias UTC (desvio de 1 h, limite) |
| `robots/parity.py` (T10.6) | soma 30 dias ao fim de MN1 — registrado no dono, sem alteração |
| Consumidores mensais (Laboratório, relações) | não qualificados |

### Limites
Fold que cai na fronteira de dois blocos de extração fica ambíguo em cada bloco e é recusado (FX
fecha no domingo da mudança, mas mercados 24/7 sofreriam); derivação/snapshot em base com zona
não suportados; ticks/book não usam a zona.

---

## Histórico dos recortes

### 1º recorte (23/10/2026)

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
