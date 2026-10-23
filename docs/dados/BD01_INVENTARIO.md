# BD01 — Inventário de dados, donos e lacunas

Recorte de 2026-10-22, vinculado a T19.1/T20.1 (mesmo mapa, estendido). Prioridade
definida pela [diretriz de base estratégica](DIRETRIZ_BASE_ESTRATEGICA.md) e pelo
[plano revisão 2](PLANO_ESTRUTURA_DADOS.md). Este recorte é **inventário**: não implementa
cálculo, não altera schema e não qualifica fonte real. Nenhum item muda de estado por
constar aqui.

## 1. Base inspecionada

| Repositório | Commit | Observação |
|---|---|---|
| App (M) | `321272f0b24d682a58aad0767dbbee713403c1f9` | branch de distribuição G19, origem sem commits posteriores em 22/10 |
| HADOUKEN (H) | `6fb02374cd652a5fa0bfd314ace9d47ed8bc1283` | catálogo nativo `Include/Framework/HADOUKEN/Safety/HDK_StudyCatalog.mqh` |
| Safety (S) | `c5a3e2162cace117820eab863e0cb590d66189c2` | não atingido por BD01 |

Inventário máquina-legível: [inventario_bd01.json](inventario_bd01.json) (164 itens,
112 IDs de feature individuais). Versão inicial (3a34bf9) tinha 109 itens; a correção V05/V09
de 23/10/2026 acrescentou fontes primárias, filhos do legado e das funções puras, e separou
pendências por natureza. Nenhum item foi removido.

**Alcance do guarda** [test_bd01_inventory.py](../../tests/test_bd01_inventory.py): falha quando
surge sem item (a) definição do registry, (b) ID emitido por produtor, (c) coluna do
`IndicatorCalculator`, (d) método público de `AdvancedIndicators`, (e) função pública de
`strategies.indicators`, ou (f) com `MT5X_HADOUKEN_ROOT` apontando para a fonte H, quando o
catálogo nativo extraído (índice, função, parâmetros e SHA-256) difere do pin registrado.
Sem a variável, (f) é pulado com motivo na suíte básica (a CI não compara o pin); no gate de composição (BD21/G19.8) roda-se com `MT5X_REQUIRE_HADOUKEN_PIN=1`, que transforma a ausência do pin em falha. O guarda **não** protege
cálculos escritos fora desses pontos (privados de estratégias são listados como AX), e o teste
de produtores confere IDs emitidos, não a maturidade numérica de cada saída (30 e 130 sessões
emitem os mesmos 60 IDs).

## 2. Contagem por tipo e classe

Classes: **A** regra definida e entrada disponível; **B** regra definida, feed real
indisponível na DEMO; **C** fórmula inexistente.

| Tipo | Itens | A | B | C | Observação |
|---|---:|---:|---:|---:|---|
| Grupos do registry T18.1 | 15 (52 IDs) | 14 | 1 | 0 | volume real é B em FX DEMO |
| Famílias de produtores T13.3 | 6 (60 IDs real/tick) | 6 | 0 | 0 | |
| Catálogo I01–I18 (I03 em duas famílias), AX01–08, FRP | 28 | 18 | 7 | 3 | ver §4 |
| Nativos MT5 (HDK_StudyCatalog) | 38 | 38 | 0 | 0 | parâmetros extraídos do pin |
| Fontes primárias | 9 | 8 | 1 | 0 | §2.1 |
| Colunas do calculador legado | 15 | 15 | 0 | 0 | 5 vazias sem pandas-ta |
| Métodos de AdvancedIndicators | 10 | 10 | 0 | 0 | coleta em tempo real |
| Funções públicas de strategies.indicators | 23 | 23 | 0 | 0 | `rma` semeia na 1ª observação |
| Métricas temporais TM01–TM08 | 8 | 8 | 0 | 0 | |
| Setups Python | 11 | 11 | 0 | 0 | consomem cálculos |
| Ferramentas (AX09) | 1 | — | — | — | fora do denominador de cálculo |

### 2.1 Fontes primárias

| ID | Origem | Campos e unidades | Persistência | Ausência/capacidade |
|---|---|---|---|---|
| SRC-OHLCV | `copy_rates` (historical_extractor) | time; OHLC em preço; tick_volume = contagem de ticks; spread em pontos; real_volume | `<símbolo>_<tf>`, upsert por time (só o valor mais recente) | real_volume NULL = desconhecido, 0 = zero conhecido |
| SRC-DERIVED | analytics.resampling sobre M1 | OHLCV + sessão, revision, computed_at_utc, source_seq | `_bars_derived`, revisões preservadas | qualidade por barra, parcial marcada |
| SRC-TICKS | `copy_ticks` (tick_extractor, blocos/retomada) | time_msc (ms), seq, bid, ask, last, volume, volume_real, flags | `<símbolo>_ticks` (PK time_msc,seq) | ticks/observed_ticks/complete_real_ticks; FX DEMO sem last/volume real |
| SRC-TRADES | não capturado | — | — | `aggressor` indisponível; flags de tick não provam agressor |
| SRC-BOOK | market_book (book_collector) | time_utc (relógio local), type 1 venda/2 compra, price, volume | `<símbolo>_book` | book_snapshots; `queue` indisponível |
| SRC-SPEC | symbol_info | point, digits, tick_size/value, volume min/step/max, contract_size, moeda, swaps | `_symbol_specs`, `_instrument_facts` (vigência, T20.5) | padrão com motivo |
| SRC-CAL | inferido do M1 (analytics.calendar) | sessão, abertura/fechamento UTC e corretor, revisão | `_market_calendar` | offset fixo; sem calendário oficial/DST histórico |
| SRC-QUALITY | data_quality + data_eligibility | capabilities{state, reason, origin} | sob demanda; manifesto quando capturado | available/missing/unknown com motivo |
| SRC-REV | resampling, feature_store, datasets | revision, source_revision, computed_at_utc, manifest_id, namespace | `_bars_derived`, `_feature_values`, `_dataset_manifests` | bruto sem histórico; legado sem fotografia não tem passado |

As contagens não são teto: novos itens nos pontos (a)–(f) entram pelo guarda.

## 3. Mapa BD → dono canônico

| BD | Dono(s) no plano canônico | Estado do dono no painel |
|---|---|---|
| BD01 | T19.1/T20.1 | integrados (documentais); estendidos aqui |
| BD02 | F15, T20.4/24/25 | T20.4/24/25 integradas |
| BD03 | T20.4/5/6 | integradas; **lacuna de identidade multi-base** (§5) |
| BD04 | F1/F4, T20.6, EXTR-01 | integradas |
| BD05 | T13.1, T11.1, T20.5/6, T16.3c | T13.1 ⏳ (implementação pronta, suíte inconclusiva); T16.3c só em branch |
| BD06 | T18.1, T19.3 | integradas |
| BD07 | T19.3, T6.1, T4.5 | integradas; caminhos paralelos permanecem |
| BD08a/b | T19.3–7, T13.3 | T19.3–T19.7 inicial integradas |
| BD09 | T19.7–10, AX | planejadas |
| BD10 | T19.11–13, T5.3/5.4, T20.6 | planejadas |
| BD11 | T19.3, G19.5–7 | G19.x validadas; sem captura offline |
| BD12 | T13.2/3, T19.3/21 | parcial (grafo `features.affected_by`) |
| BD13 | T20.17/25, T13.2/3b | T20.25 integrada; T20.17 planejada |
| BD14 | T19.3, T13.3b, T20.24/25 | integradas |
| BD15a/b | T18.2/6, T13.4 | **T18.2 só em branch** `tarefa/T18.2-associacoes` (76404a1); T13.4 em branch (827afab) |
| BD16 | T18.5/6, T20.25 | planejadas |
| BD17 | T16.3/5, T19.14, T18.3/4 | T19.14 integrada; T18.3/4 planejadas |
| BD18a/b | T18.6/9, T13.6, SDK | planejadas |
| BD19a–d | T18.8, T13.6, T19.3, G19.5–7, F5 | T18.8 só fixture sintética |
| BD20 | T20.26/29 | planejadas |
| BD21 | T19.23/29, T20.29 | parcial |
| BD22 | continuidade/status | — |

## 4. Pendências separadas por natureza (correção V09)

A primeira versão marcava sete itens como “regra indefinida”. Só três têm fórmula inexistente;
os demais têm algoritmo definido e precisam de variante, parâmetro ou fonte.

| ID | Natureza | Tratamento técnico |
|---|---|---|
| I03-BOOK | limitação de feed | (Σvol_bid − Σvol_ask)/(Σvol_bid + Σvol_ask) até profundidade k; denominador 0 → ausente; k e limiar são parâmetros/regra de estratégia; book FX DEMO não qualificado |
| I03-FOOTPRINT | limitação de feed | variantes horizontal/diagonal, razão mínima como parâmetro tipado (“300% de” = 3:1; “300% a mais” = 4:1); sem agressor na DEMO |
| I05 | limitação de feed | duas variantes explícitas: limiar absoluto em lotes; quantil causal da janela anterior; nenhum default vencedor |
| I17 KST | parametrização do estudo não escolhida | proposta referenciada (Pring): ROC 10/15/20/30, SMA 10/10/10/15, pesos 1/2/3/4, sinal SMA 9, todos configuráveis; implementação em BD09 |
| AX02 Madrid | parametrização do estudo não escolhida | publicar a variante existente `madrid_averages(first_period, period_step)` sem alegar equivalência ao estudo |
| AX03 2MV | **fórmula inexistente** | método da média e regra de cor ausentes no estudo; pendente |
| AX08 S/R | **fórmula inexistente** | versão objetiva de falha/polaridade ausente; `LEG-ADV.support_resistance` é outro algoritmo |
| FRP | **fórmula inexistente** | hipótese futura P3 (`docs/pesquisa/FRP_FLUXO_RELATIVO_PRECO.md`); não bloqueia BD05 nem o primeiro consumo |

Limiares de confirmação (razão mínima, quantil, profundidade) pertencem à estratégia e
ficam fora da fórmula. O catálogo original não é declarado integral enquanto AX03/AX08/FRP
não tiverem regra.

## 5. Divergências e lacunas encontradas no código

1. **Identidade multi-base (BD03).** `_feature_values` tem chave
   `(symbol, tf, observation_time, feature_id, algorithm_version, revision)`; não há base/
   fonte/provedor. `datasets._dataset_origin` guarda uma origem `default` por arquivo de banco.
   Hoje o isolamento entre bases depende de bancos separados; dois feeds com o mesmo símbolo
   no mesmo banco colidem. A extensão precisa ser aditiva, com namespace legado estável.
2. **Assinatura global (BD13).** `indicator_materializer.implementation_signature` faz hash de
   três módulos inteiros e das versões pandas/numpy; qualquer edição em `strategies/indicators.py`
   versiona os 52 IDs.
3. **Estudo recalcula fora do store (BD19d).** `chart_study/series.py` e `snapshot.py` chamam
   `indicator_registry.iter_core` diretamente; não leem valores materializados nem versões do store.
4. **Cálculos privados fora do registry (BD07).** `vwap_bands.daily_vwap`, `stoch_ema9.slow_stochastic`,
   `ha_hilo.ha_hilo_context`, `madrid.madrid_averages`; funções `sma/ema/adx/linreg/percentile/swing`
   sem ID. Produtores T13.3 recalculam ATR/ADX/BB em paralelo ao registry.
5. **Legado com dependência opcional (BD07).** Sem `pandas-ta` (ausente no host; doctor avisa),
   `IndicatorCalculator` grava `macd_line/signal/histogram` e `bollinger_upper/lower` como colunas
   inteiras NaN, sem estado próprio de indisponibilidade (medido em 300 barras M1). `AdvancedIndicators` alimenta a coleta em tempo real
   sem contrato/versão.
6. **Relações sem serviço (BD15).** Associações T18.2 existem só em branch (`analytics/validation.py`
   e `tests/test_analytics_associations.py`), não ancestrais da base. A UI Relações usa fixture
   sintética identificada. Reusar a branch exige recorte próprio; não reimplementar.
7. **Nativos sem captura (BD11).** Os 38 nativos têm parâmetros fixos no catálogo H e unidade
   agrupada (`price`, `price_delta`, `price_distance`, `native_N`); o app não persiste capturas
   nem declara buffers. Sem MT5, nenhum valor nativo está disponível offline.
8. **Sem serviço de consulta (BD18).** `sdk.py` expõe apenas backtest; consultas de features
   passam pela CLI (`feature_store.query_as_of`).

## 6. Primeira tarefa elegível

BD02 (bancada compacta e orçamento) depende só de BD01. Em paralelo independente, BD03 começa
pelo mapeamento de leitores/escritores das chaves do store (§5.1) antes de qualquer migração.
