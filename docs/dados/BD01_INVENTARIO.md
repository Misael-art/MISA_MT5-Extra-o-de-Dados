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

Inventário máquina-legível: [inventario_bd01.json](inventario_bd01.json) (109 itens,
112 IDs de feature individuais). O teste [test_bd01_inventory.py](../../tests/test_bd01_inventory.py)
falha se uma definição do registry ou saída de produtor surgir sem item, se um item perder
dono/estado/lacuna, se os 38 nativos ou os itens de regra indefinida saírem do denominador.

## 2. Contagem por tipo e classe

Classes: **A** regra definida e entrada disponível; **B** regra definida, feed real
indisponível na DEMO; **C** regra indefinida (decisão pendente, não é exclusão).

| Tipo | Itens | A | B | C | Observação |
|---|---:|---:|---:|---:|---|
| Grupos do registry T18.1 | 15 (52 IDs) | 14 | 1 | 0 | volume real é B em FX DEMO |
| Famílias de produtores T13.3 | 6 (60 IDs real/tick) | 6 | 0 | 0 | volume/rvol/volatility/regime/structure/calendar |
| Indicadores do catálogo I01–I18, AX01–08, FRP | 27 | 16 | 4 | 7 | ver §4 |
| Nativos MT5 (HDK_StudyCatalog) | 38 | 38 | 0 | 0 | sem captura offline no app |
| Métricas temporais TM01–TM08 | 8 | 8 | 0 | 0 | consumidor de ledger (BD17) |
| Calculadores legados e funções puras | 3 | 3 | 0 | 0 | caminhos paralelos (BD07) |
| Setups Python (consomem cálculos) | 11 | 11 | 0 | 0 | não são indicadores |
| Ferramentas (AX09) | 1 | — | — | — | fora do denominador de cálculo |

As contagens 52/38/11 não são teto: novos itens entram pelo teste-guarda.

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

## 4. Itens de regra indefinida (classe C) — decisões pendentes do proprietário

Nenhuma fórmula será escolhida por adivinhação. Cada item fica no denominador.

| ID | Decisão faltante | Evidência | Impacto |
|---|---|---|---|
| I03 | Razão mínima (300% ≠ 3:1), comparação horizontal × diagonal, profundidade do book | catálogo F19 §I03 | impede imbalance; book e footprint ficam IDs separados |
| I05 | Limiar absoluto × quantil e janela de calibração | catálogo §I05 | impede grandes negócios |
| I17 | Quatro ROCs, quatro suavizações, pesos e sinal | catálogo §I17 | impede KST |
| AX02 | Períodos das dez EMAs do leque Madrid | catálogo §4 | `madrid_averages` existe com períodos próprios; equivalência ao estudo não afirmada |
| AX03 | Método e período do 2MV | catálogo §4 | impede 2MV |
| AX08 | Regra objetiva de S/R, falha e polaridade | catálogo §4 | legado `support_resistance` sem contrato não fecha o item |
| FRP | Fórmula, grupos e método estatístico | `docs/pesquisa/FRP_FLUXO_RELATIVO_PRECO.md` | estudo futuro P3 |

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
5. **Legado com dependência opcional (BD07).** `IndicatorCalculator` muda cobertura conforme
   `pandas-ta` (ausente no host; doctor avisa). `AdvancedIndicators` alimenta a coleta em tempo real
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
