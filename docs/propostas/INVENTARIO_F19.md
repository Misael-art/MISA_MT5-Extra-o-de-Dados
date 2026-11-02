# T19.1 — Inventário reconciliado e decisões de contrato

Recorte documental de 2026-10-17, para revisão. Não implementa F19/F20 nem
qualifica operação financeira. T20.1 estende **este mesmo mapa** com Q01–Q16,
seus testes e consumidores (seção 8). O mapa não aprova os requisitos de runtime.

## 1. Base e proveniência

| Repositório | HEAD inspecionado | Árvore e alcance |
|---|---|---|
| mt5x | `489591c0940c93c5e3766e7fa2febefad96b1717` | Código integrado do PR105; 8 documentos modificados e 11 novos preexistentes, preservados. T19.0/T20.0 disponíveis na árvore, ainda não no HEAD |
| HADOUKEN (`Misael-art/projeto-indicadores-mt5`) | `480d31048ee00e92628e615848d97b20406b442b` | Clone resolvido por `[ROBOTS] library_dirs`; `ESTUDOS/` não rastreado, somente lido |
| mt5x-safety | `d45257b13f20332ade8d811eb2aec87f70178563` | Clone resolvido por `[ROBOTS] safety_lib_dir`; árvore limpa; `safety.json` e `VERSION` 1.5.0 |

Os caminhos abaixo são relativos à raiz indicada: **M** = mt5x, **H** =
HADOUKEN e **S** = mt5x-safety. Não dependem da localização pessoal dos clones.
Nenhum AGENTS.md foi encontrado em H/S; foram consultados README, plano,
andamento e épicos aplicáveis. O diretório alternativo chamado
`HADOUKEN-MQL5-Framework` tem remoto homônimo, mas não possui HEAD válido;
não é a biblioteca configurada e não fundamenta alegações de implementação.

Fontes H lidas, sem alteração, em `ESTUDOS/`:

| Arquivo | SHA-256 |
|---|---|
| `indicadores.txt` | `e701f02dac3fc3515d898cafc51cc26e31a7689209878f02672fdc7ea7bd1018` |
| `estrategias_trading_algoritmico_especificacoes_tecnicas_md.md` | `8228e6f406d5fc9bc16fc7298bd457941a7227c02b5c0e7eeb946f1a65351702` |

Hashes coincidem com o [catálogo F19](CATALOGO_F19_INDICADORES_SETUPS.md).
O catálogo conserva as regras e lacunas das fontes; este mapa registra as
bases concretas e os donos de suas extensões.

## 2. Integrado, existente em branch e proposto

**Integrado** abaixo significa presente no HEAD M indicado, sem transferir
validação nativa entre ambientes. **Em branch** significa código localizado por
`git ls-tree`/`git show` e ausência de ancestralidade com o HEAD; não foi importado,
mesclado ou revalidado por esta tarefa. Worktrees e alterações locais preservados.

| Entrega | Evidência atual | Estado para consumidores F19 |
|---|---|---|
| T13.1: calendário e barras por sessão | [calendar.py](../../mt5_extracao/analytics/calendar.py), [resampling.py](../../mt5_extracao/analytics/resampling.py), [limites](../analytics/T13.1_CALENDARIO_OBSERVADO.md) | Integrados; `_market_calendar`/`_bars_derived`. M1 observado e offset fixo, sem calendário oficial futuro/DST histórico |
| T13.2: store as-of | Branch `codex/t13.2-feature-store`, `843a85a642cbe768950f64a443334dea31932c48`: `analytics/features.py`, `feature_store.py`, `docs/analytics/T13.2_FEATURE_STORE.md` | Em branch, ausentes no HEAD. `_feature_definitions`/`_feature_values` são nomes físicos daquela entrega; não criar outro store |
| T13.3: produtores/perfis | Branch `tarefa/T13.3-perfis-regimes`, `3f9e333450278b3eced39addaed60e3a9b4ef2d2` | Em branch; famílias de volume/regime e produtores não estão integrados |
| T13.3b: atualização incremental | Branch `tarefa/T13.3b-producers-incrementais`, `00f67705e806550b720f97c36c69405874d7db38` | Em branch; conferir o dono antes de materializar pela T19.3 |
| T13.4: hipóteses/validação | Branch `tarefa/T13.4-hipoteses-walk-forward`, `827afab1618ccf53a37212e772dc80eb067feee9` | Em branch; validação F6/F16 integrada não comprova integração desta infraestrutura |
| T18.1: registry/DTO | Branch `tarefa/T18.1-registry-indicadores`, `f45618f9059feb8e0f54fa75f70f8159995891ab`: `indicator_registry.py`, `target_registry.py`, `relation_dto.py` | Em branch; envelope `relation_schema_version=1` e definições já existem ali. Não criar registry concorrente |
| T18.2: associações | Branch `tarefa/T18.2-associacoes`, `76404a16bc879cab05eb7cb6db6483d49d49b04b` | Em branch; consumidores dependem de revisão/integração e qualificação do recorte |
| T18.8: apresentação de Relações | [relations.py](../../mt5_extracao/ui/relations.py) | Integrada com fixture sintética identificada, sem serviço real F18. Registro antigo de suavização precede a correção T17.8 |
| B-02–B-11 / Composer | H `Strategies/HDK_StrategyRobot.mqh` ainda usa `HDKSignal`/`HDKExitRules`; M sem `contracts.py`, `composition/`, `presets/`; H/S sem `Contracts/` | Propostos. A declaração/generação T10.10 não equivale a Composer B-09 |

O cabeçalho histórico “nada implementado” de F13 não descreve T13.1 integrada;
o relato T13.2 na continuidade descreve uma branch, não a árvore atual. Não apagar
esses registros nem promover seu código sem integração própria. A divergência de
ciclo T12.1→T12.2→T12.3 tem decisão posterior para avançar a implementação
([plano principal](../PLANO_DE_TRABALHO.md), seção T12.3 a T12.8); operação
supervisionada e validações de concorrência continuam pendentes.

## 3. Mapa de capacidades, donos e consumidores

Cada linha separa a base entregue de uma extensão com dono único. IDs B/C/D/E/F/H
são do backlog H em `Documentation/HADOUKEN_2/propostas/`; I/J estão em M.

| Capacidade/base efetiva | Dono da base | Extensão e dono executor | Consumidor / gate |
|---|---|---|---|
| Extração por blocos: [HistoricalExtractor._save_block](../../mt5_extracao/historical_extractor.py) chama `IndicatorCalculator.calculate_technical_indicators` com aquecimento e grava por `save_ohlcv_data` | T1.2/T6.1 | T19.3 publica/materializa pelo store T13.2 e registry T18.1, após integração | Histórico, exports e contexto F16; fonte/fórmula/disponibilidade identificadas |
| Coleta em tempo real: [data_collector.py](../../mt5_extracao/data_collector.py) chama `EnhancedIndicatorCalculator`, que usa `AdvancedIndicators`, e grava a última linha por `save_data` | T4.5 (reuso) | T19.3 reconcilia variantes e alcance, sem copiar fórmulas | Base e Laboratório; indicador calculado não prova coluna persistida |
| [data_exporter.py](../../mt5_extracao/data_exporter.py) exporta tabelas; [indicators.py](../../mt5_extracao/strategies/indicators.py) calcula séries puras | T5.1/T6.1 | T19.3 acrescenta projeções do registro canônico | CLI/UI/export; função pura não equivale a feature armazenada |
| [ledger.py](../../mt5_extracao/trade_analytics/ledger.py), [importers.py](../../mt5_extracao/trade_analytics/importers.py), enrich/cache/aggregate/stats | T16.1–5, livro compartilhado H-01 | T19.14 acrescenta trajetórias/tempos sem reescrever o ledger | T19.15/16/28/31; entrada ausente impede reconstrução |
| CSV legado H `Strategies/HDK_StrategyRobot.mqh:StrategyTester` | T8.9 / T16.2b-H, integração H-14 | T16.2b-H conserva dono do exportador permanente; T19.14 consome só o que estiver disponível | Cinco colunas, uma linha por posição; não exporta a trajetória/eventos de ciclo |
| [sessions.py](../../mt5_extracao/robots/sessions.py) + [session_export.mqh.tmpl](../../mt5_extracao/robots/templates/session_export.mqh.tmpl) | T17.28 | T19.29 reutiliza a sessão isolada; paridade por evento é T20.11 | Exportação própria com entrada/preços/posição/custos/moeda, agregada por posição; não conclui T16.2b-H nem H-01 completo |
| [base.ExitRules](../../mt5_extracao/strategies/base.py), [backtester.py](../../mt5_extracao/strategies/backtester.py), [execution/engine.py](../../mt5_extracao/trade_analytics/execution/engine.py) | T6.3 e T16.6–8 | T19.15/16 tempo; T19.17 entrada; T19.18 ganho; T19.19 proteção; T19.22 gradiente; T19.24 conecta backtest completo | Preservar modo base; re-simulação de entradas fixas não é novo backtest |
| `SymbolSpec.lots_for_risk`, mínimo/passo/máximo e parcial única `partial_r/partial_fraction` | T6.3 e T16.6 | T19.56 dimensionamento/exposição; T19.57 agenda de parciais/ack/saldo | T19.25/30 e adaptador H; grid+parcial simples não é suportado pelo re-simulador |
| H/S `HDK_SafeExecutor` e `HDK_RiskGuard`: mercado, pendentes, fechamento, stop e guardas | D-02, T12.1, E (base existente) | B-04–B-08 adaptam contratos; T19.23 mapeia capacidades, sem novo executor | Toda ação nativa via Safety; cenário não testado não recebe liberação |
| H/S `HDK_PortfolioRiskGuard`; M [portfolio.py](../../mt5_extracao/robots/portfolio.py) | F-03/T12.2; F-04/F-05/T12.3 | T19.48 diagnostica etapa posterior, sem assumir conclusão das guardas | Grade/adições exigem orçamento/reserva/política/ownership e F-10/11/12 pertinentes |
| [rules.py](../../mt5_extracao/trade_analytics/rules.py), insights/validation | T16.9/10, T6.4 / I | T19.20 filtros de sessão/cota; T19.21 filtros tipados; T19.27/28/52 consomem T20.12/13 | Seleção automática só após protocolo/tentativas; nenhum resultado futuro decide entrada |
| [profit_factor.py](../../mt5_extracao/profit_factor.py) | T13.9 | T20.2/3 qualificam referência/equity; sem outra fórmula de PF | T19.28 e UI: null com motivo, nunca teto artificial; saldo encerrado não certifica risco flutuante |
| [mql.py](../../mt5_extracao/robots/mql.py), catalog/controls, leitura de `.set` | T8.1, T17.28 | T19.25 compiler/pacote; T19.50 nomes; T19.55 Default; T19.26 edição/importação Manual | Consumidos pela UI T19.30/32. Default e quatro categorias ainda não são serviço de presets F19 |
| [generate.py](../../mt5_extracao/robots/generate.py), declared/scaffold | T10.3/10 | B-09 montagem nativa; B-10 integração M; T19.2 composição canônica | Um Composer; T19.25 compila parâmetros, não cria gerador paralelo de Experts |
| [robot_controls.py](../../mt5_extracao/ui/robot_controls.py), controls/sessions | T17.28 | T19.33 verifica coerência de TF, sem outro seletor | Ativo / TF já existe; refresh/contexto/consentimento preservados |
| Tokens/theme/components/charts e Laboratório | F17/T17.25/27, fixture T18.8 | T19.30–34 UI por serviço; T20.22 referência de layout; T20.23 inspetor | Tkinter/ttk/Matplotlib, DTO real, cálculo fora dos widgets; UI-X01/FRP continuam futuros |
| Fila runner/lote/cache e `resources/` | T11.5/6, F15, T16.5 | T20.24/25 qualificam orçamento/isolamento; T19.27 consome | Pesquisa cancelável/retomável; não criar segundo scheduler/cache/store |

### Presets e inputs conferidos

`catalog.robot_from_file` e `contract.validate` leram os seis fontes e seus
manifestos H sem gravar no catálogo/banco: **6 contratos válidos, nenhum input
de papel ausente, nenhum include ausente na leitura local**. Essa leitura não
é compilação nem prova de execução. Diretório H `Experts/HADOUKEN/Estrategias/`:

| Expert (subpasta e nome-base) | `parity_with` | Inputs lidos | Preset `.set` encontrado | TF declarado |
|---|---|---:|---|---|
| HDK_BbRsi | bb_rsi | 14 | HDK_BbRsi_Conservador | M15/H1/H4/D1 |
| HDK_Donchian | donchian | 12 | HDK_Donchian_Conservador | H4/D1 |
| HDK_EmaAtr | ema_atr | 12 | HDK_EmaAtr_Conservador | H1/H4/D1 |
| HDK_Orb | orb | 10 | HDK_Orb_Conservador | M5/M15 |
| HDK_Pullback | pullback | 13 | HDK_Pullback_Conservador | H1/H4 |
| HDK_Squeeze | squeeze | 13 | HDK_Squeeze_Conservador | H1/H4 |

Todos declaram `contract=1`, versão do fonte `1.00`, papéis comuns
`magic=InpMagicNumber`, `risk_pct=InpRiskPct`, `daily_loss_pct=InpDailyLossPct`,
`max_drawdown_pct=InpMaxDrawdownPct`, `export_trades=InpExportTrades`.
O papel `stop` é `InpRangeMinutes` no ORB, `InpSwingBars` no Pullback e
`InpStopAtrMult` nos outros quatro; isto não autoriza inferir um stop universal
em pontos. Os seis `.set` sobrescrevem apenas magic/risco/limites, deixando os
parâmetros estratégicos no fonte. Não declaram `safety_lib`: usam includes H.

Também encontrados `HDK_HADOUKEN_Painel` v7.00 (65 inputs, presets
`HDK_Conservador`, `HDK_Moderado`, `HDK_Agressivo`) e `HDK_AccountGuardian`
v1.00 (8 inputs, sem preset encontrado). Perfis internos precisam ser resolvidos
pela tarefa de defaults; nomes Conservador/Agressivo não provam categorias F19.
A opção atual “Padrão do código” em `controls.DEFAULT_PRESET` deve ser reutilizada
pela T19.55, com retrato explícito/versionado e cópia Manual pela T19.26.

Para S09, `ema_atr`/`HDK_EmaAtr` oferece cruzamento 20/50 condicionado à EMA200,
stop 2 ATR e trailing 3 ATR (`InpFastPeriod`, `InpSlowPeriod`, `InpTrendPeriod`,
`InpStopAtrMult`, `InpTrailAtrMult`). Não equivale ao template genérico de SL/TP
fixos do estudo. T19.37 verificará a equivalência e declarará uma variante mínima
sem copiar Experts nem completar parâmetros ausentes por suposição.

O exportador de sessão grava `position_id`, comissão, swap, taxa e moeda, mas
`read_hdk_trades_full` projeta somente os campos legados/entrada/saída para o
ledger atual; não preserva essas colunas extras individualmente. T19.14 e H-01/H-14
devem considerar essa perda de granularidade ao consumir eventos e custos.

### Alcance das guardas de carteira

H `Documentation/HADOUKEN_2/ANDAMENTO.md` registra T12.3/F-04/F-05 como
**parciais**: orçamento global/por magic, líder e reserva implementados;
21 verificações sintéticas e 5 da API de reserva em Tester DEMO, sem ordens.
Esses ensaios são históricos, não repetidos por T19.1. Reserva do plano inteiro,
limites por ativo/grupo e concorrência operacional de dois robôs continuam
pendentes; não inferir cobertura dessas capacidades da mera presença da Safety
1.5.0. Netting, operações supervisionadas, reconciliação/restart e ambientes
precisam de suas próprias provas F-10/F-11/F-12 antes de liberar consumidores.

## 4. Decisões de contrato e migração

1. **Contrato SDK 1 preservado.** [Validador](../../mt5_extracao/robots/contract.py)
   e [schema](../../mt5_extracao/robots/robot.schema.json) recusam campos desconhecidos.
   Composição, capacidades, identidade e evidência novas ficam em artefato lateral
   versionado; nenhum `capabilities` informal entra no `robot.json` v1. T19.2 define
   schema/serialização final; T19.25 define pacote/compilação e leitura de volta.
2. **Um dono por interface.** B-02–B-07 definem os seis contratos H; B-08 adapta o
   legado. B-10 faz a integração Python; T19.2 entrega a composição e consome esses
   contratos. B-09 monta os módulos nativos; T19.30 edita a mesma composição na UI.
   T19.25 compila apenas inputs mapeados para aquela versão; nenhuma heurística
   autoriza exportar um módulo ausente como executável.
3. **Distribuição ainda depende de B-11.** A opção “duas pastas, uma biblioteca,
   uma versão” em [decisões anteriores](DECISOES_CONTRATOS_E_CALENDARIO.md) é proposta,
   não entrega. S `safety.json` só declara `include_dir=MT5X/Safety` e 11 arquivos;
   M `robots/safety_lib.py:install` usa uma única pasta. H/S ainda não têm `Contracts/`.
   B-11 registra a política antes de mudar distribuição/assinaturas. T19.1 não a
   implementa nem considera uma pasta adicional já suportada.
4. **Store e ledger únicos.** Consumir T13.2/T18.1 após sua integração, preservando
   nomes físicos internos com `_`; T19.3 amplia materialização/publicação. Preservar
   `_trade_ledger` e chave `(run_id, seq)`. Trajetórias/eventos e identidades novas
   exigem extensão aditiva versionada, sem transformar deals/parciais em novos trades.
5. **Identidade independente do rótulo.** T19.50 é dono da convenção de nomes;
   T19.55 do retrato Default; T19.26 de Manual nomeado/rascunho/round-trip. Preservar
   `.set` legados, defaults efetivos/perfis, `preset_id`, `config_hash` e hash da prova
   distintos. Nenhuma escrita de preset nesta tarefa ou autorização de operação ao gerar.
6. **Safety tem origem única.** Comparados byte a byte os 11 `.mqh` de H
   `Include/Framework/HADOUKEN/Safety/` e S `Include/MT5X/Safety/`: idênticos neste
   snapshot. Mudança de H exige distribuição versionada S e consumo M na mesma tarefa
   vinculada; igualdade das fontes não comprova versão instalada no terminal.

## 5. Cobertura integral do catálogo e lacunas

“Base” nesta tabela é reuso parcial observado, não aceite do item F19.
Nenhum I/TM/S/AX é promovido a implementado por indexação.

| IDs | Base e lacuna conferida | Dono executor / consumidores |
|---|---|---|
| I01/I02 | Ticks capturados não provam agressor/volume real; delta/CVD qualificados não integrados | T19.11 → T19.21 e filtros/Ultra |
| I03 | Book e footprint distintos; snapshots não demonstram fila | T19.12 → T19.21 |
| I04/I05 | Fonte de negócios/timestamps/tamanho por qualificar; nenhuma identidade institucional inferida | T19.13 → T19.21 |
| I06 | Alligator integrado ao registry/store ausente; SMMA/semente/offsets por fixar | T19.4 → T19.3/21 |
| I07/I09 | AO/AC não publicados como features; encadeamento/aquecimento por fixar | T19.5 → T19.3/21 |
| I08/I14 | Fractals/ZigZag causais não publicados; `swing_high/low` não é prova de pivô confirmado | T19.6 → T19.17/18/21 |
| I10 | `indicators.donchian` inclui barra atual; `strategies/donchian.py` aplica `shift(1)` ao canal para rompimento; função não é materialização | T19.7 → T19.3/16/21 |
| I11/I12 | Envelopes e canal completo não publicados; slope/R² disponíveis como primitivas | T19.7 → T19.3/21 |
| I13 | McGinley sem definição publicada/estado incremental integrado | T19.8 → T19.3/21 |
| I15 | Ultimate sem definição publicada, denominadores/aquecimento por fixar | T19.9 → T19.3/21 |
| I16/I17 | TRIX/KST sem definição publicada/encadeamento qualificado | T19.10 → T19.3/21 |
| I18 | `indicators.efficiency_ratio` existe; falta publicação causal/consumo integrado | T19.3 → T19.16/21 |
| TM01/TM02/TM04/TM08 | Duração e MAE/MFE F16 existem; trajetória/extremos no tempo/censura/ciclo incompletos | T19.14 → T19.15/28/31 |
| TM03 | `max_bars` não distingue time de gain/loss/empate | T19.15 → T19.24/31 |
| TM05/TM06/TM07 | ER/Donchian/ATR existem como primitivas; política temporal composta/relógios não | T19.16 → T19.24/31 |
| AX01 | `advanced_indicators.py` tem `vwap` cumulativa de close×volume, sem reset de sessão nessa fórmula; não certifica VWAP de negócios | T19.3/36, reuso C-10 → S01 |
| AX02 | Dez períodos Madrid não fornecidos | T19.39 → S05 |
| AX03 | Método das médias e política Renko/parcial não fechados | T19.42 → S06 |
| AX04 | Estocástico existe em `AdvancedIndicators`; não prova variante lenta/K/D exigida | T19.38 → S10 |
| AX05 | HA/Activator sem variante/semente/paridade qualificada | T19.40 → S11 |
| AX06 | Fibonacci exige âncoras confirmadas e congelamento; alvo não comprova entrada | T19.17/18 → pacote/políticas |
| AX07 | Renko por ticks não integrado; tamanho R/reversão/pavios por definir | T19.41, reuso A-03/C-08 → S02/S06 |
| AX08 | Falha/polaridade/range discricionários sem limiares objetivos fornecidos | T19.43 → S04 |
| AX09 | OCO/cópia/painéis/travas são ferramentas; reusar executor/ownership, não criar sinais fictícios | T19.44/47/48 → S03 e gestão posterior |
| S01 | VWAP percentual versus desvio; fonte/execução limite/stop por qualificar | T19.36 |
| S02 | Infraestrutura Renko, não robô autônomo; menu offline do estudo não é contrato de API | T19.41 |
| S03 | Master/slave não pode desfazer uma ordem já preenchida; intenção/admissão/risco pendentes | T19.44 |
| S04 | Operação manual não equivale a regra algorítmica definida; preservar evento manual | T19.43 |
| S05 | Períodos/gatilho/reset incompletos; não inventar leque | T19.39 |
| S06 | Fonte Renko e preços executáveis; parcial/adições e proteção exigem capacidades próprias | T19.42 |
| S07 | Direção e alvo por perna/cesta do médio ambíguos; averaging depende de política | T19.45 |
| S08 | Filtro Black Box sem fórmula; original impedido, variante transparente tem outro ID | T19.46 |
| S09 | EmaAtr/declarada são bases; equivalência/variante e políticas do piloto por verificar | T19.37 |
| S10 | Estocástico 14/3/K/D e trailing EMA9 por qualificar | T19.38 |
| S11 | HA/Activator e reversão de duas etapas, distintos da flag de inversão | T19.40 |

A seção 12 do estudo só cita famílias de grid/Martingale/Fibonacci,
Donchian/Keltner/ORB, Bollinger/VWAP/RSI, OCO/cópia/painéis/travas. Não enumera
os alegados 37/48 setups. T19.47 continua dona desse levantamento detalhado,
sem criar novos robôs para completar a contagem. FRP, agentes autônomos e
renderer alternativo conservam o estado futuro/P3.

### Lacuna reproduzida: parcial inviável ativa break-even

Em M `trade_analytics/execution/engine.py:simulate`, o ramo de break-even
fica fora da condição que confirma volume válido de redução. Caso sintético
isolado: compra 1 unidade, mínimo/passo 1, entrada 100, stop 98, parcial em 1R
com fração 0,5. A redução quantizada é 0; mesmo assim, a opção de break-even
produz saída “stop no zero a zero” a 100 na barra seguinte. Com a opção
false, encerra por “fim dos dados” a 100. Barras OHLC utilizadas:
`(100,100.5,99.5,100)`, `(100,102.5,99.5,102)`, `(101,101.5,99,100)`;
spread/slippage/comissão/swap zero. Não houve redução executada.

O caso confirma uma lacuna de gestão, mesmo com PnL final igual; não é prova
nativa nem mudança de comportamento. **Dono: T19.57**, com referência financeira
T20.2 e proteção T19.19. A leitura de
`strategies/backtester.py` e de H `Strategies/HDK_StrategyRobot.mqh:StrategyManageTick`
mostra a mesma separação entre validar a parcela e mover o stop; no H a marca
de parcial é gravada antes da tentativa. Esse lado MQL5 não foi ensaiado nesta
sessão. Não copiar esse comportamento para o contrato novo. Correção e regressão pertencem ao recorte executor,
sem alteração de runtime/testes nesta tarefa documental.

## 6. Próximo recorte e limites de publicação

Após revisão/integração desta entrega, **T20.1** é a próxima ficha documental
independente elegível na ordem de prioridade consolidada: estender este mapa
para Q01–Q16, sem reinventariar as bases. T19.2 ainda exige B-11 e delimitação
B-09/B-10; T19.3 exige integrar/revalidar T13.2/T18.1; T19.14 pode ser uma
trilha independente sobre F16, respeitando disponibilidade da fonte.

O caminho do piloto continua S09/T19.37 → Default → Manual nomeado → backtest
completo → pacote → conferência/replay nativo → UI, pelas dependências da
[ficha F19](PLANO_F19_PILARES_PRESETS.md) e do [aceite T20.30](PLANO_F20_ROBUSTEZ.md).
Não iniciar esse runtime enquanto os contratos do seu recorte estiverem ausentes.

T19.0/T20.0 e suas referências já estavam modificadas/não rastreadas antes desta
tarefa. Sua publicação/integração é pré-requisito de um PR T19.1 autocontido;
não incorporar esses arquivos inteiros como se fossem implementação desta tarefa.
O incremento próprio deve ser revisado contra a árvore inicial preservada,
sem atribuir seu diff anterior à T19.1. Não houve merge/push/instalação de
Expert/preset nem alteração de H/S.

## 7. Verificação desta entrega

Ambiente instalado com `./install.sh --yes --no-mt5` e dependências dev.
Suíte inicial antes das edições: **1058 aprovados, 9 ignorados existentes,
3 avisos em 1045,42 s**, saída 0. Suíte final: **1058 aprovados,
9 ignorados existentes, 3 avisos em 1265,91 s**, saída 0. Avisos de
`Matplotlib.tight_layout` em gráficos existentes; nenhum teste alterado/pulado.
Testes SDK docs: **3 aprovados em 0,89 s**. Doctor inicial/final:
**16 OK, 2 avisos, 0 falhas**, saída 0 (pandas-ta opcional ausente e ponte desligada).

Xvfb próprio 640×480, aproximadamente 100 DPI, sem gerenciador de janelas;
temporários exclusivos em disco, 66 GiB disponíveis no início. Essa execução
não constitui inspeção visual da matriz de resoluções/DPI F19.

Conferidos 36 links locais novos, todos os IDs do catálogo, 57 fichas F19 e
30 F20; dependências F19 inalteradas. `git diff --check` aprovado. Os 13
arquivos preexistentes fora do recorte permanecem idênticos por SHA-256;
os demais receberam apenas incrementos conferidos contra snapshot inicial.
Runtime/testes/dependências/scripts sem diff. Evidências e patch próprio em
`logs/t19-1/` (fora do Git), incluindo inventário dos seis contratos,
reprodução sintética, saídas das suítes e diagnóstico.
Nenhum ensaio visual, benchmark ou replay/DEMO é exigido por esta ficha
exclusivamente documental. Windows físico não executado; pendências operacionais
existentes permanecem no [checklist Windows](../validacao_instaladores.md).

## 8. T20.1 — cobertura Q01–Q16, donos e provas necessárias

Extensão documental do mapa T19.1, em revisão local. Base M/H/S e branches
continuam as identificadas nas seções 1–2; nenhuma integração presumida.
“Base” indica código lido e regressões existentes, não aceite integral do Q.
O dono executor é o ID que altera a capacidade; as demais fichas consomem e
qualificam a mesma entrega. Não há segundo store/ledger/Composer/experimento.

| Q | Base concreta / alcance observado | Extensão com dono executor único por recorte | Consumidores | Prova que falta para o aceite |
|---|---|---|---|---|
| Q01 | M `strategies/backtester.py`, `profit_factor.py`, `trade_analytics/stats.py`; métricas de encerradas e PF v1. `tests/test_backtester.py`/`test_profit_factor.py` são regressões | T20.2: referências financeiras; T20.3: patrimônio; T20.13: protocolo estatístico | T19.24/28/56/57, T20.8/14/23 | Contas independentes de bid/ask, custos, tick/lote e parcela; perdas flutuantes com recuperação; calibração/intervalos do protocolo. Suíte verde não certifica risco intrabarra |
| Q02 | M `analytics/calendar.py`/`resampling.py`, `_bars_derived` com revisão; `_symbol_specs` representa propriedades capturadas. Store T13.2 somente em branch | T20.4: manifesto/snapshot; T20.5: vigência/specs; T13.2 conserva dono do store | T19.3/25, T20.6/12/16 | Leitura concorrente consistente, revisão tardia reproduzível, specs vigentes e ausência histórica explícita; não usar hash de DB em gravação como snapshot |
| Q03 | M `data_quality.py`, calendário derivado e extração ticks/book; não há gate integrado por capacidade. Registry T18.1 em branch | T20.6: gate/qualidade/cobertura; I-08 permanece dono da infraestrutura nativa consumida | T19.11–13/21/29, T20.9/30 | Duplicata/gap, agressor desconhecido, real/tick-volume distintos; relatório Model4 com origem observada/gerada/desconhecida e motivo de bloqueio apenas do consumidor incompatível |
| Q04 | M `robots/contract.py`, `controls.py` e specs; seleção verifica código/parâmetros/TF. H/S sem contratos B. Não há matriz contextual completa | T20.7: matriz; T19.2: composição; T19.23: mapper H; B-11: política contratual | T19.25/29/53, T20.8/11/20/26 | Combinações/versões/contas inválidas recusadas; netting não herda hedging; round-trip legado e requisito desconhecido com motivo |
| Q05 | M `SymbolSpec`, `execution_spreads`, sizing e re-simulação; margem conjunta não modelada. H/S orçamento/reserva parcial, sem prova concorrente | T20.8: margem; T20.9: cenários; T20.10: volume; T19.56/57: lote/parcela; F-04/05: guardas nativas | T19.24/28/42/44/45, T20.21/30 | Contas de margem compartilhada/câmbio/custos, pendentes e saldo aberto; gap/rejeição/fill parcial; cenários sem fonte não certificam capacidade. Não confundir parcela voluntária com fill parcial |
| Q06 | M `robots/parity.py` casa saída/resultado, sem direção; exportador de sessão amplia posição/entrada, mas importador perde colunas extras. H CSV legado agregado | T20.11: comparador por evento; H-01/H-14 e T16.2b-H: eventos/exportação; T19.29: reconciliação do preset | T20.17/18/20/23/30 | Primeira divergência plantada por direção/volume/tempo/custo; intenção→risco→ordem→deal→posição→ciclo com IDs; nativo reconciliado. Preservar seis pares e critérios, sem dizer que todos já passam |
| Q07 | M `strategies/validation.py`, F16 rules/insights e estudo PF19 têm avaliações próprias; T13.4 em branch; não existe registro único de todas as tentativas/reserva | T20.12: registro único; T20.13: auditoria/protocolo; T13.4/I-09 consomem a mesma infraestrutura | T19.27/28/52, T20.14/15/16 | Falha/descarte/manual/cache/reexecução/consulta à reserva rastreados; purga/embargo justificados; ruído e efeito conhecido com intervalo/poder. Reserva usada para ajuste deixa de ser teste final |
| Q08 | PF canônico integrado; objetivos F19 e comparação de elegíveis não implementados | T19.28: seleção; T19.54: Ultra; T20.14: compromissos, sem objetivo paralelo | T19.32, T20.22 | Pequeno conjunto com dominância/empate conhecido, candidato inválido/sem perdas/zero atividade excluído; Defensivo com risco patrimonial; manter token FatorLucro |
| Q09 | M lote/matriz/validation; F18 em branches e apresentação sintética. Re-simulação fixa entradas, não reexecuta oportunidades | T20.15: ablação sobre backtest completo T19.24; I-02/I-05 são bases consumidas | T19.27/52/54, futuros estudos FRP | Componente útil/redundante/danoso conhecido, interação e custos comparáveis; tentativas registradas e evidência OOS. FRP não iniciado |
| Q10 | M fingerprint/catálogo/pipeline; `.set` legado e JSON `trade_analytics/export.proposal`, sem pacote reproduzível F19 | T19.25: compiler; T19.50: identidade; T19.55: Default; T20.16: manifesto ampliado; T20.17: estados/invalidação | T19.26/29/32/53, T20.18/22/27/28/30 | Round-trip isolado com binário trocado/referência ausente; hashes separados; mudança semântica invalida consumidores; rótulo não altera config; cálculo/estatística/replay/autorização separados |
| Q11 | M `robots/live.py` monitor de posições/deals; `notify.py`. Não há observador completo de intenções nem diagnóstico calibrado | T20.18: observação; T20.19: diagnóstico; I-12/T13.8/J-05/J-08 consomem eventos e serviços | Acompanhamento do preset, UI e alertas | Registro causal antes do desfecho, modo sem executor, virtual identificado; série estável/degradada/custo/dado vencido e alarmes calibrados; forward exige janela futura própria |
| Q12 | M `robots/sessions.py` separa sessões; H/S persistência de risco/reservas parcial. Agenda geral de parcelas e recuperação E-13/F-12 pendentes | T20.20: bateria; E-10/E-13/F-10/F-12: mecanismos nativos; T19.57: estado de parcelas | T19.23/29/44/53/57, T20.30 | Timeout/restart/duplicata/evento fora de ordem, reconciliação antes de novas ações, sem repetir parcial ou tocar terceiros; rede/fill ao vivo mantém ensaio DEMO específico |
| Q13 | M matriz de testes independentes e `portfolio.py` de leitura; não há pesquisa síncrona com capital compartilhado | T20.21: piloto de pesquisa, após T19.48/T20.3/8; épico F conserva guardião | Pesquisa de carteira posterior | Dois sinais disputando capital/margem, FX/exposição conjunta e concentração; pesos sem futuro. Somar curvas independentes não é prova de execução simultânea |
| Q14 | M componentes/tokens/gráficos F17, Laboratório F16 e Ativo / TF T17.28; Relações fixture sintética, DTO real não integrado | T19.30: editor; T19.31: temporal; T19.32: presets; T19.33: TF; T19.34: adaptação; T20.22: percurso/layout; T20.23: inspetor | T20.30 e investigação de cada capacidade | DTO×tabela×gráfico×export, seleção bidirecional/cursor, estados/rascunho/contexto antigo; dois temas, teclado, fonte, tamanhos/DPI F19. T20.22 não depende de T20.23; UI-P/UI-X permanecem posteriores |
| Q15 | M `robots/runner.py` limita início de jobs por CPU/RAM; `cancel()` impede pendentes, não cancela trabalho ativo. Cache F16 assina barras/calendário/config; sessões isolam destino | T20.24: orçamento/cancelamento/checkpoint; T20.25: cache/isolamento; F15 conserva medição/retenção | T19.24/27/52, lotes de pesquisa | Disco/tentativas/tempo, cancelamento cooperativo, restart sem duplicação; mudança de código/dados/spec/protocolo/seed/custo invalida cache; não limpar artefatos de terceiros |
| Q16 | M SDK/contract/scaffold/audit e ponte restrita; F15 `resources/files.py` faz backup SQLite/quick_check/restore; nenhuma biblioteca de módulos B integrada | T20.26: conformidade; T20.27: origem/permissões; T20.28: restore de pesquisa; T20.29: release; B-11/J-09 conservam políticas | SDK/Composer, pacotes e release | Módulo causal/incompatível, terceiro adulterado/licença/permissão, restore completo isolado com vínculos, formatos/builds/idiomas e instalador 2×; Windows físico explicitamente pendente |

### Resolução dos status contraditórios

- F13 “nada implementado” é snapshot anterior à T13.1 integrada; os caminhos
  T13.2/T13.3/T13.4/T18.1 da seção 2 são branches verificadas, sem integração.
- F16 “nada implementado” no topo da proposta é histórico: ledger/enrich/cache/
  re-simulação/UI estão no HEAD. Isso não aprova trajetória/ciclos nem F19/F20.
- I-05 “MQL5 não avaliado” descreve a bateria simples; `battery --deep` T10.5
  já mede vizinhos e período final. Não comprova o protocolo ampliado T20.13.
- SDK/exatidão T8.7 “nunca rodou no terminal” é anterior aos ensaios T8.7 e
  T17.28 registrados; houve prova de mecânica e replay histórico, não novo
  forward ou aprovação financeira dos seis robôs. As quatro condições da
  paridade permanecem; não equiparar MQL5 antigo×novo a Python×MQL5 aprovado.
- T12.3/F-04/F-05 têm código e harness sem ordens, ainda parciais. “Só observa”
  nos diagnósticos antigos não descreve todo o código atual; não inferir
  reserva do plano inteiro ou concorrência operacional como validadas.
- Exportação ampliada de sessão T17.28 não conclui o exportador permanente
  H/T16.2b nem garante persistência de todas as colunas extras no ledger.
- UI Relações mantém origem sintética; correção T17.8 já removeu suavização.
  Nenhum consumidor recebe prova real por haver uma captura histórica.

Essas resoluções valem para este snapshot. Os documentos históricos são
preservados; novos consumidores devem citar a linha correspondente deste mapa.

### Sequência elegível e primeiro aceite

A T20.2 é o próximo recorte P0 elegível sobre o motor atual, após entrega da
T20.1: contas independentes primeiro, execução contra o código em seguida e
correções somente das divergências daquele recorte. T20.4 e T20.24 são trilhas
independentes após T20.1; usar o mesmo registro/store/fila, sem duplicá-los.
T19.2 continua condicionada a B-11 e definição B-09/B-10; T19.3 à integração
T13.2/T18.1. A existência das branches não autoriza marcá-las integradas.

O aceite inicial T20.30 mantém S09 → Default → Manual nomeado → backtest
completo → pacote → replay nativo reconciliado → UI real, com as dependências
T20.3/6/8/11/16/17/22 e políticas realmente usadas. Não espera Ultra, carteira,
FRP, book, todos os indicadores ou seleção automática T19.28. Não conclui
T19.35 (piloto ampliado). Cada prova mantém modalidade/ambiente/versão próprios.

### Verificação T20.1

Ambiente Linux preparado novamente; baseline de código da sessão anterior
1058 aprovados/9 ignorados existentes/3 avisos, saída 0, precede estas edições.
Nenhum runtime/teste/dependência mudou entre essa execução e esta tarefa.
Suíte final e checagem documental registradas na continuidade ao concluir.
Sem novo ensaio visual/nativo/DEMO/Windows físico: o recorte é documental.

## 9. Pilar operacional — inspeção da composição em 01/11/2026

Pedido de execução ponta a ponta recebido nesta data. Esta seção atualiza o
inventário histórico acima; não promove os marcos F19/F20 a concluídos.
Base inspecionada: App `d456d07c`, HADOUKEN `b730ed4a`, Safety `3988df13`.
SA-REF-01 conserva fonte, closure e runtime `33c2fa1` da autorização de DEMO;
o desenvolvimento seguinte usa novos hashes, sem herdar aquela autorização.
Pins de desenvolvimento só mudam com a reconciliação H → S → App.

Cada linha abaixo exige o percurso preset → inputs efetivos → decisão → pedido
→ ordem/deal → posição → gestão → saída → reconciliação. Compilar um consumidor
ou testar seu corpo MQL em C++ não comprova esse percurso no broker.

| Capacidade | Implementação / consumidor e configuração | Restrição / evidência e aceite pendente |
|---|---|---|
| Margem individual e reserva percentual | H `Safety/HDK_SafeExecutor.mqh`, `Open` e `PlacePending`; `margin_buffer_pct` em percentual da margem livre, moeda da conta | Defeito confirmado: falha de `OrderCalcMargin` deixava seguir. T20.8 passa a negar cálculo ausente/negativo/não finito, margem livre inválida e reserva fora de `[0,100)`. Não é reserva agregada de planos |
| Checagem do pedido | Mesmo executor, `OrderCheck` | Falha com retcode zero e bool verdadeiro com recusa deixavam passar. Exigir bool e código de aprovação; checagem não é fill |
| Resultado e repetição | `HDK_ExecutionResult.mqh`, `OpenObserved`, adaptador B08 | Aceito/parcial/confirmado/recusado/incerto separados no observador. Remover repetição por `CONNECTION`; timeout não se repete. F04.1 fixa conta/servidor no Init do executor e mantém recusa/requote com IDs ou volume como incertos, inclusive nas retentativas. Pendência DONE/PLACED com ordem e volume ecoado, sem deal, permanece ACEITA sem fill inventado; mercado DONE sem deal continua incerto. F04.5 persiste gestão no mesmo journal; integração aos consumidores ainda pendente |
| Preço, SL e especificação | Executor existente; preço e distâncias em preço, volume em lotes | T20.8 rejeita NaN/infinito/EMPTY_VALUE, tick/ponto/passo/mín/máx inválidos, cotação cruzada, identidade básica ausente. Conta/servidor/magic não substituem posse por contribuição/sessão |
| Risco por capital / lote fixo | `HDK_RiskGuard::CalcLot`; `composition/sizing.py`, `HDK_VolumePolicy.mqh`; S09 usa bindings tipados | Saldo/equity e fração Python versus percentual MQL explícitos. F04.1 valida números/configuração/conta e exposição fresca; lote de referência negociável e risco recalculado após arredondamento para baixo. Aceites de margem conjunta e concorrência operacional pendentes |
| Orçamento da conta, robô, ativo e grupo | `HDK_PortfolioRegistry.mqh` observa conta; `HDK_RiskGuard.mqh` limita risco do magic | F04.1 consolida leituras; F04.2 persiste reservas/contador/tombstones. F04.3 vincula pedido/intenção/contexto por SHA256, revalida risco/margem e concilia ordem/deals/posição/snapshot antes de consumir; divergência posterior retém risco novamente.F04.4 conecta o envio opt-in ao SafeExecutor:154testes H/checker171/17compilações0/0; Tester confirma uma entrada/resposta/conciliação/fechamento, duplicata recusada antes/depois de encerrar, zero remanescentes e caixa. F04.5 (H7291d680/PR82) acrescenta close/parcial, SL monotônico e cancelamento ao mesmo journal: 157 testes Tools e 17 compilações sem erros/avisos; Tester confirmou uma entrada/parcial voluntária/proteção/fechamento e seis tipos de pendência colocados/cancelados, caixa e R original. Posse por contribuição, resposta e efeito separados, sem reenvio incerto. F04.6 (H ad880c13/PR83) implementa manifesto real e S09v2 no adaptador; ack do pacote exportado confirmou vínculo, mas o percurso excedeu180s e não foi aprovado. Manutenção foi corrigida offline; compilação/Tester da fonte alterada pendem. Perfil App continua bloqueado até qualificação. Pendentes/planos/ativo/grupo/consumidores e aceites operacionais continuam pendentes. O protótipo sóTesterDEMO não é distribuído pela Safety. Branches históricas1.4/1.5 não são runtime; não liberar multiordem pela existência do módulo |
| Netting / hedging e posse | PortfolioRegistry distingue modalidade; executor e host legado usam magic | Magic não prova contribuição em posição netting nem distingue sessões que o reutilizam. F04.1 recusa netting/exchange nas entradas, fechamento de posição e alterações que dependem de magic. Cancelamento por ticket próprio continua disponível. Contribuição por ativo e ensaio de netting pendentes |
| Entrada simples | SafeExecutor + host estratégico; S09/B08 e Expert Super ATR | Mercado suportado por consumidor; filtros/armamento/conta continuam próprios. Aceites de novas combinações separados |
| Limite / stop / stop-limit | `PlacePending`, boleta manual | Tipos e validade declarados, SL obrigatório; H/S têm caminho nativo. Composição automática não herda a boleta. Cancelamento concorrente com fill e restart requerem ensaio próprio |
| Orientação normal / invertida | Schema e adaptadores específicos | Normal base; inversão somente nos perfis que a implementam. Não assumir suporte no Super ATR de referência E14 implementa a inversão única do sinal fechado no S09v2.10/59inputs; qualificação nativa pendente, perfil bloqueado. |
| Gradiente linear contrário | Laboratório/re-simulação e `Tools/research_super_atr/operations.py` | Nativo não entregue. Exige política explícita CONTROLLED_GRADIENT, stop do ciclo, tetos e reserva de todos os níveis; `block_averaging=false` não é implementação |
| Gradiente direcional | Simulação operacional G19.15 | Nativo pendente; definir níveis, volumes, exposição, validade, recuperação e encerramento antes de ativar |
| Gradiente dinâmico | Estudo chama `grid_spacing_atr=.5` com ATR anterior à entrada | É espaçamento inicial por ATR. Não é âncora que acompanha mercado. Regra de reancoragem, cancelamento/fill e reserva adicional ainda não implementadas |
| SL inicial fixo / ATR | Host e políticas iniciais; S09 fixo, estratégias e Super ATR ATR | `strategy_default` delega regras existentes; não é lista universal de modalidades configuráveis. Novos presets devem declarar fonte/barra/unidade E14 compõe pontos/ATR14 fechado na política comum; distâncias para fora do tick antes do lote e ATR obrigatório ausente recusado. Novo perfil ainda em qualificação. |
| TP pontos / R coexistindo com trailing | `HDK_PositionPolicy.mqh`, adaptador nativo e executor que preserva TP ao modificar SL | Combinações são por consumidor. Testar TP ainda presente após trailing e gatilhos simultâneos; não remover TP implicitamente E14 declara TP pontos/planned_r_v1 e trailing que conserva TP. R planejado pré-fill é distinto de fixed_r Python pós-fill; novo percurso nativo pendente. |
| Trailing fixo / indicador | S09/proteção nativa fixa e regras legadas por ATR/linha | Não compartilhados automaticamente com Super ATR. T20.8 torna `ModifySL` monotônico e recusa remoção/afrouxamento; falha preserva o pedido anterior E14 implementa pontos/ATR14, amostrado uma vez por barra fechada, intenção estável e SL monotônico; testes MQL simulados, aceite nativo pendente. |
| Alvo móvel durante a posição | Indicadores e políticas específicas têm cálculos de referência | Cálculo do alvo no fill não comprova atualização contínua. Regras gerais, frequência e transição de preset ainda pendentes E14 implementa alvo virtual ATR atualizado por barra, limitado por R real e close via executor; mantém SL/TP físicos de contingência. Perfil/ciclo nativo em qualificação. |
| Breakeven por gatilho / após parcial | Executor e `HDK_NativePartialPolicy.mqh` | BE após confirmação é próprio da parcial modular; BE não pode afrouxar trailing melhor. Confirmação depende de saldo e histórico, não do bool de envio E14 acrescenta BE por gatilho em R real, arredondado para cobrir entrada sem custos e preservando trailing melhor; confirmação nativa da nova composição pendente. |
| Parciais por lotes/fração, base inicial/saldo | `HDK_PartialPolicy.mqh`, `HDK_NativePartialPolicy.mqh`, `composition/partials.py`; S09 até duas etapas | Tester DEMO. Volumes inviáveis e estado incerto explícitos. Revalidar residual/mínimo/passo e execução parcial real; não transferir licença ao Super ATR |
| R e volume iniciais persistentes | E-13: dois hosts usam fills e SL da ordem original, selecionada explicitamente no histórico; média ponderada/volume real e saldo reconciliados. Estado por conta/servidor/magic/ativo/política/ID estável | Simulado: restart/ticket, persistência, BE/trailing e configuração alterada. Tester final pela instalação QA imutável:13 posições/39deals/9+4 etapas,44inputs/caixa reconciliados;53 fontes distintas/56 vínculos de catálogo iguais à fonte final por hash. Perfil aprovado não é atualizado automaticamente. Rede real, netting compartilhado e restart do processo pendentes |
| Saídas por sinal / tempo / sessão / risco | Host, controladores temporais, Super ATR e RiskGuard | Precedência por perfil; não universal. STOP da App bloqueia entradas/supervisão, não certifica liquidação; pausar mantém gestão da exposição E14 define risco→sessão→tempo→alvo virtual→sinal oposto→parcial/BE→proteção. Quota não liquida, sinal não reverte na mesma barra e pausa mantém gestão; perfil novo pendente. |
| Gestão probabilística | `HDK_TargetProbability.mqh`, Feed/Policy; painel manual G19.16 | Experimental. Evento alvo antes de stop, horizonte, amostra mínima, contexto e indisponibilidade próprios. Não confundir percentual de capital, stop sobre preço e probabilidade; automáticos ainda sem contrato/aceite estatístico/custos |
| Preset / UI / API / compilador | `composition/registry.py`, schema/adaptadores, `presets`, catálogo/SDK e recibo nativo | Reusar essas fontes; rejeitar módulo/combinação não declarados. Revisão/hash/fonte/binário/sessão vinculados pelo pacote e auditor; UI não concede gestão financeira E14 usa os mesmos serviços para59 inputs/managed_v1/planned_r_v1/dynamic_atr_v1; campos inativos/unidades/estimativas explícitos, ativação bloqueada até prova nativa. |
| Alteração com posição aberta | Sessão/preset efetivo; E-13 persiste gatilho/fração/BE no host legado e identidade integral no modular | Mudança incompatível é recusada sem reinterpretar a exposição. Salvar revisão não altera inputs carregados; nova sessão exige conferência. Transições gerais ainda não suportadas |
| Super ATR SA-01 | Gerador `robots/super_atr_research.py`, template e sonda C++ existentes | Multi-TF causal em variante de pesquisa, armamento explícito e gates nativos próprios. Não herda toda capacidade do framework nem comprova equivalência SA-REF-01 |

### Ordem de execução e critério de fechamento

1. T20.8 / garantias do executor: corrigir prechecks, perda de conexão e
   preservação do SL; teste dos métodos reais em plataforma simulada,
   compilação dos consumidores e distribuição Safety por hashes. Reserva
   agregada continua F-04/05, sem afirmação de fechamento integral da T20.8.
2. E-13/T19.57: âncoras iniciais e estado recuperável na gestão; parcial+BE,
   trailing+TP, saídas simultâneas e falha após intenção antes de fill.
3. SA-01/T19.23/25/29: variante própria do Super ATR usando as políticas
   comuns, capacidades efetivas, presets e leitura de volta; referência congelada.
4. F-04/05/10/11/12: orçamento/reserva concorrente e contribuição netting;
   cenário de dois robôs e bloqueio explícito de múltiplos terminais não suportados.
5. D-06…10: gradientes no executor existente, após reserva e política
   CONTROLLED_GRADIENT; contrário permanece desativado até decisão pertinente.
6. G19.16: contrato do produtor probabilístico e integração opt-in por
   consumidor; insuficiência/calibração ausente não retiram SL nem inventam valor.
7. T20.20/30: percurso integrado, replay, launcher e DEMO delimitada;
   Windows físico, netting/hedging e casos sem oportunidade continuam visíveis.

Cada recorte tem PR próprio nos repositórios afetados. Estado executável,
testes/SHAs/PRs/pins e prévia ficam na continuidade/plano F19 e no registro
central existente. Esta matriz não substitui o registry de capacidades nem
cria autorização de conta. Stop não garante perda máxima exata em gap/slippage.


### F04.1 — fundação em02/11/2026

O estado individual do RiskGuard tem formato2, identidade conta/servidor/magic,
checksum, campos completos e posse exclusiva do arquivo. Escrita/leitura,
truncamento e bloqueios foram exercitados no Tester DEMO sem negociação; o
depósito inicial é um deal de saldo e permanece separado dos deals BUY/SELL.
Arquivos legados sem servidor são preservados e bloqueiam entradas até migração
explícita. Não apagar estado para eliminar um bloqueio nem herdar aceite do
runtime congelado. Falha de gravação impede novas entradas.

A carteira de leitura do App usa `order_calc_profit` nativo em moeda da conta;
falha ou leitura indisponível não é conta vazia ou risco zero. Stop-limit usa
preço de execução, e retratos conferem identidade, tickets, volumes e stops.
Rótulo de magic continua informativo, sem comprovar posse/contribuição em netting.
A ponte permite apenas os cálculos de leitura; envio e checagem de ordem seguem
recusados por ela. O guardião exporta números desconhecidos como `null`, com
estado incompleto, em vez de JSON com NaN.

A disputa sintética de reservas60+60 contra100 foi aprovada no corpo MQL em
plataforma simulada, com IDs de64bits e retenção de envio incerto. O módulo é
trabalho em implementação: falta journal durável, conciliação para consumo,
ativo/grupo/plano/margem, identidade de intenção e integração ao executor.
Esse teste e a compilação do header não autorizam operação com orçamento agregado.
A sequência dos sete marcos permanece; o pilar continua incompleto.

### E14 — proteção comum e auditoria nativa em qualificação

S09v2.11/perfil13 compõe59 inputs no mesmo adaptador/executor/journal: SL pontos/
ATR14 fechado, TP pontos/R planejado, trailing pontos/ATR por barra, BE por gatilho,
alvo virtual ATR limitado por R real, saídas por sessão/sinal e orientação.
Prioridade: risco → sessão → tempo → alvo virtual → sinal oposto → parcial/BE →
trailing/BE. Preserva TP e média ponderada/R/volume iniciais; mudanças de preset
valem na próxima sessão. ATR usa HDK_Atr/Wilder14 nas últimas250 barras fechadas.
`planned_r_v1` pré-fill é distinto do `fixed_r` Python pós-fill, ainda pendente.

O recibo histórico agora preserva preço/volume e SL/TP do deal e da ordem em
17 dígitos científicos, vinculando tickets inteiros64, posição e estado final.
R de gestão é média ponderada dos deals menos o DEAL_SL do IN original. O CSV
arredondado e o SL pedido não substituem esse dado. Perfil13 exige histórico
completo em todos os fills e recusa o formato anterior para esse aceite.
Comparações de representação da mesma cotação mantêm o limite8ulp e menos de
meio ponto; R é calculado com os bits do histórico, nunca com o stop movido.
O teste cobre os dois casos que têm o MESMO CSV/pedido: SL normalizado pelo
broker ou preservado sem arredondamento, produzindo R inicial diferente.

Auditoria final reconcilia o diário, publica hash/contexto e cruza decisões,
pedidos imutáveis, efeitos e fills. Ações sem decisão/efeito ou intenções pendentes
não aprovam resultado. Heartbeat mantém exclusão/identidade/líder sem reprojetar
reservas; toda barreira financeira continua lendo o journal completo.

Candidato2.11 fingerprint2e0a4bf209628c86,56 fontes/SHA256
b927af079a64ea02c7429e8a1f80489916e306ece02ff0628cf2d76506f8086a.
166 testes H completos em81,069s,171 fontes inalteradas/checker171. App585 testes
em130,60s/28 módulos, incluindo consumidores de eventos anteriores; doctor offline
16OK/2avisos/0falhas.17 consumidores com compilações válidas0/0 (15 observações
idênticas reaproveitadas e dois consumidores recompilados). Wrapper59 compilado0/0.
Na fonte2.11, seis backtests cada tick conciliados: ATR com7trailing/2BE, alvo
dinâmico com2closes, saída por sessão, saída por sinal, normal e invertido com
2parciais cada. Todos com diário encerrado e recursos liberados; ticks reais
não informados. Replay ATR após o corretivo Appf8cb06f coincidiu com o MESMO
pacote/banco:3posições/6deals/7trailing/2BE, diário3entradas/9ações encerradas e
recursos liberados,153,275s. As falhas anterioresRAM85%/180s foram preservadas.
O launcher compila no perfil SDK próprio antes de inicializar o Wine visual,
com prova de saída do compilador, sem reiniciar o prefixo compartilhado.
216regressões/11módulos passaram em52,91s e doctor16OK/2avisos/0falhas sem probe.
Atalho real2.11 também criou/exportou/reabriu Manualv2/59inputs em6,49s; imagens
da própria janela conferidas. Chamada direta UI/API agora admite e finaliza
seu job no mesmo Budget configurado; runner com controle existente o conserva.
Pacote/artefatos e run_id ficam ligados à tentativa; retomada preserva backtest/
replay, recusa de recursos antecede o MQL e falha no checkpoint é explícita.
167testes/14módulos em145,31s, inclusive famílias anteriores e agentes; doctor
16OK/2avisos/0falhas sem probe. Botões nativos ainda precisam do ensaio real
nessa integração; não transferir o aceite de edição para execução GUI.
CI App28d: Linux passou; Windows7falhas/4522passaram/193não executados. Shim MQL
long do teste corrigido para int64 explícito;29testes/5,09s no Linux, Windows pendente.

Na fonteH989534c3/2.10/ecbff494a15d51b9: managed OHLC1min backtest/replay
coincidiram em53,411s/89,347s. Manualv2 criado/exportado/reaberto no Tk do atalho
foi consumido pelo SDK no MESMO banco/pacote: cada tick/replay coincidiram em
68,091s/170,452s;3posições/7deals/1parcial/BE2/trailing5, caixa1,46USD,
diário3entradas/8ações conciliado e recursos liberados. Ticks reais não informados.
Botões de execução da UI ainda não exercitados; houve override só no processo de
QA para editar/exportar, e recusa visual do perfil bloqueado observada.

Alvo dinâmico2.10 também confirmou dois fechamentos em cada tick e replay,
caixa−0,96USD. O ensaio ATR terminou no MT5 em69,372s, mas o auditor recusou R
calculado com SL do CSV: o SL histórico mantém bits que a formatação8 perde.
A correção ce96 da comparação decimal isolada não bastava. Recibos originais,
falhas e pares anteriores são preservados por fonte; source2.11 resolve a falta
do dado histórico, com qualificação nativa própria ainda necessária.

Perfil continua bloqueado (revisão qualificadaNone). Safety27b8599 detached,
SA-REF-01/runtime33c2fa1 e referências preservados; protótipo não distribuído.
Nenhum PLAY nem autorização DEMO transferida. Página privada canônica seq35:
último delta245arquivos/dois bancos/quatro sessões restaurados da nuvem, todos
os hashes/contagens de TODAS as tabelas conferidos. Ensaios financeiros2.11
posteriores à coleta estão no próximo delta. Credenciais/contas/configurações/logs brutos não integram o
resgate. Fatos por fonte em E14_COMMON_POSITION_20261102.json. Pilar segue em
execução: TP R pós-fill, recuperação/capacidade/pendentes, SA-01, orçamento
ativo/grupo/plano, gradientes, probabilístico e aceites DEMO/netting/Windows físico.
