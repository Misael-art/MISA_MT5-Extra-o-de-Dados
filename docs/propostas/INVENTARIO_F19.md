# T19.1 — Inventário reconciliado e decisões de contrato

Recorte documental de 2026-10-17, para revisão. Não implementa F19/F20 nem
qualifica operação financeira. T20.1 estenderá **este mesmo mapa** com Q01–Q16,
seus testes e consumidores; sua matriz ainda não foi executada aqui.

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
