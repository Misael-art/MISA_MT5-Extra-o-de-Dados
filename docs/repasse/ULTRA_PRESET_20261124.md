# Ultra Preset por ativo — repasse da pesquisa de 24/11/2026

O marco reproduz duas estratégias do catálogo sobre 372 cotações de um ativo sintético. Donchian (A) e S09 (B) entram no mesmo livro, com capital, risco e liquidez compartilhados. Há também uma integração de hipóteses direcionais novas do Laboratório. O replay Python tem conferência independente no histórico do terminal MQL5, inclusive após reinício. A demonstração usa dados sintéticos. O aceite do Expert no Strategy Tester e a qualificação em dois ativos de mercado estão abertos.

### Cadência de revisão do estudo

O recibo original de 26 sessões identifica `fortnightly`, com cortes de 14 dias corridos. Nas novas avaliações, `weekly`, `fortnightly` e `every_15_days` representam cortes de 7, 14 e 15 dias corridos. O campo `review_interval_calendar_days` informa o intervalo. Uma data sem cotação adia a decisão até a primeira sessão observada. Os cortes seguintes continuam ancorados no início da validação. O identificador do estudo incorpora a revisão do avaliador e resultados antigos persistidos conservam as datas observadas.

## Composição para revisão

| Referência | Revisão | Papel |
|---|---|---|
| App do diagnóstico | `0285bd4f0a4f749cf83622373f9cb815cdf32f5d` | Checkout inicial com as quatro sondas reproduzidas |
| App da pesquisa F19 | `b408f7e30a6beb9236a65588a5ca388bde8bdd66` | Base do diff desta entrega, ponta da PR 388. A série em revisão já inclui ORB nominal, livro de oportunidades e pesquisa de grade |
| HADOUKEN | `d3dd3a5e9a680d0c97d58cf160c60c4e58e88a3f` | Pin de desenvolvimento, PR 92 |
| Safety | `27b85992af4b10f4cd7687f370142ac88f515375` | Pin de desenvolvimento, PR 11 |
| App T19.52 | `e19f084` / `f1bc8e0` | Qualidade e custos, PR 389. Invalidação causal, PR 390 |
| App T19.53 | `30445d4` | Carteira, transações e retomada, PR 391 |
| App T19.54 | `1624cda` | Avaliação e compilador de pesquisa, PR 392 |
| App T20.22 | `251478c` | Consulta na GUI, SDK e CLI, PR 393 |
| App T19.52 | `b1a7d14` | Padrões exploratórios dentro da carteira, PR 394 |
| App T19.53 | `9b3abec` | Reserva de custo pendente e comparação de traços, PR 395 |
| App T19.54 | `53a190ed` | Comparação pareada de gestão, PR 398 |
| App T19.53 | `94e741ff` | Livro concorrente no MQL5, PR 399 |
| App T19.53 | `04d3f418` | Executor do terminal pelo launcher, PR 400 |
| App T20.20 | `04b2ad7c` | Reinício com o livro parcial, PR 401 |

A composição está empilhada sobre as PRs de pesquisa em revisão. O arquivo `revisoes.json` do repasse identifica a ponta publicada, as bases e os comandos dos diffs. `app.patch` parte da PR 388. Os patches de HADOUKEN e Safety são vazios contra os respectivos pins.

## Reprodução pelo launcher

Use o Python do ambiente do App. Os comandos abaixo usam Bash e uma pasta de trabalho nova. No Windows a entrada equivalente é `run.bat --cli`. O snapshot exportado preserva o namespace da fonte e as revisões econômicas. Gerar outra base com `scripts/demo_ultra.py` cria outra identidade de fonte.

```bash
# KIT aponta para o repasse descompactado. QA aponta para uma pasta nova.
mkdir -p "$QA"
cli() { bash run.sh --cli --db "$QA/replay.sqlite" --json "$@" ; }
cli commands
cli laboratory ultra-capabilities
cli analytics features snapshot-import --source "$KIT/snapshot.json"
cli laboratory ultra-freeze --definition "$KIT/campaign.json" > "$QA/freeze.json"
r=$(.venv/bin/python -c 'import json,sys; print(json.load(open(sys.argv[1]))["data"]["receipt"])' "$QA/freeze.json")
cli laboratory ultra-replay --receipt "$r" --until 199
cli laboratory ultra-replay --receipt "$r" --until 199
cli laboratory ultra-replay --receipt "$r"
cli laboratory ultra-inspect --receipt "$r" --timeline
cli laboratory ultra-compare --receipt "$r" > "$QA/comparacao.json"
ref=$(.venv/bin/python -c 'import json,sys; print(json.load(open(sys.argv[1]))["data"]["reference_receipt"])' "$QA/comparacao.json")
cli laboratory ultra-report --receipt "$r" --reference "$ref" --out "$QA/carteira.html"
cli laboratory ultra-compile --receipt "$r" --out "$QA/pacote.json"
cli laboratory ultra-verify --package "$KIT/pacote.json"
cli laboratory ultra-parity --package "$QA/pacote.json"
```

O recibo esperado é `8dfaa0edd362f8bba10ee27c43fd2c2814be0cd4b00a052e3086c1eeb7a9bb1a`. A identidade do pacote é `2d3d6b4ab0d25ab60eb752fa049b45211c0810a84952c2c52a1ce51f9809d24d`. A verificação recompila a representação a partir da campanha, do instrumento e do livro. O comando de paridade sem fita registra zero frames comparados.

Para repetir o estudo, use outra base. Importe `estudo/snapshot.json`, congele `estudo/campaign.json` com `ultra-freeze` e passe o recibo a `laboratory ultra-study-freeze --receipt ID --plan "$KIT/estudo/plan.json"`. Use o `study_id` devolvido em `laboratory ultra-study-run --study ID`. Os comandos aceitam a mesma opção global `--db`. Tanto o plano quanto o resultado ficam na base. Uma nova simulação com insumo gerado localmente pode começar com `.venv/bin/python scripts/demo_ultra.py --sessions 26 --directory PASTA_NOVA`.

## Matriz de aceite

| Requisito e tarefa | Verificação | Artefato no repasse e resultado |
|---|---|---|
| Cotação cruzada e preço de negócio (T19.52) | `test_crossed_quote_does_not_reduce_cost_or_destroy_trade` | A sonda original aceitou spread −1. O consumidor de execução rejeita a cotação cruzada e preserva o registro bruto e o negócio utilizável pelo consumidor pertinente |
| Economia ausente, vigência e conversão | `test_spec_from_symbol_info_tolerates_missing_fields`, testes de snapshot e dimensionamento | Campos ausentes permanecem desconhecidos. A carteira exige instrumento vigente, moeda coincidente e grade representável em centésimos. `historico-insuficiente.json` registra motivos |
| Relógio do custo e abertura incompleta | `test_cost_uses_utc_hour_from_declared_broker_clock`, `test_orb_reference_compares_complete_late_and_no_breakout_sessions` | Calibração e execução consultam UTC no perfil novo. A pesquisa ORB nominal WIN distingue sessão tardia. A ORB legada de primeira barra conserva sua semântica própria |
| Hipótese nova, oportunidade cancelada e incerta | `test_causal_invalidation_keeps_cancelled_opportunity_and_unknown_outcome`, `test_new_pattern_replays_causal_rules_and_counterfactual` | P02 conserva evento, hash, disponibilidade, referência e predicados. A carteira aceita o módulo `pattern_definition` e recusa universo divergente. Desfecho pode continuar desconhecido |
| Duas estratégias, admissão e risco (T19.53/T20.21) | `test_reopen_partial_then_compare_portfolio_with_counterfactual` | `linha-do-tempo.json` no ordinal 198 mostra duas reservas e um fill de A. O orçamento de liquidez é um passo por cotação. B recebe fills a partir do ordinal 200. No 216 o motivo de B é `shared_margin` |
| Proteção, meta, perda local e reset | Mesmo teste e política congelada | Stop 102 e alvo 113 para a primeira entrada. No 274 A move o stop. No 348 B atinge o limite local de duas perdas enquanto A recebe ordem. O modelo reinicia cotas no dia UTC. A sequência real de sessões de bolsa exige calendário compatível |
| Interrupção e repetição | Mesmo teste chama a CLI em processos novos | `parcial.json` após 199 cotações contém um passo preenchido e um restante em A. Repetir o mesmo limite mantém o estado. `retomada.json` fecha sem posição em USD 934,40, com oito ciclos e USD 1,60 de comissões |
| Invalidação útil e prejudicial | `ultra-compare` sobre as mesmas oportunidades | `comparacao.json` mostra perda evitada de USD 6,20 e dois ganhos descartados de USD 21,80. O braço estático encerra em USD 963,80. O filtro reduz o patrimônio em USD 29,40 e recebe `reject_on_sample` |
| Contribuição, risco e custos | Curvas por tick e nova execução retirando cada dono | `comparacao.json` inclui expectativa, PF por ciclo, DD marcado, margem/exposição, pior sessão, cauda, comissões e custo de spread contra o meio. DD máximo USD 95,30, margem máxima USD 988,00. A amostra de uma sessão não estima a cauda de mercado |
| Treino, seleção, revisão e controle sem candidato (T19.54/T20.19–20) | `test_review_only_sees_completed_sessions_and_can_select_cash` e CLI do estudo | `estudo/plan.json` congela cinco tentativas. `estudo/resultado.json` cobre 26 sessões sintéticas, com sete no treino. Caixa e políticas fixa/semanal/quinzenal terminaram com variação zero. Estratégia estática −USD 501,60, carteira estática −USD 516,00. A seleção devolve `no_candidate` |
| Incerteza e degradação | Blocos comuns de cinco sessões, 500 réplicas. Perturbação na última cotação da reserva | O livro mantém tentativas desfavoráveis, Brier quando há ciclos pontuáveis, ganho subsequente em sombra, momento da violação observável e atraso na revisão. O estudo sintético ficou em caixa e não produziu uma troca ou falso alarme observado. A seleção entre cinco alternativas gerou máximo médio nulo estimado de USD 0,44 por sessão |
| Versão de origem e reversão | Versão/gestão gravadas no ciclo e revisões com IDs de origem/destino | A avaliação conserva a gestão do ciclo aberto. A troca durável do pacote em Expert, a reversão operacional e a continuidade de tickets após reinício do terminal exigem executor vinculado e ensaio nativo |
| Interface e agente (T20.22) | Launcher Tk offline com base de QA, Brave a 1060/390 px, teclado e toque simulado, APIs do SDK | `laboratorio.png`, `carteira-*.png`, `carteira.html`, `desempenho-interface.json`. Na guia Comparativo, abra Execuções gravadas e Carteiras Ultra. Escolha recibo e referência. O detalhe de cada decisão traz cotação, risco, margem, dono e ciclo |
| Reprodução, compilação e paridade | Importação oficial do snapshot, recompilação, comparação com discrepância de quantidade introduzida | `verificacao.json` confirma o pacote de pesquisa. `paridade.json` registra `missing_tester_trace`. O teste do comparador encontra a primeira divergência em `frame.events[4].steps`, ordinal 198. Esse resultado cobre o comparador |
| Microestruturas e execução física | Inventário de cobertura e capacidades | Base oficial do App sem série de mercado qualificada. O diretório Default do MT5 na partição Windows contém arquivos de barras de exemplo. Ainda são necessários ticks negociáveis, especificações vigentes e relógio/sessões para dois ativos distintos, além da fita original do Expert |

## Medições

No Jupiter x86-64 com Python 3.12.14, a mediana de 12 calibrações no mesmo quadro residente de 372 cotações passou de 0,387 para 0,879 ms. A versão nova verificou os dois lados da cotação. O resultado médio dos dois calibradores foi um ponto. O replay calculou todos os ticks em série. Nos 41 ticks com decisões, p50 foi 15,82 µs e p95 foi 41,92 µs. Esses tempos medem o núcleo Python. A exportação, os processos novos, o SQLite e a abertura do navegador têm medições separadas.

O primeiro resumo com as decisões apareceu em 113 ms a 1060 px e em 86 ms a 390 px. A mediana do filtro foi 16,6 ms a 1060 px e 16,6 ms a 390 px. O pico de RSS dos filhos da CLI foi 196.672 KiB. Os custos de importação, congelamento, replay parcial, retomada e comparação estão em `desempenho-cli.json`. O cache do SO foi compartilhado. A linha do tempo conjunta surgiu nesta série e não tem medição equivalente na revisão anterior.

## Evidência necessária para o aceite final

1. Congelar dados de dois ativos de microestruturas diferentes, com cotações executáveis, regra de sessão, instrumento vigente, moeda, margem e custos demonstrados. Distinguir ativo sem condição comprovada de reprovação econômica e de ausência de capacidade nativa.
2. Integrar o executor conjunto ao vínculo de sessão do HADOUKEN e à reserva do Safety. O adaptador dos pins vincula um magic e exige Tester DEMO. A política testada declara preenchimento limitado por tick e margem por passo. Representar a liquidez do ativo e a margem do Tester requer evidência adicional. Netting físico, tickets de hedging, grade e outras transições conservam seus gates.
3. Executar o pacote no Strategy Tester com identificação de fonte, binário, inputs, build e símbolo. Conservar journal bruto e fita normalizada. Passar os dois a `ultra-parity --package PACOTE --trace FITA --journal JOURNAL`. A comparação é exata depois da conversão em ticks, passos de lote e centésimos. O primeiro campo divergente, inclusive evento ausente ou saldo final, deve ser investigado.
4. Executar os cortes históricos e a observação prospectiva com os critérios congelados. Medir falsas intervenções e atraso em sequências com deterioração real e recuperação. A promoção econômica e a ativação operacional continuam pendentes.

### Segundo instrumento de QA

`scripts/demo_ultra.py --instrument fractional` gera outro símbolo com preço em incrementos de 0,0001, spread de dois ticks, volume mínimo/passo de 0,1 lote e valor de USD 5 por tick e lote. Um passo vale USD 0,50 por tick. O teste `test_fractional_instrument_uses_own_price_volume_and_tick_economics` carrega esse contrato, executa o mesmo serviço e verifica fills, spread e valor monetário. A fita também é sintética.

O perfil S09 dos pins usa limite de uma posição e risco máximo de 0,5% por entrada. A primeira reserva de um módulo no exemplo é de USD 6,20 com USD 1.000 de caixa. A carteira conjunta requer novas capacidades no vínculo e no executor para representar essa política.

## Livro concorrente em MQL5

A partir da PR 398, o mesmo pacote reproduz três transições de gestão de A. Os ciclos encerraram em −USD 8,20, −USD 8,20 e +USD 15,80 tanto com breakeven quanto com o stop original. Os dois braços terminaram em USD 934,40. A comparação de invalidações tem outro controle e mediu −USD 29,40.

Com a base reproduzida acima, exporte o pacote para uma pasta nova.

```sh
cli laboratory ultra-tester-export --package "$QA/pacote.json" --out "$QA/mql5"
```

O resultado indica o nome do símbolo personalizado e os hashes dos quatro arquivos. Em uma instalação privada de QA, coloque o CSV em `MQL5/Files`, o Expert `UltraConcurrent.mq5` em `MQL5/Experts` e os dois scripts em `MQL5/Scripts`. Compile os três fontes no MetaEditor. Execute `UltraImport` num gráfico disponível e confira no journal o hash do pacote e a contagem importada. O script verifica as cotações gravadas. Em um símbolo existente, confere a grade e o conteúdo antes de aceitar a repetição.

Na conta DEMO autorizada, selecione o símbolo gerado, M1, ticks reais, um agente local, otimização desativada e o intervalo do snapshot. A demonstração usa 05/10/2026 até 06/10/2026 (fim exclusivo). O Expert recebe ticks em `OnTick`. Para conferir o mesmo núcleo pelo histórico do terminal, execute o script `UltraTerminalProbe` e guarde o journal dessa execução. Ele lê os ticks do símbolo importado com `CopyTicksRange`. Cada origem tem seu próprio marcador de abertura, registros e fechamento. Forneça o journal de uma execução completa, com o pacote e a versão de fonte que a produziram.

```sh
cli laboratory ultra-tester-compare --package "$QA/pacote.json" --origin terminal_history --journal "$QA/historico.journal.log"
cli laboratory ultra-tester-compare --package "$QA/pacote.json" --origin strategy_tester --journal "$QA/tester.journal.log"
```

| Requisito | Prova | Resultado na bancada sintética |
|---|---|---|
| Dois donos e um orçamento em MQL5 | `historico.journal.log` e `conferencia-historico.json` | Build 6324. O terminal reproduziu 372 cotações e 80 eventos. O saldo final e o patrimônio foram USD 934,40. |
| Primeira divergência | `test_first_divergence_in_partial_fill_and_cash_and_source_boundaries` | Alterar o primeiro fill parcial revela o campo e o ordinal 198. Alterar caixa, remover um evento, trocar a origem e truncar o journal também têm cobertura. |
| Identidade e reprodução | `test_export_frozen_expert_in_priority_order_and_refuse_overwrite` | A verificação vincula snapshot, pacote, revisão do código MQL5 e ordem dos módulos. O journal bruto tem SHA-256. |
| Ordens da plataforma | `ultra-parity`, livro completo e journal de execução | Pendente. Esta versão calcula fills, margem e custos pelo contrato modelado do pacote. O vínculo com tickets, callbacks de transação e retomada nativa continua em aberto. |

O comparador usa inteiros em ticks, passos de volume e centésimos da moeda. Ele confere o estado agregado após cada evento e cada cotação, a identificação de cada ciclo pelo dono e instante do sinal, os níveis de proteção e valores executados. Admite o par Donchian/S09 em M1 sintético com relógio UTC, até 10.000 cotações por journal. Os arquivos de entrada do MQL5 contêm parâmetros e cotações. Os sinais, ordens modeladas e saldos são calculados durante a execução.

### Execução pelo launcher no Linux

Com a instalação portátil configurada no App e o terminal fechado, o mesmo serviço faz a compilação, a importação e a conferência. A opção `--report` recebe uma pasta de evidências. Cada chamada abre uma subpasta independente e devolve o caminho de `receipt.json`. A descoberta de capacidades funciona offline.

```sh
cli laboratory ultra-tester-capabilities
cli robots ultra-test "$QA/pacote.json" --ultra-origin terminal_history --report "$QA/ensaios"
cli robots ultra-test "$QA/pacote.json" --ultra-origin strategy_tester --report "$QA/ensaios"
```

O ensaio usa o símbolo personalizado do pacote e um gráfico EURUSD para executar os scripts auxiliares. Se o perfil usar outro nome nesse gráfico, informe `--symbol`. O terminal recebe os scripts em subpastas identificadas por hash. A origem Strategy Tester exige um recibo de conexão DEMO, usa ticks reais e um agente local, e conserva o relatório da plataforma. `terminal_busy` identifica o terminal em uso, `demo_login_required` identifica a falta de conexão DEMO e `diverged` aponta a primeira diferença observada em uma execução. O SDK oferece `ultra_run_tester`, `ultra_export_tester` e `ultra_compare_native_book`.

### Reinício do terminal após um fill parcial

O ensaio `terminal_restart` fecha o primeiro processo depois de gravar os indicadores, o livro dos dois donos, a proteção e o cursor de ticks. Outro processo confere a última cotação recebida e retoma no tick seguinte. O recibo preserva o arquivo binário, os journals de cada processo e seus hashes. Uma nova chamada também pode partir desse recibo em um perfil portátil compatível.

```sh
cli robots ultra-test "$QA/pacote.json" --ultra-origin terminal_restart --report "$QA/ensaios"
cli robots ultra-test "$QA/pacote.json" --ultra-origin terminal_restart --report "$QA/ensaios" --resume-attempt "$RECIBO"
```

No Linux com MT5 build 6324, o primeiro processo parou após a cotação 199. O dono A tinha um passo executado e um pendente. O segundo processo reencontrou esse cursor e completou 372 cotações e 80 eventos. A carteira terminou com USD 934,40. O comparador confere os dois registros do checkpoint contra o primeiro fill parcial do livro Python. Essa evidência cobre a recuperação da carteira modelada no histórico nativo. A conciliação com tickets físicos do Strategy Tester mantém seu próprio aceite.

O replay fracionário também reabriu na cotação 199 e terminou com 81 eventos e USD 957,30 de patrimônio marcado. Uma terceira chamada usou o recibo do primeiro contrato e recuperou o mesmo saldo. O lote de 18 regressões passou no Linux, e o doctor de desenvolvimento retornou 16 verificações OK, dois avisos e zero falhas.

### Preparação do símbolo para o Strategy Tester

O modo de ticks reais do Tester precisa de histórico M1 anterior ao intervalo. A bancada cria um segundo símbolo personalizado identificado pelo contrato do pacote e pela revisão do preparador. O símbolo recebe 128 minutos de cotações sintéticas constantes no último dia útil anterior e, depois, exatamente as cotações congeladas. O Expert ignora cotações anteriores ao primeiro instante da amostra. A preparação conserva o CSV e o símbolo usados na conferência pelo histórico. [Histórico mínimo no MetaTrader 5](https://www.metatrader5.com/en/terminal/help/algotrading/test_preparation)

```sh
cli robots ultra-test "$QA/pacote.json" --prepare-only --report "$QA/ensaios"
cli robots ultra-test "$QA/pacote.json" --report "$QA/ensaios"
```

O recibo `tester_contract_verified` exige a releitura dos ticks e das barras prévias, das moedas, da grade de preço, do tamanho do contrato, dos volumes e da margem inicial e de manutenção. No Linux, o MT5 releu 128 barras prévias e 372 ticks de cada pacote. Para um passo de volume, os valores observados por tick foram USD 1,00 e USD 0,50, respectivamente. A margem inicial em ambos foi USD 247,00 por passo. Os dois contratos usam o modo de futuros sintéticos para representar os valores congelados. Essa preparação permite conferir o insumo antes do login DEMO. A aprovação da execução usa o journal do agente do Tester.

A tentativa conserva o arquivo `tester.ini` antes da autenticação. Ele fixa depósito e moeda do pacote, M1, modo 4 e agente local. A repetição no Linux verificou os símbolos já criados. Passaram 32 regressões do Ultra e do serviço do Tester no host.

### Ordens simuladas da plataforma

O pacote de pesquisa de dois donos exporta também o Expert `UltraPlatformOrders`. No Strategy Tester, ele solicita uma pendente por ciclo com stop e alvo, registra o resultado de OrderCheck e OrderSend e associa os callbacks aos tickets do histórico. O sinal Donchian/S09 e o livro de referência pertencem aos templates do App. A observação da resposta usa o header de execução do Safety registrado no recibo. Em netting, uma segunda exposição simultânea com proteções independentes recebe o veto `netting_independent_protection`. A modalidade real vem do journal. [Eventos de transação do MQL5](https://www.mql5.com/en/docs/event_handlers/ontradetransaction)

```sh
cli robots ultra-test "$QA/pacote.json" --ultra-origin strategy_tester_orders --report "$QA/ensaios"
cli laboratory ultra-tester-compare --origin strategy_tester_orders --package "$QA/pacote.json" --journal "$JOURNAL_DO_AGENTE" --out "$QA/ciclos.html"
```

Em **Laboratório → Carteiras Ultra → Conferir pacote e journal do Tester**, escolha o pacote e o journal do agente. Expanda o dono e o ciclo no HTML para consultar a regra congelada, a cotação, o orçamento, as solicitações, as respostas, os tickets e os níveis de proteção observados. Os grupos aceitam foco de teclado e toque. A CLI fornece os mesmos campos em JSON. O executor conserva também o HTML original do MT5 e confere ticket, ordem, volume, preço, custos, lucro e saldo dos negócios. Uma comparação avulsa do journal tem escopo menor que o recibo completo do executor.

Os limites da conferência são zero tick, zero passo de lote e zero centavo nas unidades do contrato. O livro modela liquidez por tick e reserva margem para pendentes. O Tester pode produzir uma sequência diferente. O relatório preserva a primeira diferença e os dados de cada ciclo. O ensaio de ordens exige perfil portátil, terminal fechado no início, conta DEMO autenticada e símbolo personalizado preparado. A retomada após parcial está demonstrada no histórico modelado. Uma execução interrompida do Expert de ordens exige um ensaio próprio.

### Ler a reserva temporal do estudo

Use `laboratory ultra-study-run --study ID` e abra `policies.<política>.reserve_only`. O patrimônio de abertura inclui a marca das posições levadas da validação. O pico para drawdown começa nessa marca. O resultado líquido inclui os custos de troca, e a comissão e o custo de spread em relação ao meio da cotação têm campos próprios. O fator de lucro considera os ciclos encerrados na reserva, com os custos desses ciclos desde a entrada. O intervalo da média líquida por sessão usa blocos móveis de cinco sessões, 500 reamostragens e semente 1. Com até cinco sessões, o recibo devolve `insufficient_sessions`. Cada alternativa congelada também tem sua reserva em `attempts`. O campo `selection_status` de cada política identifica o resultado dessa seleção. A aprovação econômica exige qualificação em dados de mercado.

### Consultar um tick e continuar o lote

`laboratory ultra-explain --receipt ID --ordinal 348` abre a chave de tempo, a regra de cada dono, as ordens, os fills e as reservas observados no tick 348, contado a partir de zero. No relatório da carteira, selecione a estratégia e o ciclo para ir à primeira decisão. As ações de decisão anterior e próxima navegam pelo ciclo escolhido.

`laboratory ultra-batch --definition primeiro.json --definition seguinte.json` emite um resultado por definição e continua depois de um ativo inconclusivo. Em **Carteiras Ultra → Capacidades do pacote**, a interface confere o pacote selecionado contra o recibo e mostra se há implementação no Tester. O agente consulta o mesmo resultado com `laboratory ultra-tester-capabilities --package pacote.json`. O pacote informa os donos e as origens disponíveis na versão consultada. A seção seguinte de topologia descreve a expansão do Expert de ordens.

### Entrada direta e gradiente linear no Laboratório

`laboratory ultra-entry-compare --package pacote.json --spacing-ticks 1` registra o pacote, o espaçamento, os limites e a identidade do avaliador antes do replay. A entrada direta e a grade reservam a quantidade integral ao preço mais adverso. Cada nível da grade admite um passo de volume. A capacidade por tick é compartilhada entre os donos. Stop, alvo e limites por dono são os mesmos nos dois braços. O comparador exige que a entrada direta reproduza cada evento e cada marca do livro congelado antes de apresentar o contraste.

Em cada ciclo o recibo distingue a perda da referência enquanto havia posição, o encerramento com referência íntegra e a entrada em novo sinal após a perda da estrutura anterior. A decisão usa o bid ou ask executável daquele tick. A seleção temporal e o atraso de detecção da degradação estatística são registrados no estudo entre sessões.

Nos controles sintéticos de 372 ticks, o gradiente de um tick terminou USD 26,70 acima da entrada direta no contrato inteiro e USD 21,35 acima no fracionário. Os resultados pertencem ao modelo de liquidez da pesquisa. A campanha de ordens do Tester desta revisão executa a entrada direta.

### Topologia do Expert de ordens

A origem `strategy_tester_orders` gera um estado de sinal e de ciclo para cada dono com sinal implementado no pacote. Os parâmetros dos indicadores e a precedência vêm da política congelada. Um período sem sinal aceito pode atravessar toda a amostra com zero ordens. A consulta de capacidades lista as origens disponíveis para a topologia escolhida. Os executáveis de controle e retomada da série anterior conservam a topologia de um dono de cada estratégia.

O preparador configura e relê as permissões de operação, ordens limite, stop e alvo no símbolo personalizado. O Expert registra um veto quando o ticket pendente ou a posição alvo da gestão já não existe. A modalidade da conta e os efeitos de cada solicitação exigem o journal produzido no agente. [Permissões do símbolo em MQL5](https://www.mql5.com/en/book/automation/symbols/symbols_trade_mode)

### Sinal de catálogo e hipótese congelada no mesmo Expert

O Expert de ordens usa a função de cruzamento `HDKS09Cross` do HADOUKEN para S09. O runner confere os dois includes da biblioteca configurada antes de compilar. O recibo de exportação relaciona os hashes do contrato. A implementação de Donchian usa os parâmetros de cada dono.

Um dono `pattern_definition` pode compartilhar a conta do Tester com os demais. O Expert forma as barras M1 com o bid e avalia a hipótese no primeiro tick após o fechamento. O subconjunto disponível inclui OHLC, fechamento anterior, comparações de preços na grade, composição booleana e cancelamento antes da entrada. O journal identifica o dono, o lado, o preço de referência, o instante de disponibilidade, a regra e seu valor verdadeiro, falso ou desconhecido. A trilha compara esses valores com a definição congelada e abre os dados do ciclo. A CLI e a interface identificam os sinais do pacote e informam o motivo de recusa de uma expressão sem implementação nativa.

No pacote sintético com os donos H e B, o Laboratório observou 11 armações de H, 4 invalidações, 6 ordens modeladas, 12 preenchimentos modelados e 5 saídas em 372 ticks. A conferência desses eventos com ordens físicas requer o journal do agente e o relatório de negócios da mesma execução.

Quando a seleção qualificada contém zero estratégias, o compilador deriva do snapshot as marcas de uma carteira integralmente em caixa. O recibo de origem conserva o resultado `no_candidate`. A plataforma pode percorrer os ticks com zero donos. O comparador distingue esse caso de `no_confirmed_signal`, no qual existia um dono e seu sinal nunca ocorreu.


### Seleção da conta DEMO no perfil de QA

Em instalações nas quais um arquivo de inicialização alternativo abre o MT5 sem a conta salva ativa, forneça o número da DEMO existente em `MT5X_ULTRA_DEMO_LOGIN` no ambiente do launcher. A sonda e o Tester usam a mesma seleção e o MT5 recupera a autenticação do perfil. O recibo conserva o resultado da sonda e se a seleção foi explícita. A configuração anexada ao recibo continua reproduzível sem dados de conta. [Inicialização da plataforma](https://www.metatrader5.com/en/terminal/help/start_advanced/start)


### Ordens limite executáveis no símbolo sintético

No ensaio original hedging, o agente preservou os 372 ticks e os 80 eventos do livro modelado. A primeira solicitação de compra foi recusada pelo Tester com retorno 10015, embora o preço limite de 105 coincidisse com o ask do tick. O histórico nativo e o relatório registraram zero negócios e saldo USD 1.000,00.

O preparador desta revisão cria outro símbolo sintético com execução Exchange para reproduzir o limite executável. Ele relê esse parâmetro junto com preços, volume e margem. O Expert valida a modalidade de execução antes de enviar solicitações. A versão da preparação faz parte da identidade do símbolo, portanto o ensaio anterior pode ser conferido pelo seu próprio journal e relatório. [Ordens limite em execução Exchange](https://www.mql5.com/en/book/automation/experts/experts_order_type)


O primeiro teste Exchange passou pela validação do preço e retornou 10016 nas proteções. O novo símbolo da bancada preserva bid e ask do pacote e acrescenta Last sintético, dado pelo ponto médio arredondado para baixo na grade. A preparação relê os três preços e registra a derivação. Nenhum tick da fonte é substituído. O journal informa bid, ask, Last, nível mínimo de proteção e retorno de checagem quando a solicitação falha. [Ativação de ordens no Tester](https://www.metatrader5.com/en/terminal/help/algotrading/testing_features)

### Fatias físicas, checkpoint e troca de preset no Tester

O ensaio `strategy_tester_sliced` mantém a capacidade por tick do pacote. Ao liberar uma fatia, o Expert envia uma ordem a mercado FOK com stop e alvo, no símbolo sintético de execução instantânea e com latência zero. O preço precisa coincidir com o bid ou ask executável e respeitar o limite do ciclo. A quantidade ainda por executar permanece reservada no livro. O comparador confere a proteção de cada ticket, a margem das posições físicas e a reserva do livro em seus registros próprios. [Políticas de volume do MQL5](https://www.mql5.com/en/book/automation/experts/experts_execution_filling)

No primeiro preenchimento parcial da intenção, o Expert salva o cursor, os sinais e o livro, zera o estado em memória e relê o checkpoint. O Tester continua na mesma execução, com os tickets físicos abertos. A carga compara posição, volume, stop, alvo, saldo, patrimônio e margem. Em seguida, o ensaio ativa uma versão com outro limiar de breakeven. O ciclo em andamento conserva a regra de origem e as novas intenções identificam a regra vigente. Encerrar o Tester finaliza a sessão simulada. A validação de retomada deste modo ocorre dentro da mesma sessão. [Ciclo de vida do Expert no Tester](https://www.mql5.com/en/docs/common/expertremove)

Execute a preparação e consulte `tester_contract.symbol` no recibo. Nas configurações avançadas do Tester, escolha hedging e configure a comissão instantânea do símbolo em moeda do depósito, de acordo com a unidade do pacote. Nos dois controles de um passo por fatia, use a faixa de 0 a 100 lotes e USD 0,05 por negócio, em entradas e saídas. Feche o terminal de QA para executar o launcher. [Configurações de comissão do Tester](https://www.metatrader5.com/en/terminal/help/algotrading/testing#advanced)

```sh
cli robots ultra-test "$PACOTE" --ultra-origin strategy_tester_sliced --prepare-only --report "$RECEBOS"
cli robots ultra-test "$PACOTE" --ultra-origin strategy_tester_sliced --report "$RECEBOS"
cli laboratory ultra-tester-compare --origin strategy_tester_sliced --package "$PACOTE" --journal "$JOURNAL_DO_AGENTE" --out "$CICLOS"
```

No UltraQA Linux/Wine, os controles inteiro e fracionário percorreram 372 ticks cada. A primeira fatia executou no tick 199, com um passo físico e um passo reservado. O checkpoint preservou o ticket e a proteção. A troca manteve o limiar de breakeven 4 no ciclo aberto e aplicou o limiar 3 aos novos ciclos do dono. O journal e o HTML concordaram em 32 negócios e USD 946,40 no inteiro. No fracionário, concordaram em 36 negócios e USD 957,20. O patrimônio marcado antes do encerramento fracionário foi USD 957,30. O Tester fechou as duas posições restantes com USD 0,10 de comissão, em negócios identificados em separado no recibo.

O repasse incremental contém o perfil de comissões de cada símbolo. Em outro perfil portátil de QA, importe esse arquivo nas configurações avançadas do Strategy Tester depois da preparação. A comissão efetivamente cobrada consta nos negócios do agente. O recibo registra divergência quando o custo observado difere do custo congelado.
