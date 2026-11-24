# Ultra Preset por ativo — repasse da pesquisa de 24/11/2026

O marco reproduz duas estratégias do catálogo sobre 372 cotações de um ativo sintético. Donchian (A) e S09 (B) entram no mesmo livro, com capital, risco e liquidez compartilhados. Há também uma integração de hipóteses direcionais novas do Laboratório. A execução demonstrada é um replay Python. A evidência econômica vem de QA sintética. O aceite do Expert no Strategy Tester e a qualificação em dois ativos de mercado estão abertos.

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

O recibo esperado é `8ab608c878adc60ad7f5acd7013bbe29414f0027e26d3c29df177fa9a721c16a`. A identidade do pacote é `eab97e9444b2148c3ca42e57b5cecc74b506aebf95137e0e62dc40c70e2d7ba2`. A verificação recompila a representação a partir da campanha, do instrumento e do livro. O comando de paridade sem fita registra zero frames comparados.

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

No Jupiter x86-64 com Python 3.12.14, a mediana de 12 calibrações no mesmo quadro residente de 372 cotações passou de 0,560 para 1,232 ms. A versão nova verificou os dois lados da cotação. O resultado médio dos dois calibradores foi um ponto. O replay calculou todos os ticks em série. Nos 41 ticks com decisões, p50 foi 22,55 µs e p95 foi 60,80 µs. Esses tempos medem o núcleo Python. A exportação, os processos novos, o SQLite e a abertura do navegador têm medições separadas.

O primeiro resumo com as decisões apareceu em 121 ms a 1060 px e em 104 ms a 390 px. A mediana do filtro foi 16,7 ms nas duas larguras. O pico de RSS dos filhos da CLI foi 142.000 KiB. Os custos de importação, congelamento, replay parcial, retomada e comparação estão em `desempenho-cli.json`. O cache do SO foi compartilhado. A linha do tempo conjunta surgiu nesta série e não tem medição equivalente na revisão anterior.

## Evidência necessária para o aceite final

1. Congelar dados de dois ativos de microestruturas diferentes, com cotações executáveis, regra de sessão, instrumento vigente, moeda, margem e custos demonstrados. Distinguir ativo sem condição comprovada de reprovação econômica e de ausência de capacidade nativa.
2. Integrar o executor conjunto ao vínculo de sessão do HADOUKEN e à reserva do Safety. O adaptador dos pins vincula um magic e exige Tester DEMO. A política testada declara preenchimento limitado por tick e margem por passo. Representar a liquidez do ativo e a margem do Tester requer evidência adicional. Netting físico, tickets de hedging, grade e outras transições conservam seus gates.
3. Executar o pacote no Strategy Tester com identificação de fonte, binário, inputs, build e símbolo. Conservar journal bruto e fita normalizada. Passar os dois a `ultra-parity --package PACOTE --trace FITA --journal JOURNAL`. A comparação é exata depois da conversão em ticks, passos de lote e centésimos. O primeiro campo divergente, inclusive evento ausente ou saldo final, deve ser investigado.
4. Executar os cortes históricos e a observação prospectiva com os critérios congelados. Medir falsas intervenções e atraso em sequências com deterioração real e recuperação. A promoção econômica e a ativação operacional continuam pendentes.
