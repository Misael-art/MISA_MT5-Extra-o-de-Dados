# Laboratório de séries e confluências — T20.22-LAB1

**Status após teste do proprietário em24/10/2026: incompleto, aceite funcional
reaberto.** [Pendências e critérios](../propostas/LAB_SUPER_ATR_PENDENCIAS_20261024.md).
Os percursos abaixo descrevem a implementação inicial e seus limites, não
comprovam funcionamento em todas as seleções reais do atalho central.

Entrega inicial solicitada em24/10/2026, sobre AIA-02a. A aba **Laboratório →
Séries e confluências** usa séries reais da base, congeladas por `datasets`.
Em **Relações e aderência**, Inventário e a matriz **Indicador × indicador** são
reais: a matriz é `service.relation_matrix` sobre a análise congelada em Séries
(a mesma do CLI `laboratory matrix`). Indicador × alvo, linha do tempo, ganhos e
perdas, convergências e candidatos continuam fixture F18 demonstrativa, pois o
alvo T18.6 ainda não tem produtor. Análises do livro de operações são separadas.

## Visões e percurso

1. Atualizar fontes; escolher ativo, TFs, janela, estratégia/parâmetros e indicadores.
   Parâmetros têm campos numéricos rotulados, limites do registry e aceitam vírgula
   decimal; o contrato da estratégia é validado antes da captura.
   Os TFs seguem duração crescente. Ausência de M5, por exemplo, não cria M5 a
   partir de M1 silenciosamente. A frente de dados mantém a derivação/coleta.
2. **Congelar e analisar** cria revisões pelo store de snapshots existente e
   calcula as fórmulas registradas. Nenhum backtest, ordem, download ou otimização
   é disparado. Alterar a seleção mantém o resultado congelado identificado;
   uma resposta de seleção antiga não substitui a atual.
3. **Preço e indicador:** candles da base e valores originais, no mesmo relógio.
   Linhas de preço sobrepõem o preço apenas quando seu contrato permite; demais
   unidades ficam em painel próprio. Quebras de tempo interrompem as linhas.
4. **Estratégia entre tempos:** intenções fechadas da mesma estratégia e mesmos
   parâmetros em todos os TFs. São candidatos, não trades ou posição persistente.
5. **Indicadores × estratégia × TF:** matriz de correlação das direções canônicas,
   escala fixa de−1 a+1 e ausências distintas de zero. Dicas mostram identificadores,
   amostra e motivo de indisponibilidade.
6. **Defasagens:** preditor selecionado no detalhe, respostas de outros canais,
   atrasos0/1/3 barras da grade. As duas orientações, inclusive TF maior→menor,
   estão no JSON. A tela mostra os primeiros cinco pares na ordem do catálogo,
   sem ordenar retrospectivamente pelo melhor efeito.
7. **Cobertura:** barras da grade com valores calculáveis; detalhe da seleção
   mostra requisitos, motivos, unidade e aquecimento. Cobertura não certifica fonte.

**Visão ampliada** organiza duas colunas e gráficos maiores; inicia habilitada
quando monitor **e janela** têm largura>1920 **e** altura>1080. Uma janela
compacta num monitor grande continua compacta até a escolha do usuário. Em tela menor,
a ampliação mantém rolagem horizontal e a página mantém rolagem vertical.
Cada gráfico tem **Ampliar/Restaurar**: detalhe no monitor da janela lançadora,
toolbar de zoom/pan e Escape para voltar, usando o mesmo resultado. Não altera
resolução do host nem manipula janela alheia. Navegação por nomes completos é
reutilizada quando as abas não cabem. Claro/escuro usam os tokens existentes.

Para impedir que um sinal raro desapareça na redução de milhares de colunas da
linha temporal, cada intervalo visual conserva compra/venda; se houver ambos,
mostra roxo, **não neutro**. Essa agregação é apenas gráfica, com intervalo na
dica. Métricas e JSON conservam todos os valores. A matriz de Séries usa direções
canônicas; a matriz Indicador × indicador em Relações usa Spearman dos valores
originais (postos, sem converter unidades). Regra direcional e legenda são
explícitas no canal. Isso é uma convenção descritiva, não um gatilho financeiro novo.

## CLI e SDK, sem desktop

### Canais armazenados no Laboratório (BD19a-LAB)

**Atualizar fontes** lê também o catálogo canônico da base. A busca mostra nome,
ID, versão e unidade de cada definição registrada, inclusive sem fórmula local.
O filtro de origem separa cálculo local de fatos armazenados; filtrar não apaga
a seleção. **Limpar seleção** retira também opções ocultas pelo filtro. Para
analisar somente fatos, escolha **Sem estratégia**, limpe a seleção e marque as
versões desejadas. Uma versão sem fatos continua visível e produz diagnóstico
de ausência; nenhum cálculo/materialização é disparado para preenchê-la.

**Congelar e analisar** inclui os pares `(ID, versão)` no snapshot4 existente.
`service.analyze(..., stored_features=[(ID, versão)])` usa exclusivamente seus
contratos/histórico com `indicator_context.from_snapshot`. Em cada fechamento da
grade mais curta, aplica disponibilidade e validade declaradas, inclusive quando
a publicação ocorre entre fechamentos do TF de origem. Revisões tardias não
antecipam informação; observação antiga não substitui uma mais recente. Fora
da janela de cada snapshot o canal fica ausente. Reabrir o JSON não consulta a
base nem recalcula fórmulas; ID da análise incorpora seleção/versões/manifests
e código do adaptador. Fórmulas locais e preços mantêm sua base nominal, sem
certificação point-in-time do conjunto.

Valores numéricos mantêm unidade e parâmetros da versão, sem conversão. Entram
no gráfico em painel da unidade e nas matrizes de Spearman já existentes.
Saída categórica/booleana ou tipo não declarado fica fora das métricas numéricas
com `NON_NUMERIC_STORED_FEATURE`; `raw_values`, estados e `stored_available`
preservam os fatos. `available` conta valores numéricos na grade, enquanto
`source_available` conta observações distintas usadas; retenção não cria novas
observações. `frozen_revision_count` informa revisões capturadas, não amostra
independente. `source_ends` desses canais representa disponibilidade declarada,
e `observation_times` conserva o instante descrito pelo fato.

Não há regra direcional inferida: direções permanecem ausentes, pares direcionais
trazem `NO_DIRECTION_RULE`, e esses canais não geram candidatos/presets ou intenção
de compra/venda. Pivôs armazenados podem ser desenhados, mas ficam fora das
relações com `UNVERIFIED_STORED_EVENT` até qualificar identidade/confirmação de
eventos no adaptador; pivôs locais confirmados conservam seu contrato.

Esta etapa aceita snapshots de TFs nativos. Selecionar esses canais com M7/M9
sintéticos é recusado antes do trabalho, com orientação; não são ignorados.
Limites de32 canais,8 TFs,20.000 barras/TF e orçamento do contexto permanecem.
O botão fica desabilitado durante a análise e o indicador global do `run_bg`
mostra atividade/tempo decorrido; não estima percentual sem progresso mensurável.
Visões direcionais sem regra exibem explicação e encaminham para os valores,
em vez de sugerir ampliar a janela ou deixar apenas uma área cinza.

```sh
python -m mt5_extracao.cli --json laboratory freeze --symbol EURUSD --tf M5 \
  --stored-feature ID VERSAO
python -m mt5_extracao.cli --json laboratory analyze --manifest MANIFEST_ID \
  --stored-feature ID VERSAO --as-of 2026-10-01T03:00:00Z --out analise.json
```

Repita `--stored-feature` para várias versões; combine com `--feature` para
fórmulas locais explícitas. Sem `--feature`, a presença de `--stored-feature`
seleciona somente fatos, sem acrescentar os cinco indicadores locais padrão.
`--strategy-only` com fatos é recusado. Sem a opção nova, os padrões anteriores
permanecem. Essa integração não encerra BD19a completo, compostos/relações BD18b,
aceite financeiro, prova do relógio da corretora ou Windows físico.

```sh
python -m mt5_extracao.cli --json laboratory catalog
python -m mt5_extracao.cli --json laboratory sources
python -m mt5_extracao.cli --json laboratory freeze --symbol EURUSD \
  --tf M15 --tf H1 --tf H4 --start 2026-07-01 --end 2026-09-30T23:59:59
python -m mt5_extracao.cli --json laboratory analyze \
  --manifest ID_M15 --manifest ID_H1 --manifest ID_H4 \
  --strategy ema_atr --params '{"fast":20,"slow":50,"trend":200,"stop_atr":2.0,"trail_atr":3.0}' \
  --feature momentum.rsi_14 --feature volatility.bb_20_position \
  --as-of 2026-10-01T03:00:00Z --out diagnostico-novo.json
```

```sh
python -m mt5_extracao.cli --json laboratory matrix --analysis diagnostico-novo.json --lag 3
```

`matrix` lê só o JSON exportado (sem base): Spearman dos valores alinhados na
grade nominal, preditor deslocado `--lag` barras (0, 1, 3 ou 6), com correlação de
direção, veredito fora da amostra e IC copiados do par do relatório. Célula sem
efeito traz `reason` distinto: `UNCONFIRMED_EVENT` (pivô provisório),
`SAME_SERIES` (diagonal com defasagem), `NO_OVERLAP`, `INSUFFICIENT_PAIRS`,
`CONSTANT_SERIES` (zero válido constante não é ausência) ou o diagnóstico do canal
vazio (`WARMUP_EXCEEDS_WINDOW`, `INELIGIBLE_DATA`, `NO_VALUES`).

Relógio: `freeze` recusa antes de congelar quando o fuso do servidor não está
declarado e explica a ação. Um `broker_utc_offset` fixo só é correto se o
servidor não muda de horário dentro da janela; servidor com horário de verão
exige `broker_timezone` com `time_basis = utc`, que `datasets.capture` ainda não
aceita (contrato pendente na frente de Dados).

Os IDs vêm da resposta de `freeze`. `catalog` não abre banco, Tk ou MT5.
`sources` é leitura; `freeze` é escrita explícita no store canônico, sem tabela
paralela. `analyze` reabre os manifests e não altera fontes. `--strategy-only`
dispensa indicadores; sem `--feature`, usa cinco definições padrão do catálogo.
`--out` e a exportação da UI recusam sobrescrever arquivos existentes.

O congelamento desta visão continua em `DatabaseManager.data_inventory` com TFs
nativos do enum, consumidos por `datasets.capture`. M7 e M9 não entram nesse
enum. A leitura gráfica usa o que já está em `_bars_derived`: `laboratory.derived`
chama `analytics.resampling.latest_bars` e desenha só a última revisão fechada.
O rótulo é sintético, âncora 00:00 do calendário da corretora, não é tempo do
MT5. A UI não agrega, não preenche buraco e não mistura esses tempos com a
captura nativa. Um tempo sozinho é só a leitura, sem indicador. Dois ou mais
tempos de dia exato comparam a mesma seleção na grade do fechamento mais curto:
defasagem 0 descreve, defasagens 1/3/6 são candidatas preditivas, e um intervalo
sem grupo completo não vira candle nem par. CLI: `laboratory derived --relations`.
A seleção inicial marca só tempos nativos cuja série inteira
cabe em 20 mil barras; um tempo maior aparece na lista com a contagem e, ao
congelar sem janela, preenche De com o início das últimas 20 mil barras em
vez de carregar a série toda. Recarregar as fontes não descarta uma análise
que ainda está em curso se a seleção marcada não mudou. Sem `--relations`, `laboratory derived` só lê. Sessões e fatos materializados com
disponibilidade real continuam no Analytics/BD18a. “Sem série compatível”
nesta visão não significa que a base inteira esteja vazia.

```python
from mt5_extracao.laboratory import analyze, capture_selection, catalog

snapshots = capture_selection(db, symbol="EURUSD", timeframes_selected=["M15", "H1"])
diagnostic = analyze(snapshots, strategy_key="ema_atr",
                     feature_ids=["momentum.rsi_14"], as_of="2026-10-01T03:00:00Z")
```

### Congelar versões persistidas de indicadores (BD19a-SNAPSHOT)

`capture_selection(..., stored_features=[("ID", "VERSAO"), ...])` inclui contratos e histórico
de revisões no mesmo mecanismo de snapshots. A CLI usa pares explícitos, repetíveis:

```sh
./run.sh --cli laboratory freeze --symbol QA --tf M5 --stored-feature qa.metric v1
```

A captura opcional usa formato4; sem `stored_features`, conserva o formato3. Os formatos1/2/3
continuam legíveis e não recebem fatos da base atual ao serem abertos. Aplicações anteriores ao
formato4 precisam ser atualizadas para lê-lo. Não há outro banco ou store: os novos papéis usam
os blocos deduplicados, manifests, exportação e importação existentes.

`snapshot.feature_definitions` devolve os contratos capturados por ID/versão, incluindo unidades,
parâmetros, tipos e situação dos nomes. `snapshot.feature_history` devolve uma cópia dos fatos,
com disponibilidade, revisão e qualidade. Modificar essas cópias não altera o snapshot. Versão
não registrada é recusada; nome legado não é reconstruído nem renomeado. Um snapshot antigo
sem essas coleções exige nova captura explícita e nunca consulta a base atual para completá-las.

Barras, contratos e fatos são lidos na mesma transação consistente. Congelar indicadores exige
offset fixo declarado; isso não verifica o relógio real do servidor. A janela inclui as observações
entre a primeira abertura e o fechamento da última barra selecionada, com todas as suas revisões.
Disponibilidades posteriores permanecem registradas e devem ser filtradas por decisão; não se
preenche o início com observações anteriores à janela. A captura recusa mais de200.000 fatos por
manifesto, limitando a leitura SQL antes da publicação. Não trunca nem publica uma revisão parcial.

Esta etapa fornece a captura/reabertura; o consumo posterior por `analyze` e pela UI está
descrito em [BD19a-LAB](#canais-armazenados-no-laboratório-bd19a-lab). Barras descritivas não
se tornam historicamente point-in-time por incluir fatos.

Validação local de29/10:baseline48, rodada final171 testes e mais1 caso de zero/expiração após
serialização, total172 distintos; compatibilidade de formatos e
concorrência WAL, limite, categorias/booleanos, expiração/metadados e rejeição de pacotes incoerentes
mesmo com hashes recalculados. Launcher/CLI com dados sintéticos: valor atual12→99, snapshot
antigo12 preservado; exportar/importar/reexportar conserva bytes. Bancada1.000barras/2.000revisões,
medianas de3 capturas aquecidas: formato3 padrão269ms/80.529bytes; opcional4 com fatos624ms/
552.682bytes. É custo adicional medido, sem promessa de aceleração. Recibos locais
`state/qa-tmp/catalog-store/snapshot-*`; sem aceite de conta/MT5/Windows físico ou financeiro.

### Contexto por decisão no snapshot (BD19a-CONTEXT)

```sh
./run.sh --cli --json analytics features context --symbol QA --tf M5 \
  --manifest MANIFEST --as-of 2025-01-01T00:05:00Z
```

Com `--manifest`, o contexto usa os fatos e contratos congelados no formato4. Os argumentos
de ativo/TF precisam concordar com o snapshot. `--features` restringe os IDs capturados; sem
esse argumento, seleciona os IDs congelados. Se um ID tiver várias versões, exige
`--algorithm-version`; não escolhe a mais recente. Versão ausente ou snapshot antigo exige
nova captura explícita, sem completar a consulta com o catálogo ou os fatos da base atual.

O adaptador interno `indicator_context.from_snapshot` compartilha o mesmo `asof_join` e a
construção de valores/estados do contexto existente. Preserva unidades, parâmetros, categorias,
zero, validade e a ordem das decisões. Acrescenta `snapshot_manifest_id`, `base_namespace`,
`base_provider` e `base_server` da origem capturada, inclusive após importar em outro banco.
Decisões com identidade conflitante são recusadas. Sem `--manifest`, o modo atual permanece.

As decisões precisam declarar fuso e ficar entre a primeira abertura e o último fechamento
nominal congelado, inclusive; fora dessa janela a consulta é recusada. A disponibilidade e a
expiração são aplicadas em cada decisão, sem extrapolar a cobertura ou inventar uma validade
para fatos sem `valid_until`. O limite por consulta é200.000 pares indicador/decisão. Isso não
certifica a atualidade/completude do feed nem a disponibilidade histórica dos preços.

Baseline65, final153 testes distintos: consulta sem acesso ao banco/registry atuais, importação
com outro namespace/offset, versão ambígua/ausente, tipos, expiração, escopo, limites e fronteiras
UTC de M1/W1/MN1. Launcher/CLI sintético: manifest antigo12, base atual99; ambas as consultas
conservam seus resultados. Bancada2.000revisões/500decisões,3 chamadas aquecidas: contexto atual
101ms e congelado68ms, frames numéricos/metadados iguais; carregar o snapshot custou327ms
separadamente. Amostras se sobrepõem, sem promessa de aceleração universal. Recibos locais
`state/qa-tmp/catalog-store/frozen-context-*`; doctor offline e prévia têm recibos próprios.
Seleção/relatório na UI e os limites por tipo são descritos em
[BD19a-LAB](#canais-armazenados-no-laboratório-bd19a-lab).

### Descritiva × preditiva (LAB-07)

Cada par traz `relation`: `DESCRIPTIVE` na defasagem 0 (co-movimento) e
`PREDICTIVE_CANDIDATE` com defasagem 1/3/6 (o preditor antecede o alvo na grade).
Os preditivos trazem `out_of_sample`: corte cronológico fixo 70/30 (não
otimizado), correlação e n de ajuste e fora da amostra e `verdict`
(`HOLDOUT_CONSISTENT`, `HOLDOUT_SIGN_FLIP`, `HOLDOUT_WEAK`, `FIT_WEAK`,
`WITHIN_NULL`, `NO_GAIN_OVER_PERSISTENCE`, `HOLDOUT_INSUFFICIENT` com menos de 30
pares). Dois controles no trecho fora da amostra: `null_band_95` (quantil 95% de
|r| sob deslocamentos circulares do preditor, que preservam autocorrelação e
retenção) e `naive_holdout_correlation` (o próprio alvo com a mesma defasagem).
Só é consistente o que supera ambos e cujo `holdout_ci_95` (bootstrap de
blocos móveis, 200 reamostras, semente fixa) exclui zero; senão `CI_INCLUDES_ZERO`.
Defasagens preditivas fixas: 1, 3 e 6 barras da grade (`PREDICTIVE_LAGS`). Campos aditivos: o `analysis_id`
de seleções existentes não muda. Consistente fora da amostra não é previsão
calibrada, lucro nem validação de estratégia.

Eventos não confirmados: canais de pivô (`structure.*`, fractais e ZigZag causal)
trazem `events`. Em top/bottom, `basis=confirmation_bar_close`: o valor só existe
a partir do fechamento da barra de confirmação, nunca na origem; `confirmations`,
`delay_bars` (mín/mediana/máx origem→confirmação) e
`bars_since_last_confirmation` expõem atraso e envelhecimento do último pivô
retido. A saída `provisional` é desenho (`usable=false`): diagnóstico
`UNCONFIRMED_EVENT` e todos os seus pares saem com `reason=UNCONFIRMED_EVENT`,
sem correlação. Demais canais têm `events=null`.

### Estudo do gráfico (LAB-08)

A bancada gráfica grava um estudo JSON v2 (`laboratory/annotations.py`):
`analysis_id`, identidade (ativo, TF da grade, seleção, e por série id/nome/TF/
unidade/parâmetros/versão/fonte), anotações (`hline`, `vline`, `region`, `note`),
séries ocultas e removidas da vista e a vista (zoom `xlim` e cursor). Ocultar ou
remover uma série altera só o estudo, nunca o relatório congelado. Reabrir recusa
estudo de outra análise ou que cite série ausente; o v1 da prévia anterior ainda
é lido até a v2 ser aceita.

```sh
python -m mt5_extracao.cli --json laboratory study \
  --analysis diagnostico-novo.json --study estudo.json
```

O comando valida e resume (anotações, séries visíveis/ocultas/removidas, vista)
sem Tk. O Estudo MT5 (`chart_study`) segue com outra responsabilidade: captura
e notas sobre o gráfico do terminal; o estudo do Laboratório não o substitui nem
abre uma janela MT5.

O workflow AIA-01 descobre esses comandos no argparse real e os executa por
`cli.run`, com os mesmos envelopes/erros, sem shell ou mouse. Não há outro
scheduler, cache, feature store, journal ou servidor.

O relatório v1 inclui `analysis_id`, hash das fontes de cálculo, seleção de
manifests/parâmetros/corte, relógios, origem, qualidade, sementes, valores,
direções, timestamps das barras de origem e pares. `source_rows/source_available`
e `candidate_counts` contam barras **do TF de origem**; `available/total` e
`n_pairs` contam a **grade alinhada**. Uma intenção H4 retida em16 instantes M15
continua um candidato de origem, não16 entradas ou observações independentes.
Cada par explicita quantas barras distintas de cada lado foram reutilizadas.

## Fronteiras matemáticas e de dados

- Um ativo, mesma origem lógica/provider/server/offset declarado, até8 TFs,
 32 canais e20mil barras por TF. Recortes maiores são recusados explicitamente;
 não há truncamento escondido. Os limites contêm esta análise, não substituem
 orçamento de campanha ou Budget dos backtests.
- Cada fórmula reutiliza `analytics.indicator_registry`; estratégias usam
 `gerar_sinais` e validação tipada da composição existente. O registry resolve
 períodos/fórmulas/ausências; não há cópia dessas fórmulas no widget.
- A grade usa fechamentos do menor TF. Em cada ponto, join somente para trás
 pelo **fechamento nominal**, com validade até o próximo fechamento nominal da
 origem. Buracos não viram valor retido indefinidamente; barras ainda abertas,
 relógios desconhecidos, sobreposição, duplicatas ou origem incompatível recusam.
- Esta reconstrução não é o leitor point-in-time BD18a. Sem o histórico de
 recebimento/revisão, um fechamento nominal não prova que o dado estava disponível
 naquela data. Os flags de disponibilidade histórica/independência/aceite continuam
 falsos. Não alimenta OOS, reserva intocada, treino ou escolha automática de preset.
- Qualidade do snapshot e do prefixo fechado são verificadas. Volume real
 desconhecido/zero não ganha fallback para ticks. Warmup/NaN/constante e amostra
 insuficiente não viram correlação zero ou perfeita.
- Pearson sobre direção mede associação descritiva. Retenção, autocorrelação,
 neutros, regimes e múltiplas tentativas impedem interpretar isso como taxa de
 acerto, causalidade, probabilidade calibrada ou promessa de lucro. Acordo total
 e acordo somente com ambos ativos são separados, sem p-valores improvisados.
- Cada TF conserva a semente da sua janela. Isso não prova seed global, paridade
 Profit/MT5, estabilidade em janela móvel1500, fills ou SL/TP aplicados.

## Gerador de presets integrado

**Gerar preset…** leva o ativo e um TF do resultado congelado ao fluxo
Default/Manual existente da aba Robôs. Escolha o Expert e revise seus próprios
parâmetros: uma estratégia exploratória Python não autoriza uma tradução implícita
para outro Expert. A validação de versão/pin/inputs/closure ocorre antes de abrir
o editor. Falha restaura a seleção anterior; sucesso identifica o novo contexto
também no card. Nome, revisão, conflitos concorrentes, `.set`+manifesto,
exportação/importação e recibo carregado permanecem nos serviços atuais.

Para IA, os métodos `presets.default/save/revise/export/import/select` do workflow
já oferecem esse mesmo gerador sem janela. Não foi criado um segundo schema de
preset. Gerar/exportar não inicia robô nem aprova estatística ou execução.

**Conferência da biblioteca instalada em24/10:** os seis perfis encontrados
(S09, Pullback, S10, VWAP, Madrid e HA/HiLo) foram recusados pelo contrato de
fonte/versão/fingerprint/inputs. A closure H de desenvolvimento avançou e não é
uma das snapshots aprovadas desses perfis. A ponte visual/CLI está disponível;
geração operacional sobre essa closure permanece bloqueada. Nenhum pin ou risco
foi relaxado nesta entrega. O próximo recorte operacional deve qualificar um
pacote completo por perfil/revisão (começar por S09), preservar versões anteriores,
compilar/conferir inputs/recibo e registrar seus próprios aceites. Não substituir
a biblioteca atual inteira por headers antigos para contornar esse bloqueio.

## Inventário, investigações e próximos recortes

Nesta ponta,52 definições Analytics e11 estratégias foram descobertas do código.
Foram exercitadas em histórico EURUSD numa base privada de QA, lido do banco do
proprietário com SQLite `mode=ro/query_only`. A cópia de trabalho não altera o
banco original, sua identidade ou a posse do agente de dados. As52 definições
foram percorridas em lotes e as11 estratégias em M15/H1/H4. Resultado é
diagnóstico de disponibilidade/candidatos, sem simulação financeira ou ranking.

O catálogo nativo de Estudo tem38 fábricas e outra semântica. ADX/ADX Wilder,
SAR, CCI, Ichimoku e outros não ganham um contrato histórico por parecerem
similares a uma fórmula Python. Mesmo ATR, RSI, Fractals, Alligator e MACD
requerem conferir seed, variante, confirmação/deslocamento e parâmetros antes de
alegar paridade. Os38 continuam acessíveis pelo SDK gráfico AIA-02a.

Frentes separadas a continuar:

| Frente | Próximo contrato/aceite |
|---|---|
| Dados + catálogo | Materialização/versionamento dos itens ausentes, inclusive ADX/Super ATR/indicador estratégico, pelo dono dos dados; disponibilidade/revisão/clock explícitos |
| Comparativo de execuções | Mesmo preset/revisão/composição, custos, janela/modelo e datasets; P&L/equity/drawdown, não mistura de runs mais recentes por TF |
| Inspetor T20.23 | Cursor/zoom vinculados entre painéis, tabela→evento, fluxo/stops/equity somente com evidência própria |
| Experimentos T20.12/13 | Pré-registro de hipóteses, tentativas, OOS, maturidade e controle de multiplicidade; sem transformar esta exploração em qualificação |
| Presets T19.27/28/32 | Geração/seleção automática apenas com contratos estatísticos, compatibilidade e execução existentes; sem escolher retrospectivamente o melhor par |
| Carteira/regimes | Comparação entre ativos, custos, sessões, estabilidade, regimes, sensibilidade e manutenção operacional com suas identidades próprias |
| Qualificação de ambiente | Windows físico, DPI/múltiplos monitores físicos e limites de interação em séries maiores têm evidência própria; sem promessa de FPS |

Esta entrega amplia o percurso T20.22 com dados reais. Não declara concluídos
T20.22 completo, T20.23, gerador automático, OMNI ou aceites financeiros.

Capturas de QA do componente real, em Xvfb próprio (sem conta/terminal):

![Janela compacta1280×800](../img/laboratorio_series/handheld.png)

![Visão ampla3456×1458, tema escuro](../img/laboratorio_series/wide-dark.png)

Detalhe ampliado e tema claro estão na mesma pasta. A validação e seus cortes
estão na [evidência](../evidencias/T20_22_LABORATORIO_SERIES_20261024.json).

### Contagem de eventos de pivô (28/10/2026)

Canais de fractais/ZigZag acrescentam `event_confirmation_ends`: em cada barra da
grade, o fechamento nominal da confirmação que sustenta o valor retido. Origem,
barra de confirmação e disponibilidade nominal do último pivô ficam em
`events.latest`. Recebimento histórico continua não comprovado.

Cada par acrescenta `event_sample` com `left_events`, `right_events` e
`unique_event_pairs`. As contagens usam só pares válidos, após lag e exclusões;
um lado sem pivô tem `None`, não zero. O holdout tem sua própria contagem em
`out_of_sample.event_sample`. Não são tamanho efetivo de amostra nem prova de
independência; os controles 70/30 continuam por barras. O hover das relações e
matrizes mostra essa diferença. Relatórios antigos não recebem contagens
inferidas. Fórmulas, parâmetros e limiares continuam os mesmos.
São eventos representados pelos valores dos pares; um evento anterior ao corte
que continua retido no holdout conta como representado, sem se tornar uma nova
confirmação. O resumo JSON de `laboratory derived --relations` preserva essas
contagens e os metadados do canal, sem despejar preços por barra.
## Fontes vinculadas, candle e relatório portátil

Em **Laboratório → Séries e confluências → Fontes, candle e relatório…**, escolha
versões armazenadas e descreva explicitamente a associação objeto → ativo.
Depois use **Congelar e analisar**. A seleção pode combinar preço, volume real,
ticks, indicadores e TFs; cada unidade e versão permanece identificada.

**Recebimento no candle** coloca cada mudança no primeiro fechamento que a
conhece, sem carregar contagens nos candles seguintes. **Publicação retrospectiva**
mostra o fim da janela, preservando a disponibilidade posterior; não representa
informação conhecida na ocasião. Duplo clique no gráfico inspeciona o candle,
os recebimentos, as janelas publicadas e a proveniência. A matriz pagina todos
os pares, inclusive TFs diferentes, em defasagens 0/1/3/6; o par selecionado
mostra estabilidade em quatro blocos. Zero, ausência e constância são distintos.

```python
from mt5_extracao import sdk
report = sdk.analyze_laboratory(
    db, manifest_ids=market_snapshot_ids,
    linked_selections=[dict(manifest_id=source_snapshot_id,
                            features=source_feature_versions,
                            relationship="Associação explicitamente escolhida")],
    feature_ids=["volume.ticks", "volume.real", "price.return_1"],
    timeline_basis="received", as_of=cutoff_utc)
candle = sdk.laboratory_candle(report, 2)
crossings = sdk.laboratory_crossings(report)
sdk.export_laboratory_report(report, "cruzamentos.html")
```

CLI: `laboratory analyze --manifest ID --links links.json --timeline-basis
received --as-of UTC --out analysis.json`; `links.json` contém a lista de
`manifest_id/features/relationship` do exemplo. Capture fontes explicitamente
antes: análise SDK/CLI exige snapshots existentes e não captura implicitamente.
Use `laboratory candle --analysis analysis.json --index 2`, `laboratory crossings
--analysis analysis.json` ou `laboratory report --analysis analysis.json --out
cruzamentos.html`. O HTML é local, autossuficiente, sem envio ou rede; inclui
gráfico, filtros, matrizes, estabilidade, retornos brutos futuros e cronologia.

Relatórios com vínculos usam versão 2, integridade do conteúdo e estatísticas
congeladas. Reabertura/exportação não recalcula fórmulas, estatísticas ou modelos.
Versão 1 continua legível. Limites: 32 canais, 8 TFs, 20 mil barras, 200 mil
células/revisões; 120 segundos e 1024 MiB por operação. Fontes semanais/mensais
e M7/M9 sintéticos mantêm seus percursos próprios. Colisões de várias observações
em um candle ficam indisponíveis numericamente, com recibos preservados.
Sessões exigem calendário congelado; fases macro exigem fonte categórica datada.
Categorias não viram números. Correlação não explica causalmente o preço nem
qualifica negociação, precisão do classificador ou funcionamento nativo.

### Avaliação estatística registrada

No diálogo de fontes/cruzamentos, “Registrar protocolo JSON…” congela família,
janelas e método no ledger de experimentos existente. “Avaliar protocolo
registrado…” é uma segunda ação, com chave idempotente, espera e cancelamento.
O recibo vai ao ledger antes de ser apresentado e pode acompanhar o HTML. Abrir
ou exportar um recibo pronto não repete inferência. Protocolos de relações não
são pedidos de backtest nem podem ser enviados ao executor de estratégias.

SDK: `register_laboratory_protocol(db, report, definition)` e
`evaluate_laboratory_protocol(db, report, protocol_id, key="avaliacao-1")`.
CLI: `laboratory protocol-register --analysis analysis.json --definition
protocol.json --out registered.json`, depois `laboratory protocol-evaluate
--analysis analysis.json --protocol ID --key tentativa-1 --out evaluation.json`.
`laboratory report --analysis analysis.json --evaluation evaluation.json --out
report.html` incorpora o recibo verificado da mesma análise.

Contrato JSON completo (substitua IDs/horários pelos da análise):

```json
{
  "name": "estudo.retorno", "revision": 1,
  "channels": ["D1:price.return_1"], "horizons": [1, 3, 6],
  "windows": {
    "train": {"start": "2024-01-02T00:00Z", "end": "2024-05-01T00:00Z"},
    "validation": {"start": "2024-05-01T00:00Z", "end": "2024-09-01T00:00Z"},
    "holdout": {"start": "2024-09-01T00:00Z", "end": "2025-01-01T00:00Z"}
  },
  "session_basis": "utc_day_exploratory", "method": "spearman",
  "bootstrap_replicates": 999, "seed": 7
}
```

Janelas [início,fim), cronológicas, sem sobreposição e dentro do recorte.
Até24 combinações canal×horizonte, três janelas obrigatórias;99…1999 réplicas,
30 pares/10 sessões mínimos por resultado. Calendário congelado completo permite
`frozen_calendar`; dias UTC exigem opção exploratória explícita. Pearson ou
Spearman usam `analytics.associations`, bootstrap em blocos de sessões e
Bonferroni sobre toda a família, inclusive testes indisponíveis. Fronteiras
expurgam alvos que terminem fora da própria janela; lacunas não são comprimidas.
Disponibilidade tardia exclui a característica, inclusive na publicação
retrospectiva. Estado categórico não é convertido em código contínuo.

Registro é anterior a esta avaliação, mas a análise de origem já foi explorada.
Acesso externo anterior permanece desconhecido, reserva inédita/predição/lucro
não recebem qualificação automática. Correção cobre a família, sem controlar
por inferência toda a busca entre novas revisões. Cancelamento/falha conserva
acessos no ledger; retomar tentativa admitida sem resultado exige nova chave.

### Comparação financeira de execuções gravadas

Laboratório → Comparativo → Execuções gravadas → “Comparar execuções gravadas…”.
Escolha2 a8 runs, adicione-os à seleção e declare “Estratégias/presets no mesmo
ativo” ou “Mesmo preset entre ativos”. Abertura lê somente uma página de50
metadados. Comparar consulta os runs escolhidos, sem executar estratégias;
espera/cancelamento usam a fila da UI. Mudança de seleção/modo invalida a vista.

São exibidos operações, lucro líquido, saldo final, patrimônio final, drawdown
de encerradas e drawdown a mercado por fechamento. Métricas do motor são
preservadas; o pior drawdown não é recalculado de uma curva reduzida. HTML local
permite alternar saldo, encerradas, patrimônio e cotas em fração do capital
inicial, com os custos/configurações, revisões, fontes e períodos inspecionáveis.
Curvas usam a redução gráfica existente até240 pontos; não representam ticks.

Compatibilidade exige composição/hash válidos, recibo completo do motor,
configuração de capital/risco/custos, origem, relógio, período, moeda e qualidade
congelada compatíveis. Mesmo ativo exige snapshot idêntico; entre ativos exige
o mesmo corpo de preset removendo somente o símbolo. Especificações distintas
entre ativos permanecem declaradas, sem supor preço/tick iguais. Fluxos externos
não são qualificados nesta versão. Moeda ausente, custos/contexto incompletos,
revisão incompatível ou diferença de janela deixam os resultados separados.
Não há conversão cambial, soma entre moedas ou ranking automático.

Novos runs Python gravam `comparison_context` nas métricas existentes. Para
registrar a moeda da conta, declare `[STRATEGY] account_currency = USD` (exemplo;
use a moeda real da conta). Vazio conserva desconhecida, sem inferi-la de
currency_profit. Esta é uma declaração do usuário; não prova conta/FX histórica.
Runs antigos continuam legíveis e não são regravados para simular um recibo.
Compatibilidade técnica não concede aprovação financeira ou preditiva.

SDK: `compare_laboratory_runs(db, run_ids, mode="same_asset"|"same_preset")` e
`export_laboratory_runs(comparison, "novo.html")`. CLI: `laboratory runs --limit
50 --offset 0`, depois `laboratory compare-runs --run ID1 --run ID2 --mode
same_preset --out novo.json --html novo.html`. Saídas novas e integridade do
recibo verificadas; exportação não consulta preços atuais nem refaz o motor.
