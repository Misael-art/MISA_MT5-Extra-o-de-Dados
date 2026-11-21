# Retomar MT5X em um host limpo

## 21/11/2026 — F19-PESQ-16, projeção harmônica à vista

O modelo orça os limites inferiores dos intervalos geométricos com lotes
negociáveis e sobra em caixa. Compra única, aportes e grade usam o mesmo caixa
inicial, taxa por compra e preço de marcação. Cada braço informa estoque,
caixa, giro nominal e patrimônio. A hipótese de todas as compras a preços
declarados ainda pede replay com fills, liquidez, ações corporativas e fluxos
datados.
Passaram 36 regressões focadas no Linux. Pelo launcher, a fixture manteve
700 unidades, R$ 2.985,01 em caixa e R$ 9.285,01 de patrimônio com marcação
abaixo do piso. Recibos em f19-pesq-16-harmonic-r2.
[Ficha](docs/propostas/PESQUISA_SINAIS_E_OPERACIONAIS.md#f19-pesq-16)

## 21/11/2026 — F19-PESQ-11, projeção de risco de grade

A projeção anterior à simulação compara entrada direta, grade fixa, grade ATR
e grade direcional com o mesmo teto declarado. Usa preços em ticks, vetor de
volumes e a especificação conhecida no corte. Custos e estresse são explícitos.
Passaram 33 regressões focadas no Linux. Pelo launcher, seis WIN espaçados em
50 pontos acumularam perda bruta de 150 no sexto nível e 210 após outros
50 pontos contra. Recibos em f19-pesq-11-grid-risk.
Margem, sequência de preenchimentos e conciliação nativa seguem pendentes.
[PR #384](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/384),
SHA remoto `c73c1c4fb0bc087fadaec4a65f3322eab68a43e9`.
[Ficha](docs/propostas/PESQUISA_SINAIS_E_OPERACIONAIS.md#f19-pesq-11)

## 21/11/2026 — F19-PESQ-10, referência causal Fair Value Gap

`analytics fvg-reference` registra a zona só após fechar a terceira barra.
As visitas à borda, ao centro da zona e ao centro elegível do impulso são
observadas nas cinco barras seguintes. Lacuna ou troca de sessão interrompe a
janela. Cada zona sobreposta mantém seu próprio horizonte. Toques com violação
na mesma barra têm essa condição explícita. Controles pareados e estudo com
preços executáveis permanecem pendentes.
Passaram 20 regressões focadas no Linux. Pelo launcher a fixture tornou
a zona disponível às 09h03 UTC, tocou a borda em 106 na barra seguinte e
expirou às 09h08 UTC. Recibos em f19-pesq-10-reference e f19-pesq-10-reference-r2.
[PR #383](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/383),
SHA remoto `3cc94598992b5de2a4738cd489e435cda6f9d294`.
[Ficha](docs/propostas/PESQUISA_SINAIS_E_OPERACIONAIS.md#f19-pesq-10)

## 21/11/2026 — F19-PESQ-09, referência CHoCH H4

A referência distingue mudança de direção do fechamento e ruptura de swing.
A decisão estrutural exige pivôs já confirmados. A abertura H4 seguinte
entra como referência quando observada, com stop um tick além do swing
oposto e alvo de 1R. Lacunas encerram a continuidade. Sweep, filtros e
comparação econômica com controles continuam pendentes.
Passaram 14 regressões focadas no Linux. Pelo launcher a fixture em H4
publicou a quebra de 104 às 20h UTC e abertura modelada de 100, stop de
120 e alvo de 80. Recibos em f19-pesq-09-reference-r2.
[PR #382](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/382),
SHA remoto `369404b1546ec46abbbbf95cb1babd250bbbb5fb`.
[Ficha](docs/propostas/PESQUISA_SINAIS_E_OPERACIONAIS.md#f19-pesq-09)

## 21/11/2026 — F19-PESQ-08, referência de reteste causal

`analytics retest-reference` aplica os fractais estritos da T19.6 e só admite
uma quebra após a confirmação conhecida do pivô. O nível fica congelado na
quebra, com tolerância, visitas prévias e horizonte explícitos. Revisitas
numeradas registram o horário de disponibilidade, sem inferir o instante de toque.

Passaram 13 regressões focadas em Linux. A fixture distingue uma visita anterior
à quebra e quatro revisitas posteriores. Deslocar um pivô futuro preservou os
eventos no corte anterior. Recibos em `f19-pesq-08-reference-r2`.
Pelo launcher, a mesma bancada em calendário observado publicou a quebra de
110 às 09h08 UTC e as revisitas de ordem 1 a 4 às 09h09, 09h11, 09h13 e 09h15 UTC.
O pareamento e a comparação econômica das entradas seguem abertos.
[PR #381](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/381),
SHA remoto `396d62deee6b9af2f39be4193474a23625fe3ec1`.
[Ficha](docs/propostas/PESQUISA_SINAIS_E_OPERACIONAIS.md#f19-pesq-08)

## 21/11/2026 — F19-PESQ-07, referência do Open Strike Python

`analytics open-strike-reference` compara EMA10 e SMA10 no primeiro M15 fechado
do WIN e devolve preços modelados de mercado e pendente. A pendente fica a um
tick do extremo e expira até o próximo M15 ou o limite de sessão. Minuto ausente,
igualdade com a banda e vela neutra têm tratamento explícito. As referências por
barras mantêm execução, custos e encerramento como aceites separados.

Passaram 21 regressões focadas em Linux, incluindo SDK e cobertura de calendário.
A bancada sintética registra sinal às 09h15, gatilho de 101.010 e preço modelado
de 101.020 na abertura acima do gatilho. Recibos em `f19-pesq-07-reference-r2`.
[PR #380](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/380),
SHA remoto `3755255dab5bca93c9532f48a73944da5dfc1188`.
[Ficha](docs/propostas/PESQUISA_SINAIS_E_OPERACIONAIS.md#f19-pesq-07)

## 21/11/2026 — F19-PESQ-06, composição de saídas no ORB Python

A composição do ORB Python aceita parcial em R, stop/alvo fixos e break-even
após a redução confirmada. Em caixa de 500 pontos de índice, os dois WIN
renderam R$ 240 brutos com saídas em +500/+700 e R$ 280 com as duas em +700.
Um contrato indivisível seguiu até o alvo. A fixture confirmou -R$ 200 quando
stop e alvos coincidem na barra e R$ 100 após parcial e retorno ao break-even.
Passaram 100 regressões focadas de composição, ciclos e backtest no Linux.
O launcher executou as duas composições em 103 barras sintéticas e registrou
volumes 2/2 no caso integral e 2/1/1 no parcial. Os recibos ficam em
`f19-pesq-06-launcher`. Doctor offline com 16 OK, dois avisos e zero falhas.
SHA remoto confirmado `357edbe2e23b0c4cb216f426e4600bdb10e3de53`.
[PR #379](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/379).
O complemento Python implementa trailing por fechamento com gatilho/distância/
passo de 600/500/100 pontos de índice. As 89 regressões focadas cobriram compra
e venda. Pelo launcher, o stop avançou para 100.355 e 100.455 e a saída após gap
aconteceu em 100.405. Caixa líquido modelado de 60. Recibos em
`f19-pesq-06-stepped-launcher`. A comparação econômica em histórico qualificado
continua pendente. SHA remoto do trailing `9e4bee11e0ca8d0024aa5e775c22b53e5240fbb2`.
[Ficha](docs/propostas/PESQUISA_SINAIS_E_OPERACIONAIS.md#f19-pesq-06)

## 21/11/2026 — F19-PESQ-05, filtros estáticos do ORB

`analytics orb-reference --range-minutes 15 --width 200 750 --distance 10 400`
informa sinal original, largura, distância e veto em pontos do índice. A fixture
verificou as bordas inclusivas e que o primeiro rompimento bloqueado consome o
pregão, mesmo quando um rompimento posterior passaria.

No launcher Linux, o WINZ26 de 27 e 28/10/2026 teve dois vetos. No dia 27 a
largura foi de 1.795 pontos e a distância de 5. No dia 28 a largura foi de
1.635 e a distância de 55, então o veto foi apenas por largura. Recibo em
`f19-pesq-05-b3`. O recorte comprova as decisões por barra, com aceite econômico
e variantes de horário e normalização ainda abertos. SHA remoto confirmado
`cf96979ca012942221ae8137ae134f1debc0f364`.
[PR #378](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/378).

## 21/11/2026 — F19-PESQ-03, pré-registro Python dos braços

`experiments create` e `experiments request` aceitam o plano F19 de sinal, filtro
ou saída. A hipótese, os dois braços, o capital e os dois cenários de custo ficam
congelados com as três janelas e o embargo. A reserva exige os quatro pedidos
antes do acesso e recusa exposição registrada fora do plano.

Passaram 20 regressões focadas, incluindo o contrato público do SDK. O launcher
Linux reabriu o protocolo sintético `a399eb11a198ffb0f252a5c39d6ed4de079e4ce7e795de5702919b27afed56d5`
com os três papéis e nenhum acesso à reserva. Os recibos ficam em `f19-pesq-03-review`.
Doctor offline com 16 OK, dois avisos e zero falhas. A etapa seguinte no App é o
pareamento por contexto. A comparação por pregão WIN M1 já consulta quatro livros
do mesmo snapshot, separa o cenário adverso e mantém o pregão sem operações.
Na bancada sintética, os quatro backtests Python registrados deram diferença
média de 150 por pregão no cenário base e zero no adverso, com ambos os
intervalos inconclusivos. A fixture substituiu somente a leitura do hash do
código. Passaram 25 testes na verificação conjunta. Recibo do launcher em
`f19-pesq-03-paired-r2`. SHA remoto `10585beaf58f71366af4d72e04fe98b27de7fd98`.
[PR #377](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/377) · [Contrato](docs/propostas/PESQUISA_SINAIS_E_OPERACIONAIS.md#f19-pesq-03)

O CI anterior das PRs #374–376 encontrou duas regressões na lista pública do SDK.
A referência e as assinaturas de `opening_coverage` e `orb_reference` foram
incluídas nos respectivos PRs. As três pontas atualizadas foram confirmadas no
remoto. A suíte dirigida do SDK passou com 12 testes.

## 21/11/2026 — F19-PESQ-04, referência do ORB no App

`analytics orb-reference` compara os sinais Python legados com os da caixa
nominal congelada. A fixture atrasada inicia às
09h07. O legado sinaliza no fechamento das 09h37 e a caixa nominal exclui esse
dia por sete barras ausentes. Os casos completo e sem rompimento coincidem.

Passaram 25 testes focados. O launcher comparou 90 barras públicas M1 do WINZ26
em 27–28/10/2026 com SQLite de QA próprio e o mesmo parâmetro de 30 minutos.
Os dois percursos tiveram 30/30 barras por abertura e os mesmos sinais às
09h40, com compra no dia 27 e venda no dia 28. O primeiro alvo teórico foi
181.887,5 pontos, entre os passos de 5 pontos do contrato. O recibo de consulta
fica em `f19-pesq-04-b3`. Doctor offline com 16 OK, dois avisos e zero falhas.

O fonte fixado do HADOUKEN preserva os padrões de 30 minutos e 1,5×. A
comparação no Tester requer uma fonte WIN com a grade e os eventos de execução
qualificados. O histórico público desta bancada cobre apenas as barras do
App. A comparação Python da F19-PESQ-03 já pode usar o livro do modelo.
[Ficha de referência](docs/propostas/PESQUISA_SINAIS_E_OPERACIONAIS.md#f19-pesq-04)

## 21/11/2026 — F19-PESQ-02, livro do modelo Python

O backtest Python acrescenta as oportunidades à trajetória usada pelo ledger.
A consulta `trades opportunities` confere o vínculo de cada sinal executado com
entrada, reduções e zeragem. Fills mantêm comissões e swap observados no modelo.
O calendário de abertura inclui os pregões elegíveis sem sinais e os excluídos.

O launcher Linux confirmou, em SQLite sintético isolado, três oportunidades,
um veto, uma expiração e um ciclo encerrado, com três fills e caixa líquido de
40 no modelo. Confirmou também dois pregões elegíveis sem sinais em outra execução.
Passaram 35 testes focados no Linux. Recibos no estado local de QA
`f19-pesq-02-pytest`. SHA remoto confirmado `59ec8de8fe72864f491896aa9669e83878924f58`.
[PR #375](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/375).
A ficha tem aceite parcial:
faltam rejeição e parcial de entrada em produtor nativo, margem, exposição por
perna, tempo em drawdown e concentração. [Consulta e limites](docs/propostas/PESQUISA_SINAIS_E_OPERACIONAIS.md#f19-pesq-02)

## 21/11/2026 — F19-PESQ-01, cobertura da abertura do WIN

`analytics opening-coverage` e `sdk.opening_coverage` qualificam cada data da
grade declarada com os snapshots M1 e os fatos de vigência existentes. Janela
incompleta ou ainda aberta fica fora da população, com motivo no manifesto.
A consulta preserva os dois relógios, o vencimento conhecido no instante e as
limitações de ticks, microestrutura e histórico anterior.

O launcher Linux conferiu 2/2 aberturas em 27–28/10/2026 com 32 barras públicas
WINZ26 e SQLite isolado. Passaram 59 testes focados na `.venv` oficial. O doctor
offline registrou 16 OK, dois avisos e zero
falhas. O recibo de cobertura `48c4522533455fe321c60e2e24fcb71988874b3cbbcc38f7b2d6c87f12aaba3a`
fica no estado local de QA `f19-pesq-01-b3`. SHA remoto confirmado
`dce08e7850faf44281a5ac3df858c2ee00035e20`, sobre a PR #373, com os pins
preservados. [PR #374](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/374) · [Consulta e limites](docs/dados/B3_PUBLICO.md#cobertura-da-abertura-do-win-por-pregão)

O próximo incremento da frente é F19-PESQ-02. O livro de oportunidades deve
usar os pregões elegíveis como população, conservar exclusões e vincular as
decisões aos ciclos e fills efetivamente registrados.

## 20/11/2026 — Roteiro de pesquisa de sinais, operacionais e presets

**Para começar pela F19-PESQ-01, use o [prompt de implementação das 17 fichas](docs/propostas/PROMPT_F19_PESQ_EXECUCAO.md).**

A [fila de 17 fichas](docs/PLANO_DE_TRABALHO.md#fila-f19-pesquisa) está distribuída no [roadmap](docs/ROADMAP.md#roteiro-f19-pesquisa). A primeira entrega, [F19-PESQ-01](docs/propostas/PESQUISA_SINAIS_E_OPERACIONAIS.md#f19-pesq-01), qualifica a abertura e a cobertura do WIN por contrato. Em seguida, registrar todas as oportunidades e o protocolo de comparação e reconciliar a referência ORB Python/HADOUKEN. O [roteiro completo](docs/propostas/PESQUISA_SINAIS_E_OPERACIONAIS.md) contém as regras dos nove temas, dependências, fixtures, critérios de rejeição e promoção. As fichas estão planejadas. Os aceites de execução e de rentabilidade serão registrados por variante.

## Triplo desarmado no Tester — 16/11/2026 (PR 352)

Quatro sessões pelo launcher, na candidata `43240c4`/`97688c8` sobre a PR 351.
Primeiro bloqueio: a sessão compila na tela virtual oculta (Xvfb `:99`, sem
DRI3). O mesmo fonte levou 225 s ali e 37 s na tela real, e o prazo fixo de
300 s cortava a compilação em 95%. `[ROBOTS] compile_timeout_seconds` (60–1800,
vazio = 300) resolve: com 600 s o `.ex5` saiu nas quatro sessões. Segundo
bloqueio: os pacotes da revisão 1 tinham seed `2026-10-12T02:20`, posterior ao
início do Tester, e a sessão `d27fd590` reprovou no `OnInit` (`TRIPLE_DENY`).
A revisão 2 usa `2026-10-09T00:00` (`preparar_seed_triplo.py`), e o App passa a
recusar essa combinação ao gerar o pacote. Bloqueio atual, de ambiente: o
download preliminar de ticks reais do MetaQuotes-Demo (Access Point BR 1,
215 ms, ~300–360 KB/s) foi cancelado pelo MT5 em 3 de 4 sessões, entre 40 e
120 s. Semear o cache de ticks do terminal principal não evitou o novo download
e foi revertido. Retomar com rede estável, pelo mesmo comando da bancada com
`--compile-timeout 600`; o aceite (consenso, caixa conciliado, zero entradas)
continua pendente.

## Presets estáticos e fontes do HADOUKEN atual

HA/HiLo, Madrid, Pullback nativo, StochEMA9/S10 e VWAP Bands aceitam a
identidade exata do HADOUKEN `d3dd3a5e9a68`. Os 10, 13, 23, 15 e 11 inputs
mantêm o contrato anterior. Default e Manual vinculam a identidade de fonte
e a revisão no manifesto e recusam pacote cruzado. O exportador reabre os
pacotes com o mesmo `.set`, sem alterar revisões anteriores. A candidata
`981bc49ba28c` da PR 351 gerou, selecionou, exportou e reabriu 14 pacotes
dos sete Experts reais em SQLite de QA. Passaram 43 regressões focadas e
dois cenários de interface. A aceitação técnica do pacote habilita o percurso do
Tester DEMO. Os resultados nativos e os seletores Defensivo e FatorLucro têm
aceites próprios.

## Regressões Windows na candidata da PR 350

O teste da Análise offline carrega as 600 barras com a janela oculta e
confere os três gráficos quando a guia fica visível. Continua verificando
que a sessão não chama o SDK do MetaTrader. Os testes de encerramento do
Tester leem o recibo em UTF-8 e conferem os acentos e a liberação da sessão.
A revisão `ec2237aa11cd` passou nas duas regressões no Linux e na etapa
focada do CI Windows. As duas suítes completas, o lint, PostgreSQL e os dois
instaladores terminaram com sucesso no mesmo SHA. O launcher oficial abriu
a Análise offline com 600 barras e zero chamadas ao SDK. Os 370 hashes
conferem com o runtime da candidata original da PR 350.

## Qualidade das barras com conversão compartilhada

A avaliação reaproveita a normalização das colunas de preços e quantidades.
A seleção de vigência evita agrupar conjuntos sem candidatos. Os diagnósticos
e os contextos produziram resultados idênticos nas massas comparadas.
Passaram 96 testes de qualidade, capacidades e histórico de instrumentos.
A reabertura do lote de seis ativos verificou 12 execuções gravadas e 126
células. O teste de duração do lote ainda atingiu o limite de 60 s.

## Gravação de barras reutiliza o comando entre lotes

A gravação de OHLCV usa uma instrução parametrizada nos lotes de 200 barras.
A escolha da revisão, os recibos e as alterações do bruto continuam na
mesma transação. A regressão confere os lotes e a reversão de falha parcial.
Na bancada, a gravação das mesmas 753 barras passou de 9,58 s para 2,98 s.
Passaram 27 testes de gravação, revisões, consultas e retomada em SQLite.
O caso de comparação com três ativos passou no prazo original. O de seis
chegou ao limite de 60 s na montagem do plano do executor.

## Captura retorna o snapshot validado na gravação

A captura valida o conteúdo, grava e confere os blocos e o manifesto na
transação. O retorno reaproveita o snapshot já reconstruído. A reabertura
continua validando os dados no banco, inclusive quando o bloco foi alterado.
A bancada confirmou 36 testes, incluindo séries, importação e reabertura.

## Grade e vínculo na comparação de execuções gravadas

A comparação confere o snapshot vinculado a cada execução e a correspondência
da janela gravada. Entre ativos, a mesma revisão de preset exige também os
mesmos horários de barras observadas. Se houver diferença, as métricas de
cada execução ficam disponíveis e o HTML apresenta uma curva por vez.
Passaram 24 testes focados. O cenário ampliado com três ativos atingiu o
limite de 60 s na preparação dos backtests também na candidata anterior.

## Expectativa por dia do mês no Laboratório

A grade mantém as posições de 1 a 31 entre os meses do recorte. Selecionar
um dia no gráfico ou no controle Dia mostra sua expectativa por operação e
a amostra. Zero observado tem marca neutra e dias sem observações têm outra
marca. A faixa de contexto informa as exclusões por dia ou PnL inválido.
A área de rolagem reserva a altura do gráfico ao abrir a guia. Passaram 29
testes focados no Linux, incluindo navegação e redimensionamento em Tk/Xvfb.

## Quarentena após cancelamento por orçamento do Tester

As suítes Linux das PRs 340 e 342 encontraram o teste que ainda esperava
falha ao interromper uma cópia por falta de reserva. O teste agora confere
o cancelamento, o motivo, a liberação dos recursos e a ausência de execução
publicada. A restauração da quarentena precisa devolver o recibo original.
Sete testes focados passaram no Linux.

## Cancelamento e troca de base em Séries e confluências

Cada análise conserva sua própria solicitação de cancelamento e a base
selecionada no início. Abrir um estudo congelado cancela o cálculo anterior.
Se a base mudar durante a captura, os vínculos e o resultado não são aplicados
à nova seleção. A leitura de fontes também descarta respostas de outra base.
Atualizar as fontes da base ativa permite congelar a nova seleção.
Pares sem observações válidas preservam os estados e as contagens sem recalcular
agregações vazias. O caso M7/M9 passou na interface Tk no Linux. A comparação
isolada de duas barras produziu os mesmos 325 resultados em 5,2 s ante 10,1 s.

## Prazo de compilação do Triplo no Wine

O MetaEditor da sessão `f412ccbe82fb4f789699072c34b91ea4` registrou 100% da
geração de código no limite de 300 segundos. O perfil encerrou e liberou os
processos antes de aparecerem o binário e a linha final do compilador. O
diagnóstico da sessão agora mostra o prazo, o último progresso e o log parcial.
Treze testes focados passaram, incluindo a propagação do prazo da sessão e o
caso de 100% sem `.ex5`.

## Corte UTC do Filtro C no CI Windows

A suíte Windows da PR 338 encontrou uma comparação dependente de duas
leituras seguidas do relógio. O teste recupera o instante persistido e
verifica o corte imediatamente anterior e o corte no próprio instante.
O registro entra no Dashboard a partir do instante gravado.

## Relação descritiva entre tempos gráficos

No modo da mesma estratégia, o Laboratório mostra Pearson entre retornos de
patrimônio em cotas nos fechamentos coincidentes. A duração é comum ao par.
A tabela informa os pares válidos e explicita lacunas, séries constantes e
recibos incompatíveis. A avaliação preditiva fora da amostra continua pendente.
Passaram as verificações de serviço, CLI, HTML e Tk em Xvfb com dados próprios.

## Interrupção do Tester pelo prazo do lote

O prazo do lote interrompe a sessão e registra o motivo no recibo. A mesma
limpeza de cancelamento encerra os processos próprios e libera a posse para
uma nova sessão. Um relato de interrupção durante a compilação não produz
evidência de execução ou de paridade.

## Posse do Tester com pasta personalizada no Wine

O registro de posse confere se a pasta da sessão é acessível pela unidade C:
antes de gravar `native-active.json`. Uma pasta fora do prefixo ativo falha
antes de reservar essa posse. A regressão verifica que uma nova sessão com
pasta válida pode prosseguir depois da recusa.

## Preset, bateria e identidade MQL5 no Dashboard

A bateria aparece junto ao resultado quando está vinculada à execução e ao
escopo registrados. Baterias de referência ficam em evidências ocultas. A
identidade exibida vem da execução escolhida ou de sua própria bateria.
O preset nativo é lido por `run_id` e só aparece como associado após conferir
ativo, timeframe, Expert, fingerprint e instante da gravação no corte. Quando
o vínculo nativo constar do recibo, a faixa expõe os hashes do fonte, `.ex5`,
wrapper e inputs. Journals e arquivos locais continuam na bancada da sessão.
A prova deste fluxo usa base isolada e recibos sintéticos.

## Recibo do Filtro C no Dashboard

Os novos recibos guardam o instante da gravação em UTC, o período e a amostra
de treino e teste, as tentativas da grade em cada janela e os parâmetros
efetivos. A configuração inclui custos simulados, especificação e a vigência
que a fonte declara. O Dashboard consulta só o ativo e o timeframe escolhidos,
mostra as janelas em uma linha do tempo com seletor e mantém ausências de
recibos antigos. O Filtro C guarda a validação mais recente de cada estratégia,
ativo e timeframe. A evidência continua sendo de simulação.

## Layout da Análise ao alternar com Robôs

A Análise conserva os dados já lidos e desenha os gráficos quando a aba está
visível. O layout também aguarda o mapeamento e usa o ciclo principal de
eventos. A regressão Linux em Xvfb cobriu a abertura em escala 2.0 e a aba
Robôs em 1.33 e 2.0, mantendo o timeout de 60 segundos por teste. No Windows,
o timeout do SHA `ac0ab478` na PR 329 motivou o corretivo. A etapa visual
isolada do SHA `b93586289333` passou no CI. A suíte completa seguia em
execução e o aceite no Windows físico continua pendente.

## Laboratório — mesma estratégia entre tempos gráficos

O modo `same_strategy` compara de duas a oito execuções gravadas de uma
estratégia em um ativo, com uma execução por timeframe. A conferência inclui
parâmetros efetivos, motor, contrato, custos, moeda, snapshots vinculados e a
janela do início ao fechamento nominal da última barra. Divergências mantêm
as métricas no período original de cada execução. SDK, CLI, janela do
Laboratório e relatório HTML usam o mesmo serviço.
Dois testes de serviço/CLI e um teste Tk em Xvfb passaram no Linux. O HTML
foi aberto no navegador com dados próprios. Alternaram as curvas de saldo e
patrimônio e a seleção M2 pelo teclado, com as duas curvas restauradas e sem
erros de execução no navegador.

## Reconciliação do Tester e Replay por instante

O comparador ordena diferenças pelo relógio broker dos eventos disponíveis,
incluindo sinais, dimensionamento, negócios e livro. No mesmo segundo, o
evento nativo precede o valor resumido do livro. Diferença posterior de
contagem não encobre um campo divergente anterior. Moeda e caixa do relatório
são conferidos após os eventos e a referência da estratégia. Trinta e três
testes focados passaram no Linux, incluindo mudança de caixa com livro igual.
As sessões Wine e a paridade financeira com Python continuam com aceites
próprios.

## Pesquisa — atraso de entrada e espera pela abertura

O plano econômico de campanha aceita `entry_delay_ms` e `max_entry_wait_ms`
em `economic_plan.policy`. O primeiro conta a partir da decisão causal. O
segundo limita a espera a partir do instante em que a ordem fica pronta.
A simulação usa a primeira abertura observada a partir desse instante e
registra expiração quando o intervalo supera o limite. Os planos anteriores
conservam a regra de agendamento pela barra fechada. O recibo conserva
o instante da decisão, a abertura encontrada e o estado da execução.
Quinze verificações no Linux cobriram atraso, gap, validade dos inputs,
reconciliação de preço e reabertura do recibo persistido. Custos observados e
paridade com o Tester seguem com aceites próprios.

Os incrementos abaixo estão nas PRs [330](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/330),
[331](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/331) e
[332](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/332).
O estado dos checks por SHA e o orçamento nativo estão na bancada privada
`state/qa-tmp/corretivos-20261113/qualificacao-wine/estado-qualificacao.json`.
A prévia ativa é `ec2237aa11cd` da PR 350, com retorno `6e48e9010e38` e
370 hashes em ambas. O doctor offline registrou 16 OK, dois avisos e zero
falhas. Na candidata original `2d8122e57762`, o launcher congelou e reabriu
502 barras M1 e 251 M2 em SQLite sintético, com os mesmos manifestos e a
mesma resposta da PR 349. As candidatas anteriores registram a prova das grades divergentes,
de Dia do mês e de M7/M9. As PRs 333–350 estão encadeadas por SHA. O teste com
três ativos passou. O de seis atingiu 60 s na execução e, na reabertura de uma
cópia da sessão, confirmou 12 execuções gravadas e uma comparação compatível
de 126 células. O aceite de duração segue pendente.
A tentativa anterior chegou ao limite de 300 s. A compilação isolada
posterior `836a967c579c425b926ab6eb6594e052`, na candidata `bff77391bee8`,
gerou o `.ex5` em 196,36 s de sessão, com zero erros e avisos e liberação
dos processos. A sessão `d31440fc74dd4e5d8c135e1048532162` retomou o Triplo
1.02 desarmado no fluxo oficial. A compilação atingiu o limite de 300 s em
95% da geração de código, antes de iniciar o Tester, e os processos foram
liberados. O recibo `compiler-input-parity-d31440fc74dd.json` confirma o
mesmo MetaEditor, 292 entradas idênticas e fonte instrumentado com diferença
apenas nos identificadores de sessão. A bancada `qualificar_launcher.py`
verificou os quatro pacotes na candidata `2d8122e57762` e reconferiu o Triplo
desarmado na prévia ativa `ec2237aa11cd`. O `./run.sh` aciona o workflow
com SQLite de QA e um único job. O prazo de compilação é 300 s. A pressão
sustentada de CPU adiou a nova sessão. Retomar pelo Triplo desarmado e exigir
consenso confirmado, caixa conciliado e zero entradas. Seguir com os armados,
o par visual, S09 e a comparação com Replay e Python. Para `iCCI` e AggrVolBalance,
`MT5X_IndicadoresQA.mq5` prepara preços sintéticos e a leitura de `CopyBuffer`;
o comparador foi aferido em cinco estágios, e a coleta no terminal segue pendente.
O recibo `preset-profile-audit.json` identifica as fontes do mapper.

## Cobertura de ticks no relatório do Tester — 14/11/2026

O relatório só comprova a fração de ticks reais com percentual explícito,
válido e inequívoco. Antes, `-100%` e `100% (real ticks: 75%)` podiam ser
lidos como cobertura integral. O leitor conserva o estado inconclusivo
quando o mesmo relatório também contém uma indicação válida de 100%.
Dez testes do leitor passaram no Linux. A verificação de ticks do broker
continua vinculada às sessões nativas preparadas.

## Sessão observada na abertura da grade H1 — 14/11/2026

O fluxo T13.4 reconhece a barra que começa antes do primeiro M1 da sessão
quando a revisão derivada congelada confirma o mesmo OHLC, o fechamento da
grade, a sessão única e a cobertura integral do intervalo observado. O preço
e a data da barra conservam a grade original. O minuto de sessão conta a
partir da primeira observação. A abertura continua classificada como parcial.
A regressão reproduziu a recusa anterior e passou com a mesma base sintética,
incluindo ausência de derivação, cobertura incompleta e reabertura do snapshot.
A prova M1 com custos vigentes, plano temporal e suporte congelado continua
pendente.

## Tester e Replay no Wine — 14/11/2026

- A reconferência da prévia `6e48e9010e38` e do retorno `3d2447243704`
  validou 370 hashes em cada instalação, os dois atalhos e os pins
  HADOUKEN `d3dd3a5` e Safety `27b8599`. A ponta `ac0ab478` da PR329
  só altera este handoff. O doctor de desenvolvimento da prévia registrou
  16 OK, dois avisos e zero falhas.
- A conferência de fills do Tester e Replay passa a comparar os vínculos
  entre ordens e posições mesmo quando os tickets mudam entre sessões.
  Alterar o vínculo de uma ordem continua produzindo uma divergência.
  Triplo 1.02 usa a própria conferência do journal e dos fills uma única vez.
  Foram aprovados 75 testes focados no Linux com entradas sintéticas.
- Próximo ensaio nativo: usar os pacotes Default/Manual e a Session versionada
  para Triplo 1.02 e S09 2.18, medir a cópia do perfil, rodar Tester e Replay
  serializados e conferir diário, fills, caixa, plot, recursos e estados do
  painel. A sessão visual copia o runtime Wine e usa histórico próprio. Sua
  execução depende de folga no host. Preservar separadamente as provas de
  `iCCI` e `CopyBuffer`, os resultados anteriores e o estado dos rascunhos.

## Validação Linux dos corretivos e dos pacotes — 14/11/2026

- As PRs 319 e 320 passaram nos seis checks de produto. A compilação no
  MetaEditor/Wine detectou funções de cobertura fora da classe do painel.
  O HADOUKEN `d3dd3a5` (PR92) corrigiu a classe. Painel 7.12, S09 2.18 e
  Triplo 1.02 compilaram com zero erros e avisos em perfil isolado. A PR322
  (`4fa0bd4`) acompanha o pin e as novas identidades de fonte. O indicador
  AggrVolBalance do mesmo pin também compilou com zero erros e avisos.
- O atalho e o menu usam a prévia `6e48e9010e38` (PR329), com 370 arquivos
  verificados, HADOUKEN `d3dd3a5`, Safety `27b8599` e retorno `3d2447243704`.
  GUI Linux em Xvfb, CCI versionado, doctor offline e percurso de pacotes
  pelo launcher passaram. Os recibos estão em `state/qa-tmp/corretivos-20261113/`.
- A PR321 (`a06836d`) publica `package-plan`, verificado em SQLite legado e
  PostgreSQL de QA. A PR323 (`1897e83`) publica exportação por ativo das famílias
  de mercado SQLite com seletores definidos. A PR324 (`f2d1c9c`) publica reunião
  em coleção local com origens separadas, integridade, conflito e repetição
  idempotente. A PR325 inclui fatos analíticos, snapshots, notas e presets
  vinculados. A PR326 preserva execuções completas e ledger. A PR327 preserva
  a linhagem de relações persistidas. A PR329 preserva campanhas com roster
  completo e trilha de exposição. Os dois recortes passaram pelo launcher.
  Transporte de pacotes PostgreSQL e famílias sem contrato de seleção ainda
  exigem implementação. Os checks de cada PR continuam vinculados ao SHA.
- Os warnings NumPy da SA-REF-01 vieram da janela de descoberta vazia na
  fixture. A PR328 completa a fixture e preserva a referência congelada.
  Os 27 testes CCI/SA-REF passaram com RuntimeWarning tratado como erro.
- 195 testes do HADOUKEN, checker de 174 arquivos e 54 verificações de
  Dashboard, configurador e campanhas passaram. Os testes do Dashboard e
  do configurador exercitam dados controlados e os critérios cobertos.
- Ainda precisam de evidência o Strategy Tester DEMO com vínculo e plot
  ligado/desligado, o aceite com dados elegíveis do M1, os percursos de
  configurador/dashboard e a custódia via Drive do proprietário. O aceite
  físico do Windows continua aguardando host qualificado.

## Parecer consolidado e repasse — 14/11/2026

Fotografia atual conferida no parecer local
`../../state/qa-tmp/corretivos-20261113/PARECER_CONTINUIDADE_20261114.md`.
Os marcos abaixo são históricos; este recorte atualiza sua leitura sem apagar evidências.

- App: checkout550b6d5/codex/launcher-classes preservado, índice vazio; ponta
  publicada18fd5a76/PR320, dependente de3bca8c87/PR319 e da cadeia313–318.
- HADOUKEN566c8e32/PR91 e Safety27b8599 limpos; manifesto local reconciliado.
  Prévia habitual ainda2b3b274,365 hashes reconferidos; corretivos novos não ativados.
- PRs313–318: CI completo aprovado. PR319/320: Linux/PG/instaladores/lint
  aprovados; jobs Windows ainda em execução no corte20:18UTC, sem aceite físico.
- Nova admissão após boot16:34:53-03 permitiu101 testes focados App,195 do
  framework,checker174,27 CCI/SA-REF e doctor16OK/2avisos. Quatro warnings NumPy
  na referência de pesquisa ficam registrados. Não é prova de MT5/launcher final.
- WIP de pacotes: package_plan.py, resources/commands.py e test_asset_package_plan.py;
 15 testes passaram dentro do lote101, mas não há commit/PR da frente nem ref local
  codex/pacotes-inventario-qa. Planejador não é exportação/reunião integral.
  Checkpoint TAR/manifesto verificado em parecer-handoff-20261114, no mesmo disco.
- Próxima ação: fechar CI319/320, revisar/compilar/qualificar composição nativa e
  GUI/prévia exata; depois retomar transporte por famílias/owners e QA de reunião.
  Windows físico, dados/conta/destino pessoais e qualificação M1 têm aceites próprios.

## Prioridade corretiva definida pelo proprietário — 13/11/2026

**Assumir primeiro os itens P0 de cálculo e confiabilidade desta demanda.**
A ordem é E14-CALC-01 → IND-CCI-01 → IND-AGGR-01 → E14-METRIC-01 →
E14-SCOPE-01 → E14-EXEC-01,
seguida de SA-01-CONTRATO e SA-01-PLOT em P1. Reprodução, regressão,
correção integrada e validação dos resultados precedem a otimização de
presets e a ampliação do Expert/painel/dashboard nos caminhos afetados.
As frentes independentes de proteção de dados e diagnóstico do host continuam.

Os oito itens foram acrescentados à fila abaixo, em estado **livre**, com
responsáveis por componente. O próximo agente registra sua posse real, confere
working tree, código e PR atual e atualiza branch/PR, SHA, comando e resultado.
Os detalhes executáveis e critérios de aceite estão no
[backlog existente](docs/propostas/LAB_SUPER_ATR_PENDENCIAS_20261024.md#correções-prioritárias),
com prioridade refletida no [roadmap](docs/ROADMAP.md).
Os achados IND-CCI-01/IND-AGGR-01 e os seis vídeos complementares têm
[análise e aceites em Analytics](docs/propostas/ANALYTICS_OPERACOES.md#vídeos-complementares).
O relatório de 28 vídeos foi confrontado com a fonte, com erros externos e
capacidades existentes identificados. O
[mapa de atuação e cruzamentos](docs/propostas/ANALYTICS_OPERACOES.md#encaminhamento-para-atuação-e-cruzamentos)
liga os temas às fichas F19/F20 e explicita investigação, integração e aceite.
Publicação documental desta demanda na branch `codex/auditoria-videos-roadmap`,
dependente da composição PR308. O executor de cada correção deve registrar
seu próprio SHA e aceite na linha correspondente.
Uma dependência nativa bloqueada deve receber causa e próxima ação exatas,
permitindo avançar no próximo recorte independente. Escrita e cargas locais
continuam serializadas no checkout canônico, com orçamento antes de carga pesada.

## Aceites Windows aguardando host qualificado — determinação de 13/11/2026

O proprietário adiou os aceites específicos de Windows até disponibilizar um
host qualificado. Isso inclui instalação e integração nativas, observação
manual/física e reproduções instrumentadas DIAG-CI304/DIAG-CI-NODE. Esses aceites
estão **bloqueados por disponibilidade futura de host qualificado**. Linux,
implementação portável, instrumentação, regressões e publicação continuam.
O CI Windows habitual permanece evidência de runner, com SHA e escopo próprios.

Antes da futura execução, registrar Windows/versões suportadas, toolchain,
MT5/build quando aplicável, conta permitida sem segredos, recursos/sensores,
dados de QA, recuperação e periféricos do percurso. Não transferir aceite de
CI ou mocks para esse host. Não há autorização de negociação REAL nesta tarefa.

## Execução corrente dos corretivos — 14/11/2026

Um escritor Codex nesta tarefa. App550b6d5/codex/launcher-classes e índice vazio
preservados;93 WIPs originais conferidos em wip-recheck-20261114.json. Somente
hunks próprios em handoff/plano/Biblioteca diferem da fotografia. Publicação
usa índices isolados e docs reconstruídos do pai publicado, sem levar o WIP.

E14-CALC: H569/PR85, App72/PR313 e seis jobs de produto aprovados. CCI:
PR314/2b8bb96; Linux4655 passed/473 skipped, núcleo81%, PG/instaladores/lint
aprovados; Windows em curso. Corrige as quatro falhas BD01/catálogo do CIe0.
Versão matemática2.0+15dc1f90361c,19 testes finais/63 únicos entre lotes locais.
CLI/GUI anteriores pertencem à fonte276/versão7c344 e não certificam a precisão final.

IND-AGGR: H438/PR86 (fonte461),174 testes/checker171 aprovados; Appadf31e4/PR315
com Linux completo/PG/instaladores/lint aprovados, Windows em curso. E14-METRIC:
H231343f/PR87,181 testes/checker171 aprovados; Appeb3a2dd/PR316 integra o pin.
E14-SCOPE: H4982259/PR88,184 testes/checker171 aprovados; Appd136f7c/PR317 integra
o pin. Duas regressões de escopo e o timeout local10s estão preservados; a nova
admissão do checker no c964 passou171 arquivos em1,67s. Safety27b8599 detached.

Candidato d136 preparado e365 hashes conferidos,6.383.983B, com pinH498/Safety27.
O link habitual permanece2b e conserva retorno. O controlador GUI e a CLI de QA
agora apontam só para o artefato d136; consultar recibos antes de repetir.
GUI CCI final, MetaEditor e testes compilados locais seguem adiados por RAM/swap/
PSI; os quatro testes curtos de consumidores CCI também não foram admitidos.
Não reduzir gates nem encerrar processos alheios. Recibos privados:
state/qa-tmp/corretivos-20261113. Não há negociação, merge/release, novo ambiente,
limpeza compartilhada ou processo próprio residente.

E14-EXEC: H95af71a/PR89,185 testes/checker171/sintaxe aprovados; App7ec6476/PR318.
Journal conserva a barreira de reentrada na barra do fechamento após reinício.
Legado sem vínculo só pesquisa Tester DEMO; sem gestão automática de posições
ao vivo. Fonte S09 2.14/perfil16 passou gate leve exato; mapper completo/Session
aguardam CI próprio e admissão local. CCI2b8 concluiu os seis checks, incluindo
Windows CI. Prévia segue2b; d136 preparado é anterior ao pinH95, não ativado.

SA-01-CONTRATO assumido emH `codex/sa01-triplo-contrato`, base95af71a. Fechar
variante própria com três curvas no mesmo TF, fórmula existente, seed e consenso,
intenção por episódio e SL absoluto/TP ausente no executor vinculado existente.
Vídeo5ABRHXAaEKU indisponível à leitura por throttling; ajuda primária Nelogica
confirma TR/suavização, não prova seed/catraca. Equivalência externa permanece
hipótese. SA-REF-01 congelada; SA-01-PLOT ainda livre. Windows físico aguarda host.


Marco14:10UTC: PRsApp315/adf,316/eb3,317/d136 e318/7ec concluíram seis checks
de produto; Windows CI não certifica alvo físico. TriploH PR90 publica kernel
composto5d73, referência Decimal/binary64 corrigida4511 (CI43602602936 aprovado),
e API de SL absoluto/TP0 explícito84140d3. CI43602957255 da API falhou, em investigação.
S09 passa a2.15 pela nova closure; App318 ainda fixaH95/2.14, portanto não
tratar o checkoutH em desenvolvimento como composição instalada aprovada.
RAM4,2–4,3GiB, swap contínuo e PSI adiaram nova admissão local; não há filho
próprio de QA em execução. Continuar pelo erro de CI, depois Expert/inputs/
vínculo App, integração do pin e prévia. TestesApp devem usar cwd repos/app.

## Fila de diagnóstico e correções — 13/11/2026

Codex assumiu a escrita desta passagem no checkout canônico `repos/app`, em
`codex/launcher-classes`, HEAD `550b6d5`. O índice principal e os WIPs anteriores
foram preservados. As correções publicadas usam índice separado sobre cada PR.
Os owners e as dependências ficam na fila abaixo. A retomada deve conferir
working tree, PR e owner atual antes de assumir qualquer item.

O código composto `0e4f4c6869eaaf7ad2b0345db0e23c108802a7d4`, PR308, passou
nos seis jobs de produto. Windows teve 4.904 aprovados e 205 não executados,
com 78% de cobertura do núcleo. Linux teve 4.636 aprovados e 473 não executados,
com 81%. PostgreSQL, instaladores Linux/Windows e lint passaram. Os casos de
cancelamento, clique/transporte Node, lease e cenário macro executaram e passaram
no Windows. As PRs307 `c296538`, 309 `d0e97d6`, 310 `97da399` e 311 `cb02d78`
também passaram nos seis jobs. A documentação desta fotografia é conferida
pelo diff e pelas fontes citadas. O CI da ponta da composição está na PR308.

A prévia efetiva do atalho e menu é `development-2b3b274da6de`, com 365 arquivos
idênticos ao código composto por hash. O retorno imediato `650656f` tem 354 arquivos
conferidos, e o retorno histórico `57d13ee` tem 341. O caminho estável e os dois
atalhos foram conferidos. HADOUKEN `d4d389e` está limpo em branch e Safety
`27b8599` está limpo detached, conforme os pins existentes.

A fila tem 28 itens: 14 concluídos, seis bloqueados e oito em validação. Reavaliar os aceites de cada alvo pela evidência própria.
Os aceites possuem escopo explícito e dependências próprias.

| ID | Objetivo / owner | Estado | Branch/PR e último SHA publicado | Evidência, dependência e próximo passo |
|---|---|---|---|---|
| E14-CALC-01 | P0 · Comissões, fills, parciais e ciclos / Codex nesta tarefa, HADOUKEN-E14 e Analytics | em validação | HADOUKEN PR85 `569718c`; App PR313 `72f95e9`, `codex/e14-calc-integracao` | Consumidor comum reproduziu a falha e corrige26→28 com rateio por volume, taxas, INOUT e entradas antigas. 169 testes únicos passaram em lotes:162 antes de interrupção própria por reserva, sete restantes e sete afetados repetidos. MetaEditor/Wine0erros/0avisos,39fontes idênticas. Manifesto App em reconciliação explícita; prévia continua2b até gates. Aceite de histórico nativo/visual é próprio; Windows aguarda host qualificado |
| IND-CCI-01 | P0 · Fórmula, aquecimento e precisão CCI / Codex nesta tarefa, Dados/App | em validação | App PR314 `e0eae5d`, `codex/ind-cci-01` | 63 testes únicos/19 finais aprovados. Fórmula compartilhada, BD06, produtor aditivo e precisão por soma compensada/centralização sem quantizar valores. Fonte276 passou CLI e GUI XWayland; precisão15dc publicada, novo artefato/GUI em QA. Versões anteriores/snapshots preservados. Prévia/CI pendentes; iCCI e Windows físico têm aceites próprios |
| IND-AGGR-01 | P0 · Atualização incremental do AggrVolBalance / Codex nesta tarefa, indicadores HADOUKEN | em validação | HADOUKEN PR86 `461a067`, recibo `438e49e`; App `codex/ind-aggr-integracao` | CI Linux executou174 testes e checker171: aprovados. A regressão anterior falhou em quatro cenários. Corrigidos AS_SERIES, ausência vs zero válido e interrupção; candle/volume continua aproximação. Pin revisado no manifesto. Execução local/MetaEditor/CopyBuffer e prévia aguardam recursos; Windows físico aguarda host qualificado |
| E14-METRIC-01 | P0 · Indisponibilidade, frescor e cobertura / Codex nesta tarefa, painel HADOUKEN/Analytics | em validação | HADOUKEN PR87, pin231343f; App `codex/e14-metric-integracao` | CI reproduziu total parcial8 substituindo28; correção passou181 testes e checker171. Estados explícitos, último snapshot válido, idade/recortes, DTO/painel e leitor ativo de marcações qualificados. O pin final rejeita EMPTY_VALUE e passou181 testes no CI43581200274. MetaEditor/GUI/prévia aguardam recursos; Windows físico aguarda host qualificado |
| E14-SCOPE-01 | P0 · Escopos de métricas, risco e zeragem / Codex nesta tarefa, Safety/painel/Analytics | em validação | HADOUKEN PR88 `4982259`; App `codex/e14-scope-integracao` | Duas falhas reproduzidas em182 testes. Correção final passou184 testes/checker171 em CI. Snapshot vincula conta/servidor/moeda; painel explicita bases patrimoniais, Magic/todos os ativos e contribuição netting não qualificada. Zeragem exige inventário legível; executor revalida posse/tipo/símbolo/volume e preserva alheios. Native/GUI/prévia aguardam recursos; Windows físico aguarda host |
| E14-EXEC-01 | P0 · Intenção, parcial, incerteza e reinício / Codex nesta tarefa, Expert/Safety | em validação | HADOUKEN PR89 `95af71a`; App `codex/e14-exec-integracao` | Duas falhas reproduzidas em185 testes; correção passou185/checker171. Journal impede reentrada na barra do fechamento após reinício. Sem vínculo só pesquisa Tester DEMO; painel propaga recusa. S09 2.14/perfil16 exige fingerprint/revisão próprios e não herda aceite2.13. Fonte instalada passou gate leve; mapper/Session CI e QA nativa/prévia pendentes de recursos. Windows físico aguarda host |
| SA-01-CONTRATO | P0 · Fórmula, três curvas e consumidor / Codex nesta tarefa, HADOUKEN/App | em validação | HADOUKEN PR90 `42793107`; App `codex/sa01-triplo-integracao` | Contrato1/Expert1.00: consenso fechado, SL absoluto curva1, TP0, episódios e rearmamento após início. 192 testes framework/CI; pacote/UI/trace-journal-fills integrados, App CI e QA nativa pendentes. SA-REF-01 congelado; equivalência externa não comprovada; Windows físico aguarda host |
| SA-01-PLOT | P1 · Plot opcional com parâmetros/seed efetivos / Codex nesta tarefa, HADOUKEN/App | em validação | HADOUKEN PR91 `566c8e32`; App `codex/sa01-plot-integracao` | Mesmos pontos fechados, plot limitado e alternância pelo painel; 195 testes/checker174 passaram em API simulada. Novo perfil14 inputs, trace e comparador de pares integrados; App CI, QA visual/nativa e preview pendentes. Objetos/estudos alheios preservados; sem aceite operacional |
| DIAG-CONFIG | Mesma leitura literal na GUI, CLI e conector / App | concluído | PR299 `d0ea937` | SHA anterior publicado. Conflito apenas documental com master impedia CI. Recibo reposicionado e `merge-tree` limpo, novo SHA enviado. Código mantém os 32 testes e launcher de QA registrados. SHA remoto confirmado. Cinco jobs aprovados. Windows tentativa 1 teve 4.663 aprovados/195 não executados e três timeouts em Node/Node/g++. Caso Node já tem proposta de outro agente na PR301 `28e4c17`, com seis jobs aprovados. A tentativa diagnóstica 2 do mesmo SHA passou nos seis jobs, incluindo Windows. As falhas da primeira tentativa continuam preservadas. O timeout do compilador não foi reproduzido |
| DIAG-EXCURSOES | MAE/MFE disponíveis nas lentes e custo ask em vendas / Dados/Laboratório | concluído | PR305 `38304e0`, `codex/lab-excursoes` | Publicado e PR criada. 38 testes de excursões/enriquecimento/agregação e 21 de cache/livro/importação/CLI aprovados em lotes separados. Lote final 13 aprovados/1 PG sem servidor. Inclui legado, preservação do livro, point inválido e spread calibrado. QA por launcher CLI em base sintética confirmou ask e estado. GUI real XWayland pelo launcher aprovada em janela própria, com MAE 4/MFE 2 e ask declarados. Seis jobs de produto aprovados, incluindo Windows e PG, no SHA `38304e0`. Integração em master e na próxima prévia pertencem à composição |
| DIAG-PROTECAO-WIP | Proteção independente do resgate anterior e do checkpoint atual / integrador | concluído | página privada existente, recibo local `private-checkpoint/remote-read-proof.json` | Acesso reconferido via Pages, sem compartilhamentos. ZIP 489.138 B/1 parte, SHA `c4f843f78d979ff6aab6f79299e1151a6ed88a29897a158eac48513b806450fd`. Parte relida integralmente. Texto e ZIP decodificado conferidos. Recuperador oficial validou 19 arquivos e o WIP anterior teve seus 24 hashes conferidos. Conteúdo guarda checkpoint parcial de amostra. Base principal tem proteção e aceite próprios |
| DIAG-IDENTIDADE | Marcação de ativos pela identidade registrada / App/Dados | concluído | fluxo já integrado no App | Hipótese funcional descartada por inspeção do caminho ativo. `UIManager.refresh_inventory` lê `data_inventory` em `run_bg`, e `_inventory_loaded` agrupa por símbolo registrado. O método chamado pela rotina antiga que divide nomes é vazio. Nenhuma correção funcional necessária neste recorte |
| DIAG-CI304 | Reproduzir travamento de cancelamento/retomada / Execução App | bloqueado | PR304 `dc17f49`, seis jobs de produto aprovados | Ocorrência antiga no Windows 130029697303 e COMMIT de presets preservados. Regressões Linux passaram. Ponta atual passou no Windows completo. Lock retido continua hipótese. Dependência exata é uma reprodução Windows com captura das conexões/leases no ponto do timeout. Recorrência ausente nos SHAs recentes. Retomar instrumentação quando a falha reaparecer |
| DIAG-TICKS | Auditoria tolerante a metadados inválidos, sem certificação indevida / Dados | concluído | PR304 `dc17f49` | Auditoria v2 publicada. 15 falhas reproduzidas. Lote integrado 80 aprovados e lote final 34 aprovados, sobrepostos. Launcher `--cli trades fidelity` em 3 ticks próprios com flag infinita retorna JSON finito, classe D e intrabarra recusada. Seis jobs de produto aprovados no SHA `dc17f49`, incluindo Windows/PG. Aceites operacionais ficam separados |
| DIAG-AMOSTRA | Repetição do mesmo backtest não aumenta amostra nas lentes / Laboratório | concluído | PR306 `060afab`, `codex/lab-amostra-execucoes`, dependente da PR304 | Deduplicação qualificada por identidade do produtor e livro, com consulta integral explícita. Três backtests do produtor em 600 barras próprias guardaram 9 operações. Launcher CLI e GUI XWayland mostraram 3/9. Lotes focados aprovados, inclusive leitura legada sem releitura redundante. Seis jobs de produto aprovados no SHA final. Aceites nativos/financeiros continuam próprios |
| DIAG-RESTORE | Identidade, conteúdo e relógio da cópia restaurada / Recursos | concluído | PR307 `c296538`, `codex/restore-backup-integridade` | URI com # corrigida, mesma fotografia e inventário conferidos. Duas falhas adicionais reproduzidas após trocar relógio/fuso, corrigidas pela invalidação do cache do validador existente. Seis novos casos cobrem resultado normal/incerto e compatibilidade. Lote integrado de 39 aprovados em 14,20 s. A ponta anterior 9c passou nos seis jobs. Seis jobs de produto aprovados em c296538. GUI XWayland da composição 2b confirma troca de relógio, incerteza e recuperação em dados próprios |
| DIAG-RESTORE-RECUP | Intenção durável, resultado consultável e recuperação explícita / Recursos | concluído | PR307 `c296538`, núcleo validado em `9c2c7eb` | Produtor/lease existentes, recibos privados e identidade física. Interrupção abrupta própria com saída 17, consulta incerta, hardlink e diretório alterado bloqueados. Rollback explícito 42. GUI oficial visível em XWayland provou 7→99, 18 pulsos e intervalo máximo 0,581 s, zero erros inesperados. 31 casos finais aprovados. Pico GUI 270,2 MiB, mínimo disponível 7,96 GiB, máximo 68 °C. Seis jobs passaram em 9c. O corretivo de relógio possui validação própria na linha anterior. Energia/reinício físico exigem janela própria |
| DIAG-LAUNCHER-LOCAL | Recursos e fontes públicas abrem sem Wine / integração App | concluído | composição PR308 `f6b6274` | Recursos/B3/crypto integrados à política única após três falhas reproduzidas. Lotes de 24 e 10 casos aprovados, com ponte controlada e extração preservada na classe nativa. Publicado e integrado na composição. Prévia ainda depende do QA do artefato exato |
| DIAG-LEASE-REBOOT | Reconciliação de lease após reinício com prova da mesma máquina / Execução App | concluído | PR309 `d0e97d6`, runtime `2a5638d` | 66 testes de lease/fila passaram em 39,14 s. Primeira tentativa Windows 2a foi cancelada prematuramente a98% e continha falha do teste que lia outro descritor sob bloqueio. Leitura agora usa o descritor do lease e conserva os estados ativo/liberado, com recusa externa controlada. 7df corrigiu a leitura do lease no Windows. Sua única falha foi o prazo Node. D0e97d6 incorpora orçamento macro e corrige a origem registrada, Seis jobs de produto aprovados em d0e97d6, incluindo Windows 4705/195 e teste do descritor ativo/liberado. Fonte, controles e gates preservados. Reinício físico tem aceite próprio |
| DIAG-CI-MACRO | Propagar orçamento já declarado no cenário macro / QA/Dados | concluído | PR311 `cb02d78`. Integrado PR309 `d0e97d6` e PR308 `0e4f4c6` | Falha reproduzida com sampler1536 MiB, sem alocação equivalente. A análise recebe o orçamento já medido da coleta. 5 casos passaram em 8,40 s, incluindo recusa de 2048 MiB acima do teto 1792 MiB. Gates de memória/tempo e assertivas gráficas mantidos. Runtime2b/365 arquivos idêntico ao fonte 18fe por hash. Seis jobs de produto aprovados em cb02d78, incluindo Windows 4664/195. A composição0e também executou o cenário gráfico aprovado |
| DIAG-CI-NODE | Distinguir prazo de execução e captura Node no Windows / QA | bloqueado | PR301 `28e4c17`, proposta de outro agente preservada | Windows 7df/job131581528852 teve somente TimeoutExpired10 s no teste de clique de volume, com 4703 aprovados e195 não executados. O traceback aguarda a thread de captura stdout. Isso não prova a causa. PR301 altera10→45 e passou nos seis jobs, sem integração nesta composição. Nenhum limite foi aumentado nesta passagem. As três pontas atuais executaram e aprovaram o caso no Windows com 10 s. Causa permanece sem reprodução instrumentada no runner Windows. Próximo é capturar estado/saída do processo antes de uma recorrência. Alteração de mecanismo exige reprodução pertinente |
| DIAG-RETENCAO | Retenção conserva artefatos sem prova de posse / Recursos | concluído | PR310 `97da399`, `codex/backup-retencao-posse`, depende da PR307 | Recibos perdidos/corrompidos, troca de fonte, sobreposição de pastas e leitura incompleta reproduzidos em árvores próprias. Intenção durável, identidade/hash no ato, controle preservado, referências de restauração e SQLite protegidos. 45 casos focados passaram em 10,15 s. 29 de proteção/sessões/leitura parcial em 13,31 s, com Tk no compositor real. Sete fontes estáveis conferidas por hash. Launcher CLI confirma retenção, interrupção, consulta, perda de recibo e histórico. Lote antigo interrompido após 31 aprovados em 101,74 s, evidência retida. 51 casos finais em 14,75 s, com oito hashes estáveis. Seis jobs de produto aprovados em 97da399. GUI XWayland do artefato2b confirmou cópia e consulta no worker, preservando arquivo sem proveniência |
| DIAG-COMPOSICAO | Composição publicada, prévia e proteção reconciliadas / integrador | concluído | PR308 `0e4f4c6`. Prévia efetiva `2b3b274` | Seis jobs de produto aprovados no código 0e4f4c6. Windows 4904/205, Linux 4636/473, cobertura do núcleo78%/81%. 336 arquivos runtime idênticos ao checkout, run.bat equivalente por CRLF e 365 hashes do artefato. Ativação a674860c por link atômico aprovada, dois atalhos conferidos, pico 23,3 MiB. Launcher estável consultou cópias próprias com saída0, valor99, pico 134,6 MiB e mínimo 8,21 GiB. Estado de símbolos padrão ausente preservado. Método de produção idêntico validou fallback em ambos os artefatos. Após a reconciliação, novo QA pelo launcher estável abriu janela visível, confirmou ausência do arquivo de símbolos, restauração incerta7→recuperação99 e consulta no worker. Pico 273,2 MiB, mínimo 8,30 GiB e máximo 65 °C. Quatro estados de preferência por hash/existência preservados nessa janela isolada. Observação180 s no runtime 561 byte a byte igual. Retornos650/57 preservados. Aguardar Windows e registrar os estados terminais |
| ACEITE-NATIVO | MT5/Tester, DEMO, netting e Windows físico / operacional | bloqueado | pins H `d4d389e`, Safety `27b8599` | Sem autorização vigente de negociação nem Windows físico. Conta netting e ensaio de falhas contra broker próprios. CI/mocks não substituem esses alvos |
| ACEITE-DRIVE | OAuth real, proteção independente, pacotes por ativo e reunião / Recursos/Dados | bloqueado | PR302 integrada. PR303 `37cf1a3` com seis jobs aprovados | Inventário só de schema conferiu 30 tabelas, 7 séries registradas e 23 internas conhecidas. Snapshot canônico transporta projeção de oito campos OHLCV e famílias tipadas, sem reconstituir toda a base bruta. BD03 fixa uma base lógica por banco/schema. Solicitada decisão objetiva entre pasta QA com origens separadas e base QA única, necessária para semântica de conflito e gravação. Nenhum dado privado usado como fixture. Próximo após resposta é pacote/inventário por owner e reunião no destino escolhido. OAuth/envio do proprietário permanecem posteriores ao aceite sintético conjunto |
| ACEITE-UI | Entrada física, acessibilidade, login e observação prolongada / App | bloqueado | artefato PR308 `561152b` | Janela real XWayland e observação controlada de 180 s aprovadas, com solicitações instrumentadas e preferências de QA. Entrada física/monitor-dock-DPI exigem operador e periféricos. Acessibilidade exige percurso com tecnologia assistiva. Login exige janela exclusiva da sessão. Observação controlada de 30 min está no item independente abaixo. Esses aceites continuam específicos, sem qualificação por mocks/CI |
| ACEITE-OBSERVACAO | Observação contínua controlada de 30 min / App QA | concluído | runtime PR308 `2b3b274`, código `0e4f4c6` | Launcher estável, SQLite própria, XWayland visível e controladores instrumentados. Duração 1.800,015 s, 41.495 heartbeats, intervalo máximo 0,820 s, 59 atualizações e 30 trocas de tema, zero erros de callback. Pico 273,1 MiB, mínimo 7,85 GiB disponível e máximo 66 °C. Quatro estados de preferências por hash/existência preservados. Recibos em preview-stable-observation/proof.json e resources.json. Entrada física e tecnologia assistiva pertencem ao ACEITE-UI |
| ACEITE-M1 | Qualificação estatística/econômica prospectiva / Dados e pesquisa | bloqueado | PR297 `a87dd76`, evidência pública T134_M1_EURUSD_H1_QA_20261106.json | Recibo já publicado tem zero padrões aprovados, 0/2 eventos e contrato de custo bloqueado por calibração posterior ao período. P04 exige plano temporal e suporte pareado congelados. Depende de modelo de custos elegível antes do período e histórico compatível com sessões/derivações para nova validação prospectiva. Usar dados autorizados, com acesso/extração no aceite nativo |

### Arquitetura e configuração conferidas

O launcher classifica consultas locais antes de iniciar a ponte. `app.py` e a
CLI constroem os serviços comuns. `UIManager.run_bg` encaminha I/O ao worker.
`DatabaseManager` mantém SQLite/PostgreSQL, relógio e controles. Dados publica
identidades, revisões e snapshots pelo serviço canônico. Laboratório e Analytics
os consomem. Recursos reutiliza snapshot SQLite, lease e recibos privados para
cópia, consulta e recuperação. App compõe HADOUKEN e Safety fixados, com
execução e risco nos donos existentes. O master remoto conferido é `09a618c`.
A composição de revisão e a prévia possuem as identidades registradas acima.

Os defaults ficam em `mt5_extracao/bootstrap/config_builder.py` e no exemplo de configuração.
O launcher recebe a configuração explícita, e a configuração privada permanece
fora do Git. O `.venv` único pertence ao App e é compartilhado pela prévia.
Base ativa, pesquisa e QA mantêm seus escopos. A identidade BD03 pertence ao
conteúdo de cada banco/schema, com namespace e linhagem transportados pelos
snapshots. O plano de pacotes deve conservar os escritores/leitores canônicos.

### Evidências, orçamento e limites

Os recibos locais ficam em `state/qa-tmp/diagnostico-20261112/` na raiz MT5X.
O lote final de retenção/restauração teve 51 casos aprovados em 14,75 s, com oito
hashes de fontes estáveis. Lease/fila teve 66 aprovados em 39,14 s. Macro teve
cinco aprovados em 8,40 s com RSS simulado, inclusive recusa acima do orçamento
já declarado. Os lotes anteriores de 39 e29 casos possuem sobreposição.
Doctor offline do artefato teve 16 OK, dois avisos e zero falhas.

`preview-stable-final-qa/proof.json` registra GUI visível XWayland pelo launcher
estável, controladores instrumentados e SQLite própria. A restauração incerta
retornou7 e a recuperação explícita99. Cópia/consulta rodaram no worker. Após
reconciliar a ausência de `config/default_symbols.json`, a GUI final confirmou
esse estado. Pico273,2 MiB, mínimo 8,30 GiB disponível e máximo 65 °C. Os quatro
estados de preferências por hash/existência foram preservados nessa janela.
A observação anterior de 180 s teve 4.168 heartbeats e intervalo máximo 0,571 s.
A observação final de 30 min teve 41.495 heartbeats, intervalo máximo 0,820 s,
59 atualizações, 30 trocas de tema e zero erros de callback. O pico foi
273,1 MiB, com mínimo 7,85 GiB disponível e máximo 66 °C. O recibo
`preview-stable-observation/proof.json` delimita esse percurso controlado.

Os ensaios usam um processo e um thread de BLAS/OMP. A reserva operacional é
4,5 GiB, acima do mínimo 4 GiB do host. O orçamento da observação é1800 s,
768 MiB de RSS e32 MiB de saída, com telemetria20 s e guardas2 s. A medição de
04h28 UTC teve 8,25 GiB disponíveis, PSI de memória0 e64 °C. O trip passivo
ACPI é100 °C e o crítico105 °C. Contadores Btrfs acumulados têm6.741 erros de
corrupção, com zero erros de leitura/escrita/flush/geração, iguais à referência.
A janela observada de 30 min do kernel teve zero avisos. A causa dos reinícios
continua indeterminada. Os dados globais de swap variaram entre janelas.

A base principal configurada teve tamanho603.648.000B e mtime preservados
nas conferências por metadados, com arquivos auxiliares ausentes. O QA usou
bases sintéticas próprias. O checkpoint privado foi relido e recuperado pelo
helper existente, conforme a fila. A proteção externa atual da base principal
possui dependência de destino e política do proprietário.

Cinco artefatos intermediários próprios foram removidos após conferir os1.825
hashes, a ancestralidade no Git e as referências de processos visíveis. O recibo
`unused-artifacts-cleanup.json` conserva os manifestos e a intenção durável
08360e5a. Foram31.827.463B em 0,41 s. Prévia ativa, retornos650/57 e artefato
744 de outro trabalho foram preservados.

### Roteiro e critérios de aceite

| Prioridade | Próxima ação e dono | Dependência e aceite |
|---|---|---|

| P0 | Definir destino dos pacotes/reunião / proprietário e Recursos/Dados | A pergunta existente distingue coleção QA com origens separadas e base QA única. BD03 fixa uma base lógica por banco/schema. O contrato do destino orienta conflitos, namespace e importação. Inventário já conferiu30 tabelas, sete séries e23 controles conhecidos. O aceite exige todas as famílias/colunas legadas/revisões, relógio, hashes e dependências, plano só leitura e nova QA idempotente pelos consumidores existentes |
| P1 | Login Drive e proteção externa da base principal / proprietário e Recursos | Destino, conta, política e código de recuperação independentes autorizados, depois dos pacotes/reunião sintéticos. Exercitar renovação/revogação, retomada, checksum e recuperação própria pelo fluxo oficial |
| P1 | Interface e alvo nativo / operador e owners App/HADOUKEN/Safety | Janela exclusiva, periféricos e tecnologia assistiva para entrada física/acessibilidade/login/dock/DPI. MT5/Tester/DEMO/netting/Windows físico requerem acesso e autorização próprios. Evidência por alvo e percurso com proteção de processos/dados |
| P1 | Capturar recorrências Windows / Execução App e QA | Locks de cancelamento e prazo Node passaram nas pontas atuais. A causa de cada ocorrência anterior depende da captura de conexões/leases ou estado/saída do processo no momento da falha. A PR301 de outro agente conserva a proposta45 s. A composição usa10 s |
| P2 | Qualificação M1 / Dados e pesquisa | Modelo de custo conhecido antes do período, suporte pareado e plano P04 congelados, histórico elegível com sessões/derivações. O recibo publicado tem zero padrões aprovados |

### Ocorrências da investigação

Uma execução Windows foi cancelada prematuramente quando ainda progredia a98%.
O log foi preservado e a repetição do mesmo SHA revelou as falhas de lease e
orçamento macro, corrigidas e validadas nas pontas atuais. A atribuição anterior
da falha macro a7df foi corrigida após ler o log completo. A ocorrência foi em 2a.
O baseline de preferências começou nas sondas finais e a preservação foi comprovada
nessas janelas isoladas. Uma inspeção inicial exibiu uma URL de autorização na
saída da ferramenta. As inspeções posteriores passaram a filtrar URLs e argumentos
sensíveis. A recuperação privada e os recibos conservaram suas restrições de acesso.

O backlog completo permanece em `docs/PLANO_DE_TRABALHO.md` e nos contratos
Dados/LAB/F19/F20. Seções anteriores conservam suas fotografias datadas.

## Exposição, fidelidade de ticks e recuperação condicional — 10/11/2026

Pedido do proprietário de 09–10/11 (vídeo martingale × lote fixo). Fonte canônica:
[ANALYTICS_OPERACOES §11](docs/propostas/ANALYTICS_OPERACOES.md): auditoria (11.9), classes A–E
(11.10), especificação H0–H6 (11.11), coesão com o Ultra e piso de PF 2 (11.12), resultados e
lacunas (11.13). Entregue no App, só estudo: T16.14 `mt5x trades fidelity`, T16.15
`mt5x trades recovery` (cinco modos novos também na guia Execução) e T19.52a
`mt5x trades router`. 21 testes novos; regressão afetada 277 aprovados (Linux/Xvfb); launcher
oficial em base de QA isolada com barras reais EURUSD M15
([recibo](docs/evidencias/T1615_RECUPERACAO_ROTEADOR_EURUSD_M15_QA_20261110.json)).

Resultado: a base local é classe D (nenhum tick extraído); preço médio sem sinal reprovado em
todos os runs; adição condicionada com 3–9 casos por run (amostra insuficiente, tendência
desfavorável); política condicional sem candidato elegível; nada sustenta PF 2. HADOUKEN/Safety
intocados; nenhum gate, preset ou executor mudou. Não validado: Tester/MT5, DEMO, Windows, CI.
Próximo: extrair ticks e calibrar custos, ampliar história/ativos, corrigir runs triplicados no
livro e estudar modos de saída antes de adição (lista em 11.13). O host reiniciou uma vez durante
esta passagem, com suíte e preparação de QA simultâneas: rodar uma operação por vez.

## PR302 — corretivo do CI em 08/11/2026

Owner: **Recursos/Dados**, tarefa **T20.30-HOST1-DRIVE**, limitada a SQLite/uma conta. Na ponta remota `9d55fb50b00861f35e0617c9ccd2273ade471a52`, o CI 42400089570 falhou em `Testes (ubuntu-latest)` e `Testes (windows-latest)`; PostgreSQL, lint e os dois instaladores passaram. Logs confirmam: Ubuntu não tinha `.venv` ao chamar `run.sh`; Windows tentou executar `run.sh` e usou nome SQLite com `?` (inválido no Windows).

Corretivo para revisão: `.github/workflows/ci.yml` cria `.venv` com acesso às dependências previamente instaladas e executa `bootstrap configure --non-interactive --skip-credentials --no-doctor` antes de `pytest`. `tests/test_drive_security.py` seleciona `run.bat --cli` via `cmd.exe` no Windows e `run.sh --offline --cli` no Linux; o teste SQLite usa `a#b%c space.db` e comprova a leitura do registro `id=7` no snapshot. O `run.bat` existente não mudou; a transmissão efetiva de argumentos com espaço e `%` depende do novo Windows CI.

O recorte de código foi comparado com o SHA da PR: dois arquivos afetados; as duas regressões diretas passaram de novo no Linux em 08/11 (`2 passed`, `18 deselected`). Da passagem técnica anterior: regressão conjunta de sete módulos `104 passed` em 39,42 s, ShellCheck aprovado, doctor offline `16 OK/2 avisos/0 falhas`, YAML e bootstrap conferidos. Esses resultados locais não qualificam Windows físico, OAuth/Drive real, PostgreSQL do Drive, MT5 nem negociação. A nova execução do CI somente poderá ser declarada aprovada após resultados associados ao SHA publicado.

Checkout principal preservado em `codex/launcher-classes`/`550b6d5` com alterações de outras frentes. O incremento da PR302 deve usar índice Git isolado sobre a ponta remota, sem stage global, troca de branch ou nova árvore de trabalho; publicar somente o corretivo e as atualizações específicas desta passagem. Não atualizar a prévia do proprietário com a branch do Drive antes dos aceites correspondentes. Depois de estabilizar a PR302, próximo incremento autorizado em PR independente: **múltiplas contas Drive**, com identidades estáveis, cofres isolados, seleção explícita, confirmação da identidade antes do envio e testes sintéticos.

## Drive — estabilização sintética de 07/11/2026

A [PR302](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/302) recebe somente correções do objetivo original de SQLite/uma conta: recuperação fora de argv/JSON/logs, entrada oculta, snapshot de Recursos reutilizado, estados separados de envio/checksum/restauração, identidade remota estável e retomada após interrupção. Falha conserva a última cópia boa e o preparo cifrado. Downloads acima de 70 MB são manuais e têm plano/conferência local; restauração confere SHA-256, contagens e integridade.

Baseline: 63 testes. Regressão integrada: 99 aprovados em 42,8 s; pico observado da árvore de processos 315,4 MiB; CPU 29,6 s usuário/3,2 s sistema. I/O informado pelo SO: 2.008 blocos de entrada/1.034.576 de saída (512 B). Ajuste final de erro do cofre: 28 testes focados aprovados, com sobreposição. ShellCheck passou; doctor offline: 16 OK/2 avisos/0 falhas. Launcher oficial Linux/Xvfb com série sintética: verificar → enviar → ensaiar restauração, zero erros de callback. Não é aceite OAuth/Drive real, CI do novo SHA, Windows físico, MT5/DEMO ou PostgreSQL.

Prévia efetiva conferida em 07/11: `650656fac67d` (PR300), 354 hashes sem divergência; retorno `57d13ee` preservado. O atalho não foi atualizado neste lote. HADOUKEN `d4d389e` preservado em branch; Safety `27b8599` detached.

Checkout do App continua em `codex/launcher-classes`/`550b6d5`, com WIP alheio preservado. A publicação Drive usa índice separado sobre o SHA remoto da PR302, sem transportar as outras frentes ou trocar a branch do checkout. A cópia local do WIP tem manifesto e hashes verificados; proteção independente pendente, pois a página privada registrada não está acessível nesta sessão. Btrfs permaneceu em 6.741 corrupções acumuladas; isso não diagnostica a causa dos reinícios. Nenhum scrub/reparo/limpeza foi executado.

Próximos incrementos, em PRs distintas e dependentes: contas múltiplas com identidade estável e cofre isolado; pacotes por ativo com inventário/dependências; reunião em QA com conflitos explícitos e consumidores existentes. O proprietário fará consentimento e testes reais só depois da aprovação sintética conjunta. Nenhum dado privado enviado.

## Lote de lacunas — 06/11/2026

Um incremento por PR, todos sobre `master` `06b78eb`; nenhum merge feito.

| Passo | PR | Estado |
|---|---|---|
| 1 Composição e retorno | [295](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/295) | Docs/registro; retorno `57d13ee` conferido por hash, arranque não exercitado |
| 2 B3/Binance | [293](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/293) `35ad88c` | Recusa sem gravar, relógio por fonte, MN1 por calendário, filtro e erro estruturado; amostra pública real conferida; CI verde |
| 3 LAB-08 | [294](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/294) | Selo de conteúdo v1, leitura legada declarada, envelope do estudo; CI verde. LAB-01…10 pelo atalho, tema/DPI/monitor físico pendentes |
| 4 Launcher, Python, BD18a | [296](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/296) | Consultas locais sem ponte, Python ≥3.11, bancada BD18a (linear na seleção; sem otimização) |
| 5 T13.4-M1 | [297](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/297) | Cadeia real EURUSD H1 em SQLite/PostgreSQL, zero aprovados; simulação econômica bloqueada pelo contrato de custo |
| 6 E14 painel | [298](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/298) | Compila 0/0; bateria estática aprovada; backtest real 0 operações (painel só manual) |
| Achado | [299](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/299) | `config.ini` sem interpolação na CLI/conector |

Pendentes com dono: passo 7 (Drive) aguarda cliente OAuth do proprietário; DEMO ao vivo
exige autorização vigente e janela; netting exige conta netting; Windows físico exige host;
recuperação/capacidade/pendentes/risco agregado exigem escopo nos pins HADOUKEN/Safety;
M1 econômico exige custo conhecido antes de janela prospectiva e plano P04. Teste Windows
`test_backtest_scope_assets` expira de forma intermitente só no Windows CI (PR 299 `[ram]`, PR 298
`[3]`, esta só de docs): o worker espera o lock SQLite em `ledger.ensure_table` sem outra thread
ativa, indicando conexão ociosa com lock (journal rollback, timeout 30 s). Defeito do `master`, não
das PRs; corrigir exige reprodução no Windows e decisão sobre WAL/vida das conexões em ficha própria.

## Composição conferida — 06/11/2026

Conferência local deste host, sem instalar, trocar nem limpar releases.

| Elemento | Estado verificado |
|---|---|
| `master` remoto | `06b78eb` (merge da PR292 sobre `784a252`; acréscimo só no handoff) |
| Prévia do atalho | `~/.local/share/mt5x/app-development` → `releases/development-784a25210b04`, App `784a252`; 350 arquivos conferidos pelo `mt5x-release.json`, sem divergência |
| Atalhos | `~/.local/share/applications/` e área de trabalho: mesmo `mt5-extracao.desktop` (SHA-256 `bf617cb5…`), executável, `Exec` no `run.sh` da prévia |
| Retorno | `releases/development-57d13ee91620`, App `57d13ee` (ancestral do master); 341 arquivos conferidos, sem divergência; links de dados resolvem |
| PR293 dados públicos | `35ad88c` (correções B3/Binance sobre `e5fa91e`/`7bb510f`) |
| PR294 LAB-08 | `46cfc2e` (selo de conteúdo v1 e envelope do estudo) |
| Pins | HADOUKEN `d4d389ee`, Safety `27b8599`, detached |

Correções de registros anteriores: o recibo `state/development-preview.json`
citado abaixo não existe neste host; o recibo efetivo é o `mt5x-release.json`
de cada release. O alias `app-current` não existe; o destino de retorno é a
release `57d13ee` acima.

Retorno (troca atômica do link, com a janela do App fechada):

```bash
cd ~/.local/share/mt5x && ln -sfn releases/development-57d13ee91620 app-development.next && mv -T app-development.next app-development
```

Antes de usar, conferir de novo os hashes do manifesto da release. Limitação:
a release de retorno reutiliza `.venv`, base, configuração e logs atuais por
links; o arranque de `57d13ee` sobre esse estado não foi exercitado nesta
conferência. Para voltar à prévia, repetir o comando com
`releases/development-784a25210b04`.

## Backup no Google Drive — planejamento de 06/11/2026

A tarefa entra após a criação do cliente OAuth e o aceite do consentimento.
Cobre validação automática do destino privado, cópia íntegra das bases e ensaio
de restauração isolado. A recuperação atual continua pelo caminho privado
documentado abaixo. [T20.30-HOST1-DRIVE](docs/PLANO_DE_TRABALHO.md#t2030-host1-drive).

## Integração no master e prévia — 04/11/2026

Master `784a252` (merge commits, sem force push) recebeu, em ordem, as PRs
207–208, 210–280, 283–291, 281 (C) e 282 (D). A árvore do master é idêntica
a `e2388d0`, cujo CI na PR282 passou em Ubuntu (32 min), Windows (2h13),
PostgreSQL, instaladores Linux/Windows e lint; PR279 (`af40d91`) também
passou em todas as plataformas. PR209 (pilha bd08b) não faz parte da cadeia
e segue draft na base original. Correções de Windows integradas: corrida de
`IntegrityError` no registro de definições SQLite e orçamento de memória do
fixture LAB por RSS do processo.

Prévia `~/.local/share/mt5x/releases/development-784a25210b04` (350 arquivos
com hash) ligada em `app-development`; recibo em `state/development-preview.json`.
O atalho oficial abriu a janela principal sem traceback e iniciou a ponte MT5
via Wine. PostgreSQL de teste local: contêiner `mt5x-test-pg` (postgres:16,
127.0.0.1:5432, volume próprio, unless-stopped, 512 MB); use
`MT5X_TEST_PG_URL` igual ao do CI para a suíte PG (118 aprovados localmente).

Pendentes, fora deste host ou dependentes do usuário: Windows físico, MT5/Tester
nativo, ensaio DEMO, aceite M1 com dados elegíveis reais, gates operacionais
E14, percurso manual de Séries LAB-01…10 e compilação/bateria do painel
`hdk_hadouken_painel`.

## Revalidação de GUI, Séries e composição — 04/11/2026

App E14 `0993ae7bdf2e3354afcccf45dd2a22069a8b460c` continua na PR279
draft; HADOUKEN `d4d389ee0800d209bdba48ab36e392e6907949a4` na PR84
draft e Safety `27b85992af4b10f4cd7687f370142ac88f515375` detached.
O manifesto mantém estes pins e `setup_development.py --plan` terminou sem erro;
o App declarado ali é o bootstrap `65e621a`, distinto do checkout E14 e da prévia.
A prévia segue em `57d13ee`. No run 41747563787 da PR279, Ubuntu,
PostgreSQL, instaladores Linux/Windows e lint passaram; Windows pytest ainda
executava na consulta de 04/11 01:40 UTC. Não há aceite Windows CI final.

GUI C: branch local `codex/gui-dependencias-inicializacao` em `b0733d9`,
base E14 `0993ae7`; código `bf38e4a`. O launcher oficial foi repetido na
fonte rebatida `13f6b97` em Linux/Xvfb com configuração e SQLite isolados:
janela aberta e `quick_check` OK com dependências presentes; ausência simulada
de `pandas` mostrou diálogo/instalador oficial sem criar banco ou instalar.
Cinco testes focados passaram e doctor offline deu 16 OK/2 avisos/0 falhas.
Recibo ampliado na própria branch C; arquivos temporários removidos.

LAB-10 D: branch local `codex/lab-series-monitor-geometry` em `802e5de`,
base de código C `13f6b97`. O módulo de UI de Séries passou nesta fonte em
Linux/Xvfb: 29 aprovados, 1 ignorado, 16 avisos de layout, 127,52 s.
A asserção confere a geometria negativa solicitada e ampliar/restaurar;
posição física em monitor esquerdo e percurso pelo atalho seguem pendentes.
C e D não têm branch remota/PR por causa da guarda central compartilhada;
os SHAs locais estão no registro central. Não foi feito merge, atualização da
prévia, ensaio DEMO ou Windows físico. Próximo: conferir o resultado Windows
da PR279, resolver a guarda para publicar incrementos separados e continuar
os gates funcionais LAB/P01–P07 e operacionais E14 por fonte exata.

## E14 — largura tardia da comparação no Windows CI — 03/11/2026

Na PR279, App `19627c3c74dc2efafc60b2e417f49fa93d3d71f6`, run
`41731727371`: Linux, PostgreSQL, instaladores e lint passaram; Windows
terminou com 4566 aprovados, 193 ignorados, 1 falha e cobertura 77,74%.
A falha única foi `test_themes_scales_metrics_and_narrow_window_keep_actions_reachable`:
TkAgg deixou canvas de 580 px numa página de 558 px após mudança de escala.
Um teste novo reproduziu o alargamento tardio no host antes da correção; o
`ScrollableContent` agora reaplica a largura visível quando o conteúdo muda.
Depois, 24 testes de componentes e comparação passaram no Linux/Xvfb; doctor
offline de desenvolvimento 16 OK/2 avisos/0 falhas. Corretivo
`3312ea3d2fc2c318836cf420e84f343c75f98761` publicado na mesma PR279;
SHA local, remoto e da PR confirmados. CI run `41747461167` estava na fila
no marco de publicação; Windows físico e percurso
pelo launcher desta tela seguem aceites separados. Nenhum contrato financeiro,
pin ou dado privado mudou. Próximo: conferir o CI e continuar
os gates E14; não declarar o check Windows aprovado antes do novo run.

## Composição e limites atuais — 03/11/2026

Raiz única `/home/misael/Projects/MT5X`: App `repos/app`, HADOUKEN
`repos/hadouken`, Safety `repos/safety`. App E14 `0993ae7`/PR279 draft,
HADOUKEN `d4d389e`/PR84 draft (fonte S09 2.13 `51358a1`) e Safety
`27b8599` detached/PR11 formam a composição de desenvolvimento; o preparo
oficial aceita os pins. A prévia instalada ainda é App `57d13ee` e não
contém o perfil 2.13 ou P01–P07. Os SHAs completos, PRs e próximos passos
estão no registro central `../.workspace/projects.json` e no manifesto do
App; textos datados abaixo descrevem fontes anteriores.

E14 S09 2.13: MetaEditor/Wine 0 erros e 0 avisos, 59 arquivos iguais; Tester
Linux/Wine e replay visual do mesmo pacote sintético conciliaram oito entradas,
sete ações, quatro alterações de TP pós-fill e a caixa. Recibo público em
`docs/evidencias/E14_POSTFILL_213_TESTER_REPLAY_20261103.json`; recibos privados
protegidos na página pessoal existente, ZIP SHA256
`2a8a6dff51b953a2bf8fe6f8c93904cad086d85ff0000163d004d61c96547080`.
No SHA anterior `19627c3`, Windows CI falhou só no gráfico estreito, com 4566
testes aprovados; Linux e demais jobs passaram. O corretivo está em `3312ea3`,
e os checks da ponta documental `0993ae7` tinham PostgreSQL, instaladores e
lint aprovados; Ubuntu/Windows ainda executavam na consulta de 04/11 00:50 UTC.
Não houve DEMO ao vivo,
netting, Windows físico nem liberação do perfil operacional.

P00–P07 foram recompostos em branches locais separadas sobre E14, com LAB-03
local dependente de P07. Os testes de padrões/Séries na composição LAB-03
terminaram com 79 aprovados, três ignorados e uma falha já reproduzida na
asserção de monitor negativo herdada da base E14 neste host XWayland de um
monitor. A branch isolada LAB-10 `8822d25`, baseada em GUI C `13f6b97`,
corrige essa asserção; a fonte anterior passou 29 testes de Séries/1 ignorado,
e a fonte rebatida passou seis testes focados; falta integrar os aceites entre
PRs. O fluxo P01–P07 permanece exploratório: CI, fonte real elegível,
PostgreSQL da campanha, Windows físico e aprovação M1 seguem abertos.
Na cadeia rebatida sobre `0993ae7`, 51 testes focados de P01–P07/GUI de
padrões passaram em Linux/Xvfb. `MT5X_TEST_PG_URL` não está configurada neste
host; os dois testes PostgreSQL próprios desta cadeia não foram executados.
O inspetor oficial de checkouts confirmou, sem instalar ou atualizar a prévia,
App local `7544f6b` e pins HADOUKEN/Safety; doctor offline 16 OK/2 avisos/0 falhas.
O guarda central de push ainda impede publicar as branches de revisão;
não o contornar. Próximo: publicar esta cadeia após a decisão da guarda, seguir
recuperação/capacidade/pendentes E14 e aguardar a decisão específica do guarda
para PRs C/D/P00–P07/LAB.

## LAB-03 — regressão de alinhamento em revisão — 03/11/2026

Branch local `codex/lab03-horarios-alinhamento` sobre P07 local. A correção da
matriz e dos cabeçalhos já está na fonte desde `22c09f4`, refinada por
`4ac3a46`; esta revisão acrescenta um teste da matriz real de Horários que
compara as quatro colunas após rolagem horizontal/vertical e três larguras.
Três casos Tk dirigidos passaram em Xvfb no host. O defeito histórico da imagem
privada não foi reproduzido nesta fonte; DPI e monitor físico, percurso pelo
atalho com eventos elegíveis, Windows físico e PR/CI ainda exigem seus gates.
Nenhuma imagem privada foi copiada. Próximo: seguir os LAB restantes e E14
operacional após reconciliar a publicação da cadeia local.

## T13.4-P07 — consumidores de avaliações em revisão — 03/11/2026

Branch local `codex/t134-p07-consumidores-m1` sobre P06; o SHA `e538521`
abaixo identifica a composição anterior ao rebase de 03/11.
`analytics.pattern_queries` consulta os recibos P06 já gravados no ledger,
com namespace da base e corte de disponibilidade, e expõe inspeção dos fatos
de sinal/desfecho no `feature_store`. SDK, CLI `laboratory pattern-evaluations`
e `pattern-evaluation`, relatório HTML, guia Padrões do registro de estudos no
Laboratório e Dashboard usam a mesma leitura. O Dashboard apresenta os recibos
como exploratórios, ocultos por padrão, sem inventar lift, confiança ou
aprovação; ausência de resultado é explícita. A consulta não executa pesquisa
nem ordens. Ensaios sintéticos incluem CLI=SDK, HTML, reinício, corte anterior
ao registro e o mesmo símbolo em duas bases. Na composição local anterior com
entrada GUI e teste de monitor à esquerda, 58 testes dirigidos passaram, um foi
ignorado e o desenho emitiu 17 avisos de layout. `./run.sh --offline --config`
abriu a GUI em Xvfb isolado com SQLite de QA, e o menu Exibir → Registro de
estudos → Padrões mostrou o estado vazio sem fabricar avaliação. Com outro
SQLite sintético, a mesma rota exibiu o recibo persistido `reversal` em
`SYN / M1`, treino, zero eventos e estado exploratório, após reinício da base.
Os arquivos temporários de QA foram removidos. Amostra real elegível, CI/PR,
PostgreSQL e Windows físico permanecem separados.
Publicação aguarda ajuste específico do guarda central compartilhado para
branches de revisão; não contornar o hook. A prévia instalada segue em
`57d13ee`, sem estas mudanças. Artefatos privados permanecem no manifesto
existente, sem cópia. Próximo: completar gates de P07, revisar documentação
central e retomar os gaps operacionais E14 da fonte reconciliada.

## T13.4-P06 — fluxo inicial de padrões em revisão — 03/11/2026

Branch local `codex/t134-p06-fluxo-quatro-familias` sobre P05, que por sua vez
se apoia em P04. `analytics.pattern_families` compila reversão, continuação por
horário, gap e rompimento/falha na gramática tipada P02, com parâmetros
explícitos. `analytics.pattern_flow` lê somente snapshots congelados, preserva
namespace/relógio/sessão, registra acesso por papel antes de ler resultados,
avalia eventos e desfechos, chama baseline P04 e simulação P05 quando os planos
congelados permitem. Sinais e desfechos vão para definições distintas do
`feature_store`; o recibo fica no ledger de experimentos na mesma transação.
O preset em `presets.research` inclui fonte, definição, planos, resultado e
hashes de implementação, sem equivalência `.set` nem ativação. Replay com
fechamento presumido e sessão UTC sintética é marcado como tal, sem aceite
prospectivo. Ensaios sintéticos cobrem quatro famílias, duas bases de mesmo
símbolo, reinício, nenhum aprovado, veto a vazamento entre folds e rollback.
O conjunto P01–P06/datasets/experimentos teve 89 testes aprovados no Linux;
os seis testes diretos do fluxo foram repetidos após a contagem de negativos.
O teste transacional PostgreSQL do fluxo foi ignorado sem URL local; a CI da PR
deverá executá-lo. Nenhum dado real foi usado como fixture.
SDK e CLI `experiments pattern-create`, `pattern-request`, `pattern-evaluate` e
`pattern-status` expõem o fluxo no serviço existente, com modo sintético de
sessão apenas quando escolhido explicitamente para QA. P07 deve ligar os
consumidores de consulta à avaliação persistida.

## T13.4-P05 — adaptador econômico em revisão — 03/11/2026

Branch local `codex/t134-p05-simulacao-economica` sobre P04 para revisão
sequencial. `strategies.pattern_adapter` transforma eventos P02 verificados por
identidade/hash em sinais para `run_backtest`: entrada a mercado na próxima
abertura após disponibilidade, stop/alvo/prazo tipados e uma posição por ativo.
O modelo de custos exige origem, vigência, base bid, spread declarado, comissão
por lado, slippage, moeda e swap explícitos. O motor atual calcula lote, fill,
PnL, saldo e patrimônio; o adaptador relata evento sem execução e barra ambígua
com stop-first. Ordens limitadas, concorrência e moeda sem vínculo são recusadas.
O cenário dos testes é sintético/hipotético; nenhum resultado tem aprovação
econômica ou equivalência `.set`. Passaram 30 testes de adaptador, backtester e
eventos P02 no Linux. P06 deve persistir e consumir avaliações.

## T13.4-P04 — validação temporal em revisão — 03/11/2026

Branch local `codex/t134-p04-validacao-temporal` sobre P03. O plano de folds
aplica as mesmas janelas UTC a todas as fontes, recusa característica futura,
purga desfechos que cruzam o corte e embarga em tempo real. O baseline congela
estratos com metadados do treino; só compara pares de evento/controle da mesma
fonte, sessão e estrato após o corte de ajuste. Bootstrap por blocos de sessões
produz IC e uma permutação pareada explícita produz p diagnóstico sob hipótese
de intercambialidade; não implica causalidade. O contexto da campanha conta a
grade inteira, endpoints e definições extras, inclusive descartes, antes de
aplicar Bonferroni. Resolução de p insuficiente impede o status OK. P06 ainda
deve conectar snapshots, eventos e resultados persistidos. CI, dados reais,
Windows físico e qualificação final da reserva pendentes.

## T13.4-P03 — despacho por tipo de protocolo — 03/11/2026

Na branch local `codex/t134-p03-campanha-reserva`, a validação conjunta do
Laboratório encontrou uma regressão introduzida ao incluir campanhas de padrões
no guard do executor BASE: a recusa correta de um protocolo de associações
perdeu sua mensagem específica. O despacho agora conserva a orientação de
`laboratory protocol-evaluate` para associações e explica separadamente o fluxo
de campanha para padrões. Nenhum dos dois tipos chega ao executor de estratégia.
O caso falho e o novo caso de campanha passaram isolados (2/2); as suítes de
campanha e protocolo passaram juntas (13/13). Testes com fixtures sintéticas;
sem CI/PR da branch enquanto o guarda central bloquear novas referências.
Próximo: incorporar o corretivo nas branches P04–P07 e Laboratório, repetir a
seleção que falhou e publicar cada incremento quando houver destino autorizado.

## T13.4-P03 — campanha e reserva em revisão — 03/11/2026

Branch local `codex/t134-p03-campanha-reserva` sobre P02 local. O registro de
experimentos existente recebe protocolo versionado com roster de snapshots,
cortes globais UTC, endpoints, grade, critérios, semente e orçamento. Tentativas
idempotentes disputam o teto agregado sob o lock do store; recusas e descartes
ficam registradas. Acesso por papel e fonte é gravado antes de entregar uma
reserva; proposta automática posterior à exposição é recusada. Exposição
externa permanece desconhecida e a reserva não é qualificada como final.
Passaram 57 testes de campanha e regressão de experimentos no Linux; um teste
PostgreSQL foi ignorado porque a URL de teste local não está configurada.
P04 deve validar cortes e purga; P06 deve persistir avaliações/eventos. Sem
CI, PR, launcher, MT5 ou aceite real desta branch enquanto o guarda central
impedir publicação separada.

## T13.4-P02 — regras e eventos em revisão — 03/11/2026

Branch `codex/t134-p02-regras-eventos` sobre P01 local. `analytics.hypotheses`
valida AST JSON, unidade, fração decimal, lado, horizonte e hash imutável;
`analytics.pattern_engine` avalia barras fechadas por disponibilidade em lote
e prefixo, com estado reiniciável e fonte/símbolo/TF vinculados. Revisões
posteriores geram identidade própria sem reescrever eventos anteriores.
Desfechos futuros ficam separados, censurados até a recepção e sem alegar
lucro líquido; lacuna sem calendário comprovado também censura. O adaptador
`SignalSpec` aceita apenas o subconjunto de limiar de `close`, sem equivalência
MQL5/.set. Nove testes sintéticos passaram. Persistência, campanha, reserva,
simulação econômica, consumidores e aceite real pertencem às próximas fichas.
Branch local sem push/PR enquanto o guarda central impedir publicação separada.

## T13.4-P01 — catálogo e elegibilidade em revisão — 03/11/2026

Branch `codex/t134-p01-catalogo-elegibilidade`, baseada no P00 reconciliado.
`analytics.pattern_catalog` usa `data_inventory`, namespace de `datasets`,
qualidade/capacidades existentes e evidências de coleta/sessão do snapshot.
SDK e CLI `laboratory pattern-sources`, `pattern-qualify` e `pattern-freeze`
expõem a mesma seleção; captura limitada a 20.000 barras pelo store atual.
Duas bases sintéticas com o mesmo símbolo mantêm namespaces e snapshots
distintos. O leitor de Dados também aceita horário escalar em base com zona;
o snapshot dessa base continua bloqueado pelo contrato de offset fixo.
Passaram 57 testes de Dados/qualificação e 17 de relógio/P01 no Linux local.
Pesquisa, eventos,
simulação, dados reais, CI e launcher de GUI ainda não foram aceitos. O push
da branch documental e das branches de revisão aguarda resolução explícita
do guarda central compartilhado; não contornar o hook.

## E14 — auditoria pós-fill 2.13 e resgate privado — 03/11/2026

App `codex/e14-protecao-comum`, base `codex/f04-vinculo-presets`, PR279
draft, código e recibo `f36395334e431517752c5872c221aa6110228d49`
confirmados no remoto; HADOUKEN `d4d389ee0800d209bdba48ab36e392e6907949a4`/PR84
(fonte S09 2.13 `51358a16c1685cd79f4ffec0c8da464b72e10f5a`),
Safety `27b8599` detached/PR11. O manifesto fixa estas dependências para
desenvolvimento; a prévia instalada continua App `57d13ee`. O App concilia
TP em R pós-fill com a intenção e o efeito do journal; o perfil 2.13/15 exige
fingerprint e 59 inputs exatos. Os 109 testes dirigidos passaram. Compilação
MetaEditor/Wine: 0 erros, 0 avisos, 59 arquivos iguais à fonte. Backtest e
replay visual Tester do mesmo pacote sintético EURUSD/M5 14–16/10 terminaram
conciliados: oito entradas, sete ações, quatro alterações de TP pós-fill;
`reference_match=true` no replay. Prova pública em
`docs/evidencias/E14_POSTFILL_213_TESTER_REPLAY_20261103.json`.

O banco QA e os recibos privados estão na página pessoal de recuperação
existente, complemento de 50 arquivos/seis partes, ZIP SHA256
`2a8a6dff51b953a2bf8fe6f8c93904cad086d85ff0000163d004d61c96547080`.
Os anexos foram relidos da Library por hash e o verificador oficial extraiu
todos os arquivos; `qa.sqlite` passou `quick_check`. Não usar este ensaio para
autorizar DEMO ao vivo, netting, Windows físico ou perfil operacional. Próximo:
conferir o CI do SHA publicado e continuar os gates
E14 de recuperação/capacidade, pendentes, orçamento por ativo/grupo/plano,
gradientes, probabilístico e SA-01. A cadeia documental/LAB/T13.4 segue local
até a guarda central permitir a publicação da branch correspondente.

## E14 — integração de desenvolvimento do S09 2.12 — 03/11/2026

App `codex/e14-protecao-comum`, base `codex/f04-vinculo-presets`, publicou
`1310be9b80fa67d7a42f6a24661c69d2ee76529e` na PR279 draft; SHA local,
remoto e da PR conferidos. O manifesto de desenvolvimento agora fixa HADOUKEN
`8a11fcfeed9292b54432b4b5c6d5a02bcbb50807`/PR84, fonte S09 2.12
`f99ebde132c10bb078e78796e7ef048082ba07cc`,
e Safety `27b8599`/PR11. O perfil 2.11 anterior conserva fingerprint/revisão
próprios. `postfill_r_v1` é modo tipado distinto de R planejado e `fixed_r`
Python; gera InpTargetMode=3 e usa pacote vinculado somente no Tester DEMO.
O preparador oficial inspecionou os três checkouts e aceitou os pins; o modo
`--plan` passou, sem instalar nem alterar a prévia `57d13ee`. Na fonte App,
44 testes de perfil/UI e 97 de composição/execução de preset passaram;
o formulário Manual exportou/reabriu o modo com recibo sintético. HADOUKEN
2.12 passou duas checagens de fonte e compilou no MetaEditor/Wine com 0 erros,
0 avisos e 59 arquivos idênticos ao plano da fonte exata; o recibo público está
na PR84. CI App da nova ponta iniciou. Tester/replay por fonte 2.12,
percurso real do atalho, Windows físico, netting
e DEMO permanecem gates separados; nenhum perfil operacional foi liberado.
Os recibos privados anteriores seguem no manifesto e resgate independente;
nenhum novo recibo privado indispensável foi retido nesta etapa. Próximo:
ensaiar 2.12 no Tester sob posse/limites do runtime, conferir Windows CI e
seguir os itens E14 ainda abertos. A cadeia LAB/T13.4 local já incorporou esta
base em ordem, sem publicação por causa da guarda compartilhada.

## E14 — segunda correção de layout Windows em revisão — 03/11/2026

Na PR279, App `d68e125`, o rerun Windows 41668845297/attempt 2 concluiu com
4552 testes aprovados, 193 ignorados e 2 falhas (cobertura 77,74%). Retenção
do lease e os dois casos UTF-8 passaram; restaram busca/ações do catálogo e
gráfico da comparação em janela estreita. O teste de catálogo observou botão
pedido com 331 px, alocado em 294; a comparação observou canvas 672 px em
viewport de 558. O primeiro attempt desse SHA parou antes, no limite de 60 s
de um teste de lote com 6 ativos; não comprovou estas duas falhas.

Este incremento deixa ações do catálogo em linhas de largura integral quando
estreito e limita os canvases TkAgg à largura da página ao mudar DPI/tema.
Uma regressão sintética reproduziu recorte de botão no Linux antes da correção;
depois passaram 10 testes dirigidos de catálogo e comparação em Xvfb. O check
Windows do SHA sucessor, o launcher da fonte nova e Windows físico ainda são
gates próprios. HADOUKEN `2809938`, Safety `27b8599` e prévia instalada
`57d13ee` não mudaram. Próximo: publicar na mesma PR279, confirmar SHA remoto,
checks e rebater a cadeia local dependente sem perder os commits.

## LAB-10 — geometria de monitor em QA — 03/11/2026

Branch `codex/lab-series-monitor-geometry`, base local
`codex/gui-dependencias-inicializacao` (dependente da PR de GUI). A simulação de
monitor à esquerda pede `1024x800+-1024+-50`; neste host de um monitor o
gerenciador de janelas entrega `(256, 37)`. O teste passou a conferir o pedido
e o ciclo ampliar/restaurar, sem exigir posição física impossível. Antes falhou
1/1; depois passaram 29 testes de Séries, 1 ignorado e 16 avisos de layout
na fonte anterior `8b110aa`. A branch foi rebatida sobre GUI `13f6b97`;
o mesmo ajuste está em `d023c2f`, com seis testes GUI/monitor focados aprovados
na composição rebatida.
Na ponta local `8822d253`, o módulo completo de UI de Séries passou em
Linux/Xvfb: 29 aprovados, 1 ignorado e 16 avisos de layout em 127,52 s.
Sem mudança de runtime. Monitor físico negativo, atalho central e os demais
percursos LAB-01…10 mantêm os aceites próprios do roadmap; esta correção não
os fecha. Branch/PR remotos dependem da guarda central de revisão.

## Inicialização da GUI — corretivo local em revisão — 03/11/2026

Branch `codex/gui-dependencias-inicializacao`, rebatida sobre App
`0993ae7`/PR279; código da GUI em `bf38e4a` local. O recibo inicial do launcher
pertence à fonte equivalente `baa24c4`; depois do rebase, cinco testes da GUI
e um de monitor passaram na composição dependente.
O chamador agora consome `(sucesso, faltantes)`; ausência mostra os nomes e o
instalador oficial (`./install.sh` ou `install.bat`) e encerra a abertura.
O pré-voo do script ocorre antes dos imports de runtime, com o catálogo de
módulos do doctor; não instala pacotes automaticamente. 23 testes de entrada,
offline e doctor passaram no host. `./run.sh --offline --help` terminou com 0;
o parâmetro `--config` com `--offline` permite apontar uma sessão de QA para
configuração/base explícitas sem tocar a base principal. Cinco testes diretos
passaram. Pelo launcher `./run.sh --offline --config` em Xvfb próprio, a
dependência presente abriu a janela e criou apenas o SQLite de QA (`quick_check`
OK). Com `pandas` ausente por simulação no processo, o mesmo launcher mostrou
o diálogo e a ação `./install.sh`, sem criar banco ou iniciar instalação.
O percurso pelo mesmo launcher foi repetido na fonte rebatida `13f6b975`:
janela e SQLite isolado com `quick_check` OK; na simulação de `pandas`
ausente, diálogo e orientação ao instalador sem banco ou instalação.
Cinco testes diretos passaram; doctor offline 16 OK/2 avisos/0 falhas.
O diretório temporário de QA foi removido.
[Recibo público](docs/evidencias/GUI_DEPENDENCY_STARTUP_20261103.json).
Windows físico e CI de SHA publicado permanecem pendentes.
Doctor offline de desenvolvimento: 16 OK, 2 avisos (pandas-ta opcional e integração
nativa não sondada), 0 falhas.
Base privada, ponte, terminal, negociação e referências aceitas não foram usados
como fixture. Publicação desta branch depende da guarda central de PRs separadas.

## E14 — saneamento do Windows CI em revisão — 03/11/2026

PR279 segue aberta/draft na branch `codex/e14-protecao-comum`. No SHA
`a74f53977bd9e67250b072df43a17d0c3ee06a12`, Linux, PostgreSQL, instaladores
e lint passaram; Windows terminou com 5 falhas, 4549 testes aprovados e 193
ignorados. Este corretivo trata a leitura da trava pelo teste durante o lease
Windows, duas leituras de UTF-8 sem codificação explícita e dois controles Tk
mais largos que seus viewports. No host, 64 testes pertinentes passaram e o
doctor offline teve 16 OK/2 avisos/0 falhas. A suíte integral foi interrompida
com código 143 após 6%; não há aceite integral local. O CI do novo SHA tem
recibo distinto; Windows físico, MT5 e negociação não são certificados pelo CI.
Composição operacional: App na PR279, HADOUKEN `28099386`/PR84 e Safety
`27b85992` detached/PR11. O manifesto desta branch fixa esses componentes.
A prévia instalada ainda aponta para App `57d13ee`; atualizar somente após
publicação e verificação pelo launcher, preservando as sessões abertas.

## Estudo de descoberta de padrões —02/11/2026

Pedido atual: primeiro estudo/plano/roadmap para outro agente implementar.
[Roteiro T13.4-P00–P11](docs/analytics/DESCOBERTA_PADROES.md), com inventário
confirmado, contratos, método, testes, marcos e repasse executável na seção11.
P00 é somente documental; M1 exige P01–P07 e quatro famílias ponta a ponta.
Próxima ficha dessa frente: P01 (catálogo/elegibilidade), sem criar motor/store/
fila paralelos, sem ordens e sem reutilizar autorização DEMO anterior.
“Infut/5h25” aguarda definição; não bloqueia as famílias genéricas.

Base de origem Appc956305, PR279 operacional ainda aberta/draft; fonte e SHA
documental atuais ficam no registro central Projects/.workspace. Este estudo
preserva a frente operacional abaixo e os componentes existentes, sem repins
ou modificação de runtime/prévia. Não interpretar publicação do plano como
entrega de inteligência implementada ou aceite econômico.

## Execução do pilar operacional —01/11/2026

Pedido atual do proprietário inclui App, HADOUKEN e Safety, de ponta a ponta.
Matriz e sequência canônicas: [inventário, seção9](docs/propostas/INVENTARIO_F19.md#9-pilar-operacional--inspeção-da-composição-em-01112026).
Recorte inicial T20.8 corrige prechecks do executor, conexão incerta e proteção
monotônica. HADOUKEN f7e4cffc/PR76, Safety candidata1.11 833edc5/PR10; App
reconcilia pins de desenvolvimento sem alterar referências aceitas. Não
atualizar fingerprints aprovados por conveniência nem afirmar reserva agregada.
E-13/T19.57: HADOUKENdc90af73/PR77 (código86228777) corrige R/volume originais nos dois hosts,
identidade integral, recuperação de intenção e BE sem afrouxar trailing.142 testes
simulados e12 compilações0/0. Repetição final do Tester concluída pela fonte QA
imutável (53 fontes/56 vínculos do catálogo iguais por hash):13 posições/39deals/9 e4 etapas confirmadas,
44inputs e caixa reconciliados. Tick gerado deM1, sem aceite ao vivo/Windows.
Safety833edc5 não muda neste recorte. Próximo: F-04/05/10/11/12 no guardião já
existente no histórico, sem criar outro motor; seguir os demais marcos, sem Dashboard.
F04.1: H1f6dca03/PR78 (núcleoffb84c66), Safety27b8599/PR11 candidata1.12.
148testes H/checker170,16compilações0/0 e Smoke0/0. Estado individual completo/
truncado validado no Tester; S09 repetiu13posições/39deals/9+4etapas e44inputs.
App passa a tratar carteira desconhecida como indisponível e usa cálculo nativo
em moeda da conta. Reserva segue protótipo sóTesterDEMO fora da distribuição,
ainda sem journal/consumo/ligação ao executor; esse é o próximo trabalho.
Código App7c51bc4/PR272:182testes dirigidos e doctor16OK/2avisos/0falhas.
Prévia oficial com338arquivos conferidos; launcher mostra dados desconhecidos
como indefinido. Ponte de leitura atualizada com posse comprovada, sem terminal/
conta carregados; cálculo nativo de conta ainda não qualificado. QA F04 protegida
na página privada existente, sequência21:137arquivos/seis bancos restaurados e
conferidos, cinco anexos novos,304blocos anteriores preservados e quatro resumos
atualizados. Manifesto/recibo em state/qa-tmp/operational-pillar/protecao/f04.
O pilar permanece incompleto. Testes de plataforma simulada, compilação, Tester,
DEMO e Windows físico mantêm evidências separadas.

F04.2: H6c960fde/PR79 persiste reservas no módulo existente; journal Common é
autoritativo, GVs são projeção recuperável. SENDING/incerteza e IDs64bits não
somem por tempo/restart; truncamento/temporário interrompido bloqueiam envio.
149testes H/checker170, compilação0/0 e sete checks de I/O no Tester sem ordens.
Safety27b8599 continua detached/candidata1.12: esse protótipo não é distribuído.
Ainda faltam vínculo por intenção/ator/sessão/preset, consumo/reconciliação e
integração ao executor, antes de liberar orçamento agregado.

Corretivo CI da ponte: whitelist permite apenas order_calc_profit/margin entre
os nomes order_*. Testes recusam envio/check/cálculo desconhecido via RPC e pela
CLI;71casos pertinentes passaram, três CLI revalidados no código final.16OK/
2avisos/0falhas no doctor offline. O CI integral do pai5e1fe1c falhou nesses dois
testes desatualizados (4140passaram/459não executados); não inferir CI verde.

F04.3: Hd8567f26/PR80 vincula pedido final à intenção/contexto por SHA256 e
implementa conciliação da reserva por ordem/deals/posição/snapshot.151testes H,
checker170; compilação0/0 e oito checks de I/O no Tester, incluindo inicialização
durável da política. Fills/conciliação só em plataforma simulada; nenhuma ordem
nessa QA. Safety27b8599 continua inalterada/detached e não distribui o protótipo.
Próximo: conectar ao SafeExecutor e ao manifesto App, validar percurso nativo;
depois limites por ativo/grupo/plano, reduções e demais capacidades obrigatórias.

F04.4: H178ac146/PR81 liga entradas do SafeExecutor ao journal, opt-in sóTesterDEMO.
Intenção tem uma tentativa, SENDING é durável antes do envio e resposta observada
é distinta da gravação confirmada. Consumidor sem vínculo recusa política/journal
em vigor; liderança mantém arquivo Common exclusivo, sem roubo por pulso vencido.
154testes H/checker171/17compilações0/0. Tester na fontebb4760a5d7e8e4c6 confirmou
entrada/resposta/conciliação, duplicata recusada com posição aberta e encerrada,
uma saída e zero remanescentes; caixa−0,66USD reconciliado. Nove fontes iguais por
hash. Ordem histórica a mercado pode trazer preço0; o preço real continua exigido
nos deals. Hashes de vínculo são fixtures explícitas, sem atestação de preset App.
Safety27b8599 continua detached/candidata1.12 e não distribui o protótipo.
Proteção na página privada existente, sequência24:177arquivos/oito bancos de QA,
seis partes/oito anexos; todos os hashes/linhas SQLite conferidos e325blocos
anteriores preservados. Inclui dois timeouts e três diagnósticos inválidos.
Próximo: manifesto App e journal geral de gestão, depois ativo/grupo/plano,
pendentes/consumidores e capacidades restantes. Sem aceite ao vivo/restart/Windows.

F04.5: o SafeExecutor persiste intenções de close/parcial, proteção monotônica e
cancelamento no mesmo journal/contador/trava das entradas. Posse exige contexto e
ordem/deals/identificador estável; magic isolado não concede gestão. SENDING vem
antes do OrderSend existente, resposta bruta/request_id e efeito são distintos;
incerteza e parcial de broker não se repetem automaticamente. Sem pulso bloqueia
entrada e mantém redução comprovada. Envelope3 conserva entradas/tombstones.
157 testes Tools, checker 171 e Tester na fonte e7f7e708e66cd228: uma entrada,
parcial voluntária, avanço de SL preservando TP, fechamento do saldo e seis tipos
de pendência colocados/cancelados. Journal com 1084 linhas: sete entradas e nove ações
encerradas; R inicial de 10 USD/volume de 0,02 lote preservados e caixa −0,94 USD reconciliado.
Não é BE por gatilho financeiro nem fill parcial real do broker. Vínculo ainda
usa hashes de fixture; manifesto App e consumidores continuam em implementação.
Safety 27b8599 permanece detached/candidata 1.12, sem distribuir o protótipo.
O leitor App reconhece motivos dos logs novos sem equiparar volume executado a
posição zerada. SA-REF-01/runtime 33c2fa1 e sua autorização separada preservados.
Sem PLAY, restart real, netting ou Windows físico nesta frente.
HADOUKEN 7291d680/PR82; 17 consumidores compilados sem erros ou avisos.
App: 41 testes de paridade/reconciliação e 67 de SDK/API/docs/catálogo/Safety
aprovados; doctor offline com 16 OK, 2 avisos e nenhuma falha.


### F04.6 — pacote vinculado, S09 v2 e qualificação pendente

HADOUKEN ad880c13/PR83 acrescenta vínculo ao mesmo guardião/adaptador e exige
pacote no S09 v2.00. O App compara grafo de fontes, wrapper, binário e arquivo de
vínculo antes do lançamento. O MQL confere pacote/revisão, ator/estratégia,
sessão, ativo/timeframe e todos os 44 inputs públicos realmente lidos; o recibo
com contexto é gravado e relido. O hash do binário não é input autorreferente.
A serialização numérica preserva o double exato, sem depender de locale ou de
arredondamento decimal. UI, API e Budget usam o mesmo pacote/compilador/Session,
com atores declarados por fluxo, sem conceder mandato por nome.

O wrapper com 44 inputs e 17 consumidores compilaram sem erros ou avisos no core 484b38e4/fonte
45442cd0bcd0e67a.158 testes H e 128 casos App pertinentes passaram. O pacote Manual
real exportado confirmou ack nativo, mas excedeu 180s sem run final; não aprovado.
Recursos foram liberados; diário parcial/ack/fontes preservados. A manutenção
repetida do journal foi corrigida e158 testes offline passaram em 77,446s; essa
alteração ainda requer nova compilação/Tester. Nenhuma evidência anterior foi
promovida para a fonte alterada. Perfil App fica bloqueado enquanto a revisão
nativa não estiver qualificada. Reserva nativa da tarefa DEMO separada até 15hUTC;
continuar o trabalho independente, sem tomar seu runtime 33c2fa1 ou autorização.

Próximo: medir e qualificar o ciclo completo exportação→inputs→decisão→pedido→
posição→parcial/BE→saída→reconciliação; depois políticas comuns/SA-01 e demais
capacidades obrigatórias. O pilar continua em implementação. Safety 27b8599,
referências aceitas e SA-REF-01 permanecem preservadas.


Conferência isolada do material preservado: os44 inputs carregados e o ack do
contexto correspondem ao pacote real. Isso não aprova o ciclo interrompido.
App:254 testes pertinentes passaram em29,72s; doctor16OK/2avisos/0falhas.
A tarefa DEMO já agendada abaixo continua com runtime33c2fa1 e sua biblioteca
SA-REF-01 congelada, sem regeneração/repin para o corretivo. Nenhuma autorização
de negociação é transferida para a nova composição.

O código e o diagnóstico estão no GitHub. A cópia privada fica na conta ChatGPT
do proprietário, na página **MT5X Continuidade e recuperação em novo host 24 outubro 2026**.
**O host antigo e o SDCard não estarão disponíveis.** Não depender de caminhos
locais, anexos não enviados ou do histórico de uma sessão aberta no computador antigo.

Leia o [roteiro completo](docs/repasse/NOVO_HOST_20261024.md) e entregue ao agente
o [prompt de retomada](docs/repasse/PROMPT_NOVO_HOST_20261024.md).

## Encerramento por sessão e replay visual —02/11/2026

App57d13ee (PR279): `session.pause`/`resume`/`close_out` liquidam por ticket só a
exposição da sessão, sem UI; qualificado no Tester, DEMO não exercitada. Replay
pela UI segue não repetível em180s (179,3 aprovado/181,6 interrompido); perfil
S09 2.11 continua bloqueado. [Detalhes](docs/continuidade.md#encerramento-por-sessão-sem-interface-e-orçamento-do-replay-visual-02112026).

**03/11/2026:** HADOUKEN `2809938` limita os quadros do painel nativo no replay
visual (teste visual 85–99 s → 49,7 s). Jornada real dos botões 3/3 dentro de
180 s (replay 122,2/123,6/139,7 s, referência e ciclo funcional; backtest
87,0/73,7/83,5 s). Perfil S09 2.11 qualificado: fingerprint `527e696bdcc9f147`,
revisão `2809938`. [Detalhes](docs/continuidade.md#painel-nativo-com-quadro-limitado-e-perfil-s09-211-qualificado-03112026).

## Preparação operacional DEMO em01/11/2026

[Proposta e encerramento por posse](docs/sdk/demo_operacional.md#sa-ref-01--proposta-operacional-para-2-novembro-2026-aguarda-autorização):
02/11,10–12h America/Sao_Paulo, conta DEMO final9678, EURUSD M5/magic540121.
Autorização explícita recebida em01/11, limitada a02/11/2026,10–12h America/Sao_Paulo,
entradas antes de11h45 e referência runtime33c2fa1. Revalidar tudo antes do PLAY;
não pedir a mesma aprovação novamente. Mercado aberto não amplia essa autorização.
Retomada única nesta tarefa agendada para09h50 pelo Codex; máquina e app precisam
permanecer disponíveis. Autorização/recibos privados no estado da composição,
referenciados pelo registro central; nenhuma negociação iniciada ao registrar.
Auditor de sessão: `scripts/demo_session_audit.py`; `sa_ref_reconcile.py` mantém
somente o contrato histórico de outubro. A deduplicação preserva repetições por
origem. STOP não liquida: só encerrar supervisão após conferir pendências,
posições próprias e histórico, preservando terceiros. Pacote histórico e
complemento já enviados e vinculados na página privada existente, sequência15;
69 hashes/links conferidos,196 blocos anteriores preservados. Fonte/commit,
recibos e prévia atuais no registro central. SA-01 multi-TF é a próxima frente;
não retomar automaticamente o backlog Dashboard no prompt antigo abaixo.

## Ponta de continuidade em01/11/2026

Retome `codex/ponte-previa-coesa` (harmonização da ponte/launcher), sobre
DASH-05/PR267 (`codex/dashboard-volume`, aa969ea), DASH-04/PR266 (d19cd8c),
DASH-03/PR265, DASH-02/PR264, DASH-01/PR263, snapshots/PR262, planejamento DASH-00/PR261, comparação/PR260, retenção/PR259, vários ativos/PR258, Todos/PR257, ordenação/PR256, portabilidade/PR255, conexão/PR254, Expert/PR253 e persistência/PR252, inspetor/PR251, financeiro/PR250, protocolo/PR249, BD06-MACRO/PR248, MEN-PT/PR247, MEN-SOCIAL/PR246 e o corretivo
Windows da PR245. FRED/BCB seguem captura local e fases datadas retrospectivas.
Fontes sociais aguardam autorização real; português tem
versão própria e avaliação controlada, sem selo humano/representativo.
[Contratos e uso](docs/dados/PLANO_ESTRUTURA_DADOS.md#1820-painéis-sociais-explícitos--men-social). A extensão
BD16-LAB-LINKS integra fontes explícitas, atividade/indicadores/TFs ao candle
e relatório HTML local. [Uso](docs/sdk/laboratorio_series.md#fontes-vinculadas-candle-e-relatório-portátil)
e [evidência/limites](docs/dados/PLANO_ESTRUTURA_DADOS.md#1819-cruzamentos-no-candle-e-relatório--bd16-lab-links).
SHA/PR/recibos/prévia atuais ficam no registro central Projects/.workspace.
Não deduzir ciclos macro, causa do preço ou aceite financeiro das correlações.

## Referência de menções em29/10/2026

A branch de referência `codex/mencoes-aceite` contém MEN-00…08 sobre a cadeia
anterior do Laboratório/Dados. A ponta de código MEN-07 é
`72850fb7a5810932c89e87bbb691e3f2ff6dcb40` ([PR243](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/243));
o fechamento MEN-08 reconcilia aceite, proteção e documentação. A cadeia permanece
aberta, sem merge em master. [Estado técnico, uso e limites](docs/dados/PLANO_ESTRUTURA_DADOS.md#1818-aceite-integrado-proteção-e-continuidade-men-08).

Na página privada existente, a seção **Menções na internet e recuperação dos
ensaios de29 outubro2026** contém manifesto e9 partes do complemento MEN,
independente do pacote principal antigo. Restore conferiu160 arquivos por hash
e30 bases próprias de QA. Reutilize `scripts/recover_cloud_backup.py`, extraindo
em pasta nova de QA; nunca restaure os ensaios sobre a base principal. Não há
texto bruto de terceiros, credenciais ou base de negociação nesse complemento.
O manifesto/referências ficam privados, fora do Git público.

A raiz única é `MT5X`, App em `repos/app` e companions nos pins detached do
manifesto; orientações antigas de clones/worktrees são históricas. A prévia
habitual continua pelo launcher oficial `mt5x/app-development/run.sh`, com
runtime identificado e retorno preservado. Classificação tem domínio inglês
controlado; idiomas/fontes amplos, Expert, MT5/conta e aceite financeiro seguem
com gates próprios.

## Frente planejada: Dashboard Contínuo — 30/10/2026

[Plano canônico](mt5_extracao/dashboard/PROMPT_DASHBOARD.md): história visual
contínua de ativo ou robô, Flask local, evidências point-in-time e HTML offline
autossuficiente. A CLI `dashboard` abre a fotografia sem iniciar ponte ou análise.
Servidor, fotografia, arquivo, regime e volume estão implementados em seus
recortes; DASH-06…08 (hipótese, validação, vencido/invalidação) têm infraestrutura
na PR268 com integrações reais pendentes; próxima faixa é DASH-09. Surpresa real depende do produtor canônico.
As faixas restantes e os aceites integrais permanecem pendentes. Não criar motores de
cálculo/catálogo/publicação paralelos nem presumir T13.4/T13.5/T13.8 concluídas.

## Instalação inicial

No navegador, baixe o launcher desta branch `codex/novo-host-20261024`:

- Linux: [bootstrap-dev.sh](bootstrap-dev.sh). Execute no terminal:
  `bash bootstrap-dev.sh --yes --workspace "$HOME/Projects/MT5X"`.
- Windows: [bootstrap-dev.ps1](bootstrap-dev.ps1). Execute no PowerShell:
  `powershell -NoProfile -ExecutionPolicy Bypass -File .\bootstrap-dev.ps1 -Yes -Workspace "$HOME\Projects\MT5X"`.

O launcher instala Git se necessário, obtém o App, delega Python/ambiente/config
ao instalador oficial, instala a toolchain de desenvolvimento e clona HADOUKEN e
Safety nos commits do manifesto. Sem `--yes`/`-Yes`, apenas descreve o plano.
Não é preciso ter Git ou Python para baixar o arquivo pelo navegador.
Linux pode pedir a senha de administrador pelo próprio sistema para os pacotes.
Windows sem Git e sem winget precisa do App Installer/winget ou Git oficial antes
da repetição; o script informa essa ação, sem esconder a dependência.

O modo inicial prepara desenvolvimento e UI offline. Para instalar também MT5/
Wine pelo fluxo oficial, acrescente `--with-mt5` no Linux ou `-WithMT5` no Windows.
Essa etapa não concede aceite de perfil, conta, Tester, estratégia ou negociação.
Para preparar a integração Linux sem solicitar abertura de conta demo, use o
instalador oficial com `bash install.sh --yes --skip-system-packages --no-demo-account`
quando os pacotes nativos já estiverem preparados. A opção preserva instalação,
configuração e diagnóstico; conta e conexão permanecem etapas explícitas.

## O que continua pendente

O Laboratório está **incompleto, com aceite funcional reaberto** após o teste
real do proprietário. [LAB-01…10 e SA-01/02](docs/propostas/LAB_SUPER_ATR_PENDENCIAS_20261024.md)
registram todas as falhas e visões faltantes. Estado mais recente por item:
final de [continuidade](docs/continuidade.md) (LAB-08 bancada em 27/10). Prompts independentes:
[Laboratório](docs/repasse/PROMPT_LABORATORIO_GRAFICOS_20261024.md) e
[Expert Super ATR multi-TF](docs/repasse/PROMPT_EXPERT_SUPER_ATR_MTF_20261024.md).
O agente de Dados existente mantém cálculo/persistência/coluna Super ATR.
Não misturar as posses nem usar uma base antiga para substituir o motor atual.

Retomar pelo roteiro do novo host; os registros de21/10 que dependiam do SD são
históricos. Banco e WIP privado não ficam no repositório público. Autenticações
de Codex/GitHub/corretora são refeitas na conta do proprietário.

## Prompt de continuidade para o agente sucessor — harmonização de01/11

**Atribuição direta do proprietário:** o agente que recebe este prompt é o
executor responsável **nesta própria sessão**. Não é um pedido para preparar
outro repasse nem para escolher entre sessões. A frente de harmonização encerrou
sua implementação; não haverá implementação paralela dela no Dashboard.
Inicie a leitura e a execução agora, sem pedir confirmação para continuar.
Esta atribuição inclui as decisões rotineiras de implementação, testes,
integração, commits, publicação por PR e atualização da prévia já autorizadas.
Só peça ao proprietário acesso ausente, uma decisão realmente indispensável ou
uma ação fora da autorização vigente. A inspeção técnica de posse e recursos
continua obrigatória; ela não exige uma nova confirmação genérica do usuário.

Você é o único responsável por dar sequência ao desenvolvimento nesta composição.
Use somente `/home/misael/Projects/MT5X`, App em `repos/app`, com seu único `.venv`.
Leia as instruções AGENTS/RTK, este arquivo, os últimos registros de continuidade,
roadmap, plano e `mt5_extracao/dashboard/PROMPT_DASHBOARD.md` integralmente. Consulte
Projects/.workspace/projects.json para branch, SHA, PR, prévia e recibos atuais.
Não crie clone, worktree, árvore por agente, motor, cache ou estado administrativo
concorrente. HADOUKEN b730ed4a e Safety3988df13 são pins detached de leitura.

A harmonização preservou d19cd8c (DASH-04/corretivos CI), aa969ea (DASH-05/PR267)
e o corretivo separado da ponte/launcher. Não reaplique nem descarte esses commits.
O gerenciador da ponte aceita PID obsoleto somente com processo comprovadamente
ausente; PID vivo, ilegível ou porta sem posse continuam protegidos. O launcher
conserva o erro em log e o entrega à UI; o aviso de conexão é posterior ao término
do callback, sem tarefa falsa aguardando OK. Uma janela antiga não é atualizada
em memória pela troca da prévia: preserve trabalho/diálogo do usuário e oriente
reabertura pelo atalho se necessário. Não mate Wine, terminal ou processo alheio.

Menu e área de trabalho devem continuar no mesmo `mt5x/app-development/run.sh`.
Use o procedimento existente em docs/TESTAR_DESENVOLVIMENTO.md; registre SHA,
hashes, dados/ambiente compartilhados, retorno e aceite real no recibo existente.
Não confunda runtime instalado com checkout editável; nunca edite releases.
O aceite da ponte é comunicação de leitura, não autorização de conta, Tester ou
negociação. Não crie conta nem use negociação REAL para testar.

Próximo incremento: DASH-06, verificando a existência do produtor de hipóteses;
se ainda ausente, faça apresentação/estado indisponível honestos e registre
integração pendente. Depois siga DASH-07…14, inclusive modo Robô, priorização,
temas, integração Tk e medições fim a fim. Preserve clocks, namespaces e origem.
Surpresa comparável, hipóteses, Windows físico e aceite visual offline mantêm
gates próprios; nenhuma fixture ou teste de sintaxe os conclui. Não contorne
recusa de URL do Browser. CI Windows do pai d19cd8c tinha três falhas fora desta
faixa: retenção de artefatos, leitura UTF-8 de comparação e layout estreito da
comparação. Confira o SHA/check atual antes de atribuir ou corrigir uma falha.

Execute DASH-06…14 até a entrega integrada e validada no escopo permitido.
Não pare após planejamento, scaffolding, primeira faixa ou relatório de estado
se existir trabalho seguro a executar. Uma dependência analítica ausente,
check remoto em andamento ou falha comprovadamente anterior não bloqueia as
partes independentes. Conserve essas pendências e avance inclusive no modo Robô,
na integração Tk, no arquivo portátil e nos testes de desempenho. Não invente
produtores, fórmulas ou aceites para fechar um item. Acompanhe a CI nos marcos
de publicação, sem polling contínuo nem declarar concluído o que ainda não foi
verificado. Ao atingir um bloqueio real, identifique o requisito e a evidência
que faltam; continue todo o restante que não depende dele.

Uma tarefa por PR, publicação serial pelo guard, SHA remoto confirmado; sem
force push, merge automático ou release. Antes de branch/índice/commit/prévia
ou suíte pesada, confirme que não há outro agente usando o recurso. Uma árvore
limpa não comprova disponibilidade. Se voltar a haver concorrência, delimite
posse explicitamente; não substitua o pedido por outro projeto paralelo.

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
167testes/14módulos em145,31s, inclusive famílias anteriores e agentes. A chamada
real do botão Backtest no atalhoef0f429 confirmou pacote/banco originais,
59inputs,3posições/7deals/1parcial/2BE/5trailing, diário3entradas/8ações encerradas,
caixa1,46USD e recursos liberados,175,747s. Replay pelo botão atingiu180s e
liberou recursos; comparação não aprovada. A primeira janela reaberta manteve
o contexto padrão01→02nov/100000 e não teve operações: não exercitada; receita
corrigida repôs12→13out/10000 pelos widgets, sem alterar o recibo anterior.
Revisão posterior ligou o retentor já existente também à admissão tardia do
Budget, protegendo perfil que falhe antes de publicar run:347regressões/22módulos
em212,93s e doctor16OK/2avisos/0falhas sem probe. Evidência anterior na tela é
limpa após falha nova; permanece consultável no banco. Regressão reproduzida e
6casos Tk/7,83s passaram após o corretivo final de apresentação. Fonte MQL não mudou.
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

## Excursões nas lentes e no cache — 12/11/2026

Owner Dados/Laboratório, fila DIAG-EXCURSOES, branch `codex/lab-excursoes`.
As lentes agora preenchem MAE/MFE ausentes com o enriquecimento e preservam
valores registrados no livro. Vendas usam o ask modelado pela regra de spread
do backtester, incluindo o piso calibrado. Falta ou invalidade do point mantém
as excursões indisponíveis. O campo `excursion_basis` identifica livro,
combinação de fontes, envelope bid/ask ou ask indisponível. A versão de cálculo 2
invalida caches anteriores e acrescenta a coluna pelo mecanismo aditivo existente.
As amplitudes continuam em unidades de preço sobre envelopes de barras. A ordem
dos eventos intrabarra e o instante dos extremos da barra de saída permanecem desconhecidos.

Reprodução inicial com quatro falhas. No Linux, lotes separados tiveram 38 testes
aprovados e 21 de cache/livro/importação/CLI aprovados, incluindo 50 mil operações.
Após ajustar tipos e cobrir cache legado, o lote final teve 13 aprovados e um teste
PostgreSQL pendente por ausência de `MT5X_TEST_PG_URL`. As contagens se sobrepõem.
O lote de cache levou 56,46 s e atingiu 400 MiB de RSS. A medição durante o lote
registrou RAM disponível acima de 7,9 GiB, swap inativo e ACPI até 59 °C nas amostras.
Doctor offline aprovado com 16 OK e dois avisos. Launcher oficial CLI com SQLite
sintético confirmou MAE 4, MFE 2 e `ask_bar_envelope`, conservando o livro.
CI do novo SHA, PostgreSQL real, Windows físico e percurso GUI desta correção
permanecem pendentes. Evidência local em `state/qa-tmp/diagnostico-20261112`.

## Auditoria de ticks versão 2 — 12/11/2026

Owner Dados/Laboratório, fila DIAG-TICKS, continuação da PR304.
Foram reproduzidas 15 falhas em flags não finitas, fracionárias ou fora do
intervalo, sequência inválida/duplicada e auditoria vazia com promoção de classe.
A versão 2 registra metadados inválidos e mantém a captura original. A classe A
exige auditoria atual com linhas, e a certificação intrabarra exige sequência
sem defeitos bloqueantes. Point inválido ou conversão que exceda o número finito
é diagnosticado, conservando o spread em unidades de preço. Arrays inteiros
nullable têm seus valores ausentes contabilizados. O corretivo de timestamps
inválidos e seus seis testes já presentes no checkout foram revisados e preservados
neste incremento.

Regressão Linux em lote único com elegibilidade e recuperação teve 80 aprovados.
Após cobrir booleanos e inteiros nullable, o recorte final teve 34 aprovados em
3,47 s. Contagens sobrepostas. Doctor offline teve 16 OK e dois avisos.
O percurso CLI dedicado e a GUI aguardam uma janela sem a VM de outra tarefa,
que passou a ocupar cerca de 2 GiB com pressão de I/O. A suíte Windows da ponta
anterior falha no lote de cancelamento SQLite, item DIAG-CI304. O CI da nova
ponta terá estado próprio. Windows físico, Tester e DEMO conservam seus aceites.


### Continuação operacional da composição em12/11

PR307 publicada em `9c2c7eb`, PR308 em `f6b6274`. O novo artefato
`development-f6b6274d7221` tem365 hashes conferidos e6.357.807B de runtime,
com ambiente/dados por links e retorno650656f preservado. O link estável
continua em650656f. A primeira admissão de QA do artefato foi recusada antes
de iniciar a GUI porque outra VM voltou a executar. A pasta própria
`preview-f6-qa` contém apenas o recibo de admissão e pode ser reutilizada
após reconferência. Usar PYTHONPATH da própria release no hook e afirmar
os caminhos dos módulos para impedir importação acidental do checkout
editável. Próxima investigação independente é DIAG-LEASE-REBOOT.


## DIAG-LEASE-REBOOT — identidade e retomada em 12/11/2026

O helper comum preserva o digest legado e passa a registrar boot e namespace
Linux com a identidade de processos novos. Um boot anterior só prova término
quando o digest confirma a mesma máquina, o UUID/namespace são válidos e o
processo é anterior ao boot atual. Host estrangeiro, namespace divergente no
mesmo boot, identidade parcial, erro de leitura e registro legado sem essa
prova continuam preservados. O protocolo Windows conserva seu escopo existente.

Foram reproduzidas retomadas indevidas com identidade ausente/incompleta,
estado desconhecido e histórico de job corrompido. A fila existente agora
revalida a identidade do solicitante, estado, tempo e histórico antes do CAS,
na mesma transação. Consulta sem claim continua disponível. A recuperação do
produtor e o lease de Recursos usam o mesmo helper. Nenhum scheduler foi criado.

Validação local em Python 3.11.15, um processo e BLAS/OMP em um thread. Lotes
focados reuniram 43 casos distintos aprovados, com sobreposição entre lotes.
Os três casos finais de posse ativa, publicação interrompida e histórico de
falha passaram em 5,76 s. O launcher oficial criou uma cópia SQLite própria,
gravou evidência de boot e liberou o lease. Doctor offline teve 16 OK, dois
avisos e zero falhas. Recibos locais estão em
`state/qa-tmp/diagnostico-20261112/lease-*.xml` e `lease-launcher/proof.json`
da composição. VM alheia preservada; RAM disponível observada próxima de
6 GiB, sem ensaio de carga, alteração de serviços ou dados do proprietário.

Aceites restantes incluem CI do SHA publicado, Windows físico e reinício
físico em janela autorizada. Os metadados de boot das regressões são
controlados. A trava real inspecionada já estava liberada, sem reparo manual.
As evidências desta correção não diagnosticam a causa dos reinícios do host.


## DIAG-RETENCAO — cópias com proveniência, 12/11/2026

O expurgo global de `*.db` apagou um arquivo desconhecido no ensaio anterior
à correção. O produtor existente agora grava recibos privados `mt5x-backup/v1`
ao lado das cópias, com ID estável, identidade física da origem/artefato, bytes
e SHA-256. O nome novo inclui UUID e prefixo limitado para conservar identidade
entre solicitações. Legado sem recibo, cópia de outra base, link, conteúdo
alterado e schema ilegível/desconhecido são preservados. A leitura de cópia
recente usa a proveniência registrada. O hash integral é reconferido no expurgo.

`backup_keep` conta cópias reconhecidas da base atual. Intenções de retenção
são sincronizadas no recibo antes de cada exclusão. Resultado incerto bloqueia
novo expurgo. `resources backup-status --operation-id ID` consulta/reconcilia
presença e identidade, sem repetir exclusões. Ausência é registrada como
`absent`. Conteúdo conhecido presente recebe `not_applied`. Conteúdo alterado
continua pendente. Os recibos persistem após expirar o arquivo SQLite, mantendo
o resultado consultável. Recibos são privados, limitados a 256 KiB cada.
A GUI mostra arquivos preservados e solicita Conferir retenção pelo worker.

Validação final local teve 22 regressões novas aprovadas em 6,49 s. Um lote
anterior de 27 casos, sobreposto, passou em 7,03 s com identidade/restauração.
Dois casos existentes de cópia/CLI foram reexecutados separadamente. O lote
integrado inicial foi interrompido por orçamento após 31 aprovados em 101,74 s,
com recibo e resultados parciais preservados. O maior processo observado nesse
lote teve cerca de 155 MiB RSS, com RAM disponível acima de 6 GiB e 48 °C na
medição. A estimativa de duração desse lote foi excedida e o restante foi
dividido. Doctor offline teve 16 OK, dois avisos e zero falhas.

Launcher oficial em quatro chamadas próprias confirmou retenção, arquivo
desconhecido com valor 7 preservado, recibo conservado e consulta do ID expirado.
Outro ensaio CLI com falha controlada após excluir retornou código 1/estado
incerto e a consulta reconciliou ausência. Recibos locais em
`state/qa-tmp/diagnostico-20261112/retention-*`. Os bancos dos ensaios são
descartáveis. QA gráfico do artefato final aguarda capacidade do host, ocupado
pela VM alheia. CI do SHA publicado, Windows físico e perda de energia possuem
aceites próprios. A causa dos reinícios do host continua sem diagnóstico.


## Ponto de retomada — composição 2066d7b em 12/11/2026

PR309 publicou `2a5638d49be4043576fa27f2d56169cc1bcaae56`; PR310 publicou
`ba8963cd66cb48d0c4860a766ad762da56c3623d`, dependente da PR307. A PR308
integrou ambos em `2066d7bce6a787dbb0c80d37072d278f613f0042`, SHA remoto
conferido. Índice principal e HEAD `550b6d5` preservados. Os 337 arquivos dos
caminhos de runtime coincidem com a árvore publicada. Recibos em
`composition-runtime-proof.json`, `composition-lease.json`,
`composition-retention.json`, `lease-publication.json` e `retention-publication.json`.

`development-2066d7bce6a7` está preparado com 365 arquivos/6.374.036 B e hashes
conferidos. É artefato de runtime sem Git. O link efetivo continua em 650656f.
`state/development-preview.json` foi reconciliado com essa release/354 hashes,
sem trocar o link. O recibo anterior foi conservado em
`preview-record-before-reconciliation.json`. O QA do artefato exato está
preparado em `preview-2066-qa`, com duas bases próprias, preferências isoladas,
checagem de importação pela release, restauração incerta/recuperação e consulta
de retenção pela GUI. Ainda não foi executado. Reavaliar host antes de iniciar.

A VM externa Deck-SD-offline-DISM continua com aproximadamente 2 GiB RSS.
Medições posteriores mantiveram cerca de 6,2 GiB disponíveis, 48–52 °C e PSI
de I/O variável, frequentemente acima de 10%. Nenhum processo alheio foi
interrompido. CI remoto continua rodando, sem polling contínuo.

O contrato do próximo pacote foi registrado em `package-contract-current.json`.
Há uma pergunta de escopo pendente ao proprietário sobre o destino da reunião.
Enquanto isso, preservar fontes, investigar somente contratos/metadata e
concluir aceites independentes. A conexão normal do DatabaseManager assegura
tabelas de controle ao iniciar. Um plano prometido somente leitura precisa
usar leitura existente/URI SQLite ro para garantir ausência de DDL/criação.
Não afirmar que snapshot-export conserva toda coluna legada ou reconstitui
base bruta, pois o leitor OHLCV ativo projeta oito campos.

Próximas ações exatas são conferir resultados dos checks de PR307/308/309/310
em marcos úteis, completar GUI do artefato com orçamento quando houver margem,
só então atualizar a prévia pelo procedimento existente e reconciliar registros.
A decisão pendente determina o contrato do pacote/reunião. Persistem os aceites
físicos e financeiros delimitados na fila.


## DIAG-RETENCAO — fronteiras ampliadas em 12/11/2026

A primeira revisão deixou passar a perda de um recibo pendente. As duas
regressões seguintes reproduziram exclusão de outra cópia quando o recibo
ficou ausente ou corrompido. O produtor agora conserva todos os possíveis
alvos diante de recibo desconhecido/incompleto e de marcador de cópia sem
recibo. Recibos válidos de outras bases conservam seu isolamento. Restauração
pendente também impede expurgo. A consulta informa os recibos indisponíveis
na CLI e na GUI, com cópia nova e resultado da retenção em estados próprios.

Outra regressão reproduziu troca da identidade da origem após o snapshot com
sucesso indevido. A identidade é fixada antes do lease e reconferida depois
do snapshot/hash, incluindo a identidade do artefato. A falha conserva o ID
da solicitação e evita auditar uma base que substituiu a origem escolhida.

Quatro ensaios próprios reproduziram a limpeza genérica apagando cópia, base
ativa, arquivo e controle quando as pastas se sobrepunham. O leitor de
candidatos agora protege os diretórios de dados/controle, arquivos SQLite,
recibos e arquivos auxiliares da base ativa. Referências positivas dos recibos
de restauração conservam origem e cópia de segurança após mudar backup_dir.
O teste de mudança de pasta demonstrou preservação dessas referências e
remoção de um CSV próprio elegível. A guarda de referências continua no
serviço existente, sem catálogo ou mecanismo de limpeza paralelo.

Validação final com sete fontes registradas e hashes idênticos antes/depois
teve 44 aprovados em 9,75 s, incluindo 35 casos novos e nove de restauração/
identidade. Guardas existentes tiveram 28 aprovados e um caso gráfico sem
DISPLAY em 7,36 s. Dois casos de limpeza/CLI passaram em 2,52 s. Doctor
offline teve 16 OK, dois avisos e zero falhas. Resultados intermediários foram
repetidos após a estabilização das fontes. Nenhum gate foi afrouxado.

Quatro chamadas do launcher oficial com dados próprios produziram saídas
0/1/1/1. Após perder um recibo pendente, o ciclo conservou as cópias anteriores
e a consulta confirmou a pendência sem solicitar exclusão. Evidências em
`retention-unknown-launcher/proof.json`, `retention-frozen-final.xml`,
`retention-existing-frozen.xml` e `retention-final-frozen-sources.json`.
O artefato 2066 preparado anteriormente foi marcado como superado antes de
qualquer ativação. Publicação e composição desta revisão exigem nova
conferência de SHA, CI e QA gráfico do artefato exato.


## DIAG-RETENCAO — leitura completa do controle em 12/11/2026

Reprodução em árvores SQLite próprias mostrou que a enumeração de arquivos podia
ocultar uma falha de leitura e esquecer uma intenção pendente. Recursos agora
materializa a leitura e propaga a falha antes de qualquer expurgo. A cópia já
conferida permanece disponível, com retenção incerta e consulta explícita.

No Linux, 45 regressões de cópia/restauração passaram em 10,15 s e 29 de
proteção, sessões e leitura parcial passaram em 13,31 s. O caso Tk antes
adiado também executou no compositor real. Sete hashes de fontes foram
conferidos antes e depois. Doctor offline teve 16 OK, 2 avisos e zero falhas.
Um processo, BLAS/OMP com um thread, sem suíte completa local. Falhas originais
foram preservadas. CI deste incremento e GUI do artefato composto seguem
pendentes. Entrada física e interrupção de energia têm aceites próprios.


## DIAG-RESTORE — contrato de relógio após a troca em 12/11/2026

Duas falhas reproduzidas mostraram que a validação de relógio da sessão anterior
continuava em cache após restaurar uma base UTC ou com outro fuso. A restauração
agora invalida essa validação junto ao cache de tabelas, inclusive se o efeito
terminar incerto. A próxima gravação passa pelo validador canônico existente.

Seis casos cobrem relógio e fuso divergentes, compatibilidade e resultado
interrompido após a cópia. O lote integrado de restauração e tempo teve 39
aprovados em 14,20 s no Linux, com dados próprios, um processo e um thread.
A reprodução anterior teve duas falhas e um caso compatível aprovado. CI deste
SHA e QA do artefato composto permanecem em validação.


## Prévia candidata conferida em 12/11/2026

PR308 `561152b` contém o runtime das correções de relógio/restauração, lease e
retenção. Os 365 hashes do artefato de 6.378.666 B foram conferidos. O launcher
do próprio artefato abriu no compositor XWayland com configuração e duas bases
SQLite próprias. Recuperação 7→99, recusa de relógio incompatível, cópia e
consulta de retenção pelo worker passaram. Foram 25 pulsos, intervalo máximo
0,571 s, pico 275,2 MiB, mínimo disponível 7,90 GiB e máximo 59 °C.

Observação contínua adicional, limitada a 180 s, teve 4.168 pulsos, seis
atualizações, três alternâncias de tema e zero erros de callback. Pico
272,6 MiB, mínimo 7,73 GiB e máximo 68 °C. Quatro hashes de preferências e
favoritos do proprietário permaneceram iguais em ambos os ensaios. Os
controladores solicitaram as ações na janela visível. Entrada física,
acessibilidade, login, maior duração, hardware nativo e energia seguem próprios.
A ponta pública da retenção `97da399` reposiciona somente recibos documentais
para liberar CI após a atualização da dependência, com merge-tree limpo. O
runtime conserva o SHA de origem `fdbe0d9` usado na composição e no artefato.
O link estável continua em `650656f` até a conferência dos gates pertinentes.


## DIAG-LEASE — leitura do teste sob bloqueio em 12/11/2026

A tentativa Windows1 foi cancelada prematuramente por esta tarefa após mais
de duas horas. O log arquivado mostrou avanço até98% e uma falha anterior no
caso de reconciliação do lease. A tentativa2 no mesmo SHA foi solicitada antes
da leitura dessa falha. O teste abria outro descritor para ler o byte que
mantinha bloqueado. O contrato do Windows recusa esse acesso pelo segundo
descritor no mesmo processo. [Fonte primária](https://learn.microsoft.com/en-us/windows/win32/fileio/locking-and-unlocking-byte-ranges-in-files).

Uma reprodução local com leitura externa recusada teve1 falha/1 aprovado.
O teste passou a observar o descritor que adquiriu o lease e conserva as
assertivas de estado ativo/liberado. Também prova a recusa do leitor externo.
O bloqueio real e a produção conservam seu fluxo.66 casos locais passaram
em39,14s, um processo e BLAS/OMP com um thread. O novo Windows CI ainda
precisa confirmar a portabilidade. Os logs anteriores permanecem preservados.


### Prévia adiada por pressão do host

Os cinco jobs de produto, incluindo Linux completo, passaram nas PR295/307/
308/309/310; Windows permanece em execução. O artefato2b está preparado e seu
launcher XWayland já passou. A tentativa de troca do atalho parou na verificação
de swap antes de criar a intenção ou alterar o link. A consulta confirmou
app-development→650656f e ausência de intenção de ativação. Três amostras em
20s mostraram swap-in contínuo, incremento de swap-out na terceira janela,
MemAvailable6,4–6,5GiB, PSI de memória avg10 até0,99 e I/O avg60 de12,94–17,78.
Temperatura58°C. A ativação aguarda nova admissão com margem; fonte, manifestos
e recibos permanecem preservados. Nenhum processo alheio foi encerrado.
Próxima ação exata é reconferir pressão/carga/swap/processos e, com margem,
retomar a ativação2b seguindo o procedimento de docs/TESTAR_DESENVOLVIMENTO.md.


## Disponibilização conferida da prévia2b

O atalho e o menu agora resolvem a composição `2b3b274`, com365 arquivos
conferidos. Linux completo, PostgreSQL, lint e os dois instaladores passaram
no SHA publicado. Windows permanece em curso. O procedimento de ativação
a674860c2d9a4d6398587a43e80513a4 registrou intenção e resultado duráveis e
revalidou destino/atalhos/estado compartilhado antes da troca atômica. O pico
do processo de ativação foi23,3MiB. A conferência pelo launcher estável teve
saída0, três cópias próprias consultadas, valor99 preservado, pico134,6MiB,
mínimo8,21GiB disponíveis e duração4,09s. Retornos650/57 estão preservados.

A primeira preparação ligava uma lista legada de símbolos do checkout. A
comparação com a prévia efetiva mostrou que esse arquivo estava ausente.
O vínculo novo foi retirado antes da ativação e o arquivo legado foi preservado.
O método de produção `load_symbols` tem o mesmo conteúdo nas duas versões;
executado com objeto controlado e ausência real do arquivo, devolveu os mesmos
cinco símbolos e configuração de tempos. Essa validação foi controlada. A GUI
XWayland2b foi exercitada antes da reconciliação desse vínculo, no mesmo código.
A comprovação de hash/existência de preferências cobre os ensaios finais
isolados; o estado anterior aos primeiros ensaios permanece sem comprovação.

A admissão inicial de carga pesada foi aplicada à troca de metadados e adiou
a tentativa. A operação final foi dimensionada como leve, com128MiB previstos,
32MiB de leitura,256KiB de gravação e reserva mínima de4GiB. A GUI, builds e
suítes locais ficaram adiados durante a pressão de I/O. Foi observado copiador
de outra tarefa,13MiB lidos e13MiB gravados em3s, preservado em execução.
Kernel sem avisos nos30min consultados e Btrfs com6.741 corrupções acumuladas,
sem mudança dos contadores. Essas observações mantêm a causa dos reinícios
indeterminada. Recibos completos em state/qa-tmp/diagnostico-20261112/.


## DIAG-CI-MACRO — orçamento coerente do cenário em 12/11/2026

Windows2a/131574907486 reprovou o cenário macro por RSS do processo acima de
1024MiB. Windows7df/131581528852 passou nesse cenário e reprovou somente na
verificação Node de10s. A atribuição anterior a7df foi corrigida após ler o log.
A fixture já mede RSS e declara um orçamento na coleta; a análise passava a
receber o valor padrão. O incremento propaga o orçamento existente à análise.
O serviço de produção mantém seu contrato de memória e o cenário conserva o
mesmo teto entre etapas.

Regressão com sampler controlado em1536MiB reproduziu MemoryError na primeira
análise, em3,12s. Após a correção, o cenário concluiu e recusou2048MiB, acima
do teto já declarado de1792MiB. Cinco testes de fases, relatório e limites
passaram em8,40s no Linux. Dados próprios de95 barras, um processo e um thread.
Os valores do sampler representam simulação, com memória física limitada ao
processo de teste normal. Regressão gráfica Windows e CI da ponta seguem em
validação. Integração requerida nas PR309/308; artefato2b conserva o mesmo
runtime. Logs e falhas anteriores permanecem como evidência.

## IND-AGGR-01 · integração Linux em validação — 14/11/2026

A rotina MQL OnCalculate foi executada em adaptador C++ pelo CI Linux.
O commit de regressão f53bb30 registrou quatro falhas em 174 testes.
A correção 461a067 passou os 174 testes e o checker de 171 fontes; 438e49e
acrescenta o recibo, mantendo a implementação idêntica. HADOUKEN PR86 e
App `codex/ind-aggr-integracao` preservam o incremento em revisão. Safety
27b8599 e SA-REF-01 permanecem fixos.

O manifesto usa o novo pin de desenvolvimento. Isso não qualifica o fluxo
CopyBuffer/MT5: testes locais compilados, MetaEditor e prévia aguardam admissão
de RAM/swap/PSI. A prévia habitual continua 2b, com retorno conservado.
Recibo integrado: docs/evidencias/IND_AGGR_INTEGRACAO_20261114.json.
CCI PR314/2b8bb96 completa BD01/Biblioteca após quatro falhas de catálogo
no CI e0; seu novo CI e GUI final continuam próprios. Próximo item: E14-METRIC-01.


## E14-METRIC-01 · integração Linux em validação — 14/11/2026

HADOUKEN PR87 preserva o histórico de reprodução1c78e1b e implementação68bb6b1.
A reprodução falhou em um de175 testes: DayNet retornava8 parcial após leitura
válida28. A correção passou181 testes e checker171 no CI43580757667. O pin231343f
acrescenta regressões de sentinelas EMPTY_VALUE e também passou181 testes no
CI43581200274.

O serviço e o painel distinguem indisponível, parcial, antigo, sem saídas e
atual. Idade30s e mudança de recorte/relógio retiram a qualificação atual;
último valor válido aparece com data. O leitor ativo das anotações descarta
propriedades/tickets parciais; o livro existente omite resultado não finito.
Os comandos não dependem dessas estatísticas de apresentação; continuam pelo
RiskGuard/executor, cujo escopo será qualificado em E14-SCOPE-01.

App `codex/e14-metric-integracao` atualiza o pin de desenvolvimento, preservando
Safety27b8599 e SA-REF-01. A prévia habitual permanece2b. Conferência nativa,
GUI e prévia aguardam admissão de RAM/swap/PSI; Windows físico aguarda host.
Recibo: docs/evidencias/E14_METRIC_INTEGRACAO_20261114.json.


## E14-SCOPE-01 · integração Linux em validação — 14/11/2026

HADOUKEN PR88/4982259 corrige a confirmação de zeragem com inventário ilegível
e vincula o snapshot histórico a conta/servidor/moeda. A regressão0f02b94 falhou
em dois de182 testes; o pin final passou184 testes e checker171 no CI43584418492.
Fechamento revalida modelo/dono/tipo/símbolo/volume; filtros de símbolo desconhecido
não certificam cancelamento. Inventário comprovadamente alheio é preservado.

O painel distingue patrimônio da conta, resultado por Magic e risco até SL.
Aportes/retiradas afetam a barreira patrimonial e não liberam bloqueios persistidos.
Âncoras conservam chave/valor atuais; a data do recorte não é hora comprovada de
medição. Estado financeiro sem identidade/recorte e exposição netting/exchange
sem contribuição qualificada não aparecem como números atuais do Magic.

App `codex/e14-scope-integracao` reconcilia o pin e preserva Safety27b8599/SA-REF-01.
Checker local foi aprovado na nova admissão após timeout10s preservado; a primeira
publicação ocorreu antes de receber aquele resultado local. MetaEditor, GUI e
prévia continuam sujeitos à margem de RAM/swap/PSI. A prévia habitual segue2b.
CCI2b8bb96 e integraçãoAGGRadf31e4 já passaram Linux completo/PG/instaladores/lint;
Windows CI segue próprio. Próximo item: E14-EXEC-01.
Recibo: docs/evidencias/E14_SCOPE_INTEGRACAO_20261114.json.


## E14-EXEC-01 · integração Linux em validação — 14/11/2026

HADOUKEN PR89/95af71a publica S09 2.14: baselinee23d3615 reproduziu duas
falhas de185 testes, e a correçãob73e4bcb passou185/checker171/sintaxe no
CI43587026347. O followup documental final passou CI43587182502.
A consulta do journal antes da entrada conserva a barreira de reentrada/reversão
na barra em que houve pedido de fechamento, inclusive após reinício. Resultado
aceito/parcial/incerto continua distinto do efeito conciliado e não autoriza
reenvio para completar volume. O legado e chamadas diretas sem vínculo passam
a recusar entrada/gestão fora do Tester DEMO; não há migração operacional de
posições ao vivo. TradingAllowed propaga essa indisponibilidade ao painel.

O App fixa95af71a e acrescenta somente a identidade2.14/perfil16, fingerprint
9a6b5b468cd4d7c3,59 inputs/59 fontes. A identidade2.13 anterior e SA-REF-01
continuam congeladas. O gate real do mapper, executado de forma leve com o
catálogo canônico, aceitou a fonte exata e recusou rótulo antigo/inputs incompletos;
isso não é o mapper completo, Session ou replay nativo. `operational_acceptance`
permanece falso. Os outros perfis com closure não qualificada continuam recusados.

CCI2b8bb96 concluiu os seis checks do App no CI43578756978. A primeira fixture
62e54e89 teve erro de extração, corrigido antes da reprodução; evidência preservada.
A reserva medida do host adiou compilação e GUI finais, sem encerrar processos
alheios. A prévia habitual continua2b;d136 está preparado, sem ativação e anterior
a este pin. Próximo desenvolvimento: SA-01-CONTRATO, mantendo a QA pendente.
Recibo: docs/evidencias/E14_EXEC_INTEGRACAO_20261114.json.


## SA-01-CONTRATO · integração Triplo em validação — 14/11/2026

O contrato `super_atr_triple_closed/1` compõe três instâncias do kernel existente,
no mesmo TF/prefixo/seed: período20, SMA e desvios1/2/3 por padrão, configuráveis.
Unanimidade inicial da seed não é entrada. Um episódio novo exige retorno ao
consenso após desacordo; stop ou reinício não rearma um consenso antigo.
O Expert exige pacote vinculado ao Tester DEMO, seed broker positiva/alinhada,
risco composto até0,5%, uma posição, SL absoluto da curva1 arredondado para fora,
TP zero e nenhuma parcial/BE/trailing. Saída oposta precede entrada e mantém a
barreira existente contra reversão na mesma barra. Não há executor/store novo.

O App oferece Default desarmado e não configurado; um Manual exige primeira
barra real explícita, sem conversão para o fuso do host. Pacote/UI usam os13
inputs tipados e o mesmo Session/binding. Conferência cruza prefixo observado,
header MQL original, episódio, plano/risco/margem, bytes do journal canônico e
SL/TP lossless da ordem/deal, fills, custos e livro. Ausência não vira zero.
Trace alterado após reinício, tentativa parcial/incerta e efeito não conciliado
recusam qualificação; não autorizam reenvio. A fórmula Python antiga e SA-REF-01
permanecem byte a byte preservadas; equivalência externa continua hipótese.

HADOUKEN PR90 fixa42793107 (implementação03e6a9a):192 testes, checker e sintaxe
passaram no CI43608990320. Triplo1.00/perfil1 usa fingerprint802c44f481a9bb8a,
13 inputs/50 fontes. S09 usa nova identidade2.16/perfil17/a9379fff9788159b por
mudança legítima da closure compartilhada;2.14 anterior mantém sua identidade.
Safety27b8599 permanece intacto. O gate leve local aceitou fontes exatas e
recusou rótulo antigo/inputs incompletos (22MiB); isso não é mapper/Session/GUI.

Testes App, doctor, compilação nativa, replay e GUI finais aguardam seus gates:
a admissão local recusou a suíte por reserva de RAM, sem iniciar um compilador
nem encerrar trabalho alheio. Prévia habitual ainda2b; nenhuma ativação nova.
CI Linux/Windows, preview coerente, QA nativa e SA-01-PLOT seguem pendentes;
Windows físico requer host qualificado futuro. `operational_acceptance=false`.
Recibo: docs/evidencias/SA01_TRIPLO_INTEGRACAO_20261114.json.


### SA-01-CONTRATO · correção da fixture gráfica

O CI43610229517 de45186fd1 aprovou cinco checks, incluindo Linux e PostgreSQL.
Windows concluiu4953 testes/205 skips com uma falha na fixture nova de layout:
a última entrada da lista era orçamento em outro contêiner, gerenciado por pack,
e não o checkbox do formulário. A conferência agora exige o mesmo contêiner e
compara o botão Salvar com TODOS os campos da estratégia em grid. O gate e o
formulário permanecem íntegros. A nova revisão exige CI por SHA próprio; não
qualifica Windows físico, MT5, GUI local ou replay nativo. Admissão local repetida
recusou pytest por reserva medida de RAM, sem iniciar a suíte.


## SA-01-PLOT · integração da apresentação em validação — 14/11/2026

HADOUKEN PR91/566c8e32 (implementação6175aba) desenha os pontos fechados do
MESMO objeto CHDKSuperATRTriple e prefixo usados pelo Expert. Não há iCustom,
nova fórmula ou dependência de decisão no renderer. InpPlotEnabled=false inicia
a apresentação; Estudo no painel alterna só a visibilidade. A janela visual tem
até256 ligações por curva (768 objetos), enquanto o cálculo conserva até20000
barras da seed. Warmup/ausência não recebem linhas zero. Segmentos imutáveis
são reaproveitados, antigos próprios são podados, e a remoção é conferida.
Colisões/identidades alteradas/remoção recusada são preservadas com diagnóstico;
nenhum estudo ou tema alheio é removido. Falha visual não bloqueia gestão.

195 testes e174 arquivos MQL5 passaram no CI43611822450. O renderer real foi
exercitado com API gráfica simulada (âncoras binary64, janela, alternância,
falhas e ownership); OnTick/refresh reais receberam sequência pareada de plot
desligado/ligado/falhando. Isso não certifica aparência, latência ou fills no MT5.

O App oferece o checkbox na identidade Triplo1.01/perfil2,14 inputs/51 fontes,
fingerprintfa4219764fd15a3c, estratégia nativa super_atr_triple_plot. O pacote
Triplo1.00/13 inputs permanece com parâmetros/pin próprios, sem migração. S09
recebe identidade2.17/perfil18/d59b9d01b8cde747 pela API compartilhada de painel;
2.16 e anteriores ficam congelados. Safety27b8599 permanece fixo.

Trace de plot cruza níveis com decisões confirmadas; alternância é auditada à
parte. Comparador consultivo de pares exige dois replays DEMO ativos, conciliados,
fonte/prefixo/modelo/inputs operacionais iguais e visibilidade constante oposta;
compara decisões, risco e fills pelo protocolo existente. Seu recibo não confere
aceite visual, latência ou autorização operacional. A GUI não oferece outro
executor ou backtest Python. SA-REF-01 e produtor antigo permanecem intactos.

A primeira PR319/45186fd1 teve cinco checks aprovados. Windows:4953 passaram,
205 skips e uma falha da fixture de layout que usava o campo orçamento de outro
contêiner;3bca8c87 corrige a conferência de TODOS os campos da estratégia e segue
no CI43626382246. Novos checks App por SHA, doctor, launcher/GUI e MetaEditor/
Wine/replay pareado ainda pendentes. Admissão local recusou pytest por reserva
medida de RAM; sem interromper trabalho alheio. A prévia continua2b, sem ativação.
Windows físico aguarda host qualificado. Recibo: docs/evidencias/SA01_PLOT_INTEGRACAO_20261114.json.

## Auditoria de fechamento Windows e Ultra — 17/11/2026

Pedido: análise dos quatro pilares e sequência para finalizar a plataforma e produzir Ultra por Expert/ativo. Esta seção atualiza a leitura da fila histórica acima; reutiliza seus IDs, não cria outro roadmap. Escopo desta passagem: inspeção estática de implementação, contratos, testes existentes, ambiente e GitHub; registro documental. Nenhum build, instalação, negociação, suite de testes ou aceite nativo foi executado. Não equivale a auditoria exaustiva de cada linha nem a certificação de ausência de defeitos.

### Diagnóstico executivo e composição observada

A plataforma possui uma base substancial: ingestão, snapshots, registry/materialização, livro de operações, pesquisa temporal, composição, Default/Manual, sessões nativas, reconciliação e guardas. O fechamento ainda depende de integrar a composição comprovada, preparar o Windows na raiz única, concluir aceites nativos e completar a seleção/execução de políticas Ultra. Não é necessário recriar os quatro pilares.

Ultra é uma categoria de preset/política versionada vinculada ao Expert, ativo, timeframe e contexto; não é outro motor nem uma estratégia universal. O adendo antigo admite seleção de vários setups dentro de uma política, mas isso não obriga cada primeiro Ultra a ter múltiplos setups. A estratégia decide e consome mecanismos operacionais qualificados; Dados e Estatística fornecem entradas/evidências, nunca autorização de ordem.

Fotografia verificada nesta sessão:

- App HEAD `550b6d5fd1f094876532e2d5756eae75cb073706`, branch `codex/launcher-classes`, muitas alterações rastreadas e arquivos não rastreados de incrementos posteriores. HEAD isoladamente NÃO identifica o código presente. Preservados índice, branch e WIP; nenhum clone/worktree criado.
- HADOUKEN `d3dd3a5e9a680d0c97d58cf160c60c4e58e88a3f`, em `codex/painel-escopo-compilacao`; Safety `27b85992af4b10f4cd7687f370142ac88f515375`, detached. Pins coincidem com `docs/repasse/NOVO_HOST_MANIFESTO_20261024.json`. HADOUKEN já estava em branch: divergência da regra geral de referência detached, não corrigida por checkout automático.
- [PR351](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/351) aberta/draft, ponta `cd03aae2ed4614a19056318b7633720340432fd8`, base `ec2237aa11cda42b46a00136e0be40dc7d27c7ea`. [CI43877888506](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/actions/runs/43877888506) completed/success consultado no GitHub. Isso comprova o resultado daquele SHA, não equivalência do WIP ou aceite físico. Consulta H/PR92 pelo conector retornou404; estado remoto desse componente não certificado.
- `state/development-preview.json` ainda registra `2b3b274da6de`, enquanto o início deste handoff descreve prévia `ec2237aa11cd` e candidata PR351. Registro central também conserva referências históricas. É necessário verificar destino efetivo e hashes antes de declarar qual executável está em uso no Windows; esta auditoria não ativou prévia.
- Host consultado: Windows11 Pro, aproximadamente14,85GiB de RAM total e7,92GiB livre naquele instante. Não houve carga pesada. CPU/PSI/paginação/I/O/temperatura e espaço livre não receberam medição completa; essa fotografia não admite build ou teste longo.
- A raiz acessível é o mesmo acervo via UNC WSL. A tentativa de entrada WSL no caminho correspondente falhou com `chdir ... failed2`; não diagnostica perda dos arquivos, que foram lidos pelo Windows. `rtk` não foi localizado no PATH Windows/WSL nem nos dois locais pessoais consultados. Leituras leves usaram ferramentas disponíveis, sem instalação ou alteração global.
- `.venv` contém `bin/lib` Linux; `.venv/Scripts/python.exe` não existe. `run.bat` exige esse executável. A consulta aos caminhos/processos usuais não localizou terminal ativo nem perfil MetaQuotes padrão; não é inventário exaustivo de instalações portáteis.
- ai-memory respondeu, mas as buscas globais por MT5X não retornaram memória pertinente. Nenhuma memória anterior foi presumida, nenhum banco foi criado. Esta seção é o registro canônico solicitado.

### Achados e critérios de fechamento por frente

Classificação: **confirmado** = observado no código/host; **aceite pendente** = implementação ou evidência histórica existente sem prova atual do alvo; **investigação** = risco delimitado, sem defeito reproduzido nesta sessão. P0 antecede otimização; P1 fecha capacidades; P2 é expansão opcional conforme o preset.

| Prioridade / vínculo | Lacuna, evidência e consequência | Próxima ação, responsável e prova exigida |
|---|---|---|
| P0 · T20.30-HOST1 / BD21–22 | **Confirmado:** WIP composto sobre HEAD antigo, recibo de prévia e handoff divergentes. Testes verdes de outra ponta não certificam o checkout. | Integrador: reconciliar arquivos com incrementos/PRs e manifesto por hash, identificar runtime/retorno e revisão aceita. Não stage global, reset, repin ou merge automático. Fechar com composição única reproduzível e checks do SHA correspondente. |
| P0 · T10.W / HOST1 | **Confirmado:** ambiente Linux não atende `run.bat`; UNC é incompatível com o `cd /d` usado pelo launcher. Ensaio inofensivo `cmd /d /c cd` informou UNC não suportado e caiu em C:\Windows. O launcher completo não foi iniciado. | Bootstrap: corrigir resolução de raiz no wrapper existente, testar espaço/Unicode/UNC e retorno de erro. Qualificar ambiente Windows gerenciado sem sobrescrever o Linux nem criar checkout alternativo. A política de um único `.venv` exige solução explícita da composição, não misturar wheels Windows/Linux no mesmo ambiente. |
| P0 · T20.30-HOST1 | **Confirmado:** `bootstrap-dev.ps1` aborta em WIP e sugere outra Workspace/checkout; default aponta para outra raiz. `windows.ps1` tenta criar `.venv` quando falta o Python Windows. | Corrigir o wrapper oficial para reconhecer/reutilizar a raiz registrada, preservar WIP e diagnosticar plataforma do ambiente. Impedir criação incidental de segundo checkout e instalação sobre venv incompatível. Provar preparação e segunda execução idempotentes com configuração preservada. Não executar o bootstrap atual com `-Yes` nesta composição. |
| P0 · BD03–05 / T13.1 | **Aceite pendente:** identidade de feed/instrumento, broker versus UTC, calendário/DST, sessões e vigência das especificações precisam ser comprovados na fonte Windows escolhida. Inventário BD01 ainda registra limites de offset fixo e calendário. | Dados: extração curta autorizada, bruto e snapshot separados, duplicação/retomada/revisão/gap/barra parcial, passagem de sessão e símbolo inexistente. Provar janela e proveniência sem converter horário desconhecido por suposição. Não generalizar EURUSD para B3. |
| P0 · T2.6 / EXTR-01 | **Aceite pendente:** paridade de extração Windows×Linux não consta concluída no checklist. | Dados: mesma conta/feed/período/especificação, comparar barras/ticks exportados pelo percurso oficial. Explicar diferenças de revisão do broker; usar QA próprio, não copiar banco pessoal como fixture. Registrar linhas, duplicatas, hashes e diferenças. |
| P0 · IND-CCI-01 / IND-AGGR-01 | **Correção implementada, aceite nativo pendente:** handoff recente relata regressões e comparador; coleta `iCCI`/`CopyBuffer` ainda faltava. | Dados/HADOUKEN: comparar referência independente e buffers reais com aquecimento, barra corrente/nova, AS_SERIES, revisão, vazio e precisão. Preservar versão dos cálculos antigos; não reinterpretar séries antigas como corrigidas. |
| P0 · E14-CALC-01 / E14-METRIC-01 | **Aceite pendente:** comissões, fills/parciais/ciclos e métricas de histórico tiveram corretivos; testes simulados não fecham apuração nativa. | Operacional/Analytics: reconciliar ordens→deals→posição→ciclo, comissão/fee/swap e caixa líquido. Distinguir indisponível/desatualizado de zero. Comparar Tester, Replay e recibo do App no mesmo pacote/fonte, incluindo resultado negativo. |
| P0 · E14-SCOPE-01 / F04 | **Confirmado:** `HDK_SafeExecutor.mqh` recusa nova exposição, alteração e fechamento netting/exchange sem contribuição reconciliada; `HDK_PortfolioRiskGuard.mqh` também bloqueia reserva. | HADOUKEN/Safety: qualificar posse por contribuição e escopos conta/Magic/símbolo/plan, exposição alheia, reserva e risco agregado. Preservar a recusa até prova específica. Isso é bloqueio real para o perfil que dependa desse modo, não uma opção visual a habilitar. |
| P0 · E14-EXEC-01 | **Aceite pendente:** mapper atual declara intenção antes do efeito, sem reenvio automático, reconciliação e recusa de reentrada na mesma barra após fechamento; identidade revisada não concede aceite nativo. | Operacional: resultado incerto, parcial, rejeição, timeout, reinício do processo próprio, fechamento/reversão e recuperação sem duplicata. Registrar eventos persistidos e saldo final; não usar corte de energia ou sessão alheia como QA. |
| P0 · T19.23/29 / SA-01-CONTRATO/PLOT | **Aceite pendente:** Triplo1.02/S092.18 têm pacotes e instrumentos de conferência, mas sessões Wine recentes esgotaram300s antes do Tester. Compilação isolada concluída não prova execução. | Integração Windows: primeiro Triplo desarmado (consenso correto, zero entradas), depois armados, plot on/off, S09 e par Tester/Replay. Exigir `.ex5`, log final, inputs lidos, sinais/decisões/fills/caixa/primeira divergência e limpeza da própria lease. Sem aumentar timeout para mascarar causa. |
| P1 · BD06–14 / BD18–20 | **Aceite parcial:** registry, store, consultas e snapshots existem. Inventário BD01 contém contratos/buffers/consumidores pendentes e itens históricos que exigem reconciliação, não contagem cega de indicadores. | Dados: matriz por variante: fórmula/seed/unidade/disponibilidade, dependências, persistência incremental, revisão, consulta e consumidor. Testar prefixo causal, TF superior aberto, ausência versus zero, invalidação localizada e restauração; qualificar apenas o conjunto usado, manter restante explícito. |
| P1 · BD10 / MEN / fontes | **Limite de capacidade:** agressor/book/fila, volume real, social/português e calendário oficial dependem de fonte e semântica específicas; existência de coluna não comprova sinal utilizável. | Dados: recusar uso sem capacidade e provenance; obter fonte pertinente quando o preset depender dela. Fontes opcionais não bloqueiam primeiro Ultra que não as use. MEN não recebe validade preditiva por quantidade de menções. |
| P0 · T13.4-M1 / P04–P07 / ACEITE-M1 | **Aceite pendente:** evidência histórica registra zero padrões aprovados e custo calibrado depois do período; grade H1 foi corrigida, mas isso não resolveu elegibilidade econômica. | Estatística/Dados: congelar plano temporal, suporte pareado, custos vigentes antes do período, amostra e tentativas. Executar pipeline real até recibo de rejeição/aprovação. Não transformar zero aprovados em defeito nem ajustar reserva para obter aprovação. |
| P1 · LAB-07 / F18 | **Limite confirmado pelo contrato:** Pearson entre retornos coincidentes é relação descritiva; não comprova previsão entre TFs. | Estatística: se usado na estratégia, formular alvo/horizonte, maturidade, treino/validação e estabilidade OOS com ausências explícitas. Mostrar associação e previsão como resultados distintos. |
| P1 · T19.24/27/28 / T20.12–14 | **Implementação/integração a completar:** protocolo comum de tentativas/reserva e seletores Defensivo/FatorLucro devem consumir custos/equity/exposição compatíveis; nomes de categorias não são seletores entregues. | Pesquisa/Presets: objetivos/desempates/limiares prévios, orçamento/checkpoint, trilha de descartes e consultas à reserva, ruído/vazamento deliberado. PF indefinido, zero trades ou pouca amostra não aprovam. Reusar protocolo existente, sem segundo motor de experimentos. |
| P1 · T19.52a→52 | **Confirmado:** `conditional_policy.py` faz estudo sobre runs já gravados, separa procura/validação/reserva, limita uma posição e declara `study_only`, `native_set_equivalent=false`, `activation_authorized=false`. Não é Ultra executável. | Presets: predicados tipados, regras/contextos causais, validade/precedência/fallback e limites de busca. Congelar política e protocolo completos; o piso PF2 do contrato não é garantia de desempenho. |
| P1 · T19.52 / integridade da pesquisa | **Investigação delimitada por código:** estudo escolhe menor `run_id` por robô, calcula cortes pela distribuição de entradas e hash da política sem incluir explicitamente frações/piso PF/datas dos cortes/manifestos completos. Não há verificação explícita de comparabilidade conjunta nesse módulo; `aggregate.frame` valida referências/deduplicação, o que não prova mesma moeda/risco/período. | Reproduzir com runs de custos/moedas/janelas/revisões diferentes e protocolos distintos. Vincular hash do experimento e snapshots, explicitar escolha de run e cortes congelados. Não alterar hash de política por conveniência: separar identidade de política e identidade da avaliação. São riscos de promoção, não fraude ou bug financeiro já reproduzido. |
| P1 · T19.24/53 | **Lacuna estrutural confirmada:** roteamento de trades fechados não recalcula sinais seguintes, sizing/capital, margem e efeitos de gestão. Compiler `PresetIdentity` aceita apenas draft/Default/Manual. | Estratégia/Operacional: implementar política no executor existente e backtest completo causal, preservando SL físico e intenções. Testar checksum/schema, regra ausente, restart, transição intraposição, parcial, stop fixo/trailing e orientação sem dupla inversão. Só então exportar Ultra. |
| P1 · T19.54 | **Pendente:** comparação final/UI/exportação da política Ultra executável não é entregue pelo estudo. | Presets/Laboratório: Default e referências estáticas equivalentes, OOS/reserva intocada, custos adversos, estabilidade por período e amostra, timeline de regras/fallback e ida/volta do pacote. Reprovação e nenhum candidato são saídas válidas. |
| P1 · T19.17–22/56–57 / F04 | **Capacidade por perfil:** mapper já descreve SL pontos/ATR, TP planejado/pós-fill, trailing, BE, alvo virtual e precedência. Não é ausência de todo o operacional. Gradiente multiordem e recuperação exigem capacidades adicionais. | Operacional: matriz módulo×perfil×modo de conta, ticks/lot step/min stop/freeze, gap/slippage, parcial inviável, risco agregado/margem e pendências. Separar R planejado do R efetivo e não transferir aceite S09 para todos os Experts. |
| P1 · LAB-05/06 / T20.24–25 | **Desempenho ainda não aceito:** handoff recente registra três ativos no prazo e seis no limite60s, embora12 runs/126 células reabram. | Executor/Dados: medir por estágio (planejar/snapshot/gravar/executar/comparar), eliminar recomputação sem mudar contrato; cancelar/retomar com resultado parcial íntegro. Um worker; não aumentar paralelismo ou prazo sem evidência. |
| P1 · ACEITE-UI / LAB-01…10 / T10.W | **Aceite físico pendente:** Tk/Xvfb e CI não certificam Windows físico. | UI: launcher habitual→Análise/Robôs/Laboratório/Dashboard; dois temas, DPI96/144 e escala real, monitor/dock, teclado/foco/rolagem, acentos, resize, cancelamento e troca de base. HTML offline e navegador/porta ocupada. Acessibilidade precisa percurso próprio, não só screenshot. |
| P0/P1 · ACEITE-DRIVE / BD03 / HOST1 | **Aceite parcial:** cloud/drive_backup/asset_package/package_plan existem; não repetir alegação antiga de que tudo está por implementar. Proteção da base principal e recuperação Windows continuam aceites próprios. | Recursos/Dados: inventário por família e dependência; SQLite consistente, reunião com origens/conflitos explícitos, PostgreSQL dump/restore próprio; NTFS/arquivo aberto, interrupção e rollback. OAuth/conta/pasta privados pelo fluxo oficial; checksum remoto e restauração isolada. Nunca copiar SQLite ativo, cofre ou sessões entre sistemas. |
| P0 antes de PLAY · T8.7 / ACEITE-NATIVO | **Aceite separado:** autorização histórica de02/11 era limitada a janela/contexto; pedido atual de análise não autoriza PLAY. STOP pode desanexar sem liquidar em caminhos legados. | Operacional/proprietário: após gates, roteiro DEMO com conta/preset/risco/janela/supervisão e encerramento explícitos. Testar stop/kill/limites e exposição remanescente no percurso qualificado. Nenhuma operação REAL. |
| P1 · BD22 / T19.49 | **Documentação defasada confirmada:** `docs/sdk/exatidao.md` ainda afirma que qualidade do Tester não é lida, enquanto handoff/código recente têm auditoria de ticks. Roadmap contém fases antigas marcadas inexistentes com incrementos posteriores. | Integrador: reconciliar por versão/recibo, conservar histórico e esclarecer estado vigente. Não reabrir correção concluída nem fechar pendência por texto otimista. Revisar checklist Windows ao final de cada aceite. |

### Sequência executável no Windows

1. **Composição e proteção (P0):** identificar WIP/PR/manifesto/runtime; confirmar recuperação independente e acesso ao acervo. Resolver entrada UNC, wrapper contraditório e ambiente por OS na mesma composição antes de qualquer instalação. Não executar sugestões antigas de escolher outra Workspace.
2. **Admissão do host e instalação:** medir CPU/processos, RAM, paginação, I/O, disco persistente/temporário e sensores; reservar pelo menos4GiB e25% de RAM e10GiB de disco além do pico previsto; adiar a90°C ou sem evidência térmica. Um job, reavaliar15–30s. Downloads acima70MB preparados para o usuário. Instalar/reutilizar pelo wrapper corrigido, doctor offline e idempotência; depois descoberta MT5 sem operar.
3. **Dados e matemática:** QA pequeno no Windows, extração/retomada/paridade e qualificação CCI/AggrVolBalance; corrigir divergências antes de usar métricas para selecionar presets. Fechar identidade/calendário/custo das entradas usadas.
4. **Nativo e operacional:** compilação e Triplo desarmado; depois pares armados Tester/Replay/S09, caixa e eventos. Exercitar E14 cálculo/métrica/escopo/recovery e capacidades necessárias. Netting, carteira e gradientes ficam fechados enquanto seus gates falharem.
5. **Percurso integrado:** Default→Manual→exportar→reabrir→Tester→Replay→Laboratório/Dashboard, com mesma identidade; Windows UI, cancelamento, lotes e recuperação. Validar fonte/artefato exatos; suites pertinentes serializadas. Atualizar checklists existentes.
6. **Pesquisa e seleção:** custos e plano point-in-time, protocolo único de tentativas, reserva intocada, backtest causal e seletores automáticos; produzir evidências de reprovação quando não houver candidato. Não otimizar apenas para alcançar PF2.
7. **Ultra T19.52→53→54:** fechar contrato da política por Expert/ativo/TF, executar no HADOUKEN, conferir decisões Python/nativas e pacote, avaliar OOS/custos/risco e UI. Gestão intraposição ou vários setups só se capacidades declaradas e qualificadas.
8. **Aceite operacional separado:** DEMO sob roteiro próprio e autorização contextual; depois preservar incremento revisado, SHA remoto/PR e retorno. Merge/release/REAL não fazem parte desta análise.

### Definição verificável de fechamento

Plataforma pronta para gerar Ultra significa: composição reproduzível; dados e cálculos qualificados para as capacidades usadas; cadeia estatística sem futuro e com custos elegíveis; pacote que preserva toda a política; executor que prova proteção/posse/reconciliação; ida/volta e recuperação verificadas no Windows. Não significa completar todas as propostas opcionais nem garantir lucro. Catálogo ampliado, Market, IA, novas fontes/representações e carteira avançada são P2 quando não utilizados pelo preset escolhido; continuam rastreados nas fichas originais. Tornam-se dependências obrigatórias se a política os consumir.

Para cada aceite registrar: App/HADOUKEN/Safety e hashes da closure/binário/pacote; Windows/Python/MT5 e modo de conta; configuração de QA sem segredos; fonte/snapshot/recorte/custos; comando pelo launcher; observado versus esperado; primeira divergência; orçamento/pico/duração; limpeza própria e retorno. Ausência de evidência mantém o gate aberto. Um Ultra tecnicamente válido pode ser estatisticamente reprovado; a plataforma deve produzir essa resposta honestamente.

Validação desta passagem: leitura cruzada de instruções, plano/roadmap/handoff, inventário Dados, contratos F19, fontes Python/MQL5 e testes associados; Git local read-only; PR/CI remoto acima; sondagens leves do host. Não executados pytest, doctor ou MT5 por ausência do runtime Windows gerenciado e por escopo documental. O conteúdo anterior deste handoff foi preservado byte a byte ao acrescentar esta seção. Publicação deste relatório permanece pendente da reconciliação: o handoff já contém WIP anterior e HEAD antigo, e a PR351 tem objetivo específico de perfis; não transportar todo esse histórico para uma PR de auditoria nem commitar alterações alheias. O relatório está preservado no handoff existente, sem novos bancos, árvores ou artefatos de QA.

## Execução da auditoria no Windows — 18/11/2026 (HOST1/T10.W, T20.30-HOST1)

Composição física confirmada: `E:` é o NVMe de 1 TB do Deck (btrfs, subvolume `@home`, montado pelo WinBtrfs); `E:\misael\Projects\MT5X` é a mesma raiz de `/home/misael/Projects/MT5X`. O caminho UNC `\\wsl.localhost\Debian\mnt\deck-disks\c3221fc6-…` não estava montado nesta sessão (diretório vazio no WSL, disco não anexado); nada foi montado.

Reconciliação por conteúdo (sem reset, stage global, troca de branch ou alteração do índice real; árvore do WIP capturada num índice privado):

- App HEAD `550b6d5f` em `codex/launcher-classes`; o código do WIP equivalia à ponta da [PR351](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/351) `cd03aae2`. A ponta publicada mais nova é a [PR352](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/352) `39bbf6d6` (seis checks de produto verdes; ainda não citada na auditoria de 17/11). Todos os arquivos de código do WIP correspondiam a blobs publicados e anteriores à PR352; o delta `cd03aae2..39bbf6d6` (8 arquivos) foi aplicado ao WIP sem conflito. `docs/PLANO_DE_TRABALHO.md`, `docs/ROADMAP.md`, `docs/continuidade.md` e `docs/propostas/ANALYTICS_OPERACOES.md` do WIP têm linhas que não correspondem a nenhum blob publicado: preservados como estão e não publicados.
- HADOUKEN `d3dd3a5e` = remoto da [PR92](https://github.com/Misael-art/projeto-indicadores-mt5/pull/92) (o 404 da auditoria era do conector; `gh` lê a PR), com WIP alheio (3 documentos apagados, 5 não rastreados) preservado. Safety `27b85992` detached, limpo. Pins iguais ao manifesto.
- Prévia registrada `state/development-preview.json` continua `2b3b274d` (Linux); nenhum runtime Windows instalado. Na raiz existe um arquivo de nome acidental `)\n…panel.strategy.set(Sem` (lixo de shell de sessão anterior), preservado.

Correções do wrapper/launcher (incremento desta seção):

- Resolvedor único `mt5_extracao/bootstrap/paths.py` (`environment_dir`, `venv_platform`, `config_path`): `.venv` é do sistema que o criou. Com `.venv` Linux, o Windows usa `.venv-windows` e `config\config.windows.ini`, semeado do `config.ini` sem caminhos do terminal/Wine/ponte/bibliotecas nem absolutos POSIX; `.venv` misturado é recusado. App, CLI, conector, ponte, doctor, configurador, `setup_development.py`, `windows.ps1`, `run.bat` e `bootstrap-dev.ps1` seguem o mesmo critério. `linux.sh` recusa `.venv` do Windows.
- `bootstrap-dev.ps1`: Workspace derivada da própria composição, recusa outra Workspace sem criá-la, aceita WIP (contado no recibo, nunca alterado), sem mensagens de "escolher outra Workspace"; toolchain WinLibs (>70 MB) passa a ser instrução preparada (saída 3), não download automático. `setup_development.py` aceita WIP, registra `wip_entries_preserved`/`composition_pinned`, grava `setup-windows.json` quando o `.venv` é do Linux.
- `run.bat`: `pushd` (UNC, espaços, Unicode), escolha do ambiente sem usar o `.venv` Linux e política de consulta local como no `run.sh`.
- `windows.ps1`: instalador oficial do python.org primeiro, assinatura Authenticode da PSF conferida, `PrependPath=0`; recusa terminal com `%LOCALAPPDATA%` virtualizado por app empacotado.

Bloqueio externo encontrado (real, reproduzido duas vezes): esta sessão roda dentro do app Claude empacotado (MSIX). O Windows redireciona as escritas em `%LOCALAPPDATA%`/`%APPDATA%` para `…\Packages\Claude_pzs8sxrjxfjjc\LocalCache`. O MSI do Python 3.11.9 falhou com `0x80070003` (o serviço do Windows Installer não vê o pacote em cache), também fora do sandbox da ferramenta. Por isso não há `.venv-windows`: preparação, segunda execução idempotente, launcher GUI/CLI com ambiente e doctor offline real ficam pendentes. O mesmo redirecionamento invalida QA nativo do MT5 iniciado daqui (pasta de dados em `%APPDATA%\MetaQuotes`). Ação do proprietário: em terminal comum ou pelo Explorador, rodar `repos\app\install.bat -Yes -NoMT5` (ou `bootstrap-dev.ps1 -Yes`); depois instalar o WinLibs pela linha preparada pelo wrapper. O instalador baixado (26.216.840 B, SHA256 `5EE42C4E…5FDDE`, assinatura válida) ficou no cache virtualizado do app, não no cache real.

Validação: regressões novas `tests/test_platform_environment.py` (resolvedor, semente, configure idempotente, `run.bat` real em espaço/acentos/UNC `\\localhost\C$`, `bootstrap-dev.ps1` real com WIP/recusa/WinLibs) e contrato de WIP em `tests/test_development_host.py`: 31 aprovados, 2 pulados (instalador Linux exige POSIX). Bootstrap/config/doctor/ponte/conta/política de launch: 48 aprovados, 31 pulados, 1 falha só por `matplotlib` ausente no harness. Harness temporário: Python 3.14.10 do sistema + pytest/dependências mínimas instalados no scratchpad desta sessão, sem cache e fora do projeto; não é o ambiente gerenciado e não certifica 3.11. Percursos reais na composição: `bootstrap-dev.ps1` sem `-Yes` (raiz `E:\misael\Projects\MT5X`), recusa de `-Workspace C:\Users\Deck\Projects\MT5X` (pasta não criada), `setup_development.py --plan` (ambiente `.venv-windows`), `run.bat` local e via `\\localhost\E$` (mensagem de instalação, código 1, sem usar o `.venv` Linux), instalador recusando o terminal virtualizado. `.venv` Linux (16.718 arquivos) e `config/config.ini` com impressão digital idêntica antes/depois. Sintaxe PowerShell 0 erros; `bash -n linux.sh` ok; ShellCheck indisponível no host (fica com o CI). Host: 14,85 GiB, 7,0–7,4 GiB disponíveis, sem paginação, 49–58 °C sem throttling, E: 53 GB e C: 58 GB livres.

Achado lateral: com Python 3.14 `robots.backtest._execution_code_digest` falha (`marshal`: "unmarshallable object"), deixando execuções sem `cache_eligible`. Fora da faixa suportada 3.11–3.13; registrar antes de ampliar a faixa.

Próximo passo: o proprietário roda o instalador fora do app; então repetir aqui `install.bat` (idempotência), `doctor --development --no-probe`, `run.bat` GUI/CLI com base de QA separada e a suíte serializada em 3.11; só depois descoberta MT5 (BD03–05/T2.6) a partir de sessão não virtualizada.

## T19.52 — identidade da avaliação do estudo condicional — 18/11/2026

Resolve a "investigação delimitada" da auditoria (seleção de run, cortes temporais e identidade completa) em `presets/conditional_policy.py`, sem alterar regras, piso ou o parecer estatístico: `policy_id` inalterado; novo `evaluation` com `evaluation_id` (política + frações/instantes/origem dos cortes + piso + mínimos + capital + escolha de run + proveniência por run), `comparability` e `promotion_blockers`. Moedas declaradas diferentes recusam o estudo; proveniência ausente, manifestos diferentes, run sem operações na reserva ou escolha implícita entre runs repetidos bloqueiam a promoção mesmo com "elegível". `cuts=` reaplica cortes congelados. `mt5x trades router` mostra avaliação, cortes, modo de escolha e bloqueios. Contrato em PILARES §8.5.

Validação: `tests/test_conditional_policy.py` 10 aprovados (5 novos) no harness temporário Python 3.14 do Windows. `tests/test_trade_repeated_runs.py` não roda nesse harness (fixture exige `cache_eligible`, que o 3.14 não produz — achado registrado acima); fica com o CI 3.11. Não é Ultra executável: T19.53 (política no HADOUKEN e backtest causal completo) e T19.54 continuam pendentes e dependem dos aceites nativos.

## Windows físico — instalação, MT5 nativo e suíte completa — 18/11/2026

Continuação das seções anteriores, por instrução do proprietário de instalar e validar tudo. Recibos em `MT5X/state/qa-tmp/windows-20261118/` (fora do Git). PRs empilhadas em draft, sem merge: #353 → #354 → #355 → #356 → #357 → #358 → #359 → #360 → #361 → #362.

**Execução fora do contêiner do app:** o app Claude virtualiza `%LOCALAPPDATA%`/`%APPDATA%`. Instalação, launcher, MT5 e suíte rodaram em processos criados pelo provedor WMI (`Win32_Process.Create`, mesmo usuário). Uma sonda confirmou gravação no caminho real.

**Ambiente Windows (HOST1/T10.W):**
- `windows.ps1 -Yes -NoMT5`: Python 3.11.9 por usuário (PSF, PATH inalterado), `.venv-windows`, `config.windows.ini` semeado sem caminhos Linux.
- 2ª execução idempotente: `pip freeze` idêntico, configuração byte a byte igual com marcador preservado.
- `requirements-dev.txt`, `pip check` limpo; `doctor --development --no-probe` sem FALHA.
- `run.bat --cli` (`status`/`tables`) local e por UNC com `qa.ini` próprio.
- GUI `run.bat --offline` aberta (`gui-window.png`) e encerrada limpa.
- `.venv`/`config.ini` do Linux com impressão digital idêntica do início ao fim.

**MT5 nativo:**
- `install.bat -Yes`: `mt5setup.exe` oficial (assinatura MetaQuotes válida); licença aceita pelo proprietário na tela; MT5 em `C:\Program Files\MetaTrader 5` (build 6324).
- Achado corrigido (#359): `Start-Process -Wait` esperava o terminal aberto pelo setup; a instalação ficava parada até o MT5 ser fechado.
- Reexecução: configuração, atalho e doctor completo sem FALHA.
- Conta demo MetaQuotes-Demo (hedging, dados fictícios do projeto) aberta pelo comando oficial, com autorização explícita do proprietário.
- Achado corrigido (#360): o `tasklist` em cp850 com `PYTHONUTF8=1` devolvia `stdout None` e derrubava o comando.

**Compilação nativa (T8.4):**
- Fontes instalados na pasta de dados real (`%APPDATA%\MetaQuotes\Terminal\D0E8…\MQL5`, sem modo portátil), código de saída coerente.
- `HDK_SuperATRTriplo` e `HDK_HADOUKEN_Painel`: 0 erros, 0 avisos.
- `HDK_S09_Referencia`: 36,7 s sem contenção. O estouro anterior de 300 s ocorreu com a suíte rodando em paralelo; o prazo não foi alterado para isso.
- Na sessão do Tester o Triplo compilou em 110–221 s, com o prazo oficial de 600 s (PR352).

**Tester (T19.23/29, SA-01):**
- Duas sessões do Triplo desarmado pelo `PresetExecution` (pacote `b8daf176`, seed `2026-10-09`, EURUSD M5, ticks reais).
- Ambas pararam porque o próprio MT5 cancelou o download preliminar de ticks: 202611 completo e 202610 a 81%, em cerca de 13 s, a 1,3–2,8 MB/s, ping de 280 ms pelo Access Point BR 1. Depois veio "no history data, stop testing".
- O App não interrompeu (`process.log` vazio, sem prazo excedido).
- Mesmo sintoma do Wine: o bloqueio não é do Wine.
- Aceite (consenso, zero entradas, caixa) continua pendente. Trocar o modelo do pacote mudaria o aceite e não foi feito.
- Achado corrigido (#361): com o terminal principal aberto, a cópia do `.hcc` falhava com `shutil.Error` cru; agora a sessão para com a ação "feche o terminal ou `--close-terminal`".

**Dados (BD03–05/T2.6):**
- Extração real EURUSD M5 de 10–17/11 pelo launcher na base de QA: 1.728 barras (6 pregões × 288) e 0 duplicatas após duas execuções.
- Achado corrigido (#362): a 1ª estratégia de conexão passava a pasta do MT5 e o tratador de IPC encerrava o terminal desta instalação. Agora o `terminal64.exe` vem primeiro, como já era no Wine.
- Em aberto, contrato BD03: `--from/--to` chegam sem fuso ao `copy_rates_range` e são interpretados no fuso do host (GMT-3). Barras gravadas de `2026-11-10 03:00` a `2026-11-18 02:55`. Confirmar a base de tempo prevista antes de qualquer correção; nada foi alterado por suposição.
- Paridade Windows×Linux não executada (o Linux não está acessível nesta sessão).

**Achados de ambiente que distorcem medições locais:**
- `C:` é cartão SD: commit SQLite ~400 ms contra ~25 ms no NVMe. Suítes locais usam `TEMP`/`--basetemp` no NVMe.
- O contêiner do app deixa testes até 5× mais lentos.
- A raiz `repos\app` fica intermitentemente inacessível: abrir o diretório retorna `ERROR_SHARING_VIOLATION`, por até ~5 min, sem detentor visível ao Restart Manager. Afeta import e `git status`; o trabalho seguiu com uma junção de import.
- Concorrência alheia constante: Fôlego (PowerShell ~0,7 núcleo) e ChatGPT/Codex.

**Correções de código desta etapa:**
- #357: padrão de configuração pela raiz efetiva; teste de lease aguardando o interpretador do venv.
- #358: configuração lida da pasta corrente primeiro; `bootstrap-dev.ps1` em UTF-8. Com acentos em console cp850 o wrapper falhava; a regressão reprova a versão anterior.
- #356: produtores sem consultas pandas por dependência (59,4 s → 16,4 s).
- #359–#362: descritos acima.

**Ainda pendente:**
- Aceite do Tester (ticks), S09/plot e o par Tester/Replay, que dependem do mesmo download.
- E14 nativo, DEMO operacional com roteiro próprio (não autorizado nem executado), paridade Windows×Linux, percurso integrado completo, DPI/teclado físico.
- T19.53/54.
- WinLibs (g++, >70 MB) para os testes que compilam headers MQL.

**Suíte completa local:** Windows 11 físico, Python 3.11, fora do contêiner, um arquivo por processo, temporários no NVMe.
- Resultado bruto: 418 arquivos; 383 com junit = 4.691 testes, 37 falhas, 212 pulados. Outros 38 arquivos foram abortados por timeout (no Windows o `pytest-timeout` encerra o processo) ou parados manualmente.
- Corrigidas (#357/#358): `test_data_paths` e `test_offline_startup`, regressões da #353 no `.venv` dual.
- Corrigidas (#363): `test_restore_*`/`test_resources`/`test_backup_retention_identity` (30 falhas por recibo não regravado). Fora do contêiner: 95/95.
- Ambiente: `test_native_inputs` sem g++ (WinLibs >70 MB, ação do proprietário); `test_process_management` viu o terminal MT5 real aberto naquele instante.
- Intermitentes: `test_panel_server`, `test_ui_dashboard` e o lease filho que não imprimiu em 15 s.
- Timeouts: concentrados em produtores Analytics, backtest em lote, UI Tk e subprocessos da CLI. Coincidiram com o host saturado por outros apps, que chegaram a ~5 núcleos (WebView, ChatGPT, Fôlego, `git`/`pwsh` de outro agente).
- A reexecução isolada foi interrompida para respeitar a política de recursos. O CI Windows de cada PR (runner limpo) é a referência de tempo: #353–#359 e #361 com todos os checks verdes; #354 e #360 tiveram falhas intermitentes de UI/tempo que passaram no rerun ou na PR seguinte.
- Recibos: `state/qa-tmp/windows-20261118/suite/` (logs, junit, `summary.tsv`) e `rerun/`.

**Próximo passo:**
1. Proprietário: decidir o tratamento do download de ticks reais (rede ou outra conta/servidor) para fechar o Triplo desarmado, o par Tester/Replay e o S09; instalar o WinLibs.
2. Contrato BD03: confirmar a base de tempo de `--from/--to`.
3. Repetir a suíte em host sem saturação.
4. Revisar e mesclar a pilha #353→#363 na ordem.
### Atualização — rede estável, limpeza e atalhos (18/11/2026, noite)

- **Tester do Triplo desarmado, 3ª sessão (`8a53fb97`):** com a rede estável os ticks reais de outubro baixaram ("real ticks begin from 2026.10.12"). O Expert iniciou (`HDK_TRIPLE_START`, `MT5X_NATIVE_MODE tester=1 demo=1`) e emitiu as barras `HDK_TRIPLE_BAR`. Compilação dentro da sessão: 294,5 s, sem erros.
  - A sessão foi interrompida pela guarda de recursos da bancada: RAM livre de 4.036 MiB, abaixo da reserva obrigatória de 4 GiB, com MT5, Tester e apps de terceiros abertos. Temperatura máxima 63 °C.
  - O bloqueio de ticks deixou de ocorrer com a rede estável. Aceite (consenso, zero entradas, caixa) pendente de nova sessão após reiniciar, com mais memória livre.
- **WinLibs/g++:** não instalado. Não há `g++.exe` no `C:`, nem `WinGet\Packages` no perfil, nem cópia redirecionada para apps empacotados. Instalar num PowerShell comum, fora do app Claude:
  `winget install --id BrechtSanders.WinLibs.POSIX.UCRT --exact --source winget --scope user`.
- **Limpeza:**
  - Sessões nativas (1,5 GB de cópias de histórico e binários do MT5) removidas; logs, `relatorio.json`, `.ini` e `.htm` arquivados em `state/qa-tmp/windows-20261118/tester/sessoes/`.
  - Rascunhos de commit e temporários removidos. Bancada consolidada em `state/qa-tmp/windows-20261118/bancada/` (`run_native.cmd`, `executar_preset_windows.py`, `run_suite.sh`, `rerun_failed.sh`, `watch.ps1`, junção de import `pyroot`).
  - Resíduos do instalador na pasta privada do app Claude removidos.
  - O arquivo de nome inválido na raiz (`)` + quebra de linha + `panel.strategy.set(Sem`, de sessão Linux anterior) não pode ser endereçado pelo Windows. Remover no Linux com `rm -i -- MT5X/)*panel.strategy.set*` após conferir com `ls -lb`.
- **Atalhos:** "MT5 Extração" na área de trabalho e no menu Iniciar, pelo comando oficial `bootstrap shortcuts`. Apontam para `repos\app\run.bat`, com ícone do app. Usam `config.windows.ini` e a base principal `database/mt5_data.db`.
- **Após reiniciar:**
  1. `run_native.cmd triplo-desarmado backtest <log>` da bancada, com ≥4 GiB livres antes e durante.
  2. Plot ligado/desligado e S09.
  3. Reexecução isolada dos arquivos da suíte com timeout (`rerun_failed.sh`).
  4. Conferir se o bloqueio intermitente da raiz `repos\app` persiste.
### WinLibs e preparação de desenvolvimento completa (18/11/2026, noite)

- **Instalação:** o `winget` deste Windows não executa nada nem fora do contêiner (o alias de App Installer encerra sem saída), por isso a tentativa anterior não instalou.
  - Instalação equivalente à do winget, autorizada pelo proprietário: pacote x64 oficial `winlibs-x86_64-posix-seh-gcc-16.2.0-mingw-w64ucrt-14.0.0-r3.zip`, 263,6 MB, SHA256 `3C69305081E4A647144339A936244FCF7D3D9E558319E1AD7BE2CD2886D4B6A1`, igual ao manifesto `microsoft/winget-pkgs` 16.2.0-14.0.0-r3.
  - Pacote mantido em `MT5X/downloads/`. Extraído no layout do winget (`%LOCALAPPDATA%\Microsoft\WinGet\Packages\BrechtSanders.WinLibs.POSIX.UCRT_…\mingw64`); a pasta `bin` foi acrescentada ao PATH do usuário, preservando as entradas existentes. `g++ 16.2.0` responde.
- **`bootstrap-dev.ps1 -Yes`:** `development_ready`. Testes focados 47 aprovados / 2 pulados, `check_repo` do HADOUKEN com 174 arquivos OK, `pip check` limpo. Testes Super ATR do HADOUKEN: 2 OK em 26,8 s; a 1ª execução a frio do toolchain estourou 30 s.
- **Defeito corrigido nesta etapa:** os logs de etapa do `setup_development.py` usavam nomes fixos e sobrescreveram, na 1ª execução Windows, os recibos Linux de 26/10 em `state/` (`dependencies.log`, `doctor.json`, `hadouken-check.log`, `imports.log`, `pip-check.log`, `super-atr-tests.log`, `tests.log`).
  - Os originais Linux foram perdidos (eram recibos históricos pequenos, regeneráveis pelo setup Linux). Os conteúdos Windows foram movidos para `state/windows/primeira-execucao/`.
  - Com a correção, o sistema não dono do `.venv` grava em `state/<sistema>/`. Há regressão para isso.
- **Testes do App que dependem de g++:** fora do contêiner, 38/38 funcionalmente. `test_super_atr_reference` leva 127 s e 84 s em dois testes neste APU, acima do limite de 60 s; medido com limite diagnóstico de 300 s, sem mudar o gate.

### Tester/Replay nativo após reiniciar (19/11/2026)

Bancada: `state/qa-tmp/windows-20261118/bancada/` (`run_native.cmd`, `run_sequence.ps1`, `executar_preset_windows.py`), pacotes da qualificação Wine (`corretivos-20261113/qualificacao-wine`, HADOUKEN `d3dd3a5`), EURUSD M5, 12–14/10/2026, "cada tick com ticks reais", conta MetaQuotes-Demo, MT5 build 6324. Recibos `tester/run-<sessão>.json`; resumo `tester/sequencia-{1,2,3}.tsv`.

**Resultados (todos aceitos pela prova oficial do `PresetExecution`):**

| Caso | Modo | Sessão | Resultado |
|---|---|---|---|
| Triplo desarmado | backtest | `bb9f2829` | `contract_only`: 863 barras e 576 decisões reconciliadas com o header fixado (g++ 16.2.0), 146/138/292 consenso +1/−1/0, 0 entradas, caixa conciliado |
| Triplo plot desligado | backtest | `9924ae99` | `reconciled_backtest`: 14 posições, +198,88, 278 tentativas, 5 saídas por sinal, 28 fills |
| Triplo plot desligado | replay visual | `77e5f130` | `reconciled_replay`, `reference_match=True`, ciclo funcional, 0 pontos de plot |
| Triplo plot ligado | backtest | `49e67c96` | `reconciled_backtest`: mesmas 14 posições e +198,88 |
| Triplo plot ligado | replay visual | `52221529` | `reconciled_replay`, `reference_match=True`, ciclo funcional, 576 pontos de curva iguais às decisões |
| Par ligado × desligado | consulta | `tester/par-plot.json` | `compare_presentation_pair`: mesmas decisões e fills fechados, sem diferença |
| S09 referência | backtest | `96173cc6` | `reconciled_backtest`: 4 posições, −50,49 |
| S09 referência | replay visual | `a1b0f50a` | `reconciled_replay`, `reference_match=True`, ciclo funcional |

Escopo: Tester DEMO; sem operação ao vivo. `native_visual_qualified=false` e `operational_acceptance=false` continuam como a função oficial devolve. Cobertura de ticks reais do relatório consta como `conflicting` em `data_quality`; nada foi alterado para isso.

**Correções de contrato (PR #366, empilhada sobre #365)** — cada uma apareceu só com dados reais do Tester e mantém as recusas:
1. Confirmação após lacuna: o verificador Triplo exigia `confirmed_at == bar_at+TF`; o Expert confirma na abertura da barra seguinte (sexta 23:55 → segunda 00:00). Agora exige a próxima barra observada, na grade do TF.
2. TP ausente: o importador grava TP 0 do CSV como `None`; `_ledger_rows` recusava perfis sem alvo. Só perfil sem alvo lê ausência como 0.
3. Volume: o deal nativo pode ser o double vizinho do texto do CSV (`4.6899999999999995` × `4.69`); livro e fills comparam com 8 ULPs, como os verificadores.
4. Tentativa sem plano (limite de posições) imprime `stop=DBL_MAX`; só com `plan_known=0` vira ausência.

**Download de ticks:** 7 das 20 sessões (18–19/11) pararam porque o próprio MT5 cancelou o download preliminar ("no history data, stop testing"), sempre a 79–97%.
- Coincide com a varredura de pontos de acesso logo após o login (Access Point BR 1, ping 227–280 ms) e com quedas de conexão. Numa delas o IPv6 do provedor mudou no meio da sessão.
- Repetir a sessão resolveu em todos os casos.
- Semear o cache do terminal principal com o `202610.tkc` completo de uma sessão anterior (SHA256 `7D8D6D33…2A88`, mesmo servidor) não evitou o novo download.

**Ajustes só da bancada (fora do Git):**
- PATH do processo com o `bin` do WinLibs. Processos criados pelo WMI não herdaram o PATH do usuário atualizado.
- Junção `pyroot\scripts` para a referência Super ATR.
- Orçamento do lote: compilação + 1.200 s no backtest e + 3.600 s no replay. O replay visual de 2 pregões leva ~22–27 min aqui.
- A compilação dentro da sessão levou ~9 min em cada execução.

**Ainda pendente:**
- Reexecução isolada da suíte: ver abaixo.
- E14 nativo, DEMO operacional (roteiro próprio, não autorizado), paridade Windows×Linux, BD03 (`--from/--to` no fuso do host), DPI/teclado físico, T19.53/54.

**Reexecução isolada da suíte (19/11, host sem carga própria concorrente, fora do contêiner, um arquivo por processo):** 48 arquivos que falharam ou abortaram em 18/11 (`state/qa-tmp/windows-20261118/rerun/`).
- 1ª passada, gate normal (60 s por teste): 13 verdes, entre eles `test_native_inputs` (agora com g++) e `test_process_management`. Outros 30 estouraram 60 s e 5 falharam sem timeout.
- As 5 falhas sem timeout:
  - `test_restore_operations`, `test_restore_backup_identity`, `test_backup_retention_identity` e `test_resources` falharam por **MAX_PATH**. O recibo `native-state\resources\restore_receipts\restore__<64 hex>_<32 hex>.restore.json(.tmp)` passava de 260 caracteres com o `--basetemp` longo da bancada, e este Windows está com `LongPathsEnabled=0`. Com basetemp curto: 20/20, 14/14, 36/36 e 45/45 (`rerun/pass3/`).
  - `test_offline_startup`: o subprocesso `import app` passa de 30 s.
- Risco real a registrar: numa instalação em pasta profunda, sem LongPaths, o recibo de restauração pode não ser gravado. A restauração fica `unknown`/`uncertain` e não quebra em silêncio. Não foi alterado.
- **Lentidão de import:** `import app` leva 87 s a frio e 29 s a quente (`pandas` 4 s, `matplotlib.pyplot` 6,9 s). O venv está no E: (Btrfs via WinBtrfs), e o Defender em tempo real ficou ~60% de CPU durante o pytest.
  - Hipótese não confirmada: a varredura do Defender sobre milhares de arquivos pequenos.
  - Excluir a pasta do projeto do Defender é decisão de segurança do proprietário; não foi feito.
  - O CI Windows de cada PR continua sendo a referência de tempo.
- **2ª passada diagnóstica (limite 600 s, sem mudar o gate; `rerun/pass4/`):** dos 30 arquivos que estouraram 60 s, 23 passam integralmente. Exemplos: `test_analytics_profiles` 62/62 em 336 s, `test_ui_main_window` 78/78 em 39 min, `test_ui_laboratory` 25/25 em 18 min.
  - Os outros 7 têm 11 falhas, todas em esperas internas dos próprios testes com o host lento, sem asserção funcional divergente: `join(40/45)` do lote real em `test_backtest_scope_assets`/`test_scope_comparison`; `wait`/`pump` de 30 s e "Dashboard não concluiu" em `test_ui_backtest_assets`, `test_ui_backtest_scope`, `test_ui_dashboard`, `test_ui_laboratory_series` e `test_ui_super_atr`.
  - `test_offline_startup` falha do mesmo modo: subprocesso com prazo de 30 s.
  - O CI Windows de #363–#366 está verde nesses arquivos.
- **Próximo passo:**
  1. Proprietário: revisar e mesclar a pilha #353→#366 na ordem.
  2. Proprietário: decidir sobre exclusão do Defender/LongPaths (configuração de segurança/sistema, não alterada).
  3. Proprietário: confirmar a base de tempo BD03.
  4. Seguir com E14 nativo e T19.53/54.

## Revisão independente da implementação Windows — 19/11/2026

**Veredito: entrega parcial confirmada; encerramento da plataforma/Ultra não aprovado.** A preparação Windows avançou de fato, e há evidência nativa de Tester/Replay. Reconciliação de conteúdo dos incrementos está demonstrada no recorte abaixo, mas composição integral reproduzível, aceite local completo e Ultra executável continuam abertos. Esta revisão não autoriza merge, release ou operação ao vivo.

### Evidência verificada pelo revisor

- O caminho WSL cadastrado no chat está indisponível; os arquivos foram encontrados na composição existente `E:\misael\Projects\MT5X`, conforme o handoff de migração. Nenhum clone, worktree ou ambiente foi criado. O cadastro do app ainda aponta para o UNC antigo; o registro central conserva caminhos Linux.
- App permanece HEAD `550b6d5f`, branch `codex/launcher-classes`, com WIP. HADOUKEN `d3dd3a5e` e Safety `27b85992` coincidem com o manifesto. `state/setup-windows.json` diz `development_ready`, mas também `composition_pinned=false`; os estados não são equivalentes.
- Comparados os 38 caminhos alterados entre PR352 `39bbf6d6` e PR366 `807298aec1f7bf8c18642a851b8619f1efe8f720`: 37 arquivos de implementação/testes/documentação conferiram por blob; o handoff diferiu apenas por terminações de linha (diff ignorando EOL vazio antes desta revisão). Isso valida os incrementos examinados, não todos os arquivos do WIP. Há documentos anteriores não publicados e diferenças de modo executável dos scripts Linux, sem diferença de conteúdo nos cinco scripts examinados; conferir modos no Linux antes de integrar.
- PR366 consultada no GitHub: aberta, draft, base PR365 `21f79281`, ponta `807298ae`; CI `44606035035` completed/success. Não foi presumido aceite ou merge da pilha.
- Inspecionados diffs de ambiente/bootstrap, instalação, dados, estudo condicional, produtores e conferência nativa. As correções nativas têm regressões para gap de confirmação, TP ausente, volume vizinho em double e stop DBL_MAX sem plano; isso não concede aceite geral E14.
- `.venv-windows/pyvenv.cfg` confirma Python3.11.9; launcher usa `pushd`, ambiente Windows e configuração por plataforma. Teste independente no ambiente instalado: cinco testes de resolução/plataforma de `test_platform_environment.py` passaram em3,73s, nove não selecionados. Comando: `.venv-windows/Scripts/python.exe -m pytest --noconftest -o addopts= -p no:cacheprovider tests/test_platform_environment.py -k "platform_of_environment or single_system_installation or windows_never_reuses or mixed_venv" -q`, com autoload de plugins desabilitado e sem bytecode. Aviso de configuração `timeout` desconhecida devido aos plugins desabilitados; recorte leve, não execução do gate completo.
- Percurso independente `cmd /d /c run.bat --cli --help`: terminou com código0 e catálogo de comandos. Inicialização foi lenta e a captura apresentou acentos corrompidos; não certifica UX/encoding no console físico. Nenhum MT5 foi iniciado pelo revisor.
- Conferidos logs existentes de instalação, launcher, doctor e reexecução. Doctor de desenvolvimento arquivado:15OK/3avisos/0falhas; testes de setup em `state/windows/tests.log`:48passaram/2pulados. Não foram reexecutados instalador, GUI, compilação ou suítes pesadas.
- Conferidos sete recibos `tester/run-*.json`: Triplo desarmado `bb9f2829` contract_only; plot off/on backtests `9924ae99`/`49e67c96`, replays `77e5f130`/`52221529`; S09 `96173cc6`/`a1b0f50a`, todos reconciled no escopo declarado. Os três replays têm reference_match=true. `par-plot.json` declara mesmas decisões/fills, sem diferença, e preserva native_visual_qualified=false/operational_acceptance=false.
- Os caminhos originais artifact_dir foram removidos, mas os sete relatórios HTML existem em `tester/sessoes/<sessão>`. **7/7 hashes conferem** com evidence.data_quality.report_sha256 após decodificação BOM/UTF e normalização CRLF→LF, conforme o contrato de `mql.read_text`. Hash de bytes crus não é esse hash. Os sete mantêm real_tick_coverage_status=conflicting e elegibilidade unqualified; reconciliação não comprova cobertura integral de ticks reais.
- Recursos na revisão: RAM livre aproximadamente5,0–5,3GiB, paginação0 na medição, CPU69%, sensor TZ01 cerca50°C, C:40,8GB/E:55,3GB livres. Apenas leituras e sondas pequenas, sem nova carga pesada; processo Python observado com cerca70MB RSS durante import lento. Limites térmicos completos não medidos, logo sem admissão de suíte pesada. ai-memory não está disponível nas ferramentas desta sessão; registro mantido aqui, sem banco alternativo.

### Lacunas prioritárias após a revisão

| Prioridade / ficha | Constatação e encaminhamento |
|---|---|
| P1 · T19.52 / comparabilidade | **Defeito reproduzido:** `_comparability` em `presets/conditional_policy.py:125` guarda comparison_context nos records, mas não compara custos/risco/capital/motor. Dois runs com moeda USD e manifesto iguais, digests distintos e comparison_context com comissão/risco0,001 versus0,1 retornam `status='comprovada', blockers=[]`. Prova executada com DataFrame sintético no Python3.11 gerenciado, chamando a função real; nenhum banco alterado. A identidade da avaliação melhorou, mas não resolve a comparabilidade econômica. Reusar o contrato de comparação existente; ausência ou divergência material deve impedir o selo, com regressões positivas/negativas. Não alterar apenas a mensagem. O estudo continua sem autorização nativa, portanto não houve liberação de ordens por esta falha. |
| P0 · HOST1/BD21–22 | Reconciliação dos incrementos confirmada; fechamento da composição ainda parcial. Recibo tem composition_pinned=false; HEAD e manifesto App históricos não identificam o WIP. Recibos nativos ainda usam app_note='WIP equivalente a PR352+#353..#359', embora dependam de corretivos até366. Acrescentar vínculo verificável da fonte efetiva/closure aos recibos, preservar documentos locais e atualizar o cadastro Windows no registro oficial. Não exigir merge como substituto de prova de conteúdo. |
| P0 · preservação/recuperação | O agente admite sobrescrita e perda dos logs Linux originais em state/. A correção de namespace de logs existe em setup_development.py, mas não recupera evidência histórica. Não declarar preservação integral nem chamar novas execuções de restauração dos recibos originais. Procurar proteção independente existente; se não houver, manter perda explicitamente registrada. Fontes/dados não foram demonstrados perdidos. |
| P0 · BD03–05/T2.6 | CLI `_parse_date` gera datetime sem timezone e conector repassa as datas ao SDK. A janela real deslocada já consta do handoff. Resolver contrato de relógio pela documentação/implementação e teste entre fusos antes de usar o recorte em pesquisa; não inventar UTC nem depender do fuso do host. Paridade Windows×Linux permanece pendente. |
| P1 · Recursos/restore | MAX_PATH permanece risco real: LongPathsEnabled=0 confirmado; caminho de recibo inclui hashes longos. Logs pass3 confirmam20+14+36+45 testes passando com basetemp curto, o que não corrige uma instalação profunda. Corrigir/prevalidar caminho e provar recuperação sem exigir mudança global do sistema. Não tratar habilitar LongPaths ou excluir Defender como pré-requisito obrigatório para prosseguir com correções do App. |
| P1 · testes/desempenho | Suíte completa local não aprovada no gate original. Logs pass4 comprovam execução diagnóstica longa e falhas remanescentes (ex.: UI Super ATR2falhas); sucesso com600s não substitui60s. Separar contenção/armazenamento/import de defeitos funcionais com instrumentação. Causa Defender permanece hipótese, não motivo para excluir proteção automaticamente. CI verde não certifica tempo/UX no Deck. |
| P1 · Tester/qualidade | Resultado reconciliado confirmado, cobertura de ticks conflicting nos sete recibos. Investigar o conteúdo do relatório e o parser com regressão; só concluir cobertura após evidência inequívoca. Preservar os gates e os arquivos arquivados; corrigir os apontadores artifact_dir/manifesto de retenção para permitir revisão. Não repetir toda a campanha antes de determinar qual evidência falta. |
| P1 · IND-CCI/IND-AGGR/E14/aceite físico | Não foi encontrada nesta entrega nova prova que encerre todos os aceites iCCI/CopyBuffer, escopos/netting, parcial/incerto/restart E14, DPI/teclado/acessibilidade e recuperação integrada. Os pares S09/Triplo não cobrem automaticamente esses casos. Retomar as fichas com entradas controladas e execução serial. DEMO ao vivo exige roteiro/autorização próprios. |
| P1 · T19.53/54 e pesquisa | Ultra executável, backtest causal completo, protocolo/seletores e validação final OOS continuam incompletos. Corrigir comparabilidade antes da promoção; integrar política no executor e pacote existentes, sem transformar estudo sobre ledger em backtest completo. Nenhum Ultra foi certificado nesta revisão. |
| P2 · documentação/ambiente | O código implementa um ambiente por OS, mas AGENTS.md ainda descreve apenas `.venv`; reconciliar a política registrada com a solução adotada, sem apagar o ambiente Linux. Charset da CLI e leitura intermitente da raiz precisam reprodução específica. Scripts de bancada usam junção/PATH próprios: provar o mesmo percurso pelo launcher público antes de declarar integração de UX. |

### Próximo lote e limites desta validação

Prioridade: corrigir comparabilidade e contrato temporal; fechar identidade/proteção da composição e recibos; resolver restore/caminhos e obter execução local proporcional sob limites normais; concluir qualidade nativa e E14; depois T19.53/54. Atualizações de cadastro, evidência e documentação podem seguir em paralelo lógico, sem cargas locais concorrentes.

O revisor não alterou código, pins, índice ou configurações de segurança, nem criou outra árvore. Acrescentou somente esta revisão ao handoff, preservando o prefixo anterior. Não fez push de todo o WIP nem merge das PRs. Validação independente limitada aos diffs/recibos e sondas descritos; a evidência nativa anterior foi auditada por conteúdo/hash, não repetida. A lista acima permanece necessária para o fechamento e deve ser consumida pelas fichas originais.


## Correções da revisão independente — 19/11/2026 (noite, Windows)

Composição usada: `E:\misael\Projects\MT5X`, montagem WinBtrfs do `@home` Linux (`/home/misael/Projects/MT5X`). Não foi criado clone, worktree ou outro ambiente. HEAD do App continua `550b6d5f` (`codex/launcher-classes`) com WIP, HADOUKEN em `d3dd3a5e` e Safety em `27b85992`. Nada foi mesclado. Toda publicação usou índice privado, sem tocar o índice, o HEAD ou as branches locais.

### Pilha publicada (rascunhos sobre a #366 `807298ae`)

| PR | Ponta | Lacuna | Reprodução e resultado |
|---|---|---|---|
| #367 | `ff7c25d8` | P1 · T19.52: comparabilidade | **Antes:** a regressão falhava (`'comprovada' == 'não comprovada'`) com custos e risco diferentes. **Correção:** o contrato canônico de recibos foi extraído de `preset_assets.compare_runs` e passou a ser exigido pelo estudo: motor, código, capital/risco/custos, início, origem, relógio, moeda e especificação. Recibo ausente ou legado bloqueia; setup e parâmetros são as diferenças intencionais. **Depois:** `test_conditional_policy` 30/30, `test_laboratory_financial` 9/9; `test_laboratory_preset_assets` e `test_scope_comparison` passam funcionalmente. |
| #368 | `8c247568` | P0 · BD03–05/T2.6: fuso | **Antes:** o módulo nativo do Windows lia `datetime` sem fuso no fuso do PC (GMT-3: janela 03:00→02:55). **Correção:** `LocalMT5` aplica o mesmo `_to_epoch` da ponte Linux (sem fuso = relógio do servidor). `extract`, `ticks` e `update` usam como "agora" o último tick do servidor. **Depois:** `test_bd03_clock_contract` cobre fusos por `TZ` (UTC, GMT-3, GMT+9, EST5EDT), DST e entrada ambígua; também passam CLI, ticks, conexão, automação, atualização incremental e painel. Contrato em `docs/dados/BD05_TEMPO.md`. |
| #369 | `9960b5f3` | P1 · restauração/MAX_PATH | **Antes:** o recibo tinha 257 caracteres e o temporário atômico 270; a troca falhava no meio. **Correção:** pré-validação de todos os caminhos criados antes do primeiro efeito, com "nada foi alterado". A política `LongPathsEnabled` só é lida. **Depois:** recusa sem efeito e retomada em caminho curto; 84 testes de restauração e recursos passam com basetemp curto. |
| #370 | `ad1acbc4` | P1 · qualidade nativa e P0 · proveniência | **Antes:** o pt-BR `100% de ticks reais` virava `conflicting`. **Correção:** o parser aceita o formato, e a evidência nativa grava `app_source` (sha256 CRLF→LF por módulo carregado, conferível com `git show`). **Depois:** `test_data_eligibility` 47, `test_preset_execution` 23, consumidores nativos 102. |
| #371 | (esta seção) | P2 · documentação e handoff | `AGENTS.md` com um ambiente por sistema; esta seção. |

### Evidência do percurso real (Windows físico, launcher público, sem junção nem PATH de bancada)

- Extração de QA pelo `run.bat` na base própria `state/qa-tmp/windows-20261119-fuso`:
  - host em E. South America (GMT-3), `broker_utc_offset` vazio;
  - EURUSD M5 de 10–11/11: 576 barras, de `2026-11-10 00:00` a `2026-11-11 23:55`;
  - ticks de 10/11: 302.070, o último às `23:59:58.811`;
  - log: `extrair.log`.
- Reavaliação dirigida dos 7 relatórios arquivados, sem repetir a campanha:
  - arquivo: `state/qa-tmp/windows-20261118/tester/cobertura-reavaliada-20261119.json`;
  - `report_sha256` igual ao gravado nos recibos;
  - cobertura `reported` com 100% de ticks reais; elegibilidade ainda `unqualified`.
- Adendo das sessões: `tester/sessoes-adendo-20261119.json`.
  - Registra os arquivos arquivados com sha256 e aponta para eles (todos os `artifact_dir` originais deixaram de existir).
  - Avalia `app_note` como insuficiente.
  - **Prova:** para os aceites das 04:29 em diante, todo `presets/` e `robots/` coincide por blob com `807298ae`, exceto `execution.py`/`conditional_policy.py`, reescritos hoje. Antes desta sessão eram iguais a `807298ae`, mas o mtime não prova o conteúdo exato de cada execução.
  - `bb9f2829` (03:38) rodou antes das correções do Triplo das 04:23.
  - Os recibos `run-*.json` não foram alterados.

### Composição e recuperação

- Manifesto por conteúdo (`state/composition/manifesto_composicao.py`; prévia `manifesto-20261119-previa.json`, contra a ponta `9960b5f3`):
  - App: das 217 entradas do WIP, 211 já estão publicadas byte a byte na pilha; as 6 divergentes são 5 documentos e o teste do incremento #370.
  - HADOUKEN: tem WIP alheio, preservado (3 docs removidos, 5 docs novos não rastreados).
  - Safety está limpo.
  - Não há diferença de modo entre o índice e a ponta.
- Documentos divergentes nos dois sentidos: `docs/PLANO_DE_TRABALHO.md` (5 conflitos), `docs/continuidade.md` (1), `docs/propostas/ANALYTICS_OPERACOES.md` (2) e `docs/ROADMAP.md` (0).
  - A ponta tem seções de 12/11 ausentes no WIP, e o WIP tem acréscimos locais.
  - Os rascunhos da fusão de 3 vias estão em `state/composition/fusao-docs/`. Não foram publicados: a decisão é do dono dos documentos.
- Ref privada de retorno: `refs/mt5x/composicao/20261119` com a árvore exata do WIP (ver o manifesto final `state/composition/manifesto-20261119.json`). Não é branch e não foi enviada.
- Logs Linux de 26/10 sobrescritos em `state/`:
  - Buscados na lixeira do E: (vazia), nos inventários do `.workspace`, no ai-memory global (sem resultado) e nos pacotes de proteção de QA (outros arquivos).
  - Os snapshots Btrfs não aparecem pelo Windows.
  - A perda continua registrada; reexecutar gera evidência nova, não restaura os recibos.
  - Conferir os snapshots no Linux.
- Entrada anômala na raiz `MT5X/`: nome criado no Linux com quebra de linha (`")\n    panel.strategy.set(Sem"`).
  - Ilegível no Windows (WinError 123); preservada.
  - Conferir e remover pelo Linux, se for resíduo de comando malformado.

### Desempenho e suíte (contenção comprovada, gate não aprovado)

- Sonda `bancada/sonda_io.py`, resultado em `rerun/sonda-io-20261119.json`:
  - primeira abertura de cada arquivo: 82–101 ms, a frio, tanto no NVMe/WinBtrfs quanto no SD;
  - a quente: 3–6,5 ms;
  - leitura sequencial: 116 MiB/s;
  - `import pandas` a frio: 23 s; `import mt5_extracao.cli`: 2,2 s;
  - Defender em tempo real ativo (MsMpEng ~320 MB).
- A latência por primeira abertura explica os subprocessos que estouram 30–60 s; não foi encontrado defeito funcional nesses casos. Defender continua hipótese plausível, não causa isolada: não houve exclusão, e mudar a segurança é decisão do proprietário.
- `test_multi_asset_scope_reads_all_cells_in_one_frozen_revision[6]` passa em 77,7 s, contra o gate de 60 s. Nenhum timeout foi aumentado, e a suíte completa não rodou no gate original.
- Saída redirecionada da CLI no Windows sai em cp1252 (`e7 e3` = "çã"), como em `_tolerant_streams`; o console interativo é Unicode. Ler o arquivo redirecionado como UTF-8 mostra acentos trocados.

### Situação para a revisão

1. **Corrigido e validado:**
   - T19.52 (comparabilidade econômica);
   - contrato de fuso com extração real no Windows;
   - pré-validação MAX_PATH da restauração;
   - parser pt-BR da cobertura de ticks, com reavaliação dos 7 relatórios;
   - adendo e manifesto das sessões arquivadas.
2. **Implementado, com aceite específico pendente:**
   - `app_source` nos recibos nativos (a próxima execução nativa vai demonstrá-lo);
   - `update` com o "agora" do servidor (só testes; sem extração incremental real).
3. **Bloqueado por dependência externa:**
   - paridade Windows×Linux (Linux inacessível nesta sessão);
   - snapshots Btrfs dos logs;
   - decisão sobre Defender/LongPaths;
   - fusão dos documentos alheios;
   - DEMO ao vivo, sem roteiro nem autorização desta janela.
4. **Não concluído:**
   - suíte completa local no gate de 60 s;
   - E14 (iCCI/CopyBuffer, AggrVolBalance, escopos/netting, parcial/incerto/restart, proteção) e aceites físicos de DPI/teclado;
   - T19.53/54: protocolo/seletores, política causal no HADOUKEN e no backtest completo, pacote/consumidores e validação fora da amostra;
   - encurtar os nomes dos recibos de restauração (exige migração compatível).

**Próxima ação:**
- O proprietário revisa as PRs #367→#371 sobre a pilha #353→#366.
- No Linux: conferir os snapshots, a entrada anômala, a paridade T2.6 e a fusão dos documentos.
- Depois, E14 nativo em execução serial, com entradas controladas.
