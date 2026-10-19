# Roadmap: construtor de bases MT5 completo (Windows + Linux)

Visão: com um duplo clique (Windows) ou `./install.sh` (Linux), o usuário instala
tudo e constrói e mantém bases históricas confiáveis (OHLCV, indicadores, ticks)
do MetaTrader 5, pela interface gráfica ou por linha de comando agendada.

A partir da Fase 10 o projeto é também um **SDK para desenvolver robôs MQL5 de alto padrão**: um contrato
escrito, um ponto de partida (`mt5x robots new`), um ciclo de desenvolvimento em um comando e uma API estável.
O aplicativo amigável (automação, análise e gestão) continua sendo o mesmo; o SDK é uma camada a mais.

O detalhamento de cada tarefa (arquivos, passos, código de referência, critérios
de aceite e testes) está em [PLANO_DE_TRABALHO.md](PLANO_DE_TRABALHO.md).
As regras para agentes estão em [../AGENTS.md](../AGENTS.md).

## Fases

```
F0 Fundação ──► F1 Integridade dos dados ──► F3 CLI/automação ──► F5 Escala e novos dados
    │                                             ▲
    └─────────► F2 Núcleo desacoplado + Linux ────┘   F6 Estratégias e triagem (usa a base de F1)
                         │
                         └────────► F4 Qualidade e manutenção (contínua)
```

| Fase | Objetivo | Tarefas | Depende de | Status |
|---|---|---|---|---|
| **F0** Fundação | Instalação automática Win/Linux, configuração inicial, diagnóstico, CI | T0.1–T0.3 | — | T0.1 ✅, T0.2 ✅, T0.3 ⏳ (validação manual) |
| **F1** Integridade | Reextrair sem erro, retomar extrações, atualização incremental, dados corretos | T1.1–T1.8 | F0 | ✅ |
| **F2** Núcleo + Linux | Núcleo sem MetaTrader5/Tkinter no import; o app funciona no Linux via ponte | T2.1–T2.6 | F0 | T2.1–T2.5 ✅, T2.6 ⏳ (paridade com MT5 real) |
| **F3** CLI e automação | `mt5x extract/update/export/doctor`, agendamento, `pyproject.toml` | T3.1–T3.4 | F1, F2 | ✅ (agendamento: cron e schtasks) |
| **F4** Qualidade | Limpeza de arquivos, logging central, testes com provedor falso, docs | T4.1–T4.5 | F0 (pode ir em paralelo) | ✅ |
| **F5** Escala | Parquet/DuckDB, PostgreSQL/TimescaleDB, ticks, book, fonte externa real | T5.1–T5.6 | F1, F3 | ✅ (TimescaleDB: hypertable automática, conferida em servidor com a extensão) |
| **F6** Estratégias | Seis estratégias, backtest com custos, walk-forward/Monte Carlo (Filtro C), triagem de ativos (Filtros A e B), relatório HTML, menu na GUI | T6.1–T6.8 | F1, F3 | ✅ |
| **F7** Interface | Janela principal em abas (Ativos, Minha base, Análise, Atividade), tema claro/escuro, gráficos e números de estudo; tabelas de ativos com favorito, volume, resultado dos robôs, status e benchmark, e bateria de backtests em segundo plano | T7.1–T7.2 | F1, F6 | ✅ |
| **F8** Robôs | Catálogo de robôs (MQL5 e Python) em cards, backtest, bateria de segurança e operação em demo/real pelo robô MQL5 | T8.0–T8.9 | F6, F7 | T8.0–T8.6 ✅; T8.7 ⏳ (só simulada, falta validar com o MT5 real em demo); demais a fazer |
| **F9** Agentes e segurança | Operação sem tela (`--json`, `status`, `analyze`), auditoria `mt5x audit` e ponte do Linux restrita a funções de leitura | T9.1–T9.3 | F3, F8 (T8.3) | ✅ |
| **F10** SDK de robôs | Contrato do robô, biblioteca de segurança MQL5 versionada, `robots new`, ciclo `robots check`, bateria completa para MQL5, paridade Python × MQL5, API pública, documentação e pacote; como funções opcionais, geração de MQL5 a partir de uma estratégia declarada em Python e preparação para o MQL5 Market | T10.0–T10.12 + T10.W | F8 (T8.4–T8.6), F9 | T10.0–T10.8 e T10.10 ✅; demais a fazer (T10.9 pacote, T10.11 Market, T10.12 custo do backtester, T10.W Windows). Lacunas conhecidas em docs/sdk/exatidao.md; a do custo do backtester Python foi resolvida na T10.12 (swap e spread medido nos ticks). Opcionais: T10.10 (gerar MQL5 a partir de Python) e T10.11 (preparar para o MQL5 Market). **T10.W (validação no Windows) é pendência: não bloqueia o desenvolvimento no host atual** |
| **F11** Testes amplos e operação discreta | Montar um timeframe maior a partir de uma base menor (M5 de M1...), backtest em vários ativos de uma vez, MT5 oculto (sem ocupar a tela) e avisos em pop-up no computador a cada ação executada | T11.0–T11.10 | F1, F8, F10 | T11.0–T11.4 ✅ (T11.3 e T11.4 só validadas no Linux; avisos de operações do robô só com MT5 simulado); T11.5–T11.8 (lote, matriz, painel visual e botão na interface) ✅; T11.9–T11.10 (painel vivo, com ações) ✅ |
| **F12** HADOUKEN 2.0 | Carteira da conta, guardião global, Gradiente Linear, painel recolhível com abas, estatísticas e copiloto de IA só de leitura | T12.0–T12.8 | F8, F10 | T12.0, T12.L (janela organizada) e T12.P (painel responsivo) ✅; T12.1 e T12.2 ⏳; T12.6a ✅ (normal, tema claro, ajuste abaixo de 100%, tela física, vocabulário e linha de base de um gráfico); G-02 (arquitetura modular do painel) é a próxima tarefa elegível; T12.6b aguarda G-02, e T12.3 e demais etapas aguardam suas dependências. T12.L1–T12.L3 ✅ |
| **F13** MT5x Analytics | Catálogo de evidências sobre ativos: barras derivadas do M1 por sessão, características com data de disponibilidade (volume relativo, regime, sazonalidade), hipóteses validadas fora da amostra e um retrato pequeno que os robôs MQL5 só leem. Especificação em [analytics/ESPECIFICACAO.md](analytics/ESPECIFICACAO.md) | T13.0–T13.8 | F11 (T11.1), F12 (para T13.5 em diante) | T13.0 ✅ (plano); demais a fazer. **Nada implementado** |
| **F14** Plataforma modular | Backlog integrado do HADOUKEN e do mt5x: framework modular de estratégias, mecanismos de execução, gestão de posições e de carteira, painel, estatísticas, pesquisa e validação, IA. Índice em [propostas/README.md](propostas/README.md) | T14.0 (registro); as tarefas têm identificador próprio (`A-01` a `J-09`) | F12, F13 | T14.0 ✅ (backlog registrado). **Nada implementado**: cada tarefa entra no plano quando for a próxima |
| **F17** Modernização visual do mt5x | Sistema visual e tipográfico, componentes reutilizáveis, Laboratório e gráficos, aplicação progressiva às abas e validação visual | T17.1–T17.5 | F7, F8, F15, F16 | T17.1–T17.5 ✅ Linux; validação Windows pendente |
| **F18** Relações e aderência | Indicador × indicador, movimentos futuros, ganhos/perdas, convergências e estabilidade temporal; UI, candidatos e API de leitura | T18.0–T18.9 | F13, F16, F15; F17 para UI | T18.0 documental; T18.8 com apresentação local sintética; Analytics e integração pendentes |
| **F19** Pilares e presets | Dados de mercado/operações; entrada/orientação, lotes/parciais, ganho, stop e filtros; Default primeiro, Manual nomeado/Defensivo/Maximalista(PF)/Ultra; setups HADOUKEN e UI coerente | T19.0–T19.57 | F13/F16/F18 conforme capacidade; contratos B e Safety para consumo MQL5; F17 para UI | T19.0 documental com adendos; T19.1 em revisão local ([inventário](propostas/INVENTARIO_F19.md)); runtime planejado. Gestão da conta posterior, sem adiar travas |
| **F20** Robustez transversal | Cálculos certificados, dados/experimentos reproduzíveis, fidelidade econômica/eventos, evidências/presets, observação/recuperação, UI investigativa, recursos/SDK/restore e carteira posterior | T20.0–T20.30 | F19 e donos F1/F5/F12–18/B–J por capacidade; não espera todas as fases | T20.0 documental; 30 fichas planejadas, sem capacidade nova implementada |

## Fase 18 — Relações entre indicadores e aderência dos setups

**Planejada em 2026-10-15; execução parcial.** T18.0 registra a especificação e os prompts. T18.8 já tem uma apresentação
local para validação, com fixture identificada como sintética e componentes da F17. Registry/colunas analíticas,
associações entre indicadores e movimentos/resultados, convergências, janelas temporais, serviço local, candidatos a
presets e API somente leitura permanecem pendentes das tarefas de Analytics.

Plano completo: [Relações e aderência](propostas/RELACOES_INDICADORES_SETUP.md).
Ordem: T18.1 → T18.2 → T18.3 → T18.4; T18.5 → T18.6 → T18.7/T18.9;
T18.8 em paralelo com fixture após T18.1; a apresentação local aguarda o DTO T18.1 e integra ao serviço T18.6.
Ver dependências detalhadas no plano.
Dados insuficientes não são incompatibilidade; associação não implica causalidade ou lucro.
Primeiro consumo na base/CLI e Laboratório; depois propostas de pesquisa para presets/agentes e API opt-in.
Nenhuma mudança automática de risco, preset ou ordem.

## Marcos (cada marco = versão publicável)

| Marco | Versão | Critério de pronto |
|---|---|---|
| M0 — Instala sozinho | 0.2.0 | `install.bat` e `install.sh` concluem em máquinas limpas (checklist T0.3); CI verde |
| M1 — Base confiável | 0.3.0 | T1.1–T1.6 concluídas; reextrair o mesmo período não duplica nem falha; extração interrompida retoma de onde parou |
| M2 — Linux de verdade | 0.4.0 | T2.1–T2.5; extração M1 de 1 mês no Linux gera o mesmo resultado que no Windows (teste de paridade) |
| M3 — Sem cliques | 0.5.0 | T3.1–T3.3; `mt5x update` agendado via cron / Agendador de Tarefas mantém a base em dia |
| M4 — Manutenível | 0.6.0 | T4.1–T4.4; cobertura de testes ≥ 60% no núcleo (`database_manager`, `historical_extractor`, `mt5_backend`) |
| M5 — Escala | 1.0.0 | T5.1–T5.4; exportação Parquet, TimescaleDB opcional, ticks |
| M6 — SDK no host atual | 1.1.0 | T10.1–T10.8 concluídas e verificadas no Linux (Wine); um robô criado com `mt5x robots new` passa por `mt5x robots check` do zero |
| M7 — SDK publicado | 1.2.0 | M6 + T10.W (checklist do Windows cumprido) + T10.9 (pacote publicado) |

## Ordem recomendada de execução (uma tarefa = um PR)

1. T0.3 (validação manual dos instaladores; pode ser feita por um humano)
2. T1.1 → T1.5 → T1.4 → T1.2 → T1.3 → T1.6 → T1.7 → T1.8
3. T2.1 → T2.2 → T2.3 → T2.4 → T2.5 → T2.6
4. T4.3 (provedor falso; facilita testar as fases seguintes) → T4.1 → T4.2
5. T3.3 → T3.1 → T3.2 → T3.4
6. T4.4, T4.5
7. T5.x conforme a necessidade
8. F6 (T6.1 → T6.8) já concluída; novas estratégias seguem o roteiro em docs/estrategias.md ("Para desenvolvedores")
9. F7 (T7.1 → T7.2) já concluída; novos painéis entram em `mt5_extracao/ui/`
10. F8: T8.0 → T8.1 → T8.2 → T8.3 → T8.4 → T8.5 → T8.6 → T8.7 → T8.8 (T8.9 em paralelo depois da T8.5)
11. F9 (T9.1 → T9.3) já concluída
12. F10, em três trilhas que podem andar em paralelo (arquivos diferentes):
    - ciclo e qualidade: T10.1 → T10.4 → T10.5
    - lado MQL5: T10.2 → T10.3 → T10.6
    - superfície pública: T10.7 → T10.8 → T10.9 (a T10.9 só depois da T10.W)
    - opcionais, depois das trilhas: T10.10 (depende de T10.3 e T10.6) e T10.11 (depende de T10.4)
    - F11, pedida pelo dono do projeto antes da T10.11: T11.1 → T11.2 → T11.3 → T11.4
    - T10.W quando houver uma máquina Windows; até lá, cada tarefa acrescenta o que falta conferir ao
      checklist em [validacao_instaladores.md](validacao_instaladores.md)
13. F13 (MT5x Analytics), planejada: T13.1 → T13.2 → T13.3 → T13.4 só em Python, em paralelo com a F12; T13.5 → T13.6 → T13.7 → T13.8
    mexem no HADOUKEN e dependem da F12 (veja as dependências de cada tarefa)
14. F14 (plataforma modular), backlog: a ordem em dez etapas está em [propostas/README.md](propostas/README.md); começa pelas decisões e travas
    (etapa 0) e pelos dados (etapa 1, que é a F13)
15. F17 (modernização visual): T17.1 → T17.2 → T17.3 → T17.4 → T17.5; apresentação somente, sem alterar cálculos, contratos ou regras operacionais

As fases F1 e F2 são independentes e podem ser executadas em paralelo por
agentes diferentes, **desde que não editem os mesmos arquivos ao mesmo tempo**
(conflitos esperados: `mt5_connector.py`, `historical_extractor.py`).

## Fora do escopo (por ora)

- macOS: o pacote MetaTrader5 não existe e o Wine no macOS é instável. A arquitetura da ponte (F2) permitiria suportá-lo no futuro.
- Envio de ordens **pelo Python**: o código deste projeto nunca chama `order_send`. A partir da F8 o aplicativo
  instala, inicia, monitora e para robôs MQL5, e quem opera é o robô dentro do MT5, pela camada `Safety/` do
  HADOUKEN (veja [robos.md](robos.md)). Conta real fica desligada por padrão. O SDK (F10) não muda isso:
  ele ajuda a escrever o robô MQL5; o envio de ordens continua sendo só do robô.
- Tradução de código Python **livre** para MQL5: a T10.10 gera MQL5 só a partir de estratégias **declaradas**
  (indicadores e condições de um conjunto suportado). `gerar_sinais` escrito à mão continua sendo portado por quem
  desenvolve.
- Envio automático ao MQL5 Market e licenciamento: a T10.11 confere e empacota; o envio é feito pela pessoa, na
  área do vendedor do MQL5.com.
- Bases em nuvem gerenciadas (AWS RDS etc.): basta apontar T5.2 para o host.
- Otimização de parâmetros por ativo, a partir das características do backtest: ideia registrada pelo dono do
  projeto em 2026-10-12, para pensar depois. Sem tarefa nem desenho ainda; hoje os parâmetros só variam na
  grade pequena do walk-forward (`param_grid`).


## Fase 19 — Dados, operação, estratégias e presets

Pedido do proprietário em 2026-10-17. **T19.0 documental; T19.1 com
[inventário reconciliado](propostas/INVENTARIO_F19.md) em revisão local, sem runtime novo.**
A publicação depende da base documental preexistente. T20.1 estende o mesmo mapa
após integração; store/registry localizados em branches não são dependências integradas.
Integra as frentes já existentes, com dois ramos de dados (histórico extraído e operações de
backtest/demo/real), blocos operacionais de entrada/orientação, lotes/parciais, ganho,
stop e filtros, e módulos
de estratégia compatíveis com o HADOUKEN. O resultado é um preset manual ou selecionado
automaticamente por objetivo defensivo/fator de lucro ou uma política **Ultra** condicional,
com composição e evidência reproduzíveis. Default vem primeiro como referência da estratégia;
as quatro categorias de trabalho têm nome padronizado, incluindo o nome escolhido no Manual.

- [Especificação funcional e técnica](propostas/PILARES_DADOS_OPERACAO_PRESETS.md).
- [Catálogo integral dos indicadores, gestão temporal e setups](propostas/CATALOGO_F19_INDICADORES_SETUPS.md).
- [Tarefas, dependências, marcos e critérios de aceite](propostas/PLANO_F19_PILARES_PRESETS.md).

Começar por T19.1 (reconciliação), contratos e um percurso simples completo com S09/T19.37
e preset manual. Depois gestão temporal, objetivos automáticos e ampliação do catálogo.
Não exigir fluxo/Renko para o piloto nem presumir contratos F13/F18/B implementados.
A T17.28 já oferece timeframe em “Ativo / TF”; T19.33 verifica descoberta e coerência
em Play/Replay/Backtest e presets. Gestão global da conta fica para etapa posterior;
grades/scale-in mantêm os pré-requisitos próprios de risco, reserva e política.

Re-simulação das entradas antigas e backtest completo são evidências diferentes.
Geração de preset não inicia operação nem altera os critérios de bateria/autorização.
Sem dados/regras/candidato elegível, o resultado registra a limitação; não inventa capacidade.
O catálogo cobre os 18 indicadores distintos do TXT, métricas temporais e os 11 itens
detalhados do Markdown; os alegados 37/48 setups não enumerados permanecem lacuna explícita.
A ideia de otimização por ativo, registrada em 2026-10-12 como futura, passa a este escopo
planejado. A grade atual do walk-forward (`param_grid`) não equivale ao gerador novo.

**Adendo de 2026-10-17:** T19.50 define nomes claros e portáveis; T19.51 acrescenta a flag
experimental de inversão compra↔venda, desligada por padrão. T19.52–54 entregam o Ultra:
regras por hora/dia/semana/mês e contexto causal, que escolhem configurações suportadas
com prioridade, fallback e rastreio. Avaliar a política inteira em backtest completo/OOS;
não concatenar vencedores escolhidos retrospectivamente. Inversão de intenção não é
reversão de posição aberta. O primeiro preset manual não depende da conclusão do Ultra.

**Complemento:** T19.55 entrega o Default versionado e prioritário; T19.26/50 incluem Nome do
preset informado pelo usuário e identificação automática por metadados. Maximalista é o
rótulo do perfil FatorLucro. [Prompt único F19/F20/UI](propostas/PROMPT_CONSOLIDADO_DESENVOLVIMENTO_MT5.md) e
[matriz de validação](propostas/VALIDACAO_F19.md) aceitam replay operacional nativo MT5/DEMO
com recorte histórico ágil quando o ativo estiver indisponível online, sem exigir operação
ao vivo para cálculos/estrutura e sem substituir backtest estatístico por replay de QA.

**Lotes e parciais:** T19.56/57 estruturam volume fixo/por risco, entrada com mais de um
lote e agenda de reduções por quantidade/% com saldo e proteção explícitos. Reaproveitar
dimensionamento e parcial simples existentes; validar parciais múltiplas/gradiente antes
de declarar suporte. Implementação inclui UI/preset/Expert e reconciliação de custos/PnL,
sem inflar contagem de operações a partir das parcelas. Detalhe na especificação §6.3.

### Pesquisa futura — FRP, Fluxo Relativo do Preço

Registrar grupos de 3/4 indicadores correlacionados entre si e com movimento do preço,
com concordância direcional e possível utilidade no Expert. Coeficientes exemplificativos
0,60/0,70/0,80/0,90 serão estudados; não representam taxa de acerto ou segurança operacional.
[Conceito e etapas FRP-00–04](pesquisa/FRP_FLUXO_RELATIVO_PRECO.md).
**ESTUDO FUTURO/P3, após consolidação da base F13/F18.** Sem implementação ou fórmula
definitiva e sem bloquear F19/presets/Ultra; primeiro avaliar redundância e contribuição OOS.


## Fase 20 — Robustez da pesquisa, execução e ciclo de estratégias

**Todas as dezesseis sugestões da avaliação foram aceitas pelo proprietário em
2026-10-17. Entrega atual: documentação T20.0, sem implementação.**
F20 qualifica as frentes existentes, com mapa de cobertura Q01–Q16 e trinta fichas
T20.1–T20.30. Não cria outros backtester/store/ledger/registro de presets/executor/fila.
Cada extensão tem dono único e consome a base anterior integrada; o backlog A–J conserva
sua identidade. As 57 fichas F19 não foram renumeradas nem substituídas.

- [Requisitos, contratos, rastreabilidade e marcos](propostas/ROBUSTEZ_PESQUISA_EXECUCAO.md).
- [Fichas de implementação e verificação](propostas/PLANO_F20_ROBUSTEZ.md).
- [Prompt único para o agente executor](propostas/PROMPT_CONSOLIDADO_DESENVOLVIMENTO_MT5.md).
- [UI: requisitos atuais, refinamentos posteriores e experimento gráfico](propostas/UI_ORGANIZACAO_GRAFICOS_PRIORIDADES.md).

Ordem de investimento: referências de cálculo/patrimônio flutuante e dados identificáveis;
compatibilidade/margem/custos/paridade por evento; reprodução e protocolo de pesquisa;
percurso completo até preset validado/UI; acompanhamento/recuperação; carteira no segundo
momento. Recursos podem antecipar lotes pesados. Primeiro passo T19.1 se pendente; T20.1
estende seu mesmo mapa. Inventário já comprovado não é refeito.

T20.3 distingue drawdown patrimonial de saldo encerrado; T20.6 registra qualidade efetiva
e cobertura desconhecida de ticks; T20.12/13 controlam tentativas/reserva e revisam
critérios estatísticos sem prometer zero falso positivo ou uma ordenação universal de
Monte Carlo. T20.14/15 expõem compromissos e contribuição incremental, preservando as
categorias/objetivos F19. Pacotes e evidências T20.16/17 são versionados e invalidados
por dependência; aprovação não equivale a autorização operacional.

O piloto Manual T20.30 não espera Ultra, carteira, FRP ou todo o catálogo; não conclui
o piloto ampliado T19.35, que também exige seleção automática. Seleção automática só é
aceita com protocolo/registro qualificados, e alegação defensiva exige risco patrimonial
explicitado. T20.18–29 tratam acompanhamento, falhas, UI investigativa, recursos, módulos,
terceiros, restauração e release conforme o recorte e suas dependências.

Replay MT5 nativo continua aceito para QA operacional quando o ativo estiver fora de
pregão; rede/fill ao vivo mantêm prova DEMO própria quando necessária. Sem testes REAL.
FRP, agentes autônomos de criação/promoção, aprendizado por reforço, reotimização contínua,
alta frequência e expansão indiscriminada de indicadores permanecem posteriores/P3.

**Priorização da UI:** UI-A01–A10 entram em T19.30–34/T20.22/23 com seus serviços:
modos Analisar/Compor/Validar e comparar, contexto compacto, editor operacional por grupos,
gráfico principal, tabela/inspetor ligados, hierarquia visual, estados/seleção preservados
e adaptação/teclado/performance. A primeira tela de referência usa o percurso simples;
gráficos de sensibilidade, comparação e relações entram quando suas análises existirem.
T20.22 define o padrão de layout e T20.23 conecta inspeção temporal, sem ciclo de dependência.
Continuam Tkinter/ttk/Matplotlib. Densidade alternável e perfis completos de disposição
(UI-P01/P02) são refinamentos P2; Lightweight Charts (UI-X01) é experimento P3 não iniciado,
com futura prova comparável/decisão arquitetural. Nenhum desses itens futuros bloqueia
o aceite atual ou autoriza migração de tecnologia agora.

**Execução consolidada:** enviar somente o bloco do prompt único, sem concatenar os
antigos prompts F19/F20 ou mensagens do chat. Inclui Default/Manual nomeado/lotes/parciais,
Q01–Q16/UI e a retificação de validação: replay nativo pode concluir aceite funcional
histórico fora do pregão; só aspectos específicos de execução ao vivo ficam pendentes.
Prompts parciais mantêm seus caminhos como referências para o documento único.

## Marcos (cada marco = versão publicável)

| Marco | Versão | Critério de pronto |
|---|---|---|
| M0 — Instala sozinho | 0.2.0 | `install.bat` e `install.sh` concluem em máquinas limpas (checklist T0.3); CI verde |
| M1 — Base confiável | 0.3.0 | T1.1–T1.6 concluídas; reextrair o mesmo período não duplica nem falha; extração interrompida retoma de onde parou |
| M2 — Linux de verdade | 0.4.0 | T2.1–T2.5; extração M1 de 1 mês no Linux gera o mesmo resultado que no Windows (teste de paridade) |
| M3 — Sem cliques | 0.5.0 | T3.1–T3.3; `mt5x update` agendado via cron / Agendador de Tarefas mantém a base em dia |
| M4 — Manutenível | 0.6.0 | T4.1–T4.4; cobertura de testes ≥ 60% no núcleo (`database_manager`, `historical_extractor`, `mt5_backend`) |
| M5 — Escala | 1.0.0 | T5.1–T5.4; exportação Parquet, TimescaleDB opcional, ticks |
| M6 — SDK no host atual | 1.1.0 | T10.1–T10.8 concluídas e verificadas no Linux (Wine); um robô criado com `mt5x robots new` passa por `mt5x robots check` do zero |
| M7 — SDK publicado | 1.2.0 | M6 + T10.W (checklist do Windows cumprido) + T10.9 (pacote publicado) |

## Ordem recomendada de execução (uma tarefa = um PR)

1. T0.3 (validação manual dos instaladores; pode ser feita por um humano)
2. T1.1 → T1.5 → T1.4 → T1.2 → T1.3 → T1.6 → T1.7 → T1.8
3. T2.1 → T2.2 → T2.3 → T2.4 → T2.5 → T2.6
4. T4.3 (provedor falso; facilita testar as fases seguintes) → T4.1 → T4.2
5. T3.3 → T3.1 → T3.2 → T3.4
6. T4.4, T4.5
7. T5.x conforme a necessidade
8. F6 (T6.1 → T6.8) já concluída; novas estratégias seguem o roteiro em docs/estrategias.md ("Para desenvolvedores")
9. F7 (T7.1 → T7.2) já concluída; novos painéis entram em `mt5_extracao/ui/`
10. F8: T8.0 → T8.1 → T8.2 → T8.3 → T8.4 → T8.5 → T8.6 → T8.7 → T8.8 (T8.9 em paralelo depois da T8.5)
11. F9 (T9.1 → T9.3) já concluída
12. F10, em três trilhas que podem andar em paralelo (arquivos diferentes):
    - ciclo e qualidade: T10.1 → T10.4 → T10.5
    - lado MQL5: T10.2 → T10.3 → T10.6
    - superfície pública: T10.7 → T10.8 → T10.9 (a T10.9 só depois da T10.W)
    - opcionais, depois das trilhas: T10.10 (depende de T10.3 e T10.6) e T10.11 (depende de T10.4)
    - F11, pedida pelo dono do projeto antes da T10.11: T11.1 → T11.2 → T11.3 → T11.4
    - T10.W quando houver uma máquina Windows; até lá, cada tarefa acrescenta o que falta conferir ao
      checklist em [validacao_instaladores.md](validacao_instaladores.md)
13. F13 (MT5x Analytics), planejada: T13.1 → T13.2 → T13.3 → T13.4 só em Python, em paralelo com a F12; T13.5 → T13.6 → T13.7 → T13.8
    mexem no HADOUKEN e dependem da F12 (veja as dependências de cada tarefa)
14. F14 (plataforma modular), backlog: a ordem em dez etapas está em [propostas/README.md](propostas/README.md); começa pelas decisões e travas
    (etapa 0) e pelos dados (etapa 1, que é a F13)
15. F17 (modernização visual): T17.1 → T17.2 → T17.3 → T17.4 → T17.5; apresentação somente, sem alterar cálculos, contratos ou regras operacionais
16. F19, evolução pedida em 2026-10-17: T19.1 → contratos/dados → piloto manual → gestão temporal
    e geração defensiva/PF → catálogo completo e UI integrada. A ordem por capacidade e os
    gates entre repositórios estão no [plano F19](propostas/PLANO_F19_PILARES_PRESETS.md).
17. F20, qualificação transversal aceita em 2026-10-17: mapa T20.1 sobre T19.1 → cálculos/
    dados/fidelidade → protocolo/reprodução → percurso Manual com UI/replay → observação/
    recuperação; carteira posterior. Não esperar toda F19 para iniciar referências e
    recursos; seguir [as dependências por ficha](propostas/PLANO_F20_ROBUSTEZ.md).

As fases F1 e F2 são independentes e podem ser executadas em paralelo por
agentes diferentes, **desde que não editem os mesmos arquivos ao mesmo tempo**
(conflitos esperados: `mt5_connector.py`, `historical_extractor.py`).

## Fora do escopo (por ora)

- macOS: o pacote MetaTrader5 não existe e o Wine no macOS é instável. A arquitetura da ponte (F2) permitiria suportá-lo no futuro.
- Envio de ordens **pelo Python**: o código deste projeto nunca chama `order_send`. A partir da F8 o aplicativo
  instala, inicia, monitora e para robôs MQL5, e quem opera é o robô dentro do MT5, pela camada `Safety/` do
  HADOUKEN (veja [robos.md](robos.md)). Conta real fica desligada por padrão. O SDK (F10) não muda isso:
  ele ajuda a escrever o robô MQL5; o envio de ordens continua sendo só do robô.
- Tradução de código Python **livre** para MQL5: a T10.10 gera MQL5 só a partir de estratégias **declaradas**
  (indicadores e condições de um conjunto suportado). `gerar_sinais` escrito à mão continua sendo portado por quem
  desenvolve.
- Envio automático ao MQL5 Market e licenciamento: a T10.11 confere e empacota; o envio é feito pela pessoa, na
  área do vendedor do MQL5.com.
- Bases em nuvem gerenciadas (AWS RDS etc.): basta apontar T5.2 para o host.
