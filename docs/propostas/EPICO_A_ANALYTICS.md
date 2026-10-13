# Épico A — Base de dados e inteligência de mercado

Repositório responsável: **mt5x**. Índice, convenções e regras comuns: [README.md](README.md).

A maior parte deste épico **já está planejada** como Fase 13 do [PLANO_DE_TRABALHO.md](../PLANO_DE_TRABALHO.md),
com a especificação em [analytics/ESPECIFICACAO.md](../analytics/ESPECIFICACAO.md). As entradas abaixo apontam para
a tarefa do plano em vez de repeti-la. São novas, sem tarefa no plano: **A-03** (Renko), **A-09** na parte de
liquidez e **A-11** (correlação entre ativos).

Regras do épico: armazenamento **versionado** (toda característica tem versão do algoritmo e revisão dos dados) e
**sem olhar para o futuro** (nenhum fato usa informação posterior ao instante em que é dado como disponível).

### A-01 — Agregação automática do M1 em timeframes maiores e customizados
- **O quê e por quê:** montar M2, M3, M5, M10, M15, M30, H1, H4, D1 e N minutos a partir do M1, por sessão, com estado de qualidade por barra. É a base de tudo o que estuda mais de um timeframe sem nova extração.
- **Repositório:** mt5x · **Prioridade:** P1 · **Complexidade:** G · **Status:** PLANEJADO (é a T13.1)
- **Módulos existentes:** `resample.py` (pelo relógio, conferido contra o MT5), `timeframes.py`, `data_quality.py` · **Depende de:** A-02 · **Pré-requisitos:** decisão sobre timeframes fora do enum do MT5
- **Riscos:** trocar o `resample` atual quebraria backtests gravados e a paridade; barra parcial tratada como fechada.
- **Aceite:** testes 1 a 5 da especificação (§14); para ativo 24 horas, igual ao `resample` atual nos timeframes do MT5.
- **Testes:** séries sintéticas com resposta conhecida; comparação com barras extraídas do MT5 em dois mercados.

### A-02 — Calendário de negociação: sessões, fusos e feriados
- **O quê e por quê:** tabela de sessões por símbolo (abertura, fechamento, pausas, feriados, horário de verão). Sem ela, uma barra H4 ou D1 mistura pregões e "fechamento" é um horário fixo de exemplo.
- **Repositório:** mt5x · **Prioridade:** P0 · **Complexidade:** M · **Status:** PLANEJADO (parte da T13.1)
- **Módulos existentes:** `data_quality.check_quality` (sessão fixa 09:00 a 18:30), `DatabaseManager.time_basis` · **Depende de:** — · **Pré-requisitos:** decisão 4 do índice (fonte do calendário)
- **Riscos:** calendário deduzido das barras erra em dia atípico; fuso da corretora muda com o horário de verão.
- **Aceite:** sessão que atravessa a meia-noite, feriado e fechamento antecipado não geram barra fantasma nem barra que cruza sessões.
- **Testes:** casos sintéticos de cada situação; conferência com um mês real de um ativo da B3 e de um 24 horas.

### A-03 — Barras Renko com regras explícitas
- **O quê e por quê:** série Renko derivada, com tamanho do tijolo (fixo ou por ATR), regra de reversão e origem dos dados declarados. Pedida para as estratégias Renko (C-08).
- **Repositório:** mt5x · **Prioridade:** P3 · **Complexidade:** M · **Status:** PROPOSTO
- **Módulos existentes:** `tick_extractor.py`, `resample.py`, `timeframes.py` (não tem timeframe sem tempo) · **Depende de:** A-01, A-04 · **Pré-requisitos:** ticks reais do ativo com cobertura medida
- **Riscos:** Renko montado de M1 não sabe a ordem dos preços dentro do minuto e inventa tijolos; cada plataforma monta de um jeito, então a série não bate com a do MT5; tijolo por ATR olha para o futuro se recalculado.
- **Aceite:** a mesma entrada produz a mesma série (idempotente); série de ticks e série de M1 ficam marcadas com a origem; tijolo só existe depois do preço que o fecha.
- **Testes:** sequências sintéticas com tijolos contados à mão; reprodução a partir de um ponto no tempo.

### A-04 — Guarda histórica de características (Feature Store)
- **O quê e por quê:** gravar cada fato com quando foi observado, quando ficou disponível, até quando vale, revisão dos dados e versão do algoritmo, e consultar "o que se sabia em tal instante".
- **Repositório:** mt5x · **Prioridade:** P0 · **Complexidade:** G · **Status:** PLANEJADO (é a T13.2)
- **Módulos existentes:** `database_manager.py` (upsert sem revisão), `_extraction_log` · **Depende de:** A-01 · **Pré-requisitos:** medir SQLite contra PostgreSQL com a base real
- **Riscos:** vazamento de informação futura; volume de dados com M1 de vários anos; correção tardia sem rastro.
- **Aceite:** testes 4, 6 e 7 da especificação; consulta *as-of* reproduz o valor conhecido na data simulada.
- **Testes:** ingestão tardia, correção retroativa e reprocessamento só do que depende do que mudou.

### A-05 — Indicadores calculados previamente
- **O quê e por quê:** registro de indicadores com dependências, calculados uma vez e guardados, para estratégia, triagem e publicação lerem o mesmo número.
- **Repositório:** mt5x · **Prioridade:** P1 · **Complexidade:** M · **Status:** PLANEJADO (T13.2 e T13.3)
- **Módulos existentes:** `strategies/indicators.py`, `indicator_calculator.py`, `study.add_indicators` · **Depende de:** A-04 · **Pré-requisitos:** —
- **Riscos:** três implementações do mesmo indicador hoje; divergência com o indicador do MT5 (a paridade usa o ATR do Python).
- **Aceite:** um indicador, uma implementação; valor guardado igual ao calculado na hora para a mesma barra.
- **Testes:** igualdade entre guardado e recalculado; comparação com o MT5 nos indicadores usados pela paridade.

### A-06 — Padrões estatísticos de preços, gaps, aberturas e fechamentos
- **O quê e por quê:** hipóteses declaradas (condição, janela, desfecho, custos) avaliadas com amostra, taxa-base e resultado fora da amostra. Começa por H01 a H05 da especificação.
- **Repositório:** mt5x · **Prioridade:** P2 · **Complexidade:** G · **Status:** PLANEJADO (é a T13.4)
- **Módulos existentes:** `strategies/validation.py` (valida estratégia, não hipótese) · **Depende de:** A-07, A-08, A-10 · **Pré-requisitos:** A-02
- **Riscos:** muitas hipóteses testadas e algumas "confirmam" por acaso; gap que ocorreu antes da entrada tratado como lucro.
- **Aceite:** teste 8 da especificação; hipótese rejeitada continua no catálogo; resultado publicado qualquer que seja.
- **Testes:** dados sintéticos com efeito conhecido e sem efeito; correção para múltiplos testes.

### A-07 — Sazonalidade por dia e horário
- **O quê e por quê:** efeito de dia da semana, hora, semana do mês, vencimento e feriado sobre retorno, amplitude e volume.
- **Repositório:** mt5x · **Prioridade:** P2 · **Complexidade:** M · **Status:** PLANEJADO (parte da T13.3)
- **Módulos existentes:** `study.seasonality` (retorno e amplitude, sem volume, sem calendário) · **Depende de:** A-02, A-04 · **Pré-requisitos:** —
- **Riscos:** amostra pequena por célula (dia × hora); confundir sazonalidade com mudança estrutural.
- **Aceite:** cada célula traz amostra e intervalo; célula com pouca amostra é marcada, não escondida.
- **Testes:** série sintética com efeito plantado em um dia; série sem efeito não acusa.

### A-08 — Perfil histórico de volume e volume relativo
- **O quê e por quê:** curva esperada de volume por minuto da sessão e RVOL "até agora" comparado ao mesmo horário de sessões anteriores.
- **Repositório:** mt5x · **Prioridade:** P1 · **Complexidade:** M · **Status:** PLANEJADO (parte da T13.3)
- **Módulos existentes:** nenhum dedicado; `asset_table.volumes` (T7.2) calcula um volume médio diário simples para a interface · **Depende de:** A-02, A-04 · **Pré-requisitos:** diagnosticar o volume real por corretora
- **Riscos:** volume de ticks não é contrato negociado; zero e desconhecido são o mesmo valor hoje.
- **Aceite:** RVOL do instante t usa só o que estava disponível em t; volume real ausente não vira zero.
- **Testes:** teste 5 e 6 da especificação; sessão curta e feriado.

### A-09 — Tendências de volume e de liquidez
- **O quê e por quê:** queda ou alta persistente de volume (com robustez declarada) e, quando houver ticks e book, spread, profundidade e desequilíbrio.
- **Repositório:** mt5x · **Prioridade:** P2 (volume) e P3 (liquidez) · **Complexidade:** M · **Status:** PLANEJADO no volume (T13.3); PROPOSTO na liquidez
- **Módulos existentes:** `tick_extractor.py`, `book_collector.py`, `_symbol_specs.spread_profile` · **Depende de:** A-08 · **Pré-requisitos:** cobertura de ticks e de book medida por ativo
- **Riscos:** inferir tendência de três observações; book coletado em intervalos não reconstrói a fila.
- **Aceite:** estado `VOLUME_DECLINING` só com amostra mínima e limiar validado; liquidez só é publicada com a cobertura junto.
- **Testes:** séries sintéticas com e sem tendência; lacuna de ticks não vira liquidez zero.

### A-10 — Identificação de regimes de mercado
- **O quê e por quê:** estados `TREND`, `RANGE`, `TRANSITION` e `UNCERTAIN`, com confiança e duração mínima, para condicionar estratégia e execução.
- **Repositório:** mt5x · **Prioridade:** P1 · **Complexidade:** M · **Status:** PLANEJADO (parte da T13.3)
- **Módulos existentes:** `strategies/asset_screener.filter_b` (ADX da última barra) · **Depende de:** A-04, A-05 · **Pré-requisitos:** vocabulário de regime por estratégia
- **Riscos:** regime que troca a cada barra; um regime "universal" que não serve a nenhuma estratégia.
- **Aceite:** o regime da triagem e o deste módulo são o mesmo cálculo, ou a diferença está documentada.
- **Testes:** séries sintéticas de tendência, lateral e transição; estabilidade com ruído.

### A-11 — Correlação e comportamento entre ativos
- **O quê e por quê:** correlações móveis, defasagens e dispersão entre ativos, para contexto e para o orçamento de risco por grupo correlacionado (F-04).
- **Repositório:** mt5x · **Prioridade:** P3 · **Complexidade:** M · **Status:** PROPOSTO
- **Módulos existentes:** `asset_screener._correlations` (correlação simples para a triagem) · **Depende de:** A-02, A-04 · **Pré-requisitos:** barras alinhadas por sessão entre ativos de bolsas diferentes
- **Riscos:** supor causalidade; desalinhamento de fuso entre bolsas gera defasagem falsa.
- **Aceite:** toda correlação informa janela, amostra e sobreposição de sessões; nada é publicado como relação causal.
- **Testes:** pares sintéticos com defasagem conhecida; ativos com sessões que não se sobrepõem.

### A-12 — Monitoramento de degradação de padrões
- **O quê e por quê:** acompanhar hipóteses e características aprovadas e rebaixar sozinho a que deixar de se sustentar.
- **Repositório:** mt5x · **Prioridade:** P3 · **Complexidade:** M · **Status:** PLANEJADO (é a T13.8)
- **Módulos existentes:** `notify.py` · **Depende de:** A-06 · **Pré-requisitos:** critério de degradação definido antes de olhar os dados
- **Riscos:** alarme falso por variação normal; regra que nunca é rebaixada.
- **Aceite:** regra instável sai de `APPROVED` com o motivo gravado e trilha de quem aprovou.
- **Testes:** série que perde o efeito no meio; série estável não dispara.

### A-13 — Publicação de dados analíticos para os robôs MQL5
- **O quê e por quê:** retrato pequeno (arquivo com manifesto e hash, troca atômica) que o robô só lê, com validade por característica.
- **Repositório:** mt5x (leitor: HADOUKEN) · **Prioridade:** P1 · **Complexidade:** G · **Status:** PLANEJADO (é a T13.5)
- **Módulos existentes:** `robots/mt5_tools.common_files_dir`; hoje o mt5x só lê dessa pasta · **Depende de:** A-04, A-10 · **Pré-requisitos:** saber se um robô no Strategy Tester lê arquivo da pasta comum
- **Riscos:** robô lendo arquivo pela metade ou vencido; valor neutro inventado quando falta dado.
- **Aceite:** testes 9, 11 e 14 da especificação; dado ausente devolve `UNAVAILABLE` ou `STALE`.
- **Testes:** arquivo malformado, vencido e trocado durante a leitura; Linux/Wine e Windows.

### A-14 — Interface de Inteligência dos Ativos
- **O quê e por quê:** tela de pesquisa com filtros, mapa por dia e hora, distribuição condicional, catálogo de hipóteses e a origem de cada número.
- **Repositório:** mt5x · **Prioridade:** P2 · **Complexidade:** G · **Status:** PLANEJADO (é a T13.6)
- **Módulos existentes:** `ui/analysis.py`, painel vivo (`robots/panel_server.py`) · **Depende de:** A-06, A-13 · **Pré-requisitos:** as características existirem (não começar a tela antes)
- **Riscos:** tela que mostra probabilidade sem amostra; leitura que trava a interface.
- **Aceite:** todo número mostra amostra, validade e fonte; leitura em segundo plano.
- **Testes:** testes de janela com base sintética; base vazia e dado vencido.
