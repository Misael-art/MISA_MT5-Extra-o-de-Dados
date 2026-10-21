# Épico I — Pesquisa, otimização e validação

Repositório responsável: **mt5x**. Índice, convenções e regras comuns: [README.md](README.md).

**Os critérios de validação existentes são preservados.** O Filtro C (walk-forward, robustez e Monte Carlo, em
[estrategias.md](../estrategias.md)), a bateria de segurança e a paridade Python × MQL5 ([robos.md](../robos.md),
[sdk/exatidao.md](../sdk/exatidao.md)) continuam valendo como estão. Toda mudança de critério proposta aqui está
marcada como tal e só entra com aprovação (I-13).

**Harmonização de 2026-10-17:** as sugestões de robustez foram aceitas pelo proprietário
e detalhadas em [F20/Q01–Q16](ROBUSTEZ_PESQUISA_EXECUCAO.md) e
[T20.1–T20.30](PLANO_F20_ROBUSTEZ.md). F20 usa os motores/registros e donos existentes:
T20.8/9 qualificam I-07/I-08, T20.11 detalha I-06, T20.12/13 detalham registro/protocolo
I-03/I-04/I-09, T20.14/15 acrescentam compromissos/ablação e T20.18/19 qualificam I-12.
T19.27/28/52 consomem a mesma infraestrutura. A correção dos critérios propostos abaixo
é documental; não altera gates ou bateria em execução sem implementação versionada/testada.

### I-01 — Backtests por ativo e timeframe
- **O quê e por quê:** manter e estender o lote de backtests e a matriz de resultados para os módulos novos (sinal, execução, saída).
- **Repositório:** mt5x · **Prioridade:** P1 · **Complexidade:** P · **Status:** PLANEJADO (base entregue nas T11.5 e T11.6; extensão a módulos é nova)
- **Módulos existentes:** `robots/backtest.py` (`run_batch`), `robots/matrix.py`, `robots/runner.py` (T7.2) · **Depende de:** B-08 · **Pré-requisitos:** identificador de composição (qual sinal, qual execução, qual saída) gravado na execução
- **Riscos:** combinações demais e algumas dão lucro por acaso; a célula mostra a execução mais recente, não a melhor.
- **Aceite:** uma composição é uma linha distinta na matriz; as três regras da matriz continuam cobertas por teste.
- **Testes:** os de `tests/test_robots_batch.py` e `test_robots_matrix.py`, mais composições.

### I-02 — Teste de mecanismos de execução independente do sinal
- **O quê e por quê:** medir uma forma de entrar (limite, reteste, grade, fracionamento) com sinais sintéticos fixos, para saber o que é mérito da execução e o que é do sinal.
- **Repositório:** mt5x · **Prioridade:** P1 · **Complexidade:** G · **Status:** PROPOSTO
- **Módulos existentes:** `strategies/backtester.py` (entrada a mercado na barra seguinte) · **Depende de:** B-04, D-02 · **Pré-requisitos:** modelo de preenchimento de ordem pendente no backtester
- **Riscos:** preenchimento otimista de ordem limite; ambiguidade dentro da barra de um minuto.
- **Aceite:** mesmo conjunto de sinais, execuções diferentes, resultados comparáveis; preenchimento conservador declarado.
- **Testes:** casos sintéticos de toque sem preenchimento, gap sobre o nível e preenchimento parcial.

### I-03 — Walk-forward
- **O quê e por quê:** manter o walk-forward das estratégias e estendê-lo a composições e a hipóteses.
- **Repositório:** mt5x · **Prioridade:** P1 · **Complexidade:** M · **Status:** PLANEJADO na extensão a hipóteses (T13.4); existe para estratégias
- **Módulos existentes:** `strategies/validation.walk_forward` · **Depende de:** I-01 · **Pré-requisitos:** —
- **Riscos:** janelas que se sobrepõem ao horizonte do rótulo; reotimizar com o que foi visto no teste.
- **Aceite:** separação cronológica com embargo quando os horizontes se sobrepõem; critério atual inalterado para estratégias.
- **Testes:** os atuais de `tests/test_validation.py`, mais vazamento proposital que tem de ser pego.

### I-04 — Monte Carlo
- **O quê e por quê:** manter o Monte Carlo de operações e acrescentar o de ciclos (uma grade é uma unidade, não cinco operações independentes).
- **Repositório:** mt5x · **Prioridade:** P2 · **Complexidade:** M · **Status:** PROPOSTO na parte de ciclos; existe para operações
- **Módulos existentes:** `strategies/validation.monte_carlo`, bateria (`robots/battery.py`) · **Depende de:** H-01 · **Pré-requisitos:** livro que distingue operação de ciclo
- **Riscos:** embaralhar níveis de uma mesma grade esconde a perda concentrada.
- **Aceite:** reamostragem preserva unidade/dependência de ciclos, declara hipótese/seed
  e informa distribuição/quantis de risco. Não há exigência universal de que o drawdown
  por ciclo seja sempre maior ou igual ao por operação; comparar métodos com limites explícitos.
- **Testes:** ciclos sintéticos com perda concentrada, associação preservada, reprodução
  por seed e casos de diferentes ordenações; qualificação T20.13.

### I-05 — Análise de robustez
- **O quê e por quê:** robustez a parâmetros vizinhos, custo dobrado e subperíodos; hoje a de parâmetros não é avaliada para robôs MQL5.
- **Repositório:** mt5x · **Prioridade:** P2 · **Complexidade:** M · **Status:** PROPOSTO na parte MQL5; existe para estratégias Python
- **Módulos existentes:** `strategies/validation.robustness`, `robots/battery.py` (item "não avaliado nesta versão") · **Depende de:** I-08 · **Pré-requisitos:** repetir o Strategy Tester com parâmetros vizinhos de forma automática
- **Riscos:** tempo de máquina (cada teste MQL5 leva 10 a 30 s e não há paralelismo no mesmo terminal).
- **Aceite:** o item da bateria deixa de ser aviso e passa a ser medido.
- **Testes:** robô de exemplo com parâmetro frágil tem de reprovar.

### I-06 — Paridade Python × MQL5
- **O quê e por quê:** manter a paridade dos seis robôs como régua de toda refatoração e estendê-la a cada módulo novo que tenha par nas duas linguagens.
- **Repositório:** mt5x e HADOUKEN · **Prioridade:** P0 · **Complexidade:** M · **Status:** PLANEJADO (existe, T10.6; extensão é nova)
- **Módulos existentes:** `robots/parity.py`, [sdk/exatidao.md](../sdk/exatidao.md) · **Depende de:** B-08 · **Pré-requisitos:** linha de base medida de novo antes da etapa 2
- **Riscos:** refatorar e medir depois; diferença de custo confundida com diferença de lógica.
- **Aceite:** depois do adaptador (B-08), o lado MQL5 dos seis robôs dá os mesmos números de antes.
- **Testes:** `mt5x robots parity` nos seis, antes e depois.

### I-07 — Modelagem dos custos
- **O quê e por quê:** o backtester Python não limita margem e é otimista em custo. Falta margem por lote, slippage declarado e custo de ordem pendente.
- **Repositório:** mt5x · **Prioridade:** P1 · **Complexidade:** M · **Status:** PROPOSTO (spread medido e swap entraram na T10.12)
- **Módulos existentes:** `strategies/backtester.py`, `strategies/specs.py`, `_symbol_specs.spread_profile` · **Depende de:** — · **Pré-requisitos:** a ponte fornecer a margem por lote (hoje não fornece)
- **Riscos:** resultados abaixo de −100% hoje; entradas que a camada de segurança bloquearia.
- **Aceite:** nenhuma operação além da margem disponível; diferença para o Strategy Tester medida e registrada.
- **Testes:** casos de margem insuficiente; comparação com o Tester no robô de exemplo.

### I-08 — Testes de execução por ticks reais
- **O quê e por quê:** a decisão de operar se apoia no Strategy Tester com ticks reais. Estender isso aos mecanismos de execução e à grade.
- **Repositório:** mt5x · **Prioridade:** P1 · **Complexidade:** M · **Status:** PLANEJADO (existe para robôs, T8.5 e T10.5; extensão é nova)
- **Módulos existentes:** `robots/backtest.run_mql5`, `[ROBOTS] tester_model` · **Depende de:** D-02 · **Pré-requisitos:** ticks reais do período no terminal
- **Riscos:** modelos 1 e 2 do Tester escondem stop atravessado por gap (a bateria já não os aceita).
- **Aceite:** nenhuma aprovação de mecanismo com modelo que não seja por tick.
- **Testes:** o da bateria que recusa modelo inadequado, estendido aos módulos.

### I-09 — Detecção de sobreajuste
- **O quê e por quê:** correção para múltiplos testes, *Deflated Sharpe Ratio* e probabilidade de sobreajuste na escolha entre muitas composições.
- **Repositório:** mt5x · **Prioridade:** P2 · **Complexidade:** G · **Status:** PLANEJADO na parte de hipóteses (T13.4); PROPOSTO para composições
- **Módulos existentes:** nenhum · **Depende de:** I-01, I-03 · **Pré-requisitos:** catálogo de tudo o que foi testado, inclusive o rejeitado
- **Riscos:** critério novo reprovar o que hoje aprova (é mudança de critério: passa pela I-13).
- **Aceite:** o número de tentativas entra na conta; aprovação informa quantas alternativas foram testadas.
- **Testes:** protocolo congelado em múltiplas realizações de ruído e de efeito conhecido,
  medindo taxa de falsos positivos com intervalo e poder. Não prometer zero aprovações
  em toda realização aleatória; tentativas e pressupostos entram no relatório T20.12/13.

### I-10 — Comparação de estratégias por regime
- **O quê e por quê:** resultado de cada estratégia condicionado ao regime, fora da amostra.
- **Repositório:** mt5x · **Prioridade:** P2 · **Complexidade:** M · **Status:** PLANEJADO (é a T13.7)
- **Módulos existentes:** `robots/matrix.py` · **Depende de:** A-10, I-01 · **Pré-requisitos:** regime disponível com data de disponibilidade
- **Riscos:** poucas operações por regime; regime definido com dados do futuro.
- **Aceite:** célula com menos de 30 operações é marcada como tal.
- **Testes:** série sintética com estratégia boa em um regime só.

### I-11 — Otimização dos parâmetros dos módulos
- **O quê e por quê:** busca de parâmetros por módulo, com a validação fora da amostra como juiz. Hoje só existe a grade pequena do walk-forward.
- **Repositório:** mt5x · **Prioridade:** P3 no registro original; implementação por capacidade
  na F19 após protocolo · **Complexidade:** G · **Status:** PLANEJADO em T19.27/28/52,
  sem implementação presumida. A ideia de 2026-10-12 foi incorporada à F19 em 2026-10-17.
- **Módulos existentes:** `param_grid` das estratégias · **Depende de:** I-03, I-09 · **Pré-requisitos:** I-09 pronta (otimizar sem ela é fabricar sobreajuste)
- **Riscos:** o maior do épico: melhor parâmetro do passado apresentado como resultado.
- **Aceite:** todo parâmetro otimizado sai com o resultado fora da amostra e o número de tentativas.
- **Testes:** busca em ruído/efeito conhecido, reserva reutilizada e parâmetros frágeis;
  taxa de falso positivo/intervalo/poder conforme T20.13, sem garantia universal de zero.

### I-12 — Monitoramento de degradação de desempenho
- **O quê e por quê:** comparar o que o robô faz em demo com o que o backtest previa e avisar quando se afasta.
- **Repositório:** mt5x · **Prioridade:** P2 · **Complexidade:** M · **Status:** PROPOSTO (parte análoga para regras é a T13.8)
- **Módulos existentes:** `robots/live.py` (`monitor`), `notify.py` · **Depende de:** H-15 · **Pré-requisitos:** robô operando em demo (pendência da T8.7)
- **Riscos:** amostra pequena em demo; alarme a cada sequência normal de perdas.
- **Aceite:** o aviso informa a amostra e a diferença esperada por acaso.
- **Testes:** série que degrada e série estável.

### I-13 — Regras de validação para o que a bateria não cobre
- **O quê e por quê:** a bateria foi feita para robô de sinal automático. Painel manual, grade, execução fracionada e composições não têm critério. Definir, antes de qualquer um operar, o que cada tipo precisa provar.
- **Repositório:** mt5x · **Prioridade:** P0 · **Complexidade:** M · **Status:** PROPOSTO
- **Módulos existentes:** `robots/battery.py`, [sdk/contrato.md](../sdk/contrato.md) ("o que o robô não pode fazer") · **Depende de:** decisões 1 e 2 do índice · **Pré-requisitos:** aprovação do dono do projeto
- **Riscos:** afrouxar uma trava em vez de criar critério; critério diferente para cada caso sem registro.
- **Aceite:** cada tipo tem critérios escritos e testados; as proibições atuais (martingale, grid, preço médio) seguem valendo para quem não tem política aprovada; conta real nunca é o padrão.
- **Testes:** robôs de exemplo de cada tipo, um que deve aprovar e um que deve reprovar.
