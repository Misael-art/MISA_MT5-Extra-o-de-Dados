# Roadmap integrado HADOUKEN + mt5x: plataforma modular

> **Situação: backlog de planejamento. Nada aqui foi implementado por causa deste documento.** Registrado em
> 2026-10-13 a pedido do dono do projeto. Cada tarefa começa em `PROPOSTO` (ou `PLANEJADO`, quando já existe como
> tarefa do plano) e só muda de estado com evidência verificável.

Objetivo: evoluir o HADOUKEN (robôs MQL5) e o mt5x (dados, pesquisa e validação em Python) para uma plataforma
modular de pesquisa, desenvolvimento, validação e operação de robôs, **sem mudar o comportamento atual** enquanto
cada passo não for validado.

## Onde está cada épico

| Épico | Assunto | Repositório responsável | Documento | Tarefas |
|---|---|---|---|---|
| A | Base de dados e inteligência de mercado | mt5x | [EPICO_A_ANALYTICS.md](EPICO_A_ANALYTICS.md) | 15 |
| B | Framework modular de estratégias | HADOUKEN (integração: mt5x) | `Documentation/HADOUKEN_2/propostas/EPICO_B_FRAMEWORK_MODULAR.md` no HADOUKEN | 11 |
| C | Estratégias e indicadores | HADOUKEN e mt5x (par Python) | `.../EPICO_C_ESTRATEGIAS.md` no HADOUKEN | 12 |
| D | Mecanismos de entrada e execução | HADOUKEN (`Safety/`) | `.../EPICO_D_EXECUCAO.md` no HADOUKEN | 16 |
| E | Gestão de posições e saídas | HADOUKEN | `.../EPICO_E_POSICOES.md` no HADOUKEN | 13 |
| F | Gestão global de carteira | HADOUKEN (leitura: mt5x) | `.../EPICO_F_CARTEIRA.md` no HADOUKEN | 12 |
| G | Painel HADOUKEN | HADOUKEN (abertura e travas: mt5x) | `.../EPICO_G_PAINEL.md` no HADOUKEN | 17 |
| H | Estatísticas avançadas | HADOUKEN e mt5x | `.../EPICO_H_ESTATISTICAS.md` no HADOUKEN | 15 |
| I | Pesquisa, otimização e validação | mt5x | [EPICO_I_PESQUISA_VALIDACAO.md](EPICO_I_PESQUISA_VALIDACAO.md) | 13 |
| J | IA e agentes de desenvolvimento | mt5x e HADOUKEN | [EPICO_J_IA_AGENTES.md](EPICO_J_IA_AGENTES.md) | 9 |

Total: **10 épicos, 133 tarefas** (131 no registro inicial; A-15 e G-17 acrescentadas em 2026-10-13). O repositório HADOUKEN é `Misael-art/projeto-indicadores-mt5`; a evolução da
biblioteca `Misael-art/mt5x-safety` está descrita no `ROADMAP.md` de lá. As referências de estudo estão em
[REFERENCIAS.md](REFERENCIAS.md).

## Convenções do backlog

- **Identificador:** letra do épico e número (`A-03`, `D-07`). Não reaproveita a numeração `T<fase>.<n>` do
  [PLANO_DE_TRABALHO.md](../PLANO_DE_TRABALHO.md). Quando a proposta **já é** uma tarefa do plano, a entrada diz
  qual (`é a T13.1`) e não repete os passos: o detalhe mora no plano.
- **Campos de cada tarefa:** o quê e por quê, repositório, prioridade, complexidade, status, módulos existentes,
  dependências, pré-requisitos, riscos, critérios de aceite e testes.
- **Estados:** `PROPOSTO → PLANEJADO → EM DESENVOLVIMENTO → EM TESTE → VALIDADO → CONCLUÍDO`. `PLANEJADO` exige
  tarefa detalhada no plano; `VALIDADO` exige a evidência citada; `CONCLUÍDO` exige o merge.
- **Prioridade:** `P0` destrava ou protege as demais (segurança, contrato, decisão); `P1` caminho principal; `P2`
  importante, depois do caminho principal; `P3` experimental ou opcional.
- **Complexidade:** `P` (dias), `M` (uma a duas semanas), `G` (várias semanas), `GG` (precisa ser quebrada antes de
  começar). São estimativas de ordem de grandeza, não prazos.

## Ordem de implementação

Na ordem de dependência técnica. Uma etapa não começa interface que dependa de componente ainda inexistente.

| Etapa | O que entrega | Tarefas principais | Depende de |
|---|---|---|---|
| 0. Decisões e travas | Decisões abaixo tomadas; critério de bateria para o que hoje ela não cobre; pendências da Safety 7.1 fechadas | B-01, B-11, I-13, G-16, G-01 | — |
| 1. Dados confiáveis | Calendário, barras por sessão, guarda de características com data de disponibilidade | A-01, A-02, A-04, A-05, I-07 | 0 |
| 2. Contratos, sem mudar comportamento | Os seis contratos e o adaptador dos robôs atuais, com a paridade intacta | B-02 a B-08, D-01, E-01, E-05, E-06 | 0 |
| 3. Execução e carteira de base | Ordens pendentes no executor, cancelamento, recuperação após reinício, orçamento e reserva de risco | D-02, E-10, E-13, F-01 a F-05, F-09 a F-12 | 2 |
| 4. Livro de estatísticas | Negócio, ordem, posição, operação e ciclo como coisas distintas; KPIs conciliados | H-01 a H-10, H-14 | 3 |
| 5. Conteúdo analítico e publicação | Volume, sazonalidade, regimes, hipóteses, notícias no histórico; retrato lido pelos robôs | A-06 a A-10, A-13, A-15, C-12 | 1, 2 |
| 6. Estratégias de referência e Composer | Módulos de sinal novos, combinação sem duplicar código, teste de execução separado do sinal | C-01 a C-10, B-09, B-10, I-02, I-06 | 2, 4 |
| 7. Gradiente Linear | Grade fixa e dinâmica, ciclo e cesta, com orçamento agregado | D-06, D-07, E-11, E-12, F-06 a F-08, H-12 | 3, 4 |
| 8. Painel completo e inteligência | Painel modular recolhível com as abas; tela de inteligência dos ativos | G-02 a G-15, A-14, H-11, H-13, H-15 | 4, 5, 7 |
| 9. Avançados e experimentais | Execução algorítmica, grades de hedge, anti-martingale, aprendizado de máquina, agentes | D-08 a D-16, C-11, A-03, A-11, A-12, I-09 a I-12, J-01 a J-09 | 6, 7, 8 |

## Dependências críticas

1. ~~O executor não cria ordem pendente.~~ **Resolvida em 2026-10-13 (D-02):** o `HDK_SafeExecutor` cria, altera
   e cancela ordens limite, stop e stop-limite, validadas no Strategy Tester. Boleta, reteste, níveis fixos e
   Gradiente deixam de estar travados por isso; continuam dependendo do orçamento agregado (item 2).
2. **Não há orçamento de risco agregado.** O guardião (`HDK_AccountGuardian`) só observa. F-04 e F-05 são
   pré-requisito de qualquer mecanismo que abra mais de uma ordem por decisão (grade, fracionamento, scale-in).
3. **Conta netting nunca foi testada** e não há ativo da B3 no host. Tudo o que depende de posição por nível
   (alvo e stop por nível, grade de hedge, estatística por contribuição) fica bloqueado em netting até F-11.
4. **A paridade dos seis robôs é a régua da refatoração.** B-08 só é aceito com os mesmos números do lado MQL5.
5. **A bateria de segurança não tem critério para o que não é robô de sinal automático** (painel manual, grade,
   execução fracionada). I-13 e G-16 vêm antes de qualquer um deles operar.
6. **O backtester Python é otimista em custo e não limita margem.** A evidência que libera operação vem do
   Strategy Tester com ticks reais (I-07, I-08).
7. **Analytics depende de calendário de sessões**, que não existe (A-02).

## Compatibilidade e segurança (valem para todas as tarefas)

- **A arquitetura atual é preservada.** Módulo novo entra ao lado; nada existente é renomeado ou substituído sem
  necessidade registrada.
- **Compatibilidade retroativa dos robôs existentes:** os seis robôs de estratégia, o painel e o sentinela
  continuam compilando e com a mesma paridade a cada passo.
- **Contratos versionados.** O contrato do robô versão 1 ([sdk/contrato.md](../sdk/contrato.md)) **continua
  válido** até existir uma migração formalmente especificada (B-11).
- **Evolução controlada da `mt5x-safety`:** ela segue sendo cópia versionada da `Safety/` do HADOUKEN. Toda mudança
  na camada pede versão nova da biblioteca, teste do SDK e nova paridade dos seis robôs.
- **Risco individual e global:** toda entrada passa no limite do robô (`HDK_RiskGuard`) e, quando existir, no da
  conta (guardião).
- **Toda ordem é enviada, alterada ou cancelada pelo `HDK_SafeExecutor`.** Nenhum módulo novo chama `OrderSend`.
  O Python nunca envia ordens.
- **Netting e hedging:** todo mecanismo declara em qual tipo de conta vale e é testado nos dois, ou fica bloqueado
  no que não foi testado.
- **Estado persistente e reconciliação:** o que gere mais de uma ordem grava estado e se reconcilia com o
  terminal ao reiniciar.
- **Regras de validação para estratégias avançadas:** definidas em I-13 antes de qualquer uso; conta real nunca é
  o padrão.
- **As restrições atuais de martingale, grid e preço médio não são removidas nesta etapa.** O que hoje é proibido
  pelo contrato e pela bateria continua proibido; as propostas que tocam nisso (D-06 a D-10, D-15) dependem de
  uma política aprovada (`AveragingPolicy`, plano do HADOUKEN 2.0) e de avaliação própria.
- **Histórico e bases de dados preservados:** mudanças de schema são aditivas; barras, execuções e validações
  gravadas continuam legíveis.
- **Windows e Linux/Wine** são plataformas de primeira classe; o que só o Windows confirma vai para o checklist de
  [validacao_instaladores.md](../validacao_instaladores.md).

## Lacunas identificadas no código atual

Levantadas lendo o código em 2026-10-13. "Existe" quer dizer que o arquivo está lá, não que foi validado em operação.

| Assunto | O que existe | O que falta |
|---|---|---|
| Agregação de timeframes | `mt5x resample` (pelo relógio, até D1, só timeframes do MT5) | Sessão, calendário, cobertura por barra, timeframes customizados, Renko |
| Guarda de características | Indicadores calculados na hora (`strategies/indicators.py`, `study.py`) | Persistência versionada com data de disponibilidade |
| Estratégias Python | Seis (`ema_atr`, `donchian`, `bb_rsi`, `orb`, `pullback`, `squeeze`) e estratégias declaradas (`declared.py`) | MACD, retorno à média dedicado, price action, candlestick, Renko, VWAP, aprendizado de máquina |
| Motor MQL5 | `HDK_StrategyRobot.mqh`: sinal (`HDKSignal`) e saídas (`HDKExitRules`) no mesmo robô | Separação em contexto, sinal, execução, posição e risco; Composer |
| Execução | Mercado, ordens pendentes (desde a `mt5x-safety` 1.2.0), fechamento total e parcial, alteração de stop, break-even e trailing por ATR | Quem use as pendentes além da boleta do painel (níveis, reteste), grade, fracionamento, TWAP/VWAP |
| Carteira | `HDK_PortfolioRegistry` e sentinela que só observam; `mt5x portfolio` | Orçamento, reserva, líder, bloqueio, reconciliação, netting |
| Permissões | `ENUM_HDK_PERMISSION` e operações supervisionadas no executor | Nunca foram executadas; dependem do guardião |
| Painel | v7 com cinco abas (a quinta é a boleta de ordem pendente limite e stop, G-05) e três disposições | Recolhido, as outras abas novas e boletas, módulos |
| Estatísticas | `HDK_TradeStats` por magic; matriz de resultados no mt5x | Livro por operação e ciclo, custos, comparação com backtest |
| Validação | Walk-forward, robustez e Monte Carlo para estratégias; bateria; paridade | Múltiplos testes, sobreajuste, teste de execução separado do sinal, critério para painel e grade |
| IA | CLI para agentes (`mt5x --json`), scripts antigos em `Tools/` do HADOUKEN | Agentes com permissão definida, diagnóstico, explicações com evidência |

## Decisões que exigem aprovação do dono do projeto

| # | Decisão | Trava |
|---|---|---|
| 1 | Um painel de operação manual pode operar em demo só com a leitura do código aprovada, ou a bateria ganha critério próprio para painéis. **Respondida em 2026-10-13: em conta demo o painel manual opera sem bloqueio, salvo se a pessoa religar o bloqueio** (T12.L4 do plano). Conta real segue sem critério | G-16 |
| 2 | Política para preço médio controlado (`AveragingPolicy`): quem pode usar, com quais tetos e qual avaliação | D-06 a D-10, D-15 |
| 3 | Onde moram os contratos do framework (no HADOUKEN, na `mt5x-safety` ou em biblioteca nova) e como versionam. **Proposta em [DECISOES_CONTRATOS_E_CALENDARIO.md](DECISOES_CONTRATOS_E_CALENDARIO.md)**: duas pastas, uma biblioteca, uma versão | B-02 a B-07, B-11 |
| 4 | De onde vem o calendário de sessões e quem mantém feriados. **Proposta no mesmo documento**: passado deduzido das barras, grade semanal exportada pelo terminal, exceções em arquivo mantido | A-02 |
| 5 | Série contínua de futuros só para estudo ou também para características de execução | A-01, A-03 |
| 6 | Conta netting: abrir uma conta de teste com ativos da B3 antes de qualquer mecanismo por nível | F-11 |
| 7 | Limite de autonomia dos agentes de IA: o que podem fazer sem confirmação humana | J-09 |
| 8 | Aprendizado de máquina: só como filtro de pesquisa ou também como sinal em demo | C-11 |
| 9 | Ordem entre a Fase 12 (HADOUKEN 2.0) e a Fase 13 (Analytics) quando disputarem o mesmo tempo | etapas 1 e 3 |

## Pré-requisitos da primeira implementação

Antes de qualquer tarefa da etapa 1 ou 2 (os PRs de documentação já foram mesclados em 2026-10-13): decisões 3 e 4 tomadas; uma operação supervisionada do
executor exercitada (pendência da T12.1); a paridade dos seis robôs medida de novo como linha de base.
