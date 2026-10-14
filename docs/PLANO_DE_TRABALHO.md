# Plano de trabalho detalhado

Documento operacional para agentes (humanos ou IA, inclusive de menor capacidade)
implementarem o [ROADMAP](ROADMAP.md) **sem ambiguidade**. Antes de qualquer
tarefa, leia [AGENTS.md](../AGENTS.md) (regras obrigatórias).

## Como usar este plano

1. Pegue **a primeira tarefa não concluída** na ordem do ROADMAP cujas dependências estejam concluídas.
2. Crie um branch `tarefa/<ID>-<resumo>` (ex.: `tarefa/T1.1-upsert`).
3. Siga os **Passos** na ordem. Não pule etapas nem "melhore" coisas fora do escopo.
4. Rode **todos** os comandos da seção **Verificação**. Só continue se passarem.
5. Marque os itens de **Critérios de aceite**, faça um commit com a mensagem sugerida e abra o PR.
6. Atualize a coluna Status da tarefa na tabela abaixo (no mesmo PR).

Formato de cada tarefa: Objetivo · Depende de · Arquivos · Passos · Código de
referência · Critérios de aceite · Verificação · Armadilhas.

Comando de testes padrão (use o Python do `.venv`):

```bash
.venv/bin/python -m pytest -q          # Linux
.venv\Scripts\python -m pytest -q      # Windows
```

## Painel de status

| ID | Tarefa | Depende de | Status |
|---|---|---|---|
| T0.1 | Instaladores automáticos Windows/Linux + bootstrap | — | ✅ concluída |
| T0.2 | CI (GitHub Actions) com testes e instaladores | T0.1 | ✅ concluída |
| T0.3 | Validação manual dos instaladores em máquinas limpas | T0.1 | ⏳ parcial: Linux (família Arch) validado com MT5 real; demais ambientes pendentes (docs/validacao_instaladores.md) |
| T1.1 | Upsert no banco (reextrair sem erro/duplicata) | — | ✅ concluída |
| T1.2 | Salvar por bloco + tabela de controle + retomada | T1.1 | ✅ concluída |
| T1.3 | Atualização incremental | T1.2 | ✅ concluída |
| T1.4 | M1 via `copy_rates_range` | — | ✅ concluída |
| T1.5 | Não sobrescrever o spread histórico | — | ✅ concluída |
| T1.6 | Base de tempo (fuso) explícita | T1.1 | ✅ concluída |
| T1.7 | Nomes de tabela sem colisão | T1.1 | ✅ concluída |
| T1.8 | Relatório de qualidade dos dados | T1.2 | ✅ concluída |
| T2.1 | `mt5_backend`: MT5 local (Windows) ou via ponte (Linux) | T0.1 | ✅ concluída |
| T2.2 | Usar o backend em todo o código (sem `import MetaTrader5` direto) | T2.1 | ✅ concluída |
| T2.3 | Tirar Tkinter do núcleo | — | ✅ concluída |
| T2.4 | Gestão de processos multiplataforma | T2.2 | ✅ concluída |
| T2.5 | `initialize()` com `windows_path` no Linux | T2.2 | ✅ concluída (validação com MT5 real: T0.3/T2.6) |
| T2.6 | Teste de paridade Windows × Linux | T2.5 | ⏳ parcial: `scripts/parity_check.py` criado e lado Linux extraído; falta o lado Windows |
| T3.1 | CLI `mt5x` | T1.3, T2.2 | ✅ concluída |
| T3.2 | Agendamento (cron/systemd/Agendador de Tarefas) | T3.1 | ✅ concluída (cron e schtasks; systemd não) |
| T3.3 | `pyproject.toml` (substitui `setup.py`) | — | ✅ concluída |
| T3.4 | Timeframes como enum próprio | T2.2 | ✅ concluída |
| T4.1 | Remover arquivos mortos e unificar pontos de entrada | T0.1 | ✅ concluída |
| T4.2 | Logging centralizado | — | ✅ concluída |
| T4.3 | Provedor MT5 falso + testes do extrator e do banco | T2.1 | ✅ concluída (cobertura: banco 70%, extrator 72%, backend 93%; mínimo de 60% verificado no CI) |
| T4.4 | Atualizar `docs/arquitetura.md` e `README.md` | F1, F2 | ✅ concluída |
| T4.5 | Remover módulos "enhanced" não usados | T4.3 | ✅ concluída (3 removidos; 2 mantidos com justificativa) |
| T5.1 | Exportação Parquet / DuckDB | T1.1 | ✅ concluída |
| T5.2 | PostgreSQL / TimescaleDB | T1.1 | ✅ concluída (hypertable conferida no host com TimescaleDB 2.30.2 em contêiner, 2026-10-12) |
| T5.3 | Coleta de ticks | T1.2 | ✅ concluída (CLI `mt5x ticks`; na GUI ainda não) |
| T5.4 | Snapshots do book (DOM) | T5.3 | ✅ concluída (CLI `mt5x book`) |
| T5.5 | Fonte externa real (importação CSV) | T1.2 | ✅ concluída |
| T5.6 | Pastas de dados por usuário (`platformdirs`) | T3.3 | ✅ concluída (sem dependência nova) |
| T6.1 | Indicadores puros (`strategies/indicators.py`) | — | ✅ concluída |
| T6.2 | Seis estratégias com interface comum (`gerar_sinais`) | T6.1 | ✅ concluída |
| T6.3 | Backtester com custos reais e dimensionamento por risco | T6.2 | ✅ concluída |
| T6.4 | Validação: walk-forward, robustez ±20%, Monte Carlo, Filtro C | T6.3 | ✅ concluída |
| T6.5 | Especificações do símbolo (`_symbol_specs`) e leitura de OHLCV do banco | T1.1 | ✅ concluída |
| T6.6 | Triagem de ativos: Filtro A (eliminatório) e Filtro B (score 0–100) | T6.1, T6.5 | ✅ concluída |
| T6.7 | CLI `strategies/specs/backtest/validate/screen/report` + relatório HTML + menu na GUI | T6.4, T6.6 | ✅ concluída |
| T6.8 | Documentação `docs/estrategias.md` | T6.7 | ✅ concluída |
| T7.1 | Interface em abas com tema, inventário da base e análise (gráficos e números de estudo) | T1.8, T6.1 | ✅ concluída (teste da janela roda só onde há display; no CI Linux ele é pulado) |
| T7.2 | Tabelas de ativos (favorito, volume, robô lucrativo, status, benchmark, seleção), ordenação e bateria de backtests em segundo plano pela interface | T7.1, T8.3, T11.5 | ✅ concluída (símbolos na tela, robô MQL5 com MT5 real e Windows não conferidos; veja a tarefa) |
| T8.0 | Plano da Fase 8 (robôs) e mudança de escopo registrada | T7.1 | ✅ concluída |
| T8.1 | Catálogo de robôs: leitura de MQL5/presets, classificação de risco, tabelas `_robots*`, CLI `mt5x robots` | T8.0 | ✅ concluída (só a tabela `_robots`; `_robot_runs` e `_robot_checks` entram nas tarefas que as usam) |
| T8.2 | Aba "Robôs": cards, filtros, importação e painel "Visão geral" | T8.1 | ✅ concluída (mini-curva de capital fica para a T8.3, que grava as execuções) |
| T8.3 | Backtest pelo card (robôs Python) e aba Backtest | T8.2 | ✅ concluída |
| T8.4 | Ferramentas MT5: instalar arquivos, compilar e ler o log do MetaEditor | T8.1 | ✅ concluída (só CLI; o botão no card fica para quando a aba ganhar ações de MT5) |
| T8.5 | Backtest MQL5 pelo Strategy Tester (ini, relatório, CSV de trades) | T8.4 + HADOUKEN v7 compilando | ✅ concluída (só CLI; o HADOUKEN v7 compila e roda no Tester, sem operações por ser um painel) |
| T8.6 | Bateria de segurança (estática + estatística) | T8.3, T8.5 | ✅ concluída (só CLI; em MQL5, robustez ±20% fica como aviso "não avaliado") |
| T8.7 | Tempo real em demo: travas, início, monitoramento, PARAR TUDO | T8.6 | ✅ concluída: os cinco itens da mecânica provados em conta demo com o MT5 real (trava da bateria dispensada só nesse teste); nenhuma operação ocorreu |
| T8.8 | Tempo real em conta real: bateria operacional e travas extras | T8.7 + período em demo | a fazer |
| T8.9 | Robôs de atuação: portar as seis estratégias para MQL5 (`TryEnter`) e checar paridade | T8.5 | ⏳ parcial: seis robôs portados e compilando (PR #2 do repositório HADOUKEN); nenhum cumpre a paridade nem passa na bateria |
| T9.1 | CLI para agentes: saída `--json`, `mt5x status`, `analyze`, `commands` e `robots backtest` | T8.3 | ✅ concluída |
| T9.2 | `mt5x audit`: segurança do projeto, do patrimônio e do capital | T9.1 | ✅ concluída |
| T9.3 | Ponte restrita: só funções de leitura, sem execução de código nem pickle | T9.2 | ✅ concluída (reinicie a ponte depois de atualizar) |
| T10.0 | Plano da Fase 10 (SDK de robôs) e regra da pendência do Windows | T8.6, T9.3 | ✅ concluída |
| T10.1 | Contrato do robô v1: especificação, esquema do `robot.json` e `mt5x robots validate` | T8.6 | ✅ concluída |
| T10.2 | Biblioteca de segurança MQL5 própria e versionada, instalada pelo SDK | T8.4 | ✅ concluída (biblioteca só compilada: nenhum robô operou com ela) |
| T10.3 | `mt5x robots new`: esqueleto de robô sobre a biblioteca de segurança | T10.1, T10.2 | ✅ concluída (validada no Strategy Tester real) |
| T10.4 | `mt5x robots check`: instalar, compilar, testar e rodar a bateria em um comando, com relatório para CI | T10.1 | ✅ concluída (executada de ponta a ponta no host) |
| T10.5 | Bateria completa para MQL5: robustez ±20% e fora da amostra repetindo o Strategy Tester | T10.4 | ✅ concluída (executada no host) |
| T10.6 | Paridade Python × MQL5: `mt5x robots parity` (absorve a parte de comparação da T8.9) | T10.3, T8.5 | ✅ concluída (um par real comparado na T10.10; a ferramenta apontou as diferenças) |
| T10.7 | API Python pública e estável (`mt5_extracao.sdk`), com versão e política de mudança | T10.4 | ✅ concluída |
| T10.8 | Documentação do desenvolvedor: tutorial, referência do contrato e robô de exemplo | T10.3, T10.7 | ✅ concluída (todas as saídas do tutorial são de uma execução real no host) |
| T10.9 | Pacote publicável: versão, extras, changelog e publicação | T10.8, **T10.W** | a fazer |
| T10.10 | **Opcional.** Geração de MQL5 a partir de uma estratégia declarada em Python (`mt5x robots generate`) | T10.3, T10.6 | ✅ concluída: paridade real dentro do critério aprovado pelo dono (97,9% casadas, 96,5% dos pares a até 5%, diferença de 0,6% do giro bruto) |
| T10.11 | **Opcional.** Preparação para o MQL5 Market: conferência e pacote (`mt5x robots market`) | T10.4 | em andamento: código e testes automáticos prontos; falta backtest no MT5 conectado |
| T10.12 | **Pendência registrada.** Custo real no backtester Python: fechar a diferença para o Strategy Tester com ticks reais | T10.10 | ✅ concluída: no cenário medido, 0,00% do giro contra os ticks reais (era 1,63%); exige calibrar o spread com ticks |
| T11.0 | Plano da Fase 11 (testes amplos e operação discreta) | T10.8 | ✅ concluída |
| T11.1 | Montar um timeframe maior a partir de uma base menor (`mt5x resample`) | T1.1 | ✅ concluída (igual ao MT5 em EURUSD; ativos da B3 não conferidos) |
| T11.2 | Backtest em vários ativos de uma vez, com quadro comparativo | T11.1, T8.5 | ✅ concluída (executada no host em dois ativos reais) |
| T11.3 | MT5 oculto: terminal e Strategy Tester sem ocupar a tela | T8.5 | ✅ concluída no Linux (conferida no host); Windows não validado |
| T11.4 | Avisos em pop-up no computador a cada ação executada, inclusive as do robô | T11.3 | ✅ concluída para as ações do programa; os avisos de operações do robô só com MT5 simulado (dependem da T8.7) |
| T11.5 | Lote de backtests: robôs × ativos × timeframes em fila, com retomada | T11.2 | ✅ concluída (executada no host) |
| T11.6 | Matriz de resultados (ativo × timeframe × robô) como camada de dados | T11.5 | ✅ concluída |
| T11.7 | Painel visual em HTML: visão por ativo e visão por robô | T11.6 | ✅ concluída (conferida com dados reais do host) |
| T11.8 | Botão na interface para abrir o painel de resultados | T11.7 | ✅ concluída |
| T11.9 | Painel vivo: servidor local, atualização sozinha e ações que não mexem no MT5 | T11.8 | ✅ concluída (conferida no host) |
| T11.10 | Painel vivo: ações que abrem ou fecham o MT5 (atualizar base, especificação, teste MQL5) | T11.9 | ✅ concluída (conferida no host, com o MT5 oculto) |
| T12.0 | Plano do HADOUKEN 2.0 registrado e diagnóstico do código atual (etapa 0) | T8.9 | ✅ concluída |
| T12.1 | Safety ampliada: permissões e operações supervisionadas com auditoria (etapa 1) | T12.0 | ⏳ parcial: compila; operação supervisionada fica para depois da T12.3 e só em demo |
| T12.2 | Carteira somente leitura: registro, sentinela da conta e `mt5x portfolio` (etapa 2) | T12.1 | ⏳ parcial: implementação F-03 em curso; conciliação demo com posições e netting pendentes |
| T12.L | Organização da janela do MT5 ao iniciar um robô (janelas laterais fechadas, perfil próprio, gráfico maximizado) | T8.7 | ✅ concluída no Linux (vista em 1280×800, ligada e desligada) |
| T12.P | Painel v7 responsivo (compacto, normal, amplo) e bateria de testes dos dois painéis | T12.2 | ✅ concluída no Linux; tema claro e disposição normal vistos na T12.6a; Windows pendente |
| T12.L1 | Organização da janela configurável por partes (janelas laterais, barras, perfil próprio) e devolução sem regravar o `terminal.ini` inteiro | T12.L | ✅ concluída; partes e devolução vistas no terminal real nos testes da T12.L3 |
| T12.L2 | Opções da organização da janela na interface (diálogo Configurações), gravando o `config.ini` sem perder comentários | T12.L1 | ✅ concluída no Linux (vista em tela virtual, temas claro e escuro); Windows não visto |
| T12.L3 | "Abrir visão MT5": botão na interface e `mt5x mt5 view`, que aplicam a organização quando o usuário pedir; disposição do painel nas opções | T12.L2 | ✅ concluída e testada no MT5 real do host (tela virtual); a disposição do painel com robô de verdade e o Windows não foram vistos |
| T12.L4 | Painel manual liberado para operar em conta demo, com opção de religar o bloqueio | T12.L3 | ✅ concluída (o painel operando em demo não foi exercitado) |
| T12.6a | Painel v7: conferir o que ficou sem ver (disposição normal, tema claro, tela real) e fixar o vocabulário | T12.P | ✅ concluída no Linux (2026-10-14): normal forçada, ajuste derivado em 70%, paleta clara e tela eDP-1 1280×800 vistos; linha de base sem painel registrada; tentativa de VENDA bloqueada, sem execução nem mudança de posição; Windows pendente |
| T12.3 | Guardião global: orçamentos por robô, reserva, líder, parada e auditoria (etapa 3) | T12.2 | ⏳ em implementação autorizada; operações supervisionadas serão exercitadas depois da etapa, apenas em demo |
| T12.4 | Gradiente Linear básico: ordens pendentes no executor, boleta, grade fixa (etapa 4) | T12.3 | ⏳ parcial: ordens pendentes no executor (D-02) e boleta de ordem pendente no painel (G-05, limite e stop, exercitada em conta demo) entregues; a grade não começou |
| T12.5 | Gradiente Linear dinâmico (etapa 5) | T12.4 | a fazer |
| T12.6 | Painel completo: recolhido, compacto, abas e boletas (etapa 6) | T12.6a; cada aba depende da sua etapa (veja "Revisão das etapas do painel") | ⏳ parcial: T12.6a concluída; T12.6b e T12.6c a fazer (dividida na revisão de 2026-10-13) |
| T12.6A | Estatísticas: livro por operação e por grade, KPIs, exportação e relatório no mt5x (etapa 6A) | T12.2 | a fazer |
| T12.7 | Copiloto de IA somente leitura (etapa 7) | T12.6A | a fazer |
| T12.8 | Demo controlada da operação integrada (etapa 8) | T12.3 a T12.6A | a fazer |
| T13.0 | Plano do MT5x Analytics registrado e levantamento do que já existe | T11.1 | ✅ concluída (o HADOUKEN não foi lido) |
| T13.1 | Calendário de sessões e barras derivadas do M1 por sessão, com estado de qualidade (A1) | T11.1 | a fazer |
| T13.2 | Guarda de características com data de disponibilidade e consulta *as-of* (A2) | T13.1 | a fazer |
| T13.3 | Perfis de volume, sazonalidade e regimes (A3) | T13.2 | a fazer |
| T13.4 | Hipóteses H01 a H05 e motor de avaliação com validação temporal (A4) | T13.3 | a fazer |
| T13.5 | Publicador do retrato e leitor MQL5 somente leitura (A5) | T13.3, T12.1 | a fazer |
| T13.6 | Tela de pesquisa no mt5x e aba "Inteligência do Ativo" no painel do HADOUKEN (A6) | T13.4, T13.5, T12.6 | a fazer |
| T13.7 | Estratégia × regime e pilotos opt-in no robô (A7) | T13.5, T12.3 | a fazer |
| T13.8 | Deriva, alertas e governança das regras (A8) | T13.4, T13.7 | a fazer |
| T14.0 | Backlog integrado HADOUKEN + mt5x registrado (10 épicos, 131 tarefas, em `docs/propostas/` e no HADOUKEN) | T12.0, T13.0 | ✅ concluída (só documentação) |
| T15.0 | Plano da gestão de recursos (aba Recursos) registrado em `docs/propostas/GESTAO_RECURSOS.md` | — | ✅ concluída |
| T15.1–T15.6 | Gestão de recursos: medição, higienização, arquivo reversível, `mt5x resources`, automação opt-in e aba Recursos | T15.0 | ✅ concluída no Linux (aba vista em tela virtual, temas claro e escuro); Windows não visto |
| T15.7 | Cópia de segurança da base e limpeza de logs e exportações antigos (gestão de recursos) | T15.6 | ✅ concluída no Linux; Windows não visto |
| T15.8 | Gestão de recursos: arquivo em Parquet, aparar e retenção por idade, aviso por série e voltar a uma cópia pela interface | T15.7 | ✅ concluída no Linux; PostgreSQL real e Windows não vistos |
| T15.9 | Gestão de recursos: limites que bloqueiam a gravação na extração, retenção por ativo e `drop_chunks` do TimescaleDB | T15.8 | ✅ concluída no Linux; TimescaleDB real e Windows não vistos |
| T15.10 | Próximas frentes da gestão de recursos (guarda em ticks e book, indicadores no painel vivo) | T15.9 | proposta |
| T16.0 | Plano da aba Laboratório (analytics de operações: contexto, re-simulação de execução, insights, lente por robô e por ativo) em `docs/propostas/ANALYTICS_OPERACOES.md` | — | ✅ plano registrado, com as decisões do dono (seção 8) |
| T16.1 | Laboratório: livro de operações `_trade_ledger` (gravar, ler pelas lentes robô e ativo) | T16.0 | ✅ concluída |
| T16.2 | Laboratório: importadores para o livro (backtest Python, CSV do HADOUKEN nos dois formatos, runs antigos) e `mt5x trades import/runs` | T16.1 | ✅ concluída no mt5x; a T16.2b (colunas novas no CSV) é do HADOUKEN |
| T16.3a | Laboratório: calendário econômico de várias fontes (CSV e exportação do MT5 por MQL5), unido e enriquecido | T16.2 | ✅ concluída no Linux (script compilado no host; exportação com o terminal conectado não exercitada) |
| T16.3 | Laboratório: enriquecimento das operações (calendário, notícia, volume, volatilidade point-in-time, MAE/MFE) | T16.3a | ✅ concluída |
| T16.4 | Laboratório: agregações das duas lentes e estatística (bootstrap em blocos, amostra mínima) | T16.3 | ✅ concluída |
| T16.5 | Laboratório: cache do enriquecimento por run, com assinatura (versão, configuração, operações, calendário) | T16.4 | ✅ concluída |
| T16.11a | Laboratório: aba (lentes por robô e por ativo; guias Visão geral, Tempo e calendário, Microestrutura e Comparativo) | T16.5 | ✅ concluída no Linux (tela virtual, temas claro e escuro); Windows não visto |
| T16.6 | Laboratório: motor de re-simulação de execução com paridade com o backtester (seis estratégias) | T16.5 | ✅ concluída |
| T16.7 | Laboratório: modos de saída (alvo em R, break-even, trailing por ATR e por barras) e comparação entre modos | T16.6 | ✅ concluída |
| T16.8–T16.12 | Laboratório: livro de operações, importadores, calendário econômico de várias fontes (CSV e MT5), enriquecimento, agregações, cache, re-simulação (ponto fixo, saídas, gradientes, martingale só para robô que o suporta, com as travas dele), regras, insights, aba e `mt5x trades` | T16.0 | a fazer |
| T10.W | Validação no Windows de tudo o que foi feito só no Linux (checklist) | máquina Windows disponível | ⏳ pendência aberta; **não bloqueia as demais tarefas** |

---

## Fase 0 — Fundação

### T0.1 — Instaladores automáticos + bootstrap ✅

Já implementado. O que existe e **não deve ser quebrado**:

| Arquivo | Papel |
|---|---|
| `install.bat` → `scripts/install/windows.ps1` | Instalador Windows (Python via winget/python.org, `.venv`, MT5 `/auto`, configuração, atalho) |
| `install.sh` → `scripts/install/linux.sh` | Instalador Linux (apt/dnf/pacman/zypper, Wine/WineHQ, MT5 no Wine, Python do Windows no Wine, ponte) |
| `run.bat`, `run.sh` | Lançadores do app |
| `scripts/mt5_bridge_server.py` | Servidor RPyC que roda **no Python do Wine** e expõe `MetaTrader5` |
| `scripts/mt5-bridge.sh` | start/stop/status da ponte |
| `mt5_extracao/bootstrap/` | `configure`, `doctor`, `locate-mt5`. **Somente biblioteca padrão** |
| `config/config.ini.example` | Referência das chaves de configuração |
| `requirements*.txt` | Dependências (runtime / opcionais / dev) |
| `tests/test_bootstrap_*.py` | Testes do bootstrap e da ponte |

Valores que precisam ficar **iguais** em mais de um arquivo (se mudar um, mude todos):

| Valor | Onde |
|---|---|
| Versão do rpyc (`6.0.2`) | `requirements.txt`, `scripts/install/linux.sh` (`RPYC_VERSION`) |
| Pasta do Python no Wine (`C:\Python311`) | `mt5_extracao/bootstrap/paths.py` (`WINE_PYTHON_WINDOWS_DIR`), `scripts/install/linux.sh` (`WIN_PYTHON_DIR_WIN`, `WIN_PYTHON_DIR_UNIX_REL`) |
| Porta da ponte (`18812`) | `paths.py` (`DEFAULT_BRIDGE_PORT`), `scripts/mt5_bridge_server.py` |
| Faixa de Python aceita (3.9–3.13) | `doctor.py` (`MIN_PYTHON`, `MAX_TESTED_PYTHON`), `linux.sh` (`MIN_PY_MINOR`), `windows.ps1` (`$PythonMinor`, `$PythonMaxMinor`) |

### T0.2 — CI ✅

`.github/workflows/ci.yml`: shellcheck, parse do PowerShell, pytest (Ubuntu e Windows),
execução real de `install.sh --yes --no-mt5` e `install.bat -Yes -NoMT5 -NoShortcut`.
**Regra:** todo PR deve manter o CI verde. Não desative jobs nem testes.

### T0.3 — Validação manual dos instaladores

**Objetivo:** confirmar em máquinas limpas as etapas que o CI não cobre (download e
instalação do MT5, Wine, ponte com o MT5 real).

**Passos:** para cada ambiente abaixo, em uma VM limpa, execute o instalador e preencha
a tabela em `docs/validacao_instaladores.md` (crie o arquivo):

| Ambiente | Comando | Verificar |
|---|---|---|
| Windows 11 (usuário comum) | duplo clique em `install.bat` | UAC do MT5 aparece uma vez; atalho criado; `doctor` sem FALHA |
| Windows 10 sem winget | idem | Python instalado pelo python.org |
| Windows com MT5 de corretora já instalado | idem | `locate-mt5` encontra a pasta da corretora; nada é reinstalado |
| Ubuntu 24.04 desktop | `./install.sh` | WineHQ instalado; MT5 abre; `./scripts/mt5-bridge.sh status` = ATIVA; `doctor` com "Ponte MT5 (RPyC): OK" |
| Ubuntu 22.04 servidor (sem X) | `./install.sh --yes` | Xvfb usado; instalação conclui |
| Debian 12 | `./install.sh` | idem Ubuntu |
| Fedora 40 | `./install.sh` | Wine via dnf |
| Reexecução | rodar o instalador 2× | 2ª execução pula etapas e não altera `config.ini` |

**Critérios de aceite:** tabela preenchida; cada falha vira uma issue com o log anexado
(`logs/install-*.log`).

**Armadilhas:** o `mt5setup.exe /auto` abre o terminal ao final (o instalador Linux o fecha com
`wineserver -k`). Algumas corretoras distribuem um instalador próprio; nesse caso use
`--mt5-path` / `-MT5Path`.

---

## Fase 1 — Integridade dos dados

### T1.1 — Upsert no banco (reextrair sem erro/duplicata)

**Objetivo:** hoje `DatabaseManager.save_ohlcv_data` usa `to_sql(if_exists='append')` numa
tabela com `time` como PRIMARY KEY. Reextrair qualquer período que já exista gera
`sqlite3.IntegrityError: UNIQUE constraint failed` e **o lote inteiro é descartado**
(comportamento reproduzido e confirmado). É preciso gravar com *upsert*: insere o que é
novo e atualiza o que já existe.

**Depende de:** nada. **Arquivos:** `mt5_extracao/database_manager.py`, `tests/test_database_upsert.py` (novo).

**Passos:**
1. Em `database_manager.py`, adicione os imports:
   `from sqlalchemy import MetaData, Table` e `from sqlalchemy.dialects.sqlite import insert as sqlite_insert`.
2. Adicione o método `_upsert_dataframe` (código abaixo) na classe `DatabaseManager`.
3. Em `save_ohlcv_data`, **substitua somente** a linha
   `df_to_save.to_sql(table_name, self.engine, if_exists='append', index=True, index_label='time')`
   por `self._upsert_dataframe(table_name, df_to_save)`. Mantenha todo o resto (preparação de colunas, logs, `except`).
4. Faça o mesmo em `save_data` (linha com `to_sql(..., if_exists='append', index=True)`), **somente se** a tabela tiver PK `time`; caso contrário mantenha `to_sql`. Para saber: `inspect(self.engine).get_pk_constraint(table_name)['constrained_columns'] == ['time']`.
5. Crie o teste descrito em Verificação.

**Código de referência (testado com SQLAlchemy 2.0 + SQLite):**

```python
    # Limite seguro de linhas por INSERT: SQLite aceita até 32766 parâmetros;
    # a tabela OHLCV tem ~70 colunas -> 200 linhas = ~14 mil parâmetros.
    _UPSERT_CHUNK_ROWS = 200

    def _upsert_dataframe(self, table_name, df):
        """Insere ou atualiza linhas pela chave primária 'time'. df indexado por 'time'."""
        table = Table(table_name, MetaData(), autoload_with=self.engine)
        table_cols = [c.name for c in table.columns]
        data = df.reset_index() if 'time' not in df.columns else df
        data = data[[c for c in table_cols if c in data.columns]]
        # NaN -> None (NULL) e Timestamp -> datetime (formato de gravação do SQLAlchemy)
        data = data.astype(object).where(pd.notna(data), None)
        records = data.to_dict(orient='records')
        for r in records:
            if isinstance(r['time'], pd.Timestamp):
                r['time'] = r['time'].to_pydatetime()
        with self.engine.begin() as conn:
            for i in range(0, len(records), self._UPSERT_CHUNK_ROWS):
                stmt = sqlite_insert(table).values(records[i:i + self._UPSERT_CHUNK_ROWS])
                update_cols = {c: stmt.excluded[c] for c in data.columns if c != 'time'}
                conn.execute(stmt.on_conflict_do_update(index_elements=['time'], set_=update_cols))
        return len(records)
```

O formato gravado continua `'2024-01-01 10:00:00.000000'` (texto), **idêntico** ao que o
`to_sql` já gravava, então bases existentes continuam compatíveis.

**Critérios de aceite:**
- [ ] Salvar o mesmo DataFrame 2× retorna `True` nas duas e a contagem de linhas não muda.
- [ ] Salvar de novo com um `close` alterado atualiza o valor.
- [ ] Linhas novas junto com linhas existentes: as novas são inseridas.
- [ ] Nenhuma outra linha de `save_ohlcv_data` foi alterada além da troca do `to_sql`.

**Verificação:** crie `tests/test_database_upsert.py` usando `tmp_path`:

```python
import pandas as pd
from sqlalchemy import text
from mt5_extracao.database_manager import DatabaseManager

def _df(close1=2.0):
    return pd.DataFrame({'time': pd.to_datetime(['2024-01-01 10:00', '2024-01-01 10:01']),
                         'open': [1.0, 2.0], 'high': [1.0, 2.0], 'low': [1.0, 2.0], 'close': [1.0, close1],
                         'tick_volume': [1, 2], 'spread': [1, 1], 'real_volume': [5, 6]})

def test_upsert(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / 't.db'))
    assert db.save_ohlcv_data('WIN$N', '1 minuto', _df())
    assert db.save_ohlcv_data('WIN$N', '1 minuto', _df(close1=99.0))
    with db.engine.connect() as c:
        rows = c.execute(text('SELECT close FROM win_n_1_minuto ORDER BY time')).fetchall()
    assert [r[0] for r in rows] == [1.0, 99.0]
```

Rode o pytest; todos os testes devem passar.

**Armadilhas:** `DatabaseManager` cria `logs/` no diretório atual ao ser importado (efeito
colateral conhecido; será resolvido em T4.2). Não use `INSERT OR REPLACE`: ele apaga a linha
e perde colunas de indicadores que não vieram no DataFrame. O `on_conflict_do_update`
atualiza só as colunas enviadas.

**Commit:** `fix(db): grava OHLCV com upsert para permitir reextração sem erro`

### T1.2 — Salvar por bloco + tabela de controle + retomada

**Objetivo:** hoje `HistoricalExtractor._process_symbol` acumula todos os blocos em memória
(`all_symbol_data`) e só salva no fim. Se um bloco falha, tudo é perdido (`break` +
`symbol_success = False`). Com 10 anos de M1 isso consome muita RAM. A mudança: salvar
**cada bloco** assim que chega, registrar o bloco numa tabela de controle e, numa nova
execução, pular blocos já concluídos.

**Depende de:** T1.1. **Arquivos:** `database_manager.py`, `historical_extractor.py`, `tests/test_extraction_log.py`.

**Passos:**
1. Em `DatabaseManager`, crie a tabela de controle (chamada em `_connect` após criar o engine):

   ```sql
   CREATE TABLE IF NOT EXISTS _extraction_log (
       table_name  TEXT NOT NULL,
       block_start TIMESTAMP NOT NULL,
       block_end   TIMESTAMP NOT NULL,
       rows        INTEGER NOT NULL,
       status      TEXT NOT NULL,      -- 'ok' | 'empty' | 'failed'
       source      TEXT NOT NULL,      -- 'mt5' | nome da fonte externa
       updated_at  TIMESTAMP NOT NULL,
       PRIMARY KEY (table_name, block_start, block_end)
   )
   ```
2. Adicione `record_block(table_name, start, end, rows, status, source)` (upsert via
   `INSERT ... ON CONFLICT(table_name, block_start, block_end) DO UPDATE SET ...`) e
   `completed_blocks(table_name) -> set[(start, end)]` (somente `status IN ('ok','empty')`).
3. Em `HistoricalExtractor._process_symbol`:
   - No início: `table_name = self.db_manager.get_table_name_for_symbol(symbol, timeframe_name)` e `done = self.db_manager.completed_blocks(table_name)` (se `overwrite=True`, use `done = set()`).
   - No laço `while current_start < end_date`: se `(current_start, block_end) in done`, avance para o próximo bloco **sem** chamar o MT5.
   - Após obter `block_df` com sucesso: calcule indicadores **do bloco** (ver passo 4), chame `save_ohlcv_data` e depois `record_block(..., status='ok' if len(block_df) else 'empty', source=...)`.
   - Em falha definitiva do bloco: `record_block(..., rows=0, status='failed', ...)` e **continue** para o próximo bloco (não use `break`). O símbolo termina como falha se algum bloco falhou.
   - Remova `all_symbol_data` e o `pd.concat` final.
4. Aquecimento dos indicadores: indicadores com janela (RSI 14, MA 20, etc.) precisam de barras
   anteriores. Antes de calcular os indicadores do bloco, leia as últimas `WARMUP_BARS = 500`
   linhas da tabela com `time < current_start`
   (`SELECT * FROM "<tabela>" WHERE time < :t ORDER BY time DESC LIMIT 300`), concatene antes do
   bloco, calcule e **descarte** as linhas de aquecimento antes de salvar (filtre `time >= current_start`).
   (500 e não 300: RSI e MACD usam médias exponenciais; com 300 barras a diferença para o período
   inteiro fica em ~1e-8, acima da tolerância de aceite.)
5. Os blocos devem ter limites determinísticos para a retomada funcionar: use sempre
   `block_end = min(current_start + block_delta, end_date)` e
   `current_start = block_end + timedelta(seconds=1)`, como já é feito. Não mude essa aritmética.

**Critérios de aceite:**
- [ ] Um bloco que falha não impede a gravação dos outros.
- [ ] Reexecutar a mesma extração não chama o MT5 para blocos `ok`/`empty` (teste com provedor falso contando chamadas; se T4.3 não existir, use `unittest.mock.MagicMock` no `connector`).
- [ ] Indicadores do primeiro registro de um bloco são iguais aos obtidos extraindo o período inteiro de uma vez (tolerância `1e-9`).
- [ ] Uso de memória não cresce com o número de blocos (não há mais acumulação).

**Implementado também:** tabelas internas (prefixo `_`) ficam fora de `get_all_tables`/`get_existing_symbols`;
`delete_data_periodo` passou a comparar datas no mesmo formato de texto gravado (antes a barra do limite final
não era apagada). A retomada só pula blocos com **os mesmos limites**; um período total diferente gera outros
limites e os blocos são buscados de novo (o upsert evita duplicatas).

**Armadilhas:** o `HistoricalExtractor` roda símbolos em paralelo (`ThreadPoolExecutor`). Cada
símbolo tem sua própria tabela, mas a `_extraction_log` é compartilhada: use `engine.begin()`
por gravação (transação curta). O SQLite serializa escritas. Se ocorrer `database is locked`,
adicione `connect_args={'timeout': 30}` no `create_engine` do SQLite.

**Commit:** `feat(extracao): salva por bloco com tabela de controle e retomada`

### T1.3 — Atualização incremental

**Objetivo:** botão/comando "Atualizar" que busca apenas o que falta desde o último registro.

**Depende de:** T1.2. **Arquivos:** `database_manager.py`, `historical_extractor.py`, `ui_manager.py` (botão), testes.

**Passos:**
1. `DatabaseManager.get_last_timestamp(table_name) -> datetime | None`: `SELECT MAX(time) FROM "<tabela>"` e converta com `pd.to_datetime`. Retorne `None` se a tabela não existir (`inspect(engine).has_table`).
2. `HistoricalExtractor.update_symbols(symbols, timeframe_val, timeframe_name, include_indicators, ...)`: para cada símbolo, `start = last_ts - timedelta(minutes=timeframe_minutes)` (sobreposição de 1 barra, resolvida pelo upsert) ou, se não houver dados, `start = datetime.now() - timedelta(days=30)`. `end = datetime.now()`. Reutilize `extract_data` com `overwrite=False`.
3. Adicione na aba de extração da UI um botão "Atualizar até agora" que chama `update_symbols` com os símbolos selecionados. Siga o padrão do botão existente que chama `self.app.historical_extractor.extract_data(` em `ui_manager.py`.

**Critérios de aceite:** rodar a atualização 2× seguidas não cria linhas duplicadas; a 2ª traz 0 ou 1 barra nova.

**Implementado:** `extract_data(..., start_dates={símbolo: início})` (opcional) permite um início por símbolo;
`HistoricalExtractor.TIMEFRAME_MINUTES` dá o tamanho da barra. A última barra do dia pode estar incompleta
quando a atualização roda durante o pregão; a próxima atualização a corrige (upsert).

**Commit:** `feat(extracao): atualização incremental a partir do último registro`

### T1.4 — M1 via `copy_rates_range`

**Objetivo:** hoje, para M1, o extrator pede `bars=200000` a partir de `current_start` e filtra
depois (em `historical_extractor.py`, bloco `if timeframe_val == mt5.TIMEFRAME_M1:` dentro do
laço de retry). Isso baixa muito mais dados que o bloco e esbarra no limite "Max bars in chart"
do terminal.

**Depende de:** nada. **Arquivos:** `historical_extractor.py`.

**Passos:** no laço de retry, remova o ramo especial de M1 e use para **todos** os timeframes a
chamada que hoje está no `else`:
`self.connector.get_historical_data(symbol, timeframe_val, start_dt=current_start, end_dt=block_end, bars=None)`.

**Critérios de aceite:** o bloco M1 retorna somente barras entre `current_start` e `block_end`;
nenhuma chamada usa `bars=200000`. Se o MT5 retornar vazio para períodos antigos, o log deve dizer
"sem dados no período" e não "falha".

**Armadilhas:** para históricos M1 muito antigos o terminal pode precisar baixar dados do
servidor. `copy_rates_range` retorna o que estiver disponível. Não aumente o `chunk_days_m1`
acima de 30.

**Commit:** `fix(extracao): usa copy_rates_range também para M1`

### T1.5 — Não sobrescrever o spread histórico

**Objetivo:** em `historical_extractor.py` (bloco "Calcular indicadores se solicitado"), a linha
`final_df['spread'] = symbol_info.spread` troca o spread histórico de **todas** as barras pelo
spread do momento da extração. O MT5 já retorna o spread correto por barra.

**Passos:** remova as linhas `symbol_info = self.connector.get_symbol_info(symbol)`,
`if symbol_info:` e `final_df['spread'] = symbol_info.spread`.

**Critérios de aceite:** a coluna `spread` salva é idêntica à devolvida por `copy_rates_range`.

**Commit:** `fix(extracao): preserva o spread histórico por barra`

### T1.6 — Base de tempo explícita

**Objetivo:** o MT5 devolve `time` em segundos no **horário do servidor da corretora** (na B3,
normalmente UTC−3). Hoje o valor é gravado sem nenhuma indicação disso.

**Passos:**
1. Adicione em `config_builder.DEFAULTS` a seção `("APP", OrderedDict([("time_basis", "broker")]))`
   (valores aceitos: `broker`, que é o padrão e o comportamento atual, e `utc`). Adicione a chave
   em `config/config.ini.example`.
2. Crie a tabela `_metadata(key TEXT PRIMARY KEY, value TEXT)` e grave `time_basis` na primeira
   gravação. Se a base já tiver um valor diferente do configurado, **recuse gravar** e registre um
   erro claro (misturar bases corrompe a série).
3. Se `utc`: `DatabaseManager` recebe `utc_offset_hours` (nova chave `[APP] broker_utc_offset`,
   padrão `-3`) e converte `time - offset` antes de salvar.
4. Documente em `docs/exportacao_dados.md`.

**Critérios de aceite:** o valor padrão não muda nada nas bases existentes; os testes cobrem os dois modos.

**Implementado:** conversão centralizada em `DatabaseManager._to_storage` / `_from_storage`, aplicada ao gravar
(`save_ohlcv_data`, `save_data`), ao ler (`get_last_timestamp`, `get_rows_before`) e ao apagar (`delete_data_periodo`),
de modo que extrator, retomada e atualização continuam trabalhando no horário da corretora.

### T1.7 — Nomes de tabela sem colisão

**Objetivo:** `get_table_name_for_symbol` troca tudo que não é alfanumérico por `_`, então
`WIN$N` e `WIN_N` geram a mesma tabela.

**Passos:**
1. Crie a tabela `_symbol_tables(symbol TEXT, timeframe TEXT, table_name TEXT UNIQUE, PRIMARY KEY(symbol, timeframe))`.
2. `get_table_name_for_symbol`: (a) se o par já está mapeado, retorne o nome mapeado; (b) senão,
   calcule o nome atual; se esse nome já estiver mapeado para **outro** símbolo, acrescente
   `_` + os 6 primeiros caracteres de `hashlib.sha1(symbol.encode()).hexdigest()`; (c) grave o mapeamento.
3. Migração: na inicialização, para cada tabela existente sem mapeamento, não faça nada (o
   mapeamento é criado quando o símbolo for usado). **Não renomeie tabelas existentes.**

**Critérios de aceite:** `WIN$N` e `WIN_N` geram tabelas diferentes; bases antigas continuam lendo as mesmas tabelas.

**Implementado:** a chave do mapeamento usa o timeframe **normalizado** (`1 minuto` e `1_minuto` são o mesmo),
porque o coletor em tempo real (`data_collector.py`) usa `'1_minuto'` e a interface usa `'1 minuto'`; sem isso o
mesmo símbolo ganharia duas tabelas.

### T1.8 — Relatório de qualidade dos dados

**Objetivo:** depois de cada extração, informar lacunas e inconsistências.

**Arquivos:** novo `mt5_extracao/data_quality.py` (funções puras que recebem DataFrame), testes.

**Verificações:** (1) `high < low`, `open`/`close` fora de `[low, high]`; (2) `tick_volume == 0`;
(3) `time` duplicado; (4) lacunas maiores que 1 barra **dentro do pregão** (horário configurável
em `[APP] session_start=09:00`, `session_end=18:30`; ignore fins de semana); (5) total de barras
por dia.

**Saída:** `dict` com contagens + lista das primeiras 20 ocorrências de cada tipo; o extrator grava
um resumo no log e na `_extraction_log` (coluna nova `quality_json TEXT`).

**Critérios de aceite:** testes com DataFrames sintéticos cobrindo cada verificação.

**Implementado:** `mt5_extracao/data_quality.py`; o relatório de cada bloco fica em `_extraction_log.quality_json`
(coluna criada com `ALTER TABLE` em bases antigas) e é consultado com `mt5x quality --table <tabela> [--details]`.
Horário do pregão em `[APP] session_start` / `session_end`.

---

## Fase 2 — Núcleo desacoplado e Linux funcional

### T2.1 — `mt5_backend`: MT5 local ou via ponte

**Objetivo:** um único ponto que devolve um objeto com a **mesma API** do módulo `MetaTrader5`:
no Windows, o próprio módulo; no Linux, um adaptador que chama o MT5 do Wine pela ponte RPyC.

**Depende de:** T0.1. **Arquivos:** novo `mt5_extracao/mt5_backend.py`, `tests/test_mt5_backend.py`.

**Decisões já tomadas (não mude):**
- Datas são enviadas ao MT5 como **inteiros epoch (segundos)**, tratando `datetime` sem fuso como
  UTC (`calendar.timegm`). O MT5 aceita inteiros, o que evita diferenças de fuso entre máquinas.
- No servidor, o resultado é convertido para tipos puros do Python (listas, dicts, tuplas) antes de
  voltar, porque as structs do MT5 não podem ser desserializadas no Linux (o módulo não existe lá).
- A escolha local × ponte vem de `config.ini`: `[BRIDGE] enabled = true` → ponte; senão, import local.

**Código de referência (validado contra a ponte real com um módulo MetaTrader5 falso que devolve
arrays numpy estruturados e namedtuples):**

```python
"""Acesso ao MetaTrader 5: módulo local (Windows) ou ponte RPyC (Linux/Wine)."""
import calendar
import collections
import configparser
import datetime as _dt
import threading

import numpy as np

_REMOTE_HELPER = r'''
import MetaTrader5 as _mt5
def _mt5x_encode(v):
    if hasattr(v, "dtype") and getattr(v.dtype, "names", None):
        return {"__ndarray__": True, "descr": [(n, v.dtype[n].str) for n in v.dtype.names], "rows": v.tolist()}
    if hasattr(v, "_asdict"):
        return {"__struct__": type(v).__name__, "fields": {k: _mt5x_encode(x) for k, x in v._asdict().items()}}
    if isinstance(v, (tuple, list)):
        return [_mt5x_encode(x) for x in v] if any(hasattr(x, "_asdict") for x in v) else tuple(v)
    return v
def _mt5x_call(name, args, kwargs):
    return _mt5x_encode(getattr(_mt5, name)(*args, **kwargs))
'''


def _to_epoch(v):
    if isinstance(v, _dt.datetime):
        if v.tzinfo is not None:
            v = v.astimezone(_dt.timezone.utc).replace(tzinfo=None)
        return calendar.timegm(v.timetuple())
    return v


_struct_types = {}


def _decode(v):
    if isinstance(v, dict) and v.get("__ndarray__"):
        return np.array([tuple(r) for r in v["rows"]], dtype=[tuple(d) for d in v["descr"]])
    if isinstance(v, dict) and "__struct__" in v:
        fields = {k: _decode(x) for k, x in v["fields"].items()}
        key = (v["__struct__"], tuple(fields))
        if key not in _struct_types:
            _struct_types[key] = collections.namedtuple(v["__struct__"], list(fields))
        return _struct_types[key](**fields)
    if isinstance(v, list):
        return tuple(_decode(x) for x in v)
    return v


class RemoteMT5:
    """Mesma API do módulo MetaTrader5, executada no Python do Wine via RPyC."""

    def __init__(self, host="127.0.0.1", port=18812):
        import rpyc
        self._rpyc = rpyc
        self._lock = threading.Lock()  # uma conexão RPyC não deve ser usada por várias threads ao mesmo tempo
        self._conn = rpyc.classic.connect(host, port, keepalive=True)
        self._conn._config["sync_request_timeout"] = 300
        self._conn.execute(_REMOTE_HELPER)
        self._call = self._conn.namespace["_mt5x_call"]
        self._module = self._conn.modules["MetaTrader5"]
        self._consts = {}

    def __getattr__(self, name):
        if name.startswith("_") and name != "__version__":
            raise AttributeError(name)
        if name.isupper() or name == "__version__":
            if name not in self._consts:
                with self._lock:
                    self._consts[name] = self._rpyc.classic.obtain(getattr(self._module, name))
            return self._consts[name]

        def fn(*args, **kwargs):
            args = tuple(_to_epoch(a) for a in args)
            kwargs = {k: _to_epoch(v) for k, v in kwargs.items()}
            with self._lock:
                return _decode(self._rpyc.classic.obtain(self._call(name, args, kwargs)))
        fn.__name__ = name
        return fn


_instance = None


def get_mt5(config_path="config/config.ini"):
    """Retorna o módulo MetaTrader5 (Windows) ou RemoteMT5 (Linux). None se indisponível."""
    global _instance
    if _instance is not None:
        return _instance
    cfg = configparser.ConfigParser(interpolation=None)
    cfg.read(config_path, encoding="utf-8")
    if cfg.getboolean("BRIDGE", "enabled", fallback=False):
        _instance = RemoteMT5(cfg.get("BRIDGE", "host", fallback="127.0.0.1"),
                              cfg.getint("BRIDGE", "port", fallback=18812))
    else:
        try:
            import MetaTrader5 as mt5
            _instance = mt5
        except ImportError:
            return None
    return _instance
```

**Passos:**
1. Crie `mt5_extracao/mt5_backend.py` com o código acima.
2. Crie `tests/test_mt5_backend.py`: reaproveite o padrão de `test_bridge_server_and_probe`
   (em `tests/test_bootstrap_doctor.py`) para subir `scripts/mt5_bridge_server.py` com um
   `MetaTrader5.py` falso no `PYTHONPATH` que tenha: `TIMEFRAME_M1 = 1`; `copy_rates_range` que
   faz `assert isinstance(a, int)` e devolve um `np.array` estruturado com as colunas
   `time, open, high, low, close, tick_volume, spread, real_volume`; `symbol_info` que devolve um
   `collections.namedtuple`; `last_error` que devolve `(1, "Success")`.
3. Verifique: `pd.DataFrame(r)` tem as 8 colunas com os mesmos dtypes; `symbol_info("X").spread`
   funciona; `symbol_info("X")._asdict()` funciona; `copy_rates_from_pos` que devolve `None`
   retorna `None`; `datetime` é convertido para `int`.

**Critérios de aceite:** testes passam no CI (Linux e Windows). No Windows sem ponte,
`get_mt5()` devolve o módulo `MetaTrader5`.

**Armadilhas:** não passe `numpy.datetime64` nem `pd.Timestamp` para o MT5: converta para
`datetime` antes (`ts.to_pydatetime()`). O `_decode` para `symbols_get()` com milhares de
símbolos é lento (~1 s). É aceitável, mas não chame em laço.

### T2.2 — Usar o backend em todo o código

**Objetivo:** nenhum módulo do núcleo faz `import MetaTrader5` diretamente.

**Depende de:** T2.1. **Arquivos:** `mt5_connector.py`, `historical_extractor.py`, `error_handler.py`, `security.py`, `app.py`.

**Passos:**
1. `mt5_connector.py`: substitua o bloco

   ```python
   try:
       import MetaTrader5 as mt5
   except ImportError:
       ...
       mt5 = None
   ```

   por `from mt5_extracao.mt5_backend import get_mt5` e, no `__init__` do `MT5Connector`, após
   `self.config_path = config_path`, adicione `global mt5; mt5 = get_mt5(config_path)`. O restante
   do arquivo (153 usos de `mt5.`) continua funcionando sem alterações.
2. `historical_extractor.py`: troque `import MetaTrader5 as mt5` pelas constantes do módulo
   `mt5_extracao/timeframes.py` (crie-o já nesta tarefa, com os valores numéricos oficiais):
   `TIMEFRAME_M1 = 1`, `TIMEFRAME_M5 = 5`, `TIMEFRAME_M15 = 15`, `TIMEFRAME_M30 = 30`,
   `TIMEFRAME_H1 = 16385`, `TIMEFRAME_H4 = 16388`, `TIMEFRAME_D1 = 16408`,
   `TIMEFRAME_W1 = 32769`, `TIMEFRAME_MN1 = 49153`. Use `from . import timeframes as mt5`
   para não mexer nas linhas que usam `mt5.TIMEFRAME_*`. Para `last_error`, use `self.connector`.
3. `error_handler.py` (função `check_mt5_error`) e `security.py`: troque o `import MetaTrader5 as mt5`
   local por `from mt5_extracao.mt5_backend import get_mt5; mt5 = get_mt5()` e trate `None`.
4. `app.py`: remova `import MetaTrader5 as mt5` do bloco de imports do topo e, em
   `verificar_dependencias_criticas`, troque a verificação de `MetaTrader5` por `get_mt5() is not None`.
5. Teste em Windows (CI) e no Linux com a ponte (manual, T0.3).

**Critérios de aceite:** `grep -rn "import MetaTrader5" mt5_extracao app.py` só encontra
`mt5_backend.py`; `python -c "import mt5_extracao.historical_extractor"` funciona no Linux sem MT5;
o CI fica verde.

**Armadilhas:** constantes precisam existir no objeto remoto (`RemoteMT5.__getattr__` resolve nomes
em MAIÚSCULAS). Nunca use `hasattr(mt5, 'algo')` para decidir se o MT5 está disponível: no
`RemoteMT5` todo atributo "existe". Use `mt5 is not None`.

### T2.3 — Tirar Tkinter do núcleo

**Objetivo:** `mt5_connector.py` e `error_handler.py` importam `tkinter.messagebox` no topo, então
sem interface gráfica o núcleo não importa.

**Passos:**
1. `mt5_connector.py`: remova `from tkinter import messagebox` do topo. Nas duas perguntas ao
   usuário (procure `messagebox.askquestion`), troque por um callback opcional
   `self.ask_user: Callable[[str, str], bool] | None` recebido no `__init__` (padrão `None` → responde "não").
   Em `app.py`, passe `ask_user=lambda t, m: messagebox.askyesno(t, m)`.
2. `error_handler.py`: mova `from tkinter import messagebox` para dentro das funções que mostram
   diálogos, com `try/except ImportError` → apenas logar.

**Critérios de aceite:** `python -c "import mt5_extracao.mt5_connector, mt5_extracao.error_handler"`
funciona num Python **sem** tkinter.

### T2.4 — Gestão de processos multiplataforma

**Objetivo:** trocar `tasklist`/`taskkill`/`terminal64.exe` via `subprocess` por `psutil`,
com comportamento neutro no Linux.

**Passos:** em `mt5_connector.py`, nos métodos `_is_mt5_running`, `launch_mt5_as_admin`,
`_start_mt5_if_not_running`, `_fix_ipc_error` e `is_mt5_running_as_admin`:
- Encerrar processo: `for p in psutil.process_iter(['name']): if (p.info['name'] or '').lower() == 'terminal64.exe': p.terminate()` (e depois `psutil.wait_procs(procs, timeout=10)`).
- No Linux (`os.name != 'nt'`): `launch_mt5_as_admin` e `_start_mt5_if_not_running` retornam
  `False` com log "no Linux o terminal é iniciado pela ponte (mt5.initialize)". Não tente executar `.exe`.
- `is_admin`: no Linux retorne `os.geteuid() == 0`.

**Critérios de aceite:** nenhum `subprocess.run(["tasklist"...` / `["taskkill"...` restante.

### T2.5 — `initialize()` com `windows_path` no Linux

**Objetivo:** no Linux, `[MT5] path` é um caminho do Linux (para checar existência), mas o
`mt5.initialize(path=...)` roda **dentro do Wine** e precisa de `C:\...`.

**Passos:** em `MT5Connector._load_config`, leia também `self.mt5_windows_path = config.get('MT5', 'windows_path', fallback='') or self.mt5_path`.
Em `initialize`, nas estratégias que montam `params` com `path`, use `self.mt5_windows_path`
(e `ntpath.join`/`ntpath.dirname` em vez de `os.path` para esse valor). As verificações
`os.path.exists` continuam usando `self.mt5_path`.

**Critérios de aceite:** no Linux com a ponte, `MT5Connector().initialize()` retorna `True` (validação manual).

**Implementado:** com a ponte (`RemoteMT5`), `initialize()` não verifica nem inicia o processo
`terminal64.exe` no host: o próprio `mt5.initialize(path=C:\...)` inicia o terminal dentro do Wine.
A correção de IPC (que usa `taskkill`) é pulada nesse modo. Coberto por `tests/test_linux_bridge_e2e.py`
(ponte RPyC real + MetaTrader5 falso).

### T2.6 — Teste de paridade Windows × Linux

**Objetivo:** provar que a extração pela ponte é idêntica à local.

**Passos:** script `scripts/parity_check.py` que extrai `WIN$N` M1 de um dia fixo (ex.: o último
dia útil completo) e salva em Parquet (`df.to_parquet`, requer `pyarrow`, instale só no teste).
Rode no Windows e no Linux, na mesma conta/corretora, e compare com
`pd.testing.assert_frame_equal`. Documente o resultado em `docs/validacao_instaladores.md`.

---

## Fase 3 — CLI e automação

### T3.1 — CLI `mt5x`

**Objetivo:** operar sem interface gráfica (servidores, agendamento).

**Arquivos:** novo `mt5_extracao/cli.py`; entrada `mt5x = "mt5_extracao.cli:main"` (T3.3).

**Comandos (argparse, subcomandos):**

| Comando | Faz |
|---|---|
| `mt5x doctor` | delega para `mt5_extracao.bootstrap doctor` |
| `mt5x symbols [--group "*WIN*"]` | lista símbolos do MT5 |
| `mt5x extract --symbols WIN$N,WDO$N --tf M1 --from 2024-01-01 [--to 2024-06-30] [--indicators] [--overwrite] [--workers 4]` | extração histórica (bloqueante, com barra de progresso textual) |
| `mt5x update --symbols ... --tf M1` | atualização incremental (T1.3) |
| `mt5x export --table win_n_1_minuto --format csv\|excel\|parquet --out arquivo` | usa `DataExporter` |

**Passos:** a CLI instancia `DatabaseManager`, `MT5Connector`, `IndicatorCalculator` e
`HistoricalExtractor` do mesmo jeito que `app.py` (`load_config_and_db` e a criação do extrator),
**sem Tkinter**. `extract_data` é assíncrono: passe um `finished_callback` que aciona um
`threading.Event` e aguarde com `event.wait()`. Códigos de saída: 0 sucesso, 1 alguma falha, 2 uso incorreto.

**Critérios de aceite:** `mt5x --help` e cada subcomando `--help` funcionam; teste com o provedor falso (T4.3).

**Implementado:** `mt5_extracao/cli.py` (comandos acima + `tables`), `mt5_extracao/services.py` (montagem de
banco/fonte externa/blocos/extrator compartilhada com o `app.py`), `run.sh --cli` / `run.bat --cli`.

### T3.2 — Agendamento

**Objetivo:** manter a base atualizada automaticamente.

**Passos:** comando `mt5x schedule --every 15m --symbols ... --tf M1` que **imprime** (não
instala sozinho) o comando pronto para colar:
- Linux (cron): `*/15 9-18 * * 1-5 cd <projeto> && ./run.sh --cli update ... >> logs/cron.log 2>&1`
  (acrescente em `run.sh` o repasse `--cli` → `.venv/bin/python -m mt5_extracao.cli`).
- Windows: `schtasks /Create /SC MINUTE /MO 15 /TN "MT5 Extracao Update" /TR "\"<projeto>\run.bat\" --cli update ..."`.
Documente em `docs/instalacao.md`.

### T3.3 — `pyproject.toml`

**Objetivo:** substituir `setup.py` (hoje com dependências divergentes de `requirements.txt`).

**Passos:** crie `pyproject.toml` (setuptools) com `name = "mt5-extracao"`, `requires-python = ">=3.9"`,
`dependencies` = mesmas linhas de `requirements.txt` (com os mesmos marcadores `sys_platform`),
`[project.optional-dependencies] ta = ["pandas-ta"]`, `dev = ["pytest>=7.4", "pytest-timeout>=2.2"]`,
`[project.scripts] mt5x = "mt5_extracao.cli:main"`. Apague `setup.py`. Mantenha `requirements.txt`
(usado pelos instaladores) e adicione um teste que compara as duas listas de dependências.

### T3.4 — Timeframes como enum próprio

**Objetivo:** consolidar `mt5_extracao/timeframes.py` (criado em T2.2) como fonte única: `Enum`
com nome curto (`M1`, `H1`...), valor MT5, nome legível usado nas tabelas (`"1 minuto"`...) e
minutos por barra. Substitua os mapeamentos espalhados (`MT5Connector._convert_timeframe_to_mt5`,
`get_available_timeframes`). **Não mude os nomes legíveis existentes**, pois eles compõem os nomes das tabelas.

---

## Fase 4 — Qualidade e manutenção

### T4.1 — Remover arquivos mortos e unificar pontos de entrada

**Passos:**
1. Apague: `mt5_extracao/ui_manager.py.bak`, `fix_try.py`, `mt5_diagnostico.json` (contém caminho local de um usuário).
2. Transforme em *wrappers* finos, com aviso de descontinuação:
   - `install.py` → imprime "Use install.bat (Windows) ou ./install.sh (Linux)" e chama `python -m mt5_extracao.bootstrap configure`.
   - `verificador.py`, `check_mt5_config.py`, `mt5_troubleshooter.py` → chamam `python -m mt5_extracao.bootstrap doctor`.
   - `executar_mt5.py` → executa `app.py`.
3. Mova `test_mt5_connection.py`, `test_mt5_alternative.py`, `test_historical_extraction.py` e `mt5_workaround.py` para `scripts/manual/` (exigem MT5 real; não são testes automatizados).

**Critérios de aceite:** a raiz tem só: `app.py`, `install.bat`, `install.sh`, `run.bat`, `run.sh`, wrappers, docs e configs.

### T4.2 — Logging centralizado ✅

**Objetivo:** hoje cada módulo cria `logs/` e seus próprios handlers ao ser importado, o que
duplica linhas no console e cria pastas no diretório atual.

**Passos:** crie `mt5_extracao/logging_setup.py` com `setup_logging(log_dir, level)` (um
`RotatingFileHandler` de 5 MB × 5 arquivos e um `StreamHandler`). Remova de **todos** os módulos o
bloco `if not log.handlers: ...` e o `os.makedirs("logs")`; deixe só `log = logging.getLogger(__name__)`.
Chame `setup_logging` apenas em `app.py`, na CLI e nos scripts.

**Feito:** `mt5_extracao/logging_setup.py` (`setup_logging`/`teardown_logging`, arquivo único
`logs/mt5_extracao.log` na pasta do projeto). A CLI grava INFO no arquivo e mostra no console só
avisos/erros (tudo com `-v`). Exceções mantidas de propósito: `scripts/mt5_bridge_server.py` roda no
Python do Wine sem o pacote instalado (usa `basicConfig`), e os scripts de `scripts/manual/` são
diagnósticos avulsos. Os `basicConfig` dentro de `if __name__ == "__main__"` dos módulos também ficam.

### T4.3 — Provedor MT5 falso + testes ✅

**Objetivo:** testar extrator e banco sem MT5.

**Passos:** `tests/fakes.py` com `FakeMT5` (mesma API usada pelo código: `initialize`, `shutdown`,
`last_error`, `symbols_get`, `symbol_info`, `copy_rates_range`, `copy_rates_from_pos`, constantes
`TIMEFRAME_*`) gerando barras sintéticas determinísticas (M1 das 09:00 às 18:00 em dias úteis).
Um *fixture* do pytest faz `monkeypatch` de `mt5_extracao.mt5_backend._instance = FakeMT5()`.
Escreva testes para: extração de 3 meses em blocos; retomada (T1.2); atualização (T1.3); falha
simulada num bloco.

**Critérios de aceite:** cobertura ≥ 60% em `database_manager.py`, `historical_extractor.py` e
`mt5_backend.py` (`pytest --cov`, adicione `pytest-cov` ao `requirements-dev.txt`).

### T4.4 — Documentação ✅

Atualize `docs/arquitetura.md` (diagrama com `mt5_backend`, ponte e bootstrap) e o `README.md`
(estado real das funcionalidades). Remova do README as afirmações que não forem verdade.

### T4.5 — Remover módulos "enhanced" não usados ✅

`integrated_services.py`, `enhanced_calculation_service.py`, `enhanced_indicators.py`,
`performance_optimizer.py` e `market_data_analyzer.py` somam cerca de 3 mil linhas pouco ligadas ao fluxo.
**Passos:** com `grep`, liste quem usa cada um. Remova o que não é alcançável a partir de `app.py`/CLI,
ou integre o que for útil ao `IndicatorCalculator`. Faça **um PR por módulo**.

**Resultado** (um commit por módulo):

| Módulo | Decisão | Motivo |
|---|---|---|
| `integrated_services.py` | removido | `app.py` o criava e iniciava threads de cálculo cujos resultados nada consultava |
| `enhanced_calculation_service.py` | removido | só era usado pelo `IntegratedServices` |
| `performance_optimizer.py` | removido | só importado, nunca usado |
| `enhanced_indicators.py` (+ `advanced_indicators.py`) | **mantido** | usado pela coleta em tempo real (`DataCollector`) |
| `market_data_analyzer.py` | **mantido** | API pública documentada em `docs/indicadores_avancados.md`; sem dependências internas |

---

## Fase 5 — Escala e novos dados

| ID | Resumo do que fazer | Aceite |
|---|---|---|
| T5.1 ✅ | `DataExporter`: formatos `parquet` (via `pyarrow`, opcional) e `duckdb` (arquivo `.duckdb` com uma tabela por símbolo). Dependências em `requirements-optional.txt` | Exportar 1 milhão de linhas em < 10 s |
| T5.2 ✅ | `DatabaseManager` com `db_type = postgresql`: string de conexão em `[DATABASE] url` e senha no `.env` (`DB_PASSWORD`). Upsert com `sqlalchemy.dialects.postgresql.insert`. Se a extensão TimescaleDB existir, `create_hypertable` na criação da tabela. **Registrado:** colunas de 64 bits (`time_msc`, `tick_volume`, `real_volume`) passam a `BIGINT` em tabelas novas (no PostgreSQL `INTEGER` tem 32 bits; no SQLite não muda nada) | Serviço `postgres` no CI (job "Testes PostgreSQL", `tests/test_postgresql.py`) |
| T5.3 ✅ | Ticks: `copy_ticks_range(symbol, from, to, COPY_TICKS_ALL)` em blocos de 1 dia; tabela `<símbolo>_ticks` com PK `(time_msc, seq)` — **mudança registrada:** o MT5 devolve vários ticks no mesmo milissegundo, e uma PK só em `time_msc` descartaria ticks; `seq` é a ordem dentro do milissegundo. Blocos consecutivos compartilham a borda (a ponte envia segundos inteiros); a PK evita duplicatas. Mesma `_extraction_log` | Retomada funciona como em T1.2 |
| T5.4 ✅ | Book: `market_book_add` + `market_book_get` em laço (intervalo configurável); tabela `<símbolo>_book` (`time_utc`, `time_msc`, `type`, `price`, `volume`, PK `(time_msc, type, price)`). O MT5 não informa o horário do book: usa-se o relógio do computador em UTC (`book_collector.py`) | Coleta por 1 h sem crescer memória (teste com relógio simulado: no máximo `flush_every` snapshots em memória) |
| T5.5 ✅ | `CsvExternalSource(ExternalDataSource)` (aceita também a exportação de barras do MT5, separada por tabulação): lê CSVs de uma pasta (`[FALLBACK] csv_dir`) com colunas `time,open,high,low,close,real_volume`; tipo `Csv` em `[FALLBACK] external_source_m1_type` e a fábrica em `app.py` (onde hoje escolhe `DummyExternalSource`) | Fallback M1 preenche lacuna a partir do CSV |
| T5.6 ✅ | Pastas de dados por usuário (opcional via `[APP] data_dir`: vazio, `auto` ou caminho). **Migração:** se `database/mt5_data.db` existir no projeto, continue usando-o. **Mudanças registradas:** `services.user_data_dir()` usa só a biblioteca padrão (mesmas pastas do `platformdirs`), evitando uma dependência; e caminhos relativos de config/banco passam a valer a partir da pasta do projeto, não do diretório atual (antes, `mt5x` rodado de outra pasta criava um banco vazio ali) | Instalações antigas continuam funcionando sem ação do usuário |

---

## Fase 6 — Estratégias, backtest e triagem de ativos

Objetivo: transformar a base em **ferramenta de pesquisa**. Responder, com dados e
sem promessas, "qual estratégia faz sentido em qual ativo e timeframe agora".
**Não envia ordens** (fora do escopo, ver ROADMAP). Tudo em `mt5_extracao/strategies/`,
sem MT5 e sem Tkinter (usa só o banco); testes com séries sintéticas de `tests/market_data.py`.

Pipeline: **Filtro A** (ativo apto, semanal) → **Filtro B** (oportunidade agora, diário)
→ **Filtro C** (estratégia × ativo × timeframe validada em backtest). Só o que passa
nos três aparece como "✅ apto" no relatório.

### T6.1 — Indicadores puros ✅

`strategies/indicators.py`: `sma, ema, rma, true_range, atr, rsi, adx (plus_di/minus_di/adx),
bollinger, keltner, donchian, efficiency_ratio, linreg_slope, linreg_r2, rolling_percentile,
swing_low, swing_high`. Médias de Wilder = `ewm(alpha=1/n, adjust=False)`.
**Aceite:** `tests/test_indicators.py` (valores conhecidos + teste de "sem olhar o futuro").

### T6.2 — Seis estratégias com interface comum ✅

Arquivos: `strategies/base.py` (classes `Strategy`, `StrategyInfo`, `ExitRules`, `SIGNAL_COLUMNS`),
um arquivo por estratégia, `strategies/__init__.py` com o registro `REGISTRY` e `get_strategy(chave, **params)`.

Contrato de `Strategy.gerar_sinais(df) -> DataFrame` (mesmo índice de `df`):

| Coluna | Tipo | Significado |
|---|---|---|
| `entry_long`, `entry_short` | bool | Sinal no **fechamento** da barra; o backtester entra na **abertura da próxima** |
| `exit_long`, `exit_short` | bool | Saída por sinal (executada na abertura da próxima barra) |
| `stop_long`, `stop_short` | float | Preço do stop se a entrada ocorrer a partir deste sinal |
| `target_long`, `target_short` | float/NaN | Alvo fixo em preço (NaN = usar `ExitRules.target_r`) |
| `atr` | float | ATR(14), usado no trailing e na triagem |

Regras de saída comuns ficam em `ExitRules` (`target_r`, `partial_r`, `partial_fraction`,
`breakeven_after_partial`, `trailing_atr`, `max_bars`, `eod_exit`).

| Chave | Estratégia | Regras |
|---|---|---|
| `ema_atr` | Tendência EMA + ATR | Compra no cruzamento EMA20 > EMA50 com close > EMA200; stop 2×ATR; trailing 3×ATR; sai no cruzamento contrário |
| `donchian` | Donchian / Turtle | Compra no rompimento da máxima de 20; sai no rompimento da mínima de 10; filtro ADX > 20 **ou** ATR > média(ATR, 50); stop 2×ATR |
| `bb_rsi` | Reversão BB/RSI | ADX < 20, mínima toca a banda inferior (20, 2) e RSI(2) < 10; sai na banda central; stop 1,5×ATR; máx. 10 barras |
| `orb` | Opening Range Breakout | Faixa = máx./mín. dos primeiros 30 min do dia; compra no fechamento acima da faixa; stop no lado oposto; alvo 1,5× a faixa; uma entrada por dia; **saída obrigatória no fim do dia** |
| `pullback` | Pullback em momentum | close > EMA200, ADX > 25, RSI(14) estava < 40 e vira para cima; stop abaixo da mínima de 10 barras; alvo 2R; parcial de 50% em 1R e stop no zero a zero |
| `squeeze` | Squeeze BB/Keltner | Bollinger dentro do Keltner em alguma das últimas 5 barras; rompe a banda de Bollinger a favor do momentum (close − SMA20); stop 1,5×ATR; trailing 2,5×ATR; sai quando o momentum inverte |

Vendas são simétricas. **Armadilha:** todo sinal usa apenas dados até a barra atual
(`shift(1)` nos canais de rompimento); há teste que altera o futuro e compara os sinais.

### T6.3 — Backtester ✅

`strategies/backtester.py`: `SymbolSpec` (point, tick_size, tick_value, volume_min/step/max,
spread padrão), `BacktestConfig` (capital, risco por operação = 1%, slippage em pontos,
comissão por lote por lado), `run_backtest(df, strategy, spec, config, start_index=0) -> BacktestResult`.

Regras (documente qualquer mudança):
1. Barras do MT5 são **bid**. Compra entra em `open + spread×point + slippage`; venda sai no bid.
   Spread por barra vem da coluna `spread` (pontos); se não houver, `spec.spread_points`.
2. Stop e alvo avaliados na barra; se ambos forem tocados na mesma barra, **conta o stop** (pessimista).
   Gap além do stop executa na abertura.
3. Lote = `capital × risco / (distância_do_stop / tick_size × tick_value)`, arredondado para baixo
   no `volume_step`; se ficar abaixo de `volume_min`, a operação é **pulada** e contada em `skipped_min_lot`.
4. Saídas: stop, alvo, parcial, trailing (atualizado no fechamento), tempo (`max_bars`),
   fim do dia (`eod_exit`, fecha no fechamento da última barra do dia), sinal, fim dos dados.
5. Métricas: nº de operações, taxa de acerto, profit factor, payoff, expectativa em R, lucro líquido,
   retorno %, drawdown máximo % (curva de capital fechada).

### T6.4 — Validação (Filtro C) ✅

`strategies/validation.py`:
- `walk_forward`: divide em `n_windows + 2` partes; otimiza (grade `param_grid`, objetivo = soma de R)
  em 2 partes e testa na seguinte; indicadores aquecem com os dados anteriores (`start_index`).
- `robustness`: cada parâmetro numérico × 0,8 e × 1,2; robusto se **todas** as variantes têm PF > 1.
- `monte_carlo`: 1000 reamostragens (semente fixa) dos retornos por operação → drawdown máximo no percentil 95.
- `validate(...)` → veredito com itens: PF fora da amostra > 1,3; ≥ 100 operações fora da amostra;
  > 50% das janelas positivas; robustez; DD p95 ≤ limite (padrão 20%). Grava em `_strategy_validation`
  (tabela nova, aditiva).

### T6.5 — Especificações do símbolo e leitura do banco ✅

Tabela `_symbol_specs` (símbolo, point, digits, tick_size, tick_value, volume_min/step/max,
contract_size, currency_profit, spread, updated_at) preenchida por `mt5x specs` a partir de
`symbol_info`. Sem especificação, usa-se um padrão genérico e o relatório avisa
"especificação ausente: rode mt5x specs". `DatabaseManager.load_ohlcv(símbolo, timeframe, início, fim)`.

### T6.6 — Triagem de ativos ✅

`strategies/asset_screener.py`. Configuração em `[SCREENER]` e `[STRATEGY]` (chaves com `fallback=`).

Filtro A (eliminatório; motivo em português para cada reprovação):

| Critério | Corte padrão | Chave |
|---|---|---|
| Spread / ATR(14) (medianas das últimas 100 barras) | < 10% | `max_spread_atr` |
| Liquidez (mediana do volume) | ≥ percentil 30 dos ativos analisados | `min_liquidity_percentile` |
| Barras com problema (OHLC inválido, lacunas no pregão, gaps > 5×ATR) | < 1% | `max_bad_bars` |
| Histórico | ≥ 3 anos (H4 e maiores) ou ≥ 1 ano (H1 e menores) | `min_years_daily`, `min_years_intraday` |
| Picos de spread (> 3× a média) | ≤ 2% das barras | `spike_multiple`, `max_spike_share` |
| Lote para risco de 1% com stop de 2×ATR | ≥ lote mínimo | `[STRATEGY] risk_per_trade` |

Filtro B (score 0–100): regime (ADX > 25 tendência, < 20 lateral), força (inclinação EMA50/ATR, R² de 50 barras),
volatilidade (percentil do ATR em 100 barras), custo (spread/ATR), eficiência (Kaufman),
evento (CSV `[SCREENER] events_file` com `time,symbol,impact`: evento de alto impacto nas próximas
`event_window_hours` horas bloqueia), correlação com a carteira (`[SCREENER] portfolio`, penaliza > 0,7).
Saída: `Ativo | Válido | Regime | Estratégia sugerida | Score | Observação`.

### T6.7 — CLI, relatório e GUI ✅

`mt5x strategies` (tabela-resumo + "evite no início"), `mt5x specs`, `mt5x backtest`, `mt5x validate`,
`mt5x screen`, `mt5x report` (HTML autocontido em `exports/`, matriz ativo × estratégia com
métricas e veredito). GUI: menu "Estratégias" que gera o relatório e abre no navegador.
A orquestração fica em `strategies/research.py` (sem Tkinter) e é usada pela CLI e pela GUI.
**Armadilha:** a thread de trabalho da GUI nunca chama o Tkinter; envia atualizações por uma
`queue.Queue` que a thread principal esvazia com `after()` (chamar `root.after` de outra thread
gera `RuntimeError: main thread is not in main loop`).

### T6.8 — Documentação ✅

`docs/estrategias.md` para o usuário final: o que cada filtro faz, como ler o relatório,
tabela das estratégias, lista "evite no início" e o aviso de que backtest não garante resultado futuro.

---

## Fase 7 — Interface para estudo e análise

### T7.1 — Janela principal em abas, tema e análise ✅

**Diagnóstico que motivou a tarefa** (interface anterior):

- Tudo em uma tela só, com o tema padrão do Tk, fonte fixa "Segoe UI" (não existe no Linux) e cores fixas.
- Não havia visão do que existe na base (ativo x timeframe, período, atualização).
- "Mostrar Estatísticas" lia só M1, só as últimas 1000 barras, consultava `sqlite_master` (quebrava no
  PostgreSQL) e fazia tudo na thread da interface, com gráfico de linha simples.
- `export_data` alterava o estilo global `TFrame`/`TButton`/`TLabel`, desalinhando a janela depois de aberto.
- A detecção da data mais antiga mexia em widgets a partir da thread de trabalho.
- `update_symbols_with_data` acrescentava " ✓" ao texto do símbolo, que passava a ser tratado como outro ativo.
- Ajuda > Documentação apontava para `docs/manual.html`, que não existe; a seleção não era lembrada.

**O que existe e não deve ser quebrado:**

| Arquivo | Papel |
|---|---|
| `mt5_extracao/study.py` | Núcleo (sem Tkinter): resumo do período, indicadores do gráfico, sazonalidade, agrupamento de candles, glossário |
| `DatabaseManager.data_inventory()` | Uma linha por série de barras (símbolo, timeframe, tabela, barras, início, fim); só leitura |
| `mt5_extracao/ui/theme.py` | Paleta clara/escura, estilos ttk, `Tooltip`, `card`, `scrolled_tree` |
| `mt5_extracao/ui/inventory.py` | Aba "Minha base" |
| `mt5_extracao/ui/analysis.py` | Aba "Análise" (candles + volume + IFR, sazonalidade, retornos, qualidade) |
| `mt5_extracao/ui_manager.py` | Janela principal, aba "Ativos", aba "Atividade" e os diálogos |

**Regras para mexer na interface:**

- Tkinter só em `ui_manager.py` e em `mt5_extracao/ui/`; cálculo fica em `study.py` (testável sem tela).
- Leitura do banco e do MT5 sempre por `UIManager.run_bg(trabalho, pronto)`; a thread de trabalho nunca toca em widgets.
- Nada de cor ou fonte fixa: use `ui.theme.colors` e os estilos (`Card.TFrame`, `Surface.TLabel`, `Muted.TLabel`,
  `Accent.TButton`...). `Listbox`/`Text` passam por `theme.classic()` para acompanhar o tema escuro.
- Nunca chame `ttk.Style().configure("TFrame"|"TButton"|"TLabel", ...)` dentro de um diálogo: muda o aplicativo todo.
- Preferências (tema, seleção) ficam em `config/ui_prefs.json` (não versionado); favoritos e benchmark, na tabela
  `_asset_prefs` do banco desde a T7.2 (`config/favorites.json` é a reserva sem banco).

**Verificação:** `pytest tests/test_study.py tests/test_ui_main_window.py` (no Linux sem tela:
`xvfb-run -a .venv/bin/python -m pytest tests/test_ui_main_window.py`).

**Pendências conhecidas (não fazem parte desta tarefa):** datas da extração ainda são digitadas como texto;
exportação de várias tabelas e reconexão ao MT5 ainda rodam na thread da interface; ticks e book seguem só na CLI.

### T7.2 — Tabelas de ativos, marcação em lote e bateria de backtests pela interface ✅

Pedido do dono do projeto (2026-10-13): modernizar as tabelas das abas "Ativos" e "Minha base" e rodar backtests de
vários ativos e vários robôs pela interface, em segundo plano e sem sobrecarregar o computador.

**O que foi pedido**

1. As duas tabelas com as colunas, da esquerda para a direita: Favorito (★/☆, clique alterna e grava no banco),
   Ativo, Volume, Robô Lucrativo (`🟢 +12,4%`, `🔴 Sem Lucro`, `⚪ N/A`), Status da Base (`🟢 Atualizada`,
   `🔴 Precisa de atualização`), Benchmark (`🎯 Sim`, `❌ Não`), Detalhes na Base (`M1 (6.489 barras)`) e Seleção
   (caixa por linha).
2. Ordem padrão: favoritos no topo, depois os de maior volume, depois ordem alfabética. Clique no cabeçalho ordena
   por aquela coluna, com os favoritos sempre no topo: Robô Lucrativo traz os com lucro, Status da Base os que
   precisam de atualização, Volume do maior para o menor (e o inverso no segundo clique), Benchmark os que têm.
3. Barra de ações: "Selecionar Todos" (linhas visíveis), "Deselecionar Todos" e o contador de selecionados.
4. Em "Minha base", o botão "Iniciar Backtest (N selecionados)", desabilitado enquanto nada está marcado.
5. Diálogo com todos os robôs e estratégias do catálogo, uma caixa cada, "Selecionar todos os robôs", "Limpar
   seleção" e "Executar N Backtest(s)" (séries × robôs).
6. Execução em segundo plano (`concurrent.futures`), com monitor de CPU e memória (`psutil`) que regula quantos
   testes rodam juntos; andamento na barra de status (`Executando Backtest 4/12: PETR4 + Robô (CPU: 45% | RAM: 60%)`)
   e a coluna Robô Lucrativo atualizada assim que cada teste termina.

**Registrado na entrega**

| Arquivo | Papel |
|---|---|
| `mt5_extracao/asset_table.py` (novo) | Núcleo, sem Tkinter: tabela `_asset_prefs` (favorito e benchmark), volume, resultado dos robôs por ativo, situação da base, montagem e ordenação das linhas com `pandas` |
| `mt5_extracao/robots/runner.py` (novo) | `BatchRunner` (fila em `ThreadPoolExecutor`, eventos de andamento, cancelamento) e `ResourceGovernor` (CPU e memória pelo `psutil`) |
| `mt5_extracao/ui/backtest_dialog.py` (novo) | Diálogo de escolha dos robôs |
| `mt5_extracao/ui/inventory.py` | Aba "Minha base" com as colunas novas, marcação, benchmark e a bateria |
| `mt5_extracao/ui_manager.py` | Aba "Ativos" com as colunas novas, clique na estrela e na caixa, ordenação e barra de ações |
| `mt5_extracao/ui/theme.py` | `Theme.glyphs` (texto simples quando o Tk não desenha os símbolos) e barra horizontal em `scrolled_tree` |
| `config_builder.py`, `config/config.ini.example` | `[ROBOTS] batch_max_cpu_pct` (75), `batch_max_ram_pct` (85), `batch_max_workers` (0 = núcleos menos um) |

**Decisões tomadas onde o pedido deixava em aberto (ou batia com o que já existe)**

- **"Minha base" continua com uma linha por série (ativo × timeframe).** As ações que já existiam (Analisar,
  Exportar) e o backtest precisam do timeframe. A caixa marca a série; o contador mostra os ativos e, quando
  difere, as séries. Em "Ativos" a linha é o ativo, e a caixa é a seleção que já existia (a lista da direita).
- **Volume** é a média diária dos últimos 30 dias de dados **da base** (volume real quando a corretora informa,
  senão o de ticks). Ativo sem dados na base fica com "—" e, na ordem padrão, vem em ordem alfabética depois dos
  que têm volume. Não se consulta o MT5 para o volume de todos os ativos da conta (são milhares de consultas).
- **Robô Lucrativo** segue a regra da matriz (T11.6): vale o backtest **mais recente** de cada robô em cada ativo e
  timeframe, não o melhor. Mostra o maior retorno entre os que deram lucro líquido. **Lucro em um teste não é
  aprovação** (não exige as 30 operações nem a bateria): o aviso está na dica da aba; o veredito segue no painel de
  resultados. Em "Minha base" o valor é o daquela série; em "Ativos", o do ativo em qualquer timeframe.
- **Status da Base** usa a data da última barra gravada (`study.freshness`, a mesma regra de "em dia" da T7.1), sem
  consultar o MT5. Em "Ativos", o ativo só está "Atualizada" se todas as suas séries estiverem.
- **Benchmark:** não existia no banco. Entrou como coluna `benchmark` da `_asset_prefs`, preenchida pelo botão
  "Benchmark..." de "Minha base" (texto livre; em branco desvincula). Nenhum cálculo usa o benchmark ainda.
- **Favoritos** saíram de `config/favorites.json` para a tabela `_asset_prefs`. Na primeira abertura os do arquivo
  são levados para o banco (o arquivo fica onde está e segue como reserva quando não há banco).
- **Números com vírgula** (`+12,4%`), como no resto do aplicativo, e não com ponto como no exemplo do pedido.
- **Sem CustomTkinter:** o pedido cita "CustomTkinter / Tkinter"; o projeto usa `ttk` com tema próprio e assim
  ficou. O `Treeview` não tem caixa de seleção nem cor por célula: estrela e caixa são texto clicável, e a estrela
  não fica amarela.
- **Threads, não processos:** os backtests gravam no mesmo banco e usam o catálogo carregado; um teste de
  estratégia Python leva centésimos de segundo (T11.5). Robôs MQL5 rodam **um por vez** (o MT5 não abre duas vezes
  a mesma instalação) e precisam do terminal fechado: a interface pergunta antes de fechá-lo e, com "Não", testa só
  as estratégias Python.
- **Regulagem:** começa com um teste; a cada leitura (no mínimo 0,25 s entre elas) sobe um se a CPU estiver 15
  pontos abaixo do teto e desce um se a CPU ou a memória passarem do teto. Nunca menos que um. O teto vale para a
  decisão de **começar** mais um teste: um teste já em andamento não é interrompido.
- **Tk sem Xft:** o Python deste host (distribuição do `uv`) traz um Tk que só enxerga as fontes antigas do X11 e
  não desenha emojis, estrela nem caixa. `Theme.glyphs` detecta isso (`tk::pkgconfig get fontsystem` = `x11`) e
  troca por texto: `*`/`·`, `[x]`/`[ ]` e os rótulos sem o emoji ("Sem Lucro", "Atualizada", "Sim").

**Conferido no host (Linux):** 14 testes do núcleo (`tests/test_asset_table.py`, `tests/test_robots_runner.py`, um
deles com estratégias reais, SQLite e o `psutil`) e 5 testes novos da janela (`tests/test_ui_main_window.py`): clique
na estrela e na caixa pelas coordenadas da célula, migração dos favoritos, botão que acompanha a marcação, diálogo,
bateria em segundo plano, benchmark e o modo de texto simples. Telas vistas em tela virtual (1300×780), **no modo de texto simples**.

**Não conferido:**

- Os símbolos (★, ☑, 🟢, 🔴, 🎯) desenhados na tela: o Tk do host não os desenha. Fica para um Tk com Xft ou o Windows.
- Robô MQL5 pela bateria da interface com o MT5 real (fechar o terminal, testar, religar a ponte): só com dublê.
- A regulagem com o computador de fato carregado: testada com leituras simuladas.
- Conta com milhares de ativos: o tempo de redesenho da tabela a cada clique não foi medido.
- Windows: checklist em [validacao_instaladores.md](validacao_instaladores.md).

**Verificação:** `pytest tests/test_asset_table.py tests/test_robots_runner.py tests/test_ui_main_window.py`.

---

## Fase 8 — Gestão de estratégias e robôs

Objetivo: um lugar para **importar, avaliar, testar, aprovar e colocar para operar** robôs. O catálogo
junta dois tipos: robôs MQL5 (`.mq5`/`.ex5` + presets `.set`, como o HADOUKEN v7 do repositório
`projeto-indicadores-mt5`) e as estratégias Python da Fase 6. Guia do usuário: [robos.md](robos.md).

**Decisões registradas (2026-10-12):**

| Tema | Decisão |
|---|---|
| O que é "robô" | MQL5 e Python no mesmo catálogo; cada card diz o tipo e o que está disponível |
| Tempo real | O aplicativo inicia o robô no MT5, em demo e em conta real (real com as travas abaixo) |
| HADOUKEN | Nenhum código MQL5 é copiado para este repositório: o app lê robôs das pastas de `[ROBOTS] library_dirs` |
| Backtest de MQL5 | Strategy Tester do MT5 disparado pelo app; Monte Carlo e checklist rodam aqui |

**Princípio de segurança (vale para todas as tarefas da fase):** o Python **nunca** envia ordem. Quem opera é
o robô MQL5 dentro do MT5, pela camada `Safety/`. O app instala, configura, inicia, monitora (somente leitura)
e para (kill switch). `tests/test_no_order_send.py` (criado na T8.1) falha se `order_send` aparecer em `mt5_extracao/`.

| Destino | Exigido para iniciar |
|---|---|
| Demo | Compilado sem erros + bateria estática aprovada + bateria estatística aprovada |
| Real | Tudo acima + `[ROBOTS] allow_real = true` + `min_demo_days` (padrão 20) dias em demo com resultado dentro do esperado + risco do preset ≤ `max_real_risk_pct` (padrão 1%) + confirmação digitando o número da conta |

Demo ou real vem de `MT5Connector.account_summary()["demo"]`.

**Fatos que condicionam a fase:**

- O HADOUKEN v7 ainda não foi compilado no MetaEditor (PR #1 do outro repositório, rascunho). Até compilar, o
  card fica em "não compilado" e nada além disso é liberado.
- O v7 é um painel de operação, sem entrada automática. Estratégia automática precisa chamar `TryEnter()` (T8.9).
- O MT5 não abre uma segunda instância da mesma instalação. Rodar o Strategy Tester ou iniciar um robô pelo app
  exige fechar o terminal (e a ponte) ou usar uma segunda cópia em modo `/portable`. **Não provado no Wine**: a
  T8.4 começa provando isso à mão; se falhar, a T8.5 passa a "importar o relatório gerado no MT5".

**Arquitetura:**

```
mt5_extracao/robots/            núcleo, sem Tkinter (entra em CORE_MODULES de tests/test_core_decoupling.py)
  catalog.py     Robot (dataclass); fontes: strategies.REGISTRY + pastas MQL5; estágio do ciclo de vida
  mql.py         lê #property, input (grupo, tipo, padrão, comentário), .set, #include, #import
  risk.py        classificação de risco com motivos (Baixo / Moderado / Alto / Não avaliado)
  store.py       tabelas novas (aditivas): _robots, _robot_runs, _robot_checks
  backtest.py    Python: strategies.run_backtest · MQL5: lê relatório + CSV de trades
  battery.py     bateria de segurança -> veredito por item, com motivo e o que fazer
  mt5_tools.py   pasta de dados, instalar arquivos, compilar, ini do tester/startup, kill switch
  live.py        travas de demo/real, início, monitoramento somente leitura
mt5_extracao/ui/robots.py        aba "Robôs": grade de cards, filtros, importação
mt5_extracao/ui/robot_detail.py  painel do robô (abre ao clicar no card)
```

**Reaproveitar, não reescrever:** `StrategyInfo` (`strategies/base.py`); `run_backtest`, `compute_metrics`, `Trade`
(`strategies/backtester.py`); `validate`, `walk_forward`, `robustness`, `monte_carlo` (`strategies/validation.py`);
`load_data`, `backtest_config`, `criteria` (`strategies/research.py`); `load_spec` (`strategies/specs.py`);
`Theme`, `card`, `Tooltip`, `scrolled_tree`, `fmt_num` (`ui/theme.py`); `UIManager.run_bg`/`call_in_ui`;
`MT5Installation`, `installation_from_path` (`bootstrap/mt5_locator.py`); `linux_to_wine_path`,
`wine_to_linux_path` (`bootstrap/paths.py`). Do HADOUKEN, só contratos de arquivo: `HDK_trades_<magic>.csv`
(separador `;`), `HDK_KILL.txt` em `Common\Files` e `Presets/*.set`.

**Configuração nova** (em `DEFAULTS` e em `config/config.ini.example`, na tarefa que usar cada chave):
`[ROBOTS] library_dirs`, `metaeditor_path`, `tester_model`, `allow_real = false`, `min_demo_days = 20`,
`max_real_risk_pct = 1.0`.

### T8.0 — Plano e mudança de escopo ✅

Este texto, a linha F8 do ROADMAP, a reescrita de "Fora do escopo" e `docs/robos.md`.

### T8.1 — Catálogo de robôs ✅

**Registrado na entrega:** conferido com o HADOUKEN real (commit `b3a4bef` do PR #1): versão 7.00, 6 grupos de
parâmetros, 3 presets com risco Baixo/Moderado/Alto. Quando o robô usa um perfil nomeado (`InpProfile`), os valores
de risco ficam dentro do código MQL5; sem um `robot.json`, a classe vem do **nome** do perfil (conserv/moder/agress).
`store.py` cria só `_robots`; `_robot_runs` (T8.3) e `_robot_checks` (T8.6) ficam para quem as usa.
Biblioteca de exemplo dos testes: `tests/robots_library/`.

- **Arquivos:** `mt5_extracao/robots/{__init__,catalog,mql,risk,store}.py`, `cli.py` (`mt5x robots list|import`),
  `bootstrap/config_builder.py`, `config/config.ini.example`, testes.
- **Passos:** (1) `mql.py`: ler `.mq5` em UTF-8 (com ou sem BOM) e UTF-16, extrair `#property version/description`,
  `input`/`sinput` com grupo, tipo, padrão e comentário, `#include`, `#import`; ler `.set` (`chave=valor`, `;` comenta).
  (2) `catalog.py`: `Robot` com chave estável, tipo (`mql5`/`python`), nome, versão, regime, timeframes, arquivos,
  inputs, presets, estágio; robôs Python vêm de `REGISTRY`; MQL5 de `library_dirs` (pasta `Experts/`) ou de um
  arquivo importado; `robot.json` ao lado do EA, se existir, tem prioridade sobre a heurística.
  (3) `risk.py`: classe e motivos a partir de inputs e preset (stop obrigatório, risco por trade, limite diário,
  drawdown, preço médio/martingale/grid); só `.ex5` = "Não avaliado". (4) `store.py`: tabelas aditivas e leitura/gravação.
  (5) CLI. (6) `test_no_order_send.py`.
- **Critérios de aceite:** a pasta do HADOUKEN gera um card com versão 7.00, 6 grupos de inputs e 3 presets; os três
  presets recebem classes de risco coerentes com o perfil; bases antigas abrem sem erro; `robots/*` sem Tkinter.
- **Armadilhas:** presets guardam enums como número (`InpProfile=1`); arquivos legados em UTF-16.

### T8.2 — Aba "Robôs" ✅

**Registrado na entrega:** o painel do robô fica ao lado da grade (mesma aba), não numa janela à parte. Os cards das
estratégias Python já mostram a melhor validação gravada por `mt5x validate` (`catalog.attach_validations`) e passam
a "Testado"; a mini-curva de capital depende de `_robot_runs` e fica para a T8.3. Pastas adicionadas pela interface
são gravadas em `_robots` (tipo `library`), para não reescrever o `config.ini` do usuário. O caminho da estratégia
Python tem 3 estágios (Importado → Testado → Bateria): ela não compila nem opera. Capturas conferidas nos dois temas
com o HADOUKEN real em display virtual.

- **Arquivos:** `ui/robots.py`, `ui/robot_detail.py`, `ui_manager.py` (aba e menu Exibir), `tests/test_ui_main_window.py`.
- **Card:** nome, versão e selo do tipo; regime e timeframes; classe de risco + motivo curto; estatísticas do último
  teste (profit factor, acerto, expectativa, drawdown máximo, nº de trades) e mini-curva de capital; trilha do estágio
  (Importado → Compilado → Testado → Bateria → Demo → Real). Estado sempre em texto + ícone, nunca só cor.
- **Barra superior:** Importar robô ▾ (arquivo, pasta, reler a biblioteca), busca e filtros (tipo, regime, risco, estágio).
- **Painel (clique no card):** abas Visão geral, Backtest, Bateria de segurança e Tempo real; nesta tarefa só a
  primeira tem conteúdo, as outras dizem o que falta e em qual tarefa entram.
- **Regras:** as da Fase 7 (tema, `run_bg`, nada de cor ou fonte fixa).

### T8.3 — Backtest pelo card (Python) ✅

**Registrado na entrega:** `_robot_runs` guarda métricas e a curva de capital já reduzida (até 240 pontos, mantendo
os extremos de cada trecho, para não esconder o pior drawdown). No card, a validação tem prioridade sobre o backtest
(`Robot.headline`): backtest isolado não aprova nem reprova, por isso aparece como "Último backtest", sem veredito.
A tabela de operações do backtest não entrou no painel (o `mt5x backtest --trades-out` já exporta); fica como melhoria.

`robots/backtest.py` chama `research.load_data` + `run_backtest`, grava em `_robot_runs` (métricas e curva de capital)
e a aba Backtest mostra curva, métricas e trades. O card passa a mostrar as estatísticas e o estágio "Testado".

### T8.4 — Ferramentas MT5 ✅

**Prova manual (2026-10-12, Wine 11.17, MT5 em modo portátil no prefixo do instalador):**

| Prova | Resultado |
|---|---|
| Compilar pelo MetaEditor | ✅ gera o `.ex5` e o log UTF-16; erros e avisos vêm como `arquivo(linha,coluna) : error N: texto` e a última linha é `Result: N errors, M warnings` |
| Caminho com espaço | ⚠️ só funciona com as aspas depois dos dois-pontos (`/compile:"C:\..."`). O `wine` não repassa o argumento assim, então a linha de comando vai num `.bat` rodado por `wine cmd /c` |
| Código de saída | ⚠️ invertido: 1 quando compila, 0 quando falha (ou quando não entende o caminho). O resultado vem do log |
| Abrir o terminal com `/config:` | ✅ `terminal64.exe /portable /config:"C:\x.ini"` com `[Tester]` e `ShutdownTerminal=1` rodou o Strategy Tester (EURUSD H1, 10 dias), gravou o relatório `.htm` na pasta de dados e fechou sozinho em 12 s |

Consequências: a T8.5 pode disparar o Strategy Tester pelo app (não precisa cair para "importar o relatório"),
sempre com `/portable` no Wine (sem ele o terminal abre uma pasta de dados vazia, sem conta) e com o terminal
e a ponte fechados. A segunda instância em paralelo não foi testada.

**Registrado na entrega:** `mt5x robots install <robô>` copia e `mt5x robots compile <robô>` copia e compila; a
compilação aprovada grava o estágio "Compilado" em `_robots` (a biblioteca do usuário não recebe o `.ex5`).
Presets que já existem no MT5 com outro conteúdo são mantidos, salvo `--overwrite-presets`. Sem o MT5 conectado,
a pasta de dados vem de `origin.txt` (pasta do usuário) ou da própria instalação. Chave nova:
`[ROBOTS] metaeditor_path`. Testes em `tests/test_robots_mt5_tools.py` com um MetaEditor falso; a compilação
real das duas amostras de `tests/robots_library` foi conferida à mão. Não validado no Windows.

`robots/mt5_tools.py`: localizar a pasta de dados (`terminal_info().data_path`) e o MetaEditor; copiar o EA, seus
includes e presets para `MQL5/`; compilar (`MetaEditor64.exe /compile:<arquivo> /log`, pelo Wine no Linux) e ler o log
(UTF-16) em erros e avisos. Os executáveis entram por função injetada, para testar sem MT5. **Primeiro passo:**
prova manual, no Wine do usuário, de compilar e de abrir o terminal com `/config:`; registre o resultado aqui.

### T8.5 — Backtest MQL5 ✅

**Registrado na entrega:**

- `mt5x robots backtest <robô MQL5> --symbol X --tf H1 [--from] [--to] [--preset P] [--param nome=valor]` gera o
  `.ini` e o `.set` (UTF-16), roda `terminal64.exe /config:` e grava a execução em `_robot_runs`
  (`source = "strategy tester"`). O robô passa a "Testado". Sem `--from`/`--to`: os últimos 365 dias.
- **Diferença em relação ao texto abaixo:** o app **não** fecha o MT5 por conta própria (pode haver robô operando).
  Com o terminal aberto o comando para e explica; `--close-terminal` para a ponte, fecha o terminal, testa e
  religa a ponte se ela estava ativa.
- As operações vêm da tabela de negócios do relatório `.htm`, lida pela forma das linhas (13 colunas, tipo e
  direção em inglês), porque os títulos mudam com o idioma do terminal. As métricas são calculadas aqui, com
  resultado = lucro + swap + comissão. No teste real (Moving Average, EURUSD H1, 510 operações) bateram com as do
  MT5: resultado −860,47, profit factor 0,80, acerto 21,76%, rebaixamento do saldo 10,62%.
- Contrato do HADOUKEN, conferido no fonte do v7 (`ExportTradesCsv`): `Common\Files\HDK_trades_<magic>.csv`, ANSI,
  separador `;`, colunas `time;symbol;type;volume;result`, gravado no `OnTester` só com `InpExportTrades = true`.
  Se o robô tem esse parâmetro, o teste o liga; o CSV antigo é apagado antes e, se o novo existir, as operações
  vêm dele. Validado de ponta a ponta com um EA de sonda que grava o mesmo CSV (185 operações, soma igual à do
  relatório). **Repetido com o HADOUKEN v7 (2026-10-12):** compilou sem erros nem avisos (MT5 build 6258 no Wine) e
  rodou no Strategy Tester (EURUSD H1, 2025-10-03 a 2026-09-30, ticks reais, preset `HDK_Conservador`): 0 operações,
  porque o v7 é um painel sem entrada automática. O teste levou cerca de uma hora (o robô usa temporizador de
  500 ms). A leitura do CSV do `OnTester` com operações de verdade ficou conferida pelos robôs da T8.9.
- Sem risco inicial por operação não há expectativa em R: `expectancy_r` fica vazio e `expectancy` traz o resultado
  médio em dinheiro. `bars` fica 0 (o relatório só traz esse número em campo traduzido).
- Depósito do teste: `[STRATEGY] initial_equity`. Chave nova: `[ROBOTS] tester_model` (padrão 1, OHLC de 1 minuto).
- O relatório completo e os gráficos ficam na pasta de dados do MT5 (`mt5x_<robô>.htm`), trocados a cada teste.
- O painel do robô mostra os comandos e o resumo do último teste; o botão de rodar pelo card não entrou.
- Não validado no Windows nem com o terminal aberto de verdade (`--close-terminal` só com processos simulados).
- **Defeito achado em 2026-10-12, com o MT5 aberto de verdade, e corrigido em PR próprio:** no Wine 11.17 o
  psutil mostra o terminal com o nome de processo `main` e o executável `wine-preloader`; só o primeiro item da
  linha de comando traz `terminal64.exe`. `terminal_running()` devolvia falso, o backtest tentava abrir uma
  segunda instância e terminava em "não gerou o relatório", em vez de parar e explicar. `close_terminal()` e
  `MT5Connector._kill_terminal_processes` tinham o mesmo defeito (não achariam o processo). A detecção agora é
  `paths.is_terminal_process(nome, linha de comando)`. Conferido: com o MT5 aberto, `terminal_running()` devolve
  verdadeiro. **Ainda não conferido:** fechar um terminal real com `--close-terminal`.

Gerar o `.ini` do tester (`[Tester] Expert, ExpertParameters, Symbol, Period, Model, FromDate, ToDate, Deposit,
Report, ReplaceReport=1, ShutdownTerminal=1`), parar a ponte e o terminal, rodar, religar; ler o relatório e
`HDK_trades_<magic>.csv`. Plano B (se a T8.4 mostrar que não funciona no Wine): importar relatório e CSV gerados à mão.

### T8.6 — Bateria de segurança ✅

**Registrado na entrega:**

- `robots/battery.py` e `mt5x robots battery <robô> [--preset P]`: cada item com ✓ / ⚠ / ✗, o motivo e o que fazer;
  código de saída 1 se reprovar. O resultado fica em `_robot_checks` (tabela nova) e aparece na aba Bateria do painel.
- **Regra de aprovação:** nenhum ✗ (avisos não reprovam). Vale a bateria **mais recente**: aprovada, o robô passa a
  "Bateria"; uma reprovação posterior revoga (o estágio da bateria não é gravado em `_robots`).
- **Robô só com `.ex5`:** estática "não avaliável". Interpretei a frase da tabela abaixo como: passa para demo se a
  estatística aprovar (`approved`), mas `allows_real` fica falso para sempre.
- A estatística roda sobre as operações do backtest mais recente do robô. Para isso `_robot_runs` ganhou a coluna
  `trades_json` (aditiva, criada por `ALTER TABLE` em bases antigas); execuções anteriores pedem um novo backtest.
- **Diferenças em relação à tabela abaixo:**
  - *Fora da amostra:* estratégia Python usa a validação walk-forward gravada (`mt5x validate`). Robô MQL5 não tem
    otimização aqui, então o item é "o resultado se mantém no fim do período" (profit factor > 1 nos 30% finais).
  - *Robustez ±20%:* Python usa a validação; em MQL5 fica ⚠ "não avaliado" (exigiria repetir o Strategy Tester para
    cada parâmetro). Fica como melhoria.
  - *Estresse de custo:* um spread a mais por operação (spread × valor do ponto × volume, de `_symbol_specs`); sem a
    especificação do ativo, ⚠ "não avaliado".
  - *Ordens fora da `Safety/`:* robô que envia ordem direto e não tem camada de segurança é reprovado; robô que não
    envia ordem nenhuma (painel) recebe aviso. A expressão de envio de ordem passou para `mql.ORDER_CALL`, usada
    também pela auditoria.
- Limiares fixos no código: 200 operações, ruína ≤ 1% das simulações, aviso a partir de 10 perdas seguidas. Do
  `config.ini`: `[STRATEGY] min_profit_factor` e `max_drawdown`, e a chave nova `[ROBOTS] max_real_risk_pct`.
- Não rodei a bateria com o HADOUKEN v7 (ele ainda não compila).

| Grupo | Itens | Fonte |
|---|---|---|
| Estática | compila sem erros; encoding; stop obrigatório; sem martingale/grid/preço médio; toda ordem passa pela `Safety/` (sem `OrderSend`/`CTrade` solto); sem `#import` de DLL; magic exclusivo; risco por trade, limite diário e drawdown do preset dentro dos tetos | `mql.py` |
| Estatística | ≥ 200 trades; PF > 1,3; expectativa > 0 com custos; drawdown Monte Carlo p95/p99 ≤ limite; chance de ruína; fora da amostra; robustez ±20%; estresse de custo (spread ×2); maior sequência de perdas | `validation.py` + trades |
| Operacional (T8.8) | dias em demo; desvio demo × backtest; kill switch testado; limite diário respeitado | `live.py` |

Cada item devolve ✓ / ⚠ / ✗ com o motivo e o que fazer. Robô só com `.ex5`: estática "não avaliável", não libera conta real.

### T8.7 — Tempo real em demo ✅

**Estado: implementada e testada apenas com terminal, conta e MT5 simulados** (decisão do dono do projeto em
2026-10-12). Nenhum robô foi iniciado em conta alguma. Antes de marcar como concluída, falta provar com o MT5 real,
em conta demo:

1. que `terminal64.exe /portable /config:` com `[StartUp] Expert=...` coloca o robô no gráfico e que
   `[Experts] Enabled=1`, `AllowLiveTrading=1`, `AllowDllImport=0` são as chaves certas (vieram da documentação);
2. que `[Common] Login` e `Server` mantêm a conta conferida (sem senha no `.ini`, usando a salva no terminal);
3. que `start "" ...` dentro do `.bat` deixa o terminal aberto no Wine depois que o `wine cmd /c` retorna;
4. que o HADOUKEN para ao encontrar `Common\Files\HDK_KILL.txt` (conferido só no fonte: `CheckKillSwitch`);
5. que a ponte religada enxerga o terminal reiniciado, para o `monitor`.

**Registrado na entrega:**

- `robots/live.py` e `mt5x robots start <robô> --symbol X --tf M5 [--preset P] --close-terminal`,
  `mt5x robots monitor <robô>` e `mt5x robots stop [--release] [--close-terminal]`.
- **Travas para demo** (todas mostradas, com o que fazer): robô MQL5; `.ex5` na pasta do MT5; bateria mais recente
  aprovada **e do mesmo preset** que será usado; conta conectada e de demonstração; PARAR TUDO desligado.
- **Conta real é sempre recusada** aqui, com o número e o servidor da conta na mensagem. O `.ini` fixa o login e o
  servidor conferidos, para o robô não subir em outra conta.
- Para saber se a conta é demo o terminal precisa estar aberto; para o robô entrar no gráfico ele precisa ser
  reiniciado pelo app. Por isso o `start` exige `--close-terminal` como consentimento: sem a opção ele mostra as
  travas e não mexe em nada.
- `monitor` lê posições e negócios pelo magic number (somente leitura): abertas, sem stop, resultado aberto e
  fechado desde o início; avisa se a conta conectada não é a do início.
- `stop` grava `HDK_KILL.txt` e **não fecha posições** (o Python não envia ordens). Robô sem a camada HADOUKEN não
  obedece ao arquivo: o comando diz isso e oferece `--close-terminal`. `stop --release` remove o arquivo.
- O estado do início fica em `_robots.meta_json` (`live`); o estágio "Demo" volta ao anterior depois do `stop`.
- Só CLI: a aba "Tempo real" do painel mostra os comandos.

`robots/live.py`: travas da tabela acima; início pelo `.ini` de inicialização (`[StartUp] Expert=...`); posições e
resultado por magic com `positions_get`/`history_deals_get` (somente leitura); **PARAR TUDO** grava `HDK_KILL.txt`
em `Common\Files`. Recusa com mensagem clara se a conta conectada não for demo.

**Tentativa de validação no host (2026-10-12), com autorização do dono do projeto:** a conta do terminal é de
demonstração (MetaQuotes-Demo). **Nenhum robô foi iniciado**, porque nenhum passa na trava "bateria aprovada":
o `ExemploCruzamento` do SDK (EURUSD H1, 119 operações, profit factor 0,87), o HADOUKEN v7 (painel, 0 operações) e
os três robôs da T8.9 levados à bateria foram reprovados. A trava não foi contornada. Os cinco itens acima
continuam sem prova; a T8.7 só anda quando houver um robô aprovado. A `mt5x-safety` também obedece ao
`HDK_KILL.txt` (`CHDKRiskGuard`), então um robô do SDK aprovado serve para provar o item 4.

**Validação da mecânica em demo (2026-10-12/13, host Linux, MT5 build 6258 no Wine, conta MetaQuotes-Demo), pedida
pelo dono do projeto.** Como nenhum robô tem bateria aprovada, o teste foi feito por um script à parte, rodado pelo
dono, que dispensa **só** a trava "bateria aprovada" e mantém as outras (robô compilado, conta demo, PARAR TUDO
desligado). O `mt5x robots start` não mudou e continua recusando. Robô: `HDK_EmaAtr` (T8.9), EURUSD H1, preset
conservador (0,25% por operação).

| Item | Resultado | Evidência |
|---|---|---|
| 1. `[StartUp] Expert=...` põe o robô no gráfico; chaves de `[Experts]` | ✅ | log do terminal: `successfully initialized from start config`, `5 inputs read from expert ... set file ...HDK_EmaAtr_Conservador.set`, `expert HDK_EmaAtr (EURUSD,H1) loaded successfully` |
| 2. `[Common] Login` e `Server` mantêm a conta | ✅ | `authorized on MetaQuotes-Demo`; pela ponte, a mesma conta, de demonstração |
| 3. O terminal fica aberto depois que o comando volta | ✅ | processo vivo minutos depois; o comando voltou em menos de 1 s |
| 4. O robô para com `Common\Files\HDK_KILL.txt` | ✅ | `mt5x robots stop` às 23:59:56; log do robô às 23:59:57: `BLOQUEADO: kill switch acionado` |
| 5. A ponte religada enxerga o terminal reiniciado | ✅ | `mt5x robots monitor hdk_emaatr`: 0 posições, 0 operações |

**Defeito achado e corrigido nesta validação:** na primeira tentativa o terminal abriu sem o robô, com
`cannot load config "...mt5x_startup.ini""` no log. No Wine o programa abria o terminal com `start`, que reescreve
as aspas de `/config:"C:\...ini"`. Agora `spawn_windows_command` grava a linha original num `.bat` de nome fixo e
não espera por ele. Os testes com terminal simulado não pegavam isso.

**O que não ficou provado e o que foi visto de novo:**

- **Nenhuma operação aconteceu** (não houve sinal no intervalo). Ordem de verdade, aviso de operação do robô
  (T11.4) e `monitor` com posição aberta continuam sem prova com o MT5 real.
- O `start` grava o robô como iniciado sem conferir se o terminal carregou o arquivo de início. Na tentativa que
  falhou o programa disse "iniciado" e o robô não estava no gráfico. **Falta** ler o log do terminal depois de abrir.
- Depois do `stop` o robô continua no gráfico, bloqueado; para tirá-lo é preciso removê-lo à mão. **Não há comando
  para isso.**
- **Correção (2026-10-13):** eu tinha escrito aqui que o robô ficava no perfil e voltava quando o terminal
  reabrisse. Não é o que acontece quando o programa reinicia o terminal: ao reabri-lo com outro arquivo de início
  (para carregar o `HDK_AccountGuardian`), só o robô desse arquivo foi carregado e o `HDK_EmaAtr` **sumiu do
  gráfico** (o perfil salvo tinha o gráfico EURUSD H1 sem robô). Consequências: o arquivo de início carrega **um**
  robô por vez, e cada `start` do programa derruba o robô anterior sem avisar. Causa provável, não confirmada: o
  programa encerra o terminal à força e o perfil não é salvo com o robô. **Falta** tratar isso (fechar o terminal
  de forma limpa, ou avisar e recusar quando já houver um robô iniciado) antes de operar com mais de um robô.
- Só no Linux. O caminho do Windows (`Popen` direto, sem `.bat`) não mudou e não foi validado.
- A trava da bateria não foi exercitada com um robô aprovado, porque não existe um.

### T8.8 — Tempo real em conta real

Bateria operacional, `allow_real`, `min_demo_days`, `max_real_risk_pct` e confirmação digitando o número da conta.
É a tarefa de maior risco e a última da fase.

### T8.9 — Robôs de atuação

No repositório HADOUKEN: módulo de sinais para cada uma das seis estratégias chamando `TryEnter()`, e um `robot.json`
por robô. Aqui: comparar os trades do Strategy Tester com os do backtester Python no mesmo período.

**Registrado (2026-10-12), parcial:** feito no repositório HADOUKEN (`projeto-indicadores-mt5`, branch
`tarefa/T8.9-estrategias`, PR #2 em rascunho): motor comum (`Strategies/HDK_StrategyRobot.mqh`) com as regras do
backtester Python, indicadores com as mesmas fórmulas (`HDK_StrategyMath.mqh`) e seis robôs em
`Experts/HADOUKEN/Estrategias/` (`HDK_Donchian`, `HDK_EmaAtr`, `HDK_BbRsi`, `HDK_Orb`, `HDK_Pullback`,
`HDK_Squeeze`), cada um com `robot.json` (`parity_with`) e preset. `ClosePosition` da camada de segurança ganhou um
volume opcional, para a saída parcial; a `mt5x-safety` ainda não tem essa mudança.

Os seis compilam sem erros. Paridade no host (EURUSD, "cada tick", cerca de 13 meses):

| Robô | Operações Python × MQL5 | Casadas | Pares a até 5% | Diferença sobre o giro | Mediana MQL5/Python |
|---|---|---|---|---|---|
| `HDK_EmaAtr` (H1) | 70 × 70 | 97,1% | 86,8% | 1,7% | 0,997 |
| `HDK_Donchian` (H4) | 47 × 46 | 97,9% | 78,3% | 5,7% | 0,982 |
| `HDK_BbRsi` (H1) | 188 × 185 | 96,3% | 74,0% | 2,6% | 0,978 |
| `HDK_Squeeze` (H1) | 173 × 172 | 99,4% | 79,1% | 2,2% | 0,992 |
| `HDK_Pullback` (H1) | 76 × 73 | 88,2% | 92,5% | 2,1% | 0,987 |
| `HDK_Orb` (M15) | 257 × 239 | 91,8% | 36,0% | 1,2% | 0,941 |

**Nenhum cumpre o critério** (que não foi afrouxado): as operações coincidem, o valor não, e o MQL5 rende menos
em todos. A causa conhecida é a T10.12; a parte que não vem dela não foi medida. Bateria com ticks reais
(2024-10-01 a 2026-09-30, preset Conservador): `HDK_Donchian` 81 operações e profit factor 1,19; `HDK_BbRsi` 128 e
0,74; `HDK_Squeeze` 72 e 0,51. **Todos reprovados.** Detalhes e lacunas em
`Documentation/Guides/ROBOS_DE_ESTRATEGIA.md` daquele repositório. **Falta:** achar a causa das operações sem par
no Pullback e no ORB, medir o efeito do custo (T10.12) e testar em ativos da B3.

**Paridade repetida (2026-10-13), depois da `Safety/` 7.1 e da correção do `HDK_RiskGuard`:** o lado MQL5 dos seis
robôs deu **exatamente os mesmos números** da tabela acima (mesmas operações e mesmo resultado em todos), ou seja,
as mudanças na camada de segurança não alteraram o comportamento dos robôs. O lado Python mudou porque passou a
cobrar swap (T10.12), e a distância diminuiu um pouco em cinco dos seis:

| Robô | Casadas | Pares a até 5% | Diferença sobre o giro | Mediana MQL5/Python |
|---|---|---|---|---|
| `HDK_EmaAtr` (H1) | 97,1% | 92,6% | 1,3% | 0,997 |
| `HDK_Donchian` (H4) | 97,9% | 89,1% | 4,9% | 0,984 |
| `HDK_BbRsi` (H1) | 96,3% | 78,5% | 2,4% | 0,979 |
| `HDK_Squeeze` (H1) | 99,4% | 86,6% | 1,8% | 0,995 |
| `HDK_Pullback` (H1) | 88,2% | 94,0% | 2,0% | 0,987 |
| `HDK_Orb` (M15) | 91,8% | 36,0% | 1,2% | 0,941 |

Continuam todos fora do critério. O `ExemploCruzamento` do SDK, recompilado com a `mt5x-safety` 1.1.0, continua
dentro (97,9% casadas, 96,5% dos pares, 0,3% do giro, mediana 1,001).

**Correção feita aqui por causa desta tarefa:** `parity.run` (estratégia portada à mão) não igualava os dois lados
como `run_declared` já fazia. O robô rodava com o risco padrão dele e com os limites de perda ligados, e os dois
lados não começavam juntos; na primeira medição a mediana MQL5/Python era 0,501. Agora ele usa `parity_params` e o
início comum depois do aquecimento (`min_bars` da estratégia).

**Verificação da fase:** `PYTHONPATH=. xvfb-run -a .venv/bin/python -m pytest -q` e o `doctor` antes e depois de cada
tarefa; manual, com MT5 real em demo: importar a pasta do HADOUKEN, compilar, rodar backtest e bateria, iniciar em demo
com o preset Conservador e acionar PARAR TUDO.

---

## Fase 9 — Operação por agente e segurança

### T9.1 — CLI para agentes ✅

`--json` devolve um único objeto `{ok, command, exit_code, data, messages, errors}` por comando. `mt5x status`
resume base, robôs, validações e próximas ações (`mt5_extracao/agent.py`); `mt5x analyze`, `mt5x robots backtest`
e `mt5x commands` completam a operação sem tela.

### T9.2 — Auditoria (`mt5x audit`) ✅

`mt5_extracao/audit.py`: achados de projeto (P), patrimônio (A) e capital (C), cada um com gravidade, evidência e
como corrigir. Código de saída 1 quando há achado de gravidade alta ou crítica.

### T9.3 — Ponte restrita ✅

Fecha o achado A1 da auditoria. A ponte usava o modo aberto do RPyC (`SlaveService`), em que qualquer programa do
computador podia executar código no Python do Wine e, por ele, enviar ordens pela conta logada.

**Registrado na entrega:**

- `scripts/mt5_bridge_server.py` expõe só `info`, `const` e `call`; `call` aceita apenas os nomes de `ALLOWED`
  (leitura de dados e de estado, mais `initialize`/`login`/`shutdown`). Nenhuma função `order_*` está na lista, e
  `--allow` (só para testes) recusa nomes que comecem com `order`.
- Argumentos e resultados em JSON; `allow_pickle`, `allow_public_attrs` e `allow_all_attrs` desligados.
- Protocolo `mt5x-bridge/2`, conferido pelo cliente (`mt5_backend.BRIDGE_PROTOCOL`). Com uma ponte antiga ainda em
  execução, `RemoteMT5` e o `doctor` falham com a ação de correção: `./scripts/mt5-bridge.sh restart`.
- Aviso no log quando o servidor escuta fora de `127.0.0.1`.
- Testes em `tests/test_mt5_backend.py` (recusas, ausência de execução de código, ponte antiga) e a ponte de ponta
  a ponta em `tests/test_linux_bridge_e2e.py`.

**Conferido no host (2026-10-12):** `scripts/parity_check.py extract --symbol EURUSD --date 2026-10-09` pela ponte
restrita (`mt5x-bridge/2`) trouxe as mesmas 1.440 barras M1 da extração anterior. A comparação com o lado Windows
continua na T2.6.

---

## Fase 10 — SDK para desenvolvimento de robôs

Objetivo: além de **gerenciar** robôs prontos (Fase 8), o projeto passa a servir a quem **escreve** o robô. O motor
de qualidade já existe (instalar, compilar, Strategy Tester, bateria, tempo real em demo); a Fase 10 o empacota em
um contrato escrito, um ponto de partida, um ciclo de desenvolvimento repetível e uma API estável. O aplicativo
amigável não muda: o SDK é uma camada a mais sobre o mesmo código.

**Decisões registradas (2026-10-12):**

| Tema | Decisão |
|---|---|
| Camada de segurança MQL5 | Sai do HADOUKEN e vira **biblioteca própria, em repositório próprio, com versão**. O SDK instala uma versão fixada. O HADOUKEN e os robôs novos passam a depender dela |
| Código MQL5 neste repositório | Continua valendo "nenhum código de robô é copiado para cá". Exceção única: os **modelos** de `mt5x robots new` (esqueleto, não estratégia), que são código deste projeto |
| Quem envia ordens | Só o robô MQL5, pela biblioteca de segurança. O Python nunca envia ordens (`tests/test_no_order_send.py` continua valendo) |
| Tradução Python → MQL5 | Função **opcional** (T10.10): gera MQL5 a partir de uma estratégia **declarada** (indicadores e condições suportados), que roda igual nos dois lados. Código Python livre continua sendo portado à mão; em qualquer caso o SDK confere a **paridade** (T10.6) |
| MQL5 Market | Função **opcional** (T10.11): confere as exigências e monta o pacote. O envio é feito pela pessoa no MQL5.com |
| Windows | Pendência (T10.W). O desenvolvimento segue no host atual; veja a regra abaixo |

### Regra da pendência do Windows (vale para toda a fase)

O host de desenvolvimento atual é Linux (Manjaro) com o MT5 no Wine. Não há máquina Windows disponível. Para não
parar o projeto:

1. **Desenvolva e verifique no host atual.** Uma tarefa está concluída quando passa em "Verificação" no Linux.
   Não espere o Windows.
2. **O código continua multiplataforma** (regra do AGENTS.md): `pathlib`, nada de caminho fixo, e todo ponto em que
   o comportamento muda por sistema passa por `paths.is_windows()`. Os testes exercitam os **dois** caminhos com
   `monkeypatch` de `paths.is_windows`, como em `tests/test_robots_mt5_tools.py`.
3. **Registre o que não foi provado.** Cada tarefa acrescenta ao checklist "Pendências de validação no Windows",
   em [validacao_instaladores.md](validacao_instaladores.md), uma linha por comportamento que só o Windows real
   confirma (comando exato e resultado esperado). No texto da tarefa e no PR escreva "Não validado no Windows".
4. **Não escreva "funciona no Windows" sem ter rodado lá.** Nem no README, nem em mensagem ao usuário.
5. **O que a pendência bloqueia:** só a T10.9 (publicação) e o marco M7. Nada mais.
6. **Quando houver uma máquina Windows:** execute a T10.W de uma vez, linha por linha, corrija o que falhar em PRs
   próprios e marque cada linha com a data.

### T10.0 — Plano da Fase 10 ✅

Este texto, a linha F10 e os marcos M6/M7 do ROADMAP, a regra do Windows no AGENTS.md e o checklist inicial em
`validacao_instaladores.md` (com o que as T8.4–T8.7 deixaram sem validar).

### T10.1 — Contrato do robô v1 ✅

**Registrado na entrega:** `docs/sdk/contrato.md`, `robots/contract.py`, `robots/robot.schema.json` e
`mt5x robots validate`. `Robot.manifest` guarda o `robot.json` lido. Com contrato válido, a bateria lê risco, limites,
stop e magic pelos nomes declarados e deixa de reprovar só pela classe "Alto" heurística (os números decidem); sem
contrato, tudo como antes, mais o aviso "o robô não declara contrato". O backtest usa os papéis `magic` e
`export_trades` quando declarados. Campos `tunable`, `safety_lib` e `parity_with` já são validados, para as tarefas
T10.5, T10.2 e T10.6 usarem. Um teste garante que o esquema e o validador listam os mesmos campos. Nada desta tarefa
depende do sistema operacional: sem linha nova no checklist do Windows.

- **Objetivo:** tirar o contrato do robô das expressões regulares e pô-lo em uma especificação que a bateria confere.
- **Depende de:** T8.6.
- **Arquivos:** `docs/sdk/contrato.md` (novo), `mt5_extracao/robots/robot.schema.json` (novo),
  `mt5_extracao/robots/contract.py` (novo), `robots/battery.py`, `robots/catalog.py`, `cli.py`,
  `tests/test_robots_contract.py` (novo).
- **Passos:**
  1. Escreva `docs/sdk/contrato.md` com a versão `contract = 1` e as seções: `robot.json` (campos, tipos, quais são
     obrigatórios), parâmetros obrigatórios do robô (magic number, risco por operação, limite de perda diário,
     drawdown máximo, stop, exportação de trades), arquivos trocados (`HDK_trades_<magic>.csv`, `HDK_KILL.txt`,
     com colunas e codificação, já conferidos no fonte na T8.5/T8.7) e o que o robô **não** pode fazer (DLL, ordem
     fora da biblioteca de segurança, martingale/grid/preço médio).
  2. `robot.schema.json`: esquema do `robot.json` com `"contract": 1`. Campos de hoje (`name`, `description`,
     `regime`, `timeframes`, `markets`, `risk`) mais `inputs`: o **nome do parâmetro** de cada papel
     (`magic`, `risk_pct`, `daily_loss_pct`, `max_drawdown_pct`, `stop`, `export_trades`).
  3. `contract.py`: `validate(robot) -> List[Problem]` sem dependência nova (validação escrita à mão; não adicione
     `jsonschema` ao `requirements.txt`). Confere o `robot.json` contra o esquema e se cada parâmetro declarado em
     `inputs` existe no fonte.
  4. `battery.static_items`: quando o robô declara `"contract": 1`, use os nomes de `inputs` em vez de
     `risk.find_inputs`. Sem contrato, tudo continua como hoje (robôs antigos não podem quebrar) e a bateria
     acrescenta um item ⚠ "robô sem contrato declarado".
  5. `mt5x robots validate <robô>`: lista os problemas; código de saída 1 se houver algum.
- **Critérios de aceite:** os dois robôs de `tests/robots_library` continuam com o mesmo resultado na bateria (mais
  o aviso novo); um robô de teste com `robot.json` de contrato 1 troca os itens "não foi possível conferir" por
  valores lidos; `robot.json` inválido aponta campo e motivo em português.
- **Verificação:** `pytest -q`, `doctor`.
- **Armadilhas:** não mude o significado de campos que `catalog._manifest` já lê. Não exija contrato para robô `.ex5`.

### T10.2 — Biblioteca de segurança MQL5 versionada ✅

**Registrado na entrega (parte deste repositório):** `robots/safety_lib.py`, chave `[ROBOTS] safety_lib_dir` e a opção
`--upgrade-safety`. `mt5x robots install`/`compile` instalam a biblioteca em `MQL5/Include/<include_dir>` na versão
que o robô declara em `"safety_lib"` e gravam o marcador `MQL5/Include/mt5x-safety.json`. Versão do clone diferente
da pedida, ou outra versão já instalada no MT5, dão erro com a ação de correção; nada é trocado em silêncio. A
bateria ganhou o item "biblioteca de segurança na versão declarada" e passa a aceitar como "ordens pela camada de
segurança" o robô que inclui a biblioteca e não envia ordem por fora. **Conferido no host:** um robô de sonda que
declara a 1.0.0 e inclui `HDK_RiskGuard.mqh` e `HDK_SafeExecutor.mqh` foi instalado e compilado do zero pela CLI, sem
erros. **Não feito:** download automático da biblioteca; o HADOUKEN ainda usa a própria cópia da `Safety/`.

- **Objetivo:** o desenvolvedor recebe a camada de segurança como dependência com versão, e não copiando uma pasta
  de outro projeto.
- **Depende de:** T8.4.
- **Feito em 2026-10-12, a pedido do dono do projeto:** repositório privado
  [`Misael-art/mt5x-safety`](https://github.com/Misael-art/mt5x-safety), versão `v1.0.0`, clone local em
  `~/Projects/mt5x-safety`. Contém os oito arquivos de `Include/Framework/HADOUKEN/Safety/` do
  `projeto-indicadores-mt5` (commit `b3a4bef`), sem alteração, em `Include/MT5X/Safety/`, mais `VERSION`,
  `safety.json` e `Experts/MT5X/SafetySmoke.mq5`. **Conferido:** o teste de compilação passa no MetaEditor do
  host sem erros nem avisos. **Não conferido:** nenhum robô operou com a biblioteca; o HADOUKEN ainda não foi
  alterado para depender dela (continua com a própria cópia). O passo 1 abaixo está cumprido.
- **Arquivos (neste repositório):** `mt5_extracao/robots/safety_lib.py` (novo), `cli.py`, `config_builder.py`,
  `config/config.ini.example`, `robots/battery.py`, `tests/test_robots_safety_lib.py` (novo).
- **Passos:**
  1. No repositório da biblioteca (fora daqui): os `.mqh` da `Safety/`, um `VERSION`, um `safety.json`
     (`{"version": "...", "contract": 1, "files": [...]}`) e um EA mínimo de teste que **compila** no MetaEditor.
     Confira a compilação com `mt5x robots compile` (T8.4) no host atual.
  2. Aqui: chave `[ROBOTS] safety_lib_dir` (pasta local da biblioteca; vazio = não instalada). Sem download
     automático nesta tarefa.
  3. `safety_lib.py`: `installed_version(data_dir)`, `install(lib_dir, data_dir)` (copia para
     `MQL5/Include/`, idempotente, grava `safety.json` ao lado) e `required_version(robot)` (do `robot.json`).
  4. `mt5x robots install`/`compile` instalam a biblioteca antes, se o robô a declara. Versão diferente da pedida:
     erro claro com a ação de correção; nunca troque em silêncio.
  5. Bateria: item novo "biblioteca de segurança na versão declarada".
- **Critérios de aceite:** com a pasta da biblioteca configurada, um robô que a inclui instala e compila do zero em
  uma pasta de dados limpa; a bateria reprova quando a versão instalada não é a declarada.
- **Verificação:** `pytest -q` (biblioteca falsa em `tmp_path`) e, manual no host, compilar o EA mínimo real.
- **Armadilhas:** não versione os `.mqh` aqui. Se o HADOUKEN v7 ainda não compilar, extraia só a `Safety/`: é ela que
  precisa compilar, não o painel.

### T10.3 — `mt5x robots new` ✅

**Validação real (2026-10-12, host Linux, MT5 build 6258 no Wine, conta MetaQuotes-Demo):** robô criado com
`mt5x robots new`, com um sinal de cruzamento de médias (10 × 40) escrito nos blocos marcados, rodado por
`mt5x robots check` em EURUSD M15, de 2025-10-01 a 2026-09-30. Resultado, lido do relatório do Strategy Tester:
99 entradas, **todas com stop** (coluna S/L preenchida nas 99), lote calculado pelo risco (de 1,9 a 11,67 lotes
para 0,5% de 100.000), saídas por stop e por alvo e uma saída pela própria biblioteca (`HDK close`, ao bater um
limite). As operações foram lidas do `HDK_trades_<magic>.csv` exportado pelo robô. O que está provado é o
esqueleto operando pela biblioteca no **Strategy Tester**; em conta (demo ou real) ele nunca rodou.

**Registrado na entrega:** `robots/scaffold.py`, modelos em `robots/templates/` e `mt5x robots new <Nome> --dir <pasta>`
(registra a pasta no catálogo). O esqueleto traz risco, stop pelo ATR, limites, parada de emergência e exportação de
operações; a estratégia entra nos blocos marcados `MT5X:PARAMETROS`, `GLOBAIS`, `INIT` e `SINAL`
(`scaffold.set_section` preenche um bloco, para a T10.10 usar). **Conferido no host:** o robô recém-criado passa em
`mt5x robots validate` e compila no MetaEditor com a biblioteca real 1.0.0, sem erros nem avisos, vazio e também com
um sinal de cruzamento de médias. **Não conferido:** o robô operando no Strategy Tester. A tentativa foi feita com o
MT5 do usuário aberto e o teste, corretamente, não rodou. Antes de marcar ✅: com o MT5 fechado, criar um robô,
escrever um sinal simples, rodar `mt5x robots backtest` e confirmar que há operações e que todas têm stop.

- **Objetivo:** começar um robô novo já certo: sobre a biblioteca de segurança, com contrato, presets e teste.
- **Depende de:** T10.1, T10.2.
- **Arquivos:** `mt5_extracao/robots/templates/` (novo: `Robot.mq5.tmpl`, `robot.json.tmpl`,
  `Conservador.set.tmpl`, `README.md.tmpl`), `mt5_extracao/robots/scaffold.py` (novo), `cli.py`, `pyproject.toml`
  (incluir os modelos no pacote), `tests/test_robots_scaffold.py` (novo).
- **Passos:**
  1. `mt5x robots new <Nome> --dir <pasta> [--symbol X --tf M5]` cria `Experts/<Nome>/<Nome>.mq5`,
     `<Nome>.robot.json` (contrato 1), `Presets/<Nome>_Conservador.set` e `README.md` no layout do MT5.
  2. O modelo do EA tem: os parâmetros obrigatórios do contrato, `OnInit`/`OnTick`/`OnTester` prontos, exportação
     de trades, e **um único ponto marcado** onde o desenvolvedor escreve o sinal, chamando a função de entrada da
     biblioteca. O esqueleto não tem estratégia: sem sinal, ele não opera.
  3. Magic number: gerado e conferido contra o catálogo (não repetir um existente).
  4. Recusa sobrescrever pasta que já existe.
- **Critérios de aceite:** o robô recém-criado passa em `mt5x robots validate`, compila no MetaEditor do host sem
  erros e, na bateria estática, não tem nenhum ✗.
- **Verificação:** `pytest -q` (conteúdo gerado e `contract.validate`); manual no host: `new` → `compile`.
- **Armadilhas:** modelo é texto com marcadores simples (`{{NOME}}`); não adicione motor de templates.

### T10.4 — `mt5x robots check` ✅

**Validação real (2026-10-12):** as quatro etapas rodaram de verdade no host, em 17 s: contrato (passou), instalar e
compilar (3,8 s), backtest (10,4 s, 99 operações, profit factor 0,96) e bateria, que **reprovou** como devia um
cruzamento de médias ingênuo (menos de 200 operações, profit factor abaixo de 1,3, expectativa negativa). Código de
saída 1 e relatório JUnit com 24 casos e 4 falhas. Dois achados da execução:

- `--close-terminal` fechou um terminal real pela primeira vez (duas vezes), parou e religou a ponte. Fica conferido
  no Wine o que as T8.5 e T8.7 tinham só com processos simulados.
- Na primeira tentativa havia uma atualização do MT5 pendente: o terminal leu o `.ini`, chamou o LiveUpdate, saiu
  sem testar e foi reaberto pelo atualizador. O comando terminou em "não gerou o relatório". A segunda execução
  funcionou. `run_tester` agora reconhece o caso (relatório ausente e terminal aberto) e diz para fechar e repetir.

**Registrado na entrega:** `robots/pipeline.py` e `mt5x robots check <robô> --symbol X --tf M5 [--preset] [--report
arquivo.xml] [--close-terminal]`. Quatro etapas, parando na primeira que falhar: contrato → instalar e compilar →
backtest → bateria. Situações: passou, aviso (não reprova), falhou, não executada. `--json` devolve as etapas e a
bateria; `--report` grava JUnit XML com um caso por etapa e por item da bateria. Decisões: robô **sem** contrato
declarado recebe aviso na primeira etapa e segue (robôs antigos continuam conferíveis); backtest **sem nenhuma
operação** reprova a etapa. **Não conferido:** a execução real no host (o MT5 do usuário estava aberto); junto com a
pendência da T10.3, é o mesmo teste: `new` → sinal simples → `check`.

- **Objetivo:** o ciclo do desenvolvedor em um comando, utilizável em CI.
- **Depende de:** T10.1.
- **Arquivos:** `mt5_extracao/robots/pipeline.py` (novo), `cli.py`, `tests/test_robots_pipeline.py` (novo).
- **Passos:**
  1. `pipeline.check(db, config, robot, symbol, timeframe, start, end, preset)` executa, parando no primeiro que
     falhar: `contract.validate` → `mt5_tools.compile_robot` → `backtest.run_mql5` → `battery.run_battery`.
     Devolve as etapas com situação, duração e detalhe.
  2. `mt5x robots check <robô> --symbol X --tf M5 [...]`: imprime as etapas; `--json` devolve tudo; `--report
     arquivo.xml` grava JUnit XML (uma `testcase` por etapa e por item da bateria) com a biblioteca padrão.
  3. Código de saída: 0 só se todas as etapas passarem.
  4. Mesma regra da T8.5 para o terminal aberto: para e explica, a não ser com `--close-terminal`.
- **Critérios de aceite:** com executáveis simulados, o comando percorre as quatro etapas e o XML é lido por
  `xml.etree`; uma falha de compilação interrompe antes do backtest.
- **Verificação:** `pytest -q`; manual no host com um EA de exemplo.
- **Armadilhas:** não duplique lógica: a `pipeline` só chama o que já existe.

### T10.5 — Bateria completa para MQL5 ✅

**Registrado na entrega:** `mt5x robots battery <robô> --deep [--close-terminal]`. A partir do último backtest, roda
`2 + 2 × parâmetros de "tunable"` testes: o período em 70% inicial e 30% final, e cada parâmetro em −20% e +20%
(inteiros são arredondados; variação que não muda o valor é pulada). Os itens "fora da amostra" e "robustez" da
bateria são **trocados** pelos medidos. As execuções extras ficam em `_robot_runs` com
`source = "strategy tester (bateria)"` e não viram o "último backtest" do card nem a base da próxima bateria. Antes
de rodar, o comando diz quantos testes são e o tempo estimado. Sem `tunable` no `robot.json`, a robustez fica como
aviso. `run_mql5` e `run_battery` ganharam parâmetros no fim (`source`; `deep`, `runner`, `is_running`), com o teste
da API atualizado. **Execução real (2026-10-12, host):** 10 testes em 104 s (cerca de 10 s cada) no robô de sonda
com quatro parâmetros; a robustez reprovou o cruzamento de médias, como esperado (profit factor entre 0,81 e 1,00
nas variações).

- **Objetivo:** trocar os dois itens que hoje são aviso ou aproximação em MQL5 por medidas reais.
- **Depende de:** T10.4.
- **Arquivos:** `robots/battery.py`, `robots/backtest.py`, `robots/store.py`, `cli.py`,
  `tests/test_robots_battery.py`.
- **Passos:**
  1. **Fora da amostra:** `mt5x robots battery <robô> --deep` roda o Strategy Tester em dois períodos (70% inicial e
     30% final das datas do último backtest) e exige profit factor > 1 no período final.
  2. **Robustez ±20%:** para cada parâmetro numérico do contrato marcado como `tunable` no `robot.json`, roda o
     teste com −20% e +20%; aprovado se todas as variações mantêm profit factor acima de
     `Criteria.robustness_min_pf` (1,0; o mesmo critério das estratégias Python, em `strategies/validation.py`).
  3. Grave cada execução extra em `_robot_runs` com `source = "strategy tester (bateria)"` para não virar o
     "último backtest" do card (ajuste `attach_runs`).
  4. Mostre antes quantos testes serão rodados e o tempo estimado; sem `--deep` a bateria fica como hoje.
- **Critérios de aceite:** com o terminal simulado, o número de execuções é `2 + 2 × parâmetros`; a aprovação do
  robô só muda se algum item novo reprovar.
- **Verificação:** `pytest -q`; manual no host com o EA de exemplo (anote o tempo no plano).
- **Armadilhas:** cada teste abre e fecha o terminal (~12 s no host atual); não rode em paralelo na mesma instalação.

### T10.6 — Paridade Python × MQL5 ✅

**Registrado na entrega:** `robots/parity.py` e `mt5x robots parity <robô MQL5> --strategy <chave> --symbol X --tf M5`
(ou `"parity_with"` no `robot.json`). Roda o backtest Python e o do Strategy Tester no mesmo período e mostra:
operações de cada lado, casadas, com o mesmo sinal de resultado, as sem par (com o horário) e a diferença do
resultado total. Critério original: ≥ 95% casadas e diferença ≤ 5% do resultado líquido; **trocado em 2026-10-12**
por decisão do dono (veja a T10.10): ≥ 95% casadas, ≥ 95% dos pares a até 5%, diferença ≤ 1% do giro bruto e
mediana da razão entre 0,98 e 1,02. Código de saída 1 fora dele. `sdk` ganhou
`compare_parity`, `run_parity` e `ParityReport`.

**Diferença em relação ao passo 1:** o casamento é só pelo **horário de saída** (tolerância de uma barra), não por
horário e direção, porque `_robot_runs.trades_json` guarda horário, resultado e volume, sem a direção. O relatório
conta, em troca, quantos pares têm o mesmo sinal de resultado. Guardar a direção fica como melhoria.

**Conferido na T10.10:** um par real foi comparado (robô gerado × a própria declaração) e a ferramenta fez o que
devia: apontou as 6 operações divergentes pelo horário e a diferença de resultado. Veja os números lá. Atenção ao comparar resultado em dinheiro:
o lote do Python vem de `[STRATEGY] risk_per_trade` e o do robô, do parâmetro de risco dele; use o mesmo risco e o
mesmo capital nos dois lados.

- **Objetivo:** provar que a versão MQL5 de uma estratégia faz as mesmas operações que o protótipo em Python.
- **Depende de:** T10.3, T8.5. Absorve a parte "comparar os trades" da T8.9; portar as seis estratégias continua na T8.9.
- **Arquivos:** `mt5_extracao/robots/parity.py` (novo), `cli.py`, `tests/test_robots_parity.py` (novo).
- **Passos:**
  1. `parity.compare(python_trades, mql5_trades, tolerance)` casa as operações por horário de saída e direção e
     devolve: casadas, só no Python, só no MQL5, diferença de resultado por operação.
  2. `mt5x robots parity <robô MQL5> --strategy <chave Python> --symbol X --tf M5` roda os dois backtests no mesmo
     período e mostra o relatório. O `robot.json` pode declarar `"parity_with": "<chave>"`.
  3. Critério padrão: ≥ 95% das operações casadas e diferença total de resultado ≤ 5%. Registre os números; não
     ajuste o critério para um robô passar.
- **Critérios de aceite:** listas idênticas dão 100%; uma operação a mais de um lado aparece nomeada no relatório.
- **Verificação:** `pytest -q` com listas sintéticas.
- **Armadilhas:** diferenças legítimas existem (modelo de ticks, spread por barra, horário da corretora): o
  relatório mostra, quem decide é a pessoa.

### T10.7 — API Python pública ✅

**Registrado na entrega:** `mt5_extracao/sdk.py` reexporta 27 nomes (`API_VERSION = 1`); nenhum módulo foi movido.
Como `contract.validate` e `strategies.validation.validate` têm o mesmo nome, entram como `validate_contract` e
`validate_strategy`; `scaffold.create` entra como `new_robot`. `tests/test_sdk_api.py` fixa a lista de nomes e os
parâmetros de cada função e confere que todo nome público está em `docs/sdk/api.md`. `sdk` entrou em `CORE_MODULES`
(importa sem `MetaTrader5` e sem `tkinter`). Feita antes das T10.5 e T10.6, que estão paradas à espera da execução
real: quando elas entrarem, `parity` e a bateria `--deep` são acrescentadas aqui, no fim, sem quebrar o que existe.

- **Objetivo:** quem usa o projeto como biblioteca importa de um lugar só, que não muda sem aviso.
- **Depende de:** T10.4.
- **Arquivos:** `mt5_extracao/sdk.py` (novo), `docs/sdk/api.md` (novo), `tests/test_sdk_api.py` (novo), `pyproject.toml`.
- **Passos:**
  1. `sdk.py` reexporta, com `__all__`, o que é público: catálogo (`load_catalog`, `Robot`), contrato (`validate`),
     ferramentas (`compile_robot`), backtest (`run_python`, `run_mql5`), bateria (`run_battery`, `Report`),
     `pipeline.check`, e de estratégias `Strategy`, `get_strategy`, `run_backtest`, `validate`.
  2. `tests/test_sdk_api.py` fixa a lista de nomes e as assinaturas: mudar a API passa a exigir mudar o teste.
  3. `docs/sdk/api.md`: cada nome, o que faz e um exemplo curto. Política: versão semântica; o que está em `sdk` só
     muda de forma incompatível em versão maior, com um ciclo de aviso (`DeprecationWarning`).
  4. `sdk.py` não importa `tkinter` nem `MetaTrader5` (entra em `CORE_MODULES` de `tests/test_core_decoupling.py`).
- **Critérios de aceite:** `from mt5_extracao import sdk` funciona sem MT5 e sem Tk; o teste de superfície passa.
- **Verificação:** `pytest -q`.
- **Armadilhas:** não mova módulos: só reexporte.

### T10.8 — Documentação do desenvolvedor ✅

**Registrado na entrega:** `docs/sdk/README.md` (porta de entrada), `docs/sdk/tutorial.md` e `docs/sdk/exemplo/`
(o robô gerado no passo 8 do tutorial, com a declaração `cruzamento.json`). O tutorial foi executado inteiro no
host em 2026-10-12, em uma base e uma pasta de robôs novas, e cada bloco de saída foi colado dessa execução; só os
caminhos pessoais foram encurtados. O exemplo é um cruzamento de médias que **perde no teste e é reprovado pela
bateria**, e o texto diz isso no topo: serve para mostrar o ciclo e a bateria trabalhando. O passo 9 (conta demo)
só aponta para o guia e repete que não foi validado com o MT5 real. `tests/test_sdk_docs.py` garante que o exemplo
publicado é exatamente o que `generate` produz hoje (se o esqueleto mudar, o exemplo e o tutorial têm de ser
refeitos), que os links existem e que todo `mt5x robots <ação>` citado existe.
**Diferença em relação ao texto abaixo:** o exemplo publicado vem de `robots generate`, não de `robots new`; o
caminho do `new` com o sinal escrito à mão está no tutorial (passos 1 a 7), com saídas reais.

- **Objetivo:** alguém que nunca viu o projeto vai do zero a um robô aprovado na bateria seguindo um texto.
- **Depende de:** T10.3, T10.7.
- **Arquivos:** `docs/sdk/README.md`, `docs/sdk/tutorial.md`, `docs/sdk/exemplo/` (robô completo, criado com
  `robots new`, com sinal simples), `README.md` (seção curta apontando para `docs/sdk/`).
- **Passos:** tutorial na ordem `new` → escrever o sinal → `validate` → `compile` → `backtest` → `battery` →
  `check` → `parity` → demo. Cada passo com o comando, a saída esperada e o erro mais comum.
- **Critérios de aceite:** todos os comandos do tutorial foram executados no host atual e as saídas coladas são
  reais; o robô de exemplo passa em `mt5x robots check`.
- **Verificação:** rodar o tutorial inteiro em uma pasta limpa.
- **Armadilhas:** o exemplo é didático, não uma estratégia para operar: diga isso no topo.

### T10.9 — Pacote publicável

- **Objetivo:** instalar com `pip install` e saber o que mudou a cada versão.
- **Depende de:** T10.8 e **T10.W** (não publique um pacote que nunca rodou no Windows dizendo que suporta Windows).
- **Arquivos:** `pyproject.toml`, `CHANGELOG.md` (novo), `.github/workflows/` (job de build), `README.md`.
- **Passos:** versão única lida de um lugar; extras opcionais como em `requirements-optional.txt`; `python -m
  build` no CI; `CHANGELOG.md` no formato "Keep a Changelog". A publicação em si é ação de uma pessoa.
- **Critérios de aceite:** o wheel instala em um ambiente limpo e `mt5x --help` e `from mt5_extracao import sdk`
  funcionam; os modelos de `robots new` estão dentro do wheel.
- **Verificação:** `pip install dist/*.whl` em um venv novo.

### T10.10 — Geração de MQL5 a partir de Python (opcional) ✅

**Registrado na entrega:** `strategies/declared.py` (`SignalSpec`, `DeclaredStrategy`), `robots/generate.py` e
`mt5x robots generate <declaração.json> --name <Nome> --dir <pasta>`. A declaração é um JSON: indicadores `sma`,
`ema` e `rsi` sobre o fechamento; condições `cross_above`, `cross_below`, `above`, `below` entre indicadores,
`close` ou números (todas precisam valer); `atr_period`, `stop_atr` e `target_r`. O robô gerado é o esqueleto da
T10.3 com os blocos marcados preenchidos, os padrões de stop, alvo e ATR ajustados, `tunable` e
`"parity_with": "declared"` no `robot.json`, e a declaração gravada ao lado (`<Nome>.signal.json`).
`mt5x robots parity <robô>` compara o robô com a própria declaração (`parity.run_declared`), igualando o risco por
operação ao do backtester Python e abrindo os limites de perda só nesse teste. O ATR do stop, no lado Python, é a
média simples do true range, como o `iATR` do MT5.

**Diferença em relação ao passo 3:** a entrada é um arquivo JSON, não um `.py`. No passo 4, em vez de reescrever uma
das seis estratégias (todas usam saídas que o esqueleto não tem: trailing, saída por sinal), a prova foi feita com
um cruzamento de médias simples declarado.

**Paridade real (2026-10-12, host; EURUSD H1, 2025-10-01 a 2026-09-30; médias 10 × 40, stop 1,5 × ATR(14), alvo 2R;
risco 1% sobre 100.000; modelo OHLC de 1 minuto):**

| Medida | Python | MQL5 | Critério |
|---|---|---|---|
| Operações (fora o aquecimento) | 145 | 145 | — |
| Casadas pelo horário de saída | 139 (95,9%) | | ≥ 95% ✓ |
| Pares com o mesmo sinal de resultado | 139 de 139 | | — |
| Resultado total | −10.057,34 | −10.970,90 | diferença 9,1%; máximo 5% ✗ |

**O critério não foi cumprido** (e não foi afrouxado). O que a medição mostrou:

- Sem excluir o aquecimento eram 145 × 147 operações e 94,6% casadas: o Tester tem histórico antes do início do
  período e o Python não. `parity.compare` passou a aceitar `warmup_until`, e `run_declared` deixa de fora, dos dois
  lados, o que encerrou antes de haver barras para o maior indicador (2 operações do MQL5).
- As 6 operações sem par são as mesmas dos dois lados, com a saída em barras diferentes. Causa provável, **não
  confirmada**: dentro de uma barra H1 o Python não sabe se o stop ou o alvo veio primeiro (assume o stop) e o
  Tester sabe, pelo OHLC de 1 minuto; e o stop do Python parte do fechamento da barra do sinal, enquanto o do
  robô parte do preço de entrada.
- Nos pares casados o lote do MQL5 é sistematicamente cerca de 4% menor (mediana 0,963) e o resultado acompanha
  (mediana 0,961). **Causa não identificada.**

**Atualização (2026-10-12, correção de exatidão):** as causas foram achadas e medidas. Os "4% no lote" não eram
erro de cálculo: o robô fazia duas operações (duas perdas) no aquecimento, que o Python não fazia, e o capital dele
ficava cerca de 2% menor dali em diante; o resto vinha da âncora do stop. Corrigido na origem: o esqueleto fixa o
stop no fechamento da barra do sinal (como o backtester Python) e os dois lados começam no mesmo instante. A
exclusão do aquecimento usada na primeira medição deixou de ser necessária. Números novos, causas restantes e
todas as lacunas estão em [sdk/exatidao.md](sdk/exatidao.md). Resumo: 144 × 144 operações, 97,9% casadas e 96,5%
dos pares a até 5% com "cada tick"; a diferença total é 0,59% do giro bruto, mas 12,2% do resultado líquido.

**Decisão do dono do projeto (2026-10-12): trocar o critério pelas medidas que não dependem do líquido.** O
critério de paridade passou a ser, com as quatro condições valendo juntas:

| Condição | Limite | Medido (EURUSD H1, "cada tick") |
|---|---|---|
| Operações casadas pelo horário de saída | ≥ 95% | 141 de 144 (97,9%) |
| Pares com resultado a até 5% um do outro | ≥ 95% | 136 de 141 (96,5%) |
| Diferença do resultado total sobre o giro bruto | ≤ 1% | 0,6% |
| Mediana da razão MQL5/Python entre os pares | de 0,98 a 1,02 | 1,000 |

A quarta condição não estava na recomendação: acrescentei porque as duas medidas novas deixariam passar um viés
sistemático (todo par 4% menor, como o defeito do capital achado antes), e ela o pega. A diferença sobre o
resultado líquido continua no relatório, só como informação (12,2%). A paridade passou a rodar sempre no modelo
"cada tick", qualquer que seja `[ROBOTS] tester_model`: ela compara a **lógica**; a diferença de custo para os
ticks reais é a lacuna 1 de [sdk/exatidao.md](sdk/exatidao.md) e é medida à parte. Os limites de 1% e de 2% foram
fixados por mim, antes de rodar a medição final, e não devem ser afrouxados para um robô passar.

**Texto anterior à decisão:** o critério da T10.6 mede a diferença sobre o resultado **líquido**, que
fica instável quando o líquido é pequeno perto do giro. Não o alterei. As opções são manter (e a tarefa só fecha
com uma estratégia de resultado líquido maior) ou trocar por medidas que não dependem do líquido: pares a até 5%
(hoje 96,5%) e diferença sobre o giro bruto (hoje 0,59%).

**Texto original, antes da correção:** achar a causa dos 4% no lote (comparar `CHDKRiskGuard.CalcLot` com
`SymbolSpec.lots_for_risk` em uma operação) e decidir a âncora do stop; depois repetir a medição. Enquanto isso, o
robô gerado serve para estudo: as entradas e o sinal do resultado coincidem, o valor em dinheiro não.

- **Objetivo:** escrever a estratégia uma vez, em Python, e obter o robô MQL5 correspondente, sem traduzir à mão.
- **Depende de:** T10.3 (modelo do robô) e T10.6 (paridade, que é como se prova que a geração está certa).
- **Limite, por decisão registrada:** não é um tradutor de Python. `gerar_sinais` das seis estratégias é código
  pandas livre e **não** é convertido. O que se gera é MQL5 a partir de uma **declaração**: indicadores de uma
  lista suportada e condições de entrada, saída, stop e alvo escritas como dados.
- **Arquivos:** `mt5_extracao/strategies/declared.py` (novo), `mt5_extracao/robots/generate.py` (novo),
  `mt5_extracao/robots/templates/Signal.mqh.tmpl` (novo), `cli.py`, `tests/test_robots_generate.py` (novo).
- **Passos:**
  1. `declared.py`: `SignalSpec` (dataclass) com indicadores (`sma`, `ema`, `rsi`, `atr`, `donchian`, `bollinger`:
     só os que já existem em `strategies/indicators.py`), condições (`cruza_acima`, `cruza_abaixo`, `maior`, `menor`
     entre séries ou constantes), stop e alvo em múltiplos de ATR. `DeclaredStrategy(Strategy)` executa a
     declaração no backtester Python: mesma interface `gerar_sinais`.
  2. `generate.py`: `to_mql5(spec) -> str` gera o módulo de sinal (`iMA`, `iRSI`, `iATR`... e as condições) que entra
     no ponto marcado do modelo da T10.3. Indicador ou condição fora da lista: erro dizendo o que não é suportado.
  3. `mt5x robots generate <arquivo.py ou chave> --name <Nome> --dir <pasta>`: cria o robô (como `robots new`) já com
     o sinal gerado e `"parity_with"` no `robot.json`.
  4. Reescreva **uma** das seis estratégias como declaração (a mais simples: cruzamento de médias ou Donchian) e
     prove a paridade com a T10.6 no host. As outras só entram se couberem na declaração sem gambiarra.
- **Critérios de aceite:** o robô gerado compila no MetaEditor do host; `mt5x robots parity` dele contra a própria
  declaração em Python cumpre o critério da T10.6; os números ficam registrados aqui.
- **Verificação:** `pytest -q` (texto gerado e execução Python da declaração); manual no host: `generate` →
  `compile` → `parity`.
- **Armadilhas:** barra em formação × barra fechada (gere sempre sobre a barra fechada, índice 1); `ArraySetAsSeries`;
  o MQL5 gerado nunca envia ordem direto, só chama a entrada da biblioteca de segurança.

### T10.11 — Preparação para o MQL5 Market (opcional)

- **Objetivo:** saber se um robô está pronto para ser enviado ao MQL5 Market e montar o que o vendedor precisa.
- **Depende de:** T10.4.
- **Status:** EM ANDAMENTO (2026-10-14; log isolado por execução autorizado pelo dono do projeto).
- **Limite:** o envio **não** é automatizado. Ele é feito pela pessoa na área do vendedor do MQL5.com, que também
  roda a validação automática oficial. Esta tarefa não promete aprovação no Market.
- **Regras conferidas em 2026-10-14:** [Rules of Using the Market Service](https://www.mql5.com/en/market/rules),
  [The checks a trading robot must pass](https://www.mql5.com/en/articles/2555),
  [How to publish a product](https://www.mql5.com/en/articles/385) e
  [propriedades `#property` do MQL5](https://www.mql5.com/en/book/basis/preprocessor/preprocessor_properties).
  O serviço exige cadastro de vendedor e envio manual; a regra não será automatizada. O produto não pode mudar de
  funcionamento por tipo/número de conta ou servidor; textos, nomes de entradas e mensagens do produto devem estar
  em inglês; não pode prometer lucro nem incluir links de terceiros na apresentação. A distribuição é um único
  `.ex5`, com dependências incluídas como recursos. O Market executa testes automáticos no Strategy Tester em
  condições, símbolos e timeframes diferentes; aprovação não garante lucro nem ausência de erros. Essas regras não
  substituem a bateria do mt5x. O bloqueio de `#import` de DLL continua sendo uma política local desta tarefa.
- **Versão:** a referência de `#property` descreve o formato Market `X.Y`; o MetaEditor instalado apresenta o aviso
  Market 68 para `0.10` ("must be xxx.yyy"). Testei no Wine, em arquivo temporário: `0.10` compilou com aviso 68;
  `1.000` compilou com 0 erros e 0 avisos. O pré-check seguirá o requisito mais estrito do compilador observado no
  host (`XXX.YYY`) e ainda exigirá compilação sem avisos.
- **Arquivos:** `mt5_extracao/robots/market.py` (novo), `cli.py`, `tests/test_robots_market.py` (novo).
- **Passos:**
  1. `market.check(robot, compile_result, runs) -> List[Item]` (mesmo formato da bateria): `#property version` no
     formato `xxx.yyy`; compila sem erros **e sem avisos**; sem `#import` de DLL; `#property description`
     preenchida; sem símbolo, conta ou caminho fixo no código; trata lote mínimo/passo e falta de margem sem erro;
     backtest sem erros no log em mais de um ativo e timeframe. Cada backtest grava o trecho novo do log do terminal
     e dos agentes em `Tester/mt5x_runs/<run_id>.log`; `_robot_runs.metrics_json` guarda o caminho e se a captura
     foi bem-sucedida. Bases antigas continuam abrindo; execuções sem captura não contam como evidência nova.
  2. `mt5x robots market <robô> [--pack <pasta>]`: lê o log da última compilação, mostra os itens e aceita avisos
     que ainda precisam de revisão do vendedor; com `--pack`, copia o `.ex5`, `description.txt` e um `LEIA-ME.txt`
     com o que ainda é manual (imagens, textos da página, preço, cadastro e envio).
  3. Nunca copie o `.mq5` para o pacote sem `--with-source`; por padrão ele contém só `.ex5`, descrição e leia-me.
- **Critérios de aceite:** um robô com versão `0.10` ou com DLL é reprovado com o motivo; o pacote de um robô
  aprovado contém só o que foi pedido.
- **Verificação:** `pytest -q`.
- **Decisão do dono do projeto (2026-10-14):** capturar e registrar log isolado por execução; testes da T10.11
  podem usar a conta demo. A captura mantém apenas os bytes acrescentados durante aquele backtest, sem copiar o log
  diário inteiro. O Strategy Tester continua sendo simulação e não envia ordens a uma conta.
- **Verificação no host (2026-10-14):** antes da alteração, `.venv/bin/python -m pytest -q` — 648 aprovados,
  8 ignorados (327,36 s). Depois da implementação: 656 aprovados, 8 ignorados (367,21 s); testes focados de Market
  e Tester: 18 aprovados; `doctor`: 16 OK, 2 avisos conhecidos (`pandas-ta` opcional ausente e ponte configurada
  como desativada); `git diff --check` passou.
- **Backtest real pendente:** o terminal está aberto, mas a consulta local do MT5 devolveu `account_info = None`;
  o diagnóstico confirma `[BRIDGE] enabled = false`. Não fechei o terminal nem rodei o Tester sem confirmar a demo
  e o estado da conta. Nenhuma configuração foi alterada e nenhuma ordem foi enviada.

### T10.12 — Custo real no backtester Python ✅

**Registrado na entrega (2026-10-12).** O que foi feito:

- **Swap:** `mt5x specs` passa a gravar `swap_long`, `swap_short`, `swap_mode` e `swap_rollover3days` (colunas novas
  em `_symbol_specs`, aditivas; base antiga recebe por `ALTER TABLE`). O backtester cobra o swap de cada noite que
  a posição atravessa, com a noite tripla. Só o modo "em pontos" é calculado.
- **Spread medido nos ticks:** `mt5x specs --symbols X --calibrate` lê os ticks já extraídos (`mt5x ticks`) e grava,
  por hora do dia, a média do spread ponderada pelo tempo e a média do spread no **primeiro tick da hora**. O
  backtester cobra, em cada execução, o maior entre o spread da barra e o medido. Não usa o MT5.
- **Paridade:** o lado Python da paridade continua com o spread das barras (`real_spread=False`), porque o modelo
  "cada tick" do Tester também usa. O swap vale nos dois.
- `mt5x backtest` avisa quando o ativo não tem calibração ("custo otimista").

**Medição, no mesmo cenário da tabela abaixo** (robô `ExemploCruzamento`, EURUSD H1, de 2025-10-03 a 2026-09-30,
risco de 1%, limites abertos; Tester refeito hoje: "cada tick" 144 operações e −10.255,38; ticks reais 143 e
−12.243,69). Calibração com 3.605.060 ticks de 23 dias (quatro semanas espalhadas de 2025-11 a 2026-09):

| Custo no Python | Resultado | Contra "cada tick" (dif. sobre o giro) | Contra ticks reais: casadas | pares a até 5% | dif. sobre o giro |
|---|---|---|---|---|---|
| Barra, sem swap (antes) | −9.138,67 | 0,59% | 96,5% | 95,7% | **1,63%** |
| Barra + swap (o que a paridade usa) | −9.604,25 | 0,34% | 96,5% | 96,4% | 1,39% |
| Ticks + swap (novo padrão) | −12.247,82 | 1,06% | 97,9% | 96,5% | **0,00%** |

- **Critério de aceite cumprido:** contra os ticks reais a diferença é 0,00% do giro (máximo 1%), e a paridade em
  "cada tick" melhorou (de 0,59% para 0,34%), medida de novo pela CLI.
- **A causa, conferida nos ticks:** a hipótese 1. O spread das barras desta corretora é praticamente zero (média
  de 0,57 ponto em H1 e 0,16 em M1), mas nos ticks ele é de 2 a 3 pontos na maior parte do dia, 20 na hora da
  virada do dia e 37, em média, no primeiro tick dessa hora. Em 2026-07-23 00:00 o primeiro tick tinha 21 pontos:
  a compra saiu a 1,14129 em vez de 1,14112, o alvo de 2R subiu de 1,14336 para 1,14387 e o preço só chegou a
  1,14358. A operação que dava +1.826 em "cada tick" deu −909 com ticks reais. Essa única operação valia 1,4% do
  giro.
- **O swap** explica 466 da diferença (o Python não cobrava nada).
- **Hipótese 3 (ordem stop/alvo com barras M1): medida e descartada.** Resolver o interior da barra com M1 mudou o
  resultado em 3 (−9.141 contra −9.139); além disso a corretora só entrega cerca de 100 mil barras M1 (três
  meses). O código do experimento não ficou no projeto.
- **Revalidação das seis estratégias** (`validate`, EURUSD em H1, H4 e M15, custo antigo contra o novo): as 18
  combinações já estavam reprovadas e continuam. O custo novo piorou todas; nenhum veredito mudou. Exemplos: Squeeze
  H1, profit factor fora da amostra de 1,20 para 1,14; Squeeze H4, de 1,32 para 1,00; BB/RSI H4, de 1,37 para 1,29.

**O que não foi feito ou não está provado:**

- O acerto de 0,00% é de **um** cenário. Ele depende de uma operação na virada do dia cair do mesmo lado nos dois
  modelos; em outro ativo, período ou robô a diferença pode ser maior. Não medi outros.
- A calibração é uma média por hora de uma amostra de 23 dias. O spread da primeira semana extraída (setembro de
  2026) era bem menor que o das outras três, ou seja, ele muda ao longo do ano e a amostra pesa no resultado.
- Com a calibração, o Python fica **mais longe** do modelo "cada tick" (1,06%), como esperado: são custos
  diferentes. Por isso a paridade não usa a calibração.
- Os ticks ocupam espaço: 3,6 milhões de ticks levaram a base de 209 MB para 328 MB.
- Swap em outros modos (dinheiro, juros) fica em zero, sem aviso na saída do backtest.
- Ativos da B3 não foram medidos (a conta usada não tem).
- O caminho (b) do texto abaixo não foi adotado, e os robôs da T8.9 não foram comparados de novo com ticks reais.

**Texto original da pendência (2026-10-12), mantido como registro:**

Registrada a pedido do dono do projeto para ser retomada depois. **Nada foi alterado no backtester por causa dela.**

- **O problema:** o backtester Python (`strategies/backtester.py`) dá um resultado melhor do que o Strategy Tester
  com ticks reais para a mesma lógica. Ele afeta tudo o que usa o backtester: `mt5x backtest`, `mt5x validate`
  (Filtro C), a triagem e o lado Python da paridade. Enquanto não for resolvido, **a decisão de operar se apoia no
  Strategy Tester com ticks reais**, não no Python.
- **A medição (2026-10-12):** host Linux, MT5 build 6258 no Wine, conta MetaQuotes-Demo, EURUSD H1, de 2025-10-03
  a 2026-09-30; robô gerado da declaração de cruzamento de médias simples 10 × 40, stop 1,5 × ATR(14) fixado no
  fechamento da barra do sinal, alvo 2R, risco de 1% sobre 100.000, sem limites de perda (como faz
  `parity.parity_params`). O mesmo lado Python (144 operações, resultado −9.138,67, giro bruto 190.777) contra o
  Tester em três modelos:

  | Modelo do Tester | Operações | Casadas | Pares a até 5% | Resultado MQL5 | Diferença para o Python |
  |---|---|---|---|---|---|
  | OHLC de 1 minuto | 144 | 141 | 135 (95,7%) | −7.581,30 | +1.557 (0,82% do giro); **otimista**: stop em gap preenchido no preço do stop |
  | Cada tick | 144 | 141 | 136 (96,5%) | −10.255,38 | −1.117 (0,59% do giro; 12,2% do líquido) |
  | Cada tick com ticks reais | 144 | 140 | 134 (95,7%) | −13.120,89 | **−3.982 (2,09% do giro; 43,6% do líquido)** |

  Diferença por par casado (MQL5 − Python): com "cada tick", média −7,9 e mediana −3,3; com ticks reais, média −8,9
  e mediana −1,7. Com ticks reais, só cerca de −1.250 dos −3.982 vêm dos pares casados; o resto vem das 4 operações
  de cada lado que não casam, ou seja, que saem em outro ponto.
- **Confirmado na T11.1 (2026-10-12):** a coluna `spread` de uma barra do MT5 é o **menor** spread do período (em
  2.880 barras M5, 960 M15, 240 H1, 60 H4 e 10 D1 montadas do M1, só a regra "mínimo" reproduziu o MT5 em todas).
  O backtester usa essa coluna como o custo de cada operação, ou seja, cobra sempre o melhor spread que houve na
  barra. É a hipótese 1 abaixo, agora medida: quanto maior o timeframe, mais otimista o custo.
- **O que já se sabe:**
  - O spread gravado nas barras H1 extraídas é muito baixo: média de 0,57 ponto, percentil 95 de 2 pontos, máximo
    de 38. O backtester usa esse valor (coluna `spread`) como o custo de cada operação.
  - Dentro de uma barra o Python não sabe a ordem entre stop e alvo e assume o stop.
  - As entradas coincidem em horário e preço; a lógica é a mesma (paridade dentro do critério em "cada tick").
- **Hipóteses a testar, na ordem:**
  1. O spread da barra é o do instante da abertura (ou o mínimo), e não o que a operação paga. Com ticks reais o
     spread alarga na virada do dia e em notícia, e um stop de venda pode ser atingido pelo ask sem o bid chegar lá.
     Conferir com `mt5x ticks` em uma das operações divergentes.
  2. Parte da diferença é execução (preço do primeiro tick da barra contra `abertura + spread`).
  3. A ordem stop/alvo dentro da barra H1: repetir o lado Python resolvendo a barra com dados de M1.
- **Caminhos possíveis (a escolher quando retomar):** (a) calibrar o custo por ativo e horário com os ticks reais
  extraídos e gravá-lo em `_symbol_specs`; (b) resolver a ordem stop/alvo com barras M1 quando houver; (c) passar a
  conferir stop e alvo contra o lado certo do preço usando um spread por hora, e não o da barra; (d) no mínimo,
  uma auditoria que compare o spread das barras com o dos ticks e avise quando o backtest está otimista.
- **Como reproduzir:** `mt5x extract --symbols EURUSD --tf H1 --from 2025-10-01 --to 2026-09-30` e
  `mt5x specs --symbols EURUSD`; salve a declaração acima em um JSON e rode `mt5x robots generate <json> --name X
  --dir <pasta>`, `mt5x robots compile x` e `mt5x robots parity x --symbol EURUSD --tf H1` (modelo "cada tick").
  Para os outros modelos, chame `backtest.run_mql5(..., params=parity.parity_params(robot, config), model="4")`
  (ou `"1"`) no mesmo período e compare com `parity.compare`. Os números devem bater com a tabela acima enquanto a
  corretora mantiver o mesmo histórico.
- **Critério de aceite:** no mesmo cenário, a diferença do resultado total entre o Python e o Tester com ticks
  reais fica em até 1% do giro bruto (o limite da paridade), sem piorar a paridade em "cada tick"; e a validação
  das seis estratégias é refeita e os vereditos que mudarem são registrados.

### Correção de exatidão (2026-10-12, PR próprio, depois da T10.10)

Revisão pedida pelo dono do projeto: nenhuma inexatidão pode pôr a operação em risco. O que foi medido, corrigido
e o que continua lacuna está em [sdk/exatidao.md](sdk/exatidao.md). Mudanças de comportamento que afetam tarefas
anteriores:

- **T8.5:** `[ROBOTS] tester_model` passa a ter padrão 4 (ticks reais). O backtest recusa código alterado depois da
  compilação e grava a impressão digital do código, o código do modelo e as entradas sem stop.
- **T8.6:** quatro itens novos na estatística de robôs MQL5 (código atual, mesma configuração, modelo por tick,
  stop em toda entrada). `_robot_checks` ganha a coluna `fingerprint` (aditiva). Backtests gravados antes desta
  correção reprovam no item "código atual" e precisam ser refeitos.
- **T8.7:** a trava "bateria aprovada" exige que a aprovação seja do código que está no disco.
- **T8.4/T8.5/T8.7 e conector:** fechar o terminal só atinge o desta instalação.
- **T10.3:** o esqueleto fixa o stop no fechamento da barra do sinal; robôs criados antes mantêm a regra antiga.

### T10.W — Validação no Windows (pendência)

- **Objetivo:** provar no Windows real o que foi desenvolvido e verificado só no Linux.
- **Depende de:** uma máquina Windows 10 ou 11 com o MetaTrader 5. **Não bloqueia** as tarefas T10.1–T10.8.
- **Arquivos:** `docs/validacao_instaladores.md` (seção "Pendências de validação no Windows").
- **Passos:** execute cada linha do checklist, anote data e resultado; para cada falha, abra um PR próprio com a
  correção e um teste que simule o caso (`monkeypatch` de `paths.is_windows`).
- **Critérios de aceite:** nenhuma linha "⏳ pendente" no checklist.
- **Enquanto não for feita:** siga a "Regra da pendência do Windows" no início desta fase.

**Verificação da fase:** `PYTHONPATH=. xvfb-run -a .venv/bin/python -m pytest -q` e o `doctor` antes e depois de cada
tarefa; ao fim, no host atual: `mt5x robots new` → escrever um sinal simples → `mt5x robots check` verde.

---

## Fase 11 — Testes amplos e operação discreta

Pedida pelo dono do projeto em 2026-10-12, para ser feita antes da T10.11. Quatro pontos, um PR cada:

| Pedido | Tarefa |
|---|---|
| "Montar um tempo maior com uma extração de tempo menor (um gráfico de 5 com uma base de 1, e assim por diante)" | T11.1 |
| "Backtest em vários ativos" | T11.2 |
| "Extrair e operar com o MT5 oculto, sem ocupar tela" | T11.3 |
| "Mas com pop-up de notificação no host das ações executadas" | T11.4 |

Valem as regras da Fase 10: o desenvolvimento segue no host Linux, o que só o Windows confirma vai para o checklist
de `validacao_instaladores.md`, e nada que mexa em número de backtest entra sem ser conferido contra o MT5 real.

### T11.0 — Plano da Fase 11 ✅

Este texto, a linha F11 do ROADMAP e as quatro linhas do painel.

### T11.1 — Montar um timeframe maior a partir de uma base menor ✅

**Medição que definiu as regras (2026-10-12, host, MetaQuotes-Demo, EURUSD de 2026-09-14 a 2026-09-25):** foram
extraídos do MT5 os timeframes M1 (14.396 barras), M5, M15, H1, H4 e D1. Montando cada um a partir do M1:

| Timeframe | Barras no MT5 | Barras montadas | Abertura, máxima, mínima, fechamento e volumes | `spread` = primeira / **mínimo** / máximo / última |
|---|---|---|---|---|
| M5 | 2.880 | 2.880 | iguais em todas | 2.815 / **2.880** / 2.746 / 2.795 |
| M15 | 960 | 960 | iguais em todas | 929 / **960** / 899 / 923 |
| H1 | 240 | 240 | iguais em todas | 233 / **240** / 217 / 232 |
| H4 | 60 | 60 | iguais em todas | 53 / **60** / 45 / 60 |
| D1 | 10 | 10 | iguais em todas | 3 / **10** / 0 / 10 |

Conclusão: a coluna `spread` de uma barra do MT5 é o **menor** spread do período. Com essa regra, o comando
`mt5x resample` rodado em uma base só com o M1 produziu os cinco timeframes **iguais aos do MT5 em todas as colunas
e em todas as barras**. Esse fato também confirma a primeira hipótese da T10.12 (registrado lá).

**Registrado na entrega:** `mt5_extracao/resample.py` e `mt5x resample --symbols X --base M1 --tf M5,M15,H1
[--from] [--to]` (as opções seguem as dos outros comandos: `--base` é o timeframe de origem; `--from` e `--to` são
datas). A origem fica na tabela nova `_resampled` e `mt5x tables` mostra "(montada a partir de M1)". Rodar de novo
não duplica; se a base menor cresceu, a série montada acompanha. Recusa, com o motivo, montar de maior para menor,
timeframe que não é múltiplo e semanal ou mensal.

**Não conferido:** ativos com sessão (B3: `WIN$N`, `WDO$N`, ações), porque a conta de demonstração disponível não
os tem. O alinhamento é pelo relógio da base; em H4 a corretora pode alinhar de outro jeito. Antes de usar em um
ativo da B3, repita a comparação acima com a conta da corretora. Indicadores gravados na tabela de origem não são
copiados para a montada.

- **Objetivo:** extrair uma vez o timeframe menor (por exemplo M1) e obter dele os maiores (M5, M15, H1...), sem
  nova extração e com as mesmas barras que o MT5 daria.
- **Arquivos:** `mt5_extracao/resample.py` (novo), `cli.py`, `tests/test_resample.py` (novo), `docs/`.
- **Passos:**
  1. `resample.resample_bars(df, source_tf, target_tf)`: agrupa pelo início da barra do timeframe maior (no horário
     da base): abertura da primeira, máxima, mínima, fechamento da última, soma dos volumes. Só de menor para
     maior, com o maior múltiplo do menor, até D1 (semanal e mensal dependem do calendário da corretora e ficam de
     fora). Uma barra maior com menos barras menores do que o esperado é válida (mercado parado), como no MT5.
  2. **A regra da coluna `spread` é decidida medindo**, não supondo: extraia M1 e M5 do mesmo período no MT5 real,
     monte o M5 a partir do M1 com cada regra candidata (primeira, mínimo, máximo, última) e fique com a que
     reproduz a coluna do MT5. Registre aqui a medição. Isso importa por causa da T10.12.
  3. `mt5x resample --symbols X --from M1 --to M5,M15,H1 [--from-date --to-date]` grava as tabelas dos timeframes
     maiores (upsert, como a extração) e registra a origem. Indicadores gravados na tabela menor não são copiados.
  4. Marque a origem: a tabela montada não pode passar por extraída. Registre em `_extraction_progress` ou em uma
     tabela própria aditiva, e mostre "montado a partir de M1" em `mt5x tables`.
- **Critérios de aceite:** no período medido, o M5 montado do M1 é **igual** ao M5 extraído do MT5 em abertura,
  máxima, mínima, fechamento e volume de ticks, barra a barra; a diferença na coluna `spread`, se houver, fica
  registrada com números.
- **Verificação:** `pytest -q` (dados sintéticos) e a comparação real acima no host.
- **Armadilhas:** horário da corretora × UTC (use a base como está gravada); barras de sessões que não começam em
  múltiplo do timeframe (B3 abre 09:00: M30 e H1 alinham na hora cheia, H4 depende da corretora: confira com o MT5).

### T11.2 — Backtest em vários ativos ✅

**Registrado na entrega:** `mt5x robots backtest <robô> --symbols A,B,C --tf H1` (`backtest.run_many`), para robôs
MQL5 e estratégias Python. Um teste por ativo, quadro comparativo, cada execução gravada; um ativo que falha não
interrompe os outros e o código de saída é 1 se algum falhar. Com o MT5 aberto e sem `--close-terminal`, o comando
para com um aviso só. `mt5x robots battery <robô> --symbol X` escolhe o ativo avaliado (sem a opção, vale o
backtest mais recente). A aprovação passa a guardar o ativo e o timeframe (`_robot_checks.symbol` e `.timeframe`,
colunas aditivas) e a trava de demo **recusa iniciar o robô em um ativo ou timeframe diferente do aprovado**.
Não há resultado "de carteira": cada ativo é um teste independente, e a saída diz isso.
**Execução real (2026-10-12, host, ticks reais):** o robô do tutorial em EURUSD e GBPUSD H1, de 2026-06-01 a
2026-09-30, mais um ativo inexistente: 53 e 54 operações, o terceiro com o motivo da falha, 66 s no total, código
de saída 1.

- **Objetivo:** testar o mesmo robô ou estratégia em uma lista de ativos e ver o resultado lado a lado.
- **Arquivos:** `robots/backtest.py`, `robots/battery.py`, `cli.py`, `tests/test_robots_multi.py` (novo).
- **Passos:**
  1. `mt5x robots backtest <robô> --symbols A,B,C --tf M5 [...]`: roda um ativo por vez (robô MQL5: um Strategy
     Tester por ativo, com o terminal fechado uma vez só), grava cada execução e imprime um quadro: operações,
     acerto, profit factor, resultado, queda máxima e o motivo quando um ativo falha. Um ativo que falha não
     interrompe os outros; o código de saída é 1 se algum falhar.
  2. `--json` devolve a lista de execuções e o resumo.
  3. `mt5x robots battery <robô> --symbol X` escolhe de qual ativo é o backtest avaliado (hoje vale o mais recente).
     A aprovação da bateria passa a registrar o ativo e o timeframe; a trava de demo exige que sejam os mesmos do
     início.
  4. Sem agregação "de carteira" nesta tarefa: cada ativo é um teste independente, e o texto diz isso.
- **Critérios de aceite:** três ativos, um deles sem dados: dois resultados e um motivo; a bateria aprovada em um
  ativo não libera o início em outro.
- **Verificação:** `pytest -q`; no host, um robô em dois ativos reais.

### T11.3 — MT5 oculto ✅

**Registrado na entrega:** `[MT5] hidden = true` (padrão `false`) e `hidden_display = :99`; para uma execução só,
`MT5X_HIDDEN=1 mt5x ...` (e `MT5X_HIDDEN=0` desliga). `bootstrap/display.py` inicia o Xvfb quando preciso e monta o
ambiente dos processos do Wine (DISPLAY da tela virtual, sem WAYLAND_DISPLAY). Usam esse ambiente: a ponte
(`scripts/mt5-bridge.sh`, que pergunta ao próprio Python do projeto) e, por ela, o terminal aberto na extração; o
MetaEditor; o Strategy Tester; e o início do robô. `mt5x mt5 status` diz se o terminal desta instalação está aberto
e, com o modo ligado, lembra que ele está fora da vista; `mt5x mt5 show` grava uma captura da tela virtual (precisa
do ImageMagick). O `doctor` ganhou a verificação "MT5 oculto".

**Conferido no host (2026-10-12, Manjaro, sessão Wayland com XWayland, Wine 11.17):** primeiro a linha de base, com
o modo desligado: a lista de janelas da tela real (`xwininfo -display :0 -root -tree`) mostrou 62 janelas do MT5.
Com `MT5X_HIDDEN=1`: uma extração (EURUSD M15) e um backtest com ticks reais rodaram do começo ao fim com **0
janelas do MT5 na tela real** e 61 na tela virtual, amostradas a cada 4 s durante o teste; o backtest deu o mesmo
resultado do modo visível (53 operações, profit factor 0,68); a captura de `mt5x mt5 show` mostrou o terminal.

**Limites:**
- A tela virtual continua ativa depois dos comandos (processo pequeno); a ponte iniciada no modo oculto precisa ser
  reiniciada para voltar à tela real (`./scripts/mt5-bridge.sh restart` com o modo desligado).
- Diálogos do MT5 que pedem clique (login, atualização) ficam invisíveis: use `mt5x mt5 show` se algo travar.
- A interface gráfica do programa (`./run.sh`) não foi testada com o modo ligado; os comandos `mt5x` foram.
- **Windows:** o terminal é iniciado minimizado e sem tomar o foco (`STARTUPINFO`). Só simulado em teste: linha no
  checklist do Windows. Não é "oculto" de verdade: aparece na barra de tarefas.

- **Objetivo:** extrair, testar e operar sem a janela do MetaTrader 5 aparecer na tela.
- **Arquivos:** `mt5_extracao/bootstrap/display.py` (novo, só biblioteca padrão), `scripts/mt5-bridge.sh`,
  `robots/mt5_tools.py`, `config_builder.py`, `config/config.ini.example`, `doctor.py`, testes.
- **Passos:**
  1. Chave `[MT5] hidden = false` (padrão: nada muda para quem já usa).
  2. **Linux:** com `hidden = true`, o terminal, o MetaEditor e a ponte rodam em um display virtual (Xvfb) próprio
     e fixo, mesmo havendo uma tela real. `display.ensure()` inicia o Xvfb se preciso e devolve o `DISPLAY`; a
     ponte e `run_windows_command` passam a usá-lo. `mt5x doctor` diz se o modo oculto está ativo e se o Xvfb existe.
  3. **Ver o terminal quando precisar:** `mt5x mt5 show` tira uma captura da tela virtual para um arquivo (com o
     que estiver instalado: `import`/`xwd`), e a documentação explica como desligar o modo oculto.
  4. **Windows:** iniciar o terminal minimizado, sem tomar o foco. Não validado: linha no checklist do Windows.
- **Critérios de aceite:** no host, com `hidden = true`, uma extração e um backtest rodam do começo ao fim sem
  nenhuma janela do MT5 na tela real (conferido listando as janelas do display real) e com os mesmos resultados.
- **Armadilhas:** o terminal precisa de um display para abrir; diálogos que pedem clique (login, atualização)
  ficam invisíveis: por isso o passo 3 e os avisos da T11.4. Um terminal oculto operando não pode ser esquecido:
  o `doctor` e o `mt5x status` têm de dizer que ele está aberto.

### T11.4 — Avisos em pop-up das ações executadas ✅

**Registrado na entrega:** `mt5_extracao/notify.py` e a chave `[APP] notifications`. **Vem ligada nas instalações
novas; em um `config.ini` que já existe é preciso acrescentar `notifications = true`** (sem a chave, o padrão do
código é desligado, para não surgirem pop-ups em quem não pediu). No Linux usa `notify-send`; sem ferramenta, sem
sessão gráfica ou desligado, o aviso vira só uma linha de log. `send` nunca levanta erro, e um teste garante que um
notificador quebrado não derruba o comando.

- **Ações do programa que avisam ao terminar:** extração e atualização, compilação, backtest (um ou vários ativos),
  bateria (aprovada ou reprovada, com os itens), conferência (`robots check`), início em demo, PARAR TUDO (urgência
  crítica: fica na tela) e a liberação dele, e quando o MT5 é fechado para um teste.
- **Ações do robô:** `mt5x robots monitor <robô> --watch [--interval 15]` consulta posições e negócios pelo magic
  number (somente leitura) e emite um aviso por negócio novo (abertura: lado, volume, ativo e preço; fechamento:
  resultado), por posição sem stop, quando o PARAR TUDO está ativo e quando a leitura do MT5 cai ou volta. O que já
  existia quando o acompanhamento começa não gera aviso. Ctrl+C encerra só o acompanhamento.
- O número da conta aparece mascarado (`•••6000`); nenhum aviso leva senha nem saldo.

**Conferido no host (2026-10-12):** com `notifications = true`, ao fim de uma extração real o serviço de
notificações da sessão recebeu a chamada (capturada com `dbus-monitor`: aplicativo "MT5 Extração", título "Extração
concluída"). `live.watch` rodou duas consultas contra o MT5 real pela ponte restrita, sem erro.

**Não conferido:** os avisos de operações de um robô em conta (o início em demo, T8.7, ainda não foi validado); a
aparência do pop-up na tela (só a entrega ao sistema foi capturada); e o balão do Windows (PowerShell), só simulado:
linha no checklist. A interface gráfica do programa não ganhou avisos nesta tarefa, só os comandos `mt5x`.

- **Objetivo:** com o MT5 oculto, a pessoa fica sabendo do que aconteceu por avisos do próprio sistema.
- **Arquivos:** `mt5_extracao/notify.py` (novo, só biblioteca padrão), `robots/live.py`, `cli.py`,
  `config_builder.py`, `config/config.ini.example`, testes.
- **Passos:**
  1. `notify.send(título, texto, urgência)`: no Linux usa `notify-send`; no Windows, um balão pelo PowerShell (não
     validado: checklist). Sem ferramenta ou sem sessão gráfica, vira só registro em log. Nunca levanta erro: um
     aviso que falha não pode derrubar uma extração.
  2. Chave `[APP] notifications = true`. Nunca coloque senha, número de conta inteiro ou saldo em um aviso.
  3. **Ações do programa** que avisam ao terminar: extração e atualização (ok, falhas), compilação, backtest,
     bateria (aprovada ou reprovada), início em demo, PARAR TUDO, MT5 fechado e reaberto.
  4. **Ações do robô:** `mt5x robots monitor <robô> --watch [--interval 15]` consulta posições e negócios pelo
     magic number (somente leitura) e avisa a cada abertura e fechamento, com ativo, lado, volume e resultado;
     avisa também posição sem stop e quando o PARAR TUDO está ativo. Termina com Ctrl+C.
- **Critérios de aceite:** um aviso real aparece no host ao fim de uma extração; com o MT5 simulado, cada negócio
  novo gera exatamente um aviso e nenhum se repete entre consultas.
- **O que não será provado aqui:** os avisos de operações de um robô em conta, porque o início em demo (T8.7)
  ainda não foi validado com o MT5 real.

### Painel macro de resultados (T11.5 a T11.7)

Pedido do dono do projeto em 2026-10-12: ver de forma macro os ativos, seus timeframes, as estratégias aplicadas e
os resultados destacados. Duas visões: **por ativo** (todos os ativos com base, colunas por timeframe; ao escolher o
robô ou a estratégia aparece o resultado) e **por robô** (os ativos e timeframes em que ele teve resultado positivo).

**O que foi medido antes de planejar (host, 8 núcleos):** os backtests rodam em fila, um por vez. Estratégia Python:
0,04 s por combinação (um ano de H1, 6.207 barras). Robô MQL5: 10 a 12 s por teste com ticks gerados e cerca de 30 s
com um ano de ticks reais em H1; o MT5 não abre duas vezes a mesma instalação, então não há paralelismo (várias
cópias portáteis do terminal não foram testadas). Não há limite de quantidade, só de tempo.

**Três regras para o painel não enganar** (valem para as três tarefas):

1. "Positivo" não é "aprovado". Com muitas combinações, algumas dão lucro por acaso. O destaque forte é só de quem
   passou na bateria (robô MQL5) ou na validação (estratégia Python) **naquele ativo e timeframe**.
2. Poucas operações não provam nada: abaixo de 30 operações a célula é marcada como tal, qualquer que seja o lucro.
3. Cada número diz de onde veio: Python (otimista em custo, T10.12), Strategy Tester e em qual modelo.

Cada célula mostra a execução **mais recente** daquela combinação, não a melhor (escolher a melhor seria escolher o
acaso).

### T11.5 — Lote de backtests ✅

**Registrado na entrega:** `backtest.run_batch` e `mt5x robots backtest <r1,r2> --symbols A,B --tfs M15,H1 [--resume]`.
O lote é reconhecido pela vírgula no nome dos robôs ou por `--tfs`. `--param` não vale em lote (cada robô tem os
próprios parâmetros): use `--preset`. A retomada usa `backtest.already_tested`: em robô MQL5, só dispensa o teste se
o código, o preset e o modelo do Tester forem os mesmos.
**Execução real (2026-10-12, host):** 6 estratégias Python × 3 ativos × 4 timeframes (M15 extraído; H1, H4 e D1
montados pela T11.1) = 72 testes em 7 s. Dois robôs MQL5 × 3 ativos × 2 timeframes = 12 testes com ticks reais, de
2026-06-01 a 2026-09-30, com o MT5 oculto: 333 s (cerca de 28 s por teste). O robô com o sinal escrito à mão e o
gerado da declaração deram resultados idênticos nas seis combinações.

- **Objetivo:** preencher a matriz sem digitar um comando por combinação.
- **Arquivos:** `robots/backtest.py`, `cli.py`, `tests/test_robots_batch.py` (novo).
- **Passos:** `mt5x robots backtest <robô1,robô2> --symbols A,B --tfs M15,H1 [--from] [--to] [--preset] [--resume]
  [--close-terminal]`: roda as combinações em fila (`backtest.run_batch`), mostra cada uma ao terminar e o quadro no
  fim; `--resume` pula a combinação que já tem execução gravada da mesma configuração (robô MQL5: mesmo código,
  preset e modelo); uma falha não interrompe as outras; com o MT5 aberto, um aviso só; aviso do sistema ao terminar.
- **Critérios de aceite:** 2 robôs × 2 ativos × 2 timeframes = 8 execuções; interrompido e repetido com `--resume`,
  só roda o que falta.

### T11.6 — Matriz de resultados ✅

**Registrado na entrega:** `robots/matrix.py` (`build`, `cell_status`, `positives`) e, na API pública, `build_matrix` e
`matrix_positives`. Cada célula traz a execução mais recente, a origem do número e o veredito (bateria ou validação,
com a data). Uma aprovação cujo código mudou depois deixa de valer e a célula vira "reprovado", com o motivo. As três
regras têm um teste cada. Só leitura: `build` não roda teste nenhum.

- **Objetivo:** uma função única que o painel (e depois a interface) consome.
- **Arquivos:** `robots/matrix.py` (novo), `sdk.py`, `docs/sdk/api.md`, `tests/test_robots_matrix.py` (novo).
- **Passos:** `matrix.build(db, config)` devolve os ativos com base, os timeframes, os robôs e uma célula por
  (robô, ativo, timeframe) com as métricas da execução mais recente, a origem e a **situação**: `aprovado`,
  `reprovado`, `promissor` (lucro, 30 ou mais operações, ainda sem bateria ou validação), `poucas operações`,
  `negativo`; mais `sem teste` e `sem base` para o que falta. `matrix.positives(m, robô)` dá a visão por robô.
- **Critérios de aceite:** as três regras acima cobertas por teste; execuções auxiliares (bateria completa,
  paridade) não entram.

### T11.7 — Painel visual ✅

**Registrado na entrega:** `robots/panel.py` e `mt5x panel [--out arquivo.html] [--open]` (padrão:
`exports/painel_resultados.html`). Um arquivo só, com os dados embutidos, sem servidor e sem nada baixado da
internet (um teste confere que não há nenhum endereço externo). Visão **por ativo**: matriz ativo × timeframe do
robô escolhido, com o número em destaque à escolha (resultado, profit factor, operações ou queda máxima). Visão
**por robô**: um quadro de todos os robôs com quantas combinações há em cada situação e, abaixo, onde o robô
escolhido deu positivo, com os aprovados primeiro. Clicar em uma célula mostra o detalhe (período, origem do número,
veredito). Tema claro e escuro; em tela estreita a matriz rola dentro do cartão. Cada situação tem ícone e texto,
nunca só cor. A página guarda a visão, o robô e o número no endereço (`#robot/mql5:meurobo/return_pct`).

**Conferido com dados reais (2026-10-12, host):** 3 ativos, 4 timeframes, 8 robôs e 84 combinações (72 de
estratégias Python e 12 de robôs MQL5 com ticks reais), aberto em navegador sem interface nas duas visões, nos dois
temas e em largura de celular. Imagens: [por ativo](img/painel_por_ativo.png) e [por robô](img/painel_por_robo.png).

**Achado da conferência, corrigido na matriz:** as estratégias Python em GBPUSD e USDJPY apareciam com profit factor
0,00 porque esses ativos não tinham a especificação de contrato gravada (`mt5x specs`); o backtester usa valores
genéricos e o resultado em dinheiro não vale. A matriz ganhou a situação **não confiável** para esse caso (48 das 72
células Python do teste), que não entra na visão por robô. Outro defeito achado pelo teste: o texto embutido usava um
escape que o `JSON.parse` recusaria se um nome de robô tivesse `<!--`; passou a usar `\u003c`.

**Limites:** o painel é uma fotografia: gere de novo depois de novos testes. Não roda testes nem filtra por período.
A aparência foi vista em capturas de um navegador sem interface, não em uso com mouse e teclado.

- **Objetivo:** o painel que a pessoa abre e entende de relance.
- **Arquivos:** `robots/panel.py` (novo), `cli.py`, `tests/test_robots_panel.py` (novo), `docs/robos.md`.
- **Passos:** `mt5x panel [--out arquivo.html] [--open]` gera **um arquivo HTML só**, com os dados embutidos, sem
  servidor e sem nada baixado da internet. Visão por ativo (matriz ativo × timeframe do robô escolhido), visão por
  robô (onde deu positivo, do melhor para o pior), cor pela situação, tema claro e escuro, legenda fixa com o
  significado de cada cor e o aviso sobre custo. Nenhuma célula usa só cor: sempre texto ou ícone junto.
- **Critérios de aceite:** aberto no navegador com dados reais do host, mostra as duas visões e a imagem fica
  registrada na entrega; nada de senha, conta ou caminho pessoal no arquivo.
- **Fora desta tarefa:** rodar testes a partir do painel (ele é só leitura) e a aba na janela do programa.

### Painel vivo (T11.8 a T11.10)

Pedido do dono do projeto em 2026-10-12: o painel não achado pela interface precisa de um botão de lançamento, e
deve poder executar ações do projeto e se atualizar sozinho, em vez de ser só uma fotografia.

Um arquivo HTML aberto do disco não executa nada no computador. Para ter botões é preciso um servidor local do
próprio projeto. **Regras de segurança, obrigatórias nas duas tarefas do servidor:**

1. Escuta só em `127.0.0.1`; recusa qualquer pedido cujo cabeçalho `Host` não seja `127.0.0.1` ou `localhost`.
2. Uma chave aleatória por sessão; sem ela (no cabeçalho do pedido) nenhuma ação e nenhum dado saem. Impede que
   outra página aberta no navegador dispare ações.
3. Lista fechada de ações, cada uma com parâmetros conferidos contra o que existe (robô do catálogo, ativo da base,
   timeframe conhecido). Nada de comando livre, caminho de arquivo ou texto que vá para um interpretador.
4. Um trabalho por vez, com progresso e resultado visíveis; o que fecha o MT5 pede confirmação na página.
5. **Fora do painel, por decisão registrada:** iniciar robô em conta (o início em demo ainda não foi validado com o
   MT5 real), liberar o PARAR TUDO, editar configuração ou código. Acionar o PARAR TUDO entra, porque é a ação segura.
6. `mt5x audit` confere se o painel está exposto à rede, como faz com a ponte.

### T11.8 — Botão na interface ✅

**Registrado na entrega:** botão **Painel de resultados** na barra da aba Robôs e o item "Painel de resultados" nos
menus Exibir e Estratégias (`UIManager.open_results_panel`). Gera `exports/painel_resultados.html` em segundo plano
com o que há na base e abre no navegador. Conferido em teste com a janela real e em uma captura da aba Robôs.
Quando a T11.9 entrar, o mesmo botão passa a abrir o painel vivo.

### T11.9 — Painel vivo: servidor, atualização e ações sem MT5 ✅

**Registrado na entrega:** `robots/panel_server.py` (só biblioteca padrão), `mt5x panel --serve [--open] [--port N]`
e o botão **Painel de resultados** da interface, que agora sobe o painel vivo dentro do próprio programa (e cai para
o arquivo se a porta não abrir). A página consulta o servidor a cada 2 s (trabalho em curso) e 10 s (matriz) e se
redesenha quando a versão dos dados muda. Botões: **Testar o que falta** (estratégia Python escolhida, em cada ativo
e timeframe com base ainda sem teste), **Montar timeframes** (os maiores, a partir do menor que há na base),
**PARAR TUDO** (com confirmação na própria página) e, no detalhe da célula, **Rodar a bateria neste ativo** e
**Testar de novo**. O progresso e o resultado aparecem na faixa do painel e em um aviso do sistema.

As seis regras de segurança têm teste: pedido sem chave ou com chave errada (403), `Host` de fora ou de outra porta
(403), `Origin` de outra página (403), chave no endereço não vale em `POST`, ação fora da lista e parâmetro que não
existe (recusados com o motivo), segundo trabalho enquanto um roda (409). A lista de ações é `backtest`, `resample`,
`battery` e `stop_all`; o teste trava essa lista. `mt5x audit` ganhou o item P7 (painel só na própria máquina e
com chave).

**Conferido no host (2026-10-12):** `mt5x panel --serve` escutando só em `127.0.0.1`; sem chave 403, com chave 200,
com `Host` de outra máquina 403; uma bateria disparada pela API apareceu na página viva (captura em navegador sem
interface). **Não conferido:** os cliques dos botões em um navegador de verdade (a ação foi disparada pela mesma
chamada que o botão faz, não pelo clique).

**Limites:** o backtest de robô MQL5 pelo painel fica para a T11.10 (ele fecha o MetaTrader 5); o painel vivo só
existe enquanto o `mt5x panel --serve` ou a janela do programa estiverem abertos; a chave muda a cada vez.

- **Arquivos:** `robots/panel_server.py` (novo, só biblioteca padrão), `robots/panel.py`, `cli.py`, `ui_manager.py`,
  `audit.py`, `tests/test_panel_server.py` (novo).
- **Passos:** `mt5x panel --serve [--port N]` sobe o servidor em `127.0.0.1`, abre o navegador já com a chave e fica
  até Ctrl+C. Rotas: a página; `GET /api/matrix` (a matriz atual); `GET /api/job` (trabalho em curso: nome,
  progresso, mensagens, resultado); `POST /api/action` com uma ação da lista. Ações desta tarefa: montar timeframes
  (`resample`), backtest de estratégia Python (célula, linha ou lote), bateria no ativo da célula e PARAR TUDO. A
  página consulta o servidor a cada poucos segundos e redesenha quando a matriz muda; sem servidor (arquivo aberto
  do disco) os botões não aparecem e ela continua sendo a fotografia de sempre.
- **Critérios de aceite:** as seis regras acima, cada uma com teste (pedido sem chave, com `Host` de fora, ação fora
  da lista, parâmetro inexistente, segundo trabalho enquanto um roda); no host, um backtest disparado pelo painel
  aparece na matriz sem recarregar a página.

### T11.10 — Painel vivo: ações que usam o MT5 ✅

**Registrado na entrega:** três ações novas na lista (agora `backtest`, `resample`, `battery`, `stop_all`, `update` e
`specs`; um teste trava a lista e confere que não há nada de iniciar, liberar ou configurar):

- **Atualizar a base** (`update`): busca as barras novas das séries **extraídas** e refaz as **montadas** delas
  (T11.1) com as barras novas.
- **Gravar especificações** (`specs`): grava o contrato dos ativos, o que tira as estratégias Python da situação
  "não confiável".
- **Backtest de robô MQL5** (`backtest`): se o MetaTrader 5 desta instalação estiver aberto, o servidor responde
  428 e **nada roda** até a página repetir o pedido com a confirmação; aí fecha o terminal, testa e religa a ponte.
  Uma estratégia Python nunca pede confirmação nem toca no MT5.

O acesso ao MT5 chega ao painel por uma função dada por quem o abre (`mt5x panel --serve` ou a janela do programa);
sem ela, as ações que precisam do MT5 falham com a explicação.

**Conferido no host (2026-10-12), com o MT5 oculto, disparando cada ação pela mesma chamada que o botão faz:**
especificações de EURUSD, GBPUSD e USDJPY gravadas; base atualizada (3 séries M15) e 9 séries montadas refeitas; teste
MQL5 com o terminal aberto recusado sem a confirmação e executado com ela (45 operações em EURUSD H4, ponte religada);
as 72 combinações Python testadas de novo, e as células "não confiável" caíram de 48 para 0. Durante tudo, 0 janelas
do MT5 na tela real. Imagem: [painel vivo](img/painel_vivo.png).
**Não conferido:** os cliques em um navegador de verdade, e o Windows.

- **Passos:** atualizar a base (`update`) e gravar a especificação de contrato (`specs`), que abrem o terminal
  (oculto, se o modo estiver ligado); backtest de robô MQL5 (célula, linha ou lote), que fecha o terminal desta
  instalação e religa a ponte, sempre com confirmação na página.
- **Critérios de aceite:** no host, com o modo oculto, uma atualização e um teste MQL5 disparados pelo painel; a
  página mostra o progresso e o aviso do sistema chega no fim.

---

## Fase 12 — HADOUKEN 2.0: carteira, Gradiente Linear, painel e estatísticas

Especificação do dono do projeto (2026-10-13), registrada em `Documentation/HADOUKEN_2/PLANO.md` do repositório
HADOUKEN (`projeto-indicadores-mt5`); o andamento real de cada etapa, com o que não foi conferido, está em
`Documentation/HADOUKEN_2/ANDAMENTO.md` de lá. Aqui ficam o painel de status e a parte do mt5x.

### Ponto de retomada (2026-10-13, 08:00, host Linux)

Quem continuar começa aqui. Tudo o que foi feito está em commit, enviado e mesclado; nada ficou só na máquina.

**Onde está cada coisa**

- mt5x (este repositório): tudo no `master`.
- HADOUKEN (`~/Projects/projeto-indicadores-mt5`): veja `Documentation/HADOUKEN_2/ANDAMENTO.md` (estado das etapas
  e lista de pendências) e `TESTES_PAINEL_V7.md` (bateria do painel, correções e a organização da janela).
- `mt5x-safety` (`~/Projects/mt5x-safety`): 1.1.0, igual à `Safety/` 7.1 do HADOUKEN.

**Estado do host**

- `config/config.ini` (não versionado) tem `[ROBOTS] safety_lib_dir` e `library_dirs` apontando para os dois clones.
- Conta do terminal: MetaQuotes-Demo, hedging, saldo de 99.991,90 depois de três compras de teste pelo painel.
  Base com EURUSD em M1, M15, H1 e H4, ticks de 23 dias e a calibração de spread gravada.
- **Nenhum robô está rodando na demo.** O PARAR TUDO está liberado.
- O terminal ficou com a janela organizada (janelas laterais e barras escondidas) e no perfil de gráficos
  "HADOUKEN". O arranjo anterior está em `Config\terminal.ini.antes-do-mt5x`; `[MT5] manage_layout = false` (ou o
  botão TELA do painel) o devolve na próxima abertura pelo programa. O perfil Default tem os quatro gráficos originais.
- Scripts de rascunho e capturas de tela (fora dos repositórios): `~/Projects/mt5x-rascunhos-2026-10-13/`. O
  `t87_start.py` inicia o `HDK_EmaAtr` na demo **dispensando a trava da bateria**; só o dono do projeto o roda.

**Resolvido na rodada de correções de 2026-10-13**

- `mt5x-safety` 1.1.0 publicada, com a correção do bloqueio falso por drawdown; o SDK e o exemplo passaram a usá-la.
- Paridade dos seis robôs repetida com a camada nova: o lado MQL5 não mudou (seção T8.9).
- A organização da janela fecha o gráfico auxiliar (fica um só gráfico, maximizado) e a devolução do arranjo
  anterior foi vista no terminal.
- O `mt5x robots start` confere no log do terminal que o robô foi carregado e, se não foi, não o grava como iniciado.

**Pendências abertas, por ordem de importância**

1. **T8.7, o que falta:** uma operação de verdade de um robô automático em demo (ordem, aviso e `monitor` com
   posição aberta). Depende de sinal de mercado e de um robô aprovado, ou do script que dispensa a trava. A
   conferência do log pelo `start` tem teste automático, mas não foi exercitada pelo comando real, porque ele exige
   bateria aprovada. Recompilar um robô não recarrega o que já está no gráfico (é preciso reabrir o terminal).
2. **Backtester Python sem limite de margem:** resultados abaixo de −100% e entradas que a camada de segurança
   bloquearia (ORB). Sem tarefa ainda; precisa da margem por lote, que a ponte hoje não fornece.
3. **Bloqueio falso por drawdown:** corrigido no MQL5, sem teste automático (não há como rodar MQL5 no CI).
4. **Organização da janela:** identificadores lidos só do build 6258; o clique no botão TELA não foi feito no
   terminal (o arquivo de "desligado" foi criado à mão); Windows não visto.
5. **T12.2:** comparar o risco calculado no MQL5 com o do Python em mais de uma posição e em conta netting.
6. **T12.3 em diante:** guardião com orçamento por robô (dá uso às operações supervisionadas), ordens pendentes no
   executor, Gradiente Linear, abas novas do painel, estatísticas e copiloto.
7. **Não são de código:** nenhum robô passa na bateria (as seis estratégias reprovam em EURUSD); não há conta
   netting nem ativos da B3 no host; T10.W (Windows), T10.9 e T10.11 abertas; o PR #1 deste repositório está aberto
   com os commits já no `master` e pode ser fechado.

**Regras da fase:** o mt5x continua sem enviar ordens; no MQL5 só o `HDK_SafeExecutor` envia, altera ou cancela;
ver a conta não dá direito de mexer nela; a paridade dos seis robôs de estratégia não pode mudar; conta real nunca
é o padrão. Cada etapa tem o seu critério de aceite no plano de lá e só é marcada ✅ com ele cumprido.

### T12.0 — Plano e diagnóstico ✅

Plano registrado e código atual levantado (seção "Etapa 0" do andamento). Achados que mudam o caminho: o executor
não cria ordem pendente (a boleta e a grade dependem disso); não existe orçamento de risco global; a `mt5x-safety`
está desatualizada em relação à `Safety/`; não há conta netting no host, então nada de netting (B3) se prova aqui.

### T12.1 — Safety ampliada ⏳

No HADOUKEN: `ENUM_HDK_PERMISSION` e, no `HDK_SafeExecutor`, `SetMandate`, `Adopt`, `ScopeFor`, `SupervisedClose`,
`SupervisedCancel` e `SupervisedCloseAll`, todas auditadas em `MQL5\Files\HDK_Audit`. As funções antigas não
mudaram: seguem restritas ao próprio magic. **Compila** (painel, sentinela e seis robôs de estratégia). **Falta:**
executar uma operação supervisionada, que depende de o guardião da T12.3 chamar essas funções. Os outros itens foram
conferidos depois: a paridade dos seis robôs foi refeita com a `Safety/` 7.1 (T8.9) e, na validação posterior do
D-02/T12.4, as duas camadas produziram as mesmas 690 operações. A `mt5x-safety` 1.3.0 contém os nove arquivos do
manifesto; em 2026-10-14 eles foram comparados byte a byte com a pasta `Safety/` do HADOUKEN. A regra da bateria já
trata `Safety/` como camada permitida e tem testes para aceitar envio dentro dela e reprovar envio direto no robô;
nenhum ajuste adicional foi necessário. T12.1 segue parcial até haver um caminho exercitável para a operação
supervisionada; não foi feita operação em conta.

Atualização da frente F-03 (mesmo dia): `mt5x-safety` 1.4.0 inclui a consolidação de exposição. Em 2026-10-14 rodei a paridade T10.6 dos seis robôs em EURUSD no período
2025-10-01 a 2026-09-30 (Strategy Tester, cada tick). Nenhum cumpriu todos os limites existentes; essa comparação
mede Python × MQL5 e não é uma aprovação operacional:

| Robô | Python × MQL5 | Casadas | Pares até 5% | Diferença do giro | Mediana MQL5/Python |
|---|---:|---:|---:|---:|---:|
| EmaAtr H1 | 58 × 58 | 96,6% | 91,1% | 1,5% | 0,997 |
| Donchian H4 | 32 × 31 | 96,9% | 90,3% | 3,4% | 0,994 |
| BbRsi H1 | 167 × 164 | 96,4% | 77,6% | 2,6% | 0,978 |
| Squeeze H1 | 149 × 149 | 100,0% | 85,2% | 2,5% | 0,986 |
| Pullback H1 | 58 × 55 | 89,7% | 94,2% | 1,2% | 0,990 |
| Orb M15 | 257 × 239 | 91,8% | 36,0% | 1,2% | 0,941 |

**Pendente:** exercitar uma operação supervisionada depois da T12.3, apenas em demo. Nenhuma operação supervisionada
foi executada.

**Divergência de dependências registrada:** a validação restante de T12.1 exige T12.3, mas a tabela faz T12.2
depender de T12.1 e T12.3 depender de T12.2. A sequência forma um ciclo. É preciso esclarecer se T12.2 pode avançar
com a implementação da Safety enquanto a operação supervisionada fica para T12.3, ou se a ordem deve ser revista.
Até essa decisão, T12.1 permanece parcial e T12.2 não é iniciada.

*Harmonização de 2026-10-14:* na frente F-03 a T12.3 passou a constar como "em implementação autorizada", o que
indica a escolha de avançar com a T12.3 antes de fechar a T12.1. A ordem formal (T12.1 → T12.2 → T12.3) ainda
precisa da confirmação do dono do projeto; nada foi reordenado nesta harmonização.

### T12.2 — Carteira somente leitura ⏳

- HADOUKEN: `HDK_PortfolioRegistry.mqh` e o robô `HDK_AccountGuardian.mq5`, que só observa, avisa e grava
  `Common\Files\HDK_portfolio_<conta>.json`.
- mt5x: `robots/portfolio.py` e `mt5x portfolio` (pela ponte) ou `mt5x portfolio --guardian` (pelo arquivo do
  sentinela). Mostra posições e pendentes de todos os robôs e manuais, o dono de cada uma (pelo magic number do
  catálogo), o risco até o stop, os totais e os ativos com mais de um dono. Chave nova:
  `[ROBOTS] max_account_risk_pct` (0 = não compara; só muda o estado mostrado).
- **Conferido:** seis testes automáticos; leitura real da conta demo, vazia (estado NORMAL); o sentinela carregado
  no terminal real (`expert HDK_AccountGuardian (EURUSD,M5) loaded successfully`), gravando o retrato, lido por
  `mt5x portfolio --guardian` com os mesmos números da leitura pela ponte.
- **Falta para o aceite:** conciliar com o terminal uma conta **com posições, inclusive manuais** (a conta estava
  vazia, então nenhum cálculo de risco foi comparado); ver o quadro desenhado e a trava de "um sentinela por
  terminal"; comparar o risco calculado no MQL5 (`OrderCalcProfit`) com o do Python (valor do tick); conta netting.

### T12.L — Organização da janela do MT5 ✅

Pedido do dono do projeto (2026-10-13): ligada por padrão e fácil de desligar, deixar o MT5 só com o gráfico do que
está sendo operado. O MQL5 não fecha janelas do terminal e o projeto não usa DLL, então quem organiza é o mt5x, com
o terminal fechado, logo antes de abri-lo (`live.organize_window`, chamado pelo `start`):

- `robots/layout.py`: `apply` põe `IsVisible=0` nas janelas laterais e nas barras de ferramentas em
  `Config\terminal.ini` e guarda o arquivo original uma vez; `restore` o devolve; `prepare_profile` monta o perfil
  de gráficos "HADOUKEN" com um gráfico auxiliar maximizado, para o gráfico do robô também nascer maximizado.
- Liga e desliga: `[MT5] manage_layout` (padrão `true`) ou o botão TELA do painel do HADOUKEN, que grava
  `Common\Files\HDK_LAYOUT_OFF.txt`. Desligada, o arranjo anterior é devolvido na abertura seguinte.
- **`close_terminal` agora pede para o terminal fechar** (`taskkill` sem `/F`) antes de encerrar à força. Fechado à
  força ele não gravava a configuração nem o perfil; era a causa de o robô anterior sumir do gráfico (T8.7).
- Falha ao organizar nunca impede o início do robô.

**Conferido no host**, em tela virtual de 1280×800: terminal sem as quatro janelas e sem as barras, um só gráfico,
maximizado (o painel fecha o gráfico auxiliar); com a organização desligada, a abertura seguinte devolveu o arranjo
anterior. Sete testes automáticos (`tests/test_robots_layout.py`).
**Não conferido:** o caminho do `mt5x robots start` de ponta a ponta com um robô aprovado (o teste chamou as mesmas
funções por um script); o clique no botão TELA; outro build do MT5; Windows.

### T12.P — Painel responsivo e bateria de testes ✅

Bateria nos dois painéis, com os relatórios em `docs/bateria_painel_resultados.md` (navegador, quatro correções) e,
no repositório HADOUKEN, `Documentation/HADOUKEN_2/TESTES_PAINEL_V7.md` (catorze problemas, onze corrigidos). O
painel do MT5 passou a escolher sozinho entre as disposições compacta, normal e ampla e a ajustar o tamanho para
caber. **Defeito grave achado e corrigido lá:** o `HDK_RiskGuard` travava o robô por drawdown quando o terminal
abria com a conta ainda zerada.

### Revisão das etapas do painel e da organização da janela (2026-10-13)

Pedido do dono do projeto: rever as etapas do painel e a questão da organização da janela do MT5, com a
possibilidade de ajustá-la pelas opções da interface. **Só revisão e plano: nenhum código mudou.** Foram lidos
`robots/layout.py`, `robots/live.py`, o diálogo Configurações (`ui_manager.open_settings`), `config_builder.py` e,
no HADOUKEN, `PLANO.md`, `ANDAMENTO.md`, `TESTES_PAINEL_V7.md` e os parâmetros do `HDK_HADOUKEN_Painel.mq5`. Os
documentos do HADOUKEN **não foram alterados**; o que muda lá está indicado em cada item.

**Achados sobre a organização da janela (T12.L)**

1. **É tudo ou nada.** `layout.apply` esconde as quatro janelas laterais e as três barras juntas, e a única chave é
   `[MT5] manage_layout`. Não dá para manter a Observação do Mercado e fechar o resto, por exemplo.
2. **Os identificadores estão em pares.** `32843` e `32845` aparecem como "Observação do Mercado / Navegador" e
   `32841` e `32846` como "Caixa de Ferramentas / Testador": não se sabe qual número é qual janela. Escolher janela
   por janela exige descobrir isso no terminal antes.
3. **Devolver o arranjo anterior regrava o `terminal.ini` inteiro** com a cópia guardada na primeira organização.
   Tudo o que o terminal gravou nesse arquivo depois disso (não só a visibilidade das janelas) volta ao valor
   antigo. O conteúdo completo desse arquivo não foi inventariado, então o tamanho do risco não é conhecido.
4. **Só vale quando o programa abre o terminal por `mt5x robots start`.** Abrir o MT5 pela ponte, pela interface
   ou à mão não organiza nada, e o botão TELA do painel não tem efeito na hora: só grava a escolha.
5. **Há três interruptores para a mesma coisa:** a chave do `config.ini`, o parâmetro `InpManageLayout` do painel e
   o arquivo `HDK_LAYOUT_OFF.txt` (botão TELA). Hoje qualquer "desligado" vence. A regra não está escrita em lugar
   nenhum para o usuário.
6. **O diálogo Configurações da interface só mostra.** Ele exibe a conta, os caminhos e manda editar o arquivo e
   reiniciar. Não existe hoje nenhuma opção editável pela interface: a do layout seria a primeira.
7. **Gravar o `config.ini` pelo programa apaga os comentários.** `config_builder.write_config` regrava o arquivo
   pelo `configparser`. Quem partiu do `config.ini.example` (comentado) perderia as explicações na primeira
   alteração feita pela interface.
8. **Achado 12 da bateria do painel continua marcado como aberto** ("cada início acrescenta um gráfico ao
   perfil"). O perfil próprio "HADOUKEN" apaga os gráficos anteriores a cada início, o que deveria resolver, mas
   isso não foi conferido de novo depois da mudança.
9. **Seguem sem conferência:** o clique no botão TELA, outro build do MT5 e o Windows (já registrados na T12.L).

**Achados sobre as etapas do painel (T12.P e T12.6)**

1. **A etapa 6 foi feita pela metade e fora de ordem.** As três disposições (compacta, normal, ampla) entraram
   antes das etapas 3 a 5. Do que falta, uma parte não depende de nada (botão recolhido, quebra de linha no log)
   e outra depende de etapas que não começaram: a aba Boleta e a aba Gradiente precisam das ordens
   pendentes (T12.4), a aba Estatísticas completa é a T12.6A, a aba IA é a T12.7. Hoje o painel de status põe tudo
   sob "T12.6, depende de T12.4", o que trava também o que poderia andar.
2. **Vocabulário fixado na T12.6a.** "Disposição do painel" nomeia compacta, normal e ampla (`InpPanelLayout`);
   "estado do painel" nomeia expandido e recolhido (o botão `—` do v7 recolhe o painel em uma barra); "organização
   da janela do MT5" nomeia o arranjo do terminal aplicado pelo mt5x. "Compacta" não é um estado. A enumeração
   ativa é `ENUM_HDK_PANEL_LAYOUT`; `ENUM_HDK_LAYOUT_MODE` aparece somente no modelo genérico de `HDK_Config.mqh`,
   sem referência no painel v7.
3. **A lista de abas do plano tem 11 e a Fase 13 acrescenta uma** ("Inteligência do Ativo", T13.6). O painel tem
   quatro hoje (Visão geral, Posições, Estatísticas, Log). Não há decisão sobre como 12 abas cabem na disposição
   compacta, que mostra quatro linhas por aba.
4. **Construir em cima do que não foi visto.** A disposição normal, o parâmetro que a força, a paleta clara, o
   ajuste abaixo de 100% e uma tela física foram conferidos na T12.6a. Continuam pendentes Windows, testes de
   negociação em conta apropriada e duas instâncias ao mesmo tempo.
5. **Linha de base registrada em escopo menor.** A T12.6a mediu um gráfico com e sem painel no mesmo terminal;
   a CPU variou entre janelas e não permite atribuir a diferença ao painel. A amostra histórica de oito gráficos
   e painel (7,8% de um núcleo e 377 MB) segue sem par equivalente.

**Etapas revistas** (substituem a linha única "T12.6" como guia de ordem; os critérios de aceite da etapa 6 do
plano do HADOUKEN continuam valendo para o conjunto)

| Passo | O que é | Depende de |
|---|---|---|
| T12.L1 | Organização da janela configurável por partes, devolução sem regravar o arquivo inteiro | T12.L |
| T12.L2 | Opções da organização da janela na interface (diálogo Configurações) | T12.L1 |
| T12.6a | Fechar o que ficou sem conferência no painel v7 e fixar o vocabulário ✅ 2026-10-14 | T12.P |
| T12.6b | Estado recolhido (botão `H`), quebra de linha do log e a aba Configuração | T12.6a, G-02 (arquitetura modular do painel) |
| T12.6c | Abas que dependem de outras etapas: Boleta e Gradiente (T12.4), Carteira (T12.2), Risco e Alertas (T12.3), Estatísticas (T12.6A), IA (T12.7), Inteligência do Ativo (T13.6) | cada aba, a sua |

A T12.L1–T12.L3 foram concluídas; a T12.6a também. As próximas tarefas devem seguir a tabela de dependências acima.

### T12.L1 — Organização da janela configurável por partes ✅

**Registrado na entrega (2026-10-13):**

- Chaves novas em `[MT5]`, todas `true` por padrão: `layout_hide_navigation` (Observação do Mercado e Navegador),
  `layout_hide_toolbox` (Caixa de Ferramentas e Testador), `layout_hide_toolbars` e `layout_own_profile`.
  `manage_layout` segue como interruptor geral. Valor inválido vale o padrão, com aviso no log.
- `layout.apply(data, toolbars, navigation, toolbox)`; uma parte desligada **depois** de já ter sido escondida volta
  como o usuário tinha (pela cópia guardada). `layout.options`, `layout.status`, `layout.has_arrangement`,
  `layout.panel_off` e `layout.set_panel_off`.
- **Devolução seletiva:** `layout.restore` repõe só o `IsVisible` das janelas e barras que o módulo conhece, com o
  valor da cópia guardada, e deixa o resto do `terminal.ini` como está. O arquivo real do host tem dezenas de
  outras seções (`Settings`, `Options`, `Common`...), que antes voltavam ao valor antigo junto. A cópia
  `terminal.ini.antes-do-mt5x` continua sendo criada uma vez e apagada na devolução.
- `mt5x mt5 layout` (estado, com `--json`) e `mt5x mt5 restore-layout` (recusa com o terminal aberto).
- Sem o perfil próprio (`layout_own_profile = false`) o gráfico do robô abre no perfil em que o terminal estava, e
  cada início acrescenta um gráfico a ele (é o achado 12 da bateria do painel; com o perfil próprio há teste de que
  fica um gráfico só).
- **Decisões tomadas sem resposta do dono do projeto** (podem ser revistas): por grupo, e não janela por janela
  (decisão 1); só ao iniciar um robô, como já era (decisão 2).
- **Conferido:** os sete testes anteriores passam sem alteração; nove testes novos em
  `tests/test_robots_layout.py`; `mt5x mt5 layout` rodado no host, lendo o `terminal.ini` real (só leitura).
- **Não feito / não conferido:** o passo 1 (descobrir no terminal qual identificador é qual janela) **não foi
  feito**, porque exige fechar e reabrir o MT5 do host, e por isso as partes são os dois pares, não as quatro
  janelas; nenhuma organização nova foi aplicada ao terminal real nem vista na tela; outro build e Windows.

**Texto original da tarefa:**

- **Objetivo:** escolher o que a organização faz, e devolver o arranjo anterior sem desfazer outras configurações
  do terminal.
- **Depende de:** T12.L.
- **Arquivos previstos:** `robots/layout.py`, `robots/live.py`, `bootstrap/config_builder.py`,
  `config/config.ini.example`, `cli.py`, `tests/test_robots_layout.py`.
- **Passos:**
  1. No terminal real, fechar uma janela por vez e comparar o `terminal.ini`, para saber qual identificador é a
     Observação do Mercado, o Navegador, a Caixa de Ferramentas e o Testador. Registrar o build.
  2. Chaves novas em `[MT5]`, todas com `fallback` igual ao comportamento de hoje (a chave `manage_layout`
     continua sendo o interruptor geral): `layout_hide_side_panes`, `layout_hide_toolbars` e `layout_own_profile`
     (perfil "HADOUKEN" com o gráfico maximizado). Se o passo 1 separar as janelas, uma lista do que esconder em vez
     de uma chave só para as laterais (decisão 1).
  3. `layout.apply` passa a receber o que esconder; `organize_window` lê as chaves.
  4. Devolução seletiva: guardar **quais seções foram alteradas e o valor anterior**, e na devolução repor só
     isso. A cópia `terminal.ini.antes-do-mt5x` de quem já usa continua sendo aceita (base antiga não quebra).
  5. `mt5x mt5` ganha o estado da organização (ligada ou desligada, por quem, o que será escondido, se há arranjo
     guardado) e a devolução sob comando, com o terminal fechado.
  6. Conferir de novo o achado 12 da bateria do painel (gráficos acumulando no perfil) e fechá-lo ou reabri-lo.
- **Critérios de aceite:** com as chaves ausentes, o resultado é idêntico ao de hoje (os sete testes atuais passam
  sem mudança); cada opção desligada deixa a sua parte intacta; depois de organizar, mudar outra configuração do
  terminal e desligar, a outra configuração **permanece**; conferido no terminal real do host em 1280×800.
- **Não conferível no host:** Windows e outro build (vão para o checklist de `validacao_instaladores.md`).

### T12.L2 — Opções da organização da janela na interface ✅

**Registrado na entrega (2026-10-13):**

- `mt5_extracao/ui/settings.py` (novo, `SettingsDialog`): Arquivo > Configurações passou de um texto fixo para dois
  blocos. "Em uso" (conta, terminal, banco, arquivo) e "Janela do MetaTrader 5": a caixa geral, uma caixa por parte
  (desabilitadas com a geral desligada), o estado (ligada, desligada, desligada pelo botão TELA, ligada sem
  nenhuma parte), o que está escondido agora e os botões "Voltar a ligar" (desfaz o desligamento do botão TELA) e
  "Devolver o arranjo anterior agora" (só com o terminal fechado).
- Cada clique grava na hora, com a confirmação na própria janela; se a gravação falha, a caixa volta e a mensagem
  diz o que conferir. A configuração em memória muda junto: não precisa reiniciar o aplicativo.
- `config_builder.set_config_value(path, seção, chave, valor)`: troca só a linha da chave (ou a acrescenta ao fim
  da seção, ou a seção ao fim do arquivo), com escrita atômica. Comentários, ordem e grafia da chave ficam. Serve
  para qualquer opção futura da interface.
- **Decisões tomadas sem resposta do dono do projeto:** a disposição do painel não entrou nesta tarefa (entrou na
  T12.L3, depois da resposta); o botão TELA continua e qualquer "desligado" vence, com a interface mostrando quem desligou (decisão 4).
- **Conferido:** seis testes novos da janela (clique real nas caixas, arquivo com comentários preservado, reabrir
  com o que foi salvo, devolução recusada com o terminal aberto, botão TELA, pasta do MT5 ausente e falha de
  gravação) e três do `set_config_value`; diálogo visto em tela virtual nos temas claro e escuro.
- **Não conferido:** o diálogo na tela real e no Windows; um início de robô de verdade obedecendo às opções (o
  teste chama `live.organize_window`, que é o que o início usa).

**Texto original da tarefa:**

- **Objetivo:** ligar, desligar e ajustar a organização da janela pelo diálogo Configurações, sem editar arquivo.
- **Depende de:** T12.L1.
- **Arquivos previstos:** `ui_manager.py` (ou um painel novo em `mt5_extracao/ui/`), `bootstrap/config_builder.py`
  (gravação de uma chave preservando comentários), `tests/test_ui_main_window.py`, `tests/test_bootstrap_config.py`.
- **Passos:**
  1. Gravar uma chave no `config.ini` **sem regravar o arquivo**: trocar só a linha da chave (ou acrescentá-la na
     seção), mantendo comentários e ordem. Escrita atômica, como as demais.
  2. No diálogo Configurações, o bloco "Janela do MT5": caixa geral "Organizar a janela ao iniciar um robô" e,
     abaixo, uma caixa por opção da T12.L1, desabilitadas quando a geral está desligada.
  3. Mostrar o estado real: se o botão TELA do painel desligou a organização (arquivo `HDK_LAYOUT_OFF.txt`), dizer
     isso e oferecer "voltar a ligar" (apaga o arquivo); se há arranjo anterior guardado, o botão "Devolver o
     arranjo anterior agora", habilitado só com o terminal fechado.
  4. Texto fixo no bloco: a mudança vale na próxima vez que o programa abrir o MT5 para um robô; com o MT5 já
     aberto nada muda na hora.
  5. Leitura do estado e gravação por `run_bg` (regra da Fase 7); o `config` em memória é atualizado junto, para
     não exigir reinício do aplicativo.
- **Critérios de aceite:** marcar e desmarcar muda o `config.ini`, com os comentários do arquivo preservados; o
  início seguinte de um robô obedece; o estado do botão TELA aparece certo; testes com a pasta do MT5 simulada (sem
  MT5 real); mensagens em português com a ação de correção.
- **Armadilhas:** é a primeira opção editável da interface, então a gravação da chave deve servir para as próximas
  (não escrever algo específico do layout); não gravar em `config.ini` nada que seja segredo.

### T12.L3 — "Abrir visão MT5" e disposição do painel nas opções ✅

Respostas do dono do projeto às decisões da revisão (2026-10-13): **1** por grupo, está bom; **2** não só ao iniciar
um robô: poder mudar a opção e aplicar a visão quando quiser; **3** incluir a disposição do painel nas opções e pôr
na interface um botão bem visível de "abrir visão MT5", que respeite as opções escolhidas.

**Registrado na entrega:**

- `robots/view.py` (novo), `open_view(config, close_terminal)`: aplica a organização escolhida e abre o terminal.
  Com ele aberto, só segue se quem chamou autorizou fechar: para a ponte, pede ao terminal que feche, grava,
  abre de novo e religa a ponte (inclusive se a abertura falhar). Com a organização desligada, devolve o arranjo
  anterior. Não coloca nem tira robô de gráfico e não envia ordem.
- **Perfil de gráficos:** a visão usa o perfil "HADOUKEN" **como está**, se ele já existe (ele nasce no primeiro
  início de robô pelo programa). Refazê-lo apagaria o gráfico em que o robô ficou. Sem ele, o terminal abre no
  perfil em que estava, sem `/config` (`mt5_tools.start_terminal` passou a aceitar isso).
- **Disposição do painel:** `[MT5] panel_layout` = `auto`, `compact`, `normal` ou `wide` (os códigos 0 a 3 de
  `ENUM_HDK_PANEL_LAYOUT`, lidos do código do HADOUKEN). No início de um robô que tem o parâmetro `InpPanelLayout`,
  `live.panel_parameters` grava `MQL5/Presets/mt5x_painel.set` (cópia do preset com esse parâmetro trocado; o preset
  do usuário não muda) e o usa. Em "Abrir visão MT5", `layout.set_panel_input` troca a linha `InpPanelLayout=N` nos
  gráficos do perfil próprio, e só nele. Com `auto` e sem perfil, nada disso muda o comportamento anterior.
- **Interface:** botão "Abrir visão MT5" no cabeçalho, com ícone desenhado pelo programa (`ui/icons.py`, Pillow),
  para não depender das fontes do Tk. Em Configurações: a lista "Disposição do painel do HADOUKEN" e o botão
  "Aplicar e abrir o MT5 agora" (a mesma ação). Com o MT5 aberto, pergunta antes de fechar e avisa do efeito sobre
  robô no gráfico, coleta e extração. O trabalho roda por `run_bg`.
- **Linha de comando:** `mt5x mt5 view [--close-terminal]`; `mt5x mt5 layout` mostra a disposição escolhida.
- **Testes automáticos:** onze em `tests/test_robots_view.py` e quatro novos da janela, com o terminal simulado.
- **Testado no MT5 real do host (2026-10-13, build 6258 no Wine, tela virtual de 1280×800 do modo oculto, conta
  MetaQuotes-Demo, sem robô no gráfico), com uma captura de tela a cada passo:**
  1. `mt5x mt5 view` com o terminal fechado e tudo ligado: abriu sem as janelas laterais e sem as barras.
  2. O mesmo com o terminal aberto: recusou sem `--close-terminal`; com ele, fechou, reabriu e religou a ponte.
  3. Perfil próprio criado pela visão (`--symbol EURUSD`): **um gráfico só, EURUSD M15, maximizado**
     ([captura](img/t12_visao_mt5/visao_organizada_1280x800.png)).
  4. Barras e parte de baixo desligadas em uma cópia do `config.ini`: voltaram a aparecer; a lateral seguiu fechada.
  5. O botão "Abrir visão MT5" clicado na interface de verdade (em outra tela virtual), com a configuração real:
     perguntou antes de fechar, reabriu organizado e liberou o botão em 2,6 s.
  6. `mt5x mt5 restore-layout` com o terminal aberto: recusou. Com a organização desligada, a visão devolveu as
     quatro janelas e as barras; a visibilidade de todas ficou igual à do arquivo guardado antes dos testes.
- **Achado no teste real e corrigido:** sem perfil próprio, o terminal abria no perfil do usuário, que no host tem
  um único gráfico salvo **minimizado** (área toda cinza). A visão passou a criar o perfil próprio com um gráfico
  maximizado do ativo em foco quando esse perfil ainda não tem gráfico (`open_view(symbol=...)`, `--symbol`/`--tf`).
- **Efeito colateral do modo oculto, não desta tarefa:** rodando na tela virtual de 1280×800, o terminal regravou
  no `terminal.ini` o tamanho da janela e das áreas laterais (medidas de uma tela 4K viraram medidas de 1280×800).
  Depois dos testes o `terminal.ini` original, guardado antes, foi devolvido. Vale para qualquer uso do modo
  oculto em máquina com tela maior que a virtual; fica como pendência para a T11.3.
- **Não conferido:**
  - **A disposição do painel com um robô de verdade.** Não há robô aprovado na bateria para iniciar pelo programa
    (o script que dispensa a trava é só do dono do projeto), então nenhum gráfico com o painel existiu nos testes.
  - **A visão na tela real do dono** (2560×1980): os testes foram na tela virtual.
  - **O formato do gráfico salvo com robô** (`<inputs>` com `InpPanelLayout=N` no `.chr`) é o que se conhece do MT5;
    no host não há nenhum gráfico com robô para conferir. Se o formato for outro, a linha não é encontrada e
    nada muda (não quebra), mas a disposição não é atualizada.
  - **Arquivo de parâmetros só com `InpPanelLayout`** (início de robô sem preset e disposição diferente de `auto`):
    não se verificou se o MT5 aceita um `.set` parcial. Se não aceitar, o início acusa que o robô não foi carregado.
  - **Depois de reabrir, este programa fica desconectado** até "Verificar conexão" (a interface avisa). A reconexão
    automática não foi feita.
  - Windows.

### Correção: o programa abria sem janela e o MT5 sem a organização (2026-10-13)

Relato do dono do projeto: erro ao iniciar o MT5; depois, só o MT5 aparecia, sem as customizações, e a janela do
programa não podia ser vista nem usada.

**O que se achou** (logs do programa, do MT5 e do sistema, no host):

1. **A causa do erro era a rede.** O Wi-Fi do host oscilou a partir das 14:56 e ficou fora das 15:01 às 15:26. O
   log do MT5 dizia `no connection to MetaQuotes-Demo`; o programa mostrava seis estratégias de conexão que
   falharam, sem dizer que faltava internet.
2. **O programa conectava ao MT5 antes de montar a janela.** Com a conexão lenta ou falhando, ficava de 30
   segundos a mais de 2 minutos sem janela (medido no log: 15:02:34 a 15:04:46), com o MT5 aberto por cima.
3. **O MT5 aberto pela conexão vinha sem a organização**, que só era aplicada ao iniciar um robô ou pelo botão
   "Abrir visão MT5".
4. **Fechar a janela durante a verificação gerava exceção não tratada**, e o encerramento chamava
   `DatabaseManager.close`, que não existia.

**O que mudou:**

- `app.py` não conecta mais na inicialização: a janela é montada e `UIManager.connect_mt5` conecta em segundo
  plano (`run_bg`), com o estado no cabeçalho e a base já extraída disponível enquanto isso. Um segundo pedido
  durante a conexão é ignorado. "Verificar conexão" usa o mesmo caminho.
- Com o terminal fechado, a conexão o abre por `view.open_view`, **já organizado**. Falha ao organizar não impede
  a conexão. Com o terminal já aberto, nada é fechado nem reorganizado.
- Sem ativo pedido, a visão cria o perfil próprio com o **gráfico que o usuário já tinha** (`layout.user_chart`: o
  primeiro gráfico do perfil em que o terminal estava), maximizado. O botão "Abrir visão MT5" usa o ativo clicado
  na lista e, sem ele, o mesmo gráfico.
- `mt5_extracao/connection_help.py` (novo, sem Tkinter): confere a internet por três endereços e escolhe entre
  "sem internet", "abriu e não entrou na conta" e "não abriu", cada um com a ação de correção.
- Depois de conectar, a janela do programa é trazida para a frente (`bring_to_front`).
- `DatabaseManager.close()` e a verificação protegida contra janela já fechada.

**Conferido no host, na tela real, com o MT5 real:** programa aberto por `./run.sh` com o terminal fechado: a janela
ficou pronta em 2 s (16:06:58 a 16:07:00), o MT5 abriu organizado com EURUSD H1 maximizado e conectou em 14 s; as
duas janelas constam na tela. Catorze testes automáticos novos.
**Defeito achado nesse teste e corrigido:** a primeira versão usava o primeiro item da seleção salva como ativo do
gráfico, e abriu "AAAC". Passou a usar o gráfico que o usuário tinha.
**Não conferido:** se a janela do programa de fato fica por cima do MT5 na tela (não há como capturar a tela real
do host, que é Wayland; conferiu-se só que as duas janelas existem); a mensagem de "sem internet" com a rede
realmente fora (só com a rede simulada); Windows.

### O painel na visão do MT5, só para visualização (2026-10-13)

Relato do dono do projeto depois da correção acima: a interface e o gráfico abriram, mas o painel e os seus modos de
visão não apareciam. A visão abria só o gráfico: o painel é um robô e ninguém o colocava lá.

**O que impedia:** `mt5x robots start` exige a bateria de segurança aprovada, e o painel **nunca passa nela**. Rodada
em 2026-10-13 para `hdk_hadouken_painel`: a leitura do código foi aprovada (stop obrigatório, toda ordem pela camada
de segurança, sem DLL, sem preço médio) e a parte estatística reprovou por ter **zero operações**, porque um painel
de operação manual não opera sozinho no Strategy Tester. A trava foi desenhada para robôs automáticos.

**O que foi feito, sem afrouxar a trava de `mt5x robots start`:**

- `view.open_view` passou a pôr o painel no gráfico pelo arquivo de início (`[MT5] view_panel`, `panel_robot`,
  `panel_preset`), na conta conferida. `live.panel_gates` decide o modo:
  - **bateria inteira aprovada:** entra como qualquer robô, com a negociação ligada;
  - **sem ela:** entra **só para visualização**. Exige o robô compilado, conta de demonstração e a leitura do
    código aprovada para o código que está no disco, e a visão **aciona o PARAR TUDO** (`HDK_KILL.txt`), que a
    camada de segurança do HADOUKEN obedece. O painel aparece bloqueado até a pessoa liberar
    (`mt5x robots stop --release`).
- Se alguma exigência falha, a visão abre só com o gráfico e diz o motivo (`panel_note`).
- A conta conferida em cada conexão é guardada em `config/ui_prefs.json`, para a abertura do programa poder pôr o
  painel com o terminal ainda fechado. Sem conta conferida, o painel não entra.
- A disposição escolhida (`panel_layout`) vai no arquivo de parâmetros do painel.

**Por que o PARAR TUDO, e não a chave `AllowLiveTrading=0`:** a primeira versão usava só essa chave do arquivo de
início. No teste real, depois de o programa conectar, `terminal_info().trade_allowed` estava **ligado**. Não se
conseguiu provar pelo Python que o robô ficou impedido de operar, então a chave continua sendo gravada, mas não é
tratada como garantia.

**Conferido no host, com o MT5 real:** `expert HDK_HADOUKEN_Painel (EURUSD,H1) loaded successfully` no log do
terminal; `HDK_KILL.txt` gravado; o programa reconectou. Doze testes automáticos novos, inclusive um de que a trava
de `mt5x robots start` não mudou.
**Não conferido:** o painel na tela (a tela real do host é Wayland e não dá para capturar) e a faixa "BLOQUEADO"
nesta abertura (o log do painel só é gravado quando o terminal fecha; ela foi vista na bateria do painel v7); a
disposição escolhida aparecendo no painel; Windows.
**Achado sem correção:** ao fechar, o terminal deixou a pasta do perfil "HADOUKEN" **vazia**, sem o gráfico do
painel. Como a visão refaz o perfil a cada abertura, o painel volta, mas o que a pessoa desenhar nesse gráfico se
perde. Causa não investigada.

**Decisão do dono do projeto, em aberto:** se um painel de operação manual pode **operar** em conta demo só com a
leitura do código aprovada (sem a parte estatística, que não se aplica a ele), ou se a bateria deve ganhar um
critério próprio para painéis. Até lá, operar exige liberar o PARAR TUDO à mão.

### T12.L4 — Painel manual liberado para operar em conta demo ✅

**Decisão do dono do projeto (2026-10-13):** em conta de demonstração o painel de operação manual pode operar sem
bloqueio, a menos que a pessoa queira religar o bloqueio; ele precisa estar liberado para os testes. Responde à
decisão em aberto da seção "O painel na visão do MT5".

**Registrado na entrega:**

- `live.panel_gates`: o painel manual em conta demo entra com a negociação ligada e **sem** o PARAR TUDO. Continuam
  exigidos o robô compilado, a conta de demonstração conferida e a leitura do código aprovada para o código que
  está no disco. Conta real nunca entra por aqui.
- **Quem é "painel manual":** `live.is_manual_panel` usa a evidência do Strategy Tester. No teste mais recente do
  código atual, o robô não fez nenhuma entrada sozinho. Um robô que opera sozinho, posto como painel em
  `[MT5] panel_robot`, ou sem teste do código atual, continua abrindo só para visualização, com o PARAR TUDO. Sem
  isso a liberação seria um atalho para pular a bateria de qualquer robô.
- **Religar o bloqueio:** `[MT5] panel_demo_blocked = true`, ou a caixa "Manter o painel bloqueado mesmo em conta
  de demonstração" em Arquivo > Configurações. Valor inválido na chave vale como bloqueado.
- **O PARAR TUDO nunca é liberado sozinho.** Se já estava acionado, a visão avisa e o painel aparece bloqueado. O
  diálogo de Configurações mostra o estado e tem o botão "Liberar o PARAR TUDO", que só age com o clique da pessoa.
- **A trava da bateria de `mt5x robots start` para robôs automáticos não mudou** (há teste).
- **Defeito achado e corrigido junto:** na bateria de backtests em paralelo (T7.2), duas gravações simultâneas em
  uma base nova tentavam criar a mesma coluna de `_robot_runs` e uma falhava (`duplicate column name`). A gravação
  passou a ter uma trava; há teste que falha sem a correção.

**Conferido:** 643 testes passando; no host, o PARAR TUDO que a visão tinha acionado foi liberado a pedido do dono
do projeto (`mt5x robots stop --release`).
**Confirmado pelo dono do projeto (2026-10-13):** o painel comprou e vendeu normalmente em conta demo depois da
liberação e da correção do horário (seção seguinte).
**Não conferido:** Windows.

### Correção: horário do painel e reabertura do MT5 sem o painel (2026-10-13)

Relato do dono do projeto, no primeiro teste do painel liberado: impressão de atraso no painel; compra e venda
recusadas por horário; "ao retirar ele continuou restringindo".

**O que os registros mostram** (registro do painel em `MQL5\Files\HDK_Logs` e log do terminal):

1. **A janela de horário do painel é contada no relógio do servidor da corretora**, que no host está 6 horas à
   frente do computador. Às 20:43 (local) o fim foi mudado para 22:30 pela janela de parâmetros do MT5; no servidor
   eram 02:43, e a compra foi recusada com "fora do horário (09:15–22:30)". A mensagem não diz que o horário é o do
   servidor nem que horas são lá.
2. **A visão recoloca o painel a cada abertura com os parâmetros de fábrica** (09:15 a 17:30, o horário da B3),
   então o que se muda pelo F7 se perde quando o programa reabre o terminal.
3. **Fechar o MT5 com o programa aberto o trazia de volta "cru".** Às 20:43:44 o terminal foi fechado; a
   reconexão automática do conector o reabriu doze segundos depois, pelo `mt5.initialize`, sem a organização da
   janela e sem o painel.

**O que mudou:**

- `[MT5] panel_any_hours` e a caixa "Painel opera em qualquer horário" em Configurações: passa `InpUseHours=false`
  ao painel a cada abertura. `live.panel_parameters` agora junta as opções (disposição e horário) em um arquivo só.
- Configurações mostra o **horário do servidor agora** e a diferença para o computador, tirados da última cotação.
  Só afirma com cotação andando (duas leituras com intervalo): com o mercado fechado, a cotação parada daria um
  fuso errado. Essa leitura chega depois, sem atrasar o diálogo.
- `MT5Connector.prepare_terminal`: gancho chamado antes de cada tentativa de conexão. A interface põe nele a
  abertura do terminal já organizado e com o painel, então a reconexão automática também passa por ela.

**Conferido no host, com o MT5 real:** com a opção ligada, o registro do painel passou de "operando 09:15–22:30" a
"operando 24h" (20:54:26). Isso também confirma que o MT5 **aceita um arquivo de parâmetros parcial** (só
`InpUseHours`), item que estava como não verificado na T12.L3. Sete testes automáticos novos; 648 no total.

**Sobre o atraso, o que se sabe e o que não se sabe:**

- Medido durante a queixa: terminal a 2,7% de um núcleo e 327 MB; o programa a 0,4%; a máquina a 24% de CPU e 72%
  de memória. Nenhum dos dois estava sobrecarregando o computador.
- No código do painel: ele se atualiza por um temporizador de **500 ms** (`HDK_TIMER_MS`), e estatísticas e linhas a
  cada segundo; o clique é tratado na hora, mas o que aparece na tela espera o ciclo seguinte. O ping até a
  corretora era de 224 ms.
- **Não medido:** o tempo entre o clique e a resposta na tela. Não há como capturar a tela real do host, então a
  causa do atraso percebido é hipótese, não medida. Reduzir o ciclo ou redesenhar logo depois do clique é mudança
  no MQL5 do HADOUKEN (G-02); depois dela a leitura do código e o teste do Strategy Tester precisam ser refeitos,
  ou o painel volta a abrir bloqueado.

**Confirmado pelo dono do projeto (2026-10-13):** depois da correção, compra e venda pelo painel funcionaram
normalmente em conta demo. É relato dele; nenhuma ordem foi enviada nos testes automáticos.
**Não conferido:** a reabertura organizada depois de fechar o MT5 à mão, no terminal real (só com dublê); Windows.

**Registrado para depois, a pedido do dono do projeto (não é urgente: o estado atual atende):**

- **Resposta do painel ao clique:** reduzir o ciclo de 500 ms ou redesenhar logo depois do clique, e a mensagem de
  recusa dizer que o horário é o do servidor e que horas são lá. É a tarefa **G-17** do backlog (no HADOUKEN).
  Depois de mexer no painel, a leitura do código e o teste do Strategy Tester precisam ser refeitos, ou ele volta
  a abrir bloqueado.
- **Notícias no histórico dos ativos:** o filtro de notícias deve popular a base, em uma coluna, para o estudo das
  estatísticas no futuro. É a tarefa **A-15** ([propostas/EPICO_A_ANALYTICS.md](propostas/EPICO_A_ANALYTICS.md)).

**Próximo passo combinado:** a abertura de ordens na instância HADOUKEN, conforme o plano (ordens pendentes no
executor: D-02 do backlog, primeiro passo da etapa 4 do HADOUKEN 2.0 e da T12.4).

### T12.4, primeiro passo — Ordens pendentes no executor (D-02) ✅

Pedido do dono do projeto (2026-10-13): tratar a abertura de ordens na instância HADOUKEN, conforme o plano, pelo
caminho mais seguro. A entrega é do repositório HADOUKEN (PR #10 de lá) e da `mt5x-safety` (1.2.0); aqui ficam o
plano seguido, a evidência e o que mudou no mt5x.

**Plano seguido, na ordem combinada:**

1. Métodos novos no `HDK_SafeExecutor` (`PlacePending`, `ModifyPending`, `CancelPending`, `CancelAllPending`,
   `CancelExpired`, `PendingCount`) e a contagem do risco das pendentes no `HDK_RiskGuard` (`CheckPendingEntry`),
   **sem alterar o que existia** e sem ligar nada ao painel nem aos robôs.
2. Teste só no Strategy Tester: o robô de teste recusa iniciar fora dele, então nenhuma ordem de teste foi para conta.
3. Paridade: os seis robôs com a camada nova e com a antiga, mesmo período, operação por operação.
4. `mt5x-safety` 1.2.0 com os mesmos arquivos.
5. Painel compilado de novo, com o teste no Strategy Tester e a leitura do código refeitos.
6. Merge.

**Decidido com o dono do projeto:** sem o guardião com orçamento (T12.3), o risco de uma pendente conta só no
limite do próprio robô, como uma posição aberta. A reserva na conta inteira (F-05) entra com o guardião.

**Evidência (host Linux, MT5 build 6259 no Wine, 2026-10-13):**

- Compila sem erros nem avisos, em pasta separada da instalação (`/include` do MetaEditor).
- Teste das pendentes: **50 de 50 conferências** (EURUSD H1, cada tick). Entre elas: recusa sem stop, do lado
  errado do mercado e com volume fora do passo; stop-limite e validade com data; cancelamento por idade; pendente
  que executa e vira **posição com stop**; o risco passa de pendente para posição; preço médio recusado.
- **Os seis robôs de estratégia fizeram as mesmas 690 operações** (horário, resultado e volume) com a camada nova e
  com a antiga: EURUSD, ticks reais, 2025-10-01 a 2026-09-30. A comparação foi direta porque o período padrão da
  paridade mudou (a base de EURUSD cresceu) e a tabela da seção T8.9 deixou de ser comparável.
- Painel: compilado com a camada nova, leitura do código aprovada, teste do código atual sem entradas sozinho: a
  visão continua abrindo-o liberado em conta demo.

**O que mudou no mt5x:** o SDK passou a declarar a `mt5x-safety` 1.2.0 (`scaffold.DEFAULT_SAFETY_VERSION`, o
exemplo e a documentação do SDK). Quem já tem a 1.1.0 instalada na pasta do MT5 troca com `--upgrade-safety`.

**Achados no caminho:**

- A corretora arredonda a validade da ordem ao minuto.
- O painel no Strategy Tester é lento: 9 dias de ticks reais levaram 4 minutos, e 3 meses passaram de 4 minutos sem
  terminar. É mais um dado para a G-17 (resposta do painel).

**Não conferido:** ordem pendente em conta de demonstração (não há quem as coloque: a boleta não existe); conta
netting; ativo da B3; Windows.
**Não faz parte desta entrega:** boleta no painel (G-05, veio no passo seguinte), cancelamento automático por sinal
ou fim de pregão (E-10), reserva de risco na conta inteira (F-05), recuperação após reinício (E-13), grade (D-06).

### T12.4, segundo passo — Boleta de ordem pendente no painel (G-05) ✅

Pedido do dono do projeto (2026-10-13): seguir com a boleta, como sugerido depois das ordens pendentes. A entrega é
do repositório HADOUKEN (PR #11 de lá) e da `mt5x-safety` (1.3.0); aqui ficam o que foi feito, a evidência e o que
mudou no mt5x.

**O que o painel ganhou:** a aba "Boleta", a quinta. Tipo (limite ou stop), direção, preço escolhido pela distância
do mercado em passos de dez ticks, prévia de lote, risco, stop e alvo, botão de envio (que mostra o motivo quando
não pode enviar) e a lista das pendentes do robô, com `×` em cada uma e "Cancelar todas". Validade pelo parâmetro
novo `InpPendingMaxMin` (240 minutos). Pausa, bloqueio, fim do horário do robô e FECHAR TUDO cancelam as pendentes.
Toda ordem sai pelo `HDK_SafeExecutor`, depois do `HDK_RiskGuard`; o Python continua sem enviar ordem nenhuma.

**Evidência (host Linux, MT5 build 6259 no Wine, conta MetaQuotes-Demo, hedging, EURUSD H1, 2026-10-13).** O teste
foi feito no terminal em tela virtual, clicando no painel e lendo a conta com `mt5x portfolio`; as ordens ficaram a
100 pips do mercado e foram todas canceladas.

- Compila sem erros nem avisos; a checagem estática do HADOUKEN passa.
- Com o robô pausado o envio é recusado, com o motivo na tela.
- Compra limite e venda stop colocadas: 1,26 lote, stop e alvo na ordem, risco de US$ 149,94 cada (0,15% do saldo),
  validade de 239 minutos gravada na ordem. `mt5x portfolio` mostrou as duas, com o risco e o dono certos.
- Terceira ordem na mesma direção recusada: "preço médio bloqueado".
- `×` cancelou só a ordem escolhida; "Cancelar todas", PAUSAR e FECHAR TUDO cancelaram o que havia.
- Depois da versão final do código: compilado pelo `mt5x robots compile`, teste no Strategy Tester (2026-09-21 a
  2026-09-30, ticks reais) sem entradas, leitura do código aprovada ("nenhum envio de ordem fora de Safety/"): a
  visão continua abrindo o painel liberado em conta demo.

**O que mudou no mt5x:** o SDK passou a declarar a `mt5x-safety` 1.3.0 (`scaffold.DEFAULT_SAFETY_VERSION`, o exemplo
e a documentação do SDK). Na biblioteca a aba só aparece para o robô que liga `pend_enabled`; o robô criado pelo
`mt5x robots new` não usa o painel, então nada muda nele além do número da versão.

**Achado no caminho:** a tela virtual do modo oculto (1280x800) é baixa para o painel com o MT5 em 192 DPI; para
ver o painel inteiro o teste usou uma tela virtual de 1920x1080 criada à mão.

**Limitações desta versão:** não se digita o preço (o painel não tem campo de edição); sem stop-limite na boleta;
tipo, direção e distância voltam ao padrão quando o robô reinicia; o risco da pendente conta só no limite do
próprio robô (sem guardião, T12.3); com pendentes nas duas direções a faixa do topo diz "PAUSADO: preço médio
bloqueado", o que é verdade mas confunde.

**Não conferido:** uma pendente executando e virando posição pela boleta (só no teste do Strategy Tester do
executor); cancelamento por fim do horário e por bloqueio de risco; disposições compacta e ampla da aba; conta
netting; ativo da B3; Windows. A paleta clara e a saída física eDP-1 1280×800 foram vistas na T12.6a.

### T12.6a — Painel v7: conferência e vocabulário ✅

- **Objetivo:** não empilhar abas novas em cima do que não foi visto, e dar um nome só a cada coisa.
- **Onde:** repositório HADOUKEN (`TESTES_PAINEL_V7.md`, `PLANO.md`); aqui só o painel de status.
- **Passos:** ver a disposição normal, o parâmetro que força a disposição, o ajuste abaixo de 100% e o tema claro;
  repetir na tela real do dono; medir o terminal sem o painel, para ter a linha de base de desempenho; fixar o
  vocabulário (proposta: "organização da janela" para o MT5, "disposição" para compacta, normal e ampla, "estado"
  para expandido e recolhido, sem um estado chamado "compacto"); verificar se o `ENUM_HDK_LAYOUT_MODE` de
  `HDK_Config.mqh` ainda é usado.
- **Critério de aceite:** a lista "O que não foi testado" do relatório do painel fica só com o que depende de
  mercado, de outra conta ou do Windows.
- **Concluída (2026-10-14):** disposição normal forçada, ajuste abaixo de 100% derivado da fórmula ativa e da
  captura, paleta clara automática e tela física eDP-1 1280×800 vistos; o painel coube também após reduzir a janela.
  A linha de base de um gráfico sem painel foi registrada. O MT5 recusou uma tentativa de VENDA por AutoTrading
  desligado; nenhuma ordem foi executada nem posição alterada. Medições e limitações em
  `Documentation/HADOUKEN_2/TESTES_PAINEL_V7.md` no repositório HADOUKEN.

### T12.6b e T12.6c

Detalhadas no plano do HADOUKEN. A T12.6b depende de G-02 (arquitetura modular), conforme as dependências de
G-03/G-04 no épico; G-02 é a próxima tarefa elegível e T12.6b aguarda sua conclusão. Registro desta revisão: o
**estado recolhido** e o **minimizar**
que já existe precisam virar uma coisa só ou ter papéis declarados; a aba **Configuração** do painel deve mostrar a
organização da janela com as mesmas opções da T12.L2 (lendo o que o mt5x grava), em vez de um segundo conjunto de
interruptores; e é preciso decidir como 12 abas cabem na disposição compacta antes de criar a quinta.

**Decisões do dono do projeto** (antes da tarefa indicada)

| # | Decisão | Antes de |
|---|---|---|
| 1 | Escolher janela por janela (quatro caixas) ou por grupo (laterais, barras, perfil próprio). **Respondida: por grupo** | T12.L1 |
| 2 | A organização deve valer também quando o MT5 é aberto pela ponte ou pela interface, ou só ao iniciar um robô. **Respondida: quando o usuário pedir (T12.L3)**; abrir pela ponte continua sem organizar | T12.L1 |
| 3 | A disposição do painel (automática, compacta, normal, ampla) também entra nas opções da interface. **Respondida: sim (T12.L3)** | T12.L2 |
| 4 | O botão TELA do painel continua existindo ao lado das opções da interface, e qual vence quando divergem (hoje: qualquer "desligado" vence) | T12.L2 |
| 5 | Vocabulário do item 2 dos achados do painel | T12.6a |
| 6 | Como as abas novas cabem na disposição compacta (rolagem, menu, ou abas só nas disposições maiores) | T12.6c |

### T12.3 a T12.8 — a fazer

**Decisão do dono (2026-10-14):** avançar a implementação da Safety em T12.2/T12.3 mesmo com a validação
operacional pendente. Exercitar operações supervisionadas somente após T12.3 e somente em conta demo. A T12.3
(guardião com poder de bloquear) dá uso às operações supervisionadas da T12.1; nenhuma conta real será usada.
A T12.4 começa por ensinar o executor a criar ordens pendentes.

---

## Fase 13 — MT5x Analytics: catálogo de evidências sobre ativos

**Situação: planejada. Nada desta fase foi implementado.** Especificação do dono do projeto (2026-10-13), guardada
como veio em [analytics/ESPECIFICACAO.md](analytics/ESPECIFICACAO.md) (as seções citadas abaixo como "§" são de lá).
Esta seção é o plano de implantação: o que já existe no código, onde a especificação e o código divergem, e as
tarefas na ordem.

**Em uma frase:** o Python calcula e valida, com data de validade e amostra, fatos sobre cada ativo (volume
relativo, regime, sazonalidade, probabilidades condicionais) e os publica em um arquivo pequeno; o robô MQL5 só lê.

**Regras da fase** (valem para todas as tarefas):

1. O Python continua sem enviar ordens. Publicar um arquivo de leitura não muda isso.
2. Nenhum dado analítico dá permissão para passar por cima do `HDK_RiskGuard` ou do `HDK_SafeExecutor`.
3. Com o analytics desligado, ou com o arquivo ausente, vencido ou malformado, a decisão de cada robô é a mesma de
   hoje. A paridade dos seis robôs de estratégia não pode mudar.
4. Nenhum fato usa informação posterior ao instante em que ele é dado como disponível (regra *point-in-time*, §8).
5. Toda hipótese testada fica registrada, inclusive as rejeitadas. "Aprovada" quer dizer evidência fora da
   amostra naquele contexto, não recomendação de compra ou venda.
6. Os primeiros modos de uso no robô são só `VISUAL_ONLY` e `NO_ACTION` (§10). Bloquear ou reduzir entrada é a
   T13.7, opt-in por estratégia.
7. Mudanças de schema são aditivas (AGENTS.md): as tabelas por símbolo e timeframe de hoje não mudam.

### T13.0 — Plano e levantamento inicial ✅

Especificação guardada e plano registrado. O levantamento abaixo foi feito lendo o código do mt5x em 2026-10-13;
**o repositório HADOUKEN não foi lido nesta rodada**.

**O que já existe e a fase deve reaproveitar**

| Assunto da especificação | O que existe hoje | Onde |
|---|---|---|
| Montar timeframe maior a partir do M1 (§4.2) | `mt5x resample` (T11.1): abertura da primeira, máxima, mínima, fechamento da última, soma dos dois volumes, `spread` = o menor do período. Conferido barra a barra contra o MT5 real em EURUSD (M5, M15, H1, H4, D1) | `mt5_extracao/resample.py`, tabela `_resampled` |
| Qualidade dos dados (§4.1) | OHLC inconsistente, volume zero, horário duplicado, lacunas e barras por dia, por bloco extraído | `data_quality.py`, `_extraction_log.quality_json`, `mt5x quality` |
| Base de tempo (§4.1) | Uma coluna `time`, no horário da corretora (padrão) ou em UTC, escolhida para a base inteira | `DatabaseManager.time_basis` (T1.6) |
| Ticks e book (§2, item 6) | Extração de ticks por blocos e snapshots do book, pela CLI | `tick_extractor.py`, `book_collector.py` |
| Spread medido | Perfil de spread por hora, calculado dos ticks | `_symbol_specs.spread_profile` (T10.12) |
| Sazonalidade (§5) | Retorno e amplitude médios por hora e por dia da semana (sem volume) | `study.seasonality`, `mt5x analyze` |
| Regime (§5, H05) | "tendência", "lateral" ou "transição" pelo ADX da última barra, com eficiência, R² e percentil do ATR | `strategies/asset_screener.py` (`filter_b`) |
| Validação fora da amostra (§7.3) | Walk-forward, robustez de parâmetros e Monte Carlo, para as estratégias | `strategies/validation.py`, `mt5x validate` |
| Calendário de eventos | CSV opcional `time,symbol,impact` | `[SCREENER] events_file` |
| Pasta comum do MT5 (§9.1) | O caminho de `Common\Files` é resolvido no Windows e no Wine; o mt5x já **lê** de lá (`HDK_trades_<magic>.csv`, `HDK_portfolio_<conta>.json`) | `robots/mt5_tools.common_files_dir` |
| Resultado por robô, ativo e timeframe (§5, "saúde das estratégias") | Execução mais recente de cada combinação, com a situação | `robots/matrix.py` (T11.6) |

**Onde a especificação e o código divergem** (cada item tem a tarefa que decide)

1. **O `resample` atual agrupa pelo relógio, não pela sessão.** Uma barra H4 ou D1 pode juntar dois pregões, e não
   há estado de barra parcial, cobertura nem contagem de barras de origem. Ele também só monta timeframes que o MT5
   tem (o enum `Timeframe`) e só até D1; "qualquer múltiplo de minuto" (M7, M45...) não cabe no enum. → T13.1.
2. **O `spread` da barra montada é o mínimo do período**, porque é assim que o MT5 monta e foi isso que se conferiu.
   A especificação pede que a métrica seja declarada (§4.2). O mínimo é o melhor caso, não um custo. → T13.1: manter
   o mínimo na coluna que imita o MT5 e dar nome próprio a qualquer outra medida.
3. **A base guarda um horário só, e sem "disponível em".** Não há `time_utc` e `time_broker` lado a lado,
   `available_at`, `revision` nem `ingest_batch`: reextrair regrava a barra (upsert da T1.1) sem deixar rastro do
   valor anterior. → T13.1 e T13.2, por tabelas novas ao lado, sem mexer nas de barras.
4. **Não há calendário de sessões.** O horário de sessão do relatório de qualidade é um par fixo (09:00 a 18:30) e a
   atualização "em dia" da interface desconta três dias para o fim de semana. Feriado, pausa, fechamento antecipado
   e horário de verão não existem no código. → T13.1.
5. **Volume real zero e volume desconhecido são o mesmo valor hoje** (a coluna vem zerada do MT5 quando a corretora
   não informa). → T13.1.
6. **Não há catálogo de instrumentos nem rolagem de contrato.** O símbolo é o texto da corretora (`WIN$N`). Como a
   corretora monta a série contínua não foi verificado. → decisão em aberto 2; não entra antes de haver ativos da B3
   no host.
7. **A validação existente é de estratégia, não de hipótese:** não tem correção para múltiplos testes, *purging*,
   intervalo de confiança com dependência temporal nem taxa-base. → T13.4.
8. **O backtester Python é otimista em custo e não limita margem** (pendências registradas na T10.12 e no ponto de
   retomada da Fase 12). Qualquer "expectativa líquida" calculada com ele herda isso. → T13.4 e T13.7: a evidência
   que libera uso no robô vem do Strategy Tester com ticks reais.
9. **O nome `mt5x analytics validate` convive com `mt5x validate` e `mt5x analyze`**, que já existem e fazem outra
   coisa. → T13.2 decide os nomes antes de criar o primeiro comando.
10. **O lado MQL5 mora em outro repositório** (HADOUKEN). A T13.5 e a T13.6 têm uma parte lá, com o plano e o
    andamento de lá, como na Fase 12.

**Não levantado nesta rodada** (cada tarefa começa por conferir o que lhe diz respeito): o código do HADOUKEN
(`StrategySignal`, `StrategyRules`, painel); se um robô no Strategy Tester consegue ler um arquivo da pasta comum
(teste 14 da especificação); o tamanho real da base com M1 de vários anos e o tempo de cálculo; a qualidade do volume
real por corretora.

### T13.1 — Calendário de sessões e barras derivadas (A1)

- **Objetivo:** montar, do M1, barras de N minutos que respeitam a sessão, com estado de qualidade por barra.
- **Depende de:** T11.1.
- **Arquivos previstos:** `mt5_extracao/analytics/calendar.py` e `analytics/resampling.py` (novos), tabelas novas
  `market_calendar` e `bars_derived`, `cli.py`, testes novos. `resample.py` e as tabelas de barras atuais não mudam.
- **Passos:**
  1. Calendário por símbolo: sessões com abertura, fechamento e pausas, em UTC e no horário da corretora. Fonte a
     decidir (decisão em aberto 1): deduzido das barras M1 já gravadas, lido do MT5 (`symbol_info`), ou arquivo
     mantido à mão para feriados e horários especiais.
  2. Agregador por sessão para qualquer N inteiro de minutos, com `bar_count`, `coverage_ratio` e `quality_status`
     (`COMPLETE`, `PARTIAL`, `NO_TRADES`, `MISSING`, `CORRECTED`) e a política `partial_last_bar`.
  3. `1D` como sessão e `1440min` como janela de 24 horas, separados.
  4. Volume real: desconhecido diferente de zero.
  5. Atualização incremental: uma ingestão nova refaz só as barras afetadas.
- **Critérios de aceite:** testes 1, 2, 3, 4 (a parte das barras) e 5 da especificação (§14); para um ativo 24 horas
  (EURUSD), as barras derivadas dos timeframes do MT5 continuam iguais às do `resample` atual, que foi conferido
  contra o terminal.
- **Armadilha:** não trocar o `resample` atual por este. Os backtests gravados e a paridade usam as barras de hoje.

### T13.2 — Guarda de características com data de disponibilidade (A2)

- **Objetivo:** gravar cada fato com quando foi observado, quando ficou disponível, até quando vale, de qual
  revisão dos dados saiu e com qual versão do algoritmo; e consultar "o que se sabia em tal instante".
- **Depende de:** T13.1.
- **Arquivos previstos:** `analytics/feature_store.py`, `analytics/features.py` (registro de características e
  dependências), tabelas `feature_definitions` e `feature_values`, `cli.py` (`mt5x analytics ...`, nomes decididos
  aqui), testes.
- **Passos:** registro de características com dependências; gravação com revisão (correção tardia gera revisão nova,
  a anterior fica); consulta *as-of*; recálculo só do que depende do que mudou.
- **Critérios de aceite:** testes 4, 6 e 7 da especificação.
- **Decide antes de começar:** SQLite ou PostgreSQL para estas tabelas, medindo com a base real (decisão em aberto 6).

### T13.3 — Perfis de volume, sazonalidade e regimes (A3)

- **Objetivo:** as famílias de características da §5 que só dependem de barras: retornos e gaps, volume (médias,
  inclinação, percentil), volume intradiário e RVOL no mesmo horário, volatilidade, tendência e lateralidade,
  sazonalidade e estrutura intradiária.
- **Depende de:** T13.2.
- **Arquivos previstos:** `analytics/volume_profiles.py`, `analytics/market_regimes.py`, testes com séries sintéticas.
- **Passos:** reaproveitar `strategies/indicators.py`, `study.seasonality` e a classificação da triagem em vez de
  reescrever; estados `TREND`, `RANGE`, `TRANSITION` e `UNCERTAIN` com confiança e duração mínima; cada
  característica declara o que "confiança" quer dizer para ela.
- **Critérios de aceite:** algoritmos conferidos em séries sintéticas de resposta conhecida e em pelo menos dois
  mercados diferentes da base; o regime da triagem (`mt5x screen`) e o desta tarefa ou são o mesmo cálculo ou a
  diferença fica documentada.
- **Fica de fora:** liquidez e microestrutura (precisam de ticks e book com cobertura medida) e relações entre
  ativos. Entram como tarefas próprias quando houver dados.

### T13.4 — Hipóteses H01 a H05 e o motor de avaliação (A4)

- **Objetivo:** descrever uma hipótese em um arquivo (condição, janela, desfecho, universo, custos), avaliá-la e
  guardar o resultado com amostra, taxa-base, *lift*, expectativa líquida, intervalo de confiança e resultado fora
  da amostra.
- **Depende de:** T13.3.
- **Arquivos previstos:** `analytics/hypotheses.py`, `analytics/pattern_engine.py`, `analytics/validation.py`,
  tabelas `pattern_definitions` e `pattern_evaluations`, definições de H01 a H05, testes.
- **Passos:** formato declarativo; separação cronológica treino, validação e teste, com *purging* e embargo;
  correção para múltiplos testes; intervalo por *bootstrap* em blocos; sensibilidade a custo e atraso; estados
  `DRAFT → EXPLORATORY → VALIDATING → APPROVED → MONITORING → RETIRED`; busca exploratória sempre marcada como tal.
- **Critérios de aceite:** teste 8 da especificação; H01 a H05 avaliadas com dados reais e o resultado publicado em
  relatório, **qualquer que seja ele** (a tarefa não depende de as hipóteses se confirmarem); uma hipótese rejeitada
  continua no catálogo.
- **Armadilhas:** os números de H01 a H05 na especificação (1,5%, 0,5%, 1,15%, 17:25, 0,40%) são exemplos, não
  medidas; o horário de fechamento vem do calendário da T13.1; um gap que aconteceu antes da entrada não é lucro de
  quem entra na abertura.

### T13.5 — Publicador e leitor MQL5 somente leitura (A5)

- **Objetivo:** gravar o retrato das características em um arquivo que o robô lê sem risco de pegar um arquivo pela
  metade ou vencido.
- **Depende de:** T13.3 (publica características) e T12.1 (camada `Safety` do HADOUKEN).
- **Arquivos previstos:** aqui, `analytics/publisher.py`, tabela `feature_publications`, `cli.py`, testes; no
  HADOUKEN, `Include/Framework/HADOUKEN/Analytics/` (`HDK_AnalyticsTypes`, `HDK_AnalyticsReader`).
- **Passos:** CSV com os campos da §9.1 e manifesto com versão e hash; arquivo temporário, conferência e troca
  atômica; pasta resolvida por `mt5_tools.common_files_dir`; leitor que devolve `UNAVAILABLE` ou `STALE` em vez de
  um valor neutro inventado; prazo de validade por característica.
- **Critérios de aceite:** testes 9, 11 e 14 da especificação; conferido no MT5 real do host (Wine); o Windows fica
  no checklist de [validacao_instaladores.md](validacao_instaladores.md).
- **Decide antes de começar:** se o leitor funciona dentro do Strategy Tester (o Tester tem pasta de arquivos
  própria) e, se não, como um teste reproduz o que o robô veria; CSV ou a cópia em SQLite da §9.3, medindo.

### T13.6 — Tela de pesquisa no mt5x e aba no painel do HADOUKEN (A6)

- **Objetivo:** ver as características, as hipóteses e a origem de cada número.
- **Depende de:** T13.4 e T13.5; T7.2 (tabelas da interface) e T12.6 (abas do painel do HADOUKEN).
- **Arquivos previstos:** aqui, um painel novo em `mt5_extracao/ui/` ou uma aba no painel de resultados do
  navegador (decidir na tarefa: o painel vivo da T11.9 já tem servidor, atualização e gráficos); no HADOUKEN,
  `HDK_AnalyticsPanel`.
- **Critérios de aceite:** filtros, mapa por dia e hora, distribuição condicional, catálogo de hipóteses com
  amostra, validade e fonte; no painel do MT5, idade do dado sempre à vista e aviso de dado vencido.

### T13.7 — Estratégia × regime e pilotos opt-in (A7)

- **Objetivo:** medir o resultado de cada estratégia condicionado ao regime e permitir que uma estratégia, por
  escolha explícita, use uma evidência aprovada para bloquear a própria entrada ou reduzir o risco.
- **Depende de:** T13.5, T12.3 (guardião com orçamento) e de haver robô aprovado na bateria.
- **Arquivos previstos:** `analytics/strategy_fitness.py`, tabela `strategy_regime_stats`; no HADOUKEN,
  `HDK_AnalyticsPolicy`.
- **Critérios de aceite:** teste 10 da especificação (com o analytics desligado nada muda); bateria, paridade e
  walk-forward sem regressão; a política só reduz, nunca aumenta volume ou orçamento; evidência do Strategy Tester
  com ticks reais, não só do backtester Python.

### T13.8 — Deriva, alertas e governança das regras (A8)

- **Objetivo:** acompanhar as regras aprovadas e rebaixar sozinho a que deixar de se sustentar.
- **Depende de:** T13.4 e T13.7.
- **Arquivos previstos:** `analytics/monitoring.py`, avisos por `notify.py`.
- **Critérios de aceite:** regra instável passa de `APPROVED` para `MONITORING` ou `RETIRED` com o motivo gravado;
  trilha de quem aprovou, quando e com qual evidência; política de retenção e de cópia de segurança definida.

### Decisões em aberto (do dono do projeto, antes da tarefa indicada)

| # | Decisão | Antes de |
|---|---|---|
| 1 | De onde vem o calendário de sessões e quem mantém feriados e horários especiais | T13.1 |
| 2 | Série contínua de WIN/WDO: só para estudo ou também para características de execução (nunca executar em preço ajustado) | T13.1, quando houver ativos da B3 |
| 3 | Aceitar timeframes que o MT5 não tem (M7, M45...) ou só os do enum `Timeframe` | T13.1 |
| 4 | Nomes dos comandos `mt5x analytics ...`, dado que `validate`, `analyze` e `report` já existem | T13.2 |
| 5 | O que "confiança" significa em cada característica publicada | T13.3 |
| 6 | SQLite ou PostgreSQL para as tabelas novas; CSV ou SQLite para o retrato lido pelo MT5 (medindo) | T13.2 e T13.5 |
| 7 | Prazo de validade do retrato por frequência e o que cada estratégia faz com dado vencido | T13.5 |
| 8 | Consentimento explícito para uma evidência alterar limite operacional | T13.7 |
| 9 | Retenção, versionamento e cópia de segurança de características e hipóteses aprovadas | T13.8 |

**Ordem com a Fase 12:** T13.1 a T13.4 são só Python e não tocam no HADOUKEN, então podem andar em paralelo com a
Fase 12. T13.5 em diante mexem no HADOUKEN e dependem das tarefas da Fase 12 indicadas em cada uma.

---

## Fase 14 — Plataforma modular HADOUKEN + mt5x (backlog integrado)

**Situação: backlog registrado. Nada implementado.** Pedido do dono do projeto (2026-10-13): registrar como
tarefas futuras, rastreáveis e na ordem de dependência, as propostas de evolução do conjunto. O backlog fica fora
deste arquivo, em [propostas/README.md](propostas/README.md), porque cruza três repositórios:

| Épico | Assunto | Onde está | Relação com tarefas deste plano |
|---|---|---|---|
| A | Base de dados e inteligência de mercado | [propostas/EPICO_A_ANALYTICS.md](propostas/EPICO_A_ANALYTICS.md) | É a Fase 13 (T13.1 a T13.8), mais Renko, liquidez e correlação entre ativos |
| B | Framework modular de estratégias | HADOUKEN | novo |
| C | Estratégias e indicadores | HADOUKEN | parte existe (seis estratégias, T6.x e T8.9) |
| D | Mecanismos de entrada e execução | HADOUKEN | Gradiente Linear é T12.4 e T12.5 |
| E | Gestão de posições e saídas | HADOUKEN | parte existe no executor |
| F | Gestão global de carteira | HADOUKEN | T12.2 e T12.3 |
| G | Painel HADOUKEN | HADOUKEN | T12.6 (T12.6a a T12.6c) |
| H | Estatísticas avançadas | HADOUKEN e mt5x | T12.6A |
| I | Pesquisa, otimização e validação | [propostas/EPICO_I_PESQUISA_VALIDACAO.md](propostas/EPICO_I_PESQUISA_VALIDACAO.md) | estende T6.4, T8.6, T10.5, T10.6 e T10.12 |
| J | IA e agentes de desenvolvimento | [propostas/EPICO_J_IA_AGENTES.md](propostas/EPICO_J_IA_AGENTES.md) | T12.7 e T9.1 |

**Como este backlog convive com o plano:** as tarefas do backlog usam identificadores próprios (`A-03`, `D-07`) e
os estados `PROPOSTO → PLANEJADO → EM DESENVOLVIMENTO → EM TESTE → VALIDADO → CONCLUÍDO`. Uma tarefa só entra no
painel de status deste arquivo, com número `T<fase>.<n>` e passos detalhados, quando for a próxima a ser feita. As
que já são tarefas daqui (Fases 12 e 13) continuam valendo como estão: o backlog aponta para elas, não as repete.

**Ordem, dependências críticas, regras de compatibilidade e segurança, lacunas do código e decisões pendentes:**
em [propostas/README.md](propostas/README.md). As referências de estudo indicadas (vídeos, não assistidos nesta
rodada) estão em [propostas/REFERENCIAS.md](propostas/REFERENCIAS.md).

### T14.0 — Backlog integrado registrado ✅

Dez épicos e 131 tarefas registrados nos repositórios mt5x e HADOUKEN, e a regra de evolução da `mt5x-safety` no
repositório dela. Só documentação: nenhum código, preset, configuração ou banco foi alterado.

## Fase 15 — Gestão de recursos (espaço, higienização e automação)

**Frente paralela ao roadmap, pedida pelo dono do projeto em 2026-10-14.** A base pode ficar muito grande: a aba "Recursos" mede a base, as tabelas e o disco; aponta séries sem benchmark positivo, não usadas em estudos, obsoletas ou vazias (favoritos, seleção, benchmarks e ativos com benchmark positivo, isto é, com algum backtest lucrativo em qualquer robô ou configuração, ficam protegidos); arquiva de forma reversível (cópia conferida antes de retirar da base) e restaura; otimiza; e tem automação programada **só com opt-in** (`[RESOURCES] auto_enabled`). O mesmo pelo terminal em `mt5x resources`. Plano, princípios, regras e chaves: [propostas/GESTAO_RECURSOS.md](propostas/GESTAO_RECURSOS.md).

Sobre a regra "nunca apague tabelas de usuários": ela vale para mudanças de schema. Aqui a retirada é ação explícita do usuário (ou da automação que ele ligou), sempre precedida do arquivo, e reversível.

## Fase 16 — Laboratório: analytics de operações (contexto, execução e otimização)

**Pedida pelo dono do projeto em 2026-10-14.** Uma aba "Laboratório" que enriquece cada operação dos robôs (dia, hora,
notícia, volume, volatilidade), mostra onde eles ganham e perdem por duas lentes (por robô e por ativo), re-simula as
mesmas entradas sob outros modos de execução e propõe regras de filtro validadas em walk-forward e numa reserva final
intocada. Calendário econômico de várias fontes (CSV e exportação do MT5). Plano completo,
tarefas T16.1 a T16.12, dependências e decisões pendentes: [propostas/ANALYTICS_OPERACOES.md](propostas/ANALYTICS_OPERACOES.md).

Convivência: o pacote é `mt5_extracao/trade_analytics/` e o comando `mt5x trades`, porque `analytics/` e
`mt5x analytics` já estão reservados para a Fase 13 (contexto do mercado, sem PnL). O livro de operações
`_trade_ledger` é o mesmo da T12.6A. Martingale e grid seguem proibidos em robô real pela bateria; no Laboratório o
martingale só aparece para robô que o suporta, com as travas de segurança dele.

---

## Checklist de revisão (use em todo PR)

- [ ] Só os arquivos listados na tarefa foram alterados (ou justificativa no PR).
- [ ] `pytest` passa localmente; nenhum teste foi removido ou marcado como `skip` sem motivo técnico.
- [ ] Mensagens ao usuário em português, com a ação de correção.
- [ ] Nenhum segredo, caminho pessoal, `config.ini`, `.env` ou `.db` no commit.
- [ ] Windows **e** Linux considerados (caminhos com `pathlib`/`os.path`, nada de `\\` fixo fora de strings do Windows).
- [ ] Painel de status deste documento atualizado.
