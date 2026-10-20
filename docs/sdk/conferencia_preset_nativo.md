# Conferência nativa do pacote — T19.29 inicial

`presets.execution.PresetExecution` lê o manifesto e `.set` exportados reais pelo
loader existente, revalida fonte/perfil e exige contexto de teste explícito.
Não importa no banco, instala Expert, conecta conta ou envia ordem Python ao ler.
Somente backtest/replay histórico no Tester DEMO; PLAY/ao vivo é recusado.

Reutiliza Session, lease, perfil/compilador privados, executor de processos,
cancelamento, observador CSV e armazenamento existentes. Clone da configuração
recebe só capital **simulado** explícito em memória. Configuração do usuário,
limites Safety e sessões alheias não mudam. O `.set` preserva ordem/bytes do
exportado; o hook do runner existente confere o arquivo realmente apontado pelo
INI e contexto/modelo/depósito/visual/local, antes de iniciar, e conserva o INI.
Não há novo gerenciador de processos ou API de ordem.

Recibo de inputs conserva seu CSV/schema v1. A instrumentação opt-in acrescenta
uma linha de auditoria com Tester/Visual/DEMO lidos do **MQL5 em execução**.
Pedido Visual no INI não basta para afirmar replay: o flag nativo deve corresponder.
Todos os inputs reais também devem corresponder ao pacote e reabrir do banco.

A conferência exige observador próprio, fonte aprovada, entradas protegidas,
posições encerradas completas no livro existente e conciliação CSV/run/saldo pelo
serviço financeiro existente. Sinais registram origem/confirmação/disponibilidade;
entrada deve corresponder ao sentido/volume/instante conhecido. Resposta parcial,
aceita sem fill ou incerta não qualifica o piloto; não repetir automaticamente.

Replay compara livro e sinais com backtest nativo da mesma identidade de execução.
Tickets são próprios de cada run e ficam no rastro, sem exigir os mesmos números
entre runs. Diferença registra primeiro campo/posição, sem ampliar tolerância nem
reescrever resultados. Zero negócios é `contract_only`; cancelamento, fechamento
faltante ou prova incompleta não concede aceite. `functional_cycle` exige negócios,
replay Visual confirmado e referência conciliada. Nenhum estado autoriza ao vivo
ou significa qualificação estatística. História/cobertura de ticks reais continuam
qualificadas somente pelo relatório: cache sem revisão da corretora fica inelegível.

Native preenche na cotação/ticks disponíveis após a confirmação. `stop_first` é a
política conservadora do modelo OHLC Python, não promessa de prioridade sobre ticks
reais. Não afirmar igualdade financeira Python/nativa ou preço ideal de abertura.

Evidência versionada fica no diretório próprio da Session e tabela aditiva
`_preset_native_runs`, ligada a run/preset/revisão/contexto/fonte/arquivo e recibo.
Não regrava `_robot_runs`, livro ou resultados anteriores. Repetição idêntica é
idempotente; evidência diferente exige outro run. O relatório preserva limitações.

46 testes focados e3 verificações pertinentes de diferença/última posição passaram,
com fakes determinísticos: são provas dos contratos, **não aceite nativo**.
PostgreSQL no job existente. Jornada real do Manual pela UI/backtest/replay é a
cobertura consumidora seguinte; não repetir QA de zero negócios ou campanha numérica.
Pullback segue bloqueado; nenhum ensaio ao vivo; Windows físico permanece pendente.
