# Reutilização da ligação MT5 existente

Corretivo do conector da Fase2/T2.5; não reabre a T17.15, que corrigiu apenas
sincronização de testes da UI, nem conclui a paridade física T2.6. O enquadramento
inicial T17.15 foi recusado após leitura da ficha; nenhum PR foi aberto nesse nome.

`initialize(force_restart=False)` primeiro confere, sob o `tester_lease` existente,
terminal conectado, diretório configurado (incluindo `windows_path` pela ponte),
diretório de dados, conta, servidor, modo e credenciais efetivas de login automático.
Com correspondência positiva, reutiliza a ligação sem chamar preparação da janela,
`initialize`, `login` ou `shutdown` do SDK. O status identifica sessão existente.
O consumidor guarda essa identidade também após sua desconexão local. Se ela mudar
ou ficar indisponível, a reconexão desse consumidor recusa sem limpar a API compartilhada.
A contagem de símbolos e a recuperação de coleta respeitam a mesma afinidade.

Uma ligação iniciada por esta instância mantém encerramento pelo SDK. Uma ligação
reutilizada é liberada apenas localmente. A conexão nova mantém as estratégias,
login do `.env` e preparação anteriores; conta inicial diferente da configurada
continua usando o fluxo legado. `force_restart=True` é explícito e mantém esse fluxo.
Não há garantia de preservação para o primeiro contato que não corresponde à configuração.

A API oficial documenta informações do terminal em
[terminal_info](https://www.mql5.com/en/docs/python_metatrader5/mt5terminalinfo_py),
que usamos para a conferência. A chamada
[initialize](https://www.mql5.com/en/docs/python_metatrader5/mt5initialize_py)
pode abrir o terminal quando necessário; por isso o reuso confirmado a evita.
[shutdown](https://www.mql5.com/en/docs/python_metatrader5/mt5shutdown_py)
encerra a ligação da API; não a tratamos como prova de encerramento do processo.

A posse cobre a conferência, não reserva toda a vida da aplicação nem qualifica
concorrência entre processos. Rotinas administrativas explícitas anteriores não
foram substituídas. A identidade verificada não certifica a fonte nem os dados;
extração não libera negociação. Windows físico e sessões concorrentes permanecem
pendentes. Nenhum novo lock, configuração, executor ou limite financeiro.


Validação local: baseline33 testes anteriores, regressão nova22falhas/2sucessos
antes da implementação; seleção52aprovados com extrator, login e gancho real da UI
(34,33s),27incremental/ponte (40,29s),46casos próprios finais. Contagens sobrepostas.
Doctor sobre configuração existente20OK/1aviso pandas-ta opcional/0falhas; a primeira
invocação sem --root falhou por configuração ausente na worktree e não motivou
instalação/configuração artificial.

Candidato no host Linux/Wine: backend DEMO6276 correspondente, exposição0/0,
initialize/login/shutdown proibidos no helper; EURUSD/H1 2026-10-12..14 (base broker)
25→49barras, reabertura, atualização pelo método original, backup consistente,
cópia anterior/restauração auditada e CSV conferido em todas as colunas. UI online
real do candidato reabriu49barras e12270símbolos com conexão emprestada; captura
inspecionada. Só banco privado de64KiB, sem abrir/restaurar a base do proprietário.
O exemplo inclui os extremos fornecidos pelo provedor; não redefine intervalo ou
fonte como certificados. Colunas opcionais None/NaN mantêm ausência. Proteções,
limites de gravação, configuração e preferências originais preservados.

O último ajuste dos preflights de coleta usa auto_login=False: esses leitores
reconferem a identidade já emprestada sem introduzir login automático em chamadas
que antes não o faziam. initialize explícito continua respeitando auto_login.
Casos próprios cobrem essa opção; initialize, conferência de identidade e shutdown
usados na prova real não mudaram. Não se repetiu extração/restauração por esse ajuste.


Composição por Git com173/4d3f560 (inclui172/bfc422): conflitos somente de append
em continuidade/checklist resolvidos preservando ambos os recortes. Runtime do
conector permanece byte-idêntico ao c0e0;48testes de interação passaram31,70s
(46próprios, viewport máximo e gancho UI), com64casos alheios desse arquivo não
selecionados. Reusa a prova candidata real25→49/restore/export/online, sem repeti-la.
Nova CI/instalação da composição ainda obrigatórias; Windows isolado de172 passou
sem substituir a suíte completa. Fonte global atual muda a identidade de código;
isto não concede waiver de cache antigo ou nova qualificação financeira.
