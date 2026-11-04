# Prompt de continuidade em outro host

Você vai retomar o projeto **Misael-art/MISA_MT5-Extra-o-de-Dados** num host limpo.
Não há acesso ao computador antigo **nem ao SDCard**. Não dependa de caminhos
locais dos chats anteriores ou de caches de execução. Código/contexto público
estão no GitHub; o banco e contexto privado estão na conta ChatGPT do proprietário,
na página **MT5X Continuidade e recuperação em novo host 24 outubro 2026**.

Primeiro obtenha a branch `codex/novo-host-20261024` do repositório App e leia
`CONTINUAR_AQUI.md`, `AGENTS.md`, `docs/repasse/NOVO_HOST_20261024.md`, o manifesto
de fontes da mesma pasta, `docs/continuidade.md`, ROADMAP e PLANO_DE_TRABALHO.
App de referência anterior:65e621a9183e5231fe6bd42b1cc32f178ef2ef64, PR211;
Framework:HPR75/b730ed4a8df74f5f20eb6e289798d0232c370994;
Safety:3988df13c467bd22941183f5546caad0500f9138. Não presumir integração em master.

Prepare o ambiente pelo `bootstrap-dev.sh --yes` (Linux) ou `bootstrap-dev.ps1 -Yes`
(Windows), baixado do GitHub por uma ferramenta disponível ou navegador. Ele
instala Git se necessário e delega Python/venv/config ao instalador oficial;
um `.venv`, três clones e worktrees separadas. Sem flags de aplicação é só plano.
Instalação nativa posterior por `--with-mt5` / `-WithMT5`; não operar REAL. Antes
de seguir, confira `state/setup.json`, dependências, doctor/checks e o runtime.
Se faltar autenticação ou permissão administrativa, use o mecanismo do sistema,
sem pedir/registrar senha no chat. Não reinstale ou apague o que já estiver correto.

Abra a página privada pela mesma conta; leia o manifesto e recupere todas as
partes pelas referências persistentes. Pode usar Pages `read_page_reference` e
materializar o texto em UTF-8 sem BOM, evitando despejar base64 no chat. O helper
aceita um LF final do executor apenas se recuperar o SHA canônico. Use
`scripts/recover_cloud_backup.py` para conferir, recompor e extrair numa pasta
nova. Verifique hashes,48 tabelas/contagens do snapshot e histórico; só depois
configure a cópia de trabalho. Sem acesso aos anexos, não declarar dados
recuperados. Nunca publicar banco/configuração privada ou OAuth no GitHub.

O **Laboratório continua incompleto e teve aceite funcional reaberto** pelo teste
do proprietário. Todos os pontos estão em
`docs/propostas/LAB_SUPER_ATR_PENDENCIAS_20261024.md`:
catálogo indicador×indicador/alvo real completo, Séries/confluências vazias,
cabeçalho Horários, cores/legendas/rótulos, diferentes estratégias/robôs no mesmo
ativo, mesmo preset entre ativos, estratégia entre TFs com previsão distinguida
de correlação, bancada com indicadores e ferramentas, presets/SDK/atalho.

Há frentes independentes: agente Laboratório (LAB-01…10), agente Expert Super ATR
com confirmação entre TFs (SA-01), e agente de **Dados já existente**, dono do
cálculo e persistência/coluna Super ATR (SA-02). Leia os prompts específicos em
`docs/repasse/PROMPT_LABORATORIO_GRAFICOS_20261024.md` e
`PROMPT_EXPERT_SUPER_ATR_MTF_20261024.md`. Não iniciar os três sobre os mesmos
arquivos. Confirme a posse atual e avance na frente atribuída, sem roubar o WIP.

Prioridade inicial do integrador: demonstrar recuperação do ambiente e dados,
então reproduzir LAB-02/03 pelo atalho e integrar catálogo real LAB-01. Expert
congela contrato e testa causalidade em paralelo. Preservar a matemática/catraca/
diagonais e memória G19.9/13/15/16, os resultados negativos e os gates de risco.
Seis perfis automáticos estavam bloqueados por pin/closure: não liberar por
atalho nem inferir lucro a partir de associação acima80%.

Uma tarefa por PR, branch `codex/`, worktree própria, testes antes/depois, doctor,
CI por HEAD e limites honestos. Sem reset/clean de trabalho alheio, novo executor,
scheduler ou store paralelo. Arquivos centrais/pins/SDK e preview têm composição
serial por Git de commits revisados. Cada avanço testável precisa ser publicado
em commit/push/PR e disponível no **atalho central e menu**, com runtime identificado
e retorno preservado. Um teste por fixture não encerra falha real do usuário.

Use recursos com orçamento: um ambiente, artefatos identificados, cancelamento e
retenção existentes. Limpe apenas seus temporários sem consumidores, preservando
fontes/dados/evidências e WIP. Registre o que concluiu, o que não conseguiu provar
e o próximo passo concreto. Novo SO, Windows físico, Tester, replay e operação
financeira exigem suas próprias provas; não herdam aceite do computador antigo.
