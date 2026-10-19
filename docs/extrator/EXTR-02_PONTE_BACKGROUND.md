# EXTR-02 — Ponte em segundo plano

Recorte solicitado pelo proprietário após EXTR-01. Uma tarefa por PR.

O start anterior abria a porta, mas a ponte não permanecia após a célula iniciadora.
O gerenciador usava nohup no grupo do iniciador e stop usava pkill por nome global.
A causa interna da saída nativa anterior não foi comprovada; a regressão reproduz
SIGTERM no grupo iniciador (que nohup não isola), sem sinalizar grupos externos.

Mantida a entrada scripts/mt5-bridge.sh e seus comandos. O ciclo de vida foi movido
para bootstrap.bridge, só biblioteca padrão, reutilizando paths/display existentes.
Popen recebe stdin nulo, log próprio e start_new_session=True. Não há daemon/store
paralelo, dependência nova, serviço de boot, credencial ou configuração nova.

O PID numérico continua em logs/mt5_bridge.pid, com identidade adjacente em JSON:
boot, instante de nascimento, usuário e token de posse no ambiente do filho.
Uma trava exclusiva impede starts concorrentes. Start repetido preserva o PID;
porta alheia/registro legado sem prova não é adotado. Stop usa pidfd, confere a
identidade depois de abrir o handle e sinaliza somente aquela instância. Se o
build Python não expõe a função, usa a mesma API da libc via ctypes, sem perder
a associação ao processo. Sem suporte do Linux/libc, recusa o sinal com ação.
Wine, wineserver, terminal, Xvfb e outras sessões não são encerrados pelo stop.

A entrada muda para sua própria pasta antes de importar o módulo: executá-la a
partir de outro checkout não seleciona código antigo pelo diretório corrente.
Falhas têm mensagem em português, log e indicação de corrigir config/instalação.
Estado ativo exige porta acessível e processo de posse comprovada; parada retorna
3; erro/posse desconhecida retorna 1; ações idempotentes bem-sucedidas retornam 0.

## Verificação proporcional

Baseline 1468/18/3, saída 0, reaproveitada: 279 hashes de fontes/testes coincidem
com master ed81ca2, mesma venv/dependências existentes, nenhuma alteração de
instalação. Primeira regressão: 21 aprovados/3 falhas/4 erros por ausência da API
pidfd no build Python; corrigido pelo caminho libc. Rodada de integração:25
aprovados em7,55s. Após corrigir a seleção do checkout no lançador, os14 casos
do gerenciador passaram em3,70s. Provedores/executáveis Wine falsos, incluindo
saída do iniciador, início repetido/concorrente, porta alheia, PID legado/reutilizado,
falha de startup/log e erro acionável, estado parado e simulação Windows nativo.
ShellCheck integral dos scripts passou. A suíte global final é necessária porque
houve alteração de runtime e está registrada nos artefatos próprios.

## Prova Linux/Wine real

Configuração privada de teste: loopback127.0.0.1:18813, mesmo prefixo/terminal
existente, sem credenciais novas. O start retornou; outra execução confirmou status
ativo e repetição com o mesmo PID. Consulta pela aplicação MT5Connector, não por
outro extrator: EURUSD/M1,14/10/2026 10:00–10:10 UTC,11 barras únicas, conta
MetaQuotes-Demo/trade_mode0 comprovada antes/depois e negociação automática
inativa. Zero ordens/replay, banco principal sem escrita, sem repetir a jornada
EXTR-01 completa. O primeiro roteiro passou datas nas posições erradas da API;
foi preservado e corrigido somente para argumentos nomeados start_dt/end_dt.

Status0, start repetido0, stop0, status3, stop repetido0, start0, status0,
start repetido0. O processo posterior ao stop tem novo PID; repetições do mesmo
start conservam identidade. Um terminal preexistente conservou PID/nascimento.
A prova final continua acessível após o roteiro terminar. Logs/identidades/bases
locais em logs/extr-02-ponte-background/, nunca versionados.

## Limites e continuidade

Não instala inicialização automática no boot/login nem política de reinício.
SIGHUP/SIGTERM do grupo iniciador é distinto de parada explícita, reboot ou
política externa de encerramento. Registro PID antigo sem identidade é recusado:
conferir a instância original e encerrá-la pelo responsável antes de migrar;
nunca usar killall/pkill nem trocar a conta para liberar a porta.
O listener observado pelo Linux no Wine pode pertencer ao wineserver: não o
usar como PID da ponte. O registro é do filho próprio criado pelo gerenciador.
Windows usa a conexão nativa; Windows físico/MT5 permanece pendência específica.
Próxima capacidade elegível a conferir: T18.1, registry canônico, agora com suas
dependências T13.2/T13.3 integradas; reaproveitar a origem já existente.
