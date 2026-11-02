# Retomar MT5X em um host limpo

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
idênticas reaproveitadas e dois consumidores recompilados). Wrapper59 compilado0/0;
Tester/replay desta fonte pendentes. Versão anterior não promove a nova por inferência.

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
Nenhum PLAY nem autorização DEMO transferida. Página privada canônica seq34:
232 arquivos/cinco bancos e seis sessões concluídas restaurados da nuvem, todos
os hashes/contagens de TODAS as tabelas conferidos; sequência então ativa excluída
e no próximo delta. Credenciais/contas/configurações/logs brutos não integram o
resgate. Fatos por fonte em E14_COMMON_POSITION_20261102.json. Pilar segue em
execução: TP R pós-fill, recuperação/capacidade/pendentes, SA-01, orçamento
ativo/grupo/plano, gradientes, probabilístico e aceites DEMO/netting/Windows físico.
