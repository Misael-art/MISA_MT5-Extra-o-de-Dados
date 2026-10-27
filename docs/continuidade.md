## Novo host sem o disco antigo ou SD — 2026-10-24

O proprietário confirmou que **não levará o SDCard**. A retomada atual é pela
internet: [CONTINUAR_AQUI](../CONTINUAR_AQUI.md), [roteiro](repasse/NOVO_HOST_20261024.md)
e [prompt do novo agente](repasse/PROMPT_NOVO_HOST_20261024.md). O roteiro de21/10
é histórico; não usar sua conclusão dependente do SD como prova desta migração.

T20.30-HOST1 prepara um workspace, um ambiente e três clones identificados,
delegando aos instaladores existentes. Helpers só stdlib validam fontes e
recompõem o pacote privado sem sobrescrever dados. Banco atual consistente com48
tabelas, contexto e documentos posteriores têm preservação na página pessoal
ChatGPT **MT5X Continuidade e recuperação em novo host 24 outubro 2026**;
confirmar manifesto e recibos completos antes de declarar recuperação remota.
Não migrar venv/Wine/caches/locks/PIDs/OAuth; reautenticar no destino.

Os testes do host antigo não validam o novo SO. O próximo agente verifica
bootstrap/restore e só depois retoma LAB-01…10, SA-01 e a coordenação SA-02 com
Dados. Todos os diagnósticos do reteste e os gates/pins continuam pendentes
conforme a seção abaixo; esta tarefa não corrige os bugs do Laboratório.

## Reteste do proprietário — Laboratório incompleto, 2026-10-24

O teste real pelo atalho reportou catálogo restrito em Indicador × indicador/alvo,
falha de Séries/confluências, cabeçalho de Horários desalinhado e visuais antigos.
Também faltam comparações entre robôs/estratégias no mesmo ativo, preset idêntico
entre ativos, estratégia entre TFs com metodologia preditiva e bancada do ativo
com todos os indicadores qualificados e ferramentas de marcação.

[Mapa completo LAB-01…10/SA-01/02](propostas/LAB_SUPER_ATR_PENDENCIAS_20261024.md).
O status anterior passa a **implementação inicial incompleta, aceite reaberto**;
testes/capturas QA abaixo permanecem históricos, sem substituir o percurso do usuário.
Prompts independentes: [Laboratório](repasse/PROMPT_LABORATORIO_GRAFICOS_20261024.md)
e [Expert Super ATR multi-TF](repasse/PROMPT_EXPERT_SUPER_ATR_MTF_20261024.md).
O agente de Dados já existente mantém cálculo/persistência/coluna Super ATR.

Esta atualização é documental na branch `codex/lab-superatr-frentes`, sobre
Appaf717fa; não altera o runtime7c66fed instalado, dados, .env/config, venv,
ponte, fontes H, Safety, canal aceito ou WIP alheio. Cada agente usa worktree
própria e publica commit/PR/push; a composição da prévia/arquivos centrais é serial.
Próximo passo da frente Laboratório: reproduzir LAB-02/03 e consumir o catálogo
real em LAB-01. Expert começa pelo contrato SA-01 e testes causais, sem tomar SA-02.

## Laboratório de séries — histórico inicial T20.22-LAB1, 2026-10-24

[Repasse](repasse/T20_22_LABORATORIO_CONTINUIDADE_20261024.md),
[contrato](sdk/laboratorio_series.md) e
[evidência](evidencias/T20_22_LABORATORIO_SERIES_20261024.json).
Frente própria `codex/lab-multiframe`, PR210 sobre AppPR208/d1c1097.
Runtime7c66feda06eb publicado e instalado em `app-development`; atalho/menu
apontam para ele, com ambiente/configuração/dados/ponte/H/Safety preservados.
Novo serviço CLI/SDK/Tk e cinco visões, sem cálculo de backtest ao abrir gráficos.
52 definições e11 estratégias investigadas sobre cópia QA EURUSD M15/H1/H4.
Gerador Default/Manual conectado, mas os seis perfis encontrados na biblioteca
atual não correspondem às snapshots/pins aprovados: qualificar o pacote por
perfil (primeiro S09) é a próxima frente operacional, sem waiver financeiro.

Baseline92; seleção28; Tk final16 (sobreposição). Ampla imprimiu3014 aprovados/
259 pulados em2246,91s; o processo devolveu143 após o resumo, sem causa estabelecida.
Não é declarado encerramento amplo verde. Verificação separada dos caminhos novos
devolveu0 com20 aprovados/15 pulados, somente MainThread e atexit normal. CI da
ponta segue própria; Windows físico e demais aceites operacionais continuam pendentes.
Doctor com ambiente compartilhado20/1opcional/0. Aplicação instalada aberta offline
com DB privado; nenhuma sessão/ordem real MT5 por esta tarefa.

Limpeza própria liberou1.485.367.403 bytes lógicos; três diretórios pequenos de
metadados ainda abertos pelo `gvfsd-metadata` foram preservados, sem sinalizar o
serviço. Baseline padrão sem identificação conclusiva não foi apagada.
No SD, adicional organizado `Backups/MT5X/2026-10-24-laboratorio-series`: base QA
pequena e reutilizável,3snapshots, integridade/hashes conferidos e contexto; sem
credenciais/venv/Wine nem cópia da base oficial. Recolher WIPs das outras frentes
antes de formatar. Não houve merge em master por esta frente.

## AIA-02a — catálogo de Estudo via SDK, 2026-10-23

[Repasse fechado em2026-10-24](repasse/AIA_02A_CONTINUIDADE_20261024.md): código/PR208,
instalação, testes, preservação das outras frentes e fila para o próximo agente.

Frente própria `codex/ai-study-catalog`, base Appf4ca177 (AIA-01), sem alterar
raiz/WIP, dados/schema/produtores ou fontes H da frente do painel. [Contrato](sdk/study_cli.md).
Reutiliza o catálogo/fábrica da closure HPR75/b730 e os gates/ownership da sessão.

[QA por hash](evidencias/AIA_02A_ESTUDO_CLI_20261023.json):108 dirigidos/176 regressão (sobrepostos),
38 indicadores em102 pedidos CLI reais/DEMO, readback e remoção, PNG1280×800
inspecionado; carteira inalterada/sem ordens, MetaEditor0/0, doctor20/1opcional/0.
Corrida do auxiliar M5 na limpeza inicial observada e resolvida aguardando o
marcador do host; nenhuma fonte H editada. Dois perfis próprios removidos após
reconciliação, evidências preservadas. Baseline amplo congelado:2966/251; após a mudança:2994/251;
0falhas, cortes separados na evidência. CI da implementação/documentação tem
estado próprio, sem inferência de Windows físico.

CI da AIA-01/f4ca177: Linux, PostgreSQL, instaladores e lint aprovados; Windows
abortou por timeout60s em `test_bd18a_query::test_pagination_is_stable_and_cursor_is_bound_to_selection`
na preparação da fixture (`historical_extractor` → `database_manager` → commit SQLite),
antes do aceite do SDK. Job120748686535/run40052714594; diagnóstico registrado,
sem atribuir causa ao SDK, afrouxar teste ou editar a frente de dados. Correção/
qualificação dessa fronteira deve preservar sua posse e ter evidência própria.

## Repasse final AIA-01

[Continuidade autônoma do SDK](repasse/AIA_01_CONTINUIDADE_20261023.md): código,
PRs, comandos, provas, preview/limpeza e pendências para outro host/agente.
Implementação instalada App56226ce/Hb730; documentos posteriores podem avançar a
ponta sem mudar o código instalado.100 dirigidos/2952 amplos, seleções e limites
separados no repasse; CI remoto tem conferência por HEAD.

## AIA-01 — SDK/workflow para agentes, 2026-10-23

Frente própria `codex/ai-workflows`, sem editar WIP da raiz/pilar de dados.
Base composta por Git: Appfe92ed13a731a39dc03a69ba31f7047b3f6f8c6f (BD19c1)
+60e8870a91bace69c712e00213aaa46522ba2ced (PR205), merge
1d5ce2296dfb48e884c0175dcca05aa7d4801885. Revisão do delta próprio contra essa base;
histórias preservadas e dependências separadas. Companion Hb730ed4a8df74f5f20eb6e289798d0232c370994,
[PR75](https://github.com/Misael-art/projeto-indicadores-mt5/pull/75), sobre HPR74/e29a2c6.

[Entrada para o próximo agente](agente.md): `commands` recursivo, `workflow methods`,
JSONL local `workflow serve`, manifestos `workflow run`, `sdk.open_workflow`.
Sessões e presets delegam a Session/Consent/PresetExecution/compiler/store existentes;
apresentação opt-in por protocolo próprio com IDs, ACK/timeout incerto, ownership,
PNG nativo e espera de callback `until=ready`. Sem novo executor/scheduler/cache/journal.
O painel7.11 só amplia API de apresentação; pin manual45e31481977d09f1 é aditivo.
Não reaproveitar pins dos perfis automáticos para a closure compartilhada alterada;
conservar seus snapshots de fonte/compilação já qualificados. Safety publicado intacto.

A primeira tentativa nativa foi recusada com ponte parada; o script existente foi
iniciado em X11 removendo WAYLAND_DISPLAY apenas do processo lançador. Sem alteração
de fontes/token/credenciais da ponte. Outra tentativa registrou processo running
sem Expert pronto: agora o ACK de prontidão distingue as fases. Uma preparação
mudou de monitor e foi recusada pelo Runtime existente; a QA usa anchor explícito.
Não converter esses casos em sucesso nem repetir efeitos sem recibo.

[PR207](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/207), base de revisão
`codex/ai-workflows-base` publicada. [Evidência](evidencias/AIA_01_SDK_AGENTES_20261023.json):
CLI real PLAY e REPLAY DEMO sem ordens; dez curvas/43 parâmetros lidos no PLAY,
configuração14/EMA/.5 também em M5/M15, EMA/ATR, capturas e recusas de escopo.
REPLAY usa captura declarada da janela própria pelo SO (1280×800), sem cliques;
IndicatorParameters indisponível nesse Tester é reportado sem qualificar inputs.
As três falhas globais iniciais foram timeouts; um foi reproduzido no Git da base
anterior à implementação. Não apagar/afrouxar testes nem alterar o pilar financeiro
para ocultar esses resultados. CI/performance global continuam gates próprios.

Foco atual é SDK/CLI. Indicador estratégico, sua série/rotina de armazenamento,
amadurecimento OMNI e estratégia operacional são próximos contratos separados,
ainda pendentes. Também pendentes: demais ferramentas de Estudo/terceiros e controle
do play/pause do Tester, qualificações Windows físico/financeiras/operacionais.

## G19.10 — resolução interna Wine, 2026-10-23

Branchcodex/play-replay-monitor-fit, baseApp2ca4101/SuperATR1.01. Retorno do
dock reproduziu contêiner3456×1458 com terminal1280×800; corrigido modo interno
na cópia privada antes do primeiro Wine. Baseline85/final126, doctor20/1/0 e
prova do método de produção3456×1458. PLAY3456×1458 e REPLAY1280×800
concluídos;128 testes finais, corridaBadWindow corrigida com prova real.
App/Hnovos de10curvas/cinzaclarea incorporados porGit, sem alterar esse recorte.
[Contrato/limites](sdk/play_replay_monitores.md#g1910--área-interna-no-retorno-do-dock).

## Super ATR — G19.9, 2026-10-23

Regras do proprietário registradas:20/SMA0, inicialmente1/2/3 e agora DEZ
curvas com multiplicadores1 a10, pontilhado
azul graduado, catraca e virada por cruzamento estrito da linha anterior.
Versão1.02 mantém diagonais, amplia para10 curvas e acrescenta o temaCinza Claro;
89H/checker161/compilação0/0, instalação/visual próprios concluídos em
Appf7c3371/H3b979cf1:10 tonsRGB/diagonais/cinza neutro, painel pausado.
FonteH3b979cf1/fpd377c812aafd0c5e, Appcodex/super-atr sobrede8830e5.
126 testesApp/88H/checker161 e compilação6281/Wine0/0; versão1.01 inspecionada
com diagonais e instalada na prévia. Prova visual composta com App0b2fda3/PR204
(monitor interno Wine), sem mudança nos runtimes de catálogo/pin destePR. [Teoria, uso e continuidade](sdk/super_atr.md). Nenhum
executor/risco/Expert, pilar de dados, Safety ou canal aceito alterado. Pin manual
novo exige closurecompleta; antigos preservados.50curvas/adaptação/Profit/Windows
físico/aceite financeiro próprios. A correção anteriorPR195 tem seis checks finais
verdes e continua separada deste incremento.

## Correção de monitor PLAY/REPLAY — 2026-10-23

Branch `codex/play-replay-display` sobre G19/321272f0b24d. Detecta o monitor da
janela lançadora, com fallback se o dock foi retirado. REPLAY visível; PLAY optativo,
automáticos ocultos por padrão e painel manual reconhecido visível. Wine visual
isolado da ponte, com cópias temporárias removidas após encerramento confirmado.
Replay desenhado/avançando em1280×800 real e1920×1080 virtual;160 testes dirigidos,
doctor20/1/0. PLAY manual DEMO visível e oculto com telemetria/carteira inalterada,
sem clicar ordens; primeira leitura sem API conectada foi recusada antes da execução.
[Contrato/evidência/retomada](sdk/play_replay_monitores.md). WIP, frente de dados,
HADOUKEN/Safety e referência aceita preservados. CI, Windows físico e retorno do
dock têm conferências próprias; não é aceite financeiro ou operacional amplo.

## Prévia disponível pelo atalho habitual — 2026-10-21

Solicitação do proprietário: todo avanço testável acessível para teste.
Composição local172→177 + correção visual182 + documentação178→181 em branch
própria, com histórico preservado. Os dois atalhos passam a usar `app-development`;
`app-current` aceito e dados/WIP preservados. [Escopo e atualização](TESTAR_DESENVOLVIMENTO.md).
Operação manual do replay e falha Windows177 mantêm seus estados; prévia não é
aprovação das PRs ou implementação das funcionalidades apenas planejadas.

## Nova conferência do host — 2026-10-21

[Continuidade validada](repasse/RETOMADA_VALIDADA_20261021.md): clones independentes,
SD e restauração em leitura conferidos; ambiente preservado funcional, 56 testes
e doctor20/1/0. Novo SO não comprovado. Próximo recorte autorizado: exibição do
painel no REPLAY; melhorias de seleção/lotes e comparação entram no roadmap.

## Recuperação após reinstalação — corte de 2026-10-21

O [roteiro independente do host](repasse/RECUPERACAO_POS_FORMATACAO_20261021.md)
registra clones novos, pins, dependências, WIP e dados privados. Três clones de
rede passaram fsck; commits/HEADs locais estavam alcançáveis no GitHub; cinco
árvores WIP reais foram reconstruídas por bundle/patch/arquivos com hashes iguais.
A restauração privada SQLite preservou43 tabelas e o histórico foi reaberto pelo
store importado do clone novo, sem nova execução. Isso foi no Linux atual, não
no novo SO. Após confirmação do proprietário, o complemento privado de142 MB foi
copiado ao SD preservado: hashes relidos,43 tabelas restauradas e histórico/journal
reabertos sem execução. `ready_to_format=true` no [estado](repasse/RECUPERACAO_ESTADO_20261021.json)
vale para desenvolvimento no corte declarado; novo SO/aceite operacional pendentes.
Wine, ambientes e caches são recriados; o pacote amplo16,5 GB não foi migrado.

A fila HADOUKEN49–60 também foi localizada e conferida no GitHub:12PRs abertas,
dois checks aprovados cada no snapshot, sem integração/replay/consumidor Python
ou revisão desta frente. App172–176 tem seis checks;177 e178 têm falha Windows
analítica preservada. O mapa de retomada distingue essas filas e a instalação171.
O verificador `scripts/verify_recovery.py` usa só stdlib, confere o pacote privado
sem extrair/executar e não anuncia por si só mídia externa/restore/aceite operacional.

## Repasse do integrador e limpeza segura — 2026-10-21

O [repasse consolidado](repasse/INTEGRACAO_20261021.md) registra entregas, versões,
evidências, fila e próximos passos; o [índice da raiz](../RETOMADA_AGENTE_20261021.md)
permite localizá-lo sem a conversa. O corte instalado é AppPR171/ac7af2c3,
Hec550d77 e Safety3988df13/v1.8.0. PRs172–177 continuam publicadas, sem merge ou
instalação; atualizar checks de cada ponta e integrar serialmente se o trabalho
for retomado. Esta publicação documental não executa essa fila.

Foram compactados/verificados10bancos privados, aposentadas19cópias nativas
privadas com evidência retida e removidos76diretórios de cache gerado. O aumento
observado de espaço disponível foi aproximadamente3,4GB; a soma lógica dos
blocos retirados não equivale a bytes físicos recuperados em Btrfs. Dados,
configurações, backups do proprietário, WIP, ambiente e worktrees foram
preservados. [Escopo e medições](evidencias/REPASSE_LIMPEZA_20261021.json).

As seções abaixo são registros históricos das respectivas pontas e podem dizer
“em revisão” de entregas depois integradas. O repasse datado identifica o corte
mais recente sem apagar as falhas anteriores. Sincronização privada/Tester,
replay/aceites operacionais, Windows físico e demais gates continuam pendentes.

## Painel manual no REPLAY — correção em validação (2026-10-21)

Compilação e replay usam o mesmo DISPLAY no Linux/Wine; painel visível.
Código final com 84 testes e doctor20/1/0. Abas manuais não passaram no ensaio:
polling de botões e ocultação do fundo não resolveram; experiências retiradas
do runtime final. PR draft, sem instalação até o aceite manual. [Contrato](sdk/replay_painel_manual.md).
Base do proprietário, risco/executor e Windows físico mantêm seus limites.

## T19.34 — Resumo e erro do catálogo legíveis, em revisão (2026-10-21)

Complemento sobre176/e7656da, somente o rótulo do resumo/erro:1178px requisitados
em823px reproduziram o corte. Busca/botões passaram e permaneceram inalterados.
O resumo acompanha a largura real, com quebra à esquerda; conteúdo, modelos,
filtros e seleção preservados.11Tk finais aprovados159,10s; dois temas/quatro
larguras/fontes e erros longos, mais interação com contexto/página/cards. Histórico
privado reaberto sem cálculo; capturas inspecionadas, sem banco owner ou MT5.
[Contrato](sdk/cabecalho_catalogo.md). App171/ac7af2c segue instalado neste snapshot;
checks/integração/instalação serial172–176 e deste complemento continuam próprios.
Nenhum aceite de T19.34 ampla/Windows físico/financeiro/nativo/live.

## T19.34 — Navegação e mensagens com fonte ampliada, em revisão (2026-10-21)

Nova candidata sobre175/e174cc7: seletor Área substitui nomes comprimidos e mantém
as páginas/seleção existentes; status/base se reorganizam e Detalhes expõe texto
integral selecionável, com fonte ampliada e rolagem. Sem cálculo financeiro ou
alteração de dados/configuração/native.10Tk finais passaram131,60s;6 dirigidos do
entorno passaram52,37s (sobreposição); doctor20/1opcional/0. Histórico privado
reaberto, sem novo cálculo ou banco do proprietário; capturas inspecionadas.
[Contrato e limites](sdk/navegacao_adaptavel.md).

Permanece instalado171/ac7af2c.172bfc teve3 visuais Windows isolados aprovados45,06s,
mas a suíte parou antes da UI por timeout num commit SQLite de materialização
inalterada. Teste dirigido local passou16,84s; causa do runner não comprovada.
Reexecução seletiva do job falho no mesmo commit foi solicitada, preservando os
cinco checks aprovados. Nenhum172–175 foi integrado/instalado neste snapshot.
A fila serial e os seis checks próprios continuam obrigatórios; Windows físico,
T19.34 ampla e aceite nativo/financeiro/operacional não são conferidos.

## Recursos — Cópias disponíveis com leitura de pastas incompleta, em revisão (2026-10-21)

A prova de recuperação encontrou a aba ocupada na varredura de pastas, antes de
listar cópias. O corretivo limita cooperativamente a leitura opcional de tamanhos
e arquivos antigos. O relatório do banco e os backups permanecem disponíveis;
tamanho desconhecido fica indisponível e lista parcial não libera limpeza.
Não há novo scheduler, cache, lock, orçamento financeiro ou alteração de SQL/schema.

Baseline45 casos passaram; quatro regressões novas falharam antes do ajuste.
Seleção49 passou59,05s; mais24 proteções de retenção passaram12,65s. Após compor
o layout66571c4, três casos de interação UI passaram38,09s; cinco próprios finais
passaram11,81s, incluindo interrupção após um arquivo. Há sobreposição entre seleções.

Na candidata, UI/SQLite privado criou backup verificado455,6MB, preservou a cópia
anterior, restaurou e exportou12.405barras com todas as colunas conferidas. Mesmo
run preservado, três cópias reabertas e limpeza parcial bloqueada. Um prazo inicial
curto do ensaio foi registrado como incompleto; retomada concluiu auditoria e
verificação. Banco original aberto só para leitura, configuração/preferências intactas.
Não qualifica restauração no banco do proprietário, PG, Windows físico ou operação.

Fonte candidata ainda sem integração/instalação: Appac7af2c/Hec550/Safety1.8.0
permanece ativo. PR172/layout1b0ba510 segue em novo CI após a falha Windows de66571c4;
este corretivo conserva a ordem serial e exige seus próprios seis checks finais.
O prazo é cooperativo entre chamadas de filesystem, sem teto absoluto para I/O,
SQL ou backup/restauração. [Contrato e evidência](sdk/recursos_leitura_parcial.md).

## T19.34 — Contexto do robô sem corte horizontal, em revisão (2026-10-21)

Nova evidência: CI39589919928/66571c4 também falhou no ensaio144DPI em900×620,
com largura835/892válida, rótulo desmapeado e interseção563..293 (2.753passaram/
85skips). O cálculo de alcance usava coordenadas globais do rótulo antes de ele
ser mapeado. O ensaio agora revela o viewport por posições internas dos ancestrais
e mede coordenadas globais após mapear; prazo/asserts/temas preservados.12Tk/SDK
passaram32,04s; runtime inalterado, novo CI Windows obrigatório. Ambas as falhas
permanecem preservadas; não há merge/instalação172. PR173 depende desta correção.

PR172 permanece sem merge: CI39581746127 teve uma falha Windows no novo ensaio
144DPI (`winfo_ismapped=0`), com2.753 aprovados/85skips. Linux passou2.619/220skips;
PG55. O ensaio passou a percorrer as duas rolagens e aguardar o mapeamento e
a geometria por prazo limitado, mantendo os asserts e acrescentando visibilidade
real.12 testes Tk/SDK passaram localmente; runtime visual/financeiro inalterado.
Novo CI da correção obrigatório; a causa exata do evento no runner continua
indeterminada, sem alegação de aceite Windows físico.

Recorte isolado sobre T19.24: rótulos do detalhe usam sua largura real, em vez do
cartão externo. Regressão reproduziu688px de quebra em594px disponíveis; ajuste
aprovado em dois temas/escalas96/144 simuladas e1920/1280/900/820, incluindo900×620,
com texto longo/resize/teclado. Três testes anteriores reaproveitados no mesmo
runtime, sem alteração de métricas, seleção ou conteúdo. Prova Tk/Xvfb reabre o
mesmo histórico privado sem recalcular; capturas/medidas preservadas em evidência.
Não conclui T19.34 nem qualifica gráficos/fluxos futuros/DPI ou Windows físicos.
[Contrato](sdk/robot_context_layout.md).

Corretivo SDK1 de T19.24d7020ac composto por Git sobre esta frente, sem conflitos
ou alteração do layout já validado. Os quinze argumentos públicos permanecem;
SDK+matriz visual+exibição da composição passaram13 testes em29,68s após compor.
A nova ponta exige CI próprio; a execução anterior permanece como evidência do
snapshot anterior, sem conceder aceite ao SDK que vazava o argumento interno.

T20.12/PR170 foi integrado em86ae848 e T19.24/PR171 emac7af2c, com seis checks
aprovados nas pontas833de99/d7020ac. T19.24: Linux2.619/218skips, Windows2.752/85skips,
PG55. Appac7af2c/Hec550/Safety1.8.0 está instalado, com configurações/preferências,
atalho, ponte e terminal preservados; exposição0/0 antes e depois. Avaliação
registrada na instalação: Madrid408barras/3operações/111,0875, reabertura do mesmo
run e nova consulta/cache auditadas, com livro numericamente conciliado; isso não
qualifica livro transacional, fonte, reserva final ou estatística. Interface reabriu
o resultado sem novo cálculo. T19.34 aguarda sua própria correção/CI antes da troca.
Na instalação anterior,
a UI local reabriu/exportou12.405 barras EURUSD/H1; CSV completo e backup existente
conferidos, sem extrair/atualizar na corretora nem restaurar o banco do proprietário.

As seções seguintes preservam os snapshots anteriores de revisão; o estado
integrado/instalado acima prevalece sobre suas indicações históricas de pendência.

## T19.24 — Uma composição Python no backtest completo, em revisão (2026-10-21)

Corretivo do SDK: CI39576824302 teve uma falha de assinatura pública (2.617 testes
passaram/218 skips). O reexport direto de `run_python` expunha o argumento interno
`composition`. Wrapper explícito agora conserva os quinze argumentos/defaults do
SDK1, sem alterar `SURFACE`/`API_VERSION` ou o produtor interno. Nova prova confere
encaminhamento legado; SDK e composição passaram24 testes em27,84s. O CI final
da ponta corrigida permanece obrigatório; falha anterior preservada. Composição
continua opt-in do serviço/CLI, sem promoção silenciosa ao SDK público.

Opt-in `composition_v1` usa o BaseAdapter e o Backtester existentes, com políticas,
filtros, risco/capitalização e identidade da seleção persistidos no run. Saída
antecipada, sinais seguintes, cota e lotes são recalculados na sequência completa;
não é filtragem posterior de ledger. O padrão BASE do T20.12 mantém suas restrições.
A tela de Backtest exibe escolhas do resultado gravado sem recalcular preços/lotes.
Nenhum editor, motor, scheduler/cache paralelo ou capacidade nativa foi criado.

Validação local:66 testes de serviço/CLI/composição e125 regressões pertinentes;
seis verificações de apresentação/Tk/identidade e25 dirigidos após compor o
corretivo de identidade833de99, com sobreposição documentada. Uma
prova CLI em cópia histórica isolada Madrid408barras/3operações/111,0875 reabriu o
mesmo run e auditou nova consulta/cache; tela Tk/Xvfb conferida visualmente.
PG do novo consumidor, CI/revisão/instalação correspondentes continuam pendentes.
[Contrato, uso e limites](sdk/composed_backtest.md).

T19.24 permanece parcial: lote/matriz, carteira/margem agregada e modos sem adaptador
não foram entregues. `run_recorded` não qualifica livro/reserva/estatística. App169
1612301/Hec550/Safety1.8.0 segue instalado; T20.12/PR170 ainda não foi instalado:
CI Linux detectou divergência de assinatura na retomada após morte de processo;
corretivo833de99 publicado e composto por Git nesta frente.86 testes de identidade/
serviço e dois casos sob cobertura passaram; novo CI obrigatório em curso antes
do merge. A falha e a evidência anterior foram preservadas.
Sem nova sondagem privada/MT5; sincronização e aceites nativos continuam pendentes.

## T20.12 — Registro de uma avaliação explícita, em revisão (2026-10-21)

PR170 ainda sem merge/instalação: CI39569538013 teve uma falha Linux na retomada
após morte do processo (2.597 passaram/216 skips); PostgreSQL, lint e instaladores
passaram nessa ponta. O integrador reproduziu o erro com internação de uma constante
textual sem alteração de código/valor e corrigiu somente a serialização da assinatura
de código carregado (`marshal` protocolo0). A regressão preserva a morte real do
processo e a mesma reserva, agora com estados de internação diferentes entre pai
e filho. Checks finais da nova ponta continuam obrigatórios; nenhuma guarda de
checkpoint/identidade foi afrouxada e nenhum checkpoint antigo foi migrado.
O corretivo passou86 testes focados (52 serviço/schema/CLI,31 identidade/cache e3
processos/seeds; contagens sobrepostas com provas anteriores); PG2 depende do novo CI.

O próximo recorte usa um único registro de protocolo, alternativa, pedido deliberado,
execução técnica e acesso aos dados. Delega uma avaliação Python BASE ao `run_python`
e ao `Budget` existentes, com snapshots imutáveis escolhidos no protocolo. Aceita
somente `compound=True`; filtros e modos sem mapeamento fiel são recusados e registrados.
O orçamento tem escopo explícito `evaluation_request`, incluindo retentativas daquele
pedido; não constitui teto agregado de campanha nem busca automática.

Run, vínculo ao manifesto e journal compartilham a transação existente. O estado
`run_recorded` não afirma conciliação do livro, cuja importação ocorre depois. Cache
e retomada não transformam a mesma execução em amostras independentes; um novo pedido
deliberado continua auditado. Consulta à reserva deve ser durável antes de calcular,
reutilizar ou revelar resultado, sem apagar exposição ao mudar protocolo. Dados já
explorados ou de exposição desconhecida não recebem status de reserva intocada;
`qualified_final_holdout=False` permanece explícito neste inicial.

T20.12 completa ainda depende dos demais consumidores; T20.13, T19.27/28 e seleção de
Defensivo/Fator de Lucro/Ultra permanecem pendentes. Não há novo scheduler, cache,
motor financeiro, UI ou caminho nativo neste recorte.

Núcleo `97c3baf` composto por Git sobre master `1612301`, sem conflitos. Seus12hashes
foram conferidos;52testes próprios finais e152regressões legadas compatíveis são
reaproveitados, com sobreposição documentada. PostgreSQL2 ainda exige o CI final.
Uma prova adicional da CLI composta, em cópia isolada do histórico legado, registrou
Madrid BASE/risco0,25%:408barras/4operações/154,89; reabriu o mesmo pedido e auditou
um segundo pedido que reutilizou o mesmo run, sem nova execução/amostra. É QA de
persistência, sem qualificação de fonte, desempenho ou livro. [Contrato](sdk/experiments.md).

Estado instalado: App169 `1612301` com H `ec550d7`, Safety1.8.0 preservada. S10,
VWAP, Madrid e HA/HiLo têm Default/Manual/exportação e resultados reabertos na
aplicação instalada. HA/HiLo preserva528barras/24operações/−382,223; Madrid preserva
528barras/10operações/105,10. Configuração, atalho, ponte e principal preservados.
O registro T20.12 ainda não está instalado. Sincronização privada/Tester/replay/painel,
Windows físico e aceite ao vivo continuam pendentes; nenhum Expert novo foi carregado
por consequência dessas instalações.

## T19.40 — HA/HiLo Python e consumidor em revisão, 2026-10-21

Núcleo515a3b9 composto por Git; HA/HiLo1.20/fp5dad3bbabab0343f/H6b4f integrado no
HADOUKEN PR48. Perfil fechado10inputs, Default/Manual/três parâmetros e risco<=0,5%.
TP ausente exclusivo deste perfil, SL obrigatório; propostas por janela e intenção
reconstruída não comprovam fechamento/reversão. Variante com SL inicial fixo, sem
trailing/sempre posicionado. [Contrato e limites](sdk/ha_hilo_nativo.md).

S10/App166 d6bb557 e VWAP/App1679579a62 passaram checks finais e foram instalados
com H9334/H11bb correspondentes; configuração/atalho/ponte195627/principal preservados,
posições/ordens0 antes/depois. S10Manual rev2 e histórico528barras/5trades/−819,29
reaberto na UI instalada. Madrid/App168 em revisão/CI, ainda não instalado. HA/HiLo
histórico isolado528barras/24trades/−382,223 conciliado, sem aceite financeiro/nativo.
Sincronização privada/painel/Tester/replay/Windows físico/live continuam pendentes.

## T19.39 — Madrid Python e consumidor em revisão, 2026-10-21

Núcleo111ae036 composto por Git; fonte Madrid1.20/fp870c0f8db71c3d5a integrada no
HADOUKEN PR47. Perfil fechado de13 inputs, Default/Manual e orçamento até0,5%;
leitor confere propostas por janela observada, inclusive guards sem sinal e TP
escolhido inválido. TP fixo e reset por gatilho são variantes, sem FC/FV persistente.
Contrato/evidência: [Madrid nativo](sdk/madrid_nativo.md). Histórico Python isolado
EURUSD/H1:528 barras/10 operações/105,10, reaberto e conciliado; não qualifica
financeiramente nem substitui Tester. Cache genérico Python pode reconhecer
identidade completa, sem novo aceite operacional. S10/VWAP conservam sua fila
revisão/CI/instalação; sincronização privada e aceites nativos continuam pendentes.

## T19.36 — VWAP Python e consumidor em revisão, 2026-10-21

Fundação48feaf8 composta por Git, perfil VWAP1.20/fp7139d746ab6b2c1d ligado a
Hdc789710 (PR46 integrada11bbbd76), Default/Manual/exportação e conferência de
duas janelas OHLC/volumes/fonte. 152 testes de integração Tk/Xvfb,110 novos Python e
240 regressões reutilizados, doctor20/1/0 e duas compilações0/0. Histórico real copiado
EURUSD/M15setembro: Auto recusado por capacidade real_volume desconhecida; seleção
explícita ticks teve2112 barras/0operações/resultado0, salvo e reaberto. Não é aceite
de execução. Contrato e limites em [VWAP nativo](sdk/vwap_bands_nativo.md).

App165 integrado328cae e instalado com H741058/Safety1.8.0; dois Manuals reais S09/PB
reabertos/exportados, principal/ponte preservados, sem novo binário carregado. App166
S10b4bdf24 (correção após CIce09fe6 recusado) permanece sob novo CI final; não confundir fonte preparada com release ativa.
VWAP também aguarda revisão/CI e instalação do conjunto. Tester/replay real, painel
visual e live DEMO seguem dependentes da sincronização privada não estabelecida;
nenhuma nova sondagem sem pré-condição observável. Windows físico/cache pendentes.

## T19.38 — consumidor S10 em revisão, 2026-10-21

CI39548192469/ce09fe6 recusado: fixtures antigas de contagem e caso de re-simulação
sem especificação/contexto de Move Guard. Correção tipa a fixture, mantém lista
explícita do catálogo e recusa projeção silenciosa dessa gestão no re-simulador;
58 focados Tk/Xvfb e14 re-sim aprovados. Novo CI final/revisão exigidos antes de
integrar/instalar. Backtester, MQL/pin e protocolos nativos não mudaram.

S10 Python/Move Guard compartilhado composto por Git a partir de d4b084de, com perfil
nativo fechado1.30/fp26545f544c784c39 e Default/Manual/editor/exportação. H#30 e H#45
integradas; S09/Pullback/pins anteriores preservados. [Contrato e limites](sdk/stoch_ema9_nativo.md).
Cinco operações Python em528 barras reais de cópia consistente: −819,29; livro/reabertura
conciliados, sem aprovação financeira. Fonte/binário/pacote não substituem inputs carregados.
CI final, revisão do consumidor, instalação e Tester/replay/visual permanecem próprios.
Não repetir sondagem privada de sincronização sem nova hipótese/estado. Cache/live/Windows
físico seguem pendentes; raiz/WIP/configurações preservados.

## PM12 — consumidores nativos, 2026-10-21

H#44/H1024cee0 acrescenta painel próprio S09 1.90/Pullback Nativo1.10, com as mesmas
instâncias de risco/executor. Pausa conserva gestão; entrada continua revalidada e
congelamento não é destravado. Ambos compilados6276/Wine0/0; revisão técnica favorável,
aceite visual pendente. Novos perfis Python e pins preservam todos os perfis anteriores;
125 testes dirigidos/Tk aprovados e catálogo real44/23inputs conferido. Canvas padrão é
externo permitido pelo nome exato nesses novos perfis; qualquer header próprio ausente
bloqueia. Exportação estática não equivale a recibo de parâmetros carregados.
[Contrato e limites](sdk/painel_nativo.md). Tester-only, cache inelegível, ao vivo/Windows
físico e integração/instalação final continuam com seus critérios próprios.

## PM-12 — integração visual e distribuição, 2026-10-21

Fundações H32→36→37→38→39 integradas em288063fe. Move Guard40 integrado d94e11f8;
consumidor33 integrado422e1045, com52Tools/checker135/QA0erros0avisos e checks verdes.
Statuscaps0 e critério de trailing pós-pausa corrigidos, revisão técnica registrada.
O aceite nativo visual permanece pendente;26OK/3FAIL não visual continua reprovado.
Safety1.8 integrada3988df13: seis headers selecionados, nove arquivos de risco,
executor/contratos preservados; quinze arquivos idênticos à fonte H3005b85, smoke
standalone0erros/0avisos e dois testes do pacote. Aplicação confere todo o conjunto
antes da cópia e hashes quando declarados; baseline10→15 testes dirigidos aprovados.
[Contrato, evidências e limites](sdk/distribuicao_painel.md).

API reconectada ao principal6276 sem restart, DEMO/conectada e identidade preservada.
Duas hipóteses delimitadas de preparação do perfil não fecharam a sincronização:
autorização sem confirmação nativa; nenhum Tester/ordem iniciado, processos próprios
encerrados. Não repetir sondagem sem mudança observável. S09/Pullback/adaptador
continuam Tester-only; cache inelegível, ao vivo DEMO e Windows físico próprios.
M164 integrada710976b com seis checks verdes, incluindo Windows. Atalho do proprietário
ativado para app710/H422/Safety3988(1.8), com backup de configuração/atalho/includes e
base. Quinze hashes instalados conferidos; segunda cópia0. UI real reabriu a mesma base,
mostrou18 robôs e EMA+ATR da revisão fixada com12inputs/Conservador, sem erro de callback.
Ponte mantida no mesmo PID/identidade/token; registro de posse transferido sob lock para
a release ativa, sem restart. Nenhum EA novo carregado ou operação iniciado por isso.
Raízes WIP/configurações financeiras preservadas. H44 e perfis desta revisão só serão
ativados juntos após checks; até lá S09/Pullback anteriores continuam na instalação.

## T19.57 — aceite funcional e checks concluídos, 2026-10-20

Recorte entregue na PR#150. Seis checks da lógica7e1b aprovados, incluindo Windows,
no run existente39412714724; revisão final concluída. Vínculo nativo final8a08 com
H89139c8/fingerprint60bf18181739612d/Safety1.7 verificado por39focados e UI/Tester
reais; deltas posteriores só documentais e merge Git da base sem mudança de arquivos.
H25 integrada47f05b1, M149 integrada08c1ad5. Cabe ao fechamento GitHub da PR150
registrar seu merge exato; a árvore candidata é conferida antes da integração.
`docs/evidencias/T19_57_ACEITE_20261020.json` reúne checks, fontes, evidência e limites.

Na UI: Manual nomeado configura lotes e até duas parciais, base inicial/saldo e BE
após confirmação. Agenda inviável é recusada, pedido não vira fill. Replay conferiu
2posições/3reduções/7fills/22inputs, saldo/custos/CSV/livro e reabertura. QA de reinit
não repetiu parcial confirmada/intenção pendente; NaN/zero/negativo recusados antes
do executor. Fonte final compilada0erros/avisos; seis legados também compilados.
Cache inelegível, S09 Tester. Pullback permanece bloqueado até perfil/aceite próprio;
ao vivo DEMO, restart de processo e Windows físico conservam aceites específicos.
Não repetir campanha histórica por rotina. Próximo recorte elegível: validar o
Pullback no perfil próprio com redução real confirmada e proteção do remanescente,
sem liberar por consequência do piloto S09; políticas temporais seguem dependências.

## Integrações registradas — T19.56 e fonte parcial, 2026-10-20

M#149 integrada em08c1ad5, fonteec398eb com seis checks obrigatórios aprovados;
H#24 já integrada ema310a5a. T19.56 inicial entregue.
H#25 integrada em47f05b1, fonte89139c8/fingerprint60bf18181739612d com checks,
compilações e prova consumidora final. M#150 recebeu master por merge Git6a87a2e,
árvore de arquivos inalterada/sem conflitos, e foi retargetada para master.
Sem cópia de arquivos do usuário nem nova suíte/workflow por integração documental.
M#150 aguarda somente Windows do run existente39412714724; cinco checks aprovados.
Não iniciar outro workflow; falha concreta exige diagnóstico pertinente antes do merge.
Registro `docs/evidencias/T19_56_57_INTEGRACOES_20261020.json`.
Raízes originais preservadas (HEAD/índice/status: M124entradas, H1, S0).
Pullback bloqueado; cache inelegível; S09 Tester, ao vivo DEMO/Windows físico próprios.

## T19.57 — prova final disponível, 2026-10-20

H#25/M#150: fonte H89139c8/fingerprint60bf18181739612d/Safety1.7,
M8a08e4c. UI/backtest/replay final com2posições/3reduções confirmadas/7fills/22inputs,
CSV/livro/custos/saldo e eventos iguais; consultas persistem após reabrir.
QA de reinit/pending após3→2 não reenviou nem alterou SL; NaN/zero/negativo recusados.
Isso não prova restart de SO/terminal ou Windows físico. Artefatos e patch QA em
`docs/evidencias/T19_57_PARCIAIS_20261020.json` e guia `docs/sdk/parciais_s09.md`.
Fonte final efetivamente ensaiada; antigo candidato0fce/7e71 guardado localmente.
162focados +14leitor estrito +39vínculo final;21Tools/115check_repo; doctor20OK/1aviso/0falhas.
H891 checks verdes; aguardar M7e1b existente (compatível: só literais de fonte até8a08).
Run M0fce obsoleto cancelado após correção concreta do leitor; nenhum workflow novo
para literais/documentação. M149 ainda aguarda seu Windows existente,5checks verdes.
Integrar pela ordem real M149, depois H25/M150 com revisão/checks; preservar raízes WIP.
Cache inelegível, S09 Tester, Pullback bloqueado, ao vivo DEMO/Windows físico próprios.

## T19.57 — preparação após T19.56, 2026-10-20

Corretivo148 integrado e prova de lotes concluída; H24 integrada e M149 aguarda check
Windows existente. Próxima árvore isolada prepara confirmação parcial/saldo:
S09 1.20, até duas etapas; agenda UI, volume/passo/saldo, intenção persistida antes
do envio, redução/histórico/ownership/proteção antes de confirmar ou solicitar BE.
Pendente/incerto congela tentativas, restart não repete fechamento. Fonte H5eb48a5,
fingerprint7e71e0039cfc87ef, Safety1.7. Defeito legado Python reproduzido nos dois
sentidos (meia unidade indivisível), corrigido; sem BE quando não existe redução.
Leitura de fills/custos por posição, sem contar parciais como vitórias. Compilação
S09 final privada0erros/avisos; cobertura nativa/legada e checks ainda pendentes.
Ver `docs/sdk/parciais_s09.md`. Não desbloquear Pullback; cache inelegível, S09 Tester,
ao vivo DEMO e Windows físico conservam seus aceites. Nenhuma campanha repetida.

## T19.56 — prova consumidora de lotes, 2026-10-20

M#148 integrada em5e3c443, fonte aa0d8fd com seis checks aprovados.
T19.56 em revisão H#24/M#149. Fontes isoladas H6120466/fingerprint8fcc5dfacc9118fa,
Mec398eb e Safety1.7. UI real: Default imutável, Manual nomeado, volume fixo/risco,
reabertura/exportação, recusa de volume acima do orçamento e consulta de eventos.
Backtest por risco, backtest fixo2,35 e replay fixo: duas posições encerradas em cada,
12inputs/quatro eventos, CSV/livro/saldo conciliados; fixo replay igual ao backtest.
Três resultados persistem após reabrir; PIDs próprios encerrados, ponte/principal
preservados. Sem repetir campanha histórica; valores são observações locais.
Registro `docs/evidencias/T19_56_LOTES_20261020.json` e contrato `docs/sdk/lotes_s09.md`.
Aguardar checks existentes M149, concluir revisão e integrar H24→M149.
Próxima T19.57: parcial/saldo, confirmação efetiva antes de BE e desbloqueio Pullback.
Cache inelegível; S09 somente Tester. Ao vivo DEMO/Windows físico com aceites próprios.

# T19.56 — lotes em preparação; corretivo #148 integrado — 2026-10-20

PR148 integrada em 5e3c443, fonte final aa0d8fd: seis checks obrigatórios SUCCESS,
revisão técnica concluída, árvore integrada inteira idêntica à testada. Produção
permanece igual à prova Wine b27da93: dois Testers sequenciais, PIDs privados
observados/encerrados, resultados reabertos no banco/UI, ponte e principal DEMO
preservados. A falha Windows foi timeout de teste agregado no commit SQLite;
três cenários independentes mantiveram dados/asserções/limite60s. Não repetir a
campanha histórica. [Integração](evidencias/T20_25_INTEGRACAO_20261020.json).

T19.56 inicial prepara lote fixo/por risco na UI, Manual/revisão/.set e eventos
nativos de volume/risco/margem. H24 está em rascunho com checks aprovados, fonte
6120466/fingerprint8fcc5dfacc9118fa, somente Tester DEMO. A aplicação preserva o
perfil1.00 e seus resultados, sem migrar parâmetros para1.10. Prévia indisponível
não cria dependência de IPC: dados reais continuam obrigatórios no Tester.
A prova de novos lotes em UI/backtest/replay com negócios permanece pendente;
registry/compilação não encerram a ficha. [Escopo](sdk/lotes_s09.md).

Próxima T19.57: corrigir a confirmação da parcial e contabilizar o saldo antes
de desbloquear Pullback. O exportador existente já agrega saídas/custos por
posição, sem contar cada parcial como nova operação. Cache nativo segue inelegível;
DEMO ao vivo e Windows físico mantêm aceites próprios. Trabalho/configuração/dados
locais preservados; não retomar lotes contra bibliotecas de branches divergentes.

# T20.25 — corretivo de encerramento do backtest em revisão — 2026-10-20

O primeiro percurso Manual histórico está concluído; preservar suas evidências,
sem repetir a campanha por rotina. A próxima capacidade proposta é T19.56 inicial
(lotes fixos/por risco, uma entrada, UI/preset/nativo), seguida da correção/validação
da parcial T19.57 antes de desbloquear Pullback. Cache qualificado não é dependência
desses comportamentos; cache nativo continua inelegível por identidade histórica ausente.
S09 permanece restrito ao Tester; execução ao vivo DEMO exige aceite próprio.

Os logs do teste manual de 20/10 distinguem dois problemas: lotes de 45/60 tentativas
com 27/36 recusas por fontes/includes diferentes dos executáveis instalados; depois,
cinco sessões recusadas porque o backtest Donchian anterior concluiu com
`resources_released=False`. O encerramento após relatório importado cobria apenas
replay. Este corretivo reutiliza `_stop_owned` também no backtest, antes de liberar
contexto/posse, sem alterar livro, cálculos, proteções ou processos de outra instalação.
Se um filho sobreviver, o contexto continua protegido e o próximo Tester é recusado.

Baseline dirigido: 48 aprovados. Dois casos novos reproduziram a falha antes da
correção (card/preset); depois, 127 testes pertinentes e dois casos adicionais de
filho sobrevivente passaram. Doctor sem probe: 20 OK, um aviso de pandas-ta opcional,
nenhuma falha. Revisão local concluída. Cinco checks da fonte b27da93 passaram; Windows excedeu
60 s em um teste que reunia três cenários/19 Testers falsos, durante commit SQLite.
A regressão foi separada em três cenários independentes com todas as asserções e
o limite de 60 s mantidos; três casos passaram localmente. Não mudou runtime,
banco do usuário, cálculo ou limite financeiro. CI do código final novamente pendente
após essa falha concreta; sem disparo manual de workflow ou repetição nativa.
O ensaio dirigido Wine passou: dois Testers sequenciais, resultados reabertos no banco
e consultados na UI, PIDs privados observados e ausentes ao final. Na segunda sessão,
a limpeza encerrou um MetaEditor ainda ativo após a importação. Ponte e terminal
principal mantiveram PID/criação, com DEMO conectada antes/depois.
[Evidência efetiva](evidencias/T20_25_BACKTEST_LIBERACAO_20261020.json). Não houve nova campanha
financeira nem repetição do percurso aceito. Windows físico continua específico.

A aplicação do teste manual usou a árvore local de trabalho, enquanto master estava
em 2447259. Suas bibliotecas configuradas também apontam para branches de trabalho;
não recompilar/copiar todos os fontes indiscriminadamente nem sobrescrever o WIP.
Alinhar explicitamente a instalação utilizada aos fontes revisados antes de repetir
somente uma tentativa afetada. O marcador antigo não foi removido manualmente e,
na inspeção após as falhas, já não havia processo privado daquela sessão.

# T19.2 — aceite final do contrato e adaptador base — 2026-10-19

B-11 integrada nas PRs M132/7e2178a e H14/d018f6c. PR133 com código final9b3d3f5:
seis checks existentes aprovados na execução39336237236; revisão técnica própria
concluída e sem threads de revisão impeditivas. Candidato após retarget para master
idêntico à árvore testada, sem conflito. Fechamento somente documental, com fonte,
testes, fixtures, dependências e configuração de CI preservados; sem nova campanha.

Disponível pelo serviço: composição/capacidades v1 com erros acionáveis, JSON/hash,
base Python de cinco estratégias reutilizando o motor e regras legadas. Sem UI/preset
novo, sem liberação nativa. Pullback continua bloqueado até T19.57.
[Contrato e limites](sdk/composicao.md).

Primeiro percurso Default→Manual nomeado→backtest→preset→Tester→reconciliação continua
prioritário. HADOUKEN15/B-01,16/B-02 e17/B-03 preparados em PRs próprias e aguardam
autorização específica de merge. B-01 é documental; B-02/B-03 têm CI e provas de
contrato no Tester DEMO simulado.
Próximo contrato B-04/ExecutionPlan e depois adaptador/S09 conforme dependências;
nenhum dos seis Experts passou a consumir os novos contratos. T19.7/Ultra em backlog.

Ponte acessível127.0.0.1:18812; IPC da conta configurada indisponível (-10001/-10004),
sem login/restart ou ordens. Ensaios de contrato usaram Tester privado, bloqueado fora
de DEMO simulada, sem negócios; não certificam o percurso operacional da estratégia.
Windows físico permanece pendente, Relações sintética e gradientes simulados.

## Registro anterior de preparação da T19.2 (antes do aceite acima)

T19.2 preparada sobre B-11/PR132, vinculada a HADOUKEN14: composição/capacidades v1,
validação explícita SDK1 e adaptador de backtest base reutilizando Strategy/BacktestConfig.
112 aprovados na seleção inicial; 80 composição/SDK no fechamento após modos explícitos.
CI final/integração pendentes. Não há preset ou UI operacional
nova. Pullback bloqueado somente no novo adaptador até T19.57; legado preservado.
Próximo consumidor: contratos/adaptador nativo real e S09, seguido do percurso Default/Manual.
[Contrato e limites](sdk/composicao.md).

# Prioridade do proprietário: primeiro ciclo Manual — 2026-10-19

Master4f7a046 confirmado. T19.7 continua no backlog; percurso M19-B/T20.30 priorizado.
B-11 em revisão vinculada M/H: SDK1 preservado, sidecars separados, distribuição
nativa futura delimitada. [Política](sdk/versionamento-contratos.md). Próxima tarefa
funcional:T19.2, composição/capacidades tipadas e adaptador base. Baseline6a9d580
compatível com master294 fontes/testes; ambiente válido reutilizado. Sem alteração
de runtime, nova campanha, instalação ou ensaio nativo nesta política. Windows físico
permanece pendente; Relações sintética e gradientes do Laboratório simulados.

# T19.6 — aceite temporal concluído — 2026-10-19

PR130 integrada antes, merge c27ef93; PR131 com base master, seis checks6a9d580 aprovados:
1467Ubuntu/48PG/1569WindowsCI, coberturas77,64%/73,36%;117 casos locais distintos
compatíveis,61 nos caminhos finais e doctor21/1/0.294 fontes/testes idênticos no
fechamento documental.720 fatos DEMO atuais e720 anteriores preservados; CSV/contexto
conciliados com referência independente. Origem, confirmação e conhecimento separados;
provisórios não são sinais e ausência conhecida interrompe a janela. [Contrato e
rematerialização](analytics/T19.6_PIVOS_CAUSAIS.md). Próxima elegível:T19.7. Relações
segue sintética; nenhuma estratégia/preset. Windows físico continua pendente específico.
Trabalho local e ponte preservados.

# T19.3: conexão do snapshot aceita — 2026-10-19

Correção necessária para T19.6: inspeção de schema usa a transação recebida. [Causa e evidências](analytics/T19.3_CONEXAO_SNAPSHOT.md). PR130 aceita: seis checks1092762,1439Ubuntu/44PG/1541WindowsCI,38 locais compatíveis, doctor21/1/0. Windows físico pendente. T19.6/#131 depende desta integração.

## Fechamento final T19.5 — PR128, 2026-10-19

Seis checks existentes aprovados no11b6c53:1438 Ubuntu/175 skips,43PG,1540Windows CI/73 skips/
5avisos; cov77,64%/73,36%. Revisão atual sem pendências; cinco threads anteriores tratadas
e decisões documentadas. Fechamento só documental [skip ci],293 fontes/testes idênticos.
Integrar pelo SHA revisado após129/masterd0b9c5e, preservando as bases127→126 já integradas;
registrar SHA real após resposta do GitHub.88 focados/doctor21/1/0 e prova final04f94b5b
preservados:120M5/8saídas/960fatos, reabrir SKIPPED, referência racional e doubles exatos
cálculo/store/CSV. Não repetir custo/extração/benchmark. Versões antigas continuam explícitas,
sem perda de fatos; padrão usa a implementação atual e pode exigir rematerialização.
32 definições no registry; sem estratégia/presets/Analytics completo ou Relações real.
Próxima ficha elegível T19.6, pivôs causais, em PR própria. Windows físico permanece pendente.
Root/WIP/índice/ponte preservados.

## Publicação final corrigida T19.5 — 2026-10-19

Base129 integrada emd0b9c5e; composição7cfdc81 preserva código do serviço de master e
as correções AO/AC de672b45b.88 focados em58,85s, doctor21/1/0. Prova CLI atual com
assinatura04f94b5b:120M5/8features/960fatos, reabrir SKIPPED, cálculo/store/CSV exatos e
referência racional conciliada. CSVs inicial/final têm mesmos valores/estados para esta fonte
normal; metadados de versão mudam por contrato. Nada foi apagado nem medição repetida.
Esta publicação altera runtime desdecee301c: novo CI pertinente, uma execução sobre o código
final, incluindo os wrappers PG preservados no conflito. Depois, somente fechar docs [skip ci]
com fonte293 idêntica; revisar threads/head/base antes de integrar128. Windows físico pendente.
Sem estratégia/presets ou Relações real; sem ordens, reextração ou benchmark.

## Composição final de T19.5 com129 — 2026-10-19

PR129 integrada emd0b9c5e após seis checks/revisão; incorporada na128 por Git. Preservados
os conflitos de continuidade/checklist e os wrappers PG de ambas as bases, com AST conferido.
Serviço/coretest idênticos a master129; fórmulas/registry/teste AO/AC idênticos ao672b45b.
Verificação da composição e prova com nova assinatura antes de publicar runtime final128;
CIcee301c é histórico. Sem campanha de custo, estratégia/preset ou Relações real.
Windows físico permanece pendente específico; Root/WIP/índice/ponte preservados.

## Revisão T19.5 — base de consistência isolada, 2026-10-19

PR128 fica em rascunho enquanto a PR129/T19.3 corrige o serviço compartilhado: corrida de
fonte reproduzida no mastereab717d e escrita em lote serializada por backend. Probe SQLite
concorrente inconclusiva, sem alegação de chave duplicada reproduzida. Não misturar tarefas.
Na T19.5, mediano grande e despacho sob demanda corrigidos localmente:83 testes aprovados,
21 próprios de AO/AC. CIcee301c é histórico da fonte anterior; não representa correções ainda
não publicadas. Depois de129, merge Git, checar runtime/prova e executar verificação pertinente
da composição final. Versão global conservadora mantida, antigas explícitas sem perda dos fatos.
Sem nova campanha de custo, estratégia/presets ou Relações real. Windows físico permanece pendente.

## Publicação T19.5 após as integrações — 2026-10-19

PR127 integrada em2d7412f e PR126 emeab717d após seis checks/revisão de cada fonte.
Master incorporado na T19.5 por Git; três conflitos só documentais resolvidos preservando ambos
os registros.292 fontes/testes idênticos ao b267d8d:78 focados compatíveis/doctor21-1-0,
referência racional, prova CLI/store/CSV120M5/960fatos e assinatura57c632d4 preservados.
Esta ficha recebe uma PR própria sobre master; CI obrigatório da nova mudança de runtime,
sem reexecutar custo, extração ou campanhas numéricas. Relações segue sintética e Windows
físico permanece pendência específica. Nenhuma estratégia/preset ou consumidor H antecipado.

## Preparação T19.5 — AO/AC, 2026-10-19

Worktree isolada sobre70b2191, sem publicar antes de integrar127→126. Dependência funcional
T19.3/#125 integrada. AO SMA5−SMA34 do mediano e AC AO−SMA5(AO);8 saídas no registry,
aquecimento34/38, transformações separadas, ausência/causalidade explícitas.78 testes focados
distintos compatíveis aprovados e doctor21/1/0. Referência racional independente, lote/append,
reabertura/skip, correção tardia e CSV/store/cálculo exatos. CLI com120 M5 DEMO arquivadas,
960 fatos revision1, sem nova extração/ordens/benchmark. CI e revisão final pendentes.
Relações permanece sintética; nenhuma estratégia/preset. Windows físico específico pendente.
Ver analytics/T19.5_AO_AC.md e diário privado logs/t19-5-ao-ac-20261019.

## Fechamento do follow-up T19.3 — PR129, 2026-10-19

Seis checks existentes aprovados no1039690:1417 Ubuntu/173 skips,41PG,1519Windows CI/71 skips/
5avisos; cov77,64%/73,36%. Revisão sem pendências.292 fontes/testes idênticos preservados no
fechamento documental [skip ci], sem outra campanha. Integração desta base antecede128;
registrar SHA real após resposta do GitHub e incorporar master por Git.
128 tem correções locais672b45b/83 focados; após compor, verificar casos/prova/assinatura final
porque mudou runtime, preservando os aceites compatíveis. Nenhuma nova medição/campanha.
Versão global conservadora/antiga explícita e dados preservados; Windows físico pendente.

## Follow-up T19.3 — consistência do snapshot, 2026-10-19

Revisão da #128 confirmou em mastereab717d um job COMPLETE com fonte antiga1 após correção2.
Correção isolada no serviço compartilhado: confirmação atômica de geração em lote/conclusão/cache,
rollback com SourceChanged e retomada; escrita em lote serializa revisão por backend.135 casos
distintos compatíveis aprovados e doctor21/1/0. Cinco casos PG novos aguardam CI dedicado.
A prova de barreira SQLite foi inconclusiva, não é alegada reprodução de chave duplicada.
Versão global segue o contrato existente: mudança de implementação exige rematerialização da
versão atual ou seleção explícita da antiga, sem perda dos fatos. Sem novo fingerprint improvisado.
Este follow-up precede o fechamento da #128. Nela, medianos finitos grandes e cálculo sob demanda
foram corrigidos localmente com83 focados, sem publicar antes de compor a base compartilhada.
Sem estratégia/preset/Relações real/campanha de custo; Windows físico delimitado.
Ver analytics/T19.3_CONSISTENCIA_SNAPSHOT.md. Root/WIP/índice/ponte preservados.

## Fechamento T19.4 — PR126 após127, 2026-10-19

A correção127 foi integrada em master2d7412f antes desta ficha. Composição70b2191 com seis
checks aprovados:1412 Ubuntu/168 skips,36PG,1514Windows CI/66 skips/5avisos, cov77,64%/73,36%.
Revisão sem pendências. Master incorporado por merge Git sem conflitos novos;291 fontes/testes
idênticos ao commit do CI. Fechamento somente documental [skip ci], sem outra campanha.
34 Alligator+ponte/doctor21/1/0,62 focados originais e UI compatível reaproveitados. Referência
independente, CSV/store e medição local279/216/2ms preservados; não são metas universais.
Integrar pelo SHA revisado e registrar SHA real no diário. Próxima ficha T19.5 AO/AC, em
worktree isolada, usando o serviço existente e referência racional; sem estratégia/presets.
Relações continua sintética e Windows físico é pendência específica. Root/ponte/WIP preservados.

## Fechamento da correção EXTR-02 — PR127, 2026-10-19

Aceite concluído no63e717f: seis checks existentes aprovados,1395 Ubuntu/34 PostgreSQL/1497
Windows CI; cobertura77,64%/73,36%.17 focados e doctor21/1/0 reaproveitados. Revisão sem
pendências; fechamento somente documental [skip ci], fontes/testes preservados.
Integração por merge Git antes da PR126; registrar SHA real no diário após resposta do GitHub.
A composição runtime70b2191 da PR126 passou34 casos Alligator+ponte/doctor21/1/0 e está no CI
39284170300. Não repetir referência/custo/extração aprovados. Root/WIP/ponte preservados.
Windows físico permanece pendência específica.

## Impedimento da PR126 — correção isolada EXTR-02

Ubuntu da PR126:1408 aprovados/168 skips/1falha, cobertura77,64%. Falha no teste de stop/libc
reproduzida por corrida entre perda do ambiente e saída da identidade; não foi erro Alligator.
Correção em worktree codex/extr-02-confirmacao-saida sobre master2b27b1f: aguardar identidade,
revalidando posse em cada sinal, com prazos e registros preservados.17 testes aprovados,
determinístico antes falhou, doctor21/1/0. CI Linux/revisão pendentes. Windows nativo não foi
alterado; aceites compatíveis são reaproveitados. Não refazer cálculos/custo da PR126.
Incorporar a correção pelo fluxo Git antes de integrar T19.4; Root/ponte/WIP preservados.
Relatório: extrator/EXTR-02_CONFIRMACAO_SAIDA.md. Windows físico continua pendente específico.

## T19.4 — Alligator em validação, 2026-10-19

Base master2b27b1f da T19.3/#125,290 fontes/testes do baseline exatamente compatíveis.
Ambiente instalado, aceite1528 Linux/34PG/1497Windows e seis checks reaproveitados antes da mudança.
SMMA própria com semente SMA, mediano,13/8/5 e desenho8/5/3; rma legado preservado.
Cinco saídas novas no registry/store/CSV existente, sem estratégia/presets ou Relações real.
62 focados distintos aprovados, doctor21/1/0, referência racional e doubles CSV/store exatos.
Custo em três jobs já registrados, uma amostra cada:279/216/2ms para90 M5/append30/cache.
Medição e prova CLI usaram600 M1 DEMO arquivadas, sem reextração/ordens; RSS total explícita.
Dois casos PG novos aguardam CI; revisão/aceite CI ainda pendentes. Windows físico é pendência
específica. Relatório: analytics/T19.4_ALLIGATOR.md. Root/índice/WIP e ponte preservados.
Após concluir a ficha, próxima dependência elegível T19.5 (AO/AC), com PR separada.

## Fechamento T19.3 — seis checks aprovados, 2026-10-19

PR125/eaec23b: revisão e aceite concluídos, inclusive o check Windows existente.
1528 casos distintos CI+UI,34 PostgreSQL, cov77,64%,27 focados, doctor21/1/0;
290 fontes/testes idênticos preservados no fechamento documental [skip ci].
A integração é preparada por merge Git com guarda do HEAD/base/threads; o SHA real será
registrado no diário local depois da resposta do GitHub. Não repetir verificações aprovadas
sem mudança de fonte, falha nova ou cobertura obrigatória ausente.
T19.3 entrega materialização/contexto/CSV com avaliação numérica exact_prefix explícita;
clock desconhecido e manifesto sem vínculo retornam ausência. Relações segue sintética até
T18.6; presets, Analytics completo e Windows físico não foram concluídos. Próxima ficha
dependente após integração: T19.4, conforme preço/SMMA/semente/deslocamentos especificados.

## Aceite T19.3 — aguarda Windows existente

PR125/eaec23b: revisão concluída. Aceite Linux composto1528 casos distintos
(1392 Ubuntu +132 UI Xvfb +4 UI da bateria27 reaproveitados), cobertura77,64%.
Fonte290 idêntica, doctor21/1/0, oito casos visuais e prova CLI arquivada preservados.
A suíte local completa foi encerrada após aceite Ubuntu para evitar duplicação; seu exit1
é interrupção com teardown do pytest, não é registrado como passe. Só a cobertura gráfica
ausente foi executada. Cinco checks aprovados, check Windows existente ainda em execução.
Não disparar outro workflow, não integrar antes do resultado Windows. Ver relatório T19.3.

## Retomada T19.3 — 2026-10-19

PR #124 integrada em master791f15caeee43d9ff792966e5c591f7b1c5b3d48 depois do check
Windows já existente e da revisão. Nenhum workflow adicional foi disparado para ela.
Seis checks aprovados no ecfe057, suíte1501/26/3 e fontes285 idênticas foram reaproveitados.
Registry (18 definições) e DTO (schema1) importados na nova worktree de master antes de editar.
A tela provisória de Relações permanece sintética até T18.6; Analytics/presets não estão concluídos.

Próximo consumidor elegível: T19.3, pois T19.1/#120, T13.2/#114 e T18.1/#124 estão integradas.
Candidata isolada codex/t19.3-consumo-registry, sem copiar a árvore ou alterar HEAD/índice da Root.
Consulta causal/exportação no store existente; ER reutilizado; escrita incremental com avaliação
numérica exact_prefix, sem promessa O(delta). Relógio desconhecido e manifesto sem vínculo
retornam ausência explícita; o serviço de relações continua pendente. Ver
[relatório T19.3](analytics/T19.3_MATERIALIZACAO_INDICADORES.md).
27 testes focados finais aprovados; doctor21OK/1opcional/0falhas; CLI reutilizou 600 barras DEMO
arquivadas, 120 M5 e exportação, sem nova extração ou ordens; oito casos de UI nos dois temas,
DPI96/144 e larguras820/1280. Suíte completa/CI e revisão ainda pendentes neste registro.
Windows físico é pendência específica. Ponte própria da Root preservada em segundo plano.

## Retomada T18.1 — 2026-10-19

Integração isolada do commit canônico 80fd887 sobre EXTR-02/43a4035, preservando a worktree original.
Cinco conflitos registrados e resolvidos por preservação da base documental e do incremental T13.3b.
Contrato tipado de produtores usa versão 1.3, sem mudar fórmulas/precisão nem apagar definições/valores 1.2.1.
Baseline 1482/18/3 reaproveitada por identidade de 281 fontes/testes. PR124 em rascunho; 90 focados
aprovados e doctor21/1/0. Global final f342a61:1501/26/3, saída0, cobertura77,64%,285 fontes/testes intactos.
Seis checks CI aprovados no ecfe057; Windows concluiu na execução existente;
master60d7d13 integrado por merge Git sem mudar fontes/testes.
Ver [T18.1_REGISTRY_DTO.md](analytics/T18.1_REGISTRY_DTO.md). EXTR-02 permanece separado na PR #123.

# Continuidade: harmonização de 2026-10-14

## EXTR-02 — ponte destacada do iniciador (2026-10-19)

[Recorte e prova](extrator/EXTR-02_PONTE_BACKGROUND.md). Mesma entrada
mt5-bridge.sh, sessão própria/log, trava exclusiva e identidade de nascimento/posse;
stop só sinaliza a ponte própria por pidfd. Não encerra terminal/Wine/display.
14 casos finais do gerenciador e ShellCheck aprovados; ciclo Wine real e consulta
DEMO de11barras passaram, ponte acessível depois do iniciador terminar. Suíte final1482/18/3, saída0 e cobertura77,64%, fontes intactos; doctor21/1/0.
Ponte principal migrada para background pelo patch Git de dois arquivos próprios,
com DEMO11barras e identidade do terminal preservada; seis checks CI aprovados no43a4035. Root e credenciais preservados; Windows físico pendente.
T18.1 retomada em worktree separada pelo cherry-pick revisado de80fd887; nova composição em validação.


## EXTR-01 concluída em master; cadeia #108–#117 integrada (2026-10-19)

[Fechamento, merges e prova em master](extrator/EXTR-01_VALIDACAO_NATIVA.md).
As dez PRs foram integradas por Git, preservando o código aprovado e o trabalho
local. #117 merge a92cd8a: sete casos de recuperação/falha/CLI/Cancelar passaram
em 14,03 s; suíte global 1468/18/3 e seis checks compatíveis reaproveitados.
Jornada nativa DEMO em master: 2880→3600 barras, repetir/reabrir/consultar/CSV
exatos ao provedor. UI Tk nativa: Cancelar reconhecido, confirmados preservados,
retomada e 4736 timestamps únicos exportados. Zero ordens/replay. Doctor 21 OK,
um aviso pandas-ta opcional, nenhuma falha. Dez workflows redundantes dos merges
cancelados por SHA, sem atribuir novo verde a master. Windows físico pendente.
Estado histórico ao fechar EXTR-01, superado pelo aceite EXTR-02 acima: a ponte restrita funcionava em foreground; o start em segundo plano abriu a porta,
mas não permaneceu. Próximo recorte operacional: esclarecer essa saída e validar
permanência pelo gerenciador, sem ampliar permissões ou afetar outros terminais.
Sem nova campanha numérica ou refatoração do produto para encerrar esta jornada.


## Revisão/integração PR #117 — EXTR-01 (2026-10-19)

Base master `814ded9` conciliada por merge Git na worktree da tarefa.
Revisão: Conferidos recuperação dos intervalos falhos antes da cauda incremental, ausência de sucesso falso quando a recuperação falha, escopo por tabela/limite temporal e comparação str do estado Tcl para Cancelar. Prova CLI/UI DEMO anterior preservada; conferência curta explícita em master após o merge, sem suíte global ou benchmark novos.
Aceite reaproveitado: 1468 aprovados/18 pulos/3 avisos, saída 0, cobertura 77,64%; 279 hashes conferidos; 34 focados, quatro casos novos e três Tk; doctor 16/2/0; seis checks aprovados no HEAD `d5916ee582dd0386ae11e10bf748065f0e35dac0`,
incluindo Linux, Windows e PostgreSQL. Nenhuma revisão/comentário pendente.
Conflitos resolvidos somente no painel/continuidade, preservando relatos e
consolidando linhas por ID. Código, testes, fixtures, scripts e dependências
idênticos ao HEAD aprovado fora de docs/. Sem nova campanha numérica ou suíte.
O commit apenas documental usa [skip ci]; não atribui novos verdes ao HEAD.
Windows físico permanece pendente. Histórico anterior mantido abaixo.


## Revisão/integração PR #116 — T13.3b (2026-10-19)

Base master `2ca2fc0` conciliada por merge Git na worktree da tarefa.
Revisão: Conferidos checkpoints e confirmação transacional dos lotes, retomada/invalidação, identidade contextual, revisão da fonte, limite de retries e versão 1.2.1. Benchmark existente de 25398 registros exatos preservado, sem repetição; tentativa nativa adicional inconclusiva e Windows físico continuam delimitados.
Aceite reaproveitado: 1461 aprovados/18 pulos/3 avisos, saída 0, cobertura 77,44%; 279 hashes da suíte final conferidos; 99 focados, PostgreSQL 22, limites/retomada 7, doctor 16/2/0; seis checks aprovados no HEAD `d952d4c3b767450fc01fd0cafcc22fdb7cc2b745`,
incluindo Linux, Windows e PostgreSQL. Nenhuma revisão/comentário pendente.
Conflitos resolvidos somente no painel/continuidade, preservando relatos e
consolidando linhas por ID. Código, testes, fixtures, scripts e dependências
idênticos ao HEAD aprovado fora de docs/. Sem nova campanha numérica ou suíte.
O commit apenas documental usa [skip ci]; não atribui novos verdes ao HEAD.
Windows físico permanece pendente. Histórico anterior mantido abaixo.


## Revisão/integração PR #115 — T13.3 (2026-10-19)

Base master `2efdabf` conciliada por merge Git na worktree da tarefa.
Revisão: Conferidos UTC estrito/nanossegundos, revisões de invalidação, identidade contextual por configuração/TF e recálculo delimitado no registro existente; seis famílias reutilizam store/calendário/indicadores canônicos. Confiança de regime continua proposta de pesquisa, sem promoção operacional. Árvore não documental igual a a1c2a77.
Aceite reaproveitado: 1438 aprovados/16 pulos/3 avisos, saída 0, cobertura 77,44% em a1c2a77; 70 focados, PostgreSQL 20, doctor 16/2/0; seis checks aprovados no HEAD `e653b6134f16041116c45a2d8dc34cc630e054c6`,
incluindo Linux, Windows e PostgreSQL. Nenhuma revisão/comentário pendente.
Conflitos resolvidos somente no painel/continuidade, preservando relatos e
consolidando linhas por ID. Código, testes, fixtures, scripts e dependências
idênticos ao HEAD aprovado fora de docs/. Sem nova campanha numérica ou suíte.
O commit apenas documental usa [skip ci]; não atribui novos verdes ao HEAD.
Windows físico permanece pendente. Histórico anterior mantido abaixo.


## Revisão/integração PR #114 — T13.2 (2026-10-19)

Base master `bd5cb6d` conciliada por merge Git na worktree da tarefa.
Revisão: Conferidos store canônico aditivo, definições imutáveis, consultas por disponibilidade, ordem topológica pai/filho antes dos handlers e helper PostgreSQL com schema UUID e sentinela pública preservada. Árvore não documental idêntica ao runtime da suíte final 4b3302c.
Aceite reaproveitado: 1398 aprovados/13 pulos/3 avisos, saída 0, cobertura 77,44% em 4b3302c; PostgreSQL 17 e doctor 16/2/0; seis checks aprovados no HEAD `08850c083bc4975e09a0f412c33607b4d5c3a43e`,
incluindo Linux, Windows e PostgreSQL. Nenhuma revisão/comentário pendente.
Conflitos resolvidos somente no painel/continuidade, preservando relatos e
consolidando linhas por ID. Código, testes, fixtures, scripts e dependências
idênticos ao HEAD aprovado fora de docs/. Sem nova campanha numérica ou suíte.
O commit apenas documental usa [skip ci]; não atribui novos verdes ao HEAD.
Windows físico permanece pendente. Histórico anterior mantido abaixo.


## Revisão/integração PR #113 — T19.14 (2026-10-19)

Base master `b7c8fd5` conciliada por merge Git na worktree da tarefa.
Revisão: Conferidos relógios/fuso explícitos, censura e população do run, janelas intrabarra, sidecar aditivo ligado ao ledger/snapshot e recusa de dados divergentes; nenhum limite temporal ou financeiro ampliado. A suíte composta inclui T13.2; o aceite próprio atual é o CI do SHA 4373cea.
Aceite reaproveitado: 1388/12/3 históricos; complemento de preparo coberto pelo CI atual e composição 1398/13/3 saída 0 em 4b3302c; PostgreSQL 10 e doctor 16/2/0; seis checks aprovados no HEAD `4373cea683781171a3ec5aaf717ccb8c65a3ff26`,
incluindo Linux, Windows e PostgreSQL. Nenhuma revisão/comentário pendente.
Conflitos resolvidos somente no painel/continuidade, preservando relatos e
consolidando linhas por ID. Código, testes, fixtures, scripts e dependências
idênticos ao HEAD aprovado fora de docs/. Sem nova campanha numérica ou suíte.
O commit apenas documental usa [skip ci]; não atribui novos verdes ao HEAD.
Windows físico permanece pendente. Histórico anterior mantido abaixo.


## Revisão/integração PR #112 — T20.25 (2026-10-19)

Base master `541c1c3` conciliada por merge Git na worktree da tarefa.
Revisão: Conferidos integridade e identidade do cache, perfil por tentativa, posse/trava conforme kernel, encerramento restrito a processos próprios e retenção/quarentena com referência e banco criador protegidos. Resultados históricos separados dos complementos; composição 4b3302c contém alterações posteriores, sem alegar igualdade de árvore com a PR112.
Aceite reaproveitado: 1280/11/3 históricos; complementos com CI atual aprovado e regressão composta 1398/13/3 saída 0 em 4b3302c; doctor 16/2/0; seis checks aprovados no HEAD `4aea3029a9fe6d7eacaa85ae9b652f6a2c53369c`,
incluindo Linux, Windows e PostgreSQL. Nenhuma revisão/comentário pendente.
Conflitos resolvidos somente no painel/continuidade, preservando relatos e
consolidando linhas por ID. Código, testes, fixtures, scripts e dependências
idênticos ao HEAD aprovado fora de docs/. Sem nova campanha numérica ou suíte.
O commit apenas documental usa [skip ci]; não atribui novos verdes ao HEAD.
Windows físico permanece pendente. Histórico anterior mantido abaixo.


## Revisão/integração PR #111 — T20.24 (2026-10-19)

Base master `aed0f77` conciliada por merge Git na worktree da tarefa.
Revisão: Conferidos cancelamento cooperativo sem bloquear UI, posse do checkpoint, publicação transacional, retomada sem duplicação e compartilhamento de um manifesto por fonte/TF durante o preparo. O resultado histórico isolado não é atribuído ao complemento; CI atual e composição final cobrem a revisão.
Aceite reaproveitado: 1200/10/3 históricos; complemento 39 direcionados, regressão composta 1398/13/3 saída 0 contendo o complemento e alterações posteriores; aceite próprio pelo CI do SHA 90d63d4; doctor 16/2/0; seis checks aprovados no HEAD `90d63d48506aef87afd79c224cbcebc6b0f8de22`,
incluindo Linux, Windows e PostgreSQL. Nenhuma revisão/comentário pendente.
Conflitos resolvidos somente no painel/continuidade, preservando relatos e
consolidando linhas por ID. Código, testes, fixtures, scripts e dependências
idênticos ao HEAD aprovado fora de docs/. Sem nova campanha numérica ou suíte.
O commit apenas documental usa [skip ci]; não atribui novos verdes ao HEAD.
Windows físico permanece pendente. Histórico anterior mantido abaixo.


## T20.24 — orçamento e checkpoint das filas (2026-10-18)

[Contrato e limites](analytics/T20.24_ORCAMENTO_FILA.md). `run_batch` e
`BatchRunner` usam um orçamento comum; sem outro agendador/registro estatístico.
Limites de admissões/tempo/CPU/RAM/disco, guarda F15 e planos com manifests fixos.
Publicação e conclusão do item compartilham transação/run_id reservado; retomada
recupera resultados sem repetir publicação e preserva falhas. Teto de admissões
não cancela os já admitidos. Cancelamento explícito é cooperativo; Tester nativo
retorna antes da conferência, sem encerramento de terminal alheio. UI/CLI exibem
estado parcial/ID/motivo, não lote completo. 40 fila/UI e integração anterior107
aprovados. [PR #111](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/111):
1200 aprovados, 10 pulos e 3 avisos em 1099,06 s, saída 0; 9 pulos anteriores
e 1 PostgreSQL novo, coberto no ensaio separado. Medição local4jobs700barras:2,780s/147MiB RSS
amostrado, não promessa universal. Regressão final110 e PostgreSQL16isolado8
aprovados (contêiner removido); doctor16/2/0. Windows físico pendente.
Próxima T20.25, isolamento/cache nas implementações atuais. T20.7/T20.12 mantêm
as dependências de contratos/registro canônicos ainda não integrados.

## Revisão/integração PR #110 — T20.6 (2026-10-19)

Base master `1b2cd34` conciliada por merge Git na worktree da tarefa.
Revisão: Conferidos requisitos do registry, gates por capacidade, origem desconhecida explícita, integridade das provas por barra/minuto e distinção entre qualidade histórica e ticks reais no relatório nativo.
Aceite reaproveitado: 1174 aprovados/9 pulos existentes/3 avisos, saída 0; 117 direcionados, PostgreSQL 7, doctor 16/2/0; seis checks aprovados no HEAD `79b9d30aa5cca35f69bc1aa47ce4be6713c174d2`,
incluindo Linux, Windows e PostgreSQL. Nenhuma revisão/comentário pendente.
Conflitos resolvidos somente no painel/continuidade, preservando relatos e
consolidando linhas por ID. Código, testes, fixtures, scripts e dependências
idênticos ao HEAD aprovado fora de docs/. Sem nova campanha numérica ou suíte.
O commit apenas documental usa [skip ci]; não atribui novos verdes ao HEAD.
Windows físico permanece pendente. Histórico anterior mantido abaixo.


## T20.6 — qualidade por consumidor (2026-10-18)

[Contrato e limites](analytics/T20.6_QUALIDADE_CAPACIDADES.md). Famílias e motivos
por consumidor, sem substituição real/tick-volume ou inferência de fila. Hashes
de respostas de barras/ticks/DOM conferem a origem declarada; contagem capturada
não comprova toda a história. Captura3 congela qualidade/provas/revisões e aceita
raw ticks/book apenas quando solicitados; leitura/exportação1/2 preservadas.
Seleção temporal exige fechamento/recepção explícitos por ativo/timeframe.
Relatório diferencia qualidade histórica genérica de percentual explicitamente
rotulado de ticks reais; configuração sozinha não prova cobertura. Expert sem
requisitos declarados permanece não qualificado como robô completo.
Resultados e UI transportam decisão ou ausência legada. O registry T18.1 ainda
está em branch; adaptador usa suas definições reais, sem outra implementação.
Composição/fluxo esperam os consumidores canônicos T19.2/T18.1; store causal
continua com T13.2. [PR #110](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/110):
1174 aprovados, 9 pulos existentes e 3 avisos em 1062,21 s, saída 0; 117
direcionados, checagem congelada 71, PostgreSQL 16 isolado 7 e doctor 16/2/0.
Windows físico pendente; nenhuma mudança H/S ou feed. Próxima frente independente:
T20.24 na fila existente. T20.7 aguarda T19.2/B-11; T20.12 deve estender o registro
T13.4 ainda não integrado, sem outro store.

## Revisão/integração PR #109 — T20.5 (2026-10-19)

Base master `2bd7a11` conciliada por merge Git na worktree da tarefa.
Revisão: Conferidos vigência UTC distinta do conhecimento, revisões aditivas sem lookahead, compatibilidade de snapshots v1 e limites explícitos para fonte histórica ausente, ajuste/rolagem não executados.
Aceite reaproveitado: 1137 aprovados/9 pulos existentes/3 avisos, saída 0; 85 direcionados, PostgreSQL 7, doctor 16/2/0; seis checks aprovados no HEAD `16eddef5576043ccbfc83469fcb8f1bbd42d21b9`,
incluindo Linux, Windows e PostgreSQL. Nenhuma revisão/comentário pendente.
Conflitos resolvidos somente no painel/continuidade, preservando relatos e
consolidando linhas por ID. Código, testes, fixtures, scripts e dependências
idênticos ao HEAD aprovado fora de docs/. Sem nova campanha numérica ou suíte.
O commit apenas documental usa [skip ci]; não atribui novos verdes ao HEAD.
Windows físico permanece pendente. Histórico anterior mantido abaixo.


## T20.5 — vigência de instrumentos (2026-10-18)

[Fonte e decisões](analytics/T20.5_VIGENCIA_INSTRUMENTOS.md). Histórico aditivo de
especificações, ajustes/vencimentos/rolagens, papel da série e universo, com vigência e
conhecimento independentes. Manifesto 2 preserva os fatos; produtor anterior
formato 1 importado/exportado por bytes no teste, sem regravar revisões antigas.
Especificação atual não recebe vigência retroativa. Cobertura histórica constante
usa os fatos, mesmo com cache atual diferente. Variação de propriedades é
identificada e recusada pelos motores de especificação constante. Série contínua
permanece estudo; eventos declarados não são transformações ou ordens implícitas.
Regressão direcionada: 85 aprovados; PostgreSQL 16 isolado: 7 aprovados; doctor
16 OK/2 avisos/0 falhas. Suíte completa: 1137 aprovados, 9 pulos existentes e
3 avisos em 1019,35 s, saída 0. Windows físico pendente. Próxima T20.6.
[PR #109](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/109) sobre T20.4; nenhuma alteração de produção H/S ou contratação.

## Revisão/integração PR #108 — T20.4 (2026-10-19)

Base master `9fbd5f5` conciliada por merge Git na worktree da tarefa.
Revisão: Conferidos captura transacional SQLite/PostgreSQL, manifesto/blocos imutáveis, importação com integridade e replay/cache sem fallback à revisão corrente.
Aceite reaproveitado: 1114 aprovados/9 pulos existentes/3 avisos, saída 0; 28 direcionados, PostgreSQL 7, doctor 16/2/0; seis checks aprovados no HEAD `c991b04c85029824e74a0861fb82947ade3d227c`,
incluindo Linux, Windows e PostgreSQL. Nenhuma revisão/comentário pendente.
Conflitos resolvidos somente no painel/continuidade, preservando relatos e
consolidando linhas por ID. Código, testes, fixtures, scripts e dependências
idênticos ao HEAD aprovado fora de docs/. Sem nova campanha numérica ou suíte.
O commit apenas documental usa [skip ci]; não atribui novos verdes ao HEAD.
Windows físico permanece pendente. Histórico anterior mantido abaixo.


## T20.4 — revisões de dados (2026-10-18)

Serviço canônico aditivo de blocos/manifestos/referências, captura em uma imagem
de leitura, replay Python/livro e cache com revisão fixada. Export/import verifica
hashes sem substituir barras do destino. [Contrato e limites](analytics/T20.4_MANIFESTOS_DADOS.md).
[PR #108](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/108) em revisão sobre #107. PostgreSQL 16 isolado: sete testes aprovados; suíte completa Linux: 1114 aprovados, 9 pulos existentes e 3 avisos em 1009,24 s, saída 0. Doctor 16/2/0; 28 direcionados aprovados. Próxima T20.5.
T20.3 recebeu correção do leitor de fluxos/descendentes no PR #107: suíte final
1103 aprovados/9 pulos existentes/3 avisos em 1009,28 s, doctor 16/2/0.
Windows físico pendente; nenhuma alegação de integração ou roadmap concluído.

## Revisão de integração T20.3 (2026-10-19)

A base T20.2 foi integrada em master pelo PR #106 (f477e43), após as bases
documentais #118–#121. A revisão da #107 conferiu curvas separadas, fluxos por
cotas, colunas aditivas, leitura legada, recuperação dos fluxos em re-simulações
e compatibilidade das chamadas do SDK, sem novo impedimento identificado.

A conciliação com master alterou somente documentação. O conflito no painel
foi resolvido transferindo os aceites das linhas provisórias para as linhas
canônicas de T20.2/T20.3, preservando os relatos de continuidade. Runtime,
testes e fixtures permanecem iguais ao HEAD aprovado c0f1846 (runtime 91b2ec4).
Reaproveitados 1.103 aprovados, nove pulos existentes, três avisos, saída 0,
91 direcionados do complemento, doctor 16 OK/2 avisos/0 falhas e seis checks
aprovados, inclusive Windows CI. Hashes e árvore não documental conferidos;
nenhuma suíte ou campanha numérica repetida. A prova nativa existente mantém
seu recorte de 24 marcas EURUSD H1 no Wine; Windows físico permanece pendente.


## T20.3 — curvas patrimoniais e comparação nativa (2026-10-17)

[Decisões e limites](analytics/T20.3_PATRIMONIO_FLUTUANTE.md). Mantida a curva
de operações encerradas; acrescentados saldo realizado e patrimônio por
fechamento de barra, BID/ASK, custos realizados e swap acumulado. DD novo
identificado, percentual indefinido se o pico não for positivo, fluxos externos
separados e neutralizados por cotas na grade de fechamento. Re-simulação soma posições sobrepostas no mesmo capital; não cria
carteira com margem compartilhada. Persistência em cinco colunas aditivas;
legado lê sem inventar trajetória ou regravar linhas/schema. Modos sequenciais
de lote declaram trajetória indisponível; specs genéricas/swap não suportado
marcam capacidade limitada. Sem alteração de UI ou produção H/S.

Referências direcionadas com fluxos, persistência e DTOs: 85 aprovados.
Native fixture: MetaTester 6272/Wine 11.17, EURUSD H1 MetaQuotes-Demo, 2025-01-06,
24 observações; diferença de saldo zero e de equity 1,14e-13 USD. Fonte final
compilado sem erros/avisos. Verificador e CSV/hashes versionados em
tests/fixtures/equity_reference. Só Strategy Tester em cópia própria; Python
não enviou ordens. A prova não certifica todos os Experts/custos/moedas/contas.

Baseline 1081/9 pendências/3 avisos confirmada antes das alterações; ambiente
preparado novamente. Suíte completa final: 1103 aprovados, nove pendências existentes, três avisos,
1035,47 s, saída 0 (logs/t20-3/pytest-final.log). SDK/fluxos/docs: 30 aprovados;
parâmetro cashflows opcional ao final documentado e testado com bindings antigos.
[PR #107](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/107) em revisão
como rascunho sobre #106. Rodadas preliminares foram substituídas pelos ajustes
de DTOs, fluxos externos e referência do SDK. Doctor 16 OK/2 avisos/0
falhas. Windows físico pendente; artefatos em logs/t20-3. Próxima independente:
T20.4, respeitando store/registry de branches existentes.

## T20.2 — referências financeiras e correções locais (2026-10-17)

[Contas independentes](analytics/T20.2_REFERENCIAS_FINANCEIRAS.md) e testes dedicados.
Reprodução inicial: 7 falhas/8 aprovados, antes das correções; novos casos de
replay reproduziram mais duas divergências. Corrigidos teto de lote fora do passo,
epsilon que elevava risco, swap por parcela e por entrada de cada lote na
re-simulação. Métricas recusam PnL inválido, mantendo PF canônico v1 e os
estados legítimos sem operações/sem perdas. Sem mudança de banco ou contratos H/S.

Verificação direcionada: 96 aprovados, uma pendência PostgreSQL existente.
Suíte completa do runtime alterado: 1085 aprovados, nove pendências existentes,
três avisos, 1179,37 s, saída 0. Doctor: 16 OK/2 avisos/0 falhas.
[PR #106](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/106) em revisão
como rascunho, separado da base documental. Próximo recorte: T20.3.
Baseline completa anterior ao runtime: 1058 aprovados/9 pendências/3 avisos.
A execução documental T20.1 foi iniciada antes deste runtime; não substitui
a verificação final T20.2. Evidências/patch separados em logs/t20-2.
Windows físico/nativo não executados; parcial inviável continua em T19.57.
Não declarar toda F20 ou Q01 concluído por estas contas.

## T20.1 — cobertura Q01–Q16 no mapa único (2026-10-17)

Seção 8 do [inventário F19/F20](propostas/INVENTARIO_F19.md) ampliada com base
concreta, dono executor por extensão, consumidor, lacuna e prova necessária para
cada Q. Resolvidos no mapa snapshots contraditórios de F13/F16, I-05/--deep,
T8.7/paridade, T12.3 e exportação T17.28, preservando os relatos históricos.
Não houve implementação ou aprovação de um Q por existência de código.

Ambiente Linux preparado; baseline anterior da mesma sessão 1058/9/3 avisos,
saída 0, com runtime idêntico. Suíte final em execução. Próximo P0: T20.2,
referências financeiras independentes sobre motor atual. T20.4/24 são trilhas
elegíveis; contratos B e integração store/registry mantêm suas dependências.
Uma tarefa por entrega; patch próprio em logs/t20-1, separado da T19.1 e da
base documental preexistente. Sem mudança H/S/runtime/testes/scripts, sem
ShellCheck aplicável ou ensaio visual/nativo/Windows físico nesta ficha.

## T19.1 — inventário reconciliado para revisão local (2026-10-17)

[Inventário único F19](propostas/INVENTARIO_F19.md): SHAs M/H/S, estado integrado
versus branches, donos/consumidores, contratos, inputs/presets e cobertura
I01–I18/TM01–TM08/S01–S11/AX01–AX09. Os estudos preservam seus hashes.
T13.2/T18.1 e produtores existem em branches não integradas; não reimplementá-los.
Contratos B/Composer ainda são propostos. T17.28 já fornece seletor Ativo / TF
e exportador de sessão ampliado; não equivale ao exportador permanente T16.2b-H
nem ao livro de todos os eventos/ciclos. Onze fontes Safety H/S são idênticas,
versão S 1.5.0; instalação no terminal não foi conferida.

Reprodução sintética confirmou parcial de 50% inviável com mínimo/passo 1
ativando break-even sem redução. Dono T19.57; padrão semelhante lido no
backtester e H, sem ensaio nativo ou correção neste recorte documental.
Contrato 1 preservado; composição/capacidades via artefato lateral versionado,
com schema final T19.2 e distribuição condicionada à política B-11.

Instalador Linux --yes --no-mt5 e requirements-dev executados. Suíte inicial
antes das edições: **1058 aprovados/9 ignorados existentes/3 avisos em 1045,42 s**,
saída 0. Xvfb próprio, 640×480, aproximadamente 100 DPI, sem WM; temporários
exclusivos em disco, 66 GiB disponíveis no início. Doctor inicial 16 OK/2 avisos/
0 falhas (pandas-ta opcional ausente, ponte desligada). Suíte final: **1058
aprovados/9 ignorados existentes/3 avisos em 1265,91 s**, saída 0. SDK docs:
3 aprovados em 0,89 s; doctor final 16 OK/2 avisos/0 falhas. Conferidos 36
links locais novos, cobertura integral do catálogo e dependências F19 inalteradas;
57 fichas F19/30 F20 preservadas. git diff --check aprovado.

Nenhum runtime/teste/schema/script/Expert/preset foi alterado. H/S e estudos
somente lidos; sem prova visual/performance/MT5/replay/DEMO/Windows físico nesta
ficha documental. ShellCheck não aplicável. Pendências Windows operacionais
permanecem no checklist existente; nenhum comportamento novo por plataforma.

Árvore inicial tinha 8 documentos modificados e 11 novos de T19.0/T20.0 e
referências, preservados. Branch tarefa/T19.1-inventario-contratos; incremento
próprio separado do diff preexistente em logs/t19-1. Sem commit/push/PR para
não incorporar a base documental alheia: publicar/integrar essa base primeiro,
depois aplicar o incremento em PR próprio T19.1. Após revisão/integração, próxima
ficha independente: T20.1, estendendo este mapa para Q01–Q16; T19.2 aguarda
contratos B e T19.3 aguarda store/registry integrados.

> **T17.28 — comandos dos cards MT5 (2026-10-17):** seletores pesquisáveis de
> ativo/timeframe e preset governam a evidência mostrada. PLAY tem revisão da
> carteira/risco, duas confirmações e alerta negativo adicional. REAL mantém
> opt-in, tarja e gates, testados somente com provedores falsos. Sessões próprias
> preservam o terminal original; REPLAY visual/Model4 concluiu com 16 posições,
> -2.342,53 USD e PF0,47995551 reconciliados com saldo nativo, sem aprovação
> financeira. Cor e texto acompanharam lucro/perda simulados. PLAY/STOP pelo
> card foram exercitados no demo, sem posições/ordens; fechamento não liquida
> exposição existente. Suíte/CI finais, revisão própria e lançador instalado
> são gates do PR105, sem revisão independente presumida. Um CSV de ensaio foi
> restaurado de backup verificado e a causa corrigida; 23 arquivos compartilhados
> preservados nos ensaios seguintes. [Escopo, incidentes e limites](propostas/T17_28_CONTROLES_MT5.md).

> **T17.27 — layout nativo (2026-10-17):** o gráfico recalcula margens após resize da figura, inclusive mudança de DPI sem Configure Tk. A área conserva altura própria pela fonte, como em Análise, impedindo crescimento ao alternar páginas. Baseline998/9 em árvore/pacotes idênticos, 34 focados e 32 cenários de cópia real DPI144 passaram; figuras em600px, legendas completas. Suíte/CI/revisão e lançador instalado são gates explícitos do PR. [Recorte e evidências](propostas/T17_27_LAYOUT_DPI_LABORATORIO.md).

> **T17.26 — ativos e sinal dos backtests (2026-10-17):** os cards contam ativos
> distintos, classificando o último teste de cada ativo; ganho e perda têm setas
> próprias, zero e ausência ficam separados e discretos. A política é visível.
> Consulta SQL independente coincide nos 15 robôs da cópia real; 12 capturas em
> claro/escuro e 1280×800/1920×1080 verificadas, com clique e troca de tema em
> regressão de widgets. A composição com T17.25, suíte, CI exato e smoke instalado
> permanecem gates registrados no PR desta tarefa. [Política e evidências](propostas/T17_26_ROBOS_ATIVOS_RESULTADOS.md).

> **T17.25 — recuperação do Laboratório (2026-10-17):** assinatura do contexto inclui barras; comparação preserva a sequência original, explica ausência de entrada e descarta callbacks antigos. Execução reutiliza subvisões Resultados/Curvas/Horários, com contagens reais no mapa e layout recalculado no resize. Baseline 967/9, regressões e 28 cenários com cópia real foram conferidos. Suíte final, CI, revisão e operação instalada são gates rastreados no PR. [Escopo, evidências e limites](propostas/T17_25_LABORATORIO_COBERTURA.md).

> **T17.24 — correção do erro informado em 2026-10-16:** a projeção de bateria
> com PF indefinido passa a incluir o ícone exigido pelo detalhe do robô. A aba
> Bateria reutiliza a rolagem existente e ajusta o texto à largura útil. Baseline
> 963/9, 51 focados de PF/bateria e quatro regressões de UI passaram; a cópia da
> base real passou em 60 seleções nos dois temas e resoluções, com registros
> preservados. Suíte final, CI, revisão e smoke instalado são gates rastreados no
> PR da T17.24. [Escopo e evidências](propostas/T17_24_BATERIA_PERSISTIDA.md).

> Estado atual da frente visual (snapshot854af32, 2026-10-16): PRs #81–#93, #95/#96/#97/#98/#99/#100 integrados separadamente, com revisão própria e CI. Detalhes recolhíveis já funcionam; abertura real por `./run.sh --offline` verificada e encerrada0, configuração preservada. T17.23/PR99 integrado e operado pelo lançador real; aceite críticoUI registrado com ressalvas. T13.10/PR100 integrado e CLI instalada verificada: oito casos retrospectivos, nenhum candidato (maiorPFmodelado0,875849);963/9 e CI final aprovados. Consulte [o relatório reconciliado](propostas/CONTINUIDADE_INTEGRACAO_UI.md); as seções abaixo também conservam snapshots históricos, cujas pendências não substituem este estado atual.


## Retomada T18.8 — 2026-10-15

A apresentação local da T18.8 foi implementada. A aba Laboratório tem as visões Matriz (Indicador × indicador e Indicador × alvo),
Linha do tempo, Ganhos e perdas, Convergências e Candidatos. As subvisões usam a fixture do componente e estão
identificadas como sintéticas; veja as capturas em [docs/img/relations/T18.8](img/relations/T18.8/).

A revisão com Analytics confirmou que T18.1 (DTO/fixture versionada) e T18.6 (serviço de consulta) ainda não estão
presentes nesta base. Não integrar ao ledger F16 nem inferir estados até receber esse contrato. A UI não recalcula
estatísticas, não altera Experts, presets ou ordens, e não oferece ação operacional para candidatos. O layout reusa
tema, tokens e componentes T17; subvisões ficam roláveis em janelas menores e a linha temporal suaviza só a exibição
entre pontos recebidos, preservando marcadores e lacunas.

Validação reportada na worktree original da T18.8 (que também continha branches locais de Analytics): suíte completa
**900 aprovados, 10 ignorados** em 666,12 s; `doctor` **16 OK, 2 avisos, 0 falhas**
(`pandas-ta` opcional ausente e ponte MT5 desativada); `git diff --check` limpo. Capturas foram geradas em Linux/Xvfb
nos temas claro e escuro, em 1280×800, 1920×1080, 820×800 e escala Tk simulada de 150%. Windows real não foi executado.
Na revisão posterior, as faixas da linha do tempo passaram a usar tons neutros para reservar as cores semânticas aos
marcadores; legenda, seletor de ranking e avisos empilham/quebram texto em janelas estreitas. As quatro capturas da
linha do tempo foram renovadas depois desses ajustes.

Revisão da cadeia isolada para publicação (master + T17.1–T17.5 + T18.0 + T18.8): instalação Linux com
`./install.sh --yes --no-mt5` e `requirements-dev.txt`; primeira suíte completa **835 aprovados, 9 ignorados,
2 falhas** em 616,75 s (`test_settings_restore_the_arrangement_only_with_the_terminal_closed` e
`test_no_internet_is_said_plainly_with_what_to_do`). Ambos passaram quando repetidos isoladamente. Segunda suíte
completa: **836 aprovados, 9 ignorados, 1 falha** em 594,69 s; repetiu apenas
`test_settings_restore_the_arrangement_only_with_the_terminal_closed`, também aprovado isoladamente. A espera
assíncrona deste teste já tinha sido registrada como intermitente durante T17.3; nenhum teste foi enfraquecido nem
alterado para esconder o resultado. `doctor`: **16 OK, 2 avisos, 0 falhas** (`pandas-ta` opcional ausente e ponte
MT5 desativada). Os testes focados da apresentação T18.8: **15 aprovados**. `git diff --check` passou; nenhum script
shell mudou. Windows real não foi executado.

## Nova frente documental F18 — 2026-10-15

Pedido do dono para relações entre indicadores, preço futuro, ganhos/perdas e aderência temporal dos Experts.
T18.0 registra a [especificação](propostas/RELACOES_INDICADORES_SETUP.md), a [matriz específica da F18](analytics/MATRIZ_REQUISITOS_F18.md),
as tarefas no plano/roadmap e os prompts separados de [Analytics](analytics/PROMPT_F18_ANALYTICS.md) e [UI](analytics/PROMPT_F18_UI.md).
T18.1–T18.9 estavam inicialmente planejadas; a apresentação T18.8 foi adicionada depois. T18.1 (DTO/fixture versionada)
e T18.6 (serviço de consulta) ainda não estão nesta base, então a tela usa uma fixture sintética e identificada como provisória.
Pesquisa/serviço local independem de MQL5; aplicação operacional mantém dependências e autorizações próprias de T13.7.
Nenhum cálculo analítico, preset, Expert ou ordem foi alterado pela apresentação. Cada implementação segue uma tarefa por PR.

O trabalho de código da T18.1 está no worktree `../MISA_MT5-Extra-o-de-Dados-T18.1`, baseado no checkpoint T18.0
`cfe34dc`. A fixture/DTO é entregue nessa branch; a UI do `master` ainda requer um adaptador no serviço T18.6.

Verificação do registro: instalador sem MT5 e dependências dev executados; 5 testes focados de documentação/integração
aprovados; 11 novos links locais conferidos; `git diff --check` sem erros. Doctor do ambiente existente:
16 OK, 2 avisos (pandas-ta opcional ausente e ponte desligada), 0 falhas.
A primeira linha de base sob Xvfb foi interrompida por falta de espaço no tmpfs `/tmp` (Errno 28).
A repetição em diretório exclusivo no disco terminou com 828 aprovados e 47 ignorados em 402,66 s; o Xvfb encerrou
com erro de limpeza (`kill: No such process`, saída 1), portanto **não é validação integral da UI**.
Janela principal repetida isoladamente: 34 aprovados em 154,36 s. Não se somam essas execuções como suíte única.
Nenhum teste foi alterado/pulado por esta tarefa; não foram apagados temporários de outros agentes.
Não houve ShellCheck porque nenhum script shell mudou. Windows e MT5 real não foram validados nesta entrega.

**Leia isto antes de retomar qualquer tarefa.** Em 2026-10-14 os agentes pararam por limite de crédito com várias
frentes abertas em branches locais. A pedido do dono do projeto, todas foram integradas ao `master` numa única
harmonização (branch `integracao/harmonizacao-2026-10-14`), testadas juntas e registradas aqui. O estado de cada
tarefa continua no painel de [PLANO_DE_TRABALHO.md](PLANO_DE_TRABALHO.md).

## O que entrou no master

| Frente | Branch de origem | Commits | O que traz |
|---|---|---|---|
| T12.1 (registros) | `tarefa/T12.1-safety-ampliada` | `d7f0e5f`, `bf55ad8` | Evidências da T12.1 e a divergência de dependências T12.1 ↔ T12.3 (só docs) |
| T12.6a | `tarefa/T12.6a-painel-v7` | `013c723` | Validação do painel v7 concluída no Linux (só docs: plano e ROADMAP) |
| T10.11 | `tarefa/T10.11-preparacao-market` | `ccd51b2`, `b2dc33c` | Pré-checagem e pacote local do Market (`robots/market.py`, comandos no `cli.py`, testes) |
| F-03 / T12.3 | `tarefa/F-03-exposicao` (o mesmo commit está em `codex/t12-3-orcamento-global`) | `db6ca9f` | Exposição consolidada por ativo e grupo em `robots/portfolio.py`, `mt5x-safety` 1.4.0, chave nova de config, docs do SDK |
| Fase 15 (T15 a T15.9) | `tarefa/T15-gestao-recursos` → `T15.7` → `T15.8` → `T15.9-limites-retencao` (PRs #51 a #54) | 6 commits | Aba "Recursos", `mt5x resources`, arquivo reversível, cópias de segurança, retenção, limites que bloqueiam a gravação, automação opt-in. Plano: [propostas/GESTAO_RECURSOS.md](propostas/GESTAO_RECURSOS.md) |

Ordem das mesclagens: T12.1 → T12.6a → T10.11 → F-03 → Fase 15. Cada uma é um commit de merge próprio
("merge: ... (harmonização 2026-10-14)"), então dá para ver ou reverter uma frente sem mexer nas outras.

## Conflitos e como foram resolvidos

Só houve conflito em `docs/PLANO_DE_TRABALHO.md` (F-03 × T12.1/T12.6a). Código não conflitou.

1. **Painel de status:** ficou **T12.6a ✅ concluída** (da branch T12.6a) e **T12.3 ⏳ em implementação autorizada**
   (da F-03). As duas eram mudanças de linhas diferentes da mesma tabela.
2. **Seção da T12.1:** os dois textos foram mantidos. Primeiro vêm as evidências da T12.1; depois, como "Atualização da
   frente F-03", a `mt5x-safety` 1.4.0 e a tabela de paridade dos seis robôs; por fim, a divergência de dependências.
   "Cinco robôs" virou "seis robôs", como na F-03, que é a mais recente.
3. **Decisão em aberto, não tomada na harmonização:** a T12.1 registrou um ciclo de dependências (T12.1 precisa da
   T12.3, que depende da T12.2, que depende da T12.1). A F-03 já trata a T12.3 como "em implementação autorizada".
   **Nada foi reordenado.** A ordem formal precisa ser confirmada pelo dono do projeto (nota no fim da seção T12.1).

## Correções feitas durante a harmonização (pelo CI do PR #55)

- `resources/guard.py` (T15.9): o cache do guarda começava em 0 no relógio `time.monotonic()`, que conta desde o boot.
  Numa máquina recém-ligada a primeira checagem não rodava. Agora a primeira chamada sempre mede (teste novo).
- `tests/test_ui_main_window.py`: o fixture esperava fixos 0,6 s pela leitura da base em segundo plano; no runner do
  Windows não bastava (`test_backtest_from_the_robot_card`). Agora espera a base carregar, até 10 s. Nenhuma
  asserção mudou e nenhum teste foi pulado.

## O que NÃO entrou, e por quê

- `claude/adoring-hopper-qdtf34` (PR #1, commit `ce83918`): era um `docs/continuidade.md` antigo, da época da T8.3,
  que mandava fazer checkout daquela branch. Mesclá-lo apontaria os agentes para um estado velho. Este arquivo o
  substitui; o PR #1 foi fechado com essa explicação.

## Verificação feita na base integrada

- `pytest`: **701 aprovados, 8 pulados** (antes da harmonização o master tinha 648).
- Importação da interface e dos módulos novos; `mt5x --help` lista `portfolio`, `resources` e `robots` (com `market`).
- Não foi feito: MT5 real, Windows, PostgreSQL ou TimescaleDB reais. Seguem as pendências já registradas em cada tarefa
  e no checklist de [validacao_instaladores.md](validacao_instaladores.md).

## Como cada agente retoma

1. `git fetch origin && git switch master && git pull`.
2. **Não continue numa branch antiga sem antes trazer o master**: `git switch <sua-branch> && git merge origin/master`.
   As branches listadas acima já estão inteiras no master; para trabalho novo, crie uma branch nova a partir dele.
3. Rode `.venv/bin/python -m pytest -q` (esperado: 701 aprovados, mais os testes que você acrescentar) e o `doctor`.
4. Regras novas que valem para todos (detalhes em [propostas/GESTAO_RECURSOS.md](propostas/GESTAO_RECURSOS.md)):
   - O menu Exibir seleciona as abas pelo widget, não pelo índice: a aba "Recursos" fica entre "Robôs" e "Atividade".
   - O extrator tem `write_guard`: o caminho que grava barras precisa consultá-lo antes de gravar.
   - Para tirar dados de usuário da base, use `resources.actions.archive_tables` ou `trim_tables` (arquivo
     reversível), nunca DROP ou DELETE direto.
   - "Benchmark positivo" = algum backtest com lucro no ativo, em qualquer robô, configuração ou data. Esses ativos nunca
     são candidatos à limpeza.
   - Tabelas internas novas, todas aditivas: `_resource_snapshots` e `_resource_actions`.
5. Ao terminar a tarefa, atualize o painel do PLANO_DE_TRABALHO.md e, se mudar o estado geral, este arquivo.

## Retomada após a harmonização — T13.1

- Branch local atual: `tarefa/T13.1-calendario-sessoes`, criada a partir de `master` em `2226bf5`.
- A T13.1 implementa calendário observado em M1, captura de metadados estáveis de `symbol_info`, barras derivadas
  versionadas e o modo `mt5x resample --session-aware`. As regras e limitações estão em
  [T13.1_CALENDARIO_OBSERVADO.md](analytics/T13.1_CALENDARIO_OBSERVADO.md).
- Verificação: testes focados `.venv/bin/python -m pytest -q tests/test_analytics_calendar.py tests/test_resample.py`
  — **20 aprovados**; `doctor` — 16 OK, 2 avisos, 0 falhas. Duas repetições integrais ficaram inconclusivas: a
  primeira terminou aos 79% com 413 aprovados, 8 pulados, 85 falhas e 98 erros após `OSError: [Errno 28] No space
  left on device` no tmpfs; a segunda chegou à bateria de UI/conector, emitiu marcadores `E` e encerrou com status
  143 após cerca de 32 minutos, sem resumo final. Os temporários próprios foram removidos; o tmpfs tinha 604 MB
  livres ao fim da segunda execução. A base EURUSD foi lida sem escrita e sem conectar ao MT5. O modo legado
  `resample` não mudou.
- A branch é local; não houve push nem PR. A T13.1 fica pronta para revisão, com a regressão global inconclusiva;
  a T13.2 continua sendo a próxima tarefa da Fase 13 após integrar esta dependência.
- Worktree `../MISA_MT5-analytics` permanece ativo e não foi removido; na última conferência estava na branch
  `tarefa/T16.11b-execucao-insights`.

## Harmonização 2 (2026-10-14): Fase 16 e T13.1 no master

Branch `integracao/fase16-t13.1`, mesclada no `master` (veja o PR dela). Entraram:

| Frente | O que traz | Registro |
|---|---|---|
| Fase 16 — Laboratório (T16.0 a T16.12; PRs #56 a #70, fechados como substituídos) | Aba "Laboratório", livro de operações `_trade_ledger`, calendário econômico de várias fontes, enriquecimento, re-simulação de execução, insights validados, `mt5x trades` | [propostas/ANALYTICS_OPERACOES.md](propostas/ANALYTICS_OPERACOES.md), seção 10 |
| T13.1 — calendário de sessões (branch `tarefa/T13.1-calendario-sessoes`, commit `05506ee`) | Calendário observado em M1, barras derivadas versionadas, `mt5x resample --session-aware` | [analytics/T13.1_CALENDARIO_OBSERVADO.md](analytics/T13.1_CALENDARIO_OBSERVADO.md) |

**Ajuste feito na T13.1 durante a harmonização:** as tabelas viraram `_market_calendar` e `_bars_derived`. Sem o `_`,
o `DatabaseManager` as trataria como séries de ativo: `bars_derived` tem `close` e entraria no inventário, e o
`MIN(time)` do inventário quebraria ("Minha base", Laboratório e Recursos fora do ar). Nenhuma base real tinha as
tabelas. Teste de regressão: `tests/test_harmonizacao_frentes.py`.

**Sobre a suíte "inconclusiva" da T13.1:** as duas causas eram do ambiente, não do código:
1. o tmpfs `/tmp` estava com 3,8 GB de pastas antigas do pytest (`/tmp/pytest-of-misael/pytest-*`); foram apagadas;
2. o display `:0` fica inacessível para o shell em alguns momentos, e cada teste de interface espera minutos pelo X
   antes de desistir (os `E` e o status 143 depois de 32 min). **Rode a suíte com `xvfb-run -a`** (cerca de 6 min).

**Regra nova para todos:** tabela interna (que não é série de ativo) começa com `_`. O teste de regressão acima
protege as das Fases 13, 15 e 16.

**Próximas tarefas:** T13.2 (Fase 13) pode partir do `master`. Pendências da Fase 16 na seção 10 do plano dela
(T16.2b no HADOUKEN, exportação do calendário com o MT5 conectado, Windows real).

**Queda "Illegal instruction"/"Segmentation fault" na suíte (era a mesma "regressão inconclusiva" da T13.1):** o coletor de
lixo do Python rodava dentro de threads de segundo plano e destruía variáveis e imagens do Tk, que só aceita a thread
principal. Corrigido: a janela desliga a coleta automática (`ui_manager.setup_ui`) e coleta na thread principal a cada
~2 s (`_pump_ui_queue`); os testes religam e coletam depois de cada teste (`tests/conftest.py`). Com isso, a suíte
completa: **802 aprovados, 9 pulados, ~7,5 min** com `xvfb-run -a`.

## Retomada da T13.2 — 2026-10-14

- Implementação na branch `codex/t13.2-feature-store`, ainda não integrada: registry imutável por versão, valores
  append-only com disponibilidade UTC e revisão, consulta *as-of*, propagação seletiva de dependências e CLI
  `mt5x analytics features list/show/values/affected`. Detalhes em
  [T13.2_FEATURE_STORE.md](analytics/T13.2_FEATURE_STORE.md).
- Decisão da primeira fase: SQLite, com SQLAlchemy e DDL portátil. A base local lida somente em metadados tinha
  329,6 MB e 23 tabelas. A exportação do retrato para o MT5 permanece para a T13.5. Nenhum valor de característica
  foi calculado ou gravado na base real.
- Verificação: testes focados 20 aprovados, 2 pulados; suíte completa **809 aprovados, 10 pulados em 521,99 s**;
  `doctor` 16 OK, 2 avisos, 0 falhas. Os dois testes PostgreSQL da frente foram pulados localmente porque
  `MT5X_TEST_PG_URL` não está configurada; um teste de round-trip *as-of* foi adicionado ao job PostgreSQL do CI.
  Windows real, PostgreSQL local e MT5 real não foram testados.

## Retomada da Fase 17 — 2026-10-14

- **T17.1 concluída em 2026-10-15** no Linux; validação Windows continua pendente.
- A Fase 17 foi criada para a modernização visual do mt5x, separada das frentes de Analytics. O plano completo está em
  [propostas/INTERFACE_VISUAL.md](propostas/INTERFACE_VISUAL.md); T17.1 a T17.5 têm entregas independentes.
- T17.1 foi concluída no Linux na worktree `../MISA_MT5-Extra-o-de-Dados-T17.1`, branch `tarefa/T17.1-sistema-visual`, baseada no
  commit `e8657fe`. A worktree original da T13.3 contém alterações Analytics não commitadas e foi preservada.
- Escopo T17.1: `ui/visual_tokens.py` centraliza paletas, escala, tipografia, espaçamento, dimensões e contraste;
  `ui/theme.py` agora escolhe uma família sans instalada, aplica tamanhos mínimos de 11/10 pt, suporta ajuste de
  leitura sem duplicar o DPI e usa tokens de espaço nas tabelas, controles e abas. `tests/test_ui_theme.py` cobre
  contraste, fallback, tamanhos e notificação de redesenho. Nenhum motor, cálculo, schema ou conexão MT5 foi alterado.
- Linha de base da T17.1: `install.sh --yes --no-mt5`, `pip install -r requirements-dev.txt` e suíte antes das
  alterações (**809 aprovados, 10 pulados em 492,63 s**, com `xvfb-run -a`). A linha de base realista usa ledger e
  barras sintéticos e está em `docs/img/interface-visual/T17.1/mt5x-before-light.png` e
  `mt5x-before-dark.png`; não houve conexão ao MT5. Após a mudança, capturas com a mesma fixture estão em
  `mt5x-after-light.png` e `mt5x-after-dark.png`.
- No host Linux, a família Tk disponível é Lato; Inter/Noto Sans não aparecem entre as famílias anunciadas ao Tk.
  A escala física medida no Xvfb foi ~100 DPI e `tk scaling` 1,389; o Tk aplica essa escala aos pontos. Contraste
  textual inicial medido: `muted` 3,15:1 e `accent` 3,87:1 no fundo claro. A nova paleta é testada a pelo menos
  4,5:1 para os papéis de texto e 3:1 para séries.
- Verificação posterior: testes focados de tema/janela/Laboratório **43 aprovados em 250,42 s**; teste focado de
  tokens/tema **5 aprovados em 0,43 s**; suíte completa final **814 aprovados, 10 pulados em 718,50 s** usando
  `TMPDIR` e `--basetemp` dedicados após um primeiro run interrompido por `Errno 28` de armazenamento temporário.
  `doctor`: **16 OK, 2 avisos, 0 falhas** (opcional
  `pandas-ta` ausente e ponte desligada por `--no-mt5`). `git diff --check` passou.
- Capturas de referência do usuário não estavam no diretório de anexos. O pedido de reenvio está pendente; usar as
  capturas reais quando disponíveis na T17.3. A validação em Windows com DPI 125%/150% está pendente em
  [validacao_instaladores.md](validacao_instaladores.md); nenhum resultado Windows foi declarado.
- T17.2 concluída em 2026-10-15 na worktree `../MISA_MT5-Extra-o-de-Dados-T17.2`, branch
  `tarefa/T17.2-componentes-apresentacao`, baseada no commit `a1ad9fe`. `ui/components.py` agora reúne cabeçalho,
  faixa de filtros, painel/cartão de métrica, badge semântico, aviso, painel de estado, gráfico, tabela, progresso,
  resumo comparativo e painel recolhível de detalhes. `ui/theme.py` configura estilos de status e fallback textual
  para glifos ausentes. As ações visíveis exigem callback; a camada não calcula analytics e as telas não foram
  integradas ainda.
- T17.2: baseline **814 aprovados, 10 pulados em 723,22 s**; testes focados **18 aprovados**; suíte final
  **827 aprovados, 10 pulados em 790,90 s**; `doctor` **16 OK, 2 avisos esperados, 0 falhas**; `git diff --check`
  passou. Avisos: `pandas-ta` opcional ausente e ponte MT5 desligada por `--no-mt5`. Windows real segue pendente.
- Verificação de preservação: a worktree original começou esta frente em `tarefa/T13.3-perfis-regimes` com
  alterações locais Analytics. Na checagem final estava limpa em `tarefa/T16.3b-enriquecimento-temporal`, HEAD
  `880bf18`; o reflog contém o commit `bde3652 feat(T13.3): add profile and regime features` antes da troca de
  branch. Nenhum comando desta tarefa alterou o worktree original.
- T17.3 concluída no Linux; próxima tarefa: T17.4 (demais abas), depois T17.5
  (validação visual integrada).

## Retomada T17.3 — 2026-10-15

- T17.3 iniciada na worktree `../MISA_MT5-Extra-o-de-Dados-T17.3`, branch `tarefa/T17.3-laboratorio-graficos`,
  baseada no commit `0d81adb` da T17.2. Instalação Linux com `./install.sh --yes --no-mt5` e dependências de
  desenvolvimento concluídas; `doctor` do instalador informou 16 OK, 2 avisos esperados, 0 falhas.
- Baseline antes das mudanças: **826 aprovados, 10 pulados e 1 falha em 660,14 s** no teste
  `test_settings_restore_the_arrangement_only_with_the_terminal_closed`; a mensagem de conclusão não chegou em
  0,6 s. Repetição isolada: **1 aprovado em 6,43 s**. Não alteramos a tela de Configurações, fora do escopo da T17.3.
- As cinco capturas originais do pedido não estavam disponíveis na worktree nem foram reenviadas; a comparação da
  T17.3 usa fixture sintética e fica registrada para incorporar as referências reais quando disponíveis.

## Conclusão da T17.3 — 2026-10-15

- Na worktree `../MISA_MT5-Extra-o-de-Dados-T17.3`, branch `tarefa/T17.3-laboratorio-graficos`, baseada em `0d81adb`,
  a visão geral do Laboratório passou a usar uma tabela rolável de 24 horários com valor, unidade, `n` e estado de
  amostra explícitos. Células sem operações mostram `— · n=0`; o asterisco indica operações positivas abaixo do mínimo.
- Horários, microestrutura e comparação agora oferecem subtelas para ler um gráfico por vez. Gráficos de barras e
  heatmaps mostram unidade, contagem e qualidade; heatmaps distinguem ausência de dados, e a correlação mantém a
  escala fixa −1…1 com legenda.
- Capturas em 1280×800, com MT5 desconectado, estão em `docs/img/interface-visual/T17.3/`: visão geral antes/depois
  em claro e escuro e análise temporal antes/depois em claro. Foram inspecionadas também as vistas Curvas, Calendário,
  Microestrutura indisponível e Correlação com dois robôs.
- Testes focados: **27 aprovados em 16,89 s**. Suíte completa: **831 aprovados, 10 pulados, 1 falha em 560,91 s**;
  a falha foi a mesma intermitência fora de escopo observada na baseline, em
  `test_settings_restore_the_arrangement_only_with_the_terminal_closed`; repetição isolada: **1 aprovado em 5,84 s**.
  `doctor`: **16 OK, 2 avisos esperados, 0 falhas**; `git diff --check` passou.
- As cinco capturas originais do pedido não foram reenviadas. Nenhum MT5 real foi conectado. Windows e DPI alto ficam
  pendentes para a T17.5/checklist; não se declara validação Windows.
- Próxima tarefa: T17.4, aplicação progressiva às abas restantes.

## Conclusão da T17.4 — 2026-10-15

- T17.4 concluída no Linux na worktree `../MISA_MT5-Extra-o-de-Dados-T17.4`, branch
  `tarefa/T17.4-aplicacao-abas`, baseada no commit T17.3 `ccde9ce`. Instalação com
  `./install.sh --yes --no-mt5` e dependências de desenvolvimento concluídas antes do trabalho; baseline: **832 aprovados,
  10 pulados em 618,76 s**.
- As abas Ativos, Minha base, Análise, Robôs, Recursos e Atividade agora reutilizam os componentes visuais existentes
  e reorganizam filtros, métricas, tabelas e detalhes para janelas menores. Ativos e Robôs mantêm os controles inferiores
  alcançáveis por rolagem; a tabela de parâmetros de robô pode ser rolada horizontalmente. Nenhum cálculo, contrato ou
  regra operacional foi alterado.
- Capturas antes/depois das seis telas, com MT5 desconectado e fixture sintética, estão em
  `docs/img/interface-visual/T17.4/`: antes/depois em claro a 1280×800, depois em escuro a 1280×800 e depois em claro a
  900×850. As vistas inferiores de Ativos e Robôs também foram capturadas a 900×620. As cinco imagens de referência
  originais citadas no pedido não estavam disponíveis e seguem pendentes.
- Verificação: teste responsivo focado **1 aprovado**; suíte completa final **833 aprovados, 10 pulados em 653,73 s**;
  `doctor` **16 OK, 2 avisos esperados, 0 falhas** (`pandas-ta` opcional ausente e ponte desativada por `--no-mt5`);
  `py_compile` e `git diff --check` passaram. A primeira suíte completa parou porque o `tmpfs` de sessão do pytest
  encheu; a repetição usou diretórios temporários em `/var/tmp` e terminou normalmente.
- Nenhum MT5 real foi conectado. A validação em Windows, DPI alto e 1920×1080 segue pendente para T17.5/checklist;
  não se declara validação Windows. Próxima tarefa: T17.5, validação visual integrada e documentação final da fase.

## Conclusão da T17.5 — 2026-10-15

- T17.5 concluída no Linux na worktree `../MISA_MT5-Extra-o-de-Dados-T17.5`, branch
  `tarefa/T17.5-validacao-visual`, baseada no commit T17.4 `c3f625b`. A instalação e as dependências de desenvolvimento
  foram preparadas antes da baseline; esta entrega fecha a documentação e as evidências sem alteração de cálculo ou API.
- As seis telas foram capturadas em claro/escuro a 1920×1080, com Xvfb em 96 DPI (`tk scaling=1,333`) e 144 DPI
  (`tk scaling=1,998`), em `docs/img/interface-visual/T17.5/`. Há capturas adicionais de Análise com 10 barras e de
  Ativos com cotação sintética grande. Os pares antes/depois 1280×800 e as capturas narrow 900×850/900×620 estão em
  `docs/img/interface-visual/T17.4/`.
- Fixture local sem MT5 real: 46 símbolos, 600 barras M1, 480 M15 e uma série curta de 10 barras. A Análise recusou o
  ativo sem dados e abriu as séries existentes. `Ctrl+F`, busca, Tab e resize foram exercitados; em 900×620 Ativos
  empilha os painéis e expõe rolagem, e em 1920×1080 volta ao leiaute lado a lado. O display Xvfb não oferece estado
  de maximização de um gerenciador real. As imagens originais do pedido continuam indisponíveis.
- Na baseline direta do desktop: **831 aprovados, 10 pulados, 2 falhas intermitentes** (teste de desempenho de agregação
  em 1,59 s frente ao limite de 1,5 s; teste responsivo sem conseguir reduzir a janela desktop de 1280 px). As duas
  repetições isoladas passaram, com a responsiva em Xvfb. Suíte final completa em Xvfb 1280×800: **833 aprovados,
  10 pulados em 674,22 s**. `doctor`: **16 OK, 2 avisos esperados, 0 falhas** (`pandas-ta` opcional ausente e ponte
  MT5 desligada); `git diff --check` passou.
- A Fase 17 está concluída no Linux. Windows real, DPI de 125%/150%, maximização pelo sistema e glifos/fontes do Tk
  continuam na pendência T17.1–T17.5 de `docs/validacao_instaladores.md`; não se declara validação Windows.

## T17.7 — referência temporal e população (2026-10-16)

- Implementação em branch `codex/t17-7-contexto-temporal`, PR #81 draft, base direta
  master. Sem merge. PRs #79/#80 permanecem separados e sem autorização de integração.
- Horários identifica entrada/saída e população; seleção mista explicita saídas
  excluídas da matriz sem alterar os KPIs. Visões temporais identificam referência
  mista. Sem mudança de agregação, valores, risco ou ordens.
- Baseline 837 aprovados/9 ignorados em 726,65 s; primeira suíte posterior
  838 aprovados/9 ignorados em 583,13 s; testes focados finais 6 aprovados.
  Revalidação no commit 061f751: **838 aprovados, 9 ignorados em 705,84 s**, saída 0.
- Dados reais em cópia consistente: 263 operações parciais, temas claro/escuro,
  1280×800/1920×1080, exportação UI de 12405 barras conferida; encerramento saída 0.
  Doctor principal aprovado; worktree deliberadamente offline acusa caminho MT5 vazio.
  Nenhum shell alterado; Windows real pendente.
- DPI144 desde a criação do Tk revelou cortes no cabeçalho/filtros e matriz sem
  altura em 1280×800: problema crítico ainda aberto, T17.9 proposta. T17.8 proposta
  remove suavização existente em Relações. WIP T17.6 preservada byte a byte.
- Auditoria e evidências em [AUDITORIA_UI_20261016.md](propostas/AUDITORIA_UI_20261016.md).
  Aceite visual integral e campanha PF ≥ 1,9 permanecem pendentes.

## T17.9 — responsividade do Laboratório (2026-10-16)

- Branch `codex/t17-9-laboratorio-responsivo`, PR #82 draft, empilhado sobre #81.
  Código em 972658c. Sem merge. Contexto de seleção fixo, cabeçalho/filtros
  reorganizados, resultados roláveis com área útil, colunas medidas pela fonte
  e rolagem horizontal; componente reutilizado de Relações, sem alteração analítica.
- Baseline 838 aprovados/9 ignorados em 697,62 s. Focados 31 aprovados em 44,07 s.
  Suíte completa no commit 972658c: **840 aprovados, 9 ignorados em 733,11 s**, saída 0.
- Inspeção real iterativa com 263 operações parciais: DPI144 desde o início do Tk,
  1280×800/1920×1080, claro/escuro e rolagem; nova rodada em DPI96 incluiu 900×620.
  Cortes de aviso e valores encontrados na primeira rodada foram corrigidos na
  segunda. Capturas em `docs/img/interface-visual/T17.9/`.
- Doctor isolado 15 OK/2 avisos/1 falha por caminho MT5 deliberadamente vazio;
  principal validado separadamente. Windows real pendente, nenhum shell alterado.
- Pedido posterior do dono: atenção discreta a positivos/negativos/ausências em
  Horários e Minha base. T17.12 planejada reutilizando seletivamente T17.6
  preservada, sem importar suavização. Pré-requisito T17.12a preserva métricas
  ausentes e distingue zero/perda/sem resultado na apresentação compartilhada.
  Aceite visual global e campanha PF ≥ 1,9 continuam pendentes.

### Revalidação CI da T17.9 no Windows

O job Windows de uma branch descendente retornou duas colunas após o teste pedir
1920 px; a asserção exigia quatro. O teste foi corrigido na tarefa de origem
para verificar todos os controles mapeados e inteiros dentro dos limites reais
do painel, mantendo a checagem dos valores. A causa precisa da diferença de
fontes/DPI/tamanho efetivo no runner não foi medida. Não se exige um número fixo
de colunas quando o conteúdo precisa refluír. Nenhuma asserção de acesso, valor,
seleção ou rolagem foi retirada. Windows manual continua pendente.

## T17.12a — disponibilidade e apresentação dos resultados (2026-10-16)

- PR #83 draft, branch `codex/t17-12a-disponibilidade-resultados`, base #82.
  `asset_table` preserva métricas ausentes/não finitas como indisponíveis, mantém
  o run mais recente e mostra zero e percentuais negativos explicitamente.
  Nenhuma fórmula financeira, banco, risco ou ordem muda.
- Baseline: 840 aprovados/9 ignorados em 745,70 s. Focados: 14 aprovados em 1,87 s.
  Primeira suíte posterior: 847 aprovados/1 falha/9 ignorados em 553,43 s: feedback
  de conexão ainda em andamento após espera fixa de 0,8 s no teste de falta de
  internet, já registrado como intermitente. Repetição isolada passou em 5,64 s;
  repetição completa no mesmo a886351: **848 aprovados, 9 ignorados em 556,06 s**,
  saída 0. Nenhum teste foi removido, pulado ou enfraquecido.
- 271 runs completos/13 séries reais mantêm células e retornos iguais; UI Minha
  base inspecionada, com zero/perda existentes agora visíveis. Capturas
  `docs/img/interface-visual/T17.12a/`. Doctor offline: 15 OK/2 avisos/1 falha
  por caminho MT5 vazio deliberado; principal validado separadamente.
- T17.12 aplica a hierarquia discreta de atenção pedida pelo dono. Uma prévia
  privada com dados reais confirmou o estilo; o código visual ainda não foi
  incorporado nesta tarefa. WIP T17.6 preservada. Sem merge; PF pendente do aceite.

## T17.12 — hierarquia discreta de atenção (2026-10-16)

- Branch `codex/t17-12-hierarquia-atencao`, dependente de #83. Horários recebe
  destaque azul suave nos positivos, perdas sóbrias e ausência neutra. Minha base
  usa a mesma semântica, com seleção nativa destacada e qualidade legível.
- Sem mudança de cálculos ou dados: n e asterisco de pouca amostra permanecem;
  traço sem operações remete a n=0 na legenda. Positivo não significa validação.
- MatrixTable reutilizada seletivamente do WIP T17.6, preservado integralmente;
  nenhum gradiente por magnitude ou suavização importado. Teclado e cabeçalho fixo
  validados. 30 testes focados aprovados; suíte completa no 5388ec8: 852 aprovados,
  9 ignorados em 598,30 s, saída 0. PR #84 draft.
- Operação real com cópia consistente da base, sem terminal, em ambos os temas,
  DPI96/144 e resoluções 1280×800/1920×1080; capturas em
  `docs/img/interface-visual/T17.12/`. Doctor offline tem falha deliberada de path
  MT5; Windows visual manual e aceite global pendentes. #83 CI completo verde.
- Nenhum PR foi mesclado. Campanha PF ≥1,9 continua posterior ao aceite visual
  e à contabilidade de custos; nenhum resultado financeiro novo foi produzido.

- Revalidação do host principal após T17.12: doctor 21 OK/1 aviso/0 falhas.
  T17.6 preservada, incluindo 22 arquivos não rastreados e diffs staged/unstaged.

## T17.8 — traço observado em Relações (2026-10-16)

- Branch `codex/t17-8-traco-observado`, base T17.12/#84. Remove interpolação
  cúbica da timeline: agora um ponto por janela recebida, com NaN preservando
  lacunas; traço reto explicitado na legenda. Estados, marcadores, tabela, seleção
  e gestão de callbacks permanecem. Fonte continua fixture sintética declarada,
  sem vínculo com base real/serviço F18; não há resultado financeiro nesta entrega.
- Ambiente próprio instalado e dev. Baseline própria: 851 aprovados/1 falha/9
  ignorados em 653,29 s, teste de conexão com espera fixa de 0,8 s já intermitente;
  repetição isolada passou (1 em 6,04 s). Supervisor terminou 143 antes do filho,
  que escreveu o resumo; exit do filho não capturado. Não chamar baseline verde.
  A base de código foi aprovada separadamente na T17.12: 852/9, saída 0.
- Focados: 11 aprovados em 23,10 s; cenário de lacuna interna/seleção nos dois
  temas repetido após a última asserção: 1 aprovado em 2,85 s. Suíte completa do
  código b9fd90a: 852 aprovados/1 falha/9 ignorados em 652,79 s, saída 1.
  A falha repetiu o mesmo teste de conexão/espera fixa da baseline; não é suíte
  global aprovada. Correção da sincronização exige tarefa separada.
- Aplicação aberta com base vazia de auditoria, terminal desativado, Relações
  explicitamente sintética, ambos os temas, DPI96/144 e 1280×800/1920×1080.
  Capturas em `docs/img/interface-visual/T17.8/`; doctor offline 15 OK/2 avisos/
  1 falha deliberada por MT5.path vazio. Host principal: 21 OK/1 aviso/0 falhas.
- Auditoria encontrou espaço vertical ocioso abaixo da área analítica enquanto
  a timeline exige rolagem interna, especialmente em 1920×1080. Próximo recorte
  de ocupação da altura deve preservar mínimos/rolagem e ser testado separadamente.
- Sem merge, operação em conta ou campanha PF. Revisão/integração pendentes.

## T17.15 — sincronização do teste de conexão (2026-10-16)

- PR #86 draft, branch `codex/t17-15-sincronizacao-conexao`, base #85;
  ambiente próprio preparado e baseline completa da T17.8 registrada com a
  mesma falha repetida de espera fixa. Somente teste/documentação nesta tarefa.
- O cenário de internet/conta/recuperação aguarda `_connecting` terminar com
  prazo de10 s, mantendo todas as asserções e verificando botão disponível.
  Parametrizado com worker imediato e worker de1 s para ultrapassar os0,8 s
  anteriores. Nenhuma mudança de conector, runtime ou negociação.
- Focados: 5 aprovados/32 não selecionados em23,75 s. Suíte completa20765d8
  aprovada: **854 aprovados/9 ignorados em765,21 s, saída0**. Doctor do mesmo runtime
  offline já registrado15 OK/2 avisos/1 falha deliberada de MT5.path vazio.
- Operação adicional de UI sobre cópia real: diálogo15séries×15robôs, limpar/
  selecionar/cancelar sem iniciar; lote histórico EURUSD H1 com6estratégias
  Python e1worker, quatro concluídos/zero falhas/dois pendentes cancelados.
  Resultados negativos preservados, dados marcados como expostos à pesquisa.
  Não é campanha PF19 nem reserva independente. Sem ordem ou terminal.
- T17.14 preparada em worktree próprio, baseline rodando antes de modificar
  layout. T17.16 proposta após seletor H4 mostrar resultado H1 anterior e botão
  cortado no detalhe do robô; sem mudança de cálculos. Nenhum merge realizado.

## T17.14 — altura da área analítica (2026-10-16)

- Branch `codex/t17-14-altura-analitica`, baseT17.15/#86, ambiente próprio
  instalado/dev e baseline854 aprovados/9 ignorados em768,23 s, saída0.
- ScrollableContent preenche altura disponível sem suprimir altura requerida;
  cálculo desconta borda e usa layout estabilizado para textos com quebra de
  linhas. Callback único, cancelado ao destruir o componente. Sem dados/cálculos.
- Rodadas: primeiro teste detectou2px de rolagem artificial e foi corrigido;
  teste de limite passou. Rodada seguinte34 focados aprovados em44,07 s; inspeção
  de resize detectou atraso na altura após wrap, corrigido e coberto por novo
  teste. Três casos de rolagem/wrap/Destroy passaram em0,79 s. Focados finais:
  35 aprovados em52,74 s; suíte40853f2:856 aprovados/9 ignorados
  em713,62 s, saída0. Suíte anterior60d8cc2 foi interrompida143 por
  código supersedido, sem afirmar sucesso.
- Operação antes/depois clara/escura,DPI96/144,1280×800/1920×1080 e ciclo de
  resize: contêiner maior preenche830 dos832px em96DPI (2px são borda); viewport
  interno sobe267→468px. Medidas exatas preservadas após estreitar/alargar,
  gráficos com altura mínima preservada. Capturasdocs/img/interface-visual/T17.14.
  Relações continua explicitamente sintética, sem vínculo à base local vazia.
- Limites independentes: detalhe lateral143px emDPI144(T17.17), seletor/resultado
  e botão de Backtest(T17.16), retornozero emAnálise(T17.13), empates/lacunas
  temporais(T17.10/T17.11). Aceite global, revisão/merge e PF19 pendentes.

- Doctor deste recorte:15 OK/2 avisos/1 falha porMT5.path vazio deliberado
  e ponte desativada; não é diagnóstico verde. Host principal segue diagnóstico
  separado21 OK/1 aviso/0 falhas. PR #87 draft, sem merge.

## T17.17 — detalhe legível em Relações (2026-10-16)

- Branch `codex/t17-17-detalhe-legivel`, baseT17.14/#87. `_split_view` mede
  reserva de texto pela fonte real e mantém proporção das colunas; se não
  couber, empilha e conserva rolagem e mínimos dos gráficos/tabelas. Nenhuma
  mudança de dados/seleção/estados/engine; fonte continua sintética declarada.
- Ambiente próprio instalado/dev; baselinead31f06:856 aprovados/9 ignorados
  em732,12 s, saída0. Focados finais12 aprovados em30,05 s. Suíte completa
  e8bdb11:857 aprovados/9 ignorados em808,56 s, saída0.
  O resultado não substitui a revisão independente nem oCI noSHAfinal.
- Aplicação operada emdois temas,DPI96/144,1920×1080/1280×800/820×800 e
  retorno à janela grande. Em144DPI, detalhe143→441px (mínimo388) em1920;
  em1280e820 empilha1154/774px; gráfico344px preservado. Capturas
  `docs/img/interface-visual/T17.17/`. Nenhum resultado sintético chamado real.
- Próximos problemas:T17.16 contexto/backtestbotão;T17.13 retornozeroemAnálise;
  T17.10/T17.11 semântica deempates/lacunas exige núcleo. Revisão/integração
  ainda pendentes; #79/#80 fora da autorização nova solicitada, nenhuma conta.

- Inspeção adicionalDPI144/1280: PageDown pelo teclado e rolagem levam ao
  detalhe empilhado; seleção OOS atualiza o detalhe. Sessão própria encerrada0.
  Doctoroffline15 OK/2 avisos/1 falha deliberada deMT5.path vazio; hostprincipal
  comdiagnóstico separado21 OK/1 aviso/0 falhas. PR #88 draft, semmerge.
- CI da dependência #86: primeiratentativaWindowstimeout60 s emfixtureSQL
  inalterada; uma reexecução no mesmo8246686 passou, todoschecksverdes. Falha
  inicial preservada nos logs e descrição, sem alterar timeout ou testes.

- CI Windows do 2706c66 revelou duas falhas reais: painel interno recebia
  novamente os 4 px já reservados pelo split. Removido espaçamento duplicado
  de Linha do tempo, Distribuição e Convergência, preservando as exigências de
  largura dos testes. Log inicial preservado; validação da correção em execução.

- Correção de espaçamento: 13 testes de Relações aprovados em58,77 s;
  operação96/144DPI e ciclo1920→1280→820→1920, ambos temas, confirmou
  detalhe acima do mínimo. DPI144:445px(min388) em1920, empilha nas menores;
  DPI96:453px em1920 e292px(min276) em1280. Interfaces encerradas0.
  Capturas de demonstração identificadas SYNTHETIC; suíte completa
  final b35f274:858 aprovados/9 ignorados em1118,60 s, saída0. Windows
  do mesmo commit aprovado em20m47s.

- Primeira repetição local do ajuste:784 aprovados/83 ignorados, saída0;
  não aceita para QA porque74 testes gráficos ficaram sem display após falha
  do Xvfb no roteiro privado. Mantido log da tentativa. Roteiro corrigido
  (-noreset e displayfd aberto), repetido mesmo SHA:858/9, sem exclusões
  gráficas. Os nove ignorados esperados são integrações opcionais separadas
  e são executadas em seus jobs do CI. Nenhum teste alterado para contornar
  a falha do ambiente. Doctor:15 OK/2 avisos/1 falha deliberada MT5.path vazio.

## T17.16 — contexto e controles do Backtest (2026-10-16)

- Branch `codex/t17-16-contexto-backtest`, baseT17.17/#88; ambiente próprio
  instalado/dev, baseline9f40e96:857 aprovados/9 ignorados em823,04 s, saída0.
- Seleção para próximo teste separada do resultado armazenado; após teste desta
  sessão, a proveniência da seleção/base identifica compatibilidade ou resultado
  anterior. Histórico não recebe equivalência de período presumida. Erro não
  apaga resultado/curva, pedido duplicado não inicia, callback de outro robô não
  muda o detalhe atual. Hook existente atualizaAtivos/MinhaBase imediatamente.
- Controles refluem4/2/1colunas eBacktest usaScrollableContent daT17.14.
  Três focados aprovados em21,68 s, incluindo erro/seleção/troca de robô; geometria
  DPI144:1 aprovado em11,82 s. Suíte58cb114:859 aprovados/9 ignorados
  em937,37 s, saída0. PR #89 draft.
- Operação real: cópia privada consistente da base, dois temas,DPI96/144 e
  1280×800/1920×1080; botão inteiro, PageDownacessa resultado. Novo testePython
  EURUSDH1 concluído, texto da seleção atual confirmado e muda paraanterior após
  alterarperíodo. Dados expostos registrados, semOOS/campanhaPF19, nenhuma conta
  outerminal. Evidênciasdocs/img/interface-visual/T17.16 e logsprivados.
- Semmerge; aceitevisualglobal/revisão/integração pendentes.

- DoctorofflineT17.16:15 OK/2 avisos/1 falha deliberadaMT5.pathvazio;
  principal preservada comdiagnóstico separado21 OK/1 aviso/0 falhas.

## T17.13 — retorno de mercado com atenção coerente (2026-10-16)

- Branch `codex/t17-13-retorno-mercado`, base T17.16/#89. Ambiente próprio
  instalado e baseline 859 aprovados/9 ignorados, saída 0, antes do código.
- Retorno positivo recebe azul suave, negativo discreto, zero neutro e ausência
  explícita. Sinais textuais preservam a leitura sem depender da cor. Legenda
  distingue retorno de preço e lucro de robô. Mesmos tokens de T17.12; sem
  mudança em fórmulas, candles ou banco.
- Oito testes focados aprovados em 42,24 s; incluem zero, ausência, NaN/Inf,
  tema, limpeza e contraste mínimo 4,5. Suíte final 65b13d0: 865 aprovados/9 ignorados em1251,92 s, saída0. PR #90 draft.
- Interface operada em cópia consistente da base real EURUSD H1: resumo
  idêntico ao anterior após normalizar somente datas para JSON. Dois temas,
  96/144 DPI, 1280×800/1920×1080; cenários zero/ausência rotulados CONTROLLED.
  Capturas em docs/img/interface-visual/T17.13. Tentativas do roteiro com
  serialização incorreta/rodapé antigo foram retidas nos logs e substituídas
  por novas capturas, sem editar imagens.
- Doctor isolado: 15 OK/2 avisos/1 falha de MT5.path vazio deliberado.
  Nenhuma operação em conta, nenhum merge. Clipping preexistente da Análise
  em 144 DPI/1280 permanece achado separado, sem ampliar este recorte.

## T17.10 — operações zeradas distintas de perdas (2026-10-16)

- Branch `codex/t17-10-resultados-zerados`, base T17.13/#90. Ambiente próprio
  instalado; baseline no88cbe6c:865 aprovados/9 ignorados em1114,07 s, saída0,
  antes do código. Correção de espaçamento da dependência T17.17 reconciliada.
- Reprodução real BBRSI:10.777 operações,4.071 ganhos,6.366 perdas,340 zeradas;
  o gráfico anterior mostrava6.706 perdedoras. Novo `aggregate.outcome_counts`
  classifica o PnL no núcleo (>0/<0/=0/ausente ou inválido), preservando a
  dimensão ausente e a população. A UI renderiza as séries prontas e omite
  categorias sem operações da legenda. Ganhos em azul, perdas discretas com
  trama, zeradas neutras. Não muda PF, expectativa, equity ou ledger/schema.
- Quatorze testes do núcleo aprovados em3,00 s e oito de Laboratório em31,62 s;
  incluem zero, ausentes/não finitos/booleanos, categoria vazia, dimensão ausente,
  métricas preservadas, dois temas e atualização da seleção sem série antiga.
  Suíte completa no85b81ef:875 aprovados/9 ignorados em944,54 s, saída0; PR #91 draft.
- Aplicação real em cópia consistente, ambos temas,96/144 DPI e1280/1920:
  barras reconciliadas com4.071/6.366/340 e soma10.777. Ledger antes/depois
  com hash idêntico; quick_check dos dois bancos OK. Contagens CSV geradas pela API do núcleo e gráfico
  exportado conferidos nos artefatos privados. PageDown e rolagem alcançam a
  base do gráfico em144 DPI; sessões próprias encerradas0. Capturas em
  docs/img/interface-visual/T17.10. Dados existentes expostos, sem novo backtest
  ou campanha financeira; esses agregados não são evidência OOS.
- Doctor isolado:15 OK/2 avisos/1 falha deliberada de MT5.path vazio. Principal
  integrada até #87 tem diagnóstico separado21 OK/1 aviso/0 falhas, ponte
  consultada em modo somente leitura; nenhuma operação em conta.
- Pendências separadas observadas: aviso parcial atribui origem só a MQL5 mesmo
  na seleção Python; densidade dos rótulos horários em DPI alto e contexto
  completo nos exports de gráficos exigem recortes próprios. Não é aceite
  visual global, nem demonstração da meta financeira.

## T17.11 — grade temporal e lacunas explícitas (2026-10-16)

- Branch `codex/t17-11-lacunas-horarias`, base T17.10/#91. Ambiente próprio
  instalado; baseline no 1851b1d: 875 aprovados/9 ignorados em 917,57 s, saída 0,
  antes do código. Pai e5ec314 reconciliado, com documentação adicional.
- Reprodução real MQLExemplo: 263 operações parciais, 37 faixas; 00:00 e 02:30
  apareciam em posições consecutivas e conectadas. Novo `hourly_summary` fornece
  grade de 24 h pela configuração do enriquecimento. Faixas com n=0 têm medidas
  ausentes. Metadados separam horário inválido e PnL indisponível; as medidas
  observadas continuam vindo de `by_dimension`.
- O componente de barras preserva None/nonfinite como ausência. A linha quebra
  em NaN; o espaçamento dos rótulos do eixo preserva todas as faixas e medidas.
  n=0 é distinto de amostra pequena e retorno zero. Ledger, cache, fórmulas
  financeiras e parâmetros foram preservados.
- 29 testes de núcleo/desenho aprovados em 7,78 s e 9 de UI em 38,51 s, incluindo
  grades configuráveis, vazio, exclusões, zero observado, segmentos e temas.
  Suíte completa no 3b4e6d1: 886 aprovados/9 ignorados em 879,42 s, saída 0. PR #92 draft.
- Operação real nos dois temas, 96/144 DPI e 1280×800/1920×1080: 48 posições,
  37 medidas, 11 lacunas e 4 segmentos. Expectativa, acerto, n e marca de amostra
  dos grupos observados comparados com a tabela original e preservados. Rótulos
  sem sobreposição medidos no renderer; PageDown/rolagem exercitados. CSV de
  verificação da API e gráfico exportado conferidos. Referência permanece Saída,
  sem inferir entrada. Capturas em docs/img/interface-visual/T17.11.
- Sessões encerradas com saída 0. A primeira gravação da auditoria falhou ao
  serializar um escalar NumPy; erro preservado e normalização corrigida no
  roteiro, mantendo valores e capturas. Doctor isolado: 15 OK/2 avisos/1 falha
  deliberada de MT5.path vazio. Não houve novo backtest, campanha PF19 ou
  operação em conta nesta tarefa.

## T17.20 — restauração com sincronização observável (2026-10-16)

- Branch `codex/t17-20-sincronizacao-arranjo`, base da principal após #89.
  Ambiente próprio instalado; baseline b9e54c9: 860 aprovados/9 ignorados em
  868,84 s, saída 0, antes do código.
- Duas execuções Windows do a893f54 falharam na espera fixa de 0,6 s do teste
  antigo. Logs e identidade da fixture/runtime preservados. A primeira correção
  local revelou outra etapa: feedback encerrado antes do refresh que reabilita
  o botão. Esperar apenas a mensagem permitia ignorar a segunda invocação.
- O teste agora aguarda callback e estado esperado do diálogo com prazo finito,
  incluindo o primeiro refresh. Cenários de restauração imediata e I/O falso
  atrasado em 1 s; nenhum MT5 real. Todas as verificações originais de proteção
  e bytes UTF-16 mantidas, com conferência por AST; novas verificações confirmam
  nenhuma restauração com terminal aberto e uma única chamada quando fechado.
- Quatro focados aprovados em 18,68 s. A tentativa local anterior, com 3 aprovações
  e 1 falha, permanece registrada. Suíte completa no cc13d0e em execução; resultado
  final pendente. Diff de runtime vazio: não houve alteração da aplicação.
- Doctor isolado: 15 OK/2 avisos/1 falha deliberada de MT5.path vazio. Nenhuma
  conta, configuração ou arquivo do terminal do usuário foi alterado.

## ORG05 — estado da integração visual (2026-10-16)

A principal eed5f34 reúne os PRs #81–#89 e #93, integrados separadamente após revisão
técnica do agente executor, checks no head final e smoke no módulo instalado com
configuração/dados privados. O trabalho original T17.6, as configurações e as
bases do usuário foram preservados. Nenhum agente adicional foi acionado.

Estado por PR, commits de merge, verificações no host, falhas preservadas e
pendências estão em [Continuidade da integração visual](propostas/CONTINUIDADE_INTEGRACAO_UI.md).
O painel do plano foi reconciliado com essas integrações. #90 recebeu a correção
separada do teste assíncrono (#93) e aguarda novo CI; #91/#92 possuem implementação, testes e
operação real registrados, mas ainda dependem da cadeia de integração.

O smoke histórico do #89 manteve as 129 operações, PF 0,637667 e resultado
negativo da execução anterior; ledger reconciliado e contexto anterior após
alterar período. São dados previamente expostos, sem campanha/OOS. A meta
financeira não foi demonstrada. Aceite global da UI, dados/custos e pesquisa
permanecem frentes explícitas. #79/#80, releases e contas seguem separados.

ORG05 altera somente documentação. Baseline a13c2c6: 856 aprovados/9 ignorados,
saída 0; base depois reconciliada com b9e54c9. Suíte final c3d3785: 860 aprovados/9
ignorados em 591,95 s, saída 0, após repetir a execução interrompida por reboot.
Depois recebeu #93, mantendo diff próprio somente documental. Doctor atual da
principal após restaurar apenas o servidor RPC: 21 OK/1 aviso/0 falhas. Pedido
de informações recolhíveis registrado em T17.21, ainda sem código neste snapshot.
## T17.18 — Análise e cabeçalho respeitam a fonte (2026-10-16)

- Ambiente isolado instalado/dev; baseline a893f54 repetida após reboot com
  866 aprovados/9 ignorados em623,39 s, saída0. Recebeu correção test-only #93.
- Filtros agrupados e cartões refluem pelo tamanho pedido pela fonte e valor;
  resultados em ScrollableContent existente, leitura OHLC quebrada e figuras
  com altura mínima em contêiner próprio. O contêiner evita que o backend
  Matplotlib/DPI sobrescreva o tamanho pedido do canvas. Cabeçalho mantém
  nome/conexão/ações legíveis; a legenda secundária só aparece quando cabe.
- Primeira tentativa entrou em ciclo de geometria; execução interrompida e
  logs preservados. Chave de layout e retirada do reset transitório de wrap
  estabilizaram a atualização. Dez focados existentes passaram em47,44 s;
  cinco de geometria em40,48 s; após contêiner,14 em74,99 s; fonte maior e
  número extenso:cinco em43,92 s. Assertivas nativas de largura, bounds e
  não sobreposição, PageDown, dados inalterados e cancelamento ao destruir.
- Aplicação operada com EURUSD H1 real,1.537 barras e resumo idêntico: dois
  temas,96/144 DPI,1920/1280/900 e leitura OHLC. Na escala real o backend
  reduziu o requisito do canvas; a correção do contêiner manteve a altura.
  O primeiro roteiro de hover sintético perdeu o readout num Leave nativo;
  repetido em144 DPI com movimento real do mouse, verificação aprovada.
  As duas sessões finais encerraram0. Nenhuma fórmula/candle/indicador mudou.
- Roteiros passaram a aguardar a base disponível antes de abrir o ativo;
  abrir cedo acionava o diálogo informativo previsto, não falha no produto.
  Suíte completa/doctor/CI e integração ainda pendentes neste snapshot.

- A revisão dos valores de geometria revelou aumento de1347→2051 px após
  redimensionar/alternar tema em DPI real. O full076053b foi interrompido com
  saída143 antes de alterar código; esse resultado não é aprovação. Contêiner
  agora conserva requisito próprio e desliga propagação do grid, evitando
  realimentação do requisito do backend. Teste verifica limites mínimo E máximo.
- Cinco focados com fonte maior, valor extenso e limites passaram em29,24 s.
  Operação real repetida:144 DPI ficou em600 px nos dois temas e1920/1280/900;
  96 DPI em480 px nas janelas menores e609 px quando há área. Texto/candles e
  resumo das1.537 barras idênticos; hover por mouse real, teclado e rolagem
  conferidos. Duas sessões encerradas0. Capturas finais substituem somente as
  imagens geradas nesta tarefa; tentativas anteriores mantidas nos artefatos.
- Resultado da suíte final/doctor/CI e revisão é atualizado no PR; não houve
  operação em conta, nova pesquisa financeira ou modificação de base do usuário.

## T17.21 — informação recolhível sem perder a seleção (2026-10-16)

- Ambiente próprio instalado/dev; baseline eed5f34 com 861 aprovados/9 ignorados
  em653,15 s, saída0, antes do código. Reutiliza DetailsPanel, sem dependências.
- Variante de informação com botão ℹ e texto de ação, Enter/Espaço e Escape;
  recolher conserva o texto e devolve foco ao botão quando necessário. Seis
  detalhes de seleção começam fechados; mudança de seleção atualiza o conteúdo
  sem expansão automática. O split devolve largura/altura à análise e conserva
  a largura de leitura de T17.17 quando aberto.
- Critérios extensos dos estados e explicação do traçado ficam sob informação;
  contexto primário e avisos sintéticos/custos/ausência/validade permanecem
  expostos. Recorte avançado mantém seus dados e passa ao mesmo botão informativo.
- Trinta testes existentes de componentes/Relações passaram em23,42 s; com
  os novos contratos de teclado, foco, texto fechado, seleção e ganho de área,
  33 passaram em28,97 s. Verificações anteriores de detalhe legível permanecem
  exercitadas com o painel explicitamente aberto; assertivas do componente
  original conferidas por AST. Suíte final/operação visual/doctor pendentes.
- As referências são a descrição do usuário e as telas atuais; anexos temporários
  ausentes não foram alegados como inspecionados. Relações continua demonstração
  sintética, sem ligação ao DTO/serviço F18. Não houve cálculo ou mudança de dados.

- Suíte no43a14b5 concluída: 864 passed, 9 skipped in 609.05s (0:10:09), saída0. Nas capturas da
  operação visual, texto/seleção/gráfico/tabelas permaneceram idênticos; clique
  real por mouse e Escape/foco funcionaram em96/144 DPI e três larguras.
  O glifo ℹ, apesar de presente no texto, não apareceu na fonte do host. Antes
  do aceite final, será substituído pelo ícone desenhado do sistema existente
  (`ui/icons.py`), com imagem não vazia e atualização por tema verificadas.
  A suíte anterior permanece evidência da versão anterior, não da correção.

- Ícone desenhado implementado no sistema Pillow/PhotoImage existente, com
  tamanho pela fonte, contraste do tema e rótulo acessível. Trinta e três
  focados passaram em27,72 s; tentativa anterior teve NameError na variável
  do próprio teste, corrigida sem mudar o contrato. Fotos novas96/144 DPI
  mostram o ícone; clique real, Escape/foco, paridade de texto/gráfico/tabela
  e seis perfis por escala passaram. As duas sessões encerraram0.
- Validação final após a correção e reconciliação com a principal é registrada
  no PR desta tarefa; a suíte864/9 anterior não substitui essa execução.

## Retomada T17.18 — altura nativa em DPI alto

- Validação combinada d05106f:896 aprovados/9 ignorados em742,33s, saída0.
  O CI Windows encontrou uma falha nova no limite superior da figura:892px
  para teto864px com fonte maior. Sem repetição cega ou tolerância adicional: o
  código deixa de usar a altura requisitada inicial do backend, que também pode
  incorporar DPI, e usa480px ou24linhas da fonte. Novo teste injeta pedido
  inicial de1920px e conserva as mesmas exigências de limite mínimo/máximo em
  duas escalas. As execuções anteriores ficam preservadas; resultado final do
  novo código e novo CI serão registrados antes da integração.

- O CI Windows do acb617f ainda encontrou excesso de altura:989px na regressão
  injetada e892px no resize, para teto864px. O diagnóstico local confirmou que
  o pack podia expandir a figura pela área atribuída à página do Notebook.
  Agora somente a largura vem do pack; a altura é calculada pela fonte e pelo
  espaço útil do viewport, descontando cartões, legenda, abas e leitura OHLC.
  Abas ocultas conservam o mínimo e a seleção/resize recalcula a aba visível.
  A regressão também força uma página de2048px: o fonte imutável acb617f falha
  nas duas escalas no host (2009>550 e2002>744); o novo código passa nos mesmos
  cenários. Cinco focados finais:38,37s. O primeiro roteiro de reprodução teve
  erro de importação de fakes (saída4), corrigido somente no harness; não foi
  contado como demonstração da regressão. Nenhum limite foi relaxado.

## T13.9 — PF definido e compatibilidade histórica (2026-10-16)

Implementação em PR97, sem campanha financeira. Um contrato comum remove tetos
99/999, representa vazio/sem perdas/inválido como null com motivo e conserva
razões finitas. Validação/bateria não aprovam por indefinição; consumidores
aceitam ausência. Projeções antigas recalculam PF quando o livro está completo
e mantêm os registros originais, inclusive aprovações de bateria que precisam
ser refeitas quando vinculadas a PF indefinido. Banco antigo continua abrindo.

Ambiente instalado/dev e baseline887/9 anteriores ao código. Focados156, depois
93 e56 nas revisões; suíte240f92a com926/9 em681,75s, saída0. O head final recebe
ajuste de proveniência JSON/ordenação, com validação específica e integral no
PR. Uma suíte anterior foi encerrada241 para incorporar a revisão, não contada
como aprovação. Doctor isolado15/2/1deliberado; principal21/1/0 separado.

Cópia pré-exposta:276 execuções e chaves únicas,14 vazias com PF0 sem denominador;
276 livros e4.140 valores de outras métricas preservados. UI real263operações
continua PF0,87; testes visuais controlados identificados verificam sem perdas
e razão2000, com motivo legível em144DPI. Não demonstram meta financeira.
Detalhes de escopo, limites e compatibilidade em
[PF definido](propostas/T13_9_PF_DEFINIDO.md).

## T17.22 — análise local explícita no ponto de entrada (2026-10-16)

Opção `--offline` implementada, sem persistência no INI. O app usa a lista
local de símbolos, inventário e análises; o conector dispensa SDK e caminho de
terminal nessa sessão. Inicialização/encerramento/erro não alcançam o SDK.
Cabeçalho neutro identifica análise local, ações de conexão/abertura/admin são
desativadas e explicam como reabrir; chamadas diretas também são guardadas,
inclusive a oferta de conta demo. O modo padrão permanece disponível. O fim
do programa encerra apenas seu próprio conector, em lugar de resolver o SDK
global incondicionalmente. Não altera estratégia, dados ou risco.

Baseline 8bacadf:890 aprovados/9 ignorados em810,86s, saída0, antes do código.
Focados iniciais13/12,14s; após contagem explícita das chamadas bloqueadas,
parser/default/help/cleanup e guardas de UI:8 aprovados em16,56s. Subprocesso
com a classe real/main abriu600 barras sintéticas de teste, desenhou análises
e encerrou sem chamada ao SDK/diálogos, mantendo o INI idêntico. Testes de
conector simulam Windows/Linux e preservam outro backend já existente.
Recebeu a cadeia T13.9/T17.18, com diff próprio restrito ao início local;
15 focados combinados em53,78s. Suíte final, CI, operação instalada pelo
lançador e diagnóstico serão registrados no PR. Windows real permanece
pendente; nenhum terminal/conta do usuário foi iniciado ou encerrado.

- A inspeção nativa do modo local encontrou a lista de símbolos configurados
  ainda chamada de “ativos da conta” e ações de extração/coleta habilitadas.
  Rótulos agora identificam a lista local; extração/coleta e chamadas diretas
  ficam desativadas, com a ação de reabrir explicada. Análises/importação
  continuam disponíveis. Oito focados passaram em16,47s após esse ajuste.
  A suíte56f73b2 foi interrompida241 antes da revisão, sem alegar aprovação;
  novo head tem suíte/CI próprios. Primeira inspeção offline1537barras saiu0.

## T13.10 — estudo PF19 limitado (2026-10-16)

Protocolo2383efd registrado antes dos resultados. Depois do aceite críticoUI e do lançador real99 (10.445BBRSI/31runs, configuração preservada), oito backtests EURUSD H1/M15, EMAATR/Donchian padrão e custos base/adverso foram executados sequencialmente no ce9fae9. Todos tiveram resultado modelado negativo; maiorPF0,875849 noDonchianM15/669operações. Nenhum OOS independente e custos/moeda não certificados; zero candidatos aprovados. Não ampliar a busca para forçar1,9. [Relatório, ICs, gates e reprodução](pesquisa/PF19_RELATORIO_20261016.md).

A base de origem e o protocolo mantiveram os hashes; os oito livros foram reconciliados independentemente por contagem/GP/GL/resultado/PF. O código reusa backtester/estratégias/PF, sem SDK/terminal/contas ou mudança de parâmetros/risco. Baseline947/9 e sete focados (incluindo subprocessos CLI sintéticos) passaram; suíte geral e CI do PR100 em curso. Não confundir execução de pesquisa com integração ou aprovação financeira.

## T20.25 — Cache e isolamento das execuções (em revisão)

A entrega amplia cache F16, manifests T20.4, fila T20.24, sessões T17.28 e
retenção F15. Identidade e checksum precedem reuso; o SDK padrão usa perfil
privado, arquivo por tentativa e posse entre processos com proteção contra
filhos órfãos. A preparação reserva espaço por arquivo e mantém cancelamento
cooperativo. Perfis recusados antes da compilação podem comprovar liberação e
ser recolhidos por quarentena reversível. Referências SQL, inclusive o livro CSV
fora do perfil, permanecem protegidas.

Suíte completa final: 1280 aprovados, 11 pulos esperados e 3 avisos em 1334.28 s, saída 0, com Xvfb privado.
O vínculo ao banco criador passou em 133 testes dirigidos e 15 de retenção; outro
banco não pode recolher o perfil e identidades desconhecidas são preservadas.
114 testes cobrem preparação/posse/budget/tester. PostgreSQL: 9 aprovados; doctor: 16 OK, 2 avisos e 0 falhas
aprovados. Ensaio real Wine repetido: SDK padrão, uma operação reconciliada,
origem/CSV compartilhado preservados e processos liberados. Quarentena e
restauração real preservaram o manifesto. A composição final passou na validação
local completa. Windows físico e revisão independente permanecem pendentes.

[PR #112](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/112), branch codex/t20.25-cache-isolamento, empilhado sobre a T20.24/PR111. Não consome
registry/Composer/registro estatístico ausentes; o histórico nativo continua sem
identidade suficiente para cache entre experimentos. [Contratos e limites](analytics/T20.25_CACHE_ISOLAMENTO.md).

## T19.14 — Trajetórias temporais (em revisão)

Relógios explícitos, trajetórias disponíveis de preço e diários de ciclos/parciais
foram integrados ao ledger por sidecar aditivo versionado. Publicação Python e
re-simulações salvas registram o diário; `mt5x trades trajectory` consulta caixa
por ciclo, preço, limiares e distribuição de duração. Saída intrabarra permanece
intervalar; cortes anteriores não usam a saída futura. Snapshots novos de uma
re-simulação antiga pertencem só à derivada. Cobertura contínua, sessão histórica,
moeda/custos ausentes e precisão nativa não são presumidos.

Baseline: 1280/11/3. Dirigidos finais: 100 em 98,61 s; PostgreSQL16 isolado:
10 em 12,69 s, container removido; doctor: 16 OK/2 avisos/0 falhas. Medição ABBA
BENCH/M15/4000 barras: hashes de trades/curvas/métricas iguais; publicação
0,968→1,737 s e banco 1,137→2,070 MiB, com 2733 observações. Leitura de cópia de
artefato nativo anterior preservou origem e explicitou cobertura só de endpoints;
não foi novo replay nem trajetória contínua. Suíte completa final: 1381 aprovados,
12 pulos e 3 avisos em 1378,80 s, saída 0, com Xvfb privado e fontes intactas
por hash. Omissão inicial na expectativa da assinatura SDK foi corrigida
com oito testes de compatibilidade, preservando a tentativa reprovada.
[PR113](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/113), sobre
T20.25/PR112. Windows físico e revisão independente pendentes.

O dono é `trade_analytics`, consumindo ledger F16, snapshots T20.4/6 e motores
existentes. A exportação permanente H/T16.2b-H e o registro T20.12 não são
reimplementados. [Contrato do recorte](analytics/T19.14_TRAJETORIAS_TEMPORAIS.md).

## Integração canônica T13.2 — 2026-10-18

Origem843a85a reaplicada com atribuição no [PR114](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/114),
com ordenação topológica corrigida e schemas PostgreSQL privados do helper80fd2f3.
Baseline1388/12/3; final Linux/Xvfb1398/13/3 em1930,28s, saída0,
cobertura77,44% e fontes/testes intactos por hash. PostgreSQL17 e doctor16/2/0.
O timeout herdado de captura duplicada foi corrigido na T20.24/PR111; lote
real60,47→39,86s, quatro livros/curvas idênticos por hash. A tentativa
reprovada1396/13/3+1falha foi preservada. Dez arquivos Python integrados
ao workspace principal com HEAD/índice/INI/.env preservados; CLI help saiu0.
Ubuntu1269/142 e jobs PostgreSQL/lint/instaladores aprovados em4b3302c;
Windows CI1381/30/5 e seis checks de execução aprovados em4b3302c; Windows físico pendente. Próxima integração:
T13.3 canônica bde3652; preservar a ordenação e o helper de schemas.
O aceite ponta a ponta dos testes4/7 inclui os produtores/publicador seguintes.

## Integração canônica T13.3 — 2026-10-18 (em validação)

Origem bde3652 reaplicada sobre a T13.2 integrada. A linha de base compartilhada
1398/13/3, saída0 e cobertura77,44%, foi conferida por hash antes das mudanças;
ambiente e requirements permanecem os mesmos da regressão aprovada.
A ordenação topológica e as fixtures PostgreSQL privadas foram preservadas.
O arquivo de matriz genérica da origem dependia do commit documental e8657fe,
ausente na base atual; seu conteúdo canônico foi incorporado como registro
histórico datado, com os resultados atuais discriminados no plano/contrato.
Os números833/10 e os dois mercados de2026-10-15 são evidência histórica,
não aceite desta composição. Novos testes/medições e PR próprios pendentes.


## T13.3 — integração validada (2026-10-18; PR115)

Perfis, RVOL, volatilidade, regimes, estrutura e calendário canônicos integrados
com UTC explícito, histórico de invalidações e versões por configuração/TF.
Código `a1c2a77`: suíte final 1438/16/3 em 1.836,34 s, saída 0, cobertura 77,44%;
seis checks de execução aprovados (CI Ubuntu 1309/145, Windows 1421/33/5,
PostgreSQL 20; lint e dois instaladores sem MT5). Os resultados anteriores
1433/15/3 são pré-identidade e foram arquivados separadamente.

Ambiente principal: 70 focados e 20 PostgreSQL aprovados; 11 arquivos Python
copiados com guards/backup, sem mudar HEAD/índice/configuração/`.env`. Doctor
16/2/0 e CLI compute --help aprovados. EURUSD/AGG: 156.249 registros preservados
exceto algorithm_version; base original somente leitura e SHA igual. Detalhes
e limites em [T13.3_PERFIS_REGIMES](analytics/T13.3_PERFIS_REGIMES.md).

Windows físico/MT5 e decisão da confiança continuam pendentes; sem ordens,
sem alterações em H/S e sem certificar publicação/leitor ou a cadeia completa.
Próxima: T13.3b, runtime 03570b0/docs 4ede7fa; preservar identidade por TF,
algoritmo-base 1.2, UTC explícito e helper de schemas PG isolados.


## T13.3b — fechamento da implementação (2026-10-18; PR116)

Runtime 40fbef5: produtores incrementais/retomáveis com identidade contextual,
limites e algoritmo 1.2.1 de janela finita estável. Uma única suíte final: 1461
aprovados, 18 pulos, 3 avisos em 2154,08 s, saída 0, cobertura 77,44%, fontes
intactos. Focados 99, PostgreSQL 22, limites/retomada 7 e doctor 16/2/0 aprovados.
Benchmark compute: 25398 registros exatamente iguais, inclusive hashes;
12187/1521 ms, sem aquisição/resampling e sem ganho demonstrado de RSS.
A tentativa extra EURUSD/AGG permanece inconclusiva, com artefatos preservados.
Seis checks do runtime 40fbef5 aprovados, incluindo Windows. PR116 pronta para revisão, sem merge
e sem aplicação ao workspace principal. Próxima prioridade do proprietário:
EXTR-01, conforme propostas/PROMPT_RETOMADA_EXTRATOR.md; não repetir a
investigação numérica corrigida. Windows físico permanece pendente.


## EXTR-01 — implementação encerrada, em revisão (2026-10-18)

Prioridade atual do proprietário registrada em propostas/PROMPT_RETOMADA_EXTRATOR.md.
Runtime 677dfe3 corrige atualização que informava sucesso ignorando bloco falho
anterior ao último timestamp, e o acionamento de Cancelar no diálogo ttk.
A prova isolada recupera 540 barras, termina com 1801 timestamps únicos e CSV
exatamente igual ao provedor falso. Atualização 0,910 s; repetição 0,050 s;
jornada medida 2,418 s, sem Analytics/resampling ou feed nativo. Não alegar
ganho global: o antes era incompleto. Focados 34 e provas adicionais de CLI/UI;
doctor 16/2/0. Baseline PR116 reutilizado por equivalência exata antes da edição.
Suíte única final 1468/18/3 em 2237,29 s, saída 0, cobertura 77,64%, fontes
intactos. Seis checks do código 6096ff8 aprovados, incluindo Windows; PR117 pronta
para revisão, sem merge. Ponte desativada com
conexão local recusada limita apenas prova nativa;
Windows físico pendente. Workspace principal preservado, sem merge.
[Relatório e limites](extrator/EXTR-01_CICLO_RETOMADA.md).


## EXTR-01 — prova nativa e integração preparada (2026-10-19)

Ponte efetiva estava desativada e sem serviço; servidor restrito existente
iniciado em 127.0.0.1:18812, habilitado e conta MetaQuotes-Demo/trade_mode=0
comprovada. Credenciais, Safety e sessões de terceiros preservados; sem ordens.
Jornada real EURUSD/M1 14–16/10: 2880→3600, repetição idempotente, reabertura
e CSV exatamente iguais ao provedor em relógio/OHLCV/volumes/spread. UI Tk real
com cancelamento/retomada/exportação, 4652 timestamps únicos. Doctor 21/1/0.
Não houve mudança de runtime nem repetição de suítes ou benchmark.
Revisão local de #116/#117 sem impedimento nos caminhos percorridos; revisões
externas ainda não registradas. Branch de preparação preserva master489591c
e a cadeia aberta #106→#117; não houve merge remoto nem cópia de fontes ao
workspace principal. Próxima tarefa: revisão/integração da base #106/T20.2 e
progressão ordenada dos dependentes. Windows físico permanece pendente.
[Prova e limites](extrator/EXTR-01_VALIDACAO_NATIVA.md).

## T19.0 — publicação isolada da base documental (2026-10-19)

Publica a especificação/catálogo/plano e adendos já preparados pelo proprietário,
com a base preservada em logs/t19-1/base; os documentos comuns de prompt/validação
conservam a prioridade posterior do extrator. Sem atribuir criação desse conteúdo
a esta consolidação e sem transportar runtime ou o diff local inteiro. F20 e o
mapa T19.1/T20.1 têm PRs separados na sequência; referências futuras são dependências
documentais explícitas, não alegações de implementação. Base de código master489591c,
sem alterações em mt5_extracao/tests/dependências/scripts. Aceite automatizado vigente
1058/9/3 registrado na base documental reaproveitado, sem repetição de suíte.
Verificação deste recorte: diff e links locais, com ausências das bases seguintes
registradas até a conferência da cadeia completa. Nenhuma operação ou experimento.


## T20.0 — publicação isolada da base documental (2026-10-19)

Publica requisitos Q01–Q16 e fichas F20 preparados pelo proprietário, usando
a base anterior à extensão T20.1 preservada em logs/t20-1/base. Não transporta
runtime nem o inventário da tarefa seguinte. T19.0 permanece base documental
e T19.1/T20.1 serão publicados por seus patches próprios. Aceite de código
master489591c reaproveitado; conferência documental e ausência de alterações
em fontes/testes/dependências/scripts, sem repetir suíte ou pesquisa numérica.


### Publicação T19.1 — conciliação de contexto (2026-10-19)

O patch preservado não aplicava diretamente ao painel/continuidade da nova base.
Comparação em três vias com logs/t19-1/base preservou os textos existentes;
a sobreposição da continuidade foi resolvida inserindo somente as linhas novas
do patch, sem incorporar os registros preexistentes como autoria T19.1. Sem
reinterpretação de inventário, contratos ou aceites. Divergência e conciliação
registradas em logs/integracao-base-106-20261019/, sem tocar o trabalho original.


### T20.1 — publicação do mapa e conferência da base (2026-10-19)

Patch próprio original conciliado em três vias, sem conflitos, sobre a cadeia
documental T19.0→T20.0→T19.1. Conferidos 57 fichas F19, 30 fichas F20 e 16 Qs;
nenhum novo link sem destino. Três links históricos de evidência ficam resolvidos
pela cadeia de runtime já existente. Sem alterações de fonte/teste/script/dep,
sem nova suíte ou campanha numérica. Registros originais de testes/doctor são
históricos, não novas execuções. Conteúdo do proprietário e patches próprios
separados; a árvore local original não foi alterada por esta publicação.


## T20.2 — revisão e integração da base documental (2026-10-19)

Pré-requisitos publicados/integrados por PR118/T19.0, PR119/T20.0, PR120/T19.1
e PR121/T20.1, com conteúdo do proprietário e patches próprios separados.
Na conciliação com a PR106, somente a continuidade conflitou; ambos os relatos
foram preservados e a linha T20.2 foi unificada no painel F20. Runtime/testes
continuam exatamente os do head1ad3755 e runtimefc9e593 já aprovados:1081/9/3,
saída0, seis checks CI, incluindo Windows. Sem nova suíte ou campanha numérica.
Revisão do diff sem achado bloqueante nesse recorte; limites financeiros/nativos
explicitados no relatório permanecem. B-11 continua proposto, sem aprovação
implícita. Base documental não autoriza operação REAL. Próxima dependente: PR107.

## Continuidade B06 / primeiro percurso — 2026-10-19

HADOUKEN #15→#16→#17 integradas em ordem, depois B04 #18 (`db933dd`) e B05 #19 (`4db5e8f`), com revisão, CI de fonte e QA nativo de contratos. Fechamentos apenas documentais não repetiram campanhas. [B06/SDK1](sdk/decisao_risco_nativa.md) liga H20 e Safety5; API nova validada, integração vinculada em revisão. Nenhum novo runtime Python.

IPC da ponte existente recuperado com initialize portátil compatível; conta DEMO e terminal conectado comprovados antes/depois. Serviço HistoricalExtractor extraiu60 barras M1 EURUSD em banco privado e leitura em outro processo confirmou60 tempos únicos, sem ordens/login/reinício. Evidência local ignorada em logs/m19-b-primeiro-ciclo-20261019/ipc-historical-short-proof.json. Callback de verificação corrigido por readback, sem repetir extração já gravada.

A UI atual mantém seus serviços anteriores; não há novo Default/Manual/preset operacional ou S09 entregue por B06. Simulação Python, QA de contratos no Tester e operação ao vivo DEMO continuam distintos. Próximo: B07/adapter nativo, S09 com SL/TP fixos, depois Default→Manual nomeado→backtest→preset carregado→replay com negócios e resultado reconciliado. Aceite nativo do percurso permanece pendente, Pullback bloqueado até T19.57 e Windows físico pendente específico.

## Continuidade B07 / primeiro percurso — 2026-10-19

B06 integrado H20 (`ef4e401`) → S5 (`405e3b3`) → M134 (`9a2a79c`). [Resultado nativo B07](sdk/resultado_execucao_nativa.md) tem teste focado, CI H39347476261, SDK1(11+0), smoke e seis compilações nativas sem erros/avisos. QA do contrato:1entrada comSL e1operação fechada simuladas, preços/volumes/dono no histórico,0falhas. Sem ordem ao vivo ou nova campanha financeira. Integração vinculada B07 em revisão; nenhuma UI S09/Default/Manual ainda entregue. Próximo: consumidor nativo mínimo B08/T19.23 → S09 fixo → preset e replay efetivo reconciliado; Pullback bloqueado e Windows físico específico.

## T19.18 inicial — suporte fixo antes do S09 (2026-10-19)

[Alvo em pontos/R](sdk/politicas_ganho.md) no motor/composição existentes; comportamento base e seis hashes canônicos antigos preservados. Referências de preço de entrada efetivo, conflito e exportação aprovadas; só expectativa antiga de R sem adaptador foi atualizada e revalidada. CI obrigatório pendente; sem UI nova, preset carregado, liberação Pullback ou aceite operacional. O consumidor H22 inicial de contratos já tem QA nativo com1operação sintética protegida; S09 ainda precisa ser implementado e demonstrado com preset. Próxima política mínima: stop fixo T19.19 e serviço nativo T19.23, sem esperar variantes do catálogo.

## T19.19 inicial — stop antes do lote (2026-10-19)

PR136/T19.18 aguardando seus checks existentes. [Proteção fixa](sdk/protecao_inicial.md) tem142testes focados aprovados: fill com gap/spread/slip, nível antes do risco/lote, ausência, tick, fonte única, CSV, prefixo e trailing que não recua. Fonte base e SDK1 preservados; nenhuma operação ao vivo. H22 (`8cca516`) integrou consumidor nativo inicial dos contratos, sem migrar os seis; setupS09/serviço/Default/Manual/preset/UI ainda pendentes. Stop segue em PR separada sobre136; não duplicar workflows. Windows físico específico, Pullback bloqueado.

### T19.37 inicial — S09 referência mínima

Em revisão: estratégia `s09_reference`, EMA20/50 fechada com aquecimento250,
SL100/TP200 pontos no fill, composição/backtester/CSV existentes; seis anteriores
preservados. Referência independente e quatro sinais iguais aos do ensaio H,
sem alegar paridade financeira, fidelidade à fonte, Default/Manual gerado/replay
visual/UI ou aceite estatístico. Ver `docs/sdk/s09_referencia.md`.
Suporte nativo do mapper ainda indisponível; Pullback bloqueado; Windows físico pendente.
T19.23 inicial: conferência opt-in dos inputs efetivamente carregados no Tester
DEMO, pelo Session existente; recibo versionado e tabela aditiva, sem regravar
resultados. Ainda não é adaptador de composição/S09/preset/UI nem aceite operacional.
Pullback bloqueado; Windows físico pendente. Ver `docs/sdk/inputs_nativos.md`.

### T19.23 — perfil nativo S09 inicial

Em revisão, dependente de #138 (recibo) e #139 (S09), com HADOUKEN #23 integrada.
Mapper explícito da composição para os dez inputs da fonte aprovada, defaults do
perfil, conversão fração→percentual e gates de modos/limites/fingerprint. Sem escrita
de preset, UI, operação ao vivo ou aceite; identidade carregada ainda deve ser
comprovada no pacote/replay. Não conclui adoção dos seis. Ver `docs/sdk/perfil_s09_nativo.md`.
Pullback bloqueado; Windows físico pendente.

### T19.25 inicial — pacote tipado do preset S09

Em revisão, dependente do perfil #140. Manifesto v1 fechado + .set UTF-16 pelo
serializador/leitor existentes; identidade/revisão/linhagem, contexto explícito e
hashes distintos de composição/pacote/execução.60 testes focados aprovados, sem
instalar/iniciar Expert ou escrever no usuário. Nome final T19.50, Default T19.55,
Manual T19.26 e replay/UI ainda pendentes. Ver `docs/sdk/pacote_preset.md`.
Pullback bloqueado; Windows físico pendente.

### T19.50 inicial — nomes portáteis de Default/Manual

Em revisão, dependente de #141. Rótulo Unicode, slug/ID/revisão/hash separados,
nome canônico estável com hash12, limites/nomes Windows e chave de colisão; legado
não é reescrito e alias Maximalista é reconhecido sem novo gerador.51 testes focados
aprovados. Persistência/exportador/UI são consumidores; ver `docs/sdk/nomes_presets.md`.
Sem estratégia/preset automático ou aceite novo; Windows físico pendente.

### T19.55 inicial — Default imutável e seleção persistida

Em revisão, dependente de #142. Snapshot dos defaults efetivos do perfil S09 por
versão/fonte/contexto, IDs estáveis, schema aditivo e criação/reabertura idempotente.
Seleção por contexto não é substituída ao criar Default.87 testes focados aprovados;
PostgreSQL pelo job existente. Ver `docs/sdk/default_versionado.md`.
Manual, exportação e UI ainda são consumidores; sem operação/aceite novo, Pullback
bloqueado e Windows físico pendente.

### T19.26 inicial — Manual nomeado, revisão e pacote preservado

Em revisão, dependente de #143. Clone Default→Manual com ID/nome/linhagem, salvar e
reabrir, revisão imutável/Salvar idempotente, rename de apresentação preservando
identidade de execução. Exportação canônica atômica sem overwrite e importação
verificada/identidade preservada; rascunho compilado sem aceite.71 testes focados
mais4 verificações pertinentes aprovados; PostgreSQL pelo job existente.
Ver `docs/sdk/manual_nomeado.md`; UI/execução/recibo e negócios reais no Tester ainda
pendentes. Nenhuma operação ao vivo; Pullback bloqueado; Windows físico pendente.

### T19.29 inicial — conferência nativa do pacote real

Em revisão, dependente de #144. Arquivo .set/INI efetivamente enviados, recibo de
inputs e modo Visual lidos do MQL5, Session/observador/livro/saldo existentes,
evidência aditiva e primeira divergência.46 testes focados mais3 pertinentes
aprovados com fakes; não constituem aceite nativo. Jornada real com negócios e UI
segue pendente; ver `docs/sdk/conferencia_preset_nativo.md`.
Master já integra políticas iniciais #136 b976ffa e #137 cc58de4 com árvore inteira
igual aos respectivos HEADs/seis checks aprovados. H23 d60e421 integra S09 nativa.
Zero negócios/ensaio de contrato não fecha o percurso. Pullback bloqueado e Windows
físico pendente específico.


## 2026-10-20 — primeiro percurso Manual histórico

[Relatório T20.30 inicial](aceite/PRIMEIRO_PERCURSO_MANUAL.md): HADOUKEN #15–#23,
Safety #5–#6 e Extrator #136–#146 integrados; revisão própria e seis checks do código
final, com árvore inteira idêntica por integração. #138 incorporada por ancestralidade
na #140. Recibo T20_30_INTEGRACOES_20261020.json conserva ordem/fontes/merges/checks.
Marcações anteriores datadas “em revisão” ficam como histórico; o painel e este
snapshot registram o estado atual. Trabalho local, configuração e dados preservados.
O painel agrupa os IDs F19; preservados seus grupos e explicitados apenas os recortes
entregues, sem inventar linhas/conclusão de capacidades planejadas.

UI: Default somente leitura → Manual nomeado/revisões → pacote → backtest nativo →
Replay Visual → consulta/arquivos/CSV. Piloto S09 EURUSD/M5: dois negócios encerrados
em cada passagem, dez inputs carregados e sinais/CSV/livro/saldo sem divergência.
Falhas Xvfb anteriores preservadas; usar a tela do Wine resolveu o replay, sem
repetir backtest aprovado. O gerenciamento existente encerra só o visualizador
próprio após relatório importado. Ponte/terminal principal permaneceram os mesmos,
DEMO conectada, sem initialize/shutdown/login no SDK principal. UI reaberta em dois
temas/DPI96/144, com teclado/rolagem e campos não salvos preservados.

Windows144 inicialmente teve duas falhas de fixture: leitura de manifesto UTF-8 pela
codificação padrão. Três leituras ficaram explícitas, dois casos pertinentes passaram;
nenhum runtime mudou. A correção seguiu por merge para145/146 e os seis checks de cada
HEAD final passaram. Evidências compatíveis e negócios nativos foram reaproveitados;
workflows próprios dos HEADs supersedidos foram cancelados, sem duplicar o novo run.

Python modelado não recebe paridade financeira por essa prova. Histórico nativo com
negócios está comprovado; ao vivo DEMO não exercitado. Dois negócios não certificam
performance/OOS; histórico/realticks não qualificados e cache inelegível. Pullback
bloqueado até T19.57. Windows físico específico. Relações permanece sintética; este
recorte não conclui Analytics, catálogo, Ultra, seleção automática ou toda F19/F20.


T19.57 — recorte Pullback próprio (2026-10-20): perfil nativo em revisão isolada,
com UI Default/Manual, swing/alvoR, lote e metade em1R com confirmação/BE compartilhados.
Legado e Python continuam com bloqueio próprio; não há desbloqueio por herança do
aceite S09. Ver `docs/sdk/pullback_nativo.md`. Aceite final do replay/checks pendente;
cache inelegível, S09 somente Tester, DEMO ao vivo/Windows físico/carteira específicos.


T19.57 Pullback — aceite dirigido Linux/Wine:5 posições vendidas encerradas,
2 parciais1→0,5 com BE confirmado e12 fills; backtest/replay/CSV/livro/saldo conciliados.
23 inputs reais, fontes/includes/executáveis identificados; UI Manual salvo/reaberto,
parcial inviável bloqueada antes de outro Tester, consulta persistente. H#26→M#151;
fonte runtime Hae07e7b/Mc33fa1e. Evidência T19_57_PULLBACK_20261020.json.
20 processos próprios ausentes, ponte/principal preservados; trabalho local preservado.
Compra nativa não exercitada neste recorte; Python/legado têm restrições próprias.
Integração aguarda o Windows CI existente de M151, sem novo workflow.
Próxima ficha prioritária: T19.15 inicial (stop temporal/time gain/loss), reutilizando
T19.14 já integrada pela PR113/bd5cb6d; falta consumo no perfil nativo/UI, sem inferir extremos entre fills.
Cache inelegível; S09 somente Tester; DEMO ao vivo, Windows físico e carteira específicos.


Fechamento T19.57 Pullback — H#26 integrada em8ee507b; M#151 com revisão concluída
e seis checks existentes aprovados no runtimec33fa1e (inclusive Windows CI).
HEAD posterior altera apenas documentação/evidência, mantendo o código exercitado.
Aceite histórico do novo perfil: entrada/swing/R, lote1, parcial1→0,5, BE após
confirmação, encerramentos SL/TP, fills/custos e caixa reconciliados; sem promover
legado/Python, cache, live, carteira ou Windows físico. Ver recibos T19_57_PULLBACK_*.
Próxima lacuna concreta: T19.15, políticas temporais no perfil nativo e na UI.
Reutilizar relógios/identidades/trajetórias de T19.14 (já integrada), sem fabricar extremos futuros.


Integrações Pullback concluídas: HADOUKEN#26 em8ee507b, seguida de Extrator#151
em9c28c35; árvores inteiras iguais aos HEADs revisados e código igual aos runtimes
Hae07e7b/Mc33fa1e, com evidências compatíveis reutilizadas. Nenhum conflito/runtime
novo no merge. Correção do próximo recorte: T19.14 já está integrada (PR113/bd5cb6d);
a próxima ficha é T19.15 inicial, consumo de política temporal pela UI e perfil
nativo. Reutilizar trajetórias/relógios existentes; não recriar T19.14 ou qualificar
cache para prosseguir. Os limites do aceite histórico permanecem os do recibo.


T19.57 — corretivo pré-T19.15 na re-simulação: parcial inviável preserva SL/estado;
BE só acompanha close_part efetivo. Corrige dois casos long/short de lote indivisível,
com controles de redução viável.19 testes pertinentes aprovados; código nativo e
backtester já aprovados preservados, sem repetir seus ensaios. Dados/resultados antigos
não são regravados. Cache permanece inelegível e modalidades mantêm aceites próprios.


T19.15 inicial em revisão isolada: política temporal tipada (segundos desde o fill),
limite absoluto/progresso e prazos gain/loss/tie; moeda e região de empate explícitas.
S09v1.30/30 inputs, UI Default/Manual e recibo de decisão/ack; usa guardas, execução,
parciais e trajetórias existentes, preservando max_bars e perfis anteriores.
Depende de MPR153 (parcial inviável na re-simulação) e HADOUKENPR27; protótipo não
concede aceite até replay próprio com saídas/fills reconciliados.176 testes pertinentes
M compatíveis e testes do controlador/header H; ver docs/sdk/saidas_temporais.md.
Cache inelegível; S09 Tester, live DEMO/Windows físico/carteira permanecem específicos.
T20.25 — scanner durante encerramento (2026-10-20): ensaio dirigido T19.15
interrompido antes do Tester ao observar cmdline vazio de processo em término.
Linux agora ignora somente zombie ou processo comprovadamente ausente; processo
vivo sem identidade mantém recusa, com PID e ação na mensagem. Windows continua
identificado pelo executável. 69 testes de isolamento/controles aprovados; nenhuma
liberação de recursos inferida, nenhuma sessão alheia encerrada. Retomar apenas os
casos temporais pendentes após incorporar este corretivo pelo Git.

Windows CI de M#153 revelou timeout em test_cli_batch: o processo falso simulava
Linux mas varria processos reais do runner. A fixture de comandos falsos agora
usa inventário vazio, substituível pelos testes explícitos de ownership. 74 testes
de lote/isolamento/controles aprovados; código de proteção real preservado.


T19.15 inicial — aceite histórico dirigido concluído: S09v1.30 permite configurar
Manual nomeado com prazo absoluto, lucro mínimo e gain/loss/empate; salvar/reabrir/
exportar e conferir decisões/confirmações na UI. Quatro recortes, oito resultados
persistidos,4 posições/8fills cada;30 inputs reais, CSV/livro/saldo reconciliados.
42 PIDs próprios observados ausentes, scanner final vazio, ponte/principal e WIP
preservados. Backtest de progresso recuperado dos artefatos após SIGTERM do coordenador
(causa não comprovada), sem reexecutar Tester; ausência de recursos conferida após
a interrupção. H#27 integrada7759344; depende de M155→M153, checks M existentes
aguardados antes de integrar M154. Fonte Mffcbb0c/H09068ad; fixture/docs posteriores
não alteram runtime. Recibo T19_15_TEMPORAL_20261020.json. Cache inelegível; S09
Tester; live DEMO, Windows físico, carteira e gradientes específicos. Próxima ficha
elegível a delimitar: T19.16 depende de T19.15 e dos contratos ER/Donchian/ATR;
confirmar dependências antes de implementar, sem qualificar cache por otimização.

Próximo recorte T19.16: ER(20) já existe no registry canônico M
(analytics/indicator_registry.py); ATR Wilder já existe em HDK_StrategyMath sobre
barras fechadas. Falta consumidor ER causal tipado no perfil nativo e contrato de
atualização do prazo, sem reiniciar idade/afastar SL. A variante com canal continua
dependente de T19.7; reutilizar Donchian existente, sem declarar essa ficha concluída.
Inspeção delimitada de dependências, sem código/campanha nova dessa próxima tarefa.

Integrações pré-T19.15 concluídas: M#155 emcec01c1 (varredura/fixture) → M#153
em133eaa6 (parcial/BE na re-simulação), ambas com seis checks no fonte final e
árvore inteira idêntica ao HEAD revisado. H#27 já em7759344. M#154 retarget para
master com runtime inalterado; aguardar o Windows existente no fonte8f9c36c.
Recursos: após todos os iniciadores saírem,42 PIDs ausentes,0zombies/0reutilizados,
oito perfis vazios e ponte/principal preservados. Cópias do cache cifrado de login
do perfil recuperado também limpas pela mesma regra da rotina de encerramento.

Fechamento T19.15 inicial: seis checks obrigatórios aprovados no fonte8f9c36c,
inclusive Windows CI; revisão técnica e aceite funcional concluídos. HEADs
posteriores só documentam evidência/dependências, mantendo código e testes idênticos.
Dependências integradas H27/7759344 → M155/cec01c1 → M153/133eaa6. M154 final
pronta para merge em master, candidato inteiro conferido contra a árvore revisada.
UI e oito resultados históricos preservados; próxima lacuna é consumidor ER causal
de T19.16, seguido da variante de canal condicionada a T19.7. Sem campanha repetida.
Cache inelegível; S09 Tester; live DEMO/Windows físico/carteira/gradientes separados.


T19.16 inicial em árvore isolada: TM05, ER(20) canônico após idade mínima explícita.
Novo perfil S09v1.40 opt-in; controlador temporal compartilhado conserva âncora,
prioridade, confirmação e congelamento. UI/32 inputs e recibo independente de21
closes fechados. Revisão, checks e aceite dirigido pendentes; sem aceite de canal/ATR,
Pullback ER ou live. Detalhes em docs/sdk/tempo_er.md.


T19.16 inicial — TM05/ER com idade: aceite funcional dirigido concluído no S09v1.40,
fingerprint24a4d21ae609d9c7, H#28 runtime994d1ba; M#156 runtime75846d1.
Manual configura idade mínima/limiar, salva/reabre/exporta; consulta mostra ER e
instante conhecido. EURUSD/M5,14→15/10/2026: um backtest e um replay,4 posições/8fills
por execução,4 saídas ER reais e32inputs conferidos; referência Decimal/canônica,
fonte/22includes/binários identificados, caixa/livro/eventos conciliados.
12PIDs próprios encerrados, zero zombies/sobras; ponte/principal preservados.
147 testes dirigidos e doctor20OK/1opcional/0falhas;26Tools no provedor.
Checks existentes do consumidor em curso; integração H#28→M#156 ainda pendente.
Recibo: docs/evidencias/T19_16_ER_20261020.json. Sem repetir campanha anterior.
Aceite restrito ao cenário; não habilita Pullback ER, canal/ATR, carteira, gradientes,
cache ou live. Próximo recorte elegível após integração: TM06, canal Donchian + tempo,
com T19.7 verificada. S09 somente Tester; Windows físico/live DEMO separados.

Integração T19.16 provedor: H#28 emc58d8ba, dois checks no runtime994d1ba;
árvore final idêntica à revisada e fonte preservado. M#156 aguarda somente
suítes Ubuntu/Windows existentes; demais quatro checks aprovados. Alinhamento
dos dois executáveis instrumentados ao fonte revisado/22includes e INI do Tester
conferido no recibo. Trabalho original preservado (HEAD/index/branch/status).


Fechamento T19.16 inicial/TM05: seis checks obrigatórios aprovados no runtime
75846d1d699010ffcf84011f7c5f4586b62dc332 (workflow39458567536). H#28 integrada
c58d8ba; M#156 entrega o consumidor revisado, com delta posterior apenas documental.
Aceite funcional ER e igualdade de contextos/livro backtest↔replay preservados,
sem nova execução/campanha. Windows físico detalhado no checklist específico;
S09 somente Tester, cache inelegível e demais limites mantidos. TM06/Donchian+tempo
é condicionado à publicação Donchian da ficha T19.7, encontrada ainda pendente; T19.16 completa não foi aceita.


T19.7 inicial/I10 — dependência concreta encontrada ao retomar TM06: função Donchian
existia, mas canal não estava publicado no registry/store. Recorte isolado adiciona
14 IDs de Donchian(20), referências atual/anterior explícitas, posição/distâncias,
materialização, consulta e CSV existentes; UI exporta valores do store com relógio
explicitamente declarado. Revisões/gaps conhecidos preservam disponibilidade/as-of.
Testes dirigidos e doctor aprovados; checks/revisão em curso. Não conclui Envelopes,
regressão ou TM06. Nenhum replay aprovado repetido; ER/M#156 preservado.
Contrato e limites: docs/analytics/T19.7_DONCHIAN.md.


Fechamento T19.7 inicial/I10 na PR157: seis checks obrigatórios aprovados no
runtime1b50519e1baf831867d1f729b9900e86a47d9ec9, workflow39469169508. Revisão
sem pendências impeditivas; delta posterior só documental. Donchian(20) publicado
em14IDs, consulta/store/CSV/ação UI conferidos. Envelopes/regressão continuam
pendentes; TM06 tem aceite separado no consumidor M158/H29. Fonte de teste controlada
não é aceite operacional MT5. Recibo T19_7_DONCHIAN_20261020.json; nenhuma campanha
histórica ou replay ER repetidos. Windows físico/cache/live mantêm seus limites.

T19.16/TM06 em revisão dependente de M#157/I10: novo perfil S09v1.50 configura
prazo desde último close favorável além do canal anterior20; exige rompimento
observado e retorno inclusivo. Idade/SL/prazos antigos preservados. UI/33inputs,
contexto/journal com instante conhecido e reconciliação; testes dirigidos e compilação
aprovados, aceite nativo/checks pendentes. H runtimeeaaa059; sem liberação live,
Pullback temporal, ATR adaptativo, gradientes ou carteira. Contrato tempo_donchian.md.

Revisão TM06 identificou falta de teto absoluto obrigatório. Guardas Python/nativo
agora recusam canal ativo sem max_seconds positivo desde primeiro fill; teste
específico aprovado. Ensaio preliminar sem teto preservado, sem aceite final.
Novo ensaio mantém janela/sizing/SL/TP e acrescenta teto3600s: nova verificação
justificada por mudança de proteção/runtime, sem busca estatística.


T19.16/TM06 — aceite dirigido final concluído: S09v1.50/b39a586b622e4713,
H runtimeeaaa059 e M runtimea9c2b66. UI configura idle30s e teto obrigatório3600s,
lote0,25/SL500/TP1000. Um backtest e um replay finais no mesmo dia EURUSD/M5:
2 posições/4fills, uma saída absoluta e uma Donchian por execução;18observações,
33inputs carregados, journal/livro/fills/caixa conciliados. Fonte/23includes/binários
conferidos;12PIDs próprios encerrados, ponte/principal e trabalho original preservados.
Consulta/reabertura/exportação UI conferidas; preliminar sem teto excluído do aceite.
H#29 tem dois checks aprovados; M#157/H#29 integradas, M#158 com seis checks aprovados.
Recibo T19_16_DONCHIAN_20261020.json; sem aceite para ATR adaptativo ou Pullback
temporal, live DEMO, carteira/gradientes/cache. Próxima dependência elegível após
fechamento: TM07, horizonte ATR limitado, mantendo os contratos e aceites próprios.

Fechamento TM06: dependências M#157 (bd11940) e H#29 (596f2c8) integradas antes
do consumidor M#158. Seis checks existentes aprovados no runtimea9c2b66:
Ubuntu1895/199 skips, Windows CI2012/82 skips, PostgreSQL52; sem novo workflow
ou repetição de campanha. Base master incorporada por merge, conflitos somente
documentais resolvidos preservando I10 e TM06; código final idêntico ao fonte
revisado/testado/nativo. Delta posterior somente docs. Windows físico segue pendente.

T19.16/TM07 — ATR limitado em implementação/revisão: novo perfil S09v1.60,
35 inputs; prazo base × ATR de entrada/ATR conhecido, mínimo e teto absoluto
obrigatórios desde primeiro fill. Base/semente250 fechadas congeladas; ATR14
canônico/Decimal, sem reset por parcial/reinício/correção. UI Manual opt-in,
consulta de contexto e recibo/replay pelo serviço existente. 180 testes focados
Python/UI/contratos/recibos, 28 Tools nativos e estática124 aprovados; compilação
isolada aprovada, 2 PIDs próprios observados encerrados. Ensaio funcional dirigido
e checks finais pendentes. H#31/runtime11d0c40; não altera painel ou Pullback.
S09 somente Tester; cache inelegível; live DEMO/Windows físico/carteira/gradientes
e relógios volumétricos mantêm aceites próprios. Contrato docs/sdk/tempo_atr.md.

Ensaio TM07 preliminar recusado: saídas ATR ocorreram, mas snapshots extensos
da fonte não apareceram no journal real; replay dependente interrompido. Fonte
agora em blocos curtos identificados; teste de tamanho reproduziu o defeito e
passou com a correção. 16 testes de recibo incluindo blocos ausentes/repetidos/
truncados aprovados. Perfil corrigido db930f548c8e10d0. Recompilação e mesmo par
funcional serão repetidos pela mudança concreta de runtime; preliminar preservado
sem aceite. CI final será o da revisão corrigida, sem reaproveitar fonte invalidada.

TM07 — aceite dirigido corrigido concluído no perfil1.60/db930f548c8e10d0,
H runtime8b40cac e M runtime295c323. Mesmo EURUSD/M5 14→15/10/2026,
base600/min60/teto3600s; lote0,25/SL500/TP1000 fixados antes do ensaio.
Backtest e replay:4 posições/8fills cada,4 saídas ATR,9 contextos e35inputs
carregados por execução; horizontes588–626s observados, contexto/livro/fills/
caixa7,75USD conciliados. Fonte/24includes/wrapper/binários conferidos.
22PIDs próprios observados encerrados,5 perfis vazios (inclui preliminar/compilações),
ponte/principal preservados. UI: Manual salvo/reaberto, consulta e exportação
conferidas sem novo ensaio. Preliminar recusado preservado sem aceite.
H#31: dois checks corrigidos aprovados; M#159: aguardar workflow39485700757
existente no código295c323. CI92d6aac cancelado após mudança concreta de runtime.
Recibo docs/evidencias/T19_16_ATR_20261020.json. Sem liberação live, Pullback
temporal, carteira/gradientes ou relógios volumétricos; Windows físico pendente.

TM07 — novo achado concreto de recuperação: base ATR perdida na reinicialização
recusava controlador inteiro. Teste real de header reproduziu e passou após correção:
só ATR inelegível, idade/teto/SL conhecidos preservados. Hash/identidade diferentes
continuam recusados. Par8b40cac/295c323 preservado para caminho normal, sem substituir
aceite do novo fonte. Novo alinhamento e checks obrigatórios necessários pela mudança
de runtime; não há busca de outra janela nem campanha histórica ampliada.

TM07 — fonte final após guarda de recuperação: H52c1001/Mf5583f7,
S09v1.60/4afcfe4bcef17c18. Compilação e novo par alinhados na mesma janela
e parâmetros:4 posições/8fills/9contextos cada,35inputs; replay/caixa7,75USD
conciliados, horizontes588–626s.24includes/wrapper/binários exatos.36PIDs
próprios observados encerrados em8 perfis no conjunto (inclui versões preservadas),
ponte/principal intactos; nenhuma sessão nativa ativa. Manual/consulta/exportação
UI reabertos no banco existente, sem novo ensaio. Falha de perda de base ATR
validada por header real/plataforma falsa; par nativo final exercitou caminho
normal, sem forçar corrupção/crash. Checks H finais aprovados; M aguarda somente
workflow39488078713 existente. Revisões substituídas canceladas pelos defeitos
concretos registrados. Recibo T19_16_ATR_20261020.json; limites permanecem.


Fechamento TM07: H#31 integrada em fe0e6e9, runtime52c1001. Os seis checks
obrigatórios existentes aprovaram Mf5583f7, workflow39488078713. Código final
revisado e ensaio funcional com negócios/35inputs conciliados; alterações
posteriores só documentais. M#159 segue essa dependência e a conferência da árvore
final. Recibo T19_16_ATR_20261020.json;36 processos próprios encerrados e8 perfis
vazios comprovados, ponte/principal preservados. Manual configura base/mínimo/teto
ATR, salva/reabre, consulta contextos conhecidos e exporta pelo serviço existente.
Próxima ficha: T19.20, intervalos/dias/ciclos por sessão com fuso e contadores
de primeiro fill explícitos; calendário observado T13.1 reutilizado sem presumir
DST ou sessões futuras. S09 somente Tester, cache inelegível; live DEMO, Windows
físico, Pullback temporal, carteira/gradientes e relógios volumétricos continuam
com aceites próprios. Não houve nova campanha histórica nem dispatch duplicado.

Painel modular — planejamento: layout de fichário/compacto/expansão aprovado pelo proprietário.
[Inventário estático e atividades PM-00–PM-12](propostas/PAINEL_MODULAR_COBERTURA.md)
fixam M b139f04/H 596f2c8/S dbff0d2 como bases documentais; Safety não foi promovido
para executável/snapshot. Previstos: painel padrão a incorporar a cada EA e identidade
por gráfico com degradê e marca d'água; serviços faltantes não bloqueiam o núcleo.
G-02 precede a implementação do painel e as extensões seguem suas dependências.
Registro integrado em fila documental após TM07, sem MT5/compilação/runtime nesta
entrega. S09 somente Tester; cache de execução/evidência inelegível; histórico/replay
não habilitam live DEMO. Carteira concorrente e Windows físico mantêm aceites próprios.


T19.20 inicial aceita na PR161, após H34/08cf58b: horários permitidos/excluídos,
dias ancorados e cota por primeiros fills no perfil S09v1.70/43inputs. Seis checks
existentes aprovaram o runtime372b762, workflow39499390366; revisão técnica concluída
e delta posterior somente documental. Um backtest e um replay dirigidos:1posição,
2fills,10eventos e8recusas (5dia+1exclusão+1cota+1janela) por execução; livro/CSV/
caixa2USD e eventos iguais.14PIDs próprios encerrados,3perfis vazios, ponte/principal
preservados. Manual salvo/reaberto e consulta/exportação após fim do iniciador,
sem novo ensaio. Recibo docs/evidencias/T19_20_SESSAO_20261020.json.
Próximo recorte: cooldown após encerramento confirmado, com relógio/âncora
explícitos e sem consumir/reiniciar por parcial ou ordem recusada. Fechamento
por horário permanece separado. S09 somente Tester; cache inelegível; Pullback
neste gate, live DEMO, Windows físico, carteira/gradientes e DST/calendário futuro
conservam seus próprios aceites. Nenhuma campanha numérica nova.


T19.20/02 em revisão isolada: espera em segundos após fechamento total confirmado,
S09v1.80/44inputs. Parcial não inicia/reinicia; histórico/saldo/identidade incertos
bloqueiam apenas entradas. UI Manual/save/reopen/export e recibo independente
reutilizam os serviços. Gestão temporal/SL/parciais preservados. Perfil anterior
sem migração; fechamento agendado separado. Contrato docs/sdk/cooldown_entradas.md.
S09 somente Tester; cache inelegível; live DEMO/Windows físico/Pullback neste gate/
carteira/gradientes mantêm aceites próprios. Par nativo e checks finais pendentes.


T19.20/02 — estado parcial em H35→M162, sem merge: S09v1.80/44inputs,
Manual salvo/reaberto/exportado, consulta pós-iniciador conferida. Backtest dirigido
EURUSD/M5 14→15/10/2026:2posições/4fills,2recusas de cooldown,18USD conciliados;
parciais/ausência/fronteira1ms/reinício cobertos por header real/plataforma falsa.
Replay não gerou relatório: terminal privado autorizou DEMO mas não sincronizou
(connect status3,100). Preparação própria sem EA/ordens limitada90s também sem
confirmação; replay dependente interrompido. LiveUpdate observado não comprova causa.
18PIDs próprios ausentes,0zombies,4perfis vazios, ponte/principal preservados;
27includes/wrapper/binário/INI do backtest alinhados.124+63 testes sobrepostos,
30Tools/estática127/doctor21OK aprovados. H2checks aprovados; M workflow39507890504
existente no fonte4517299 com4checks aprovados, Ubuntu/Windows em andamento.
Próximo passo concreto: recuperar sincronização do perfil isolado, conferir
compatibilidade de fontes/binário/parâmetros/plataforma e executar somente replay
faltante se o backtest continuar compatível; depois integrar H35→M162 com6checks.
Recibo docs/evidencias/T19_20_COOLDOWN_20261020.json. Sem nova rodada antes de corrigir
pré-condição; S09 somente Tester/cache inelegível/live DEMO e Windows físico próprios.


T19.20/02 — aceite funcional concluído: S09v1.80/44inputs, H e232bef/M4517299,
Manual salvo/reaberto/exportado e consulta pós-iniciador. Backtest preservado
8fefd89e e replay06b9dae1:2posições/4fills/4decisões,2recusas por espera e nova
entrada posterior, caixa18USD/eventos/livro/CSV iguais.27PIDs próprios ausentes,
0zombies/0reutilizados,5perfis vazios; ponte/principal e raízes preservados.
Duas falhas preliminares de sincronização continuam arquivadas. Pré-condição
corrigida somente no perfil próprio: catálogo nativo de símbolos da mesma DEMO/build,
sem qualificar cache de execução/evidência; sincronização confirmada antes do Tester.
Não afirma recuperação automática nem promoção para live.27includes/wrappers/binários
alinhados nas duas execuções aceitas. Próximo passo: concluir workflow39507890504
existente no runtime4517299 e integrar H35→M162 após revisão/6checks.
S09 permanece somente Tester; Pullback neste gate/live DEMO/Windows físico/carteira/
gradientes mantêm aceites próprios. Recibo T19_20_COOLDOWN_20261020.json.


Fechamento T19.20/02: seis checks existentes aprovados no runtime4517299,
workflow39507890504, com Windows CI. H35 integrada e5f4842; consumidor M162 revisado,
fonte/testes e par nativo conciliados. Alterações posteriores somente documentais.
Manual configura espera após fechamento total, salva/reabre/exporta e consulta
origem/fechamento/liberação; parcial não inicia/reinicia. Aceite limitado ao cenário
com catálogo nativo preparado; recuperação automática universal não entregue.
Pedido posterior de ao vivo DEMO tem estado próprio: EMA+ATR/Conservador passou no
backtest do contexto, mas replay de pré-aceite não iniciou por sincronização privada
indisponível em90s. Nenhuma ordem ao vivo foi enviada. S09/Pullback Nativo/adaptador
piloto seguem somente Tester; cache inelegível, REAL proibida e Windows físico próprio.
Próximo bloqueador operacional: sincronização nativa estável do perfil próprio;
sem nova tentativa dependente até mudar a pré-condição. Sem nova campanha numérica.


Complemento T8.7 solicitado pelo proprietário: ao vivo DEMO tem aceite próprio.
Expert compatível proposto EMA+ATR/EURUSD/H1, preset conservador existente0,25%,
sem novos limites. UI/backtest dirigido13→15/10/2026:1posição/-250,92USD/12inputs,
CSV/caixa conciliados; gates atuais do PLAY passaram. Replay próprio nem iniciou:
perfil privado não confirmou sincronização em90s apesar de catálogo nativo preparado.
Mitigação do cooldown não é universal; causa final da intermitência não comprovada.
14PIDs próprios ausentes/2perfis vazios; ponte/principal conectados DEMO e preservados.
Nenhuma operação ao vivo enviada, nenhum aceite operacional, nenhum runtime alterado.
Próximo passo: pré-condição nativa estável → replay faltante compatível → revalidar
conta/contexto/proteções/carteira → observação DEMO pela UI. Não repetir tentativa
sem mudança da pré-condição. S09/Pullback Nativo/adaptador piloto seguem Tester-only;
cache inelegível, REAL proibida, Windows físico/carteira/gradientes próprios.
Contrato docs/sdk/demo_operacional.md; recibo T8_7_DEMO_PARCIAL_20261020.json.

Integração anterior conferida: H35/e5f4842 → M162/a0cd3c4, seis checks no runtime4517299, árvore final revisada idêntica. Este complemento T8.7 altera apenas documentação e não promove execução/aceite ao vivo.


### Corretivo F2/T2.5 — sessão correspondente reutilizada (2026-10-21)

Em revisão sobre Recursos/layout compostos e070897. O conector confere terminal,
caminhos e conta antes de inicializar; sessão correspondente é emprestada e sua
liberação local preserva a API. Perda/alteração posterior recusa também no preflight
da coleta, sem reconectar silenciosamente essa sessão. Conexão nova/login explícito
mantêm o caminho anterior. Sem novo gerenciador/lock/limite; usa tester_lease existente.
T17.15 tratava só espera de teste e não é o escopo deste runtime.

Prova real candidata: DEMO6276, primário e ponte preservados, zero posições/ordens;
initialize/login/shutdown bloqueados no helper. Extração EURUSD/H1 fechada25barras,
atualização49, reabertura/cópia/restauração auditada/cópia anterior e CSV todas as
colunas iguais em banco privado pequeno. UI online abriu as49 e liberou consumidor
sem desligar a API. Não abriu/restaurou o banco do proprietário. None/NaN nas colunas
opcionais do CSV mantêm semântica de ausência, sem cálculo sintético/substituição.
SDK documenta limites e evidência própria registra testes. CI final e instalação
continuam pendentes; sincronização privada/Tester/aceite financeiro e Windows físico
não qualificados, sem repetir sondagem nativa.


### T20.12 — consulta do journal na UI (2026-10-21)

Complemento em revisão sobre174/c0e0f9c: Exibir → Registro de estudos abre nomes,
revisões, pedidos/recusas/descarte/reutilização, eventos e exposição compartilhada.
Usa store.status existente e um índice SELECT sem criar schema em base antiga.
Só metadados: não calcula, consulta resultado financeiro nem registra falso acesso
à reserva. Critérios/orçamento/snapshots e limites de qualificação vêm do canônico.
Sem novo cache/journal/motor. Serviço/economia/robots/SDK/estratégias preservados.
6finais UI,30seleção/1PGlocalpendente (sobrepostos);2protocolos/4pedidos/17eventos
reais em cópia só das cinco tabelas de metadados, menu real/capturas inspecionadas,
mesmo bf4d/status inalterado. Não repetiu avaliação ou restore. CI, instalação,
Windows físico e demais consumidores/estatísticaT20.13 permanecem pendentes.

### T19.34 — corpo desmapeado por altura (2026-10-21)

1b0 e e070 falharam Windows no mesmo900×620/144DPI, widthfits mas mapped0.
Nenhum foi integrado/instalado. Novo recorte em172 usa ScrollableContent já existente
em volta da página (filtros/resumo/corpo), mantendo as rolagens anteriores e modelos.
Reprodução Linux escala suportada1,5 deixou o corpo semmapa antes;3dirigidos44,70 e
4adjacentes28,36 passaram após. Primeiro Windows isolado3casos antecede a suíte CI
completa/60%inalterada, sem waiver/dispatch. Nova captura histórica privada48fede,
3trades/111,0875,24bounds/2temas+escala1,5, sem cálculo; contexto acessível, navegação
/globalstatus ainda têm cortes com fonte extrema. T19.34 ampla/Windows físico pendentes.
Compor por Git em173/174/175 antes de promover qualquer ponta; nova CI/instalação próprias.

## Frente de dados BD (plano rev. 2) — retomada de 23/10/2026

PRs empilhados sobre `codex/g19-distribuicao`: #190 BD01 → #191 BD02 → #192 BD03 → #193 BD04.
Todos **parciais**; nenhum instalado no canal de desenvolvimento. Documentos em `docs/dados/`;
matriz `docs/dados/matriz_aceites_dados_20261022_rev2.json` (E01–E06 em execução, demais
planejados); recibos com SHAs completos em `docs/dados/recibos/recibos_20261023.json`.

Mudanças de produção até aqui: identidade lógica da base (`datasets.identity/fork/declare_source`,
`feature_store.with_base_identity`, base congelada em notas novas, `base_namespace` no DTO) e
histórico do bruto OHLCV (`_ohlcv_revisions`, `received_at`, `load_ohlcv_as_of`).

Retomada: suítes com `xvfb-run -a .venv/bin/python -m pytest -q` (sem janelas no display do
proprietário; 3 testes de geometria falham no display real e passam sob Xvfb — pendência visual
no dono da UI). Doctor: `--root` da worktree BD01, que tem `config/config.ini` próprio
(`--no-mt5`, ponte desativada). Próximo: BD05 (tempo/sessões/W1/MN1).

### Atualização de 23/10/2026 (2ª revisão independente, R01–R07)

Contraexemplos reproduzidos antes da correção (log `r01_r03_antes_adbe614.txt` na fotografia):
R01 (101 em vez de 250), R03 (300 em vez de 250), R02 (as-of devolvendo barra não recebida e
anterior à observação). Correções: contrato de recebimento `_ohlcv_versions`/`_ohlcv_receipts`,
as-of causal/inspect, extrator com fonte e recebimento (R04), mensagem do fork, zona do corretor
e offset desconhecido (R06), assinatura global como limitação temporária sem mudar o grafo (R05),
recibos `docs/dados/recibos/recibos_20261023b.json` com E06 pela entrada efetiva (R07).
Primeira cadeia funcional: provedor falso → extrator → M1 → M5 → store → `sdk.query_series` →
`mt5x analytics features series`. Pilha: #190…#194 e novos PRs BD06, BD07, BD08a, BD18a, BD19c1.
Nada instalado; marco M2 parcial (consumidores além da CLI pendentes).

## LAB-02/03 · 2026-10-26 (branch de continuidade codex/novo-host-20261024, PR 212)

- LAB-02: `laboratory.service` passa a emitir `diagnosis` (código/mensagem/ação) por canal vazio
  (`WARMUP_EXCEEDS_WINDOW`, `INELIGIBLE_DATA`, `NO_VALUES`, `NO_OVERLAP`); janela acima de 20 000 barras
  sugere início concreto; janela vazia explica. A UI mostra o motivo no status, detalhes, Cobertura,
  Relações e Linha do tempo em vez de área cinza; segmentos de uma barra ganham marcador.
- LAB-03: `MatrixTable` calcula a mesma largura por coluna para cabeçalho e corpo (inclui texto maior
  que o nominal) com scrollregions idênticas; teste por coluna após rolagem V/H, resize e texto largo.
- Testes: 131 passaram nas suítes de Laboratório/componentes; `test_expanded_chart_uses_absolute_negative_monitor_coordinates`
  já falhava antes da mudança neste host (gerenciador de janelas); `test_large_monitor...` é intermitente sob carga.
- Limitação: base local vazia; reprodução com dados SINTÉTICOS controlados (sessões com lacunas, M5/H1/D1).
  Falta confirmar pelo atalho com a base real do proprietário; DPI/monitor físico não qualificados.
- Próximo: `rtk proxy .venv/bin/python -m pytest -q tests/test_laboratory_series.py tests/test_ui_components.py`; depois LAB-01.

## LAB-01 · 2026-10-26 (branch de continuidade, PR 212)

- `laboratory/inventory.py` (somente leitura): une registry BD06, definições/versões do pilar de Dados
  (`feature_store.list_definitions`) e fatos agregados por ativo/TF; estados `STORED`, `COMPUTABLE`,
  `STORED_ONLY`, `UNAVAILABLE`, cada um com motivo e ação; busca, filtros (família/estado/ativo/TF) e
  cursor amarrado aos filtros. CLI: `mt5x laboratory inventory [--query --state --symbol --tf --page-size --cursor]`.
- Relações ganhou a visão **Inventário** (dados reais, paginação, resposta obsoleta descartada, erro de fundo
  com motivo); as demais visões continuam identificadas como fixture sintética até o DTO T18.1/serviço T18.6.
- Séries: seletor de indicadores busca no inventário e preserva a seleção ao filtrar; nova definição do
  registry aparece sem editar o seletor (teste).
- Pendente do LAB-01: matrizes Indicador × indicador/alvo ainda são fixture — exigem produtor de relações
  (T18.6) do pilar de Dados; não foi criado cálculo paralelo.
- Testes: 112 passaram; falhas intermitentes de geometria/WM: `large_monitor`, `negative_monitor` (preexistente),
  `closed_selection_details` (passa isolado).

## LAB-04 (parcial) · 2026-10-26

- `charts.bars` (Dia do mês, Dia da semana, Faixa horária, Notícias): escala divergente simétrica por
  magnitude (forte = intenso, fraco = claro com piso visível), zero com traço neutro, ausente com ×,
  valores sempre horizontais e só quando cabem; `n=` saiu das barras para a legenda (n total, escala ±,
  pouca amostra, zero, sem dados) e o hover. Temas claro/escuro conferidos por renderização.
- Pendente do LAB-04: revisar heatmaps/demais visões com o mesmo critério e aceite visual do proprietário.

## LAB-08 (parcial) · 2026-10-26

- Gráfico do ativo em Séries: candles + todas as séries selecionadas; unidade de preço sobreposta, demais
  unidades em até 3 subjanelas com eixo de tempo compartilhado (zoom/cursor vinculados no Ampliar);
  várias instâncias parametrizadas; clique na legenda oculta/mostra.
- `laboratory/annotations.py`: linha horizontal/vertical, região, nota; editar texto, excluir, desfazer;
  exportar/reabrir JSON amarrado ao `analysis_id` (recusa estudo de outra análise).
- Pendente: arrastar/editar geometria pelo mouse, remover série sem recongelar, mais de 3 unidades
  simultâneas (hoje avisa e pede ocultar), integração com o Estudo MT5 (`chart_study`) e aceite do proprietário.

## LAB-08 (bancada gráfica) · 2026-10-27 (branch de continuidade, PR 212, commit 3a2efb6)

Supera a pendência "arrastar geometria / remover série / >3 unidades / Estudo" do registro LAB-08 de 26/10.
- Estudo v2 (`laboratory/annotations.py`): identidade da análise (ativo, grade, seleção e por série
  unidade/parâmetros/versão/fonte), séries ocultas e **removidas da vista** (sem recongelar; desfazer devolve),
  vista (zoom/cursor), selecionar/mover com desfazer; recusa outra análise (cita ativo/TF) e série ausente.
  v1 continua legível até aceite da v2.
- Gráfico: até 8 subjanelas por unidade (110 px cada, rolagem da página); acima disso lista as unidades
  excedentes. Cursor vinculado em todos os painéis; zoom preservado em redesenho, ampliar/restaurar e reabrir.
  Ferramenta Selecionar/mover; botões do estudo refluem em largura estreita (antes Exportar/Abrir saíam da tela em 1280 px).
- CLI: `mt5x laboratory study --analysis A.json --study S.json` (sem Tk).
- `chart_study` (Estudo MT5) não foi fundido: responsabilidade distinta (captura/notas do terminal).
- Testes: 45 nas suítes de Laboratório/anotações/séries; falha preexistente `negative_monitor` (WM) mantida.
- QA no host: Tk real em Xvfb privado com análise **real** (EURUSD H1+H4, 1029 fechamentos, 18 séries/9 unidades)
  de uma cópia descartável da base recuperada; `broker_utc_offset = -3` vindo do config do host anterior, só na
  config de QA (a config ativa segue sem fuso e a análise real é recusada nela — decisão do proprietário).
  Exercitado: cliques/arraste por eventos Tk (xdotool não move o ponteiro neste Xvfb), remover/desfazer,
  zoom/cursor, ampliar/restaurar, exportar/reabrir/recusar, tema claro/escuro, 1280×2500 e 900×700.
  Não exercitado: mouse físico, DPI/monitor físico, handheld.
- Bordas da região: arrastar a borda redimensiona só aquele lado (`Study.edge/resize`), com desfazer.
- Pendente LAB-08: aceite do proprietário; anotações só no painel de preço (mover entre painéis não existe).
