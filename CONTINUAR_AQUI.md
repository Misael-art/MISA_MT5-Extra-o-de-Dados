# Retomar MT5X em um host limpo

## Prioridade corretiva definida pelo proprietário — 13/11/2026

**Assumir primeiro os itens P0 de cálculo e confiabilidade desta demanda.**
A ordem é E14-CALC-01 → IND-CCI-01 → IND-AGGR-01 → E14-METRIC-01 →
E14-SCOPE-01 → E14-EXEC-01,
seguida de SA-01-CONTRATO e SA-01-PLOT em P1. Reprodução, regressão,
correção integrada e validação dos resultados precedem a otimização de
presets e a ampliação do Expert/painel/dashboard nos caminhos afetados.
As frentes independentes de proteção de dados e diagnóstico do host continuam.

Os oito itens foram acrescentados à fila abaixo, em estado **livre**, com
responsáveis por componente. O próximo agente registra sua posse real, confere
working tree, código e PR atual e atualiza branch/PR, SHA, comando e resultado.
Os detalhes executáveis e critérios de aceite estão no
[backlog existente](docs/propostas/LAB_SUPER_ATR_PENDENCIAS_20261024.md#correções-prioritárias),
com prioridade refletida no [roadmap](docs/ROADMAP.md).
Os achados IND-CCI-01/IND-AGGR-01 e os seis vídeos complementares têm
[análise e aceites em Analytics](docs/propostas/ANALYTICS_OPERACOES.md#vídeos-complementares).
O relatório de 28 vídeos foi confrontado com a fonte, com erros externos e
capacidades existentes identificados. O
[mapa de atuação e cruzamentos](docs/propostas/ANALYTICS_OPERACOES.md#encaminhamento-para-atuação-e-cruzamentos)
liga os temas às fichas F19/F20 e explicita investigação, integração e aceite.
Publicação documental desta demanda na branch `codex/auditoria-videos-roadmap`,
dependente da composição PR308. O executor de cada correção deve registrar
seu próprio SHA e aceite na linha correspondente.
Uma dependência nativa bloqueada deve receber causa e próxima ação exatas,
permitindo avançar no próximo recorte independente. Escrita e cargas locais
continuam serializadas no checkout canônico, com orçamento antes de carga pesada.

## Aceites Windows aguardando host qualificado — determinação de 13/11/2026

O proprietário adiou os aceites específicos de Windows até disponibilizar um
host qualificado. Isso inclui instalação e integração nativas, observação
manual/física e reproduções instrumentadas DIAG-CI304/DIAG-CI-NODE. Esses aceites
estão **bloqueados por disponibilidade futura de host qualificado**. Linux,
implementação portável, instrumentação, regressões e publicação continuam.
O CI Windows habitual permanece evidência de runner, com SHA e escopo próprios.

Antes da futura execução, registrar Windows/versões suportadas, toolchain,
MT5/build quando aplicável, conta permitida sem segredos, recursos/sensores,
dados de QA, recuperação e periféricos do percurso. Não transferir aceite de
CI ou mocks para esse host. Não há autorização de negociação REAL nesta tarefa.

## Fila de diagnóstico e correções — 13/11/2026

Codex assumiu a escrita desta passagem no checkout canônico `repos/app`, em
`codex/launcher-classes`, HEAD `550b6d5`. O índice principal e os WIPs anteriores
foram preservados. As correções publicadas usam índice separado sobre cada PR.
Os owners e as dependências ficam na fila abaixo. A retomada deve conferir
working tree, PR e owner atual antes de assumir qualquer item.

O código composto `0e4f4c6869eaaf7ad2b0345db0e23c108802a7d4`, PR308, passou
nos seis jobs de produto. Windows teve 4.904 aprovados e 205 não executados,
com 78% de cobertura do núcleo. Linux teve 4.636 aprovados e 473 não executados,
com 81%. PostgreSQL, instaladores Linux/Windows e lint passaram. Os casos de
cancelamento, clique/transporte Node, lease e cenário macro executaram e passaram
no Windows. As PRs307 `c296538`, 309 `d0e97d6`, 310 `97da399` e 311 `cb02d78`
também passaram nos seis jobs. A documentação desta fotografia é conferida
pelo diff e pelas fontes citadas. O CI da ponta da composição está na PR308.

A prévia efetiva do atalho e menu é `development-2b3b274da6de`, com 365 arquivos
idênticos ao código composto por hash. O retorno imediato `650656f` tem 354 arquivos
conferidos, e o retorno histórico `57d13ee` tem 341. O caminho estável e os dois
atalhos foram conferidos. HADOUKEN `d4d389e` está limpo em branch e Safety
`27b8599` está limpo detached, conforme os pins existentes.

A fila tem28 itens:14 concluídos, seis bloqueados, seis livres e dois em validação. Reavaliar os aceites de cada alvo pela evidência própria.
Os aceites possuem escopo explícito e dependências próprias.

| ID | Objetivo / owner | Estado | Branch/PR e último SHA publicado | Evidência, dependência e próximo passo |
|---|---|---|---|---|
| E14-CALC-01 | P0 · Comissões, fills, parciais e ciclos / Codex nesta tarefa, HADOUKEN-E14 e Analytics | em validação | HADOUKEN PR85 `569718c`; App PR313 `72f95e9`, `codex/e14-calc-integracao` | Consumidor comum reproduziu a falha e corrige26→28 com rateio por volume, taxas, INOUT e entradas antigas. 169 testes únicos passaram em lotes:162 antes de interrupção própria por reserva, sete restantes e sete afetados repetidos. MetaEditor/Wine0erros/0avisos,39fontes idênticas. Manifesto App em reconciliação explícita; prévia continua2b até gates. Aceite de histórico nativo/visual é próprio; Windows aguarda host qualificado |
| IND-CCI-01 | P0 · Fórmula e aquecimento do CCI / Codex nesta tarefa, Dados e indicadores App | em validação | App `codex/ind-cci-01`; PR/SHA pendentes | Falha reproduzida:66,6667/aquecimento27. Fórmula compartilhada corrigida para123,8095/aquecimento14. Enhanced propaga período; coleta concilia sobreposição; registry/BD06/store publicam2.0+hash aditivo, com resets e preservação as-of. 62 testes únicos passaram em lotes; nove falhas BD06 reproduzidas e corrigidas,18 casos finais aprovados. Doctor16/2/0 offline. Launcher oficial em QA própria aprovou materialize/context/export;281,5MiB e mínimo5,28GiB disponíveis. CI/prévia pendentes. Paridade iCCI e Windows têm aceites próprios |
| IND-AGGR-01 | P0 · Atualização incremental do AggrVolBalance / indicadores HADOUKEN, executor a assumir | livre | Branch/PR a conferir e definir por objetivo. SHA da correção pendente | Arrays e buffers AS_SERIES true, loop começa em prev_calculated−1. Com100/100 atualiza índice99; com101/100 atualiza99/100, deixando a barra corrente0 fora. Inspeção e ensaio de índices, sem execução MQL nativa. Próximo é comparar incremental/reconstrução, corrigir a seleção e qualificar consumidores/artefato com composição revisada |
| E14-METRIC-01 | P0 · Indisponibilidade, frescor e cobertura / painel HADOUKEN e Analytics, executor a assumir | livre | Branch/PR a conferir e definir por objetivo. SHA da correção pendente | TradeStats.Refresh zera acumuladores antes de HistorySelect. Próximo é reproduzir falha/leitura incompleta, implementar estado explícito e testar recuperação, zero válido e métricas derivadas. Reutiliza o contexto contábil de E14-CALC-01 |
| E14-SCOPE-01 | P0 · Escopos de métricas, risco e zeragem / Safety, painel e Analytics, executor a assumir | livre | Branch/PR a conferir e definir por objetivo. SHA da correção pendente | Guard usa equity da conta, estatísticas filtram Magic e pânico alcança símbolos desse Magic. Esses fatos exigem política e rótulos explícitos. Próximo é qualificar escopo, reproduzir ambiguidades e corrigir consumidores afetados, com proteção de posições alheias |
| E14-EXEC-01 | P0 · Intenção durável, parcial, incerteza, reinício e reversão / Expert-HADOUKEN e Safety, executor a assumir | livre | Branch/PR a conferir e definir por objetivo. SHA da correção pendente | Qualificar consumidores vigentes StrategyRobot/NativeBaseAdapter, vínculo durável e recuperação. Reproduzir fronteiras e aplicar a política vigente de saída/reversão. Corrigir lacunas demonstradas. Compatibilidade do futuro Triplo pertence a SA-01-CONTRATO. Ensaio nativo exige sessão autorizada própria |
| SA-01-CONTRATO | P1 · Contrato Triplo e compatibilidade SL/TP / Expert, Dados e integração, executor a assumir | livre | Branch/PR a conferir e definir por objetivo. SHA da implementação pendente | Relato descreve três bandas no mesmo TF20/fatores1/2/3, fonte atual usa multi-TF e saída diferente. Fórmula/seed/reentrada continuam abertas. Adaptador vinculado exige SL/TP positivos. Próximo é fechar a variante e dependências P0 aplicáveis |
| SA-01-PLOT | P1 · Plot opcional com parâmetros/seed efetivos / Expert e painel, executor a assumir | livre | Branch/PR a conferir e definir por objetivo. SHA da implementação pendente | Catálogo atual chama indicador pelos defaults e exibe dez curvas. Depende de SA-01-CONTRATO. Próximo é vincular as três curvas ao mesmo cálculo e qualificar visual ligado/desligado, preservando estudos alheios |
| DIAG-CONFIG | Mesma leitura literal na GUI, CLI e conector / App | concluído | PR299 `d0ea937` | SHA anterior publicado. Conflito apenas documental com master impedia CI. Recibo reposicionado e `merge-tree` limpo, novo SHA enviado. Código mantém os 32 testes e launcher de QA registrados. SHA remoto confirmado. Cinco jobs aprovados. Windows tentativa 1 teve 4.663 aprovados/195 não executados e três timeouts em Node/Node/g++. Caso Node já tem proposta de outro agente na PR301 `28e4c17`, com seis jobs aprovados. A tentativa diagnóstica 2 do mesmo SHA passou nos seis jobs, incluindo Windows. As falhas da primeira tentativa continuam preservadas. O timeout do compilador não foi reproduzido |
| DIAG-EXCURSOES | MAE/MFE disponíveis nas lentes e custo ask em vendas / Dados/Laboratório | concluído | PR305 `38304e0`, `codex/lab-excursoes` | Publicado e PR criada. 38 testes de excursões/enriquecimento/agregação e 21 de cache/livro/importação/CLI aprovados em lotes separados. Lote final 13 aprovados/1 PG sem servidor. Inclui legado, preservação do livro, point inválido e spread calibrado. QA por launcher CLI em base sintética confirmou ask e estado. GUI real XWayland pelo launcher aprovada em janela própria, com MAE 4/MFE 2 e ask declarados. Seis jobs de produto aprovados, incluindo Windows e PG, no SHA `38304e0`. Integração em master e na próxima prévia pertencem à composição |
| DIAG-PROTECAO-WIP | Proteção independente do resgate anterior e do checkpoint atual / integrador | concluído | página privada existente, recibo local `private-checkpoint/remote-read-proof.json` | Acesso reconferido via Pages, sem compartilhamentos. ZIP 489.138 B/1 parte, SHA `c4f843f78d979ff6aab6f79299e1151a6ed88a29897a158eac48513b806450fd`. Parte relida integralmente. Texto e ZIP decodificado conferidos. Recuperador oficial validou 19 arquivos e o WIP anterior teve seus 24 hashes conferidos. Conteúdo guarda checkpoint parcial de amostra. Base principal tem proteção e aceite próprios |
| DIAG-IDENTIDADE | Marcação de ativos pela identidade registrada / App/Dados | concluído | fluxo já integrado no App | Hipótese funcional descartada por inspeção do caminho ativo. `UIManager.refresh_inventory` lê `data_inventory` em `run_bg`, e `_inventory_loaded` agrupa por símbolo registrado. O método chamado pela rotina antiga que divide nomes é vazio. Nenhuma correção funcional necessária neste recorte |
| DIAG-CI304 | Reproduzir travamento de cancelamento/retomada / Execução App | bloqueado | PR304 `dc17f49`, seis jobs de produto aprovados | Ocorrência antiga no Windows 130029697303 e COMMIT de presets preservados. Regressões Linux passaram. Ponta atual passou no Windows completo. Lock retido continua hipótese. Dependência exata é uma reprodução Windows com captura das conexões/leases no ponto do timeout. Recorrência ausente nos SHAs recentes. Retomar instrumentação quando a falha reaparecer |
| DIAG-TICKS | Auditoria tolerante a metadados inválidos, sem certificação indevida / Dados | concluído | PR304 `dc17f49` | Auditoria v2 publicada. 15 falhas reproduzidas. Lote integrado 80 aprovados e lote final 34 aprovados, sobrepostos. Launcher `--cli trades fidelity` em 3 ticks próprios com flag infinita retorna JSON finito, classe D e intrabarra recusada. Seis jobs de produto aprovados no SHA `dc17f49`, incluindo Windows/PG. Aceites operacionais ficam separados |
| DIAG-AMOSTRA | Repetição do mesmo backtest não aumenta amostra nas lentes / Laboratório | concluído | PR306 `060afab`, `codex/lab-amostra-execucoes`, dependente da PR304 | Deduplicação qualificada por identidade do produtor e livro, com consulta integral explícita. Três backtests do produtor em 600 barras próprias guardaram 9 operações. Launcher CLI e GUI XWayland mostraram 3/9. Lotes focados aprovados, inclusive leitura legada sem releitura redundante. Seis jobs de produto aprovados no SHA final. Aceites nativos/financeiros continuam próprios |
| DIAG-RESTORE | Identidade, conteúdo e relógio da cópia restaurada / Recursos | concluído | PR307 `c296538`, `codex/restore-backup-integridade` | URI com # corrigida, mesma fotografia e inventário conferidos. Duas falhas adicionais reproduzidas após trocar relógio/fuso, corrigidas pela invalidação do cache do validador existente. Seis novos casos cobrem resultado normal/incerto e compatibilidade. Lote integrado de 39 aprovados em 14,20 s. A ponta anterior 9c passou nos seis jobs. Seis jobs de produto aprovados em c296538. GUI XWayland da composição 2b confirma troca de relógio, incerteza e recuperação em dados próprios |
| DIAG-RESTORE-RECUP | Intenção durável, resultado consultável e recuperação explícita / Recursos | concluído | PR307 `c296538`, núcleo validado em `9c2c7eb` | Produtor/lease existentes, recibos privados e identidade física. Interrupção abrupta própria com saída 17, consulta incerta, hardlink e diretório alterado bloqueados. Rollback explícito 42. GUI oficial visível em XWayland provou 7→99, 18 pulsos e intervalo máximo 0,581 s, zero erros inesperados. 31 casos finais aprovados. Pico GUI 270,2 MiB, mínimo disponível 7,96 GiB, máximo 68 °C. Seis jobs passaram em 9c. O corretivo de relógio possui validação própria na linha anterior. Energia/reinício físico exigem janela própria |
| DIAG-LAUNCHER-LOCAL | Recursos e fontes públicas abrem sem Wine / integração App | concluído | composição PR308 `f6b6274` | Recursos/B3/crypto integrados à política única após três falhas reproduzidas. Lotes de 24 e 10 casos aprovados, com ponte controlada e extração preservada na classe nativa. Publicado e integrado na composição. Prévia ainda depende do QA do artefato exato |
| DIAG-LEASE-REBOOT | Reconciliação de lease após reinício com prova da mesma máquina / Execução App | concluído | PR309 `d0e97d6`, runtime `2a5638d` | 66 testes de lease/fila passaram em 39,14 s. Primeira tentativa Windows 2a foi cancelada prematuramente a98% e continha falha do teste que lia outro descritor sob bloqueio. Leitura agora usa o descritor do lease e conserva os estados ativo/liberado, com recusa externa controlada. 7df corrigiu a leitura do lease no Windows. Sua única falha foi o prazo Node. D0e97d6 incorpora orçamento macro e corrige a origem registrada, Seis jobs de produto aprovados em d0e97d6, incluindo Windows 4705/195 e teste do descritor ativo/liberado. Fonte, controles e gates preservados. Reinício físico tem aceite próprio |
| DIAG-CI-MACRO | Propagar orçamento já declarado no cenário macro / QA/Dados | concluído | PR311 `cb02d78`. Integrado PR309 `d0e97d6` e PR308 `0e4f4c6` | Falha reproduzida com sampler1536 MiB, sem alocação equivalente. A análise recebe o orçamento já medido da coleta. 5 casos passaram em 8,40 s, incluindo recusa de 2048 MiB acima do teto 1792 MiB. Gates de memória/tempo e assertivas gráficas mantidos. Runtime2b/365 arquivos idêntico ao fonte 18fe por hash. Seis jobs de produto aprovados em cb02d78, incluindo Windows 4664/195. A composição0e também executou o cenário gráfico aprovado |
| DIAG-CI-NODE | Distinguir prazo de execução e captura Node no Windows / QA | bloqueado | PR301 `28e4c17`, proposta de outro agente preservada | Windows 7df/job131581528852 teve somente TimeoutExpired10 s no teste de clique de volume, com 4703 aprovados e195 não executados. O traceback aguarda a thread de captura stdout. Isso não prova a causa. PR301 altera10→45 e passou nos seis jobs, sem integração nesta composição. Nenhum limite foi aumentado nesta passagem. As três pontas atuais executaram e aprovaram o caso no Windows com 10 s. Causa permanece sem reprodução instrumentada no runner Windows. Próximo é capturar estado/saída do processo antes de uma recorrência. Alteração de mecanismo exige reprodução pertinente |
| DIAG-RETENCAO | Retenção conserva artefatos sem prova de posse / Recursos | concluído | PR310 `97da399`, `codex/backup-retencao-posse`, depende da PR307 | Recibos perdidos/corrompidos, troca de fonte, sobreposição de pastas e leitura incompleta reproduzidos em árvores próprias. Intenção durável, identidade/hash no ato, controle preservado, referências de restauração e SQLite protegidos. 45 casos focados passaram em 10,15 s. 29 de proteção/sessões/leitura parcial em 13,31 s, com Tk no compositor real. Sete fontes estáveis conferidas por hash. Launcher CLI confirma retenção, interrupção, consulta, perda de recibo e histórico. Lote antigo interrompido após 31 aprovados em 101,74 s, evidência retida. 51 casos finais em 14,75 s, com oito hashes estáveis. Seis jobs de produto aprovados em 97da399. GUI XWayland do artefato2b confirmou cópia e consulta no worker, preservando arquivo sem proveniência |
| DIAG-COMPOSICAO | Composição publicada, prévia e proteção reconciliadas / integrador | concluído | PR308 `0e4f4c6`. Prévia efetiva `2b3b274` | Seis jobs de produto aprovados no código 0e4f4c6. Windows 4904/205, Linux 4636/473, cobertura do núcleo78%/81%. 336 arquivos runtime idênticos ao checkout, run.bat equivalente por CRLF e 365 hashes do artefato. Ativação a674860c por link atômico aprovada, dois atalhos conferidos, pico 23,3 MiB. Launcher estável consultou cópias próprias com saída0, valor99, pico 134,6 MiB e mínimo 8,21 GiB. Estado de símbolos padrão ausente preservado. Método de produção idêntico validou fallback em ambos os artefatos. Após a reconciliação, novo QA pelo launcher estável abriu janela visível, confirmou ausência do arquivo de símbolos, restauração incerta7→recuperação99 e consulta no worker. Pico 273,2 MiB, mínimo 8,30 GiB e máximo 65 °C. Quatro estados de preferência por hash/existência preservados nessa janela isolada. Observação180 s no runtime 561 byte a byte igual. Retornos650/57 preservados. Aguardar Windows e registrar os estados terminais |
| ACEITE-NATIVO | MT5/Tester, DEMO, netting e Windows físico / operacional | bloqueado | pins H `d4d389e`, Safety `27b8599` | Sem autorização vigente de negociação nem Windows físico. Conta netting e ensaio de falhas contra broker próprios. CI/mocks não substituem esses alvos |
| ACEITE-DRIVE | OAuth real, proteção independente, pacotes por ativo e reunião / Recursos/Dados | bloqueado | PR302 integrada. PR303 `37cf1a3` com seis jobs aprovados | Inventário só de schema conferiu 30 tabelas, 7 séries registradas e 23 internas conhecidas. Snapshot canônico transporta projeção de oito campos OHLCV e famílias tipadas, sem reconstituir toda a base bruta. BD03 fixa uma base lógica por banco/schema. Solicitada decisão objetiva entre pasta QA com origens separadas e base QA única, necessária para semântica de conflito e gravação. Nenhum dado privado usado como fixture. Próximo após resposta é pacote/inventário por owner e reunião no destino escolhido. OAuth/envio do proprietário permanecem posteriores ao aceite sintético conjunto |
| ACEITE-UI | Entrada física, acessibilidade, login e observação prolongada / App | bloqueado | artefato PR308 `561152b` | Janela real XWayland e observação controlada de 180 s aprovadas, com solicitações instrumentadas e preferências de QA. Entrada física/monitor-dock-DPI exigem operador e periféricos. Acessibilidade exige percurso com tecnologia assistiva. Login exige janela exclusiva da sessão. Observação controlada de 30 min está no item independente abaixo. Esses aceites continuam específicos, sem qualificação por mocks/CI |
| ACEITE-OBSERVACAO | Observação contínua controlada de 30 min / App QA | concluído | runtime PR308 `2b3b274`, código `0e4f4c6` | Launcher estável, SQLite própria, XWayland visível e controladores instrumentados. Duração 1.800,015 s, 41.495 heartbeats, intervalo máximo 0,820 s, 59 atualizações e 30 trocas de tema, zero erros de callback. Pico 273,1 MiB, mínimo 7,85 GiB disponível e máximo 66 °C. Quatro estados de preferências por hash/existência preservados. Recibos em preview-stable-observation/proof.json e resources.json. Entrada física e tecnologia assistiva pertencem ao ACEITE-UI |
| ACEITE-M1 | Qualificação estatística/econômica prospectiva / Dados e pesquisa | bloqueado | PR297 `a87dd76`, evidência pública T134_M1_EURUSD_H1_QA_20261106.json | Recibo já publicado tem zero padrões aprovados, 0/2 eventos e contrato de custo bloqueado por calibração posterior ao período. P04 exige plano temporal e suporte pareado congelados. Depende de modelo de custos elegível antes do período e histórico compatível com sessões/derivações para nova validação prospectiva. Usar dados autorizados, com acesso/extração no aceite nativo |

### Arquitetura e configuração conferidas

O launcher classifica consultas locais antes de iniciar a ponte. `app.py` e a
CLI constroem os serviços comuns. `UIManager.run_bg` encaminha I/O ao worker.
`DatabaseManager` mantém SQLite/PostgreSQL, relógio e controles. Dados publica
identidades, revisões e snapshots pelo serviço canônico. Laboratório e Analytics
os consomem. Recursos reutiliza snapshot SQLite, lease e recibos privados para
cópia, consulta e recuperação. App compõe HADOUKEN e Safety fixados, com
execução e risco nos donos existentes. O master remoto conferido é `09a618c`.
A composição de revisão e a prévia possuem as identidades registradas acima.

Os defaults ficam em `mt5_extracao/bootstrap/config_builder.py` e no exemplo de configuração.
O launcher recebe a configuração explícita, e a configuração privada permanece
fora do Git. O `.venv` único pertence ao App e é compartilhado pela prévia.
Base ativa, pesquisa e QA mantêm seus escopos. A identidade BD03 pertence ao
conteúdo de cada banco/schema, com namespace e linhagem transportados pelos
snapshots. O plano de pacotes deve conservar os escritores/leitores canônicos.

### Evidências, orçamento e limites

Os recibos locais ficam em `state/qa-tmp/diagnostico-20261112/` na raiz MT5X.
O lote final de retenção/restauração teve 51 casos aprovados em 14,75 s, com oito
hashes de fontes estáveis. Lease/fila teve 66 aprovados em 39,14 s. Macro teve
cinco aprovados em 8,40 s com RSS simulado, inclusive recusa acima do orçamento
já declarado. Os lotes anteriores de 39 e29 casos possuem sobreposição.
Doctor offline do artefato teve 16 OK, dois avisos e zero falhas.

`preview-stable-final-qa/proof.json` registra GUI visível XWayland pelo launcher
estável, controladores instrumentados e SQLite própria. A restauração incerta
retornou7 e a recuperação explícita99. Cópia/consulta rodaram no worker. Após
reconciliar a ausência de `config/default_symbols.json`, a GUI final confirmou
esse estado. Pico273,2 MiB, mínimo 8,30 GiB disponível e máximo 65 °C. Os quatro
estados de preferências por hash/existência foram preservados nessa janela.
A observação anterior de 180 s teve 4.168 heartbeats e intervalo máximo 0,571 s.
A observação final de 30 min teve 41.495 heartbeats, intervalo máximo 0,820 s,
59 atualizações, 30 trocas de tema e zero erros de callback. O pico foi
273,1 MiB, com mínimo 7,85 GiB disponível e máximo 66 °C. O recibo
`preview-stable-observation/proof.json` delimita esse percurso controlado.

Os ensaios usam um processo e um thread de BLAS/OMP. A reserva operacional é
4,5 GiB, acima do mínimo 4 GiB do host. O orçamento da observação é1800 s,
768 MiB de RSS e32 MiB de saída, com telemetria20 s e guardas2 s. A medição de
04h28 UTC teve 8,25 GiB disponíveis, PSI de memória0 e64 °C. O trip passivo
ACPI é100 °C e o crítico105 °C. Contadores Btrfs acumulados têm6.741 erros de
corrupção, com zero erros de leitura/escrita/flush/geração, iguais à referência.
A janela observada de 30 min do kernel teve zero avisos. A causa dos reinícios
continua indeterminada. Os dados globais de swap variaram entre janelas.

A base principal configurada teve tamanho603.648.000B e mtime preservados
nas conferências por metadados, com arquivos auxiliares ausentes. O QA usou
bases sintéticas próprias. O checkpoint privado foi relido e recuperado pelo
helper existente, conforme a fila. A proteção externa atual da base principal
possui dependência de destino e política do proprietário.

Cinco artefatos intermediários próprios foram removidos após conferir os1.825
hashes, a ancestralidade no Git e as referências de processos visíveis. O recibo
`unused-artifacts-cleanup.json` conserva os manifestos e a intenção durável
08360e5a. Foram31.827.463B em 0,41 s. Prévia ativa, retornos650/57 e artefato
744 de outro trabalho foram preservados.

### Roteiro e critérios de aceite

| Prioridade | Próxima ação e dono | Dependência e aceite |
|---|---|---|

| P0 | Definir destino dos pacotes/reunião / proprietário e Recursos/Dados | A pergunta existente distingue coleção QA com origens separadas e base QA única. BD03 fixa uma base lógica por banco/schema. O contrato do destino orienta conflitos, namespace e importação. Inventário já conferiu30 tabelas, sete séries e23 controles conhecidos. O aceite exige todas as famílias/colunas legadas/revisões, relógio, hashes e dependências, plano só leitura e nova QA idempotente pelos consumidores existentes |
| P1 | Login Drive e proteção externa da base principal / proprietário e Recursos | Destino, conta, política e código de recuperação independentes autorizados, depois dos pacotes/reunião sintéticos. Exercitar renovação/revogação, retomada, checksum e recuperação própria pelo fluxo oficial |
| P1 | Interface e alvo nativo / operador e owners App/HADOUKEN/Safety | Janela exclusiva, periféricos e tecnologia assistiva para entrada física/acessibilidade/login/dock/DPI. MT5/Tester/DEMO/netting/Windows físico requerem acesso e autorização próprios. Evidência por alvo e percurso com proteção de processos/dados |
| P1 | Capturar recorrências Windows / Execução App e QA | Locks de cancelamento e prazo Node passaram nas pontas atuais. A causa de cada ocorrência anterior depende da captura de conexões/leases ou estado/saída do processo no momento da falha. A PR301 de outro agente conserva a proposta45 s. A composição usa10 s |
| P2 | Qualificação M1 / Dados e pesquisa | Modelo de custo conhecido antes do período, suporte pareado e plano P04 congelados, histórico elegível com sessões/derivações. O recibo publicado tem zero padrões aprovados |

### Ocorrências da investigação

Uma execução Windows foi cancelada prematuramente quando ainda progredia a98%.
O log foi preservado e a repetição do mesmo SHA revelou as falhas de lease e
orçamento macro, corrigidas e validadas nas pontas atuais. A atribuição anterior
da falha macro a7df foi corrigida após ler o log completo. A ocorrência foi em 2a.
O baseline de preferências começou nas sondas finais e a preservação foi comprovada
nessas janelas isoladas. Uma inspeção inicial exibiu uma URL de autorização na
saída da ferramenta. As inspeções posteriores passaram a filtrar URLs e argumentos
sensíveis. A recuperação privada e os recibos conservaram suas restrições de acesso.

O backlog completo permanece em `docs/PLANO_DE_TRABALHO.md` e nos contratos
Dados/LAB/F19/F20. Seções anteriores conservam suas fotografias datadas.

## Exposição, fidelidade de ticks e recuperação condicional — 10/11/2026

Pedido do proprietário de 09–10/11 (vídeo martingale × lote fixo). Fonte canônica:
[ANALYTICS_OPERACOES §11](docs/propostas/ANALYTICS_OPERACOES.md): auditoria (11.9), classes A–E
(11.10), especificação H0–H6 (11.11), coesão com o Ultra e piso de PF 2 (11.12), resultados e
lacunas (11.13). Entregue no App, só estudo: T16.14 `mt5x trades fidelity`, T16.15
`mt5x trades recovery` (cinco modos novos também na guia Execução) e T19.52a
`mt5x trades router`. 21 testes novos; regressão afetada 277 aprovados (Linux/Xvfb); launcher
oficial em base de QA isolada com barras reais EURUSD M15
([recibo](docs/evidencias/T1615_RECUPERACAO_ROTEADOR_EURUSD_M15_QA_20261110.json)).

Resultado: a base local é classe D (nenhum tick extraído); preço médio sem sinal reprovado em
todos os runs; adição condicionada com 3–9 casos por run (amostra insuficiente, tendência
desfavorável); política condicional sem candidato elegível; nada sustenta PF 2. HADOUKEN/Safety
intocados; nenhum gate, preset ou executor mudou. Não validado: Tester/MT5, DEMO, Windows, CI.
Próximo: extrair ticks e calibrar custos, ampliar história/ativos, corrigir runs triplicados no
livro e estudar modos de saída antes de adição (lista em 11.13). O host reiniciou uma vez durante
esta passagem, com suíte e preparação de QA simultâneas: rodar uma operação por vez.

## PR302 — corretivo do CI em 08/11/2026

Owner: **Recursos/Dados**, tarefa **T20.30-HOST1-DRIVE**, limitada a SQLite/uma conta. Na ponta remota `9d55fb50b00861f35e0617c9ccd2273ade471a52`, o CI 42400089570 falhou em `Testes (ubuntu-latest)` e `Testes (windows-latest)`; PostgreSQL, lint e os dois instaladores passaram. Logs confirmam: Ubuntu não tinha `.venv` ao chamar `run.sh`; Windows tentou executar `run.sh` e usou nome SQLite com `?` (inválido no Windows).

Corretivo para revisão: `.github/workflows/ci.yml` cria `.venv` com acesso às dependências previamente instaladas e executa `bootstrap configure --non-interactive --skip-credentials --no-doctor` antes de `pytest`. `tests/test_drive_security.py` seleciona `run.bat --cli` via `cmd.exe` no Windows e `run.sh --offline --cli` no Linux; o teste SQLite usa `a#b%c space.db` e comprova a leitura do registro `id=7` no snapshot. O `run.bat` existente não mudou; a transmissão efetiva de argumentos com espaço e `%` depende do novo Windows CI.

O recorte de código foi comparado com o SHA da PR: dois arquivos afetados; as duas regressões diretas passaram de novo no Linux em 08/11 (`2 passed`, `18 deselected`). Da passagem técnica anterior: regressão conjunta de sete módulos `104 passed` em 39,42 s, ShellCheck aprovado, doctor offline `16 OK/2 avisos/0 falhas`, YAML e bootstrap conferidos. Esses resultados locais não qualificam Windows físico, OAuth/Drive real, PostgreSQL do Drive, MT5 nem negociação. A nova execução do CI somente poderá ser declarada aprovada após resultados associados ao SHA publicado.

Checkout principal preservado em `codex/launcher-classes`/`550b6d5` com alterações de outras frentes. O incremento da PR302 deve usar índice Git isolado sobre a ponta remota, sem stage global, troca de branch ou nova árvore de trabalho; publicar somente o corretivo e as atualizações específicas desta passagem. Não atualizar a prévia do proprietário com a branch do Drive antes dos aceites correspondentes. Depois de estabilizar a PR302, próximo incremento autorizado em PR independente: **múltiplas contas Drive**, com identidades estáveis, cofres isolados, seleção explícita, confirmação da identidade antes do envio e testes sintéticos.

## Drive — estabilização sintética de 07/11/2026

A [PR302](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/302) recebe somente correções do objetivo original de SQLite/uma conta: recuperação fora de argv/JSON/logs, entrada oculta, snapshot de Recursos reutilizado, estados separados de envio/checksum/restauração, identidade remota estável e retomada após interrupção. Falha conserva a última cópia boa e o preparo cifrado. Downloads acima de 70 MB são manuais e têm plano/conferência local; restauração confere SHA-256, contagens e integridade.

Baseline: 63 testes. Regressão integrada: 99 aprovados em 42,8 s; pico observado da árvore de processos 315,4 MiB; CPU 29,6 s usuário/3,2 s sistema. I/O informado pelo SO: 2.008 blocos de entrada/1.034.576 de saída (512 B). Ajuste final de erro do cofre: 28 testes focados aprovados, com sobreposição. ShellCheck passou; doctor offline: 16 OK/2 avisos/0 falhas. Launcher oficial Linux/Xvfb com série sintética: verificar → enviar → ensaiar restauração, zero erros de callback. Não é aceite OAuth/Drive real, CI do novo SHA, Windows físico, MT5/DEMO ou PostgreSQL.

Prévia efetiva conferida em 07/11: `650656fac67d` (PR300), 354 hashes sem divergência; retorno `57d13ee` preservado. O atalho não foi atualizado neste lote. HADOUKEN `d4d389e` preservado em branch; Safety `27b8599` detached.

Checkout do App continua em `codex/launcher-classes`/`550b6d5`, com WIP alheio preservado. A publicação Drive usa índice separado sobre o SHA remoto da PR302, sem transportar as outras frentes ou trocar a branch do checkout. A cópia local do WIP tem manifesto e hashes verificados; proteção independente pendente, pois a página privada registrada não está acessível nesta sessão. Btrfs permaneceu em 6.741 corrupções acumuladas; isso não diagnostica a causa dos reinícios. Nenhum scrub/reparo/limpeza foi executado.

Próximos incrementos, em PRs distintas e dependentes: contas múltiplas com identidade estável e cofre isolado; pacotes por ativo com inventário/dependências; reunião em QA com conflitos explícitos e consumidores existentes. O proprietário fará consentimento e testes reais só depois da aprovação sintética conjunta. Nenhum dado privado enviado.

## DIAG-CI-MACRO — orçamento coerente do cenário em 12/11/2026

Windows2a/131574907486 reprovou o cenário macro por RSS do processo acima de
1024MiB. Windows7df/131581528852 passou nesse cenário e reprovou somente na
verificação Node de10s. A atribuição anterior a7df foi corrigida após ler o log.
A fixture já mede RSS e declara um orçamento na coleta; a análise passava a
receber o valor padrão. O incremento propaga o orçamento existente à análise.
O serviço de produção mantém seu contrato de memória e o cenário conserva o
mesmo teto entre etapas.

Regressão com sampler controlado em1536MiB reproduziu MemoryError na primeira
análise, em3,12s. Após a correção, o cenário concluiu e recusou2048MiB, acima
do teto já declarado de1792MiB. Cinco testes de fases, relatório e limites
passaram em8,40s no Linux. Dados próprios de95 barras, um processo e um thread.
Os valores do sampler representam simulação, com memória física limitada ao
processo de teste normal. Regressão gráfica Windows e CI da ponta seguem em
validação. Integração requerida nas PR309/308; artefato2b conserva o mesmo
runtime. Logs e falhas anteriores permanecem como evidência.

## Integração no master e prévia — 04/11/2026

Master `784a252` (merge commits, sem force push) recebeu, em ordem, as PRs
207–208, 210–280, 283–291, 281 (C) e 282 (D). A árvore do master é idêntica
a `e2388d0`, cujo CI na PR282 passou em Ubuntu (32 min), Windows (2h13),
PostgreSQL, instaladores Linux/Windows e lint; PR279 (`af40d91`) também
passou em todas as plataformas. PR209 (pilha bd08b) não faz parte da cadeia
e segue draft na base original. Correções de Windows integradas: corrida de
`IntegrityError` no registro de definições SQLite e orçamento de memória do
fixture LAB por RSS do processo.

Prévia `~/.local/share/mt5x/releases/development-784a25210b04` (350 arquivos
com hash) ligada em `app-development`; recibo em `state/development-preview.json`.
O atalho oficial abriu a janela principal sem traceback e iniciou a ponte MT5
via Wine. PostgreSQL de teste local: contêiner `mt5x-test-pg` (postgres:16,
127.0.0.1:5432, volume próprio, unless-stopped, 512 MB); use
`MT5X_TEST_PG_URL` igual ao do CI para a suíte PG (118 aprovados localmente).

Pendentes, fora deste host ou dependentes do usuário: Windows físico, MT5/Tester
nativo, ensaio DEMO, aceite M1 com dados elegíveis reais, gates operacionais
E14, percurso manual de Séries LAB-01…10 e compilação/bateria do painel
`hdk_hadouken_painel`.

## Revalidação de GUI, Séries e composição — 04/11/2026

App E14 `0993ae7bdf2e3354afcccf45dd2a22069a8b460c` continua na PR279
draft; HADOUKEN `d4d389ee0800d209bdba48ab36e392e6907949a4` na PR84
draft e Safety `27b85992af4b10f4cd7687f370142ac88f515375` detached.
O manifesto mantém estes pins e `setup_development.py --plan` terminou sem erro;
o App declarado ali é o bootstrap `65e621a`, distinto do checkout E14 e da prévia.
A prévia segue em `57d13ee`. No run 41747563787 da PR279, Ubuntu,
PostgreSQL, instaladores Linux/Windows e lint passaram; Windows pytest ainda
executava na consulta de 04/11 01:40 UTC. Não há aceite Windows CI final.

GUI C: branch local `codex/gui-dependencias-inicializacao` em `b0733d9`,
base E14 `0993ae7`; código `bf38e4a`. O launcher oficial foi repetido na
fonte rebatida `13f6b97` em Linux/Xvfb com configuração e SQLite isolados:
janela aberta e `quick_check` OK com dependências presentes; ausência simulada
de `pandas` mostrou diálogo/instalador oficial sem criar banco ou instalar.
Cinco testes focados passaram e doctor offline deu 16 OK/2 avisos/0 falhas.
Recibo ampliado na própria branch C; arquivos temporários removidos.

LAB-10 D: branch local `codex/lab-series-monitor-geometry` em `802e5de`,
base de código C `13f6b97`. O módulo de UI de Séries passou nesta fonte em
Linux/Xvfb: 29 aprovados, 1 ignorado, 16 avisos de layout, 127,52 s.
A asserção confere a geometria negativa solicitada e ampliar/restaurar;
posição física em monitor esquerdo e percurso pelo atalho seguem pendentes.
C e D não têm branch remota/PR por causa da guarda central compartilhada;
os SHAs locais estão no registro central. Não foi feito merge, atualização da
prévia, ensaio DEMO ou Windows físico. Próximo: conferir o resultado Windows
da PR279, resolver a guarda para publicar incrementos separados e continuar
os gates funcionais LAB/P01–P07 e operacionais E14 por fonte exata.

## E14 — largura tardia da comparação no Windows CI — 03/11/2026

Na PR279, App `19627c3c74dc2efafc60b2e417f49fa93d3d71f6`, run
`41731727371`: Linux, PostgreSQL, instaladores e lint passaram; Windows
terminou com 4566 aprovados, 193 ignorados, 1 falha e cobertura 77,74%.
A falha única foi `test_themes_scales_metrics_and_narrow_window_keep_actions_reachable`:
TkAgg deixou canvas de 580 px numa página de 558 px após mudança de escala.
Um teste novo reproduziu o alargamento tardio no host antes da correção; o
`ScrollableContent` agora reaplica a largura visível quando o conteúdo muda.
Depois, 24 testes de componentes e comparação passaram no Linux/Xvfb; doctor
offline de desenvolvimento 16 OK/2 avisos/0 falhas. Corretivo
`3312ea3d2fc2c318836cf420e84f343c75f98761` publicado na mesma PR279;
SHA local, remoto e da PR confirmados. CI run `41747461167` estava na fila
no marco de publicação; Windows físico e percurso
pelo launcher desta tela seguem aceites separados. Nenhum contrato financeiro,
pin ou dado privado mudou. Próximo: conferir o CI e continuar
os gates E14; não declarar o check Windows aprovado antes do novo run.

## Composição e limites atuais — 03/11/2026

Raiz única `/home/misael/Projects/MT5X`: App `repos/app`, HADOUKEN
`repos/hadouken`, Safety `repos/safety`. App E14 `0993ae7`/PR279 draft,
HADOUKEN `d4d389e`/PR84 draft (fonte S09 2.13 `51358a1`) e Safety
`27b8599` detached/PR11 formam a composição de desenvolvimento; o preparo
oficial aceita os pins. A prévia instalada ainda é App `57d13ee` e não
contém o perfil 2.13 ou P01–P07. Os SHAs completos, PRs e próximos passos
estão no registro central `../.workspace/projects.json` e no manifesto do
App; textos datados abaixo descrevem fontes anteriores.

E14 S09 2.13: MetaEditor/Wine 0 erros e 0 avisos, 59 arquivos iguais; Tester
Linux/Wine e replay visual do mesmo pacote sintético conciliaram oito entradas,
sete ações, quatro alterações de TP pós-fill e a caixa. Recibo público em
`docs/evidencias/E14_POSTFILL_213_TESTER_REPLAY_20261103.json`; recibos privados
protegidos na página pessoal existente, ZIP SHA256
`2a8a6dff51b953a2bf8fe6f8c93904cad086d85ff0000163d004d61c96547080`.
No SHA anterior `19627c3`, Windows CI falhou só no gráfico estreito, com 4566
testes aprovados; Linux e demais jobs passaram. O corretivo está em `3312ea3`,
e os checks da ponta documental `0993ae7` tinham PostgreSQL, instaladores e
lint aprovados; Ubuntu/Windows ainda executavam na consulta de 04/11 00:50 UTC.
Não houve DEMO ao vivo,
netting, Windows físico nem liberação do perfil operacional.

P00–P07 foram recompostos em branches locais separadas sobre E14, com LAB-03
local dependente de P07. Os testes de padrões/Séries na composição LAB-03
terminaram com 79 aprovados, três ignorados e uma falha já reproduzida na
asserção de monitor negativo herdada da base E14 neste host XWayland de um
monitor. A branch isolada LAB-10 `8822d25`, baseada em GUI C `13f6b97`,
corrige essa asserção; a fonte anterior passou 29 testes de Séries/1 ignorado,
e a fonte rebatida passou seis testes focados; falta integrar os aceites entre
PRs. O fluxo P01–P07 permanece exploratório: CI, fonte real elegível,
PostgreSQL da campanha, Windows físico e aprovação M1 seguem abertos.
Na cadeia rebatida sobre `0993ae7`, 51 testes focados de P01–P07/GUI de
padrões passaram em Linux/Xvfb. `MT5X_TEST_PG_URL` não está configurada neste
host; os dois testes PostgreSQL próprios desta cadeia não foram executados.
O inspetor oficial de checkouts confirmou, sem instalar ou atualizar a prévia,
App local `7544f6b` e pins HADOUKEN/Safety; doctor offline 16 OK/2 avisos/0 falhas.
O guarda central de push ainda impede publicar as branches de revisão;
não o contornar. Próximo: publicar esta cadeia após a decisão da guarda, seguir
recuperação/capacidade/pendentes E14 e aguardar a decisão específica do guarda
para PRs C/D/P00–P07/LAB.

## LAB-03 — regressão de alinhamento em revisão — 03/11/2026

Branch local `codex/lab03-horarios-alinhamento` sobre P07 local. A correção da
matriz e dos cabeçalhos já está na fonte desde `22c09f4`, refinada por
`4ac3a46`; esta revisão acrescenta um teste da matriz real de Horários que
compara as quatro colunas após rolagem horizontal/vertical e três larguras.
Três casos Tk dirigidos passaram em Xvfb no host. O defeito histórico da imagem
privada não foi reproduzido nesta fonte; DPI e monitor físico, percurso pelo
atalho com eventos elegíveis, Windows físico e PR/CI ainda exigem seus gates.
Nenhuma imagem privada foi copiada. Próximo: seguir os LAB restantes e E14
operacional após reconciliar a publicação da cadeia local.

## T13.4-P07 — consumidores de avaliações em revisão — 03/11/2026

Branch local `codex/t134-p07-consumidores-m1` sobre P06; o SHA `e538521`
abaixo identifica a composição anterior ao rebase de 03/11.
`analytics.pattern_queries` consulta os recibos P06 já gravados no ledger,
com namespace da base e corte de disponibilidade, e expõe inspeção dos fatos
de sinal/desfecho no `feature_store`. SDK, CLI `laboratory pattern-evaluations`
e `pattern-evaluation`, relatório HTML, guia Padrões do registro de estudos no
Laboratório e Dashboard usam a mesma leitura. O Dashboard apresenta os recibos
como exploratórios, ocultos por padrão, sem inventar lift, confiança ou
aprovação; ausência de resultado é explícita. A consulta não executa pesquisa
nem ordens. Ensaios sintéticos incluem CLI=SDK, HTML, reinício, corte anterior
ao registro e o mesmo símbolo em duas bases. Na composição local anterior com
entrada GUI e teste de monitor à esquerda, 58 testes dirigidos passaram, um foi
ignorado e o desenho emitiu 17 avisos de layout. `./run.sh --offline --config`
abriu a GUI em Xvfb isolado com SQLite de QA, e o menu Exibir → Registro de
estudos → Padrões mostrou o estado vazio sem fabricar avaliação. Com outro
SQLite sintético, a mesma rota exibiu o recibo persistido `reversal` em
`SYN / M1`, treino, zero eventos e estado exploratório, após reinício da base.
Os arquivos temporários de QA foram removidos. Amostra real elegível, CI/PR,
PostgreSQL e Windows físico permanecem separados.
Publicação aguarda ajuste específico do guarda central compartilhado para
branches de revisão; não contornar o hook. A prévia instalada segue em
`57d13ee`, sem estas mudanças. Artefatos privados permanecem no manifesto
existente, sem cópia. Próximo: completar gates de P07, revisar documentação
central e retomar os gaps operacionais E14 da fonte reconciliada.

## T13.4-P06 — fluxo inicial de padrões em revisão — 03/11/2026

Branch local `codex/t134-p06-fluxo-quatro-familias` sobre P05, que por sua vez
se apoia em P04. `analytics.pattern_families` compila reversão, continuação por
horário, gap e rompimento/falha na gramática tipada P02, com parâmetros
explícitos. `analytics.pattern_flow` lê somente snapshots congelados, preserva
namespace/relógio/sessão, registra acesso por papel antes de ler resultados,
avalia eventos e desfechos, chama baseline P04 e simulação P05 quando os planos
congelados permitem. Sinais e desfechos vão para definições distintas do
`feature_store`; o recibo fica no ledger de experimentos na mesma transação.
O preset em `presets.research` inclui fonte, definição, planos, resultado e
hashes de implementação, sem equivalência `.set` nem ativação. Replay com
fechamento presumido e sessão UTC sintética é marcado como tal, sem aceite
prospectivo. Ensaios sintéticos cobrem quatro famílias, duas bases de mesmo
símbolo, reinício, nenhum aprovado, veto a vazamento entre folds e rollback.
O conjunto P01–P06/datasets/experimentos teve 89 testes aprovados no Linux;
os seis testes diretos do fluxo foram repetidos após a contagem de negativos.
O teste transacional PostgreSQL do fluxo foi ignorado sem URL local; a CI da PR
deverá executá-lo. Nenhum dado real foi usado como fixture.
SDK e CLI `experiments pattern-create`, `pattern-request`, `pattern-evaluate` e
`pattern-status` expõem o fluxo no serviço existente, com modo sintético de
sessão apenas quando escolhido explicitamente para QA. P07 deve ligar os
consumidores de consulta à avaliação persistida.

## T13.4-P05 — adaptador econômico em revisão — 03/11/2026

Branch local `codex/t134-p05-simulacao-economica` sobre P04 para revisão
sequencial. `strategies.pattern_adapter` transforma eventos P02 verificados por
identidade/hash em sinais para `run_backtest`: entrada a mercado na próxima
abertura após disponibilidade, stop/alvo/prazo tipados e uma posição por ativo.
O modelo de custos exige origem, vigência, base bid, spread declarado, comissão
por lado, slippage, moeda e swap explícitos. O motor atual calcula lote, fill,
PnL, saldo e patrimônio; o adaptador relata evento sem execução e barra ambígua
com stop-first. Ordens limitadas, concorrência e moeda sem vínculo são recusadas.
O cenário dos testes é sintético/hipotético; nenhum resultado tem aprovação
econômica ou equivalência `.set`. Passaram 30 testes de adaptador, backtester e
eventos P02 no Linux. P06 deve persistir e consumir avaliações.

## T13.4-P04 — validação temporal em revisão — 03/11/2026

Branch local `codex/t134-p04-validacao-temporal` sobre P03. O plano de folds
aplica as mesmas janelas UTC a todas as fontes, recusa característica futura,
purga desfechos que cruzam o corte e embarga em tempo real. O baseline congela
estratos com metadados do treino; só compara pares de evento/controle da mesma
fonte, sessão e estrato após o corte de ajuste. Bootstrap por blocos de sessões
produz IC e uma permutação pareada explícita produz p diagnóstico sob hipótese
de intercambialidade; não implica causalidade. O contexto da campanha conta a
grade inteira, endpoints e definições extras, inclusive descartes, antes de
aplicar Bonferroni. Resolução de p insuficiente impede o status OK. P06 ainda
deve conectar snapshots, eventos e resultados persistidos. CI, dados reais,
Windows físico e qualificação final da reserva pendentes.

## T13.4-P03 — despacho por tipo de protocolo — 03/11/2026

Na branch local `codex/t134-p03-campanha-reserva`, a validação conjunta do
Laboratório encontrou uma regressão introduzida ao incluir campanhas de padrões
no guard do executor BASE: a recusa correta de um protocolo de associações
perdeu sua mensagem específica. O despacho agora conserva a orientação de
`laboratory protocol-evaluate` para associações e explica separadamente o fluxo
de campanha para padrões. Nenhum dos dois tipos chega ao executor de estratégia.
O caso falho e o novo caso de campanha passaram isolados (2/2); as suítes de
campanha e protocolo passaram juntas (13/13). Testes com fixtures sintéticas;
sem CI/PR da branch enquanto o guarda central bloquear novas referências.
Próximo: incorporar o corretivo nas branches P04–P07 e Laboratório, repetir a
seleção que falhou e publicar cada incremento quando houver destino autorizado.

## T13.4-P03 — campanha e reserva em revisão — 03/11/2026

Branch local `codex/t134-p03-campanha-reserva` sobre P02 local. O registro de
experimentos existente recebe protocolo versionado com roster de snapshots,
cortes globais UTC, endpoints, grade, critérios, semente e orçamento. Tentativas
idempotentes disputam o teto agregado sob o lock do store; recusas e descartes
ficam registradas. Acesso por papel e fonte é gravado antes de entregar uma
reserva; proposta automática posterior à exposição é recusada. Exposição
externa permanece desconhecida e a reserva não é qualificada como final.
Passaram 57 testes de campanha e regressão de experimentos no Linux; um teste
PostgreSQL foi ignorado porque a URL de teste local não está configurada.
P04 deve validar cortes e purga; P06 deve persistir avaliações/eventos. Sem
CI, PR, launcher, MT5 ou aceite real desta branch enquanto o guarda central
impedir publicação separada.

## T13.4-P02 — regras e eventos em revisão — 03/11/2026

Branch `codex/t134-p02-regras-eventos` sobre P01 local. `analytics.hypotheses`
valida AST JSON, unidade, fração decimal, lado, horizonte e hash imutável;
`analytics.pattern_engine` avalia barras fechadas por disponibilidade em lote
e prefixo, com estado reiniciável e fonte/símbolo/TF vinculados. Revisões
posteriores geram identidade própria sem reescrever eventos anteriores.
Desfechos futuros ficam separados, censurados até a recepção e sem alegar
lucro líquido; lacuna sem calendário comprovado também censura. O adaptador
`SignalSpec` aceita apenas o subconjunto de limiar de `close`, sem equivalência
MQL5/.set. Nove testes sintéticos passaram. Persistência, campanha, reserva,
simulação econômica, consumidores e aceite real pertencem às próximas fichas.
Branch local sem push/PR enquanto o guarda central impedir publicação separada.

## T13.4-P01 — catálogo e elegibilidade em revisão — 03/11/2026

Branch `codex/t134-p01-catalogo-elegibilidade`, baseada no P00 reconciliado.
`analytics.pattern_catalog` usa `data_inventory`, namespace de `datasets`,
qualidade/capacidades existentes e evidências de coleta/sessão do snapshot.
SDK e CLI `laboratory pattern-sources`, `pattern-qualify` e `pattern-freeze`
expõem a mesma seleção; captura limitada a 20.000 barras pelo store atual.
Duas bases sintéticas com o mesmo símbolo mantêm namespaces e snapshots
distintos. O leitor de Dados também aceita horário escalar em base com zona;
o snapshot dessa base continua bloqueado pelo contrato de offset fixo.
Passaram 57 testes de Dados/qualificação e 17 de relógio/P01 no Linux local.
Pesquisa, eventos,
simulação, dados reais, CI e launcher de GUI ainda não foram aceitos. O push
da branch documental e das branches de revisão aguarda resolução explícita
do guarda central compartilhado; não contornar o hook.

## E14 — auditoria pós-fill 2.13 e resgate privado — 03/11/2026

App `codex/e14-protecao-comum`, base `codex/f04-vinculo-presets`, PR279
draft, código e recibo `f36395334e431517752c5872c221aa6110228d49`
confirmados no remoto; HADOUKEN `d4d389ee0800d209bdba48ab36e392e6907949a4`/PR84
(fonte S09 2.13 `51358a16c1685cd79f4ffec0c8da464b72e10f5a`),
Safety `27b8599` detached/PR11. O manifesto fixa estas dependências para
desenvolvimento; a prévia instalada continua App `57d13ee`. O App concilia
TP em R pós-fill com a intenção e o efeito do journal; o perfil 2.13/15 exige
fingerprint e 59 inputs exatos. Os 109 testes dirigidos passaram. Compilação
MetaEditor/Wine: 0 erros, 0 avisos, 59 arquivos iguais à fonte. Backtest e
replay visual Tester do mesmo pacote sintético EURUSD/M5 14–16/10 terminaram
conciliados: oito entradas, sete ações, quatro alterações de TP pós-fill;
`reference_match=true` no replay. Prova pública em
`docs/evidencias/E14_POSTFILL_213_TESTER_REPLAY_20261103.json`.

O banco QA e os recibos privados estão na página pessoal de recuperação
existente, complemento de 50 arquivos/seis partes, ZIP SHA256
`2a8a6dff51b953a2bf8fe6f8c93904cad086d85ff0000163d004d61c96547080`.
Os anexos foram relidos da Library por hash e o verificador oficial extraiu
todos os arquivos; `qa.sqlite` passou `quick_check`. Não usar este ensaio para
autorizar DEMO ao vivo, netting, Windows físico ou perfil operacional. Próximo:
conferir o CI do SHA publicado e continuar os gates
E14 de recuperação/capacidade, pendentes, orçamento por ativo/grupo/plano,
gradientes, probabilístico e SA-01. A cadeia documental/LAB/T13.4 segue local
até a guarda central permitir a publicação da branch correspondente.

## E14 — integração de desenvolvimento do S09 2.12 — 03/11/2026

App `codex/e14-protecao-comum`, base `codex/f04-vinculo-presets`, publicou
`1310be9b80fa67d7a42f6a24661c69d2ee76529e` na PR279 draft; SHA local,
remoto e da PR conferidos. O manifesto de desenvolvimento agora fixa HADOUKEN
`8a11fcfeed9292b54432b4b5c6d5a02bcbb50807`/PR84, fonte S09 2.12
`f99ebde132c10bb078e78796e7ef048082ba07cc`,
e Safety `27b8599`/PR11. O perfil 2.11 anterior conserva fingerprint/revisão
próprios. `postfill_r_v1` é modo tipado distinto de R planejado e `fixed_r`
Python; gera InpTargetMode=3 e usa pacote vinculado somente no Tester DEMO.
O preparador oficial inspecionou os três checkouts e aceitou os pins; o modo
`--plan` passou, sem instalar nem alterar a prévia `57d13ee`. Na fonte App,
44 testes de perfil/UI e 97 de composição/execução de preset passaram;
o formulário Manual exportou/reabriu o modo com recibo sintético. HADOUKEN
2.12 passou duas checagens de fonte e compilou no MetaEditor/Wine com 0 erros,
0 avisos e 59 arquivos idênticos ao plano da fonte exata; o recibo público está
na PR84. CI App da nova ponta iniciou. Tester/replay por fonte 2.12,
percurso real do atalho, Windows físico, netting
e DEMO permanecem gates separados; nenhum perfil operacional foi liberado.
Os recibos privados anteriores seguem no manifesto e resgate independente;
nenhum novo recibo privado indispensável foi retido nesta etapa. Próximo:
ensaiar 2.12 no Tester sob posse/limites do runtime, conferir Windows CI e
seguir os itens E14 ainda abertos. A cadeia LAB/T13.4 local já incorporou esta
base em ordem, sem publicação por causa da guarda compartilhada.

## E14 — segunda correção de layout Windows em revisão — 03/11/2026

Na PR279, App `d68e125`, o rerun Windows 41668845297/attempt 2 concluiu com
4552 testes aprovados, 193 ignorados e 2 falhas (cobertura 77,74%). Retenção
do lease e os dois casos UTF-8 passaram; restaram busca/ações do catálogo e
gráfico da comparação em janela estreita. O teste de catálogo observou botão
pedido com 331 px, alocado em 294; a comparação observou canvas 672 px em
viewport de 558. O primeiro attempt desse SHA parou antes, no limite de 60 s
de um teste de lote com 6 ativos; não comprovou estas duas falhas.

Este incremento deixa ações do catálogo em linhas de largura integral quando
estreito e limita os canvases TkAgg à largura da página ao mudar DPI/tema.
Uma regressão sintética reproduziu recorte de botão no Linux antes da correção;
depois passaram 10 testes dirigidos de catálogo e comparação em Xvfb. O check
Windows do SHA sucessor, o launcher da fonte nova e Windows físico ainda são
gates próprios. HADOUKEN `2809938`, Safety `27b8599` e prévia instalada
`57d13ee` não mudaram. Próximo: publicar na mesma PR279, confirmar SHA remoto,
checks e rebater a cadeia local dependente sem perder os commits.

## LAB-10 — geometria de monitor em QA — 03/11/2026

Branch `codex/lab-series-monitor-geometry`, base local
`codex/gui-dependencias-inicializacao` (dependente da PR de GUI). A simulação de
monitor à esquerda pede `1024x800+-1024+-50`; neste host de um monitor o
gerenciador de janelas entrega `(256, 37)`. O teste passou a conferir o pedido
e o ciclo ampliar/restaurar, sem exigir posição física impossível. Antes falhou
1/1; depois passaram 29 testes de Séries, 1 ignorado e 16 avisos de layout
na fonte anterior `8b110aa`. A branch foi rebatida sobre GUI `13f6b97`;
o mesmo ajuste está em `d023c2f`, com seis testes GUI/monitor focados aprovados
na composição rebatida.
Na ponta local `8822d253`, o módulo completo de UI de Séries passou em
Linux/Xvfb: 29 aprovados, 1 ignorado e 16 avisos de layout em 127,52 s.
Sem mudança de runtime. Monitor físico negativo, atalho central e os demais
percursos LAB-01…10 mantêm os aceites próprios do roadmap; esta correção não
os fecha. Branch/PR remotos dependem da guarda central de revisão.

## Inicialização da GUI — corretivo local em revisão — 03/11/2026

Branch `codex/gui-dependencias-inicializacao`, rebatida sobre App
`0993ae7`/PR279; código da GUI em `bf38e4a` local. O recibo inicial do launcher
pertence à fonte equivalente `baa24c4`; depois do rebase, cinco testes da GUI
e um de monitor passaram na composição dependente.
O chamador agora consome `(sucesso, faltantes)`; ausência mostra os nomes e o
instalador oficial (`./install.sh` ou `install.bat`) e encerra a abertura.
O pré-voo do script ocorre antes dos imports de runtime, com o catálogo de
módulos do doctor; não instala pacotes automaticamente. 23 testes de entrada,
offline e doctor passaram no host. `./run.sh --offline --help` terminou com 0;
o parâmetro `--config` com `--offline` permite apontar uma sessão de QA para
configuração/base explícitas sem tocar a base principal. Cinco testes diretos
passaram. Pelo launcher `./run.sh --offline --config` em Xvfb próprio, a
dependência presente abriu a janela e criou apenas o SQLite de QA (`quick_check`
OK). Com `pandas` ausente por simulação no processo, o mesmo launcher mostrou
o diálogo e a ação `./install.sh`, sem criar banco ou iniciar instalação.
O percurso pelo mesmo launcher foi repetido na fonte rebatida `13f6b975`:
janela e SQLite isolado com `quick_check` OK; na simulação de `pandas`
ausente, diálogo e orientação ao instalador sem banco ou instalação.
Cinco testes diretos passaram; doctor offline 16 OK/2 avisos/0 falhas.
O diretório temporário de QA foi removido.
[Recibo público](docs/evidencias/GUI_DEPENDENCY_STARTUP_20261103.json).
Windows físico e CI de SHA publicado permanecem pendentes.
Doctor offline de desenvolvimento: 16 OK, 2 avisos (pandas-ta opcional e integração
nativa não sondada), 0 falhas.
Base privada, ponte, terminal, negociação e referências aceitas não foram usados
como fixture. Publicação desta branch depende da guarda central de PRs separadas.

## E14 — saneamento do Windows CI em revisão — 03/11/2026

PR279 segue aberta/draft na branch `codex/e14-protecao-comum`. No SHA
`a74f53977bd9e67250b072df43a17d0c3ee06a12`, Linux, PostgreSQL, instaladores
e lint passaram; Windows terminou com 5 falhas, 4549 testes aprovados e 193
ignorados. Este corretivo trata a leitura da trava pelo teste durante o lease
Windows, duas leituras de UTF-8 sem codificação explícita e dois controles Tk
mais largos que seus viewports. No host, 64 testes pertinentes passaram e o
doctor offline teve 16 OK/2 avisos/0 falhas. A suíte integral foi interrompida
com código 143 após 6%; não há aceite integral local. O CI do novo SHA tem
recibo distinto; Windows físico, MT5 e negociação não são certificados pelo CI.
Composição operacional: App na PR279, HADOUKEN `28099386`/PR84 e Safety
`27b85992` detached/PR11. O manifesto desta branch fixa esses componentes.
A prévia instalada ainda aponta para App `57d13ee`; atualizar somente após
publicação e verificação pelo launcher, preservando as sessões abertas.

## Estudo de descoberta de padrões —02/11/2026

Pedido atual: primeiro estudo/plano/roadmap para outro agente implementar.
[Roteiro T13.4-P00–P11](docs/analytics/DESCOBERTA_PADROES.md), com inventário
confirmado, contratos, método, testes, marcos e repasse executável na seção11.
P00 é somente documental; M1 exige P01–P07 e quatro famílias ponta a ponta.
Próxima ficha dessa frente: P01 (catálogo/elegibilidade), sem criar motor/store/
fila paralelos, sem ordens e sem reutilizar autorização DEMO anterior.
“Infut/5h25” aguarda definição; não bloqueia as famílias genéricas.

Base de origem Appc956305, PR279 operacional ainda aberta/draft; fonte e SHA
documental atuais ficam no registro central Projects/.workspace. Este estudo
preserva a frente operacional abaixo e os componentes existentes, sem repins
ou modificação de runtime/prévia. Não interpretar publicação do plano como
entrega de inteligência implementada ou aceite econômico.

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

## Encerramento por sessão e replay visual —02/11/2026

App57d13ee (PR279): `session.pause`/`resume`/`close_out` liquidam por ticket só a
exposição da sessão, sem UI; qualificado no Tester, DEMO não exercitada. Replay
pela UI segue não repetível em180s (179,3 aprovado/181,6 interrompido); perfil
S09 2.11 continua bloqueado. [Detalhes](docs/continuidade.md#encerramento-por-sessão-sem-interface-e-orçamento-do-replay-visual-02112026).

**03/11/2026:** HADOUKEN `2809938` limita os quadros do painel nativo no replay
visual (teste visual 85–99 s → 49,7 s). Jornada real dos botões 3/3 dentro de
180 s (replay 122,2/123,6/139,7 s, referência e ciclo funcional; backtest
87,0/73,7/83,5 s). Perfil S09 2.11 qualificado: fingerprint `527e696bdcc9f147`,
revisão `2809938`. [Detalhes](docs/continuidade.md#painel-nativo-com-quadro-limitado-e-perfil-s09-211-qualificado-03112026).

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
idênticas reaproveitadas e dois consumidores recompilados). Wrapper59 compilado0/0.
Na fonte2.11, seis backtests cada tick conciliados: ATR com7trailing/2BE, alvo
dinâmico com2closes, saída por sessão, saída por sinal, normal e invertido com
2parciais cada. Todos com diário encerrado e recursos liberados; ticks reais
não informados. Replay ATR após o corretivo Appf8cb06f coincidiu com o MESMO
pacote/banco:3posições/6deals/7trailing/2BE, diário3entradas/9ações encerradas e
recursos liberados,153,275s. As falhas anterioresRAM85%/180s foram preservadas.
O launcher compila no perfil SDK próprio antes de inicializar o Wine visual,
com prova de saída do compilador, sem reiniciar o prefixo compartilhado.
216regressões/11módulos passaram em52,91s e doctor16OK/2avisos/0falhas sem probe.
Atalho real2.11 também criou/exportou/reabriu Manualv2/59inputs em6,49s; imagens
da própria janela conferidas. Chamada direta UI/API agora admite e finaliza
seu job no mesmo Budget configurado; runner com controle existente o conserva.
Pacote/artefatos e run_id ficam ligados à tentativa; retomada preserva backtest/
replay, recusa de recursos antecede o MQL e falha no checkpoint é explícita.
167testes/14módulos em145,31s, inclusive famílias anteriores e agentes. A chamada
real do botão Backtest no atalhoef0f429 confirmou pacote/banco originais,
59inputs,3posições/7deals/1parcial/2BE/5trailing, diário3entradas/8ações encerradas,
caixa1,46USD e recursos liberados,175,747s. Replay pelo botão atingiu180s e
liberou recursos; comparação não aprovada. A primeira janela reaberta manteve
o contexto padrão01→02nov/100000 e não teve operações: não exercitada; receita
corrigida repôs12→13out/10000 pelos widgets, sem alterar o recibo anterior.
Revisão posterior ligou o retentor já existente também à admissão tardia do
Budget, protegendo perfil que falhe antes de publicar run:347regressões/22módulos
em212,93s e doctor16OK/2avisos/0falhas sem probe. Evidência anterior na tela é
limpa após falha nova; permanece consultável no banco. Regressão reproduzida e
6casos Tk/7,83s passaram após o corretivo final de apresentação. Fonte MQL não mudou.
CI App28d: Linux passou; Windows7falhas/4522passaram/193não executados. Shim MQL
long do teste corrigido para int64 explícito;29testes/5,09s no Linux, Windows pendente.

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
Nenhum PLAY nem autorização DEMO transferida. Página privada canônica seq35:
último delta245arquivos/dois bancos/quatro sessões restaurados da nuvem, todos
os hashes/contagens de TODAS as tabelas conferidos. Ensaios financeiros2.11
posteriores à coleta estão no próximo delta. Credenciais/contas/configurações/logs brutos não integram o
resgate. Fatos por fonte em E14_COMMON_POSITION_20261102.json. Pilar segue em
execução: TP R pós-fill, recuperação/capacidade/pendentes, SA-01, orçamento
ativo/grupo/plano, gradientes, probabilístico e aceites DEMO/netting/Windows físico.

## Auditoria de ticks versão 2 — 12/11/2026

Owner Dados/Laboratório, fila DIAG-TICKS, continuação da PR304.
Foram reproduzidas 15 falhas em flags não finitas, fracionárias ou fora do
intervalo, sequência inválida/duplicada e auditoria vazia com promoção de classe.
A versão 2 registra metadados inválidos e mantém a captura original. A classe A
exige auditoria atual com linhas, e a certificação intrabarra exige sequência
sem defeitos bloqueantes. Point inválido ou conversão que exceda o número finito
é diagnosticado, conservando o spread em unidades de preço. Arrays inteiros
nullable têm seus valores ausentes contabilizados. O corretivo de timestamps
inválidos e seus seis testes já presentes no checkout foram revisados e preservados
neste incremento.

Regressão Linux em lote único com elegibilidade e recuperação teve 80 aprovados.
Após cobrir booleanos e inteiros nullable, o recorte final teve 34 aprovados em
3,47 s. Contagens sobrepostas. Doctor offline teve 16 OK e dois avisos.
O percurso CLI dedicado e a GUI aguardam uma janela sem a VM de outra tarefa,
que passou a ocupar cerca de 2 GiB com pressão de I/O. A suíte Windows da ponta
anterior falha no lote de cancelamento SQLite, item DIAG-CI304. O CI da nova
ponta terá estado próprio. Windows físico, Tester e DEMO conservam seus aceites.

## DIAG-AMOSTRA — agrupamento de execuções em 12/11/2026

Owner escritor Codex, Laboratório. Branch `codex/lab-amostra-execucoes`,
encadeada na PR304 `dc17f49`, sem alterar a branch do checkout único.
O índice principal e os WIPs anteriores foram preservados.

O agrupamento padrão nas lentes exige execução Python qualificada pelo
produtor existente, com identidade v1, snapshot íntegro e mesmo escopo,
código, parâmetros, custos/avaliação/moeda e conteúdo integral do livro.
A janela de exibição usa a equivalência do run completo. Legados com identidade
incompleta são preservados com incerteza explícita. Referência de snapshot
quebrada interrompe a análise. A consulta integral está na CLI por
`trades report --include-repeated-runs` e na GUI pelo controle correspondente.
A seleção explícita de run no roteador mantém acesso às repetições guardadas.
As contagens excluídas acompanham os filtros exibidos. O livro continua íntegro.

Reprodução e validação locais em SQLite próprio, Python 3.11 do `.venv`,
BLAS/OMP com um thread. Dois limites novos falharam antes do ajuste, moeda e
diferença fora do período visível. Lote de regressão 47 aprovados em 42,16 s,
com sobreposição com os lotes anteriores de 35 e 43 casos. Três backtests
reais do produtor em 600 barras sintéticas guardaram nove operações.
Launcher CLI e janela própria XWayland mostraram três na visão padrão e nove
na integral. Recibos em `state/qa-tmp/diagnostico-20261112` da raiz MT5X.
A primeira chamada `--offline --cli` foi recusada pelo wrapper e a primeira
sonda GUI falhou por configuração da instrumentação; os percursos corrigidos
estão registrados no handoff local e nos recibos.

A regressão adicional de referência de snapshot ausente e o recorte PostgreSQL
foram adicionados após o lote local. A validação final da árvore publicada
foi direcionada ao CI existente enquanto a VM de outro projeto mantém pressão
de I/O no host. O CI completo permanece um gate. O último ajuste de integridade
aguarda evidência do CI. QA descrito corresponde à composição do checkout com
PR304 e PR305, com o overlay de amostra. A árvore desta PR conserva o
recorte de amostra sobre PR304, com as excursões em PR própria.

Aceite deste incremento exige os novos testes no CI, preservação de seleção
explícita/legado/filtros/moeda, regressões do livro/cache/roteador e validação
da composição da próxima prévia. Entrada física, acessibilidade, login,
Windows físico, MT5 e negociação têm aceites próprios.

## DIAG-AMOSTRA — leitura legada e validação retomada em 12/11/2026

A retomada reconferiu fonte, processos e PRs. O host estava sem VM ou build
concorrente, com aproximadamente 7,8 GiB disponíveis e swap sem atividade.
As regressões foram divididas em lotes proporcionais ao orçamento.

A leitura integral de cada execução agora ocorre após qualificar os metadados
do produtor. Uma regressão com banco e produtor próprios confirmou a leitura
redundante para um run legado antes do ajuste e sua eliminação depois dele.
O livro legado e a indicação de equivalência desconhecida são preservados.
As execuções qualificadas passam pela comparação integral e validação de snapshot.

Depois do ajuste, lotes de 9 e 26 casos passaram, com 14 casos da amostra e
21 de assinaturas/cache no conjunto. O lote anterior de contexto/roteador passou
com 14 casos. Os resultados têm sobreposição com os recibos anteriores.
Doctor novamente com 16 OK, 2 avisos e zero falhas. O teste de 50 mil operações
continua com seu limite original e integra o gate completo do CI.

O primeiro lote orçamentado de 60 s terminou após 11 casos, com pico observado
de 159,3 MiB. O processo próprio foi encerrado e as duas fontes temporariamente
usadas na validação, junto com o índice principal, foram restauradas. O ensaio
parcial permanece em `sample-final-tests.json`; os lotes separados concluídos
estão no handoff local. A QA local usa a composição do checkout, com os corretivos
de excursões. O CI da PR valida a árvore publicada sobre PR304. Sua conclusão
e a próxima prévia continuam pendentes.


## DIAG-LEASE-REBOOT — identidade e retomada em 12/11/2026

O helper comum preserva o digest legado e passa a registrar boot e namespace
Linux com a identidade de processos novos. Um boot anterior só prova término
quando o digest confirma a mesma máquina, o UUID/namespace são válidos e o
processo é anterior ao boot atual. Host estrangeiro, namespace divergente no
mesmo boot, identidade parcial, erro de leitura e registro legado sem essa
prova continuam preservados. O protocolo Windows conserva seu escopo existente.

Foram reproduzidas retomadas indevidas com identidade ausente/incompleta,
estado desconhecido e histórico de job corrompido. A fila existente agora
revalida a identidade do solicitante, estado, tempo e histórico antes do CAS,
na mesma transação. Consulta sem claim continua disponível. A recuperação do
produtor e o lease de Recursos usam o mesmo helper. Nenhum scheduler foi criado.

Validação local em Python 3.11.15, um processo e BLAS/OMP em um thread. Lotes
focados reuniram 43 casos distintos aprovados, com sobreposição entre lotes.
Os três casos finais de posse ativa, publicação interrompida e histórico de
falha passaram em 5,76 s. O launcher oficial criou uma cópia SQLite própria,
gravou evidência de boot e liberou o lease. Doctor offline teve 16 OK, dois
avisos e zero falhas. Recibos locais estão em
`state/qa-tmp/diagnostico-20261112/lease-*.xml` e `lease-launcher/proof.json`
da composição. VM alheia preservada; RAM disponível observada próxima de
6 GiB, sem ensaio de carga, alteração de serviços ou dados do proprietário.

Aceites restantes incluem CI do SHA publicado, Windows físico e reinício
físico em janela autorizada. Os metadados de boot das regressões são
controlados. A trava real inspecionada já estava liberada, sem reparo manual.
As evidências desta correção não diagnosticam a causa dos reinícios do host.


## DIAG-RETENCAO — cópias com proveniência, 12/11/2026

O expurgo global de `*.db` apagou um arquivo desconhecido no ensaio anterior
à correção. O produtor existente agora grava recibos privados `mt5x-backup/v1`
ao lado das cópias, com ID estável, identidade física da origem/artefato, bytes
e SHA-256. O nome novo inclui UUID e prefixo limitado para conservar identidade
entre solicitações. Legado sem recibo, cópia de outra base, link, conteúdo
alterado e schema ilegível/desconhecido são preservados. A leitura de cópia
recente usa a proveniência registrada. O hash integral é reconferido no expurgo.

`backup_keep` conta cópias reconhecidas da base atual. Intenções de retenção
são sincronizadas no recibo antes de cada exclusão. Resultado incerto bloqueia
novo expurgo. `resources backup-status --operation-id ID` consulta/reconcilia
presença e identidade, sem repetir exclusões. Ausência é registrada como
`absent`. Conteúdo conhecido presente recebe `not_applied`. Conteúdo alterado
continua pendente. Os recibos persistem após expirar o arquivo SQLite, mantendo
o resultado consultável. Recibos são privados, limitados a 256 KiB cada.
A GUI mostra arquivos preservados e solicita Conferir retenção pelo worker.

Validação final local teve 22 regressões novas aprovadas em 6,49 s. Um lote
anterior de 27 casos, sobreposto, passou em 7,03 s com identidade/restauração.
Dois casos existentes de cópia/CLI foram reexecutados separadamente. O lote
integrado inicial foi interrompido por orçamento após 31 aprovados em 101,74 s,
com recibo e resultados parciais preservados. O maior processo observado nesse
lote teve cerca de 155 MiB RSS, com RAM disponível acima de 6 GiB e 48 °C na
medição. A estimativa de duração desse lote foi excedida e o restante foi
dividido. Doctor offline teve 16 OK, dois avisos e zero falhas.

Launcher oficial em quatro chamadas próprias confirmou retenção, arquivo
desconhecido com valor 7 preservado, recibo conservado e consulta do ID expirado.
Outro ensaio CLI com falha controlada após excluir retornou código 1/estado
incerto e a consulta reconciliou ausência. Recibos locais em
`state/qa-tmp/diagnostico-20261112/retention-*`. Os bancos dos ensaios são
descartáveis. QA gráfico do artefato final aguarda capacidade do host, ocupado
pela VM alheia. CI do SHA publicado, Windows físico e perda de energia possuem
aceites próprios. A causa dos reinícios do host continua sem diagnóstico.


## DIAG-RETENCAO — fronteiras ampliadas em 12/11/2026

A primeira revisão deixou passar a perda de um recibo pendente. As duas
regressões seguintes reproduziram exclusão de outra cópia quando o recibo
ficou ausente ou corrompido. O produtor agora conserva todos os possíveis
alvos diante de recibo desconhecido/incompleto e de marcador de cópia sem
recibo. Recibos válidos de outras bases conservam seu isolamento. Restauração
pendente também impede expurgo. A consulta informa os recibos indisponíveis
na CLI e na GUI, com cópia nova e resultado da retenção em estados próprios.

Outra regressão reproduziu troca da identidade da origem após o snapshot com
sucesso indevido. A identidade é fixada antes do lease e reconferida depois
do snapshot/hash, incluindo a identidade do artefato. A falha conserva o ID
da solicitação e evita auditar uma base que substituiu a origem escolhida.

Quatro ensaios próprios reproduziram a limpeza genérica apagando cópia, base
ativa, arquivo e controle quando as pastas se sobrepunham. O leitor de
candidatos agora protege os diretórios de dados/controle, arquivos SQLite,
recibos e arquivos auxiliares da base ativa. Referências positivas dos recibos
de restauração conservam origem e cópia de segurança após mudar backup_dir.
O teste de mudança de pasta demonstrou preservação dessas referências e
remoção de um CSV próprio elegível. A guarda de referências continua no
serviço existente, sem catálogo ou mecanismo de limpeza paralelo.

Validação final com sete fontes registradas e hashes idênticos antes/depois
teve 44 aprovados em 9,75 s, incluindo 35 casos novos e nove de restauração/
identidade. Guardas existentes tiveram 28 aprovados e um caso gráfico sem
DISPLAY em 7,36 s. Dois casos de limpeza/CLI passaram em 2,52 s. Doctor
offline teve 16 OK, dois avisos e zero falhas. Resultados intermediários foram
repetidos após a estabilização das fontes. Nenhum gate foi afrouxado.

Quatro chamadas do launcher oficial com dados próprios produziram saídas
0/1/1/1. Após perder um recibo pendente, o ciclo conservou as cópias anteriores
e a consulta confirmou a pendência sem solicitar exclusão. Evidências em
`retention-unknown-launcher/proof.json`, `retention-frozen-final.xml`,
`retention-existing-frozen.xml` e `retention-final-frozen-sources.json`.
O artefato 2066 preparado anteriormente foi marcado como superado antes de
qualquer ativação. Publicação e composição desta revisão exigem nova
conferência de SHA, CI e QA gráfico do artefato exato.


## DIAG-RETENCAO — leitura completa do controle em 12/11/2026

Reprodução em árvores SQLite próprias mostrou que a enumeração de arquivos podia
ocultar uma falha de leitura e esquecer uma intenção pendente. Recursos agora
materializa a leitura e propaga a falha antes de qualquer expurgo. A cópia já
conferida permanece disponível, com retenção incerta e consulta explícita.

No Linux, 45 regressões de cópia/restauração passaram em 10,15 s e 29 de
proteção, sessões e leitura parcial passaram em 13,31 s. O caso Tk antes
adiado também executou no compositor real. Sete hashes de fontes foram
conferidos antes e depois. Doctor offline teve 16 OK, 2 avisos e zero falhas.
Um processo, BLAS/OMP com um thread, sem suíte completa local. Falhas originais
foram preservadas. CI deste incremento e GUI do artefato composto seguem
pendentes. Entrada física e interrupção de energia têm aceites próprios.


## DIAG-RESTORE — contrato de relógio após a troca em 12/11/2026

Duas falhas reproduzidas mostraram que a validação de relógio da sessão anterior
continuava em cache após restaurar uma base UTC ou com outro fuso. A restauração
agora invalida essa validação junto ao cache de tabelas, inclusive se o efeito
terminar incerto. A próxima gravação passa pelo validador canônico existente.

Seis casos cobrem relógio e fuso divergentes, compatibilidade e resultado
interrompido após a cópia. O lote integrado de restauração e tempo teve 39
aprovados em 14,20 s no Linux, com dados próprios, um processo e um thread.
A reprodução anterior teve duas falhas e um caso compatível aprovado. CI deste
SHA e QA do artefato composto permanecem em validação.


## DIAG-LEASE — leitura do teste sob bloqueio em 12/11/2026

A tentativa Windows1 foi cancelada prematuramente por esta tarefa após mais
de duas horas. O log arquivado mostrou avanço até98% e uma falha anterior no
caso de reconciliação do lease. A tentativa2 no mesmo SHA foi solicitada antes
da leitura dessa falha. O teste abria outro descritor para ler o byte que
mantinha bloqueado. O contrato do Windows recusa esse acesso pelo segundo
descritor no mesmo processo. [Fonte primária](https://learn.microsoft.com/en-us/windows/win32/fileio/locking-and-unlocking-byte-ranges-in-files).

Uma reprodução local com leitura externa recusada teve1 falha/1 aprovado.
O teste passou a observar o descritor que adquiriu o lease e conserva as
assertivas de estado ativo/liberado. Também prova a recusa do leitor externo.
O bloqueio real e a produção conservam seu fluxo.66 casos locais passaram
em39,14s, um processo e BLAS/OMP com um thread. O novo Windows CI ainda
precisa confirmar a portabilidade. Os logs anteriores permanecem preservados.
