# Fase 15 — Gestão de recursos (espaço, higienização e automação)

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


**Pedido do dono do projeto (2026-10-14):** abrir uma frente paralela ao roadmap com uma aba de gestão de recursos
na interface. O primeiro problema é o espaço: a base pode ficar muito grande. A aba deve monitorar a base e o disco,
higienizar, permitir limpar ativos sem resultado positivo, não usados em estudos ou obsoletos, e oferecer
monitoramento e automação programada **somente se o usuário ligar** (opt-in). Deve ser robusta, resiliente e
preparada para novas frentes (logs, exportações, cópias de segurança).

Branch: `tarefa/T15-gestao-recursos`, em worktree separado, para não interferir nos agentes do roadmap. Os arquivos
novos ficam em `mt5_extracao/resources/` e `mt5_extracao/ui/resources.py`; nos arquivos compartilhados as mudanças
são pontuais (registro da aba, do subcomando e das chaves de configuração).

## Princípios

1. **Nada some sem volta.** "Limpar" um ativo é **arquivar**: a tabela é copiada para um arquivo SQLite próprio em
   `database/arquivo/`, a cópia é conferida (mesmo número de linhas) e só então a tabela sai da base. Restaurar
   devolve a tabela e o registro dela. A regra do AGENTS.md ("nunca apague tabelas de usuários") vale para
   mudanças de schema; aqui a remoção é uma **ação explícita do usuário**, sempre precedida do arquivo.
2. **Protegidos nunca são candidatos:** favoritos, ativos na seleção, ativos com benchmark vinculado, ativos usados
   como benchmark de outro, e ativos com **benchmark positivo**: já houve backtest e o ativo ficou positivo em
   algum dos robôs ou configurações testados (qualquer execução, não só a mais recente; definição do dono do
   projeto, 2026-10-14).
3. **Automação é opt-in e conservadora.** Com `[RESOURCES] auto_enabled = false` (padrão) nada roda sozinho. Ligada,
   cada ação automática tem a sua chave própria, também desligada por padrão, e a automação só arquiva (reversível).
4. **Medir é barato e não trava a interface.** Toda leitura roda em segundo plano; medição pesada (tamanho por
   tabela) usa `dbstat` do SQLite quando disponível e cai para estimativa por linhas quando não.
5. **Uma ação por vez:** trava de arquivo (`database/arquivo/.lock`) impede que a interface e o agendador do sistema
   (cron / Agendador de Tarefas) mexam na base ao mesmo tempo.
6. **Tudo auditado:** cada ação grava em `_resource_actions` (quando, o quê, origem ui/cli/auto, bytes antes e
   depois). Retratos periódicos ficam em `_resource_snapshots` (mudança aditiva: bases antigas continuam abrindo).
7. **Extensível:** as regras de higienização e os "módulos" de medição ficam em registros (`hygiene.RULES`,
   `storage.EXTRA_DIRS`); uma frente nova acrescenta uma entrada, sem mexer na aba.

## Tarefas

| Tarefa | O quê | Estado |
|---|---|---|
| T15.0 | Este plano | ✅ |
| T15.1 | Medição: tamanho da base e de cada tabela, espaço recuperável, disco livre, pastas (logs, exportações, arquivo); retratos e projeção de crescimento | ✅ |
| T15.2 | Higienização: regras `sem_lucro`, `sem_estudo`, `obsoleta`, `vazia` e registros órfãos, com proteções | ✅ |
| T15.3 | Ações seguras: arquivar (com conferência), restaurar, otimizar (VACUUM com checagem de disco), limpar órfãos; auditoria e trava | ✅ |
| T15.4 | `mt5x resources status | scan | archive | restore | archives | vacuum | orphans | auto | history` | ✅ |
| T15.5 | Automação opt-in: política em `[RESOURCES]`, ciclo (medir → alertar → agir), alertas pelo `notify`, agendador na interface e comando para o cron | ✅ |
| T15.6 | Aba "Recursos" na interface: indicadores, Espaço, Higienização, Arquivo, Automação e Histórico | ✅ (vista em tela virtual no Linux; Windows não visto) |
| T15.7 | Cópia de segurança conferida (SQLite, API de backup + `quick_check`, guarda as N mais novas) e limpeza de logs e exportações antigos; guia "Cópias e arquivos", `mt5x resources backup | backups | files`, opções na automação | ✅ (Linux) |
| T15.8 | Arquivo em Parquet (`archive_format`); aparar séries por idade com retenção automática para intradiário e ticks/book (SQLite e PostgreSQL, reversível); aviso de série acima de `max_series_mb`; voltar a uma cópia de segurança pela interface e por `mt5x resources restore-backup` (guarda antes o estado atual) | ✅ (Linux; PostgreSQL só pelo caminho genérico exercitado sobre SQLite) |
| T15.9 | Limites que **bloqueiam** a gravação (`block_disk_free_pct`, `block_db_gb`: o extrator para antes de gravar o bloco, com o motivo); retenção por ativo (`retention_overrides`); `drop_chunks` do TimescaleDB ao aparar hypertables (o DELETE cuida do bloco parcial) | ✅ (Linux; TimescaleDB real não visto) |
| T15.10 | Próximas frentes: guarda também nos ticks e no book (hoje só nas barras), painel vivo no navegador com os indicadores de espaço | proposta |

## Regras de higienização (T15.2)

| Regra | Quando a série é candidata |
|---|---|
| `sem_lucro` | Houve backtest no ativo e nenhum, de nenhum robô ou configuração, foi positivo (sem benchmark positivo) |
| `sem_estudo` | Nenhum backtest no ativo (em nenhum timeframe) |
| `obsoleta` | Última barra mais velha que `[RESOURCES] obsolete_days` (padrão 180) |
| `vazia` | Tabela sem linhas |

Órfãos: linhas de `_symbol_tables` e `_extraction_log` que apontam para tabelas que não existem mais.

## Configuração (`[RESOURCES]`)

| Chave | Padrão | Uso |
|---|---|---|
| `auto_enabled` | `false` | Liga o monitoramento automático (opt-in) |
| `monitor_interval_minutes` | `60` | Intervalo entre ciclos |
| `alert_db_gb` | `10` | Avisa quando a base passar disso (0 = desligado) |
| `alert_disk_free_pct` | `10` | Avisa quando o disco livre cair abaixo disso (%) |
| `auto_vacuum` | `false` | Otimiza sozinho quando o recuperável passar de `vacuum_min_mb` |
| `vacuum_min_mb` | `200` | Limiar da otimização automática |
| `auto_clean_orphans` | `false` | Remove registros órfãos sozinho |
| `auto_archive` | `false` | Arquiva sozinho as séries das regras em `auto_archive_rules` |
| `auto_archive_rules` | `obsoleta,vazia` | Regras que a automação pode arquivar |
| `obsolete_days` | `180` | Idade para a regra `obsoleta` |
| `archive_dir` | vazio | Pasta do arquivo (vazio = `arquivo/` ao lado da base) |
| `keep_snapshots_days` | `365` | Quanto histórico de medições guardar |
| `auto_backup` | `false` | Cópia de segurança automática da base (só SQLite) |
| `backup_interval_hours` | `24` | Intervalo entre cópias |
| `backup_keep` | `5` | Cópias reconhecidas da base atual guardadas pela retenção. Arquivos sem prova de posse/conteúdo e operações pendentes são preservados |
| `backup_dir` | vazio | Pasta das cópias (vazio = `copias/` ao lado da base) |
| `auto_clean_files` | `false` | Apaga sozinho logs e exportações antigos |
| `logs_keep_days` | `30` | Idade dos logs apagados (0 = nunca; o log em uso nunca é apagado) |
| `exports_keep_days` | `0` | Idade das exportações apagadas (0 = nunca) |
| `archive_format` | `sqlite` | `sqlite` ou `parquet` (menor; exige pyarrow) |
| `auto_trim` | `false` | Apara sozinho o histórico pela retenção abaixo |
| `retention_intraday_days` | `0` | Retenção das barras menores que 1 dia (0 = tudo) |
| `retention_ticks_days` | `0` | Retenção de ticks e book (0 = tudo) |
| `max_series_mb` | `0` | Avisa quando uma série passar disso (0 = desligado) |
| `block_disk_free_pct` | `0` | Extração para de gravar com disco livre abaixo disso (%; 0 = nunca) |
| `block_db_gb` | `0` | Extração para de gravar com a base acima disso (GB; 0 = nunca) |
| `retention_overrides` | vazio | Retenção por ativo, `ATIVO:dias` (0 = guarda tudo do ativo) |

## Validação

- Testes em `tests/test_resources.py` (medição, regras, arquivar/restaurar ida e volta, trava, automação, CLI).
- Windows: só simulado. Pendências no checklist de `docs/validacao_instaladores.md` (T10.W).

## Restauração com intenção e recuperação — 12/11/2026

O fluxo SQLite de `resources.files.restore_backup` usa o produtor de backups
existente e o mesmo lease de Recursos durante preparo, conferência e troca.
A GUI solicita o serviço em `run_bg`. Extrações e backtests devem estar parados
antes da confirmação. A composição de desenvolvimento acrescenta Recursos à
política local do launcher Linux, pela integração das PR296/308.

Cada solicitação nova recebe um ID e um manifesto privado `mt5x-restore/v1`
no estado compartilhado de Recursos, em `restore_receipts`. A identidade física
da base e o vínculo com sua localização são conferidos no ato. O recibo continua
consultável após mudar `backup_dir` ou abrir um alias da mesma base. Operação pendente com localização
original indisponível ou identidade inconclusiva exige reconciliação dos recibos.

O marco `prepared` é persistido antes da cópia de segurança. O marco `applying`
é confirmado depois da conferência SHA-256 da cópia anterior e antes da troca.
A cópia anterior e os manifestos novos têm permissão privada. Arquivo e entrada
de diretório são sincronizados no POSIX. Windows sincroniza o arquivo e mantém
o aceite físico de persistência após falha abrupta.

Sob o lease liberado pelo produtor, preparo interrompido antes de `applying`
pode ser reconciliado como falha anterior à troca. Um marco `applying` sem
conclusão representa resultado incerto. Nova troca fica bloqueada até consultar
o resultado e solicitar a recuperação correspondente. Leitura parcial, schema
desconhecido, alias inconsistente e checksum divergente preservam os dados.

```bash
rtk ./run.sh --cli resources restore-backup CAMINHO --yes
rtk ./run.sh --cli resources restore-status --operation-id ID
rtk ./run.sh --cli resources restore-backup CAMINHO --operation-id ID
rtk ./run.sh --cli resources restore-backup COPIA_ANTERIOR --recover-operation ID --yes
```

`--operation-id` consulta a intenção existente. Um ID ausente retorna resultado
indisponível. A recuperação é uma intenção nova ligada ao ID incerto e aceita
a cópia anterior vinculada, com SHA-256 revalidado. A aba Recursos oferece
Conferir restauração e solicita confirmação antes do retorno.

A retenção conserva cópias com resultado pendente ou manifesto ilegível.
Recibos concluídos continuam como evidência privada após a retenção das cópias
de dados. Eles pertencem ao estado de recuperação do host. Antes de voltar
a uma prévia antiga, reconciliar as operações pendentes pela fonte que reconhece
esse contrato. A prévia de retorno tem aceite operacional próprio.

Regressões locais cobrem interrupção antes/depois do marco, processo próprio
encerrado após o efeito, consulta sem repetição, troca de diretório, hardlink,
recuperação explícita, manifestos inválidos e retenção. O launcher CLI exerceu
esses caminhos em SQLite sintético. GUI real da revisão final, CI do SHA
publicado, falha de energia e Windows físico têm resultados próprios no handoff.

### Identidade entre sessões e QA da GUI

O vínculo pelo caminho também é consultado quando a identidade física muda.
Substituir o arquivo ativo com uma operação pendente produz resultado desconhecido
e preserva o arquivo novo. Bases independentes continuam com intenções próprias.
Recibos concluídos permanecem consultáveis quando um alias mantém o arquivo
físico e o nome anterior é retirado. Registros v1 publicados são preservados.

Em12/11,31 casos focados passaram em10,38s. A GUI pelo launcher oficial em
XWayland mostrou a guia Cópias e arquivos e o botão Conferir restauração,
solicitou a primeira troca, declarou o resultado incerto da falha injetada e
recuperou o valor anterior99 com nova confirmação. Teve18 pulsos do loop,
intervalo máximo observado0,581s e zero erros inesperados. Tema foi alternado
com preferências de QA isoladas e hashes do proprietário preservados. Pico
270,2MiB, mínimo7,96GiB disponíveis e máximo68°C. Os botões foram invocados
pelo controlador, com a janela real mapeada. Entrada física e falha de energia
conservam seus aceites. O CI deve ser associado à ponta publicada.


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
