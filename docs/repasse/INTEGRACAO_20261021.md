# Repasse da integração e limpeza segura — 21/10/2026

> Convenções: `<APP_ROOT>` é a raiz do checkout da aplicação no host; `<H_ROOT>` é a raiz HADOUKEN; `<WORKTREES_APP/H/SAFETY>` são as raízes de worktrees dos três repositórios. `<RELEASES_ROOT>` é a pasta de releases apontada pelo lançador local. Localize-as pelo Git e pelos links da instalação, sem presumir um caminho pessoal. **D** é `<APP_ROOT>/logs/integracao-pm12-20261020/`. Menções a arquivos em D são referências locais, não links para artefatos publicados.

O [índice na raiz](../../RETOMADA_AGENTE_20261021.md), o [plano](../PLANO_DE_TRABALHO.md), a [continuidade](../continuidade.md) e a [evidência resumida](../evidencias/REPASSE_LIMPEZA_20261021.json) são versionados. Os scripts de operação, logs brutos, imagens privadas, bancos, arquivos compactados e backups permanecem locais. Não executar a limpeza novamente; o manifesto local registra os caminhos já aposentados.

## Estado de parada e leitura inicial

O proprietário pediu **registrar tudo e limpar os temporários com segurança**. O desenvolvimento e as integrações funcionais foram interrompidos para cumprir esse pedido; a instrução seguinte autorizou commit, push e PR deste repasse documental. Nada das PRs #172–177 foi integrado ou instalado nesta parada. Não executar os scripts de retomada automaticamente.

Esta versão publicável registra o repasse local e as evidências versionadas de cada tarefa. Os registros operacionais privados permanecem no host e não fazem parte deste commit. As revisões de manutenção recebidas estão refletidas abaixo; autoria dos respectivos cores foi explicitada, sem alegação de revisão independente.

Diretórios:

| Conteúdo | Local |
|---|---|
| Aplicação e dados do proprietário | `<APP_ROOT>` |
| HADOUKEN principal antigo, preservado | `<H_ROOT>` |
| Worktrees da aplicação | `<WORKTREES_APP>/` |
| Worktrees HADOUKEN | `<WORKTREES_H>/` |
| Worktrees Safety | `<WORKTREES_SAFETY>/` |
| **D: recibos, scripts, logs, PNGs e este repasse** | `<APP_ROOT>/logs/integracao-pm12-20261020/` |
| Impedimento nativo anterior | `<APP_ROOT>/logs/t8-7-live-demo-20261020/working-state.json` |
| Instalações imutáveis usadas pelo lançador | `<RELEASES_ROOT>/` |

Leia `AGENTS.md`, `<CODEX_HOME>/RTK.md`, os planos/continuidade **da versão correta** e este repasse. Todos os comandos de terminal começam com `rtk`; comandos sem filtro usam `rtk proxy`. O proprietário autorizou evidência proporcional: não reinstalar ambiente nem repetir suíte integral/jornada nativa por rotina. Não contornar os checks obrigatórios.

## Última conferência e resultado da limpeza

**Carimbo: 2026-10-21T15:54:46.619898+00:00.** CI consultado em 2026-10-21T15:52:09.971536+00:00: PRs **172/173/174/175: seis checks aprovados cada**; **176: cinco aprovados e Windows em execução**; **177: cinco aprovados e Windows falhou**. Nenhuma delas foi integrada/instalada. Falha177 guardada em `catalog-4ab-windows-failure.txt/.json`: timeout60s em `test_analytics_profiles::test_late_correction_forces_full_recompute_and_revises_dependent_rvol`,2% da suíte, stack em revisão/hash da origem analítica, antes de UI. Três UI isolados passaram60,90s. `analytics/producers.py` e esse teste têm diff vazio de ac7 até4ab. **Causa não comprovada; não corrigi, não fiz teste adicional nem solicitei rerun nesta parada.** Ler o log antes da decisão seguinte.

Limpeza concluída: **10 bancos privados compactados e verificados**, **19 cópias nativas privadas aposentadas com evidência arquivada**, **76 diretórios de caches gerados removidos**. O espaço disponível observado no volume passou de **22.69GB para 26.08GB**, aumento de aproximadamente **3.39GB**. A diferença lógica de blocos dos caminhos é maior, mas não equivale a espaço físico em Btrfs com extents compartilhados/compactados; não anunciá-la como bytes físicos recuperados. O delta do volume é uma fotografia sujeita a atividade concorrente do host.

Verificação pós-limpeza: nenhuma mudança inesperada nos WIPs originais, configurações/preferências, links de instalação e seis worktrees publicadas. Única nova entrada local da raiz é o índice `RETOMADA_AGENTE_20261021.md`. Backups do proprietário preservados. `native-active` ausente e lock adquirido/liberado sem espera no snapshot; **sem reserva mantida**. Conferências completas em `cleanup-protected-before.json`, `cleanup-protected-after.json`, `cleanup-summary-20261021.json` e `handoff-native-resource-snapshot.json`.

## O que está realmente integrado e instalado

| Componente | Versão ativa / referência |
|---|---|
| Aplicação, PR #171 | `ac7af2c392611bb1bd262c7806810eb4289559b3` |
| HADOUKEN | `ec550d77caae74e9f5f4ffe30cea5ccd29621624` |
| Safety 1.8.0 | `3988df13c467bd22941183f5546caad0500f9138` |
| Aplicação ativa | `~/.local/share/mt5x/app-current -> releases/app-ac7af2c39261` |
| Fontes H ativas | `~/.local/share/mt5x/hadouken-current -> releases/hadouken-ec550d77caae` |

Instalação por arquivo Git do merge revisado, manifesto de arquivos/hashes, links controlados para dados e configurações mutáveis da raiz, possibilidade de retorno. Foram conferidos 840 arquivos da aplicação, 415 hashes HADOUKEN, seis pins nativos e 15 headers Safety mais Canvas padrão. O atalho usa `app-current/run.sh`. Compilar/exportar não demonstrou carregamento do Expert no Tester.

Preservados: `.venv`, `database`, `logs`, `exports`, `config/config.ini`, símbolos padrão, `.env` quando existente, preferências e favoritos. O link ausente das preferências na instalação antiga foi corrigido reversivelmente; não se inventou arquivo de favoritos ausente. Preferências canônicas: `config/ui_prefs.json`, hash conferido nos registros locais; seu conteúdo não é publicado.

Principal da aplicação: branch antiga `tarefa/T19.1-inventario-contratos`, HEAD `489591c0940c93c5e3766e7fa2febefad96b1717`, 124 entradas de WIP preexistentes no levantamento anterior. Principal H: branch antiga `codex/t12-3-orcamento-global`, HEAD `480d3104`, `ESTUDOS` não rastreado preservado. Checkout Safety antigo com WIP também preservado. **Esses HEADs não são master nem a versão instalada.** O índice novo na raiz é a única adição de continuidade desta parada; não sobrescrevi os documentos WIP.

## Entregas concluídas no ciclo de integração

Os recibos `*-merge-receipt.json`, `visual-merge-receipts.json`, manifestos e provas em D identificam os hashes completos e checks de cada integração. As evidências da própria tarefa ficam em `docs/evidencias/`, `docs/sdk/` e `Documentation/HADOUKEN_2/` nas árvores revisadas.

1. **Pilha visual H #32/#36/#37/#38/#39** integrada serialmente: host/gates, compacto/abas, temas/marcas, Candle Time e degradê. Fundamentos sintéticos visuais não equivalem a aceite operacional. O status de consumidor sem capacidade manual foi corrigido: bloqueio/pausa/entrada prevalecem, depois atividade/fallback, sem falso “Só VENDA”. Fontes do consumidor antigo não substituíram executor/risco atuais.
2. **Move Guard único, H #40**: composição e endurecimento numérico de `c0c73c96`, sem gestor paralelo. Recusa NaN/Inf/EMPTY; compra só aumenta SL, venda só diminui; usa `SafeExecutor.ModifySL`, tick/stops/freeze e TP preservado. Shims reais cobrem predicado, bloco ATR→guard e métodos do executor. Bool/LastRetcode não são recibo de SL aplicado; concorrência/servidor pendentes.
3. **Safety 1.8.0 e consumidor App #164**: 15 headers com closure local; Canvas externo exato. Risco/executor/contratos atuais preservados. Instalador Python confere todos os fontes/hashes antes do primeiro I/O no destino e copia os bytes já lidos. Rejeição de pacote mantém destino; erros de escrita depois do início não têm rollback transacional. Recibos `safety-merge-receipt.json`, `app-merge-receipt.json`, `safety-smoke-proof.json`.
4. **S09/Pullback e painel, H #44 / App #165**: mesmas instâncias de risco/executor; `ManagementTick` continua durante pausa, `GuardTick` bloqueia entrada. Temporal/parcial mantêm ordem e reconsulta de posição. S09 1.90, fp `b069f6b52c76c77d`, 44 inputs; PB 1.10, fp `e1b14494047477b2`, 23 inputs. UI usa capacidades do perfil validado antes do editor, sem allowlist de versão permissiva.
5. **S10 Estocástico/EMA9, H #30/#45 / App #166**: produtor valida HLC, agregações e prefixo; warmup corrigido para `max(300,k+slowing+d-1)`. K razão de somas, D SMA, EMA C0. Python core `d4b084de`; consumidor corretivo `b4bdf24f`, merge App `d6bb5571b63c4f6838b198326c2dc48577d9f571`. H observado 1.30, fp `26545f544c784c39`, 15 inputs. TP sinal-close com 0=off; overrides explícitos usam fill/R. Stop/guard compartilhados. Leitura estrita de **todas** ocorrências SIGNAL/CONTEXT/BAR e count incremental anterior/anterior+1. `T16.6` recusa genericamente projeção de MoveGuard sem série, sem allowlist S10 e sem fingir re-simulação completa.
6. **VWAP Bands, H #41/#46 / App #167**: H rejeita HLC/volume inválido e overflow até próxima data; âncora inicial parcial indisponível; um CopyRates compara todos os horários/OHLC e transporta volumes reais/ticks do mesmo snapshot. Core Python `48feaf8b`, consumidor `6981fd65`, merge `9579a62f703c3c503f921f332f898f2147944744`. H 1.20, fp `7139d746ab6b2c1d`, 11 inputs. Target band 0 significa VWAP central **com TP**, não TPoff. Auto H seleciona uma série por toda janela; Python usa prefixo causal e não é equivalência automática à janela móvel. Auto no serviço Python continua recusado se real_volume sózeros tem elegibilidade unknown; source=1 explícito foi usado e registrado. Sem waiver por outro resolver.
7. **Madrid, H #42/#47 / App #168**: variante declarada, 10 EMAs de progressão aritmética, seed C0; ATR14 seed TR0/RMA1/14; warmup 300 fixo. Reset por gatilho/barra admite igualdade anterior e não garante FC/FV persistente/restart. TP fixo, não móvel. Core Python `111ae036`; consumidor `43234c6a`, merge `c5ba3346403f059f512b596be8895c12043b9d3a`. H 1.20, fp `870c0f8db71c3d5a`, 13 inputs. Point mode usa M1 como referência com SymbolSpec; profit override não remove filtro de distância. MoveGuard compartilhado; stop fixo substitui só inicial. Cache genérico pode ser eligible por identidade completa; isso não qualifica reuso operacional.
8. **HA/HiLo, H #43/#48 / App #169**: estado inicial 0 e rompimento estrito de SMA anterior, SMA ativa atualiza sem novo rompimento. Difere do indicador gráfico na inicialização/igualdade; janela1500 pode perder virada anterior. HA/HiLo finitos/positivos, prefixo dependente inválido indefinido. SL fixo na linha de entrada ±buffer; sem TP/MoveGuard. Saída→reconsulta→tentativa oposta pode terminar flat se risco/proteção/posição remanescente impedirem entrada. Core Python `515a3b90`, consumidor `469dc798`, merge `1612301ff693e4c4998e16b71f45e7fb8858ddac`. H 1.20, fp `5dad3bbabab0343f`, 10 inputs. Leitor recompõe cada janela e preserva intenção de saída mesmo com entrada filtrada; não prova fechamento causal do broker.
9. **T20.12 inicial, App #170**: core `97c3bafd` e composição/correção `833de994`, merge `86ae848e6bf6326ca9920f7c1020c5a0f98fe39f`. Pacote canônico `experiments/{schema,store,service,commands}.py` + CLI. Protocolo imutável congela critério/objetivo versionados, grade/seed, roles/snapshots/evaluation_start/trade_from e orçamento **por evaluation_request**, não campanha. BASE compound=True inicialmente, risco/configuração efetiva explícitos. Reusa Budget.begin/admit/JobBudget e identidade/cache existentes. RUN+bind+job+journal `run_recorded` na conexão/transação existente; importação do livro é posterior e não qualificada como atômica. Acesso durável antes de cálculo/cache/entrega; tuning/exposição sticky entre protocolos por origem/símbolo/TF/intervalo sobreposto. `qualified_final_holdout=False` sempre. Retentativa mantém run_id reservado, cache alheio só antes da primeira admissão. Morte real antes/depois do commit testada. Correção marshal normalizado versão0 estabiliza identidade entre processos.
10. **T19.24 composição no registro, App #171**: head `d7020ace`, merge/instalação `ac7af2c3`. Caminho `composition_v1` interno reutiliza mesmo BaseAdapter/Backtester/store/control/importer; composição/hash/compound e configuração econômica entram na identidade e validação pré-publicação. Wrapper público SDK preserva os 15 argumentos anteriores. Caminho `base_v1` mantém restrições. Todos os seis checks finais foram aprovados nas integrações #166–171.

Contagens relevantes, com sobreposição explícita: #170 Linux 2601/216 skipped, Windows 2733/84 skipped, PG54; #171 Linux2619/218 skipped, Windows2752/85 skipped, PG55. Núcleos novos têm referência Decimal e shims reais, não equivalência externa/financeira. HA core80/seleção432+3GUI e consumidor220; Madrid core129/seleção257+3GUI; S10 e VWAP possuem seleções e regressões detalhadas em suas evidências. Não somar seleções sobrepostas como testes independentes.

## Percursos realmente feitos na instalação do proprietário — não repetir

Foram utilizados os fluxos reais da aplicação, preservando Default e nomeando/versionando Manual. Histórico negativo e recusas continuam visíveis; não houve promoção financeira ou operação REAL.

| Perfil | Manual / parâmetros úteis | Histórico Python registrado |
|---|---|---|
| S09/PB | revisões2, risco0,25%; S09 cooldown3600; capacidades anteriores preservadas | Exportação/readback44/23 inputs; não é recibo do Tester |
| S10 | `identificador local (ver recibo)`, rev2, offset60, target0, risco0,25% | run `identificador local (ver recibo)`:528barras,5trades,−819,29 |
| VWAP | `identificador local (ver recibo)`, rev2, entry1,5/target0/stop150/source1, risco0,25% | run `identificador local (ver recibo)`:2112barras,0trades,0 |
| Madrid | Manual rev2, first7/step6/ATR1,5/points100/trail8/offset0,3, risco0,25% |528barras,10trades,105,10; spec histórica desconhecida |
| HA/HiLo | Manual rev2, period12/offset0/buffer15, risco0,25% |528barras,24trades,−382,223 |

Provas `owner-*-ui-proof.json`, `owner-*-python-history-proof.json`, `owner-*-history-ui-proof.json` e PNGs correspondentes. Exportações estáticas15/11/13/10 inputs conferidas; **não** foi comprovado binário/preset carregado no Tester dessas estratégias.

Registro #170 real: protocolo `identificador local (ver recibo)`, request `identificador local (ver recibo)`, run `identificador local (ver recibo)`:408barras/4trades/154,89. Reabertura idempotente e novo pedido com cache mantiveram a mesma observação e dois acessos auditados.

Composição #171 real: protocolo **`identificador local (ver recibo)`**, request **`identificador local (ver recibo)`**, run **`identificador local (ver recibo)`**:408barras/**3trades/111,08749999999556**. Madrid composition_v1, risco0,0025/capital inicial, compound=False, SL100points/TP1,5R/temporal3600/quota1. Livro numérico reconciliado nesse percurso, flags de qualificação preservadas false. Reabertura/cache auditados sem nova observação; capturas reais inspecionadas. `owner-composed-cli-proof.json`, `owner-composed-history-ui-proof.json`, comentário PR171 `6314581395`. **Não executar novamente o driver once-only.**

Dados já conferidos: inventário real12ativos/15séries/194855barras; CSV EURUSD/H1 12405barras com todas as colunas em `exports/owner_EURUSD_H1_20261021.csv`; backup consistente existente preservado. `owner-local-data-export-proof.json`. Ainda não houve atualização do provedor no banco do proprietário neste recorte; essa jornada futura é separada.

## Fila publicada e ainda não integrada

**Ordem obrigatória:172→173→174→175→176→177.** Cada filho contém o pai por Git. Preservar a ponta ao retarget para master depois do merge do pai; não trocar fontes por cópia de diretório. Todos os seis diretórios abaixo estavam limpos e suas pontas remotas conferidas. O snapshot de CI é temporal: consultar de novo antes de qualquer merge.

| PR | Branch / diretório sob `_worktrees/mt5x/` | Ponta completa | Base publicada | CI |
|---|---|---|---|---|
| [172](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/172) | `codex/t19-34-contexto-responsivo` / `t19-34-contexto-responsivo` | `bfc422a523386ecd6b9e8448b963e638151fb552` | master/ac7 |39612225486, tentativa2 |
| [173](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/173) | `codex/t15-1-recursos-varredura` / `t15-1-recursos-varredura` | `4d3f560660b7b5e8a5394145172942f1d0899cf5` |172 |39612793239 |
| [174](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/174) | `codex/t2-5-conexao-preservada` / `t2-5-conexao-preservada` | `9e070335ef1d72e0e4681513972fe63092cceeae` |173 |39613378749 |
| [175](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/175) | `codex/t20-12-consulta-ui` / `t20-12-consulta-ui` | `e174cc789b614f7403c82eb439b829ff008929a5` |174 |39613966019 |
| [176](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/176) | `codex/t19-34-navegacao-adaptavel` / `t19-34-navegacao-adaptavel` | `e7656daba8a3156b26b7cf8983484c20b9c9bcea` |175 |39620811804, tentativa2 |
| [177](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/177) | `codex/t19-34-cabecalho-catalogo` / `t19-34-cabecalho-catalogo` | `4ab87025e1e829021cbe4b6bb2e380ee7b67ee85` |176 |39623750354 |

Checks e URLs exatos em `queue-current-checks.json`; carimbo do snapshot em `current-handoff.json`. Revisões técnicas #172–175:5600870466/5600916940/5600974857/5601022887. #176/177 têm proposta concreta/testes/provas e guardas preparadas para revisão final de integração, sem simular independência do autor.

### #172 — contexto e página de robôs responsivos

RobotDetail quebra texto na largura realmente alocada−4. RobotsPanel usa o ScrollableContent já existente para filtros/resumo/corpo quando altura/fontes deixam o corpo sem mapa. Mantém os scrolls internos. Testes revelam controles por coordenadas lógicas e exigem mapa/visibilidade/interseção/largura/texto/foco; asserts não foram afrouxados. Pipeline Windows acrescenta só um processo cedo com três testes críticos, mantendo a suíte integral/coverage60% original e seis checks.

3dirigidos44,70s+4adjacentes28,36s+1restauraçãoDPI9,99s sobrepostos. Histórico privado `48fede...`, 3trades/111,0875 reaberto sem cálculo;24bounds, dois temas/quatro tamanhos e fonte1,5/144DPI, PNGs inspecionados. `t19-34-page-context-ui-proof.json`, evidência versionada `T19_34_VIEWPORT_PAGINA_20261021.json`. Não qualifica toda T19.34/Windows físico.

### #173 — recursos sem varredura interminável

Só `resources/storage.py`, `resources/files.py`, `ui/resources.py`. Prazo cooperativo monotônico1s para leituras opcionais; desconhecido permanece None/incompleto, lista velha incompleta descartada, limpeza desabilitada inclusive no callback. Relatório DB/backups continua útil. I/O individual/SQL/backup não são hard deadline. Não cria scheduler nem muda retenção/backup.

Baseline45;4regressores falharam antes;49pass59,05s+24retenção12,65s+5finais11,81s sobrepostos; composição4UI51,11s. Windows final2760passed/85skipped/5warnings3522,11s, todos seis checks aprovados no snapshot. Prova privada de455,6MB: backup→marcador controlado→restauração+cópia de segurança/auditoria→CSV12405todascolunas/readback, **completa e não repetir**. Primeira90s interrompida preservada, retomada300s concluiu. Banco do proprietário apenas RO, sem restauração. `private-recovery-ui-proof.json`, `private-recovery-resources-readback-proof.json`, PNGs e scripts preservados; cópias grandes compactadas no encerramento.

### #174 — reutilizar conexão existente sem resetar API compartilhada

Único runtime `mt5_connector.py`. Preflight metadata sob lease valida caminho/data_path/conta/login/server/trade_mode e reutiliza mesma sessão saudável sem initialize/login/shutdown/prepare. Ownership local e borrowed sticky; identidade trocada/ausente na sessão emprestada recusa fallback. Caminho novo não verificado mantém legado; force_restart explícito continua separado. Coleção auto_login=False preserva autenticação anterior.

46próprios4,04s+52interação34,33s+27ponte40,29s sobrepostos; composição48interação31,70s. QA real somente leitura DEMO6276 proibiu mutadores SDK; mesmo principal/ponte. Banco privado64KiB: extração/atualização real25→49barras EURUSD/H1 de12–14out, backup/safety/restoreaudit/CSVtodascolunas, App candidato online12270símbolos/49barras; desconexão local preservou backend. Windows2806passed/85skipped/5warnings3322,60s e seis checks aprovados. `connection-private-data-proof.json`, `connection-private-ui-proof.json`, scripts `connection_existing_readonly_preflight.py`, `connection_private_data_journey.py`, `connection_private_ui_readback.py`.

### #175 — consulta canônica dos estudos pela aplicação

Store.list_protocols metadataSELECT, novo `ui/experiments.py` e entrada Exibir→Registro de estudos. Mesma store.status, sem avaliação/acesso de reserva novo. Pedidos/Dadosereserva/Protocolo/Eventos deixam claro run_recorded/reused/rejected/discarded/requested e exposição desconhecida/sticky/qualificação false.

6próprios21,31s;30seleção44,70s/1PG local semURL, composição10UI67,44s sobrepostos; CI PG56 e seis checks aprovados. App candidato real usou só cópia das cinco tabelas de journal:2protocolos/4pedidos/2execuções/17eventos, mesmo bf4d, SHA/data_version/config/prefs intactos, PNGs inspecionados. Primeiro wrapper Xvfb cleanup1 depois da provaPython completa preservado; readback estreito posterior exit0. `experiments-metadata-candidate-proof.json`, `experiments_metadata_candidate_readback.py`.

### #176 — navegação adaptável e mensagens de status completas

Somente dois runtimes UI: componentes aditivos AreaNavigation/AdaptiveStatusBar e ligação UIManager. Mesmas páginas Notebook/modelos/seleção/menu. Nomes completos em combobox readonly quando abas não cabem, restaura abas largas/foco. Status em duas linhas/ellipsis por pixels quando precisa; texto integral/modelo/log preservados e detalhes readonly selecionáveis, fonte do tema, scrollbar funcional, Space/CtrlEnd/Escape. Cancela idle pendente ao destruir.

Baseline3pass20,05s;2regressores falharam antes; final10pass131,60s(7novos+3contexto/página),6entorno52,37s sobrepostos; doctor20OK/1pandas-ta opcional/0falhas. Falha inicial de assert ttk layout corrigiu expectativa do teste (`null` não significa Notebook.label), preservando exigência de nomes/foco/visibilidade. Inspeção visual detectou/fixou fonte pequena e scrollbar sem espaço no detalhe.16runtimes protegidos e prefixo anterior components preservados.

Histórico privado48fede sem cálculo,24bounds, temas/tamanhos/DPI. `t19-34-navigation-context-ui-proof.json`, `t19-34-navigation-status-readable-proof.json`, PNGs finais e capturas anteriores preservados. SDK `navegacao_adaptavel.md`, evidência `T19_34_NAVEGACAO_STATUS_20261021.json` com7hashes. Instalação/aceite físico ainda pendentes.

### #177 — resumo e erro do catálogo legíveis

Único runtime RobotsPanel: Label resumo/erro preenche X, alinhado à esquerda e wrap atual−4;9adições/2remoções. Filtros/busca/ações/seleção intactos: regressão reproduziu1178px pedidos para823disponíveis; busca já passou e não foi mudada. Final11pass159,10s(5próprios+3contexto/página+3entorno); dois temas/96e144DPI/fontes1e1,5/quatro tamanhos+repetições e erroQA identificado.18runtimes protegidos iguais ao176, sem bootstrap/dependências novos; doctor compatível17620/1/0.

Mesmo histórico privado sem cálculo, resumo782px dentro823, duas capturas normais e uma erroQA inspecionadas. `catalog-header-ui-proof.json`, `catalog-header-final-focused.log`, `catalog-header-900-light.png`, dark e controlled-error. SDK `cabecalho_catalogo.md`, evidência `T19_34_RESUMO_CATALOGO_20261021.json` com6hashes. Não qualifica cards/gráficos/todaT19.34.

## Falhas de CI preservadas e limites de reaproveitamento

172bfc tentativa1 Windows119263768039: timeout60s em SQLite `dbapi_connection.commit` no teste inalterado `test_indicator_materialization::test_guard_and_cancel_preserve_confirmations`,33%, antes de UI. Três UI isolados passaram45,06s; alvo local inalterado passou16,84s. **Causa do runner não comprovada.** Uma única reexecução seletiva de job falho no mesmo SHA foi solicitada e confirmada (tentativa2, Windows119287571631); não houve aumento de timeout/alteraçãoSQL/asserts. Arquivos `responsive-bfc-windows-failure.*`, `responsive-bfc-selective-rerun.json`, `responsive-bfc-attempt2-status.json`, comentário6318844888.

176e765 tentativa1 Windows119293370235: timeout60s SQLitecommit em `test_causal_pivots::test_pivot_origin_confirmation_and_context_boundary`, antesUI; três UI isolados passaram49,91s, alvo local inalterado1pass6,18s. Depois de workflow completo, uma única reexecução seletiva foi confirmada (tentativa2, Windows119307524563), outros cinco checks aprovados preservados. `navigation-e765-windows-failure.*`, `navigation-e765-selective-rerun.json`, `navigation-e765-attempt2-status.json`, comentário6319691704. Não insistir com reexecuções cegas; se a segunda falhar, ler a causa real.

Falhas Windows mais antigas1728056/665/1b0 de mapping/altura/coords foram preservadas. Não substituí-las por sucesso nem afirmar causa exata remota sem prova. O bfc reproduziu problema com fonte1,5/144DPI local e corrigiu a página real. Aprovação completa173/174/175 não dispensa seis checks exatos172/176/177.

## Como retomar a integração serial

1. Atualize o snapshot remoto com `poll_queue_ci.js` pelo **Composio CLI local autenticado**; `queue-current-checks.json` contém URLs dos jobs. Não disparar workflow manual ou duplicado. Ferramentas SaaS via CLI preferidas; escrita incerta exige GET de reconciliação, nunca repetir por outro conector.
2. Para172, exigir open/master/ponta bfc, master igual ao recibo171, seis checks success e discussão impeditiva resolvida. Revisão de autoria própria explicitada; rótulo consultivo de risco não inventa nova exigência de permissão humana. Não mesclar só cinco checks.
3. Scripts `merge_responsive_consumer.js`, `merge_resource_consumer.js`, `merge_connection_consumer.js`, `merge_experiment_ui_consumer.js`, `merge_navigation_consumer.js`, `merge_catalog_header_consumer.js` estão **preparados, não executados**. Revalidam ponta/base/master anterior/6checks/discussões. Cada um escreve seu recibo. Ler antes de executar; guardas não substituem julgamento nem estado remoto atualizado.
4. Depois de cada merge, buscar master e conferir tree do merge igual à tree revisada. Só então retarget do próximo filho para master sem mudar ponta; conferir checks continuam na ponta correta. Manter históricos, sem force push.
5. `prepare_{responsive,resource,connection,experiment_ui,navigation,catalog_header}_release.py` usa arquivo Git do merge/recibo, hashes e links mutáveis. `activate_*_release.py` exige instalação anterior exata, pins/closure/Safety/hashes, config/prefs/atalho backups, allow_real=False, mesmo principal/ponte/contaDEMO e exposições conhecidas sob lease, rollback atômico do link. **Não rodados para172–177.**
6. Usar os readbacks estreitos abaixo; não refazer cálculos/QA privado compatível. As cópias privadas grandes foram compactadas; se um helper exigir caminho antigo, primeiro consultar manifesto da limpeza e restaurar só a cópia privada necessária. Nunca substituir pelo DB do proprietário nem disparar novamente campanha só para recriar arquivo.

Scripts de percurso do proprietário preparados, AST/sintaxe conferidos, **não executados**:

| Após instalar | Script / finalidade |
|---|---|
|172 | `capture_owner_composed_history.py --layout`: mesmo bf4d em leitura, página/scroll/fonte, autocalcular desligado |
|173 | `capture_owner_resources.py`: leitura de relatório/backups; sem restore/limpeza do proprietário |
|174 | `owner_data_provider_journey.py`: **uma vez**, novo EURUSD/M30 previamente ausente, backup com prune=False, SHA de todas tabelas antigas+journal, mutadores initialize/login/shutdown proibidos, extraçãoUI12–13out e Atualizaratéagora, CSVtodascolunas; leases/guardas existentes. Barra corrente provisória. Não executado. Se prova parcial existir, inspecionar etapa e retomar, não executar novamente cegamente |
|174 | `capture_owner_data_reopened.py`: reabrir offline e comparar mesmoCSV/linhas/hash |
|175 | `capture_owner_experiment_registry.py`: journalRootRO/data_version/SHA/config/prefs, menu real, mesmos27edf/bf4d |
|176 | `capture_owner_navigation.py`: offline, mesmo histórico/journal/config/prefs, fonte máxima/navegação/status integral/CtrlEnd |
|177 | `capture_owner_catalog_header.py`: offline, mesmo histórico, resumo largura/texto; sem injetar erro falso no proprietário |

Não manter lease na thread principal enquanto worker de dados precisa adquiri-lo. Scripts de dados do proprietário são mutações controladas e têm guardas próprias/backup; não confundir com simples captura. Restauração do banco do proprietário **não foi feita nem está implícita**.

## Estado nativo e impedimentos reais

Sem reserva/lease/contexto nativo mantido ao encerrar. Uma fotografia de disponibilidade não reserva o recurso. Usar `tester_lease` e o registro de contexto existentes no próximo uso; não criar locks paralelos. Na última identidade observada, ponte e principal build6276/DEMO estavam preservados. Não reiniciei ponte/principal e não sinalizei processos de terceiros. Identidades de processos são fotografias locais, não garantias futuras; comparar PID+creation time/boot/token conforme scripts. A última verificação de exposição foi0posições/0ordens no QA correspondente, **não é afirmação de saldo/exposição corrente**.

Perfil privado autorizado emUS2 não comprovou sincronização90s; hipótese DNS falhou. Houve descoberta de um perfil sincronizado, porém Tester não produziu livro; correção de formato de data do driver e tentativas delimitadas visual/direct/warm posteriores não fecharam a sincronização. **Causa final continua não comprovada.** Não repetir sondagem90s sem hipótese nova e pré-condição observável. A API do principal conectada só qualificou leitura/dados, não Tester privado nem Expert carregado.

Harness final `Tests/HDK_PanelHost_Acceptance.mq5` SHA-256 **`8eaac095d4dc50d936e286547a0e6e9948b830e7b695f50efe8b1c1541b200ca`**: Tester+DEMO+visual antesinit; pendenteSL via RiskGuard/Executor em fase separada/max1 contando pendentes; ExitConfirmed exige histórico magic/símbolo/volumes/motivo efetivo. Pausa precede baselineSL/minimização na mesma barra. Trailing exige observar posição viva com SL maior que baseline pós-pausa; saída pelo SL original é evidência protetora separada e reprova trailing estrito. Finish pausa/reconcilia exposição/objetos e OnTester reprova incompleto/falhas. Antigo26OK/3FAIL não visual permanece **não aceito**.

Pendentes: passagem visual desse harness; Expert de produção com host; S09/PB Tester visual e recibo real de inputs/binário; novos quatroExperts/replay/livro/fechamentos causais; aspectos de broker/stops/freeze/assíncrono/restart/múltiplas instâncias/concorrência/netting/hedging; Windows físico/DPI/multigráfico. Cliques injetados OnChartEvent e QA sintético não provam interação física. Nenhuma operação REAL autorizada. `allow_real=False` permanece. Histórico/replay não habilita vivo automaticamente.

T8EMAATR EURUSD/H1 conservador legado: histórico negativo de−250,92 preservado. Falta replay exato compatível antes de revalidar contexto/carteira/conta/proteções e fazer o aceite DEMO ao vivo já abrangido pelo pedido original. Não afirmar conclusão operacional enquanto esses gates faltarem.

## Trabalho futuro que ainda não foi iniciado/concluído

- T20.12 **parcial**: ligar consumidores restantes, eventual validação por candidato/fold e acessos, qualificação de livro/reserva/estatística. UI175 só consulta metadados. Orçamento atual por pedido não é teto agregado de campanha; countjobs/run_id não mede alternativas estatísticas.
- T20.13 calibração/qualificação: reserva final congelada, diário de acesso, maturidade/purga/horizontes, dependência/blocos/ciclos/poder/DSR/PBO/múltiplas tentativas ainda pendentes. Dados já explorados continuam exposição externa unknown; nunca ganham intocado retroativo por novo protocolo.
- T19.27/28 e Defensivo/Fator de Lucro/Ultra dependem desses gates e dos seus itens do plano. Manual não precisa esperar seletores. Não confundir geradorMQL explícito, parser `presets/protocol.py` ou save_validation UPSERT com diário canônico de tentativas.
- `strategies/validation.py::robustness` aplica±20% indiscriminadamente: Madrid trail_line10→12 é inválido por limite legítimo. Observação estática registrada; **nenhuma implementação/branch de correção T20.13 iniciada**. Tratar enums/limites no protocolo, sem afrouxar estratégia nem forçar trades.
- Carteira/gradientes/múltiplas entradas precisam orçamento agregado, ownership, reservas, margem, reconciliação/recuperação e concorrência qualificados; não liberados pelas entregas atuais.
- UI172/176/177 resolve recortes concretos; cards/gráficos/acessibilidade ampla/organização integral continuam conforme plano. PM01/cestas/Finalizar/editorLoteSLTP não receberam efeitos inventados.
- Paridade Profit/iStochastic/indicador gráfico e estabilidade de seeds/janela móvel1500/restart não qualificadas. Python frame fixo difere do nativo por janela. Propostas de guard/SL/TP não provam ModifySL aplicado.
- FRP/estudos futuros permanecem registrados; não viraram bloqueio artificial nem entrega implementada.

## Limpeza segura e preservação de evidências

Consulte **`cleanup-manifest-20261021.json`** para resultado, bytes e hashes finais. A limpeza tem lista explícita dos caminhos gerados nesta integração, verificação de processos/descritores/cwd/mapas, estabilidade dos arquivos e posse do lease existente ao retirar cópias nativas. Não fez varredura destrutiva genérica.

- Seis bancos históricos privados S10/VWAP/Madrid/HA/experimentos/composição e quatro cópias privadas do ensaio de recuperação foram compactados individualmente com gzip. SHA-256 do conteúdo descompactado foi conferido antes de remover o original. Permanecem recuperáveis, sem novo cálculo, em **`retained-qa-archives-20261021/`**. São clones, não o banco real nem os backups de rollback do proprietário.
- Cópias nativas explicitamente identificadas nas provas foram retiradas depois de arquivar/verificar fontes, headers, bináriosQA/ex5, configs, logs e relatórios. Foram dispensados executáveis copiados da plataforma, caches de provedor `Bases` e `temp`; hashes/inventários retidos mostram o que existia. Uma sessão futura deve criar contexto novo pelas APIs existentes, não supor sessão antiga pronta.
- Cópia `safety-smoke-native` descartável recebeu o mesmo tratamento. Bytecode local do diretório de scripts foi removido. Scripts/provas/PNGs/falhas/recibos não foram apagados.
- **Preservados todos os `owner-*-backup-*`, configurações, `.env`, dados/exportações reais, versões instaladas/rollback, `.venv`, worktrees/branches publicadas e WIP antigo.** Sessões/caches compartilhados sem autoria comprovada não foram removidos. Não esvaziar `/tmp`, Wine principal, cache global ou worktrees de outros agentes.

Para acessar um DB privado compactado: obter caminho/hash do manifesto; descompactar para **novo caminho privado**, conferir SHA-256 com `sha256_uncompressed`; abrir em RO se apenas consulta. Não restaurar sobre `database/mt5_data.db`. Os arquivos tar nativos devem ser inspecionados antes de qualquer extração; não executar seu launch.bat automaticamente. Nenhum arquivo aposentado deve ser citado como ainda presente no caminho original.

`cleanup-protected-before.json` e a verificação pós-limpeza registram preservação do WIP/config/prefs/links e limpeza das seis worktrees publicadas. O índice versionado `RETOMADA_AGENTE_20261021.md` aponta para este repasse; no checkout antigo do host, o índice local também aponta para os recibos ignorados. Os arquivos locais em logs permanecem ignorados pelo Git e não devem ser forçados para um PR.

## Índices finais de conferência

- `evidence-index-20261021.json`: catálogo pesquisável dos recibos, scripts, provas, capturas e hashes preservados.
- `handoff-final-verification.json`: conferência final dos29arquivos compactados, caminhos retirados, links locais e preservação dos WIPs/configurações/seis pontas publicadas.
- `worktrees-preservadas-app.txt` e `worktrees-preservadas-hadouken.txt`: inventário Git para localizar trabalho; não é lista de material descartável.
- `cleanup-summary-20261021.json`: resultado e interpretação correta do espaço disponível no Btrfs.

Esta publicação não executa merge, instalação, reexecução177, teste financeiro, mutaçãoSDK ou processo nativo. A PR documental terá somente o CI automático do repositório, sem disparo manual duplicado. A documentação completa de operação permanece local/ignorada em logs. Esta síntese publicável e seu índice versionado permitem localizar o repasse sem publicar aqueles arquivos.
