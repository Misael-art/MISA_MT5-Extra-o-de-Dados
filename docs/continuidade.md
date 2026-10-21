# T19.6 integrada — 2026-10-19

[PR130](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/130) integrada
em master `c27ef93`, depois [PR131](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/131)
em `4f7a046`. Correção da conexão do snapshot separada da ficha de pivôs. Seis checks
por fonte aprovados; fonte final T19.6:1467Ubuntu/48PG/1569WindowsCI, coberturas
77,64%/73,36%.117 casos locais distintos compatíveis,61 nos caminhos finais,
doctor21/1/0;132 regressões gráficas compatíveis reaproveitadas. Fechamentos e merge
de base somente documentais,294 fontes/testes idênticos ao6a9d580.

Origem, confirmação e instante conhecido separados; candidatos provisórios não são
valores disponíveis. Ausência conhecida interrompe a janela e invalida o prefixo;
revisões da fonte preservam decisões históricas.38 definições em master,32 antigas
preservadas; assinatura7a06cf9749ae1f2cc309464e152862ec9b3a4d1b652eee55a9f8aeb2fa212d6c
confirmada em import frio da base integrada. CLI/contexto/CSV reconciliados em DEMO
arquivado:120M5,720 fatos atuais+720 anteriores preservados;24 Fractals e zero
reversões ZigZag1% na amostra, confirmações ZigZag cobertas por referência determinística.
Versões antigas seguem consultáveis explicitamente; a nova versão padrão exige
materializar as features escolhidas, sem excluir dados anteriores.

Ponte própria conferida ativa em127.0.0.1:18812; sem reinício. Trabalho local, HEAD e
índice preservados; apenas este registro e as linhas do painel foram atualizados.
Windows físico continua pendente específico; Relações continua sintética. Próxima
tarefa concreta elegível: **T19.7 — Donchian, Envelopes e regressão**.

## T19.5 integrada e recorte concluído — 2026-10-19

Ordem real:127/2d7412f →126/eab717d; follow-up129/d0b9c5e →128/eae8f0ac54a7568a42b1955e67aacde3415311b8.
Cada integração teve revisão e checks existentes aprovados. Fonte final128/11b6c53:1438 Ubuntu/
175 skips,43PG,1540Windows CI/73 skips/5avisos, cobertura77,64%/73,36%. Fechamento6f169a5
só docs [skip ci],293 fontes/testes idênticos ao código do CI39303613947 e ao master integrado.
Importação fria comprovou32 definições,8 AO/AC e5 Alligator; assinatura04f94b5b igual à prova.
88 focados/doctor21/1/0, referência racional e CLI/store/CSV120M5/8saídas/960fatos exatos,
reabrir SKIPPED. Fonte DEMO arquivada preservada; sem reextração, ordens ou nova campanha de custo.
129 corrigiu a corrida de fonte/snapshot e serialização de retries em lote em PR própria, com
1417 Ubuntu/41PG/1519Windows CI. Os cinco apontamentos128 foram tratados/documentados.
Versões antigas permanecem consultáveis/exportáveis explicitamente; padrão atual pode exigir
rematerialização por hash global, sem perda dos fatos nem fallback silencioso.
Windows físico permanece pendente específico, Relações continua sintética e nenhum consumidor
operacional H/estratégia/preset foi antecipado. Tempos da T19.4 são amostras locais daquela versão.
Próxima elegível: T19.6 — pivôs causais Fractals/ZigZag, com confirmação temporal, em PR própria.
Root/HEAD/índice/WIP preservados; ponte própria mantida em segundo plano, sem restart.

## Fonte final de128 após129 integrada — 2026-10-19

PR129 integrada emd0b9c5e8f87d34d0993b5b10bcd4755f19452db0 com seis checks1039690:1417
Ubuntu/41PG/1519Windows CI. Fechamento58d3d4a só docs,292 fontes/testes idênticos ao aceite.
128 incorporou129 por Git, preservando conflitos em docs e wrappers PG (AST de ambas as bases).
Runtime final publicado11b6c53,293 fontes/testes idênticos à composição7cfdc81:88 focados,
doctor21/1/0 e prova atualCLI/store/CSV120M5/8features/960fatos exatos, reabrir SKIPPED.
Assinatura04f94b5b; CSVs inicial/final normais têm mesmos doubles/estados, versões distintas
por contrato.43PG finais aprovados; Linux/Windows existentes no run39303613947 em execução.
Cinco threads anteriores tratadas/documentadas: dois fixes AO/AC, dois na base129 e contrato
global de versões explícito/testado. Revisão atual sem pendências; conferir novamente antes
da integração. Não atribuir checkscee301c ao novo runtime. Depois, fechar só docs [skip ci]
com fonte293 idêntica e integrar pelo SHA revisado. Root/WIP/índice/ponte preservados;
sem estratégia/presets, Relações real, ordem, reextração ou benchmark. Windows físico pendente.

## Revisão de128 e follow-up de consistência129 — 2026-10-19

127 e126 permanecem integradas em2d7412f/eab717d. A revisão128 identificou corrida herdada
de fonte: reprodução em master terminou COMPLETE snapshot1 após fonte2, com valor divergente.
Correção em PR129/T19.3, fonte1039690: confirma geração na transação de lote/conclusão/cache,
rollback/retomada e serialização de revisões por backend.135 casos locais distintos compatíveis,
doctor21/1/0 e41 PostgreSQL aprovados; Linux/Windows existentes em execução39296281422.
Probe de barreira SQLite inconclusiva, não é alegada reprodução de chave duplicada.
PR128 rascunho: code publicadocee301c/1428Ubuntu/38PG, Windows existente preservado; correções
locais672b45b de mediano grande/despacho têm83 focados. Não atribuir checkscee às correções.
Depois de integrar129, incorporar master por Git, verificar composição/casos/prova com novo
hash e publicar runtime final em128. Versões globais continuam conservadoras: materializar
a atual ou consultar/exportar antiga explícita, sem perder fatos ou fallback silencioso.
Sem campanha de custo, estratégia/preset, Relações real ou operação de conta. Windows físico
continua pendente específico. Root/WIP/índice e ponte em segundo plano preservados.

## Integrações127→126 concluídas; T19.5 publicada — 2026-10-19

PR127 integrada em2d7412f e PR126 emeab717dd75cfe1452f0a963c02d96ec9caddf033, nessa ordem,
após seis checks e revisão de cada código. Fechamento135c9c2 preservou291 fontes/testes do
CI70b2191:1412 Ubuntu/36PG/1514Windows CI. Registry24 e5 saídas Alligator importados e
verificados iguais a master; assinatura medida3df4baf5 preservada, sem nova campanha.
T19.5/#128 publicada emcee301c, após incorporar master por Git. Três conflitos documentais
resolvidos preservando registros;292 fontes/testes idênticos ao b267d8d aprovado localmente.
78 focados compatíveis, doctor21/1/0 e CLI/store/CSV120M5/8features/960fatos; nova suíte CI
aguardada por mudança de runtime. Sem estratégia/presets/Relações real; referência racional,
aquecimento/ausência, causalidade e reprocessamento conferidos. Root/WIP/índice/ponte preservados;
apenas bridge.py recebeu patch pontual integrado127. Windows físico é pendência específica.

## Integração PR127 e composição final T19.4 — 2026-10-19

PR127 integrada em master2d7412f851b1c9d8090b0372dfacfc6e00188b30 após seis checks existentes
aprovados:1395 Ubuntu/34PG/1497Windows CI,17 focados e doctor21/1/0. Fechamento35a653b
somente documental,290 fontes/testes idênticos ao63e717f. Revisão sem pendências.
Root recebeu apenas patch Git de bridge.py, byte a byte igual ao integrado, sem reiniciar/sinalizar
a ponte própria, ativa em127.0.0.1:18812. HEAD/índice/WIP preservados.
PR126: composição70b2191, master127 incorporado por Git sem conflitos novos;291 fontes/testes
idênticos ao CI atual.1412 Ubuntu/36PG e instaladores/lint aprovados; Windows existente pendente.
34 casos da composição/doctor21/1/0 aprovados; custo/referência/CSV da T19.4 preservados.
T19.5 preparada isoladamente, commit localb267d8d, sem publicação antes de127→126:78 casos
distintos compatíveis, referência racional e CLI/store/CSV de8 features/120M5/960fatos.
Não repetir campanhas aprovadas sem falha/código novo; tempos da T19.4 são amostras locais.
Windows físico e Relações real permanecem pendências específicas; sem estratégia/presets.

## Impedimento T19.4 isolado — PR127/EXTR-02

A PR126 permanece fonte9f2ffc3:62 focados aprovados, referência/custo/CSV preservados.
CI Ubuntu teve1408aprovados/168skips e uma falha no teste legado stop/libc. Corrida de
encerramento foi reproduzida deterministicamente: perder ambiente/token não prova fim da
identidade. Correção isolada na PR127, fonte63e717f sobre master2b27b1f; aguarda identidade,
revalida posse antes de cada sinal e preserva prazos5/2s.17 testes passaram sem enfraquecer
asserções antigas, doctor21/1/0. Só Bridge.stop mudou; Windows nativo/AST main inalterados,
caminho simulado aprovado e evidência compatível da base reaproveitável. Windows físico pendente.
Incorporar PR127 por Git e resolver somente conflitos reais antes de fechar PR126.
Nenhuma materialização/benchmark foi repetida. Root/índice/ponte real seguem preservados.
Artefatos em logs/extr-02-confirmacao-saida-20261019 e logs/t19-4-alligator-20261019.

## T19.4 — PR126 pronta, aguarda CI existente

Fonte9f2ffc331954478ebcd0b21773cab6cb1eeb2251, base master2b27b1f após T19.3/#125.
SMMA com semente SMA, mediano,13/8/5 e desenho8/5/3 separados da disponibilidade.
62 casos focados distintos aprovados, referência racional e CSV/store/cálculo exatos;
19 contratos anteriores intactos, doctor21/1/0,132 casos gráficos compatíveis reaproveitados
(quatro casos da ação CSV executados no código atual). Medição pelo diário, uma amostra/fase:
90 M5 iniciais279ms; append30/prefixo120:216ms; cache2ms; RSS total152–156MiB, sem O(delta).
CLI conferiu120 M5 de600 M1 DEMO arquivadas, sem extração/ordens. Revisão concluída;
quatro checks aprovados, Ubuntu/Windows na execução existente. Não reiniciar workflows.
Artefatos: logs/t19-4-alligator-20261019/current-state.json e materialization-cost.json.
Root HEAD/índice/WIP e ponte preservados; runtime Alligator restrito à worktree até integrar.
Após fechar a ficha, próxima elegível T19.5 (AO/AC), em PR separada. Relações segue sintética,
sem estratégia/preset Alligator; Windows físico permanece pendência delimitada.

## Integração T19.3 concluída — 2026-10-19

PR125 integrada por merge Git em master2b27b1ff25afe260ba1231a4ea8679b42a480e68.
Pré-requisito T18.1/PR124 já integrado em791f15c, registry/DTO confirmados em master.
Código aceito eaec23b, fechamento documental d608817 [skip ci], nenhuma diferença não documental
entre master e a fonte aceita;290 arquivos Python/testes preservados. Seis checks aprovados.
Linux:1528 casos distintos (1392 Ubuntu +132 UI Xvfb +4 UI compatíveis reaproveitados),
cov77,64%; PostgreSQL dedicado34 aprovados; Windows CI1497/61/5, cov73,36%, piso60%.
Doctor21/1/0,27 focados, oito cenários visuais e CLI com600 barras DEMO arquivadas→120M5.
A suíte completa local interrompida não é contabilizada como passe; evidências compatíveis
substituíram a parte redundante e só a cobertura de display ausente foi concluída.
Root HEAD/índice/WIP e ponte própria preservados, sem copiar runtime da worktree.
Registro real: logs/t19-3-consumo-registry-20261019/integration-final.json.

Próxima ficha dependente elegível: T19.4 (Alligator), seguindo preço/SMMA/semente/períodos e
deslocamento causal antes de consumo. T18.2 exige T13.4 validada/integrada; T19.2 aguarda
contratos B correspondentes. Relações permanece sintética até T18.6. Avaliação exact_prefix,
vínculo de manifesto ausente e relógio desconhecido mantêm limites explícitos; presets,
Analytics completo e Windows físico não foram concluídos.

## T19.3 — pronta; somente check Windows existente pendente

PR125: código eaec23b, documentação2e2d3e9 [skip ci], base master791f15c da PR124.
Revisão concluída;1528 casos distintos aprovados (1392 CI Ubuntu +132 UI sob Xvfb +4 UI
compatíveis reaproveitados);34 PostgreSQL no job dedicado, cov77,64%,27 focados, doctor21/1/0.
Processos locais de testes encerrados. A suíte completa local não é contada como passe: foi
interrompida após aceite Ubuntu para evitar duplicação; o teardown de pytest gerou exit1
por SIGINT, sem falha de teste anterior. Cobertura de display ausente concluída com132/0.
Cinco checks aprovados no eaec23b; somente Testes (windows-latest), job118085092630,
segue na execução39260070639. Não iniciar outro workflow. Integrar apenas quando passar,
com revisão/threads e fontes conferidas. Root/índice e ponte própria preservados.
Artefatos em logs/t19-3-consumo-registry-20261019/current-state.json e suite-acceptance.json.
Relações continua sintética até T18.6; Windows físico, presets e Analytics completo pendentes.

## Consumidor T19.3 — candidata em validação, 2026-10-19

PR124 integrada em master791f15c, reutilizando o check Windows existente e os seis checks aprovados.
Registry/DTO confirmados na worktree de master. Próximo consumidor elegível T19.3 está na
[PR125](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/125), eaec23b sobre essa base.
27 testes focados e doctor21/1/0; aplicação CLI reutilizou600 barras DEMO arquivadas→120M5,
sem extração/ordens novas; oito casos UI do novo cabeçalho/ação. Uma falha de contrato antigo
foi reproduzida/corrigida; somente a suíte local anterior foi encerrada e o aceite final está em execução.
Logs e runner próprios em logs/t19-3-consumo-registry-20261019. Revisão registrada; suíte completa
final e CI ainda pendentes. Não integrar antes de passar. Relações segue sintética até T18.6;
presets/Analytics completo e Windows físico permanecem pendentes. Runtime T19.3 restrito à worktree,
sem alterar HEAD/índice ou substituir trabalho local da Root. Ponte própria preservada ativa.

# Continuidade: harmonização de 2026-10-14

## EXTR-02 — ponte principal em segundo plano (2026-10-19)

[PR123](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/123), código43a4035,
validado na worktree de master:1482 aprovados/18 ignorados/3 avisos em1948,46s,
saída0, cobertura77,64%,281 fontes/testes inalterados. Os14 casos finais do
novo ciclo de vida e ShellCheck passaram; doctor na candidata e no Root21/1/0.
PR123 integrada em master60d7d13; seis checks CI aprovados no código43a4035,
fechamento documental75f84d3. Windows físico é pendência específica, sem
afirmação de MT5 real nessa plataforma.

Mantida a entrada scripts/mt5-bridge.sh; helpers em bootstrap.bridge, só stdlib,
reutilizam paths/display. Sessão própria, stdin nulo e log para permanecer após
o iniciador; trava exclusiva, PID/nascimento/boot/usuário/token para posse.
Stop via pidfd só da instância própria, sem pkill, Wine/terminal/display preservados.

Após os aceites Linux, git apply --check confirmou ausência de conflito: aplicado
somente patch Git do lançador e módulo novo no Root. Nenhuma cópia de árvore;
HEAD489591c/índice e outras alterações locais preservados, credenciais/configuração
com hashes iguais. A suíte completa é da candidata, não de toda a árvore Root.
Os dois arquivos aplicados coincidem byte a byte com o código aprovado.

A antiga ponte foreground própria PID60845 teve conta DEMO/terminal conectado e
identidade novamente comprovados antes de encerrar apenas esse processo via pidfd.
Um terminal manteve PID/nascimento. Nova ponte background PID195627 na porta usual
127.0.0.1:18812; outro comando confirmou status e start repetido no mesmo PID.
Aplicação MT5Connector consultou11barras EURUSD/M1,14/10/2026 10:00–10:10UTC;
MetaQuotes-Demo/trade_mode0 comprovado antes/depois, conectado,trade_allowed=false,
zero ordens/replay, sem escrita na base principal. Ponte de teste18813 encerrada
pelo gerenciador; principal preservada. Primeiro roteiro passou datas posicionais
incorretas; preservado/corrigido apenas para argumentos nomeados. O lançador também
recebeu regressão por ser chamado de outro checkout. Não repetida a jornada EXTR-01.

Sem auto-start de boot/login ou política de reinício; comprovação é da permanência
após o comando iniciador. Migração de PID legado sem identidade exige confirmação
da instância original; nunca sinalizar wineserver pelo PID do socket. A preparação
própria de database/ faltante na worktree usou ensure_work_dirs; doctor passou.
Artefatos privados:logs/extr-02-ponte-background/. T18.1 já retomada na PR124 por
cherry-pick revisado80fd887, sobre master60d7d13.90 focados aprovados, doctor21/1/0,
Suíte global1501/26/3 saída0, cobertura77,64%,285 fontes/testes intactos.
PR124 integrada em master791f15c; seis checks CI aprovados, Windows concluiu na execução existente.
Registry/DTO disponíveis na nova base; Analytics/presets e serviço/UI de Relações permanecem pendentes.
T19.3 retomada em worktree própria como consumidor do registry/store. Contrato tipado1.3 preserva definições/valores legados1.2.1; não altera
fórmulas/precisão. Worktree original e WIP principal preservados; código T18.1
permanece na worktree própria, sem aplicação indiscriminada no Root. Sem campanha
estatística ou mudança de UI. Artefatos:logs/t18-1-integracao-20261019/.


## Cadeia #108–#117 e EXTR-01 concluídas em master (2026-10-19)

Master remoto `ed81ca2755b823893f7439beb29489f2bf4861d8`: [fechamento documental PR #122](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/122).
[Evidência final integrada](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/blob/master/docs/extrator/EXTR-01_VALIDACAO_NATIVA.md).
Uma tarefa por PR, merges Git com as bases anteriores integradas primeiro.
Árvore fora de docs/ igual aos HEADs aprovados antes/após cada merge; seis checks
por PR compatíveis reaproveitados. Os 279 hashes da suíte final da #116/#117
conferiram. Nenhuma suíte global, PostgreSQL ou campanha numérica nova. Os dez
workflows dos merges que repetiam runtimes aprovados foram cancelados por SHA;
conclusão cancelled confirmada, sem alegar verde novo no merge.

| PR | Tarefa | Merge em master |
|---|---|---|
| #108 | T20.4 | `2bd7a11` |
| #109 | T20.5 | `1b2cd34` |
| #110 | T20.6 | `aed0f77` |
| #111 | T20.24 | `541c1c3` |
| #112 | T20.25 | `b7c8fd5` |
| #113 | T19.14 | `bd5cb6d` |
| #114 | T13.2 | `2efdabf` |
| #115 | T13.3 | `2ca2fc0` |
| #116 | T13.3b | `814ded9` |
| #117 | EXTR-01 | `a92cd8a` |

No master de runtime `a92cd8a`, sete casos existentes passaram em 14,03 s: falha
persistente sem sucesso falso, recuperação histórica/idempotência, escopo/teto
temporal, CLI/reabertura/exportação e Cancelar Tk. Testes globais da #117:
1468/18/3, saída 0, cobertura 77,64%, preservados. O master documental final é
idêntico fora de docs/ à árvore desta prova e ao runtime aprovado `677dfe3`.

Primeira tentativa nativa foi bloqueada antes da extração por ausência do serviço
127.0.0.1:18812. Configuração já enabled=true, sem Wine/terminal ativo. O start
existente abriu a porta mas não permaneceu após a célula; logs preservados, causa
interna não comprovada. Foreground existente corrigiu o acesso, sem ampliar
permissões, modificar credenciais/configuração ou encerrar processos alheios.
PID python.exe60845 e instante de criação conferidos; controle existente atualizado.
Ponte mt5x-bridge/2, loopback, sem --allow; MetaQuotes-Demo/trade_mode0, conectada,
trade_allowed=false. Zero ordens/replay. Foreground ativo na sessão própria16242.

Jornada EURUSD/M1 14/10/2026 00:00 a16/10 12:00UTC, banco próprio:2880→3600,
720 anexadas; repetição leu2 e conservou valores/contagem. Reabrir/consultar/CSV
exatos ao provedor em OHLCV/volumes/spread/timestamps, sem arredondar/fabricar barras.
Extrair2,889s; atualizar0,737s; repetir0,047s; reabrir0,047s; CSV0,095s; total4,178s
incluindo confronto adicional. Fonte/escrita discriminadas, Analytics/resampling
fora desta jornada. UI Tk real/conector nativo: seleção/período/extração, Cancelar
reconhecido, confirmados preservados, retomada e4736timestamps únicos exportados.
Doctor21OK/1aviso pandas-ta opcional/0falhas, saída0. Windows físico pendente.

Root permanece na branch tarefa/T19.1-inventario-contratos, HEAD489591c e índice
anteriores, sem copiar runtimes ou substituir trabalho local. A prova usou worktree
de master. Próximo recorte operacional: diagnosticar a saída do start em segundo
plano e validar permanência pelo gerenciador, preservando terminal/conta/permissões.
Essa durabilidade não foi comprovada e não impede a jornada já concluída.
Artefatos:logs/integracao-108-117-20261019/; códigos/bancos/CSV/configurações privados
não versionados. Nenhum teste local em execução ao encerrar; ponte útil preservada.



## T20.3 integrada; próxima revisão T20.4 (2026-10-19)

A [PR #107](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/107) foi
integrada em master no commit `9fbd5f5dcc91189f9bc37dd88ee5975165e8d993`, após
T20.2/#106 e bases documentais #118–#121. Revisados os caminhos de patrimônio,
fluxos por cotas, persistência aditiva, leitura legada, re-simulação e SDK, sem
novo impedimento identificado. Sem revisões ou comentários de revisão pendentes.

A conciliação `bcf5064` trouxe a base por merge Git em worktree própria;
o conflito do painel foi resolvido preservando os aceites na linha canônica.
Código, testes e fixtures não foram alterados: árvore inteira fora de docs/
idêntica ao HEAD aprovado `c0f1846` (runtime `91b2ec4`), inclusive após o merge
em master; nove hashes registrados conferidos. Reaproveitados 1.103 aprovados,
nove pulos existentes, três avisos, saída 0, 91 direcionados do complemento,
doctor 16 OK/2 avisos/0 falhas e seis checks aprovados no run 39093597075.
Nenhuma suíte ou campanha numérica repetida. O commit somente documental usou
`[skip ci]`; os verdes reaproveitados pertencem ao SHA de código aprovado.

A prova nativa existente conserva o recorte de 24 marcas EURUSD H1 no Wine,
MetaQuotes-Demo: saldo sem diferença e patrimônio com máximo 1,14e-13 USD.
Não houve nova execução nativa ou envio de ordens. Windows físico permanece
pendência delimitada; a evidência não amplia capacidades para todos os Experts,
moedas, margem ou extremos intrabarra.

A [PR #108](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/108),
T20.4, foi apontada para master somente após a integração da #107, preservando
HEAD `c991b04c85029824e74a0861fb82947ade3d227c`. Próxima tarefa concreta:
revisar seus manifestos/snapshots e integrar, reaproveitando os aceites existentes
se a árvore de código permanecer igual. Sem bloqueio concreto para essa revisão.
Root mantém HEAD `489591c`, branch e índice anteriores; trabalho local preservado.
Nenhum processo de testes ou investigação foi iniciado nesta retomada.
Artefatos de integração: `logs/integracao-107-20261019/`.

## Retomada do extrator com critérios proporcionais 2026-10-18

O proprietário priorizou extração, atualização, persistência, consulta/exportação e
recuperação sobre novas campanhas de precisão analítica. A avaliação e o
[prompt de retomada](propostas/PROMPT_RETOMADA_EXTRATOR.md) identificam a atividade como
T13.3b e preservam suas entregas de estado/checkpoint/invalidação. O benchmark corrigido
tem 25.398 registros iguais; seu tempo exclui aquisição/resampling, sem prova de economia
de memória. A divergência de Bollinger também afetava hashes; corrigida, não justifica
investigação adicional sem falha nova. O prompt anterior contribuía para a dispersão
ao enfatizar qualificação sem um critério de encerramento suficientemente claro.

Última situação lida/informada da PR116: rascunho, commit de referência40fbef5, uma suíte
completa a14% sem falha registrada naquele ponto; aceite ainda pendente do resultado.
Artefatos em logs/t13-3b-integracao. Esta avaliação não reiniciou, cancelou ou alterou
essa execução, sua worktree ou a PR. Código aplicado ao workspace principal continua
distinto de merge: HEAD489591c e mudanças locais preexistentes foram preservados.

EXTR-01 foi registrada como próxima prioridade no painel/roadmap: jornada curta real
do extrator, medir etapas, corrigir a primeira lacuna comprovada e concluir sem refatorar
por conveniência. Falha/recuperação determinísticas e UI/CLI coerentes; base de teste
gravável isolada, sem novas cópias/hashes integrais a cada comando. Precisão de origem,
contratos, causalidade e segurança continuam obrigatórias; repetição exige gatilho concreto.
O prompt consolidado aponta à nova prioridade; F19/F20 e planos futuros não foram removidos.

Verificação desta avaliação: 29 testes existentes do núcleo de extração/atualização/
retomada/banco passaram em39,94s, saída0; SDK docs3 antes/3 depois (1,36s/0,91s), saída0;
doctor16OK/2avisos/0falhas, saída0 (pandas-ta opcional ausente e ponte desativada).
Links locais da documentação revista e cercas de código conferidos; git diff --check
limpo. Logs próprios em logs/avaliacao-retomada-extrator-20261018. Sem execução de nova
suíte integral, acesso nativo MT5, ordens, benchmark ou alteração de runtime/testes por
esta avaliação. O teste focado não substitui aceite global da candidata nem prova
extração do servidor real. Preparação e evidência integral da sessão anterior não são
atribuídas a esta entrega. Nenhum .sh alterado; ShellCheck não aplicável.

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

## T20.4 — revisões de dados (2026-10-18)

Serviço canônico aditivo de blocos/manifestos/referências, captura em uma imagem
de leitura, replay Python/livro e cache com revisão fixada. Export/import verifica
hashes sem substituir barras do destino. [Contrato e limites](analytics/T20.4_MANIFESTOS_DADOS.md).
[PR #108](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/108) em revisão sobre #107. PostgreSQL 16 isolado: sete testes aprovados; suíte completa Linux: 1114 aprovados, 9 pulos existentes e 3 avisos em 1009,24 s, saída 0. Doctor 16/2/0; 28 direcionados aprovados. Próxima T20.5.
T20.3 recebeu correção do leitor de fluxos/descendentes no PR #107: suíte final
1103 aprovados/9 pulos existentes/3 avisos em 1009,28 s, doctor 16/2/0.
Windows físico pendente; nenhuma alegação de integração ou roadmap concluído.

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

## Prompt único — F19/F20/UI e validação MT5 consolidados (2026-10-17)

Pedido do proprietário e anexo lidos; conteúdo reunido no
[prompt único de desenvolvimento mt5x/HADOUKEN](propostas/PROMPT_CONSOLIDADO_DESENVOLVIMENTO_MT5.md).
Um bloco copiável, com oito seções e contexto independente do histórico do chat: leitura,
estado real/ordem, pilares, presets/nomes/lotes/parciais, cálculos/dados/pesquisa Q01–Q16,
UI atual/posterior/experimental, validação e entrega. Planos continuam especificação.

Retificada a regra antiga do anexo que exigia execução ao vivo DEMO em todo caso:
replay nativo MT5/Strategy Tester visual em ambiente DEMO pode concluir o aceite
funcional histórico coberto/reconciliado, inclusive fora de pregão. Rede/latência/fill
ao vivo mantêm ensaio DEMO específico e pendência delimitada. Mocks/Python complementam
a evidência nativa; QA histórico não é OOS/forward/fill ao vivo. Nunca conta REAL,
sem aumentar limites/afrouxar guardas ou interferir em operações/sessões de terceiros.

PROMPT_F19_EXECUCAO.md/PROMPT_F20_EXECUCAO.md mantêm os caminhos como referências de
compatibilidade e agora apontam ao prompt único, sem blocos concorrentes. Seus conteúdos
anteriores foram preservados em logs/prompt-consolidado-20261017/anteriores, fora do Git;
anexo original não foi alterado. Roadmap, painel, índices, planos, matriz e documento UI
apontam à entrada única. T20.0 recebe adendo documental; preservadas 57 fichas F19,
30 F20 e prioridades UI, sem novas tarefas/implementação/execução de ordens.

Verificação própria: SDK docs **3 aprovados antes (0,81 s) e 3 depois (0,85 s)**,
saída 0; doctor **16 OK/2 avisos/0 falhas**, saída 0 (pandas-ta opcional ausente e ponte
desligada). Um bloco completo/oito seções, leituras locais e requisitos centrais,
redirecionamentos sem recursão, links/campos/IDs e dependências sem ciclos conferidos;
relatório em logs/prompt-consolidado-20261017, fora do versionamento. git diff --check
limpo. Runtime/testes/dependências sem diff, nenhum .sh alterado; ambiente preparado e
suíte integral anterior da mesma sessão não repetidos nem atribuídos a esta consolidação.
Sem implementação, ensaio MT5/replay/DEMO, validação visual/Windows físico ou medição de
performance: a entrega ajusta as instruções para o agente executor, não as executa.

## UI — prioridades atuais, refinamentos e experimento separados (2026-10-17)

Pedido do proprietário registrado em [organização e gráficos por prioridade](propostas/UI_ORGANIZACAO_GRAFICOS_PRIORIDADES.md)
e vinculado a roadmap/painel, F17/F19/Q14-F20, matriz de validação e prompts F19/F20.
Adendo documental T20.0; nenhuma tela, renderer, cálculo ou dependência implementada.

UI-A01–A10 são requisitos atuais por capacidade: modos Analisar/Compor/Validar e comparar,
contexto compacto, editor recolhível por grupos, hierarquia/legibilidade, catálogo de
gráficos com dados reais, seleção/inspetor, tabelas/insights, rascunho/resultado e
preservação de contexto, adaptação/acessibilidade/performance e tela de referência.
Donos permanecem T19.30–34/T20.22/23; as 57 fichas F19 e 30 F20 foram preservadas.
Sensibilidade/presets/relações entram com seus serviços, sem bloquear o primeiro Manual.

UI-P01/P02 (densidade alternável e perfis completos de disposição) ficam POSTERIOR/P2.
UI-X01 (candidato Lightweight Charts) fica EXPERIMENTO/P3, não iniciado, fora do caminho
crítico: exige futura prova comparável e decisão arquitetural antes de adoção. Continuam
Tkinter/ttk/Matplotlib; não instalar nova biblioteca nem migrar toolkit no PR da UI atual.
T20.22 define o padrão de navegação/layout; T20.23 liga a inspeção temporal e depende da
primeira, sem dependência inversa. Uma tarefa por PR, sem reabrir a F17 histórica.

Verificação própria: SDK docs **3 aprovados antes (0,81 s) e 3 depois (0,86 s)**,
saída 0; doctor **16 OK/2 avisos/0 falhas**, saída 0 (pandas-ta opcional ausente e ponte
desligada). Campos/IDs, 10 requisitos atuais/2 posteriores/1 experimento, links locais
novos, cercas/whitespace e dependências F19/F20 sem ciclos conferidos; relatório em
logs/ui-prioridades-20261017, fora do versionamento. git diff --check limpo; runtime,
testes e dependências sem diff. Ambiente preparado e suíte integral anterior da mesma
sessão não repetidos nem atribuídos ao adendo. Nenhum .sh alterado; ShellCheck não
aplicável. Sem execução/validação visual de UI, benchmark, MT5/replay/DEMO ou Windows
físico nesta entrega documental; provas futuras permanecem nos respectivos donos.

## F20 — dezesseis sugestões de robustez incorporadas (2026-10-17)

Proprietário aceitou todas as sugestões da avaliação. T20.0 registra
[Q01–Q16 e marcos](propostas/ROBUSTEZ_PESQUISA_EXECUCAO.md),
[trinta fichas T20.1–T20.30](propostas/PLANO_F20_ROBUSTEZ.md) e
[prompt de execução F19/F20](propostas/PROMPT_F20_EXECUCAO.md), com painel e roadmap.
Somente documentação entregue; nenhuma capacidade nova implementada ou validada em conta.

Cobertura: referências matemáticas/patrimônio flutuante, dados/specs reproduzíveis,
qualidade/capacidades, margem/custos/capacidade por volume, paridade por evento,
tentativas/reserva/protocolo, compromissos/ablação, pacote/estados/invalidação,
observação/diagnóstico, falhas/retomada, carteira posterior, UI investigativa,
orçamento/cache/isolamento, conformidade/terceiros/restore/release.
As 57 fichas F19 e catálogo integral são preservados, sem novas categorias de preset,
stores/ledgers/executores concorrentes ou antecipação de FRP/IA avançada.

Retomada: T19.1 se pendente; T20.1 estende o mesmo inventário e define dono único para
cada extensão, sem refazer base já entregue. T20.12/13 integram o protocolo T19.27/28/52;
T20.3 explicita risco patrimonial para Defensivo. Primeiro aceite Manual T20.30 pode
anteceder o piloto ampliado T19.35 e não o conclui. Mercado fechado continua permitindo
replay nativo com cobertura; QA não é OOS/forward/fill DEMO ao vivo. Gestão da carteira
é posterior sem adiar travas de múltiplas ordens; nenhum ensaio REAL autorizado.

I-04/I-09/I-11 foram harmonizados documentalmente: não exigir ordenação universal de
drawdowns Monte Carlo nem zero falsos positivos em toda realização de ruído. Registrar
hipóteses, calibração/intervalo/poder e versão de critérios. Bateria/código não mudaram;
implementação futura segue I-13, regressões e guardas, sem relaxar testes para passar.

Verificação própria deste complemento: SDK docs **3 aprovados antes (0,71 s) e
3 depois (0,85 s)**, saída 0; doctor **16 OK/2 avisos/0 falhas**, saída 0
(pandas-ta opcional ausente e ponte desligada). Conferidas fichas/campos Q01–Q16,
57 tarefas F19 e 30 F20, dependências sem ciclos e links locais novos; relatório
documental em logs/f20-documentacao-20261017, fora do versionamento. git diff --check
limpo. Ambiente instalado e suíte integral anterior da mesma sessão não repetidos
nem atribuídos a esta edição: runtime, testes e dependências continuam sem diff.
Nenhum .sh alterado; ShellCheck não aplicável. Sem ensaio MT5/replay/DEMO, Windows
físico, medição de performance ou certificação financeira nesta entrega documental.

## F19 — gestão de lotes e parciais estruturada (2026-10-17)

Requisito adicional explicitado na especificação §6.3, catálogo, UI/schema/preset, prompt
e matriz de validação. T19.56/57 ampliam o plano para 57 fichas: lote fixo/por risco,
volume maior que um em uma entrada, adições distintas e saídas parciais simples/múltiplas
por quantidade/% com base definida, etapas/ack, saldo protegido e reconciliação.

Leitura de código confirmou SymbolSpec.lots_for_risk e ExitRules.partial_r/partial_fraction;
re-simulador executa a parcial existente somente sem grid. Não declarar suporte geral a
parciais múltiplas/gradiente por essa base simples existir. A implementação futura amplia
esses módulos e preserva o legado, sem duplicar sizing/engine ou adiar gestão local para a conta.

Contas conhecidas e backtests validam volume/risco/custos/métricas; replay nativo/DEMO observa
volume maior que um, parcelas, saldo e proteção. Não tratar cada parcela como novo trade/PF.
Somente documentação alterada nesta entrega. SDK docs: 3 aprovados antes (0,75 s) e 3 depois
(0,82 s), saída 0; doctor 16 OK/2 avisos/0 falhas, saída 0; 57 fichas completas,
145 dependências sem ciclos, 65 links locais novos válidos e git diff --check limpo.
Runtime/testes/dependências sem diff; ambiente da mesma sessão e suíte integral anterior
não repetida/atribuída a este adendo. Nenhum ensaio MT5/replay/conta, Windows físico ou
validação financeira foi executado nesta entrega documental; nenhum .sh mudou.

## F19 — Default, Manual nomeado e prompt/replay retificados (2026-10-17)

Default da estratégia é o primeiro preset de referência, versionado, sem otimização;
reusar seleção de padrões existente, preservar escolha explícita e criar Manual por cópia.
Usuário informa Nome do preset no Manual: texto/slug/ID e nome canônico têm papéis separados,
com prévia, detecção de duplicatas e preservação no round-trip. Maximalista é o rótulo de
apresentação do perfil FatorLucro, sem objetivo/categoria duplicados. Especificação §§8.4.1/8.4.2;
T19.26/32/50 ampliadas e T19.55 adicionada. São 55 fichas planejadas, FRP segue futuro.

[Prompt completo do executor](propostas/PROMPT_F19_EXECUCAO.md) agora salvo no projeto e
[matriz de validação](propostas/VALIDACAO_F19.md) retificam a exigência de operação ao vivo:
estrutura/cálculos por testes/backtests apropriados; gradientes/stops e demais comportamentos
operacionais observados no MT5 nativo, em replay visual ou DEMO ao vivo disponível. Agente
pode escolher recorte curto com aquecimento/eventos suficientes para QA; não constitui OOS.
Mercado fechado/ativo indisponível online não bloqueia replay com fonte válida; limitações
específicas permanecem delimitadas. Nenhuma validação de conta REAL autorizada.

Atualização somente documental, sem ensaio MT5/conta ou implementação de presets.
Verificações próprias: SDK docs 3 aprovados antes (0,74 s) e 3 depois (0,79 s), saída 0;
doctor 16 OK/2 avisos/0 falhas, saída 0; 55 fichas completas, 138 dependências sem ciclos,
65 links locais novos válidos e git diff --check limpo. Runtime/testes/dependências sem diff.
Ambiente preparado na mesma sessão; suíte integral anterior não foi repetida nem atribuída
a este complemento. Resultados abaixo permanecem históricos; nenhum ensaio operacional
ou Windows físico foi executado por esta entrega documental. Nenhum .sh alterado.

## FRP — registro de pesquisa futura (2026-10-17)

Ideia do proprietário registrada em [FRP — Fluxo Relativo do Preço](pesquisa/FRP_FLUXO_RELATIVO_PRECO.md)
e ligada a roadmap, painel principal, F18, especificação/catálogo/plano F19 e índice de propostas.
FRP-00 é somente registro; FRP-01–04 têm estado ESTUDO FUTURO/P3, após consolidar a base.
Grupos de 3/4 indicadores correlacionados entre si e com movimento do preço; explorar
limiares 0,60/0,70/0,80/0,90 e concordância ascendente, com possível relação ao Expert.
Fórmula/normalização/horizonte ainda serão concebidos; estudar redundância, alvos maduros,
contribuição incremental e validação OOS. Coeficientes não são percentuais de segurança.
Não iniciar pesquisa aplicada/integração agora nem tornar FRP dependência dos presets/Ultra;
as 54 fichas F19 permanecem. Nenhum código, schema, dado, Expert ou preset alterado.
Verificações próprias: SDK docs 3 aprovados antes (0,81 s) e 3 depois (0,75 s), saída 0;
doctor 16 OK/2 avisos/0 falhas, saída 0; 44 links locais novos do conjunto documental
conferidos, 54 fichas F19 preservadas e git diff --check limpo. A suíte integral 1058/9
e verificações dos adendos abaixo permanecem evidências históricas distintas, sem nova
execução integral atribuída ao FRP. Runtime/testes/dependências sem diff; ambiente da mesma sessão.

## Adendo F19 / T19.0 — nomes, Ultra e inversão (2026-10-17)

Pedido adicional incorporado à [especificação](propostas/PILARES_DADOS_OPERACAO_PRESETS.md)
§§6.2/8.4/8.5, [catálogo](propostas/CATALOGO_F19_INDICADORES_SETUPS.md),
[plano](propostas/PLANO_F19_PILARES_PRESETS.md), roadmap, índice e painel principal.
Agora são quatro categorias: Manual, Defensivo, Fator de lucro e Ultra.
T19.50–54 ampliam para 54 fichas planejadas, sem renumerar as anteriores.

Nomes têm robô/ativo/série/categoria/orientação/revisão/hash, rótulo legível, ID independente,
normalização portável, proteção contra colisão e preservação dos legados. Ultra é política
condicional por calendário/contexto causal, com prioridade, fallback, rastro e backtest
completo da política congelada; não é costura retrospectiva de vencedores. A flag experimental
de inversão troca compra↔venda antes do plano, recalcula níveis/risco e fica desligada por padrão;
não reverte posição aberta. Entrada Fibonacci também foi explicitada.

Adendo somente documental. A suíte integral 1058/9 abaixo é a linha de base anterior,
não foi repetida/atribuída a este adendo. Runtime/testes/dependências seguem sem diff;
ambiente instalado na mesma sessão. Focados SDK antes da edição: 3 aprovados em 0,77 s.
Focados finais SDK: **3 aprovados em 0,73 s**, saída 0; doctor **16 OK/2 avisos/0 falhas**,
saída 0. Verificados 54 fichas completas, 134 dependências sem ciclos e 32 links locais novos;
git diff --check limpo. Nenhum arquivo de runtime, teste ou dependência foi alterado.
Somente documentação atualizada; Windows físico/MT5 e resultados financeiros não foram
validados por este adendo. Nenhum .sh mudou, portanto ShellCheck não se aplica.

## F19 / T19.0 — planejamento dos pilares e geração de presets (2026-10-17)

Pedido do proprietário estruturado em [especificação](propostas/PILARES_DADOS_OPERACAO_PRESETS.md),
[catálogo completo](propostas/CATALOGO_F19_INDICADORES_SETUPS.md) e
[plano executável](propostas/PLANO_F19_PILARES_PRESETS.md). Somente documentação nesta entrega;
T19.1–T19.49 planejadas, nenhuma capacidade nova declarada implementada.

Dois ramos de dados (mercado e operações); composição de entrada, ganho, stop e filtros;
tempo de trade descritivo e executável; presets manual/defensivo/PF; integração com contratos
HADOUKEN; UI operacional organizada, gráficos e responsividade. O catálogo cobre I01–I18,
TM01–TM08, AX01–AX09 e S01–S11, mantendo a lacuna dos supostos 37/48 setups não enumerados.
Gestão da conta tem planejamento em segundo momento, sem dispensar travas multiordem.

Base inspecionada: 489591c, merge PR105. Timeframe já está no seletor “Ativo / TF” da T17.28;
T19.33 verifica coerência/descoberta, sem duplicar a entrega. Store T13.2, registry T18.1 e
contratos B/Composer não foram presumidos disponíveis; T19.1 reconcilia donos/estado e é a
próxima tarefa desta frente. Primeiro piloto: S09/T19.37 com preset manual e conferência nativa.

Ambiente Linux preparado com instalador --yes --no-mt5 e requirements-dev antes das edições.
Baseline integral antes dos documentos: **1052 aprovados, 9 ignorados existentes, 6 falhas,
3 avisos em 858,39 s**, saída 1. Falhas preexistentes nesta execução: reflow 900px da janela
principal, largura 820px de Relações e quatro diálogos de Robôs com erro Tk “window not viewable”.
Não houve mudança de código/teste para corrigi-las ou ocultá-las. As seis passaram em
Xvfb (6 aprovados, 1 aviso, 16,75 s). A primeira repetição integral em Xvfb esgotou o
tmpfs de runtime de 1,5 GB: 86 falhas, 527 aprovados, 8 ignorados e 101 erros em 228,40 s,
saída 1/Errno 28; não é validação do código. Os três diretórios pytest criados por esta
sessão foram preservados em disco, liberando o runtime, sem apagar artefatos de terceiros.
A repetição integral em Xvfb com TMPDIR exclusivo em disco terminou com **1058 aprovados,
9 ignorados existentes, 3 avisos em 875,34 s**, saída 0. Nenhum teste foi modificado,
desativado ou pulado nesta entrega. Os avisos são de tight_layout em gráficos existentes.
Não somar os testes focados à suíte nem confundir validação do software com campanha financeira.

Doctor final: **16 OK, 2 avisos, 0 falhas**, saída 0 (pandas-ta opcional ausente e ponte
desativada). Conferidos 29 links locais novos, 49 fichas completas, 111 dependências internas
explícitas sem ciclos e a cobertura integral do catálogo; git diff --check passou.
A checagem ampla também encontrou referência histórica ausente de T13.2, registrada na
especificação sem inventar o arquivo. Logs e artefatos próprios estão em logs/f19-validation-20261017,
fora do versionamento. Nenhum arquivo de runtime/teste teve diff.

Fontes dos estudos lidas integralmente e identificadas por SHA-256 no catálogo; originais
preservados. Nenhum script, código, banco, Expert ou preset foi editado nesta tarefa.
Nenhuma conta foi operada; Windows físico não executado. ShellCheck não se aplica sem .sh alterado.

## Estado recente das demais frentes

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


## Retomada do extrator — T13.3b e EXTR-01 (2026-10-18)

Prioridade atual do proprietário: propostas/PROMPT_RETOMADA_EXTRATOR.md, lido
integralmente. A execução existente da PR116 foi aproveitada sem duplicar suíte
local: 1461/18/3 em 2154,08 s, saída 0, cobertura 77,44%, fontes intactos.
Hashes vinculados ao runtime 40fbef5; seis checks de execução aprovados, incluindo
Windows. PR116 pronta para revisão, sem merge. A investigação numérica encerrou;
benchmark exato de 25398 registros permanece válido. A tentativa real extra
interrompida continua inconclusiva, preservada e não repetida.

EXTR-01/PR117: runtime 677dfe3 recupera intervalos failed anteriores ao último
timestamp antes da cauda incremental e conserva falha quando a fonte não volta.
A prova do diálogo Tk revelou Cancelar inoperante por comparação de estado Tcl;
correção limitada a uma linha e regressão mantida. Banco/extrator/UI são os únicos
módulos de produção alterados. Baseline reutilizado da PR116 somente após
comprovar fontes, Python e pacotes iguais no worktree isolado antes da edição.

Jornada FakeMT5/M1 em banco gravável próprio: falha intermediária preserva 1080
barras, retomada recupera 540 e cauda, termina com 1801 timestamps únicos e CSV
exatamente igual ao provedor em OHLCV/volumes/spread. Atualização correta 0,910 s,
repetição idempotente 0,050 s, total medido 2,418 s; sem Analytics/resampling
neste percurso e sem alegar ganho sobre o antes incompleto. CLI, reabertura/CSV
e resultados/cancelamento do diálogo Tk cobertos. Focados 34, novas provas
específicas e três cenários Tk aprovados; doctor 16 OK/2 avisos/0 falhas.
Suíte final única 1468/18/3 em 2237,29 s, saída 0, cobertura 77,64%, fontes
intactos. Seis checks do código 6096ff8 aprovados, incluindo Windows. PR117 pronta
para revisão; implementação encerrada, sem merge.

Prova nativa delimitada: ponte desativada e conexão recusada na porta local
configurada; terminal, conta e credenciais não alterados. Fakes não certificam
feed real; Windows físico segue pendente. Código das PR116/117 permanece nas
worktrees logs/t13-3b-integracao/pr e logs/extr-01/pr, sem aplicação ao workspace
principal e sem merge. Logs/checkpoints próprios preservados. Relatório EXTR-01:
PR117, docs/extrator/EXTR-01_CICLO_RETOMADA.md. Próximo passo: revisão das PRs; prova DEMO nativa quando houver ponte disponível,
sem iniciar outra campanha numérica ou novas frentes F19/F20 agora.


## EXTR-01 — jornada DEMO nativa aprovada (2026-10-19)

Ponte antes desativada e sem listener foi corrigida com o servidor restrito
existente em 127.0.0.1:18812 e somente enabled=true na configuração efetiva.
Wine/Python/terminal já instalados; nenhum processo/sessão de terceiros foi
encerrado. PID próprio registrado no controle existente. Conta MetaQuotes-Demo,
trade_mode=0, conexão e negociação automática desabilitada comprovadas. Sem
credenciais em argumentos/relatórios, sem ordens e sem replay. Doctor 21 OK,
um aviso opcional, zero falhas, saída 0. Base principal preservada.

Código das PR116/117 sem mudanças; seus aceites automatizados foram reutilizados.
Revisão técnica local dos contratos/lotes/transações/UTC/retomada/cancelamento
sem achado bloqueante nos caminhos percorridos; nenhuma revisão externa
registrada. master489591c contido na candidata. Preparação Git em
codex/integracao-extrator-116-117, com a cadeia aberta #106→#117 preservada;
evidência d5916ee avançada por fast-forward para a PR117. Nenhum merge remoto
nem cópia de fontes para o workspace principal; HEAD/índice mantidos.

Jornada nativa EURUSD/M1: 14/10 a 16/10/2026 12h UTC, 2880→3600 barras,
720 novas, repetição sem duplicação ou mudança de valores, banco fechado e
reaberto e CSV exato ao provedor em relógio/OHLCV/volumes/spread. Extrair
3,628 s, atualizar 0,944 s, repetir 0,066 s, consultar 0,060 s e CSV 0,115 s.
Sem Analytics/resampling ou campanha de benchmark. UI Tk real/conector nativo:
seleção/período, extração, atualização, cancelamento reconhecido, blocos
confirmados preservados, retomada e CSV (4652 timestamps únicos). A primeira
observação do arquivo assíncrono foi inconclusiva; roteiro corrigido para
aguardar on_success, única repetição justificada dessa checagem, log mantido.

Prova/limites: PR117, docs/extrator/EXTR-01_VALIDACAO_NATIVA.md; artefatos
privados logs/retomada-nativa-20261019/. Próxima tarefa concreta: revisão e
integração da base #106/T20.2, seguindo #107…#117 na ordem aplicável. Windows
físico continua pendente e nenhuma nova pesquisa numérica foi iniciada.


## Integração da base T20.2 — 2026-10-19

A revisão da PR106 confirmou seis checks aprovados, incluindo Windows, e o
runtime fc9e593/head1ad3755 inalterado (hashes e diff conferidos). Seu rascunho
dependia de documentação preexistente ainda local. Essa base foi publicada e
integrada separadamente, sem transportar o diff inteiro do workspace: PR118
T19.0, PR119 T20.0, PR120 T19.1 e PR121 T20.1. Conteúdo do proprietário e patches
originais preservados; 57 fichas F19,30 F20,16 Qs e links conferidos. O patch
T19.1 não aplicava em dois contextos; conciliação em três vias e preservação
das linhas novas/relatos foram registradas. T20.1 conciliou sem conflitos.

Quatro jobs CI redundantes disparados para essas mudanças exclusivamente
documentais foram cancelados, com aceites anteriores mantidos; merges docs
identificados skip ci para não reiniciar suítes de código inalterado. Não houve
remoção de teste, tolerância ampliada, novo oráculo, benchmark ou campanha.
Na PR106, somente continuidade conflitou; ambos os relatos foram preservados
e o painel T20.2 unificado. Código/testes/dependências/scripts continuam
exatamente os aprovados:1081/9/3 em1179,37s,saída0,seis checks.

PR106 retirada do rascunho e integrada em master pelo merge
f477e4351c9e0060a047cb3dade996010843ceee. Fonte não documental de master
conferida igual a1ad3755. Evidências privadas logs/integracao-base-106-20261019/.
Root permanece em489591c, com alterações locais/índice preservados e sem copiar
fontes ou trocar seu branch. Nenhuma aprovação implícita de B-11 ou liberação
REAL. A prova DEMO do extrator da PR117 não certifica paridade financeira deste
motor. Windows físico e limites de swap/moeda/margem permanecem pendentes.
Próxima tarefa concreta: revisar/conciliar PR107 T20.3 sobre master com #106
integrada e seguir as dependentes na ordem.
