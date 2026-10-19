# EXTR-01 — ciclo M1 e retomada de blocos falhos

## Recorte e defeito comprovado

Prioridade do proprietário: extrair, atualizar, persistir, consultar/exportar e
recuperar dados, conforme [diretriz](../propostas/PROMPT_RETOMADA_EXTRATOR.md).
Uma jornada pequena usa WIN$N/M1, 02–05/01/2024 e o FakeMT5 existente. Origem
sintética explicitamente identificada; não comprova acesso ao servidor real.
Bancos de teste graváveis separados; nenhuma escrita na base principal.
Analytics, resampling e indicadores opcionais não integram esta jornada.

A primeira extração falha no bloco intermediário de 03/01, preservando 1080
barras confirmadas nos dias 02 e 04. Antes da correção, atualizar até 05/01 às
12h retornava sucesso, mas conservava o bloco failed e suas 540 barras ausentes.
O último timestamp não identifica lacunas anteriores. A regressão foi
reproduzida nos dois cenários: fonte recuperada e fonte ainda indisponível.

## Correção e donos

O dono continua HistoricalExtractor; DatabaseManager usa _extraction_log já
existente. failed_blocks consulta apenas a tabela solicitada e os intervalos
falhos encerrados até o limite da atualização, em ordem determinística.
A mesma coordenação de extração retoma esses intervalos exatos e depois busca
a cauda incremental com a sobreposição de uma barra já existente. Não se
reextraem integralmente blocos confirmados. Falha persistente mantém resultado
de falha; cancelamento permanece cooperativo. Não há store, fila, extrator,
backup manager, tabela, dependência ou configuração nova.

A prova do diálogo revelou que o estado ttk retornado como objeto Tcl não
acionava Cancelar na comparação direta. A correção limitada a uma linha converte
o estado em texto antes da comparação. O teste que revelou a falha permanece,
com os cenários de sucesso/falha/cancelamento no diálogo real sob Xvfb.

Arquivos de produção: database_manager.py, historical_extractor.py e uma linha
em ui_manager.py. Testes: test_incremental_update.py, test_cli.py e
 test_ui_main_window.py. Outros módulos de produto permanecem fora do recorte.

## Jornada e fidelidade

A jornada CLI usa extract --symbols 'WIN$N' --tf M1 --from 2024-01-02
--to 2024-01-03, update --symbols 'WIN$N' --tf M1, repetição de update,
reabertura do banco e export --table win_n_1_minuto --out <destino.csv>,
com configuração e provedor falsos isolados pelo fixture já existente.
Enquanto a fonte continua falhando, extract/update retornam EXIT_FAIL e
informam a falha; após recuperação, retornam EXIT_OK. A repetição mantém
1261 timestamps únicos no recorte CLI (02–04/01 às 12h).

Na jornada medida de três blocos mais cauda (02–05/01 às 12h), a recuperação
terminou com 1801 timestamps únicos e zero blocos failed. O CSV, lido com
float_precision=round_trip, coincide exatamente com o provedor em time,
open/high/low/close, tick_volume, real_volume e spread. Não se arredondou dado
nem se sintetizou candle para cobrir ausência. O bloco confirmado permanece
igual após a falha e a retomada. O banco foi fechado e reaberto antes da consulta.

No diálogo real: selecionar ativo, M1 e período; Iniciar Extração ou Atualizar
até agora; acompanhar progresso; Cancelar; conferir resumo e inventário.
As três apresentações usam o callback público real. A execução do núcleo e a
exportação são comprovadas pela jornada CLI; os testes Tk usam um coordenador
controlado, sem afirmar conexão MT5 real ou interação humana. A restauração de
backup não foi necessária ao destino de teste novo; não se criou outra estrutura.

## Medidas locais — uma execução por estado

| Etapa | Antes | Depois |
|---|---:|---:|
| Extração com falha controlada | 2,149 s | 1,336 s |
| Atualização após recuperação da fonte | 0,506 s, sem recuperar a lacuna | 0,910 s, com recuperação |
| Repetição idempotente | não medida | 0,050 s |
| Reabrir e consultar | não medida | 0,035 s |
| Exportar CSV | não medida | 0,056 s |

O antes incorreto não executava o mesmo trabalho: retornava 1261 barras em vez
de 1801. Portanto não há alegação de ganho de velocidade. Não se repetiu a versão
reprovada só para ampliar métricas. No depois, a jornada total medida foi 2,418 s
(incluindo verificações locais); aquisição nativa e interface não foram medidas.

| Etapa corrigida | Chamadas da fonte | Linhas retornadas/tentadas na escrita | Tempo fonte | Tempo escrita |
|---|---:|---:|---:|---:|
| Extração inicial parcial | 5 | 1080 | 0,00857 s | 1,24455 s |
| Recuperação e cauda | 2 | 723 | 0,00506 s | 0,85394 s |
| Atualização repetida | 1 | 2 | 0,00114 s | 0,03118 s |

As 723 incluem duas barras de sobreposição; são 721 chaves adicionais. Escrita
mede save_ohlcv_data e inclui sua preparação/upsert; o restante do total contém
controle de blocos e verificações. Retries internos podem multiplicar as chamadas
MT5 dentro de uma chamada do conector. Não há reconstrução derivada ou Analytics
neste recorte. SQLite final 544768 bytes, CSV 218708 bytes; pico RSS do processo
164429824 bytes. Não há medida de I/O físico nem pico por etapa; não se infere
redução de memória. Tempos são uma amostra local com caches não controlados.

## Verificação e limites

Ambiente instalado via install.sh --yes --no-mt5 e requirements-dev.txt. Baseline
reutilizado da execução única da PR116: 1461/18/3, saída 0, cobertura 77,44%.
A equivalência de fontes antes de editar, Python e todos os pacotes foi comprovada
no worktree EXTR-01 (40fbef5). Não é um passe de árvore diferente por suposição.
Runtime corrigido: 677dfe3. Testes focados: 34 aprovados em 43,97 s;
quatro provas adicionais de consulta/CLI/UI passaram e a nova falha de Cancelar
foi corrigida; seus três cenários passaram em 10,09 s. A tentativa reprovada
(1 falha/4 aprovados) foi preservada. Doctor 16 OK, dois avisos, zero falhas.
Suíte completa única: **1468 aprovados, 18 pulos, 3 avisos em 2237,29 s**,
saída **0**, cobertura **77,64%**, fontes intactos. Os seis checks do código
6096ff8 passaram: Ubuntu, Windows, PostgreSQL, lint e os dois instaladores.
Implementação encerrada e PR117 pronta para revisão, sem merge. Esse commit tem os mesmos
fontes/testes de 677dfe3; os commits posteriores alteram apenas documentação.
ShellCheck não se aplica: nenhum .sh alterado. Windows físico pendente.

A prova MT5 nativa permanece pendente porque a ponte está desativada na
configuração disponível e a porta local configurada recusou a conexão
(ConnectionRefusedError). Não se alteraram terminal, conta, credenciais ou Safety.
Fakes demonstram persistência/retomada e fidelidade estrutural; não acesso ao feed.
Não houve ordens nem operações em conta real. Nenhum conteúdo deste recorte foi
aplicado ao workspace principal. PR116 está pronta para revisão, com sua suíte única
1461/18/3 e seis checks do runtime 40fbef5 aprovados. Não houve merge.

Artefatos próprios: logs/extr-01/{before,after}.json, baseline-equivalence.json,
regression-red.log, targeted.log, journey-tests.log, ui-cancel-green.log,
doctor-final.log e full-final.*. O relatório versionado é autossuficiente; logs
não são versionados. Não iniciar novo benchmark/campanha numérica sem defeito novo.
