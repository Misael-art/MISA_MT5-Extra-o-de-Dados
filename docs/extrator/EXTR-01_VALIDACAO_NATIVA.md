# EXTR-01 — validação nativa DEMO e preparação da integração

Validação em 2026-10-19, após a revisão técnica local das PRs #116/#117.
Código sem alterações: runtime #116 40fbef5, head d952d4c; runtime #117
677dfe3, head 6ebdc76. As diferenças posteriores aos runtimes são documentais.
Não foram repetidos pytest completo, PostgreSQL, oráculos ou benchmarks.
Aceites conservados: #116 1461/18/3, saída 0, cobertura 77,44%, seis checks;
#117 1468/18/3, saída 0, cobertura 77,64%, seis checks.

## Revisão e integração

A revisão local percorreu os diffs de checkpoints/lotes/transações, esquema
aditivo, identidade contextual/UTC, consulta de intervalos falhos, retomada e
cancelamento. Não encontrou impedimento material nos caminhos revisados;
não substitui revisão externa. GitHub: PR116/117 abertas, prontas, mergeState
CLEAN, sem revisões externas registradas na consulta. Nenhum merge remoto feito.

Dependência completa observada: #106 → #107 → #108 → #109 → #110 → #111 →
#112 → #113 → #114 → #115 → #116 → #117. #106 tem base master; cada seguinte
consome a branch anterior. Não redirecionar #116/#117 diretamente a master
ignorando as bases. O master buscado era 489591c e é ancestral da candidata.
Branch de preparação: codex/integracao-extrator-116-117, inicialmente no head
6ebdc76 da #117, contendo d952d4c/#116 e todos os antecessores. A prova executou
esse código; o ambiente vigente da #117 foi reutilizado. Nenhuma árvore foi
copiada para o workspace principal, cujo HEAD e índice foram preservados.

Próxima tarefa concreta de integração: revisar a base #106 (T20.2) e avançar
pela cadeia na ordem acima pelo fluxo de revisão/merge aplicável. Resolver
apenas conflitos reais; mudança de código ou nova falha determina a cobertura
necessária. Aprovações existentes não são descartadas por troca de agente.

## Impedimento da ponte corrigido

Configuração efetiva tinha [BRIDGE] enabled=false, host 127.0.0.1, porta 18812.
Não havia listener nem terminal Wine ativo; Wine, Python Windows, pacote MT5 e
terminal estavam instalados. Foi iniciado o servidor existente restrito
mt5x-bridge/2, sem --allow e sem funções de ordem, na mesma porta local.
Seu PID foi identificado e registrado no controle existente; nenhum processo
ou sessão de terceiros foi encerrado. Alteração da configuração principal:
somente enabled=false → true, com cópia anterior privada. A configuração da
prova aponta para SQLite próprio; a base principal não foi aberta para escrita.

A inicialização usou o terminal já instalado e sua conta cadastrada, sem login,
senha ou troca de conta via argumentos. Conta comprovada antes/depois:
MetaQuotes-Demo, trade_mode=0 igual a ACCOUNT_TRADE_MODE_DEMO, terminal conectado.
Negociação automática estava desabilitada. Não se enviou ordem nem se usou
replay. Nenhuma credencial foi exibida ou alterada. Ponte acessível somente em
loopback; protocolo e bloqueio de funções de ordem permanecem os existentes.
Doctor após a correção efetiva: saída 0, 21 OK, um aviso opcional, zero falhas.

## Jornada histórica nativa

EURUSD/M1, 14/10/2026 00:00 UTC a 16/10/2026 12:00 UTC. Dois blocos iniciais
pelo conector real/ponte e cmd_extract existente: 2880 barras. update_symbols
existente anexou 720, lendo 722 com a sobreposição prevista: total 3600.
A segunda atualização leu duas barras sobrepostas e não mudou dados nem
contagem. Banco fechado/reaberto: consulta exatamente igual. cmd_export CSV
retornou saída 0. Extração CLI retornou saída 0; callbacks das atualizações
informaram um sucesso, nenhuma falha/cancelamento.

Confronto exato com copy_rates_range nativo: 3600 timestamps únicos e campos
time/open/high/low/close/tick_volume/real_volume/spread iguais, incluindo CSV
lido com round_trip. Não houve arredondamento nem preenchimento de candles.
Relógio persistido corresponde aos epochs UTC do provedor, sem deslocamento
introduzido; Analytics/resampling e sessão derivada não foram executados.

| Etapa | Total | Fonte | Escrita | Linhas da fonte/tentadas na escrita |
|---|---:|---:|---:|---:|
| Extrair | 3,628 s | 0,068 s / 2 chamadas | 3,255 s / 2 chamadas | 2880 |
| Atualizar | 0,944 s | 0,014 s / 1 chamada | 0,885 s / 1 chamada | 722 |
| Repetir | 0,066 s | 0,007 s / 1 chamada | 0,036 s / 1 chamada | 2 |
| Reabrir/consultar | 0,060 s | — | — | 3600 consultadas |
| CSV | 0,115 s | — | — | 3600 exportadas |

Tempo observado total 5,129 s inclui confronto adicional com o provedor e
verificações locais. É a medida desta jornada curta, sem campanha de benchmark,
sem ganho percentual, SLO ou alegação de economia de memória.

## Ações da aplicação real

UI Tk real sob display Xvfb próprio, usando o conector nativo e conta DEMO.
Foram exercitados seleção EURUSD/M1, período 14–15/10, Iniciar Extração,
Atualizar até agora, Cancelar, retomar, consulta/inventário e Exportar CSV.
O cancelamento foi reconhecido e terminou com um cancelado; os blocos
confirmados permaneceram. Retomada informou um sucesso. Atualização da UI usa
agora real, conforme seu rótulo; seu banco isolado é distinto do recorte fixo.
Terminou com 4652 timestamps únicos e 4652 linhas exportadas. Seleção do destino
e confirmação modal foram automatizadas; trabalho de fonte/escrita/exportação
foi executado pelos componentes reais, sem coordenador/provedor falso.

A primeira observação do exportador leu o arquivo antes da conclusão do trabalho
assíncrono e terminou inconclusiva. Log preservado; corrigido somente o roteiro
para esperar o callback on_success existente. Repetiu-se apenas a checagem da
UI no mesmo banco, sem mudança de produto ou tolerância. O segundo roteiro
comprovou callback de sucesso e contagens. Não houve nova suíte automatizada.

Artefatos locais privados em logs/retomada-nativa-20261019: demo-gate.json,
bridge-owned.json, integration-manifest.json, native-result.json, native.sqlite,
native.csv, native-ui-result.json, native-ui.sqlite, native-ui.csv e logs.
Configurações/cópias, contas e bases não são versionadas. Windows físico continua
pendência delimitada; sua ausência não invalida o aceite Linux/CI nem esta prova.
