# EXTR-01 — validação nativa DEMO e preparação da integração

## Fechamento em master após a integração (2026-10-19)

Estado vigente: **EXTR-01 concluída no recorte Linux/Wine**, com PR #117 integrada
em `a92cd8a13b305d5551b9244a07f810b9bc55415f`. Esta seção supera os estados históricos
de PRs abertas e bases pendentes preservados mais abaixo. Uma tarefa por PR,
integração por merge Git e avanço da base seguinte somente após integrar a anterior.
Nenhuma árvore foi copiada ao Root; seu HEAD, branch, índice e trabalho local permanecem.

| PR | Tarefa | Merge em master | HEAD de código/evidência aprovado antes da conciliação |
|---|---|---|---|
| [#108](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/108) | T20.4 | `2bd7a11` | `c991b04` |
| [#109](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/109) | T20.5 | `1b2cd34` | `16eddef` |
| [#110](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/110) | T20.6 | `aed0f77` | `79b9d30` |
| [#111](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/111) | T20.24 | `541c1c3` | `90d63d4` |
| [#112](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/112) | T20.25 | `b7c8fd5` | `4aea302` |
| [#113](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/113) | T19.14 | `bd5cb6d` | `4373cea` |
| [#114](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/114) | T13.2 | `2efdabf` | `08850c0` |
| [#115](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/115) | T13.3 | `2ca2fc0` | `e653b61` |
| [#116](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/116) | T13.3b | `814ded9` | `d952d4c` |
| [#117](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/117) | EXTR-01 | `a92cd8a` | `d5916ee` |

As bases #106/#107 e documentais #118–#121 já estavam integradas. Cada conciliação
desta cadeia alterou somente documentação em relação ao HEAD aprovado da respectiva
PR. Conferência da árvore inteira fora de docs/ antes e após cada merge; seis checks
aprovados por PR reaproveitados, incluindo Linux/Windows/PostgreSQL. Os 279 hashes
de fontes/testes da suíte final da #116 e da #117 também conferiram. Aceite final
#117: **1468 aprovados/18 pulos existentes/3 avisos, saída 0, cobertura 77,64%**.
Resultados históricos da #111/#112/#113 foram distinguidos dos complementos: o CI
próprio do HEAD corrente é seu aceite; a suíte composta 1398/13/3 contém também
alterações posteriores e não foi atribuída a uma árvore anterior como se fosse igual.

Os dez workflows automáticos dos merges repetiam suítes de runtimes iguais aos
já aprovados nas PRs. Foram cancelados apenas esses runs identificados por SHA;
conclusão `cancelled` confirmada, sem alegar verde novo em master. Os resultados
originais de execução permanecem. O estado transitório UNSTABLE da #117 foi
resolvido pelo término SUCCESS do Sourcery e confirmação CLEAN antes do merge;
não houve falha nova de runtime. Nenhuma suíte global, PostgreSQL ou benchmark foi
iniciada nesta integração. Scripts de produto e dependências não foram alterados.

### Conferência solicitada no código integrado

No master `a92cd8a`, **sete testes passaram em 14,03 s**, saída 0: recuperar bloco
histórico anterior à última barra; falha persistente sem sucesso falso; repetição
idempotente; escopo/teto temporal dos blocos falhos; jornada CLI com reabertura e
exportação; diálogo Tk com sucesso/falha/Cancelar. São os casos existentes, com
provedor falso e Xvfb próprio. Esta conferência foi explicitamente solicitada pelo
proprietário após o merge; não substitui nem repete a suíte global aprovada.

### Ponte e conta DEMO

A primeira tentativa nativa em master parou antes da extração: configuração efetiva
[BRIDGE] enabled=true, 127.0.0.1:18812, conexão recusada, serviço e terminal ausentes.
O comando start existente chegou a abrir a porta, mas o lançador não permaneceu
após o término da célula; logs e tentativa reprovada preservados. Não se atribui
uma causa interna de encerramento que não foi observada. A execução foreground
existente permaneceu ativa e corrigiu a indisponibilidade para esta jornada.

Ponte restrita mt5x-bridge/2, 28 funções de leitura/conexão, sem --allow. Inicializado
somente o terminal já configurado, sem login/senha passados e sem troca de conta.
Conta MetaQuotes-Demo, trade_mode=0/ACCOUNT_TRADE_MODE_DEMO, conectada; negociação
automática desabilitada. Identificado o processo python.exe da ponte por comando,
usuário e instante de criação; registrado no PID file do gerenciador existente.
O listener Linux do Wine é wineserver: esse PID não foi usado como PID da ponte
nem encerrado. Nenhuma credencial, sessão de terceiro ou configuração foi alterada
nesta retomada. A prova não enviou ordens nem usou replay.

Serviço ativo em foreground na sessão própria; sua persistência após encerrar essa
sessão não foi comprovada. Próximo ajuste operacional delimitado: diagnosticar a
saída do start em segundo plano e validar a permanência do serviço pelo gerenciador,
sem reiniciar o terminal ou ampliar as permissões da ponte. Não bloqueia esta jornada
já concluída, e não foi apresentado como serviço persistente instalado.

### Jornada nativa curta em master

Conector real, EURUSD/M1, 14/10/2026 00:00 a 16/10/2026 12:00 UTC, SQLite próprio e
gravável. Extração **2880** barras; atualização anexou **720**, total **3600**. Leitura
incremental de 722 linhas inclui sobreposição. Repetição leu duas linhas e conservou
contagem/valores. Reabrir, consultar e exportar CSV passaram. Os 3600 timestamps são
únicos; time/OHLC/tick_volume/real_volume/spread coincidem exatamente com
copy_rates_range nativo e CSV lido com round_trip, sem arredondamento ou candles
fabricados. CLI de extração/exportação: saída 0. Conta DEMO confirmada antes/depois.

| Etapa | Tempo observado | Linhas da fonte ou consultadas/exportadas |
|---|---:|---:|
| Extrair | 2.889 s | 2880 |
| Atualizar | 0.737 s | 722 |
| Repetir | 0.047 s | 2 |
| Reabrir/consultar | 0.047 s | 3600 |
| Exportar CSV | 0.095 s | 3600 |

Total observado: 4.178 s, incluindo confronto
adicional com o provedor. Fonte/escrita separadas nos logs; Analytics/resampling não
integraram este percurso. É uma jornada curta, sem promessa de performance global
ou economia de memória; o benchmark T13.3b anterior permanece com seu escopo próprio.

UI Tk real, conector nativo, display Xvfb próprio: selecionar EURUSD/M1 e período,
extrair, atualizar até agora, Cancelar, preservar confirmados, retomar e exportar.
O diálogo informou um cancelado e nenhum sucesso no cancelamento; retomada informou
um sucesso. **4736 timestamps únicos e 4736 linhas exportadas**, no banco próprio
da UI. O corte desse percurso é o agora real, como rotulado, distinto da jornada fixa.
Modal de destino automatizado e callback final de exportação aguardado; nenhuma fonte
ou coordenação de extração foi simulada nessa prova de UI.

Doctor do código integrado com configuração efetiva: **21 OK, um aviso opcional
pandas-ta, zero falhas, saída 0**. Não houve alteração de .sh; ShellCheck adicional
não aplicável. **Windows físico permanece pendente**, sem invalidar o aceite Linux/Wine.
Artefatos privados em logs/integracao-108-117-20261019/; execução nativa e testes em
validacao-master/. Nenhum banco, CSV, caminho pessoal ou credencial foi versionado.

## Histórico da prova anterior à integração

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
