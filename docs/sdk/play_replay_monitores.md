# PLAY/REPLAY: monitor e gráfico da sessão

Correção de 23/10/2026 sobre a composição G19 (`321272f0b24d`).

O lançamento consulta as telas conectadas e escolhe o monitor que contém o centro
da janela da aplicação. Usa as coordenadas X11/XWayland ou Win32, considerando a
escala aplicada pelo sistema; não soma monitores nem fixa uma resolução menor.
Sem janela lançadora, usa a posição do ponteiro. Um centro que ficou fora da área
após retirar o dock escolhe a tela conectada mais próxima. Cada janela própria é
posicionada/maximizada uma vez, permitindo redimensionamento posterior.

- REPLAY sempre exibe o gráfico.
- PLAY oferece **Exibir gráfico durante o PLAY**. O padrão dos Experts automáticos
  é desmarcado; o painel manual reconhecido começa marcado.
- Oculto significa display virtual no Linux e janela minimizada no Windows;
  o Expert continua com seu gráfico interno. O backtest permanece oculto.
- A escolha é deste lançamento. Não muda o preset, risco, confirmações de conta,
  kill switch, comportamento do Expert, nem a configuração da ponte.

## Causa e correção Linux/Wine

O servidor Wine compartilhado já havia inicializado sua tela em 1280×800 na ponte
oculta. Trocar apenas DISPLAY ou maximizar a janela externa não alterou a área
Win32 de desenho: o teste podia avançar enquanto a visualização ficava preta ou
recortada. Um desktop nomeado no mesmo servidor também não bastou.

Sessões visíveis e todo PLAY Linux (inclusive oculto) usam um prefixo temporário próprio dentro do perfil UUID:
registro e arquivos Windows copiados, C: restrito ao perfil e desktop Wine iniciado
na resolução do monitor escolhido. Explorer precisa ser o primeiro processo de
cada inicialização; compilação e gráfico usam esse ambiente privado. O servidor
privado é encerrado entre compilação e execução e ao finalizar. As telas são
reconsultadas; mudança de dimensões durante a preparação pede novo lançamento,
para não continuar com modos de vídeo incompatíveis já inicializados. O servidor da ponte e os outros terminais continuam independentes. O PLAY oculto
assim também fica independente de um terminal principal já aberto na tela real.

Um `run` externo injetado conserva a posse do seu processo/ambiente; o runtime
visual próprio é preparado somente pelo lançador da sessão. A regressão existente
de exportação visual/CSV compartilhado cobre essa fronteira, sem exigir Wine
real para runners simulados.

Somente Common/Files compartilha os canais UUID existentes. A cópia do sistema
consome temporariamente espaço (cerca de 400 MB no host observado), sob as mesmas
verificações de orçamento/cancelamento usadas na preparação. Após confirmar o
encerramento de seu servidor e liberar o contexto nativo, remove suas cópias e
links; conserva logs, relatório e artefatos da sessão. Falha de encerramento mantém
os arquivos protegidos. A espera comprova encerramento mesmo se nenhum servidor
chegou a iniciar; um prefixo que a preparação não criou nunca recebe sinal.
Não há exclusão global de prefixos ou caches.

## Evidência e limites

[Registro verificável](../evidencias/PLAY_REPLAY_MONITORES_20261023.json):
160 testes dirigidos aprovados/1 exclusão prévia de plataforma e12 de interface; monitor retirado,
coordenadas negativas, tamanhos diferentes, Linux/Windows simulado, posse por
prefixo+UUID, orçamento/cancelamento e preservação em falha de encerramento.
A interface tem três novas verificações de escolha/padrão e passagem do monitor,
além dos testes anteriores de consentimento.

Replay real do painel manual em Tester, EURUSD/H1, 20–21/10/2026:
[handheld 1280×800](../img/replay_display/handheld-1280x800.png) e
[tela Xvfb 1920×1080](../img/replay_display/virtual-1920x1080.png), ambos com candles,
indicadores, painel e avanço observado. Cancelamentos deliberados encerraram
somente processos próprios e removeram o runtime temporário. Sem cliques de ordem,
negociação REAL ou aceite financeiro; não são testes completos de gestão/replay.

O dock foi desconectado durante a investigação: seleção com múltiplas telas e
retirada é coberta pelos testes; o retorno físico do dock e combinações de escala
seguem uma conferência manual específica. Não há migração automática de uma
sessão já aberta ao conectar/desconectar monitores. Windows físico tem aceite
próprio. PLAY do painel manual também foi conferido em DEMO: gráfico visível e
modo oculto, ambos com telemetria, cancelamento próprio e carteira inalterada, sem
cliques de ordem. A primeira sondagem, antes de conectar a API, foi recusada; após
o fluxo normal de conexão os dois ensaios passaram. Isso não qualifica negociação
ou a gestão de ordens. A mudança de exibição não soluciona credenciais,
indisponibilidade de corretora ou ausência de dados.

A suíte integral da base no display compartilhado terminou com 2947 aprovados,
52 excluídos e duas falhas de geometria da UI. Essas duas verificações passaram
na mesma base em Xvfb isolado, sem mudar seus testes. Resultado integral da
correção/CI deve ser consultado no PR; não apagar as falhas originais.

## Retomar e disponibilizar

Branch `codex/play-replay-display`, baseada em G19; o WIP antigo e a frente de dados
não foram incorporados. HADOUKEN `6fb02374cd65` e Safety `c5a3e2162cac` permanecem
inalterados. Após conferir os checks, usar release imutável do commit publicado em
`app-development`, preservando dados/configuração/ambiente e `app-current` aceito.
O atalho habitual e o menu devem usar essa mesma release. Ver
[procedimento](../TESTAR_DESENVOLVIMENTO.md).

## G19.10 — área interna no retorno do dock

A volta do dock revelou um contraexemplo na correção anterior: o contêiner
X11 ocupava3456×1458, mas o terminal/área Win32 permaneciam1280×800. Captura da
janela externa, isoladamente, não comprova preenchimento do gráfico.

Wine11.17 constrói a lista de modos virtuais a partir da entrada
`HKCU/Software/Wine/Explorer/Desktops/Default`, inclusive para desktop UUID.
Sem essa entrada, a lista observada terminava em1280×800, resolução do primário,
e a tentativa de3456×1458 era recusada. Ver código primário
[add_virtual_source/get_default_desktop_size](https://github.com/wine-mirror/wine/blob/wine-11.17/dlls/win32u/sysparams.c).

O runtime agora prepara `Default=larguraxaltura` no user.reg da cópia privada,
ANTES do primeiro Wine, com as dimensões retornadas pelo monitor selecionado.
Mantém os outros valores e todo registro original. Chamadas seguintes da mesma
sessão não reescrevem registro vivo; tamanho diferente durante a preparação
recusa e pede novo lançamento. Não altera modo/resolução dos monitores do host,
registro da ponte, DPI, cálculos, negociação ou Windows nativo.

Baseline85 testes;126 finais selecionados, doctor20/1opcional/0. O método de
produção foi executado em prefixo isolado: GetSystemMetrics e área virtual
passaram de1280×800 para3456×1458; servidor/cópias próprios removidos ao terminar.
Lançamentos normais PLAY/REPLAY e inspeção do gráfico correspondente em curso.
[Evidência complementar](../evidencias/PLAY_REPLAY_MONITOR_INTERNO_20261023.json).
