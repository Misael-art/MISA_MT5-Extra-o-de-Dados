# Testar o desenvolvimento pelo atalho habitual

## Conferência de 07/11/2026

Prévia efetiva conferida em 07/11: `650656fac67d` (PR300), 354 hashes sem divergência; retorno `57d13ee` preservado. O atalho não foi atualizado neste lote. HADOUKEN `d4d389e` preservado em branch; Safety `27b8599` detached.

Drive segue em revisão na PR302; as composições abaixo são históricas.

## Correção de monitor PLAY/REPLAY — 23/10/2026

A branch `codex/play-replay-display` acrescenta a correção sobre App G19
`321272f0b24d`, com HADOUKEN `6fb02374cd65` e Safety `c5a3e2162cac` preservados.
Ao publicar a release de desenvolvimento, conferir o commit no manifesto e os
atalhos/menu no mesmo `app-development`. A referência aceita continua separada.
REPLAY sempre visual; PLAY automático oculto por padrão e painel manual visível,
com escolha por lançamento. [Comportamento, provas e pendências](sdk/play_replay_monitores.md).
A composição descrita abaixo registra a prévia anterior e seu procedimento de retorno.

O proprietário determinou em21/10/2026 que os avanços fiquem disponíveis para
teste. O atalho da área de trabalho apontava para um checkout antigo com WIP,
enquanto o menu abria a release aceita App171. Agora ambos devem abrir a mesma
**prévia de desenvolvimento**, identificada pelo nome do atalho e pelo manifesto.

## Composição disponibilizada

Branch `codex/desenvolvimento-testavel-20261021`, composta por Git, preservando
o histórico e sem copiar arquivos do WIP antigo:

| Fonte | Conteúdo para teste |
|---|---|
| App171/ac7af2c3 | Base aceita, presets, histórico e serviços já integrados |
| App172→173→174→175→176→177 | Contexto responsivo dos robôs, Recursos com varredura parcial, reutilização de conexão, consulta de estudos, navegação adaptável e resumo do catálogo |
| App182/46651fd | Mesmo DISPLAY durante compilação e replay no Linux/Wine |
| App178→179→180→181 | Continuidade/recuperação e roadmap atualizado |
| HADOUKEN ec550d77 / Safety3988df13 | Conjunto aceito existente, preservado |

As PRs citadas continuam com seu estado remoto próprio. A composição local é
para teste, não merge/aceite das PRs. A falha Windows da177 e a operação manual
do replay ainda pendente permanecem registradas. A prévia inclui o código de
exibição do painel, mas não declara resolvida a interação manual das abas.

TFs em ordem crescente, Todos, seleção de vários ativos e gráficos comparativos
do mesmo preset foram pedidos **no roadmap** e continuam planejados; não aparecem
como funcionalidades implementadas. A frente HADOUKEN49–61 e WIPs não revisados
mantêm suas dependências, sem instalação indiscriminada de fontes incompatíveis.

## Caminho do atalho e dados

Os dois atalhos `mt5-extracao.desktop` usam o mesmo destino estável
`mt5x/app-development/run.sh` sob a pasta de dados do usuário. O link aponta para
uma release criada do commit publicado. `mt5x-release.json` nessa release registra
SHA, composição, hashes dos arquivos e modalidade `development_preview`.

A release anterior da prévia permanece disponível para retorno (o alias
`app-current` não existe neste host; ver [CONTINUAR_AQUI](../CONTINUAR_AQUI.md)). Só a prévia é usada pelo atalho habitual solicitado. A configuração,
base, preferências, logs, exportações e `.venv` existentes são reutilizados por
links; não se copia banco nem instalação Wine/MT5 para publicar uma interface.
Não publicar configuração, credenciais ou conteúdo da base no GitHub.

## Verificação da disponibilização

- Conferir que todos os commits da composição são ancestrais da prévia e que
  seus arquivos runtime foram preservados na integração.
- Executar regressões dos componentes compostos e diagnóstico `doctor --no-probe`.
- Verificar arquivos/hashes da release e resolução dos links para os dados atuais.
- Validar os dois arquivos desktop, permissão executável e destinos iguais.
- Abrir a interface da mesma release em modo local, conferir catálogo/navegação
  e encerrar somente a janela de QA. Isso não qualifica operação nativa do replay.

O atalho habitual abre o fluxo normal com conexão MT5. O modo local é usado
somente na conferência da interface, sem iniciar uma nova campanha ou operação.
Os resultados concretos ficam no recibo local de disponibilização.

## Continuidade e espaço em disco

A cada avanço testado, publicar commit/push, preparar a nova release de código,
conferir o manifesto e trocar atomicamente `app-development`. Manter atalhos
coerentes e uma referência anterior para retorno; não apontar a janela aberta
para um working tree que está sendo editado ou trocado de branch.

Não criar outro ambiente virtual, cópias de banco ou caches MT5 para essa troca.
Retirar apenas staging próprio e releases de prévia substituídas sem processos
ou referências, após a validação. Preservar fontes, histórico, WIP e diagnósticos
relevantes; não limpar caches globais ou worktrees do proprietário.


## Prévia candidata conferida em 12/11/2026

PR308 `561152b` contém o runtime das correções de relógio/restauração, lease e
retenção. Os 365 hashes do artefato de 6.378.666 B foram conferidos. O launcher
do próprio artefato abriu no compositor XWayland com configuração e duas bases
SQLite próprias. Recuperação 7→99, recusa de relógio incompatível, cópia e
consulta de retenção pelo worker passaram. Foram 25 pulsos, intervalo máximo
0,571 s, pico 275,2 MiB, mínimo disponível 7,90 GiB e máximo 59 °C.

Observação contínua adicional, limitada a 180 s, teve 4.168 pulsos, seis
atualizações, três alternâncias de tema e zero erros de callback. Pico
272,6 MiB, mínimo 7,73 GiB e máximo 68 °C. Quatro hashes de preferências e
favoritos do proprietário permaneceram iguais em ambos os ensaios. Os
controladores solicitaram as ações na janela visível. Entrada física,
acessibilidade, login, maior duração, hardware nativo e energia seguem próprios.
A ponta pública da retenção `97da399` reposiciona somente recibos documentais
para liberar CI após a atualização da dependência, com merge-tree limpo. O
runtime conserva o SHA de origem `fdbe0d9` usado na composição e no artefato.
O link estável continua em `650656f` até a conferência dos gates pertinentes.


## Disponibilização conferida da prévia2b

O atalho e o menu agora resolvem a composição `2b3b274`, com365 arquivos
conferidos. Linux completo, PostgreSQL, lint e os dois instaladores passaram
no SHA publicado. Windows permanece em curso. O procedimento de ativação
a674860c2d9a4d6398587a43e80513a4 registrou intenção e resultado duráveis e
revalidou destino/atalhos/estado compartilhado antes da troca atômica. O pico
do processo de ativação foi23,3MiB. A conferência pelo launcher estável teve
saída0, três cópias próprias consultadas, valor99 preservado, pico134,6MiB,
mínimo8,21GiB disponíveis e duração4,09s. Retornos650/57 estão preservados.

A primeira preparação ligava uma lista legada de símbolos do checkout. A
comparação com a prévia efetiva mostrou que esse arquivo estava ausente.
O vínculo novo foi retirado antes da ativação e o arquivo legado foi preservado.
O método de produção `load_symbols` tem o mesmo conteúdo nas duas versões;
executado com objeto controlado e ausência real do arquivo, devolveu os mesmos
cinco símbolos e configuração de tempos. Essa validação foi controlada. A GUI
XWayland2b foi exercitada antes da reconciliação desse vínculo, no mesmo código.
A comprovação de hash/existência de preferências cobre os ensaios finais
isolados; o estado anterior aos primeiros ensaios permanece sem comprovação.

A admissão inicial de carga pesada foi aplicada à troca de metadados e adiou
a tentativa. A operação final foi dimensionada como leve, com128MiB previstos,
32MiB de leitura,256KiB de gravação e reserva mínima de4GiB. A GUI, builds e
suítes locais ficaram adiados durante a pressão de I/O. Foi observado copiador
de outra tarefa,13MiB lidos e13MiB gravados em3s, preservado em execução.
Kernel sem avisos nos30min consultados e Btrfs com6.741 corrupções acumuladas,
sem mudança dos contadores. Essas observações mantêm a causa dos reinícios
indeterminada. Recibos completos em state/qa-tmp/diagnostico-20261112/.
