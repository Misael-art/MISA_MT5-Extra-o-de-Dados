# Testar o desenvolvimento pelo atalho habitual

## Diagnóstico e composição conferidos em 12/11/2026

A fila vigente, a arquitetura, os owners e o roteiro priorizado estão em
[CONTINUAR_AQUI](../CONTINUAR_AQUI.md). A fonte composta é PR308 `561152b`,
com PR307 `c296538`, PR309 `2a5638d` e runtime PR310 `fdbe0d9`; a ponta
documental da PR310 é `97da399`, com conflito de recibos resolvido e CI iniciado.
A prévia efetiva continua `650656f` enquanto os gates pertinentes executam.

O artefato tem365 hashes do Git publicado e6.378.666B. Launcher real XWayland
confirmou restauração/recuperação7→99, relógio incompatível recusado, cópia e
consulta de retenção pelo worker, com preferências próprias. Observação adicional
de180s teve4.168 pulsos, seis leituras, três temas e zero erros de callback.
Pico272,6MiB, mínimo7,73GiB disponíveis e máximo68°C. São solicitações
instrumentadas na janela visível; entrada física, tecnologia assistiva, login,
maior duração e alvos nativos possuem aceites próprios.51 regressões finais
passaram em14,75s, com oito fontes estáveis por hash. Doctor16/2/0 offline.

Windows309 foi cancelado prematuramente após mais de duas horas. O log arquivado
provou avanço até98%; a repetição foi solicitada somente para o job Windows no
mesmo SHA, preservando o histórico. Os demais CIs continuam com estado próprio.
A pergunta sobre destino da reunião em QA aguarda resposta. Base privada não
foi fixture; seus bytes/mtime ativos permaneceram iguais na conferência final.


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

A referência aceita `app-current` e sua release permanecem disponíveis para
retorno. Só a prévia é usada pelo atalho habitual solicitado. A configuração,
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

## Conferência de 12/11/2026

O link efetivo aponta para `development-650656fac67d`, com 354 hashes aprovados. Retorno `development-57d13ee91620`, com 341 hashes aprovados. `state/development-preview.json` existe e ainda cita `784a252`. O destino efetivo e o manifesto da release foram conferidos. O arranque do retorno sobre dados atuais permanece pendente. Os novos corretivos estão publicados para revisão e a composição da próxima prévia tem validação própria na [fila](../CONTINUAR_AQUI.md).
