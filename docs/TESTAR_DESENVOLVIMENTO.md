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


## Reconferência de composição em12/11/2026

Composição revisável na branch `codex/composicao-diagnostico-20261112`,
com as fontes publicadas PR293–300,303–307. Estado, owners, SHAs, evidências
e próximos passos estão na fila de [continuidade](../CONTINUAR_AQUI.md). Prévia identificada
pelo manifesto e com retorno preservado. Integração financeira, Windows físico,
OAuth real, entrada física e observação prolongada conservam aceites próprios.

Os commits aplicados na composição são ancestrais da prévia. Patches de PRs
independentes mantêm o vínculo entre SHA original e SHA aplicado no manifesto.
Confira ambos e os hashes runtime. O checkout único e seu índice principal
permanecem preservados. Retorno primário `development-650656fac67d`,
com retorno histórico adicional `development-57d13ee91620`. Os ambientes,
dados e configuração atuais são reutilizados por links.


## Composição de diagnóstico em12/11/2026

A branch `codex/composicao-diagnostico-20261112` reúne as fontes públicas
revisadas de configuração, dados, Laboratório, Drive e restauração. A fila
com owners, SHAs, evidências e próximos passos está em [continuidade](../CONTINUAR_AQUI.md).
O destino efetivo de `app-development` e o `mt5x-release.json` identificam
a prévia usada pelo atalho. A conferência do artefato e os checks se referem
à fonte publicada. Retorno, ambiente e dados são preservados.

A restauração inclui intenção durável, consulta e recuperação explícita.
Teve31 regressões focadas, seis casos existentes em lote próprio, CLI oficial
e GUI XWayland em SQLite sintético, com preferências isoladas. O CI do SHA
publicado e os aceites de hardware, entrada física, login, falha de energia
e operação nativa têm registros próprios. Seções datadas anteriores conservam
suas fotografias históricas.
