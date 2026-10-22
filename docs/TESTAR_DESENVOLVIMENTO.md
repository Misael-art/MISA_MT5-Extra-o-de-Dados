# Testar o desenvolvimento pelo atalho habitual

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
