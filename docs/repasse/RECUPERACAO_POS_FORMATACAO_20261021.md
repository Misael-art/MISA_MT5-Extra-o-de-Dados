# Retomar o desenvolvimento depois da reinstalação

**Atualização de24/10/2026:** o proprietário confirmou que não levará o SD e não
terá acesso ao host antigo. Este roteiro e seu pacote são históricos. A entrada
atual é [CONTINUAR_AQUI](../../CONTINUAR_AQUI.md), com bootstrap e recuperação
privada remota. Não usar o `ready_to_format` deste corte antigo como prova da
preservação atual; conferir o manifesto e os recibos da nova entrega.

Este roteiro complementa o [repasse da integração](INTEGRACAO_20261021.md).
O [estado verificável](RECUPERACAO_ESTADO_20261021.json) registra o corte, os
[nomes das referências locais](REFS_PRE_FORMATACAO_20261021.json), a fila remota
e os testes realizados. O objetivo é recuperar código, trabalho pendente,
contexto e dados privados; não conceder aceite operacional ou financeiro.

## Antes de formatar

**Preservação para retomar o desenvolvimento concluída no corte registrado.**
O proprietário confirmou que o SD ficará fora da formatação e pediu somente o
necessário: fontes, contexto e bases úteis. O complemento privado está em
`Backups/MT5X/2026-10-21-desenvolvimento` nessa mídia, com `LEIA_PRIMEIRO.md`,
manifesto, verificador e recibos. São aproximadamente 142 MB, em vez do pacote
amplo de 16,5 GB preparado anteriormente. O SD foi identificado como dispositivo
separado do SSD; todos os arquivos foram relidos e conferidos por SHA-256.

O banco restaurado **a partir do SD** passou quick_check, hash e contagens das 43
tabelas. O store do clone GitHub reabriu 3 trades existentes e o journal sem criar
backtest ou consulta. Os três bundles foram verificados. `ready_to_format=true`
no estado JSON refere-se apenas ao escopo de desenvolvimento declarado, nesse
corte. O novo SO ainda não foi instalado/testado; não é aceite operacional nem
imagem do host ou promessa de preservar caches de corretora.

Depois desse ensaio, as grandes cópias temporárias preparadas nesta tarefa foram
removidas do SSD. O aumento de espaço livre observado foi 16,6 GB; a soma lógica
removida foi 17,58 GB e difere do espaço físico em Btrfs. Fontes, banco, configuração
e evidências originais do proprietário foram preservados. O recibo privado da
limpeza identifica os diretórios descartáveis; nenhuma limpeza global foi executada.

Se houver novas alterações depois desse corte, atualizar a preservação antes de
apagar o SSD. Manter o SD fora da seleção de discos a formatar. O recibo em
`receipts/` registra a mídia e o ensaio; conservar esse diretório com o pacote.

Não colocar banco, configuração privada, `.env`, logs brutos, arquivos compactados
privados ou credenciais no repositório público. Reautenticar as ferramentas após a
reinstalação; não transportar sessões de autenticação como se fossem portáveis.

## O que foi comprovado antes da reinstalação

Foram clonados os três repositórios pela rede em diretórios novos, sem depender
dos objetos dos checkouts antigos. `git fsck --full` passou nos três. Todos os
commits das referências locais e HEADs das worktrees inventariadas eram
alcançáveis nesses clones, mesmo quando o nome de uma branch local não existia
no servidor. O inventário contém 222 referências App, 74 HADOUKEN e 13 Safety;
isso é uma fotografia, não uma quantidade de entregas independentes.

O WIP de cinco árvores com conteúdo real foi reconstruído em repositórios vazios
usando bundles, patches separados do índice/working tree e arquivos novos.
Os hashes de todos os arquivos regulares reconstruídos coincidiram. Links de
`.venv`/banco e bytecode foram classificados separadamente, sem confundi-los com
código novo. Há snapshots remotos `backup/formatacao-20261021/wip/...`; são
**preservação de trabalho não revisado**, não branches para instalar ou mesclar
indiscriminadamente. O estado JSON identifica os quatro snapshots principais.

Uma cópia SQLite foi criada pela API de backup com a origem aberta em leitura.
Uma restauração privada passou no `quick_check`, preservou todos os bytes por
SHA-256 e as contagens das 43 tabelas. O store da aplicação, importado do clone
novo, reabriu o histórico composto já existente e seu journal sem novo backtest
ou acesso de reserva. Os estados de qualificação continuaram false.

Nos clones isolados, os seis perfis nativos foram descritos com seus pins e
44/23/15/11/13/10 inputs; os 15 hashes Safety1.8.0 coincidiram. A instalação Safety
em uma pasta nova copiou15headers, repetiu com0cópias e preservou todos os bytes
e o marcador1.8.0, sem usar terminal. Canvas padrão ainda depende da plataforma. O validador inicial
precisou corrigir sua resolução de caminhos: os arquivos da distribuição ficam
em `Include/<include_dir>/<nome>`. Os erros do ensaio foram preservados; nenhum
header, manifesto, executor ou risco foi alterado para obter sucesso.

**Todos esses ensaios usaram o Linux atual e o ambiente existente.** Ainda falta
validar a instalação no novo sistema e os
aceites nativos já pendentes. Não descrever este ensaio como Windows físico,
novo SO testado, Tester executado ou prova de negociação.

O verificador passou 23 testes no baseline e 28 após acrescentar o perfil explícito
mínimo (cinco casos adicionais). O perfil completo continua exigindo suas cinco
categorias. Doctor com configuração existente: 20 OK/1 aviso pandas-ta opcional/0 falhas,
sem probe. A primeira chamada na worktree isolada registrou 15 OK/1 aviso/1 falha por
configuração privada ausente; foi mantida como diagnóstico daquele diretório,
sem copiar credenciais para o checkout. A chamada com `--root` explícito usou a
configuração existente em leitura. CI da nova ponta continua obrigatório para merge.

## Código aceito e trabalho que continua em fila

| Repositório | Referência de código instalado no corte |
|---|---|
| [App](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados) | `ac7af2c392611bb1bd262c7806810eb4289559b3`, PR171 |
| [HADOUKEN](https://github.com/Misael-art/projeto-indicadores-mt5) | `ec550d77caae74e9f5f4ffe30cea5ccd29621624` |
| [Safety](https://github.com/Misael-art/mt5x-safety) | `3988df13c467bd22941183f5546caad0500f9138`, 1.8.0 |

O repasse anterior foi publicado na AppPR178, commit
`a6f5dc2d989fa8a83b22e7f1e0e089c58a5cc690`. Este roteiro é uma continuação
documental com um verificador de arquivos independente das dependências do app.

App172→173→174→175→176→177 continua sendo a ordem serial de integração funcional.
No novo snapshot, 172–176 têm seis checks aprovados;177 falhou Windows antes de UI.
A PR178 também teve timeout Windows no teste analítico
`test_regime_append_continues_adx_and_confirmation_state`, antes da UI.
Falha de CI não é apagada por haver uma cópia de recuperação. Atualizar os checks
da ponta exata antes de integrar e não repetir jobs sem examinar a falha.

Também existe uma frente HADOUKEN49–60 que não estava no primeiro repasse:
TSMOM, IntradayMomentum, DonchianH4, TurtleSoup, RSI2Trend, faixaÁsia/ORB,
NR7Breakout, GapFade, ValueArea, OvernightDrift, referência de regime e seu registro.
As 12 PRs estão abertas e tinham dois checks aprovados cada no snapshot consultado.
As pontas/URLs estão no estado JSON. **Não estão integradas, não têm os respectivos
consumidores Python/aceites de replay e não foram revisadas por esta recuperação.**
Integrar cada uma exige preservar o motor atual e compor semanticamente as fixtures
B05, pois oito novos Experts alteram a contagem compartilhada. Não substituir o
motor inteiro por uma base antiga. B3, validação externa, contrato de regime e
C-22/C-23/C-24 têm pendências próprias.

As [notas da frente HADOUKEN](HADOUKEN_NOTAS_FORMATACAO_20261021.md) preservam o
contexto dos dois documentos locais mais recentes em forma normalizada. O pacote
privado conserva os originais exatos. Referências históricas dessa frente a
build6258/IPC não substituem a evidência do principal build6276 usado pelo
integrador nem comprovam a causa do bloqueio privado de sincronização.

Outras PRs abertas também foram inventariadas: App79 (ORG-02/governança Git),
App80 (ORG-04/isolation PostgreSQL) e HADOUKEN61 (continuidade da frente T22).
Não foram promovidas automaticamente. O [índice completo de PRs abertas](PRS_ABERTAS_PRE_FORMATACAO_20261021.json)
e o estado JSON preservam essas referências para revisão, inclusive quando uma
ponta antiga já for alcançável no histórico por outra integração.

## Recriar os clones em um diretório novo

Obter Git e Python pelo instalador confiável do sistema escolhido e autenticar o
acesso aos repositórios privados. Os comandos abaixo usam ferramentas nativas para
um host novo; agentes que tenham RTK instalado devem manter o prefixo `rtk` conforme
suas instruções. Não depender da existência de um caminho pessoal antigo.

```bash
git clone https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados.git mt5x
git clone https://github.com/Misael-art/projeto-indicadores-mt5.git hadouken
git clone https://github.com/Misael-art/mt5x-safety.git mt5x-safety

git -C mt5x fetch origin codex/recuperacao-pos-formatacao-20261021
git -C mt5x worktree add ../mt5x-recovery origin/codex/recuperacao-pos-formatacao-20261021

git -C mt5x switch --detach ac7af2c392611bb1bd262c7806810eb4289559b3
git -C hadouken switch --detach ec550d77caae74e9f5f4ffe30cea5ccd29621624
git -C mt5x-safety switch --detach 3988df13c467bd22941183f5546caad0500f9138
git -C mt5x fsck --full
git -C hadouken fsck --full
git -C mt5x-safety fsck --full
```

Esses `switch` são para clones novos. Não executar sobre checkout com WIP.
A worktree `mt5x-recovery` mantém o roteiro/verificador disponível enquanto o
checkout de execução conserva a referência aceita. Para desenvolver, criar uma
branch `codex/...` a partir da referência escolhida e registrar a nova base.

Para recuperar uma referência local antiga, conferir o SHA no inventário JSON e
criar um nome sob `archive/local/...` em vez de substituir master/main. Os antigos
master/main locais eram atrasados. Para inspecionar WIP remoto, criar uma worktree
separada a partir do snapshot correspondente. Nada disso equivale a integração.

## Conferir e restaurar o pacote privado

O perfil explícito `source_development_v1` exige três categorias:

| Categoria | Conteúdo efetivamente salvo no SD |
|---|---|
| database | snapshot SQLite consistente, dados preparados, presets, históricos e journal; 96 MB comprimidos |
| project_state | configuração efetiva e preferências; `.env` somente se existisse no corte |
| git_and_context | três bundles, cinco WIPs reais, mapas de refs, repasse, helpers e recibos essenciais |

O banco é útil para agilidade: preserva 43 tabelas, 381 runs, 18 revisões de presets,
8 manifests de datasets, dados preparados e o journal de experimentos. Uma nova
extração continua possível. Não é necessário transportar cada exportação ou cache.
O contexto compacto inclui os arquivos do nível principal do repasse do integrador
(JSON/Markdown/scripts/diagnósticos/capturas), o estado de sincronização e os patches
WIP. Helpers arquivados têm caminhos/SHAs antigos e exigem revisão; não executar
merge/ativação/sondagem automaticamente ao restaurar.

Por orientação explícita do proprietário, não foram migrados Wine/MT5, binários
repetidos, `.venv`, node_modules, bytecode, caches pytest/provedor, exportações
regeneráveis, transcrições brutas dos chats nem logs extensos aninhados de execução.
Não alegar preservação integral dos artefatos nativos históricos ou replay literal
de dados externos baixados novamente. Os recibos principais foram mantidos; os
fontes permitem recompilar. Caminhos antigos no banco podem apontar para artefatos
omitidos: o registro persiste, o arquivo deve ser recriado ou requalificado.

Manifestos antigos sem `preservation_profile` continuam no perfil `full_state_v1`,
que exige também `native_evidence` e `toolchain`; esses requisitos não foram
relaxados. O perfil reduzido é uma escolha explícita de escopo. Nenhum dos dois
perfis transforma hash de arquivo em prova de conteúdo sem o ensaio de restore.

O verificador usa só a biblioteca padrão e pode rodar antes de instalar o app:

```bash
python3 mt5x-recovery/scripts/verify_recovery.py \
  --backup-dir /destino/privado/do/pacote \
  --manifest /destino/privado/do/pacote/recovery-manifest.json
```

No Windows, usar `python` no lugar de `python3` e caminhos locais adequados. Para extrair os arquivos `.tar.zst`, obter também ferramentas `tar`/`zstd` compatíveis com o novo sistema; a conferência de hashes não depende delas. O
verificador recusa pacote incompleto, categoria ausente, arquivo repetido,
caminho externo, tamanho ou hash divergente. **Ele não extrai, não autentica a
origem do manifesto e não comprova sozinho mídia externa ou restauração.**

Listar os arquivos compactados antes de extrair. Extrair um pacote confiável para
uma pasta nova e vazia, mantendo o original intocado. O snapshot principal está
em `data/database.tar.zst`, como `owner-database.sqlite`; conferir SHA e `quick_check`
antes de colocá-lo na pasta `database/` do checkout novo. Se já houver outro banco
na instalação, não sobrescrevê-lo: fazer o ensaio em outro diretório.

Reabrir primeiro uma cópia de ensaio. Conferir tabelas/contagens, Default/Manual,
históricos positivos e negativos, pedidos do journal e flags false. Não recalcular
históricos para esconder uma diferença. Preservar as evidências de falha.

Os registros antigos podem conter caminhos absolutos. O mapa privado registra o
layout de origem. Preferir o mesmo layout ao restaurar inicialmente; se mudar,
reconfigurar os caminhos e testar cada consumidor de artefatos. Não reescrever
IDs, hashes, books ou parâmetros financeiros para fazer um registro antigo passar.
Caches antigos não ganham elegibilidade por serem copiados para a nova máquina.

## Instalar ambiente e revalidar no sistema novo

Recriar `.venv`; não copiar o ambiente antigo. O [inventário de versões](AMBIENTE_REFERENCIA_20261021.json)
e as [restrições Linux observadas](constraints-linux-20261021.txt) ajudam a reproduzir
o ambiente, sem alterar as dependências oficiais do projeto. Usar a lista de
restrições como `-c`, não como lista de novos pacotes a instalar. Ela foi medida no
Linux3.11 e não qualifica Windows ou outro conjunto de wheels.

Linux, a partir do checkout novo `mt5x` e com `RECOVERY` apontando para a worktree
`mt5x-recovery`:

```bash
RECOVERY=../mt5x-recovery
./install.sh --yes --no-mt5
.venv/bin/python -m pip install -r requirements-dev.txt \
  -c "$RECOVERY/docs/repasse/constraints-linux-20261021.txt"
.venv/bin/python -m pytest -q
.venv/bin/python -m mt5_extracao.bootstrap doctor --no-probe
```

Windows: `install.bat -Yes -NoMT5`, depois
`.venv\Scripts\python.exe -m pip install -r requirements-dev.txt`,
`.venv\Scripts\python.exe -m pytest -q` e
`.venv\Scripts\python.exe -m mt5_extracao.bootstrap doctor --no-probe`.
Não aplicar automaticamente as restrições Linux no Windows. Registrar a versão
real do Python/pacotes e os resultados daquele sistema.

O instalador pode precisar de acesso à rede e permissões do sistema. Ele é o dono
do bootstrap; não montar outro instalador paralelo. O primeiro doctor sem terminal
instalado pode informar justamente essa ausência; isso não é aceite do MT5. Falhas
reais de dependência/caminho devem ser resolvidas antes do teste correspondente.
Os testes do HADOUKEN em `Tools/tests` precisam também do compilador C++ usado nos
shims; seguir suas instruções e `Tools/check_repo.py`, sem exigir terminal real.

Criar configuração para o novo host e comparar com a cópia privada antiga.
Migrar escolhas e credenciais para os locais suportados, corrigindo caminhos pelo
fluxo `configure` existente. Não restaurar um `config.ini` antigo sem conferir os
caminhos. Manter `allow_real=false`; não imprimir credenciais. Regenerar atalhos
pelo instalador, sem executar cegamente um `.desktop` antigo com caminho inválido.

MT5/Wine é uma etapa posterior: instalar a plataforma em uma área nova, entrar em
DEMO com negociação automática desativada e conferir conta/servidor/build/símbolos.
A toolchain não foi transportada no pacote reduzido. Não iniciar perfis,
EAs, `terminal64.exe` ou `launch.bat` arquivados antes dessas conferências.
Instalar Safety/headers como conjunto e compilar a partir das fontes/pins aceitos.
Compilar não comprova Expert ou `.set` efetivamente carregado.

Não restaurar locks, `native-active.json`, PIDs, tokens de posse da ponte ou links
de release apontando para o host antigo. O lease e o registro de contexto existentes
devem criar o estado novo. Não inventar um segundo lock nem reiniciar processos de
terceiros para investigar. Uma sincronização nova só é evidência de seu contexto;
não transforma testes sintéticos em aceite de execução.

## Prompt de continuidade para o próximo agente

> Leia AGENTS.md, este roteiro, RECUPERACAO_ESTADO_20261021.json e o repasse da
> integração. Confirme os três clones e os pins; localize o pacote privado e seu
> recibo de cópia externa. O SD preserva o complemento de desenvolvimento; os
> componentes regeneráveis omitidos estão explícitos, sem promessa de imagem integral.
> Recrie dependências e configuração do novo SO, confira a cópia dos dados em uma
> pasta de ensaio e reabra os registros existentes antes de escrever no banco.
> Preserve snapshots WIP sem mesclá-los indiscriminadamente. App172–177 tem uma fila
> serial própria; HADOUKEN49–60 tem consumidores/contratos e aceites pendentes;61 registra sua continuidade.
> Atualize os checks das pontas exatas, sem GO novo para ações já autorizadas e sem
> contornar verificações obrigatórias. Não opere REAL, eleve limites ou repita a
> sondagem de sincronização sem nova pré-condição. Nenhum teste no Linux antigo
> substitui o aceite do novo SO, Windows físico, Tester, replay ou DEMO operacional.
