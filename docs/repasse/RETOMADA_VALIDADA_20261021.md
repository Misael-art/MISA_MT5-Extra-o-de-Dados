# Continuidade validada no host encontrado — 21/10/2026

## Ambiente efetivamente encontrado

BigLinux baseado em Manjaro, kernel Linux 6.18.49-1, x86_64. Python do sistema
3.14.7; ambiente de desenvolvimento preservado Python 3.11.15. O host mantém
checkouts antigos, ambiente virtual, Wine, instalação e dados. **Não há evidência
de reinstalação do SO neste atendimento.** A validação abaixo é do host encontrado.

Foram criados três clones independentes pela rede, todos com `git fsck --full`
aprovado. A branch de recuperação ficou acessível numa worktree separada:

| Componente | Branch/base recuperada | SHA |
|---|---|---|
| App | master | ac7af2c392611bb1bd262c7806810eb4289559b3 |
| HADOUKEN | master | ec550d77caae74e9f5f4ffe30cea5ccd29621624 |
| Safety | master, versão 1.8.0 | 3988df13c467bd22941183f5546caad0500f9138 |
| Repasse App | codex/recuperacao-pos-formatacao-20261021 | 4516321882f206395ea4da1476dfdf5c03cd7a8a |

Os links ativos continuam nas três referências aceitas. Os checkouts antigos
com WIP foram preservados. Nenhum snapshot WIP foi aplicado à base nova.

Como o ambiente existente está funcional, foi reutilizado por link local, sem
reinstalar dependências nem sobrescrever atalhos/configuração. Isso segue a
orientação de evidência proporcional do [repasse](INTEGRACAO_20261021.md).
Uma futura reinstalação exige executar novamente os instaladores oficiais.

## SD e restauração de ensaio

A mídia foi localizada pelo inventário do sistema: partição SD separada do NVMe.
Nenhum caminho de montagem foi fixado no código ou na configuração. O pacote
original `Backups/MT5X/2026-10-21-desenvolvimento` foi mantido intacto.

- SHA-256 do manifesto: `d5ce7216b223b03dd6f4a316571f7c5583753b3545e9854b97590ea344c39fb9`.
- Verificador do commit publicado: 18 arquivos conferidos, perfil
  `source_development_v1`, três categorias válidas.
- Arquivo compactado listado antes da extração; extração em pasta nova e vazia.
- SQLite restaurado: SHA-256
  `98af98df1c8a1123690b37fa40dee0bf5f412c566888928b1fae5109d9536c83`,
  `quick_check=ok`, 43 tabelas e todas as contagens iguais ao snapshot.
- Store da App do clone novo reabriu o histórico composto com 3 trades e o
  journal com 2 pedidos/9 eventos. Conexão em leitura; hash final inalterado.
- Sem novo backtest, acesso de reserva ou alteração de IDs, livros e parâmetros.
  Qualificação estatística, livro qualificado e holdout final continuam false.

Configuração privada existente conferida pelo doctor em leitura. Seus caminhos
de terminal, Python Wine e pasta de banco existem no host atual; não houve
necessidade de migração. Dados, credenciais, configuração e recibos privados
permanecem fora do Git. A cópia descompactada de ensaio pode ser removida depois
do recibo, pois o pacote original e o banco do proprietário continuam preservados.

## Verificação e fila remota

Baseline sobre o clone App aceito: **56 testes aprovados**, cobrindo controles,
modo oculto, UI dos controles e documentação SDK. Doctor `--no-probe` com raiz
explícita: **20 OK / 1 aviso opcional pandas-ta / 0 falhas**. Verificador/SDK após registro:
**28 testes aprovados**. A primeira chamada
posicionou `--root` depois do subcomando e foi recusada pelo parser; corrigida
sem alterar o programa. O primeiro script de consulta chamou `journal`, nome
inexistente; a consulta correta `experiments.store.status` passou em leitura.

Consulta GitHub desta retomada, sem merges nem reruns:

| Fila | Estado observado |
|---|---|
| App172→173→174→175→176 | Abertas, seis checks obrigatórios aprovados em cada ponta |
| App177 / 4ab87025 | Aberta, Windows falhou; cinco checks aprovados |
| App178 / a6f5dc2d | Aberta, Windows falhou; cinco checks aprovados |
| App179 / 45163218 | Draft aberta; seis checks obrigatórios aprovados |
| HADOUKEN49–61 | Abertas; não integradas por esta retomada |

As falhas Windows de 177/178 já estão documentadas no repasse; não foram
reclassificadas nem houve campanha de repetição. Consultar a ponta atual antes
de integrar. Preservar ordem serial e composição semântica das fixtures nativas.

## Próximo recorte autorizado

O proprietário priorizou corrigir a exibição do painel de gestão ao iniciar
REPLAY. Investigar a sessão visual e o Expert efetivamente selecionado, reproduzir
a lacuna e entregar correção isolada com regressão. Aceite: janela/painel acessível,
controles manuais preservados, backtest discreto mantido e encerramento restrito
à própria sessão. Windows físico e execução nativa exigem prova específica.

Registrar em PR documental separado a ordenação crescente de TFs, opção Todos
no backtest, seleção de vários ativos com orçamento do host e visão gráfica da
mesma estratégia/revisão de preset por TF. Reutilizar fila, orçamento, matriz e
histórico existentes. Não libera seletores estatísticos ou operação REAL.

Gestão de disco: reutilizar um ambiente, evitar cópias de Wine/cache e retirar
somente temporários próprios após conferência. Publicar cada avanço coerente
por commit/push/PR, mantendo dados privados e WIP do proprietário preservados.
