# G-18 — painel vertical e legibilidade

Registro do pedido de 2026-10-21. Uma tarefa por PR, com implementação no
framework e distribuição correspondente; a App recebe o registro e o aceite.

## Requisitos e entrega

1. Horizontal preservado; vertical de 360 unidades contra 380/460 do
   horizontal normal/amplo. Abas Operar, Risco, Cestas, Análise e Visual à
   esquerda; cores, hierarquia, módulos e ações existentes preservados.
   Botão em Visual e ícone geométrico no cabeçalho, com tooltip e persistência
   por magic fora do Tester. Alternar só redesenha; não envia ordens.
2. Mostrar posição comprada/vendida, contagem de cada direção e total, com
   lotes e resultado aberto. Totais incluem todos os ativos do magic e são
   calculados antes do limite de 16 da lista. Hedging misto e vazio explícitos.
3. Painel em primeiro plano: linhas e setas no fundo; etiquetas fora do
   retângulo do painel após resize, arrasto, recolher/expandir e alternância.
4. Resultados verdes/vermelhos com altura/largura medidas pela fonte, padding
   por DPI e posicionamento dentro da área útil. Preservar resultado, sinal,
   unidade e livro. Sem espaço livre, omitir só a etiqueta, nunca o registro.

## Fontes e verificação

- HADOUKEN: [PR62](https://github.com/Misael-art/projeto-indicadores-mt5/pull/62),
  código2c475fc1, baseec550d77; inclui rolagem do corpo, rodapé fixo e
  margem do eixo temporal, considerando a origem do painel.
- Safety: [PR8](https://github.com/Misael-art/mt5x-safety/pull/8), versão
  candidata1.9.0, contrato1, manifesto/hashes e source_revision coerentes.
- Framework: baseline77; final83 testes/2 subtestes; check_repo145 arquivos.
  Coleta real via shimC++ cobre25 posições/limite16, outro magic, símbolos
  diferentes e fechamento. Geometria cobre96/144/192 DPI.
- Compilação Wine de16 fontes (painel, sentinela, doze robôs e dois QAs):
  zero erros/avisos. SafetySmoke também compila sem erros/avisos.
- QA nativo offline, sem conta/credenciais/executor:173 verificações sem
  falhas em cada execução96/144 DPI. Capturas e roteiro versionados no
  framework: Documentation/HADOUKEN_2/G18_ORIENTACAO_LEGIBILIDADE.md.
  Fixtures sintéticas não certificam operação financeira/Tester do servidor.
- Safety:2 testes de manifesto/dependências; SDK/App:24 testes dirigidos
  na revisão final (13 API/docs e11 biblioteca Safety).
  Doctor após ativar a prévia:20 OK,1 aviso opcional (pandas-ta),0 falhas.

## Aceite para integração/instalação

- [x] Implementar os quatro pontos e manter contratos/execução/risco.
- [x] Testes dirigidos, compilações e QA offline96/144 DPI.
- [ ] Revisão dos três PRs e checks da ponta exata.
- [ ] Nova paridade dos seis robôs, exigida pela regra de versão Safety.
- [ ] Replay visual operacional no Tester com posições reais do livro.
- [x] Instalar prévia identificada para teste pelo atalho habitual, com
      backup/recibo, catálogo e adaptador da App conferidos.
- [ ] Conferir o replay operacional no MT5 do proprietário e promover a
      release aceita somente após os aceites financeiros e revisão.
- [ ] Windows físico: duas orientações, temas, DPI125/150%, posições mistas,
      arrasto/minimização, resize, reabertura e etiquetas perto das bordas.

Não declarar o projeto todo concluído por entregar este recorte. O motor,
RiskGuard, SafeExecutor, estatísticas e livro mantêm seus arquivos; isso
não substitui a medição de paridade exigida para aceitar a distribuição.


## Baseline da App no checkout principal

Antes de alterar os documentos da App: instalador --yes --no-mt5 e
requirements-dev concluídos; suíte1437 aprovados,16 pulos,4 avisos e1 falha
em1702,03s. Falha: test_analysis_keeps_native_text_and_chart_size_through_resize
[1.3333333333333333], tests/test_ui_main_window.py:1177: PageDown não mudou
a posição vertical do viewport (0,0→0,0); stderr também registrou callbacks
Tk destruídos. Causa não demonstrada; não classificar como flake nem
atribuir às alterações MQL5. O checkout principal contém WIP anterior e
não representa o runtime App171 da árvore documental. Nenhum fonte Tk foi
modificado ou teste desativado para ocultar essa falha. O resultado não
valida como verde a suíte geral da App.

Reprodução isolada única no Xvfb1600×1100: o mesmo teste passou em10,45s.
O resultado inicial permanece preservado; não demonstra a causa nem torna
a suíte geral aprovada. Após os documentos,14 testes SDK passaram em3,07s
e doctor repetiu16 OK/2 avisos/0 falhas. Nenhum teste foi pulado/removido.

## Prévia disponibilizada em 2026-10-22

Por instrução do proprietário, os atalhos da Área de trabalho e menu abrem
a mesma prévia App7b2062d4ef32 já existente. Seus453 arquivos Python/shell
foram conferidos pelos hashes da release, sem alteração. HADOUKENc9561f5e
e Safetyacc23c4 foram exportados por Git para releases próprias, com424 e26
arquivos conferidos. Links `hadouken-development`/`safety-development`
selecionam a candidata; `app-current`/`hadouken-current`/`safety-current`
permanecem nas referências aceitas anteriores.

O instalador obrigatório --no-mt5 havia refeito os atalhos para o checkout
WIP e desativado a ponte. Corrigido: destino estável `app-development/run.sh`,
ponte reabilitada e somente as duas chaves de bibliotecas alteradas, sem
mudar parâmetros financeiros. Backup privado da configuração/atalhos e
manifesto da App acompanha `logs/g18-conclusao-20261021/preview/activation.json`.
Dados, preferências e ambiente virtual continuam reutilizados pelos mesmos
links. Nenhuma base foi copiada para publicar a prévia.

Quatorze binários compilados foram disponibilizados no MT5 em
`Experts/MT5X_Desenvolvimento_G18/HADOUKEN`. Fontes/binários originais do
terminal, incluindo cinco diferenças locais em relação à base aceita,
foram preservados. O Navigator permite testar a pasta identificada; pela
App, iniciar um NOVO replay usa as fontes da candidata. Uma sessão já aberta
mantém seu binário e a janela antiga da App precisa ser reaberta pelo atalho.

Ambos os desktops passaram desktop-file-validate, permissões/destinos foram
conferidos e a confiança do atalho foi marcada. A GUI da release instalada
abriu em display isolado/offline, catálogoG18 correto e configuração/
preferências inalteradas. A janela existente do proprietário foi preservada.
O adaptador real da App para BbRsi compilou e o header copiado no perfil
privado coincide com a release; contexto liberado sem processos restantes.
Isso verifica preparo/compilação, não o carregamento no Tester do servidor.

Revisão final do rodapé: o teste mais rigoroso registrou169 acertos/4 falhas
antes de corrigir o desconto da origem vertical. Recibo reprovado preservado;
após correção,173/0 em cada escala96/144 DPI, inclusive rodapé acima do eixo.
Uma sondagem privada de sincronização sem Experts não apresentou estágio de
autorização/sincronização; não houve nova campanha sem condição observável.
No último exame somente leitura, SDK sem terminal/conta inicializados. A
autenticação DEMO foi solicitada ao proprietário diretamente no MT5, sem
solicitar senha no chat. Replay conectado e nova paridade permanecem pendentes.
