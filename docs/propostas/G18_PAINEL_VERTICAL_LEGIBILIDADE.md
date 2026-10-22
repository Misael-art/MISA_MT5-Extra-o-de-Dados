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
  código6444f306 e documentação94d9ab88, baseec550d77.
- Safety: [PR8](https://github.com/Misael-art/mt5x-safety/pull/8), versão
  candidata1.9.0, contrato1, manifesto/hashes e source_revision coerentes.
- Framework: baseline77; final81 testes/2 subtestes; check_repo145 arquivos.
  Coleta real via shimC++ cobre25 posições/limite16, outro magic, símbolos
  diferentes e fechamento. Geometria cobre96/144/192 DPI.
- Compilação Wine de16 fontes (painel, sentinela, doze robôs e dois QAs):
  zero erros/avisos. SafetySmoke também compila sem erros/avisos.
- QA nativo offline, sem conta/credenciais/executor:159 verificações sem
  falhas em cada execução96/144 DPI. Capturas e roteiro versionados no
  framework: Documentation/HADOUKEN_2/G18_ORIENTACAO_LEGIBILIDADE.md.
  Fixtures sintéticas não certificam operação financeira/Tester do servidor.
- Safety:2 testes de manifesto/dependências; SDK/App171:14 testes dirigidos.
  Doctor local:16 OK,2 avisos (pandas-ta opcional/ponte desativada),0 falhas.

## Aceite para integração/instalação

- [x] Implementar os quatro pontos e manter contratos/execução/risco.
- [x] Testes dirigidos, compilações e QA offline96/144 DPI.
- [ ] Revisão dos três PRs e checks da ponta exata.
- [ ] Nova paridade dos seis robôs, exigida pela regra de versão Safety.
- [ ] Replay visual operacional no Tester com posições reais do livro.
- [ ] Instalar a versão identificada, com backup/recibo e conferência no MT5
      do proprietário; preservar a fila App172–177 e os pins aceitos.
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
