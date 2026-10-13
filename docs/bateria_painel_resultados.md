# Bateria de testes do painel de resultados (2026-10-13)

Painel vivo (`mt5x panel --serve`) testado no Chrome, no host Linux, com a base real (12 ativos, 15 robôs e
estratégias, 66 combinações). O painel do HADOUKEN no MT5 tem o relatório dele em
`Documentation/HADOUKEN_2/TESTES_PAINEL_V7.md`, no repositório HADOUKEN.

## O que passou

| Área | O que foi feito | Resultado |
|---|---|---|
| Carga | abrir a página | 0,5 s até carregar; 74 KB; nenhum arquivo externo; console sem erros |
| Dados | `/api/matrix` | 0,17 s por consulta, uma a cada 10 s |
| Segurança | sem chave, chave errada, `Host` forjado, caminho inexistente, `../` | 403 ou 404 em todos; a página não pode ser embutida em outra |
| Visões | "Por ativo" e "Por robô" | trocam e atualizam o endereço (dá para voltar e compartilhar) |
| Filtros | robô, número em destaque, filtro de ativo | funcionam; o número em destaque muda todas as células |
| Tema | claro e escuro | os dois legíveis |
| Ações | "Testar o que falta" | rodou, mostrou o progresso e o resultado linha a linha; a matriz se atualizou sozinha |
| Acessibilidade | idioma, rótulos e títulos | `lang="pt-BR"`; todos os campos com rótulo; h1, h2, h3 em ordem |

## Defeitos achados e corrigidos neste PR

1. **Clicar numa célula parecia não fazer nada.** O detalhe abre abaixo da tabela, fora da tela (a 1.557 px em uma
   janela de 962). Agora a página rola até ele.
2. **Filtro sem resultado dizia "Nenhum ativo com base. Extraia dados com: mt5x extract"**, como se a base
   estivesse vazia. Agora diz que nada combina com o filtro.
3. **"HDK_Squeeze (1 testes)"**: plural errado.
4. **O tema escolhido não era lembrado** ao recarregar. Agora fica gravado no navegador.

Os quatro foram conferidos de novo no navegador depois da correção.

## Achados que ficaram abertos

- **Resultado abaixo de −100%** (até −316,0% em ACR M1, −147,9% em EURUSD M1). O backtester Python não confere
  margem: com um stop muito curto, o lote para arriscar 1% fica maior do que o capital sustenta, e um gap além do
  stop perde muitas vezes o risco previsto. Depois que o capital fica negativo ele para de operar, mas o estrago já
  está feito. É o mesmo ponto que apareceu na T8.9 (ORB), onde a camada de segurança do robô bloqueou as entradas
  por margem e o Python não. Todas as células assim estão marcadas "não confiável" ou são M1. **É defeito do
  backtester, não do painel; registrado como pendência, sem tarefa ainda: limitar o lote pela margem.**
- **O `HDK_AccountGuardian` aparece na lista de robôs**, com "0 testadas". Ele é um sentinela, não opera.
- **Em tela larga o conteúdo ocupa uma coluna de 1.280 px** e o resto fica vazio. É escolha de layout, não defeito.
- **"Origem do número: backtester Python (otimista em custo)"** aparece mesmo em ativo com o spread calibrado
  (T10.12). O texto é fixo.

## O que não foi testado

- Largura de celular e de tablet: a janela não pôde ser redimensionada no ambiente de teste. O CSS tem uma regra
  para até 640 px, não vista funcionando.
- O aviso de "ocupado" ao pedir uma segunda ação com outra em andamento: a primeira terminou rápido demais.
- As ações que usam o MT5 pelo navegador (atualizar a base, gravar especificações, teste de robô MQL5) e o
  PARAR TUDO pelo botão. Elas têm testes automáticos e foram conferidas na T11.10 pela chamada direta.
- Teclado e leitor de tela.
- Firefox e Safari.

## Painel do HADOUKEN no MT5

Na mesma rodada o painel v7 ganhou disposições compacta, normal e ampla, escolhidas pelo tamanho do gráfico, e as
correções da bateria dele. Relatório e PR no repositório HADOUKEN (`Documentation/HADOUKEN_2/TESTES_PAINEL_V7.md`,
PR #4). Um achado de lá que é do mt5x: **cada início pelo arquivo de início acrescenta um gráfico ao perfil do
terminal** (havia cinco EURUSD M15 no fim dos testes) e **recompilar um robô não recarrega o que já está no
gráfico**; os dois ficam como pendência da T8.7.
