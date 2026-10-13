# Tutorial: do zero a um robô conferido

Você vai criar um robô MQL5 sobre a biblioteca de segurança, escrever um sinal, compilar, testar no Strategy
Tester e passar pela bateria de segurança. No fim, um atalho: gerar o robô a partir de uma estratégia declarada e
provar que ele faz as mesmas operações que a versão em Python.

> **O exemplo é didático, não é uma estratégia para operar.** O cruzamento de médias usado aqui **perde dinheiro**
> no teste e é reprovado pela bateria. É de propósito: você vê o ciclo completo e vê a bateria fazendo o papel dela.

As saídas abaixo são reais: foram coladas de uma execução em 2026-10-12 (Linux, MetaTrader 5 build 6258 no Wine,
conta MetaQuotes-Demo). Só os caminhos pessoais foram encurtados (`~/robos`, `<pasta do MT5>`). Os seus números
vão variar com a corretora e a data. Nada disto foi validado no Windows.

## Antes de começar

1. Instale o programa (`./install.sh`) e confira com `mt5x doctor`.
2. Clone a biblioteca de segurança e aponte a configuração para ela, em `config/config.ini`:

   ```ini
   [ROBOTS]
   safety_lib_dir = ~/Projects/mt5x-safety
   ```

3. Para o passo da paridade (no fim), tenha as barras do ativo na base:

   ```bash
   mt5x extract --symbols EURUSD --tf H1 --from 2025-10-01 --to 2026-09-30
   mt5x specs --symbols EURUSD
   ```

O Strategy Tester só roda com o MetaTrader 5 **fechado**. Se ele estiver aberto, os comandos de teste param e
explicam; com `--close-terminal` eles fecham o terminal desta instalação, testam e religam a ponte.

## 1. Criar o robô

```text
$ mt5x robots new MeuRobo --dir ~/robos
Robô MeuRobo criado:
  ~/robos/Experts/MeuRobo/MeuRobo.mq5
  ~/robos/Experts/MeuRobo/MeuRobo.robot.json
  ~/robos/Experts/MeuRobo/README.md
  ~/robos/Presets/MeuRobo_Conservador.set
A pasta ~/robos foi adicionada ao catálogo.
Agora escreva o sinal em MeuRobo.mq5 (veja o README ao lado) e rode: mt5x robots compile meurobo
```

O esqueleto já traz o risco por operação, o stop pelo ATR, os limites de perda, a parada de emergência e a
exportação das operações. Ele **não opera** até você escrever o sinal.

*Erro comum:* `Já existe um robô chamado ...`. Escolha outro nome; o programa não sobrescreve.

## 2. Escrever o sinal

Abra `~/robos/Experts/MeuRobo/MeuRobo.mq5`. A estratégia entra em quatro blocos marcados. Para um cruzamento de
médias simples:

```mql5
//--- MT5X:PARAMETROS:INICIO (parâmetros da sua estratégia)
input group "5. Estratégia"
input int    InpFastPeriod      = 10;    // Média rápida
input int    InpSlowPeriod      = 40;    // Média lenta
//--- MT5X:PARAMETROS:FIM

//--- MT5X:GLOBAIS:INICIO (handles de indicadores da sua estratégia)
int g_fast = INVALID_HANDLE;
int g_slow = INVALID_HANDLE;
//--- MT5X:GLOBAIS:FIM

bool SignalInit()
{
//--- MT5X:INIT:INICIO
   g_fast = iMA(_Symbol, PERIOD_CURRENT, InpFastPeriod, 0, MODE_SMA, PRICE_CLOSE);
   g_slow = iMA(_Symbol, PERIOD_CURRENT, InpSlowPeriod, 0, MODE_SMA, PRICE_CLOSE);
   return (g_fast != INVALID_HANDLE && g_slow != INVALID_HANDLE);
//--- MT5X:INIT:FIM
}

int Signal()
{
//--- MT5X:SINAL:INICIO
   double fast[2], slow[2];                       // [0] = penúltima barra fechada, [1] = última
   if(CopyBuffer(g_fast, 0, 1, 2, fast) != 2 || CopyBuffer(g_slow, 0, 1, 2, slow) != 2) return 0;
   if(fast[1] > slow[1] && fast[0] <= slow[0]) return 1;    // cruzou para cima: comprar
   if(fast[1] < slow[1] && fast[0] >= slow[0]) return -1;   // cruzou para baixo: vender
   return 0;
//--- MT5X:SINAL:FIM
}
```

Três regras:

- **Só barras fechadas.** O terceiro argumento de `CopyBuffer` é 1, nunca 0: a barra 0 ainda está se formando e
  um sinal sobre ela muda de ideia.
- **Não envie ordens.** Nada de `OrderSend` nem `CTrade`: devolva +1, −1 ou 0. Quem envia é o esqueleto, pela
  biblioteca de segurança, sempre com stop. A bateria reprova ordem fora dela.
- **Liste os parâmetros da estratégia em `tunable`**, no `MeuRobo.robot.json`, para a bateria completa variá-los.

O stop é um nível fixado no fechamento da barra do sinal (fechamento ∓ ATR × `InpStopAtrMult`); o lote e o alvo
usam a distância real, do preço de entrada até esse nível.

## 3. Conferir o contrato

```text
$ mt5x robots validate meurobo
MeuRobo: contrato 1 válido.
```

*Erro comum:* `inputs.magic: o robô não tem o parâmetro '...'`. Você renomeou um parâmetro no fonte e não no
`robot.json`. O contrato está em [contrato.md](contrato.md).

## 4. Instalar e compilar

```text
$ mt5x robots compile meurobo
  biblioteca de segurança 1.0.0 instalada na pasta do MT5
  copiado  <pasta do MT5>/MQL5/Experts/MeuRobo/MeuRobo.mq5
  copiado  <pasta do MT5>/MQL5/Presets/MeuRobo_Conservador.set
MeuRobo: Compilado sem erros (0 aviso(s)): MeuRobo.ex5
```

O comando copia o robô, os presets e a biblioteca de segurança para a pasta do MetaTrader 5 e compila no
MetaEditor. A sua pasta `~/robos` não é alterada.

*Erro comum:* erros de compilação aparecem com arquivo, linha e coluna, por exemplo
`MeuRobo.mq5(61,4): erro 256: undeclared identifier 'g_fast'` (faltou o bloco GLOBAIS).

## 5. Backtest no Strategy Tester

```text
$ mt5x robots backtest meurobo --symbol EURUSD --tf H1 --from 2025-10-03 --to 2026-09-30 --close-terminal
Fechando o MetaTrader 5 para rodar o teste...
MeuRobo em EURUSD H1 pelo Strategy Tester (2025-10-03 a 2026-09-30, modelo: cada tick com ticks reais)
  Operações: 120   Acerto: 30,8%   Profit factor: 0,86   Resultado médio: -48,72 por operação
  Resultado: -5846,71 (-5,8%)   Queda máxima: 10,0%
  Operações lidas de: HDK_trades.csv. Relatório completo: <pasta do MT5>/mt5x_meurobo.htm
Execução gravada (aparece no card do robô). Um backtest isolado não valida o robô.
```

É uma simulação: nenhuma ordem vai para a sua conta. O modelo padrão é o de ticks reais, o mais fiel. Sem
`--from` e `--to`, vale o último ano.

*Erros comuns:* `O código de MeuRobo mudou desde a última compilação` (você editou o fonte: compile de novo);
`O MetaTrader 5 está aberto` (feche-o ou use `--close-terminal`).

## 6. Bateria de segurança

```text
$ mt5x robots battery meurobo
Bateria de segurança de MeuRobo

Estática: aprovada
  ✓ Contrato do robô válido: contrato 1
  ✓ Compila sem erros: compilado no MetaEditor
  ✓ Arquivos em UTF-8 ou UTF-16: 1 arquivo(s) conferido(s)
  ✓ Stop obrigatório: o stop é sempre definido
  ⚠ Sem martingale, grid ou preço médio: não encontrei parâmetro de martingale, grid ou preço médio
  ✓ Toda ordem passa pela camada de segurança: o robô inclui a biblioteca de segurança e não envia ordem por fora
  ✓ Biblioteca de segurança na versão declarada: mt5x-safety 1.0.0
  ✓ Sem DLL externa: nenhum #import de DLL
  ✓ Magic number exclusivo: magic 107964
  ✓ Risco e limites dentro dos tetos: 0,50% por operação (teto 1,00%); limite diário 2,00%; drawdown máximo 10,00% (teto 20,00%)

Estatística: reprovada
  ✓ Há um backtest gravado: EURUSD H1, 2025-10-03 a 2026-09-30
  ✓ Backtest do código atual: o código testado é o que está no disco
  ✓ Backtest com a mesma configuração avaliada: padrão do código
  ✓ Backtest em modelo por tick: modelo: cada tick com ticks reais
  ✓ Toda entrada do teste tem stop: 120 entradas, todas com stop na ordem
  ✗ Pelo menos 200 operações: 120 operações
      O que fazer: Teste um período maior ou um timeframe menor.
  ✗ Profit factor acima de 1,30: profit factor 0,86
      O que fazer: O que o robô ganha não cobre o que perde com folga: não opere este ajuste.
  ✗ Expectativa positiva com custos: -48,72 por operação, em média
      O que fazer: Na média cada operação perde dinheiro.
  ⚠ Drawdown de Monte Carlo até 20%: p95 19,0% · p99 22,3% em 1000 simulações
      O que fazer: Reduza o risco por operação: embaralhando as operações, a queda passa do limite.
  ✓ Chance de ruína desprezível: 0,0% das simulações perdem metade do capital (máximo 1%)
  ✗ Resultado se mantém no fim do período: profit factor 0,56 nas últimas 36 operações (30% finais do teste)
      O que fazer: O robô parou de funcionar na parte mais recente do teste.
  ⚠ Robustez com parâmetros ±20%: não avaliado nesta versão para robôs MQL5 (exige repetir o Strategy Tester com os parâmetros vizinhos)
      O que fazer: Repita o backtest com --param mudando cada parâmetro em ±20% e compare.
  ✗ Aguenta o dobro do spread: profit factor 0,81 com um spread a mais por operação (8 pontos)
      O que fazer: O lucro some com um custo um pouco maior: margem pequena demais.
  ⚠ Sequência de perdas suportável: maior sequência: 12 perdas seguidas
      O que fazer: Confira se você aguenta essa sequência sem desligar o robô.

Reprovada: corrija os itens marcados com ✗ e rode a bateria de novo.
```

A parte de código passou: há stop em toda entrada (conferido no relatório do teste, não só no código), as ordens
passam pela biblioteca e os limites estão dentro dos tetos. A parte estatística **reprovou**, como devia: poucas
operações, o robô perde na média e não aguenta um custo um pouco maior. Um ✗ reprova; um ⚠ não, mas leia cada um.

Para medir de verdade a robustez e o "fora da amostra", rode `mt5x robots battery meurobo --deep`: ele repete o
Strategy Tester com cada parâmetro de `tunable` 20% abaixo e acima, e nas partes inicial e final do período.

## 7. Tudo de uma vez

Depois da primeira volta, um comando só faz contrato, compilação, backtest e bateria, e serve para CI:

```text
$ mt5x robots check meurobo --symbol EURUSD --tf H1 --from 2025-10-03 --to 2026-09-30 --report relatorio.xml
Conferência de MeuRobo:
  ✓ Contrato do robô (passou, 0,0 s): contrato 1
  ✓ Instalar e compilar (passou, 3,7 s): Compilado sem erros (0 aviso(s)): MeuRobo.ex5
  ✓ Backtest no Strategy Tester (passou, 29,4 s): 120 operações, profit factor 0,86
  ✗ Bateria de segurança (falhou, 0,0 s): reprovou em: Pelo menos 200 operações; Profit factor acima de 1,30; Expectativa positiva com custos; Resultado se mantém no fim do período; Aguenta o dobro do spread
Relatório JUnit: relatorio.xml
Há etapa reprovada: corrija e rode de novo.
```

O código de saída é 0 só quando tudo passa (aqui foi 1). O `relatorio.xml` é JUnit: esta execução gerou 28 casos,
6 com falha. Com `--json`, a saída é um único objeto com as etapas e a bateria.

## 8. Atalho: gerar o robô de uma declaração e provar a paridade

Se a regra de entrada cabe em indicadores e condições simples, escreva-a como dados e deixe o programa gerar o
sinal MQL5. A mesma declaração roda no backtester Python. O arquivo [exemplo/cruzamento.json](exemplo/cruzamento.json):

```json
{
  "key": "cruzamento",
  "name": "Cruzamento de médias",
  "indicators": {"rapida": {"kind": "sma", "period": 10}, "lenta": {"kind": "sma", "period": 40}},
  "long": [["cross_above", "rapida", "lenta"]],
  "short": [["cross_below", "rapida", "lenta"]],
  "atr_period": 14, "stop_atr": 1.5, "target_r": 2.0
}
```

Indicadores aceitos: `sma`, `ema` e `rsi` sobre o fechamento. Condições: `cross_above`, `cross_below`,
`above` e `below`, entre indicadores, `close` ou números; todas as de um lado precisam valer. Não é um tradutor
de Python: o que não cabe na declaração você escreve à mão, como no passo 2.

```text
$ mt5x robots generate docs/sdk/exemplo/cruzamento.json --name ExemploCruzamento --dir docs/sdk/exemplo
Robô ExemploCruzamento gerado a partir da declaração:
  docs/sdk/exemplo/Experts/ExemploCruzamento/ExemploCruzamento.mq5
  docs/sdk/exemplo/Experts/ExemploCruzamento/ExemploCruzamento.robot.json
  docs/sdk/exemplo/Experts/ExemploCruzamento/README.md
  docs/sdk/exemplo/Presets/ExemploCruzamento_Conservador.set
  docs/sdk/exemplo/Experts/ExemploCruzamento/ExemploCruzamento.signal.json
A pasta docs/sdk/exemplo foi adicionada ao catálogo.
Compile e prove a paridade com a declaração: mt5x robots compile exemplocruzamento && mt5x robots parity exemplocruzamento --symbol <ativo> --tf <timeframe>
```

```text
$ mt5x robots compile exemplocruzamento
  biblioteca de segurança 1.0.0 instalada na pasta do MT5
  copiado  <pasta do MT5>/MQL5/Experts/ExemploCruzamento/ExemploCruzamento.mq5
  copiado  <pasta do MT5>/MQL5/Presets/ExemploCruzamento_Conservador.set
ExemploCruzamento: Compilado sem erros (0 aviso(s)): ExemploCruzamento.ex5
```

```text
$ mt5x robots parity exemplocruzamento --symbol EURUSD --tf H1
Paridade de ExemploCruzamento (MQL5) com Cruzamento de médias (Python), EURUSD H1:
  Operações: 144 no Python, 144 no MQL5; 141 casadas (97,9%; mínimo 95,0%), 141 com o mesmo sinal de resultado
  Par a par: 136 de 141 com resultado até 5,0% um do outro (96,5%; mínimo 95,0%); mediana da razão MQL5/Python 1,000 (de 0,98 a 1,02)
  Resultado: -9138,67 no Python e -10255,38 no MQL5; a diferença é 0,6% do giro bruto (máximo 1,0%) e 12,2% do resultado líquido (só informativo)
  Modelo do Tester: cada tick. A paridade compara a lógica; o custo com ticks reais é maior do que o do backtester Python (veja docs/sdk/exatidao.md).
  Os dois lados começam a operar em 2025-10-03 00:00, com o mesmo capital (as barras anteriores só aquecem os indicadores do Python).
  Só no Python (3): 2025-10-24 15:00, 2026-05-06 02:00, 2026-07-06 11:00
  Só no MQL5 (3): 2025-10-27 14:04, 2026-05-06 07:30, 2026-07-06 12:49
Paridade dentro do critério.
```

A paridade diz que o robô gerado faz as **mesmas operações** que a declaração em Python (mesmas entradas, mesmo
sinal de resultado, valores próximos). Ela **não** diz que o Python acerta o custo: com ticks reais o mesmo robô
perde mais do que no backtester Python. Leia [exatidao.md](exatidao.md) antes de confiar em qualquer número.

O robô gerado neste passo é o que está em [exemplo/](exemplo/).

## 9. Conta de demonstração

Só depois de a bateria aprovar: `mt5x robots start <robô> --symbol <ativo> --tf <timeframe> --close-terminal`. O
programa recusa conta real. **Esta parte ainda não foi validada com o MetaTrader 5 real**: confira no próprio MT5
se o robô entrou no gráfico. Veja [robos.md](../robos.md), "Rodar em conta de demonstração".

## E agora

- Referência do contrato: [contrato.md](contrato.md).
- Usar tudo isso de dentro de um programa Python: [api.md](api.md).
- O que está provado e o que ainda é lacuna: [exatidao.md](exatidao.md).
