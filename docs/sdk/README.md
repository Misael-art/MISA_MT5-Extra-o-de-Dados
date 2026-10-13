# SDK de robôs

Além de gerenciar robôs prontos, o `mt5x` ajuda a **escrever** robôs MQL5 para o MetaTrader 5 com um padrão alto:
toda ordem passa por uma biblioteca de segurança, e o que vai para uma conta é o que foi compilado, testado e
conferido.

| Você quer | Leia |
|---|---|
| Fazer o primeiro robô, passo a passo | [tutorial.md](tutorial.md) |
| Saber o que um robô precisa declarar | [contrato.md](contrato.md) |
| Usar o SDK de dentro de um programa Python | [api.md](api.md) |
| Saber o que está provado e o que ainda é lacuna | [exatidao.md](exatidao.md) |
| Ver um robô completo | [exemplo/](exemplo/) |

## O ciclo

```text
mt5x robots new       cria o esqueleto sobre a biblioteca de segurança (ou: generate, de uma estratégia declarada)
mt5x robots validate  confere o robot.json contra o contrato
mt5x robots compile   instala no MetaTrader 5 e compila no MetaEditor
mt5x robots backtest  roda o Strategy Tester (ticks reais) e grava as operações
mt5x robots battery   bateria de segurança: código, preset e estatística (--deep repete o Tester)
mt5x robots check     os quatro passos acima em um comando, com relatório JUnit para CI
mt5x robots parity    o robô faz as mesmas operações que a versão em Python?
```

## O que o SDK garante, e o que não

- **Garante:** o robô criado pelo esqueleto não envia ordem sem stop nem fora da biblioteca; a bateria só aprova o
  código e o preset que foram de fato testados, em modelo por tick; conta real é recusada.
- **Não garante lucro.** A bateria é o mínimo para rodar em conta de demonstração.
- **Não está provado:** o tempo real em demo nunca rodou em um terminal de verdade, a biblioteca de segurança só
  foi exercitada no Strategy Tester e nada foi validado no Windows. A lista completa está em
  [exatidao.md](exatidao.md).

## Peças

- Biblioteca de segurança MQL5: repositório [mt5x-safety](https://github.com/Misael-art/mt5x-safety), instalada
  pelo `mt5x` na versão que cada robô declara.
- O Python **nunca envia ordens**: quem opera é o robô, dentro do MetaTrader 5.
- O exemplo em [exemplo/](exemplo/) é didático: o cruzamento de médias perde dinheiro no teste e é reprovado pela
  bateria. Não é uma estratégia para operar.
