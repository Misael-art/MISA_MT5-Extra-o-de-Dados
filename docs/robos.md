# Robôs: catálogo, testes e operação

> **Em construção.** Esta página descreve o módulo planejado na Fase 8. O que já funciona está marcado em
> [PLANO_DE_TRABALHO.md](PLANO_DE_TRABALHO.md) (painel de status, tarefas T8.x).

A aba **Robôs** reúne num só lugar tudo o que pode operar ou ser estudado:

| Tipo | O que é | De onde vem |
|---|---|---|
| **MQL5** | Expert Advisor do MetaTrader 5 (`.mq5` ou `.ex5`) com seus presets (`.set`) | Pastas indicadas em `[ROBOTS] library_dirs` (por exemplo, o clone do `projeto-indicadores-mt5`) ou um arquivo importado |
| **Python** | As seis estratégias de [estrategias.md](estrategias.md) | Já vêm com o programa |

## O caminho de um robô

```
Importado → Compilado → Testado → Bateria → Demo → Real
```

Cada card mostra em que ponto o robô está e o que falta para o próximo. Ninguém pula etapa.

| Etapa | O que acontece |
|---|---|
| Importado | O programa leu o robô: versão, parâmetros, presets e a classe de risco |
| Compilado | O MetaEditor compilou sem erros (só MQL5) |
| Testado | Há um backtest gravado, com custos |
| Bateria | A bateria de segurança foi aprovada |
| Demo | Está rodando em conta de demonstração |
| Real | Está rodando em conta real |

## Quem envia as ordens

**Este programa nunca envia ordens.** Quem opera é o robô, dentro do MetaTrader 5, usando a camada de
segurança do HADOUKEN (stop obrigatório, limite de perda diário e semanal, drawdown máximo, sem preço médio).
O programa instala o robô, inicia, acompanha o resultado e tem o botão **PARAR TUDO**.

## Exigências para operar

| Destino | Exigido |
|---|---|
| Demo | Compilado sem erros e bateria de segurança aprovada (código e estatística) |
| Real | Tudo acima, `allow_real = true` na configuração, pelo menos `min_demo_days` dias em demo com resultado dentro do esperado, risco por operação até `max_real_risk_pct` e confirmação digitando o número da conta |

Conta real vem **desligada** de fábrica.

## Classe de risco

| Classe | Significado |
|---|---|
| Baixo | Stop obrigatório, risco por operação até 0,5%, limites de perda ligados, sem preço médio |
| Moderado | Stop obrigatório, risco por operação até 1%, limites de perda ligados |
| Alto | Risco por operação acima de 1%, ou algum limite desligado, ou sinais de martingale/grid/preço médio |
| Não avaliado | Robô sem código-fonte (`.ex5`): não dá para verificar. Não é liberado para conta real |

A classe sempre vem com o motivo. Ela é calculada a partir dos parâmetros e do preset, não é uma garantia.

## Configuração (`config/config.ini`)

```ini
[ROBOTS]
# Pastas com robôs MQL5, separadas por ponto e vírgula
library_dirs =
allow_real = false
min_demo_days = 20
max_real_risk_pct = 1.0
```

> Resultado de backtest **não garante** resultado futuro. Comece sempre em demo, com o preset Conservador.
