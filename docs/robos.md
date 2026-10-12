# Robôs: catálogo, testes e operação

> **Em construção.** Esta página descreve o módulo planejado na Fase 8. O que já funciona está marcado em
> [PLANO_DE_TRABALHO.md](PLANO_DE_TRABALHO.md) (painel de status, tarefas T8.x).

A aba **Robôs** reúne num só lugar tudo o que pode operar ou ser estudado:

| Tipo | O que é | De onde vem |
|---|---|---|
| **MQL5** | Expert Advisor do MetaTrader 5 (`.mq5` ou `.ex5`) com seus presets (`.set`) | Pastas indicadas em `[ROBOTS] library_dirs` (por exemplo, o clone do `projeto-indicadores-mt5`) ou um arquivo importado |
| **Python** | As seis estratégias de [estrategias.md](estrategias.md) | Já vêm com o programa |

## Ver o catálogo (já funciona)

Na interface: aba **Robôs**. Cada card mostra o tipo, o regime, a classe de risco com o motivo, as estatísticas do
melhor teste gravado e o estágio. Clique no card para ver os parâmetros (do código ou de um preset), os testes e o
que falta para operar. **Importar robô** aceita um arquivo `.mq5`/`.ex5` ou uma pasta no formato do MT5.

Estratégias Python: na aba **Backtest** do painel, escolha o ativo, o timeframe e o período e clique em
**Rodar backtest**. O resultado (operações, acerto, profit factor, expectativa, resultado, queda máxima e a curva de
capital) fica gravado e aparece no card. Backtest isolado serve para estudar; quem aprova ou reprova é a validação.

Na linha de comando:

```bash
mt5x robots                       # todos os robôs: tipo, regime, risco de cada preset e estágio
mt5x robots list hdk_hadouken_painel   # um robô: parâmetros por grupo
mt5x robots import caminho/Robo.mq5    # acrescenta um robô avulso (.mq5 ou .ex5)
mt5x robots compile hdk_hadouken_painel   # copia o robô para o MT5 e compila no MetaEditor
mt5x robots install hdk_hadouken_painel   # só copia (robô, includes e presets)
mt5x robots backtest hdk_hadouken_painel --symbol WIN$N --tf M5 --from 2026-01-01 --preset Conservador
mt5x robots validate hdk_hadouken_painel  # confere o robot.json contra o contrato (docs/sdk/contrato.md)
mt5x robots battery hdk_hadouken_painel --preset Conservador   # bateria de segurança
mt5x robots check hdk_hadouken_painel --symbol WIN$N --tf M5   # contrato, compilação, backtest e bateria de uma vez
mt5x robots start hdk_hadouken_painel --symbol WIN$N --tf M5 --preset Conservador --close-terminal   # conta DEMO
mt5x robots monitor hdk_hadouken_painel   # posições e resultado do robô
mt5x robots stop                          # PARAR TUDO
```

`compile` copia o robô, seus includes e presets para a pasta `MQL5` do MetaTrader 5, compila e mostra os erros e
avisos do MetaEditor com arquivo, linha e coluna. Sem erros, o robô passa a **Compilado**. A sua pasta de robôs
não é alterada. Um preset que você já ajustou no MT5 é mantido; para trocá-lo pelo da pasta, use
`--overwrite-presets`. Feche o MetaEditor antes, se ele estiver aberto.

`backtest` de um robô MQL5 roda no **Strategy Tester** do MetaTrader 5: é uma simulação, nenhuma ordem vai
para a sua conta. O robô precisa estar compilado e o MT5 precisa estar **fechado** (ele não abre duas vezes).
Se estiver aberto, o programa avisa e não mexe em nada; com `--close-terminal` ele fecha o MT5, roda o teste e
religa a ponte. Não use `--close-terminal` com um robô operando. O teste usa o capital de
`[STRATEGY] initial_equity`, os parâmetros do `--preset` (e de `--param nome=valor`) e o modelo de
`[ROBOTS] tester_model`. O resultado fica gravado no card; o relatório completo do MT5 fica na pasta do
terminal (`mt5x_<robô>.htm`). Sem `--from` e `--to`, vale o último ano.

Um `robot.json` opcional ao lado do robô (`<nome>.robot.json`) informa o que o código não diz:

```json
{"name": "Meu robô", "regime": "tendência", "timeframes": ["M5", "M15"], "markets": "índice e dólar futuros",
 "risk": {"risk_pct": 0.5, "daily_loss_pct": 2, "max_drawdown_pct": 10, "mandatory_stop": true}}
```

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

## Bateria de segurança

`mt5x robots battery <robô>` confere dois grupos e mostra cada item com ✓ (passou), ⚠ (aviso) ou ✗ (reprovou),
sempre com o motivo e o que fazer:

| Grupo | O que confere |
|---|---|
| Código e preset | Compila sem erros, arquivos em UTF-8, stop obrigatório, sem martingale/grid/preço médio, toda ordem pela camada de segurança, sem DLL, magic number exclusivo, risco e limites dentro dos tetos |
| Estatística | Sobre o último backtest: pelo menos 200 operações, profit factor acima do mínimo, expectativa positiva, queda máxima de Monte Carlo dentro do limite, chance de ruína, resultado mantido no fim do período, o dobro do spread, sequência de perdas |

Um ✗ reprova; avisos não reprovam, mas leia cada um. Vale sempre a bateria **mais recente**: se um backtest novo
reprovar, o robô volta a "Testado". Robô sem código-fonte (`.ex5`) pode passar para demo só pela estatística e
nunca é liberado para conta real. Para o item do spread, grave antes a especificação do ativo (`mt5x specs`).

**Aprovação não é garantia de lucro.** É o mínimo para o robô rodar em conta de demonstração.

## Rodar em conta de demonstração

> **Aviso:** esta parte ainda não foi validada com o MetaTrader 5 real. Use somente em conta de demonstração e
> confira no próprio MT5 se o robô entrou no gráfico.

`mt5x robots start <robô> --symbol <ativo> --tf <timeframe>` mostra as exigências, uma a uma: robô compilado, bateria
aprovada com o mesmo preset, conta **de demonstração** conectada e PARAR TUDO desligado. Se a conta conectada for
real, o programa recusa. Com tudo certo, repita com `--close-terminal`: o programa fecha o MetaTrader 5 e o abre de
novo com o robô no gráfico, na mesma conta.

`mt5x robots monitor <robô>` mostra as posições abertas, as que estão sem stop e o resultado desde o início.

`mt5x robots stop` é o **PARAR TUDO**: os robôs com a camada de segurança do HADOUKEN param de abrir operações.
Ele **não fecha as posições abertas** (este programa nunca envia ordens): feche-as no MetaTrader 5. Um robô sem
essa camada não obedece; para ele, use `mt5x robots stop --close-terminal`, que fecha o MT5. Para voltar a
operar: `mt5x robots stop --release`.

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
# MetaEditor usado para compilar (vazio = o da instalação do MT5)
metaeditor_path =
# Modelo do Strategy Tester: 0 cada tick, 1 OHLC de 1 minuto, 2 preços de abertura, 4 ticks reais
tester_model = 1
# Teto de risco por operação (%) aceito pela bateria de segurança
max_real_risk_pct = 1.0
```

As chaves de operação (`allow_real`, `min_demo_days`, `max_real_risk_pct`) entram junto com as tarefas T8.7 e T8.8.

> Resultado de backtest **não garante** resultado futuro. Comece sempre em demo, com o preset Conservador.
