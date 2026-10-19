# Contrato do robô (versão 1)

O contrato é o que um robô MQL5 **declara** para o `mt5x` conferir sem adivinhar: quais parâmetros são o magic
number, o risco e os limites, e o que ele promete não fazer. Fica em um arquivo `robot.json` ao lado do robô.

Robô sem contrato continua funcionando: o programa tenta reconhecer os parâmetros pelo nome e pelo comentário, e a
bateria de segurança mostra um aviso. Com contrato, os valores são lidos pelo nome declarado.

Conferir um robô: `mt5x robots validate <robô>`.

## Onde fica o arquivo

`<Nome>.robot.json` (ao lado de `<Nome>.mq5`) ou `robot.json` na mesma pasta. Codificação UTF-8.

## Exemplo

```json
{
  "contract": 1,
  "name": "Meu robô",
  "description": "Rompimento de canal com stop pelo ATR.",
  "regime": "rompimento",
  "timeframes": ["M5", "M15"],
  "markets": "índice e dólar futuros",
  "inputs": {
    "magic": "InpMagicNumber",
    "risk_pct": "InpRiskPct",
    "daily_loss_pct": "InpDailyLossPct",
    "max_drawdown_pct": "InpMaxDrawdownPct",
    "stop": "InpStopAtrMult",
    "export_trades": "InpExportTrades"
  },
  "tunable": ["InpChannelPeriod", "InpStopAtrMult"],
  "safety_lib": "1.4.0"
}
```

## Campos

| Campo | Tipo | Obrigatório | Significado |
|---|---|---|---|
| `contract` | inteiro | sim | Versão do contrato. Hoje: `1` |
| `inputs` | objeto | sim | Nome do parâmetro (`input`) que cumpre cada papel; veja abaixo |
| `name`, `description` | texto | não | Como o robô aparece no catálogo |
| `regime` | texto | não | tendência, lateral, rompimento ou compressão |
| `timeframes` | lista de textos | não | Timeframes para os quais o robô foi feito |
| `markets` | texto | não | Mercados para os quais o robô foi feito |
| `risk` | objeto | não | Valores fixos no código, quando não há parâmetro: `risk_pct`, `daily_loss_pct`, `max_drawdown_pct`, `mandatory_stop` |
| `tunable` | lista de textos | não | Parâmetros numéricos da estratégia, variados em ±20% no teste de robustez |
| `safety_lib` | texto | não | Versão exigida da biblioteca `mt5x-safety` |
| `parity_with` | texto | não | Chave da estratégia Python de que este robô é a versão MQL5 |

Campo fora desta lista é erro (evita erro de digitação passar despercebido).

## Papéis dos parâmetros (`inputs`)

| Papel | Obrigatório | O parâmetro representa |
|---|---|---|
| `magic` | sim | Magic number: identifica as operações do robô. Diferente de zero e exclusivo no catálogo |
| `risk_pct` | sim | Risco por operação, em % do capital |
| `daily_loss_pct` | sim | Perda máxima diária, em %. Zero significa "sem limite" e reprova na bateria |
| `max_drawdown_pct` | sim | Drawdown máximo desde o pico, em % |
| `stop` | sim | Tamanho do stop (pontos, múltiplo de ATR...). Maior que zero = stop sempre definido |
| `export_trades` | não | Liga a exportação das operações no fim do backtest |

Os cinco primeiros precisam ser numéricos. O nome tem de existir como `input` no `.mq5`.

## Arquivos que o robô e o `mt5x` trocam

Todos na pasta comum do MetaTrader (`Common\Files`).

| Arquivo | Quem grava | Conteúdo |
|---|---|---|
| `HDK_trades_<magic>.csv` | o robô, no `OnTester`, quando o parâmetro de `export_trades` está ligado | ANSI, separador `;`, cabeçalho `time;symbol;type;volume;result`. Uma linha por operação **fechada**; `time` como `AAAA.MM.DD HH:MM`; `result` = lucro + swap + comissão |
| `HDK_KILL.txt` | o `mt5x` (`mt5x robots stop`) | Enquanto existir, o robô não abre novas operações. O conteúdo não importa |

## Biblioteca de segurança

O robô que declara `"safety_lib": "1.4.0"` usa a biblioteca [mt5x-safety](https://github.com/Misael-art/mt5x-safety).
Clone o repositório, aponte `[ROBOTS] safety_lib_dir` para a pasta do clone e o `mt5x robots install` (ou `compile`)
copia a biblioteca para `MQL5/Include/MT5X/Safety` na versão pedida. Se a pasta do MT5 já tiver outra versão, o
programa para e explica; para trocar, use `--upgrade-safety`.

## O que o robô não pode fazer

A bateria de segurança reprova:

- importar DLL (`#import "x.dll"`);
- enviar ordem fora da biblioteca de segurança (`OrderSend` ou `CTrade` direto);
- operar sem stop;
- aumentar a posição depois de uma perda (martingale, grid, preço médio);
- risco por operação acima do teto de `[ROBOTS] max_real_risk_pct`, ou sem limite de perda diário.

## Versões do contrato

Uma mudança que invalide um `robot.json` existente cria a versão 2; o `mt5x` continuará lendo a versão 1.
O esquema de referência está em `mt5_extracao/robots/robot.schema.json`.

## Inventário de extensões F19

A [reconciliação T19.1](../propostas/INVENTARIO_F19.md) registra os seis Experts,
inputs, presets, exportações e donos B/F19 no snapshot inspecionado. Composição
e capacidades novas exigem artefato lateral versionado ou migração formal por
B-11; não são campos adicionais aceitos por este contrato 1. O inventário é
documental, sem mudança do schema/validador nem aprovação operacional.
