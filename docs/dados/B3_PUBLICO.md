# Dados públicos da B3 sem corretora

A conta de demonstração da MetaQuotes (`MetaQuotes-Demo`) não tem ativos da B3: `IND`, `WINN` e
`DOL` lá são ETFs americanos, e `ITUB`/`VALE` são ADRs em dólar. Para ter Ibovespa, miniíndice e
ações sem conta em corretora, `mt5x b3` importa os arquivos públicos da própria B3.

| Ação | Origem | Cobertura | Resultado |
|---|---|---|---|
| `daily` | COTAHIST (`bvmf.bmfbovespa.com.br/InstDados/SerHist`) | mercado à vista, desde os anos 1990 | barras D1 por código (PETR4, BOVA11…) |
| `trades` | negócios por instrumento (`arquivos.b3.com.br/rapinegocios/tickercsv`) | ações e futuros (WINZ26, WDOZ26…), só os pregões recentes (cerca de um mês) | barras M1 ou maiores, a partir de cada negócio |

```bash
mt5x b3 daily --symbols PETR4,VALE3,BOVA11 --from 2026-09-01 --to 2026-11-03
mt5x b3 trades --symbols WINZ26,PETR4 --from 2026-10-21 --to 2026-11-03
mt5x b3 daily --file ~/Downloads/COTAHIST_A2025.ZIP --symbols PETR4,VALE3
mt5x --db database/b3_publico.db backtest --symbol WINZ26 --tf M1 --strategy <chave>
```

- **Banco próprio.** Os dados vão para `database/b3_publico.db` (ou o `--db` informado), com fonte
  `b3_publico@B3`. Uma base aceita uma única fonte, então a B3 nunca se mistura com a MetaQuotes.
  Base de outra fonte é recusada antes de qualquer gravação (barras, origem e especificações
  ficam intactas). O relógio é o de Brasília (UTC-3), independente do `config.ini`. `--symbols`
  também filtra arquivos locais; arquivo com cabeçalho inválido é ignorado com motivo no JSON.
  Para consultar ou testar, passe o mesmo `--db` aos outros comandos.
- **Horário.** É o de Brasília, como a B3 publica, na base `broker`.
- **Arquivos originais.** Ficam em `database/b3_arquivos/` para reimportar sem baixar de novo.
  Fins de semana são pulados. Feriados e dias fora do período disponível aparecem como "ignorado".
- **Limite de download.** O arquivo anual `COTAHIST_A<ano>.ZIP` tem cerca de 90 MB, acima do limite
  de 70 MB do projeto. O comando recusa o download e explica como baixar o arquivo à mão, para
  depois importá-lo com `--file`.
- **Preços diários sem ajuste.** O COTAHIST traz o preço negociado em cada dia, sem ajuste por
  proventos ou desdobramentos.
- **Volume diário.** O número de negócios do COTAHIST tem 5 dígitos e trunca dias acima de 99.999
  negócios. A quantidade negociada (`real_volume`) é exata.
- **Especificação.** É gravada na primeira importação de cada código, a partir das regras da B3.
  WIN, IND, WDO e DOL: 1 contrato, com passo de 5 / 5 / 0,5 / 0,5 pontos e R$ 0,20 / 1 / 10 / 50
  por ponto. Ações e ETFs: lote de 100 e passo de R$ 0,01. Spread e corretagem ficam 0, então o
  custo do backtest é otimista.
- **Contratos futuros.** Cada vencimento é um símbolo próprio (WINV26, WINZ26…). Não há série
  contínua: a rolagem segue o contrato de fatos de instrumentos (`analytics/instruments.py`), que
  separa a série contínua de estudo do contrato executável.
- **Conferência.** Para PETR4 em 03/11/2026, a soma das barras M1 importadas reproduziu exatamente
  a máxima, a mínima, o fechamento e a quantidade da barra diária do COTAHIST.

Para operar ou usar o Testador nativo do MT5 com ativos da B3, ainda é preciso o servidor de uma
corretora brasileira. Basta uma conta de demonstração, sem conta real.

## Cobertura da abertura do WIN por pregão

`analytics opening-coverage` lê snapshots M1 congelados e um calendário declarado.
Use um snapshot por contrato da mesma base e inclua no calendário cada data da
população. Dias sem pregão ou sem confirmação da grade usam `closed` ou `unknown`
e dispensam os horários. A escolha de contrato por data é explícita.

```json
{
  "source": "B3, Ofício 050/2026-VNC, Anexo 1",
  "sessions": [
    {
      "date": "2026-10-27",
      "contract": "WINZ26",
      "kind": "regular",
      "known_at": "2026-10-13T23:59:59-03:00",
      "open": "09:00",
      "close": "18:25",
      "auction_open": "08:55"
    }
  ]
}
```

Congele os dados com `analytics.datasets.capture(db, "WINZ26", "M1", start, end)`.
Declare os fatos históricos de vencimento e especificação no registro existente
`analytics.instruments.record` antes da captura. Inclua `families=("ticks",)` se a
fonte tiver ticks armazenados. A consulta também está disponível em `sdk.opening_coverage`.

```bash
./run.sh --cli --db database/b3_publico.db --json analytics opening-coverage \
  --manifest <ID-do-snapshot> --calendar <calendario.json> --as-of 2026-10-27T09:15:00-03:00
```

O resultado identifica fonte, sessão, vencimento conhecido na abertura, revisão dos
dados, `status`, motivos e horários esperados e observados. O intervalo nominal
é fechado à esquerda e aberto à direita. Para 09h00–09h15, as 15 barras de
09h00 a 09h14 precisam estar fechadas até `--as-of`. Sessão especial, calendário
desconhecido, contrato não qualificado e lacuna excluem a faixa. A abertura
observada às 09h07 é preservada no recibo com sete minutos faltantes, se os demais
minutos estiverem presentes.

A largura é dada em pontos de índice e em ticks quando houver passo histórico.
O gap em pontos e o ATR de 14 M1 de Wilder usam a sessão anterior declarada,
com preço e janela completos no mesmo contrato. O ATR reinicia dentro daquele
pregão. A troca de contrato mantém essas medidas indisponíveis. A pré-abertura
publicada no calendário informa o horário programado. Ticks, spread, flags de
agressor e fila têm evidência própria. As barras B3 agregadas pelo importador
deixam spread e contagem de ticks brutos indisponíveis.

A prova pelo launcher usou 32 barras públicas do WINZ26 em SQLite de QA, com dados
limitados à abertura de 27 e 28/10/2026. Os dois pregões tiveram 15/15 barras na
janela. As larguras observadas foram 1.795 e 1.635 pontos. Ticks brutos, passo
histórico congelado e o pregão anterior completo ficaram fora desse recorte.
[Horários e pré-abertura](https://www.b3.com.br/data/files/99/B7/82/67/8A631A103BBB511AAC094EA8/OC%20050-2026-VNC%20NOVOS%20HORARIOS%20DE%20NEGOCIACAO_PT.pdf) · [Vencimento](https://www.b3.com.br/pt_br/solucoes/plataformas/puma-trading-system/para-participantes-e-traders/calendario-de-negociacao/vencimentos/calendario-de-vencimentos-de-contratos-financeiros/)
