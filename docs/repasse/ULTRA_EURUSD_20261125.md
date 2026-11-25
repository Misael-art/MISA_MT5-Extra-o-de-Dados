# Ultra Preset · EURUSD na MetaQuotes-Demo

O controle anterior permanece reproduzível pela revisão `c925c92187fa` da PR 412.
Os casos sintéticos cobrem 372 ticks por instrumento, 32 e 36 negócios no Tester,
retomada de uma reserva parcial no mesmo passe e gestão por versão de abertura.
O [repasse anterior](ULTRA_PRESET_20261124.md) documenta os limites da grade,
do preenchimento nativo e do netting, além da divergência da hipótese no tick 199.

O protocolo desta etapa congela treino, validação, reserva inédita e ensaio técnico
em [Ultra EURUSD](ULTRA_EURUSD_PROTOCOLO_20261125.md). A janela técnica de
23/11/2026, das 08h às 10h no relógio bruto do MT5, usa o EURUSD original do
servidor. O manifesto de aquisição tem SHA-256
`ad05add4962d6e29b398247360d3f14618be29d3754f807c9c218b38d977c0f1`.

| Verificação | Evidência da janela técnica |
|---|---|
| Catálogo | 11 referências Python. Super ATR triplo tem um contrato nativo e uma variante gráfica. Hipóteses executáveis exigem regra versionada. O recorte concorrente atual cobre sinais Donchian, S09 e hipóteses M1 restritas. |
| Ticks do broker | 18.207 cotações utilizáveis em 120 minutos. Há 61 registros adicionais no mesmo milissegundo, zero duplicatas idênticas, zero cotações cruzadas e uma lacuna interna máxima de 19,225 s. |
| Grade e volumes | Todas as cotações estão na grade de 0,00001 após desconsiderar até um ULP da representação binária. O spread varia de 2 a 4 ticks. A fita contém cotações e nenhum volume negociado positivo. |
| Barras M1 | A recoleta reproduziu os oito blocos da janela. O OHLC reconstruído pelo bid coincide com o broker em 120 barras. A contagem de eventos da fita diverge de `tick_volume` nas 120 barras. A série de volume das barras precisa de evidência própria. |
| Tester modelo 4 | O agente 6327 entregou as mesmas 18.207 cotações, com horários, preços, volumes e sequência iguais. Os flags divergem desde o ordinal zero. O agente recebeu 130 e a ponte recebeu 1154. O XOR é `0x400` em todos os ticks. O campo original continua na fita. |
| Contrato observado | Conta USD, hedging, alavancagem 1:100; USD 1 por tick e lote, lote/passo mínimo 0,01, execução Market e FOK, stops e freeze declarados em 0 pontos. São propriedades observadas na coleta. |
| Economia | Comissão, margem aplicada, swap no período, conversão e calendário histórico precisam de evidência com vigência. A modalidade de volume negociado da VWAP fica inelegível para a fita observada. A modalidade de contagem de cotações exige histórico e âncora de sessão suficientes. |

A referência de tempo é o eixo bruto do MT5. O SDK preserva o inteiro original
recebido da API. Na falta de confirmação histórica, o manifesto registra o UTC
como inconclusivo. MetaQuotes documenta que o modo de ticks reais pode gerar ticks
a partir de barras M1 quando falta histórico de ticks. Nesta janela a comparação
usa o fluxo de `OnTick` diretamente. [Tester e ticks reais](https://www.mql5.com/en/docs/runtime/testing)

## Ciclo técnico S09 no Tester

O recibo `637e6446b0cb4723be5c9607f0a2d2ec` compara 119 fechamentos M1, com
igualdade de direção e janela de disponibilidade para os dez sinais. O comparador
confere as EMAs disponíveis com tolerância absoluta de `1e-12`. O perfil permite
uma tentativa. Preço, volume e moeda coincidem pela grade de tick, lote e centavo.
O relatório do agente declara 100% de ticks reais na execução.

| Evento no relógio do MT5 | Fato conciliado |
|---|---|
| 08:31:01.798 | S09 compra 0,01 lote a 1,09968. Ordem 2, negócio 2, posição 2. |
| Após o fill | SL 1,09948 e TP 1,09998. Risco calculado de USD 0,20 contra limite de USD 5,00. Margem verificada de USD 11,00. |
| 08:42:16.686 | Ordem 3 e negócio 3 encerram a posição 2 pelo TP a 1,09998. |
| Fechamento do Tester | +USD 0,30, custos registrados zero, sem posição aberta. Journal e relatório conferem com caixa e patrimônio de USD 1.000,30. |

Os custos realizados pertencem ao controle no Tester. A comissão e a regra de
conversão histórica do broker continuam sem qualificação. Os mesmos inputs são
reconstruídos do protocolo, sem abrir a reserva inédita.

## Reprodução no launcher

Abra o terminal portátil na conta DEMO autorizada para a coleta. Defina um destino
novo para `SNAPSHOT` e preserve os CSVs e o manifesto sem edição.

```sh
./run.sh --cli --json laboratory ultra-tester-capabilities
./run.sh --cli --json laboratory ultra-market-capture --symbol EURUSD --server MetaQuotes-Demo \
  --start 2026-11-23T08:00:00 --end 2026-11-23T10:00:00 --out "$SNAPSHOT"
./run.sh --cli --json laboratory ultra-market-verify --snapshot "$SNAPSHOT"
```

Feche o terminal de coleta para liberar o perfil ao Tester. O comando usa a
seleção DEMO salva no MT5 e suporta a seleção explícita documentada no repasse
anterior. Use a política de capacidade do host antes de iniciar o Tester.

```sh
./run.sh --cli --json laboratory ultra-market-tester-run --snapshot "$SNAPSHOT" --out "$RUNS"
./run.sh --cli --json laboratory ultra-market-tester-run --snapshot "$SNAPSHOT" --out "$RUNS" --mode s09_cycle
```

No Laboratório, abra **Carteiras Ultra** para consultar o catálogo, verificar a
fita ou executar a sonda passiva. Cada tentativa nova guarda fonte, binário, hashes,
configuração do Tester, journal do agente, relatório e recibo quando disponíveis.
O modo `s09_cycle` é um controle de um ciclo com ordens simuladas, parâmetros
fixados no protocolo e proteção nativa. O caminho prospectivo nesta etapa é um
observador passivo preparado para a DEMO com negociação automática desativada.
