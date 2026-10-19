# Referência nativa T20.3

`native.csv` é a exportação não editada de `ACCOUNT_BALANCE`/`ACCOUNT_EQUITY`
do Strategy Tester: MetaTrader/MetaTester build 6272, Wine 11.17, EURUSD H1,
MetaQuotes-Demo, conta de teste USD 1.000, 6 de janeiro de 2025, modelo 4.
O log do agente confirmou ticks reais desde `2025.01.06 00:00:00`.
O fonte final compilou sem erros nem avisos. Hashes e escopo estão no manifesto.
O `.gitattributes` local preserva os bytes do fonte e do CSV no checkout, inclusive
com `core.autocrlf=true`; não normalizar finais de linha desses dois arquivos.

Uma compra de 0,01 lote abre na segunda barra e fecha na sexta. A execução é
intradiária; o custo líquido zero foi conciliado por preço/tick/lote contra a
mudança do saldo nativo, sem generalizar comissão/swap zero a outro contexto.
Há perda aberta e recuperação: drawdown patrimonial 0,23 USD, enquanto o saldo
das operações inteiras encerradas não apresenta drawdown nesse cenário.

As chaves `bar_time`/`entry_bar`/`exit_bar` usam a convenção de início da barra
do servidor. Os valores da linha são a última observação daquela barra. Os
instantes nativos de entrada/saída também foram retidos. Não inferir UTC desses
inteiros nem usar esta comparação de fechamento como prova de fills intrabarra.

## Reproduzir a comparação sem MT5

Na raiz do projeto, depois do instalador:

```sh
.venv/bin/python tests/fixtures/equity_reference/verify.py
```

No Windows, usar `.venv\Scripts\python` no mesmo comando. O verificador confere
hashes, curvas completas e drawdown. Tolerância absoluta monetária 10⁻⁸ USD e
percentual 10⁻¹², sem tolerância relativa. O resultado observado foi saldo com
diferença zero e equity com diferença máxima 1,14 × 10⁻¹³ USD.

## Repetir o ensaio nativo

Usar uma instalação de QA isolada, com histórico do símbolo/período. Copiar e
compilar `MT5X_EquityReference.mq5` nela; selecionar no Strategy Tester EURUSD,
H1, 2025-01-06 até 2025-01-07, depósito 1.000 USD, modelo de ticks reais (4).
Escolher `InpOutput` exclusivo para não sobrescrever evidência anterior. O CSV
sai em Common/Files. O Expert recusa inicialização fora do Tester; não usá-lo
como estratégia operacional. Preservar o CSV e revisar a origem/versões antes
de atualizar o manifesto. Não substituir o fixture só para eliminar divergência.

O ensaio observado usou a infraestrutura existente de cópia privada da sessão,
compilador e `run_tester`, com porta própria e somente agente local, sem fechar
o terminal do usuário. Os comandos executados estão em `logs/t20-3/` no host.
Nenhuma ordem foi enviada por Python ou fora do Strategy Tester.

Este fixture não qualifica todos os Experts H, moeda cruzada, custos variáveis,
parciais/fills, margens, carteira, risco intrabarra ou Windows físico. Esses
limites não são convertidos em resultados zero; capacidades dos DTOs continuam
explícitas. Os demais casos T20.3 têm referências manuais nos testes Python.
