# T19.24 inicial — backtest persistido de uma composição Python

Uma composição explícita pode executar o percurso completo de sinais, filtros,
entradas, gestão, saídas e capital no Backtester existente. O resultado conserva
a composição efetiva, seu hash e a curva no store de runs; a tela de Backtest
apresenta as escolhas gravadas quando o resultado é reaberto. Não há editor de
composição novo neste recorte.

Este é um consumidor inicial de T19.24. Lote/matriz de alternativas, carteira,
concorrência, margem agregada e modos ainda sem adaptador continuam pendentes.
Não é uma implementação de busca/seleção T19.27/28 nem uma qualificação estatística.

## Execução e capacidades

`robots.backtest.run_python(..., composition=...)` recebe uma `Composition` válida
ou seu objeto JSON. Ela já contém estratégia, parâmetros, ativo, timeframe,
dimensionamento, políticas e filtros; combinar com `strategy=` ou `params=` é
recusado. A chave do robô Python, o contexto do snapshot e a especificação do
símbolo precisam corresponder. O serviço continua aplicando os gates existentes
de qualidade/elegibilidade dos dados antes de gerar sinais.

A chamada delega ao `BaseAdapter` e ao mesmo `run_backtest`. Só os modos aceitos
por `composition.compatibility` podem executar: estratégia registrada, risco e
capitalização, proteção/alvo padrão ou os overrides de pontos/R já suportados,
políticas temporais existentes e filtros de sessão/cooldown existentes. O contrato
continua limitado a uma posição. Pullback e módulos sem adaptador permanecem
recusados. Não há nova permissão nativa ou expansão de lotes/parciais/grades.

`risk_fraction` e `compound` vêm da composição. Capital inicial, comissão,
slippage e demais settings seguem o contrato existente. Stops/alvos, parciais
internas e Move Guard mantêm a precedência do adaptador/motor atual; este recorte
não implementa outra gestão nem altera as fórmulas das estratégias.

`trade_from` aquece o prefixo dos indicadores, mas impede entradas anteriores ao
início de avaliação. Políticas que usam calendário recebem a convenção temporal
do snapshot; relógio ausente não é inventado. A verificação cooperativa do
`JobBudget` atravessa o adaptador e o motor, inclusive após gerar os sinais.

## Registro auditável e retomada

O [journal T20.12](experiments.md) ganha um executor opt-in explícito:
`composition_v1`. O padrão `base_v1` conserva suas restrições iniciais, inclusive
`compound=True` e ausência de filtros/overrides. Não há migração silenciosa dos
pedidos antigos. Executor desconhecido não é reinterpretado como BASE.

O pedido `composition_v1` congela composição e settings efetivos; usa os mesmos
snapshots, papéis, `Budget.begin/admit`, reserva de `run_id` e fronteira transacional
de publicação do registro inicial. Cancelamento antes de publicar não deixa run
parcial, e a retentativa preserva a reserva. O orçamento continua por pedido,
incluindo retentativas; não passa a ser teto de campanha.

A identidade de execução inclui a composição canônica e o código carregado do
adaptador, além dos dados/regras/spec/settings/runtime existentes. Alterar política
não reutiliza a identidade BASE ou de outra seleção. Os gates anteriores de
inelegibilidade continuam valendo; S10/VWAP não ganham cache qualificado por receber
uma composição. Identidade genérica elegível não prova independência estatística.

Persistência acrescenta em `metrics`: `composition`, `composition_hash`,
`composition_executor` e `compound`. Não há tabela/cache de resultados paralelo.
A importação do livro continua posterior à transação de run/journal;
`run_recorded` mantém `ledger_qualified=False`, `statistical_approval=False` e
`qualified_final_holdout=False`. Consultas e reuso continuam auditados antes de
entregar o resultado; exposição histórica desconhecida não vira reserva intocada.

## Uso

```python
from mt5_extracao import experiments
from mt5_extracao.composition import Composition, base_composition
from mt5_extracao.strategies.temporal import TemporalPolicy

value = base_composition("madrid", symbol="EURUSD", timeframe="H1",
                         risk_fraction=.0025, compound=False).to_dict()
value["protection"] = {"mode": "fixed_points", "params": {"stop_points": 100}}
value["profit_exit"] = {"mode": "fixed_r", "params": {"target_r": 1.5}}
value["conditional_policy"] = {
    "mode": "temporal_v1", "params": TemporalPolicy(max_seconds=3600).to_dict(),
}
composition = Composition(value)
request = experiments.request(db, protocol_id, "manual-1", composition,
                              executor="composition_v1", config=config)
result = experiments.evaluate(db, request["request_id"], config=config)
```

O protocolo e seus snapshots devem existir; `protocol_id`, `db` e `config` são
objetos do chamador. O exemplo usa as unidades do contrato, não constitui escolha
de investimento ou autorização ao vivo.

```console
mt5x --config arquivo.ini --json experiments request --protocol ID --key manual-1 --composition composicao.json --executor composition_v1
mt5x --config arquivo.ini --json experiments evaluate PEDIDO
mt5x --config arquivo.ini --json experiments status ID
```

A tela lê a composição do run persistido e mostra risco, base de capital,
proteção/alvo, prazo e filtros selecionados; não estima lotes/preços nos widgets.
O botão usual de Backtest ainda usa seu fluxo anterior. Reexecutar uma composição
exige a chamada explícita acima; abrir um resultado não a executa novamente.

## Evidência e limites

Fixtures de sinal controlado percorrem o motor real e demonstram saída antecipada
liberando quatro sinais onde BASE mantém uma operação; cota diária voltando a uma;
capitalização alterando lotes seguintes; `trade_from`, cancelamento e reserva;
identidade por política e preservação da inelegibilidade. Não são sinais de produção.

Uma prova separada da CLI usou cópia isolada de histórico EURUSD/H1 já explorado:
Madrid, 408 barras, 3 operações, resultado 111,0875; stop 100 pontos, alvo 1,5R,
prazo 3600s, cota de uma entrada e capital inicial fixo. O mesmo pedido reabriu o
run; outra consulta deliberada reutilizou a observação e permaneceu no journal.
A tela Tk/Xvfb reabriu esse resultado e suas escolhas. Isso comprova o percurso
de persistência/apresentação, sem certificar desempenho, fonte ou especificação
histórica. O ledger não ganha qualificação transacional por essa prova.

Paridade externa, corretora/stops/freeze, fluxo nativo, carteira, Windows físico
e aceite operacional continuam próprios. Não existe nova execução MT5 neste recorte.
