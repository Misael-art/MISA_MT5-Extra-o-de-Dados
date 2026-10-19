# T19.2 — Composição operacional e capacidades v1

Implementação em branch dependente da B-11, sem liberação nativa. O SDK1 continua
fechado e seus caminhos legados permanecem disponíveis. Os dois sidecars seguem a
[política de versões](versionamento-contratos.md).

## Uso funcional

```python
from mt5_extracao.composition import BaseAdapter, Composition, base_composition

composition = base_composition(
    "ema_atr", symbol="EURUSD", timeframe="H1",
    params={"fast": 10}, risk_fraction=0.005, compound=False,
)
text = composition.to_json()
assert Composition(text).config_hash == composition.config_hash
adapter = BaseAdapter(composition)
result = adapter.run(bars, spec, costs_config, timeframe="H1")
```

O adaptador monta a Strategy existente com todos os parâmetros efetivos e chama
`strategies.backtester.run_backtest`. Reexecuta sinais, filtros internos, operações,
custos e patrimônio pelo motor atual. Nenhuma fórmula financeira nem indicador foi
copiado. `costs_config` é BacktestConfig: capital, custos, spread e swap são preservados;
risco/compound vêm da composição. A fração 0.005 significa 0,5%, sem conversão implícita
para o input percentual nativo. O objeto original de custos não é alterado.

O chamador carrega barras da série selecionada e informa o timeframe dessa origem;
o adaptador recusa símbolo da especificação ou timeframe divergentes. Isso confere
contexto declarado, não certifica a origem/qualidade das barras. O fluxo real deve
continuar usando os metadados de qualidade/proveniência existentes.

## Contrato e identidade

`composition_schema_version:1` contém strategy(key/params), context(symbol/timeframe),
orientation, entry, profit_exit, protection, sizing, partial_exit_plan,
conditional_policy, filters ordenados, execution_order e bar_policy. Cada módulo tem
mode/params fechados. Parâmetros e unidades estão em `composition/registry.py`:
barra, minuto, ATR, múltiplo de risco R, ponto, índice 0–100, fração e booleano.
Não há inferência entre pontos/preço/ATR, fração/percentual ou período/timeframe.

A serialização JSON tem chaves ordenadas, UTF-8, sem espaços opcionais; SHA256 completo
é o config_hash. Guardar uma cópia JSON imutável impede que mutações posteriores em
dicionários alterem a configuração já validada. Alterações de valores/contexto geram
outro hash; ordem de chaves não gera. Tipos numéricos são preservados no round-trip:
1 e 1.0 têm representações distintas. Custos/dados/fingerprint/evidência não estão nesse
hash operacional e precisam acompanhar a identificação completa do backtest/pacote.
Nome, preset_id, categoria e revisões pertencem às T19.50/55/26, sem segunda convenção.

Versão, campos, tipos, finitude, unidades, precedência e combinações são verificados.
Bool não substitui inteiro/número; strings não são convertidas. JSON duplicado, modo
inexistente e parcial no/depois do alvo total têm erro com campo e ação. Precedência
base: sinal fechado → filtros → dimensionamento → próxima abertura. Intrabar OHLC
conservador usa stop antes do alvo e uma posição. Isso não afirma sequência real de ticks.

## Suporte e rascunhos

| Capacidade | Estado do novo adaptador |
|---|---|
| EMA/ATR, Donchian, BB/RSI, ORB, Squeeze; parâmetros próprios | Base Python com regras legadas preservadas |
| Orientação normal, entrada a mercado na próxima abertura, risco fracionário | Suportado no backtest Python |
| Ganho, proteção e parcial `strategy_default` | Preserva exatamente regras internas; não desliga trailing, filtros nem saídas legadas |
| Pullback | Retrato preservado; execução pelo novo adaptador indisponível até T19.57 |
| Inversão, limit/stop, alvo R, stop em pontos, filtro ADX adicional, parcial R | Rascunho tipado com round-trip; adaptador recusa com motivo |
| Condicional, gradientes, multiordem, Renko | Não implementados; modo desconhecido é recusado |
| Montagem/execução modular MQL5 | Indisponível até T19.23/B-08/B-10; sidecar não habilita suporte |

Recursos novos desligados reproduzem o legado: `strategy_default` significa delegar,
não apagar funcionalidades próprias da estratégia. Em particular, Pullback já tem
parcial no legado. A inspeção atual confirmou `StrategyManageTick` H/b173bec: marcação
antes da confirmação e BE mesmo com volume parcial inviável. O backtester integrado
também marca/move BE fora da condição de volume válido. O recorte não modifica esses
motores; bloqueia Pullback no novo adaptador e mantém correção/regressão/ensaio em T19.57.
Nenhum controle nativo foi habilitado, nem o defeito foi considerado corrigido.

## Capacidades e SDK1

`capabilities_schema_version:1` declara robot(key/version/fingerprint), backend=mql5,
strategy_key, modules e bindings. Modules declara modos por função; lista vazia
identifica indisponibilidade. Solicitar modo não declarado produz motivo explícito. Cada binding é um caminho presente na composição com input,
type e unit explícitos; dois caminhos não podem competir pelo mesmo input. É uma
**declaração parcial**, não prova de cobertura completa nem pacote exportável.

`robots.contract.validate_composition(robot, composition, capabilities)` confere o
SDK1, schema, vínculo ao robô do catálogo, fingerprint existente, nomes reais dos
inputs e seus tipos. Não altera `FIELDS`, manifests, catálogo, estados de aprovação
ou os leitores de presets legados. Enum/conversão de unidade exigem adaptador próprio;
por exemplo fração de risco não é mapeável diretamente a input em percentual. Ausência
ou incompatibilidade tem motivo, sem nomes prováveis. Mesmo sidecar válido não afirma
módulo instalado, compilado, carregado nem autorizado.

## Evidências e limites

Fonte base: B-11/M4212f59 sobre master4f7a046. Baseline6a9d580 tem fontes/testes
compatíveis com essa base documental; ambiente, fixtures e dependências preservados.
Validação local focada inicial: `pytest -q tests/test_composition.py tests/test_robots_contract.py
 tests/test_strategies.py tests/test_backtester.py --no-cov`: 112 aprovados. Compara
sinais/regras e backtest completo das cinco estratégias liberadas com o motor legado,
com operações não vazias, custos, dimensionamento e todas as curvas. Trata-se de
compatibilidade do adaptador, sem alegação de referência independente para nova matemática
(não há cálculo financeiro novo). Contrato, rascunhos, erros e identidade também cobertos.

Após completar a declaração explícita de modos por capacidade, 80 testes de composição/SDK1
aprovados no fechamento (`pytest -q tests/test_composition.py tests/test_robots_contract.py
--no-cov`). Consumidores sem alteração reaproveitados da seleção inicial.

CI obrigatório do código final deve ser acompanhado uma vez; essa execução local não
é atribuída aos checks antigos nem certifica Windows físico. Sem nova instalação,
doctor, benchmark, PostgreSQL local ou jornada nativa porque esses caminhos não mudam.

Próximos consumidores: contratos/adaptador nativos usados por T19.23 e variante S09
em T19.37; depois compilação de preset T19.25, identidade T19.50, retrato Default T19.55
e cópia Manual T19.26. T19.2 não conclui o percurso M19-B/T20.30 nem antecipa a UI T19.30.
