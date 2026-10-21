# T20.12 inicial — journal canônico de experimentos

Este recorte registra declarações e fatos de avaliação. Não conclui T20.12, não
qualifica critérios estatísticos e não implementa T20.13/T19.27/28, seleção
Defensivo/PF/Ultra, busca automática ou execução nativa. Pai exato:
`469dc79880ca20c73b7582e380265e664a758236`.

## Contrato e persistência

O único serviço/store novo é `mt5_extracao.experiments`, compartilhável pelos
consumidores futuros T13.4/I/F19. Não existe outro scheduler, motor ou cache de
resultados. As tabelas internas aditivas conservam:

- `_experiment_protocols`: revisão imutável do protocolo e contexto dos papéis.
- `_experiment_requests`: alternativa efetiva, origem e pedido deliberado.
- `_experiment_executions`: cada admissão técnica e seu `run_id` reservado.
- `_experiment_events`: journal ordenado de pedidos, acessos, admissões, recusas,
  falhas, interrupções, reutilizações e publicação.
- `_experiment_lock`: serialização transacional entre escritores SQLite/PG,
  inclusive acesso à reserva concorrente com uma nova proposta.

Revisões do mesmo nome/número não podem mudar de conteúdo. A identidade canônica
inclui todos os campos declarados. `criteria` e `objective` exigem `id` e `version`;
demais metadados são declarações, sem cálculo ou aprovação por parte do journal.
`grid` é uma lista declarada, com `[]` para alternativas manuais; não é executada.
`seed` é registrado, sem introduzir aleatoriedade ou uma nova regra no Backtester.
O contrato é JSON finito; chaves duplicadas no arquivo CLI são recusadas.

Cada papel `train`, `validation` ou `holdout` aponta para seu próprio
`data_manifest_id` já existente e um `evaluation_start` explícito. O serviço só
carrega/verifica snapshots. Não recaptura a base atual, deriva snapshot filho ou
troca dados ausentes/corrompidos por dados correntes. Todos os papéis precisam
ter mesma origem lógica, símbolo, TF e convenção temporal. A avaliação começa na
primeira barra disponível em/apos `evaluation_start` (`trade_from`) e termina na
última barra do respectivo snapshot. Os intervalos de **avaliação** são ordenados e
não sobrepostos; aquecimento anterior pode sobrepor. Esta regra por rótulos de
barras não constitui maturidade/purga/embargo estatisticamente qualificados.

## Uma composição BASE explícita

`request` recebe `Composition`, papel, chave idempotente, origem `manual` ou
`automatic` e finalidade `evaluation` ou `tuning`. Origem automática é apenas um
fato declarado pelo chamador; não liga um seletor. A alternativa usa composição
efetiva, manifesto, início da avaliação e configuração econômica efetiva. Pedidos
distintos da mesma alternativa ficam no journal, mesmo ao reutilizar um run.

Só os modos BASE, sem filtros/overrides, podem ser mapeados. As restrições de
`compatibility` existentes continuam valendo (inclusive Pullback indisponível no
novo adaptador). Parâmetros são os da estratégia registrada. `risk_fraction`
substitui explicitamente `STRATEGY.risk_per_trade`; capital, comissão e slippage
efetivos são congelados, sem copiar configuração inteira/segredos. O caminho
`run_python` existente não lê `compound` da configuração: somente `compound=True`
é admitido. Nenhum wrapper de estratégia, motor matemático ou regra de saída é
criado para contornar isso.

Composição interpretável mas inválida/não mapeável vira pedido `rejected`, com
motivo, antes do cálculo; descarte explícito vira `discarded`. Erro de sintaxe do
JSON ou protocolo inexistente é recusado pela CLI antes de haver pedido admitido.
Os protocolos não recebem dados legados como evidência retrospectiva de registro.

## Orçamento e publicação

O protocolo exige `budget.scope="evaluation_request"` e os cinco limites
explícitos de `robots.budget.Limits`: `max_jobs`, `max_seconds`, `hard_cpu_pct`,
`hard_ram_pct`, `min_free_mb`. Mesmas unidades e regras existentes; zero desliga o
respectivo teto. Não há teto agregado de campanha neste recorte.

Cada pedido usa seu próprio `Budget.begin` com plano fechado contendo manifesto,
identidade e configuração. `admit` reserva o `run_id`; `JobBudget` aplica recursos,
tempo, cancelamento cooperativo e orçamento acumulado nas retentativas daquele
pedido. Retomada usa o mesmo batch/run; nova admissão consome orçamento e ganha
outra execução técnica auditada. O Budget injetado pela API precisa ser o tipo
real, com mesmo banco/configuração/batch e limites exatos, sem posse ativa.
`begin` confere o plano; a injeção não concede limite diferente. Código/dados/
configuração alterados não são aceitos como a retomada do plano antigo.

Se o processo morrer antes da publicação, outra admissão técnica recupera a
reserva do mesmo `run_id`, dentro dos limites restantes; a execução abandonada
fica marcada como interrompida. Se morrer após o commit e antes de encerrar o
batch, a retomada entrega aquele run e recupera apenas o checkpoint, sem novo
cálculo. A posse ativa de outro executor não é tomada para fazer essa limpeza.

O controle adiciona seu vínculo pelo `control.result(..., conn=...)` da API
existente. `_robot_runs`, `datasets.bind`, conclusão do job e journal `run_recorded`
compartilham a transação. Falha de bind/journal reverte esses componentes; erro e
reserva técnica ficam registrados para retomada. A identidade observada dentro
da transação precisa corresponder à esperada, sem alteração silenciosa.

**O livro não pertence a essa transação:** `run_python` chama o importer depois,
via `importers.safely`. `run_recorded` significa somente o run e vínculo publicados;
`ledger_qualified=False`, `statistical_approval=False` e
`qualified_final_holdout=False`. Falha posterior não apaga o run nem produz outro
para tentar corrigir o livro. Journal de erro pós-publicação conserva esse limite.

## Reuso e exposição

Reuso consulta o store existente `_robot_runs` pela mesma
`_python_execution_identity`, sem outra cópia/cache de resultados. Só identidade
com `cache_eligible=True` pode ser reutilizada entre pedidos. A reutilização é
auditada como `reused` e não cria execução técnica/amostra nova. Identidade
inelegível não ganha digest de observação nem aprovação de independência. Mesmo
pedido já publicado continua idempotente pelo `run_id` exato, inclusive quando
a identidade era inelegível. Run referenciado ausente gera erro e não é recriado.
Pedido já admitido retoma seu `run_id` reservado, mesmo se outro pedido publicar
um resultado de mesma identidade durante a interrupção; o cache não troca essa
reserva.
Esses IDs não demonstram independência estatística entre alternativas.

O acesso é uma transação durável **antes** do cálculo, busca em cache ou entrega
do resultado. Se o journal falhar, a operação recusa o acesso. `holdout_access`
também permite que outro consumidor registre a intenção antes de consultar; isso
não intercepta leitores externos ainda não integrados. `status` mostra o journal
e os IDs, sem carregar métricas financeiras dos runs. Para nova consulta
deliberada, crie outra chave de pedido; repetir exatamente o mesmo pedido mantém
a mesma observação.

Exposição é compartilhada por origem lógica do snapshot + símbolo/TF e intervalos
de avaliação sobrepostos, independentemente de protocolo, revisão ou hash de uma
correção dos dados. Acesso `tuning` e nova alternativa proposta após acesso tornam
o escopo `used_for_tuning`; não há operação para limpar esse fato. Mesmo sem ajuste,
um acesso continua `access_recorded`. Sem acesso registrado, o estado é
`no_recorded_access`, **não** “reserva intocada”: exposição prévia/externa é sempre
`unknown` e `qualified_final_holdout=False`. Outra origem lógica não recebe prova
automática de ausência de exposição. Nenhum OOS/holdout é usado para selecionar
candidatos neste serviço.

## API e CLI

```python
from dataclasses import asdict
from mt5_extracao import experiments
from mt5_extracao.composition import base_composition
from mt5_extracao.robots.budget import Limits

definition = {
    "schema_version": 1, "name": "meu-estudo", "revision": 1,
    "criteria": {"id": "criterio-declarado", "version": 1},
    "objective": {"id": "objetivo-declarado", "version": 1},
    "grid": [], "seed": 42,
    "budget": {"scope": "evaluation_request", "limits": asdict(Limits())},
    "roles": {"train": {
        "data_manifest_id": existing_snapshot_id,
        "evaluation_start": "2026-01-20T00:00:00",
    }},
}
p = experiments.create(db, definition)
c = base_composition("ema_atr", symbol="QA", timeframe="H1", risk_fraction=.005)
q = experiments.request(db, p["protocol_id"], "manual-1", c, config=config)
result = experiments.evaluate(db, q["request_id"], config=config)
state = experiments.status(db, p["protocol_id"])
```

```console
mt5x --config arquivo.ini --json experiments create --file protocolo.json
mt5x --config arquivo.ini --json experiments request --protocol ID --key manual-1 --composition composicao.json --role train
mt5x --config arquivo.ini --json experiments evaluate PEDIDO
mt5x --config arquivo.ini --json experiments status ID
mt5x --config arquivo.ini --json experiments holdout_access ID --key consulta-1 --purpose tuning
```

A CLI não inicializa MT5, não envia notificações e não executa código declarado
no protocolo. Windows físico, registro de todos os consumidores, orçamento de
campanha, seleção/qualificação estatística e gestão de permissões de pesquisa
continuam pendentes. Testes PostgreSQL usam apenas schemas UUID do helper existente
e entram no job `tests/test_*_pg.py` da CI; ausência de URL local não é aceite PG.
