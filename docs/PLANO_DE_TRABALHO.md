# Plano de trabalho detalhado

Documento operacional para agentes (humanos ou IA, inclusive de menor capacidade)
implementarem o [ROADMAP](ROADMAP.md) **sem ambiguidade**. Antes de qualquer
tarefa, leia [AGENTS.md](../AGENTS.md) (regras obrigatórias).

## Como usar este plano

1. Pegue **a primeira tarefa não concluída** na ordem do ROADMAP cujas dependências estejam concluídas.
2. Crie um branch `tarefa/<ID>-<resumo>` (ex.: `tarefa/T1.1-upsert`).
3. Siga os **Passos** na ordem. Não pule etapas nem "melhore" coisas fora do escopo.
4. Rode **todos** os comandos da seção **Verificação**. Só continue se passarem.
5. Marque os itens de **Critérios de aceite**, faça um commit com a mensagem sugerida e abra o PR.
6. Atualize a coluna Status da tarefa na tabela abaixo (no mesmo PR).

Formato de cada tarefa: Objetivo · Depende de · Arquivos · Passos · Código de
referência · Critérios de aceite · Verificação · Armadilhas.

Comando de testes padrão (use o Python do `.venv`):

```bash
.venv/bin/python -m pytest -q          # Linux
.venv\Scripts\python -m pytest -q      # Windows
```

## Painel de status

| ID | Tarefa | Depende de | Status |
|---|---|---|---|
| T0.1 | Instaladores automáticos Windows/Linux + bootstrap | — | ✅ concluída |
| T0.2 | CI (GitHub Actions) com testes e instaladores | T0.1 | ✅ concluída |
| T0.3 | Validação manual dos instaladores em máquinas limpas | T0.1 | ⏳ |
| T1.1 | Upsert no banco (reextrair sem erro/duplicata) | — | ✅ concluída |
| T1.2 | Salvar por bloco + tabela de controle + retomada | T1.1 | ✅ concluída |
| T1.3 | Atualização incremental | T1.2 | ✅ concluída |
| T1.4 | M1 via `copy_rates_range` | — | ✅ concluída |
| T1.5 | Não sobrescrever o spread histórico | — | ✅ concluída |
| T1.6 | Base de tempo (fuso) explícita | T1.1 | ⏳ |
| T1.7 | Nomes de tabela sem colisão | T1.1 | ⏳ |
| T1.8 | Relatório de qualidade dos dados | T1.2 | ⏳ |
| T2.1 | `mt5_backend`: MT5 local (Windows) ou via ponte (Linux) | T0.1 | ✅ concluída |
| T2.2 | Usar o backend em todo o código (sem `import MetaTrader5` direto) | T2.1 | ✅ concluída |
| T2.3 | Tirar Tkinter do núcleo | — | ✅ concluída |
| T2.4 | Gestão de processos multiplataforma | T2.2 | ✅ concluída |
| T2.5 | `initialize()` com `windows_path` no Linux | T2.2 | ✅ concluída (validação com MT5 real: T0.3/T2.6) |
| T2.6 | Teste de paridade Windows × Linux | T2.5 | ⏳ |
| T3.1 | CLI `mt5x` | T1.3, T2.2 | ⏳ |
| T3.2 | Agendamento (cron/systemd/Agendador de Tarefas) | T3.1 | ⏳ |
| T3.3 | `pyproject.toml` (substitui `setup.py`) | — | ✅ concluída |
| T3.4 | Timeframes como enum próprio | T2.2 | ✅ concluída |
| T4.1 | Remover arquivos mortos e unificar pontos de entrada | T0.1 | ⏳ |
| T4.2 | Logging centralizado | — | ⏳ |
| T4.3 | Provedor MT5 falso + testes do extrator e do banco | T2.1 | 🔄 `tests/fakes.py` e `conftest.py` criados; falta cobertura ≥ 60% |
| T4.4 | Atualizar `docs/arquitetura.md` e `README.md` | F1, F2 | ⏳ |
| T4.5 | Remover módulos "enhanced" não usados | T4.3 | ⏳ |
| T5.1 | Exportação Parquet / DuckDB | T1.1 | ⏳ |
| T5.2 | PostgreSQL / TimescaleDB | T1.1 | ⏳ |
| T5.3 | Coleta de ticks | T1.2 | ⏳ |
| T5.4 | Snapshots do book (DOM) | T5.3 | ⏳ |
| T5.5 | Fonte externa real (importação CSV) | T1.2 | ⏳ |
| T5.6 | Pastas de dados por usuário (`platformdirs`) | T3.3 | ⏳ |

---

## Fase 0 — Fundação

### T0.1 — Instaladores automáticos + bootstrap ✅

Já implementado. O que existe e **não deve ser quebrado**:

| Arquivo | Papel |
|---|---|
| `install.bat` → `scripts/install/windows.ps1` | Instalador Windows (Python via winget/python.org, `.venv`, MT5 `/auto`, configuração, atalho) |
| `install.sh` → `scripts/install/linux.sh` | Instalador Linux (apt/dnf/pacman/zypper, Wine/WineHQ, MT5 no Wine, Python do Windows no Wine, ponte) |
| `run.bat`, `run.sh` | Lançadores do app |
| `scripts/mt5_bridge_server.py` | Servidor RPyC que roda **no Python do Wine** e expõe `MetaTrader5` |
| `scripts/mt5-bridge.sh` | start/stop/status da ponte |
| `mt5_extracao/bootstrap/` | `configure`, `doctor`, `locate-mt5`. **Somente biblioteca padrão** |
| `config/config.ini.example` | Referência das chaves de configuração |
| `requirements*.txt` | Dependências (runtime / opcionais / dev) |
| `tests/test_bootstrap_*.py` | Testes do bootstrap e da ponte |

Valores que precisam ficar **iguais** em mais de um arquivo (se mudar um, mude todos):

| Valor | Onde |
|---|---|
| Versão do rpyc (`6.0.2`) | `requirements.txt`, `scripts/install/linux.sh` (`RPYC_VERSION`) |
| Pasta do Python no Wine (`C:\Python311`) | `mt5_extracao/bootstrap/paths.py` (`WINE_PYTHON_WINDOWS_DIR`), `scripts/install/linux.sh` (`WIN_PYTHON_DIR_WIN`, `WIN_PYTHON_DIR_UNIX_REL`) |
| Porta da ponte (`18812`) | `paths.py` (`DEFAULT_BRIDGE_PORT`), `scripts/mt5_bridge_server.py` |
| Faixa de Python aceita (3.9–3.13) | `doctor.py` (`MIN_PYTHON`, `MAX_TESTED_PYTHON`), `linux.sh` (`MIN_PY_MINOR`), `windows.ps1` (`$PythonMinor`, `$PythonMaxMinor`) |

### T0.2 — CI ✅

`.github/workflows/ci.yml`: shellcheck, parse do PowerShell, pytest (Ubuntu e Windows),
execução real de `install.sh --yes --no-mt5` e `install.bat -Yes -NoMT5 -NoShortcut`.
**Regra:** todo PR deve manter o CI verde. Não desative jobs nem testes.

### T0.3 — Validação manual dos instaladores

**Objetivo:** confirmar em máquinas limpas as etapas que o CI não cobre (download e
instalação do MT5, Wine, ponte com o MT5 real).

**Passos:** para cada ambiente abaixo, em uma VM limpa, execute o instalador e preencha
a tabela em `docs/validacao_instaladores.md` (crie o arquivo):

| Ambiente | Comando | Verificar |
|---|---|---|
| Windows 11 (usuário comum) | duplo clique em `install.bat` | UAC do MT5 aparece uma vez; atalho criado; `doctor` sem FALHA |
| Windows 10 sem winget | idem | Python instalado pelo python.org |
| Windows com MT5 de corretora já instalado | idem | `locate-mt5` encontra a pasta da corretora; nada é reinstalado |
| Ubuntu 24.04 desktop | `./install.sh` | WineHQ instalado; MT5 abre; `./scripts/mt5-bridge.sh status` = ATIVA; `doctor` com "Ponte MT5 (RPyC): OK" |
| Ubuntu 22.04 servidor (sem X) | `./install.sh --yes` | Xvfb usado; instalação conclui |
| Debian 12 | `./install.sh` | idem Ubuntu |
| Fedora 40 | `./install.sh` | Wine via dnf |
| Reexecução | rodar o instalador 2× | 2ª execução pula etapas e não altera `config.ini` |

**Critérios de aceite:** tabela preenchida; cada falha vira uma issue com o log anexado
(`logs/install-*.log`).

**Armadilhas:** o `mt5setup.exe /auto` abre o terminal ao final (o instalador Linux o fecha com
`wineserver -k`). Algumas corretoras distribuem um instalador próprio; nesse caso use
`--mt5-path` / `-MT5Path`.

---

## Fase 1 — Integridade dos dados

### T1.1 — Upsert no banco (reextrair sem erro/duplicata)

**Objetivo:** hoje `DatabaseManager.save_ohlcv_data` usa `to_sql(if_exists='append')` numa
tabela com `time` como PRIMARY KEY. Reextrair qualquer período que já exista gera
`sqlite3.IntegrityError: UNIQUE constraint failed` e **o lote inteiro é descartado**
(comportamento reproduzido e confirmado). É preciso gravar com *upsert*: insere o que é
novo e atualiza o que já existe.

**Depende de:** nada. **Arquivos:** `mt5_extracao/database_manager.py`, `tests/test_database_upsert.py` (novo).

**Passos:**
1. Em `database_manager.py`, adicione os imports:
   `from sqlalchemy import MetaData, Table` e `from sqlalchemy.dialects.sqlite import insert as sqlite_insert`.
2. Adicione o método `_upsert_dataframe` (código abaixo) na classe `DatabaseManager`.
3. Em `save_ohlcv_data`, **substitua somente** a linha
   `df_to_save.to_sql(table_name, self.engine, if_exists='append', index=True, index_label='time')`
   por `self._upsert_dataframe(table_name, df_to_save)`. Mantenha todo o resto (preparação de colunas, logs, `except`).
4. Faça o mesmo em `save_data` (linha com `to_sql(..., if_exists='append', index=True)`), **somente se** a tabela tiver PK `time`; caso contrário mantenha `to_sql`. Para saber: `inspect(self.engine).get_pk_constraint(table_name)['constrained_columns'] == ['time']`.
5. Crie o teste descrito em Verificação.

**Código de referência (testado com SQLAlchemy 2.0 + SQLite):**

```python
    # Limite seguro de linhas por INSERT: SQLite aceita até 32766 parâmetros;
    # a tabela OHLCV tem ~70 colunas -> 200 linhas = ~14 mil parâmetros.
    _UPSERT_CHUNK_ROWS = 200

    def _upsert_dataframe(self, table_name, df):
        """Insere ou atualiza linhas pela chave primária 'time'. df indexado por 'time'."""
        table = Table(table_name, MetaData(), autoload_with=self.engine)
        table_cols = [c.name for c in table.columns]
        data = df.reset_index() if 'time' not in df.columns else df
        data = data[[c for c in table_cols if c in data.columns]]
        # NaN -> None (NULL) e Timestamp -> datetime (formato de gravação do SQLAlchemy)
        data = data.astype(object).where(pd.notna(data), None)
        records = data.to_dict(orient='records')
        for r in records:
            if isinstance(r['time'], pd.Timestamp):
                r['time'] = r['time'].to_pydatetime()
        with self.engine.begin() as conn:
            for i in range(0, len(records), self._UPSERT_CHUNK_ROWS):
                stmt = sqlite_insert(table).values(records[i:i + self._UPSERT_CHUNK_ROWS])
                update_cols = {c: stmt.excluded[c] for c in data.columns if c != 'time'}
                conn.execute(stmt.on_conflict_do_update(index_elements=['time'], set_=update_cols))
        return len(records)
```

O formato gravado continua `'2024-01-01 10:00:00.000000'` (texto), **idêntico** ao que o
`to_sql` já gravava, então bases existentes continuam compatíveis.

**Critérios de aceite:**
- [ ] Salvar o mesmo DataFrame 2× retorna `True` nas duas e a contagem de linhas não muda.
- [ ] Salvar de novo com um `close` alterado atualiza o valor.
- [ ] Linhas novas junto com linhas existentes: as novas são inseridas.
- [ ] Nenhuma outra linha de `save_ohlcv_data` foi alterada além da troca do `to_sql`.

**Verificação:** crie `tests/test_database_upsert.py` usando `tmp_path`:

```python
import pandas as pd
from sqlalchemy import text
from mt5_extracao.database_manager import DatabaseManager

def _df(close1=2.0):
    return pd.DataFrame({'time': pd.to_datetime(['2024-01-01 10:00', '2024-01-01 10:01']),
                         'open': [1.0, 2.0], 'high': [1.0, 2.0], 'low': [1.0, 2.0], 'close': [1.0, close1],
                         'tick_volume': [1, 2], 'spread': [1, 1], 'real_volume': [5, 6]})

def test_upsert(tmp_path):
    db = DatabaseManager(db_path=str(tmp_path / 't.db'))
    assert db.save_ohlcv_data('WIN$N', '1 minuto', _df())
    assert db.save_ohlcv_data('WIN$N', '1 minuto', _df(close1=99.0))
    with db.engine.connect() as c:
        rows = c.execute(text('SELECT close FROM win_n_1_minuto ORDER BY time')).fetchall()
    assert [r[0] for r in rows] == [1.0, 99.0]
```

Rode o pytest; todos os testes devem passar.

**Armadilhas:** `DatabaseManager` cria `logs/` no diretório atual ao ser importado (efeito
colateral conhecido; será resolvido em T4.2). Não use `INSERT OR REPLACE`: ele apaga a linha
e perde colunas de indicadores que não vieram no DataFrame. O `on_conflict_do_update`
atualiza só as colunas enviadas.

**Commit:** `fix(db): grava OHLCV com upsert para permitir reextração sem erro`

### T1.2 — Salvar por bloco + tabela de controle + retomada

**Objetivo:** hoje `HistoricalExtractor._process_symbol` acumula todos os blocos em memória
(`all_symbol_data`) e só salva no fim. Se um bloco falha, tudo é perdido (`break` +
`symbol_success = False`). Com 10 anos de M1 isso consome muita RAM. A mudança: salvar
**cada bloco** assim que chega, registrar o bloco numa tabela de controle e, numa nova
execução, pular blocos já concluídos.

**Depende de:** T1.1. **Arquivos:** `database_manager.py`, `historical_extractor.py`, `tests/test_extraction_log.py`.

**Passos:**
1. Em `DatabaseManager`, crie a tabela de controle (chamada em `_connect` após criar o engine):

   ```sql
   CREATE TABLE IF NOT EXISTS _extraction_log (
       table_name  TEXT NOT NULL,
       block_start TIMESTAMP NOT NULL,
       block_end   TIMESTAMP NOT NULL,
       rows        INTEGER NOT NULL,
       status      TEXT NOT NULL,      -- 'ok' | 'empty' | 'failed'
       source      TEXT NOT NULL,      -- 'mt5' | nome da fonte externa
       updated_at  TIMESTAMP NOT NULL,
       PRIMARY KEY (table_name, block_start, block_end)
   )
   ```
2. Adicione `record_block(table_name, start, end, rows, status, source)` (upsert via
   `INSERT ... ON CONFLICT(table_name, block_start, block_end) DO UPDATE SET ...`) e
   `completed_blocks(table_name) -> set[(start, end)]` (somente `status IN ('ok','empty')`).
3. Em `HistoricalExtractor._process_symbol`:
   - No início: `table_name = self.db_manager.get_table_name_for_symbol(symbol, timeframe_name)` e `done = self.db_manager.completed_blocks(table_name)` (se `overwrite=True`, use `done = set()`).
   - No laço `while current_start < end_date`: se `(current_start, block_end) in done`, avance para o próximo bloco **sem** chamar o MT5.
   - Após obter `block_df` com sucesso: calcule indicadores **do bloco** (ver passo 4), chame `save_ohlcv_data` e depois `record_block(..., status='ok' if len(block_df) else 'empty', source=...)`.
   - Em falha definitiva do bloco: `record_block(..., rows=0, status='failed', ...)` e **continue** para o próximo bloco (não use `break`). O símbolo termina como falha se algum bloco falhou.
   - Remova `all_symbol_data` e o `pd.concat` final.
4. Aquecimento dos indicadores: indicadores com janela (RSI 14, MA 20, etc.) precisam de barras
   anteriores. Antes de calcular os indicadores do bloco, leia as últimas `WARMUP_BARS = 500`
   linhas da tabela com `time < current_start`
   (`SELECT * FROM "<tabela>" WHERE time < :t ORDER BY time DESC LIMIT 300`), concatene antes do
   bloco, calcule e **descarte** as linhas de aquecimento antes de salvar (filtre `time >= current_start`).
   (500 e não 300: RSI e MACD usam médias exponenciais; com 300 barras a diferença para o período
   inteiro fica em ~1e-8, acima da tolerância de aceite.)
5. Os blocos devem ter limites determinísticos para a retomada funcionar: use sempre
   `block_end = min(current_start + block_delta, end_date)` e
   `current_start = block_end + timedelta(seconds=1)`, como já é feito. Não mude essa aritmética.

**Critérios de aceite:**
- [ ] Um bloco que falha não impede a gravação dos outros.
- [ ] Reexecutar a mesma extração não chama o MT5 para blocos `ok`/`empty` (teste com provedor falso contando chamadas; se T4.3 não existir, use `unittest.mock.MagicMock` no `connector`).
- [ ] Indicadores do primeiro registro de um bloco são iguais aos obtidos extraindo o período inteiro de uma vez (tolerância `1e-9`).
- [ ] Uso de memória não cresce com o número de blocos (não há mais acumulação).

**Implementado também:** tabelas internas (prefixo `_`) ficam fora de `get_all_tables`/`get_existing_symbols`;
`delete_data_periodo` passou a comparar datas no mesmo formato de texto gravado (antes a barra do limite final
não era apagada). A retomada só pula blocos com **os mesmos limites**; um período total diferente gera outros
limites e os blocos são buscados de novo (o upsert evita duplicatas).

**Armadilhas:** o `HistoricalExtractor` roda símbolos em paralelo (`ThreadPoolExecutor`). Cada
símbolo tem sua própria tabela, mas a `_extraction_log` é compartilhada: use `engine.begin()`
por gravação (transação curta). O SQLite serializa escritas. Se ocorrer `database is locked`,
adicione `connect_args={'timeout': 30}` no `create_engine` do SQLite.

**Commit:** `feat(extracao): salva por bloco com tabela de controle e retomada`

### T1.3 — Atualização incremental

**Objetivo:** botão/comando "Atualizar" que busca apenas o que falta desde o último registro.

**Depende de:** T1.2. **Arquivos:** `database_manager.py`, `historical_extractor.py`, `ui_manager.py` (botão), testes.

**Passos:**
1. `DatabaseManager.get_last_timestamp(table_name) -> datetime | None`: `SELECT MAX(time) FROM "<tabela>"` e converta com `pd.to_datetime`. Retorne `None` se a tabela não existir (`inspect(engine).has_table`).
2. `HistoricalExtractor.update_symbols(symbols, timeframe_val, timeframe_name, include_indicators, ...)`: para cada símbolo, `start = last_ts - timedelta(minutes=timeframe_minutes)` (sobreposição de 1 barra, resolvida pelo upsert) ou, se não houver dados, `start = datetime.now() - timedelta(days=30)`. `end = datetime.now()`. Reutilize `extract_data` com `overwrite=False`.
3. Adicione na aba de extração da UI um botão "Atualizar até agora" que chama `update_symbols` com os símbolos selecionados. Siga o padrão do botão existente que chama `self.app.historical_extractor.extract_data(` em `ui_manager.py`.

**Critérios de aceite:** rodar a atualização 2× seguidas não cria linhas duplicadas; a 2ª traz 0 ou 1 barra nova.

**Implementado:** `extract_data(..., start_dates={símbolo: início})` (opcional) permite um início por símbolo;
`HistoricalExtractor.TIMEFRAME_MINUTES` dá o tamanho da barra. A última barra do dia pode estar incompleta
quando a atualização roda durante o pregão; a próxima atualização a corrige (upsert).

**Commit:** `feat(extracao): atualização incremental a partir do último registro`

### T1.4 — M1 via `copy_rates_range`

**Objetivo:** hoje, para M1, o extrator pede `bars=200000` a partir de `current_start` e filtra
depois (em `historical_extractor.py`, bloco `if timeframe_val == mt5.TIMEFRAME_M1:` dentro do
laço de retry). Isso baixa muito mais dados que o bloco e esbarra no limite "Max bars in chart"
do terminal.

**Depende de:** nada. **Arquivos:** `historical_extractor.py`.

**Passos:** no laço de retry, remova o ramo especial de M1 e use para **todos** os timeframes a
chamada que hoje está no `else`:
`self.connector.get_historical_data(symbol, timeframe_val, start_dt=current_start, end_dt=block_end, bars=None)`.

**Critérios de aceite:** o bloco M1 retorna somente barras entre `current_start` e `block_end`;
nenhuma chamada usa `bars=200000`. Se o MT5 retornar vazio para períodos antigos, o log deve dizer
"sem dados no período" e não "falha".

**Armadilhas:** para históricos M1 muito antigos o terminal pode precisar baixar dados do
servidor. `copy_rates_range` retorna o que estiver disponível. Não aumente o `chunk_days_m1`
acima de 30.

**Commit:** `fix(extracao): usa copy_rates_range também para M1`

### T1.5 — Não sobrescrever o spread histórico

**Objetivo:** em `historical_extractor.py` (bloco "Calcular indicadores se solicitado"), a linha
`final_df['spread'] = symbol_info.spread` troca o spread histórico de **todas** as barras pelo
spread do momento da extração. O MT5 já retorna o spread correto por barra.

**Passos:** remova as linhas `symbol_info = self.connector.get_symbol_info(symbol)`,
`if symbol_info:` e `final_df['spread'] = symbol_info.spread`.

**Critérios de aceite:** a coluna `spread` salva é idêntica à devolvida por `copy_rates_range`.

**Commit:** `fix(extracao): preserva o spread histórico por barra`

### T1.6 — Base de tempo explícita

**Objetivo:** o MT5 devolve `time` em segundos no **horário do servidor da corretora** (na B3,
normalmente UTC−3). Hoje o valor é gravado sem nenhuma indicação disso.

**Passos:**
1. Adicione em `config_builder.DEFAULTS` a seção `("APP", OrderedDict([("time_basis", "broker")]))`
   (valores aceitos: `broker`, que é o padrão e o comportamento atual, e `utc`). Adicione a chave
   em `config/config.ini.example`.
2. Crie a tabela `_metadata(key TEXT PRIMARY KEY, value TEXT)` e grave `time_basis` na primeira
   gravação. Se a base já tiver um valor diferente do configurado, **recuse gravar** e registre um
   erro claro (misturar bases corrompe a série).
3. Se `utc`: `DatabaseManager` recebe `utc_offset_hours` (nova chave `[APP] broker_utc_offset`,
   padrão `-3`) e converte `time - offset` antes de salvar.
4. Documente em `docs/exportacao_dados.md`.

**Critérios de aceite:** o valor padrão não muda nada nas bases existentes; os testes cobrem os dois modos.

### T1.7 — Nomes de tabela sem colisão

**Objetivo:** `get_table_name_for_symbol` troca tudo que não é alfanumérico por `_`, então
`WIN$N` e `WIN_N` geram a mesma tabela.

**Passos:**
1. Crie a tabela `_symbol_tables(symbol TEXT, timeframe TEXT, table_name TEXT UNIQUE, PRIMARY KEY(symbol, timeframe))`.
2. `get_table_name_for_symbol`: (a) se o par já está mapeado, retorne o nome mapeado; (b) senão,
   calcule o nome atual; se esse nome já estiver mapeado para **outro** símbolo, acrescente
   `_` + os 6 primeiros caracteres de `hashlib.sha1(symbol.encode()).hexdigest()`; (c) grave o mapeamento.
3. Migração: na inicialização, para cada tabela existente sem mapeamento, não faça nada (o
   mapeamento é criado quando o símbolo for usado). **Não renomeie tabelas existentes.**

**Critérios de aceite:** `WIN$N` e `WIN_N` geram tabelas diferentes; bases antigas continuam lendo as mesmas tabelas.

### T1.8 — Relatório de qualidade dos dados

**Objetivo:** depois de cada extração, informar lacunas e inconsistências.

**Arquivos:** novo `mt5_extracao/data_quality.py` (funções puras que recebem DataFrame), testes.

**Verificações:** (1) `high < low`, `open`/`close` fora de `[low, high]`; (2) `tick_volume == 0`;
(3) `time` duplicado; (4) lacunas maiores que 1 barra **dentro do pregão** (horário configurável
em `[APP] session_start=09:00`, `session_end=18:30`; ignore fins de semana); (5) total de barras
por dia.

**Saída:** `dict` com contagens + lista das primeiras 20 ocorrências de cada tipo; o extrator grava
um resumo no log e na `_extraction_log` (coluna nova `quality_json TEXT`).

**Critérios de aceite:** testes com DataFrames sintéticos cobrindo cada verificação.

---

## Fase 2 — Núcleo desacoplado e Linux funcional

### T2.1 — `mt5_backend`: MT5 local ou via ponte

**Objetivo:** um único ponto que devolve um objeto com a **mesma API** do módulo `MetaTrader5`:
no Windows, o próprio módulo; no Linux, um adaptador que chama o MT5 do Wine pela ponte RPyC.

**Depende de:** T0.1. **Arquivos:** novo `mt5_extracao/mt5_backend.py`, `tests/test_mt5_backend.py`.

**Decisões já tomadas (não mude):**
- Datas são enviadas ao MT5 como **inteiros epoch (segundos)**, tratando `datetime` sem fuso como
  UTC (`calendar.timegm`). O MT5 aceita inteiros, o que evita diferenças de fuso entre máquinas.
- No servidor, o resultado é convertido para tipos puros do Python (listas, dicts, tuplas) antes de
  voltar, porque as structs do MT5 não podem ser desserializadas no Linux (o módulo não existe lá).
- A escolha local × ponte vem de `config.ini`: `[BRIDGE] enabled = true` → ponte; senão, import local.

**Código de referência (validado contra a ponte real com um módulo MetaTrader5 falso que devolve
arrays numpy estruturados e namedtuples):**

```python
"""Acesso ao MetaTrader 5: módulo local (Windows) ou ponte RPyC (Linux/Wine)."""
import calendar
import collections
import configparser
import datetime as _dt
import threading

import numpy as np

_REMOTE_HELPER = r'''
import MetaTrader5 as _mt5
def _mt5x_encode(v):
    if hasattr(v, "dtype") and getattr(v.dtype, "names", None):
        return {"__ndarray__": True, "descr": [(n, v.dtype[n].str) for n in v.dtype.names], "rows": v.tolist()}
    if hasattr(v, "_asdict"):
        return {"__struct__": type(v).__name__, "fields": {k: _mt5x_encode(x) for k, x in v._asdict().items()}}
    if isinstance(v, (tuple, list)):
        return [_mt5x_encode(x) for x in v] if any(hasattr(x, "_asdict") for x in v) else tuple(v)
    return v
def _mt5x_call(name, args, kwargs):
    return _mt5x_encode(getattr(_mt5, name)(*args, **kwargs))
'''


def _to_epoch(v):
    if isinstance(v, _dt.datetime):
        if v.tzinfo is not None:
            v = v.astimezone(_dt.timezone.utc).replace(tzinfo=None)
        return calendar.timegm(v.timetuple())
    return v


_struct_types = {}


def _decode(v):
    if isinstance(v, dict) and v.get("__ndarray__"):
        return np.array([tuple(r) for r in v["rows"]], dtype=[tuple(d) for d in v["descr"]])
    if isinstance(v, dict) and "__struct__" in v:
        fields = {k: _decode(x) for k, x in v["fields"].items()}
        key = (v["__struct__"], tuple(fields))
        if key not in _struct_types:
            _struct_types[key] = collections.namedtuple(v["__struct__"], list(fields))
        return _struct_types[key](**fields)
    if isinstance(v, list):
        return tuple(_decode(x) for x in v)
    return v


class RemoteMT5:
    """Mesma API do módulo MetaTrader5, executada no Python do Wine via RPyC."""

    def __init__(self, host="127.0.0.1", port=18812):
        import rpyc
        self._rpyc = rpyc
        self._lock = threading.Lock()  # uma conexão RPyC não deve ser usada por várias threads ao mesmo tempo
        self._conn = rpyc.classic.connect(host, port, keepalive=True)
        self._conn._config["sync_request_timeout"] = 300
        self._conn.execute(_REMOTE_HELPER)
        self._call = self._conn.namespace["_mt5x_call"]
        self._module = self._conn.modules["MetaTrader5"]
        self._consts = {}

    def __getattr__(self, name):
        if name.startswith("_") and name != "__version__":
            raise AttributeError(name)
        if name.isupper() or name == "__version__":
            if name not in self._consts:
                with self._lock:
                    self._consts[name] = self._rpyc.classic.obtain(getattr(self._module, name))
            return self._consts[name]

        def fn(*args, **kwargs):
            args = tuple(_to_epoch(a) for a in args)
            kwargs = {k: _to_epoch(v) for k, v in kwargs.items()}
            with self._lock:
                return _decode(self._rpyc.classic.obtain(self._call(name, args, kwargs)))
        fn.__name__ = name
        return fn


_instance = None


def get_mt5(config_path="config/config.ini"):
    """Retorna o módulo MetaTrader5 (Windows) ou RemoteMT5 (Linux). None se indisponível."""
    global _instance
    if _instance is not None:
        return _instance
    cfg = configparser.ConfigParser(interpolation=None)
    cfg.read(config_path, encoding="utf-8")
    if cfg.getboolean("BRIDGE", "enabled", fallback=False):
        _instance = RemoteMT5(cfg.get("BRIDGE", "host", fallback="127.0.0.1"),
                              cfg.getint("BRIDGE", "port", fallback=18812))
    else:
        try:
            import MetaTrader5 as mt5
            _instance = mt5
        except ImportError:
            return None
    return _instance
```

**Passos:**
1. Crie `mt5_extracao/mt5_backend.py` com o código acima.
2. Crie `tests/test_mt5_backend.py`: reaproveite o padrão de `test_bridge_server_and_probe`
   (em `tests/test_bootstrap_doctor.py`) para subir `scripts/mt5_bridge_server.py` com um
   `MetaTrader5.py` falso no `PYTHONPATH` que tenha: `TIMEFRAME_M1 = 1`; `copy_rates_range` que
   faz `assert isinstance(a, int)` e devolve um `np.array` estruturado com as colunas
   `time, open, high, low, close, tick_volume, spread, real_volume`; `symbol_info` que devolve um
   `collections.namedtuple`; `last_error` que devolve `(1, "Success")`.
3. Verifique: `pd.DataFrame(r)` tem as 8 colunas com os mesmos dtypes; `symbol_info("X").spread`
   funciona; `symbol_info("X")._asdict()` funciona; `copy_rates_from_pos` que devolve `None`
   retorna `None`; `datetime` é convertido para `int`.

**Critérios de aceite:** testes passam no CI (Linux e Windows). No Windows sem ponte,
`get_mt5()` devolve o módulo `MetaTrader5`.

**Armadilhas:** não passe `numpy.datetime64` nem `pd.Timestamp` para o MT5: converta para
`datetime` antes (`ts.to_pydatetime()`). O `_decode` para `symbols_get()` com milhares de
símbolos é lento (~1 s). É aceitável, mas não chame em laço.

### T2.2 — Usar o backend em todo o código

**Objetivo:** nenhum módulo do núcleo faz `import MetaTrader5` diretamente.

**Depende de:** T2.1. **Arquivos:** `mt5_connector.py`, `historical_extractor.py`, `error_handler.py`, `security.py`, `app.py`.

**Passos:**
1. `mt5_connector.py`: substitua o bloco

   ```python
   try:
       import MetaTrader5 as mt5
   except ImportError:
       ...
       mt5 = None
   ```

   por `from mt5_extracao.mt5_backend import get_mt5` e, no `__init__` do `MT5Connector`, após
   `self.config_path = config_path`, adicione `global mt5; mt5 = get_mt5(config_path)`. O restante
   do arquivo (153 usos de `mt5.`) continua funcionando sem alterações.
2. `historical_extractor.py`: troque `import MetaTrader5 as mt5` pelas constantes do módulo
   `mt5_extracao/timeframes.py` (crie-o já nesta tarefa, com os valores numéricos oficiais):
   `TIMEFRAME_M1 = 1`, `TIMEFRAME_M5 = 5`, `TIMEFRAME_M15 = 15`, `TIMEFRAME_M30 = 30`,
   `TIMEFRAME_H1 = 16385`, `TIMEFRAME_H4 = 16388`, `TIMEFRAME_D1 = 16408`,
   `TIMEFRAME_W1 = 32769`, `TIMEFRAME_MN1 = 49153`. Use `from . import timeframes as mt5`
   para não mexer nas linhas que usam `mt5.TIMEFRAME_*`. Para `last_error`, use `self.connector`.
3. `error_handler.py` (função `check_mt5_error`) e `security.py`: troque o `import MetaTrader5 as mt5`
   local por `from mt5_extracao.mt5_backend import get_mt5; mt5 = get_mt5()` e trate `None`.
4. `app.py`: remova `import MetaTrader5 as mt5` do bloco de imports do topo e, em
   `verificar_dependencias_criticas`, troque a verificação de `MetaTrader5` por `get_mt5() is not None`.
5. Teste em Windows (CI) e no Linux com a ponte (manual, T0.3).

**Critérios de aceite:** `grep -rn "import MetaTrader5" mt5_extracao app.py` só encontra
`mt5_backend.py`; `python -c "import mt5_extracao.historical_extractor"` funciona no Linux sem MT5;
o CI fica verde.

**Armadilhas:** constantes precisam existir no objeto remoto (`RemoteMT5.__getattr__` resolve nomes
em MAIÚSCULAS). Nunca use `hasattr(mt5, 'algo')` para decidir se o MT5 está disponível: no
`RemoteMT5` todo atributo "existe". Use `mt5 is not None`.

### T2.3 — Tirar Tkinter do núcleo

**Objetivo:** `mt5_connector.py` e `error_handler.py` importam `tkinter.messagebox` no topo, então
sem interface gráfica o núcleo não importa.

**Passos:**
1. `mt5_connector.py`: remova `from tkinter import messagebox` do topo. Nas duas perguntas ao
   usuário (procure `messagebox.askquestion`), troque por um callback opcional
   `self.ask_user: Callable[[str, str], bool] | None` recebido no `__init__` (padrão `None` → responde "não").
   Em `app.py`, passe `ask_user=lambda t, m: messagebox.askyesno(t, m)`.
2. `error_handler.py`: mova `from tkinter import messagebox` para dentro das funções que mostram
   diálogos, com `try/except ImportError` → apenas logar.

**Critérios de aceite:** `python -c "import mt5_extracao.mt5_connector, mt5_extracao.error_handler"`
funciona num Python **sem** tkinter.

### T2.4 — Gestão de processos multiplataforma

**Objetivo:** trocar `tasklist`/`taskkill`/`terminal64.exe` via `subprocess` por `psutil`,
com comportamento neutro no Linux.

**Passos:** em `mt5_connector.py`, nos métodos `_is_mt5_running`, `launch_mt5_as_admin`,
`_start_mt5_if_not_running`, `_fix_ipc_error` e `is_mt5_running_as_admin`:
- Encerrar processo: `for p in psutil.process_iter(['name']): if (p.info['name'] or '').lower() == 'terminal64.exe': p.terminate()` (e depois `psutil.wait_procs(procs, timeout=10)`).
- No Linux (`os.name != 'nt'`): `launch_mt5_as_admin` e `_start_mt5_if_not_running` retornam
  `False` com log "no Linux o terminal é iniciado pela ponte (mt5.initialize)". Não tente executar `.exe`.
- `is_admin`: no Linux retorne `os.geteuid() == 0`.

**Critérios de aceite:** nenhum `subprocess.run(["tasklist"...` / `["taskkill"...` restante.

### T2.5 — `initialize()` com `windows_path` no Linux

**Objetivo:** no Linux, `[MT5] path` é um caminho do Linux (para checar existência), mas o
`mt5.initialize(path=...)` roda **dentro do Wine** e precisa de `C:\...`.

**Passos:** em `MT5Connector._load_config`, leia também `self.mt5_windows_path = config.get('MT5', 'windows_path', fallback='') or self.mt5_path`.
Em `initialize`, nas estratégias que montam `params` com `path`, use `self.mt5_windows_path`
(e `ntpath.join`/`ntpath.dirname` em vez de `os.path` para esse valor). As verificações
`os.path.exists` continuam usando `self.mt5_path`.

**Critérios de aceite:** no Linux com a ponte, `MT5Connector().initialize()` retorna `True` (validação manual).

**Implementado:** com a ponte (`RemoteMT5`), `initialize()` não verifica nem inicia o processo
`terminal64.exe` no host: o próprio `mt5.initialize(path=C:\...)` inicia o terminal dentro do Wine.
A correção de IPC (que usa `taskkill`) é pulada nesse modo. Coberto por `tests/test_linux_bridge_e2e.py`
(ponte RPyC real + MetaTrader5 falso).

### T2.6 — Teste de paridade Windows × Linux

**Objetivo:** provar que a extração pela ponte é idêntica à local.

**Passos:** script `scripts/parity_check.py` que extrai `WIN$N` M1 de um dia fixo (ex.: o último
dia útil completo) e salva em Parquet (`df.to_parquet`, requer `pyarrow`, instale só no teste).
Rode no Windows e no Linux, na mesma conta/corretora, e compare com
`pd.testing.assert_frame_equal`. Documente o resultado em `docs/validacao_instaladores.md`.

---

## Fase 3 — CLI e automação

### T3.1 — CLI `mt5x`

**Objetivo:** operar sem interface gráfica (servidores, agendamento).

**Arquivos:** novo `mt5_extracao/cli.py`; entrada `mt5x = "mt5_extracao.cli:main"` (T3.3).

**Comandos (argparse, subcomandos):**

| Comando | Faz |
|---|---|
| `mt5x doctor` | delega para `mt5_extracao.bootstrap doctor` |
| `mt5x symbols [--group "*WIN*"]` | lista símbolos do MT5 |
| `mt5x extract --symbols WIN$N,WDO$N --tf M1 --from 2024-01-01 [--to 2024-06-30] [--indicators] [--overwrite] [--workers 4]` | extração histórica (bloqueante, com barra de progresso textual) |
| `mt5x update --symbols ... --tf M1` | atualização incremental (T1.3) |
| `mt5x export --table win_n_1_minuto --format csv\|excel\|parquet --out arquivo` | usa `DataExporter` |

**Passos:** a CLI instancia `DatabaseManager`, `MT5Connector`, `IndicatorCalculator` e
`HistoricalExtractor` do mesmo jeito que `app.py` (`load_config_and_db` e a criação do extrator),
**sem Tkinter**. `extract_data` é assíncrono: passe um `finished_callback` que aciona um
`threading.Event` e aguarde com `event.wait()`. Códigos de saída: 0 sucesso, 1 alguma falha, 2 uso incorreto.

**Critérios de aceite:** `mt5x --help` e cada subcomando `--help` funcionam; teste com o provedor falso (T4.3).

### T3.2 — Agendamento

**Objetivo:** manter a base atualizada automaticamente.

**Passos:** comando `mt5x schedule --every 15m --symbols ... --tf M1` que **imprime** (não
instala sozinho) o comando pronto para colar:
- Linux (cron): `*/15 9-18 * * 1-5 cd <projeto> && ./run.sh --cli update ... >> logs/cron.log 2>&1`
  (acrescente em `run.sh` o repasse `--cli` → `.venv/bin/python -m mt5_extracao.cli`).
- Windows: `schtasks /Create /SC MINUTE /MO 15 /TN "MT5 Extracao Update" /TR "\"<projeto>\run.bat\" --cli update ..."`.
Documente em `docs/instalacao.md`.

### T3.3 — `pyproject.toml`

**Objetivo:** substituir `setup.py` (hoje com dependências divergentes de `requirements.txt`).

**Passos:** crie `pyproject.toml` (setuptools) com `name = "mt5-extracao"`, `requires-python = ">=3.9"`,
`dependencies` = mesmas linhas de `requirements.txt` (com os mesmos marcadores `sys_platform`),
`[project.optional-dependencies] ta = ["pandas-ta"]`, `dev = ["pytest>=7.4", "pytest-timeout>=2.2"]`,
`[project.scripts] mt5x = "mt5_extracao.cli:main"`. Apague `setup.py`. Mantenha `requirements.txt`
(usado pelos instaladores) e adicione um teste que compara as duas listas de dependências.

### T3.4 — Timeframes como enum próprio

**Objetivo:** consolidar `mt5_extracao/timeframes.py` (criado em T2.2) como fonte única: `Enum`
com nome curto (`M1`, `H1`...), valor MT5, nome legível usado nas tabelas (`"1 minuto"`...) e
minutos por barra. Substitua os mapeamentos espalhados (`MT5Connector._convert_timeframe_to_mt5`,
`get_available_timeframes`). **Não mude os nomes legíveis existentes**, pois eles compõem os nomes das tabelas.

---

## Fase 4 — Qualidade e manutenção

### T4.1 — Remover arquivos mortos e unificar pontos de entrada

**Passos:**
1. Apague: `mt5_extracao/ui_manager.py.bak`, `fix_try.py`, `mt5_diagnostico.json` (contém caminho local de um usuário).
2. Transforme em *wrappers* finos, com aviso de descontinuação:
   - `install.py` → imprime "Use install.bat (Windows) ou ./install.sh (Linux)" e chama `python -m mt5_extracao.bootstrap configure`.
   - `verificador.py`, `check_mt5_config.py`, `mt5_troubleshooter.py` → chamam `python -m mt5_extracao.bootstrap doctor`.
   - `executar_mt5.py` → executa `app.py`.
3. Mova `test_mt5_connection.py`, `test_mt5_alternative.py`, `test_historical_extraction.py` e `mt5_workaround.py` para `scripts/manual/` (exigem MT5 real; não são testes automatizados).

**Critérios de aceite:** a raiz tem só: `app.py`, `install.bat`, `install.sh`, `run.bat`, `run.sh`, wrappers, docs e configs.

### T4.2 — Logging centralizado

**Objetivo:** hoje cada módulo cria `logs/` e seus próprios handlers ao ser importado, o que
duplica linhas no console e cria pastas no diretório atual.

**Passos:** crie `mt5_extracao/logging_setup.py` com `setup_logging(log_dir, level)` (um
`RotatingFileHandler` de 5 MB × 5 arquivos e um `StreamHandler`). Remova de **todos** os módulos o
bloco `if not log.handlers: ...` e o `os.makedirs("logs")`; deixe só `log = logging.getLogger(__name__)`.
Chame `setup_logging` apenas em `app.py`, na CLI e nos scripts.

### T4.3 — Provedor MT5 falso + testes

**Objetivo:** testar extrator e banco sem MT5.

**Passos:** `tests/fakes.py` com `FakeMT5` (mesma API usada pelo código: `initialize`, `shutdown`,
`last_error`, `symbols_get`, `symbol_info`, `copy_rates_range`, `copy_rates_from_pos`, constantes
`TIMEFRAME_*`) gerando barras sintéticas determinísticas (M1 das 09:00 às 18:00 em dias úteis).
Um *fixture* do pytest faz `monkeypatch` de `mt5_extracao.mt5_backend._instance = FakeMT5()`.
Escreva testes para: extração de 3 meses em blocos; retomada (T1.2); atualização (T1.3); falha
simulada num bloco.

**Critérios de aceite:** cobertura ≥ 60% em `database_manager.py`, `historical_extractor.py` e
`mt5_backend.py` (`pytest --cov`, adicione `pytest-cov` ao `requirements-dev.txt`).

### T4.4 — Documentação

Atualize `docs/arquitetura.md` (diagrama com `mt5_backend`, ponte e bootstrap) e o `README.md`
(estado real das funcionalidades). Remova do README as afirmações que não forem verdade.

### T4.5 — Remover módulos "enhanced" não usados

`integrated_services.py`, `enhanced_calculation_service.py`, `enhanced_indicators.py`,
`performance_optimizer.py` e `market_data_analyzer.py` somam cerca de 3 mil linhas pouco ligadas ao fluxo.
**Passos:** com `grep`, liste quem usa cada um. Remova o que não é alcançável a partir de `app.py`/CLI,
ou integre o que for útil ao `IndicatorCalculator`. Faça **um PR por módulo**.

---

## Fase 5 — Escala e novos dados

| ID | Resumo do que fazer | Aceite |
|---|---|---|
| T5.1 | `DataExporter`: formatos `parquet` (via `pyarrow`, opcional) e `duckdb` (arquivo `.duckdb` com uma tabela por símbolo). Dependências em `requirements-optional.txt` | Exportar 1 milhão de linhas em < 10 s |
| T5.2 | `DatabaseManager` com `db_type = postgresql`: string de conexão em `[DATABASE] url` e senha no `.env` (`DB_PASSWORD`). Upsert com `sqlalchemy.dialects.postgresql.insert`. Se a extensão TimescaleDB existir, `create_hypertable` na criação da tabela | Testes com `testcontainers` ou serviço `postgres` no CI |
| T5.3 | Ticks: `copy_ticks_range(symbol, from, to, COPY_TICKS_ALL)` em blocos de 1 dia; tabela `<símbolo>_ticks` com PK `(time_msc)`; mesma `_extraction_log` | Retomada funciona como em T1.2 |
| T5.4 | Book: `market_book_add` + `market_book_get` em laço (intervalo configurável); tabela `<símbolo>_book` (`time_msc`, `type`, `price`, `volume`) | Coleta por 1 h sem crescer memória |
| T5.5 | `CsvExternalSource(ExternalDataSource)`: lê CSVs de uma pasta (`[FALLBACK] csv_dir`) com colunas `time,open,high,low,close,real_volume`; tipo `Csv` em `[FALLBACK] external_source_m1_type` e a fábrica em `app.py` (onde hoje escolhe `DummyExternalSource`) | Fallback M1 preenche lacuna a partir do CSV |
| T5.6 | Pastas de dados por usuário com `platformdirs` (opcional via `[APP] data_dir`). **Migração:** se `database/mt5_data.db` existir no projeto, continue usando-o | Instalações antigas continuam funcionando sem ação do usuário |

---

## Checklist de revisão (use em todo PR)

- [ ] Só os arquivos listados na tarefa foram alterados (ou justificativa no PR).
- [ ] `pytest` passa localmente; nenhum teste foi removido ou marcado como `skip` sem motivo técnico.
- [ ] Mensagens ao usuário em português, com a ação de correção.
- [ ] Nenhum segredo, caminho pessoal, `config.ini`, `.env` ou `.db` no commit.
- [ ] Windows **e** Linux considerados (caminhos com `pathlib`/`os.path`, nada de `\\` fixo fora de strings do Windows).
- [ ] Painel de status deste documento atualizado.
