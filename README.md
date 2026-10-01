# MT5 Extração de Dados

Construtor de bases de dados do MetaTrader 5 para **Windows e Linux**: instala sozinho, extrai e
mantém atualizado o histórico OHLCV (com indicadores) em SQLite, pela interface gráfica ou pela
linha de comando, e ajuda a pesquisar estratégias sobre a base construída.

## Funcionalidades

- **Instalação automática**: Python, dependências, MetaTrader 5 e configuração inicial (no Linux, MT5 no Wine com uma ponte para o pacote `MetaTrader5`).
- **Extração histórica confiável**:
  - busca em blocos, gravando cada bloco assim que chega;
  - retoma de onde parou e reextrai sem duplicar;
  - roda vários símbolos em paralelo.
- **Atualização incremental**: do último registro até agora ("Atualizar até agora" na interface, `mt5x update` agendável via cron/Agendador de Tarefas).
- **Qualidade dos dados**: relatório por bloco (OHLC inválido, volume zero, duplicatas, lacunas no pregão).
- **Banco SQLite** (padrão, um arquivo) **ou PostgreSQL/TimescaleDB** (servidor):
  - tabela por símbolo e timeframe, com nomes sem colisão entre símbolos;
  - base de tempo explícita (horário da corretora ou UTC).
- **Ticks** (bid/ask/last) em blocos com retomada: `mt5x ticks`, tabela `<símbolo>_ticks`.
- **Book de ofertas** (DOM): snapshots em intervalos regulares com `mt5x book`, tabela `<símbolo>_book`.
- **Indicadores técnicos** opcionais na extração e coleta M1 em tempo real.
- **Exportação** para CSV, Excel, Parquet e DuckDB (interface ou `mt5x export`).
- **Credenciais** só no arquivo `.env` (privado, fora do git).
- **Linha de comando `mt5x`** para servidores e automação; códigos de saída padronizados.
- **Estratégias e triagem de ativos**: seis estratégias clássicas, backtest com custos reais,
  validação (walk-forward, robustez, Monte Carlo) e triagem de ativos com relatório HTML.
  Veja [docs/estrategias.md](docs/estrategias.md). Não envia ordens.

## Instalação (automática)

| Sistema | Como instalar | Como abrir |
|---|---|---|
| **Windows 10/11** | Duplo clique em `install.bat` | Atalho **MT5 Extração** na Área de Trabalho ou `run.bat` |
| **Linux** (Ubuntu, Debian, Fedora, Arch, openSUSE) | `./install.sh` | Menu de aplicativos ou `./run.sh` |

O instalador baixa e configura o Python, as dependências, o **MetaTrader 5** (no Linux, via Wine,
com uma ponte para o pacote `MetaTrader5`) e gera `config/config.ini`. Pode ser executado de novo
a qualquer momento: as etapas prontas são puladas e a sua configuração é preservada.

Detalhes, opções e solução de problemas: **[docs/instalacao.md](docs/instalacao.md)**.

Diagnóstico do ambiente:

```
.venv/bin/python -m mt5_extracao.bootstrap doctor      # Linux
.venv\Scripts\python -m mt5_extracao.bootstrap doctor  # Windows
```

> No Linux, o aplicativo acessa o MT5 do Wine pela ponte RPyC, iniciada automaticamente pelo `run.sh`.
> A validação com o terminal real no Wine está pendente (tarefa T0.3/T2.6 do [plano](docs/PLANO_DE_TRABALHO.md)).

## Uso

```
run.bat        # Windows
./run.sh       # Linux
```

Linha de comando (servidores, agendamento): `./run.sh --cli extract --symbols 'WIN$N' --tf M1 --from 2024-01-01`,
`./run.sh --cli update ...`, `./run.sh --cli schedule --every 15m ...` — veja [docs/instalacao.md](docs/instalacao.md#linha-de-comando-sem-interface-gráfica).

### Extração de Dados

1. Selecione os símbolos desejados
2. Escolha o timeframe
3. Configure o período desejado
4. Inicie a extração

### Exportação de Dados

1. Acesse o menu "Arquivo" > "Exportar Dados" e escolha CSV ou Excel
2. Selecione a tabela a exportar
3. Adicione filtros (opcional, ex.: `time > '2024-01-01'`)
4. Escolha o local para salvar

## Configuração Avançada (`config/config.ini`)

O arquivo é gerado pelo instalador (referência completa em `config/config.ini.example`) e permite ajustar alguns comportamentos:

- **`[MT5]`**:
   - `path`: Caminho para a instalação do MetaTrader 5.
   - Credenciais **não** ficam aqui: use `.env` (`MT5_LOGIN`, `MT5_PASSWORD`, `MT5_SERVER`), criado pelo assistente `python -m mt5_extracao.bootstrap configure`.
- **`[BRIDGE]`** (Linux): `enabled`, `host`, `port`, `wine_python` da ponte RPyC para o MT5 no Wine.
- **`[DATABASE]`**:
   - `type`: `sqlite` (padrão) ou `postgresql`.
   - `path`: Caminho para o arquivo do banco de dados SQLite.
   - `url`: endereço do PostgreSQL, ex.: `postgresql://usuario@localhost:5432/mt5`. A senha vai em
     `DB_PASSWORD` no arquivo `.env` (nunca no `config.ini`). Requer o pacote opcional `psycopg`.
     Se a extensão TimescaleDB estiver instalada no banco, as tabelas de barras viram hypertables.
- **`[FALLBACK]`**:
   - `external_source_m1_fallback_enabled`: `True` ou `False` para habilitar o fallback para dados M1 se a extração MT5 falhar.
   - `external_source_m1_type`: `Csv` preenche blocos M1 que o MT5 não entregou com arquivos da pasta
     `csv_dir` (`<símbolo>.csv` com `time,open,high,low,close,...` ou a exportação de barras do próprio
     MT5, "Barras → Exportar"); `Dummy` só para testes.
   - `csv_dir`: pasta dos arquivos CSV.
- **`[EXTRACTION]`**:
   - `chunk_days_m1`: Tamanho do bloco (em dias) para extração M1 (padrão: 30).
   - `chunk_days_m5_m15`: Tamanho do bloco para M5/M15 (padrão: 90).
   - `chunk_days_default`: Tamanho do bloco para outros timeframes (padrão: 365).
- **`[APP]`**: horário do pregão (`session_start`, `session_end`, usados no relatório de qualidade),
  `time_basis` (`broker` ou `utc`), `broker_utc_offset` e `data_dir` (onde fica o banco: vazio = pasta do
  projeto; `auto` = pasta de dados do usuário; ou um caminho — um banco que já exista no projeto continua
  sendo usado). Caminhos relativos valem a partir da pasta do projeto, então o `mt5x` funciona de qualquer pasta.
- **`[STRATEGY]`** e **`[SCREENER]`**: capital, risco, custos e cortes dos filtros de estratégia
  (veja [docs/estrategias.md](docs/estrategias.md)).

Logs: `logs/mt5_extracao.log` (gira a cada 5 MB).

## Documentação

Para mais detalhes técnicos e planos, consulte a documentação em `docs/`:

- [Instalação automática](docs/instalacao.md)
- [Estratégias, backtest e triagem de ativos](docs/estrategias.md)
- [Roadmap](docs/ROADMAP.md) e [Plano de trabalho detalhado](docs/PLANO_DE_TRABALHO.md)
- [Regras para agentes/contribuidores](AGENTS.md)

- [Plano de Extração Histórica (Original)](docs/plano_extracao_historica.md)
- [Plano Fase 2: Fallback M1 e Chunking Dinâmico](docs/plano_fallback_m1.md)
- [Arquitetura](docs/arquitetura.md)
- [Exportação de Dados](docs/exportacao_dados.md)
- [Gerenciamento de Credenciais](docs/credenciais.md)

## Licença

Este projeto é licenciado sob os termos da licença MIT. 