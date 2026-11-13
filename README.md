# MT5 Extração de Dados

## Composição conferida em 13/11/2026

A PR308 reúne dados públicos, Laboratório, Dashboard, Drive e Recursos das
frentes revisadas. O código composto `0e4f4c6` passou nos seis jobs de produto,
incluindo Linux 4.636/473 e Windows 4.904/205 aprovados/não executados. A prévia
do atalho e menu é `2b3b274`, com 365 arquivos de execução idênticos por hash.
GUI XWayland visível e launcher oficial foram exercitados em dados próprios.

A fila tem 20 itens, com 14 concluídos e seis bloqueados. A observação controlada
de 30 min foi aprovada com solicitações instrumentadas. Entrada física,
acessibilidade, login, integração nativa e qualificação financeira possuem
aceites próprios.
Pacotes/reunião e proteção da base principal dependem das decisões registradas.
A causa dos reinícios do host permanece indeterminada. A fonte de continuidade
reúne owners, SHAs, recibos, limites e próximos passos. [Continuidade](CONTINUAR_AQUI.md)

## Estado conferido — 07/11/2026

Prévia efetiva conferida em 07/11: `650656fac67d` (PR300), 354 hashes sem divergência; retorno `57d13ee` preservado. O atalho não foi atualizado neste lote. HADOUKEN `d4d389e` preservado em branch; Safety `27b8599` detached.

Drive: [PR302](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/302), SQLite cifrado com código em entrada privada, retry sem duplicata e restauração sintética conferida. 99 testes integrados e 28 focados no ajuste final do cofre, com sobreposição; doctor offline sem falhas. Contas múltiplas, pacotes por ativo e reunião seguem em PRs próprias, antes do teste real. Drive ainda não está no atalho. [Fontes e limites](CONTINUAR_AQUI.md). As seções datadas abaixo são fotografias históricas.

## Estado de desenvolvimento neste host — 03/11/2026

A raiz canônica é `/home/misael/Projects/MT5X`: este App fica em `repos/app`,
HADOUKEN em `repos/hadouken` e Safety em `repos/safety`. O manifesto do App fixa
HADOUKEN `d4d389e` (fonte S09 2.13 `51358a1`) e Safety `27b8599`. A fonte
operacional E14 publicada é o App `0993ae7` na [PR279](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/279),
com HADOUKEN na [PR84](https://github.com/Misael-art/projeto-indicadores-mt5/pull/84)
e Safety na [PR11](https://github.com/Misael-art/mt5x-safety/pull/11). São
revisões abertas; não equivalem a merge ou aceite operacional.

O atalho de desenvolvimento instalado ainda executa a prévia `57d13ee`. No
checkout, `./run.sh` abre a GUI e `./run.sh --cli` abre a CLI. A branch local
separada de correção da GUI permite `./run.sh --offline --config <arquivo-de-QA>`
para uma configuração/base isolada; ela ainda não está nesta fonte.
O estudo documental T13.4-P00 e os incrementos P01–P07 estão em branches locais
encadeadas, com implementação e ensaios sintéticos até os consumidores. A GUI
da composição local de QA anterior exibiu um recibo sintético persistido em
Padrões pelo launcher; essa composição não está instalada;
nenhum candidato ou dado real foi aprovado por isso. A branch LAB-03 acrescenta
regressão de alinhamento. Push/PR dessas branches aguardam ajuste do guarda
central de publicação. No CI de `19627c3`, Linux e os demais jobs passaram;
Windows falhou no gráfico estreito. O corretivo está na ponta `0993ae7`:
PostgreSQL, instaladores e lint passaram; Ubuntu/Windows ainda executavam na
consulta de 04/11 00:50 UTC. O S09 2.13
compilou e conciliou backtest/replay no Tester Linux/Wine com pacote sintético;
DEMO ao vivo, netting, Windows físico e dados reais elegíveis têm aceites separados.
[Fontes, testes e próximos passos](CONTINUAR_AQUI.md) ·
[roadmap](docs/ROADMAP.md) · [plano](docs/PLANO_DE_TRABALHO.md).

**Retomar em um host limpo, sem o disco antigo ou SD:**
[comece por aqui](CONTINUAR_AQUI.md). Wrappers Linux/Windows, fontes fixadas,
recuperação privada pela conta ChatGPT e prompts de continuidade. O Laboratório
permanece incompleto; os próximos recortes estão registrados no roadmap.

Construtor de bases de dados do MetaTrader 5 para **Windows e Linux**: oferece instaladores, extrai e
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
- **Estratégias e triagem de ativos**: seis estratégias clássicas, backtest com custos configurados,
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

> No Linux, o aplicativo acessa o MT5 do Wine pela ponte RPyC, iniciada pelo `run.sh` fora do modo `--offline`.
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
- Avisos do sistema (pop-up) a cada ação concluída: `[APP] notifications = true`; `mt5x robots monitor <robô> --watch`
- Painel visual dos resultados por ativo, timeframe e robô: `mt5x panel --open` (veja [robôs](docs/robos.md))
- MT5 oculto, sem ocupar a tela: `[MT5] hidden = true` (veja [instalação](docs/instalacao.md)); `mt5x mt5 status|show`
- Montar timeframes maiores de uma base menor: `mt5x resample --symbols EURUSD --base M1 --tf M5,M15,H1`
- [Robôs: catálogo, backtest, bateria de segurança e conta demo](docs/robos.md)
- [SDK para desenvolver robôs MQL5](docs/sdk/README.md): tutorial, contrato, API e o que ainda é lacuna
- [Roadmap](docs/ROADMAP.md) e [Plano de trabalho detalhado](docs/PLANO_DE_TRABALHO.md)
- [Regras para agentes/contribuidores](AGENTS.md)

- [Plano de Extração Histórica (Original)](docs/plano_extracao_historica.md)
- [Plano Fase 2: Fallback M1 e Chunking Dinâmico](docs/plano_fallback_m1.md)
- [Arquitetura](docs/arquitetura.md)
- [Exportação de Dados](docs/exportacao_dados.md)
- [Gerenciamento de Credenciais](docs/credenciais.md)

## Licença

Este projeto é licenciado sob os termos da licença MIT.
Agentes e engenheiros: [workflow completo por CLI/SDK, sem cliques](docs/agente.md).
