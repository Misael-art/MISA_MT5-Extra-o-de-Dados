# MT5 Extração de Dados

Aplicação para extração e armazenamento de dados financeiros do MetaTrader 5.

## Funcionalidades

- **Conexão com MT5**: Interface com o MetaTrader 5 para extração de dados.
- **Coleta de Dados**: Extração de dados em tempo real (ticks) e históricos (OHLCV).
- **Extração Histórica Robusta**: Módulo `HistoricalExtractor` com busca em blocos (chunking) configurável, retentativas e paralelização. Suporta fallback para fontes externas (configurável) para dados M1.
- **Armazenamento em Banco**: Armazena dados em banco SQLite local, com criação/atualização automática de schema.
- **Cálculo de Indicadores**: Calcula indicadores técnicos básicos e avançados.
- **Exportação**: Exporta dados para formatos CSV e Excel.
- **Gerenciamento de Credenciais**: Credenciais no arquivo `.env` (privado, fora do git).
- **Interface Gráfica**: Interface amigável para interação com o usuário.

## Novidades

- **Exportação de Dados**: Nova funcionalidade para exportação de dados para CSV e Excel.
- **Gerenciamento de Credenciais**: Armazenamento seguro de credenciais via variáveis de ambiente.
- **Tratamento de Erros**: Sistema robusto de tratamento e registro de erros.
- **Extração Histórica Aprimorada**: Implementação do `HistoricalExtractor` com chunking, retries e paralelização.
- **Fallback M1 e Chunking Dinâmico**: Adicionada a capacidade de usar fontes externas como fallback para M1 e configuração dinâmica do tamanho dos blocos de extração.
- **Correção de Schema DB**: Resolvido problema com nomes de colunas contendo caracteres especiais e garantida a criação de tabelas com schema completo.

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

1. Acesse o menu "Dados" > "Exportar Dados"
2. Escolha o formato desejado (CSV ou Excel)
3. Selecione a tabela a exportar
4. Adicione filtros (opcional)
5. Escolha o local para salvar

## Configuração Avançada (`config/config.ini`)

O arquivo é gerado pelo instalador (referência completa em `config/config.ini.example`) e permite ajustar alguns comportamentos:

- **`[MT5]`**:
   - `path`: Caminho para a instalação do MetaTrader 5.
   - Credenciais **não** ficam aqui: use `.env` (`MT5_LOGIN`, `MT5_PASSWORD`, `MT5_SERVER`), criado pelo assistente `python -m mt5_extracao.bootstrap configure`.
- **`[BRIDGE]`** (Linux): `enabled`, `host`, `port`, `wine_python` da ponte RPyC para o MT5 no Wine.
- **`[DATABASE]`**:
   - `type`: Tipo do banco (atualmente apenas `sqlite`).
   - `path`: Caminho para o arquivo do banco de dados SQLite.
- **`[FALLBACK]`**:
   - `external_source_m1_fallback_enabled`: `True` ou `False` para habilitar o fallback para dados M1 se a extração MT5 falhar.
   - `external_source_m1_type`: Tipo da fonte externa (atualmente suporta `Dummy` para testes).
- **`[EXTRACTION]`**:
   - `chunk_days_m1`: Tamanho do bloco (em dias) para extração M1 (padrão: 30).
   - `chunk_days_m5_m15`: Tamanho do bloco para M5/M15 (padrão: 90).
   - `chunk_days_default`: Tamanho do bloco para outros timeframes (padrão: 365).

## Documentação

Para mais detalhes técnicos e planos, consulte a documentação em `docs/`:

- [Instalação automática](docs/instalacao.md)
- [Roadmap](docs/ROADMAP.md) e [Plano de trabalho detalhado](docs/PLANO_DE_TRABALHO.md)
- [Regras para agentes/contribuidores](AGENTS.md)

- [Plano de Extração Histórica (Original)](docs/plano_extracao_historica.md)
- [Plano Fase 2: Fallback M1 e Chunking Dinâmico](docs/plano_fallback_m1.md)
- [Arquitetura Geral](docs/arquitetura.md) (Pode precisar de atualização)
- [Exportação de Dados](docs/exportacao_dados.md)
- [Gerenciamento de Credenciais](docs/credenciais.md)

## Licença

Este projeto é licenciado sob os termos da licença MIT. 