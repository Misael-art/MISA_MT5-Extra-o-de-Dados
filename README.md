# MT5 Extração de Dados

Aplicação para extração e armazenamento de dados financeiros do MetaTrader 5.

## Funcionalidades

- **Conexão com MT5**: Interface com o MetaTrader 5 para extração de dados.
- **Coleta de Dados**: Extração de dados em tempo real (ticks) e históricos (OHLCV).
- **Extração Histórica Robusta**: Módulo `HistoricalExtractor` com busca em blocos (chunking) configurável, retentativas e paralelização. Suporta fallback para fontes externas (configurável) para dados M1.
- **Armazenamento em Banco**: Armazena dados em banco SQLite local, com criação/atualização automática de schema.
- **Cálculo de Indicadores**: Calcula indicadores técnicos básicos e avançados.
- **Exportação**: Exporta dados para formatos CSV e Excel.
- **Gerenciamento de Credenciais**: Armazena credenciais de forma segura (atualmente via `config.ini`).
- **Interface Gráfica**: Interface amigável para interação com o usuário.

## Novidades

- **Exportação de Dados**: Nova funcionalidade para exportação de dados para CSV e Excel.
- **Gerenciamento de Credenciais**: Armazenamento seguro de credenciais via variáveis de ambiente.
- **Tratamento de Erros**: Sistema robusto de tratamento e registro de erros.
- **Extração Histórica Aprimorada**: Implementação do `HistoricalExtractor` com chunking, retries e paralelização.
- **Fallback M1 e Chunking Dinâmico**: Adicionada a capacidade de usar fontes externas como fallback para M1 e configuração dinâmica do tamanho dos blocos de extração.
- **Correção de Schema DB**: Resolvido problema com nomes de colunas contendo caracteres especiais e garantida a criação de tabelas com schema completo.

## Requisitos

- Python 3.8+
- MetaTrader 5 instalado
- Bibliotecas listadas em `requirements.txt`

## Instalação

1. Clone o repositório
2. Instale as dependências:
   ```
   pip install -r requirements.txt
   ```
3. Configure o arquivo `config/config.ini` com o caminho do MT5 e, opcionalmente, credenciais e configurações avançadas (veja abaixo).
## Uso

Execute o aplicativo principal:

```
python app.py
```

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

O arquivo `config/config.ini` permite ajustar alguns comportamentos:

- **`[MT5]`**:
   - `path`: Caminho para a instalação do MetaTrader 5.
   - `login`, `password`, `server`: Credenciais (opcional, se não fornecidas, tentará conectar sem login específico).
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

- [Plano de Extração Histórica (Original)](docs/plano_extracao_historica.md)
- [Plano Fase 2: Fallback M1 e Chunking Dinâmico](docs/plano_fallback_m1.md)
- [Arquitetura Geral](docs/arquitetura.md) (Pode precisar de atualização)
- [Exportação de Dados](docs/exportacao_dados.md)
- [Gerenciamento de Credenciais](docs/credenciais.md)

## Licença

Este projeto é licenciado sob os termos da licença MIT. 