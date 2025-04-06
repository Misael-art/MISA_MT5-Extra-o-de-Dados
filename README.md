# MT5 Extração de Dados

Aplicação para extração e armazenamento de dados financeiros do MetaTrader 5.

## Funcionalidades

- **Conexão com MT5**: Interface com o MetaTrader 5 para extração de dados.
- **Coleta de Dados**: Extração de dados históricos e em tempo real.
- **Armazenamento em Banco**: Armazena dados em banco SQLite local.
- **Exportação**: Exporta dados para formatos CSV e Excel.
- **Gerenciamento de Credenciais**: Armazena credenciais de forma segura.
- **Interface Gráfica**: Interface amigável para interação com o usuário.

## Novidades

- **Exportação de Dados**: Nova funcionalidade para exportação de dados para CSV e Excel.
- **Gerenciamento de Credenciais**: Armazenamento seguro de credenciais via variáveis de ambiente.
- **Tratamento de Erros**: Sistema robusto de tratamento e registro de erros.

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
3. Configure o arquivo `.env` com suas credenciais (opcional)

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

## Documentação

Para mais informações, consulte a documentação em `docs/`:

- [Exportação de Dados](docs/exportacao_dados.md)
- [Gerenciamento de Credenciais](docs/credenciais.md)

## Licença

Este projeto é licenciado sob os termos da licença MIT. 