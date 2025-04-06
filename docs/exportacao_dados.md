# Documentação: Exportação de Dados

## Visão Geral
O sistema de exportação de dados permite extrair os dados coletados do MT5 para formatos como CSV e Excel, facilitando a análise posterior em outras ferramentas.

## Funcionalidades

### Exportação para CSV
- Exporta uma tabela específica para um arquivo CSV
- Permite aplicar filtros para selecionar apenas os dados desejados
- Adiciona timestamp aos nomes de arquivos para facilitar o controle de versão

### Exportação para Excel (XLSX)
- Exporta uma tabela específica para uma planilha Excel
- Preserva os tipos de dados originais quando possível
- Permite aplicar filtros para selecionar apenas os dados desejados

### Exportação Múltipla
- Exporta várias tabelas de uma só vez
- No formato Excel: cria uma planilha separada para cada tabela
- No formato CSV: cria um arquivo para cada tabela em um diretório com um arquivo de índice

## Como Usar

### Na Interface Gráfica

1. Acesse o menu "Dados" > "Exportar Dados" e selecione o formato desejado:
   - Exportar para CSV
   - Exportar para Excel
   - Exportar Múltiplas Tabelas

2. Para exportação única:
   - Selecione a tabela desejada na lista
   - Opcionalmente, adicione filtros usando a sintaxe SQL WHERE
   - Clique em "Exportar" e escolha o local para salvar o arquivo

3. Para exportação múltipla:
   - Selecione as tabelas desejadas na lista
   - Escolha o formato de saída (Excel ou CSV)
   - Clique em "Exportar" e escolha o local para salvar

### Exemplos de Filtros

Os filtros seguem a sintaxe SQL WHERE. Exemplos:

- `time > '2023-01-01'` - Dados a partir de 1º de janeiro de 2023
- `time BETWEEN '2023-01-01' AND '2023-12-31'` - Dados do ano de 2023
- `close > open` - Apenas candles de alta (fechamento maior que abertura)
- `volume > 1000` - Apenas períodos com volume maior que 1000

## Notas Importantes

- Os arquivos são salvos por padrão no diretório "exports/"
- Para CSV, use ponto-e-vírgula (;) como separador ao abrir no Excel para evitar problemas com separadores decimais
- Para exportação múltipla em CSV, um arquivo JSON de índice é criado para mapear as tabelas aos arquivos

## Solução de Problemas

### Arquivos vazios ou faltando dados
- Verifique se a tabela contém dados no período desejado
- Tente exportar sem filtros para confirmar que há dados disponíveis
- Verifique permissões de escrita no diretório de destino

### Erros na exportação para Excel
- Certifique-se de que a biblioteca openpyxl está instalada: `pip install openpyxl`
- Verifique se o arquivo não está aberto em outro programa

### Problemas com caracteres especiais
- Use a codificação UTF-8 ao abrir os arquivos CSV em editores de texto
- Para Excel, os caracteres especiais devem ser tratados automaticamente 