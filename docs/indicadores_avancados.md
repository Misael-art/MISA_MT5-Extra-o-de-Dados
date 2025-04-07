# Documentação de Indicadores Avançados

## Visão Geral

O sistema de indicadores avançados expande significativamente as capacidades de análise da plataforma MT5 Extração, fornecendo ferramentas de análise técnica, estatística e contextual de alta qualidade para os dados financeiros coletados.

## Módulos Principais

### EnhancedIndicatorCalculator

Este módulo centraliza e integra todos os indicadores disponíveis, atuando como uma interface unificada para cálculos de indicadores básicos e avançados.

```python
from mt5_extracao.enhanced_indicators import EnhancedIndicatorCalculator

calculator = EnhancedIndicatorCalculator()
df_with_indicators = calculator.calculate_all_indicators(df)
```

### AdvancedIndicators

Implementa indicadores técnicos avançados que vão além dos indicadores básicos (RSI, MACD, Médias Móveis).

```python
from mt5_extracao.advanced_indicators import AdvancedIndicators

advanced_calc = AdvancedIndicators()
stochastic = advanced_calc.stochastic_oscillator(df['high'], df['low'], df['close'])
adx_result = advanced_calc.adx(df['high'], df['low'], df['close'])
```

### MarketDataAnalyzer

Fornece análises contextuais relacionadas ao mercado, como sessões de negociação, volatilidade por horário e eventos econômicos.

```python
from mt5_extracao.market_data_analyzer import MarketDataAnalyzer

analyzer = MarketDataAnalyzer()
df_with_market_context = analyzer.analyze_market_data(df, symbol='WIN$N')
```

## Indicadores Técnicos Disponíveis

### Indicadores Básicos (já existentes)
- RSI (Índice de Força Relativa)
- MACD (Convergência/Divergência de Médias Móveis)
- Médias Móveis (MA20)
- Bandas de Bollinger
- ATR (Average True Range)
- True Range

### Indicadores Avançados (novos)
- **Oscilador Estocástico (%K e %D)**: Mede o nível de preço atual em relação à faixa de preços durante um período específico
- **ADX (Average Directional Index)**: Mede a força da tendência, independentemente da direção
- **CCI (Commodity Channel Index)**: Identifica condições de sobrecompra e sobrevenda
- **Níveis de Fibonacci**: Importantes níveis de suporte e resistência baseados na sequência de Fibonacci

### Análises Estatísticas
- **Skewness (Assimetria)**: Mede a assimetria da distribuição dos retornos
- **Kurtosis (Curtose)**: Mede as "caudas" da distribuição dos retornos
- **Volatilidade**: Calculada com base nos retornos logarítmicos

### Análise de Volume
- **OBV (On-Balance Volume)**: Relaciona volume com mudanças de preço
- **Razão de Volume**: Compara o volume atual com a média móvel
- **VWAP (Volume Weighted Average Price)**: Preço médio ponderado pelo volume
- **PVT (Price-Volume Trend)**: Correlaciona volume com variações percentuais de preço

### Identificação de Padrões de Candlestick
- Doji
- Martelo e Martelo Invertido
- Engolfo de Alta e Baixa
- Estrela da Manhã e Estrela da Noite

### Análise de Tendência
- Direção da Tendência (alta, baixa, neutra)
- Força da Tendência
- Duração da Tendência
- Inclinação da Regressão Linear
- R² (coeficiente de determinação)

### Suporte e Resistência
- Detecção automática de níveis de suporte
- Detecção automática de níveis de resistência

## Análise Contextual do Mercado

### Sessões de Mercado
- Identificação automática da sessão (pré-mercado, regular, após mercado)
- Detecção de períodos de alta/baixa volatilidade

### Eventos Econômicos
- Integração com calendário de eventos econômicos
- Identificação de eventos próximos ao período analisado

### Informações de Contratos Futuros
- Dias até a expiração do contrato
- Identificação de último dia de negociação

### Análise Temporal
- Identificação de padrões sazonais (dia da semana, hora do dia)
- Identificação de períodos especiais (final de mês, final de semana)

## Como Usar

### Cálculo de Todos os Indicadores

Para calcular todos os indicadores disponíveis em um DataFrame:

```python
from mt5_extracao.enhanced_indicators import EnhancedIndicatorCalculator

calculator = EnhancedIndicatorCalculator()
df_with_indicators = calculator.calculate_all_indicators(
    df,
    include_market_context=True,
    include_advanced_stats=True,
    include_candle_patterns=True,
    include_volume_analysis=True,
    include_trend_analysis=True,
    include_support_resistance=True,
    period=20
)
```

### Cálculo de Indicadores Específicos

Para calcular apenas indicadores específicos:

```python
calculator = EnhancedIndicatorCalculator()
df_with_indicators = calculator.calculate_advanced_indicators(
    df,
    indicators=['stochastic', 'adx', 'fibonacci']
)
```

### Análise de Padrões de Preço

Para identificar padrões complexos no histórico de preços:

```python
patterns = calculator.analyze_price_patterns(df)

# Exemplo de uso dos padrões identificados
for pattern_name, occurrences in patterns['candle_patterns'].items():
    print(f"Padrão {pattern_name} encontrado {len(occurrences)} vezes")
```

### Análise Contextual de Mercado

Para adicionar contexto de mercado ao DataFrame:

```python
from mt5_extracao.market_data_analyzer import MarketDataAnalyzer

analyzer = MarketDataAnalyzer(timezone='America/Sao_Paulo')
df_with_context = analyzer.analyze_market_data(df, symbol='WIN$N')

# Verificar sessões de mercado
session_counts = df_with_context['market_session'].value_counts()
print(f"Sessões de mercado: {session_counts}")

# Verificar volatilidade por período
if 'volatility_regime_20' in df_with_context.columns:
    regime_counts = df_with_context['volatility_regime_20'].value_counts()
    print(f"Regimes de volatilidade: {regime_counts}")
```

## Considerações de Desempenho

O cálculo de todos os indicadores avançados pode ser intensivo em termos de processamento. Para otimizar o desempenho:

1. **Cálculo seletivo**: Use apenas os indicadores necessários para sua análise
2. **Cache de resultados**: Armazene resultados intermediários para reutilização
3. **Processamento em lote**: Para grandes volumes de dados, processe em lotes menores

Por exemplo, para dados históricos extensos, utilize:

```python
# Processa em lotes de 1000 registros
batch_size = 1000
results = []

for i in range(0, len(df), batch_size):
    batch = df.iloc[i:i+batch_size].copy()
    processed_batch = calculator.calculate_all_indicators(
        batch,
        include_candle_patterns=False,  # Desativa cálculos intensivos
        include_support_resistance=False
    )
    results.append(processed_batch)

# Combina os resultados
final_df = pd.concat(results, ignore_index=True)
```

## Exemplo de Análise Completa

```python
import pandas as pd
from mt5_extracao.enhanced_indicators import EnhancedIndicatorCalculator
from mt5_extracao.market_data_analyzer import MarketDataAnalyzer

# Carrega dados
df = connector.get_historical_data('WIN$N', timeframe='1min', bars=1000)

# Calcula indicadores técnicos
calculator = EnhancedIndicatorCalculator()
df_with_indicators = calculator.calculate_all_indicators(df)

# Adiciona contexto de mercado
analyzer = MarketDataAnalyzer()
df_complete = analyzer.analyze_market_data(df_with_indicators, symbol='WIN$N')

# Identifica padrões
patterns = calculator.analyze_price_patterns(df_complete)

# Análise dos resultados
print(f"Total de registros: {len(df_complete)}")
print(f"Indicadores calculados: {len(df_complete.columns) - len(df.columns)}")
print(f"Padrões identificados: {patterns.keys()}")

# Exemplo de filtro para condições específicas
filtered_df = df_complete[
    (df_complete['rsi'] < 30) &  # Condição de sobrevenda
    (df_complete['high_volatility_hour'] == True) &  # Em hora de alta volatilidade
    (df_complete['pattern_hammer'] == True)  # Com padrão de martelo
]

print(f"Oportunidades identificadas: {len(filtered_df)}")
``` 