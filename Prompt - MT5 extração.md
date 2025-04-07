Você vai atuar como especialista em desenvolvimento sênior/engenheiro de dados e programação em Python com especialização em Meta Trader 5.

## CONTEXTO 
Desenvolver uma aplicação robusta e automatizada para coleta, processamento e análise de dados em tempo real e históricos do mercado financeiro , com foco em contratos futuros do BMF (Bolsa de Mercadorias e Futuros) negociados na plataforma MetaTrader 5 (MT5). O objetivo é fornecer aos traders, analistas e investidores:

Dados precisos (OHLCV, volume, ticks, indicadores técnicos).
Estatísticas comparativas (variações percentuais, padrões de velas, sentimentos de mercado).
Visualização intuitiva para tomada de decisão ágil.
Armazenamento estruturado para backtesting e análises futuras.
Estratégias Adotadas
1. Arquitetura Técnica
Tecnologias Principais :
Python : Para automação, processamento de dados e interface gráfica.
MetaTrader 5 API : Para integração direta com a plataforma e coleta de dados.
Banco de Dados (SQL/NoSQL) :
TimescaleDB (PostgreSQL) para séries temporais.
SQLite para protótipos rápidos.
Bibliotecas Especializadas :
pandas_ta e TA-Lib para cálculo de indicadores técnicos.
plotly e matplotlib para visualização de dados.
Modularidade :
Separação clara entre módulos de coleta , processamento , armazenamento e interface .
Exemplo de fluxo:
Copiar
1
MT5 → Coleta de Dados → Processamento (Indicadores) → Banco de Dados → Interface do Usuário  
2. Estratégias de Coleta de Dados
Automatização Completa :
Scripts Python agendados via cron (Linux) ou Task Scheduler (Windows) para coleta contínua.
Detecção automática do MT5 no sistema (via caminhos padrão do Windows).
Granularidade Temporal :
Foco em dados minuto a minuto (M1) para alta precisão em análises intraday.
Armazenamento de ticks e profundidade de mercado (DOM) para estratégias de alta frequência.
Validação de Dados :
Verificação de consistência (ex: high >= low, volume > 0).
Tratamento de gaps (ex: interpolação linear para dados faltantes).
3. Processamento de Dados e Indicadores
Indicadores Técnicos :
Cálculo em lote usando pandas_ta para eficiência:
python
Copiar
1
2
df['rsi'] = ta.rsi(df['close'], length=14)  
df.ta.macd(append=True)  
Personalização de parâmetros (ex: ajuste de períodos para médias móveis).
Estatísticas Comparativas :
Variações Percentuais :
Comparação com abertura/fechamento do dia, vencimentos anteriores e intervalos personalizados (5m, 15m, etc.).
Padrões de Velas :
Detecção automática via lógica condicional (ex: Engulfing, Hammer).
4. Armazenamento e Escalabilidade
Modelagem do Banco de Dados :
Tabela por símbolo (ex: winfut_1min_data).
Índices em colunas críticas (time, symbol) para consultas rápidas.
Escalabilidade :
Uso de bancos de dados em nuvem (ex: AWS RDS) para grandes volumes.
Particionamento por tempo (ex: tabelas separadas por mês/ano).
5. Interface do Usuário (GUI)
Funcionalidades Principais :
Seleção de tickers via lista dinâmica (atualizada do MT5).
Visualização de estatísticas em tempo real (ex: volatilidade, padrões detectados).
Exportação de dados para CSV/Excel.
Frameworks :
Tkinter para simplicidade e leveza.
Plotly Dash para dashboards interativos (opcional).
6. Segurança e Conformidade
Criptografia :
Senhas e chaves de API armazenadas com cryptography ou Vault.
Compliance :
Respeito a horários de negociação (ex: desativar coleta quando o mercado está fechado).
Fluxo de Trabalho Completo
Coleta :
Dados brutos (OHLCV, ticks) são extraídos do MT5 via API.
Processamento :
Cálculo de indicadores técnicos e estatísticas.
Armazenamento :
Dados salvos em banco de dados com timestamps em UTC.
Visualização :
Interface exibe dados agregados, gráficos e alertas (ex: "RSI em sobrecompra").
Benefícios Esperados
Redução de Erros : Automação elimina falhas humanas na coleta manual.
Velocidade : Processamento em lote e banco de dados otimizado para consultas rápidas.
Flexibilidade : Adaptação a novos símbolos ou indicadores sem refatoração completa.
Exemplo de Saída do Sistema
WIN$N
2024-05-20 10:15:00
68.2
0.45
+1.2%
Bullish Engulfing

Este projeto combina tecnologia de ponta , análise financeira rigorosa e design centrado no usuário para transformar dados brutos em insights acionáveis, posicionando-se como uma ferramenta indispensável para traders profissionais. 

## ETAPAS DE TRABALHO OBRIGATÓRIAS
1. ANÁLISE: Examine a estrutura existente e identifique onde seu trabalho se encaixa
2. PLANEJAMENTO: Descreva sua abordagem aderindo às convenções do projeto
3. IMPLEMENTAÇÃO: Desenvolva código seguindo as convenções rigorosas
4. VALIDAÇÃO: Verifique conformidade com as diretrizes

## REGRAS INVIOLÁVEIS
. Regras de Integridade dos Dados
Validação Rigorosa :
Regra : Todos os dados coletados do MT5 devem ser validados (ex: verificar se high ≥ low, se volume ≥ 0).
Porquê : Dados corrompidos ou inconsistentes invalidam análises e modelos.
Tratamento de Dados Faltantes :
Regra : Implementar estratégias claras para lidar com gaps (ex: interpolação, exclusão ou marcação de dados faltantes).
Porquê : Dados incompletos geram erros em backtests e estatísticas.
Timestamps em UTC :
Regra : Armazenar todos os horários em UTC para evitar ambiguidades de fuso horário.
Porquê : Fusos locais podem causar confusão em dados históricos.
2. Regras de Qualidade de Código
Padrões de Codificação :
Regra : Seguir PEP8 (Python) ou diretrizes do corretor (MQL5) para formatação consistente.
Porquê : Código legível facilita manutenção e colaboração.
Testes Automatizados :
Regra : Implementar testes unitários para todos os cálculos críticos (ex: RSI, MACD, percentuais de variação).
Porquê : Evita regressões e garante precisão matemática.
Versionamento Rigoroso :
Regra : Usar Git com branches semânticos (ex: main, develop, feature/nome).
Porquê : Permite rastrear mudanças e reverter erros rapidamente.
3. Regras de Performance e Escalabilidade
Otimização de Consultas :
Regra : Indexar colunas críticas no banco de dados (ex: time, symbol).
Porquê : Consultas lentas travam a aplicação em grandes volumes de dados.
Processamento Assíncrono :
Regra : Usar filas de tarefas (ex: Celery, RabbitMQ) para coleta e processamento de dados em segundo plano.
Porquê : Evita bloqueios na interface e melhora a responsividade.
Cache Inteligente :
Regra : Armazenar em cache dados frequentemente acessados (ex: símbolos do MT5, configurações do usuário).
Porquê : Reduz carga no servidor e acelera operações repetitivas.
4. Regras de Segurança
Proteção de Dados Sensíveis :
Regra : Criptografar senhas, chaves de API e dados de conexão com o MT5.
Porquê : Vazamentos comprometem a integridade do sistema e do usuário.
Auditoria de Dependências :
Regra : Verificar vulnerabilidades em bibliotecas externas (ex: MetaTrader5, pandas) com ferramentas como snyk.
Porquê : Bibliotecas desatualizadas são portas de entrada para ataques.
5. Regras de Experiência do Usuário (UX)
Interface Intuitiva :
Regra : Seguir princípios de design minimalista (ex: evitar sobrecarga de botões, usar cores consistentes).
Porquê : Usuários não adotam ferramentas complexas ou confusas.
Feedback em Tempo Real :
Regra : Exibir status de processamento (ex: "Coletando dados do DOL$N... 70% concluído").
Porquê : Mantém o usuário informado e reduz a sensação de "congelamento".
6. Regras de Colaboração e Gestão
Reuniões Diárias (Daily Standups) :
Regra : Reuniões de 15 minutos para atualizações de progresso e bloqueios.
Porquê : Problemas são identificados rapidamente.
Documentação Clara :
Regra : Manter documentação atualizada (ex: arquitetura, fluxo de dados, API).
Porquê : Novos membros da equipe precisam entender o sistema rapidamente.
7. Regras de Conformidade Financeira
Respeito a Horários de Negociação :
Regra : Desativar coleta de dados quando o mercado estiver fechado (ex: verificar trading_hours).
Porquê : Dados fora de horário são irrelevantes ou incorretos.
Padrões Regulatórios :
Regra : Garantir conformidade com diretrizes como GDPR (dados do usuário) e MiFID II (registros de transações).
Porquê : Evita penalidades legais.
8. Regras de Escalabilidade Futura
Modularidade :
Regra : Separar componentes em módulos independentes (ex: data_collector, indicator_calculator, ui).
Porquê : Facilita atualizações e integração de novas funcionalidades.
Suporte a Multiplas Fontes :
Regra : Projetar o sistema para suportar outras plataformas além do MT5 (ex: TradingView, APIs de criptomoedas).
Porquê : Expande o escopo do projeto sem refatoração completa.
9. Regras de Gerenciamento de Riscos
Backup Automático :
Regra : Backup diário do banco de dados e logs críticos.
Porquê : Dados perdidos são irrecuperáveis.
Fail-Safe Mechanisms :
Regra : Implementar fallbacks para falhas no MT5 (ex: reconexão automática).
Porquê : Evita paralisação total do sistema.
10. Regras de Melhoria Contínua
Retrospectivas Pós-Entrega :
Regra : Revisar cada versão para identificar pontos de melhoria.
Porquê : O feedback contínuo evita estagnação.
Monitoramento de Métricas :
Regra : Usar ferramentas como Prometheus ou Grafana para monitorar performance e erros.
Porquê : Problemas são detectados antes de impactar usuários.
Exemplo de Checklist Pré-Lançamento
Dados
✔️ Validação de OHLCV, ✔️ Tratamento de gaps
Código
✔️ Testes unitários, ✔️ Análise estática (ex:
pylint
)
Segurança
✔️ Senhas criptografadas, ✔️ Auditoria de dependências
UX
✔️ Interface testada com usuários reais
- Siga a estrutura de diretórios definida sem exceções
- Documentação Doxygen para todas as funções públicas

## TAREFA ATUAL
Preciso que você aja como um arquiteto de software sênior/engenheiro de diagnóstico experiente deste projeto robusto como o proposto (integração com MetaTrader 5, processamento de dados, cálculos de indicadores técnicos, armazenamento em banco de dados e interface gráfica),. Sua tarefa não é apenas encontrar o primeiro erro, mas realizar uma investigação minuciosa e holística de todo o código-fonte fornecido. O objetivo final é obter um diagnóstico completo e um plano de ação claro para tornar o projeto funcional, robusto e mais fácil de manter.

Instruções Específicas:

Análise de Dependências: Mapeie e analise rigorosamente as dependências entre os diferentes módulos, classes, funções e arquivos do projeto. Identifique explicitamente quaisquer referências circulares e explique o impacto delas.
Verificação de Completude: Procure por chamadas a funções, métodos ou importações que não estejam definidas, implementadas ou acessíveis no escopo correto (funções ausentes/referências quebradas).
Análise Estrutural e de Qualidade: Avalie a arquitetura geral do projeto. Considere aspectos como:
Acoplamento e Coesão: Os módulos estão muito interdependentes (alto acoplamento) ou as responsabilidades estão bem divididas (alta coesão)?
Princípios SOLID (se aplicável à linguagem/paradigma): O código adere a bons princípios de design?
Complexidade Ciclomática: Existem funções ou métodos excessivamente complexos que podem ser fontes de erro e difíceis de testar/manter?
Tratamento de Erros: A gestão de exceções e erros é consistente e robusta?
Potenciais Gargalos de Performance ou Condições de Corrida (se aplicável).
Identificação de Causa Raiz: Para os problemas mais críticos encontrados (especialmente os que impedem o funcionamento), tente identificar a causa raiz, não apenas o sintoma.
Não se limite aos problemas mencionados: Investigue outros possíveis erros lógicos, problemas de configuração, más práticas de codificação ou inconsistências que possam estar contribuindo para o mau funcionamento ou para a dificuldade de manutenção.


faça uma solução completa, é necessário continuar trabalhando no código para resolver os problemas restantes.

1. Analise os arquivos existentes relacionados à tarefa
2. Identifique os componentes que sua implementação precisa interagir
3. Desenvolva uma solução que:
   - Siga todas as convenções de nomenclatura
   - Respeite a separação de responsabilidades
   - Mantenha compatibilidade com a estrutura existente
   - Implemente apenas o necessário para a tarefa atual
4. Documente todas as funções e estruturas seguindo o padrão do projeto
5. Verifique a conformidade com o roadmap em '.cursor/roadmap.md'

## ENTREGÁVEIS

Formato da Resposta Desejada:

Por favor, organize sua resposta da seguinte forma:

Sumário Executivo: Uma visão geral dos problemas mais críticos e da saúde geral do projeto.
Análise Detalhada:
Lista de Referências Circulares identificadas, com os componentes envolvidos e o impacto.
Lista de Funções/Referências Ausentes ou quebradas encontradas.
Discussão sobre a Estrutura e Qualidade do Código, destacando pontos fortes e fracos com exemplos.
Lista de Outros Problemas Significativos identificados (erros lógicos, má práticas, etc.).
Diagnóstico e Causa Raiz: Explicação das causas prováveis para o não funcionamento do projeto, conectando os problemas identificados.
Plano de Ação Recomendado:

Uma sequência de passos priorizada para corrigir os problemas. Comece pelas correções bloqueadoras ou estruturais.
Sugestões de refatoração para resolver as dependências circulares e melhorar a estrutura geral (ex: introduzir interfaces, inversão de dependência, reorganizar módulos).
Recomendações para melhorar a qualidade geral do código e prevenir problemas futuros.
dar diretrizes claras ao Engenheiro de Dados separando atividade em serie e avaliando se é possivel alguma atividade em paralela sem o risco minimo de comprometimento da integridade do projeto
dar diretrizes claras ao Desenvolvedor Frontend/Interface Gráfica separando atividade em serie e avaliando se é possivel alguma atividade em paralela sem o risco minimo de comprometimento da integridade do projeto
dar diretrizes claras ao Engenheiro de Dados separando atividade em serie e avaliando se é possivel alguma atividade em paralela sem o risco minimo de comprometimento da integridade do projeto
dar diretrizes claras ao Desenvolvedor Especialista em Mercados Financeiros separando atividade em serie e avaliando se é possivel alguma atividade em paralela sem o risco minimo de comprometimento da integridade do projeto
dar diretrizes claras ao Cientista de Dados separando atividade em serie e avaliando se é possivel alguma atividade em paralela sem o risco minimo de comprometimento da integridade do projeto

Foco: A profundidade e a abordagem sistêmica são cruciais. Preciso entender todos os fatores que contribuem para o problema e ter um caminho claro para a solução."

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Objetivo:
Desenvolver uma aplicação desktop (Python) que automatize a coleta de dados históricos e em tempo real do MetaTrader 5 (MT5), incluindo todos os campos solicitados , armazenando-os em uma base de dados e gerando estatísticas detalhadas.

Funcionalidades Principais:
Localização Automática do MT5:
Detectar automaticamente o caminho de instalação do MT5 no sistema (Windows).
Se não encontrado, permitir que o usuário selecione manualmente o diretório.
Seleção de Ticker:
Listar todos os símbolos disponíveis no MT5 (ex: Winfut, Dolfut, EURUSD, etc.).
Permitir que o usuário escolha um ou mais tickers para monitoramento.
Coleta de Dados Minuto a Minuto:
Extrair todos os campos abaixo para cada candle de 1 minuto:
Dados Básicos:
time (timestamp do candle).
open, high, low, close, volume (OHLCV).
tick_volume (volume de ticks no intervalo).
spread (diferença entre bid e ask no início do candle).
last (último preço negociado).
trading_hours (status de mercado aberto/fechado).
Indicadores Técnicos:
rsi (14 períodos).
macd_line, macd_signal, macd_histogram (padrão MT5).
ma_20 (Média Móvel Simples de 20 períodos).
bollinger_upper, bollinger_lower (bandas de Bollinger).
atr (Average True Range, 14 períodos).
true_range (diferença entre high e low do candle).
Percentuais de Variação:
%_var_open_5min: Variação vs abertura do último intervalo de 5 minutos.
%_var_close_15min: Variação vs fechamento do último intervalo de 15 minutos.
%_var_high_30min: Variação vs máxima do último intervalo de 30 minutos.
%_var_low_60min: Variação vs mínima do último intervalo de 60 minutos.
%_var_open_daily: Variação vs abertura do dia atual.
%_var_close_prev_day: Variação vs fechamento do dia anterior.
Dados Avançados:
dom_levels: Profundidade de mercado (bid/ask em 5 níveis).
trader_sentiment: % de compradores/vendedores (simulado via volume).
candle_pattern: Padrões detectados (ex: Engulfing, Hammer, Doji).
Armazenamento em Base de Dados:
Usar SQLite ou PostgreSQL para armazenar dados em tabelas estruturadas.
Criar uma tabela por ticker (ex: winfut_1min_data).
Garantir que não haja duplicação de dados (verificar timestamps existentes).
Estatísticas do Período:
Exibir resumo estatístico ao final da coleta:
Tickers salvos e seus intervalos de datas.
Média de volatilidade (ATR).
Máximo e mínimo históricos.
Contagem de padrões de candle (ex: "5 Hammer detectados").
Interface Gráfica (GUI):
Usar Tkinter ou PyQt para criar uma interface amigável.
Botões para:
Selecionar tickers.
Iniciar/Pausar coleta.
Visualizar estatísticas.
Fluxo de Trabalho da Aplicação:
Inicialização:
Detectar MT5 → Conectar via API (MetaTrader5 Python package).
Listar símbolos disponíveis.
Coleta de Dados:
Para cada ticker selecionado:
Usar copy_rates_from() para OHLCV.
Calcular indicadores técnicos (RSI, MACD, etc.) com pandas_ta ou TA-Lib.
Capturar dados de profundidade de mercado (MarketBookGet()).
Detectar padrões de candle com lógica personalizada.
Armazenamento:
Salvar dados em lote (batch) para otimizar performance.
Criar índices na base de dados para consultas rápidas.
Estatísticas:
Calcular métricas resumo (ex: df.describe()).
Gerar gráficos simples (ex: volatilidade diária).
Exemplo de Código Base (Python):
python
Copiar
1
2
3
4
5
6
7
8
9
10
11
12
13
14
15
16
17
18
19
20
21
22
23
24
25
26
27
28
29
30
31
32
33
34
35
36
37
38
39
40
41
42
43
44
45
46
47
48
49
50
51
52
⌄
⌄
⌄
⌄
⌄
⌄
⌄
⌄
import MetaTrader5 as mt5
import pandas as pd
import pandas_ta as ta
from tkinter import *
from sqlalchemy import create_engine

# Inicializar MT5
def initialize_mt5():
    if not mt5.initialize():
        print("Erro ao inicializar MT5")
        return False
    return True

# Listar símbolos disponíveis
def get_symbols():
    symbols = mt5.symbols_get()
    return [s.name for s in symbols]

# Coletar dados minuto a minuto
def fetch_data(symbol, start_time, end_time):
    rates = mt5.copy_rates_range(symbol, mt5.TIMEFRAME_M1, start_time, end_time)
    df = pd.DataFrame(rates)
    df['time'] = pd.to_datetime(df['time'], unit='s')
    
    # Calcular indicadores
    df['rsi'] = ta.rsi(df['close'], length=14)
    df.ta.macd(append=True)
    df.ta.bbands(append=True)
    df['atr'] = ta.atr(df['high'], df['low'], df['close'], length=14)
    
    # Percentuais de variação
    df['%_var_open_daily'] = (df['close'] - df['open']) / df['open'] * 100
    
    return df

# Salvar no banco
def save_to_db(df, symbol):
    engine = create_engine('sqlite:///mt5_data.db')
    df.to_sql(f"{symbol}_1min", engine, if_exists='append', index=False)

# Interface Gráfica
class App:
    def __init__(self, root):
        self.root = root
        self.symbols = get_symbols()
        # ... (adicionar widgets para seleção de ticker e botões)

if __name__ == "__main__":
    initialize_mt5()
    root = Tk()
    app = App(root)
    root.mainloop()
Requisitos Técnicos:
Bibliotecas Python:
MetaTrader5, pandas, pandas_ta, sqlalchemy, tkinter.
Tratamento de Erros:
Verificar conexão com MT5.
Lidar com falhas na coleta de dados (ex: mercado fechado).
Documentação:
Incluir um README.md explicando a instalação e uso.
Próximos Passos:

Implementar detecção automática do MT5.
Desenvolver a lógica de cálculo dos percentuais de variação.
Integrar a detecção de padrões de candle.
Criar a interface gráfica.

## ----------------------------------------------------------------------------------