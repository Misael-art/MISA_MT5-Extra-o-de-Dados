# Plano de Ação: Aprimoramento da Extração Histórica

Este plano detalha as etapas para aprimorar a funcionalidade de extração de dados históricos do projeto, focando em garantir a completude dos dados, adicionar a opção de sobrescrever e otimizar o processo com paralelização.

## Etapas do Plano

1.  **Refatoração e Criação da Classe `HistoricalExtractor`:**
    *   **Objetivo:** Melhorar a organização do código e a separação de responsabilidades.
    *   **Ação:** Criar um novo arquivo (`mt5_extracao/historical_extractor.py`) e mover a lógica de orquestração da extração histórica (atualmente na função `start_historical_extraction_logic` dentro de `app.py`) para uma nova classe `HistoricalExtractor`.
    *   **Benefícios:** Código mais limpo, coeso e fácil de manter. `app.py` ficará mais enxuto.

2.  **Implementação da Extração em Blocos (Chunking) na `HistoricalExtractor`:**
    *   **Objetivo:** Garantir a busca completa dos dados, respeitando os limites da API MT5.
    *   **Ação:** Modificar a lógica dentro da `HistoricalExtractor` para dividir o intervalo de tempo total em blocos menores (ex: anual) e buscar dados bloco a bloco usando `mt5_connector.get_historical_data`.
    *   **Benefícios:** Evita falhas e dados incompletos causados por buscar volumes excessivos de uma só vez.

3.  **Implementação da Funcionalidade `overwrite` no `DatabaseManager`:**
    *   **Objetivo:** Permitir que o usuário escolha sobrescrever dados existentes.
    *   **Ação:** Adicionar um método ao `DatabaseManager` (ex: `delete_data_periodo`) para remover registros existentes por período. Chamar este método na `HistoricalExtractor` antes de salvar, se `overwrite` for `True`.
    *   **Benefícios:** Atende à necessidade de poder atualizar ou corrigir dados históricos.

4.  **Melhoria na Robustez (Retries com Backoff):**
    *   **Objetivo:** Aumentar a chance de sucesso na busca de cada bloco.
    *   **Ação:** Dentro do loop de busca por bloco na `HistoricalExtractor`, implementar retentativas com espera exponencial (*exponential backoff*) em caso de falha na chamada a `mt5_connector.get_historical_data`.
    *   **Benefícios:** Torna a extração mais tolerante a falhas temporárias.

5.  **Implementação da Paralelização por Símbolo:**
    *   **Objetivo:** Acelerar a extração processando múltiplos símbolos simultaneamente.
    *   **Ação:** Utilizar `concurrent.futures.ThreadPoolExecutor` na `HistoricalExtractor` para executar a lógica de extração (busca por blocos, salvamento) para vários símbolos em paralelo, com um número limitado de workers. Garantir thread-safety.
    *   **Benefícios:** Redução do tempo total de extração.

## Diagrama de Fluxo Proposto (com Paralelização)

```mermaid
sequenceDiagram
    participant UI (UIManager)
    participant App (MT5Extracao)
    participant Extractor (HistoricalExtractor)
    participant ThreadPool
    participant Worker (Thread)
    participant Connector (MT5Connector)
    participant DB (DatabaseManager)

    UI->>App: Iniciar Extração Histórica (símbolos, período, overwrite=True)
    App->>Extractor: iniciar_extracao(símbolos, período, overwrite=True, callbacks)
    Extractor->>ThreadPool: Criar Pool de Workers (limitado, ex: 4)
    Extractor->>Extractor: Dividir período total em Blocos (ex: Anual)

    loop Para cada Símbolo na lista
        Extractor->>ThreadPool: Submeter tarefa(Símbolo, Blocos, overwrite) para um Worker
    end

    par Worker 1 executa para Símbolo A
        Worker->>Worker: Processar Blocos para Símbolo A
        loop Para cada Bloco de Tempo
            opt Sobrescrever habilitado
                Worker->>DB: delete_data_periodo(Símbolo A, timeframe, bloco_tempo) # Atenção: Thread-safety DB
            end
            loop Tentativas com Backoff
                Worker->>Connector: get_historical_data(Símbolo A, bloco_tempo) # Atenção: Thread-safety Connector/API
                alt Sucesso
                    break
                else Falha
                    Worker->>Worker: Aguardar (backoff)
                end
            end
            alt Dados Obtidos
                 Worker->>DB: save_ohlcv_data(Símbolo A, timeframe, df_bloco) # Atenção: Thread-safety DB
            else Falha Final
                 Worker->>Extractor: Registrar falha (Símbolo A, bloco)
            end
        end
        Worker->>Extractor: Atualizar Progresso Geral (Símbolo A concluído)
    and Worker 2 executa para Símbolo B
        Worker->>Worker: Processar Blocos para Símbolo B
        loop Para cada Bloco de Tempo
            opt Sobrescrever habilitado
                Worker->>DB: delete_data_periodo(Símbolo B, timeframe, bloco_tempo)
            end
            loop Tentativas com Backoff
                Worker->>Connector: get_historical_data(Símbolo B, bloco_tempo)
                 alt Sucesso
                    break
                else Falha
                    Worker->>Worker: Aguardar (backoff)
                end
            end
             alt Dados Obtidos
                 Worker->>DB: save_ohlcv_data(Símbolo B, timeframe, df_bloco)
            else Falha Final
                 Worker->>Extractor: Registrar falha (Símbolo B, bloco)
            end
        end
         Worker->>Extractor: Atualizar Progresso Geral (Símbolo B concluído)
    and ... (outros workers)
    end

    Extractor->>Extractor: Aguardar conclusão de todas as tarefas
    Extractor->>UI: Finalizar Extração (via callback - resumo sucesso/falha)