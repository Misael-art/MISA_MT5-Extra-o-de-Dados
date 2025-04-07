# Plano Detalhado - Fase 2 (Revisado): Implementação de Fallback e Chunking Dinâmico para Extração M1

**Objetivo Principal:** Modificar o sistema para permitir o uso de uma fonte de dados externa como fallback automático para a extração M1, caso MT5 falhe, e otimizar a extração ajustando o tamanho dos blocos (chunking) dinamicamente com base no timeframe.

**Princípios:**

*   **Modularidade:** Isolar a lógica de acesso a fontes externas para facilitar futuras implementações.
*   **Configurabilidade:** Permitir que o usuário habilite/desabilite e configure facilmente qual fonte externa usar e os parâmetros de chunking.
*   **Fallback:** A fonte externa só será acionada para blocos M1 se a extração via MT5 falhar repetidamente.
*   **Investigação Contínua:** O plano inclui investigar as limitações do MT5 como primeiro passo prático.

**Etapas do Plano (Revisadas):**

1.  **Investigação das Limitações Reais da Extração M1 via MT5:**
    *   **Por quê:** Antes de construir a complexidade do fallback, é crucial entender *se* e *quando* ele é realmente necessário.
    *   **Ação:** Desenvolver ou adaptar scripts de teste para extrair grandes volumes de dados M1 (ex: 1, 2, 5 anos para WIN, WDO, ações comuns) usando o `HistoricalExtractor` atual.
    *   **Coletar:** Tempo de execução, taxas de sucesso/falha por bloco, número de barras retornadas, ocorrência de lacunas nos dados.
    *   **Entregável:** Um relatório (ex: `docs/investigacao_mt5_m1.md`) resumindo os resultados, performance e limitações encontradas. Isso guiará a decisão sobre a urgência e a necessidade de implementar uma fonte externa real.

2.  **Definição de uma Interface para Fontes de Dados Externas:**
    *   **Por quê:** Criar um contrato claro para qualquer futura fonte de dados externa, desacoplando o `HistoricalExtractor` dos detalhes de implementação específicos.
    *   **Ação:** Definir uma classe abstrata ou protocolo em Python (ex: `ExternalDataSource`) com métodos essenciais:
        *   `get_historical_m1_data(symbol, start_dt, end_dt)`: Busca dados M1. Retorna um DataFrame Pandas ou `None` em caso de falha.
        *   `is_configured()`: Verifica se a fonte está pronta para uso (ex: API key configurada).
    *   **Localização:** Poderia ser um novo arquivo, como `mt5_extracao/external_data_source.py`.

3.  **Adaptação do `HistoricalExtractor` para Suportar Fallback e Chunking Dinâmico:**
    *   **Por quê:** Integrar a lógica de fallback e otimizar a busca por blocos.
    *   **Ação:**
        *   **Chunking Dinâmico:**
            *   Modificar `_process_symbol` para definir `block_delta` com base no `timeframe_val` e valores lidos da configuração (`chunk_days_m1`, `chunk_days_m5_m15`, `chunk_days_default`).
            *   Passar os valores de configuração para o `HistoricalExtractor` via `__init__` ou ler diretamente do objeto de configuração global.
        *   **Fallback:**
            *   Modificar `HistoricalExtractor.__init__` para aceitar `Optional[ExternalDataSource]`.
            *   Dentro de `_process_symbol`, após a falha em obter bloco M1 via MT5 (após retries):
                *   Verificar `self.external_source` e `is_configured()`.
                *   Chamar `get_historical_m1_data(...)` da fonte externa.
                *   Usar dados externos se retornados e válidos.
                *   Marcar falha definitiva somente se MT5 e fallback falharem.
    *   **Impacto:** Otimização da busca MT5 e adição da capacidade de fallback M1.

4.  **Implementação de um Provedor "Dummy" (Exemplo):**
    *   **Por quê:** Permitir testar a lógica de fallback no `HistoricalExtractor` mesmo sem uma fonte externa real implementada.
    *   **Ação:** Criar uma classe `DummyExternalSource` que implementa a interface `ExternalDataSource`. Seus métodos podem apenas logar mensagens indicando que foram chamados e retornar `None` ou um DataFrame vazio.
    *   **Benefício:** Essencial para testes unitários e de integração da mecânica de fallback.

5.  **Sistema de Configuração:** (Expandido)
    *   **Por quê:** Permitir controle do fallback e dos tamanhos dos blocos.
    *   **Ação:**
        *   Adicionar seção `[FALLBACK]` ao `config.ini` com:
            *   `external_source_m1_fallback_enabled=False`
            *   `external_source_m1_type=None` (ou 'Dummy')
            *   (Opcional) Chaves para fontes específicas (API keys, etc.).
        *   Adicionar seção `[EXTRACTION]` ao `config.ini` com:
            *   `chunk_days_m1=30`
            *   `chunk_days_m5_m15=90`
            *   `chunk_days_default=365`
        *   Modificar `app.py` para ler a configuração de fallback e instanciar a fonte externa.
        *   Modificar `app.py` (ou onde `HistoricalExtractor` é criado) para ler as configurações de chunking e passá-las para o `HistoricalExtractor`.

6.  **Documentação:**
    *   **Por quê:** Garantir que a nova funcionalidade e como usá-la sejam claras.
    *   **Ação:** Atualizar a documentação existente (`README.md`, `docs/arquitetura.md`):
        *   Descrever a funcionalidade de fallback M1.
        *   Explicar como habilitar e configurar (inicialmente com o Dummy).
        *   Documentar a interface `ExternalDataSource` para desenvolvedores que queiram adicionar novas fontes.
        *   Explicar a configuração do chunking dinâmico.
        *   Incluir o link para o relatório da investigação MT5 M1 (Etapa 1).

**Diagrama de Fluxo (Foco na Lógica de Fallback M1 - Mantido):**

```mermaid
graph TD
    A[Iniciar processamento do bloco M1] --> B{Tentar buscar bloco via MT5 (com retries)};
    B -- Sucesso --> G[Usar dados MT5];
    B -- Falha (após retries) --> C{Fallback Externo Habilitado E Configurado?};
    C -- Sim --> D{Tentar buscar bloco via Fonte Externa};
    C -- Não --> F[Erro: Falha ao obter bloco M1];
    D -- Sucesso --> E[Usar dados da Fonte Externa];
    D -- Falha --> F;
    E --> H[Continuar processamento do símbolo];
    G --> H;
    F --> I[Interromper extração para este símbolo];

    style F fill:#f9f,stroke:#333,stroke-width:2px;
    style I fill:#f9f,stroke:#333,stroke-width:2px;