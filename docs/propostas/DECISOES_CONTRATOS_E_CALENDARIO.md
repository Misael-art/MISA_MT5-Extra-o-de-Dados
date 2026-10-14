# Proposta: onde moram os contratos do framework e de onde vem o calendário de sessões

> A decisão de calendário da T13.1 foi adotada em 2026-10-14 e está registrada abaixo. A decisão sobre contratos
> (seção 2) continua como proposta. O documento foi pedido pelo dono do projeto em 2026-10-13 ("faça uma busca das
> fontes e proponha como viabilizar"). São as decisões 3 e 4 do [índice do roadmap](README.md). Cada afirmação diz
> se foi conferida no código, lida em uma fonte externa ou é recomendação.

## 1. Calendário de sessões (decisão 4)

### O que as fontes dizem

| Fonte | O que oferece | O que não oferece | Como foi conferido |
|---|---|---|---|
| Terminal MT5, pelo MQL5 (`SymbolInfoSessionTrade` e `SymbolInfoSessionQuote`) | Sessões de negociação e de cotação **por símbolo e por dia da semana**, como a corretora configurou. Início e fim em segundos desde 00:00, no horário do servidor | Feriados e dias de horário especial: é uma grade semanal, sem datas. Se a corretora não informar, a função devolve falso | Documentação da MetaQuotes. O HADOUKEN **já usa** `SymbolInfoSessionTrade` em `HDK_MarketFilter.mqh` e em `HDK_StrategyRobot.mqh` |
| Pacote `MetaTrader5` do Python (a ponte) | Barras, ticks, book, conta, especificação do símbolo | **Nenhuma função de sessão.** A lista oficial tem 32 funções e nenhuma devolve horário de sessão | Página oficial do pacote; a lista de funções aceitas pela ponte (`scripts/mt5_bridge_server.py`) confirma |
| Barras M1 já gravadas na base | O que **de fato** aconteceu: em que minutos houve barra, em cada dia. Dia sem barra é dia sem pregão; dia curto é sessão reduzida | O futuro. E não distingue "mercado fechado" de "falha na extração" sem o registro de blocos | Medido na base do host: AAAU e ACH abrem às 16:30 e fecham às 22:59 (horário do servidor) nos cinco dias da semana, 21 dias cada |
| Biblioteca `exchange_calendars` (Python) | Calendário de mais de 50 bolsas, inclusive "BMF Bovespa" (código `BVMF`), com feriados e sessões consultáveis | Trata como fechado tudo o que não é pregão regular (leilões, pré e pós). É **por bolsa, não por instrumento**: o horário do mini-índice não é o das ações. Mantida por contribuições, sem garantia de exatidão | README do projeto. **Não instalada nem testada aqui** |
| B3, calendário oficial anual (ofício circular) | Feriados sem pregão e dias de horário especial (em 2026: Carnaval em 16 e 17/02, Quarta-feira de Cinzas com negociação a partir das 13h, sem sessão em 24 e 31/12, pregão normal em 9 de julho) | Não é um arquivo para programa ler; a B3 avisa que pode alterar o calendário | Notícia da B3 de 09/01/2026 |

### Conclusão das fontes

Nenhuma fonte sozinha resolve. A grade semanal está no terminal e só o MQL5 a lê; os feriados não estão no
terminal; a base diz a verdade sobre o passado e nada sobre o futuro.

### Decisão adotada para T13.1 (2026-10-14)

- O calendário histórico parte das barras M1. O comando `mt5x resample --session-aware` também registra os campos
  estáveis de `symbol_info` do MT5 como metadados do símbolo.
- O dia de sessão usa por padrão `00:00` no horário da corretora, configurável com `--session-anchor HH:MM` para
  representar sessões noturnas. Um gap M1 maior que `--session-gap-minutes` (padrão 60) abre outra sessão observada.
- `symbol_info` do Python não contém a grade semanal de sessões. Portanto a saída é uma inferência histórica, não um
  calendário oficial nem uma previsão de feriados.
- A estrutura guarda fonte e metadados genéricos para permitir acréscimo futuro de outras fontes. **Nenhuma fonte
  externa ou calendário de feriados foi consultado ou incorporado na T13.1.**
- O offset UTC usa `[APP] broker_utc_offset`, fixo no histórico; mudanças de horário de verão não são reconstruídas.

Regras completas e evidências da implementação: [T13.1_CALENDARIO_OBSERVADO.md](../analytics/T13.1_CALENDARIO_OBSERVADO.md).

### Proposta: três camadas, cada uma com o seu papel

1. **Passado: deduzido das barras M1 da base.** Para montar barras por sessão e estudar o histórico (A-01), a sessão
   de um dia é o intervalo em que houve barras naquele dia, conferido contra o registro de extração
   (`_extraction_log`) para não chamar de feriado um bloco que falhou. É Python puro, sem dependência nova, e pode
   começar já. Feriado e fechamento antecipado saem de graça: são o que os dados mostram.
2. **Grade semanal do presente: exportada pelo terminal.** Um exportador MQL5 pequeno grava
   `Common\Files\HDK_sessions_<servidor>.json` com as sessões de negociação e de cotação de cada símbolo e a
   diferença entre o horário do servidor e o UTC naquele instante. O mt5x lê o arquivo, do mesmo jeito que já lê
   `HDK_portfolio_<conta>.json`. O candidato natural é o `HDK_AccountGuardian`, que já é o sentinela residente.
3. **Exceções do futuro: arquivo mantido, com semente automática.** `config/calendario_excecoes.csv` (data,
   mercado ou símbolo, fechado ou horário especial). A semente vem de `exchange_calendars` como dependência
   **opcional**, e a pessoa corrige com o ofício anual da B3. Toda linha diz de onde veio.

A camada 1 também serve de **conferência** das outras duas: se o terminal diz que há sessão e a base não tem barras
(ou o contrário) em vários dias, o programa avisa em vez de escolher um lado.

### Como viabilizar

| Passo | O quê | Onde | Depende de |
|---|---|---|---|
| 1 | Dedução das sessões a partir do M1, com a tabela `_market_calendar` e o estado de cada dia (normal, curto, sem pregão, sem dados) | mt5x, T13.1 | nada: pode ser o primeiro passo da implementação |
| 2 | Guardar a diferença servidor × UTC por data, deduzida das barras quando a base é `broker` | mt5x, T13.1 | passo 1 |
| 3 | Exportador das sessões no sentinela, e leitor no mt5x | HADOUKEN e mt5x | passo 1 (a tabela existir) |
| 4 | Arquivo de exceções, com a semente opcional | mt5x | passo 1 |
| 5 | Aviso de divergência entre as camadas | mt5x | passos 1 e 3 |

### O que não foi verificado

- Se a corretora do host preenche as sessões por símbolo (a função pode devolver falso). Só se sabe que o código do
  HADOUKEN a chama.
- `exchange_calendars`: não instalada; a exatidão do calendário `BVMF` para derivativos não foi conferida.
- **Nenhum ativo da B3 está na base do host.** A dedução foi medida em dois ativos americanos com 21 dias cada;
  sessão que atravessa a meia-noite, pausa no meio do pregão e horário de verão não foram exercitados.
- O horário de negociação do mini-índice não foi levantado: a busca não trouxe a página oficial da B3.

## 2. Onde moram os contratos do framework (decisão 3)

Os contratos são `MarketContext`, `SignalIntent`, `ExecutionPlan`, `PositionPolicy`, `RiskDecision` e
`ExecutionResult` (épico B, no repositório do HADOUKEN).

### O que o código diz hoje (conferido em 2026-10-13)

- Os robôs do HADOUKEN incluem a camada por `<Framework/HADOUKEN/Safety/...>`; os robôs feitos com o SDK, por
  `<MT5X/Safety/...>`, que o mt5x instala da `mt5x-safety` na versão que o `robot.json` declara.
- `Include/MT5X/Safety` da `mt5x-safety` é **idêntica, arquivo a arquivo**, à `Safety/` do HADOUKEN (comparação
  feita nesta data).
- `safety.json` já tem um campo `"contract": 1`, e o contrato do robô no mt5x tem versão (`docs/sdk/contrato.md`).
- O mt5x já lê arquivos `.mqh` e `.mq5` para extrair parâmetros (`robots/mql.py`).
- Convenção do HADOUKEN: struct copiada por atribuição não pode ter `string` nem array.

### Opções

| Opção | A favor | Contra |
|---|---|---|
| A. Tudo no HADOUKEN, em uma pasta nova `Contracts/` | Um lugar só; fica perto do motor de estratégias | Robô do SDK não recebe os contratos: o mt5x só instala a `mt5x-safety` |
| B. Tudo dentro de `Safety/` | Já é distribuído e versionado | Engorda a camada que precisa ser pequena para ser auditada; `SignalIntent` não é assunto de segurança |
| C. Biblioteca nova, em repositório próprio | Separação limpa | Um terceiro repositório para manter em sincronia, com um mantenedor só; hoje já são dois que precisam bater |
| **D. Duas pastas, uma biblioteca, uma versão** | Mantém a `Safety/` pequena e entrega os contratos a todo robô | Muda o que a `mt5x-safety` distribui (uma pasta a mais) |

### Recomendação: opção D

1. **A origem continua sendo o HADOUKEN.** Pasta nova `Include/Framework/HADOUKEN/Contracts/`, só com tipos
   (structs e enums), sem lógica e sem nenhuma chamada de negociação.
2. **`RiskDecision` e `ExecutionResult` ficam ao lado da `Safety/`**, porque quem os produz é a camada de risco e o
   executor; os outros quatro ficam em `Contracts/`. Nenhum deles muda as assinaturas atuais: entram como
   chamadas novas.
3. **A `mt5x-safety` passa a espelhar as duas pastas** (`MT5X/Safety` e `MT5X/Contracts`), com **uma versão só**.
   `safety.json` ganha um campo `"contracts": 1` ao lado do `"contract": 1` que já existe. Uma mudança incompatível
   em um contrato sobe a versão maior da biblioteca.
4. **O mt5x tem o espelho em Python** (`mt5_extracao/contracts.py`, com *dataclasses*) e um teste que lê os campos
   dos `.mqh` e falha se os dois lados divergirem. É o que garante a paridade dos contratos, e reaproveita o leitor
   de MQL5 que já existe.
5. **O contrato 1 do robô continua válido.** Robô que não usa os contratos novos não muda em nada.

### Como viabilizar

| Passo | O quê | Onde | Tarefa do backlog |
|---|---|---|---|
| 1 | Escrever a política de versionamento (o item 3 acima) | HADOUKEN e mt5x | B-11 |
| 2 | Mapa do que cada camada faz hoje, a partir do código | HADOUKEN | B-01 |
| 3 | Os seis tipos, vazios de lógica, compilando sozinhos | HADOUKEN | B-02 a B-07 |
| 4 | Espelho em Python e teste de igualdade dos campos | mt5x | B-10 (antecipada nesta parte) |
| 5 | Versão nova da `mt5x-safety` com a segunda pasta; o instalador do mt5x copia as duas | mt5x-safety e mt5x | B-06, B-07 |
| 6 | Adaptador dos seis robôs, com a paridade como régua | HADOUKEN | B-08 |

### O que não foi verificado

- Se o instalador da biblioteca no mt5x (`robots/safety_lib.py`) aceita mais de uma pasta de include sem mudança
  de formato do `safety.json`: hoje ele lê um `include_dir` só.
- Se os tipos cabem na restrição de struct sem `string` (o motivo de uma decisão de risco é texto: provavelmente
  vai como código de motivo, com o texto à parte).

## Fontes

- MetaQuotes, `SymbolInfoSessionTrade`: https://www.mql5.com/en/docs/marketinformation/symbolinfosessiontrade
- MetaQuotes, integração com Python (lista de funções): https://www.mql5.com/en/docs/python_metatrader5
- Fórum MQL5, sessão de cotação e sessão de negociação: https://www.mql5.com/en/forum/23529/852751
- `exchange_calendars`: https://github.com/gerrymanoim/exchange_calendars
- B3, calendário de negociação de 2026: https://www.b3.com.br/pt_br/noticias/calendario-de-negociacao-da-b3-confira-o-funcionamento-da-bolsa-em-2026.htm
