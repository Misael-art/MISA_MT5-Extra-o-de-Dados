# Épico J — IA e agentes de desenvolvimento

Repositórios: **mt5x** e **HADOUKEN**. Índice, convenções e regras comuns: [README.md](README.md).

Regra do épico: **a IA não contorna os mecanismos de segurança e não envia ordens pelo mt5x.** Toda ação sugerida
passa por intenção, confirmação humana, guarda global, `HDK_RiskGuard` e `HDK_SafeExecutor`. O primeiro modo de
qualquer agente é somente leitura.

### J-01 — Agente de análise do projeto
- **O quê e por quê:** agente que mapeia o código antes de propor mudança e mostra a diferença entre o atual e o proposto.
- **Repositório:** mt5x e HADOUKEN · **Prioridade:** P3 · **Complexidade:** M · **Status:** PROPOSTO
- **Módulos existentes:** `mt5x commands`, `mt5x status`, `mt5x --json` (T9.1), `AGENTS.md`; `Tools/MCP_*.py` do HADOUKEN (antigos, não auditados) · **Depende de:** J-09 · **Pré-requisitos:** auditar ou aposentar os scripts de `Tools/`
- **Riscos:** agente que assume que o plano é o código; scripts antigos com acesso não revisto.
- **Aceite:** toda afirmação sobre o código cita o arquivo; nada é dado como existente só por estar planejado.
- **Testes:** perguntas com resposta conhecida sobre o repositório.

### J-02 — Agente de geração de estratégias
- **O quê e por quê:** gerar estratégias no formato declarado, que já tem geração de MQL5 e paridade.
- **Repositório:** mt5x · **Prioridade:** P3 · **Complexidade:** G · **Status:** PROPOSTO
- **Módulos existentes:** `strategies/declared.py`, `robots/generate.py` (T10.10), `mt5x robots new` · **Depende de:** B-09, I-09 · **Pré-requisitos:** registro de quantas estratégias foram geradas e testadas
- **Riscos:** fábrica de sobreajuste; estratégia gerada fora do conjunto suportado.
- **Aceite:** só gera o que a declaração suporta; cada tentativa entra no catálogo de testes.
- **Testes:** estratégia gerada passa pelo ciclo `robots check` sem atalho.

### J-03 — Agente de testes e validação
- **O quê e por quê:** rodar compilação, Tester, bateria e paridade e relatar o resultado como ele é.
- **Repositório:** mt5x · **Prioridade:** P2 · **Complexidade:** M · **Status:** PROPOSTO
- **Módulos existentes:** `robots/pipeline.py` (`robots check`), `mt5x audit` · **Depende de:** J-09 · **Pré-requisitos:** —
- **Riscos:** agente que desativa teste para passar; relatório que omite o que não rodou.
- **Aceite:** o relatório separa o que passou, o que falhou e o que não foi rodado.
- **Testes:** robô que falha em cada etapa do ciclo.

### J-04 — Agente de documentação
- **O quê e por quê:** manter plano, andamento e checklist do Windows coerentes com o código.
- **Repositório:** mt5x e HADOUKEN · **Prioridade:** P3 · **Complexidade:** P · **Status:** PROPOSTO
- **Módulos existentes:** `tests/test_sdk_docs.py` (confere a documentação do SDK) · **Depende de:** — · **Pré-requisitos:** —
- **Riscos:** documentação que afirma o que não foi conferido.
- **Aceite:** status só muda com evidência citada; "não conferido" continua escrito.
- **Testes:** verificação de links e de identificadores citados.

### J-05 — Assistente de diagnóstico das operações
- **O quê e por quê:** explicar por que um robô entrou, não entrou ou foi bloqueado, a partir dos registros.
- **Repositório:** mt5x (dados: HADOUKEN) · **Prioridade:** P2 · **Complexidade:** M · **Status:** PLANEJADO (parte da T12.7)
- **Módulos existentes:** `robots/live.monitor`, log e auditoria do HADOUKEN (`HDK_Audit`), `mt5x portfolio` · **Depende de:** H-01, F-09 · **Pré-requisitos:** registros estruturados das decisões (B-06, B-07)
- **Riscos:** explicação inventada quando falta registro.
- **Aceite:** cada explicação aponta a linha do registro; sem registro, diz que não sabe.
- **Testes:** casos gravados com causa conhecida.

### J-06 — Análise inteligente de comportamento dos ativos
- **O quê e por quê:** resumir em texto o que as características e hipóteses dizem de um ativo.
- **Repositório:** mt5x · **Prioridade:** P3 · **Complexidade:** M · **Status:** PROPOSTO
- **Módulos existentes:** `mt5x analyze`, `study.py` · **Depende de:** A-06, A-10 · **Pré-requisitos:** as características existirem
- **Riscos:** texto fluente sobre amostra pequena.
- **Aceite:** toda frase com número traz amostra e validade.
- **Testes:** ativo sem dados suficientes produz "dados insuficientes".

### J-07 — Recomendações de configuração baseadas em evidências
- **O quê e por quê:** sugerir preset, timeframe ou estratégia para um ativo, com a evidência que sustenta.
- **Repositório:** mt5x · **Prioridade:** P3 · **Complexidade:** M · **Status:** PROPOSTO
- **Módulos existentes:** `asset_screener.suggest_strategy`, `robots/matrix.positives` · **Depende de:** I-10, J-06 · **Pré-requisitos:** —
- **Riscos:** recomendação lida como promessa de resultado.
- **Aceite:** recomendação nunca altera configuração sozinha; "aprovado" não é "recomendado operar".
- **Testes:** sem evidência suficiente, não recomenda.

### J-08 — Explicabilidade das decisões
- **O quê e por quê:** cada decisão de entrar, bloquear ou reduzir risco carrega o motivo, para pessoa e agente lerem.
- **Repositório:** HADOUKEN e mt5x · **Prioridade:** P2 · **Complexidade:** M · **Status:** PROPOSTO
- **Módulos existentes:** motivos de bloqueio do `HDK_SafeExecutor` e do `HDK_RiskGuard`; `HDK_Logger` · **Depende de:** B-06, B-07 · **Pré-requisitos:** —
- **Riscos:** motivo genérico; registro que atrasa o `OnTick`.
- **Aceite:** toda `RiskDecision` negativa tem motivo específico e a regra que o gerou.
- **Testes:** cada bloqueio conhecido produz o seu motivo.

### J-09 — Controle de permissões para ações automatizadas
- **O quê e por quê:** dizer o que um agente pode ler, propor e executar, e registrar o que fez.
- **Repositório:** mt5x e HADOUKEN · **Prioridade:** P0 dentro do épico · **Complexidade:** M · **Status:** PROPOSTO
- **Módulos existentes:** ponte restrita a leitura (T9.3), `ENUM_HDK_PERMISSION`, auditoria do executor, `mt5x audit` · **Depende de:** decisão 7 do índice · **Pré-requisitos:** —
- **Riscos:** agente com o mesmo poder do dono do projeto; permissão concedida uma vez valendo para sempre.
- **Aceite:** nenhuma ação que mexa em conta, terminal ou trava roda sem confirmação humana registrada; a ponte continua sem função de envio de ordem (o teste `test_no_order_send.py` segue valendo).
- **Testes:** tentativa de ação fora da permissão é recusada e registrada.
