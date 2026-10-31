# Dashboard Contínuo do mt5x — Inteligência de Ativos e Robôs

**Registro:** 30/10/2026. **Estado:** plano de implantação; Dashboard ainda não implementado por este registro.
**Vínculo:** Fase 13 / T13.6, com consumo dos serviços atuais de Dados, Laboratório, Análise e Robôs.
**Identificador desta frente:** `DASH`. As etapas `DASH-00` a `DASH-14` abaixo detalham a entrega aditiva; não substituem as tarefas dos produtores.

## 1. Objetivo e mandato de produto

Construir um dashboard rico, moderno e flexível que conte, em uma única página de rolagem contínua, a história relevante de **um ativo ou de um robô escolhido**. O usuário deve reconhecer o que está acontecendo, o que mudou, qual evidência sustenta a leitura, o que já foi validado e o que perdeu validade, sem montar mentalmente números dispersos entre abas.

Todos os cálculos analíticos continuam nas fontes oficiais. O Dashboard recebe resultados já produzidos, pondera sua relevância para apresentação e os organiza em uma narrativa visual coesa. Seleção e hierarquia visual pertencem ao Dashboard; fórmulas financeiras, critérios de aprovação e regras operacionais pertencem aos serviços existentes.

O produto deve responder visualmente a estas perguntas:

1. Qual é a maior surpresa deste ativo ou robô neste recorte?
2. Em que regime e contexto de volume isso acontece?
3. Quais hipóteses ou resultados sustentam essa leitura, com qual amostra e incerteza?
4. O que sobreviveu à validação temporal e fora da amostra?
5. O que mudou, venceu ou foi invalidado desde a revisão comparável anterior?
6. O que foi omitido e por quê?

**Princípio de composição:** inventariar toda capacidade disponível da ferramenta; apresentar primeiro as evidências relevantes e elegíveis, mantendo os detalhes acessíveis na própria página. Não transformar esse inventário em uma parede de cartões, uma reprodução das abas ou uma pontuação universal de qualidade do ativo/robô.

### 1.1 Origem deste documento

Este arquivo registra o prompt do proprietário e o desdobra em contrato, integração, sequência de implantação e aceite. O texto recebido tinha uma interrupção entre o problema inicial e os itens finais da jornada, além de numeração repetida. As seções seguintes consolidam o conteúdo fornecido; a jornada foi explicitada a partir do objetivo informado, sem alegar recuperação literal dos trechos ausentes.

O pedido adicional do proprietário amplia expressamente a seleção inicial de ativo+TF para **ativo ou robô**. Ambas as entradas são requisitos do produto final, ainda que a primeira fatia técnica use um ativo e uma faixa de surpresa.

## 2. Contexto real, referências e fronteiras de responsabilidade

O mt5x é um framework Python de extração, armazenamento, análise e validação de históricos do MetaTrader 5, com interface Tkinter. O contexto de produto inclui Ativos, Minha base, Análise, Robôs e Atividade; extração M1 com retomada; backtester com custos e walk-forward; Laboratório com séries, correlações, defasagens e cobertura; feature store temporal; catálogo de hipóteses e snapshots para consumo MQL5 como evolução da Fase 13.

**Separar visão desejada de capacidade comprovada.** Na inspeção de 30/10/2026, sobre a base de código `54b6f3993e8bd39d37a373d7f767afc89f94b349`:

| Responsabilidade | Fonte encontrada | Uso e limite no Dashboard |
|---|---|---|
| Consulta de características e versões | `analytics/feature_store.py`, `analytics/query_service.py` | Reutilizar consultas canônicas, identidade da base, qualidade e disponibilidade temporal. |
| Volume, regime, sessão e produtores | `analytics/volume_profiles.py`, `market_regimes.py`, `session_structure.py`, `producers.py` | Ler saídas persistidas e suas definições; não recalcular ao abrir a página. |
| Relações, evidências e séries congeladas | `analytics/associations.py`, `relation_dto.py`, `datasets.py`; `laboratory/service.py` | Preservar estados exploratórios, recortes, manifestos, relógios e limites das relações. |
| Resultados de robôs e validações | `robots/catalog.py`, `robots/store.py`, `strategies/validation.py` | Vincular robô, versão, preset, execução, modelo e janela; nenhuma execução é iniciada pela leitura. |
| Relatórios já existentes | `strategies/report.py`, `laboratory/report.py` | Referências de tom, escape e exportação portátil; não copiar sua estrutura nem duplicar seu processamento. |
| Hipóteses, publicação MQL5 e governança F13 | T13.4, T13.5 e T13.8 no plano oficial | Ainda registradas como pendentes; não presumir catálogo completo, publicador ou monitor prontos. Reconferir antes de cada integração. |
| Interface de inteligência | T13.6 no plano oficial | Este Dashboard detalha a parte de pesquisa local no App. A aba MQL5/HADOUKEN mantém contrato e aceite próprios. |

`analytics/hypotheses.py`, `pattern_engine.py`, `publisher.py` e `monitoring.py` aparecem como arquivos **previstos** na especificação F13; não foram encontrados nessa base. Não criar versões substitutas dentro de `dashboard/`. Uma dependência ausente recebe estado explícito; sua implementação segue a frente responsável.

O `APPROVED` do catálogo de robôs corresponde à etapa de bateria (`bateria` no código atual). Ele **não é** o estado `APPROVED` de uma hipótese estatística. Relações `EXPLORATORY/INCONCLUSIVE` também não equivalem a hipóteses aprovadas.

### 2.1 Referências obrigatórias

Ler o [AGENTS.md](../../AGENTS.md) e as instruções dos diretórios ancestrais antes de qualquer mudança. Trabalhar no checkout canônico existente, preservando alterações de outras frentes e os pins de HADOUKEN/Safety.

- [Arquitetura](../../docs/arquitetura.md).
- [Roadmap](../../docs/ROADMAP.md) e [plano de trabalho](../../docs/PLANO_DE_TRABALHO.md), Fase 13.
- [Especificação Analytics](../../docs/analytics/ESPECIFICACAO.md), especialmente tempo, estados, publicação e interface.
- [Matriz de requisitos](../../docs/analytics/MATRIZ_REQUISITOS.md). O caminho citado originalmente como `propostas/MATRIZ_REQUISITOS.md` corresponde, neste checkout, a `docs/analytics/MATRIZ_REQUISITOS.md`.
- [Feature store](../../docs/analytics/T13.2_FEATURE_STORE.md) e [perfis/regimes](../../docs/analytics/T13.3_PERFIS_REGIMES.md).
- [Estrutura e posse de dados](../../docs/dados/PLANO_ESTRUTURA_DADOS.md) e [Laboratório](../../docs/sdk/laboratorio_series.md).
- [Continuidade](../../docs/continuidade.md), [retomada](../../CONTINUAR_AQUI.md) e [prévia de desenvolvimento](../../docs/TESTAR_DESENVOLVIMENTO.md).

Os documentos F13 contêm estados históricos. Quando divergirem, reconferir código, contratos e status atual do plano; registrar a divergência, sem promover um requisito a entrega por inferência.

## 3. Jornada e narrativa contínua

1. O usuário abre o Dashboard pela interface atual e escolhe **Ativo** ou **Robô**.
2. Define o contexto mínimo: ativo e TF; ou robô, revisão/preset e resultado persistido, com ativo/TF identificados. O recorte e o instante de referência ficam visíveis.
3. A página completa apresenta surpresa, contexto, evidência, validação e validade em faixas contínuas, com gráficos, números dominantes e rótulos curtos.
4. Um clique em qualquer evidência abre seu aprofundamento **dentro da mesma página**, preservando seleção e posição de leitura.
5. Um clique em “Omitidas · N” expande as evidências ocultas, com amostra e motivo em uma linha.
6. O usuário alterna tema sem recarregar e pode arquivar a fotografia completa em um HTML reabrível sem servidor.

### 3.1 Modo Ativo

Identidade mínima: base/origem, símbolo bruto e identidade canônica quando disponível, contrato ou metodologia da série contínua, TF, janela, calendário/fuso, `as_of` e versões das características. Símbolos homônimos de bases/corretoras distintas não são intercambiáveis.

A narrativa liga a surpresa ao regime, ao volume relativo, às relações e hipóteses aplicáveis ao contexto, à evidência temporal e à saúde das fontes. Outros ativos/TFs aparecem somente como contexto explicitamente vinculado, sem mistura de unidades, sessões ou versões.

### 3.2 Modo Robô

Identidade mínima: `robot_key`, fingerprint/revisão, preset e hash/revisão, `run_id` ou conjunto de execuções congelado, ativo, TF, período, modelo de execução, custos e capital/moeda quando necessários às métricas exibidas.

A narrativa começa pela maior mudança ou resultado comparável **já persistido**, conecta comportamento e risco ao contexto dos ativos, mostra validações e evidencia onde o resultado é inconclusivo. Resultados financeiros, contexto de mercado e estados da bateria mantêm rótulos próprios.

Se o robô tiver várias execuções, explicitar qual está selecionada. Agregações entre ativos/TFs exigem um resultado comparativo canônico com metodologia compatível; não somar taxas, PF ou curvas de execuções heterogêneas no Dashboard. Versões/presets diferentes nunca viram uma comparação temporal implícita.

Robô sem resultado continua selecionável: mostrar “Sem resultado neste recorte” e as evidências de contexto efetivamente disponíveis. A tela não inicia backtest, compilação, sessão, sincronização ou negociação para preencher uma lacuna. O modo Robô é gate obrigatório do aceite final.

### 3.3 Aprofundamento e baixa carga cognitiva

- Uma mensagem visual dominante por faixa; poucas métricas auxiliares diretamente relacionadas.
- Rótulos, unidades, datas e valores junto ao elemento. Texto curto auxilia o gráfico; parágrafos não são o veículo principal.
- Detalhes progressivos mostram recorte, amostra, baseline, IC, custos, OOS, origem e motivo de elegibilidade, conforme disponíveis.
- Filtros secundários ficam recolhidos. Alterações de seleção geram outra fotografia explícita; não mudam silenciosamente a história atual.
- Teclado, foco visível e alternativa textual compacta para gráficos. Cor, brilho, tooltip e hover não podem ser a única forma de entender uma evidência.
- Sem slides, paginação da narrativa, navegação obrigatória entre abas ou dashboards concorrentes para cada métrica.

## 4. Stack fixa e arquitetura de integração

### 4.1 Componentes

- **Flask** serve a página local, em loopback, porta padrão **5055**, configurável.
- **HTML5 + CSS3**, com variáveis de tema claro/escuro.
- **ECharts** para todos os gráficos. Plotly.js somente se uma limitação concreta do ECharts for demonstrada e registrada; evitar dois motores sem necessidade.
- **Tkinter permanece a interface do aplicativo.** Ele abre o navegador por `webbrowser`, sem renderizar HTML.
- Dados **JSON embutidos no HTML**, com recursos suficientes para um arquivo estático, versionável e reabrível meses depois.

```text
Ativos / Robôs (Tkinter) → seleção explícita → generator.py
                                             ↓
               serviços canônicos de leitura + resultados persistidos
                                             ↓
                            árvore JSON + proveniência + prioridades
                                             ↓
                    server.py / templates / dashboard.js / ECharts
                                             ↓
                         navegador aberto por opener.py
                                             ↓
                    exportação HTML autossuficiente e imutável
```

### 4.2 Estrutura prevista

```text
mt5_extracao/dashboard/
  PROMPT_DASHBOARD.md
  __init__.py
  server.py
  opener.py
  generator.py
  templates/
    index.html
    sections/
      surpresa.html
      regime.html
      volume.html
      hipotese.html
      validacao.html
      stale.html
      ocultas.html
  static/
    css/dashboard.css
    js/dashboard.js
    js/charts/
      surprise.js
      regime.js
      volume.js
      hypothesis.js
      validation.js
    vendor/                  # biblioteca gráfica local e licença
```

Criar cada arquivo quando sua etapa o utilizar. Estados stale/ocultas usam HTML/CSS e os gráficos que já representem a evidência; não exigem um novo gráfico só para existir um arquivo.

Os partials definem a estrutura semântica das faixas. `dashboard.js` instancia essas estruturas a partir da árvore JSON; os módulos de gráficos recebem payloads prontos. Servidor e exportador usam o mesmo documento e a mesma regra de renderização, sem caminhos divergentes para métricas e elegibilidade.

### 4.3 Contratos e ciclo de vida

**`generator.py`:** recebe símbolo/TF e o contexto temporal no modo Ativo; no modo Robô recebe sua identidade e referência dos resultados. Consulta o feature store e as evidências existentes, resolve elegibilidade, monta as seções, registra ausências, prioriza e devolve JSON estrito. A assinatura final deve seguir as convenções do App, sem introduzir uma API pública paralela de dados.

Usar `query_service` para catálogo/séries/manifestos quando seu contrato atender; `feature_store.query_as_of` para a fotografia temporal e `query_history` com filtragem causal explícita para histórico e auditoria quando necessário. `query_history` inclui revisões futuras e fatos vencidos: seu retorno bruto não pode alimentar a narrativa como conhecimento disponível no passado.

**`server.py`:** inicia uma instância local para a sessão do aplicativo, aguarda prontidão antes da abertura e encerra recursos próprios ao finalizar. Porta ocupada informa a correção em português, sem matar processo alheio ou mudar de porta silenciosamente. Rotas do Dashboard leem e exportam; não executam ações de robôs. Inspecionar o servidor existente em `robots/panel_server.py` para convenções de ciclo de vida, sem acoplar suas ações ao Dashboard.

**`opener.py`:** detecta plataforma e ambiente de execução, respeita a configuração e usa `webbrowser`/controladores explícitos. Windows: Edge/Chrome disponíveis; Linux/Wine: Firefox/Chromium e caminhos do prefixo quando aplicáveis. Navegador ausente produz instrução curta e URL copiável. Não instalar navegador, converter caminho host/Wine por concatenação nem presumir navegador dentro de todo prefixo. Validar o caminho realmente usado no host.

**Integração Tkinter:** acesso aditivo a partir de Ativos e Robôs; geração fora da thread da UI por mecanismo existente, sem novo scheduler/executor. Atualizações de widgets voltam à thread principal. Clique repetido não dispara servidores duplicados; troca rápida de seleção não permite que uma resposta antiga substitua a mais recente. Cancelamento/erro conserva a seleção anterior identificada.

**Configuração e dependências:** registrar chaves novas em `bootstrap/config_builder.py` e `config/config.ini.example`, com `fallback=` para instalações antigas. Flask só entra em dependências quando houver código que o utilize. Recursos gráficos ficam locais, com versão e licença registradas; não depender de CDN. Não instalar dependências nem alterar configuração nesta entrega documental.

## 5. Contrato da fotografia e integridade temporal

### 5.1 Envelope versionado

O contrato proposto abaixo é de **apresentação**, não de substituição dos DTOs analíticos. Finalizar e testar seu schema em `DASH-02`.

| Campo | Conteúdo obrigatório |
|---|---|
| `schema_version`, `snapshot_id` | Versão de leitura e identidade da fotografia. |
| `generated_at_utc`, `as_of_utc`, `data_cutoff_utc` | Instante de geração, conhecimento consultado e limite efetivo dos dados; não são sinônimos. |
| `subject`, `selection` | Modo ativo/robô e suas identidades, filtros, recorte, calendário/fuso e referências de execução. |
| `provenance` | Identidade lógica da base, manifestos/IDs e hashes disponíveis, fontes/revisões, algoritmos, versão do App/gerador, política de relevância e versão dos recursos gráficos. |
| `capabilities` | Disponível, sem dados, não aplicável ou dependência pendente, com código e motivo curto por família. |
| `sections` | Todas as faixas auditáveis, com ordem, estado, elegibilidade, evidência, payload visual e origem. |
| `alerts` | Invalidações/correções conhecidas no `as_of`, com impacto e vínculo à evidência afetada. |
| `baseline_snapshot_id` | Revisão anterior comparável usada para novidade; `null` se inexistente. |
| `integrity` | Digest do payload canônico, com algoritmo e escopo excluindo o próprio campo de hash; hashes de recursos quando aplicáveis. |

### 5.2 Contrato mínimo por faixa

`id`, `kind`, `title`, `subject_ref`, `source_refs`, `as_of_utc`, `status_namespace`, `source_status`, `display_state`, `default_visible`, `hidden_reason_code`, `hidden_reason_short`, `n_observations`, `unit`, `quality`, `temporal`, `evidence`, `priority`, `visual` e `details`.

- `source_status` conserva o estado original; `status_namespace` distingue hipótese, robô, relação e execução.
- `display_state` representa `AVAILABLE`, `INSUFFICIENT`, `UNAVAILABLE`, `STALE`, `INVALIDATED` ou `NOT_APPLICABLE`, sem escrever um novo estado na origem.
- `evidence` referencia os valores existentes de lift/expectativa, taxa-base, IC e seu nível/método, amostra, custos, walk-forward e OOS. Campo desconhecido é `null` com motivo, nunca zero fabricado.
- `temporal` conserva `observation_time`, `available_at_utc`, `valid_until_utc`, `source_revision` e versão do algoritmo para os fatos usados. Evidências com várias fontes preservam seus vínculos individuais.
- `priority` contém fatores normalizados, entradas usadas, grupo de elegibilidade, score, desempate e versão da política. Explicar a ordem deve ser possível sem consultar um banco atual.
- `visual` fornece séries, intervalos, unidades, rótulos e domínios de escala; `details` permite aprofundamento offline sem buscar dados depois.
- IDs de faixa são únicos e estáveis na fotografia. `default_visible=false` mantém a evidência no JSON auditável sem montar seu cartão/gráfico no fluxo principal.

### 5.3 Regras temporais e estados sem dados

1. Um fato só pode influenciar o contexto em `t` quando `available_at_utc <= t`, observação compatível e qualidade elegível. Havendo validade, `t < valid_until_utc`.
2. Não incorporar barra ainda aberta, incompleta ou futura como fechada. Preservar a política do produtor e a sessão observada, sem completar o calendário por suposição.
3. Sparkline/histórico deve declarar se mostra o conhecimento em cada instante ou a reconstrução conhecida no corte. Não misturar as duas leituras. Séries de decisões históricas usam junção temporal por decisão.
4. Fotografia vencida pode aparecer como **contexto histórico stale**, identificada com seu instante original. Ela não volta a ser evidência vigente.
5. `STALE` decorre de validade/contrato da fonte. Ausência de prazo é “Validade não declarada”, não autorização para fabricar TTL. Idade sozinha não define expiração.
6. Falta de dados, zero legítimo, insuficiência, não aplicável, vencimento e invalidação são estados distintos. Sem dado não vira linha horizontal em zero.
7. Consulta de várias fontes usa revisão/manifesto consistente ou verifica estabilidade de geração; alteração durante a leitura pede repetição limitada ou erro claro. Não combinar silenciosamente gerações distintas.
8. Schema desconhecido ou payload inválido interrompe a geração afetada com motivo. O leitor offline não tenta reinterpretar contrato incompatível.

### 5.4 Arquivamento autossuficiente

O arquivo final inclui **JSON, CSS, JavaScript, biblioteca gráfica, ícones e fontes necessárias**, embutidos; preferir fontes do sistema. Não basta embutir JSON e deixar ECharts, partials ou detalhes apontando para Flask/CDN.

- Arquivo abre por `file://` com servidor desligado e rede indisponível, conservando todas as faixas, temas, interações e aprofundamentos do recorte arquivado.
- Exportar para o destino oficial de relatórios/`exports/` conforme configuração, nunca para fontes sincronizadas, código ou arquivos de usuário já existentes sem autorização explícita.
- Gravar por mecanismo seguro de arquivo temporário e finalização sem sobrescrita acidental; validar JSON e recursos antes de anunciar sucesso.
- Escape de `<`, `>`, `&` e sequências perigosas ao embutir JSON; texto de origem tratado como texto, nunca HTML executável. JSON estrito sem `NaN`/infinito.
- Hash comprova correspondência de conteúdo; não constitui assinatura de autenticidade. O escopo do hash deve ser reproduzível e documentado.
- Versões de renderer, política e recursos seguem dentro do artefato. Arquivos antigos não passam a usar o JavaScript novo do servidor.
- Idade/stale e estado de evidência são os da geração/corte, identificados como tal. Relógio do navegador não reescreve a fotografia histórica ao reabrir meses depois.
- Invalidação posterior gera **nova fotografia**, com vínculo à anterior. O HTML antigo permanece intacto e não tem como conhecer correções posteriores sem nova consulta explícita.
- Ocultar/expandir uma faixa ou alternar tema não altera os fatos, hashes ou arquivo original. Estado de interação é local à página; nova exportação de contexto gera outra identidade quando seu conteúdo mudar.
- Não embutir credenciais, contas identificáveis, caminhos privados ou texto bruto de terceiros. Usar identidades lógicas e evidências compartilháveis.

## 6. Vocabulários visuais por faixa

**Regra de ouro: se precisa de legenda, a visualização falhou.** Eixos, unidades, rótulos diretos e amostra são informação; uma chave separada de cores/séries não pode ser necessária para entender o gráfico. Tooltip aprofunda, mas não resgata um desenho incompreensível.

| Faixa | Forma obrigatória | Leitura direta e interação |
|---|---|---|
| Surpresa | Número gigante, seta direcional, sparkline de 30 dias | Nome da métrica, unidade, referência e recorte ao lado do valor; seta descreve variação, sem significar ordem. Amostra fraca permanece visível como tal. Clique revela baseline e origem. |
| Regime | Heatmap: X = hora, Y = dia da semana, cor = intensidade | Título identifica a intensidade representada e sua unidade. Célula atual destacada com brilho e contorno/rótulo; valores ou rótulos diretos dispensam escala de cores externa. Lacuna visível, sem fingir intensidade zero. |
| Volume | Área suave, mediana tracejada, preenchimento apenas acima do limiar RVOL | Séries/limiar rotulados na própria linha, com `RVOL x`, fonte de volume e comparação no mesmo horário/sessão. Suavização não inventa observações nem altera os valores nos detalhes. |
| Hipótese aprovada | Cartão com anéis concêntricos: amostra no centro, lift no anel e confiança no externo | `n`, lift e IC/nível/método com rótulos diretos e escala declarada. Anéis não convertem lift sem limite em percentual nem IC em probabilidade de lucro. Se a confiança não tiver semântica definida, anel neutro e “Não informada”. |
| Validação | Faixa temporal walk-forward com zonas treino/validação/teste e marcação OOS | Nomes e datas dentro/junto das zonas; cada fold identificado. Purging/embargo aparecem quando presentes no recibo. Sobreposição visual nunca transforma treino em teste. Clique mostra custos, amostra e resultado daquele fold. |
| Stale | Borda âmbar fina e idade curta | “Snapshot de … · idade na geração …” junto à evidência afetada. Evitar repetir toda a mesma evidência numa faixa redundante. Não usar vermelho para vencimento. |
| Ocultas | Ícone de balança desequilibrada, contagem e tamanho da amostra | Uma linha por motivo; expansão em um clique, recolhimento em um clique. `n` desconhecido aparece como “n não informado”. Ícone acompanhado de nome acessível. |
| Correção retroativa | Alerta no topo, com vermelho reservado à invalidação retroativa | Identifica evidência, revisão anterior/nova, instante conhecido e impacto. Detalhe e amostra acessíveis no mesmo fluxo, inclusive para hipótese retirada. |

As cinco primeiras famílias de gráficos usam ECharts. Anéis podem usar séries concêntricas com rótulos próprios; não chamar circunferência preenchida de evidência validada quando o campo está ausente.

### 6.1 Tema e comportamento visual

CSS com tokens semânticos de superfície, texto, borda, foco, destaque e âmbar de stale. Vermelho reservado à invalidação retroativa; perdas/desvios negativos usam sinal, posição, unidade e rótulo. Tema claro/escuro atualiza também cores dos gráficos, sem recarregar ou refazer consultas.

Layouts responsivos e tipografia numérica legível. Validar os tamanhos usuais do App e zoom; nenhuma rolagem horizontal deve ser necessária para ler a narrativa. Respeitar redução de movimento; brilho não pisca. Descartar instâncias de gráficos removidos e redimensionar ao expandir detalhes/faixas.

## 7. Elegibilidade, priorização e governança de apresentação

### 7.1 Ordem obrigatória

1. **Exceção de integridade:** invalidação retroativa conhecida no corte aparece no topo, mesmo que a origem esteja `RETIRED`.
2. **Surpresa:** maior número-surpresa em magnitude absoluta abre a narrativa regular, mesmo com evidência fraca. O tamanho da amostra e a insuficiência ficam visíveis.
3. **Hipóteses `APPROVED`:** as de maior impacto sustentado e lift relevante vêm em seguida.
4. **`VALIDATING`:** depois das aprovadas, sempre com rótulo de validação em andamento.
5. Contexto de regime, volume, validação e stale fica ligado à evidência que explica, em ordem ajustada por relevância dentro desses limites.
6. **`EXPLORATORY`:** somente expansão manual. **`DRAFT`:** somente auditoria manual, sem aparência de conclusão.
7. **`MONITORING`:** não herda aprovação automaticamente; mostrar a mudança/deriva quando houver recibo, conservando estado e motivo. Se não elegível no contexto, manter na auditoria.
8. **`RETIRED`:** não renderiza na narrativa nem na lista comum de ocultas; exceção exclusiva da correção retroativa acima.

“Absoluto” significa maior magnitude da **mesma medida de surpresa comparável**, definida pelo produtor. Não escolher o maior número bruto entre preço, RVOL, percentual e lift. Ausência dessa medida não autoriza criar um novo cálculo analítico no Dashboard: registrar dependência e mostrar “Surpresa indisponível”. A escolha da métrica/fonte deve ficar fechada em `DASH-02` antes de sua promoção para dado real.

### 7.2 Três pesos multiplicados e normalizados

Para cada candidata elegível e comparável:

```text
I = impacto normalizado: magnitude de lift / expectativa segundo a família
E = evidência normalizada: amostra, IC, walk-forward e OOS disponíveis
N = novidade normalizada: mudança desde revisão comparável, deriva,
    rebaixamento ou snapshot vencido
score_bruto = I × E × N
score_normalizado = score_bruto / soma(score_bruto das candidatas do grupo)
```

Essa é uma política de **ordenação visual**. Não redefine lift, IC, amostra mínima, aprovação ou validade. Em `DASH-11`, registrar transformações, referências, limites e tratamento de ausentes por família, com fixtures determinísticas. Campos sem método/semântica não recebem confiança inventada.

Regras para fechar a política sem falhas de ordenação:

- Magnitudes de unidades diferentes só são comparadas após normalização documentada e domínio compatível. Direção/sinal original permanece visível.
- Novidade neutra ou primeira revisão não equivale a fator zero: usar fator basal positivo documentado, sinalizando “Sem comparação anterior”. Ausência de medição não significa ausência de mudança.
- Se algum fator não puder ser apurado, registrar `score_state=UNAVAILABLE`; não esconder a ausência atribuindo valor favorável. A ordenação cai no desempate determinístico dentro do grupo permitido.
- Se a soma for zero, não dividir por zero nem apagar o grupo. Ordenar por impacto conhecido, evidência conhecida e ID estável, registrando o motivo.
- Empates: grupo obrigatório, score válido, magnitude de impacto comparável, evidência e ID estável. Políticas e parâmetros ficam no snapshot.
- Não comparar novidade entre identidades, presets, versões, janelas, custos ou políticas incompatíveis. Sem predecessor compatível, `baseline_snapshot_id=null`.
- Sem baseline persistido disponível, aceitar referência explícita a fotografia anterior do mecanismo existente de relatórios; não criar cache, tabela de histórico ou journal financeiro para obter novidade.

Regras de elegibilidade e estados da origem são aplicadas **antes** do score; nenhum produto de pesos torna exploratória uma hipótese aprovada nem promove exploratória para aprovada. A exceção da surpresa afeta destaque, sem elevar seu nível de evidência.

### 7.3 Rejeitadas e ocultação em um clique

“Rejeitada/omitida” no Dashboard pode significar fora do contexto, evidência insuficiente, origem exploratória, duplicação explicativa ou ocultação manual. **Não é um novo estado do catálogo.** Conservar o estado canônico em campo separado e um motivo curto para apresentação.

- Por padrão, o fluxo principal não cria cartão/gráfico das faixas rejeitadas.
- O controle “Omitidas · N” sempre permite auditá-las em um clique, com motivo e amostra.
- Uma faixa exibida pode ser ocultada localmente e reaparecer na lista auditável; restaurar mantém seu estado original e a posição prevista pela política.
- Expandir exploratória mostra seu rótulo e evidência, sem transformá-la em aprovada. Ocultar faixa não reprova nem retira hipótese na origem.
- Alerta de invalidação retroativa permanece sinalizado no topo; pode recolher detalhes, mas não desaparecer por regra genérica de ocultação.
- Nenhuma edição/aprovação/rejeição operacional é feita pelo Dashboard. A auditoria apenas lê os motivos já existentes ou a decisão visual desta fotografia.

## 8. Inventário de capacidades e integração gradual

Antes de implementar cada família, manter em seu PR a ligação **capacidade → serviço/DTO → evidência → faixa → detalhe → teste**. O catálogo do produto final cobre:

| Capacidade | Lugar na história | Condição para aparecer |
|---|---|---|
| Extração, qualidade, cobertura e identidade | Base temporal e confiança nos dados | Recibos/cobertura disponíveis; lacunas não viram uma conclusão sobre o mercado. |
| Sessão, sazonalidade, volatilidade e regime | Surpresa/contexto | Valores persistidos com algoritmo, unidade e calendário conhecidos. |
| Volume relativo e estrutura intradiária | Volume e seu aprofundamento | Mesmo horário/sessão e família de volume declarada; real e tick volume distintos. |
| Séries, correlações, defasagens e relações | Evidência vinculada e detalhes | Recorte e origem preservados; exploração só manual; correlação não rotulada como causa. |
| Hipóteses, taxa-base, lift e IC | Cartão de hipótese e detalhes | Contrato canônico de hipótese/evidência efetivamente disponível. |
| Custos, backtest e walk-forward/OOS | Validação da evidência/robô | Resultado persistido com identidade, amostra e método; sem executar novos estudos. |
| Robô, preset, bateria, execuções e risco | Narrativa Robô | Vínculos conferidos; aceite de bateria separado de evidência estatística e PnL realizado. |
| Publicação, revisão, validade e deriva | Stale/correção/novidade | Recibo canônico existente; não exigir publicação MQL5 para consultar pesquisa local. |
| Fontes vinculadas, macro e menções | Contexto secundário, quando pertinente | Disponibilidade temporal, qualificação e limites já aprovados pela fonte; não ampliar acesso ou concluir causalidade. |

Capacidade sem fonte pronta fica registrada em `capabilities`, com motivo curto consultável. Não espalhar cartões vazios para cada função nem esconder a limitação com demonstração sintética no fluxo real. Esse inventário deve ser reconferido no fechamento, para não reduzir o produto apenas a regime/volume e esquecer a história dos robôs e das relações existentes.

## 9. Ordem de execução e entregáveis por etapa

**Uma etapa por PR.** Reusar branch/PR da mesma etapa quando existir. Não criar checkout paralelo, alterar pins ou incluir WIP alheio. As dependências podem ser preparadas documentalmente, mas uma faixa só é declarada integrada depois de ter origem e teste demonstrados.

| Etapa | Entrega concreta | Dependências | Aceite local da etapa |
|---|---|---|---|
| DASH-00 | Este plano registrado e ligado ao painel oficial | Pedido do proprietário | Referências/diff revisados; nenhuma alegação de Dashboard funcionando. |
| DASH-01 | `server.py` + `opener.py` + casca `index.html`; página vazia abre no navegador | Instruções/configuração e fonte única de lifecycle | Porta configurável; prontidão, encerramento, porta ocupada e ausência de navegador tratados. |
| DASH-02 | `generator.py` com **uma faixa de surpresa** e JSON real do feature store; schema/proveniência | DASH-01; fonte de surpresa e leitura causal conferidas | Fotografia real ou indisponibilidade honesta; métrica, unidade, recorte, identidade e clocks explícitos; sem valores demo no caminho real. |
| DASH-03 | `dashboard.js` monta a surpresa a partir do JSON; sparkline 30 dias; primeira exportação portátil | DASH-02 | Número/seta/série clicáveis; lacunas/insuficiência explícitas; mesmo payload no servidor e arquivo offline. |
| DASH-04 | Faixa de **regime**, partial e módulo próprios | DASH-03; saída canônica de regime | Heatmap hora × dia, intensidade definida e célula atual destacada; ausência não vira zero. |
| DASH-05 | Faixa de **volume** | DASH-04; perfis canônicos | Mediana tracejada, RVOL/limiar e realce acima dele, comparação de sessão consistente. |
| DASH-06 | Faixa de **hipótese** | DASH-05; contrato/evidência T13.4 realmente disponível | Anéis com amostra/lift/confiança definidos; APPROVED distinto de bateria e exploração. Sem dependência pronta, registrar pendência; não criar motor substituto. |
| DASH-07 | Faixa de **validação** | DASH-06; recibos de validação canônicos | Treino/validação/teste/OOS por fold e custos conferíveis; dados incompletos não aparentam teste aprovado. |
| DASH-08 | Apresentação **stale** e invalidação retroativa | DASH-07; metadados de validade/revisão | Âmbar para vencido, vermelho só para invalidação; fotografia antiga intacta. |
| DASH-09 | **Ocultas**, descarte e restauração de apresentação em um clique | DASH-08 | Não renderizam por padrão; expandem com `n` e motivo; RETIRED respeita exceção. |
| DASH-10 | Seleção e narrativa **Robô**, ligadas aos resultados persistidos e contexto | DASH-09; catálogo/store/recibos existentes | Identidade por revisão/preset/run; ausência e várias execuções tratadas; nenhuma execução iniciada. |
| DASH-11 | Priorização automática por impacto × evidência × novidade | DASH-10; entradas e baseline comparáveis | Precedências, normalizações, zero/ausentes, empates e scores explicáveis no próprio JSON. |
| DASH-12 | Alternância **claro/escuro**, acabamento responsivo e acessibilidade | DASH-11 | Tema sem reload/refetch; gráficos acompanham; teclado, foco e leitura sem legenda. |
| DASH-13 | Integração final em Ativos e Robôs pelo launcher habitual; desempenho e regressões | DASH-12 | UI não bloqueia, troca rápida não mistura seleção, geração/renderização medidas no host; exportação offline completa. |
| DASH-14 | Aceite consolidado, documentação de uso/limites e continuidade | DASH-13 | Matriz abaixo preenchida com evidências reais e pendências por plataforma; incremento publicado conforme regras vigentes, sem merge/release automático. |

A ordem de **implementação** das faixas solicitada é preservada: **surpresa → regime → volume → hipótese → validação → stale → ocultas**. Essa sequência não fixa a posição das famílias na página final: a narrativa renderizada obedece à precedência de §7.1, com contexto vinculado à evidência que explica. O modo Robô entra explicitamente antes da priorização final para que suas evidências façam parte da política. Sua identidade e contratos são previstos desde DASH-02.

O tema tem tokens desde a casca, mas a alternância é entregue na etapa própria. Testes pertinentes acompanham cada PR; DASH-14 consolida os resultados, não adia todos os testes para o final.

As dependências entre etapas da tabela exigem a infraestrutura de apresentação correspondente, não aprovação de um produtor ainda ausente. Por exemplo, DASH-06 pode entregar o contrato visual, o leitor da interface disponível e o estado explícito de dependência pendente, testados com fixture identificada; essa infraestrutura libera DASH-07…10, inclusive Robô. Registrar separadamente **infraestrutura validada** e **integração real pendente**: não marcar a faixa funcional ou a etapa inteira como concluída nesse caso. Assim que o produtor existir, integrar sua evidência no PR da mesma etapa, reconferindo o contrato.

Uma dependência T13.4/T13.5/T13.8 ainda ausente não impede a fatia local já elegível, mas mantém a faixa dependente e o aceite completo pendentes. Qualificação estatística/financeira, publicação MQL5 e aba HADOUKEN continuam nas frentes originais.

## 10. Testes e critérios de aceite verificáveis

### 10.1 Matriz de aceite do produto

Todos os itens começam **pendentes**. Este documento não é evidência de execução.

| ID | Critério | Evidência exigida para concluir |
|---|---|---|
| AC-01 | Uma escolha ativo+TF gera a página completa em **menos de 3 s** no host de validação | Host/SO/navegador, identidade e tamanho da fixture/recorte, limites do payload, tempos de consulta/geração/render e tempo total até gráficos visíveis. Medir série representativa de pelo menos 20 seleções, frio e quente identificados; registrar máximo e p95, ambos abaixo de 3 s no domínio aceito. |
| AC-02 | Visão por ativo **e** robô conta uma história coerente | Percurso com ativo, robô com resultado, robô sem resultado e robô com vários presets/runs; origem e limites reconhecíveis por leitura direta. |
| AC-03 | Zero texto corrido como veículo principal | Inspeção das faixas: número/gráfico/rótulo curto comunica cada conclusão; detalhes extensos ficam recolhidos. |
| AC-04 | Faixa rejeitada some e reaparece com motivo em um clique | Teste de DOM e interação: ausente do fluxo inicial, JSON auditável preservado, expansão/restauração com amostra/motivo, sem escrita na origem. |
| AC-05 | Mesma informação e interação no Windows nativo e Linux/Wine | Mesma fixture e payload, conferência visual/funcional em navegadores disponíveis, abertura pelo Tkinter e caminhos reais. Não exigir pixels tipográficos idênticos entre engines; exigir conteúdo, hierarquia e comportamento equivalentes. |
| AC-06 | HTML arquivado meses depois mostra os mesmos dados | Reabrir cópia em `file://`, servidor desligado/rede bloqueada, relógio de teste avançado; hashes, valores, clocks, estados e recursos preservados. Ensaio simulado de futuro identificado como tal. |
| AC-07 | Tema claro/escuro sem recarregar | Seleção, scroll, detalhes e dados preservados; nenhuma consulta ou navegação; todos os gráficos recoloridos. |
| AC-08 | Nenhum gráfico exige legenda | Tarefa de leitura por inspeção: distinguir medida, unidade, referência, estado e período usando rótulos diretos; tooltip apenas complementa. |
| AC-09 | Scroll contínuo e aprofundamento na página | Percurso por faixas e detalhes sem slides, fragmentação da história ou saída obrigatória para outra guia. |
| AC-10 | Priorização reproduzível e temporalmente correta | Fixtures com precedência, surpresa fraca, baseline ausente, score zero, empate, exploratória e RETIRED/correção; nenhum dado futuro ou versão incompatível. |
| AC-11 | Integração aditiva e sem efeitos operacionais | Abrir/fechar Dashboard não altera Ativos/Robôs, schemas de dados, estado da hipótese, arquivos MQL5, ordens ou resultados existentes. |
| AC-12 | Fotografia tem proveniência e trata falhas honestamente | Identidade completa, recursos locais, schema validado, ausências explícitas; troca de geração detectada e JSON adversarial não executa HTML/script. |

O limite de 3 s é fim a fim, da confirmação da seleção à página completa utilizável, incluindo gráficos padrão. Detalhes recolhidos podem ser instanciados ao expandir, mas seus dados já devem estar no arquivo. Medir também o modo Robô no mesmo orçamento quando seu domínio estiver fechado.

Não atingir o limite reduzindo silenciosamente evidências, usando fixture minúscula, omitindo faixas elegíveis ou contando apenas o tempo do Flask. Definir em DASH-02 o domínio de volume/recorte representativo e o teto de payload com base em medição, sem criar cache ou recalcular análises. Consultas limitadas que excedam capacidade devem informar o limite e pedir recorte explícito.

### 10.2 Cobertura mínima automatizada

Fixtures sintéticas usam os contratos reais de leitura/persistência em base própria de QA. Nenhum teste exige MT5 real ou usa negociação REAL/dados privados como fixture.

- **Gerador:** ativo/robô, identidade completa, ausência de fonte, símbolo não encontrado, base indisponível, versões distintas, `NaN`/infinito e limites de seleção.
- **Temporal:** disponibilidade posterior à observação, barra aberta, validade no instante limite, revisões tardias, histórico sem vazamento futuro e mistura de gerações recusada.
- **Prioridade:** precedência do alerta/surpresa/aprovada/validando, sinais e magnitudes comparáveis, amostra fraca, primeira revisão, score zero/ausente e empate estável.
- **Estados:** `DRAFT`, `EXPLORATORY`, `VALIDATING`, `APPROVED`, `MONITORING`, `RETIRED`, namespaces de robô/relação e motivo de rejeição. Nenhuma promoção por renderização.
- **HTML/JS:** fixture de página completa; faixas rejeitadas sem cartão/gráfico padrão; expandir/ocultar/restaurar, clique de aprofundamento, tema e seleção preservados.
- **Arquivo:** abrir offline com servidor parado e sem requisições de rede; sem fetch obrigatório em detalhes; valores/estados idênticos após avanço do relógio; exportação sem sobrescrita e digest reproduzível.
- **Entrada hostil:** títulos/motivos com `</script>`, caracteres especiais e texto HTML não executam código nem quebram o JSON embutido.
- **Servidor/opener:** bind local, porta configurada/ocupada, prontidão, encerramento próprio, navegador inexistente, caracteres especiais em caminhos e ramos Windows/Linux simulados.
- **Integração Tk:** clique repetido, cancelamento e troca rápida de seleção não duplicam instâncias nem exibem resposta antiga como atual; abas existentes continuam utilizáveis.

Testes de renderização/interação devem exercitar o navegador; analisar texto do HTML não comprova que gráficos funcionam. Dados sintéticos validam contrato/UI, sem qualificar uma hipótese de mercado ou rentabilidade.

### 10.3 Gates, evidências e plataformas

Seguir os comandos oficiais em AGENTS.md e o wrapper/ambiente de CONTINUAR_AQUI. Para código, executar testes pertinentes, suíte/gates exigidos pelo projeto e doctor; shellcheck quando houver alteração de shell. Para este lote somente documental, revisar diff, referências e coerência, sem alegar teste de produto.

Cada PR registra: etapa/escopo, fonte canônica, commit, testes e comandos/resultados, fixture/recorte, evidência visual quando aplicável, tempos/limites e o que continua pendente. Disponibilizar avanço testável na prévia habitual após os checks pertinentes, preservando o ambiente e o retorno existentes.

Linux/Wine e Windows nativo são de primeira classe. Exercitar os ramos simulados no desenvolvimento não substitui Windows físico. Se o host disponível for Linux, concluir a etapa nele e registrar o aceite Windows pendente no [checklist existente](../../docs/validacao_instaladores.md), sem afirmar equivalência já comprovada. A certificação completa multiplataforma só fecha com a evidência real de ambos.

## 11. O que não fazer

- Não migrar Tkinter nem incorporar um renderizador HTML nele.
- Não alterar cálculos, contratos analíticos, custos, regras operacionais, bateria ou políticas Safety.
- Não criar scheduler, executor, cache, journal financeiro ou outra fonte de estado de negócio.
- Não enviar ordens; Python continua sem `order_send`.
- Não duplicar o que `strategies/report.py` já faz; ele é referência de tom, não de estrutura.
- Não bloquear a aba Ativos nem substituir suas ferramentas. Dashboard é aditivo e também acessível por Robôs.
- Não executar backtest/reprocessamento, publicar snapshots MQL5 ou promover/rebaixar hipóteses ao abrir um gráfico.
- Não inventar confiança, IC, OOS, baseline, comparação entre versões ou valores neutros para preencher ausência.
- Não usar CDN, dados consultados depois da abertura ou recursos do servidor como dependência do HTML arquivado.
- Não acrescentar outra tecnologia de frontend/framework de aplicação sem necessidade demonstrada dentro da stack fixa.
- Não declarar escopo completo com apenas um protótipo de surpresa, fixtures bonitas ou testes sem percurso visual real.

## 12. Riscos concretos, decisões e definição de conclusão

| Risco/decisão | Tratamento e momento de fechamento |
|---|---|
| Catálogo de hipóteses/publicador previstos como se já existissem | Reconferir em DASH-00/02; fonte pendente fica explícita. DASH-06/08 não inventam serviços substitutos. |
| “Confiança” sem definição única | Conservar método/nível por fonte; anel sem valor quando ausente. Fechar mapeamento em DASH-06, sem resolver a Decisão 5 da F13 por desenho gráfico. |
| Surpresa entre grandezas diferentes | Selecionar medida canônica comparável e seu histórico em DASH-02; não criar z-score/cálculo novo no Dashboard. |
| Normalizações e novidade sem baseline | Fechar tabela/política versionada em DASH-11; neutro positivo, comparação compatível e ausência explícita. |
| Perda de proveniência por consultas simultâneas/revisões | Usar snapshot/manifesto consistente ou verificação de geração; recusar mistura em DASH-02. |
| Página moderna com carga cognitiva ainda alta | Revisar narrativa real de ativo e robô, hierarquia e uma mensagem por faixa em DASH-12/14. |
| HTML “portátil” dependente de CDN/Flask | Empacotar recursos desde DASH-03; ensaio offline completo em DASH-13/14. |
| Biblioteca gráfica e payload tornam o carregamento lento | Medir desde a primeira fatia, limitar por domínio explícito e reaproveitar leituras dentro da mesma geração, sem cache persistente novo. |
| Browser/abertura distintos entre SOs | Caminhos via configuração/plataforma, testes simulados e percurso real no host; pendência Windows registrada sem bloquear o avanço Linux. |
| Confundir ocultação visual com mudança de aprovação | UI de leitura e estado de apresentação separados; nenhuma escrita no catálogo. |
| Expandir o Dashboard para reparar produtores e Lab | Registrar dependência na frente dona; corrigir aqui somente apresentação/integração de seu escopo. |

**Este registro está concluído quando:** o arquivo existe no repositório real, está ligado ao roadmap/painel/continuidade e teve escopo e referências revisados. Ele não implementa o Dashboard.

**A implantação está concluída quando:** as etapas DASH-01…14 e AC-01…12 têm evidência correspondente; seleção de ativo e robô funciona no fluxo habitual; a narrativa é visual e contínua; ocultas e detalhes são auditáveis em um clique; prioridades e estados são explicáveis; HTML reabre offline com os mesmos fatos; e aceites não exercitados continuam declarados como pendências, sem selo de conclusão integral.

**Próxima ação de implementação:** DASH-01 — página vazia servida localmente e aberta por `opener.py`, com configuração e lifecycle testados. Depois, DASH-02 usa uma faixa real de surpresa e fecha identidade, origem, schema, domínio de desempenho e corte temporal antes de adicionar as demais faixas.

### Registro de implementação — 31/10/2026

DASH-01: servidor Flask local e opener implementados; casca sem métricas,
CLI `dashboard --no-browser --seconds N`, porta5055 configurável e recursos
empacotados. Configurações antigas usam fallback; `[DASHBOARD] browser` vazio
usa o navegador do host, ou escolha explícita/executável sem shell. Windows
tem descoberta Edge/Chrome/Firefox testada por simulação; físico pendente.

Servidor inicia/encerra recursos próprios, verifica prontidão e recusa porta
ocupada sem escolher outra. Capacidade efêmera, Host/Origin e métodos limitam
o acesso local. Não há rotas operacionais. O launcher identifica `dashboard`
antes de iniciar a ponte, inclusive após opções globais; demais comandos
conservam seu fluxo. Abrir a casca não constrói banco ou conector.

Uso: `./run.sh --cli dashboard` (Windows: `run.bat --cli dashboard`).
Encerrar com Ctrl+C; `--no-browser` permite copiar a URL. Para testes limitados,
`--seconds 30` encerra o servidor próprio. Browser real no Linux abriu a página;
evidência local em `state/qa-tmp/dashboard`. Próxima etapa: DASH-02.
DASH-02…14 e AC-01…12 continuam pendentes; esta casca não é a narrativa entregue.

### DASH-02 — fotografia e limites

Infraestrutura implementada; **integração da surpresa real pendente**. A busca
em Analytics/Laboratório não encontrou medida canônica comparável de surpresa.
Não se transformou preço, RVOL ou percentil em outro escore. Necessidade para
Dados: medida/ID e versão, unidade/domínio comparável, referência/baseline,
amostra e série30dias com clocks/validade. Sem esse contrato, a primeira faixa
explica `SURPRISE_PRODUCER_MISSING`; valor/seta/série continuam ausentes.

`generator.generate` lê `query_service.definition_catalog` e
`feature_store.query_history` dentro de `DatabaseManager.consistent_read`;
`datasets.identity(connection=...)` não inicializa schema. Base sem identidade
produz erro com ação, sem DDL. O histórico é explicitamente filtrado por
observação, recebimento e disponibilidade até o corte; a precedência de revisão
é a do store, conservando vencidos/invalidados para auditoria. A série é a
**reconstrução conhecida no corte**, não o conhecimento em cada decisão antiga.
Contratos registrados depois do corte ficam fora. Versões coexistentes exigem
escolha explícita, sem comparar unidades diferentes. Fonte/namespace, contexto
do relógio, revisão e metadados temporais permitidos ficam na fotografia.

O schema1 reserva envelope, seções, fontes, inventário, ausências e prioridade
indisponível. Identidade Robô, leitor de resultados e sua validação entram em
DASH-10; não se apresenta Ativo como se fosse esse percurso. Hash SHA256 usa
JSON canônico ordenado, sem `integrity`, com `snapshot_id=null`; comprova
correspondência, sem assinatura de autenticidade. Servidor copia e valida o
payload ao abrir, sem consultar base depois nem aceitar alteração do chamador.
JSON embutido usa escape do Jinja; texto de origem não é HTML executável.

Domínio inicial explícito: janela até31dias,20.000 revisões consultadas e8MiB
de JSON. Excesso é recusado e pede recorte; sem cache/truncamento. Bancada
Linux sintética reutilizável:28dias,4features,672 observações/feature,
2.688fatos/1.209.172bytes.20 gerações: primeira256ms, máximo382ms;
p95 das19 seguintes363ms. Esses tempos são só consulta/geração, **não AC-01**.
A medição fim a fim com gráficos continua pendente em DASH-13/14.

CLI: `dashboard --symbol QA --tf M5 --as-of DATA_COM_FUSO`; `--from`/`--to`
recortam, `--version ID VERSAO` resolve ambiguidade. `--fixture-label` identifica
QA explicitamente. Na UI, corte, janela, motivo e proveniência ficam visíveis
ou expansíveis; nenhuma faixa inventa evidência para ocupar espaço.

| Capacidade | Serviço/DTO consumido ou identificado | Evidência → faixa/detalhe → teste |
|---|---|---|
| Identidade/qualidade/cobertura | datasets.identity, feature_store | namespace/clocks/qualidade → contexto/auditoria → test_dashboard_generator |
| Calendário/retornos/volatilidade/regime/volume | definition_catalog + query_history | fatos/versões capturados; faixas DASH-04/05 pendentes → mesmo teste temporal |
| Séries/correlações/defasagens | query_service.relations, relation_dto, laboratory.service | leitor de apresentação pendente; exploração não promovida |
| Estudos/notas/fontes/macro/menções | laboratory.service/links e snapshots | contrato identificado, integração contextual pendente |
| Hipóteses/taxa-base/lift/IC | T13.4 | dependência ausente → indisponibilidade explícita → teste de ausência |
| Backtest/custos/OOS/robôs/comparações | robots.catalog/store, strategies.validation, laboratory.financial | leitores DASH-07/10 pendentes; bateria não é aprovação estatística |
| Validade/revisão/deriva/publicação | feature_store; governança T13.8/publicador T13.5 pendentes | clocks retidos → DASH-08; sem TTL inventado |

Todos os itens de `CAPABILITY_SOURCES` ficam no JSON com estado/motivo; fonte
identificada não é integração concluída. Próxima: DASH-03, renderer/arquivo
autossuficiente; dependências ausentes não impedem as faixas independentes.

### DASH-03 — renderer e arquivo portátil

`document.render` é a fonte única do HTML HTTP e arquivado. Dados, CSS, JS,
Apache ECharts6.0.0, licença/NOTICE e partials ficam embutidos; nenhuma URL de
recurso é necessária. Fonte da biblioteca: tag oficial6.0.0 do projeto Apache;
manifesto local registra origem, bytes e SHA256. Recursos/templates são lidos
uma vez na compilação e conferidos contra a identidade capturada; mudança de
geração é recusada. O servidor conserva o documento compilado da sessão.

Transporte interno: JSON canônico em Base64 UTF8, com digest separado. O leitor
JS confere os bytes por SHA256 antes de montar faixas. Isso conserva0.0,−0.0 e
expoentes do JSON Python; o download JSON reinsere identidade/integrity sem
reformatar os fatos, e volta a passar pelo validador Python. Hash não é assinatura.
Texto entra por textContent; tooltip usa richText, sem HTML de origem. CSP por
hash de script, sem connect-src nem recursos externos. CSS pode desenhar SVG
do ECharts. O arquivo não usa Flask, fetch, CDN ou relógio atual para validade.

`--html exports/arquivo-novo.html` publica o mesmo documento por
`presets.manual._publish`, com fsync/finalização atômica sem sobrescrever.
Repetição idêntica é idempotente; arquivo diferente/link é preservado. O
renderer de surpresa prevê número/seta, histórico com lacunas, unidade,
baseline, amostra e detalhe/alternativa textual; o caminho real ainda mostra
ausência do produtor. A fixture de surpresa negativa e amostra3 é rotulada
como sintética; não é um fato de mercado nem conclui essa integração.

Testes: HTML HTTP=arquivo byte a byte, decoder/JS real, JSON hostil,
adulteração/schema desconhecido, relógio futuro simulado no transporte,
versões de recursos divergentes, tamanho e preservação do destino. HTTP real
no Browser: fotografia conferida, dados/proveniência e expansão por clique e
teclado. Abertura `file://` foi recusada pela política de URL do Browser;
nenhum outro caminho foi usado para contornar. **Aceite visual/interação do
arquivo offline, surpresa desenhada no navegador e AC-06 permanecem pendentes.**
Teste de recursos/CSP e simulação JS não substituem esse percurso real.

As próximas faixas consomem o envelope pronto, sem repetir leitura/materialização:
DASH-04 (regime) e DASH-05 (volume), conservando as ausências explicitadas.

### DASH-04 — faixa de regime

`sections.regime` adapta `regime.state` e `regime.confidence` já persistidos pelo
produtor canônico; não calcula indicador, média ou frequência. Cada célula
dia × hora UTC mostra o último registro do recorte; a confiança só entra quando
tem a mesma versão/observação e método declarado no contrato
(`parameters.confidence`), com `confidence_is_probability=false`. Último estado
vencido/invalidado oculta a faixa (`NO_DATA`) sem recorrer a registro anterior;
ausência nunca vira zero. `charts/regime.js` desenha o heatmap (célula do corte
com contorno/brilho, janela de 6 h em telas estreitas, teclado e leitura textual).

Testes: `tests/test_dashboard_sections.py` (corte, método ausente, versão
divergente, vencido, ausência, recursos embutidos). Browser HTTP com base QA
sintética: heatmap e célula do corte renderizados, sem erros de console.
Pendências: rótulos das células em 24 colunas ficam pequenos no desktop
(revisar em DASH-12); aceite offline/AC-06 continua pendente. Próxima: DASH-05 (volume).
