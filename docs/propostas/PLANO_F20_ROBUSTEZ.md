# F20 — Plano executável de robustez e reprodução

**T20.0: documentação entregue em 2026-10-17. T20.1–T20.30: PLANEJADAS.**
Uma tarefa por PR; nenhuma implementação ou validação financeira nesta entrega.

[Requisitos Q01–Q16 e arquitetura](ROBUSTEZ_PESQUISA_EXECUCAO.md),
[prompt único executor](PROMPT_CONSOLIDADO_DESENVOLVIMENTO_MT5.md), [roadmap](../ROADMAP.md),
[painel principal](../PLANO_DE_TRABALHO.md) e [validação](VALIDACAO_F19.md).

**Priorização específica da UI (2026-10-17):**
[UI-A01–A10 agora, UI-P01/P02 posteriores e UI-X01 experimental](UI_ORGANIZACAO_GRAFICOS_PRIORIDADES.md).
São requisitos vinculados às fichas existentes, sem aumentar as 30 tarefas. UI atual
permanece em Tkinter/ttk/Matplotlib. Não iniciar experimento/troca de biblioteca como
efeito lateral de T20.22/23; gráficos condicionais acompanham seus serviços reais.

## 1. Como executar e preservar os responsáveis

1. Ler AGENTS.md, suas referências, continuidade e planos F19/F20. Preparar ambiente,
   medir a linha de base e conferir o código integrado antes de alterar.
2. Começar por T19.1 se o inventário estiver pendente; T20.1 estende esse mesmo mapa.
   Não existe dependência de T19.1 em T20.1 nem necessidade de dois inventários.
3. Escolher a primeira ficha elegível pelo marco/capacidade. Dependência documental
   não prova entrega de runtime. Fichas condicionais exigem apenas o módulo usado.
4. Um dono por mudança: a coluna **Integra** identifica o épico/entrega anterior,
   não um segundo trabalho para implementar em paralelo. Quando uma extensão já
   foi entregue ali, consumir/testar a versão e registrar cobertura T20. Se ainda
   tiver um executor designado com o mesmo escopo, registrar esse ID no mapa e
   qualificar seu resultado, sem duplicar PR/motor. Não alterar atribuição silenciosamente.
5. Seguir passos da ficha. Divergência com código interrompe o recorte e é registrada,
   sem solução inventada. Módulos/arquivos novos citados são propostas, não APIs existentes.
6. Entregar a verificação comum V e a específica; atualizar status, continuidade e
   limitações Windows. PRs em repositórios diferentes compartilham o ID e ordem de integração.

M = mt5x; H = HADOUKEN; S = mt5x-safety quando a alteração realmente atingir sua cópia
versionada. Não editar H/S incidentalmente por causa de uma tarefa documental em M.
Não criar subagentes, infraestrutura ou permissões de operação por inferência deste plano.

## 2. Ordem e dependências do percurso

| Marco | Tarefas | Dependência/limite |
|---|---|---|
| M20-A — Referências e dados | T20.1 → T20.2/4 → T20.3/5/6 | Pode avançar sobre o motor atual sem esperar todo o catálogo F19 |
| M20-B — Fidelidade operacional | T20.7 → T20.8/9/11; T20.10 amplia capacidade | Dependências reais de contrato e capacidade; gates de risco preservados |
| M20-C — Pesquisa controlada | T20.12 → T20.13 → T19.27/28; depois T20.14/15 | Registro/critério antes da conclusão da seleção automática; Manual não espera DSR/PBO |
| M20-D — Primeiro ciclo reproduzível | Piloto F19 + T20.16/17/22/23 → T20.30 | Só capacidades simples usadas; não exige Ultra, carteira, fluxo ou todos os módulos |
| M20-E — Acompanhamento e manutenção | T20.18/19/20 e T20.24–29 | Qualificação por ambiente; rede/fill ao vivo pede prova DEMO específica |
| M20-F — Carteira posterior | T20.21 | Segundo momento, com pesquisa síncrona e sem substituir o guardião F |

T20.24 (recursos) pode antecipar lotes pesados após T20.1. T20.26–29 entram quando seus
consumidores precisarem; não condicionar todo estudo local a uma release multiplataforma.
T20.30 aceita o recorte declarado, não marca as outras 29 fichas como concluídas.

## 3. Fichas de implementação

### T20.1 — Mapa único de cobertura, donos e evidência

- **Repo/prioridade:** M, leitura H/S; P0. **Depende de:** T20.0, T19.1.
- **Integra:** todos os Q; inventário F19 e índice A–J.
- **Arquivos:** planos F19/F20, continuidade, roadmap e contratos/documentos dos donos.
- **Passos:** (1) confrontar código, versões e provas com cada Q; (2) registrar dono/base,
  extensão faltante e consumidor; (3) resolver referências de status contraditórias no mapa,
  mantendo histórico e limitando alegações ao ambiente medido.
- **Aceite:** Q01–Q16 com responsável único, lacuna e teste; bases ausentes não presumidas;
  primeiro percurso elegível identificado sem depender do catálogo inteiro.
- **V:** links/IDs e grafo de dependências; somente documentação nesta ficha.

### T20.2 — Casos independentes de referência financeira

- **Repo/prioridade:** M; P0. **Depende de:** T20.1.
- **Integra:** Q01, T6.3/4, T13.9, H/F16 e T19.56/57 conforme capacidade.
- **Arquivos:** `strategies/backtester.py`, `strategies/specs.py`, `trade_analytics/stats.py`,
  sizing/custos existentes e fixtures/testes dedicados de referência.
- **Passos:** (1) escrever contas/valores esperados independentemente; (2) executar os módulos
  atuais contra as referências; (3) corrigir somente divergências do recorte com regressão.
- **Aceite:** unidades/moedas/PnL/custos/risco/arredondamento e trade/ciclo conciliados; PF
  segue o módulo canônico; ausência e erro não viram zero; tolerâncias justificadas.
- **V:** casos manuais revisáveis, limites/negativos, comparação independente; V comum.

### T20.3 — Patrimônio flutuante e drawdown identificado

- **Repo/prioridade:** M; P0. **Depende de:** T20.2.
- **Integra:** Q01, motor atual/T19.24 e métricas H; não cria simulador paralelo.
- **Arquivos:** `strategies/backtester.py`, engine/aggregate do `trade_analytics`, DTOs/métricas.
- **Passos:** (1) definir marcação, custos, câmbio, fluxo externo e frequência; (2) acrescentar
  curva/métrica de patrimônio aberto; (3) distinguir saldo encerrado em armazenamento/saída.
- **Aceite:** perda flutuante seguida de recuperação aparece no risco; parcela não duplica
  PnL; dados sem resolução suficiente informam limite; legado abre e conserva significado.
- **V:** múltiplas posições, gap, parcial, saldo não positivo, fluxo externo e comparação
  com saldo/equity nativos no cenário suportado; V comum.

### T20.4 — Manifestos imutáveis de dados de experimento

- **Repo/prioridade:** M; P0. **Depende de:** T20.1.
- **Integra:** Q02, F1/F5, T13.2/A-04 e cache F16.
- **Arquivos:** banco/exportadores/cache existentes; manifesto proposto no serviço canônico.
- **Passos:** (1) definir conteúdo canônico/revisões e snapshot consistente; (2) registrar
  hashes/referências sem copiar a base inteira por execução; (3) preservar revisão referenciada.
- **Aceite:** correção gera identidade nova; experimento antigo reproduz a revisão ou informa
  indisponibilidade; leitura concorrente é consistente; política de retenção respeita F15.
- **V:** ingestão tardia, correção, concorrência, exportação/importação e referência ausente;
  consulta causal de features testa T13.2 integrado quando usado; V comum.

### T20.5 — Vigência de especificações e transformações de séries

- **Repo/prioridade:** M; P1. **Depende de:** T20.4.
- **Integra:** Q02, A-01/A-02, T6.5, calendário integrado T13.1 quando aplicável.
- **Arquivos:** `strategies/specs.py`, `_symbol_specs`, calendário/exportação e modelo histórico.
- **Passos:** (1) registrar fonte/vigência das propriedades; (2) representar ajustes,
  vencimentos/rolagens e universo disponível sem fabricar eventos; (3) propagar a revisão.
- **Aceite:** propriedade atual não aparece como histórica conhecida; série contínua de
  estudo e contrato executável separados; fonte externa ausente deixa limitação explícita.
- **V:** troca de passo/sessão/contrato, evento conhecido tardiamente, símbolo descontinuado
  em universo de teste e base antiga; V comum. Não exige contratação de dados.

### T20.6 — Gate de qualidade por capacidade e cobertura de ticks

- **Repo/prioridade:** M; P0. **Depende de:** T20.4.
- **Integra:** Q03, T1.8/T13.1/T18.1, I-08 e indicadores de fluxo F19.
- **Arquivos:** qualidade, importadores/relatórios, contratos/registry e serviço de elegibilidade.
- **Passos:** (1) definir requisitos por família de dado; (2) medir cobertura/ausências e
  origem; (3) propagar elegibilidade e motivo para API/UI/experimento.
- **Aceite:** tick volume não passa como volume real; sem book não há fila inferida; modo
  de ticks reais sem comprovação não recebe cobertura integral; falta bloqueia só seu consumidor.
- **V:** lacunas/duplicatas, origem gerada/observada/desconhecida, causalidade multi-TF/ativo
  e relatório MT5 sem campo de qualidade; V comum.

### T20.7 — Matriz verificável de compatibilidade

- **Repo/prioridade:** M; H/S só para exportar capacidade faltante; P0.
  **Depende de:** T20.1, T19.2 e política B-11 integrada.
- **Integra:** Q04, T19.23/29, contrato SDK e F-11.
- **Arquivos:** composição/registry canônicos, specs, contrato/sidecar e catálogo/DTOs.
- **Passos:** (1) definir e importar capacidades contextualizadas; (2) validar combinações;
  (3) associar evidência por versão/ativo/corretora/conta/ambiente.
- **Aceite:** suportado, simulado, validado e incompatível são estados distintos; netting
  não herda prova hedging; contrato 1 e arquivos antigos continuam válidos.
- **V:** restrição de volume/stops/filling e módulo não suportado, round-trip e legado; V comum.

### T20.8 — Margem e exposição conjunta no motor canônico

- **Repo/prioridade:** M, H/S se o contrato de leitura exigir; P0.
  **Depende de:** T20.3, T20.7; interfaces de ordens/risco do módulo usado integradas.
- **Integra:** Q05, I-07, T19.24/56 e F-04/F-05, sem substituir as guardas nativas.
- **Arquivos:** backtester/specs/sizing, backend de leitura e modelo de capital existente.
- **Passos:** (1) definir modelo histórico e limites; (2) considerar capital flutuante,
  reservas e exposição simultânea; (3) rejeitar entradas inelegíveis e explicar a decisão.
- **Aceite:** sem entrada além da margem do modelo; pendentes/netting/hedging sem dupla
  contagem; limitação histórica explícita; `OrderCalcMargin` atual não recalcula o passado.
- **V:** perdas abertas, várias ordens, margem insuficiente, câmbio/custos e confronto
  nativo para cada tipo alegado; V comum e gates multiordem intactos.

### T20.9 — Cenários de custos e preenchimento adverso

- **Repo/prioridade:** M; H apenas por capacidade nativa; P1.
  **Depende de:** T20.6, T20.8.
- **Integra:** Q05, I-07/I-08, modelo de preenchimento I-02 e T19.24.
- **Arquivos:** motor/custos existentes, importadores e relatório de calibração.
- **Passos:** (1) separar parâmetros calibrados de hipóteses; (2) aplicar cenários versionados
  de spread/gap/atraso/rejeição/fill parcial; (3) comparar erro e efeitos no capital.
- **Aceite:** toque não garante fill sem justificativa; fechamento parcial e fill parcial
  distintos; cenário adverso não afrouxa política de risco nem altera resultado baseline.
- **V:** sequências determinísticas de evento e replay nativo onde reproduzível; execução
  de rede/fill ao vivo só recebe prova DEMO específica; V comum.

### T20.10 — Curvas de capacidade por volume

- **Repo/prioridade:** M; P2. **Depende de:** T20.9; fonte de liquidez qualificada se usada.
- **Integra:** Q05, A-09, lotes F19 e matriz/lote existentes.
- **Arquivos:** serviços de experimento/matriz e comparação, sem novo motor.
- **Passos:** (1) declarar volumes/cenários e hipóteses de impacto; (2) reexecutar com custos,
  capital e restrições; (3) comparar risco/resultado líquido e elegibilidade.
- **Aceite:** sem dado de fila/participação suficiente não há capacidade certificada;
  multiplicar linearmente PnL antigo não substitui simulação do volume alterado.
- **V:** passo/mínimo/margem, dado de liquidez ausente e resultado reproduzível; V comum.

### T20.11 — Paridade com primeira divergência por evento

- **Repo/prioridade:** M/H; S se alterar exportação versionada; P0.
  **Depende de:** T20.2, T20.7 e exportação de eventos H/F16 integrada no recorte.
- **Integra:** Q06, I-06, T10.6, T16.2b e T19.29.
- **Arquivos:** `robots/parity.py`, ledger/importadores e exportador H existentes.
- **Passos:** (1) correlacionar intenção/ordem/deal/posição/ciclo; (2) comparar por campos e
  instantes; (3) reportar primeira divergência e classe provável com eventos fonte.
- **Aceite:** diferença de direção/volume/evento não escondida por lucro total próximo;
  eventos fora de ordem/duplicados reconciliados; tolerâncias declaradas antes da comparação.
- **V:** fixtures com divergência plantada, parciais, custo, relógio e regressão dos seis
  pares existentes nos ambientes disponíveis; V comum e evidência nativa do recorte.

### T20.12 — Registro único de experimentos e consultas à reserva

- **Repo/prioridade:** M; P0. **Depende de:** T20.1, T20.4.
- **Integra:** Q07, I-09, T13.4 e T19.27; registro único consumido por essas frentes.
- **Arquivos:** persistência/serviço de experimentos canônicos; migração aditiva e DTOs.
- **Passos:** (1) definir protocolo/tentativa/execução técnica; (2) gravar alternativas,
  falhas/descartes e acessos ao holdout; (3) rastrear decisões e consumo posterior do resultado.
- **Aceite:** alternativa manual conta; cache não apaga tentativa; reexecução idêntica não
  vira amostra nova; reserva usada para ajustar perde status de teste final intocado.
- **V:** busca interrompida, falha, duplicação técnica, acesso à reserva e migração; V comum.

### T20.13 — Auditoria e qualificação do protocolo estatístico

- **Repo/prioridade:** M; P0. **Depende de:** T20.2, T20.12.
- **Integra:** Q01/Q07, I-03/I-04/I-09/I-11/I-13, T13.4 e T19.27.
- **Arquivos:** `strategies/validation.py`, serviço estatístico F16/F18 e bateria quando
  o critério versionado exigir; métodos/protocolo documentados no mesmo PR.
- **Passos:** (1) conferir fórmulas/hipóteses/dependências; (2) preservar ciclos/blocos e
  purgar horizontes sobrepostos quando necessário; (3) medir calibração em ruído/efeito
  conhecido e registrar a versão dos critérios aplicados.
- **Aceite:** sem garantia universal de ordenação dos DDs MC nem zero falsos positivos;
  taxa/intervalo/poder e seed conhecidos; DSR/PBO só quando seus requisitos forem satisfeitos.
- **V:** vazamento plantado, dependência temporal, múltiplas tentativas e protocolos legados;
  mudança de bateria rastreada por I-13, sem desativar testes; V comum.

### T20.14 — Comparação de compromissos entre presets elegíveis

- **Repo/prioridade:** M; P1. **Depende de:** T20.3, T20.13, T19.28.
- **Integra:** Q08, T19.54 quando Ultra for implementado; sem categoria/objetivo PF novo.
- **Arquivos:** seleção/DTOs/relatório do gerador e comparação existentes.
- **Passos:** (1) congelar restrições/objetivos/desempates; (2) calcular dominância e
  alternativas elegíveis; (3) expor compromissos e motivo de seleção.
- **Aceite:** não operar/sem amostra/PF indefinido não ganha por conveniência; risco inclui
  patrimônio e frequência; Ultra compete com baselines, sem superioridade presumida.
- **V:** conjunto pequeno com dominância conhecida, empate, candidato inválido e efeito de
  custo; integração UI pelo dono T19.32/T20.22 conforme mapa; V comum.

### T20.15 — Ablação e contribuição incremental dos componentes

- **Repo/prioridade:** M; P1. **Depende de:** T20.12, T20.13, T19.24 e módulos usados.
- **Integra:** Q09, I-02/I-05, relações F18; infraestrutura futura FRP, sem iniciar FRP.
- **Arquivos:** lote/experimentos e comparações existentes.
- **Passos:** (1) definir baseline e remoções/substituições antes do teste; (2) reexecutar
  sinal/gestão/capital completos; (3) comparar efeitos/incerteza e sensibilidade.
- **Aceite:** risco/custos/protocolo comparáveis; custos e transições incluídos; não editar
  PnL anterior nem concluir causalidade apenas por melhora de uma amostra.
- **V:** componente redundante, útil e danoso conhecidos, interação e orçamento/tentativas;
  V comum. FRP permanece com suas dependências futuras.

### T20.16 — Extensão reproduzível do pacote de estratégia/preset

- **Repo/prioridade:** M; H/S só para versões consumidas; P1.
  **Depende de:** T20.4, T20.7, T20.12, T19.25, T19.50.
- **Integra:** Q10, pacote F19/B-11 e SDK, sem segundo catálogo/compiler.
- **Arquivos:** manifesto/exportação/importação do pacote F19, contratos e testes.
- **Passos:** (1) referenciar fontes/binários/build/framework/dependências/dados/protocolo;
  (2) exportar hashes e requisitos de reprodução; (3) importar/conferir em ambiente isolado.
- **Aceite:** configuração/ID/evidência preservados; dado indistribuível referenciado com
  limitação; sem segredos/caminhos pessoais; corrupção e incompatibilidade não ignoradas.
- **V:** round-trip, binário trocado, artefato ausente, legado e reprodução do recorte; V comum.

### T20.17 — Estados de evidência e invalidação por dependência

- **Repo/prioridade:** M; P1. **Depende de:** T20.11, T20.16.
- **Integra:** Q10, estados de preset F19, bateria e governança T13.8.
- **Arquivos:** grafo de evidências/manifesto/catálogo existentes e DTOs.
- **Passos:** (1) separar dimensões de cálculo/compatibilidade/estatística/replay/DEMO/plataforma;
  (2) ligar provas aos hashes/escopo; (3) invalidar consumidores materialmente afetados.
- **Aceite:** rótulo Manual não invalida configuração sem motivo; alteração de stop/dado não
  herda prova anterior; reprovação fica no histórico; evidência não autoriza envio de ordem.
- **V:** alterações por dimensão, referência removida, ciclo de dependência recusado e legado;
  V comum. Invalidação não altera preset em uso automaticamente.

### T20.18 — Observação sem ordens e comparação prospectiva

- **Repo/prioridade:** M/H; P2. **Depende de:** T20.11, T20.17 e contrato de sinal do módulo.
- **Integra:** Q11, I-12, J-05/J-08 e monitores existentes.
- **Arquivos:** observador/monitor/exportação H e leitura M, sem envio de ordem pelo Python.
- **Passos:** (1) registrar sinais e bloqueios antes do desfecho; (2) identificar simulação
  virtual opcional; (3) comparar decisões congeladas e dados/execuções DEMO observados.
- **Aceite:** modo de observação não envia ordem; PnL virtual não aparece como fill real;
  divergência tem eventos fonte, escopo e limitação.
- **V:** ausência de chamada ao executor em observação, callbacks repetidos e comparação
  conhecida; forward real requer janela futura e não é substituído por replay; V comum.

### T20.19 — Diagnóstico de degradação e alarmes calibrados

- **Repo/prioridade:** M; P2. **Depende de:** T20.13, T20.18.
- **Integra:** Q11, I-12/T13.8, features F13 se utilizadas e notificações existentes.
- **Arquivos:** serviços de monitoramento/governança, alertas e DTOs.
- **Passos:** (1) fixar população, janelas, limites e persistência; (2) separar custo/dados/
  operação/distribuição/variação; (3) emitir diagnóstico rastreável com amostra e incerteza.
- **Aceite:** série estável avalia alarmes falsos; alteração conhecida é detectada dentro
  de limites medidos; drift isolado não vira perda de vantagem nem troca de preset automática.
- **V:** cenários estável/degradado/dado vencido/custo maior, repetição de alerta e amostra
  insuficiente; V comum, com janela prospectiva mantida pendente quando não observada.

### T20.20 — Bateria de falhas, retomada e reconciliação operacional

- **Repo/prioridade:** M/H/S conforme dono; P0 no componente operacional.
  **Depende de:** T20.7, T20.11, persistência/ownership e guardas do módulo integrados.
- **Integra:** Q12, E-10/E-13/F-09–F-12, T17.28 e T19.57 se houver parciais.
- **Arquivos:** sessões/monitor, executor/estado H e cenários de falha dedicados.
- **Passos:** (1) fixar invariantes e operações próprias; (2) injetar quedas/reinício/timeout/
  duplicação; (3) reconciliar antes de retomar ações e registrar resultado.
- **Aceite:** sem duplicar entrada/parcial, desproteger saldo ou tocar terceiros; timeout
  não causa reenvio cego; pausar/cancelar/liquidar são políticas distintas.
- **V:** fakes determinísticos e ensaios nativos suportados; rede/retomada ao vivo em DEMO
  quando necessária; nunca derrubar terminal alheio; V comum.

### T20.21 — Piloto posterior de pesquisa de carteira síncrona

- **Repo/prioridade:** M; P2, segundo momento. **Depende de:** T20.3, T20.8, T19.48.
- **Integra:** Q13, épico F/A-11 e dados/limites integrados; não implementa todo o guardião.
- **Arquivos:** análise de carteira/experimentos e modelo de capital canônicos.
- **Passos:** (1) escolher duas estratégias suportadas e protocolo; (2) executar exposição,
  capital/margem/câmbio conjuntos no tempo; (3) avaliar concentração/pesos/cenários adversos.
- **Aceite:** competição por capital altera operações quando cabível; correlação simples
  não basta como risco agregado; nenhuma alocação usa o futuro ou eleva limites automaticamente.
- **V:** concorrência conhecida, sinais síncronos/assíncronos, concentração e custos; qualquer
  execução nativa respeita guardas F e autorização DEMO; V comum.

### T20.22 — Percurso de pesquisa na UI e estados verificáveis

- **Repo/prioridade:** M; P1. **Depende de:** T20.17, T19.26, T19.30 e serviços reais do recorte;
  T19.32 para seleção automática, quando exibida como funcional.
- **Integra:** Q14, F17/F18 e T19.30/33/34; amplia navegação existente sem novo toolkit.
- **Arquivos:** UI/Laboratório/Robôs e componentes/DTOs existentes.
- **Passos:** (1) ligar hipótese/dados/composição/experimento/preset/prova/acompanhamento;
  (2) apresentar opções progressivas e limitações; (3) preservar contexto/teclado/seleção.
  Aplicar UI-A01–A04/UI-A07–A10: modos Analisar/Compor/Validar e comparar, cabeçalho/contexto
  compactos, composição recolhível, gráfico principal, tabela e inspetor sob demanda;
  rascunho distinto do resultado e resumos legíveis. Mapear telas atuais antes de reorganizar.
  A tela de referência define o padrão deste recorte; ligação temporal do inspetor é T20.23,
  sem dependência inversa para concluir T20.22. Refinamentos P2 e experimento P3 ficam fora.
- **Aceite:** nenhuma tela fabrica estatística; rótulos/unidades/qualidade/pendências visíveis;
  atualizar ou cancelar não publica resultado de seleção antiga; sem seletores TF duplicados.
  Ações existentes alcançáveis, seleção distinta de habilitação de regra, resultado associado
  à configuração efetiva e restauração visual sem trocar preset/parâmetros financeiros.
- **V:** percurso pela aplicação real, base vazia/legada, claro/escuro, matriz visual F19,
  teclado/DPI/fonte e dados iguais ao DTO; V comum.

### T20.23 — Inspetor temporal de decisões e gráficos vinculados

- **Repo/prioridade:** M; P1. **Depende de:** T20.3, T20.11, T20.22.
- **Integra:** Q14, gráficos F17/F19 e rastreio J-08.
- **Arquivos:** gráficos/seleção de operação e DTOs no Laboratório existente.
- **Passos:** (1) alinhar preço/eventos/stops/parcelas/exposição/patrimônio; (2) revelar
  regra/versão/contexto ao selecionar; (3) comparar versões com contexto econômico explícito.
  Aplicar UI-A05/A06/A09 e catálogo visual: seleção bidirecional tabela/gráfico, cursor
  temporal/zoom coordenados quando comparáveis, ampliar/restaurar/exportar e acesso ao
  detalhe por teclado. Candles/eventos e saldo/equity/DD entram conforme dados qualificados;
  sensibilidade, comparação de presets e relações aguardam os serviços próprios.
- **Aceite:** lacunas/zero/ausência preservados; redução gráfica não muda métrica/extremos;
  saldo e patrimônio identificados; sem inferir causa ausente nos registros.
  Seleção exploratória não vira OOS/preset automático; fluxo/qualidade não inventados.
  Medir interação/renderização em cenários comparáveis; usar base atual sem novo renderer
  ou promessa de FPS. Não exigir todos os gráficos do catálogo para o primeiro Manual.
- **V:** instantes conhecidos, zoom/resize, grandes séries, tabela×gráfico e matriz visual;
  prova nativa dos eventos operacionais já exigida no recorte; V comum.

### T20.24 — Orçamento e cancelamento na fila de experimentos

- **Repo/prioridade:** M; P1. **Depende de:** T20.1.
- **Integra:** Q15, T11.5/6, F15 e T19.27, sem segundo agendador.
- **Arquivos:** runner/fila/status/recursos existentes e integração de progresso.
- **Passos:** (1) definir limites de busca/CPU/RAM/disco/slots; (2) pré-checar temporários
  e interromper cooperativamente; (3) guardar checkpoint e estado parcial explícito.
- **Aceite:** limite/cancelamento não publica experimento completo; retomada não repete
  efeito nem apaga falha; UI segue responsiva; não remove temporários de terceiros.
- **V:** disco insuficiente, limite, cancelamento/restart e medição de tempo/memória/I/O;
  configurações novas em DEFAULTS/example/fallback; V comum.

### T20.25 — Cache verificável e isolamento das execuções

- **Repo/prioridade:** M; P1. **Depende de:** T20.4, T20.24.
- **Integra:** Q15, caches F16, sessões T17.28, retenção F15 e registro T20.12 se consumido.
- **Arquivos:** cache/runner/sessões e manifests existentes.
- **Passos:** (1) incluir identidades relevantes na chave; (2) separar terminais/perfis/
  relatórios por execução; (3) liberar só recursos próprios e proteger artefatos referenciados.
- **Aceite:** mudança de código/dados/custo/protocolo não reutiliza resultado indevido;
  jobs não sobrescrevem relatório alheio; não paraleliza terminal sem suporte comprovado.
- **V:** concorrência, colisão, cache obsoleto, retomada e limpeza reversível; V comum.

### T20.26 — Conformidade de módulos do SDK/Composer

- **Repo/prioridade:** M/H; S se necessário; P1.
  **Depende de:** T20.2, T20.7 e interfaces B/SDK integradas para o módulo.
- **Integra:** Q16, B-09/B-11, T19.2/23 e SDK existente.
- **Arquivos:** registry/schema/template/validador do SDK e testes de conformidade.
- **Passos:** (1) exigir tipos/unidades/aquecimento/tempo/estado/capacidades; (2) aplicar
  casos de referência a um módulo; (3) documentar inclusão sem duplicar motor/execução.
- **Aceite:** conformidade verifica comportamento/causalidade, não apenas atributos;
  legado compila e preserva paridade; preset não executa código arbitrário.
- **V:** módulo correto/incompatível/com dado futuro e tutorial reproduzível; V comum.

### T20.27 — Integridade e permissões de artefatos de terceiros

- **Repo/prioridade:** M; H conforme integração; P1. **Depende de:** T20.16.
- **Integra:** Q16, T9.2/3, SDK/bateria e J-09; sem automatizar promoção/execução.
- **Arquivos:** importação/instalação/auditoria do SDK, manifestos e sessões isoladas.
- **Passos:** (1) registrar origem/hashes/licença/includes; (2) identificar necessidades
  de DLL/rede/arquivos; (3) aplicar permissões explícitas e isolamento ao fluxo existente.
- **Aceite:** compilar não marca artefato confiável; importação não executa no terminal
  pessoal; sem credenciais em logs/pacote e sem envio de ordens pelo Python.
- **V:** artefato trocado, dependência ausente, permissão negada, caminhos malformados e
  legado; análise estática não promete detectar todo comportamento malicioso; V comum.

### T20.28 — Restauração coerente do patrimônio de pesquisa

- **Repo/prioridade:** M; P1. **Depende de:** T20.4, T20.16; F15 integrado.
- **Integra:** Q16, backup/restore T15.7/8, sem nova ferramenta concorrente.
- **Arquivos:** recursos/backup/restore existentes, manifestos de pesquisa e relatório.
- **Passos:** (1) definir conjunto consistente e política de segredos; (2) restaurar em
  destino isolado; (3) conferir banco/presets/experimentos/evidências e retomada sem ordens.
- **Aceite:** origem preservada em falha; arquivo criado não basta como prova; vínculos
  e legado abrem; credenciais fora de pacote portável; duração/perda observadas registradas.
- **V:** backup durante atividade, corrupção/falta de parte, migração antiga e recuperação
  completa do pequeno cenário; V comum, sem substituir a base do usuário.

### T20.29 — Qualificação de release e leitores nativos

- **Repo/prioridade:** M; P1 no release. **Depende de:** T20.16, T20.27, T20.28.
- **Integra:** Q16, T0.3/T10.W e instaladores/SDK/importadores existentes.
- **Arquivos:** instaladores/pacote, leitores MT5, CI e checklist Windows, por recorte.
- **Passos:** (1) registrar ambiente/versões reais; (2) conferir instalação repetida e
  importação de formatos/builds/idiomas suportados; (3) documentar matriz e pendências.
- **Aceite:** configuração do usuário preservada; parsing desconhecido falha claramente,
  sem inventar lucro zero; Linux/Wine provado, Windows físico só quando executado.
- **V:** instalação 2× em ambiente de teste, fixtures de relatório, paths.is_windows e
  checklist real quando disponível; ShellCheck se alterar .sh; V comum.

### T20.30 — Aceite do primeiro percurso reproduzível

- **Repo/prioridade:** M, ensaio nativo H; P1.
  **Depende de:** T19.26, T19.29, S09/T19.37, T20.3, T20.6, T20.8, T20.11,
  T20.16, T20.17, T20.22 e layout do recorte validado pela matriz visual F19 §10.3.
- **Integra:** Q01–Q16 como matriz de cobertura, não como conclusão de todos; T19.35/49.
  Aceite Manual antecede o piloto ampliado T19.35, que inclui seleção automática;
  não declarar T19.35 concluída pela execução apenas do Manual.
  Não exige a conclusão da T19.34 ampliada nem seus objetivos automáticos.
- **Arquivos:** relatório de aceite, painel/continuidade e evidências do recorte.
- **Passos:** (1) fixar uma estratégia/ativo/TF e composição simples; (2) percorrer Default,
  Manual nomeado, backtest completo, pacote, importação/UI e replay nativo; (3) reproduzir
  o resultado e reconciliar dados/eventos/métricas com limitações registradas.
- **Aceite:** conta DEMO/Tester corretamente identificados; sem atribuir replay a fills
  ao vivo; mercado fechado permite recorte funcional ágil; nenhum Q pendente fica oculto.
- **V:** V comum, UI real e replay nativo; seleção automática só recebe aceite quando
  T20.12/13 e T19.27/28 integrados; lotes/parciais/Ultra somente se usados e comprovados.

## 4. Verificação comum V e evidências

- Ambiente/linha de base e suíte final conforme AGENTS; testes novos de comportamento
  com fakes, sem depender de MT5 no CI. Doctor e ShellCheck quando aplicável.
- Casos independentes e regressão do legado; não desativar teste nem ampliar tolerância
  para esconder falha. Registrar divergência real e ambiental com resultado/saída completos.
- Persistência aditiva, defaults/fallback, caminhos portáveis, segredos fora de artefatos
  e isolamento do trabalho do usuário. Atualizar checklist Windows sem alegação indevida.
- Medir tempo/memória/I/O/UI nos recortes que alteram performance, com mesmo cenário.
- Aplicar [VALIDACAO_F19.md](VALIDACAO_F19.md): estrutural ≠ financeiro OOS ≠ replay
  funcional ≠ DEMO ao vivo. Ensaios só em DEMO, nunca REAL; guardas e ownership preservados.
- Relatório contém IDs Q/tarefas, SHA/versões/hashes, conjunto de dados/especificação,
  parâmetros/unidades/protocolo/seed, modalidade, eventos, resultado, incerteza e limitações.
  Capturas/logs brutos sem dados pessoais ficam em destino apropriado, não expostos no Git.

## 5. Estado e retomada

Nesta entrega T20.0 é documental, com as dezesseis sugestões aceitas e trinta fichas
de implementação/qualificação. T19.1–57 continuam planejadas; FRP continua futuro.
Primeiro passo é T19.1 quando pendente e T20.1 como extensão de cobertura, sem reiniciar
inventário já comprovado. Não iniciar automaticamente todas as fichas nem um PR monolítico.
O painel principal e a continuidade registram resultados próprios desta documentação,
separados das evidências históricas da implementação do aplicativo.
