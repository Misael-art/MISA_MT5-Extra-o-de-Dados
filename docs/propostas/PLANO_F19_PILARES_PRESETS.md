# F19 — Plano executável: pilares, presets e integração HADOUKEN

**T19.0 documental com adendos. T19.1: inventário em revisão local; T19.2–T19.57 planejadas, sem runtime novo.**
Pedido do proprietário em 2026-10-17. Uma tarefa por PR, sem misturar correções externas.
Não há autorização de operação financeira, alteração de limites ou instalação de presets por este plano.

Contratos: [especificação](PILARES_DADOS_OPERACAO_PRESETS.md).
Cobertura e regras: [catálogo](CATALOGO_F19_INDICADORES_SETUPS.md).
Índices: [roadmap](../ROADMAP.md), [plano principal](../PLANO_DE_TRABALHO.md),
[backlog modular](README.md).
Execução orientada pelo [prompt único](PROMPT_CONSOLIDADO_DESENVOLVIMENTO_MT5.md) e pela
[matriz de validação](VALIDACAO_F19.md), incluindo replay operacional nativo quando adequado.

**Complemento aprovado F20:** [Q01–Q16](ROBUSTEZ_PESQUISA_EXECUCAO.md) e
[T20.1–T20.30](PLANO_F20_ROBUSTEZ.md) qualificam cálculos/dados/execução/pesquisa e o
ciclo operacional. T20.1 estende o mapa T19.1, sem substituí-lo; as 57 fichas abaixo
mantêm os IDs e responsáveis. T20.30 oferece aceite inicial do Manual reproduzível;
T19.35 conserva o piloto ampliado com objetivos automáticos. Não esperar o segundo
para entregar o primeiro nem declarar T19.35 pronta por um ensaio apenas Manual.

## 1. Como executar sem duplicar frentes

1. Ler AGENTS.md e o estado de continuidade. Conferir branch/SHA/árvore e não sobrescrever trabalho alheio.
2. Instalar conforme AGENTS e rodar suíte antes das mudanças; guardar resultados.
3. Começar por T19.1. Selecionar depois a primeira tarefa cujas dependências reais estejam integradas.
4. Conferir contratos/caminhos no código. Se o passo não corresponder ao repositório, parar esse recorte
   e registrar divergência no PR; não inventar substituição para fechar a tarefa.
5. Executar passos na ordem da ficha; implementar somente os IDs/capacidades daquela tarefa.
6. Testar invariantes específicos, suíte completa e doctor; ShellCheck se houver alteração .sh.
7. Atualizar estado/evidências aqui e no painel principal, continuidade e pendências Windows aplicáveis.
8. PR informa o que funciona, o que permanece parcial e os hashes/artefatos da validação.

Os caminhos novos abaixo são **propostos**, relativos à raiz indicada, e não APIs implementadas.
Definir nomes finais na tarefa de contrato, respeitando o código integrado.
Tarefa com mudança em mais de um repositório usa PRs vinculados por **mesmo ID** e ordem de
integração explícita; cada PR altera apenas aquele recorte. Não editar o clone HADOUKEN
como efeito lateral de uma tarefa documental no mt5x.

### Propriedade das dependências existentes

| Entrega necessária | Dono já definido | Papel da F19 |
|---|---|---|
| Sessões/barras derivadas | T13.1 | Consumir o integrado, sem refazer calendário |
| Store causal e registry | T13.2 e T18.1 | T19.3 amplia materialização/consumo e integra I01–I18 |
| Inferência temporal/experimentos | T13.4, I-03/I-09/I-11 | T19.27 preenche apenas capacidades faltantes para composições |
| Relações/candidatos estatísticos | T18.2–T18.7 | Consumidores opcionais do gerador; preset manual não depende de toda F18 |
| Contratos e adaptador de robôs | B-01–B-08/B-11 | Reusar versões integradas; T19.2/23 não implementam contratos paralelos |
| Composer | B-09/B-10 | F19 detalha editor/preset e integração; acordar um só ID dono para cada parte |
| Segurança multiordem | T12.3–T12.5, F-04/F-05/F-10/F-11/F-12, I-13 e política de averaging | Pré-requisitos da liberação operacional de grades; simulação pode anteceder |
| Livro/exportação | T16.1/T16.2/T16.2b e H | Ampliar trajetória e eventos aditivamente |
| Estilo/UI/timeframe | F17 e T17.28 | Reusar e verificar; não duplicar seletores ou trocar toolkit |
| Referências de cálculo e patrimônio flutuante | T20.2/3 sobre motores atuais | T19.24/28/56/57 usam as mesmas fórmulas e distinguem saldo/equity |
| Manifestos, qualidade e compatibilidade contextual | T20.4–7 | T19.3/23/25/29 consomem revisões/capacidades, sem outro store |
| Margem/custos e paridade por evento | I-07/I-06 detalhados em T20.8/9/11 | Qualificar a execução daquele módulo, sem outro executor |
| Registro e protocolo de pesquisa | T20.12/13 integrados a I/T13.4 | T19.27/28/52 usam registro único antes de concluir seleção automática |
| Reprodução e estados de evidência | T20.16/17 sobre pacote T19.25/50 | Versionar/invalidar provas sem segundo catálogo |

T19.1 publica o mapa de qual tarefa anterior entrega cada contrato e do que falta.
Dependência pendente bloqueia apenas o consumidor correspondente: não bloquear toda pesquisa
Python por MQL5 nem exigir todos os indicadores para gerar um preset simples.
Planejar trilhas independentes não solicita criação de subagentes.

## 2. Marcos e ordem

| Marco | Entrega verificável | Caminho |
|---|---|---|
| M19-A | Contratos, inventário e extensão causal definidos | T19.1 → T19.2/T19.3, respeitando T13.2/T18.1 |
| M19-B | Default primeiro e preset manual nomeado executável/reconciliado | Núcleo simples T19.17–21 → T19.24/25/50/55/26; T19.23/29 e S09/T19.37 |
| M19-C | Tempo de trade analisável e executável | T19.14 → T19.15 → T19.16; UI T19.31 e teste completo |
| M19-L | Gestão de lotes e realização parcial no pilar operacional | T19.56 → T19.57; reuso de dimensionamento/parcial simples e extensão comprovada |
| M19-D | Geração defensiva/PF e UI operacional utilizáveis | T19.27/28 → T19.32; T19.30/33/34 → T19.35 |
| M19-U | Ultra condicional, nomes padronizados e inversão experimental | T19.50/51 → T19.52 → T19.53 → T19.54; preset simples não espera Ultra |
| M19-E | Catálogo completo implementado onde especificado e dados disponíveis | T19.4–13 e T19.36–47, com cada lacuna remanescente visível |
| M19-F | Robustecimento posterior da conta | T19.48, respeitando o épico F existente |
| M19-G | Aceite integrado e reconciliação documental | T19.49; não declara itens impedidos como concluídos |

Começar pelo ciclo vertical **S09 / cruzamento de médias → entrada simples → alvo/stop
suportados → filtros simples → preset manual → Tester**, sem esperar Renko ou fluxo de ordens.
O Default é a referência inicial; criar o Manual por cópia nomeada, preservando o original.
Depois entregar tempo e os objetivos automáticos sobre capacidades comprovadas.
Nomes padronizados (T19.50) entram no primeiro gerador; inversão (T19.51) é uma capacidade
experimental opcional. Ultra usa T19.52–54 após a infraestrutura de validação/composição.
Completar o catálogo nas trilhas de dados e estratégias; grades mantêm gates próprios.
Não há datas estimadas: dependências e evidência determinam a ordem.

## 3. Fichas de implementação

Nas fichas, **V** significa verificação comum da seção 4 mais os testes listados.
**M** = mt5x; **H** = HADOUKEN; **S** = biblioteca mt5x-safety, somente quando aplicável.
Todos os IDs abaixo começam em PLANEJADO. Campos “IDs” remetem ao catálogo integral.

### T19.1 — Inventário reconciliado e decisões de contrato

**Estado (2026-10-17): em revisão local.** [Entrega, evidências e decisões](INVENTARIO_F19.md).
Conferidos HEAD/branches H/M/S, seis contratos/inputs/presets e exportações,
11 fontes Safety idênticas, catálogo e lacunas. Sem alteração de runtime.
T20.1 estende este mapa após integração, sem novo inventário; publicação da
base documental T19.0/T20.0 ainda pendente. Ver resultados na continuidade.

- **Repo/prioridade:** M, leitura H/S; P0. **Depende de:** T19.0.
- **Arquivos:** documentos F19, plano principal, contratos SDK e backlog B/D/E/F/I.
- **Passos:** (1) conferir SHA e módulos/presets/exportações reais; (2) mapear capacidades atuais e
  responsáveis anteriores; (3) registrar decisões de schemas, propriedade e migração e lacunas das fontes.
- **Aceite:** nenhuma capacidade presumida; T17.28 reconhecida; caminhos ausentes registrados;
  um responsável por contrato/store/Composer; contrato 1 preservado.
- **V:** revisão cruzada de paths, nomes de inputs, schemas e links; sem alterar runtime.

### T19.2 — Composição canônica e capacidades tipadas

**Estado (2026-10-19): implementação em revisão, dependente da política B-11.**
[Schema/capacidades e adaptador base](../sdk/composicao.md) funcionam em Python sobre
as estratégias existentes; rascunhos sem adaptador têm motivo. Pullback fica indisponível
no novo caminho até T19.57 (confirmação de parcial/BE). SDK1 e caminhos legados preservados.
CI final e integração pendentes; não conclui preset Default/Manual nem execução modular nativa.

- **Repo/prioridade:** M; P0. **Depende de:** T19.1, política B-11 e definição correspondente B-09/B-10.
- **Arquivos:** propostas `composition/schema.py`, `composition/registry.py`; integração `robots/contract.py`;
  schema separado de capacidades ou migração formal, nunca campo solto no contrato 1.
- **Passos:** (1) definir entrada/orientação/ganho/stop/filtros/lotes/parciais e unidades; (2) validação tipada
  de compatibilidade e precedência; (3) serialização canônica/hash e adaptador do modo base.
- **Aceite:** round-trip sem perda; defaults desligados reproduzem legado; inválido/sem suporte
  tem motivo; nomes dos inputs são declarados, sem heurística para gerar preset.
- **V:** contrato antigo, enum/bool/número, combinação inválida, hash estável e versão desconhecida.

### T19.3 — Materialização, exportação e consumo de indicadores

- **Repo/prioridade:** M; P1. **Depende de:** T19.1, T13.2 e T18.1 integradas.
- **Arquivos:** registry/store canônicos F13/F18, extração/exportação e adaptadores de `trade_analytics`.
- **Passos:** (1) inventariar fórmulas já chamadas no fluxo; (2) registrar variantes/ER e dependências;
  (3) materializar incrementalmente; (4) expor colunas/consulta de contexto ao Laboratório e exports.
- **Aceite:** versão/parâmetros/available_at e estados preservados; cache invalida por fonte/fórmula;
  sem store duplicado ou alteração destrutiva de barras.
- **V:** treino prefixado, lote × incremental, reinício, barras superiores abertas, banco antigo e exportação.

### T19.4 — Alligator (I06)

- **Repo/prioridade:** M, par H quando consumidor operacional; P1. **Depende de:** T19.3.
- **Arquivos:** indicadores puros/registry e testes; módulo matemático H correspondente.
- **Passos:** fixar SMMA/semente/preço/períodos/offsets → calcular três linhas/transformações → publicar.
- **Aceite:** deslocamento visual não antecipa disponibilidade; aquecimento e variante identificados.
- **V:** janela constante/tendência, offsets, comparação incremental e paridade quando exposto ao H.

### T19.5 — AO e AC (I07/I09)

**Estado (2026-10-19): aceite final concluído na PR128/11b6c53, após129/masterd0b9c5e.**
88 focados na composição corrigida após129/masterd0b9c5e; doctor21/1/0 e prova CLI/store/CSV racional.
Seis checks aprovados:1438 Ubuntu/43PG/1540Windows CI; assinatura04f94b5b e293 fontes/testes preservados.
CI e revisão final concluídos; Windows físico delimitado. [Contrato e evidências](../analytics/T19.5_AO_AC.md).

- **Repo/prioridade:** M/H; P1. **Depende de:** T19.3.
- **Arquivos:** indicadores puros, registry e matemática H.
- **Passos:** AO 5/34 no mediano → AC por suavização definida → transformações e exportação.
- **Aceite:** fórmulas encadeadas compartilhadas; cor/inclinação não altera série numérica.
- **V:** cálculo manual, aquecimento combinado, zero, lote/incremental e paridade.

### T19.6 — Pivôs causais: Fractals e ZigZag (I08/I14)

- **Estado:** concluída na PR131 sobre master após #130; seis checks6a9d580 aprovados; [contratos e evidências](../analytics/T19.6_PIVOS_CAUSAIS.md).

- **Repo/prioridade:** M/H; P1. **Depende de:** T19.3.
- **Arquivos:** indicadores/registry, persistência de confirmação e revisões.
- **Passos:** definir empate/confirmação → separar retrospectivo/provisório/confirmado → integrar consumidores.
- **Aceite:** tempo central distinto da confirmação; decisões passadas não mudam com repaint.
- **V:** série armadilha, empate, extensão futura, reinício incremental e pivô ainda não confirmado.

### T19.7 — Donchian, Envelopes e regressão (I10/I11/I12)

- **Repo/prioridade:** M/H; P1. **Depende de:** T19.3.
- **Arquivos:** `strategies/indicators.py`, registry, canais e consumidores.
- **Passos:** reutilizar Donchian/slope/R² → completar envelopes/canais → publicar posição/distância.
- **Aceite:** variantes de barra/referência e desvio explícitas; não duplicar Donchian existente.
- **V:** canal anterior, janela constante, regressão conhecida, unidade %, exportação e paridade.

### T19.8 — McGinley Dynamic (I13)

- **Repo/prioridade:** M/H; P1. **Depende de:** T19.3.
- **Arquivos:** indicadores puros, registry e par matemático.
- **Passos:** registrar variante/semente/limites → cálculo recursivo → estado incremental/publicação.
- **Aceite:** resultado finito quando definido; zero/denominador inválido com política explícita.
- **V:** recorrência manual, reset, gap/NaN, preços extremos e paridade.

### T19.9 — Ultimate Oscillator (I15)

- **Repo/prioridade:** M/H; P1. **Depende de:** T19.3.
- **Arquivos:** indicadores puros/registry e matemática H.
- **Passos:** definir pressão/TR → razões em três janelas → pesos/sinalizações/exportação.
- **Aceite:** escala e períodos versionados; ausência não vira zero.
- **V:** fechamento anterior, soma conhecida, denominador zero e paridade.

### T19.10 — TRIX e KST (I16/I17)

- **Repo/prioridade:** M/H; P1. **Depende de:** T19.3.
- **Arquivos:** indicadores puros, registry e pares H.
- **Passos:** definir suavizações/ROC e sementes → cálculo e sinal → publicação.
- **Aceite:** aquecimento pelo maior encadeamento; escala igual em todos os consumidores.
- **V:** constante, tendência, múltiplos períodos, extensão futura e paridade.

### T19.11 — Agressão e CVD (I01/I02)

- **Repo/prioridade:** M/H; P1 condicionado à fonte. **Depende de:** T19.3, captura T5.3 qualificada.
- **Arquivos:** ingestão de negócios/ticks, registry e agregação por sessão.
- **Passos:** auditar lado/volume → classificar desconhecido → delta → CVD/reset e publicação.
- **Aceite:** sem dado real suficiente, estado indisponível com motivo; proxy não rotulado como agressão real.
- **V:** reconciliação de volumes, fonte sem agressor, sessão noturna, lacuna e reinício.

### T19.12 — Imbalance de agressão e book (I03)

- **Repo/prioridade:** M/H; P1 condicionado à fonte. **Depende de:** T19.11 para footprint;
  T19.3/T5.4 qualificada para book.
- **Arquivos:** ingestão de book/negócios, registry e agregadores por preço.
- **Passos:** separar variantes → definir razão/lado/diagonal/profundidade → calcular e publicar.
- **Aceite:** book e footprint distintos; “300% mais” não convertido implicitamente em razão 3.
- **V:** denominador zero, níveis faltantes, snapshot antigo, cancelamentos e timestamp de disponibilidade.

### T19.13 — Velocidade da fita e grandes negócios (I04/I05)

- **Repo/prioridade:** M/H; P1 condicionado à fonte. **Depende de:** T19.3, negócios qualificados.
- **Arquivos:** agregadores por evento/tempo, registry e detecção de limiares.
- **Passos:** normalizar timestamps/unidades → janelas/quantis só passados → eventos e exportação.
- **Aceite:** sem inferência de identidade institucional; não simular negócios a partir de candle.
- **V:** rajada, eventos simultâneos, quantil sem futuro, unidades e lacunas.

### T19.14 — Trajetória temporal de trades e ciclos (TM01/02/04/08)

- **Repo/prioridade:** M, exportação H se necessária; P1. **Depende de:** T19.1, T16.1/2/3;
  histórico completo quando aplicável.
- **Arquivos:** `trade_analytics/ledger.py`, `enrich.py`, propostas de trajetórias e stats;
  exportador de eventos nativos existente.
- **Passos:** definir identidade/unidades/relógios → reconstruir trajetória disponível → tempos/extremos/
  limiares/censura → persistir e consultar sem alterar ledger bruto.
- **Aceite:** origem demo/real/Python/Tester separada; n em risco e incerteza; entrada ausente impede métricas.
- **V:** parcial, posição aberta, gaps, sessão, ganho antes da perda, custos/moeda e extrema temporal conhecida.

### T19.15 — Stop temporal, time de gain e time de loss (TM02/03)

- **Repo/prioridade:** M/H; P1. **Depende de:** T19.2/14; adaptador B necessário no H.
- **Arquivos:** `strategies/base.py`, backtester, `execution/engine.py`, política H e mapeamento de inputs.
- **Passos:** registrar semântica dos relógios/limiares → avaliação causal → motivo de saída/estado → mapear preset.
- **Aceite:** preservar max_bars legado; regra de lucro mínimo, prazos gain/loss/empate e stop protetor coexistem.
- **V:** N−1/N/N+1, igualdade do limiar, buy/sell, custos, reinício, empate com stop/alvo e paridade.

### T19.16 — Tempo com ER, Donchian e ATR (TM05/06/07)

- **Repo/prioridade:** M/H; P1. **Depende de:** T19.15, T19.3/7.
- **Arquivos:** políticas temporais e transformações de contexto; matemática/política H.
- **Passos:** ER/tempo → canal e idade sem extremo → prazo ATR limitado e instante de atualização.
- **Variantes do estudo:** especificar relógios volumétrico/por amplitude com unidade/fonte/reset;
  implementação após qualificar dados e contagem, sem conversão implícita em minutos.
- **Aceite:** sem MAE/MFE futuros; reset por adição é explícito; prazo não cresce indefinidamente em perda.
- **V:** ER zero/NaN, ATR dobra/zero, retorno ao canal, ciclo com adições e disponibilidade no H.

### T19.17 — Módulos de entrada simples e pendente

- **Repo/prioridade:** M/H; P1. **Depende de:** T19.2; B-04/B-08 e executor D-02 no H.
- **Arquivos:** backtester/execução, módulo de entrada composto; adaptadores H.
- **Passos:** mercado → nível fixo/limite/stop/reteste → validade/cancelamento e resultado de preenchimento.
  Referências de entrada Fibonacci reutilizam níveis causais T19.6/18, quando suportados.
- **Aceite:** sinal separado da execução; preço conhecido antes da ordem; modelo conservador declarado.
- **V:** toque sem preenchimento, gap, expiração, rejeição/parcial e modo legado sem regressão.

### T19.18 — Catálogo de ganho e Fibonacci (AX06)

- **Repo/prioridade:** M/H; P1. **Depende de:** T19.2; T19.6 para Fibonacci por pivô;
  indicador correspondente para cada variante.
- **Arquivos:** políticas de ganho, mapeamento/preset e implementação H.
- **Passos:** fixos pontos/%/R → ATR/média/VWAP/canal → parcial → Fibonacci causal.
- **Escopo:** preservar parcial simples existente; agenda geral de múltiplas reduções,
  base percentual/saldo e integração com grid são extensão T19.57, sem outro motor paralelo.
- **Aceite:** alvo com fonte/âncora/momento definidos; saída de ganho não mistura stop; sem preço futuro.
- **V:** retração/extensão, direção, precisão/tick, parcial mínima, alvo móvel e conflito de saídas.

### T19.19 — Stops e proteção por indicador

- **Repo/prioridade:** M/H/S se afetar executor; P1. **Depende de:** T19.2, indicadores correspondentes.
- **Arquivos:** políticas de posição, motor existente e Safety quando necessário.
- **Passos:** stop fixo/ATR → trailing fixo/ATR/barras/média → break-even/trava de avanço → integração.
- **Aceite:** stop inicial obrigatório, monotonia e prioridade; High-Low só após AX05/T19.40.
- **V:** long/short, recuo do indicador, gap, stops mínimos/freeze/volume e paridade legada.

### T19.20 — Horários, dias e frequência por sessão

- **Repo/prioridade:** M/H; P1. **Depende de:** T19.2, calendário T13.1 e contadores de eventos necessários.
- **Arquivos:** `trade_analytics/rules.py`, filtros de composição e estado de sessão H.
- **Passos:** intervalos permitidos/excluídos → dias → 1/3/N ciclos → cooldown/fechamento separado.
- **Aceite:** fuso/sessão explícitos; conta preenchimento de ciclo; persistência/reconciliação do contador.
- **V:** meia-noite/DST quando suportado, rejeição, cancelamento, parcial, gradiente, reversão e reinício.

### T19.21 — Filtros genéricos de indicador e qualidade

- **Repo/prioridade:** M/H; P1. **Depende de:** T19.2/3, indicador/fonte usado.
- **Arquivos:** registry tipado, filtros, regras/insights e adaptador H.
- **Passos:** operadores por tipo → grupos AND/OR limitados → política sem dado → consulta causal.
- **Aceite:** todo Ixx pode ser referenciado quando disponível; efeito só sobre novas entradas;
  não executar expressão arbitrária nem usar surpresa econômica antes da publicação.
- **V:** ausente/vencido/NaN, operador inválido, TF superior aberto, agrupamentos e bloqueio auditável.

### T19.22 — Gradiente fixo/dinâmico/direcional em composição

- **Repo/prioridade:** M; P1 pesquisa, operação condicionada. **Depende de:** T19.2/17/19/20.
- **Arquivos:** `execution/engine.py`, `modes.py`, sizing e simulação completa.
- **Passos:** traduzir modos atuais → direção/spacing independentes → stop variável/filtros →
  ciclo/exposição/custos e recuperação simulada.
- **Aceite:** mesma configuração reproduz legado; não confundir fator constante com progressão;
  rótulo de estudo permanece enquanto H/Safety não suportarem.
- **V:** todos os níveis contra antes do stop, adição a favor, margem/tetos, ciclo e cancelamento de pendentes.

### T19.23 — Adaptador HADOUKEN e capacidades executáveis

- **Repo/prioridade:** H/M, distribuição S se necessária; P0 para uso nativo.
  **Depende de:** T19.2, B-02–B-08/B-11 integrados, capacidades individuais T19.15–22.
- **Arquivos:** adaptador do `HDK_StrategyRobot.mqh`, contratos e registro/mapper mt5x.
- **Passos:** consumir contratos oficiais → declarar módulos/inputs efetivos → resolver defaults/perfis →
  auditar ida/volta e versão → manter adaptador dos seis robôs.
- **Aceite:** não criar ordem fora do executor; sem promessa de módulo ainda ausente; grade
  operacional só com T12.3/4/5, política de averaging e avaliação específica aplicáveis.
- **V:** compilação sem erro/aviso, paridade dos seis, versão incompatível e netting/hedging conforme suporte.

### T19.24 — Backtest completo de composições e lote

- **Repo/prioridade:** M; P1. **Depende de:** T19.2/17 e políticas/filtros usados.
- **Arquivos:** `strategies/backtester.py`, `robots/backtest.py`, lote/matriz e importadores.
- **Passos:** ligar sinal→filtro→ordem→posição→capital em sequência → gravar composição/run →
  comparação com baseline/re-simulação → jobs canceláveis.
- **Aceite:** não filtrar ledger e chamar de novo backtest; riscos/cotas/sinais seguintes recalculados.
  Novo motor reutiliza custos/metrics/ledger existentes.
- **V:** saída antecipada libera sinal seguinte, limite diário elimina entrada, capital altera lote,
  sobreposição/margem, cancelamento e paridade antiga.

### T19.25 — Pacote de preset e compilador de parâmetros

- **Repo/prioridade:** M; P0. **Depende de:** T19.2 e esquema de capacidades acordado.
- **Arquivos:** propostas `presets/schema.py`, `presets/compiler.py`; reuso `robots/mql.py`/contract/catalog.
- **Passos:** composição canônica → mapeamento tipado de papéis/inputs por robô/versão →
  manifesto + .set + contexto de teste → leitura de volta e hash.
- **Aceite:** regra sem input/suporte impede .set; contrato 1 e presets antigos continuam válidos;
  perfis internos não sobrescrevem valores silenciosamente.
  Categoria, orientação e política Ultra têm versão/hash e suporte declarado, sem novos campos
  informais no contrato 1; convenção de nomes é entregue pela T19.50.
  Lotes/exposição e agenda de parciais entram com base/unidade/etapas; mapeamento só é
  executável para capacidades comprovadas T19.56/57, sem omitir etapas não suportadas.
- **V:** enum/bool/precisão/unidade, UTF-16 legado, defaults/perfis, TF interno e input desconhecido.

### T19.26 — Preset manual, rascunho e importação

- **Repo/prioridade:** M; P1. **Depende de:** T19.25/50/55.
- **Arquivos:** serviço `presets`, CLI proposta a definir sem colisão e catálogo.
- **Passos:** Default/Salvar como Manual → pedir Nome do preset e prévia canônica →
  salvar/reabrir composição manual → validar capacidades → exportar pacote →
  importar com identidade/estado → versionar edição.
- **Aceite:** rascunho não validado é útil e claramente rotulado; sem sobrescrever .set do usuário;
  escrita atômica; importação não instala/inicia Expert.
  Nome informado, slug, ID e vínculo ao Default de origem preservados em catálogo/manifesto/importação.
- **V:** ida/volta, regravação idempotente, arquivo malformado, caminhos Win/Linux e edição invalida evidência.
  Nome vazio/duplicado, acentos e renomeação de apresentação sem mudar parâmetros/evidência.

### T19.27 — Protocolo de busca, tentativas e validação temporal

- **Repo/prioridade:** M; P1. **Depende de:** T19.24/25, T20.12/13 e capacidades verificadas T13.4/I.
- **Arquivos:** validação existente e propostas `presets/experiments.py`.
- **Passos:** registrar orçamento/grade/objetivo → cronologia/folds e maturidade → executar
  tentativas com checkpoint → seleção no treino → reserva congelada.
- **Aceite:** falhas/descartes/consultas à reserva registrados; nenhum OOS entra na seleção;
  busca tem limites de tempo/memória/disco/tentativas via F15.
  Consumir registro único e versão de protocolo T20.12/13; reserva consultada para ajuste
  perde status de teste final intocado. Não duplicar `experiments` em F19 e F20.
- **V:** ruído, vazamento deliberado, trades sobrepostos, retomada reproduzível, semente e reserva reutilizada.

### T19.28 — Seletores Defensivo e Maximalista (Fator de lucro)

- **Repo/prioridade:** M; P1. **Depende de:** T19.27/14, T20.3 e equity/custos qualificados.
- **Arquivos:** proposta `presets/objectives.py`, reuso métricas/PF T13.9.
- **Passos:** registrar gates/limiares/desempates → filtrar elegíveis → ranquear por objetivo →
  justificar seleção/rejeição → congelar para conferência final.
- **Aceite:** defensivo considera equity/caudas/exposição; PF finito após custos; sem candidato
  é resultado válido; zero trades, amostra fraca e PF indefinido não ganham.
  Risco patrimonial/frequência/limitação não se confundem com drawdown de saldo encerrado.
  Comparação adicional T20.14 conserva o objetivo de cada categoria, sem score PF oculto.
- **V:** campeão aparente sem perdas, mínimo de atividade, empate estável, moedas/risco diferentes,
  ausência de equity intratrade e bom treino com validação negativa.

### T19.29 — Conferência nativa do preset e reconciliação

- **Repo/prioridade:** M/H; P1. **Depende de:** T19.23/24/26 e setup/políticas suportados.
- **Arquivos:** sessões/tester/parity/importadores existentes e serviço de presets.
- **Passos:** gerar revisão técnica do pacote → teste nativo isolado → conferir inputs/log →
  conciliar sinais/ordens/deals/ciclos/custos → anexar relatório e estado.
- **Aceite:** nenhuma substituição de arquivos originais; Model/qualidade efetiva registrados;
  cancelamento não aprova; divergência tem motivo, não tolerância ampliada sem justificativa.
- **V:** fakes determinísticos, nativo no host quando disponível, sessão interrompida,
  input ignorado e erro na última posição/fechamento.

### T19.30 — Editor operacional organizado no Laboratório

- **Repo/prioridade:** M; P1. **Depende de:** T19.2 e DTO/serviço das capacidades exibidas;
  componentes F17 e T17.25/27.
- **Arquivos:** `ui/laboratory.py`, novos componentes seletivos, sem lógica financeira no widget.
- **Passos:** grupos entrada/ganho/stop/filtros → parâmetros condicionais → resumo →
  ações de comparação/backtest e persistência de seleção.
  Grupo Lotes e parciais: modo fixo/risco, volume efetivo, etapas/parcelas e saldo; habilitar
  capacidades efetivamente entregues T19.56/57, mantendo o legado e explicando inviabilidade.
  Flag de inversão perto da entrada, desligada por padrão; habilitar só com T19.51 suportada.
- **Aceite:** opções só aparecem como executáveis se suportadas; combinação incoerente explica erro;
  cálculo assíncrono e proteção contra resultado de seleção antiga.
- **V:** edição→serialização→reabertura sem perda; teclado, inválidos, ausência e refresh.

### T19.31 — Análise temporal e gráficos (TM08)

- **Repo/prioridade:** M; P1. **Depende de:** T19.14/15 e DTO real.
- **Arquivos:** `ui/laboratory.py`, `ui/charts.py`, projeções de stats.
- **Passos:** duração × resultado → MFE/MAE × tempo → quantis/condicionais →
  comparação de regras temporais com fronteiras de validação.
- **Aceite:** n em risco, censura, custo e unidade visíveis; hover/zoom/export;
  não transformar somente vencedores em chance de sucesso dos sobreviventes.
- **V:** trajetórias curtas/abertas/ausentes, ganho/perda/zero e igualdade entre gráfico/export/DTO.

### T19.32 — Gerador na UI e comparação de candidatos

- **Repo/prioridade:** M; P1. **Depende de:** T19.26/28/29/30.
- **Arquivos:** UI Laboratório/presets, serviço de experimentos e catálogo.
- **Passos:** Default primeiro; quatro categorias Manual/Defensivo/Maximalista(Fator de lucro)/Ultra → protocolo/gates antes da busca → progresso/cancelamento →
  comparação/evidência → salvar pacote e disponibilizar no catálogo.
  Nome canônico vem de T19.50; Ultra fica explicitamente indisponível até seu serviço/capacidade
  de T19.53/54, sem bloquear a entrega dos três perfis estáticos.
- **Aceite:** “compatível”, “validado” e “autorizado” separados; sem vencedor explicado;
  não inicia operação ao gerar preset.
  Reusar seleção padrão existente; nome do Manual obrigatório com prévia e Default copiado,
  não sobrescrito; alias Maximalista aponta para FatorLucro, sem categoria duplicada.
- **V:** rascunho, resultado insuficiente, busca sem elegível, cancelamento, edição e recuperação.

### T19.33 — Timeframe coerente em Robôs, Play, Replay e Backtest

- **Repo/prioridade:** M; P1. **Depende de:** T17.28 existente; T19.25 para presets novos.
- **Arquivos:** `ui/robot_controls.py`, `robot_detail.py`, `robots/controls.py`/sessions.
- **Passos:** testar seletor atual “Ativo / TF” → conferir contexto em cada ação/detalhe/relatório →
  corrigir apenas lacuna demonstrada → deixar seleção/fonte compreensíveis.
- **Aceite:** não criar segundo seletor conflitante; troca de TF invalida evidência/consentimento
  correspondente; múltiplos TFs/Renko têm semântica distinta; seleção sobrevive ao refresh.
- **V:** H1→M15 sem métricas antigas, preset com TF interno, replay/backtest recebem TF correto;
  regressões `test_robot_controls.py`/`test_ui_robot_controls.py`.

### T19.34 — Responsividade, acessibilidade e gráficos integrados

- **Repo/prioridade:** M; P1. **Depende de:** T19.30/31/32/33.
- **Arquivos:** componentes/charts/tokens e telas tocadas, capturas em pasta da tarefa.
- **Passos:** aplicar matriz da especificação → medir bounds/foco/altura →
  operar fluxos reais disponíveis → corrigir recorte visual sem alterar cálculos.
- **Aceite:** controles alcançáveis, reflow/rolagem, dois temas, fonte ampliada, DPI e sem ciclos de geometria;
  legenda/n/ausência/custos preservados.
- **V:** 1920/1280/900/820, incluindo altura 620 no perfil 900, DPI96/144, teclado, textos longos, troca de aba/tema,
  resize repetido, exportação e checklist Windows.

### T19.35 — Piloto integrado mt5x → preset → HADOUKEN

- **Repo/prioridade:** M/H; P1. **Depende de:** T19.29/28/34 e S09/T19.37.
- **Arquivos:** testes de integração, evidência e documentação do piloto.
- **Passos:** congelar dados/composição → percurso manual → objetivo defensivo/PF →
  carregar pacote no Tester → conciliar e demonstrar leitura no catálogo.
- **Aceite:** mesmo hash/valores efetivos e TF em todo percurso; ausência de candidato automático
  é aceitável se corretamente demonstrada; não alegar aprovação financeira sem evidência.
- **V:** todos os fluxos com dados sintéticos identificados, histórico real quando disponível
  com limitações; nenhum teste precisa de conta real.

### T19.36 — Setup VWAP Bands Scalper (S01/AX01)

- **Repo/prioridade:** M/H; P1. **Depende de:** T19.3/17/18/19/21/23 e fonte de volume/sessão qualificada.
- **Arquivos:** módulo de sinal/indicador e Expert/adaptador H, manifesto/preset.
- **Passos:** fixar bandas percentuais versus desvio → calcular VWAP/limites → sinal/ordens →
  preset e backtest completo.
- **Aceite:** fonte real/proxy explícita; âncora de sessão; stops em unidade do ativo; sem grid implícito.
- **V:** volume zero, reinício de sessão, aproximação, limites/gap e protocolo comum de setup (§4).

### T19.37 — Referência de médias e template (S09)

- **Repo/prioridade:** M/H; P1, primeiro setup do piloto. **Depende de:** T19.2/17/18/19/20/23.
- **Arquivos:** reuso `ema_atr`/declarada, template/Expert H e presets de referência.
- **Passos:** verificar equivalência com existente → declarar variante mínima →
  compor entradas/saídas/filtros suportados → documentar preset/TF e teste.
- **Aceite:** não criar framework duplicado; variantes em configuração; preservação dos seis robôs.
- **V:** protocolo comum, preset manual conhecido e paridade entre default antigo e composição equivalente.

### T19.38 — Estocástico lento + EMA9 (S10/AX04)

- **Repo/prioridade:** M/H; P1. **Depende de:** T19.3/19/21/23.
- **Arquivos:** indicadores reutilizados, sinal e Expert/preset H.
- **Passos:** resolver K/D/14/3 → cruzamento/limiares → trailing EMA com Move Guard → preset.
- **Aceite:** regra objetiva, igualdade/reentrada definidas e stop nunca afasta.
- **V:** protocolo comum, série no limite 25/75, média recua, gap e ambos os lados.

### T19.39 — Madrid e ciclo (S05/AX02)

- **Repo/prioridade:** M/H; P1 após fechar regras. **Depende de:** T19.3/18/19/23 e períodos definidos.
- **Arquivos:** sinal leque, estado de ciclo e políticas de média.
- **Passos:** registrar dez períodos/gatilho/reset → alinhamento/afastamento →
  alvo/stop móvel e flags → preset.
- **Aceite:** período ausente impede implementação fiel; cruzamento e anti-spam causais.
- **V:** protocolo comum, igualdade de médias, reset/reinício e flags em sinais consecutivos.

### T19.40 — Heiken-Ashi + High-Low (S11/AX05)

- **Repo/prioridade:** M/H; P1. **Depende de:** T19.3/19/20/23; reversão definida no contrato.
- **Arquivos:** indicadores HA/Activator, módulo de sinal e executor/adaptador de reversão.
- **Passos:** definir fórmulas/seed → condições/neutro → fechar e reentrar com risco/filtros → preset.
- **Aceite:** preços HA não viram fill; proteção e filtros precedem “sempre posicionado”.
- **V:** protocolo comum, duas etapas/rejeição/volume residual, conta netting/hedging conforme suporte.

### T19.41 — Infraestrutura Renko por ticks (S02/AX07)

- **Repo/prioridade:** M/H; P1 condicionado à fonte. **Depende de:** T19.1/3 e ticks qualificados;
  reusar A-03/C-08 no backlog.
- **Arquivos:** transformação de série, registry e gerador/consumidor H.
- **Passos:** fixar tamanho R/reversão/pavios/origem → gerar por evento →
  símbolo/série customizada → checkpoints e paridade de tijolos.
- **Aceite:** ordem/timestamps determinísticos; nenhuma inferência fictícia de trajetória OHLC;
  menu/API do MT5 verificados antes de implementar.
- **V:** múltiplos tijolos/tick, gap, pavio, reinício, tijolo provisório e ticks idênticos nos dois lados.

### T19.42 — 2MV com piramidação Renko (S06/AX03)

- **Repo/prioridade:** M/H; P1 após suporte. **Depende de:** T19.41/22/23,
  média/tamanho/parcial definidos; risco multiordem comprovado para operação.
- **Arquivos:** sinal 2MV, política de parcial/adição e Expert/preset.
- **Passos:** canal/cor → limites em preços executáveis → parcial/adições → saída/reconciliação.
- **Aceite:** sem preenchimento em preço sintético indisponível; limite de adições e reserva;
  lucro da parcial não autoriza ignorar exposição.
- **V:** protocolo comum, vários tijolos/tick, cor neutra, inversão, cota diária e reinício.

### T19.43 — Manual + gradiente e Price Action (S04/AX08)

- **Repo/prioridade:** M/H; P1 após especificação. **Depende de:** T19.6/20/22/23,
  políticas manuais/grade vigentes.
- **Arquivos:** eventos de intenção manual, sinal PA opcional e composição.
- **Passos:** manter manual como manual → definir variante objetiva distinta →
  contexto macro/falha/polaridade → grade/parciais/filtros e preset.
- **Aceite:** ação discricionária é reproduzível por evento registrado; robô automático não
  recebe nome/fidelidade indevidos; exemplos pós-11h/lote não viram universais.
- **V:** protocolo comum, falha não confirmada, zona intermediária, filtro M30 aberto e dono da posição.

### T19.44 — Grid Connect como adaptador de intenção (S03)

- **Repo/prioridade:** H/M; P1 condicionado. **Depende de:** T19.22/23,
  B-04/B-07, T12.3–5 e política de averaging para execução.
- **Arquivos:** adaptador master/slave, executor e trilha de eventos/ciclos.
- **Passos:** protocolo de intenção → deduplicação/ownership → plano fracionado →
  orçamento/pendentes/trailing → reconciliação.
- **Aceite:** não desfazer ordem executada como se nunca tivesse existido; sem dois gestores
  disputando posição; sem multiplicação de lote oculta.
- **V:** protocolo comum de ferramenta, sinal duplicado, parcial/rejeição, perda do master e restart.

### T19.45 — Alaska & Square (S07)

- **Repo/prioridade:** M/H; P1 condicionado. **Depende de:** T19.17/18/20/22/23,
  alvo do médio esclarecido e política de averaging comprovada.
- **Arquivos:** sinal H1 short-only, política de adição/cesta e Expert/preset.
- **Passos:** esclarecer alvo/perna/cesta → venda stop → uma adição limitada →
  anti-spam H1 e stop global → preset.
- **Aceite:** referência 200/100/400/150/3/120 tem unidade e direção;
  nenhum stop é removido pelo médio.
- **V:** protocolo comum, gap no nível, médio e stop na mesma barra, alvo de cada perna e perda diária.

### T19.46 — Round Six e lacuna Black Box (S08)

- **Repo/prioridade:** documentação M/H; P1 de especificação. **Depende de:** T19.1.
  Código depende de T19.22/23, parâmetros/filtro definidos e política aplicável.
- **Arquivos:** ficha S08 e, somente após regra disponível, sinal/política/Expert.
- **Passos:** registrar o filtro ausente → obter regra ou definir variante transparente com ID próprio →
  fixar canal/níveis/lotes/stop → então implementar/testar.
- **Aceite:** sem fórmula, original permanece impedido; não declarar variante equivalente por aparência.
- **V:** revisão de especificação; protocolo comum apenas quando houver código verificável.

### T19.47 — Histórico 1–37/48 e ferramentas adicionais

- **Repo/prioridade:** documentação M/H; P1 de cobertura. **Depende de:** T19.1.
- **Arquivos:** catálogo e referências/requisitos dos épicos C/D/E/F.
- **Passos:** distinguir regras fornecidas e famílias genéricas → mapear equivalentes existentes →
  listar conteúdo faltante e tarefas próprias de cada novo setup/ferramenta identificável.
- **Aceite:** não inventar 37/48 itens; martingale/Fibonacci, ORB/Keltner/BB/RSI,
  OCO/cópia/painéis/travas têm destino e restrições explícitos.
- **V:** comparação integral com seção 12 e nenhuma afirmação de robô implementado por indexação.

### T19.48 — Robustecimento da gestão da conta (etapa posterior)

- **Repo/prioridade:** H/S, consumidores M; P2 posterior, pré-requisitos de segurança não adiados.
  **Depende de:** revisão atual de T12.2/T12.3 e épico F.
- **Arquivos:** guardião/registro/permissões/executor, carteira e auditoria existentes.
- **Passos:** inventariar o módulo que já opera → cobrir orçamento/reserva/ownership →
  conciliar margem/ciclos/limites → recuperação/liderança → netting/hedging e observabilidade.
- **Aceite:** evitar reimplementar épico F; cada lacuna ganha subtarefa própria antes do código.
  T19.48 é tarefa de diagnóstico/planejamento da etapa, não PR único de toda gestão global.
- **V:** revisão do estado e roteiro de testes de concorrência, falha, restart e conta;
  nenhuma operação real autorizada por esta ficha.

### T19.49 — Aceite integrado e continuidade da frente

- **Repo/prioridade:** M/H; P1 ao fim do recorte entregue. **Depende de:** T19.35 e entregas
  de catálogo incluídas na versão, identificadas previamente.
- **Arquivos:** docs, matriz de cobertura e evidências de versão.
- **Passos:** congelar recorte/pendências → repetir percurso completo →
  conferir toda rastreabilidade → reconciliar painel/roadmap/continuidade e checklist Windows.
- **Extensão:** o aceite da F19 completa inclui T19.50–57; piloto anterior pode ser aceito
  como recorte sem nomear Ultra/inversão como entregues.
- **Aceite:** nenhum item impedido/pendente rotulado como pronto; restante do catálogo continua
  aberto e impede dizer que a F19 inteira foi concluída; gestão posterior mantém estado próprio.
- **V:** suíte completa/doctor e percurso instalado; schemas/links; paridade/custos/ledger preservados.

### T19.50 — Convenção de nomes e identidade, incluindo nome do Manual

- **Repo/prioridade:** M; P1. **Depende de:** T19.2/25.
- **Arquivos:** serviço de presets/compiler/catalog, formatos de exportação e apresentação do nome na UI.
- **Passos:** implementar gramática §8.4 da especificação → normalizar tokens/escopo → separar
  preset_id/hash completo/user_name/name_slug/nome canônico → versionar → integrar ao Default,
  às quatro categorias de trabalho e artefatos. Nome do usuário entra no Manual desde a criação.
- **Aceite:** nome legível com robô/ativo/série/categoria/orientação/revisão/hash; reexportação
  estável; legado não renomeado; colisões não sobrescrevem arquivos; sem performance no nome.
  Nome livre aparece no rótulo/manifesto/round-trip, com slug seguro; Maximalista é alias de
  apresentação de FatorLucro e identificação automática usa metadados tipados.
- **V:** símbolos/acento/caracteres reservados, nomes longos/Windows case-insensitive,
  colisão de normalização e hash curto, revisão, nome vazio/duplicado, renomeação de apresentação,
  clone em outro caminho, aliases e round-trip com nome original.

### T19.51 — Inversão experimental da intenção de entrada

- **Repo/prioridade:** M/H; P1 experimental. **Depende de:** T19.2/17/19; adaptador B no H.
- **Arquivos:** transformação de intenção/planejamento, schema de composição, capacidades e flag da UI.
- **Passos:** campo orientation.invert_entry_signal=false → preservar sinal original → inverter intenção uma vez →
  recalcular plano/níveis/risco e avaliar filtros direcionais → gravar direção efetiva → mapear preset/H.
- **Aceite:** compra↔venda e ausência→ausência; desligado preserva legado; posição aberta/saída
  não são revertidas por essa flag; módulo sem transformação válida recusa o plano com motivo.
- **V:** buy/sell/neutro, aplicar inversão duas vezes recupera a direção, limite/stop/Fibonacci,
  gap/bid/ask/custos, SL/TP do lado correto, filtro direcional, preset/TF e paridade Python/MQL5.

### T19.52 — Ultra: descoberta e contrato de regras condicionais

- **Repo/prioridade:** M; P1 após base de pesquisa. **Depende de:** T19.2/21/24/27;
  T19.51 somente para candidatos que usem inversão.
- **Arquivos:** proposta presets/conditional_policy.py, registry de condições/composição e experimentos.
- **Passos:** formalizar §8.5 → predicados de hora/dia/semana/mês e mercado → busca limitada no treino →
  prioridade/alternativa padrão/âmbito de aplicação → congelar política para validação.
- **Aceite:** cada regra escolhe módulos suportados, tem ID/versão e n/qualidade; limites de
  complexidade/tentativas prévios; sem costurar vencedores retrospectivos ou usar resultado futuro.
- **V:** condições simultâneas, regra ausente/vencida, redundância de indicadores, calendário,
  features futuras, ruído, poucas sessões e replay determinístico de predicados/contextos.

### T19.53 — Ultra: execução no HADOUKEN e pacote compatível

- **Repo/prioridade:** M/H, S se afetada; P1. **Depende de:** T19.52/23/25/29/50;
  T19.51 quando a política usar inversão e gates multiordem quando usar grade.
- **Arquivos:** seletor de política/adaptador H, compilador/leitor versionado e eventos de decisão.
- **Passos:** carregar/verificar política vinculada ao .set → avaliar regras e precedência →
  aplicar/congelar no início → transições explicitamente suportadas → rastro e recuperação.
- **Aceite:** Expert aplica a mesma regra/contexto que Python; fallback preserva proteção;
  checksum/schema inválido bloqueia novas entradas; nome/manifesto não escondem módulo ausente.
  Gestão intraposição nunca remove stop físico, afasta proteção ou reinicia idade/cota indevidamente.
- **V:** assinatura/hash errado, regra/feature/versão ausente, conflito, restart, mudança de janela,
  parcial/pendente/ciclo, stop fixo↔trailing↔tempo, herança/override da orientação sem dupla
  inversão e paridade da sequência de decisões.

### T19.54 — Ultra: backtest completo, avaliação e UI de regras

- **Repo/prioridade:** M/H; P1. **Depende de:** T19.53/24/27/28/32/34.
- **Arquivos:** backtester/lotes/validação, UI de presets/Laboratório, gráficos e relatórios de evidência.
- **Passos:** executar política congelada em sequência → comparar com Default e três categorias/referência
  estática equivalente → avaliar OOS/custos adversos → apresentar regras/decisões/nome/estado → exportar.
- **Aceite:** objetivo de resultado líquido e risco definido antes da busca; ganho máximo não
  prometido; candidato insuficiente/reprovado identificado; Ultra não herda evidência estática.
  Linha do tempo identifica regra, fallback, orientação e transição; oráculo retrospectivo não exportável.
- **V:** regra que só vence com futuro, regime muda, conflito/fallback, nenhum candidato,
  resultado pior que estático, OOS por período, moeda/custos/exposição, export/import e UI responsiva.

### T19.55 — Default da estratégia como primeiro preset

- **Repo/prioridade:** M/H quando resolver padrões nativos; P1 no primeiro ciclo.
  **Depende de:** T19.2/25/50 e padrões efetivos da estratégia conhecidos.
- **Arquivos:** catálogo/controles/presets e UI; reuso da seleção de padrões existente.
- **Passos:** inventariar defaults/perfis internos → retrato versionado/origem/valores →
  Default primeiro → seleção inicial somente se não houver escolha → fornecer origem/valores
  para cópia pela T19.26 → exportar. O editor/Salvar como Manual completo pertence à T19.26.
- **Aceite:** uma referência Default por estratégia/versão/escopo, sem duplicar a opção legada;
  não otimizado/não editado no lugar; nome/ID/valores no round-trip; refresh não troca seleção
  explícita; nova versão preserva a antiga e não herda evidência sem conferência.
- **V:** ordem estável em filtros/ordenação, catálogo reaberto, perfil efetivo, TF, ausência
  de .set físico legado, dados de origem para clone Manual, compilação nova e paridade com defaults da fonte.

### T19.56 — Gestão de lotes, quantidade fixa/por risco e exposição

- **Repo/prioridade:** M/H; P1 do pilar operacional. **Depende de:** T19.2/17 e especificação
  do símbolo; contratos/adaptador B/T19.23 no H quando aplicável.
- **Arquivos:** sizing/backtester/execução existentes, SymbolSpec, contratos/preset e controles de lotes.
- **Passos:** inventariar lots_for_risk e limites reais → modo fixo/risco e unidades →
  normalizar volume solicitado/efetivo → mais de um lote em uma entrada → orçamento de
  adições/ciclo suportados → prévia/UI e mapeamento H.
- **Aceite:** lote não fixado em 1 ou casas universais; volume válido nunca aumenta risco
  aprovado; não confundir quantidade com número de ordens/trades; gates multiordem mantidos;
  fonte/dados de margem ausentes não viram margem ilimitada; lote fixo não contorna guardas.
- **V:** mínimo/passo/máximo, zero/negativo/NaN/infinito, quantidade >1, decimal/inteiro,
  abaixo do mínimo, custos/margem/exposição, ordens pendentes, buy/sell e paridade M/H.

### T19.57 — Plano de parciais simples/múltiplas e gestão do saldo

- **Repo/prioridade:** M/H; P1 do pilar operacional. **Depende de:** T19.56/18/19/24,
  livro/eventos F16 e adaptador H/capacidade correspondente; grid exige seus gates próprios.
- **Arquivos:** política de posição/execução, ledger/importadores/métricas, schema/compilador
  de preset, UI Lotes e parciais e executor seguro H; reuso dos fechamentos existentes.
- **Passos:** adaptar parcial legada → agenda com IDs/gatilhos/quantidade ou percentual/base →
  quantizar redução/saldo → estados/ack/deduplicação/restart → proteção pós-parcial →
  conciliar volumes/custos/PnL e métricas por trade/ciclo → UI/preset/H → prova nativa.
- **Aceite:** não prometer grid+parcial com a restrição atual do re-simulador; combinação
  só habilitada após implementação/teste; etapa inviável tem motivo; nunca inverter posição,
  remover proteção do saldo, repetir fechamento ou inflar acerto/PF contando parciais como trades.
- **V:** exemplos §6.3 (3→2→1→0; 0,30→0,20→0,10→0), 50% inviável de lote indivisível,
  bases inicial/saldo, soma/quantidade inválida, custos por preenchimento, adição após parcial,
  parcial inviável sem ativação indevida de break-even,
  stop/alvo simultâneos, rejeição/fill parcial, restart, ownership/netting/hedging suportados;
  contas conhecidas/backtest e replay nativo/DEMO com observação do volume/remanescente.

## 4. Verificação comum e definição de pronto por PR

Comandos executados no ambiente instalado, usando RTK conforme instruções locais:

```bash
rtk proxy .venv/bin/python -m pytest -q
rtk proxy .venv/bin/python -m mt5_extracao.bootstrap doctor
rtk git diff --check
```

Se houver .sh alterado, acrescentar ShellCheck dos scripts exigidos pelo AGENTS.
Não pular testes para obter verde. Falha inicial é registrada e distinguida de regressão;
não resolver defeitos externos dentro do PR da tarefa.

Schema aditivo e banco antigo abrindo; caminhos portáveis; nenhuma dependência externa no
bootstrap. Toda chave nova entra em DEFAULTS e config.ini.example com fallback; dependência
de runtime/opcional vai ao arquivo correto. Segredos ficam no .env, sem entrar em logs,
manifestos ou argumentos. Regras técnicas do AGENTS continuam obrigatórias em cada recorte.

No ambiente desktop, conferir a capacidade do TMPDIR antes da suíte. Se apontar para runtime
pequeno em memória, usar diretório exclusivo em disco para artefatos e temporários, sem
remover dados de outros agentes. Registrar display/escala/WM usados para a validação da UI.

Para **cada setup** S01/S04–S11 implementado:
- entrada/saída long/short aplicáveis, neutro, igualdade, aquecimento, sinal repetido e reset;
- eventos/preços/custos/volume/stop/TF/preset idênticos nos consumidores correspondentes;
- futuro acrescentado não modifica decisão antiga; HA/Renko/pivô respeitam disponibilidade;
- backtest completo, variante de custo adverso, amostra e origem explicitadas;
- par Python/MQL5, paridade de indicador/sinal/eventos e divergências justificadas;
- estado e proteção após reinício, desconexão, rejeição, parcial e fim de sessão;
- bateria/Safety e tipo de conta aplicáveis; preset sem módulo não é gerado como executável.

S02 valida série; S03 valida ferramenta/ownership. Não fingir que são robôs de sinal.
Simuladores/fakes cobrem ambos os caminhos `paths.is_windows`; Windows/MT5 físico tem
checklist próprio, não bloqueia desenvolvimento Linux nem é declarado validado sem execução.
Nenhuma evidência sintética ou smoke de software é resultado financeiro/OOS real.

**Retificação de validação:** aplicar [VALIDACAO_F19.md](VALIDACAO_F19.md). Usar testes de
estrutura/backtests para seus respectivos requisitos; observar gradientes/stops e demais
comportamentos operacionais no MT5 nativo, em replay visual ou DEMO ao vivo disponível.
Ativo offline/fora de pregão admite replay e recorte curto com cobertura funcional demonstrada;
não exigir operação ao vivo para todos os aceites nem chamar recorte de QA de evidência OOS.
Somente condições que realmente dependem de execução ao vivo mantêm pendência específica.

## 5. Roteiro de retomada para agente

“Leia AGENTS.md, continuidade, a especificação, o catálogo e este plano.
Comece em T19.1 se ainda não houver inventário aceito. Caso contrário, informe a tarefa
e suas dependências integradas antes de editar. Use o código atual como evidência;
não assuma que um contrato proposto já exista. Implemente uma tarefa por PR, incluindo
teste, documentação e aceites. Não altere parâmetros de conta nem aplique preset ao
gerá-lo. Se faltar regra, dado ou módulo, registre o motivo e preserve estado pendente.”

## 6. Evidência desta entrega documental T19.0

- Base inicial: `489591c`, árvore limpa antes de instalar/preparar o ambiente.
- Instalador Linux `--yes --no-mt5` e dependências dev executados.
- Suíte inicial desktop: 1052 aprovados/9 ignorados/6 falhas de UI, saída 1.
  As seis passaram em Xvfb; uma tentativa integral esgotou o runtime temporário e foi
  preservada como falha de ambiente. Suíte final em Xvfb/TMPDIR em disco:
  **1058 aprovados/9 ignorados/3 avisos em 875,34 s**, saída 0, sem alterar testes.
- Doctor final: **16 OK/2 avisos/0 falhas**, saída 0; opcional pandas-ta ausente e ponte desativada.
- Verificação documental: links locais, cobertura I01–I18/TM01–TM08/S01–S11/AX01–AX09,
  49 fichas, 111 dependências internas sem ciclo e `git diff --check`; 29 links novos válidos.
- Sem código, banco, Expert ou preset alterado por esta entrega. Fontes de estudo preservadas.
- Sem ShellCheck: nenhum script .sh foi alterado. Windows físico não executado.

## 7. Adendo T19.0 — nomes, Ultra e inversão (2026-10-17)

Pedido adicional registrado na especificação §§6.2/8.4/8.5, na matriz de requisitos do catálogo,
no roadmap e no painel principal. T19.1–49 conservam seus IDs; T19.50–54 detalham as novas
capacidades. Agora são 54 fichas planejadas. Nenhum gerador/preset/Expert foi implementado
por este adendo documental. A suíte integral acima precede o adendo e não é uma nova execução;
as verificações próprias são: SDK docs 3 aprovados antes/3 depois (0,77/0,73 s), doctor
16 OK/2 avisos/0 falhas, 54 fichas/134 dependências sem ciclos, 32 links novos e diff --check
limpo. Detalhes na continuidade. Runtime/testes/dependências não foram alterados.

## 8. Reserva de pesquisa futura — FRP

[FRP — Fluxo Relativo do Preço](../pesquisa/FRP_FLUXO_RELATIVO_PRECO.md), registrado como
FRP-00 a pedido do proprietário. FRP-01–04 são etapas **ESTUDO FUTURO/P3**, não tarefas
elegíveis pela regra de pegar a próxima ficha planejada. O plano atual contém 57 fichas,
incluindo Default e gestão de lotes/parciais; FRP não altera essa contagem.

Conceber a matemática depois da consolidação da base/registry/relações reais F13/F18:
grupos de 3/4 indicadores, correlação entre si e com movimento de preço, limiares a explorar
e utilidade incremental no Expert. Não iniciar protótipo ou mineração nesta entrega,
não exigir FRP para presets/Ultra e não tratar correlação como percentual de segurança.
Promover cada etapa a PLANEJADO somente com pré-requisitos e escopo próprios definidos.

## 9. Adendo T19.0 — Default, Manual nomeado e validação proporcional

Pedido de 2026-10-17: Default da estratégia primeiro; Nome do preset informado pelo usuário
no Manual com slug/nome canônico e identificação por metadados; Maximalista como apresentação
do perfil FatorLucro. T19.26/32/50 ampliadas, T19.55 adicionada, sem renumerar as anteriores.
Prompt e matriz de validação agora estão versionados junto ao plano, com replay nativo
admitido para o pilar operacional e testes/backtests próprios para estrutura/cálculos/pesquisa.
Somente documentação nesta entrega; verificação própria registrada na continuidade.

Verificação do complemento: SDK docs 3 aprovados antes/3 depois (0,74/0,79 s); doctor
16 OK/2 avisos/0 falhas; 55 fichas/138 dependências sem ciclos e 65 links novos válidos;
diff --check limpo, sem alteração de runtime/testes/dependências. Não houve novo ensaio
MT5/replay/conta nem execução integral atribuída a este complemento documental.

## 10. Adendo T19.0 — lotes e parciais no pilar operacional

Requisito explicitado na especificação §6.3 e integrado ao catálogo, schema, UI, prompt e
matriz de validação. T19.56/57 ampliam para 57 fichas. Dimensionamento por risco e parcial
simples existentes são reaproveitados; múltiplos estágios e combinação com grid exigem
extensão comprovada, não são declarados prontos por haver campos de parcial no código.
Somente documentação nesta entrega; verificações próprias registradas na continuidade.

SDK docs 3 aprovados antes/3 depois (0,75/0,82 s); doctor 16 OK/2 avisos/0 falhas;
57 fichas/145 dependências sem ciclos, 65 links novos válidos e diff --check limpo.
Sem alteração de runtime/testes/dependências ou ensaio nativo/financeiro neste registro.
