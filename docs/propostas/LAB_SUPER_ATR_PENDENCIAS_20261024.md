# Laboratório e Super ATR — pendências e divisão de frentes

Registro do proprietário em **24/10/2026**, após testar pelo atalho central.
**Laboratório incompleto; aceite funcional reaberto.** Este documento registra
trabalho a executar, não entrega as correções nem inicia os agentes.
Tarefa documental LAB-SA-00; cada implementação abaixo terá seu próprio PR.

## Ponto de partida e evidência

- App: `codex/lab-multiframe`, fonte documental
  `af717fa90bd4d514fb226622862bca572ae41f88`, runtime
  `7c66feda06eb065b5cd591fd22ba5b602cffa624`,
  [PR210 em rascunho](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/210).
  Depende de AppPR207/208 e HPR75. Ainda não é master integrado.
- [Repasse anterior](../repasse/T20_22_LABORATORIO_CONTINUIDADE_20261024.md)
  conserva testes, limitações, instalação e backup. Seus testes não substituem
  os percursos reais agora reportados como falhos. A ampla anterior imprimiu
  3014/259 e terminou com código143 após o resumo; a seleção nova20/15 terminou0.
- Três imagens fornecidas pelo proprietário: sufixos `533b4e69…` (visão entre TFs
  cinza, com uma barra/NaN no detalhe), `495f1993…` (Horários, cabeçalho desalinhado)
  e `cec7624f…` (Dia do mês, rótulos verticais e cores antigas). As originais
  ficam no contexto privado; contêm identificação de conta e não são publicadas.
  Imagem vazia é sintoma: a causa precisa ser reproduzida, não deduzida da captura.
- `ui/relations.py` ainda declara uma fixture sintética, com quatro indicadores.
  Os 52 cálculos Analytics, os 38 indicadores de Estudo nativo e o inventário
  de séries realmente persistidas são conjuntos distintos. Nenhum desses números
  comprova cobertura completa da base. Serviços de consulta BD18a/BD19c1 já
  existem: consumir seus contratos, sem novo catálogo ou store concorrente.

## Backlog verificável

P1 indica percurso ausente/falho ou dependência funcional; P2, refinamento visual.
Todos permanecem **pendentes**, salvo o registro documental LAB-SA-00.

| ID / plano existente | Prioridade / dono | Trabalho e critério de aceite |
|---|---|---|
| LAB-01 · T18.6/8, BD19 | P1 · Laboratório, contrato com Dados | Indicador × indicador e Indicador × alvo devem descobrir **todo o inventário da base**, variantes/outputs/params incluídos. Busca e filtros com motivo explícito para indisponibilidade; sem lista curta fixa nem fixture apresentada como dado real. Teste de inclusão de uma nova definição sem editar o seletor, paginação e identidade da fonte. |
| LAB-02 · T20.22-LAB1 | P1 · Laboratório | Reproduzir Séries e confluências pelo atalho e corrigir a geração das visões. Identificar DB/contexto/manifestos/TFs/corte, warmup e exceções. Dados elegíveis produzem curvas e eventos; dados insuficientes produzem motivo e ação útil, nunca apenas área cinza ou sucesso com NaN. Cobrir resposta obsoleta/cancelamento e erro de fundo. |
| LAB-03 · T17.2/5 | P1 · Laboratório | Horários: cabeçalho e corpo compartilham larguras, posição horizontal e resize. Validar alinhamento de **cada coluna** após rolagem vertical/horizontal, maximizar/restaurar, DPI e mudança de monitor; não apenas captura estática inicial. |
| LAB-04 · T17.5, T20.22 | P2 · Laboratório | Modernizar Dia do mês e revisar as demais visões. Escala divergente consistente, magnitude forte em tons intensos e fraca em tons claros, zero neutro, ausente distinto. Valores horizontais e legíveis; remover `n=…` repetido sobre todas as barras, levando amostra/insuficiência para legenda contextual, tooltip ou detalhes. Preservar denominadores e limitações, com temas claro/escuro e escala visível. |
| LAB-05 · T20.22/23 | P1 · Laboratório | Visão gráfica do **mesmo ativo com estratégias/robôs diferentes**: preço, sinais/eventos e curvas comparáveis, seleção identificada e navegação temporal vinculada. Distinguir intenção, operação e execução; comparar só recortes/modelos/custos compatíveis ou explicitar incompatibilidade. |
| LAB-06 · T19.33, T20.22/24/25 | P1 · Laboratório | Visão do **mesmo robô com a mesma revisão de preset em diferentes ativos**. Mostrar hash/revisão efetiva, intervalos, especificações/unidades e custos; curvas normalizadas quando moedas/escalas diferirem. Último run por ativo não comprova mesmo preset. Reusar orçamento/fila; não disparar backtests ao abrir a tela. |
| LAB-07 · T20.22/23, T20.13 | P1 · Laboratório | Mesma estratégia entre TFs, com comparação sincronizada e grau de relação **descritiva e preditiva separados**. Fixar direção/horizonte/defasagem, elegibilidade as-of, denominadores, eventos não confirmados, controles e incerteza. Avaliação temporal fora da amostra para alegar previsão; sem ela, rotular exploratório. M7/M9 somente pelo contrato de barras derivadas, não inventados na UI. |
| LAB-08 · T20.23, G19.6/7, AIA-02 | P1 · Laboratório | Gráfico do ativo com candles e todas as séries qualificadas selecionáveis: sobreposição/subjanelas conforme unidade, múltiplas instâncias parametrizadas, legenda/ocultar/remover, cursor/zoom vinculados. Ferramentas de linha, horizontal/vertical, região e nota, editar/excluir/desfazer e exportar/reabrir o estudo com identidade. Reusar Estudo existente quando aplicável, sem limitar o laboratório a abrir uma janela MT5. |
| LAB-09 · T19.32, F19, AIA-02 | P1 · Laboratório + integração serial | Gerador de presets coeso via Default/Manual/store/compiler existentes. Corrigir a descoberta e qualificar a closure de cada perfil; os seis perfis automáticos encontrados estavam bloqueados por versão/pin/inputs. Não remover gate para habilitar botão nem criar formato paralelo. Saída do estudo é candidato rastreável, não preset aprovado por correlação. |
| LAB-10 · T20.30, AIA-02 | P1 · Integrador / cada frente | Todas as capacidades por serviço/SDK/CLI e UI cliente; parâmetros tipados, JSON, IDs, exportação, erros úteis e resultado verificável. Publicar composição imutável e testada no atalho/menu centrais, com rollback. Validar handheld, monitor amplo, DPI, coordenadas negativas e desconexão do dock; ampliar/restaurar gráfico sem perder contexto, rolagem na tela menor. |
| SA-01 · EST-OP, G19.9/13/15 | P1 · Expert Super ATR | Expert que consome a matemática existente e exige confirmação causal entre TFs. Congelar variante de entrada/saída/risco/linhas/TFs antes do código; usar somente barras fechadas disponíveis, recusando confirmação futura/ausente/vencida. Reutilizar risco/executor/host e testar pelos percursos de preset/CLI/atalho. Primeiro recorte Tester+DEMO; sem autorização REAL. |
| SA-02 · EST-IND, BD06/07/18/19 | P1 · **Agente do pilar de dados já existente** | Cálculo e persistência do Super ATR, com coluna/projeção consultável pelo usuário e série canônica identificada por parâmetros/versão/linha/seed/janela/fonte. Store aditivo, consulta e as-of/qualidade, repetição idempotente, revisão preservada e paridade declarada com MQL. Laboratório e Expert são consumidores, não donos de outro storage. |

As quatro comparações não são a mesma entrega: indicador entre TFs, estratégia
entre TFs, estratégias diferentes no mesmo ativo e preset idêntico entre ativos.
O inspetor financeiro ainda depende de eventos de execução/exposição comprovados;
não inferir close, SL aplicado ou fill a partir de uma curva do indicador.

## Divisão de posse e dependências

| Frente | Pode editar no seu recorte | Preserva / consome |
|---|---|---|
| Agente Laboratório | `laboratory/`, UI/gráficos/componentes, adaptadores de leitura, testes e docs próprios | Produtores/schema/ingestão/registry canônico da frente de dados; matemática Super ATR; motor financeiro e executor |
| Agente Expert | Novo consumidor MQL5 Super ATR, testes/contrato próprios e adaptação de perfil por commit revisado | Cálculo canônico, RiskGuard/SafeExecutor/PanelHost, fontes da outra frente do painel, dados e componentes visuais do Laboratório |
| Agente Dados existente | Cálculo/store/migração/materialização/query canônicos, incluindo SA-02 mediante seu contrato | Preserva WIPs dos consumidores; entrega série/envelope versionado para ambos |
| Integrador | Mapas centrais, contrato compartilhado e composição/publicação da prévia | Só compõe commits congelados por Git; evita edição simultânea de `cli.py`, SDK, catálogos, manifests e pins |

Cada agente cria worktree/branch próprias a partir da composição identificada.
Não usar a raiz com WIP como checkout de edição. Arquivo central compartilhado
tem **um dono por vez**; os demais entregam patch/commit para composição serial.
Teste nativo exige `tester_lease`, namespace e processos próprios. Não encerrar
Wine/MT5/ponte ou serviços globais de outro agente para obter a reserva.

### Acordo necessário com Dados — antes de consumir SA-02

O integrador e o dono de Dados congelam a definição do Super ATR e a forma do
envelope, reutilizando BD06/07/18. Campos necessários: ativo/contrato, fonte e
dataset/manifesto, TF/calendário/fuso, abertura/fim da barra, `available_at`, revisão,
ID/versão da fórmula, período/tipo de média/desvio/lado inicial, seed/início da
janela, nível/estado/ATR/virada e qualidade/warmup. Os nomes concretos seguem o DTO
canônico: esta lista é requisito, não um schema implementado por este documento.

O pedido de **coluna do indicador Super ATR na base** precisa resultar em algo
visível e consultável, inclusive exportação. Não sobrescrever uma coluna sem
identidade ao mudar parâmetros nem criar cinquenta colunas ambíguas por consumidor.
Fechamento nominal não comprova instante histórico de recebimento. Ausente,
provisório, inválido, zero válido, warmup e revisão não são o mesmo estado.
Sem disponibilidade histórica certificada, a UI/estudo deve declarar o limite.

Enquanto esse contrato estiver pendente, os agentes podem implementar UI e
testes com fixtures **declaradas**, e matemática/consumidor com séries congeladas.
Não declarar integração à base ou paridade ponta a ponta concluída por isso.

## Ordem e entrega

1. Laboratório inicia pelo reproduzível LAB-02/03 e catálogo real LAB-01, em PRs
   próprios; após isso LAB-04/08 e comparações LAB-05/06/07. Presets/SDK/atalho
   acompanham cada recorte, sem esperar o fim para testar a integração.
2. Em paralelo, Expert lê a memória e congela a variante SA-01; Dados mantém SA-02.
   Não esperar o redesenho visual para testar causalidade do consumidor.
3. Integrar serialmente bibliotecas, pins e leitores pelos contratos aprovados.
   Publicar fonte/commit/PR/push e prévia identificada; não promover canal aceito
   nem fazer merge automático por um gráfico parecer correto.
4. Aceite de cada item registra comandos, exit codes, identidade do runtime
   realmente lançado pelo atalho, seleção/dados sem segredos, capturas saneadas,
   limites e pendências. Aplicar testes negativos de warmup/NaN/lacunas/revisão,
   não esconder indisponibilidade para preencher a tela. Windows físico é pendência
   explícita; instalação/compilação/QA sintética não qualificam operação financeira.

Prompts completos, prontos para agentes independentes:

- [Laboratório, ferramentas e modernização](../repasse/PROMPT_LABORATORIO_GRAFICOS_20261024.md).
- [Expert Super ATR com confirmação entre TFs](../repasse/PROMPT_EXPERT_SUPER_ATR_MTF_20261024.md).
