# Backtest em todos os TFs e comparação por preset

Pedido do proprietário de 21/10/2026. Requisitos e entrega incremental.
Complementa T19.33, T20.22/23/24/25 e os componentes F17/T11 existentes. A correção
da exibição do painel manual no REPLAY é prioritária e tem PR próprio.

## Seleção no card — T19.33

**T19.33a (30/10):** ordenação por duração e deduplicação por TF canônico nos
seletores do card MQL5 e do backtest Python. O rótulo da tabela local permanece
intacto; seleção disponível e contexto/preset são preservados após atualização
e mudança de tema. Valores desconhecidos continuam visíveis no fim, sem serem
convertidos em TF válido. O diálogo só captura entrada depois de ser mapeado
pelo gerenciador de janelas, evitando `grab failed: window not viewable`.
**T19.33b permanece pendente:** Todos, plano revisável, filtro de elegibilidade,
identidade congelada e expansão no executor existente. Nenhum lote é disparado
pela simples seleção ou atualização dos TFs.

1. Ordenar TFs por duração crescente usando `timeframes.Timeframe`/parser atuais,
   preservando os nomes legíveis da base. Exemplo: M1, M5, M15, M30, H1, H4, D1,
   W1, MN1; não ordenar strings alfabeticamente nem colocar H1 antes de M5.
2. Oferecer **Todos** como opção de escopo, depois dos TFs individuais. Essa opção
   expande um backtest em jobs separados; não é um TF enviado ao terminal ou motor.
3. Antes de iniciar, mostrar ativo(s), TFs efetivos, quantidade de combinações,
   janela, estratégia e identidade/revisão/hash do preset congelado. Preservar
   Default e Manual e suas restrições de TF interno.
4. No Python, expandir apenas séries elegíveis disponíveis na base. No MQL5,
   considerar TFs nativos suportados pelo perfil e dados/modelo necessários.
   Explicitar TFs excluídos por ausência de base, warmup, capacidade ou preset;
   não baixar/resamplear dados silenciosamente nem alterar parâmetros internos.
5. PLAY/REPLAY continuam exigindo um ativo e um TF. Ao selecionar Todos, indicar
   a escolha individual necessária antes de habilitar essas ações; não executar
   uma carteira ao vivo nem abrir várias sessões visuais por esse seletor.

Aceite: ordem correta para M1/M5/M15/H1/H4/D1, ausência de duplicatas por alias,
seleção preservada no refresh; Todos gera uma execução por combinação elegível
sem enviar a palavra ao motor; contexto/preset exatos nos relatórios e no histórico;
incompatibilidades e lote vazio explicados; regressão da seleção individual.

## Vários ativos e capacidade do host — T20.24/25

**Viabilidade inicial: sim, para selecionar vários ativos e enfileirar os testes.**
`robots/runner.py` já tem `BatchRunner`, `ResourceGovernor`, limites de CPU/RAM e
workers. `robots/budget.py` mantém orçamento/checkpoint; recursos e retenção já
possuem donos. A UI deve usar esses mecanismos, sem outro scheduler ou cache.

Permitir seleção múltipla e **Todos os ativos elegíveis** com prévia do produto
ativo × TF. Selecionar 3 ou 6 não promete executar 3 ou 6 ao mesmo tempo. No host
observado há 8 CPUs lógicas e cerca de 14 GiB de RAM, mas isso não determina o
número seguro de execuções; o custo depende da série, estratégia e modalidade.

- Começar com baixa concorrência e admitir novos jobs conforme os limites
  existentes `batch_max_cpu_pct`, `batch_max_ram_pct`, `batch_max_workers`, tempo
  e espaço de disco. Medir RAM de pico, CPU, I/O, temporários e resposta da UI.
- Backtests Python podem usar workers existentes. O ganho depende do perfil de
  cálculo/GIL e deve ser medido, sem promessa de aceleração linear.
- **MQL5 continua com um job nativo por vez**, usando contexto/lease existentes.
  Vários terminais exigem qualificação específica de ownership, portas, relatórios,
  cache, licenças/servidor e orçamento; não liberar por ter RAM/CPU disponível.
- Preset, janela e origem dos dados ficam congelados por lote e identificados
  por job. Reusar checkpoint, cancelamento e retomada; concluir parcialmente
  mantém erros/recusas visíveis e não ganha estado de lote completo.
- Verificar espaço antes de copiar dados nativos; evitar duplicar instalação e
  cache para cada TF. Limpar só temporários próprios após os consumidores saírem,
  preservando recibos mínimos, histórico, falhas e dados/configuração do usuário.

Aceite: 1/3/6 ativos, saturação de RAM/CPU/disco, cancelamento, retomada sem
duplicar efeitos e UI operável. Registrar concorrência efetiva, latência e uso de
recursos no host; reduzir/serializar ao atingir limites. O limite é configurável,
com DEFAULTS/example/fallback quando houver chave nova.

## Visão gráfica limpa — T20.22/23

Reutilizar `robots/matrix.py`, store/histórico, gráficos Matplotlib e tokens F17.
A matriz atual usa a execução mais recente por robô/ativo/TF; **essa chave sozinha
não garante o mesmo preset**. A consulta deve filtrar identidade/revisão/hash do
preset, variante/composição, janela, origem/modelo e dataset para evitar misturas.
Não selecionar retrospectivamente o melhor run de cada TF.

Contexto fixo no cabeçalho: estratégia, nome/revisão do preset, ativo, intervalo,
capital, custos e modalidade. TFs crescentes no eixo e nas tabelas.

| Área | Apresentação |
|---|---|
| Resumo | Quantos TFs concluídos/pendentes/falhos e cobertura comum |
| Comparação principal | Retorno líquido percentual por TF, positivo/negativo legíveis e linha zero |
| Risco e amostra | Drawdown e número de operações, com detalhe de PF/custos/cobertura |
| Evolução | Pequenos gráficos de capital por TF; normalização e base declaradas, zoom/detalhe sob demanda |
| Vários ativos | Matriz ativo × TF com escala/legenda estáveis e filtro por métrica |
| Inspetor | Clique abre histórico, curva, trades, preset efetivo e motivo de inelegibilidade |

Sem teste, sem dados, falha, cancelado, zero trades e PF indefinido são estados
distintos. Resultados de janelas/modelos/custos incompatíveis são identificados;
comparação comum requer dados elegíveis e cobertura explícita. Não executar novos
testes apenas para abrir a visão. Usar textos/unidades/ícones além das cores,
temas claro/escuro, teclado, rolagem e disposição responsiva sem sobrecarregar.

Aceite: mesmos números do store para o preset escolhido, resultados negativos
preservados, histórico imutável, detalhe da célula correto, estados ausentes
legíveis e desempenho aceitável com todos os TFs e 3/6 ativos. Lucro por TF não
significa aprovação estatística; amostra, exposição/múltiplas tentativas e gates
T20.12/13 mantêm os requisitos próprios.

## Ordem de entrega

Correção REPLAY → T19.33 ordenação/escopo Todos → integração da seleção múltipla
com T20.24/25 → comparação filtrada pelo preset em T20.22/23. Uma tarefa por PR,
com baseline pertinente, regressões e evidência proporcional. Registrar cada
avanço validado no GitHub e retirar temporários próprios após a conferência.
