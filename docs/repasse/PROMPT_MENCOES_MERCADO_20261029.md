# Prompt — menções na internet para indicadores, relações e estratégias

Implemente a extensão **MEN** no MT5X/Intel5, repositório
`Misael-art/MISA_MT5-Extra-o-de-Dados`, executando o
[capítulo18 do plano canônico de Dados](../dados/PLANO_ESTRUTURA_DADOS.md#18-menções-na-internet--quantidade-positivo-e-negativo).
Esse capítulo contém requisitos, contratos, matriz de fontes, donos, incrementos
MEN-00…08 e bateria de aceite. Este prompt é a entrada de execução, não outro plano.

## Objetivo e decisão do proprietário

Capturar menções na internet a ativos, empresas e temas e disponibilizar **três
medidas primárias: Quantidade, Positivo e Negativo**. Médias, comportamento em
torno da média, desvios, razões, normalizações e correlações serão tratados pelos
indicadores/Analytics/Laboratório existentes. Não criar uma segunda análise de
atenção ou um índice obrigatório no coletor.

O dado é mais uma entrada para cruzamentos com preço, ativos, utilizadores,
indicadores, estratégias e operações. O proprietário chama esse ambiente de
**Strator**: localizar a superfície oficial correspondente, sem criar um produto
ou módulo homônimo. Menções não validam uma estratégia isoladamente. Correlação
nula é resultado legítimo; uma relação forte não autoriza operação financeira.

Não acrescentar como medidas obrigatórias: previsão de alta/baixa, alcance,
reputação, score de atenção ou recomendação de compra/venda. Temas culturais como
K-pop/zumbis podem ter séries próprias; vínculo com empresa/instrumento é uma
configuração explícita, sem descoberta automática de investimentos neste escopo.

## Leitura e reconciliação inicial

Leia integralmente as instruções aplicáveis: `/home/misael/.codex/RTK.md`,
`/home/misael/Projects/AGENTS.md`, `/home/misael/Projects/MT5X/AGENTS.md` e
`repos/app/AGENTS.md`. Prefixe shell com `rtk`. A regra atual de raiz única
prevalece sobre prompts históricos que mandavam criar clones/worktrees.

Leia o registro `Projects/.workspace/projects.json`, `repos/app/CONTINUAR_AQUI.md`,
`docs/continuidade.md`, `docs/ROADMAP.md`, `docs/PLANO_DE_TRABALHO.md`, o plano de
Dados (especialmente §§11–12 e18), `docs/dados/BD01_INVENTARIO.md`, os contratos
BD03/04/05/06/14/18/19 atingidos, `docs/sdk/laboratorio_series.md`, `docs/sdk/api.md`
e [posse LAB/Super ATR](../propostas/LAB_SUPER_ATR_PENDENCIAS_20261024.md).
Localize a implementação oficial de notícias, fontes, indicadores e relações.

Confira branch/upstream, remoto, PRs, ancestry, pins, checks e prévia instalada.
Fotografia do planejamento em29/10: App
`9f9559287521e4f832d5b1e9249aa13aba5862ab`, PR234, em cadeia de PRs abertos;
isso não prova integração em master. Havia WIP de janelas móveis na branch
`codex/janelas-moveis-congeladas`; preserve-o. Reconfira no início, sem usar esta
fotografia como pin nem publicar alterações alheias na frente MEN.

A única composição neste host é `/home/misael/Projects/MT5X`: App em `repos/app`,
HADOUKEN em `repos/hadouken`, Safety em `repos/safety`. Reutilize wrapper, único
`.venv`, caches e recibos. Preserve companions detached nos commits do manifesto;
não editar/repinar por conveniência, nem criar outro checkout/store/scheduler/
journal/executor. Arquivos compartilhados e instalação são compostos serialmente.

## Contrato Q/P/N que não pode ser alterado por conveniência

- Q conta publicações elegíveis distintas por objeto, fonte, consulta e janela;
  repetir a palavra no item não aumenta Q. Dois objetos recebem classificações
  próprias. Instrumento, companhia/marca e tema não são identidades equivalentes.
- P/N são contagens inteiras não negativas, não percentuais/probabilidades/scores.
  Classificam avaliação do objeto, não direção do preço. Rótulos exclusivos,
  `P + N <= Q`; neutro/misto/indeterminado ficam fora de P/N e em metadados,
  sem quarta medida obrigatória nem conversão automática do restante em neutro.
- Aliases propostos: `mentions.quantity`, `mentions.positive`, `mentions.negative`.
  MEN-01 confere convenções/colisões. Usar três características numéricas no store
  canônico; projeção de três colunas não exige novas colunas em OHLCV.
- Política inicial de documentos: originais/comentários/respostas contam;
  repostagem simples e duplicação de transporte não. Quote com comentário próprio
  pode ser outro item. Agregado com outra política conserva identidade própria.
- Q/P/N compartilham população/revisão. Q total1000 com amostra Q100/P30/N20
  não permite publicar P300/N200: total fica com P/N indisponíveis, e amostra,
  se útil, recebe série separada com seu próprio Q/P/N. Não extrapolar.
- Zero exige coleta bem-sucedida; falta de acesso, truncamento, coleta parcial
  ou classificação pendente exigem estado/motivo. Nunca copiar última contagem
  nem preencher P=N=0 para ocultar ausência de classificador.
- Fonte, consulta, aliases, idioma, região, tipos de publicação, deduplicação,
  resolução, classificador e versões são identidade/metadados, não novos scores.
  Demonstrar que duas fontes/consultas do mesmo símbolo não colidem em valores
  ou checkpoints. Não trocar o provedor global da base de preços pelo de menções.
- Resolver ouro metal/medalha, Vale companhia/verbo e Ibovespa/ETF/futuro. Filtro
  por autor público/canal/comunidade só onde suportado e com escopo versionado;
  consumidores do App seguem acesso existente, sem coletar informações privadas.
- Janelas `[início,fim)` UTC explícitas; distinguir publicação, captura, fechamento,
  processamento e disponibilidade. Publicação10h capturada10h20 não aparece
  às10h05; P/N prontos10h22 não aparecem às10h20. Revisões anexadas e snapshots
  antigos preservados. Backfill/modelo posterior têm proveniência retrospectiva.
- Reagregar somente janelas com cobertura/alinhamento/revisões consistentes.
  Não repartir dado diário em M1, somar acumulados sobrepostos ou contar repetição
  de um evento como amostra independente. Derivações usam o fluxo existente.

## Fontes oficiais e ferramentas

Todas as fontes discutidas estão na matriz §18.5; revalide termos, acesso,
quota, histórico, retenção e capacidades. Lista documental não prova integração:

1. **X:** [Post Counts](https://docs.x.com/x-api/posts/counts/introduction) e
   [API](https://docs.x.com/x-api). Contagem não retorna textos; corpus e arquivo
   histórico dependem da capacidade de acesso. Manter total/amostra distintos.
2. **Bluesky:** [Jetstream](https://github.com/bluesky-social/jetstream) e
   [referência HTTP](https://endpoints.bsky.app/). Candidato inicial se elegível;
   cursor, lacunas, reconexão, coleções e limites de histórico são explícitos.
3. **YouTube:** [commentThreads.list](https://developers.google.com/youtube/data/reference/rest/v3/commentThreads/list)
   e [comments.list](https://developers.google.com/youtube/v3/docs/comments/list).
   Painel de vídeos/canais, paginação/respostas e quota; não todo o YouTube.
4. **Reddit:** [termos](https://redditinc.com/policies/data-api-terms) e
   [acesso](https://support.reddithelp.com/hc/en-us/articles/14945211791892-Developer-Platform-Accessing-Reddit-Data).
   Uso comercial depende de acordo; sem acesso, registrar pendência sem contorno
   por scraping e continuar as partes independentes.
5. **Sites, blogs e fóruns:** selecionar publicadores elegíveis e registrar sua
   API/feed, URLs/IDs, regras e cobertura. Não há endpoint universal aprovado.
   Snippet e texto truncado não equivalem a conteúdo integral.
6. **Brandwatch:** [Data Retrieval](https://developers.brandwatch.com/docs/data-retrieval),
   [APIs](https://www.brandwatch.com/products/apis/) e
   [Consumer Research](https://www.brandwatch.com/products/consumer-research/features/).
   Verificar contrato, corpus dos agregados, categorias, idiomas e exportação.
7. **LSEG MarketPsych:** [oferta](https://www.lseg.com/en/data-analytics/market-data/quantitative-economic-data-solutions/marketpsych-analytics-and-models)
   e [MarketPsych](https://www.marketpsych.com/home). Examinar esquema real e
   licença antes de mapear Q/P/N; score proprietário não é contagem.
8. **Google Trends:** [definições](https://support.google.com/trends/answer/4365533?hl=pt-BR).
   Complemento de buscas relativas0–100, não preenche Q/P/N de menções.
9. **GDELT:** [DOC2.0](https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/).
   Reusar frente de notícias; tom/volume relativo não são contagens P/N e não
   substituem cobertura de conversa social.
10. **Classificação:** [FinBERT, pesquisa](https://arxiv.org/abs/1908.10063) e
    [implementação](https://github.com/ProsusAI/finBERT) como referência, sem
    presumir qualidade em português/gírias. Fixar modelo/tokenizador/licença,
    provedor/prompt/regras, idioma e versão; não impor GPU ou serviço pago.

Primeiro uma fonte real elegível ponta a ponta; depois segunda fonte pelo mesmo
contrato. Fornecedores adicionais sem acesso ficam pendentes, sem bloquear o
núcleo nem serem chamados de integrados. Não somar fornecedor com API direta
do mesmo corpus. Nenhuma assinatura/coleta onerosa está autorizada por esta lista.

## Execução e posse

Execute MEN-01…08 conforme dependências do §18.8, uma tarefa por PR. MEN-00
é documentação. Comece por inventário/contrato e fixture determinística; avance
até solução integrada e validada, sem encerrar apenas em proposta/scaffolding
quando a implementação estiver autorizada. Registre bloqueio concreto de acesso
e continue o trabalho independente, sem substituir prova real por mock.

Reutilize `database_manager`, `analytics.datasets`, `feature_store`, `features`,
`producers`, `producer_jobs`, `query_service`, `indicator_context`, materializador,
SDK/CLI e snapshots/transporte existentes. Entradas/recibos só recebem tabela
aditiva se houver lacuna comprovada; não criar segundo feature_store. Verifique
chaves de jobs/checkpoints multiconsulta. Não prender transação SQL durante rede.

Dados é dono de fonte/objeto/classificação/contagens/tempo/store/consulta.
Laboratório consome seleção/gráfico/médias/relações pelos serviços atuais.
Cruzamentos ausentes ficam no dono BD16/17/F18/LAB correspondente, com contrato
e aceite próprios. Não reimplementar motor para aparentar que todos os cruzamentos
estão prontos. Expert exige consumo/contrato operacional próprio posteriormente.

Rede tem paginação, retry limitado,429/Retry-After, timeout, cobertura, cursor,
cancelamento e retomada idempotente. Classificação tem erro/abstenção explícitos
e avaliação por objeto em amostra manual de desenvolvimento separada do teste;
fixar critérios antes da avaliação e relatar precisão/recall por classe/abstenções.
Persistir saída/versionamento para replay sem chamar um LLM atual. Classificador
não qualificado não recebe selo de capacidade real validada.

Corpus/texto e dados de autores seguem licença/minimização/retenção e destino
próprio; insumos ignorados têm manifesto e proteção independente. Não incluir
texto bruto em exportação de snapshot por conveniência nem publicar corpora,
consultas privadas ou credenciais no repositório público.

## Verificação, publicação e definição de conclusão

Execute a bateria §18.9: Q10/P3/N2, multiobjeto, repetição de palavra/transporte,
fontes/consultas/bases isoladas, zero/ausência, total versus amostra, score recusado
como contagem, bordas/tempos, chegada/classificação tardias, revisão/snapshot,
reamostragem sem cobertura, cancelamento/quota/retomada, migração repetida,
SQLite/PG afetados e leitura sem rede/classificação implícitas. Teste relações
conhecidas e nulas pelo motor existente, sem exigir resultado financeiro.

Use wrapper/`.venv`, testes pertinentes, gates obrigatórios e doctor. Confira
RAM/disco/processos antes de trabalho pesado; uma suíte/build pesada por vez.
Downloads/modelos/pacotes acima70MB ficam preparados para o usuário, inclusive
partes. Não instalar globalmente nem criar serviço recorrente. Reutilize acesso
válido; quando faltar, solicite só a ação indispensável no fluxo oficial,
nunca senha/token no chat, enquanto progride nas partes independentes.

Demonstre captura pública pequena no host, ingestão/classificação/consulta,
reabertura/revisão e percurso SDK/CLI/Laboratório pelo launcher oficial em QA
separada. Três séries descobertas, fonte/versão/estado visíveis, indicadores e
uma relação suportada com preço/indicador; inspecione espera/erro/cancelamento.
Depois qualifique segunda fonte. Mock, CI e pesquisa retrospectiva não certificam
fonte real, latência operacional, MT5/Wine, Windows físico ou conta. Não negociar
REAL nem usar banco privado como fixture.

Por incremento, revise arquivos explícitos, confira destino/visibilidade e
faça commit/push/PR com SHA remoto/checks confirmados. Atualize plano/painel/
continuidade; runtime validado deve chegar à prévia pelo mecanismo existente,
com retorno preservado. Sem force push, merge automático, release documental
ou repin por conveniência. Registros administrativos somente em
`Projects/.workspace`, preservando atualizações de outras tarefas.

Entregue o que funciona e onde, contratos/versões, fontes exercitadas, testes/
jornada, commit/PR/SHA remoto, instalação e limites. Mecanismo completo exige
primeira fonte real, classificação qualificada, Q/P/N no catálogo/store/snapshot/
Laboratório e segunda fonte sem reescrever consumidores. Discrimine acessos
opcionais e cada cruzamento pendente. Utilidade financeira e operação de Expert
têm estudos/gates próprios e não condicionam o aceite técnico da captura.
