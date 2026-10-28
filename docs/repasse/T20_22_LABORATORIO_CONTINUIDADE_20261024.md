# Continuidade — Laboratório de séries, 24/10/2026

**Reteste posterior do proprietário: aceite funcional reaberto.** A entrega
inicial abaixo não concluiu o Laboratório. Falhas e ausências foram registradas
em [LAB-01…10/SA-01/02](../propostas/LAB_SUPER_ATR_PENDENCIAS_20261024.md), com
prompts de Laboratório e Expert e posse preservada da frente de dados.
Manter evidências históricas, reproduzir o caso real pelo atalho e corrigir
os percursos; não inferir aceite pelas capturas QA anteriores.

Frente independente `codex/lab-multiframe`, [PR210](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/210),
base exata AppPR208/d1c1097067129919b734e67203c363f0662639e2. Implementação
fd6062885e303b43fe2d20c6a7bf557dc40306c7, refinamento de interface
7c66feda06eb065b5cd591fd22ba5b602cffa624. Não houve merge em master.
Repositório: Misael-art/MISA_MT5-Extra-o-de-Dados. No novo host, buscar essa branch
ou `pull/210/head`, instalar pelo procedimento do SO e ler AGENTS antes de editar.
Dependências anteriores: AppPR207/208 e HPR75, conforme repasse AIA-02a.

## Entrega

[Contrato, limites, CLI e próximas frentes](../sdk/laboratorio_series.md).
Serviço em `mt5_extracao/laboratory`, sem Tk/MT5 no núcleo. CLI
`laboratory catalog|sources|freeze|analyze`; workflow `cli.run` descobre os mesmos
argumentos. UI em `ui/laboratory_series.py`, integrada ao Laboratório existente.
Cinco visões reais, mesma estratégia/params em vários TFs, correlação direcional,
defasagens nos dois sentidos, cobertura, ampliação/restauração, claro/escuro,
rolagem e campos numéricos tipados. Presets delegam ao Default/Manual existente.

Não foi criado outro store/fila/cache, motor de backtest ou gestor financeiro.
Corpos de dados/registry/produtores, presets/compiler, estratégias/backtester,
robôs/execução, H/Safety e configuração do proprietário foram preservados.
A raiz com WIP dos outros agentes não foi usada como checkout de edição.

`analysis_id` associa dados/params/corte e hash das fontes. Recálculo só ocorre
ao analisar; ampliar ou reabrir o detalhe usa o mesmo DTO. Atualização obsoleta
não substitui seleção nova. Warmup/constante/indisponível e volume real zero
continuam distintos de zero neutro ou correlação perfeita. A redução gráfica
conserva sinais raros e mostra conflitos; não altera estatística. Intenção retida
do TF maior não vira várias entradas nas marcas nem na contagem de origem.

## Validação e disponibilização

Resultados e hashes na [evidência](../evidencias/T20_22_LABORATORIO_SERIES_20261024.json).
Baseline focada92; seleção28; interface final16, com seleções sobrepostas.
79 regressões anteriores aos últimos refinamentos visuais. Doctor20OK/1pandas-ta
opcional/0falhas reutilizando configuração/ambiente existentes; o primeiro doctor
isolado registrou falta de config local, sem copiar/sobrescrever configuração.
Ampla:3014 aprovados/259 pulados em2246,91s, sem falha de teste impressa; retorno
do processo143 após o resumo, sem causa estabelecida. Não alegar exit0/encerramento
verde dessa execução. A seleção nova terminou0:20 aprovados/15 pulados em32,88s,
somente MainThread após pytest e atexit normal. A coleta final tem3280 testes;
sete casos GUI adicionados após o início da ampla estão nos16Tk finais. Não
foram removidos/desativados casos. CI de cada HEAD tem estado próprio na evidência.
Nenhum teste foi retirado ou afrouxado; GUI foi exercitada em Xvfb próprio.

QA histórico:52 definições,11 estratégias × M15/H1/H4 do EURUSD, julho–setembro2026.
Leitura do original SQLite com `mode=ro/query_only`; cálculos em cópia privada,
namespace próprio, sem certificação da fonte/aceite financeiro. Nenhuma sessão
MT5, Tester ou ordem real foi iniciada por esse recorte.
Capturas1280×800/3456×1458, dois temas e detalhe inspecionados. Coordenadas
negativas testadas na sintaxe absoluta do Tk; hardware Windows/DPI/dock físico
mantém pendência própria. Não há promessa de FPS.

Prévia `app-development` instalada com runtime7c66feda06eb. Atalhos/menu usam
o destino estável `mt5x/app-development/run.sh`; configuração, .env, dados,
ambiente e ponte existentes reutilizados. A ponte manteve a identidade/PID;
nenhum processo alheio foi encerrado. `app-current`, H e Safety atuais/de
desenvolvimento preservados; releases anteriores permanecem para retorno.
Aplicação instalada foi aberta offline com DB privado e módulo da release correto.
Uma janela antiga pode continuar em memória: reabrir o aplicativo carrega a prévia.
O commit documental posterior não requer reinstalar o runtime idêntico.

## Pendências prioritárias e donos

1. **Perfis/presets operacionais:** conferência estática da biblioteca configurada
   recusou os seis perfis automáticos por snapshot/pin/inputs. A conexão ao gerador
   não aprova essa closure. Qualificar serialmente a versão atual, começando por
   S09, em recorte próprio com fonte completa, versão nova/pin e inputs/recibo;
   preservar perfis anteriores. Não alterar risco/executor para fazer o gate passar.
2. **Frente de dados:** integrar leitores de barras derivadas/customTF e fatos
   materializados BD18a com `available_at/revision`. Este recorte consome snapshots
   OHLCV em TFs nativos e reconstrução nominal; não contém histórico de recebimento.
   Catálogo Analytics52 não é catálogo nativo38. ADX/Super ATR/indicador estratégico
   e demais lacunas exigem contratos próprios, sem fórmula aproximada ou store novo.
3. **Comparação financeira:** mesmo preset/revisão/composição, janela/custos/modelo
   e datasets; matriz de runs mais recentes não garante isso. Não converter contagem
   de intenções em trades, acerto, lucro ou probabilidade calibrada.
4. **T20.23:** cursor/zoom/tabela vinculados e eventos reais de SL/TP/exposição/equity,
   somente quando as fontes comprovarem cada informação. Não atribuir causa ao close
   por intenção reconstruída.
5. **Estatística:** T20.12/13/19.27/28 continuam com pré-registro, tentativas, reserva,
   maturidade e OOS próprios. Recorte já explorado não ganha reserva intocada.
   Não escolher retrospectivamente o melhor indicador/lag como preset aprovado.
6. Conferir CI da ponta e preservar diagnósticos de falhas herdadas; não editar o
   pilar de dados para ocultar timeout. Windows físico e aceites nativos separados.

## Preservação antes da formatação

No SD: `Backups/MT5X/2026-10-24-laboratorio-series`. Contém continuidade/refs e
uma base QA pequena reutilizável: EURUSD6336M15/1584H1/396H4 +3 snapshots,
copiada por SQLite backup, integrity_check=ok e hashes conferidos. Não inclui
contas, credenciais, venv/Wine nem cópia da base oficial. Fonte/qualidade/offset
estão limitados no README; serve à retomada gráfica, sem licença operacional.
Não substituir o backup maior existente da base do proprietário por esse QA.

Remover somente basetemps/SQLite/wheel/build próprios após saída dos consumidores;
preservar fontes, evidências, refs, rollback, WIP e dados dos demais agentes. Os
bytes concretos liberados e a conferência remota/SD estão no fechamento da evidência.
Antes de formatar, recolher também os repasses e WIPs das outras frentes.

Fechamento da limpeza:1.485.367.403 bytes lógicos liberados,41 raízes próprias
processadas, nenhum processo sinalizado. Três subárvores `share/gvfs-metadata`
(99.920 bytes observados no fechamento) permanecem por estarem abertas no serviço
preexistente `gvfsd-metadata`. Não encerrar esse serviço para limpar o restante.
A raiz padrão da baseline sem prova de posse foi preservada; a raiz804 da primeira
bateria nova foi identificada pelo log e removida. Logs/recibos/fontes de QA e
capturas relevantes foram mantidos. Arquivos existentes de dados/configuração e
worktrees de terceiros não foram removidos. Build/wheel próprios verificados e
retirados; o wheel final continha os três módulos novos byte a byte.

## Auditoria executada e correções de qualidade/eventos — 28/10/2026

Escopo desta rodada: interface Tk pelo launcher oficial, consulta das bases em
transação somente leitura, oráculos de agregação M1 e regressões sintéticas.
Não houve negociação, ativação de Expert, mudança de pin, conversão de histórico
ou alteração de offset. A base ativa e a recuperada continuam separadas.
Os recibos e capturas atuais ficam na composição em
`state/qa-tmp/product-audit-20261028/`, fora do Git público. Nenhuma operação do
livro, conta, credencial ou conteúdo privado acompanha esta publicação.

### Percursos e estado observado

| Etapa | Entrada → processamento → resultado | Aceite e limite |
|---|---|---|
| Ativos / conexão | Configuração local → catálogo e inventário → seleção e conexão desativada | GUI local exercitada; conexão/extração nativa não qualificada nesta rodada |
| Minha base | Tabelas oficiais → inventário → sete séries EURUSD | Exibição e seleção exercitadas; data recente não é certificação de integridade |
| Análise | Série/período → indicadores, retornos e diagnóstico → cartões/gráfico/qualidade | Série válida desenhada; fonte sintética inválida agora bloqueia cartões e abre Qualidade |
| Laboratório / livro | Runs persistidos → cache e agregação → KPIs/mapas | Carregamento real conferido; agregação descritiva sem qualificar custos, carteira ou estratégia |
| Séries / preparação | Inventário → TFs e parâmetros → snapshot canônico | Seleções e tela pequena conferidas; relógio histórico continua pendente |
| Séries / relações | Snapshots QA → fórmulas/fechamentos/pares → cinco visões existentes | Fractais, ZigZag e provisórios exercitados; eventos contados nos pares válidos e no holdout |
| Reprodução | Fixture v3 antiga → importação/validação/exportação | Bytes preservados, inclusive a semântica histórica de volume ausente; regressão própria |
| Recursos | Base → medição existente em segundo plano → tabelas/estado | Tela carregada; nenhuma limpeza ou otimização foi executada |
| Falhas e respostas antigas | Entrada inválida/seleção trocada → diagnóstico/descarte → contexto preservado | Regressões Tk e serviços; não equivale a interrupção real do terminal |

### Mapa compacto de problemas

| Impacto | Evidência e causa | Correção / aceite |
|---|---|---|
| Alto: preço inválido aparentava estar pronto | `high=NaN` em três barras retornava `has_problems=False`; comparações com NaN não acusavam incoerência | Checker mede não finitos, ausências, ordem, horários e volumes. Análise recusa preços/cronologia inválidos antes de calcular e limpa resultado anterior. Regressões e GUI QA |
| Alto: qualidade induzia sobrescrita sem diagnóstico de sessão | Texto mandava reextrair com sobrescrita para qualquer intervalo sem barra | Explica candidatos a lacuna e pede conferência de sessões/feriados/blocos; sem preenchimento, exclusão ou correção automática |
| Alto: mudança do diagnóstico poderia invalidar snapshots v3 | Validação real do livro detectou igualdade estrita da qualidade congelada | DTO histórico de `data_eligibility.assess` preservado; checker atual é usado na consulta. Fixture sintética gerada com código 5e8acf7 importa/exporta byte a byte; validação estrita de snapshots mantida |
| Alto: barras retidas pareciam muitas observações de pivô | `unique_source_pairs` contava fechamentos de barras, inclusive sem nova confirmação | Identidade por fechamento da confirmação; contagem distinta em pares válidos e holdout, com lag e exclusões. UI mostra separadamente barras/eventos. Último evento distingue origem, barra de confirmação e disponibilidade nominal |
| Alto: fuso histórico não comprovado | Configuração ativa declara offset fixo, sem zona; documentação oficial do SDK descreve timestamps UTC, enquanto conector e base usam convenção broker | Pendente de reconciliação no pilar de Dados, sem migração presumida. A UI agora expõe a configuração e seu limite; nenhuma análise real recebe aceite temporal por esta auditoria |
| Médio: cobertura não equivale a arquivo restaurado íntegro | Quatro `quick_check=ok`; catálogo recuperado contém referência AACT sem tabela e séries esparsas | Inventário verificável local, sem reconstrução fictícia. Distinguir ausência de origem, falha de coleta e mercado fechado depende do contrato/fonte |
| Médio: UI ainda apresenta pausas | Heartbeat da rodada completa teve atraso máximo de cerca de 2 s; livro ~9,9 s, fontes ~2 s no host sob carga | Medição registrada, sem alegar ganho de desempenho. Próximo recorte: desenho incremental pelas estruturas existentes, preservando seleção/resize/tema |

### Dados e análises

A consulta atual encontrou 127.705 barras nativas na base ativa, em sete TFs,
e 24.479 barras M7/M9. A reagregação independente do M1, ancorada em meia-noite,
coincidiu em todas as 13.749 M7 e 10.730 M9: OHLC, volumes, número de grupos,
sem janela completa ausente ou barra armazenada sem suporte na origem.
Isso prova a agregação nessa convenção de relógio; não prova UTC, recebimento,
calendário oficial ou cobertura integral do provedor.

A base recuperada passou novamente em `quick_check` e manteve o hash durante
a leitura: 194.855 barras em quinze tabelas existentes e 3.605.060 ticks;
uma décima sexta referência do catálogo não tem tabela. O inventário privado
registra fonte, intervalo, atualização, nulos, não finitos, duplicações,
incoerências e intervalos maiores que o TF. Os bancos de pesquisa e QA também
foram lidos separadamente; tabelas sem mapeamento não são tratadas como vazias.
Não se publica o inventário privado como fixture.

Os pivôs continuam causais: os oráculos independentes por janelas/racionais e
as extensões de prefixo conferem confirmação tardia e ausência de repaint
retrospectivo. Provisórios continuam `UNCONFIRMED_EVENT`. A nova contagem não
altera parâmetros, limiares, correlações ou o corte 70/30: os testes de holdout
continuam por barras, explicitamente sem prova de independência dos eventos.
O valor retido não vira nova confirmação quando o fechamento da origem muda.
Relatórios antigos continuam abrindo sem inventar contagens ausentes.

O contrato temporal exige trabalho próprio: comparar epoch bruto do SDK,
representação no terminal e convenção gravada, em sessão própria com ownership;
qualificar mudanças históricas de offset antes de reinterpretar UTC. A referência
[oficial de barras](https://www.mql5.com/en/docs/python_metatrader5/mt5copyratesrange_py)
e a [de ticks](https://www.mql5.com/en/docs/python_metatrader5/mt5copyticksrange_py)
foram consultadas nesta rodada e não confirmam, sozinhas, o significado do
histórico já existente. Não assumir uma zona IANA nem trocar +3 por zero para
liberar uma análise.

### Medição e fechamento

Checker, três repetições na mesma massa sintética: mediana em 20 mil barras
44,02 → 47,19 ms; em 96.629 barras 167,86 → 162,06 ms. Variação sob carga,
sem alegação de aceleração. A consulta continua em segundo plano. Na GUI:
leitura da análise ~0,24 s, diagnóstico inválido ~0,22 s e cálculo QA com oito
canais/960 fechamentos ~2,79 s (inclui publicação; poll de 100 ms).

Critério de encerramento deste incremento: regressões de qualidade, compatibilidade
v3, contagem de eventos, gates oficiais e repetição pelo launcher sem erro;
fonte publicada e prévia atualizada com SHA remoto. O encerramento **do produto**
continua condicionado a relógio/proveniência reconciliados, classificação das
lacunas, qualificação de barras parciais legadas, jornada de extração/retomada
nativa própria e Windows físico. LAB-07 completo, SA-01/02 e aceite financeiro
não são concedidos por este recorte.

Oráculo numérico adicional nesta rodada: 1.611 barras H1 da base ativa foram
recalculadas por recorrência escalar, sem chamar `ewm` de produção. RSI e ATR
coincidiram com a variante legada (erros absolutos máximos 2,85e-14 e 1,74e-18,
mesma máscara de aquecimento). Oráculos independentes por janelas/racionais
reconciliaram 421 eventos de fractais e cinco de ZigZag nessa massa. Não é prova
de UTC nem de equivalência da semente inicial à dos indicadores nativos: RMA
legada usa semente EWM, distinção já reconhecida em `test_indicators.py`.
