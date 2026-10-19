# Retomada do desenvolvimento do extrator

**Diretriz do proprietário em 2026-10-18:** priorizar extração, atualização, armazenamento,
consulta/exportação e recuperação dos dados. Preservação da base e rigor analítico são
condições do desenvolvimento, com verificações proporcionais; não constituem uma frente
de pesquisa ilimitada. Esta diretriz ajusta a ordem do
[prompt consolidado](PROMPT_CONSOLIDADO_DESENVOLVIMENTO_MT5.md), preservando segurança,
contratos, testes obrigatórios e as funcionalidades futuras já planejadas.

## Avaliação do trabalho e ponto de retomada

A atividade identificada nos registros é **T13.3b**, produtores Analytics incrementais,
e não T20.13. Há entregas úteis de checkpoints, retomada, invalidação, continuidade de
estado e restrição das leituras. O erro inicial de identidade entre configurações e
timeframes também foi corrigido na T13.3. Esses trabalhos devem ser aproveitados.

O registro de fechamento da T13.3b informa 99 testes focados, 22 PostgreSQL e sete
verificações de limites/retomada aprovados. O benchmark corrigido informa 25.398 registros
iguais, inclusive hashes. A divergência anterior de Bollinger era de até aproximadamente
1,87e-14; além de valores, atingia identidades usadas na reexecução. A correção é útil,
mas os resultados corrigidos não justificam novas rodadas sem defeito ou requisito novo.

No benchmark corrigido, `compute` levou 12,187 s na reconstrução e 1,521 s na atualização,
em uma medição local. Preparação, aquisição e resampling ficaram fora da janela; a memória
praticamente igual não comprova sua redução. A tentativa adicional EURUSD/AGG interrompida
permanece inconclusiva. A T13.3 anterior tem evidência própria; não confundir as duas.

A [PR #116](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/116) foi informada
pelo proprietário como rascunho, com uma suíte completa em execução a 14%, sem falha até
aquele ponto. Os registros locais apontam `logs/t13-3b-integracao/`, worktree `pr/`, branch
`codex/t13.3b-integracao-f19` e commit `40fbef5`. São referências para conferir a situação
atual, não uma aprovação da suíte ou confirmação de merge.

O workspace principal contém várias mudanças e arquivos novos, enquanto seu HEAD ainda
é `489591c`. Código aplicado ao diretório e integração Git/PR são estados distintos.
Não usar `reset`, checkout destrutivo, limpeza ou cópia indiscriminada para conciliá-los.

O código de `analytics/resampling.py:resample_symbol` carrega M1 sem recorte e só depois
aplica limites na derivação. Isso confirma leitura integral nessa etapa, mas não demonstra
sozinho que ela seja o maior gargalo. `extract`, `update`, `export` e `analytics features
compute` têm percursos separados na CLI. Falta comprovar a melhoria do conjunto do
produto, não refazer todas essas funções porque já existem.

O prompt anterior enfatizou qualificação e preservação sem um limite suficientemente
claro de investigação. A correção de direção é encerrar T13.3b com as provas obrigatórias,
aproveitar suas entregas e medir uma jornada curta do extrator antes de expandir Analytics.

Fontes locais: [aceite T13.3](../analytics/T13.3_PERFIS_REGIMES.md),
[store T13.2](../analytics/T13.2_FEATURE_STORE.md),
[histórico de continuidade](../continuidade.md), código/fixtures existentes e relatório
T13.3b na worktree indicada. Os resultados da candidata foram lidos, não reexecutados
por esta avaliação; sua suíte global permanece sob responsabilidade da execução existente.

## Tarefa seguinte do extrator

**EXTR-01 — Qualificar o ciclo do extrator e corrigir a primeira lacuna comprovada.**
Estado: planejada, prioridade atual. Não renumera F19/F20 nem implementa um novo extrator.
Depende do fechamento documentado da T13.3b apenas para consumir sua candidata; partes
independentes podem ser inspecionadas sem alterar os arquivos sob teste de outro agente.

Arquivos de referência: `historical_extractor.py`, `mt5_connector.py`, `database_manager.py`,
`data_exporter.py`, `cli.py`, UI de extração/inventário e, quando fizerem parte do percurso,
`analytics/resampling.py`/`producers.py`. Confirmar donos e listar o conjunto mínimo de
arquivos antes da edição. Não alterar todos esses módulos por conveniência.

Passos: conferir a execução existente; escolher uma jornada curta; medir cada etapa;
reproduzir a primeira falha ou custo evitável; fazer a menor correção coerente; executar
testes apropriados e demonstrar o percurso. Havendo várias lacunas independentes, separar
as demais no backlog, sem fazer um PR monolítico. Se o percurso já passar e não houver
gargalo relevante, entregar a evidência e concluir sem fabricar uma refatoração.

Aceite: extração persiste dados válidos, atualização repetida é idempotente, falha/retomada
preserva blocos confirmados, banco reaberto consulta/exporta o mesmo recorte e UI/CLI
informam sucesso/parcial/falha corretamente. Medições separam fonte, escrita, derivação,
Analytics opcional e exportação. Critérios de precisão e recuperação estão no prompt abaixo.

## Prompt para enviar ao próximo agente

```text
Assuma a retomada do mt5x priorizando o produto extrator: extrair, atualizar, armazenar,
consultar/exportar e recuperar dados corretamente, com desempenho medido e UI coerente.
Esta é a prioridade atual do proprietário. O restante de F19/F20 continua no roadmap,
mas não exige concluir toda a precisão estatística antes de entregar esse ciclo.

CONTEXTO E LEITURA

Trabalhe no repositório mt5x disponibilizado. Leia AGENTS.md e referências, esta diretriz
docs/propostas/PROMPT_RETOMADA_EXTRATOR.md, as entradas atuais de docs/continuidade.md,
docs/ROADMAP.md e docs/PLANO_DE_TRABALHO.md. Leia os contratos e fichas do recorte escolhido;
consulte o restante do prompt consolidado quando necessário, sem reiniciar todo inventário.
Use RTK e preserve trabalho do usuário. Não use reset/clean/reversão nem copie a árvore
inteira de outra worktree. Não crie outro store, produtor, extrator, fila ou backup manager.

FECHAR T13.3b SEM REINICIAR A INVESTIGAÇÃO

A PR #116 é o ponto de retomada: https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/116
Último estado informado: rascunho; uma suíte completa a 14%, sem falha registrada até ali.
Artefatos locais: logs/t13-3b-integracao/, worktree pr/, branch codex/t13.3b-integracao-f19,
commit de referência 40fbef5. Revalide SHA/processo/log/resultado atuais, sem confiar no
percentual antigo. Checkpoint contém campos históricos; prevalece resultado final da
execução identificada. PID reutilizado não comprova que seja o processo original.

Não inicie outra suíte completa, benchmark, PostgreSQL ou validação EURUSD/AGG apenas
porque começou outro agente. Não altere os fontes usados na suíte em andamento. Enquanto
outro agente for dono dessa execução, limite-se à leitura e a trabalho independente isolado.
Não termine processos alheios. Após a transição de responsabilidade, observe a execução
existente e registre seu código de saída, resumo e SHA. Se encerrou sem resultado,
registre inconclusivo; nova execução só com motivo concreto e plano para corrigir a causa.

Há 99 focados, 22 PostgreSQL, sete de limites/retomada aprovados e benchmark com 25.398
registros iguais, inclusive hashes. A investigação numérica foi corrigida em 1.2.1;
não refaça oráculos/variações/novas sementes sem uma falha nova. A validação adicional
real interrompida conserva a pendência, sem converter todo o trabalho em reprovado.

Se a suíte final/CI exigido passar, feche o recorte de implementação e seu relatório,
seguindo as autorizações de publicação/integração existentes. Não faça merge nem marque
integração concluída sem o fluxo aplicável. Se falhar, reproduza a falha relevante,
faça correção limitada, rode o teste afetado e os checks obrigatórios do código alterado.
Se o impedimento for externo e não houver correção segura neste escopo, entregue o estado
parcial e o próximo passo. Nem aprovação por conveniência nem investigação sem fim.

ENTREGA SEGUINTE EXTR-01

Use a ficha EXTR-01 desta diretriz, uma tarefa por PR. Prepare o ambiente e linha de base
conforme AGENTS, aproveitando resultado vigente somente se suas regras permitem e se o
mesmo código/ambiente estiver comprovado. Não use um resultado de outra árvore como passe.
Meça uma jornada pequena e reproduzível antes de escolher a correção. Comece com um ativo
disponível, M1 e um intervalo histórico suficiente a alguns blocos; acrescente segundo
ativo/timeframe apenas se exercitar um caso distinto necessário. Não rode anos/todos
os ativos para comprovar uma atualização curta.

1. Extraia para banco de teste gravável com o conector real MT5 em ambiente DEMO, quando
   disponível. Confira conta/servidor sem divulgar credenciais, intervalo, fuso, OHLCV,
   volume/spread e origem. Não sintetize candle para preencher ausência de negociação.
2. Atualize sobre esse banco com sobreposição prevista. Repetir o mesmo intervalo não
   duplica timestamps nem muda valores sem revisão da fonte; anexar barras processa o
   necessário. Falha de rede/parcialidade aparece como tal, sem sucesso inventado.
3. Exercite uma interrupção/erro controlado e retomada usando fakes ou recurso de teste
   próprio: blocos confirmados permanecem, pendentes são recuperados. Não derrube o MT5
   ou processo de terceiros para provar recuperação.
4. Feche e reabra a conexão/banco, consulte o intervalo e exporte por um formato existente.
   Confronte contagens, chaves/timestamps, valores relevantes e ausência; falha não retorna
   arquivo aparentemente válido. Teste restauração em destino isolado pelo recurso já
   existente quando aplicável; não crie outra estrutura de cópias para essa prova.
5. Se barras derivadas/Analytics fizerem parte do percurso, meça essas etapas separadas
   e o tempo total. O benchmark T13.3b mede compute, exclui aquisição/resampling e não
   demonstra economia de memória. Não o apresente como velocidade do extrator completo.
6. Confira a aplicação real: selecionar ativo/TF/período, extrair/atualizar, progresso,
   falha/cancelamento/retomada e consulta/exportação disponíveis. Ajuste apenas a UI do
   recorte; não redesenhe Laboratório/gráficos nem migre toolkit para fechar EXTR-01.

Escolha a primeira falha ou gargalo medido e corrija o mínimo necessário. O resampling
atual lê M1 integralmente; se isso dominar a atualização, limite leituras/reprocessamento
com índices/intervalo/estado existentes, respeitando sessão, aquecimento, correções antigas
e dependências recursivas. Não cortar contexto arbitrariamente para parecer incremental.
Se uma correção exige reconstrução maior, declare-a como fallback e meça separadamente.
Outras falhas independentes entram em tarefas posteriores, sem um patch para todo o app.

PRECISÃO E PRESERVAÇÃO PROPORCIONAIS

Bloqueiam a entrega: perda/duplicação/corrupção, timestamp/fuso/intervalo errado, volume
ou preço persistido incorretamente, leitura inconsistente, checkpoint que perde dados,
sucesso falso, valor futuro numa decisão e divergência que muda categoria/regra/hash
contratado ou resultado além da tolerância definida. Não relativize esses casos.

Na comparação de origem e persistência, mantenha fidelidade dos campos do provedor e
precisão de armazenamento; não arredonde OHLCV para conseguir igualdade. Para floats
analíticos derivados, use tolerância já especificada e justificada pela escala/fórmula.
Identidades e hashes continuam determinísticos conforme o contrato. Diferença já corrigida
ou dentro do contrato sem impacto não vira nova investigação. Não amplie tolerância,
troque fórmula/teste ou normalize hashes apenas para obter verde; mudança necessária
de contrato deve ser explícita, versionada e tratada separadamente.

Base de teste precisa ser gravável para demonstrar extração/persistência. Proteja a base
do usuário com destino isolado e cópia consistente inicial quando necessária; reutilize
esse ambiente. Não exigir que o banco de teste conserve o hash após gravações esperadas.
Não copiar/hashear toda a base e todos os fontes a cada comando. Confira configuração,
destino, esquema e amostras/contagens do escopo; fingerprints integrais só quando uma
operação ou contrato realmente os exigir. SQLite em uso exige snapshot/cópia consistente,
não cópia ingênua de arquivo/WAL ativo. Não escrever na base principal sem necessidade
e autorização aplicável; nunca apagar dados para recuperar espaço de testes de terceiros.

CRITÉRIOS DE CONCLUSÃO E LIMITES DAS RODADAS

Antes de executar, liste em poucas linhas: tarefa, defeito/risco, arquivos, cenários,
medidas e condição de término. Plano curto seguido de execução; não outro inventário geral.
Durante implementação use testes direcionados. No fechamento, rode a suíte/doctor e
checks obrigatórios conforme AGENTS; ShellCheck somente se alterar .sh. Não pule, apague
ou desative testes. Uma suíte em andamento não é falha por demorar nem motivo de cópia.

Depois de um resultado aprovado, repetir/ampliar só por código alterado, falha nova,
cobertura obrigatória ausente ou evidência contraditória. Indique o gatilho antes da
nova rodada. Lentidão requer diagnóstico de progresso/recursos antes de reinício.
Se duas tentativas falharem pela mesma causa ambiental e não houver correção nova,
entregue o impedimento e o que já foi validado; não continue reconstruindo ambientes.
Isso limita repetição sem aprendizado, não autoriza ignorar defeito material.

Registre antes/depois tempo total e por etapa, linhas lidas/gravadas/reprocessadas e
memória/I/O quando disponíveis, em condições comparáveis. Não impor ganho percentual
arbitrário nem alegar performance global com etapa omitida. Preserve o resultado ruim.
Se tudo estiver correto e sem gargalo relevante, encerre a prova sem refatoração artificial.

MT5 E CONTINUIDADE DO ROADMAP

Extração histórica real é leitura pelo terminal MT5/ponte; mercado fechado não exige
abrir ordens nem usar replay para baixar histórico disponível. Falta de feed/credencial
mantém só a prova nativa pendente; fakes comprovam estrutura, não acesso real ao servidor.
Para futuras tarefas operacionais de Expert, mantém-se replay nativo/Strategy Tester
visual em ambiente DEMO aceito fora de pregão; execução ao vivo DEMO nos limites vigentes
apenas quando necessária. Nunca conta REAL, aumentar limites ou contornar Safety.

Concluída a cadeia do extrator e a otimização vigente, retome F19/F20 por dependências
reais, incluindo presets/UI quando elegíveis. Não iniciar agora T13.4, DSR/PBO, FRP,
Ultra, carteira ou novos experimentos numéricos só por serem a próxima linha do backlog.
Não excluir essas frentes; manter pendências e prioridades explícitas. Windows físico
é pendência delimitada, não bloqueio de toda entrega Linux. Nenhuma revisão financeira
ou prova de lucro é requisito para demonstrar extração/armazenamento corretos.

ENTREGA

Atualize o painel e a continuidade sem sobrescrever relato concorrente. Informe: tarefa,
correção/evidência concreta, arquivos, testes/saídas/SHA, jornada via CLI/UI, medidas do
extrator e limites, estado da PR #116 e próximo recorte elegível. Relatório curto com
provas relevantes; logs/checkpoints em destino próprio. Não parar em mais um planejamento.
Finalize ao satisfazer os critérios ou apresentar impedimento específico; não inicie nova
campanha sem uma necessidade demonstrada. Preserve código/evidências e feche seu recorte.
```
