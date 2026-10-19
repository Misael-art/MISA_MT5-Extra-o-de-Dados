# Prompt único — desenvolvimento integrado mt5x/HADOUKEN

**Prioridade retificada pelo proprietário em 2026-10-18:** para a retomada atual, use o
[prompt do extrator](PROMPT_RETOMADA_EXTRATOR.md). Fechar a execução existente T13.3b/PR116
sem duplicar suítes e priorizar extração, atualização, persistência, consulta/exportação
e recuperação, com critérios proporcionais. Essa diretriz prevalece sobre a ordem ampla
abaixo; o contexto F19/F20/UI e as guardas permanecem. Não reiniciar inventário nem
campanhas numéricas já resolvidas antes de entregar o ciclo do extrator.

**Versão consolidada de 2026-10-17, solicitada pelo proprietário.** Reúne o prompt
anexado e as retificações F19/F20/UI/validação. Substitui a necessidade de concatenar
mensagens ou os prompts parciais F19/F20; os planos continuam sendo a especificação.
Copie e envie ao agente **somente o bloco abaixo**. Ele não depende do histórico do chat.

Retificação incorporada: replay nativo pode concluir o aceite funcional histórico;
execução ao vivo DEMO só é indispensável para aspectos que dependem dela. Default,
Manual nomeado, lotes/parciais, robustez F20 e prioridades de UI estão incluídos.
Este arquivo é uma instrução para execução futura, não prova de implementação/ensaio.

```text
Você é responsável por desenvolver a evolução integrada do mt5x e do framework HADOUKEN,
completando as tarefas documentadas sem perder arquitetura, dados, segurança ou comportamento.
Produza entregas concretas, testadas, documentadas e revisáveis, uma tarefa por PR.
Não se limite a sugerir um plano quando a tarefa elegível já autoriza implementação.
Planejamento não comprova implementação; correção do software não comprova lucro futuro.

1. REPOSITÓRIOS, LEITURA E ESTADO REAL

Trabalhe na raiz do repositório mt5x disponibilizado nesta tarefa. Localize o clone
HADOUKEN e a biblioteca mt5x-safety pela configuração/documentação existentes; não fixe
caminhos pessoais no código nem nos documentos versionados. Antes de alterar cada
repositório, leia integralmente seu AGENTS.md e referências. Use RTK conforme instruído.
Preserve trabalho existente, inclusive alterações/documentos ainda não commitados.

No mt5x, leia integralmente, usando caminhos relativos à raiz:
- AGENTS.md e suas referências
- docs/continuidade.md
- docs/ROADMAP.md
- docs/PLANO_DE_TRABALHO.md
- docs/propostas/PILARES_DADOS_OPERACAO_PRESETS.md
- docs/propostas/CATALOGO_F19_INDICADORES_SETUPS.md
- docs/propostas/PLANO_F19_PILARES_PRESETS.md
- docs/propostas/ROBUSTEZ_PESQUISA_EXECUCAO.md
- docs/propostas/PLANO_F20_ROBUSTEZ.md
- docs/propostas/UI_ORGANIZACAO_GRAFICOS_PRIORIDADES.md
- docs/propostas/VALIDACAO_F19.md
- docs/sdk/contrato.md

Consulte F13/F15/F16/F17/F18 e épicos B/C/D/E/F/H/I/J conforme as dependências reais.
No HADOUKEN, consulte as instruções/contratos aplicáveis e os estudos originais:
ESTUDOS/indicadores.txt e
ESTUDOS/estrategias_trading_algoritmico_especificacoes_tecnicas_md.md.
O catálogo F19 identifica fontes, cobertura e lacunas. Não invente regras ausentes.
Não é necessário concatenar ou reler os prompts parciais F19/F20 para obter contexto.

Confira SHA, árvore, código, contratos e evidências. Um arquivo existente ou uma tarefa
documentada não prova integração/validação. Confronte snapshots antigos com o estado real.
Comece em T19.1 se o inventário estiver pendente; T20.1 estende esse mesmo mapa para
Q01–Q16, sem outro inventário. Se já estiverem entregues, use o mapa e a primeira tarefa
elegível, sem refazer trabalho comprovado. Registre o dono de cada extensão e seu consumidor.

Prepare o ambiente e execute a linha de base conforme AGENTS antes das alterações.
Confira espaço livre e use temporários próprios em disco quando necessário; não apague
artefatos de terceiros. Registre display/DPI/ambiente das verificações visuais.

2. OBJETIVO E PILARES

O produto é uma plataforma de pesquisa, construção, validação e acompanhamento de
estratégias/Experts MT5, com estes pilares integrados:

- Dados de mercado: histórico extraído, indicadores/estatísticas, sessões, cobertura,
  revisões e disponibilidade temporal explícitas.
- Dados de operações: backtest e histórico demo/real importado, contexto antes da entrada,
  trajetória observável, duração, MAE/MFE, eficiência temporal, eventos e ciclos.
- Operacional: entrada, orientação normal/invertida, lotes/exposição/parciais, alvos de
  ganho, stops/proteção, saídas temporais e filtros de horário/dia/frequência/indicadores.
- Estratégico: sinais/setups modulares HADOUKEN, separados de execução/gestão reutilizáveis.
- Presets: Default primeiro e quatro categorias de trabalho, com pacote/identidade e
  compatibilidade comprováveis.
- UI e robustez: experiência moderna/adaptativa, cálculos corretos, reprodução dos
  experimentos, fidelidade operacional, rastreabilidade, recuperação e desempenho medido.

Preserve a cobertura integral do catálogo: 18 indicadores distintos do TXT, métricas
temporais e 11 itens detalhados do Markdown. Os alegados 37/48 setups não enumerados e
fórmulas incompletas permanecem lacunas explícitas, sem estratégia fictícia para fechar lista.
Indicador de trajetória/tempo do trade depende da operação, não apenas de OHLCV.

3. ORDEM, ESCOPO E COMPATIBILIDADE

Uma tarefa por PR, seguindo passos/dependências/aceites da ficha. Não tente implementar
toda F19/F20 num único PR. PRs necessários em repositórios diferentes usam o mesmo ID,
com vínculos e ordem de integração. Uma mudança tem um dono; consumir/qualificar a
entrega anterior não autoriza reimplementá-la em paralelo.

Priorize cálculos/dados/fidelidade → reprodução/protocolo → ciclo completo com preset/UI
→ acompanhamento/recuperação → carteira em segundo momento. Recursos podem antecipar
lotes pesados. Primeiro percurso simples: estratégia/sinal de referência S09/T19.37,
Default → Manual nomeado → backtest completo → pacote → conferência/replay nativo → UI.
Não esperar todos os indicadores, Renko, book, objetivos automáticos, Ultra ou carteira.
T20.30 aceita esse Manual; T19.35 conserva o piloto ampliado com objetivos automáticos.
T19.27/28/52 consomem registro/protocolo T20.12/13 antes de concluir seleção automática.

Se um passo não corresponder ao código, pare aquele recorte, registre a divergência no
PR/continuidade e prossiga somente em trabalho independente autorizado. Não invente
regras, afrouxe critérios nem misture correções externas para encerrar a tarefa. Se a
primeira ficha elegível for documental, entregue esse escopo sem alegar implementação
ou executar ordens para forçar uma demonstração operacional inexistente.

Reutilize registry/store, ledger, fórmulas, backtester, importadores, sessões, fila/cache,
recursos, componentes visuais e Safety. Não copie Experts só para variar stop ou filtro.
Preserve contrato 1; extensão usa schema/sidecar versionado ou migração formal. Mudanças
de banco são aditivas, bases/presets antigos continuam abrindo e arquivos do usuário
não são sobrescritos. Configuração nova segue DEFAULTS/config.ini.example/fallback.
Caminhos portáveis, bootstrap só biblioteca padrão, dependências no arquivo apropriado,
segredos só no .env e fora de logs/manifestos/argumentos de comando. Preserve os limites
de importação de MetaTrader5/tkinter e demais regras técnicas do AGENTS.
Python não envia ordens. MQL5 opera pelo executor seguro e guardas aplicáveis.

4. PRESSETS, IDENTIDADE E POLÍTICAS OPERACIONAIS

Default é o primeiro preset de cada estratégia/versão: retrato dos padrões efetivos,
sem otimização, versionado e preservado. Reuse a opção de padrões existente; refresh
não substitui seleção explícita. Editar Default gera Salvar como Manual, sem sobrescrevê-lo.

As quatro categorias de trabalho são:
- Manual: usuário informa Nome do preset e valores; rascunho não significa validado.
- Defensivo: menor risco entre elegíveis, com patrimônio flutuante/custos/caudas,
  atividade e amostra suficientes; não escolher simplesmente deixar de operar.
- Maximalista (Fator de lucro): manter token/ID FatorLucro e PF definido após custos;
  não criar categoria duplicada nem substituir o objetivo por score oculto.
- Ultra: política causal inteira de regras por calendário/contexto, com prioridade,
  alternativa padrão, transições suportadas e rastreio. Comparar com Default/soluções
  simples e validar em backtest completo/OOS; não costurar vencedores retrospectivos.

Siga a convenção de nomes da especificação F19 §8.4 para robô/ativo/série/categoria,
nome do usuário no Manual, orientação/revisão/hash e proteção contra colisões. Preserve
nome legível e slug portável em catálogo/manifesto/importação/relatório. Nome não substitui
preset_id, config_hash ou hash da evidência; edição e renomeação têm os efeitos definidos
na especificação. Não renomeie presets legados nem reconheça categoria só pelo nome livre.
Gerar/importar preset não instala/inicia Expert nem concede autorização de operar.

Lotes/parciais são gestão operacional local, não trabalho adiado para a gestão da conta.
Reutilize sizing/parcial simples e amplie conforme §6.3/T19.56/57: fixo/por risco, volume
maior que um na unidade do ativo, adições separadas, etapas de redução por quantidade/%
com base inicial/saldo definida, mínimo/passo/máximo, margem, risco e proteção do saldo.
Não aumente volume para viabilizar parcela impossível. Rejeição/fill parcial/restart/ack
não podem duplicar fechamento nem ativar break-even como se uma parcial inválida ocorresse.
Concilie custos/PnL por evento e trade/ciclo; parcelas não são novos trades independentes.
Grid/parciais múltiplas/netting/hedging são capacidades distintas, só oferecidas quando
implementadas/testadas e com política de risco/reserva/ownership aplicável.

Inversão é opcional/experimental, desligada por padrão: compra↔venda antes do plano,
sem sinal→sem sinal; recalcular níveis, lado, volume/risco/custos. Não é reversão de
posição aberta. No Ultra, orientação efetiva é aplicada uma vez, conforme herança/regra.
Gestão global da conta é posterior, preservando desde já guardas agregadas exigidas por
múltiplas ordens. Não remova proibições de grid/martingale/preço médio sem política aprovada.

5. CORREÇÃO MATEMÁTICA, DADOS E PESQUISA

Para cada cálculo, declare fórmula/variante, parâmetros, unidade/moeda, aquecimento,
fonte, instante de disponibilidade, precisão/tolerância e política de ausência.
Use contas manuais pequenas, referência independente, invariantes e paridade Python/MQL5;
testar a implementação contra ela mesma não basta. Preserve o PF canônico T13.9:
ausência não vira zero, denominador ausente não vira infinito/teto artificial.

Confira bid/ask, pontos/ticks/preço/%/R/dinheiro, comissão/spread/slippage/swap suportado,
lote/passo/parciais/margem/exposição/câmbio e relógios/fusos/sessões. Distinga saldo encerrado
de patrimônio com posições abertas e drawdown; frequência em barras não prova extremo
intrabarra. T20.2/3 qualificam a referência e o risco, sem reescrever métricas antigas.

Separe contexto anterior, estado observável e desfecho posterior. Não use barra superior
aberta, pivô não confirmado, dado revisado sem disponibilidade ou resultado futuro em
decisão passada. Alinhamento entre ativos/TFs é causal. Rótulos maduros e ciclos
sobrepostos exigem tratamento temporal pertinente.

Re-simular as mesmas entradas/filtrar ledger/trocar PnL não equivale a novo backtest.
Se muda ocupação, sinal, oportunidade, capital, margem, lote, frequência, entrada ou
saída, reexecute a estratégia completa em sequência com custos e limites.

Aplique todos os requisitos Q01–Q16 da F20 no recorte correspondente:
1) referência de cálculos e patrimônio flutuante;
2) manifestos/revisões/snapshot consistente de dados e specs historicamente vigentes;
3) qualidade por capacidade, com ausências e origem observada/gerada/desconhecida;
4) compatibilidade por versão/ativo/corretora/conta/ambiente, sem herdar prova hedging em netting;
5) margem/custos/exposição conjunta, cenários adversos e capacidade por volume;
6) eventos intenção→risco→ordem→deal→posição→parcial/ciclo e primeira divergência material;
7) registro único de todas as tentativas, falhas/descartes e consultas à reserva;
8) compromissos entre candidatos elegíveis, preservando objetivos e categorias;
9) ablação/contribuição incremental e baselines comparáveis;
10) pacote reproduzível, estados por dimensão e invalidação dos consumidores afetados;
11) observação/forward e diagnóstico de custo/dado/operação/drift/variação;
12) falhas, reinício, timeout/eventos repetidos e reconciliação antes de novas ações;
13) pesquisa de carteira síncrona no segundo momento, com capital/margem/câmbio compartilhados;
14) UI investigativa com origem da decisão;
15) orçamento, cancelamento, retomada, cache versionado e isolamento dos jobs;
16) conformidade SDK, origem/permissões de terceiros, restore coerente e release qualificado.

Não usar propriedade/margem atual como fato histórico conhecido. OrderCalcMargin isolado
não considera toda exposição; OrderCheck não garante execução. Custo/impacto/fill sem
fonte suficiente é hipótese de cenário, sem alegação de capacidade certificada.
Tick volume não é volume negociado; book esparso não reconstrói fila. Falta bloqueia só
o consumidor incompatível. Ticks reais/Model4 podem incluir minutos gerados: registrar
cobertura demonstrável ou desconhecimento, sem inventar qualidade integral.

Protocolo de busca define orçamento, tentativas manuais/automáticas, custos/risco, folds,
seed, treino/validação/reserva e critérios antes de testar. Cache não apaga alternativas;
reexecução técnica idêntica não é amostra independente. Reserva usada para ajustar passa
a validação e não é teste final intocado; não recrie independência recortando o mesmo
histórico visto. Use purga/embargo/blocos quando pertinentes, sem fórmula universal cega.
DSR/PBO e métodos afins exigem seus pressupostos. Não impor que todo Monte Carlo por ciclo
seja pior, nem prometer zero falso positivo em todo ruído; medir calibração/intervalo/poder.
Mudança de critério é versionada/testada por I-13, sem afrouxar bateria para produzir vencedor.

Pacotes referenciam código/binário/Expert/Safety/framework/build MT5, dados/specs, parâmetros,
contratos/protocolo/seed/dependências efetivas e evidências. Compatível, estatisticamente
avaliado, funcionalmente validado e autorizado são dimensões diferentes. Não transferir
evidência entre versões/ativos/ambientes sem fundamento. Manter histórico de reprovações.
Timeout não autoriza reenvio cego. Pausar entradas, cancelar pendentes e liquidar posições
próprias são políticas distintas. Observação sem ordens/PnL virtual não são fills DEMO.
Drift isolado não prova perda de vantagem; não trocar/reotimizar preset silenciosamente.

6. UI MODERNA, ADAPTATIVA E PERFORMANCE

Entregue a UI exigida pela ficha junto da capacidade, consumindo DTOs/serviços reais.
UI-A01–A10 entram agora por capacidade; UI-P01/P02 (densidade alternável/perfis completos)
são posteriores; UI-X01 (candidato Lightweight Charts) é experimento futuro não iniciado.
FRP é ESTUDO FUTURO/P3: não implementar, minerar grupos ou torná-lo dependência atual.
Agentes autônomos de criação/promoção, reforço, reotimização contínua e alta frequência
também ficam posteriores; não antecipar experimentos por estarem documentados.

Preserve Tkinter/ttk/Matplotlib, tokens/componentes F17 e abas/ações existentes. Não instale
outro renderer/toolkit na tarefa atual. Organize o Laboratório em Analisar, Compor,
Validar e comparar; contexto compacto com estratégia/ativo/TF/preset/período/origem/modalidade;
editor recolhível por grupos; gráfico principal; tabela e inspetor sob demanda.
Rascunho e configuração do resultado são distintos; refresh não perde seleção. A seleção
de timeframe já existe em Ativo / TF: complete coerência sem seletor duplicado.

T20.22 define/entrega a referência de navegação/layout; T20.23 conecta inspeção temporal,
sem dependência inversa. Preço/eventos e saldo/equity/DD usam dados qualificados;
sensibilidade, relações e comparação de presets entram com seus serviços, sem bloquear
Manual por falta de todos os gráficos. Fixture provisória continua identificada.
Seleção tabela↔gráfico, cursor/zoom coordenados quando alinháveis, regra/contexto por
evento, ampliar/restaurar/exportar e detalhe por teclado. Mostrar unidades/moeda/fuso/n,
estados e motivo de indisponibilidade; selecionar linha não é habilitar regra.
Preserve pontos/gaps/extremos, zero e ausências; não suavize para falsear perdas/risco.
Seleção exploratória não redefine OOS/protocolo nem aplica preset automaticamente.

Nenhum cálculo financeiro nos widgets. Trabalho pesado fora da thread da UI, operações
Tk na principal, descarte de resposta de contexto antigo. Medir tempo/memória/I/O/resposta
antes/depois com dados e condições comparáveis; usar fila/cache/incremental/atualização
parcial quando fundamentados. Não prometer FPS nem alterar resultado por velocidade.
Valide UI real claro/escuro, fonte ampliada, teclado/foco/rolagem, 1920×1080, 1280×800,
900×800, 900×620, 820×800, DPI96/144 e escalas 125/150% quando disponíveis. Sem cortes,
sobreposições, controles inacessíveis, gráfico sem altura útil ou escala aplicada duas vezes.

7. VALIDAÇÃO NO MT5, INCLUSIVE FORA DO PREGÃO

Aplique docs/propostas/VALIDACAO_F19.md. Cada prova tem seu alcance:
- cálculos/estrutura/contratos/nomes: referências independentes, testes de núcleo/integração;
- utilidade estatística/seleção: backtest completo com custos/risco e protocolo temporal/OOS;
- apresentação: operar UI real e conferir valores/comandos/resultados com DTOs/evidência;
- comportamento operacional: observar e reconciliar o Expert no terminal MT5 nativo,
  mantendo o ambiente de conta DEMO, por replay visual ou execução ao vivo em DEMO.

Quando o ativo estiver em negociação, podem ser executadas operações de teste ao vivo
em DEMO dentro dos limites/guardas existentes. A autorização desta tarefa não remove
revisões e consentimentos implementados na UI. Nunca operar conta REAL.
Comprove/revalide conta, servidor, modalidade DEMO, símbolo/TF/preset, ownership, permissões
e limites antes de iniciar e antes de ação operacional; mudança de identidade interrompe
a ação. Se DEMO não puder ser comprovada, não envie ordens; delimite o ensaio impedido.
Use a menor exposição que exercite o cenário e seja permitida pelos limites existentes,
com stop protetor e identificação exclusiva de teste. Se o cenário exigir volume/risco
não permitido, delimite esse ensaio sem ampliar limites.
Não aumente limites, remova travas, adote/feche posições ou processos/arquivos de terceiros.

Quando o ativo estiver fora do horário de funcionamento ou indisponível online, utilize
replay nativo do MT5/Strategy Tester visual com histórico/especificação válidos, no ambiente
DEMO. Mercado fechado não impede concluir a validação funcional por replay. Essa prova
vale para os comportamentos históricos efetivamente cobertos e reconciliados; não exige
ordens ao vivo para todo aceite. Não abandonar a etapa nativa por haver mocks aprovados.

Exercite os comportamentos aplicáveis à tarefa/capacidade implementada: entradas, gradiente,
volume maior que um/parciais e saldo protegido, stops, alvos, trailing/break-even, saídas
temporais, filtros, inversão, regras/transições Ultra, limites, cancelamentos e encerramentos.
Confira também comandos/estados/resultados apresentados pela interface da aplicação.
Capacidade não implementada não vira aprovada por constar desta lista; registre seu escopo.

Antes do replay, liste eventos esperados, aquecimento e razão do período. Pode escolher
recorte curto com amostras que agilizem QA; amplie/use outro recorte se faltar cobertura,
incluindo casos negativos/perdedores. Não escolher somente ganhos nem relaxar risco para
forçar evento. Prefira ticks reais quando sequência intrabarra importar e houver fonte;
registre modelo/qualidade/limitações reais. Desenho na tela não prova ordem/acionamento.
Outro ativo de teste tem escopo próprio, sem trocar silenciosamente o símbolo original.

Mocks e testes Python complementam essa evidência nativa. O aceite funcional pode ser
demonstrado pelo replay nativo, sem exigir mercado aberto. Ordens do Tester são históricas,
não operações executadas na conta DEMO ao vivo; replay de QA não é OOS nem forward.
Aspectos que dependam especificamente de rede/latência/preenchimento ao vivo permanecem
como pendências delimitadas, com próximo ensaio DEMO claro, sem bloquear os demais aceites.
Se faltar histórico/infraestrutura para um ensaio, registre a limitação exata e conclua
verificações independentes; não invente execução ou declare concluído o que não foi observado.

Registre para cada ensaio:
- modalidade: replay histórico ou execução ao vivo em DEMO;
- ativo, corretora/servidor sem dados pessoais, tipo de conta, timeframe e período;
- modelo de ticks, qualidade/cobertura disponível e limitações;
- SHA/versão do Expert/framework/Safety/build MT5, preset/ID/revisão e parâmetros efetivos;
- dados/specs/hashes/protocolo/seed pertinentes;
- eventos e operações, tickets/IDs próprios quando pertinentes, horários, volumes,
  proteções, custos, resultados, logs/capturas e pendências;
- reconciliação com histórico/relatório produzido pelo MT5 e valores exibidos pela UI.

8. TESTES, ENTREGA E CONTINUIDADE

Execute os testes/doctor e ShellCheck se alterar .sh conforme AGENTS. Testes automatizados
não exigem MT5 real; fakes não substituem os ensaios nativos aplicáveis. Não desative,
pule/apague/relaxe testes para obter verde; preserve falhas/tentativas e diferencie
regressão, problema anterior e ambiente. Não apresente execução parcial como suíte completa.
Comandos Linux, usando RTK quando instruído:
rtk proxy .venv/bin/python -m pytest -q
rtk proxy .venv/bin/python -m mt5_extracao.bootstrap doctor
rtk proxy git diff --check
Use equivalentes Windows conforme AGENTS. Instaladores permanecem idempotentes e mensagens
em português. Windows/Linux são primeira classe: simule caminhos Windows em testes e
registre o que exige Windows físico no checklist, sem alegar prova que não ocorreu nem
bloquear desenvolvimento Linux por essa pendência.

Atualize painel de status, mapa de cobertura/donos, continuidade, contratos, documentação
e checklist Windows pertinentes. Estado só muda com evidência do recorte. Não declare
toda F19/F20 pronta nem ative uma capacidade incompatível porque outro cenário passou.
Não exponha segredos/número pessoal de conta/caminhos pessoais; artefatos brutos ficam
no destino próprio e relatório sanitizado referencia as provas necessárias.

Entregue: tarefa/propósito e mudança; arquivos/contratos; testes com resultados/saídas;
verificação visual e medições; evidência nativa replay/DEMO aplicável, cobertura e
reconciliação; limitações específicas; estado atualizado e próxima tarefa elegível.

Comece agora pela inspeção e primeira tarefa elegível. Avance até a entrega concreta
do seu recorte, incluindo validação pertinente, documentação e resultado revisável.
```
