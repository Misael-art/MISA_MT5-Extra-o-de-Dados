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
