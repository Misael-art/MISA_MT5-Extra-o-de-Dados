# Prompt — agente do Expert Super ATR com confirmação entre TFs

Você implementará uma frente independente do **Expert Super ATR** no HADOUKEN,
integrada ao App Misael-art/MISA_MT5-Extra-o-de-Dados. O proprietário quer um
consumidor com confirmação entre tempos gráficos, testável pelo atalho central.
Há um agente do Laboratório e **outro agente já responsável pelo pilar de dados**.
Não assuma suas posses nem implemente outro cálculo/store/executor concorrente.

## Base e referências que não podem se perder

App: obter `codex/lab-superatr-frentes` e ler este prompt e o
[backlog compartilhado](../propostas/LAB_SUPER_ATR_PENDENCIAS_20261024.md).
Base documental anterior `af717fa90bd4d514fb226622862bca572ae41f88`, runtime de
Laboratório `7c66feda06eb065b5cd591fd22ba5b602cffa624`, PR210 em rascunho.
Dependências AppPR207/208; não presumir integração em master.

Framework: Misael-art/projeto-indicadores-mt5, closure HPR75 na branch
`codex/ai-workflows`, commit `b730ed4a8df74f5f20eb6e289798d0232c370994`.
Antes de escolher a base, confira a composição atual e a frente paralela do
painel. Preserve avanços posteriores revisados, sem substituir arquivos completos
por versões de bases antigas. Branch/worktree próprias `codex/`; não editar a raiz
com WIP nem o branch de outro agente.

Leia AGENTS de ambos os repositórios, seus planos/continuidade e:

- App `docs/sdk/super_atr.md`, `docs/sdk/alvo_probabilistico.md`,
  `docs/agente.md`, `docs/TESTAR_DESENVOLVIMENTO.md`, contratos B e SDK/presets.
- H `Include/Framework/HADOUKEN/Indicators/HDK_SuperATR.mqh`,
  `Indicators/HADOUKEN/Study/HDK_SuperATR.mq5` e
  `Include/Framework/HADOUKEN/Safety/HDK_StudySuperATR.mqh`.
- H `Documentation/HADOUKEN_2/propostas/G19_9_SUPER_ATR.md`: memória canônica,
  matemática e hipóteses; G19_13_LAB_COMPARACAO_MULTITEMPOS.md,
  G19_15_INDICADOR_E_ESTRATEGIAS.md e G19_16_ALVO_PROBABILISTICO.md na mesma pasta.
- H `Documentation/HADOUKEN_2/estudos/`: SUPER_ATR_20261023,
  SUPER_ATR_ESCALAS_MANUTENCAO_20261023, SUPER_ATR_DESACORDOS_20261023 e
  SUPER_ATR_GESTAO_20261023, começando pelos LEIA_ME/CONTINUIDADE e protocolos.
  Os resultados negativos e dados já explorados permanecem registrados.

## Matemática existente a preservar

Arrays cronológicos. TR inicial=H−L; demais TR=max(H−L, |H−Cprev|, |L−Cprev|).
Período p; média0=SMA aritmética,1=EMA com primeira SMA completa como semente e
alpha2/(p+1), **não RMA de Wilder nem EMA semeada no primeiro fechamento**.
Distância d×ATR; base alta=C−distância, baixa=C+distância. Compare C atual à
**linha anterior antes de recalcular a base**: alta vira baixa se C<linha anterior,
baixa vira alta se C>linha anterior; igualdade conserva. Na virada reseta na base
do novo lado; sem virada retém max(base,anterior) na alta e min na baixa.
Lado inicial explícito, alta por padrão; zeroATR válido. Copiar a política real de
HLC/EMPTY/overflow/sufixo inválido e janela/seed, sem tolerância inventada que mude
o gatilho. Não presumir que seed fixa equivale à janela móvel após restart.

Padrão visual existente: dez curvas d1…10, período20 e SMA0, pontilhadas, azul
claro→escuro conforme desvio, **diagonal ligando a virada**, fundo cinza claro
degradê opcional. A expansão de pesquisa para30 e ambição50 não estão entregues
como indicador nativo. Preserve o componente reutilizável e parametrizável;
ampliação de buffers tem recorte/medição próprios, sem misturar com ordens.
Diagonal não prova percurso intrabar nem cruzamento em outro TF.

## Congelar a estratégia antes de implementá-la

O pedido exige confirmação entre TFs, mas a estratégia operacional completa não
foi especificada. Reuse convenções explícitas da memória e registre as escolhas
abertas, em vez de converter hipótese em regra financeira silenciosa. Entregue
um contrato versionado de candidato de pesquisa: TF base/confirmadores, curvas
e parâmetros por TF; evento de entrada; alinhamento versus nova virada; ordem,
idade e validade da confirmação; instante de decisão e execução; SL inicial,
TP/saída oposta; lote/risco; posição limite, reentrada, pausa e dados inválidos.
Se faltar decisão indispensável, solicite somente essa definição e avance nos
testes/matemática independentes. Não escolha o melhor retrospectivo como default.

A hipótese d3 noM5 refletir rompimento emM15, d2 emM10 e d1 emM5 **não é
identidade matemática**. Calcule os TFs efetivos e confirme apenas a última barra
fechada/disponível de cada um, com calendário/fuso/âncora explícitos. Recuse
confirmação ausente, revisada incompatível, em formação ou vencida. Documente o
desempate de fechamentos simultâneos. Nunca preencher o início de M15 com seu
fechamento futuro, nem contar várias vezes o estado retido de um mesmo candle.
TFs sintéticos comoM7/M9 vêm do contrato de Dados, não de agregação escondida no EA.

Concordância acima80% foi exploratória, não acerto de trade. G19.15 não aprovou
candidato após os custos assumidos. Preserve isso; registre tentativas/controle/
OOS pelos mecanismos existentes. O alvo probabilístico G19.16 já tem modelo e
gestão experimental separados; não recrie nem ligue parcial80%/saída90% como
consequência automática deste Expert. Pressão/consenso não são probabilidade.

## Implementação e fronteiras

1. Novo consumidor usando o Super ATR canônico e motor/host/gestão existentes;
   sem duplicar fórmula, outro OrderSend ou novo gestor. Reutilize RiskGuard,
   SafeExecutor, PanelHost, regras de lotes e stop. Pausa recusa novas entradas,
   preservando proteção/gestão. Escopo magic+símbolo, limites, stop e kill mantidos.
2. Valide preço, point/tick, volume, stops/freeze, lado protetor e TP preservado
   no fluxo real do executor. Proposta de linha não é SL aceito. Estado observado
   de execução prevalece ao bool/LastRetcode; não repetir ordem após resposta
   aceita/parcial/incerta sem a política existente de reconciliação/trava.
   Restart/múltiplas instâncias/retentativas não ganham garantia sem teste próprio.
3. Primeiro consumidor **Tester+DEMO**, gate anterior à inicialização operacional.
   Registre o que é compilação, shim, Tester e execução DEMO observada. Nenhuma
   promoção REAL, relaxamento de consentimento ou pin para fazer UI passar.
4. Dados é dono de SA-02: materialização/coluna ou projeção Super ATR consultável,
   parâmetros/versão/seed/janela, fonte/manifesto, clocks/available_at/revisão,
   estado/nível/ATR/qualidade. Combine o envelope existente BD06/07/18 com seu
   dono; não criar tabela/store por Expert ou modificar schema na raiz. Comparar
   Python/MQL só com fórmula, janela e arredondamento declaradamente iguais.
5. Preset/perfil App aditivo, inputs tipados, versão/fingerprint/closure completos
   e validação antes de abrir editor/executar. Preserve perfis/pins antigos e
   biblioteca de risco atual. Os seis perfis automáticos encontrados na prévia
   estavam bloqueados; este trabalho não os libera incidentalmente. Safety
   publicado só muda em tarefa própria. CLI/SDK usa o mesmo compiler/serviço.

## Testes e integração exigidos

- Baseline e regressão proporcional; corpos MQL reais em shim para TR/SMA/EMA,
  seed, warmup, catraca, ambas as viradas, igualdade, ATRzero, gaps, especiais,
  overflow, revisão e replay/restart por janela declarada. Não apenas regex.
- Teste causal multi-TF: barra superior incompleta, confirmação anterior/vencida,
  dado atrasado/revisado, gaps, simultaneidade e evento deduplicado. Alterar o
  futuro não altera decisão anterior; confirmação não usa o candle atual.
- Consumidor real com plataforma simulada: risco/pausa/magic/lado, recusa de
  ordem/stop, fill parcial/incerto, gestão preservada e nenhuma duplicação.
- MetaEditor0/0 e Tester DEMO com lease/sessão/processos próprios; capturas e
  recibos/log/histórico reconciliados, sem confundir desaparecimento com stop
  executado. Sem kill global de Wine/MT5/ponte nem processos de outro agente.
  Não enviar ordens pela conta/sessão do usuário para fazer QA.
- Visual independente de execução: dez curvas e diagonais coerentes com o
  cálculo. Integração CLI/preset/PLAY/REPLAY respeita a capacidade efetivamente
  qualificada; robô oculto por padrão quando aplicável, painel manual visível.
- Commit/push/PR por tarefa, evidências com fontes/params/dados e limitações.
  Entregue hashes congelados ao integrador; App/H/SDK/catálogos compartilhados e
  preview são compostos serialmente, sem sobrescrever WIP do Laboratório/Dados/
  painel. Atualize plano/continuidade pelo dono central.
- Instale a composição verificada no destino estável do atalho central/menu,
  preservando config/.env/base/venv, canal aceito e rollback. Verifique pela
  própria entrada do usuário e informe o runtime realmente carregado. Monitor
  de lançamento, handheld/dock/DPI e Windows físico têm verificação delimitada.
- Não declarar lucratividade, paridade Profit, dados certificados ou operação
  aprovada por compilação/QA visual. Limpar somente seus temporários, preservando
  fontes, evidências úteis, dados e trabalhos alheios.
