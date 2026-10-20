# T20.22 — consumidor inicial do percurso S09 na UI

A aba Robôs mantém Tk/ttk e o gerenciamento de sessões existente. No card
`HDK_S09_Referencia`, **Default / Manual**, BACKTEST e REPLAY abrem o mesmo editor.
Escolha Ativo / TF no card antes de abrir. O recorte usa o perfil S09 aprovado da
T19.23 e os serviços das T19.25/26/29/50/55; não redefine contratos financeiros.

## Comportamento

- Default do código somente para leitura; “Salvar como Manual” exige nome e mostra
  o filename portátil antes de salvar. Seleção, identidade e revisão vêm do banco.
- Manual permite períodos EMA, aquecimento, SL e TP fixos. Salvar cria revisão;
  erro conserva os campos e a última revisão salva. Alterações não salvas bloqueiam
  troca de seleção, clonagem, exportação e execução. Reabrir recupera o salvo.
- Renomear conserva identidade/revisão/evidência operacional. Exportação publica
  pacote canônico pelo serviço existente, sem sobrescrever arquivos divergentes;
  importação aceita somente o Manual verificável deste contexto.
- Datas e capital são explicitamente simulados. O modelo inicial é cada tick;
  cobertura de ticks reais é lida do relatório, nunca presumida pelo seletor.
- Backtest e Replay nativos carregam o pacote exportado. A sessão é registrada em
  `CardControls.sessions`: STOP, fechamento da aplicação, lease e isolamento usam
  os mecanismos existentes. Fechar apenas o editor durante a execução o oculta;
  reabrir o card recupera o editor e Cancelar afeta somente sua sessão.
- A conferência exibe inputs esperados/lidos, negócios com SL/TP, sinais com origem,
  confirmação e disponibilidade no relógio do servidor, resultado e primeira
  divergência. Consultar resultados salvos usa revisão/contexto/execution_hash.
- Zero negócios é somente ensaio de contratos. Replay só indica percurso conferido
  com negócios, inputs/modo nativos e livro/CSV/saldo conciliados e comparação exata
  com backtest da mesma revisão/contexto. A simulação Python é outro motor.

Risco/proteções são somente informativos: o perfil conserva até 0,5% por saldo,
SL obrigatório e uma posição; parcial, BE, trailing e PLAY ficam indisponíveis.
Pullback continua bloqueado no novo adaptador até T19.57. Execução ao vivo DEMO
não é comprovada pelo Tester. Não há execução REAL, ajuste de limites, presets
automáticos ou promoção estatística.

## Verificação e limites

Seis testes de widgets reais com serviços reais de presets (fronteira MT5 falsa)
cobrem nome, edição, revisão, reabertura, exportação, preservação do Default,
offline, seleção alterada, reutilização da janela e zero negócios. Os controles
anteriores e seus consumidores são verificados proporcionalmente à mudança.
A prova nativa do Manual/Replay está em
`docs/evidencias/T19_29_MANUAL_NATIVO_20261020.json`: dois negócios protegidos em
cada passagem, dez inputs, Visual efetivo e nenhuma divergência do livro/sinais.
O resultado foi reaberto pela consulta da UI na base de teste. As duas falhas de
inicialização visual em Xvfb estão preservadas; o replay funcionou na tela :0
usada pelo Wine. O backtest aprovado não foi repetido. O visualizador concluído
foi encerrado pelo mecanismo próprio e o caminho normal recebeu a chamada
correspondente na T19.29, sem recalcular os negócios.

O ensaio exercitou UIManager, card e diálogo reais, serviços reais, banco privado e
Session/MT5 reais. Os seis testes de widgets usam MT5 falso e não substituem essa
prova. Arquivos da sessão/CSV também são recuperados pelo run salvo depois de
reabrir o editor, sem depender de um objeto Session ainda em memória.

Este é o acesso inicial ao primeiro percurso: não conclui o layout e todos os
modos/gráficos/inspetores de T20.22/23, Analytics, Relações ou o catálogo inteiro.
Windows físico continua pendente: foco/rolagem/DPI, exportação em NTFS e visual
nativo; CI Windows simulado/automatizado não substitui esse ensaio.

A consulta real posterior expôs um callback de reflow com `wraplength` vazio no Tk. A conversão trata esse valor como zero; os seis testes agora capturam exceções de callback em vez de apenas ler stderr. Essa correção é de UI e não invalida cálculos/negócios nativos registrados.
