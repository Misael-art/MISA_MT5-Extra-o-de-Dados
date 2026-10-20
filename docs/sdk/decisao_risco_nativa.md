# B06 — Decisão de risco nativa e SDK1

Esta é a ligação documental da tarefa B06 entre mt5x, HADOUKEN e a biblioteca
mt5x-safety. Não habilita o backend nativo de composição; T19.23/B08 continuam
pendentes. Não muda o schema `robot.json`1 ou os sidecars T19.2, não cria fórmula
Python de risco e não altera o instalador. `robots/safety_lib.py` existente é
quem lê/instala a versão declarada do clone: nenhuma atualização automática.

Origem do DTO/API: HADOUKEN `Safety/HDK_RiskDecision.mqh`, cópia versionada exata
em `mt5x-safety/Include/MT5X/Safety`. Contrato simples v1 com estado
aprovado/reduzido/negado, regra, instante, volume solicitado/final e risco na moeda
da conta. Motivo textual externo preservado. O adaptador usa as APIs existentes
BlockReason/Paused/TradingAllowed/CalcLot/CheckEntry; nenhuma matemática paralela.
Suporte base: uma entrada a mercado. Planos múltiplos/pendentes são recusados;
volume/orçamento do plano não podem aumentar. Uma aprovação não é fill/reserva.

Base efetiva integrada: nove arquivos H/master iguais a S/main1.3.0. As versões
locais1.4/1.5 são branches não publicadas e continuam preservadas, fora desta
integração. A adição B06 recebe versão1.6.0 para não reutilizar esses números;
é **1.3 + somente B06**, sem exposição/orçamento global1.4/1.5. Manifesto
`safety.json` contrato1 e único include_dir `MT5X/Safety` permanecem válidos.
Distribuir Contracts/ em outra pasta ainda requer a migração formal da B11.

Compatibilidade medida com o serviço existente: leitor SDK1 reconhece1.6;
primeira instalação em diretório temporário copia10 arquivos, segunda0. Os nove
headers antigos e fingerprints dos seis Experts permanecem idênticos, sem
repetição de campanha numérica. SafetySmoke compila0erros/avisos. O teste da API
nova e o QA nativo têm evidências próprias no HADOUKEN. O QA precisa de marcadores
nativos explícitos: estado completed da sessão e relatório sem negócios não
comprovam inicialização ou aceite operacional, sobretudo se o diário registrar
falha de autorização do agente Tester.

O próximo consumidor deve verificar B04/prazo/identidade/preço da decisão, enviar
só por SafeExecutor e capturar B07 para reconciliar o resultado. O piloto S09
exige SL/TP fixos efetivos e negócios; EMAATR com stop/trailing ATR não é S09.
A tela Relações continua sintética. Nenhum novo controle Default/Manual/preset
foi entregue por esta documentação; simulação Python, Tester nativo e execução
ao vivo DEMO são modalidades distintas. Pullback continua indisponível no novo
adaptador até T19.57; Windows físico permanece pendente específico.

PRs vinculadas: [HADOUKEN #20](https://github.com/Misael-art/projeto-indicadores-mt5/pull/20)
e [Safety #5](https://github.com/Misael-art/mt5x-safety/pull/5).
H: CI39346061822 aprovado na fonte `fad0acdb94f9916103b490d70c5ad3a9492b268c`;
fechamento apenas Markdown. S: fonte `2cc175fa9a43102879bfc7ad42b302bcc32cb499`.
Provas versionadas em H: `B06_DECISAO_NATIVA_20261019.json` e
`B06_SDK1_COMPAT_20261019.json`, com hashes e limites do ensaio.
Revisão técnica local concluída; integrar H→S→este vínculo documental após os
checks/revisões finais. Código Python idêntico a master528b211 e evidências
aprovadas da PR133 reaproveitadas; não há nova suíte/workflow por estes Markdown.
Não afirmar aceite operacional ou funcionalidade de UI a partir deste contrato.
