# Inputs efetivamente carregados no Tester — T19.23 inicial

`robots.sessions.Session` reutiliza a instrumentação, compilação, perfil privado,
lease, cancelamento e relatório existentes. `input_receipt_fields` é uma lista
explícita dos inputs **públicos** de um perfil revisado; nunca descobrir/gravar
credenciais de Experts arbitrários. Tipos iniciais: inteiros, double/float finitos,
bool e string contendo somente identificador ASCII portátil. Enum, cor, datetime,
texto livre e tipos não declarados permanecem sem suporte neste recorte.

`expected_native_inputs` compara os valores reais após OnInit bem-sucedido, antes
do primeiro tick. Ausência, diferença, versão/sessão/ativo/TF incompatíveis ou
recibo incompleto falham com ação de correção. Só Tester DEMO; a opção é rejeitada
em PLAY e exige fonte instrumentável. Nenhum limite financeiro é modificado.

O recibo UUID UTF-16 tem versão 1, contexto, instante conhecido e valores lidos das
variáveis MQL5. Não é cópia dos parâmetros Python nem somente eco do hash.
O CSV é preservado no diretório próprio da sessão e seu SHA-256 no relatório.
Tabela aditiva `_native_input_receipts` vincula somente o run/diretório próprios;
reabrir `store.load_runs` recupera `native_inputs`. Regravação idêntica é idempotente;
outro recibo exige nova execução. Métricas, ledger e runs anteriores não mudam.

`preset_verified` exige comparação e cobertura de todos os inputs da fonte.
Lista parcial ou sem valores esperados fornece somente prova dos contratos
exercitados. `operational_acceptance` permanece falso: negócios, política de saída,
custos, contexto e reconciliação são verificados pelas tarefas consumidoras.
Um ensaio sem negócios não comprova S09 nem o percurso operacional.

O uso anterior, sem a opção, mantém geração, inputs, SDK 1 e comportamento anteriores.
Não implementa perfil S09, mapeamento da composição, compilador/nome do preset ou
UI. O adaptador dos seis Experts não é migrado; Pullback permanece bloqueado.
Windows físico continua pendente; os testes de CI não o substituem.

Evidência local: `docs/evidencias/T19_23_INPUTS_NATIVOS_20261019.json`.
124 testes focados aprovados; compilação nativa 0 erros/avisos, quatro inputs reais
carregados com valores distintos dos defaults, recibo reaberto do banco de teste,
recursos próprios liberados. Zero negócios, sem aceite operacional. PostgreSQL e
Windows simulado são cobertos pelos checks existentes da PR; Windows físico pendente.
