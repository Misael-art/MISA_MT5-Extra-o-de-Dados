# T8.7 — complemento de aceite operacional ao vivo DEMO

O proprietário solicitou execução ao vivo DEMO em20/10/2026. Esse aceite é próprio:
S09, Pullback Nativo e o adaptador piloto continuam com bloqueios explícitos de
Strategy Tester DEMO. Nenhuma dessas proteções foi removida. As provas históricas
anteriores e a mecânica T8.7 já registrada permanecem preservadas; ensaio sem negócios
não é aceite operacional de uma estratégia.

## Contexto preparado e limite atual

Expert existente EMA+ATR, EURUSD/H1, preset legado `HDK_EmaAtr_Conservador`:
risco0,25% por operação, perda diária1%, semanal2% e drawdown5%, sem mudar limites.
O caminho PLAY e seus arquivos de execução/UI são idênticos aos da master integrada
46f31b6; nenhum runtime foi alterado nesta avaliação. Safety usada: snapshot aprovado
92c5208/1.7, com fonte/12includes/wrappers/binários isolados e identificados.
Nenhuma biblioteca de branch de trabalho foi recompilada ou instalada no principal.

A UI configurou ativo/TF/preset e executou **um** backtest dirigido do contexto
que ainda não tinha referência compatível:13→15/10/2026. Uma posição encerrada,
sem entrada desprotegida, resultado-250,92USD e caixa/CSV conciliados. Não é campanha
estatística, otimização ou recomendação de desempenho. O resultado negativo deve
continuar visível na revisão do PLAY, sem fabricar uma aprovação.

Os12inputs reais foram confrontados com o diário do agente e preset/defaults.
As verificações atuais do PLAY passaram após esse backtest. A leitura da conta
confirmou DEMO/permissão para Experts, cotações atualizando e nenhuma posição/ordem
no instante verificado; essa leitura não autoriza outra conta/contexto.

**Ao vivo não foi iniciado.** O replay próprio foi interrompido ANTES de iniciar o
Tester: terminal privado não confirmou sincronização em90s. O catálogo nativo da
mesma conta DEMO/build estava preparado; portanto essa mitigação, que permitiu o
replay do cooldown, não é uma recuperação universal. A causa final da intermitência
não foi comprovada. Não desligar proteções, trocar credenciais ou reiniciar o
principal/ponte para contornar a pré-condição.

14PIDs próprios observados ausentes,0zombies/0reutilizados,2perfis vazios e nenhuma
reserva nativa remanescente. Ponte/principal preservados e conectados à DEMO.
Recibo [T8_7_DEMO_PARCIAL_20261020.json](../evidencias/T8_7_DEMO_PARCIAL_20261020.json).

## Próxima ação concreta

Recuperar sincronização estável do perfil próprio e registrar confirmação nativa;
só depois concluir o replay faltante, preservando o backtest se código/parâmetros/
plataforma continuarem compatíveis. Revalidar conta DEMO/contexto/carteira/proteções
e realizar a observação ao vivo pela UI. Sem alteração da pré-condição, não repetir
sessões dependentes. PLAY/STOP sem negócios só comprovam os contratos exercitados.

O preset usado aqui é o legado conservador existente. Não comprova carregamento ao
vivo de pacote Default/Manual do piloto, nem libera S09/Pullback, carteira/gradientes,
cache ou Windows físico. STOP existente desanexa a sessão própria e não liquida
posições; esse limite precisa ser considerado antes de encerrar uma sessão com saldo.


## SA-REF-01 — proposta operacional para 2 novembro 2026 (aguarda autorização)

**Autorização recebida em01/11/2026:** o proprietário aprovou este roteiro da
PR269/runtime `33c2fa1`, somente em02/11,10–12h America/Sao_Paulo, entradas antes
de11h45. Inclui cancelamento de pendências e fechamento integral apenas da
exposição com origem nesta sessão comprovada, também na interrupção antecipada.
Posse ambígua impede fechamento; exposição alheia deve ser preservada. Revalidar
as pré-condições no início; sem REAL, fechamento global, relaxamento de proteção
ou extensão automática. Casos não exercitados e recibos de execução/encerramento
são obrigatórios. Se restar exposição, manter supervisão e informar o proprietário.
A autorização recebida não é um recibo de execução. Retomada única da tarefa
agendada para09h50; sem host/app disponível ou mecanismos de supervisão e
encerramento, não lançar. Registro privado e estado corrente no registro central.

Este roteiro substitui o roteiro preliminar de 01/11 na continuidade. Não inicia
PLAY pelo horário ou abertura do mercado. A confirmação multi-TF SA-01 é a frente
seguinte; diferenças de lote e as duas M1 ausentes continuam documentadas.
Não exige igualdade financeira com o Python nem altera regras da estratégia.

### Identidade e janela propostas

- Conta conferida em 01/11/2026 às 19:40:56 America/Sao_Paulo (22:40:56Z), pelo
  `MT5Connector.initialize(auto_login=False)` + `controls.account_identity`:
  **DEMO, MetaQuotes-Demo, final 9678**; zero posições e zero ordens na fotografia.
  Identidade completa permanece somente no recibo privado. Revalidar no início;
  outra conta, modo desconhecido ou REAL aborta o ensaio.
- **02/11/2026, 10:00–12:00 America/Sao_Paulo (UTC−03:00; 13:00–15:00Z)**.
  Entradas só até 11:45; últimos 15 minutos reservados à liquidação/conferência.
  Se autorização chegar depois, propor nova janela; não iniciar atrasado por conta própria.
- PLAY **Session** própria da App (`robots/sessions.py`, serviço
  `SessionController.review/open` ou SDK `Session` com `controls.Consent`),
  terminal portátil isolado e UUID gerado pelo serviço. Rótulo do ensaio:
  `SAREF01-DEMO-20261102`; o UUID real será registrado antes de lançar o terminal.
  Não usar o `robots start` legado que abre o terminal principal.
- EURUSD/M5; **magic 540121**, exclusivo para este ensaio na conta; preset
  `SAREF01_G1915_SAREF01_DEMO`, semente broker **2026.09.01 00:00**,
  variante 1, armado, risco 0,25%, perda diária/semanal 2%/4%, DD 10%, máximo
  uma posição. Sem alterar o preset histórico. A Session escreve `session.set`
  efetivo, inclusive `InpExportTrades=false` para não tocar CSV compartilhado;
  o histórico nativo da conta fornece os deals. Guardar ambos os hashes.
- Leitura nativa de 01/11: **12.659 M5**, primeira barra em 01/09, última barra
  epoch 1793401200 (relógio broker nativo, sem inferir fuso). Limite 20.000:
  **7.341 barras de folga**, ou **6.341** mantendo reserva de 1.000.
  Esta é a contagem disponível no terminal, não na base principal. Antes do PLAY,
  repetir a leitura: semente deve existir, histórico estar sincronizado, contagem
  ≤19.000, sem `SAREF_BLOCKED`. Não mudar a semente em silêncio.

Pré-condições adicionais: terminal/build e fonte/closure conferidos nos pins;
compilar wrapper próprio pelo MetaEditor oficial; arquivar EX5 realmente
executado, fonte incluído e hashes; gate de conta/consentimento revisado para o
contexto exato; cotação recente e sessão EURUSD aberta no terminal. O horário
proposto não prova abertura de mercado. Pré-leitura com catálogo/registro existente
em modo somente leitura encontrou o run Tester DEMO `a0e00b7a…` correspondente
e zero impedimentos em `controls.play_issues`; esse resultado expira com mudanças
de código/conta/carteira/contexto e deve ser refeito no início. Se um gate do PLAY recusar, registrar a
recusa e abortar o lançamento; não fabricar aprovação estatística ou bypass.
Em conta netting, nenhuma posição alheia EURUSD pode existir: mistura de posse
aborta o ensaio. Revalidar posições/pendências/magic após compilar e antes de operar.

### Casos e evidência de cada um

| Caso | Ação controlada | Evidência esperada |
|---|---|---|
| Identidade/inputs | Recibo nativo opt-in com a lista pública `super_atr_research.AUDIT_INPUT_FIELDS` | UUID, conta DEMO, símbolo/TF, todos os inputs efetivos conferidos com `session.set`; hashes fonte/wrapper/EX5, compile log, build e início/fim observados |
| Operação | Observar uma decisão elegível espontânea, sem forçar entrada | `SAREF_DECISION` → `HDK_STRATEGY_SIGNAL` (inclusive recusa) → ticket de ordem/deal → `DEAL_POSITION_ID` → saída(s), volume restante e motivos do histórico |
| Pausa/retomada | Botão PAUSAR do painel HADOUKEN desta Session; depois retomar pela mesma instância | Estado PAUSADO, nenhum negócio de entrada enquanto pausado; decisões/sinais posteriores e nenhum retry duplicado |
| Reinício | Somente com carteira própria zerada, parar a Session e abrir uma sucessora própria revisada | Dois UUIDs/EX5 e continuidade separada, barra corrente pulada; nenhuma segunda entrada da mesma decisão |
| Recusa | Observar recusa real sem relaxar limites; se não ocorrer, registrar não exercitado | Motivo completo `recusada`/`opened=0`, nenhum deal correspondente; recusa por limite QA já tem evidência Tester separada, sem induzir perda ao vivo |
| Desconexão | Com carteira própria zerada e entradas pausadas, desconectar só o login do terminal portátil e reconectar pelo fluxo MT5 | Horários do diário, ausência de ordens durante a falha e nenhuma duplicação após retorno; não cortar rede do host nem reiniciar ponte/terminal principal |
| Encerramento | Procedimento abaixo | Bloqueio de entradas, cada cancelamento/saída por ticket, duas fotografias frescas, histórico/deals e auditoria sem exposição própria pendente |

Caso sem oportunidade em duas horas fica **não exercitado**, nunca aprovado por
zero negócios. Não estender o horário nem criar outra estratégia/preset para
forçá-lo. Preservar recusas, eventos não pareados e divergências na evidência.

Interromper novas entradas imediatamente por modo não DEMO/identidade alterada,
ordem sem decisão, entrada duplicada, posição sem SL, execução repetidamente
recusada, perda diária ≥2%, limite de risco do host, prefixo revisado, semente
indisponível/sem margem ou perda de supervisão. Pausar e tratar exposição;
interrupção não significa liquidação. A sessão não recebe aceite financeiro.

### Encerramento autorizado pelo roteiro

A aprovação deste roteiro inclui **cancelar pendências e fechar integralmente
apenas posições comprovadamente originadas nesta sessão**, inclusive antes do
horário final por critério de interrupção. Nenhum fechamento global é autorizado.

1. Às 11:45 (ou interrupção), usar **PAUSAR no painel do host desta Session**,
   conferir PAUSADO no painel/diário e ausência de novas entradas. `robots stop`
   grava `HDK_KILL.txt` e bloqueia novas entradas em todos os consumidores desse
   Common; não liquida e pode alcançar outras sessões. Não usá-lo como rotina
   de encerramento isolado. Se o painel não responder, desanexar apenas o Expert
   do gráfico próprio no MT5; manter o terminal e a supervisão abertos para liquidar.
2. Levantar ordens pendentes e posições atuais pelo terminal/histórico e snapshot
   somente leitura. Selecionar por conta + símbolo + magic + intervalo + ordens/deals
   de entrada ligados ao UUID. Magic ou símbolo isolados não provam posse.
   `POSITION_TICKET`, `POSITION_IDENTIFIER`, `DEAL_POSITION_ID` e ticket de ordem
   são campos distintos. Posição líquida compartilhada/posse ambígua exige avaliação
   do proprietário: manter supervisão e não agir sobre exposição possivelmente alheia.
3. No **terminal MT5 desta sessão**, aba Negociação, cancelar **cada pendência
   própria por ticket**, confirmar no histórico de ordens e reler a lista. Fechar
   **cada posição própria por ticket** na mesma aba, confirmar o(s) deal(s) de saída
   e volume restante zero. O host/SL/TP pode ter fechado antes: reler antes de agir
   para não enviar fechamento redundante. Não usar FECHAR TUDO, fechamento por
   símbolo, automação Python de envio de ordens nem kill de rede/processos globais.
4. Recolher duas fotografias novas de ordens/posições, separadas por um ciclo do
   terminal e consulta do histórico. Zero pendências próprias + zero posições
   próprias + volumes de entrada/saída conciliados são necessários. Registrar
   também o inventário alheio, preservado, sem publicar dados privados.
5. Somente depois, `session.stop`/STOP da App encerra o processo/lease **próprio**.
   Exportar diário, inputs e histórico; executar a auditoria abaixo e arquivar os
   recibos. Não declarar “tudo parado” por telemetria antiga, CSV vazio, painel
   fechado ou sucesso do STOP. Falha de liquidação/mercado fechado/desconexão:
   registrar **exposição remanescente**, manter supervisão e informar ticket,
   volume e causa ao proprietário; o fim da janela não autoriza abandono.

### Auditoria por sessão (novembro e datas posteriores)

`scripts/sa_ref_reconcile.py` permanece **reprodução histórica fixa de outubro**;
seus pacotes, M1 e preset histórico não são insumos substituíveis de uma sessão nova.
A auditoria operacional usa `robots.session_audit`, os leitores `parity`, os
recibos públicos `native_inputs` e o backend oficial. Sem gêmeo Python/banco novo.

No SDK `Session(..., mode="play", account=conta_demo, consent=consentimento,
input_receipt_fields=AUDIT_INPUT_FIELDS, expected_native_inputs=valores_efetivos)`
emite recibo nativo próprio antes do primeiro tick. A leitura opt-in DEMO é
separada do recibo Tester; REAL permanece recusado. O controlador padrão não
liga esse opt-in sozinho: para este ensaio usar Session pelo SDK existente,
com os mesmos gates `controls.play_issues`/`Consent`; não lançar outro executor.
Conferir recibo no `Common/Files/mt5x_session_<UUID>_inputs.csv`, cópia exata do
`session.set` e do wrapper/EX5 da pasta própria. O recibo não aprova operação.

Capturar com backend já conectado (nenhum login/envio de ordem pelo script):

```sh
.venv/bin/python scripts/demo_session_audit.py snapshot --session-id UUID \
  --symbol EURUSD --magic 540121 --start 2026-11-02T13:00:00+00:00 \
  --end 2026-11-02T15:00:00+00:00 --out PASTA_PRIVADA/snapshot.json
.venv/bin/python scripts/demo_session_audit.py audit --session-id UUID \
  --symbol EURUSD --magic 540121 --tf M5 --log PERFIL_PROPRIO/logs/20261102.log \
  --preset PERFIL_PROPRIO/MQL5/Presets/session.set --source FONTE_INCLUIDO.mq5 \
  --wrapper PERFIL_PROPRIO/MQL5/Experts/mt5x_session.mq5 \
  --binary PERFIL_PROPRIO/MQL5/Experts/mt5x_session.ex5 --inputs RECIBO_INPUTS.csv \
  --snapshot PASTA_PRIVADA/snapshot.json --out PASTA_PRIVADA/auditoria.json
```

Adaptar `end` ao instante real da consulta; capturar também o estado anterior e
as duas conferências finais em arquivos distintos. `--log` pode repetir para
logs próprios: cada origem conserva contagens; repetição interna ou conflitos
permanecem ambíguos. O JSON conserva eventos, tentativas/refusas não pareadas,
deals órfãos, exposição e relógios nativos/broker versus observação com fuso.
A cadeia usa `deal.ticket → order/position_id`; saídas manuais de magic zero são
ligadas pela posição. Reversões netting e posse misturada ficam ambíguas.
Sem fotografia íntegra, estado final **unknown**; fotografia sem exposição é
**flat_snapshot**, jamais um aceite operacional automático.

Regressões/ensaio de arquivos com dados **sintéticos de 03/11/2026** demonstram
entrada/ordem/posição com IDs distintos, saídas parciais/manuais, recusa, repetição,
conflito, exposição e falha de identidade. Não são prova de execução DEMO real.
