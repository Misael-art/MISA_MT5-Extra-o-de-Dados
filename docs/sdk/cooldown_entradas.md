# T19.20/02 — espera após fechamento total confirmado

No Default S09v1.80 o cooldown está desativado. Crie um Manual nomeado, informe
**Espera após fechamento total confirmado (segundos)**, salve a revisão e reabra.
Zero desativa; valores inteiros1..2147483647 ativam. Horários/dias/cota de sessão
podem ser combinados e continuam independentes da espera. O pacote exportado
mapeia `filters[i].params.seconds` para `InpCooldownSeconds`, unidade
`second_after_confirmed_final_close`. Perfil9 tem44inputs; perfis1–8 e presets
anteriores permanecem consultáveis, sem migração silenciosa.

O gate só pode limitar novas entradas. SL, saídas temporais, parciais e saldo
continuam no executor compartilhado. Não há encerramento agendado neste recorte.
S09 continua restrito ao Strategy Tester DEMO; nenhum aceite histórico habilita
live DEMO. Pullback neste gate, carteira/gradientes e Windows físico conservam
aceites próprios; cache de execução/evidência continua inelegível.

## Âncora e disponibilidade

No MT5 a âncora é o `DEAL_TIME_MSC` do último fechamento TOTAL próprio comprovado.
Entradas/saídas são agregadas pelo identificador da posição em unidades inteiras
do passo de volume. Redução parcial não inicia/reinicia a espera. Confere-se saldo
visível, ownership, histórico e identidade persistida. Ordem recusada/cancelada sem
fill não cria âncora; desaparecimento de posição sem fechamento confirmado não
vira autorização. História incompleta/corrigida, hash incompatível, falha de
persistência e relógio regressivo recusam apenas novas entradas, com motivo no diário.

Relógio broker em milissegundos; tick coerente com TimeCurrent, sem inferir UTC,
DST, pregão ou fechamento futuro. Liberação em `último fechamento + segundos*1000`,
**inclusive**, também após reinício com histórico/identidade compatíveis.
Sem fechamento anterior em contexto novo verificado, a primeira entrada não espera.

Na simulação Python, declarar `time_basis="broker"` e timeframe. Saída na abertura
fica conhecida na abertura; saída intrabar/no fechamento fica conhecida no fim
nominal da barra, reutilizando o relógio do Journal. O `Trade.exit_time` legado
não prova tick intrabar. A simulação OHLC não substitui execução/replay MT5.

## Consulta e evidência

A aba **Espera · fechamento confirmado** expõe origem, instante conhecido,
fechamento total, liberação, duração e motivo; milissegundos ficam acessíveis.
Consulta e pacote/CSV reutilizam persistência/exportação existentes. O recibo
`HDK_COOLDOWN` é validado por referência independente de saldo dos fills (Decimal),
sem chamar o predicado de produção. Replay compara eventos, inputs, posições,
reduções/fills/custos e resultado. Aceite requer negócios, recusa durante espera
e nova entrada posterior conciliados, não apenas ensaio sem negócios.

Estado parcial registrado em `docs/evidencias/T19_20_COOLDOWN_20261020.json`.
UI salvou/reabriu/exportou Manual;44inputs efetivamente carregados. EURUSD/M5,
14→15/10/2026, espera3600s/saída absoluta600s: backtest2posições/4fills,
2recusas por espera e nova entrada posterior; livro/CSV/caixa18USD conciliados.
Consulta após fim do iniciador:4decisões/4fills/44inputs, sem outro ensaio.

**Replay bloqueado, sem aceite funcional e sem integração das PRs H35→M162.**
Perfil privado autenticou, mas Tester recusou por falta de sincronização com servidor
(connect status3,100). Uma preparação limitada a90s, sem Expert/ordens, também não
confirmou sincronização; replay dependente nem iniciou. LiveUpdate disponível foi
observado, sem atribuir-lhe causa comprovada. Não haverá outra tentativa sem corrigir
esta pré-condição; preservar backtest e conferir compatibilidade antes de reutilizá-lo.

18PIDs próprios observados ausentes/0reutilizados/0zombies,4perfis vazios e contexto
nativo liberado; ponte e principal preservados/conectados DEMO.27includes, wrapper,
binário e INI do backtest conferidos.124 testes dirigidos e63 regressões pertinentes
aprovados (grupos sobrepostos),30Tools/estática127 e doctor21OK/1opcional/0falhas.
H35 tem2checks aprovados; M162 mantém workflow39507890504 existente no runtime4517299:
4checks aprovados, Ubuntu/Windows em andamento neste registro; não duplicar workflow.
Revisão técnica sem achados impeditivos de código; aceite nativo continua pendente.
