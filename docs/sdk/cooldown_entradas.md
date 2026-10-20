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

Implementação e verificação dirigidas em andamento; aceite e revisões finais
serão registrados em `docs/evidencias/T19_20_COOLDOWN_20261020.json` após conclusão.
