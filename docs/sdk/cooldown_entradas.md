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

Aceite funcional dirigido registrado em `docs/evidencias/T19_20_COOLDOWN_20261020.json`.
UI salvou/reabriu/exportou Manual;44inputs efetivamente carregados. EURUSD/M5,
14→15/10/2026, espera3600s/saída absoluta600s: backtest e replay2posições/4fills,
2recusas e nova entrada posterior;4decisões/eventos/livro/CSV/caixa18USD iguais.
Consulta após fim do iniciador:4decisões/4fills/44inputs, sem outro ensaio.

Pré-condição nativa: catálogo de símbolos da mesma conta DEMO/build6274 preparado
somente no perfil próprio, sem credenciais em texto ou alteração de principal/ponte.
A sincronização foi explicitamente confirmada antes do replay; duas falhas anteriores
permanecem arquivadas, sem aceite. Recuperação automática não foi implementada.
Este catálogo nativo não qualifica cache de execução/evidência, que segue inelegível.

27PIDs próprios observados ausentes/0reutilizados/0zombies,5perfis vazios e contexto
nativo liberado; ponte e principal preservados/conectados DEMO.27includes, wrappers,
binários e INIs conferidos nos dois resultados aceitos.124 testes dirigidos e63
regressões pertinentes aprovados (grupos sobrepostos),30Tools/estática127 e doctor
21OK/1opcional/0falhas. H35 tem2checks aprovados; M162 mantém workflow39507890504
existente no runtime4517299, sem novo dispatch. Integração H35→M162 condicionada
à revisão e aos6checks. Aceite limitado ao perfil/cenário: S09 continua somente Tester.
Pedido posterior de execução ao vivo DEMO tem avaliação/aceite próprios, com Expert
compatível; não remove os bloqueios do S09 ou do adaptador piloto.
