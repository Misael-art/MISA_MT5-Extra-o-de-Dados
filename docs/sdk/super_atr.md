# Super ATR no gráfico do painel — G19.9

## Persistência SA-02

No Laboratório → Séries e confluências → Fontes vinculadas, selecione um ativo
com candles nativos e use **Materializar Super ATR…**. Informe a raiz canônica
HADOUKEN existente, janela em horário broker, período, média0=SMA/1=EMA e desvio.
A barra inicial fixa a semente; alongar a janela pelo fim preserva o prefixo.
O formulário mostra atividade e permite solicitar cancelamento; a etapa do
compilador termina antes de responder ao cancelamento. Não instala ferramentas.

O produtor usa o header e o adaptador C++ originais do HADOUKEN no commit do
manifesto oficial, sem outra fórmula Python. Grava `trend.super_atr.level`,
`trend.super_atr.atr` (números), `trend.super_atr.side` e
`trend.super_atr.turn` (categorias up/down/none). Lado inicial=up; primeiro valor
válido tem virada=none. ATRzero é válido. Warmup e sufixo inválido ficam
UNAVAILABLE, com motivo, sem preencher lacunas com zero. Fórmula, parâmetros,
seed, compilador, hashes do header/adaptador, versão, fonte, manifesto, abertura/
fim UTC, revisão e clocks acompanham os fatos no store/journal existentes.

`available_at` e `observed_at` são o recebimento local da materialização; o
fechamento nominal **não** prova recebimento histórico. Para estudar a curva
histórica, vincule a fonte congelada com **Publicação retrospectiva**, explicitando
esse limite. Consulta no corte anterior à materialização não retorna fatos.
Reexecução idêntica é idempotente; correção conserva o histórico as-of e revisa
o sufixo dependente. Fonte congelada/exportação reutilizam BD18/SDK existente.

CLI: `analytics features super-atr-capabilities --framework RAIZ` e
`analytics features super-atr-materialize --framework RAIZ --manifest ID
--period 20 --mean 0 --deviation 1`. Não altera pins, base principal, conta ou
ordens. Execução do header em C++ não certifica MetaTrader, Expert, Profit ou
desempenho financeiro. As evidências históricas de G19.9 abaixo são de escopo
distinto da materialização SA-02.

O estudoEURUSD/M5 usa DEZ curvas de período20, média aritmética0, com desvios
1 a10, conforme a ampliação solicitada pelo proprietário. Todas pontilhadas em azul, do claro ao escuro conforme o multiplicador.
Na virada, os pontos da mesma curva se ligam em diagonal pontilhada; não há
vazio por troca de estado. Essa ligação visual não muda o cálculo da catraca.
Cada curva pode mudar período, multiplicador e média0/1 (aritmética/exponencial)
nas Propriedades do indicadorMT5/Ctrl+I. Não são ordens ou stops aplicados.

Abrir PLAY do painel manual → Análise → Estudo → + Indicador →
“HADOUKEN · Super ATR · dez curvas”. Precisa da biblioteca candidata completa;
a App instala/compila o indicador antes do adaptador. Aparece na janela principal,
sem requerer uma subjanela. A biblioteca anterior, sem o marcador novo, continua
aceita pela App. Não usar o template anteriorEstudo_ATR_1_3 como especificação.

A linha usa fechamento±desvio×ATR, máxima retida na alta/mínima na baixa.
Inverte quando o fechamento cruza estritamente a linha anterior, resetando a
catraca. Igualdade conserva o lado. PrimeiraSMA completa semeia a média; EMA
seguealpha2/(N+1). Lado inicial explícito, alta por padrão. O candle corrente
é provisório e recalculado a partir da barra anterior, sem acumular várias
catracas no mesmo candle. HLC inválido/overflow torna o sufixo indisponível.

[Contrato matemático e notas teóricas](https://github.com/Misael-art/projeto-indicadores-mt5/blob/3a3137ca/Documentation/HADOUKEN_2/propostas/G19_9_SUPER_ATR.md).
Guarda: oATR altera a distância da BASE, mas a catraca pode impedir recuo da
linha já retida. Estado de alta/baixa é discreto; linha/ATR também guardam
memória numérica. Desvios distintos podem virar em barras diferentes e as
curvas não precisam permanecer aninhadas. Otimização por volatilidade/volume/
tendência/ativo e50 curvas são pesquisa futura, sem adaptação implementada.

Catálogo da App acrescenta somente o marcadorHDK_StudySuperATR.mqh → ativo
HDK_SuperATR.mq5 e fecha seus includes. Pin manuald377c812aafd0c5e foi conferido
na biblioteca real3b979cf1, três ativos e matemática; alterar a fórmula invalida
a identificação. Mantém pins antigos, conta, consentimentos, stop, limites e
exceção manual exclusiva. Nenhuma capacidade automática é ampliada; novos
fingerprints de outros consumidores precisam dos próprios aceites. Não
atualiza a distribuiçãoSafety nem o canal aceitoapp-current.

Validação local:126 testesApp;88HADOUKEN e checker161; compilação6281/Wine
0erros/0avisos nos três ativos e adaptador do painel. Identidade real conferida
com mutação recusada. Doctor20OK/1opcionalpandas-ta/0falhas, no perfil instalado
reutilizado (bootstrap/configuração não alterados). Versão1.01 compilada e inspecionada:EURUSD/M5, três curvas pontilhadas,
patamares e diagonais nos dois sentidos, fundoMinimalB&W/clean, sem marca
d’água/relógio, painel pausado e nenhuma exposição observada. A prova foi feita
na composição App0b2fda3 +He01540f3, que inclui a correção adicional de monitor
[PR204](https://github.com/Misael-art/MISA_MT5-Extra-o-de-Dados/pull/204).
Esta correção é necessária para preencher também a área interna Wine no monitor
maior; os runtimes de catálogo/pin destePR são idênticos nessa composição.
A prévia do atalho usa essa composição; canal aceito preservado. [Evidência](../evidencias/G19_9_SUPER_ATR_20261023.json).

Windows físico, referência numérica externaProfit,50curvas e aceites financeiros/
operacionais não são qualificados por este estudo. Alteração externa dos
parâmetros conserva a políticaG19 de preservar identidades modificadas:
controle/remoção pelo diálogoMT5 quando o controlador não reconhece a cópia.

![Três curvas pontilhadas e diagonais no EURUSD M5](../img/super_atr/eurusd-m5-three-curves.png)

Captura da área de gráfico3456×1363, monitor3456×1458 informado pelo sistema;
contagens dos pixelsRGB2940/2586/2493. Conexão diagonal é apresentação entre
amostras, sem mudar a catraca ou afirmar um caminho de preço intrabar.

### Versão1.02 — dez curvas e cinza claro

A versão1.02 amplia os inputs independentes para dez curvas. Preserva as
regras da catraca/diagonais. O tema **Cinza Claro** fica na última página de
Visual → Gráfico e temas; com Identidade do gráfico → Degradê ligado, o fundo
vai deRGB242 a210, neutro nos três canais. Marca d’água e Candle Time podem
ser desligados nos controles existentes. Índices dos15 temas anteriores
preservados. Versão1.02 instalada e inspecionada no atalho habitual, composta com
Appf7c3371/H3b979cf1:3456×1458,10 tonsRGB presentes, diagonais, fundo neutro
com amostras240/225/212, painel pausado e nenhuma ação de ordem enviada; a captura acima é
histórica1.01 de três curvas, não prova de dez.

![Dez curvas Super ATR sobre degradê cinza claro](../img/super_atr/eurusd-m5-ten-curves-gray.png)

Validação1.02:89Tools/checker161/compilação6281Wine0/0,126App; composição
monitor128 testes +58 dirigidos pósmerge (sobrepostos). PréviaAppf7c3371 e
H3b979cf1 têm runtime final; commits documentais posteriores não mudam fontes.
PLAY no dock3456×1458 e REPLAY no handheld1280×800 verificados separadamente;
replay foi de1.01 com indicadores anteriores, não prova das10 curvas.
