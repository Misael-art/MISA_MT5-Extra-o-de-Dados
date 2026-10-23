# Super ATR no gráfico do painel — G19.9

O estudoEURUSD/M5 usa três curvas de período20, média aritmética0, com desvios
1/2/3. Todas pontilhadas em azul, do claro ao escuro conforme o multiplicador.
Cada curva pode mudar período, multiplicador e média0/1 (aritmética/exponencial)
nas Propriedades do indicadorMT5/Ctrl+I. Não são ordens ou stops aplicados.

Abrir PLAY do painel manual → Análise → Estudo → + Indicador →
“HADOUKEN · Super ATR · três curvas”. Precisa da biblioteca candidata completa;
a App instala/compila o indicador antes do adaptador. Aparece na janela principal,
sem requerer uma subjanela. A biblioteca anterior, sem o marcador novo, continua
aceita pela App. Não usar o template anteriorEstudo_ATR_1_3 como especificação.

A linha usa fechamento±desvio×ATR, máxima retida na alta/mínima na baixa.
Inverte quando o fechamento cruza estritamente a linha anterior, resetando a
catraca. Igualdade conserva o lado. PrimeiraSMA completa semeia a média; EMA
seguealpha2/(N+1). Lado inicial explícito, alta por padrão. O candle corrente
é provisório e recalculado a partir da barra anterior, sem acumular várias
catracas no mesmo candle. HLC inválido/overflow torna o sufixo indisponível.

[Contrato matemático e notas teóricas](https://github.com/Misael-art/projeto-indicadores-mt5/blob/316cb6ba1395198d30dd5068877df25f69e24e0c/Documentation/HADOUKEN_2/propostas/G19_9_SUPER_ATR.md).
Guarda: oATR altera a distância da BASE, mas a catraca pode impedir recuo da
linha já retida. Estado de alta/baixa é discreto; linha/ATR também guardam
memória numérica. Desvios distintos podem virar em barras diferentes e as
curvas não precisam permanecer aninhadas. Otimização por volatilidade/volume/
tendência/ativo e50 curvas são pesquisa futura, sem adaptação implementada.

Catálogo da App acrescenta somente o marcadorHDK_StudySuperATR.mqh → ativo
HDK_SuperATR.mq5 e fecha seus includes. Pin manual44b1a783eae8435b foi conferido
na biblioteca real316cb6ba, três ativos e matemática; alterar a fórmula invalida
a identificação. Mantém pins antigos, conta, consentimentos, stop, limites e
exceção manual exclusiva. Nenhuma capacidade automática é ampliada; novos
fingerprints de outros consumidores precisam dos próprios aceites. Não
atualiza a distribuiçãoSafety nem o canal aceitoapp-current.

Validação local:126 testesApp;88HADOUKEN e checker161; compilação6281/Wine
0erros/0avisos nos três ativos e adaptador do painel. Identidade real conferida
com mutação recusada. Doctor20OK/1opcionalpandas-ta/0falhas, no perfil instalado
reutilizado (bootstrap/configuração não alterados). Inspeção visual e instalação
candidata em curso. [Evidência](../evidencias/G19_9_SUPER_ATR_20261023.json).

Windows físico, referência numérica externaProfit,50curvas e aceites financeiros/
operacionais não são qualificados por este estudo. Alteração externa dos
parâmetros conserva a políticaG19 de preservar identidades modificadas:
controle/remoção pelo diálogoMT5 quando o controlador não reconhece a cópia.
