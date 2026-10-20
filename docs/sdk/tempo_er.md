# T19.16 inicial — TM05, ER com idade

Recorte: ER(20) já canônico, sem implementar canal/ATR adaptativo, estratégia nova,
gerador ou outro store. `temporal_er_v1` acrescenta idade mínima em segundos e
limiar ER em fração0..1 ao contrato temporal anterior, que permanece aceito.

ER = abs(close[t]-close[t-20])/sum(abs(diff(close)),20), com21 closes fechados.
Caminho constante válido dá zero; aquecimento, NaN, preço ausente, janela incompleta
ou observação futura não dão zero. A política usa o mesmo timeframe da estratégia.
Sai quando idade>=mínimo e ER estritamente abaixo do limiar. Igualdade mantém;
faixa de igualdade numérica ±1e-12 na escala0..1 também mantém, para não transformar
ruído das20 somas em ordem. Essa faixa é contrato desta política, não arredonda nem
modifica o ER canônico salvo/exportado. Limiar zero não autoriza essa saída.

Idade desde primeiro fill, incluindo gaps; parcial/reinit não reiniciam idade.
ER nunca estende prazo. Proteções, limite absoluto e condições temporais já existentes
têm prioridade; ER desconhecido deixa essas proteções em vigor. ER não depende de
PnL estimado. Sem gradiente/adições neste recorte; essas capacidades mantêm bloqueio.

OHLC decide no fechamento e executa na próxima abertura, preservando precedência de
SL/TP no gap. Nativo lê21 closes de barras anteriores à barra atual recebida; observa
no tick de decisão e registra essa disponibilidade, inclusive correções recebidas.
Diário identifica janela e valor; recibo verifica ER com referência independente,
disponibilidade, condição, volume/fills e caixa, usando o serviço existente.

Novo perfil S09, opt-in no Manual: salvar/reabrir/exportar, inputs conferidos e
consulta de contexto ER e confirmação. Perfis anteriores/resultados permanecem
legíveis, sem migração automática ou promoção de temporal ER ao Pullback.

Ambiente e baseline T19.15 já aprovados e compatíveis são reutilizados. Verificações
novas: ausência/zero, N−1/N/N+1, igualdade/faixa, prefixo e correção da fonte,
prioridade/SL, parcial sem reset, reinit/falta de confirmação e UI. Ensaio dirigido
somente dos ramos novos, após conferir fontes/includes/executável/inputs; sem repetir
campanha histórica. Aceite funcional dirigido concluído, integração/checks em curso.
Recibo em ../evidencias/T19_16_ER_20261020.json: 4 operações no backtest e 4 no replay,
4 fechamentos ER por execução; 32 inputs, caixa e eventos reconciliados.
Fontes e 22 includes instalados conferidos; binários identificados, 12 processos
próprios encerrados e ponte/principal preservados. 147 testes dirigidos aprovados.
Cache inelegível; S09 somente Tester;
live DEMO, Windows físico, carteira e gradientes separados.
