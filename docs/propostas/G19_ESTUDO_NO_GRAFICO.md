# G-19 — Play manual e ferramentas de estudo no gráfico

Pedido do proprietário em2026-10-22. Continuação do G-18; manter o design,
as duas orientações e a prévia acessível pelo atalho habitual. Uma tarefa por
PR, com testes/aceite e atualização do status em cada entrega.

| Tarefa | Entrega | Dependência | Estado |
|---|---|---|---|
| G19.1 | PLAY do painel manual sem backtest | controles/card/sessão atuais | Validado; instalação pendente |
| G19.2 | Etiquetas compactas, quebra de linha e tamanho de fonte | G-18 | PR63 validado; instalação pendente |
| G19.3 | Entrada/saída: A sinal e B tipo de execução, independentes | eventos observados | PR64 validado; instalação pendente |
| G19.4 | Sinais ignorados amarelos e motivo curto, visibilidade configurável | G19.3 | Em validação nativa |
| G19.5 | Análise → Estudo: notas, editor e fotografia persistida no banco | contrato nativo/App | Em implementação |
| G19.6 | Lápis livre e coloração de candles por indicador/estratégia | G19.5/catálogo | Planejada |
| G19.7 | Indicadores múltiplos com +, agrupar/desagrupar por item | G19.5/catálogo | Planejada |

## Regras de implementação e aceite

1. Painel manual identificado pelo contrato e fonte próprios, sem exigir
   resultado histórico para classificá-lo. Nunca classificar um Expert
   automático como manual só porque não operou em um backtest. Retirar o
   requisito de backtest no núcleo, card e revisão PLAY, sem fabricar um run.
   Preservar leitura de segurança, conta, stop, limites e parada explícita.
2. Padrão: Buy/Sell na primeira linha e resultado na segunda; opção para
   linha única. Fonte inicial igual à atual, com controles de tamanho e
   restauração. Medir cada linha e o conjunto, respeitar DPI, painel e eixos.
   Mesmas preferências para trades e sinais ignorados, persistidas por gráfico.
3. Controles independentes para sinal(A) e tipo(B) na entrada e na saída:
   nenhum/A/B/ambos. Fixar, gradiente, breakeven e trailing só quando a origem
   da execução os comprovar. Não deduzir sinal de estratégia a partir de um
   lucro, magic ou comentário livre. Histórico sem proveniência: indisponível.
4. Registrar o sinal candidato antes das decisões de gestão. Somente eventos
   realmente observados e não executados geram marca amarela. Motivo conhecido
   em uma palavra (ex.: Limite, Horário, Spread, Pausa); detalhes na dica. Não
   inventar oportunidades retrospectivas. Visibilidade ligada por padrão e
   desligável; opção de quebra e fonte igual à dos trades. Registro separado
   do livro financeiro: recusa não é negócio nem resultado zero.
5. Estudo na aba Análise: nota posicionada em tempo/preço, indicador visual
   de pena, editor, texto persistido em coluna e snapshot imutável associado.
   Guardar símbolo/TF, candle escolhido, horário da captura, origem/parâmetros,
   valores e indisponibilidades. Snapshots históricos observados na captura
   não são prova de disponibilidade histórica de indicadores que repintam.
   Nativos do MT5 e cálculos efetivamente disponíveis do extrator; não simular
   valores ausentes. Reabrir a nota não recalcula nem altera a fotografia.
   Usar migração aditiva e banco/serviços existentes, sem banco paralelo.
6. Lápis desenha livremente, com seleção/remoção apenas dos próprios objetos;
   não captura ordens ou arrasto do painel. Coloração de candles seleciona
   regra real do catálogo. Itens do extrator primeiro e com destaque de fonte;
   nativos/customizados disponíveis identificados com origem e parâmetros.
7. Inserção de vários indicadores e agrupamento reversível por caixa de
   seleção; mesma escala agrupada por padrão, separável pelo usuário. Nunca
   remover indicadores externos ao Estudo. Cuidar do ciclo de vida dos handles,
   de subjanela/símbolo/TF e de remoção/recriação sem duplicar objetos.

## Ordem e verificação

Ambiente existente reutilizado; instalador obrigatório executado em árvore
isolada com restauração dos atalhos, sem alterar configuração operacional.
Baseline do framework83 testes/2 subtestes em40,60s, antes das alterações.
Baseline completo da App:2892 aprovados,52 pulados,3 avisos, na composição
publicada7b2062d4ef32 em display isolado. WIP da raiz preservado.

Para cada tarefa: testes significativos sem MT5 obrigatório; compilação MQL5
quando aplicável; ensaio nativo/GUI no Linux/Wine; doctor; recibo de instalação
da ponta exata na prévia. Windows físico permanece pendência registrada.
Não declarar disponíveis ferramentas apenas documentadas ou controles sem
consumidor real. Os ensaios não enviam ordens a conta REAL.

## Validação parcial em2026-10-22

G19.2: [PR63](https://github.com/Misael-art/projeto-indicadores-mt5/pull/63),83 testes/2 subtestes,16 compilações sem erros/avisos e182 controles nativos por96/144 DPI.
G19.3: [PR64](https://github.com/Misael-art/projeto-indicadores-mt5/pull/64),83 testes/2 subtestes, quatro consumidores recompilados com headers finais por hash,195 controles nativos por96/144 DPI.
App PLAY:65 testes dirigidos aprovados; suíte completa final2901 aprovados,52 pulados,3 avisos. Doctor16 OK/2 avisos/0 falhas no perfil isolado.
Notas:15 testes dirigidos aprovados (banco/fotografia/transporte/UI). Integração nativa e instalação ainda pendentes.
